package llm_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/callerhttp"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
	"github.com/vibe-agi/human/llm/workerws"
	workersqlite "github.com/vibe-agi/human/llm/workerws/sqlite"
)

const networkFaultCodecID llm.CodecID = "test.network-fault"

// TestOfficialAdaptersRecoverOneResponseAcrossThreePartyOutage is the
// cross-adapter fault matrix. It deliberately uses real HTTP and WebSocket
// sockets plus both official SQLite durability adapters. There are no soak
// delays: every phase advances on a protocol or lifecycle boundary.
func TestOfficialAdaptersRecoverOneResponseAcrossThreePartyOutage(t *testing.T) {
	servicePath := filepath.Join(t.TempDir(), "service.db")
	journalPath := filepath.Join(t.TempDir(), "worker-journal.db")
	body := []byte(`{"model":"human","stream":true,"messages":[{"role":"user","blocks":[{"type":"text","text":"help"}]}]}`)

	service := openNetworkFaultService(t, servicePath)
	callerRuntime, callerServer := openNetworkFaultCaller(t, service)
	workerRuntime, workerServer := openNetworkFaultGateway(t, service)
	client := openNetworkFaultWorker(t, workerServer.URL, journalPath)
	registerNetworkFaultCleanup(t, service, callerRuntime, callerServer, workerRuntime, workerServer, client)

	// The caller observes the committed stream prefix and then disappears. Its
	// request context is canceled, but the durable response and assignment live
	// beyond that socket.
	firstRequest := newNetworkFaultRequest(t, callerServer.URL, body)
	firstResponse, err := http.DefaultClient.Do(firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	if firstResponse.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(firstResponse.Body)
		firstResponse.Body.Close()
		t.Fatalf("initial caller status = %d body=%q", firstResponse.StatusCode, payload)
	}
	reader := bufio.NewReader(firstResponse.Body)
	if prefix, readErr := reader.ReadString('\n'); readErr != nil || prefix != "start\n" {
		firstResponse.Body.Close()
		t.Fatalf("initial committed stream prefix = %q, %v", prefix, readErr)
	}
	assignment := receiveNetworkFaultAssignment(t, client.Assignments())
	if err := client.ConfirmAssignment(t.Context(), assignment.ID); err != nil {
		firstResponse.Body.Close()
		t.Fatalf("confirm application assignment: %v", err)
	}
	if err := firstResponse.Body.Close(); err != nil {
		t.Fatalf("disconnect caller: %v", err)
	}
	shutdownNetworkFaultRuntime(t, callerRuntime)
	callerServer.Close()

	// Drop the WebSocket gateway before accepting either event. The remote
	// worker durably queues a deterministic stale-lease poison followed by the
	// real final response; FIFO replay must settle the poison without stranding
	// the follower.
	shutdownNetworkFaultRuntime(t, workerRuntime)
	workerServer.Close()
	poison := llm.WorkerEventDelivery{
		ID:       "delivery-stale-lease",
		Identity: assignment.Assignment.Identity,
		LeaseID:  "lease-stale",
		Event: llm.Event{
			ID: "event-stale-lease", Type: llm.EventProgress, Text: "must not become visible",
		},
	}
	final := llm.WorkerEventDelivery{
		ID:       "delivery-final",
		Identity: assignment.Assignment.Identity,
		LeaseID:  assignment.Assignment.Lease.ID,
		Event:    llm.Event{ID: "event-final", Type: llm.EventFinal, Text: "done"},
	}
	if err := client.SendEvent(t.Context(), poison); err != nil {
		t.Fatalf("journal poison while gateway offline: %v", err)
	}
	if err := client.SendEvent(t.Context(), final); err != nil {
		t.Fatalf("journal final while gateway offline: %v", err)
	}
	shutdownNetworkFaultRuntime(t, client)
	shutdownNetworkFaultRuntime(t, service)

	// At this point caller, worker process, WebSocket gateway, and Service are all
	// offline. Reopen the two SQLite resources through fresh runtimes, proving
	// that no process-local queue is required for convergence.
	service = openNetworkFaultService(t, servicePath)
	callerRuntime, callerServer = openNetworkFaultCaller(t, service)
	workerRuntime, workerServer = openNetworkFaultGateway(t, service)
	client = openNetworkFaultWorker(t, workerServer.URL, journalPath)
	registerNetworkFaultCleanup(t, service, callerRuntime, callerServer, workerRuntime, workerServer, client)

	select {
	case rejected, open := <-client.Rejections():
		if !open {
			t.Fatal("worker stopped before presenting the durable poison NACK")
		}
		if rejected.Delivery.ID != poison.ID ||
			rejected.Receipt.Decision != llm.WorkerEventNACK ||
			rejected.Receipt.Code != llm.WorkerRejectStaleLease {
			t.Fatalf("poison settlement = %#v", rejected)
		}
		if err := client.ConfirmRejection(t.Context(), poison.ID); err != nil {
			t.Fatalf("confirm poison rejection: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("poison did not settle after recovery")
	}

	// Exact caller retry blocks only until the healthy FIFO follower commits.
	// A second retry must be byte-identical and must not enqueue another response.
	firstComplete := doNetworkFaultRequest(t, callerServer.URL, body)
	secondComplete := doNetworkFaultRequest(t, callerServer.URL, body)
	if firstComplete != "start\nfinal:done\n" {
		t.Fatalf("recovered response = %q", firstComplete)
	}
	if secondComplete != firstComplete {
		t.Fatalf("exact HTTP replay changed bytes: first=%q second=%q", firstComplete, secondComplete)
	}

	// Inspect the transport-neutral replay once. The stale event has no wire
	// effect, and the healthy final appears exactly once in durable response data.
	admission, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "request-a", CodecID: networkFaultCodecID, Body: body,
	})
	if err != nil {
		t.Fatalf("inspect exact admission replay: %v", err)
	}
	if !admission.Replay {
		t.Fatal("HTTP retries created a new admission")
	}
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "request-a", RequestDigest: admission.RequestDigest,
	})
	if err != nil {
		t.Fatalf("inspect durable response: %v", err)
	}
	if !page.Complete || len(page.Events) != 2 ||
		string(page.Events[0].Data) != "start\n" || string(page.Events[1].Data) != "final:done\n" {
		t.Fatalf("durable exact-once response = %#v", page)
	}
}

func openNetworkFaultService(t *testing.T, path string) *llm.Service {
	t.Helper()
	store, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "network-fault-test",
		Store:        store,
		Codecs: []llm.CodecRegistration{{
			Codec: networkFaultCodec{}, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: http.StatusOK,
		}},
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
		Admission: llm.AdmitAll(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func openNetworkFaultCaller(
	t *testing.T,
	service llm.CallerEndpoint,
) (llm.CallerTransportRuntime, *httptest.Server) {
	t.Helper()
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: callerhttp.AuthenticateFunc(func(ctx context.Context, _ *http.Request) (callerhttp.Identity, error) {
			if err := ctx.Err(); err != nil {
				return callerhttp.Identity{}, err
			}
			return callerhttp.Identity{CallerID: "caller-a"}, nil
		}),
		Routes: []callerhttp.Route{{
			Method: http.MethodPost, Path: "/v1/chat/completions", CodecID: networkFaultCodecID,
		}},
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), service)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, httptest.NewServer(transport)
}

func openNetworkFaultGateway(
	t *testing.T,
	service llm.WorkerEndpoint,
) (llm.WorkerTransportRuntime, *httptest.Server) {
	t.Helper()
	transport, err := workerws.New(workerws.Config{
		GatewayID: "gateway-a",
		Authenticator: workerws.AuthenticateFunc(func(ctx context.Context, _ *http.Request) (workerws.Identity, error) {
			if err := ctx.Err(); err != nil {
				return workerws.Identity{}, err
			}
			return workerws.Identity{Worker: "worker-a"}, nil
		}),
		WriteTimeout: time.Second,
		PingInterval: time.Hour,
		PingTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), service)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, httptest.NewServer(transport)
}

func openNetworkFaultWorker(t *testing.T, gatewayURL, journalPath string) *workerws.Client {
	t.Helper()
	journal, err := workersqlite.Open(t.Context(), workersqlite.Config{Path: journalPath})
	if err != nil {
		t.Fatal(err)
	}
	client, err := workerws.NewClient(t.Context(), workerws.ClientConfig{
		URL:                 "ws" + strings.TrimPrefix(gatewayURL, "http"),
		Gateway:             "gateway-a",
		Worker:              "worker-a",
		Journal:             journal,
		ConnectTimeout:      time.Second,
		WriteTimeout:        time.Second,
		ReconnectMinDelay:   time.Millisecond,
		ReconnectMaxDelay:   10 * time.Millisecond,
		ReconnectResetAfter: time.Second,
		ReleaseTimeout:      time.Second,
		NotificationBuffer:  8,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newNetworkFaultRequest(t *testing.T, serverURL string, body []byte) *http.Request {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, serverURL+"/v1/chat/completions", strings.NewReader(string(body)),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "request-a")
	return request
}

func doNetworkFaultRequest(t *testing.T, serverURL string, body []byte) string {
	t.Helper()
	response, err := http.DefaultClient.Do(newNetworkFaultRequest(t, serverURL, body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("HTTP retry status = %d body=%q", response.StatusCode, payload)
	}
	return string(payload)
}

func receiveNetworkFaultAssignment(
	t *testing.T,
	deliveries <-chan llm.WorkerAssignmentDelivery,
) llm.WorkerAssignmentDelivery {
	t.Helper()
	select {
	case delivery, open := <-deliveries:
		if !open {
			t.Fatal("worker stopped before presenting assignment")
		}
		return delivery
	case <-time.After(3 * time.Second):
		t.Fatal("worker assignment was not presented")
		return llm.WorkerAssignmentDelivery{}
	}
}

func shutdownNetworkFaultRuntime(t *testing.T, runtime framework.Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown %T: %v", runtime, err)
	}
}

func registerNetworkFaultCleanup(
	t *testing.T,
	service *llm.Service,
	callerRuntime llm.CallerTransportRuntime,
	callerServer *httptest.Server,
	workerRuntime llm.WorkerTransportRuntime,
	workerServer *httptest.Server,
	client *workerws.Client,
) {
	t.Helper()
	t.Cleanup(func() {
		shutdownNetworkFaultRuntime(t, client)
		shutdownNetworkFaultRuntime(t, workerRuntime)
		workerServer.Close()
		shutdownNetworkFaultRuntime(t, callerRuntime)
		callerServer.Close()
		shutdownNetworkFaultRuntime(t, service)
	})
}

type networkFaultCodec struct{}

func (networkFaultCodec) Description() llm.CodecDescription {
	return llm.CodecDescription{
		Contract:    framework.Contract{ID: llm.CodecContractID, Major: llm.CodecContractMajor},
		ID:          networkFaultCodecID,
		Version:     "1",
		Fingerprint: llm.Fingerprint([]byte("network-fault-codec-v1")),
		Limits: llm.CodecLimits{
			MaxRequestBytes: 1 << 20, MaxStreamFrameBytes: 1 << 20,
			MaxStreamFramesPerStep: 4, MaxAggregateBytes: 1 << 20,
			MaxAdmissionErrorBytes: 1 << 20,
		},
		OverloadedStatus: http.StatusServiceUnavailable,
	}
}

func (networkFaultCodec) Decode(body []byte) (llm.Request, error) {
	var request llm.Request
	if err := json.Unmarshal(body, &request); err != nil {
		return llm.Request{}, err
	}
	return request, nil
}

func (networkFaultCodec) NewStream(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &networkFaultEncoder{stream: true}, nil
}

func (networkFaultCodec) NewAggregate(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &networkFaultEncoder{}, nil
}

func (networkFaultCodec) AdmissionError(failure llm.AdmissionFailure) ([]byte, error) {
	return json.Marshal(failure)
}

type networkFaultEncoder struct {
	stream bool
	events []llm.Event
}

func (encoder *networkFaultEncoder) Start() ([][]byte, error) {
	if encoder.stream {
		return [][]byte{[]byte("start\n")}, nil
	}
	return nil, nil
}

func (encoder *networkFaultEncoder) Encode(event llm.Event, _ llm.EventSeed) ([][]byte, bool, error) {
	encoder.events = append(encoder.events, event)
	if encoder.stream {
		return [][]byte{[]byte(fmt.Sprintf("%s:%s\n", event.Type, event.Text))}, event.EndsResponse(), nil
	}
	if !event.EndsResponse() {
		return nil, false, nil
	}
	body, err := json.Marshal(encoder.events)
	return [][]byte{body}, true, err
}
