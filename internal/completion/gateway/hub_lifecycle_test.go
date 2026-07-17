package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/hub"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/store/sqlite"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerws"
)

type timeoutE2EWorkerAuthenticator struct {
	connections atomic.Int64
}

func (authenticator *timeoutE2EWorkerAuthenticator) AuthenticateRequest(
	request *http.Request,
) (auth.Principal, error) {
	token := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	if token != "hae_timeout_worker" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	authenticator.connections.Add(1)
	return auth.Principal{
		Type: auth.PrincipalWorker, SubjectID: "worker-timeout-e2e", KeyID: "key-timeout-e2e",
	}, nil
}

func TestExpiryRetiresLiveHubSession(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "expiry-retirement.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	workerHub := hub.New(1)
	worker, err := workerHub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour,
		MaxPending:        20 * time.Millisecond,
	}, database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(gateway)
	defer httpServer.Close()

	result := startEventRequest(
		t, context.Background(), httpServer.URL, "/v1/chat/completions",
		chatBody("let the session expire", false), "expiry-retirement",
	)
	_ = waitAssignment(t, worker)
	response := waitEventResponse(t, result)
	if response.err != nil || !bytes.Contains(response.body, []byte("human_timeout")) {
		t.Fatalf("expiry response = %d, %q, %v", response.status, response.body, response.err)
	}
	var reservation *hub.Reservation
	deadline := time.Now().Add(time.Second)
	for reservation == nil {
		reservation, err = workerHub.Reserve("worker")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expiry did not release hub capacity: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	reservation.Release()
}

func TestGatewayExpiryRejectsLateWorkerEventWithoutPoisoningConnection(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "timeout-poison-e2e.db"))
	if err != nil {
		t.Fatal(err)
	}
	workerHub := hub.New(2)
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour,
		MaxPending:        250 * time.Millisecond,
	}, database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	if err := gateway.Recover(runContext); err != nil {
		cancelRun()
		_ = database.Close()
		t.Fatal(err)
	}
	gatewayHTTP := httptest.NewServer(gateway)
	workerAuthenticator := &timeoutE2EWorkerAuthenticator{}
	workerServer, err := workerws.New(
		workerws.Config{PingInterval: time.Hour},
		workerAuthenticator, workerHub, database,
	)
	if err != nil {
		gatewayHTTP.Close()
		cancelRun()
		gateway.Wait()
		_ = database.Close()
		t.Fatal(err)
	}
	workerHTTP := httptest.NewServer(workerServer)
	testContext, cancelTest := context.WithTimeout(context.Background(), 8*time.Second)
	client, err := workerclient.DialWithOutbox(
		testContext,
		"ws"+strings.TrimPrefix(workerHTTP.URL, "http"),
		"hae_timeout_worker",
		filepath.Join(t.TempDir(), "timeout-worker-outbox.db"),
	)
	if err != nil {
		cancelTest()
		workerHTTP.Close()
		gatewayHTTP.Close()
		cancelRun()
		gateway.Wait()
		_ = database.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		cancelTest()
		workerHTTP.Close()
		gatewayHTTP.Close()
		cancelRun()
		gateway.Wait()
		_ = database.Close()
	})

	for client.WorkerID() != "worker-timeout-e2e" {
		select {
		case message, open := <-client.Messages():
			if !open {
				t.Fatal("worker client closed before hello")
			}
			if message.Err != nil {
				t.Fatalf("worker client failed before hello: %v", message.Err)
			}
		case <-time.After(time.Millisecond):
		case <-testContext.Done():
			t.Fatal("worker client did not establish its authenticated identity")
		}
	}

	expiredBody := chatBody("expire at the real gateway boundary", false)
	expiredResult := startEventRequest(
		t, testContext, gatewayHTTP.URL, "/v1/chat/completions",
		expiredBody, "timeout-poison-expired",
	)
	expiredAssignment := waitWorkerClientAssignment(t, testContext, client.Messages())
	expiredResponse := waitEventResponse(t, expiredResult)
	if expiredResponse.err != nil || expiredResponse.status != http.StatusOK ||
		!bytes.Contains(expiredResponse.body, []byte("human_timeout")) ||
		!bytes.Contains(expiredResponse.body, []byte("data: [DONE]")) {
		t.Fatalf(
			"gateway expiry response = %d, %q, %v",
			expiredResponse.status, expiredResponse.body, expiredResponse.err,
		)
	}
	expiredKey := storeapi.RequestKey{
		CallerID: expiredAssignment.CallerID, IdempotencyKey: expiredAssignment.IdempotencyKey,
	}
	lookup, err := database.LookupRequest(
		testContext, expiredKey, mustRequestDigest(t, expiredBody),
	)
	if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateExpired {
		t.Fatalf("durable gateway expiry = %+v, %v", lookup, err)
	}
	expiredEvents, err := database.ListResponseEvents(testContext, expiredKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	var persistedExpiry completion.Event
	for _, stored := range expiredEvents {
		if stored.Kind != responseEventStep {
			continue
		}
		var step persistedStep
		if err := json.Unmarshal(stored.Data, &step); err != nil {
			t.Fatal(err)
		}
		if step.Event.Type == completion.EventExpired {
			persistedExpiry = step.Event
		}
	}
	if persistedExpiry.ID == "" {
		t.Fatalf("gateway expiry has no persisted EventExpired step: %+v", expiredEvents)
	}
	expiryDigest, err := workerEventDigest(persistedExpiry)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := database.LookupWorkerEventReceipt(testContext, expiredKey, persistedExpiry.ID)
	if err != nil || receipt.Digest != expiryDigest {
		t.Fatalf("gateway expiry has no durable terminal receipt: %+v, %v", receipt, err)
	}

	const lateEventID = "late-after-gateway-timeout"
	if err := client.SendEvent(testContext, expiredAssignment, completion.Event{
		ID: lateEventID, Type: completion.EventFinal, Text: "too late",
	}); err != nil {
		t.Fatal(err)
	}
	liveBody := chatBody("prove the same worker connection still works", false)
	liveResult := startEventRequest(
		t, testContext, gatewayHTTP.URL, "/v1/chat/completions",
		liveBody, "timeout-poison-live",
	)

	rejectionSeen := false
	liveSent := false
	var liveResponse eventResponse
	liveResponseSeen := false
	for !rejectionSeen || !liveResponseSeen {
		select {
		case message, open := <-client.Messages():
			if !open {
				t.Fatal("worker client closed while resolving the late event")
			}
			if message.Err != nil {
				t.Fatalf("late business event interrupted the worker connection: %v", message.Err)
			}
			if message.ConnectionRestored {
				t.Fatal("late business event forced a worker reconnect")
			}
			if message.EventRejected != nil {
				rejected := message.EventRejected
				if rejected.CallerID != expiredAssignment.CallerID ||
					rejected.IdempotencyKey != expiredAssignment.IdempotencyKey ||
					rejected.EventID != lateEventID {
					t.Fatalf("late event rejection = %+v", rejected)
				}
				if message.RejectedEvent == nil || message.RejectedEvent.ID != lateEventID ||
					message.RejectedEvent.Type != completion.EventFinal ||
					message.RejectedEvent.Text != "too late" {
					t.Fatalf("late durable event body = %+v", message.RejectedEvent)
				}
				rejectionSeen = true
			}
			if message.Assignment != nil {
				assignment := *message.Assignment
				if assignment.IdempotencyKey != "timeout-poison-live" {
					t.Fatalf("unexpected assignment while testing timeout recovery: %+v", assignment)
				}
				if liveSent {
					t.Fatalf("live assignment was delivered more than once: %+v", assignment)
				}
				liveSent = true
				if err := client.SendEvent(testContext, assignment, completion.Event{
					ID: "timeout-live-accepted", Type: completion.EventAccepted,
				}); err != nil {
					t.Fatal(err)
				}
				if err := client.SendEvent(testContext, assignment, completion.Event{
					ID: "timeout-live-final", Type: completion.EventFinal, Text: "live response",
				}); err != nil {
					t.Fatal(err)
				}
			}
		case liveResponse = <-liveResult:
			liveResponseSeen = true
		case <-testContext.Done():
			t.Fatal("gateway timeout poison E2E did not finish")
		}
	}
	if !liveSent || liveResponse.err != nil || liveResponse.status != http.StatusOK ||
		!bytes.Contains(liveResponse.body, []byte("live response")) ||
		!bytes.Contains(liveResponse.body, []byte("data: [DONE]")) {
		t.Fatalf("post-timeout live response = %d, %q, %v", liveResponse.status, liveResponse.body, liveResponse.err)
	}
	if got := workerAuthenticator.connections.Load(); got != 1 {
		t.Fatalf("late event caused %d worker WebSocket handshakes, want exactly 1", got)
	}

	// A rejected late event must never be mistaken for a persisted worker step.
	expiredEvents, err = database.ListResponseEvents(testContext, expiredKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, stored := range expiredEvents {
		if stored.Kind != responseEventStep {
			continue
		}
		var step persistedStep
		if err := json.Unmarshal(stored.Data, &step); err != nil {
			t.Fatal(err)
		}
		if step.Event.ID == lateEventID {
			t.Fatalf("rejected late event was persisted as applied work: %+v", step.Event)
		}
	}
}

func waitWorkerClientAssignment(
	t *testing.T,
	ctx context.Context,
	messages <-chan workerclient.Message,
) completion.Assignment {
	t.Helper()
	for {
		select {
		case message, open := <-messages:
			if !open {
				t.Fatal("worker client closed before receiving an assignment")
			}
			if message.Err != nil {
				t.Fatalf("worker client failed before receiving an assignment: %v", message.Err)
			}
			if message.ConnectionRestored {
				t.Fatal("worker client reconnected before receiving an assignment")
			}
			if message.Assignment != nil {
				return *message.Assignment
			}
		case <-ctx.Done():
			t.Fatal("worker client did not receive an assignment")
			return completion.Assignment{}
		}
	}
}

func TestRuntimeConsumerExitRetiresLiveHubSession(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "consumer-exit.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	workerHub := hub.New(1)
	worker, err := workerHub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour,
		MaxPending:        time.Hour,
	}, database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	if err := gateway.Recover(runContext); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(gateway)
	defer httpServer.Close()

	clientContext, cancelClient := context.WithCancel(context.Background())
	result := startEventRequest(
		t, clientContext, httpServer.URL, "/v1/chat/completions",
		chatBody("cancel the runtime", false), "runtime-consumer-exit",
	)
	_ = waitAssignment(t, worker)
	cancelRun()

	// consumeSession used to return without retiring its live Hub session,
	// leaking the only capacity slot. Runtime exit now owns fail-closed abort.
	var reservation *hub.Reservation
	deadline := time.Now().Add(time.Second)
	for reservation == nil {
		reservation, err = workerHub.Reserve("worker")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer exit did not release hub capacity: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	reservation.Release()
	cancelClient()
	_ = waitEventResponse(t, result)
}
