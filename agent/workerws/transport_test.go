package workerws

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

func TestTransportBindsAuthenticationAndSettlesDeliveries(t *testing.T) {
	endpoint := newFakeEndpoint()
	transport, err := New(Config{
		GatewayID: "gateway-a",
		Authenticator: AuthenticateFunc(func(context.Context, *http.Request) (Identity, error) {
			return Identity{Authority: "tenant-a", Worker: "worker-a"}, nil
		}),
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown transport: %v", err)
		}
	})

	server := httptest.NewServer(transport)
	t.Cleanup(server.Close)
	connection := dialWorker(t, server.URL, "session-a")
	defer connection.CloseNow()

	helloMessage := readEnvelope(t, connection)
	if helloMessage.Type != messageHello {
		t.Fatalf("first message type = %q", helloMessage.Type)
	}
	gotHello, err := decodePayload[hello](helloMessage)
	if err != nil {
		t.Fatal(err)
	}
	if gotHello.Gateway != "gateway-a" || gotHello.Authority != "tenant-a" ||
		gotHello.Worker != "worker-a" || gotHello.Session != "session-a" {
		t.Fatalf("hello = %#v", gotHello)
	}

	core := endpoint.connection(t)
	delivery := validAssignment("assignment-1")
	core.assignments <- delivery
	message := readEnvelope(t, connection)
	if message.Type != messageAssignment {
		t.Fatalf("assignment message type = %q", message.Type)
	}
	gotAssignment, err := decodePayload[agent.WorkerAssignmentDelivery](message)
	if err != nil || gotAssignment.ID != delivery.ID {
		t.Fatalf("assignment = %#v, %v", gotAssignment, err)
	}
	writeEnvelope(t, connection, messageAssignmentACK, delivery.ID)
	select {
	case got := <-core.assignmentACKs:
		if got != delivery.ID {
			t.Fatalf("assignment ACK = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("assignment ACK did not reach endpoint")
	}

	event := validEvent("event-delivery-1", "event-1", delivery.Assignment)
	writeEnvelope(t, connection, messageEvent, event)
	receiptMessage := readEnvelope(t, connection)
	if receiptMessage.Type != messageEventReceipt {
		t.Fatalf("receipt message type = %q", receiptMessage.Type)
	}
	receipt, err := decodePayload[agent.WorkerEventReceipt](receiptMessage)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Decision != agent.WorkerEventACK || receipt.Delivery != event.ID || receipt.Event != event.Event.ID {
		t.Fatalf("receipt = %#v", receipt)
	}
	if committed := <-core.events; committed.Event.Task.Workspace.Authority != "tenant-a" {
		t.Fatalf("committed event escaped authenticated authority: %#v", committed)
	}
}

func TestTransportClassifiesAuthenticationFaultsWithoutLeakingCause(t *testing.T) {
	secret := errors.New("secret Agent identity-provider detail")
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{
			name: "unauthenticated",
			err: framework.NewFault(
				framework.CodeUnauthenticated, framework.RetryNever, "do not expose", secret,
			),
			status: http.StatusUnauthorized,
		},
		{
			name: "forbidden",
			err: framework.NewFault(
				framework.CodeForbidden, framework.RetryNever, "do not expose", secret,
			),
			status: http.StatusForbidden,
		},
		{
			name: "provider unavailable",
			err: framework.NewFault(
				framework.CodeUnavailable, framework.RetryBackoff, "do not expose", secret,
			),
			status: http.StatusServiceUnavailable,
		},
		{name: "unclassified infrastructure error", err: secret, status: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport, err := New(Config{
				GatewayID: "gateway-a",
				Authenticator: AuthenticateFunc(func(context.Context, *http.Request) (Identity, error) {
					return Identity{}, test.err
				}),
				PingInterval: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := transport.Start(t.Context(), newFakeEndpoint())
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Shutdown(context.Background())
			server := httptest.NewServer(transport)
			defer server.Close()

			header := http.Header{}
			header.Set(SessionHeader, "session-auth")
			connection, response, dialErr := websocket.Dial(
				t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"),
				&websocket.DialOptions{HTTPHeader: header},
			)
			if connection != nil {
				connection.CloseNow()
			}
			if dialErr == nil || response == nil {
				t.Fatalf("authentication dial = connection=%v response=%v err=%v", connection, response, dialErr)
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if response.StatusCode != test.status {
				t.Fatalf("authentication status = %d, want %d", response.StatusCode, test.status)
			}
			if bytes.Contains(body, []byte(secret.Error())) || bytes.Contains(body, []byte("do not expose")) {
				t.Fatalf("authentication response leaked private diagnostics: %q", body)
			}
		})
	}
}

func TestTransportShutdownCancelsAdmittedAuthenticationAndDrainsHandler(t *testing.T) {
	entered := make(chan struct{})
	var enteredOnce sync.Once
	transport, err := New(Config{
		GatewayID: "gateway-a",
		Authenticator: AuthenticateFunc(func(ctx context.Context, _ *http.Request) (Identity, error) {
			enteredOnce.Do(func() { close(entered) })
			<-ctx.Done()
			return Identity{}, ctx.Err()
		}),
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), newFakeEndpoint())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(transport)
	defer server.Close()
	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := server.Client().Get(server.URL)
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	waitSignal(t, entered, "blocking authenticator")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown with admitted authenticator: %v", err)
	}
	waitSignal(t, runtime.Done(), "transport with admitted authenticator")
	select {
	case <-requestDone:
	case <-time.After(3 * time.Second):
		t.Fatal("authentication handler survived transport shutdown")
	}
}

func TestTransportNacksMalformedEventWithoutCallingEndpoint(t *testing.T) {
	endpoint := newFakeEndpoint()
	transport, err := New(Config{
		GatewayID: "gateway-a",
		Authenticator: AuthenticateFunc(func(context.Context, *http.Request) (Identity, error) {
			return Identity{Authority: "tenant-a", Worker: "worker-a"}, nil
		}),
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	server := httptest.NewServer(transport)
	defer server.Close()
	connection := dialWorker(t, server.URL, "session-malformed")
	defer connection.CloseNow()
	_ = readEnvelope(t, connection)
	core := endpoint.connection(t)

	malformed := agent.WorkerEventDelivery{
		ID: "delivery-malformed",
		Event: agent.WorkerEvent{
			ID: "event-malformed", Kind: "launch_missiles",
			Task: validTaskRef(), Fence: 1, ExpectedRevision: 1,
		},
	}
	writeEnvelope(t, connection, messageEvent, malformed)
	receiptMessage := readEnvelope(t, connection)
	receipt, err := decodePayload[agent.WorkerEventReceipt](receiptMessage)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Decision != agent.WorkerEventNACK || receipt.Code != agent.WorkerRejectInvalid {
		t.Fatalf("malformed receipt = %#v", receipt)
	}
	select {
	case called := <-core.events:
		t.Fatalf("malformed event reached endpoint: %#v", called)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestTransportRejectsTypedNilAuthenticatorAndUnknownWireFields(t *testing.T) {
	var typedNil AuthenticateFunc
	if _, err := New(Config{GatewayID: "gateway-a", Authenticator: typedNil}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("typed nil authenticator error = %v", err)
	}

	endpoint := newFakeEndpoint()
	transport, err := New(Config{
		GatewayID: "gateway-a",
		Authenticator: AuthenticateFunc(func(context.Context, *http.Request) (Identity, error) {
			return Identity{Authority: "tenant-a", Worker: "worker-a"}, nil
		}),
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	server := httptest.NewServer(transport)
	defer server.Close()
	connection := dialWorker(t, server.URL, "session-strict-json")
	defer connection.CloseNow()
	_ = readEnvelope(t, connection)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	unknown := []byte(`{"version":"1","type":"assignment_ack","payload":"assignment-x","future":true}`)
	if err := connection.Write(ctx, websocket.MessageText, unknown); err != nil {
		t.Fatalf("write unknown wire field: %v", err)
	}
	var message envelope
	if err := wsjson.Read(ctx, connection, &message); err == nil {
		t.Fatalf("unknown wire field left connection open: %#v", message)
	}
}

func TestTransportLeavesUnsettledEventForReconnect(t *testing.T) {
	endpoint := newFakeEndpoint()
	endpoint.commitError = agent.ErrWorkerDeliveryIndeterminate
	transport, err := New(Config{
		GatewayID: "gateway-a",
		Authenticator: AuthenticateFunc(func(context.Context, *http.Request) (Identity, error) {
			return Identity{Authority: "tenant-a", Worker: "worker-a"}, nil
		}),
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	server := httptest.NewServer(transport)
	defer server.Close()

	connection := dialWorker(t, server.URL, "session-indeterminate")
	_ = readEnvelope(t, connection)
	event := validEvent("delivery-retry", "event-retry", validAssignment("assignment-retry").Assignment)
	writeEnvelope(t, connection, messageEvent, event)
	readCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var message envelope
	err = wsjson.Read(readCtx, connection, &message)
	if err == nil {
		t.Fatalf("unsettled event received a terminal wire message: %#v", message)
	}
	connection.CloseNow()

	endpoint.mu.Lock()
	endpoint.commitError = nil
	endpoint.mu.Unlock()
	reconnected := dialWorker(t, server.URL, "session-retry")
	defer reconnected.CloseNow()
	_ = readEnvelope(t, reconnected)
	writeEnvelope(t, reconnected, messageEvent, event)
	receiptMessage := readEnvelope(t, reconnected)
	receipt, err := decodePayload[agent.WorkerEventReceipt](receiptMessage)
	if err != nil || receipt.Decision != agent.WorkerEventACK {
		t.Fatalf("retry receipt = %#v, %v", receipt, err)
	}
}

func dialWorker(t *testing.T, serverURL, session string) *websocket.Conn {
	t.Helper()
	header := http.Header{}
	header.Set(SessionHeader, session)
	connection, response, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(serverURL, "http"), &websocket.DialOptions{
		HTTPHeader: header,
	})
	if err != nil {
		if response != nil {
			t.Fatalf("dial worker: %v (HTTP %d)", err, response.StatusCode)
		}
		t.Fatalf("dial worker: %v", err)
	}
	return connection
}

func readEnvelope(t *testing.T, connection *websocket.Conn) envelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var message envelope
	if err := wsjson.Read(ctx, connection, &message); err != nil {
		t.Fatalf("read worker message: %v", err)
	}
	return message
}

func writeEnvelope(t *testing.T, connection *websocket.Conn, kind messageType, payload any) {
	t.Helper()
	message, err := newEnvelope(kind, payload)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, connection, message); err != nil {
		t.Fatalf("write worker message: %v", err)
	}
}

func waitSignal(t *testing.T, done <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s did not stop", name)
	}
}

func validTaskRef() agent.TaskRef {
	return agent.TaskRef{
		Workspace: agent.WorkspaceRef{Authority: "tenant-a", ID: "workspace-a"},
		ID:        "task-a",
	}
}

func validAssignment(id agent.WorkerDeliveryID) agent.WorkerAssignmentDelivery {
	now := time.Unix(1_750_000_000, 0).UTC()
	ref := validTaskRef()
	grant := agent.LeaseGrant{Task: ref, Worker: "worker-a", Fence: 1}
	return agent.WorkerAssignmentDelivery{
		ID: id,
		Assignment: agent.LeaseAssignment{
			Grant: grant,
			Task: agent.Task{
				Ref: ref, Context: agent.ContextRef{Authority: "tenant-a", ID: "context-a"},
				State: agent.TaskSubmitted, Revision: 1, MessageCount: 1, EventCount: 1,
				CreatedAt: now, UpdatedAt: now,
			},
			GrantedAt: now,
		},
	}
}

func validEvent(deliveryID agent.WorkerDeliveryID, eventID agent.CommandID, assignment agent.LeaseAssignment) agent.WorkerEventDelivery {
	return agent.WorkerEventDelivery{
		ID: deliveryID,
		Event: agent.WorkerEvent{
			ID: eventID, Kind: agent.WorkerEventAcceptTask,
			Task: assignment.Grant.Task, Fence: assignment.Grant.Fence,
			ExpectedRevision: assignment.Task.Revision,
		},
	}
}

type fakeEndpoint struct {
	mu          sync.Mutex
	connections chan *fakeConnection
	commitError error
}

func newFakeEndpoint() *fakeEndpoint {
	return &fakeEndpoint{connections: make(chan *fakeConnection, 8)}
}

func (endpoint *fakeEndpoint) OpenWorker(_ context.Context, principal agent.AuthenticatedWorker) (agent.WorkerConnection, error) {
	connection := &fakeConnection{
		principal:      principal,
		assignments:    make(chan agent.WorkerAssignmentDelivery, 8),
		assignmentACKs: make(chan agent.WorkerDeliveryID, 8),
		events:         make(chan agent.WorkerEventDelivery, 8),
		done:           make(chan struct{}),
		endpoint:       endpoint,
	}
	endpoint.connections <- connection
	return connection, nil
}

func (endpoint *fakeEndpoint) connection(t *testing.T) *fakeConnection {
	t.Helper()
	select {
	case connection := <-endpoint.connections:
		return connection
	case <-time.After(2 * time.Second):
		t.Fatal("endpoint did not open worker connection")
		return nil
	}
}

type fakeConnection struct {
	principal      agent.AuthenticatedWorker
	assignments    chan agent.WorkerAssignmentDelivery
	assignmentACKs chan agent.WorkerDeliveryID
	events         chan agent.WorkerEventDelivery
	done           chan struct{}
	endpoint       *fakeEndpoint
	closeOnce      sync.Once
}

func (connection *fakeConnection) Principal() agent.AuthenticatedWorker { return connection.principal }
func (connection *fakeConnection) Assignments() <-chan agent.WorkerAssignmentDelivery {
	return connection.assignments
}
func (connection *fakeConnection) AckAssignment(_ context.Context, id agent.WorkerDeliveryID) error {
	connection.assignmentACKs <- id
	return nil
}
func (connection *fakeConnection) CommitEvent(_ context.Context, delivery agent.WorkerEventDelivery) (agent.WorkerEventReceipt, error) {
	connection.events <- delivery
	connection.endpoint.mu.Lock()
	err := connection.endpoint.commitError
	connection.endpoint.mu.Unlock()
	if err != nil {
		return agent.WorkerEventReceipt{}, err
	}
	return agent.WorkerEventReceipt{
		Delivery: delivery.ID, Event: delivery.Event.ID, Decision: agent.WorkerEventACK,
	}, nil
}
func (connection *fakeConnection) Done() <-chan struct{} { return connection.done }
func (*fakeConnection) Err() error                       { return nil }
func (connection *fakeConnection) Shutdown(context.Context) error {
	connection.closeOnce.Do(func() { close(connection.done) })
	return nil
}

var _ agent.WorkerEndpoint = (*fakeEndpoint)(nil)
var _ agent.WorkerConnection = (*fakeConnection)(nil)
