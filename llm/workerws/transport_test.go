package workerws

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

func TestTransportAuthenticatesIdentityAndUsesExactSettlementBoundaries(t *testing.T) {
	endpoint := newFakeEndpoint()
	transport, running, server := startTestTransport(t, endpoint, Config{
		GatewayID: "gateway-a",
		Authenticator: AuthenticateFunc(func(_ context.Context, request *http.Request) (Identity, error) {
			if request.Header.Get("Authorization") != "Bearer worker-secret" {
				return Identity{}, framework.NewFault(
					framework.CodeUnauthenticated, framework.RetryNever, "denied", nil,
				)
			}
			return Identity{Worker: "worker-a"}, nil
		}),
	})
	_ = transport
	_ = running

	unauthorized, response, err := dial(server.URL, "session-unauthorized", "")
	if unauthorized != nil {
		unauthorized.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized dial = connection=%v response=%v err=%v", unauthorized, response, err)
	}

	connection := dialTestWorker(t, server.URL, "session-a", "worker-secret")
	defer connection.CloseNow()
	helloMessage := readEnvelope(t, connection)
	if helloMessage.Type != messageHello {
		t.Fatalf("first message type = %q", helloMessage.Type)
	}
	greeting, err := decodePayload[hello](helloMessage)
	if err != nil {
		t.Fatal(err)
	}
	if greeting.Gateway != "gateway-a" || greeting.Worker != "worker-a" || greeting.Session != "session-a" {
		t.Fatalf("hello = %#v", greeting)
	}

	core := endpoint.nextConnection(t)
	if got, want := core.Principal(), (llm.AuthenticatedWorker{WorkerID: "worker-a", SessionID: "session-a"}); got != want {
		t.Fatalf("endpoint principal = %#v, want %#v", got, want)
	}

	assignment := validAssignment("one", true)
	core.assignments <- assignment
	wireAssignment := readEnvelope(t, connection)
	if wireAssignment.Type != messageAssignment {
		t.Fatalf("assignment message type = %q", wireAssignment.Type)
	}
	gotAssignment, err := decodePayload[llm.WorkerAssignmentDelivery](wireAssignment)
	if err != nil || gotAssignment.ID != assignment.ID {
		t.Fatalf("assignment = %#v, %v", gotAssignment, err)
	}
	select {
	case early := <-endpoint.assignmentACKs:
		t.Fatalf("writing assignment frame acknowledged it early: %q", early)
	case <-time.After(75 * time.Millisecond):
	}
	writeEnvelope(t, connection, messageAssignmentACK, assignment.ID)
	select {
	case got := <-endpoint.assignmentACKs:
		if got != assignment.ID {
			t.Fatalf("assignment ACK = %q, want %q", got, assignment.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("assignment ACK did not reach endpoint")
	}

	event := validEvent("healthy", assignment.Assignment, llm.EventProgress)
	writeEnvelope(t, connection, messageEvent, event)
	receiptMessage := readEnvelope(t, connection)
	receipt, err := decodePayload[llm.WorkerEventReceipt](receiptMessage)
	if err != nil {
		t.Fatal(err)
	}
	if receiptMessage.Type != messageEventReceipt || receipt.Decision != llm.WorkerEventACK ||
		receipt.Delivery != event.ID || receipt.EventID != event.Event.ID {
		t.Fatalf("event receipt = %#v (%q)", receipt, receiptMessage.Type)
	}
	if committed := receiveEvent(t, endpoint.events); committed.ID != event.ID {
		t.Fatalf("committed event = %#v", committed)
	}

	endpoint.setCommit(func(_ context.Context, delivery llm.WorkerEventDelivery) (llm.WorkerEventReceipt, error) {
		return llm.WorkerEventReceipt{
			Delivery: delivery.ID,
			EventID:  delivery.Event.ID,
			Decision: llm.WorkerEventNACK,
			Code:     llm.WorkerRejectStateConflict,
			Message:  "response state changed",
		}, nil
	})
	rejected := validEvent("rejected", assignment.Assignment, llm.EventProgress)
	writeEnvelope(t, connection, messageEvent, rejected)
	nack, err := decodePayload[llm.WorkerEventReceipt](readEnvelope(t, connection))
	if err != nil {
		t.Fatal(err)
	}
	if nack.Decision != llm.WorkerEventNACK || nack.Code != llm.WorkerRejectStateConflict ||
		nack.Delivery != rejected.ID || nack.EventID != rejected.Event.ID {
		t.Fatalf("endpoint NACK changed on wire: %#v", nack)
	}
}

func TestTransportClassifiesAuthenticationFaultsWithoutLeakingCause(t *testing.T) {
	secret := errors.New("secret worker identity-provider detail")
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
			_, _, server := startTestTransport(t, newFakeEndpoint(), Config{
				GatewayID: "gateway-a",
				Authenticator: AuthenticateFunc(func(context.Context, *http.Request) (Identity, error) {
					return Identity{}, test.err
				}),
				PingInterval: time.Hour,
			})
			connection, response, err := dial(server.URL, "session-auth", "")
			if connection != nil {
				connection.CloseNow()
			}
			if err == nil || response == nil {
				t.Fatalf("authentication dial = connection=%v response=%v err=%v", connection, response, err)
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

func TestTransportSettlesShapePoisonWithoutBlockingFollower(t *testing.T) {
	endpoint := newFakeEndpoint()
	_, _, server := startTestTransport(t, endpoint, defaultTestConfig())
	connection := dialTestWorker(t, server.URL, "session-poison", "")
	defer connection.CloseNow()
	_ = readEnvelope(t, connection)
	_ = endpoint.nextConnection(t)

	assignment := validAssignment("poison", true).Assignment
	poison := validEvent("poison", assignment, llm.EventProgress)
	poison.Event.Type = "future_event"
	writeEnvelope(t, connection, messageEvent, poison)
	receipt, err := decodePayload[llm.WorkerEventReceipt](readEnvelope(t, connection))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Decision != llm.WorkerEventNACK || receipt.Code != llm.WorkerRejectInvalid ||
		receipt.Delivery != poison.ID || receipt.EventID != poison.Event.ID {
		t.Fatalf("poison receipt = %#v", receipt)
	}
	select {
	case reached := <-endpoint.events:
		t.Fatalf("shape-invalid event reached endpoint: %#v", reached)
	case <-time.After(75 * time.Millisecond):
	}

	healthy := validEvent("after-poison", assignment, llm.EventProgress)
	writeEnvelope(t, connection, messageEvent, healthy)
	follower, err := decodePayload[llm.WorkerEventReceipt](readEnvelope(t, connection))
	if err != nil || follower.Decision != llm.WorkerEventACK || follower.Delivery != healthy.ID {
		t.Fatalf("poison blocked healthy follower: receipt=%#v err=%v", follower, err)
	}
}

func TestTransportDisconnectBeforeAssignmentACKPermitsExactRedelivery(t *testing.T) {
	endpoint := newFakeEndpoint()
	_, _, server := startTestTransport(t, endpoint, defaultTestConfig())
	delivery := validAssignment("redelivery", true)

	firstWire := dialTestWorker(t, server.URL, "session-first", "")
	_ = readEnvelope(t, firstWire)
	firstCore := endpoint.nextConnection(t)
	firstCore.assignments <- delivery
	firstDelivery, err := decodePayload[llm.WorkerAssignmentDelivery](readEnvelope(t, firstWire))
	if err != nil || firstDelivery.ID != delivery.ID {
		t.Fatalf("first delivery = %#v, %v", firstDelivery, err)
	}
	firstWire.CloseNow()
	waitDone(t, firstCore.Done(), "first endpoint connection")
	select {
	case early := <-endpoint.assignmentACKs:
		t.Fatalf("disconnect acknowledged unconfirmed assignment: %q", early)
	default:
	}

	secondWire := dialTestWorker(t, server.URL, "session-second", "")
	defer secondWire.CloseNow()
	_ = readEnvelope(t, secondWire)
	secondCore := endpoint.nextConnection(t)
	secondCore.assignments <- llm.CloneWorkerAssignmentDelivery(delivery)
	replayed, err := decodePayload[llm.WorkerAssignmentDelivery](readEnvelope(t, secondWire))
	if err != nil || replayed.ID != delivery.ID {
		t.Fatalf("replayed assignment = %#v, %v", replayed, err)
	}
	firstJSON, _ := json.Marshal(firstDelivery)
	secondJSON, _ := json.Marshal(replayed)
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("assignment replay changed bytes:\nfirst  %s\nsecond %s", firstJSON, secondJSON)
	}
	writeEnvelope(t, secondWire, messageAssignmentACK, replayed.ID)
	select {
	case got := <-endpoint.assignmentACKs:
		if got != replayed.ID {
			t.Fatalf("replay ACK = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replayed assignment was not acknowledged")
	}
}

func TestTransportLeavesUnsettledEventForExactReconnectRetry(t *testing.T) {
	endpoint := newFakeEndpoint()
	var attempts int
	endpoint.setCommit(func(_ context.Context, delivery llm.WorkerEventDelivery) (llm.WorkerEventReceipt, error) {
		attempts++
		if attempts == 1 {
			return llm.WorkerEventReceipt{}, llm.ErrWorkerDeliveryIndeterminate
		}
		return ackReceipt(delivery), nil
	})
	_, _, server := startTestTransport(t, endpoint, defaultTestConfig())
	delivery := validEvent("commit-unknown", validAssignment("commit-unknown", true).Assignment, llm.EventProgress)

	first := dialTestWorker(t, server.URL, "session-first", "")
	_ = readEnvelope(t, first)
	firstCore := endpoint.nextConnection(t)
	writeEnvelope(t, first, messageEvent, delivery)
	assertConnectionClosedWithoutEnvelope(t, first)
	waitDone(t, firstCore.Done(), "first endpoint connection")

	second := dialTestWorker(t, server.URL, "session-second", "")
	defer second.CloseNow()
	_ = readEnvelope(t, second)
	_ = endpoint.nextConnection(t)
	writeEnvelope(t, second, messageEvent, llm.CloneWorkerEventDelivery(delivery))
	receipt, err := decodePayload[llm.WorkerEventReceipt](readEnvelope(t, second))
	if err != nil || receipt.Decision != llm.WorkerEventACK || receipt.Delivery != delivery.ID {
		t.Fatalf("retry receipt = %#v, %v", receipt, err)
	}
	firstSeen := receiveEvent(t, endpoint.events)
	secondSeen := receiveEvent(t, endpoint.events)
	firstJSON, _ := json.Marshal(firstSeen)
	secondJSON, _ := json.Marshal(secondSeen)
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("unsettled event retry changed: first=%s second=%s", firstJSON, secondJSON)
	}
}

func TestTransportSerializesCommitEventAndPropagatesReadBackpressure(t *testing.T) {
	endpoint := newFakeEndpoint()
	release := make(chan struct{})
	endpoint.setCommit(func(ctx context.Context, delivery llm.WorkerEventDelivery) (llm.WorkerEventReceipt, error) {
		select {
		case <-release:
			return ackReceipt(delivery), nil
		case <-ctx.Done():
			return llm.WorkerEventReceipt{}, ctx.Err()
		}
	})
	_, _, server := startTestTransport(t, endpoint, defaultTestConfig())
	connection := dialTestWorker(t, server.URL, "session-serial", "")
	defer connection.CloseNow()
	_ = readEnvelope(t, connection)
	_ = endpoint.nextConnection(t)
	assignment := validAssignment("serial", true).Assignment
	first := validEvent("serial-one", assignment, llm.EventProgress)
	second := validEvent("serial-two", assignment, llm.EventProgress)
	writeEnvelope(t, connection, messageEvent, first)
	writeEnvelope(t, connection, messageEvent, second)
	if got := receiveEvent(t, endpoint.events); got.ID != first.ID {
		t.Fatalf("first commit = %q, want %q", got.ID, first.ID)
	}
	select {
	case early := <-endpoint.events:
		t.Fatalf("second event bypassed unresolved commit: %#v", early)
	case <-time.After(100 * time.Millisecond):
	}
	release <- struct{}{}
	firstReceipt, err := decodePayload[llm.WorkerEventReceipt](readEnvelope(t, connection))
	if err != nil || firstReceipt.Delivery != first.ID {
		t.Fatalf("first receipt = %#v, %v", firstReceipt, err)
	}
	if got := receiveEvent(t, endpoint.events); got.ID != second.ID {
		t.Fatalf("second commit = %q, want %q", got.ID, second.ID)
	}
	release <- struct{}{}
	secondReceipt, err := decodePayload[llm.WorkerEventReceipt](readEnvelope(t, connection))
	if err != nil || secondReceipt.Delivery != second.ID {
		t.Fatalf("second receipt = %#v, %v", secondReceipt, err)
	}
}

func TestTransportRejectsOversizeBinaryAndUnknownJSON(t *testing.T) {
	tests := []struct {
		name  string
		write func(*testing.T, *websocket.Conn)
	}{
		{
			name: "oversize",
			write: func(t *testing.T, connection *websocket.Conn) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				if err := connection.Write(ctx, websocket.MessageText, []byte(strings.Repeat("x", 2048))); err != nil {
					t.Fatalf("write oversize frame: %v", err)
				}
			},
		},
		{
			name: "binary",
			write: func(t *testing.T, connection *websocket.Conn) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				if err := connection.Write(ctx, websocket.MessageBinary, []byte(`{"version":"1"}`)); err != nil {
					t.Fatalf("write binary frame: %v", err)
				}
			},
		},
		{
			name: "unknown envelope field",
			write: func(t *testing.T, connection *websocket.Conn) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				encoded := []byte(`{"version":"1","type":"assignment_ack","payload":"delivery-a","future":true}`)
				if err := connection.Write(ctx, websocket.MessageText, encoded); err != nil {
					t.Fatalf("write unknown field: %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := newFakeEndpoint()
			config := defaultTestConfig()
			config.ReadLimit = 1024
			_, _, server := startTestTransport(t, endpoint, config)
			connection := dialTestWorker(t, server.URL, "session-invalid", "")
			defer connection.CloseNow()
			_ = readEnvelope(t, connection)
			_ = endpoint.nextConnection(t)
			test.write(t, connection)
			assertConnectionClosedWithoutEnvelope(t, connection)
			select {
			case event := <-endpoint.events:
				t.Fatalf("invalid wire input reached endpoint: %#v", event)
			default:
			}
		})
	}
}

func TestTransportRejectsEndpointPrincipalSubstitution(t *testing.T) {
	endpoint := newFakeEndpoint()
	endpoint.overridePrincipal = &llm.AuthenticatedWorker{WorkerID: "worker-b", SessionID: "session-a"}
	_, _, server := startTestTransport(t, endpoint, defaultTestConfig())
	connection := dialTestWorker(t, server.URL, "session-a", "")
	defer connection.CloseNow()
	assertConnectionClosedWithoutEnvelope(t, connection)
	core := endpoint.nextConnection(t)
	waitDone(t, core.Done(), "substituted endpoint connection")
}

func TestTransportShutdownStopsAdmissionClosesOwnedConnectionsAndBorrowsEndpoint(t *testing.T) {
	endpoint := newFakeEndpoint()
	config := defaultTestConfig()
	transport, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	initialization, cancelInitialization := context.WithCancel(context.Background())
	running, err := transport.Start(initialization, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	cancelInitialization()
	select {
	case <-running.Done():
		t.Fatal("transport retained Start context as runtime lifetime")
	default:
	}
	server := httptest.NewServer(transport)
	defer server.Close()
	connection := dialTestWorker(t, server.URL, "session-shutdown", "")
	_ = readEnvelope(t, connection)
	core := endpoint.nextConnection(t)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := running.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	waitDone(t, running.Done(), "transport runtime")
	waitDone(t, core.Done(), "owned worker connection")
	assertConnectionClosedWithoutEnvelope(t, connection)
	if endpoint.closed {
		t.Fatal("transport shut down borrowed WorkerEndpoint")
	}
	if running.Err() != nil {
		t.Fatalf("clean runtime error = %v", running.Err())
	}
	if err := running.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeated shutdown: %v", err)
	}

	late, response, err := dial(server.URL, "session-late", "")
	if late != nil {
		late.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown admission = connection=%v response=%v err=%v", late, response, err)
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
	running, err := transport.Start(t.Context(), newFakeEndpoint())
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
	waitDone(t, entered, "blocking authenticator")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := running.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown with admitted authenticator: %v", err)
	}
	waitDone(t, running.Done(), "transport with admitted authenticator")
	select {
	case <-requestDone:
	case <-time.After(3 * time.Second):
		t.Fatal("authentication handler survived transport shutdown")
	}
}

func TestTransportKeepaliveReleasesPeerThatStopsReading(t *testing.T) {
	endpoint := newFakeEndpoint()
	config := defaultTestConfig()
	config.PingInterval = 20 * time.Millisecond
	config.PingTimeout = 40 * time.Millisecond
	_, _, server := startTestTransport(t, endpoint, config)
	connection := dialTestWorker(t, server.URL, "session-half-open", "")
	defer connection.CloseNow()
	_ = readEnvelope(t, connection)
	core := endpoint.nextConnection(t)

	// coder/websocket processes control frames while its peer reads. Stop all
	// client reads after hello: the server ping must time out, close the socket,
	// cancel its read goroutine, and shut down the opened core connection.
	waitDone(t, core.Done(), "half-open worker connection")
}

func TestTransportConfigurationAndStrictSessionIdentity(t *testing.T) {
	var typedNil AuthenticateFunc
	if _, err := New(Config{GatewayID: "gateway-a", Authenticator: typedNil}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("typed-nil authenticator error = %v", err)
	}
	if _, err := New(Config{GatewayID: "bad gateway", Authenticator: allowWorker("worker-a")}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid gateway error = %v", err)
	}
	if _, err := New(Config{GatewayID: "gateway-a", Authenticator: allowWorker("worker-a"), ReadLimit: 100}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid read limit error = %v", err)
	}
	if _, err := New(Config{
		GatewayID: "gateway-a", Authenticator: allowWorker("worker-a"), WriteTimeout: -time.Second,
	}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid timeout error = %v", err)
	}

	endpoint := newFakeEndpoint()
	transport, err := New(defaultTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	transport.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/worker", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("handler before Start status = %d", recorder.Code)
	}
	var nilEndpointValue *fakeEndpoint
	if _, err := transport.Start(t.Context(), nilEndpointValue); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("typed-nil endpoint error = %v", err)
	}
	cancelled, cancelStart := context.WithCancel(context.Background())
	cancelStart()
	if _, err := transport.Start(cancelled, endpoint); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Start error = %v", err)
	}
	running, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer running.Shutdown(context.Background())
	if _, err := transport.Start(t.Context(), endpoint); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start error = %v", err)
	}
	server := httptest.NewServer(transport)
	defer server.Close()
	connection, response, err := dial(server.URL, "bad session", "")
	if connection != nil {
		connection.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("invalid session admission = connection=%v response=%v err=%v", connection, response, err)
	}
}

func defaultTestConfig() Config {
	return Config{
		GatewayID:     "gateway-a",
		Authenticator: allowWorker("worker-a"),
		PingInterval:  time.Hour,
		PingTimeout:   time.Second,
		WriteTimeout:  2 * time.Second,
	}
}

func allowWorker(worker llm.WorkerID) Authenticator {
	return AuthenticateFunc(func(context.Context, *http.Request) (Identity, error) {
		return Identity{Worker: worker}, nil
	})
}

func startTestTransport(
	t *testing.T,
	endpoint *fakeEndpoint,
	config Config,
) (*Transport, llm.WorkerTransportRuntime, *httptest.Server) {
	t.Helper()
	if config.PingInterval == 0 {
		config.PingInterval = time.Hour
	}
	transport, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	running, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(transport)
	t.Cleanup(func() {
		server.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := running.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown transport: %v", err)
		}
	})
	return transport, running, server
}

func dialTestWorker(t *testing.T, serverURL, session, token string) *websocket.Conn {
	t.Helper()
	connection, response, err := dial(serverURL, session, token)
	if err != nil {
		if response != nil {
			t.Fatalf("dial worker: %v (HTTP %d)", err, response.StatusCode)
		}
		t.Fatalf("dial worker: %v", err)
	}
	return connection
}

func dial(serverURL, session, token string) (*websocket.Conn, *http.Response, error) {
	header := http.Header{}
	if session != "" {
		header.Set(SessionHeader, session)
	}
	if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverURL, "http"), &websocket.DialOptions{
		HTTPHeader: header,
	})
}

func readEnvelope(t *testing.T, connection *websocket.Conn) envelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
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
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, connection, message); err != nil {
		t.Fatalf("write worker message: %v", err)
	}
}

func assertConnectionClosedWithoutEnvelope(t *testing.T, connection *websocket.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var message envelope
	if err := wsjson.Read(ctx, connection, &message); err == nil {
		t.Fatalf("connection produced unexpected terminal envelope: %#v", message)
	}
}

func waitDone(t *testing.T, done <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s did not stop", name)
	}
}

func receiveEvent(t *testing.T, events <-chan llm.WorkerEventDelivery) llm.WorkerEventDelivery {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("endpoint did not receive event")
		return llm.WorkerEventDelivery{}
	}
}

func validAssignment(label string, stream bool) llm.WorkerAssignmentDelivery {
	boundary := llm.AssignmentAfterAdmission
	if stream {
		boundary = llm.AssignmentAfterResponse
	}
	return llm.WorkerAssignmentDelivery{
		ID: llm.WorkerDeliveryID("delivery-" + label),
		Assignment: llm.Assignment{
			Identity: llm.CompletionIdentity{
				CallerID:       llm.CallerID("caller-a"),
				RequestID:      "request-" + label,
				TaskID:         llm.TaskID("task-" + label),
				WorkspaceKey:   "workspace-a",
				IdempotencyKey: llm.IdempotencyKey("idem-" + label),
			},
			Lease:    llm.WorkerLease{ID: llm.WorkerLeaseID("lease-" + label), Owner: "worker-a"},
			Boundary: boundary,
			Task: llm.TaskContext{
				TaskID: llm.TaskID("task-" + label), WorkspaceKey: "workspace-a",
				CapabilityTier: llm.TierWorkspace, HarnessID: "harness-a", HarnessVersion: "1",
				HarnessSessionID: "session-a", WorkspaceRoot: "/workspace/a",
			},
			Request: llm.Request{
				Model:  "human-expert",
				Stream: stream,
				Messages: []llm.Message{{
					Role:   llm.RoleUser,
					Blocks: []llm.Block{{Type: llm.BlockText, Text: "inspect the workspace"}},
				}},
			},
		},
	}
}

func validEvent(label string, assignment llm.Assignment, kind llm.EventType) llm.WorkerEventDelivery {
	event := llm.Event{ID: "event-" + label, Type: kind}
	switch kind {
	case llm.EventAccepted:
	case llm.EventProgress, llm.EventFinal, llm.EventClarification:
		event.Text = "human response " + label
	case llm.EventFailed:
		event.ErrorCode = "human_failed"
		event.Error = "human response failed"
	}
	return llm.WorkerEventDelivery{
		ID:       llm.WorkerDeliveryID("event-delivery-" + label),
		Identity: assignment.Identity,
		LeaseID:  assignment.Lease.ID,
		Event:    event,
	}
}

func ackReceipt(delivery llm.WorkerEventDelivery) llm.WorkerEventReceipt {
	return llm.WorkerEventReceipt{
		Delivery: delivery.ID,
		EventID:  delivery.Event.ID,
		Decision: llm.WorkerEventACK,
	}
}

type commitFunc func(context.Context, llm.WorkerEventDelivery) (llm.WorkerEventReceipt, error)

type fakeEndpoint struct {
	mu                sync.Mutex
	connections       chan *fakeConnection
	assignmentACKs    chan llm.WorkerDeliveryID
	events            chan llm.WorkerEventDelivery
	commit            commitFunc
	overridePrincipal *llm.AuthenticatedWorker
	closed            bool
}

func newFakeEndpoint() *fakeEndpoint {
	return &fakeEndpoint{
		connections:    make(chan *fakeConnection, 16),
		assignmentACKs: make(chan llm.WorkerDeliveryID, 16),
		events:         make(chan llm.WorkerEventDelivery, 16),
	}
}

func (endpoint *fakeEndpoint) setCommit(commit commitFunc) {
	endpoint.mu.Lock()
	endpoint.commit = commit
	endpoint.mu.Unlock()
}

func (endpoint *fakeEndpoint) OpenWorker(
	_ context.Context,
	principal llm.AuthenticatedWorker,
) (llm.WorkerConnection, error) {
	endpoint.mu.Lock()
	if endpoint.overridePrincipal != nil {
		principal = *endpoint.overridePrincipal
	}
	endpoint.mu.Unlock()
	connection := &fakeConnection{
		principal:   principal,
		assignments: make(chan llm.WorkerAssignmentDelivery, 8),
		done:        make(chan struct{}),
		endpoint:    endpoint,
	}
	endpoint.connections <- connection
	return connection, nil
}

func (endpoint *fakeEndpoint) nextConnection(t *testing.T) *fakeConnection {
	t.Helper()
	select {
	case connection := <-endpoint.connections:
		return connection
	case <-time.After(3 * time.Second):
		t.Fatal("endpoint did not open worker connection")
		return nil
	}
}

type fakeConnection struct {
	principal   llm.AuthenticatedWorker
	assignments chan llm.WorkerAssignmentDelivery
	done        chan struct{}
	endpoint    *fakeEndpoint
	closeOnce   sync.Once
}

func (connection *fakeConnection) Principal() llm.AuthenticatedWorker { return connection.principal }
func (connection *fakeConnection) Assignments() <-chan llm.WorkerAssignmentDelivery {
	return connection.assignments
}
func (connection *fakeConnection) AckAssignment(_ context.Context, id llm.WorkerDeliveryID) error {
	connection.endpoint.assignmentACKs <- id
	return nil
}
func (connection *fakeConnection) CommitEvent(
	ctx context.Context,
	delivery llm.WorkerEventDelivery,
) (llm.WorkerEventReceipt, error) {
	connection.endpoint.events <- llm.CloneWorkerEventDelivery(delivery)
	connection.endpoint.mu.Lock()
	commit := connection.endpoint.commit
	connection.endpoint.mu.Unlock()
	if commit != nil {
		return commit(ctx, delivery)
	}
	return ackReceipt(delivery), nil
}
func (connection *fakeConnection) Done() <-chan struct{} { return connection.done }
func (*fakeConnection) Err() error                       { return nil }
func (connection *fakeConnection) Shutdown(context.Context) error {
	connection.closeOnce.Do(func() { close(connection.done) })
	return nil
}

var _ llm.WorkerEndpoint = (*fakeEndpoint)(nil)
var _ llm.WorkerConnection = (*fakeConnection)(nil)
