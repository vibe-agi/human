package workerws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

func TestJournalExactReplayFIFOAndSettlementTombstones(t *testing.T) {
	journal := newMemoryJournal()
	if err := journal.Bind(t.Context(), JournalBinding{Gateway: "gateway-a", Authority: "tenant-a", Worker: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	first := validAssignment("assignment-journal-1")
	second := validAssignment("assignment-journal-2")
	firstDigest, _ := digestJournalValue(first)
	secondDigest, _ := digestJournalValue(second)

	state, err := journal.PutAssignment(t.Context(), JournalAssignment{Digest: firstDigest, Delivery: first})
	if err != nil || state != JournalEntryPending {
		t.Fatalf("put first assignment = %q, %v", state, err)
	}
	_, _ = journal.PutAssignment(t.Context(), JournalAssignment{Digest: secondDigest, Delivery: second})
	state, err = journal.PutAssignment(t.Context(), JournalAssignment{Digest: firstDigest, Delivery: first})
	if err != nil || state != JournalEntryPending {
		t.Fatalf("exact replay = %q, %v", state, err)
	}
	conflict := first
	conflict.Assignment.Task.Revision++
	conflictDigest, _ := digestJournalValue(conflict)
	if _, err := journal.PutAssignment(t.Context(), JournalAssignment{Digest: conflictDigest, Delivery: conflict}); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("different assignment replay error = %v", err)
	}
	records, err := journal.ListAssignments(t.Context(), 0, 10)
	if err != nil || len(records) != 2 || records[0].Delivery.ID != first.ID || records[1].Delivery.ID != second.ID {
		t.Fatalf("FIFO assignments = %#v, %v", records, err)
	}
	if err := journal.ConfirmAssignment(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}
	state, err = journal.PutAssignment(t.Context(), JournalAssignment{Digest: firstDigest, Delivery: first})
	if err != nil || state != JournalEntrySettled {
		t.Fatalf("confirmed exact replay = %q, %v", state, err)
	}

	event := validEvent("event-journal-1", "event-command-journal-1", first.Assignment)
	eventDigest, _ := digestJournalValue(event)
	if state, err := journal.PutEvent(t.Context(), JournalEvent{Digest: eventDigest, Delivery: event}); err != nil || state != JournalEntryPending {
		t.Fatalf("put event = %q, %v", state, err)
	}
	receipt := agent.WorkerEventReceipt{
		Delivery: event.ID, Event: event.Event.ID, Decision: agent.WorkerEventNACK,
		Code: agent.WorkerRejectState, Message: "task already completed",
	}
	receiptDigest, _ := digestJournalValue(receipt)
	if err := journal.SettleEvent(t.Context(), receipt, eventDigest, receiptDigest); err != nil {
		t.Fatal(err)
	}
	if events, _ := journal.ListEvents(t.Context(), 0, 10); len(events) != 0 {
		t.Fatalf("settled outbox still contains %#v", events)
	}
	if rejections, _ := journal.ListRejections(t.Context(), 0, 10); len(rejections) != 1 || rejections[0].Receipt != receipt {
		t.Fatalf("atomic NACK inbox = %#v", rejections)
	}
	if err := journal.SettleEvent(t.Context(), receipt, eventDigest, receiptDigest); err != nil {
		t.Fatalf("exact settlement replay: %v", err)
	}
	if err := journal.ConfirmRejection(t.Context(), event.ID); err != nil {
		t.Fatal(err)
	}
	if err := journal.ConfirmRejection(t.Context(), event.ID); err != nil {
		t.Fatalf("confirm rejection is not idempotent: %v", err)
	}
}

func TestJournalBindingIsPermanentCorrectnessIdentity(t *testing.T) {
	journal := newMemoryJournal()
	if _, err := journal.ListEvents(t.Context(), 0, 1); !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("unbound journal error = %v", err)
	}
	binding := JournalBinding{Gateway: "gateway-a", Authority: "tenant-a", Worker: "worker-a"}
	if err := journal.Bind(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	if err := journal.Bind(t.Context(), binding); err != nil {
		t.Fatalf("exact binding replay: %v", err)
	}
	binding.Gateway = "gateway-b"
	if err := journal.Bind(t.Context(), binding); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("different gateway binding error = %v", err)
	}
}

func TestClientAssignmentJournalReplayAndLostACKTombstone(t *testing.T) {
	journal := newMemoryJournal()
	endpoint := newFakeEndpoint()
	var sessionsMu sync.Mutex
	var sessions []string
	transport, err := New(Config{
		GatewayID: "gateway-a",
		Authenticator: AuthenticateFunc(func(_ context.Context, request *http.Request) (Identity, error) {
			sessionsMu.Lock()
			sessions = append(sessions, request.Header.Get(SessionHeader))
			sessionsMu.Unlock()
			return Identity{Authority: "tenant-a", Worker: "worker-a"}, nil
		}),
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	transportRuntime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(transport)
	t.Cleanup(func() {
		server.Close()
		_ = transportRuntime.Shutdown(context.Background())
	})

	client1 := newTestClient(t, server.URL, journal)
	core1 := endpoint.connection(t)
	delivery := validAssignment("assignment-client-replay")
	core1.assignments <- delivery
	assertAssignment(t, client1.Assignments(), delivery.ID)
	assertDeliveryID(t, core1.assignmentACKs, delivery.ID)
	shutdownClient(t, client1)

	client2 := newTestClient(t, server.URL, journal)
	core2 := endpoint.connection(t)
	assertAssignment(t, client2.Assignments(), delivery.ID)
	if err := client2.ConfirmAssignment(t.Context(), delivery.ID); err != nil {
		t.Fatal(err)
	}

	// Simulate a server whose prior wire ACK was lost. The settled digest
	// tombstone permits another wire ACK but suppresses duplicate app delivery.
	core2.assignments <- delivery
	assertDeliveryID(t, core2.assignmentACKs, delivery.ID)
	select {
	case duplicate := <-client2.Assignments():
		t.Fatalf("settled assignment was re-presented: %#v", duplicate)
	case <-time.After(150 * time.Millisecond):
	}
	shutdownClient(t, client2)

	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	if len(sessions) < 2 || sessions[0] == sessions[1] || sessions[0] == "" || sessions[1] == "" {
		t.Fatalf("reconnect session identities = %#v", sessions)
	}
}

func TestClientReplaysEventAfterUnsettledReceiptLoss(t *testing.T) {
	journal := newMemoryJournal()
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
	server := httptest.NewServer(transport)
	defer server.Close()
	defer runtime.Shutdown(context.Background())

	client := newTestClient(t, server.URL, journal)
	defer shutdownClient(t, client)
	core1 := endpoint.connection(t)
	assignment := validAssignment("assignment-event-retry")
	event := validEvent("event-delivery-retry", "event-command-retry", assignment.Assignment)
	if err := client.SendEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	first := assertEvent(t, core1.events)
	if first.ID != event.ID || first.Event.ID != event.Event.ID {
		t.Fatalf("first event = %#v", first)
	}
	endpoint.mu.Lock()
	endpoint.commitError = nil
	endpoint.mu.Unlock()

	core2 := endpoint.connection(t)
	second := assertEvent(t, core2.events)
	if second.ID != first.ID || second.Event != first.Event {
		t.Fatalf("event replay changed: first=%#v second=%#v", first, second)
	}
	eventually(t, func() bool {
		records, _ := journal.ListEvents(t.Context(), 0, 10)
		return len(records) == 0
	})
}

func TestClientNACKRemovesPoisonAndDeliversFollowingEvent(t *testing.T) {
	journal := newMemoryJournal()
	endpoint := newDecisionEndpoint("event-delivery-nack")
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
	server := httptest.NewServer(transport)
	defer server.Close()
	defer runtime.Shutdown(context.Background())
	client := newTestClient(t, server.URL, journal)
	connection := endpoint.connection(t)
	assignment := validAssignment("assignment-nack")
	first := validEvent("event-delivery-nack", "event-command-nack", assignment.Assignment)
	second := validEvent("event-delivery-after-nack", "event-command-after-nack", assignment.Assignment)
	if err := client.SendEvent(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := client.SendEvent(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	gotFirst := assertEvent(t, connection.events)
	gotSecond := assertEvent(t, connection.events)
	if gotFirst.ID != first.ID || gotSecond.ID != second.ID {
		t.Fatalf("event order = %q then %q", gotFirst.ID, gotSecond.ID)
	}
	eventually(t, func() bool {
		records, _ := journal.ListEvents(t.Context(), 0, 10)
		rejections, _ := journal.ListRejections(t.Context(), 0, 10)
		return len(records) == 0 && len(rejections) == 1
	})
	shutdownClient(t, client)

	restarted := newTestClient(t, server.URL, journal)
	_ = endpoint.connection(t)
	select {
	case rejected := <-restarted.Rejections():
		if rejected.Delivery.ID != first.ID || rejected.Receipt.Decision != agent.WorkerEventNACK {
			t.Fatalf("NACK = %#v", rejected)
		}
		if rejected.Delivery.Event != first.Event {
			t.Fatalf("rejected event payload changed: %#v", rejected.Delivery)
		}
		if err := restarted.ConfirmRejection(t.Context(), rejected.Receipt.Delivery); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("NACK was not durably presented")
	}
	shutdownClient(t, restarted)
	third := newTestClient(t, server.URL, journal)
	_ = endpoint.connection(t)
	select {
	case rejected := <-third.Rejections():
		t.Fatalf("confirmed rejection replayed: %#v", rejected)
	case <-time.After(150 * time.Millisecond):
	}
	shutdownClient(t, third)
}

func TestClientQueuesWhileGatewayAndWorkerAreOfflineThenRecovers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	journal := newMemoryJournal()
	client1 := newTestClientURL(t, "ws://"+address, journal)
	assignment := validAssignment("assignment-offline")
	event := validEvent("event-delivery-offline", "event-command-offline", assignment.Assignment)
	if err := client1.SendEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	shutdownClient(t, client1)

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
	listener, err = net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: transport}
	go httpServer.Serve(listener)
	defer httpServer.Close()
	defer runtime.Shutdown(context.Background())

	client2 := newTestClientURL(t, "ws://"+address, journal)
	defer shutdownClient(t, client2)
	connection := endpoint.connection(t)
	got := assertEvent(t, connection.events)
	if got.ID != event.ID || got.Event != event.Event {
		t.Fatalf("offline replay = %#v", got)
	}
}

func TestClientAuthenticationFailureIsTerminalWithoutReconnectStorm(t *testing.T) {
	journal := newMemoryJournal()
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(response, "no", http.StatusUnauthorized)
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, journal)
	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("authentication rejection did not terminate client")
	}
	if !errors.Is(client.Err(), ErrClientAuthentication) {
		t.Fatalf("client error = %v", client.Err())
	}
	time.Sleep(50 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("authentication attempts = %d, want 1", got)
	}
}

func TestClientSendsOnlyOneFIFOEventUntilItsReceipt(t *testing.T) {
	journal := newMemoryJournal()
	firstSeen := make(chan agent.WorkerEventDelivery, 1)
	secondSeen := make(chan agent.WorkerEventDelivery, 1)
	releaseFirst := make(chan struct{})
	handlerErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(response, request, nil)
		if err != nil {
			handlerErrors <- err
			return
		}
		defer connection.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		session := request.Header.Get(SessionHeader)
		greeting, _ := newEnvelope(messageHello, hello{Gateway: "gateway-a", Authority: "tenant-a", Worker: "worker-a", Session: session})
		if err := wsjson.Write(ctx, connection, greeting); err != nil {
			handlerErrors <- err
			return
		}
		firstMessage, err := readClientEnvelope(ctx, connection)
		if err != nil {
			handlerErrors <- err
			return
		}
		first, err := decodePayload[agent.WorkerEventDelivery](firstMessage)
		if err != nil {
			handlerErrors <- err
			return
		}
		firstSeen <- first
		secondResult := make(chan incomingClientMessage, 1)
		go func() {
			message, readErr := readClientEnvelope(ctx, connection)
			secondResult <- incomingClientMessage{message: message, err: readErr}
		}()
		<-releaseFirst
		nack := agent.WorkerEventReceipt{
			Delivery: first.ID, Event: first.Event.ID, Decision: agent.WorkerEventNACK,
			Code: agent.WorkerRejectState, Message: "release FIFO head",
		}
		nackMessage, _ := newEnvelope(messageEventReceipt, nack)
		if err := wsjson.Write(ctx, connection, nackMessage); err != nil {
			handlerErrors <- err
			return
		}
		secondRead := <-secondResult
		if secondRead.err != nil {
			handlerErrors <- secondRead.err
			return
		}
		second, err := decodePayload[agent.WorkerEventDelivery](secondRead.message)
		if err != nil {
			handlerErrors <- err
			return
		}
		secondSeen <- second
		ack := agent.WorkerEventReceipt{Delivery: second.ID, Event: second.Event.ID, Decision: agent.WorkerEventACK}
		ackMessage, _ := newEnvelope(messageEventReceipt, ack)
		if err := wsjson.Write(ctx, connection, ackMessage); err != nil {
			handlerErrors <- err
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, journal)
	defer shutdownClient(t, client)
	assignment := validAssignment("assignment-single-flight")
	first := validEvent("event-fifo-1", "event-command-fifo-1", assignment.Assignment)
	second := validEvent("event-fifo-2", "event-command-fifo-2", assignment.Assignment)
	if err := client.SendEvent(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := client.SendEvent(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if got := assertEvent(t, firstSeen); got.ID != first.ID {
		t.Fatalf("first wire event = %q", got.ID)
	}
	select {
	case got := <-secondSeen:
		t.Fatalf("second event sent before head receipt: %#v", got)
	case err := <-handlerErrors:
		t.Fatalf("raw gateway failed: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(releaseFirst)
	if got := assertEvent(t, secondSeen); got.ID != second.ID {
		t.Fatalf("second wire event = %q", got.ID)
	}
}

func TestClientStrictlyRejectsUnknownOrBinaryEnvelope(t *testing.T) {
	for _, binary := range []bool{false, true} {
		name := "unknown-field"
		if binary {
			name = "binary"
		}
		t.Run(name, func(t *testing.T) {
			journal := newMemoryJournal()
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				connection, err := websocket.Accept(response, request, nil)
				if err != nil {
					return
				}
				defer connection.CloseNow()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				session := request.Header.Get(SessionHeader)
				encoded := []byte(fmt.Sprintf(`{"version":"1","type":"hello","payload":{"authority":"tenant-a","worker":"worker-a","session":%q},"unknown":true}`, session))
				kind := websocket.MessageText
				if binary {
					message, _ := newEnvelope(messageHello, hello{Gateway: "gateway-a", Authority: "tenant-a", Worker: "worker-a", Session: session})
					encoded, _ = json.Marshal(message)
					kind = websocket.MessageBinary
				}
				_ = connection.Write(ctx, kind, encoded)
				<-ctx.Done()
			}))
			defer server.Close()
			client := newTestClient(t, server.URL, journal)
			select {
			case <-client.Done():
			case <-time.After(2 * time.Second):
				t.Fatal("invalid envelope did not terminate client")
			}
			if !errors.Is(client.Err(), ErrClientProtocol) {
				t.Fatalf("client error = %v", client.Err())
			}
		})
	}
}

func TestClientReconcilesCommitUnknownJournalWrites(t *testing.T) {
	t.Run("assignment", func(t *testing.T) {
		journal := newMemoryJournal()
		journal.putAssignmentCommitUnknown = true
		endpoint := newFakeEndpoint()
		transport, _ := New(Config{GatewayID: "gateway-a", Authenticator: AuthenticateFunc(func(context.Context, *http.Request) (Identity, error) {
			return Identity{Authority: "tenant-a", Worker: "worker-a"}, nil
		}), PingInterval: time.Hour})
		runtime, _ := transport.Start(t.Context(), endpoint)
		server := httptest.NewServer(transport)
		defer server.Close()
		defer runtime.Shutdown(context.Background())
		client := newTestClient(t, server.URL, journal)
		defer shutdownClient(t, client)
		core := endpoint.connection(t)
		delivery := validAssignment("assignment-commit-unknown")
		core.assignments <- delivery
		assertAssignment(t, client.Assignments(), delivery.ID)
	})

	t.Run("nack", func(t *testing.T) {
		journal := newMemoryJournal()
		journal.settleCommitUnknown = true
		endpoint := newDecisionEndpoint("event-commit-unknown")
		transport, _ := New(Config{GatewayID: "gateway-a", Authenticator: AuthenticateFunc(func(context.Context, *http.Request) (Identity, error) {
			return Identity{Authority: "tenant-a", Worker: "worker-a"}, nil
		}), PingInterval: time.Hour})
		runtime, _ := transport.Start(t.Context(), endpoint)
		server := httptest.NewServer(transport)
		defer server.Close()
		defer runtime.Shutdown(context.Background())
		client := newTestClient(t, server.URL, journal)
		defer shutdownClient(t, client)
		_ = endpoint.connection(t)
		assignment := validAssignment("assignment-nack-unknown")
		event := validEvent("event-commit-unknown", "event-command-commit-unknown", assignment.Assignment)
		if err := client.SendEvent(t.Context(), event); err != nil {
			t.Fatal(err)
		}
		select {
		case rejected := <-client.Rejections():
			if rejected.Delivery.ID != event.ID {
				t.Fatalf("rejected event = %#v", rejected)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("commit-unknown NACK was not reconciled")
		}
	})
}

func TestClientRejectsTypedNilHeaderProvider(t *testing.T) {
	journal := newMemoryJournal()
	var provider *nilTestHeaderProvider
	config := testClientConfig("ws://127.0.0.1:1", framework.Borrow[Journal](journal))
	config.HeaderProvider = provider
	if _, err := NewClient(t.Context(), config); !errors.Is(err, ErrClientConfiguration) {
		t.Fatalf("typed-nil header provider error = %v", err)
	}
}

func TestClientReleasesOnlyOwnedJournal(t *testing.T) {
	journal := newMemoryJournal()
	var releases atomic.Int64
	owned, err := framework.Own[Journal](journal, func(context.Context) error {
		releases.Add(1)
		journal.close()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(t.Context(), testClientConfig("ws://127.0.0.1:1", owned))
	if err != nil {
		t.Fatal(err)
	}
	shutdownClient(t, client)
	if releases.Load() != 1 {
		t.Fatalf("owned releases = %d", releases.Load())
	}

	borrowedJournal := newMemoryJournal()
	borrowed, err := NewClient(t.Context(), testClientConfig("ws://127.0.0.1:1", framework.Borrow[Journal](borrowedJournal)))
	if err != nil {
		t.Fatal(err)
	}
	shutdownClient(t, borrowed)
	if _, err := borrowedJournal.ListEvents(t.Context(), 0, 1); err != nil {
		t.Fatalf("borrowed journal was released: %v", err)
	}
}

func TestClientShutdownDrainsAdmittedJournalOperationBeforeRelease(t *testing.T) {
	base := newMemoryJournal()
	blocking := &blockingPutJournal{
		memoryJournal: base, entered: make(chan struct{}), unblock: make(chan struct{}),
	}
	var releases atomic.Int64
	owned, err := framework.Own[Journal](blocking, func(context.Context) error {
		releases.Add(1)
		base.close()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(t.Context(), testClientConfig("ws://127.0.0.1:1", owned))
	if err != nil {
		t.Fatal(err)
	}
	assignment := validAssignment("assignment-drain")
	event := validEvent("event-drain", "event-command-drain", assignment.Assignment)
	sendDone := make(chan error, 1)
	go func() { sendDone <- client.SendEvent(context.Background(), event) }()
	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("PutEvent did not enter Journal")
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- client.Shutdown(context.Background()) }()
	eventually(t, func() bool {
		client.operationMu.Lock()
		defer client.operationMu.Unlock()
		return !client.accepting
	})
	probe := event
	probe.ID = "event-drain-probe"
	probe.Event.ID = "event-command-drain-probe"
	if err := client.SendEvent(context.Background(), probe); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("new operation during shutdown = %v", err)
	}
	if releases.Load() != 0 {
		t.Fatal("owned Journal released while admitted PutEvent was active")
	}
	close(blocking.unblock)
	if err := <-sendDone; err != nil {
		t.Fatalf("admitted SendEvent failed: %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
	if releases.Load() != 1 {
		t.Fatalf("owned releases = %d, want 1", releases.Load())
	}
}

func newTestClient(t *testing.T, serverURL string, journal Journal) *Client {
	t.Helper()
	return newTestClientURL(t, "ws"+strings.TrimPrefix(serverURL, "http"), journal)
}

func newTestClientURL(t *testing.T, target string, journal Journal) *Client {
	t.Helper()
	client, err := NewClient(t.Context(), testClientConfig(target, framework.Borrow(journal)))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testClientConfig(target string, resource framework.Resource[Journal]) ClientConfig {
	return ClientConfig{
		URL: target, Gateway: "gateway-a", Authority: "tenant-a", Worker: "worker-a", Journal: resource,
		ConnectTimeout: 250 * time.Millisecond, WriteTimeout: time.Second,
		ReconnectMinDelay: 5 * time.Millisecond, ReconnectMaxDelay: 25 * time.Millisecond,
		ReconnectResetAfter: time.Second,
	}
}

func shutdownClient(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown client: %v", err)
	}
}

func assertAssignment(t *testing.T, channel <-chan agent.WorkerAssignmentDelivery, id agent.WorkerDeliveryID) {
	t.Helper()
	select {
	case delivery := <-channel:
		if delivery.ID != id {
			t.Fatalf("assignment = %q, want %q", delivery.ID, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("assignment %q was not delivered", id)
	}
}

func assertDeliveryID(t *testing.T, channel <-chan agent.WorkerDeliveryID, id agent.WorkerDeliveryID) {
	t.Helper()
	select {
	case got := <-channel:
		if got != id {
			t.Fatalf("delivery id = %q, want %q", got, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("delivery id %q was not observed", id)
	}
}

func assertEvent(t *testing.T, channel <-chan agent.WorkerEventDelivery) agent.WorkerEventDelivery {
	t.Helper()
	select {
	case event := <-channel:
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("worker event was not delivered")
		return agent.WorkerEventDelivery{}
	}
}

func eventually(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

type decisionEndpoint struct {
	nack        agent.WorkerDeliveryID
	connections chan *decisionConnection
}

func newDecisionEndpoint(nack agent.WorkerDeliveryID) *decisionEndpoint {
	return &decisionEndpoint{nack: nack, connections: make(chan *decisionConnection, 4)}
}

func (endpoint *decisionEndpoint) OpenWorker(_ context.Context, principal agent.AuthenticatedWorker) (agent.WorkerConnection, error) {
	connection := &decisionConnection{
		principal: principal, endpoint: endpoint, assignments: make(chan agent.WorkerAssignmentDelivery),
		events: make(chan agent.WorkerEventDelivery, 8), done: make(chan struct{}),
	}
	endpoint.connections <- connection
	return connection, nil
}

func (endpoint *decisionEndpoint) connection(t *testing.T) *decisionConnection {
	t.Helper()
	select {
	case connection := <-endpoint.connections:
		return connection
	case <-time.After(2 * time.Second):
		t.Fatal("decision endpoint did not connect")
		return nil
	}
}

type decisionConnection struct {
	principal   agent.AuthenticatedWorker
	endpoint    *decisionEndpoint
	assignments chan agent.WorkerAssignmentDelivery
	events      chan agent.WorkerEventDelivery
	done        chan struct{}
	once        sync.Once
}

func (connection *decisionConnection) Principal() agent.AuthenticatedWorker {
	return connection.principal
}
func (connection *decisionConnection) Assignments() <-chan agent.WorkerAssignmentDelivery {
	return connection.assignments
}
func (*decisionConnection) AckAssignment(context.Context, agent.WorkerDeliveryID) error { return nil }
func (connection *decisionConnection) CommitEvent(_ context.Context, delivery agent.WorkerEventDelivery) (agent.WorkerEventReceipt, error) {
	connection.events <- delivery
	receipt := agent.WorkerEventReceipt{Delivery: delivery.ID, Event: delivery.Event.ID, Decision: agent.WorkerEventACK}
	if delivery.ID == connection.endpoint.nack {
		receipt.Decision = agent.WorkerEventNACK
		receipt.Code = agent.WorkerRejectState
		receipt.Message = "deterministic rejection"
	}
	return receipt, nil
}
func (connection *decisionConnection) Done() <-chan struct{} { return connection.done }
func (*decisionConnection) Err() error                       { return nil }
func (connection *decisionConnection) Shutdown(context.Context) error {
	connection.once.Do(func() { close(connection.done) })
	return nil
}

var _ agent.WorkerEndpoint = (*decisionEndpoint)(nil)
var _ agent.WorkerConnection = (*decisionConnection)(nil)

type nilTestHeaderProvider struct{}

func (*nilTestHeaderProvider) WorkerHeaders(context.Context) (http.Header, error) {
	return nil, nil
}

type blockingPutJournal struct {
	*memoryJournal
	entered chan struct{}
	unblock chan struct{}
	once    sync.Once
}

func (journal *blockingPutJournal) PutEvent(ctx context.Context, record JournalEvent) (JournalEntryState, error) {
	journal.once.Do(func() { close(journal.entered) })
	select {
	case <-journal.unblock:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return journal.memoryJournal.PutEvent(ctx, record)
}

var _ Journal = (*blockingPutJournal)(nil)
