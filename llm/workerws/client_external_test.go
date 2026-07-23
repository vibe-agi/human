package workerws_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/workerws"
	workersqlite "github.com/vibe-agi/human/llm/workerws/sqlite"
)

func TestClientJournalsAssignmentBeforeWireACKAndReplaysApplicationInbox(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	endpoint := newClientTestEndpoint()
	durabilityChecked := make(chan error, 1)
	endpoint.ackHook = func(id llm.WorkerDeliveryID) error {
		records, err := journal.ListAssignments(t.Context(), 0, 10)
		if err != nil {
			durabilityChecked <- err
			return nil
		}
		if len(records) != 1 || records[0].Delivery.ID != id {
			durabilityChecked <- errors.New("assignment ACK crossed wire before journal durability")
			return nil
		}
		durabilityChecked <- nil
		return nil
	}
	server, runtime := startClientTestGateway(t, endpoint)
	defer server.Close()
	defer runtime.Shutdown(context.Background())

	first := newClientTestClient(t, server.URL, journal)
	connection1 := endpoint.connection(t)
	delivery := clientTestAssignment("assignment-replay")
	connection1.assignments <- delivery
	assertClientAssignment(t, first.Assignments(), delivery.ID)
	assertClientDeliveryID(t, connection1.assignmentACKs, delivery.ID)
	if err := <-durabilityChecked; err != nil {
		t.Fatal(err)
	}
	endpoint.mu.Lock()
	endpoint.ackHook = nil
	endpoint.mu.Unlock()
	shutdownClientTestClient(t, first)

	// The wire ACK removes gateway ownership, not the application inbox. A
	// process restart re-presents the durable assignment until the host confirms.
	second := newClientTestClient(t, server.URL, journal)
	connection2 := endpoint.connection(t)
	assertClientAssignment(t, second.Assignments(), delivery.ID)
	if err := second.ConfirmAssignment(t.Context(), delivery.ID); err != nil {
		t.Fatal(err)
	}

	// Simulate a lost earlier assignment ACK. The compact local tombstone permits
	// another wire ACK but suppresses a duplicate application presentation.
	connection2.assignments <- delivery
	assertClientDeliveryID(t, connection2.assignmentACKs, delivery.ID)
	select {
	case duplicate := <-second.Assignments():
		t.Fatalf("settled assignment re-presented: %#v", duplicate)
	case <-time.After(100 * time.Millisecond):
	}
	shutdownClientTestClient(t, second)
}

func TestClientReplaysExactEventAfterCommittedACKIsLost(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	endpoint := newClientTestEndpoint()
	var commits atomic.Int64
	var committedMu sync.Mutex
	committed := make(map[llm.WorkerDeliveryID]llm.WorkerEventDelivery)
	endpoint.decision = func(delivery llm.WorkerEventDelivery) (llm.WorkerEventReceipt, error) {
		committedMu.Lock()
		prior, exists := committed[delivery.ID]
		if !exists {
			committed[delivery.ID] = llm.CloneWorkerEventDelivery(delivery)
		}
		committedMu.Unlock()
		if exists && !reflect.DeepEqual(prior, delivery) {
			return llm.WorkerEventReceipt{}, llm.ErrWorkerDeliveryConflict
		}
		if commits.Add(1) == 1 {
			// The domain commit happened, but the transport did not receive a usable
			// receipt. Closing and exact replay is the only safe client behavior.
			return llm.WorkerEventReceipt{}, llm.ErrWorkerDeliveryIndeterminate
		}
		return clientTestACK(delivery), nil
	}
	server, runtime := startClientTestGateway(t, endpoint)
	defer server.Close()
	defer runtime.Shutdown(context.Background())
	client := newClientTestClient(t, server.URL, journal)
	defer shutdownClientTestClient(t, client)

	firstConnection := endpoint.connection(t)
	event := clientTestEvent("delivery-ack-loss")
	if err := client.SendEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	first := assertClientEvent(t, firstConnection.events)
	secondConnection := endpoint.connection(t)
	second := assertClientEvent(t, secondConnection.events)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("exact replay changed event:\nfirst:  %#v\nsecond: %#v", first, second)
	}
	eventuallyClientTest(t, func() bool {
		pending, _ := journal.ListEvents(t.Context(), 0, 10)
		return len(pending) == 0
	})
}

func TestClientNACKAtomicallyRemovesPoisonAndPreservesFIFO(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	endpoint := newClientTestEndpoint()
	endpoint.decision = func(delivery llm.WorkerEventDelivery) (llm.WorkerEventReceipt, error) {
		if delivery.ID == "delivery-poison" {
			return llm.WorkerEventReceipt{
				Delivery: delivery.ID, EventID: delivery.Event.ID, Decision: llm.WorkerEventNACK,
				Code: llm.WorkerRejectInvalid, Message: "deterministic rejection",
			}, nil
		}
		return clientTestACK(delivery), nil
	}
	server, runtime := startClientTestGateway(t, endpoint)
	defer server.Close()
	defer runtime.Shutdown(context.Background())
	client := newClientTestClient(t, server.URL, journal)
	connection := endpoint.connection(t)

	first := clientTestEvent("delivery-poison")
	second := clientTestEvent("delivery-after-poison")
	if err := client.SendEvent(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := client.SendEvent(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if got := assertClientEvent(t, connection.events); got.ID != first.ID {
		t.Fatalf("first event = %q", got.ID)
	}
	if got := assertClientEvent(t, connection.events); got.ID != second.ID {
		t.Fatalf("second event = %q", got.ID)
	}
	select {
	case rejected := <-client.Rejections():
		if rejected.Delivery.ID != first.ID || rejected.Receipt.Decision != llm.WorkerEventNACK {
			t.Fatalf("rejection = %#v", rejected)
		}
		if err := client.ConfirmRejection(t.Context(), rejected.Delivery.ID); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("durable NACK was not presented")
	}
	eventuallyClientTest(t, func() bool {
		pending, _ := journal.ListEvents(t.Context(), 0, 10)
		rejected, _ := journal.ListRejections(t.Context(), 0, 10)
		return len(pending) == 0 && len(rejected) == 0
	})
	shutdownClientTestClient(t, client)
}

func TestClientWorkerGatewayAndApplicationCanAllGoOfflineThenRecover(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	path := filepath.Join(t.TempDir(), "worker-journal.db")
	firstResource, err := workersqlite.Open(t.Context(), workersqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}

	// Gateway is offline. The worker process durably accepts an event, then also
	// goes offline. No remote caller/session is needed to keep retry state alive.
	first, err := workerws.NewClient(t.Context(), clientTestConfig("ws://"+address, firstResource))
	if err != nil {
		t.Fatal(err)
	}
	event := clientTestEvent("delivery-three-party-offline")
	if err := first.SendEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	shutdownClientTestClient(t, first)

	endpoint := newClientTestEndpoint()
	transport, err := workerws.New(workerws.Config{
		GatewayID: "gateway-a",
		Authenticator: workerws.AuthenticateFunc(func(context.Context, *http.Request) (workerws.Identity, error) {
			return workerws.Identity{Worker: "worker-a"}, nil
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

	secondResource, err := workersqlite.Open(t.Context(), workersqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	second, err := workerws.NewClient(t.Context(), clientTestConfig("ws://"+address, secondResource))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdownClientTestClient(t, second)
	connection := endpoint.connection(t)
	if got := assertClientEvent(t, connection.events); !reflect.DeepEqual(got, event) {
		t.Fatalf("offline replay changed: %#v", got)
	}
	assignment := clientTestAssignment("assignment-after-all-recover")
	connection.assignments <- assignment
	assertClientAssignment(t, second.Assignments(), assignment.ID)
}

func TestClientConcurrentProducersFollowDurableFIFO(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	client := newClientTestClientURL(t, "ws://"+address, journal)

	const count = 32
	start := make(chan struct{})
	errorsByProducer := make(chan error, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errorsByProducer <- client.SendEvent(t.Context(), clientTestEvent(
				llm.WorkerDeliveryID("delivery-concurrent-"+leftPad(index)),
			))
		}()
	}
	close(start)
	wait.Wait()
	close(errorsByProducer)
	for err := range errorsByProducer {
		if err != nil {
			t.Fatalf("concurrent SendEvent: %v", err)
		}
	}
	pending, err := journal.ListEvents(t.Context(), 0, count)
	if err != nil || len(pending) != count {
		t.Fatalf("durable FIFO snapshot = %d, %v", len(pending), err)
	}
	want := make([]llm.WorkerDeliveryID, len(pending))
	for index := range pending {
		want[index] = pending[index].Delivery.ID
	}

	endpoint := newClientTestEndpoint()
	transport, err := workerws.New(workerws.Config{
		GatewayID: "gateway-a",
		Authenticator: workerws.AuthenticateFunc(func(context.Context, *http.Request) (workerws.Identity, error) {
			return workerws.Identity{Worker: "worker-a"}, nil
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
	connection := endpoint.connection(t)
	for index, id := range want {
		if got := assertClientEvent(t, connection.events); got.ID != id {
			t.Fatalf("wire FIFO[%d] = %q, want %q", index, got.ID, id)
		}
	}
	shutdownClientTestClient(t, client)
}

func TestClientRejectsOversizeBeforeJournalingAndStopsAdmissionOnShutdown(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	client := newClientTestClientURL(t, "ws://127.0.0.1:1", journal)
	oversize := clientTestEvent("delivery-oversize")
	oversize.Event.Text = strings.Repeat("x", (16<<20)+1)
	if err := client.SendEvent(t.Context(), oversize); !errors.Is(err, workerws.ErrClientMessageTooLarge) {
		t.Fatalf("oversize error = %v, want ErrClientMessageTooLarge", err)
	}
	if pending, err := journal.ListEvents(t.Context(), 0, 1); err != nil || len(pending) != 0 {
		t.Fatalf("oversize event reached Journal: %#v, %v", pending, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := client.SendEvent(cancelled, clientTestEvent("delivery-cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled SendEvent = %v, want context.Canceled", err)
	}
	shutdownClientTestClient(t, client)
	if err := client.SendEvent(t.Context(), clientTestEvent("delivery-after-shutdown")); !errors.Is(err, workerws.ErrClientClosed) {
		t.Fatalf("SendEvent after shutdown = %v, want ErrClientClosed", err)
	}
}

func TestClientCanonicalizesJSONNumbersBeforeDurableOutbox(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	client := newClientTestClientURL(t, "ws://127.0.0.1:1", journal)
	event := clientTestEvent("delivery-large-integer")
	event.Event = llm.Event{
		ID: "event-large-integer", Type: llm.EventToolCalls,
		ToolCalls: []llm.ToolCall{{
			ID: "tool-large-integer", Namespace: "human", Name: "write_file",
			Input: map[string]any{"value": uint64(18446744073709551615)},
		}},
	}
	if err := client.SendEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	pending, err := journal.ListEvents(t.Context(), 0, 1)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending event = %#v, %v", pending, err)
	}
	value, ok := pending[0].Delivery.Event.ToolCalls[0].Input["value"].(json.Number)
	if !ok || value.String() != "18446744073709551615" {
		t.Fatalf("canonical large integer = %#v", pending[0].Delivery.Event.ToolCalls[0].Input["value"])
	}
	shutdownClientTestClient(t, client)
}

func TestClientScansShortJournalPagesUntilExplicitEOF(t *testing.T) {
	base, _ := humantest.NewMemoryLLMWorkerJournal()
	if err := base.Bind(t.Context(), workerws.JournalBinding{Gateway: "gateway-a", Worker: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	const count = 3
	for index := 0; index < count; index++ {
		assignment := clientTestAssignment(llm.WorkerDeliveryID("assignment-short-" + leftPad(index)))
		if _, err := base.PutAssignment(t.Context(), workerws.JournalAssignment{
			Digest: clientTestDigest(assignment), Delivery: assignment,
		}); err != nil {
			t.Fatal(err)
		}
		event := clientTestEvent(llm.WorkerDeliveryID("event-short-" + leftPad(index)))
		eventDigest := clientTestDigest(event)
		if _, err := base.PutEvent(t.Context(), workerws.JournalEvent{
			Digest: eventDigest, Delivery: event,
		}); err != nil {
			t.Fatal(err)
		}
		receipt := llm.WorkerEventReceipt{
			Delivery: event.ID, EventID: event.Event.ID, Decision: llm.WorkerEventNACK,
			Code: llm.WorkerRejectInvalid, Message: "short-page fixture",
		}
		if err := base.SettleEvent(t.Context(), receipt, eventDigest, clientTestDigest(receipt)); err != nil {
			t.Fatal(err)
		}
	}
	short := humantest.ShortPageLLMWorkerJournal{Journal: base, PageSize: 1}
	client := newClientTestClientURL(t, "ws://127.0.0.1:1", short)
	for index := 0; index < count; index++ {
		assertClientAssignment(t, client.Assignments(), llm.WorkerDeliveryID("assignment-short-"+leftPad(index)))
	}
	for index := 0; index < count; index++ {
		select {
		case rejected := <-client.Rejections():
			want := llm.WorkerDeliveryID("event-short-" + leftPad(index))
			if rejected.Delivery.ID != want {
				t.Fatalf("rejection[%d] = %q, want %q", index, rejected.Delivery.ID, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("rejection %d was stranded after a short page", index)
		}
	}
	shutdownClientTestClient(t, client)
}

func TestClientFailsClearlyWhenReopenedWithSmallerWriteLimit(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	firstConfig := clientTestConfig("ws://127.0.0.1:1", framework.Borrow[workerws.Journal](journal))
	firstConfig.WriteLimit = 4096
	first, err := workerws.NewClient(t.Context(), firstConfig)
	if err != nil {
		t.Fatal(err)
	}
	event := clientTestEvent("delivery-config-decrease")
	event.Event.Text = strings.Repeat("x", 2048)
	if err := first.SendEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	shutdownClientTestClient(t, first)

	endpoint := newClientTestEndpoint()
	server, runtime := startClientTestGateway(t, endpoint)
	defer server.Close()
	defer runtime.Shutdown(context.Background())
	secondConfig := clientTestConfig(
		"ws"+strings.TrimPrefix(server.URL, "http"), framework.Borrow[workerws.Journal](journal),
	)
	secondConfig.WriteLimit = 1024
	second, err := workerws.NewClient(t.Context(), secondConfig)
	if err != nil {
		t.Fatal(err)
	}
	_ = endpoint.connection(t)
	select {
	case <-second.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("incompatible pending event did not terminate client")
	}
	if !errors.Is(second.Err(), workerws.ErrClientConfiguration) {
		t.Fatalf("client error = %v, want ErrClientConfiguration", second.Err())
	}
	pending, err := journal.ListEvents(t.Context(), 0, 10)
	if err != nil || len(pending) != 1 || pending[0].Delivery.ID != event.ID {
		t.Fatalf("incompatible pending event was not retained: %#v, %v", pending, err)
	}
}

func TestClientOwnedAndBorrowedJournalLifecycles(t *testing.T) {
	ownedJournal, ownedRelease := humantest.NewMemoryLLMWorkerJournal()
	var ownedCalls atomic.Int64
	owned, err := framework.Own[workerws.Journal](ownedJournal, func(ctx context.Context) error {
		ownedCalls.Add(1)
		return ownedRelease(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	ownedClient, err := workerws.NewClient(t.Context(), clientTestConfig("ws://127.0.0.1:1", owned))
	if err != nil {
		t.Fatal(err)
	}
	shutdownClientTestClient(t, ownedClient)
	if ownedCalls.Load() != 1 {
		t.Fatalf("owned Journal releases = %d, want 1", ownedCalls.Load())
	}

	borrowedJournal, _ := humantest.NewMemoryLLMWorkerJournal()
	borrowedClient, err := workerws.NewClient(t.Context(), clientTestConfig(
		"ws://127.0.0.1:1", framework.Borrow[workerws.Journal](borrowedJournal),
	))
	if err != nil {
		t.Fatal(err)
	}
	shutdownClientTestClient(t, borrowedClient)
	if _, err := borrowedJournal.ListEvents(t.Context(), 0, 1); err != nil {
		t.Fatalf("borrowed Journal was released: %v", err)
	}

	failedJournal, failedRelease := humantest.NewMemoryLLMWorkerJournal()
	var failedCalls atomic.Int64
	failedOwned, err := framework.Own[workerws.Journal](failedJournal, func(ctx context.Context) error {
		failedCalls.Add(1)
		return failedRelease(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workerws.NewClient(t.Context(), clientTestConfig("http://not-websocket", failedOwned)); !errors.Is(err, workerws.ErrClientConfiguration) {
		t.Fatalf("invalid constructor error = %v", err)
	}
	if failedCalls.Load() != 1 {
		t.Fatalf("constructor-failure releases = %d, want 1", failedCalls.Load())
	}
}

func TestClientAuthenticationFailureIsTerminalWithoutReconnectStorm(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(response, "denied", http.StatusUnauthorized)
	}))
	defer server.Close()
	client := newClientTestClient(t, server.URL, journal)
	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("authentication failure did not terminate client")
	}
	if !errors.Is(client.Err(), workerws.ErrClientAuthentication) {
		t.Fatalf("client error = %v", client.Err())
	}
	time.Sleep(30 * time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatalf("authentication attempts = %d, want 1", attempts.Load())
	}
}

func TestClientSupportsStaticHTTPHeaderWithoutHeaderProvider(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	endpoint := newClientTestEndpoint()
	transport, err := workerws.New(workerws.Config{
		GatewayID: "gateway-a",
		Authenticator: workerws.AuthenticateFunc(func(_ context.Context, request *http.Request) (workerws.Identity, error) {
			if request.Header.Get("X-Test-Worker-Token") != "secret" {
				return workerws.Identity{}, errors.New("missing static token")
			}
			return workerws.Identity{Worker: "worker-a"}, nil
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
	config := clientTestConfig(
		"ws"+strings.TrimPrefix(server.URL, "http"), framework.Borrow[workerws.Journal](journal),
	)
	config.HTTPHeader = http.Header{"X-Test-Worker-Token": []string{"secret"}}
	config.HTTPHeader.Set(workerws.SessionHeader, "stale-session")
	client, err := workerws.NewClient(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	connection := endpoint.connection(t)
	if connection.principal.SessionID == "stale-session" || connection.principal.SessionID == "" {
		t.Fatalf("client did not replace caller-supplied session header: %q", connection.principal.SessionID)
	}
	shutdownClientTestClient(t, client)
}

func TestClientRefreshesShortLivedCredentialOnEveryReconnect(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	endpoint := newClientTestEndpoint()
	var credential atomic.Value
	credential.Store("worker-token-1")
	var providerCalls atomic.Int64
	transport, err := workerws.New(workerws.Config{
		GatewayID: "gateway-a",
		Authenticator: workerws.AuthenticateFunc(func(_ context.Context, request *http.Request) (workerws.Identity, error) {
			want := "Bearer " + credential.Load().(string)
			if request.Header.Get("Authorization") != want {
				return workerws.Identity{}, errors.New("stale worker credential")
			}
			return workerws.Identity{Worker: "worker-a"}, nil
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
	hijacked := make(chan net.Conn, 4)
	server := httptest.NewUnstartedServer(transport)
	server.Config.ConnState = func(connection net.Conn, state http.ConnState) {
		if state == http.StateHijacked {
			hijacked <- connection
		}
	}
	server.Start()
	defer server.Close()
	defer runtime.Shutdown(context.Background())
	config := clientTestConfig(
		"ws"+strings.TrimPrefix(server.URL, "http"), framework.Borrow[workerws.Journal](journal),
	)
	config.HeaderProvider = workerws.HeaderProviderFunc(func(context.Context) (http.Header, error) {
		providerCalls.Add(1)
		return http.Header{"Authorization": []string{"Bearer " + credential.Load().(string)}}, nil
	})
	client, err := workerws.NewClient(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer shutdownClientTestClient(t, client)

	first := endpoint.connection(t)
	var firstSocket net.Conn
	select {
	case firstSocket = <-hijacked:
	case <-time.After(3 * time.Second):
		t.Fatal("worker WebSocket was not observed as a hijacked HTTP connection")
	}
	credential.Store("worker-token-2")
	// A deployment proxy or listener restart drops the socket without a
	// protocol close frame. The durable client must redial and ask the provider
	// for a fresh short-lived credential rather than replaying stale headers.
	if err := firstSocket.Close(); err != nil {
		t.Fatal(err)
	}
	second := endpoint.connection(t)
	if second.principal.WorkerID != "worker-a" || second.principal.SessionID == first.principal.SessionID {
		t.Fatalf("rotated reconnect principal = %#v, first = %#v", second.principal, first.principal)
	}
	if providerCalls.Load() < 2 {
		t.Fatalf("header provider calls = %d, want at least two", providerCalls.Load())
	}
}

func TestClientReconnectsAcrossHostManagedTLSCertificateRotation(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	endpoint := newClientTestEndpoint()
	transport, err := workerws.New(workerws.Config{
		GatewayID: "gateway-a",
		Authenticator: workerws.AuthenticateFunc(func(context.Context, *http.Request) (workerws.Identity, error) {
			return workerws.Identity{Worker: "worker-a"}, nil
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

	firstCertificate, firstRoots := clientTestTLSIdentity(t, 1)
	secondCertificate, secondRoots := clientTestTLSIdentity(t, 2)
	var activeCertificate atomic.Value
	activeCertificate.Store(firstCertificate)
	var activeRoots atomic.Value
	activeRoots.Store(firstRoots)
	var handshakes atomic.Int64
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			handshakes.Add(1)
			certificate := activeCertificate.Load().(tls.Certificate)
			return &certificate, nil
		},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hijacked := make(chan net.Conn, 4)
	httpServer := &http.Server{Handler: transport, ConnState: func(connection net.Conn, state http.ConnState) {
		if state == http.StateHijacked {
			hijacked <- connection
		}
	}}
	go func() { _ = httpServer.Serve(tls.NewListener(listener, tlsConfig)) }()
	defer httpServer.Close()

	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: time.Second}}
	httpTransport := &http.Transport{
		DialTLSContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			clone := *dialer
			clone.Config = &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    activeRoots.Load().(*x509.CertPool),
			}
			return clone.DialContext(ctx, network, address)
		},
	}
	defer httpTransport.CloseIdleConnections()
	config := clientTestConfig(
		"wss://"+listener.Addr().String(), framework.Borrow[workerws.Journal](journal),
	)
	config.HTTPClient = &http.Client{Transport: httpTransport}
	client, err := workerws.NewClient(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer shutdownClientTestClient(t, client)

	first := endpoint.connection(t)
	var firstSocket net.Conn
	select {
	case firstSocket = <-hijacked:
	case <-time.After(3 * time.Second):
		t.Fatal("first WSS connection was not observed")
	}
	// Rotate both the served certificate and the host-owned trust bundle, then
	// emulate a proxy rolling restart. The durable client must reconnect using
	// its injected HTTP transport; HumanLLM itself never owns certificate files.
	activeCertificate.Store(secondCertificate)
	activeRoots.Store(secondRoots)
	if err := firstSocket.Close(); err != nil {
		t.Fatal(err)
	}
	second := endpoint.connection(t)
	if second.principal.SessionID == first.principal.SessionID {
		t.Fatalf("certificate rotation reused worker session %q", second.principal.SessionID)
	}
	if handshakes.Load() < 2 {
		t.Fatalf("TLS handshakes = %d, want at least two", handshakes.Load())
	}
}

func TestClientUsesInjectedReconnectBackoff(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	attempts := make(chan uint32, 8)
	client, err := workerws.NewClient(t.Context(), workerws.ClientConfig{
		URL:     "ws" + strings.TrimPrefix(server.URL, "http"),
		Gateway: "gateway-a", Worker: "worker-a", Journal: framework.Borrow[workerws.Journal](journal),
		ConnectTimeout: time.Second, WriteTimeout: time.Second,
		ReconnectMinDelay: time.Millisecond, ReconnectMaxDelay: 20 * time.Millisecond,
		ReconnectResetAfter: time.Second,
		Backoff: workerws.ReconnectBackoffFunc(func(attempt uint32) time.Duration {
			select {
			case attempts <- attempt:
			default:
			}
			return time.Millisecond
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for want := uint32(1); want <= 3; want++ {
		select {
		case got := <-attempts:
			if got != want {
				t.Fatalf("backoff attempt = %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("backoff attempt %d was not observed", want)
		}
	}
	shutdownClientTestClient(t, client)
}

func TestClientConnectionConflictIsTerminalWithoutReconnectStorm(t *testing.T) {
	journal, _ := humantest.NewMemoryLLMWorkerJournal()
	var attempts atomic.Int64
	transport, err := workerws.New(workerws.Config{
		GatewayID: "gateway-a",
		Authenticator: workerws.AuthenticateFunc(func(context.Context, *http.Request) (workerws.Identity, error) {
			attempts.Add(1)
			return workerws.Identity{Worker: "worker-a"}, nil
		}),
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), rejectingClientTestEndpoint{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(transport)
	defer server.Close()
	defer runtime.Shutdown(context.Background())
	client := newClientTestClient(t, server.URL, journal)
	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection conflict did not terminate the duplicate worker")
	}
	if !errors.Is(client.Err(), workerws.ErrClientConnectionConflict) {
		t.Fatalf("client error = %v", client.Err())
	}
	time.Sleep(30 * time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatalf("connection attempts = %d, want 1", attempts.Load())
	}
}

type clientTestEndpoint struct {
	connections chan *clientTestConnection
	mu          sync.Mutex
	decision    func(llm.WorkerEventDelivery) (llm.WorkerEventReceipt, error)
	ackHook     func(llm.WorkerDeliveryID) error
}

type rejectingClientTestEndpoint struct{}

func (rejectingClientTestEndpoint) OpenWorker(
	context.Context,
	llm.AuthenticatedWorker,
) (llm.WorkerConnection, error) {
	return nil, llm.ErrWorkerConnectionConflict
}

func newClientTestEndpoint() *clientTestEndpoint {
	return &clientTestEndpoint{connections: make(chan *clientTestConnection, 64)}
}

func (endpoint *clientTestEndpoint) OpenWorker(
	_ context.Context,
	principal llm.AuthenticatedWorker,
) (llm.WorkerConnection, error) {
	connection := &clientTestConnection{
		principal: principal, endpoint: endpoint,
		assignments:    make(chan llm.WorkerAssignmentDelivery, 16),
		assignmentACKs: make(chan llm.WorkerDeliveryID, 16),
		events:         make(chan llm.WorkerEventDelivery, 128), done: make(chan struct{}),
	}
	endpoint.connections <- connection
	return connection, nil
}

func (endpoint *clientTestEndpoint) connection(t *testing.T) *clientTestConnection {
	t.Helper()
	select {
	case connection := <-endpoint.connections:
		return connection
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not connect")
		return nil
	}
}

type clientTestConnection struct {
	principal      llm.AuthenticatedWorker
	endpoint       *clientTestEndpoint
	assignments    chan llm.WorkerAssignmentDelivery
	assignmentACKs chan llm.WorkerDeliveryID
	events         chan llm.WorkerEventDelivery
	done           chan struct{}
	once           sync.Once
}

func (connection *clientTestConnection) Principal() llm.AuthenticatedWorker {
	return connection.principal
}
func (connection *clientTestConnection) Assignments() <-chan llm.WorkerAssignmentDelivery {
	return connection.assignments
}
func (connection *clientTestConnection) AckAssignment(_ context.Context, id llm.WorkerDeliveryID) error {
	connection.endpoint.mu.Lock()
	hook := connection.endpoint.ackHook
	connection.endpoint.mu.Unlock()
	if hook != nil {
		if err := hook(id); err != nil {
			return err
		}
	}
	connection.assignmentACKs <- id
	return nil
}
func (connection *clientTestConnection) CommitEvent(
	_ context.Context,
	delivery llm.WorkerEventDelivery,
) (llm.WorkerEventReceipt, error) {
	connection.events <- llm.CloneWorkerEventDelivery(delivery)
	connection.endpoint.mu.Lock()
	decision := connection.endpoint.decision
	connection.endpoint.mu.Unlock()
	if decision != nil {
		return decision(delivery)
	}
	return clientTestACK(delivery), nil
}
func (connection *clientTestConnection) Done() <-chan struct{} { return connection.done }
func (*clientTestConnection) Err() error                       { return nil }
func (connection *clientTestConnection) Shutdown(context.Context) error {
	connection.once.Do(func() { close(connection.done) })
	return nil
}

func startClientTestGateway(
	t *testing.T,
	endpoint llm.WorkerEndpoint,
) (*httptest.Server, llm.WorkerTransportRuntime) {
	t.Helper()
	transport, err := workerws.New(workerws.Config{
		GatewayID: "gateway-a",
		Authenticator: workerws.AuthenticateFunc(func(context.Context, *http.Request) (workerws.Identity, error) {
			return workerws.Identity{Worker: "worker-a"}, nil
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
	return httptest.NewServer(transport), runtime
}

func newClientTestClient(t *testing.T, serverURL string, journal workerws.Journal) *workerws.Client {
	t.Helper()
	return newClientTestClientURL(t, "ws"+strings.TrimPrefix(serverURL, "http"), journal)
}

func newClientTestClientURL(t *testing.T, target string, journal workerws.Journal) *workerws.Client {
	t.Helper()
	client, err := workerws.NewClient(t.Context(), clientTestConfig(target, framework.Borrow(journal)))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func clientTestConfig(target string, journal framework.Resource[workerws.Journal]) workerws.ClientConfig {
	return workerws.ClientConfig{
		URL: target, Gateway: "gateway-a", Worker: "worker-a", Journal: journal,
		ConnectTimeout: 250 * time.Millisecond, WriteTimeout: time.Second,
		ReconnectMinDelay: 5 * time.Millisecond, ReconnectMaxDelay: 25 * time.Millisecond,
		ReconnectResetAfter: time.Second,
	}
}

func clientTestTLSIdentity(t *testing.T, serial int64) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	encoded, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(encoded)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{
		Certificate: [][]byte{encoded}, PrivateKey: privateKey, Leaf: leaf,
	}, pool
}

func shutdownClientTestClient(t *testing.T, client *workerws.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown client: %v", err)
	}
}

func clientTestAssignment(id llm.WorkerDeliveryID) llm.WorkerAssignmentDelivery {
	return llm.WorkerAssignmentDelivery{
		ID: id,
		Assignment: llm.Assignment{
			Identity: clientTestIdentity(), Lease: llm.WorkerLease{ID: "lease-a", Owner: "worker-a"},
			Boundary: llm.AssignmentAfterResponse,
			Task: llm.TaskContext{
				TaskID: "task-a", WorkspaceKey: "workspace-a", CapabilityTier: llm.TierWorkspace,
				HarnessID: "harness-a", HarnessVersion: "1", HarnessSessionID: "session-a",
			},
			Request: llm.Request{
				Model: "human", Stream: true,
				Messages: []llm.Message{{Role: llm.RoleUser, Blocks: []llm.Block{{Type: llm.BlockText, Text: "Help me."}}}},
			},
		},
	}
}

func clientTestEvent(id llm.WorkerDeliveryID) llm.WorkerEventDelivery {
	return llm.WorkerEventDelivery{
		ID: id, Identity: clientTestIdentity(), LeaseID: "lease-a",
		Event: llm.Event{ID: "event-" + string(id), Type: llm.EventProgress, Text: "working"},
	}
}

func clientTestIdentity() llm.CompletionIdentity {
	return llm.CompletionIdentity{
		CallerID: "caller-a", RequestID: "request-a", TaskID: "task-a",
		WorkspaceKey: "workspace-a", IdempotencyKey: "key-a",
	}
}

func clientTestACK(delivery llm.WorkerEventDelivery) llm.WorkerEventReceipt {
	return llm.WorkerEventReceipt{
		Delivery: delivery.ID, EventID: delivery.Event.ID, Decision: llm.WorkerEventACK,
	}
}

func clientTestDigest(value any) workerws.JournalDigest {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(encoded)
	return workerws.JournalDigest(hex.EncodeToString(digest[:]))
}

func assertClientAssignment(
	t *testing.T,
	channel <-chan llm.WorkerAssignmentDelivery,
	id llm.WorkerDeliveryID,
) {
	t.Helper()
	select {
	case delivery := <-channel:
		if delivery.ID != id {
			t.Fatalf("assignment = %q, want %q", delivery.ID, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("assignment %q was not presented", id)
	}
}

func assertClientDeliveryID(t *testing.T, channel <-chan llm.WorkerDeliveryID, id llm.WorkerDeliveryID) {
	t.Helper()
	select {
	case got := <-channel:
		if got != id {
			t.Fatalf("delivery = %q, want %q", got, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("delivery %q was not acknowledged", id)
	}
}

func assertClientEvent(t *testing.T, channel <-chan llm.WorkerEventDelivery) llm.WorkerEventDelivery {
	t.Helper()
	select {
	case delivery := <-channel:
		return delivery
	case <-time.After(3 * time.Second):
		t.Fatal("event was not delivered")
		return llm.WorkerEventDelivery{}
	}
}

func eventuallyClientTest(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func leftPad(value int) string {
	return fmt.Sprintf("%03d", value)
}

var _ llm.WorkerEndpoint = (*clientTestEndpoint)(nil)
var _ llm.WorkerEndpoint = rejectingClientTestEndpoint{}
var _ llm.WorkerConnection = (*clientTestConnection)(nil)
