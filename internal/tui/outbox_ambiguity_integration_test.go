package tui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerproto"
	"github.com/vibe-agi/human/internal/workerstate"
)

type outboxObservedEvent struct {
	connection int32
	message    workerproto.Event
}

func TestRecoveredPendingKeepsAssignmentWhenExactEventMayAlreadyBeInDurableOutbox(t *testing.T) {
	const (
		workerID = "worker-outbox-ambiguity"
		token    = "worker-token"
		scope    = "test:outbox-ambiguity"
	)
	observed := make(chan outboxObservedEvent, 8)
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(response, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		generation := connections.Add(1)
		hello, err := workerproto.NewEnvelope(
			workerproto.MessageHello,
			workerproto.Hello{WorkerID: workerID},
		)
		if err != nil {
			return
		}
		hello.Seq = 1
		if err := wsjson.Write(request.Context(), connection, hello); err != nil {
			return
		}
		for {
			var envelope workerproto.Envelope
			if err := wsjson.Read(request.Context(), connection, &envelope); err != nil {
				return
			}
			if envelope.Type != workerproto.MessageEvent {
				continue
			}
			var event workerproto.Event
			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				return
			}
			observed <- outboxObservedEvent{connection: generation, message: event}
			// Deliberately send no cumulative ACK. The event remains in the durable
			// outbox, modeling a crash after its Put committed but before ACK became
			// observable to either the worker client or Bubble Tea.
		}
	}))
	t.Cleanup(server.Close)
	workerURL := "ws" + strings.TrimPrefix(server.URL, "http")
	outboxPath := filepath.Join(t.TempDir(), "worker-outbox.db")
	statePath := filepath.Join(t.TempDir(), "state", "worker-state.db")

	assignmentA1 := allPaneStateAssignment("outbox-ambiguity-request")
	assignmentA1.LeaseOwner = workerID
	assignmentA1.HarnessSessionID = "assignment-A1"
	event := completion.Event{
		ID: "event-outbox-ambiguity", Type: completion.EventFinal, Text: "stable final",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := workerclient.DialWithOutboxScope(ctx, workerURL, token, outboxPath, scope)
	if err != nil {
		t.Fatal(err)
	}
	waitForWorkerIdentity(t, ctx, first, workerID)
	if err := first.SendEvent(ctx, assignmentA1, event); err != nil {
		t.Fatal(err)
	}
	assertObservedOutboxEvent(t, ctx, observed, 1, assignmentA1, event)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	gatewayID, err := workerclient.GatewayIdentity(workerURL, scope)
	if err != nil {
		t.Fatal(err)
	}
	stateStore, err := workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := stateStore.Bind(ctx, gatewayID, workerID); err != nil {
		t.Fatal(err)
	}
	pending := pendingSend{
		kind: pendingReply, assignment: assignmentA1, reply: event.Text, event: event,
		remainingDraft: persistedDraft{Version: workerStateVersion, TaskEditIndex: -1},
	}
	payload, err := json.Marshal(persistedPendingFromSend(pending, pendingSendDispositionSend))
	if err != nil {
		t.Fatal(err)
	}
	key := pendingSendStateKey(pending)
	if _, err := stateStore.Put(ctx, key.scope, key.kind, payload); err != nil {
		t.Fatal(err)
	}
	if err := stateStore.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := workerclient.DialWithOutboxScope(ctx, workerURL, token, outboxPath, scope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	waitForWorkerIdentity(t, ctx, second, workerID)
	// This is emitted by automatic startup flush, before the TUI is constructed
	// or asked to resume its recovered pending row. It proves A1 really survived
	// in the worker client's durable outbox across the simulated crash.
	assertObservedOutboxEvent(t, ctx, observed, 2, assignmentA1, event)

	stateStore, err = workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stateStore.Close() })
	model := New(second, WithStateStore(stateStore))
	if !model.pending.recovered || !model.pending.durable ||
		!samePersistedAssignment(model.pending.assignment, assignmentA1) || model.pending.event.ID != event.ID {
		t.Fatalf("pending A1 was not recovered exactly: %+v", model.pending)
	}

	assignmentA2 := assignmentA1
	assignmentA2.HarnessSessionID = "assignment-A2"
	updated, _ := model.Update(networkMessage{Assignment: &assignmentA2})
	model = updated.(Model)
	if !samePersistedAssignment(model.pending.assignment, assignmentA1) || !model.pending.durable {
		t.Fatalf("replayed A2 rewrote crash-ambiguous pending A1: %+v", model.pending)
	}

	updated, resend := model.Update(resumeDurablePendingRequest{eventID: event.ID})
	model = updated.(Model)
	if resend == nil || !model.pending.outboxInFlight {
		t.Fatalf("recovered A1 did not enter the durable outbox handoff: %+v", model.pending)
	}
	result, ok := resend().(eventSent)
	if !ok || result.err != nil {
		t.Fatalf("exact A1 resend conflicted with its existing durable outbox row: %#v", result)
	}
	if !samePersistedAssignment(model.pending.assignment, assignmentA1) {
		t.Fatalf("successful idempotent resend changed the frozen assignment: %+v", model.pending.assignment)
	}
}

func waitForWorkerIdentity(
	t *testing.T,
	ctx context.Context,
	client *workerclient.Client,
	want string,
) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for client.WorkerID() != want {
		select {
		case <-ctx.Done():
			t.Fatalf("worker identity = %q, want %q: %v", client.WorkerID(), want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertObservedOutboxEvent(
	t *testing.T,
	ctx context.Context,
	observed <-chan outboxObservedEvent,
	wantConnection int32,
	assignment completion.Assignment,
	event completion.Event,
) {
	t.Helper()
	for {
		select {
		case got := <-observed:
			if got.connection != wantConnection {
				continue
			}
			if got.message.CallerID != assignment.CallerID ||
				got.message.IdempotencyKey != assignment.IdempotencyKey ||
				got.message.Event.ID != event.ID || got.message.Event.Type != event.Type ||
				got.message.Event.Text != event.Text {
				t.Fatalf("connection %d outbox event = %+v", wantConnection, got.message)
			}
			return
		case <-ctx.Done():
			t.Fatalf("connection %d did not flush durable event: %v", wantConnection, ctx.Err())
		}
	}
}
