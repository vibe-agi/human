package tui

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerproto"
	"github.com/vibe-agi/human/internal/workerstate"
)

// A rejected event remains in workerclient's durable inbox until the TUI has
// committed both the restored draft and pending-journal deletion. If the
// process crashes in the narrow window before ConfirmRejectedEvent commits,
// the inbox must replay E without appending E to the already-materialized
// draft a second time.
func TestRejectedInboxReplayAfterMaterializationBeforeConfirmationIsExactlyOnce(t *testing.T) {
	const (
		workerID    = "worker-rejection-confirmation-crash"
		token       = "worker-token"
		outboxScope = "test:rejection-confirmation-crash"
	)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	assignment := allPaneStateAssignment("rejection-confirmation-crash")
	assignment.LeaseOwner = workerID
	event := completion.Event{
		ID: "event-rejection-confirmation-crash", Type: completion.EventProgress,
		Text: "rejected durable segment\n\n",
	}

	var rejectionSent atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(response, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		hello, err := workerproto.NewEnvelope(
			workerproto.MessageHello, workerproto.Hello{WorkerID: workerID},
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
			var published workerproto.Event
			if err := json.Unmarshal(envelope.Payload, &published); err != nil {
				return
			}
			if published.Event.ID != event.ID || !rejectionSent.CompareAndSwap(false, true) {
				continue
			}
			rejection, err := workerproto.NewEnvelope(
				workerproto.MessageEventRejected,
				workerproto.EventRejected{
					CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
					EventID: event.ID, Message: "expired at the gateway",
				},
			)
			if err != nil {
				return
			}
			rejection.Seq = 2
			rejection.Ack = envelope.Seq
			if err := wsjson.Write(request.Context(), connection, rejection); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	workerURL := "ws" + strings.TrimPrefix(server.URL, "http")
	outboxPath := filepath.Join(t.TempDir(), "outbox", "worker.db")
	statePath := filepath.Join(t.TempDir(), "state", "worker.db")

	// Move E through the real workerclient transaction from the send outbox into
	// the durable rejected inbox. Merely receiving this notification does not
	// acknowledge or delete its inbox record.
	first, err := workerclient.DialWithOutboxScope(ctx, workerURL, token, outboxPath, outboxScope)
	if err != nil {
		t.Fatal(err)
	}
	waitForWorkerIdentity(t, ctx, first, workerID)
	if err := first.SendEvent(ctx, assignment, event); err != nil {
		t.Fatal(err)
	}
	firstRejection := waitForRejectedInboxEvent(t, ctx, first, event.ID)
	if firstRejection.RejectedEvent == nil || firstRejection.RejectedEvent.Text != event.Text {
		t.Fatalf("first durable rejected event = %+v", firstRejection)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// Exact crash fixture: E and the independent local tail/panes are already
	// materialized in tui_draft_v3, the pending-send row has been deleted, but
	// ConfirmRejectedEvent has not run and the durable inbox still owns E.
	store, err := workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	gatewayID, err := workerclient.GatewayIdentity(workerURL, outboxScope)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Bind(ctx, gatewayID, workerID); err != nil {
		t.Fatal(err)
	}
	tasks := []agentTask{
		{Content: "keep task in progress", Status: taskInProgress, Priority: "high"},
		{Content: "keep task edit", Status: taskPending, Priority: "medium"},
	}
	materialized := persistedDraft{
		Version: workerStateVersion,
		Authority: mergeDraftAuthorities(
			eventDraftAuthority(event.ID), assignmentDraftAuthority(assignment),
		),
		RejectedEventIDs:   []string{event.ID},
		RejectedEventKinds: map[string]pendingSendKind{event.ID: pendingReply},
		Focus:              persistedFocusTasks,
		Reply:              "rejected durable segment\n\nindependent reply tail",
		ReplyRejected:      "rejected durable segment",
		ReplyTail:          "independent reply tail",
		Command:            "go test ./... # independent command",
		HasTasks:           true,
		Tasks:              persistTasks(tasks),
		TaskSelected:       1,
		TaskDirty:          true,
		TaskEditing:        true,
		TaskEditIndex:      1,
		TaskInput:          "keep task edit with unsaved text",
		ToolInput:          `search {"query":"independent advanced draft"}`,
		ToolCallIDs:        []string{"tool-independent-stable"},
	}
	payload, err := json.Marshal(materialized)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, stateScope(assignment), workerStateDraftKind, payload); err != nil {
		t.Fatal(err)
	}
	assertMaterializedRejectionRows(t, ctx, store, assignment, event.ID, materialized)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart before confirmation. The real rejected inbox reoffers E; reducing
	// it through Model.Update must recognize RejectedEventIDs and retain exactly
	// one source segment plus every independent pane.
	second, err := workerclient.DialWithOutboxScope(ctx, workerURL, token, outboxPath, outboxScope)
	if err != nil {
		t.Fatal(err)
	}
	waitForWorkerIdentity(t, ctx, second, workerID)
	store, err = workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	model := New(second, WithStateStore(store))
	replayed := waitForRejectedInboxEvent(t, ctx, second, event.ID)
	updated, command := model.Update(networkMessage(replayed))
	model = updated.(Model)
	model, command = settleRejectionBeforeConfirmation(t, model, command)
	assertMaterializedDraftModel(t, model, event.ID, materialized, tasks)
	assertMaterializedRejectionRows(t, ctx, store, assignment, event.ID, materialized)
	if finalizer, exists := model.rejectionFinalizers[event.ID]; !exists || !finalizer.confirming {
		t.Fatalf("replayed rejection did not stop at the confirmation barrier: %+v", finalizer)
	}

	confirmation, ok := confirmRejectedEvent(second, event.ID)().(rejectedEventConfirmed)
	if !ok || confirmation.err != nil {
		t.Fatalf("real rejected inbox confirmation = %#v", confirmation)
	}
	updated, trailing := model.Update(confirmation)
	model = updated.(Model)
	model = settleTrailingStateWrites(t, model, trailing)
	if _, exists := model.rejectionFinalizers[event.ID]; exists {
		t.Fatalf("successful confirmation left a live finalizer: %+v", model.rejectionFinalizers[event.ID])
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}

	// A second restart proves both authorities: the TUI draft still contains E
	// exactly once, while workerclient retained only the payload-free rejection
	// tombstone and therefore cannot replay the inbox item or resend E.
	third, err := workerclient.DialWithOutboxScope(ctx, workerURL, token, outboxPath, outboxScope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = third.Close() })
	waitForWorkerIdentity(t, ctx, third, workerID)
	store, err = workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	finalModel := New(third, WithStateStore(store)).activateAssignment(assignment)
	assertMaterializedDraftModel(t, finalModel, event.ID, materialized, tasks)
	if err := third.SendEvent(ctx, assignment, event); !errors.Is(err, workerclient.ErrEventPreviouslyRejected) {
		t.Fatalf("confirmed E did not reopen as a rejection tombstone: %v", err)
	}
	assertNoRejectedInboxEvent(t, third, event.ID, 150*time.Millisecond)
}

func assertNoRejectedInboxEvent(
	t *testing.T,
	client *workerclient.Client,
	eventID string,
	duration time.Duration,
) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case message, open := <-client.Messages():
			if !open {
				t.Fatal("worker client closed while checking the confirmed rejected inbox")
			}
			if message.EventRejected != nil && message.EventRejected.EventID == eventID {
				t.Fatalf("confirmed durable inbox item replayed after restart: %+v", message)
			}
		case <-timer.C:
			return
		}
	}
}

func waitForRejectedInboxEvent(
	t *testing.T,
	ctx context.Context,
	client *workerclient.Client,
	eventID string,
) workerclient.Message {
	t.Helper()
	for {
		select {
		case message, open := <-client.Messages():
			if !open {
				t.Fatalf("worker client closed before rejected inbox event %q", eventID)
			}
			if message.EventRejected != nil && message.EventRejected.EventID == eventID {
				return message
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for rejected inbox event %q: %v", eventID, ctx.Err())
		}
	}
}

func settleRejectionBeforeConfirmation(
	t *testing.T,
	model Model,
	command tea.Cmd,
) (Model, tea.Cmd) {
	t.Helper()
	for model.stateWriting {
		result := trailingStateWriteResult(t, command)
		updated, next := model.Update(result)
		model = updated.(Model)
		command = next
	}
	return model, command
}

func assertMaterializedRejectionRows(
	t *testing.T,
	ctx context.Context,
	store *workerstate.Store,
	assignment completion.Assignment,
	eventID string,
	want persistedDraft,
) {
	t.Helper()
	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var drafts int
	for _, record := range records {
		if record.Scope != stateScope(assignment) {
			continue
		}
		switch record.Kind {
		case workerStatePendingSendKind:
			t.Fatalf("materialized rejection retained its pending-send row: %+v", record)
		case workerStateDraftKind:
			drafts++
			var got persistedDraft
			if err := json.Unmarshal(record.Payload, &got); err != nil {
				t.Fatal(err)
			}
			if strings.Count(got.Reply, "rejected durable segment") != 1 ||
				strings.Count(got.Reply, "independent reply tail") != 1 ||
				!reflect.DeepEqual(got.RejectedEventIDs, []string{eventID}) ||
				got.RejectedEventKinds[eventID] != pendingReply ||
				got.Command != want.Command || !reflect.DeepEqual(got.Tasks, want.Tasks) ||
				got.TaskEditing != want.TaskEditing || got.TaskInput != want.TaskInput ||
				got.ToolInput != want.ToolInput || !reflect.DeepEqual(got.ToolCallIDs, want.ToolCallIDs) {
				t.Fatalf("materialized rejection row = %+v", got)
			}
		}
	}
	if drafts != 1 {
		t.Fatalf("materialized draft rows = %d, want exactly one; records=%+v", drafts, records)
	}
}

func assertMaterializedDraftModel(
	t *testing.T,
	model Model,
	eventID string,
	want persistedDraft,
	tasks []agentTask,
) {
	t.Helper()
	if strings.Count(model.replyInput, "rejected durable segment") != 1 ||
		strings.Count(model.replyInput, "independent reply tail") != 1 ||
		model.replyInput != want.Reply || model.commandInput != want.Command ||
		!reflect.DeepEqual(model.agentTasks, tasks) || model.taskSelected != want.TaskSelected ||
		!model.taskDirty || !model.taskEditing || model.taskEditIndex != want.TaskEditIndex ||
		model.taskInput != want.TaskInput || !model.composing || model.input != want.ToolInput ||
		!reflect.DeepEqual(model.toolCallIDs, want.ToolCallIDs) ||
		!reflect.DeepEqual(model.draftRejectedEvents, []string{eventID}) ||
		model.draftRejectedKinds[eventID] != pendingReply {
		t.Fatalf("materialized rejection model = reply=%q command=%q tasks=%+v selected=%d "+
			"dirty=%t editing=%t index=%d taskInput=%q composing=%t input=%q ids=%v rejected=%v",
			model.replyInput, model.commandInput, model.agentTasks, model.taskSelected,
			model.taskDirty, model.taskEditing, model.taskEditIndex, model.taskInput,
			model.composing, model.input, model.toolCallIDs, model.draftRejectedEvents)
	}
}
