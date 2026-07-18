package tui

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerstate"
)

func TestRestartAfterRestoredDraftPutBeforePendingDeleteDoesNotDuplicateSource(t *testing.T) {
	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	_ = New(newFakeClient(), WithStateStore(store))

	assignment := allPaneStateAssignment("restore-materialized-before-delete")
	scope := stateScope(assignment)
	event := completion.Event{
		ID: "event-restore-materialized-before-delete", Type: completion.EventProgress,
		Text: "sent segment\n\n",
	}
	tasks := []agentTask{{Content: "keep task once", Status: taskPending, Priority: "medium"}}
	original := persistedDraft{
		Version: workerStateVersion, Authority: assignmentDraftAuthority(assignment),
		Focus: persistedFocusReply, Reply: "sent segment", Command: "keep command once",
		HasTasks: true, Tasks: persistTasks(tasks), TaskDirty: true,
		TaskEditing: true, TaskEditIndex: 0, TaskInput: "editing once",
		ToolInput: `search {"query":"keep once"}`, ToolCallIDs: []string{"tool-keep-once"},
	}
	putWorkerStatePayload := func(kind string, value any) {
		t.Helper()
		payload, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if _, putErr := store.Put(ctx, scope, kind, payload); putErr != nil {
			t.Fatal(putErr)
		}
	}
	putWorkerStatePayload(workerStateDraftKind, original)

	pending := pendingSend{
		kind: pendingReply, assignment: assignment, reply: "sent segment", event: event,
		remainingDraft: persistedDraft{
			Version: workerStateVersion, Authority: assignmentDraftAuthority(assignment),
			Focus: persistedFocusCommand, Command: "keep command once",
			HasTasks: true, Tasks: persistTasks(tasks), TaskDirty: true,
			TaskEditing: true, TaskEditIndex: 0, TaskInput: "editing once",
			ToolInput: `search {"query":"keep once"}`, ToolCallIDs: []string{"tool-keep-once"},
		},
	}
	putWorkerStatePayload(workerStatePendingSendKind, persistedPendingFromSend(pending, pendingSendDispositionSend))
	putWorkerStatePayload(workerStatePendingSendKind, persistedPendingFromSend(pending, pendingSendDispositionRestore))

	// This is the exact crash window: the UI has materialized the restore into a
	// complete draft row, but the following pending-journal Delete has not run.
	materialized := original
	materialized.Authority = mergeDraftAuthorities(
		eventDraftAuthority(event.ID), assignmentDraftAuthority(assignment),
	)
	materialized.RejectedEventIDs = []string{event.ID}
	materialized.RejectedEventKinds = map[string]pendingSendKind{event.ID: pendingReply}
	materialized.Reply = "sent segment\n\nnew tail"
	materialized.ReplyRejected = "sent segment"
	materialized.ReplyTail = "new tail"
	putWorkerStatePayload(workerStateDraftKind, materialized)

	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var sawDraft, sawRestorePending bool
	for _, record := range records {
		switch record.Kind {
		case workerStateDraftKind:
			sawDraft = true
		case workerStatePendingSendKind:
			var saved persistedPendingSend
			if err := json.Unmarshal(record.Payload, &saved); err != nil {
				t.Fatal(err)
			}
			sawRestorePending = saved.Disposition == pendingSendDispositionRestore
		}
	}
	if !sawDraft || !sawRestorePending {
		t.Fatalf("crash fixture lacks materialized draft or restore journal: %+v", records)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	restarted := New(newFakeClient(), WithStateStore(store)).activateAssignment(assignment)

	if restarted.replyInput != materialized.Reply ||
		strings.Count(restarted.replyInput, "sent segment") != 1 ||
		strings.Count(restarted.replyInput, "new tail") != 1 {
		t.Fatalf("restored reply source/tail = %q, want each segment exactly once", restarted.replyInput)
	}
	if restarted.commandInput != materialized.Command ||
		strings.Count(restarted.commandInput, "keep command once") != 1 {
		t.Fatalf("restored command = %q", restarted.commandInput)
	}
	if !reflect.DeepEqual(restarted.agentTasks, tasks) || !restarted.taskDirty ||
		!restarted.taskEditing || restarted.taskEditIndex != 0 || restarted.taskInput != "editing once" {
		t.Fatalf("restored task pane = tasks=%+v dirty=%t editing=%t index=%d input=%q",
			restarted.agentTasks, restarted.taskDirty, restarted.taskEditing,
			restarted.taskEditIndex, restarted.taskInput)
	}
	if !restarted.composing || restarted.input != materialized.ToolInput ||
		!reflect.DeepEqual(restarted.toolCallIDs, materialized.ToolCallIDs) {
		t.Fatalf("restored advanced pane = composing=%t input=%q ids=%v",
			restarted.composing, restarted.input, restarted.toolCallIDs)
	}
}
