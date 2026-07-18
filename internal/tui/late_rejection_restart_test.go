package tui

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerproto"
	"github.com/vibe-agi/human/internal/workerstate"
)

func TestLateAcceptedRejectionPreservesEveryDraftAcrossWorkerStateRestart(t *testing.T) {
	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}

	client := newFakeClient()
	assignment := allPaneStateAssignment("late-accepted-rejection")
	model, accepted := acceptLateRejectionSource(t, New(client, WithStateStore(store)), client, assignment)
	want := installLateRejectionDraft(&model, "accepted")
	model = flushWorkerState(t, model)

	updated, finalization := model.Update(durableLateRejection(assignment, accepted))
	model = updated.(Model)
	model = finishLateRejectionFinalization(t, model, client, accepted.ID, finalization)
	if model.active != nil {
		t.Fatalf("late Accepted rejection left the rejected request active: %+v", model)
	}
	assertLateRejectionDraft(t, model, want)
	if !reflect.DeepEqual(model.toolCallIDs, want.toolCallIDs) {
		t.Fatalf("late Accepted rejection changed advanced IDs before restart: got %v want %v",
			model.toolCallIDs, want.toolCallIDs)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	replayClient := newFakeClient()
	restarted, _ := acceptLateRejectionSource(
		t, New(replayClient, WithStateStore(store)), replayClient, assignment,
	)
	assertLateRejectionDraft(t, restarted, want)
	if !reflect.DeepEqual(restarted.toolCallIDs, want.toolCallIDs) {
		t.Fatalf("restart regenerated or lost advanced IDs: got %v want %v",
			restarted.toolCallIDs, want.toolCallIDs)
	}
}

func TestLateProgressRejectionMergesSourceWithNewDraftsAcrossWorkerStateRestart(t *testing.T) {
	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}

	client := newFakeClient()
	assignment := allPaneStateAssignment("late-progress-rejection")
	model, _ := acceptLateRejectionSource(t, New(client, WithStateStore(store)), client, assignment)
	const rejectedReply = "progress already handed to the local outbox"
	model.setReplyValue(rejectedReply)
	model = flushWorkerState(t, model)
	model, progress := commitDurableProgressForLateRejection(t, model, client)

	want := installLateRejectionDraft(&model, "newer")
	want.reply = rejectedReply + "\n\n" + want.reply
	model = flushWorkerState(t, model)

	updated, finalization := model.Update(durableLateRejection(assignment, progress))
	model = updated.(Model)
	model = finishLateRejectionFinalization(t, model, client, progress.ID, finalization)
	if model.active != nil {
		t.Fatalf("late Progress rejection left the expired request active: %+v", model)
	}
	assertLateRejectionDraft(t, model, want)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	replayClient := newFakeClient()
	restarted, _ := acceptLateRejectionSource(
		t, New(replayClient, WithStateStore(store)), replayClient, assignment,
	)
	assertLateRejectionDraft(t, restarted, want)
	if !reflect.DeepEqual(restarted.toolCallIDs, want.toolCallIDs) {
		t.Fatalf("late Progress restart changed newer advanced IDs: got %v want %v",
			restarted.toolCallIDs, want.toolCallIDs)
	}
}

type lateRejectionDraft struct {
	reply         string
	command       string
	tasks         []agentTask
	selected      int
	taskEditing   bool
	taskEditIndex int
	taskInput     string
	toolInput     string
	toolCallIDs   []string
}

func installLateRejectionDraft(model *Model, prefix string) lateRejectionDraft {
	draft := lateRejectionDraft{
		reply:   prefix + " unsent reply",
		command: "go test ./... # " + prefix,
		tasks: []agentTask{
			{Content: prefix + " task in progress", Status: taskInProgress, Priority: "high"},
			{Content: prefix + " queued task", Status: taskPending, Priority: "medium"},
		},
		selected:      1,
		taskEditing:   true,
		taskEditIndex: 1,
		taskInput:     prefix + " queued task with an unsaved edit",
		toolInput:     `search {"query":"` + prefix + ` advanced draft"}`,
		toolCallIDs:   []string{"tool-" + prefix + "-stable"},
	}
	model.setReplyValue(draft.reply)
	model.setCommandValue(draft.command)
	model.agentTasks = append([]agentTask(nil), draft.tasks...)
	model.taskSelected = draft.selected
	model.taskDirty = true
	model.taskEditing = draft.taskEditing
	model.taskEditIndex = draft.taskEditIndex
	model.taskInput = draft.taskInput
	model.composing = true
	model.input = draft.toolInput
	model.toolCallIDs = append([]string(nil), draft.toolCallIDs...)
	model.focus = focusTasks
	return draft
}

func assertLateRejectionDraft(t *testing.T, model Model, want lateRejectionDraft) {
	t.Helper()
	if model.replyInput != want.reply || model.commandInput != want.command ||
		!reflect.DeepEqual(model.agentTasks, want.tasks) || model.taskSelected != want.selected ||
		!model.taskDirty || model.taskEditing != want.taskEditing ||
		model.taskEditIndex != want.taskEditIndex || model.taskInput != want.taskInput ||
		!model.composing || model.input != want.toolInput ||
		!reflect.DeepEqual(model.toolCallIDs, want.toolCallIDs) {
		t.Fatalf("late rejection draft mismatch: got reply=%q command=%q tasks=%+v selected=%d "+
			"dirty=%t editing=%t editIndex=%d taskInput=%q composing=%t toolInput=%q ids=%v; want %+v",
			model.replyInput, model.commandInput, model.agentTasks, model.taskSelected,
			model.taskDirty, model.taskEditing, model.taskEditIndex, model.taskInput,
			model.composing, model.input, model.toolCallIDs, want)
	}
}

func acceptLateRejectionSource(
	t *testing.T,
	model Model,
	client *fakeClient,
	assignment completion.Assignment,
) (Model, completion.Event) {
	t.Helper()
	updated, _ := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	updated, send := model.acceptSelected()
	model = updated.(Model)
	if send == nil || model.pending.kind != pendingAccept {
		t.Fatalf("source assignment was not staged for acceptance: %+v", model)
	}
	message, ok := send().(eventSent)
	if !ok || message.err != nil {
		t.Fatalf("Accepted local outbox result = %#v", message)
	}
	if len(client.events) == 0 {
		t.Fatal("Accepted event did not reach the local outbox client")
	}
	accepted := client.events[len(client.events)-1]
	if accepted.ID != message.eventID || accepted.Type != completion.EventAccepted {
		t.Fatalf("Accepted event = %+v, acknowledgement = %+v", accepted, message)
	}
	updated, trailing := model.Update(message)
	model = updated.(Model)
	model = settleTrailingStateWrites(t, model, trailing)
	if model.active == nil || model.active.SessionKey() != assignment.SessionKey() ||
		model.pending.kind != pendingNone {
		t.Fatalf("Accepted success did not activate its exact assignment: %+v", model)
	}
	return model, accepted
}

func commitDurableProgressForLateRejection(
	t *testing.T,
	model Model,
	client *fakeClient,
) (Model, completion.Event) {
	t.Helper()
	updated, _ := model.sendReply(completion.EventProgress, false)
	model = updated.(Model)
	if model.pending.kind != pendingReply || model.pending.event.Type != completion.EventProgress {
		t.Fatalf("Progress was not staged as a durable reply: %+v", model.pending)
	}
	updated, stateWrite := model.Update(tea.WindowSizeMsg{Width: model.width, Height: model.height})
	model = updated.(Model)
	stateResult := workerStateResult(t, stateWrite)
	updated, outboxWrite := model.Update(stateResult)
	model = updated.(Model)
	outboxMessages := executeTestCommand(outboxWrite)
	ack, acknowledged := firstTestMessage[eventSent](outboxMessages)
	if !acknowledged || ack.err != nil || len(client.events) == 0 {
		t.Fatalf("Progress local outbox result = %+v events=%+v", ack, client.events)
	}
	// The pending-intent commit may start cleanup of the older whole-editor
	// snapshot in the same Batch as the outbox Put. Reduce that real SQLite
	// result before the event acknowledgement so stateWriting cannot remain as
	// a process-local ghost after both operations have actually committed.
	if cleanup, exists := firstTestMessage[stateWriteResult](outboxMessages); exists {
		updated, trailing := model.Update(cleanup)
		model = updated.(Model)
		model = settleTrailingStateWrites(t, model, trailing)
	}
	progress := client.events[len(client.events)-1]
	if progress.ID != ack.eventID || progress.Type != completion.EventProgress {
		t.Fatalf("Progress event = %+v, acknowledgement = %+v", progress, ack)
	}
	updated, trailing := model.Update(ack)
	model = updated.(Model)
	model = settleTrailingStateWrites(t, model, trailing)
	if model.active == nil || model.pending.kind != pendingNone {
		t.Fatalf("successful Progress did not leave its stream active: %+v", model)
	}
	return model, progress
}

func durableLateRejection(
	assignment completion.Assignment,
	event completion.Event,
) networkMessage {
	return networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: event.ID, Message: "completion session expired after local outbox success",
		},
		RejectedEvent:      &event,
		RejectedAssignment: &assignment,
	}
}

func finishLateRejectionFinalization(
	t *testing.T,
	model Model,
	client *fakeClient,
	eventID string,
	command tea.Cmd,
) Model {
	t.Helper()
	for steps := 0; steps < 100; steps++ {
		if model.stateWriting {
			result := trailingStateWriteResult(t, command)
			updated, next := model.Update(result)
			model = updated.(Model)
			command = next
			continue
		}
		finalizer, exists := model.rejectionFinalizers[eventID]
		if !exists {
			if len(client.confirmed) != 1 || client.confirmed[0] != eventID {
				t.Fatalf("rejected inbox confirmations = %v, want exactly %q", client.confirmed, eventID)
			}
			return model
		}
		if !finalizer.cleanupDone || !finalizer.pendingDeleted ||
			finalizer.mirrorConfirmationPending || finalizer.cleanupInFlight {
			t.Fatalf("non-mirror late rejection did not reach a confirmable finalizer: %+v", finalizer)
		}
		if !finalizer.confirming {
			command = model.advanceRejectionFinalization(eventID)
			finalizer = model.rejectionFinalizers[eventID]
			if !finalizer.confirming {
				t.Fatalf("late rejection finalizer remained blocked after state sync: %+v", finalizer)
			}
		}
		confirmation, ok := confirmRejectedEvent(client, eventID)().(rejectedEventConfirmed)
		if !ok || confirmation.err != nil {
			t.Fatalf("rejected inbox confirmation = %#v", confirmation)
		}
		updated, next := model.Update(confirmation)
		model = updated.(Model)
		command = next
	}
	t.Fatal("late rejection finalization did not quiesce")
	return model
}
