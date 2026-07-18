package tui

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerstate"
)

func TestLateFinalRejectionMergesSourceAndRemainderWithoutOverwritingUnrelatedEditor(t *testing.T) {
	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}

	client := newFakeClient()
	source := allPaneStateAssignment("same-key-late-final-source")
	model, _ := acceptLateRejectionSource(
		t, New(client, WithStateStore(store)), client, source,
	)
	wantSource := installLateRejectionDraft(&model, "source")
	model = flushWorkerState(t, model)

	model, finalEvent := commitDurableFinalForSameKeyRejection(t, model, client)
	sourceKey := stateRecordKey{scope: stateScope(source), kind: workerStateDraftKind}
	sourceRemainder, exists := model.stateDrafts[sourceKey]
	if !exists {
		t.Fatal("successful local Final handoff did not retain the unsent source remainder")
	}
	if sourceRemainder.draft.Reply != "" {
		t.Fatalf("successful Final remainder retained the already-sent reply: %+v", sourceRemainder.draft)
	}
	assertPersistedSameKeyDraft(t, sourceRemainder.draft, lateRejectionDraft{
		command:       wantSource.command,
		tasks:         wantSource.tasks,
		selected:      wantSource.selected,
		taskEditing:   wantSource.taskEditing,
		taskEditIndex: wantSource.taskEditIndex,
		taskInput:     wantSource.taskInput,
		toolInput:     wantSource.toolInput,
		toolCallIDs:   wantSource.toolCallIDs,
	})

	unrelated := allPaneStateAssignment("same-key-unrelated-active")
	unrelated.CallerID = "caller-unrelated-to-late-final"
	unrelated.WorkspaceKey = "workspace-unrelated-to-late-final"
	unrelated.TaskID = "task-unrelated-to-late-final"
	unrelated.HarnessSessionID = "harness-unrelated-to-late-final"
	unrelated.Root = filepath.Join(t.TempDir(), "unrelated-workspace")
	model, _ = acceptLateRejectionSource(t, model, client, unrelated)
	wantUnrelated := installLateRejectionDraft(&model, "unrelated")
	model = flushWorkerState(t, model)

	updated, finalization := model.Update(durableLateRejection(source, finalEvent))
	model = updated.(Model)
	assertUnrelatedEditorUnchanged(t, model, unrelated, wantUnrelated)
	model = finishLateRejectionFinalization(
		t, model, client, finalEvent.ID, finalization,
	)
	assertUnrelatedEditorUnchanged(t, model, unrelated, wantUnrelated)
	model = flushWorkerState(t, model)

	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var sourceRows int
	var durableSource persistedDraft
	for _, record := range records {
		if record.Scope != stateScope(source) {
			continue
		}
		switch record.Kind {
		case workerStateDraftKind:
			sourceRows++
			if err := json.Unmarshal(record.Payload, &durableSource); err != nil {
				t.Fatalf("decode merged source draft: %v", err)
			}
		case workerStatePendingSendKind:
			t.Fatalf("confirmed late rejection left the source pending journal behind: %+v", record)
		}
	}
	if sourceRows != 1 {
		t.Fatalf("source draft rows = %d, want one merged authority; records=%+v", sourceRows, records)
	}
	assertPersistedSameKeyDraft(t, durableSource, wantSource)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	restarted := New(newFakeClient(), WithStateStore(store))
	restartedSource, exists := restarted.stateDrafts[sourceKey]
	if !exists {
		t.Fatalf("restart lost the merged rejected source draft: %+v", restarted.stateDrafts)
	}
	assertPersistedSameKeyDraft(t, restartedSource.draft, wantSource)
	unrelatedKey := stateRecordKey{scope: stateScope(unrelated), kind: workerStateDraftKind}
	restartedUnrelated, exists := restarted.stateDrafts[unrelatedKey]
	if !exists {
		t.Fatalf("restart lost the unrelated active editor: %+v", restarted.stateDrafts)
	}
	assertPersistedSameKeyDraft(t, restartedUnrelated.draft, wantUnrelated)

	nextSourceTurn := source
	nextSourceTurn.IdempotencyKey = "same-key-next-source-turn"
	restarted = restarted.activateAssignment(nextSourceTurn)
	assertLateRejectionDraft(t, restarted, wantSource)
	if _, exists := restarted.stateDrafts[unrelatedKey]; !exists {
		t.Fatal("recovering the rejected source consumed the unrelated editor row")
	}
}

func commitDurableFinalForSameKeyRejection(
	t *testing.T,
	model Model,
	client *fakeClient,
) (Model, completion.Event) {
	t.Helper()
	updated, _ := model.sendReply(completion.EventFinal, true)
	model = updated.(Model)
	if model.pending.kind != pendingReply || model.pending.event.Type != completion.EventFinal ||
		model.active != nil {
		t.Fatalf("Final was not staged as a terminal durable reply: active=%+v pending=%+v",
			model.active, model.pending)
	}

	updated, stateWrite := model.Update(tea.WindowSizeMsg{Width: model.width, Height: model.height})
	model = updated.(Model)
	updated, outboxWrite := model.Update(workerStateResult(t, stateWrite))
	model = updated.(Model)
	outboxMessages := executeTestCommand(outboxWrite)
	ack, ok := firstTestMessage[eventSent](outboxMessages)
	if !ok || ack.err != nil || len(client.events) == 0 {
		t.Fatalf("Final local outbox result = %+v events=%+v", ack, client.events)
	}
	if cleanup, exists := firstTestMessage[stateWriteResult](outboxMessages); exists {
		updated, trailing := model.Update(cleanup)
		model = updated.(Model)
		model = settleTrailingStateWrites(t, model, trailing)
	}
	finalEvent := client.events[len(client.events)-1]
	if finalEvent.ID != ack.eventID || finalEvent.Type != completion.EventFinal {
		t.Fatalf("Final event = %+v, acknowledgement = %+v", finalEvent, ack)
	}

	updated, trailing := model.Update(ack)
	model = updated.(Model)
	model = settleTrailingStateWrites(t, model, trailing)
	if model.pending.kind != pendingNone || model.active != nil {
		t.Fatalf("successful terminal Final did not settle locally: active=%+v pending=%+v",
			model.active, model.pending)
	}
	return model, finalEvent
}

func assertUnrelatedEditorUnchanged(
	t *testing.T,
	model Model,
	assignment completion.Assignment,
	want lateRejectionDraft,
) {
	t.Helper()
	if model.active == nil || model.active.SessionKey() != assignment.SessionKey() {
		t.Fatalf("late rejection replaced the unrelated active request: %+v", model.active)
	}
	assertLateRejectionDraft(t, model, want)
}

func assertPersistedSameKeyDraft(t *testing.T, got persistedDraft, want lateRejectionDraft) {
	t.Helper()
	if got.Reply != want.reply || got.Command != want.command ||
		!reflect.DeepEqual(restoreTasks(got.Tasks), want.tasks) ||
		got.TaskSelected != want.selected || got.TaskDirty != (len(want.tasks) > 0) ||
		got.TaskEditing != want.taskEditing || got.TaskEditIndex != want.taskEditIndex ||
		got.TaskInput != want.taskInput || got.ToolInput != want.toolInput ||
		!reflect.DeepEqual(got.ToolCallIDs, want.toolCallIDs) {
		t.Fatalf("persisted same-key draft mismatch: got=%+v want=%+v", got, want)
	}
}
