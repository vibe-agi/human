package tui

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerstate"
)

// Two Progress events may both have committed to the durable worker outbox
// before the gateway rejects either one. The rejected inbox is ordered, but a
// process can restart between its items. Recovery must therefore retain the
// already-rejected prefix and insert the next rejected segment before the
// still-local editor tail, without duplicating either segment or consuming
// drafts owned by another pane or scope.
func TestRejectedProgressOutboxOrderSurvivesRestartBetweenNACKs(t *testing.T) {
	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}

	client := newFakeClient()
	assignment := allPaneStateAssignment("two-late-progress-events")
	model, _ := acceptLateRejectionSource(
		t, New(client, WithStateStore(store)), client, assignment,
	)

	const firstText = "first durable progress segment"
	model.setReplyValue(firstText)
	model = flushWorkerState(t, model)
	model, first := commitDurableProgressForLateRejection(t, model, client)

	const secondText = "second durable progress segment"
	model.setReplyValue(secondText)
	model = flushWorkerState(t, model)
	model, second := commitDurableProgressForLateRejection(t, model, client)
	if model.pending.kind != pendingNone || len(client.events) < 3 {
		t.Fatalf("both Progress events were not handed to the local durable outbox: %+v", model)
	}

	local := installLateRejectionDraft(&model, "independent")
	otherAssignment := allPaneStateAssignment("unrelated-request")
	otherAssignment.WorkspaceKey += "-other"
	otherAssignment.TaskID += "-other"
	const otherReply = "unrelated scope draft"
	const otherCommand = "go test ./unrelated/..."
	model.storeRejectedDraft(rejectedDraftState{
		assignment:    otherAssignment,
		authority:     assignmentDraftAuthority(otherAssignment),
		kind:          pendingReply,
		hasReply:      true,
		reply:         otherReply,
		replyRejected: otherReply,
		hasCommand:    true,
		command:       otherCommand,
	})
	model = flushWorkerState(t, model)

	updated, finalization := model.Update(durableLateRejection(assignment, first))
	model = updated.(Model)
	model = finishLateRejectionFinalization(t, model, client, first.ID, finalization)
	wantAfterFirst := local
	wantAfterFirst.reply = firstText + "\n\n" + local.reply
	assertLateRejectionDraft(t, model, wantAfterFirst)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}

	// On reconnect the worker outbox delivers the still-unconfirmed second NACK
	// before the gateway needs to replay the assignment. This model therefore has
	// no process-local transcript or pending send to help with reconstruction.
	replayClient := newFakeClient()
	restarted := New(replayClient, WithStateStore(store))
	updated, finalization = restarted.Update(durableLateRejection(assignment, second))
	restarted = updated.(Model)
	restarted = finishLateRejectionFinalization(t, restarted, replayClient, second.ID, finalization)

	wantFinal := local
	wantFinal.reply = firstText + "\n\n" + secondText + "\n\n" + local.reply
	assertLateRejectionDraft(t, restarted, wantFinal)
	if draft, exists := restarted.stateDrafts[stateRecordKey{
		scope: stateScope(otherAssignment), kind: workerStateDraftKind,
	}]; !exists || draft.draft.Reply != otherReply || draft.draft.Command != otherCommand {
		t.Fatalf("second same-session NACK consumed an unrelated scope draft: %+v", restarted.stateDrafts)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workerstate.Open(ctx, statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// A second restart proves that the finalizer committed the exact merged
	// authority before confirming E2, rather than leaving the correct value only
	// in process memory.
	finalModel := New(newFakeClient(), WithStateStore(store)).activateAssignment(assignment)
	assertLateRejectionDraft(t, finalModel, wantFinal)

	otherModel := New(newFakeClient(), WithStateStore(store)).activateAssignment(otherAssignment)
	if otherModel.replyInput != otherReply || otherModel.commandInput != otherCommand {
		t.Fatalf("unrelated scope draft changed across same-session NACK recovery: %+v", otherModel)
	}
	if first.Type != completion.EventProgress || second.Type != completion.EventProgress ||
		first.ID == "" || second.ID == "" || first.ID == second.ID {
		t.Fatalf("test did not exercise two distinct durable Progress events: first=%+v second=%+v", first, second)
	}
}
