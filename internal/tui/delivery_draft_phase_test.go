package tui

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/workerstate"
)

func TestDeliveryEditsDuringIntentPhaseCrossLatestSaveAheadBeforeOutbox(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "worker.db")
	mirrorRoot := t.TempDir()
	initial := deliveryPhaseDraft("initial")
	model, store, client, assignment, event, intentAndDraftWrite :=
		stageDeliveryPhaseFalseWithDraft(t, statePath, mirrorRoot, "live-edit", initial)

	// Both commands were captured from the phase=false reduction. Complete them,
	// but deliberately deliver neither result to Bubble Tea yet: the operator can
	// continue editing while RecordDeliveryIntents and the ordinary draft Put run.
	asyncMessages := executeTestCommand(intentAndDraftWrite)
	confirmation, ok := firstTestMessage[mirrorConfirmationReady](asyncMessages)
	if !ok || confirmation.err != nil {
		t.Fatalf("delivery intent confirmation = %+v", confirmation)
	}
	oldDraftWrite, ok := firstTestMessage[stateWriteResult](asyncMessages)
	if !ok || oldDraftWrite.err != nil || oldDraftWrite.operation.key.kind != workerStateDraftKind {
		t.Fatalf("concurrent initial draft write = %+v", oldDraftWrite)
	}

	phaseSnapshot := deliveryPhaseDraft("phase")
	phaseSnapshot.apply(&model)
	updated, confirmationCommand := model.Update(confirmation)
	model = updated.(Model)
	if containsStateWriteResult(executeTestCommand(confirmationCommand)) {
		t.Fatal("phase confirmation bypassed the already-running draft writer")
	}

	updated, phaseWrite := model.Update(oldDraftWrite)
	model = updated.(Model)
	phaseResult := workerStateResult(t, phaseWrite)
	var firstPhase persistedPendingSend
	if err := json.Unmarshal(phaseResult.operation.payload, &firstPhase); err != nil {
		t.Fatal(err)
	}
	phaseSnapshot.assertPersisted(t, firstPhase.Remaining)

	// The phase=true payload is now immutable and its Put has completed, but its
	// result has not reached Bubble Tea. Editing in this window must force one
	// more pending-row write before startRecordedMirrorDelivery opens the outbox.
	latest := deliveryPhaseDraft("latest")
	latest.apply(&model)
	updated, latestBoundary := model.Update(phaseResult)
	model = updated.(Model)
	if len(client.events) != 0 || model.pending.durable || model.active == nil {
		t.Fatalf("first phase=true Put reached outbox before latest editor snapshot: pending=%+v events=%+v",
			model.pending, client.events)
	}
	latestResult := workerStateResult(t, latestBoundary)
	var persisted persistedPendingSend
	if err := json.Unmarshal(latestResult.operation.payload, &persisted); err != nil {
		t.Fatal(err)
	}
	latest.assertPersisted(t, persisted.Remaining)

	updated, outbox := model.Update(latestResult)
	model = updated.(Model)
	if model.active != nil || model.delivery.stage != deliverySending {
		t.Fatalf("latest save-ahead boundary did not open the outbox: %+v", model)
	}
	outboxMessages := executeTestCommand(outbox)
	ack, ok := firstTestMessage[eventSent](outboxMessages)
	if !ok || ack.err != nil || ack.eventID != event.ID || len(client.events) != 1 {
		t.Fatalf("exact delivery outbox result = %+v, events=%+v", ack, client.events)
	}
	// Crash after the outbox command completed but before either its eventSent or
	// the concurrent old draft-row deletion was reduced. The latest pane snapshot
	// must now be owned by the pending journal itself.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	restarted := New(
		newFakeClient(), WithStateStore(store),
		WithMirrorManager(newFilesystemMirrorManager(mirrorRoot)),
	)
	if restarted.pending.kind != pendingDelivery || !restarted.pending.recovered ||
		restarted.pending.event.ID != event.ID ||
		restarted.pending.assignment.SessionKey() != assignment.SessionKey() {
		t.Fatalf("latest delivery journal did not recover: %+v", restarted.pending)
	}
	latest.assertModel(t, restarted)
}

func TestRecoveredDeliveryNewerDraftRevisionSurvivesSuccessAndRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "worker.db")
	mirrorRoot := t.TempDir()
	initial := deliveryPhaseDraft("intent")
	model, store, _, assignment, event, _ :=
		stageDeliveryPhaseFalseWithDraft(t, statePath, mirrorRoot, "newer-revision", initial)

	newer := deliveryPhaseDraft("newer")
	payload, err := json.Marshal(newer.persisted(assignment))
	if err != nil {
		t.Fatal(err)
	}
	draftRecord, err := store.Put(
		context.Background(), stateScope(assignment), workerStateDraftKind, payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var intentCreated int64
	for _, record := range records {
		if record.Kind == workerStatePendingSendKind && record.Scope == stateScope(assignment) {
			intentCreated = record.CreatedRevision
		}
	}
	if intentCreated == 0 || draftRecord.Revision <= intentCreated {
		t.Fatalf("test did not create draft revision newer than intent: draft=%d intent=%d",
			draftRecord.Revision, intentCreated)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	manager := newFilesystemMirrorManager(mirrorRoot)
	model = New(client, WithStateStore(store), WithMirrorManager(manager))
	if model.pending.kind != pendingDelivery || !model.pending.recovered ||
		model.pending.deliveryIntentRecorded {
		t.Fatalf("phase=false delivery did not recover: %+v", model.pending)
	}
	newer.assertModel(t, model)

	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	model.mirrors[prepared.namespace] = prepared.workspace
	confirmation := model.resumePendingDelivery(model.pending)().(mirrorConfirmationReady)
	if confirmation.err != nil {
		t.Fatal(confirmation.err)
	}
	updated, phaseWrite := model.Update(confirmation)
	model = updated.(Model)
	latestResult := workerStateResult(t, phaseWrite)
	var latestPending persistedPendingSend
	if err := json.Unmarshal(latestResult.operation.payload, &latestPending); err != nil {
		t.Fatal(err)
	}
	newer.assertPersisted(t, latestPending.Remaining)

	updated, outbox := model.Update(latestResult)
	model = updated.(Model)
	if model.active != nil || model.delivery.stage != deliverySending {
		t.Fatalf("phase=true snapshot did not open the recovered delivery outbox: %+v", model)
	}
	outboxMessages := executeTestCommand(outbox)
	ack, ok := firstTestMessage[eventSent](outboxMessages)
	if !ok || ack.err != nil || ack.eventID != event.ID || len(client.events) != 1 {
		t.Fatalf("recovered delivery outbox result = %+v, events=%+v", ack, client.events)
	}
	updated, afterAck := model.Update(ack)
	model = updated.(Model)
	stateMessages := append([]tea.Msg(nil), outboxMessages...)
	stateMessages = append(stateMessages, executeTestCommand(afterAck)...)
	model = reduceDeliveryStateResults(t, model, stateMessages)
	model = flushWorkerState(t, model)
	if model.pending.kind != pendingNone {
		t.Fatalf("successful recovered delivery remained pending: %+v", model.pending)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	restarted := New(newFakeClient(), WithStateStore(store))
	restarted = restarted.activateAssignment(assignment)
	newer.assertModel(t, restarted)
}

type deliveryPhaseDraftValues struct {
	prefix string
}

func deliveryPhaseDraft(prefix string) deliveryPhaseDraftValues {
	return deliveryPhaseDraftValues{prefix: prefix}
}

func (values deliveryPhaseDraftValues) apply(model *Model) {
	model.setReplyValue(values.prefix + " reply")
	model.setCommandValue(values.prefix + " command")
	model.agentTasks = []agentTask{{
		Content: values.prefix + " task", Status: taskInProgress, Priority: "high",
	}}
	model.taskSelected = 0
	model.taskDirty = true
	model.taskEditing = true
	model.taskEditIndex = 0
	model.taskInput = values.prefix + " task edit"
	model.composing = true
	model.input = values.prefix + ` tool {"query":"keep"}`
	model.toolCallIDs = []string{values.prefix + "-tool-id"}
	model.focus = focusCommand
}

func (values deliveryPhaseDraftValues) persisted(assignment completion.Assignment) persistedDraft {
	return persistedDraft{
		Version: workerStateVersion, Authority: assignmentDraftAuthority(assignment),
		Focus: persistedFocusCommand, Reply: values.prefix + " reply", Command: values.prefix + " command",
		HasTasks: true, Tasks: []persistedTask{{
			Content: values.prefix + " task", Status: taskInProgress, Priority: "high",
		}},
		TaskDirty: true, TaskEditing: true, TaskEditIndex: 0,
		TaskInput:   values.prefix + " task edit",
		ToolInput:   values.prefix + ` tool {"query":"keep"}`,
		ToolCallIDs: []string{values.prefix + "-tool-id"},
	}
}

func (values deliveryPhaseDraftValues) assertPersisted(t *testing.T, got persistedDraft) {
	t.Helper()
	want := values.persisted(completion.Assignment{})
	// Authority belongs to assignment identity and is asserted through the live
	// model/restart checks. Focus is presentation state and is deliberately absent
	// from the delivery editor digest; compare every editor-bearing field here.
	got.Authority = ""
	want.Authority = ""
	got.Focus = ""
	want.Focus = ""
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted delivery remainder = %+v, want %+v", got, want)
	}
}

func (values deliveryPhaseDraftValues) assertModel(t *testing.T, model Model) {
	t.Helper()
	if model.replyInput != values.prefix+" reply" ||
		model.commandInput != values.prefix+" command" ||
		len(model.agentTasks) != 1 || model.agentTasks[0].Content != values.prefix+" task" ||
		model.agentTasks[0].Status != taskInProgress || model.agentTasks[0].Priority != "high" ||
		!model.taskDirty || !model.taskEditing || model.taskEditIndex != 0 ||
		model.taskInput != values.prefix+" task edit" || !model.composing ||
		model.input != values.prefix+` tool {"query":"keep"}` ||
		!reflect.DeepEqual(model.toolCallIDs, []string{values.prefix + "-tool-id"}) {
		t.Fatalf("delivery draft panes did not survive: reply=%q command=%q tasks=%+v dirty=%t editing=%t edit=%d taskInput=%q composing=%t input=%q ids=%v",
			model.replyInput, model.commandInput, model.agentTasks, model.taskDirty,
			model.taskEditing, model.taskEditIndex, model.taskInput, model.composing,
			model.input, model.toolCallIDs)
	}
}

func stageDeliveryPhaseFalseWithDraft(
	t *testing.T,
	statePath string,
	mirrorRoot string,
	suffix string,
	draft deliveryPhaseDraftValues,
) (Model, *workerstate.Store, *fakeClient, completion.Assignment, completion.Event, tea.Cmd) {
	t.Helper()
	store, err := workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	manager := newFilesystemMirrorManager(mirrorRoot)
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role:   canonical.RoleUser,
		Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "write durable.txt " + suffix}},
	}}})
	assignment.IdempotencyKey = "request-delivery-draft-" + suffix
	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	if err := os.WriteFile(filepath.Join(prepared.workspace.Dir(), "durable.txt"), []byte("exact"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	model := New(client, WithStateStore(store), WithMirrorManager(manager))
	model.active = &assignment
	model.rememberContext(assignment)
	model.mirrors[prepared.namespace] = prepared.workspace
	updated, review := model.startMirrorReview()
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, review))
	model = updated.(Model)
	updated, _ = model.previewMirrorDelivery()
	model = updated.(Model)
	draft.apply(&model)
	event := completion.Event{
		ID: model.delivery.eventID, Type: completion.EventToolCalls,
		ToolCalls: append([]completion.ToolCall(nil), model.delivery.calls...),
	}
	// Invoke the same confirmation transition directly so the fixture can cover
	// a partially edited Tasks pane. A real Enter cannot be both "finish this task
	// edit" and "confirm delivery" in one keystroke; the correctness boundary under
	// test starts after confirmation has selected the immutable delivery event.
	updated, _ = model.confirmMirrorDelivery()
	model = updated.(Model)
	updated, pendingWrite := model.Update(tea.WindowSizeMsg{Width: model.width, Height: model.height})
	model = updated.(Model)
	if model.pending.kind != pendingDelivery || model.pending.durable || pendingWrite == nil {
		t.Fatalf("delivery did not stage phase=false journal: %+v", model.pending)
	}
	updated, intentAndDraftWrite := model.Update(workerStateResult(t, pendingWrite))
	model = updated.(Model)
	if !model.pending.durable || intentAndDraftWrite == nil || len(client.events) != 0 {
		t.Fatalf("phase=false journal did not gate mirror intent: %+v", model.pending)
	}
	return model, store, client, assignment, event, intentAndDraftWrite
}

func reduceDeliveryStateResults(t *testing.T, model Model, messages []tea.Msg) Model {
	t.Helper()
	queue := append([]tea.Msg(nil), messages...)
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 64 {
			t.Fatal("delivery state reductions did not quiesce")
		}
		message := queue[0]
		queue = queue[1:]
		result, ok := message.(stateWriteResult)
		if !ok {
			continue
		}
		updated, next := model.Update(result)
		model = updated.(Model)
		queue = append(queue, executeTestCommand(next)...)
	}
	return model
}

func containsStateWriteResult(messages []tea.Msg) bool {
	_, ok := firstTestMessage[stateWriteResult](messages)
	return ok
}
