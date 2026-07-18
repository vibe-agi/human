package tui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	workmirror "github.com/vibe-agi/human/internal/mirror"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerproto"
	"github.com/vibe-agi/human/internal/workerstate"
)

func TestRejectionRecoveryBarrierWaitsForWriterCleanupAndJournalDelete(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	workspace := newRejectionBarrierWorkspace(log)
	workspace.blockRecord = true
	workspace.blockDiscard = true
	store := newRejectionBarrierStateStore(log)

	assignment, pending := rejectionBarrierDelivery("writer", true)
	workspace.changes = cloneMirrorChanges(pending.deliveryChanges)
	key := pendingSendStateKey(pending)
	store.blockDelete = key
	model := rejectionBarrierModel(client, store)
	model.pending = pending
	model.active = cloneAssignment(&assignment)
	model.lastContext = cloneAssignment(&assignment)
	model.mirrors[pending.deliveryNamespace] = workspace
	model.delivery = rejectionBarrierDeliveryReview(pending)
	seedRejectionBarrierPending(t, &model, store, pending)

	writerResult := make(chan mirrorConfirmationReady, 1)
	go func() {
		writerResult <- model.resumePendingDelivery(pending)().(mirrorConfirmationReady)
	}()
	waitRejectionBarrierSignal(t, workspace.recordStarted, "mirror writer start")

	client.messages <- workerclient.Message{ConnectionRestored: true}
	updated, rejectionCommand := model.Update(rejectionBarrierMessage(pending, "expired while writing"))
	model = updated.(Model)
	if finalizer := model.rejectionFinalizers[pending.event.ID]; !finalizer.mirrorConfirmationPending {
		t.Fatalf("rejection did not wait for the in-flight mirror writer: %+v", finalizer)
	}

	rejectionMessages := make(chan []tea.Msg, 1)
	go func() { rejectionMessages <- executeTestCommand(rejectionCommand) }()
	waitRejectionBarrierSignal(t, store.deleteStarted, "pending journal delete start")
	assertRejectionBarrierNotSignaled(t, workspace.discardStarted, "cleanup started before mirror writer settled")
	assertRejectionBarrierNotConfirmed(t, client, pending.event.ID)

	close(workspace.recordRelease)
	confirmation := waitRejectionBarrierValue(t, writerResult, "mirror writer result")
	if confirmation.err != nil {
		t.Fatalf("mirror writer failed: %v", confirmation.err)
	}
	if !workspace.hasIntent(pending.event.ToolCalls[0].ID) {
		t.Fatal("mirror writer did not create the intent used to prove cleanup ordering")
	}

	updated, cleanupCommand := model.Update(confirmation)
	model = updated.(Model)
	cleanupMessages := make(chan []tea.Msg, 1)
	go func() { cleanupMessages <- executeTestCommand(cleanupCommand) }()
	waitRejectionBarrierSignal(t, workspace.discardStarted, "rejected intent cleanup start")
	assertRejectionBarrierNotConfirmed(t, client, pending.event.ID)

	close(workspace.discardRelease)
	discarded := firstRequiredRejectionBarrierMessage[mirrorIntentsDiscarded](
		t, waitRejectionBarrierValue(t, cleanupMessages, "intent cleanup result"),
	)
	if discarded.err != nil {
		t.Fatalf("intent cleanup failed: %v", discarded.err)
	}
	updated, command := model.Update(discarded)
	model = updated.(Model)
	if command != nil {
		t.Fatal("cleanup bypassed the still-blocked pending-journal delete")
	}
	assertRejectionBarrierNotConfirmed(t, client, pending.event.ID)

	close(store.deleteRelease)
	stateResult := firstRequiredRejectionBarrierMessage[stateWriteResult](
		t, waitRejectionBarrierValue(t, rejectionMessages, "pending journal delete result"),
	)
	if stateResult.err != nil || !stateResult.operation.delete || stateResult.operation.key != key {
		t.Fatalf("unexpected pending journal result: %+v", stateResult)
	}
	updated, confirmCommand := model.Update(stateResult)
	model = updated.(Model)
	confirmed := firstRequiredRejectionBarrierMessage[rejectedEventConfirmed](
		t, executeTestCommand(confirmCommand),
	)
	if confirmed.err != nil || confirmed.eventID != pending.event.ID {
		t.Fatalf("unexpected rejected-event confirmation: %+v", confirmed)
	}
	updated, _ = model.Update(confirmed)
	model = updated.(Model)

	if workspace.intentCount() != 0 {
		t.Fatalf("rejected delivery left mirror intents behind: %v", workspace.intentIDs())
	}
	if store.has(key) {
		t.Fatal("rejected delivery left its pending journal behind")
	}
	if _, exists := model.rejectionFinalizers[pending.event.ID]; exists {
		t.Fatal("successful confirmation left a rejection finalizer behind")
	}
	log.requireBefore(t, "mirror_record_done", "mirror_discard_done")
	log.requireBefore(t, "mirror_discard_done", "confirm:"+pending.event.ID)
	log.requireBefore(t, "state_delete_done:"+key.scope.SessionKey, "confirm:"+pending.event.ID)
}

func TestRejectionRecoveryBarrierRemovesSecondRecoveryWithoutOverwritingActiveEditor(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	store := newRejectionBarrierStateStore(log)
	model := rejectionBarrierModel(client, store)

	active := rejectionBarrierAssignment("active")
	model.active = cloneAssignment(&active)
	model.lastContext = cloneAssignment(&active)
	model.focus = focusCommand
	model.setReplyValue("current reply stays here")
	model.setCommandValue("go test ./current")
	model.agentTasks = []agentTask{{Content: "current task", Status: taskInProgress}}
	model.taskSelected = 0
	model.taskDirty = true
	model.taskEditing = true
	model.taskEditIndex = 0
	model.taskInput = "current task edit"
	model.composing = true
	model.input = `{"current":true}`
	model.toolCallIDs = []string{"current-tool"}

	current := rejectionBarrierReply("current-pending", completion.EventProgress, "current pending")
	first := rejectionBarrierReply("queued-first", completion.EventFinal, "first")
	target := rejectionBarrierReply("queued-target", completion.EventFinal, "target draft")
	third := rejectionBarrierReply("queued-third", completion.EventFinal, "third")
	model.pending = current
	model.pendingRecoveries = []pendingSend{first, target, third}
	seedRejectionBarrierDesiredState(t, &model, store)
	targetKey := pendingSendStateKey(target)

	client.messages <- workerclient.Message{ConnectionRestored: true}
	updated, command := model.Update(rejectionBarrierMessage(target, "target rejected"))
	model = updated.(Model)
	if len(model.pendingRecoveries) != 2 ||
		model.pendingRecoveries[0].event.ID != first.event.ID ||
		model.pendingRecoveries[1].event.ID != third.event.ID {
		t.Fatalf("second queued recovery was not removed surgically: %+v", model.pendingRecoveries)
	}
	assertRejectionBarrierCurrentEditor(t, model, active, current)

	driveRejectionBarrierCommands(t, &model, command)
	assertRejectionBarrierCurrentEditor(t, model, active, current)
	if store.has(targetKey) {
		t.Fatal("rejected queued recovery left its pending journal behind")
	}
	if got := client.confirmedIDs(); len(got) != 1 || got[0] != target.event.ID {
		t.Fatalf("queued recovery confirmations = %v, want [%s]", got, target.event.ID)
	}
}

func TestRejectionRecoveryBarrierPreviouslyRejectedDeliveryCleansInsteadOfRetrying(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	client.sendErr = workerclient.ErrEventPreviouslyRejected
	workspace := newRejectionBarrierWorkspace(log)
	store := newRejectionBarrierStateStore(log)

	assignment, pending := rejectionBarrierDelivery("previously-rejected", true)
	key := pendingSendStateKey(pending)
	workspace.seedIntents(pending.event.ToolCalls)
	model := rejectionBarrierModel(client, store)
	model.pending = pending
	model.active = cloneAssignment(&assignment)
	model.lastContext = cloneAssignment(&assignment)
	model.mirrors[pending.deliveryNamespace] = workspace
	model.delivery = rejectionBarrierDeliveryReview(pending)
	seedRejectionBarrierPending(t, &model, store, pending)

	sent := sendPersistedEvent(client, store, pending)().(eventSent)
	if !errors.Is(sent.err, workerclient.ErrEventPreviouslyRejected) {
		t.Fatalf("send result = %v, want ErrEventPreviouslyRejected", sent.err)
	}
	updated, command := model.Update(sent)
	model = updated.(Model)
	if model.pending.kind != pendingNone {
		t.Fatalf("previously rejected event remained pending for retry: %+v", model.pending)
	}
	messages := driveRejectionBarrierCommands(t, &model, command)
	for _, message := range messages {
		if retry, ok := message.(pendingSendRetry); ok {
			t.Fatalf("previously rejected event scheduled retry: %+v", retry)
		}
	}
	if workspace.intentCount() != 0 {
		t.Fatalf("previously rejected delivery left mirror intents: %v", workspace.intentIDs())
	}
	if store.has(key) {
		t.Fatal("previously rejected delivery left its pending journal")
	}
	if got := client.confirmedIDs(); len(got) != 1 || got[0] != pending.event.ID {
		t.Fatalf("previously rejected confirmations = %v, want [%s]", got, pending.event.ID)
	}
	log.requireBefore(t, "mirror_discard_done", "confirm:"+pending.event.ID)
	log.requireBefore(t, "state_delete_done:"+key.scope.SessionKey, "confirm:"+pending.event.ID)
}

func TestRejectionRecoveryBarrierMirrorFailurePausesUntilCurrentTerminalSuccess(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	model := New(client)

	assignment, failed := rejectionBarrierDelivery("failed-mirror", true)
	workspace := newRejectionBarrierWorkspace(log)
	workspace.seedIntents(failed.event.ToolCalls)
	model.mirrors[failed.deliveryNamespace] = workspace
	next := rejectionBarrierReply("next-recovery", completion.EventFinal, "next recovered response")
	model.pending = failed
	model.pendingRecoveries = []pendingSend{next}
	model.active = cloneAssignment(&assignment)
	model.lastContext = cloneAssignment(&assignment)
	model.delivery = rejectionBarrierDeliveryReview(failed)

	updated, command := model.Update(mirrorConfirmationReady{
		sessionKey: assignment.SessionKey(), namespace: failed.deliveryNamespace,
		generation: failed.deliveryGeneration, eventID: failed.event.ID,
		err: errors.New("mirror changed during recovery"),
	})
	model = updated.(Model)
	driveRejectionBarrierCommands(t, &model, command)
	if !model.recoveryDrainPaused || model.pending.kind != pendingNone ||
		model.active == nil || model.active.SessionKey() != assignment.SessionKey() ||
		len(model.pendingRecoveries) != 1 {
		t.Fatalf("mirror failure did not pause later recoveries on the current request: %+v", model)
	}
	if len(client.sentEvents()) != 0 {
		t.Fatal("later recovery was sent while the failed request still owned the operator")
	}

	model.setReplyValue("current request recovered successfully")
	updated, currentCommand := model.sendReply(completion.EventFinal, true)
	model = updated.(Model)
	currentResult := currentCommand().(eventSent)
	if currentResult.err != nil {
		t.Fatalf("current terminal response failed: %v", currentResult.err)
	}
	if got := client.sentEvents(); len(got) != 1 || got[0].ID == next.event.ID {
		t.Fatalf("events before terminal acknowledgement = %+v", got)
	}

	updated, resumeCommand := model.Update(currentResult)
	model = updated.(Model)
	if model.recoveryDrainPaused || model.pending.event.ID != next.event.ID ||
		len(model.pendingRecoveries) != 0 {
		t.Fatalf("terminal success did not resume the next recovery: %+v", model)
	}
	resumeMessages := executeTestCommand(resumeCommand)
	resumed, ok := firstTestMessage[eventSent](resumeMessages)
	if !ok || resumed.eventID != next.event.ID || resumed.err != nil {
		t.Fatalf("next recovery was not sent after terminal success: %#v", resumeMessages)
	}
	if got := client.sentEvents(); len(got) != 2 || got[1].ID != next.event.ID {
		t.Fatalf("sent event order = %+v", got)
	}
}

func TestPendingDeliveryCleanupRetryKeepsJournalUntilIntentIsGone(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	store := newRejectionBarrierStateStore(log)
	workspace := newRejectionBarrierWorkspace(log)
	workspace.discardFailures = 1
	assignment, pending := rejectionBarrierDelivery("cleanup-retry", true)
	workspace.seedIntents(pending.event.ToolCalls)

	model := rejectionBarrierModel(client, store)
	model.pending = pending
	model.active = cloneAssignment(&assignment)
	model.lastContext = cloneAssignment(&assignment)
	model.delivery = rejectionBarrierDeliveryReview(pending)
	model.mirrors[pending.deliveryNamespace] = workspace
	seedRejectionBarrierPending(t, &model, store, pending)
	key := pendingSendStateKey(pending)

	updated, cleanupCommand := model.Update(mirrorConfirmationReady{
		sessionKey: assignment.SessionKey(), namespace: pending.deliveryNamespace,
		generation: pending.deliveryGeneration, eventID: pending.event.ID,
		err: errors.New("workspace changed after preview"),
	})
	model = updated.(Model)
	failedCleanup := firstRequiredRejectionBarrierMessage[pendingDeliveryIntentsDiscarded](
		t, executeTestCommand(cleanupCommand),
	)
	if failedCleanup.err == nil {
		t.Fatal("injected intent cleanup failure unexpectedly succeeded")
	}
	updated, retryCommand := model.Update(failedCleanup)
	model = updated.(Model)
	if retryCommand == nil || model.pending.event.ID != pending.event.ID ||
		model.delivery.stage != deliveryDiscarding || !store.has(key) ||
		!workspace.hasIntent(pending.event.ToolCalls[0].ID) || len(client.sentEvents()) != 0 {
		t.Fatalf("cleanup failure lost its recovery authority: model=%+v", model)
	}

	driveRejectionBarrierCommands(t, &model, retryCommand)
	if model.pending.kind != pendingNone || store.has(key) || workspace.intentCount() != 0 {
		t.Fatalf("cleanup retry did not converge: pending=%+v intents=%v", model.pending, workspace.intentIDs())
	}
	if len(client.sentEvents()) != 0 {
		t.Fatal("failed confirmation reached the worker outbox")
	}
}

func TestRejectedDeliveryWaitsForPhaseWriteAndJournalDelete(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	store := newRejectionBarrierStateStore(log)
	workspace := newRejectionBarrierWorkspace(log)
	assignment, pending := rejectionBarrierDelivery("phase-write-rejection", true)
	workspace.seedIntents(pending.event.ToolCalls)

	model := rejectionBarrierModel(client, store)
	model.pending = pending
	model.active = cloneAssignment(&assignment)
	model.lastContext = cloneAssignment(&assignment)
	model.delivery = rejectionBarrierDeliveryReview(pending)
	model.mirrors[pending.deliveryNamespace] = workspace
	seedRejectionBarrierPending(t, &model, store, pending)
	key := pendingSendStateKey(pending)

	// RecordDeliveryIntents has completed, but its phase=true state replacement
	// is still in flight. pending.durable is false during this exact window.
	updated, phaseWrite := model.Update(mirrorConfirmationReady{
		sessionKey: assignment.SessionKey(), namespace: pending.deliveryNamespace,
		generation: pending.deliveryGeneration, eventID: pending.event.ID,
		changes: cloneMirrorChanges(pending.deliveryChanges),
		calls:   append([]completion.ToolCall(nil), pending.event.ToolCalls...),
	})
	model = updated.(Model)
	if model.pending.durable || phaseWrite == nil || !model.stateWriting {
		t.Fatalf("delivery did not enter the phase-write window: %+v", model.pending)
	}

	client.messages <- workerclient.Message{ConnectionRestored: true}
	updated, rejectionCommand := model.Update(rejectionBarrierMessage(pending, "rejected during phase write"))
	model = updated.(Model)
	finalizer := model.rejectionFinalizers[pending.event.ID]
	if !finalizer.waitsForPendingDelete || finalizer.pendingDeleted {
		t.Fatalf("rejection lost the existing phase=false journal barrier: %+v", finalizer)
	}
	cleanup := firstRequiredRejectionBarrierMessage[mirrorIntentsDiscarded](
		t, executeTestCommand(rejectionCommand),
	)
	updated, command := model.Update(cleanup)
	model = updated.(Model)
	if command != nil || len(client.confirmedIDs()) != 0 {
		t.Fatal("intent cleanup bypassed the in-flight phase write")
	}

	phaseResult := workerStateResult(t, phaseWrite)
	updated, deleteCommand := model.Update(phaseResult)
	model = updated.(Model)
	if deleteCommand == nil || len(client.confirmedIDs()) != 0 {
		t.Fatal("phase write completion bypassed pending-journal deletion")
	}
	deleteResult := workerStateResult(t, deleteCommand)
	if !deleteResult.operation.delete || deleteResult.operation.key != key {
		t.Fatalf("phase write was followed by %+v, want exact pending delete", deleteResult.operation)
	}
	updated, confirmCommand := model.Update(deleteResult)
	model = updated.(Model)
	confirmed := firstRequiredRejectionBarrierMessage[rejectedEventConfirmed](
		t, executeTestCommand(confirmCommand),
	)
	if confirmed.err != nil || confirmed.eventID != pending.event.ID {
		t.Fatalf("confirmation after delete = %+v", confirmed)
	}
	if store.has(key) {
		t.Fatal("rejected phase-write delivery left its pending journal")
	}
	log.requireBefore(t, "mirror_discard_done", "state_delete_done:"+key.scope.SessionKey)
	log.requireBefore(t, "state_delete_done:"+key.scope.SessionKey, "confirm:"+pending.event.ID)
}

func TestRecoveredProgressSendFailurePausesLaterRecoveryUntilCorrectionFinal(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	model := New(client)

	failed := rejectionBarrierReply("progress-send-failure", completion.EventProgress, "partial answer")
	next := rejectionBarrierReply("progress-send-failure-next", completion.EventFinal, "queued final")
	assignment := failed.assignment
	model.pending = failed
	model.pendingRecoveries = []pendingSend{next}
	model.active = cloneAssignment(&assignment)
	model.lastContext = cloneAssignment(&assignment)
	model.recoveredSessions[assignment.SessionKey()] = struct{}{}

	client.sendErr = definitelyNotStored("injected local outbox failure")
	failedResult := sendPersistedEvent(client, nil, failed)().(eventSent)
	client.sendErr = nil
	if failedResult.err == nil {
		t.Fatal("injected recovered progress send unexpectedly succeeded")
	}
	updated, command := model.Update(failedResult)
	model = updated.(Model)
	if command != nil {
		t.Fatal("ordinary recovered send failure tried to drain the later event")
	}
	if _, suppressed := model.recoveredSessions[assignment.SessionKey()]; suppressed {
		t.Fatal("failed recovered progress still suppressed replay of its source request")
	}
	if !model.recoveryDrainPaused || model.pending.kind != pendingNone ||
		model.active == nil || model.active.SessionKey() != assignment.SessionKey() ||
		len(model.pendingRecoveries) != 1 || model.pendingRecoveries[0].event.ID != next.event.ID {
		t.Fatalf("failed recovered progress did not retain the correction barrier: %+v", model)
	}
	if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 0 {
		t.Fatalf("later recovery sent %d time(s) before correction", got)
	}

	model.setReplyValue("corrected terminal answer")
	updated, correctionCommand := model.sendReply(completion.EventFinal, true)
	model = updated.(Model)
	correction := firstRequiredRejectionBarrierMessage[eventSent](
		t, executeTestCommand(correctionCommand),
	)
	if correction.err != nil {
		t.Fatalf("correction final failed: %v", correction.err)
	}
	if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 0 {
		t.Fatalf("later recovery sent %d time(s) before correction acknowledgement", got)
	}

	updated, resumeCommand := model.Update(correction)
	model = updated.(Model)
	if model.recoveryDrainPaused || model.pending.event.ID != next.event.ID ||
		len(model.pendingRecoveries) != 0 {
		t.Fatalf("correction final did not release exactly the next recovery: %+v", model)
	}
	resumed := firstRequiredRejectionBarrierMessage[eventSent](
		t, executeTestCommand(resumeCommand),
	)
	if resumed.err != nil || resumed.eventID != next.event.ID {
		t.Fatalf("later recovered event result = %+v", resumed)
	}
	updated, trailing := model.Update(resumed)
	model = updated.(Model)
	if trailing != nil {
		t.Fatal("last recovered event unexpectedly scheduled another send")
	}
	if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 1 {
		t.Fatalf("later recovery sent %d time(s), want exactly once", got)
	}
}

func TestLateProgressRejectionDrainsOnlyAfterJournalDeleteAndConfirmation(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	store := newRejectionBarrierStateStore(log)
	model := rejectionBarrierModel(client, store)

	// This progress event has already left model.pending, but its exact save-ahead
	// row remains until the asynchronous delete result is reduced.
	rejected := rejectionBarrierReply("late-progress", completion.EventProgress, "")
	rejected.remainingDraft = persistedDraft{Version: workerStateVersion, TaskEditIndex: -1}
	next := rejectionBarrierReply("late-progress-next", completion.EventFinal, "queued final")
	assignment := rejected.assignment
	model.active = cloneAssignment(&assignment)
	model.lastContext = cloneAssignment(&assignment)
	model.recoveryDrainPaused = true
	model.pendingRecoveries = []pendingSend{next}
	model.recoveredSessions[assignment.SessionKey()] = struct{}{}
	seedRejectionBarrierPending(t, &model, store, rejected)
	seedRejectionBarrierPending(t, &model, store, next)
	key := pendingSendStateKey(rejected)
	store.blockDelete = key
	if found, ok := model.rejectedPendingJournal(rejected.event.ID, assignment.SessionKey()); !ok || found != key {
		var persisted persistedPendingSend
		payload := model.stateSynced[key]
		_ = json.Unmarshal([]byte(payload), &persisted)
		t.Fatalf("seeded late progress journal is not discoverable: key=%+v found=%+v valid=%v payload=%s",
			key, found, validatePersistedPendingSend(key.scope, persisted), payload)
	}

	client.messages <- workerclient.Message{ConnectionRestored: true}
	updated, rejectionCommand := model.Update(rejectionBarrierMessage(rejected, "late progress rejected"))
	model = updated.(Model)
	finalizer, exists := model.rejectionFinalizers[rejected.event.ID]
	if !exists || !finalizer.waitsForPendingDelete || !finalizer.resumeRecoveries {
		t.Fatalf("late progress rejection did not retain its drain barriers: %+v", finalizer)
	}
	if model.active != nil || len(model.pendingRecoveries) != 1 {
		t.Fatalf("late progress rejection corrupted the queued recovery state: %+v", model)
	}
	if _, suppressed := model.recoveredSessions[assignment.SessionKey()]; suppressed {
		t.Fatal("late rejection still suppressed replay of its source request")
	}

	rejectionMessages := make(chan []tea.Msg, 1)
	go func() { rejectionMessages <- executeTestCommand(rejectionCommand) }()
	waitRejectionBarrierSignal(t, store.deleteStarted, "late progress journal delete")
	assertRejectionBarrierNotConfirmed(t, client, rejected.event.ID)
	if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 0 {
		t.Fatalf("later recovery sent %d time(s) before journal deletion", got)
	}

	close(store.deleteRelease)
	deleteResult := firstRequiredRejectionBarrierMessage[stateWriteResult](
		t, waitRejectionBarrierValue(t, rejectionMessages, "late progress journal result"),
	)
	if deleteResult.err != nil || !deleteResult.operation.delete || deleteResult.operation.key != key {
		t.Fatalf("unexpected late progress journal result: %+v", deleteResult)
	}
	updated, confirmCommand := model.Update(deleteResult)
	model = updated.(Model)
	if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 0 {
		t.Fatalf("later recovery sent %d time(s) before rejection confirmation", got)
	}
	confirmed := firstRequiredRejectionBarrierMessage[rejectedEventConfirmed](
		t, executeTestCommand(confirmCommand),
	)
	if confirmed.err != nil || confirmed.eventID != rejected.event.ID {
		t.Fatalf("late progress confirmation = %+v", confirmed)
	}
	updated, resumeCommand := model.Update(confirmed)
	model = updated.(Model)
	resumed := firstRequiredRejectionBarrierMessage[eventSent](
		t, executeTestCommand(resumeCommand),
	)
	if resumed.err != nil || resumed.eventID != next.event.ID {
		t.Fatalf("later recovery did not resume after the complete finalizer: %+v", resumed)
	}
	if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 1 {
		t.Fatalf("later recovery sent %d time(s), want exactly once", got)
	}
	log.requireBefore(t, "state_delete_done:"+key.scope.SessionKey, "confirm:"+rejected.event.ID)
}

func TestRejectionFinalizerKeepsRecoveryPausedBehindUnrelatedWork(t *testing.T) {
	t.Run("pending", func(t *testing.T) {
		log := &rejectionBarrierLog{}
		client := newRejectionBarrierClient(log)
		model := New(client)
		next := rejectionBarrierReply("finalizer-pending-next", completion.EventFinal, "queued final")
		unrelated := rejectionBarrierReply("finalizer-unrelated-pending", completion.EventFinal, "unrelated final")
		unrelated.recovered = false
		model.pending = unrelated
		model.pendingRecoveries = []pendingSend{next}

		confirmCommand := model.beginRejectionFinalization(
			"rejected-before-unrelated-pending", "test-rejected-scope",
			stateRecordKey{}, false, false, nil, true,
		)
		confirmed := firstRequiredRejectionBarrierMessage[rejectedEventConfirmed](
			t, executeTestCommand(confirmCommand),
		)
		updated, command := model.Update(confirmed)
		model = updated.(Model)
		if command != nil || !model.recoveryDrainPaused || model.pending.event.ID != unrelated.event.ID {
			t.Fatalf("finalizer did not stay paused behind unrelated pending work: %+v", model)
		}
		if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 0 {
			t.Fatalf("later recovery sent %d time(s) while unrelated work was pending", got)
		}

		unrelatedResult := sendPersistedEvent(client, nil, unrelated)().(eventSent)
		if unrelatedResult.err != nil {
			t.Fatalf("unrelated pending terminal failed: %v", unrelatedResult.err)
		}
		updated, resumeCommand := model.Update(unrelatedResult)
		model = updated.(Model)
		resumed := firstRequiredRejectionBarrierMessage[eventSent](
			t, executeTestCommand(resumeCommand),
		)
		if resumed.err != nil || resumed.eventID != next.event.ID {
			t.Fatalf("later recovery did not resume after unrelated pending terminal: %+v", resumed)
		}
		if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 1 {
			t.Fatalf("later recovery sent %d time(s), want exactly once", got)
		}
	})

	t.Run("active", func(t *testing.T) {
		log := &rejectionBarrierLog{}
		client := newRejectionBarrierClient(log)
		model := New(client)
		next := rejectionBarrierReply("finalizer-active-next", completion.EventFinal, "queued final")
		unrelated := rejectionBarrierAssignment("finalizer-unrelated-active")
		model.active = cloneAssignment(&unrelated)
		model.lastContext = cloneAssignment(&unrelated)
		model.pendingRecoveries = []pendingSend{next}

		confirmCommand := model.beginRejectionFinalization(
			"rejected-before-unrelated-active", "test-rejected-scope",
			stateRecordKey{}, false, false, nil, true,
		)
		confirmed := firstRequiredRejectionBarrierMessage[rejectedEventConfirmed](
			t, executeTestCommand(confirmCommand),
		)
		updated, command := model.Update(confirmed)
		model = updated.(Model)
		if command != nil || !model.recoveryDrainPaused ||
			model.active == nil || model.active.SessionKey() != unrelated.SessionKey() {
			t.Fatalf("finalizer did not stay paused behind unrelated active work: %+v", model)
		}
		if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 0 {
			t.Fatalf("later recovery sent %d time(s) while unrelated work was active", got)
		}

		model.setReplyValue("unrelated terminal answer")
		updated, unrelatedCommand := model.sendReply(completion.EventFinal, true)
		model = updated.(Model)
		unrelatedResult := firstRequiredRejectionBarrierMessage[eventSent](
			t, executeTestCommand(unrelatedCommand),
		)
		if unrelatedResult.err != nil {
			t.Fatalf("unrelated active terminal failed: %v", unrelatedResult.err)
		}
		updated, resumeCommand := model.Update(unrelatedResult)
		model = updated.(Model)
		resumed := firstRequiredRejectionBarrierMessage[eventSent](
			t, executeTestCommand(resumeCommand),
		)
		if resumed.err != nil || resumed.eventID != next.event.ID {
			t.Fatalf("later recovery did not resume after unrelated active terminal: %+v", resumed)
		}
		if got := countRejectionBarrierEvent(client.sentEvents(), next.event.ID); got != 1 {
			t.Fatalf("later recovery sent %d time(s), want exactly once", got)
		}
	})
}

func TestStalePendingPutCannotAuthorizeNewerDeliveryPhase(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	store := newRejectionBarrierStateStore(log)
	assignment, pending := rejectionBarrierDelivery("stale-phase-put", true)
	model := rejectionBarrierModel(client, store)
	model.pending = pending
	model.active = cloneAssignment(&assignment)
	model.lastContext = cloneAssignment(&assignment)
	model.delivery = rejectionBarrierDeliveryReview(pending)
	seedRejectionBarrierPending(t, &model, store, pending)
	key := pendingSendStateKey(pending)
	stalePayload := append(json.RawMessage(nil), []byte(model.stateSynced[key])...)

	// An assignment refresh has a phase=false replacement Put in flight when the
	// mirror writer completes and advances in-memory state to phase=true.
	model.stateWriting = true
	updated, command := model.Update(mirrorConfirmationReady{
		sessionKey: assignment.SessionKey(), namespace: pending.deliveryNamespace,
		generation: pending.deliveryGeneration, eventID: pending.event.ID,
		changes: cloneMirrorChanges(pending.deliveryChanges),
		calls:   append([]completion.ToolCall(nil), pending.event.ToolCalls...),
	})
	model = updated.(Model)
	if command != nil || model.pending.durable || !model.pending.deliveryIntentRecorded {
		t.Fatalf("writer success did not wait behind the older state Put: %+v", model.pending)
	}

	updated, phaseWrite := model.Update(stateWriteResult{
		operation: stateWriteOperation{key: key, payload: stalePayload},
	})
	model = updated.(Model)
	if model.pending.durable || phaseWrite == nil || len(client.sentEvents()) != 0 {
		t.Fatalf("stale phase=false Put authorized the outbox: pending=%+v events=%+v", model.pending, client.sentEvents())
	}

	phaseResult := workerStateResult(t, phaseWrite)
	updated, outbox := model.Update(phaseResult)
	model = updated.(Model)
	if !model.pending.durable || outbox == nil || len(client.sentEvents()) != 0 {
		t.Fatalf("exact phase=true Put did not open the outbox gate: %+v", model.pending)
	}
	ack := firstRequiredRejectionBarrierMessage[eventSent](t, executeTestCommand(outbox))
	if ack.err != nil || ack.eventID != pending.event.ID {
		t.Fatalf("outbox acknowledgement = %+v", ack)
	}
	if got := client.sentEvents(); len(got) != 1 || got[0].ID != pending.event.ID {
		t.Fatalf("exact delivery events = %+v", got)
	}
}

func TestLateMirrorPreparedCannotRetargetDeliveryRejectionCleanup(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	store := newRejectionBarrierStateStore(log)
	oldWorkspace := newRejectionBarrierWorkspace(log)
	newWorkspace := newRejectionBarrierWorkspace(log)
	manager := &rejectionBarrierRotatingMirrorManager{
		workspaces: []MirrorWorkspace{oldWorkspace, newWorkspace},
	}
	assignment, pending := rejectionBarrierDelivery("late-mirror-prepare", false)
	oldWorkspace.changes = cloneMirrorChanges(pending.deliveryChanges)
	newWorkspace.changes = cloneMirrorChanges(pending.deliveryChanges)

	initialPrepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if initialPrepared.err != nil || initialPrepared.workspace != oldWorkspace {
		t.Fatalf("initial mirror open = %+v", initialPrepared)
	}
	model := rejectionBarrierModel(client, store)
	model.mirrorManager = manager
	model.pending = pending
	model.active = cloneAssignment(&assignment)
	model.lastContext = cloneAssignment(&assignment)
	model.delivery = rejectionBarrierDeliveryReview(pending)
	model.mirrors[pending.deliveryNamespace] = initialPrepared.workspace
	seedRejectionBarrierPending(t, &model, store, pending)
	key := pendingSendStateKey(pending)

	// The exact old workspace records provenance before the phase=true worker
	// state replacement is allowed to open the durable outbox gate.
	confirmation := firstRequiredRejectionBarrierMessage[mirrorConfirmationReady](
		t, executeTestCommand(model.resumePendingDelivery(pending)),
	)
	if confirmation.err != nil || !oldWorkspace.hasIntent(pending.event.ToolCalls[0].ID) {
		t.Fatalf("old mirror did not record the delivery intent: %+v", confirmation)
	}
	updated, phaseWrite := model.Update(confirmation)
	model = updated.(Model)
	if model.pending.durable || !model.pending.deliveryIntentRecorded || !model.stateWriting || phaseWrite == nil {
		t.Fatalf("delivery did not enter its phase=true Put window: %+v", model.pending)
	}
	phaseResult := workerStateResult(t, phaseWrite)
	if phaseResult.err != nil || phaseResult.operation.key != key || phaseResult.operation.delete {
		t.Fatalf("phase=true state Put = %+v", phaseResult)
	}
	// Deliberately do not reduce phaseResult yet. The store has committed the Put,
	// while Bubble Tea still considers that callback in flight.

	latePrepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if latePrepared.err != nil || latePrepared.workspace != newWorkspace || manager.openCount() != 2 {
		t.Fatalf("late mirror open did not return the distinct workspace: prepared=%+v opens=%d",
			latePrepared, manager.openCount())
	}
	updated, lateCommand := model.Update(latePrepared)
	model = updated.(Model)
	if model.mirrors[pending.deliveryNamespace] != oldWorkspace {
		t.Fatal("late same-session mirror preparation replaced the workspace owning the recorded intent")
	}
	// Settle the non-watchable fake's watch-start command; it carries no state or
	// delivery side effects and keeps the test free of abandoned command results.
	for _, message := range executeTestCommand(lateCommand) {
		if started, ok := message.(mirrorWatchStarted); ok {
			updated, _ = model.Update(started)
			model = updated.(Model)
		}
	}

	client.messages <- workerclient.Message{ConnectionRestored: true}
	updated, rejectionCommand := model.Update(rejectionBarrierMessage(pending, "delivery expired"))
	model = updated.(Model)
	cleanup := firstRequiredRejectionBarrierMessage[mirrorIntentsDiscarded](
		t, executeTestCommand(rejectionCommand),
	)
	if cleanup.err != nil || cleanup.eventID != pending.event.ID {
		t.Fatalf("rejection cleanup result = %+v", cleanup)
	}
	updated, command := model.Update(cleanup)
	model = updated.(Model)
	if command != nil || len(client.confirmedIDs()) != 0 {
		t.Fatal("rejection cleanup bypassed the in-flight phase callback")
	}

	updated, deleteCommand := model.Update(phaseResult)
	model = updated.(Model)
	deleteResult := workerStateResult(t, deleteCommand)
	if deleteResult.err != nil || !deleteResult.operation.delete || deleteResult.operation.key != key {
		t.Fatalf("post-phase pending delete = %+v", deleteResult)
	}
	updated, confirmCommand := model.Update(deleteResult)
	model = updated.(Model)
	confirmed := firstRequiredRejectionBarrierMessage[rejectedEventConfirmed](
		t, executeTestCommand(confirmCommand),
	)
	if confirmed.err != nil || confirmed.eventID != pending.event.ID {
		t.Fatalf("rejected delivery tombstone = %+v", confirmed)
	}
	updated, _ = model.Update(confirmed)
	model = updated.(Model)

	if oldWorkspace.intentCount() != 0 || oldWorkspace.discardCount() != 1 {
		t.Fatalf("old workspace intent was not cleaned exactly once: intents=%v discards=%d",
			oldWorkspace.intentIDs(), oldWorkspace.discardCount())
	}
	if newWorkspace.intentCount() != 0 || newWorkspace.discardCount() != 0 {
		t.Fatalf("new workspace was incorrectly used for cleanup: intents=%v discards=%d",
			newWorkspace.intentIDs(), newWorkspace.discardCount())
	}
	if got := client.confirmedIDs(); len(got) != 1 || got[0] != pending.event.ID {
		t.Fatalf("rejected delivery confirmations = %v, want [%s]", got, pending.event.ID)
	}
}

func TestRejectedPendingAmbiguousPutRetriesWithDeleteBeforeConfirmation(t *testing.T) {
	for _, test := range []struct {
		name             string
		writeBeforeError bool
	}{
		{name: "committed_but_callback_failed", writeBeforeError: true},
		{name: "failed_before_commit", writeBeforeError: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			log := &rejectionBarrierLog{}
			client := newRejectionBarrierClient(log)
			store := newRejectionBarrierStateStore(log)
			store.putFailure = errors.New("injected commit-ambiguous pending Put failure")
			store.putFailureAfterWrite = test.writeBeforeError
			model := rejectionBarrierModel(client, store)

			pending := rejectionBarrierReply("ambiguous-put-"+test.name, completion.EventProgress, "")
			pending.durable = false
			pending.recovered = false
			assignment := pending.assignment
			model.pending = pending
			model.active = cloneAssignment(&assignment)
			model.lastContext = cloneAssignment(&assignment)
			key := pendingSendStateKey(pending)

			updated, putCommand := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
			model = updated.(Model)
			if putCommand == nil || !model.stateWriting || model.pending.durable {
				t.Fatalf("pending Put was not staged behind the durable gate: %+v", model)
			}
			if _, managed := model.stateManaged[key]; !managed {
				t.Fatal("scheduled pending Put did not mark its key as managed")
			}
			putResult := workerStateResult(t, putCommand)
			if putResult.err == nil || putResult.operation.delete || putResult.operation.key != key {
				t.Fatalf("injected ambiguous Put result = %+v", putResult)
			}
			if got := store.has(key); got != test.writeBeforeError {
				t.Fatalf("store row after ambiguous Put = %t, want %t", got, test.writeBeforeError)
			}

			// Deliver the durable rejection before Bubble Tea reduces the Put result.
			// stateManaged is therefore the only evidence that the Put may have committed.
			client.messages <- workerclient.Message{ConnectionRestored: true}
			updated, rejectionCommand := model.Update(rejectionBarrierMessage(pending, "progress expired"))
			model = updated.(Model)
			finalizer, exists := model.rejectionFinalizers[pending.event.ID]
			if !exists || !finalizer.waitsForPendingDelete || finalizer.pendingDeleted {
				t.Fatalf("rejection lost its maybe-committed pending row barrier: %+v", finalizer)
			}
			executeTestCommand(rejectionCommand)
			assertRejectionBarrierNotConfirmed(t, client, pending.event.ID)

			updated, _ = model.Update(putResult)
			model = updated.(Model)
			if !model.stateRetryPending {
				t.Fatal("ambiguous pending Put did not enter state retry")
			}
			assertRejectionBarrierNotConfirmed(t, client, pending.event.ID)

			updated, deleteCommand := model.Update(stateRetryReady{})
			model = updated.(Model)
			if deleteCommand == nil {
				t.Fatal("state retry did not issue an idempotent Delete for the maybe-committed pending row")
			}
			deleteResult := workerStateResult(t, deleteCommand)
			if deleteResult.err != nil || !deleteResult.operation.delete || deleteResult.operation.key != key {
				t.Fatalf("state retry operation = %+v, want exact pending Delete", deleteResult)
			}
			assertRejectionBarrierNotConfirmed(t, client, pending.event.ID)

			updated, confirmCommand := model.Update(deleteResult)
			model = updated.(Model)
			confirmed := firstRequiredRejectionBarrierMessage[rejectedEventConfirmed](
				t, executeTestCommand(confirmCommand),
			)
			if confirmed.err != nil || confirmed.eventID != pending.event.ID {
				t.Fatalf("rejection confirmation after ambiguous Put cleanup = %+v", confirmed)
			}
			updated, _ = model.Update(confirmed)
			model = updated.(Model)
			if store.has(key) {
				t.Fatal("idempotent Delete left the maybe-committed pending row behind")
			}
			if got := client.confirmedIDs(); len(got) != 1 || got[0] != pending.event.ID {
				t.Fatalf("rejected event confirmations = %v, want [%s]", got, pending.event.ID)
			}
			log.requireBefore(t, "state_delete_done:"+key.scope.SessionKey, "confirm:"+pending.event.ID)
		})
	}
}

func TestRejectionFinalizerBlocksSameToolCallIDResendUntilCleanupCompletes(t *testing.T) {
	client := newFakeClient()
	assignment := allPaneStateAssignment("rejection-resend-barrier")
	model := New(client).activateAssignment(assignment)
	model.composing = true
	model.input = "search {}"
	model.toolCallIDs = []string{"tool-rejected-and-restored"}
	model.rejectionFinalizers["event-rejected"] = rejectionFinalizer{
		scope:           rejectedDraftScopeKey(assignment),
		cleanupInFlight: true,
	}

	updated, command := model.sendDeclaredToolCalls()
	model = updated.(Model)
	if command != nil || model.pending.kind != pendingNone || !model.composing ||
		model.input != "search {}" || len(model.toolCallIDs) != 1 || len(client.events) != 0 {
		t.Fatalf("same-ID resend crossed an unfinished rejection barrier: %+v", model)
	}

	delete(model.rejectionFinalizers, "event-rejected")
	// Cleanup permanently tombstones the rejected call ID in the workspace
	// journal. The restored editor therefore owns a fresh ID before it can send.
	model.toolCallIDs = []string{"tool-replacement-after-rejection"}
	updated, command = model.sendDeclaredToolCalls()
	model = updated.(Model)
	if command == nil || model.pending.kind != pendingAdvancedTools ||
		len(model.pending.event.ToolCalls) != 1 ||
		model.pending.event.ToolCalls[0].ID != "tool-replacement-after-rejection" {
		t.Fatalf("restored tool draft did not become sendable after cleanup: %+v", model)
	}
}

func TestRejectionFinalizerDoesNotBlockUnrelatedTaskScope(t *testing.T) {
	client := newFakeClient()
	blocked := allPaneStateAssignment("blocked-rejection-scope")
	unrelated := allPaneStateAssignment("unrelated-rejection-scope")
	unrelated.CallerID = "unrelated-caller"
	unrelated.WorkspaceKey = "unrelated-workspace"
	unrelated.TaskID = "unrelated-task"

	model := New(client).activateAssignment(unrelated)
	model.composing = true
	model.input = "search {}"
	model.toolCallIDs = []string{"tool-unrelated"}
	model.rejectionFinalizers["event-blocked"] = rejectionFinalizer{
		scope: rejectedDraftScopeKey(blocked), cleanupInFlight: true,
	}

	updated, command := model.sendDeclaredToolCalls()
	model = updated.(Model)
	if command == nil || model.pending.kind != pendingAdvancedTools ||
		len(model.pending.event.ToolCalls) != 1 ||
		model.pending.event.ToolCalls[0].ID != "tool-unrelated" {
		t.Fatalf("unrelated task was blocked by another rejection cleanup: %+v", model)
	}
}

func TestRejectionFinalizerQueuesRepeatedAutomaticContinuationOnce(t *testing.T) {
	client := newFakeClient()
	source := allPaneStateAssignment("continuation-source")
	call := completion.ToolCall{ID: "tool-awaiting-result", Name: "search", Input: map[string]any{}}
	continuation := toolResultTurn(source, "continuation-replayed", call.ID)
	model := New(client)
	model.expectContinuation(source, []completion.ToolCall{call})
	model.rejectionFinalizers["event-earlier-rejected"] = rejectionFinalizer{
		scope: rejectedDraftScopeKey(continuation), cleanupInFlight: true,
	}

	for range 2 {
		updated, _ := model.Update(networkMessage{Assignment: &continuation})
		model = updated.(Model)
	}
	if len(model.assignments) != 1 ||
		model.assignments[0].SessionKey() != continuation.SessionKey() ||
		model.pending.kind != pendingNone {
		t.Fatalf("replayed blocked continuation was duplicated or accepted: %+v", model)
	}
}

func TestQueuedAutomaticContinuationReplayLeavesNoGhostInbox(t *testing.T) {
	for _, test := range []struct {
		name    string
		sendErr error
	}{
		{name: "accept succeeds"},
		{name: "accept fails", sendErr: definitelyNotStored("injected accept failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			source := allPaneStateAssignment("queued-continuation-" + test.name)
			call := completion.ToolCall{ID: "tool-awaiting-result", Name: "search", Input: map[string]any{}}
			continuation := toolResultTurn(source, "continuation-replayed", call.ID)
			model := New(client)
			model.expectContinuation(source, []completion.ToolCall{call})
			model.rejectionFinalizers["event-earlier-rejected"] = rejectionFinalizer{
				scope: rejectedDraftScopeKey(continuation), cleanupInFlight: true,
			}

			updated, _ := model.Update(networkMessage{Assignment: &continuation})
			model = updated.(Model)
			if len(model.assignments) != 1 {
				t.Fatalf("blocked continuation was not queued exactly once: %+v", model)
			}
			delete(model.rejectionFinalizers, "event-earlier-rejected")

			updated, command := model.Update(networkMessage{Assignment: &continuation})
			model = updated.(Model)
			if command == nil || model.pending.kind != pendingAccept || !model.pending.automatic ||
				len(model.assignments) != 0 {
				t.Fatalf("automatic accept did not consume the queued replay: %+v", model)
			}

			eventID := model.pending.eventID
			updated, _ = model.Update(eventSent{eventID: eventID, err: test.sendErr})
			model = updated.(Model)
			if test.sendErr == nil {
				if model.active == nil || model.active.SessionKey() != continuation.SessionKey() ||
					model.pending.kind != pendingNone || len(model.assignments) != 0 {
					t.Fatalf("successful automatic accept left a ghost Inbox row: %+v", model)
				}
				return
			}
			if model.active != nil || model.pending.kind != pendingNone ||
				len(model.assignments) != 1 ||
				model.assignments[0].SessionKey() != continuation.SessionKey() {
				t.Fatalf("failed automatic accept did not restore exactly one Inbox row: %+v", model)
			}
		})
	}
}

func TestRestoreDecisionWriteBeforeErrorNeverCrossesBackIntoOutboxSend(t *testing.T) {
	log := &rejectionBarrierLog{}
	client := newRejectionBarrierClient(log)
	client.sendErr = definitelyNotStored("local outbox precondition failed")
	store := newRejectionBarrierStateStore(log)
	assignment := allPaneStateAssignment("restore-write-before-error")
	event := completion.Event{
		ID: "event-restore-write-before-error", Type: completion.EventProgress,
		Text: "restore exactly once\n\n",
	}
	pending := pendingSend{
		kind: pendingReply, eventID: event.ID, assignment: assignment,
		reply: "restore exactly once", event: event, durable: true,
		remainingDraft: persistedDraft{Version: workerStateVersion, TaskEditIndex: -1},
	}
	key := pendingSendStateKey(pending)
	payload, err := json.Marshal(persistedPendingFromSend(pending, pendingSendDispositionSend))
	if err != nil {
		t.Fatal(err)
	}
	store.seed(key, payload)
	model := New(client, WithStateStore(store))
	model.stateBound = true
	model.pending = pending
	model.stateManaged[key] = struct{}{}
	model.stateSynced[key] = string(payload)

	store.mu.Lock()
	store.putFailure = errors.New("commit reply was lost")
	store.putFailureAfterWrite = true
	store.mu.Unlock()
	first, ok := sendPersistedEvent(client, store, model.pending)().(eventSent)
	if !ok || first.intentErr == nil || !first.restorePending || first.err == nil {
		t.Fatalf("write-before-error restore result = %#v", first)
	}
	if len(client.sentEvents()) != 1 {
		t.Fatalf("initial outbox calls = %d, want 1", len(client.sentEvents()))
	}
	updated, _ := model.Update(first)
	model = updated.(Model)
	if model.pending.kind == pendingNone {
		t.Fatal("ambiguous restore-state commit cleared the pending journal")
	}

	updated, retry := model.Update(pendingRestoreRetry{
		eventID: event.ID, attempt: 1, sendErr: first.err,
	})
	model = updated.(Model)
	if retry == nil {
		t.Fatal("restore-only retry was not scheduled")
	}
	second, ok := commandResult(t, retry).(eventSent)
	if !ok || second.intentErr != nil || !second.restorePending || second.err == nil {
		t.Fatalf("restore-only retry result = %#v", second)
	}
	if len(client.sentEvents()) != 1 {
		t.Fatalf("restore-state retry crossed back into SendEvent: calls=%d", len(client.sentEvents()))
	}
	updated, _ = model.Update(second)
	model = updated.(Model)
	if model.pending.kind != pendingNone || model.replyInput != "restore exactly once" || model.active == nil {
		t.Fatalf("durable restore did not settle exactly once: pending=%+v reply=%q active=%+v",
			model.pending, model.replyInput, model.active)
	}
}

func TestLateRejectedDraftDoesNotOverwriteNewerActiveEditors(t *testing.T) {
	rejection := func(assignment completion.Assignment, event completion.Event) networkMessage {
		return networkMessage{
			EventRejected: &workerproto.EventRejected{
				CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
				EventID: event.ID, Message: "older response expired",
			},
			RejectedEvent: &event, RejectedAssignment: &assignment,
		}
	}

	t.Run("reply", func(t *testing.T) {
		older := allPaneStateAssignment("older-rejected-reply")
		newer := nextTurn(older, "newer-active-reply")
		model := New(newFakeClient()).activateAssignment(newer)
		model.setReplyValue("newer unsent reply")
		event := completion.Event{
			ID: "event-older-reply", Type: completion.EventProgress, Text: "older rejected reply\n\n",
		}

		updated, _ := model.Update(rejection(older, event))
		model = updated.(Model)
		draft, saved := model.rejectedDraftForAssignment(newer)
		if model.active == nil || model.active.SessionKey() != newer.SessionKey() ||
			model.replyInput != "newer unsent reply" || !saved ||
			!draft.hasReply || draft.reply != "older rejected reply" {
			t.Fatalf("late rejected reply overwrote or lost the active editor: %+v draft=%+v", model, draft)
		}

		later := model
		later.active = nil
		later.setReplyValue("")
		delete(later.rejectionFinalizers, event.ID)
		later = later.activateAssignment(nextTurn(older, "later-reply-consumer"))
		if later.replyInput != "older rejected reply" {
			t.Fatalf("parked rejected reply was not consumed on the next safe activation: %+v", later)
		}
		if _, remains := later.rejectedDraftForAssignment(older); remains {
			t.Fatal("consumed rejected reply remained in the scope tray")
		}
	})

	t.Run("advanced tools", func(t *testing.T) {
		older := allPaneStateAssignment("older-rejected-tools")
		newer := nextTurn(older, "newer-active-tools")
		model := New(newFakeClient()).activateAssignment(newer)
		model.composing = true
		model.input = `search {"newer":true}`
		model.toolCallIDs = []string{"tool-newer-editor"}
		event := completion.Event{
			ID: "event-older-tools", Type: completion.EventToolCalls,
			ToolCalls: []completion.ToolCall{{
				ID: "tool-older-rejected", Name: "search", Input: map[string]any{"older": true},
			}},
		}

		updated, _ := model.Update(rejection(older, event))
		model = updated.(Model)
		draft, saved := model.rejectedDraftForAssignment(newer)
		if model.active == nil || model.active.SessionKey() != newer.SessionKey() ||
			!model.composing || model.input != `search {"newer":true}` ||
			len(model.toolCallIDs) != 1 || model.toolCallIDs[0] != "tool-newer-editor" ||
			!saved || !draft.hasTools || draft.toolInput != `search {"older":true}` ||
			len(draft.toolCallIDs) != 1 || draft.toolCallIDs[0] == "tool-older-rejected" {
			t.Fatalf("late rejected tools overwrote or lost the active composer: %+v draft=%+v", model, draft)
		}

		later := model
		later.active = nil
		later.composing = false
		later.input = ""
		later.toolCallIDs = nil
		delete(later.rejectionFinalizers, event.ID)
		later = later.activateAssignment(nextTurn(older, "later-tools-consumer"))
		if !later.composing || later.input != `search {"older":true}` ||
			len(later.toolCallIDs) != 1 || later.toolCallIDs[0] != draft.toolCallIDs[0] {
			t.Fatalf("parked rejected tools were not consumed on the next safe activation: %+v", later)
		}
		if _, remains := later.rejectedDraftForAssignment(older); remains {
			t.Fatal("consumed rejected tools remained in the scope tray")
		}
	})
}

type rejectionBarrierClient struct {
	messages chan workerclient.Message
	log      *rejectionBarrierLog

	mu        sync.Mutex
	sendErr   error
	sent      []completion.Event
	confirmed []string
}

func newRejectionBarrierClient(log *rejectionBarrierLog) *rejectionBarrierClient {
	return &rejectionBarrierClient{
		messages: make(chan workerclient.Message, 8),
		log:      log,
	}
}

func (client *rejectionBarrierClient) Messages() <-chan workerclient.Message { return client.messages }

func (client *rejectionBarrierClient) SendEvent(
	_ context.Context,
	_ completion.Assignment,
	event completion.Event,
) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.sent = append(client.sent, event)
	return client.sendErr
}

func (client *rejectionBarrierClient) ConfirmRejectedEvent(_ context.Context, eventID string) error {
	client.log.add("confirm:" + eventID)
	client.mu.Lock()
	defer client.mu.Unlock()
	client.confirmed = append(client.confirmed, eventID)
	return nil
}

func (client *rejectionBarrierClient) confirmedIDs() []string {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]string(nil), client.confirmed...)
}

func (client *rejectionBarrierClient) sentEvents() []completion.Event {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]completion.Event(nil), client.sent...)
}

type rejectionBarrierStateStore struct {
	log *rejectionBarrierLog

	mu                   sync.Mutex
	records              map[stateRecordKey]json.RawMessage
	blockPut             stateRecordKey
	putStarted           chan struct{}
	putRelease           chan struct{}
	putFailure           error
	putFailureAfterWrite bool
	blockDelete          stateRecordKey
	deleteStarted        chan struct{}
	deleteRelease        chan struct{}
}

func newRejectionBarrierStateStore(log *rejectionBarrierLog) *rejectionBarrierStateStore {
	return &rejectionBarrierStateStore{
		log: log, records: make(map[stateRecordKey]json.RawMessage),
		putStarted: make(chan struct{}, 1), putRelease: make(chan struct{}),
		deleteStarted: make(chan struct{}, 1), deleteRelease: make(chan struct{}),
	}
}

func (store *rejectionBarrierStateStore) Bind(context.Context, string, string) error { return nil }

func (store *rejectionBarrierStateStore) Put(
	ctx context.Context,
	scope workerstate.Scope,
	kind string,
	payload json.RawMessage,
) (workerstate.Record, error) {
	key := stateRecordKey{scope: scope, kind: kind}
	if key == store.blockPut {
		store.putStarted <- struct{}{}
		select {
		case <-store.putRelease:
		case <-ctx.Done():
			return workerstate.Record{}, ctx.Err()
		}
	}
	copyPayload := append(json.RawMessage(nil), payload...)
	store.mu.Lock()
	failure := store.putFailure
	failureAfterWrite := store.putFailureAfterWrite
	store.putFailure = nil
	store.putFailureAfterWrite = false
	if failure != nil && !failureAfterWrite {
		store.mu.Unlock()
		return workerstate.Record{}, failure
	}
	store.records[key] = copyPayload
	store.mu.Unlock()
	record := workerstate.Record{Scope: scope, Kind: kind, Payload: copyPayload}
	if failure != nil {
		return record, failure
	}
	return record, nil
}

func (store *rejectionBarrierStateStore) Delete(
	ctx context.Context,
	scope workerstate.Scope,
	kind string,
) error {
	key := stateRecordKey{scope: scope, kind: kind}
	if key == store.blockDelete {
		store.deleteStarted <- struct{}{}
		select {
		case <-store.deleteRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	store.mu.Lock()
	delete(store.records, key)
	store.mu.Unlock()
	store.log.add("state_delete_done:" + key.scope.SessionKey)
	return nil
}

func (store *rejectionBarrierStateStore) List(context.Context) ([]workerstate.Record, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	records := make([]workerstate.Record, 0, len(store.records))
	for key, payload := range store.records {
		records = append(records, workerstate.Record{
			Scope: key.scope, Kind: key.kind, Payload: append(json.RawMessage(nil), payload...),
		})
	}
	return records, nil
}

func (store *rejectionBarrierStateStore) seed(key stateRecordKey, payload json.RawMessage) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.records[key] = append(json.RawMessage(nil), payload...)
}

func (store *rejectionBarrierStateStore) has(key stateRecordKey) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, exists := store.records[key]
	return exists
}

type rejectionBarrierWorkspace struct {
	log *rejectionBarrierLog

	mu      sync.Mutex
	changes []workmirror.Change
	intents map[string]completion.ToolCall

	blockRecord     bool
	recordStarted   chan struct{}
	recordRelease   chan struct{}
	blockDiscard    bool
	discardFailures int
	discardCalls    int
	discardStarted  chan struct{}
	discardRelease  chan struct{}
}

func newRejectionBarrierWorkspace(log *rejectionBarrierLog) *rejectionBarrierWorkspace {
	return &rejectionBarrierWorkspace{
		log: log, intents: make(map[string]completion.ToolCall),
		recordStarted: make(chan struct{}, 1), recordRelease: make(chan struct{}),
		discardStarted: make(chan struct{}, 1), discardRelease: make(chan struct{}),
	}
}

func (workspace *rejectionBarrierWorkspace) Dir() string { return "/barrier-mirror" }

func (workspace *rejectionBarrierWorkspace) ReconcileRequestForProfile(
	canonical.Request,
	*adapter.Profile,
	string,
) (workmirror.ReconcileReport, error) {
	return workmirror.ReconcileReport{}, nil
}

func (workspace *rejectionBarrierWorkspace) Review() ([]workmirror.Change, error) {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	return cloneMirrorChanges(workspace.changes), nil
}

func (workspace *rejectionBarrierWorkspace) RecordDeliveryIntents(
	_ []workmirror.Change,
	calls []completion.ToolCall,
	_ *adapter.Profile,
	_ string,
) error {
	if workspace.blockRecord {
		workspace.recordStarted <- struct{}{}
		<-workspace.recordRelease
	}
	workspace.mu.Lock()
	for _, call := range calls {
		workspace.intents[call.ID] = call
	}
	workspace.mu.Unlock()
	workspace.log.add("mirror_record_done")
	return nil
}

func (workspace *rejectionBarrierWorkspace) DiscardToolIntents(
	calls []completion.ToolCall,
	_ *adapter.Profile,
) error {
	if workspace.blockDiscard {
		workspace.discardStarted <- struct{}{}
		<-workspace.discardRelease
	}
	workspace.mu.Lock()
	workspace.discardCalls++
	if workspace.discardFailures > 0 {
		workspace.discardFailures--
		workspace.mu.Unlock()
		return errors.New("injected mirror intent cleanup failure")
	}
	for _, call := range calls {
		delete(workspace.intents, call.ID)
	}
	workspace.mu.Unlock()
	workspace.log.add("mirror_discard_done")
	return nil
}

func (workspace *rejectionBarrierWorkspace) seedIntents(calls []completion.ToolCall) {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	for _, call := range calls {
		workspace.intents[call.ID] = call
	}
}

func (workspace *rejectionBarrierWorkspace) hasIntent(id string) bool {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	_, exists := workspace.intents[id]
	return exists
}

func (workspace *rejectionBarrierWorkspace) intentCount() int {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	return len(workspace.intents)
}

func (workspace *rejectionBarrierWorkspace) intentIDs() []string {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	ids := make([]string, 0, len(workspace.intents))
	for id := range workspace.intents {
		ids = append(ids, id)
	}
	return ids
}

func (workspace *rejectionBarrierWorkspace) discardCount() int {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	return workspace.discardCalls
}

type rejectionBarrierRotatingMirrorManager struct {
	mu         sync.Mutex
	workspaces []MirrorWorkspace
	opened     int
}

func (manager *rejectionBarrierRotatingMirrorManager) Open(_, _ string) (MirrorWorkspace, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.opened >= len(manager.workspaces) {
		return nil, errors.New("injected rotating mirror manager exhaustion")
	}
	workspace := manager.workspaces[manager.opened]
	manager.opened++
	return workspace, nil
}

func (manager *rejectionBarrierRotatingMirrorManager) openCount() int {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.opened
}

type rejectionBarrierLog struct {
	mu      sync.Mutex
	entries []string
}

func (log *rejectionBarrierLog) add(entry string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.entries = append(log.entries, entry)
}

func (log *rejectionBarrierLog) requireBefore(t *testing.T, first, second string) {
	t.Helper()
	log.mu.Lock()
	defer log.mu.Unlock()
	firstIndex, secondIndex := -1, -1
	for index, entry := range log.entries {
		if entry == first && firstIndex < 0 {
			firstIndex = index
		}
		if entry == second && secondIndex < 0 {
			secondIndex = index
		}
	}
	if firstIndex < 0 || secondIndex < 0 || firstIndex >= secondIndex {
		t.Fatalf("operation order %q before %q not observed: %v", first, second, log.entries)
	}
}

func rejectionBarrierModel(
	client *rejectionBarrierClient,
	store *rejectionBarrierStateStore,
) Model {
	model := New(client)
	model.stateStore = store
	model.stateBound = true
	model.stateLoaded = true
	return model
}

func rejectionBarrierAssignment(suffix string) completion.Assignment {
	profile := adapter.HumanShimProfile()
	return completion.Assignment{
		CallerID: "caller-" + suffix, WorkspaceKey: "workspace-" + suffix,
		TaskID: "task-" + suffix, IdempotencyKey: "request-" + suffix,
		CapabilityTier: completion.TierWorkspace,
		HarnessID:      profile.HarnessID, HarnessVersion: profile.HarnessVersion,
		Root: "/workspace", Adapter: &profile,
		Request: canonical.Request{
			Messages: []canonical.Message{{
				Role:   canonical.RoleUser,
				Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "request " + suffix}},
			}},
			Tools: []canonical.Tool{{Name: profile.Write.Name, InputSchema: []byte(`{"type":"object"}`)}},
		},
	}
}

func rejectionBarrierDelivery(suffix string, recovered bool) (completion.Assignment, pendingSend) {
	assignment := rejectionBarrierAssignment(suffix)
	change := workmirror.Change{
		Path: "file-" + suffix + ".txt", Kind: workmirror.ChangeWrite,
		NewContent: []byte("content " + suffix), Reasons: []string{"test"},
	}
	call := completion.ToolCall{
		ID: "tool-" + suffix, Name: assignment.Adapter.Write.Name,
		Input: map[string]any{"path": change.Path, "content": string(change.NewContent)},
	}
	event := completion.Event{
		ID: "event-" + suffix, Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{call},
	}
	pending := pendingSend{
		kind: pendingDelivery, eventID: event.ID, assignment: assignment,
		context: cloneAssignment(&assignment), event: event,
		durable: true, recovered: recovered,
		deliveryNamespace:  mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey),
		deliveryGeneration: 1, deliveryChanges: []workmirror.Change{change},
	}
	return assignment, pending
}

func rejectionBarrierReply(
	suffix string,
	eventType completion.EventType,
	text string,
) pendingSend {
	assignment := rejectionBarrierAssignment(suffix)
	event := completion.Event{ID: "event-" + suffix, Type: eventType, Text: text}
	return pendingSend{
		kind: pendingReply, eventID: event.ID, assignment: assignment,
		context: cloneAssignment(&assignment), reply: text, event: event,
		durable: true, recovered: true,
	}
}

func rejectionBarrierDeliveryReview(pending pendingSend) deliveryReview {
	return deliveryReview{
		stage: deliveryConfirming, sessionKey: pending.assignment.SessionKey(),
		namespace: pending.deliveryNamespace, changes: cloneMirrorChanges(pending.deliveryChanges),
		calls:   append([]completion.ToolCall(nil), pending.event.ToolCalls...),
		eventID: pending.event.ID, generation: pending.deliveryGeneration,
		assignment: pending.assignment, context: cloneAssignment(pending.context),
	}
}

func rejectionBarrierMessage(pending pendingSend, message string) networkMessage {
	event := pending.event
	assignment := pending.assignment
	return networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: event.ID, Message: message,
		},
		RejectedEvent: &event, RejectedAssignment: &assignment,
	}
}

func seedRejectionBarrierPending(
	t *testing.T,
	model *Model,
	store *rejectionBarrierStateStore,
	pending pendingSend,
) {
	t.Helper()
	key := pendingSendStateKey(pending)
	payload, err := json.Marshal(persistedPendingFromSend(pending, pendingSendDispositionSend))
	if err != nil {
		t.Fatal(err)
	}
	model.stateManaged[key] = struct{}{}
	model.stateSynced[key] = string(payload)
	store.seed(key, payload)
}

func seedRejectionBarrierDesiredState(
	t *testing.T,
	model *Model,
	store *rejectionBarrierStateStore,
) {
	t.Helper()
	desired, _, err := model.desiredWorkerState()
	if err != nil {
		t.Fatal(err)
	}
	for key, payload := range desired {
		model.stateManaged[key] = struct{}{}
		model.stateSynced[key] = string(payload)
		store.seed(key, payload)
	}
}

func assertRejectionBarrierCurrentEditor(
	t *testing.T,
	model Model,
	active completion.Assignment,
	current pendingSend,
) {
	t.Helper()
	if model.active == nil || model.active.SessionKey() != active.SessionKey() ||
		model.pending.event.ID != current.event.ID {
		t.Fatalf("queued rejection replaced current request: active=%+v pending=%+v", model.active, model.pending)
	}
	if model.focus != focusCommand || model.replyInput != "current reply stays here" ||
		model.commandInput != "go test ./current" || !model.composing ||
		model.input != `{"current":true}` || len(model.toolCallIDs) != 1 ||
		model.toolCallIDs[0] != "current-tool" || len(model.agentTasks) != 1 ||
		model.agentTasks[0].Content != "current task" || !model.taskDirty ||
		!model.taskEditing || model.taskInput != "current task edit" {
		t.Fatalf("queued rejection overwrote current editor: %+v", model)
	}
}

func driveRejectionBarrierCommands(t *testing.T, model *Model, command tea.Cmd) []tea.Msg {
	t.Helper()
	queue := executeTestCommand(command)
	all := append([]tea.Msg(nil), queue...)
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 64 {
			t.Fatal("rejection command driver exceeded 64 reductions")
		}
		message := queue[0]
		queue = queue[1:]
		switch message.(type) {
		case stateWriteResult, mirrorIntentsDiscarded, pendingDeliveryIntentsDiscarded, rejectedEventConfirmed:
			updated, next := model.Update(message)
			*model = updated.(Model)
			produced := executeTestCommand(next)
			all = append(all, produced...)
			queue = append(queue, produced...)
		}
	}
	return all
}

func waitRejectionBarrierSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func waitRejectionBarrierValue[T any](t *testing.T, values <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		var zero T
		return zero
	}
}

func assertRejectionBarrierNotSignaled(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
		t.Fatal(message)
	default:
	}
}

func assertRejectionBarrierNotConfirmed(
	t *testing.T,
	client *rejectionBarrierClient,
	eventID string,
) {
	t.Helper()
	if confirmed := client.confirmedIDs(); len(confirmed) != 0 {
		t.Fatalf("event %s confirmed before all barriers settled: %v", eventID, confirmed)
	}
}

func firstRequiredRejectionBarrierMessage[T any](t *testing.T, messages []tea.Msg) T {
	t.Helper()
	message, ok := firstTestMessage[T](messages)
	if !ok {
		t.Fatalf("required %T message missing from %#v", *new(T), messages)
	}
	return message
}

func countRejectionBarrierEvent(events []completion.Event, eventID string) int {
	count := 0
	for _, event := range events {
		if event.ID == eventID {
			count++
		}
	}
	return count
}

func (log *rejectionBarrierLog) String() string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return fmt.Sprint(log.entries)
}
