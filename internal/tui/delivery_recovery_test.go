package tui

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	workmirror "github.com/vibe-agi/human/internal/mirror"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerstate"
)

type deliveryCrashPoint string

const (
	crashAfterDeliveryState  deliveryCrashPoint = "after state"
	crashAfterMirrorIntent   deliveryCrashPoint = "after mirror intent"
	crashAfterDeliveryOutbox deliveryCrashPoint = "after outbox"
)

func TestPendingDeliveryCrashWindowsRecoverViaInit(t *testing.T) {
	for _, point := range []deliveryCrashPoint{
		crashAfterDeliveryState,
		crashAfterMirrorIntent,
		crashAfterDeliveryOutbox,
	} {
		t.Run(string(point), func(t *testing.T) {
			statePath := filepath.Join(t.TempDir(), "state", "worker-state.db")
			mirrorRoot := t.TempDir()
			assignment, event := stageDeliveryCrashPoint(t, statePath, mirrorRoot, point)

			client := newFakeClient()
			store, err := workerstate.Open(context.Background(), statePath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			model := New(
				client,
				WithStateStore(store),
				WithMirrorManager(newFilesystemMirrorManager(mirrorRoot)),
			)
			if model.pending.kind != pendingDelivery || !model.pending.durable ||
				model.pending.event.ID != event.ID ||
				model.pending.event.ToolCalls[0].ID != event.ToolCalls[0].ID {
				t.Fatalf("exact delivery journal did not recover at %s: %+v", point, model.pending)
			}
			client.messages <- workerclient.Message{ConnectionRestored: true}
			bootMessages := executeTestCommand(model.Init())
			if resume, ok := firstTestMessage[resumeDurablePendingRequest](bootMessages); ok {
				updated, command := model.Update(resume)
				model = updated.(Model)
				bootMessages = append(bootMessages, executeTestCommand(command)...)
			}
			if point == crashAfterDeliveryOutbox {
				if len(client.events) != 1 || client.events[0].ID != event.ID ||
					len(client.events[0].ToolCalls) != 1 ||
					client.events[0].ToolCalls[0].ID != event.ToolCalls[0].ID ||
					!containsEventSent(bootMessages) {
					t.Fatalf("recorded delivery phase did not replay the exact outbox event: %+v", client.events)
				}
				return
			}
			if len(client.events) != 0 || containsEventSent(bootMessages) {
				t.Fatalf("Init sent delivery before mirror intent verification at %s", point)
			}
			prepared, ok := firstTestMessage[mirrorPrepared](bootMessages)
			if !ok || prepared.sessionKey != assignment.SessionKey() {
				t.Fatalf("Init did not prepare the recovered mirror at %s: %#v", point, bootMessages)
			}

			updated, confirm := model.Update(prepared)
			model = updated.(Model)
			confirmMessages := executeTestCommand(confirm)
			if len(client.events) != 0 || containsEventSent(confirmMessages) {
				t.Fatalf("mirror preparation bypassed intent verification at %s", point)
			}
			confirmation, ok := firstTestMessage[mirrorConfirmationReady](confirmMessages)
			if !ok || confirmation.err != nil {
				t.Fatalf("recovered mirror intent was not confirmed at %s: %+v", point, confirmation)
			}

			updated, phaseWrite := model.Update(confirmation)
			model = updated.(Model)
			phaseResult := workerStateResult(t, phaseWrite)
			updated, outbox := model.Update(phaseResult)
			model = updated.(Model)
			outboxMessages := executeTestCommand(outbox)
			if len(client.events) != 1 || client.events[0].ID != event.ID ||
				len(client.events[0].ToolCalls) != 1 ||
				client.events[0].ToolCalls[0].ID != event.ToolCalls[0].ID {
				t.Fatalf("recovered delivery changed IDs at %s: %+v", point, client.events)
			}
			if _, ok := firstTestMessage[eventSent](outboxMessages); !ok {
				t.Fatalf("recovered delivery did not reach outbox at %s: %#v", point, outboxMessages)
			}
		})
	}
}

func TestPendingDeliveryIdentityReadyRecoveryConfirmsIntentBeforeOutbox(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "worker-state.db")
	mirrorRoot := t.TempDir()
	assignment, event := stageDeliveryCrashPoint(
		t, statePath, mirrorRoot, crashAfterDeliveryState,
	)
	store, err := workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	client.identity = workerclient.Identity{}
	model := New(
		client,
		WithStateStore(store),
		WithMirrorManager(newFilesystemMirrorManager(mirrorRoot)),
	)
	if model.stateBound || model.pending.kind != pendingNone {
		t.Fatalf("offline worker loaded state before authenticated identity: %+v", model)
	}
	client.messages <- workerclient.Message{ConnectionRestored: true}
	identity := workerclient.Identity{GatewayID: "scope:test-gateway", WorkerID: "worker"}
	updated, recovery := model.Update(networkMessage{IdentityReady: &identity})
	model = updated.(Model)
	if model.pending.kind != pendingDelivery || model.pending.event.ID != event.ID {
		t.Fatalf("IdentityReady did not load exact delivery: %+v", model.pending)
	}
	recoveryMessages := executeTestCommand(recovery)
	if len(client.events) != 0 || containsEventSent(recoveryMessages) {
		t.Fatal("IdentityReady sent delivery before mirror intent verification")
	}
	prepared, ok := firstTestMessage[mirrorPrepared](recoveryMessages)
	if !ok || prepared.sessionKey != assignment.SessionKey() {
		t.Fatalf("IdentityReady did not prepare mirror: %#v", recoveryMessages)
	}
	updated, confirm := model.Update(prepared)
	model = updated.(Model)
	confirmationMessages := executeTestCommand(confirm)
	if len(client.events) != 0 {
		t.Fatal("IdentityReady recovery reached outbox before confirmation was reduced")
	}
	confirmation, ok := firstTestMessage[mirrorConfirmationReady](confirmationMessages)
	if !ok || confirmation.err != nil {
		t.Fatalf("IdentityReady mirror confirmation = %+v", confirmation)
	}
	updated, phaseWrite := model.Update(confirmation)
	model = updated.(Model)
	phaseResult := workerStateResult(t, phaseWrite)
	updated, outbox := model.Update(phaseResult)
	model = updated.(Model)
	executeTestCommand(outbox)
	if len(client.events) != 1 || client.events[0].ID != event.ID ||
		client.events[0].ToolCalls[0].ID != event.ToolCalls[0].ID {
		t.Fatalf("IdentityReady recovery changed exact event: %+v", client.events)
	}
}

func TestPendingDeliveryOutboxFailureRetainsDurableJournal(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "worker-state.db")
	mirrorRoot := t.TempDir()
	_, event := stageDeliveryCrashPoint(t, statePath, mirrorRoot, crashAfterMirrorIntent)
	store, err := workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	client.sendErr = definitelyNotStored("local outbox unavailable")
	manager := newFilesystemMirrorManager(mirrorRoot)
	model := New(client, WithStateStore(store), WithMirrorManager(manager))
	prepared := prepareMirror(manager, model.pending.assignment)().(mirrorPrepared)
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
	phaseResult := workerStateResult(t, phaseWrite)
	updated, outbox := model.Update(phaseResult)
	model = updated.(Model)
	messages := executeTestCommand(outbox)
	ack, ok := firstTestMessage[eventSent](messages)
	if !ok || ack.intentErr == nil {
		t.Fatalf("local outbox failure was not classified as retryable: %#v", messages)
	}
	updated, retry := model.Update(ack)
	model = updated.(Model)
	if retry == nil || model.pending.kind != pendingDelivery || !model.pending.durable ||
		model.pending.event.ID != event.ID || model.delivery.stage != deliverySending ||
		model.active != nil || !strings.Contains(model.status, "will retry") {
		t.Fatalf("durable delivery was converted into an ordinary draft: %+v", model)
	}
	records, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range records {
		if record.Kind == workerStatePendingSendKind && record.Scope == stateScope(model.pending.assignment) {
			found = true
		}
	}
	if !found {
		t.Fatal("durable pending-delivery row disappeared after local outbox failure")
	}
}

func TestPendingDeliveryConfirmationErrorNeverBypassesIntentAndEventuallyDeletesJournal(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "worker-state.db")
	mirrorRoot := t.TempDir()
	assignment, _ := stageDeliveryCrashPoint(t, statePath, mirrorRoot, crashAfterMirrorIntent)
	if count := mirrorDeliveryIntentCount(t, mirrorRoot, assignment); count != 1 {
		t.Fatalf("staged crash window has %d delivery intents, want 1", count)
	}
	workspacePath := filepath.Join(mirrorRoot, assignment.CallerID, assignment.WorkspaceKey, "durable.txt")
	if err := os.WriteFile(workspacePath, []byte("changed after confirmation"), 0o600); err != nil {
		t.Fatal(err)
	}

	// First process detects the changed mirror and clears pending in memory, but
	// crashes before its state-row delete command executes.
	store, err := workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	manager := newFilesystemMirrorManager(mirrorRoot)
	model := New(client, WithStateStore(store), WithMirrorManager(manager))
	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	model.mirrors[prepared.namespace] = prepared.workspace
	confirmation := model.resumePendingDelivery(model.pending)().(mirrorConfirmationReady)
	if confirmation.err == nil {
		t.Fatal("changed mirror unexpectedly matched the persisted delivery")
	}
	updated, cleanupCommand := model.Update(confirmation)
	model = updated.(Model)
	if model.pending.kind != pendingDelivery || model.delivery.stage != deliveryDiscarding ||
		len(client.events) != 0 || cleanupCommand == nil {
		t.Fatalf("confirmation failure did not retain its journal for intent cleanup: %+v", model)
	}
	cleanup, ok := firstTestMessage[pendingDeliveryIntentsDiscarded](executeTestCommand(cleanupCommand))
	if !ok || cleanup.err != nil {
		t.Fatalf("intent cleanup before correction = %+v", cleanup)
	}
	updated, deleteCommand := model.Update(cleanup)
	model = updated.(Model)
	if model.pending.kind != pendingNone || deleteCommand == nil {
		t.Fatalf("cleaned delivery did not stage safe journal deletion: %+v", model)
	}
	if count := mirrorDeliveryIntentCount(t, mirrorRoot, assignment); count != 0 {
		t.Fatalf("confirmation failure left %d orphan delivery intents", count)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Remove the original transient mismatch before restart. The mirror-side
	// discard tombstone, not a repeated Review error, must prevent the stale send
	// journal from resurrecting this exact event.
	if err := os.WriteFile(workspacePath, []byte("exact"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The still-present row is recovered, but it is fresh-confirmed against the
	// mirror again. Its durable discard tombstone refuses to re-record the exact
	// call, so it never jumps into SendEvent even though the transient mismatch
	// is now gone.
	store, err = workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client = newFakeClient()
	manager = newFilesystemMirrorManager(mirrorRoot)
	model = New(client, WithStateStore(store), WithMirrorManager(manager))
	prepared = prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	model.mirrors[prepared.namespace] = prepared.workspace
	confirmation = model.resumePendingDelivery(model.pending)().(mirrorConfirmationReady)
	if confirmation.err == nil || len(client.events) != 0 {
		t.Fatal("recovered discarded delivery bypassed its mirror tombstone")
	}
	updated, cleanupCommand = model.Update(confirmation)
	model = updated.(Model)
	cleanup, ok = firstTestMessage[pendingDeliveryIntentsDiscarded](executeTestCommand(cleanupCommand))
	if !ok || cleanup.err != nil {
		t.Fatalf("recovered intent cleanup = %+v", cleanup)
	}
	updated, deleteCommand = model.Update(cleanup)
	model = updated.(Model)
	executeTestCommand(deleteCommand)
	records, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.Kind == workerStatePendingSendKind && record.Scope.SessionKey == assignment.SessionKey() {
			t.Fatalf("failed confirmation left a permanent pending journal: %+v", record)
		}
	}
}

func TestRecordedDeliveryIntentPhaseSkipsChangedMirrorAndReplaysExactOutboxEvent(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "worker-state.db")
	mirrorRoot := t.TempDir()
	assignment, event := stageDeliveryCrashPoint(
		t, statePath, mirrorRoot, crashAfterDeliveryOutbox,
	)
	if err := os.WriteFile(
		filepath.Join(mirrorRoot, assignment.CallerID, assignment.WorkspaceKey, "durable.txt"),
		[]byte("newer unsent draft"), 0o600,
	); err != nil {
		t.Fatal(err)
	}

	store, err := workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	client.messages <- workerclient.Message{ConnectionRestored: true}
	model := New(
		client, WithStateStore(store),
		WithMirrorManager(newFilesystemMirrorManager(mirrorRoot)),
	)
	if !model.pending.deliveryIntentRecorded {
		t.Fatal("recovered delivery lost its durable intent-recorded phase")
	}
	messages := executeTestCommand(model.Init())
	resume, ok := firstTestMessage[resumeDurablePendingRequest](messages)
	if !ok {
		t.Fatalf("Init did not request exact delivery replay: %#v", messages)
	}
	updated, outbox := model.Update(resume)
	model = updated.(Model)
	messages = append(messages, executeTestCommand(outbox)...)
	if _, prepared := firstTestMessage[mirrorPrepared](messages); prepared {
		t.Fatal("intent-recorded recovery re-reviewed a newer scratch draft")
	}
	if len(client.events) != 1 || client.events[0].ID != event.ID ||
		client.events[0].ToolCalls[0].ID != event.ToolCalls[0].ID {
		t.Fatalf("recorded delivery phase changed its exact event: %+v", client.events)
	}
	content, err := os.ReadFile(filepath.Join(
		mirrorRoot, assignment.CallerID, assignment.WorkspaceKey, "durable.txt",
	))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "newer unsent draft" {
		t.Fatalf("recovery overwrote newer scratch content with %q", content)
	}
}

func TestRecoveredRecordedDeliveryFreezesAssignmentBeforeOutbox(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "worker-state.db")
	mirrorRoot := t.TempDir()
	assignment, event := stageDeliveryCrashPoint(
		t, statePath, mirrorRoot, crashAfterDeliveryOutbox,
	)
	store, err := workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	model := New(
		client, WithStateStore(store),
		WithMirrorManager(newFilesystemMirrorManager(mirrorRoot)),
	)
	if !model.pending.deliveryIntentRecorded || !model.pending.durable {
		t.Fatalf("recorded delivery did not recover: %+v", model.pending)
	}
	frozen := model.pending.assignment
	refreshed := assignment
	refreshed.LeaseOwner = "worker"
	refreshed.HarnessSessionID = "must-not-rewrite-recovery"
	client.messages <- workerclient.Message{ConnectionRestored: true}
	updated, outbox := model.Update(networkMessage{Assignment: &refreshed})
	model = updated.(Model)
	if !model.pending.durable || !samePersistedAssignment(model.pending.assignment, frozen) ||
		model.delivery.stage != deliverySending || !model.pending.outboxInFlight || len(client.events) != 0 {
		t.Fatalf("recovered assignment replay rewrote or bypassed the frozen event: %+v", model.pending)
	}
	executeTestCommand(outbox)
	if len(client.events) != 1 || client.events[0].ID != event.ID ||
		len(client.assignments) != 1 || !samePersistedAssignment(client.assignments[0], frozen) {
		t.Fatalf("outbox did not preserve the exact recovered snapshot: assignments=%+v events=%+v",
			client.assignments, client.events)
	}
}

func TestRecoveredRecordedDeliveryFreezesAssignmentAfterOutboxCapture(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "worker-state.db")
	mirrorRoot := t.TempDir()
	assignment, event := stageDeliveryCrashPoint(
		t, statePath, mirrorRoot, crashAfterDeliveryOutbox,
	)
	store, err := workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	model := New(
		client, WithStateStore(store),
		WithMirrorManager(newFilesystemMirrorManager(mirrorRoot)),
	)
	updated, capturedOutbox := model.Update(resumeDurablePendingRequest{eventID: event.ID})
	model = updated.(Model)
	if model.delivery.stage != deliverySending || !model.pending.outboxInFlight {
		t.Fatalf("delivery did not freeze its captured outbox payload: %+v", model.pending)
	}
	frozen := model.pending.assignment
	refreshed := assignment
	refreshed.LeaseOwner = "worker"
	refreshed.HarnessSessionID = "must-not-rewrite-captured-outbox"
	updated, _ = model.Update(networkMessage{Assignment: &refreshed})
	model = updated.(Model)
	if !samePersistedAssignment(model.pending.assignment, frozen) || !model.pending.durable ||
		!model.pendingSendStateSynchronized(model.pending) {
		t.Fatalf("late replay diverged the pending journal from captured outbox: %+v", model.pending)
	}
	if !samePersistedAssignment(model.delivery.assignment, frozen) {
		t.Fatalf("late replay rewrote delivery routing away from its durable snapshot: %+v", model.delivery.assignment)
	}
	executeTestCommand(capturedOutbox)
	if len(client.assignments) != 1 || !samePersistedAssignment(client.assignments[0], frozen) ||
		len(client.events) != 1 || client.events[0].ID != event.ID {
		t.Fatalf("captured outbox payload changed after replay: assignments=%+v events=%+v",
			client.assignments, client.events)
	}
}

func TestPendingDeliveryStateDeepCopiesReviewedChangeBytesAndReasons(t *testing.T) {
	assignment := workspaceAssignment(canonical.Request{})
	change := workmirror.Change{
		Kind: workmirror.ChangeEdit, Path: "copy.txt",
		OldContent: []byte("old"), NewContent: []byte("new"), Reasons: []string{"reviewed"},
	}
	pending := pendingSend{
		kind: pendingDelivery, assignment: assignment,
		event: completion.Event{
			ID: "event-copy", Type: completion.EventToolCalls,
			ToolCalls: []completion.ToolCall{{ID: "tool-copy", Name: "human_edit_file"}},
		},
		remainingDraft:     persistedDraft{Version: workerStateVersion, TaskEditIndex: -1},
		deliveryNamespace:  mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey),
		deliveryGeneration: 1, deliveryChanges: []workmirror.Change{change},
	}
	persisted := persistedPendingFromSend(pending, pendingSendDispositionSend)
	pending.deliveryChanges[0].OldContent[0] = 'X'
	pending.deliveryChanges[0].NewContent[0] = 'Y'
	pending.deliveryChanges[0].Reasons[0] = "mutated"
	if string(persisted.DeliveryChanges[0].OldContent) != "old" ||
		string(persisted.DeliveryChanges[0].NewContent) != "new" ||
		persisted.DeliveryChanges[0].Reasons[0] != "reviewed" {
		t.Fatalf("persisted delivery aliased the live review: %+v", persisted.DeliveryChanges[0])
	}
	recovered := pendingSendFromPersisted(persisted)
	persisted.DeliveryChanges[0].OldContent[0] = 'Z'
	persisted.DeliveryChanges[0].Reasons[0] = "changed again"
	if string(recovered.deliveryChanges[0].OldContent) != "old" ||
		recovered.deliveryChanges[0].Reasons[0] != "reviewed" {
		t.Fatalf("recovered delivery aliased decoded state: %+v", recovered.deliveryChanges[0])
	}
}

func stageDeliveryCrashPoint(
	t *testing.T,
	statePath string,
	mirrorRoot string,
	point deliveryCrashPoint,
) (completion.Assignment, completion.Event) {
	t.Helper()
	store, err := workerstate.Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	manager := newFilesystemMirrorManager(mirrorRoot)
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role:   canonical.RoleUser,
		Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "write durable.txt"}},
	}}})
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
	event := completion.Event{
		ID: model.delivery.eventID, Type: completion.EventToolCalls,
		ToolCalls: append([]completion.ToolCall(nil), model.delivery.calls...),
	}
	updated, stateWrite := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	if model.pending.kind != pendingDelivery || model.pending.durable || stateWrite == nil {
		t.Fatalf("delivery did not enter save-ahead state: %+v", model.pending)
	}
	if len(client.events) != 0 {
		t.Fatal("delivery reached outbox before state journal")
	}
	stateResult := workerStateResult(t, stateWrite)
	updated, confirm := model.Update(stateResult)
	model = updated.(Model)
	if !model.pending.durable || confirm == nil || len(client.events) != 0 {
		t.Fatalf("state journal did not gate mirror intent: %+v", model.pending)
	}
	if point == crashAfterDeliveryState {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		return assignment, event
	}
	confirmation := commandResult(t, confirm).(mirrorConfirmationReady)
	if confirmation.err != nil || len(client.events) != 0 {
		t.Fatalf("mirror intent confirmation failed before %s: %+v", point, confirmation)
	}
	if point == crashAfterMirrorIntent {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		return assignment, event
	}
	updated, phaseWrite := model.Update(confirmation)
	model = updated.(Model)
	phaseResult := workerStateResult(t, phaseWrite)
	updated, outbox := model.Update(phaseResult)
	model = updated.(Model)
	outboxMessages := executeTestCommand(outbox)
	if len(client.events) != 1 || client.events[0].ID != event.ID {
		t.Fatalf("initial outbox event before crash = %+v", client.events)
	}
	if _, ok := firstTestMessage[eventSent](outboxMessages); !ok {
		t.Fatalf("outbox command produced no event acknowledgement: %#v", outboxMessages)
	}
	// Deliberately do not reduce eventSent: the pending journal must survive and
	// make the restart replay this same event idempotently.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return assignment, event
}

func executeTestCommand(command tea.Cmd) []tea.Msg {
	if command == nil {
		return nil
	}
	message := command()
	batch, ok := message.(tea.BatchMsg)
	if !ok {
		if started, ok := message.(mirrorWatchStarted); ok && started.cancel != nil {
			started.cancel()
		}
		return []tea.Msg{message}
	}
	messages := make([]tea.Msg, 0, len(batch))
	for _, child := range batch {
		messages = append(messages, executeTestCommand(child)...)
	}
	return messages
}

func firstTestMessage[T any](messages []tea.Msg) (T, bool) {
	for _, message := range messages {
		if typed, ok := message.(T); ok {
			return typed, true
		}
	}
	var zero T
	return zero, false
}

func containsEventSent(messages []tea.Msg) bool {
	_, ok := firstTestMessage[eventSent](messages)
	return ok
}

func mirrorDeliveryIntentCount(
	t *testing.T,
	mirrorRoot string,
	assignment completion.Assignment,
) int {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join(
		mirrorRoot, ".human-state", assignment.CallerID, assignment.WorkspaceKey, "baseline.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		Deliveries map[string]json.RawMessage `json:"deliveries"`
	}
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatal(err)
	}
	return len(state.Deliveries)
}
