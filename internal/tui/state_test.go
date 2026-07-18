package tui

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerstate"
)

func TestWorkerStateRestoresDraftsAcrossRestartAndRemoteSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	assignment := persistentRemoteAssignment("request-before-restart")
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(assignment)
	model.setReplyValue("reply after reconnect")
	model.setCommandValue("go test ./...")
	model.agentTasks = []agentTask{
		{Content: "preserve state", Status: taskInProgress, Priority: "high"},
		{Content: "resume work", Status: taskPending, Priority: "medium"},
	}
	model.taskSelected = 1
	model.taskDirty = true
	model.taskEditing = true
	model.taskEditIndex = 1
	model.taskInput = "resume work safely"
	model.focus = focusTasks
	model = flushWorkerState(t, model)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	restarted := New(newFakeClient(), WithStateStore(store))
	afterRestart := persistentRemoteAssignment("request-after-restart")
	restarted = restarted.activateAssignment(afterRestart)
	if restarted.replyInput != "reply after reconnect" || restarted.commandInput != "go test ./..." {
		t.Fatalf("text drafts were not restored: reply=%q command=%q", restarted.replyInput, restarted.commandInput)
	}
	if len(restarted.agentTasks) != 2 || restarted.agentTasks[0].Content != "preserve state" ||
		restarted.taskSelected != 1 || !restarted.taskDirty || !restarted.taskEditing ||
		restarted.taskEditIndex != 1 || restarted.taskInput != "resume work safely" {
		t.Fatalf("task draft was not restored: %+v", restarted)
	}
	if restarted.focus != focusTasks {
		t.Fatalf("input focus was not restored: %v", restarted.focus)
	}
	if !strings.Contains(restarted.status, "recovered local draft") {
		t.Fatalf("draft recovery is not visible: %q", restarted.status)
	}

	// Clearing every draft component deletes both the new-session snapshot and
	// the consumed old-session record. A later restart must not resurrect it.
	restarted.setReplyValue("")
	restarted.setCommandValue("")
	restarted.taskDirty = false
	restarted.taskEditing = false
	restarted.taskEditIndex = -1
	restarted.taskInput = ""
	restarted = flushWorkerState(t, restarted)
	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.Kind == workerStateDraftKind {
			t.Fatalf("cleared draft remained durable: %+v", record)
		}
	}
}

func TestWorkerStateChatDraftDoesNotCrossSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	chat := testAssignment()
	chat.CapabilityTier = completion.TierChat
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(chat)
	model.setReplyValue("belongs only to the original HTTP session")
	model = flushWorkerState(t, model)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	restarted := New(newFakeClient(), WithStateStore(store))
	otherSession := chat
	otherSession.IdempotencyKey = "different-request"
	restarted = restarted.activateAssignment(otherSession)
	if restarted.replyInput != "" || restarted.commandInput != "" || restarted.taskDirty {
		t.Fatalf("Chat draft crossed SessionKey: %+v", restarted)
	}
	if len(restarted.stateDrafts) != 1 {
		t.Fatalf("isolated original Chat draft was discarded: %+v", restarted.stateDrafts)
	}
}

func TestWorkerStateContinuationSurvivesRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	assignment := persistentRemoteAssignment("continuation-origin")
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(assignment)
	model.expectHandoff(assignment)
	model.active = nil
	model = flushWorkerState(t, model)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	client := newFakeClient()
	restarted := New(client, WithStateStore(store))
	if !restarted.continueHandoff || restarted.continueOrigin != assignment.SessionKey() ||
		restarted.continueCaller != assignment.CallerID || restarted.lastContext == nil {
		t.Fatalf("continuation was not recovered: %+v", restarted)
	}
	next := persistentRemoteAssignment("continuation-next")
	updated, _ := restarted.Update(networkMessage{Assignment: &next})
	restarted = updated.(Model)
	if restarted.pending.kind != pendingAccept || !restarted.pending.automatic ||
		restarted.pending.assignment.SessionKey() != next.SessionKey() {
		t.Fatalf("recovered continuation did not resume exact stable scope: %+v", restarted)
	}
}

func TestWorkerStateRestoresMultipleParkedContinuationsAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	first := persistentRemoteAssignment("continuation-first-origin")
	first.CallerID = "caller-first"
	first.WorkspaceKey = "workspace-first"
	first.TaskID = "task-first"
	second := persistentRemoteAssignment("continuation-second-origin")
	second.CallerID = "caller-second"
	second.WorkspaceKey = "workspace-second"
	second.TaskID = "task-second"

	model := New(newFakeClient(), WithStateStore(store))
	model.rememberContext(first)
	model.expectContinuation(first, []completion.ToolCall{{ID: "tool-first", Name: "read"}})
	model.rememberContext(second)
	model.expectContinuation(second, []completion.ToolCall{{ID: "tool-second", Name: "read"}})
	if len(model.parkedContinuations) != 1 || model.continueOrigin != second.SessionKey() {
		t.Fatalf("continuations were not independently staged before restart: %+v", model)
	}
	model = flushWorkerState(t, model)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	restarted := New(newFakeClient(), WithStateStore(store))
	if len(restarted.parkedContinuations) != 1 ||
		restarted.parkedContinuations[0].origin != first.SessionKey() ||
		restarted.continueOrigin != second.SessionKey() {
		t.Fatalf("restart did not recover every continuation: %+v", restarted)
	}

	firstResult := toolResultTurn(first, "continuation-first-result", "tool-first")
	if !restarted.matchesContinuation(firstResult) || restarted.continueOrigin != first.SessionKey() ||
		len(restarted.parkedContinuations) != 1 ||
		restarted.parkedContinuations[0].origin != second.SessionKey() {
		t.Fatalf("first recovered continuation did not resume independently: %+v", restarted)
	}
	restarted.clearContinuation()
	secondResult := toolResultTurn(second, "continuation-second-result", "tool-second")
	if !restarted.matchesContinuation(secondResult) || restarted.continueOrigin != second.SessionKey() ||
		len(restarted.parkedContinuations) != 0 {
		t.Fatalf("second recovered continuation was lost: %+v", restarted)
	}
}

func TestWorkerStateRestoresFocusWithoutRestoringOldCommandAuthority(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	authorized := persistentRemoteAssignment("authorized-command")
	authorized.Request.Tools = append(authorized.Request.Tools, canonical.Tool{
		Name: "bash",
		InputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"command":{"type":"string"}},
			"required":["command"],
			"additionalProperties":false
		}`),
	})
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(authorized)
	model.setCommandValue("go test ./internal/tui")
	model.commandConfirm = model.commandInput
	model.focus = focusCommand
	model = flushWorkerState(t, model)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	restarted := New(newFakeClient(), WithStateStore(store))
	downgraded := persistentRemoteAssignment("downgraded-command") // no bash declaration
	restarted = restarted.activateAssignment(downgraded)
	if restarted.commandInput != "go test ./internal/tui" {
		t.Fatalf("command draft was not restored: %q", restarted.commandInput)
	}
	if restarted.focus == focusCommand {
		t.Fatalf("old command authority restored focus against the new assignment: %+v", restarted)
	}
	if restarted.commandConfirm != "" {
		t.Fatalf("dangerous command confirmation survived restart: %q", restarted.commandConfirm)
	}
	if _, reason := restarted.commandTarget(); reason == "" {
		t.Fatal("downgraded assignment unexpectedly retained command authority")
	}
}

func TestWorkerStateBadRecordIsVisibleAndDoesNotBlockHealthyDraft(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := workerstate.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Bind(ctx, "scope:test-gateway", "worker"); err != nil {
		t.Fatal(err)
	}
	healthyAssignment := persistentRemoteAssignment("healthy-state")
	healthyScope := stateScope(healthyAssignment)
	healthyPayload, err := json.Marshal(persistedDraft{
		Version: workerStateVersion, Reply: "healthy draft", TaskEditIndex: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, healthyScope, workerStateDraftKind, healthyPayload); err != nil {
		t.Fatal(err)
	}
	badAssignment := persistentRemoteAssignment("bad-state")
	badPayload := json.RawMessage(`{"version":1,"has_tasks":true,"tasks":[{"content":"","status":"exploded","priority":"urgent"}]}`)
	if _, err := store.Put(ctx, stateScope(badAssignment), workerStateDraftKind, badPayload); err != nil {
		t.Fatal(err)
	}

	model := New(newFakeClient(), WithStateStore(store))
	if !strings.Contains(model.stateLoadWarning, "ignored 1 corrupt recovery record") ||
		!strings.Contains(model.visibleStatus(), "corrupt recovery record") {
		t.Fatalf("bad state warning is not visible: load=%q visible=%q", model.stateLoadWarning, model.visibleStatus())
	}
	model = model.activateAssignment(healthyAssignment)
	if model.replyInput != "healthy draft" {
		t.Fatalf("bad row blocked healthy draft recovery: %+v", model)
	}
}

func TestWorkerStateSerializesInFlightUpdatesSoLatestDraftWins(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := workerstate.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	assignment := persistentRemoteAssignment("rapid-input")
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(assignment)

	model.setReplyValue("older")
	updated, firstWrite := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	if !model.stateWriting || firstWrite == nil {
		t.Fatalf("first draft write was not started: %+v", model)
	}
	model.setReplyValue("latest")
	updated, whileWriting := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	if whileWriting != nil {
		// No UI command is expected for WindowSizeMsg. In particular, a second
		// concurrent SQLite write would permit completion-order rollback.
		t.Fatalf("second write started while the first was in flight: %T", whileWriting())
	}

	result := workerStateResult(t, firstWrite)
	updated, nextWrite := model.Update(result)
	model = updated.(Model)
	model = flushWorkerStateCommand(t, model, nextWrite)
	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %+v", records)
	}
	var persisted persistedDraft
	if err := json.Unmarshal(records[0].Payload, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Reply != "latest" {
		t.Fatalf("stale in-flight write won: %+v", persisted)
	}
}

func TestWorkerStatePendingSendSurvivesCrashBeforeOutboxCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	draftAssignment := persistentRemoteAssignment("draft-before-pending-send")
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(draftAssignment)
	model.focus = focusReply
	model.setReplyValue("exact reply that must survive the crash")
	model = flushWorkerState(t, model)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	assignment := persistentRemoteAssignment("pending-send-before-outbox")
	client := newFakeClient()
	model = New(client, WithStateStore(store)).activateAssignment(assignment)
	if model.replyInput != "exact reply that must survive the crash" {
		t.Fatalf("older-session draft did not enter the new remote request: %+v", model)
	}

	updated, _ := model.sendReply(completion.EventProgress, false)
	model = updated.(Model)
	updated, intentWrite := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	intentResult := workerStateResult(t, intentWrite)
	updated, outboxWrite := model.Update(intentResult)
	model = updated.(Model)
	if outboxWrite == nil || !model.pending.durable {
		t.Fatalf("pending response did not reach the durable pre-outbox boundary: %+v", model.pending)
	}
	if len(client.events) != 0 {
		t.Fatalf("event reached the outbox before its recovery intent committed: %+v", client.events)
	}
	// Do not execute outboxWrite. This is the exact crash window: the editor was
	// cleared and the send intent committed, while neither the local outbox write
	// nor the old draft-row cleanup has run.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	replayClient := newFakeClient()
	restarted := New(replayClient, WithStateStore(store))
	if restarted.pending.kind != pendingReply || !restarted.pending.durable ||
		!restarted.pending.recovered || restarted.pending.reply != "exact reply that must survive the crash" ||
		restarted.pending.event.Text != "exact reply that must survive the crash\n\n" {
		t.Fatalf("restart did not recover the exact pending event: %+v", restarted.pending)
	}
	if restarted.active == nil || restarted.active.SessionKey() != assignment.SessionKey() || restarted.lastContext == nil {
		t.Fatalf("recovered progress did not restore its active stream: %+v", restarted)
	}
	if got := countLocalAssistantText(restarted.lastContext.Request.Messages, "exact reply that must survive the crash"); got != 1 {
		t.Fatalf("optimistic recovered progress count = %d, want 1", got)
	}
	originalEvent := restarted.pending.event
	replayed := assignment
	replayed.LeaseOwner = "worker"
	replayed.HarnessSessionID = "refreshed-routing"
	updated, replayState := restarted.Update(networkMessage{Assignment: &replayed})
	restarted = updated.(Model)
	if len(restarted.assignments) != 0 || restarted.active == nil ||
		restarted.pending.assignment.HarnessSessionID != "refreshed-routing" ||
		!reflect.DeepEqual(restarted.pending.event, originalEvent) {
		t.Fatalf("same-session replay was not suppressed/refreshed exactly: %+v", restarted)
	}
	restarted = settleTrailingStateWrites(t, restarted, replayState)
	if _, stale := restarted.stateDrafts[stateRecordKey{
		scope: stateScope(assignment), kind: workerStateDraftKind,
	}]; stale {
		t.Fatal("pre-intent draft remained independently restorable after the exact event became authoritative")
	}

	sent := sendPersistedEvent(replayClient, store, restarted.pending)()
	ack, ok := sent.(eventSent)
	if !ok || ack.err != nil {
		t.Fatalf("recovered event handoff = %#v", sent)
	}
	updated, cleanup := restarted.Update(ack)
	restarted = updated.(Model)
	if restarted.active == nil || restarted.active.SessionKey() != assignment.SessionKey() {
		t.Fatalf("progress handoff closed its recovered stream: %+v", restarted)
	}
	restarted = flushWorkerStateCommand(t, restarted, cleanup)
	if len(replayClient.events) != 1 || replayClient.events[0].ID != ack.eventID ||
		replayClient.events[0].Text != "exact reply that must survive the crash\n\n" {
		t.Fatalf("restart did not enqueue exactly one original event: %+v", replayClient.events)
	}
	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.Kind == workerStatePendingSendKind ||
			(record.Kind == workerStateDraftKind && recoverableDraftScope(record.Scope, stateScope(assignment))) {
			t.Fatalf("completed recovery state was not cleaned up: %+v", records)
		}
	}
}

func TestWorkerStateSendFailureDecisionSurvivesCrashBeforeUIRestore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	assignment := persistentRemoteAssignment("pending-send-restore")
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(assignment)
	model.focus = focusReply
	model.setReplyValue("restore me instead of silently retrying")
	model = flushWorkerState(t, model)
	updated, _ := model.sendReply(completion.EventProgress, false)
	model = updated.(Model)
	updated, intentWrite := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	updated, outboxWrite := model.Update(workerStateResult(t, intentWrite))
	model = updated.(Model)
	if outboxWrite == nil || !model.pending.durable {
		t.Fatalf("pending response did not reach the durable pre-outbox boundary: %+v", model.pending)
	}

	failing := newFakeClient()
	failing.sendErr = errors.New("local outbox unavailable")
	// Re-run the command with a deterministic failing client. sendPersistedEvent
	// commits disposition=restore before returning eventSent; dropping that UI
	// message simulates a crash after the decision but before in-memory restore.
	decision := sendPersistedEvent(failing, store, model.pending)()
	message, ok := decision.(eventSent)
	if !ok || message.err == nil || len(message.restorePayload) == 0 || message.intentErr != nil {
		t.Fatalf("send failure did not durably choose draft restoration: %#v", decision)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	restarted := New(newFakeClient(), WithStateStore(store))
	if restarted.pending.kind != pendingNone {
		t.Fatalf("durably failed event was silently scheduled for retry: %+v", restarted.pending)
	}
	restarted = restarted.activateAssignment(assignment)
	if restarted.replyInput != "restore me instead of silently retrying" || restarted.focus != focusReply {
		t.Fatalf("durable restore decision did not recover the editor draft: %+v", restarted)
	}
}

func TestPendingIntentCrashPreservesEveryUnsentPane(t *testing.T) {
	tests := []struct {
		name string
		kind pendingSendKind
		send func(Model) (tea.Model, tea.Cmd)
	}{
		{name: "reply", kind: pendingReply, send: func(model Model) (tea.Model, tea.Cmd) {
			return model.sendReply(completion.EventProgress, false)
		}},
		{name: "command", kind: pendingCommand, send: func(model Model) (tea.Model, tea.Cmd) {
			return model.sendCommand()
		}},
		{name: "tasks", kind: pendingTasks, send: func(model Model) (tea.Model, tea.Cmd) {
			return model.sendAgentTasks()
		}},
		{name: "advanced tools", kind: pendingAdvancedTools, send: func(model Model) (tea.Model, tea.Cmd) {
			return model.sendDeclaredToolCalls()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state", "worker-state.db")
			store, err := workerstate.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			assignment := allPaneStateAssignment("all-panes-" + test.name)
			model := New(newFakeClient(), WithStateStore(store)).activateAssignment(assignment)
			populateEveryPane(&model)
			model = flushWorkerState(t, model)

			updated, _ := test.send(model)
			model = updated.(Model)
			if model.pending.kind != test.kind {
				t.Fatalf("staged kind = %d, want %d: %+v", model.pending.kind, test.kind, model)
			}
			updated, write := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
			model = updated.(Model)
			result := workerStateResult(t, write)
			if result.err != nil || result.operation.key.kind != workerStatePendingSendKind {
				t.Fatalf("first committed post-send write = %+v, want pending intent", result)
			}
			// Crash after the pending transaction commits but before Bubble Tea
			// reduces its result. The older whole-editor draft is still present.
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = workerstate.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			restarted := New(newFakeClient(), WithStateStore(store))
			if restarted.pending.kind != test.kind || !restarted.pending.recovered {
				t.Fatalf("pending event not recovered: %+v", restarted.pending)
			}
			if restarted.active == nil {
				restarted = restarted.activateAssignment(assignment)
			}
			assertOtherPanesSurvived(t, restarted, test.kind)
		})
	}
}

func TestSendWaitsForOutstandingStateTransactionWithoutMutatingDrafts(t *testing.T) {
	tests := []struct {
		name string
		kind pendingSendKind
		send func(Model) (tea.Model, tea.Cmd)
	}{
		{name: "reply", kind: pendingReply, send: func(model Model) (tea.Model, tea.Cmd) {
			return model.sendReply(completion.EventProgress, false)
		}},
		{name: "command", kind: pendingCommand, send: func(model Model) (tea.Model, tea.Cmd) { return model.sendCommand() }},
		{name: "tasks", kind: pendingTasks, send: func(model Model) (tea.Model, tea.Cmd) { return model.sendAgentTasks() }},
		{name: "advanced", kind: pendingAdvancedTools, send: func(model Model) (tea.Model, tea.Cmd) { return model.sendDeclaredToolCalls() }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := workerstate.Open(ctx, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			model := New(newFakeClient(), WithStateStore(store)).activateAssignment(allPaneStateAssignment("busy-" + test.name))
			populateEveryPane(&model)
			before, ok := model.currentPersistedDraft()
			if !ok {
				t.Fatal("test draft is empty")
			}
			model.stateWriting = true // exact state after a DB command ran but before its result was reduced
			updated, command := test.send(model)
			after := updated.(Model)
			got, _ := after.currentPersistedDraft()
			if command != nil || after.pending.kind != pendingNone || after.deferredSend.kind != test.kind ||
				after.deferredSend.sessionKey != model.active.SessionKey() || !reflect.DeepEqual(got, before) {
				t.Fatalf("busy state send mutated draft: before=%+v after=%+v pending=%+v", before, got, after.pending)
			}
			if !strings.Contains(after.status, "input locked") {
				t.Fatalf("busy state diagnosis = %q", after.status)
			}
		})
	}
}

func TestDeferredReplyProgressSendsOnceAfterLatestDraftIsDurable(t *testing.T) {
	ctx := context.Background()
	store, err := workerstate.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	assignment := allPaneStateAssignment("deferred-progress")
	model := New(client, WithStateStore(store)).activateAssignment(assignment)
	model.focus = focusReply
	model.setReplyValue("older snapshot")
	updated, olderWrite := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	model.setReplyValue("exact progress")

	updated, send := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	if send != nil || model.deferredSend.kind != pendingReply || model.replyInput != "exact progress" ||
		model.pending.kind != pendingNone || len(client.events) != 0 {
		t.Fatalf("single Enter did not queue an untouched progress draft: model=%+v events=%+v", model, client.events)
	}

	model, send = applyStateWrite(t, model, olderWrite)
	model, send = applyStateWrite(t, model, send)
	if model.deferredSend.kind != pendingNone || model.pending.kind != pendingReply ||
		model.pending.event.Type != completion.EventProgress || model.replyInput != "" || model.active == nil ||
		len(client.events) != 0 {
		t.Fatalf("deferred progress was not staged exactly after draft sync: model=%+v events=%+v", model, client.events)
	}
	model, send = applyStateWrite(t, model, send)
	ack := deferredEventResult(t, send)
	updated, _ = model.Update(ack)
	model = updated.(Model)
	if ack.err != nil || len(client.events) != 1 || client.events[0].Type != completion.EventProgress ||
		client.events[0].Text != "exact progress\n\n" || model.pending.kind != pendingNone || model.active == nil {
		t.Fatalf("deferred progress delivery = ack=%+v model=%+v events=%+v", ack, model, client.events)
	}
}

func TestDeferredCtrlDFinalClosesOnlyAfterLatestDraftIsDurable(t *testing.T) {
	ctx := context.Background()
	store, err := workerstate.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	assignment := allPaneStateAssignment("deferred-final")
	model := New(client, WithStateStore(store)).activateAssignment(assignment)
	model.focus = focusReply
	model.setReplyValue("older final")
	updated, olderWrite := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	model.setReplyValue("exact final")

	updated, _ = model.Update(tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl})
	model = updated.(Model)
	if model.deferredSend.kind != pendingReply || model.deferredSend.replyType != completion.EventFinal ||
		!model.deferredSend.endResponse || !model.deferredSend.allowEmpty || model.active == nil {
		t.Fatalf("Ctrl+D did not queue the final gesture without closing the stream: %+v", model)
	}
	model, next := applyStateWrite(t, model, olderWrite)
	model, next = applyStateWrite(t, model, next)
	if model.active != nil || model.pending.event.Type != completion.EventFinal || len(client.events) != 0 {
		t.Fatalf("final mutated at the wrong durability boundary: model=%+v events=%+v", model, client.events)
	}
	model, next = applyStateWrite(t, model, next)
	ack := deferredEventResult(t, next)
	updated, _ = model.Update(ack)
	model = updated.(Model)
	if ack.err != nil || len(client.events) != 1 || client.events[0].Type != completion.EventFinal ||
		client.events[0].Text != "exact final" || model.active != nil || model.pending.kind != pendingNone {
		t.Fatalf("deferred final delivery = ack=%+v model=%+v events=%+v", ack, model, client.events)
	}
}

func TestDeferredSendEscCancelsAndFreezesDraft(t *testing.T) {
	ctx := context.Background()
	store, err := workerstate.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(allPaneStateAssignment("deferred-esc"))
	model.focus = focusReply
	model.setReplyValue("keep exactly")
	model.stateWriting = true
	updated, _ := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	updated, _ = model.Update(press("x", 'x'))
	model = updated.(Model)
	updated, _ = model.Update(tea.PasteMsg{Content: "ignored paste"})
	model = updated.(Model)
	if model.replyInput != "keep exactly" || model.deferredSend.kind != pendingReply {
		t.Fatalf("input changed while deferred send was waiting: %+v", model)
	}
	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	model = updated.(Model)
	if model.deferredSend.kind != pendingNone || model.replyInput != "keep exactly" || model.active == nil ||
		!strings.Contains(model.status, "draft retained") {
		t.Fatalf("Esc did not surgically cancel the queued gesture: %+v", model)
	}
}

func TestDeferredSendRetriesStateWriteAndHandsOffExactlyOnce(t *testing.T) {
	ctx := context.Background()
	store, err := workerstate.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	model := New(client, WithStateStore(store)).activateAssignment(allPaneStateAssignment("deferred-retry"))
	model.focus = focusReply
	model.setReplyValue("old")
	updated, firstWrite := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	model.setReplyValue("retry exactly once")
	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)

	ambiguous := workerStateResult(t, firstWrite)
	ambiguous.err = errors.New("injected state write failure")
	updated, _ = model.Update(ambiguous)
	model = updated.(Model)
	if !model.stateRetryPending || model.deferredSend.kind != pendingReply || len(client.events) != 0 {
		t.Fatalf("state failure released the deferred send: model=%+v events=%+v", model, client.events)
	}
	updated, retryWrite := model.Update(stateRetryReady{})
	model = updated.(Model)
	model, retryWrite = applyStateWrite(t, model, retryWrite)
	if model.pending.kind != pendingReply || len(client.events) != 0 {
		t.Fatalf("retry did not stage one pending intent: model=%+v events=%+v", model, client.events)
	}
	model, retryWrite = applyStateWrite(t, model, retryWrite)
	ack := deferredEventResult(t, retryWrite)
	updated, _ = model.Update(ack)
	model = updated.(Model)
	if ack.err != nil || len(client.events) != 1 || client.events[0].Text != "retry exactly once\n\n" ||
		model.pending.kind != pendingNone {
		t.Fatalf("retry handoff was not exact-once: ack=%+v model=%+v events=%+v", ack, model, client.events)
	}
}

func TestDeferredSendSessionLossPinsDraftBeforeCommittedDeleteIsReduced(t *testing.T) {
	ctx := context.Background()
	store, err := workerstate.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	original := allPaneStateAssignment("deferred-session-loss")
	model := New(client, WithStateStore(store)).activateAssignment(original)
	model.focus = focusReply
	model.setReplyValue("older durable draft")
	model = flushWorkerState(t, model)
	model.setReplyValue("")
	updated, deletion := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	deleteResult := workerStateResult(t, deletion) // committed, not yet reduced
	if !deleteResult.operation.delete {
		t.Fatalf("expected committed draft delete, got %+v", deleteResult)
	}
	model.setReplyValue("must survive old-session loss")
	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	if model.deferredSend.kind != pendingReply {
		t.Fatalf("send was not deferred in the delete window: %+v", model)
	}

	replacement := allPaneStateAssignment("replacement-session")
	model.active = &replacement
	model.setReplyValue("replacement draft")
	updated, repair := model.Update(deleteResult)
	model = updated.(Model)
	if model.deferredSend.kind != pendingNone || model.pending.kind != pendingNone ||
		model.replyInput != "replacement draft" || len(client.events) != 0 {
		t.Fatalf("inactive deferred send crossed sessions: model=%+v events=%+v", model, client.events)
	}
	model = flushWorkerStateCommand(t, model, repair)
	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range records {
		if record.Kind != workerStateDraftKind || record.Scope != stateScope(original) {
			continue
		}
		var draft persistedDraft
		if err := json.Unmarshal(record.Payload, &draft); err != nil {
			t.Fatal(err)
		}
		if draft.Reply != "must survive old-session loss" {
			t.Fatalf("pinned source draft = %+v", draft)
		}
		found = true
	}
	if !found {
		t.Fatalf("source-session draft was deleted after cancellation: %+v", records)
	}
}

func TestDeferredSendStillFailsClosedBeforeWorkerIdentityBinding(t *testing.T) {
	ctx := context.Background()
	store, err := workerstate.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	client.identity = workerclient.Identity{}
	model := New(client, WithStateStore(store)).activateAssignment(allPaneStateAssignment("unbound-deferred"))
	model.focus = focusReply
	model.setReplyValue("do not send unbound")
	updated, command := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	if command != nil || model.deferredSend.kind != pendingNone || model.pending.kind != pendingNone ||
		model.replyInput != "do not send unbound" || len(client.events) != 0 ||
		!strings.Contains(model.status, "sending remains disabled") {
		t.Fatalf("unbound state queued or sent an event: model=%+v events=%+v", model, client.events)
	}
}

func TestSendAfterCommittedDeleteBeforeResultReductionKeepsReply(t *testing.T) {
	ctx := context.Background()
	store, err := workerstate.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(allPaneStateAssignment("delete-window"))
	model.setReplyValue("old durable reply")
	model = flushWorkerState(t, model)
	model.setReplyValue("")
	updated, deletion := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	result := workerStateResult(t, deletion) // DELETE committed in SQLite
	if result.err != nil || !result.operation.delete || !model.stateWriting {
		t.Fatalf("did not reach committed-delete/unreduced-result window: result=%+v model=%+v", result, model)
	}
	model.setReplyValue("new reply typed during result delivery")
	updated, command := model.sendReply(completion.EventProgress, false)
	model = updated.(Model)
	if command != nil || model.pending.kind != pendingNone || model.replyInput != "new reply typed during result delivery" {
		t.Fatalf("send crossed an unsettled delete: %+v", model)
	}
}

func TestRecoveredFinalSuppressesReplayWithoutReopeningStream(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	assignment := allPaneStateAssignment("recovered-final")
	model := New(newFakeClient(), WithStateStore(store)).activateAssignment(assignment)
	model.setReplyValue("final body")
	model = flushWorkerState(t, model)
	updated, _ := model.sendReply(completion.EventFinal, true)
	model = updated.(Model)
	updated, write := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	result := workerStateResult(t, write)
	if result.err != nil || result.operation.key.kind != workerStatePendingSendKind {
		t.Fatalf("pending final was not first durable write: %+v", result)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client := newFakeClient()
	restarted := New(client, WithStateStore(store))
	if restarted.pending.event.Type != completion.EventFinal || restarted.active != nil {
		t.Fatalf("final recovery reopened an active stream: %+v", restarted)
	}
	original := restarted.pending.event
	replayed := assignment
	replayed.LeaseOwner = "worker"
	replayed.HarnessSessionID = "new-route"
	updated, _ = restarted.Update(networkMessage{Assignment: &replayed})
	restarted = updated.(Model)
	if len(restarted.assignments) != 0 || restarted.active != nil ||
		restarted.pending.assignment.HarnessSessionID != "new-route" ||
		!reflect.DeepEqual(restarted.pending.event, original) {
		t.Fatalf("final replay was not suppressed with stable event body: %+v", restarted)
	}
	message := sendPersistedEvent(client, store, restarted.pending)()
	ack, ok := message.(eventSent)
	if !ok || ack.err != nil {
		t.Fatalf("final replay handoff = %#v", message)
	}
	updated, _ = restarted.Update(ack)
	restarted = updated.(Model)
	if restarted.pending.kind != pendingNone || restarted.active != nil || len(client.events) != 1 ||
		client.events[0].Type != completion.EventFinal || client.events[0].Text != "final body" {
		t.Fatalf("final recovery result = model=%+v events=%+v", restarted, client.events)
	}
	updated, _ = restarted.Update(networkMessage{Assignment: &replayed})
	restarted = updated.(Model)
	if len(restarted.assignments) != 0 || restarted.active != nil {
		t.Fatalf("post-handoff source replay entered Inbox: %+v", restarted)
	}
}

func TestMultipleRecoveredPendingEventsResumeInOrder(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	_ = New(client, WithStateStore(store)) // authenticated fake binds the namespace
	firstAssignment := allPaneStateAssignment("multi-progress")
	secondAssignment := allPaneStateAssignment("multi-final")
	secondAssignment.CallerID = "second-caller"
	secondAssignment.WorkspaceKey = "second-workspace"
	secondAssignment.TaskID = "second-task"
	first := pendingSend{
		kind: pendingReply, assignment: firstAssignment, reply: "first progress",
		event:          completion.Event{ID: "event-multi-progress", Type: completion.EventProgress, Text: "first progress\n\n"},
		remainingDraft: persistedDraft{Version: workerStateVersion, TaskEditIndex: -1},
	}
	second := pendingSend{
		kind: pendingReply, assignment: secondAssignment, reply: "second final",
		event:          completion.Event{ID: "event-multi-final", Type: completion.EventFinal, Text: "second final"},
		remainingDraft: persistedDraft{Version: workerStateVersion, TaskEditIndex: -1},
	}
	for _, pending := range []pendingSend{first, second} {
		payload, marshalErr := json.Marshal(persistedPendingFromSend(pending, pendingSendDispositionSend))
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		key := pendingSendStateKey(pending)
		if _, err := store.Put(ctx, key.scope, key.kind, payload); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	client = newFakeClient()
	model := New(client, WithStateStore(store))
	if model.pending.event.ID != first.event.ID || len(model.pendingRecoveries) != 1 ||
		model.active == nil || model.active.SessionKey() != firstAssignment.SessionKey() {
		t.Fatalf("pending order/first progress recovery = %+v", model)
	}
	firstAck := sendPersistedEvent(client, store, model.pending)().(eventSent)
	updated, next := model.Update(firstAck)
	model = updated.(Model)
	if model.pending.event.ID != second.event.ID || model.active == nil ||
		model.active.SessionKey() != firstAssignment.SessionKey() || next == nil {
		t.Fatalf("second pending did not resume without replacing active progress: %+v", model)
	}
	secondReplay := secondAssignment
	secondReplay.HarnessSessionID = "second-refreshed"
	updated, _ = model.Update(networkMessage{Assignment: &secondReplay})
	model = updated.(Model)
	if len(model.assignments) != 0 || model.pending.assignment.HarnessSessionID != "second-refreshed" {
		t.Fatalf("queued terminal replay entered Inbox: %+v", model)
	}
}

func TestPendingDraftReconciliationPreservesNewerReplyTail(t *testing.T) {
	tests := []struct {
		name        string
		disposition string
		newerTail   bool
		wantReply   string
	}{
		{name: "send uses atomic remaining panes", disposition: pendingSendDispositionSend, wantReply: ""},
		{name: "restore uses original once", disposition: pendingSendDispositionRestore, wantReply: "sent segment"},
		{name: "send keeps post-intent tail", disposition: pendingSendDispositionSend, newerTail: true, wantReply: "new tail"},
		{name: "restore prepends original to post-intent tail", disposition: pendingSendDispositionRestore, newerTail: true, wantReply: "sent segment\n\nnew tail"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state", "worker.db")
			store, err := workerstate.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			_ = New(client, WithStateStore(store))
			assignment := allPaneStateAssignment("tail-" + test.name)
			scope := stateScope(assignment)
			before := persistedDraft{
				Version: workerStateVersion, Focus: persistedFocusReply,
				Reply: "sent segment", Command: "keep command", TaskEditIndex: -1,
			}
			payload, _ := json.Marshal(before)
			if _, err := store.Put(ctx, scope, workerStateDraftKind, payload); err != nil {
				t.Fatal(err)
			}
			pending := pendingSend{
				kind: pendingReply, assignment: assignment, reply: "sent segment",
				event: completion.Event{ID: "event-tail", Type: completion.EventProgress, Text: "sent segment\n\n"},
				remainingDraft: persistedDraft{
					Version: workerStateVersion, Focus: persistedFocusCommand,
					Command: "keep command", TaskEditIndex: -1,
				},
			}
			pendingPayload, _ := json.Marshal(persistedPendingFromSend(pending, pendingSendDispositionSend))
			if _, err := store.Put(ctx, scope, workerStatePendingSendKind, pendingPayload); err != nil {
				t.Fatal(err)
			}
			if test.newerTail {
				tail := persistedDraft{
					Version: workerStateVersion, Focus: persistedFocusReply,
					Reply: "new tail", Command: "keep command", TaskEditIndex: -1,
				}
				tailPayload, _ := json.Marshal(tail)
				if _, err := store.Put(ctx, scope, workerStateDraftKind, tailPayload); err != nil {
					t.Fatal(err)
				}
			}
			if test.disposition == pendingSendDispositionRestore {
				restorePayload, _ := json.Marshal(persistedPendingFromSend(pending, pendingSendDispositionRestore))
				if _, err := store.Put(ctx, scope, workerStatePendingSendKind, restorePayload); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			rewriteWorkerStateWallTimes(t, path, test.newerTail)
			store, err = workerstate.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			restarted := New(newFakeClient(), WithStateStore(store))
			if restarted.active == nil {
				restarted = restarted.activateAssignment(assignment)
			}
			if restarted.replyInput != test.wantReply || restarted.commandInput != "keep command" {
				t.Fatalf("reconciled draft = reply %q command %q", restarted.replyInput, restarted.commandInput)
			}
		})
	}
}

func TestWorkerStateLoadsOnlyAfterAuthenticatedIdentityAndSeparatesSubjects(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "worker.db")
	assignment := persistentRemoteAssignment("identity-draft")
	identityA := workerclient.Identity{GatewayID: "scope:gateway-a", WorkerID: "worker-a"}
	identityB := workerclient.Identity{GatewayID: "scope:gateway-a", WorkerID: "worker-b"}

	store, err := workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	clientA := newFakeClient()
	clientA.identity = identityA
	model := New(clientA, WithStateStore(store)).activateAssignment(assignment)
	model.setReplyValue("subject A private draft")
	model = flushWorkerState(t, model)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	offline := newFakeClient()
	offline.identity = workerclient.Identity{}
	beforeHello := New(offline, WithStateStore(store))
	if beforeHello.stateBound || beforeHello.stateLoaded || len(beforeHello.stateDrafts) != 0 ||
		beforeHello.replyInput != "" || beforeHello.pending.kind != pendingNone {
		t.Fatalf("state was visible before authenticated Hello: %+v", beforeHello)
	}
	updated, _ := beforeHello.Update(networkMessage{IdentityReady: &identityA})
	afterHello := updated.(Model)
	if !afterHello.stateBound || !afterHello.stateLoaded || len(afterHello.stateDrafts) != 1 {
		t.Fatalf("identity-ready did not load subject state: %+v", afterHello)
	}
	afterHello = afterHello.activateAssignment(assignment)
	if afterHello.replyInput != "subject A private draft" {
		t.Fatalf("subject A draft did not restore after Hello: %+v", afterHello)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	clientB := newFakeClient()
	clientB.identity = identityB
	otherSubject := New(clientB, WithStateStore(store)).activateAssignment(assignment)
	if otherSubject.replyInput != "" || len(otherSubject.stateDrafts) != 0 || otherSubject.pending.kind != pendingNone {
		t.Fatalf("subject B observed subject A state: %+v", otherSubject)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// A token rotation does not alter the authenticated subject or gateway
	// identity. A fresh client instance therefore recovers the same namespace.
	store, err = workerstate.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rotatedCredential := newFakeClient()
	rotatedCredential.identity = identityA
	recovered := New(rotatedCredential, WithStateStore(store)).activateAssignment(assignment)
	if recovered.replyInput != "subject A private draft" {
		t.Fatalf("same-subject credential rotation lost state: %+v", recovered)
	}
}

func allPaneStateAssignment(idempotencyKey string) completion.Assignment {
	assignment := persistentRemoteAssignment(idempotencyKey)
	assignment.ExecAllowed = true
	assignment.Request.Tools = append(assignment.Request.Tools,
		canonical.Tool{Name: "bash", InputSchema: json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`)},
		canonical.Tool{Name: "search", InputSchema: json.RawMessage(`{"type":"object"}`)},
	)
	return assignment
}

func populateEveryPane(model *Model) {
	model.setReplyValue("keep reply")
	model.setCommandValue("pwd")
	model.agentTasks = []agentTask{{Content: "keep task", Status: taskPending, Priority: "medium"}}
	model.taskSelected = 0
	model.taskDirty = true
	model.composing = true
	model.input = `search {"query":"keep tool"}`
	model.toolCallIDs = []string{"tool-keep-stable"}
}

func assertOtherPanesSurvived(t *testing.T, model Model, sent pendingSendKind) {
	t.Helper()
	wantReply := sent != pendingReply
	wantCommand := sent != pendingCommand
	wantTasks := sent != pendingTasks
	wantTools := sent != pendingAdvancedTools
	if (model.replyInput == "keep reply") != wantReply ||
		(model.commandInput == "pwd") != wantCommand ||
		(len(model.agentTasks) == 1 && model.agentTasks[0].Content == "keep task" && model.taskDirty) != wantTasks ||
		(model.composing && model.input == `search {"query":"keep tool"}` &&
			reflect.DeepEqual(model.toolCallIDs, []string{"tool-keep-stable"})) != wantTools {
		t.Fatalf("pane recovery after kind %d = reply=%q command=%q tasks=%+v dirty=%t compose=%t input=%q ids=%v",
			sent, model.replyInput, model.commandInput, model.agentTasks, model.taskDirty,
			model.composing, model.input, model.toolCallIDs)
	}
}

func countLocalAssistantText(messages []canonical.Message, text string) int {
	count := 0
	for _, message := range messages {
		if message.Role != canonical.RoleAssistant {
			continue
		}
		for _, block := range message.Blocks {
			if block.Type == canonical.BlockText && block.Text == text {
				count++
			}
		}
	}
	return count
}

func rewriteWorkerStateWallTimes(t *testing.T, path string, newerDraft bool) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`UPDATE worker_state SET updated_at = ?`, time.Unix(100, 0).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if newerDraft {
		// The causally newer draft now has an earlier wall time than the intent.
		if _, err := database.Exec(`UPDATE worker_state SET updated_at = ? WHERE kind = ?`,
			time.Unix(50, 0).UnixNano(), workerStateDraftKind,
		); err != nil {
			t.Fatal(err)
		}
	}
}

func persistentRemoteAssignment(idempotencyKey string) completion.Assignment {
	assignment := remoteStreamAssignment()
	assignment.IdempotencyKey = idempotencyKey
	assignment.Request.Tools = append(assignment.Request.Tools, taskTool("todowrite", openCodeTodoWriteSchema))
	return assignment
}

func flushWorkerState(t *testing.T, model Model) Model {
	t.Helper()
	updated, command := model.Update(tea.WindowSizeMsg{Width: model.width, Height: model.height})
	model = updated.(Model)
	return flushWorkerStateCommand(t, model, command)
}

func flushWorkerStateCommand(t *testing.T, model Model, command tea.Cmd) Model {
	t.Helper()
	commands := []tea.Cmd{command}
	for steps := 0; len(commands) > 0; steps++ {
		if steps > 1000 {
			t.Fatal("worker state command queue did not quiesce")
		}
		command = commands[0]
		commands = commands[1:]
		if command == nil {
			continue
		}
		message := command()
		if batch, ok := message.(tea.BatchMsg); ok {
			commands = append(commands, batch...)
			continue
		}
		switch message := message.(type) {
		case stateWriteResult:
			updated, next := model.Update(message)
			model = updated.(Model)
			commands = append(commands, next)
		case stateRetryReady:
			updated, next := model.Update(message)
			model = updated.(Model)
			commands = append(commands, next)
		}
	}
	if model.stateWriting {
		t.Fatal("worker state write remained in flight")
	}
	return model
}

func workerStateResult(t *testing.T, command tea.Cmd) stateWriteResult {
	t.Helper()
	if command == nil {
		t.Fatal("expected worker state command")
	}
	queue := []tea.Cmd{command}
	for len(queue) > 0 {
		command = queue[0]
		queue = queue[1:]
		if command == nil {
			continue
		}
		message := command()
		if batch, ok := message.(tea.BatchMsg); ok {
			queue = append(queue, batch...)
			continue
		}
		if result, ok := message.(stateWriteResult); ok {
			return result
		}
	}
	t.Fatal("worker state command returned no result")
	return stateWriteResult{}
}

func applyStateWrite(t *testing.T, model Model, command tea.Cmd) (Model, tea.Cmd) {
	t.Helper()
	result := workerStateResult(t, command)
	updated, next := model.Update(result)
	return updated.(Model), next
}

func deferredEventResult(t *testing.T, command tea.Cmd) eventSent {
	t.Helper()
	if command == nil {
		t.Fatal("expected deferred event command")
	}
	queue := []tea.Cmd{command}
	for len(queue) > 0 {
		command = queue[0]
		queue = queue[1:]
		if command == nil {
			continue
		}
		message := command()
		if batch, ok := message.(tea.BatchMsg); ok {
			queue = append(queue, batch...)
			continue
		}
		if result, ok := message.(eventSent); ok {
			return result
		}
	}
	t.Fatal("deferred event command returned no event result")
	return eventSent{}
}

func settleTrailingStateWrites(t *testing.T, model Model, command tea.Cmd) Model {
	t.Helper()
	for model.stateWriting {
		result := trailingStateWriteResult(t, command)
		updated, next := model.Update(result)
		model = updated.(Model)
		command = next
	}
	return model
}

func trailingStateWriteResult(t *testing.T, command tea.Cmd) stateWriteResult {
	t.Helper()
	if command == nil {
		t.Fatal("state write is marked in flight without a command")
	}
	message := command()
	for {
		batch, ok := message.(tea.BatchMsg)
		if !ok {
			break
		}
		if len(batch) == 0 {
			t.Fatal("empty command batch while state write is in flight")
		}
		// Update appends nextStateCommand after network/UI commands. Descend from
		// the tail so a waitForNetwork command is never executed by this helper.
		message = batch[len(batch)-1]()
	}
	result, ok := message.(stateWriteResult)
	if !ok {
		t.Fatalf("trailing state command returned %T", message)
	}
	return result
}
