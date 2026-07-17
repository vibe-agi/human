package tui

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
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
