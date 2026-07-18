package tui

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	workmirror "github.com/vibe-agi/human/internal/mirror"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerproto"
)

func TestAcceptAndRejectOutboxFailuresKeepInboxAssignment(t *testing.T) {
	tests := []struct {
		name      string
		key       tea.KeyPressMsg
		wantKind  pendingSendKind
		wantEvent completion.EventType
	}{
		{
			name:      "accept",
			key:       tea.KeyPressMsg{Text: "a", Code: 'a'},
			wantKind:  pendingAccept,
			wantEvent: completion.EventAccepted,
		},
		{
			name:      "reject",
			key:       tea.KeyPressMsg{Text: "r", Code: 'r'},
			wantKind:  pendingReject,
			wantEvent: completion.EventRejected,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			client.sendErr = definitelyNotStored("outbox unavailable")
			assignment := testAssignment()
			model := updateModel(t, New(client), networkMessage{Assignment: &assignment})

			model, send := updateModelWithCommand(t, model, test.key)
			if send == nil || model.pending.kind != test.wantKind || len(model.assignments) != 1 {
				t.Fatalf("staged %s = %+v", test.name, model)
			}
			model = finishCommand(t, model, send)

			if model.pending.kind != pendingNone || model.active != nil || len(model.assignments) != 1 {
				t.Fatalf("failed %s lost or activated Inbox request: %+v", test.name, model)
			}
			if model.assignments[0].SessionKey() != assignment.SessionKey() {
				t.Fatalf("failed %s kept wrong request: %+v", test.name, model.assignments)
			}
			if len(client.events) != 1 || client.events[0].Type != test.wantEvent {
				t.Fatalf("failed %s events = %+v", test.name, client.events)
			}
			if !strings.Contains(model.status, test.name+" failed; request kept in Inbox") {
				t.Fatalf("failed %s status = %q", test.name, model.status)
			}
		})
	}
}

func TestTerminalEventCommitClearsStaleBusyStatusWithQueuedInbox(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	first := testAssignment()
	model := updateModel(t, New(client), networkMessage{Assignment: &first})

	var accept tea.Cmd
	model, accept = updateModelWithCommand(t, model, press("a", 'a'))
	model = finishCommand(t, model, accept)
	if model.active == nil {
		t.Fatal("first request was not accepted")
	}
	model.setReplyValue("auxiliary complete")
	updated, final := model.finishConversation()
	model = updated.(Model)
	if final == nil || model.pending.kind != pendingReply || model.active != nil {
		t.Fatalf("final response was not staged: %+v", model)
	}

	second := testAssignment()
	second.TaskID = "task-next"
	second.IdempotencyKey = "request-next"
	model = updateModel(t, model, networkMessage{Assignment: &second})
	if len(model.assignments) != 1 {
		t.Fatalf("next request was not queued while final committed: %+v", model)
	}
	model, blocked := updateModelWithCommand(t, model, press("a", 'a'))
	if blocked != nil || !strings.Contains(model.status, "still being committed") {
		t.Fatalf("accept was not safely blocked during final commit: command=%v model=%+v", blocked, model)
	}

	model = finishCommand(t, model, final)
	if model.responseInFlight() || !strings.Contains(model.status, "1 request(s) waiting in Inbox") {
		t.Fatalf("final acknowledgement left a stale busy state: %+v", model)
	}
	model, accept = updateModelWithCommand(t, model, press("a", 'a'))
	if accept == nil || model.pending.kind != pendingAccept {
		t.Fatalf("queued request remained unresponsive after final commit: %+v", model)
	}
}

func TestRemoteHandoffOnlyAutoAcceptsExactStableIdentity(t *testing.T) {
	t.Run("matching identity", func(t *testing.T) {
		client := newFakeClient()
		assignment := remoteStreamAssignment()
		model := handoffAssignment(t, client, assignment)
		continuation := nextTurn(assignment, "request-remote-next")

		model, _ = updateModelWithCommand(t, model, networkMessage{Assignment: &continuation})
		if model.pending.kind != pendingAccept || !model.pending.automatic || model.active != nil ||
			len(model.assignments) != 0 || model.pending.assignment.SessionKey() != continuation.SessionKey() {
			t.Fatalf("matching Remote handoff did not begin automatic accept: %+v", model)
		}
	})

	wrongIdentities := []struct {
		name   string
		mutate func(*completion.Assignment)
	}{
		{name: "caller", mutate: func(next *completion.Assignment) { next.CallerID = "other-caller" }},
		{name: "workspace", mutate: func(next *completion.Assignment) { next.WorkspaceKey = "other-workspace" }},
		{name: "task", mutate: func(next *completion.Assignment) { next.TaskID = "other-task" }},
		{name: "tier", mutate: func(next *completion.Assignment) { next.CapabilityTier = completion.TierWorkspace }},
	}
	for _, test := range wrongIdentities {
		t.Run("wrong "+test.name, func(t *testing.T) {
			client := newFakeClient()
			assignment := remoteStreamAssignment()
			model := handoffAssignment(t, client, assignment)
			continuation := nextTurn(assignment, "request-wrong-"+test.name)
			test.mutate(&continuation)

			model = updateModel(t, model, networkMessage{Assignment: &continuation})
			if model.pending.kind != pendingNone || model.active != nil || len(model.assignments) != 1 {
				t.Fatalf("wrong %s identity was automatically accepted: %+v", test.name, model)
			}
			if model.assignments[0].SessionKey() != continuation.SessionKey() || !model.continueHandoff {
				t.Fatalf("wrong %s identity corrupted Inbox or handoff wait: %+v", test.name, model)
			}
		})
	}
}

func TestAutomaticContinuationPreservesUnsyncedTaskDraft(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	assignment.Request.Tools = []canonical.Tool{taskTool("todowrite", openCodeTodoWriteSchema)}
	model := handoffAssignment(t, client, assignment)
	model.agentTasks = []agentTask{{
		Content: "local unsynced plan", Status: taskInProgress, Priority: "high",
	}}
	model.taskSelected = 0
	model.taskDirty = true
	model.taskEditing = true
	model.taskEditIndex = 0
	model.taskInput = "local edit in progress"

	continuation := nextTurn(assignment, "request-draft-continuation")
	model, accepted := updateModelWithCommand(t, model, networkMessage{Assignment: &continuation})
	if accepted == nil || model.pending.kind != pendingAccept || !model.pending.automatic {
		t.Fatalf("matching continuation was not staged: %+v", model)
	}
	// The network update also schedules the next subscription, so acknowledge
	// the durable accept directly instead of executing an unrelated Batch arm.
	model = updateModel(t, model, eventSent{eventID: model.pending.eventID})
	if model.active == nil || model.active.SessionKey() != continuation.SessionKey() ||
		len(model.agentTasks) != 1 || model.agentTasks[0].Content != "local unsynced plan" ||
		!model.taskDirty || !model.taskEditing || model.taskEditIndex != 0 ||
		model.taskInput != "local edit in progress" {
		t.Fatalf("automatic continuation discarded task draft: %+v", model)
	}
	if !strings.Contains(model.status, "Tasks draft retained") {
		t.Fatalf("retained draft is not visible: %q", model.status)
	}
}

func TestGatewayEventRejectionClosesOnlyItsSession(t *testing.T) {
	t.Run("matching active response returns to Inbox focus", func(t *testing.T) {
		client := newFakeClient()
		assignment := testAssignment()
		model := receiveAndAccept(t, New(client), assignment)
		model.composing = true
		model.focus = focusTasks
		model.input = "stale tool payload"
		model.toolCallIDs = []string{"tool-stale"}

		model = updateModel(t, model, networkMessage{EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: "late-progress", Message: "completion session is already closed",
		}})
		if model.active != nil || model.focus != focusTasks || !model.composing ||
			model.input != "stale tool payload" ||
			!reflect.DeepEqual(model.toolCallIDs, []string{"tool-stale"}) || model.detailMode {
			t.Fatalf("matching rejection did not close the session and preserve its draft: %+v", model)
		}
	})

	t.Run("matching task update becomes an unsynchronized draft", func(t *testing.T) {
		client := newFakeClient()
		assignment := testAssignment()
		assignment.Request.Tools = []canonical.Tool{taskTool("todowrite", openCodeTodoWriteSchema)}
		model := receiveAndAccept(t, New(client), assignment)
		model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab, Mod: tea.ModShift})
		model = enterTaskDraft(t, model, "retain rejected task update")

		model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
		if send == nil || model.pending.kind != pendingTasks {
			t.Fatalf("task update was not staged: %+v", model)
		}
		eventID := model.pending.eventID
		model = finishCommand(t, model, send)
		if !model.taskSyncWait || model.continueOrigin != assignment.SessionKey() {
			t.Fatalf("task update did not enter continuation wait: %+v", model)
		}

		model, subscription := updateModelWithCommand(t, model, networkMessage{EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: eventID, Message: "completion session is already closed",
		}, RejectedEvent: &client.events[len(client.events)-1]})
		if subscription == nil {
			t.Fatal("event rejection stopped the network subscription")
		}
		if model.active != nil || model.taskSyncWait || !model.taskDirty ||
			model.continueOrigin != "" || len(model.agentTasks) != 1 ||
			model.agentTasks[0].Content != "retain rejected task update" {
			t.Fatalf("event rejection did not safely close the task session: %+v", model)
		}
		if model.connection != connectionConnected ||
			!strings.Contains(model.status, "response not delivered") {
			t.Fatalf("event rejection was presented as a connection failure: %+v", model)
		}
	})

	t.Run("unrelated active session is preserved", func(t *testing.T) {
		client := newFakeClient()
		assignment := testAssignment()
		model := receiveAndAccept(t, New(client), assignment)
		model.taskSyncWait = true
		model.continueOrigin = assignment.SessionKey()

		model = updateModel(t, model, networkMessage{EventRejected: &workerproto.EventRejected{
			CallerID: "other-caller", IdempotencyKey: "other-request",
			EventID: "event-old", Message: "completion session is already closed",
		}})
		if model.active == nil || model.active.SessionKey() != assignment.SessionKey() ||
			!model.taskSyncWait || model.taskDirty || model.continueOrigin != assignment.SessionKey() {
			t.Fatalf("unrelated rejection mutated the active session: %+v", model)
		}
	})
}

func TestRejectedCommittedReplyRetractsOnlyItsOptimisticTurnAndSurvivesContinuation(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	model := receiveAndAccept(t, New(client), assignment)
	beforeMessages := len(model.lastContext.Request.Messages)
	model.setReplyValue("late human reply")

	updated, send := model.sendReply(completion.EventProgress, false)
	model = updated.(Model)
	model = finishCommand(t, model, send)
	rejectedEvent := client.events[len(client.events)-1]
	if model.pending.kind != pendingNone || len(model.lastContext.Request.Messages) != beforeMessages+1 {
		t.Fatalf("locally committed reply was not optimistic: %+v", model)
	}

	model = updateModel(t, model, rejectedMessage(assignment, rejectedEvent))
	_, hasDraft := model.rejectedDraftForAssignment(assignment)
	if model.active != nil || model.replyInput != "late human reply" || model.focus != focusReply ||
		!hasDraft || len(model.lastContext.Request.Messages) != beforeMessages {
		t.Fatalf("rejected reply was not retracted and restored: %+v", model)
	}

	continuation := nextTurn(assignment, "reply-retry-continuation")
	model = model.activateAssignment(continuation)
	_, hasDraft = model.rejectedDraftForAssignment(continuation)
	if model.active == nil || model.active.SessionKey() != continuation.SessionKey() ||
		model.replyInput != "late human reply" || model.focus != focusReply || hasDraft {
		t.Fatalf("same-scope continuation discarded rejected reply draft: %+v", model)
	}
}

func TestDurableRejectedInboxColdStartRestoresThenConfirms(t *testing.T) {
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	rejectedEvent := completion.Event{
		ID: "event-cold-rejection", Type: completion.EventProgress, Text: "recover after restart\n\n",
	}
	message := networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: rejectedEvent.ID,
			Message: "completion session is already closed",
		},
		RejectedEvent:      &rejectedEvent,
		RejectedAssignment: &assignment,
	}

	model, command := updateModelWithCommand(t, New(client), message)
	draft, restored := model.rejectedDraftForAssignment(assignment)
	if !restored || model.replyInput != "recover after restart" ||
		draft.reply != "recover after restart" || model.focus != focusReply {
		t.Fatalf("cold-start rejected inbox did not restore its draft: %+v", model)
	}
	if command == nil {
		t.Fatal("cold-start rejection did not schedule durable confirmation")
	}

	// The rejection handler also renews the network subscription. Feed that
	// command one advisory message so the test can execute every Batch child
	// and prove confirmation was actually scheduled after Model.Update applied
	// the draft.
	client.messages <- workerclient.Message{ConnectionRestored: true}
	root := command()
	batch, ok := root.(tea.BatchMsg)
	if !ok {
		t.Fatalf("rejection command = %T, want tea.BatchMsg", root)
	}
	var confirmation *rejectedEventConfirmed
	for _, child := range batch {
		if child == nil {
			continue
		}
		if result, ok := child().(rejectedEventConfirmed); ok {
			copy := result
			confirmation = &copy
		}
	}
	if confirmation == nil || confirmation.eventID != rejectedEvent.ID || confirmation.err != nil ||
		!reflect.DeepEqual(client.confirmed, []string{rejectedEvent.ID}) {
		t.Fatalf("durable rejection confirmation = %+v, calls = %v", confirmation, client.confirmed)
	}
}

func TestDurableRejectedInboxColdStartRestoresToolDraftKinds(t *testing.T) {
	tests := []struct {
		name       string
		assignment completion.Assignment
		event      completion.Event
		assert     func(*testing.T, Model, rejectedDraftState)
	}{
		{
			name: "command",
			assignment: func() completion.Assignment {
				assignment := remoteStreamAssignment()
				assignment.ExecAllowed = true
				assignment.Request.Tools = []canonical.Tool{{
					Name: "bash", InputSchema: []byte(`{
						"type":"object",
						"properties":{"command":{"type":"string"}},
						"required":["command"]
					}`),
				}}
				return assignment
			}(),
			event: completion.Event{
				ID: "event-cold-command", Type: completion.EventToolCalls,
				ToolCalls: []completion.ToolCall{{
					ID: "tool-cold-command", Name: "bash",
					Input: map[string]any{"command": "go test ./..."},
				}},
			},
			assert: func(t *testing.T, model Model, draft rejectedDraftState) {
				if !draft.hasCommand || draft.command != "go test ./..." ||
					model.commandInput != "go test ./..." || model.focus != focusCommand {
					t.Fatalf("command draft = %+v, model = %+v", draft, model)
				}
			},
		},
		{
			name: "tasks",
			assignment: func() completion.Assignment {
				assignment := remoteStreamAssignment()
				assignment.Request.Tools = []canonical.Tool{
					taskTool("todowrite", openCodeTodoWriteSchema),
				}
				return assignment
			}(),
			event: completion.Event{
				ID: "event-cold-tasks", Type: completion.EventToolCalls,
				ToolCalls: []completion.ToolCall{{
					ID: "tool-cold-tasks", Name: "todowrite",
					Input: map[string]any{"todos": []any{map[string]any{
						"content": "retain the plan", "status": "pending", "priority": "high",
					}}},
				}},
			},
			assert: func(t *testing.T, model Model, draft rejectedDraftState) {
				if !draft.hasTasks || len(draft.tasks) != 1 ||
					draft.tasks[0].Content != "retain the plan" || len(model.agentTasks) != 1 ||
					model.agentTasks[0].Content != "retain the plan" || !model.taskDirty ||
					model.focus != focusTasks {
					t.Fatalf("tasks draft = %+v, model = %+v", draft, model)
				}
			},
		},
		{
			name:       "advanced tools",
			assignment: remoteStreamAssignment(),
			event: completion.Event{
				ID: "event-cold-tools", Type: completion.EventToolCalls,
				ToolCalls: []completion.ToolCall{{
					ID: "tool-cold-custom", Name: "custom_tool",
					Input: map[string]any{"path": "notes.txt"},
				}},
			},
			assert: func(t *testing.T, model Model, draft rejectedDraftState) {
				if !draft.hasTools || draft.toolInput != `custom_tool {"path":"notes.txt"}` ||
					len(draft.toolCallIDs) != 1 || draft.toolCallIDs[0] == "tool-cold-custom" ||
					!strings.HasPrefix(draft.toolCallIDs[0], "tool_") ||
					!model.composing || model.input != draft.toolInput || model.focus != focusTasks {
					t.Fatalf("advanced draft = %+v, model = %+v", draft, model)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			message := networkMessage{
				EventRejected: &workerproto.EventRejected{
					CallerID:       test.assignment.CallerID,
					IdempotencyKey: test.assignment.IdempotencyKey,
					EventID:        test.event.ID,
				},
				RejectedEvent:      &test.event,
				RejectedAssignment: &test.assignment,
			}
			model := updateModel(t, New(client), message)
			draft, restored := model.rejectedDraftForAssignment(test.assignment)
			if !restored {
				t.Fatalf("cold-start %s draft was not retained: %+v", test.name, model)
			}
			test.assert(t, model, draft)
		})
	}
}

func TestDuplicateDurableRejectionDoesNotMergeDraftTwice(t *testing.T) {
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	rejectedEvent := completion.Event{
		ID: "event-duplicate-rejection", Type: completion.EventProgress, Text: "one copy\n\n",
	}
	message := networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: rejectedEvent.ID,
		},
		RejectedEvent:      &rejectedEvent,
		RejectedAssignment: &assignment,
	}

	model := updateModel(t, New(client), message)
	model = updateModel(t, model, message)
	draft, restored := model.rejectedDraftForAssignment(assignment)
	if !restored || model.replyInput != "one copy" || draft.reply != "one copy" ||
		len(model.handledRejections) != 1 {
		t.Fatalf("duplicate rejection was applied more than once: %+v", model)
	}
}

func TestPendingRejectedSendWaitsForDurableInboxWithoutDoubleRestore(t *testing.T) {
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	model := receiveAndAccept(t, New(client), assignment)
	client.sendErr = workerclient.ErrEventRejectionPending
	model.setReplyValue("one durable copy")
	updated, send := model.sendReply(completion.EventProgress, false)
	model = finishCommand(t, updated.(Model), send)
	if model.pending.kind != pendingReply || model.pending.reply != "one durable copy" ||
		model.replyInput != "" || !strings.Contains(model.status, "durable inbox") {
		t.Fatalf("pending rejection was restored before its durable message: %+v", model)
	}
	rejectedEvent := client.events[len(client.events)-1]
	model = updateModel(t, model, networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: rejectedEvent.ID,
		},
		RejectedEvent:      &rejectedEvent,
		RejectedAssignment: &assignment,
	})
	draft, restored := model.rejectedDraftForAssignment(assignment)
	if !restored || draft.reply != "one durable copy" || model.replyInput != "one durable copy" ||
		model.pending.kind != pendingNone {
		t.Fatalf("durable rejection restored zero or duplicate copies: %+v", model)
	}
}

func TestRejectedInboxConfirmationFailureKeepsVisibleDraft(t *testing.T) {
	client := newFakeClient()
	client.confirmationErrs = []error{errors.New("sqlite busy"), nil}
	assignment := remoteStreamAssignment()
	rejectedEvent := completion.Event{
		ID: "event-confirm-failure", Type: completion.EventProgress, Text: "safe draft\n\n",
	}
	message := networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: rejectedEvent.ID,
		},
		RejectedEvent:      &rejectedEvent,
		RejectedAssignment: &assignment,
	}

	model := updateModel(t, New(client), message)
	result := confirmRejectedEvent(client, rejectedEvent.ID)()
	model, retry := updateModelWithCommand(t, model, result)
	draft, restored := model.rejectedDraftForAssignment(assignment)
	if !restored || draft.reply != "safe draft" || model.replyInput != "safe draft" ||
		!strings.Contains(model.status, "safe on disk") || retry == nil {
		t.Fatalf("confirmation failure discarded or hid the restored draft: %+v", model)
	}
	retryResult := retry()
	confirmed, ok := retryResult.(rejectedEventConfirmed)
	if !ok || confirmed.attempt != 1 || confirmed.err != nil {
		t.Fatalf("confirmation retry = %#v", retryResult)
	}
	model = updateModel(t, model, confirmed)
	_, restored = model.rejectedDraftForAssignment(assignment)
	if !restored || model.replyInput != "safe draft" ||
		!strings.Contains(model.status, "confirmation recovered") ||
		!reflect.DeepEqual(client.confirmed, []string{rejectedEvent.ID, rejectedEvent.ID}) {
		t.Fatalf("confirmation retry did not recover safely: model=%+v calls=%v", model, client.confirmed)
	}
}

func TestRejectedIntentCleanupRetriesBeforeInboxConfirmation(t *testing.T) {
	client := newFakeClient()
	eventID := "event-intent-cleanup-order"
	cleanupAttempts := 0
	cleanup := func() tea.Msg {
		cleanupAttempts++
		if cleanupAttempts == 1 {
			return mirrorIntentsDiscarded{reason: "fault injection", err: errors.New("mirror sqlite busy")}
		}
		return mirrorIntentsDiscarded{reason: "fault injection"}
	}

	model := New(client)
	first := finalizeRejectedEvent(client, eventID, cleanup)()
	if len(client.confirmed) != 0 {
		t.Fatalf("rejected inbox confirmed before intent cleanup: %v", client.confirmed)
	}
	model, retry := updateModelWithCommand(t, model, first)
	if retry == nil || !strings.Contains(model.status, "intent cleanup failed") {
		t.Fatalf("cleanup failure did not retain a retry: %+v", model)
	}
	if len(client.confirmed) != 0 {
		t.Fatalf("cleanup failure confirmed rejected inbox: %v", client.confirmed)
	}

	second := retry()
	discarded, ok := second.(mirrorIntentsDiscarded)
	if !ok || discarded.attempt != 1 || discarded.err != nil || cleanupAttempts != 2 {
		t.Fatalf("cleanup retry = %#v, attempts = %d", second, cleanupAttempts)
	}
	model, confirm := updateModelWithCommand(t, model, discarded)
	if confirm == nil || len(client.confirmed) != 0 ||
		!strings.Contains(model.status, "cleanup recovered") {
		t.Fatalf("successful cleanup did not stage confirmation: model=%+v calls=%v", model, client.confirmed)
	}
	confirmation, ok := confirm().(rejectedEventConfirmed)
	if !ok || confirmation.err != nil || confirmation.eventID != eventID ||
		!reflect.DeepEqual(client.confirmed, []string{eventID}) {
		t.Fatalf("post-cleanup confirmation = %#v, calls = %v", confirmation, client.confirmed)
	}
}

func TestRejectedCommittedReplyPreservesNewerUnsentTail(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	model := receiveAndAccept(t, New(client), assignment)

	model.setReplyValue("already sent segment")
	updated, send := model.sendReply(completion.EventProgress, false)
	model = finishCommand(t, updated.(Model), send)
	rejectedEvent := client.events[len(client.events)-1]
	// The local outbox commit has completed, so the expert is allowed to keep
	// typing while the older event is still in flight to the gateway.
	model.setReplyValue("new unsent draft")

	model = updateModel(t, model, rejectedMessage(assignment, rejectedEvent))
	draft, hasDraft := model.rejectedDraftForAssignment(assignment)
	want := "already sent segment\n\nnew unsent draft"
	if !hasDraft || model.replyInput != want || draft.reply != want ||
		draft.replyRejected != "already sent segment" || draft.replyTail != "new unsent draft" {
		t.Fatalf("late rejection overwrote the newer local draft: %+v", model)
	}
}

func TestMultipleCommittedRejectionsInsertBeforeNewerUnsentTail(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	model := receiveAndAccept(t, New(client), assignment)

	sendProgress := func(text string) completion.Event {
		model.setReplyValue(text)
		updated, send := model.sendReply(completion.EventProgress, false)
		model = finishCommand(t, updated.(Model), send)
		return client.events[len(client.events)-1]
	}
	first := sendProgress("first sent segment")
	second := sendProgress("second sent segment")
	model.setReplyValue("new unsent draft")

	model = updateModel(t, model, rejectedMessage(assignment, first))
	model = updateModel(t, model, rejectedMessage(assignment, second))
	draft, hasDraft := model.rejectedDraftForAssignment(assignment)
	want := "first sent segment\n\nsecond sent segment\n\nnew unsent draft"
	if !hasDraft || model.replyInput != want || draft.reply != want ||
		draft.replyRejected != "first sent segment\n\nsecond sent segment" ||
		draft.replyTail != "new unsent draft" {
		t.Fatalf("rejected segment ordering lost the unsent tail: %+v", model)
	}
}

func TestRejectedCommittedReplyPreservesTailWhileCommandIsPending(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	assignment.ExecAllowed = true
	assignment.Request.Tools = []canonical.Tool{{
		Name: "bash",
		InputSchema: []byte(`{
			"type":"object",
			"properties":{"command":{"type":"string"}},
			"required":["command"]
		}`),
	}}
	model := receiveAndAccept(t, New(client), assignment)

	model.setReplyValue("already sent segment")
	updated, progressSend := model.sendReply(completion.EventProgress, false)
	model = finishCommand(t, updated.(Model), progressSend)
	progressEvent := client.events[len(client.events)-1]
	model.setReplyValue("new unsent reply")
	model.focus = focusCommand
	model.setCommandValue("go test ./...")
	updated, commandSend := model.sendCommand()
	model = updated.(Model)
	if commandSend == nil || model.pending.kind != pendingCommand {
		t.Fatalf("command was not pending: %+v", model)
	}

	model = updateModel(t, model, rejectedMessage(assignment, progressEvent))
	want := "already sent segment\n\nnew unsent reply"
	if model.replyInput != want || model.commandInput != "" || model.pending.kind != pendingCommand {
		t.Fatalf("progress rejection lost the reply tail beside a pending command: %+v", model)
	}
}

func TestRejectedCommittedCommandRestoresCommandWithoutLeakingToAnotherScope(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	assignment.ExecAllowed = true
	assignment.Request.Tools = []canonical.Tool{{
		Name: "bash",
		InputSchema: []byte(`{
			"type":"object",
			"properties":{"command":{"type":"string"}},
			"required":["command"]
		}`),
	}}
	model := receiveAndAccept(t, New(client), assignment)
	beforeMessages := len(model.lastContext.Request.Messages)
	model.focus = focusCommand
	model.setCommandValue("printf retry-me")

	updated, send := model.sendCommand()
	model = updated.(Model)
	model = finishCommand(t, model, send)
	rejectedEvent := client.events[len(client.events)-1]
	model = updateModel(t, model, rejectedMessage(assignment, rejectedEvent))
	_, hasDraft := model.rejectedDraftForAssignment(assignment)
	if model.active != nil || model.commandInput != "printf retry-me" || model.focus != focusCommand ||
		!hasDraft || len(model.lastContext.Request.Messages) != beforeMessages {
		t.Fatalf("rejected command was not restored in the command pane: %+v", model)
	}

	unrelated := assignment
	unrelated.CallerID = "other-caller"
	unrelated.WorkspaceKey = "other-workspace"
	unrelated.TaskID = "other-task"
	unrelated.IdempotencyKey = "other-request"
	model = model.activateAssignment(unrelated)
	_, hasDraft = model.rejectedDraftForAssignment(assignment)
	if model.commandInput != "" || model.composing || !hasDraft {
		t.Fatalf("rejected command leaked into unrelated assignment: %+v", model)
	}

	continuation := nextTurn(assignment, "command-retry-continuation")
	model = model.activateAssignment(continuation)
	_, hasDraft = model.rejectedDraftForAssignment(continuation)
	if model.commandInput != "printf retry-me" || model.focus != focusCommand || hasDraft {
		t.Fatalf("original scope did not recover its command draft: %+v", model)
	}
}

func TestRejectedAdvancedBashCallWithExtraArgumentsDoesNotLoseThemInCommandPane(t *testing.T) {
	t.Parallel()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{{
		Name: "bash",
		InputSchema: []byte(`{
			"type":"object",
			"properties":{"command":{"type":"string"},"timeout":{"type":"integer"}},
			"required":["command"]
		}`),
	}}
	model := receiveAndAccept(t, New(newFakeClient()), assignment)
	beforeMessages := len(model.lastContext.Request.Messages)
	event := completion.Event{
		ID: "event-advanced-bash", Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{
			ID: "tool-advanced-bash", Name: "bash",
			Input: map[string]any{"command": "sleep 1", "timeout": float64(5)},
		}},
	}
	model.appendLocalToolCall(assignment, event.ToolCalls[0])
	model.active = nil

	model = updateModel(t, model, rejectedMessage(assignment, event))
	if model.commandInput != "" || !model.composing ||
		model.input != `bash {"command":"sleep 1","timeout":5}` ||
		len(model.toolCallIDs) != 1 || model.toolCallIDs[0] == "tool-advanced-bash" ||
		!strings.HasPrefix(model.toolCallIDs[0], "tool_") ||
		len(model.lastContext.Request.Messages) != beforeMessages {
		t.Fatalf("advanced bash rejection lost its non-command arguments: %+v", model)
	}
}

func TestRejectedCommittedAdvancedToolsRestoreExactComposer(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{
		{Name: "search", InputSchema: []byte(`{"type":"object"}`)},
		{Name: "lookup", InputSchema: []byte(`{"type":"object"}`)},
	}
	model := receiveAndAccept(t, New(client), assignment)
	beforeMessages := len(model.lastContext.Request.Messages)
	model.composing = true
	model.input = "search {\"query\":\"status\"}\nlookup {\"path\":\"README.md\"}"
	model.toolCallIDs = []string{"tool-search-stable", "tool-lookup-stable"}

	updated, send := model.sendDeclaredToolCalls()
	model = updated.(Model)
	model = finishCommand(t, model, send)
	rejectedEvent := client.events[len(client.events)-1]
	model = updateModel(t, model, rejectedMessage(assignment, rejectedEvent))
	if model.active != nil || !model.composing ||
		model.input != "search {\"query\":\"status\"}\nlookup {\"path\":\"README.md\"}" ||
		len(model.toolCallIDs) != 2 ||
		model.toolCallIDs[0] == "tool-search-stable" || model.toolCallIDs[1] == "tool-lookup-stable" ||
		!strings.HasPrefix(model.toolCallIDs[0], "tool_") ||
		!strings.HasPrefix(model.toolCallIDs[1], "tool_") ||
		model.toolCallIDs[0] == model.toolCallIDs[1] ||
		len(model.lastContext.Request.Messages) != beforeMessages {
		t.Fatalf("rejected advanced tools were not restored with fresh call IDs: %+v", model)
	}
}

func TestRejectedMirrorDeliveryRetractsTranscriptAndRequiresFreshReview(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := workspaceAssignment(testAssignment().Request)
	model := receiveAndAccept(t, New(client), assignment)
	beforeMessages := len(model.lastContext.Request.Messages)
	event := completion.Event{
		ID: "event-mirror-rejected", Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{
			ID: "tool-mirror-rejected", Name: "human_write_file",
			Input: map[string]any{
				"path": "/workspace/retry.txt", "content": "retry", "encoding": "utf-8",
			},
		}},
	}
	model.appendLocalToolCall(assignment, event.ToolCalls[0])
	model.expectContinuation(assignment, event.ToolCalls)
	model.active = nil

	model = updateModel(t, model, rejectedMessage(assignment, event))
	_, hasDraft := model.rejectedDraftForAssignment(assignment)
	if len(model.lastContext.Request.Messages) != beforeMessages || hasDraft ||
		model.composing || model.input != "" || !strings.Contains(model.status, "re-review the mirror") {
		t.Fatalf("rejected mirror delivery bypassed fresh review: %+v", model)
	}
}

func TestRejectionBeforeEventSentUsesPendingSnapshotAndLateAckCannotClearDraft(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	model := receiveAndAccept(t, New(client), assignment)
	beforeMessages := len(model.lastContext.Request.Messages)
	model.setReplyValue("first segment")

	updated, send := model.sendReply(completion.EventProgress, false)
	model = updated.(Model)
	localAck := commandResult(t, send)
	rejectedEvent := client.events[len(client.events)-1]
	model = updateModel(t, model, tea.KeyPressMsg{Text: "typed while committing", Code: 0})
	if model.pending.kind != pendingReply {
		t.Fatalf("reply was not still pending before the local acknowledgement: %+v", model)
	}

	model = updateModel(t, model, rejectedMessage(assignment, rejectedEvent))
	if model.active != nil || model.pending.kind != pendingNone ||
		model.replyInput != "first segment\n\ntyped while committing" ||
		len(model.lastContext.Request.Messages) != beforeMessages {
		t.Fatalf("pending rejection did not restore its exact pre-send context and draft: %+v", model)
	}
	model = updateModel(t, model, localAck)
	if model.active != nil || model.replyInput != "first segment\n\ntyped while committing" ||
		model.pending.kind != pendingNone {
		t.Fatalf("late eventSent acknowledgement cleared a rejected draft: %+v", model)
	}
}

func TestMultipleRejectedProgressEventsMergeInOutboxOrder(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	model := receiveAndAccept(t, New(client), assignment)
	beforeMessages := len(model.lastContext.Request.Messages)

	model.setReplyValue("first late segment")
	updated, firstSend := model.sendReply(completion.EventProgress, false)
	model = finishCommand(t, updated.(Model), firstSend)
	firstEvent := client.events[len(client.events)-1]

	model.setReplyValue("second late segment")
	updated, secondSend := model.sendReply(completion.EventProgress, false)
	model = updated.(Model)
	secondAck := commandResult(t, secondSend)
	secondEvent := client.events[len(client.events)-1]
	model = updateModel(t, model, tea.KeyPressMsg{Text: "draft after both", Code: 0})

	model = updateModel(t, model, rejectedMessage(assignment, firstEvent))
	draft, hasDraft := model.rejectedDraftForAssignment(assignment)
	if model.pending.eventID != secondEvent.ID || model.replyInput != "draft after both" ||
		!hasDraft || draft.reply != "first late segment" ||
		len(model.lastContext.Request.Messages) != beforeMessages+1 {
		t.Fatalf("first rejection corrupted the later pending segment: %+v", model)
	}

	model = updateModel(t, model, rejectedMessage(assignment, secondEvent))
	draft, hasDraft = model.rejectedDraftForAssignment(assignment)
	want := "first late segment\n\nsecond late segment\n\ndraft after both"
	if model.pending.kind != pendingNone || model.replyInput != want ||
		!hasDraft || draft.reply != want ||
		len(model.lastContext.Request.Messages) != beforeMessages {
		t.Fatalf("consecutive rejected progress was not merged in order: %+v", model)
	}
	model = updateModel(t, model, secondAck)
	if model.replyInput != want || model.pending.kind != pendingNone {
		t.Fatalf("late local acknowledgement cleared merged rejected progress: %+v", model)
	}
}

func TestRejectedProgressAndCommandDraftsCoexist(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := remoteStreamAssignment()
	assignment.ExecAllowed = true
	assignment.Request.Tools = []canonical.Tool{{
		Name: "bash",
		InputSchema: []byte(`{
			"type":"object",
			"properties":{"command":{"type":"string"}},
			"required":["command"]
		}`),
	}}
	model := receiveAndAccept(t, New(client), assignment)
	beforeMessages := len(model.lastContext.Request.Messages)

	model.setReplyValue("explanation before command")
	updated, progressSend := model.sendReply(completion.EventProgress, false)
	model = finishCommand(t, updated.(Model), progressSend)
	progressEvent := client.events[len(client.events)-1]

	model.setCommandValue("go test ./...")
	updated, commandSend := model.sendCommand()
	model = finishCommand(t, updated.(Model), commandSend)
	commandEvent := client.events[len(client.events)-1]

	model = updateModel(t, model, rejectedMessage(assignment, progressEvent))
	model = updateModel(t, model, rejectedMessage(assignment, commandEvent))
	draft, hasDraft := model.rejectedDraftForAssignment(assignment)
	if model.replyInput != "explanation before command" || model.commandInput != "go test ./..." ||
		!hasDraft || !draft.hasReply || !draft.hasCommand ||
		len(model.lastContext.Request.Messages) != beforeMessages {
		t.Fatalf("mixed rejected drafts overwrote one another: %+v", model)
	}

	continuation := nextTurn(assignment, "mixed-draft-continuation")
	model = model.activateAssignment(continuation)
	_, hasDraft = model.rejectedDraftForAssignment(continuation)
	if model.replyInput != "explanation before command" || model.commandInput != "go test ./..." ||
		model.focus != focusCommand || hasDraft {
		t.Fatalf("same-scope continuation did not restore mixed drafts: %+v", model)
	}
}

func TestRejectedDraftTrayKeepsIndependentTaskScopes(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	first := remoteStreamAssignment()
	model := receiveAndAccept(t, New(client), first)
	model.setReplyValue("first task draft")
	updated, send := model.sendReply(completion.EventProgress, false)
	model = finishCommand(t, updated.(Model), send)
	firstRejected := client.events[len(client.events)-1]
	model = updateModel(t, model, rejectedMessage(first, firstRejected))
	model = updateModel(t, model, rejectedEventConfirmed{eventID: firstRejected.ID})

	second := remoteStreamAssignment()
	second.CallerID = "second-caller"
	second.WorkspaceKey = "second-workspace"
	second.TaskID = "second-task"
	second.IdempotencyKey = "second-request"
	model = model.activateAssignment(second)
	model.setReplyValue("second task draft")
	updated, send = model.sendReply(completion.EventProgress, false)
	model = finishCommand(t, updated.(Model), send)
	secondRejected := client.events[len(client.events)-1]
	model = updateModel(t, model, rejectedMessage(second, secondRejected))
	model = updateModel(t, model, rejectedEventConfirmed{eventID: secondRejected.ID})

	firstDraft, hasFirst := model.rejectedDraftForAssignment(first)
	secondDraft, hasSecond := model.rejectedDraftForAssignment(second)
	if !hasFirst || !hasSecond || firstDraft.reply != "first task draft" ||
		secondDraft.reply != "second task draft" || len(model.rejectedDrafts) != 2 {
		t.Fatalf("independent rejected drafts overwrote one another: %+v", model.rejectedDrafts)
	}

	firstRetry := nextTurn(first, "first-task-retry")
	model = model.activateAssignment(firstRetry)
	_, hasFirst = model.rejectedDraftForAssignment(firstRetry)
	_, hasSecond = model.rejectedDraftForAssignment(second)
	if model.replyInput != "first task draft" || hasFirst || !hasSecond {
		t.Fatalf("first task did not consume only its own draft: %+v", model)
	}

	secondRetry := nextTurn(second, "second-task-retry")
	model = model.activateAssignment(secondRetry)
	_, hasSecond = model.rejectedDraftForAssignment(secondRetry)
	if model.replyInput != "second task draft" || hasSecond || len(model.rejectedDrafts) != 0 {
		t.Fatalf("second task draft was not independently recoverable: %+v", model)
	}
}

func TestRejectedDraftTrayReportsBoundedEviction(t *testing.T) {
	t.Parallel()
	model := New(newFakeClient())
	var first completion.Assignment
	for index := 0; index <= maxRejectedDraftScopes; index++ {
		assignment := remoteStreamAssignment()
		assignment.CallerID = fmt.Sprintf("caller-%02d", index)
		assignment.WorkspaceKey = fmt.Sprintf("workspace-%02d", index)
		assignment.TaskID = fmt.Sprintf("task-%02d", index)
		assignment.IdempotencyKey = fmt.Sprintf("request-%02d", index)
		if index == 0 {
			first = assignment
		}
		evicted := model.installRejectedDraft(rejectedDraftState{
			assignment: assignment, kind: pendingReply, hasReply: true,
			reply: "saved draft", replyRejected: "saved draft",
		})
		if evicted != (index == maxRejectedDraftScopes) {
			t.Fatalf("install %d eviction = %t", index, evicted)
		}
	}
	if len(model.rejectedDrafts) != maxRejectedDraftScopes {
		t.Fatalf("bounded tray size = %d", len(model.rejectedDrafts))
	}
	if _, retained := model.rejectedDraftForAssignment(first); retained {
		t.Fatal("oldest rejected draft was not evicted at the documented bound")
	}
}

func TestRejectedChatDraftNeverCrossesIntoAnotherRequest(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	assignment.CapabilityTier = completion.TierChat
	assignment.TaskID = "advisory-chat-task"
	model := receiveAndAccept(t, New(client), assignment)
	model.setReplyValue("expired chat draft")

	updated, send := model.sendReply(completion.EventProgress, false)
	model = finishCommand(t, updated.(Model), send)
	rejectedEvent := client.events[len(client.events)-1]
	model = updateModel(t, model, rejectedMessage(assignment, rejectedEvent))
	_, oldDraft := model.rejectedDraftForAssignment(assignment)
	if !oldDraft || model.replyInput != "expired chat draft" {
		t.Fatalf("rejected Chat draft was not retained locally: %+v", model)
	}

	next := nextTurn(assignment, "another-chat-request")
	// Chat task_id is not a correctness identity even when a caller repeats it.
	model = model.activateAssignment(next)
	_, oldDraft = model.rejectedDraftForAssignment(assignment)
	_, nextDraft := model.rejectedDraftForAssignment(next)
	if model.replyInput != "" || model.commandInput != "" || model.composing ||
		!oldDraft || nextDraft {
		t.Fatalf("rejected Chat draft leaked into another request: %+v", model)
	}
}

func TestRejectedOriginalEventCannotMutateUnrelatedActiveDraft(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	model := receiveAndAccept(t, New(client), assignment)
	model.setReplyValue("current caller draft")
	before := cloneAssignment(model.lastContext)
	rejectedEvent := completion.Event{ID: "event-other", Type: completion.EventProgress, Text: "other\n\n"}

	model = updateModel(t, model, networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: "other-caller", IdempotencyKey: "other-request", EventID: rejectedEvent.ID,
			Message: "other session expired",
		},
		RejectedEvent: &rejectedEvent,
	})
	if model.active == nil || model.active.SessionKey() != assignment.SessionKey() ||
		model.replyInput != "current caller draft" || len(model.rejectedDrafts) != 0 ||
		!reflect.DeepEqual(model.lastContext, before) {
		t.Fatalf("unrelated rejected event mutated current session: %+v", model)
	}
}

func rejectedMessage(assignment completion.Assignment, event completion.Event) networkMessage {
	return networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: event.ID,
			Message: "completion session is already closed",
		},
		RejectedEvent: &event,
	}
}

func TestContinuationOriginReplayIsNeverAcceptedAsTheNextTurn(t *testing.T) {
	t.Run("before local outbox acknowledgement", func(t *testing.T) {
		client := newFakeClient()
		assignment := remoteStreamAssignment()
		model := receiveAndAccept(t, New(client), assignment)
		model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: 'r', Mod: tea.ModCtrl})
		if send == nil || model.pending.kind != pendingReply || !model.continueHandoff {
			t.Fatalf("handoff was not staged: %+v", model)
		}
		pendingID := model.pending.eventID
		replayed := assignment
		replayed.LeaseOwner = "replacement-before-ack"

		model, _ = updateModelWithCommand(t, model, networkMessage{Assignment: &replayed})
		if model.pending.kind != pendingReply || model.pending.eventID != pendingID ||
			model.pending.assignment.LeaseOwner != "replacement-before-ack" || model.active != nil ||
			len(model.assignments) != 0 || !model.continueHandoff {
			t.Fatalf("origin replay displaced the pending handoff: %+v", model)
		}
		if len(client.events) != 1 || client.events[0].Type != completion.EventAccepted {
			t.Fatalf("origin replay emitted an event: %+v", client.events)
		}
	})

	t.Run("after local outbox acknowledgement", func(t *testing.T) {
		client := newFakeClient()
		assignment := remoteStreamAssignment()
		model := handoffAssignment(t, client, assignment)
		replayed := assignment
		replayed.LeaseOwner = "replacement-after-ack"

		model, _ = updateModelWithCommand(t, model, networkMessage{Assignment: &replayed})
		if model.pending.kind != pendingNone || model.active != nil || len(model.assignments) != 0 ||
			!model.continueHandoff || model.continueOrigin != assignment.SessionKey() ||
			model.lastContext == nil || model.lastContext.LeaseOwner != "replacement-after-ack" {
			t.Fatalf("origin replay became a continuation or Inbox item: %+v", model)
		}
		if len(client.events) != 2 || client.events[1].Type != completion.EventClarification {
			t.Fatalf("origin replay emitted an extra event: %+v", client.events)
		}
	})
}

func TestFinalizedSourceReplayNeverReturnsToInbox(t *testing.T) {
	for _, order := range []string{"before local outbox acknowledgement", "after local outbox acknowledgement"} {
		t.Run(order, func(t *testing.T) {
			client := newFakeClient()
			assignment := remoteStreamAssignment()
			model := receiveAndAccept(t, New(client), assignment)
			model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl})
			if send == nil || model.pending.kind != pendingReply || model.active != nil ||
				model.continueOrigin != "" || model.continueHandoff || len(model.continueIDs) != 0 {
				t.Fatalf("final was not staged without a continuation: %+v", model)
			}

			replayed := assignment
			replayed.LeaseOwner = "replacement-final"
			if order == "before local outbox acknowledgement" {
				model = updateModel(t, model, networkMessage{Assignment: &replayed})
				if model.pending.kind != pendingReply || model.pending.assignment.LeaseOwner != "replacement-final" {
					t.Fatalf("source replay displaced pending final: %+v", model)
				}
				model = finishCommand(t, model, send)
			} else {
				model = finishCommand(t, model, send)
				model = updateModel(t, model, networkMessage{Assignment: &replayed})
			}

			if model.pending.kind != pendingNone || model.active != nil || len(model.assignments) != 0 ||
				model.lastContext == nil || model.lastContext.LeaseOwner != "replacement-final" {
				t.Fatalf("finalized source replay became an Inbox request: %+v", model)
			}
			if len(client.events) != 2 || client.events[0].Type != completion.EventAccepted ||
				client.events[1].Type != completion.EventFinal {
				t.Fatalf("finalized source replay emitted an extra event: %+v", client.events)
			}
		})
	}
}

func TestChatHandoffQueuesNextRequestAndClearsFalseWait(t *testing.T) {
	client := newFakeClient()
	assignment := testAssignment()
	assignment.CapabilityTier = completion.TierChat
	model := handoffAssignment(t, client, assignment)
	continuation := nextTurn(assignment, "request-chat-next")
	continuation.TaskID = "next-chat-task"

	model = updateModel(t, model, networkMessage{Assignment: &continuation})
	if model.pending.kind != pendingNone || model.active != nil || len(model.assignments) != 1 {
		t.Fatalf("Chat continuation was not surfaced in Inbox: %+v", model)
	}
	if model.continueHandoff || model.continueCaller != "" || model.continueWorkspace != "" ||
		model.continueTaskID != "" || model.continueTier != "" || len(model.continueIDs) != 0 {
		t.Fatalf("Chat continuation left a false automatic-resume wait: %+v", model)
	}
}

func TestToolContinuationRequiresEveryExpectedResultID(t *testing.T) {
	assignment := remoteStreamAssignment()
	calls := []completion.ToolCall{
		{ID: "tool-first", Name: "first", Input: map[string]any{}},
		{ID: "tool-second", Name: "second", Input: map[string]any{}},
	}
	model := New(newFakeClient())
	model.expectContinuation(assignment, calls)

	partial := toolResultTurn(assignment, "request-partial", "tool-first")
	model = updateModel(t, model, networkMessage{Assignment: &partial})
	if model.pending.kind != pendingNone || len(model.assignments) != 1 {
		t.Fatalf("one of two tool results resumed the stream: %+v", model)
	}

	complete := toolResultTurn(assignment, "request-complete", "tool-first", "tool-second")
	model = updateModel(t, model, networkMessage{Assignment: &complete})
	if model.pending.kind != pendingAccept || !model.pending.automatic ||
		model.pending.assignment.SessionKey() != complete.SessionKey() {
		t.Fatalf("complete tool results did not begin automatic accept: %+v", model)
	}
	if len(model.assignments) != 1 || model.assignments[0].SessionKey() != partial.SessionKey() {
		t.Fatalf("automatic continuation corrupted the unrelated partial request: %+v", model.assignments)
	}
}

func TestInterleavedToolContinuationsRemainIndependent(t *testing.T) {
	first := remoteStreamAssignment()
	first.CallerID = "caller-first"
	first.WorkspaceKey = "workspace-first"
	first.TaskID = "task-first"
	first.IdempotencyKey = "request-first"
	second := remoteStreamAssignment()
	second.CallerID = "caller-second"
	second.WorkspaceKey = "workspace-second"
	second.TaskID = "task-second"
	second.IdempotencyKey = "request-second"

	model := New(newFakeClient())
	model.rememberContext(first)
	model.expectContinuation(first, []completion.ToolCall{{ID: "tool-first", Name: "read"}})
	model.rememberContext(second)
	model.expectContinuation(second, []completion.ToolCall{{ID: "tool-second", Name: "read"}})
	if len(model.parkedContinuations) != 1 || model.continueOrigin != second.SessionKey() {
		t.Fatalf("continuations were not parked independently: %+v", model)
	}

	firstResult := toolResultTurn(first, "request-first-result", "tool-first")
	if !model.matchesContinuation(firstResult) || model.continueOrigin != first.SessionKey() ||
		len(model.parkedContinuations) != 1 ||
		model.parkedContinuations[0].origin != second.SessionKey() ||
		model.lastContext == nil || model.lastContext.CallerID != first.CallerID {
		t.Fatalf("first result did not promote its own continuation: %+v", model)
	}
	model.clearContinuation()
	secondResult := toolResultTurn(second, "request-second-result", "tool-second")
	if !model.matchesContinuation(secondResult) || model.continueOrigin != second.SessionKey() ||
		len(model.parkedContinuations) != 0 ||
		model.lastContext == nil || model.lastContext.CallerID != second.CallerID {
		t.Fatalf("second result lost its parked continuation: %+v", model)
	}
}

func TestContinuationWaitsWhileInboxDecisionIsCommitting(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	stream := remoteStreamAssignment()
	model := New(client)
	model.expectHandoff(stream)

	inbox := testAssignment()
	inbox.CallerID = "unrelated-caller"
	model.assignments = []completion.Assignment{inbox}
	model.pending = pendingSend{
		kind: pendingReject, eventID: "reject-in-flight", assignment: inbox,
	}
	continuation := nextTurn(stream, "request-after-handoff")

	model, _ = updateModelWithCommand(t, model, networkMessage{Assignment: &continuation})
	if model.pending.kind != pendingReject || model.pending.eventID != "reject-in-flight" ||
		model.pending.assignment.SessionKey() != inbox.SessionKey() {
		t.Fatalf("continuation overwrote an in-flight Inbox decision: %+v", model.pending)
	}
	if len(model.assignments) != 2 || model.assignments[1].SessionKey() != continuation.SessionKey() {
		t.Fatalf("continuation was not retained in Inbox: %+v", model.assignments)
	}
	if len(client.events) != 0 {
		t.Fatalf("continuation sent an event before the Inbox decision completed: %+v", client.events)
	}
}

func TestCommandAndTaskSendFailuresRestorePreSendTranscript(t *testing.T) {
	t.Run("command", func(t *testing.T) {
		client := newFakeClient()
		assignment := testAssignment()
		assignment.Request.Tools = []canonical.Tool{{
			Name:        "bash",
			InputSchema: []byte(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`),
		}}
		model := receiveAndAccept(t, New(client), assignment)
		model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab})
		model = updateModel(t, model, tea.KeyPressMsg{Text: "pwd", Code: 0})
		before := cloneAssignment(model.lastContext)
		client.sendErr = definitelyNotStored("command outbox failed")

		model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
		if send == nil || model.pending.kind != pendingCommand || model.active != nil {
			t.Fatalf("command was not staged: %+v", model)
		}
		model = finishCommand(t, model, send)

		if model.active == nil || model.focus != focusCommand || model.commandInput != "pwd" ||
			model.pending.kind != pendingNone {
			t.Fatalf("failed command draft was not restored: %+v", model)
		}
		assertTranscriptEqual(t, model.lastContext, before, "command")
	})

	t.Run("tasks", func(t *testing.T) {
		client := newFakeClient()
		assignment := testAssignment()
		assignment.Request.Tools = []canonical.Tool{taskTool("todowrite", openCodeTodoWriteSchema)}
		model := receiveAndAccept(t, New(client), assignment)
		model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab, Mod: tea.ModShift})
		model = enterTaskDraft(t, model, "keep this task")
		before := cloneAssignment(model.lastContext)
		client.sendErr = definitelyNotStored("task outbox failed")

		model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
		if send == nil || model.pending.kind != pendingTasks || model.active != nil {
			t.Fatalf("task update was not staged: %+v", model)
		}
		model = finishCommand(t, model, send)

		if model.active == nil || model.focus != focusTasks || !model.taskDirty || model.taskSyncWait ||
			len(model.agentTasks) != 1 || model.agentTasks[0].Content != "keep this task" {
			t.Fatalf("failed task draft was not restored: %+v", model)
		}
		assertTranscriptEqual(t, model.lastContext, before, "task")
	})
}

func TestFileDeliveryAckAndContinuationMayArriveInEitherOrder(t *testing.T) {
	for _, order := range []string{"delivery ack first", "continuation first"} {
		t.Run(order, func(t *testing.T) {
			client := newFakeClient()
			model, assignment, call, deliveryAck := stageSyntheticDelivery(t, client)
			continuation := toolResultTurn(assignment, "request-file-next", call.ID)

			if order == "delivery ack first" {
				model = updateModel(t, model, deliveryAck)
				if model.delivery.stage != deliveryNone || len(model.continueIDs) != 1 {
					t.Fatalf("delivery acknowledgement lost continuation expectation: %+v", model)
				}
				model = updateModel(t, model, networkMessage{Assignment: &continuation})
			} else {
				model = updateModel(t, model, networkMessage{Assignment: &continuation})
				if model.pending.kind != pendingAccept || model.delivery.stage != deliveryNone {
					t.Fatalf("early continuation did not supersede delivery wait: %+v", model)
				}
				model = updateModel(t, model, deliveryAck)
			}

			if model.pending.kind != pendingAccept || !model.pending.automatic ||
				model.pending.assignment.SessionKey() != continuation.SessionKey() {
				t.Fatalf("%s did not begin automatic accept: %+v", order, model)
			}
		})
	}
}

func TestSameSessionReplayDoesNotBreakFileDeliverySending(t *testing.T) {
	client := newFakeClient()
	model, assignment, call, deliveryAck := stageSyntheticDelivery(t, client)
	replayed := assignment
	replayed.LeaseOwner = "replacement-lease"

	model = updateModel(t, model, networkMessage{Assignment: &replayed})
	if model.delivery.stage != deliverySending || model.delivery.assignment.LeaseOwner != "replacement-lease" ||
		model.active != nil || model.pending.kind != pendingDelivery || len(model.continueIDs) != 1 {
		t.Fatalf("same-session replay broke delivery sending: %+v", model)
	}

	model = updateModel(t, model, deliveryAck)
	if model.delivery.stage != deliveryNone || len(model.continueIDs) != 1 {
		t.Fatalf("delivery acknowledgement after replay lost continuation: %+v", model)
	}
	continuation := toolResultTurn(assignment, "request-file-after-replay", call.ID)
	model = updateModel(t, model, networkMessage{Assignment: &continuation})
	if model.pending.kind != pendingAccept || !model.pending.automatic ||
		model.pending.assignment.SessionKey() != continuation.SessionKey() {
		t.Fatalf("continuation after same-session replay was not accepted: %+v", model)
	}
}

func remoteStreamAssignment() completion.Assignment {
	assignment := testAssignment()
	assignment.CallerID = "remote-caller"
	assignment.WorkspaceKey = "remote-workspace"
	assignment.TaskID = "remote-task"
	assignment.IdempotencyKey = "request-remote"
	assignment.CapabilityTier = completion.TierRemoteTools
	return assignment
}

func handoffAssignment(t *testing.T, client *fakeClient, assignment completion.Assignment) Model {
	t.Helper()
	model := receiveAndAccept(t, New(client), assignment)
	model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: 'r', Mod: tea.ModCtrl})
	if send == nil || model.active != nil || !model.continueHandoff {
		t.Fatalf("handoff was not staged: %+v", model)
	}
	model = finishCommand(t, model, send)
	if model.pending.kind != pendingNone || !model.continueHandoff {
		t.Fatalf("handoff acknowledgement lost wait state: %+v", model)
	}
	return model
}

func nextTurn(assignment completion.Assignment, idempotencyKey string) completion.Assignment {
	next := assignment
	next.IdempotencyKey = idempotencyKey
	next.Request.Messages = []canonical.Message{{
		Role:   canonical.RoleUser,
		Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "continue"}},
	}}
	return next
}

func toolResultTurn(assignment completion.Assignment, idempotencyKey string, toolCallIDs ...string) completion.Assignment {
	next := assignment
	next.IdempotencyKey = idempotencyKey
	blocks := make([]canonical.Block, 0, len(toolCallIDs))
	for _, id := range toolCallIDs {
		blocks = append(blocks, canonical.Block{
			Type: canonical.BlockToolResult, ToolCallID: id, Output: `{"ok":true}`,
		})
	}
	next.Request.Messages = []canonical.Message{{Role: canonical.RoleTool, Blocks: blocks}}
	return next
}

func assertTranscriptEqual(t *testing.T, got, want *completion.Assignment, operation string) {
	t.Helper()
	if got == nil || want == nil || !reflect.DeepEqual(got.Request.Messages, want.Request.Messages) {
		t.Fatalf("failed %s transcript differs from pre-send state:\ngot:  %#v\nwant: %#v", operation, got, want)
	}
}

func stageSyntheticDelivery(
	t *testing.T,
	client *fakeClient,
) (Model, completion.Assignment, completion.ToolCall, eventSent) {
	t.Helper()
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role:   canonical.RoleUser,
		Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "write the file"}},
	}}})
	call := completion.ToolCall{
		ID: "tool-file-delivery", Name: "human_write_file",
		Input: map[string]any{"path": "/workspace/new.txt", "content": "new"},
	}
	model := New(client)
	model.active = &assignment
	model.rememberContext(assignment)
	namespace := mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)
	model.delivery = deliveryReview{
		stage: deliveryConfirming, sessionKey: assignment.SessionKey(), namespace: namespace,
		calls: []completion.ToolCall{call}, eventID: "event-file-delivery", generation: 1,
	}
	model.pending = pendingSend{
		kind: pendingDelivery, eventID: "event-file-delivery", assignment: assignment,
		context: cloneAssignment(model.lastContext), toolCalls: []completion.ToolCall{call},
		event: completion.Event{
			ID: "event-file-delivery", Type: completion.EventToolCalls,
			ToolCalls: []completion.ToolCall{call},
		},
		durable: true, deliveryNamespace: namespace, deliveryGeneration: 1,
		deliveryChanges: []workmirror.Change{{
			Kind: workmirror.ChangeWrite, Path: "new.txt", NewContent: []byte("new"),
		}},
	}

	model, send := updateModelWithCommand(t, model, mirrorConfirmationReady{
		sessionKey: assignment.SessionKey(), namespace: namespace, generation: 1,
		eventID: "event-file-delivery", calls: []completion.ToolCall{call},
	})
	if send == nil || model.delivery.stage != deliverySending || model.active != nil ||
		len(model.continueIDs) != 1 {
		t.Fatalf("file delivery was not staged: %+v", model)
	}
	message := commandResult(t, send)
	ack, ok := message.(eventSent)
	if !ok {
		t.Fatalf("file delivery command returned %T, want eventSent", message)
	}
	if ack.err != nil || len(client.events) != 1 || client.events[0].Type != completion.EventToolCalls {
		t.Fatalf("file delivery acknowledgement/events = %+v / %+v", ack, client.events)
	}
	return model, assignment, call, ack
}
