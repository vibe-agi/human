package tui

import (
	"reflect"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerproto"
)

func TestAssignmentBeforeReplayedNACKMergesEachPaneExactlyOnce(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := allPaneStateAssignment("assignment-before-replayed-nack")
	event := completion.Event{
		ID: "event-assignment-before-nack", Type: completion.EventProgress,
		Text: "rejected once\n\n",
	}
	draft := persistedDraft{
		Version: workerStateVersion, Authority: eventDraftAuthority(event.ID),
		RejectedEventIDs:   []string{event.ID},
		RejectedEventKinds: map[string]pendingSendKind{event.ID: pendingReply},
		Focus:              persistedFocusReply,
		Reply:              "rejected once",
		ReplyRejected:      "rejected once",
		Command:            "pwd",
		HasTasks:           true,
		Tasks:              []persistedTask{{Content: "keep task", Status: taskPending, Priority: "medium"}},
		TaskDirty:          true,
		TaskEditIndex:      -1,
		ToolInput:          `search {"query":"keep tool"}`,
		ToolCallIDs:        []string{"tool-keep-stable"},
	}
	model := New(client)
	model.stateDrafts[stateRecordKey{
		scope: stateScope(assignment), kind: workerStateDraftKind,
	}] = savedStateDraft{draft: draft, updatedAt: time.Now().UTC(), revision: 1}
	model = model.activateAssignment(assignment)

	model = updateModel(t, model, networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: event.ID, Message: "replayed durable rejection",
		},
		RejectedEvent:      &event,
		RejectedAssignment: &assignment,
	})

	assertEveryPaneRestored(t, model, "rejected once")
	if model.replyInput != "rejected once" || len(model.draftRejectedEvents) != 1 ||
		model.draftRejectedEvents[0] != event.ID {
		t.Fatalf("replayed NACK duplicated source or provenance: reply=%q ids=%v",
			model.replyInput, model.draftRejectedEvents)
	}
}

func TestTerminalNACKBeforeLocalSendResultPreservesEveryPane(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := allPaneStateAssignment("terminal-nack-before-send-result")
	model := New(client).activateAssignment(assignment)
	populateEveryPane(&model)

	updated, send := model.sendReply(completion.EventFinal, true)
	model = updated.(Model)
	if send == nil || model.pending.kind != pendingReply || model.active != nil {
		t.Fatalf("terminal reply was not staged before NACK: %+v", model)
	}
	rejected := model.pending.event
	model = updateModel(t, model, networkMessage{
		EventRejected: &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: rejected.ID, Message: "completion expired",
		},
		RejectedEvent:      &rejected,
		RejectedAssignment: &assignment,
	})

	assertEveryPaneRestored(t, model, "keep reply")
	if model.pending.kind != pendingNone || model.active != nil {
		t.Fatalf("terminal NACK left a live send/session: pending=%+v active=%+v", model.pending, model.active)
	}
}

func TestContinuationBeforeLocalSendResultPreservesIndependentPanes(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := allPaneStateAssignment("continuation-before-send-result")
	model := New(client).activateAssignment(assignment)
	populateEveryPane(&model)

	updated, send := model.sendReply(completion.EventClarification, true)
	model = updated.(Model)
	if send == nil || model.pending.kind != pendingReply || model.active != nil || !model.continueHandoff {
		t.Fatalf("clarification was not staged before continuation: %+v", model)
	}
	oldEventID := model.pending.event.ID
	continuation := nextTurn(assignment, "continuation-proves-old-send")
	model, accept := updateModelWithCommand(t, model, networkMessage{Assignment: &continuation})
	if accept == nil || model.pending.kind != pendingAccept || !model.pending.automatic ||
		model.pending.event.ID == oldEventID {
		t.Fatalf("continuation did not replace the proven terminal send with one accept: %+v", model)
	}

	model = updateModel(t, model, eventSent{eventID: model.pending.event.ID})
	assertOtherPanesSurvived(t, model, pendingReply)
	if model.active == nil || model.active.SessionKey() != continuation.SessionKey() || model.replyInput != "" {
		t.Fatalf("continuation restored the delivered source reply or lost ownership: %+v", model)
	}
}

func TestPreviouslyRejectedSendResultPreservesEveryPane(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := allPaneStateAssignment("previously-rejected-before-cleanup")
	model := New(client).activateAssignment(assignment)
	populateEveryPane(&model)

	updated, send := model.sendReply(completion.EventFinal, true)
	model = updated.(Model)
	if send == nil || model.pending.kind != pendingReply || model.active != nil {
		t.Fatalf("terminal reply was not staged: %+v", model)
	}
	eventID := model.pending.event.ID
	model = updateModel(t, model, eventSent{
		eventID: eventID,
		err:     workerclient.ErrEventPreviouslyRejected,
	})

	assertEveryPaneRestored(t, model, "keep reply")
	if model.pending.kind != pendingNone || model.active != nil {
		t.Fatalf("previous rejection left a live send/session: pending=%+v active=%+v", model.pending, model.active)
	}
}

func TestDuplicateDurableToolNACKDoesNotDuplicateCommandOrComposer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		assignment completion.Assignment
		event      completion.Event
		assert     func(*testing.T, Model)
	}{
		{
			name: "command", assignment: allPaneStateAssignment("duplicate-command-nack"),
			event: completion.Event{
				ID: "event-duplicate-command-nack", Type: completion.EventToolCalls,
				ToolCalls: []completion.ToolCall{{
					ID: "tool-duplicate-command", Name: "bash",
					Input: map[string]any{"command": "go test ./..."},
				}},
			},
			assert: func(t *testing.T, model Model) {
				if model.commandInput != "go test ./..." {
					t.Fatalf("duplicate command NACK changed restored command: %q", model.commandInput)
				}
			},
		},
		{
			name: "advanced tools", assignment: allPaneStateAssignment("duplicate-tools-nack"),
			event: completion.Event{
				ID: "event-duplicate-tools-nack", Type: completion.EventToolCalls,
				ToolCalls: []completion.ToolCall{{
					ID: "tool-duplicate-search", Name: "search",
					Input: map[string]any{"query": "one copy"},
				}},
			},
			assert: func(t *testing.T, model Model) {
				if !model.composing || model.input != `search {"query":"one copy"}` ||
					len(model.toolCallIDs) != 1 {
					t.Fatalf("duplicate advanced-tool NACK changed composer: input=%q ids=%v",
						model.input, model.toolCallIDs)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := networkMessage{
				EventRejected: &workerproto.EventRejected{
					CallerID: test.assignment.CallerID, IdempotencyKey: test.assignment.IdempotencyKey,
					EventID: test.event.ID,
				},
				RejectedEvent:      &test.event,
				RejectedAssignment: &test.assignment,
			}
			model := updateModel(t, New(newFakeClient()), message)
			model = updateModel(t, model, message)
			test.assert(t, model)
		})
	}
}

func assertEveryPaneRestored(t *testing.T, model Model, reply string) {
	t.Helper()
	if model.replyInput != reply || model.commandInput != "pwd" ||
		len(model.agentTasks) != 1 || model.agentTasks[0].Content != "keep task" || !model.taskDirty ||
		!model.composing || model.input != `search {"query":"keep tool"}` ||
		!reflect.DeepEqual(model.toolCallIDs, []string{"tool-keep-stable"}) {
		t.Fatalf("all-pane recovery failed: reply=%q command=%q tasks=%+v dirty=%t composing=%t input=%q ids=%v",
			model.replyInput, model.commandInput, model.agentTasks, model.taskDirty,
			model.composing, model.input, model.toolCallIDs)
	}
}
