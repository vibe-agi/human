package tui

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

func TestChatReplyStreamsPlainTextContinuously(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := receiveAndAccept(t, New(client), testAssignment())

	model = updateModel(t, model, tea.KeyPressMsg{Text: "中文 aqtx [] {}", Code: 0})
	model = updateModel(t, model, tea.KeyPressMsg{Code: 'j', Mod: tea.ModCtrl})
	model = updateModel(t, model, tea.KeyPressMsg{Text: "第二行", Code: 0})
	model, firstSend := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if firstSend == nil {
		t.Fatal("Enter did not stream the first reply segment")
	}
	if model.active == nil || model.focus != focusReply || model.pending.kind != pendingReply || model.replyInput != "" {
		t.Fatalf("first progress segment closed or dirtied the human turn: %+v", model)
	}
	model = finishCommand(t, model, firstSend)
	if model.active == nil || model.focus != focusReply || model.pending.kind != pendingNone {
		t.Fatalf("first progress acknowledgement did not keep the stream open: %+v", model)
	}

	model = updateModel(t, model, tea.KeyPressMsg{Text: "我再补一段", Code: 0})
	model, secondSend := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if secondSend == nil {
		t.Fatal("Enter did not stream the second reply segment")
	}
	model = finishCommand(t, model, secondSend)

	if len(client.events) != 3 {
		t.Fatalf("events = %+v", client.events)
	}
	if client.events[0].Type != completion.EventAccepted ||
		client.events[1].Type != completion.EventProgress ||
		client.events[1].Text != "中文 aqtx [] {}\n第二行\n\n" ||
		client.events[2].Type != completion.EventProgress ||
		client.events[2].Text != "我再补一段\n\n" {
		t.Fatalf("continuous stream events = %+v", client.events)
	}
	if model.active == nil || model.focus != focusReply || model.replyInput != "" {
		t.Fatalf("second progress segment closed the human turn: %+v", model)
	}
	plain := ansi.Strip(model.View().Content)
	for _, want := range []string{"HUMAN", "中文 aqtx [] {}", "我再补一段", "HUMAN TURN"} {
		if !strings.Contains(plain, want) {
			t.Fatalf("streamed chat omitted %q:\n%s", want, plain)
		}
	}
}

func TestPendingProgressKeepsTypingButBlocksASecondResponseEvent(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := receiveAndAccept(t, New(client), testAssignment())

	model = updateModel(t, model, tea.KeyPressMsg{Text: "first", Code: 0})
	model, firstSend := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if firstSend == nil {
		t.Fatal("first progress segment did not produce a command")
	}
	firstResult := commandResult(t, firstSend)
	if len(client.events) != 2 || client.events[1].Type != completion.EventProgress {
		t.Fatalf("first progress event = %+v", client.events)
	}

	model = updateModel(t, model, tea.KeyPressMsg{Text: "typed while saving", Code: 0})
	model, duplicate := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if duplicate != nil || model.replyInput != "typed while saving" || len(client.events) != 2 {
		t.Fatalf("pending event allowed a duplicate response or lost the new draft: model=%+v events=%+v", model, client.events)
	}
	if !strings.Contains(model.status, "still being committed") {
		t.Fatalf("pending response status = %q", model.status)
	}

	updated, _ := model.Update(firstResult)
	model = updated.(Model)
	model, secondSend := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if secondSend == nil {
		t.Fatal("draft typed during the pending send was not sendable after acknowledgement")
	}
	model = finishCommand(t, model, secondSend)
	if len(client.events) != 3 || client.events[2].Type != completion.EventProgress ||
		client.events[2].Text != "typed while saving\n\n" || model.active == nil {
		t.Fatalf("second progress event = %+v, model = %+v", client.events, model)
	}
}

func TestModifiedEnterNewlinesRemainAvailableWithoutSending(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := receiveAndAccept(t, New(client), testAssignment())
	model.setReplyValue("first")

	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyEnter, Mod: tea.ModShift})
	model = updateModel(t, model, tea.KeyPressMsg{Text: "second", Code: 0})
	model = updateModel(t, model, tea.KeyPressMsg{Code: 'j', Mod: tea.ModCtrl})
	model = updateModel(t, model, tea.KeyPressMsg{Text: "third", Code: 0})
	if model.replyInput != "first\nsecond\nthird" {
		t.Fatalf("modified Enter did not insert newlines: %q", model.replyInput)
	}
	if len(client.events) != 1 || client.events[0].Type != completion.EventAccepted {
		t.Fatalf("newline key sent a response event: %+v", client.events)
	}
	view := model.View()
	if !view.KeyboardEnhancements.ReportAlternateKeys || view.KeyboardEnhancements.ReportAllKeysAsEscapeCodes {
		t.Fatalf("unsafe or missing keyboard enhancements: %+v", view.KeyboardEnhancements)
	}
}

func TestReplyLifecycleUsesHandoffAndExplicitFinish(t *testing.T) {
	t.Parallel()

	t.Run("handoff", func(t *testing.T) {
		client := newFakeClient()
		model := receiveAndAccept(t, New(client), testAssignment())
		model = updateModel(t, model, tea.KeyPressMsg{Text: "please continue", Code: 0})
		model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: 'r', Mod: tea.ModCtrl})
		if send == nil || model.active != nil || model.focus != focusTasks || !model.continueHandoff {
			t.Fatalf("Ctrl+R did not hand the turn to the Agent: %+v", model)
		}
		model = finishCommand(t, model, send)
		if len(client.events) != 2 || client.events[1].Type != completion.EventClarification ||
			client.events[1].Text != "please continue" {
			t.Fatalf("handoff events = %+v", client.events)
		}
		if plain := ansi.Strip(model.View().Content); !strings.Contains(plain, "WAITING FOR AGENT") {
			t.Fatalf("handoff state is not visible:\n%s", plain)
		}
	})

	t.Run("finish", func(t *testing.T) {
		client := newFakeClient()
		model := receiveAndAccept(t, New(client), testAssignment())
		model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl})
		if send == nil || model.active != nil || model.focus != focusTasks || model.continueHandoff {
			t.Fatalf("Ctrl+D did not explicitly finish the conversation: %+v", model)
		}
		model = finishCommand(t, model, send)
		if len(client.events) != 2 || client.events[1].Type != completion.EventFinal || client.events[1].Text != "" {
			t.Fatalf("final events = %+v", client.events)
		}
		if model.active != nil || model.pending.kind != pendingNone {
			t.Fatalf("final acknowledgement left an active response: %+v", model)
		}
	})
}

func TestInboxRequiresExplicitAcceptOrReject(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	assignment := testAssignment()
	model = updateModel(t, model, networkMessage{Assignment: &assignment})

	model, command := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if command != nil || model.active != nil || len(model.assignments) != 1 || len(client.events) != 0 {
		t.Fatalf("Inbox Enter unexpectedly accepted the request: %+v", model)
	}
	if !strings.Contains(model.status, "press a to accept or r to reject") {
		t.Fatalf("Inbox Enter guidance = %q", model.status)
	}

	model, command = updateModelWithCommand(t, model, tea.KeyPressMsg{Text: "r", Code: 'r'})
	if command == nil || model.pending.kind != pendingReject || len(model.assignments) != 1 {
		t.Fatalf("reject did not wait for its local acknowledgement: %+v", model)
	}
	model = finishCommand(t, model, command)
	if len(model.assignments) != 0 || len(client.events) != 1 || client.events[0].Type != completion.EventRejected {
		t.Fatalf("rejected model = %+v, events = %+v", model, client.events)
	}
}

func TestReplyTextareaSupportsCursorEditingAndMultilinePaste(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := receiveAndAccept(t, New(client), testAssignment())

	model = updateModel(t, model, tea.KeyPressMsg{Text: "ac", Code: 0})
	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyLeft})
	model = updateModel(t, model, tea.KeyPressMsg{Text: "b", Code: 'b'})
	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyEnd})
	model = updateModel(t, model, tea.PasteMsg{Content: "\nline two\n第三行"})
	if model.replyInput != "abc\nline two\n第三行" {
		t.Fatalf("textarea edit = %q", model.replyInput)
	}

	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyUp})
	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyEnd})
	model = updateModel(t, model, tea.KeyPressMsg{Text: "!", Code: '!'})
	if model.replyInput != "abc\nline two!\n第三行" {
		t.Fatalf("multiline cursor edit = %q", model.replyInput)
	}

	model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if send == nil {
		t.Fatal("edited multiline reply did not stream")
	}
	model = finishCommand(t, model, send)
	if got := client.events[len(client.events)-1]; got.Type != completion.EventProgress ||
		got.Text != "abc\nline two!\n第三行\n\n" {
		t.Fatalf("multiline progress = %+v", got)
	}
}

func TestChatViewportPagesHistoryWithoutLeavingReplyFocus(t *testing.T) {
	t.Parallel()
	assignment := testAssignment()
	assignment.Request.Messages = nil
	for index := range 48 {
		assignment.Request.Messages = append(assignment.Request.Messages, canonical.Message{
			Role: canonical.RoleUser,
			Blocks: []canonical.Block{{
				Type: canonical.BlockText,
				Text: fmt.Sprintf("history-%02d %s", index, strings.Repeat("context ", 8)),
			}},
		})
	}
	model := receiveAndAccept(t, New(newFakeClient()), assignment)
	model = updateModel(t, model, tea.WindowSizeMsg{Width: 80, Height: 24})
	_ = model.View()
	bottom := model.ui.chat.YOffset()
	if bottom <= 0 || !model.ui.chat.AtBottom() {
		t.Fatalf("history viewport did not start at the newest messages: offset=%d", bottom)
	}

	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyPgUp})
	up := model.ui.chat.YOffset()
	if up >= bottom || model.ui.chatFollow || model.focus != focusReply {
		t.Fatalf("PgUp did not page chat history independently: before=%d after=%d model=%+v", bottom, up, model)
	}
	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyPgDown})
	if model.ui.chat.YOffset() <= up || !model.ui.chat.AtBottom() || !model.ui.chatFollow || model.focus != focusReply {
		t.Fatalf("PgDn did not return to live follow mode: offset=%d model=%+v", model.ui.chat.YOffset(), model)
	}
}

func TestWorkspaceLeavesMouseSelectionToTheTerminal(t *testing.T) {
	t.Parallel()
	model := New(newFakeClient())
	view := model.View()
	if view.MouseMode != tea.MouseModeNone {
		t.Fatalf("mouse mode = %v, want MouseModeNone so terminal drag-selection works", view.MouseMode)
	}
	if model.ui.chat.MouseWheelEnabled {
		t.Fatal("chat viewport enables mouse wheel even though the terminal owns mouse selection")
	}
}

func TestModernWorkspaceHasExactCompactAndWideGeometry(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{
		taskTool("todowrite", openCodeTodoWriteSchema),
		{Name: "bash", InputSchema: json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`)},
	}
	model := receiveAndAccept(t, New(client), assignment)

	for _, size := range []struct {
		name          string
		width, height int
		wide          bool
	}{
		{name: "compact", width: 80, height: 24},
		{name: "wide", width: 120, height: 30, wide: true},
	} {
		t.Run(size.name, func(t *testing.T) {
			resized := updateModel(t, model, tea.WindowSizeMsg{Width: size.width, Height: size.height})
			plain, lines := assertModernWorkspaceGeometry(t, resized.View().Content, size.width, size.height)
			assertAnchorsInOrder(t, plain, "Chat", "Tasks", "Command", "Reply", "Status")
			for _, want := range []string{
				"HUMAN TURN", "Agent plan", "runs on client Agent",
				"Enter stream", "Ctrl+R handoff", "Ctrl+D end",
			} {
				if !strings.Contains(plain, want) {
					t.Fatalf("%s layout omitted %q:\n%s", size.name, want, plain)
				}
			}
			if size.wide {
				if !strings.Contains(lines[1], "Chat") || !strings.Contains(lines[1], "Tasks") {
					t.Fatalf("wide layout did not put Tasks in the right rail:\n%s", plain)
				}
			} else if strings.Contains(lines[1], "Tasks") {
				t.Fatalf("compact layout unexpectedly rendered a side rail:\n%s", plain)
			}
		})
	}
}

func TestIdleWorkspaceShowsOneWaitingStateAndNoInactiveActions(t *testing.T) {
	t.Parallel()

	for _, size := range []struct {
		name          string
		width, height int
	}{
		{name: "compact", width: 80, height: 24},
		{name: "wide", width: 120, height: 30},
	} {
		t.Run(size.name, func(t *testing.T) {
			model := updateModel(t, New(newFakeClient()), tea.WindowSizeMsg{Width: size.width, Height: size.height})
			plain, _ := assertModernWorkspaceGeometry(t, model.View().Content, size.width, size.height)

			if count := strings.Count(plain, "Waiting for"); count != 1 {
				t.Fatalf("idle workspace has %d waiting messages, want 1:\n%s", count, plain)
			}
			for _, want := range []string{
				"IDLE", "No queued requests.",
				"Waiting for OpenCode, Claude Code, or Codex to call the Human Expert model",
				"Reply · available after accepting a request", "Status  ready", "Ctrl+C quit",
			} {
				if !strings.Contains(plain, want) {
					t.Fatalf("idle workspace omitted %q:\n%s", want, plain)
				}
			}
			for range 3 {
				plain = ansi.Strip(model.View().Content)
				for _, unwanted := range []string{
					"Waiting for the client Agent", "a accept", "r reject",
					"Enter stream", "Enter send command", "Ctrl+R handoff", "Ctrl+D end",
				} {
					if strings.Contains(plain, unwanted) {
						t.Fatalf("idle workspace exposed inactive action %q:\n%s", unwanted, plain)
					}
				}
				model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab})
			}
		})
	}
}

func TestQueuedWorkspaceShowsInboxActionsButNoReplyActions(t *testing.T) {
	t.Parallel()
	model := New(newFakeClient())
	assignment := testAssignment()
	model = updateModel(t, model, networkMessage{Assignment: &assignment})
	plain := ansi.Strip(model.View().Content)

	for _, want := range []string{
		"INBOX 1", "Reply · accept the selected Inbox request to reply",
		"a accept", "r reject",
	} {
		if !strings.Contains(plain, want) {
			t.Fatalf("queued workspace omitted %q:\n%s", want, plain)
		}
	}
	for _, unwanted := range []string{"Enter stream", "Ctrl+R handoff", "Ctrl+D end"} {
		if strings.Contains(plain, unwanted) {
			t.Fatalf("queued workspace exposed reply action %q:\n%s", unwanted, plain)
		}
	}
}

func TestModernHeaderAndCommandWarningTellTheTruth(t *testing.T) {
	t.Run("reconnecting header", func(t *testing.T) {
		model := updateModel(t, New(newFakeClient()), networkMessage{Err: errors.New("socket closed")})
		plain := ansi.Strip(model.View().Content)
		if strings.Contains(plain, "● connected") || !strings.Contains(plain, "reconnecting") {
			t.Fatalf("connection header contradicts network state:\n%s", plain)
		}
	})

	t.Run("warning retains exact command", func(t *testing.T) {
		assignment := testAssignment()
		assignment.Request.Tools = []canonical.Tool{{
			Name:        "bash",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`),
		}}
		model := receiveAndAccept(t, New(newFakeClient()), assignment)
		model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab})
		model = updateModel(t, model, tea.PasteMsg{Content: "rm -rf /tmp/human-test"})
		model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
		if send != nil {
			t.Fatal("dangerous command bypassed the confirmation screen")
		}
		plain := ansi.Strip(model.View().Content)
		if !strings.Contains(plain, "rm -rf /tmp/human-test") ||
			!strings.Contains(plain, "Enter again to send") {
			t.Fatalf("warning hid the command being confirmed:\n%s", plain)
		}
	})
}

func TestTasksPaneEditsAndSyncsOpenCodePlan(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{taskTool("todowrite", openCodeTodoWriteSchema)}
	model := receiveAndAccept(t, New(client), assignment)

	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab, Mod: tea.ModShift})
	if model.focus != focusTasks {
		t.Fatalf("focus = %v, want Tasks", model.focus)
	}
	model = enterTaskDraft(t, model, "修 P1：安全与挂起类")
	model = enterTaskDraft(t, model, "修 P2：传输健壮性")
	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeySpace})
	if len(model.agentTasks) != 2 || model.agentTasks[1].Status != taskInProgress || !model.taskDirty {
		t.Fatalf("task draft = %+v", model.agentTasks)
	}

	model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	if send == nil || model.active != nil || !model.taskSyncWait {
		t.Fatalf("task sync state = %+v", model)
	}
	model = finishCommand(t, model, send)
	if len(client.events) != 2 || client.events[1].Type != completion.EventToolCalls ||
		len(client.events[1].ToolCalls) != 1 || client.events[1].ToolCalls[0].Name != "todowrite" {
		t.Fatalf("task event = %+v", client.events)
	}
	call := client.events[1].ToolCalls[0]
	todos, ok := call.Input["todos"].([]map[string]any)
	if !ok || len(todos) != 2 || todos[1]["status"] != "in_progress" || todos[1]["content"] != "修 P2：传输健壮性" {
		t.Fatalf("todowrite input = %#v", call.Input)
	}

	resultJSON, err := json.Marshal(todos)
	if err != nil {
		t.Fatal(err)
	}
	continuation := testAssignment()
	continuation.TaskID = "task-next"
	continuation.IdempotencyKey = "request-next"
	continuation.Request.Tools = assignment.Request.Tools
	continuation.Request.Messages = []canonical.Message{
		{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: call.ID, ToolName: call.Name, Input: call.Input,
		}}},
		{Role: canonical.RoleTool, Blocks: []canonical.Block{{
			Type: canonical.BlockToolResult, ToolCallID: call.ID, Output: string(resultJSON),
		}}},
	}
	model, accepted := updateModelWithCommand(t, model, networkMessage{Assignment: &continuation})
	if accepted == nil || model.active != nil || model.pending.kind != pendingAccept || len(model.assignments) != 0 {
		t.Fatalf("continuation was not automatically accepted: %+v", model)
	}
	// The returned command is a batch that also waits on the network. Feed the
	// local acceptance acknowledgement directly so this unit test never blocks.
	model = updateModel(t, model, eventSent{eventID: model.pending.eventID})
	if model.active == nil || model.active.TaskID != "task-next" || model.focus != focusReply || model.taskSyncWait {
		t.Fatalf("continuation acknowledgement did not resume the desk: %+v", model)
	}
	if len(model.agentTasks) != 2 || model.agentTasks[1].Status != taskInProgress {
		t.Fatalf("confirmed task list = %+v", model.agentTasks)
	}
}

func TestDeclaredBashCommandPaneSendsToolCallWithoutLocalWorkspaceGuess(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{{
		Name: "bash", InputSchema: json.RawMessage(`{
          "type":"object",
          "properties":{
            "command":{"type":"string"},
            "timeout":{"type":"integer"},
            "workdir":{"type":"string"}
          },
          "required":["command"]
        }`),
	}}
	model := receiveAndAccept(t, New(client), assignment)
	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab})
	if model.focus != focusCommand {
		t.Fatalf("focus = %v, want Command", model.focus)
	}
	model = updateModel(t, model, tea.KeyPressMsg{Text: "pwd", Code: 0})
	model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if send == nil {
		t.Fatal("command Enter did not produce a send command")
	}
	model = finishCommand(t, model, send)
	call := client.events[len(client.events)-1].ToolCalls[0]
	if call.Name != "bash" || call.Input["command"] != "pwd" {
		t.Fatalf("bash call = %+v", call)
	}
	if _, exists := call.Input["workdir"]; exists {
		t.Fatalf("Basic command guessed a workspace: %+v", call.Input)
	}
}

func TestFailedProgressSendRestoresTurnAndMergedDraft(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := receiveAndAccept(t, New(client), testAssignment())
	client.sendErr = errors.New("outbox unavailable")
	model = updateModel(t, model, tea.KeyPressMsg{Text: "do not lose me", Code: 0})
	model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if send == nil || model.active == nil || model.pending.kind != pendingReply {
		t.Fatalf("pending progress state = %+v", model)
	}
	model = updateModel(t, model, tea.KeyPressMsg{Text: "new tail", Code: 0})
	model = finishCommand(t, model, send)
	if model.active == nil || model.focus != focusReply || model.replyInput != "do not lose me\n\nnew tail" ||
		model.pending.kind != pendingNone || !strings.Contains(model.status, "draft restored") {
		t.Fatalf("failed send did not restore the merged draft: %+v", model)
	}
}

func receiveAndAccept(t *testing.T, model Model, assignment completion.Assignment) Model {
	t.Helper()
	model = updateModel(t, model, networkMessage{Assignment: &assignment})
	model, accepted := updateModelWithCommand(t, model, tea.KeyPressMsg{Text: "a", Code: 'a'})
	if accepted == nil || model.active != nil || model.pending.kind != pendingAccept {
		t.Fatalf("Inbox a did not start a pending acceptance: %+v", model)
	}
	model = finishCommand(t, model, accepted)
	if model.active == nil || model.focus != focusReply || model.pending.kind != pendingNone {
		t.Fatalf("accepted acknowledgement did not activate the request: %+v", model)
	}
	return model
}

func updateModel(t *testing.T, model Model, message tea.Msg) Model {
	t.Helper()
	updated, _ := model.Update(message)
	return updated.(Model)
}

func updateModelWithCommand(t *testing.T, model Model, message tea.Msg) (Model, tea.Cmd) {
	t.Helper()
	updated, command := model.Update(message)
	return updated.(Model), command
}

func finishCommand(t *testing.T, model Model, command tea.Cmd) Model {
	t.Helper()
	if command == nil {
		t.Fatal("expected a Bubble Tea command")
	}
	message := commandResult(t, command)
	if message == nil {
		t.Fatal("Bubble Tea command returned no acknowledgement")
	}
	return updateModel(t, model, message)
}

// commandResult unwraps the Batch used to start an animation alongside an
// asynchronous operation. Bubble Tea normally dispatches BatchMsg itself;
// direct model tests need to select the operation acknowledgement while
// ignoring the independent spinner tick.
func commandResult(t *testing.T, command tea.Cmd) tea.Msg {
	t.Helper()
	if command == nil {
		t.Fatal("expected a Bubble Tea command")
	}
	message := command()
	batch, ok := message.(tea.BatchMsg)
	if !ok {
		return message
	}
	var result tea.Msg
	for _, child := range batch {
		if child == nil {
			continue
		}
		candidate := child()
		if _, animation := candidate.(spinner.TickMsg); animation {
			continue
		}
		if candidate == nil {
			continue
		}
		if result != nil {
			t.Fatalf("Bubble Tea batch returned multiple operation messages: %T and %T", result, candidate)
		}
		result = candidate
	}
	return result
}

func assertModernWorkspaceGeometry(t *testing.T, rendered string, width, height int) (string, []string) {
	t.Helper()
	plain := ansi.Strip(rendered)
	lines := strings.Split(plain, "\n")
	if len(lines) != height {
		t.Fatalf("workspace is %d rows, want %d:\n%s", len(lines), height, plain)
	}
	for index, line := range lines {
		if cells := ansi.StringWidth(line); cells != width {
			t.Fatalf("workspace row %d is %d cells, want %d: %q", index, cells, width, line)
		}
	}
	return plain, lines
}

func assertAnchorsInOrder(t *testing.T, rendered string, anchors ...string) {
	t.Helper()
	previous := -1
	for _, anchor := range anchors {
		index := strings.Index(rendered, anchor)
		if index < 0 {
			t.Fatalf("workspace omitted %q:\n%s", anchor, rendered)
		}
		if index <= previous {
			t.Fatalf("workspace anchor %q appeared out of order:\n%s", anchor, rendered)
		}
		previous = index
	}
}

func enterTaskDraft(t *testing.T, model Model, content string) Model {
	t.Helper()
	model = updateModel(t, model, tea.KeyPressMsg{Text: "n", Code: 'n'})
	model = updateModel(t, model, tea.KeyPressMsg{Text: content, Code: 0})
	return updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
}
