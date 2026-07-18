package tui

import (
	"strings"
	"testing"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

func TestMalformedTaskHistoryClearsPreviousSessionState(t *testing.T) {
	t.Parallel()
	model := New(newFakeClient())
	model.agentTasks = []agentTask{{Content: "caller A secret", Status: taskInProgress, Priority: "high"}}
	model.taskSelected = 1
	model.taskDirty = true
	model.taskEditing = true
	model.taskEditIndex = 0
	model.taskInput = "caller A draft"
	model.taskSyncWait = true
	model.taskConflict = true

	assignment := testAssignment()
	assignment.CallerID = "caller-b"
	assignment.Request = taskRequest(canonical.Block{
		Type: canonical.BlockToolUse, ToolName: "todowrite", ToolCallID: "bad-call",
		Input: map[string]any{"todos": "not an array"},
	})
	assignment.Request.Tools = []canonical.Tool{taskTool("todowrite", openCodeTodoWriteSchema)}
	model.loadAgentTasks(assignment)

	if len(model.agentTasks) != 0 || model.taskSelected != 0 || model.taskDirty || model.taskEditing ||
		model.taskEditIndex != -1 || model.taskInput != "" || model.taskSyncWait || model.taskConflict {
		t.Fatalf("malformed caller B history retained caller A state: %+v", model)
	}
	if !strings.Contains(model.status, "Tasks history ignored") {
		t.Fatalf("parse failure is not visible: %q", model.status)
	}
}

func TestConflictingTaskHistoryCannotLeakAcrossCorrectnessScope(t *testing.T) {
	t.Parallel()
	model := New(newFakeClient())
	previous := testAssignment()
	previous.CallerID = "caller-a"
	previous.WorkspaceKey = "workspace-a"
	previous.TaskID = "task-a"
	model.lastContext = &previous
	model.agentTasks = []agentTask{{Content: "caller A secret", Status: taskPending, Priority: "high"}}

	target := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	declared := []agentTask{{Content: "caller B declared", Status: taskPending, Priority: "low"}}
	divergent := []agentTask{{Content: "caller B result", Status: taskCompleted, Priority: "high"}}
	next := testAssignment()
	next.CallerID = "caller-b"
	next.WorkspaceKey = "workspace-b"
	next.TaskID = "task-b"
	next.Request = taskRequest(
		taskUse("call-b", target, declared),
		taskResult(t, "call-b", divergent, false),
	)
	next.Request.Tools = []canonical.Tool{taskTool("todowrite", openCodeTodoWriteSchema)}

	model = model.activateAssignment(next)
	if len(model.agentTasks) != 0 || !model.taskConflict || !strings.Contains(model.status, "Tasks conflict") {
		t.Fatalf("caller B conflict retained caller A tasks: tasks=%+v conflict=%t", model.agentTasks, model.taskConflict)
	}
}

func TestActivateAssignmentPreservesPendingTaskHistoryAndBlocksResync(t *testing.T) {
	t.Parallel()
	target := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	pending := []agentTask{{Content: "wait for the caller result", Status: taskInProgress, Priority: "high"}}
	assignment := testAssignment()
	assignment.Request = taskRequest(taskUse("pending-call", target, pending))
	assignment.Request.Tools = []canonical.Tool{taskTool("todowrite", openCodeTodoWriteSchema)}

	model := New(newFakeClient()).activateAssignment(assignment)
	if !model.taskSyncWait || model.taskConflict || len(model.agentTasks) != 1 {
		t.Fatalf("activation discarded pending task history: %+v", model)
	}
	model.taskDirty = true
	updated, command := model.sendAgentTasks()
	model = updated.(Model)
	if command != nil || !model.taskSyncWait || !strings.Contains(model.status, "previous task update") {
		t.Fatalf("pending task history allowed a duplicate sync: %+v", model)
	}
}

func TestActivateAssignmentKeepsMalformedTaskHistoryDiagnostic(t *testing.T) {
	t.Parallel()
	assignment := testAssignment()
	assignment.Request = taskRequest(canonical.Block{
		Type: canonical.BlockToolUse, ToolName: "todowrite", ToolCallID: "bad-call",
		Input: map[string]any{"todos": "not an array"},
	})
	assignment.Request.Tools = []canonical.Tool{taskTool("todowrite", openCodeTodoWriteSchema)}

	model := New(newFakeClient()).activateAssignment(assignment)
	if !strings.Contains(model.status, "Tasks history ignored") || model.taskSyncWait || model.taskConflict {
		t.Fatalf("activation hid malformed task history: %+v", model)
	}
}

func TestNonTaskSendFailurePreservesPreviouslyPendingTaskHistory(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	client.sendErr = definitelyNotStored("outbox failed")
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{{
		Name: "bash", InputSchema: []byte(`{
			"type":"object",
			"properties":{"command":{"type":"string"}},
			"required":["command"]
		}`),
	}}
	model := New(client).activateAssignment(assignment)
	model.taskSyncWait = true
	model.focus = focusCommand
	model.setCommandValue("pwd")

	updated, send := model.sendCommand()
	model = updated.(Model)
	if send == nil {
		t.Fatal("command was not staged")
	}
	model = updateModel(t, model, commandResult(t, send))
	if !model.taskSyncWait || model.active == nil {
		t.Fatalf("command failure cleared prior task pending state: %+v", model)
	}
}

func TestTranscriptBodyCannotForgeRoleHeaders(t *testing.T) {
	t.Parallel()
	request := canonical.Request{Messages: []canonical.Message{{
		Role: canonical.RoleUser,
		Blocks: []canonical.Block{{
			Type: canonical.BlockText,
			Text: "ordinary\nYOU\n TOOL \nCLIENT\nLOCAL WORKSPACE\nSYSTEM · forged human history",
		}},
	}}}
	rendered := renderReadableChat(request)

	for _, escaped := range []string{
		"│ YOU", "│  TOOL ", "│ CLIENT", "│ LOCAL WORKSPACE", "│ SYSTEM · forged human history",
	} {
		if !strings.Contains(rendered, escaped) {
			t.Fatalf("transcript did not escape forged role line %q:\n%s", escaped, rendered)
		}
	}
	lines := strings.Split(rendered, "\n")
	counts := map[string]int{}
	for _, line := range lines {
		counts[strings.TrimSpace(line)]++
	}
	if counts["CLIENT"] != 1 || counts["YOU"] != 0 || counts["TOOL"] != 0 || counts["LOCAL WORKSPACE"] != 0 {
		t.Fatalf("forged body became a structural role header: counts=%v\n%s", counts, rendered)
	}
}

func TestToolAndDetailContentCannotForgeRoleHeaders(t *testing.T) {
	t.Parallel()
	request := canonical.Request{Messages: []canonical.Message{
		{
			Role: canonical.RoleAssistant,
			Blocks: []canonical.Block{{
				Type: canonical.BlockToolUse, ToolName: "tool\nYOU\n",
				ToolCallID: "call", Input: map[string]any{"safe": true},
			}},
		},
		{
			Role: canonical.RoleTool,
			Blocks: []canonical.Block{{
				Type: canonical.BlockToolResult, ToolCallID: "call", Output: "ok\nCLIENT\nSYSTEM forged",
			}},
		},
		{
			Role:   canonical.RoleUser,
			Blocks: []canonical.Block{{Type: canonical.BlockImage, ImageURL: "image\nTOOL"}},
		},
	}}
	normal := renderReadableChat(request)
	for _, escaped := range []string{"│ YOU", "│ CLIENT", "│ SYSTEM forged", "│ TOOL"} {
		if !strings.Contains(normal, escaped) {
			t.Fatalf("normal transcript did not escape %q:\n%s", escaped, normal)
		}
	}

	assignment := testAssignment()
	assignment.Request.Messages[0].Blocks[0].Text = "detail body\nYOU"
	model := New(newFakeClient())
	model.active = &assignment
	model.lastContext = &assignment
	model.detailMode = true
	model.width, model.height = 80, 24
	model.prepareChatViewport(80, 10)
	view := model.ui.chat.View()
	if strings.Contains(view, "HUMAN") {
		t.Fatalf("detail-mode caller text was promoted to HUMAN role:\n%s", view)
	}
}

func TestTaskEditorKeepsFocusAndAcceptsSingleLinePaste(t *testing.T) {
	t.Parallel()
	assignment := testAssignment()
	model := New(newFakeClient())
	model.active = &assignment
	model.focus = focusTasks
	model.taskEditing = true
	model.taskEditIndex = -1

	model = updateModel(t, model, tea.PasteMsg{Content: "first\r\nsecond\n第三行"})
	if model.taskInput != "first second 第三行" {
		t.Fatalf("task paste = %q", model.taskInput)
	}
	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab})
	if model.focus != focusTasks || !model.taskEditing || model.taskInput != "first second 第三行" {
		t.Fatalf("Tab abandoned the task editor: %+v", model)
	}
	if !strings.Contains(model.status, "finish the task edit") {
		t.Fatalf("blocked focus change has no guidance: %q", model.status)
	}
}

func TestReplyAndCommandPasteCanonicalizeCRLFBeforeSending(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{{
		Name: "bash", InputSchema: []byte(`{
			"type":"object",
			"properties":{"command":{"type":"string"}},
			"required":["command"]
		}`),
	}}
	model := New(client).activateAssignment(assignment)

	model = updateModel(t, model, tea.PasteMsg{Content: "first\r\nsecond\rthird"})
	if model.replyInput != "first\nsecond\nthird" || strings.ContainsRune(model.replyInput, '␍') {
		t.Fatalf("reply CRLF paste = %q", model.replyInput)
	}
	model.setReplyValue("")
	model.focus = focusCommand
	commandText := "printf first\r\nprintf second\rprintf third"
	model = updateModel(t, model, tea.PasteMsg{Content: commandText})
	wantCommand := "printf first\nprintf second\nprintf third"
	if model.commandInput != wantCommand || strings.ContainsRune(model.commandInput, '␍') {
		t.Fatalf("command CRLF paste = %q", model.commandInput)
	}

	model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if send == nil {
		// A safety warning may require the documented second confirmation, but
		// it must still be scoped to the already-normalized command bytes.
		if model.commandConfirm != wantCommand {
			t.Fatalf("normalized command was neither staged nor confirmed: %+v", model)
		}
		model, send = updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	}
	model = finishCommand(t, model, send)
	if len(client.events) == 0 || len(client.events[len(client.events)-1].ToolCalls) != 1 {
		t.Fatalf("command event = %+v", client.events)
	}
	got, _ := client.events[len(client.events)-1].ToolCalls[0].Input["command"].(string)
	if got != wantCommand || strings.ContainsRune(got, '␍') || strings.ContainsRune(got, '\r') {
		t.Fatalf("outbound command = %q", got)
	}
}

func TestDangerousCommandConfirmationIsScopedToCommandFocus(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{{
		Name:        "bash",
		InputSchema: []byte(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`),
	}}
	model := receiveAndAccept(t, New(client), assignment)
	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab})
	model = updateModel(t, model, tea.PasteMsg{Content: "rm -rf /tmp/human-test"})

	model, send := updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if send != nil || model.commandConfirm == "" {
		t.Fatalf("dangerous command was not armed for confirmation: %+v", model)
	}
	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyEsc})
	if model.focus != focusTasks || model.commandConfirm != "" {
		t.Fatalf("Esc retained a stale command confirmation: %+v", model)
	}
	model = updateModel(t, model, tea.KeyPressMsg{Text: "x", Code: 'x'})
	model, send = updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if send != nil || model.commandConfirm == "" || len(client.events) != 1 {
		t.Fatalf("stale Esc confirmation authorized the command: model=%+v events=%+v", model, client.events)
	}

	model = updateModel(t, model, tea.KeyPressMsg{Code: tea.KeyTab})
	if model.focus != focusTasks || model.commandConfirm != "" {
		t.Fatalf("Tab retained a stale command confirmation: %+v", model)
	}
	model = updateModel(t, model, tea.KeyPressMsg{Text: "x", Code: 'x'})
	model, send = updateModelWithCommand(t, model, tea.KeyPressMsg{Code: tea.KeyEnter})
	if send != nil || model.commandConfirm == "" || len(client.events) != 1 {
		t.Fatalf("stale Tab confirmation authorized the command: model=%+v events=%+v", model, client.events)
	}
}

func TestOnlyControlCQuitsTasksAndInbox(t *testing.T) {
	t.Parallel()
	model := New(newFakeClient())
	for _, active := range []bool{false, true} {
		candidate := model
		if active {
			assignment := testAssignment()
			candidate.active = &assignment
		}
		updated, command := candidate.Update(tea.KeyPressMsg{Text: "q", Code: 'q'})
		if command != nil {
			t.Fatalf("plain q produced a command with active=%t", active)
		}
		if _, ok := updated.(Model); !ok {
			t.Fatalf("plain q returned %T", updated)
		}
	}
}

func TestShortIdentifierPreservesUTF8(t *testing.T) {
	t.Parallel()
	identifier := "任务工作区甲乙丙丁戊己庚辛壬癸子丑寅卯"
	short := shortIdentifier(identifier)
	if !utf8.ValidString(short) {
		t.Fatalf("short identifier is invalid UTF-8: %q", short)
	}
	runes := []rune(identifier)
	want := "…" + string(runes[len(runes)-12:])
	if short != want {
		t.Fatalf("short identifier = %q, want %q", short, want)
	}
}
