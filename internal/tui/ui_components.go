package tui

import (
	"fmt"
	"image/color"
	"strings"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/vibe-agi/human/internal/completion/safety"
)

const wideWorkspaceWidth = 112

type workspaceUI struct {
	reply       textarea.Model
	command     textarea.Model
	chat        viewport.Model
	spinner     spinner.Model
	chatWidth   int
	chatHeight  int
	chatDirty   bool
	chatState   chatPresentationState
	dark        bool
	chatFollow  bool
	chatTop     bool
	initialized bool
}

type chatPresentationState struct {
	contextSession string
	messageCount   int
	toolCount      int
	detail         bool
	composing      bool
	composeInput   string
	deliveryStage  deliveryStage
	deliveryKey    string
	deliveryEvents int
}

type workspaceTheme struct {
	accent  color.Color
	text    color.Color
	muted   color.Color
	subtle  color.Color
	success color.Color
	warning color.Color
	danger  color.Color
	tool    color.Color
}

type workspaceLayout struct {
	width, height                  int
	header, footer                 int
	chat, tasks, command, reply    int
	wide                           bool
	leftWidth, railWidth, bodyRows int
}

func newWorkspaceUI() workspaceUI {
	ui := workspaceUI{dark: true, chatFollow: true, chatDirty: true, initialized: true}
	ui.reply = newTextarea("Message the client Agent…", 3)
	ui.command = newTextarea("Command for the client Agent…", 2)
	ui.chat = viewport.New()
	ui.chat.SoftWrap = true
	ui.chat.FillHeight = true
	// Leave the mouse to the terminal so operators can drag-select and copy
	// transcript text. Chat history remains available through PgUp/PgDn.
	ui.chat.MouseWheelEnabled = false
	ui.spinner = spinner.New(spinner.WithSpinner(spinner.MiniDot))
	ui.applyTheme()
	return ui
}

func newTextarea(placeholder string, maxHeight int) textarea.Model {
	editor := textarea.New()
	editor.Prompt = "❯ "
	editor.Placeholder = placeholder
	editor.ShowLineNumbers = false
	editor.DynamicHeight = true
	editor.MinHeight = 1
	editor.MaxHeight = maxHeight
	editor.MaxContentHeight = 256
	editor.CharLimit = 32 * 1024
	// Enter belongs to the parent model: it sends a stream segment. Newlines
	// remain available in every terminal via Ctrl+J and, with keyboard
	// disambiguation, via Shift+Enter.
	editor.KeyMap.InsertNewline.SetKeys("ctrl+j", "shift+enter")
	editor.SetPromptFunc(2, func(info textarea.PromptInfo) string {
		if info.LineNumber == 0 {
			return "❯ "
		}
		return "  "
	})
	editor.SetVirtualCursor(false)
	editor.SetWidth(80)
	return editor
}

func (ui *workspaceUI) handleSystemMessage(message tea.Msg, animate bool) (tea.Cmd, bool) {
	switch message := message.(type) {
	case tea.BackgroundColorMsg:
		ui.dark = message.IsDark()
		ui.applyTheme()
		ui.chatDirty = true
		return nil, true
	case spinner.TickMsg:
		if !animate {
			return nil, true
		}
		var command tea.Cmd
		ui.spinner, command = ui.spinner.Update(message)
		return command, true
	default:
		return nil, false
	}
}

func (ui *workspaceUI) applyTheme() {
	theme := ui.theme()
	configure := func(editor *textarea.Model) {
		styles := textarea.DefaultStyles(ui.dark)
		styles.Focused.Base = lipgloss.NewStyle()
		styles.Focused.Text = lipgloss.NewStyle().Foreground(theme.text)
		styles.Focused.Prompt = lipgloss.NewStyle().Foreground(theme.accent).Bold(true)
		styles.Focused.Placeholder = lipgloss.NewStyle().Foreground(theme.muted)
		styles.Focused.CursorLine = lipgloss.NewStyle()
		styles.Blurred.Base = lipgloss.NewStyle()
		styles.Blurred.Text = lipgloss.NewStyle().Foreground(theme.muted)
		styles.Blurred.Prompt = lipgloss.NewStyle().Foreground(theme.subtle)
		styles.Blurred.Placeholder = lipgloss.NewStyle().Foreground(theme.subtle)
		styles.Blurred.CursorLine = lipgloss.NewStyle()
		styles.Cursor.Color = theme.accent
		styles.Cursor.Shape = tea.CursorBar
		styles.Cursor.Blink = true
		editor.SetStyles(styles)
	}
	configure(&ui.reply)
	configure(&ui.command)
	ui.spinner.Style = lipgloss.NewStyle().Foreground(theme.warning)
}

func (ui workspaceUI) theme() workspaceTheme {
	pick := lipgloss.LightDark(ui.dark)
	return workspaceTheme{
		accent:  pick(lipgloss.Color("#6D28D9"), lipgloss.Color("#C4A7FF")),
		text:    pick(lipgloss.Color("#202124"), lipgloss.Color("#E7E7E7")),
		muted:   pick(lipgloss.Color("#667085"), lipgloss.Color("#929292")),
		subtle:  pick(lipgloss.Color("#D0D5DD"), lipgloss.Color("#454545")),
		success: pick(lipgloss.Color("#067647"), lipgloss.Color("#5FD7A0")),
		warning: pick(lipgloss.Color("#B54708"), lipgloss.Color("#F0B35A")),
		danger:  pick(lipgloss.Color("#B42318"), lipgloss.Color("#FF7B72")),
		tool:    pick(lipgloss.Color("#175CD3"), lipgloss.Color("#79C0FF")),
	}
}

func (model *Model) syncUI() {
	if !model.ui.initialized {
		model.ui = newWorkspaceUI()
	}
	if model.ui.reply.Value() != model.replyInput {
		model.ui.reply.SetValue(model.replyInput)
		model.ui.reply.MoveToEnd()
	}
	if model.ui.command.Value() != model.commandInput {
		model.ui.command.SetValue(model.commandInput)
		model.ui.command.MoveToEnd()
	}
	model.ui.setFocus(model.focus, model.active != nil && !model.composing)
}

func (ui *workspaceUI) setFocus(focus inputFocus, enabled bool) {
	ui.reply.Blur()
	ui.command.Blur()
	if !enabled {
		return
	}
	switch focus {
	case focusReply:
		_ = ui.reply.Focus()
	case focusCommand:
		_ = ui.command.Focus()
	}
}

func (model *Model) setReplyValue(value string) {
	model.replyInput = value
	model.ui.reply.SetValue(value)
	model.ui.reply.MoveToEnd()
}

func (model *Model) setCommandValue(value string) {
	model.commandInput = value
	model.ui.command.SetValue(value)
	model.ui.command.MoveToEnd()
}

func (model *Model) updateReplyEditor(message tea.Msg) tea.Cmd {
	message = safeEditorMessage(message)
	var command tea.Cmd
	model.ui.reply, command = model.ui.reply.Update(message)
	model.replyInput = model.ui.reply.Value()
	return command
}

func (model *Model) updateCommandEditor(message tea.Msg) tea.Cmd {
	message = safeEditorMessage(message)
	var command tea.Cmd
	model.ui.command, command = model.ui.command.Update(message)
	model.commandInput = model.ui.command.Value()
	return command
}

func safeEditorMessage(message tea.Msg) tea.Msg {
	switch message := message.(type) {
	case tea.KeyPressMsg:
		message.Text = terminalSafe(message.Text)
		return message
	case tea.PasteMsg:
		message.Content = terminalSafe(normalizeInputNewlines(message.Content))
		return message
	default:
		return message
	}
}

func (model *Model) resizeUI() {
	layout := modernWorkspaceLayout(model.width, model.height, model.focus, model.active != nil)
	replyWidth := layout.width
	commandWidth := layout.width
	if layout.wide {
		replyWidth = layout.leftWidth
		commandWidth = layout.railWidth
	}
	replyCapacity := max(1, layout.reply-1)
	commandCapacity := max(1, layout.command-1)
	if safetyCommandSummary(model.commandInput) != "" && commandCapacity > 1 {
		commandCapacity--
	}
	model.ui.reply.MaxHeight = replyCapacity
	model.ui.reply.SetWidth(max(8, replyWidth-2))
	model.ui.command.MaxHeight = commandCapacity
	model.ui.command.SetWidth(max(8, commandWidth-2))
	chatWidth := layout.width
	if layout.wide {
		chatWidth = layout.leftWidth
	}
	model.prepareChatViewport(chatWidth, max(1, layout.chat-1))
}

func (model *Model) invalidateChat() {
	model.ui.chatDirty = true
}

func modernWorkspaceLayout(width, height int, focus inputFocus, active bool) workspaceLayout {
	if width <= 0 {
		width = 80
	}
	if height <= 0 {
		height = 24
	}
	layout := workspaceLayout{width: width, height: height, header: 1, footer: 2}
	if width >= wideWorkspaceWidth && height >= 24 {
		layout.wide = true
		layout.railWidth = width / 3
		if layout.railWidth < 34 {
			layout.railWidth = 34
		}
		if layout.railWidth > 44 {
			layout.railWidth = 44
		}
		layout.leftWidth = width - layout.railWidth - 1
		layout.bodyRows = height - layout.header - layout.footer
		layout.reply = 5
		if !active {
			layout.reply = 1
		}
		layout.chat = layout.bodyRows - layout.reply
		layout.command = max(4, min(7, layout.bodyRows/4))
		layout.tasks = layout.bodyRows - layout.command
		return layout
	}

	layout.reply, layout.tasks, layout.command = 5, 2, 2
	if !active {
		layout.reply = 1
	}
	switch focus {
	case focusTasks:
		layout.tasks = 5
	case focusCommand:
		layout.command = 4
	}
	if height < 20 {
		layout.footer = 1
		if active {
			layout.reply = 3
		} else {
			layout.reply = 1
		}
		layout.tasks = 2
		layout.command = 2
	}
	layout.chat = height - layout.header - layout.footer - layout.reply - layout.tasks - layout.command
	if layout.chat < 3 {
		layout.chat = 3
	}
	return layout
}

func (model *Model) prepareChatViewport(width, height int) {
	width = max(1, width)
	height = max(1, height)
	state := model.chatPresentationState()
	geometryChanged := model.ui.chatWidth != width || model.ui.chatHeight != height
	if !geometryChanged && !model.ui.chatDirty && model.ui.chatState == state {
		return
	}

	assignment := model.contextAssignment()
	content := "No queued requests.\n\nWaiting for OpenCode, Claude Code, or Codex to call the Human Expert model…"
	styleRoles := false
	if model.delivery.stage == deliveryPreviewed || model.delivery.stage == deliveryConfirming ||
		model.delivery.stage == deliveryConfirmed || model.delivery.stage == deliverySending {
		content = strings.TrimSpace(renderDeliveryReview(model.delivery))
	} else if assignment != nil {
		if model.detailMode || model.composing {
			content = strings.Join(model.contextSections(*assignment), "\n\n")
		} else {
			content = renderReadableChat(assignment.Request)
			if directory := model.mirrorDirectory(*assignment); directory != "" {
				content += "\n\nLOCAL WORKSPACE\n" + directory +
					"\nEdit this copy; changes are reviewed and sent to the client Agent."
			}
			styleRoles = true
		}
	}
	if styleRoles {
		content = model.styleTranscript(content)
	} else {
		// Detail, tool-composer, and file-review views contain caller-controlled
		// multiline fields. Render them safely, but never infer role styling from
		// their text: a standalone "YOU" must not become a HUMAN header.
		content = terminalSafe(content)
	}
	if missing := height - lipgloss.Height(content); missing > 0 {
		content = strings.Repeat("\n", missing) + content
	}
	wasAtBottom := model.ui.chat.AtBottom()
	model.ui.chat.SetWidth(width)
	model.ui.chat.SetHeight(height)
	model.ui.chat.SetContent(content)
	if model.ui.chatTop {
		model.ui.chat.GotoTop()
		model.ui.chatTop = false
	} else if model.ui.chatFollow || wasAtBottom {
		model.ui.chat.GotoBottom()
	}
	model.ui.chatWidth = width
	model.ui.chatHeight = height
	model.ui.chatState = state
	model.ui.chatDirty = false
}

func (model Model) chatPresentationState() chatPresentationState {
	state := chatPresentationState{
		detail:         model.detailMode,
		composing:      model.composing,
		composeInput:   model.input,
		deliveryStage:  model.delivery.stage,
		deliveryKey:    model.delivery.sessionKey,
		deliveryEvents: len(model.delivery.changes) + len(model.delivery.calls),
	}
	if assignment := model.contextAssignment(); assignment != nil {
		state.contextSession = assignment.SessionKey()
		state.messageCount = len(assignment.Request.Messages)
		state.toolCount = len(assignment.Request.Tools)
	}
	return state
}

func (model Model) styleTranscript(content string) string {
	theme := model.ui.theme()
	client := lipgloss.NewStyle().Foreground(theme.text).Bold(true)
	human := lipgloss.NewStyle().Foreground(theme.accent).Bold(true)
	tool := lipgloss.NewStyle().Foreground(theme.tool).Bold(true)
	muted := lipgloss.NewStyle().Foreground(theme.muted)
	lines := strings.Split(terminalSafe(content), "\n")
	for index, line := range lines {
		switch strings.TrimSpace(line) {
		case "CLIENT":
			lines[index] = client.Render("CLIENT")
		case "YOU":
			lines[index] = human.Render("HUMAN")
		case "TOOL":
			lines[index] = tool.Render("TOOL")
		case "LOCAL WORKSPACE":
			lines[index] = muted.Render("LOCAL WORKSPACE")
		default:
			if strings.HasPrefix(strings.TrimSpace(line), "SYSTEM") {
				lines[index] = muted.Render(line)
			}
		}
	}
	return strings.Join(lines, "\n")
}

func (model Model) renderModernWorkspace() string {
	layout := modernWorkspaceLayout(model.width, model.height, model.focus, model.active != nil)
	theme := model.ui.theme()
	if layout.width < 50 || layout.height < 16 {
		message := lipgloss.NewStyle().Foreground(theme.warning).Bold(true).Render("Terminal too small")
		detail := lipgloss.NewStyle().Foreground(theme.muted).Render("Resize to at least 50×16; 80×24 is recommended.")
		return joinExactRows([]string{
			padStyled(lipgloss.NewStyle().Foreground(theme.accent).Bold(true).Render("human"), layout.width),
			"",
			padStyled(message, layout.width),
			padStyled(detail, layout.width),
		}, layout.height, layout.width)
	}
	header := model.renderModernHeader(layout.width, theme)
	footer := model.renderModernFooter(layout.width, layout.footer, theme)

	if layout.wide {
		chatName, chatMeta := model.modernChatTitle()
		chat := joinExactRows([]string{
			model.modernTitle(chatName, chatMeta, false, layout.leftWidth, theme),
			model.ui.chat.View(),
		}, layout.chat, layout.leftWidth)
		reply := model.renderModernReply(layout.leftWidth, layout.reply, theme)
		left := joinExactRows([]string{chat, reply}, layout.bodyRows, layout.leftWidth)
		tasks := model.renderModernTasks(layout.railWidth, layout.tasks, theme)
		command := model.renderModernCommand(layout.railWidth, layout.command, theme)
		right := joinExactRows([]string{tasks, command}, layout.bodyRows, layout.railWidth)
		body := lipgloss.JoinHorizontal(lipgloss.Top, left, " ", right)
		return joinExactRows([]string{header, body, footer}, layout.height, layout.width)
	}

	chatName, chatMeta := model.modernChatTitle()
	chat := joinExactRows([]string{
		model.modernTitle(chatName, chatMeta, false, layout.width, theme),
		model.ui.chat.View(),
	}, layout.chat, layout.width)
	tasks := model.renderModernTasks(layout.width, layout.tasks, theme)
	command := model.renderModernCommand(layout.width, layout.command, theme)
	reply := model.renderModernReply(layout.width, layout.reply, theme)
	return joinExactRows([]string{header, chat, tasks, command, reply, footer}, layout.height, layout.width)
}

func (model Model) modernChatTitle() (string, string) {
	if model.delivery.stage == deliveryPreviewed {
		return "Files", "REVIEW · Enter confirm · Esc cancel"
	}
	if model.delivery.stage == deliveryConfirming {
		return "Files", "CHECKING EXACT DELIVERY"
	}
	if model.delivery.stage == deliveryConfirmed {
		return "Files", "CONFIRMED · Enter retry exact event"
	}
	if model.delivery.stage == deliverySending {
		return "Files", "SENDING TO CLIENT AGENT"
	}
	return "Chat", model.chatStateLabel()
}

func (model Model) chatStateLabel() string {
	if model.active != nil {
		return "HUMAN TURN"
	}
	if len(model.continueIDs) > 0 || model.continueHandoff {
		return "WAITING FOR AGENT"
	}
	if len(model.assignments) > 0 {
		return fmt.Sprintf("INBOX %d", len(model.assignments))
	}
	return "IDLE"
}

func (model Model) renderModernHeader(width int, theme workspaceTheme) string {
	brand := lipgloss.NewStyle().Bold(true).Foreground(theme.accent).Render("human")
	connection := lipgloss.NewStyle().Foreground(theme.success).Render("● connected")
	switch model.connection {
	case connectionReconnecting:
		connection = lipgloss.NewStyle().Foreground(theme.warning).Render(model.ui.spinner.View() + " reconnecting")
	case connectionClosed:
		connection = lipgloss.NewStyle().Foreground(theme.danger).Render("○ disconnected")
	}
	stateStyle := lipgloss.NewStyle().Foreground(theme.muted)
	if model.active != nil {
		stateStyle = lipgloss.NewStyle().Foreground(theme.warning).Bold(true)
	}
	state := model.chatStateLabel()
	if model.connection == connectionConnected && model.responseCommitInFlight() {
		state = model.ui.spinner.View() + " " + state
	}
	parts := []string{brand, connection, stateStyle.Render(state)}
	if assignment := model.contextAssignment(); assignment != nil {
		parts = append(parts, lipgloss.NewStyle().Foreground(theme.muted).Render(
			shortIdentifier(terminalSafe(assignment.TaskID)),
		))
	}
	return padStyled(strings.Join(parts, "  "), width)
}

func shortIdentifier(value string) string {
	runes := []rune(value)
	if len(runes) <= 14 {
		return value
	}
	return "…" + string(runes[len(runes)-12:])
}

func (model Model) modernTitle(name, meta string, focused bool, width int, theme workspaceTheme) string {
	name = terminalSafe(name)
	meta = terminalSafe(meta)
	marker := lipgloss.NewStyle().Foreground(theme.subtle).Render("·")
	labelStyle := lipgloss.NewStyle().Foreground(theme.muted).Bold(true)
	if focused {
		marker = lipgloss.NewStyle().Foreground(theme.accent).Bold(true).Render("▸")
		labelStyle = lipgloss.NewStyle().Foreground(theme.accent).Bold(true)
	}
	left := marker + " " + labelStyle.Render(name)
	if meta != "" {
		left += lipgloss.NewStyle().Foreground(theme.muted).Render(" · " + meta)
	}
	remaining := width - ansi.StringWidth(left) - 1
	if remaining > 0 {
		left += " " + lipgloss.NewStyle().Foreground(theme.subtle).Render(strings.Repeat("─", remaining))
	}
	return padStyled(left, width)
}

func (model Model) renderModernReply(width, height int, theme workspaceTheme) string {
	if model.active == nil && !model.composing {
		meta := "available after accepting a request"
		switch {
		case len(model.assignments) > 0:
			meta = "accept the selected Inbox request to reply"
		case len(model.continueIDs) > 0 || model.continueHandoff:
			meta = "Agent has the turn"
		}
		title := model.modernTitle("Reply", meta, false, width, theme)
		return joinExactRows([]string{title}, height, width)
	}

	meta := ""
	if model.composing {
		meta = "advanced tool calls"
	}
	title := model.modernTitle("Reply", meta, model.focus == focusReply && model.active != nil && !model.composing, width, theme)
	bodyHeight := max(1, height-1)
	if model.composing {
		value := model.input
		if value == "" {
			value = "<tool-name> <JSON object> · one call per line"
		}
		body := fitPlainBlock(value, bodyHeight, width-2)
		body = lipgloss.NewStyle().Foreground(theme.text).PaddingLeft(2).Render(body)
		return joinExactRows([]string{title, body}, height, width)
	}
	body := model.ui.reply.View()
	return joinExactRows([]string{title, body}, height, width)
}

func (model Model) renderModernTasks(width, height int, theme workspaceTheme) string {
	meta := "Agent plan"
	if model.active == nil && len(model.assignments) > 0 {
		meta = fmt.Sprintf("Inbox %d/%d · Agent plan", model.selected+1, len(model.assignments))
	}
	if model.taskDirty {
		meta = "unsynced Agent plan"
	}
	title := model.modernTitle("Tasks", meta, model.focus == focusTasks, width, theme)
	rows := model.renderAgentTaskRows(width, max(1, height-1))
	for index, row := range rows {
		trimmed := strings.TrimSpace(row)
		switch {
		case strings.Contains(trimmed, "✓"):
			rows[index] = lipgloss.NewStyle().Foreground(theme.success).Render(row)
		case strings.Contains(trimmed, "◐"):
			rows[index] = lipgloss.NewStyle().Foreground(theme.warning).Render(row)
		case strings.Contains(strings.ToLower(trimmed), "disabled"), strings.Contains(strings.ToLower(trimmed), "conflict"):
			rows[index] = lipgloss.NewStyle().Foreground(theme.danger).Render(row)
		default:
			rows[index] = lipgloss.NewStyle().Foreground(theme.text).Render(row)
		}
	}
	return joinExactRows([]string{title, strings.Join(rows, "\n")}, height, width)
}

func (model Model) renderModernCommand(width, height int, theme workspaceTheme) string {
	target, reason := model.commandTarget()
	meta := "disabled"
	if reason == "" {
		meta = target.name + " · runs on client Agent"
	}
	title := model.modernTitle("Command", meta, model.focus == focusCommand, width, theme)
	bodyHeight := max(1, height-1)
	if reason != "" {
		body := lipgloss.NewStyle().Foreground(theme.muted).PaddingLeft(2).Render(fitPlainBlock(reason, bodyHeight, width-2))
		return joinExactRows([]string{title, body}, height, width)
	}
	decision := safetyCommandSummary(model.commandInput)
	editorHeight := bodyHeight
	if decision != "" && bodyHeight > 1 {
		editorHeight--
	}
	body := joinExactRows([]string{model.ui.command.View()}, editorHeight, width)
	if decision != "" && bodyHeight > 1 {
		body = joinExactRows([]string{body, lipgloss.NewStyle().Foreground(theme.warning).Render("⚠ " + decision)}, bodyHeight, width)
	}
	return joinExactRows([]string{title, body}, height, width)
}

func (model Model) workspaceCursor(layout workspaceLayout) *tea.Cursor {
	var cursor *tea.Cursor
	x, y := 0, 0
	switch {
	case model.active != nil && !model.composing && model.focus == focusReply && model.pending.kind != pendingAccept:
		cursor = model.ui.reply.Cursor()
		if layout.wide {
			y = layout.header + layout.chat + 1
		} else {
			y = layout.header + layout.chat + layout.tasks + layout.command + 1
		}
	case model.active != nil && !model.composing && model.focus == focusCommand:
		if _, reason := model.commandTarget(); reason != "" {
			return nil
		}
		cursor = model.ui.command.Cursor()
		if layout.wide {
			x = layout.leftWidth + 1
			y = layout.header + layout.tasks + 1
		} else {
			y = layout.header + layout.chat + layout.tasks + 1
		}
	}
	if cursor == nil {
		return nil
	}
	cursor.Position.X += x
	cursor.Position.Y += y
	return cursor
}

func safetyCommandSummary(command string) string {
	if strings.TrimSpace(command) == "" {
		return ""
	}
	decision := safety.CheckCommand(command, true)
	if len(decision.Reasons) == 0 {
		return ""
	}
	return strings.Join(decision.Reasons, "; ") + " · Enter again to send"
}

func (model Model) renderModernFooter(width, height int, theme workspaceTheme) string {
	visibleStatus := model.visibleStatus()
	statusStyle := lipgloss.NewStyle().Foreground(theme.muted)
	if strings.Contains(strings.ToLower(visibleStatus), "error") || strings.Contains(strings.ToLower(visibleStatus), "failed") ||
		strings.Contains(strings.ToLower(visibleStatus), "corrupt") {
		statusStyle = lipgloss.NewStyle().Foreground(theme.danger)
	}
	status := statusStyle.Render("Status  " + terminalSafe(visibleStatus))
	helpText := "Tab focus · PgUp/PgDn chat · Ctrl+C quit"
	if model.composing {
		helpText = "Ctrl+S send tools · Esc cancel"
	} else {
		switch model.focus {
		case focusReply:
			if model.active != nil {
				helpText = "Enter stream · Ctrl+J newline · Ctrl+R handoff · Ctrl+D end · Tab focus"
			}
		case focusCommand:
			if _, reason := model.commandTarget(); model.active != nil && reason == "" {
				helpText = "Enter send command · :pull path hydrate · Ctrl+J newline · Esc tasks · Tab focus"
			}
		case focusTasks:
			if model.active == nil && len(model.assignments) > 0 {
				helpText = "a accept · r reject · ↑/↓ choose · v details · Tab focus"
			} else if model.active != nil {
				_, taskReason := taskTargetForRequest(model.active.Request)
				if taskReason == "" && !model.taskConflict && !model.taskSyncWait {
					helpText = "Enter edit · n new · Space status · Ctrl+S sync · R files · t tools"
				} else {
					helpText = "v details · R review files · t advanced tools · Tab focus"
				}
			}
		}
	}
	help := lipgloss.NewStyle().Foreground(theme.muted).Render(helpText)
	if height <= 1 {
		return padStyled(status, width)
	}
	return joinExactRows([]string{padStyled(status, width), padStyled(help, width)}, height, width)
}

func fitPlainBlock(value string, height, width int) string {
	lines := boundedTailLines(wrapDisplayLines(value, max(1, width)), max(1, height), "… earlier input hidden")
	return joinExactRows(lines, height, width)
}

func joinExactRows(blocks []string, height, width int) string {
	lines := make([]string, 0, height)
	for _, block := range blocks {
		lines = append(lines, strings.Split(block, "\n")...)
	}
	if len(lines) > height {
		lines = lines[len(lines)-height:]
	}
	for len(lines) < height {
		lines = append(lines, "")
	}
	for index := range lines {
		lines[index] = padStyled(lines[index], width)
	}
	return strings.Join(lines, "\n")
}

func padStyled(value string, width int) string {
	if width <= 0 {
		return ""
	}
	value = ansi.Truncate(value, width, "…")
	if missing := width - ansi.StringWidth(value); missing > 0 {
		value += strings.Repeat(" ", missing)
	}
	return value
}
