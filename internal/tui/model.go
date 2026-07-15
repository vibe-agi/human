// Package tui contains the Bubble Tea worker interface.
package tui

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/safety"
	workmirror "github.com/vibe-agi/human/internal/mirror"
	"github.com/vibe-agi/human/internal/workerclient"
)

type Client interface {
	Messages() <-chan workerclient.Message
	SendEvent(context.Context, completion.Assignment, completion.Event) error
}

type ViewMode int

const (
	ViewQueue ViewMode = iota
	ViewDesk
)

type Model struct {
	client        Client
	mirrorManager MirrorManager
	mirrors       map[string]MirrorWorkspace
	mirrorStatus  map[string]string
	assignments   []completion.Assignment
	selected      int
	active        *completion.Assignment
	view          ViewMode
	composing     bool
	composeKind   composeKind
	input         string
	status        string
	delivery      deliveryReview
}

type Option func(*Model)

// WithMirrorRoot enables the caller/workspace-scoped scratch mirror. Chat
// assignments still bypass it even when this option is configured.
func WithMirrorRoot(root string) Option {
	return func(model *Model) {
		if strings.TrimSpace(root) != "" {
			model.mirrorManager = newFilesystemMirrorManager(root)
		}
	}
}

// WithMirrorManager supplies a mirror implementation. It is useful for
// embedding the TUI and for deterministic boundary tests.
func WithMirrorManager(manager MirrorManager) Option {
	return func(model *Model) { model.mirrorManager = manager }
}

type composeKind int

const (
	composeText composeKind = iota
	composeExec
)

type deliveryStage int

const (
	deliveryNone deliveryStage = iota
	deliveryReviewed
	deliveryPreviewed
	deliverySending
)

type deliveryReview struct {
	stage      deliveryStage
	sessionKey string
	namespace  string
	changes    []workmirror.Change
	calls      []completion.ToolCall
	eventID    string
}

type networkMessage workerclient.Message
type eventSent struct{ err error }
type deliveryEventSent struct {
	sessionKey string
	err        error
}

func New(client Client, options ...Option) Model {
	model := Model{
		client: client, view: ViewQueue, status: "connected · waiting for requests",
		mirrors: make(map[string]MirrorWorkspace), mirrorStatus: make(map[string]string),
	}
	for _, option := range options {
		option(&model)
	}
	return model
}

func (model Model) Init() tea.Cmd {
	return waitForNetwork(model.client)
}

func (model Model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch message := message.(type) {
	case networkMessage:
		if message.Err != nil {
			model.status = "connection error: " + message.Err.Error()
			return model, waitForNetwork(model.client)
		}
		if message.Assignment != nil {
			incoming := *message.Assignment
			commands := []tea.Cmd{waitForNetwork(model.client)}
			if model.mirrorManager != nil && mirrorEnabled(incoming) {
				commands = append(commands, prepareMirror(model.mirrorManager, incoming))
			}
			if model.active != nil && model.active.SessionKey() == incoming.SessionKey() {
				model.active = &incoming
				model.delivery = deliveryReview{}
				model.status = "reconnected · active request restored"
				return model, tea.Batch(commands...)
			}
			for index := range model.assignments {
				if model.assignments[index].SessionKey() == incoming.SessionKey() {
					model.assignments[index] = incoming
					model.status = fmt.Sprintf("%d request(s) queued", len(model.assignments))
					return model, tea.Batch(commands...)
				}
			}
			model.assignments = append(model.assignments, incoming)
			model.status = fmt.Sprintf("%d request(s) queued", len(model.assignments))
			return model, tea.Batch(commands...)
		}
		return model, waitForNetwork(model.client)
	case mirrorPrepared:
		if message.err != nil {
			model.mirrorStatus[message.namespace] = "mirror error: " + message.err.Error()
			if model.active != nil && model.active.SessionKey() == message.sessionKey {
				model.status = model.mirrorStatus[message.namespace]
			}
			return model, nil
		}
		model.mirrors[message.namespace] = message.workspace
		model.mirrorStatus[message.namespace] = reconcileSummary(message.report)
		if model.active != nil && model.active.SessionKey() == message.sessionKey {
			model.status = model.mirrorStatus[message.namespace]
		}
		return model, nil
	case mirrorReviewReady:
		if model.active == nil || model.active.SessionKey() != message.sessionKey {
			return model, nil
		}
		if message.err != nil {
			model.delivery = deliveryReview{}
			model.status = "review failed: " + message.err.Error()
			return model, nil
		}
		model.delivery = deliveryReview{
			stage: deliveryReviewed, sessionKey: message.sessionKey,
			namespace: message.namespace, changes: message.changes,
		}
		if len(message.changes) == 0 {
			model.status = "mirror has no unconfirmed changes"
		} else {
			model.status = fmt.Sprintf("reviewed %d change(s) · ctrl+p to preview", len(message.changes))
		}
		return model, nil
	case mirrorConfirmationReady:
		if model.active == nil || model.active.SessionKey() != message.sessionKey ||
			model.delivery.stage != deliveryPreviewed || model.delivery.sessionKey != message.sessionKey {
			return model, nil
		}
		if message.err != nil {
			model.delivery = deliveryReview{}
			model.status = "delivery not sent: " + message.err.Error()
			return model, nil
		}
		assignment := *model.active
		model.delivery.stage = deliverySending
		model.status = fmt.Sprintf("confirmed · sending %d file tool call(s)…", len(message.calls))
		return model, sendDeliveryEvent(model.client, assignment, completion.Event{
			ID: model.delivery.eventID, Type: completion.EventToolCalls, ToolCalls: message.calls,
		})
	case deliveryEventSent:
		if model.active == nil || model.active.SessionKey() != message.sessionKey ||
			model.delivery.stage != deliverySending {
			return model, nil
		}
		if message.err != nil {
			model.delivery.stage = deliveryPreviewed
			model.status = "delivery send failed; preview retained: " + message.err.Error()
			return model, nil
		}
		count := len(model.delivery.calls)
		model.active = nil
		model.delivery = deliveryReview{}
		model.view = ViewQueue
		model.status = fmt.Sprintf("confirmed · %d file tool call(s) sent", count)
		return model, nil
	case eventSent:
		if message.err != nil {
			model.status = "send failed: " + message.err.Error()
		}
		return model, nil
	case tea.KeyPressMsg:
		return model.updateKey(message)
	default:
		return model, nil
	}
}

func (model Model) updateKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	if model.composing {
		switch key.Keystroke() {
		case "esc":
			model.composing = false
			model.input = ""
		case "backspace":
			if len(model.input) > 0 {
				runes := []rune(model.input)
				model.input = string(runes[:len(runes)-1])
			}
		case "enter":
			model.input += "\n"
		case "ctrl+p":
			if model.composeKind == composeExec {
				return model, nil
			}
			return model.sendText(completion.EventProgress, false)
		case "ctrl+r":
			if model.composeKind == composeExec {
				return model, nil
			}
			return model.sendText(completion.EventClarification, true)
		case "ctrl+s":
			if model.composeKind == composeExec {
				return model.sendExec()
			}
			return model.sendText(completion.EventFinal, true)
		default:
			if key.Key().Text != "" {
				model.input += key.Key().Text
			}
		}
		return model, nil
	}

	switch key.Keystroke() {
	case "ctrl+c", "q":
		return model, tea.Quit
	case "tab":
		if model.view == ViewQueue && model.active != nil {
			model.view = ViewDesk
		} else {
			model.view = ViewQueue
		}
	case "up", "k":
		if model.view == ViewQueue && model.selected > 0 {
			model.selected--
		}
	case "down", "j":
		if model.view == ViewQueue && model.selected+1 < len(model.assignments) {
			model.selected++
		}
	case "a":
		if model.view == ViewQueue && len(model.assignments) > 0 {
			assignment := model.assignments[model.selected]
			model.active = &assignment
			model.delivery = deliveryReview{}
			model.assignments = append(model.assignments[:model.selected], model.assignments[model.selected+1:]...)
			if model.selected >= len(model.assignments) && model.selected > 0 {
				model.selected--
			}
			model.view = ViewDesk
			model.status = "accepted " + assignment.TaskID
			return model, sendEvent(model.client, assignment, completion.Event{Type: completion.EventAccepted})
		}
	case "r":
		if model.view == ViewQueue && len(model.assignments) > 0 {
			assignment := model.assignments[model.selected]
			model.assignments = append(model.assignments[:model.selected], model.assignments[model.selected+1:]...)
			return model, sendEvent(model.client, assignment, completion.Event{
				Type: completion.EventRejected, ErrorCode: "human_rejected", Error: "human rejected the request",
			})
		}
	case "c":
		if model.view == ViewDesk && model.active != nil {
			model.composing = true
			model.composeKind = composeText
			model.input = ""
		}
	case "x":
		if model.view == ViewDesk && model.active != nil {
			switch {
			case model.active.Adapter == nil || model.active.Adapter.Exec == nil:
				model.status = "this harness has no explicit exec adapter"
			case !model.active.ExecAllowed:
				model.status = "exec is disabled for this task"
			default:
				model.composing = true
				model.composeKind = composeExec
				model.input = ""
			}
		}
	case "R", "shift+r":
		return model.startMirrorReview()
	case "ctrl+p":
		return model.previewMirrorDelivery()
	case "enter":
		return model.confirmMirrorDelivery()
	case "esc":
		if model.delivery.stage == deliveryReviewed || model.delivery.stage == deliveryPreviewed {
			model.delivery = deliveryReview{}
			model.status = "delivery review canceled"
		}
	}
	return model, nil
}

func (model Model) startMirrorReview() (tea.Model, tea.Cmd) {
	if model.view != ViewDesk || model.active == nil {
		return model, nil
	}
	if !mirrorEnabled(*model.active) {
		model.delivery = deliveryReview{}
		if model.active.CapabilityTier == completion.TierChat || model.active.CapabilityTier == "" {
			model.status = "Chat tier has no workspace mirror"
		} else {
			model.status = "workspace delivery requires the exact human-shim adapter"
		}
		return model, nil
	}
	namespace := mirrorNamespace(model.active.CallerID, model.active.WorkspaceKey)
	workspace := model.mirrors[namespace]
	if workspace == nil {
		model.status = "mirror is still preparing; try review again"
		return model, nil
	}
	model.delivery = deliveryReview{}
	model.status = "reviewing mirror changes…"
	return model, reviewMirror(workspace, *model.active)
}

func (model Model) previewMirrorDelivery() (tea.Model, tea.Cmd) {
	if model.view != ViewDesk || model.active == nil ||
		model.delivery.stage != deliveryReviewed ||
		model.delivery.sessionKey != model.active.SessionKey() {
		return model, nil
	}
	if len(model.delivery.changes) == 0 {
		model.status = "nothing to deliver"
		return model, nil
	}
	calls, err := workmirror.BuildToolCalls(model.delivery.changes)
	if err == nil {
		err = validateMirrorCalls(model.active.Request, calls)
	}
	var eventID string
	if err == nil {
		eventID, err = canonical.NewOpaqueID("event_")
	}
	if err != nil {
		model.delivery = deliveryReview{}
		model.status = "preview failed: " + err.Error()
		return model, nil
	}
	model.delivery.stage = deliveryPreviewed
	model.delivery.calls = calls
	model.delivery.eventID = eventID
	model.status = "preview ready · enter to confirm, esc to cancel"
	return model, nil
}

func (model Model) confirmMirrorDelivery() (tea.Model, tea.Cmd) {
	if model.view != ViewDesk || model.active == nil ||
		model.delivery.stage != deliveryPreviewed ||
		model.delivery.sessionKey != model.active.SessionKey() {
		return model, nil
	}
	workspace := model.mirrors[model.delivery.namespace]
	if workspace == nil {
		model.delivery = deliveryReview{}
		model.status = "delivery not sent: mirror is unavailable"
		return model, nil
	}
	model.status = "checking mirror has not changed since preview…"
	return model, confirmMirror(
		workspace, *model.active, model.delivery.changes, model.delivery.calls,
	)
}

func (model Model) sendExec() (tea.Model, tea.Cmd) {
	if model.active == nil || strings.TrimSpace(model.input) == "" ||
		model.active.Adapter == nil || model.active.Adapter.Exec == nil || !model.active.ExecAllowed {
		return model, nil
	}
	callID, err := canonical.NewOpaqueID("tool_")
	if err != nil {
		model.status = "allocate tool-call id: " + err.Error()
		return model, nil
	}
	execTool := model.active.Adapter.Exec
	commandField := execTool.Args["command"]
	if commandField == "" {
		commandField = "command"
	}
	input := map[string]any{commandField: model.input}
	if execTool.CWDField != "" {
		input[execTool.CWDField] = "/workspace"
	}
	assignment := *model.active
	event := completion.Event{Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{{
		ID: callID, Name: execTool.Name, Input: input,
	}}}
	model.input = ""
	model.composing = false
	model.active = nil
	model.view = ViewQueue
	model.status = "command tool call sent"
	return model, sendEvent(model.client, assignment, event)
}

func (model Model) sendText(eventType completion.EventType, endResponse bool) (tea.Model, tea.Cmd) {
	if model.active == nil || strings.TrimSpace(model.input) == "" {
		return model, nil
	}
	assignment := *model.active
	event := completion.Event{Type: eventType, Text: model.input}
	model.input = ""
	if endResponse {
		model.composing = false
		model.active = nil
		model.view = ViewQueue
		model.status = "response sent"
	}
	return model, sendEvent(model.client, assignment, event)
}

func (model Model) View() tea.View {
	var builder strings.Builder
	builder.WriteString("Human Agent · ")
	if model.view == ViewQueue {
		builder.WriteString("Queue\n\n")
		if len(model.assignments) == 0 {
			builder.WriteString("  No queued requests\n")
		}
		for index, assignment := range model.assignments {
			cursor := " "
			if index == model.selected {
				cursor = ">"
			}
			fmt.Fprintf(&builder, "%s %s · %s · %s\n", cursor, assignment.CallerID, assignment.TaskID, requestPreview(assignment.Request))
		}
		builder.WriteString("\n[a] accept  [r] reject  [q] quit\n")
	} else {
		builder.WriteString("Desk\n\n")
		if model.active != nil {
			fmt.Fprintf(&builder, "Task: %s\n", model.active.TaskID)
			if mirrorEnabled(*model.active) {
				namespace := mirrorNamespace(model.active.CallerID, model.active.WorkspaceKey)
				fmt.Fprintf(&builder, "Workspace: %s\n", model.active.WorkspaceKey)
				if workspace := model.mirrors[namespace]; workspace != nil {
					fmt.Fprintf(&builder, "Mirror: %s\n", workspace.Dir())
				}
				if status := model.mirrorStatus[namespace]; status != "" {
					fmt.Fprintf(&builder, "Reconcile: %s\n", status)
				}
			} else {
				builder.WriteString("Workspace: disabled for this capability tier/adapter\n")
			}
			fmt.Fprintf(&builder, "\n%s\n", renderRequest(model.active.Request))
			builder.WriteString(renderDeliveryReview(model.delivery))
		}
		if model.composing {
			if model.composeKind == composeExec {
				decision := safety.CheckCommand(model.input, model.active != nil && model.active.ExecAllowed)
				warning := ""
				if decision.Severity != safety.SeverityAllow {
					warning = "\nSafety: " + strings.Join(decision.Reasons, "; ")
				}
				fmt.Fprintf(&builder, "\nRemote command:\n%s█%s\n\n[ctrl+s] preview/send tool call  [esc] cancel\n", model.input, warning)
			} else {
				fmt.Fprintf(&builder, "\nReply:\n%s█\n\n[ctrl+p] progress  [ctrl+r] clarification  [ctrl+s] final  [esc] cancel\n", model.input)
			}
		} else {
			builder.WriteString("\n[c] compose  [x] remote command  [R] review files  [ctrl+p] preview delivery  [enter] confirm  [tab] queue  [q] quit\n")
		}
	}
	fmt.Fprintf(&builder, "\n%s", model.status)
	view := tea.NewView(builder.String())
	view.AltScreen = true
	return view
}

func renderDeliveryReview(review deliveryReview) string {
	if review.stage == deliveryNone {
		return ""
	}
	var builder strings.Builder
	builder.WriteString("\nDelivery review (not sent):\n")
	if len(review.changes) == 0 {
		builder.WriteString("  no changes\n")
		return builder.String()
	}
	for _, change := range review.changes {
		marker := map[workmirror.ChangeKind]string{
			workmirror.ChangeWrite: "+", workmirror.ChangeEdit: "~", workmirror.ChangeDelete: "-",
		}[change.Kind]
		fmt.Fprintf(&builder, "  %s %s", marker, change.Path)
		if len(change.Reasons) > 0 {
			fmt.Fprintf(&builder, " [%s]", strings.Join(change.Reasons, "; "))
		}
		builder.WriteByte('\n')
		if review.stage == deliveryPreviewed || review.stage == deliverySending {
			builder.WriteString(renderChangePreview(change))
		}
	}
	if review.stage == deliveryReviewed {
		builder.WriteString("  [ctrl+p] build exact tool-call preview\n")
	} else if review.stage == deliveryPreviewed {
		builder.WriteString("  [enter] confirm and send exactly this preview  [esc] cancel\n")
	} else {
		builder.WriteString("  sending confirmed tool calls…\n")
	}
	return builder.String()
}

func renderChangePreview(change workmirror.Change) string {
	const contentLimit = 2048
	var builder strings.Builder
	fmt.Fprintf(&builder, "    expected caller hash: %s\n", change.ExpectedSHA)
	switch change.Kind {
	case workmirror.ChangeWrite:
		fmt.Fprintf(&builder, "    new content:\n%s\n", indentPreview(change.NewContent, contentLimit))
	case workmirror.ChangeEdit:
		fmt.Fprintf(&builder, "    before:\n%s\n", indentPreview(change.OldContent, contentLimit))
		fmt.Fprintf(&builder, "    after:\n%s\n", indentPreview(change.NewContent, contentLimit))
	case workmirror.ChangeDelete:
		builder.WriteString("    delete caller file if its hash still matches\n")
	}
	return builder.String()
}

func indentPreview(content []byte, limit int) string {
	if len(content) == 0 {
		return "      (empty)"
	}
	if !isText(content) {
		return fmt.Sprintf("      (binary, %d bytes)", len(content))
	}
	total := len(content)
	truncated := len(content) > limit
	if truncated {
		content = content[:limit]
	}
	text := strings.ReplaceAll(string(content), "\n", "\n      ")
	text = "      " + text
	if truncated {
		text += fmt.Sprintf("\n      … preview truncated; exact payload is %d bytes", total)
	}
	return text
}

func isText(content []byte) bool {
	if !utf8.Valid(content) {
		return false
	}
	for _, value := range content {
		if value == 0 {
			return false
		}
	}
	return true
}

func waitForNetwork(client Client) tea.Cmd {
	return func() tea.Msg {
		message, open := <-client.Messages()
		if !open {
			return networkMessage{Err: context.Canceled}
		}
		return networkMessage(message)
	}
}

func sendEvent(client Client, assignment completion.Assignment, event completion.Event) tea.Cmd {
	if event.ID == "" {
		id, err := canonical.NewOpaqueID("event_")
		if err != nil {
			return func() tea.Msg { return eventSent{err: err} }
		}
		event.ID = id
	}
	return func() tea.Msg {
		return eventSent{err: client.SendEvent(context.Background(), assignment, event)}
	}
}

func sendDeliveryEvent(client Client, assignment completion.Assignment, event completion.Event) tea.Cmd {
	if event.ID == "" {
		id, err := canonical.NewOpaqueID("event_")
		if err != nil {
			return func() tea.Msg { return deliveryEventSent{sessionKey: assignment.SessionKey(), err: err} }
		}
		event.ID = id
	}
	return func() tea.Msg {
		return deliveryEventSent{
			sessionKey: assignment.SessionKey(),
			err:        client.SendEvent(context.Background(), assignment, event),
		}
	}
}

func requestPreview(request canonical.Request) string {
	for index := len(request.Messages) - 1; index >= 0; index-- {
		for _, block := range request.Messages[index].Blocks {
			if block.Type == canonical.BlockText {
				text := strings.ReplaceAll(block.Text, "\n", " ")
				if len([]rune(text)) > 60 {
					text = string([]rune(text)[:60]) + "…"
				}
				return text
			}
		}
	}
	return "(tool context)"
}

func renderRequest(request canonical.Request) string {
	var builder strings.Builder
	if request.System != "" {
		builder.WriteString("[system folded]\n")
	}
	for _, message := range request.Messages {
		fmt.Fprintf(&builder, "\n%s:\n", message.Role)
		for _, block := range message.Blocks {
			switch block.Type {
			case canonical.BlockText:
				builder.WriteString(block.Text + "\n")
			case canonical.BlockToolUse:
				fmt.Fprintf(&builder, "[tool use %s %s]\n", block.ToolName, block.ToolCallID)
			case canonical.BlockToolResult:
				fmt.Fprintf(&builder, "[tool result %s] %v\n", block.ToolCallID, block.Output)
			case canonical.BlockImage:
				builder.WriteString("[image]\n")
			}
		}
	}
	return builder.String()
}
