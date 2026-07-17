package tui

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unicode"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/safety"
	workmirror "github.com/vibe-agi/human/internal/mirror"
	"github.com/vibe-agi/human/internal/workerclient"
)

type fakeClient struct {
	messages         chan workerclient.Message
	events           []completion.Event
	confirmed        []string
	sendErr          error
	confirmationErr  error
	confirmationErrs []error
}

func newFakeClient() *fakeClient {
	return &fakeClient{messages: make(chan workerclient.Message, 4)}
}

func (client *fakeClient) Messages() <-chan workerclient.Message { return client.messages }
func (client *fakeClient) SendEvent(_ context.Context, _ completion.Assignment, event completion.Event) error {
	client.events = append(client.events, event)
	return client.sendErr
}
func (client *fakeClient) ConfirmRejectedEvent(_ context.Context, eventID string) error {
	client.confirmed = append(client.confirmed, eventID)
	if len(client.confirmationErrs) > 0 {
		err := client.confirmationErrs[0]
		client.confirmationErrs = client.confirmationErrs[1:]
		return err
	}
	return client.confirmationErr
}

func testAssignment() completion.Assignment {
	return completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
		Request: canonical.Request{Messages: []canonical.Message{{
			Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "help"}},
		}}},
	}
}

func workspaceAssignment(request canonical.Request) completion.Assignment {
	profile := adapter.HumanShimProfile()
	if len(request.Tools) == 0 {
		for _, name := range []string{"human_write_file", "human_edit_file", "human_delete_file"} {
			request.Tools = append(request.Tools, canonical.Tool{Name: name, InputSchema: []byte(`{}`)})
		}
	}
	return completion.Assignment{
		CallerID: "caller-a", WorkspaceKey: "workspace-a", TaskID: "task-a",
		IdempotencyKey: "request-a", CapabilityTier: completion.TierWorkspace,
		HarnessID: adapter.HumanShimID, HarnessVersion: adapter.HumanShimVersion,
		Adapter: &profile, Request: request,
	}
}

func press(text string, code rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Text: text, Code: code}
}

func TestInboxAcceptStreamsThenFinishesExplicitly(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	assignment := testAssignment()
	updated, _ := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	updated, command := model.Update(press("a", 'a'))
	model = updated.(Model)
	if command == nil || model.active != nil || model.pending.kind != pendingAccept {
		t.Fatalf("accept was not staged durably: %+v", model)
	}
	updated, _ = model.Update(commandResult(t, command))
	model = updated.(Model)
	if model.active == nil {
		t.Fatalf("accepted model = %+v", model)
	}
	if len(client.events) != 1 || client.events[0].Type != completion.EventAccepted {
		t.Fatalf("events = %+v", client.events)
	}
	updated, _ = model.Update(press("o", 'o'))
	model = updated.(Model)
	updated, _ = model.Update(press("k", 'k'))
	model = updated.(Model)
	updated, command = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, command))
	model = updated.(Model)
	if model.active == nil || len(client.events) != 2 || client.events[1].Type != completion.EventProgress || client.events[1].Text != "ok\n\n" {
		t.Fatalf("progress model = %+v, events = %+v", model, client.events)
	}
	updated, command = model.Update(tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl})
	model = updated.(Model)
	if command == nil || model.active != nil {
		t.Fatalf("explicit finish was not staged: %+v", model)
	}
	updated, _ = model.Update(commandResult(t, command))
	model = updated.(Model)
	if len(client.events) != 3 || client.events[2].Type != completion.EventFinal {
		t.Fatalf("final events = %+v", client.events)
	}
}

func TestQueueReject(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	assignment := testAssignment()
	updated, _ := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	updated, command := model.Update(press("r", 'r'))
	model = updated.(Model)
	if len(model.assignments) != 1 || model.pending.kind != pendingReject {
		t.Fatalf("reject was not staged safely: %+v", model)
	}
	updated, _ = model.Update(commandResult(t, command))
	model = updated.(Model)
	if len(model.assignments) != 0 || len(client.events) != 1 || client.events[0].Type != completion.EventRejected {
		t.Fatalf("model = %+v, events = %+v", model, client.events)
	}
}

func TestReconnectNotificationClearsStaleConnectionError(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)

	updated, command := model.Update(networkMessage{Err: errors.New("socket closed")})
	model = updated.(Model)
	if command == nil || !strings.Contains(model.status, "connection error") {
		t.Fatalf("disconnect model = %+v", model)
	}

	updated, command = model.Update(networkMessage{ConnectionRestored: true})
	model = updated.(Model)
	if command == nil || model.status != "reconnected · ready" {
		t.Fatalf("reconnected model = %+v", model)
	}
}

func TestWorkerTokenConflictWaitsForIncumbentWithoutBecomingTerminal(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	updated, command := model.Update(networkMessage{Err: fmt.Errorf(
		"%w: another process owns this token", workerclient.ErrWorkerAlreadyConnected,
	)})
	model = updated.(Model)
	if command == nil || model.connection != connectionReconnecting || model.connectionTerminal != "" ||
		!strings.Contains(model.status, "waiting without displacing") {
		t.Fatalf("token conflict model = %+v", model)
	}
	updated, command = model.Update(networkMessage{ConnectionRestored: true})
	model = updated.(Model)
	if command == nil || model.connection != connectionConnected || model.status != "reconnected · ready" {
		t.Fatalf("token conflict did not recover: %+v", model)
	}
}

func TestWorkerOwnershipViolationRemainsActionableAfterNetworkCloses(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	updated, command := model.Update(networkMessage{Err: fmt.Errorf(
		"%w: wrong durable owner", workerclient.ErrWorkerOwnershipViolation,
	)})
	model = updated.(Model)
	if command == nil || model.connection != connectionClosed ||
		!strings.Contains(model.status, "outbox retained") {
		t.Fatalf("ownership violation model = %+v", model)
	}
	updated, command = model.Update(networkClosed{})
	model = updated.(Model)
	if command != nil || !strings.Contains(model.status, "verify the token and task owner") {
		t.Fatalf("terminal ownership diagnosis was lost: %+v", model)
	}
}

func TestClosedNetworkChannelStopsSubscription(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	close(client.messages)

	message := waitForNetwork(client)()
	if _, ok := message.(networkClosed); !ok {
		t.Fatalf("closed client channel produced %T, want networkClosed", message)
	}
	updated, command := model.Update(message)
	model = updated.(Model)
	if command != nil {
		t.Fatal("closed client channel scheduled another network subscription")
	}
	if model.connection != connectionClosed || model.status != "worker connection closed" {
		t.Fatalf("closed connection model = %+v", model)
	}
	view := ansi.Strip(model.View().Content)
	if !strings.Contains(view, "disconnected") || strings.Contains(view, "● connected") {
		t.Fatalf("closed connection header contradicts terminal state:\n%s", view)
	}
}

func TestRemoteCommandRequiresExplicitAdapterAndTaskOptIn(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	assignment := testAssignment()
	profile := adapter.HumanShimProfile()
	assignment.Adapter = &profile
	assignment.CapabilityTier = completion.TierRemoteTools
	assignment.Request.Tools = append(assignment.Request.Tools, canonical.Tool{
		Name: "human_exec", InputSchema: json.RawMessage(`{"type":"object"}`),
	})
	updated, _ := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	updated, command := model.Update(press("a", 'a'))
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, command))
	model = updated.(Model)
	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyTab})
	model = updated.(Model)
	updated, _ = model.Update(press("x", 'x'))
	model = updated.(Model)
	if model.commandInput != "" || model.status != "command disabled · remote exec is not authorized for this task" {
		t.Fatalf("disabled exec model = %+v", model)
	}

	model.active.ExecAllowed = true
	if _, reason := model.commandTarget(); reason != "" || model.focus != focusCommand {
		t.Fatalf("enabled exec model = %+v", model)
	}
	for _, key := range "printf ok" {
		updated, _ = model.Update(press(string(key), key))
		model = updated.(Model)
	}
	updated, command = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	_ = commandResult(t, command)
	if len(client.events) != 2 || client.events[1].Type != completion.EventToolCalls || len(client.events[1].ToolCalls) != 1 {
		t.Fatalf("events = %+v", client.events)
	}
	call := client.events[1].ToolCalls[0]
	if call.Name != "human_exec" || call.Input["command"] != "printf ok" || call.Input["cwd"] != "/workspace" {
		t.Fatalf("exec tool call = %+v", call)
	}
}

func TestCallerDeclaredToolCallFromDesk(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{{
		Name: "search", Description: "Search the caller workspace",
		InputSchema: []byte(`{"type":"object","properties":{"query":{"type":"string"}}}`),
	}}
	updated, _ := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	updated, accepted := model.Update(press("a", 'a'))
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, accepted))
	model = updated.(Model)

	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	model = updated.(Model)
	updated, _ = model.Update(press("v", 'v'))
	model = updated.(Model)
	view := ansi.Strip(model.View().Content)
	for _, want := range []string{
		"Declared tools (full descriptions",
		"description: Search the caller workspace",
		"t advanced tools",
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("desk view does not contain %q:\n%s", want, view)
		}
	}
	if strings.Contains(view, "input schema:") || strings.Contains(view, `"properties"`) {
		t.Fatalf("desk expanded tool schema:\n%s", view)
	}

	updated, _ = model.Update(press("t", 't'))
	model = updated.(Model)
	if !model.composing || len(model.toolCallIDs) != 1 ||
		!strings.HasPrefix(model.toolCallIDs[0], "tool_") {
		t.Fatalf("tool composer = %+v", model)
	}
	stableCallID := model.toolCallIDs[0]
	view = ansi.Strip(model.View().Content)
	for _, want := range []string{"Ctrl+S send tools", "<tool-name> <JSON object>"} {
		if !strings.Contains(view, want) {
			t.Fatalf("tool input view does not contain %q:\n%s", want, view)
		}
	}
	model.input = "sea"
	if selectedDeclaredTool(model.active.Request.Tools, currentToolCallLine(model.input)) != nil {
		t.Fatalf("partial tool name selected a schema: %q", model.input)
	}
	model.input = ""
	for _, key := range "search" {
		updated, _ = model.Update(press(string(key), key))
		model = updated.(Model)
	}
	view = collectContextPages(&model)
	for _, want := range []string{"Selected tool schema (full, paged): search", `{"type":"object","properties":{"query":{"type":"string"}}}`} {
		if !strings.Contains(view, want) {
			t.Fatalf("matched tool schema pages do not contain %q:\n%s", want, view)
		}
	}
	for _, key := range ` {"query":"status","limit":2}` {
		updated, _ = model.Update(press(string(key), key))
		model = updated.(Model)
	}
	updated, send := model.Update(tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	model = updated.(Model)
	if send == nil || model.active != nil || model.composing {
		t.Fatalf("sent tool model = %+v", model)
	}
	_ = commandResult(t, send)
	if len(client.events) != 2 || client.events[1].Type != completion.EventToolCalls ||
		len(client.events[1].ToolCalls) != 1 {
		t.Fatalf("events = %+v", client.events)
	}
	call := client.events[1].ToolCalls[0]
	if call.ID != stableCallID || call.Name != "search" || call.Input["query"] != "status" || call.Input["limit"] != float64(2) {
		t.Fatalf("tool call = %+v", call)
	}
}

func TestCallerDeclaredToolCallsSupportMultipleLinesAndStableIDs(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{
		{Name: "search", InputSchema: []byte(`{"marker":"search-schema"}`)},
		{Name: "lookup", InputSchema: []byte(`{"marker":"lookup-schema"}`)},
	}
	model := New(client)
	model.active = &assignment
	updated, _ := model.Update(press("t", 't'))
	model = updated.(Model)
	firstID := model.toolCallIDs[0]

	pasted := "search {\"query\":\"human\"}\nundeclared {}"
	updated, _ = model.Update(tea.KeyPressMsg{Text: pasted, Code: 'v'})
	model = updated.(Model)
	if len(model.toolCallIDs) != 2 || model.toolCallIDs[0] != firstID ||
		!strings.HasPrefix(model.toolCallIDs[1], "tool_") || model.toolCallIDs[1] == firstID {
		t.Fatalf("multiline call ids = %+v", model.toolCallIDs)
	}
	stableIDs := append([]string(nil), model.toolCallIDs...)
	updated, send := model.Update(tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	model = updated.(Model)
	if send != nil || !model.composing || model.active == nil ||
		!strings.Contains(model.status, `line 2: caller did not declare tool "undeclared"`) ||
		len(model.toolCallIDs) != 2 || model.toolCallIDs[0] != stableIDs[0] || model.toolCallIDs[1] != stableIDs[1] {
		t.Fatalf("failed multiline validation model = %+v", model)
	}

	model.input = "search {\"query\":\"human\"}\nlookup {\"path\":\"README.md\"}"
	preview := renderToolComposeContext(assignment.Request.Tools, currentToolCallLine(model.input))
	if !strings.Contains(preview, "Input schema for lookup:") || !strings.Contains(preview, "lookup-schema") ||
		strings.Contains(preview, "search-schema") {
		t.Fatalf("current-line schema preview:\n%s", preview)
	}
	view := collectContextPages(&model)
	if !strings.Contains(view, "Selected tool schema (full, paged): lookup") {
		t.Fatalf("selected schema is not retained in paged chat details:\n%s", view)
	}
	updated, send = model.Update(tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	model = updated.(Model)
	if send == nil || model.active != nil || model.composing {
		t.Fatalf("sent multiline model = %+v", model)
	}
	_ = commandResult(t, send)
	if len(client.events) != 1 || client.events[0].Type != completion.EventToolCalls ||
		len(client.events[0].ToolCalls) != 2 {
		t.Fatalf("events = %+v", client.events)
	}
	first, second := client.events[0].ToolCalls[0], client.events[0].ToolCalls[1]
	if first.ID != stableIDs[0] || first.Name != "search" || first.Input["query"] != "human" ||
		second.ID != stableIDs[1] || second.Name != "lookup" || second.Input["path"] != "README.md" {
		t.Fatalf("tool calls = %+v", client.events[0].ToolCalls)
	}
}

func TestCallerDeclaredToolCallLineIDsTrackEnterAndBackspace(t *testing.T) {
	t.Parallel()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{{Name: "search", InputSchema: []byte(`{}`)}}
	model := New(newFakeClient())
	model.active = &assignment
	updated, _ := model.Update(press("t", 't'))
	model = updated.(Model)
	firstID := model.toolCallIDs[0]
	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	if model.input != "\n" || len(model.toolCallIDs) != 2 || model.toolCallIDs[0] != firstID {
		t.Fatalf("ids after enter = %+v, input = %q", model.toolCallIDs, model.input)
	}
	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyBackspace})
	model = updated.(Model)
	if model.input != "" || len(model.toolCallIDs) != 1 || model.toolCallIDs[0] != firstID {
		t.Fatalf("ids after deleting line = %+v, input = %q", model.toolCallIDs, model.input)
	}
}

func TestDeclaredToolRenderingBoundsDescriptionAndSelectedSchema(t *testing.T) {
	t.Parallel()
	longDescription := "first line\n" + strings.Repeat("d", toolDescriptionPreviewRunes+40) + " description-tail"
	longSchema := []byte(`{"type":"object","description":"` + strings.Repeat("界", toolSchemaPreviewRunes+76) + ` schema-tail"}`)
	tools := []canonical.Tool{
		{Name: "large", Description: longDescription, InputSchema: longSchema},
		{Name: "other", Description: "Other tool", InputSchema: []byte(`{"marker":"other-schema"}`)},
	}
	assignment := testAssignment()
	assignment.Request.Tools = tools
	model := New(newFakeClient())
	model.active = &assignment
	model.detailMode = true

	listing := renderDeclaredTools(tools)
	if strings.Count(listing, "\n") != len(tools)+1 {
		t.Fatalf("tool list is not one line per tool:\n%s", listing)
	}
	if !strings.Contains(listing, "first line") || !strings.Contains(listing, "…") ||
		strings.Contains(listing, "description-tail") || strings.Contains(listing, "schema-tail") ||
		strings.Contains(listing, "other-schema") {
		t.Fatalf("declared-tool summary is not bounded:\n%s", listing)
	}
	view := collectContextPages(&model)
	if !strings.Contains(view, "first line") || !strings.Contains(view, "description-tail") ||
		strings.Contains(view, "schema-tail") || strings.Contains(view, "other-schema") {
		t.Fatalf("full details did not retain descriptions or exposed unselected schemas:\n%s", view)
	}

	model.composing = true
	model.input = "large "
	view = collectContextPages(&model)
	for _, want := range []string{"Selected tool schema (full, paged): large", "schema-tail", "description-tail"} {
		if !strings.Contains(view, want) {
			t.Fatalf("selected schema view does not contain %q:\n%s", want, view)
		}
	}
	if strings.Contains(view, "other-schema") {
		t.Fatalf("selected schema pages exposed an unselected schema:\n%s", view)
	}
	preview := renderToolComposeContext(tools, "large ")
	if !strings.Contains(preview, "Input schema for large:") || !strings.Contains(preview, "schema preview truncated") ||
		!strings.Contains(preview, "exact schema is ") || strings.Contains(preview, "schema-tail") ||
		strings.Contains(preview, "other-schema") {
		t.Fatalf("bounded selected-schema preview:\n%s", preview)
	}
}

func TestCallerDeclaredToolCallRejectsUndeclaredNameAndKeepsStableID(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	assignment.Request.Tools = []canonical.Tool{{Name: "search", InputSchema: []byte(`{"type":"object"}`)}}
	model := New(client)
	model.active = &assignment

	updated, _ := model.Update(press("t", 't'))
	model = updated.(Model)
	stableCallID := model.toolCallIDs[0]
	model.input = `delete_everything {}`
	updated, send := model.Update(tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	model = updated.(Model)
	if send != nil || !model.composing || len(model.toolCallIDs) != 1 ||
		model.toolCallIDs[0] != stableCallID || model.active == nil ||
		!strings.Contains(model.status, `caller did not declare tool "delete_everything"`) {
		t.Fatalf("rejected tool model = %+v", model)
	}

	model.input = `search {"query":"safe"}`
	updated, send = model.Update(tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	model = updated.(Model)
	if send == nil {
		t.Fatal("corrected declared tool call was not sent")
	}
	_ = commandResult(t, send)
	if len(client.events) != 1 || client.events[0].ToolCalls[0].ID != stableCallID {
		t.Fatalf("events = %+v", client.events)
	}
}

func TestCallerDeclaredToolCallRequiresOneJSONObject(t *testing.T) {
	t.Parallel()
	request := canonical.Request{Tools: []canonical.Tool{{Name: "search", InputSchema: []byte(`{}`)}}}
	for _, input := range []string{
		"search", "search []", "search null", `search {"query":"one"} {"query":"two"}`,
	} {
		input := input
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			if _, err := parseDeclaredToolCall(request, input, "tool_stable"); err == nil {
				t.Fatalf("parseDeclaredToolCall(%q) succeeded", input)
			}
		})
	}
}

func TestAdvancedToolInputUsesQualifiedNamespaceAndHonorsSerialPolicy(t *testing.T) {
	t.Parallel()
	request := canonical.Request{
		ToolCallPolicy: canonical.ToolCallsSerial,
		Tools: []canonical.Tool{
			{Name: "read", InputSchema: json.RawMessage(`{}`)},
			{Namespace: "workspace", Name: "read", InputSchema: json.RawMessage(`{}`)},
		},
	}
	call, err := parseDeclaredToolCall(request, `workspace::read {"path":"README.md"}`, "call-ns")
	if err != nil {
		t.Fatal(err)
	}
	if call.Namespace != "workspace" || call.Name != "read" || call.QualifiedName() != "workspace::read" {
		t.Fatalf("namespaced call = %+v", call)
	}
	if _, err := parseDeclaredToolCall(request, `other::read {}`, "call-other"); err == nil {
		t.Fatal("undeclared namespace accepted")
	}
	if _, err := parseDeclaredToolCalls(
		request, "read {}\nworkspace::read {}", []string{"call-1", "call-2"},
	); err == nil || !strings.Contains(err.Error(), "one tool call") {
		t.Fatalf("serial multi-call error = %v", err)
	}
	draft, ids, ok := advancedDraftFromCalls([]completion.ToolCall{call})
	if !ok || draft != `workspace::read {"path":"README.md"}` || len(ids) != 1 || ids[0] != "call-ns" {
		t.Fatalf("namespaced rejected draft = %q, %#v, %v", draft, ids, ok)
	}
}

func TestHostedCapabilityIsVisibleButOpaqueStateIsNotRendered(t *testing.T) {
	t.Parallel()
	request := canonical.Request{
		Messages: []canonical.Message{{
			Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "inspect"}},
		}},
		HostedCapabilities: []canonical.HostedCapability{{
			Type: "web_search", Configuration: json.RawMessage(`{"type":"web_search","secret":"hosted-secret"}`),
		}},
		OpaqueInput: []canonical.OpaqueInput{{
			Type: "reasoning", SHA256: strings.Repeat("a", 64),
		}},
	}
	for name, rendered := range map[string]string{
		"chat":   renderReadableChat(request),
		"detail": renderRequest(request),
	} {
		if !strings.Contains(rendered, "web_search") || !strings.Contains(rendered, "Human cannot call") {
			t.Fatalf("%s did not disclose hosted capability: %q", name, rendered)
		}
		if strings.Contains(rendered, "hosted-secret") || strings.Contains(rendered, "reasoning-secret") {
			t.Fatalf("%s leaked opaque provider state: %q", name, rendered)
		}
	}
}

func TestAdapterExecFallbackRequiresExactTopLevelDeclaration(t *testing.T) {
	t.Parallel()
	profile := adapter.HumanShimProfile()
	assignment := testAssignment()
	assignment.Adapter = &profile
	assignment.CapabilityTier = completion.TierRemoteTools
	assignment.ExecAllowed = true
	assignment.Request.Tools = []canonical.Tool{{
		Namespace: "wrapped", Name: profile.Exec.Name, InputSchema: json.RawMessage(`{}`),
	}}
	if _, reason := commandTargetForAssignment(assignment); !strings.Contains(reason, "did not declare adapter exec tool") {
		t.Fatalf("namespaced lookalike enabled command pane: %q", reason)
	}
	assignment.Request.Tools = append(assignment.Request.Tools, canonical.Tool{
		Name: profile.Exec.Name, InputSchema: json.RawMessage(`{}`),
	})
	if target, reason := commandTargetForAssignment(assignment); reason != "" || target.name != profile.Exec.Name {
		t.Fatalf("exact adapter exec declaration rejected: target=%+v reason=%q", target, reason)
	}
}

func TestCommandTargetMirrorsAdapterAuthorization(t *testing.T) {
	t.Parallel()
	profile := adapter.HumanShimProfile()
	assignment := testAssignment()
	assignment.Adapter = &profile
	assignment.CapabilityTier = completion.TierRemoteTools
	assignment.Request.Tools = []canonical.Tool{{
		Name: "bash", InputSchema: json.RawMessage(`{
  "type":"object",
  "properties":{"command":{"type":"string"}},
  "required":["command"],
  "additionalProperties":false
}`),
	}}

	if _, reason := commandTargetForAssignment(assignment); reason != "command disabled · remote exec is not authorized for this task" {
		t.Fatalf("unclassified command tool bypassed exec authorization: %q", reason)
	}
	assignment.ExecAllowed = true
	if target, reason := commandTargetForAssignment(assignment); reason != "" || target.name != "bash" {
		t.Fatalf("authorized unclassified command target = %+v, %q", target, reason)
	}

	assignment.ExecAllowed = false
	assignment.CapabilityTier = completion.TierChat
	if target, reason := commandTargetForAssignment(assignment); reason != "" || target.name != "bash" {
		t.Fatalf("chat-tier declared command target = %+v, %q", target, reason)
	}
}

func TestCallerDeclaredToolEntryRequiresTools(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := testAssignment()
	model := New(client)
	model.active = &assignment
	updated, command := model.Update(press("t", 't'))
	model = updated.(Model)
	if command != nil || model.composing || model.status != "caller declared no tools for this request" {
		t.Fatalf("tool-less model = %+v", model)
	}
}

func TestDeskKeepsInteractionTailVisibleAt24x80(t *testing.T) {
	t.Parallel()
	assignment := testAssignment()
	assignment.Request.System = "system-request-marker " + strings.Repeat("policy ", 20)
	assignment.Request.Messages = nil
	for index := range 30 {
		text := fmt.Sprintf("message-%02d %s", index, strings.Repeat("context ", 16))
		if index == 0 {
			text = "oldest-request-marker " + text
		}
		assignment.Request.Messages = append(assignment.Request.Messages, canonical.Message{
			Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: text}},
		})
	}
	for index := range 20 {
		assignment.Request.Tools = append(assignment.Request.Tools, canonical.Tool{
			Name:        fmt.Sprintf("tool_%02d", index),
			Description: strings.Repeat("long description ", 12),
			InputSchema: []byte(fmt.Sprintf(
				`{"type":"object","marker":"schema-%02d","description":"%s"}`,
				index, strings.Repeat("schema ", 80),
			)),
		})
	}
	model := New(newFakeClient())
	model.active = &assignment
	model.status = "operator-visible-status"
	updated, _ := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)

	view := ansi.Strip(model.View().Content)
	assertViewFits(t, view, 80, 24)
	for _, want := range []string{
		"Chat · HUMAN TURN", "Reply", "Tasks · Agent plan", "Command",
		"t advanced tools", "Status  operator-visible-status",
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("bounded desk does not contain %q:\n%s", want, view)
		}
	}

	updated, _ = model.Update(press("v", 'v'))
	model = updated.(Model)
	view = collectContextPages(&model)
	foundOldest := strings.Contains(view, "oldest-request-marker")
	foundSystem := strings.Contains(view, "system-request-marker")
	if !foundOldest || !foundSystem {
		t.Fatalf("paged context did not expose full retained request: oldest=%t system=%t", foundOldest, foundSystem)
	}

	model.focus = focusReply
	model.replyInput = strings.Repeat("draft line\n", 20)
	view = ansi.Strip(model.View().Content)
	assertViewFits(t, view, 80, 24)
	for _, want := range []string{
		"Reply", "draft line", "Status  operator-visible-status",
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("reply composer does not contain %q:\n%s", want, view)
		}
	}

	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	model = updated.(Model)
	updated, _ = model.Update(press("t", 't'))
	model = updated.(Model)
	view = ansi.Strip(model.View().Content)
	assertViewFits(t, view, 80, 24)
	for _, want := range []string{
		"Ctrl+S send tools", "<tool-name> <JSON object>", "Status  operator-visible-status",
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("tool composer does not contain %q:\n%s", want, view)
		}
	}

	model.input = "tool_00 "
	view = collectContextPages(&model)
	if !strings.Contains(view, "Selected tool schema (full, paged): tool_00") || !strings.Contains(view, "schema-00") {
		t.Fatalf("selected schema is not available in paged chat details:\n%s", view)
	}

	model.input = `missing {}`
	updated, command := model.Update(tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	model = updated.(Model)
	if command != nil {
		t.Fatal("invalid tool call returned a send command")
	}
	view = ansi.Strip(model.View().Content)
	assertViewFits(t, view, 80, 24)
	if !strings.Contains(view, "Status  tool calls not sent: line 1:") ||
		!strings.Contains(view, "Ctrl+S send tools") {
		t.Fatalf("validation error or operation hint is off-screen:\n%s", view)
	}
}

func TestViewNeutralizesUntrustedTerminalControls(t *testing.T) {
	t.Parallel()
	danger := "\x1b[8mHIDDEN\x1b[0m" +
		"\x1b]8;;https://evil.invalid\aLINK\x1b]8;;\a" +
		"\x1b[2JCLEAR\rCR\bBS\u009b31mC1\u202eBIDI"
	assignment := testAssignment()
	assignment.CallerID = "caller-" + danger
	assignment.TaskID = "task-" + danger
	assignment.Request.System = "system " + danger + "\n\tindented-system"
	assignment.Request.Messages = []canonical.Message{
		{Role: canonical.RoleUser, Blocks: []canonical.Block{
			{Type: canonical.BlockText, Text: "line-one\n\tline-two " + danger},
			{Type: canonical.BlockImage, ImageURL: "https://image.invalid/" + danger},
		}},
		{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: "call-1", ToolName: "danger",
			Input: map[string]any{"value": danger},
		}}},
		{Role: canonical.RoleTool, Blocks: []canonical.Block{{
			Type: canonical.BlockToolResult, ToolCallID: "call-1", Output: map[string]any{"value": danger},
		}}},
	}
	assignment.Request.Tools = []canonical.Tool{{
		Name: "danger", Description: "description " + danger,
		InputSchema: []byte("{\"type\":\"object\",\"description\":\"" + danger + "\"}"),
	}}
	model := New(newFakeClient())
	model.active = &assignment
	model.status = "SAFE_STATUS " + danger
	updated, _ := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	updated, _ = model.Update(press("t", 't'))
	model = updated.(Model)
	model.input = "danger "

	initial := model.View().Content
	assertViewFits(t, initial, 80, 24)
	assertNoDangerousTerminalControls(t, initial)
	plainInitial := ansi.Strip(initial)
	if !strings.Contains(plainInitial, "Ctrl+S send tools") || !strings.Contains(plainInitial, "Status  SAFE_STATUS") {
		t.Fatalf("terminal input hid controls/status:\n%s", plainInitial)
	}
	rendered := collectContextPages(&model)
	for _, visible := range []string{
		"␛[8m", "␛]8;;https://evil.invalid␇", "␛[2J", "␍CR␈BS", "⟦U+009B⟧", "⟦U+202E⟧",
		"line-one", "line-two", "indented-system",
	} {
		if !strings.Contains(rendered, visible) {
			t.Fatalf("sanitized paged view omitted %q:\n%s", visible, rendered)
		}
	}
}

func TestInboxSelectionIsBoundedAndVisibleInUnifiedWorkspace(t *testing.T) {
	t.Parallel()
	danger := "\x1b[8m\x1b[2J\r\b\u009b31m"
	model := New(newFakeClient())
	model.status = "QUEUE_STATUS " + danger
	for index := range 32 {
		assignment := testAssignment()
		assignment.CallerID = fmt.Sprintf("caller-%02d-%s", index, danger)
		assignment.TaskID = fmt.Sprintf("task-%02d-%s", index, danger)
		assignment.Request.Messages[0].Blocks[0].Text = fmt.Sprintf("preview-%02d %s", index, danger)
		model.assignments = append(model.assignments, assignment)
	}
	updated, _ := model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model = updated.(Model)
	view := model.View().Content
	assertViewFits(t, view, 80, 24)
	assertNoDangerousTerminalControls(t, view)
	plain := ansi.Strip(view)
	for _, want := range []string{"INBOX 32", "Inbox 1/32 · Agent plan", "a accept · r reject", "preview-00", "Status  QUEUE_STATUS"} {
		if !strings.Contains(plain, want) {
			t.Fatalf("initial queue does not contain %q:\n%s", want, view)
		}
	}

	for range 31 {
		updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyDown})
		model = updated.(Model)
	}
	view = model.View().Content
	assertViewFits(t, view, 80, 24)
	assertNoDangerousTerminalControls(t, view)
	plain = ansi.Strip(view)
	for _, want := range []string{"INBOX 32", "Inbox 32/32 · Agent plan", "a accept · r reject", "preview-31", "Status  QUEUE_STATUS"} {
		if !strings.Contains(plain, want) {
			t.Fatalf("scrolled queue does not contain %q:\n%s", want, view)
		}
	}
}

func assertViewFits(t *testing.T, view string, width, height int) {
	t.Helper()
	lines := strings.Split(view, "\n")
	if len(lines) > height {
		t.Fatalf("view has %d lines, want <= %d:\n%s", len(lines), height, view)
	}
	for index, line := range lines {
		if cells := ansi.StringWidth(line); cells > width {
			t.Fatalf("view line %d has %d cells, want <= %d: %q", index+1, cells, width, line)
		}
	}
}

func collectContextPages(model *Model) string {
	model.syncUI()
	model.resizeUI()
	model.ui.chatFollow = false
	model.ui.chat.GotoTop()
	var rendered strings.Builder
	for page := 0; page < 256; page++ {
		rendered.WriteString(ansi.Strip(model.View().Content))
		rendered.WriteByte('\n')
		if model.ui.chat.AtBottom() {
			break
		}
		updated, _ := model.Update(tea.KeyPressMsg{Code: tea.KeyPgDown})
		*model = updated.(Model)
	}
	return rendered.String()
}

func assertNoDangerousTerminalControls(t *testing.T, view string) {
	t.Helper()
	for _, sequence := range []string{"\x1b[2J", "\x1b]", "\x1b[8m", "\u009b"} {
		if strings.Contains(view, sequence) {
			t.Fatalf("view contains injected terminal sequence %q: %q", sequence, view)
		}
	}
	for _, character := range ansi.Strip(view) {
		if character == '\n' {
			continue
		}
		if unicode.IsControl(character) || isBidiControl(character) {
			t.Fatalf("view contains dangerous terminal/control rune U+%04X: %q", character, view)
		}
	}
}

func TestDisplayWrappingKeepsGraphemeClustersIntact(t *testing.T) {
	t.Parallel()
	emoji := "👩‍💻"
	input := strings.Repeat("a", 79) + emoji
	lines := wrapDisplayLines(input, 80)
	if len(lines) != 2 || lines[1] != emoji || strings.Join(lines, "") != input {
		t.Fatalf("emoji grapheme was split: %#v", lines)
	}
	combining := "e\u0301"
	input = strings.Repeat("a", 79) + combining + "x"
	lines = wrapDisplayLines(input, 80)
	if len(lines) != 2 || !strings.HasSuffix(lines[0], combining) || lines[1] != "x" {
		t.Fatalf("combining grapheme was split: %#v", lines)
	}
}

func TestReconnectAssignmentDoesNotDuplicateActiveDesk(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	assignment := testAssignment()
	updated, _ := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	updated, accept := model.Update(press("a", 'a'))
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, accept))
	model = updated.(Model)
	redelivered := assignment
	redelivered.LeaseOwner = "worker"
	updated, _ = model.Update(networkMessage{Assignment: &redelivered})
	model = updated.(Model)
	if model.active == nil || model.active.LeaseOwner != "worker" || len(model.assignments) != 0 {
		t.Fatalf("reconnected model = %+v", model)
	}
}

func TestWorkspaceAssignmentReconcilesOnlyUnseenToolResults(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	manager := newFilesystemMirrorManager(root)
	content := []byte("caller truth")
	fingerprint := callerfs.Fingerprint(content)
	request := canonical.Request{Messages: []canonical.Message{
		{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: "read-1", ToolName: "human_read_file",
			Input: map[string]any{"path": "/workspace/src/file.txt"},
		}}},
		{Role: canonical.RoleTool, Blocks: []canonical.Block{{
			Type: canonical.BlockToolResult, ToolCallID: "read-1",
			Output: `{"content":{"path":"/workspace/src/file.txt","sha256":"` + fingerprint + `","content":"caller truth","encoding":"utf-8"},"is_error":false}`,
		}}},
	}}
	assignment := workspaceAssignment(request)
	message := prepareMirror(manager, assignment)().(mirrorPrepared)
	if message.err != nil || message.namespace != mirrorNamespace("caller-a", "workspace-a") ||
		len(message.report.Confirmed) != 1 {
		t.Fatalf("first reconciliation = %+v", message)
	}
	wantedDir := filepath.Join(root, "caller-a", "workspace-a")
	if message.workspace.Dir() != wantedDir {
		t.Fatalf("mirror dir = %q, want %q", message.workspace.Dir(), wantedDir)
	}
	path := filepath.Join(wantedDir, "src", "file.txt")
	if err := os.WriteFile(path, []byte("worker edit"), 0o600); err != nil {
		t.Fatal(err)
	}

	second := assignment
	second.TaskID = "task-b"
	second.IdempotencyKey = "request-b"
	replayed := prepareMirror(manager, second)().(mirrorPrepared)
	if replayed.err != nil || len(replayed.report.Confirmed) != 0 {
		t.Fatalf("replayed reconciliation = %+v", replayed)
	}
	if replayed.workspace != message.workspace {
		t.Fatal("same caller/workspace opened a task-scoped mirror")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "worker edit" {
		t.Fatalf("historical result overwrote worker edit: %q, %v", data, err)
	}
}

func TestWorkspaceDeliveryRequiresReviewPreviewAndFreshConfirmation(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	root := t.TempDir()
	manager := newFilesystemMirrorManager(root)
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "change it"}},
	}}})
	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	original := []byte("before\n")
	if err := prepared.workspace.(*workmirror.Workspace).Hydrate(
		"/workspace/file.txt", original, callerfs.Fingerprint(original),
	); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(prepared.workspace.Dir(), "file.txt")
	if err := os.WriteFile(path, []byte("after\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	model := New(client, WithMirrorManager(manager))
	model.active = &assignment
	model.mirrors[prepared.namespace] = prepared.workspace

	updated, reviewCommand := model.Update(press("R", 'R'))
	model = updated.(Model)
	if len(client.events) != 0 || reviewCommand == nil {
		t.Fatalf("review sent an event or did not start: %+v", client.events)
	}
	updated, _ = model.Update(commandResult(t, reviewCommand))
	model = updated.(Model)
	if model.delivery.stage != deliveryReviewed || len(model.delivery.changes) != 1 || len(client.events) != 0 {
		t.Fatalf("review state = %+v, events = %+v", model.delivery, client.events)
	}

	updated, _ = model.Update(tea.KeyPressMsg{Code: 'p', Mod: tea.ModCtrl})
	model = updated.(Model)
	if model.delivery.stage != deliveryPreviewed || len(model.delivery.calls) != 1 || len(client.events) != 0 {
		t.Fatalf("preview state = %+v, events = %+v", model.delivery, client.events)
	}
	model.detailMode = true
	previewScreen := ansi.Strip(model.View().Content)
	if !strings.Contains(previewScreen, "Files · REVIEW") || !strings.Contains(previewScreen, "file.txt") {
		t.Fatalf("file confirmation screen hid its path or review state:\n%s", previewScreen)
	}
	view := collectContextPages(&model)
	if !strings.Contains(view, "file.txt") || !strings.Contains(view, "before") ||
		!strings.Contains(view, "after") || !strings.Contains(view, "not sent") {
		t.Fatalf("preview view omitted review content:\n%s", view)
	}

	updated, confirmationCommand := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	if confirmationCommand == nil || len(client.events) != 0 {
		t.Fatal("enter did not stage confirmation or sent before the fresh review")
	}
	updated, sendCommand := model.Update(commandResult(t, confirmationCommand))
	model = updated.(Model)
	if sendCommand == nil || len(client.events) != 0 {
		t.Fatal("confirmation did not stage the tool event")
	}
	_ = commandResult(t, sendCommand)
	if len(client.events) != 1 || client.events[0].Type != completion.EventToolCalls ||
		len(client.events[0].ToolCalls) != 1 {
		t.Fatalf("events = %+v", client.events)
	}
	call := client.events[0].ToolCalls[0]
	if call.Name != "human_edit_file" || call.Input["expected_sha256"] != callerfs.Fingerprint(original) ||
		call.Input["new_string"] != "after\n" {
		t.Fatalf("tool call = %+v", call)
	}
}

func TestLiveWorkspaceReviewStagesChangesAndOptionalAutoSend(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		autoSend bool
	}{
		{name: "review mode"},
		{name: "live mode", autoSend: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			manager := newFilesystemMirrorManager(t.TempDir())
			assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
				Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "change it"}},
			}}})
			prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
			if prepared.err != nil {
				t.Fatal(prepared.err)
			}
			if err := os.WriteFile(filepath.Join(prepared.workspace.Dir(), "live.txt"), []byte("live"), 0o600); err != nil {
				t.Fatal(err)
			}
			changes, err := prepared.workspace.Review()
			if err != nil || len(changes) != 1 {
				t.Fatalf("live review = %+v, %v", changes, err)
			}
			model := New(client, WithMirrorManager(manager), WithWorkspaceAutoSend(test.autoSend))
			model.active = &assignment
			model.mirrors[prepared.namespace] = prepared.workspace
			generation := model.requireMirrorReview(prepared.namespace)
			model.mirrorReviewing[prepared.namespace] = generation

			updated, command := model.Update(mirrorReviewReady{
				sessionKey: assignment.SessionKey(), namespace: prepared.namespace,
				generation: generation, changes: changes, automatic: true,
			})
			model = updated.(Model)
			if !test.autoSend {
				if command != nil || model.delivery.stage != deliveryReviewed || len(client.events) != 0 ||
					!strings.Contains(model.status, "workspace changed") {
					t.Fatalf("review-mode state = %+v events=%+v", model, client.events)
				}
				return
			}
			if command == nil || model.delivery.stage != deliveryConfirming {
				t.Fatalf("live mode did not enter fresh confirmation: %+v", model)
			}
			updated, send := model.Update(commandResult(t, command))
			model = updated.(Model)
			if send == nil || model.delivery.stage != deliverySending {
				t.Fatalf("live confirmation did not stage delivery: %+v", model)
			}
			_ = commandResult(t, send)
			if len(client.events) != 1 || client.events[0].Type != completion.EventToolCalls ||
				len(client.events[0].ToolCalls) != 1 || client.events[0].ToolCalls[0].Name != "human_write_file" {
				t.Fatalf("live events = %+v", client.events)
			}
		})
	}
}

func TestLiveWorkspaceAutoSendStopsForConflictWarning(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "change it"}},
	}}})
	model := New(client, WithWorkspaceAutoSend(true))
	model.active = &assignment
	namespace := mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)
	generation := model.requireMirrorReview(namespace)
	model.mirrorReviewing[namespace] = generation
	change := workmirror.Change{
		Kind: workmirror.ChangeEdit, Path: "file.txt", OldContent: []byte("caller"),
		NewContent: []byte("human"), ExpectedSHA: callerfs.Fingerprint([]byte("caller")),
		Warning: safety.SeverityWarn, Reasons: []string{"caller changed; merge before sending"},
	}
	updated, command := model.Update(mirrorReviewReady{
		sessionKey: assignment.SessionKey(),
		namespace:  namespace,
		generation: generation,
		changes:    []workmirror.Change{change}, automatic: true,
	})
	model = updated.(Model)
	if command != nil || model.delivery.stage != deliveryReviewed || len(client.events) != 0 ||
		!strings.Contains(model.status, "requires Human confirmation") {
		t.Fatalf("warning bypassed Human review: model=%+v events=%+v", model, client.events)
	}
}

func TestMirrorReviewShowsPerFileSkipWithoutBlockingOtherChanges(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "change it"}},
	}}})
	model := New(client)
	model.active = &assignment
	namespace := mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)
	generation := model.requireMirrorReview(namespace)
	model.mirrorReviewing[namespace] = generation
	change := workmirror.Change{
		Kind: workmirror.ChangeWrite, Path: "src/main.go", NewContent: []byte("package main\n"),
		ExpectedSHA: callerfs.AbsentFingerprint,
	}
	updated, command := model.Update(mirrorReviewReady{
		sessionKey: assignment.SessionKey(), namespace: namespace, generation: generation,
		changes: []workmirror.Change{change},
		diagnostics: []workmirror.ReviewDiagnostic{{
			Path: "node_modules/pkg/link", Reason: "symbolic links are not reviewed or delivered",
		}},
	})
	model = updated.(Model)
	if command != nil || model.delivery.stage != deliveryReviewed ||
		!strings.Contains(model.status, "src/main.go") && !strings.Contains(model.status, "1 change") ||
		!strings.Contains(model.status, "node_modules/pkg/link") || !strings.Contains(model.status, "symbolic links") {
		t.Fatalf("review diagnostics were hidden or blocked delivery: status=%q delivery=%+v", model.status, model.delivery)
	}
}

func TestLiveWorkspaceAutoSendStopsForSkippedWorkspaceEntry(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "change it"}},
	}}})
	model := New(client, WithWorkspaceAutoSend(true))
	model.active = &assignment
	namespace := mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)
	generation := model.requireMirrorReview(namespace)
	model.mirrorReviewing[namespace] = generation
	updated, command := model.Update(mirrorReviewReady{
		sessionKey: assignment.SessionKey(), namespace: namespace, generation: generation,
		changes: []workmirror.Change{{
			Kind: workmirror.ChangeWrite, Path: "src/main.go", NewContent: []byte("package main\n"),
			ExpectedSHA: callerfs.AbsentFingerprint, Warning: safety.SeverityAllow,
		}},
		diagnostics: []workmirror.ReviewDiagnostic{{
			Path: "node_modules/pkg/link", Reason: "symbolic links are not reviewed or delivered",
		}},
		automatic: true,
	})
	model = updated.(Model)
	if command != nil || model.delivery.stage != deliveryReviewed || len(client.events) != 0 ||
		!strings.Contains(model.status, "skipped workspace entries require Human confirmation") {
		t.Fatalf("skipped entry bypassed Human review: status=%q delivery=%+v events=%+v", model.status, model.delivery, client.events)
	}
}

func TestOpenCodeLiveWorkspaceEmitsNativeAbsoluteWriteAndKeepsAdapterWarningVisible(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	profile := adapter.OpenCode11718Profile()
	root := filepath.Join(t.TempDir(), "caller-root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	assignment := completion.Assignment{
		CallerID: "caller-a", WorkspaceKey: "workspace-a", TaskID: "ses_a",
		IdempotencyKey: "request-a", CapabilityTier: completion.TierWorkspace,
		HarnessID: adapter.OpenCodeID, HarnessVersion: adapter.OpenCodeVersion,
		Root: root, Adapter: &profile,
		Request: canonical.Request{
			Dialect: canonical.DialectOpenAIChat, Model: "human-expert", Stream: true,
			Messages: []canonical.Message{{Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "add a file"}}}},
			Tools: []canonical.Tool{{Name: "write", InputSchema: []byte(`{
			  "type":"object",
			  "properties":{"filePath":{"type":"string"},"content":{"type":"string"}},
			  "required":["filePath","content"]
			}`)}},
		},
	}
	if !mirrorEnabled(assignment) {
		t.Fatal("exact OpenCode workspace profile was not enabled")
	}
	manager := newFilesystemMirrorManager(t.TempDir())
	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	if err := os.MkdirAll(filepath.Join(prepared.workspace.Dir(), "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prepared.workspace.Dir(), "src", "live.txt"), []byte("from human\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	changes, err := prepared.workspace.Review()
	if err != nil || len(changes) != 1 {
		t.Fatalf("native mirror review = %+v, %v", changes, err)
	}
	model := New(client, WithMirrorManager(manager), WithWorkspaceAutoSend(true))
	model.active = &assignment
	model.mirrors[prepared.namespace] = prepared.workspace
	generation := model.requireMirrorReview(prepared.namespace)
	model.mirrorReviewing[prepared.namespace] = generation
	updated, confirm := model.Update(mirrorReviewReady{
		sessionKey: assignment.SessionKey(),
		namespace:  mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey),
		generation: generation, changes: changes, automatic: true,
	})
	model = updated.(Model)
	if confirm == nil || model.delivery.stage != deliveryConfirming || len(model.delivery.warnings) == 0 {
		t.Fatalf("native live preview = %+v; status=%s", model.delivery, model.status)
	}
	if !strings.Contains(renderDeliveryReview(model.delivery), "not CAS-protected") {
		t.Fatalf("adapter warning was hidden: %s", renderDeliveryReview(model.delivery))
	}
	updated, send := model.Update(commandResult(t, confirm))
	model = updated.(Model)
	if send == nil || model.delivery.stage != deliverySending {
		t.Fatalf("native live confirmation did not stage delivery: %+v", model.delivery)
	}
	_ = commandResult(t, send)
	if len(client.events) != 1 || len(client.events[0].ToolCalls) != 1 {
		t.Fatalf("native live events = %+v", client.events)
	}
	call := client.events[0].ToolCalls[0]
	if call.Name != "write" || call.Input["filePath"] != filepath.Join(root, "src", "live.txt") ||
		call.Input["content"] != "from human\n" {
		t.Fatalf("native OpenCode call = %+v", call)
	}
}

func TestOpenCodePreviewDeliversSupportedChangesWithoutLosingPendingDelete(t *testing.T) {
	t.Parallel()
	profile := adapter.OpenCode11718Profile()
	root := t.TempDir()
	assignment := completion.Assignment{
		CallerID: "caller", WorkspaceKey: "workspace", TaskID: "task",
		IdempotencyKey: "request", CapabilityTier: completion.TierWorkspace,
		HarnessID: adapter.OpenCodeID, HarnessVersion: adapter.OpenCodeVersion,
		Root: root, Adapter: &profile,
		Request: canonical.Request{Tools: []canonical.Tool{{
			Name: "write", InputSchema: []byte(`{
			  "type":"object",
			  "properties":{"filePath":{"type":"string"},"content":{"type":"string"}},
			  "required":["filePath","content"]
			}`),
		}}},
	}
	deleted := workmirror.Change{
		Kind: workmirror.ChangeDelete, Path: "removed.txt",
		ExpectedSHA: callerfs.Fingerprint([]byte("old")), Warning: safety.SeverityAllow,
	}
	write := workmirror.Change{
		Kind: workmirror.ChangeWrite, Path: "kept.txt", NewContent: []byte("new"),
		ExpectedSHA: callerfs.AbsentFingerprint, Warning: safety.SeverityAllow,
	}
	model := New(newFakeClient())
	model.active = &assignment
	model.delivery = deliveryReview{
		stage: deliveryReviewed, sessionKey: assignment.SessionKey(),
		changes: []workmirror.Change{deleted, write},
	}
	updated, command := model.previewMirrorDelivery()
	model = updated.(Model)
	if command != nil || model.delivery.stage != deliveryPreviewed ||
		len(model.delivery.calls) != 1 || model.delivery.calls[0].Name != "write" ||
		len(model.delivery.changes) != 1 || model.delivery.changes[0].Path != "kept.txt" {
		t.Fatalf("preview deliverable subset = %+v", model.delivery)
	}
	if warning := strings.Join(model.delivery.warnings, " "); !strings.Contains(warning, "removed.txt") || !strings.Contains(warning, "remains pending") {
		t.Fatalf("pending deletion warning = %#v", model.delivery.warnings)
	}
	selected := selectReviewedChanges([]workmirror.Change{deleted, write}, model.delivery.changes)
	if !sameChanges(selected, []workmirror.Change{write}) {
		t.Fatalf("confirmation subset = %+v", selected)
	}
}

func TestOpenCodeAutoSendStopsWhenDeleteCannotBeDelivered(t *testing.T) {
	t.Parallel()
	profile := adapter.OpenCode11718Profile()
	root := t.TempDir()
	assignment := completion.Assignment{
		CallerID: "caller", WorkspaceKey: "workspace", TaskID: "task",
		IdempotencyKey: "request", CapabilityTier: completion.TierWorkspace,
		HarnessID: adapter.OpenCodeID, HarnessVersion: adapter.OpenCodeVersion,
		Root: root, Adapter: &profile,
		Request: canonical.Request{Tools: []canonical.Tool{{
			Name: "write", InputSchema: []byte(`{
			  "type":"object",
			  "properties":{"filePath":{"type":"string"},"content":{"type":"string"}},
			  "required":["filePath","content"]
			}`),
		}}},
	}
	deleted := workmirror.Change{
		Kind: workmirror.ChangeDelete, Path: "removed.txt",
		ExpectedSHA: callerfs.Fingerprint([]byte("old")), Warning: safety.SeverityAllow,
	}
	write := workmirror.Change{
		Kind: workmirror.ChangeWrite, Path: "kept.txt", NewContent: []byte("new"),
		ExpectedSHA: callerfs.AbsentFingerprint, Warning: safety.SeverityAllow,
	}
	client := newFakeClient()
	model := New(client, WithWorkspaceAutoSend(true))
	model.active = &assignment
	namespace := mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)
	generation := model.requireMirrorReview(namespace)
	model.mirrorReviewing[namespace] = generation
	updated, command := model.Update(mirrorReviewReady{
		sessionKey: assignment.SessionKey(), namespace: namespace, generation: generation,
		changes: []workmirror.Change{deleted, write}, automatic: true,
	})
	model = updated.(Model)
	if command != nil || model.delivery.stage != deliveryPreviewed || len(client.events) != 0 ||
		len(model.delivery.changes) != 1 || model.delivery.changes[0].Path != "kept.txt" ||
		!strings.Contains(model.status, "undeliverable changes remain pending") {
		t.Fatalf("partial auto-send was not blocked: status=%q delivery=%+v events=%+v", model.status, model.delivery, client.events)
	}
}

func TestOpenCodeCommandPullBootstrapsExactMirrorFile(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	profile := adapter.OpenCode11718Profile()
	root := filepath.Join(t.TempDir(), "caller-root")
	assignment := completion.Assignment{
		CallerID: "caller", WorkspaceKey: "workspace", TaskID: "task", IdempotencyKey: "request",
		CapabilityTier: completion.TierWorkspace, HarnessID: adapter.OpenCodeID,
		HarnessVersion: adapter.OpenCodeVersion, Root: root, ExecAllowed: true, Adapter: &profile,
		Request: canonical.Request{
			Dialect: canonical.DialectOpenAIChat, Model: "human-expert", Stream: true,
			Messages: []canonical.Message{{Role: canonical.RoleUser, Blocks: []canonical.Block{{
				Type: canonical.BlockText, Text: "inspect src/file.txt",
			}}}},
			Tools: []canonical.Tool{{Name: "bash", InputSchema: []byte(`{
				"type":"object",
				"properties":{"command":{"type":"string"},"workdir":{"type":"string"},"timeout":{"type":"integer"}},
				"required":["command"],"additionalProperties":false
			}`)}},
		},
	}
	manager := newFilesystemMirrorManager(t.TempDir())
	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	model := New(client, WithMirrorManager(manager))
	model.active = &assignment
	model.mirrors[prepared.namespace] = prepared.workspace
	model.focus = focusCommand
	model.setCommandValue(":pull src/file.txt")

	updated, send := model.sendCommand()
	model = updated.(Model)
	if send == nil || model.active != nil || model.pending.kind != pendingCommand {
		t.Fatalf("workspace pull was not staged: %+v", model)
	}
	_ = commandResult(t, send)
	if len(client.events) != 1 || len(client.events[0].ToolCalls) != 1 {
		t.Fatalf("workspace pull events = %+v", client.events)
	}
	call := client.events[0].ToolCalls[0]
	if call.Name != "bash" || !strings.Contains(fmt.Sprint(call.Input["command"]), "opencode debug file read") ||
		call.Input["workdir"] != root {
		t.Fatalf("workspace pull call = %+v", call)
	}
	if path, ok := workspacePullDraftFromCall(assignment, call); !ok || path != "src/file.txt" {
		t.Fatalf("workspace pull reverse mapping = %q/%t", path, ok)
	}

	content := []byte("exact caller bytes\n")
	payload, err := json.Marshal(map[string]string{
		"content": base64.StdEncoding.EncodeToString(content), "encoding": "base64", "mime": "text/plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := canonical.Request{Messages: []canonical.Message{
		{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: call.ID, ToolName: call.Name, Input: call.Input,
		}}},
		{Role: canonical.RoleTool, Blocks: []canonical.Block{{
			Type: canonical.BlockToolResult, ToolCallID: call.ID, Output: string(payload),
		}}},
	}}
	reconciled, err := prepared.workspace.ReconcileRequestForProfile(request, &profile, root)
	if err != nil || len(reconciled.Confirmed) != 1 {
		t.Fatalf("workspace pull reconcile = %+v, %v", reconciled, err)
	}
	got, err := os.ReadFile(filepath.Join(prepared.workspace.Dir(), "src", "file.txt"))
	if err != nil || string(got) != string(content) {
		t.Fatalf("workspace pull content = %q, %v", got, err)
	}
}

func TestRejectedOpenCodePullRestoresDraftAndCleansIntentAfterRestart(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	profile := adapter.OpenCode11718Profile()
	callerRoot := t.TempDir()
	assignment := completion.Assignment{
		CallerID: "caller", WorkspaceKey: "rejected-pull", TaskID: "task", IdempotencyKey: "request",
		CapabilityTier: completion.TierWorkspace, HarnessID: adapter.OpenCodeID,
		HarnessVersion: adapter.OpenCodeVersion, Root: callerRoot, ExecAllowed: true, Adapter: &profile,
		Request: canonical.Request{
			Dialect: canonical.DialectOpenAIChat, Model: "human-expert", Stream: true,
			Messages: []canonical.Message{{Role: canonical.RoleUser, Blocks: []canonical.Block{{
				Type: canonical.BlockText, Text: "inspect src/it's.txt",
			}}}},
			Tools: []canonical.Tool{{Name: "bash", InputSchema: []byte(`{
				"type":"object",
				"properties":{"command":{"type":"string"},"workdir":{"type":"string"}},
				"required":["command"],"additionalProperties":false
			}`)}},
		},
	}
	mirrorRoot := t.TempDir()
	manager := newFilesystemMirrorManager(mirrorRoot)
	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	model := New(client, WithMirrorManager(manager))
	model.active = &assignment
	model.mirrors[prepared.namespace] = prepared.workspace
	model.focus = focusCommand
	model.setCommandValue(":pull src/it's.txt")
	updated, send := model.sendCommand()
	model = updated.(Model)
	if send == nil {
		t.Fatal("workspace pull was not staged")
	}
	_ = commandResult(t, send)
	if len(client.events) != 1 || len(client.events[0].ToolCalls) != 1 {
		t.Fatalf("workspace pull event = %+v", client.events)
	}
	event := client.events[0]
	draft, ok := rejectedDraftFromEvent(assignment, event)
	if !ok || !draft.hasCommand || draft.command != ":pull src/it's.txt" {
		t.Fatalf("rejected workspace pull draft = %+v/%t", draft, ok)
	}

	// Model restart: its in-memory mirror cache is intentionally empty. The
	// cleanup path must reopen the correctness namespace through MirrorManager.
	firstClient := newFakeClient()
	restarted := New(firstClient, WithMirrorManager(newFilesystemMirrorManager(mirrorRoot)))
	cleanup := restarted.discardIntentCommand(
		assignment, event.ToolCalls, "durable event rejection",
	)
	if cleanup == nil {
		t.Fatal("restarted model did not schedule intent cleanup")
	}
	result, ok := finalizeRejectedEvent(firstClient, event.ID, cleanup)().(mirrorIntentsDiscarded)
	if !ok || result.err != nil {
		t.Fatalf("restarted intent cleanup = %#v", result)
	}
	if len(firstClient.confirmed) != 0 {
		t.Fatalf("rejected inbox confirmed before cleanup result was applied: %v", firstClient.confirmed)
	}

	// Crash after the mirror cleanup committed but before Bubble Tea could
	// schedule ConfirmRejectedEvent. The rejected inbox is still durable, so a
	// second process replays it, repeats the idempotent cleanup, then confirms.
	secondClient := newFakeClient()
	afterCrash := New(secondClient, WithMirrorManager(newFilesystemMirrorManager(mirrorRoot)))
	replayedCleanup := afterCrash.discardIntentCommand(
		assignment, event.ToolCalls, "replayed durable event rejection",
	)
	replayed := finalizeRejectedEvent(secondClient, event.ID, replayedCleanup)()
	afterCrash, confirm := updateModelWithCommand(t, afterCrash, replayed)
	if confirm == nil || len(secondClient.confirmed) != 0 {
		t.Fatalf("replayed cleanup did not precede confirmation: model=%+v calls=%v", afterCrash, secondClient.confirmed)
	}
	confirmed, ok := confirm().(rejectedEventConfirmed)
	if !ok || confirmed.err != nil || !reflect.DeepEqual(secondClient.confirmed, []string{event.ID}) {
		t.Fatalf("replayed rejected inbox confirmation = %#v, calls=%v", confirmed, secondClient.confirmed)
	}
	statePath := filepath.Join(mirrorRoot, ".human-state", assignment.CallerID, assignment.WorkspaceKey, "baseline.json")
	state, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(state, []byte(event.ToolCalls[0].ID)) {
		t.Fatalf("rejected pull intent remained durable: %s", state)
	}
}

func TestWorkspaceProfileVersionOrSchemaDriftFailsClosed(t *testing.T) {
	t.Parallel()
	profile := adapter.OpenCode11718Profile()
	assignment := completion.Assignment{
		CallerID: "caller", WorkspaceKey: "workspace", TaskID: "task", IdempotencyKey: "request",
		CapabilityTier: completion.TierWorkspace, HarnessID: adapter.OpenCodeID,
		HarnessVersion: "1.17.19", Root: "/repo", Adapter: &profile,
	}
	if mirrorEnabled(assignment) {
		t.Fatal("unknown OpenCode version inherited workspace mutation capability")
	}
	request := canonical.Request{Tools: []canonical.Tool{{
		Name: "write", InputSchema: []byte(`{"type":"object","properties":{"path":{"type":"string"}}}`),
	}}}
	if err := validateMirrorCalls(request, []completion.ToolCall{{
		Name: "write", Input: map[string]any{"filePath": "/repo/file", "content": "x"},
	}}); err == nil {
		t.Fatal("schema drift accepted a profile field the caller no longer declares")
	}
}

func TestWorkspaceDeliveryRejectsChangesAfterPreview(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	root := t.TempDir()
	manager := newFilesystemMirrorManager(root)
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "change it"}},
	}}})
	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	if err := os.WriteFile(filepath.Join(prepared.workspace.Dir(), "new.txt"), []byte("v1"), 0o600); err != nil {
		t.Fatal(err)
	}
	model := New(client, WithMirrorManager(manager))
	model.active = &assignment
	model.mirrors[prepared.namespace] = prepared.workspace
	updated, command := model.Update(press("R", 'R'))
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, command))
	model = updated.(Model)
	updated, _ = model.Update(tea.KeyPressMsg{Code: 'p', Mod: tea.ModCtrl})
	model = updated.(Model)
	if err := os.WriteFile(filepath.Join(prepared.workspace.Dir(), "new.txt"), []byte("v2"), 0o600); err != nil {
		t.Fatal(err)
	}
	updated, command = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	updated, sendCommand := model.Update(commandResult(t, command))
	model = updated.(Model)
	if sendCommand == nil || len(client.events) != 0 || model.active == nil ||
		!strings.Contains(model.status, "changed after preview") {
		t.Fatalf("changed preview was sent: model=%+v events=%+v", model, client.events)
	}
	updated, _ = model.Update(commandResult(t, sendCommand))
	model = updated.(Model)
	if model.delivery.stage != deliveryReviewed || len(model.delivery.changes) != 1 ||
		string(model.delivery.changes[0].NewContent) != "v2" {
		t.Fatalf("changed preview did not refresh to the newest workspace: %+v", model.delivery)
	}
}

func TestWorkspacePreviewRejectsToolEventBeyondWireBudget(t *testing.T) {
	t.Parallel()
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "create large file"}},
	}}})
	model := New(newFakeClient())
	model.active = &assignment
	model.delivery = deliveryReview{
		stage: deliveryReviewed, sessionKey: assignment.SessionKey(),
		changes: []workmirror.Change{{
			Kind: workmirror.ChangeWrite, Path: "large.txt",
			NewContent: bytes.Repeat([]byte("x"), 9<<20), ExpectedSHA: callerfs.AbsentFingerprint,
		}},
	}
	updated, command := model.previewMirrorDelivery()
	model = updated.(Model)
	if command != nil || model.delivery.stage != deliveryNone ||
		!strings.Contains(model.status, "worker protocol message exceeds the wire limit") {
		t.Fatalf("oversized Workspace preview = stage %d status %q", model.delivery.stage, model.status)
	}
}

func TestWorkspaceDeliverySendFailureRetainsPreviewAndStableEventID(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	client.sendErr = errors.New("ack lost")
	manager := newFilesystemMirrorManager(t.TempDir())
	assignment := workspaceAssignment(canonical.Request{Messages: []canonical.Message{{
		Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "add it"}},
	}}})
	prepared := prepareMirror(manager, assignment)().(mirrorPrepared)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	if err := os.WriteFile(filepath.Join(prepared.workspace.Dir(), "new.txt"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	model := New(client, WithMirrorManager(manager))
	model.active = &assignment
	model.mirrors[prepared.namespace] = prepared.workspace
	updated, command := model.Update(press("R", 'R'))
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, command))
	model = updated.(Model)
	updated, _ = model.Update(tea.KeyPressMsg{Code: 'p', Mod: tea.ModCtrl})
	model = updated.(Model)
	firstEventID := model.delivery.eventID
	updated, command = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	updated, command = model.Update(commandResult(t, command))
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, command))
	model = updated.(Model)
	if model.active == nil || model.delivery.stage != deliveryConfirmed ||
		model.delivery.eventID != firstEventID || !strings.Contains(model.status, "exact event") {
		t.Fatalf("send failure lost delivery state: %+v", model)
	}
	client.sendErr = nil
	updated, command = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, command))
	model = updated.(Model)
	if model.active != nil || len(client.events) != 2 ||
		client.events[0].ID == "" || client.events[0].ID != client.events[1].ID {
		t.Fatalf("retry did not reuse the confirmed event: model=%+v events=%+v", model, client.events)
	}
}

func TestChatTierNeverOpensOrReviewsMirror(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	manager := &countingMirrorManager{}
	assignment := testAssignment()
	assignment.CapabilityTier = completion.TierChat
	model := New(client, WithMirrorManager(manager))
	updated, networkCommand := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	close(client.messages)
	_ = commandResult(t, networkCommand)
	model.active = &assignment
	updated, command := model.Update(press("R", 'R'))
	model = updated.(Model)
	if command != nil || manager.opens != 0 || len(client.events) != 0 ||
		model.status != "Chat tier has no workspace mirror" {
		t.Fatalf("Chat mirror state: %+v, opens=%d events=%+v", model, manager.opens, client.events)
	}
}

type countingMirrorManager struct{ opens int }

func (manager *countingMirrorManager) Open(_, _ string) (MirrorWorkspace, error) {
	manager.opens++
	return nil, nil
}
