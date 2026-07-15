package tui

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	workmirror "github.com/vibe-agi/human/internal/mirror"
	"github.com/vibe-agi/human/internal/workerclient"
)

type fakeClient struct {
	messages chan workerclient.Message
	events   []completion.Event
	sendErr  error
}

func newFakeClient() *fakeClient {
	return &fakeClient{messages: make(chan workerclient.Message, 4)}
}

func (client *fakeClient) Messages() <-chan workerclient.Message { return client.messages }
func (client *fakeClient) SendEvent(_ context.Context, _ completion.Assignment, event completion.Event) error {
	client.events = append(client.events, event)
	return client.sendErr
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

func TestQueueAcceptComposeAndFinal(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	assignment := testAssignment()
	updated, _ := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	updated, command := model.Update(press("a", 'a'))
	model = updated.(Model)
	if model.active == nil || model.view != ViewDesk {
		t.Fatalf("accepted model = %+v", model)
	}
	_ = command()
	if len(client.events) != 1 || client.events[0].Type != completion.EventAccepted {
		t.Fatalf("events = %+v", client.events)
	}
	updated, _ = model.Update(press("c", 'c'))
	model = updated.(Model)
	updated, _ = model.Update(press("o", 'o'))
	model = updated.(Model)
	updated, _ = model.Update(press("k", 'k'))
	model = updated.(Model)
	updated, command = model.Update(tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	model = updated.(Model)
	_ = command()
	if model.active != nil || len(client.events) != 2 || client.events[1].Type != completion.EventFinal || client.events[1].Text != "ok" {
		t.Fatalf("final model = %+v, events = %+v", model, client.events)
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
	_ = command()
	if len(model.assignments) != 0 || len(client.events) != 1 || client.events[0].Type != completion.EventRejected {
		t.Fatalf("model = %+v, events = %+v", model, client.events)
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
	updated, _ := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	updated, command := model.Update(press("a", 'a'))
	model = updated.(Model)
	_ = command()
	updated, _ = model.Update(press("x", 'x'))
	model = updated.(Model)
	if model.composing || model.status != "exec is disabled for this task" {
		t.Fatalf("disabled exec model = %+v", model)
	}

	model.active.ExecAllowed = true
	updated, _ = model.Update(press("x", 'x'))
	model = updated.(Model)
	if !model.composing || model.composeKind != composeExec {
		t.Fatalf("enabled exec model = %+v", model)
	}
	for _, key := range []rune("printf ok") {
		updated, _ = model.Update(press(string(key), key))
		model = updated.(Model)
	}
	updated, command = model.Update(tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	model = updated.(Model)
	_ = command()
	if len(client.events) != 2 || client.events[1].Type != completion.EventToolCalls || len(client.events[1].ToolCalls) != 1 {
		t.Fatalf("events = %+v", client.events)
	}
	call := client.events[1].ToolCalls[0]
	if call.Name != "human_exec" || call.Input["command"] != "printf ok" || call.Input["cwd"] != "/workspace" {
		t.Fatalf("exec tool call = %+v", call)
	}
}

func TestReconnectAssignmentDoesNotDuplicateActiveDesk(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	model := New(client)
	assignment := testAssignment()
	updated, _ := model.Update(networkMessage{Assignment: &assignment})
	model = updated.(Model)
	updated, _ = model.Update(press("a", 'a'))
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
	model.view = ViewDesk
	model.mirrors[prepared.namespace] = prepared.workspace

	updated, reviewCommand := model.Update(press("R", 'R'))
	model = updated.(Model)
	if len(client.events) != 0 || reviewCommand == nil {
		t.Fatalf("review sent an event or did not start: %+v", client.events)
	}
	updated, _ = model.Update(reviewCommand())
	model = updated.(Model)
	if model.delivery.stage != deliveryReviewed || len(model.delivery.changes) != 1 || len(client.events) != 0 {
		t.Fatalf("review state = %+v, events = %+v", model.delivery, client.events)
	}

	updated, _ = model.Update(tea.KeyPressMsg{Code: 'p', Mod: tea.ModCtrl})
	model = updated.(Model)
	if model.delivery.stage != deliveryPreviewed || len(model.delivery.calls) != 1 || len(client.events) != 0 {
		t.Fatalf("preview state = %+v, events = %+v", model.delivery, client.events)
	}
	view := model.View().Content
	if !strings.Contains(view, "before") || !strings.Contains(view, "after") || !strings.Contains(view, "not sent") {
		t.Fatalf("preview view omitted review content:\n%s", view)
	}

	updated, confirmationCommand := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	if confirmationCommand == nil || len(client.events) != 0 {
		t.Fatal("enter did not stage confirmation or sent before the fresh review")
	}
	updated, sendCommand := model.Update(confirmationCommand())
	model = updated.(Model)
	if sendCommand == nil || len(client.events) != 0 {
		t.Fatal("confirmation did not stage the tool event")
	}
	_ = sendCommand()
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
	model.view = ViewDesk
	model.mirrors[prepared.namespace] = prepared.workspace
	updated, command := model.Update(press("R", 'R'))
	model = updated.(Model)
	updated, _ = model.Update(command())
	model = updated.(Model)
	updated, _ = model.Update(tea.KeyPressMsg{Code: 'p', Mod: tea.ModCtrl})
	model = updated.(Model)
	if err := os.WriteFile(filepath.Join(prepared.workspace.Dir(), "new.txt"), []byte("v2"), 0o600); err != nil {
		t.Fatal(err)
	}
	updated, command = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	updated, sendCommand := model.Update(command())
	model = updated.(Model)
	if sendCommand != nil || len(client.events) != 0 || model.active == nil ||
		!strings.Contains(model.status, "changed after preview") {
		t.Fatalf("changed preview was sent: model=%+v events=%+v", model, client.events)
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
	model.view = ViewDesk
	model.mirrors[prepared.namespace] = prepared.workspace
	updated, command := model.Update(press("R", 'R'))
	model = updated.(Model)
	updated, _ = model.Update(command())
	model = updated.(Model)
	updated, _ = model.Update(tea.KeyPressMsg{Code: 'p', Mod: tea.ModCtrl})
	model = updated.(Model)
	firstEventID := model.delivery.eventID
	updated, command = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	updated, command = model.Update(command())
	model = updated.(Model)
	updated, _ = model.Update(command())
	model = updated.(Model)
	if model.active == nil || model.delivery.stage != deliveryPreviewed ||
		model.delivery.eventID != firstEventID || !strings.Contains(model.status, "preview retained") {
		t.Fatalf("send failure lost delivery state: %+v", model)
	}
	client.sendErr = nil
	updated, command = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(Model)
	updated, command = model.Update(command())
	model = updated.(Model)
	updated, _ = model.Update(command())
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
	_ = networkCommand()
	model.active = &assignment
	model.view = ViewDesk
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
