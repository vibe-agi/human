package mirror

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

func TestBuildToolCallsForOpenCode11718UsesOnlyProfileMappings(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "caller-workspace")
	profile := adapter.OpenCode11718Profile()
	changes := []Change{
		{
			Kind: ChangeEdit, Path: "src/edit.txt", OldContent: []byte("old\n"),
			NewContent: []byte("new\n"), ExpectedSHA: callerfs.Fingerprint([]byte("old\n")),
		},
		{
			Kind: ChangeWrite, Path: "src/new.txt", NewContent: []byte("created\n"),
			ExpectedSHA: callerfs.AbsentFingerprint,
		},
	}
	report, err := BuildToolCallsForProfile(changes, &profile, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Calls) != 2 || report.Calls[0].ID == "" || report.Calls[1].ID == "" {
		t.Fatalf("calls = %+v", report.Calls)
	}
	wantEdit := map[string]any{
		"filePath":  filepath.Join(root, "src", "edit.txt"),
		"oldString": "old\n",
		"newString": "new\n",
	}
	if report.Calls[0].Name != "edit" || !reflect.DeepEqual(report.Calls[0].Input, wantEdit) {
		t.Fatalf("edit call = %+v, want input %#v", report.Calls[0], wantEdit)
	}
	wantWrite := map[string]any{
		"filePath": filepath.Join(root, "src", "new.txt"),
		"content":  "created\n",
	}
	if report.Calls[1].Name != "write" || !reflect.DeepEqual(report.Calls[1].Input, wantWrite) {
		t.Fatalf("write call = %+v, want input %#v", report.Calls[1], wantWrite)
	}
	if len(report.Warnings) != 2 {
		t.Fatalf("warnings = %#v", report.Warnings)
	}
	for _, warning := range report.Warnings {
		if !strings.Contains(warning, "not CAS-protected") {
			t.Fatalf("warning does not disclose downgrade: %q", warning)
		}
	}
}

func TestBuildToolCallsForProfileReportsCapabilityDowngrades(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	profile := adapter.OpenCode11718Profile()
	profile.Edit = nil
	change := Change{
		Kind: ChangeEdit, Path: "file.txt", OldContent: []byte("old"),
		NewContent: []byte("new"), ExpectedSHA: callerfs.Fingerprint([]byte("old")),
	}
	report, err := BuildToolCallsForProfile([]Change{change}, &profile, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Calls) != 1 || report.Calls[0].Name != "write" ||
		report.Calls[0].Input["content"] != "new" {
		t.Fatalf("write fallback = %+v", report.Calls)
	}
	if joined := strings.Join(report.Warnings, " "); !strings.Contains(joined, "downgraded edit") || !strings.Contains(joined, "not CAS-protected") {
		t.Fatalf("fallback warnings = %#v", report.Warnings)
	}

	profile = adapter.OpenCode11718Profile()
	deleteReport, err := BuildToolCallsForProfile([]Change{{
		Kind: ChangeDelete, Path: "file.txt", ExpectedSHA: change.ExpectedSHA,
	}}, &profile, root)
	if err != nil || len(deleteReport.Calls) != 0 || len(deleteReport.Changes) != 0 ||
		len(deleteReport.Warnings) != 1 || !strings.Contains(deleteReport.Warnings[0], "deletion remains pending") {
		t.Fatalf("missing delete capability report = %+v, %v", deleteReport, err)
	}
	_, err = BuildToolCallsForProfile([]Change{{
		Kind: ChangeWrite, Path: "binary.bin", NewContent: []byte{0xff},
	}}, &profile, root)
	if err == nil || !strings.Contains(err.Error(), "no binary encoding field") {
		t.Fatalf("binary downgrade error = %v", err)
	}
	if _, err := BuildToolCallsForProfile([]Change{change}, nil, root); err == nil ||
		!strings.Contains(err.Error(), "no exact adapter profile") {
		t.Fatalf("missing profile error = %v", err)
	}
}

func TestBuildToolCallsSkipsOnlyUnmappedDeleteFromMixedBatch(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	profile := adapter.OpenCode11718Profile()
	write := Change{
		Kind: ChangeWrite, Path: "kept.txt", NewContent: []byte("deliver me"),
		ExpectedSHA: callerfs.AbsentFingerprint,
	}
	deleted := Change{
		Kind: ChangeDelete, Path: "removed.txt",
		ExpectedSHA: callerfs.Fingerprint([]byte("old")),
	}
	report, err := BuildToolCallsForProfile([]Change{deleted, write}, &profile, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Calls) != 1 || report.Calls[0].Name != "write" ||
		len(report.Changes) != 1 || report.Changes[0].Path != "kept.txt" {
		t.Fatalf("deliverable subset = %+v", report)
	}
	if joined := strings.Join(report.Warnings, " "); !strings.Contains(joined, "removed.txt") ||
		!strings.Contains(joined, "deletion remains pending") {
		t.Fatalf("delete warning = %#v", report.Warnings)
	}
}

func TestReconcileOpenCode11718MutationUsesExactSuccessContract(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	baseline := []byte("old\n")
	if err := workspace.Hydrate("file.txt", baseline, callerfs.Fingerprint(baseline)); err != nil {
		t.Fatal(err)
	}
	updated := []byte("new\n")
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "file.txt"), updated, 0o600); err != nil {
		t.Fatal(err)
	}
	callerRoot := filepath.Join(t.TempDir(), "real-caller")
	profile := adapter.OpenCode11718Profile()
	request := profileToolResultRequest(
		"edit-1", "edit",
		map[string]any{
			"filePath":  filepath.Join(callerRoot, "file.txt"),
			"oldString": "old\n", "newString": "new\n",
		},
		"Edit applied successfully.", false,
	)
	report, err := workspace.ReconcileRequestForProfile(request, &profile, callerRoot)
	if err != nil || len(report.Confirmed) != 1 || len(report.Failed) != 0 {
		t.Fatalf("reconcile report = %+v, %v", report, err)
	}
	if len(report.Warnings) != 1 || !strings.Contains(report.Warnings[0], "not CAS proof") {
		t.Fatalf("reconcile warnings = %#v", report.Warnings)
	}
	if changes, err := workspace.Review(); err != nil || len(changes) != 0 {
		t.Fatalf("confirmed OpenCode edit = %+v, %v", changes, err)
	}
	replayed, err := workspace.ReconcileRequestForProfile(request, &profile, callerRoot)
	if err != nil || len(replayed.Confirmed) != 0 || len(replayed.Warnings) != 0 {
		t.Fatalf("replayed report = %+v, %v", replayed, err)
	}
	rewritten := profileToolResultRequest(
		"edit-1", "edit",
		map[string]any{
			"filePath":  filepath.Join(callerRoot, "file.txt"),
			"oldString": "old\n", "newString": "rewritten history\n",
		},
		"Edit applied successfully.", false,
	)
	if _, err := workspace.ReconcileRequestForProfile(rewritten, &profile, callerRoot); err == nil ||
		!strings.Contains(err.Error(), "changed after reconciliation") {
		t.Fatalf("rewritten native tool use was accepted: %v", err)
	}
}

func TestRecordedOpenCodeDeliveryAdvancesSentBaselineAndKeepsNewerHumanDraft(t *testing.T) {
	t.Parallel()
	mirrorRoot := t.TempDir()
	workspace, err := Open(mirrorRoot, "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	v0 := []byte("version zero\n")
	if err := workspace.Hydrate("file.txt", v0, callerfs.Fingerprint(v0)); err != nil {
		t.Fatal(err)
	}
	v1 := []byte("version one sent\n")
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "file.txt"), v1, 0o600); err != nil {
		t.Fatal(err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 1 {
		t.Fatalf("v1 review = %+v, %v", changes, err)
	}
	callerRoot := filepath.Join(t.TempDir(), "caller-root")
	profile := adapter.OpenCode11718Profile()
	report, err := BuildToolCallsForProfile(changes, &profile, callerRoot)
	if err != nil || len(report.Calls) != 1 || report.Calls[0].Name != "edit" {
		t.Fatalf("v1 delivery = %+v, %v", report, err)
	}
	if err := workspace.RecordDeliveryIntents(changes, report.Calls, &profile, callerRoot); err != nil {
		t.Fatal(err)
	}

	// Saving v2 after the v1 event entered the outbox must not make the v1
	// success compare against the wrong local content.
	v2 := []byte("version two still local\n")
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "file.txt"), v2, 0o600); err != nil {
		t.Fatal(err)
	}
	workspace, err = Open(mirrorRoot, "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	call := report.Calls[0]
	request := profileToolResultRequest(
		call.ID, call.Name, call.Input, "Edit applied successfully.", false,
	)
	reconciled, err := workspace.ReconcileRequestForProfile(request, &profile, callerRoot)
	if err != nil || len(reconciled.Confirmed) != 1 || len(reconciled.Failed) != 0 {
		t.Fatalf("v1 result = %+v, %v", reconciled, err)
	}
	pending, err := workspace.Review()
	if err != nil || len(pending) != 1 || pending[0].Kind != ChangeEdit ||
		string(pending[0].OldContent) != string(v1) || string(pending[0].NewContent) != string(v2) {
		t.Fatalf("post-result review = %+v, %v", pending, err)
	}

	// Both the advanced baseline and the consumed result receipt survive a
	// worker restart; replay cannot apply the intent twice.
	workspace, err = Open(mirrorRoot, "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := workspace.ReconcileRequestForProfile(request, &profile, callerRoot)
	if err != nil || len(replayed.Confirmed) != 0 {
		t.Fatalf("replayed v1 result = %+v, %v", replayed, err)
	}
	pending, err = workspace.Review()
	if err != nil || len(pending) != 1 || string(pending[0].OldContent) != string(v1) {
		t.Fatalf("restarted baseline = %+v, %v", pending, err)
	}
}

func TestRecordDeliveryIntentsRejectsBatchAtomically(t *testing.T) {
	t.Parallel()
	mirrorRoot := t.TempDir()
	workspace, err := Open(mirrorRoot, "caller", "atomic-delivery")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"a.txt", "b.txt"} {
		before := []byte("before " + path + "\n")
		if err := workspace.Hydrate(path, before, callerfs.Fingerprint(before)); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(workspace.Dir(), path), []byte("after "+path+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 2 {
		t.Fatalf("review = %+v, %v", changes, err)
	}
	callerRoot := filepath.Join(t.TempDir(), "caller-root")
	profile := adapter.OpenCode11718Profile()
	report, err := BuildToolCallsForProfile(changes, &profile, callerRoot)
	if err != nil || len(report.Calls) != 2 {
		t.Fatalf("build = %+v, %v", report, err)
	}
	broken := append([]completion.ToolCall(nil), report.Calls...)
	broken[1].Input = cloneInputMap(broken[1].Input)
	broken[1].Input["newString"] = "not the reviewed content"
	if err := workspace.RecordDeliveryIntents(changes, broken, &profile, callerRoot); err == nil {
		t.Fatal("partially invalid delivery batch was accepted")
	}
	if len(workspace.state.Deliveries) != 0 {
		t.Fatalf("failed batch polluted in-memory intents: %+v", workspace.state.Deliveries)
	}
	reopened, err := Open(mirrorRoot, "caller", "atomic-delivery")
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.state.Deliveries) != 0 {
		t.Fatalf("failed batch polluted durable intents: %+v", reopened.state.Deliveries)
	}
	if err := reopened.RecordDeliveryIntents(changes, report.Calls, &profile, callerRoot); err != nil {
		t.Fatalf("valid batch after rejection: %v", err)
	}
	if len(reopened.state.Deliveries) != 2 {
		t.Fatalf("valid batch intents = %d, want 2", len(reopened.state.Deliveries))
	}
}

func cloneInputMap(input map[string]any) map[string]any {
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func TestReconcileOpenCode11718DoesNotConfirmMismatchedMutation(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	baseline := []byte("old\n")
	if err := workspace.Hydrate("file.txt", baseline, callerfs.Fingerprint(baseline)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "file.txt"), []byte("reviewed draft\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	callerRoot := filepath.Join(t.TempDir(), "real-caller")
	profile := adapter.OpenCode11718Profile()
	request := profileToolResultRequest(
		"edit-mismatch", "edit",
		map[string]any{
			"filePath":  filepath.Join(callerRoot, "file.txt"),
			"oldString": "old\n", "newString": "different payload\n",
		},
		"Edit applied successfully.", false,
	)
	report, err := workspace.ReconcileRequestForProfile(request, &profile, callerRoot)
	if err != nil || len(report.Confirmed) != 0 ||
		!strings.Contains(report.Failed["edit-mismatch"], "does not match") {
		t.Fatalf("mismatched report = %+v, %v", report, err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 1 || string(changes[0].NewContent) != "reviewed draft\n" {
		t.Fatalf("mismatched result advanced baseline: %+v, %v", changes, err)
	}
}

func TestReconcileOpenCode11718ReadFailsClosedOnLossyDisplay(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	callerRoot := filepath.Join(t.TempDir(), "real-caller")
	profile := adapter.OpenCode11718Profile()
	request := profileToolResultRequest(
		"read-1", "read", map[string]any{"filePath": filepath.Join(callerRoot, "file.txt")},
		"<path>file.txt</path>\n<type>file</type>\n<content>\n1: value\n\n"+
			"(End of file - total 1 lines)\n</content>", false,
	)
	report, err := workspace.ReconcileRequestForProfile(request, &profile, callerRoot)
	if err != nil || len(report.Confirmed) != 0 || len(report.Failed) != 0 ||
		len(report.Warnings) != 1 || !strings.Contains(report.Warnings[0], "not hydrated") {
		t.Fatalf("read report = %+v, %v", report, err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Dir(), "file.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lossy read created a baseline: %v", err)
	}
	// The deliberate skip is ledgered, so cumulative history does not emit the
	// same warning on every completion.
	replayed, err := workspace.ReconcileRequestForProfile(request, &profile, callerRoot)
	if err != nil || len(replayed.Warnings) != 0 {
		t.Fatalf("read replay = %+v, %v", replayed, err)
	}
}

func TestOpenCodeExactWorkspacePullHydratesNativeMirror(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	callerRoot := filepath.Join(t.TempDir(), "caller's workspace")
	profile := adapter.OpenCode11718Profile()
	call, err := BuildHydrationToolCallForProfile("src/it's.txt", &profile, callerRoot)
	if err != nil {
		t.Fatal(err)
	}
	command, _ := call.Input["command"].(string)
	if call.Name != "bash" || !strings.Contains(command, "opencode debug file read --pure") ||
		!strings.Contains(command, `'"'"'`) || call.Input["workdir"] != callerRoot {
		t.Fatalf("workspace pull call = %+v", call)
	}
	if err := workspace.RecordHydrationIntent("src/it's.txt", call, &profile, callerRoot); err != nil {
		t.Fatal(err)
	}
	content := []byte("exact bytes from caller\n\x00")
	payload, err := json.Marshal(map[string]string{
		"content":  base64.StdEncoding.EncodeToString(content),
		"encoding": "base64", "mime": "application/octet-stream",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := profileToolResultRequest(call.ID, call.Name, call.Input, string(payload), false)
	reconciled, err := workspace.ReconcileRequestForProfile(request, &profile, callerRoot)
	if err != nil || len(reconciled.Confirmed) != 1 || len(reconciled.Failed) != 0 ||
		len(reconciled.Warnings) != 1 || !strings.Contains(reconciled.Warnings[0], "exact base64") {
		t.Fatalf("workspace pull result = %+v, %v", reconciled, err)
	}
	got, err := os.ReadFile(filepath.Join(workspace.Dir(), "src", "it's.txt"))
	if err != nil || !reflect.DeepEqual(got, content) {
		t.Fatalf("hydrated content = %q, %v", got, err)
	}
	if changes, err := workspace.Review(); err != nil || len(changes) != 0 {
		t.Fatalf("hydrated mirror is dirty: %+v, %v", changes, err)
	}
}

func TestOpenCodeExactWorkspacePullAcceptsEmptyFile(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace-empty")
	if err != nil {
		t.Fatal(err)
	}
	callerRoot := t.TempDir()
	profile := adapter.OpenCode11718Profile()
	call, err := BuildHydrationToolCallForProfile("empty.txt", &profile, callerRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := workspace.RecordHydrationIntent("empty.txt", call, &profile, callerRoot); err != nil {
		t.Fatal(err)
	}
	payload := `{"content":"","encoding":"base64","mime":"text/plain"}`
	request := profileToolResultRequest(call.ID, call.Name, call.Input, payload, false)
	reconciled, err := workspace.ReconcileRequestForProfile(request, &profile, callerRoot)
	if err != nil || len(reconciled.Confirmed) != 1 || len(reconciled.Failed) != 0 {
		t.Fatalf("empty workspace pull = %+v, %v", reconciled, err)
	}
	got, err := os.ReadFile(filepath.Join(workspace.Dir(), "empty.txt"))
	if err != nil || len(got) != 0 {
		t.Fatalf("empty hydrated content = %q, %v", got, err)
	}
}

func TestOpenCodeExactWorkspacePullQuotesLeadingDashPath(t *testing.T) {
	t.Parallel()
	profile := adapter.OpenCode11718Profile()
	call, err := BuildHydrationToolCallForProfile("-fixture.txt", &profile, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if command, _ := call.Input["command"].(string); !strings.HasSuffix(command, " './-fixture.txt'") {
		t.Fatalf("leading-dash pull command = %q", command)
	}
}

func TestWorkspaceToolIntentsDiscardAtomically(t *testing.T) {
	t.Parallel()
	mirrorRoot := t.TempDir()
	workspace, err := Open(mirrorRoot, "caller", "discard-intents")
	if err != nil {
		t.Fatal(err)
	}
	before := []byte("before\n")
	if err := workspace.Hydrate("edit.txt", before, callerfs.Fingerprint(before)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "edit.txt"), []byte("after\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 1 {
		t.Fatalf("review = %+v, %v", changes, err)
	}
	profile := adapter.OpenCode11718Profile()
	callerRoot := t.TempDir()
	delivery, err := BuildToolCallsForProfile(changes, &profile, callerRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := workspace.RecordDeliveryIntents(changes, delivery.Calls, &profile, callerRoot); err != nil {
		t.Fatal(err)
	}
	pull, err := BuildHydrationToolCallForProfile("pull.txt", &profile, callerRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := workspace.RecordHydrationIntent("pull.txt", pull, &profile, callerRoot); err != nil {
		t.Fatal(err)
	}
	wrong := pull
	wrong.Input = cloneInputMap(pull.Input)
	wrong.Input["command"] = "different"
	if err := workspace.DiscardToolIntents([]completion.ToolCall{wrong}, &profile); err == nil {
		t.Fatal("mismatched intent discard succeeded")
	}
	if len(workspace.state.Deliveries) != 1 || len(workspace.state.Hydrations) != 1 {
		t.Fatalf("mismatched discard changed state: %+v", workspace.state)
	}
	all := append(append([]completion.ToolCall(nil), delivery.Calls...), pull)
	if err := workspace.DiscardToolIntents(all, &profile); err != nil {
		t.Fatal(err)
	}
	if len(workspace.state.Deliveries) != 0 || len(workspace.state.Hydrations) != 0 {
		t.Fatalf("discarded intent state = %+v", workspace.state)
	}
	reopened, err := Open(mirrorRoot, "caller", "discard-intents")
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.state.Deliveries) != 0 || len(reopened.state.Hydrations) != 0 {
		t.Fatalf("durable discarded intent state = %+v", reopened.state)
	}
}

func TestRecordHydrationIntentRollsBackMemoryWhenSaveFails(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "hydrate-rollback")
	if err != nil {
		t.Fatal(err)
	}
	profile := adapter.OpenCode11718Profile()
	callerRoot := t.TempDir()
	call, err := BuildHydrationToolCallForProfile("file.txt", &profile, callerRoot)
	if err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0o600); err != nil {
		t.Fatal(err)
	}
	workspace.stateDir = blocker
	if err := workspace.RecordHydrationIntent("file.txt", call, &profile, callerRoot); err == nil {
		t.Fatal("hydration intent unexpectedly saved through a non-directory")
	}
	if len(workspace.state.Hydrations) != 0 {
		t.Fatalf("failed hydration save polluted memory: %+v", workspace.state.Hydrations)
	}
}

func TestReconcileForProfileDoesNotGuessToolsFromNamesOrSchemas(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	profile := adapter.OpenCode11718Profile()
	request := profileToolResultRequest(
		"foreign-1", "human_write_file",
		map[string]any{"filePath": filepath.Join(t.TempDir(), "file.txt")},
		"Wrote file successfully.", false,
	)
	report, err := workspace.ReconcileRequestForProfile(request, &profile, t.TempDir())
	if err != nil || len(report.Confirmed) != 0 || len(report.Failed) != 0 || len(report.Warnings) != 0 {
		t.Fatalf("foreign result was inferred from its name: %+v, %v", report, err)
	}
	missing, err := workspace.ReconcileRequestForProfile(request, nil, t.TempDir())
	if err != nil || len(missing.Warnings) != 1 ||
		!strings.Contains(missing.Warnings[0], "no exact adapter profile") {
		t.Fatalf("missing profile downgrade = %+v, %v", missing, err)
	}
}

func profileToolResultRequest(
	id, name string,
	input map[string]any,
	output any,
	isError bool,
) canonical.Request {
	return canonical.Request{Messages: []canonical.Message{
		{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: id, ToolName: name, Input: input,
		}}},
		{Role: canonical.RoleTool, Blocks: []canonical.Block{{
			Type: canonical.BlockToolResult, ToolCallID: id, Output: output, IsError: isError,
		}}},
	}}
}
