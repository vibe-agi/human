package mirror

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/safety"
)

func TestHydrateReviewBuildAndConfirmCASLoop(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	workspace, err := Open(root, "caller-1", "workspace-1")
	if err != nil {
		t.Fatal(err)
	}
	original := []byte("one\ntwo\n")
	originalHash := callerfs.Fingerprint(original)
	if err := workspace.Hydrate("/workspace/src/file.txt", original, originalHash); err != nil {
		t.Fatal(err)
	}
	updated := []byte("one\nchanged\n")
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "src", "file.txt"), updated, 0o600); err != nil {
		t.Fatal(err)
	}
	changes, err := workspace.Review()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Kind != ChangeEdit || string(changes[0].OldContent) != string(original) ||
		string(changes[0].NewContent) != string(updated) || changes[0].ExpectedSHA != originalHash {
		t.Fatalf("changes = %+v", changes)
	}
	calls, err := BuildToolCalls(changes)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].Name != "human_edit_file" || calls[0].Input["expected_sha256"] != originalHash {
		t.Fatalf("calls = %+v", calls)
	}
	updatedHash := callerfs.Fingerprint(updated)
	if err := workspace.Confirm("src/file.txt", updatedHash, false); err != nil {
		t.Fatal(err)
	}
	if changes, err := workspace.Review(); err != nil || len(changes) != 0 {
		t.Fatalf("confirmed changes = %+v, %v", changes, err)
	}
	reopened, err := Open(root, "caller-1", "workspace-1")
	if err != nil {
		t.Fatal(err)
	}
	if changes, err := reopened.Review(); err != nil || len(changes) != 0 {
		t.Fatalf("reopened changes = %+v, %v", changes, err)
	}
}

func TestNewDeleteSensitiveAndFailedConfirmationRemainExplicit(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	original := []byte("old")
	if err := workspace.Hydrate("old.txt", original, callerfs.Fingerprint(original)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(workspace.Dir(), "old.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Dir(), ".env"), []byte("SECRET=x"), 0o600); err != nil {
		t.Fatal(err)
	}
	changes, err := workspace.Review()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0].Path != ".env" || changes[0].Warning != safety.SeverityWarn || changes[1].Kind != ChangeDelete {
		t.Fatalf("changes = %+v", changes)
	}
	if err := workspace.Confirm(".env", "sha256:wrong", false); !errors.Is(err, ErrBaselineDrift) {
		t.Fatalf("bad confirmation error = %v", err)
	}
	if changes, err := workspace.Review(); err != nil || len(changes) != 2 {
		t.Fatalf("failed confirmation advanced baseline: %+v, %v", changes, err)
	}
	calls, err := BuildToolCalls(changes)
	if err != nil {
		t.Fatal(err)
	}
	if calls[0].Name != "human_write_file" || calls[1].Name != "human_delete_file" {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestMirrorRejectsTraversalAndSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink privileges vary on Windows")
	}
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	if err := workspace.Hydrate("../escape", []byte("x"), callerfs.Fingerprint([]byte("x"))); !errors.Is(err, ErrMirrorEscape) {
		t.Fatalf("traversal error = %v", err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(workspace.Dir(), "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.Review(); !errors.Is(err, ErrMirrorEscape) {
		t.Fatalf("symlink review error = %v", err)
	}
}

func TestBinaryEditFallsBackToCASWrite(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	original := []byte{0xff, 0x00}
	if err := workspace.Hydrate("image.bin", original, callerfs.Fingerprint(original)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "image.bin"), []byte{0xfe, 0x01}, 0o600); err != nil {
		t.Fatal(err)
	}
	changes, _ := workspace.Review()
	calls, err := BuildToolCalls(changes)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].Name != "human_write_file" || calls[0].Input["encoding"] != "base64" {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestReconcileRequestHydratesEachToolResultOnlyOnce(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("caller truth")
	fingerprint := callerfs.Fingerprint(content)
	result := `{"content":{"path":"/workspace/file.txt","sha256":"` + fingerprint + `","size":12,"content":"caller truth","encoding":"utf-8"},"is_error":false}`
	request := canonical.Request{Messages: []canonical.Message{
		{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: "read-1", ToolName: "human_read_file",
			Input: map[string]any{"path": "/workspace/file.txt"},
		}}},
		{Role: canonical.RoleTool, Blocks: []canonical.Block{{
			Type: canonical.BlockToolResult, ToolCallID: "read-1", Output: result,
		}}},
	}}
	report, err := workspace.ReconcileRequest(request)
	if err != nil || len(report.Confirmed) != 1 || len(report.Failed) != 0 {
		t.Fatalf("report = %+v, %v", report, err)
	}
	path := filepath.Join(workspace.Dir(), "file.txt")
	if data, _ := os.ReadFile(path); string(data) != "caller truth" {
		t.Fatalf("hydrated content = %q", data)
	}
	if err := os.WriteFile(path, []byte("worker edit"), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err = workspace.ReconcileRequest(request)
	if err != nil || len(report.Confirmed) != 0 {
		t.Fatalf("replayed report = %+v, %v", report, err)
	}
	if data, _ := os.ReadFile(path); string(data) != "worker edit" {
		t.Fatalf("historical read overwrote worker edit: %q", data)
	}
}

func TestFailedToolResultDoesNotAdvanceMirrorBaseline(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	original := []byte("original")
	if err := workspace.Hydrate("file.txt", original, callerfs.Fingerprint(original)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "file.txt"), []byte("worker edit"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := canonical.Request{Messages: []canonical.Message{
		{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: "edit-1", ToolName: "human_edit_file",
			Input: map[string]any{"path": "/workspace/file.txt"},
		}}},
		{Role: canonical.RoleTool, Blocks: []canonical.Block{{
			Type: canonical.BlockToolResult, ToolCallID: "edit-1",
			Output: `{"content":"precondition failed","is_error":true,"error_code":"cas_mismatch"}`,
		}}},
	}}
	report, err := workspace.ReconcileRequest(request)
	if err != nil || report.Failed["edit-1"] != "precondition failed" {
		t.Fatalf("report = %+v, %v", report, err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 1 || changes[0].ExpectedSHA != callerfs.Fingerprint(original) {
		t.Fatalf("changes = %+v, %v", changes, err)
	}
}
