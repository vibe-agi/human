package mirror

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/safety"
)

func buildHumanShimToolCalls(changes []Change) ([]completion.ToolCall, error) {
	profile := adapter.HumanShimProfile()
	report, err := BuildToolCallsForProfile(changes, &profile, "/workspace")
	return report.Calls, err
}

func reconcileHumanShim(workspace *Workspace, request canonical.Request) (ReconcileReport, error) {
	profile := adapter.HumanShimProfile()
	return workspace.ReconcileRequestForProfile(request, &profile, "/workspace")
}

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
	calls, err := buildHumanShimToolCalls(changes)
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
	calls, err := buildHumanShimToolCalls(changes)
	if err != nil {
		t.Fatal(err)
	}
	if calls[0].Name != "human_write_file" || calls[1].Name != "human_delete_file" {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestMirrorRejectsTraversalAndIsolatesSymlinks(t *testing.T) {
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
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "deliverable.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 1 || changes[0].Path != "deliverable.txt" {
		t.Fatalf("review with isolated symlink = %+v, %v", changes, err)
	}
	diagnostics := workspace.ReviewDiagnostics()
	if len(diagnostics) != 1 || diagnostics[0].Path != "escape" ||
		!strings.Contains(diagnostics[0].Reason, "symbolic link") {
		t.Fatalf("symlink diagnostics = %+v", diagnostics)
	}
}

func TestMirrorUnsupportedTrackedEntryIsNotMisreportedAsDelete(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink privileges vary on Windows")
	}
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	baseline := []byte("caller baseline")
	if err := workspace.Hydrate("tracked.txt", baseline, callerfs.Fingerprint(baseline)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(workspace.Dir(), "tracked.txt")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), path); err != nil {
		t.Fatal(err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 0 {
		t.Fatalf("tracked symlink became a caller mutation: %+v, %v", changes, err)
	}
	if diagnostics := workspace.ReviewDiagnostics(); len(diagnostics) != 1 || diagnostics[0].Path != "tracked.txt" {
		t.Fatalf("tracked symlink diagnostics = %+v", diagnostics)
	}
}

func TestMirrorSymlinkedTrackedDirectoryDoesNotDeleteItsBaselineChildren(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink privileges vary on Windows")
	}
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	baseline := []byte("caller baseline")
	if err := workspace.Hydrate("tracked/child.txt", baseline, callerfs.Fingerprint(baseline)); err != nil {
		t.Fatal(err)
	}
	tracked := filepath.Join(workspace.Dir(), "tracked")
	if err := os.RemoveAll(tracked); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), tracked); err != nil {
		t.Fatal(err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 0 {
		t.Fatalf("symlinked tracked directory became child deletes: %+v, %v", changes, err)
	}
	if diagnostics := workspace.ReviewDiagnostics(); len(diagnostics) != 1 || diagnostics[0].Path != "tracked" {
		t.Fatalf("tracked directory diagnostics = %+v", diagnostics)
	}
}

func TestMirrorOversizedFileDoesNotBlockUnrelatedReview(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	workspace.maxFile = 8
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "large.bin"), []byte("more than eight bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "small.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 1 || changes[0].Path != "small.txt" {
		t.Fatalf("review with oversized file = %+v, %v", changes, err)
	}
	diagnostics := workspace.ReviewDiagnostics()
	if len(diagnostics) != 1 || diagnostics[0].Path != "large.bin" ||
		!strings.Contains(diagnostics[0].Reason, "8-byte") {
		t.Fatalf("oversized diagnostics = %+v", diagnostics)
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
	calls, err := buildHumanShimToolCalls(changes)
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
	report, err := reconcileHumanShim(workspace, request)
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
	report, err = reconcileHumanShim(workspace, request)
	if err != nil || len(report.Confirmed) != 0 {
		t.Fatalf("replayed report = %+v, %v", report, err)
	}
	if data, _ := os.ReadFile(path); string(data) != "worker edit" {
		t.Fatalf("historical read overwrote worker edit: %q", data)
	}
}

func TestHydratePreservesDirtyDraftAndRebasesAgainstNewCallerTruth(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	workspace, err := Open(root, "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	original := []byte("caller v1\n")
	if err := workspace.Hydrate("file.txt", original, callerfs.Fingerprint(original)); err != nil {
		t.Fatal(err)
	}
	draft := []byte("human draft\n")
	path := filepath.Join(workspace.Dir(), "file.txt")
	if err := os.WriteFile(path, draft, 0o600); err != nil {
		t.Fatal(err)
	}

	callerV2 := []byte("caller v2\n")
	callerV2Hash := callerfs.Fingerprint(callerV2)
	if err := workspace.Hydrate("file.txt", callerV2, callerV2Hash); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(draft) {
		t.Fatalf("dirty Human draft was overwritten: %q, %v", got, err)
	}
	changes, err := workspace.Review()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].ExpectedSHA != callerV2Hash ||
		string(changes[0].OldContent) != string(callerV2) ||
		string(changes[0].NewContent) != string(draft) ||
		changes[0].Warning != safety.SeverityWarn ||
		!strings.Contains(strings.Join(changes[0].Reasons, " "), "merge before sending") {
		t.Fatalf("rebased conflict = %+v", changes)
	}

	reopened, err := Open(root, "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	changes, err = reopened.Review()
	if err != nil || len(changes) != 1 || changes[0].Warning != safety.SeverityWarn {
		t.Fatalf("persisted conflict = %+v, %v", changes, err)
	}
}

func TestHydrateRepeatedCallerBaselineNeverErasesLocalDeleteOrEdit(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	original := []byte("caller\n")
	fingerprint := callerfs.Fingerprint(original)
	if err := workspace.Hydrate("edited.txt", original, fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Hydrate("deleted.txt", original, fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "edited.txt"), []byte("draft\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(workspace.Dir(), "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Hydrate("edited.txt", original, fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Hydrate("deleted.txt", original, fingerprint); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(workspace.Dir(), "edited.txt")); string(got) != "draft\n" {
		t.Fatalf("repeated baseline overwrote edit: %q", got)
	}
	if _, err := os.Stat(filepath.Join(workspace.Dir(), "deleted.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("repeated baseline restored local delete: %v", err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 2 {
		t.Fatalf("preserved changes = %+v, %v", changes, err)
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
	report, err := reconcileHumanShim(workspace, request)
	if err != nil || report.Failed["edit-1"] != "precondition failed" {
		t.Fatalf("report = %+v, %v", report, err)
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 1 || changes[0].ExpectedSHA != callerfs.Fingerprint(original) {
		t.Fatalf("changes = %+v, %v", changes, err)
	}
}

func TestReconcileCallerRenameMovesUnmodifiedScratchSource(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("caller truth")
	fingerprint := callerfs.Fingerprint(content)
	if err := workspace.Hydrate("old/name.txt", content, fingerprint); err != nil {
		t.Fatal(err)
	}
	request := renameResultRequest(
		"rename-1", "/workspace/old/name.txt", "/workspace/new/name.txt", fingerprint,
	)
	report, err := reconcileHumanShim(workspace, request)
	if err != nil || len(report.Confirmed) != 1 || len(report.Failed) != 0 {
		t.Fatalf("rename report = %+v, %v", report, err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Dir(), "old", "name.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("old scratch path still exists: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(workspace.Dir(), "new", "name.txt")); err != nil || string(got) != string(content) {
		t.Fatalf("renamed scratch content = %q, %v", got, err)
	}
	if changes, err := workspace.Review(); err != nil || len(changes) != 0 {
		t.Fatalf("confirmed rename still drifts: %+v, %v", changes, err)
	}
	// Full-history replay is idempotent and must not disturb a later edit.
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "new", "name.txt"), []byte("later edit"), 0o600); err != nil {
		t.Fatal(err)
	}
	if report, err := reconcileHumanShim(workspace, request); err != nil || len(report.Confirmed) != 0 {
		t.Fatalf("rename replay = %+v, %v", report, err)
	}
	if got, _ := os.ReadFile(filepath.Join(workspace.Dir(), "new", "name.txt")); string(got) != "later edit" {
		t.Fatalf("rename replay overwrote later edit: %q", got)
	}
}

func TestReconcileCallerRenamePreservesUnconfirmedSourceEdit(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	baseline := []byte("baseline")
	fingerprint := callerfs.Fingerprint(baseline)
	if err := workspace.Hydrate("old.txt", baseline, fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "old.txt"), []byte("unconfirmed local edit"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := renameResultRequest("rename-conflict", "/workspace/old.txt", "/workspace/new.txt", fingerprint)
	report, err := reconcileHumanShim(workspace, request)
	if err != nil || !strings.Contains(report.Failed["rename-conflict"], ErrBaselineDrift.Error()) {
		t.Fatalf("rename conflict report = %+v, %v", report, err)
	}
	if got, _ := os.ReadFile(filepath.Join(workspace.Dir(), "old.txt")); string(got) != "unconfirmed local edit" {
		t.Fatalf("rename conflict lost source draft: %q", got)
	}
	if _, err := os.Stat(filepath.Join(workspace.Dir(), "new.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("rename conflict created destination: %v", err)
	}
}

func TestConfirmRenameFinishesCrashLikeDuplicatePaths(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("duplicate content")
	fingerprint := callerfs.Fingerprint(content)
	if err := workspace.Hydrate("from.txt", content, fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Dir(), "to.txt"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ConfirmRename("from.txt", "to.txt", fingerprint); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Dir(), "from.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("duplicate source survived reconciliation: %v", err)
	}
	if changes, err := workspace.Review(); err != nil || len(changes) != 0 {
		t.Fatalf("crash-like rename still drifts: %+v, %v", changes, err)
	}
}

func TestConfirmRenameNeverRemovesAliasedDestination(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("keep me")
	fingerprint := callerfs.Fingerprint(content)
	if err := workspace.Hydrate("Case.txt", content, fingerprint); err != nil {
		t.Fatal(err)
	}
	from := filepath.Join(workspace.Dir(), "Case.txt")
	to := filepath.Join(workspace.Dir(), "case.txt")
	fromInfo, err := os.Stat(from)
	if err != nil {
		t.Fatal(err)
	}
	toInfo, err := os.Stat(to)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skip("filesystem is case-sensitive")
	}
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(fromInfo, toInfo) {
		t.Skip("paths are not aliases on this filesystem")
	}
	if err := workspace.ConfirmRename("Case.txt", "case.txt", fingerprint); !errors.Is(err, ErrBaselineDrift) {
		t.Fatalf("ConfirmRename error = %v, want ErrBaselineDrift", err)
	}
	if got, err := os.ReadFile(from); err != nil || string(got) != string(content) {
		t.Fatalf("aliased destination was removed or changed: %q, %v", got, err)
	}
}

func TestConfirmRenameRejectsHardLinkedPathsWithoutDeletingEither(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("same inode content")
	fingerprint := callerfs.Fingerprint(content)
	if err := workspace.Hydrate("from.txt", content, fingerprint); err != nil {
		t.Fatal(err)
	}
	from := filepath.Join(workspace.Dir(), "from.txt")
	to := filepath.Join(workspace.Dir(), "to.txt")
	if err := os.Link(from, to); err != nil {
		t.Fatal(err)
	}
	if err := workspace.ConfirmRename("from.txt", "to.txt", fingerprint); !errors.Is(err, ErrBaselineDrift) {
		t.Fatalf("ConfirmRename error = %v, want ErrBaselineDrift", err)
	}
	for _, path := range []string{from, to} {
		if got, err := os.ReadFile(path); err != nil || string(got) != string(content) {
			t.Fatalf("hard-linked path %q was removed or changed: %q, %v", path, got, err)
		}
	}
}

func renameResultRequest(id, from, to, fingerprint string) canonical.Request {
	result := `{"content":{"from":` + quoteJSON(from) + `,"to":` + quoteJSON(to) +
		`,"sha256":` + quoteJSON(fingerprint) + `},"is_error":false}`
	return canonical.Request{Messages: []canonical.Message{
		{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: id, ToolName: "human_rename_file",
			Input: map[string]any{"from": from, "to": to},
		}}},
		{Role: canonical.RoleTool, Blocks: []canonical.Block{{
			Type: canonical.BlockToolResult, ToolCallID: id, Output: result,
		}}},
	}}
}

func quoteJSON(value string) string {
	payload, _ := json.Marshal(value)
	return string(payload)
}
