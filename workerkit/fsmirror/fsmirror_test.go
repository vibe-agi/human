package fsmirror_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
	"github.com/vibe-agi/human/workerkit/fsmirror"
)

var testScope = workerkit.WorkspaceScope{Caller: "caller-a", WorkspaceKey: "workspace-a"}
var testBinding = workerkit.SessionBinding{
	Scope: testScope, HarnessID: "opencode", HarnessVersion: "1.17.18",
	HarnessSessionID: "session-a",
}

func writeBuilder(change workerkit.Change, content []byte, _ workerkit.MirrorResolve) ([]llm.ToolCall, error) {
	return []llm.ToolCall{{
		ID: "call-" + change.ID, Name: "write",
		Input: map[string]any{"filePath": change.Path, "content": string(content)},
	}}, nil
}

func openMirror(t *testing.T, root string, baseline string) *fsmirror.Mirror {
	t.Helper()
	mirror, err := fsmirror.Open(t.Context(), fsmirror.Config{
		Root: t.TempDir(), Scope: testScope,
		Build: writeBuilder, BaselineFile: baseline, Debounce: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mirror.Close(); err != nil {
			t.Errorf("close mirror: %v", err)
		}
	})
	if _, err := mirror.PrepareSession(t.Context(), testBinding, root); err != nil {
		t.Fatal(err)
	}
	return mirror
}

func testWorkspace(t *testing.T, mirror *fsmirror.Mirror, root string) workerkit.HumanWorkspace {
	t.Helper()
	workspace, err := mirror.PrepareSession(t.Context(), testBinding, root)
	if err != nil {
		t.Fatal(err)
	}
	return workspace
}

func nextReview(t *testing.T, mirror *fsmirror.Mirror, condition func(workerkit.Review) bool) workerkit.Review {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case review := <-mirror.Reviews():
			if condition(review) {
				return review
			}
		case <-deadline:
			t.Fatal("expected review never arrived")
		}
	}
}

func TestMirrorReviewsCreateModifyDelete(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "existing.txt"), []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mirror := openMirror(t, root, "")

	initial := nextReview(t, mirror, func(workerkit.Review) bool { return true })
	if len(initial.Changes) != 0 {
		t.Fatalf("initial review = %+v, want clean", initial.Changes)
	}

	if err := os.WriteFile(filepath.Join(root, "new.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review := nextReview(t, mirror, func(review workerkit.Review) bool { return len(review.Changes) == 1 })
	if review.Changes[0].Kind != workerkit.ChangeCreate || review.Changes[0].Path != "new.txt" ||
		!strings.Contains(review.Changes[0].Diff, "+hello") {
		t.Fatalf("create change = %+v", review.Changes[0])
	}

	if err := os.WriteFile(filepath.Join(root, "existing.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review = nextReview(t, mirror, func(review workerkit.Review) bool { return len(review.Changes) == 2 })
	var modified workerkit.Change
	for _, change := range review.Changes {
		if change.Path == "existing.txt" {
			modified = change
		}
	}
	if modified.Kind != workerkit.ChangeModify || !strings.Contains(modified.Diff, "-old") ||
		!strings.Contains(modified.Diff, "+new") {
		t.Fatalf("modify change = %+v", modified)
	}

	if err := os.Remove(filepath.Join(root, "existing.txt")); err != nil {
		t.Fatal(err)
	}
	review = nextReview(t, mirror, func(review workerkit.Review) bool {
		for _, change := range review.Changes {
			if change.Path == "existing.txt" && change.Kind == workerkit.ChangeDelete {
				return true
			}
		}
		return false
	})
	_ = review
}

func TestMirrorResolveFreezesBytesAndSettleAdvancesBaseline(t *testing.T) {
	root := t.TempDir()
	mirror := openMirror(t, root, "")
	nextReview(t, mirror, func(workerkit.Review) bool { return true })

	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review := nextReview(t, mirror, func(review workerkit.Review) bool { return len(review.Changes) == 1 })
	change := review.Changes[0]

	if _, err := mirror.Resolve(t.Context(), workerkit.MirrorResolve{
		ChangeIDs: []string{change.ID},
		Scope:     workerkit.WorkspaceScope{Caller: "caller-b", WorkspaceKey: "workspace-a"},
	}); !errors.Is(err, workerkit.ErrMirrorScopeMismatch) {
		t.Fatalf("cross-caller mirror resolve error = %v, want ErrMirrorScopeMismatch", err)
	}
	calls, err := mirror.Resolve(t.Context(), workerkit.MirrorResolve{
		ChangeIDs: []string{change.ID}, Scope: testScope,
		Workspace: testWorkspace(t, mirror, root),
		Tools:     []llm.Tool{{Name: "write", InputSchema: []byte(`{"type":"object"}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].Input["content"] != "package main\n" {
		t.Fatalf("resolved calls = %+v", calls)
	}

	// The in-flight change disappears from review until settlement.
	review = nextReview(t, mirror, func(review workerkit.Review) bool { return len(review.Changes) == 0 })
	_ = review

	if err := mirror.Settle(t.Context(), workerkit.MirrorSettlement{
		ChangeIDs: []string{change.ID}, Outcome: workerkit.MirrorDelivered,
	}); err != nil {
		t.Fatal(err)
	}
	// Delivered: still clean, and a further edit diffs against the new baseline.
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review = nextReview(t, mirror, func(review workerkit.Review) bool { return len(review.Changes) == 1 })
	if review.Changes[0].Kind != workerkit.ChangeModify ||
		strings.Contains(review.Changes[0].Diff, "-package main\n+") {
		t.Fatalf("post-delivery change = %+v", review.Changes[0])
	}

	// Idempotent settle of an already settled batch is a no-op.
	if err := mirror.Settle(t.Context(), workerkit.MirrorSettlement{
		ChangeIDs: []string{change.ID}, Outcome: workerkit.MirrorDelivered,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMirrorDiscardDropsChange(t *testing.T) {
	root := t.TempDir()
	mirror := openMirror(t, root, "")
	nextReview(t, mirror, func(workerkit.Review) bool { return true })
	if err := os.WriteFile(filepath.Join(root, "scratch.txt"), []byte("temp\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review := nextReview(t, mirror, func(review workerkit.Review) bool { return len(review.Changes) == 1 })
	if err := mirror.Settle(t.Context(), workerkit.MirrorSettlement{
		ChangeIDs: []string{review.Changes[0].ID}, Outcome: workerkit.MirrorDiscarded,
	}); err != nil {
		t.Fatal(err)
	}
	review = nextReview(t, mirror, func(review workerkit.Review) bool { return len(review.Changes) == 0 })
	_ = review
}

func TestMirrorBaselinePersistsAcrossReopen(t *testing.T) {
	root := t.TempDir()
	baseline := filepath.Join(t.TempDir(), "baseline.json")
	first, err := fsmirror.Open(t.Context(), fsmirror.Config{
		Root: t.TempDir(), Scope: testScope,
		Build: writeBuilder, BaselineFile: baseline, Debounce: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstWorkspace := testWorkspace(t, first, root)
	// Deliver one file so the baseline is persisted, then edit it again and
	// crash (close) before delivering the second edit.
	if err := os.WriteFile(filepath.Join(root, "kept.txt"), []byte("v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review := nextReview(t, first, func(review workerkit.Review) bool { return len(review.Changes) == 1 })
	if _, err := first.Resolve(t.Context(), workerkit.MirrorResolve{
		ChangeIDs: []string{review.Changes[0].ID}, Scope: testScope, Workspace: firstWorkspace,
	}); err != nil {
		t.Fatal(err)
	}
	if err := first.Settle(t.Context(), workerkit.MirrorSettlement{
		ChangeIDs: []string{review.Changes[0].ID}, Outcome: workerkit.MirrorDelivered,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "kept.txt"), []byte("v2 undelivered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	nextReview(t, first, func(review workerkit.Review) bool { return len(review.Changes) == 1 })
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openMirror(t, root, baseline)
	review = nextReview(t, reopened, func(review workerkit.Review) bool { return len(review.Changes) == 1 })
	change := review.Changes[0]
	if change.Path != "kept.txt" || change.Kind != workerkit.ChangeModify ||
		!strings.Contains(change.Diff, "-v1") || !strings.Contains(change.Diff, "+v2 undelivered") {
		t.Fatalf("undelivered edit did not survive restart: %+v", change)
	}
	_ = context.Background()
}

func TestSessionWorkspaceDefaultsAndExplicitRepoSwitchSeedsBaseline(t *testing.T) {
	mirror := openMirror(t, t.TempDir(), "")
	// openMirror already prepared testBinding against its explicit test root;
	// use a distinct session for the default-directory contract.
	binding := testBinding
	binding.HarnessSessionID = "session-default-and-switch"
	workspace, err := mirror.PrepareSession(t.Context(), binding, "")
	if err != nil {
		t.Fatal(err)
	}
	wantID, err := workerkit.SessionWorkspaceID(binding)
	if err != nil {
		t.Fatal(err)
	}
	if workspace.ID != wantID || filepath.Base(workspace.Path) != wantID {
		t.Fatalf("default Human workspace = %+v, want stable child %s", workspace, wantID)
	}

	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "existing.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	switched, err := mirror.PrepareSession(t.Context(), binding, repo)
	if err != nil {
		t.Fatal(err)
	}
	canonicalRepo, err := filepath.EvalSymlinks(repo)
	if err != nil {
		t.Fatal(err)
	}
	if switched.ID != wantID || switched.Path != canonicalRepo {
		t.Fatalf("switched Human workspace = %+v, want id %s path %s", switched, wantID, canonicalRepo)
	}
	// Existing repo contents are the switch baseline, not a whole-repo create.
	nextReview(t, mirror, func(review workerkit.Review) bool {
		for _, change := range review.Changes {
			if change.WorkspaceID == wantID {
				return false
			}
		}
		return true
	})
	if err := os.WriteFile(filepath.Join(repo, "existing.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review := nextReview(t, mirror, func(review workerkit.Review) bool {
		for _, change := range review.Changes {
			if change.WorkspaceID == wantID && change.Path == "existing.txt" {
				return true
			}
		}
		return false
	})
	for _, change := range review.Changes {
		if change.WorkspaceID == wantID && (change.Path != "existing.txt" || change.Kind != workerkit.ChangeModify) {
			t.Fatalf("switched repo change = %+v", change)
		}
	}
}

func TestMirrorWatchesRapidlyNestedDirectories(t *testing.T) {
	root := t.TempDir()
	mirror := openMirror(t, root, "")
	nextReview(t, mirror, func(workerkit.Review) bool { return true })

	// mkdir -p a/b/c then edit only under the deepest directory: the watch on
	// the new subtree must be installed so the edit produces a review change.
	deep := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(deep, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deep, "deep.txt"), []byte("nested\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review := nextReview(t, mirror, func(review workerkit.Review) bool {
		for _, change := range review.Changes {
			if change.Path == "a/b/c/deep.txt" {
				return true
			}
		}
		return false
	})
	_ = review
}

func TestMirrorIgnoresGitAndSymlinks(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	mirror := openMirror(t, root, "")
	nextReview(t, mirror, func(workerkit.Review) bool { return true })

	if err := os.WriteFile(filepath.Join(root, ".git", "config"), []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, ".git", "config"), filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "visible.txt"), []byte("ok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review := nextReview(t, mirror, func(review workerkit.Review) bool {
		for _, change := range review.Changes {
			if change.Path == "visible.txt" {
				return true
			}
		}
		return false
	})
	for _, change := range review.Changes {
		if strings.Contains(change.Path, ".git") {
			t.Fatalf(".git leaked into review: %+v", change)
		}
		if change.Path == "alias" && change.Warning == "" {
			t.Fatalf("symlink was not quarantined: %+v", change)
		}
	}
}

func TestOpenCodeNativeBuilderUsesExactEditSnapshotAndFailsClosedForDelete(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "native.txt")
	if err := os.WriteFile(path, []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mirror, err := fsmirror.Open(t.Context(), fsmirror.Config{
		Root: t.TempDir(), Scope: testScope,
		BuildSnapshot: fsmirror.OpenCodeNativeBuilder(), Debounce: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mirror.Close() })
	workspace := testWorkspace(t, mirror, root)
	nextReview(t, mirror, func(workerkit.Review) bool { return true })

	if err := os.WriteFile(path, []byte("after\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review := nextReview(t, mirror, func(review workerkit.Review) bool {
		return len(review.Changes) == 1 && review.Changes[0].Kind == workerkit.ChangeModify
	})
	change := review.Changes[0]
	calls, err := mirror.Resolve(t.Context(), workerkit.MirrorResolve{
		ChangeIDs: []string{change.ID}, Scope: testScope, Workspace: workspace,
		Tools: []llm.Tool{{Name: "write"}, {Name: "edit"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].Name != "edit" || calls[0].Input["filePath"] != "native.txt" ||
		calls[0].Input["oldString"] != "before\n" || calls[0].Input["newString"] != "after\n" {
		t.Fatalf("native edit calls = %+v", calls)
	}
	if err := mirror.Settle(t.Context(), workerkit.MirrorSettlement{
		ChangeIDs: []string{change.ID}, Outcome: workerkit.MirrorDelivered,
	}); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	review = nextReview(t, mirror, func(review workerkit.Review) bool {
		return len(review.Changes) == 1 && review.Changes[0].Kind == workerkit.ChangeDelete
	})
	if _, err := mirror.Resolve(t.Context(), workerkit.MirrorResolve{
		ChangeIDs: []string{review.Changes[0].ID}, Scope: testScope, Workspace: workspace,
		Tools: []llm.Tool{{Name: "write"}, {Name: "edit"}},
	}); err == nil || !strings.Contains(err.Error(), "delete has no mapped native tool") {
		t.Fatalf("native delete error = %v", err)
	}
}

func TestClaudeCodeNativeBuilderUsesExactWriteAndEditSchemas(t *testing.T) {
	builder := fsmirror.ClaudeCodeNativeBuilder()
	request := workerkit.MirrorResolve{
		Tools: []llm.Tool{{Name: "Write"}, {Name: "Edit"}},
	}
	created, err := builder(fsmirror.BuildSnapshot{
		Change: workerkit.Change{ID: "create", Path: "nested/new.txt", Kind: workerkit.ChangeCreate},
		After:  []byte("created\n"),
	}, request)
	if err != nil {
		t.Fatal(err)
	}
	wantPath := "nested/new.txt"
	if len(created) != 1 || created[0].Name != "Write" ||
		created[0].Input["file_path"] != wantPath || created[0].Input["content"] != "created\n" {
		t.Fatalf("Claude Write call = %+v", created)
	}

	modified, err := builder(fsmirror.BuildSnapshot{
		Change: workerkit.Change{ID: "modify", Path: "nested/new.txt", Kind: workerkit.ChangeModify},
		Before: []byte("created\n"), After: []byte("modified\n"),
	}, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(modified) != 1 || modified[0].Name != "Edit" ||
		modified[0].Input["file_path"] != wantPath || modified[0].Input["old_string"] != "created\n" ||
		modified[0].Input["new_string"] != "modified\n" {
		t.Fatalf("Claude Edit call = %+v", modified)
	}
}

func TestCodexApplyPatchBuilderUsesFreeformProjectRelativePatches(t *testing.T) {
	builder := fsmirror.CodexApplyPatchBuilder()
	request := workerkit.MirrorResolve{
		Tools: []llm.Tool{{Name: "apply_patch", InputKind: llm.ToolInputText}},
	}
	tests := []struct {
		name     string
		snapshot fsmirror.BuildSnapshot
		want     string
	}{
		{
			name: "create",
			snapshot: fsmirror.BuildSnapshot{
				Change: workerkit.Change{
					ID: "create", Path: "nested/new.txt", Kind: workerkit.ChangeCreate,
				},
				After: []byte("first\nsecond\n"),
			},
			want: "*** Begin Patch\n*** Add File: nested/new.txt\n+first\n+second\n*** End Patch",
		},
		{
			name: "modify",
			snapshot: fsmirror.BuildSnapshot{
				Change: workerkit.Change{
					ID: "modify", Path: "nested/new.txt", Kind: workerkit.ChangeModify,
				},
				Before: []byte("first\nsecond\n"), After: []byte("first\nchanged\n"),
			},
			want: "*** Begin Patch\n*** Update File: nested/new.txt\n@@\n-first\n-second\n+first\n+changed\n*** End of File\n*** End Patch",
		},
		{
			name: "delete",
			snapshot: fsmirror.BuildSnapshot{
				Change: workerkit.Change{
					ID: "delete", Path: "nested/new.txt", Kind: workerkit.ChangeDelete,
				},
				Before: []byte{0xff, 0xfe},
			},
			want: "*** Begin Patch\n*** Delete File: nested/new.txt\n*** End Patch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls, err := builder(test.snapshot, request)
			if err != nil {
				t.Fatal(err)
			}
			if len(calls) != 1 || calls[0].Name != "apply_patch" ||
				calls[0].Input != nil || calls[0].TextInput == nil ||
				*calls[0].TextInput != test.want {
				t.Fatalf("Codex apply_patch call = %+v", calls)
			}
		})
	}
}

func TestCodexApplyPatchBuilderFailsClosedForLossyInputs(t *testing.T) {
	builder := fsmirror.CodexApplyPatchBuilder()
	request := workerkit.MirrorResolve{
		Tools: []llm.Tool{{Name: "apply_patch", InputKind: llm.ToolInputText}},
	}
	tests := []fsmirror.BuildSnapshot{
		{
			Change: workerkit.Change{ID: "empty", Path: "empty.txt", Kind: workerkit.ChangeCreate},
		},
		{
			Change: workerkit.Change{ID: "newline", Path: "no-newline.txt", Kind: workerkit.ChangeCreate},
			After:  []byte("no newline"),
		},
		{
			Change: workerkit.Change{ID: "binary", Path: "binary.txt", Kind: workerkit.ChangeModify},
			Before: []byte("old\n"), After: []byte{0xff, '\n'},
		},
		{
			Change: workerkit.Change{ID: "path", Path: "bad\n*** Delete File: other", Kind: workerkit.ChangeDelete},
		},
	}
	for _, snapshot := range tests {
		if calls, err := builder(snapshot, request); err == nil || calls != nil {
			t.Fatalf("lossy snapshot produced calls %+v, error %v", calls, err)
		}
	}
	if calls, err := builder(fsmirror.BuildSnapshot{
		Change: workerkit.Change{ID: "wrong-kind", Path: "file.txt", Kind: workerkit.ChangeCreate},
		After:  []byte("hello\n"),
	}, workerkit.MirrorResolve{
		Tools: []llm.Tool{{Name: "apply_patch"}},
	}); err == nil || calls != nil {
		t.Fatalf("JSON apply_patch declaration produced calls %+v, error %v", calls, err)
	}
}

func TestNativeBuilderRegistryDispatchesExactProfileAndFailsClosed(t *testing.T) {
	registry, err := fsmirror.NewNativeBuilderRegistry(
		fsmirror.NativeProfile{HarnessID: "opencode", HarnessVersion: "1.17.18", Build: fsmirror.OpenCodeNativeBuilder()},
		fsmirror.NativeProfile{HarnessID: "claude-code", HarnessVersion: "2.1.217", Build: fsmirror.ClaudeCodeNativeBuilder()},
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := fsmirror.BuildSnapshot{
		Change: workerkit.Change{ID: "create", Path: "profile.txt", Kind: workerkit.ChangeCreate},
		After:  []byte("profile\n"),
	}
	request := workerkit.MirrorResolve{
		HarnessID: "claude-code", HarnessVersion: "2.1.217",
		Tools: []llm.Tool{{Name: "Write"}, {Name: "Edit"}},
	}
	calls, err := registry.Build(snapshot, request)
	if err != nil || len(calls) != 1 || calls[0].Name != "Write" {
		t.Fatalf("exact profile calls = %+v, error %v", calls, err)
	}
	request.HarnessVersion = "2.1.218"
	if _, err := registry.Build(snapshot, request); err == nil ||
		!strings.Contains(err.Error(), "no native file profile") {
		t.Fatalf("unknown profile error = %v", err)
	}
}

func TestNativeBuilderUsesProjectRelativePathWithoutAgentFilesystemKnowledge(t *testing.T) {
	t.Parallel()
	builder := fsmirror.OpenCodeNativeBuilder()
	snapshot := fsmirror.BuildSnapshot{
		Change: workerkit.Change{ID: "remote-path", Path: "src/hello.go", Kind: workerkit.ChangeCreate},
		After:  []byte("package hello\n"),
	}
	calls, err := builder(snapshot, workerkit.MirrorResolve{
		Tools: []llm.Tool{{Name: "write"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].Input["filePath"] != "src/hello.go" {
		t.Fatalf("calls = %+v, want project-relative path", calls)
	}
}
