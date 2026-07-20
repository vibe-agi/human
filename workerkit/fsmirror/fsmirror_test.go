package fsmirror_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
	"github.com/vibe-agi/human/workerkit/fsmirror"
)

func writeBuilder(change workerkit.Change, content []byte, _ workerkit.MirrorResolve) ([]llm.ToolCall, error) {
	return []llm.ToolCall{{
		ID: "call-" + change.ID, Name: "write",
		Input: map[string]any{"filePath": change.Path, "content": string(content)},
	}}, nil
}

func openMirror(t *testing.T, root string, baseline string) *fsmirror.Mirror {
	t.Helper()
	mirror, err := fsmirror.Open(t.Context(), fsmirror.Config{
		Root: root, Build: writeBuilder, BaselineFile: baseline, Debounce: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mirror.Close(); err != nil {
			t.Errorf("close mirror: %v", err)
		}
	})
	return mirror
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

	calls, err := mirror.Resolve(t.Context(), workerkit.MirrorResolve{
		ChangeIDs: []string{change.ID},
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
		Root: root, Build: writeBuilder, BaselineFile: baseline, Debounce: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Deliver one file so the baseline is persisted, then edit it again and
	// crash (close) before delivering the second edit.
	if err := os.WriteFile(filepath.Join(root, "kept.txt"), []byte("v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review := nextReview(t, first, func(review workerkit.Review) bool { return len(review.Changes) == 1 })
	if _, err := first.Resolve(t.Context(), workerkit.MirrorResolve{
		ChangeIDs: []string{review.Changes[0].ID},
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
