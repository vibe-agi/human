package mirror

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWatchReportsNestedWorkspaceChangesAndReviewRemainsAuthoritative(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := workspace.Watch(ctx, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(workspace.Dir(), "new", "nested")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(nested, "file.txt")
	if err := os.WriteFile(path, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("mirror watcher did not report nested change")
	}
	changes, err := workspace.Review()
	if err != nil || len(changes) != 1 || changes[0].Path != "new/nested/file.txt" ||
		string(changes[0].NewContent) != "two" {
		t.Fatalf("review after coalesced event = %+v, %v", changes, err)
	}
}

func TestWatchIgnoresGitInternalsAndClosesWithContext(t *testing.T) {
	t.Parallel()
	workspace, err := Open(t.TempDir(), "caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	gitDir := filepath.Join(workspace.Dir(), ".git")
	if err := os.MkdirAll(gitDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	events, err := workspace.Watch(ctx, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "index"), []byte("ignored"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf(".git mutation produced event: %+v", event)
	case <-time.After(80 * time.Millisecond):
	}
	cancel()
	select {
	case _, open := <-events:
		if open {
			t.Fatal("watch event stream remained open after cancellation")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watch event stream did not close after cancellation")
	}
}
