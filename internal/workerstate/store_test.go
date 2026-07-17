package workerstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
)

func TestStoreRestartUpsertListAndDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "private", "worker-state.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	chat := testScope("chat-session", completion.TierChat)
	remote := Scope{
		CallerID: "caller-1", WorkspaceKey: "workspace-1", TaskID: "task-1",
		SessionKey: "caller-1\x00remote-key", Tier: completion.TierRemoteTools,
	}
	first, err := store.Put(ctx, chat, "reply_draft", json.RawMessage(`{"text":"first"}`))
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"text":"replacement"}`)
	replaced, err := store.Put(ctx, chat, "reply_draft", payload)
	if err != nil {
		t.Fatal(err)
	}
	payload[9] = 'X'
	if string(replaced.Payload) != `{"text":"replacement"}` {
		t.Fatalf("Put retained caller payload: %q", replaced.Payload)
	}
	if replaced.UpdatedAt.Before(first.UpdatedAt) {
		t.Fatalf("replacement timestamp moved backwards: %v then %v", first.UpdatedAt, replaced.UpdatedAt)
	}
	if _, err := store.Put(ctx, remote, "continuation", json.RawMessage(`{"accepted":true}`)); err != nil {
		t.Fatal(err)
	}

	beforeRestart, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeRestart) != 2 {
		t.Fatalf("records before restart = %d, want 2: %+v", len(beforeRestart), beforeRestart)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	afterRestart, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterRestart) != 2 {
		t.Fatalf("records after restart = %d, want 2: %+v", len(afterRestart), afterRestart)
	}
	assertRecord(t, afterRestart, chat, "reply_draft", `{"text":"replacement"}`)
	assertRecord(t, afterRestart, remote, "continuation", `{"accepted":true}`)
	for _, record := range afterRestart {
		if record.UpdatedAt.IsZero() || record.UpdatedAt.Location() != time.UTC {
			t.Fatalf("invalid persisted time: %v (%v)", record.UpdatedAt, record.UpdatedAt.Location())
		}
	}

	if err := store.Delete(ctx, chat, "reply_draft"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, chat, "reply_draft"); err != nil {
		t.Fatalf("idempotent Delete: %v", err)
	}
	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Kind != "continuation" {
		t.Fatalf("records after delete = %+v", records)
	}
}

func TestStorePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode bits are not portable to Windows")
	}
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	directory := filepath.Join(root, "state")
	path := filepath.Join(directory, "worker.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	assertMode(t, directory, 0o700)
	assertMode(t, path, 0o600)

	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	store, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	assertMode(t, path, 0o600)

	insecure := filepath.Join(root, "insecure")
	if err := os.Mkdir(insecure, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(insecure, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, filepath.Join(insecure, "worker.db")); err == nil {
		t.Fatal("Open accepted a non-private parent directory")
	}
	assertMode(t, insecure, 0o755)
}

func TestStoreConcurrentUpserts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "state", "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	scope := testScope("shared-session", completion.TierWorkspace)
	const writers = 48
	errorsByWriter := make(chan error, writers*2)
	var wait sync.WaitGroup
	for index := 0; index < writers; index++ {
		index := index
		wait.Add(2)
		go func() {
			defer wait.Done()
			payload := json.RawMessage(fmt.Sprintf(`{"writer":%d}`, index))
			if _, err := store.Put(ctx, scope, "reply_draft", payload); err != nil {
				errorsByWriter <- err
			}
		}()
		go func() {
			defer wait.Done()
			kind := fmt.Sprintf("task_draft_%02d", index)
			payload := json.RawMessage(fmt.Sprintf(`{"writer":%d}`, index))
			if _, err := store.Put(ctx, scope, kind, payload); err != nil {
				errorsByWriter <- err
			}
		}()
	}
	wait.Wait()
	close(errorsByWriter)
	for err := range errorsByWriter {
		t.Errorf("concurrent Put: %v", err)
	}
	if t.Failed() {
		return
	}

	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != writers+1 {
		t.Fatalf("records = %d, want %d", len(records), writers+1)
	}
	var shared int
	for _, record := range records {
		if !json.Valid(record.Payload) {
			t.Fatalf("invalid concurrent payload: %q", record.Payload)
		}
		if record.Kind == "reply_draft" {
			shared++
		}
	}
	if shared != 1 {
		t.Fatalf("same-key rows = %d, want 1", shared)
	}
}

func TestListIsolatesCorruptRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	goodBefore := testScope("good-before", completion.TierChat)
	goodAfter := testScope("good-after", completion.TierChat)
	if _, err := store.Put(ctx, goodBefore, "reply", json.RawMessage(`{"ok":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO worker_state
		(caller_id, workspace_key, task_id, session_key, tier, kind, payload, updated_at)
		VALUES
		('caller-corrupt-json', '', '', 'bad-json', 'chat', 'reply', x'7b', 2),
		('caller-corrupt-tier', '', '', 'bad-tier', 'administrator', 'reply', x'7b7d', 3),
		('caller-corrupt-time', '', '', 'bad-time', 'chat', 'reply', x'7b7d', 'never')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, goodAfter, "reply", json.RawMessage(`{"ok":2}`)); err != nil {
		t.Fatal(err)
	}

	records, err := store.List(ctx)
	var corrupt *CorruptRecordsError
	if !errors.As(err, &corrupt) {
		t.Fatalf("List error = %T %v, want CorruptRecordsError", err, err)
	}
	if len(corrupt.Records) != 3 {
		t.Fatalf("corrupt rows = %d, want 3: %+v", len(corrupt.Records), corrupt.Records)
	}
	if len(records) != 2 {
		t.Fatalf("healthy records = %d, want 2: %+v", len(records), records)
	}
	assertRecord(t, records, goodBefore, "reply", `{"ok":1}`)
	assertRecord(t, records, goodAfter, "reply", `{"ok":2}`)
}

func TestStoreRejectsInvalidInputWithoutMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if _, err := Open(ctx, ""); err == nil {
		t.Fatal("Open accepted an empty path")
	}
	if _, err := Open(nil, ":memory:"); err == nil {
		t.Fatal("Open accepted a nil context")
	}
	store, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	valid := testScope("session", completion.TierChat)
	tests := []struct {
		name    string
		scope   Scope
		kind    string
		payload json.RawMessage
	}{
		{name: "caller", scope: Scope{SessionKey: "session", Tier: completion.TierChat}, kind: "reply", payload: json.RawMessage(`{}`)},
		{name: "session", scope: Scope{CallerID: "caller", Tier: completion.TierChat}, kind: "reply", payload: json.RawMessage(`{}`)},
		{name: "tier empty", scope: Scope{CallerID: "caller", SessionKey: "session"}, kind: "reply", payload: json.RawMessage(`{}`)},
		{name: "tier unknown", scope: Scope{CallerID: "caller", SessionKey: "session", Tier: "admin"}, kind: "reply", payload: json.RawMessage(`{}`)},
		{name: "tier noncanonical", scope: Scope{CallerID: "caller", SessionKey: "session", Tier: "CHAT"}, kind: "reply", payload: json.RawMessage(`{}`)},
		{name: "remote workspace", scope: Scope{CallerID: "caller", TaskID: "task", SessionKey: "session", Tier: completion.TierRemoteTools}, kind: "reply", payload: json.RawMessage(`{}`)},
		{name: "remote task", scope: Scope{CallerID: "caller", WorkspaceKey: "workspace", SessionKey: "session", Tier: completion.TierRemoteTools}, kind: "reply", payload: json.RawMessage(`{}`)},
		{name: "kind", scope: valid, payload: json.RawMessage(`{}`)},
		{name: "payload empty", scope: valid, kind: "reply"},
		{name: "payload invalid", scope: valid, kind: "reply", payload: json.RawMessage(`{`)},
		{name: "workspace whitespace", scope: Scope{CallerID: "caller", WorkspaceKey: " ", SessionKey: "session", Tier: completion.TierChat}, kind: "reply", payload: json.RawMessage(`{}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.Put(ctx, test.scope, test.kind, test.payload); err == nil {
				t.Fatal("Put accepted invalid input")
			}
		})
	}
	records, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("invalid Put mutated store: %+v", records)
	}
}

func testScope(session string, tier completion.CapabilityTier) Scope {
	scope := Scope{CallerID: "caller-1", SessionKey: "caller-1\x00" + session, Tier: tier}
	if tier != completion.TierChat {
		scope.WorkspaceKey = "workspace-1"
		scope.TaskID = "task-1"
	}
	return scope
}

func assertRecord(t *testing.T, records []Record, scope Scope, kind, payload string) {
	t.Helper()
	for _, record := range records {
		if record.Scope == scope && record.Kind == kind {
			if string(record.Payload) != payload {
				t.Fatalf("record %s payload = %q, want %q", kind, record.Payload, payload)
			}
			return
		}
	}
	t.Fatalf("record not found: scope=%+v kind=%q in %+v", scope, kind, records)
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %04o, want %04o", path, got, want)
	}
}
