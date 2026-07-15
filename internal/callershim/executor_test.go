package callershim

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/internal/callerfs"
)

type executorFixture struct {
	executor  *Executor
	ledger    *SQLiteLedger
	workspace string
	ledgerDSN string
}

func newExecutorFixture(t *testing.T, execEnabled bool) *executorFixture {
	t.Helper()
	workspace := t.TempDir()
	root, err := callerfs.OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	dsn := filepath.Join(t.TempDir(), "caller-ledger.db")
	ledger, err := OpenSQLiteLedger(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutor(ExecutorConfig{Root: root, Ledger: ledger, ExecEnabled: execEnabled})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ledger.Close() })
	return &executorFixture{executor: executor, ledger: ledger, workspace: workspace, ledgerDSN: dsn}
}

func toolRequest(id, name string, input any) ToolRequest {
	payload, _ := json.Marshal(input)
	return ToolRequest{CallerID: "caller-1", TaskID: "task-1", ToolCallID: id, Name: name, Input: payload}
}

func TestMutationReplayNeverExecutesTwice(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	request := toolRequest("write-1", "human_write_file", map[string]any{
		"path": "/workspace/file.txt", "content": "first", "expected_sha256": callerfs.AbsentFingerprint,
	})
	first, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || first.IsError {
		t.Fatalf("first response = %+v, %v", first, err)
	}
	path := filepath.Join(fixture.workspace, "file.txt")
	if err := os.WriteFile(path, []byte("concurrent user change"), 0o600); err != nil {
		t.Fatal(err)
	}
	replay, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || replay.IsError {
		t.Fatalf("replay response = %+v, %v", replay, err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "concurrent user change" {
		t.Fatalf("replay executed the write again: %q", content)
	}

	changed := toolRequest("write-1", "human_write_file", map[string]any{
		"path": "/workspace/file.txt", "content": "different", "expected_sha256": callerfs.AbsentFingerprint,
	})
	if _, err := fixture.executor.Execute(context.Background(), changed); !errors.Is(err, ErrExecutionReplay) {
		t.Fatalf("same id with different input error = %v", err)
	}
}

func TestFailedCASResultIsDurableAndReplayable(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	path := filepath.Join(fixture.workspace, "file.txt")
	if err := os.WriteFile(path, []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := toolRequest("edit-1", "human_edit_file", map[string]any{
		"path": "file.txt", "old_string": "current", "new_string": "new", "expected_sha256": "sha256:stale",
	})
	first, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || !first.IsError || first.ErrorCode != "cas_mismatch" {
		t.Fatalf("first response = %+v, %v", first, err)
	}
	if err := os.WriteFile(path, []byte("new caller state"), 0o600); err != nil {
		t.Fatal(err)
	}
	replay, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || !replay.IsError || replay.ErrorCode != "cas_mismatch" {
		t.Fatalf("replay response = %+v, %v", replay, err)
	}
	content, _ := os.ReadFile(path)
	if string(content) != "new caller state" {
		t.Fatalf("failed CAS replay changed the file: %q", content)
	}
}

func TestLedgerSurvivesProcessRestart(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	request := toolRequest("write-restart", "human_write_file", map[string]any{
		"path": "restart.txt", "content": "once", "expected_sha256": callerfs.AbsentFingerprint,
	})
	if response, err := fixture.executor.Execute(context.Background(), request); err != nil || response.IsError {
		t.Fatalf("initial response = %+v, %v", response, err)
	}
	if err := fixture.ledger.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenSQLiteLedger(context.Background(), fixture.ledgerDSN)
	if err != nil {
		t.Fatal(err)
	}
	fixture.ledger = ledger
	root, _ := callerfs.OpenRoot(fixture.workspace)
	executor, err := NewExecutor(ExecutorConfig{Root: root, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	if response, err := executor.Execute(context.Background(), request); err != nil || response.IsError {
		t.Fatalf("restart replay = %+v, %v", response, err)
	}
}

func TestReadSearchAndExecDefaultClosed(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	if err := os.MkdirAll(filepath.Join(fixture.workspace, "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.workspace, "src", "main.txt"), []byte("line\nneedle\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	read, err := fixture.executor.Execute(context.Background(), toolRequest("read-1", "human_read_file", map[string]any{
		"path": "/workspace/src/main.txt",
	}))
	if err != nil || read.IsError {
		t.Fatalf("read = %+v, %v", read, err)
	}
	search, err := fixture.executor.Execute(context.Background(), toolRequest("search-1", "human_search", map[string]any{
		"path": "/workspace", "query": "needle",
	}))
	if err != nil || search.IsError {
		t.Fatalf("search = %+v, %v", search, err)
	}
	execResponse, err := fixture.executor.Execute(context.Background(), toolRequest("exec-1", "human_exec", map[string]any{
		"command": "touch should-not-exist", "cwd": "/workspace",
	}))
	if err != nil || !execResponse.IsError {
		t.Fatalf("disabled exec = %+v, %v", execResponse, err)
	}
	if _, err := os.Stat(filepath.Join(fixture.workspace, "should-not-exist")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disabled command executed: %v", err)
	}
}

func TestPendingExecutionFailsClosed(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	request := toolRequest("pending-1", "human_write_file", map[string]any{
		"path": "pending.txt", "content": "never", "expected_sha256": callerfs.AbsentFingerprint,
	})
	digest, _, err := requestDigest(request.Name, request.Input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.ledger.Begin(context.Background(), ExecutionKey{
		CallerID: request.CallerID, TaskID: request.TaskID, ToolCallID: request.ToolCallID,
	}, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.Execute(context.Background(), request); !errors.Is(err, ErrExecutionPending) {
		t.Fatalf("pending replay error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.workspace, "pending.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending execution was rerun: %v", err)
	}
}
