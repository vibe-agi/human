package callershim

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/callerfs"
)

type cancelAfterBeginLedger struct {
	Ledger
	cancel context.CancelFunc
}

type failCompleteLedger struct {
	Ledger
}

func (ledger failCompleteLedger) Complete(context.Context, ExecutionKey, []byte) (Execution, error) {
	return Execution{}, errors.New("injected completion failure")
}

type completeThenErrorLedger struct {
	Ledger
}

func (ledger completeThenErrorLedger) Complete(
	ctx context.Context,
	key ExecutionKey,
	response []byte,
) (Execution, error) {
	if _, err := ledger.Ledger.Complete(ctx, key, response); err != nil {
		return Execution{}, err
	}
	return Execution{}, errors.New("injected ambiguous commit result")
}

func (ledger cancelAfterBeginLedger) Begin(
	ctx context.Context,
	key ExecutionKey,
	digest string,
) (BeginResult, error) {
	result, err := ledger.Ledger.Begin(ctx, key, digest)
	if err == nil {
		ledger.cancel()
	}
	return result, err
}

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

func TestOversizedReadBecomesSmallDurableToolError(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	fixture.executor.config.MaxReadBytes = 8
	path := filepath.Join(fixture.workspace, "large.txt")
	if err := os.WriteFile(path, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := toolRequest("read-large", "human_read_file", map[string]any{"path": "large.txt"})
	first, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || !first.IsError || first.ErrorCode != "result_too_large" {
		t.Fatalf("oversized read = %+v, %v", first, err)
	}
	if text, ok := first.Content.(string); !ok || !strings.Contains(text, "limit 8") {
		t.Fatalf("oversized read diagnostic = %#v", first.Content)
	}
	// The bounded error, not the oversized bytes, is the immutable result for
	// this call id. A new call id can retry after the file changes.
	if err := os.WriteFile(path, []byte("small"), 0o600); err != nil {
		t.Fatal(err)
	}
	replay, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || !replay.IsError || replay.ErrorCode != "result_too_large" {
		t.Fatalf("oversized read replay = %+v, %v", replay, err)
	}
	retry, err := fixture.executor.Execute(context.Background(),
		toolRequest("read-small", "human_read_file", map[string]any{"path": "large.txt"}))
	if err != nil || retry.IsError {
		t.Fatalf("new bounded read = %+v, %v", retry, err)
	}
}

func TestOversizedSearchResultBecomesSmallDurableToolError(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	fixture.executor.config.MaxResultBytes = 512
	path := filepath.Join(fixture.workspace, "bundle.js")
	if err := os.WriteFile(path, []byte("needle "+strings.Repeat("x", 2048)), 0o600); err != nil {
		t.Fatal(err)
	}
	request := toolRequest("search-large", "human_search", map[string]any{
		"path": ".", "query": "needle", "max_results": 100,
	})
	first, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || !first.IsError || first.ErrorCode != "result_too_large" {
		t.Fatalf("oversized search = %+v, %v", first, err)
	}
	if err := os.WriteFile(path, []byte("needle small"), 0o600); err != nil {
		t.Fatal(err)
	}
	replay, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || !replay.IsError || replay.ErrorCode != "result_too_large" {
		t.Fatalf("oversized search replay = %+v, %v", replay, err)
	}
	retry, err := fixture.executor.Execute(context.Background(),
		toolRequest("search-small", "human_search", map[string]any{
			"path": ".", "query": "needle", "max_results": 100,
		}))
	if err != nil || retry.IsError {
		t.Fatalf("new bounded search = %+v, %v", retry, err)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > fixture.executor.config.MaxResultBytes {
		t.Fatalf("bounded search error encoded to %d bytes", len(encoded))
	}
}

func TestOversizedMutatingResultRequiresReconciliationBeforeRetry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fixture command uses POSIX shell syntax")
	}
	fixture := newExecutorFixture(t, true)
	executor, err := NewExecutor(ExecutorConfig{
		Root: fixture.executor.config.Root, Ledger: fixture.ledger, ExecEnabled: true,
		MaxResultBytes: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := executor.Execute(context.Background(), toolRequest(
		"large-mutating-result", "human_exec", map[string]any{
			"command": "printf applied > side-effect.txt; printf '%04096d' 0",
			"cwd":     "/workspace",
		},
	))
	if err != nil || !response.IsError || response.ErrorCode != "result_too_large" {
		t.Fatalf("oversized mutating result = %+v, %v", response, err)
	}
	message, ok := response.Content.(string)
	if !ok || !strings.Contains(message, "may already have changed external state") ||
		!strings.Contains(message, "reconcile") {
		t.Fatalf("oversized mutating guidance = %#v", response.Content)
	}
	content, err := os.ReadFile(filepath.Join(fixture.workspace, "side-effect.txt"))
	if err != nil || string(content) != "applied" {
		t.Fatalf("command side effect = %q, %v", content, err)
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

func TestPendingExecutionBecomesReplayableIndeterminateAfterRestart(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	request := toolRequest("pending-restart", "human_write_file", map[string]any{
		"path": "pending-restart.txt", "content": "must-not-run", "expected_sha256": callerfs.AbsentFingerprint,
	})
	digest, _, err := requestDigest(request.Name, request.Input)
	if err != nil {
		t.Fatal(err)
	}
	key := ExecutionKey{CallerID: request.CallerID, TaskID: request.TaskID, ToolCallID: request.ToolCallID}
	if _, err := fixture.ledger.Begin(context.Background(), key, digest); err != nil {
		t.Fatal(err)
	}
	if err := fixture.ledger.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenSQLiteLedger(context.Background(), fixture.ledgerDSN)
	if err != nil {
		t.Fatal(err)
	}
	fixture.ledger = ledger
	root, err := callerfs.OpenRoot(fixture.workspace)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutor(ExecutorConfig{Root: root, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	first, err := executor.Execute(context.Background(), request)
	if err != nil || !first.IsError || first.ErrorCode != "execution_outcome_indeterminate" {
		t.Fatalf("recovered pending response = %+v, %v", first, err)
	}
	replay, err := executor.Execute(context.Background(), request)
	if err != nil || replay != first {
		t.Fatalf("indeterminate replay = %+v, %v; want %+v", replay, err, first)
	}
	if _, err := os.Stat(filepath.Join(fixture.workspace, "pending-restart.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered pending execution was rerun: %v", err)
	}
	newCall := request
	newCall.ToolCallID = "pending-reconciled"
	result, err := executor.Execute(context.Background(), newCall)
	if err != nil || result.IsError {
		t.Fatalf("explicit new call after reconciliation = %+v, %v", result, err)
	}
}

func TestCompletionFailureBecomesDurableIndeterminateWithoutRerun(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	executor, err := NewExecutor(ExecutorConfig{
		Root: fixture.executor.config.Root, Ledger: failCompleteLedger{Ledger: fixture.ledger},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := toolRequest("ambiguous-write", "human_write_file", map[string]any{
		"path": "ambiguous.txt", "content": "written-once", "expected_sha256": callerfs.AbsentFingerprint,
	})
	first, err := executor.Execute(context.Background(), request)
	if err != nil || !first.IsError || first.ErrorCode != "execution_outcome_indeterminate" {
		t.Fatalf("completion failure response = %+v, %v", first, err)
	}
	if content, err := os.ReadFile(filepath.Join(fixture.workspace, "ambiguous.txt")); err != nil || string(content) != "written-once" {
		t.Fatalf("mutation result = %q, %v", content, err)
	}
	replay, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || replay != first {
		t.Fatalf("indeterminate replay = %+v, %v; want %+v", replay, err, first)
	}
}

func TestAmbiguousCommitReadsBackCompletedResult(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	executor, err := NewExecutor(ExecutorConfig{
		Root: fixture.executor.config.Root, Ledger: completeThenErrorLedger{Ledger: fixture.ledger},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := toolRequest("ambiguous-commit", "human_write_file", map[string]any{
		"path": "committed-despite-error.txt", "content": "once", "expected_sha256": callerfs.AbsentFingerprint,
	})
	first, err := executor.Execute(context.Background(), request)
	if err != nil || first.IsError {
		t.Fatalf("ambiguous committed response = %+v, %v", first, err)
	}
	replay, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || replay.IsError {
		t.Fatalf("completed replay = %+v, %v", replay, err)
	}
}

func TestExecutedMutationCommitsLedgerAfterRequestCancellation(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	executor, err := NewExecutor(ExecutorConfig{
		Root:   fixture.executor.config.Root,
		Ledger: cancelAfterBeginLedger{Ledger: fixture.ledger, cancel: cancel},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := toolRequest("cancel-after-write", "human_write_file", map[string]any{
		"path": "committed.txt", "content": "once", "expected_sha256": callerfs.AbsentFingerprint,
	})
	first, err := executor.Execute(ctx, request)
	if err != nil || first.IsError {
		t.Fatalf("canceled request result = %+v, %v", first, err)
	}
	if ctx.Err() == nil {
		t.Fatal("test ledger did not cancel request context")
	}
	replay, err := fixture.executor.Execute(context.Background(), request)
	if err != nil || replay.IsError {
		t.Fatalf("durable replay after request cancellation = %+v, %v", replay, err)
	}
}

func TestExecTimeoutKillsDescendantPipeHolders(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix process-group behavior")
	}
	fixture := newExecutorFixture(t, true)
	executor, err := NewExecutor(ExecutorConfig{
		Root: fixture.executor.config.Root, Ledger: fixture.ledger, ExecEnabled: true,
		DefaultTimeout: 100 * time.Millisecond, MaxTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	response, err := executor.Execute(context.Background(), toolRequest(
		"timeout-descendants", "human_exec", map[string]any{
			"command": "sleep 30 & wait", "cwd": "/workspace", "timeout_ms": 100,
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if !response.IsError || response.ErrorCode != "command_failed" {
		t.Fatalf("timeout response = %+v", response)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("command timeout retained a descendant pipe for %s", elapsed)
	}
}
