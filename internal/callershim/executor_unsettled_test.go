package callershim

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/callerfs"
)

type failFirstTerminalLedger struct {
	Ledger
	completeCommits bool
	markAlwaysFails bool

	mu            sync.Mutex
	completeCalls int
	markCalls     int
}

func (ledger *failFirstTerminalLedger) Complete(
	ctx context.Context,
	key ExecutionKey,
	response []byte,
) (Execution, error) {
	ledger.mu.Lock()
	ledger.completeCalls++
	commit := ledger.completeCommits
	ledger.mu.Unlock()
	if commit {
		if _, err := ledger.Ledger.Complete(ctx, key, response); err != nil {
			return Execution{}, err
		}
	}
	return Execution{}, errors.New("injected completion acknowledgement failure")
}

func (ledger *failFirstTerminalLedger) MarkIndeterminate(
	ctx context.Context,
	key ExecutionKey,
	response []byte,
) (Execution, error) {
	ledger.mu.Lock()
	ledger.markCalls++
	call := ledger.markCalls
	ledger.mu.Unlock()
	if call == 1 || ledger.markAlwaysFails {
		return Execution{}, errors.New("injected indeterminate persistence failure")
	}
	return ledger.Ledger.MarkIndeterminate(ctx, key, response)
}

func TestExecutorPersistentTerminalStoreFailureReturnsWithoutRerunningTool(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	ledger := &failFirstTerminalLedger{Ledger: fixture.ledger, markAlwaysFails: true}
	executor, err := NewExecutor(ExecutorConfig{
		Root: fixture.executor.config.Root, Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := toolRequest("persistent-terminal-failure", "human_write_file", map[string]any{
		"path": "store-down.txt", "content": "once", "expected_sha256": callerfs.AbsentFingerprint,
	})

	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, executeErr := executor.Execute(ctx, request)
		cancel()
		if executeErr == nil || !strings.Contains(executeErr.Error(), "indeterminate persistence") {
			t.Fatalf("attempt %d persistent terminal error = %v", attempt+1, executeErr)
		}
	}
	if complete, mark := ledger.calls(); complete != 1 || mark != 2 {
		t.Fatalf("persistent failure calls = complete %d, mark %d; tool path was re-entered", complete, mark)
	}
	if content, err := os.ReadFile(filepath.Join(fixture.workspace, "store-down.txt")); err != nil || string(content) != "once" {
		t.Fatalf("persistent failure side effect = %q, %v", content, err)
	}
}

func (ledger *failFirstTerminalLedger) calls() (complete, mark int) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.completeCalls, ledger.markCalls
}

func TestExecutorRetriesIndeterminatePersistenceWithoutRerunningTool(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	ledger := &failFirstTerminalLedger{Ledger: fixture.ledger}
	executor, err := NewExecutor(ExecutorConfig{
		Root: fixture.executor.config.Root, Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := toolRequest("double-persistence-failure", "human_write_file", map[string]any{
		"path": "once.txt", "content": "written-once", "expected_sha256": callerfs.AbsentFingerprint,
	})

	if _, err := executor.Execute(context.Background(), request); err == nil ||
		!stringsContainAll(err.Error(), "completion acknowledgement", "indeterminate persistence") {
		t.Fatalf("first double persistence failure = %v", err)
	}
	path := filepath.Join(fixture.workspace, "once.txt")
	if content, err := os.ReadFile(path); err != nil || string(content) != "written-once" {
		t.Fatalf("first tool side effect = %q, %v", content, err)
	}

	response, err := executor.Execute(context.Background(), request)
	if err != nil || !response.IsError || response.ErrorCode != "execution_outcome_indeterminate" {
		t.Fatalf("current-process recovery response = %+v, %v", response, err)
	}
	if complete, mark := ledger.calls(); complete != 1 || mark != 2 {
		t.Fatalf("persistence calls after recovery = complete %d, mark %d; tool path was re-entered", complete, mark)
	}
	if content, err := os.ReadFile(path); err != nil || string(content) != "written-once" {
		t.Fatalf("tool side effect after recovery = %q, %v", content, err)
	}

	replay, err := executor.Execute(context.Background(), request)
	if err != nil || !reflect.DeepEqual(replay, response) {
		t.Fatalf("durable indeterminate replay = %+v, %v; want %+v", replay, err, response)
	}
}

func TestExecutorRetryReadsCompletedResultAfterAmbiguousCommitAndFailedFallback(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, false)
	ledger := &failFirstTerminalLedger{Ledger: fixture.ledger, completeCommits: true}
	executor, err := NewExecutor(ExecutorConfig{
		Root: fixture.executor.config.Root, Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := toolRequest("committed-before-double-failure", "human_write_file", map[string]any{
		"path": "committed.txt", "content": "durable", "expected_sha256": callerfs.AbsentFingerprint,
	})

	if _, err := executor.Execute(context.Background(), request); err == nil ||
		!stringsContainAll(err.Error(), "completion acknowledgement", "indeterminate persistence") {
		t.Fatalf("ambiguous committed first result = %v", err)
	}
	path := filepath.Join(fixture.workspace, "committed.txt")
	if err := os.WriteFile(path, []byte("caller-changed-after-commit"), 0o600); err != nil {
		t.Fatal(err)
	}

	completed, err := executor.Execute(context.Background(), request)
	if err != nil || completed.IsError {
		t.Fatalf("completed read-back after ambiguous commit = %+v, %v", completed, err)
	}
	if complete, mark := ledger.calls(); complete != 1 || mark != 2 {
		t.Fatalf("persistence calls after completed read-back = complete %d, mark %d", complete, mark)
	}
	if content, err := os.ReadFile(path); err != nil || string(content) != "caller-changed-after-commit" {
		t.Fatalf("completed read-back reran tool: %q, %v", content, err)
	}

	replay, err := executor.Execute(context.Background(), request)
	if err != nil || !reflect.DeepEqual(replay, completed) {
		t.Fatalf("completed durable replay = %+v, %v; want %+v", replay, err, completed)
	}
}

func stringsContainAll(value string, fragments ...string) bool {
	for _, fragment := range fragments {
		if !strings.Contains(value, fragment) {
			return false
		}
	}
	return true
}
