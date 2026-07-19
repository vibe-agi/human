package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
)

const (
	llmStoreCrashChildEnv = "HUMAN_LLM_SQLITE_STORE_CRASH_CHILD"
	llmStoreCrashPathEnv  = "HUMAN_LLM_SQLITE_STORE_CRASH_PATH"
	llmStoreCrashExitCode = 93
)

func TestStoreConformance(t *testing.T) {
	humantest.TestLLMStore(t, func(
		ctx context.Context,
		test testing.TB,
	) (llm.Store, framework.ReleaseFunc, error) {
		resource, err := llmsqlite.Open(ctx, llmsqlite.Config{
			Path: filepath.Join(test.TempDir(), "llm.db"),
		})
		if err != nil {
			return nil, nil, err
		}
		store, err := resource.Value()
		if err != nil {
			_ = resource.Release(context.Background())
			return nil, nil, err
		}
		return store, resource.Release, nil
	})
}

func TestStoreCommitUnknownReconciliation(t *testing.T) {
	humantest.TestLLMServiceCommitUnknownReconciliation(t, func(
		ctx context.Context,
		test testing.TB,
	) (llm.Store, framework.ReleaseFunc, error) {
		resource, err := llmsqlite.Open(ctx, llmsqlite.Config{
			Path: filepath.Join(test.TempDir(), "llm.db"),
		})
		if err != nil {
			return nil, nil, err
		}
		store, err := resource.Value()
		if err != nil {
			_ = resource.Release(context.Background())
			return nil, nil, err
		}
		return store, resource.Release, nil
	})
}

func TestOwnedResourcePersistsAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "llm.db")
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if !resource.Owned() {
		t.Fatal("SQLite Store resource is borrowed")
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	binding := llm.StoreBinding{DeploymentID: "deployment-persistent"}
	if err := store.Bind(t.Context(), binding); err != nil {
		t.Fatalf("bind SQLite Store: %v", err)
	}
	task, request := sqliteFixture(t)
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertRequest(request); err != nil {
			return err
		}
		if err := tx.InsertResponseEvent(llm.StoreResponseEventRecord{
			Request: request.Key, Sequence: 1, Kind: llm.StoreEventWire,
			Worker:        "worker-a",
			WorkerEventID: "worker-event-a", WorkerEventDigest: "event-digest-a",
			Data: []byte("data: first\n\n"), CreatedAt: task.CreatedAt.Add(time.Second),
		}); err != nil {
			return err
		}
		return tx.InsertWorkerReceipt(llm.StoreWorkerReceiptRecord{
			Request: request.Key, EventID: "worker-event-a", Worker: "worker-a",
			Digest: "event-digest-a", CreatedAt: task.CreatedAt.Add(2 * time.Second),
		})
	}); err != nil {
		t.Fatalf("seed SQLite Store: %v", err)
	}
	if second, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path}); !errors.Is(err, llmsqlite.ErrDatabaseInUse) {
		if err == nil {
			_ = second.Release(t.Context())
		}
		t.Fatalf("second owner error = %v, want ErrDatabaseInUse", err)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := resource.Value(); !errors.Is(err, framework.ErrResourceReleased) {
		t.Fatalf("Value after release = %v", err)
	}
	if err := store.View(t.Context(), func(llm.StoreView) error { return nil }); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("retained Store after release = %v", err)
	}

	reopened, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path})
	if err != nil {
		t.Fatalf("reopen Store: %v", err)
	}
	defer reopened.Release(context.Background())
	reopenedStore, err := reopened.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedStore.Bind(t.Context(), binding); err != nil {
		t.Fatalf("exact binding after reopen: %v", err)
	}
	if err := reopenedStore.Bind(t.Context(), llm.StoreBinding{DeploymentID: "deployment-other"}); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("divergent binding after reopen = %v, want ErrStoreConflict", err)
	}
	if err := reopenedStore.View(t.Context(), func(view llm.StoreView) error {
		got, err := view.LoadRequest(request.Key, llm.StoreReadLimit{MaxBytes: 1 << 20})
		if err != nil {
			return err
		}
		if !bytes.Equal(got.CanonicalPayload, request.CanonicalPayload) || got.RequestID != request.RequestID {
			t.Fatalf("reopened request = %#v", got)
		}
		events, err := view.ScanResponseEvents(llm.StoreResponseEventScan{
			Request: request.Key, Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: 1 << 20},
		})
		if err != nil || len(events) != 1 || string(events[0].Data) != "data: first\n\n" {
			t.Fatalf("reopened events = %#v, %v", events, err)
		}
		receipt, err := view.LoadWorkerReceipt(request.Key, "worker-event-a")
		if err != nil || receipt.Digest != "event-digest-a" {
			t.Fatalf("reopened receipt = %#v, %v", receipt, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRecoversCommittedTransactionAfterAbruptProcessExit(t *testing.T) {
	if os.Getenv(llmStoreCrashChildEnv) == "1" {
		if err := seedLLMStoreBeforeCrash(t, os.Getenv(llmStoreCrashPathEnv)); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(llmStoreCrashExitCode + 1)
		}
		os.Exit(llmStoreCrashExitCode) // intentionally bypass Resource.Release
	}

	path := filepath.Join(t.TempDir(), "llm-crash.db")
	command := exec.Command(os.Args[0], "-test.run=^TestStoreRecoversCommittedTransactionAfterAbruptProcessExit$", "-test.count=1")
	command.Env = append(os.Environ(), llmStoreCrashChildEnv+"=1", llmStoreCrashPathEnv+"="+path)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != llmStoreCrashExitCode {
		t.Fatalf("crash child = %v (exit %d), want %d; output:\n%s", err, llmStoreProcessExitCode(exit), llmStoreCrashExitCode, output)
	}

	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path})
	if err != nil {
		t.Fatalf("open HumanLLM Store after process crash: %v", err)
	}
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Bind(t.Context(), llm.StoreBinding{DeploymentID: "deployment-crash"}); err != nil {
		t.Fatalf("durable binding after process crash: %v", err)
	}
	wantTask, wantRequest := sqliteFixture(t)
	if err := store.View(t.Context(), func(view llm.StoreView) error {
		task, err := view.LoadTask(wantTask.Key)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(task, wantTask) {
			return fmt.Errorf("task after crash = %#v, want %#v", task, wantTask)
		}
		request, err := view.LoadRequest(wantRequest.Key, llm.StoreReadLimit{MaxBytes: 1 << 20})
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(request, wantRequest) {
			return fmt.Errorf("request after crash = %#v, want %#v", request, wantRequest)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func seedLLMStoreBeforeCrash(t *testing.T, path string) error {
	resource, err := llmsqlite.Open(context.Background(), llmsqlite.Config{Path: path})
	if err != nil {
		return err
	}
	store, err := resource.Value()
	if err != nil {
		return err
	}
	if err := store.Bind(context.Background(), llm.StoreBinding{DeploymentID: "deployment-crash"}); err != nil {
		return err
	}
	task, request := sqliteFixture(t)
	return store.Update(context.Background(), func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		return tx.InsertRequest(request)
	})
}

func llmStoreProcessExitCode(err *exec.ExitError) int {
	if err == nil {
		return 0
	}
	return err.ExitCode()
}

func TestUpdateRollbackAndEscapedTransaction(t *testing.T) {
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Release(context.Background())
	store, _ := resource.Value()
	task, _ := sqliteFixture(t)
	abort := errors.New("abort")
	var escaped llm.StoreTx
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		escaped = tx
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatalf("rollback error = %v", err)
	}
	if _, err := escaped.LoadTask(task.Key); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("escaped transaction error = %v", err)
	}
	if err := store.View(t.Context(), func(view llm.StoreView) error {
		_, err := view.LoadTask(task.Key)
		return err
	}); !errors.Is(err, llm.ErrStoreRecordNotFound) {
		t.Fatalf("rolled-back Task = %v", err)
	}
}

func TestOpenSecuresFileAndRejectsForeignSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private.db")
	if err := os.WriteFile(path, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("database mode = %04o, want 0600", got)
		}
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}

	foreign := filepath.Join(t.TempDir(), "foreign.db")
	database, err := sql.Open("sqlite", foreign)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE foreign_state (id INTEGER PRIMARY KEY)`); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if opened, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: foreign}); !errors.Is(err, llmsqlite.ErrUnsupportedSchema) {
		if err == nil {
			_ = opened.Release(t.Context())
		}
		t.Fatalf("foreign schema error = %v, want ErrUnsupportedSchema", err)
	}
}

func TestRecoveryReceiptRetentionAndTombstoneRoundTrip(t *testing.T) {
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Release(context.Background())
	store, _ := resource.Value()
	task, request := sqliteFixture(t)
	eventTime := task.CreatedAt.Add(time.Second)
	tool := llm.StoreToolExecutionRecord{
		Key:         llm.StoreToolExecutionKey{Task: task.Key, ToolCallID: "tool-a"},
		InputDigest: "tool-input-a", State: llm.ToolExecutionPending,
		Revision: 1, CreatedAt: eventTime,
	}
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertRequest(request); err != nil {
			return err
		}
		if err := tx.InsertResponseEvent(llm.StoreResponseEventRecord{
			Request: request.Key, Sequence: 1, Kind: llm.StoreEventCheckpoint,
			Worker:        "worker-a",
			WorkerEventID: "event-a", WorkerEventDigest: "digest-a",
			Data: []byte("checkpoint"), CreatedAt: eventTime,
		}); err != nil {
			return err
		}
		if err := tx.InsertResponseEvent(llm.StoreResponseEventRecord{
			Request: request.Key, Sequence: 2, Kind: llm.StoreEventWire,
			Worker:        "worker-a",
			WorkerEventID: "event-a", WorkerEventDigest: "digest-a",
			Data: []byte("wire"), CreatedAt: eventTime,
		}); err != nil {
			return err
		}
		return tx.InsertToolExecution(tool)
	}); err != nil {
		t.Fatalf("seed recovery records: %v", err)
	}
	if err := store.View(t.Context(), func(view llm.StoreView) error {
		recovery, err := view.ScanRecovery(llm.StoreRecoveryScan{
			Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: 1 << 20},
		})
		if err != nil || len(recovery) != 1 || recovery[0].Request.Key != request.Key {
			t.Fatalf("initial recovery scan = %#v, %v", recovery, err)
		}
		tools, err := view.ScanToolExecutions(llm.StoreToolExecutionScan{
			Task: task.Key, Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: 1 << 20},
		})
		if err != nil || len(tools) != 1 || tools[0].Key != tool.Key {
			t.Fatalf("tool scan = %#v, %v", tools, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	completedAt := eventTime.Add(time.Second)
	completedRequest := request
	completedRequest.Decision = llm.StoreResponseDecision{
		StatusCode: 200, ContentType: "text/event-stream", Body: []byte("done"),
	}
	completedRequest.ResponseComplete = true
	completedRequest.LastEventSequence = 2
	completedRequest.Revision = 2
	completedRequest.CompletedAt = &completedAt
	completedTool := tool
	completedTool.State = llm.ToolExecutionCompleted
	completedTool.Result = []byte(`{"ok":true}`)
	completedTool.IsError = false
	completedTool.Revision = 2
	completedTool.CompletedAt = &completedAt
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		changed, err := tx.CompareAndSwapRequest(llm.StoreRequestMutation{
			Key: request.Key, ExpectedRevision: 1, Next: completedRequest,
		})
		if err != nil || !changed {
			return errors.Join(errors.New("complete request CAS did not change"), err)
		}
		changed, err = tx.CompareAndSwapToolExecution(llm.StoreToolExecutionMutation{
			Key: tool.Key, ExpectedRevision: 1, Next: completedTool,
		})
		if err != nil || !changed {
			return errors.Join(errors.New("complete tool CAS did not change"), err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.View(t.Context(), func(view llm.StoreView) error {
		candidates, err := view.ScanRetention(llm.StoreRetentionScan{
			CompletedBefore: completedAt.Add(time.Second), Limit: 10,
		})
		if err != nil || len(candidates) != 1 || !candidates[0].UnacknowledgedWorkerEvent {
			t.Fatalf("unacknowledged retention = %#v, %v", candidates, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		return tx.InsertWorkerReceipt(llm.StoreWorkerReceiptRecord{
			Request: request.Key, EventID: "event-a", Worker: "worker-a",
			Digest: "digest-a", CreatedAt: completedAt,
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.View(t.Context(), func(view llm.StoreView) error {
		recovery, err := view.ScanRecovery(llm.StoreRecoveryScan{
			Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: 1 << 20},
		})
		if err != nil || len(recovery) != 0 {
			t.Fatalf("settled recovery scan = %#v, %v", recovery, err)
		}
		candidates, err := view.ScanRetention(llm.StoreRetentionScan{
			CompletedBefore: completedAt.Add(time.Second), Limit: 10,
		})
		if err != nil || len(candidates) != 1 || candidates[0].UnacknowledgedWorkerEvent {
			t.Fatalf("settled retention = %#v, %v", candidates, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	prunedAt := completedAt.Add(2 * time.Second)
	pruned := completedRequest
	pruned.CanonicalPayload = nil
	pruned.Decision.Body = nil
	pruned.PayloadPrunedAt = &prunedAt
	pruned.Revision = 3
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		changed, err := tx.CompareAndSwapRequest(llm.StoreRequestMutation{
			Key: request.Key, ExpectedRevision: 2, Next: pruned,
		})
		if err != nil || !changed {
			return errors.Join(errors.New("prune request CAS did not change"), err)
		}
		deleted, err := tx.DeleteTombstonedResponseEvents(request.Key)
		if err != nil || deleted != 2 {
			return errors.Join(errors.New("delete tombstoned events count is not two"), err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.View(t.Context(), func(view llm.StoreView) error {
		events, err := view.ScanResponseEvents(llm.StoreResponseEventScan{
			Request: request.Key, Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: 1 << 20},
		})
		if err != nil || len(events) != 0 {
			t.Fatalf("events after prune = %#v, %v", events, err)
		}
		if _, err := view.LoadWorkerReceipt(request.Key, "event-a"); err != nil {
			t.Fatalf("receipt did not survive prune: %v", err)
		}
		got, err := view.LoadRequest(request.Key, llm.StoreReadLimit{MaxBytes: 1})
		if err != nil || got.PayloadPrunedAt == nil || got.CanonicalPayload != nil || got.Decision.Body != nil {
			t.Fatalf("tombstoned request = %#v, %v", got, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func sqliteFixture(t *testing.T) (llm.StoreTaskRecord, llm.StoreRequestRecord) {
	t.Helper()
	description := llm.CodecDescription{
		Contract: framework.Contract{ID: llm.CodecContractID, Major: llm.CodecContractMajor},
		ID:       "test.chat", Version: "1.0.0", Fingerprint: llm.Fingerprint([]byte("test-codec-v1")),
		Limits: llm.CodecLimits{
			MaxRequestBytes: 1 << 20, MaxStreamFrameBytes: 1 << 20,
			MaxStreamFramesPerStep: 32, MaxAggregateBytes: 1 << 20,
			MaxAdmissionErrorBytes: 64 << 10,
		},
		OverloadedStatus: 503,
	}
	codec, err := llm.NewCodecSnapshot(description)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 19, 4, 5, 6, 7, time.UTC)
	task := llm.StoreTaskRecord{
		Key:          llm.StoreTaskKey{Caller: "caller-a", Task: "task-a"},
		WorkspaceKey: "workspace-a", CapabilityTier: llm.TierRemoteTools, Codec: codec,
		HarnessID: "codex", HarnessVersion: "1", HarnessSessionID: "session-a",
		WorkspaceRoot: "/workspace", State: llm.TaskAwaitingCaller,
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	request := llm.StoreRequestRecord{
		Key:  llm.StoreRequestKey{Caller: task.Key.Caller, IdempotencyKey: "idempotency-a"},
		Task: task.Key, RequestID: "request-a", ResponseID: "response-a",
		RequestDigest: "request-digest-a", Codec: codec, Mode: llm.ResponseStream,
		CanonicalPayload: []byte(`{"model":"human"}`), Revision: 1, CreatedAt: now,
	}
	return task, request
}
