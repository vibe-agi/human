package customstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/examples/custom-framework/customstore"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
)

const (
	customCrashChildEnv = "HUMAN_CUSTOM_STORE_CRASH_CHILD"
	customCrashPathEnv  = "HUMAN_CUSTOM_STORE_CRASH_PATH"
	customCrashExitCode = 91
)

func TestStoreConformance(t *testing.T) {
	humantest.TestLLMStore(t, func(
		ctx context.Context,
		test testing.TB,
	) (llm.Store, framework.ReleaseFunc, error) {
		resource, err := customstore.Open(ctx, fileConfig(
			filepath.Join(test.TempDir(), "custom-store.snapshot"),
		))
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
		resource, err := customstore.Open(ctx, fileConfig(
			filepath.Join(test.TempDir(), "custom-store.snapshot"),
		))
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

func TestFileStoreReleaseAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable.snapshot")
	firstResource, err := customstore.Open(t.Context(), fileConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	first, err := firstResource.Value()
	if err != nil {
		t.Fatal(err)
	}
	binding := llm.StoreBinding{DeploymentID: "deployment-reopen"}
	if err := first.Bind(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	task := durableTask()
	request := durableRequest(task)
	event := llm.StoreResponseEventRecord{
		Request: request.Key, Sequence: 1, Kind: llm.StoreEventWire,
		Worker: "worker-reopen", WorkerEventID: "event-reopen",
		WorkerEventDigest: "sha256:event-reopen", Data: []byte("wire-reopen"),
		CreatedAt: time.Unix(3, 123).UTC(),
	}
	receipt := llm.StoreWorkerReceiptRecord{
		Request: request.Key, EventID: event.WorkerEventID, Worker: event.Worker,
		Digest: event.WorkerEventDigest, CreatedAt: time.Unix(4, 123).UTC(),
	}
	tool := llm.StoreToolExecutionRecord{
		Key:         llm.StoreToolExecutionKey{Task: task.Key, ToolCallID: "tool-reopen"},
		InputDigest: "sha256:tool-reopen", State: llm.ToolExecutionPending,
		Revision: 1, CreatedAt: time.Unix(5, 123).UTC(),
	}
	if err := first.Update(t.Context(), func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertRequest(request); err != nil {
			return err
		}
		if err := tx.InsertResponseEvent(event); err != nil {
			return err
		}
		if err := tx.InsertWorkerReceipt(receipt); err != nil {
			return err
		}
		return tx.InsertToolExecution(tool)
	}); err != nil {
		t.Fatal(err)
	}
	if err := firstResource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := first.View(t.Context(), func(llm.StoreView) error { return nil }); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("released Store View error = %v, want ErrStoreClosed", err)
	}

	secondResource, err := customstore.Open(t.Context(), fileConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	second, err := secondResource.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Bind(t.Context(), binding); err != nil {
		t.Fatalf("replay persisted binding: %v", err)
	}
	if err := second.Bind(t.Context(), llm.StoreBinding{DeploymentID: "other-deployment"}); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("different binding after reopen = %v, want conflict", err)
	}
	if err := second.View(t.Context(), func(view llm.StoreView) error {
		loaded, err := view.LoadTask(task.Key)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(loaded, task) {
			return errors.New("reopened task differs from committed record")
		}
		loadedRequest, err := view.LoadRequest(request.Key, llm.StoreReadLimit{MaxBytes: 1 << 20})
		if err != nil || !reflect.DeepEqual(loadedRequest, request) {
			return errors.New("reopened request differs from committed record")
		}
		events, err := view.ScanResponseEvents(llm.StoreResponseEventScan{
			Request: request.Key, Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: 1 << 20},
		})
		if err != nil || !reflect.DeepEqual(events, []llm.StoreResponseEventRecord{event}) {
			return errors.New("reopened response events differ from committed records")
		}
		loadedReceipt, err := view.LoadWorkerReceipt(request.Key, receipt.EventID)
		if err != nil || !reflect.DeepEqual(loadedReceipt, receipt) {
			return errors.New("reopened receipt differs from committed record")
		}
		loadedTool, err := view.LoadToolExecution(tool.Key, llm.StoreReadLimit{MaxBytes: 1 << 20})
		if err != nil || !reflect.DeepEqual(loadedTool, tool) {
			return errors.New("reopened tool execution differs from committed record")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := secondResource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreRecoversCommittedSnapshotAfterAbruptProcessExit(t *testing.T) {
	if os.Getenv(customCrashChildEnv) == "1" {
		if err := seedCustomStoreBeforeCrash(os.Getenv(customCrashPathEnv)); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(customCrashExitCode + 1)
		}
		// No Resource.Release and no testing cleanup: the parent must recover the
		// snapshot made durable before Update returned.
		os.Exit(customCrashExitCode)
	}

	path := filepath.Join(t.TempDir(), "crash.snapshot")
	command := exec.Command(os.Args[0], "-test.run=^TestFileStoreRecoversCommittedSnapshotAfterAbruptProcessExit$", "-test.count=1")
	command.Env = append(os.Environ(), customCrashChildEnv+"=1", customCrashPathEnv+"="+path)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != customCrashExitCode {
		t.Fatalf("crash child = %v (exit %d), want %d; output:\n%s", err, customProcessExitCode(exit), customCrashExitCode, output)
	}

	resource, err := customstore.Open(t.Context(), fileConfig(path))
	if err != nil {
		t.Fatalf("open custom Store after process crash: %v", err)
	}
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Bind(t.Context(), llm.StoreBinding{DeploymentID: "deployment-crash"}); err != nil {
		t.Fatalf("durable binding after process crash: %v", err)
	}
	wantTask := durableTask()
	wantRequest := durableRequest(wantTask)
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

func seedCustomStoreBeforeCrash(path string) error {
	resource, err := customstore.Open(context.Background(), fileConfig(path))
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
	task := durableTask()
	request := durableRequest(task)
	return store.Update(context.Background(), func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		return tx.InsertRequest(request)
	})
}

func customProcessExitCode(err *exec.ExitError) int {
	if err == nil {
		return 0
	}
	return err.ExitCode()
}

func TestFileStoreCorruptionFailsClosedAndReleasesPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{
			name: "truncated",
			mutate: func(encoded []byte) []byte {
				return encoded[:len(encoded)/2]
			},
		},
		{
			name: "checksum mismatch",
			mutate: func(encoded []byte) []byte {
				return bytes.Replace(encoded, []byte("deployment-corrupt"), []byte("deployment-tampered"), 1)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "corrupt.snapshot")
			resource, err := customstore.Open(t.Context(), fileConfig(path))
			if err != nil {
				t.Fatal(err)
			}
			store, err := resource.Value()
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Bind(t.Context(), llm.StoreBinding{DeploymentID: "deployment-corrupt"}); err != nil {
				t.Fatal(err)
			}
			if err := resource.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			mutated := test.mutate(encoded)
			if bytes.Equal(mutated, encoded) {
				t.Fatal("corruption mutation did not change snapshot")
			}
			if err := os.WriteFile(path, mutated, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := customstore.Open(t.Context(), fileConfig(path)); !errors.Is(err, llm.ErrStoreCorruptRecord) {
				t.Fatalf("open corrupt snapshot = %v, want ErrStoreCorruptRecord", err)
			}

			// A failed constructor must release its path reservation. Replacing the
			// bad image with a new path lets the same process open it immediately.
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			recovered, err := customstore.Open(t.Context(), fileConfig(path))
			if err != nil {
				t.Fatalf("open path after failed construction: %v", err)
			}
			if err := recovered.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFileStoreOwnsOneLivePathAndReleaseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned.snapshot")
	resource, err := customstore.Open(t.Context(), fileConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	if !resource.Owned() {
		t.Fatal("Open returned a borrowed Resource")
	}
	if _, err := customstore.Open(t.Context(), fileConfig(path)); err == nil {
		t.Fatal("second live handle acquired the same single-process snapshot")
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := resource.Value(); !errors.Is(err, framework.ErrResourceReleased) {
		t.Fatalf("released Resource Value error = %v", err)
	}
	reopened, err := customstore.Open(t.Context(), fileConfig(path))
	if err != nil {
		t.Fatalf("path remained reserved after release: %v", err)
	}
	if err := reopened.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreRejectsOversizedCommitBeforeReplacingSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded.snapshot")
	config := fileConfig(path)
	config.MaxSnapshotBytes = 2 << 10
	resource, err := customstore.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	binding := llm.StoreBinding{DeploymentID: "deployment-bounded"}
	if err := store.Bind(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	task := durableTask()
	request := durableRequest(task)
	request.CanonicalPayload = bytes.Repeat([]byte("x"), 8<<10)
	err = store.Update(t.Context(), func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		return tx.InsertRequest(request)
	})
	if !errors.Is(err, customstore.ErrSnapshotTooLarge) {
		t.Fatalf("oversized Update error = %v, want ErrSnapshotTooLarge", err)
	}
	if err := store.View(t.Context(), func(view llm.StoreView) error {
		_, err := view.LoadTask(task.Key)
		return err
	}); !errors.Is(err, llm.ErrStoreRecordNotFound) {
		t.Fatalf("oversized Update became visible: %v", err)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened, err := customstore.Open(t.Context(), config)
	if err != nil {
		t.Fatalf("reopen previous bounded snapshot: %v", err)
	}
	reopenedStore, err := reopened.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedStore.Bind(t.Context(), binding); err != nil {
		t.Fatalf("previous binding did not survive rejected commit: %v", err)
	}
	if err := reopened.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreReleaseDeadlineFinishesAsynchronously(t *testing.T) {
	path := filepath.Join(t.TempDir(), "release.snapshot")
	resource, err := customstore.Open(t.Context(), fileConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	unblock := make(chan struct{})
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- store.Update(context.Background(), func(llm.StoreTx) error {
			close(entered)
			<-unblock
			return nil
		})
	}()
	<-entered
	releaseCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = resource.Release(releaseCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded Release error = %v, want deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Release ignored deadline for %v", elapsed)
	}
	if _, err := resource.Value(); !errors.Is(err, framework.ErrResourceReleased) {
		t.Fatalf("Resource Value during asynchronous close = %v", err)
	}
	close(unblock)
	if err := <-updateDone; err != nil {
		t.Fatalf("in-flight Update completion: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		reopened, openErr := customstore.Open(t.Context(), fileConfig(path))
		if openErr == nil {
			if err := reopened.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background release did not relinquish path: %v", openErr)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFileStoreCanonicalizesParentAliases(t *testing.T) {
	realDirectory := t.TempDir()
	aliasDirectory := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(realDirectory, aliasDirectory); err != nil {
		t.Skipf("symbolic links unavailable: %v", err)
	}
	realPath := filepath.Join(realDirectory, "store.snapshot")
	aliasPath := filepath.Join(aliasDirectory, "store.snapshot")
	resource, err := customstore.Open(t.Context(), fileConfig(realPath))
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Release(context.Background())
	if _, err := customstore.Open(t.Context(), fileConfig(aliasPath)); err == nil ||
		!strings.Contains(err.Error(), "already open") {
		t.Fatalf("open through parent symlink alias = %v, want same-process refusal", err)
	}
}

func TestFileStoreCrossProcessOwnershipLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "process-lock.snapshot")
	resource, err := customstore.Open(t.Context(), fileConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Release(context.Background())
	command := exec.Command(os.Args[0], "-test.run=^TestFileStoreLockHelper$")
	command.Env = append(os.Environ(), "CUSTOMSTORE_LOCK_HELPER=1", "CUSTOMSTORE_LOCK_PATH="+path)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("lock helper: %v\n%s", err, output)
	}
}

func TestFileStoreLockHelper(t *testing.T) {
	if os.Getenv("CUSTOMSTORE_LOCK_HELPER") != "1" {
		return
	}
	resource, err := customstore.Open(t.Context(), fileConfig(os.Getenv("CUSTOMSTORE_LOCK_PATH")))
	if err == nil {
		_ = resource.Release(context.Background())
		t.Fatal("child process acquired parent-owned snapshot")
	}
	if !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("child lock error = %v", err)
	}
}

func TestFileStoreValidatesRecordsAndCASUniqueness(t *testing.T) {
	resource, err := customstore.Open(t.Context(), fileConfig(filepath.Join(t.TempDir(), "invariants.snapshot")))
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Release(context.Background())
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}

	invalidTask := durableTask()
	invalidTask.State = "invented"
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		return tx.InsertTask(invalidTask)
	}); !errors.Is(err, llm.ErrStoreInvalidArgument) {
		t.Fatalf("invalid task state = %v, want ErrStoreInvalidArgument", err)
	}

	terminal := durableTask()
	terminal.State = llm.TaskCompleted
	open := durableTask()
	open.Key.Task = "task-open-affinity-owner"
	open.CreatedAt = open.CreatedAt.Add(time.Nanosecond)
	open.UpdatedAt = open.CreatedAt
	completedRequest := durableRequest(open)
	completedRequest.ResponseComplete = true
	completedRequest.CompletedAt = timePointer(time.Unix(6, 123).UTC())
	activeRequest := durableRequest(open)
	activeRequest.Key.IdempotencyKey = "request-active"
	activeRequest.RequestID = "request-id-active"
	activeRequest.ResponseID = "response-id-active"
	activeRequest.RequestDigest = "sha256:request-active"
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		if err := tx.InsertTask(terminal); err != nil {
			return err
		}
		if err := tx.InsertTask(open); err != nil {
			return err
		}
		if err := tx.InsertRequest(completedRequest); err != nil {
			return err
		}
		return tx.InsertRequest(activeRequest)
	}); err != nil {
		t.Fatal(err)
	}

	reopenedTask := terminal
	reopenedTask.State = llm.TaskAwaitingHuman
	reopenedTask.Revision = 2
	reopenedTask.UpdatedAt = reopenedTask.UpdatedAt.Add(time.Nanosecond)
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		_, err := tx.CompareAndSwapTask(llm.StoreTaskMutation{
			Key: terminal.Key, ExpectedRevision: 1, Next: reopenedTask,
		})
		return err
	}); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("terminal-to-open affinity collision = %v, want conflict", err)
	}

	reactivated := completedRequest
	reactivated.ResponseComplete = false
	reactivated.CompletedAt = nil
	reactivated.Revision = 2
	if err := store.Update(t.Context(), func(tx llm.StoreTx) error {
		_, err := tx.CompareAndSwapRequest(llm.StoreRequestMutation{
			Key: completedRequest.Key, ExpectedRevision: 1, Next: reactivated,
		})
		return err
	}); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("completed-to-active request collision = %v, want conflict", err)
	}

	if err := store.View(t.Context(), func(view llm.StoreView) error {
		_, err := view.ScanResponseEvents(llm.StoreResponseEventScan{
			Request: activeRequest.Key,
			Kinds:   []llm.StoreResponseEventKind{llm.StoreEventWire, llm.StoreEventWire},
			Limit:   1, ReadLimit: llm.StoreReadLimit{MaxBytes: 1},
		})
		return err
	}); !errors.Is(err, llm.ErrStoreInvalidArgument) {
		t.Fatalf("duplicate scan kinds = %v, want ErrStoreInvalidArgument", err)
	}
}

func TestOwnedAndBorrowedLifecycle(t *testing.T) {
	t.Run("owned releases wrapped resource once", func(t *testing.T) {
		base, releaseBase := humantest.NewMemoryLLMStore()
		var releases atomic.Uint64
		resource, err := framework.Own[llm.Store](base, func(ctx context.Context) error {
			releases.Add(1)
			return releaseBase(ctx)
		})
		if err != nil {
			t.Fatal(err)
		}
		decorated, err := customstore.Own(t.Context(), resource, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := decorated.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := decorated.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		if releases.Load() != 1 {
			t.Fatalf("wrapped releases = %d, want 1", releases.Load())
		}
		if _, err := resource.Value(); !errors.Is(err, framework.ErrResourceReleased) {
			t.Fatalf("wrapped resource remains live: %v", err)
		}
	})

	t.Run("borrowed leaves wrapped resource live", func(t *testing.T) {
		base, release := humantest.NewMemoryLLMStore()
		t.Cleanup(func() { _ = release(context.Background()) })
		decorated, err := customstore.Borrow(t.Context(), base, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := decorated.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := base.Bind(t.Context(), llm.StoreBinding{DeploymentID: "still-host-owned"}); err != nil {
			t.Fatalf("borrowed Store was released: %v", err)
		}
		if _, err := customstore.Own(
			t.Context(), framework.Borrow[llm.Store](base), testConfig(),
		); err == nil {
			t.Fatal("Own accepted a borrowed wrapped Resource")
		}
	})
}

func openOwned(
	ctx context.Context,
	resource framework.Resource[llm.Store],
) (llm.Store, framework.ReleaseFunc, error) {
	decorated, err := customstore.Own(ctx, resource, testConfig())
	if err != nil {
		return nil, nil, err
	}
	store, err := decorated.Value()
	if err != nil {
		_ = decorated.Release(context.Background())
		return nil, nil, err
	}
	return store, decorated.Release, nil
}

func testConfig() customstore.Config {
	return customstore.Config{
		Provider: "example.custom-store",
		Version:  "1",
		Audit:    func(string) {},
	}
}

func fileConfig(path string) customstore.Config {
	config := testConfig()
	config.Path = path
	return config
}

func durableTask() llm.StoreTaskRecord {
	created := time.Unix(1, 123).UTC()
	return llm.StoreTaskRecord{
		Key:            llm.StoreTaskKey{Caller: "caller-reopen", Task: "task-reopen"},
		WorkspaceKey:   "workspace-reopen",
		CapabilityTier: llm.TierWorkspace,
		Codec: llm.CodecSnapshot{
			Contract: framework.Contract{
				ID: llm.CodecContractID, Major: llm.CodecContractMajor,
				Features: map[framework.Feature]uint16{"example.feature": 1},
			},
			ID: "example.codec", Version: "1",
			Fingerprint: llm.Fingerprint([]byte("custom-store-reopen-codec")),
		},
		HarnessID:        "example-harness",
		HarnessVersion:   "1",
		HarnessSessionID: "session-reopen",
		WorkspaceRoot:    "/workspace/reopen",
		ExecAllowed:      true,
		State:            llm.TaskAwaitingCaller,
		Revision:         1,
		CreatedAt:        created,
		UpdatedAt:        created,
	}
}

func durableRequest(task llm.StoreTaskRecord) llm.StoreRequestRecord {
	return llm.StoreRequestRecord{
		Key: llm.StoreRequestKey{
			Caller: task.Key.Caller, IdempotencyKey: "request-reopen",
		},
		Task: task.Key, RequestID: "request-id-reopen", ResponseID: "response-id-reopen",
		RequestDigest: "sha256:request-reopen", Codec: task.Codec,
		Mode: llm.ResponseStream, CanonicalPayload: []byte("canonical-reopen"),
		Revision: 1, CreatedAt: time.Unix(2, 123).UTC(),
	}
}

func timePointer(value time.Time) *time.Time { return &value }
