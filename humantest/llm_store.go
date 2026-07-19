package humantest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

// LLMStoreFactory opens a new, empty HumanLLM Store for one conformance
// subtest. Every invocation must return an independent Store. release must be
// non-nil and release every resource opened by the factory; TestLLMStore calls
// it exactly once even when the subtest fails.
//
// The construction context bounds only the factory call and subtest. release
// receives a fresh context so a cancelled operation cannot prevent cleanup.
type LLMStoreFactory func(
	context.Context,
	testing.TB,
) (llm.Store, framework.ReleaseFunc, error)

// TestLLMStore runs the mandatory black-box conformance suite for a HumanLLM
// Store. It verifies the complete public primitive contract: transaction
// lifetime and isolation, Task and Request identity constraints, exact opaque
// byte ownership and budgets, ordered response events, worker receipts, the
// task-wide tool ledger, recovery and retention scans, and tombstone cleanup.
//
// Passing this suite is necessary but does not replace adapter-specific crash,
// migration, durability, corruption, and infrastructure fault-injection tests.
func TestLLMStore(t *testing.T, factory LLMStoreFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("HumanLLM Store conformance factory is nil")
	}

	tests := []struct {
		name string
		run  func(context.Context, *testing.T, llm.Store)
	}{
		{"description_and_callback_lifetime", testLLMStoreDescriptionAndLifetime},
		{"rollback_and_dirty_read_isolation", testLLMStoreRollback},
		{"read_your_writes", testLLMStoreReadYourWrites},
		{"stable_view_snapshot", testLLMStoreSnapshot},
		{"concurrent_updates_are_serializable", testLLMStoreConcurrentUpdates},
		{"task_affinity_compare_and_swap_and_identity", testLLMStoreTasks},
		{"request_activity_compare_and_swap_bytes_and_identity", testLLMStoreRequests},
		{"response_event_order_filters_and_budgets", testLLMStoreResponseEvents},
		{"worker_receipts", testLLMStoreWorkerReceipts},
		{"tool_ledger_compare_and_swap_and_scan", testLLMStoreToolLedger},
		{"nil_and_empty_opaque_bytes", testLLMStoreNilAndEmptyBytes},
		{"recovery_incomplete_unacknowledged_and_cursor", testLLMStoreRecovery},
		{"retention_cursor_and_unacknowledged_marker", testLLMStoreRetention},
		{"tombstone_deletes_events_but_preserves_receipts", testLLMStoreTombstone},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			t.Cleanup(cancel)

			store, release, err := factory(ctx, t)
			if release != nil {
				t.Cleanup(func() {
					releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer releaseCancel()
					if err := release(releaseCtx); err != nil {
						t.Errorf("release HumanLLM Store: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatalf("open fresh HumanLLM Store: %v", err)
			}
			if store == nil {
				t.Fatal("factory returned a nil HumanLLM Store")
			}
			if release == nil {
				t.Fatal("factory returned a nil release function")
			}
			test.run(ctx, t, store)
		})
	}
}

func testLLMStoreDescriptionAndLifetime(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	first := store.Description()
	if err := first.Validate(); err != nil {
		t.Fatalf("Description does not satisfy HumanLLM Store contract: %v", err)
	}
	if second := store.Description(); !reflect.DeepEqual(first, second) {
		t.Fatalf("Description is not static:\nfirst:  %#v\nsecond: %#v", first, second)
	}
	if err := store.View(ctx, nil); !errors.Is(err, llm.ErrStoreInvalidArgument) {
		t.Fatalf("nil View callback error = %v, want ErrStoreInvalidArgument", err)
	}
	if err := store.Update(ctx, nil); !errors.Is(err, llm.ErrStoreInvalidArgument) {
		t.Fatalf("nil Update callback error = %v, want ErrStoreInvalidArgument", err)
	}

	viewFailure := errors.New("humantest View callback result")
	var viewCalls atomic.Int32
	var retainedView llm.StoreView
	err := store.View(ctx, func(view llm.StoreView) error {
		viewCalls.Add(1)
		retainedView = view
		return viewFailure
	})
	if !errors.Is(err, viewFailure) || viewCalls.Load() != 1 {
		t.Fatalf("View error/calls = %v/%d, want callback error/exactly 1", err, viewCalls.Load())
	}
	if _, err := retainedView.LoadTask(llm.StoreTaskKey{Caller: "expired", Task: "view"}); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("retained View error = %v, want ErrStoreClosed", err)
	}

	updateFailure := errors.New("humantest Update callback result")
	var updateCalls atomic.Int32
	var retainedTx llm.StoreTx
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		updateCalls.Add(1)
		retainedTx = tx
		return updateFailure
	})
	if !errors.Is(err, updateFailure) || updateCalls.Load() != 1 {
		t.Fatalf("Update error/calls = %v/%d, want callback error/exactly 1", err, updateCalls.Load())
	}
	if _, err := retainedTx.LoadTask(llm.StoreTaskKey{Caller: "expired", Task: "tx"}); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("retained StoreTx error = %v, want ErrStoreClosed", err)
	}

	var successfulViewCalls atomic.Int32
	var successfulView llm.StoreView
	if err := store.View(ctx, func(view llm.StoreView) error {
		successfulViewCalls.Add(1)
		successfulView = view
		return nil
	}); err != nil || successfulViewCalls.Load() != 1 {
		t.Fatalf("successful View error/calls = %v/%d, want nil/exactly 1", err, successfulViewCalls.Load())
	}
	if _, err := successfulView.LoadTask(llm.StoreTaskKey{Caller: "expired", Task: "successful-view"}); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("retained successful View error = %v, want ErrStoreClosed", err)
	}
	var successfulUpdateCalls atomic.Int32
	var successfulTx llm.StoreTx
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		successfulUpdateCalls.Add(1)
		successfulTx = tx
		return nil
	}); err != nil || successfulUpdateCalls.Load() != 1 {
		t.Fatalf("successful Update error/calls = %v/%d, want nil/exactly 1", err, successfulUpdateCalls.Load())
	}
	if _, err := successfulTx.LoadTask(llm.StoreTaskKey{Caller: "expired", Task: "successful-tx"}); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("retained successful StoreTx error = %v, want ErrStoreClosed", err)
	}
}

func testLLMStoreRollback(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	rollback := errors.New("humantest requested rollback")
	dirtyTask := llmConformanceTask("rolled-back", 1, llm.TaskAdmitted)
	dirtyRequest := llmConformanceRequest(dirtyTask, "rolled-back", []byte("must-not-commit"))
	dirtyEvent := llmConformanceEvent(dirtyRequest.Key, 1, llm.StoreEventWire, "", "", []byte("must-not-commit-wire"))
	err := store.Update(ctx, func(tx llm.StoreTx) error {
		if err := tx.InsertTask(dirtyTask); err != nil {
			return err
		}
		if err := tx.InsertRequest(dirtyRequest); err != nil {
			return err
		}
		if err := tx.InsertResponseEvent(dirtyEvent); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("Update rollback error = %v, want %v", err, rollback)
	}
	err = store.View(ctx, func(view llm.StoreView) error {
		if _, err := view.LoadTask(dirtyTask.Key); !errors.Is(err, llm.ErrStoreRecordNotFound) {
			return fmt.Errorf("rolled-back task lookup error = %v, want not found", err)
		}
		if _, err := view.LoadRequest(dirtyRequest.Key, llmGenerousReadLimit()); !errors.Is(err, llm.ErrStoreRecordNotFound) {
			return fmt.Errorf("rolled-back request lookup error = %v, want not found", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		if err := tx.InsertTask(dirtyTask); err != nil {
			return err
		}
		if err := tx.InsertRequest(dirtyRequest); err != nil {
			return err
		}
		return tx.InsertResponseEvent(dirtyEvent)
	}); err != nil {
		t.Fatalf("rolled-back identities remained reserved: %v", err)
	}

	// An uncommitted transaction may block a View or let it observe the previous
	// MVCC snapshot, but its staged identity must never become visible.
	isolatedTask := llmConformanceTask("dirty-isolation", 1, llm.TaskAdmitted)
	staged := make(chan struct{})
	finish := make(chan struct{})
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- store.Update(ctx, func(tx llm.StoreTx) error {
			if err := tx.InsertTask(isolatedTask); err != nil {
				return err
			}
			close(staged)
			select {
			case <-finish:
				return rollback
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-staged:
	case err := <-updateDone:
		t.Fatalf("Update ended before dirty-read probe: %v", err)
	case <-ctx.Done():
		t.Fatalf("wait for staged transaction: %v", ctx.Err())
	}
	viewDone := make(chan error, 1)
	go func() {
		viewDone <- store.View(ctx, func(view llm.StoreView) error {
			_, err := view.LoadTask(isolatedTask.Key)
			if errors.Is(err, llm.ErrStoreRecordNotFound) {
				return nil
			}
			return fmt.Errorf("uncommitted task became visible: %v", err)
		})
	}()
	earlyView := false
	select {
	case err := <-viewDone:
		earlyView = true
		if err != nil {
			close(finish)
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		close(finish)
		t.Fatalf("wait for dirty-read View: %v", ctx.Err())
	}
	close(finish)
	if err := <-updateDone; !errors.Is(err, rollback) {
		t.Fatalf("dirty transaction rollback = %v, want %v", err, rollback)
	}
	if !earlyView {
		if err := <-viewDone; err != nil {
			t.Fatalf("View after rollback: %v", err)
		}
	}
}

func testLLMStoreReadYourWrites(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	task := llmConformanceTask("read-own", 1, llm.TaskAdmitted)
	request := llmConformanceRequest(task, "read-own", []byte("canonical-read-own"))
	event := llmConformanceEvent(request.Key, 1, llm.StoreEventWire, "worker-event-read-own", llmDigest('e'), []byte("wire-read-own"))
	receipt := llm.StoreWorkerReceiptRecord{
		Request: request.Key, EventID: event.WorkerEventID, Worker: "worker-read-own",
		Digest: event.WorkerEventDigest, CreatedAt: llmConformanceTime(4),
	}
	tool := llmConformanceTool(task, "tool-read-own", llm.ToolExecutionPending, nil)
	err := store.Update(ctx, func(tx llm.StoreTx) error {
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
		if err := tx.InsertToolExecution(tool); err != nil {
			return err
		}
		storedTask, err := tx.LoadTask(task.Key)
		if err != nil || !reflect.DeepEqual(storedTask, task) {
			return fmt.Errorf("read inserted task = %#v, error %v", storedTask, err)
		}
		storedRequest, err := tx.LoadRequest(request.Key, llmGenerousReadLimit())
		if err != nil || !reflect.DeepEqual(storedRequest, request) {
			return fmt.Errorf("read inserted request = %#v, error %v", storedRequest, err)
		}
		events, err := tx.ScanResponseEvents(llm.StoreResponseEventScan{
			Request: request.Key, Limit: 10, ReadLimit: llmGenerousReadLimit(),
		})
		if err != nil || len(events) != 1 || !reflect.DeepEqual(events[0], event) {
			return fmt.Errorf("read inserted event = %#v, error %v", events, err)
		}
		storedReceipt, err := tx.LoadWorkerReceipt(request.Key, receipt.EventID)
		if err != nil || !reflect.DeepEqual(storedReceipt, receipt) {
			return fmt.Errorf("read inserted receipt = %#v, error %v", storedReceipt, err)
		}
		storedTool, err := tx.LoadToolExecution(tool.Key, llmGenerousReadLimit())
		if err != nil || !reflect.DeepEqual(storedTool, tool) {
			return fmt.Errorf("read inserted tool = %#v, error %v", storedTool, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read-your-writes Update: %v", err)
	}
}

func testLLMStoreSnapshot(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	initial := llmConformanceTask("snapshot", 1, llm.TaskAdmitted)
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertTask(initial) }); err != nil {
		t.Fatalf("insert snapshot task: %v", err)
	}
	firstRead := make(chan struct{})
	continueView := make(chan struct{})
	viewDone := make(chan error, 1)
	go func() {
		viewDone <- store.View(ctx, func(view llm.StoreView) error {
			first, err := view.LoadTask(initial.Key)
			if err != nil || first.Revision != 1 {
				return fmt.Errorf("first snapshot task = %#v, error %v", first, err)
			}
			close(firstRead)
			select {
			case <-continueView:
			case <-ctx.Done():
				return ctx.Err()
			}
			second, err := view.LoadTask(initial.Key)
			if err != nil || second.Revision != 1 {
				return fmt.Errorf("View snapshot changed to %#v, error %v", second, err)
			}
			return nil
		})
	}()
	select {
	case <-firstRead:
	case err := <-viewDone:
		t.Fatalf("View ended before first read: %v", err)
	case <-ctx.Done():
		t.Fatalf("wait for first snapshot read: %v", ctx.Err())
	}
	updated := initial
	updated.State = llm.TaskAwaitingHuman
	updated.Revision = 2
	updated.UpdatedAt = llmConformanceTime(2)
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- store.Update(ctx, func(tx llm.StoreTx) error {
			changed, err := tx.CompareAndSwapTask(llm.StoreTaskMutation{Key: initial.Key, ExpectedRevision: 1, Next: updated})
			if err != nil {
				return err
			}
			if !changed {
				return errors.New("snapshot writer CAS returned false")
			}
			return nil
		})
	}()
	select {
	case err := <-updateDone:
		if err != nil {
			close(continueView)
			t.Fatalf("snapshot writer: %v", err)
		}
		updateDone <- nil
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		close(continueView)
		t.Fatalf("wait for snapshot writer: %v", ctx.Err())
	}
	close(continueView)
	if err := <-viewDone; err != nil {
		t.Fatalf("stable View snapshot: %v", err)
	}
	if err := <-updateDone; err != nil {
		t.Fatalf("snapshot writer completion: %v", err)
	}
	llmAssertTask(ctx, t, store, updated)
}

func testLLMStoreConcurrentUpdates(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	initial := llmConformanceTask("serialized", 1, llm.TaskAdmitted)
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertTask(initial) }); err != nil {
		t.Fatalf("insert serialization task: %v", err)
	}
	const writers = 12
	start := make(chan struct{})
	results := make(chan error, writers)
	callbackCalls := make([]atomic.Int32, writers)
	for index := range writers {
		go func() {
			<-start
			err := store.Update(ctx, func(tx llm.StoreTx) error {
				callbackCalls[index].Add(1)
				current, err := tx.LoadTask(initial.Key)
				if err != nil {
					return err
				}
				next := current
				next.Revision++
				next.UpdatedAt = next.UpdatedAt.Add(time.Nanosecond)
				changed, err := tx.CompareAndSwapTask(llm.StoreTaskMutation{
					Key: current.Key, ExpectedRevision: current.Revision, Next: next,
				})
				if err != nil {
					return err
				}
				if !changed {
					return fmt.Errorf("CAS missed after reading revision %d in one Update", current.Revision)
				}
				return nil
			})
			results <- err
		}()
	}
	close(start)
	var successes uint64
	for range writers {
		select {
		case err := <-results:
			if errors.Is(err, llm.ErrStoreCommitUnknown) {
				t.Errorf("ambiguous commit without fault injection: %v", err)
			} else if err == nil {
				successes++
			}
		case <-ctx.Done():
			t.Fatalf("wait for concurrent Updates: %v", ctx.Err())
		}
	}
	for index := range writers {
		if calls := callbackCalls[index].Load(); calls != 1 {
			t.Errorf("Update callback %d calls = %d, want 1", index, calls)
		}
	}
	if t.Failed() {
		return
	}
	if successes == 0 {
		t.Fatal("every concurrent Update aborted")
	}
	want := initial
	want.Revision += successes
	want.UpdatedAt = want.UpdatedAt.Add(time.Duration(successes))
	llmAssertTask(ctx, t, store, want)
}

func testLLMStoreTasks(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	initial := llmConformanceTask("task", 1, llm.TaskAdmitted)
	inputFeatures := initial.Codec.Contract.Features
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertTask(initial) }); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	inputFeatures["humantest.seed"] = 99
	loaded := llmLoadTask(ctx, t, store, initial.Key)
	if loaded.Codec.Contract.Features["humantest.seed"] != 1 {
		t.Fatalf("Store retained Task codec feature map alias: %#v", loaded.Codec.Contract.Features)
	}
	loaded.Codec.Contract.Features["humantest.seed"] = 77
	if again := llmLoadTask(ctx, t, store, initial.Key); again.Codec.Contract.Features["humantest.seed"] != 1 {
		t.Fatalf("Task read returned aliased codec map: %#v", again.Codec.Contract.Features)
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertTask(initial) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("duplicate Task insert error = %v, want ErrStoreConflict", err)
	}

	affinity := llmTaskAffinity(initial)
	err := store.View(ctx, func(view llm.StoreView) error {
		found, err := view.FindOpenTask(affinity)
		if err != nil || found.Key != initial.Key {
			return fmt.Errorf("FindOpenTask = %#v, error %v", found, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	conflicting := llmConformanceTask("task-same-affinity", 1, llm.TaskAdmitted)
	conflicting.WorkspaceKey = initial.WorkspaceKey
	conflicting.HarnessID = initial.HarnessID
	conflicting.HarnessVersion = initial.HarnessVersion
	conflicting.HarnessSessionID = initial.HarnessSessionID
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertTask(conflicting) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("open-affinity conflict error = %v, want ErrStoreConflict", err)
	}

	leased := llmLoadTask(ctx, t, store, initial.Key)
	leased.State = llm.TaskLeased
	leased.LeaseOwner = "worker-a"
	leased.LeaseID = "lease-a"
	leased.Revision = 2
	leased.UpdatedAt = llmConformanceTime(2)
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		changed, err := tx.CompareAndSwapTask(llm.StoreTaskMutation{Key: initial.Key, ExpectedRevision: 1, Next: leased})
		if err != nil || !changed {
			return fmt.Errorf("matching Task CAS changed/error = %v/%v", changed, err)
		}
		stale := leased
		stale.Revision = 3
		changed, err = tx.CompareAndSwapTask(llm.StoreTaskMutation{Key: initial.Key, ExpectedRevision: 1, Next: stale})
		if err != nil || changed {
			return fmt.Errorf("stale Task CAS changed/error = %v/%v", changed, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Task CAS: %v", err)
	}
	skippedRevision := leased
	skippedRevision.Revision = 4
	skippedRevision.UpdatedAt = llmConformanceTime(4)
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		_, err := tx.CompareAndSwapTask(llm.StoreTaskMutation{Key: initial.Key, ExpectedRevision: 2, Next: skippedRevision})
		return err
	})
	if !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("skipped Task revision error = %v, want ErrStoreConflict", err)
	}
	immutable := leased
	immutable.WorkspaceRoot = "/changed/identity"
	immutable.Revision = 3
	immutable.UpdatedAt = llmConformanceTime(3)
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		_, err := tx.CompareAndSwapTask(llm.StoreTaskMutation{Key: initial.Key, ExpectedRevision: 2, Next: immutable})
		return err
	})
	if !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("immutable Task CAS error = %v, want ErrStoreConflict", err)
	}
	terminal := leased
	terminal.State = llm.TaskCompleted
	terminal.Revision = 3
	terminal.UpdatedAt = llmConformanceTime(3)
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		changed, err := tx.CompareAndSwapTask(llm.StoreTaskMutation{Key: initial.Key, ExpectedRevision: 2, Next: terminal})
		if err != nil || !changed {
			return fmt.Errorf("terminal Task CAS changed/error = %v/%v", changed, err)
		}
		return tx.InsertTask(conflicting)
	}); err != nil {
		t.Fatalf("reuse affinity after terminal Task: %v", err)
	}
	err = store.View(ctx, func(view llm.StoreView) error {
		found, err := view.FindOpenTask(affinity)
		if err != nil || found.Key != conflicting.Key {
			return fmt.Errorf("FindOpenTask after terminal = %#v, error %v", found, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testLLMStoreRequests(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	task := llmConformanceTask("request-task", 1, llm.TaskAdmitted)
	canonical := []byte("canonical-request-bytes")
	body := []byte("aggregate-decision-body")
	request := llmConformanceRequest(task, "request-a", canonical)
	request.Mode = llm.ResponseAggregate
	request.Decision = llm.StoreResponseDecision{StatusCode: 200, ContentType: "application/json", RetryAfter: "", Body: body}
	features := request.Codec.Contract.Features
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		return tx.InsertRequest(request)
	}); err != nil {
		t.Fatalf("insert Request: %v", err)
	}
	mutateLLMBytes(canonical)
	mutateLLMBytes(body)
	features["humantest.seed"] = 55

	readLimit := int64(len("canonical-request-bytes") + len("aggregate-decision-body"))
	loaded := llmLoadRequest(ctx, t, store, request.Key, readLimit)
	if string(loaded.CanonicalPayload) != "canonical-request-bytes" || string(loaded.Decision.Body) != "aggregate-decision-body" ||
		loaded.Codec.Contract.Features["humantest.seed"] != 1 {
		t.Fatalf("Request input ownership lost: %#v", loaded)
	}
	mutateLLMBytes(loaded.CanonicalPayload)
	mutateLLMBytes(loaded.Decision.Body)
	loaded.Codec.Contract.Features["humantest.seed"] = 44
	again := llmLoadRequest(ctx, t, store, request.Key, llmGenerousReadLimit().MaxBytes)
	if string(again.CanonicalPayload) != "canonical-request-bytes" || string(again.Decision.Body) != "aggregate-decision-body" ||
		again.Codec.Contract.Features["humantest.seed"] != 1 {
		t.Fatalf("Request read returned aliases: %#v", again)
	}
	llmAssertOversized(t, "Request aggregate budget", func() error {
		return store.View(ctx, func(view llm.StoreView) error {
			_, err := view.LoadRequest(request.Key, llm.StoreReadLimit{MaxBytes: readLimit - 1})
			return err
		})
	})
	decisionLimit := int64(len("aggregate-decision-body"))
	err := store.View(ctx, func(view llm.StoreView) error {
		decision, err := view.LoadResponseDecision(request.Key, llm.StoreReadLimit{MaxBytes: decisionLimit})
		if err != nil || !reflect.DeepEqual(decision, again.Decision) {
			return fmt.Errorf("LoadResponseDecision = %#v, error %v", decision, err)
		}
		decision.Body[0] ^= 0xff
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision := llmLoadDecision(ctx, t, store, request.Key, llmGenerousReadLimit()); string(decision.Body) != "aggregate-decision-body" {
		t.Fatalf("response decision read alias changed body: %#v", decision)
	}
	llmAssertOversized(t, "response decision budget", func() error {
		return store.View(ctx, func(view llm.StoreView) error {
			_, err := view.LoadResponseDecision(request.Key, llm.StoreReadLimit{MaxBytes: decisionLimit - 1})
			return err
		})
	})

	err = store.View(ctx, func(view llm.StoreView) error {
		head, err := view.FindActiveRequest(task.Key)
		if err != nil || head.Key != request.Key || head.DecisionStatus != 200 || len(head.Codec.Contract.Features) == 0 {
			return fmt.Errorf("FindActiveRequest = %#v, error %v", head, err)
		}
		loadedHead, err := view.LoadRequestHead(request.Key)
		if err != nil || !reflect.DeepEqual(head, loadedHead) {
			return fmt.Errorf("LoadRequestHead = %#v, error %v, want %#v", loadedHead, err, head)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertRequest(request) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("duplicate Request insert error = %v, want ErrStoreConflict", err)
	}
	second := llmConformanceRequest(task, "request-b", []byte("second"))
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertRequest(second) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("active Request conflict error = %v, want ErrStoreConflict", err)
	}
	completed := again
	completed.ResponseComplete = true
	completedAt := llmConformanceTime(6)
	completed.CompletedAt = &completedAt
	completed.Revision = 2
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		changed, err := tx.CompareAndSwapRequest(llm.StoreRequestMutation{Key: request.Key, ExpectedRevision: 1, Next: completed})
		if err != nil || !changed {
			return fmt.Errorf("matching Request CAS changed/error = %v/%v", changed, err)
		}
		stale := completed
		stale.Revision = 3
		changed, err = tx.CompareAndSwapRequest(llm.StoreRequestMutation{Key: request.Key, ExpectedRevision: 1, Next: stale})
		if err != nil || changed {
			return fmt.Errorf("stale Request CAS changed/error = %v/%v", changed, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Request CAS: %v", err)
	}
	skippedRevision := completed
	skippedRevision.Revision = 4
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		_, err := tx.CompareAndSwapRequest(llm.StoreRequestMutation{Key: request.Key, ExpectedRevision: 2, Next: skippedRevision})
		return err
	})
	if !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("skipped Request revision error = %v, want ErrStoreConflict", err)
	}
	immutable := completed
	immutable.RequestID = "changed-request-id"
	immutable.Revision = 3
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		_, err := tx.CompareAndSwapRequest(llm.StoreRequestMutation{Key: request.Key, ExpectedRevision: 2, Next: immutable})
		return err
	})
	if !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("immutable Request CAS error = %v, want ErrStoreConflict", err)
	}
	modifiedPayload := completed
	modifiedPayload.CanonicalPayload = []byte("changed-canonical")
	modifiedPayload.Revision = 3
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		_, err := tx.CompareAndSwapRequest(llm.StoreRequestMutation{Key: request.Key, ExpectedRevision: 2, Next: modifiedPayload})
		return err
	})
	if !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("canonical Request mutation error = %v, want ErrStoreConflict", err)
	}
	differentCodec := llmConformanceRequest(task, "request-codec", []byte("codec"))
	differentCodec.Codec.Fingerprint = llm.Fingerprint([]byte("different codec"))
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertRequest(differentCodec) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("Task/Request codec mismatch error = %v, want ErrStoreConflict", err)
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertRequest(second) }); err != nil {
		t.Fatalf("insert next Request after completion: %v", err)
	}
}

func testLLMStoreResponseEvents(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	task := llmConformanceTask("events", 1, llm.TaskAdmitted)
	request := llmConformanceRequest(task, "events", []byte("canonical"))
	oneData := []byte("one")
	events := []llm.StoreResponseEventRecord{
		llmConformanceEvent(request.Key, 3, llm.StoreEventWire, "worker-event-a", llmDigest('e'), []byte("three")),
		llmConformanceEvent(request.Key, 1, llm.StoreEventCheckpoint, "", "", oneData),
		llmConformanceEvent(request.Key, 2, llm.StoreEventWire, "worker-event-a", llmDigest('e'), []byte("two-two")),
		llmConformanceEvent(request.Key, 4, llm.StoreEventWire, "worker-event-b", llmDigest('f'), []byte("four")),
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertRequest(request); err != nil {
			return err
		}
		for _, event := range events {
			if err := tx.InsertResponseEvent(event); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("insert response events: %v", err)
	}
	mutateLLMBytes(oneData)
	all := llmScanEvents(ctx, t, store, llm.StoreResponseEventScan{
		Request: request.Key, Limit: 10, ReadLimit: llmGenerousReadLimit(),
	})
	if sequences := llmEventSequences(all); !reflect.DeepEqual(sequences, []uint64{1, 2, 3, 4}) || string(all[0].Data) != "one" {
		t.Fatalf("response event order/ownership = %v / %#v", sequences, all)
	}
	all[0].Data[0] ^= 0xff
	if again := llmScanEvents(ctx, t, store, llm.StoreResponseEventScan{Request: request.Key, Limit: 10, ReadLimit: llmGenerousReadLimit()}); string(again[0].Data) != "one" {
		t.Fatalf("response event read returned aliased Data: %#v", again[0])
	}
	wire := llmScanEvents(ctx, t, store, llm.StoreResponseEventScan{
		Request: request.Key, After: 1, Kinds: []llm.StoreResponseEventKind{llm.StoreEventWire},
		Limit: 2, ReadLimit: llmGenerousReadLimit(),
	})
	if got := llmEventSequences(wire); !reflect.DeepEqual(got, []uint64{2, 3}) {
		t.Fatalf("kind/after/limit event scan = %v, want [2 3]", got)
	}
	worker := llmScanEvents(ctx, t, store, llm.StoreResponseEventScan{
		Request: request.Key, WorkerEventID: "worker-event-a", Limit: 10, ReadLimit: llmGenerousReadLimit(),
	})
	if got := llmEventSequences(worker); !reflect.DeepEqual(got, []uint64{2, 3}) {
		t.Fatalf("worker event filter = %v, want [2 3]", got)
	}
	prefix := llmScanEvents(ctx, t, store, llm.StoreResponseEventScan{
		Request: request.Key, Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: int64(len("one") + len("two-two"))},
	})
	if got := llmEventSequences(prefix); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("response event byte prefix = %v, want [1 2]", got)
	}
	llmAssertOversized(t, "first response event", func() error {
		return store.View(ctx, func(view llm.StoreView) error {
			_, err := view.ScanResponseEvents(llm.StoreResponseEventScan{
				Request: request.Key, Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: 2},
			})
			return err
		})
	})
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertResponseEvent(events[1]) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("duplicate response sequence error = %v, want ErrStoreConflict", err)
	}
	wrongDigest := llmConformanceEvent(request.Key, 5, llm.StoreEventWire, "worker-event-a", llmDigest('x'), []byte("five"))
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertResponseEvent(wrongDigest) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("worker-event digest conflict error = %v, want ErrStoreConflict", err)
	}
}

func testLLMStoreWorkerReceipts(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	task := llmConformanceTask("receipt", 1, llm.TaskAdmitted)
	request := llmConformanceRequest(task, "receipt", []byte("canonical"))
	receipt := llm.StoreWorkerReceiptRecord{
		Request: request.Key, EventID: "event-receipt", Worker: "worker-a",
		Digest: llmDigest('r'), CreatedAt: llmConformanceTime(5),
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertRequest(request); err != nil {
			return err
		}
		return tx.InsertWorkerReceipt(receipt)
	}); err != nil {
		t.Fatalf("insert worker receipt: %v", err)
	}
	err := store.View(ctx, func(view llm.StoreView) error {
		got, err := view.LoadWorkerReceipt(request.Key, receipt.EventID)
		if err != nil || !reflect.DeepEqual(got, receipt) {
			return fmt.Errorf("worker receipt = %#v, error %v", got, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertWorkerReceipt(receipt) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("duplicate worker receipt error = %v, want ErrStoreConflict", err)
	}
	conflicting := receipt
	conflicting.Digest = llmDigest('z')
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertWorkerReceipt(conflicting) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("conflicting worker receipt error = %v, want ErrStoreConflict", err)
	}
}

func testLLMStoreToolLedger(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	task := llmConformanceTask("tools", 1, llm.TaskAdmitted)
	aData := []byte("AAAA")
	records := []llm.StoreToolExecutionRecord{
		llmConformanceTool(task, "tool-b", llm.ToolExecutionPending, nil),
		llmConformanceTool(task, "tool-a", llm.ToolExecutionCompleted, aData),
		llmConformanceTool(task, "tool-c", llm.ToolExecutionPending, nil),
	}
	completedAt := llmConformanceTime(7)
	records[1].CompletedAt = &completedAt
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		for _, record := range records {
			if err := tx.InsertToolExecution(record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("insert tool ledger: %v", err)
	}
	mutateLLMBytes(aData)
	loaded := llmLoadTool(ctx, t, store, records[1].Key, int64(len("AAAA")))
	if string(loaded.Result) != "AAAA" {
		t.Fatalf("tool input ownership lost: %#v", loaded)
	}
	loaded.Result[0] ^= 0xff
	if again := llmLoadTool(ctx, t, store, records[1].Key, llmGenerousReadLimit().MaxBytes); string(again.Result) != "AAAA" {
		t.Fatalf("tool read returned aliased Result: %#v", again)
	}
	llmAssertOversized(t, "tool result", func() error {
		return store.View(ctx, func(view llm.StoreView) error {
			_, err := view.LoadToolExecution(records[1].Key, llm.StoreReadLimit{MaxBytes: 3})
			return err
		})
	})

	completedB := records[0]
	completedB.State = llm.ToolExecutionCompleted
	completedB.Result = []byte("BBBBBB")
	completedB.IsError = true
	completedB.Revision = 2
	completedB.CompletedAt = &completedAt
	err := store.Update(ctx, func(tx llm.StoreTx) error {
		changed, err := tx.CompareAndSwapToolExecution(llm.StoreToolExecutionMutation{
			Key: records[0].Key, ExpectedRevision: 1, Next: completedB,
		})
		if err != nil || !changed {
			return fmt.Errorf("matching tool CAS changed/error = %v/%v", changed, err)
		}
		stale := completedB
		stale.Revision = 3
		changed, err = tx.CompareAndSwapToolExecution(llm.StoreToolExecutionMutation{
			Key: records[0].Key, ExpectedRevision: 1, Next: stale,
		})
		if err != nil || changed {
			return fmt.Errorf("stale tool CAS changed/error = %v/%v", changed, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("tool ledger CAS: %v", err)
	}
	skippedRevision := completedB
	skippedRevision.Revision = 4
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		_, err := tx.CompareAndSwapToolExecution(llm.StoreToolExecutionMutation{
			Key: completedB.Key, ExpectedRevision: 2, Next: skippedRevision,
		})
		return err
	})
	if !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("skipped tool revision error = %v, want ErrStoreConflict", err)
	}
	immutable := completedB
	immutable.InputDigest = llmDigest('q')
	immutable.Revision = 3
	err = store.Update(ctx, func(tx llm.StoreTx) error {
		_, err := tx.CompareAndSwapToolExecution(llm.StoreToolExecutionMutation{
			Key: completedB.Key, ExpectedRevision: 2, Next: immutable,
		})
		return err
	})
	if !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("immutable tool CAS error = %v, want ErrStoreConflict", err)
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error { return tx.InsertToolExecution(records[1]) }); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("duplicate tool insert error = %v, want ErrStoreConflict", err)
	}

	all := llmScanTools(ctx, t, store, llm.StoreToolExecutionScan{
		Task: task.Key, Limit: 10, ReadLimit: llmGenerousReadLimit(),
	})
	if got := llmToolIDs(all); !reflect.DeepEqual(got, []llm.ToolCallID{"tool-a", "tool-b", "tool-c"}) {
		t.Fatalf("tool scan order = %v", got)
	}
	prefix := llmScanTools(ctx, t, store, llm.StoreToolExecutionScan{
		Task: task.Key, Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: 9},
	})
	if got := llmToolIDs(prefix); !reflect.DeepEqual(got, []llm.ToolCallID{"tool-a"}) {
		t.Fatalf("tool scan byte prefix = %v, want [tool-a]", got)
	}
	llmAssertOversized(t, "first tool scan record", func() error {
		return store.View(ctx, func(view llm.StoreView) error {
			_, err := view.ScanToolExecutions(llm.StoreToolExecutionScan{
				Task: task.Key, State: llm.ToolExecutionCompleted, Limit: 10,
				ReadLimit: llm.StoreReadLimit{MaxBytes: 3},
			})
			return err
		})
	})
	pending := llmScanTools(ctx, t, store, llm.StoreToolExecutionScan{
		Task: task.Key, State: llm.ToolExecutionPending, Limit: 10, ReadLimit: llmGenerousReadLimit(),
	})
	if got := llmToolIDs(pending); !reflect.DeepEqual(got, []llm.ToolCallID{"tool-c"}) {
		t.Fatalf("pending tool filter = %v, want [tool-c]", got)
	}
	after := llmScanTools(ctx, t, store, llm.StoreToolExecutionScan{
		Task: task.Key, After: "tool-a", Limit: 10, ReadLimit: llmGenerousReadLimit(),
	})
	if got := llmToolIDs(after); !reflect.DeepEqual(got, []llm.ToolCallID{"tool-b", "tool-c"}) {
		t.Fatalf("tool after cursor = %v", got)
	}
}

func testLLMStoreNilAndEmptyBytes(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	nilTask := llmConformanceTask("nil-bytes", 1, llm.TaskCompleted)
	emptyTask := llmConformanceTask("empty-bytes", 1, llm.TaskCompleted)
	nilRequest := llmConformanceRequest(nilTask, "nil-bytes", nil)
	emptyRequest := llmConformanceRequest(emptyTask, "empty-bytes", make([]byte, 0))
	completedAt := llmConformanceTime(8)
	for _, request := range []*llm.StoreRequestRecord{&nilRequest, &emptyRequest} {
		request.ResponseComplete = true
		request.CompletedAt = &completedAt
		request.Decision.StatusCode = 200
		request.Decision.ContentType = "application/json"
	}
	nilRequest.Decision.Body = nil
	emptyRequest.Decision.Body = make([]byte, 0)
	nilEvent := llmConformanceEvent(nilRequest.Key, 1, llm.StoreEventWire, "", "", nil)
	emptyEvent := llmConformanceEvent(emptyRequest.Key, 1, llm.StoreEventWire, "", "", make([]byte, 0))
	nilTool := llmConformanceTool(nilTask, "tool-nil", llm.ToolExecutionPending, nil)
	emptyTool := llmConformanceTool(emptyTask, "tool-empty", llm.ToolExecutionCompleted, make([]byte, 0))
	emptyTool.CompletedAt = &completedAt

	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		for _, task := range []llm.StoreTaskRecord{nilTask, emptyTask} {
			if err := tx.InsertTask(task); err != nil {
				return err
			}
		}
		for _, request := range []llm.StoreRequestRecord{nilRequest, emptyRequest} {
			if err := tx.InsertRequest(request); err != nil {
				return err
			}
		}
		for _, event := range []llm.StoreResponseEventRecord{nilEvent, emptyEvent} {
			if err := tx.InsertResponseEvent(event); err != nil {
				return err
			}
		}
		for _, tool := range []llm.StoreToolExecutionRecord{nilTool, emptyTool} {
			if err := tx.InsertToolExecution(tool); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("insert nil/empty byte fixtures: %v", err)
	}

	loadedNil := llmLoadRequest(ctx, t, store, nilRequest.Key, 1)
	loadedEmpty := llmLoadRequest(ctx, t, store, emptyRequest.Key, 1)
	if loadedNil.CanonicalPayload != nil || loadedNil.Decision.Body != nil {
		t.Fatalf("nil Request bytes became non-nil: %#v", loadedNil)
	}
	if loadedEmpty.CanonicalPayload == nil || loadedEmpty.Decision.Body == nil ||
		len(loadedEmpty.CanonicalPayload) != 0 || len(loadedEmpty.Decision.Body) != 0 {
		t.Fatalf("non-nil empty Request bytes lost identity: %#v", loadedEmpty)
	}
	if decision := llmLoadDecision(ctx, t, store, nilRequest.Key, llm.StoreReadLimit{MaxBytes: 1}); decision.Body != nil {
		t.Fatalf("nil decision Body became non-nil: %#v", decision)
	}
	if decision := llmLoadDecision(ctx, t, store, emptyRequest.Key, llm.StoreReadLimit{MaxBytes: 1}); decision.Body == nil || len(decision.Body) != 0 {
		t.Fatalf("non-nil empty decision Body lost identity: %#v", decision)
	}
	nilEvents := llmScanEvents(ctx, t, store, llm.StoreResponseEventScan{Request: nilRequest.Key, Limit: 1, ReadLimit: llm.StoreReadLimit{MaxBytes: 1}})
	emptyEvents := llmScanEvents(ctx, t, store, llm.StoreResponseEventScan{Request: emptyRequest.Key, Limit: 1, ReadLimit: llm.StoreReadLimit{MaxBytes: 1}})
	if nilEvents[0].Data != nil {
		t.Fatalf("nil response-event Data became non-nil: %#v", nilEvents[0])
	}
	if emptyEvents[0].Data == nil || len(emptyEvents[0].Data) != 0 {
		t.Fatalf("non-nil empty response-event Data lost identity: %#v", emptyEvents[0])
	}
	if tool := llmLoadTool(ctx, t, store, nilTool.Key, 1); tool.Result != nil {
		t.Fatalf("nil tool Result became non-nil: %#v", tool)
	}
	if tool := llmLoadTool(ctx, t, store, emptyTool.Key, 1); tool.Result == nil || len(tool.Result) != 0 {
		t.Fatalf("non-nil empty tool Result lost identity: %#v", tool)
	}
}

func testLLMStoreRecovery(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	type fixture struct {
		task    llm.StoreTaskRecord
		request llm.StoreRequestRecord
	}
	fixtures := []fixture{
		{llmConformanceTaskFor("caller-b", "recovery-first", 1, llm.TaskAdmitted), llm.StoreRequestRecord{}},
		{llmConformanceTaskFor("caller-a", "recovery-unacked", 1, llm.TaskAdmitted), llm.StoreRequestRecord{}},
		{llmConformanceTaskFor("caller-a", "recovery-incomplete", 1, llm.TaskAdmitted), llm.StoreRequestRecord{}},
		{llmConformanceTaskFor("caller-c", "recovery-acked", 1, llm.TaskAdmitted), llm.StoreRequestRecord{}},
		{llmConformanceTaskFor("caller-d", "recovery-quarantined", 1, llm.TaskAdmitted), llm.StoreRequestRecord{}},
		{llmConformanceTaskFor("caller-e", "recovery-digest-mismatch", 1, llm.TaskAdmitted), llm.StoreRequestRecord{}},
		{llmConformanceTaskFor("caller-z", "recovery-late", 1, llm.TaskAdmitted), llm.StoreRequestRecord{}},
	}
	for index := range fixtures {
		fixtures[index].request = llmConformanceRequest(fixtures[index].task, strings.TrimPrefix(string(fixtures[index].task.Key.Task), "task-"), []byte(fmt.Sprintf("payload-%d", index)))
	}
	fixtures[0].request.CreatedAt = llmConformanceTime(20)
	fixtures[1].request.CreatedAt = llmConformanceTime(20)
	fixtures[1].request.ResponseComplete = true
	completed := llmConformanceTime(21)
	fixtures[1].request.CompletedAt = &completed
	fixtures[2].request.CreatedAt = llmConformanceTime(20)
	fixtures[3].request.CreatedAt = llmConformanceTime(30)
	fixtures[3].request.ResponseComplete = true
	fixtures[3].request.CompletedAt = &completed
	fixtures[4].request.CreatedAt = llmConformanceTime(40)
	fixtures[4].request.RecoveryQuarantined = true
	fixtures[5].request.CreatedAt = llmConformanceTime(20)
	fixtures[5].request.ResponseComplete = true
	fixtures[5].request.CompletedAt = &completed
	fixtures[6].request.CreatedAt = llmConformanceTime(50)

	unackedEvent := llmConformanceEvent(fixtures[1].request.Key, 1, llm.StoreEventWire, "unacked-event", llmDigest('u'), []byte("wire-u"))
	ackedEvent := llmConformanceEvent(fixtures[3].request.Key, 1, llm.StoreEventWire, "acked-event", llmDigest('a'), []byte("wire-a"))
	mismatchedEvent := llmConformanceEvent(fixtures[5].request.Key, 1, llm.StoreEventWire, "mismatched-event", llmDigest('m'), []byte("wire-m"))
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		for _, fixture := range fixtures {
			if err := tx.InsertTask(fixture.task); err != nil {
				return err
			}
			if err := tx.InsertRequest(fixture.request); err != nil {
				return err
			}
		}
		if err := tx.InsertResponseEvent(unackedEvent); err != nil {
			return err
		}
		if err := tx.InsertResponseEvent(ackedEvent); err != nil {
			return err
		}
		if err := tx.InsertResponseEvent(mismatchedEvent); err != nil {
			return err
		}
		if err := tx.InsertWorkerReceipt(llm.StoreWorkerReceiptRecord{
			Request: ackedEvent.Request, EventID: ackedEvent.WorkerEventID, Worker: "worker-a",
			Digest: ackedEvent.WorkerEventDigest, CreatedAt: llmConformanceTime(31),
		}); err != nil {
			return err
		}
		return tx.InsertWorkerReceipt(llm.StoreWorkerReceiptRecord{
			Request: mismatchedEvent.Request, EventID: mismatchedEvent.WorkerEventID, Worker: "worker-a",
			Digest: llmDigest('n'), CreatedAt: llmConformanceTime(31),
		})
	}); err != nil {
		t.Fatalf("insert recovery fixtures: %v", err)
	}

	records := llmScanRecovery(ctx, t, store, llm.StoreRecoveryScan{Limit: 10, ReadLimit: llmGenerousReadLimit()})
	want := []llm.StoreRequestKey{
		fixtures[2].request.Key, fixtures[1].request.Key, fixtures[0].request.Key,
		fixtures[5].request.Key, fixtures[6].request.Key,
	}
	if got := llmRecoveryKeys(records); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovery order/selection = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(records[0].Task, fixtures[2].task) {
		t.Fatalf("recovery Task and Request were not one snapshot: %#v", records[0])
	}
	cursor := llm.StoreRecoveryCursor{
		CreatedAt: records[0].Request.CreatedAt, Caller: records[0].Request.Key.Caller,
		IdempotencyKey: records[0].Request.Key.IdempotencyKey,
	}
	after := llmScanRecovery(ctx, t, store, llm.StoreRecoveryScan{After: &cursor, Limit: 10, ReadLimit: llmGenerousReadLimit()})
	if got := llmRecoveryKeys(after); !reflect.DeepEqual(got, want[1:]) {
		t.Fatalf("recovery cursor = %v, want %v", got, want[1:])
	}
	callerCursor := llm.StoreRecoveryCursor{
		CreatedAt: records[2].Request.CreatedAt, Caller: records[2].Request.Key.Caller,
		IdempotencyKey: records[2].Request.Key.IdempotencyKey,
	}
	afterCaller := llmScanRecovery(ctx, t, store, llm.StoreRecoveryScan{After: &callerCursor, Limit: 10, ReadLimit: llmGenerousReadLimit()})
	if got := llmRecoveryKeys(afterCaller); !reflect.DeepEqual(got, want[3:]) {
		t.Fatalf("recovery caller cursor = %v, want %v", got, want[3:])
	}
	firstBytes := int64(len(fixtures[2].request.CanonicalPayload) + len(fixtures[2].request.Decision.Body))
	prefix := llmScanRecovery(ctx, t, store, llm.StoreRecoveryScan{Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: firstBytes}})
	if got := llmRecoveryKeys(prefix); !reflect.DeepEqual(got, want[:1]) {
		t.Fatalf("recovery byte prefix = %v, want %v", got, want[:1])
	}
	llmAssertOversized(t, "first recovery record", func() error {
		return store.View(ctx, func(view llm.StoreView) error {
			_, err := view.ScanRecovery(llm.StoreRecoveryScan{Limit: 10, ReadLimit: llm.StoreReadLimit{MaxBytes: firstBytes - 1}})
			return err
		})
	})
	records[0].Request.CanonicalPayload[0] ^= 0xff
	if again := llmScanRecovery(ctx, t, store, llm.StoreRecoveryScan{Limit: 1, ReadLimit: llmGenerousReadLimit()}); string(again[0].Request.CanonicalPayload) != string(fixtures[2].request.CanonicalPayload) {
		t.Fatal("recovery scan returned an aliased Request payload")
	}
}

func testLLMStoreRetention(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	type fixture struct {
		task    llm.StoreTaskRecord
		request llm.StoreRequestRecord
	}
	makeFixture := func(caller, id string, created, completed int64) fixture {
		task := llmConformanceTaskFor(llm.CallerID(caller), "retention-"+id, 1, llm.TaskAdmitted)
		request := llmConformanceRequest(task, "retention-"+id, []byte("retention-"+id))
		request.CreatedAt = llmConformanceTime(created)
		request.ResponseComplete = true
		if completed > 0 {
			value := llmConformanceTime(completed)
			request.CompletedAt = &value
		}
		return fixture{task, request}
	}
	fixtures := []fixture{
		makeFixture("caller-b", "legacy", 20, 0),
		makeFixture("caller-a", "unacked", 21, 25),
		makeFixture("caller-a", "acked", 22, 26),
		makeFixture("caller-a", "tie-a", 23, 30),
		makeFixture("caller-a", "tie-z", 24, 30),
		makeFixture("caller-b", "tie-b", 24, 30),
		makeFixture("caller-c", "future", 25, 40),
		makeFixture("caller-d", "tombstoned", 26, 27),
		makeFixture("caller-e", "incomplete", 27, 0),
	}
	fixtures[7].request.PayloadPrunedAt = func() *time.Time { value := llmConformanceTime(35); return &value }()
	fixtures[7].request.CanonicalPayload = nil
	fixtures[8].request.ResponseComplete = false
	unacked := llmConformanceEvent(fixtures[1].request.Key, 1, llm.StoreEventWire, "retention-unacked", llmDigest('u'), []byte("u"))
	acked := llmConformanceEvent(fixtures[2].request.Key, 1, llm.StoreEventWire, "retention-acked", llmDigest('a'), []byte("a"))
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		for _, fixture := range fixtures {
			if err := tx.InsertTask(fixture.task); err != nil {
				return err
			}
			if err := tx.InsertRequest(fixture.request); err != nil {
				return err
			}
		}
		if err := tx.InsertResponseEvent(unacked); err != nil {
			return err
		}
		if err := tx.InsertResponseEvent(acked); err != nil {
			return err
		}
		return tx.InsertWorkerReceipt(llm.StoreWorkerReceiptRecord{
			Request: acked.Request, EventID: acked.WorkerEventID, Worker: "worker-a",
			Digest: acked.WorkerEventDigest, CreatedAt: llmConformanceTime(27),
		})
	}); err != nil {
		t.Fatalf("insert retention fixtures: %v", err)
	}

	candidates := llmScanRetention(ctx, t, store, llm.StoreRetentionScan{
		CompletedBefore: llmConformanceTime(30), Limit: 20,
	})
	want := []llm.StoreRequestKey{
		fixtures[0].request.Key, fixtures[1].request.Key, fixtures[2].request.Key,
		fixtures[3].request.Key, fixtures[4].request.Key, fixtures[5].request.Key,
	}
	if got := llmRetentionKeys(candidates); !reflect.DeepEqual(got, want) {
		t.Fatalf("retention order/selection = %v, want %v", got, want)
	}
	if !candidates[1].UnacknowledgedWorkerEvent || candidates[2].UnacknowledgedWorkerEvent {
		t.Fatalf("retention unacknowledged markers = %#v", candidates)
	}
	if !candidates[0].EffectiveCompletedAt.Equal(fixtures[0].request.CreatedAt) {
		t.Fatalf("legacy effective completion = %v, want CreatedAt %v", candidates[0].EffectiveCompletedAt, fixtures[0].request.CreatedAt)
	}
	cursor := llm.StoreRetentionCursor{
		CompletedAt: candidates[1].EffectiveCompletedAt, Caller: candidates[1].Request.Key.Caller,
		IdempotencyKey: candidates[1].Request.Key.IdempotencyKey,
	}
	after := llmScanRetention(ctx, t, store, llm.StoreRetentionScan{
		CompletedBefore: llmConformanceTime(30), After: &cursor, Limit: 20,
	})
	if got := llmRetentionKeys(after); !reflect.DeepEqual(got, want[2:]) {
		t.Fatalf("retention cursor = %v, want %v", got, want[2:])
	}
	tieCursor := llm.StoreRetentionCursor{
		CompletedAt: candidates[3].EffectiveCompletedAt, Caller: candidates[3].Request.Key.Caller,
		IdempotencyKey: candidates[3].Request.Key.IdempotencyKey,
	}
	afterTie := llmScanRetention(ctx, t, store, llm.StoreRetentionScan{
		CompletedBefore: llmConformanceTime(30), After: &tieCursor, Limit: 20,
	})
	if got := llmRetentionKeys(afterTie); !reflect.DeepEqual(got, want[4:]) {
		t.Fatalf("retention tie cursor = %v, want %v", got, want[4:])
	}
}

func testLLMStoreTombstone(ctx context.Context, t *testing.T, store llm.Store) {
	t.Helper()
	task := llmConformanceTask("tombstone", 1, llm.TaskCompleted)
	request := llmConformanceRequest(task, "tombstone", []byte("private-canonical"))
	request.ResponseComplete = true
	completedAt := llmConformanceTime(20)
	request.CompletedAt = &completedAt
	request.Decision = llm.StoreResponseDecision{StatusCode: 200, ContentType: "application/json", Body: []byte("private-body")}
	events := []llm.StoreResponseEventRecord{
		llmConformanceEvent(request.Key, 1, llm.StoreEventCheckpoint, "", "", []byte("checkpoint")),
		llmConformanceEvent(request.Key, 2, llm.StoreEventWire, "terminal-event", llmDigest('t'), []byte("wire")),
	}
	receipt := llm.StoreWorkerReceiptRecord{
		Request: request.Key, EventID: "terminal-event", Worker: "worker-a",
		Digest: llmDigest('t'), CreatedAt: llmConformanceTime(21),
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertRequest(request); err != nil {
			return err
		}
		for _, event := range events {
			if err := tx.InsertResponseEvent(event); err != nil {
				return err
			}
		}
		return tx.InsertWorkerReceipt(receipt)
	}); err != nil {
		t.Fatalf("insert tombstone fixtures: %v", err)
	}
	if err := store.Update(ctx, func(tx llm.StoreTx) error {
		_, err := tx.DeleteTombstonedResponseEvents(request.Key)
		return err
	}); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("delete before tombstone error = %v, want ErrStoreConflict", err)
	}

	prunedAt := llmConformanceTime(30)
	tombstone := request
	tombstone.CanonicalPayload = nil
	tombstone.Decision.Body = nil
	tombstone.PayloadPrunedAt = &prunedAt
	tombstone.Revision = 2
	err := store.Update(ctx, func(tx llm.StoreTx) error {
		changed, err := tx.CompareAndSwapRequest(llm.StoreRequestMutation{
			Key: request.Key, ExpectedRevision: 1, Next: tombstone,
		})
		if err != nil || !changed {
			return fmt.Errorf("tombstone Request CAS changed/error = %v/%v", changed, err)
		}
		deleted, err := tx.DeleteTombstonedResponseEvents(request.Key)
		if err != nil || deleted != 2 {
			return fmt.Errorf("delete tombstoned events count/error = %d/%v", deleted, err)
		}
		if _, err := tx.LoadWorkerReceipt(request.Key, receipt.EventID); err != nil {
			return fmt.Errorf("receipt missing in tombstone transaction: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("tombstone transaction: %v", err)
	}
	stored := llmLoadRequest(ctx, t, store, request.Key, 1)
	if len(stored.CanonicalPayload) != 0 || len(stored.Decision.Body) != 0 || stored.PayloadPrunedAt == nil ||
		stored.RequestDigest != request.RequestDigest || stored.RequestID != request.RequestID || stored.ResponseID != request.ResponseID {
		t.Fatalf("invalid durable Request tombstone: %#v", stored)
	}
	if got := llmScanEvents(ctx, t, store, llm.StoreResponseEventScan{Request: request.Key, Limit: 10, ReadLimit: llmGenerousReadLimit()}); len(got) != 0 {
		t.Fatalf("tombstoned response events remain: %#v", got)
	}
	err = store.View(ctx, func(view llm.StoreView) error {
		got, err := view.LoadWorkerReceipt(request.Key, receipt.EventID)
		if err != nil || !reflect.DeepEqual(got, receipt) {
			return fmt.Errorf("receipt after tombstone = %#v, error %v", got, err)
		}
		candidates, err := view.ScanRetention(llm.StoreRetentionScan{CompletedBefore: llmConformanceTime(100), Limit: 10})
		if err != nil {
			return err
		}
		for _, candidate := range candidates {
			if candidate.Request.Key == request.Key {
				return errors.New("tombstoned Request remained a retention candidate")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func llmConformanceCodec() llm.CodecSnapshot {
	return llm.CodecSnapshot{
		Contract: framework.Contract{
			ID: llm.CodecContractID, Major: llm.CodecContractMajor,
			Features: map[framework.Feature]uint16{"humantest.seed": 1},
		},
		ID: "humantest.chat", Version: "2026.07.19",
		Fingerprint: llm.Fingerprint([]byte("humantest deterministic codec")),
	}
}

func llmConformanceTask(id string, revision uint64, state llm.TaskState) llm.StoreTaskRecord {
	return llmConformanceTaskFor("humantest-caller", id, revision, state)
}

func llmConformanceTaskFor(caller llm.CallerID, id string, revision uint64, state llm.TaskState) llm.StoreTaskRecord {
	return llm.StoreTaskRecord{
		Key:          llm.StoreTaskKey{Caller: caller, Task: llm.TaskID("task-" + id)},
		WorkspaceKey: "workspace-" + id, CapabilityTier: llm.TierWorkspace,
		Codec: llmConformanceCodec(), HarnessID: "humantest-harness",
		HarnessVersion: "1", HarnessSessionID: "session-" + id,
		WorkspaceRoot: "/workspace/" + id, ExecAllowed: true, State: state,
		Revision: revision, CreatedAt: llmConformanceTime(1), UpdatedAt: llmConformanceTime(1),
	}
}

func llmConformanceRequest(task llm.StoreTaskRecord, id string, payload []byte) llm.StoreRequestRecord {
	return llm.StoreRequestRecord{
		Key:  llm.StoreRequestKey{Caller: task.Key.Caller, IdempotencyKey: llm.IdempotencyKey("key-" + id)},
		Task: task.Key, RequestID: "request-" + id, ResponseID: "response-" + id,
		RequestDigest: llm.StoreDigest(llmDigest(rune(id[0]))), Codec: cloneLLMCodec(task.Codec),
		Mode: llm.ResponseStream, CanonicalPayload: payload,
		Revision: 1, CreatedAt: llmConformanceTime(2),
	}
}

func llmConformanceEvent(
	request llm.StoreRequestKey,
	sequence uint64,
	kind llm.StoreResponseEventKind,
	workerEventID string,
	workerDigest llm.StoreDigest,
	data []byte,
) llm.StoreResponseEventRecord {
	return llm.StoreResponseEventRecord{
		Request: request, Sequence: sequence, Kind: kind,
		WorkerEventID: workerEventID, WorkerEventDigest: workerDigest,
		Data: data, CreatedAt: llmConformanceTime(int64(sequence) + 2),
	}
}

func llmConformanceTool(task llm.StoreTaskRecord, id string, state llm.ToolExecutionState, result []byte) llm.StoreToolExecutionRecord {
	return llm.StoreToolExecutionRecord{
		Key:         llm.StoreToolExecutionKey{Task: task.Key, ToolCallID: llm.ToolCallID(id)},
		InputDigest: llmDigest(rune(id[len(id)-1])), State: state, Result: result,
		Revision: 1, CreatedAt: llmConformanceTime(3),
	}
}

func llmTaskAffinity(task llm.StoreTaskRecord) llm.StoreTaskAffinity {
	return llm.StoreTaskAffinity{
		Caller: task.Key.Caller, WorkspaceKey: task.WorkspaceKey,
		HarnessID: task.HarnessID, HarnessVersion: task.HarnessVersion,
		HarnessSessionID: task.HarnessSessionID,
	}
}

func llmConformanceTime(second int64) time.Time {
	return time.Unix(second, 123_000_000).UTC()
}

func llmDigest(character rune) llm.StoreDigest {
	if character == 0 {
		character = '0'
	}
	return llm.StoreDigest("sha256:" + strings.Repeat(string(character), 64))
}

func llmGenerousReadLimit() llm.StoreReadLimit { return llm.StoreReadLimit{MaxBytes: 1 << 20} }

func cloneLLMCodec(codec llm.CodecSnapshot) llm.CodecSnapshot {
	cloned := codec
	cloned.Contract.Features = make(map[framework.Feature]uint16, len(codec.Contract.Features))
	for feature, version := range codec.Contract.Features {
		cloned.Contract.Features[feature] = version
	}
	return cloned
}

func mutateLLMBytes(value []byte) {
	for index := range value {
		value[index] ^= 0xff
	}
}

func llmAssertOversized(t *testing.T, label string, operation func() error) {
	t.Helper()
	if err := operation(); !errors.Is(err, llm.ErrStoreRecordTooLarge) {
		t.Fatalf("%s error = %v, want ErrStoreRecordTooLarge", label, err)
	}
}

func llmLoadTask(ctx context.Context, t *testing.T, store llm.Store, key llm.StoreTaskKey) llm.StoreTaskRecord {
	t.Helper()
	var result llm.StoreTaskRecord
	if err := store.View(ctx, func(view llm.StoreView) error {
		var err error
		result, err = view.LoadTask(key)
		return err
	}); err != nil {
		t.Fatalf("load Task %v: %v", key, err)
	}
	return result
}

func llmAssertTask(ctx context.Context, t *testing.T, store llm.Store, want llm.StoreTaskRecord) {
	t.Helper()
	if got := llmLoadTask(ctx, t, store, want.Key); !reflect.DeepEqual(got, want) {
		t.Fatalf("Task = %#v, want %#v", got, want)
	}
}

func llmLoadRequest(ctx context.Context, t *testing.T, store llm.Store, key llm.StoreRequestKey, limit int64) llm.StoreRequestRecord {
	t.Helper()
	var result llm.StoreRequestRecord
	if err := store.View(ctx, func(view llm.StoreView) error {
		var err error
		result, err = view.LoadRequest(key, llm.StoreReadLimit{MaxBytes: limit})
		return err
	}); err != nil {
		t.Fatalf("load Request %v: %v", key, err)
	}
	return result
}

func llmLoadDecision(ctx context.Context, t *testing.T, store llm.Store, key llm.StoreRequestKey, limit llm.StoreReadLimit) llm.StoreResponseDecision {
	t.Helper()
	var result llm.StoreResponseDecision
	if err := store.View(ctx, func(view llm.StoreView) error {
		var err error
		result, err = view.LoadResponseDecision(key, limit)
		return err
	}); err != nil {
		t.Fatalf("load response decision %v: %v", key, err)
	}
	return result
}

func llmScanEvents(ctx context.Context, t *testing.T, store llm.Store, scan llm.StoreResponseEventScan) []llm.StoreResponseEventRecord {
	t.Helper()
	var result []llm.StoreResponseEventRecord
	if err := store.View(ctx, func(view llm.StoreView) error {
		var err error
		result, err = view.ScanResponseEvents(scan)
		return err
	}); err != nil {
		t.Fatalf("scan response events: %v", err)
	}
	return result
}

func llmEventSequences(records []llm.StoreResponseEventRecord) []uint64 {
	result := make([]uint64, len(records))
	for index := range records {
		result[index] = records[index].Sequence
	}
	return result
}

func llmLoadTool(ctx context.Context, t *testing.T, store llm.Store, key llm.StoreToolExecutionKey, limit int64) llm.StoreToolExecutionRecord {
	t.Helper()
	var result llm.StoreToolExecutionRecord
	if err := store.View(ctx, func(view llm.StoreView) error {
		var err error
		result, err = view.LoadToolExecution(key, llm.StoreReadLimit{MaxBytes: limit})
		return err
	}); err != nil {
		t.Fatalf("load tool execution %v: %v", key, err)
	}
	return result
}

func llmScanTools(ctx context.Context, t *testing.T, store llm.Store, scan llm.StoreToolExecutionScan) []llm.StoreToolExecutionRecord {
	t.Helper()
	var result []llm.StoreToolExecutionRecord
	if err := store.View(ctx, func(view llm.StoreView) error {
		var err error
		result, err = view.ScanToolExecutions(scan)
		return err
	}); err != nil {
		t.Fatalf("scan tool executions: %v", err)
	}
	return result
}

func llmToolIDs(records []llm.StoreToolExecutionRecord) []llm.ToolCallID {
	result := make([]llm.ToolCallID, len(records))
	for index := range records {
		result[index] = records[index].Key.ToolCallID
	}
	return result
}

func llmScanRecovery(ctx context.Context, t *testing.T, store llm.Store, scan llm.StoreRecoveryScan) []llm.StoreRecoveryRecord {
	t.Helper()
	var result []llm.StoreRecoveryRecord
	if err := store.View(ctx, func(view llm.StoreView) error {
		var err error
		result, err = view.ScanRecovery(scan)
		return err
	}); err != nil {
		t.Fatalf("scan recovery: %v", err)
	}
	return result
}

func llmRecoveryKeys(records []llm.StoreRecoveryRecord) []llm.StoreRequestKey {
	result := make([]llm.StoreRequestKey, len(records))
	for index := range records {
		result[index] = records[index].Request.Key
	}
	return result
}

func llmScanRetention(ctx context.Context, t *testing.T, store llm.Store, scan llm.StoreRetentionScan) []llm.StoreRetentionCandidate {
	t.Helper()
	var result []llm.StoreRetentionCandidate
	if err := store.View(ctx, func(view llm.StoreView) error {
		var err error
		result, err = view.ScanRetention(scan)
		return err
	}); err != nil {
		t.Fatalf("scan retention: %v", err)
	}
	return result
}

func llmRetentionKeys(records []llm.StoreRetentionCandidate) []llm.StoreRequestKey {
	result := make([]llm.StoreRequestKey, len(records))
	for index := range records {
		result[index] = records[index].Request.Key
	}
	return result
}
