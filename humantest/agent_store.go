// Package humantest provides reusable black-box conformance suites for Human
// framework ports. Provider packages run these suites against the same factory
// they expose to applications; the suites never depend on a physical schema or
// another implementation detail.
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

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

// AgentStoreFactory opens a new, empty Agent Store for one conformance
// subtest. Every invocation must return an independent Store. release must be
// non-nil, idempotent, and release every resource opened by the factory.
// TestAgentStore always invokes it during cleanup and may invoke it earlier to
// verify the released Store's error classification.
//
// A provider normally exposes a test like:
//
//	func TestStoreConformance(t *testing.T) {
//		humantest.TestAgentStore(t, func(ctx context.Context, t testing.TB) (
//			agent.Store, framework.ReleaseFunc, error,
//		) {
//			return openTestStore(ctx, t.TempDir())
//		})
//	}
//
// The context bounds construction and the subtest and must not be retained.
// release receives a fresh context so a failed or cancelled operation cannot
// prevent deterministic cleanup.
type AgentStoreFactory func(
	context.Context,
	testing.TB,
) (agent.Store, framework.ReleaseFunc, error)

// TestAgentStore runs the mandatory Agent Store primitive conformance suite.
// It verifies the major-contract foundations on which Agent commands depend:
// metadata negotiation, callback cardinality, atomic rollback, transactional
// read-your-writes, every StoreView/StoreTx primitive, scan ordering and
// pagination, task/Artifact/Workspace CAS, lease fencing, typed immutable
// insert conflicts, stable View snapshots, opaque-byte ownership and bounded
// reads, and strict serialization under concurrent Updates.
//
// This is a black-box suite. Passing it is necessary but does not replace a
// provider's durability, fault-injection, migration, and infrastructure tests.
func TestAgentStore(t *testing.T, factory AgentStoreFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("Agent Store conformance factory is nil")
	}

	tests := []struct {
		name string
		run  func(context.Context, *testing.T, agent.Store, framework.ReleaseFunc)
	}{
		{"description_and_contract", adaptAgentStoreTest(testAgentStoreDescription)},
		{"callback_exactly_once", adaptAgentStoreTest(testAgentStoreCallbackExactlyOnce)},
		{"context_and_callback_lifetime", adaptAgentStoreTest(testAgentStoreContextAndCallbackLifetime)},
		{"empty_reads_and_limits", adaptAgentStoreTest(testAgentStoreEmptyReadsAndLimits)},
		{"update_rollback", adaptAgentStoreTest(testAgentStoreUpdateRollback)},
		{"read_your_writes", adaptAgentStoreTest(testAgentStoreReadYourWrites)},
		{"task_compare_and_swap", adaptAgentStoreTest(testAgentStoreTaskCAS)},
		{"immutable_insert_conflict", adaptAgentStoreTest(testAgentStoreImmutableConflict)},
		{"typed_logical_constraints", adaptAgentStoreTest(testAgentStoreLogicalConstraints)},
		{"task_resolution_and_scans", adaptAgentStoreTest(testAgentStoreTaskScans)},
		{"message_and_event_scans", adaptAgentStoreTest(testAgentStoreMessageAndEventScans)},
		{"lease_grants_claim_and_scan", adaptAgentStoreTest(testAgentStoreLeases)},
		{"artifact_submission_and_receipt", adaptAgentStoreTest(testAgentStoreArtifactSubmissionReceipt)},
		{"workspace_head_compare_and_swap", adaptAgentStoreTest(testAgentStoreWorkspaceHead)},
		{"stable_view_snapshot", adaptAgentStoreTest(testAgentStoreSnapshot)},
		{"command_bytes_and_limit", adaptAgentStoreTest(testAgentStoreCommandBytes)},
		{"message_bytes_and_limit", adaptAgentStoreTest(testAgentStoreMessageBytes)},
		{"artifact_bytes_and_limit", adaptAgentStoreTest(testAgentStoreArtifactBytes)},
		{"concurrent_updates_are_serializable", adaptAgentStoreTest(testAgentStoreConcurrentUpdates)},
		{"released_store_is_closed", testAgentStoreReleased},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			t.Cleanup(cancel)

			store, release, err := factory(ctx, t)
			if release != nil {
				t.Cleanup(func() {
					releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer releaseCancel()
					if err := release(releaseCtx); err != nil {
						t.Errorf("release Agent Store: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatalf("open fresh Agent Store: %v", err)
			}
			if store == nil {
				t.Fatal("factory returned a nil Agent Store")
			}
			if release == nil {
				t.Fatal("factory returned a nil release function")
			}

			test.run(ctx, t, store, release)
		})
	}
}

func adaptAgentStoreTest(
	test func(context.Context, *testing.T, agent.Store),
) func(context.Context, *testing.T, agent.Store, framework.ReleaseFunc) {
	return func(ctx context.Context, t *testing.T, store agent.Store, _ framework.ReleaseFunc) {
		test(ctx, t, store)
	}
}

func testAgentStoreDescription(_ context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	first := store.Description()
	if err := first.Validate(); err != nil {
		t.Fatalf("Description does not satisfy Agent Store contract: %v", err)
	}
	second := store.Description()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Description is not static:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

func testAgentStoreCallbackExactlyOnce(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	viewFailure := errors.New("humantest View callback result")
	var viewCalls atomic.Int32
	err := store.View(ctx, func(agent.StoreView) error {
		viewCalls.Add(1)
		return viewFailure
	})
	if !errors.Is(err, viewFailure) {
		t.Fatalf("View callback error = %v, want %v", err, viewFailure)
	}
	if calls := viewCalls.Load(); calls != 1 {
		t.Fatalf("View callback calls = %d, want exactly 1", calls)
	}

	updateFailure := errors.New("humantest Update callback result")
	var updateCalls atomic.Int32
	err = store.Update(ctx, func(agent.StoreTx) error {
		updateCalls.Add(1)
		return updateFailure
	})
	if !errors.Is(err, updateFailure) {
		t.Fatalf("Update callback error = %v, want %v", err, updateFailure)
	}
	if calls := updateCalls.Load(); calls != 1 {
		t.Fatalf("Update callback calls = %d, want exactly 1", calls)
	}
}

func testAgentStoreUpdateRollback(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	command := conformanceCommand("rollback-command", []byte("must-not-commit"))
	task := conformanceTask("rollback-task", 1)
	rollback := errors.New("humantest requested rollback")
	err := store.Update(ctx, func(tx agent.StoreTx) error {
		if err := tx.InsertCommand(command); err != nil {
			return err
		}
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("Update rollback error = %v, want %v", err, rollback)
	}

	err = store.View(ctx, func(view agent.StoreView) error {
		if _, err := view.LookupCommand(commandKey(command), generousReadLimit()); !errors.Is(err, agent.ErrStoreRecordNotFound) {
			return fmt.Errorf("rolled-back command lookup error = %v, want not found", err)
		}
		if _, err := view.LoadTask(task.Task.Ref); !errors.Is(err, agent.ErrStoreRecordNotFound) {
			return fmt.Errorf("rolled-back task lookup error = %v, want not found", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		if err := tx.InsertCommand(command); err != nil {
			return err
		}
		return tx.InsertTask(task)
	}); err != nil {
		t.Fatalf("rolled-back identities remained reserved: %v", err)
	}

	// Uncommitted writes must not leak to a concurrent View. An MVCC Store may
	// return the old snapshot immediately; a lock-serialized Store may wait until
	// rollback. Both must report the staged identities as absent.
	dirtyCommand := conformanceCommand("dirty-command", []byte("uncommitted"))
	dirtyTask := conformanceTask("dirty-task", 1)
	staged := make(chan struct{})
	finishRollback := make(chan struct{})
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- store.Update(ctx, func(tx agent.StoreTx) error {
			if err := tx.InsertCommand(dirtyCommand); err != nil {
				return err
			}
			if err := tx.InsertTask(dirtyTask); err != nil {
				return err
			}
			close(staged)
			select {
			case <-finishRollback:
				return rollback
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-staged:
	case err := <-updateDone:
		t.Fatalf("Update ended before staging rollback records: %v", err)
	case <-ctx.Done():
		t.Fatalf("wait for staged rollback records: %v", ctx.Err())
	}

	viewDone := make(chan error, 1)
	go func() {
		viewDone <- store.View(ctx, func(view agent.StoreView) error {
			if _, err := view.LookupCommand(commandKey(dirtyCommand), generousReadLimit()); !errors.Is(err, agent.ErrStoreRecordNotFound) {
				return fmt.Errorf("uncommitted command became visible: %v", err)
			}
			if _, err := view.LoadTask(dirtyTask.Task.Ref); !errors.Is(err, agent.ErrStoreRecordNotFound) {
				return fmt.Errorf("uncommitted task became visible: %v", err)
			}
			return nil
		})
	}()
	earlyView := false
	select {
	case err := <-viewDone:
		earlyView = true
		if err != nil {
			close(finishRollback)
			t.Fatalf("View observed uncommitted rollback writes: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		close(finishRollback)
		t.Fatalf("wait for concurrent rollback View: %v", ctx.Err())
	}
	close(finishRollback)
	select {
	case err := <-updateDone:
		if !errors.Is(err, rollback) {
			t.Fatalf("concurrent rollback error = %v, want %v", err, rollback)
		}
	case <-ctx.Done():
		t.Fatalf("wait for concurrent rollback: %v", ctx.Err())
	}
	if !earlyView {
		select {
		case err := <-viewDone:
			if err != nil {
				t.Fatalf("View after concurrent rollback: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("wait for View after rollback: %v", ctx.Err())
		}
	}
}

func testAgentStoreReadYourWrites(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	command := conformanceCommand("read-own-command", []byte("canonical-result"))
	task := conformanceTask("read-own-task", 1)
	err := store.Update(ctx, func(tx agent.StoreTx) error {
		if err := tx.InsertCommand(command); err != nil {
			return err
		}
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		storedCommand, err := tx.LookupCommand(commandKey(command), generousReadLimit())
		if err != nil {
			return fmt.Errorf("read inserted command in Update: %w", err)
		}
		if !reflect.DeepEqual(storedCommand, command) {
			return fmt.Errorf("read inserted command = %#v, want %#v", storedCommand, command)
		}
		storedTask, err := tx.LoadTask(task.Task.Ref)
		if err != nil {
			return fmt.Errorf("read inserted task in Update: %w", err)
		}
		if !reflect.DeepEqual(storedTask, task) {
			return fmt.Errorf("read inserted task = %#v, want %#v", storedTask, task)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read-your-writes Update: %v", err)
	}
	assertCommandRecord(ctx, t, store, command)
	assertTaskRecord(ctx, t, store, task)
}

func testAgentStoreTaskCAS(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	initial := conformanceTask("cas-task", 1)
	if err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertTask(initial) }); err != nil {
		t.Fatalf("insert CAS task: %v", err)
	}

	unleased := agent.StoreLeaseState{}
	leased := initial
	leased.Task.Revision = 2
	leased.Task.EventCount = 2
	leased.Lease = agent.StoreLeaseState{Owner: "worker-a", Fence: 1}
	err := store.Update(ctx, func(tx agent.StoreTx) error {
		changed, err := tx.CompareAndSwapTask(agent.StoreTaskMutation{
			Ref: initial.Task.Ref,
			Condition: agent.StoreTaskCondition{
				ExpectedRevision: 1,
				ExpectedLease:    &unleased,
			},
			Next: leased,
		})
		if err != nil {
			return err
		}
		if !changed {
			return errors.New("matching task CAS returned false")
		}
		stored, err := tx.LoadTask(initial.Task.Ref)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(stored, leased) {
			return fmt.Errorf("task after matching CAS = %#v, want %#v", stored, leased)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("matching task CAS: %v", err)
	}

	staleNext := leased
	staleNext.Task.Revision = 99
	staleNext.Task.EventCount = 99
	err = store.Update(ctx, func(tx agent.StoreTx) error {
		changed, err := tx.CompareAndSwapTask(agent.StoreTaskMutation{
			Ref:       initial.Task.Ref,
			Condition: agent.StoreTaskCondition{ExpectedRevision: 1},
			Next:      staleNext,
		})
		if err != nil {
			return err
		}
		if changed {
			return errors.New("stale task revision CAS returned true")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stale task CAS: %v", err)
	}

	wrongLease := agent.StoreLeaseState{Owner: "worker-b", Fence: 1}
	err = store.Update(ctx, func(tx agent.StoreTx) error {
		changed, err := tx.CompareAndSwapTask(agent.StoreTaskMutation{
			Ref: initial.Task.Ref,
			Condition: agent.StoreTaskCondition{
				ExpectedRevision: 2,
				ExpectedLease:    &wrongLease,
			},
			Next: staleNext,
		})
		if err != nil {
			return err
		}
		if changed {
			return errors.New("wrong lease/fence task CAS returned true")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("wrong-lease task CAS: %v", err)
	}

	assertTaskRecord(ctx, t, store, leased)
}

func testAgentStoreImmutableConflict(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	original := conformanceCommand("immutable-command", []byte("first-result"))
	if err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertCommand(original) }); err != nil {
		t.Fatalf("insert immutable command: %v", err)
	}
	if err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertCommand(original) }); !errors.Is(err, agent.ErrStoreConflict) {
		t.Fatalf("exact immutable duplicate insert error = %v, want ErrStoreConflict", err)
	}

	conflicting := original
	conflicting.Result = []byte("different-result")
	err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertCommand(conflicting) })
	if !errors.Is(err, agent.ErrStoreConflict) {
		t.Fatalf("immutable duplicate insert error = %v, want ErrStoreConflict", err)
	}
	assertCommandRecord(ctx, t, store, original)
}

func testAgentStoreSnapshot(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	initial := conformanceTask("snapshot-task", 1)
	if err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertTask(initial) }); err != nil {
		t.Fatalf("insert snapshot task: %v", err)
	}

	firstRead := make(chan struct{})
	continueView := make(chan struct{})
	viewDone := make(chan error, 1)
	go func() {
		viewDone <- store.View(ctx, func(view agent.StoreView) error {
			first, err := view.LoadTask(initial.Task.Ref)
			if err != nil {
				return err
			}
			if first.Task.Revision != 1 {
				return fmt.Errorf("first snapshot revision = %d, want 1", first.Task.Revision)
			}
			close(firstRead)
			select {
			case <-continueView:
			case <-ctx.Done():
				return ctx.Err()
			}
			second, err := view.LoadTask(initial.Task.Ref)
			if err != nil {
				return err
			}
			if second.Task.Revision != 1 {
				return fmt.Errorf("View snapshot changed from revision 1 to %d", second.Task.Revision)
			}
			return nil
		})
	}()

	select {
	case <-firstRead:
	case err := <-viewDone:
		t.Fatalf("View ended before first snapshot read: %v", err)
	case <-ctx.Done():
		t.Fatalf("wait for first snapshot read: %v", ctx.Err())
	}

	updated := initial
	updated.Task.Revision = 2
	updated.Task.EventCount = 2
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- store.Update(ctx, func(tx agent.StoreTx) error {
			changed, err := tx.CompareAndSwapTask(agent.StoreTaskMutation{
				Ref:       initial.Task.Ref,
				Condition: agent.StoreTaskCondition{ExpectedRevision: 1},
				Next:      updated,
			})
			if err != nil {
				return err
			}
			if !changed {
				return errors.New("snapshot writer CAS returned false")
			}
			return nil
		})
	}()

	// An MVCC implementation may commit while View is open; a lock-serialized
	// implementation may wait for View. Both are valid. Giving the writer a short
	// opportunity to commit makes an unstable non-snapshot View observable without
	// requiring concurrent progress from every conforming Store.
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

	select {
	case err := <-viewDone:
		if err != nil {
			t.Fatalf("stable View snapshot: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for View completion: %v", ctx.Err())
	}
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("snapshot writer completion: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for snapshot writer completion: %v", ctx.Err())
	}
	assertTaskRecord(ctx, t, store, updated)
}

func testAgentStoreCommandBytes(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	originalBytes := []byte("command-canonical-bytes")
	record := conformanceCommand("byte-command", originalBytes)
	if err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertCommand(record) }); err != nil {
		t.Fatalf("insert byte command: %v", err)
	}
	mutateBytes(originalBytes)

	load := func(limit int64) (agent.StoreCommandRecord, error) {
		var stored agent.StoreCommandRecord
		err := store.View(ctx, func(view agent.StoreView) error {
			var err error
			stored, err = view.LookupCommand(commandKey(record), agent.StoreReadLimit{MaxBytes: limit})
			return err
		})
		return stored, err
	}
	stored, err := load(int64(len("command-canonical-bytes")))
	if err != nil {
		t.Fatalf("load command at exact byte limit: %v", err)
	}
	if got := string(stored.Result); got != "command-canonical-bytes" {
		t.Fatalf("stored command bytes = %q, want original bytes", got)
	}
	mutateBytes(stored.Result)
	again, err := load(generousReadLimit().MaxBytes)
	if err != nil || string(again.Result) != "command-canonical-bytes" {
		t.Fatalf("command read alias changed persisted bytes: record=%#v error=%v", again, err)
	}
	assertOversizedRead(t, "command zero limit", func() error { _, err := load(0); return err })
	assertOversizedRead(t, "command short limit", func() error {
		_, err := load(int64(len("command-canonical-bytes") - 1))
		return err
	})
}

func testAgentStoreMessageBytes(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	task := conformanceTask("byte-message-task", 1)
	originalBytes := []byte("message-canonical-parts")
	record := agent.StoreMessageRecord{
		ID: "byte-message", Task: task.Task.Ref, Sequence: 1,
		Author: agent.AuthorCaller, EncodedParts: originalBytes,
		PartsDigest: "sha256:message", CreatedAt: conformanceTime(2),
	}
	want := record
	want.EncodedParts = append([]byte(nil), record.EncodedParts...)
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		return tx.InsertMessage(record)
	}); err != nil {
		t.Fatalf("insert byte message: %v", err)
	}
	mutateBytes(originalBytes)

	key := agent.StoreMessageKey{Authority: task.Task.Ref.Workspace.Authority, ID: record.ID}
	load := func(limit int64) (agent.StoreMessageRecord, error) {
		var stored agent.StoreMessageRecord
		err := store.View(ctx, func(view agent.StoreView) error {
			var err error
			stored, err = view.LoadMessage(key, agent.StoreReadLimit{MaxBytes: limit})
			return err
		})
		return stored, err
	}
	stored, err := load(int64(len("message-canonical-parts")))
	if err != nil || !reflect.DeepEqual(stored, want) {
		t.Fatalf("message bytes at exact limit: record=%#v error=%v", stored, err)
	}
	mutateBytes(stored.EncodedParts)
	again, err := load(generousReadLimit().MaxBytes)
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatalf("message read alias changed persisted bytes: record=%#v error=%v", again, err)
	}
	assertOversizedRead(t, "message zero limit", func() error { _, err := load(0); return err })
	assertOversizedRead(t, "message short limit", func() error {
		_, err := load(int64(len("message-canonical-parts") - 1))
		return err
	})
}

func testAgentStoreArtifactBytes(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	task := conformanceTask("byte-artifact-task", 1)
	ref := agent.ArtifactRef{Workspace: task.Task.Ref.Workspace, ID: "byte-artifact"}
	state := agent.ArtifactFrozen
	linkedTask := task
	linkedTask.Task.Revision = 2
	linkedTask.Task.EventCount = 2
	linkedTask.Task.State = agent.TaskWorking
	linkedTask.Task.UpdatedAt = conformanceTime(3)
	linkedTask.Task.Artifact = &ref
	linkedTask.ArtifactState = &state
	originalBytes := []byte("artifact-canonical-payload")
	record := agent.StoreArtifactRecord{
		Artifact: agent.Artifact{
			Ref: ref, Task: task.Task.Ref, State: state,
			BaseRevision: "revision-base", ResultRevision: "revision-result",
			Digest: "sha256:artifact", PayloadDigest: "sha256:payload",
			// Plaintext size is deliberately unrelated to EncodedPayload length.
			// Store adapters must apply read limits to physical encoded bytes while
			// preserving the core-owned plaintext metadata exactly.
			PayloadSize: 7, MediaType: "application/octet-stream",
			FrozenAt: conformanceTime(3),
		},
		EncodedPayload: originalBytes,
	}
	want := record
	want.EncodedPayload = append([]byte(nil), record.EncodedPayload...)
	if err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertTask(task) }); err != nil {
		t.Fatalf("insert Artifact task: %v", err)
	}
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		if err := tx.InsertArtifact(record); err != nil {
			return err
		}
		changed, err := tx.CompareAndSwapTask(agent.StoreTaskMutation{
			Ref:       task.Task.Ref,
			Condition: agent.StoreTaskCondition{ExpectedRevision: 1},
			Next:      linkedTask,
		})
		if err != nil {
			return err
		}
		if !changed {
			return errors.New("link Artifact task CAS returned false")
		}
		return nil
	}); err != nil {
		t.Fatalf("insert byte Artifact: %v", err)
	}
	mutateBytes(originalBytes)

	load := func(limit int64) (agent.StoreArtifactRecord, error) {
		var stored agent.StoreArtifactRecord
		err := store.View(ctx, func(view agent.StoreView) error {
			var err error
			stored, err = view.LoadArtifact(ref, agent.StoreReadLimit{MaxBytes: limit})
			return err
		})
		return stored, err
	}
	stored, err := load(int64(len("artifact-canonical-payload")))
	if err != nil || !reflect.DeepEqual(stored, want) {
		t.Fatalf("Artifact bytes at exact limit: record=%#v error=%v", stored, err)
	}
	mutateBytes(stored.EncodedPayload)
	again, err := load(generousReadLimit().MaxBytes)
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatalf("Artifact read alias changed persisted bytes: record=%#v error=%v", again, err)
	}
	assertOversizedRead(t, "Artifact zero limit", func() error { _, err := load(0); return err })
	assertOversizedRead(t, "Artifact short limit", func() error {
		_, err := load(int64(len("artifact-canonical-payload") - 1))
		return err
	})
}

func testAgentStoreConcurrentUpdates(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	initial := conformanceTask("serial-task", 1)
	if err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertTask(initial) }); err != nil {
		t.Fatalf("insert serialization task: %v", err)
	}

	const writers = 12
	start := make(chan struct{})
	results := make(chan error, writers)
	callbackCalls := make([]atomic.Int32, writers)
	for index := range writers {
		go func() {
			select {
			case <-start:
			case <-ctx.Done():
				results <- ctx.Err()
				return
			}
			err := store.Update(ctx, func(tx agent.StoreTx) error {
				callbackCalls[index].Add(1)
				current, err := tx.LoadTask(initial.Task.Ref)
				if err != nil {
					return err
				}
				next := current
				next.Task.Revision++
				next.Task.EventCount++
				changed, err := tx.CompareAndSwapTask(agent.StoreTaskMutation{
					Ref:       initial.Task.Ref,
					Condition: agent.StoreTaskCondition{ExpectedRevision: current.Task.Revision},
					Next:      next,
				})
				if err != nil {
					return err
				}
				if !changed {
					return fmt.Errorf("CAS missed after reading revision %d in the same serializable Update", current.Task.Revision)
				}
				return nil
			})
			results <- err
		}()
	}
	close(start)
	successes := uint64(0)
	for range writers {
		select {
		case err := <-results:
			if errors.Is(err, agent.ErrStoreCommitUnknown) {
				t.Errorf("concurrent Update returned an ambiguous commit without fault injection: %v", err)
			} else if err == nil {
				successes++
			}
		case <-ctx.Done():
			t.Fatalf("wait for concurrent Updates: %v", ctx.Err())
		}
	}
	for index := range writers {
		if calls := callbackCalls[index].Load(); calls != 1 {
			t.Errorf("concurrent Update callback %d calls = %d, want exactly 1", index, calls)
		}
	}
	if t.Failed() {
		return
	}
	if successes == 0 {
		t.Fatal("every concurrent Update aborted; serialization made no progress")
	}

	want := initial
	// Serializable implementations may serialize all writers internally or abort
	// a contending transaction after its one callback. Both are valid. Every nil
	// result, however, is a committed atomic increment, while every definite error
	// guarantees no commit. The final revision therefore has this exact value.
	want.Task.Revision += successes
	want.Task.EventCount += successes
	assertTaskRecord(ctx, t, store, want)
}

func conformanceCommand(id agent.CommandID, result []byte) agent.StoreCommandRecord {
	return agent.StoreCommandRecord{
		Authority: "humantest-authority", ID: id, Kind: "humantest-command",
		IntentDigest: agent.StoreDigest(strings.Repeat("a", 64)), ResultKind: "humantest-result",
		Result: result, ResultDigest: agent.StoreDigest(strings.Repeat("b", 64)), CreatedAt: conformanceTime(1),
	}
}

func conformanceTask(id agent.TaskID, revision uint64) agent.StoreTaskRecord {
	workspaceRef := agent.WorkspaceRef{Authority: "humantest-authority", ID: "humantest-workspace"}
	return agent.StoreTaskRecord{Task: agent.Task{
		Ref:     agent.TaskRef{Workspace: workspaceRef, ID: id},
		Context: agent.ContextRef{Authority: workspaceRef.Authority, ID: "humantest-context"},
		State:   agent.TaskSubmitted, Revision: revision,
		MessageCount: 1, EventCount: revision,
		CreatedAt: conformanceTime(1), UpdatedAt: conformanceTime(1),
	}}
}

func conformanceTime(second int64) time.Time {
	return time.Unix(second, 123_000_000).UTC()
}

func commandKey(record agent.StoreCommandRecord) agent.StoreCommandKey {
	return agent.StoreCommandKey{Authority: record.Authority, ID: record.ID}
}

func generousReadLimit() agent.StoreReadLimit {
	return agent.StoreReadLimit{MaxBytes: 1 << 20}
}

func assertCommandRecord(
	ctx context.Context,
	t *testing.T,
	store agent.Store,
	want agent.StoreCommandRecord,
) {
	t.Helper()
	err := store.View(ctx, func(view agent.StoreView) error {
		got, err := view.LookupCommand(commandKey(want), generousReadLimit())
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("command = %#v, want %#v", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("assert command record: %v", err)
	}
}

func assertTaskRecord(ctx context.Context, t *testing.T, store agent.Store, want agent.StoreTaskRecord) {
	t.Helper()
	err := store.View(ctx, func(view agent.StoreView) error {
		got, err := view.LoadTask(want.Task.Ref)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("task = %#v, want %#v", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("assert task record: %v", err)
	}
}

func mutateBytes(value []byte) {
	for index := range value {
		value[index] ^= 0xff
	}
}

func assertOversizedRead(t *testing.T, label string, read func() error) {
	t.Helper()
	if err := read(); !errors.Is(err, agent.ErrStoreRecordTooLarge) {
		t.Fatalf("%s error = %v, want ErrStoreRecordTooLarge", label, err)
	}
}
