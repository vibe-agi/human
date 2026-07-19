package agent

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func openSQLiteStoreBridge(t *testing.T) Store {
	t.Helper()
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	})
	if _, err := database.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	if _, err := database.Exec(agentSchema); err != nil {
		t.Fatalf("initialize schema: %v", err)
	}
	return newSQLiteStore(database)
}

func sqliteStoreSeedRecords(t *testing.T) (
	StoreTaskRecord,
	StoreMessageRecord,
	StoreEventRecord,
	StoreCommandRecord,
) {
	t.Helper()
	now := time.Date(2026, 7, 19, 1, 2, 3, 4, time.UTC)
	ref := TaskRef{
		Workspace: WorkspaceRef{Authority: "authority-a", ID: "workspace-a"},
		ID:        "task-a",
	}
	parts := []Part{{MediaType: "text/plain", Data: []byte("hello")}}
	encoded, err := json.Marshal(parts)
	if err != nil {
		t.Fatalf("encode message parts: %v", err)
	}
	digest, err := contentDigest(parts)
	if err != nil {
		t.Fatalf("digest message parts: %v", err)
	}
	task := StoreTaskRecord{Task: Task{
		Ref: ref, Context: ContextRef{Authority: "authority-a", ID: "context-a"},
		State: TaskSubmitted, Revision: 1, MessageCount: 1, EventCount: 1,
		CreatedAt: now, UpdatedAt: now,
	}}
	message := StoreMessageRecord{
		ID: "message-a", Task: ref, Sequence: 1, Author: AuthorCaller,
		EncodedParts: encoded, PartsDigest: StoreDigest(digest), CreatedAt: now,
	}
	event := StoreEventRecord{Event: Event{
		Task: ref, Sequence: 1, Type: EventTaskSubmitted,
		State: TaskSubmitted, Revision: 1, Message: message.ID, OccurredAt: now,
	}}
	result := []byte(`{"task":"task-a"}`)
	command := StoreCommandRecord{
		Authority: "authority-a", ID: "command-a", Kind: "create_task",
		IntentDigest: "intent-a", ResultKind: "task", Result: result,
		ResultDigest: StoreDigest(byteDigest(result)), CreatedAt: now,
	}
	return task, message, event, command
}

func TestSQLiteStoreUpdateRollbackAndCallbackLifetime(t *testing.T) {
	store := openSQLiteStoreBridge(t)
	task, message, event, command := sqliteStoreSeedRecords(t)
	abort := errors.New("inject rollback")
	callbackCalls := 0
	var escaped StoreTx
	err := store.Update(context.Background(), func(tx StoreTx) error {
		callbackCalls++
		escaped = tx
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertMessage(message); err != nil {
			return err
		}
		if err := tx.InsertEvent(event); err != nil {
			return err
		}
		if err := tx.InsertCommand(command); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) || callbackCalls != 1 {
		t.Fatalf("Update error/calls = %v/%d", err, callbackCalls)
	}
	if _, err := escaped.LoadTask(task.Task.Ref); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("escaped transaction error = %v", err)
	}
	if err := store.View(context.Background(), func(view StoreView) error {
		_, err := view.LoadTask(task.Task.Ref)
		if !errors.Is(err, ErrStoreRecordNotFound) {
			t.Fatalf("rolled-back Task error = %v", err)
		}
		_, err = view.LookupCommand(
			StoreCommandKey{Authority: command.Authority, ID: command.ID},
			StoreReadLimit{MaxBytes: 1024},
		)
		if !errors.Is(err, ErrStoreRecordNotFound) {
			t.Fatalf("rolled-back command error = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect rollback: %v", err)
	}
}

func TestSQLiteStoreCallbackPanicReleasesTransaction(t *testing.T) {
	store := openSQLiteStoreBridge(t)
	panicValue := errors.New("callback panic")
	func() {
		defer func() {
			if recovered := recover(); recovered != panicValue {
				t.Fatalf("recovered panic = %#v, want %#v", recovered, panicValue)
			}
		}()
		_ = store.Update(context.Background(), func(StoreTx) error {
			panic(panicValue)
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.View(ctx, func(StoreView) error { return nil }); err != nil {
		t.Fatalf("View after panicking callback: %v", err)
	}
}

func TestSQLiteStoreCommitSnapshotAndByteOwnership(t *testing.T) {
	store := openSQLiteStoreBridge(t)
	task, message, event, command := sqliteStoreSeedRecords(t)
	wantParts := bytes.Clone(message.EncodedParts)
	wantResult := bytes.Clone(command.Result)
	if err := store.Update(context.Background(), func(tx StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertMessage(message); err != nil {
			return err
		}
		if err := tx.InsertEvent(event); err != nil {
			return err
		}
		return tx.InsertCommand(command)
	}); err != nil {
		t.Fatalf("commit seed records: %v", err)
	}
	message.EncodedParts[0] ^= 0xff
	command.Result[0] ^= 0xff

	var escaped StoreView
	if err := store.View(context.Background(), func(view StoreView) error {
		escaped = view
		loadedTask, err := view.LoadTask(task.Task.Ref)
		if err != nil || loadedTask.Task != task.Task {
			t.Fatalf("loaded Task = %#v, %v", loadedTask, err)
		}
		resolved, err := view.ResolveTask(task.Task.Ref.Workspace.Authority, task.Task.Ref.ID)
		if err != nil || resolved != task.Task.Ref {
			t.Fatalf("resolved Task = %#v, %v", resolved, err)
		}
		loadedMessage, err := view.LoadMessage(
			StoreMessageKey{Authority: message.Task.Workspace.Authority, ID: message.ID},
			StoreReadLimit{MaxBytes: 1024},
		)
		if err != nil || !bytes.Equal(loadedMessage.EncodedParts, wantParts) {
			t.Fatalf("loaded message = %q, %v", loadedMessage.EncodedParts, err)
		}
		loadedCommand, err := view.LookupCommand(
			StoreCommandKey{Authority: command.Authority, ID: command.ID},
			StoreReadLimit{MaxBytes: 1024},
		)
		if err != nil || !bytes.Equal(loadedCommand.Result, wantResult) {
			t.Fatalf("loaded command = %q, %v", loadedCommand.Result, err)
		}
		loadedMessage.EncodedParts[0] ^= 0xff
		loadedCommand.Result[0] ^= 0xff
		contextRecords, err := view.ScanContextTasks(StoreTaskContextScan{
			Context: task.Task.Context, Limit: 2,
		})
		if err != nil || len(contextRecords) != 1 || contextRecords[0].Task.Ref != task.Task.Ref {
			t.Fatalf("context scan = %#v, %v", contextRecords, err)
		}
		authorityRecords, err := view.ScanAuthorityTasks(StoreTaskAuthorityScan{
			Authority: task.Task.Ref.Workspace.Authority, Limit: 2,
		})
		if err != nil || authorityRecords.TotalSize != 1 || len(authorityRecords.Records) != 1 {
			t.Fatalf("authority scan = %#v, %v", authorityRecords, err)
		}
		messages, err := view.ScanMessages(StoreMessageScan{
			Task: task.Task.Ref, Limit: 2, ReadLimit: StoreReadLimit{MaxBytes: 1024},
		})
		if err != nil || len(messages) != 1 || !bytes.Equal(messages[0].EncodedParts, wantParts) {
			t.Fatalf("message scan = %#v, %v", messages, err)
		}
		events, err := view.ScanEvents(StoreEventScan{Task: task.Task.Ref, Limit: 2})
		if err != nil || len(events) != 1 || events[0].Event != event.Event {
			t.Fatalf("event scan = %#v, %v", events, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("read committed snapshot: %v", err)
	}
	if _, err := escaped.LoadTask(task.Task.Ref); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("escaped View error = %v", err)
	}
	if err := store.View(context.Background(), func(view StoreView) error {
		if _, err := view.LookupCommand(
			StoreCommandKey{Authority: command.Authority, ID: command.ID},
			StoreReadLimit{},
		); !errors.Is(err, ErrStoreRecordTooLarge) {
			t.Fatalf("zero-budget command error = %v", err)
		}
		loaded, err := view.LookupCommand(
			StoreCommandKey{Authority: command.Authority, ID: command.ID},
			StoreReadLimit{MaxBytes: 1024},
		)
		if err != nil || !bytes.Equal(loaded.Result, wantResult) {
			t.Fatalf("second command read = %q, %v", loaded.Result, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify returned-byte isolation: %v", err)
	}
}

func TestSQLiteStoreLeaseHistoryAndTaskCAS(t *testing.T) {
	store := openSQLiteStoreBridge(t)
	task, message, event, command := sqliteStoreSeedRecords(t)
	if err := store.Update(context.Background(), func(tx StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertMessage(message); err != nil {
			return err
		}
		if err := tx.InsertEvent(event); err != nil {
			return err
		}
		return tx.InsertCommand(command)
	}); err != nil {
		t.Fatalf("seed Task: %v", err)
	}
	grant := StoreLeaseGrantRecord{
		Grant:     LeaseGrant{Task: task.Task.Ref, Worker: "worker-a", Fence: 1},
		GrantedAt: task.Task.CreatedAt.Add(time.Second),
	}
	if err := store.Update(context.Background(), func(tx StoreTx) error {
		if err := tx.InsertLeaseGrant(grant); err != nil {
			return err
		}
		next := cloneStoreTaskRecord(task)
		next.Lease = StoreLeaseState{Owner: grant.Grant.Worker, Fence: grant.Grant.Fence}
		unleased := StoreLeaseState{}
		changed, err := tx.CompareAndSwapTask(StoreTaskMutation{
			Ref: task.Task.Ref,
			Condition: StoreTaskCondition{
				ExpectedRevision: task.Task.Revision,
				ExpectedLease:    &unleased,
			},
			Next: next,
		})
		if err != nil {
			return err
		}
		if !changed {
			t.Fatal("lease Task CAS unexpectedly missed")
		}
		changed, err = tx.CompareAndSwapTask(StoreTaskMutation{
			Ref: task.Task.Ref,
			Condition: StoreTaskCondition{
				ExpectedRevision: task.Task.Revision,
				ExpectedLease:    &unleased,
			},
			Next: next,
		})
		if err != nil {
			return err
		}
		if changed {
			t.Fatal("stale lease CAS changed Task")
		}
		return nil
	}); err != nil {
		t.Fatalf("grant lease: %v", err)
	}
	if err := store.View(context.Background(), func(view StoreView) error {
		loaded, err := view.LoadTask(task.Task.Ref)
		if err != nil || loaded.Lease != (StoreLeaseState{Owner: "worker-a", Fence: 1}) {
			t.Fatalf("loaded lease = %#v, %v", loaded.Lease, err)
		}
		exact, err := view.LoadLeaseGrant(task.Task.Ref, 1)
		if err != nil || exact != grant {
			t.Fatalf("exact grant = %#v, %v", exact, err)
		}
		latest, err := view.LoadLatestLeaseGrant(task.Task.Ref)
		if err != nil || latest != grant {
			t.Fatalf("latest grant = %#v, %v", latest, err)
		}
		leases, err := view.ScanLeases(StoreLeaseScan{
			Authority: "authority-a", Worker: "worker-a", Limit: 2,
		})
		if err != nil || len(leases) != 1 || leases[0].Grant != grant.Grant {
			t.Fatalf("lease scan = %#v, %v", leases, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect lease: %v", err)
	}
}
