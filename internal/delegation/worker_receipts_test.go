package delegation

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestWorkerCommandReceiptPersistsAndRejectsReuse(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "delegation.db")
	store, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	receipt := WorkerCommandReceipt{
		EventID: "event-1", WorkerID: "worker-1", TaskID: "task-1",
		CommandDigest: "digest-1",
	}
	result := []byte(`{"event_id":"event-1"}`)
	stored, replay, err := store.ExecuteWorkerCommand(ctx, receipt, func(context.Context, WorkerCommandAuthority) ([]byte, bool, error) {
		return result, true, nil
	})
	if err != nil || replay || string(stored.Result) != string(result) {
		t.Fatal(err)
	}
	stored, replay, err = store.ExecuteWorkerCommand(ctx, receipt, func(context.Context, WorkerCommandAuthority) ([]byte, bool, error) {
		t.Fatal("exact replay re-entered domain callback")
		return nil, false, nil
	})
	if err != nil || !replay || string(stored.Result) != string(result) {
		t.Fatalf("exact duplicate = %#v, replay=%v, error=%v", stored, replay, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, err := store.LookupWorkerCommandReceipt(ctx, "event-1", "worker-1", "digest-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Result) != string(result) || got.TaskID != receipt.TaskID {
		t.Fatalf("receipt = %#v", got)
	}

	for name, identity := range map[string][2]string{
		"worker": {"worker-2", "digest-1"},
		"digest": {"worker-1", "digest-2"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := store.LookupWorkerCommandReceipt(ctx, "event-1", identity[0], identity[1])
			if !errors.Is(err, ErrIdempotencyConflict) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	changed := receipt
	changed.CommandDigest = "digest-2"
	if _, _, err := store.ExecuteWorkerCommand(ctx, changed, func(context.Context, WorkerCommandAuthority) ([]byte, bool, error) {
		return []byte(`{"changed":true}`), true, nil
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed result error = %v", err)
	}
}

func TestWorkerCommandReceiptInsertFailureRollsBackEveryDomainEffect(t *testing.T) {
	type scenario struct {
		taskID string
		apply  WorkerCommandApply
	}
	setups := map[string]func(*testing.T, context.Context, *Store) scenario{
		"accept": func(t *testing.T, ctx context.Context, store *Store) scenario {
			created, err := store.CreateTask(ctx, CreateTaskInput{ID: "receipt-fail-accept", CallerID: "caller"})
			if err != nil {
				t.Fatal(err)
			}
			return scenario{taskID: created.Task.ID, apply: func(ctx context.Context, authority WorkerCommandAuthority) ([]byte, bool, error) {
				_, err := authority.AcceptTask(ctx, AcceptTaskInput{
					CommandInput: command(created.Task, "accept"), WorkerID: "worker",
				})
				return []byte(`{"ok":true}`), true, err
			}}
		},
		"deliver": func(t *testing.T, ctx context.Context, store *Store) scenario {
			task := createAcceptedTask(t, ctx, store, "receipt-fail-deliver")
			return scenario{taskID: task.ID, apply: func(ctx context.Context, authority WorkerCommandAuthority) ([]byte, bool, error) {
				_, err := authority.DeliverTurn(ctx, DeliverTurnInput{
					CommandInput: command(task, "deliver"), ArtifactID: "receipt-fail-artifact",
					ArtifactMediaType: GitPatchMediaType, ArtifactData: []byte("patch"),
				})
				return []byte(`{"ok":true}`), true, err
			}}
		},
		"exec": func(t *testing.T, ctx context.Context, store *Store) scenario {
			task := createAcceptedTask(t, ctx, store, "receipt-fail-exec")
			return scenario{taskID: task.ID, apply: func(ctx context.Context, authority WorkerCommandAuthority) ([]byte, bool, error) {
				_, err := authority.RequestExec(ctx, RequestExecInput{
					CommandInput: command(task, "exec"), WorkerID: "worker",
					RequestID: "receipt-fail-request", Command: "pwd", Reason: "inspect workspace",
				})
				return []byte(`{"ok":true}`), true, err
			}}
		},
		"confirm_rewind": func(t *testing.T, ctx context.Context, store *Store) scenario {
			task := createAcceptedTask(t, ctx, store, "receipt-fail-rewind")
			delivered, err := store.DeliverTurn(ctx, DeliverTurnInput{
				CommandInput: command(task, "deliver"), ArtifactID: "receipt-fail-rewind-artifact",
				ArtifactMediaType: GitPatchMediaType, ArtifactData: []byte("patch"),
			})
			if err != nil {
				t.Fatal(err)
			}
			pending, err := store.RequestRewind(ctx, RequestRewindInput{
				CommandInput: command(delivered.Task, "rewind"), TargetTurn: 0,
			})
			if err != nil {
				t.Fatal(err)
			}
			return scenario{taskID: pending.Task.ID, apply: func(ctx context.Context, authority WorkerCommandAuthority) ([]byte, bool, error) {
				_, err := authority.ConfirmRewind(ctx, command(pending.Task, "confirm"))
				return []byte(`{"ok":true}`), true, err
			}}
		},
	}

	for name, setup := range setups {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
			test := setup(t, ctx, store)
			before, err := store.LoadSnapshot(ctx, test.taskID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(ctx, `
				CREATE TRIGGER fail_worker_receipt
				BEFORE INSERT ON delegation_worker_command_receipts
				BEGIN
				  SELECT RAISE(ABORT, 'injected worker receipt failure');
				END`); err != nil {
				t.Fatal(err)
			}
			receipt := WorkerCommandReceipt{
				EventID: "event-" + name, WorkerID: "worker", TaskID: test.taskID,
				CommandDigest: "digest-" + name,
			}
			if _, _, err := store.ExecuteWorkerCommand(ctx, receipt, test.apply); err == nil {
				t.Fatal("receipt insertion failure unexpectedly succeeded")
			}
			after, err := store.LoadSnapshot(ctx, test.taskID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("domain effect survived receipt failure\nbefore: %#v\nafter:  %#v", before, after)
			}
			if _, err := store.LookupWorkerCommandReceipt(
				ctx, receipt.EventID, receipt.WorkerID, receipt.CommandDigest,
			); !errors.Is(err, ErrNotFound) {
				t.Fatalf("receipt survived failed transaction: %v", err)
			}
		})
	}
}

func TestWorkerCommandInfrastructureFailureRollsBackEffectAndReceipt(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	created, err := store.CreateTask(ctx, CreateTaskInput{ID: "infra-fail-accept", CallerID: "caller"})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadSnapshot(ctx, created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	receipt := WorkerCommandReceipt{
		EventID: "event-infra-fail", WorkerID: "worker", TaskID: created.Task.ID,
		CommandDigest: "digest-infra-fail",
	}
	injected := errors.New("injected infrastructure failure")
	_, _, err = store.ExecuteWorkerCommand(ctx, receipt, func(ctx context.Context, authority WorkerCommandAuthority) ([]byte, bool, error) {
		if _, err := authority.AcceptTask(ctx, AcceptTaskInput{
			CommandInput: command(created.Task, "accept"), WorkerID: "worker",
		}); err != nil {
			return nil, false, err
		}
		return nil, false, injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("infrastructure failure = %v", err)
	}
	after, err := store.LoadSnapshot(ctx, created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("domain effect survived infrastructure failure\nbefore: %#v\nafter:  %#v", before, after)
	}
	if _, err := store.LookupWorkerCommandReceipt(
		ctx, receipt.EventID, receipt.WorkerID, receipt.CommandDigest,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("receipt survived infrastructure failure: %v", err)
	}
}
