package workerws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
)

var errInjectedAfterCommit = errors.New("injected response loss after atomic commit")

type failAfterCommitAuthority struct {
	*delegation.Store
	fail atomic.Bool
}

func (authority *failAfterCommitAuthority) ExecuteWorkerCommand(
	ctx context.Context,
	receipt delegation.WorkerCommandReceipt,
	apply delegation.WorkerCommandApply,
) (delegation.WorkerCommandReceipt, bool, error) {
	stored, replay, err := authority.Store.ExecuteWorkerCommand(ctx, receipt, apply)
	if err == nil && !replay && authority.fail.CompareAndSwap(true, false) {
		return delegation.WorkerCommandReceipt{}, false, errInjectedAfterCommit
	}
	return stored, replay, err
}

func TestAtomicWorkerCommandsReplayOriginalResultAfterCommitResponseLossAndRestart(t *testing.T) {
	setups := map[string]func(*testing.T, *delegation.Store) workerproto.Command{
		"accept": func(t *testing.T, store *delegation.Store) workerproto.Command {
			created := createDelegationTask(t, store, "atomic-accept")
			return workerproto.Command{EventID: "event-accept", Kind: workerproto.CommandAccept,
				TaskID: created.Task.ID, ExpectedRevision: created.Task.Revision}
		},
		"reject": func(t *testing.T, store *delegation.Store) workerproto.Command {
			created := createDelegationTask(t, store, "atomic-reject")
			return workerproto.Command{EventID: "event-reject", Kind: workerproto.CommandReject,
				TaskID: created.Task.ID, ExpectedRevision: created.Task.Revision}
		},
		"deliver": func(t *testing.T, store *delegation.Store) workerproto.Command {
			task := createWorkingTask(t, store, "atomic-deliver")
			return workerproto.Command{EventID: "event-deliver", Kind: workerproto.CommandDeliver,
				TaskID: task.ID, ExpectedRevision: task.Revision, Delivery: &workerproto.Delivery{
					ArtifactID: "artifact-atomic-deliver", ArtifactMediaType: delegation.GitPatchMediaType,
					ArtifactData: []byte("patch"), ArtifactMetadata: []byte(`{"turn":1}`),
				}}
		},
		"exec": func(t *testing.T, store *delegation.Store) workerproto.Command {
			task := createWorkingTask(t, store, "atomic-exec")
			return workerproto.Command{EventID: "event-exec", Kind: workerproto.CommandExec,
				TaskID: task.ID, ExpectedRevision: task.Revision, Exec: &workerproto.ExecRequest{
					RequestID: "request-atomic-exec", Command: "pwd", Reason: "inspect caller workspace",
				}}
		},
		"complete": func(t *testing.T, store *delegation.Store) workerproto.Command {
			task := createWorkingTask(t, store, "atomic-complete")
			return workerproto.Command{EventID: "event-complete", Kind: workerproto.CommandComplete,
				TaskID: task.ID, ExpectedRevision: task.Revision}
		},
		"fail": func(t *testing.T, store *delegation.Store) workerproto.Command {
			task := createWorkingTask(t, store, "atomic-fail")
			return workerproto.Command{EventID: "event-fail", Kind: workerproto.CommandFail,
				TaskID: task.ID, ExpectedRevision: task.Revision}
		},
		"confirm_rewind": func(t *testing.T, store *delegation.Store) workerproto.Command {
			task := createRewindPendingTask(t, store, "atomic-confirm-rewind")
			return workerproto.Command{EventID: "event-confirm-rewind", Kind: workerproto.CommandConfirmRewind,
				TaskID: task.ID, ExpectedRevision: task.Revision}
		},
		"reject_rewind": func(t *testing.T, store *delegation.Store) workerproto.Command {
			task := createRewindPendingTask(t, store, "atomic-reject-rewind")
			return workerproto.Command{EventID: "event-reject-rewind", Kind: workerproto.CommandRejectRewind,
				TaskID: task.ID, ExpectedRevision: task.Revision}
		},
	}

	for name, setup := range setups {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "authority.db")
			store, err := delegation.OpenSQLite(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			command := setup(t, store)
			before, err := store.GetTask(ctx, command.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			fault := &failAfterCommitAuthority{Store: store}
			fault.fail.Store(true)
			server, err := New(Config{RemoteExec: true}, testAuthenticator{}, fault)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := server.executeCommand(ctx, "worker-1", command); !errors.Is(err, errInjectedAfterCommit) {
				t.Fatalf("first command error = %v", err)
			}
			after, err := store.GetTask(ctx, command.TaskID)
			if err != nil || after.Revision != before.Revision+1 {
				t.Fatalf("effect after lost response = %+v, %v; before = %+v", after, err, before)
			}
			digest := workerCommandDigest(t, command)
			receipt, err := store.LookupWorkerCommandReceipt(ctx, command.EventID, "worker-1", digest)
			if err != nil {
				t.Fatalf("atomic receipt = %v", err)
			}
			var original workerproto.CommandResult
			if err := json.Unmarshal(receipt.Result, &original); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			restarted, err := delegation.OpenSQLite(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			restartedServer, err := New(Config{RemoteExec: true}, testAuthenticator{}, restarted)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := restartedServer.executeCommand(ctx, "worker-1", command)
			if err != nil || !replayed.Replay {
				t.Fatalf("restart replay = %+v, %v", replayed, err)
			}
			replayed.Replay = false
			if !reflect.DeepEqual(replayed, original) {
				t.Fatalf("replay differs from committed result\nreplay:   %#v\noriginal: %#v", replayed, original)
			}
			finalTask, err := restarted.GetTask(ctx, command.TaskID)
			if err != nil || finalTask.Revision != after.Revision {
				t.Fatalf("replay advanced effect = %+v, %v; committed = %+v", finalTask, err, after)
			}
		})
	}
}

func TestRejectedDeliverRollsBackPartialTurnAndPersistsStableErrorReceipt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "authority.db")
	store, err := delegation.OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	owner := createWorkingTask(t, store, "artifact-owner")
	if _, err := store.DeliverTurn(ctx, delegation.DeliverTurnInput{
		CommandInput: delegation.CommandInput{TaskID: owner.ID, ExpectedRevision: owner.Revision},
		ArtifactID:   "shared-artifact", ArtifactMediaType: delegation.GitPatchMediaType,
		ArtifactData: []byte("owner patch"),
	}); err != nil {
		t.Fatal(err)
	}
	target := createWorkingTask(t, store, "artifact-target")
	command := workerproto.Command{
		EventID: "event-rejected-delivery", Kind: workerproto.CommandDeliver,
		TaskID: target.ID, ExpectedRevision: target.Revision,
		Delivery: &workerproto.Delivery{
			ArtifactID: "shared-artifact", ArtifactMediaType: delegation.GitPatchMediaType,
			ArtifactData: []byte("different patch"),
		},
	}
	server, err := New(Config{}, testAuthenticator{}, store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := server.executeCommand(ctx, "worker-1", command)
	if err != nil || result.Error == nil {
		t.Fatalf("rejected delivery = %+v, %v", result, err)
	}
	if result.Error.Code != "already_exists" {
		t.Fatalf("rejected delivery code = %q, want already_exists", result.Error.Code)
	}
	after, err := store.GetTask(ctx, target.ID)
	if err != nil || after.Revision != target.Revision || after.State != delegation.StateWorking || after.LatestTurn != 0 {
		t.Fatalf("partial delivery changed task = %+v, %v; before = %+v", after, err, target)
	}
	if _, err := store.GetArtifact(ctx, target.ID, 1); !errors.Is(err, delegation.ErrNotFound) {
		t.Fatalf("partial delivery leaked artifact/turn: %v", err)
	}
	events, err := store.ListEvents(ctx, target.ID, 0)
	if err != nil || len(events) != 2 {
		t.Fatalf("partial delivery leaked event = %+v, %v", events, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := delegation.OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restartedServer, err := New(Config{}, testAuthenticator{}, restarted)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restartedServer.executeCommand(ctx, "worker-1", command)
	if err != nil || !replayed.Replay {
		t.Fatalf("stable rejection restart replay = %+v, %v; first = %+v", replayed, err, result)
	}
	replayed.Replay = false
	if !reflect.DeepEqual(replayed, result) {
		t.Fatalf("rejected result changed across restart\nreplay: %#v\nfirst:  %#v", replayed, result)
	}
	afterRestart, err := restarted.GetTask(ctx, target.ID)
	if err != nil || !reflect.DeepEqual(afterRestart, after) {
		t.Fatalf("rejected replay changed task = %+v, %v; before restart = %+v", afterRestart, err, after)
	}
	restartedEvents, err := restarted.ListEvents(ctx, target.ID, 0)
	if err != nil || !reflect.DeepEqual(restartedEvents, events) {
		t.Fatalf("rejected replay changed events = %+v, %v; before restart = %+v", restartedEvents, err, events)
	}
}

func TestAtomicWorkerCommandConcurrentReplayAndReceiptIdentityConflicts(t *testing.T) {
	ctx := context.Background()
	store, err := delegation.OpenSQLite(ctx, filepath.Join(t.TempDir(), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	created := createDelegationTask(t, store, "concurrent-accept")
	command := workerproto.Command{EventID: "event-concurrent", Kind: workerproto.CommandAccept,
		TaskID: created.Task.ID, ExpectedRevision: created.Task.Revision}
	left, err := New(Config{}, testAuthenticator{}, store)
	if err != nil {
		t.Fatal(err)
	}
	right, err := New(Config{}, testAuthenticator{}, store)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result workerproto.CommandResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for _, server := range []*Server{left, right} {
		go func(server *Server) {
			<-start
			result, err := server.executeCommand(ctx, "worker-1", command)
			outcomes <- outcome{result: result, err: err}
		}(server)
	}
	close(start)
	first, second := <-outcomes, <-outcomes
	if first.err != nil || second.err != nil || first.result.Replay == second.result.Replay {
		t.Fatalf("concurrent results = %+v / %+v", first, second)
	}
	task, err := store.GetTask(ctx, command.TaskID)
	if err != nil || task.Revision != created.Task.Revision+1 || task.State != delegation.StateWorking {
		t.Fatalf("concurrent command effect = %+v, %v", task, err)
	}

	otherWorker, err := left.executeCommand(ctx, "worker-2", command)
	if err != nil || otherWorker.Error == nil || otherWorker.Error.Code != "idempotency_conflict" {
		t.Fatalf("cross-worker receipt reuse = %+v, %v", otherWorker, err)
	}
	other := createDelegationTask(t, store, "other-task")
	changed := command
	changed.TaskID = other.Task.ID
	changed.ExpectedRevision = other.Task.Revision
	otherTask, err := left.executeCommand(ctx, "worker-1", changed)
	if err != nil || otherTask.Error == nil || otherTask.Error.Code != "idempotency_conflict" {
		t.Fatalf("cross-task receipt reuse = %+v, %v", otherTask, err)
	}
}

func createDelegationTask(t *testing.T, store *delegation.Store, id string) delegation.TransitionResult {
	t.Helper()
	created, err := store.CreateTask(context.Background(), delegation.CreateTaskInput{ID: id, CallerID: "caller-1"})
	if err != nil {
		t.Fatal(err)
	}
	return created
}

func createWorkingTask(t *testing.T, store *delegation.Store, id string) delegation.Task {
	t.Helper()
	created := createDelegationTask(t, store, id)
	accepted, err := store.AcceptTask(context.Background(), delegation.AcceptTaskInput{
		CommandInput: delegation.CommandInput{TaskID: id, ExpectedRevision: created.Task.Revision},
		WorkerID:     "worker-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return accepted.Task
}

func createRewindPendingTask(t *testing.T, store *delegation.Store, id string) delegation.Task {
	t.Helper()
	working := createWorkingTask(t, store, id)
	delivered, err := store.DeliverTurn(context.Background(), delegation.DeliverTurnInput{
		CommandInput: delegation.CommandInput{TaskID: id, ExpectedRevision: working.Revision},
		ArtifactID:   id + "-artifact", ArtifactMediaType: delegation.GitPatchMediaType,
		ArtifactData: []byte("patch"), ArtifactMetadata: []byte(`{"turn":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	rewind, err := store.RequestRewind(context.Background(), delegation.RequestRewindInput{
		CommandInput: delegation.CommandInput{TaskID: id, ExpectedRevision: delivered.Task.Revision},
		TargetTurn:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rewind.Task
}

func workerCommandDigest(t *testing.T, command workerproto.Command) string {
	t.Helper()
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}
