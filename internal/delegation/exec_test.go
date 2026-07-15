package delegation

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestExecRequestAndResolutionAreDurableIdempotentAndStateNeutral(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	task := createAcceptedTask(t, ctx, store, "task-exec")
	input := RequestExecInput{
		CommandInput: CommandInput{TaskID: task.ID, ExpectedRevision: task.Revision},
		WorkerID:     "worker", RequestID: "exec-1", Command: "adb devices",
		CWD: "/workspace", TimeoutMS: 15_000, Reason: "inspect attached device",
	}
	requested, err := store.RequestExec(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if requested.Task.State != StateWorking || requested.Task.Revision != task.Revision+1 ||
		requested.Request.Status != ExecPending || requested.Event.Kind != EventExecRequested {
		t.Fatalf("requested exec = %+v", requested)
	}
	replay, err := store.RequestExec(ctx, input)
	if err != nil || !replay.Replay || replay.Request.RequestSequence != requested.Request.RequestSequence {
		t.Fatalf("request replay = %+v, %v", replay, err)
	}
	conflict := input
	conflict.Command = "uname -a"
	if _, err := store.RequestExec(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("request idempotency conflict = %v", err)
	}

	resolution := ResolveExecInput{
		CommandInput: CommandInput{TaskID: task.ID, ExpectedRevision: requested.Task.Revision},
		RequestID:    "exec-1", ResolutionID: "resolve-1", Approved: true,
		ExitCode: 0, Stdout: []byte("device-1\tdevice\n"), Stderr: []byte{},
	}
	resolved, err := store.ResolveExec(ctx, resolution)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Task.State != StateWorking || resolved.Request.Status != ExecCompleted ||
		resolved.Request.ExitCode == nil || *resolved.Request.ExitCode != 0 ||
		resolved.Event.Kind != EventExecCompleted {
		t.Fatalf("resolved exec = %+v", resolved)
	}
	replayedResolution, err := store.ResolveExec(ctx, resolution)
	if err != nil || !replayedResolution.Replay {
		t.Fatalf("resolution replay = %+v, %v", replayedResolution, err)
	}
	changedResolution := resolution
	changedResolution.Stdout = []byte("different")
	if _, err := store.ResolveExec(ctx, changedResolution); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("resolution idempotency conflict = %v", err)
	}

	snapshot, err := store.LoadSnapshot(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Exec) != 1 || snapshot.Exec[0].Status != ExecCompleted ||
		len(snapshot.Events) != int(snapshot.Task.Revision) {
		t.Fatalf("exec snapshot = %+v", snapshot)
	}
}

func TestExecDenialAndWorkerOwnershipFailClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	task := createAcceptedTask(t, ctx, store, "task-exec-deny")
	wrongWorker := RequestExecInput{
		CommandInput: CommandInput{TaskID: task.ID, ExpectedRevision: task.Revision},
		WorkerID:     "other", RequestID: "exec-wrong", Command: "pwd", Reason: "inspect cwd",
	}
	if _, err := store.RequestExec(ctx, wrongWorker); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("wrong worker request error = %v", err)
	}
	requested, err := store.RequestExec(ctx, RequestExecInput{
		CommandInput: CommandInput{TaskID: task.ID, ExpectedRevision: task.Revision},
		WorkerID:     "worker", RequestID: "exec-deny", Command: "sudo reboot", Reason: "restart device",
	})
	if err != nil {
		t.Fatal(err)
	}
	denied, err := store.ResolveExec(ctx, ResolveExecInput{
		CommandInput: CommandInput{TaskID: task.ID, ExpectedRevision: requested.Task.Revision},
		RequestID:    "exec-deny", ResolutionID: "deny-1", Error: "not authorized",
	})
	if err != nil {
		t.Fatal(err)
	}
	if denied.Request.Status != ExecDenied || denied.Request.Error != "not authorized" ||
		denied.Request.ExitCode != nil || denied.Event.Kind != EventExecDenied {
		t.Fatalf("denied exec = %+v", denied)
	}
}

func TestPendingExecMustBeResolvedBeforeTerminalTransition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	task := createAcceptedTask(t, ctx, store, "task-exec-terminal")
	requested, err := store.RequestExec(ctx, RequestExecInput{
		CommandInput: CommandInput{TaskID: task.ID, ExpectedRevision: task.Revision},
		WorkerID:     "worker", RequestID: "exec-pending", Command: "pwd", Reason: "inspect cwd",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteTask(ctx, CommandInput{
		TaskID: task.ID, ExpectedRevision: requested.Task.Revision,
	}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("complete with pending exec error = %v", err)
	}
	unchanged, err := store.GetTask(ctx, task.ID)
	if err != nil || unchanged.State != StateWorking || unchanged.Revision != requested.Task.Revision {
		t.Fatalf("terminal rejection changed task = %+v, %v", unchanged, err)
	}
	denied, err := store.ResolveExec(ctx, ResolveExecInput{
		CommandInput: CommandInput{TaskID: task.ID, ExpectedRevision: unchanged.Revision},
		RequestID:    "exec-pending", ResolutionID: "deny-before-complete", Error: "not needed",
	})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := store.CompleteTask(ctx, CommandInput{
		TaskID: task.ID, ExpectedRevision: denied.Task.Revision,
	})
	if err != nil || completed.Task.State != StateCompleted {
		t.Fatalf("complete after denial = %+v, %v", completed, err)
	}
}
