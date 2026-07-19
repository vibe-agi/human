package agent_test

import (
	"context"
	"errors"
	. "github.com/vibe-agi/human/agent"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/workspace"
)

func TestLeaseFenceRecoveryAndSameWorkerGeneration(t *testing.T) {
	service, path := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "lease-context", "lease-workspace", "lease-task")
	created, err := service.CreateTask(ctx, createCommand(
		"create-lease", contextRef, taskRef, "initial-lease", "start",
	))
	if err != nil {
		t.Fatal(err)
	}
	acquire := AcquireLeaseCommand{ID: "acquire-1", Task: taskRef, Worker: "worker-a"}
	first, err := service.AcquireLease(ctx, acquire)
	if err != nil {
		t.Fatal(err)
	}
	if first.Grant.Fence != 1 || first.Task != created || first.GrantedAt.IsZero() {
		t.Fatalf("first lease = %#v", first)
	}
	replay, err := service.AcquireLease(ctx, acquire)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replay, first) {
		t.Fatalf("lease replay differs:\n got %#v\nwant %#v", replay, first)
	}
	conflict := acquire
	conflict.Worker = "worker-b"
	if _, err := service.AcquireLease(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting acquire replay error = %v", err)
	}
	if _, err := service.AcquireLease(ctx, AcquireLeaseCommand{
		ID: "acquire-busy", Task: taskRef, Worker: "worker-b",
	}); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("second active lease error = %v", err)
	}

	accepted, err := service.AcceptTask(ctx, WorkerTaskCommand{
		Meta: WorkerCommandMeta{ID: "accept-lease", ExpectedRevision: created.Revision, Grant: first.Grant},
		Task: taskRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestAgentWithConfig(t, path, DefaultConfig())
	recovered, err := reopened.GetLease(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Grant != first.Grant || recovered.Task != accepted || !recovered.GrantedAt.Equal(first.GrantedAt) {
		t.Fatalf("recovered lease = %#v, want %#v", recovered, first)
	}
	page, err := reopened.ListLeases(ctx, taskRef.Workspace.Authority, "worker-a", LeasePageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.HasMore || page.Items[0].Grant != first.Grant {
		t.Fatalf("recovered lease page = %#v", page)
	}

	if err := reopened.FenceLease(ctx, FenceLeaseCommand{ID: "fence-1", Grant: first.Grant}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.FenceLease(ctx, FenceLeaseCommand{ID: "fence-1", Grant: first.Grant}); err != nil {
		t.Fatalf("fence replay: %v", err)
	}
	if _, err := reopened.GetLease(ctx, taskRef); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("fenced current lease error = %v", err)
	}
	second, err := reopened.AcquireLease(ctx, AcquireLeaseCommand{
		ID: "acquire-2", Task: taskRef, Worker: "worker-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Grant.Fence != first.Grant.Fence+1 || second.Grant.Worker != first.Grant.Worker {
		t.Fatalf("same-worker replacement lease = %#v", second)
	}

	before, err := reopened.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.RequestInput(ctx, WorkerMessageCommand{
		Meta: WorkerCommandMeta{ID: "stale-question", ExpectedRevision: before.Revision, Grant: first.Grant},
		Task: taskRef, Message: textMessage("stale-question-message", "old generation"),
	}); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("old same-worker generation error = %v", err)
	}
	after, err := reopened.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("stale generation mutated Task:\n got %#v\nwant %#v", after, before)
	}

	// A committed command is an observation, not a new effect. It remains an
	// exact ACK after its grant has been fenced and replaced.
	acceptedReplay, err := reopened.AcceptTask(ctx, WorkerTaskCommand{
		Meta: WorkerCommandMeta{ID: "accept-lease", ExpectedRevision: created.Revision, Grant: first.Grant},
		Task: taskRef,
	})
	if err != nil {
		t.Fatalf("committed worker replay after fence: %v", err)
	}
	if !reflect.DeepEqual(acceptedReplay, accepted) {
		t.Fatalf("accepted replay differs:\n got %#v\nwant %#v", acceptedReplay, accepted)
	}
}

func TestTerminalWorkerCommandReplayAfterLeaseRetired(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "terminal-context", "terminal-workspace", "terminal-task")
	working := createWorkingTask(t, service, contextRef, taskRef, "terminal-lease")
	lease, err := service.GetLease(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	command := CompleteTaskCommand{
		Meta: WorkerCommandMeta{
			ID: "complete-terminal", ExpectedRevision: working.Revision, Grant: lease.Grant,
		},
		Task: taskRef, Submission: "terminal-submission",
		Message: textMessage("terminal-final", "done"),
	}
	completed, err := service.CompleteTask(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetLease(ctx, taskRef); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("terminal Task retained lease: %v", err)
	}
	replayed, err := service.CompleteTask(ctx, command)
	if err != nil {
		t.Fatalf("terminal exact replay: %v", err)
	}
	if !reflect.DeepEqual(replayed, completed) {
		t.Fatalf("terminal replay differs:\n got %#v\nwant %#v", replayed, completed)
	}
	if _, err := service.FailTask(ctx, WorkerTaskCommand{
		Meta: WorkerCommandMeta{
			ID: "late-fail", ExpectedRevision: completed.Revision, Grant: lease.Grant,
		},
		Task: taskRef,
	}); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("late uncommitted worker command error = %v", err)
	}
}

func TestStaleLeaseCannotFreezeArtifactOrInitializeWorkspaceHead(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "stale-context", "stale-workspace", "stale-task")
	working := createWorkingTask(t, service, contextRef, taskRef, "stale-freeze")
	first, err := service.GetLease(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.FenceLease(ctx, FenceLeaseCommand{ID: "stale-fence", Grant: first.Grant}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.AcquireLease(ctx, AcquireLeaseCommand{
		ID: "stale-reassign", Task: taskRef, Worker: "worker-test",
	}); err != nil {
		t.Fatal(err)
	}
	command := FreezeArtifactCommand{
		Meta: WorkerCommandMeta{
			ID: "stale-freeze", ExpectedRevision: working.Revision, Grant: first.Grant,
		},
		Task: taskRef, Artifact: "stale-artifact", ExpectedBaseRevision: "stale-base",
		Payload: workspacePayload(`{"changes":[{"path":"a","content":"b"}]}`),
	}
	if _, err := service.FreezeArtifact(ctx, command); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale freeze error = %v", err)
	}
	if _, err := service.GetArtifact(ctx, ArtifactRef{Workspace: taskRef.Workspace, ID: command.Artifact}); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("stale freeze left Artifact: %v", err)
	}
	if _, err := service.GetWorkspaceHead(ctx, taskRef.Workspace); !errors.Is(err, ErrWorkspaceNotFound) {
		t.Fatalf("stale freeze initialized Workspace head: %v", err)
	}
	after, err := service.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, working) {
		t.Fatalf("stale freeze mutated Task:\n got %#v\nwant %#v", after, working)
	}
}

func TestCallerCancelAndWorkerCompleteCommitAtMostOneTerminal(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "race-context", "race-workspace", "race-task")
	working := createWorkingTask(t, service, contextRef, taskRef, "cancel-complete-race")
	lease, err := service.GetLease(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsSeen := make(chan error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		_, err := service.CancelTask(ctx, TaskCommand{
			Meta: CommandMeta{ID: "race-cancel", ExpectedRevision: working.Revision}, Task: taskRef,
		})
		errorsSeen <- err
	}()
	go func() {
		defer wait.Done()
		<-start
		_, err := service.CompleteTask(ctx, CompleteTaskCommand{
			Meta: WorkerCommandMeta{
				ID: "race-complete", ExpectedRevision: working.Revision, Grant: lease.Grant,
			},
			Task: taskRef, Submission: "race-submission",
			Message: textMessage("race-final", "done"),
		})
		errorsSeen <- err
	}()
	close(start)
	wait.Wait()
	close(errorsSeen)
	successes := 0
	for err := range errorsSeen {
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, ErrStaleLease) && !errors.Is(err, ErrRevisionConflict) &&
			!errors.Is(err, ErrTerminalTask) {
			t.Fatalf("terminal race error = %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("terminal race successes = %d, want 1", successes)
	}
	final, err := service.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if !final.State.Terminal() || final.Revision != working.Revision+1 {
		t.Fatalf("terminal race result = %#v", final)
	}
	if _, err := service.GetLease(ctx, taskRef); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("terminal race retained lease: %v", err)
	}
}

func TestLeaseHistoryCorruptionFailsClosed(t *testing.T) {
	t.Run("active owner mismatch", func(t *testing.T) {
		service, _ := openTestAgent(t)
		ctx := context.Background()
		contextRef, taskRef := refs("tenant-a", "corrupt-context", "corrupt-workspace", "corrupt-task")
		created, err := service.CreateTask(ctx, createCommand(
			"corrupt-create", contextRef, taskRef, "corrupt-message", "start",
		))
		if err != nil {
			t.Fatal(err)
		}
		assignment, err := service.AcquireLease(ctx, AcquireLeaseCommand{
			ID: "corrupt-acquire", Task: taskRef, Worker: "worker-a",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.database.ExecContext(ctx, `
			UPDATE agent_lease_grants
			SET worker_id = 'worker-b'
			WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND fence = ?`,
			taskRef.Workspace.Authority, taskRef.Workspace.ID, taskRef.ID, assignment.Grant.Fence,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := service.ListLeases(ctx, taskRef.Workspace.Authority, "worker-a", LeasePageRequest{}); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("list mismatched lease history error = %v", err)
		}
		if _, err := service.GetLease(ctx, taskRef); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("get mismatched lease history error = %v", err)
		}
		if _, err := service.AcceptTask(ctx, WorkerTaskCommand{
			Meta: WorkerCommandMeta{
				ID: "corrupt-accept", ExpectedRevision: created.Revision, Grant: assignment.Grant,
			},
			Task: taskRef,
		}); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("mutate with mismatched lease history error = %v", err)
		}
		if _, err := service.AcquireLease(ctx, AcquireLeaseCommand{
			ID: "corrupt-reacquire", Task: taskRef, Worker: "worker-c",
		}); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("acquire over mismatched lease history error = %v", err)
		}
	})

	t.Run("retired history missing", func(t *testing.T) {
		service, _ := openTestAgent(t)
		ctx := context.Background()
		contextRef, taskRef := refs("tenant-a", "retired-context", "retired-workspace", "retired-task")
		if _, err := service.CreateTask(ctx, createCommand(
			"retired-create", contextRef, taskRef, "retired-message", "start",
		)); err != nil {
			t.Fatal(err)
		}
		assignment, err := service.AcquireLease(ctx, AcquireLeaseCommand{
			ID: "retired-acquire", Task: taskRef, Worker: "worker-a",
		})
		if err != nil {
			t.Fatal(err)
		}
		fence := FenceLeaseCommand{ID: "retired-fence", Grant: assignment.Grant}
		if err := service.FenceLease(ctx, fence); err != nil {
			t.Fatal(err)
		}
		if _, err := service.database.ExecContext(ctx, `
			DELETE FROM agent_lease_grants
			WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND fence = ?`,
			taskRef.Workspace.Authority, taskRef.Workspace.ID, taskRef.ID, assignment.Grant.Fence,
		); err != nil {
			t.Fatal(err)
		}
		if err := service.FenceLease(ctx, fence); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("fence replay without history error = %v", err)
		}
		if _, err := service.AcquireLease(ctx, AcquireLeaseCommand{
			ID: "retired-reacquire", Task: taskRef, Worker: "worker-b",
		}); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("acquire after missing retired history error = %v", err)
		}
		if _, err := service.GetLease(ctx, taskRef); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("get after missing retired history error = %v", err)
		}
	})

	t.Run("grant predates task", func(t *testing.T) {
		service, _ := openTestAgent(t)
		ctx := context.Background()
		contextRef, taskRef := refs("tenant-a", "time-context", "time-workspace", "time-task")
		created, err := service.CreateTask(ctx, createCommand(
			"time-create", contextRef, taskRef, "time-message", "start",
		))
		if err != nil {
			t.Fatal(err)
		}
		assignment, err := service.AcquireLease(ctx, AcquireLeaseCommand{
			ID: "time-acquire", Task: taskRef, Worker: "worker-a",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.database.ExecContext(ctx, `
			UPDATE agent_lease_grants
			SET granted_at = ?
			WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND fence = ?`,
			unixNano(created.CreatedAt.Add(-time.Nanosecond)), taskRef.Workspace.Authority,
			taskRef.Workspace.ID, taskRef.ID, assignment.Grant.Fence,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := service.GetLease(ctx, taskRef); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("lease before Task error = %v", err)
		}
		if _, err := service.ListLeases(ctx, taskRef.Workspace.Authority, "worker-a", LeasePageRequest{}); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("list lease before Task error = %v", err)
		}
	})
}

func TestLeasePaginationTracksReplacementGenerationAtSameTimestamp(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	clock := time.Date(2026, time.July, 19, 12, 0, 0, 123, time.UTC)
	service.now = func() time.Time { return clock }
	contextRef, firstRef := refs("tenant-a", "page-context", "page-workspace", "page-a")
	_, secondRef := refs("tenant-a", "page-context", "page-workspace", "page-b")
	for index, ref := range []TaskRef{firstRef, secondRef} {
		suffix := string(rune('a' + index))
		if _, err := service.CreateTask(ctx, createCommand(
			"page-create-"+suffix, contextRef, ref, "page-message-"+suffix, "start",
		)); err != nil {
			t.Fatal(err)
		}
		if _, err := service.AcquireLease(ctx, AcquireLeaseCommand{
			ID: CommandID("page-acquire-" + suffix), Task: ref, Worker: "worker-a",
		}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := service.ListLeases(ctx, firstRef.Workspace.Authority, "worker-a", LeasePageRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || !page.HasMore || page.Next == nil ||
		page.Items[0].Grant.Task != firstRef || page.Next.Fence != 1 {
		t.Fatalf("first lease page = %#v", page)
	}
	first := page.Items[0]
	if err := service.FenceLease(ctx, FenceLeaseCommand{ID: "page-fence-a", Grant: first.Grant}); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(-time.Hour)
	replacement, err := service.AcquireLease(ctx, AcquireLeaseCommand{
		ID: "page-reacquire-a", Task: firstRef, Worker: "worker-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Grant.Fence != 2 || !replacement.GrantedAt.Equal(first.GrantedAt) {
		t.Fatalf("replacement lease = %#v, first = %#v", replacement, first)
	}
	next, err := service.ListLeases(ctx, firstRef.Workspace.Authority, "worker-a", LeasePageRequest{
		After: page.Next, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Items) != 1 || next.Items[0].Grant != replacement.Grant {
		t.Fatalf("replacement lease page = %#v", next)
	}
}

func TestLeasePageCursorRejectsInvalidTimeAndFence(t *testing.T) {
	service, _ := openTestAgent(t)
	valid := LeasePageCursor{
		GrantedAt: time.Now().UTC(), Workspace: "workspace", Task: "task", Fence: 1,
	}
	invalid := []LeasePageCursor{
		{
			GrantedAt: time.Date(2500, time.January, 1, 0, 0, 0, 0, time.UTC),
			Workspace: "workspace", Task: "task", Fence: 1,
		},
		{GrantedAt: valid.GrantedAt, Workspace: valid.Workspace, Task: valid.Task},
		{
			GrantedAt: valid.GrantedAt, Workspace: valid.Workspace, Task: valid.Task,
			Fence: LeaseFence(^uint64(0)),
		},
	}
	for _, cursor := range invalid {
		if _, err := service.ListLeases(
			context.Background(), "tenant-a", "worker-a", LeasePageRequest{After: &cursor},
		); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("invalid cursor %#v error = %v", cursor, err)
		}
	}
}

func workspacePayload(data string) workspace.Payload {
	return workspace.Payload{
		MediaType: "application/vnd.human.workspace+json",
		Data:      []byte(data),
	}
}
