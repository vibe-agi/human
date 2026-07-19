package agent_test

import (
	"context"
	"errors"
	"fmt"
	. "github.com/vibe-agi/human/agent"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestClaimLeaseSelectsOldestTaskWithStableTies(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	clock := time.Date(2026, time.July, 19, 9, 0, 0, 123, time.UTC)
	service.now = func() time.Time { return clock }
	contextRef, canceledRef := refs("tenant-a", "claim-context", "workspace-0", "task-canceled")
	createdCanceled, err := service.CreateTask(ctx, createCommand(
		"create-canceled", contextRef, canceledRef, "message-canceled", "cancel me",
	))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CancelTask(ctx, TaskCommand{
		Meta: CommandMeta{ID: "cancel-before-claim", ExpectedRevision: createdCanceled.Revision},
		Task: canceledRef,
	}); err != nil {
		t.Fatal(err)
	}

	clock = clock.Add(time.Second)
	_, oldestRef := refs("tenant-a", "claim-context", "workspace-z", "task-oldest")
	if _, err := service.CreateTask(ctx, createCommand(
		"create-oldest", contextRef, oldestRef, "message-oldest", "oldest claimable",
	)); err != nil {
		t.Fatal(err)
	}

	clock = clock.Add(time.Second)
	_, tieBRef := refs("tenant-a", "claim-context", "workspace-b", "task-c")
	_, tieA2Ref := refs("tenant-a", "claim-context", "workspace-a", "task-b")
	_, tieA1Ref := refs("tenant-a", "claim-context", "workspace-a", "task-a")
	for index, ref := range []TaskRef{tieBRef, tieA2Ref, tieA1Ref} {
		suffix := fmt.Sprintf("tie-%d", index)
		if _, err := service.CreateTask(ctx, createCommand(
			"create-"+suffix, contextRef, ref,
			"message-"+suffix, "same timestamp",
		)); err != nil {
			t.Fatal(err)
		}
	}

	want := []TaskRef{oldestRef, tieA1Ref, tieA2Ref, tieBRef}
	for index, wantRef := range want {
		assignment, err := service.ClaimLease(ctx, ClaimLeaseCommand{
			ID: CommandID(fmt.Sprintf("claim-%d", index)), Authority: "tenant-a",
			Worker: WorkerID(fmt.Sprintf("worker-%d", index)),
		})
		if err != nil {
			t.Fatal(err)
		}
		if assignment.Grant.Task != wantRef || assignment.Grant.Fence != 1 ||
			assignment.Task.Ref != wantRef || assignment.Task.State.Terminal() {
			t.Fatalf("claim %d = %#v, want Task %v", index, assignment, wantRef)
		}
	}
	if _, err := service.ClaimLease(ctx, ClaimLeaseCommand{
		ID: "claim-empty", Authority: "tenant-a", Worker: "worker-empty",
	}); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("empty claim error = %v", err)
	}
	if _, err := service.GetLease(ctx, canceledRef); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("terminal Task was claimed: %v", err)
	}
	if _, err := service.ClaimLease(ctx, ClaimLeaseCommand{
		ID: "claim-other-authority", Authority: "tenant-b", Worker: "worker-other",
	}); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("cross-authority empty claim error = %v", err)
	}
}

func TestClaimLeaseExactReplaySurvivesProgressTerminalFenceAndReopen(t *testing.T) {
	service, path := openTestAgent(t)
	ctx := context.Background()
	contextRef, terminalRef := refs("tenant-a", "replay-context", "replay-workspace", "task-terminal")
	created, err := service.CreateTask(ctx, createCommand(
		"replay-create-terminal", contextRef, terminalRef, "replay-message-terminal", "finish",
	))
	if err != nil {
		t.Fatal(err)
	}
	terminalCommand := ClaimLeaseCommand{ID: "claim-terminal", Authority: "tenant-a", Worker: "worker-a"}
	terminalAssignment, err := service.ClaimLease(ctx, terminalCommand)
	if err != nil {
		t.Fatal(err)
	}
	working, err := service.AcceptTask(ctx, WorkerTaskCommand{
		Meta: WorkerCommandMeta{
			ID: "replay-accept", ExpectedRevision: created.Revision, Grant: terminalAssignment.Grant,
		},
		Task: terminalRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := service.ClaimLease(ctx, terminalCommand); err != nil || !reflect.DeepEqual(replay, terminalAssignment) {
		t.Fatalf("claim replay after progress = %#v, %v; want %#v", replay, err, terminalAssignment)
	}
	if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: WorkerCommandMeta{
			ID: "replay-complete", ExpectedRevision: working.Revision, Grant: terminalAssignment.Grant,
		},
		Task: terminalRef, Submission: "replay-submission",
		Message: textMessage("replay-final", "done"),
	}); err != nil {
		t.Fatal(err)
	}
	if replay, err := service.ClaimLease(ctx, terminalCommand); err != nil || !reflect.DeepEqual(replay, terminalAssignment) {
		t.Fatalf("claim replay after terminal = %#v, %v; want %#v", replay, err, terminalAssignment)
	}

	_, fencedRef := refs("tenant-a", "replay-context", "replay-workspace", "task-fenced")
	if _, err := service.CreateTask(ctx, createCommand(
		"replay-create-fenced", contextRef, fencedRef, "replay-message-fenced", "fence",
	)); err != nil {
		t.Fatal(err)
	}
	fencedCommand := ClaimLeaseCommand{ID: "claim-fenced", Authority: "tenant-a", Worker: "worker-b"}
	fencedAssignment, err := service.ClaimLease(ctx, fencedCommand)
	if err != nil {
		t.Fatal(err)
	}
	if fencedAssignment.Grant.Task != fencedRef {
		t.Fatalf("fenced claim Task = %v, want %v", fencedAssignment.Grant.Task, fencedRef)
	}
	if err := service.FenceLease(ctx, FenceLeaseCommand{
		ID: "replay-fence", Grant: fencedAssignment.Grant,
	}); err != nil {
		t.Fatal(err)
	}
	if replay, err := service.ClaimLease(ctx, fencedCommand); err != nil || !reflect.DeepEqual(replay, fencedAssignment) {
		t.Fatalf("claim replay after fence = %#v, %v; want %#v", replay, err, fencedAssignment)
	}
	conflict := fencedCommand
	conflict.Worker = "worker-c"
	if _, err := service.ClaimLease(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting claim replay error = %v", err)
	}

	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestAgentWithConfig(t, path, DefaultConfig())
	for _, test := range []struct {
		command ClaimLeaseCommand
		want    LeaseAssignment
	}{
		{command: terminalCommand, want: terminalAssignment},
		{command: fencedCommand, want: fencedAssignment},
	} {
		replay, err := reopened.ClaimLease(ctx, test.command)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(replay, test.want) {
			t.Fatalf("reopened claim replay = %#v, want %#v", replay, test.want)
		}
	}
}

func TestConcurrentClaimLeaseNeverDuplicatesTask(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	clock := time.Date(2026, time.July, 19, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return clock }
	contextRef, _ := refs("tenant-a", "race-context", "race-workspace", "unused")
	const taskCount = 24
	const claimantCount = taskCount * 2
	for index := 0; index < taskCount; index++ {
		_, ref := refs(
			"tenant-a", "race-context", "race-workspace",
			fmt.Sprintf("task-%02d", index),
		)
		if _, err := service.CreateTask(ctx, createCommand(
			fmt.Sprintf("race-create-%02d", index), contextRef, ref,
			fmt.Sprintf("race-message-%02d", index), "claim concurrently",
		)); err != nil {
			t.Fatal(err)
		}
	}

	type result struct {
		worker     WorkerID
		assignment LeaseAssignment
		err        error
	}
	start := make(chan struct{})
	results := make(chan result, claimantCount)
	var wait sync.WaitGroup
	wait.Add(claimantCount)
	for index := 0; index < claimantCount; index++ {
		index := index
		go func() {
			defer wait.Done()
			<-start
			worker := WorkerID(fmt.Sprintf("worker-%02d", index))
			assignment, err := service.ClaimLease(ctx, ClaimLeaseCommand{
				ID:        CommandID(fmt.Sprintf("race-claim-%02d", index)),
				Authority: "tenant-a", Worker: worker,
			})
			results <- result{worker: worker, assignment: assignment, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	claimed := make(map[TaskRef]LeaseAssignment, taskCount)
	notFound := 0
	for result := range results {
		if errors.Is(result.err, ErrLeaseNotFound) {
			notFound++
			continue
		}
		if result.err != nil {
			t.Fatalf("concurrent claim: %v", result.err)
		}
		assignment := result.assignment
		if assignment.Grant.Worker != result.worker || assignment.Grant.Fence != 1 {
			t.Fatalf("claim for %q = %#v", result.worker, assignment)
		}
		if previous, exists := claimed[assignment.Grant.Task]; exists {
			t.Fatalf("Task %v claimed twice: %#v and %#v", assignment.Grant.Task, previous, assignment)
		}
		claimed[assignment.Grant.Task] = assignment
	}
	if len(claimed) != taskCount || notFound != claimantCount-taskCount {
		t.Fatalf("claimed=%d notFound=%d, want %d/%d", len(claimed), notFound, taskCount, claimantCount-taskCount)
	}
	for ref, want := range claimed {
		got, err := service.GetLease(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if got.Grant != want.Grant || !got.GrantedAt.Equal(want.GrantedAt) {
			t.Fatalf("durable lease for %v = %#v, want %#v", ref, got, want)
		}
	}
}

func TestClaimLeaseCorruptionFailsClosed(t *testing.T) {
	t.Run("candidate fence differs from history", func(t *testing.T) {
		service, _ := openTestAgent(t)
		ctx := context.Background()
		contextRef, taskRef := refs("tenant-a", "corrupt-context", "corrupt-workspace", "corrupt-candidate")
		if _, err := service.CreateTask(ctx, createCommand(
			"corrupt-create", contextRef, taskRef, "corrupt-message", "claim",
		)); err != nil {
			t.Fatal(err)
		}
		if _, err := service.database.ExecContext(ctx, `
			UPDATE agent_tasks SET lease_fence = 1
			WHERE authority_id = ? AND workspace_id = ? AND task_id = ?`,
			taskRef.Workspace.Authority, taskRef.Workspace.ID, taskRef.ID,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := service.ClaimLease(ctx, ClaimLeaseCommand{
			ID: "corrupt-claim", Authority: "tenant-a", Worker: "worker-a",
		}); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("claim corrupt candidate error = %v", err)
		}
		var commandCount, grantCount int
		if err := service.database.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM agent_commands
			WHERE authority_id = ? AND command_id = ?`, "tenant-a", "corrupt-claim",
		).Scan(&commandCount); err != nil {
			t.Fatal(err)
		}
		if err := service.database.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM agent_lease_grants
			WHERE authority_id = ? AND workspace_id = ? AND task_id = ?`,
			taskRef.Workspace.Authority, taskRef.Workspace.ID, taskRef.ID,
		).Scan(&grantCount); err != nil {
			t.Fatal(err)
		}
		if commandCount != 0 || grantCount != 0 {
			t.Fatalf("failed claim committed command=%d grants=%d", commandCount, grantCount)
		}
	})

	t.Run("replay grant history differs", func(t *testing.T) {
		service, _ := openTestAgent(t)
		ctx := context.Background()
		contextRef, taskRef := refs("tenant-a", "corrupt-context", "corrupt-workspace", "corrupt-replay")
		if _, err := service.CreateTask(ctx, createCommand(
			"replay-corrupt-create", contextRef, taskRef, "replay-corrupt-message", "claim",
		)); err != nil {
			t.Fatal(err)
		}
		command := ClaimLeaseCommand{ID: "replay-corrupt-claim", Authority: "tenant-a", Worker: "worker-a"}
		assignment, err := service.ClaimLease(ctx, command)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.database.ExecContext(ctx, `
			UPDATE agent_lease_grants SET worker_id = 'worker-b'
			WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND fence = ?`,
			taskRef.Workspace.Authority, taskRef.Workspace.ID, taskRef.ID, assignment.Grant.Fence,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := service.ClaimLease(ctx, command); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("claim replay with corrupt history error = %v", err)
		}
	})
}
