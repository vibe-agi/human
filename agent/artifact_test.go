package agent

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/vibe-agi/human/workspace"
)

func createWorkingTask(t *testing.T, service *Agent, contextRef ContextRef, taskRef TaskRef, suffix string) Task {
	t.Helper()
	ctx := context.Background()
	created, err := service.CreateTask(ctx, createCommand(
		"create-"+suffix, contextRef, taskRef, "initial-"+suffix, "perform workspace work",
	))
	if err != nil {
		t.Fatal(err)
	}
	grant := acquireTestLease(t, service, taskRef)
	working, err := service.AcceptTask(ctx, WorkerTaskCommand{
		Meta: WorkerCommandMeta{ID: CommandID("accept-" + suffix), ExpectedRevision: created.Revision, Grant: grant},
		Task: taskRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	return working
}

func freezeCommand(t *testing.T, service *Agent, suffix string, task Task, base workspace.Revision, data []byte) FreezeArtifactCommand {
	t.Helper()
	return FreezeArtifactCommand{
		Meta: workerMeta(t, service, task.Ref, CommandID("freeze-"+suffix), task.Revision),
		Task: task.Ref, Artifact: ArtifactID("artifact-" + suffix),
		ExpectedBaseRevision: base,
		Payload:              workspace.Payload{MediaType: "application/vnd.human.workspace+json", Data: data},
	}
}

func TestArtifactFreezePublishReceiptAndRecovery(t *testing.T) {
	service, path := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "context-a", "workspace-a", "task-a")
	working := createWorkingTask(t, service, contextRef, taskRef, "a")
	payload := []byte(`{"changes":[{"path":"main.go","content":"package main"}]}`)
	command := freezeCommand(t, service, "a", working, "workspace-base-a", payload)
	frozen, err := service.FreezeArtifact(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if frozen.Task.State != TaskWorking || frozen.Task.Revision != working.Revision+1 ||
		frozen.Task.Artifact == nil || *frozen.Task.Artifact != frozen.Artifact.Ref ||
		frozen.Artifact.State != ArtifactFrozen || frozen.Artifact.BaseRevision != "workspace-base-a" ||
		frozen.Artifact.ResultRevision == frozen.Artifact.BaseRevision {
		t.Fatalf("frozen result = %#v", frozen)
	}
	replayed, err := service.FreezeArtifact(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, frozen) {
		t.Fatalf("freeze replay differs:\n got %#v\nwant %#v", replayed, frozen)
	}
	payload[0] = 'X'
	content, err := service.GetArtifact(ctx, frozen.Artifact.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(content.Payload.Data) != `{"changes":[{"path":"main.go","content":"package main"}]}` {
		t.Fatalf("stored payload changed through caller slice: %q", content.Payload.Data)
	}
	content.Payload.Data[0] = 'X'
	contentAgain, err := service.GetArtifact(ctx, frozen.Artifact.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(contentAgain.Payload.Data) != `{"changes":[{"path":"main.go","content":"package main"}]}` {
		t.Fatalf("stored payload changed through returned slice: %q", contentAgain.Payload.Data)
	}

	completed, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: workerMeta(t, service, taskRef, "complete-a", frozen.Task.Revision),
		Task: taskRef, Submission: "submission-a", Artifact: &frozen.Artifact.Ref,
		Message: textMessage("final-a", "workspace Artifact ready"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != TaskCompleted || completed.Submission == nil ||
		completed.Submission.Artifact == nil || *completed.Submission.Artifact != frozen.Artifact.Ref {
		t.Fatalf("completed task = %#v", completed)
	}
	published, err := service.GetArtifact(ctx, frozen.Artifact.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if published.Artifact.State != ArtifactPublished || published.Artifact.PublishedAt == nil {
		t.Fatalf("published Artifact = %#v", published.Artifact)
	}

	receiptCommand := RecordApplyReceiptCommand{
		CommandID: "receipt-command-a", Receipt: "receipt-a", Artifact: frozen.Artifact.Ref,
		ArtifactDigest: frozen.Artifact.Digest, BaseRevision: frozen.Artifact.BaseRevision,
		ResultRevision: frozen.Artifact.ResultRevision, Decision: workspace.ApplySuccess,
		ObservedRevision: frozen.Artifact.ResultRevision,
	}
	wrongObservation := receiptCommand
	wrongObservation.CommandID = "receipt-command-wrong-observation"
	wrongObservation.Receipt = "receipt-wrong-observation"
	wrongObservation.ObservedRevision = frozen.Artifact.BaseRevision
	if _, err := service.RecordApplyReceipt(ctx, wrongObservation); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("wrong success observation error = %v", err)
	}
	beforeReceipt, err := service.GetWorkspaceHead(ctx, taskRef.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if beforeReceipt.ConfirmedRevision != frozen.Artifact.BaseRevision {
		t.Fatalf("invalid receipt advanced head to %q", beforeReceipt.ConfirmedRevision)
	}
	wrongDigest := receiptCommand
	wrongDigest.CommandID = "receipt-command-wrong-digest"
	wrongDigest.Receipt = "receipt-wrong-digest"
	wrongDigest.ArtifactDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	if _, err := service.RecordApplyReceipt(ctx, wrongDigest); !errors.Is(err, ErrReceiptConflict) {
		t.Fatalf("wrong Artifact digest error = %v", err)
	}
	receipt, err := service.RecordApplyReceipt(ctx, receiptCommand)
	if err != nil {
		t.Fatal(err)
	}
	receiptReplay, err := service.RecordApplyReceipt(ctx, receiptCommand)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(receiptReplay, receipt) {
		t.Fatalf("receipt replay differs:\n got %#v\nwant %#v", receiptReplay, receipt)
	}
	changedDecision := receiptCommand
	changedDecision.CommandID = "receipt-command-a-changed"
	changedDecision.Receipt = "receipt-a-changed"
	changedDecision.Decision = workspace.ApplyRejected
	changedDecision.ObservedRevision = ""
	if _, err := service.RecordApplyReceipt(ctx, changedDecision); !errors.Is(err, ErrReceiptConflict) {
		t.Fatalf("changed immutable receipt error = %v", err)
	}
	head, err := service.GetWorkspaceHead(ctx, taskRef.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if head.ConfirmedRevision != frozen.Artifact.ResultRevision {
		t.Fatalf("workspace head = %q, want %q", head.ConfirmedRevision, frozen.Artifact.ResultRevision)
	}

	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.DatabasePath = path
	reopened, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.GetArtifact(ctx, frozen.Artifact.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered, published) {
		t.Fatalf("recovered Artifact differs:\n got %#v\nwant %#v", recovered, published)
	}
	recoveredReceipt, err := reopened.GetApplyReceipt(ctx, frozen.Artifact.Ref)
	if err != nil || !reflect.DeepEqual(recoveredReceipt, receipt) {
		t.Fatalf("recovered receipt = %#v, %v", recoveredReceipt, err)
	}
}

func TestFreezeReplayIgnoresLoweredAdmissionLimit(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agent.db")
	config := DefaultConfig()
	config.DatabasePath = path
	config.MaxArtifactBytes = 2
	service, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	contextRef, taskRef := refs("tenant-a", "limit-context", "limit-workspace", "limit-task")
	working := createWorkingTask(t, service, contextRef, taskRef, "limit")
	command := freezeCommand(t, service, "limit", working, "limit-base", []byte("{}"))
	frozen, err := service.FreezeArtifact(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	config.MaxArtifactBytes = 1
	reopened, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replayed, err := reopened.FreezeArtifact(ctx, command)
	if err != nil {
		t.Fatalf("exact replay after lowered admission limit: %v", err)
	}
	if !reflect.DeepEqual(replayed, frozen) {
		t.Fatalf("freeze replay changed after lowered limit:\n got %#v\nwant %#v", replayed, frozen)
	}
	newContext, newTask := refs("tenant-a", "limit-context-2", "limit-workspace-2", "limit-task-2")
	newWorking := createWorkingTask(t, reopened, newContext, newTask, "limit-new")
	if _, err := reopened.FreezeArtifact(ctx, freezeCommand(
		t, reopened, "limit-new", newWorking, "limit-base-2", []byte("{}"),
	)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("new Artifact bypassed lowered admission limit: %v", err)
	}
}

func TestSharedWorkspaceOnlyOneSuccessReceipt(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextA, taskA := refs("tenant-a", "context-a", "shared-workspace", "task-a")
	contextB, taskB := refs("tenant-a", "context-b", "shared-workspace", "task-b")
	workingA := createWorkingTask(t, service, contextA, taskA, "a")
	workingB := createWorkingTask(t, service, contextB, taskB, "b")
	frozenA, err := service.FreezeArtifact(ctx, freezeCommand(t, service, "a", workingA, "shared-base", []byte(`{"edit":"A"}`)))
	if err != nil {
		t.Fatal(err)
	}
	frozenB, err := service.FreezeArtifact(ctx, freezeCommand(t, service, "b", workingB, "shared-base", []byte(`{"edit":"B"}`)))
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		suffix string
		ref    TaskRef
		frozen FreezeArtifactResult
	}{
		{"a", taskA, frozenA}, {"b", taskB, frozenB},
	} {
		artifactRef := item.frozen.Artifact.Ref
		if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
			Meta: workerMeta(t, service, item.ref, CommandID("complete-"+item.suffix), item.frozen.Task.Revision),
			Task: item.ref, Submission: SubmissionID("submission-" + item.suffix), Artifact: &artifactRef,
			Message: textMessage("final-"+item.suffix, "done"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	receiptFor := func(commandID, receiptID string, artifact Artifact, decision workspace.ApplyDecision) RecordApplyReceiptCommand {
		command := RecordApplyReceiptCommand{
			CommandID: CommandID(commandID), Receipt: ApplyReceiptID(receiptID), Artifact: artifact.Ref,
			ArtifactDigest: artifact.Digest, BaseRevision: artifact.BaseRevision,
			ResultRevision: artifact.ResultRevision, Decision: decision,
		}
		if decision == workspace.ApplySuccess {
			command.ObservedRevision = artifact.ResultRevision
		}
		return command
	}
	if _, err := service.RecordApplyReceipt(ctx, receiptFor(
		"receipt-command-a", "receipt-a", frozenA.Artifact, workspace.ApplySuccess,
	)); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RecordApplyReceipt(ctx, receiptFor(
		"receipt-command-b-bad", "receipt-b-bad", frozenB.Artifact, workspace.ApplySuccess,
	)); !errors.Is(err, ErrWorkspaceConflict) {
		t.Fatalf("second success error = %v", err)
	}
	conflictCommand := receiptFor(
		"receipt-command-b", "receipt-b", frozenB.Artifact, workspace.ApplyConflict,
	)
	conflictCommand.ObservedRevision = frozenA.Artifact.ResultRevision
	receiptB, err := service.RecordApplyReceipt(ctx, conflictCommand)
	if err != nil {
		t.Fatal(err)
	}
	if receiptB.Decision != workspace.ApplyConflict {
		t.Fatalf("second receipt = %#v", receiptB)
	}
	head, err := service.GetWorkspaceHead(ctx, taskA.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if head.ConfirmedRevision != frozenA.Artifact.ResultRevision {
		t.Fatalf("forked workspace head = %q", head.ConfirmedRevision)
	}
}

func TestConcurrentSharedWorkspaceSuccessCAS(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextA, taskA := refs("tenant-a", "parallel-a", "parallel-workspace", "parallel-task-a")
	contextB, taskB := refs("tenant-a", "parallel-b", "parallel-workspace", "parallel-task-b")
	workingA := createWorkingTask(t, service, contextA, taskA, "parallel-a")
	workingB := createWorkingTask(t, service, contextB, taskB, "parallel-b")
	frozen := make([]FreezeArtifactResult, 2)
	var err error
	frozen[0], err = service.FreezeArtifact(ctx, freezeCommand(
		t, service, "parallel-a", workingA, "parallel-base", []byte(`{"edit":"A"}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	frozen[1], err = service.FreezeArtifact(ctx, freezeCommand(
		t, service, "parallel-b", workingB, "parallel-base", []byte(`{"edit":"B"}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	for index, ref := range []TaskRef{taskA, taskB} {
		artifactRef := frozen[index].Artifact.Ref
		if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
			Meta: workerMeta(t, service, ref,
				CommandID("parallel-complete-"+string(rune('a'+index))), frozen[index].Task.Revision),
			Task: ref, Submission: SubmissionID("parallel-submission-" + string(rune('a'+index))),
			Artifact: &artifactRef, Message: textMessage("parallel-final-"+string(rune('a'+index)), "done"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	type outcome struct {
		index   int
		receipt ApplyReceipt
		err     error
	}
	outcomes := make(chan outcome, len(frozen))
	var wait sync.WaitGroup
	for index, item := range frozen {
		index, artifact := index, item.Artifact
		wait.Add(1)
		go func() {
			defer wait.Done()
			receipt, err := service.RecordApplyReceipt(ctx, RecordApplyReceiptCommand{
				CommandID: CommandID("parallel-receipt-command-" + string(rune('a'+index))),
				Receipt:   ApplyReceiptID("parallel-receipt-" + string(rune('a'+index))),
				Artifact:  artifact.Ref, ArtifactDigest: artifact.Digest,
				BaseRevision: artifact.BaseRevision, ResultRevision: artifact.ResultRevision,
				ObservedRevision: artifact.ResultRevision, Decision: workspace.ApplySuccess,
			})
			outcomes <- outcome{index: index, receipt: receipt, err: err}
		}()
	}
	wait.Wait()
	close(outcomes)
	var winner *outcome
	conflicts := 0
	for result := range outcomes {
		switch {
		case result.err == nil:
			copy := result
			winner = &copy
		case errors.Is(result.err, ErrWorkspaceConflict):
			conflicts++
		default:
			t.Fatalf("parallel success receipt error = %v", result.err)
		}
	}
	if winner == nil || conflicts != 1 {
		t.Fatalf("winner=%#v conflicts=%d", winner, conflicts)
	}
	head, err := service.GetWorkspaceHead(ctx, taskA.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if head.ConfirmedRevision != frozen[winner.index].Artifact.ResultRevision {
		t.Fatalf("parallel head = %q, winner = %#v", head.ConfirmedRevision, winner)
	}
}

func TestArtifactPublicationRollbackAndDiscard(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, firstRef := refs("tenant-a", "context", "workspace", "task-a")
	_, secondRef := refs("tenant-a", "context", "workspace", "task-b")
	first := createWorkingTask(t, service, contextRef, firstRef, "a")
	second := createWorkingTask(t, service, contextRef, secondRef, "b")
	if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: workerMeta(t, service, firstRef, "complete-a", first.Revision),
		Task: firstRef, Submission: "shared-submission", Message: textMessage("final-a", "done"),
	}); err != nil {
		t.Fatal(err)
	}
	frozen, err := service.FreezeArtifact(ctx, freezeCommand(t, service, "b", second, "base", []byte(`{"edit":"B"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: workerMeta(t, service, secondRef, "content-only-b", frozen.Task.Revision),
		Task: secondRef, Submission: "content-submission-b", Message: textMessage("content-final-b", "done"),
	}); !errors.Is(err, ErrArtifactState) {
		t.Fatalf("content completion with frozen Artifact error = %v", err)
	}
	artifactRef := frozen.Artifact.Ref
	if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: workerMeta(t, service, secondRef, "complete-b", frozen.Task.Revision),
		Task: secondRef, Submission: "shared-submission", Artifact: &artifactRef,
		Message: textMessage("final-b", "done"),
	}); !errors.Is(err, ErrSubmissionConflict) {
		t.Fatalf("publication conflict error = %v", err)
	}
	unchanged, err := service.GetTask(ctx, secondRef)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := service.GetArtifact(ctx, artifactRef)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.State != TaskWorking || unchanged.Revision != frozen.Task.Revision ||
		unchanged.Submission != nil || artifact.Artifact.State != ArtifactFrozen {
		t.Fatalf("failed publication leaked state: task=%#v Artifact=%#v", unchanged, artifact.Artifact)
	}
	canceled, err := service.CancelTask(ctx, TaskCommand{
		Meta: CommandMeta{ID: "cancel-b", ExpectedRevision: unchanged.Revision}, Task: secondRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	discarded, err := service.GetArtifact(ctx, artifactRef)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.State != TaskCanceled || discarded.Artifact.State != ArtifactDiscarded || discarded.Artifact.DiscardedAt == nil {
		t.Fatalf("cancel/discard = %#v / %#v", canceled, discarded.Artifact)
	}
}

func TestFailDiscardsFrozenArtifact(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "fail-context", "fail-workspace", "fail-task")
	working := createWorkingTask(t, service, contextRef, taskRef, "fail")
	frozen, err := service.FreezeArtifact(ctx, freezeCommand(
		t, service, "fail", working, "fail-base", []byte(`{"edit":"fail"}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	failed, err := service.FailTask(ctx, WorkerTaskCommand{
		Meta: workerMeta(t, service, taskRef, "fail-task", frozen.Task.Revision), Task: taskRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	content, err := service.GetArtifact(ctx, frozen.Artifact.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != TaskFailed || content.Artifact.State != ArtifactDiscarded || content.Artifact.DiscardedAt == nil {
		t.Fatalf("fail/discard = %#v / %#v", failed, content.Artifact)
	}
}

func TestReceiptUniqueFailureRollsBackWorkspaceCAS(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextA, taskA := refs("tenant-a", "receipt-a", "receipt-workspace", "receipt-task-a")
	contextB, taskB := refs("tenant-a", "receipt-b", "receipt-workspace", "receipt-task-b")
	working := []Task{
		createWorkingTask(t, service, contextA, taskA, "receipt-a"),
		createWorkingTask(t, service, contextB, taskB, "receipt-b"),
	}
	frozen := make([]FreezeArtifactResult, 2)
	for index, suffix := range []string{"receipt-a", "receipt-b"} {
		var err error
		frozen[index], err = service.FreezeArtifact(ctx, freezeCommand(
			t, service, suffix, working[index], "receipt-base", []byte(`{"edit":"`+suffix+`"}`),
		))
		if err != nil {
			t.Fatal(err)
		}
		artifactRef := frozen[index].Artifact.Ref
		if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
			Meta: workerMeta(t, service, []TaskRef{taskA, taskB}[index], CommandID("complete-"+suffix), frozen[index].Task.Revision),
			Task: []TaskRef{taskA, taskB}[index], Submission: SubmissionID("submission-" + suffix),
			Artifact: &artifactRef, Message: textMessage("final-"+suffix, "done"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	rejectedArtifact := frozen[0].Artifact
	if _, err := service.RecordApplyReceipt(ctx, RecordApplyReceiptCommand{
		CommandID: "receipt-rejected-command", Receipt: "shared-receipt-id",
		Artifact: rejectedArtifact.Ref, ArtifactDigest: rejectedArtifact.Digest,
		BaseRevision: rejectedArtifact.BaseRevision, ResultRevision: rejectedArtifact.ResultRevision,
		Decision: workspace.ApplyRejected,
	}); err != nil {
		t.Fatal(err)
	}
	successArtifact := frozen[1].Artifact
	success := RecordApplyReceiptCommand{
		CommandID: "receipt-success-command", Receipt: "shared-receipt-id",
		Artifact: successArtifact.Ref, ArtifactDigest: successArtifact.Digest,
		BaseRevision: successArtifact.BaseRevision, ResultRevision: successArtifact.ResultRevision,
		ObservedRevision: successArtifact.ResultRevision, Decision: workspace.ApplySuccess,
	}
	if _, err := service.RecordApplyReceipt(ctx, success); !errors.Is(err, ErrReceiptConflict) {
		t.Fatalf("duplicate receipt id error = %v", err)
	}
	head, err := service.GetWorkspaceHead(ctx, taskA.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if head.ConfirmedRevision != successArtifact.BaseRevision {
		t.Fatalf("failed receipt leaked head update: %q", head.ConfirmedRevision)
	}
	if _, err := service.GetApplyReceipt(ctx, successArtifact.Ref); !errors.Is(err, ErrReceiptNotFound) {
		t.Fatalf("failed receipt persisted = %v", err)
	}
	var commandCount int
	if err := service.database.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_commands
		WHERE authority_id = ? AND command_id = ?`, taskA.Workspace.Authority, success.CommandID,
	).Scan(&commandCount); err != nil {
		t.Fatal(err)
	}
	if commandCount != 0 {
		t.Fatalf("failed receipt command rows = %d", commandCount)
	}
}

func TestArtifactConflictRollsBackWorkspaceHeadBootstrap(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextA, taskA := refs("tenant-a", "bootstrap-a", "bootstrap-workspace-a", "bootstrap-task-a")
	contextB, taskB := refs("tenant-a", "bootstrap-b", "bootstrap-workspace-b", "bootstrap-task-b")
	workingA := createWorkingTask(t, service, contextA, taskA, "bootstrap-a")
	workingB := createWorkingTask(t, service, contextB, taskB, "bootstrap-b")
	frozenA, err := service.FreezeArtifact(ctx, freezeCommand(
		t, service, "bootstrap-a", workingA, "bootstrap-base-a", []byte(`{"edit":"A"}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	conflict := freezeCommand(t, service, "bootstrap-b", workingB, "bootstrap-base-b", []byte(`{"edit":"B"}`))
	conflict.Artifact = frozenA.Artifact.Ref.ID
	if _, err := service.FreezeArtifact(ctx, conflict); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("duplicate Artifact id error = %v", err)
	}
	if _, err := service.GetWorkspaceHead(ctx, taskB.Workspace); !errors.Is(err, ErrWorkspaceNotFound) {
		t.Fatalf("failed freeze leaked Workspace head = %v", err)
	}
	task, err := service.GetTask(ctx, taskB)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != TaskWorking || task.Revision != workingB.Revision || task.Artifact != nil {
		t.Fatalf("failed freeze leaked Task state = %#v", task)
	}
}

func TestReceiptReadAndReplayRejectArtifactIdentityDrift(t *testing.T) {
	service, path := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "drift-context", "drift-workspace", "drift-task")
	working := createWorkingTask(t, service, contextRef, taskRef, "drift")
	frozen, err := service.FreezeArtifact(ctx, freezeCommand(
		t, service, "drift", working, "drift-base", []byte(`{"edit":"drift"}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	artifactRef := frozen.Artifact.Ref
	if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: workerMeta(t, service, taskRef, "complete-drift", frozen.Task.Revision),
		Task: taskRef, Submission: "submission-drift", Artifact: &artifactRef,
		Message: textMessage("final-drift", "done"),
	}); err != nil {
		t.Fatal(err)
	}
	command := RecordApplyReceiptCommand{
		CommandID: "receipt-command-drift", Receipt: "receipt-drift", Artifact: artifactRef,
		ArtifactDigest: frozen.Artifact.Digest, BaseRevision: frozen.Artifact.BaseRevision,
		ResultRevision: frozen.Artifact.ResultRevision, ObservedRevision: frozen.Artifact.ResultRevision,
		Decision: workspace.ApplySuccess,
	}
	if _, err := service.RecordApplyReceipt(ctx, command); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		UPDATE agent_apply_receipts SET base_revision = 'drifted-valid-base'
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		artifactRef.Workspace.Authority, artifactRef.Workspace.ID, artifactRef.ID,
	); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.DatabasePath = path
	reopened, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.GetApplyReceipt(ctx, artifactRef); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("drifted receipt read error = %v", err)
	}
	if _, err := reopened.RecordApplyReceipt(ctx, command); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("drifted receipt replay error = %v", err)
	}
}

func TestArtifactPayloadCorruptionFailsReadAndReplay(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "payload-context", "payload-workspace", "payload-task")
	working := createWorkingTask(t, service, contextRef, taskRef, "payload")
	command := freezeCommand(t, service, "payload", working, "payload-base", []byte(`{"edit":"A"}`))
	frozen, err := service.FreezeArtifact(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.database.ExecContext(ctx, `
		UPDATE agent_artifacts SET payload = ?
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		[]byte(`{"edit":"B"}`), frozen.Artifact.Ref.Workspace.Authority,
		frozen.Artifact.Ref.Workspace.ID, frozen.Artifact.Ref.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetArtifact(ctx, frozen.Artifact.Ref); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("corrupt Artifact read error = %v", err)
	}
	if _, err := service.FreezeArtifact(ctx, command); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("corrupt Artifact replay error = %v", err)
	}
}

func TestSubmissionCannotReferenceAnotherTasksArtifact(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, firstRef := refs("tenant-a", "fk-context", "fk-workspace", "fk-task-a")
	_, secondRef := refs("tenant-a", "fk-context", "fk-workspace", "fk-task-b")
	first := createWorkingTask(t, service, contextRef, firstRef, "fk-a")
	_ = createWorkingTask(t, service, contextRef, secondRef, "fk-b")
	frozen, err := service.FreezeArtifact(ctx, freezeCommand(t, service, "fk-a", first, "fk-base", []byte(`{"edit":"A"}`)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.database.ExecContext(ctx, `
		INSERT INTO agent_submissions (
		  authority_id, workspace_id, task_id, submission_id,
		  final_message_id, artifact_id, published_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		secondRef.Workspace.Authority, secondRef.Workspace.ID, secondRef.ID,
		"cross-task-submission", "initial-fk-b", frozen.Artifact.Ref.ID, unixNano(service.now().UTC()),
	)
	if err == nil {
		t.Fatal("cross-Task Artifact reference passed SQLite foreign key")
	}
}

func TestTaskReadRejectsMismatchedArtifactState(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "state-context", "state-workspace", "state-task")
	working := createWorkingTask(t, service, contextRef, taskRef, "state")
	frozen, err := service.FreezeArtifact(ctx, freezeCommand(
		t, service, "state", working, "state-base", []byte(`{"edit":"state"}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	artifactRef := frozen.Artifact.Ref
	if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: workerMeta(t, service, taskRef, "complete-state", frozen.Task.Revision),
		Task: taskRef, Submission: "submission-state", Artifact: &artifactRef,
		Message: textMessage("final-state", "done"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.database.ExecContext(ctx, `
		UPDATE agent_artifacts SET state = ?, published_at = NULL
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		ArtifactFrozen, artifactRef.Workspace.Authority, artifactRef.Workspace.ID, artifactRef.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetTask(ctx, taskRef); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("mismatched Task/Artifact read error = %v", err)
	}
}
