package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	storeapi "github.com/vibe-agi/human/internal/store"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "human.db")
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return db
}

func requestInput() storeapi.BeginRequestInput {
	return storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "task-1"},
			WorkspaceKey:   "workspace-1",
			CapabilityTier: completion.TierRemoteTools,
			Dialect:        canonical.DialectAnthropic,
			HarnessID:      "claude-code",
			HarnessVersion: "1.0.0",
			Root:           "/workspace/repo",
		},
		IdempotencyKey: "request-1",
		RequestDigest:  "digest-1",
		CanonicalRequest: canonical.Request{
			Dialect: canonical.DialectAnthropic,
			Model:   "human",
			Stream:  true,
			Messages: []canonical.Message{{
				Role:   canonical.RoleUser,
				Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "Please inspect the repository."}},
			}},
		},
	}
}

func TestBeginRequestIdempotency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()

	created, err := db.BeginRequest(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if created.Replay || created.Task.State != completion.StateAdmitted {
		t.Fatalf("created = %+v", created)
	}
	lookup, err := db.LookupRequest(ctx, storeapi.RequestKey{
		CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey,
	}, input.RequestDigest)
	if err != nil || !lookup.Replay {
		t.Fatalf("LookupRequest() = %+v, %v", lookup, err)
	}

	replay, err := db.BeginRequest(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replay || replay.Task.TaskID != input.TaskID {
		t.Fatalf("replay = %+v", replay)
	}

	input.RequestDigest = "different"
	if _, err := db.LookupRequest(ctx, storeapi.RequestKey{
		CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey,
	}, input.RequestDigest); !errors.Is(err, storeapi.ErrIdempotencyConflict) {
		t.Fatalf("lookup different digest error = %v", err)
	}
	if _, err := db.BeginRequest(ctx, input); !errors.Is(err, storeapi.ErrIdempotencyConflict) {
		t.Fatalf("different digest error = %v", err)
	}
}

func TestTaskIdentityAndLeaseAreSticky(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := input.Task.TaskKey
	leased, err := db.TransitionTask(ctx, key, completion.StateAdmitted, completion.StateLeased, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if leased.LeaseOwner != "worker-1" {
		t.Fatalf("lease owner = %q", leased.LeaseOwner)
	}
	if _, err := db.TransitionTask(ctx, key, completion.StateLeased, completion.StateAwaitingHuman, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionTask(ctx, key, completion.StateAwaitingHuman, completion.StateResponded, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionTask(ctx, key, completion.StateResponded, completion.StateAwaitingCaller, ""); err != nil {
		t.Fatal(err)
	}
	next := input
	next.IdempotencyKey = "request-2"
	next.RequestDigest = "digest-2"
	reconciled, err := db.BeginRequest(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.Task.State != completion.StateReconciled {
		t.Fatalf("clarification reply state = %s", reconciled.Task.State)
	}
	if _, err := db.TransitionTask(ctx, key, completion.StateReconciled, completion.StateLeased, "worker-2"); !errors.Is(err, storeapi.ErrLeaseConflict) {
		t.Fatalf("different worker error = %v", err)
	}
	if _, err := db.TransitionTask(ctx, key, completion.StateReconciled, completion.StateLeased, "worker-1"); err != nil {
		t.Fatal(err)
	}

	next.WorkspaceKey = "other-workspace"
	next.IdempotencyKey = "request-3"
	next.RequestDigest = "digest-3"
	if _, err := db.BeginRequest(ctx, next); !errors.Is(err, storeapi.ErrTaskConflict) {
		t.Fatalf("task identity conflict error = %v", err)
	}
}

func TestBeginRequestReconcilesAllPendingToolResultsAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := input.Task.TaskKey
	transition := func(from, to completion.State, worker string) {
		t.Helper()
		if _, err := db.TransitionTask(ctx, key, from, to, worker); err != nil {
			t.Fatal(err)
		}
	}
	transition(completion.StateAdmitted, completion.StateLeased, "worker-1")
	transition(completion.StateLeased, completion.StateAwaitingHuman, "")
	transition(completion.StateAwaitingHuman, completion.StateResponded, "")
	for _, id := range []string{"read-1", "edit-1"} {
		if _, err := db.BeginToolExecution(ctx, storeapi.ToolExecutionKey{
			CallerID: input.CallerID, TaskID: input.TaskID, ToolCallID: id,
		}, "digest-"+id); err != nil {
			t.Fatal(err)
		}
	}
	transition(completion.StateResponded, completion.StateToolsDispatched, "")
	transition(completion.StateToolsDispatched, completion.StateAwaitingResults, "")

	followup := input
	followup.IdempotencyKey = "request-2"
	followup.RequestDigest = "digest-2"
	followup.ToolResults = []storeapi.ToolResult{{
		ToolCallID: "read-1", Result: []byte(`{"text":"contents"}`),
	}}
	if _, err := db.BeginRequest(ctx, followup); !errors.Is(err, storeapi.ErrToolResultsMissing) {
		t.Fatalf("partial reconciliation error = %v", err)
	}
	if _, err := db.LookupRequest(ctx, storeapi.RequestKey{
		CallerID: input.CallerID, IdempotencyKey: followup.IdempotencyKey,
	}, followup.RequestDigest); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("partial reconciliation admitted a request: %v", err)
	}
	pending, err := db.BeginToolExecution(ctx, storeapi.ToolExecutionKey{
		CallerID: input.CallerID, TaskID: input.TaskID, ToolCallID: "read-1",
	}, "digest-read-1")
	if err != nil || pending.Execution.Status != "pending" {
		t.Fatalf("partial reconciliation was not rolled back: %+v, %v", pending, err)
	}

	followup.ToolResults = append(followup.ToolResults, storeapi.ToolResult{
		ToolCallID: "edit-1", Result: []byte(`{"error":"cas mismatch"}`), IsError: true,
	})
	reconciled, err := db.BeginRequest(ctx, followup)
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.Task.State != completion.StateReconciled || reconciled.Task.LeaseOwner != "worker-1" {
		t.Fatalf("reconciled task = %+v", reconciled.Task)
	}
	for _, id := range []string{"read-1", "edit-1"} {
		execution, err := db.BeginToolExecution(ctx, storeapi.ToolExecutionKey{
			CallerID: input.CallerID, TaskID: input.TaskID, ToolCallID: id,
		}, "digest-"+id)
		if err != nil || execution.Execution.Status != "completed" {
			t.Fatalf("execution %s = %+v, %v", id, execution, err)
		}
	}
}

func TestResponseEventLogAndCompletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	if _, err := db.BeginResponse(ctx, key); err != nil {
		t.Fatal(err)
	}
	first, err := db.AppendResponseEvent(ctx, key, "delta", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.AppendResponseEvent(ctx, key, "done", []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 2 {
		t.Fatalf("sequences = %d, %d", first.Sequence, second.Sequence)
	}
	events, err := db.ListResponseEvents(ctx, key, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || string(events[0].Data) != "two" {
		t.Fatalf("events = %+v", events)
	}
	if err := db.CompleteRequest(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRequest(ctx, key); err != nil {
		t.Fatalf("idempotent CompleteRequest() error = %v", err)
	}
	if _, err := db.AppendResponseEvent(ctx, key, "late", nil); err == nil {
		t.Fatal("event appended after completion")
	}
}

func TestWorkerResponseEventCheckpointIsIdempotentByKindIDAndDigest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	if _, err := db.BeginResponse(ctx, key); err != nil {
		t.Fatal(err)
	}
	first, err := db.AppendWorkerResponseEvent(ctx, key, "step", "event-1", "digest-1", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := db.AppendWorkerResponseEvent(ctx, key, "step", "event-1", "digest-1", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if replay.Sequence != first.Sequence || replay.EventID != "event-1" || replay.EventDigest != "digest-1" {
		t.Fatalf("worker event replay = %+v, first = %+v", replay, first)
	}
	if _, err := db.AppendWorkerResponseEvent(
		ctx, key, "step", "event-1", "digest-1", []byte("changed"),
	); !errors.Is(err, storeapi.ErrWorkerEventConflict) {
		t.Fatalf("worker event payload conflict = %v", err)
	}
	if _, err := db.AppendWorkerResponseEvent(
		ctx, key, "step", "event-1", "digest-2", []byte("one"),
	); !errors.Is(err, storeapi.ErrWorkerEventConflict) {
		t.Fatalf("worker event digest conflict = %v", err)
	}
	applied, err := db.AppendWorkerResponseEvent(ctx, key, "applied", "event-1", "digest-1", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if applied.Sequence == first.Sequence || applied.Kind != "applied" {
		t.Fatalf("applied checkpoint = %+v", applied)
	}
	events, err := db.ListResponseEvents(ctx, key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].EventID != "event-1" || events[1].EventDigest != "digest-1" {
		t.Fatalf("indexed worker response events = %+v", events)
	}
}

func TestPreStreamFailureDecisionIsAtomicAndImmutable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	decision := storeapi.ResponseDecision{
		StatusCode: 500, ContentType: "application/json", Body: []byte(`{"error":"cannot stream"}`),
	}
	failed, err := db.FailRequest(ctx, key, completion.StateAdmitted, decision)
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := db.LookupRequest(ctx, key, input.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Response.StatusCode != 500 || !failed.ResponseComplete ||
		lookup.Task.State != completion.StateFailed ||
		string(lookup.Request.Response.Body) != string(decision.Body) {
		t.Fatalf("atomic failed response = %+v, task = %+v", lookup.Request, lookup.Task)
	}
	if _, err := db.FailRequest(ctx, key, completion.StateAdmitted, decision); err != nil {
		t.Fatalf("idempotent FailRequest() error = %v", err)
	}
	if _, err := db.BeginResponse(ctx, key); !errors.Is(err, storeapi.ErrStateConflict) {
		t.Fatalf("terminal failure changed to stream: %v", err)
	}
	changed := decision
	changed.Body = []byte(`{"error":"different"}`)
	if _, err := db.FailRequest(ctx, key, completion.StateAdmitted, changed); !errors.Is(err, storeapi.ErrStateConflict) {
		t.Fatalf("terminal failure changed body: %v", err)
	}
}

func TestToolExecutionLedger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := storeapi.ToolExecutionKey{CallerID: input.CallerID, TaskID: input.TaskID, ToolCallID: "tool-1"}
	created, err := db.BeginToolExecution(ctx, key, "tool-digest")
	if err != nil {
		t.Fatal(err)
	}
	if created.Replay || created.Execution.Status != "pending" {
		t.Fatalf("created = %+v", created)
	}
	replayPending, err := db.BeginToolExecution(ctx, key, "tool-digest")
	if err != nil {
		t.Fatal(err)
	}
	if !replayPending.Replay || replayPending.Execution.Status != "pending" {
		t.Fatalf("pending replay = %+v", replayPending)
	}
	if _, err := db.BeginToolExecution(ctx, key, "different"); !errors.Is(err, storeapi.ErrToolCallConflict) {
		t.Fatalf("tool digest conflict error = %v", err)
	}
	completed, err := db.CompleteToolExecution(ctx, key, []byte(`{"ok":true}`), false)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != "completed" {
		t.Fatalf("completed = %+v", completed)
	}
	replay, err := db.BeginToolExecution(ctx, key, "tool-digest")
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replay || string(replay.Execution.Result) != `{"ok":true}` {
		t.Fatalf("completed replay = %+v", replay)
	}
	if _, err := db.CompleteToolExecution(ctx, key, []byte(`{"ok":true}`), false); err != nil {
		t.Fatalf("idempotent completion error = %v", err)
	}
}
