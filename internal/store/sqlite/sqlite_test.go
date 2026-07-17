package sqlite

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
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

func TestOpenUsesPrivateFileWithoutChangingCallerDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows file modes are ACL-backed")
	}
	parent := filepath.Join(t.TempDir(), "caller-owned")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name      string
		precreate bool
	}{
		{name: "new"},
		{name: "existing broad permissions", precreate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(parent, test.name+".db")
			if test.precreate {
				if err := os.WriteFile(path, nil, 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			store, err := Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			assertFileMode(t, path, 0o600)
			assertFileMode(t, parent, 0o755)
		})
	}
}

func assertFileMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %04o, want %04o", path, got, want)
	}
}

func TestOpenRejectsUnversionedSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "human.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `DROP TABLE human_schema`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, path); !errors.Is(err, errUnsupportedSchema) {
		t.Fatalf("Open unversioned schema error = %v", err)
	}
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
			LeaseOwner:     "worker-1",
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

func beginStreamingResponse(t *testing.T, db *Store, input storeapi.BeginRequestInput) storeapi.RequestKey {
	t.Helper()
	key := storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	if _, err := db.BeginResponse(context.Background(), key); err != nil {
		t.Fatalf("BeginResponse() error = %v", err)
	}
	return key
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
	if created.Replay || created.Task.State != completion.StateAdmitted || created.Task.LeaseOwner != "worker-1" {
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
	requestKey := beginStreamingResponse(t, db, input)
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
	if err := db.CompleteRequest(ctx, requestKey); err != nil {
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
	requestKey := beginStreamingResponse(t, db, input)
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
	if err := db.CompleteRequest(ctx, requestKey); err != nil {
		t.Fatal(err)
	}

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

func TestReadResponseKeepsCompletionAndCursorEventsInOneSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := beginStreamingResponse(t, db, input)
	first, err := db.AppendResponseEvent(ctx, key, "wire", []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	open, err := db.ReadResponse(ctx, key, 0)
	if err != nil || open.Response.StatusCode != 200 || open.ResponseComplete ||
		len(open.Events) != 1 || open.Events[0].Sequence != first.Sequence {
		t.Fatalf("open response snapshot = %+v, %v", open, err)
	}

	terminal, err := db.AppendResponseEvent(ctx, key, "applied", []byte("terminal"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRequest(ctx, key); err != nil {
		t.Fatal(err)
	}
	complete, err := db.ReadResponse(ctx, key, first.Sequence)
	if err != nil || !complete.ResponseComplete || len(complete.Events) != 1 ||
		complete.Events[0].Sequence != terminal.Sequence || string(complete.Events[0].Data) != "terminal" {
		t.Fatalf("complete response snapshot = %+v, %v", complete, err)
	}
	complete.Response.Body = append(complete.Response.Body, 'x')
	complete.Events[0].Data[0] = 'X'
	reloaded, err := db.ReadResponse(ctx, key, first.Sequence)
	if err != nil || string(reloaded.Events[0].Data) != "terminal" {
		t.Fatalf("response snapshot aliases store memory: %+v, %v", reloaded, err)
	}
}

func TestListWorkerEventStagesUsesIndexedID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := beginStreamingResponse(t, db, input)
	if _, err := db.AppendWorkerResponseEvent(ctx, key, "step", "other", "digest-other", []byte("other")); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"step", "applied"} {
		if _, err := db.AppendWorkerResponseEvent(ctx, key, kind, "target", "digest-target", []byte(kind)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.AppendResponseEvent(ctx, key, "wire", []byte("not-a-stage")); err != nil {
		t.Fatal(err)
	}

	events, err := db.ListWorkerEventStages(ctx, key, "target")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("worker stages = %+v", events)
	}
	for _, event := range events {
		if event.EventID == "other" || event.Kind == "wire" {
			t.Fatalf("unrelated indexed event leaked into stage lookup: %+v", events)
		}
	}
	if events[0].Sequence >= events[1].Sequence {
		t.Fatalf("worker stages are not sequence ordered: %+v", events)
	}
	if _, err := db.ListWorkerEventStages(ctx, key, ""); err == nil {
		t.Fatal("empty worker event id was accepted")
	}
}

func TestCompleteNonStreamingResponseAtomicallyDecidesAndReplays(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	input.CanonicalRequest.Stream = false
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	before, err := db.LookupRequest(ctx, key, input.RequestDigest)
	if err != nil || before.Request.Response.StatusCode != 0 || before.Request.ResponseComplete {
		t.Fatalf("pre-terminal decision = %+v, %v", before.Request, err)
	}
	decision := storeapi.ResponseDecision{
		StatusCode: 409, ContentType: "application/json", RetryAfter: "3",
		Body: []byte(`{"error":{"message":"rejected"}}`),
	}
	completed, err := db.CompleteNonStreamingResponse(ctx, key, decision)
	if err != nil {
		t.Fatal(err)
	}
	if !completed.ResponseComplete || !responseDecisionsEqual(completed.Response, decision) || completed.CompletedAt == nil {
		t.Fatalf("completed aggregate = %+v", completed)
	}
	replay, err := db.CompleteNonStreamingResponse(ctx, key, decision)
	if err != nil || !responseDecisionsEqual(replay.Response, decision) {
		t.Fatalf("aggregate replay = %+v, %v", replay, err)
	}
	changed := decision
	changed.Body = []byte(`{"error":{"message":"changed"}}`)
	if _, err := db.CompleteNonStreamingResponse(ctx, key, changed); !errors.Is(err, storeapi.ErrStateConflict) {
		t.Fatalf("changed aggregate decision = %v", err)
	}
	if _, err := db.AppendResponseEvent(ctx, key, "late", nil); err == nil {
		t.Fatal("event appended after aggregate completion")
	}
}

func TestCompleteNonStreamingResponseRejectsStreamingRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	_, err := db.CompleteNonStreamingResponse(ctx, storeapi.RequestKey{
		CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey,
	}, storeapi.ResponseDecision{
		StatusCode: 200, ContentType: "application/json", Body: []byte(`{}`),
	})
	if !errors.Is(err, storeapi.ErrStateConflict) {
		t.Fatalf("streaming aggregate completion = %v", err)
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
		lookup.Task.State != completion.StateAdmitted || lookup.Task.RetryRequestDigest != input.RequestDigest ||
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

	different := input
	different.IdempotencyKey = "request-different"
	different.RequestDigest = "different-digest"
	if _, err := db.BeginRequest(ctx, different); !errors.Is(err, storeapi.ErrTaskNotReady) {
		t.Fatalf("different request used pre-stream retry grant: %v", err)
	}
	retry := input
	retry.IdempotencyKey = "request-retry"
	retried, err := db.BeginRequest(ctx, retry)
	if err != nil {
		t.Fatalf("exact request retry with new key: %v", err)
	}
	if retried.Replay || retried.Task.State != completion.StateAdmitted || retried.Task.RetryRequestDigest != "" {
		t.Fatalf("retried admission = %+v", retried)
	}
	old, err := db.LookupRequest(ctx, key, input.RequestDigest)
	if err != nil || old.Request.Response.StatusCode != decision.StatusCode ||
		!bytes.Equal(old.Request.Response.Body, decision.Body) {
		t.Fatalf("old key lost exact failure replay: %+v, %v", old.Request, err)
	}
}

func TestPreStreamRetryAllowsOnlyOneConcurrentNewKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	decision := storeapi.ResponseDecision{
		StatusCode: 500, ContentType: "application/json", Body: []byte(`{"error":"retry"}`),
	}
	if _, err := db.FailRequest(ctx, key, completion.StateAdmitted, decision); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for _, idempotencyKey := range []string{"retry-a", "retry-b"} {
		candidate := input
		candidate.IdempotencyKey = idempotencyKey
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := db.BeginRequest(ctx, candidate)
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	succeeded := 0
	conflicted := 0
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, storeapi.ErrTaskNotReady):
			conflicted++
		default:
			t.Fatalf("concurrent retry error = %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("concurrent retry outcomes: success=%d conflict=%d", succeeded, conflicted)
	}
	var active int
	if err := db.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM completion_requests
		WHERE caller_id = ? AND task_id = ? AND response_complete = 0`,
		input.CallerID, input.TaskID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("active requests = %d, want 1", active)
	}
}

func TestPreStreamFailurePreservesReconciledTaskAndTransitionClearsRetryGrant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	requestKey := beginStreamingResponse(t, db, input)
	taskKey := input.Task.TaskKey
	transitions := []struct {
		from, to completion.State
		worker   string
	}{
		{completion.StateAdmitted, completion.StateLeased, "worker-1"},
		{completion.StateLeased, completion.StateAwaitingHuman, ""},
		{completion.StateAwaitingHuman, completion.StateResponded, ""},
		{completion.StateResponded, completion.StateAwaitingCaller, ""},
	}
	for _, transition := range transitions {
		if _, err := db.TransitionTask(ctx, taskKey, transition.from, transition.to, transition.worker); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.CompleteRequest(ctx, requestKey); err != nil {
		t.Fatal(err)
	}
	followup := input
	followup.IdempotencyKey = "request-reconciled"
	followup.RequestDigest = "digest-reconciled"
	admitted, err := db.BeginRequest(ctx, followup)
	if err != nil || admitted.Task.State != completion.StateReconciled {
		t.Fatalf("reconciled admission = %+v, %v", admitted, err)
	}
	decision := storeapi.ResponseDecision{
		StatusCode: 500, ContentType: "application/json", Body: []byte(`{"error":"metadata"}`),
	}
	if _, err := db.FailRequest(ctx, admitted.Request.RequestKey, completion.StateReconciled, decision); err != nil {
		t.Fatal(err)
	}
	failedTask, err := db.GetTask(ctx, taskKey)
	if err != nil || failedTask.State != completion.StateReconciled ||
		failedTask.RetryRequestDigest != followup.RequestDigest {
		t.Fatalf("task after reconciled pre-stream failure = %+v, %v", failedTask, err)
	}
	leased, err := db.TransitionTask(ctx, taskKey, completion.StateReconciled, completion.StateLeased, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if leased.RetryRequestDigest != "" {
		t.Fatalf("state progress retained retry grant: %+v", leased)
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
	if _, err := db.LookupToolExecution(ctx, key); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("missing tool execution lookup error = %v", err)
	}
	created, err := db.BeginToolExecution(ctx, key, "tool-digest")
	if err != nil {
		t.Fatal(err)
	}
	if created.Replay || created.Execution.Status != "pending" {
		t.Fatalf("created = %+v", created)
	}
	lookedUp, err := db.LookupToolExecution(ctx, key)
	if err != nil || lookedUp.RequestDigest != "tool-digest" || lookedUp.Status != "pending" {
		t.Fatalf("tool execution lookup = %+v, %v", lookedUp, err)
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
