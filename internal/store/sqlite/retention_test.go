package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	storeapi "github.com/vibe-agi/human/internal/store"
)

func TestPurgeExpiredCompletionPayloadsLeavesImmutableReplayTombstone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	current := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	db.now = func() time.Time { return current }

	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	if _, err := db.BeginResponse(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AppendResponseEvent(ctx, key, "wire", []byte("secret response bytes")); err != nil {
		t.Fatal(err)
	}
	toolKey := storeapi.ToolExecutionKey{
		CallerID: input.CallerID, TaskID: input.TaskID, ToolCallID: "tool-secret",
	}
	if _, err := db.BeginToolExecution(ctx, toolKey, "tool-call-digest"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompleteToolExecution(ctx, toolKey, []byte(`{"secret":"tool result"}`), false); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRequest(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionTask(ctx, input.TaskKey, completion.StateAdmitted, completion.StateCanceled, ""); err != nil {
		t.Fatal(err)
	}

	current = current.Add(23 * time.Hour)
	if count, err := db.PurgeExpiredCompletionPayloads(ctx, current.Add(-24*time.Hour)); err != nil || count != 0 {
		t.Fatalf("purge inside replay grace = %d, %v", count, err)
	}
	if _, err := db.LookupRequest(ctx, key, input.RequestDigest); err != nil {
		t.Fatalf("lookup inside replay grace: %v", err)
	}

	current = current.Add(2 * time.Hour)
	if count, err := db.PurgeExpiredCompletionPayloads(ctx, current.Add(-24*time.Hour)); err != nil || count != 1 {
		t.Fatalf("purge after replay grace = %d, %v", count, err)
	}

	var canonicalLength, responseBodyLength, prunedAt int64
	var responseStatus, responseComplete int
	if err := db.db.QueryRowContext(ctx, `
		SELECT length(canonical_request), length(response_body), payload_pruned_at,
		       response_status, response_complete
		FROM completion_requests
		WHERE caller_id = ? AND idempotency_key = ?`, key.CallerID, key.IdempotencyKey).Scan(
		&canonicalLength, &responseBodyLength, &prunedAt, &responseStatus, &responseComplete,
	); err != nil {
		t.Fatal(err)
	}
	if canonicalLength != 0 || responseBodyLength != 0 || prunedAt == 0 ||
		responseStatus != 200 || responseComplete != 1 {
		t.Fatalf("request tombstone = canonical:%d body:%d pruned:%d status:%d complete:%d",
			canonicalLength, responseBodyLength, prunedAt, responseStatus, responseComplete)
	}
	var eventCount int
	if err := db.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ?`, key.CallerID, key.IdempotencyKey).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 {
		t.Fatalf("response events retained after purge = %d", eventCount)
	}
	read, err := db.ReadResponse(ctx, key, 0)
	if err != nil || !read.PayloadPruned || !read.ResponseComplete || len(read.Events) != 0 {
		t.Fatalf("pruned response snapshot = %+v, %v", read, err)
	}
	var toolResultIsNull bool
	if err := db.db.QueryRowContext(ctx, `
		SELECT result IS NULL FROM completion_tool_executions
		WHERE caller_id = ? AND task_id = ? AND tool_call_id = ?`,
		toolKey.CallerID, toolKey.TaskID, toolKey.ToolCallID).Scan(&toolResultIsNull); err != nil {
		t.Fatal(err)
	}
	if !toolResultIsNull {
		t.Fatal("terminal task tool-result payload was retained")
	}

	if _, err := db.LookupRequest(ctx, key, input.RequestDigest); !errors.Is(err, storeapi.ErrReplayPayloadExpired) {
		t.Fatalf("same digest lookup after purge = %v", err)
	}
	if _, err := db.BeginRequest(ctx, input); !errors.Is(err, storeapi.ErrReplayPayloadExpired) {
		t.Fatalf("same request admission after purge = %v", err)
	}
	if _, err := db.LookupRequest(ctx, key, "different-digest"); !errors.Is(err, storeapi.ErrIdempotencyConflict) {
		t.Fatalf("different digest lookup after purge = %v", err)
	}
	changed := input
	changed.RequestDigest = "different-digest"
	if _, err := db.BeginRequest(ctx, changed); !errors.Is(err, storeapi.ErrIdempotencyConflict) {
		t.Fatalf("different request admission after purge = %v", err)
	}
	if count, err := db.PurgeExpiredCompletionPayloads(ctx, current); err != nil || count != 0 {
		t.Fatalf("idempotent completion purge = %d, %v", count, err)
	}
	if snapshot, err := db.ListRecoverableRequests(ctx); err != nil || len(snapshot.Requests) != 0 || len(snapshot.Issues) != 0 {
		t.Fatalf("tombstone entered recovery snapshot = %+v, %v", snapshot, err)
	}
}

func TestPurgeExpiredCompletionPayloadsProtectsActiveAndUnacknowledgedRequests(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	current := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	db.now = func() time.Time { return current }

	active := requestInput()
	active.TaskID = "active-task"
	active.IdempotencyKey = "active-request"
	active.RequestDigest = "active-digest"
	if _, err := db.BeginRequest(ctx, active); err != nil {
		t.Fatal(err)
	}

	unacknowledged := requestInput()
	unacknowledged.TaskID = "unacknowledged-task"
	unacknowledged.IdempotencyKey = "unacknowledged-request"
	unacknowledged.RequestDigest = "unacknowledged-digest"
	if _, err := db.BeginRequest(ctx, unacknowledged); err != nil {
		t.Fatal(err)
	}
	unacknowledgedKey := storeapi.RequestKey{
		CallerID: unacknowledged.CallerID, IdempotencyKey: unacknowledged.IdempotencyKey,
	}
	if _, err := db.BeginResponse(ctx, unacknowledgedKey); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AppendWorkerResponseEvent(
		ctx, unacknowledgedKey, "step", "final-event", "event-digest", []byte("recoverable step"),
	); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRequest(ctx, unacknowledgedKey); err != nil {
		t.Fatal(err)
	}

	current = current.Add(48 * time.Hour)
	if count, err := db.PurgeExpiredCompletionPayloads(ctx, current.Add(-24*time.Hour)); err != nil || count != 0 {
		t.Fatalf("purge with live correctness payloads = %d, %v", count, err)
	}
	if _, err := db.LookupRequest(ctx, storeapi.RequestKey{
		CallerID: active.CallerID, IdempotencyKey: active.IdempotencyKey,
	}, active.RequestDigest); err != nil {
		t.Fatalf("active request was pruned: %v", err)
	}
	if _, err := db.LookupRequest(ctx, unacknowledgedKey, unacknowledged.RequestDigest); err != nil {
		t.Fatalf("unacknowledged request was pruned: %v", err)
	}

	if _, err := db.RecordWorkerEventReceipt(ctx, unacknowledgedKey, "final-event", "worker-retention", "event-digest"); err != nil {
		t.Fatal(err)
	}
	if count, err := db.PurgeExpiredCompletionPayloads(ctx, current.Add(-24*time.Hour)); err != nil || count != 1 {
		t.Fatalf("purge after durable receipt = %d, %v", count, err)
	}
	if _, err := db.LookupRequest(ctx, unacknowledgedKey, unacknowledged.RequestDigest); !errors.Is(err, storeapi.ErrReplayPayloadExpired) {
		t.Fatalf("acknowledged request replay = %v", err)
	}
	receipt, err := db.LookupWorkerEventReceipt(ctx, unacknowledgedKey, "final-event")
	if err != nil || receipt.EventID != "final-event" {
		t.Fatalf("worker receipt after payload purge = %+v, %v", receipt, err)
	}
	var responseEventCount int
	if err := db.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ?`,
		unacknowledgedKey.CallerID, unacknowledgedKey.IdempotencyKey).Scan(&responseEventCount); err != nil {
		t.Fatal(err)
	}
	if responseEventCount != 0 {
		t.Fatalf("acknowledged response payload events retained = %d", responseEventCount)
	}
	if _, err := db.LookupRequest(ctx, storeapi.RequestKey{
		CallerID: active.CallerID, IdempotencyKey: active.IdempotencyKey,
	}, active.RequestDigest); err != nil {
		t.Fatalf("active request was pruned by later pass: %v", err)
	}
}

func TestPurgeExpiredCompletionPayloadsPrunesPreStreamResponseBody(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	current := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	db.now = func() time.Time { return current }

	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}
	key := storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	decision := storeapi.ResponseDecision{
		StatusCode: 503, ContentType: "application/json", RetryAfter: "5",
		Body: []byte(`{"error":{"message":"sensitive failure detail"}}`),
	}
	if _, err := db.FailRequest(ctx, key, completion.StateAdmitted, decision); err != nil {
		t.Fatal(err)
	}
	current = current.Add(25 * time.Hour)
	if count, err := db.PurgeExpiredCompletionPayloads(ctx, current.Add(-24*time.Hour)); err != nil || count != 1 {
		t.Fatalf("pre-stream purge = %d, %v", count, err)
	}
	var status, bodyLength int
	var retryAfter string
	if err := db.db.QueryRowContext(ctx, `
		SELECT response_status, response_retry_after, length(response_body)
		FROM completion_requests WHERE caller_id = ? AND idempotency_key = ?`,
		key.CallerID, key.IdempotencyKey).Scan(&status, &retryAfter, &bodyLength); err != nil {
		t.Fatal(err)
	}
	if status != 503 || retryAfter != "5" || bodyLength != 0 {
		t.Fatalf("pre-stream tombstone = status:%d retry:%q body:%d", status, retryAfter, bodyLength)
	}
	if _, err := db.LookupRequest(ctx, key, input.RequestDigest); !errors.Is(err, storeapi.ErrReplayPayloadExpired) {
		t.Fatalf("pre-stream replay after purge = %v", err)
	}
	var rowCount int
	if err := db.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM completion_requests
		WHERE caller_id = ? AND idempotency_key = ?`, key.CallerID, key.IdempotencyKey).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if rowCount != 1 {
		t.Fatalf("pre-stream idempotency tombstone count = %d", rowCount)
	}
}

func TestPurgeExpiredCompletionPayloadsRequiresCutoff(t *testing.T) {
	t.Parallel()
	db := openTestStore(t)
	if _, err := db.PurgeExpiredCompletionPayloads(context.Background(), time.Time{}); err == nil {
		t.Fatal("PurgeExpiredCompletionPayloads() accepted a zero cutoff")
	}
}

func TestOpenEnablesSecureDelete(t *testing.T) {
	t.Parallel()
	db := openTestStore(t)
	var enabled int
	if err := db.db.QueryRowContext(context.Background(), "PRAGMA secure_delete").Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled != 1 {
		t.Fatalf("PRAGMA secure_delete = %d", enabled)
	}
}
