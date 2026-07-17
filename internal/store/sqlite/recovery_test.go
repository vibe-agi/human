package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	storeapi "github.com/vibe-agi/human/internal/store"
)

func TestRecoverableRequestsSurviveReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "human.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	recoverable := requestInput()
	recoverable.ExecAllowed = true
	recoverable.CanonicalRequest.Messages = append(recoverable.CanonicalRequest.Messages,
		canonical.Message{
			Role: canonical.RoleAssistant,
			Blocks: []canonical.Block{{
				Type: canonical.BlockToolUse, ToolCallID: "call-1", ToolName: "read_file",
				Input: map[string]any{"path": "README.md", "line": float64(7)},
			}},
		})
	recoverable.CanonicalRequest.Tools = []canonical.Tool{{
		Name: "read_file", Description: "Read one workspace file",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`),
	}}
	if _, err := db.BeginRequest(ctx, recoverable); err != nil {
		db.Close()
		t.Fatal(err)
	}

	completed := requestInput()
	completed.TaskID = "task-completed-response"
	completed.IdempotencyKey = "request-completed-response"
	if _, err := db.BeginRequest(ctx, completed); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.BeginResponse(ctx, storeapi.RequestKey{
		CallerID: completed.CallerID, IdempotencyKey: completed.IdempotencyKey,
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.CompleteRequest(ctx, storeapi.RequestKey{
		CallerID: completed.CallerID, IdempotencyKey: completed.IdempotencyKey,
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}

	terminal := requestInput()
	terminal.TaskID = "task-terminal"
	terminal.IdempotencyKey = "request-terminal"
	if _, err := db.BeginRequest(ctx, terminal); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.TransitionTask(ctx, terminal.TaskKey, completion.StateAdmitted, completion.StateCanceled, ""); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	snapshot, err := db.ListRecoverableRequests(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Issues) != 0 {
		t.Fatalf("ListRecoverableRequests() issues = %+v", snapshot.Issues)
	}
	if len(snapshot.Requests) != 1 {
		t.Fatalf("ListRecoverableRequests() returned %d entries: %+v", len(snapshot.Requests), snapshot.Requests)
	}
	got := snapshot.Requests[0]
	if !got.Replay || got.Request.RequestKey != (storeapi.RequestKey{
		CallerID: recoverable.CallerID, IdempotencyKey: recoverable.IdempotencyKey,
	}) || got.Task.TaskKey != recoverable.TaskKey {
		t.Fatalf("recovered identity = %+v", got)
	}
	if got.Task.ExecAllowed != recoverable.ExecAllowed || got.Task.Root != recoverable.Root {
		t.Fatalf("recovered task = %+v", got.Task)
	}
	if string(mustCanonicalJSON(t, got.Request.CanonicalRequest)) != string(mustCanonicalJSON(t, recoverable.CanonicalRequest)) {
		t.Fatalf("canonical request changed across reopen\ngot:  %s\nwant: %s",
			mustCanonicalJSON(t, got.Request.CanonicalRequest), mustCanonicalJSON(t, recoverable.CanonicalRequest))
	}
}

func TestBeginRequestChecksCanonicalPayloadOnReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	if _, err := db.BeginRequest(ctx, input); err != nil {
		t.Fatal(err)
	}

	replayed, err := db.BeginRequest(ctx, input)
	if err != nil || !replayed.Replay {
		t.Fatalf("same canonical request replay = %+v, %v", replayed, err)
	}
	changed := input
	changed.CanonicalRequest.Messages = append([]canonical.Message(nil), input.CanonicalRequest.Messages...)
	changed.CanonicalRequest.Messages[0].Blocks = append(
		[]canonical.Block(nil), input.CanonicalRequest.Messages[0].Blocks...,
	)
	changed.CanonicalRequest.Messages[0].Blocks[0].Text = "A different request"
	if _, err := db.BeginRequest(ctx, changed); !errors.Is(err, storeapi.ErrIdempotencyConflict) {
		t.Fatalf("same digest with different canonical payload error = %v", err)
	}

	lookup, err := db.LookupRequest(ctx, storeapi.RequestKey{
		CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey,
	}, input.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	if string(mustCanonicalJSON(t, lookup.Request.CanonicalRequest)) != string(mustCanonicalJSON(t, input.CanonicalRequest)) {
		t.Fatalf("LookupRequest() canonical payload = %+v", lookup.Request.CanonicalRequest)
	}

	invalid := requestInput()
	invalid.TaskID = "task-invalid"
	invalid.IdempotencyKey = "request-invalid"
	invalid.CanonicalRequest = canonical.Request{}
	if _, err := db.BeginRequest(ctx, invalid); err == nil {
		t.Fatal("BeginRequest() accepted an invalid canonical request")
	}
}

func TestRawRecoveryQuarantineMakesCorruptRecordsFinite(t *testing.T) {
	tests := []struct {
		name      string
		corrupt   func(*testing.T, *Store, storeapi.BeginRequestInput)
		wantState completion.State
	}{
		{
			name: "missing canonical payload",
			corrupt: func(t *testing.T, db *Store, input storeapi.BeginRequestInput) {
				t.Helper()
				if _, err := db.db.ExecContext(context.Background(), `
					UPDATE completion_requests SET canonical_request = X''
					WHERE caller_id = ? AND idempotency_key = ?`,
					input.CallerID, input.IdempotencyKey,
				); err != nil {
					t.Fatal(err)
				}
			},
			wantState: completion.StateAdmitted,
		},
		{
			name: "invalid canonical JSON",
			corrupt: func(t *testing.T, db *Store, input storeapi.BeginRequestInput) {
				t.Helper()
				if _, err := db.db.ExecContext(context.Background(), `
					UPDATE completion_requests SET canonical_request = '{'
					WHERE caller_id = ? AND idempotency_key = ?`,
					input.CallerID, input.IdempotencyKey,
				); err != nil {
					t.Fatal(err)
				}
			},
			wantState: completion.StateAdmitted,
		},
		{
			name: "invalid task state",
			corrupt: func(t *testing.T, db *Store, input storeapi.BeginRequestInput) {
				t.Helper()
				if _, err := db.db.ExecContext(context.Background(), `
					UPDATE completion_tasks SET state = 'impossible'
					WHERE caller_id = ? AND task_id = ?`, input.CallerID, input.TaskID,
				); err != nil {
					t.Fatal(err)
				}
			},
			wantState: completion.StateFailed,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := openTestStore(t)
			input := requestInput()
			if _, err := db.BeginRequest(ctx, input); err != nil {
				t.Fatal(err)
			}
			test.corrupt(t, db, input)

			snapshot, err := db.ListRecoverableRequests(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Requests) != 0 || len(snapshot.Issues) != 1 {
				t.Fatalf("corrupt recovery snapshot = %+v", snapshot)
			}
			issue := snapshot.Issues[0]
			if issue.RequestKey != (storeapi.RequestKey{
				CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey,
			}) || issue.Dialect != input.Dialect || issue.ResponseStatus != 0 ||
				!errors.Is(issue.Err, storeapi.ErrUnrecoverableRequest) {
				t.Fatalf("recovery issue = %+v", issue)
			}
			failure := storeapi.ResponseDecision{
				StatusCode: 500, ContentType: "application/json",
				Body: []byte(`{"error":{"code":"recovery_failed"}}`),
			}
			quarantine := storeapi.RecoveryQuarantine{
				RequestKey: issue.RequestKey, Failure: failure,
			}
			if err := db.QuarantineRecoveryRequest(ctx, quarantine); err != nil {
				t.Fatal(err)
			}
			// The raw transition is idempotent and cannot duplicate terminal data.
			if err := db.QuarantineRecoveryRequest(ctx, quarantine); err != nil {
				t.Fatalf("idempotent quarantine: %v", err)
			}

			lookup, err := db.LookupRequest(ctx, issue.RequestKey, input.RequestDigest)
			if err != nil {
				t.Fatal(err)
			}
			if !lookup.Request.RecoveryQuarantined || !lookup.Request.ResponseComplete ||
				lookup.Request.Response.StatusCode != 500 ||
				!bytes.Equal(lookup.Request.Response.Body, failure.Body) ||
				lookup.Task.State != test.wantState {
				t.Fatalf("durable raw quarantine = %+v", lookup)
			}
			if _, err := db.LookupRequest(ctx, issue.RequestKey, "different-digest"); !errors.Is(err, storeapi.ErrIdempotencyConflict) {
				t.Fatalf("quarantined digest conflict = %v", err)
			}
			replayed, err := db.BeginRequest(ctx, input)
			if err != nil || !replayed.Replay || !replayed.Request.RecoveryQuarantined {
				t.Fatalf("same request quarantine replay = %+v, %v", replayed, err)
			}
			snapshot, err = db.ListRecoverableRequests(ctx)
			if err != nil || len(snapshot.Requests) != 0 || len(snapshot.Issues) != 0 {
				t.Fatalf("quarantine re-entered recovery = %+v, %v", snapshot, err)
			}
		})
	}
}

func TestRawRecoveryQuarantinePreservesCommitted200AndAppendsTerminal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	input := requestInput()
	created, err := db.BeginRequest(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AppendResponseEvent(
		ctx, created.Request.RequestKey, "stream",
		[]byte(`{"response_id":"msg_existing","model":"human","created_at_unix":7}`),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AppendResponseEvent(
		ctx, created.Request.RequestKey, "wire", []byte("event: message_start\ndata: {}\n\n"),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginResponse(ctx, created.Request.RequestKey); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `
		UPDATE completion_requests SET canonical_request = X''
		WHERE caller_id = ? AND idempotency_key = ?`, input.CallerID, input.IdempotencyKey,
	); err != nil {
		t.Fatal(err)
	}

	snapshot, err := db.ListRecoverableRequests(ctx)
	if err != nil || len(snapshot.Issues) != 1 {
		t.Fatalf("committed corrupt snapshot = %+v, %v", snapshot, err)
	}
	issue := snapshot.Issues[0]
	if issue.ResponseStatus != 200 || !bytes.Contains(issue.StreamMetadata, []byte("msg_existing")) {
		t.Fatalf("committed recovery issue = %+v", issue)
	}
	terminal := []byte("event: error\ndata: {\"error\":{\"code\":\"recovery_failed\"}}\n\n")
	quarantine := storeapi.RecoveryQuarantine{
		RequestKey: issue.RequestKey,
		Failure: storeapi.ResponseDecision{
			StatusCode: 500, ContentType: "application/json", Body: []byte(`{"error":{}}`),
		},
		StreamTerminal: terminal,
	}
	if err := db.QuarantineRecoveryRequest(ctx, quarantine); err != nil {
		t.Fatal(err)
	}
	if err := db.QuarantineRecoveryRequest(ctx, quarantine); err != nil {
		t.Fatal(err)
	}
	lookup, err := db.LookupRequest(ctx, issue.RequestKey, input.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	if !lookup.Request.RecoveryQuarantined || !lookup.Request.ResponseComplete ||
		lookup.Request.Response.StatusCode != 200 || lookup.Task.State != completion.StateFailed {
		t.Fatalf("committed 200 quarantine = %+v", lookup)
	}
	read, err := db.ReadResponse(ctx, issue.RequestKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Events) != 3 || !bytes.Equal(read.Events[2].Data, terminal) {
		t.Fatalf("committed 200 terminal events = %+v", read.Events)
	}
}

func mustCanonicalJSON(t *testing.T, request canonical.Request) []byte {
	t.Helper()
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
