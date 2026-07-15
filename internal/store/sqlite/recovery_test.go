package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	storeapi "github.com/vibe-agi/human/internal/store"
	_ "modernc.org/sqlite"
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
	requests, err := db.ListRecoverableRequests(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("ListRecoverableRequests() returned %d entries: %+v", len(requests), requests)
	}
	got := requests[0]
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

func TestOpenMigratesLegacyCanonicalRequestColumnExplicitly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.ExecContext(ctx, `
		CREATE TABLE completion_tasks (
		  caller_id TEXT NOT NULL,
		  task_id TEXT NOT NULL,
		  workspace_key TEXT NOT NULL,
		  capability_tier TEXT NOT NULL,
		  dialect TEXT NOT NULL,
		  harness_id TEXT NOT NULL,
		  harness_version TEXT NOT NULL,
		  workspace_root TEXT NOT NULL,
		  state TEXT NOT NULL,
		  lease_owner TEXT NOT NULL DEFAULT '',
		  revision INTEGER NOT NULL DEFAULT 1,
		  created_at INTEGER NOT NULL,
		  updated_at INTEGER NOT NULL,
		  PRIMARY KEY (caller_id, task_id)
		);
		CREATE TABLE completion_requests (
		  caller_id TEXT NOT NULL,
		  idempotency_key TEXT NOT NULL,
		  task_id TEXT NOT NULL,
		  request_digest TEXT NOT NULL,
		  response_complete INTEGER NOT NULL DEFAULT 0,
		  created_at INTEGER NOT NULL,
		  completed_at INTEGER,
		  PRIMARY KEY (caller_id, idempotency_key),
		  FOREIGN KEY (caller_id, task_id)
		    REFERENCES completion_tasks(caller_id, task_id)
		);
		CREATE TABLE completion_response_events (
		  caller_id TEXT NOT NULL,
		  idempotency_key TEXT NOT NULL,
		  sequence INTEGER NOT NULL,
		  kind TEXT NOT NULL,
		  data BLOB NOT NULL,
		  created_at INTEGER NOT NULL,
		  PRIMARY KEY (caller_id, idempotency_key, sequence),
		  FOREIGN KEY (caller_id, idempotency_key)
		    REFERENCES completion_requests(caller_id, idempotency_key)
		);
		INSERT INTO completion_tasks (
		  caller_id, task_id, workspace_key, capability_tier, dialect,
		  harness_id, harness_version, workspace_root, state, created_at, updated_at
		) VALUES ('legacy-caller', 'legacy-task', 'legacy-workspace', 'remote_tools',
		          'anthropic_messages', 'claude-code', '0.1', '/legacy', 'admitted', 1, 1);
		INSERT INTO completion_requests (
		  caller_id, idempotency_key, task_id, request_digest, created_at
		) VALUES ('legacy-caller', 'legacy-request', 'legacy-task', 'legacy-digest', 1);
		INSERT INTO completion_tasks (
		  caller_id, task_id, workspace_key, capability_tier, dialect,
		  harness_id, harness_version, workspace_root, state, created_at, updated_at
		) VALUES
		  ('legacy-caller', 'legacy-stream-task', '', 'chat', 'openai_chat', '', '', '', 'completed', 2, 2),
		  ('legacy-caller', 'legacy-failed-task', '', 'chat', 'openai_chat', '', '', '', 'failed', 3, 3);
		INSERT INTO completion_requests (
		  caller_id, idempotency_key, task_id, request_digest,
		  response_complete, created_at, completed_at
		) VALUES
		  ('legacy-caller', 'legacy-stream', 'legacy-stream-task', 'stream-digest', 1, 2, 2),
		  ('legacy-caller', 'legacy-failed', 'legacy-failed-task', 'failed-digest', 1, 3, 3);
		INSERT INTO completion_response_events (
		  caller_id, idempotency_key, sequence, kind, data, created_at
		) VALUES ('legacy-caller', 'legacy-stream', 1, 'applied', X'7B7D', 2);
	`)
	if err != nil {
		legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ListRecoverableRequests(ctx); !errors.Is(err, storeapi.ErrUnrecoverableRequest) ||
		!strings.Contains(err.Error(), "legacy-caller/legacy-request") {
		t.Fatalf("legacy incomplete request recovery error = %v", err)
	}
	var streamStatus int
	var streamContentType string
	if err := db.db.QueryRowContext(ctx, `
		SELECT response_status, response_content_type
		FROM completion_requests
		WHERE caller_id = 'legacy-caller' AND idempotency_key = 'legacy-stream'`,
	).Scan(&streamStatus, &streamContentType); err != nil {
		t.Fatal(err)
	}
	if streamStatus != 200 || streamContentType != "text/event-stream" {
		t.Fatalf("legacy stream decision = %d, %q", streamStatus, streamContentType)
	}
	var failedStatus int
	var failedBody []byte
	if err := db.db.QueryRowContext(ctx, `
		SELECT response_status, response_body
		FROM completion_requests
		WHERE caller_id = 'legacy-caller' AND idempotency_key = 'legacy-failed'`,
	).Scan(&failedStatus, &failedBody); err != nil {
		t.Fatal(err)
	}
	if failedStatus != 500 || !bytes.Contains(failedBody, []byte("legacy_response_unrecoverable")) {
		t.Fatalf("legacy ambiguous decision = %d, %q", failedStatus, failedBody)
	}

	input := requestInput()
	input.TaskID = "new-task"
	input.IdempotencyKey = "new-request"
	created, err := db.BeginRequest(ctx, input)
	if err != nil {
		t.Fatalf("BeginRequest() after legacy migration: %v", err)
	}
	if string(mustCanonicalJSON(t, created.Request.CanonicalRequest)) != string(mustCanonicalJSON(t, input.CanonicalRequest)) {
		t.Fatalf("new canonical payload after migration = %+v", created.Request.CanonicalRequest)
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
