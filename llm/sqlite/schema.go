package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

const (
	schemaVersion     = 1
	schemaFingerprint = "human-llm-store-v1-20260719b"
)

// ErrUnsupportedSchema rejects migrations and accidental database sharing.
// This pre-release adapter accepts only an empty database or its exact schema.
var ErrUnsupportedSchema = errors.New("unsupported HumanLLM SQLite schema; use a dedicated database")

const schema = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS human_schema (
  component TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  fingerprint TEXT NOT NULL
);
INSERT INTO human_schema (component, version, fingerprint)
VALUES ('llm-store', 1, 'human-llm-store-v1-20260719b')
ON CONFLICT(component) DO NOTHING;

CREATE TABLE IF NOT EXISTS llm_tasks (
  caller_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  workspace_key TEXT NOT NULL,
  capability_tier TEXT NOT NULL,
  codec BLOB NOT NULL,
  harness_id TEXT NOT NULL,
  harness_version TEXT NOT NULL,
  harness_session_id TEXT NOT NULL,
  state TEXT NOT NULL,
  revision INTEGER NOT NULL CHECK(revision > 0),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  record BLOB NOT NULL,
  PRIMARY KEY (caller_id, task_id),
  CHECK(updated_at >= created_at)
);
CREATE UNIQUE INDEX IF NOT EXISTS llm_tasks_open_affinity_idx
  ON llm_tasks(
    caller_id, workspace_key, harness_id, harness_version, harness_session_id
  )
  WHERE state NOT IN ('completed', 'canceled', 'rejected', 'expired', 'failed');

CREATE TABLE IF NOT EXISTS llm_requests (
  caller_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  task_caller_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  response_id TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  codec BLOB NOT NULL,
  mode TEXT NOT NULL,
  canonical_payload BLOB NOT NULL,
  decision_status INTEGER NOT NULL,
  decision_content_type TEXT NOT NULL,
  decision_retry_after TEXT NOT NULL,
  decision_body BLOB NOT NULL,
  decision_body_is_nil INTEGER NOT NULL CHECK(decision_body_is_nil IN (0, 1)),
  response_complete INTEGER NOT NULL CHECK(response_complete IN (0, 1)),
  recovery_quarantined INTEGER NOT NULL CHECK(recovery_quarantined IN (0, 1)),
  last_event_sequence INTEGER NOT NULL CHECK(last_event_sequence >= 0),
  revision INTEGER NOT NULL CHECK(revision > 0),
  created_at INTEGER NOT NULL,
  completed_at INTEGER,
  payload_pruned_at INTEGER,
  record BLOB NOT NULL,
  PRIMARY KEY (caller_id, idempotency_key),
  UNIQUE (caller_id, request_id),
  UNIQUE (caller_id, response_id),
  FOREIGN KEY (task_caller_id, task_id)
    REFERENCES llm_tasks(caller_id, task_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS llm_requests_active_task_idx
  ON llm_requests(task_caller_id, task_id)
  WHERE response_complete = 0;
CREATE INDEX IF NOT EXISTS llm_requests_recovery_idx
  ON llm_requests(recovery_quarantined, response_complete, created_at, caller_id, idempotency_key);
CREATE INDEX IF NOT EXISTS llm_requests_retention_idx
  ON llm_requests(response_complete, payload_pruned_at, completed_at, created_at, caller_id, idempotency_key);

CREATE TABLE IF NOT EXISTS llm_response_events (
  caller_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence > 0),
  kind TEXT NOT NULL CHECK(kind IN ('checkpoint', 'wire')),
  worker_event_id TEXT NOT NULL,
  worker_event_digest TEXT NOT NULL,
  data BLOB NOT NULL,
  data_is_nil INTEGER NOT NULL CHECK(data_is_nil IN (0, 1)),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (caller_id, idempotency_key, sequence),
  FOREIGN KEY (caller_id, idempotency_key)
    REFERENCES llm_requests(caller_id, idempotency_key)
    ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS llm_response_events_worker_idx
  ON llm_response_events(caller_id, idempotency_key, worker_event_id, sequence)
  WHERE worker_event_id <> '';

CREATE TABLE IF NOT EXISTS llm_worker_receipts (
  caller_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  event_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (caller_id, idempotency_key, event_id),
  FOREIGN KEY (caller_id, idempotency_key)
    REFERENCES llm_requests(caller_id, idempotency_key)
    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS llm_tool_executions (
  task_caller_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  tool_call_id TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('pending', 'completed')),
  result BLOB NOT NULL,
  result_is_nil INTEGER NOT NULL CHECK(result_is_nil IN (0, 1)),
  revision INTEGER NOT NULL CHECK(revision > 0),
  created_at INTEGER NOT NULL,
  record BLOB NOT NULL,
  PRIMARY KEY (task_caller_id, task_id, tool_call_id),
  FOREIGN KEY (task_caller_id, task_id)
    REFERENCES llm_tasks(caller_id, task_id)
    ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS llm_tool_executions_scan_idx
  ON llm_tool_executions(task_caller_id, task_id, state, tool_call_id);
`

func requireCurrentOrEmptySchema(ctx context.Context, database *sql.DB) error {
	rows, err := database.QueryContext(ctx, `
		SELECT name FROM sqlite_schema
		WHERE type IN ('table', 'index', 'view', 'trigger')
		  AND name NOT LIKE 'sqlite_%'
		ORDER BY name`)
	if err != nil {
		return fmt.Errorf("inspect HumanLLM SQLite schema: %w", err)
	}
	var objects []string
	for rows.Next() {
		var object string
		if err := rows.Scan(&object); err != nil {
			_ = rows.Close()
			return fmt.Errorf("inspect HumanLLM SQLite schema object: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("inspect HumanLLM SQLite schema objects: %w", err)
	}
	if len(objects) == 0 {
		return nil
	}
	expected := []string{
		"human_schema",
		"llm_requests",
		"llm_requests_active_task_idx",
		"llm_requests_recovery_idx",
		"llm_requests_retention_idx",
		"llm_response_events",
		"llm_response_events_worker_idx",
		"llm_tasks",
		"llm_tasks_open_affinity_idx",
		"llm_tool_executions",
		"llm_tool_executions_scan_idx",
		"llm_worker_receipts",
	}
	sort.Strings(expected)
	if len(objects) != len(expected) {
		return fmt.Errorf("%w: schema objects %v, want %v", ErrUnsupportedSchema, objects, expected)
	}
	for index := range expected {
		if objects[index] != expected[index] {
			return fmt.Errorf("%w: schema objects %v, want %v", ErrUnsupportedSchema, objects, expected)
		}
	}
	var version int
	var fingerprint string
	if err := database.QueryRowContext(ctx, `
		SELECT version, fingerprint FROM human_schema
		WHERE component = 'llm-store'`).Scan(&version, &fingerprint); err != nil {
		return fmt.Errorf("%w: missing llm-store schema marker", ErrUnsupportedSchema)
	}
	if version != schemaVersion || fingerprint != schemaFingerprint {
		return fmt.Errorf(
			"%w: version %d (%q), want %d (%q)",
			ErrUnsupportedSchema, version, fingerprint,
			schemaVersion, schemaFingerprint,
		)
	}
	return nil
}
