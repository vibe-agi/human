package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const (
	agentSchemaVersion     = 2
	agentSchemaFingerprint = "human-agent-v2-20260719a"
)

var errUnsupportedSchema = errors.New("unsupported HumanAgent sqlite schema; use a dedicated database")

const agentSchema = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS human_schema (
  component TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  fingerprint TEXT NOT NULL
);
INSERT INTO human_schema (component, version, fingerprint)
VALUES ('agent', 2, 'human-agent-v2-20260719a')
ON CONFLICT(component) DO NOTHING;

CREATE TABLE IF NOT EXISTS agent_tasks (
  authority_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  context_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN (
    'submitted', 'working', 'input_required',
    'completed', 'canceled', 'rejected', 'failed'
  )),
  revision INTEGER NOT NULL CHECK(revision > 0),
  message_count INTEGER NOT NULL CHECK(message_count > 0),
  event_count INTEGER NOT NULL CHECK(event_count > 0),
  lease_owner TEXT NOT NULL DEFAULT '',
  lease_fence INTEGER NOT NULL DEFAULT 0 CHECK(lease_fence >= 0),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (authority_id, workspace_id, task_id),
  UNIQUE (authority_id, task_id),
  CHECK(revision = event_count),
  CHECK(lease_owner = '' OR lease_fence > 0),
  CHECK(
    state NOT IN ('completed', 'canceled', 'rejected', 'failed')
    OR lease_owner = ''
  ),
  CHECK(updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS agent_lease_grants (
  authority_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  fence INTEGER NOT NULL CHECK(fence > 0),
  worker_id TEXT NOT NULL CHECK(worker_id <> ''),
  granted_at INTEGER NOT NULL,
  PRIMARY KEY (authority_id, workspace_id, task_id, fence),
  FOREIGN KEY (authority_id, workspace_id, task_id)
    REFERENCES agent_tasks(authority_id, workspace_id, task_id)
    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS agent_messages (
  authority_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence > 0),
  author TEXT NOT NULL CHECK(author IN ('caller', 'agent')),
  parts BLOB NOT NULL,
  digest TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (authority_id, message_id),
  UNIQUE (authority_id, workspace_id, task_id, sequence),
  UNIQUE (authority_id, workspace_id, task_id, message_id),
  FOREIGN KEY (authority_id, workspace_id, task_id)
    REFERENCES agent_tasks(authority_id, workspace_id, task_id)
    ON DELETE CASCADE,
  CHECK(
    (sequence % 2 = 1 AND author = 'caller') OR
    (sequence % 2 = 0 AND author = 'agent')
  )
);

CREATE TABLE IF NOT EXISTS agent_workspace_heads (
  authority_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  confirmed_revision TEXT NOT NULL CHECK(confirmed_revision <> ''),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (authority_id, workspace_id),
  CHECK(updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS agent_artifacts (
  authority_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('frozen', 'published', 'discarded')),
  base_revision TEXT NOT NULL CHECK(base_revision <> ''),
  result_revision TEXT NOT NULL CHECK(result_revision <> ''),
  artifact_digest TEXT NOT NULL CHECK(artifact_digest <> ''),
  payload_digest TEXT NOT NULL CHECK(payload_digest <> ''),
  media_type TEXT NOT NULL CHECK(media_type <> ''),
  payload BLOB NOT NULL,
  payload_size INTEGER NOT NULL CHECK(payload_size > 0 AND payload_size <= 67108864),
  frozen_at INTEGER NOT NULL,
  published_at INTEGER,
  discarded_at INTEGER,
  PRIMARY KEY (authority_id, workspace_id, artifact_id),
  UNIQUE (authority_id, artifact_id),
  UNIQUE (authority_id, workspace_id, task_id),
  UNIQUE (authority_id, workspace_id, task_id, artifact_id),
  UNIQUE (
    authority_id, workspace_id, artifact_id,
    artifact_digest, base_revision, result_revision
  ),
  FOREIGN KEY (authority_id, workspace_id, task_id)
    REFERENCES agent_tasks(authority_id, workspace_id, task_id)
    ON DELETE CASCADE,
  CHECK(base_revision <> result_revision),
  CHECK(
    (state = 'frozen' AND published_at IS NULL AND discarded_at IS NULL) OR
    (state = 'published' AND published_at IS NOT NULL AND published_at >= frozen_at AND discarded_at IS NULL) OR
    (state = 'discarded' AND published_at IS NULL AND discarded_at IS NOT NULL AND discarded_at >= frozen_at)
  )
);

CREATE TABLE IF NOT EXISTS agent_submissions (
  authority_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  submission_id TEXT NOT NULL,
  final_message_id TEXT NOT NULL,
  artifact_id TEXT,
  published_at INTEGER NOT NULL,
  PRIMARY KEY (authority_id, workspace_id, task_id),
  UNIQUE (authority_id, submission_id),
  FOREIGN KEY (authority_id, workspace_id, task_id)
    REFERENCES agent_tasks(authority_id, workspace_id, task_id)
    ON DELETE CASCADE,
  FOREIGN KEY (authority_id, workspace_id, task_id, final_message_id)
    REFERENCES agent_messages(authority_id, workspace_id, task_id, message_id),
  FOREIGN KEY (authority_id, workspace_id, task_id, artifact_id)
    REFERENCES agent_artifacts(authority_id, workspace_id, task_id, artifact_id)
);

CREATE TABLE IF NOT EXISTS agent_apply_receipts (
  authority_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  receipt_id TEXT NOT NULL,
  decision TEXT NOT NULL CHECK(decision IN (
    'success', 'conflict', 'rejected', 'indeterminate'
  )),
  artifact_digest TEXT NOT NULL,
  base_revision TEXT NOT NULL,
  result_revision TEXT NOT NULL,
  observed_revision TEXT NOT NULL DEFAULT '',
  code TEXT NOT NULL DEFAULT '',
  message TEXT NOT NULL DEFAULT '',
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (authority_id, workspace_id, artifact_id),
  UNIQUE (authority_id, receipt_id),
  FOREIGN KEY (
    authority_id, workspace_id, artifact_id,
    artifact_digest, base_revision, result_revision
  ) REFERENCES agent_artifacts(
    authority_id, workspace_id, artifact_id,
    artifact_digest, base_revision, result_revision
  )
);

CREATE TABLE IF NOT EXISTS agent_commands (
  authority_id TEXT NOT NULL,
  command_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  digest TEXT NOT NULL,
  result_kind TEXT NOT NULL,
  result BLOB NOT NULL,
  result_digest TEXT NOT NULL CHECK(length(result_digest) = 64),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (authority_id, command_id)
);

CREATE TABLE IF NOT EXISTS agent_events (
  authority_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence > 0),
  kind TEXT NOT NULL CHECK(kind IN (
    'task_submitted', 'task_accepted', 'task_rejected',
    'input_required', 'caller_replied', 'task_canceled',
    'task_failed', 'task_completed', 'artifact_frozen'
  )),
  state TEXT NOT NULL,
  revision INTEGER NOT NULL CHECK(revision > 0),
  message_id TEXT NOT NULL DEFAULT '',
  submission_id TEXT NOT NULL DEFAULT '',
  artifact_id TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  PRIMARY KEY (authority_id, workspace_id, task_id, sequence),
  CHECK(sequence = revision),
  FOREIGN KEY (authority_id, workspace_id, task_id)
    REFERENCES agent_tasks(authority_id, workspace_id, task_id)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS agent_tasks_context_idx
  ON agent_tasks(authority_id, context_id, created_at, task_id);
CREATE INDEX IF NOT EXISTS agent_tasks_workspace_idx
  ON agent_tasks(authority_id, workspace_id, created_at, task_id);
CREATE INDEX IF NOT EXISTS agent_tasks_claimable_idx
  ON agent_tasks(authority_id, state, lease_owner, created_at, workspace_id, task_id);
CREATE INDEX IF NOT EXISTS agent_tasks_lease_owner_idx
  ON agent_tasks(authority_id, lease_owner, created_at, workspace_id, task_id)
  WHERE lease_owner <> '';
CREATE INDEX IF NOT EXISTS agent_events_task_idx
  ON agent_events(authority_id, workspace_id, task_id, sequence);
CREATE INDEX IF NOT EXISTS agent_artifacts_task_idx
  ON agent_artifacts(authority_id, workspace_id, task_id);
`

func requireCurrentOrEmptySchema(ctx context.Context, database *sql.DB) error {
	var tableCount int
	if err := database.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
		return fmt.Errorf("inspect HumanAgent sqlite schema: %w", err)
	}
	if tableCount == 0 {
		return nil
	}
	var version int
	var fingerprint string
	if err := database.QueryRowContext(ctx, `
		SELECT version, fingerprint
		FROM human_schema
		WHERE component = 'agent'`).Scan(&version, &fingerprint); err != nil {
		return fmt.Errorf("%w: missing agent schema marker", errUnsupportedSchema)
	}
	if version != agentSchemaVersion || fingerprint != agentSchemaFingerprint {
		return fmt.Errorf(
			"%w: version %d (%q), want %d (%q)",
			errUnsupportedSchema, version, fingerprint,
			agentSchemaVersion, agentSchemaFingerprint,
		)
	}
	return nil
}
