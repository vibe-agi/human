// Package sqlite implements the shared Store contracts using pure-Go SQLite.
package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/sqlitefile"
	storeapi "github.com/vibe-agi/human/internal/store"
	_ "modernc.org/sqlite"
)

const (
	httpStatusUndecided = 0
	httpStatusOK        = 200
	schemaVersion       = 1
	schemaFingerprint   = "human-gateway-v1-20260717"
)

var errUnsupportedSchema = errors.New("unsupported sqlite schema; recreate database")

const schema = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS human_schema (
  component TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  fingerprint TEXT NOT NULL
);
INSERT INTO human_schema (component, version, fingerprint)
VALUES ('gateway', 1, 'human-gateway-v1-20260717')
ON CONFLICT(component) DO NOTHING;

CREATE TABLE IF NOT EXISTS completion_tasks (
  caller_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  workspace_key TEXT NOT NULL,
  capability_tier TEXT NOT NULL,
  dialect TEXT NOT NULL,
  harness_id TEXT NOT NULL,
  harness_version TEXT NOT NULL,
  harness_session_id TEXT NOT NULL DEFAULT '',
  workspace_root TEXT NOT NULL,
  exec_allowed INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL,
  lease_owner TEXT NOT NULL DEFAULT '',
  retry_request_digest TEXT NOT NULL DEFAULT '',
  revision INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (caller_id, task_id)
);

CREATE TABLE IF NOT EXISTS completion_requests (
  caller_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  task_id TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  canonical_request BLOB NOT NULL,
  response_status INTEGER NOT NULL DEFAULT 0,
  response_content_type TEXT NOT NULL DEFAULT '',
  response_retry_after TEXT NOT NULL DEFAULT '',
  response_body BLOB NOT NULL DEFAULT X'',
  response_complete INTEGER NOT NULL DEFAULT 0,
  recovery_quarantined INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  completed_at INTEGER,
  payload_pruned_at INTEGER,
  PRIMARY KEY (caller_id, idempotency_key),
  FOREIGN KEY (caller_id, task_id)
    REFERENCES completion_tasks(caller_id, task_id)
);

CREATE TABLE IF NOT EXISTS completion_response_events (
  caller_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  kind TEXT NOT NULL,
  event_id TEXT NOT NULL DEFAULT '',
  event_digest TEXT NOT NULL DEFAULT '',
  data BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (caller_id, idempotency_key, sequence),
  FOREIGN KEY (caller_id, idempotency_key)
    REFERENCES completion_requests(caller_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS completion_worker_event_receipts (
  caller_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  event_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  event_digest TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (caller_id, idempotency_key, event_id),
  FOREIGN KEY (caller_id, idempotency_key)
    REFERENCES completion_requests(caller_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS completion_tool_executions (
  caller_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  tool_call_id TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending', 'completed')),
  result BLOB,
  is_error INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  completed_at INTEGER,
  PRIMARY KEY (caller_id, task_id, tool_call_id),
  FOREIGN KEY (caller_id, task_id)
    REFERENCES completion_tasks(caller_id, task_id)
);

CREATE TABLE IF NOT EXISTS api_tokens (
  key_id TEXT PRIMARY KEY,
  principal_type TEXT NOT NULL CHECK(principal_type IN ('caller', 'worker')),
  subject_id TEXT NOT NULL,
  token_hash BLOB NOT NULL UNIQUE,
  created_at INTEGER NOT NULL,
  revoked_at INTEGER
);

CREATE TABLE IF NOT EXISTS audit_metadata (
  id TEXT PRIMARY KEY,
  caller_id TEXT NOT NULL,
  workspace_key TEXT NOT NULL,
  task_id TEXT NOT NULL,
  dialect TEXT NOT NULL,
  key_id TEXT NOT NULL,
  pending_ms INTEGER NOT NULL CHECK(pending_ms >= 0),
  gen_ms INTEGER NOT NULL CHECK(gen_ms >= 0),
  error TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_payloads (
  audit_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  data BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  PRIMARY KEY (audit_id, kind),
  FOREIGN KEY (audit_id) REFERENCES audit_metadata(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS completion_tasks_state_idx
  ON completion_tasks(state, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS completion_tasks_open_affinity_idx
  ON completion_tasks(caller_id, workspace_key, harness_id, harness_version, harness_session_id)
  WHERE harness_session_id <> ''
    AND state NOT IN ('completed', 'canceled', 'rejected', 'expired', 'failed');
CREATE INDEX IF NOT EXISTS completion_requests_task_idx
  ON completion_requests(caller_id, task_id, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS completion_requests_one_active_task_idx
  ON completion_requests(caller_id, task_id)
  WHERE response_complete = 0;
CREATE INDEX IF NOT EXISTS completion_requests_replay_retention_idx
  ON completion_requests(completed_at)
  WHERE response_complete = 1 AND payload_pruned_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS completion_response_worker_event_idx
  ON completion_response_events(caller_id, idempotency_key, kind, event_id)
  WHERE event_id <> '';
CREATE INDEX IF NOT EXISTS completion_response_worker_event_lookup_idx
  ON completion_response_events(caller_id, idempotency_key, event_id, kind)
  WHERE event_id <> '';
CREATE INDEX IF NOT EXISTS completion_worker_receipts_created_idx
  ON completion_worker_event_receipts(created_at);
CREATE INDEX IF NOT EXISTS audit_metadata_caller_created_idx
  ON audit_metadata(caller_id, created_at);
CREATE INDEX IF NOT EXISTS audit_metadata_workspace_created_idx
  ON audit_metadata(workspace_key, created_at);
CREATE INDEX IF NOT EXISTS audit_payloads_expiry_idx
  ON audit_payloads(expires_at);
`

type Store struct {
	db  *sql.DB
	now func() time.Time
}

var _ storeapi.CompletionStore = (*Store)(nil)
var _ storeapi.TokenStore = (*Store)(nil)
var _ storeapi.AuditStore = (*Store)(nil)
var _ storeapi.WorkerEventReceiptStore = (*Store)(nil)

func Open(ctx context.Context, dsn string) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("sqlite dsn is required")
	}
	location, err := sqlitefile.PreparePrivate(dsn, "gateway SQLite database")
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Completion mode intentionally starts as a single-instance SQLite deployment.
	// Serializing through one connection also gives deterministic transaction
	// semantics for request admission and tool execution ledgers.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure sqlite busy timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA secure_delete = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure sqlite secure delete: %w", err)
	}
	var secureDelete int
	if err := db.QueryRowContext(ctx, "PRAGMA secure_delete").Scan(&secureDelete); err != nil {
		db.Close()
		return nil, fmt.Errorf("verify sqlite secure delete: %w", err)
	}
	if secureDelete != 1 {
		db.Close()
		return nil, errors.New("sqlite secure delete could not be enabled")
	}
	if err := requireCurrentOrEmptySchema(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize sqlite schema: %w", err)
	}
	return &Store{db: db, now: time.Now}, nil
}

func requireCurrentOrEmptySchema(ctx context.Context, db *sql.DB) error {
	var tableCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
		return fmt.Errorf("inspect sqlite schema: %w", err)
	}
	if tableCount == 0 {
		return nil
	}
	var version int
	var fingerprint string
	if err := db.QueryRowContext(ctx, `
		SELECT version, fingerprint
		FROM human_schema
		WHERE component = 'gateway'`).Scan(&version, &fingerprint); err != nil {
		return fmt.Errorf("%w: missing gateway schema marker", errUnsupportedSchema)
	}
	if version != schemaVersion || fingerprint != schemaFingerprint {
		return fmt.Errorf(
			"%w: gateway schema version %d (%q), want %d (%q)",
			errUnsupportedSchema, version, fingerprint, schemaVersion, schemaFingerprint,
		)
	}
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// Ping proves that the gateway database can query its current schema now. It
// is deliberately stronger than merely checking that sql.DB still owns a
// driver handle (or evaluating SELECT 1 without touching the database file):
// readiness must fail when SQLite becomes unavailable or damaged at runtime.
func (s *Store) Ping(ctx context.Context) error {
	var version int
	var fingerprint string
	if err := s.db.QueryRowContext(ctx, `
		SELECT version, fingerprint
		FROM human_schema
		WHERE component = 'gateway'`).Scan(&version, &fingerprint); err != nil {
		return fmt.Errorf("query sqlite readiness probe: %w", err)
	}
	if version != schemaVersion || fingerprint != schemaFingerprint {
		return fmt.Errorf(
			"query sqlite readiness probe: schema version %d (%q), want %d (%q)",
			version, fingerprint, schemaVersion, schemaFingerprint,
		)
	}
	return nil
}

func (s *Store) LookupRequest(
	ctx context.Context,
	key storeapi.RequestKey,
	requestDigest string,
) (storeapi.BeginRequestResult, error) {
	request, err := getRequest(ctx, s.db, key)
	if err != nil {
		return storeapi.BeginRequestResult{}, err
	}
	if request.RequestDigest != requestDigest {
		return storeapi.BeginRequestResult{}, storeapi.ErrIdempotencyConflict
	}
	if request.PayloadPrunedAt != nil {
		return storeapi.BeginRequestResult{}, storeapi.ErrReplayPayloadExpired
	}
	task, err := getTask(ctx, s.db, storeapi.TaskKey{CallerID: request.CallerID, TaskID: request.TaskID})
	if err != nil {
		return storeapi.BeginRequestResult{}, err
	}
	return storeapi.BeginRequestResult{Task: task, Request: request, Replay: true}, nil
}

// ListRecoverableRequests returns incomplete responses whose owning task can
// still make progress, terminal tasks with a durable response step awaiting
// publication, and completed responses whose final durable receipt was not
// committed. It reads task identity and canonical request payloads from one
// snapshot so callers can reconstruct assignments after a restart.
func (s *Store) ListRecoverableRequests(ctx context.Context) (storeapi.RecoverySnapshot, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return storeapi.RecoverySnapshot{}, fmt.Errorf("begin recovery snapshot: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT request.caller_id, request.idempotency_key,
		       task.dialect, request.response_status
		FROM completion_requests AS request
		JOIN completion_tasks AS task
		  ON task.caller_id = request.caller_id AND task.task_id = request.task_id
		WHERE request.recovery_quarantined = 0
		  AND ((
		    request.response_complete = 0
		    AND (
		      task.state NOT IN (?, ?, ?, ?, ?)
		      OR EXISTS (
		        SELECT 1
		        FROM completion_response_events AS event
		        WHERE event.caller_id = request.caller_id
		          AND event.idempotency_key = request.idempotency_key
		          AND event.kind = 'step'
		      )
		    )
		  ) OR EXISTS (
		    SELECT 1
		    FROM completion_response_events AS step
		    WHERE step.caller_id = request.caller_id
		      AND step.idempotency_key = request.idempotency_key
		      AND step.kind = 'step'
		      AND step.event_id <> ''
		      AND NOT EXISTS (
		        SELECT 1
		        FROM completion_worker_event_receipts AS receipt
		        WHERE receipt.caller_id = step.caller_id
		          AND receipt.idempotency_key = step.idempotency_key
		          AND receipt.event_id = step.event_id
		      )
		  ))
		ORDER BY request.created_at, request.caller_id, request.idempotency_key`,
		completion.StateCompleted, completion.StateCanceled, completion.StateRejected,
		completion.StateExpired, completion.StateFailed)
	if err != nil {
		return storeapi.RecoverySnapshot{}, fmt.Errorf("list recoverable request keys: %w", err)
	}
	type recoverableKey struct {
		storeapi.RequestKey
		dialect        canonical.Dialect
		responseStatus int
	}
	var keys []recoverableKey
	for rows.Next() {
		var key recoverableKey
		if err := rows.Scan(
			&key.CallerID, &key.IdempotencyKey, &key.dialect, &key.responseStatus,
		); err != nil {
			rows.Close()
			return storeapi.RecoverySnapshot{}, fmt.Errorf("scan recoverable request key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil {
		return storeapi.RecoverySnapshot{}, fmt.Errorf("close recoverable request keys: %w", err)
	}
	if err := rows.Err(); err != nil {
		return storeapi.RecoverySnapshot{}, fmt.Errorf("iterate recoverable request keys: %w", err)
	}

	snapshot := storeapi.RecoverySnapshot{
		Requests: make([]storeapi.BeginRequestResult, 0, len(keys)),
		Issues:   make([]storeapi.RecoveryIssue, 0),
	}
	for _, key := range keys {
		request, err := getRequest(ctx, tx, key.RequestKey)
		if err != nil {
			if errors.Is(err, storeapi.ErrUnrecoverableRequest) {
				metadata, metadataErr := recoveryStreamMetadata(ctx, tx, key.RequestKey)
				if metadataErr != nil {
					return storeapi.RecoverySnapshot{}, metadataErr
				}
				snapshot.Issues = append(snapshot.Issues, storeapi.RecoveryIssue{
					RequestKey:     key.RequestKey,
					Dialect:        key.dialect,
					ResponseStatus: key.responseStatus,
					StreamMetadata: metadata,
					Err:            fmt.Errorf("load recoverable request: %w", err),
				})
				continue
			}
			return storeapi.RecoverySnapshot{}, fmt.Errorf("load recoverable request: %w", err)
		}
		task, err := getTask(ctx, tx, storeapi.TaskKey{CallerID: request.CallerID, TaskID: request.TaskID})
		if err != nil {
			return storeapi.RecoverySnapshot{}, fmt.Errorf("load recoverable task: %w", err)
		}
		if !task.State.Valid() {
			metadata, metadataErr := recoveryStreamMetadata(ctx, tx, key.RequestKey)
			if metadataErr != nil {
				return storeapi.RecoverySnapshot{}, metadataErr
			}
			snapshot.Issues = append(snapshot.Issues, storeapi.RecoveryIssue{
				RequestKey:     key.RequestKey,
				Dialect:        task.Dialect,
				ResponseStatus: request.Response.StatusCode,
				StreamMetadata: metadata,
				Err: fmt.Errorf(
					"%w: %s/%s has invalid recoverable task state %q",
					storeapi.ErrUnrecoverableRequest, key.CallerID, key.IdempotencyKey, task.State,
				),
			})
			continue
		}
		snapshot.Requests = append(snapshot.Requests, storeapi.BeginRequestResult{
			Task: task, Request: request, Replay: true,
		})
	}
	if err := tx.Commit(); err != nil {
		return storeapi.RecoverySnapshot{}, fmt.Errorf("commit recovery snapshot: %w", err)
	}
	return snapshot, nil
}

func recoveryStreamMetadata(
	ctx context.Context,
	db queryer,
	key storeapi.RequestKey,
) ([]byte, error) {
	var metadata []byte
	err := db.QueryRowContext(ctx, `
		SELECT data
		FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ? AND kind = 'stream'
		ORDER BY sequence
		LIMIT 1`, key.CallerID, key.IdempotencyKey).Scan(&metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf(
			"load recovery stream metadata for %s/%s: %w",
			key.CallerID, key.IdempotencyKey, err,
		)
	}
	return bytes.Clone(metadata), nil
}

// QuarantineRecoveryRequest turns one corrupt recovery record into a durable,
// finite replay without decoding canonical_request. The request digest remains
// untouched, so the existing idempotency key still rejects a different body.
// A committed 200 is irreversible: in that case the transaction appends one
// explicit terminal SSE frame and completes the response instead of rewriting
// the HTTP decision.
func (s *Store) QuarantineRecoveryRequest(
	ctx context.Context,
	quarantine storeapi.RecoveryQuarantine,
) error {
	if strings.TrimSpace(quarantine.CallerID) == "" ||
		strings.TrimSpace(quarantine.IdempotencyKey) == "" {
		return errors.New("recovery quarantine request key is required")
	}
	if quarantine.Failure.StatusCode < 400 || quarantine.Failure.StatusCode > 599 ||
		strings.TrimSpace(quarantine.Failure.ContentType) == "" ||
		len(quarantine.Failure.Body) == 0 {
		return errors.New("recovery quarantine requires a finite HTTP failure")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin recovery quarantine transaction: %w", err)
	}
	defer tx.Rollback()

	var taskID, requestDigest, taskState string
	var responseStatus, responseComplete, alreadyQuarantined int
	err = tx.QueryRowContext(ctx, `
		SELECT request.task_id, request.request_digest,
		       request.response_status, request.response_complete,
		       request.recovery_quarantined, task.state
		FROM completion_requests AS request
		JOIN completion_tasks AS task
		  ON task.caller_id = request.caller_id AND task.task_id = request.task_id
		WHERE request.caller_id = ? AND request.idempotency_key = ?`,
		quarantine.CallerID, quarantine.IdempotencyKey,
	).Scan(
		&taskID, &requestDigest, &responseStatus, &responseComplete,
		&alreadyQuarantined, &taskState,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("load raw recovery quarantine record: %w", err)
	}
	if alreadyQuarantined != 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit recovery quarantine replay: %w", err)
		}
		return nil
	}

	now := s.now().UTC()
	// A complete response with a real decision is already finite. Mark it so
	// future lookups bypass the corrupt canonical payload, but preserve every
	// byte of its prior response.
	if responseComplete != 0 && responseStatus != httpStatusUndecided {
		if _, err := tx.ExecContext(ctx, `
			UPDATE completion_requests
			SET recovery_quarantined = 1
			WHERE caller_id = ? AND idempotency_key = ?`,
			quarantine.CallerID, quarantine.IdempotencyKey,
		); err != nil {
			return fmt.Errorf("mark completed recovery quarantine: %w", err)
		}
	} else if responseStatus == httpStatusOK {
		if len(quarantine.StreamTerminal) == 0 {
			return errors.New("committed streaming recovery quarantine requires a terminal frame")
		}
		var sequence int64
		if err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(MAX(sequence), 0) + 1
			FROM completion_response_events
			WHERE caller_id = ? AND idempotency_key = ?`,
			quarantine.CallerID, quarantine.IdempotencyKey,
		).Scan(&sequence); err != nil {
			return fmt.Errorf("allocate recovery quarantine event sequence: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO completion_response_events (
			  caller_id, idempotency_key, sequence, kind, data, created_at
			) VALUES (?, ?, ?, 'wire', ?, ?)`,
			quarantine.CallerID, quarantine.IdempotencyKey, sequence,
			quarantine.StreamTerminal, toUnixNano(now),
		); err != nil {
			return fmt.Errorf("append recovery quarantine terminal frame: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE completion_requests
			SET response_content_type = CASE
			      WHEN response_content_type = '' THEN 'text/event-stream'
			      ELSE response_content_type
			    END,
			    response_complete = 1, completed_at = ?, recovery_quarantined = 1
			WHERE caller_id = ? AND idempotency_key = ?`,
			toUnixNano(now), quarantine.CallerID, quarantine.IdempotencyKey,
		); err != nil {
			return fmt.Errorf("complete streaming recovery quarantine: %w", err)
		}
	} else {
		// Status 0 is the normal pre-stream case. Any other incomplete non-200
		// status is itself an impossible/corrupt decision, so replace it with the
		// same explicit 500 rather than preserving another endless shape.
		if _, err := tx.ExecContext(ctx, `
			UPDATE completion_requests
			SET response_status = ?, response_content_type = ?, response_retry_after = ?,
			    response_body = ?, response_complete = 1, completed_at = ?,
			    recovery_quarantined = 1
			WHERE caller_id = ? AND idempotency_key = ?`,
			quarantine.Failure.StatusCode, quarantine.Failure.ContentType,
			quarantine.Failure.RetryAfter, quarantine.Failure.Body, toUnixNano(now),
			quarantine.CallerID, quarantine.IdempotencyKey,
		); err != nil {
			return fmt.Errorf("commit pre-stream recovery quarantine: %w", err)
		}
	}

	state := completion.State(taskState)
	switch {
	case responseStatus == httpStatusUndecided &&
		(state == completion.StateAdmitted || state == completion.StateReconciled):
		// Match FailRequest semantics: an explicit new key may retry the exact
		// digest, while this quarantined key always replays its finite 500.
		if _, err := tx.ExecContext(ctx, `
			UPDATE completion_tasks
			SET retry_request_digest = ?, revision = revision + 1, updated_at = ?
			WHERE caller_id = ? AND task_id = ?`,
			requestDigest, toUnixNano(now), quarantine.CallerID, taskID,
		); err != nil {
			return fmt.Errorf("preserve recovery quarantine retry grant: %w", err)
		}
	case !state.Valid() || !state.IsTerminal():
		// Once 200 was committed, or if the task state itself is corrupt, there
		// is no safe continuation point. Repair the task to a finite terminal
		// state and release any stale lease.
		if _, err := tx.ExecContext(ctx, `
			UPDATE completion_tasks
			SET state = ?, lease_owner = '', retry_request_digest = '',
			    revision = revision + 1, updated_at = ?
			WHERE caller_id = ? AND task_id = ?`,
			completion.StateFailed, toUnixNano(now), quarantine.CallerID, taskID,
		); err != nil {
			return fmt.Errorf("terminate recovery quarantine task: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit recovery quarantine transaction: %w", err)
	}
	return nil
}

func (s *Store) BeginRequest(ctx context.Context, input storeapi.BeginRequestInput) (storeapi.BeginRequestResult, error) {
	if input.State == "" {
		input.State = completion.StateAdmitted
	}
	if input.State != completion.StateAdmitted {
		return storeapi.BeginRequestResult{}, fmt.Errorf("new task state must be admitted, got %q", input.State)
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" || strings.TrimSpace(input.RequestDigest) == "" {
		return storeapi.BeginRequestResult{}, errors.New("idempotency key and request digest are required")
	}
	canonicalPayload, err := marshalCanonicalRequest(input.CanonicalRequest)
	if err != nil {
		return storeapi.BeginRequestResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.BeginRequestResult{}, fmt.Errorf("begin admission transaction: %w", err)
	}
	defer tx.Rollback()

	request, err := getRequest(ctx, tx, storeapi.RequestKey{
		CallerID:       input.CallerID,
		IdempotencyKey: input.IdempotencyKey,
	})
	if err == nil {
		if request.RequestDigest != input.RequestDigest {
			return storeapi.BeginRequestResult{}, storeapi.ErrIdempotencyConflict
		}
		if request.PayloadPrunedAt != nil {
			return storeapi.BeginRequestResult{}, storeapi.ErrReplayPayloadExpired
		}
		if !request.RecoveryQuarantined {
			storedPayload, err := marshalCanonicalRequest(request.CanonicalRequest)
			if err != nil {
				return storeapi.BeginRequestResult{}, err
			}
			if !bytes.Equal(storedPayload, canonicalPayload) {
				return storeapi.BeginRequestResult{}, storeapi.ErrIdempotencyConflict
			}
		}
		task, err := getTask(ctx, tx, storeapi.TaskKey{CallerID: request.CallerID, TaskID: request.TaskID})
		if err != nil {
			return storeapi.BeginRequestResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return storeapi.BeginRequestResult{}, fmt.Errorf("commit request replay lookup: %w", err)
		}
		return storeapi.BeginRequestResult{Task: task, Request: request, Replay: true}, nil
	}
	if !errors.Is(err, storeapi.ErrNotFound) {
		return storeapi.BeginRequestResult{}, err
	}

	taskKey := storeapi.TaskKey{CallerID: input.CallerID, TaskID: input.TaskID}
	task, err := getTask(ctx, tx, taskKey)
	switch {
	case err == nil:
		if task.WorkspaceKey != input.WorkspaceKey || task.CapabilityTier != input.CapabilityTier ||
			task.Dialect != input.Dialect || task.HarnessID != input.HarnessID ||
			task.HarnessVersion != input.HarnessVersion || task.HarnessSessionID != input.HarnessSessionID ||
			task.Root != input.Root || task.ExecAllowed != input.ExecAllowed {
			return storeapi.BeginRequestResult{}, storeapi.ErrTaskConflict
		}
		active, err := hasActiveRequestTx(ctx, tx, taskKey)
		if err != nil {
			return storeapi.BeginRequestResult{}, err
		}
		if active {
			return storeapi.BeginRequestResult{}, fmt.Errorf(
				"%w: task already has an active completion request", storeapi.ErrTaskNotReady,
			)
		}
		switch task.State {
		case completion.StateAwaitingCaller:
			task, err = transitionTaskTx(ctx, tx, task, completion.StateReconciled, "", s.now)
			if err != nil {
				return storeapi.BeginRequestResult{}, err
			}
		case completion.StateAwaitingResults:
			task, err = reconcileToolResultsTx(ctx, tx, task, input.ToolResults, s.now)
			if err != nil {
				return storeapi.BeginRequestResult{}, err
			}
		case completion.StateAdmitted, completion.StateReconciled:
			if task.RetryRequestDigest == "" || task.RetryRequestDigest != input.RequestDigest {
				return storeapi.BeginRequestResult{}, fmt.Errorf(
					"%w: current state is %s", storeapi.ErrTaskNotReady, task.State,
				)
			}
			task, err = clearRetryRequestDigestTx(ctx, tx, task, s.now)
			if err != nil {
				return storeapi.BeginRequestResult{}, err
			}
		default:
			if task.State.IsTerminal() {
				return storeapi.BeginRequestResult{}, fmt.Errorf("%w: task is terminal", storeapi.ErrTaskConflict)
			}
			return storeapi.BeginRequestResult{}, fmt.Errorf("%w: current state is %s", storeapi.ErrTaskNotReady, task.State)
		}
	case errors.Is(err, storeapi.ErrNotFound):
		now := s.now().UTC()
		task = input.Task
		task.State = completion.StateAdmitted
		task.Revision = 1
		task.CreatedAt = now
		task.UpdatedAt = now
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO completion_tasks (
			  caller_id, task_id, workspace_key, capability_tier, dialect,
			  harness_id, harness_version, harness_session_id, workspace_root, exec_allowed, state,
			  lease_owner, retry_request_digest, revision, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 1, ?, ?)`,
			task.CallerID, task.TaskID, task.WorkspaceKey, task.CapabilityTier, task.Dialect,
			task.HarnessID, task.HarnessVersion, task.HarnessSessionID, task.Root, task.ExecAllowed, task.State, task.LeaseOwner,
			toUnixNano(now), toUnixNano(now)); err != nil {
			return storeapi.BeginRequestResult{}, fmt.Errorf("insert completion task: %w", err)
		}
	default:
		return storeapi.BeginRequestResult{}, err
	}

	createdAt := s.now().UTC()
	request = storeapi.Request{
		RequestKey: storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey},
		TaskID:     input.TaskID, RequestDigest: input.RequestDigest,
		CanonicalRequest: input.CanonicalRequest, CreatedAt: createdAt,
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO completion_requests (
		  caller_id, idempotency_key, task_id, request_digest, canonical_request, created_at
		) VALUES (?, ?, ?, ?, ?, ?)`, request.CallerID, request.IdempotencyKey,
		request.TaskID, request.RequestDigest, canonicalPayload, toUnixNano(createdAt)); err != nil {
		return storeapi.BeginRequestResult{}, fmt.Errorf("insert completion request: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return storeapi.BeginRequestResult{}, fmt.Errorf("commit request admission: %w", err)
	}
	return storeapi.BeginRequestResult{Task: task, Request: request}, nil
}

// BeginResponse durably commits the 200/SSE boundary before an assignment can
// become visible to a worker. A retry that races the original request waits
// for this decision instead of guessing from the existence of response frames.
func (s *Store) BeginResponse(ctx context.Context, key storeapi.RequestKey) (storeapi.Request, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("begin response decision transaction: %w", err)
	}
	defer tx.Rollback()
	request, err := getRequest(ctx, tx, key)
	if err != nil {
		return storeapi.Request{}, err
	}
	switch request.Response.StatusCode {
	case httpStatusUndecided:
		if request.ResponseComplete {
			return storeapi.Request{}, storeapi.ErrStateConflict
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE completion_requests
			SET response_status = ?, response_content_type = ?
			WHERE caller_id = ? AND idempotency_key = ?
			  AND response_status = 0 AND response_complete = 0`,
			httpStatusOK, "text/event-stream", key.CallerID, key.IdempotencyKey)
		if err != nil {
			return storeapi.Request{}, fmt.Errorf("commit streaming response decision: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return storeapi.Request{}, fmt.Errorf("read streaming response decision row count: %w", err)
		}
		if rows != 1 {
			return storeapi.Request{}, storeapi.ErrStateConflict
		}
		request.Response.StatusCode = httpStatusOK
		request.Response.ContentType = "text/event-stream"
	case httpStatusOK:
		// Idempotent recovery after a crash immediately following the commit.
	default:
		return storeapi.Request{}, storeapi.ErrStateConflict
	}
	task, err := getTask(ctx, tx, storeapi.TaskKey{CallerID: request.CallerID, TaskID: request.TaskID})
	if err != nil {
		return storeapi.Request{}, err
	}
	if _, err := clearRetryRequestDigestTx(ctx, tx, task, s.now); err != nil {
		return storeapi.Request{}, err
	}
	if err := tx.Commit(); err != nil {
		return storeapi.Request{}, fmt.Errorf("commit response decision: %w", err)
	}
	return request, nil
}

// CompleteNonStreamingResponse atomically chooses the final HTTP status/body
// and closes a non-streaming request. Until this operation commits, duplicate
// callers observe an undecided response and wait; afterward every replay gets
// the exact same bytes. Canonical Stream=false is the durable response-mode
// marker, so no assignment can be mistaken for a streaming response on restart.
func (s *Store) CompleteNonStreamingResponse(
	ctx context.Context,
	key storeapi.RequestKey,
	response storeapi.ResponseDecision,
) (storeapi.Request, error) {
	if response.StatusCode < 200 || response.StatusCode > 599 ||
		strings.TrimSpace(response.ContentType) == "" || len(response.Body) == 0 {
		return storeapi.Request{}, errors.New("terminal non-streaming response requires status, content type, and body")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("begin non-streaming response transaction: %w", err)
	}
	defer tx.Rollback()
	request, err := getRequest(ctx, tx, key)
	if err != nil {
		return storeapi.Request{}, err
	}
	if request.CanonicalRequest.Stream {
		return storeapi.Request{}, fmt.Errorf("%w: request is streaming", storeapi.ErrStateConflict)
	}
	if request.Response.StatusCode != httpStatusUndecided || request.ResponseComplete {
		if request.ResponseComplete && responseDecisionsEqual(request.Response, response) {
			task, err := getTask(ctx, tx, storeapi.TaskKey{CallerID: request.CallerID, TaskID: request.TaskID})
			if err != nil {
				return storeapi.Request{}, err
			}
			if _, err := clearRetryRequestDigestTx(ctx, tx, task, s.now); err != nil {
				return storeapi.Request{}, err
			}
			if err := tx.Commit(); err != nil {
				return storeapi.Request{}, fmt.Errorf("commit non-streaming response replay: %w", err)
			}
			return request, nil
		}
		return storeapi.Request{}, storeapi.ErrStateConflict
	}
	completedAt := s.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE completion_requests
		SET response_status = ?, response_content_type = ?, response_retry_after = ?,
		    response_body = ?, response_complete = 1, completed_at = ?
		WHERE caller_id = ? AND idempotency_key = ?
		  AND response_status = 0 AND response_complete = 0`,
		response.StatusCode, response.ContentType, response.RetryAfter, response.Body,
		toUnixNano(completedAt), key.CallerID, key.IdempotencyKey)
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("commit non-streaming response: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("read non-streaming response row count: %w", err)
	}
	if rows != 1 {
		return storeapi.Request{}, storeapi.ErrStateConflict
	}
	task, err := getTask(ctx, tx, storeapi.TaskKey{CallerID: request.CallerID, TaskID: request.TaskID})
	if err != nil {
		return storeapi.Request{}, err
	}
	if _, err := clearRetryRequestDigestTx(ctx, tx, task, s.now); err != nil {
		return storeapi.Request{}, err
	}
	if err := tx.Commit(); err != nil {
		return storeapi.Request{}, fmt.Errorf("commit non-streaming response transaction: %w", err)
	}
	request.Response = cloneResponseDecision(response)
	request.ResponseComplete = true
	request.CompletedAt = &completedAt
	return request, nil
}

// FailRequest atomically chooses a terminal pre-stream HTTP response and
// completes only that request. The task remains admitted/reconciled and keeps
// the failed canonical digest as a narrow retry grant: a new idempotency key
// may retry that exact request, but cannot smuggle a different continuation
// into a turn which never crossed its response boundary.
func (s *Store) FailRequest(
	ctx context.Context,
	key storeapi.RequestKey,
	expected completion.State,
	response storeapi.ResponseDecision,
) (storeapi.Request, error) {
	if response.StatusCode < 400 || response.StatusCode > 599 ||
		strings.TrimSpace(response.ContentType) == "" || len(response.Body) == 0 {
		return storeapi.Request{}, errors.New("terminal pre-stream response requires status, content type, and body")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("begin failed response transaction: %w", err)
	}
	defer tx.Rollback()
	request, err := getRequest(ctx, tx, key)
	if err != nil {
		return storeapi.Request{}, err
	}
	if request.Response.StatusCode != httpStatusUndecided {
		if request.ResponseComplete && responseDecisionsEqual(request.Response, response) {
			return request, tx.Commit()
		}
		return storeapi.Request{}, storeapi.ErrStateConflict
	}
	if request.ResponseComplete {
		return storeapi.Request{}, storeapi.ErrStateConflict
	}
	if expected != completion.StateAdmitted && expected != completion.StateReconciled {
		return storeapi.Request{}, fmt.Errorf(
			"%w: pre-stream failure cannot preserve task state %s", storeapi.ErrStateConflict, expected,
		)
	}
	task, err := getTask(ctx, tx, storeapi.TaskKey{CallerID: request.CallerID, TaskID: request.TaskID})
	if err != nil {
		return storeapi.Request{}, err
	}
	if task.State != expected {
		return storeapi.Request{}, storeapi.ErrStateConflict
	}
	failedAt := s.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE completion_tasks
		SET retry_request_digest = ?, revision = revision + 1, updated_at = ?
		WHERE caller_id = ? AND task_id = ? AND state = ? AND revision = ?`,
		request.RequestDigest, toUnixNano(failedAt), task.CallerID, task.TaskID, task.State, task.Revision)
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("record retryable pre-stream failure: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("read retryable failure task row count: %w", err)
	}
	if rows != 1 {
		return storeapi.Request{}, storeapi.ErrStateConflict
	}
	completedAt := s.now().UTC()
	result, err = tx.ExecContext(ctx, `
		UPDATE completion_requests
		SET response_status = ?, response_content_type = ?, response_retry_after = ?,
		    response_body = ?, response_complete = 1, completed_at = ?
		WHERE caller_id = ? AND idempotency_key = ?
		  AND response_status = 0 AND response_complete = 0`,
		response.StatusCode, response.ContentType, response.RetryAfter, response.Body,
		toUnixNano(completedAt), key.CallerID, key.IdempotencyKey)
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("commit failed response decision: %w", err)
	}
	rows, err = result.RowsAffected()
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("read failed response decision row count: %w", err)
	}
	if rows != 1 {
		return storeapi.Request{}, storeapi.ErrStateConflict
	}
	if err := tx.Commit(); err != nil {
		return storeapi.Request{}, fmt.Errorf("commit failed response: %w", err)
	}
	request.Response = cloneResponseDecision(response)
	request.ResponseComplete = true
	request.CompletedAt = &completedAt
	return request, nil
}

func reconcileToolResultsTx(
	ctx context.Context,
	tx *sql.Tx,
	task storeapi.Task,
	results []storeapi.ToolResult,
	now func() time.Time,
) (storeapi.Task, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT tool_call_id
		FROM completion_tool_executions
		WHERE caller_id = ? AND task_id = ? AND status = 'pending'
		ORDER BY tool_call_id`, task.CallerID, task.TaskID)
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("list pending tool executions: %w", err)
	}
	var pending []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return storeapi.Task{}, fmt.Errorf("scan pending tool execution: %w", err)
		}
		pending = append(pending, id)
	}
	if err := rows.Close(); err != nil {
		return storeapi.Task{}, fmt.Errorf("close pending tool executions: %w", err)
	}
	if err := rows.Err(); err != nil {
		return storeapi.Task{}, fmt.Errorf("iterate pending tool executions: %w", err)
	}
	if len(pending) == 0 {
		return storeapi.Task{}, fmt.Errorf("%w: task has no pending tool calls", storeapi.ErrTaskNotReady)
	}

	byID := make(map[string]storeapi.ToolResult, len(results))
	for _, result := range results {
		if strings.TrimSpace(result.ToolCallID) == "" || !json.Valid(result.Result) {
			return storeapi.Task{}, errors.New("tool result id and canonical JSON result are required")
		}
		if previous, exists := byID[result.ToolCallID]; exists {
			if previous.IsError != result.IsError || !bytes.Equal(previous.Result, result.Result) {
				return storeapi.Task{}, storeapi.ErrToolCallConflict
			}
			continue
		}
		byID[result.ToolCallID] = result
	}
	for _, id := range pending {
		result, exists := byID[id]
		if !exists {
			return storeapi.Task{}, fmt.Errorf("%w: %s", storeapi.ErrToolResultsMissing, id)
		}
		completedAt := now().UTC()
		updated, err := tx.ExecContext(ctx, `
			UPDATE completion_tool_executions
			SET status = 'completed', result = ?, is_error = ?, completed_at = ?
			WHERE caller_id = ? AND task_id = ? AND tool_call_id = ? AND status = 'pending'`,
			result.Result, result.IsError, toUnixNano(completedAt), task.CallerID, task.TaskID, id)
		if err != nil {
			return storeapi.Task{}, fmt.Errorf("reconcile tool execution %q: %w", id, err)
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return storeapi.Task{}, fmt.Errorf("read reconciled tool row count: %w", err)
		}
		if count != 1 {
			return storeapi.Task{}, storeapi.ErrStateConflict
		}
	}
	return transitionTaskTx(ctx, tx, task, completion.StateReconciled, "", now)
}

func transitionTaskTx(
	ctx context.Context,
	tx *sql.Tx,
	task storeapi.Task,
	next completion.State,
	workerID string,
	now func() time.Time,
) (storeapi.Task, error) {
	if err := completion.ValidateTransition(task.State, next); err != nil {
		return storeapi.Task{}, err
	}
	leaseOwner := task.LeaseOwner
	if next == completion.StateLeased {
		if strings.TrimSpace(workerID) == "" {
			return storeapi.Task{}, errors.New("worker id is required when entering leased")
		}
		if leaseOwner == "" {
			leaseOwner = workerID
		} else if leaseOwner != workerID {
			return storeapi.Task{}, storeapi.ErrLeaseConflict
		}
	}
	updatedAt := now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE completion_tasks
		SET state = ?, lease_owner = ?, retry_request_digest = '',
		    revision = revision + 1, updated_at = ?
		WHERE caller_id = ? AND task_id = ? AND state = ? AND revision = ?`,
		next, leaseOwner, toUnixNano(updatedAt), task.CallerID, task.TaskID, task.State, task.Revision)
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("update completion task state: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("read transition row count: %w", err)
	}
	if count != 1 {
		return storeapi.Task{}, storeapi.ErrStateConflict
	}
	task.State = next
	task.LeaseOwner = leaseOwner
	task.RetryRequestDigest = ""
	task.Revision++
	task.UpdatedAt = updatedAt
	return task, nil
}

func hasActiveRequestTx(ctx context.Context, tx *sql.Tx, key storeapi.TaskKey) (bool, error) {
	var active int
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
		  SELECT 1
		  FROM completion_requests
		  WHERE caller_id = ? AND task_id = ? AND response_complete = 0
		)`, key.CallerID, key.TaskID).Scan(&active); err != nil {
		return false, fmt.Errorf("inspect active completion request: %w", err)
	}
	return active != 0, nil
}

func clearRetryRequestDigestTx(
	ctx context.Context,
	tx *sql.Tx,
	task storeapi.Task,
	now func() time.Time,
) (storeapi.Task, error) {
	if task.RetryRequestDigest == "" {
		return task, nil
	}
	updatedAt := now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE completion_tasks
		SET retry_request_digest = '', revision = revision + 1, updated_at = ?
		WHERE caller_id = ? AND task_id = ? AND state = ? AND revision = ?`,
		toUnixNano(updatedAt), task.CallerID, task.TaskID, task.State, task.Revision)
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("clear retry request digest: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("read retry digest clear row count: %w", err)
	}
	if count != 1 {
		return storeapi.Task{}, storeapi.ErrStateConflict
	}
	task.RetryRequestDigest = ""
	task.Revision++
	task.UpdatedAt = updatedAt
	return task, nil
}

func (s *Store) GetTask(ctx context.Context, key storeapi.TaskKey) (storeapi.Task, error) {
	return getTask(ctx, s.db, key)
}

func (s *Store) FindOpenHarnessTask(
	ctx context.Context,
	affinity storeapi.TaskAffinity,
) (storeapi.Task, error) {
	if strings.TrimSpace(affinity.CallerID) == "" || strings.TrimSpace(affinity.WorkspaceKey) == "" ||
		strings.TrimSpace(affinity.HarnessID) == "" || strings.TrimSpace(affinity.HarnessVersion) == "" ||
		strings.TrimSpace(affinity.HarnessSessionID) == "" {
		return storeapi.Task{}, errors.New("complete harness task affinity is required")
	}
	var taskID string
	err := s.db.QueryRowContext(ctx, `
		SELECT task_id
		FROM completion_tasks
		WHERE caller_id = ? AND workspace_key = ? AND harness_id = ?
		  AND harness_version = ? AND harness_session_id = ?
		  AND state NOT IN (?, ?, ?, ?, ?)`,
		affinity.CallerID, affinity.WorkspaceKey, affinity.HarnessID,
		affinity.HarnessVersion, affinity.HarnessSessionID,
		completion.StateCompleted, completion.StateCanceled, completion.StateRejected,
		completion.StateExpired, completion.StateFailed,
	).Scan(&taskID)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.Task{}, storeapi.ErrNotFound
	}
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("find open harness task: %w", err)
	}
	return getTask(ctx, s.db, storeapi.TaskKey{CallerID: affinity.CallerID, TaskID: taskID})
}

func (s *Store) TransitionTask(
	ctx context.Context,
	key storeapi.TaskKey,
	expected completion.State,
	next completion.State,
	workerID string,
) (storeapi.Task, error) {
	if err := completion.ValidateTransition(expected, next); err != nil {
		return storeapi.Task{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("begin transition transaction: %w", err)
	}
	defer tx.Rollback()
	task, err := getTask(ctx, tx, key)
	if err != nil {
		return storeapi.Task{}, err
	}
	if task.State != expected {
		return storeapi.Task{}, storeapi.ErrStateConflict
	}
	leaseOwner := task.LeaseOwner
	if next == completion.StateLeased {
		if strings.TrimSpace(workerID) == "" {
			return storeapi.Task{}, errors.New("worker id is required when entering leased")
		}
		if leaseOwner == "" {
			leaseOwner = workerID
		} else if leaseOwner != workerID {
			return storeapi.Task{}, storeapi.ErrLeaseConflict
		}
	}
	now := s.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE completion_tasks
		SET state = ?, lease_owner = ?, retry_request_digest = '',
		    revision = revision + 1, updated_at = ?
		WHERE caller_id = ? AND task_id = ? AND state = ? AND revision = ?`,
		next, leaseOwner, toUnixNano(now), key.CallerID, key.TaskID, expected, task.Revision)
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("update completion task state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("read transition row count: %w", err)
	}
	if rows != 1 {
		return storeapi.Task{}, storeapi.ErrStateConflict
	}
	if err := tx.Commit(); err != nil {
		return storeapi.Task{}, fmt.Errorf("commit task transition: %w", err)
	}
	task.State = next
	task.LeaseOwner = leaseOwner
	task.RetryRequestDigest = ""
	task.Revision++
	task.UpdatedAt = now
	return task, nil
}

func (s *Store) AppendResponseEvent(
	ctx context.Context,
	key storeapi.RequestKey,
	kind string,
	data []byte,
) (storeapi.ResponseEvent, error) {
	if strings.TrimSpace(kind) == "" {
		return storeapi.ResponseEvent{}, errors.New("response event kind is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.ResponseEvent{}, fmt.Errorf("begin response event transaction: %w", err)
	}
	defer tx.Rollback()
	request, err := getRequest(ctx, tx, key)
	if err != nil {
		return storeapi.ResponseEvent{}, err
	}
	if request.ResponseComplete {
		return storeapi.ResponseEvent{}, errors.New("response event log is complete")
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(sequence), 0) + 1
		FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ?`, key.CallerID, key.IdempotencyKey).Scan(&sequence); err != nil {
		return storeapi.ResponseEvent{}, fmt.Errorf("allocate response event sequence: %w", err)
	}
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO completion_response_events (
		  caller_id, idempotency_key, sequence, kind, data, created_at
		) VALUES (?, ?, ?, ?, ?, ?)`, key.CallerID, key.IdempotencyKey,
		sequence, kind, data, toUnixNano(now)); err != nil {
		return storeapi.ResponseEvent{}, fmt.Errorf("insert response event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return storeapi.ResponseEvent{}, fmt.Errorf("commit response event: %w", err)
	}
	return storeapi.ResponseEvent{RequestKey: key, Sequence: sequence, Kind: kind, Data: bytes.Clone(data), CreatedAt: now}, nil
}

// AppendWorkerResponseEvent appends one durable stage per worker event ID and
// kind. Exact retries return the original row; reusing an ID with different
// payload or digest fails closed. This is the online counterpart of the
// worker-event receipt and prevents a transient later-stage failure from
// poisoning recovery with duplicate step/applied rows.
func (s *Store) AppendWorkerResponseEvent(
	ctx context.Context,
	key storeapi.RequestKey,
	kind string,
	eventID string,
	eventDigest string,
	data []byte,
) (storeapi.ResponseEvent, error) {
	if strings.TrimSpace(kind) == "" || strings.TrimSpace(eventID) == "" || strings.TrimSpace(eventDigest) == "" {
		return storeapi.ResponseEvent{}, errors.New("worker response event kind, id, and digest are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.ResponseEvent{}, fmt.Errorf("begin worker response event transaction: %w", err)
	}
	defer tx.Rollback()
	existing := storeapi.ResponseEvent{
		RequestKey: key, Kind: kind, EventID: eventID,
	}
	var createdAt int64
	err = tx.QueryRowContext(ctx, `
		SELECT sequence, event_digest, data, created_at
		FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ? AND kind = ? AND event_id = ?`,
		key.CallerID, key.IdempotencyKey, kind, eventID,
	).Scan(&existing.Sequence, &existing.EventDigest, &existing.Data, &createdAt)
	if err == nil {
		if existing.EventDigest != eventDigest || !bytes.Equal(existing.Data, data) {
			return storeapi.ResponseEvent{}, storeapi.ErrWorkerEventConflict
		}
		existing.CreatedAt = fromUnixNano(createdAt)
		if err := tx.Commit(); err != nil {
			return storeapi.ResponseEvent{}, fmt.Errorf("commit worker response event replay: %w", err)
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return storeapi.ResponseEvent{}, fmt.Errorf("lookup worker response event: %w", err)
	}
	request, err := getRequest(ctx, tx, key)
	if err != nil {
		return storeapi.ResponseEvent{}, err
	}
	if request.ResponseComplete {
		return storeapi.ResponseEvent{}, errors.New("response event log is complete")
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(sequence), 0) + 1
		FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ?`, key.CallerID, key.IdempotencyKey).Scan(&sequence); err != nil {
		return storeapi.ResponseEvent{}, fmt.Errorf("allocate worker response event sequence: %w", err)
	}
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO completion_response_events (
		  caller_id, idempotency_key, sequence, kind, event_id, event_digest, data, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, key.CallerID, key.IdempotencyKey,
		sequence, kind, eventID, eventDigest, data, toUnixNano(now)); err != nil {
		return storeapi.ResponseEvent{}, fmt.Errorf("insert worker response event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return storeapi.ResponseEvent{}, fmt.Errorf("commit worker response event: %w", err)
	}
	return storeapi.ResponseEvent{
		RequestKey: key, Sequence: sequence, Kind: kind,
		EventID: eventID, EventDigest: eventDigest, Data: bytes.Clone(data), CreatedAt: now,
	}, nil
}

// ReadResponse returns response state and cursor events from one SQLite read
// transaction. The streaming handler relies on this snapshot guarantee: once
// response_complete is visible, the terminal applied event must be visible in
// the same result.
func (s *Store) ReadResponse(
	ctx context.Context,
	key storeapi.RequestKey,
	after int64,
) (storeapi.ResponseRead, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return storeapi.ResponseRead{}, fmt.Errorf("begin response read snapshot: %w", err)
	}
	defer tx.Rollback()

	read := storeapi.ResponseRead{RequestKey: key}
	var complete, payloadPruned int
	err = tx.QueryRowContext(ctx, `
		SELECT response_status, response_content_type, response_retry_after,
		       response_body, response_complete, payload_pruned_at IS NOT NULL
		FROM completion_requests
		WHERE caller_id = ? AND idempotency_key = ?`,
		key.CallerID, key.IdempotencyKey,
	).Scan(
		&read.Response.StatusCode, &read.Response.ContentType,
		&read.Response.RetryAfter, &read.Response.Body, &complete, &payloadPruned,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.ResponseRead{}, storeapi.ErrNotFound
	}
	if err != nil {
		return storeapi.ResponseRead{}, fmt.Errorf("read completion response state: %w", err)
	}
	read.ResponseComplete = complete != 0
	read.PayloadPruned = payloadPruned != 0
	read.Response.Body = bytes.Clone(read.Response.Body)
	read.Events, err = listResponseEvents(ctx, tx, key, after, responseEventsAll, "")
	if err != nil {
		return storeapi.ResponseRead{}, err
	}
	if err := tx.Commit(); err != nil {
		return storeapi.ResponseRead{}, fmt.Errorf("commit response read snapshot: %w", err)
	}
	return read, nil
}

func (s *Store) ListResponseEvents(ctx context.Context, key storeapi.RequestKey, after int64) ([]storeapi.ResponseEvent, error) {
	return listResponseEvents(ctx, s.db, key, after, responseEventsAll, "")
}

// ListWorkerEventStages uses the event-id index for online step/applied
// reconciliation.
func (s *Store) ListWorkerEventStages(
	ctx context.Context,
	key storeapi.RequestKey,
	eventID string,
) ([]storeapi.ResponseEvent, error) {
	if strings.TrimSpace(eventID) == "" {
		return nil, errors.New("worker event id is required")
	}
	return listResponseEvents(ctx, s.db, key, 0, responseEventsIndexed, eventID)
}

type responseEventQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type responseEventFilter uint8

const (
	responseEventsAll responseEventFilter = iota
	responseEventsIndexed
)

func listResponseEvents(
	ctx context.Context,
	db responseEventQueryer,
	key storeapi.RequestKey,
	after int64,
	filter responseEventFilter,
	eventID string,
) ([]storeapi.ResponseEvent, error) {
	query := `
		SELECT sequence, kind, event_id, event_digest, data, created_at
		FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ? AND sequence > ?
		`
	arguments := []any{key.CallerID, key.IdempotencyKey, after}
	switch filter {
	case responseEventsAll:
	case responseEventsIndexed:
		query += " AND event_id = ? AND kind IN ('step', 'applied')"
		arguments = append(arguments, eventID)
	default:
		return nil, errors.New("invalid response event filter")
	}
	query += " ORDER BY sequence"
	rows, err := db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("list response events: %w", err)
	}
	defer rows.Close()
	var events []storeapi.ResponseEvent
	for rows.Next() {
		var event storeapi.ResponseEvent
		var createdAt int64
		event.RequestKey = key
		if err := rows.Scan(
			&event.Sequence, &event.Kind, &event.EventID, &event.EventDigest, &event.Data, &createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan response event: %w", err)
		}
		event.CreatedAt = fromUnixNano(createdAt)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate response events: %w", err)
	}
	return events, nil
}

func (s *Store) RecordWorkerEventReceipt(
	ctx context.Context,
	key storeapi.RequestKey,
	eventID string,
	workerID string,
	digest string,
) (storeapi.WorkerEventReceipt, error) {
	if strings.TrimSpace(eventID) == "" || strings.TrimSpace(workerID) == "" || strings.TrimSpace(digest) == "" {
		return storeapi.WorkerEventReceipt{}, errors.New("worker event receipt requires event id, worker id, and digest")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.WorkerEventReceipt{}, fmt.Errorf("begin worker event receipt transaction: %w", err)
	}
	defer tx.Rollback()
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO completion_worker_event_receipts (
		  caller_id, idempotency_key, event_id, worker_id, event_digest, created_at
		) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id, idempotency_key, event_id) DO NOTHING`,
		key.CallerID, key.IdempotencyKey, eventID, workerID, digest, toUnixNano(now)); err != nil {
		return storeapi.WorkerEventReceipt{}, fmt.Errorf("insert worker event receipt: %w", err)
	}
	var storedWorkerID string
	var storedDigest string
	var createdAt int64
	if err := tx.QueryRowContext(ctx, `
		SELECT worker_id, event_digest, created_at
		FROM completion_worker_event_receipts
		WHERE caller_id = ? AND idempotency_key = ? AND event_id = ?`,
		key.CallerID, key.IdempotencyKey, eventID).Scan(&storedWorkerID, &storedDigest, &createdAt); err != nil {
		return storeapi.WorkerEventReceipt{}, fmt.Errorf("read worker event receipt: %w", err)
	}
	if storedWorkerID != workerID || storedDigest != digest {
		return storeapi.WorkerEventReceipt{}, storeapi.ErrWorkerEventConflict
	}
	if err := tx.Commit(); err != nil {
		return storeapi.WorkerEventReceipt{}, fmt.Errorf("commit worker event receipt: %w", err)
	}
	return storeapi.WorkerEventReceipt{
		RequestKey: key, EventID: eventID, WorkerID: storedWorkerID,
		Digest: storedDigest, CreatedAt: fromUnixNano(createdAt),
	}, nil
}

func (s *Store) LookupWorkerEventReceipt(
	ctx context.Context,
	key storeapi.RequestKey,
	eventID string,
) (storeapi.WorkerEventReceipt, error) {
	if strings.TrimSpace(eventID) == "" {
		return storeapi.WorkerEventReceipt{}, errors.New("worker event receipt requires event id")
	}
	var workerID string
	var storedDigest string
	var createdAt int64
	err := s.db.QueryRowContext(ctx, `
		SELECT worker_id, event_digest, created_at
		FROM completion_worker_event_receipts
		WHERE caller_id = ? AND idempotency_key = ? AND event_id = ?`,
		key.CallerID, key.IdempotencyKey, eventID).Scan(&workerID, &storedDigest, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.WorkerEventReceipt{}, storeapi.ErrNotFound
	}
	if err != nil {
		return storeapi.WorkerEventReceipt{}, fmt.Errorf("lookup worker event receipt: %w", err)
	}
	return storeapi.WorkerEventReceipt{
		RequestKey: key, EventID: eventID, WorkerID: workerID,
		Digest: storedDigest, CreatedAt: fromUnixNano(createdAt),
	}, nil
}

func (s *Store) CompleteRequest(ctx context.Context, key storeapi.RequestKey) error {
	now := s.now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE completion_requests
		SET response_complete = 1, completed_at = ?
		WHERE caller_id = ? AND idempotency_key = ?
		  AND response_status = ? AND response_complete = 0`,
		toUnixNano(now), key.CallerID, key.IdempotencyKey, httpStatusOK)
	if err != nil {
		return fmt.Errorf("complete response event log: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read request completion row count: %w", err)
	}
	if rows == 1 {
		return nil
	}
	request, err := getRequest(ctx, s.db, key)
	if err != nil {
		return err
	}
	if request.ResponseComplete {
		return nil
	}
	if request.Response.StatusCode != httpStatusOK {
		return fmt.Errorf("%w: streaming response boundary is not committed", storeapi.ErrStateConflict)
	}
	return storeapi.ErrStateConflict
}

// PurgeExpiredCompletionPayloads replaces completed request replay data with
// immutable idempotency tombstones. Active and otherwise recoverable requests
// are deliberately excluded: a completed response with an unacknowledged
// worker step still needs its canonical request and event log after restart.
//
// The caller supplies the completion cutoff (normally now minus the configured
// replay grace). A malformed completed row without completed_at conservatively
// uses created_at rather than retaining payload forever. Audit retention is separate.
func (s *Store) PurgeExpiredCompletionPayloads(ctx context.Context, completedBefore time.Time) (int64, error) {
	if completedBefore.IsZero() {
		return 0, errors.New("completion payload purge cutoff is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin completion payload purge: %w", err)
	}
	defer tx.Rollback()

	prunedAt := s.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE completion_requests AS request
		SET canonical_request = X'', response_body = X'', payload_pruned_at = ?
		WHERE request.response_complete = 1
		  AND COALESCE(request.completed_at, request.created_at) <= ?
		  AND request.payload_pruned_at IS NULL
		  AND NOT EXISTS (
		    SELECT 1
		    FROM completion_response_events AS step
		    WHERE step.caller_id = request.caller_id
		      AND step.idempotency_key = request.idempotency_key
		      AND step.kind = 'step'
		      AND step.event_id <> ''
		      AND NOT EXISTS (
		        SELECT 1
		        FROM completion_worker_event_receipts AS receipt
		        WHERE receipt.caller_id = step.caller_id
		          AND receipt.idempotency_key = step.idempotency_key
		          AND receipt.event_id = step.event_id
		      )
		  )`, toUnixNano(prunedAt), toUnixNano(completedBefore.UTC()))
	if err != nil {
		return 0, fmt.Errorf("tombstone expired completion payloads: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read completion payload purge count: %w", err)
	}

	// Tool results are task-scoped rather than request-scoped. Keep them while
	// any request in that task still has replay/recovery data; once the whole
	// terminal task consists only of tombstones, the result bytes are no longer
	// part of any live correctness decision.
	if _, err := tx.ExecContext(ctx, `
		UPDATE completion_tool_executions AS execution
		SET result = NULL
		WHERE execution.status = 'completed'
		  AND execution.result IS NOT NULL
		  AND EXISTS (
		    SELECT 1
		    FROM completion_tasks AS task
		    WHERE task.caller_id = execution.caller_id
		      AND task.task_id = execution.task_id
		      AND task.state IN (?, ?, ?, ?, ?)
		  )
		  AND EXISTS (
		    SELECT 1
		    FROM completion_requests AS request
		    WHERE request.caller_id = execution.caller_id
		      AND request.task_id = execution.task_id
		      AND request.payload_pruned_at IS NOT NULL
		  )
		  AND NOT EXISTS (
		    SELECT 1
		    FROM completion_requests AS request
		    WHERE request.caller_id = execution.caller_id
		      AND request.task_id = execution.task_id
		      AND request.payload_pruned_at IS NULL
		  )`, completion.StateCompleted, completion.StateCanceled, completion.StateRejected,
		completion.StateExpired, completion.StateFailed); err != nil {
		return 0, fmt.Errorf("prune completed tool result payloads: %w", err)
	}
	// Worker receipts contain only event identity and digest metadata. Retain
	// them with the request tombstone so a delayed durable-outbox duplicate can
	// still be acknowledged without recreating response payloads or a session.
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM completion_response_events
		WHERE EXISTS (
		  SELECT 1 FROM completion_requests AS request
		  WHERE request.caller_id = completion_response_events.caller_id
		    AND request.idempotency_key = completion_response_events.idempotency_key
		    AND request.payload_pruned_at IS NOT NULL
		)`); err != nil {
		return 0, fmt.Errorf("prune completion response events: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit completion payload purge: %w", err)
	}
	return count, nil
}

func (s *Store) BeginToolExecution(
	ctx context.Context,
	key storeapi.ToolExecutionKey,
	requestDigest string,
) (storeapi.BeginToolExecutionResult, error) {
	if strings.TrimSpace(requestDigest) == "" {
		return storeapi.BeginToolExecutionResult{}, errors.New("tool request digest is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.BeginToolExecutionResult{}, fmt.Errorf("begin tool execution transaction: %w", err)
	}
	defer tx.Rollback()
	execution, err := getToolExecution(ctx, tx, key)
	if err == nil {
		if execution.RequestDigest != requestDigest {
			return storeapi.BeginToolExecutionResult{}, storeapi.ErrToolCallConflict
		}
		if err := tx.Commit(); err != nil {
			return storeapi.BeginToolExecutionResult{}, fmt.Errorf("commit tool replay lookup: %w", err)
		}
		return storeapi.BeginToolExecutionResult{Execution: execution, Replay: true}, nil
	}
	if !errors.Is(err, storeapi.ErrNotFound) {
		return storeapi.BeginToolExecutionResult{}, err
	}
	if _, err := getTask(ctx, tx, storeapi.TaskKey{CallerID: key.CallerID, TaskID: key.TaskID}); err != nil {
		return storeapi.BeginToolExecutionResult{}, err
	}
	now := s.now().UTC()
	execution = storeapi.ToolExecution{
		ToolExecutionKey: key,
		RequestDigest:    requestDigest,
		Status:           "pending",
		CreatedAt:        now,
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO completion_tool_executions (
		  caller_id, task_id, tool_call_id, request_digest, status, created_at
		) VALUES (?, ?, ?, ?, 'pending', ?)`, key.CallerID, key.TaskID,
		key.ToolCallID, requestDigest, toUnixNano(now)); err != nil {
		return storeapi.BeginToolExecutionResult{}, fmt.Errorf("insert tool execution: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return storeapi.BeginToolExecutionResult{}, fmt.Errorf("commit tool execution: %w", err)
	}
	return storeapi.BeginToolExecutionResult{Execution: execution}, nil
}

func (s *Store) LookupToolExecution(
	ctx context.Context,
	key storeapi.ToolExecutionKey,
) (storeapi.ToolExecution, error) {
	return getToolExecution(ctx, s.db, key)
}

func (s *Store) CompleteToolExecution(
	ctx context.Context,
	key storeapi.ToolExecutionKey,
	resultPayload []byte,
	isError bool,
) (storeapi.ToolExecution, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeapi.ToolExecution{}, fmt.Errorf("begin tool completion transaction: %w", err)
	}
	defer tx.Rollback()
	execution, err := getToolExecution(ctx, tx, key)
	if err != nil {
		return storeapi.ToolExecution{}, err
	}
	if execution.Status == "completed" {
		if execution.IsError == isError && bytes.Equal(execution.Result, resultPayload) {
			if err := tx.Commit(); err != nil {
				return storeapi.ToolExecution{}, fmt.Errorf("commit completed tool replay lookup: %w", err)
			}
			return execution, nil
		}
		return storeapi.ToolExecution{}, storeapi.ErrToolCallConflict
	}
	now := s.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE completion_tool_executions
		SET status = 'completed', result = ?, is_error = ?, completed_at = ?
		WHERE caller_id = ? AND task_id = ? AND tool_call_id = ? AND status = 'pending'`,
		resultPayload, isError, toUnixNano(now), key.CallerID, key.TaskID, key.ToolCallID)
	if err != nil {
		return storeapi.ToolExecution{}, fmt.Errorf("complete tool execution: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return storeapi.ToolExecution{}, fmt.Errorf("read tool completion row count: %w", err)
	}
	if rows != 1 {
		return storeapi.ToolExecution{}, storeapi.ErrStateConflict
	}
	if err := tx.Commit(); err != nil {
		return storeapi.ToolExecution{}, fmt.Errorf("commit tool completion: %w", err)
	}
	execution.Status = "completed"
	execution.Result = bytes.Clone(resultPayload)
	execution.IsError = isError
	execution.CompletedAt = &now
	return execution, nil
}

func (s *Store) CreateAPIToken(ctx context.Context, token storeapi.APIToken) error {
	if token.KeyID == "" || token.PrincipalType == "" || token.SubjectID == "" || len(token.TokenHash) == 0 {
		return errors.New("key id, principal type, subject id, and token hash are required")
	}
	createdAt := token.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = s.now().UTC()
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO api_tokens (key_id, principal_type, subject_id, token_hash, created_at)
		VALUES (?, ?, ?, ?, ?)`, token.KeyID, token.PrincipalType, token.SubjectID, token.TokenHash, toUnixNano(createdAt)); err != nil {
		return fmt.Errorf("insert api token: %w", err)
	}
	return nil
}

func (s *Store) FindAPITokenByHash(ctx context.Context, tokenHash []byte) (storeapi.APIToken, error) {
	var token storeapi.APIToken
	var createdAt int64
	var revokedAt sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT key_id, principal_type, subject_id, token_hash, created_at, revoked_at
		FROM api_tokens WHERE token_hash = ?`, tokenHash).Scan(
		&token.KeyID, &token.PrincipalType, &token.SubjectID, &token.TokenHash, &createdAt, &revokedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.APIToken{}, storeapi.ErrNotFound
	}
	if err != nil {
		return storeapi.APIToken{}, fmt.Errorf("find api token: %w", err)
	}
	token.CreatedAt = fromUnixNano(createdAt)
	if revokedAt.Valid {
		value := fromUnixNano(revokedAt.Int64)
		token.RevokedAt = &value
	}
	return token, nil
}

func (s *Store) RevokeAPIToken(ctx context.Context, keyID string) error {
	now := s.now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE api_tokens SET revoked_at = COALESCE(revoked_at, ?)
		WHERE key_id = ?`, toUnixNano(now), keyID)
	if err != nil {
		return fmt.Errorf("revoke api token: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read api token revoke row count: %w", err)
	}
	if rows == 0 {
		return storeapi.ErrNotFound
	}
	return nil
}

func (s *Store) CreateAuditMetadata(
	ctx context.Context,
	metadata storeapi.AuditMetadata,
) (storeapi.AuditMetadata, error) {
	if strings.TrimSpace(metadata.ID) == "" || strings.TrimSpace(metadata.CallerID) == "" ||
		strings.TrimSpace(metadata.TaskID) == "" ||
		strings.TrimSpace(string(metadata.Dialect)) == "" || strings.TrimSpace(metadata.KeyID) == "" {
		return storeapi.AuditMetadata{}, errors.New("audit id, caller id, task id, dialect, and key id are required")
	}
	if metadata.PendingMS < 0 || metadata.GenMS < 0 {
		return storeapi.AuditMetadata{}, errors.New("audit durations must not be negative")
	}
	createdAt := metadata.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = s.now().UTC()
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO audit_metadata (
		  id, caller_id, workspace_key, task_id, dialect, key_id,
		  pending_ms, gen_ms, error, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		metadata.ID, metadata.CallerID, metadata.WorkspaceKey, metadata.TaskID,
		metadata.Dialect, metadata.KeyID, metadata.PendingMS, metadata.GenMS,
		metadata.Error, toUnixNano(createdAt)); err != nil {
		existing, getErr := s.GetAuditMetadata(ctx, metadata.ID)
		if getErr == nil && existing.CallerID == metadata.CallerID && existing.WorkspaceKey == metadata.WorkspaceKey &&
			existing.TaskID == metadata.TaskID && existing.Dialect == metadata.Dialect && existing.KeyID == metadata.KeyID {
			return existing, nil
		}
		return storeapi.AuditMetadata{}, fmt.Errorf("insert audit metadata: %w", err)
	}
	metadata.CreatedAt = createdAt
	return metadata, nil
}

func (s *Store) CompleteAuditMetadata(
	ctx context.Context,
	id string,
	pendingMS int64,
	genMS int64,
	errorMessage string,
) (storeapi.AuditMetadata, error) {
	if strings.TrimSpace(id) == "" || pendingMS < 0 || genMS < 0 {
		return storeapi.AuditMetadata{}, errors.New("audit id and non-negative durations are required")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE audit_metadata
		SET pending_ms = ?, gen_ms = ?, error = ?
		WHERE id = ?`, pendingMS, genMS, errorMessage, id)
	if err != nil {
		return storeapi.AuditMetadata{}, fmt.Errorf("complete audit metadata: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return storeapi.AuditMetadata{}, fmt.Errorf("read audit completion row count: %w", err)
	}
	if rows == 0 {
		return storeapi.AuditMetadata{}, storeapi.ErrNotFound
	}
	return s.GetAuditMetadata(ctx, id)
}

func (s *Store) GetAuditMetadata(ctx context.Context, id string) (storeapi.AuditMetadata, error) {
	var metadata storeapi.AuditMetadata
	var dialect string
	var createdAt int64
	err := s.db.QueryRowContext(ctx, `
		SELECT id, caller_id, workspace_key, task_id, dialect, key_id,
		       pending_ms, gen_ms, error, created_at
		FROM audit_metadata WHERE id = ?`, id).Scan(
		&metadata.ID, &metadata.CallerID, &metadata.WorkspaceKey, &metadata.TaskID,
		&dialect, &metadata.KeyID, &metadata.PendingMS, &metadata.GenMS,
		&metadata.Error, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.AuditMetadata{}, storeapi.ErrNotFound
	}
	if err != nil {
		return storeapi.AuditMetadata{}, fmt.Errorf("get audit metadata: %w", err)
	}
	metadata.Dialect = canonical.Dialect(dialect)
	metadata.CreatedAt = fromUnixNano(createdAt)
	return metadata, nil
}

func (s *Store) StoreAuditPayload(
	ctx context.Context,
	payload storeapi.AuditPayload,
) (storeapi.AuditPayload, error) {
	if strings.TrimSpace(payload.AuditID) == "" || strings.TrimSpace(payload.Kind) == "" {
		return storeapi.AuditPayload{}, errors.New("audit id and payload kind are required")
	}
	if payload.Data == nil {
		return storeapi.AuditPayload{}, errors.New("audit payload data is required")
	}
	if payload.ExpiresAt.IsZero() {
		return storeapi.AuditPayload{}, errors.New("audit payload expiry is required")
	}
	createdAt := payload.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = s.now().UTC()
	}
	expiresAt := payload.ExpiresAt.UTC()
	if !expiresAt.After(createdAt) {
		return storeapi.AuditPayload{}, errors.New("audit payload expiry must be after creation")
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO audit_payloads (audit_id, kind, data, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)`, payload.AuditID, payload.Kind, payload.Data,
		toUnixNano(createdAt), toUnixNano(expiresAt)); err != nil {
		return storeapi.AuditPayload{}, fmt.Errorf("insert audit payload: %w", err)
	}
	payload.Data = bytes.Clone(payload.Data)
	payload.CreatedAt = createdAt
	payload.ExpiresAt = expiresAt
	return payload, nil
}

func (s *Store) GetAuditPayload(ctx context.Context, auditID, kind string) (storeapi.AuditPayload, error) {
	var payload storeapi.AuditPayload
	var createdAt, expiresAt int64
	err := s.db.QueryRowContext(ctx, `
		SELECT audit_id, kind, data, created_at, expires_at
		FROM audit_payloads
		WHERE audit_id = ? AND kind = ? AND expires_at > ?`,
		auditID, kind, toUnixNano(s.now().UTC())).Scan(
		&payload.AuditID, &payload.Kind, &payload.Data, &createdAt, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.AuditPayload{}, storeapi.ErrNotFound
	}
	if err != nil {
		return storeapi.AuditPayload{}, fmt.Errorf("get audit payload: %w", err)
	}
	payload.CreatedAt = fromUnixNano(createdAt)
	payload.ExpiresAt = fromUnixNano(expiresAt)
	return payload, nil
}

func (s *Store) PurgeExpiredAuditPayloads(ctx context.Context, before time.Time) (int64, error) {
	if before.IsZero() {
		return 0, errors.New("audit payload purge cutoff is required")
	}
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM audit_payloads WHERE expires_at <= ?`, toUnixNano(before.UTC()))
	if err != nil {
		return 0, fmt.Errorf("purge expired audit payloads: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read audit payload purge count: %w", err)
	}
	return count, nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getTask(ctx context.Context, db queryer, key storeapi.TaskKey) (storeapi.Task, error) {
	var task storeapi.Task
	var tier, dialect, state string
	var execAllowed int
	var createdAt, updatedAt int64
	err := db.QueryRowContext(ctx, `
		SELECT workspace_key, capability_tier, dialect, harness_id,
		       harness_version, harness_session_id, workspace_root, exec_allowed, state, lease_owner,
		       retry_request_digest, revision,
		       created_at, updated_at
		FROM completion_tasks
		WHERE caller_id = ? AND task_id = ?`, key.CallerID, key.TaskID).Scan(
		&task.WorkspaceKey, &tier, &dialect, &task.HarnessID,
		&task.HarnessVersion, &task.HarnessSessionID, &task.Root, &execAllowed, &state, &task.LeaseOwner,
		&task.RetryRequestDigest, &task.Revision,
		&createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.Task{}, storeapi.ErrNotFound
	}
	if err != nil {
		return storeapi.Task{}, fmt.Errorf("get completion task: %w", err)
	}
	task.TaskKey = key
	task.CapabilityTier = completion.CapabilityTier(tier)
	task.Dialect = canonical.Dialect(dialect)
	task.State = completion.State(state)
	task.ExecAllowed = execAllowed != 0
	task.CreatedAt = fromUnixNano(createdAt)
	task.UpdatedAt = fromUnixNano(updatedAt)
	return task, nil
}

func getRequest(ctx context.Context, db queryer, key storeapi.RequestKey) (storeapi.Request, error) {
	var request storeapi.Request
	var canonicalPayload []byte
	var responseBody []byte
	var complete, recoveryQuarantined int
	var createdAt int64
	var completedAt sql.NullInt64
	var payloadPrunedAt sql.NullInt64
	err := db.QueryRowContext(ctx, `
		SELECT task_id, request_digest, canonical_request,
		       response_status, response_content_type, response_retry_after, response_body,
		       response_complete, recovery_quarantined,
		       created_at, completed_at, payload_pruned_at
		FROM completion_requests
		WHERE caller_id = ? AND idempotency_key = ?`, key.CallerID, key.IdempotencyKey).Scan(
		&request.TaskID, &request.RequestDigest, &canonicalPayload,
		&request.Response.StatusCode, &request.Response.ContentType, &request.Response.RetryAfter, &responseBody,
		&complete, &recoveryQuarantined, &createdAt, &completedAt, &payloadPrunedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.Request{}, storeapi.ErrNotFound
	}
	if err != nil {
		return storeapi.Request{}, fmt.Errorf("get completion request: %w", err)
	}
	request.RequestKey = key
	request.Response.Body = bytes.Clone(responseBody)
	request.RecoveryQuarantined = recoveryQuarantined != 0
	if payloadPrunedAt.Valid {
		value := fromUnixNano(payloadPrunedAt.Int64)
		request.PayloadPrunedAt = &value
	} else if !request.RecoveryQuarantined {
		request.CanonicalRequest, err = unmarshalCanonicalRequest(canonicalPayload, key)
		if err != nil {
			return storeapi.Request{}, err
		}
	}
	request.ResponseComplete = complete != 0
	request.CreatedAt = fromUnixNano(createdAt)
	if completedAt.Valid {
		value := fromUnixNano(completedAt.Int64)
		request.CompletedAt = &value
	}
	return request, nil
}

func cloneResponseDecision(response storeapi.ResponseDecision) storeapi.ResponseDecision {
	response.Body = bytes.Clone(response.Body)
	return response
}

func responseDecisionsEqual(left, right storeapi.ResponseDecision) bool {
	return left.StatusCode == right.StatusCode && left.ContentType == right.ContentType &&
		left.RetryAfter == right.RetryAfter && bytes.Equal(left.Body, right.Body)
}

func marshalCanonicalRequest(request canonical.Request) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("canonical request: %w", err)
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical request: %w", err)
	}
	return payload, nil
}

func unmarshalCanonicalRequest(payload []byte, key storeapi.RequestKey) (canonical.Request, error) {
	if len(payload) == 0 {
		return canonical.Request{}, fmt.Errorf(
			"%w: %s/%s has no canonical payload",
			storeapi.ErrUnrecoverableRequest, key.CallerID, key.IdempotencyKey,
		)
	}
	var request canonical.Request
	if err := json.Unmarshal(payload, &request); err != nil {
		return canonical.Request{}, fmt.Errorf(
			"%w: %s/%s canonical payload is invalid JSON: %v",
			storeapi.ErrUnrecoverableRequest, key.CallerID, key.IdempotencyKey, err,
		)
	}
	if err := request.Validate(); err != nil {
		return canonical.Request{}, fmt.Errorf(
			"%w: %s/%s canonical payload is invalid: %v",
			storeapi.ErrUnrecoverableRequest, key.CallerID, key.IdempotencyKey, err,
		)
	}
	return request, nil
}

func getToolExecution(ctx context.Context, db queryer, key storeapi.ToolExecutionKey) (storeapi.ToolExecution, error) {
	var execution storeapi.ToolExecution
	var result []byte
	var isError int
	var createdAt int64
	var completedAt sql.NullInt64
	err := db.QueryRowContext(ctx, `
		SELECT request_digest, status, result, is_error, created_at, completed_at
		FROM completion_tool_executions
		WHERE caller_id = ? AND task_id = ? AND tool_call_id = ?`,
		key.CallerID, key.TaskID, key.ToolCallID).Scan(
		&execution.RequestDigest, &execution.Status, &result, &isError, &createdAt, &completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.ToolExecution{}, storeapi.ErrNotFound
	}
	if err != nil {
		return storeapi.ToolExecution{}, fmt.Errorf("get tool execution: %w", err)
	}
	execution.ToolExecutionKey = key
	execution.Result = bytes.Clone(result)
	execution.IsError = isError != 0
	execution.CreatedAt = fromUnixNano(createdAt)
	if completedAt.Valid {
		value := fromUnixNano(completedAt.Int64)
		execution.CompletedAt = &value
	}
	return execution, nil
}

func toUnixNano(value time.Time) int64 {
	return value.UnixNano()
}

func fromUnixNano(value int64) time.Time {
	return time.Unix(0, value).UTC()
}
