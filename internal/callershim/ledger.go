// Package callershim provides the demand-side execution boundary for the
// project-owned completion-mode harness adapter.
package callershim

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	ErrLedgerNotFound          = errors.New("caller tool execution not found")
	ErrExecutionReplay         = errors.New("tool call id was reused with different input")
	ErrExecutionPending        = errors.New("tool execution is still in progress")
	ErrLedgerInUse             = errors.New("caller tool ledger is already held by another live owner")
	errUnsupportedLedgerSchema = errors.New("unsupported caller ledger schema; recreate database")
)

type ExecutionKey struct {
	CallerID   string
	TaskID     string
	ToolCallID string
}

type Execution struct {
	ExecutionKey
	RequestDigest string
	Status        string
	Response      []byte
	CreatedAt     time.Time
	CompletedAt   *time.Time
}

type BeginResult struct {
	Execution Execution
	Replay    bool
}

type Ledger interface {
	Begin(context.Context, ExecutionKey, string) (BeginResult, error)
	Complete(context.Context, ExecutionKey, []byte) (Execution, error)
	MarkIndeterminate(context.Context, ExecutionKey, []byte) (Execution, error)
}

type SQLiteLedger struct {
	db        *sql.DB
	now       func() time.Time
	ownerLock *ledgerOwnerLock
	closeOnce sync.Once
	closeErr  error
}

const (
	ledgerSchemaVersion     = 2
	ledgerSchemaFingerprint = "human-caller-ledger-v2-20260717"
)

const ledgerSchema = `
CREATE TABLE IF NOT EXISTS caller_ledger_schema (
  component TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  fingerprint TEXT NOT NULL
);
INSERT INTO caller_ledger_schema (component, version, fingerprint)
VALUES ('caller-ledger', 2, 'human-caller-ledger-v2-20260717')
ON CONFLICT(component) DO NOTHING;

CREATE TABLE IF NOT EXISTS caller_tool_executions (
  caller_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  tool_call_id TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending', 'completed', 'indeterminate')),
  response BLOB,
  created_at INTEGER NOT NULL,
  completed_at INTEGER,
  CHECK (
    (status = 'pending' AND response IS NULL AND completed_at IS NULL) OR
    (status IN ('completed', 'indeterminate') AND response IS NOT NULL AND completed_at IS NOT NULL)
  ),
  PRIMARY KEY (caller_id, task_id, tool_call_id)
);`

func OpenSQLiteLedger(ctx context.Context, dsn string) (*SQLiteLedger, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("caller ledger dsn is required")
	}
	ownerLock, err := acquireLedgerOwnerLock(dsn)
	if err != nil {
		return nil, err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner && ownerLock != nil {
			_ = ownerLock.Close()
		}
	}()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, err
	}
	if err := requireCurrentOrEmptyLedgerSchema(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, ledgerSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize caller tool ledger schema: %w", err)
	}
	ledger := &SQLiteLedger{db: db, now: time.Now, ownerLock: ownerLock}
	if err := ledger.recoverPending(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover caller tool ledger: %w", err)
	}
	releaseOwner = false
	return ledger, nil
}

func requireCurrentOrEmptyLedgerSchema(ctx context.Context, db *sql.DB) error {
	var tableCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
		return fmt.Errorf("inspect caller ledger schema: %w", err)
	}
	if tableCount == 0 {
		return nil
	}

	var version int
	var fingerprint string
	if err := db.QueryRowContext(ctx, `
		SELECT version, fingerprint
		FROM caller_ledger_schema
		WHERE component = 'caller-ledger'`).Scan(&version, &fingerprint); err != nil {
		return fmt.Errorf("%w: missing caller-ledger schema marker", errUnsupportedLedgerSchema)
	}
	if version != ledgerSchemaVersion || fingerprint != ledgerSchemaFingerprint {
		return fmt.Errorf(
			"%w: caller-ledger schema version %d (%q), want %d (%q)",
			errUnsupportedLedgerSchema,
			version, fingerprint, ledgerSchemaVersion, ledgerSchemaFingerprint,
		)
	}
	return nil
}

func (ledger *SQLiteLedger) Close() error {
	ledger.closeOnce.Do(func() {
		// Keep ownership until every database operation has drained. Releasing the
		// lock first would let a new process run recoverPending against work that
		// this owner was still committing.
		ledger.closeErr = ledger.db.Close()
		if ledger.ownerLock != nil {
			ledger.closeErr = errors.Join(ledger.closeErr, ledger.ownerLock.Close())
		}
	})
	return ledger.closeErr
}

func (ledger *SQLiteLedger) Begin(ctx context.Context, key ExecutionKey, digest string) (BeginResult, error) {
	if key.CallerID == "" || key.TaskID == "" || key.ToolCallID == "" || digest == "" {
		return BeginResult{}, errors.New("caller, task, tool call, and digest are required")
	}
	tx, err := ledger.db.BeginTx(ctx, nil)
	if err != nil {
		return BeginResult{}, err
	}
	defer tx.Rollback()
	existing, err := getExecution(ctx, tx, key)
	if err == nil {
		if existing.RequestDigest != digest {
			return BeginResult{}, ErrExecutionReplay
		}
		if err := tx.Commit(); err != nil {
			return BeginResult{}, err
		}
		return BeginResult{Execution: existing, Replay: true}, nil
	}
	if !errors.Is(err, ErrLedgerNotFound) {
		return BeginResult{}, err
	}
	now := ledger.now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO caller_tool_executions (
		  caller_id, task_id, tool_call_id, request_digest, status, created_at
		) VALUES (?, ?, ?, ?, 'pending', ?)`, key.CallerID, key.TaskID, key.ToolCallID, digest, now.UnixNano()); err != nil {
		return BeginResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return BeginResult{}, err
	}
	return BeginResult{Execution: Execution{ExecutionKey: key, RequestDigest: digest, Status: "pending", CreatedAt: now}}, nil
}

func (ledger *SQLiteLedger) Complete(ctx context.Context, key ExecutionKey, response []byte) (Execution, error) {
	tx, err := ledger.db.BeginTx(ctx, nil)
	if err != nil {
		return Execution{}, err
	}
	defer tx.Rollback()
	execution, err := getExecution(ctx, tx, key)
	if err != nil {
		return Execution{}, err
	}
	if execution.Status == "completed" {
		if !bytes.Equal(execution.Response, response) {
			return Execution{}, ErrExecutionReplay
		}
		if err := tx.Commit(); err != nil {
			return Execution{}, err
		}
		return execution, nil
	}
	if execution.Status == "indeterminate" {
		return Execution{}, ErrExecutionReplay
	}
	now := ledger.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE caller_tool_executions
		SET status = 'completed', response = ?, completed_at = ?
		WHERE caller_id = ? AND task_id = ? AND tool_call_id = ? AND status = 'pending'`,
		response, now.UnixNano(), key.CallerID, key.TaskID, key.ToolCallID)
	if err != nil {
		return Execution{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		if err != nil {
			return Execution{}, err
		}
		return Execution{}, ErrExecutionReplay
	}
	if err := tx.Commit(); err != nil {
		return Execution{}, err
	}
	execution.Status = "completed"
	execution.Response = bytes.Clone(response)
	execution.CompletedAt = &now
	return execution, nil
}

func (ledger *SQLiteLedger) MarkIndeterminate(
	ctx context.Context,
	key ExecutionKey,
	response []byte,
) (Execution, error) {
	if len(response) == 0 {
		return Execution{}, errors.New("indeterminate execution response is required")
	}
	tx, err := ledger.db.BeginTx(ctx, nil)
	if err != nil {
		return Execution{}, err
	}
	defer tx.Rollback()
	execution, err := getExecution(ctx, tx, key)
	if err != nil {
		return Execution{}, err
	}
	if execution.Status != "pending" {
		if err := tx.Commit(); err != nil {
			return Execution{}, err
		}
		return execution, nil
	}
	now := ledger.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE caller_tool_executions
		SET status = 'indeterminate', response = ?, completed_at = ?
		WHERE caller_id = ? AND task_id = ? AND tool_call_id = ? AND status = 'pending'`,
		response, now.UnixNano(), key.CallerID, key.TaskID, key.ToolCallID)
	if err != nil {
		return Execution{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Execution{}, err
	}
	if rows != 1 {
		return Execution{}, ErrExecutionReplay
	}
	if err := tx.Commit(); err != nil {
		return Execution{}, err
	}
	execution.Status = "indeterminate"
	execution.Response = bytes.Clone(response)
	execution.CompletedAt = &now
	return execution, nil
}

// recoverPending converts executions owned by a previous shim process into a
// replayable terminal result. Retrying them automatically would be unsafe: the
// process may have changed the workspace and crashed before recording the
// response. OpenSQLiteLedger is the single-owner process boundary for a ledger.
func (ledger *SQLiteLedger) recoverPending(ctx context.Context) error {
	response, err := json.Marshal(indeterminateToolResponse())
	if err != nil {
		return fmt.Errorf("encode indeterminate execution response: %w", err)
	}
	_, err = ledger.db.ExecContext(ctx, `
		UPDATE caller_tool_executions
		SET status = 'indeterminate', response = ?, completed_at = ?
		WHERE status = 'pending'`, response, ledger.now().UTC().UnixNano())
	return err
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getExecution(ctx context.Context, db rowQueryer, key ExecutionKey) (Execution, error) {
	var execution Execution
	var createdAt int64
	var completedAt sql.NullInt64
	err := db.QueryRowContext(ctx, `
		SELECT request_digest, status, response, created_at, completed_at
		FROM caller_tool_executions
		WHERE caller_id = ? AND task_id = ? AND tool_call_id = ?`,
		key.CallerID, key.TaskID, key.ToolCallID).Scan(
		&execution.RequestDigest, &execution.Status, &execution.Response, &createdAt, &completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Execution{}, ErrLedgerNotFound
	}
	if err != nil {
		return Execution{}, err
	}
	execution.ExecutionKey = key
	execution.CreatedAt = time.Unix(0, createdAt).UTC()
	if completedAt.Valid {
		value := time.Unix(0, completedAt.Int64).UTC()
		execution.CompletedAt = &value
	}
	return execution, nil
}
