package workerclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"github.com/vibe-agi/human/internal/workerproto"
	_ "modernc.org/sqlite"
)

var (
	errUnsupportedOutboxSchema = errors.New("unsupported worker outbox schema; recreate outbox database")
	errOutboxConflict          = errors.New("worker outbox event id conflicts with a different payload")
	errOutboxIdentityConflict  = errors.New("worker outbox is bound to a different authenticated worker identity")
	errRejectedConflict        = errors.New("worker rejected event id conflicts with different durable content")
	errRejectedUnknown         = errors.New("worker rejected event is not durable locally")
	errRejectedNotSent         = errors.New("worker rejected event was not sent on this connection")
	errRejectedAckBehind       = errors.New("worker rejection ACK is behind the rejected event sequence")
	// ErrEventQuarantined reports that an event ID belongs to a corrupt durable
	// record which was isolated from delivery. The raw record remains available
	// for operator inspection and the ID must not be reused implicitly.
	ErrEventQuarantined = errors.New("worker event is quarantined after durable outbox corruption")
	// ErrEventPreviouslyRejected reports that an exact event is already in the
	// confirmed rejection tombstone, rather than the send outbox.
	ErrEventPreviouslyRejected = errors.New("worker event was already rejected")
	// ErrEventRejectionPending reports that the exact rejected event remains in
	// the durable inbox and will be offered through Client.Messages.
	ErrEventRejectionPending = errors.New("worker event rejection is pending local recovery")
)

const (
	outboxSchemaVersion     = 3
	outboxSchemaFingerprint = "human-worker-outbox-v3-20260718"
)

type outboxRecord struct {
	EventID    string
	TaskID     string
	Assignment completion.Assignment
	Message    workerproto.Event
	CreatedAt  time.Time
}

type rejectedRecord struct {
	InboxSequence int64
	EventID       string
	TaskID        string
	Assignment    completion.Assignment
	Message       workerproto.Event
	Rejection     workerproto.EventRejected
	CreatedAt     time.Time
	RejectedAt    time.Time
}

// OutboxQuarantine is a payload-free operator notice. Corrupt raw rows are
// retained in worker_outbox_quarantine, while healthy rows continue to send.
// EventIDs is deliberately bounded and contains only durable opaque IDs.
type OutboxQuarantine struct {
	Count    int
	EventIDs []string
	Path     string
}

type rawOutboxRecord struct {
	eventID    string
	taskID     string
	assignment []byte
	payload    []byte
	createdAt  int64
}

type durableOutbox struct {
	db               *sql.DB
	endpointIdentity string
	bindMu           sync.Mutex
	workerID         string
	namespace        atomic.Pointer[string]
	path             string
	owner            *ownerlock.Lock
}

// openDurableOutbox opens the shared worker database. workerID may be empty
// until the gateway's authenticated hello arrives. A non-empty value is used
// by focused store tests and is bound through the same validation path.
func openDurableOutbox(ctx context.Context, path, endpoint, workerID string) (*durableOutbox, error) {
	return openDurableOutboxWithScope(ctx, path, endpoint, "", workerID)
}

func openDurableOutboxWithScope(
	ctx context.Context,
	path, endpoint, scope, workerID string,
) (*durableOutbox, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("worker outbox path is required")
	}
	endpointIdentity, err := GatewayIdentity(endpoint, scope)
	if err != nil {
		return nil, err
	}
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve worker outbox path: %w", err)
		}
		path = absolute
		directory := filepath.Dir(path)
		directoryInfo, statErr := os.Stat(directory)
		switch {
		case errors.Is(statErr, os.ErrNotExist):
			if err := os.MkdirAll(directory, 0o700); err != nil {
				return nil, fmt.Errorf("create worker outbox directory: %w", err)
			}
		case statErr != nil:
			return nil, fmt.Errorf("inspect worker outbox directory: %w", statErr)
		case !directoryInfo.IsDir():
			return nil, errors.New("worker outbox parent is not a directory")
		case runtime.GOOS != "windows" && directoryInfo.Mode().Perm()&0o022 != 0:
			return nil, errors.New("worker outbox directory must not be group- or world-writable")
		}
		// The outbox necessarily holds unacknowledged response text and tool
		// calls. Keep the database private; SQLite inherits this mode for its
		// transient rollback journal.
	}
	location, err := sqlitefile.PreparePrivate(path, "worker outbox database")
	if err != nil {
		return nil, err
	}
	owner, err := ownerlock.Acquire(location, "worker outbox database")
	if err != nil {
		return nil, err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner && owner != nil {
			_ = owner.Close()
		}
	}()

	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		return nil, fmt.Errorf("open worker outbox: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode = DELETE",
		"PRAGMA synchronous = FULL",
		"PRAGMA secure_delete = ON",
		"PRAGMA busy_timeout = 5000",
	} {
		if _, err := database.ExecContext(ctx, pragma); err != nil {
			database.Close()
			return nil, fmt.Errorf("configure worker outbox: %w", err)
		}
	}
	if err := requireCurrentOrEmptyOutboxSchema(ctx, database); err != nil {
		database.Close()
		return nil, err
	}
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS worker_outbox_schema (
		  component TEXT PRIMARY KEY,
		  version INTEGER NOT NULL,
		  fingerprint TEXT NOT NULL
		);
		INSERT INTO worker_outbox_schema (component, version, fingerprint)
			VALUES ('outbox', 3, 'human-worker-outbox-v3-20260718')
		ON CONFLICT(component) DO NOTHING;
		CREATE TABLE IF NOT EXISTS worker_outbox (
		  namespace TEXT NOT NULL,
		  event_id TEXT NOT NULL,
		  task_id TEXT NOT NULL,
		  assignment BLOB NOT NULL,
		  payload BLOB NOT NULL,
		  created_at INTEGER NOT NULL,
		  PRIMARY KEY (namespace, event_id)
		);
		CREATE INDEX IF NOT EXISTS worker_outbox_created
		  ON worker_outbox(namespace, created_at, event_id);
		CREATE TABLE IF NOT EXISTS worker_outbox_quarantine (
		  namespace TEXT NOT NULL,
		  event_id TEXT NOT NULL,
		  task_id TEXT NOT NULL,
		  assignment BLOB NOT NULL,
		  payload BLOB NOT NULL,
		  created_at INTEGER NOT NULL,
		  quarantined_at INTEGER NOT NULL,
		  reason TEXT NOT NULL,
		  PRIMARY KEY (namespace, event_id)
		);
		CREATE INDEX IF NOT EXISTS worker_outbox_quarantine_created
		  ON worker_outbox_quarantine(namespace, quarantined_at, event_id);
		CREATE TABLE IF NOT EXISTS worker_rejected_inbox (
		  inbox_seq INTEGER PRIMARY KEY AUTOINCREMENT,
		  namespace TEXT NOT NULL,
		  event_id TEXT NOT NULL,
		  task_id TEXT NOT NULL,
		  assignment BLOB NOT NULL,
		  payload BLOB NOT NULL,
		  rejection BLOB NOT NULL,
		  created_at INTEGER NOT NULL,
		  rejected_at INTEGER NOT NULL,
		  UNIQUE (namespace, event_id)
		);
		CREATE INDEX IF NOT EXISTS worker_rejected_inbox_created
		  ON worker_rejected_inbox(namespace, inbox_seq);
		CREATE TABLE IF NOT EXISTS worker_rejected_confirmed (
		  namespace TEXT NOT NULL,
		  event_id TEXT NOT NULL,
		  event_digest TEXT NOT NULL,
		  assignment_digest TEXT NOT NULL,
		  rejection_digest TEXT NOT NULL,
		  confirmed_at INTEGER NOT NULL,
		  PRIMARY KEY (namespace, event_id)
		);`); err != nil {
		database.Close()
		return nil, fmt.Errorf("initialize worker outbox: %w", err)
	}
	outbox := &durableOutbox{db: database, endpointIdentity: endpointIdentity, path: path, owner: owner}
	if strings.TrimSpace(workerID) != "" {
		if err := outbox.bindWorker(workerID); err != nil {
			_ = database.Close()
			return nil, err
		}
	}
	releaseOwner = false
	return outbox, nil
}

// GatewayIdentity returns the token-free correctness identity shared by the
// worker outbox and TUI state. A logical scope is appropriate for an embedded
// gateway whose listener changes; otherwise the canonical worker endpoint is
// used. Callers must never reuse a logical scope for another gateway database.
func GatewayIdentity(endpoint, logicalScope string) (string, error) {
	if logicalScope = strings.TrimSpace(logicalScope); logicalScope != "" {
		if len(logicalScope) > 1024 {
			return "", errors.New("worker gateway scope exceeds 1024 bytes")
		}
		sum := sha256.Sum256([]byte(logicalScope))
		return "scope:" + hex.EncodeToString(sum[:]), nil
	}
	return canonicalWorkerEndpoint(endpoint)
}

func outboxNamespaceForIdentity(gatewayID, workerID string) (string, error) {
	gatewayID = strings.TrimSpace(gatewayID)
	workerID = strings.TrimSpace(workerID)
	if gatewayID == "" || !completion.IsStableKey(workerID) {
		return "", errors.New("outbox identity requires gateway identity and stable worker subject")
	}
	sum := sha256.Sum256([]byte(gatewayID + "\x00" + workerID))
	return hex.EncodeToString(sum[:]), nil
}

// bindWorker selects the only namespace this process may access. The worker
// identity comes from the gateway's authenticated hello, never from the token
// text or caller configuration. Consequently credential rotation preserves
// pending events for the same worker, while another gateway endpoint or worker
// subject receives a disjoint namespace in the same SQLite file.
func (outbox *durableOutbox) bindWorker(workerID string) error {
	workerID = strings.TrimSpace(workerID)
	if !completion.IsStableKey(workerID) {
		return errors.New("authenticated worker identity must be a stable key")
	}
	namespace, err := outboxNamespaceForIdentity(outbox.endpointIdentity, workerID)
	if err != nil {
		return err
	}
	outbox.bindMu.Lock()
	defer outbox.bindMu.Unlock()
	boundNamespace := outbox.namespace.Load()
	if boundNamespace != nil && (*boundNamespace != namespace || outbox.workerID != workerID) {
		return errOutboxIdentityConflict
	}
	outbox.workerID = workerID
	outbox.namespace.Store(&namespace)
	return nil
}

func (outbox *durableOutbox) isBound() bool {
	return outbox != nil && outbox.namespace.Load() != nil
}

func (outbox *durableOutbox) namespaceValue() string {
	if outbox == nil {
		return ""
	}
	namespace := outbox.namespace.Load()
	if namespace == nil {
		return ""
	}
	return *namespace
}

func requireCurrentOrEmptyOutboxSchema(ctx context.Context, database *sql.DB) error {
	var tableCount int
	if err := database.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
		return fmt.Errorf("inspect worker outbox schema: %w", err)
	}
	if tableCount == 0 {
		return nil
	}
	var version int
	var fingerprint string
	if err := database.QueryRowContext(ctx, `
		SELECT version, fingerprint
		FROM worker_outbox_schema
		WHERE component = 'outbox'`).Scan(&version, &fingerprint); err != nil {
		return fmt.Errorf("%w: missing outbox schema marker", errUnsupportedOutboxSchema)
	}
	if version != outboxSchemaVersion || fingerprint != outboxSchemaFingerprint {
		return fmt.Errorf(
			"%w: outbox schema version %d (%q), want %d (%q)",
			errUnsupportedOutboxSchema,
			version, fingerprint, outboxSchemaVersion, outboxSchemaFingerprint,
		)
	}
	return nil
}

func (outbox *durableOutbox) Put(
	ctx context.Context,
	assignment completion.Assignment,
	event completion.Event,
) (outboxRecord, error) {
	if strings.TrimSpace(assignment.CallerID) == "" || strings.TrimSpace(assignment.IdempotencyKey) == "" ||
		strings.TrimSpace(assignment.TaskID) == "" {
		return outboxRecord{}, fmt.Errorf("%w: worker event requires caller, task, and idempotency identity", ErrEventNotStored)
	}
	if strings.TrimSpace(event.ID) == "" || event.Type == "" {
		return outboxRecord{}, fmt.Errorf("%w: worker event requires id and type", ErrEventNotStored)
	}
	record := outboxRecord{
		EventID: event.ID, TaskID: assignment.TaskID,
		Assignment: durableAssignmentSnapshot(assignment),
		Message: workerproto.Event{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey, Event: event,
		},
		CreatedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(record.Message)
	if err != nil {
		return outboxRecord{}, fmt.Errorf("%w: marshal worker outbox event: %v", ErrEventNotStored, err)
	}
	assignmentPayload, err := json.Marshal(record.Assignment)
	if err != nil {
		return outboxRecord{}, fmt.Errorf("%w: marshal worker outbox assignment snapshot: %v", ErrEventNotStored, err)
	}
	tx, err := outbox.db.BeginTx(ctx, nil)
	if err != nil {
		return outboxRecord{}, fmt.Errorf("begin worker outbox transaction: %w", err)
	}
	defer tx.Rollback()
	var storedTask string
	var storedPayload []byte
	var storedAssignment []byte
	err = tx.QueryRowContext(ctx, `
		SELECT task_id, assignment, payload
		FROM worker_outbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), event.ID).
		Scan(&storedTask, &storedAssignment, &storedPayload)
	switch {
	case err == nil:
		if storedTask != assignment.TaskID || !bytes.Equal(storedPayload, payload) ||
			!bytes.Equal(storedAssignment, assignmentPayload) {
			return outboxRecord{}, fmt.Errorf("%w: %w", ErrEventNotStored, errOutboxConflict)
		}
		if err := tx.Commit(); err != nil {
			return outboxRecord{}, fmt.Errorf("commit worker outbox replay: %w", err)
		}
		return record, nil
	case !errors.Is(err, sql.ErrNoRows):
		return outboxRecord{}, fmt.Errorf("inspect worker outbox event: %w", err)
	}
	var quarantined int
	err = tx.QueryRowContext(ctx, `
		SELECT 1
		FROM worker_outbox_quarantine
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), event.ID).Scan(&quarantined)
	switch {
	case err == nil:
		return outboxRecord{}, ErrEventQuarantined
	case !errors.Is(err, sql.ErrNoRows):
		return outboxRecord{}, fmt.Errorf("inspect quarantined worker event: %w", err)
	}
	err = tx.QueryRowContext(ctx, `
			SELECT task_id, assignment, payload
		FROM worker_rejected_inbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), event.ID).
		Scan(&storedTask, &storedAssignment, &storedPayload)
	switch {
	case err == nil:
		// An exact event which has already been rejected is still durably
		// resolved. Treat a local retry as idempotent without putting it back in
		// the ordinary outbox and sending it to the gateway again.
		if storedTask != assignment.TaskID || !bytes.Equal(storedPayload, payload) ||
			!bytes.Equal(storedAssignment, assignmentPayload) {
			return outboxRecord{}, fmt.Errorf("%w: %w", ErrEventNotStored, errOutboxConflict)
		}
		if err := tx.Commit(); err != nil {
			return outboxRecord{}, fmt.Errorf("commit worker rejected event replay: %w", err)
		}
		return record, ErrEventRejectionPending
	case !errors.Is(err, sql.ErrNoRows):
		return outboxRecord{}, fmt.Errorf("inspect worker rejected event: %w", err)
	}
	var confirmedEventDigest string
	var confirmedAssignmentDigest string
	err = tx.QueryRowContext(ctx, `
		SELECT event_digest, assignment_digest
		FROM worker_rejected_confirmed
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), event.ID).
		Scan(&confirmedEventDigest, &confirmedAssignmentDigest)
	switch {
	case err == nil:
		if confirmedEventDigest != digestHex(payload) ||
			confirmedAssignmentDigest != digestHex(assignmentPayload) {
			return outboxRecord{}, fmt.Errorf("%w: %w", ErrEventNotStored, errOutboxConflict)
		}
		if err := tx.Commit(); err != nil {
			return outboxRecord{}, fmt.Errorf("commit confirmed worker rejection replay: %w", err)
		}
		return record, ErrEventPreviouslyRejected
	case !errors.Is(err, sql.ErrNoRows):
		return outboxRecord{}, fmt.Errorf("inspect confirmed worker rejection: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO worker_outbox (namespace, event_id, task_id, assignment, payload, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`, outbox.namespaceValue(), event.ID, assignment.TaskID,
		assignmentPayload, payload, record.CreatedAt.UnixNano()); err != nil {
		return outboxRecord{}, fmt.Errorf("persist worker outbox event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return outboxRecord{}, fmt.Errorf("commit worker outbox event: %w", err)
	}
	return record, nil
}

// durableAssignmentSnapshot keeps the routing/capability and tool context
// needed to restore rejected task, command, and mirror drafts, without copying
// the conversation transcript or arbitrary request metadata into the local
// retry database.
func durableAssignmentSnapshot(assignment completion.Assignment) completion.Assignment {
	snapshot := assignment
	snapshot.Request.System = ""
	snapshot.Request.Messages = nil
	snapshot.Request.Metadata = nil
	return snapshot
}

func (outbox *durableOutbox) List(ctx context.Context) ([]outboxRecord, error) {
	rows, err := outbox.db.QueryContext(ctx, `
			SELECT event_id, task_id, assignment, payload, created_at
		FROM worker_outbox
		WHERE namespace = ?
		ORDER BY created_at, event_id`, outbox.namespaceValue())
	if err != nil {
		return nil, fmt.Errorf("list worker outbox events: %w", err)
	}
	var rawRecords []rawOutboxRecord
	for rows.Next() {
		var record rawOutboxRecord
		if err := rows.Scan(
			&record.eventID, &record.taskID, &record.assignment, &record.payload, &record.createdAt,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan worker outbox event: %w", err)
		}
		rawRecords = append(rawRecords, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate worker outbox events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close worker outbox rows: %w", err)
	}

	// Decode only after closing the query. The outbox intentionally uses one
	// SQLite connection, so trying to move a corrupt row while rows is open would
	// deadlock behind that same connection. Every move below is one transaction:
	// a crash leaves the row either sendable or quarantined, never silently gone.
	records := make([]outboxRecord, 0, len(rawRecords))
	for _, raw := range rawRecords {
		record := outboxRecord{EventID: raw.eventID, TaskID: raw.taskID}
		if err := decodeOutboxRecord(
			&record, raw.assignment, raw.payload, raw.createdAt,
		); err != nil {
			if quarantineErr := outbox.quarantine(ctx, raw, err); quarantineErr != nil {
				return records, quarantineErr
			}
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

func (outbox *durableOutbox) quarantine(
	ctx context.Context,
	record rawOutboxRecord,
	decodeErr error,
) error {
	tx, err := outbox.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin worker outbox quarantine: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO worker_outbox_quarantine (
		  namespace, event_id, task_id, assignment, payload,
		  created_at, quarantined_at, reason
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		outbox.namespaceValue(), record.eventID, record.taskID, record.assignment, record.payload,
		record.createdAt, time.Now().UTC().UnixNano(), decodeErr.Error(),
	); err != nil {
		return fmt.Errorf("preserve corrupt worker outbox event %q: %w", record.eventID, err)
	}
	result, err := tx.ExecContext(ctx, `
		DELETE FROM worker_outbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), record.eventID)
	if err != nil {
		return fmt.Errorf("isolate corrupt worker outbox event %q: %w", record.eventID, err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify corrupt worker outbox isolation %q: %w", record.eventID, err)
	}
	if deleted != 1 {
		return fmt.Errorf("isolate corrupt worker outbox event %q: durable row changed concurrently", record.eventID)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit worker outbox quarantine %q: %w", record.eventID, err)
	}
	return nil
}

func (outbox *durableOutbox) QuarantineSummary(ctx context.Context) (OutboxQuarantine, error) {
	var summary OutboxQuarantine
	summary.Path = outbox.path
	if err := outbox.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM worker_outbox_quarantine
		WHERE namespace = ?`, outbox.namespaceValue()).Scan(&summary.Count); err != nil {
		return OutboxQuarantine{}, fmt.Errorf("count quarantined worker outbox events: %w", err)
	}
	if summary.Count == 0 {
		return summary, nil
	}
	rows, err := outbox.db.QueryContext(ctx, `
		SELECT event_id
		FROM worker_outbox_quarantine
		WHERE namespace = ?
		ORDER BY quarantined_at, event_id
		LIMIT 8`, outbox.namespaceValue())
	if err != nil {
		return OutboxQuarantine{}, fmt.Errorf("list quarantined worker outbox events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return OutboxQuarantine{}, fmt.Errorf("scan quarantined worker outbox event: %w", err)
		}
		summary.EventIDs = append(summary.EventIDs, eventID)
	}
	if err := rows.Err(); err != nil {
		return OutboxQuarantine{}, fmt.Errorf("iterate quarantined worker outbox events: %w", err)
	}
	return summary, nil
}

// Lookup returns the exact payload while an event remains in the send outbox.
func (outbox *durableOutbox) Lookup(ctx context.Context, eventID string) (outboxRecord, bool, error) {
	if strings.TrimSpace(eventID) == "" {
		return outboxRecord{}, false, errors.New("worker outbox event id is required")
	}
	var record outboxRecord
	var assignment []byte
	var payload []byte
	var createdAt int64
	err := outbox.db.QueryRowContext(ctx, `
		SELECT event_id, task_id, assignment, payload, created_at
		FROM worker_outbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), eventID).
		Scan(&record.EventID, &record.TaskID, &assignment, &payload, &createdAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return outboxRecord{}, false, nil
	case err != nil:
		return outboxRecord{}, false, fmt.Errorf("look up worker outbox event: %w", err)
	}
	if err := decodeOutboxRecord(&record, assignment, payload, createdAt); err != nil {
		return outboxRecord{}, false, err
	}
	return record, true, nil
}

func decodeOutboxRecord(
	record *outboxRecord,
	assignmentPayload []byte,
	payload []byte,
	createdAt int64,
) error {
	if err := json.Unmarshal(payload, &record.Message); err != nil {
		return fmt.Errorf("decode worker outbox event %q: %w", record.EventID, err)
	}
	if err := json.Unmarshal(assignmentPayload, &record.Assignment); err != nil {
		return fmt.Errorf("decode worker outbox assignment %q: %w", record.EventID, err)
	}
	if record.Message.Event.ID != record.EventID || record.Message.CallerID == "" ||
		record.Message.IdempotencyKey == "" || record.TaskID == "" ||
		record.Assignment.CallerID != record.Message.CallerID ||
		record.Assignment.IdempotencyKey != record.Message.IdempotencyKey ||
		record.Assignment.TaskID != record.TaskID {
		return fmt.Errorf("worker outbox event %q has invalid durable identity", record.EventID)
	}
	record.CreatedAt = time.Unix(0, createdAt).UTC()
	return nil
}

func (outbox *durableOutbox) Delete(ctx context.Context, eventID string) error {
	return outbox.DeleteMany(ctx, []string{eventID})
}

// DeleteMany atomically removes one cumulative-ACK batch so durable rows and
// the caller's in-memory inflight map can advance as a single logical step.
func (outbox *durableOutbox) DeleteMany(ctx context.Context, eventIDs []string) error {
	unique, err := uniqueEventIDs(eventIDs)
	if err != nil || len(unique) == 0 {
		return err
	}
	tx, err := outbox.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin acknowledged worker outbox delete: %w", err)
	}
	defer tx.Rollback()
	if err := outbox.deleteOutboxRows(ctx, tx, unique); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit acknowledged worker outbox delete: %w", err)
	}
	return nil
}

// RejectAndAcknowledge atomically moves one rejected event out of the send
// outbox and applies every cumulative ACK carried by the same server frame.
// The original payload therefore remains durable even if the process crashes
// immediately after commit, while an aborted transaction leaves every outbox
// row available for an exact retry.
func (outbox *durableOutbox) RejectAndAcknowledge(
	ctx context.Context,
	rejection workerproto.EventRejected,
	acknowledgedEventIDs []string,
	allowOutboxMove bool,
) (rejectedRecord, bool, error) {
	if strings.TrimSpace(rejection.CallerID) == "" ||
		strings.TrimSpace(rejection.IdempotencyKey) == "" ||
		strings.TrimSpace(rejection.EventID) == "" {
		return rejectedRecord{}, false, errors.New("invalid worker event rejection")
	}
	acknowledged, err := uniqueEventIDs(acknowledgedEventIDs)
	if err != nil {
		return rejectedRecord{}, false, err
	}
	rejectionPayload, err := json.Marshal(rejection)
	if err != nil {
		return rejectedRecord{}, false, fmt.Errorf("marshal worker event rejection: %w", err)
	}
	tx, err := outbox.db.BeginTx(ctx, nil)
	if err != nil {
		return rejectedRecord{}, false, fmt.Errorf("begin worker rejection transaction: %w", err)
	}
	defer tx.Rollback()
	acknowledged = append(acknowledged, rejection.EventID)
	acknowledged, err = uniqueEventIDs(acknowledged)
	if err != nil {
		return rejectedRecord{}, false, err
	}

	var confirmedEventDigest string
	var confirmedAssignmentDigest string
	var confirmedRejectionDigest string
	err = tx.QueryRowContext(ctx, `
		SELECT event_digest, assignment_digest, rejection_digest
		FROM worker_rejected_confirmed
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), rejection.EventID).
		Scan(&confirmedEventDigest, &confirmedAssignmentDigest, &confirmedRejectionDigest)
	switch {
	case err == nil:
		if confirmedRejectionDigest != digestHex(rejectionPayload) {
			return rejectedRecord{}, false, errRejectedConflict
		}
		current, exists, lookupErr := outbox.lookupOutboxTx(ctx, tx, rejection.EventID)
		if lookupErr != nil {
			return rejectedRecord{}, false, lookupErr
		}
		if exists {
			payload, marshalErr := json.Marshal(current.Message)
			if marshalErr != nil {
				return rejectedRecord{}, false, fmt.Errorf("marshal tombstoned worker outbox event: %w", marshalErr)
			}
			if confirmedEventDigest != digestHex(payload) {
				return rejectedRecord{}, false, errRejectedConflict
			}
			assignmentPayload, marshalErr := json.Marshal(current.Assignment)
			if marshalErr != nil {
				return rejectedRecord{}, false, fmt.Errorf("marshal tombstoned worker assignment: %w", marshalErr)
			}
			if confirmedAssignmentDigest != digestHex(assignmentPayload) {
				return rejectedRecord{}, false, errRejectedConflict
			}
		}
		// A confirmed tombstone is intentionally payload-free. It prevents a
		// lost protocol ACK from recreating the inbox while the cumulative ACK
		// still resolves any other events carried by this frame.
		if err := outbox.deleteOutboxRows(ctx, tx, acknowledged); err != nil {
			return rejectedRecord{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return rejectedRecord{}, false, fmt.Errorf("commit confirmed worker rejection replay: %w", err)
		}
		return rejectedRecord{}, false, nil
	case !errors.Is(err, sql.ErrNoRows):
		return rejectedRecord{}, false, fmt.Errorf("inspect confirmed worker rejection: %w", err)
	}

	record, found, err := outbox.lookupRejectedTx(ctx, tx, rejection.EventID)
	if err != nil {
		return rejectedRecord{}, false, err
	}
	if found {
		storedRejection, marshalErr := json.Marshal(record.Rejection)
		if marshalErr != nil {
			return rejectedRecord{}, false, fmt.Errorf("marshal durable worker event rejection: %w", marshalErr)
		}
		if !bytes.Equal(storedRejection, rejectionPayload) {
			return rejectedRecord{}, false, errRejectedConflict
		}
		if err := outbox.deleteOutboxRows(ctx, tx, acknowledged); err != nil {
			return rejectedRecord{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return rejectedRecord{}, false, fmt.Errorf("commit duplicate worker rejection: %w", err)
		}
		return record, true, nil
	}

	original, found, err := outbox.lookupOutboxTx(ctx, tx, rejection.EventID)
	if err != nil {
		return rejectedRecord{}, false, err
	}
	if !found {
		return rejectedRecord{}, false, errRejectedUnknown
	}
	if !allowOutboxMove {
		return rejectedRecord{}, false, errRejectedNotSent
	}
	if original.Message.CallerID != rejection.CallerID ||
		original.Message.IdempotencyKey != rejection.IdempotencyKey {
		return rejectedRecord{}, false, errors.New("worker event rejection identity does not match durable outbox")
	}
	rejectedAt := time.Now().UTC()
	payload, marshalErr := json.Marshal(original.Message)
	if marshalErr != nil {
		return rejectedRecord{}, false, fmt.Errorf("marshal rejected worker outbox event: %w", marshalErr)
	}
	assignmentPayload, marshalErr := json.Marshal(original.Assignment)
	if marshalErr != nil {
		return rejectedRecord{}, false, fmt.Errorf("marshal rejected worker assignment: %w", marshalErr)
	}
	result, err := tx.ExecContext(ctx, `
			INSERT INTO worker_rejected_inbox (
			  namespace, event_id, task_id, assignment, payload, rejection, created_at, rejected_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		outbox.namespaceValue(), original.EventID, original.TaskID, assignmentPayload, payload, rejectionPayload,
		original.CreatedAt.UnixNano(), rejectedAt.UnixNano(),
	)
	if err != nil {
		return rejectedRecord{}, false, fmt.Errorf("persist rejected worker event: %w", err)
	}
	inboxSequence, err := result.LastInsertId()
	if err != nil {
		return rejectedRecord{}, false, fmt.Errorf("read rejected worker inbox sequence: %w", err)
	}
	record = rejectedRecord{
		InboxSequence: inboxSequence, EventID: original.EventID, TaskID: original.TaskID,
		Assignment: original.Assignment, Message: original.Message,
		Rejection: rejection, CreatedAt: original.CreatedAt, RejectedAt: rejectedAt,
	}

	// The target must leave the send outbox even when a future protocol peer
	// sends event_rejected without including it in the cumulative ACK. Deduping
	// keeps the delete batch deterministic when it is already acknowledged.
	if err := outbox.deleteOutboxRows(ctx, tx, acknowledged); err != nil {
		return rejectedRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return rejectedRecord{}, false, fmt.Errorf("commit worker rejection transaction: %w", err)
	}
	return record, true, nil
}

func (outbox *durableOutbox) ListRejected(ctx context.Context) ([]rejectedRecord, error) {
	rows, err := outbox.db.QueryContext(ctx, `
		SELECT inbox_seq, event_id, task_id, assignment, payload, rejection, created_at, rejected_at
		FROM worker_rejected_inbox
		WHERE namespace = ?
		ORDER BY inbox_seq`, outbox.namespaceValue())
	if err != nil {
		return nil, fmt.Errorf("list rejected worker events: %w", err)
	}
	defer rows.Close()
	var records []rejectedRecord
	for rows.Next() {
		record, err := scanRejectedRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rejected worker events: %w", err)
	}
	return records, nil
}

// OldestRejected materializes only the one correctness-bearing row the TUI
// can currently consume. A later corrupt or oversized row must not block a
// healthy queue head, and an unconfirmed head must not force a full-payload
// backlog scan on every dispatcher tick.
func (outbox *durableOutbox) OldestRejected(
	ctx context.Context,
) (rejectedRecord, bool, error) {
	row := outbox.db.QueryRowContext(ctx, `
		SELECT inbox_seq, event_id, task_id, assignment, payload, rejection, created_at, rejected_at
		FROM worker_rejected_inbox
		WHERE namespace = ?
		ORDER BY inbox_seq
		LIMIT 1`, outbox.namespaceValue())
	record, err := scanRejectedRecord(row)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return rejectedRecord{}, false, nil
	case err != nil:
		return rejectedRecord{}, false, err
	default:
		return record, true, nil
	}
}

func (outbox *durableOutbox) DeleteRejected(ctx context.Context, eventID string) error {
	if strings.TrimSpace(eventID) == "" {
		return errors.New("rejected worker event id is required")
	}
	tx, err := outbox.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin confirmed worker rejection transaction: %w", err)
	}
	defer tx.Rollback()
	record, found, err := outbox.lookupRejectedTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if !found {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit absent worker rejection confirmation: %w", err)
		}
		return nil
	}
	eventPayload, err := json.Marshal(record.Message)
	if err != nil {
		return fmt.Errorf("marshal confirmed rejected worker event: %w", err)
	}
	rejectionPayload, err := json.Marshal(record.Rejection)
	if err != nil {
		return fmt.Errorf("marshal confirmed worker rejection: %w", err)
	}
	eventDigest := digestHex(eventPayload)
	assignmentPayload, err := json.Marshal(record.Assignment)
	if err != nil {
		return fmt.Errorf("marshal confirmed rejected worker assignment: %w", err)
	}
	assignmentDigest := digestHex(assignmentPayload)
	rejectionDigest := digestHex(rejectionPayload)
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO worker_rejected_confirmed (
		  namespace, event_id, event_digest, assignment_digest, rejection_digest, confirmed_at
		) VALUES (?, ?, ?, ?, ?, ?)`, outbox.namespaceValue(), eventID, eventDigest,
		assignmentDigest, rejectionDigest, time.Now().UTC().UnixNano()); err != nil {
		return fmt.Errorf("persist confirmed worker rejection tombstone: %w", err)
	}
	var storedEventDigest string
	var storedAssignmentDigest string
	var storedRejectionDigest string
	if err := tx.QueryRowContext(ctx, `
		SELECT event_digest, assignment_digest, rejection_digest
		FROM worker_rejected_confirmed
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), eventID).
		Scan(&storedEventDigest, &storedAssignmentDigest, &storedRejectionDigest); err != nil {
		return fmt.Errorf("verify confirmed worker rejection tombstone: %w", err)
	}
	if storedEventDigest != eventDigest || storedAssignmentDigest != assignmentDigest ||
		storedRejectionDigest != rejectionDigest {
		return errRejectedConflict
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM worker_rejected_inbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), eventID); err != nil {
		return fmt.Errorf("delete confirmed worker rejection: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit confirmed worker rejection: %w", err)
	}
	return nil
}

func (outbox *durableOutbox) lookupOutboxTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
) (outboxRecord, bool, error) {
	var record outboxRecord
	var assignment []byte
	var payload []byte
	var createdAt int64
	err := tx.QueryRowContext(ctx, `
		SELECT event_id, task_id, assignment, payload, created_at
		FROM worker_outbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), eventID).
		Scan(&record.EventID, &record.TaskID, &assignment, &payload, &createdAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return outboxRecord{}, false, nil
	case err != nil:
		return outboxRecord{}, false, fmt.Errorf("look up worker outbox event in rejection transaction: %w", err)
	}
	if err := decodeOutboxRecord(&record, assignment, payload, createdAt); err != nil {
		return outboxRecord{}, false, err
	}
	return record, true, nil
}

func (outbox *durableOutbox) lookupRejectedTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
) (rejectedRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT inbox_seq, event_id, task_id, assignment, payload, rejection, created_at, rejected_at
		FROM worker_rejected_inbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), eventID)
	record, err := scanRejectedRecord(row)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return rejectedRecord{}, false, nil
	case err != nil:
		return rejectedRecord{}, false, err
	default:
		return record, true, nil
	}
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRejectedRecord(row rowScanner) (rejectedRecord, error) {
	var record rejectedRecord
	var assignment []byte
	var payload []byte
	var rejection []byte
	var createdAt int64
	var rejectedAt int64
	if err := row.Scan(
		&record.InboxSequence, &record.EventID, &record.TaskID, &assignment,
		&payload, &rejection, &createdAt, &rejectedAt,
	); err != nil {
		return rejectedRecord{}, err
	}
	if err := json.Unmarshal(payload, &record.Message); err != nil {
		return rejectedRecord{}, fmt.Errorf("decode rejected worker event %q: %w", record.EventID, err)
	}
	if err := json.Unmarshal(rejection, &record.Rejection); err != nil {
		return rejectedRecord{}, fmt.Errorf("decode worker event rejection %q: %w", record.EventID, err)
	}
	if err := json.Unmarshal(assignment, &record.Assignment); err != nil {
		return rejectedRecord{}, fmt.Errorf("decode rejected worker assignment %q: %w", record.EventID, err)
	}
	if record.EventID == "" || record.TaskID == "" || record.Message.Event.ID != record.EventID ||
		record.Message.CallerID == "" || record.Message.IdempotencyKey == "" ||
		record.Assignment.CallerID != record.Message.CallerID ||
		record.Assignment.IdempotencyKey != record.Message.IdempotencyKey ||
		record.Assignment.TaskID != record.TaskID ||
		record.Rejection.EventID != record.EventID ||
		record.Rejection.CallerID != record.Message.CallerID ||
		record.Rejection.IdempotencyKey != record.Message.IdempotencyKey {
		return rejectedRecord{}, fmt.Errorf("rejected worker event %q has invalid durable identity", record.EventID)
	}
	record.CreatedAt = time.Unix(0, createdAt).UTC()
	record.RejectedAt = time.Unix(0, rejectedAt).UTC()
	return record, nil
}

func digestHex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func uniqueEventIDs(eventIDs []string) ([]string, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(eventIDs))
	unique := make([]string, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		if strings.TrimSpace(eventID) == "" {
			return nil, errors.New("worker outbox event id is required")
		}
		if _, duplicate := seen[eventID]; duplicate {
			continue
		}
		seen[eventID] = struct{}{}
		unique = append(unique, eventID)
	}
	return unique, nil
}

func (outbox *durableOutbox) deleteOutboxRows(ctx context.Context, tx *sql.Tx, eventIDs []string) error {
	for _, eventID := range eventIDs {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM worker_outbox
			WHERE namespace = ? AND event_id = ?`, outbox.namespaceValue(), eventID); err != nil {
			return fmt.Errorf("delete acknowledged worker outbox event: %w", err)
		}
	}
	return nil
}

// RebindOutboxIdentity validates and rewrites one offline outbox restored into
// a different logical gateway location. It acquires the normal owner lock and
// rejects mixed-identity archives; callers should invoke it on staging files
// before atomically replacing live data.
func RebindOutboxIdentity(ctx context.Context, path, oldGatewayID, newGatewayID, workerID string) error {
	if ctx == nil {
		return errors.New("rebind worker outbox: context is required")
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("inspect worker outbox for identity rebind: %w", err)
	}
	oldNamespace, err := outboxNamespaceForIdentity(oldGatewayID, workerID)
	if err != nil {
		return fmt.Errorf("validate old worker outbox identity: %w", err)
	}
	newNamespace, err := outboxNamespaceForIdentity(newGatewayID, workerID)
	if err != nil {
		return fmt.Errorf("validate new worker outbox identity: %w", err)
	}
	outbox, err := openDurableOutboxWithScope(
		ctx, path, "ws://127.0.0.1/internal/v1/worker/ws", "offline-outbox-identity-inspection", "",
	)
	if err != nil {
		return err
	}
	defer outbox.Close()
	tx, err := outbox.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin worker outbox identity rebind: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
		SELECT namespace FROM worker_outbox
		UNION SELECT namespace FROM worker_outbox_quarantine
		UNION SELECT namespace FROM worker_rejected_inbox
		UNION SELECT namespace FROM worker_rejected_confirmed`)
	if err != nil {
		return fmt.Errorf("inspect worker outbox identities: %w", err)
	}
	for rows.Next() {
		var namespace string
		if err := rows.Scan(&namespace); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan worker outbox identity: %w", err)
		}
		if namespace != oldNamespace {
			_ = rows.Close()
			return errors.New("worker outbox contains correctness rows for another gateway or worker identity")
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate worker outbox identities: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close worker outbox identity rows: %w", err)
	}
	if oldNamespace != newNamespace {
		for _, table := range []string{
			"worker_outbox", "worker_outbox_quarantine", "worker_rejected_inbox", "worker_rejected_confirmed",
		} {
			if _, err := tx.ExecContext(ctx,
				"UPDATE "+table+" SET namespace = ? WHERE namespace = ?",
				newNamespace, oldNamespace,
			); err != nil {
				return fmt.Errorf("rebind %s identity: %w", table, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit worker outbox identity rebind: %w", err)
	}
	return nil
}

func (outbox *durableOutbox) Close() error {
	if outbox == nil {
		return nil
	}
	return errors.Join(outbox.db.Close(), outbox.owner.Close())
}
