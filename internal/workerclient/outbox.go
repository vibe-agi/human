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
	"strings"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerproto"
	_ "modernc.org/sqlite"
)

var (
	errUnsupportedOutboxSchema = errors.New("unsupported worker outbox schema; recreate outbox database")
	errOutboxConflict          = errors.New("worker outbox event id conflicts with a different payload")
	errRejectedConflict        = errors.New("worker rejected event id conflicts with different durable content")
	errRejectedUnknown         = errors.New("worker rejected event is not durable locally")
	errRejectedNotSent         = errors.New("worker rejected event was not sent on this connection")
	errRejectedAckBehind       = errors.New("worker rejection ACK is behind the rejected event sequence")
	// ErrEventPreviouslyRejected reports that an exact event is already in the
	// confirmed rejection tombstone, rather than the send outbox.
	ErrEventPreviouslyRejected = errors.New("worker event was already rejected")
	// ErrEventRejectionPending reports that the exact rejected event remains in
	// the durable inbox and will be offered through Client.Messages.
	ErrEventRejectionPending = errors.New("worker event rejection is pending local recovery")
)

const (
	outboxSchemaVersion     = 1
	outboxSchemaFingerprint = "human-worker-outbox-v1-20260717"
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

type durableOutbox struct {
	db        *sql.DB
	namespace string
}

func openDurableOutbox(ctx context.Context, path, endpoint, token string) (*durableOutbox, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("worker outbox path is required")
	}
	if strings.TrimSpace(endpoint) == "" || strings.TrimSpace(token) == "" {
		return nil, errors.New("worker outbox endpoint and token are required")
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
		case directoryInfo.Mode().Perm()&0o022 != 0:
			return nil, errors.New("worker outbox directory must not be group- or world-writable")
		}
		// The outbox necessarily holds unacknowledged response text and tool
		// calls. Keep the database private; SQLite inherits this mode for its
		// transient rollback journal.
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, fmt.Errorf("create worker outbox database: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close worker outbox database file: %w", err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			return nil, fmt.Errorf("secure worker outbox database: %w", err)
		}
	}

	database, err := sql.Open("sqlite", path)
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
		VALUES ('outbox', 1, 'human-worker-outbox-v1-20260717')
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
	sum := sha256.Sum256([]byte(endpoint + "\x00" + token))
	return &durableOutbox{db: database, namespace: hex.EncodeToString(sum[:])}, nil
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
		return outboxRecord{}, errors.New("worker event requires caller, task, and idempotency identity")
	}
	if strings.TrimSpace(event.ID) == "" || event.Type == "" {
		return outboxRecord{}, errors.New("worker event requires id and type")
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
		return outboxRecord{}, fmt.Errorf("marshal worker outbox event: %w", err)
	}
	assignmentPayload, err := json.Marshal(record.Assignment)
	if err != nil {
		return outboxRecord{}, fmt.Errorf("marshal worker outbox assignment snapshot: %w", err)
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
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, event.ID).
		Scan(&storedTask, &storedAssignment, &storedPayload)
	switch {
	case err == nil:
		if storedTask != assignment.TaskID || !bytes.Equal(storedPayload, payload) ||
			!bytes.Equal(storedAssignment, assignmentPayload) {
			return outboxRecord{}, errOutboxConflict
		}
		if err := tx.Commit(); err != nil {
			return outboxRecord{}, fmt.Errorf("commit worker outbox replay: %w", err)
		}
		return record, nil
	case !errors.Is(err, sql.ErrNoRows):
		return outboxRecord{}, fmt.Errorf("inspect worker outbox event: %w", err)
	}
	err = tx.QueryRowContext(ctx, `
		SELECT task_id, assignment, payload
		FROM worker_rejected_inbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, event.ID).
		Scan(&storedTask, &storedAssignment, &storedPayload)
	switch {
	case err == nil:
		// An exact event which has already been rejected is still durably
		// resolved. Treat a local retry as idempotent without putting it back in
		// the ordinary outbox and sending it to the gateway again.
		if storedTask != assignment.TaskID || !bytes.Equal(storedPayload, payload) ||
			!bytes.Equal(storedAssignment, assignmentPayload) {
			return outboxRecord{}, errOutboxConflict
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
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, event.ID).
		Scan(&confirmedEventDigest, &confirmedAssignmentDigest)
	switch {
	case err == nil:
		if confirmedEventDigest != digestHex(payload) ||
			confirmedAssignmentDigest != digestHex(assignmentPayload) {
			return outboxRecord{}, errOutboxConflict
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
		VALUES (?, ?, ?, ?, ?, ?)`, outbox.namespace, event.ID, assignment.TaskID,
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
		ORDER BY created_at, event_id`, outbox.namespace)
	if err != nil {
		return nil, fmt.Errorf("list worker outbox events: %w", err)
	}
	defer rows.Close()
	var records []outboxRecord
	for rows.Next() {
		var record outboxRecord
		var assignment []byte
		var payload []byte
		var createdAt int64
		if err := rows.Scan(&record.EventID, &record.TaskID, &assignment, &payload, &createdAt); err != nil {
			return nil, fmt.Errorf("scan worker outbox event: %w", err)
		}
		if err := decodeOutboxRecord(&record, assignment, payload, createdAt); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate worker outbox events: %w", err)
	}
	return records, nil
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
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, eventID).
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
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, rejection.EventID).
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
		outbox.namespace, original.EventID, original.TaskID, assignmentPayload, payload, rejectionPayload,
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
		ORDER BY inbox_seq`, outbox.namespace)
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
		LIMIT 1`, outbox.namespace)
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
		) VALUES (?, ?, ?, ?, ?, ?)`, outbox.namespace, eventID, eventDigest,
		assignmentDigest, rejectionDigest, time.Now().UTC().UnixNano()); err != nil {
		return fmt.Errorf("persist confirmed worker rejection tombstone: %w", err)
	}
	var storedEventDigest string
	var storedAssignmentDigest string
	var storedRejectionDigest string
	if err := tx.QueryRowContext(ctx, `
		SELECT event_digest, assignment_digest, rejection_digest
		FROM worker_rejected_confirmed
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, eventID).
		Scan(&storedEventDigest, &storedAssignmentDigest, &storedRejectionDigest); err != nil {
		return fmt.Errorf("verify confirmed worker rejection tombstone: %w", err)
	}
	if storedEventDigest != eventDigest || storedAssignmentDigest != assignmentDigest ||
		storedRejectionDigest != rejectionDigest {
		return errRejectedConflict
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM worker_rejected_inbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, eventID); err != nil {
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
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, eventID).
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
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, eventID)
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
			WHERE namespace = ? AND event_id = ?`, outbox.namespace, eventID); err != nil {
			return fmt.Errorf("delete acknowledged worker outbox event: %w", err)
		}
	}
	return nil
}

func (outbox *durableOutbox) Close() error {
	return outbox.db.Close()
}
