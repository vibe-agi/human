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

var errOutboxConflict = errors.New("worker outbox event id conflicts with a different payload")

type outboxRecord struct {
	EventID   string
	TaskID    string
	Message   workerproto.Event
	CreatedAt time.Time
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
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS worker_outbox (
		  namespace TEXT NOT NULL,
		  event_id TEXT NOT NULL,
		  task_id TEXT NOT NULL,
		  payload BLOB NOT NULL,
		  created_at INTEGER NOT NULL,
		  PRIMARY KEY (namespace, event_id)
		);
		CREATE INDEX IF NOT EXISTS worker_outbox_created
		  ON worker_outbox(namespace, created_at, event_id);`); err != nil {
		database.Close()
		return nil, fmt.Errorf("migrate worker outbox: %w", err)
	}
	sum := sha256.Sum256([]byte(endpoint + "\x00" + token))
	return &durableOutbox{db: database, namespace: hex.EncodeToString(sum[:])}, nil
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
		EventID: event.ID,
		TaskID:  assignment.TaskID,
		Message: workerproto.Event{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey, Event: event,
		},
		CreatedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(record.Message)
	if err != nil {
		return outboxRecord{}, fmt.Errorf("marshal worker outbox event: %w", err)
	}
	tx, err := outbox.db.BeginTx(ctx, nil)
	if err != nil {
		return outboxRecord{}, fmt.Errorf("begin worker outbox transaction: %w", err)
	}
	defer tx.Rollback()
	var storedTask string
	var storedPayload []byte
	err = tx.QueryRowContext(ctx, `
		SELECT task_id, payload
		FROM worker_outbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, event.ID).Scan(&storedTask, &storedPayload)
	switch {
	case err == nil:
		if storedTask != assignment.TaskID || !bytes.Equal(storedPayload, payload) {
			return outboxRecord{}, errOutboxConflict
		}
		if err := tx.Commit(); err != nil {
			return outboxRecord{}, fmt.Errorf("commit worker outbox replay: %w", err)
		}
		return record, nil
	case !errors.Is(err, sql.ErrNoRows):
		return outboxRecord{}, fmt.Errorf("inspect worker outbox event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO worker_outbox (namespace, event_id, task_id, payload, created_at)
		VALUES (?, ?, ?, ?, ?)`, outbox.namespace, event.ID, assignment.TaskID, payload, record.CreatedAt.UnixNano()); err != nil {
		return outboxRecord{}, fmt.Errorf("persist worker outbox event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return outboxRecord{}, fmt.Errorf("commit worker outbox event: %w", err)
	}
	return record, nil
}

func (outbox *durableOutbox) List(ctx context.Context) ([]outboxRecord, error) {
	rows, err := outbox.db.QueryContext(ctx, `
		SELECT event_id, task_id, payload, created_at
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
		var payload []byte
		var createdAt int64
		if err := rows.Scan(&record.EventID, &record.TaskID, &payload, &createdAt); err != nil {
			return nil, fmt.Errorf("scan worker outbox event: %w", err)
		}
		if err := json.Unmarshal(payload, &record.Message); err != nil {
			return nil, fmt.Errorf("decode worker outbox event %q: %w", record.EventID, err)
		}
		if record.Message.Event.ID != record.EventID || record.Message.CallerID == "" ||
			record.Message.IdempotencyKey == "" || record.TaskID == "" {
			return nil, fmt.Errorf("worker outbox event %q has invalid durable identity", record.EventID)
		}
		record.CreatedAt = time.Unix(0, createdAt).UTC()
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate worker outbox events: %w", err)
	}
	return records, nil
}

func (outbox *durableOutbox) Delete(ctx context.Context, eventID string) error {
	if strings.TrimSpace(eventID) == "" {
		return errors.New("worker outbox event id is required")
	}
	if _, err := outbox.db.ExecContext(ctx, `
		DELETE FROM worker_outbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, eventID); err != nil {
		return fmt.Errorf("delete acknowledged worker outbox event: %w", err)
	}
	return nil
}

func (outbox *durableOutbox) Close() error {
	return outbox.db.Close()
}
