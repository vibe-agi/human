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

	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
	_ "modernc.org/sqlite"
)

type outboxRecord struct {
	EventID   string
	Digest    string
	Command   workerproto.Command
	CreatedAt time.Time
}

type durableOutbox struct {
	db        *sql.DB
	namespace string
}

func openDurableOutbox(ctx context.Context, path, endpoint, token string) (*durableOutbox, error) {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(endpoint) == "" || strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("%w: outbox path, endpoint, and token are required", delegation.ErrInvalidInput)
	}
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve delegation worker outbox path: %w", err)
		}
		path = absolute
		directory := filepath.Dir(path)
		info, err := os.Stat(directory)
		switch {
		case errors.Is(err, os.ErrNotExist):
			if err := os.MkdirAll(directory, 0o700); err != nil {
				return nil, fmt.Errorf("create delegation worker outbox directory: %w", err)
			}
		case err != nil:
			return nil, fmt.Errorf("inspect delegation worker outbox directory: %w", err)
		case !info.IsDir():
			return nil, errors.New("delegation worker outbox parent is not a directory")
		case info.Mode().Perm()&0o022 != 0:
			return nil, errors.New("delegation worker outbox directory must not be group- or world-writable")
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, fmt.Errorf("create delegation worker outbox: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close delegation worker outbox: %w", err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			return nil, fmt.Errorf("secure delegation worker outbox: %w", err)
		}
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open delegation worker outbox: %w", err)
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
			return nil, fmt.Errorf("configure delegation worker outbox: %w", err)
		}
	}
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS delegation_worker_outbox (
		  namespace TEXT NOT NULL,
		  event_id TEXT NOT NULL,
		  command_digest TEXT NOT NULL,
		  payload BLOB NOT NULL,
		  created_at INTEGER NOT NULL,
		  PRIMARY KEY (namespace, event_id)
		);
		CREATE INDEX IF NOT EXISTS delegation_worker_outbox_created
		  ON delegation_worker_outbox(namespace, created_at, event_id);`); err != nil {
		database.Close()
		return nil, fmt.Errorf("migrate delegation worker outbox: %w", err)
	}
	namespaceBytes := sha256.Sum256([]byte(endpoint + "\x00" + token))
	return &durableOutbox{db: database, namespace: hex.EncodeToString(namespaceBytes[:])}, nil
}

func (outbox *durableOutbox) Put(ctx context.Context, command workerproto.Command) (outboxRecord, error) {
	if err := command.Validate(); err != nil {
		return outboxRecord{}, err
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return outboxRecord{}, fmt.Errorf("encode delegation worker command: %w", err)
	}
	digestBytes := sha256.Sum256(payload)
	record := outboxRecord{
		EventID: command.EventID, Digest: hex.EncodeToString(digestBytes[:]), Command: command,
		CreatedAt: time.Now().UTC(),
	}
	tx, err := outbox.db.BeginTx(ctx, nil)
	if err != nil {
		return outboxRecord{}, fmt.Errorf("begin delegation worker outbox: %w", err)
	}
	defer tx.Rollback()
	var storedDigest string
	var storedPayload []byte
	err = tx.QueryRowContext(ctx, `
		SELECT command_digest, payload FROM delegation_worker_outbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, command.EventID).Scan(
		&storedDigest, &storedPayload)
	switch {
	case err == nil:
		if storedDigest != record.Digest || !bytes.Equal(storedPayload, payload) {
			return outboxRecord{}, fmt.Errorf("%w: worker command %q", delegation.ErrIdempotencyConflict, command.EventID)
		}
		if err := tx.Commit(); err != nil {
			return outboxRecord{}, fmt.Errorf("commit delegation worker outbox replay: %w", err)
		}
		return record, nil
	case !errors.Is(err, sql.ErrNoRows):
		return outboxRecord{}, fmt.Errorf("inspect delegation worker outbox: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO delegation_worker_outbox (
		  namespace, event_id, command_digest, payload, created_at
		) VALUES (?, ?, ?, ?, ?)`, outbox.namespace, command.EventID, record.Digest,
		payload, record.CreatedAt.UnixNano()); err != nil {
		return outboxRecord{}, fmt.Errorf("persist delegation worker command: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return outboxRecord{}, fmt.Errorf("commit delegation worker command: %w", err)
	}
	return record, nil
}

func (outbox *durableOutbox) List(ctx context.Context) ([]outboxRecord, error) {
	rows, err := outbox.db.QueryContext(ctx, `
		SELECT event_id, command_digest, payload, created_at
		FROM delegation_worker_outbox WHERE namespace = ?
		ORDER BY created_at, event_id`, outbox.namespace)
	if err != nil {
		return nil, fmt.Errorf("list delegation worker commands: %w", err)
	}
	defer rows.Close()
	var records []outboxRecord
	for rows.Next() {
		var record outboxRecord
		var payload []byte
		var createdAt int64
		if err := rows.Scan(&record.EventID, &record.Digest, &payload, &createdAt); err != nil {
			return nil, fmt.Errorf("scan delegation worker command: %w", err)
		}
		if err := json.Unmarshal(payload, &record.Command); err != nil {
			return nil, fmt.Errorf("decode delegation worker command %q: %w", record.EventID, err)
		}
		if record.Command.EventID != record.EventID || record.Command.Validate() != nil {
			return nil, fmt.Errorf("delegation worker outbox command %q has invalid identity", record.EventID)
		}
		digestBytes := sha256.Sum256(payload)
		if hex.EncodeToString(digestBytes[:]) != record.Digest {
			return nil, fmt.Errorf("delegation worker outbox command %q digest mismatch", record.EventID)
		}
		record.CreatedAt = time.Unix(0, createdAt).UTC()
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delegation worker commands: %w", err)
	}
	return records, nil
}

func (outbox *durableOutbox) Delete(ctx context.Context, eventID string) error {
	if strings.TrimSpace(eventID) == "" {
		return fmt.Errorf("%w: event id is required", delegation.ErrInvalidInput)
	}
	if _, err := outbox.db.ExecContext(ctx, `
		DELETE FROM delegation_worker_outbox
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, eventID); err != nil {
		return fmt.Errorf("delete completed delegation worker command: %w", err)
	}
	return nil
}

func (outbox *durableOutbox) Close() error { return outbox.db.Close() }
