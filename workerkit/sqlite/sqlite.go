// Package sqlite provides the official durable SQLite implementation of the
// workerkit StateStore port.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"github.com/vibe-agi/human/workerkit"
	_ "modernc.org/sqlite"
)

const (
	schemaVersion     = 1
	schemaFingerprint = "human-workerkit-state-v1-20260720a"
	databasePurpose   = "workerkit state database"

	// maxRecordBytes bounds one encoded conversation. Conversations are display
	// and recovery state, not payload storage; a record this large indicates a
	// runaway transcript rather than legitimate use.
	maxRecordBytes = 8 << 20
)

var (
	// ErrDatabaseInUse means another live process or Resource owns the same
	// file-backed state database.
	ErrDatabaseInUse = errors.New("workerkit state database is already held by another live owner")

	// ErrUnsupportedSchema rejects migration and accidental database sharing:
	// only an empty database or the exact current schema is accepted.
	ErrUnsupportedSchema = errors.New("unsupported workerkit state sqlite schema; recreate database")

	// ErrRecordTooLarge rejects a conversation whose encoded form exceeds the
	// at-rest budget.
	ErrRecordTooLarge = errors.New("workerkit conversation exceeds the state record limit")
)

// Config selects one dedicated state persistence identity.
type Config struct {
	Path string
}

// Open constructs an owned SQLite StateStore Resource. Path may be an
// ordinary filesystem path or an independent :memory: database. ctx bounds
// construction only.
func Open(ctx context.Context, config Config) (framework.Resource[workerkit.StateStore], error) {
	var zero framework.Resource[workerkit.StateStore]
	if ctx == nil {
		return zero, errors.New("open workerkit state sqlite: context is required")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	resolved, err := resolveDatabasePath(config.Path)
	if err != nil {
		return zero, err
	}
	location, err := sqlitefile.PreparePrivate(resolved, databasePurpose)
	if err != nil {
		return zero, err
	}
	owner, err := ownerlock.Acquire(location, databasePurpose)
	if err != nil {
		if errors.Is(err, ownerlock.ErrInUse) {
			return zero, fmt.Errorf("%w: %v", ErrDatabaseInUse, err)
		}
		return zero, err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner {
			_ = owner.Close()
		}
	}()

	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		return zero, fmt.Errorf("open workerkit state sqlite: %w", err)
	}
	closeDatabase := true
	defer func() {
		if closeDatabase {
			_ = database.Close()
		}
	}()
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if err := configureDatabase(ctx, database); err != nil {
		return zero, err
	}
	if err := requireCurrentOrEmptySchema(ctx, database); err != nil {
		return zero, err
	}
	if err := initializeSchema(ctx, database); err != nil {
		return zero, err
	}

	store := &store{database: database}
	resource, err := framework.Own[workerkit.StateStore](store, func(context.Context) error {
		store.lifecycle.Lock()
		defer store.lifecycle.Unlock()
		if store.closed {
			return nil
		}
		store.closed = true
		return errors.Join(database.Close(), owner.Close())
	})
	if err != nil {
		return zero, fmt.Errorf("own workerkit state sqlite Resource: %w", err)
	}
	closeDatabase = false
	releaseOwner = false
	return resource, nil
}

type store struct {
	database *sql.DB

	lifecycle sync.RWMutex
	closed    bool
}

var _ workerkit.StateStore = (*store)(nil)

func (store *store) acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("workerkit state operation requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.lifecycle.RLock()
	if store.closed {
		store.lifecycle.RUnlock()
		return nil, workerkit.ErrClosed
	}
	return store.lifecycle.RUnlock, nil
}

func (store *store) SaveConversation(ctx context.Context, conversation workerkit.Conversation) error {
	release, err := store.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	if conversation.Key.Caller == "" || conversation.Key.TaskID == "" {
		return fmt.Errorf("%w: conversation key requires caller and task", workerkit.ErrInvalidCommand)
	}
	record, err := json.Marshal(conversation)
	if err != nil {
		return fmt.Errorf("encode workerkit conversation: %w", err)
	}
	if len(record) > maxRecordBytes {
		return fmt.Errorf("%w: %d bytes", ErrRecordTooLarge, len(record))
	}
	if _, err := store.database.ExecContext(ctx, `
		INSERT INTO workerkit_conversations (caller, task, record)
		VALUES (?, ?, ?)
		ON CONFLICT(caller, task) DO UPDATE SET record = excluded.record`,
		string(conversation.Key.Caller), string(conversation.Key.TaskID), record,
	); err != nil {
		return fmt.Errorf("persist workerkit conversation: %w", err)
	}
	return nil
}

func (store *store) DeleteConversation(ctx context.Context, key workerkit.ConversationKey) error {
	release, err := store.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	if _, err := store.database.ExecContext(ctx, `
		DELETE FROM workerkit_conversations WHERE caller = ? AND task = ?`,
		string(key.Caller), string(key.TaskID),
	); err != nil {
		return fmt.Errorf("delete workerkit conversation: %w", err)
	}
	return nil
}

func (store *store) ListConversations(ctx context.Context) ([]workerkit.Conversation, error) {
	release, err := store.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	rows, err := store.database.QueryContext(ctx, `
		SELECT record FROM workerkit_conversations ORDER BY caller, task`)
	if err != nil {
		return nil, fmt.Errorf("list workerkit conversations: %w", err)
	}
	defer rows.Close()
	var conversations []workerkit.Conversation
	for rows.Next() {
		var record []byte
		if err := rows.Scan(&record); err != nil {
			return nil, fmt.Errorf("scan workerkit conversation: %w", err)
		}
		if len(record) > maxRecordBytes {
			return nil, fmt.Errorf("%w: stored record is %d bytes", ErrRecordTooLarge, len(record))
		}
		var conversation workerkit.Conversation
		if err := json.Unmarshal(record, &conversation); err != nil {
			return nil, fmt.Errorf("decode workerkit conversation: %w", err)
		}
		conversations = append(conversations, conversation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workerkit conversations: %w", err)
	}
	return conversations, nil
}

func configureDatabase(ctx context.Context, database *sql.DB) error {
	var journalMode string
	if err := database.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("configure workerkit state sqlite WAL: %w", err)
	}
	for _, pragma := range []string{
		"PRAGMA synchronous = FULL",
		"PRAGMA secure_delete = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := database.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configure workerkit state sqlite: %w", err)
		}
	}
	return nil
}

func resolveDatabasePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("workerkit state SQLite path is required")
	}
	if path == ":memory:" || strings.HasPrefix(strings.ToLower(path), "file:") {
		return path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve workerkit state database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", fmt.Errorf("create workerkit state database directory: %w", err)
	}
	return absolute, nil
}

func requireCurrentOrEmptySchema(ctx context.Context, database *sql.DB) error {
	rows, err := database.QueryContext(ctx, `
		SELECT name
		FROM sqlite_schema
		WHERE type IN ('table', 'index', 'view', 'trigger')
		  AND name NOT LIKE 'sqlite_%'
		ORDER BY name`)
	if err != nil {
		return fmt.Errorf("inspect workerkit state sqlite schema: %w", err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			rows.Close()
			return fmt.Errorf("inspect workerkit state sqlite table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("inspect workerkit state sqlite tables: %w", err)
	}
	if len(tables) == 0 {
		return nil
	}
	expected := []string{"workerkit_state_meta", "workerkit_conversations"}
	sort.Strings(expected)
	if len(tables) != len(expected) {
		return unsupportedSchema("tables %v, want %v", tables, expected)
	}
	for index := range expected {
		if tables[index] != expected[index] {
			return unsupportedSchema("tables %v, want %v", tables, expected)
		}
	}
	var version int
	var fingerprint string
	if err := database.QueryRowContext(ctx, `
		SELECT schema_version, schema_fingerprint
		FROM workerkit_state_meta
		WHERE singleton = 1`).Scan(&version, &fingerprint); err != nil {
		return unsupportedSchema("missing schema marker: %v", err)
	}
	if version != schemaVersion || fingerprint != schemaFingerprint {
		return unsupportedSchema(
			"version %d (%q), want %d (%q)",
			version, fingerprint, schemaVersion, schemaFingerprint,
		)
	}
	return nil
}

func initializeSchema(ctx context.Context, database *sql.DB) error {
	tx, err := database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin workerkit state schema transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, stateSchema); err != nil {
		return fmt.Errorf("initialize workerkit state sqlite schema: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workerkit state sqlite schema: %w", err)
	}
	return nil
}

func unsupportedSchema(format string, arguments ...any) error {
	return errors.Join(ErrUnsupportedSchema, fmt.Errorf(format, arguments...))
}

const stateSchema = `
CREATE TABLE IF NOT EXISTS workerkit_state_meta (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  schema_version INTEGER NOT NULL,
  schema_fingerprint TEXT NOT NULL
);
INSERT INTO workerkit_state_meta (singleton, schema_version, schema_fingerprint)
VALUES (1, 1, 'human-workerkit-state-v1-20260720a')
ON CONFLICT(singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS workerkit_conversations (
  caller TEXT NOT NULL CHECK(caller <> ''),
  task TEXT NOT NULL CHECK(task <> ''),
  record BLOB NOT NULL CHECK(length(record) > 0 AND length(record) <= 8388608),
  PRIMARY KEY (caller, task)
);
`
