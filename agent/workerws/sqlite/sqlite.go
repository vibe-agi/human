// Package sqlite provides the official durable SQLite implementation of the
// Agent remote-worker Journal port.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/vibe-agi/human/agent/workerws"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	_ "modernc.org/sqlite"
)

const (
	schemaVersion     = 1
	schemaFingerprint = "human-agent-worker-journal-v1-20260719b"
	databasePurpose   = "HumanAgent worker Journal database"

	// DefaultMaxPendingRecords and DefaultMaxPendingBytes bound all Journal
	// records which still retain payloads: assignments, outbound events, and
	// application-facing rejections. Tombstones do not consume the quota.
	DefaultMaxPendingRecords = 4096
	DefaultMaxPendingBytes   = int64(512 << 20)
)

var (
	// ErrDatabaseInUse means another live process or Resource owns the same
	// file-backed Journal. Sharing one SQLite file between Journal instances is
	// deliberately unsupported.
	ErrDatabaseInUse = errors.New("Agent worker Journal database is already held by another live owner")

	// ErrUnsupportedSchema rejects migration and accidental database sharing.
	// The adapter is pre-release and intentionally accepts only an empty database
	// or its exact current schema.
	ErrUnsupportedSchema = errors.New("unsupported Agent worker Journal sqlite schema; use a dedicated database")
)

// Config selects one dedicated worker Journal persistence identity and its
// live-payload budget. A zero quota selects the documented default. Quotas are
// operational rather than durable: reopening with a lower value is allowed;
// an existing over-budget backlog can always settle or compact, while new
// assignment/event records are rejected with workerws.ErrJournalLimit.
type Config struct {
	Path              string
	MaxPendingRecords int
	MaxPendingBytes   int64
}

func (config Config) withDefaults() (Config, error) {
	if config.MaxPendingRecords < 0 {
		return Config{}, errors.New("Agent worker Journal MaxPendingRecords must not be negative")
	}
	if config.MaxPendingBytes < 0 {
		return Config{}, errors.New("Agent worker Journal MaxPendingBytes must not be negative")
	}
	if config.MaxPendingRecords == 0 {
		config.MaxPendingRecords = DefaultMaxPendingRecords
	}
	if config.MaxPendingBytes == 0 {
		config.MaxPendingBytes = DefaultMaxPendingBytes
	}
	return config, nil
}

// Open constructs an owned SQLite Journal Resource. Config.Path may be an
// ordinary filesystem path or an independent :memory: database. Shared-memory SQLite
// DSNs are rejected because they cannot satisfy the adapter's single-owner
// lifecycle guarantee. ctx bounds construction only and is not retained.
func Open(ctx context.Context, config Config) (framework.Resource[workerws.Journal], error) {
	if ctx == nil {
		return framework.Resource[workerws.Journal]{}, errors.New("open Agent worker Journal sqlite: context is required")
	}
	if err := ctx.Err(); err != nil {
		return framework.Resource[workerws.Journal]{}, err
	}
	config, err := config.withDefaults()
	if err != nil {
		return framework.Resource[workerws.Journal]{}, err
	}
	resolved, err := resolveDatabasePath(config.Path)
	if err != nil {
		return framework.Resource[workerws.Journal]{}, err
	}
	location, err := sqlitefile.PreparePrivate(resolved, databasePurpose)
	if err != nil {
		return framework.Resource[workerws.Journal]{}, err
	}
	owner, err := ownerlock.Acquire(location, databasePurpose)
	if err != nil {
		if errors.Is(err, ownerlock.ErrInUse) {
			return framework.Resource[workerws.Journal]{}, fmt.Errorf("%w: %v", ErrDatabaseInUse, err)
		}
		return framework.Resource[workerws.Journal]{}, err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner {
			_ = owner.Close()
		}
	}()

	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		return framework.Resource[workerws.Journal]{}, fmt.Errorf("open Agent worker Journal sqlite: %w", err)
	}
	closeDatabase := true
	defer func() {
		if closeDatabase {
			_ = database.Close()
		}
	}()
	// One connection gives every mutation one unambiguous serial order. WAL is
	// still useful for crash recovery and for future read-pool separation.
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if err := configureDatabase(ctx, database); err != nil {
		return framework.Resource[workerws.Journal]{}, err
	}
	if err := requireCurrentOrEmptySchema(ctx, database); err != nil {
		return framework.Resource[workerws.Journal]{}, err
	}
	if err := initializeSchema(ctx, database); err != nil {
		return framework.Resource[workerws.Journal]{}, err
	}

	journal := &journal{
		database:          database,
		maxPendingRecords: int64(config.MaxPendingRecords),
		maxPendingBytes:   config.MaxPendingBytes,
	}
	journal.commitTx = func(tx *sql.Tx) error { return tx.Commit() }
	resource, err := framework.Own[workerws.Journal](journal, func(context.Context) error {
		journal.lifecycle.Lock()
		if journal.closed {
			journal.lifecycle.Unlock()
			return nil
		}
		journal.closed = true
		closeErr := database.Close()
		ownerErr := owner.Close()
		journal.lifecycle.Unlock()
		return errors.Join(closeErr, ownerErr)
	})
	if err != nil {
		return framework.Resource[workerws.Journal]{}, fmt.Errorf("own Agent worker Journal sqlite Resource: %w", err)
	}
	closeDatabase = false
	releaseOwner = false
	return resource, nil
}

type journal struct {
	database          *sql.DB
	maxPendingRecords int64
	maxPendingBytes   int64

	lifecycle sync.RWMutex
	closed    bool
	commitTx  func(*sql.Tx) error
}

var _ workerws.Journal = (*journal)(nil)

func (*journal) Description() workerws.JournalDescription {
	return workerws.JournalDescription{
		Contract: framework.Contract{
			ID: workerws.JournalContractID, Major: workerws.JournalContractMajor,
		},
		Provider: "sqlite",
		Version:  fmt.Sprintf("schema-%d", schemaVersion),
	}
}

func (journal *journal) acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("Agent worker Journal operation requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	journal.lifecycle.RLock()
	if journal.closed {
		journal.lifecycle.RUnlock()
		return nil, workerws.ErrJournalClosed
	}
	return journal.lifecycle.RUnlock, nil
}

// update performs exactly one transaction and never retries fn. A failed
// Commit has an unknowable outcome even when the driver reports a familiar
// context or I/O error; callers reconcile by repeating the exact operation.
func (journal *journal) update(ctx context.Context, fn func(*sql.Tx) error) error {
	release, err := journal.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	tx, err := journal.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	if err := journal.commitTx(tx); err != nil {
		return errors.Join(workerws.ErrJournalCommitUnknown, fmt.Errorf("commit Agent worker Journal transaction: %w", err))
	}
	return nil
}

func configureDatabase(ctx context.Context, database *sql.DB) error {
	var journalMode string
	if err := database.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("configure Agent worker Journal sqlite WAL: %w", err)
	}
	for _, pragma := range []string{
		"PRAGMA synchronous = FULL",
		"PRAGMA secure_delete = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := database.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configure Agent worker Journal sqlite: %w", err)
		}
	}
	return nil
}

func resolveDatabasePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("Agent worker Journal SQLite path is required")
	}
	if path == ":memory:" || strings.HasPrefix(strings.ToLower(path), "file:") {
		return path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve Agent worker Journal database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", fmt.Errorf("create Agent worker Journal database directory: %w", err)
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
		return fmt.Errorf("inspect Agent worker Journal sqlite schema: %w", err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			rows.Close()
			return fmt.Errorf("inspect Agent worker Journal sqlite table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("inspect Agent worker Journal sqlite tables: %w", err)
	}
	if len(tables) == 0 {
		return nil
	}

	expected := []string{
		"agent_worker_journal_assignments_pending_idx",
		"agent_worker_journal_assignments",
		"agent_worker_journal_events_pending_idx",
		"agent_worker_journal_events",
		"agent_worker_journal_meta",
		"agent_worker_journal_rejections_pending_idx",
		"agent_worker_journal_rejections",
	}
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
		FROM agent_worker_journal_meta
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
		return fmt.Errorf("begin Agent worker Journal schema transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, journalSchema); err != nil {
		return fmt.Errorf("initialize Agent worker Journal sqlite schema: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Agent worker Journal sqlite schema: %w", err)
	}
	return nil
}

func unsupportedSchema(format string, arguments ...any) error {
	return errors.Join(
		ErrUnsupportedSchema,
		workerws.ErrJournalCorrupt,
		fmt.Errorf(format, arguments...),
	)
}

const journalSchema = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS agent_worker_journal_meta (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  schema_version INTEGER NOT NULL,
  schema_fingerprint TEXT NOT NULL,
  next_sequence INTEGER NOT NULL CHECK(next_sequence >= 0),
  binding_gateway_id TEXT,
  binding_authority_id TEXT,
  binding_worker_id TEXT,
  CHECK(
    (binding_gateway_id IS NULL AND binding_authority_id IS NULL AND binding_worker_id IS NULL)
    OR
    (binding_gateway_id <> '' AND binding_authority_id <> '' AND binding_worker_id <> '')
  )
);
INSERT INTO agent_worker_journal_meta (
  singleton, schema_version, schema_fingerprint, next_sequence
) VALUES (1, 1, 'human-agent-worker-journal-v1-20260719b', 0)
ON CONFLICT(singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS agent_worker_journal_assignments (
  delivery_id TEXT PRIMARY KEY CHECK(delivery_id <> ''),
  sequence INTEGER NOT NULL UNIQUE CHECK(sequence > 0),
  digest TEXT NOT NULL CHECK(length(digest) = 64),
  state TEXT NOT NULL CHECK(state IN ('pending', 'settled')),
  payload BLOB,
  CHECK(payload IS NULL OR (length(payload) > 0 AND length(payload) <= 100663296)),
  CHECK(
    (state = 'pending' AND payload IS NOT NULL)
    OR (state = 'settled' AND payload IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS agent_worker_journal_events (
  delivery_id TEXT PRIMARY KEY CHECK(delivery_id <> ''),
  sequence INTEGER NOT NULL UNIQUE CHECK(sequence > 0),
  digest TEXT NOT NULL CHECK(length(digest) = 64),
  state TEXT NOT NULL CHECK(state IN ('pending', 'settled')),
  payload BLOB,
  receipt_digest TEXT,
  CHECK(payload IS NULL OR (length(payload) > 0 AND length(payload) <= 100663296)),
  CHECK(
    (state = 'pending' AND payload IS NOT NULL AND receipt_digest IS NULL)
    OR
    (state = 'settled' AND payload IS NULL AND length(receipt_digest) = 64)
  )
);

CREATE TABLE IF NOT EXISTS agent_worker_journal_rejections (
  delivery_id TEXT PRIMARY KEY CHECK(delivery_id <> ''),
  sequence INTEGER NOT NULL UNIQUE CHECK(sequence > 0),
  event_digest TEXT NOT NULL CHECK(length(event_digest) = 64),
  receipt_digest TEXT NOT NULL CHECK(length(receipt_digest) = 64),
  state TEXT NOT NULL CHECK(state IN ('pending', 'settled')),
  payload BLOB,
  CHECK(payload IS NULL OR (length(payload) > 0 AND length(payload) <= 100728832)),
  CHECK(
    (state = 'pending' AND payload IS NOT NULL)
    OR (state = 'settled' AND payload IS NULL)
  ),
  FOREIGN KEY(delivery_id)
    REFERENCES agent_worker_journal_events(delivery_id)
    ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS agent_worker_journal_assignments_pending_idx
  ON agent_worker_journal_assignments(sequence)
  WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS agent_worker_journal_events_pending_idx
  ON agent_worker_journal_events(sequence)
  WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS agent_worker_journal_rejections_pending_idx
  ON agent_worker_journal_rejections(sequence)
  WHERE state = 'pending';
`
