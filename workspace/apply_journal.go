package workspace

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	_ "modernc.org/sqlite"
)

const (
	applyJournalSchemaVersion     = 1
	applyJournalSchemaFingerprint = "human-workspace-apply-journal-v1-20260719"
	defaultApplyCommitTimeout     = 10 * time.Second
)

const applyJournalSchema = `
CREATE TABLE IF NOT EXISTS workspace_apply_schema (
  component TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  fingerprint TEXT NOT NULL
);
INSERT INTO workspace_apply_schema (component, version, fingerprint)
VALUES ('workspace-apply-journal', 1, 'human-workspace-apply-journal-v1-20260719')
ON CONFLICT(component) DO NOTHING;

CREATE TABLE IF NOT EXISTS workspace_artifact_applies (
  authority_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  intent BLOB NOT NULL,
  intent_digest TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('pending', 'success', 'conflict', 'rejected', 'indeterminate')),
  outcome BLOB,
  outcome_digest TEXT,
  created_at INTEGER NOT NULL,
  completed_at INTEGER,
  CHECK (
    (state = 'pending' AND outcome IS NULL AND outcome_digest IS NULL AND completed_at IS NULL) OR
    (state != 'pending' AND outcome IS NOT NULL AND outcome_digest IS NOT NULL AND completed_at IS NOT NULL)
  ),
  PRIMARY KEY (authority_id, workspace_id, artifact_id)
);`

var errUnsupportedApplyJournalSchema = errors.New("unsupported workspace apply journal schema; recreate database")

// ApplyJournalConfig selects one dedicated caller-side journal. DatabasePath
// must not be shared with HumanAgent or HumanLLM stores. CommitTimeout bounds
// detached terminal commits after an external side effect; zero uses 10s.
type ApplyJournalConfig struct {
	DatabasePath  string
	CommitTimeout time.Duration
}

func (config ApplyJournalConfig) withDefaults() (ApplyJournalConfig, error) {
	config.DatabasePath = strings.TrimSpace(config.DatabasePath)
	if config.DatabasePath == "" {
		return ApplyJournalConfig{}, fmt.Errorf("%w: apply journal database path is required", ErrInvalidApply)
	}
	if config.CommitTimeout == 0 {
		config.CommitTimeout = defaultApplyCommitTimeout
	}
	if config.CommitTimeout < time.Millisecond || config.CommitTimeout > 5*time.Minute {
		return ApplyJournalConfig{}, fmt.Errorf("%w: commit timeout must be 1ms..5m", ErrInvalidApply)
	}
	return config, nil
}

type applyLockEntry struct {
	token chan struct{}
	refs  int
}

// SQLiteApplyJournal is a single-owner, embeddable implementation of
// ApplyJournal. SQLite transactions cover durable intent and terminal receipt;
// the external CAS deliberately runs outside a database transaction.
type SQLiteApplyJournal struct {
	database      *sql.DB
	owner         *ownerlock.Lock
	commitTimeout time.Duration
	now           func() time.Time

	lifecycle sync.Mutex
	active    sync.WaitGroup
	closed    bool
	closeOnce sync.Once
	closeErr  error

	locksMu sync.Mutex
	locks   map[ApplyIdentity]*applyLockEntry
}

var _ ApplyJournal = (*SQLiteApplyJournal)(nil)

// OpenSQLiteApplyJournal initializes a clean-break schema, takes exclusive
// process ownership, then atomically terminalizes every pending row recovered
// from a previous owner. It never invokes a CASApplier during recovery.
func OpenSQLiteApplyJournal(ctx context.Context, config ApplyJournalConfig) (*SQLiteApplyJournal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalidApply)
	}
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	config.DatabasePath, err = resolveApplyJournalPath(config.DatabasePath)
	if err != nil {
		return nil, err
	}
	location, err := sqlitefile.PreparePrivate(config.DatabasePath, "workspace apply journal")
	if err != nil {
		return nil, err
	}
	owner, err := ownerlock.Acquire(location, "workspace apply journal")
	if err != nil {
		if errors.Is(err, ownerlock.ErrInUse) {
			return nil, fmt.Errorf("%w: %v", ErrApplyJournalInUse, err)
		}
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
		return nil, fmt.Errorf("open workspace apply journal: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	closeDatabase := true
	defer func() {
		if closeDatabase {
			_ = database.Close()
		}
	}()
	for _, pragma := range []string{
		"PRAGMA journal_mode = DELETE",
		"PRAGMA synchronous = FULL",
		"PRAGMA secure_delete = ON",
		"PRAGMA busy_timeout = 5000",
	} {
		if _, err := database.ExecContext(ctx, pragma); err != nil {
			return nil, fmt.Errorf("configure workspace apply journal: %w", err)
		}
	}
	if err := requireCurrentOrEmptyApplyJournalSchema(ctx, database); err != nil {
		return nil, err
	}
	if _, err := database.ExecContext(ctx, applyJournalSchema); err != nil {
		return nil, fmt.Errorf("initialize workspace apply journal: %w", err)
	}
	journal := &SQLiteApplyJournal{
		database: database, owner: owner, commitTimeout: config.CommitTimeout,
		now: time.Now, locks: make(map[ApplyIdentity]*applyLockEntry),
	}
	if err := journal.recoverPending(ctx); err != nil {
		return nil, fmt.Errorf("recover workspace apply journal: %w", err)
	}
	closeDatabase = false
	releaseOwner = false
	return journal, nil
}

func resolveApplyJournalPath(path string) (string, error) {
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve workspace apply journal path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", fmt.Errorf("create workspace apply journal directory: %w", err)
	}
	return absolute, nil
}

func requireCurrentOrEmptyApplyJournalSchema(ctx context.Context, database *sql.DB) error {
	var tableCount int
	if err := database.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
		return fmt.Errorf("inspect workspace apply journal schema: %w", err)
	}
	if tableCount == 0 {
		return nil
	}
	var version int
	var fingerprint string
	if err := database.QueryRowContext(ctx, `
		SELECT version, fingerprint FROM workspace_apply_schema
		WHERE component = 'workspace-apply-journal'`).Scan(&version, &fingerprint); err != nil {
		return fmt.Errorf("%w: missing schema marker", errUnsupportedApplyJournalSchema)
	}
	if version != applyJournalSchemaVersion || fingerprint != applyJournalSchemaFingerprint {
		return fmt.Errorf(
			"%w: version %d (%q), want %d (%q)", errUnsupportedApplyJournalSchema,
			version, fingerprint, applyJournalSchemaVersion, applyJournalSchemaFingerprint,
		)
	}
	return nil
}

// Close waits for every in-process Apply and Lookup before releasing database
// ownership. It is idempotent.
func (journal *SQLiteApplyJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.closeOnce.Do(func() {
		journal.lifecycle.Lock()
		journal.closed = true
		journal.lifecycle.Unlock()
		// No operation can increment active after closed becomes visible because
		// acquire performs both actions under lifecycle. User CAS callbacks may
		// reenter Lookup without contending with a queued lifecycle writer.
		journal.active.Wait()
		if journal.database != nil {
			journal.closeErr = journal.database.Close()
		}
		if journal.owner != nil {
			journal.closeErr = errors.Join(journal.closeErr, journal.owner.Close())
		}
	})
	return journal.closeErr
}

// Apply persists pending before invoking applier. Exact retries replay their
// terminal record. A reused identity with any different intent fails closed.
func (journal *SQLiteApplyJournal) Apply(
	ctx context.Context,
	intent ApplyIntent,
	applier CASApplier,
) (ApplyResult, error) {
	if ctx == nil {
		return ApplyResult{}, fmt.Errorf("%w: context is required", ErrInvalidApply)
	}
	if applier == nil {
		return ApplyResult{}, fmt.Errorf("%w: CAS applier is required", ErrInvalidApply)
	}
	intent = cloneApplyIntent(intent)
	encoded, digest, err := canonicalApplyIntent(intent)
	if err != nil {
		return ApplyResult{}, err
	}
	releaseLifecycle, err := journal.acquire()
	if err != nil {
		return ApplyResult{}, err
	}
	defer releaseLifecycle()
	unlock, err := journal.lockIdentity(ctx, intent.Identity)
	if err != nil {
		return ApplyResult{}, err
	}
	defer unlock()

	record, replay, err := journal.begin(ctx, intent, encoded, digest)
	if err != nil {
		return ApplyResult{}, err
	}
	if replay {
		return ApplyResult{Record: cloneApplyRecord(record), Replay: true}, nil
	}

	outcome, applyErr := applier.ApplyCAS(ctx, cloneApplyIntent(intent))
	switch {
	case applyErr != nil:
		outcome = indeterminateOutcome("cas_callback_error", applyErr.Error())
	case validateCASOutcome(outcome, intent) != nil:
		outcome = indeterminateOutcome("invalid_cas_result", "CAS applier returned an invalid result")
	default:
		outcome = cloneCASOutcome(outcome)
	}
	commitContext, cancelCommit := context.WithTimeout(context.WithoutCancel(ctx), journal.commitTimeout)
	completed, err := journal.complete(commitContext, intent.Identity, encoded, outcome)
	cancelCommit()
	if err != nil {
		// Returning the durable pending record is intentional: the external effect
		// may have happened. A later exact retry or process recovery will convert
		// this row to indeterminate without invoking applier again.
		return ApplyResult{Record: cloneApplyRecord(record)}, fmt.Errorf("commit workspace apply terminal outcome: %w", err)
	}
	return ApplyResult{Record: cloneApplyRecord(completed)}, nil
}

// Lookup reads a durable record without changing a live pending apply.
func (journal *SQLiteApplyJournal) Lookup(ctx context.Context, identity ApplyIdentity) (ApplyRecord, error) {
	if ctx == nil {
		return ApplyRecord{}, fmt.Errorf("%w: context is required", ErrInvalidApply)
	}
	if err := validateApplyIdentity(identity); err != nil {
		return ApplyRecord{}, err
	}
	release, err := journal.acquire()
	if err != nil {
		return ApplyRecord{}, err
	}
	defer release()
	record, _, err := loadApplyRecord(ctx, journal.database, identity)
	return cloneApplyRecord(record), err
}

func (journal *SQLiteApplyJournal) acquire() (func(), error) {
	if journal == nil {
		return nil, ErrApplyJournalClosed
	}
	journal.lifecycle.Lock()
	if journal.closed || journal.database == nil {
		journal.lifecycle.Unlock()
		return nil, ErrApplyJournalClosed
	}
	journal.active.Add(1)
	journal.lifecycle.Unlock()
	return journal.active.Done, nil
}

func (journal *SQLiteApplyJournal) lockIdentity(ctx context.Context, identity ApplyIdentity) (func(), error) {
	journal.locksMu.Lock()
	entry := journal.locks[identity]
	if entry == nil {
		entry = &applyLockEntry{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		journal.locks[identity] = entry
	}
	entry.refs++
	journal.locksMu.Unlock()
	select {
	case <-ctx.Done():
		journal.releaseIdentityReference(identity, entry)
		return nil, ctx.Err()
	case <-entry.token:
	}
	return func() {
		entry.token <- struct{}{}
		journal.releaseIdentityReference(identity, entry)
	}, nil
}

func (journal *SQLiteApplyJournal) releaseIdentityReference(identity ApplyIdentity, entry *applyLockEntry) {
	journal.locksMu.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(journal.locks, identity)
	}
	journal.locksMu.Unlock()
}

func (journal *SQLiteApplyJournal) begin(
	ctx context.Context,
	intent ApplyIntent,
	encoded []byte,
	digest Digest,
) (ApplyRecord, bool, error) {
	tx, err := journal.database.BeginTx(ctx, nil)
	if err != nil {
		return ApplyRecord{}, false, err
	}
	defer tx.Rollback()
	existing, storedIntent, err := loadApplyRecord(ctx, tx, intent.Identity)
	if err == nil {
		if existing.IntentDigest != digest || !bytes.Equal(storedIntent, encoded) {
			return ApplyRecord{}, false, ErrApplyIntentConflict
		}
		if existing.State == ApplyStatePending {
			outcome := indeterminateOutcome(
				"unresolved_pending",
				"a prior apply attempt has no durable terminal outcome; automatic replay is unsafe",
			)
			existing, err = terminalizeApply(ctx, tx, existing, outcome, journal.now().UTC())
			if err != nil {
				return ApplyRecord{}, false, err
			}
		}
		if err := tx.Commit(); err != nil {
			return ApplyRecord{}, false, err
		}
		return existing, true, nil
	}
	if !errors.Is(err, ErrApplyNotFound) {
		return ApplyRecord{}, false, err
	}
	now := journal.now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO workspace_artifact_applies (
		  authority_id, workspace_id, artifact_id, intent, intent_digest, state, created_at
		) VALUES (?, ?, ?, ?, ?, 'pending', ?)`,
		intent.Identity.Authority, intent.Identity.Workspace, intent.Identity.Artifact,
		encoded, digest, now.UnixNano(),
	); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return ApplyRecord{}, false, ErrApplyIntentConflict
		}
		return ApplyRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return ApplyRecord{}, false, err
	}
	return ApplyRecord{
		Intent: cloneApplyIntent(intent), IntentDigest: digest,
		State: ApplyStatePending, CreatedAt: now,
	}, false, nil
}

func (journal *SQLiteApplyJournal) complete(
	ctx context.Context,
	identity ApplyIdentity,
	encodedIntent []byte,
	outcome CASOutcome,
) (ApplyRecord, error) {
	tx, err := journal.database.BeginTx(ctx, nil)
	if err != nil {
		return ApplyRecord{}, err
	}
	defer tx.Rollback()
	record, storedIntent, err := loadApplyRecord(ctx, tx, identity)
	if err != nil {
		return ApplyRecord{}, err
	}
	if !bytes.Equal(storedIntent, encodedIntent) {
		return ApplyRecord{}, ErrApplyIntentConflict
	}
	if record.State != ApplyStatePending {
		if record.Outcome != nil && *record.Outcome == outcome {
			if err := tx.Commit(); err != nil {
				return ApplyRecord{}, err
			}
			return record, nil
		}
		return ApplyRecord{}, ErrApplyIntentConflict
	}
	record, err = terminalizeApply(ctx, tx, record, outcome, journal.now().UTC())
	if err != nil {
		return ApplyRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return ApplyRecord{}, err
	}
	return record, nil
}

func terminalizeApply(
	ctx context.Context,
	tx *sql.Tx,
	record ApplyRecord,
	outcome CASOutcome,
	now time.Time,
) (ApplyRecord, error) {
	if err := validateCASOutcome(outcome, record.Intent); err != nil {
		return ApplyRecord{}, err
	}
	// Wall clocks can move backwards across a suspend or reboot. Time is audit
	// metadata, not a fencing oracle, so clamp rather than manufacture a corrupt
	// record whose terminal timestamp precedes its durable intent.
	if now.Before(record.CreatedAt) {
		now = record.CreatedAt
	}
	encoded, err := json.Marshal(outcome)
	if err != nil {
		return ApplyRecord{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE workspace_artifact_applies
		SET state = ?, outcome = ?, outcome_digest = ?, completed_at = ?
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ? AND state = 'pending'`,
		stateForDecision(outcome.Decision), encoded, sha256Digest(encoded), now.UnixNano(),
		record.Intent.Identity.Authority, record.Intent.Identity.Workspace, record.Intent.Identity.Artifact,
	)
	if err != nil {
		return ApplyRecord{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return ApplyRecord{}, err
	}
	if rows != 1 {
		return ApplyRecord{}, ErrApplyIntentConflict
	}
	record.State = stateForDecision(outcome.Decision)
	storedOutcome := cloneCASOutcome(outcome)
	record.Outcome = &storedOutcome
	record.CompletedAt = &now
	return record, nil
}

// recoverPending is one atomic state transition. It deliberately does not read
// payloads and never calls user code: a previous process could have completed
// the external CAS immediately before stopping.
func (journal *SQLiteApplyJournal) recoverPending(ctx context.Context) error {
	outcome := indeterminateOutcome(
		"recovered_pending",
		"the previous journal owner stopped before recording a terminal apply outcome",
	)
	encoded, err := json.Marshal(outcome)
	if err != nil {
		return err
	}
	now := journal.now().UTC().UnixNano()
	_, err = journal.database.ExecContext(ctx, `
		UPDATE workspace_artifact_applies
		SET state = 'indeterminate', outcome = ?, outcome_digest = ?,
		    completed_at = CASE WHEN created_at > ? THEN created_at ELSE ? END
		WHERE state = 'pending'`, encoded, sha256Digest(encoded),
		now, now)
	return err
}

type applyRowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadApplyRecord(
	ctx context.Context,
	database applyRowQueryer,
	identity ApplyIdentity,
) (ApplyRecord, []byte, error) {
	var encodedIntent, encodedOutcome []byte
	var intentDigest string
	var state string
	var outcomeDigest sql.NullString
	var createdAt int64
	var completedAt sql.NullInt64
	err := database.QueryRowContext(ctx, `
		SELECT intent, intent_digest, state, outcome, outcome_digest, created_at, completed_at
		FROM workspace_artifact_applies
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		identity.Authority, identity.Workspace, identity.Artifact,
	).Scan(&encodedIntent, &intentDigest, &state, &encodedOutcome, &outcomeDigest, &createdAt, &completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ApplyRecord{}, nil, ErrApplyNotFound
	}
	if err != nil {
		return ApplyRecord{}, nil, err
	}
	var intent ApplyIntent
	if err := json.Unmarshal(encodedIntent, &intent); err != nil {
		return ApplyRecord{}, nil, fmt.Errorf("%w: decode intent: %v", ErrCorruptApplyJournal, err)
	}
	canonical, calculatedDigest, err := canonicalApplyIntent(intent)
	if err != nil || !bytes.Equal(canonical, encodedIntent) || string(calculatedDigest) != intentDigest || intent.Identity != identity {
		return ApplyRecord{}, nil, fmt.Errorf("%w: invalid intent for Artifact %q", ErrCorruptApplyJournal, identity.Artifact)
	}
	record := ApplyRecord{
		Intent: cloneApplyIntent(intent), IntentDigest: Digest(intentDigest),
		State: ApplyState(state), CreatedAt: time.Unix(0, createdAt).UTC(),
	}
	if record.CreatedAt.IsZero() {
		return ApplyRecord{}, nil, fmt.Errorf("%w: invalid creation time", ErrCorruptApplyJournal)
	}
	if record.State == ApplyStatePending {
		if len(encodedOutcome) != 0 || outcomeDigest.Valid || completedAt.Valid {
			return ApplyRecord{}, nil, fmt.Errorf("%w: pending apply has terminal fields", ErrCorruptApplyJournal)
		}
		return record, bytes.Clone(encodedIntent), nil
	}
	if !record.State.Terminal() || len(encodedOutcome) == 0 || !outcomeDigest.Valid || !completedAt.Valid ||
		string(sha256Digest(encodedOutcome)) != outcomeDigest.String {
		return ApplyRecord{}, nil, fmt.Errorf("%w: invalid terminal fields", ErrCorruptApplyJournal)
	}
	var outcome CASOutcome
	if err := json.Unmarshal(encodedOutcome, &outcome); err != nil ||
		validateCASOutcome(outcome, intent) != nil || stateForDecision(outcome.Decision) != record.State {
		return ApplyRecord{}, nil, fmt.Errorf("%w: invalid terminal outcome", ErrCorruptApplyJournal)
	}
	completed := time.Unix(0, completedAt.Int64).UTC()
	if completed.Before(record.CreatedAt) {
		return ApplyRecord{}, nil, fmt.Errorf("%w: terminal time precedes creation", ErrCorruptApplyJournal)
	}
	record.Outcome = &outcome
	record.CompletedAt = &completed
	return record, bytes.Clone(encodedIntent), nil
}
