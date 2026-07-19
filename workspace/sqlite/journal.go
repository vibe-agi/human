// Package sqlite provides the official single-owner SQLite implementation of
// workspace.Store. The transport-neutral workspace package does not
// import this adapter or the SQLite driver.
package sqlite

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
	"sync"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"github.com/vibe-agi/human/workspace"
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

var (
	// ErrInUse means another live resource owns the same SQLite journal file.
	ErrInUse = errors.New("workspace/sqlite: apply journal is already in use")
	// ErrUnsupportedSchema means the file is neither empty nor the exact schema
	// supported by this clean-break adapter.
	ErrUnsupportedSchema = errors.New("workspace/sqlite: unsupported apply journal schema; recreate database")
)

// Config selects one dedicated caller-side journal. Path must not be shared
// with HumanAgent or HumanLLM stores. CommitTimeout bounds detached terminal
// commits after an external side effect; zero uses 10s.
type Config struct {
	Path          string
	CommitTimeout time.Duration
}

func (config Config) withDefaults() (Config, error) {
	config.Path = strings.TrimSpace(config.Path)
	if config.Path == "" {
		return Config{}, fmt.Errorf("%w: apply journal database path is required", workspace.ErrInvalidApply)
	}
	if config.CommitTimeout == 0 {
		config.CommitTimeout = defaultApplyCommitTimeout
	}
	if config.CommitTimeout < time.Millisecond || config.CommitTimeout > 5*time.Minute {
		return Config{}, fmt.Errorf("%w: commit timeout must be 1ms..5m", workspace.ErrInvalidApply)
	}
	return config, nil
}

type applyLockEntry struct {
	token chan struct{}
	refs  int
}

// applyJournal is a single-owner workspace.Store. SQLite transactions
// cover durable intent and terminal receipt; the external CAS deliberately
// runs outside a database transaction.
type applyJournal struct {
	database      *sql.DB
	owner         *ownerlock.Lock
	commitTimeout time.Duration
	now           func() time.Time

	lifecycle sync.Mutex
	active    sync.WaitGroup
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error

	locksMu sync.Mutex
	locks   map[workspace.ApplyIdentity]*applyLockEntry
}

var _ workspace.Store = (*applyJournal)(nil)

func (*applyJournal) Description() workspace.StoreDescription {
	return workspace.StoreDescription{
		Contract: framework.Contract{ID: workspace.StoreContractID, Major: workspace.StoreContractMajor},
		Provider: "sqlite",
		Version:  fmt.Sprintf("schema-%d", applyJournalSchemaVersion),
	}
}

// Open initializes a clean-break schema, takes exclusive process ownership,
// then atomically terminalizes every pending row recovered from a previous
// owner. It never invokes a workspace.CASApplier during recovery. The returned
// owned Resource must be released after every consumer has stopped using it.
func Open(ctx context.Context, config Config) (framework.Resource[workspace.Store], error) {
	if ctx == nil {
		return framework.Resource[workspace.Store]{}, fmt.Errorf("%w: context is required", workspace.ErrInvalidApply)
	}
	if err := ctx.Err(); err != nil {
		return framework.Resource[workspace.Store]{}, err
	}
	config, err := config.withDefaults()
	if err != nil {
		return framework.Resource[workspace.Store]{}, err
	}
	config.Path, err = resolveApplyJournalPath(config.Path)
	if err != nil {
		return framework.Resource[workspace.Store]{}, err
	}
	location, err := sqlitefile.PreparePrivate(config.Path, "workspace apply journal")
	if err != nil {
		return framework.Resource[workspace.Store]{}, err
	}
	owner, err := ownerlock.Acquire(location, "workspace apply journal")
	if err != nil {
		if errors.Is(err, ownerlock.ErrInUse) {
			return framework.Resource[workspace.Store]{}, fmt.Errorf("%w: %v", ErrInUse, err)
		}
		return framework.Resource[workspace.Store]{}, err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner && owner != nil {
			_ = owner.Close()
		}
	}()

	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		return framework.Resource[workspace.Store]{}, fmt.Errorf("open workspace apply journal: %w", err)
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
			return framework.Resource[workspace.Store]{}, fmt.Errorf("configure workspace apply journal: %w", err)
		}
	}
	if err := requireCurrentOrEmptyApplyJournalSchema(ctx, database); err != nil {
		return framework.Resource[workspace.Store]{}, err
	}
	if _, err := database.ExecContext(ctx, applyJournalSchema); err != nil {
		return framework.Resource[workspace.Store]{}, fmt.Errorf("initialize workspace apply journal: %w", err)
	}
	journal := &applyJournal{
		database: database, owner: owner, commitTimeout: config.CommitTimeout,
		now: time.Now, closeDone: make(chan struct{}),
		locks: make(map[workspace.ApplyIdentity]*applyLockEntry),
	}
	if err := journal.recoverPending(ctx); err != nil {
		return framework.Resource[workspace.Store]{}, fmt.Errorf("recover workspace apply journal: %w", err)
	}
	resource, err := framework.Own[workspace.Store](journal, func(releaseContext context.Context) error {
		return journal.close(releaseContext)
	})
	if err != nil {
		return framework.Resource[workspace.Store]{}, errors.Join(err, journal.close(context.Background()))
	}
	closeDatabase = false
	releaseOwner = false
	return resource, nil
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
		return fmt.Errorf("%w: missing schema marker", ErrUnsupportedSchema)
	}
	if version != applyJournalSchemaVersion || fingerprint != applyJournalSchemaFingerprint {
		return fmt.Errorf(
			"%w: version %d (%q), want %d (%q)", ErrUnsupportedSchema,
			version, fingerprint, applyJournalSchemaVersion, applyJournalSchemaFingerprint,
		)
	}
	return nil
}

// close stops admission immediately, then waits in the background for every
// in-process Apply and Lookup before releasing database ownership. ctx bounds
// only the caller's wait; an already-admitted CAS remains responsible for
// reaching a terminal record before the database and owner lock are released.
func (journal *applyJournal) close(ctx context.Context) error {
	if journal == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("close workspace SQLite Store: context is required")
	}
	journal.closeOnce.Do(func() {
		journal.lifecycle.Lock()
		journal.closed = true
		journal.lifecycle.Unlock()
		go func() {
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
			close(journal.closeDone)
		}()
	})
	select {
	case <-journal.closeDone:
		return journal.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Apply persists pending before invoking applier. Exact retries replay their
// terminal record. A reused identity with any different intent fails closed.
func (journal *applyJournal) Apply(
	ctx context.Context,
	intent workspace.ApplyIntent,
	applier workspace.CASApplier,
) (workspace.ApplyResult, error) {
	if ctx == nil {
		return workspace.ApplyResult{}, fmt.Errorf("%w: context is required", workspace.ErrInvalidApply)
	}
	if applier == nil {
		return workspace.ApplyResult{}, fmt.Errorf("%w: CAS applier is required", workspace.ErrInvalidApply)
	}
	intent = workspace.CloneApplyIntent(intent)
	encoded, digest, err := workspace.CanonicalApplyIntent(intent)
	if err != nil {
		return workspace.ApplyResult{}, err
	}
	releaseLifecycle, err := journal.acquire()
	if err != nil {
		return workspace.ApplyResult{}, err
	}
	defer releaseLifecycle()
	unlock, err := journal.lockIdentity(ctx, intent.Identity)
	if err != nil {
		return workspace.ApplyResult{}, err
	}
	defer unlock()

	record, replay, err := journal.begin(ctx, intent, encoded, digest)
	if err != nil {
		return workspace.ApplyResult{}, err
	}
	if replay {
		return workspace.ApplyResult{Record: workspace.CloneApplyRecord(record), Replay: true}, nil
	}

	outcome, applyErr := applier.ApplyCAS(ctx, workspace.CloneApplyIntent(intent))
	switch {
	case applyErr != nil:
		outcome = workspace.IndeterminateOutcome("cas_callback_error", applyErr.Error())
	case workspace.ValidateCASOutcome(outcome, intent) != nil:
		outcome = workspace.IndeterminateOutcome("invalid_cas_result", "CAS applier returned an invalid result")
	default:
		outcome = workspace.CloneCASOutcome(outcome)
	}
	commitContext, cancelCommit := context.WithTimeout(context.WithoutCancel(ctx), journal.commitTimeout)
	completed, err := journal.complete(commitContext, intent.Identity, encoded, outcome)
	cancelCommit()
	if err != nil {
		// Returning the durable pending record is intentional: the external effect
		// may have happened. A later exact retry or process recovery will convert
		// this row to indeterminate without invoking applier again.
		return workspace.ApplyResult{Record: workspace.CloneApplyRecord(record)}, fmt.Errorf("commit workspace apply terminal outcome: %w", err)
	}
	return workspace.ApplyResult{Record: workspace.CloneApplyRecord(completed)}, nil
}

// Lookup reads a durable record without changing a live pending apply.
func (journal *applyJournal) Lookup(ctx context.Context, identity workspace.ApplyIdentity) (workspace.ApplyRecord, error) {
	if ctx == nil {
		return workspace.ApplyRecord{}, fmt.Errorf("%w: context is required", workspace.ErrInvalidApply)
	}
	if err := workspace.ValidateApplyIdentity(identity); err != nil {
		return workspace.ApplyRecord{}, err
	}
	release, err := journal.acquire()
	if err != nil {
		return workspace.ApplyRecord{}, err
	}
	defer release()
	record, _, err := loadApplyRecord(ctx, journal.database, identity)
	return workspace.CloneApplyRecord(record), err
}

func (journal *applyJournal) acquire() (func(), error) {
	if journal == nil {
		return nil, workspace.ErrStoreClosed
	}
	journal.lifecycle.Lock()
	if journal.closed || journal.database == nil {
		journal.lifecycle.Unlock()
		return nil, workspace.ErrStoreClosed
	}
	journal.active.Add(1)
	journal.lifecycle.Unlock()
	return journal.active.Done, nil
}

func (journal *applyJournal) lockIdentity(ctx context.Context, identity workspace.ApplyIdentity) (func(), error) {
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

func (journal *applyJournal) releaseIdentityReference(identity workspace.ApplyIdentity, entry *applyLockEntry) {
	journal.locksMu.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(journal.locks, identity)
	}
	journal.locksMu.Unlock()
}

func (journal *applyJournal) begin(
	ctx context.Context,
	intent workspace.ApplyIntent,
	encoded []byte,
	digest workspace.Digest,
) (workspace.ApplyRecord, bool, error) {
	tx, err := journal.database.BeginTx(ctx, nil)
	if err != nil {
		return workspace.ApplyRecord{}, false, err
	}
	defer tx.Rollback()
	existing, storedIntent, err := loadApplyRecord(ctx, tx, intent.Identity)
	if err == nil {
		if existing.IntentDigest != digest || !bytes.Equal(storedIntent, encoded) {
			return workspace.ApplyRecord{}, false, workspace.ErrApplyIntentConflict
		}
		if existing.State == workspace.ApplyStatePending {
			outcome := workspace.IndeterminateOutcome(
				"unresolved_pending",
				"a prior apply attempt has no durable terminal outcome; automatic replay is unsafe",
			)
			existing, err = terminalizeApply(ctx, tx, existing, outcome, journal.now().UTC())
			if err != nil {
				return workspace.ApplyRecord{}, false, err
			}
		}
		if err := tx.Commit(); err != nil {
			return workspace.ApplyRecord{}, false, err
		}
		return existing, true, nil
	}
	if !errors.Is(err, workspace.ErrApplyNotFound) {
		return workspace.ApplyRecord{}, false, err
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
			return workspace.ApplyRecord{}, false, workspace.ErrApplyIntentConflict
		}
		return workspace.ApplyRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return workspace.ApplyRecord{}, false, err
	}
	return workspace.ApplyRecord{
		Intent: workspace.CloneApplyIntent(intent), IntentDigest: digest,
		State: workspace.ApplyStatePending, CreatedAt: now,
	}, false, nil
}

func (journal *applyJournal) complete(
	ctx context.Context,
	identity workspace.ApplyIdentity,
	encodedIntent []byte,
	outcome workspace.CASOutcome,
) (workspace.ApplyRecord, error) {
	tx, err := journal.database.BeginTx(ctx, nil)
	if err != nil {
		return workspace.ApplyRecord{}, err
	}
	defer tx.Rollback()
	record, storedIntent, err := loadApplyRecord(ctx, tx, identity)
	if err != nil {
		return workspace.ApplyRecord{}, err
	}
	if !bytes.Equal(storedIntent, encodedIntent) {
		return workspace.ApplyRecord{}, workspace.ErrApplyIntentConflict
	}
	if record.State != workspace.ApplyStatePending {
		if record.Outcome != nil && *record.Outcome == outcome {
			if err := tx.Commit(); err != nil {
				return workspace.ApplyRecord{}, err
			}
			return record, nil
		}
		return workspace.ApplyRecord{}, workspace.ErrApplyIntentConflict
	}
	record, err = terminalizeApply(ctx, tx, record, outcome, journal.now().UTC())
	if err != nil {
		return workspace.ApplyRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return workspace.ApplyRecord{}, err
	}
	return record, nil
}

func terminalizeApply(
	ctx context.Context,
	tx *sql.Tx,
	record workspace.ApplyRecord,
	outcome workspace.CASOutcome,
	now time.Time,
) (workspace.ApplyRecord, error) {
	if err := workspace.ValidateCASOutcome(outcome, record.Intent); err != nil {
		return workspace.ApplyRecord{}, err
	}
	// Wall clocks can move backwards across a suspend or reboot. Time is audit
	// metadata, not a fencing oracle, so clamp rather than manufacture a corrupt
	// record whose terminal timestamp precedes its durable intent.
	if now.Before(record.CreatedAt) {
		now = record.CreatedAt
	}
	encoded, err := json.Marshal(outcome)
	if err != nil {
		return workspace.ApplyRecord{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE workspace_artifact_applies
		SET state = ?, outcome = ?, outcome_digest = ?, completed_at = ?
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ? AND state = 'pending'`,
		stateForDecision(outcome.Decision), encoded, digestBytes(encoded), now.UnixNano(),
		record.Intent.Identity.Authority, record.Intent.Identity.Workspace, record.Intent.Identity.Artifact,
	)
	if err != nil {
		return workspace.ApplyRecord{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return workspace.ApplyRecord{}, err
	}
	if rows != 1 {
		return workspace.ApplyRecord{}, workspace.ErrApplyIntentConflict
	}
	record.State = stateForDecision(outcome.Decision)
	storedOutcome := workspace.CloneCASOutcome(outcome)
	record.Outcome = &storedOutcome
	record.CompletedAt = &now
	return record, nil
}

// recoverPending is one atomic state transition. It deliberately does not read
// payloads and never calls user code: a previous process could have completed
// the external CAS immediately before stopping.
func (journal *applyJournal) recoverPending(ctx context.Context) error {
	outcome := workspace.IndeterminateOutcome(
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
		WHERE state = 'pending'`, encoded, digestBytes(encoded),
		now, now)
	return err
}

type applyRowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadApplyRecord(
	ctx context.Context,
	database applyRowQueryer,
	identity workspace.ApplyIdentity,
) (workspace.ApplyRecord, []byte, error) {
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
		return workspace.ApplyRecord{}, nil, workspace.ErrApplyNotFound
	}
	if err != nil {
		return workspace.ApplyRecord{}, nil, err
	}
	var intent workspace.ApplyIntent
	if err := json.Unmarshal(encodedIntent, &intent); err != nil {
		return workspace.ApplyRecord{}, nil, fmt.Errorf("%w: decode intent: %v", workspace.ErrCorruptStore, err)
	}
	canonical, calculatedDigest, err := workspace.CanonicalApplyIntent(intent)
	if err != nil || !bytes.Equal(canonical, encodedIntent) || string(calculatedDigest) != intentDigest || intent.Identity != identity {
		return workspace.ApplyRecord{}, nil, fmt.Errorf("%w: invalid intent for Artifact %q", workspace.ErrCorruptStore, identity.Artifact)
	}
	record := workspace.ApplyRecord{
		Intent: workspace.CloneApplyIntent(intent), IntentDigest: workspace.Digest(intentDigest),
		State: workspace.ApplyState(state), CreatedAt: time.Unix(0, createdAt).UTC(),
	}
	if record.CreatedAt.IsZero() {
		return workspace.ApplyRecord{}, nil, fmt.Errorf("%w: invalid creation time", workspace.ErrCorruptStore)
	}
	if record.State == workspace.ApplyStatePending {
		if len(encodedOutcome) != 0 || outcomeDigest.Valid || completedAt.Valid {
			return workspace.ApplyRecord{}, nil, fmt.Errorf("%w: pending apply has terminal fields", workspace.ErrCorruptStore)
		}
		return record, bytes.Clone(encodedIntent), nil
	}
	if !record.State.Terminal() || len(encodedOutcome) == 0 || !outcomeDigest.Valid || !completedAt.Valid ||
		string(digestBytes(encodedOutcome)) != outcomeDigest.String {
		return workspace.ApplyRecord{}, nil, fmt.Errorf("%w: invalid terminal fields", workspace.ErrCorruptStore)
	}
	var outcome workspace.CASOutcome
	if err := json.Unmarshal(encodedOutcome, &outcome); err != nil ||
		workspace.ValidateCASOutcome(outcome, intent) != nil || stateForDecision(outcome.Decision) != record.State {
		return workspace.ApplyRecord{}, nil, fmt.Errorf("%w: invalid terminal outcome", workspace.ErrCorruptStore)
	}
	completed := time.Unix(0, completedAt.Int64).UTC()
	if completed.Before(record.CreatedAt) {
		return workspace.ApplyRecord{}, nil, fmt.Errorf("%w: terminal time precedes creation", workspace.ErrCorruptStore)
	}
	record.Outcome = &outcome
	record.CompletedAt = &completed
	return record, bytes.Clone(encodedIntent), nil
}

func stateForDecision(decision workspace.ApplyDecision) workspace.ApplyState {
	return workspace.ApplyState(decision)
}

func digestBytes(value []byte) workspace.Digest {
	sum := sha256.Sum256(value)
	return workspace.Digest("sha256:" + hex.EncodeToString(sum[:]))
}
