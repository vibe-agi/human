// Package workerstate persists worker-side UI state that must survive a
// disconnect or process restart. It deliberately has no dependency on the
// TUI so recovery policy can be tested independently from rendering.
package workerstate

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	_ "modernc.org/sqlite"
)

const (
	workerStateSchemaVersion     = 3
	workerStateSchemaFingerprint = "human-worker-state-v3-20260718"
)

var errUnsupportedWorkerStateSchema = errors.New("unsupported worker state schema; recreate database")

var (
	// ErrIdentityUnavailable means authenticated gateway/worker identity has not
	// been bound yet. State must remain invisible until gateway Hello.
	ErrIdentityUnavailable = errors.New("worker state identity is not established")
	// ErrIdentityConflict prevents one Store instance from switching subjects
	// after it has exposed or mutated a namespace.
	ErrIdentityConflict = errors.New("worker state is bound to a different authenticated identity")
)

const workerStateSchema = `
CREATE TABLE IF NOT EXISTS worker_state_schema (
  component TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  fingerprint TEXT NOT NULL
);
INSERT INTO worker_state_schema (component, version, fingerprint)
VALUES ('worker-state', 3, 'human-worker-state-v3-20260718')
ON CONFLICT(component) DO NOTHING;

CREATE TABLE IF NOT EXISTS worker_state_sequence (
	  component TEXT PRIMARY KEY,
	  value INTEGER NOT NULL
) WITHOUT ROWID;
INSERT INTO worker_state_sequence (component, value)
VALUES ('record', 0)
ON CONFLICT(component) DO NOTHING;

CREATE TABLE IF NOT EXISTS worker_state (
	  gateway_id TEXT NOT NULL,
	  worker_id TEXT NOT NULL,
	  caller_id TEXT NOT NULL,
  workspace_key TEXT NOT NULL,
  task_id TEXT NOT NULL,
  session_key TEXT NOT NULL,
  tier TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload BLOB NOT NULL,
  updated_at INTEGER NOT NULL,
	  created_revision INTEGER NOT NULL,
	  revision INTEGER NOT NULL,
	  PRIMARY KEY (gateway_id, worker_id, caller_id, workspace_key, task_id, session_key, tier, kind)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS worker_state_updated
	  ON worker_state(gateway_id, worker_id, revision, caller_id, workspace_key, task_id, session_key, tier, kind);`

// Scope is the correctness namespace for one piece of worker state.
// SessionKey is opaque: completion.Assignment.SessionKey contains a NUL
// separator and must not be treated as a path or a printable stable key.
type Scope struct {
	CallerID     string                    `json:"caller_id"`
	WorkspaceKey string                    `json:"workspace_key,omitempty"`
	TaskID       string                    `json:"task_id,omitempty"`
	SessionKey   string                    `json:"session_key"`
	Tier         completion.CapabilityTier `json:"tier"`
}

// Record is one durable value. UpdatedAt is assigned by Store.Put and is
// always returned in UTC.
type Record struct {
	Scope     Scope           `json:"scope"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
	UpdatedAt time.Time       `json:"updated_at"`
	// CreatedRevision is assigned once when the scope/kind row is inserted;
	// Revision changes on every Put. Both are database-local monotonic values and
	// are the only safe causal ordering source when wall clocks jump or tie.
	CreatedRevision int64 `json:"created_revision"`
	Revision        int64 `json:"revision"`
}

// Identity is one authenticated gateway/worker namespace present in a state
// database. Tokens are deliberately excluded.
type Identity struct {
	GatewayID string `json:"gateway_id"`
	WorkerID  string `json:"worker_id"`
}

// CorruptRecord describes one row which List could not safely decode.
// Other healthy rows are still returned.
type CorruptRecord struct {
	Scope Scope
	Kind  string
	Err   error
}

// CorruptRecordsError reports isolated row corruption. Callers may use the
// records returned alongside this error; none of the corrupt rows are
// included in that result.
type CorruptRecordsError struct {
	Records []CorruptRecord
}

func (err *CorruptRecordsError) Error() string {
	if err == nil || len(err.Records) == 0 {
		return ""
	}
	return fmt.Sprintf("worker state contains %d corrupt record(s); first is %s: %v",
		len(err.Records), recordLabel(err.Records[0].Scope, err.Records[0].Kind), err.Records[0].Err)
}

func (err *CorruptRecordsError) Unwrap() error {
	if err == nil || len(err.Records) == 0 {
		return nil
	}
	return err.Records[0].Err
}

// Store is a single-process SQLite state store. SQLite access is serialized
// so concurrent Put/Delete/List calls share one transactional order.
type Store struct {
	db       *sql.DB
	now      func() time.Time
	bindMu   sync.RWMutex
	gateway  string
	workerID string
	owner    *ownerlock.Lock
}

// Open opens or creates a state database. A newly created parent directory is
// mode 0700 and the database is always mode 0600. Existing parent directories
// must already be private; Open never changes permissions on a caller-owned
// directory.
func Open(ctx context.Context, path string) (*Store, error) {
	if ctx == nil {
		return nil, errors.New("open worker state: context is required")
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("worker state path is required")
	}

	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve worker state path: %w", err)
		}
		path = absolute
		if err := preparePrivateDirectory(path); err != nil {
			return nil, err
		}
	}
	location, err := sqlitefile.PreparePrivate(path, "worker state database")
	if err != nil {
		return nil, err
	}
	owner, err := ownerlock.Acquire(location, "worker state database")
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
		return nil, fmt.Errorf("open worker state: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	closeOnError := func(err error) (*Store, error) {
		_ = database.Close()
		return nil, err
	}

	for _, pragma := range []string{
		"PRAGMA journal_mode = DELETE",
		"PRAGMA synchronous = FULL",
		"PRAGMA secure_delete = ON",
		"PRAGMA busy_timeout = 5000",
	} {
		if _, err := database.ExecContext(ctx, pragma); err != nil {
			return closeOnError(fmt.Errorf("configure worker state: %w", err))
		}
	}
	if err := requireCurrentOrEmptyWorkerStateSchema(ctx, database); err != nil {
		return closeOnError(err)
	}
	if _, err := database.ExecContext(ctx, workerStateSchema); err != nil {
		return closeOnError(fmt.Errorf("initialize worker state schema: %w", err))
	}

	releaseOwner = false
	return &Store{db: database, now: time.Now, owner: owner}, nil
}

// Bind selects the only authenticated correctness namespace this Store
// instance may access. It is idempotent for credential rotation because tokens
// are deliberately absent; a different gateway or worker subject is rejected.
func (store *Store) Bind(ctx context.Context, gatewayID, workerID string) error {
	if err := store.readyDatabase(ctx); err != nil {
		return err
	}
	gatewayID = strings.TrimSpace(gatewayID)
	workerID = strings.TrimSpace(workerID)
	if gatewayID == "" || workerID == "" {
		return errors.New("worker state binding requires gateway and worker identity")
	}
	if len(gatewayID) > 1024 || len(workerID) > 512 || !utf8.ValidString(gatewayID) || !utf8.ValidString(workerID) {
		return errors.New("worker state binding identity is invalid")
	}
	store.bindMu.Lock()
	defer store.bindMu.Unlock()
	if store.gateway == "" && store.workerID == "" {
		store.gateway = gatewayID
		store.workerID = workerID
		return nil
	}
	if store.gateway != gatewayID || store.workerID != workerID {
		return ErrIdentityConflict
	}
	return nil
}

func (store *Store) identity(ctx context.Context) (string, string, error) {
	if err := store.readyDatabase(ctx); err != nil {
		return "", "", err
	}
	store.bindMu.RLock()
	defer store.bindMu.RUnlock()
	if store.gateway == "" || store.workerID == "" {
		return "", "", ErrIdentityUnavailable
	}
	return store.gateway, store.workerID, nil
}

func requireCurrentOrEmptyWorkerStateSchema(ctx context.Context, database *sql.DB) error {
	var tableCount int
	if err := database.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
		return fmt.Errorf("inspect worker state schema: %w", err)
	}
	if tableCount == 0 {
		return nil
	}

	var version int
	var fingerprint string
	if err := database.QueryRowContext(ctx, `
		SELECT version, fingerprint
		FROM worker_state_schema
		WHERE component = 'worker-state'`).Scan(&version, &fingerprint); err != nil {
		return fmt.Errorf("%w: missing worker-state schema marker", errUnsupportedWorkerStateSchema)
	}
	if version != workerStateSchemaVersion || fingerprint != workerStateSchemaFingerprint {
		return fmt.Errorf(
			"%w: worker-state schema version %d (%q), want %d (%q)",
			errUnsupportedWorkerStateSchema,
			version, fingerprint, workerStateSchemaVersion, workerStateSchemaFingerprint,
		)
	}
	return nil
}

// Put transactionally inserts or replaces a value in one scope. Payload is
// copied before Put returns, so later caller mutation cannot change the value
// represented by the returned Record.
func (store *Store) Put(
	ctx context.Context,
	scope Scope,
	kind string,
	payload json.RawMessage,
) (Record, error) {
	gatewayID, workerID, err := store.identity(ctx)
	if err != nil {
		return Record{}, err
	}
	if err := validateKey(scope, kind); err != nil {
		return Record{}, err
	}
	if len(payload) == 0 || !json.Valid(payload) {
		return Record{}, errors.New("worker state payload must be valid JSON")
	}
	payload = append(json.RawMessage(nil), payload...)
	updatedAt := store.now().UTC()
	if updatedAt.IsZero() {
		return Record{}, errors.New("worker state clock returned zero time")
	}

	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, fmt.Errorf("begin worker state update: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()
	var revision int64
	if err := transaction.QueryRowContext(ctx, `
		UPDATE worker_state_sequence
		SET value = value + 1
		WHERE component = 'record'
		RETURNING value`).Scan(&revision); err != nil {
		return Record{}, fmt.Errorf("allocate worker state revision: %w", err)
	}
	if revision <= 0 {
		return Record{}, errors.New("worker state revision must be positive")
	}
	if _, err := transaction.ExecContext(ctx, `
		INSERT INTO worker_state (
		  gateway_id, worker_id, caller_id, workspace_key, task_id, session_key, tier, kind,
		  payload, updated_at, created_revision, revision
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (gateway_id, worker_id, caller_id, workspace_key, task_id, session_key, tier, kind)
		DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at, revision = excluded.revision`,
		gatewayID, workerID, scope.CallerID, scope.WorkspaceKey, scope.TaskID, scope.SessionKey,
		string(scope.Tier), kind, []byte(payload), updatedAt.UnixNano(), revision, revision,
	); err != nil {
		return Record{}, fmt.Errorf("put worker state: %w", err)
	}
	var createdRevision int64
	if err := transaction.QueryRowContext(ctx, `
		SELECT created_revision FROM worker_state
		WHERE gateway_id = ? AND worker_id = ?
		  AND caller_id = ? AND workspace_key = ? AND task_id = ?
		  AND session_key = ? AND tier = ? AND kind = ?`,
		gatewayID, workerID, scope.CallerID, scope.WorkspaceKey, scope.TaskID, scope.SessionKey,
		string(scope.Tier), kind,
	).Scan(&createdRevision); err != nil {
		return Record{}, fmt.Errorf("read worker state created revision: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return Record{}, fmt.Errorf("commit worker state: %w", err)
	}
	return Record{
		Scope: scope, Kind: kind, Payload: payload, UpdatedAt: updatedAt,
		CreatedRevision: createdRevision, Revision: revision,
	}, nil
}

// Delete removes one exact scope/kind value. Deleting an absent value is
// successful, making cleanup idempotent across recovery attempts.
func (store *Store) Delete(ctx context.Context, scope Scope, kind string) error {
	gatewayID, workerID, err := store.identity(ctx)
	if err != nil {
		return err
	}
	if err := validateKey(scope, kind); err != nil {
		return err
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin worker state delete: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.ExecContext(ctx, `
		DELETE FROM worker_state
		WHERE gateway_id = ? AND worker_id = ?
		  AND caller_id = ? AND workspace_key = ? AND task_id = ?
		  AND session_key = ? AND tier = ? AND kind = ?`,
		gatewayID, workerID, scope.CallerID, scope.WorkspaceKey, scope.TaskID, scope.SessionKey,
		string(scope.Tier), kind,
	); err != nil {
		return fmt.Errorf("delete worker state: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit worker state delete: %w", err)
	}
	return nil
}

// List returns every healthy record in deterministic update/key order. Bad
// rows are isolated in CorruptRecordsError rather than hiding healthy state.
func (store *Store) List(ctx context.Context) ([]Record, error) {
	gatewayID, workerID, err := store.identity(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT caller_id, workspace_key, task_id, session_key, tier, kind, payload, updated_at,
		       created_revision, revision
		FROM worker_state
		WHERE gateway_id = ? AND worker_id = ?
		ORDER BY revision, caller_id, workspace_key, task_id, session_key, tier, kind`, gatewayID, workerID)
	if err != nil {
		return nil, fmt.Errorf("list worker state: %w", err)
	}
	defer rows.Close()

	records := make([]Record, 0)
	corrupt := make([]CorruptRecord, 0)
	for rows.Next() {
		var scope Scope
		var tier string
		var kind string
		var payload []byte
		var rawUpdatedAt any
		var createdRevision int64
		var revision int64
		if err := rows.Scan(
			&scope.CallerID, &scope.WorkspaceKey, &scope.TaskID, &scope.SessionKey,
			&tier, &kind, &payload, &rawUpdatedAt, &createdRevision, &revision,
		); err != nil {
			// Scan errors are local to the current SQLite row; continue so one
			// externally damaged value cannot suppress later state.
			corrupt = append(corrupt, CorruptRecord{Scope: scope, Kind: kind, Err: fmt.Errorf("scan row: %w", err)})
			continue
		}
		scope.Tier = completion.CapabilityTier(tier)
		if err := validateKey(scope, kind); err != nil {
			corrupt = append(corrupt, CorruptRecord{Scope: scope, Kind: kind, Err: err})
			continue
		}
		if len(payload) == 0 || !json.Valid(payload) {
			corrupt = append(corrupt, CorruptRecord{Scope: scope, Kind: kind, Err: errors.New("payload is not valid JSON")})
			continue
		}
		updatedNanos, err := sqliteInt64(rawUpdatedAt)
		if err != nil || updatedNanos <= 0 || createdRevision <= 0 || revision < createdRevision {
			if err == nil {
				err = errors.New("timestamp/revision ordering is invalid")
			}
			corrupt = append(corrupt, CorruptRecord{Scope: scope, Kind: kind, Err: fmt.Errorf("invalid updated_at: %w", err)})
			continue
		}
		records = append(records, Record{
			Scope: scope, Kind: kind,
			Payload:         append(json.RawMessage(nil), payload...),
			UpdatedAt:       time.Unix(0, updatedNanos).UTC(),
			CreatedRevision: createdRevision, Revision: revision,
		})
	}
	if err := rows.Err(); err != nil {
		return records, fmt.Errorf("iterate worker state: %w", err)
	}
	if len(corrupt) != 0 {
		return records, &CorruptRecordsError{Records: corrupt}
	}
	return records, nil
}

// Identities inspects namespace metadata without exposing any draft payload.
// It is intended for offline backup verification; ordinary TUI reads still
// require Bind and List.
func (store *Store) Identities(ctx context.Context) ([]Identity, error) {
	if err := store.readyDatabase(ctx); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT DISTINCT gateway_id, worker_id
		FROM worker_state
		ORDER BY gateway_id, worker_id`)
	if err != nil {
		return nil, fmt.Errorf("inspect worker state identities: %w", err)
	}
	defer rows.Close()
	identities := make([]Identity, 0)
	for rows.Next() {
		var identity Identity
		if err := rows.Scan(&identity.GatewayID, &identity.WorkerID); err != nil {
			return nil, fmt.Errorf("scan worker state identity: %w", err)
		}
		identities = append(identities, identity)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate worker state identities: %w", err)
	}
	return identities, nil
}

// RebindIdentity validates and rewrites an offline staged state database for a
// restored gateway path. Every correctness row must belong to the declared old
// identity; a mixed archive is rejected rather than partially rewritten.
func RebindIdentity(ctx context.Context, path, oldGatewayID, newGatewayID, workerID string) error {
	if ctx == nil {
		return errors.New("rebind worker state: context is required")
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("inspect worker state for identity rebind: %w", err)
	}
	oldIdentity := Identity{GatewayID: strings.TrimSpace(oldGatewayID), WorkerID: strings.TrimSpace(workerID)}
	newIdentity := Identity{GatewayID: strings.TrimSpace(newGatewayID), WorkerID: strings.TrimSpace(workerID)}
	if oldIdentity.GatewayID == "" || newIdentity.GatewayID == "" || oldIdentity.WorkerID == "" {
		return errors.New("worker state rebind requires old/new gateway identity and worker subject")
	}
	store, err := Open(ctx, path)
	if err != nil {
		return err
	}
	defer store.Close()
	identities, err := store.Identities(ctx)
	if err != nil {
		return err
	}
	for _, identity := range identities {
		if identity != oldIdentity {
			return errors.New("worker state contains correctness rows for another gateway or worker identity")
		}
	}
	if oldIdentity.GatewayID == newIdentity.GatewayID {
		return nil
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin worker state identity rebind: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		UPDATE worker_state SET gateway_id = ?
		WHERE gateway_id = ? AND worker_id = ?`,
		newIdentity.GatewayID, oldIdentity.GatewayID, oldIdentity.WorkerID,
	); err != nil {
		return fmt.Errorf("rebind worker state identity: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit worker state identity rebind: %w", err)
	}
	return nil
}

// Close releases the database handle.
func (store *Store) Close() error {
	if store == nil || store.db == nil {
		return nil
	}
	return errors.Join(store.db.Close(), store.owner.Close())
}

func (store *Store) readyDatabase(ctx context.Context) error {
	if store == nil || store.db == nil {
		return errors.New("worker state store is not open")
	}
	if ctx == nil {
		return errors.New("worker state context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func preparePrivateDirectory(path string) error {
	directory := filepath.Dir(path)
	info, err := os.Stat(directory)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return fmt.Errorf("create worker state directory: %w", err)
		}
		if err := os.Chmod(directory, 0o700); err != nil {
			return fmt.Errorf("secure worker state directory: %w", err)
		}
	case err != nil:
		return fmt.Errorf("inspect worker state directory: %w", err)
	case !info.IsDir():
		return errors.New("worker state parent is not a directory")
	case runtime.GOOS != "windows" && info.Mode().Perm() != 0o700:
		return fmt.Errorf("worker state directory must have mode 0700 (got %04o)", info.Mode().Perm())
	}
	return nil
}

func validateKey(scope Scope, kind string) error {
	if strings.TrimSpace(scope.CallerID) == "" {
		return errors.New("worker state caller_id is required")
	}
	if strings.TrimSpace(scope.SessionKey) == "" {
		return errors.New("worker state session_key is required")
	}
	if strings.TrimSpace(kind) == "" {
		return errors.New("worker state kind is required")
	}
	if scope.WorkspaceKey != "" && strings.TrimSpace(scope.WorkspaceKey) == "" {
		return errors.New("worker state workspace_key cannot be whitespace")
	}
	if scope.TaskID != "" && strings.TrimSpace(scope.TaskID) == "" {
		return errors.New("worker state task_id cannot be whitespace")
	}
	parsed, err := completion.ParseCapabilityTier(string(scope.Tier))
	if err != nil || parsed != scope.Tier || scope.Tier == "" {
		return fmt.Errorf("worker state tier %q is invalid", scope.Tier)
	}
	if scope.Tier != completion.TierChat {
		if strings.TrimSpace(scope.WorkspaceKey) == "" {
			return errors.New("worker state workspace_key is required for tool-capable tiers")
		}
		if strings.TrimSpace(scope.TaskID) == "" {
			return errors.New("worker state task_id is required for tool-capable tiers")
		}
	}
	return nil
}

func sqliteInt64(value any) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case []byte:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected SQLite type %T", value)
	}
}

func recordLabel(scope Scope, kind string) string {
	return fmt.Sprintf("caller=%q workspace=%q task=%q session=%q tier=%q kind=%q",
		scope.CallerID, scope.WorkspaceKey, scope.TaskID, scope.SessionKey, scope.Tier, kind)
}
