package delegation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteSchema = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS delegation_tasks (
  task_id TEXT PRIMARY KEY,
  caller_id TEXT NOT NULL,
  context_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN (
    'submitted', 'working', 'input-required', 'rewind-pending',
    'completed', 'canceled', 'rejected', 'failed'
  )),
  worker_id TEXT NOT NULL DEFAULT '',
  latest_turn INTEGER NOT NULL DEFAULT 0 CHECK(latest_turn >= 0),
  next_turn INTEGER NOT NULL DEFAULT 1 CHECK(next_turn > latest_turn),
  pending_rewind_to INTEGER CHECK(pending_rewind_to >= 0),
  revision INTEGER NOT NULL DEFAULT 1 CHECK(revision >= 1),
  metadata BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  CHECK (
    (state = 'rewind-pending' AND pending_rewind_to IS NOT NULL) OR
    (state <> 'rewind-pending' AND pending_rewind_to IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS delegation_turns (
  task_id TEXT NOT NULL,
  turn_number INTEGER NOT NULL CHECK(turn_number > 0),
  superseded_at_revision INTEGER CHECK(superseded_at_revision > 0),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (task_id, turn_number),
  FOREIGN KEY (task_id) REFERENCES delegation_tasks(task_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS delegation_artifacts (
  artifact_id TEXT NOT NULL UNIQUE,
  task_id TEXT NOT NULL,
  turn_number INTEGER NOT NULL,
  media_type TEXT NOT NULL,
  data BLOB NOT NULL,
  metadata BLOB NOT NULL,
  sha256 TEXT NOT NULL,
  superseded_at_revision INTEGER CHECK(superseded_at_revision > 0),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (task_id, turn_number),
  FOREIGN KEY (task_id, turn_number)
    REFERENCES delegation_turns(task_id, turn_number) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS delegation_events (
  task_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence > 0),
  kind TEXT NOT NULL,
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  turn_number INTEGER,
  data BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (task_id, sequence),
  FOREIGN KEY (task_id) REFERENCES delegation_tasks(task_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS delegation_messages (
  caller_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence > 0),
  role TEXT NOT NULL,
  data BLOB NOT NULL,
  sha256 TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (caller_id, message_id),
  UNIQUE (task_id, sequence),
  FOREIGN KEY (task_id) REFERENCES delegation_tasks(task_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS delegation_worker_command_receipts (
  event_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  command_digest TEXT NOT NULL,
  result BLOB NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS delegation_exec_requests (
  task_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  command TEXT NOT NULL,
  cwd TEXT NOT NULL,
  timeout_ms INTEGER NOT NULL CHECK(timeout_ms >= 0),
  reason TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending', 'completed', 'denied', 'failed')),
  exit_code INTEGER,
  stdout BLOB NOT NULL,
  stderr BLOB NOT NULL,
  error TEXT NOT NULL,
  truncated INTEGER NOT NULL DEFAULT 0,
  timed_out INTEGER NOT NULL DEFAULT 0,
  request_digest TEXT NOT NULL,
  resolution_id TEXT NOT NULL DEFAULT '',
  resolution_digest TEXT NOT NULL DEFAULT '',
  request_sequence INTEGER NOT NULL CHECK(request_sequence > 0),
  resolution_sequence INTEGER CHECK(resolution_sequence > request_sequence),
  created_at INTEGER NOT NULL,
  resolved_at INTEGER,
  PRIMARY KEY (task_id, request_id),
  UNIQUE (task_id, request_sequence),
  FOREIGN KEY (task_id) REFERENCES delegation_tasks(task_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS delegation_tasks_state_created_idx
  ON delegation_tasks(state, created_at, task_id);
CREATE INDEX IF NOT EXISTS delegation_events_task_sequence_idx
  ON delegation_events(task_id, sequence);
CREATE INDEX IF NOT EXISTS delegation_turns_live_idx
  ON delegation_turns(task_id, superseded_at_revision, turn_number);
CREATE INDEX IF NOT EXISTS delegation_messages_task_sequence_idx
  ON delegation_messages(task_id, sequence);
CREATE INDEX IF NOT EXISTS delegation_worker_receipts_task_idx
  ON delegation_worker_command_receipts(task_id, created_at);
CREATE INDEX IF NOT EXISTS delegation_exec_task_status_idx
  ON delegation_exec_requests(task_id, status, request_sequence);
CREATE UNIQUE INDEX IF NOT EXISTS delegation_exec_resolution_idx
  ON delegation_exec_requests(task_id, resolution_id) WHERE resolution_id <> '';
`

// Store is an independently usable SQLite authority for delegation mode.
// Domain changes are serialized through transactions and guarded by Task.Revision.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

// OpenSQLite opens or creates the delegation tables in dsn. Table names are
// prefixed, so the same database file may also contain completion-mode tables.
func OpenSQLite(ctx context.Context, dsn string) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: sqlite dsn is required", ErrInvalidInput)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open delegation sqlite: %w", err)
	}
	// One connection makes in-memory databases deterministic and enforces the
	// single-writer authority within a process. Revision CAS protects stale and
	// racing commands submitted to that authority.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure delegation sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqliteSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate delegation sqlite: %w", err)
	}
	if err := ensureDelegationColumn(ctx, db, "delegation_tasks", "context_id", `
		ALTER TABLE delegation_tasks ADD COLUMN context_id TEXT NOT NULL DEFAULT ''`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE delegation_tasks SET context_id = task_id WHERE context_id = ''`); err != nil {
		db.Close()
		return nil, fmt.Errorf("backfill delegation context ids: %w", err)
	}
	return &Store{db: db, now: time.Now}, nil
}

func (store *Store) Close() error { return store.db.Close() }

func (store *Store) GetTask(ctx context.Context, taskID string) (Task, error) {
	if strings.TrimSpace(taskID) == "" {
		return Task{}, fmt.Errorf("%w: task id is required", ErrInvalidInput)
	}
	return getTask(ctx, store.db, taskID)
}

// ListRecoverableTasks returns every non-terminal task in a stable order.
func (store *Store) ListRecoverableTasks(ctx context.Context) ([]Task, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT task_id, caller_id, context_id, state, worker_id, latest_turn, next_turn,
		       pending_rewind_to, revision, metadata, created_at, updated_at
		FROM delegation_tasks
		WHERE state NOT IN ('completed', 'canceled', 'rejected', 'failed')
		ORDER BY created_at, task_id`)
	if err != nil {
		return nil, fmt.Errorf("list recoverable delegation tasks: %w", err)
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recoverable delegation tasks: %w", err)
	}
	return tasks, nil
}

// ListTasks returns all tasks owned by caller, including terminal tasks, in a
// deterministic newest-first order suitable for protocol pagination.
func (store *Store) ListTasks(ctx context.Context, callerID string) ([]Task, error) {
	callerID = strings.TrimSpace(callerID)
	if callerID == "" {
		return nil, fmt.Errorf("%w: caller id is required", ErrInvalidInput)
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT task_id, caller_id, context_id, state, worker_id, latest_turn, next_turn,
		       pending_rewind_to, revision, metadata, created_at, updated_at
		FROM delegation_tasks WHERE caller_id = ?
		ORDER BY updated_at DESC, task_id DESC`, callerID)
	if err != nil {
		return nil, fmt.Errorf("list delegation tasks for caller: %w", err)
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delegation tasks for caller: %w", err)
	}
	return tasks, nil
}

func (store *Store) ListTurns(ctx context.Context, taskID string) ([]Turn, error) {
	if _, err := store.GetTask(ctx, taskID); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT task_id, turn_number, superseded_at_revision, created_at
		FROM delegation_turns WHERE task_id = ? ORDER BY turn_number`, taskID)
	if err != nil {
		return nil, fmt.Errorf("list delegation turns: %w", err)
	}
	defer rows.Close()
	var turns []Turn
	for rows.Next() {
		turn, err := scanTurn(rows)
		if err != nil {
			return nil, err
		}
		turns = append(turns, turn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delegation turns: %w", err)
	}
	return turns, nil
}

func (store *Store) GetArtifact(ctx context.Context, taskID string, turnNumber int64) (Artifact, error) {
	if strings.TrimSpace(taskID) == "" || turnNumber <= 0 {
		return Artifact{}, fmt.Errorf("%w: task id and positive turn are required", ErrInvalidInput)
	}
	return getArtifact(ctx, store.db, taskID, turnNumber)
}

func (store *Store) LatestArtifact(ctx context.Context, taskID string) (Artifact, error) {
	if strings.TrimSpace(taskID) == "" {
		return Artifact{}, fmt.Errorf("%w: task id is required", ErrInvalidInput)
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Artifact{}, fmt.Errorf("begin latest artifact snapshot: %w", err)
	}
	defer tx.Rollback()
	task, err := getTask(ctx, tx, taskID)
	if err != nil {
		return Artifact{}, err
	}
	if task.LatestTurn == 0 {
		return Artifact{}, fmt.Errorf("%w: task %q is still at its base", ErrNotFound, taskID)
	}
	artifact, err := getArtifact(ctx, tx, taskID, task.LatestTurn)
	if err != nil {
		return Artifact{}, err
	}
	if artifact.Superseded() {
		return Artifact{}, fmt.Errorf("stored latest artifact is superseded: %w", ErrInvalidRewind)
	}
	if err := tx.Commit(); err != nil {
		return Artifact{}, fmt.Errorf("commit latest artifact snapshot: %w", err)
	}
	return artifact, nil
}

// ListEvents returns immutable events whose sequence is greater than after.
func (store *Store) ListEvents(ctx context.Context, taskID string, after int64) ([]Event, error) {
	if after < 0 {
		return nil, fmt.Errorf("%w: event cursor cannot be negative", ErrInvalidInput)
	}
	if _, err := store.GetTask(ctx, taskID); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT task_id, sequence, kind, from_state, to_state, turn_number, data, created_at
		FROM delegation_events
		WHERE task_id = ? AND sequence > ?
		ORDER BY sequence`, taskID, after)
	if err != nil {
		return nil, fmt.Errorf("list delegation events: %w", err)
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delegation events: %w", err)
	}
	return events, nil
}

// LoadSnapshot reads task, turn, artifact, and event state from one SQLite
// snapshot. It is the preferred restart boundary when a consumer must rebuild
// an in-memory task without mixing revisions.
func (store *Store) LoadSnapshot(ctx context.Context, taskID string) (Snapshot, error) {
	if strings.TrimSpace(taskID) == "" {
		return Snapshot{}, fmt.Errorf("%w: task id is required", ErrInvalidInput)
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Snapshot{}, fmt.Errorf("begin delegation recovery snapshot: %w", err)
	}
	defer tx.Rollback()
	task, err := getTask(ctx, tx, taskID)
	if err != nil {
		return Snapshot{}, err
	}
	turnRows, err := tx.QueryContext(ctx, `
		SELECT task_id, turn_number, superseded_at_revision, created_at
		FROM delegation_turns WHERE task_id = ? ORDER BY turn_number`, taskID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("load snapshot turns: %w", err)
	}
	var turns []Turn
	for turnRows.Next() {
		turn, err := scanTurn(turnRows)
		if err != nil {
			turnRows.Close()
			return Snapshot{}, err
		}
		turns = append(turns, turn)
	}
	if err := turnRows.Close(); err != nil {
		return Snapshot{}, fmt.Errorf("close snapshot turns: %w", err)
	}
	if err := turnRows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("iterate snapshot turns: %w", err)
	}

	artifactRows, err := tx.QueryContext(ctx, `
		SELECT artifact_id, task_id, turn_number, media_type, data, metadata, sha256,
		       superseded_at_revision, created_at
		FROM delegation_artifacts WHERE task_id = ? ORDER BY turn_number`, taskID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("load snapshot artifacts: %w", err)
	}
	var artifacts []Artifact
	for artifactRows.Next() {
		artifact, err := scanArtifact(artifactRows)
		if err != nil {
			artifactRows.Close()
			return Snapshot{}, err
		}
		artifacts = append(artifacts, artifact)
	}
	if err := artifactRows.Close(); err != nil {
		return Snapshot{}, fmt.Errorf("close snapshot artifacts: %w", err)
	}
	if err := artifactRows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("iterate snapshot artifacts: %w", err)
	}

	messageRows, err := tx.QueryContext(ctx, `
		SELECT task_id, caller_id, message_id, sequence, role, data, sha256, created_at
		FROM delegation_messages WHERE task_id = ? ORDER BY sequence`, taskID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("load snapshot messages: %w", err)
	}
	var messages []Message
	for messageRows.Next() {
		message, err := scanMessage(messageRows)
		if err != nil {
			messageRows.Close()
			return Snapshot{}, err
		}
		messages = append(messages, message)
	}
	if err := messageRows.Close(); err != nil {
		return Snapshot{}, fmt.Errorf("close snapshot messages: %w", err)
	}
	if err := messageRows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("iterate snapshot messages: %w", err)
	}

	execRows, err := tx.QueryContext(ctx, execSelect+` WHERE task_id = ? ORDER BY request_sequence`, taskID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("load snapshot exec requests: %w", err)
	}
	var execRequests []ExecRequest
	for execRows.Next() {
		request, _, _, err := scanExecRequest(execRows)
		if err != nil {
			execRows.Close()
			return Snapshot{}, err
		}
		execRequests = append(execRequests, request)
	}
	if err := execRows.Close(); err != nil {
		return Snapshot{}, fmt.Errorf("close snapshot exec requests: %w", err)
	}
	if err := execRows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("iterate snapshot exec requests: %w", err)
	}

	eventRows, err := tx.QueryContext(ctx, `
		SELECT task_id, sequence, kind, from_state, to_state, turn_number, data, created_at
		FROM delegation_events WHERE task_id = ? ORDER BY sequence`, taskID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("load snapshot events: %w", err)
	}
	var events []Event
	for eventRows.Next() {
		event, err := scanEvent(eventRows)
		if err != nil {
			eventRows.Close()
			return Snapshot{}, err
		}
		events = append(events, event)
	}
	if err := eventRows.Close(); err != nil {
		return Snapshot{}, fmt.Errorf("close snapshot events: %w", err)
	}
	if err := eventRows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("iterate snapshot events: %w", err)
	}
	snapshot := Snapshot{
		Task: task, Turns: turns, Artifacts: artifacts, Messages: messages,
		Exec: execRequests, Events: events,
	}
	if err := validateSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, fmt.Errorf("commit delegation recovery snapshot: %w", err)
	}
	return snapshot, nil
}

func validateSnapshot(snapshot Snapshot) error {
	task := snapshot.Task
	if int64(len(snapshot.Events)) != task.Revision {
		return fmt.Errorf(
			"task %q revision %d has %d events: %w",
			task.ID, task.Revision, len(snapshot.Events), ErrInvalidInput,
		)
	}
	for index, event := range snapshot.Events {
		if event.Sequence != int64(index+1) {
			return fmt.Errorf("task %q has an event sequence gap at %d: %w",
				task.ID, index+1, ErrInvalidInput)
		}
	}
	if len(snapshot.Events) > 0 && snapshot.Events[len(snapshot.Events)-1].ToState != task.State {
		return fmt.Errorf("task %q state disagrees with its last event: %w", task.ID, ErrInvalidInput)
	}
	var previousMessageSequence int64
	for _, message := range snapshot.Messages {
		if message.TaskID != task.ID || message.CallerID != task.CallerID ||
			message.Sequence <= previousMessageSequence || message.Sequence > task.Revision ||
			message.ID == "" || message.Role == "" {
			return fmt.Errorf("task %q has invalid message %q: %w",
				task.ID, message.ID, ErrInvalidInput)
		}
		if message.SHA256 != digestMessage(message.Role, message.Data) {
			return fmt.Errorf("task %q message %q digest mismatch: %w",
				task.ID, message.ID, ErrInvalidInput)
		}
		previousMessageSequence = message.Sequence
	}
	var previousExecSequence int64
	for _, request := range snapshot.Exec {
		if request.TaskID != task.ID || request.ID == "" || request.WorkerID == "" ||
			request.Command == "" || request.Reason == "" || !request.Status.Valid() ||
			request.RequestSequence <= previousExecSequence || request.RequestSequence > task.Revision {
			return fmt.Errorf("task %q has invalid exec request %q: %w", task.ID, request.ID, ErrInvalidInput)
		}
		if request.Status == ExecPending {
			if request.ResolutionSequence != nil || request.ResolvedAt != nil || request.ResolutionID != "" {
				return fmt.Errorf("task %q pending exec request %q has a resolution: %w", task.ID, request.ID, ErrInvalidInput)
			}
		} else if request.ResolutionSequence == nil || *request.ResolutionSequence > task.Revision ||
			request.ResolvedAt == nil || request.ResolutionID == "" {
			return fmt.Errorf("task %q resolved exec request %q is incomplete: %w", task.ID, request.ID, ErrInvalidInput)
		}
		previousExecSequence = request.RequestSequence
	}
	if len(snapshot.Turns) != len(snapshot.Artifacts) || int64(len(snapshot.Turns))+1 != task.NextTurn {
		return fmt.Errorf(
			"task %q has %d turns, %d artifacts, and next turn %d: %w",
			task.ID, len(snapshot.Turns), len(snapshot.Artifacts), task.NextTurn, ErrInvalidInput,
		)
	}
	foundLatest := task.LatestTurn == 0
	for index, turn := range snapshot.Turns {
		artifact := snapshot.Artifacts[index]
		if turn.Number != int64(index+1) || artifact.TurnNumber != turn.Number ||
			artifact.TaskID != task.ID || turn.TaskID != task.ID {
			return fmt.Errorf("task %q has a non-monotonic turn/artifact pair at index %d: %w",
				task.ID, index, ErrInvalidInput)
		}
		if !sameOptionalInt64(turn.SupersededAtRevision, artifact.SupersededAtRevision) {
			return fmt.Errorf("task %q turn %d disagrees with its artifact supersession: %w",
				task.ID, turn.Number, ErrInvalidInput)
		}
		if turn.SupersededAtRevision != nil && *turn.SupersededAtRevision > task.Revision {
			return fmt.Errorf("task %q turn %d was superseded by a future revision: %w",
				task.ID, turn.Number, ErrInvalidInput)
		}
		if turn.Number > task.LatestTurn && !turn.Superseded() {
			return fmt.Errorf("task %q has a live turn %d newer than its anchor %d: %w",
				task.ID, turn.Number, task.LatestTurn, ErrInvalidRewind)
		}
		if turn.Number == task.LatestTurn {
			if turn.Superseded() {
				return fmt.Errorf("task %q latest turn %d is superseded: %w",
					task.ID, turn.Number, ErrInvalidRewind)
			}
			foundLatest = true
		}
		digest := sha256.Sum256(artifact.Data)
		if artifact.SHA256 != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("task %q artifact %q digest mismatch: %w",
				task.ID, artifact.ID, ErrInvalidInput)
		}
	}
	if !foundLatest {
		return fmt.Errorf("task %q latest turn %d does not exist: %w",
			task.ID, task.LatestTurn, ErrInvalidRewind)
	}
	return nil
}

func sameOptionalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

type rowScanner interface {
	Scan(...any) error
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getTask(ctx context.Context, queryer queryRower, taskID string) (Task, error) {
	row := queryer.QueryRowContext(ctx, `
		SELECT task_id, caller_id, context_id, state, worker_id, latest_turn, next_turn,
		       pending_rewind_to, revision, metadata, created_at, updated_at
		FROM delegation_tasks WHERE task_id = ?`, taskID)
	return scanTask(row)
}

func scanTask(row rowScanner) (Task, error) {
	var task Task
	var state string
	var pending sql.NullInt64
	var created, updated int64
	if err := row.Scan(
		&task.ID, &task.CallerID, &task.ContextID, &state, &task.WorkerID, &task.LatestTurn,
		&task.NextTurn, &pending, &task.Revision, &task.Metadata, &created, &updated,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Task{}, ErrNotFound
		}
		return Task{}, fmt.Errorf("scan delegation task: %w", err)
	}
	task.State = State(state)
	task.PendingRewindTo = nullableInt64(pending)
	task.CreatedAt = fromUnixNano(created)
	task.UpdatedAt = fromUnixNano(updated)
	if err := validateStoredTask(task); err != nil {
		return Task{}, err
	}
	return task, nil
}

func getArtifact(ctx context.Context, queryer queryRower, taskID string, turnNumber int64) (Artifact, error) {
	row := queryer.QueryRowContext(ctx, `
		SELECT artifact_id, task_id, turn_number, media_type, data, metadata, sha256,
		       superseded_at_revision, created_at
		FROM delegation_artifacts WHERE task_id = ? AND turn_number = ?`, taskID, turnNumber)
	return scanArtifact(row)
}

func scanTurn(row rowScanner) (Turn, error) {
	var turn Turn
	var superseded sql.NullInt64
	var created int64
	if err := row.Scan(&turn.TaskID, &turn.Number, &superseded, &created); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Turn{}, ErrNotFound
		}
		return Turn{}, fmt.Errorf("scan delegation turn: %w", err)
	}
	turn.SupersededAtRevision = nullableInt64(superseded)
	turn.CreatedAt = fromUnixNano(created)
	return turn, nil
}

func scanArtifact(row rowScanner) (Artifact, error) {
	var artifact Artifact
	var superseded sql.NullInt64
	var created int64
	if err := row.Scan(
		&artifact.ID, &artifact.TaskID, &artifact.TurnNumber, &artifact.MediaType,
		&artifact.Data, &artifact.Metadata, &artifact.SHA256, &superseded, &created,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Artifact{}, ErrNotFound
		}
		return Artifact{}, fmt.Errorf("scan delegation artifact: %w", err)
	}
	artifact.SupersededAtRevision = nullableInt64(superseded)
	artifact.CreatedAt = fromUnixNano(created)
	return artifact, nil
}

func scanEvent(row rowScanner) (Event, error) {
	var event Event
	var kind, from, to string
	var turn sql.NullInt64
	var created int64
	if err := row.Scan(
		&event.TaskID, &event.Sequence, &kind, &from, &to, &turn,
		&event.Data, &created,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Event{}, ErrNotFound
		}
		return Event{}, fmt.Errorf("scan delegation event: %w", err)
	}
	event.Kind = EventKind(kind)
	event.FromState = State(from)
	event.ToState = State(to)
	event.TurnNumber = nullableInt64(turn)
	event.CreatedAt = fromUnixNano(created)
	return event, nil
}

func validateStoredTask(task Task) error {
	if strings.TrimSpace(task.ID) == "" || strings.TrimSpace(task.CallerID) == "" ||
		strings.TrimSpace(task.ContextID) == "" || !task.State.Valid() ||
		task.Revision < 1 || task.LatestTurn < 0 ||
		task.NextTurn <= task.LatestTurn {
		return fmt.Errorf("invalid stored delegation task %q: %w", task.ID, ErrInvalidInput)
	}
	if task.State == StateRewindPending {
		if task.PendingRewindTo == nil || *task.PendingRewindTo < 0 ||
			*task.PendingRewindTo >= task.LatestTurn {
			return fmt.Errorf("invalid stored rewind for task %q: %w", task.ID, ErrInvalidRewind)
		}
	} else if task.PendingRewindTo != nil {
		return fmt.Errorf("unexpected stored rewind for task %q: %w", task.ID, ErrInvalidRewind)
	}
	return nil
}

func nullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}

func fromUnixNano(value int64) time.Time { return time.Unix(0, value).UTC() }

func isUniqueConstraint(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unique constraint failed")
}

func ensureDelegationColumn(
	ctx context.Context,
	db *sql.DB,
	table string,
	column string,
	alter string,
) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return fmt.Errorf("inspect delegation schema %s.%s: %w", table, column, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("scan delegation schema %s: %w", table, err)
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate delegation schema %s: %w", table, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close delegation schema %s: %w", table, err)
	}
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("add delegation column %s.%s: %w", table, column, err)
	}
	return nil
}
