package agent

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	_ "modernc.org/sqlite"
)

const (
	maxPartBytes        = 2 << 20
	maxPageBytes        = 4 << 20
	defaultArtifactMax  = 16 << 20
	absoluteArtifactMax = 64 << 20
)

var stableID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// Config controls one durable HumanAgent domain instance. This first public
// driver is intentionally single-owner SQLite; DatabasePath is explicit so an
// embedder cannot accidentally share state with an unrelated LLM gateway.
type Config struct {
	// DatabasePath selects the dedicated HumanAgent SQLite identity. Missing
	// parent directories are created privately; do not share this file with LLM.
	DatabasePath string
	// MaxArtifactBytes limits newly frozen payloads. Reads continue to accept
	// already committed payloads up to the schema hard limit of 64 MiB.
	MaxArtifactBytes int64
}

// DefaultConfig returns Agent defaults without selecting a database identity.
func DefaultConfig() Config {
	return Config{MaxArtifactBytes: defaultArtifactMax}
}

func (config Config) withDefaults() (Config, error) {
	config.DatabasePath = strings.TrimSpace(config.DatabasePath)
	if config.DatabasePath == "" {
		return Config{}, fmt.Errorf("%w: agent database path is required", ErrInvalidArgument)
	}
	if config.MaxArtifactBytes == 0 {
		config.MaxArtifactBytes = defaultArtifactMax
	}
	if config.MaxArtifactBytes < 1 || config.MaxArtifactBytes > absoluteArtifactMax {
		return Config{}, fmt.Errorf("%w: artifact byte limit must be 1..%d", ErrInvalidArgument, absoluteArtifactMax)
	}
	return config, nil
}

// Agent owns the durable HumanAgent domain store. It does not own an HTTP
// listener or worker transport; those are adapters over this lifecycle.
type Agent struct {
	database         *sql.DB
	owner            *ownerlock.Lock
	now              func() time.Time
	maxArtifactBytes int64

	lifecycle sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

// Open initializes or recovers a durable task-oriented Agent. ctx controls
// only the open operation; after Open returns, method contexts and Close own
// the Agent lifetime.
func Open(ctx context.Context, config Config) (*Agent, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalidArgument)
	}
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	config.DatabasePath, err = resolveAgentDatabasePath(config.DatabasePath)
	if err != nil {
		return nil, err
	}
	location, err := sqlitefile.PreparePrivate(config.DatabasePath, "HumanAgent database")
	if err != nil {
		return nil, err
	}
	owner, err := ownerlock.Acquire(location, "HumanAgent database")
	if err != nil {
		if errors.Is(err, ownerlock.ErrInUse) {
			return nil, fmt.Errorf("%w: %v", ErrDatabaseInUse, err)
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
		return nil, fmt.Errorf("open HumanAgent sqlite: %w", err)
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
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := database.ExecContext(ctx, pragma); err != nil {
			return nil, fmt.Errorf("configure HumanAgent sqlite: %w", err)
		}
	}
	if err := requireCurrentOrEmptySchema(ctx, database); err != nil {
		return nil, err
	}
	if _, err := database.ExecContext(ctx, agentSchema); err != nil {
		return nil, fmt.Errorf("initialize HumanAgent sqlite schema: %w", err)
	}

	closeDatabase = false
	releaseOwner = false
	return &Agent{
		database: database, owner: owner, now: time.Now,
		maxArtifactBytes: config.MaxArtifactBytes,
	}, nil
}

func resolveAgentDatabasePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve HumanAgent database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", fmt.Errorf("create HumanAgent database directory: %w", err)
	}
	return absolute, nil
}

// Close waits for in-process operations, then closes SQLite and releases its
// cross-process owner lock. It is idempotent.
func (agent *Agent) Close() error {
	if agent == nil {
		return nil
	}
	agent.closeOnce.Do(func() {
		agent.lifecycle.Lock()
		defer agent.lifecycle.Unlock()
		agent.closed = true
		if agent.database != nil {
			agent.closeErr = agent.database.Close()
		}
		if agent.owner != nil {
			agent.closeErr = errors.Join(agent.closeErr, agent.owner.Close())
		}
	})
	return agent.closeErr
}

func (agent *Agent) acquire() (*sql.DB, func(), error) {
	if agent == nil {
		return nil, func() {}, ErrClosed
	}
	agent.lifecycle.RLock()
	if agent.closed || agent.database == nil {
		agent.lifecycle.RUnlock()
		return nil, func() {}, ErrClosed
	}
	return agent.database, agent.lifecycle.RUnlock, nil
}

func (agent *Agent) CreateTask(ctx context.Context, command CreateTaskCommand) (Task, error) {
	if err := validateCallContext(ctx); err != nil {
		return Task{}, err
	}
	command.Message = cloneMessageInput(command.Message)
	if err := validateCreateCommand(command); err != nil {
		return Task{}, err
	}
	digest, err := commandDigest("create_task", command)
	if err != nil {
		return Task{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return Task{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, fmt.Errorf("begin create Agent task: %w", err)
	}
	defer tx.Rollback()
	if replay, found, err := replayTaskCommand(ctx, tx, command.Task.Workspace.Authority, command.Meta.ID, "create_task", digest); err != nil {
		return Task{}, err
	} else if found {
		if replay.Ref != command.Task || replay.Context != command.Context {
			return Task{}, fmt.Errorf("%w: create command result identity mismatch", ErrCorruptStore)
		}
		return replay, nil
	}
	now := agent.now().UTC()
	task := Task{
		Ref: command.Task, Context: command.Context, State: TaskSubmitted,
		Revision: 1, MessageCount: 1, EventCount: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_tasks (
		  authority_id, workspace_id, task_id, context_id, state,
		  revision, message_count, event_count, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, 1, 1, 1, ?, ?)`,
		command.Task.Workspace.Authority, command.Task.Workspace.ID, command.Task.ID,
		command.Context.ID, TaskSubmitted, unixNano(now), unixNano(now),
	); err != nil {
		if uniqueConstraint(err) {
			return Task{}, ErrTaskConflict
		}
		return Task{}, fmt.Errorf("insert Agent task: %w", err)
	}
	message := Message{
		ID: command.Message.ID, Task: command.Task, Sequence: 1,
		Author: AuthorCaller, Parts: cloneParts(command.Message.Parts), CreatedAt: now,
	}
	if err := insertMessage(ctx, tx, message); err != nil {
		return Task{}, err
	}
	event := Event{
		Task: command.Task, Sequence: 1, Type: EventTaskSubmitted,
		State: TaskSubmitted, Revision: 1, Message: message.ID, OccurredAt: now,
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return Task{}, err
	}
	if err := recordTaskCommand(ctx, tx, command.Task.Workspace.Authority, command.Meta.ID, "create_task", digest, task, now); err != nil {
		return Task{}, err
	}
	if err := tx.Commit(); err != nil {
		return Task{}, fmt.Errorf("commit Agent task creation: %w", err)
	}
	return cloneTask(task), nil
}

func (agent *Agent) AcceptTask(ctx context.Context, command TaskCommand) (Task, error) {
	return agent.transition(ctx, "accept_task", command.Meta, command.Task, command, transition{
		allowed: []TaskState{TaskSubmitted}, next: TaskWorking, event: EventTaskAccepted,
	})
}

func (agent *Agent) RejectTask(ctx context.Context, command TaskCommand) (Task, error) {
	return agent.transition(ctx, "reject_task", command.Meta, command.Task, command, transition{
		allowed: []TaskState{TaskSubmitted}, next: TaskRejected, event: EventTaskRejected,
	})
}

func (agent *Agent) RequestInput(ctx context.Context, command MessageCommand) (Task, error) {
	if err := validateCallContext(ctx); err != nil {
		return Task{}, err
	}
	command.Message = cloneMessageInput(command.Message)
	if err := validateMessageInput(command.Message); err != nil {
		return Task{}, err
	}
	return agent.transition(ctx, "request_input", command.Meta, command.Task, command, transition{
		allowed: []TaskState{TaskWorking}, next: TaskInputRequired,
		event: EventInputRequired, author: AuthorAgent, message: &command.Message,
	})
}

func (agent *Agent) ReplyTask(ctx context.Context, command MessageCommand) (Task, error) {
	if err := validateCallContext(ctx); err != nil {
		return Task{}, err
	}
	command.Message = cloneMessageInput(command.Message)
	if err := validateMessageInput(command.Message); err != nil {
		return Task{}, err
	}
	return agent.transition(ctx, "reply_task", command.Meta, command.Task, command, transition{
		allowed: []TaskState{TaskInputRequired}, next: TaskWorking,
		event: EventCallerReplied, author: AuthorCaller, message: &command.Message,
	})
}

func (agent *Agent) CancelTask(ctx context.Context, command TaskCommand) (Task, error) {
	return agent.transition(ctx, "cancel_task", command.Meta, command.Task, command, transition{
		allowed: []TaskState{TaskSubmitted, TaskWorking, TaskInputRequired},
		next:    TaskCanceled, event: EventTaskCanceled, discardArtifact: true,
	})
}

func (agent *Agent) FailTask(ctx context.Context, command TaskCommand) (Task, error) {
	return agent.transition(ctx, "fail_task", command.Meta, command.Task, command, transition{
		allowed: []TaskState{TaskWorking, TaskInputRequired},
		next:    TaskFailed, event: EventTaskFailed, discardArtifact: true,
	})
}

func (agent *Agent) CompleteTask(ctx context.Context, command CompleteTaskCommand) (Task, error) {
	if err := validateCallContext(ctx); err != nil {
		return Task{}, err
	}
	command.Message = cloneMessageInput(command.Message)
	if command.Artifact != nil {
		artifact := *command.Artifact
		command.Artifact = &artifact
		if err := validateArtifactRef(artifact); err != nil || artifact.Workspace != command.Task.Workspace {
			return Task{}, fmt.Errorf("%w: completion Artifact does not belong to Task workspace", ErrInvalidArgument)
		}
	}
	if err := validateStable("submission id", string(command.Submission)); err != nil {
		return Task{}, err
	}
	if err := validateMessageInput(command.Message); err != nil {
		return Task{}, err
	}
	return agent.transition(ctx, "complete_task", command.Meta, command.Task, command, transition{
		allowed: []TaskState{TaskWorking}, next: TaskCompleted,
		event: EventTaskCompleted, author: AuthorAgent, message: &command.Message,
		submission: command.Submission, artifact: command.Artifact,
	})
}

type transition struct {
	allowed         []TaskState
	next            TaskState
	event           EventType
	author          Author
	message         *MessageInput
	submission      SubmissionID
	artifact        *ArtifactRef
	discardArtifact bool
}

func (agent *Agent) transition(
	ctx context.Context,
	kind string,
	meta CommandMeta,
	ref TaskRef,
	command any,
	change transition,
) (Task, error) {
	if err := validateCallContext(ctx); err != nil {
		return Task{}, err
	}
	if err := validateMeta(meta, false); err != nil {
		return Task{}, err
	}
	if err := validateTaskRef(ref); err != nil {
		return Task{}, err
	}
	digest, err := commandDigest(kind, command)
	if err != nil {
		return Task{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return Task{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, fmt.Errorf("begin %s: %w", kind, err)
	}
	defer tx.Rollback()
	if replay, found, err := replayTaskCommand(ctx, tx, ref.Workspace.Authority, meta.ID, kind, digest); err != nil {
		return Task{}, err
	} else if found {
		if replay.Ref != ref {
			return Task{}, fmt.Errorf("%w: transition command result identity mismatch", ErrCorruptStore)
		}
		return replay, nil
	}
	current, err := loadTask(ctx, tx, ref)
	if err != nil {
		return Task{}, err
	}
	if current.Revision != meta.ExpectedRevision {
		return Task{}, &RevisionConflictError{Expected: meta.ExpectedRevision, Actual: current.Revision}
	}
	if current.State.Terminal() {
		return Task{}, &TransitionError{Operation: kind, State: current.State, Terminal: true}
	}
	if !containsState(change.allowed, current.State) {
		return Task{}, &TransitionError{Operation: kind, State: current.State}
	}
	if current.Revision >= math.MaxInt64 || current.EventCount >= math.MaxInt64 ||
		(change.message != nil && current.MessageCount >= math.MaxInt64) {
		return Task{}, fmt.Errorf("%w: task counters exhausted SQLite integer range", ErrRevisionConflict)
	}

	now := timestampAtLeast(agent.now(), current.UpdatedAt)
	next := current
	next.State = change.next
	next.Revision++
	next.EventCount++
	next.UpdatedAt = now
	var message Message
	if change.message != nil {
		next.MessageCount++
		message = Message{
			ID: change.message.ID, Task: ref, Sequence: next.MessageCount,
			Author: change.author, Parts: cloneParts(change.message.Parts), CreatedAt: now,
		}
	}
	if change.submission != "" {
		if (current.Artifact == nil) != (change.artifact == nil) ||
			(current.Artifact != nil && *current.Artifact != *change.artifact) {
			return Task{}, fmt.Errorf("%w: completion must publish the Task's exact frozen Artifact", ErrArtifactState)
		}
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE agent_tasks
		SET state = ?, revision = ?, message_count = ?, event_count = ?, updated_at = ?
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND revision = ?`,
		next.State, next.Revision, next.MessageCount, next.EventCount, unixNano(now),
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID, meta.ExpectedRevision,
	)
	if err != nil {
		return Task{}, fmt.Errorf("update Agent task for %s: %w", kind, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return Task{}, fmt.Errorf("inspect Agent task update for %s: %w", kind, err)
	}
	if affected != 1 {
		latest, loadErr := loadTask(ctx, tx, ref)
		if loadErr != nil {
			return Task{}, loadErr
		}
		return Task{}, &RevisionConflictError{Expected: meta.ExpectedRevision, Actual: latest.Revision}
	}
	if change.message != nil {
		if err := insertMessage(ctx, tx, message); err != nil {
			return Task{}, err
		}
	}
	if change.discardArtifact && current.Artifact != nil {
		result, err := tx.ExecContext(ctx, `
			UPDATE agent_artifacts
			SET state = ?, discarded_at = ?
			WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?
			  AND task_id = ? AND state = ?`,
			ArtifactDiscarded, unixNano(now), ref.Workspace.Authority, ref.Workspace.ID,
			current.Artifact.ID, ref.ID, ArtifactFrozen,
		)
		if err != nil {
			return Task{}, fmt.Errorf("discard Agent Artifact: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil || affected != 1 {
			return Task{}, fmt.Errorf("%w: Task Artifact is not frozen", ErrArtifactState)
		}
	}
	if change.artifact != nil {
		result, err := tx.ExecContext(ctx, `
			UPDATE agent_artifacts
			SET state = ?, published_at = ?
			WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?
			  AND task_id = ? AND state = ?`,
			ArtifactPublished, unixNano(now), ref.Workspace.Authority, ref.Workspace.ID,
			change.artifact.ID, ref.ID, ArtifactFrozen,
		)
		if err != nil {
			return Task{}, fmt.Errorf("publish Agent Artifact: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil || affected != 1 {
			return Task{}, fmt.Errorf("%w: Task Artifact is not frozen", ErrArtifactState)
		}
	}
	if change.submission != "" {
		submission := Submission{
			ID: change.submission, Task: ref, FinalMessage: message.ID,
			Artifact: change.artifact, PublishedAt: now,
		}
		var artifactID any
		if change.artifact != nil {
			artifactID = change.artifact.ID
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO agent_submissions (
			  authority_id, workspace_id, task_id, submission_id,
			  final_message_id, artifact_id, published_at
			) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
			submission.ID, submission.FinalMessage, artifactID, unixNano(now),
		); err != nil {
			if uniqueConstraint(err) {
				return Task{}, ErrSubmissionConflict
			}
			return Task{}, fmt.Errorf("publish Agent submission: %w", err)
		}
		next.Submission = &submission
	}
	event := Event{
		Task: ref, Sequence: next.EventCount, Type: change.event,
		State: next.State, Revision: next.Revision, OccurredAt: now,
	}
	if change.message != nil {
		event.Message = message.ID
	}
	if change.submission != "" {
		event.Submission = change.submission
	}
	if change.artifact != nil {
		event.Artifact = change.artifact.ID
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return Task{}, err
	}
	if err := recordTaskCommand(ctx, tx, ref.Workspace.Authority, meta.ID, kind, digest, next, now); err != nil {
		return Task{}, err
	}
	if err := tx.Commit(); err != nil {
		return Task{}, fmt.Errorf("commit %s: %w", kind, err)
	}
	return cloneTask(next), nil
}

func (agent *Agent) GetTask(ctx context.Context, ref TaskRef) (Task, error) {
	if err := validateCallContext(ctx); err != nil {
		return Task{}, err
	}
	if err := validateTaskRef(ref); err != nil {
		return Task{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return Task{}, err
	}
	defer release()
	return loadTask(ctx, database, ref)
}

func (agent *Agent) ListTasks(ctx context.Context, contextRef ContextRef, request TaskPageRequest) (TaskPage, error) {
	if err := validateCallContext(ctx); err != nil {
		return TaskPage{}, err
	}
	if err := validateContextRef(contextRef); err != nil {
		return TaskPage{}, err
	}
	limit, err := normalizePageLimit(request.Limit)
	if err != nil {
		return TaskPage{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return TaskPage{}, err
	}
	defer release()
	const selectTasks = `
		SELECT t.workspace_id, t.task_id, t.context_id, t.state, t.revision,
		       t.message_count, t.event_count, t.created_at, t.updated_at,
		       a.artifact_id, a.state,
		       s.submission_id, s.final_message_id, s.artifact_id, s.published_at
		FROM agent_tasks AS t
		LEFT JOIN agent_artifacts AS a
		  ON a.authority_id = t.authority_id
		 AND a.workspace_id = t.workspace_id
		 AND a.task_id = t.task_id
		LEFT JOIN agent_submissions AS s
		  ON s.authority_id = t.authority_id
		 AND s.workspace_id = t.workspace_id
		 AND s.task_id = t.task_id
		WHERE t.authority_id = ? AND t.context_id = ?`
	var rows *sql.Rows
	if request.After == nil {
		rows, err = database.QueryContext(ctx, selectTasks+`
			ORDER BY t.created_at, t.workspace_id, t.task_id
			LIMIT ?`, contextRef.Authority, contextRef.ID, limit+1)
	} else {
		if err := validateTaskCursor(*request.After); err != nil {
			return TaskPage{}, err
		}
		created := unixNano(request.After.CreatedAt.UTC())
		rows, err = database.QueryContext(ctx, selectTasks+`
			AND (t.created_at > ? OR
			     (t.created_at = ? AND
			       (t.workspace_id > ? OR
			        (t.workspace_id = ? AND t.task_id > ?))))
			ORDER BY t.created_at, t.workspace_id, t.task_id
			LIMIT ?`, contextRef.Authority, contextRef.ID,
			created, created, request.After.Workspace, request.After.Workspace,
			request.After.Task, limit+1)
	}
	if err != nil {
		return TaskPage{}, fmt.Errorf("list Agent tasks: %w", err)
	}
	defer rows.Close()
	tasks := make([]Task, 0, limit+1)
	for rows.Next() {
		task, err := scanTask(rows, contextRef.Authority, true)
		if err != nil {
			return TaskPage{}, err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return TaskPage{}, fmt.Errorf("list Agent tasks: %w", err)
	}
	page := TaskPage{Items: tasks}
	if len(page.Items) > limit {
		page.HasMore = true
		page.Items = page.Items[:limit]
	}
	if page.HasMore && len(page.Items) > 0 {
		last := page.Items[len(page.Items)-1]
		page.Next = &TaskPageCursor{
			CreatedAt: last.CreatedAt, Workspace: last.Ref.Workspace.ID, Task: last.Ref.ID,
		}
	}
	return page, nil
}

func (agent *Agent) ListMessages(ctx context.Context, ref TaskRef, request PageRequest) (MessagePage, error) {
	if err := validateCallContext(ctx); err != nil {
		return MessagePage{}, err
	}
	if err := validateTaskRef(ref); err != nil {
		return MessagePage{}, err
	}
	limit, err := normalizePageRequest(request)
	if err != nil {
		return MessagePage{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return MessagePage{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return MessagePage{}, fmt.Errorf("begin Agent message page: %w", err)
	}
	defer tx.Rollback()
	task, err := loadTask(ctx, tx, ref)
	if err != nil {
		return MessagePage{}, err
	}
	if request.After > task.MessageCount {
		return MessagePage{}, fmt.Errorf("%w: message cursor exceeds Task history", ErrInvalidArgument)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT message_id, sequence, author,
		       CASE WHEN length(parts) <= ? THEN parts ELSE NULL END,
		       digest, created_at
		FROM agent_messages
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND sequence > ?
		ORDER BY sequence
		LIMIT ?`, maxPageBytes, ref.Workspace.Authority, ref.Workspace.ID, ref.ID, request.After, limit+1)
	if err != nil {
		return MessagePage{}, fmt.Errorf("list Agent messages: %w", err)
	}
	defer rows.Close()
	page := MessagePage{Items: make([]Message, 0, limit)}
	encodedBytes := 0
	expected := request.After + 1
	for rows.Next() {
		var message Message
		var parts []byte
		var digest string
		var created int64
		message.Task = ref
		if err := rows.Scan(&message.ID, &message.Sequence, &message.Author, &parts, &digest, &created); err != nil {
			return MessagePage{}, fmt.Errorf("scan Agent message: %w", err)
		}
		if message.Sequence != expected {
			return MessagePage{}, fmt.Errorf("%w: Agent message history has a sequence gap", ErrCorruptStore)
		}
		if len(page.Items) == limit || (len(page.Items) > 0 && encodedBytes+len(parts) > maxPageBytes) {
			page.HasMore = true
			break
		}
		if err := json.Unmarshal(parts, &message.Parts); err != nil {
			return MessagePage{}, fmt.Errorf("%w: decode Agent message %q: %v", ErrCorruptStore, message.ID, err)
		}
		actual, err := contentDigest(message.Parts)
		if err != nil || actual != digest {
			return MessagePage{}, fmt.Errorf("%w: Agent message %q content digest mismatch", ErrCorruptStore, message.ID)
		}
		message.CreatedAt = fromUnixNano(created)
		if err := validateStoredMessage(message); err != nil {
			return MessagePage{}, err
		}
		encodedBytes += len(parts)
		page.Items = append(page.Items, cloneMessage(message))
		expected++
	}
	if err := rows.Err(); err != nil {
		return MessagePage{}, fmt.Errorf("list Agent messages: %w", err)
	}
	page.Next = request.After
	if len(page.Items) > 0 {
		page.Next = page.Items[len(page.Items)-1].Sequence
	}
	if !page.HasMore && page.Next != task.MessageCount {
		return MessagePage{}, fmt.Errorf("%w: Agent message history does not match Task count", ErrCorruptStore)
	}
	if err := rows.Close(); err != nil {
		return MessagePage{}, fmt.Errorf("close Agent message page: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return MessagePage{}, fmt.Errorf("commit Agent message page: %w", err)
	}
	return page, nil
}

func (agent *Agent) ReadEvents(ctx context.Context, ref TaskRef, request PageRequest) (EventPage, error) {
	if err := validateCallContext(ctx); err != nil {
		return EventPage{}, err
	}
	if err := validateTaskRef(ref); err != nil {
		return EventPage{}, err
	}
	limit, err := normalizePageRequest(request)
	if err != nil {
		return EventPage{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return EventPage{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return EventPage{}, fmt.Errorf("begin Agent event page: %w", err)
	}
	defer tx.Rollback()
	task, err := loadTask(ctx, tx, ref)
	if err != nil {
		return EventPage{}, err
	}
	if request.After > task.EventCount {
		return EventPage{}, fmt.Errorf("%w: event cursor exceeds Task history", ErrInvalidArgument)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT sequence, kind, state, revision, message_id, submission_id, artifact_id, created_at
		FROM agent_events
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND sequence > ?
		ORDER BY sequence
		LIMIT ?`, ref.Workspace.Authority, ref.Workspace.ID, ref.ID, request.After, limit+1)
	if err != nil {
		return EventPage{}, fmt.Errorf("read Agent events: %w", err)
	}
	defer rows.Close()
	page := EventPage{Items: make([]Event, 0, limit)}
	expected := request.After + 1
	for rows.Next() {
		var event Event
		var occurred int64
		event.Task = ref
		if err := rows.Scan(
			&event.Sequence, &event.Type, &event.State, &event.Revision,
			&event.Message, &event.Submission, &event.Artifact, &occurred,
		); err != nil {
			return EventPage{}, fmt.Errorf("scan Agent event: %w", err)
		}
		if event.Sequence != expected {
			return EventPage{}, fmt.Errorf("%w: Agent event history has a sequence gap", ErrCorruptStore)
		}
		if len(page.Items) == limit {
			page.HasMore = true
			break
		}
		event.OccurredAt = fromUnixNano(occurred)
		if err := validateStoredEvent(event); err != nil {
			return EventPage{}, err
		}
		page.Items = append(page.Items, event)
		expected++
	}
	if err := rows.Err(); err != nil {
		return EventPage{}, fmt.Errorf("read Agent events: %w", err)
	}
	page.Next = request.After
	if len(page.Items) > 0 {
		page.Next = page.Items[len(page.Items)-1].Sequence
	}
	if !page.HasMore && page.Next != task.EventCount {
		return EventPage{}, fmt.Errorf("%w: Agent event history does not match Task count", ErrCorruptStore)
	}
	if err := rows.Close(); err != nil {
		return EventPage{}, fmt.Errorf("close Agent event page: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return EventPage{}, fmt.Errorf("commit Agent event page: %w", err)
	}
	return page, nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type rowScanner interface {
	Scan(...any) error
}

func scanTask(scanner rowScanner, authority AuthorityID, includesRef bool, refs ...TaskRef) (Task, error) {
	var task Task
	if !includesRef {
		if len(refs) != 1 {
			return Task{}, fmt.Errorf("%w: internal task reference is missing", ErrCorruptStore)
		}
		task.Ref = refs[0]
	}
	var created, updated int64
	var artifactID, artifactState, submissionID, finalMessageID, submissionArtifactID sql.NullString
	var publishedAt sql.NullInt64
	destinations := make([]any, 0, 12)
	if includesRef {
		destinations = append(destinations, &task.Ref.Workspace.ID, &task.Ref.ID)
		task.Ref.Workspace.Authority = authority
	}
	destinations = append(destinations,
		&task.Context.ID, &task.State, &task.Revision, &task.MessageCount,
		&task.EventCount, &created, &updated,
		&artifactID, &artifactState, &submissionID, &finalMessageID, &submissionArtifactID, &publishedAt,
	)
	if err := scanner.Scan(destinations...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Task{}, sql.ErrNoRows
		}
		return Task{}, fmt.Errorf("scan Agent task: %w", err)
	}
	task.Context.Authority = authority
	task.CreatedAt = fromUnixNano(created)
	task.UpdatedAt = fromUnixNano(updated)
	if artifactID.Valid {
		task.Artifact = &ArtifactRef{Workspace: task.Ref.Workspace, ID: ArtifactID(artifactID.String)}
	}
	if artifactID.Valid != artifactState.Valid {
		return Task{}, fmt.Errorf("%w: partial Artifact for task %q", ErrCorruptStore, task.Ref.ID)
	}
	if submissionID.Valid || finalMessageID.Valid || publishedAt.Valid {
		if !submissionID.Valid || !finalMessageID.Valid || !publishedAt.Valid {
			return Task{}, fmt.Errorf("%w: partial submission for task %q", ErrCorruptStore, task.Ref.ID)
		}
		task.Submission = &Submission{
			ID: SubmissionID(submissionID.String), Task: task.Ref,
			FinalMessage: MessageID(finalMessageID.String), PublishedAt: fromUnixNano(publishedAt.Int64),
		}
		if submissionArtifactID.Valid {
			task.Submission.Artifact = &ArtifactRef{
				Workspace: task.Ref.Workspace, ID: ArtifactID(submissionArtifactID.String),
			}
		}
	} else if submissionArtifactID.Valid {
		return Task{}, fmt.Errorf("%w: orphaned submission Artifact for task %q", ErrCorruptStore, task.Ref.ID)
	}
	if err := validateStoredTask(task); err != nil {
		return Task{}, err
	}
	if artifactState.Valid {
		state := ArtifactState(artifactState.String)
		valid := (state == ArtifactFrozen && (task.State == TaskWorking || task.State == TaskInputRequired)) ||
			(state == ArtifactPublished && task.State == TaskCompleted) ||
			(state == ArtifactDiscarded && (task.State == TaskCanceled || task.State == TaskFailed))
		if !valid {
			return Task{}, fmt.Errorf("%w: Artifact state %q does not match task %q state %q", ErrCorruptStore, state, task.Ref.ID, task.State)
		}
	}
	return task, nil
}

func loadTask(ctx context.Context, query queryer, ref TaskRef) (Task, error) {
	row := query.QueryRowContext(ctx, `
		SELECT t.context_id, t.state, t.revision, t.message_count, t.event_count,
		       t.created_at, t.updated_at,
		       a.artifact_id, a.state,
		       s.submission_id, s.final_message_id, s.artifact_id, s.published_at
		FROM agent_tasks AS t
		LEFT JOIN agent_artifacts AS a
		  ON a.authority_id = t.authority_id
		 AND a.workspace_id = t.workspace_id
		 AND a.task_id = t.task_id
		LEFT JOIN agent_submissions AS s
		  ON s.authority_id = t.authority_id
		 AND s.workspace_id = t.workspace_id
		 AND s.task_id = t.task_id
		WHERE t.authority_id = ? AND t.workspace_id = ? AND t.task_id = ?`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
	)
	task, err := scanTask(row, ref.Workspace.Authority, false, ref)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Task{}, ErrNotFound
		}
		return Task{}, err
	}
	return cloneTask(task), nil
}

func validateStoredTask(task Task) error {
	if err := validateTaskRef(task.Ref); err != nil {
		return fmt.Errorf("%w: invalid task key: %v", ErrCorruptStore, err)
	}
	if err := validateContextRef(task.Context); err != nil || task.Context.Authority != task.Ref.Workspace.Authority {
		return fmt.Errorf("%w: invalid context for task %q", ErrCorruptStore, task.Ref.ID)
	}
	if !validTaskState(task.State) || task.Revision == 0 || task.Revision > math.MaxInt64 ||
		task.MessageCount == 0 || task.MessageCount > math.MaxInt64 ||
		task.EventCount == 0 || task.EventCount > math.MaxInt64 ||
		task.Revision != task.EventCount || task.MessageCount > task.Revision ||
		task.CreatedAt.IsZero() || task.UpdatedAt.Before(task.CreatedAt) {
		return fmt.Errorf("%w: invalid counters, timestamps, or state for task %q", ErrCorruptStore, task.Ref.ID)
	}
	if (task.State == TaskCompleted) != (task.Submission != nil) {
		return fmt.Errorf("%w: completed/submission invariant failed for task %q", ErrCorruptStore, task.Ref.ID)
	}
	if task.Artifact != nil {
		if err := validateArtifactRef(*task.Artifact); err != nil || task.Artifact.Workspace != task.Ref.Workspace {
			return fmt.Errorf("%w: invalid Artifact reference for task %q", ErrCorruptStore, task.Ref.ID)
		}
	}
	if task.Submission != nil {
		if task.Submission.Task != task.Ref || validateStable("submission id", string(task.Submission.ID)) != nil ||
			validateStable("final message id", string(task.Submission.FinalMessage)) != nil ||
			task.Submission.PublishedAt.IsZero() || !task.Submission.PublishedAt.Equal(task.UpdatedAt) {
			return fmt.Errorf("%w: invalid submission for task %q", ErrCorruptStore, task.Ref.ID)
		}
		if (task.Artifact == nil) != (task.Submission.Artifact == nil) ||
			(task.Artifact != nil && *task.Artifact != *task.Submission.Artifact) {
			return fmt.Errorf("%w: submission Artifact invariant failed for task %q", ErrCorruptStore, task.Ref.ID)
		}
	}
	return nil
}

func validateStoredMessage(message Message) error {
	if err := validateTaskRef(message.Task); err != nil {
		return fmt.Errorf("%w: invalid message task: %v", ErrCorruptStore, err)
	}
	wantAuthor := AuthorAgent
	if message.Sequence%2 == 1 {
		wantAuthor = AuthorCaller
	}
	if message.Sequence == 0 || message.Sequence > math.MaxInt64 ||
		message.Author != wantAuthor || message.CreatedAt.IsZero() {
		return fmt.Errorf("%w: invalid message %q", ErrCorruptStore, message.ID)
	}
	if err := validateMessageInput(MessageInput{ID: message.ID, Parts: message.Parts}); err != nil {
		return fmt.Errorf("%w: invalid message %q: %v", ErrCorruptStore, message.ID, err)
	}
	return nil
}

func validateStoredEvent(event Event) error {
	if err := validateTaskRef(event.Task); err != nil || event.Sequence == 0 || event.Sequence > math.MaxInt64 ||
		event.Revision == 0 || event.Revision > math.MaxInt64 || event.Sequence != event.Revision ||
		event.OccurredAt.IsZero() {
		return fmt.Errorf("%w: invalid event sequence for task %q", ErrCorruptStore, event.Task.ID)
	}
	wantState := map[EventType]TaskState{
		EventTaskSubmitted: TaskSubmitted, EventTaskAccepted: TaskWorking,
		EventTaskRejected: TaskRejected, EventInputRequired: TaskInputRequired,
		EventCallerReplied: TaskWorking, EventTaskCanceled: TaskCanceled,
		EventTaskFailed: TaskFailed, EventTaskCompleted: TaskCompleted,
		EventArtifactFrozen: TaskWorking,
	}[event.Type]
	if wantState == "" || event.State != wantState {
		return fmt.Errorf("%w: invalid event %q for task %q", ErrCorruptStore, event.Type, event.Task.ID)
	}
	wantsMessage := event.Type == EventTaskSubmitted || event.Type == EventInputRequired ||
		event.Type == EventCallerReplied || event.Type == EventTaskCompleted
	hasArtifact := event.Artifact != ""
	artifactInvalid := (event.Type == EventArtifactFrozen && !hasArtifact) ||
		(event.Type != EventArtifactFrozen && event.Type != EventTaskCompleted && hasArtifact)
	if wantsMessage != (event.Message != "") || (event.Type == EventTaskCompleted) != (event.Submission != "") || artifactInvalid {
		return fmt.Errorf("%w: invalid event references for task %q", ErrCorruptStore, event.Task.ID)
	}
	return nil
}

func validTaskState(state TaskState) bool {
	switch state {
	case TaskSubmitted, TaskWorking, TaskInputRequired, TaskCompleted, TaskCanceled, TaskRejected, TaskFailed:
		return true
	default:
		return false
	}
}

func replayTaskCommand(
	ctx context.Context,
	tx *sql.Tx,
	authority AuthorityID,
	id CommandID,
	kind, digest string,
) (Task, bool, error) {
	var storedKind, storedDigest, resultKind, storedResultDigest string
	var result []byte
	err := tx.QueryRowContext(ctx, `
		SELECT kind, digest, result_kind, result, result_digest
		FROM agent_commands
		WHERE authority_id = ? AND command_id = ?`, authority, id,
	).Scan(&storedKind, &storedDigest, &resultKind, &result, &storedResultDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, fmt.Errorf("lookup Agent command: %w", err)
	}
	if byteDigest(result) != storedResultDigest {
		return Task{}, false, fmt.Errorf("%w: Agent command result digest mismatch", ErrCorruptStore)
	}
	if storedKind != kind || storedDigest != digest || resultKind != "task" {
		return Task{}, false, ErrIdempotencyConflict
	}
	var task Task
	if err := json.Unmarshal(result, &task); err != nil {
		return Task{}, false, fmt.Errorf("decode Agent command result: %w", err)
	}
	if err := validateStoredTask(task); err != nil {
		return Task{}, false, fmt.Errorf("decode Agent command result: %w", err)
	}
	if task.Ref.Workspace.Authority != authority {
		return Task{}, false, fmt.Errorf("%w: command result authority mismatch", ErrCorruptStore)
	}
	return cloneTask(task), true, nil
}

func recordTaskCommand(
	ctx context.Context,
	tx *sql.Tx,
	authority AuthorityID,
	id CommandID,
	kind, digest string,
	task Task,
	now time.Time,
) error {
	result, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("encode Agent command result: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_commands (
		  authority_id, command_id, kind, digest, result_kind, result, result_digest, created_at
		) VALUES (?, ?, ?, ?, 'task', ?, ?, ?)`, authority, id, kind, digest,
		result, byteDigest(result), unixNano(now)); err != nil {
		if uniqueConstraint(err) {
			return ErrIdempotencyConflict
		}
		return fmt.Errorf("record Agent command: %w", err)
	}
	return nil
}

func insertMessage(ctx context.Context, tx *sql.Tx, message Message) error {
	parts, err := json.Marshal(message.Parts)
	if err != nil {
		return fmt.Errorf("encode Agent message: %w", err)
	}
	digest, err := contentDigest(message.Parts)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_messages (
		  authority_id, message_id, workspace_id, task_id, sequence,
		  author, parts, digest, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		message.Task.Workspace.Authority, message.ID, message.Task.Workspace.ID,
		message.Task.ID, message.Sequence, message.Author, parts, digest,
		unixNano(message.CreatedAt),
	); err != nil {
		if uniqueConstraint(err) {
			return ErrMessageConflict
		}
		return fmt.Errorf("insert Agent message: %w", err)
	}
	return nil
}

func insertEvent(ctx context.Context, tx *sql.Tx, event Event) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_events (
		  authority_id, workspace_id, task_id, sequence, kind, state,
		  revision, message_id, submission_id, artifact_id, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.Task.Workspace.Authority, event.Task.Workspace.ID, event.Task.ID,
		event.Sequence, event.Type, event.State, event.Revision, event.Message,
		event.Submission, event.Artifact, unixNano(event.OccurredAt),
	); err != nil {
		return fmt.Errorf("append Agent event: %w", err)
	}
	return nil
}

func validateCreateCommand(command CreateTaskCommand) error {
	if err := validateMeta(command.Meta, true); err != nil {
		return err
	}
	if err := validateTaskRef(command.Task); err != nil {
		return err
	}
	if err := validateContextRef(command.Context); err != nil {
		return err
	}
	if command.Task.Workspace.Authority != command.Context.Authority {
		return fmt.Errorf("%w: Task Workspace and Context authorities differ", ErrInvalidArgument)
	}
	return validateMessageInput(command.Message)
}

func validateCallContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidArgument)
	}
	return nil
}

func normalizePageLimit(limit int) (int, error) {
	if limit == 0 {
		return MaxPageSize, nil
	}
	if limit < 0 || limit > MaxPageSize {
		return 0, fmt.Errorf("%w: page limit must be 1..%d or zero", ErrInvalidArgument, MaxPageSize)
	}
	return limit, nil
}

func normalizePageRequest(request PageRequest) (int, error) {
	if request.After > math.MaxInt64 {
		return 0, fmt.Errorf("%w: page cursor exceeds SQLite integer range", ErrInvalidArgument)
	}
	return normalizePageLimit(request.Limit)
}

func validateTaskCursor(cursor TaskPageCursor) error {
	if cursor.CreatedAt.IsZero() || !fromUnixNano(unixNano(cursor.CreatedAt.UTC())).Equal(cursor.CreatedAt.UTC()) {
		return fmt.Errorf("%w: task page cursor time is outside SQLite nanosecond range", ErrInvalidArgument)
	}
	if err := validateStable("cursor workspace id", string(cursor.Workspace)); err != nil {
		return err
	}
	return validateStable("cursor task id", string(cursor.Task))
}

func validateMeta(meta CommandMeta, create bool) error {
	if err := validateStable("command id", string(meta.ID)); err != nil {
		return err
	}
	if create && meta.ExpectedRevision != 0 {
		return fmt.Errorf("%w: create expected revision must be zero", ErrInvalidArgument)
	}
	if !create && meta.ExpectedRevision == 0 {
		return fmt.Errorf("%w: expected revision must be positive", ErrInvalidArgument)
	}
	if meta.ExpectedRevision > math.MaxInt64 {
		return fmt.Errorf("%w: expected revision exceeds SQLite integer range", ErrInvalidArgument)
	}
	return nil
}

func validateTaskRef(ref TaskRef) error {
	if err := validateWorkspaceRef(ref.Workspace); err != nil {
		return err
	}
	return validateStable("task id", string(ref.ID))
}

func validateWorkspaceRef(ref WorkspaceRef) error {
	if err := validateStable("authority id", string(ref.Authority)); err != nil {
		return err
	}
	return validateStable("workspace id", string(ref.ID))
}

func validateContextRef(ref ContextRef) error {
	if err := validateStable("authority id", string(ref.Authority)); err != nil {
		return err
	}
	return validateStable("context id", string(ref.ID))
}

func validateMessageInput(input MessageInput) error {
	if err := validateStable("message id", string(input.ID)); err != nil {
		return err
	}
	if len(input.Parts) == 0 || len(input.Parts) > 32 {
		return fmt.Errorf("%w: message must contain 1..32 parts", ErrInvalidArgument)
	}
	total := 0
	for _, part := range input.Parts {
		mediaType := strings.TrimSpace(part.MediaType)
		if mediaType == "" || mediaType != part.MediaType || len(mediaType) > 128 || !utf8.ValidString(mediaType) || strings.ContainsAny(mediaType, "\r\n\x00") {
			return fmt.Errorf("%w: message media type is invalid", ErrInvalidArgument)
		}
		if _, _, err := mime.ParseMediaType(mediaType); err != nil {
			return fmt.Errorf("%w: message media type is invalid: %v", ErrInvalidArgument, err)
		}
		if len(part.Data) == 0 {
			return fmt.Errorf("%w: message parts must not be empty", ErrInvalidArgument)
		}
		total += len(part.Data)
		if total > maxPartBytes {
			return fmt.Errorf("%w: message exceeds %d bytes", ErrInvalidArgument, maxPartBytes)
		}
	}
	return nil
}

func validateStable(label, value string) error {
	if !stableID.MatchString(value) {
		return fmt.Errorf("%w: %s must match %s", ErrInvalidArgument, label, stableID.String())
	}
	return nil
}

func commandDigest(kind string, command any) (string, error) {
	encoded, err := json.Marshal(struct {
		Kind    string `json:"kind"`
		Command any    `json:"command"`
	}{Kind: kind, Command: command})
	if err != nil {
		return "", fmt.Errorf("encode Agent command identity: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func contentDigest(parts []Part) (string, error) {
	encoded, err := json.Marshal(parts)
	if err != nil {
		return "", fmt.Errorf("encode Agent message identity: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func byteDigest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func timestampAtLeast(candidate time.Time, floors ...time.Time) time.Time {
	candidate = candidate.UTC()
	for _, floor := range floors {
		floor = floor.UTC()
		if candidate.Before(floor) {
			candidate = floor
		}
	}
	return candidate
}

func containsState(states []TaskState, candidate TaskState) bool {
	for _, state := range states {
		if state == candidate {
			return true
		}
	}
	return false
}

func cloneParts(parts []Part) []Part {
	cloned := make([]Part, len(parts))
	for index, part := range parts {
		cloned[index] = Part{MediaType: part.MediaType, Data: append([]byte(nil), part.Data...)}
	}
	return cloned
}

func cloneMessageInput(input MessageInput) MessageInput {
	input.Parts = cloneParts(input.Parts)
	return input
}

func cloneMessage(message Message) Message {
	message.Parts = cloneParts(message.Parts)
	return message
}

func cloneTask(task Task) Task {
	if task.Artifact != nil {
		artifact := *task.Artifact
		task.Artifact = &artifact
	}
	if task.Submission != nil {
		submission := *task.Submission
		if submission.Artifact != nil {
			artifact := *submission.Artifact
			submission.Artifact = &artifact
		}
		task.Submission = &submission
	}
	return task
}

func uniqueConstraint(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unique constraint failed")
}

func unixNano(value time.Time) int64 { return value.UnixNano() }

func fromUnixNano(value int64) time.Time { return time.Unix(0, value).UTC() }
