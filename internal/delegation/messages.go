package delegation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// CreateTaskWithMessage atomically creates a submitted task and its initial
// caller message. The caller-scoped message ID is the idempotency key: an exact
// retry returns the existing task even when the caller did not receive its ID.
func (store *Store) CreateTaskWithMessage(
	ctx context.Context,
	input CreateTaskInput,
	messageInput MessageInput,
) (MessageResult, error) {
	input.ID = strings.TrimSpace(input.ID)
	input.CallerID = strings.TrimSpace(input.CallerID)
	input.ContextID = strings.TrimSpace(input.ContextID)
	if input.ID == "" || input.CallerID == "" {
		return MessageResult{}, fmt.Errorf("%w: task id and caller id are required", ErrInvalidInput)
	}
	if input.ContextID == "" {
		input.ContextID = input.ID
	}
	messageInput.ID = strings.TrimSpace(messageInput.ID)
	messageInput.Role = strings.TrimSpace(messageInput.Role)
	if messageInput.ID == "" || messageInput.Role == "" {
		return MessageResult{}, fmt.Errorf("%w: message id and role are required", ErrInvalidInput)
	}
	digest := digestMessage(messageInput.Role, messageInput.Data)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return MessageResult{}, fmt.Errorf("begin delegation task message: %w", err)
	}
	defer tx.Rollback()
	existing, err := getMessage(ctx, tx, input.CallerID, messageInput.ID)
	if err == nil {
		if existing.SHA256 != digest || existing.Role != messageInput.Role {
			return MessageResult{}, fmt.Errorf("%w: message %q", ErrIdempotencyConflict, messageInput.ID)
		}
		task, err := getTask(ctx, tx, existing.TaskID)
		if err != nil {
			return MessageResult{}, err
		}
		event, err := getEvent(ctx, tx, task.ID, existing.Sequence)
		if err != nil {
			return MessageResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return MessageResult{}, fmt.Errorf("commit delegation message replay: %w", err)
		}
		return MessageResult{Task: task, Message: existing, Event: event, Replay: true}, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return MessageResult{}, err
	}

	now := store.now().UTC()
	task := Task{
		ID: input.ID, CallerID: input.CallerID, ContextID: input.ContextID,
		State: StateSubmitted, NextTurn: 1, Revision: 1,
		Metadata: cloneBytes(input.Metadata), CreatedAt: now, UpdatedAt: now,
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO delegation_tasks (
		  task_id, caller_id, context_id, state, worker_id, latest_turn, next_turn,
		  pending_rewind_to, revision, metadata, created_at, updated_at
		) VALUES (?, ?, ?, ?, '', 0, 1, NULL, 1, ?, ?, ?)`,
		task.ID, task.CallerID, task.ContextID, task.State,
		nonNilBytes(task.Metadata), now.UnixNano(), now.UnixNano())
	if err != nil {
		if isUniqueConstraint(err) {
			return MessageResult{}, fmt.Errorf("%w: task %q", ErrAlreadyExists, task.ID)
		}
		return MessageResult{}, fmt.Errorf("insert delegation task with message: %w", err)
	}
	message := Message{
		TaskID: task.ID, CallerID: task.CallerID, ID: messageInput.ID,
		Sequence: 1, Role: messageInput.Role, Data: cloneBytes(messageInput.Data),
		SHA256: digest, CreatedAt: now,
	}
	if err := insertMessage(ctx, tx, message); err != nil {
		return MessageResult{}, err
	}
	event := Event{
		TaskID: task.ID, Sequence: 1, Kind: EventTaskSubmitted,
		ToState: StateSubmitted, Data: cloneBytes(messageInput.Data), CreatedAt: now,
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return MessageResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return MessageResult{}, fmt.Errorf("commit delegation task with message: %w", err)
	}
	return MessageResult{Task: task, Message: message, Event: event}, nil
}

// AppendMessage appends one idempotently keyed message. Input-required tasks
// resume working; other non-terminal tasks retain their state so steering can
// be queued without inventing an A2A lifecycle transition.
func (store *Store) AppendMessage(
	ctx context.Context,
	input CommandInput,
	messageInput MessageInput,
) (MessageResult, error) {
	if err := validateCommand(input); err != nil {
		return MessageResult{}, err
	}
	messageInput.ID = strings.TrimSpace(messageInput.ID)
	messageInput.Role = strings.TrimSpace(messageInput.Role)
	if messageInput.ID == "" || messageInput.Role == "" {
		return MessageResult{}, fmt.Errorf("%w: message id and role are required", ErrInvalidInput)
	}
	digest := digestMessage(messageInput.Role, messageInput.Data)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return MessageResult{}, fmt.Errorf("begin append delegation message: %w", err)
	}
	defer tx.Rollback()
	task, err := getTask(ctx, tx, input.TaskID)
	if err != nil {
		return MessageResult{}, err
	}
	existing, err := getMessage(ctx, tx, task.CallerID, messageInput.ID)
	if err == nil {
		if existing.TaskID != task.ID || existing.SHA256 != digest || existing.Role != messageInput.Role {
			return MessageResult{}, fmt.Errorf("%w: message %q", ErrIdempotencyConflict, messageInput.ID)
		}
		event, err := getEvent(ctx, tx, task.ID, existing.Sequence)
		if err != nil {
			return MessageResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return MessageResult{}, fmt.Errorf("commit appended message replay: %w", err)
		}
		return MessageResult{Task: task, Message: existing, Event: event, Replay: true}, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return MessageResult{}, err
	}
	if task.Revision != input.ExpectedRevision {
		return MessageResult{}, fmt.Errorf(
			"%w: task %q is at revision %d, expected %d",
			ErrRevisionConflict, task.ID, task.Revision, input.ExpectedRevision,
		)
	}
	if task.State.IsTerminal() {
		return MessageResult{}, transitionError(task.State, task.State)
	}

	from := task.State
	to := from
	kind := EventMessageAppended
	if from == StateInputRequired {
		to = StateWorking
		kind = EventCallerReplied
	}
	now := store.now().UTC()
	task.State = to
	task.Revision++
	task.UpdatedAt = now
	message := Message{
		TaskID: task.ID, CallerID: task.CallerID, ID: messageInput.ID,
		Sequence: task.Revision, Role: messageInput.Role, Data: cloneBytes(messageInput.Data),
		SHA256: digest, CreatedAt: now,
	}
	if err := insertMessage(ctx, tx, message); err != nil {
		return MessageResult{}, err
	}
	if err := persistTask(ctx, tx, task, input.ExpectedRevision); err != nil {
		return MessageResult{}, err
	}
	event := Event{
		TaskID: task.ID, Sequence: task.Revision, Kind: kind,
		FromState: from, ToState: to, TurnNumber: int64Pointer(task.LatestTurn),
		Data: cloneBytes(messageInput.Data), CreatedAt: now,
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return MessageResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return MessageResult{}, fmt.Errorf("commit appended delegation message: %w", err)
	}
	return MessageResult{Task: task, Message: message, Event: event}, nil
}

func (store *Store) ListMessages(ctx context.Context, taskID string) ([]Message, error) {
	if _, err := store.GetTask(ctx, taskID); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT task_id, caller_id, message_id, sequence, role, data, sha256, created_at
		FROM delegation_messages WHERE task_id = ? ORDER BY sequence`, taskID)
	if err != nil {
		return nil, fmt.Errorf("list delegation messages: %w", err)
	}
	defer rows.Close()
	var messages []Message
	for rows.Next() {
		message, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delegation messages: %w", err)
	}
	return messages, nil
}

func getMessage(
	ctx context.Context,
	queryer queryRower,
	callerID string,
	messageID string,
) (Message, error) {
	row := queryer.QueryRowContext(ctx, `
		SELECT task_id, caller_id, message_id, sequence, role, data, sha256, created_at
		FROM delegation_messages WHERE caller_id = ? AND message_id = ?`, callerID, messageID)
	return scanMessage(row)
}

func scanMessage(row rowScanner) (Message, error) {
	var message Message
	var created int64
	if err := row.Scan(
		&message.TaskID, &message.CallerID, &message.ID, &message.Sequence,
		&message.Role, &message.Data, &message.SHA256, &created,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Message{}, ErrNotFound
		}
		return Message{}, fmt.Errorf("scan delegation message: %w", err)
	}
	message.CreatedAt = fromUnixNano(created)
	return message, nil
}

func insertMessage(ctx context.Context, tx *sql.Tx, message Message) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO delegation_messages (
		  caller_id, message_id, task_id, sequence, role, data, sha256, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		message.CallerID, message.ID, message.TaskID, message.Sequence, message.Role,
		nonNilBytes(message.Data), message.SHA256, message.CreatedAt.UnixNano())
	if err != nil {
		if isUniqueConstraint(err) {
			return fmt.Errorf("%w: message %q", ErrIdempotencyConflict, message.ID)
		}
		return fmt.Errorf("insert delegation message: %w", err)
	}
	return nil
}

func getEvent(ctx context.Context, queryer queryRower, taskID string, sequence int64) (Event, error) {
	row := queryer.QueryRowContext(ctx, `
		SELECT task_id, sequence, kind, from_state, to_state, turn_number, data, created_at
		FROM delegation_events WHERE task_id = ? AND sequence = ?`, taskID, sequence)
	return scanEvent(row)
}

func digestMessage(role string, data []byte) string {
	digest := sha256.New()
	digest.Write([]byte(role))
	digest.Write([]byte{0})
	digest.Write(data)
	return hex.EncodeToString(digest.Sum(nil))
}
