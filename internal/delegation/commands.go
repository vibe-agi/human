package delegation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
)

func (store *Store) CreateTask(ctx context.Context, input CreateTaskInput) (TransitionResult, error) {
	input.ID = strings.TrimSpace(input.ID)
	input.CallerID = strings.TrimSpace(input.CallerID)
	if input.ID == "" || input.CallerID == "" {
		return TransitionResult{}, fmt.Errorf("%w: task id and caller id are required", ErrInvalidInput)
	}
	input.ContextID = strings.TrimSpace(input.ContextID)
	if input.ContextID == "" {
		input.ContextID = input.ID
	}
	now := store.now().UTC()
	task := Task{
		ID:        input.ID,
		CallerID:  input.CallerID,
		ContextID: input.ContextID,
		State:     StateSubmitted,
		NextTurn:  1,
		Revision:  1,
		Metadata:  cloneBytes(input.Metadata),
		CreatedAt: now,
		UpdatedAt: now,
	}
	event := Event{
		TaskID:    task.ID,
		Sequence:  1,
		Kind:      EventTaskSubmitted,
		ToState:   StateSubmitted,
		Data:      []byte{},
		CreatedAt: now,
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return TransitionResult{}, fmt.Errorf("begin create delegation task: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO delegation_tasks (
		  task_id, caller_id, context_id, state, worker_id, latest_turn, next_turn,
		  pending_rewind_to, revision, metadata, created_at, updated_at
		) VALUES (?, ?, ?, ?, '', 0, 1, NULL, 1, ?, ?, ?)`,
		task.ID, task.CallerID, task.ContextID, task.State, nonNilBytes(task.Metadata),
		now.UnixNano(), now.UnixNano())
	if err != nil {
		if isUniqueConstraint(err) {
			return TransitionResult{}, fmt.Errorf("%w: task %q", ErrAlreadyExists, task.ID)
		}
		return TransitionResult{}, fmt.Errorf("insert delegation task: %w", err)
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return TransitionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return TransitionResult{}, fmt.Errorf("commit delegation task: %w", err)
	}
	return TransitionResult{Task: task, Event: event}, nil
}

func (store *Store) AcceptTask(ctx context.Context, input AcceptTaskInput) (TransitionResult, error) {
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	if input.WorkerID == "" {
		return TransitionResult{}, fmt.Errorf("%w: worker id is required", ErrInvalidInput)
	}
	return store.transition(ctx, input.CommandInput, []State{StateSubmitted}, StateWorking,
		EventTaskAccepted, &input.WorkerID)
}

func (store *Store) RejectTask(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return store.transition(ctx, input, []State{StateSubmitted}, StateRejected,
		EventTaskRejected, nil)
}

func (store *Store) Reply(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return store.transition(ctx, input, []State{StateInputRequired}, StateWorking,
		EventCallerReplied, nil)
}

func (store *Store) CompleteTask(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return store.transition(ctx, input, []State{StateWorking, StateInputRequired}, StateCompleted,
		EventTaskCompleted, nil)
}

func (store *Store) FailTask(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return store.transition(ctx, input, []State{StateWorking, StateInputRequired}, StateFailed,
		EventTaskFailed, nil)
}

func (store *Store) CancelTask(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return store.transition(ctx, input,
		[]State{StateSubmitted, StateWorking, StateInputRequired, StateRewindPending},
		StateCanceled, EventTaskCanceled, nil)
}

func (store *Store) DeliverTurn(ctx context.Context, input DeliverTurnInput) (DeliveryResult, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return DeliveryResult{}, fmt.Errorf("begin delegation delivery: %w", err)
	}
	defer tx.Rollback()
	result, err := store.deliverTurnTx(ctx, tx, input)
	if err != nil {
		return DeliveryResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeliveryResult{}, fmt.Errorf("commit delegation delivery: %w", err)
	}
	return result, nil
}

func (store *Store) deliverTurnTx(ctx context.Context, tx *sql.Tx, input DeliverTurnInput) (DeliveryResult, error) {
	if err := validateCommand(input.CommandInput); err != nil {
		return DeliveryResult{}, err
	}
	input.ArtifactID = strings.TrimSpace(input.ArtifactID)
	input.ArtifactMediaType = strings.TrimSpace(input.ArtifactMediaType)
	if input.ArtifactID == "" || input.ArtifactMediaType == "" {
		return DeliveryResult{}, fmt.Errorf("%w: artifact id and media type are required", ErrInvalidInput)
	}
	task, err := commandTask(ctx, tx, input.CommandInput)
	if err != nil {
		return DeliveryResult{}, err
	}
	if task.State != StateWorking {
		return DeliveryResult{}, transitionError(task.State, StateInputRequired)
	}

	now := store.now().UTC()
	number := task.NextTurn
	nextRevision := task.Revision + 1
	turn := Turn{TaskID: task.ID, Number: number, CreatedAt: now}
	digest := sha256.Sum256(input.ArtifactData)
	artifact := Artifact{
		ID:         input.ArtifactID,
		TaskID:     task.ID,
		TurnNumber: number,
		MediaType:  input.ArtifactMediaType,
		Data:       cloneBytes(input.ArtifactData),
		Metadata:   cloneBytes(input.ArtifactMetadata),
		SHA256:     hex.EncodeToString(digest[:]),
		CreatedAt:  now,
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO delegation_turns (task_id, turn_number, superseded_at_revision, created_at)
		VALUES (?, ?, NULL, ?)`, task.ID, number, now.UnixNano()); err != nil {
		return DeliveryResult{}, fmt.Errorf("insert delegation turn: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO delegation_artifacts (
		  artifact_id, task_id, turn_number, media_type, data, metadata, sha256,
		  superseded_at_revision, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)`,
		artifact.ID, artifact.TaskID, artifact.TurnNumber, artifact.MediaType,
		nonNilBytes(artifact.Data), nonNilBytes(artifact.Metadata), artifact.SHA256,
		now.UnixNano()); err != nil {
		if isUniqueConstraint(err) {
			return DeliveryResult{}, fmt.Errorf("%w: artifact %q", ErrAlreadyExists, artifact.ID)
		}
		return DeliveryResult{}, fmt.Errorf("insert delegation artifact: %w", err)
	}
	task.State = StateInputRequired
	task.LatestTurn = number
	task.NextTurn = number + 1
	task.PendingRewindTo = nil
	task.Revision = nextRevision
	task.UpdatedAt = now
	if err := persistTask(ctx, tx, task, input.ExpectedRevision); err != nil {
		return DeliveryResult{}, err
	}
	event := Event{
		TaskID:     task.ID,
		Sequence:   nextRevision,
		Kind:       EventTurnDelivered,
		FromState:  StateWorking,
		ToState:    StateInputRequired,
		TurnNumber: int64Pointer(number),
		Data:       cloneBytes(input.Data),
		CreatedAt:  now,
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return DeliveryResult{}, err
	}
	return DeliveryResult{Task: task, Turn: turn, Artifact: artifact, Event: event}, nil
}

func (store *Store) RequestRewind(ctx context.Context, input RequestRewindInput) (TransitionResult, error) {
	if err := validateCommand(input.CommandInput); err != nil {
		return TransitionResult{}, err
	}
	if input.TargetTurn < 0 {
		return TransitionResult{}, fmt.Errorf("%w: rewind target cannot be negative", ErrInvalidRewind)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return TransitionResult{}, fmt.Errorf("begin rewind request: %w", err)
	}
	defer tx.Rollback()
	task, err := commandTask(ctx, tx, input.CommandInput)
	if err != nil {
		return TransitionResult{}, err
	}
	if task.State != StateWorking && task.State != StateInputRequired {
		return TransitionResult{}, transitionError(task.State, StateRewindPending)
	}
	if err := ensureLiveRewindTarget(ctx, tx, task, input.TargetTurn); err != nil {
		return TransitionResult{}, err
	}
	from := task.State
	now := store.now().UTC()
	task.State = StateRewindPending
	task.PendingRewindTo = int64Pointer(input.TargetTurn)
	task.Revision++
	task.UpdatedAt = now
	if err := persistTask(ctx, tx, task, input.ExpectedRevision); err != nil {
		return TransitionResult{}, err
	}
	event := Event{
		TaskID: task.ID, Sequence: task.Revision, Kind: EventRewindRequested,
		FromState: from, ToState: StateRewindPending,
		TurnNumber: int64Pointer(input.TargetTurn), Data: cloneBytes(input.Data), CreatedAt: now,
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return TransitionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return TransitionResult{}, fmt.Errorf("commit rewind request: %w", err)
	}
	return TransitionResult{Task: task, Event: event}, nil
}

func (store *Store) ConfirmRewind(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return store.resolveRewind(ctx, input, true)
}

func (store *Store) RejectRewind(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return store.resolveRewind(ctx, input, false)
}

func (store *Store) resolveRewind(
	ctx context.Context,
	input CommandInput,
	confirm bool,
) (TransitionResult, error) {
	if err := validateCommand(input); err != nil {
		return TransitionResult{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return TransitionResult{}, fmt.Errorf("begin rewind resolution: %w", err)
	}
	defer tx.Rollback()
	result, err := store.resolveRewindTx(ctx, tx, input, confirm)
	if err != nil {
		return TransitionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return TransitionResult{}, fmt.Errorf("commit rewind resolution: %w", err)
	}
	return result, nil
}

func (store *Store) resolveRewindTx(
	ctx context.Context,
	tx *sql.Tx,
	input CommandInput,
	confirm bool,
) (TransitionResult, error) {
	if err := validateCommand(input); err != nil {
		return TransitionResult{}, err
	}
	task, err := commandTask(ctx, tx, input)
	if err != nil {
		return TransitionResult{}, err
	}
	if task.State != StateRewindPending || task.PendingRewindTo == nil {
		return TransitionResult{}, transitionError(task.State, StateInputRequired)
	}
	target := *task.PendingRewindTo
	if err := ensureLiveRewindTarget(ctx, tx, task, target); err != nil {
		return TransitionResult{}, err
	}
	now := store.now().UTC()
	nextRevision := task.Revision + 1
	kind := EventRewindRejected
	if confirm {
		kind = EventRewindConfirmed
		if _, err := tx.ExecContext(ctx, `
			UPDATE delegation_turns
			SET superseded_at_revision = ?
			WHERE task_id = ? AND turn_number > ? AND superseded_at_revision IS NULL`,
			nextRevision, task.ID, target); err != nil {
			return TransitionResult{}, fmt.Errorf("supersede delegation turns: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE delegation_artifacts
			SET superseded_at_revision = ?
			WHERE task_id = ? AND turn_number > ? AND superseded_at_revision IS NULL`,
			nextRevision, task.ID, target); err != nil {
			return TransitionResult{}, fmt.Errorf("supersede delegation artifacts: %w", err)
		}
		task.LatestTurn = target
	}
	task.State = StateInputRequired
	task.PendingRewindTo = nil
	task.Revision = nextRevision
	task.UpdatedAt = now
	if err := persistTask(ctx, tx, task, input.ExpectedRevision); err != nil {
		return TransitionResult{}, err
	}
	event := Event{
		TaskID: task.ID, Sequence: nextRevision, Kind: kind,
		FromState: StateRewindPending, ToState: StateInputRequired,
		TurnNumber: int64Pointer(target), Data: cloneBytes(input.Data), CreatedAt: now,
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return TransitionResult{}, err
	}
	return TransitionResult{Task: task, Event: event}, nil
}

func (store *Store) transition(
	ctx context.Context,
	input CommandInput,
	allowed []State,
	to State,
	kind EventKind,
	workerID *string,
) (TransitionResult, error) {
	if err := validateCommand(input); err != nil {
		return TransitionResult{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return TransitionResult{}, fmt.Errorf("begin delegation transition: %w", err)
	}
	defer tx.Rollback()
	result, err := store.transitionTx(ctx, tx, input, allowed, to, kind, workerID)
	if err != nil {
		return TransitionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return TransitionResult{}, fmt.Errorf("commit delegation transition: %w", err)
	}
	return result, nil
}

func (store *Store) transitionTx(
	ctx context.Context,
	tx *sql.Tx,
	input CommandInput,
	allowed []State,
	to State,
	kind EventKind,
	workerID *string,
) (TransitionResult, error) {
	task, err := commandTask(ctx, tx, input)
	if err != nil {
		return TransitionResult{}, err
	}
	if !slices.Contains(allowed, task.State) {
		return TransitionResult{}, transitionError(task.State, to)
	}
	if to.IsTerminal() {
		var pendingExec int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM delegation_exec_requests
			WHERE task_id = ? AND status = 'pending'`, task.ID).Scan(&pendingExec); err != nil {
			return TransitionResult{}, fmt.Errorf("inspect pending command requests: %w", err)
		}
		if pendingExec != 0 {
			return TransitionResult{}, fmt.Errorf(
				"%w: resolve %d pending command request(s) before making task terminal",
				ErrInvalidTransition, pendingExec,
			)
		}
	}
	from := task.State
	now := store.now().UTC()
	task.State = to
	if workerID != nil {
		task.WorkerID = *workerID
	}
	task.PendingRewindTo = nil
	task.Revision++
	task.UpdatedAt = now
	if err := persistTask(ctx, tx, task, input.ExpectedRevision); err != nil {
		return TransitionResult{}, err
	}
	event := Event{
		TaskID: task.ID, Sequence: task.Revision, Kind: kind,
		FromState: from, ToState: to, Data: cloneBytes(input.Data), CreatedAt: now,
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return TransitionResult{}, err
	}
	return TransitionResult{Task: task, Event: event}, nil
}

func commandTask(ctx context.Context, tx *sql.Tx, input CommandInput) (Task, error) {
	task, err := getTask(ctx, tx, input.TaskID)
	if err != nil {
		return Task{}, err
	}
	if task.Revision != input.ExpectedRevision {
		return Task{}, fmt.Errorf(
			"%w: task %q is at revision %d, expected %d",
			ErrRevisionConflict, task.ID, task.Revision, input.ExpectedRevision,
		)
	}
	return task, nil
}

func persistTask(ctx context.Context, tx *sql.Tx, task Task, previousRevision int64) error {
	if err := validateStoredTask(task); err != nil {
		return err
	}
	var pending any
	if task.PendingRewindTo != nil {
		pending = *task.PendingRewindTo
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE delegation_tasks
		SET state = ?, worker_id = ?, latest_turn = ?, next_turn = ?,
		    pending_rewind_to = ?, revision = ?, updated_at = ?
		WHERE task_id = ? AND revision = ?`,
		task.State, task.WorkerID, task.LatestTurn, task.NextTurn, pending,
		task.Revision, task.UpdatedAt.UnixNano(), task.ID, previousRevision)
	if err != nil {
		return fmt.Errorf("update delegation task: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count delegation task update: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("%w: task %q", ErrRevisionConflict, task.ID)
	}
	return nil
}

func insertEvent(ctx context.Context, tx *sql.Tx, event Event) error {
	var turn any
	if event.TurnNumber != nil {
		turn = *event.TurnNumber
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO delegation_events (
		  task_id, sequence, kind, from_state, to_state, turn_number, data, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		event.TaskID, event.Sequence, event.Kind, event.FromState, event.ToState,
		turn, nonNilBytes(event.Data), event.CreatedAt.UnixNano())
	if err != nil {
		return fmt.Errorf("append delegation event: %w", err)
	}
	return nil
}

func ensureLiveRewindTarget(ctx context.Context, tx *sql.Tx, task Task, target int64) error {
	if target < 0 || target >= task.LatestTurn {
		return fmt.Errorf(
			"%w: target %d must be older than latest turn %d",
			ErrInvalidRewind, target, task.LatestTurn,
		)
	}
	if target == 0 {
		return nil
	}
	var superseded sql.NullInt64
	err := tx.QueryRowContext(ctx, `
		SELECT superseded_at_revision FROM delegation_turns
		WHERE task_id = ? AND turn_number = ?`, task.ID, target).Scan(&superseded)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: target turn %d was never delivered", ErrInvalidRewind, target)
	}
	if err != nil {
		return fmt.Errorf("load rewind target: %w", err)
	}
	if superseded.Valid {
		return fmt.Errorf("%w: target turn %d is superseded", ErrInvalidRewind, target)
	}
	return nil
}

func validateCommand(input CommandInput) error {
	if strings.TrimSpace(input.TaskID) == "" || input.ExpectedRevision < 1 {
		return fmt.Errorf("%w: task id and positive expected revision are required", ErrInvalidInput)
	}
	return nil
}

func transitionError(from, to State) error {
	return fmt.Errorf("%w: %q -> %q", ErrInvalidTransition, from, to)
}

func int64Pointer(value int64) *int64 { return &value }

func cloneBytes(value []byte) []byte { return bytes.Clone(nonNilBytes(value)) }

func nonNilBytes(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}
