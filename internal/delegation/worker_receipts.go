package delegation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// WorkerCommandAuthority is the transaction-local delegation authority used
// while applying one private worker command. Implementations must keep every
// method call in the same transaction as the command receipt.
type WorkerCommandAuthority interface {
	GetTask(context.Context, string) (Task, error)
	AcceptTask(context.Context, AcceptTaskInput) (TransitionResult, error)
	RejectTask(context.Context, CommandInput) (TransitionResult, error)
	DeliverTurn(context.Context, DeliverTurnInput) (DeliveryResult, error)
	RequestExec(context.Context, RequestExecInput) (ExecResult, error)
	CompleteTask(context.Context, CommandInput) (TransitionResult, error)
	FailTask(context.Context, CommandInput) (TransitionResult, error)
	ConfirmRewind(context.Context, CommandInput) (TransitionResult, error)
	RejectRewind(context.Context, CommandInput) (TransitionResult, error)
}

// WorkerCommandApply encodes the transport result while the authority
// transaction is still open. commitEffect is false for a stable domain
// rejection: ExecuteWorkerCommand rolls the effect back to a savepoint but
// still commits the encoded rejection receipt. Infrastructure failures are
// returned and roll back both effect and receipt.
type WorkerCommandApply func(context.Context, WorkerCommandAuthority) (result []byte, commitEffect bool, err error)

// WorkerCommandReceipt is the durable result of one worker transport command.
// EventID is globally unique within the delegation authority. WorkerID and
// CommandDigest prevent another worker or a changed payload from reusing it.
type WorkerCommandReceipt struct {
	EventID       string
	WorkerID      string
	TaskID        string
	CommandDigest string
	Result        []byte
	CreatedAt     time.Time
}

// ExecuteWorkerCommand atomically applies one worker command and stores the
// exact result that will be returned on the private transport. A replay reads
// that result without re-entering domain logic. This closes the otherwise
// unavoidable crash window between a committed revision change and receipt
// publication.
func (store *Store) ExecuteWorkerCommand(
	ctx context.Context,
	receipt WorkerCommandReceipt,
	apply WorkerCommandApply,
) (WorkerCommandReceipt, bool, error) {
	receipt.EventID = strings.TrimSpace(receipt.EventID)
	receipt.WorkerID = strings.TrimSpace(receipt.WorkerID)
	receipt.TaskID = strings.TrimSpace(receipt.TaskID)
	receipt.CommandDigest = strings.TrimSpace(receipt.CommandDigest)
	if receipt.EventID == "" || receipt.WorkerID == "" || receipt.TaskID == "" ||
		receipt.CommandDigest == "" || apply == nil {
		return WorkerCommandReceipt{}, false, fmt.Errorf("%w: command receipt identity and apply callback are required", ErrInvalidInput)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerCommandReceipt{}, false, fmt.Errorf("begin atomic worker command: %w", err)
	}
	defer tx.Rollback()

	existing, err := lookupWorkerCommandReceipt(ctx, tx, receipt.EventID)
	if err == nil {
		if existing.WorkerID != receipt.WorkerID || existing.TaskID != receipt.TaskID ||
			existing.CommandDigest != receipt.CommandDigest {
			return WorkerCommandReceipt{}, false, fmt.Errorf("%w: worker command event %q was reused", ErrIdempotencyConflict, receipt.EventID)
		}
		if err := tx.Commit(); err != nil {
			return WorkerCommandReceipt{}, false, fmt.Errorf("commit worker command replay: %w", err)
		}
		return existing, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return WorkerCommandReceipt{}, false, err
	}

	if _, err := tx.ExecContext(ctx, "SAVEPOINT worker_command_effect"); err != nil {
		return WorkerCommandReceipt{}, false, fmt.Errorf("create worker command savepoint: %w", err)
	}
	result, commitEffect, err := apply(ctx, &workerCommandTx{store: store, tx: tx})
	if err != nil {
		return WorkerCommandReceipt{}, false, err
	}
	if !commitEffect {
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO worker_command_effect"); err != nil {
			return WorkerCommandReceipt{}, false, fmt.Errorf("roll back rejected worker command: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, "RELEASE worker_command_effect"); err != nil {
		return WorkerCommandReceipt{}, false, fmt.Errorf("release worker command savepoint: %w", err)
	}
	if len(result) == 0 {
		return WorkerCommandReceipt{}, false, fmt.Errorf("%w: worker command result is required", ErrInvalidInput)
	}
	receipt.Result = cloneBytes(result)
	if receipt.CreatedAt.IsZero() {
		receipt.CreatedAt = store.now().UTC()
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO delegation_worker_command_receipts (
		  event_id, worker_id, task_id, command_digest, result, created_at
		) VALUES (?, ?, ?, ?, ?, ?)`, receipt.EventID, receipt.WorkerID, receipt.TaskID,
		receipt.CommandDigest, nonNilBytes(receipt.Result), receipt.CreatedAt.UnixNano()); err != nil {
		if isUniqueConstraint(err) {
			return WorkerCommandReceipt{}, false, fmt.Errorf("%w: worker command event %q raced", ErrIdempotencyConflict, receipt.EventID)
		}
		return WorkerCommandReceipt{}, false, fmt.Errorf("insert atomic worker command receipt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return WorkerCommandReceipt{}, false, fmt.Errorf("commit atomic worker command: %w", err)
	}
	return receipt, false, nil
}

type workerCommandTx struct {
	store *Store
	tx    *sql.Tx
}

func (authority *workerCommandTx) GetTask(ctx context.Context, taskID string) (Task, error) {
	return getTask(ctx, authority.tx, taskID)
}

func (authority *workerCommandTx) AcceptTask(ctx context.Context, input AcceptTaskInput) (TransitionResult, error) {
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	if input.WorkerID == "" {
		return TransitionResult{}, fmt.Errorf("%w: worker id is required", ErrInvalidInput)
	}
	return authority.store.transitionTx(ctx, authority.tx, input.CommandInput,
		[]State{StateSubmitted}, StateWorking, EventTaskAccepted, &input.WorkerID)
}

func (authority *workerCommandTx) RejectTask(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return authority.store.transitionTx(ctx, authority.tx, input,
		[]State{StateSubmitted}, StateRejected, EventTaskRejected, nil)
}

func (authority *workerCommandTx) CompleteTask(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return authority.store.transitionTx(ctx, authority.tx, input,
		[]State{StateWorking, StateInputRequired}, StateCompleted, EventTaskCompleted, nil)
}

func (authority *workerCommandTx) FailTask(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return authority.store.transitionTx(ctx, authority.tx, input,
		[]State{StateWorking, StateInputRequired}, StateFailed, EventTaskFailed, nil)
}

func (authority *workerCommandTx) DeliverTurn(ctx context.Context, input DeliverTurnInput) (DeliveryResult, error) {
	return authority.store.deliverTurnTx(ctx, authority.tx, input)
}

func (authority *workerCommandTx) RequestExec(ctx context.Context, input RequestExecInput) (ExecResult, error) {
	return authority.store.requestExecTx(ctx, authority.tx, input)
}

func (authority *workerCommandTx) ConfirmRewind(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return authority.store.resolveRewindTx(ctx, authority.tx, input, true)
}

func (authority *workerCommandTx) RejectRewind(ctx context.Context, input CommandInput) (TransitionResult, error) {
	return authority.store.resolveRewindTx(ctx, authority.tx, input, false)
}

// LookupWorkerCommandReceipt returns a previously committed command result.
// A reused event ID with a different principal or command is a conflict, not a
// cache miss, so callers cannot execute the changed command.
func (store *Store) LookupWorkerCommandReceipt(
	ctx context.Context,
	eventID string,
	workerID string,
	commandDigest string,
) (WorkerCommandReceipt, error) {
	eventID = strings.TrimSpace(eventID)
	workerID = strings.TrimSpace(workerID)
	commandDigest = strings.TrimSpace(commandDigest)
	if eventID == "" || workerID == "" || commandDigest == "" {
		return WorkerCommandReceipt{}, fmt.Errorf("%w: receipt identity is required", ErrInvalidInput)
	}
	receipt, err := lookupWorkerCommandReceipt(ctx, store.db, eventID)
	if err != nil {
		return WorkerCommandReceipt{}, err
	}
	if receipt.WorkerID != workerID || receipt.CommandDigest != commandDigest {
		return WorkerCommandReceipt{}, fmt.Errorf("%w: worker command event %q was reused", ErrIdempotencyConflict, eventID)
	}
	return receipt, nil
}

type workerCommandReceiptQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func lookupWorkerCommandReceipt(
	ctx context.Context,
	queryer workerCommandReceiptQueryer,
	eventID string,
) (WorkerCommandReceipt, error) {
	var receipt WorkerCommandReceipt
	var createdAt int64
	err := queryer.QueryRowContext(ctx, `
		SELECT event_id, worker_id, task_id, command_digest, result, created_at
		FROM delegation_worker_command_receipts WHERE event_id = ?`, eventID).Scan(
		&receipt.EventID, &receipt.WorkerID, &receipt.TaskID, &receipt.CommandDigest,
		&receipt.Result, &createdAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return WorkerCommandReceipt{}, fmt.Errorf("%w: worker command receipt %q", ErrNotFound, eventID)
		}
		return WorkerCommandReceipt{}, fmt.Errorf("lookup worker command receipt: %w", err)
	}
	receipt.Result = cloneBytes(receipt.Result)
	receipt.CreatedAt = time.Unix(0, createdAt).UTC()
	return receipt, nil
}
