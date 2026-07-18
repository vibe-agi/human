package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

// ClaimLease atomically selects and grants the oldest claimable Task within an
// authority. Exact retries observe the originally committed assignment even
// after that Task advances, terminates, or its lease is fenced.
func (agent *Agent) ClaimLease(ctx context.Context, command ClaimLeaseCommand) (LeaseAssignment, error) {
	if err := validateCallContext(ctx); err != nil {
		return LeaseAssignment{}, err
	}
	if err := validateStable("command id", string(command.ID)); err != nil {
		return LeaseAssignment{}, err
	}
	if err := validateStable("authority id", string(command.Authority)); err != nil {
		return LeaseAssignment{}, err
	}
	if err := validateStable("worker id", string(command.Worker)); err != nil {
		return LeaseAssignment{}, err
	}
	digest, err := commandDigest("claim_lease", command)
	if err != nil {
		return LeaseAssignment{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return LeaseAssignment{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return LeaseAssignment{}, fmt.Errorf("begin Agent lease claim: %w", err)
	}
	defer tx.Rollback()

	var replay LeaseAssignment
	if found, err := replayJSONCommand(
		ctx, tx, command.Authority, command.ID,
		"claim_lease", digest, "lease_assignment", &replay,
	); err != nil {
		return LeaseAssignment{}, err
	} else if found {
		if err := validateLeaseAssignment(replay); err != nil ||
			replay.Grant.Task.Workspace.Authority != command.Authority ||
			replay.Grant.Worker != command.Worker {
			return LeaseAssignment{}, fmt.Errorf("%w: invalid replayed Agent lease claim", ErrCorruptStore)
		}
		if err := verifyLeaseHistory(ctx, tx, replay); err != nil {
			return LeaseAssignment{}, err
		}
		return cloneLeaseAssignment(replay), nil
	}

	var ref TaskRef
	ref.Workspace.Authority = command.Authority
	if err := tx.QueryRowContext(ctx, `
		SELECT workspace_id, task_id
		FROM agent_tasks
		WHERE authority_id = ?
		  AND lease_owner = ''
		  AND state NOT IN ('completed', 'canceled', 'rejected', 'failed')
		ORDER BY created_at, workspace_id, task_id
		LIMIT 1`, command.Authority,
	).Scan(&ref.Workspace.ID, &ref.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return LeaseAssignment{}, ErrLeaseNotFound
		}
		return LeaseAssignment{}, fmt.Errorf("select claimable Agent Task: %w", err)
	}
	if err := validateTaskRef(ref); err != nil {
		return LeaseAssignment{}, fmt.Errorf("%w: invalid claimable Agent Task identity", ErrCorruptStore)
	}
	task, err := loadTask(ctx, tx, ref)
	if err != nil {
		return LeaseAssignment{}, err
	}
	owner, fence, previousGrantedAt, err := loadLeaseState(ctx, tx, ref)
	if err != nil {
		return LeaseAssignment{}, err
	}
	if task.State.Terminal() || owner != "" {
		return LeaseAssignment{}, fmt.Errorf("%w: selected Agent Task is not claimable", ErrCorruptStore)
	}
	if fence >= LeaseFence(math.MaxInt64) {
		return LeaseAssignment{}, ErrLeaseFenceExhausted
	}

	nextFence := fence + 1
	now := timestampAtLeast(agent.now(), task.UpdatedAt, previousGrantedAt)
	assignment := LeaseAssignment{
		Grant: LeaseGrant{Task: ref, Worker: command.Worker, Fence: nextFence},
		Task:  task, GrantedAt: now,
	}
	if err := validateLeaseAssignment(assignment); err != nil {
		return LeaseAssignment{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_lease_grants (
		  authority_id, workspace_id, task_id, fence, worker_id, granted_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		command.Authority, ref.Workspace.ID, ref.ID,
		nextFence, command.Worker, unixNano(now),
	); err != nil {
		if uniqueConstraint(err) {
			return LeaseAssignment{}, fmt.Errorf("%w: duplicate Agent lease generation", ErrCorruptStore)
		}
		return LeaseAssignment{}, fmt.Errorf("record claimed Agent lease grant: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE agent_tasks
		SET lease_owner = ?, lease_fence = ?
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ?
		  AND lease_owner = '' AND lease_fence = ?
		  AND state NOT IN ('completed', 'canceled', 'rejected', 'failed')`,
		command.Worker, nextFence, command.Authority, ref.Workspace.ID, ref.ID, fence,
	)
	if err != nil {
		return LeaseAssignment{}, fmt.Errorf("claim Agent lease: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return LeaseAssignment{}, fmt.Errorf("inspect Agent lease claim: %w", err)
	}
	if affected != 1 {
		return LeaseAssignment{}, ErrLeaseUnavailable
	}
	if err := recordJSONCommand(
		ctx, tx, command.Authority, command.ID,
		"claim_lease", digest, "lease_assignment", assignment, now,
	); err != nil {
		return LeaseAssignment{}, err
	}
	if err := tx.Commit(); err != nil {
		return LeaseAssignment{}, fmt.Errorf("commit Agent lease claim: %w", err)
	}
	return cloneLeaseAssignment(assignment), nil
}
