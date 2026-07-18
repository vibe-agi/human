package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"
)

// AcquireLease durably grants one unleased, non-terminal Task to a worker.
// The returned assignment is not observable until the grant, immutable grant
// history, and command receipt have committed together.
func (agent *Agent) AcquireLease(ctx context.Context, command AcquireLeaseCommand) (LeaseAssignment, error) {
	if err := validateCallContext(ctx); err != nil {
		return LeaseAssignment{}, err
	}
	if err := validateStable("command id", string(command.ID)); err != nil {
		return LeaseAssignment{}, err
	}
	if err := validateTaskRef(command.Task); err != nil {
		return LeaseAssignment{}, err
	}
	if err := validateStable("worker id", string(command.Worker)); err != nil {
		return LeaseAssignment{}, err
	}
	digest, err := commandDigest("acquire_lease", command)
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
		return LeaseAssignment{}, fmt.Errorf("begin Agent lease acquisition: %w", err)
	}
	defer tx.Rollback()

	var replay LeaseAssignment
	if found, err := replayJSONCommand(
		ctx, tx, command.Task.Workspace.Authority, command.ID,
		"acquire_lease", digest, "lease_assignment", &replay,
	); err != nil {
		return LeaseAssignment{}, err
	} else if found {
		if err := validateLeaseAssignment(replay); err != nil ||
			replay.Grant.Task != command.Task || replay.Grant.Worker != command.Worker {
			return LeaseAssignment{}, fmt.Errorf("%w: invalid replayed Agent lease assignment", ErrCorruptStore)
		}
		if err := verifyLeaseHistory(ctx, tx, replay); err != nil {
			return LeaseAssignment{}, err
		}
		return cloneLeaseAssignment(replay), nil
	}

	task, err := loadTask(ctx, tx, command.Task)
	if err != nil {
		return LeaseAssignment{}, err
	}
	owner, fence, previousGrantedAt, err := loadLeaseState(ctx, tx, command.Task)
	if err != nil {
		return LeaseAssignment{}, err
	}
	if task.State.Terminal() {
		return LeaseAssignment{}, &TransitionError{Operation: "acquire_lease", State: task.State, Terminal: true}
	}
	if owner != "" {
		return LeaseAssignment{}, ErrLeaseUnavailable
	}
	if fence >= LeaseFence(math.MaxInt64) {
		return LeaseAssignment{}, ErrLeaseFenceExhausted
	}
	nextFence := fence + 1
	now := timestampAtLeast(agent.now(), task.UpdatedAt, previousGrantedAt)
	grant := LeaseGrant{Task: command.Task, Worker: command.Worker, Fence: nextFence}
	assignment := LeaseAssignment{Grant: grant, Task: task, GrantedAt: now}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_lease_grants (
		  authority_id, workspace_id, task_id, fence, worker_id, granted_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		command.Task.Workspace.Authority, command.Task.Workspace.ID, command.Task.ID,
		nextFence, command.Worker, unixNano(now),
	); err != nil {
		if uniqueConstraint(err) {
			return LeaseAssignment{}, fmt.Errorf("%w: duplicate Agent lease generation", ErrCorruptStore)
		}
		return LeaseAssignment{}, fmt.Errorf("record Agent lease grant: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE agent_tasks
		SET lease_owner = ?, lease_fence = ?
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ?
		  AND lease_owner = '' AND lease_fence = ?
		  AND state NOT IN ('completed', 'canceled', 'rejected', 'failed')`,
		command.Worker, nextFence, command.Task.Workspace.Authority,
		command.Task.Workspace.ID, command.Task.ID, fence,
	)
	if err != nil {
		return LeaseAssignment{}, fmt.Errorf("acquire Agent lease: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return LeaseAssignment{}, fmt.Errorf("inspect Agent lease acquisition: %w", err)
	}
	if affected != 1 {
		return LeaseAssignment{}, ErrLeaseUnavailable
	}
	if err := recordJSONCommand(
		ctx, tx, command.Task.Workspace.Authority, command.ID,
		"acquire_lease", digest, "lease_assignment", assignment, now,
	); err != nil {
		return LeaseAssignment{}, err
	}
	if err := tx.Commit(); err != nil {
		return LeaseAssignment{}, fmt.Errorf("commit Agent lease acquisition: %w", err)
	}
	return cloneLeaseAssignment(assignment), nil
}

// FenceLease retires exactly one current lease generation. It never rewinds
// the durable fence; a later acquisition therefore cannot authorize an event
// produced under this grant, even when the same worker reconnects.
func (agent *Agent) FenceLease(ctx context.Context, command FenceLeaseCommand) error {
	if err := validateCallContext(ctx); err != nil {
		return err
	}
	if err := validateStable("command id", string(command.ID)); err != nil {
		return err
	}
	if err := validateLeaseGrant(command.Grant); err != nil {
		return err
	}
	digest, err := commandDigest("fence_lease", command)
	if err != nil {
		return err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return err
	}
	defer release()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Agent lease fence: %w", err)
	}
	defer tx.Rollback()

	var replay LeaseGrant
	if found, err := replayJSONCommand(
		ctx, tx, command.Grant.Task.Workspace.Authority, command.ID,
		"fence_lease", digest, "lease_grant", &replay,
	); err != nil {
		return err
	} else if found {
		if replay != command.Grant || validateLeaseGrant(replay) != nil {
			return fmt.Errorf("%w: invalid replayed Agent lease fence", ErrCorruptStore)
		}
		if _, err := verifyLeaseGrantHistory(ctx, tx, replay); err != nil {
			return err
		}
		return nil
	}
	if err := requireCurrentLease(ctx, tx, command.Grant); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE agent_tasks
		SET lease_owner = ''
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ?
		  AND lease_owner = ? AND lease_fence = ?`,
		command.Grant.Task.Workspace.Authority, command.Grant.Task.Workspace.ID,
		command.Grant.Task.ID, command.Grant.Worker, command.Grant.Fence,
	)
	if err != nil {
		return fmt.Errorf("fence Agent lease: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect Agent lease fence: %w", err)
	}
	if affected != 1 {
		return ErrStaleLease
	}
	now := agent.now().UTC()
	if err := recordJSONCommand(
		ctx, tx, command.Grant.Task.Workspace.Authority, command.ID,
		"fence_lease", digest, "lease_grant", command.Grant, now,
	); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Agent lease fence: %w", err)
	}
	return nil
}

// GetLease returns the current committed assignment for a Task. An unleased
// Task and a retired generation both return ErrLeaseNotFound.
func (agent *Agent) GetLease(ctx context.Context, ref TaskRef) (LeaseAssignment, error) {
	if err := validateCallContext(ctx); err != nil {
		return LeaseAssignment{}, err
	}
	if err := validateTaskRef(ref); err != nil {
		return LeaseAssignment{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return LeaseAssignment{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return LeaseAssignment{}, fmt.Errorf("begin Agent lease read: %w", err)
	}
	defer tx.Rollback()
	assignment, err := loadLeaseAssignment(ctx, tx, ref)
	if err != nil {
		return LeaseAssignment{}, err
	}
	if err := tx.Commit(); err != nil {
		return LeaseAssignment{}, fmt.Errorf("commit Agent lease read: %w", err)
	}
	return cloneLeaseAssignment(assignment), nil
}

// ListLeases returns current assignments for one authenticated worker. This is
// the restart recovery scan: dispatchers must republish these exact Task/Fence
// pairs instead of acquiring replacement generations.
func (agent *Agent) ListLeases(
	ctx context.Context,
	authority AuthorityID,
	worker WorkerID,
	request LeasePageRequest,
) (LeasePage, error) {
	if err := validateCallContext(ctx); err != nil {
		return LeasePage{}, err
	}
	if err := validateStable("authority id", string(authority)); err != nil {
		return LeasePage{}, err
	}
	if err := validateStable("worker id", string(worker)); err != nil {
		return LeasePage{}, err
	}
	limit, err := normalizePageLimit(request.Limit)
	if err != nil {
		return LeasePage{}, err
	}
	if request.After != nil {
		if !validLeaseTimestamp(request.After.GrantedAt) ||
			validateStable("workspace id", string(request.After.Workspace)) != nil ||
			validateStable("task id", string(request.After.Task)) != nil ||
			request.After.Fence == 0 || request.After.Fence > LeaseFence(math.MaxInt64) {
			return LeasePage{}, fmt.Errorf("%w: invalid lease page cursor", ErrInvalidArgument)
		}
	}
	database, release, err := agent.acquire()
	if err != nil {
		return LeasePage{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return LeasePage{}, fmt.Errorf("begin Agent lease list: %w", err)
	}
	defer tx.Rollback()

	query := `
		SELECT t.workspace_id, t.task_id, t.lease_fence, g.worker_id, g.granted_at
		FROM agent_tasks AS t
		LEFT JOIN agent_lease_grants AS g
		  ON g.authority_id = t.authority_id
		 AND g.workspace_id = t.workspace_id
		 AND g.task_id = t.task_id
		 AND g.fence = t.lease_fence
		WHERE t.authority_id = ? AND t.lease_owner = ?`
	arguments := []any{authority, worker}
	if request.After != nil {
		cursorTime := unixNano(request.After.GrantedAt.UTC())
		query += ` AND (g.granted_at > ? OR
		  (g.granted_at = ? AND (t.workspace_id > ? OR
		    (t.workspace_id = ? AND (t.task_id > ? OR
		      (t.task_id = ? AND t.lease_fence > ?))))))`
		arguments = append(
			arguments,
			cursorTime, cursorTime,
			request.After.Workspace, request.After.Workspace,
			request.After.Task, request.After.Task, request.After.Fence,
		)
	}
	query += ` ORDER BY g.granted_at, t.workspace_id, t.task_id, t.lease_fence LIMIT ?`
	arguments = append(arguments, limit+1)
	rows, err := tx.QueryContext(ctx, query, arguments...)
	if err != nil {
		return LeasePage{}, fmt.Errorf("list Agent leases: %w", err)
	}
	type leaseRow struct {
		ref       TaskRef
		fence     LeaseFence
		grantedAt time.Time
	}
	listed := make([]leaseRow, 0, limit+1)
	for rows.Next() {
		var row leaseRow
		var historyWorker sql.NullString
		var grantedAt sql.NullInt64
		row.ref.Workspace.Authority = authority
		if err := rows.Scan(
			&row.ref.Workspace.ID, &row.ref.ID, &row.fence,
			&historyWorker, &grantedAt,
		); err != nil {
			_ = rows.Close()
			return LeasePage{}, fmt.Errorf("scan Agent lease: %w", err)
		}
		if !historyWorker.Valid || !grantedAt.Valid || WorkerID(historyWorker.String) != worker {
			_ = rows.Close()
			return LeasePage{}, fmt.Errorf(
				"%w: current Agent lease differs from durable grant", ErrCorruptStore,
			)
		}
		row.grantedAt = fromUnixNano(grantedAt.Int64)
		listed = append(listed, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return LeasePage{}, fmt.Errorf("list Agent leases: %w", err)
	}
	if err := rows.Close(); err != nil {
		return LeasePage{}, fmt.Errorf("close Agent lease list: %w", err)
	}

	page := LeasePage{Items: make([]LeaseAssignment, 0, min(len(listed), limit))}
	if len(listed) > limit {
		page.HasMore = true
		listed = listed[:limit]
	}
	for _, row := range listed {
		owner, fence, grantedAt, err := loadLeaseState(ctx, tx, row.ref)
		if err != nil {
			return LeasePage{}, err
		}
		if owner != worker || fence != row.fence || !grantedAt.Equal(row.grantedAt) {
			return LeasePage{}, fmt.Errorf(
				"%w: listed Agent lease differs from current durable state", ErrCorruptStore,
			)
		}
		task, err := loadTask(ctx, tx, row.ref)
		if err != nil {
			return LeasePage{}, err
		}
		assignment := LeaseAssignment{
			Grant: LeaseGrant{Task: row.ref, Worker: worker, Fence: row.fence},
			Task:  task, GrantedAt: row.grantedAt,
		}
		if err := validateLeaseAssignment(assignment); err != nil {
			return LeasePage{}, err
		}
		page.Items = append(page.Items, cloneLeaseAssignment(assignment))
	}
	if page.HasMore && len(page.Items) > 0 {
		last := page.Items[len(page.Items)-1]
		page.Next = &LeasePageCursor{
			GrantedAt: last.GrantedAt,
			Workspace: last.Grant.Task.Workspace.ID,
			Task:      last.Grant.Task.ID,
			Fence:     last.Grant.Fence,
		}
	}
	if err := tx.Commit(); err != nil {
		return LeasePage{}, fmt.Errorf("commit Agent lease list: %w", err)
	}
	return page, nil
}

func loadLeaseAssignment(ctx context.Context, tx *sql.Tx, ref TaskRef) (LeaseAssignment, error) {
	task, err := loadTask(ctx, tx, ref)
	if err != nil {
		return LeaseAssignment{}, err
	}
	owner, fence, grantedAt, err := loadLeaseState(ctx, tx, ref)
	if err != nil {
		return LeaseAssignment{}, err
	}
	if owner == "" {
		return LeaseAssignment{}, ErrLeaseNotFound
	}
	assignment := LeaseAssignment{
		Grant: LeaseGrant{Task: ref, Worker: owner, Fence: fence},
		Task:  task, GrantedAt: grantedAt,
	}
	if err := validateLeaseAssignment(assignment); err != nil {
		return LeaseAssignment{}, err
	}
	return assignment, nil
}

func loadLeaseState(
	ctx context.Context,
	query queryer,
	ref TaskRef,
) (WorkerID, LeaseFence, time.Time, error) {
	var ownerValue string
	var fenceValue int64
	var createdAtValue int64
	if err := query.QueryRowContext(ctx, `
		SELECT lease_owner, lease_fence, created_at
		FROM agent_tasks
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ?`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
	).Scan(&ownerValue, &fenceValue, &createdAtValue); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, time.Time{}, ErrNotFound
		}
		return "", 0, time.Time{}, fmt.Errorf("load Agent lease state: %w", err)
	}
	if fenceValue < 0 {
		return "", 0, time.Time{}, fmt.Errorf("%w: Agent lease fence is negative", ErrCorruptStore)
	}
	owner := WorkerID(ownerValue)
	fence := LeaseFence(fenceValue)
	if owner != "" {
		if err := validateStable("worker id", ownerValue); err != nil {
			return "", 0, time.Time{}, fmt.Errorf("%w: invalid Agent lease owner", ErrCorruptStore)
		}
	}
	latestFence, err := loadLatestLeaseHistoryFence(ctx, query, ref)
	if err != nil {
		return "", 0, time.Time{}, err
	}
	if latestFence != fence {
		return "", 0, time.Time{}, fmt.Errorf(
			"%w: Agent lease fence differs from durable grant history", ErrCorruptStore,
		)
	}
	if fence == 0 {
		if owner != "" {
			return "", 0, time.Time{}, fmt.Errorf("%w: Agent lease owner has zero fence", ErrCorruptStore)
		}
		return owner, fence, time.Time{}, nil
	}
	historyWorker, grantedAt, err := loadLeaseGrantHistory(ctx, query, ref, fence)
	if err != nil {
		return "", 0, time.Time{}, err
	}
	if owner != "" && historyWorker != owner {
		return "", 0, time.Time{}, fmt.Errorf(
			"%w: current Agent lease owner differs from durable grant", ErrCorruptStore,
		)
	}
	if grantedAt.Before(fromUnixNano(createdAtValue)) {
		return "", 0, time.Time{}, fmt.Errorf(
			"%w: Agent lease predates its Task", ErrCorruptStore,
		)
	}
	return owner, fence, grantedAt, nil
}

func requireCurrentLease(ctx context.Context, query queryer, grant LeaseGrant) error {
	if err := validateLeaseGrant(grant); err != nil {
		return err
	}
	owner, fence, _, err := loadLeaseState(ctx, query, grant.Task)
	if err != nil {
		return err
	}
	if owner != grant.Worker || fence != grant.Fence {
		return ErrStaleLease
	}
	return nil
}

func verifyLeaseHistory(ctx context.Context, tx *sql.Tx, assignment LeaseAssignment) error {
	grantedAt, err := verifyLeaseGrantHistory(ctx, tx, assignment.Grant)
	if err != nil {
		return err
	}
	if !grantedAt.Equal(assignment.GrantedAt) {
		return fmt.Errorf("%w: replayed Agent lease differs from history", ErrCorruptStore)
	}
	return nil
}

func loadLatestLeaseHistoryFence(
	ctx context.Context,
	query queryer,
	ref TaskRef,
) (LeaseFence, error) {
	var fenceValue int64
	if err := query.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(fence), 0)
		FROM agent_lease_grants
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ?`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
	).Scan(&fenceValue); err != nil {
		return 0, fmt.Errorf("load latest Agent lease history: %w", err)
	}
	if fenceValue < 0 {
		return 0, fmt.Errorf("%w: Agent lease history has a negative fence", ErrCorruptStore)
	}
	return LeaseFence(fenceValue), nil
}

func loadLeaseGrantHistory(
	ctx context.Context,
	query queryer,
	ref TaskRef,
	fence LeaseFence,
) (WorkerID, time.Time, error) {
	var workerValue string
	var grantedAtValue int64
	if err := query.QueryRowContext(ctx, `
		SELECT worker_id, granted_at
		FROM agent_lease_grants
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND fence = ?`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID, fence,
	).Scan(&workerValue, &grantedAtValue); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", time.Time{}, fmt.Errorf(
				"%w: Agent lease has no durable grant history", ErrCorruptStore,
			)
		}
		return "", time.Time{}, fmt.Errorf("load Agent lease grant history: %w", err)
	}
	if err := validateStable("worker id", workerValue); err != nil {
		return "", time.Time{}, fmt.Errorf("%w: invalid Agent lease history worker", ErrCorruptStore)
	}
	return WorkerID(workerValue), fromUnixNano(grantedAtValue), nil
}

func verifyLeaseGrantHistory(
	ctx context.Context,
	query queryer,
	grant LeaseGrant,
) (time.Time, error) {
	worker, grantedAt, err := loadLeaseGrantHistory(ctx, query, grant.Task, grant.Fence)
	if err != nil {
		return time.Time{}, err
	}
	if worker != grant.Worker {
		return time.Time{}, fmt.Errorf(
			"%w: Agent lease differs from durable grant history", ErrCorruptStore,
		)
	}
	var createdAtValue int64
	if err := query.QueryRowContext(ctx, `
		SELECT created_at
		FROM agent_tasks
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ?`,
		grant.Task.Workspace.Authority, grant.Task.Workspace.ID, grant.Task.ID,
	).Scan(&createdAtValue); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, fmt.Errorf("%w: Agent lease Task is missing", ErrCorruptStore)
		}
		return time.Time{}, fmt.Errorf("load Agent lease Task timestamp: %w", err)
	}
	if grantedAt.Before(fromUnixNano(createdAtValue)) {
		return time.Time{}, fmt.Errorf("%w: Agent lease predates its Task", ErrCorruptStore)
	}
	return grantedAt, nil
}

func validateLeaseGrant(grant LeaseGrant) error {
	if err := validateTaskRef(grant.Task); err != nil {
		return err
	}
	if err := validateStable("worker id", string(grant.Worker)); err != nil {
		return err
	}
	if grant.Fence == 0 || grant.Fence > LeaseFence(math.MaxInt64) {
		return fmt.Errorf("%w: lease fence must be 1..%d", ErrInvalidArgument, int64(math.MaxInt64))
	}
	return nil
}

func validateWorkerMeta(meta WorkerCommandMeta, task TaskRef) error {
	if err := validateMeta(CommandMeta{ID: meta.ID, ExpectedRevision: meta.ExpectedRevision}, false); err != nil {
		return err
	}
	if err := validateLeaseGrant(meta.Grant); err != nil {
		return err
	}
	if meta.Grant.Task != task {
		return fmt.Errorf("%w: worker grant does not belong to command Task", ErrInvalidArgument)
	}
	return nil
}

func commandMeta(meta WorkerCommandMeta) CommandMeta {
	return CommandMeta{ID: meta.ID, ExpectedRevision: meta.ExpectedRevision}
}

func validateLeaseAssignment(assignment LeaseAssignment) error {
	if err := validateLeaseGrant(assignment.Grant); err != nil {
		return fmt.Errorf("%w: invalid Agent lease grant", ErrCorruptStore)
	}
	if err := validateStoredTask(assignment.Task); err != nil ||
		assignment.Task.Ref != assignment.Grant.Task || assignment.Task.State.Terminal() ||
		!validLeaseTimestamp(assignment.GrantedAt) ||
		assignment.GrantedAt.Before(assignment.Task.CreatedAt) {
		return fmt.Errorf("%w: invalid Agent lease assignment", ErrCorruptStore)
	}
	return nil
}

func validLeaseTimestamp(value time.Time) bool {
	if value.IsZero() {
		return false
	}
	value = value.UTC()
	return fromUnixNano(unixNano(value)).Equal(value)
}

func cloneLeaseAssignment(assignment LeaseAssignment) LeaseAssignment {
	assignment.Task = cloneTask(assignment.Task)
	return assignment
}
