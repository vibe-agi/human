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
	store, release, err := agent.acquireStore()
	if err != nil {
		return LeaseAssignment{}, err
	}
	defer release()
	var assignment LeaseAssignment
	err = store.Update(ctx, func(tx StoreTx) error {
		var replay LeaseAssignment
		if found, err := replayJSONCommandFromStore(
			tx, command.Task.Workspace.Authority, command.ID,
			"acquire_lease", digest, "lease_assignment", &replay,
		); err != nil {
			return err
		} else if found {
			if err := validateLeaseAssignment(replay); err != nil ||
				replay.Grant.Task != command.Task || replay.Grant.Worker != command.Worker {
				return fmt.Errorf("%w: invalid replayed Agent lease assignment", ErrCorruptStore)
			}
			if err := verifyLeaseAssignmentFromStore(tx, replay); err != nil {
				return err
			}
			assignment = cloneLeaseAssignment(replay)
			return nil
		}

		record, err := loadTaskRecordFromStore(tx, command.Task)
		if err != nil {
			return err
		}
		previousGrantedAt, err := validateLeaseHistoryHeadFromStore(tx, record)
		if err != nil {
			return err
		}
		if record.Task.State.Terminal() {
			return &TransitionError{Operation: "acquire_lease", State: record.Task.State, Terminal: true}
		}
		if record.Lease.Owner != "" {
			return ErrLeaseUnavailable
		}
		if record.Lease.Fence >= LeaseFence(math.MaxInt64) {
			return ErrLeaseFenceExhausted
		}
		nextFence := record.Lease.Fence + 1
		now := timestampAtLeast(agent.now(), record.Task.UpdatedAt, previousGrantedAt)
		assignment = LeaseAssignment{
			Grant: LeaseGrant{Task: command.Task, Worker: command.Worker, Fence: nextFence},
			Task:  cloneTask(record.Task), GrantedAt: now,
		}
		if err := tx.InsertLeaseGrant(StoreLeaseGrantRecord{
			Grant: assignment.Grant, GrantedAt: now,
		}); err != nil {
			if errors.Is(err, ErrStoreConflict) {
				return fmt.Errorf("%w: duplicate Agent lease generation", ErrCorruptStore)
			}
			return fmt.Errorf("record Agent lease grant: %w", err)
		}
		expectedLease := record.Lease
		next := record
		next.Task = cloneTask(record.Task)
		next.Lease = StoreLeaseState{Owner: command.Worker, Fence: nextFence}
		changed, err := tx.CompareAndSwapTask(StoreTaskMutation{
			Ref: command.Task,
			Condition: StoreTaskCondition{
				ExpectedRevision: record.Task.Revision,
				ExpectedLease:    &expectedLease,
			},
			Next: next,
		})
		if err != nil {
			return fmt.Errorf("acquire Agent lease: %w", err)
		}
		if !changed {
			return ErrLeaseUnavailable
		}
		return recordJSONCommandToStore(
			tx, command.Task.Workspace.Authority, command.ID,
			"acquire_lease", digest, "lease_assignment", assignment, now,
		)
	})
	if err != nil {
		return LeaseAssignment{}, err
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
	store, release, err := agent.acquireStore()
	if err != nil {
		return err
	}
	defer release()
	err = store.Update(ctx, func(tx StoreTx) error {
		var replay LeaseGrant
		if found, err := replayJSONCommandFromStore(
			tx, command.Grant.Task.Workspace.Authority, command.ID,
			"fence_lease", digest, "lease_grant", &replay,
		); err != nil {
			return err
		} else if found {
			if replay != command.Grant || validateLeaseGrant(replay) != nil {
				return fmt.Errorf("%w: invalid replayed Agent lease fence", ErrCorruptStore)
			}
			_, err := verifyLeaseGrantHistoryFromStore(tx, replay)
			if errors.Is(err, ErrStaleLease) {
				return fmt.Errorf("%w: replayed Agent lease fence has no durable grant history", ErrCorruptStore)
			}
			return err
		}
		record, err := loadTaskRecordFromStore(tx, command.Grant.Task)
		if err != nil {
			return err
		}
		if err := requireCurrentLeaseFromStore(tx, command.Grant); err != nil {
			return err
		}
		expectedLease := record.Lease
		next := record
		next.Task = cloneTask(record.Task)
		next.Lease = StoreLeaseState{Fence: command.Grant.Fence}
		changed, err := tx.CompareAndSwapTask(StoreTaskMutation{
			Ref: command.Grant.Task,
			Condition: StoreTaskCondition{
				ExpectedRevision: record.Task.Revision,
				ExpectedLease:    &expectedLease,
			},
			Next: next,
		})
		if err != nil {
			return fmt.Errorf("fence Agent lease: %w", err)
		}
		if !changed {
			return ErrStaleLease
		}
		now := agent.now().UTC()
		return recordJSONCommandToStore(
			tx, command.Grant.Task.Workspace.Authority, command.ID,
			"fence_lease", digest, "lease_grant", command.Grant, now,
		)
	})
	return err
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
	store, release, err := agent.acquireStore()
	if err != nil {
		return LeaseAssignment{}, err
	}
	defer release()
	var assignment LeaseAssignment
	err = store.View(ctx, func(view StoreView) error {
		var err error
		assignment, err = loadLeaseAssignmentFromStore(view, ref)
		return err
	})
	if err != nil {
		return LeaseAssignment{}, err
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
	store, release, err := agent.acquireStore()
	if err != nil {
		return LeasePage{}, err
	}
	defer release()
	page := LeasePage{}
	err = store.View(ctx, func(view StoreView) error {
		listed, err := view.ScanLeases(StoreLeaseScan{
			Authority: authority,
			Worker:    worker,
			After:     request.After,
			Limit:     limit + 1,
		})
		if err != nil {
			return fmt.Errorf("list Agent leases: %w", err)
		}
		page.Items = make([]LeaseAssignment, 0, min(len(listed), limit))
		if len(listed) > limit {
			page.HasMore = true
			listed = listed[:limit]
		}
		for _, assignment := range listed {
			if err := validateLeaseAssignment(assignment); err != nil ||
				assignment.Grant.Task.Workspace.Authority != authority ||
				assignment.Grant.Worker != worker {
				return fmt.Errorf("%w: invalid listed Agent lease", ErrCorruptStore)
			}
			if err := verifyLeaseAssignmentFromStore(view, assignment); err != nil {
				return err
			}
			record, err := loadTaskRecordFromStore(view, assignment.Grant.Task)
			if err != nil {
				return err
			}
			if record.Lease.Owner != worker || record.Lease.Fence != assignment.Grant.Fence ||
				record.Task.Revision != assignment.Task.Revision {
				return fmt.Errorf("%w: listed Agent lease differs from current durable state", ErrCorruptStore)
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
		return nil
	})
	if err != nil {
		return LeasePage{}, err
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
