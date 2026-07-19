package agent

import (
	"context"
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
		now, err := checkedClockTime(agent.now)
		if err != nil {
			return err
		}
		now = timestampAtLeast(now, record.Task.UpdatedAt, previousGrantedAt)
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
		now, err := checkedClockTime(agent.now)
		if err != nil {
			return err
		}
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
