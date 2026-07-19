package agent

import (
	"context"
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
	store, release, err := agent.acquireStore()
	if err != nil {
		return LeaseAssignment{}, err
	}
	defer release()
	var assignment LeaseAssignment
	err = store.Update(ctx, func(tx StoreTx) error {
		var replay LeaseAssignment
		if found, err := replayJSONCommandFromStore(
			tx, command.Authority, command.ID,
			"claim_lease", digest, "lease_assignment", &replay,
		); err != nil {
			return err
		} else if found {
			if err := validateLeaseAssignment(replay); err != nil ||
				replay.Grant.Task.Workspace.Authority != command.Authority ||
				replay.Grant.Worker != command.Worker {
				return fmt.Errorf("%w: invalid replayed Agent lease claim", ErrCorruptStore)
			}
			if err := verifyLeaseAssignmentFromStore(tx, replay); err != nil {
				return err
			}
			assignment = cloneLeaseAssignment(replay)
			return nil
		}

		record, err := tx.FindClaimableTask(command.Authority)
		if errors.Is(err, ErrStoreRecordNotFound) {
			return ErrLeaseNotFound
		}
		if err != nil {
			return fmt.Errorf("select claimable Agent Task: %w", err)
		}
		if err := validateStoreTaskRecord(record); err != nil ||
			record.Task.Ref.Workspace.Authority != command.Authority ||
			record.Task.State.Terminal() || record.Lease.Owner != "" {
			return fmt.Errorf("%w: selected Agent Task is not claimable", ErrCorruptStore)
		}
		previousGrantedAt, err := validateLeaseHistoryHeadFromStore(tx, record)
		if err != nil {
			return err
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
			Grant: LeaseGrant{Task: record.Task.Ref, Worker: command.Worker, Fence: nextFence},
			Task:  cloneTask(record.Task), GrantedAt: now,
		}
		if err := validateLeaseAssignment(assignment); err != nil {
			return err
		}
		if err := tx.InsertLeaseGrant(StoreLeaseGrantRecord{
			Grant: assignment.Grant, GrantedAt: now,
		}); err != nil {
			if errors.Is(err, ErrStoreConflict) {
				return fmt.Errorf("%w: duplicate Agent lease generation", ErrCorruptStore)
			}
			return fmt.Errorf("record claimed Agent lease grant: %w", err)
		}
		expectedLease := record.Lease
		next := record
		next.Task = cloneTask(record.Task)
		next.Lease = StoreLeaseState{Owner: command.Worker, Fence: nextFence}
		changed, err := tx.CompareAndSwapTask(StoreTaskMutation{
			Ref: record.Task.Ref,
			Condition: StoreTaskCondition{
				ExpectedRevision: record.Task.Revision,
				ExpectedLease:    &expectedLease,
			},
			Next: next,
		})
		if err != nil {
			return fmt.Errorf("claim Agent lease: %w", err)
		}
		if !changed {
			return ErrLeaseUnavailable
		}
		return recordJSONCommandToStore(
			tx, command.Authority, command.ID,
			"claim_lease", digest, "lease_assignment", assignment, now,
		)
	})
	if err != nil {
		return LeaseAssignment{}, err
	}
	return cloneLeaseAssignment(assignment), nil
}
