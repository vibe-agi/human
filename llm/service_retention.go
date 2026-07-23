package llm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vibe-agi/human/observe"
)

// RetentionPolicy is an explicit host decision. Service owns the correctness
// of tombstoning, while the embedding host owns when to run it and how long
// replay payloads remain available. This keeps wall-clock scheduling out of the
// transport-neutral core and gives custom hosts the same operation used by the
// reference local runtime.
type RetentionPolicy struct {
	CompletedBefore time.Time
	BatchSize       int
	// RetainToolResults preserves completed task-ledger result payloads. The
	// default false prunes them once their task is terminal; compact identities,
	// digests and settlement receipts remain.
	RetainToolResults bool
}

// RetentionResult reports durable work performed by one sweep.
type RetentionResult struct {
	Scanned           uint64
	RequestsPruned    uint64
	EventsDeleted     uint64
	ToolResultsPruned uint64
	Deferred          uint64
}

// RunRetention replaces eligible completed request payloads with immutable
// replay-expired tombstones. It is safe to call repeatedly and concurrently;
// revision CAS makes a racing request/event transition win without data loss,
// and a later sweep retries the candidate.
func (service *Service) RunRetention(ctx context.Context, policy RetentionPolicy) (RetentionResult, error) {
	end, err := service.beginOperation()
	if err != nil {
		return RetentionResult{}, err
	}
	defer end()
	if ctx == nil {
		return RetentionResult{}, fmt.Errorf("%w: context is required", ErrInvalidServiceConfig)
	}
	if err := ctx.Err(); err != nil {
		return RetentionResult{}, err
	}
	if policy.CompletedBefore.IsZero() {
		return RetentionResult{}, fmt.Errorf("%w: retention completion boundary is required", ErrInvalidServiceConfig)
	}
	batchSize := policy.BatchSize
	if batchSize == 0 {
		batchSize = defaultResponseLimit
	}
	if batchSize < 1 || batchSize > maximumResponseLimit {
		return RetentionResult{}, fmt.Errorf(
			"%w: retention batch size must be 1..%d", ErrInvalidServiceConfig, maximumResponseLimit,
		)
	}
	prunedAt, err := checkedTime(service.clock)
	if err != nil {
		return RetentionResult{}, err
	}
	boundary := policy.CompletedBefore.UTC()
	var result RetentionResult
	var cursor *StoreRetentionCursor
	for {
		var candidates []StoreRetentionCandidate
		err := service.store.View(ctx, func(view StoreView) error {
			loaded, scanErr := view.ScanRetention(StoreRetentionScan{
				CompletedBefore: boundary, After: cursor, Limit: batchSize,
			})
			candidates = loaded
			return scanErr
		})
		if err != nil {
			return result, fmt.Errorf("scan HumanLLM retention: %w", err)
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			result.Scanned++
			if candidate.UnacknowledgedWorkerEvent {
				result.Deferred++
				continue
			}
			one, pruneErr := service.pruneRetentionCandidate(
				ctx, candidate, boundary, prunedAt, policy.RetainToolResults,
			)
			result.RequestsPruned += one.RequestsPruned
			result.EventsDeleted += one.EventsDeleted
			result.ToolResultsPruned += one.ToolResultsPruned
			result.Deferred += one.Deferred
			if pruneErr != nil {
				return result, pruneErr
			}
		}
		last := candidates[len(candidates)-1]
		cursor = &StoreRetentionCursor{
			CompletedAt: last.EffectiveCompletedAt,
			Caller:      last.Request.Key.Caller, IdempotencyKey: last.Request.Key.IdempotencyKey,
		}
	}
	observe.Emit(service.observer, observe.Event{
		Kind: observe.KindRetentionCompleted,
		Detail: fmt.Sprintf("requests=%d events=%d tool_results=%d deferred=%d",
			result.RequestsPruned, result.EventsDeleted, result.ToolResultsPruned, result.Deferred),
	})
	return result, nil
}

func (service *Service) pruneRetentionCandidate(
	ctx context.Context,
	candidate StoreRetentionCandidate,
	boundary time.Time,
	prunedAt time.Time,
	retainToolResults bool,
) (RetentionResult, error) {
	var result RetentionResult
	err := service.store.Update(ctx, func(tx StoreTx) error {
		current, err := tx.LoadRequest(candidate.Request.Key, StoreReadLimit{MaxBytes: maximumRecoveryReadLimitBytes})
		if err != nil {
			return err
		}
		effective := current.CreatedAt
		if current.CompletedAt != nil {
			effective = *current.CompletedAt
		}
		if !current.ResponseComplete || current.PayloadPrunedAt != nil || effective.After(boundary) {
			result.Deferred++
			return nil
		}
		// The scan reported this completed candidate with no unacknowledged
		// worker event. Once a response is complete the core never appends a new
		// worker-associated response event without its atomic receipt, so that
		// safety predicate cannot regress before this serializable update.
		next := current
		next.CanonicalPayload = nil
		next.Decision.Body = nil
		next.PayloadPrunedAt = &prunedAt
		next.Revision++
		changed, err := tx.CompareAndSwapRequest(StoreRequestMutation{
			Key: current.Key, ExpectedRevision: current.Revision, Next: next,
		})
		if err != nil {
			return err
		}
		if !changed {
			result.Deferred++
			return nil
		}
		deleted, err := tx.DeleteTombstonedResponseEvents(current.Key)
		if err != nil {
			return err
		}
		result.RequestsPruned = 1
		result.EventsDeleted = deleted
		if retainToolResults {
			return nil
		}
		task, err := tx.LoadTask(current.Task)
		if err != nil {
			return err
		}
		if !task.State.Terminal() {
			return nil
		}
		var after ToolCallID
		for {
			tools, scanErr := tx.ScanToolExecutions(StoreToolExecutionScan{
				Task: current.Task, State: ToolExecutionCompleted, After: after,
				Limit: maximumResponseLimit, ReadLimit: StoreReadLimit{MaxBytes: maximumRecoveryReadLimitBytes},
			})
			if scanErr != nil {
				return scanErr
			}
			if len(tools) == 0 {
				break
			}
			for _, tool := range tools {
				if tool.ResultPrunedAt != nil {
					continue
				}
				nextTool := tool
				nextTool.Result = nil
				nextTool.ResultPrunedAt = &prunedAt
				nextTool.Revision++
				changed, changeErr := tx.CompareAndSwapToolExecution(StoreToolExecutionMutation{
					Key: tool.Key, ExpectedRevision: tool.Revision, Next: nextTool,
				})
				if changeErr != nil {
					return changeErr
				}
				if changed {
					result.ToolResultsPruned++
				}
			}
			after = tools[len(tools)-1].Key.ToolCallID
		}
		return nil
	})
	if err == nil {
		return result, nil
	}
	if !errors.Is(err, ErrStoreCommitUnknown) {
		// Update callbacks are atomic. Any counters accumulated inside a callback
		// whose transaction definitely failed describe rolled-back work and must
		// not escape as a false success.
		return RetentionResult{}, fmt.Errorf("prune HumanLLM request %s/%s: %w",
			candidate.Request.Key.Caller, candidate.Request.Key.IdempotencyKey, err)
	}
	// The callback is atomic. A tombstoned head proves the event deletion and
	// any tool-result mutations committed with it; exact counts are unknowable,
	// so report request success without inventing secondary counts.
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var head StoreRequestHead
	reconcileErr := service.store.View(reconcileCtx, func(view StoreView) error {
		loaded, loadErr := view.LoadRequestHead(candidate.Request.Key)
		head = loaded
		return loadErr
	})
	if reconcileErr == nil && head.PayloadPrunedAt != nil {
		return RetentionResult{RequestsPruned: 1}, nil
	}
	return RetentionResult{}, fmt.Errorf("reconcile HumanLLM retention: %w", errors.Join(err, reconcileErr))
}
