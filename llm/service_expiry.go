package llm

import (
	"context"
	"fmt"
	"time"

	"github.com/vibe-agi/human/observe"
)

// ExpiryPolicy is an explicit host decision for incomplete model responses.
// Service owns the durable state transition and deterministic response bytes;
// the embedding host owns the wall-clock schedule and deadline. Keeping the
// sweep out of an internal goroutine makes the same mechanism usable by local,
// clustered, and test hosts without hidden lifecycle work.
type ExpiryPolicy struct {
	PendingBefore time.Time
	BatchSize     int
}

// ExpiryResult reports one sweep. Deferred records raced another durable
// transition or were not in a state where an expiry event could be committed;
// a later sweep may inspect them again.
type ExpiryResult struct {
	Scanned  uint64
	Expired  uint64
	Deferred uint64
}

// RunExpiry durably completes eligible requests with EventExpired. It is safe
// to call repeatedly and concurrently: each expiry has a deterministic event
// identity and uses the normal worker-event commit path, so response encoding,
// receipts, revision CAS, commit-unknown reconciliation, waiter wakeups, and
// assignment cleanup remain one correctness implementation.
func (service *Service) RunExpiry(ctx context.Context, policy ExpiryPolicy) (ExpiryResult, error) {
	end, err := service.beginOperation()
	if err != nil {
		return ExpiryResult{}, err
	}
	defer end()
	if ctx == nil {
		return ExpiryResult{}, fmt.Errorf("%w: context is required", ErrInvalidServiceConfig)
	}
	if err := ctx.Err(); err != nil {
		return ExpiryResult{}, err
	}
	if policy.PendingBefore.IsZero() {
		return ExpiryResult{}, fmt.Errorf("%w: expiry pending boundary is required", ErrInvalidServiceConfig)
	}
	batchSize := policy.BatchSize
	if batchSize == 0 {
		batchSize = defaultResponseLimit
	}
	if batchSize < 1 || batchSize > maximumResponseLimit {
		return ExpiryResult{}, fmt.Errorf(
			"%w: expiry batch size must be 1..%d", ErrInvalidServiceConfig, maximumResponseLimit,
		)
	}

	boundary := policy.PendingBefore.UTC()
	var result ExpiryResult
	var cursor *StoreRecoveryCursor
	finished := false
	for !finished {
		var records []StoreRecoveryRecord
		err := service.store.View(ctx, func(view StoreView) error {
			loaded, scanErr := view.ScanRecovery(StoreRecoveryScan{
				After: cursor, Limit: batchSize,
				ReadLimit: StoreReadLimit{MaxBytes: maximumRecoveryReadLimitBytes},
			})
			records = loaded
			return scanErr
		})
		if err != nil {
			return result, fmt.Errorf("scan HumanLLM expiry: %w", err)
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			if record.Request.CreatedAt.After(boundary) {
				// Recovery order begins with CreatedAt, so every later record is
				// newer than the host's boundary as well.
				finished = true
				break
			}
			if record.Request.ResponseComplete || record.Request.RecoveryQuarantined {
				continue
			}
			result.Scanned++
			expired, expireErr := service.expireRecoveryRecord(ctx, record)
			if expireErr != nil {
				return result, expireErr
			}
			if expired {
				result.Expired++
			} else {
				result.Deferred++
			}
		}
		last := records[len(records)-1].Request
		cursor = &StoreRecoveryCursor{
			CreatedAt: last.CreatedAt, Caller: last.Key.Caller,
			IdempotencyKey: last.Key.IdempotencyKey,
		}
	}
	observe.Emit(service.observer, observe.Event{
		Kind:   observe.KindExpiryCompleted,
		Detail: fmt.Sprintf("expired=%d deferred=%d", result.Expired, result.Deferred),
	})
	return result, nil
}

func (service *Service) expireRecoveryRecord(ctx context.Context, record StoreRecoveryRecord) (bool, error) {
	if record.Task.LeaseOwner == "" || record.Task.LeaseID == "" {
		return false, nil
	}
	identity := CompletionIdentity{
		CallerID: record.Request.Key.Caller, RequestID: record.Request.RequestID,
		TaskID: record.Request.Task.Task, WorkspaceKey: record.Task.WorkspaceKey,
		IdempotencyKey: record.Request.Key.IdempotencyKey,
	}
	digest, err := stableDigest(struct {
		Request StoreRequestKey `json:"request"`
		ID      string          `json:"request_id"`
	}{record.Request.Key, record.Request.RequestID})
	if err != nil {
		return false, fmt.Errorf("identify HumanLLM expiry: %w", err)
	}
	delivery := WorkerEventDelivery{
		ID:       WorkerDeliveryID("expiry_" + string(digest)),
		Identity: identity,
		LeaseID:  record.Task.LeaseID,
		Event: Event{
			ID: "expiry-" + string(digest), Type: EventExpired,
			ErrorCode: "human_timeout", Error: "timed out waiting for a human response",
		},
	}
	// The system connection does not impersonate a network session: it carries
	// only the worker identity already authorized by the durable lease and then
	// enters the exact normal event-commit path.
	connection := &serviceWorkerConnection{
		service: service,
		principal: AuthenticatedWorker{
			WorkerID: record.Task.LeaseOwner, SessionID: WorkerSessionID("service-expiry"),
		},
	}
	receipt, err := service.commitWorkerEvent(ctx, connection, delivery)
	if err != nil {
		return false, fmt.Errorf("expire HumanLLM request %s/%s: %w",
			record.Request.Key.Caller, record.Request.Key.IdempotencyKey, err)
	}
	if receipt.Decision != WorkerEventACK {
		return false, nil
	}
	service.clearDetached(record.Task.Key)
	service.raiseRequestExpiredNotice(record.Task.LeaseOwner, identity)
	return true, nil
}

func (service *Service) raiseRequestExpiredNotice(owner WorkerID, identity CompletionIdentity) {
	service.mu.Lock()
	connection := service.connections[owner]
	service.mu.Unlock()
	if connection == nil {
		return
	}
	connection.emitNotice(WorkerNotice{
		Code: "request_expired", Message: "This request expired before a human response was delivered.",
		Caller: identity.CallerID, TaskID: identity.TaskID, RequestID: identity.RequestID,
	})
}
