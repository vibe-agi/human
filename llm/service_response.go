package llm

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ReadResponse returns the current exact durable response projection without
// waiting for another event.
func (service *Service) ReadResponse(ctx context.Context, query ResponseQuery) (ResponsePage, error) {
	return service.readResponse(ctx, query, true)
}

// WaitResponse waits until the response advances beyond query.After, commits a
// decision, or completes. Notifications are hints only; every return value is
// reconstructed from Store, so process restart and lost wakeups are harmless.
func (service *Service) WaitResponse(ctx context.Context, query ResponseQuery) (ResponsePage, error) {
	end, err := service.beginOperation()
	if err != nil {
		return ResponsePage{}, err
	}
	defer end()
	if ctx == nil {
		return ResponsePage{}, fmt.Errorf("%w: context is required", ErrInvalidServiceConfig)
	}
	for {
		signal, releaseSignal := service.responseSignal(StoreRequestKey{
			Caller: query.CallerID, IdempotencyKey: query.IdempotencyKey,
		})
		page, readErr := service.readResponse(ctx, query, false)
		if readErr != nil {
			releaseSignal()
			return ResponsePage{}, readErr
		}
		if page.Cursor > query.After || page.Complete || (query.After == 0 && page.DecisionCommitted) {
			releaseSignal()
			return page, nil
		}
		var waitErr error
		select {
		case <-signal:
		case <-ctx.Done():
			waitErr = ctx.Err()
		case <-service.stopping:
			waitErr = ErrServiceClosed
		}
		releaseSignal()
		if waitErr != nil {
			return ResponsePage{}, waitErr
		}
	}
}

func (service *Service) readResponse(
	ctx context.Context,
	query ResponseQuery,
	ownedOperation bool,
) (ResponsePage, error) {
	if ownedOperation {
		end, err := service.beginOperation()
		if err != nil {
			return ResponsePage{}, err
		}
		defer end()
	}
	if ctx == nil {
		return ResponsePage{}, fmt.Errorf("%w: context is required", ErrInvalidServiceConfig)
	}
	if err := ctx.Err(); err != nil {
		return ResponsePage{}, err
	}
	if err := validateWorkerStableKey("caller id", string(query.CallerID)); err != nil {
		return ResponsePage{}, err
	}
	if err := validateWorkerStableKey("idempotency key", string(query.IdempotencyKey)); err != nil {
		return ResponsePage{}, err
	}
	if query.RequestDigest == "" {
		return ResponsePage{}, ErrResponseDigest
	}
	limit := query.Limit
	if limit == 0 {
		limit = defaultResponseLimit
	}
	if limit < 1 || limit > maximumResponseLimit {
		return ResponsePage{}, fmt.Errorf("%w: response limit must be 1..%d", ErrInvalidServiceConfig, maximumResponseLimit)
	}
	maxBytes := query.MaxBytes
	if maxBytes == 0 {
		maxBytes = service.readLimitBytes
	}
	if maxBytes < 1 || maxBytes > service.readLimitBytes {
		return ResponsePage{}, fmt.Errorf("%w: response byte limit is invalid", ErrInvalidServiceConfig)
	}
	key := StoreRequestKey{Caller: query.CallerID, IdempotencyKey: query.IdempotencyKey}
	var request StoreRequestHead
	var decision StoreResponseDecision
	var task StoreTaskRecord
	var events []StoreResponseEventRecord
	err := service.store.View(ctx, func(view StoreView) error {
		loaded, loadErr := view.LoadRequestHead(key)
		if loadErr != nil {
			return loadErr
		}
		request = loaded
		decision, loadErr = view.LoadResponseDecision(key, StoreReadLimit{MaxBytes: maxBytes})
		if loadErr != nil {
			return loadErr
		}
		task, loadErr = view.LoadTask(request.Task)
		if loadErr != nil {
			return loadErr
		}
		if query.After > request.LastEventSequence {
			return fmt.Errorf("%w: response cursor is ahead of durable state", ErrInvalidServiceConfig)
		}
		batch, scanErr := view.ScanResponseEvents(StoreResponseEventScan{
			Request: key, After: query.After, Limit: limit,
			ReadLimit: StoreReadLimit{MaxBytes: maxBytes},
		})
		events = batch
		return scanErr
	})
	if errors.Is(err, ErrStoreRecordNotFound) {
		return ResponsePage{}, ErrResponseNotFound
	}
	if err != nil {
		return ResponsePage{}, err
	}
	if request.RequestDigest != query.RequestDigest {
		return ResponsePage{}, ErrResponseDigest
	}
	if request.PayloadPrunedAt != nil {
		return ResponsePage{}, ErrReplayExpired
	}
	page := ResponsePage{
		Identity: CompletionIdentity{
			CallerID: request.Key.Caller, RequestID: request.RequestID,
			TaskID: request.Task.Task, IdempotencyKey: request.Key.IdempotencyKey,
		},
		RequestDigest: request.RequestDigest, Mode: request.Mode,
		DecisionCommitted: request.DecisionStatus != 0,
		Decision: ResponseDecision{
			StatusCode: decision.StatusCode, ContentType: decision.ContentType,
			RetryAfter: decision.RetryAfter,
		},
		Cursor: query.After,
	}
	page.Identity.WorkspaceKey = task.WorkspaceKey
	if decision.Body != nil {
		body, openErr := service.openPayload(ctx, responseBodyBinding(key, request.RequestID), decision.Body)
		if openErr != nil {
			return ResponsePage{}, openErr
		}
		page.Decision.Body = body
	}
	for _, event := range events {
		page.Cursor = event.Sequence
		if event.Kind != StoreEventWire {
			continue
		}
		data, openErr := service.openPayload(ctx, responseEventBinding(key, request.RequestID, event.Sequence), event.Data)
		if openErr != nil {
			return ResponsePage{}, openErr
		}
		page.Events = append(page.Events, WireEvent{Sequence: event.Sequence, Data: data})
	}
	page.Complete = request.ResponseComplete &&
		(request.Mode == ResponseAggregate || page.Cursor == request.LastEventSequence)
	return page, nil
}

type responseSignalState struct {
	ready   chan struct{}
	waiters uint64
}

func (service *Service) responseSignal(key StoreRequestKey) (<-chan struct{}, func()) {
	service.mu.Lock()
	state := service.signals[key]
	if state == nil {
		state = &responseSignalState{ready: make(chan struct{})}
		service.signals[key] = state
	}
	state.waiters++
	service.mu.Unlock()
	var once sync.Once
	return state.ready, func() {
		once.Do(func() {
			service.mu.Lock()
			defer service.mu.Unlock()
			if current := service.signals[key]; current == state {
				state.waiters--
				if state.waiters == 0 {
					delete(service.signals, key)
				}
			}
		})
	}
}

func (service *Service) signalResponse(key StoreRequestKey) {
	service.mu.Lock()
	current := service.signals[key]
	if current != nil {
		delete(service.signals, key)
		close(current.ready)
	}
	service.mu.Unlock()
}
