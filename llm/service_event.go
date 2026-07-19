package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type durableEventState struct {
	task           StoreTaskRecord
	request        StoreRequestRecord
	events         []StoreResponseEventRecord
	toolCallExists bool
}

func (service *Service) ackAssignment(
	ctx context.Context,
	connection *serviceWorkerConnection,
	deliveryID WorkerDeliveryID,
) error {
	end, err := service.beginOperation()
	if err != nil {
		return err
	}
	defer end()
	if ctx == nil {
		return ErrWorkerDelivery
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	connection.mu.Lock()
	_, sent := connection.sent[deliveryID]
	_, alreadyAcked := connection.acked[deliveryID]
	connection.mu.Unlock()
	if alreadyAcked {
		return nil
	}
	if !sent {
		return ErrWorkerDeliveryNotFound
	}
	service.mu.Lock()
	assignment := service.assignments[deliveryID]
	service.mu.Unlock()
	if assignment == nil || assignment.delivery.Assignment.Lease.Owner != connection.principal.WorkerID {
		return ErrWorkerDeliveryNotFound
	}
	lease := assignment.delivery.Assignment.Lease
	taskKey := assignment.delivery.Assignment.Identity
	storeKey := StoreTaskKey{Caller: taskKey.CallerID, Task: taskKey.TaskID}
	now, err := checkedTime(service.clock)
	if err != nil {
		return err
	}
	commitErr := service.store.Update(ctx, func(tx StoreTx) error {
		task, loadErr := tx.LoadTask(storeKey)
		if loadErr != nil {
			return loadErr
		}
		if task.LeaseOwner != lease.Owner || task.LeaseID != lease.ID {
			return ErrWorkerDeliveryConflict
		}
		if task.State == TaskAwaitingHuman {
			return nil
		}
		if task.State != TaskLeased {
			return ErrWorkerDeliveryConflict
		}
		next := task
		next.State = TaskAwaitingHuman
		next.Revision++
		next.UpdatedAt = timestampAtLeast(now, task.UpdatedAt)
		changed, changeErr := tx.CompareAndSwapTask(StoreTaskMutation{
			Key: storeKey, ExpectedRevision: task.Revision, Next: next,
		})
		if changeErr != nil {
			return changeErr
		}
		if !changed {
			return ErrWorkerDeliveryIndeterminate
		}
		return nil
	})
	if commitErr != nil && errors.Is(commitErr, ErrStoreCommitUnknown) {
		reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		commitErr = service.store.View(reconcileCtx, func(view StoreView) error {
			task, loadErr := view.LoadTask(storeKey)
			if loadErr != nil {
				return loadErr
			}
			if task.LeaseOwner == lease.Owner && task.LeaseID == lease.ID && task.State == TaskAwaitingHuman {
				return nil
			}
			return ErrWorkerDeliveryIndeterminate
		})
	}
	if commitErr != nil {
		return &WorkerDeliveryError{Delivery: deliveryID, Cause: commitErr}
	}
	connection.mu.Lock()
	delete(connection.sent, deliveryID)
	connection.acked[deliveryID] = struct{}{}
	connection.mu.Unlock()
	service.mu.Lock()
	assignment.pending = false
	delete(service.pending[connection.principal.WorkerID], deliveryID)
	service.mu.Unlock()
	connection.signal()
	return nil
}

func (service *Service) commitWorkerEvent(
	ctx context.Context,
	connection *serviceWorkerConnection,
	delivery WorkerEventDelivery,
) (WorkerEventReceipt, error) {
	end, err := service.beginOperation()
	if err != nil {
		return WorkerEventReceipt{}, err
	}
	defer end()
	if ctx == nil {
		return WorkerEventReceipt{}, ErrWorkerDelivery
	}
	if err := ctx.Err(); err != nil {
		return WorkerEventReceipt{}, err
	}
	normalizedEvent, normalizationErr := normalizeCanonicalJSON(delivery.Event)
	if normalizationErr != nil {
		if validateWorkerStableKey("delivery id", string(delivery.ID)) == nil &&
			validateWorkerEventID(delivery.Event.ID) == nil {
			return nackReceipt(delivery, WorkerRejectInvalid, "worker event is not canonical JSON"), nil
		}
		return WorkerEventReceipt{}, normalizationErr
	}
	delivery.Event = normalizedEvent
	if err := delivery.ValidateFor(connection.principal); err != nil {
		if validateWorkerStableKey("delivery id", string(delivery.ID)) == nil &&
			validateWorkerEventID(delivery.Event.ID) == nil {
			return nackReceipt(delivery, WorkerRejectInvalid, "worker event is invalid"), nil
		}
		return WorkerEventReceipt{}, err
	}
	digest, err := workerEventDigest(delivery)
	if err != nil {
		return nackReceipt(delivery, WorkerRejectInvalid, "worker event cannot be identified"), nil
	}
	storedReceipt, receiptFound, err := service.loadWorkerReceipt(ctx, delivery)
	if err != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{
			Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: err,
		}
	}
	if receiptFound {
		if storedReceipt.Worker == connection.principal.WorkerID && storedReceipt.Digest == digest {
			key := StoreRequestKey{
				Caller: delivery.Identity.CallerID, IdempotencyKey: delivery.Identity.IdempotencyKey,
			}
			service.signalResponse(key)
			if delivery.Event.EndsResponse() {
				service.completeAssignment(connection, delivery.Identity.RequestID, delivery.LeaseID)
			}
			return ackReceipt(delivery), nil
		}
		return nackReceipt(delivery, WorkerRejectEventConflict, "event id was reused with different content"), nil
	}
	encodedDelivery, err := json.Marshal(delivery)
	if err != nil || int64(len(encodedDelivery))+workerEnvelopeReserve > service.workerPayloadLimitBytes {
		return nackReceipt(delivery, WorkerRejectInvalid, "worker event exceeds the core payload limit"), nil
	}
	state, err := service.loadDurableEventState(ctx, delivery)
	if errors.Is(err, ErrStoreRecordNotFound) {
		return nackReceipt(delivery, WorkerRejectNotFound, "completion is not known"), nil
	}
	if err != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: err}
	}
	if state.request.ResponseComplete {
		return nackReceipt(delivery, WorkerRejectResponseClosed, "response is already closed"), nil
	}
	if state.task.LeaseOwner != connection.principal.WorkerID {
		return nackReceipt(delivery, WorkerRejectStaleLease, "completion was leased to another worker"), nil
	}
	if state.task.LeaseID != delivery.LeaseID {
		return nackReceipt(delivery, WorkerRejectStaleLease, "worker lease is stale"), nil
	}
	if state.request.RequestID != delivery.Identity.RequestID ||
		state.request.Task.Task != delivery.Identity.TaskID ||
		state.task.WorkspaceKey != delivery.Identity.WorkspaceKey {
		return nackReceipt(delivery, WorkerRejectNotFound, "completion identity does not match"), nil
	}
	if state.toolCallExists {
		return nackReceipt(delivery, WorkerRejectToolConflict, "tool call id was already used by this task"), nil
	}
	registration, exists := service.codecs[state.request.Codec.ID]
	if !exists || !registration.snapshot.Equal(state.request.Codec) {
		return WorkerEventReceipt{}, &WorkerDeliveryError{
			Delivery: delivery.ID, EventID: delivery.Event.ID,
			Cause: fmt.Errorf("%w: persisted Codec is not registered", ErrInvalidCodecContract),
		}
	}
	request, encoder, err := service.rebuildEncoder(ctx, registration, state)
	if err != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: err}
	}
	now, err := checkedTime(service.clock)
	if err != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: err}
	}
	nextState, toolRecords, rejection := service.planWorkerEvent(ctx, state.task, request, delivery.Event, now)
	if rejection != "" {
		return nackReceipt(delivery, rejection, "worker event conflicts with durable task state"), nil
	}
	seed, err := service.seeds.EventSeed(ctx, EventSeedContext{
		Identity: delivery.Identity, Event: cloneEvent(delivery.Event),
	})
	if err != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: err}
	}
	seed, err = normalizeEventSeed(seed)
	if err != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{
			Delivery: delivery.ID, EventID: delivery.Event.ID,
			Cause: fmt.Errorf("invalid EventSeed JSON: %w", err),
		}
	}
	if err := seed.Validate(); err != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{
			Delivery: delivery.ID, EventID: delivery.Event.ID,
			Cause: fmt.Errorf("invalid EventSeed: %w", err),
		}
	}
	if seed.EncodedAtUnix <= 0 {
		return WorkerEventReceipt{}, &WorkerDeliveryError{
			Delivery: delivery.ID, EventID: delivery.Event.ID,
			Cause: errors.New("invalid EventSeed: encoded-at time must be positive"),
		}
	}
	frames, done, err := encoder.Encode(cloneEvent(delivery.Event), seed)
	if err != nil {
		return nackReceipt(delivery, WorkerRejectInvalid, "Codec rejected worker event"), nil
	}
	if state.request.Mode == ResponseStream {
		err = registration.description.Limits.CheckStreamFrames(frames)
	} else {
		err = registration.description.Limits.CheckAggregateFrames(frames, done)
	}
	if err != nil || done != delivery.Event.EndsResponse() {
		return nackReceipt(delivery, WorkerRejectInvalid, "Codec rejected the event output contract"), nil
	}
	if err := checkPersistedFrameStep(frames); err != nil {
		return nackReceipt(delivery, WorkerRejectInvalid, "Codec exceeded the durable event step limit"), nil
	}
	checkpoint, err := json.Marshal(encoderCheckpoint{
		Version: 1, Kind: "event", Event: ptrEvent(cloneEvent(delivery.Event)), Seed: &seed,
		LeaseID: delivery.LeaseID, Worker: connection.principal.WorkerID,
	})
	if err != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: err}
	}
	sequence := state.request.LastEventSequence + 1
	checkpointStored, err := service.sealPayload(ctx,
		responseEventBinding(state.request.Key, state.request.RequestID, sequence), checkpoint)
	if err != nil {
		if errors.Is(err, errPersistedPayloadLimit) {
			return nackReceipt(delivery, WorkerRejectInvalid, "worker event exceeds the persistence limit"), nil
		}
		return WorkerEventReceipt{}, &WorkerDeliveryError{Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: err}
	}
	newEvents := []StoreResponseEventRecord{{
		Request: state.request.Key, Sequence: sequence, Kind: StoreEventCheckpoint,
		Worker:        connection.principal.WorkerID,
		WorkerEventID: delivery.Event.ID, WorkerEventDigest: digest,
		Data: checkpointStored, CreatedAt: now,
	}}
	var aggregateBody []byte
	if state.request.Mode == ResponseStream {
		for _, frame := range frames {
			sequence++
			stored, sealErr := service.sealPayload(ctx,
				responseEventBinding(state.request.Key, state.request.RequestID, sequence), frame)
			if sealErr != nil {
				if errors.Is(sealErr, errPersistedPayloadLimit) {
					return nackReceipt(delivery, WorkerRejectInvalid, "response frame exceeds the persistence limit"), nil
				}
				return WorkerEventReceipt{}, &WorkerDeliveryError{Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: sealErr}
			}
			newEvents = append(newEvents, StoreResponseEventRecord{
				Request: state.request.Key, Sequence: sequence, Kind: StoreEventWire,
				Worker:        connection.principal.WorkerID,
				WorkerEventID: delivery.Event.ID, WorkerEventDigest: digest,
				Data: stored, CreatedAt: now,
			})
		}
	} else if done {
		aggregateBody, err = service.sealPayload(ctx,
			responseBodyBinding(state.request.Key, state.request.RequestID), frames[0])
		if err != nil {
			if errors.Is(err, errPersistedPayloadLimit) {
				return nackReceipt(delivery, WorkerRejectInvalid, "response body exceeds the persistence limit"), nil
			}
			return WorkerEventReceipt{}, &WorkerDeliveryError{Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: err}
		}
		if !storeRequestPayloadAllowed(
			int64(len(state.request.CanonicalPayload)), int64(len(aggregateBody)), service.readLimitBytes,
		) {
			return nackReceipt(delivery, WorkerRejectInvalid, "response record exceeds the persistence limit"), nil
		}
	}
	nextRequest := state.request
	nextRequest.LastEventSequence = sequence
	nextRequest.Revision++
	if done {
		nextRequest.ResponseComplete = true
		completed := now
		nextRequest.CompletedAt = &completed
		if nextRequest.Mode == ResponseAggregate {
			nextRequest.Decision = StoreResponseDecision{
				StatusCode:  aggregateTerminalStatus(registration.success, delivery.Event.Type),
				ContentType: registration.aggregateType,
				Body:        aggregateBody,
			}
		}
	}
	var nextTask *StoreTaskRecord
	if nextState != state.task.State {
		updated := state.task
		updated.State = nextState
		updated.Revision++
		updated.UpdatedAt = timestampAtLeast(now, state.task.UpdatedAt)
		nextTask = &updated
	}
	receipt := StoreWorkerReceiptRecord{
		Request: state.request.Key, EventID: delivery.Event.ID,
		Worker: connection.principal.WorkerID, Digest: digest, CreatedAt: now,
	}
	commitErr := service.store.Update(ctx, func(tx StoreTx) error {
		existing, receiptErr := tx.LoadWorkerReceipt(state.request.Key, delivery.Event.ID)
		if receiptErr == nil {
			if existing.Worker == receipt.Worker && existing.Digest == receipt.Digest {
				return nil
			}
			return ErrWorkerDeliveryConflict
		}
		if !errors.Is(receiptErr, ErrStoreRecordNotFound) {
			return receiptErr
		}
		currentRequest, loadErr := tx.LoadRequest(state.request.Key, StoreReadLimit{MaxBytes: service.readLimitBytes})
		if loadErr != nil {
			return loadErr
		}
		currentTask, loadErr := tx.LoadTask(state.task.Key)
		if loadErr != nil {
			return loadErr
		}
		if currentRequest.Revision != state.request.Revision || currentTask.Revision != state.task.Revision ||
			currentRequest.ResponseComplete || currentTask.LeaseOwner != connection.principal.WorkerID ||
			currentTask.LeaseID != delivery.LeaseID {
			return ErrWorkerDeliveryIndeterminate
		}
		for _, record := range toolRecords {
			if insertErr := tx.InsertToolExecution(record); insertErr != nil {
				return insertErr
			}
		}
		for _, event := range newEvents {
			if insertErr := tx.InsertResponseEvent(event); insertErr != nil {
				return insertErr
			}
		}
		if nextTask != nil {
			changed, changeErr := tx.CompareAndSwapTask(StoreTaskMutation{
				Key: state.task.Key, ExpectedRevision: state.task.Revision, Next: *nextTask,
			})
			if changeErr != nil {
				return changeErr
			}
			if !changed {
				return ErrWorkerDeliveryIndeterminate
			}
		}
		changed, changeErr := tx.CompareAndSwapRequest(StoreRequestMutation{
			Key: state.request.Key, ExpectedRevision: state.request.Revision, Next: nextRequest,
		})
		if changeErr != nil {
			return changeErr
		}
		if !changed {
			return ErrWorkerDeliveryIndeterminate
		}
		return tx.InsertWorkerReceipt(receipt)
	})
	if commitErr != nil && errors.Is(commitErr, ErrStoreCommitUnknown) {
		// The transaction may have committed. Wake readers before reconciliation;
		// a spurious wake is harmless, while suppressing it can strand a waiter
		// after the Store becomes temporarily unreadable.
		service.signalResponse(state.request.Key)
		matched, reconcileErr := service.reconcileWorkerReceipt(ctx, receipt)
		if reconcileErr != nil {
			return WorkerEventReceipt{}, &WorkerDeliveryError{
				Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: reconcileErr,
			}
		}
		if !matched {
			return WorkerEventReceipt{}, &WorkerDeliveryError{
				Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: ErrWorkerDeliveryIndeterminate,
			}
		}
		commitErr = nil
	}
	if commitErr != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{Delivery: delivery.ID, EventID: delivery.Event.ID, Cause: commitErr}
	}
	service.signalResponse(state.request.Key)
	if done {
		service.completeAssignment(connection, state.request.RequestID, delivery.LeaseID)
	}
	return ackReceipt(delivery), nil
}

func aggregateTerminalStatus(success int, event EventType) int {
	switch event {
	case EventRejected:
		return 409
	case EventExpired:
		return 504
	case EventFailed:
		return 500
	case EventUnavailable:
		return 503
	default:
		return success
	}
}

func (service *Service) loadWorkerReceipt(
	ctx context.Context,
	delivery WorkerEventDelivery,
) (StoreWorkerReceiptRecord, bool, error) {
	key := StoreRequestKey{Caller: delivery.Identity.CallerID, IdempotencyKey: delivery.Identity.IdempotencyKey}
	var receipt StoreWorkerReceiptRecord
	err := service.store.View(ctx, func(view StoreView) error {
		loaded, loadErr := view.LoadWorkerReceipt(key, delivery.Event.ID)
		receipt = loaded
		return loadErr
	})
	if errors.Is(err, ErrStoreRecordNotFound) {
		return StoreWorkerReceiptRecord{}, false, nil
	}
	if err != nil {
		return StoreWorkerReceiptRecord{}, false, err
	}
	if receipt.Request != key || receipt.EventID != delivery.Event.ID {
		return StoreWorkerReceiptRecord{}, false, errors.New("worker receipt identity does not match its lookup key")
	}
	return receipt, true, nil
}

func (service *Service) loadDurableEventState(
	ctx context.Context,
	delivery WorkerEventDelivery,
) (durableEventState, error) {
	key := StoreRequestKey{Caller: delivery.Identity.CallerID, IdempotencyKey: delivery.Identity.IdempotencyKey}
	var state durableEventState
	err := service.store.View(ctx, func(view StoreView) error {
		request, loadErr := view.LoadRequest(key, StoreReadLimit{MaxBytes: service.readLimitBytes})
		if loadErr != nil {
			return loadErr
		}
		state.request = request
		task, loadErr := view.LoadTask(request.Task)
		if loadErr != nil {
			return loadErr
		}
		state.task = task
		if delivery.Event.Type == EventToolCalls {
			for _, call := range delivery.Event.ToolCalls {
				_, toolErr := view.LoadToolExecution(StoreToolExecutionKey{
					Task: request.Task, ToolCallID: ToolCallID(call.ID),
				}, StoreReadLimit{MaxBytes: service.readLimitBytes})
				if toolErr == nil {
					state.toolCallExists = true
					break
				}
				if !errors.Is(toolErr, ErrStoreRecordNotFound) {
					return toolErr
				}
			}
		}
		var after uint64
		for {
			batch, scanErr := view.ScanResponseEvents(StoreResponseEventScan{
				Request: key, After: after, Limit: maximumResponseLimit,
				ReadLimit: StoreReadLimit{MaxBytes: service.readLimitBytes},
			})
			if scanErr != nil {
				return scanErr
			}
			if len(batch) == 0 {
				break
			}
			state.events = append(state.events, batch...)
			after = batch[len(batch)-1].Sequence
		}
		return nil
	})
	return state, err
}

func (service *Service) reconcileWorkerReceipt(
	ctx context.Context,
	want StoreWorkerReceiptRecord,
) (bool, error) {
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var found StoreWorkerReceiptRecord
	err := service.store.View(reconcileCtx, func(view StoreView) error {
		loaded, loadErr := view.LoadWorkerReceipt(want.Request, want.EventID)
		found = loaded
		return loadErr
	})
	if errors.Is(err, ErrStoreRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if found.Worker != want.Worker || found.Digest != want.Digest {
		return false, ErrWorkerDeliveryConflict
	}
	return true, nil
}

func (service *Service) planWorkerEvent(
	ctx context.Context,
	task StoreTaskRecord,
	request Request,
	event Event,
	now time.Time,
) (TaskState, []StoreToolExecutionRecord, WorkerRejectionCode) {
	state := task.State
	switch event.Type {
	case EventAccepted:
		if state == TaskAwaitingHuman {
			return state, nil, ""
		}
		return state, nil, WorkerRejectStateConflict
	case EventProgress:
		if state != TaskAwaitingHuman {
			return state, nil, WorkerRejectStateConflict
		}
		return state, nil, ""
	case EventFinal:
		if state != TaskAwaitingHuman {
			return state, nil, WorkerRejectStateConflict
		}
		return TaskCompleted, nil, ""
	case EventClarification:
		if state != TaskAwaitingHuman {
			return state, nil, WorkerRejectStateConflict
		}
		if task.CapabilityTier == TierChat {
			return TaskCompleted, nil, ""
		}
		return TaskAwaitingCaller, nil, ""
	case EventToolCalls:
		if state != TaskAwaitingHuman {
			return state, nil, WorkerRejectStateConflict
		}
		if rejection := service.validateAndAuthorizeToolCalls(ctx, task, request, event.ToolCalls); rejection != "" {
			return state, nil, rejection
		}
		if task.CapabilityTier == TierChat {
			return TaskCompleted, nil, ""
		}
		records := make([]StoreToolExecutionRecord, 0, len(event.ToolCalls))
		for _, call := range event.ToolCalls {
			digest, digestErr := stableDigest(struct {
				Namespace string         `json:"namespace,omitempty"`
				Name      string         `json:"name"`
				Input     map[string]any `json:"input"`
			}{call.Namespace, call.Name, call.Input})
			if digestErr != nil {
				return state, nil, WorkerRejectInvalid
			}
			records = append(records, StoreToolExecutionRecord{
				Key:         StoreToolExecutionKey{Task: task.Key, ToolCallID: ToolCallID(call.ID)},
				InputDigest: digest, State: ToolExecutionPending, Revision: 1, CreatedAt: now,
			})
		}
		return TaskAwaitingResults, records, ""
	case EventRejected:
		return TaskRejected, nil, terminalEventAllowed(state)
	case EventExpired:
		return TaskExpired, nil, terminalEventAllowed(state)
	case EventFailed, EventUnavailable:
		return TaskFailed, nil, terminalEventAllowed(state)
	default:
		return state, nil, WorkerRejectInvalid
	}
}

func terminalEventAllowed(state TaskState) WorkerRejectionCode {
	if state == TaskAwaitingHuman {
		return ""
	}
	return WorkerRejectStateConflict
}

func (service *Service) validateAndAuthorizeToolCalls(
	ctx context.Context,
	task StoreTaskRecord,
	request Request,
	calls []ToolCall,
) WorkerRejectionCode {
	declared := make(map[string]struct{}, len(request.Tools))
	for _, tool := range request.Tools {
		declared[tool.QualifiedName()] = struct{}{}
	}
	for _, call := range calls {
		if _, ok := declared[call.QualifiedName()]; !ok {
			return WorkerRejectForbidden
		}
		if task.CapabilityTier != TierChat {
			if service.toolAuthorizer == nil {
				return WorkerRejectForbidden
			}
			authorization := ToolAuthorization{
				CallerID: task.Key.Caller, Task: cloneStoreTaskRecord(task),
				Request: cloneTransportRequest(request), Call: cloneToolCall(call),
			}
			if err := service.toolAuthorizer.AuthorizeTool(ctx, authorization); err != nil {
				return WorkerRejectForbidden
			}
		}
	}
	return ""
}

func workerEventDigest(delivery WorkerEventDelivery) (StoreDigest, error) {
	return stableDigest(struct {
		Identity CompletionIdentity `json:"identity"`
		LeaseID  WorkerLeaseID      `json:"lease_id"`
		Event    Event              `json:"event"`
	}{delivery.Identity, delivery.LeaseID, delivery.Event})
}

func ackReceipt(delivery WorkerEventDelivery) WorkerEventReceipt {
	return WorkerEventReceipt{
		Delivery: delivery.ID, EventID: delivery.Event.ID, Decision: WorkerEventACK,
	}
}

func nackReceipt(delivery WorkerEventDelivery, code WorkerRejectionCode, message string) WorkerEventReceipt {
	return WorkerEventReceipt{
		Delivery: delivery.ID, EventID: delivery.Event.ID, Decision: WorkerEventNACK,
		Code: code, Message: message,
	}
}

func ptrEvent(event Event) *Event { return &event }

func cloneToolCall(call ToolCall) ToolCall {
	call.Input = cloneTransportMap(call.Input)
	return call
}

func cloneEvent(event Event) Event {
	event.ToolCalls = cloneTransportToolCalls(event.ToolCalls)
	return event
}

func timestampAtLeast(candidate, floor time.Time) time.Time {
	if candidate.Before(floor) {
		return floor
	}
	return candidate
}
