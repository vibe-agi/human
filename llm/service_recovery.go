package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (service *Service) recover(ctx context.Context) error {
	var cursor *StoreRecoveryCursor
	for {
		var batch []StoreRecoveryRecord
		err := service.store.View(ctx, func(view StoreView) error {
			loaded, scanErr := view.ScanRecovery(StoreRecoveryScan{
				After: cursor, Limit: defaultResponseLimit,
				ReadLimit: StoreReadLimit{MaxBytes: maximumRecoveryReadLimitBytes},
			})
			batch = loaded
			return scanErr
		})
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, record := range batch {
			if err := service.recoverRequest(ctx, record); err != nil {
				if quarantineErr := service.quarantineRecovery(ctx, record, err); quarantineErr != nil {
					return fmt.Errorf(
						"quarantine request %s/%s after %v: %w",
						record.Request.Key.Caller, record.Request.Key.IdempotencyKey, err, quarantineErr,
					)
				}
			}
		}
		last := batch[len(batch)-1].Request
		cursor = &StoreRecoveryCursor{
			CreatedAt: last.CreatedAt, Caller: last.Key.Caller, IdempotencyKey: last.Key.IdempotencyKey,
		}
	}
}

func (service *Service) quarantineRecovery(
	ctx context.Context,
	record StoreRecoveryRecord,
	recoveryCause error,
) error {
	if record.Request.ResponseComplete && record.Request.RecoveryQuarantined {
		return nil
	}
	failure := AdmissionFailure{
		Status: 500, Code: "recovery_failed",
		Message: "HumanLLM could not safely recover this response",
	}
	body := []byte(`{"error":{"code":"recovery_failed","message":"HumanLLM could not safely recover this response"}}`)
	contentType := "application/json"
	if registration, exists := service.codecs[record.Request.Codec.ID]; exists &&
		registration.snapshot.Equal(record.Request.Codec) {
		encoded, err := registration.codec.AdmissionError(failure)
		if err == nil && registration.description.Limits.CheckAdmissionError(encoded) == nil {
			body = encoded
		}
		contentType = registration.aggregateType
	}
	now, err := checkedTime(service.clock)
	if err != nil {
		return err
	}
	nextRequest := record.Request
	nextRequest.RecoveryQuarantined = true
	nextRequest.Revision++
	var newEvents []StoreResponseEventRecord
	if record.Request.ResponseComplete {
		// An already-complete response can be selected for recovery when its
		// worker receipt is corrupt or missing. Quarantine that bookkeeping fault
		// without rewriting caller-visible bytes or appending a second terminal.
	} else if record.Request.Mode == ResponseStream && record.Request.Decision.StatusCode != 0 {
		nextRequest.ResponseComplete = true
		completed := now
		nextRequest.CompletedAt = &completed
		frames := service.recoveryTerminalFrames(ctx, record)
		if len(frames) != 0 {
			sequence := record.Request.LastEventSequence + 1
			checkpoint, marshalErr := json.Marshal(struct {
				Version uint8  `json:"version"`
				Kind    string `json:"kind"`
				Code    string `json:"code"`
			}{Version: 1, Kind: "quarantine", Code: "recovery_failed"})
			if marshalErr != nil {
				return marshalErr
			}
			storedCheckpoint, sealErr := service.sealPayload(ctx,
				responseEventBinding(record.Request.Key, record.Request.RequestID, sequence), checkpoint)
			if sealErr != nil && !errors.Is(sealErr, errPersistedPayloadLimit) {
				return sealErr
			}
			if sealErr == nil {
				candidate := []StoreResponseEventRecord{{
					Request: record.Request.Key, Sequence: sequence, Kind: StoreEventCheckpoint,
					Data: storedCheckpoint, CreatedAt: now,
				}}
				for _, frame := range frames {
					sequence++
					storedFrame, frameErr := service.sealPayload(ctx,
						responseEventBinding(record.Request.Key, record.Request.RequestID, sequence), frame)
					if errors.Is(frameErr, errPersistedPayloadLimit) {
						candidate = nil
						break
					}
					if frameErr != nil {
						return frameErr
					}
					candidate = append(candidate, StoreResponseEventRecord{
						Request: record.Request.Key, Sequence: sequence, Kind: StoreEventWire,
						Data: storedFrame, CreatedAt: now,
					})
				}
				if candidate != nil {
					newEvents = candidate
					nextRequest.LastEventSequence = sequence
				}
			}
		}
	} else {
		nextRequest.ResponseComplete = true
		completed := now
		nextRequest.CompletedAt = &completed
		storedBody, sealErr := service.sealPayload(ctx,
			responseBodyBinding(record.Request.Key, record.Request.RequestID), body)
		if sealErr != nil {
			if !errors.Is(sealErr, errPersistedPayloadLimit) {
				return sealErr
			}
			storedBody = nil
		}
		if !storeRequestPayloadAllowed(
			int64(len(record.Request.CanonicalPayload)), int64(len(storedBody)), service.readLimitBytes,
		) {
			// Preserve a finite HTTP error decision even when the corrupt request
			// consumed the complete Store read budget. An empty diagnostic body is
			// preferable to making startup and replay permanently impossible.
			storedBody = nil
		}
		nextRequest.Decision = StoreResponseDecision{
			StatusCode: failure.Status, ContentType: contentType, Body: storedBody,
		}
	}
	var nextTask *StoreTaskRecord
	if !record.Task.State.Terminal() {
		updated := record.Task
		updated.State = TaskFailed
		updated.Revision++
		updated.UpdatedAt = timestampAtLeast(now, record.Task.UpdatedAt)
		nextTask = &updated
	}
	commitErr := service.store.Update(ctx, func(tx StoreTx) error {
		currentRequest, loadErr := tx.LoadRequest(
			record.Request.Key, StoreReadLimit{MaxBytes: maximumRecoveryReadLimitBytes},
		)
		if loadErr != nil {
			return loadErr
		}
		if currentRequest.RecoveryQuarantined && currentRequest.ResponseComplete {
			return nil
		}
		if currentRequest.Revision != record.Request.Revision {
			return ErrWorkerDeliveryIndeterminate
		}
		if nextTask != nil {
			currentTask, taskErr := tx.LoadTask(record.Task.Key)
			if taskErr != nil {
				return taskErr
			}
			if currentTask.Revision != record.Task.Revision {
				return ErrWorkerDeliveryIndeterminate
			}
			changed, taskErr := tx.CompareAndSwapTask(StoreTaskMutation{
				Key: record.Task.Key, ExpectedRevision: record.Task.Revision, Next: *nextTask,
			})
			if taskErr != nil {
				return taskErr
			}
			if !changed {
				return ErrWorkerDeliveryIndeterminate
			}
		}
		for _, event := range newEvents {
			if insertErr := tx.InsertResponseEvent(event); insertErr != nil {
				return insertErr
			}
		}
		changed, changeErr := tx.CompareAndSwapRequest(StoreRequestMutation{
			Key: record.Request.Key, ExpectedRevision: record.Request.Revision, Next: nextRequest,
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
			head, loadErr := view.LoadRequestHead(record.Request.Key)
			if loadErr != nil {
				return loadErr
			}
			if !head.RecoveryQuarantined || !head.ResponseComplete {
				return ErrWorkerDeliveryIndeterminate
			}
			return nil
		})
	}
	if commitErr != nil {
		return errors.Join(fmt.Errorf("persist finite recovery failure: %w", commitErr), recoveryCause)
	}
	return nil
}

// recoveryTerminalFrames is best-effort: when deterministic replay is still
// possible it asks the exact persisted Codec state to encode a native failed
// terminal. If the history itself is corrupt, quarantine closes the response
// without appending dialect-invalid bytes; EOF is safer than injecting an
// aggregate AdmissionError body into an established stream.
func (service *Service) recoveryTerminalFrames(
	ctx context.Context,
	record StoreRecoveryRecord,
) [][]byte {
	registration, exists := service.codecs[record.Request.Codec.ID]
	if !exists || !registration.snapshot.Equal(record.Request.Codec) {
		return nil
	}
	var events []StoreResponseEventRecord
	err := service.store.View(ctx, func(view StoreView) error {
		var after uint64
		for {
			batch, scanErr := view.ScanResponseEvents(StoreResponseEventScan{
				Request: record.Request.Key, After: after, Limit: maximumResponseLimit,
				ReadLimit: StoreReadLimit{MaxBytes: service.readLimitBytes},
			})
			if scanErr != nil {
				return scanErr
			}
			if len(batch) == 0 {
				return nil
			}
			events = append(events, batch...)
			after = batch[len(batch)-1].Sequence
		}
	})
	if err != nil {
		return nil
	}
	state := durableEventState{task: record.Task, request: record.Request, events: events}
	_, encoder, err := service.rebuildEncoder(ctx, registration, state)
	if err != nil {
		return nil
	}
	identity := CompletionIdentity{
		CallerID: record.Request.Key.Caller, RequestID: record.Request.RequestID,
		TaskID: record.Request.Task.Task, WorkspaceKey: record.Task.WorkspaceKey,
		IdempotencyKey: record.Request.Key.IdempotencyKey,
	}
	event := Event{
		ID: "recovery-" + record.Request.RequestID, Type: EventFailed,
		ErrorCode: "recovery_failed", Error: "HumanLLM could not safely recover this response",
	}
	seed, err := service.seeds.EventSeed(ctx, EventSeedContext{Identity: identity, Event: cloneEvent(event)})
	if err == nil {
		seed, err = normalizeEventSeed(seed)
	}
	if err != nil || seed.Validate() != nil || seed.EncodedAtUnix <= 0 {
		return nil
	}
	frames, done, err := encoder.Encode(event, seed)
	if err != nil || !done || registration.description.Limits.CheckStreamFrames(frames) != nil ||
		checkPersistedFrameStep(frames) != nil {
		return nil
	}
	return frames
}

func (service *Service) recoverRequest(ctx context.Context, record StoreRecoveryRecord) error {
	if record.Request.RecoveryQuarantined || record.Request.PayloadPrunedAt != nil {
		return errors.New("in-flight request is quarantined or tombstoned")
	}
	if record.Task.Key != record.Request.Task || record.Task.LeaseOwner == "" || record.Task.LeaseID == "" {
		return errors.New("in-flight request has no valid durable lease")
	}
	if record.Request.ResponseComplete {
		return errors.New("complete response has an unacknowledged worker event")
	}
	registration, exists := service.codecs[record.Request.Codec.ID]
	if !exists || !registration.snapshot.Equal(record.Request.Codec) || !record.Task.Codec.Equal(record.Request.Codec) {
		return errors.New("persisted Codec identity is not registered exactly")
	}
	var events []StoreResponseEventRecord
	err := service.store.View(ctx, func(view StoreView) error {
		var after uint64
		for {
			batch, scanErr := view.ScanResponseEvents(StoreResponseEventScan{
				Request: record.Request.Key, After: after, Limit: maximumResponseLimit,
				ReadLimit: StoreReadLimit{MaxBytes: service.readLimitBytes},
			})
			if scanErr != nil {
				return scanErr
			}
			if len(batch) == 0 {
				return nil
			}
			events = append(events, batch...)
			after = batch[len(batch)-1].Sequence
		}
	})
	if err != nil {
		return err
	}
	state := durableEventState{task: record.Task, request: record.Request, events: events}
	request, _, err := service.rebuildEncoder(ctx, registration, state)
	if err != nil {
		return err
	}
	if err := service.validateRecoveryReceipts(ctx, record.Request, events); err != nil {
		return err
	}
	identity := CompletionIdentity{
		CallerID: record.Request.Key.Caller, RequestID: record.Request.RequestID,
		TaskID: record.Task.Key.Task, WorkspaceKey: record.Task.WorkspaceKey,
		IdempotencyKey: record.Request.Key.IdempotencyKey,
	}
	boundary := AssignmentAfterAdmission
	if record.Request.Mode == ResponseStream {
		if record.Request.Decision.StatusCode == 0 {
			return errors.New("streaming request lacks its durable response decision")
		}
		boundary = AssignmentAfterResponse
	}
	delivery := WorkerAssignmentDelivery{
		ID: stableAssignmentDeliveryID(record.Request.RequestID, record.Task.LeaseID),
		Assignment: Assignment{
			Identity: identity,
			Lease:    WorkerLease{ID: record.Task.LeaseID, Owner: record.Task.LeaseOwner},
			Boundary: boundary, Task: taskContextFromRecord(record.Task), Request: request,
		},
	}
	if err := delivery.ValidateFor(AuthenticatedWorker{
		WorkerID: record.Task.LeaseOwner, SessionID: assignmentValidationSession,
	}); err != nil {
		return fmt.Errorf("validate recovered worker assignment: %w", err)
	}
	encodedDelivery, err := json.Marshal(delivery)
	if err != nil {
		return fmt.Errorf("encode recovered worker assignment: %w", err)
	}
	if int64(len(encodedDelivery))+workerEnvelopeReserve > service.workerPayloadLimitBytes {
		return errors.New("recovered worker assignment exceeds the current payload limit")
	}
	switch record.Task.State {
	case TaskLeased:
		service.addAssignment(record.Request.Key, delivery, true, record.Request.CreatedAt)
	case TaskAwaitingHuman:
		service.addAssignment(record.Request.Key, delivery, false, record.Request.CreatedAt)
	default:
		// A response may be between events (for example awaiting caller after a
		// clarification) only if its terminal decision was committed atomically.
		if !record.Request.ResponseComplete {
			return fmt.Errorf("incomplete response has incompatible task state %q", record.Task.State)
		}
	}
	return nil
}

type recoveryReceiptExpectation struct {
	eventID string
	worker  WorkerID
	digest  StoreDigest
}

func (service *Service) validateRecoveryReceipts(
	ctx context.Context,
	request StoreRequestRecord,
	events []StoreResponseEventRecord,
) error {
	expectations := make([]recoveryReceiptExpectation, 0)
	seen := make(map[string]struct{})
	for _, record := range events {
		if record.Kind != StoreEventCheckpoint || record.WorkerEventID == "" {
			continue
		}
		if _, duplicate := seen[record.WorkerEventID]; duplicate {
			return fmt.Errorf("worker event %q appears more than once in response history", record.WorkerEventID)
		}
		seen[record.WorkerEventID] = struct{}{}
		checkpoint, err := service.openCheckpoint(ctx, request, record)
		if err != nil {
			return err
		}
		if checkpoint.Worker == "" || record.Worker != checkpoint.Worker || checkpoint.Event == nil ||
			checkpoint.Event.ID != record.WorkerEventID || record.WorkerEventDigest == "" {
			return fmt.Errorf("worker event %q has no durable ownership proof", record.WorkerEventID)
		}
		expectations = append(expectations, recoveryReceiptExpectation{
			eventID: record.WorkerEventID, worker: checkpoint.Worker,
			digest: record.WorkerEventDigest,
		})
	}
	return service.store.View(ctx, func(view StoreView) error {
		for _, expected := range expectations {
			receipt, err := view.LoadWorkerReceipt(request.Key, expected.eventID)
			if err != nil {
				if errors.Is(err, ErrStoreRecordNotFound) {
					return fmt.Errorf("worker event %q is missing its durable receipt", expected.eventID)
				}
				return err
			}
			if receipt.Request != request.Key || receipt.EventID != expected.eventID ||
				receipt.Worker != expected.worker || receipt.Digest != expected.digest {
				return fmt.Errorf("worker event %q receipt does not match its durable checkpoint", expected.eventID)
			}
		}
		return nil
	})
}

func (service *Service) rebuildEncoder(
	ctx context.Context,
	registration registeredCodec,
	state durableEventState,
) (Request, Encoder, error) {
	canonical, err := service.openPayload(ctx,
		requestPayloadBinding(state.request.Key, state.request.RequestID), state.request.CanonicalPayload)
	if err != nil {
		return Request{}, nil, err
	}
	var request Request
	if err := decodeCanonicalJSON(canonical, &request); err != nil {
		return Request{}, nil, fmt.Errorf("decode canonical request: %w", err)
	}
	if err := request.Validate(); err != nil {
		return Request{}, nil, fmt.Errorf("validate canonical request: %w", err)
	}
	reencoded, err := json.Marshal(request)
	if err != nil || !bytes.Equal(reencoded, canonical) {
		return Request{}, nil, errors.New("canonical request persistence is not canonical")
	}
	digest, _, err := canonicalRequestDigest(registration.snapshot, request)
	if err != nil || digest != state.request.RequestDigest {
		return Request{}, nil, errors.New("canonical request digest does not match persistence")
	}
	if len(state.events) == 0 || state.events[0].Sequence != 1 || state.events[0].Kind != StoreEventCheckpoint ||
		state.events[0].Worker != "" || state.events[0].WorkerEventID != "" || state.events[0].WorkerEventDigest != "" {
		return Request{}, nil, errors.New("response session checkpoint is missing")
	}
	for index, event := range state.events {
		if event.Request != state.request.Key || event.Sequence != uint64(index+1) {
			return Request{}, nil, errors.New("response event history is not contiguous or belongs to another request")
		}
	}
	checkpoint, err := service.openCheckpoint(ctx, state.request, state.events[0])
	if err != nil {
		return Request{}, nil, err
	}
	if checkpoint.Kind != "session" || checkpoint.Session == nil || checkpoint.Event != nil || checkpoint.Seed != nil ||
		checkpoint.LeaseID != "" || checkpoint.Worker != "" {
		return Request{}, nil, errors.New("first response checkpoint is not a session")
	}
	session := *checkpoint.Session
	if session.ResponseID != state.request.ResponseID || session.Model != request.Model ||
		session.Seed.ToolCallPolicy != request.ToolCallPolicy {
		return Request{}, nil, errors.New("response session checkpoint identity mismatch")
	}
	if err := session.Validate(); err != nil {
		return Request{}, nil, err
	}
	var encoder Encoder
	if state.request.Mode == ResponseStream {
		encoder, err = registration.codec.NewStream(session)
	} else if state.request.Mode == ResponseAggregate {
		encoder, err = registration.codec.NewAggregate(session)
	} else {
		return Request{}, nil, errors.New("response mode is invalid")
	}
	if err != nil || nilInterface(encoder) {
		return Request{}, nil, fmt.Errorf("rebuild response encoder: %w", err)
	}
	frames, err := encoder.Start()
	if err != nil {
		return Request{}, nil, err
	}
	if state.request.Mode == ResponseStream {
		err = registration.description.Limits.CheckStreamFrames(frames)
	} else {
		err = registration.description.Limits.CheckAggregateFrames(frames, false)
	}
	if err != nil {
		return Request{}, nil, err
	}
	if err := checkPersistedFrameStep(frames); err != nil {
		return Request{}, nil, err
	}
	index, err := service.verifyPersistedFrames(ctx, state.request, state.events, 1, frames, "", "", "")
	if err != nil {
		return Request{}, nil, err
	}
	ended := false
	identity := CompletionIdentity{
		CallerID: state.request.Key.Caller, RequestID: state.request.RequestID,
		TaskID: state.request.Task.Task, WorkspaceKey: state.task.WorkspaceKey,
		IdempotencyKey: state.request.Key.IdempotencyKey,
	}
	for index < len(state.events) {
		record := state.events[index]
		if record.Kind != StoreEventCheckpoint || record.WorkerEventID == "" || record.WorkerEventDigest == "" {
			return Request{}, nil, errors.New("response event history has an orphan wire frame")
		}
		checkpoint, err = service.openCheckpoint(ctx, state.request, record)
		if err != nil {
			return Request{}, nil, err
		}
		if checkpoint.Kind != "event" || checkpoint.Event == nil || checkpoint.Seed == nil || checkpoint.Session != nil ||
			checkpoint.LeaseID == "" || validateWorkerStableKey("checkpoint worker", string(checkpoint.Worker)) != nil {
			return Request{}, nil, errors.New("response event checkpoint shape is invalid")
		}
		if checkpoint.Worker != record.Worker || checkpoint.Event.ID != record.WorkerEventID {
			return Request{}, nil, errors.New("response event checkpoint identity mismatch")
		}
		digest, digestErr := workerEventDigest(WorkerEventDelivery{
			Identity: identity, LeaseID: checkpoint.LeaseID, Event: *checkpoint.Event,
		})
		if digestErr != nil || digest != record.WorkerEventDigest {
			return Request{}, nil, errors.New("response event checkpoint digest mismatch")
		}
		frames, done, encodeErr := encoder.Encode(cloneEvent(*checkpoint.Event), *checkpoint.Seed)
		if encodeErr != nil {
			return Request{}, nil, encodeErr
		}
		if done != checkpoint.Event.EndsResponse() || ended {
			return Request{}, nil, errors.New("response event terminal history is invalid")
		}
		if state.request.Mode == ResponseStream {
			encodeErr = registration.description.Limits.CheckStreamFrames(frames)
		} else {
			encodeErr = registration.description.Limits.CheckAggregateFrames(frames, done)
		}
		if encodeErr != nil {
			return Request{}, nil, encodeErr
		}
		if encodeErr = checkPersistedFrameStep(frames); encodeErr != nil {
			return Request{}, nil, encodeErr
		}
		ended = done
		index, err = service.verifyPersistedFrames(
			ctx, state.request, state.events, index+1, frames,
			record.Worker, record.WorkerEventID, record.WorkerEventDigest,
		)
		if err != nil {
			return Request{}, nil, err
		}
	}
	if state.request.LastEventSequence != state.events[len(state.events)-1].Sequence {
		return Request{}, nil, errors.New("response event cursor does not match history")
	}
	if ended != state.request.ResponseComplete {
		return Request{}, nil, errors.New("response terminal history does not match request state")
	}
	return request, encoder, nil
}

func (service *Service) openCheckpoint(
	ctx context.Context,
	request StoreRequestRecord,
	event StoreResponseEventRecord,
) (encoderCheckpoint, error) {
	encoded, err := service.openPayload(ctx,
		responseEventBinding(request.Key, request.RequestID, event.Sequence), event.Data)
	if err != nil {
		return encoderCheckpoint{}, err
	}
	var checkpoint encoderCheckpoint
	if err := decodeCanonicalJSON(encoded, &checkpoint); err != nil {
		return encoderCheckpoint{}, fmt.Errorf("decode response checkpoint: %w", err)
	}
	canonical, err := json.Marshal(checkpoint)
	if err != nil || !bytes.Equal(canonical, encoded) || checkpoint.Version != 1 {
		return encoderCheckpoint{}, errors.New("response checkpoint is not canonical version 1")
	}
	return checkpoint, nil
}

func (service *Service) verifyPersistedFrames(
	ctx context.Context,
	request StoreRequestRecord,
	events []StoreResponseEventRecord,
	index int,
	frames [][]byte,
	worker WorkerID,
	eventID string,
	digest StoreDigest,
) (int, error) {
	for _, expected := range frames {
		if index >= len(events) {
			return index, errors.New("response history is missing an encoded frame")
		}
		persisted := events[index]
		if persisted.Kind != StoreEventWire || persisted.Worker != worker || persisted.WorkerEventID != eventID ||
			persisted.WorkerEventDigest != digest {
			return index, errors.New("response frame ownership does not match its checkpoint")
		}
		actual, err := service.openPayload(ctx,
			responseEventBinding(request.Key, request.RequestID, persisted.Sequence), persisted.Data)
		if err != nil {
			return index, err
		}
		if !bytes.Equal(actual, expected) {
			return index, errors.New("response frame differs from deterministic Codec replay")
		}
		index++
	}
	return index, nil
}
