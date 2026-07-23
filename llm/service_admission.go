package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/vibe-agi/human/observe"
)

type admissionTaskPlan struct {
	existing  bool
	preempt   bool
	preempted StoreRequestHead
	task      StoreTaskRecord
	results   []toolResultMutation
}

type toolResultMutation struct {
	current StoreToolExecutionRecord
	next    StoreToolExecutionRecord
}

type submittedToolResult struct {
	data    []byte
	isError bool
}

func assignmentTaskContext(input TaskContext, taskID TaskID, plan admissionTaskPlan) TaskContext {
	if plan.existing {
		return taskContextFromRecord(plan.task)
	}
	input = normalizeTaskContext(input)
	if input.CapabilityTier != TierChat {
		input.TaskID = taskID
	}
	return input
}

func taskContextFromRecord(task StoreTaskRecord) TaskContext {
	context := TaskContext{
		WorkspaceKey: task.WorkspaceKey, CapabilityTier: task.CapabilityTier,
		HarnessID: task.HarnessID, HarnessVersion: task.HarnessVersion,
		HarnessSessionID: task.HarnessSessionID, ExecAllowed: task.ExecAllowed,
	}
	context = normalizeTaskContext(context)
	if context.CapabilityTier != TierChat {
		context.TaskID = task.Key.Task
	}
	return context
}

type encoderCheckpoint struct {
	Version uint8           `json:"version"`
	Kind    string          `json:"kind"`
	Session *EncoderSession `json:"session,omitempty"`
	Event   *Event          `json:"event,omitempty"`
	Seed    *EventSeed      `json:"seed,omitempty"`
	LeaseID WorkerLeaseID   `json:"lease_id,omitempty"`
	Worker  WorkerID        `json:"worker_id,omitempty"`
}

// Admit durably decides one canonical model response before making its
// assignment visible. Exact retries return the original response projection
// and never enqueue a second assignment.
func (service *Service) Admit(ctx context.Context, input AdmissionRequest) (AdmissionResult, error) {
	end, err := service.beginOperation()
	if err != nil {
		return AdmissionResult{}, err
	}
	defer end()
	if ctx == nil {
		return AdmissionResult{}, fmt.Errorf("%w: context is required", ErrInvalidServiceConfig)
	}
	if err := ctx.Err(); err != nil {
		return AdmissionResult{}, err
	}
	if err := validateWorkerStableKey("caller id", string(input.CallerID)); err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 400, Code: "invalid_caller", Message: "caller identity is invalid",
		}, 0, err)
	}
	if err := validateWorkerStableKey("idempotency key", string(input.IdempotencyKey)); err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 400, Code: "invalid_idempotency_key", Message: "idempotency key is invalid",
		}, 0, err)
	}
	registration, exists := service.codecs[input.CodecID]
	if !exists {
		return AdmissionResult{}, fmt.Errorf("%w: Codec %q is not registered", ErrInvalidServiceConfig, input.CodecID)
	}
	input.Task = normalizeTaskContext(input.Task)
	if err := validateTaskContext(input.Task); err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 400, Code: "invalid_task", Message: "task identity is invalid",
		}, 0, err)
	}
	if err := registration.description.Limits.CheckRequestSize(int64(len(input.Body))); err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 413, Code: "request_too_large", Message: "request body exceeds the Codec limit",
		}, 0, err)
	}
	request, err := registration.codec.Decode(input.Body)
	if err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 400, Code: "invalid_request", Message: "request body is invalid",
		}, 0, err)
	}
	request, err = normalizeCanonicalJSON(request)
	if err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 400, Code: "invalid_request", Message: "decoded request is not canonical JSON",
		}, 0, err)
	}
	if err := request.Validate(); err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 400, Code: "invalid_request", Message: "decoded request is invalid",
		}, 0, err)
	}
	digest, canonical, err := canonicalRequestDigest(registration.snapshot, request)
	if err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 500, Code: "encoding_failed", Message: "request identity could not be computed",
		}, 0, err)
	}
	key := StoreRequestKey{Caller: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	if replay, found, replayErr := service.replayAdmission(ctx, key, digest); found || replayErr != nil {
		return replay, replayErr
	}
	policyDecision, policyErr := service.admission.Admit(ctx, AdmissionContext{
		CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey,
		Codec:   cloneCodecDescription(registration.description),
		Request: cloneTransportRequest(request), Task: input.Task,
		CallerAttributes: cloneCallerAttributes(input.CallerAttributes),
	})
	if policyErr != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 500, Code: "policy_failed", Message: "admission policy failed",
		}, 0, policyErr)
	}
	if !policyDecision.Allowed {
		if err := policyDecision.Failure.Validate(); err != nil {
			return AdmissionResult{}, fmt.Errorf("%w: AdmissionPolicy returned invalid denial: %v", ErrInvalidServiceConfig, err)
		}
		return AdmissionResult{}, service.admissionFailure(input.CodecID, policyDecision.Failure, policyDecision.RetryAfter, ErrWorkerRouteDenied)
	}

	taskID := input.Task.TaskID
	if taskID == "" && input.Task.CapabilityTier != TierChat {
		var affinityTask StoreTaskRecord
		findErr := service.store.View(ctx, func(view StoreView) error {
			loaded, loadErr := view.FindOpenTask(StoreTaskAffinity{
				Caller: input.CallerID, WorkspaceKey: input.Task.WorkspaceKey,
				HarnessID: input.Task.HarnessID, HarnessVersion: input.Task.HarnessVersion,
				HarnessSessionID: input.Task.HarnessSessionID,
			})
			affinityTask = loaded
			return loadErr
		})
		if findErr == nil {
			taskID = affinityTask.Key.Task
		} else if !errors.Is(findErr, ErrStoreRecordNotFound) {
			return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "task affinity lookup failed", findErr)
		}
	}
	if taskID == "" {
		allocated, allocationErr := service.ids.NewID(ctx, IDTask)
		if allocationErr != nil || validGeneratedID(IDTask, allocated) != nil {
			return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
				Status: 500, Code: "id_allocation_failed", Message: "task identity could not be allocated",
			}, 0, errors.Join(allocationErr, validGeneratedID(IDTask, allocated)))
		}
		taskID = TaskID(allocated)
	}
	taskKey := StoreTaskKey{Caller: input.CallerID, Task: taskID}
	plan, err := service.planAdmissionTask(ctx, taskKey, input.Task, registration.snapshot, request)
	if err != nil {
		if errors.Is(err, errPersistedPayloadLimit) {
			return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
				Status: 413, Code: "tool_result_too_large", Message: "tool results exceed the persistence limit",
			}, 0, err)
		}
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 409, Code: "task_conflict", Message: "task cannot accept this request",
		}, 0, err)
	}
	worker, err := service.routeAdmission(ctx, input, request, taskID, plan)
	if err != nil {
		status := registration.description.OverloadedStatus
		code := "worker_unavailable"
		message := "no Human worker can accept this request"
		retry := 5 * time.Second
		if errors.Is(err, ErrWorkerRouteDenied) {
			status, code = 403, "worker_route_denied"
			retry = 0
		}
		if errors.Is(err, ErrWorkerRouterRequired) {
			status, code = 500, "worker_router_required"
			message = "multiple Human workers are connected; the deployment must configure a WorkerRouter"
			retry = 0
		}
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: status, Code: code, Message: message,
		}, retry, err)
	}

	requestID, err := service.allocateID(ctx, IDRequest)
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "request id allocation failed", err)
	}
	responseID, err := service.allocateID(ctx, IDResponse)
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "response id allocation failed", err)
	}
	leaseValue, err := service.allocateID(ctx, IDLease)
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "lease id allocation failed", err)
	}
	lease := WorkerLease{ID: WorkerLeaseID(leaseValue), Owner: worker}
	identity := CompletionIdentity{
		CallerID: input.CallerID, RequestID: requestID, TaskID: taskID,
		WorkspaceKey: input.Task.WorkspaceKey, IdempotencyKey: input.IdempotencyKey,
	}
	sessionSeed, err := service.seeds.SessionSeed(ctx, SeedContext{Identity: identity, Request: cloneTransportRequest(request)})
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "session seed generation failed", err)
	}
	sessionSeed.ToolCallPolicy = request.ToolCallPolicy
	sessionSeed, err = normalizeSessionSeed(sessionSeed)
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "session seed is not canonical", err)
	}
	session := EncoderSession{ResponseID: responseID, Model: request.Model, Seed: sessionSeed}
	if err := session.Validate(); err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "session seed is invalid", err)
	}
	var encoder Encoder
	if request.Stream {
		encoder, err = registration.codec.NewStream(session)
	} else {
		encoder, err = registration.codec.NewAggregate(session)
	}
	if err != nil || nilInterface(encoder) {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "response encoder could not start", err)
	}
	startFrames, err := encoder.Start()
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "response encoder start failed", err)
	}
	if request.Stream {
		err = registration.description.Limits.CheckStreamFrames(startFrames)
	} else {
		err = registration.description.Limits.CheckAggregateFrames(startFrames, false)
	}
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "response encoder violated its contract", err)
	}
	if err := checkPersistedFrameStep(startFrames); err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "response encoder exceeded durable step limit", err)
	}
	checkpointBytes, err := json.Marshal(encoderCheckpoint{Version: 1, Kind: "session", Session: &session})
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "session checkpoint encoding failed", err)
	}
	sealedCanonical, err := service.sealPayload(ctx, requestPayloadBinding(key, requestID), canonical)
	if err != nil {
		if errors.Is(err, errPersistedPayloadLimit) {
			return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
				Status: 413, Code: "request_storage_too_large", Message: "canonical request exceeds the persistence limit",
			}, 0, err)
		}
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "request protection failed", err)
	}
	if !storeRequestPayloadAllowed(int64(len(sealedCanonical)), 0, service.readLimitBytes) {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 413, Code: "request_storage_too_large", Message: "canonical request exceeds the persistence limit",
		}, 0, errPersistedPayloadLimit)
	}
	sequence := uint64(1)
	sealedCheckpoint, err := service.sealPayload(ctx, responseEventBinding(key, requestID, sequence), checkpointBytes)
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "checkpoint protection failed", err)
	}
	startEvents := []StoreResponseEventRecord{{
		Request: key, Sequence: sequence, Kind: StoreEventCheckpoint, Data: sealedCheckpoint,
	}}
	for _, frame := range startFrames {
		sequence++
		sealedFrame, sealErr := service.sealPayload(ctx, responseEventBinding(key, requestID, sequence), frame)
		if sealErr != nil {
			return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "response protection failed", sealErr)
		}
		startEvents = append(startEvents, StoreResponseEventRecord{
			Request: key, Sequence: sequence, Kind: StoreEventWire, Data: sealedFrame,
		})
	}
	now, err := checkedTime(service.clock)
	if err != nil {
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "clock failed", err)
	}
	for index := range startEvents {
		startEvents[index].CreatedAt = now
	}
	mode := ResponseAggregate
	decision := StoreResponseDecision{}
	boundary := AssignmentAfterAdmission
	if request.Stream {
		mode = ResponseStream
		decision = StoreResponseDecision{StatusCode: registration.success, ContentType: registration.streamType}
		boundary = AssignmentAfterResponse
	}
	delivery := WorkerAssignmentDelivery{
		ID: stableAssignmentDeliveryID(requestID, lease.ID),
		Assignment: Assignment{
			Identity: identity, Lease: lease, Boundary: boundary,
			Task:    assignmentTaskContext(input.Task, taskID, plan),
			Request: cloneTransportRequest(request),
		},
	}
	if err := delivery.ValidateFor(AuthenticatedWorker{
		WorkerID: worker, SessionID: assignmentValidationSession,
	}); err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 400, Code: "invalid_assignment", Message: "request cannot be assigned to a Human worker",
		}, 0, err)
	}
	encodedAssignment, err := json.Marshal(delivery)
	if err != nil {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 400, Code: "invalid_assignment", Message: "request cannot be encoded for a Human worker",
		}, 0, err)
	}
	if int64(len(encodedAssignment))+workerEnvelopeReserve > service.workerPayloadLimitBytes {
		return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
			Status: 413, Code: "assignment_too_large", Message: "canonical request exceeds the worker transport limit",
		}, 0, ErrWorkerDelivery)
	}
	task := StoreTaskRecord{
		Key: taskKey, WorkspaceKey: input.Task.WorkspaceKey, CapabilityTier: input.Task.CapabilityTier,
		Codec: registration.snapshot, HarnessID: input.Task.HarnessID, HarnessVersion: input.Task.HarnessVersion,
		HarnessSessionID: input.Task.HarnessSessionID, ExecAllowed: input.Task.ExecAllowed,
		State: TaskLeased, LeaseOwner: worker, LeaseID: lease.ID,
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	if plan.existing {
		task = plan.task
		task.State, task.LeaseOwner, task.LeaseID = TaskLeased, worker, lease.ID
		task.Revision++
		task.UpdatedAt = timestampAtLeast(now, plan.task.UpdatedAt)
	}
	record := StoreRequestRecord{
		Key: key, Task: taskKey, RequestID: requestID, ResponseID: responseID,
		RequestDigest: digest, Codec: registration.snapshot, Mode: mode,
		CanonicalPayload: sealedCanonical, Decision: decision, LastEventSequence: sequence,
		Revision: 1, CreatedAt: now,
	}
	for index := range plan.results {
		plan.results[index].next.Revision = plan.results[index].current.Revision + 1
		completed := timestampAtLeast(now, plan.results[index].current.CreatedAt)
		plan.results[index].next.CompletedAt = &completed
	}
	commitErr := service.store.Update(ctx, func(tx StoreTx) error {
		if _, loadErr := tx.LoadRequestHead(key); loadErr == nil {
			return &StoreConflictError{Constraint: StoreConstraintRequestKey, Key: fmt.Sprint(key)}
		} else if !errors.Is(loadErr, ErrStoreRecordNotFound) {
			return loadErr
		}
		if plan.existing {
			current, loadErr := tx.LoadTask(taskKey)
			if loadErr != nil {
				return loadErr
			}
			if current.Revision != plan.task.Revision || current.State != plan.task.State {
				return ErrTaskConflict
			}
			if plan.preempt {
				// Supersede the abandoned in-flight request so the resuming
				// request takes over: marking it ResponseComplete makes it a
				// terminal RequestSuperseded and clears active_request_per_task.
				active, findErr := tx.FindActiveRequest(taskKey)
				if findErr != nil {
					if errors.Is(findErr, ErrStoreRecordNotFound) {
						return ErrTaskConflict
					}
					return findErr
				}
				// Planning observed one exact detached request. Re-check every
				// immutable identity plus its revision inside this transaction so
				// a concurrent continuation or late cleanup cannot cause us to
				// supersede whichever request merely happens to be active now.
				if active.Key != plan.preempted.Key ||
					active.RequestID != plan.preempted.RequestID ||
					active.Revision != plan.preempted.Revision ||
					active.ResponseComplete {
					return ErrTaskConflict
				}
				stale, loadReqErr := tx.LoadRequest(active.Key, StoreReadLimit{MaxBytes: service.readLimitBytes})
				if loadReqErr != nil {
					return loadReqErr
				}
				if stale.RequestID != plan.preempted.RequestID || stale.Revision != plan.preempted.Revision || stale.ResponseComplete {
					return ErrTaskConflict
				}
				superseded := stale
				superseded.ResponseComplete = true
				supersededAt := now
				superseded.CompletedAt = &supersededAt
				superseded.Revision = stale.Revision + 1
				swapped, swapErr := tx.CompareAndSwapRequest(StoreRequestMutation{
					Key: active.Key, ExpectedRevision: stale.Revision, Next: superseded,
				})
				if swapErr != nil {
					return swapErr
				}
				if !swapped {
					return ErrTaskConflict
				}
			}
			changed, changeErr := tx.CompareAndSwapTask(StoreTaskMutation{
				Key: taskKey, ExpectedRevision: plan.task.Revision, Next: task,
			})
			if changeErr != nil {
				return changeErr
			}
			if !changed {
				return ErrTaskConflict
			}
		} else if insertErr := tx.InsertTask(task); insertErr != nil {
			return insertErr
		}
		for _, result := range plan.results {
			changed, changeErr := tx.CompareAndSwapToolExecution(StoreToolExecutionMutation{
				Key: result.current.Key, ExpectedRevision: result.current.Revision, Next: result.next,
			})
			if changeErr != nil {
				return changeErr
			}
			if !changed {
				return ErrTaskConflict
			}
		}
		if insertErr := tx.InsertRequest(record); insertErr != nil {
			return insertErr
		}
		for _, event := range startEvents {
			if insertErr := tx.InsertResponseEvent(event); insertErr != nil {
				return insertErr
			}
		}
		return nil
	})
	if commitErr != nil {
		if errors.Is(commitErr, ErrStoreCommitUnknown) {
			replay, reconcileErr := service.reconcileAdmissionCommit(ctx, key, digest)
			if reconcileErr == nil {
				return replay, nil
			}
			return AdmissionResult{}, service.internalAdmissionError(
				input.CodecID, "admission commit could not be reconciled", errors.Join(commitErr, reconcileErr),
			)
		}
		if replay, found, replayErr := service.replayAdmission(ctx, key, digest); found || replayErr != nil {
			return replay, replayErr
		}
		if errors.Is(commitErr, ErrStoreConflict) || errors.Is(commitErr, ErrTaskConflict) {
			return AdmissionResult{}, service.admissionFailure(input.CodecID, AdmissionFailure{
				Status: 409, Code: "task_conflict", Message: "task changed while the request was admitted; retry the exact request",
			}, 0, errors.Join(ErrTaskConflict, commitErr))
		}
		return AdmissionResult{}, service.internalAdmissionError(input.CodecID, "admission persistence failed", commitErr)
	}
	service.addAssignment(key, delivery, true, now)
	if plan.preempt {
		// Clear only after the replacement request is durably committed. Clearing
		// during planning makes a transient/unknown commit strand the abandoned
		// task by consuming its sole takeover authorization.
		service.clearDetached(taskKey)
	}
	page, err := service.readResponse(ctx, ResponseQuery{
		CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey,
		RequestDigest: digest, Limit: defaultResponseLimit, MaxBytes: service.readLimitBytes,
	}, false)
	if err != nil {
		return AdmissionResult{}, err
	}
	observe.Emit(service.observer, observe.Event{
		Kind: observe.KindAdmissionAdmitted, Caller: string(input.CallerID),
		Task: string(taskID), Worker: string(worker),
	})
	return AdmissionResult{Identity: identity, RequestDigest: digest, Response: page}, nil
}

func (service *Service) reconcileAdmissionCommit(
	ctx context.Context,
	key StoreRequestKey,
	digest StoreDigest,
) (AdmissionResult, error) {
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var recovery StoreRecoveryRecord
	err := service.store.View(reconcileCtx, func(view StoreView) error {
		request, loadErr := view.LoadRequest(key, StoreReadLimit{MaxBytes: service.readLimitBytes})
		if loadErr != nil {
			return loadErr
		}
		task, loadErr := view.LoadTask(request.Task)
		recovery = StoreRecoveryRecord{Request: request, Task: task}
		return loadErr
	})
	if err != nil {
		return AdmissionResult{}, err
	}
	if recovery.Request.RequestDigest != digest {
		return AdmissionResult{}, ErrIdempotencyConflict
	}
	if !recovery.Request.ResponseComplete && !recovery.Request.RecoveryQuarantined {
		if err := service.recoverRequest(reconcileCtx, recovery); err != nil {
			if quarantineErr := service.quarantineRecovery(reconcileCtx, recovery, err); quarantineErr != nil {
				return AdmissionResult{}, errors.Join(err, quarantineErr)
			}
		}
	}
	replay, found, err := service.replayAdmission(reconcileCtx, key, digest)
	if err != nil {
		return AdmissionResult{}, err
	}
	if !found {
		return AdmissionResult{}, ErrWorkerDeliveryIndeterminate
	}
	service.clearDetached(recovery.Task.Key)
	return replay, nil
}

func (service *Service) allocateID(ctx context.Context, kind IDKind) (string, error) {
	value, err := service.ids.NewID(ctx, kind)
	if err != nil {
		return "", err
	}
	if err := validGeneratedID(kind, value); err != nil {
		return "", err
	}
	return value, nil
}

func (service *Service) routeAdmission(
	ctx context.Context,
	input AdmissionRequest,
	request Request,
	taskID TaskID,
	plan admissionTaskPlan,
) (WorkerID, error) {
	if plan.existing && plan.task.LeaseOwner != "" {
		return plan.task.LeaseOwner, nil
	}
	candidates := service.connectedWorkers()
	if service.router == nil {
		switch len(candidates) {
		case 0:
			return "", ErrWorkerUnavailable
		case 1:
			return candidates[0], nil
		default:
			// A silent ErrWorkerUnavailable here would disguise a deployment
			// configuration error as a retryable capacity condition.
			return "", ErrWorkerRouterRequired
		}
	}
	worker, err := service.router.RouteWorker(ctx, WorkerRouteRequest{
		CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey,
		TaskID: taskID, Request: cloneTransportRequest(request), Task: input.Task,
		Candidates:       append([]WorkerID(nil), candidates...),
		CallerAttributes: cloneCallerAttributes(input.CallerAttributes),
	})
	if err != nil {
		return "", err
	}
	if err := validateWorkerStableKey("worker id", string(worker)); err != nil {
		if worker == "" {
			return "", ErrWorkerUnavailable
		}
		return "", fmt.Errorf("%w: router returned invalid worker: %v", ErrWorkerRouteDenied, err)
	}
	return worker, nil
}

func (service *Service) planAdmissionTask(
	ctx context.Context,
	key StoreTaskKey,
	input TaskContext,
	codec CodecSnapshot,
	request Request,
) (admissionTaskPlan, error) {
	if input.CapabilityTier == TierChat {
		return admissionTaskPlan{}, nil
	}
	var task StoreTaskRecord
	var tools []StoreToolExecutionRecord
	err := service.store.View(ctx, func(view StoreView) error {
		loaded, loadErr := view.LoadTask(key)
		if errors.Is(loadErr, ErrStoreRecordNotFound) {
			return nil
		}
		if loadErr != nil {
			return loadErr
		}
		task = loaded
		if task.State == TaskAwaitingResults {
			var after ToolCallID
			for {
				batch, scanErr := view.ScanToolExecutions(StoreToolExecutionScan{
					Task: key, After: after, Limit: maximumResponseLimit,
					ReadLimit: StoreReadLimit{MaxBytes: service.readLimitBytes},
				})
				if scanErr != nil {
					return scanErr
				}
				tools = append(tools, batch...)
				if len(batch) == 0 {
					break
				}
				after = batch[len(batch)-1].Key.ToolCallID
			}
		}
		return nil
	})
	if err != nil {
		return admissionTaskPlan{}, err
	}
	if task.Key == (StoreTaskKey{}) {
		return admissionTaskPlan{}, nil
	}
	if !sameTaskContext(task, input) || !task.Codec.Equal(codec) {
		return admissionTaskPlan{}, ErrTaskConflict
	}
	switch task.State {
	case TaskAwaitingCaller:
		return admissionTaskPlan{existing: true, task: task}, nil
	case TaskAwaitingResults:
		results := extractSubmittedToolResults(request)
		plan := admissionTaskPlan{existing: true, task: task}
		for _, execution := range tools {
			if execution.State != ToolExecutionPending {
				continue
			}
			submitted, exists := results[execution.Key.ToolCallID]
			if !exists {
				return admissionTaskPlan{}, ErrTaskConflict
			}
			sealed, sealErr := service.sealPayload(ctx, toolResultBinding(execution.Key), submitted.data)
			if sealErr != nil {
				return admissionTaskPlan{}, sealErr
			}
			next := execution
			next.State, next.Result, next.IsError = ToolExecutionCompleted, sealed, submitted.isError
			plan.results = append(plan.results, toolResultMutation{current: execution, next: next})
		}
		if len(plan.results) == 0 {
			return admissionTaskPlan{}, ErrTaskConflict
		}
		return plan, nil
	default:
		// Scenario C: a resume takes over ONLY a task whose in-flight caller has
		// detached (service.callerDetached) and whose current request is still
		// in-flight — admitted but not durably closed. This is RequestInFlight in
		// HumanLLM.tla (Admitted/Decided/Streaming): a stream commits its 200 at
		// admission to open the SSE channel, so "not closed" (an active request
		// that is not yet ResponseComplete), NOT "pre-decision", is what makes
		// takeover safe — the gone caller got a status line but no answer. A
		// terminal task or an already-closed response is never preempted (that
		// would resurrect a delivered answer). Anything else stays a hard conflict.
		if !task.State.Terminal() {
			var head StoreRequestHead
			headErr := service.store.View(ctx, func(view StoreView) error {
				loaded, loadErr := view.FindActiveRequest(key)
				head = loaded
				return loadErr
			})
			if headErr == nil && !head.ResponseComplete && service.callerDetached(key, head) {
				return admissionTaskPlan{existing: true, preempt: true, preempted: head, task: task}, nil
			}
		}
		return admissionTaskPlan{}, ErrTaskConflict
	}
}

func extractSubmittedToolResults(request Request) map[ToolCallID]submittedToolResult {
	results := make(map[ToolCallID]submittedToolResult)
	conflicted := make(map[ToolCallID]struct{})
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Type != BlockToolResult || block.ToolCallID == "" {
				continue
			}
			encoded, err := json.Marshal(block.Output)
			if err != nil {
				continue
			}
			id := ToolCallID(block.ToolCallID)
			if _, conflict := conflicted[id]; conflict {
				continue
			}
			candidate := submittedToolResult{data: encoded, isError: block.IsError}
			if existing, found := results[id]; found &&
				(!bytes.Equal(existing.data, candidate.data) || existing.isError != candidate.isError) {
				delete(results, id)
				conflicted[id] = struct{}{}
				continue
			}
			results[id] = candidate
		}
	}
	return results
}

func (service *Service) replayAdmission(
	ctx context.Context,
	key StoreRequestKey,
	digest StoreDigest,
) (AdmissionResult, bool, error) {
	var head StoreRequestHead
	err := service.store.View(ctx, func(view StoreView) error {
		loaded, loadErr := view.LoadRequestHead(key)
		head = loaded
		return loadErr
	})
	if errors.Is(err, ErrStoreRecordNotFound) {
		return AdmissionResult{}, false, nil
	}
	if err != nil {
		return AdmissionResult{}, true, err
	}
	if head.RequestDigest != digest {
		return AdmissionResult{}, true, ErrIdempotencyConflict
	}
	if head.PayloadPrunedAt != nil {
		return AdmissionResult{}, true, ErrReplayExpired
	}
	if !head.ResponseComplete && !head.RecoveryQuarantined {
		var recovery StoreRecoveryRecord
		err = service.store.View(ctx, func(view StoreView) error {
			request, loadErr := view.LoadRequest(key, StoreReadLimit{MaxBytes: service.readLimitBytes})
			if loadErr != nil {
				return loadErr
			}
			task, loadErr := view.LoadTask(request.Task)
			recovery = StoreRecoveryRecord{Request: request, Task: task}
			return loadErr
		})
		if err != nil {
			return AdmissionResult{}, true, err
		}
		if recovery.Request.RequestDigest != digest {
			return AdmissionResult{}, true, ErrIdempotencyConflict
		}
		if !recovery.Request.ResponseComplete && !recovery.Request.RecoveryQuarantined {
			if recoverErr := service.recoverRequest(ctx, recovery); recoverErr != nil {
				// A terminal worker event may have won after the snapshot. Re-read the
				// head before surfacing a stale recovery error; a stale assignment is
				// harmless because its lease is checked at ACK/CommitEvent.
				var latest StoreRequestHead
				latestErr := service.store.View(ctx, func(view StoreView) error {
					loaded, loadErr := view.LoadRequestHead(key)
					latest = loaded
					return loadErr
				})
				if latestErr != nil || (!latest.ResponseComplete && !latest.RecoveryQuarantined) {
					return AdmissionResult{}, true, errors.Join(recoverErr, latestErr)
				}
			}
		}
	}
	page, err := service.readResponse(ctx, ResponseQuery{
		CallerID: key.Caller, IdempotencyKey: key.IdempotencyKey, RequestDigest: digest,
		Limit: defaultResponseLimit, MaxBytes: service.readLimitBytes,
	}, false)
	if err != nil {
		return AdmissionResult{}, true, err
	}
	return AdmissionResult{Identity: page.Identity, RequestDigest: digest, Replay: true, Response: page}, true, nil
}

func (service *Service) addAssignment(
	key StoreRequestKey,
	delivery WorkerAssignmentDelivery,
	pending bool,
	createdAt time.Time,
) {
	state := &assignmentState{
		delivery: CloneWorkerAssignmentDelivery(delivery), request: key,
		pending: pending, createdAt: createdAt,
	}
	service.mu.Lock()
	if _, exists := service.assignments[delivery.ID]; exists {
		service.mu.Unlock()
		return
	}
	service.assignments[delivery.ID] = state
	if pending {
		byWorker := service.pending[delivery.Assignment.Lease.Owner]
		if byWorker == nil {
			byWorker = make(map[WorkerDeliveryID]*assignmentState)
			service.pending[delivery.Assignment.Lease.Owner] = byWorker
		}
		byWorker[delivery.ID] = state
	}
	connection := service.connections[delivery.Assignment.Lease.Owner]
	service.mu.Unlock()
	if connection != nil {
		connection.signal()
	}
}

func (service *Service) admissionFailure(
	codecID CodecID,
	failure AdmissionFailure,
	retryAfter time.Duration,
	cause error,
) error {
	registration, exists := service.codecs[codecID]
	if !exists {
		return cause
	}
	if err := failure.Validate(); err != nil {
		return errors.Join(cause, err)
	}
	body, err := registration.codec.AdmissionError(failure)
	if err != nil {
		return errors.Join(cause, err)
	}
	if err := registration.description.Limits.CheckAdmissionError(body); err != nil {
		return errors.Join(cause, err)
	}
	observe.Emit(service.observer, observe.Event{
		Kind: observe.KindAdmissionRejected, Detail: failure.Code, Err: cause,
	})
	return &AdmissionError{
		Failure: failure, ContentType: registration.aggregateType,
		RetryAfter: parseRetryAfter(retryAfter), Body: append([]byte(nil), body...), Cause: cause,
	}
}

func (service *Service) internalAdmissionError(codec CodecID, detail string, cause error) error {
	return service.admissionFailure(codec, AdmissionFailure{
		Status: 500, Code: "internal_error", Message: "HumanLLM could not admit the request",
	}, 0, fmt.Errorf("%s: %w", detail, cause))
}
