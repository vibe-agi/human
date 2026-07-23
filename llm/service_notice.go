package llm

import "context"

type detachedCaller struct {
	request   StoreRequestKey
	requestID string
}

// DetachCaller records that the in-flight request holding a task has lost its
// caller: the request socket went away before a durable completion. This is the
// single caller-liveness signal behind both availability scenarios, unifying
// what were two half-features:
//
//   - Scenario C (takeover): it gates AdmitPreemptDetached — a resuming
//     completion preempts the task ONLY because its prior caller is gone, never
//     a live caller or a transport retry. This mirrors the formal callerDetached
//     precondition in HumanLLM.tla.
//   - Scenario B (banner): it raises the human-facing "caller disconnected"
//     notice so the operator does not reply into the void.
//
// It is advisory in-memory state: a lost mark is safe because a restart drops
// every caller socket and the next completion is treated as a fresh top-level
// turn. It is cleared when the task is reconciled (taken over or advanced).
func (service *Service) DetachCaller(ctx context.Context, identity CompletionIdentity) {
	end, err := service.beginOperation()
	if err != nil {
		return
	}
	defer end()
	if ctx == nil || ctx.Err() != nil {
		return
	}
	task := StoreTaskKey{Caller: identity.CallerID, Task: identity.TaskID}
	request := StoreRequestKey{Caller: identity.CallerID, IdempotencyKey: identity.IdempotencyKey}
	if task.Caller == "" || task.Task == "" || request.IdempotencyKey == "" || identity.RequestID == "" {
		return
	}
	// Bind the advisory mark to the exact active request. An old canceled HTTP
	// handler may finish after a retry or continuation has already installed
	// newer work on this task; that stale cleanup must never authorize takeover.
	var active StoreRequestHead
	if err := service.store.View(ctx, func(view StoreView) error {
		loaded, loadErr := view.FindActiveRequest(task)
		active = loaded
		return loadErr
	}); err != nil || active.Key != request || active.RequestID != identity.RequestID || active.ResponseComplete {
		return
	}
	service.mu.Lock()
	service.detached[task] = detachedCaller{request: request, requestID: identity.RequestID}
	service.mu.Unlock()
	service.raiseCallerGoneNotice(ctx, task, identity.RequestID)
}

// callerDetached reports whether the exact active request for the task has
// detached. A task-only bit is insufficient because transport cleanup is
// asynchronous with respect to admission.
func (service *Service) callerDetached(task StoreTaskKey, active StoreRequestHead) bool {
	service.mu.Lock()
	detached, ok := service.detached[task]
	service.mu.Unlock()
	return ok && detached.request == active.Key && detached.requestID == active.RequestID
}

// clearDetached drops a task's detachment mark once it has been reconciled, so
// the entry does not leak and cannot cause a spurious later preemption.
func (service *Service) clearDetached(task StoreTaskKey) {
	service.mu.Lock()
	delete(service.detached, task)
	service.mu.Unlock()
}

// raiseCallerGoneNotice surfaces the human-facing alert (scenario B): it finds
// the worker holding the task and emits a caller-gone WorkerNotice. The banner
// text is the basic implementation's default; the WorkerNoticer/NoticeSource
// ports let an embedder surface it differently.
func (service *Service) raiseCallerGoneNotice(ctx context.Context, task StoreTaskKey, requestID string) {
	if ctx == nil {
		return
	}
	var owner WorkerID
	if err := service.store.View(ctx, func(view StoreView) error {
		loaded, loadErr := view.LoadTask(task)
		if loadErr != nil {
			return loadErr
		}
		owner = loaded.LeaseOwner
		return nil
	}); err != nil || owner == "" {
		return
	}
	service.mu.Lock()
	connection := service.connections[owner]
	service.mu.Unlock()
	if connection != nil {
		connection.emitNotice(WorkerNotice{
			Code:      "caller_gone",
			Message:   "The caller disconnected before this turn finished; they may not return. You can end the conversation.",
			Caller:    task.Caller,
			TaskID:    task.Task,
			RequestID: requestID,
		})
	}
}
