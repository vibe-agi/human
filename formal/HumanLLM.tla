------------------------------ MODULE HumanLLM ------------------------------
(***************************************************************************)
(* HumanLLM is the realtime model-provider surface.  A logical task may own *)
(* several completion requests, but every request owns one independently    *)
(* durable HTTP/stream response.  tool_calls closes that response; caller    *)
(* results arrive only on a later completion of the same logical task.      *)
(***************************************************************************)
EXTENDS HumanCommon, TLC

CONSTANTS Callers, Workspaces, Tasks, Requests, Digests,
          MaxVersion, MaxCursor, MaxClarifications, CallerAvailable

ASSUME /\ Callers # {}
       /\ Workspaces # {}
       /\ Tasks # {}
       /\ Requests # {}
       /\ Digests # {}
       /\ MaxVersion \in Nat \ {0}
       /\ MaxCursor \in Nat \ {0, 1}
       /\ MaxClarifications \in Nat
       /\ CallerAvailable \in BOOLEAN

Versions == 1..MaxVersion

TaskUnused          == "task_unused"
TaskActive          == "task_active"
TaskAwaitingResults == "task_awaiting_results"
TaskAwaitingCaller  == "task_awaiting_caller"
TaskReconciled      == "task_reconciled"
TaskCompleted       == "task_completed"
TaskFailed          == "task_failed"
TaskStates == {
  TaskUnused, TaskActive, TaskAwaitingResults, TaskAwaitingCaller,
  TaskReconciled, TaskCompleted, TaskFailed
}
TaskTerminalStates == {TaskCompleted, TaskFailed}

RequestUnused    == "request_unused"
RequestAdmitted  == "request_admitted"
RequestDecided   == "request_decided"
RequestStreaming == "request_streaming"
RequestClosed    == "request_closed"
RequestSuperseded == "request_superseded"
RequestStates == {
  RequestUnused, RequestAdmitted, RequestDecided,
  RequestStreaming, RequestClosed, RequestSuperseded
}

DecisionNone     == "decision_none"
DecisionOK       == "decision_200"
DecisionConflict == "decision_409"
Decisions == {DecisionNone, DecisionOK, DecisionConflict}

NoTerminalKind == "no_terminal"
ResponseKinds == {NoTerminalKind} \cup LLMResponseTerminalStates
FrameKinds == {"progress"} \union LLMResponseTerminalStates
BoundedTraces ==
  UNION {[1..length -> FrameKinds] : length \in 0..MaxCursor}

ResultMissing == "result_missing"
ResultSuccess == "result_success"
ResultFailure == "result_failure"
ResultStates == {ResultMissing, ResultSuccess, ResultFailure}
ResultMaps == [Versions -> ResultStates]

SubmitNone     == "submit_none"
SubmitAdmitted == "submit_admitted"
SubmitReplay   == "submit_replay"
SubmitConflict == "submit_conflict"
SubmitDispositions == {
  SubmitNone, SubmitAdmitted, SubmitReplay, SubmitConflict
}

DefaultCaller    == CHOOSE caller \in Callers : TRUE
DefaultWorkspace == CHOOSE workspace \in Workspaces : TRUE
DefaultTask      == CHOOSE task \in Tasks : TRUE
DefaultRequest   == CHOOSE request \in Requests : TRUE
DefaultDigest    == CHOOSE digest \in Digests : TRUE
MissingResults   == [version \in Versions |-> ResultMissing]

NoSubmission == [
  request     |-> DefaultRequest,
  digest      |-> DefaultDigest,
  disposition |-> SubmitNone,
  decision    |-> DecisionNone,
  cursor      |-> 0,
  terminal    |-> FALSE,
  trace       |-> <<>>
]

VARIABLES
  taskStatus,
  taskCaller,
  taskWorkspace,
  taskRequests,
  taskCurrentRequest,
  issuedVersions,
  pendingVersions,
  successfulVersions,
  failedVersions,
  baselineVersion,
  clarificationCount,
  terminalTasks,
  terminalRequestCount,
  requestStatus,
  requestTask,
  requestCaller,
  requestWorkspace,
  requestDigest,
  requestResults,
  httpDecision,
  streamCursor,
  streamTerminal,
  closedCursor,
  responseKind,
  responseTrace,
  callerDetached,
  lastSubmission

TaskScope(task) == <<taskCaller[task], taskWorkspace[task]>>
RequestScope(request) == <<requestCaller[request], requestWorkspace[request]>>
TaskKeyID(task) == <<"task", task>>
RequestKeyID(request) == <<"request", request>>
TaskKey(task) == ScopedKey(LLMKind, TaskScope(task), TaskKeyID(task))
RequestKey(request) ==
  ScopedKey(LLMKind, RequestScope(request), RequestKeyID(request))
AllScopes == Callers \X Workspaces
AllTaskKeyIDs == {TaskKeyID(task) : task \in Tasks}
AllRequestKeyIDs == {RequestKeyID(request) : request \in Requests}

MaxOf(versions) ==
  IF versions = {}
  THEN 0
  ELSE CHOOSE version \in versions :
         \A other \in versions : other <= version

durable == <<
  taskStatus, taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,
  issuedVersions, pendingVersions, successfulVersions, failedVersions,
  baselineVersion, clarificationCount, terminalTasks, terminalRequestCount,
  requestStatus, requestTask, requestCaller, requestWorkspace, requestDigest,
  requestResults, httpDecision, streamCursor, streamTerminal, closedCursor,
  responseKind, responseTrace, callerDetached
>>

vars == <<durable, lastSubmission>>

Init ==
  /\ taskStatus = [task \in Tasks |-> TaskUnused]
  /\ taskCaller = [task \in Tasks |-> DefaultCaller]
  /\ taskWorkspace = [task \in Tasks |-> DefaultWorkspace]
  /\ taskRequests = [task \in Tasks |-> {}]
  /\ taskCurrentRequest = [task \in Tasks |-> DefaultRequest]
  /\ issuedVersions = [task \in Tasks |-> {}]
  /\ pendingVersions = [task \in Tasks |-> {}]
  /\ successfulVersions = [task \in Tasks |-> {}]
  /\ failedVersions = [task \in Tasks |-> {}]
  /\ baselineVersion = [task \in Tasks |-> 0]
  /\ clarificationCount = [task \in Tasks |-> 0]
  /\ terminalTasks = {}
  /\ terminalRequestCount = [task \in Tasks |-> 0]
  /\ requestStatus = [request \in Requests |-> RequestUnused]
  /\ requestTask = [request \in Requests |-> DefaultTask]
  /\ requestCaller = [request \in Requests |-> DefaultCaller]
  /\ requestWorkspace = [request \in Requests |-> DefaultWorkspace]
  /\ requestDigest = [request \in Requests |-> DefaultDigest]
  /\ requestResults = [request \in Requests |-> MissingResults]
  /\ httpDecision = [request \in Requests |-> DecisionNone]
  /\ streamCursor = [request \in Requests |-> 0]
  /\ streamTerminal = [request \in Requests |-> FALSE]
  /\ closedCursor = [request \in Requests |-> 0]
  /\ responseKind = [request \in Requests |-> NoTerminalKind]
  /\ responseTrace = [request \in Requests |-> <<>>]
  /\ callerDetached = [request \in Requests |-> FALSE]
  /\ lastSubmission = NoSubmission

ObserveAdmission(request, digest) == [
  request     |-> request,
  digest      |-> digest,
  disposition |-> SubmitAdmitted,
  decision    |-> DecisionNone,
  cursor      |-> 0,
  terminal    |-> FALSE,
  trace       |-> <<>>
]

AdmitFresh(caller, workspace, task, request, digest) ==
  /\ requestStatus[request] = RequestUnused
  /\ taskStatus[task] = TaskUnused
  /\ requestStatus' = [requestStatus EXCEPT ![request] = RequestAdmitted]
  /\ requestTask' = [requestTask EXCEPT ![request] = task]
  /\ requestCaller' = [requestCaller EXCEPT ![request] = caller]
  /\ requestWorkspace' = [requestWorkspace EXCEPT ![request] = workspace]
  /\ requestDigest' = [requestDigest EXCEPT ![request] = digest]
  /\ taskStatus' = [taskStatus EXCEPT ![task] = TaskActive]
  /\ taskCaller' = [taskCaller EXCEPT ![task] = caller]
  /\ taskWorkspace' = [taskWorkspace EXCEPT ![task] = workspace]
  /\ taskRequests' = [taskRequests EXCEPT ![task] = {request}]
  /\ taskCurrentRequest' = [taskCurrentRequest EXCEPT ![task] = request]
  /\ lastSubmission' = ObserveAdmission(request, digest)
  /\ UNCHANGED <<
       issuedVersions, pendingVersions, successfulVersions, failedVersions,
       baselineVersion, clarificationCount, terminalTasks, terminalRequestCount,
       requestResults, httpDecision, streamCursor, streamTerminal,
       closedCursor, responseKind, responseTrace, callerDetached
     >>

Replay(request, digest) ==
  /\ requestStatus[request] # RequestUnused
  /\ requestDigest[request] = digest
  /\ lastSubmission' = [
       request     |-> request,
       digest      |-> digest,
       disposition |-> SubmitReplay,
       decision    |-> httpDecision[request],
       cursor      |-> streamCursor[request],
       terminal    |-> streamTerminal[request],
       trace       |-> responseTrace[request]
     ]
  /\ UNCHANGED durable

RejectDigestConflict(request, digest) ==
  /\ requestStatus[request] # RequestUnused
  /\ requestDigest[request] # digest
  /\ lastSubmission' = [
       request     |-> request,
       digest      |-> digest,
       disposition |-> SubmitConflict,
       decision    |-> DecisionConflict,
       cursor      |-> 0,
       terminal    |-> TRUE,
       trace       |-> <<>>
     ]
  /\ UNCHANGED durable

ValidResultMap(task, results) ==
  /\ results \in ResultMaps
  /\ \A version \in pendingVersions[task] :
       results[version] \in {ResultSuccess, ResultFailure}
  /\ \A version \in Versions \ pendingVersions[task] :
       results[version] = ResultMissing

AdmitResultTurn(caller, workspace, task, request, digest, results) ==
  /\ CallerAvailable
  /\ taskStatus[task] = TaskAwaitingResults
  /\ taskCaller[task] = caller
  /\ taskWorkspace[task] = workspace
  /\ requestStatus[taskCurrentRequest[task]] = RequestClosed
  /\ requestStatus[request] = RequestUnused
  /\ ValidResultMap(task, results)
  /\ requestStatus' = [requestStatus EXCEPT ![request] = RequestAdmitted]
  /\ requestTask' = [requestTask EXCEPT ![request] = task]
  /\ requestCaller' = [requestCaller EXCEPT ![request] = caller]
  /\ requestWorkspace' = [requestWorkspace EXCEPT ![request] = workspace]
  /\ requestDigest' = [requestDigest EXCEPT ![request] = digest]
  /\ requestResults' = [requestResults EXCEPT ![request] = results]
  /\ taskRequests' = [taskRequests EXCEPT ![task] = @ \cup {request}]
  /\ taskCurrentRequest' = [taskCurrentRequest EXCEPT ![task] = request]
  /\ lastSubmission' = ObserveAdmission(request, digest)
  /\ UNCHANGED <<
       taskStatus, taskCaller, taskWorkspace,
       issuedVersions, pendingVersions, successfulVersions, failedVersions,
       baselineVersion, clarificationCount, terminalTasks, terminalRequestCount,
       httpDecision, streamCursor, streamTerminal, closedCursor, responseKind,
       responseTrace, callerDetached
     >>

(* A clarification closes one model response but keeps the logical task     *)
(* alive. The caller's follow-up is a new completion on that same task.     *)
AdmitCallerTurn(caller, workspace, task, request, digest) ==
  /\ CallerAvailable
  /\ taskStatus[task] = TaskAwaitingCaller
  /\ taskCaller[task] = caller
  /\ taskWorkspace[task] = workspace
  /\ requestStatus[taskCurrentRequest[task]] = RequestClosed
  /\ requestStatus[request] = RequestUnused
  /\ requestStatus' = [requestStatus EXCEPT ![request] = RequestAdmitted]
  /\ requestTask' = [requestTask EXCEPT ![request] = task]
  /\ requestCaller' = [requestCaller EXCEPT ![request] = caller]
  /\ requestWorkspace' = [requestWorkspace EXCEPT ![request] = workspace]
  /\ requestDigest' = [requestDigest EXCEPT ![request] = digest]
  /\ taskRequests' = [taskRequests EXCEPT ![task] = @ \cup {request}]
  /\ taskCurrentRequest' = [taskCurrentRequest EXCEPT ![task] = request]
  /\ taskStatus' = [taskStatus EXCEPT ![task] = TaskActive]
  /\ lastSubmission' = ObserveAdmission(request, digest)
  /\ UNCHANGED <<
       taskCaller, taskWorkspace,
       issuedVersions, pendingVersions, successfulVersions, failedVersions,
       baselineVersion, clarificationCount, terminalTasks, terminalRequestCount,
       requestResults, httpDecision, streamCursor, streamTerminal,
       closedCursor, responseKind, responseTrace, callerDetached
     >>

(* A request is in-flight while it holds a caller but has not durably closed.   *)
(* A stream commits its HTTP 200 at admission to open the SSE channel, so a      *)
(* request parked awaiting the human is RequestDecided/RequestStreaming, not     *)
(* RequestAdmitted — the caller has a status line but no answer yet. Caller      *)
(* detach and detached-preemption both key off this in-flight window, not the   *)
(* pre-decision instant, so a resume can take over a parked stream too.         *)
RequestInFlight(request) ==
  requestStatus[request] \in {RequestAdmitted, RequestDecided, RequestStreaming}

(* The in-flight request's caller socket went away before its response closed.  *)
(* This advisory liveness fact is what gates AdmitPreemptDetached: a resume     *)
(* takes over only a genuinely abandoned request, never a live caller or a      *)
(* transport retry (which keeps callerDetached FALSE and is reconciled via      *)
(* Replay). It can be observed anywhere in the in-flight window — including a    *)
(* parked stream that has already sent its 200 — but never once the response    *)
(* has durably closed.                                                          *)
DetachCaller(request) ==
  /\ RequestInFlight(request)
  /\ ~callerDetached[request]
  /\ callerDetached' = [callerDetached EXCEPT ![request] = TRUE]
  /\ UNCHANGED <<
       taskStatus, taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,
       issuedVersions, pendingVersions, successfulVersions, failedVersions,
       baselineVersion, clarificationCount, terminalTasks, terminalRequestCount,
       requestStatus, requestTask, requestCaller, requestWorkspace, requestDigest,
       requestResults, httpDecision, streamCursor, streamTerminal, closedCursor,
       responseKind, responseTrace, lastSubmission
     >>

(* A resuming completion preempts the in-flight request ONLY when that         *)
(* request's caller has detached (callerDetached). A live caller or a          *)
(* transport retry is never preempted — the detach fact, not a digest guess,   *)
(* is the precondition. The superseded request may be anywhere in its in-flight *)
(* window (a parked stream that already sent its 200 included); it is superseded*)
(* and the task taken over, so a gone caller never blocks the resume (charter,  *)
(* C). It is never a closed response: that would resurrect a delivered answer.  *)
AdmitPreemptDetached(caller, workspace, task, request, digest) ==
  /\ taskStatus[task] = TaskActive
  /\ taskCaller[task] = caller
  /\ taskWorkspace[task] = workspace
  /\ RequestInFlight(taskCurrentRequest[task])
  /\ callerDetached[taskCurrentRequest[task]]
  /\ taskCurrentRequest[task] # request
  /\ requestStatus[request] = RequestUnused
  /\ requestStatus' = [requestStatus EXCEPT
       ![taskCurrentRequest[task]] = RequestSuperseded, ![request] = RequestAdmitted]
  /\ requestTask' = [requestTask EXCEPT ![request] = task]
  /\ requestCaller' = [requestCaller EXCEPT ![request] = caller]
  /\ requestWorkspace' = [requestWorkspace EXCEPT ![request] = workspace]
  /\ requestDigest' = [requestDigest EXCEPT ![request] = digest]
  /\ taskRequests' = [taskRequests EXCEPT ![task] = @ \cup {request}]
  /\ taskCurrentRequest' = [taskCurrentRequest EXCEPT ![task] = request]
  /\ lastSubmission' = ObserveAdmission(request, digest)
  /\ UNCHANGED <<
       taskStatus, taskCaller, taskWorkspace,
       issuedVersions, pendingVersions, successfulVersions, failedVersions,
       baselineVersion, clarificationCount, terminalTasks, terminalRequestCount,
       requestResults, httpDecision, streamCursor, streamTerminal,
       closedCursor, responseKind, responseTrace, callerDetached
     >>

ReconcileResult(task, request, version) ==
  /\ taskStatus[task] = TaskAwaitingResults
  /\ taskCurrentRequest[task] = request
  /\ requestStatus[request] = RequestAdmitted
  /\ version \in pendingVersions[task]
  /\ requestResults[request][version] \in {ResultSuccess, ResultFailure}
  /\ pendingVersions' =
       [pendingVersions EXCEPT ![task] = @ \ {version}]
  /\ successfulVersions' =
       IF requestResults[request][version] = ResultSuccess
       THEN [successfulVersions EXCEPT ![task] = @ \cup {version}]
       ELSE successfulVersions
  /\ failedVersions' =
       IF requestResults[request][version] = ResultFailure
       THEN [failedVersions EXCEPT ![task] = @ \cup {version}]
       ELSE failedVersions
  /\ baselineVersion' =
       IF requestResults[request][version] = ResultSuccess
       THEN [baselineVersion EXCEPT ![task] = MaxOf(successfulVersions'[task])]
       ELSE baselineVersion
  /\ taskStatus' =
       IF pendingVersions'[task] = {}
       THEN [taskStatus EXCEPT ![task] = TaskReconciled]
       ELSE taskStatus
  /\ lastSubmission' = NoSubmission
  /\ UNCHANGED <<
       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,
       issuedVersions, clarificationCount, terminalTasks, terminalRequestCount,
       requestStatus, requestTask, requestCaller, requestWorkspace,
       requestDigest, requestResults, httpDecision, streamCursor,
       streamTerminal, closedCursor, responseKind, responseTrace, callerDetached
     >>

DecideHTTP(request) ==
  LET task == requestTask[request] IN
  /\ requestStatus[request] = RequestAdmitted
  /\ taskCurrentRequest[task] = request
  /\ taskStatus[task] \in {TaskActive, TaskReconciled}
  /\ pendingVersions[task] = {}
  /\ httpDecision[request] = DecisionNone
  /\ httpDecision' = [httpDecision EXCEPT ![request] = DecisionOK]
  /\ requestStatus' = [requestStatus EXCEPT ![request] = RequestDecided]
  /\ taskStatus' = [taskStatus EXCEPT ![task] = TaskActive]
  /\ lastSubmission' = NoSubmission
  /\ UNCHANGED <<
       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,
       issuedVersions, pendingVersions, successfulVersions, failedVersions,
       baselineVersion, clarificationCount, terminalTasks, terminalRequestCount,
       requestTask, requestCaller, requestWorkspace, requestDigest,
       requestResults, streamCursor, streamTerminal, closedCursor,
       responseKind, responseTrace, callerDetached
     >>

StartStream(request) ==
  /\ requestStatus[request] = RequestDecided
  /\ httpDecision[request] = DecisionOK
  /\ requestStatus' = [requestStatus EXCEPT ![request] = RequestStreaming]
  /\ lastSubmission' = NoSubmission
  /\ UNCHANGED <<
       taskStatus, taskCaller, taskWorkspace, taskRequests,
       taskCurrentRequest, issuedVersions, pendingVersions,
       successfulVersions, failedVersions, baselineVersion, clarificationCount, terminalTasks,
       terminalRequestCount, requestTask, requestCaller, requestWorkspace,
       requestDigest, requestResults, httpDecision, streamCursor,
       streamTerminal, closedCursor, responseKind, responseTrace, callerDetached
     >>

EmitProgress(request) ==
  /\ requestStatus[request] = RequestStreaming
  /\ ~streamTerminal[request]
  /\ streamCursor[request] + 1 < MaxCursor
  /\ streamCursor' = [streamCursor EXCEPT ![request] = @ + 1]
  /\ responseTrace' =
       [responseTrace EXCEPT ![request] = Append(@, "progress")]
  /\ lastSubmission' = NoSubmission
  /\ UNCHANGED <<
       taskStatus, taskCaller, taskWorkspace, taskRequests,
       taskCurrentRequest, issuedVersions, pendingVersions,
       successfulVersions, failedVersions, baselineVersion, clarificationCount, terminalTasks,
       terminalRequestCount, requestStatus, requestTask, requestCaller,
       requestWorkspace, requestDigest, requestResults, httpDecision,
       streamTerminal, closedCursor, responseKind, callerDetached
     >>

CloseResponse(request, kind) ==
  /\ requestStatus' = [requestStatus EXCEPT ![request] = RequestClosed]
  /\ streamCursor' = [streamCursor EXCEPT ![request] = @ + 1]
  /\ streamTerminal' = [streamTerminal EXCEPT ![request] = TRUE]
  /\ closedCursor' = [closedCursor EXCEPT ![request] = streamCursor'[request]]
  /\ responseKind' = [responseKind EXCEPT ![request] = kind]
  /\ responseTrace' = [responseTrace EXCEPT ![request] = Append(@, kind)]

FinalText(request) ==
  LET task == requestTask[request] IN
  /\ requestStatus[request] = RequestStreaming
  /\ taskStatus[task] = TaskActive
  /\ taskCurrentRequest[task] = request
  /\ ~streamTerminal[request]
  /\ streamCursor[request] < MaxCursor
  /\ CloseResponse(request, "text_final")
  /\ taskStatus' = [taskStatus EXCEPT ![task] = TaskCompleted]
  /\ terminalTasks' = terminalTasks \cup {task}
  /\ terminalRequestCount' =
       [terminalRequestCount EXCEPT ![task] = Cardinality(taskRequests[task])]
  /\ lastSubmission' = NoSubmission
  /\ UNCHANGED <<
       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,
       issuedVersions, pendingVersions, successfulVersions, failedVersions,
       baselineVersion, clarificationCount, requestTask, requestCaller, requestWorkspace,
       requestDigest, requestResults, httpDecision, callerDetached
     >>

DispatchToolCalls(request, versions) ==
  LET task == requestTask[request] IN
  /\ requestStatus[request] = RequestStreaming
  /\ taskStatus[task] = TaskActive
  /\ taskCurrentRequest[task] = request
  /\ ~streamTerminal[request]
  /\ streamCursor[request] < MaxCursor
  /\ versions \in SUBSET (Versions \ issuedVersions[task])
  /\ versions # {}
  /\ pendingVersions[task] = {}
  /\ CloseResponse(request, "tool_calls")
  /\ issuedVersions' =
       [issuedVersions EXCEPT ![task] = @ \cup versions]
  /\ pendingVersions' = [pendingVersions EXCEPT ![task] = versions]
  /\ taskStatus' = [taskStatus EXCEPT ![task] = TaskAwaitingResults]
  /\ lastSubmission' = NoSubmission
  /\ UNCHANGED <<
       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,
       successfulVersions, failedVersions, baselineVersion, clarificationCount, terminalTasks,
       terminalRequestCount, requestTask, requestCaller, requestWorkspace,
       requestDigest, requestResults, httpDecision, callerDetached
     >>

Clarify(request) ==
  LET task == requestTask[request] IN
  /\ requestStatus[request] = RequestStreaming
  /\ taskStatus[task] = TaskActive
  /\ taskCurrentRequest[task] = request
  /\ clarificationCount[task] < MaxClarifications
  /\ ~streamTerminal[request]
  /\ streamCursor[request] < MaxCursor
  /\ CloseResponse(request, "clarification")
  /\ taskStatus' = [taskStatus EXCEPT ![task] = TaskAwaitingCaller]
  /\ clarificationCount' = [clarificationCount EXCEPT ![task] = @ + 1]
  /\ lastSubmission' = NoSubmission
  /\ UNCHANGED <<
       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,
       issuedVersions, pendingVersions, successfulVersions, failedVersions,
       baselineVersion, terminalTasks, terminalRequestCount,
       requestTask, requestCaller, requestWorkspace,
       requestDigest, requestResults, httpDecision, callerDetached
     >>

FailResponse(request) ==
  LET task == requestTask[request] IN
  /\ requestStatus[request] = RequestStreaming
  /\ taskStatus[task] = TaskActive
  /\ taskCurrentRequest[task] = request
  /\ ~streamTerminal[request]
  /\ streamCursor[request] < MaxCursor
  /\ CloseResponse(request, "error")
  /\ taskStatus' = [taskStatus EXCEPT ![task] = TaskFailed]
  /\ terminalTasks' = terminalTasks \cup {task}
  /\ terminalRequestCount' =
       [terminalRequestCount EXCEPT ![task] = Cardinality(taskRequests[task])]
  /\ lastSubmission' = NoSubmission
  /\ UNCHANGED <<
       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,
       issuedVersions, pendingVersions, successfulVersions, failedVersions,
       baselineVersion, clarificationCount,
       requestTask, requestCaller, requestWorkspace,
       requestDigest, requestResults, httpDecision, callerDetached
     >>

AdmitFreshSome ==
  \E caller \in Callers, workspace \in Workspaces,
     task \in Tasks, request \in Requests, digest \in Digests :
    AdmitFresh(caller, workspace, task, request, digest)

ReplaySome ==
  \E request \in Requests, digest \in Digests : Replay(request, digest)

RejectDigestConflictSome ==
  \E request \in Requests, digest \in Digests :
    RejectDigestConflict(request, digest)

AdmitResultTurnSome ==
  \E caller \in Callers, workspace \in Workspaces,
     task \in Tasks, request \in Requests, digest \in Digests,
     results \in ResultMaps :
    AdmitResultTurn(caller, workspace, task, request, digest, results)

AdmitCallerTurnSome ==
  \E caller \in Callers, workspace \in Workspaces,
     task \in Tasks, request \in Requests, digest \in Digests :
    AdmitCallerTurn(caller, workspace, task, request, digest)

AdmitPreemptDetachedSome ==
  \E caller \in Callers, workspace \in Workspaces,
     task \in Tasks, request \in Requests, digest \in Digests :
    AdmitPreemptDetached(caller, workspace, task, request, digest)

DetachCallerSome == \E request \in Requests : DetachCaller(request)

ReconcileResultSome ==
  \E task \in Tasks, request \in Requests, version \in Versions :
    ReconcileResult(task, request, version)

DecideHTTPSome == \E request \in Requests : DecideHTTP(request)
StartStreamSome == \E request \in Requests : StartStream(request)
EmitProgressSome == \E request \in Requests : EmitProgress(request)

HumanResponseSome ==
  \/ \E request \in Requests : FinalText(request)
  \/ \E request \in Requests, versions \in SUBSET Versions :
       DispatchToolCalls(request, versions)
  \/ \E request \in Requests : Clarify(request)
  \/ \E request \in Requests : FailResponse(request)

(* A bounded model eventually exhausts its finite Request identifiers.  An *)
(* awaiting-caller/result Task is then an explicit environment boundary,   *)
(* not an internal protocol deadlock.  The production protocol obtains a   *)
(* fresh request id from the caller; CallerAvailable states whether that    *)
(* environment step is part of this model run.                              *)
ProtocolQuiescent ==
  /\ \A request \in Requests :
       requestStatus[request] \in {RequestUnused, RequestClosed, RequestSuperseded}
  /\ \A task \in Tasks :
       \/ taskStatus[task] \in {TaskUnused} \union TaskTerminalStates
       \/ /\ taskStatus[task] \in {TaskAwaitingResults, TaskAwaitingCaller}
          /\ (\lnot CallerAvailable \/
                (Requests \ taskRequests[task]) = {})

Terminating == ProtocolQuiescent /\ UNCHANGED vars

(* A state constraint for configs that verify the non-preemption liveness      *)
(* envelope: with no caller ever detaching, AdmitPreemptDetached stays          *)
(* disabled, so the base config checks ordinary task termination without        *)
(* preemption consuming the bounded request pool. The preempt config omits it   *)
(* to exercise takeover (and correspondingly omits TasksEventuallyTerminal,     *)
(* since a preempt legitimately spends a request id — an environment boundary   *)
(* ProtocolQuiescent already admits, not a protocol deadlock).                  *)
NoDetach == \A request \in Requests : ~callerDetached[request]

Next ==
  \/ AdmitFreshSome
  \/ ReplaySome
  \/ RejectDigestConflictSome
  \/ AdmitResultTurnSome
  \/ AdmitCallerTurnSome
  \/ AdmitPreemptDetachedSome
  \/ DetachCallerSome
  \/ ReconcileResultSome
  \/ DecideHTTPSome
  \/ StartStreamSome
  \/ EmitProgressSome
  \/ HumanResponseSome

FullNext == Next \/ Terminating

SafetySpec == Init /\ [][FullNext]_vars

LivenessSpec ==
  /\ SafetySpec
  /\ WF_vars(AdmitResultTurnSome)
  /\ WF_vars(AdmitCallerTurnSome)
  /\ WF_vars(ReconcileResultSome)
  /\ WF_vars(AdmitPreemptDetachedSome)
  /\ WF_vars(DecideHTTPSome)
  /\ WF_vars(StartStreamSome)
  /\ WF_vars(EmitProgressSome)
  /\ WF_vars(HumanResponseSome)

(* Forced realtime UX harness: one response must carry two independently   *)
(* ordered Human progress segments before its final text. The general model*)
(* permits any finite number bounded by MaxCursor; this harness makes the    *)
(* continuous-stream path non-vacuous in the checked state graph.            *)
ProgressAdmit ==
  AdmitFresh(DefaultCaller, DefaultWorkspace, DefaultTask,
             DefaultRequest, DefaultDigest)
ProgressDecide == DecideHTTP(DefaultRequest)
ProgressStart == StartStream(DefaultRequest)
ProgressEmit == EmitProgress(DefaultRequest)
ProgressFinal ==
  /\ streamCursor[DefaultRequest] = MaxCursor - 1
  /\ FinalText(DefaultRequest)
ProgressDone ==
  /\ taskStatus[DefaultTask] = TaskCompleted
  /\ responseTrace[DefaultRequest] =
       <<"progress", "progress", "text_final">>
ProgressTerminating == ProgressDone /\ UNCHANGED vars

ProgressNext ==
  \/ ProgressAdmit
  \/ ProgressDecide
  \/ ProgressStart
  \/ ProgressEmit
  \/ ProgressFinal
  \/ ProgressTerminating

ProgressSpec ==
  /\ Init
  /\ [][ProgressNext]_vars
  /\ WF_vars(ProgressAdmit)
  /\ WF_vars(ProgressDecide)
  /\ WF_vars(ProgressStart)
  /\ WF_vars(ProgressEmit)
  /\ WF_vars(ProgressFinal)

TypeOK ==
  /\ taskStatus \in [Tasks -> TaskStates]
  /\ taskCaller \in [Tasks -> Callers]
  /\ taskWorkspace \in [Tasks -> Workspaces]
  /\ taskRequests \in [Tasks -> SUBSET Requests]
  /\ taskCurrentRequest \in [Tasks -> Requests]
  /\ issuedVersions \in [Tasks -> SUBSET Versions]
  /\ pendingVersions \in [Tasks -> SUBSET Versions]
  /\ successfulVersions \in [Tasks -> SUBSET Versions]
  /\ failedVersions \in [Tasks -> SUBSET Versions]
  /\ baselineVersion \in [Tasks -> 0..MaxVersion]
  /\ clarificationCount \in [Tasks -> 0..MaxClarifications]
  /\ terminalTasks \in SUBSET Tasks
  /\ terminalRequestCount \in [Tasks -> 0..Cardinality(Requests)]
  /\ requestStatus \in [Requests -> RequestStates]
  /\ requestTask \in [Requests -> Tasks]
  /\ requestCaller \in [Requests -> Callers]
  /\ requestWorkspace \in [Requests -> Workspaces]
  /\ requestDigest \in [Requests -> Digests]
  /\ requestResults \in [Requests -> ResultMaps]
  /\ httpDecision \in [Requests -> Decisions]
  /\ streamCursor \in [Requests -> 0..MaxCursor]
  /\ streamTerminal \in [Requests -> BOOLEAN]
  /\ closedCursor \in [Requests -> 0..MaxCursor]
  /\ responseKind \in [Requests -> ResponseKinds]
  /\ responseTrace \in [Requests -> BoundedTraces]
  /\ callerDetached \in [Requests -> BOOLEAN]
  /\ lastSubmission \in [
       request     : Requests,
       digest      : Digests,
       disposition : SubmitDispositions,
       decision    : Decisions,
       cursor      : 0..MaxCursor,
       terminal    : BOOLEAN,
       trace       : BoundedTraces
     ]

KeysRemainOnLLMSurface ==
  /\ LLMKind \in Surfaces
  /\ AgentKind \in Surfaces
  /\ LLMKind # AgentKind
  /\ \A task \in Tasks :
       taskStatus[task] # TaskUnused =>
         /\ IsScopedKey(TaskKey(task), {LLMKind}, AllScopes, AllTaskKeyIDs)
         /\ KeyKind(TaskKey(task)) = LLMKind
         /\ KeyScope(TaskKey(task)) = TaskScope(task)
         /\ KeyID(TaskKey(task)) = TaskKeyID(task)
  /\ \A request \in Requests :
       requestStatus[request] # RequestUnused =>
         /\ IsScopedKey(
              RequestKey(request), {LLMKind}, AllScopes, AllRequestKeyIDs)
         /\ KeyKind(RequestKey(request)) = LLMKind
         /\ KeyScope(RequestKey(request)) = RequestScope(request)
         /\ KeyID(RequestKey(request)) = RequestKeyID(request)

RequestOwnershipIsStable ==
  /\ \A task \in Tasks :
       taskStatus[task] # TaskUnused =>
         /\ taskRequests[task] # {}
         /\ taskCurrentRequest[task] \in taskRequests[task]
         /\ \A request \in taskRequests[task] :
              /\ requestStatus[request] # RequestUnused
              /\ requestTask[request] = task
              /\ requestCaller[request] = taskCaller[task]
              /\ requestWorkspace[request] = taskWorkspace[task]
  /\ \A request \in Requests :
       requestStatus[request] # RequestUnused =>
         request \in taskRequests[requestTask[request]]

IdempotencyObservationsAreExact ==
  /\ lastSubmission.disposition = SubmitReplay =>
       /\ requestStatus[lastSubmission.request] # RequestUnused
       /\ lastSubmission.digest = requestDigest[lastSubmission.request]
       /\ lastSubmission.decision = httpDecision[lastSubmission.request]
       /\ lastSubmission.cursor = streamCursor[lastSubmission.request]
       /\ lastSubmission.terminal = streamTerminal[lastSubmission.request]
       /\ lastSubmission.trace = responseTrace[lastSubmission.request]
  /\ lastSubmission.disposition = SubmitConflict =>
       /\ requestStatus[lastSubmission.request] # RequestUnused
       /\ lastSubmission.digest # requestDigest[lastSubmission.request]
       /\ lastSubmission.decision = DecisionConflict

HTTPDecisionPrecedesVisibility ==
  \A request \in Requests :
    \/ requestStatus[request] = RequestUnused
    \/ responseTrace[request] = <<>>
    \/ httpDecision[request] = DecisionOK

TraceMatchesCursor ==
  \A request \in Requests :
    /\ Len(responseTrace[request]) = streamCursor[request]
    /\ (streamTerminal[request] =>
          responseTrace[request][Len(responseTrace[request])] =
            responseKind[request])

StreamTerminalIsFinal ==
  \A request \in Requests :
    /\ (streamTerminal[request] <=>
          requestStatus[request] = RequestClosed)
    /\ (streamTerminal[request] <=>
          IsLLMResponseTerminal(responseKind[request]))
    /\ (streamTerminal[request] =>
          /\ streamCursor[request] = closedCursor[request]
          /\ streamCursor[request] > 0)

ToolCallsCloseOnlyTheirResponse ==
  \A request \in Requests :
    responseKind[request] = "tool_calls" =>
      /\ requestStatus[request] = RequestClosed
      /\ streamTerminal[request]
      /\ issuedVersions[requestTask[request]] # {}

ClarificationsCloseOnlyTheirResponse ==
  \A request \in Requests :
    responseKind[request] = "clarification" =>
      /\ requestStatus[request] = RequestClosed
      /\ streamTerminal[request]

ResultBucketsDoNotSwallowEachOther ==
  \A task \in Tasks :
    /\ issuedVersions[task] =
         pendingVersions[task] \cup
         successfulVersions[task] \cup failedVersions[task]
    /\ pendingVersions[task] \cap successfulVersions[task] = {}
    /\ pendingVersions[task] \cap failedVersions[task] = {}
    /\ successfulVersions[task] \cap failedVersions[task] = {}

BaselineAdvancesOnlyOnSuccess ==
  \A task \in Tasks :
    baselineVersion[task] = MaxOf(successfulVersions[task])

ResultTurnReconcilesBeforeResponse ==
  \A request \in Requests :
    requestResults[request] # MissingResults /\
    taskCurrentRequest[requestTask[request]] = request /\
    requestStatus[request] \in
      {RequestDecided, RequestStreaming, RequestClosed} =>
      \A version \in Versions :
        requestResults[request][version] # ResultMissing =>
          version \notin pendingVersions[requestTask[request]]

TerminalTaskCannotBeReopened ==
  /\ terminalTasks = {task \in Tasks : taskStatus[task] \in TaskTerminalStates}
  /\ \A task \in terminalTasks :
       Cardinality(taskRequests[task]) = terminalRequestCount[task]

RequestIdentityImmutable ==
  [][\A request \in Requests :
       requestStatus[request] # RequestUnused =>
         /\ requestTask'[request] = requestTask[request]
         /\ requestCaller'[request] = requestCaller[request]
         /\ requestWorkspace'[request] = requestWorkspace[request]
         /\ requestDigest'[request] = requestDigest[request]
         /\ requestResults'[request] = requestResults[request]]_vars

HTTPDecisionsImmutable ==
  [][\A request \in Requests :
       httpDecision[request] # DecisionNone =>
         httpDecision'[request] = httpDecision[request]]_vars

ClosedResponsesImmutable ==
  [][\A request \in Requests :
       requestStatus[request] = RequestClosed =>
         /\ requestStatus'[request] = RequestClosed
         /\ streamCursor'[request] = streamCursor[request]
         /\ streamTerminal'[request] = streamTerminal[request]
         /\ closedCursor'[request] = closedCursor[request]
         /\ responseKind'[request] = responseKind[request]
         /\ responseTrace'[request] = responseTrace[request]]_vars

ClarificationTransitionIsNonterminal ==
  [][\A request \in Requests :
       /\ responseKind[request] # "clarification"
       /\ responseKind'[request] = "clarification"
       => taskStatus'[requestTask'[request]] = TaskAwaitingCaller]_vars

ResponseTerminalTransitionMatchesTask ==
  [][\A request \in Requests :
       /\ responseKind[request] # responseKind'[request]
       /\ IsLLMResponseTerminal(responseKind'[request])
       => LET task == requestTask'[request]
          IN CASE responseKind'[request] = "text_final" ->
                    /\ taskStatus'[task] = TaskCompleted
                    /\ task \in terminalTasks'
                    /\ terminalRequestCount'[task] =
                         Cardinality(taskRequests'[task])
               [] responseKind'[request] = "error" ->
                    /\ taskStatus'[task] = TaskFailed
                    /\ task \in terminalTasks'
                    /\ terminalRequestCount'[task] =
                         Cardinality(taskRequests'[task])
               [] responseKind'[request] = "tool_calls" ->
                    /\ taskStatus'[task] = TaskAwaitingResults
                    /\ task \notin terminalTasks'
                    /\ pendingVersions'[task] # {}
                    /\ pendingVersions'[task] =
                         issuedVersions'[task] \ issuedVersions[task]
               [] responseKind'[request] = "clarification" ->
                    /\ taskStatus'[task] = TaskAwaitingCaller
                    /\ task \notin terminalTasks']_vars

ReconcileResultIsExact ==
  [][\A task \in Tasks :
       /\ pendingVersions'[task] \subseteq pendingVersions[task]
       /\ pendingVersions'[task] # pendingVersions[task]
       => LET removed == pendingVersions[task] \ pendingVersions'[task]
          IN /\ Cardinality(removed) = 1
             /\ LET version == CHOOSE candidate \in removed : TRUE
                    request == taskCurrentRequest[task]
                IN /\ successfulVersions'[task] =
                         IF requestResults[request][version] = ResultSuccess
                         THEN successfulVersions[task] \union {version}
                         ELSE successfulVersions[task]
                   /\ failedVersions'[task] =
                         IF requestResults[request][version] = ResultFailure
                         THEN failedVersions[task] \union {version}
                         ELSE failedVersions[task]
                   /\ taskStatus'[task] =
                         IF pendingVersions'[task] = {}
                         THEN TaskReconciled
                         ELSE taskStatus[task]
             /\ \A other \in Tasks \ {task} :
                  /\ pendingVersions'[other] = pendingVersions[other]
                  /\ successfulVersions'[other] = successfulVersions[other]
                  /\ failedVersions'[other] = failedVersions[other]]_vars

ResponseTracesAppendOnly ==
  [][\A request \in Requests :
       /\ Len(responseTrace'[request]) >= Len(responseTrace[request])
       /\ SubSeq(responseTrace'[request], 1, Len(responseTrace[request])) =
            responseTrace[request]]_vars

NoCallerLeavesResultsPending ==
  CallerAvailable \/
    \A task \in Tasks :
      /\ taskStatus[task] # TaskReconciled
      /\ successfulVersions[task] = {}
      /\ failedVersions[task] = {}
      /\ baselineVersion[task] = 0

ResponsesEventuallyClose ==
  \A request \in Requests :
    [](requestStatus[request] # RequestUnused =>
       <> (requestStatus[request] \in {RequestClosed, RequestSuperseded}))

(* Availability under a gone caller (scenario C): a task held by a stale       *)
(* in-flight request (Active, current still Admitted) never blocks a resume    *)
(* forever.  The current request eventually advances (decided/closed) or is    *)
(* superseded by a resuming turn, or the task terminates — a resume is never   *)
(* permanently reconciliation-conflicted.  This is the liveness the takeover   *)
(* restores.                                                                    *)
ResumeNeverPermanentlyBlocked ==
  \A task \in Tasks :
    []( ( taskStatus[task] = TaskActive
          /\ requestStatus[taskCurrentRequest[task]] = RequestAdmitted )
        => <>( requestStatus[taskCurrentRequest[task]] # RequestAdmitted
               \/ taskStatus[task] \in TaskTerminalStates ) )

TasksEventuallyTerminal ==
  \A task \in Tasks :
    [](taskStatus[task] # TaskUnused =>
       <> (taskStatus[task] \in TaskTerminalStates))

MultipleProgressSegmentsEventuallyClose == <>ProgressDone

=============================================================================
