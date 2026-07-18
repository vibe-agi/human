#!/usr/bin/env python3
"""Generate one intentionally broken TLA+ module for run-checks.sh.

These are reachability/oracle tests, not alternative designs.  Each mutation
removes exactly one protocol guarantee and TLC must reject the result for the
named invariant/property.  Generated modules live only in the runner's
temporary directory.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def replacement(old: str, new: str) -> tuple[str, str]:
    return old, new


MUTANTS: dict[str, tuple[str, list[tuple[str, str]]]] = {
    "agent_transport_owner_only": (
        "HumanAgentTransport.tla",
        [replacement(
            "CommitGrantCurrent(envelope) == GrantCurrent(envelope)\n",
            "CommitGrantCurrent(envelope) ==\n"
            "  leaseOwner[EnvelopeTask(envelope)] = envelope.worker\n",
        )],
    ),
    "agent_transport_precheck_only": (
        "HumanAgentTransport.tla",
        [replacement(
            "CommitGrantCurrent(envelope) == GrantCurrent(envelope)\n",
            "CommitGrantCurrent(envelope) == envelope \\in grantPrechecked\n",
        )],
    ),
    "agent_transport_revision_precheck_only": (
        "HumanAgentTransport.tla",
        [replacement(
            "CommitRevisionCurrent(envelope) == RevisionCurrent(envelope)\n",
            "CommitRevisionCurrent(envelope) == envelope \\in revisionPrechecked\n",
        )],
    ),
    "agent_transport_replay_requires_grant": (
        "HumanAgentTransport.tla",
        [replacement(
            "ReplayAuthorization(envelope) == TRUE\n",
            "ReplayAuthorization(envelope) == GrantCurrent(envelope)\n",
        )],
    ),
    "agent_transport_replay_reeffects": (
        "HumanAgentTransport.tla",
        [replacement(
            "  /\\ UNCHANGED <<taskVars, outbox, wire, effects, commandReceipts,\n"
            "                  commitFacts, effectCount, ackWire, nackWire, settled,\n"
            "                  environmentVars>>\n\n"
            "RejectPrepared(envelope) ==\n",
            "  /\\ effectCount' =\n"
            "       [effectCount EXCEPT ![CommandInput(envelope)] = @ + 1]\n"
            "  /\\ UNCHANGED <<taskVars, outbox, wire, effects, commandReceipts,\n"
            "                  commitFacts, ackWire, nackWire, settled,\n"
            "                  environmentVars>>\n\n"
            "RejectPrepared(envelope) ==\n",
        )],
    ),
    "agent_transport_early_ack": (
        "HumanAgentTransport.tla",
        [replacement(
            "  /\\ envelope \\in deliveryAccepted \\cap outbox\n"
            "  /\\ envelope \\notin ackWire\n",
            "  /\\ envelope \\in outbox\n"
            "  /\\ envelope \\notin ackWire\n",
        )],
    ),
    "agent_transport_split_effect_receipt": (
        "HumanAgentTransport.tla",
        [replacement(
            "     /\\ commandReceipts' = commandReceipts \\union {command}\n"
            "     /\\ commitFacts' = commitFacts \\union {fact}\n",
            "     /\\ commandReceipts' = commandReceipts\n"
            "     /\\ commitFacts' = commitFacts \\union {fact}\n",
        )],
    ),
    "agent_transport_terminal_keeps_grant": (
        "HumanAgentTransport.tla",
        [replacement(
            "     /\\ leaseOwner' =\n"
            "          IF TerminalOperation(envelope)\n"
            "          THEN [leaseOwner EXCEPT ![ref] = NoWorker]\n"
            "          ELSE leaseOwner\n",
            "     /\\ leaseOwner' = leaseOwner\n",
        )],
    ),
    "agent_transport_visible_precommit": (
        "HumanAgentTransport.tla",
        [replacement(
            "ExposeGrant(grant) ==\n"
            "  /\\ grant \\in durableGrants \\ visibleGrants\n"
            "  /\\ grant = CurrentGrant(TaskRef(grant.authority, grant.workspace, grant.task))\n",
            "ExposeGrant(grant) ==\n"
            "  /\\ grant \\in (durableGrants \\union grantPrepared) \\ visibleGrants\n"
            "  \\* MUTANT: expose an internal grant before its transaction commits.\n",
        )],
    ),
    "agent_transport_digest_conflict_replays": (
        "HumanAgentTransport.tla",
        [replacement(
            "ExactCommitted(envelope) ==\n"
            "  CommandInput(envelope) \\in commandReceipts\n",
            "ExactCommitted(envelope) ==\n"
            "  \\E receipt \\in commandReceipts :\n"
            "    CommandKey(receipt) = CommandKey(envelope)\n",
        )],
    ),
    "agent_transport_stale_nack_keeps_outbox": (
        "HumanAgentTransport.tla",
        [replacement(
            "ConsumeNACK(envelope) ==\n"
            "  /\\ envelope \\in nackWire \\cap outbox\n"
            "  /\\ outbox' = outbox \\ {envelope}\n",
            "ConsumeNACK(envelope) ==\n"
            "  /\\ envelope \\in nackWire \\cap outbox\n"
            "  /\\ outbox' = outbox\n",
        )],
    ),
    "sequence_old_frame_absorbs_future_ack": (
        "HumanWorkerSequence.tla",
        [replacement(
            "  /\\ wireFrame' = Head(serverQueue)\n"
            "  /\\ serverQueue' = Tail(serverQueue)\n",
            "  /\\ wireFrame' =\n"
            "       [Head(serverQueue) EXCEPT !.ack = serverCommitted]\n"
            "  /\\ serverQueue' = Tail(serverQueue)\n",
        )],
    ),
    "sequence_rejection_not_atomic": (
        "HumanWorkerSequence.tla",
        [replacement(
            "  /\\ clientRejectedInbox' =\n"
            "       IF wireFrame.kind = RejectionFrame\n"
            "       THEN clientRejectedInbox \\union {Late}\n"
            "       ELSE clientRejectedInbox\n",
            "  /\\ clientRejectedInbox' = clientRejectedInbox\n",
        )],
    ),
    "sequence_stale_snapshot_regresses_ack": (
        "HumanWorkerSequence.tla",
        [replacement(
            "              Frame(PostRejectionAssignmentFrame, serverLastQueuedAck))\n",
            "              Frame(PostRejectionAssignmentFrame, 1))\n",
        )],
    ),
    "runtime_ack_before_durable": (
        "HumanRuntime.tla",
        [replacement(
            "  /\\ envelope \\in receipts \\cap outbox\n"
            "  /\\ envelope \\notin ackWire\n",
            "  /\\ envelope \\in outbox\n"
            "  /\\ envelope \\notin ackWire\n",
        )],
    ),
    "runtime_crash_drops_outbox": (
        "HumanRuntime.tla",
        [replacement(
            "  /\\ wire' = {}\n"
            "  /\\ ackWire' = {}\n"
            "  /\\ nackWire' = {}\n"
            "  /\\ faultsLeft' = faultsLeft - 1\n"
            "  /\\ UNCHANGED <<callerUp, workerUp,\n"
            "                  outbox, enqueued, effects, receipts, rejections, settled,\n",
            "  /\\ outbox' = {}\n"
            "  /\\ wire' = {}\n"
            "  /\\ ackWire' = {}\n"
            "  /\\ nackWire' = {}\n"
            "  /\\ faultsLeft' = faultsLeft - 1\n"
            "  /\\ UNCHANGED <<callerUp, workerUp,\n"
            "                  enqueued, effects, receipts, rejections, settled,\n",
        )],
    ),
    "runtime_durable_history_drops_retired": (
        "HumanRuntime.tla",
        [replacement(
            "  /\\ visibleOutcomes' = visibleOutcomes \\union {item}\n"
            "  /\\ UNCHANGED <<admitted, queue, leaseOwner, leaseFence, leaseGrants,\n"
            "                  visibleAssignments, retired, durableOutcomes,\n",
            "  /\\ visibleOutcomes' = visibleOutcomes \\union {item}\n"
            "  /\\ retired' = retired \\ {item}\n"
            "  /\\ UNCHANGED <<admitted, queue, leaseOwner, leaseFence, leaseGrants,\n"
            "                  visibleAssignments, durableOutcomes,\n",
        )],
    ),
    "runtime_stale_fence_commits": (
        "HumanRuntime.tla",
        [replacement(
            "  /\\ ValidCurrentEnvelope(envelope)\n"
            "  /\\ ReceiptFor(envelope.event) = {}\n",
            "  \\* MUTANT: a stale lease may commit\n"
            "  /\\ ReceiptFor(envelope.event) = {}\n",
        )],
    ),
    "runtime_late_event_poison": (
        "HumanRuntime.tla",
        [replacement(
            "  /\\ \\/ ~ValidCurrentEnvelope(envelope)\n"
            "     \\/ ReceiptFor(envelope.event) # {}\n",
            "  /\\ \\/ /\\ ~ValidCurrentEnvelope(envelope)\n"
            "           /\\ EnvelopeItem(envelope) \\notin retired\n"
            "     \\/ ReceiptFor(envelope.event) # {}\n",
        )],
    ),
    "runtime_workspace_lost_update": (
        "HumanRuntime.tla",
        [
            replacement(
                "  /\\ proposal.base = workspaceVersion[ProposalScope(proposal)]\n"
                "  /\\ workspaceVersion' =\n",
                "  \\* MUTANT: stale proposals overwrite the current version\n"
                "  /\\ workspaceVersion' =\n",
            ),
            replacement(
                "  \\/ (committedChanges # {} /\\ pendingChanges # {} /\\ ConflictChangeAny)\n",
                "  \\/ (committedChanges # {} /\\ pendingChanges # {} /\\ CommitChangeAny)\n",
            ),
        ],
    ),
    "llm_digest_conflict_lies": (
        "HumanLLM.tla",
        [replacement(
            "       digest      |-> digest,\n"
            "       disposition |-> SubmitConflict,\n",
            "       digest      |-> requestDigest[request],\n"
            "       disposition |-> SubmitConflict,\n",
        )],
    ),
    "llm_stream_before_decision": (
        "HumanLLM.tla",
        [replacement(
            "StartStream(request) ==\n"
            "  /\\ requestStatus[request] = RequestDecided\n"
            "  /\\ httpDecision[request] = DecisionOK\n",
            "StartStream(request) ==\n"
            "  /\\ requestStatus[request] = RequestAdmitted\n"
            "  /\\ httpDecision[request] = DecisionNone\n",
        )],
    ),
    "llm_request_results_mutate": (
        "HumanLLM.tla",
        [replacement(
            "  /\\ lastSubmission' = NoSubmission\n"
            "  /\\ UNCHANGED <<\n"
            "       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,\n"
            "       issuedVersions, pendingVersions, successfulVersions, failedVersions,\n"
            "       baselineVersion, clarificationCount, terminalTasks, terminalRequestCount,\n"
            "       requestTask, requestCaller, requestWorkspace, requestDigest,\n"
            "       requestResults, streamCursor, streamTerminal, closedCursor,\n"
            "       responseKind, responseTrace\n"
            "     >>\n\n"
            "StartStream(request) ==\n",
            "  /\\ lastSubmission' = NoSubmission\n"
            "  /\\ requestResults' =\n"
            "       [requestResults EXCEPT ![request] =\n"
            "          [version \\in Versions |->\n"
            "             IF version = 1 THEN ResultSuccess ELSE ResultMissing]]\n"
            "  /\\ UNCHANGED <<\n"
            "       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,\n"
            "       issuedVersions, pendingVersions, successfulVersions, failedVersions,\n"
            "       baselineVersion, clarificationCount, terminalTasks, terminalRequestCount,\n"
            "       requestTask, requestCaller, requestWorkspace, requestDigest,\n"
            "       streamCursor, streamTerminal, closedCursor,\n"
            "       responseKind, responseTrace\n"
            "     >>\n\n"
            "StartStream(request) ==\n",
        )],
    ),
    "llm_terminal_reopens": (
        "HumanLLM.tla",
        [replacement(
            "  /\\ taskStatus[task] = TaskAwaitingCaller\n"
            "  /\\ taskCaller[task] = caller\n",
            "  /\\ taskStatus[task] = TaskCompleted\n"
            "  /\\ taskCaller[task] = caller\n",
        )],
    ),
    "llm_failure_advances_baseline": (
        "HumanLLM.tla",
        [replacement(
            "  /\\ baselineVersion' =\n"
            "       IF requestResults[request][version] = ResultSuccess\n"
            "       THEN [baselineVersion EXCEPT ![task] = MaxOf(successfulVersions'[task])]\n"
            "       ELSE baselineVersion\n",
            "  /\\ baselineVersion' =\n"
            "       [baselineVersion EXCEPT ![task] = version]\n",
        )],
    ),
    "llm_tool_calls_terminate_task": (
        "HumanLLM.tla",
        [replacement(
            "  /\\ taskStatus' = [taskStatus EXCEPT ![task] = TaskAwaitingResults]\n"
            "  /\\ lastSubmission' = NoSubmission\n"
            "  /\\ UNCHANGED <<\n"
            "       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,\n"
            "       successfulVersions, failedVersions, baselineVersion, clarificationCount, terminalTasks,\n"
            "       terminalRequestCount, requestTask, requestCaller, requestWorkspace,\n",
            "  /\\ taskStatus' = [taskStatus EXCEPT ![task] = TaskFailed]\n"
            "  /\\ terminalTasks' = terminalTasks \\union {task}\n"
            "  /\\ terminalRequestCount' =\n"
            "       [terminalRequestCount EXCEPT ![task] = Cardinality(taskRequests[task])]\n"
            "  /\\ lastSubmission' = NoSubmission\n"
            "  /\\ UNCHANGED <<\n"
            "       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,\n"
            "       successfulVersions, failedVersions, baselineVersion, clarificationCount,\n"
            "       requestTask, requestCaller, requestWorkspace,\n",
        )],
    ),
    "llm_error_marked_completed": (
        "HumanLLM.tla",
        [replacement(
            "  /\\ CloseResponse(request, \"error\")\n"
            "  /\\ taskStatus' = [taskStatus EXCEPT ![task] = TaskFailed]\n",
            "  /\\ CloseResponse(request, \"error\")\n"
            "  /\\ taskStatus' = [taskStatus EXCEPT ![task] = TaskCompleted]\n",
        )],
    ),
    "llm_text_final_marked_failed": (
        "HumanLLM.tla",
        [replacement(
            "  /\\ taskStatus' = [taskStatus EXCEPT ![task] = TaskCompleted]\n",
            "  /\\ taskStatus' = [taskStatus EXCEPT ![task] = TaskFailed]\n",
        )],
    ),
    "llm_clarification_terminates_task": (
        "HumanLLM.tla",
        [replacement(
            "  /\\ taskStatus' = [taskStatus EXCEPT ![task] = TaskAwaitingCaller]\n"
            "  /\\ clarificationCount' = [clarificationCount EXCEPT ![task] = @ + 1]\n"
            "  /\\ lastSubmission' = NoSubmission\n"
            "  /\\ UNCHANGED <<\n"
            "       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,\n"
            "       issuedVersions, pendingVersions, successfulVersions, failedVersions,\n"
            "       baselineVersion, terminalTasks, terminalRequestCount,\n",
            "  /\\ taskStatus' = [taskStatus EXCEPT ![task] = TaskCompleted]\n"
            "  /\\ terminalTasks' = terminalTasks \\cup {task}\n"
            "  /\\ terminalRequestCount' =\n"
            "       [terminalRequestCount EXCEPT ![task] = Cardinality(taskRequests[task])]\n"
            "  /\\ clarificationCount' = [clarificationCount EXCEPT ![task] = @ + 1]\n"
            "  /\\ lastSubmission' = NoSubmission\n"
            "  /\\ UNCHANGED <<\n"
            "       taskCaller, taskWorkspace, taskRequests, taskCurrentRequest,\n"
            "       issuedVersions, pendingVersions, successfulVersions, failedVersions,\n"
            "       baselineVersion,\n",
        )],
    ),
    "llm_reconcile_misclassifies_result": (
        "HumanLLM.tla",
        [
            replacement(
                "  /\\ successfulVersions' =\n"
                "       IF requestResults[request][version] = ResultSuccess\n"
                "       THEN [successfulVersions EXCEPT ![task] = @ \\cup {version}]\n",
                "  /\\ successfulVersions' =\n"
                "       IF requestResults[request][version] = ResultFailure\n"
                "       THEN [successfulVersions EXCEPT ![task] = @ \\cup {version}]\n",
            ),
            replacement(
                "  /\\ failedVersions' =\n"
                "       IF requestResults[request][version] = ResultFailure\n"
                "       THEN [failedVersions EXCEPT ![task] = @ \\cup {version}]\n",
                "  /\\ failedVersions' =\n"
                "       IF requestResults[request][version] = ResultSuccess\n"
                "       THEN [failedVersions EXCEPT ![task] = @ \\cup {version}]\n",
            ),
            replacement(
                "  /\\ baselineVersion' =\n"
                "       IF requestResults[request][version] = ResultSuccess\n"
                "       THEN [baselineVersion EXCEPT ![task] = MaxOf(successfulVersions'[task])]\n",
                "  /\\ baselineVersion' =\n"
                "       IF requestResults[request][version] = ResultFailure\n"
                "       THEN [baselineVersion EXCEPT ![task] = MaxOf(successfulVersions'[task])]\n",
            ),
        ],
    ),
    "llm_terminal_overwrites_progress": (
        "HumanLLM.tla",
        [replacement(
            "  /\\ responseTrace' = [responseTrace EXCEPT ![request] = Append(@, kind)]\n",
            "  /\\ responseTrace' = [responseTrace EXCEPT ![request] = <<kind>>]\n",
        )],
    ),
    "llm_reconcile_swallows_pending": (
        "HumanLLM.tla",
        [
            replacement(
                "  /\\ pendingVersions' =\n"
                "       [pendingVersions EXCEPT ![task] = @ \\ {version}]\n",
                "  /\\ pendingVersions' =\n"
                "       [pendingVersions EXCEPT ![task] = {}]\n",
            ),
            replacement(
                "       THEN [successfulVersions EXCEPT ![task] = @ \\cup {version}]\n",
                "       THEN [successfulVersions EXCEPT\n"
                "               ![task] = @ \\union pendingVersions[task]]\n",
            ),
            replacement(
                "       THEN [failedVersions EXCEPT ![task] = @ \\cup {version}]\n",
                "       THEN [failedVersions EXCEPT\n"
                "               ![task] = @ \\union pendingVersions[task]]\n",
            ),
        ],
    ),
    "agent_terminal_reopens": (
        "HumanAgent.tla",
        [replacement(
            "CallerReply(task, message) ==\n"
            "  /\\ taskState[task] = InputNeeded\n",
            "CallerReply(task, message) ==\n"
            "  /\\ taskState[task] \\in {InputNeeded, Completed}\n",
        )],
    ),
    "agent_submission_nonatomic": (
        "HumanAgent.tla",
        [replacement(
            "  /\\ taskState' = [taskState EXCEPT ![task] = Completed]\n"
            "  /\\ artifactState' = [artifactState EXCEPT\n"
            "                         ![artifact] = ArtifactPublished]\n",
            "  /\\ taskState' = taskState\n"
            "  /\\ artifactState' = [artifactState EXCEPT\n"
            "                         ![artifact] = ArtifactPublished]\n",
        )],
    ),
    "agent_dirty_apply_succeeds": (
        "HumanAgent.tla",
        [replacement(
            "     /\\ ~dirty[workspace]\n"
            "     /\\ baseline[workspace] = artifactBase[artifact]\n",
            "     \\* MUTANT: an unverified external edit is ignored\n"
            "     /\\ baseline[workspace] = artifactBase[artifact]\n",
        )],
    ),
    "agent_wrong_base_succeeds": (
        "HumanAgent.tla",
        [replacement(
            "     /\\ baseline[workspace] = artifactBase[artifact]\n"
            "     /\\ receipt' = [receipt EXCEPT ![artifact] = ReceiptSuccess]\n",
            "     /\\ baseline[workspace] >= artifactBase[artifact]\n"
            "     /\\ receipt' = [receipt EXCEPT ![artifact] = ReceiptSuccess]\n",
        )],
    ),
    "agent_cancel_leaves_frozen": (
        "HumanAgent.tla",
        [replacement(
            "CancelTask(task) ==\n"
            "  /\\ TaskActive(task)\n"
            "  /\\ taskState' = [taskState EXCEPT ![task] = Canceled]\n"
            "  /\\ artifactState' =\n"
            "       [artifact \\in ArtifactIds |->\n"
            "         IF artifactTask[artifact] = task /\\\n"
            "            artifactState[artifact] = ArtifactFrozen\n"
            "           THEN ArtifactDiscarded\n"
            "           ELSE artifactState[artifact]]\n",
            "CancelTask(task) ==\n"
            "  /\\ TaskActive(task)\n"
            "  /\\ taskState' = [taskState EXCEPT ![task] = Canceled]\n"
            "  /\\ artifactState' = artifactState\n",
        )],
    ),
    "agent_wrong_message_role": (
        "HumanAgent.tla",
        [replacement(
            "RequestInput(task, message) ==\n"
            "  /\\ taskState[task] = Working\n"
            "  /\\ messageAuthor[message] = NoAuthor\n"
            "  /\\ taskState' = [taskState EXCEPT ![task] = InputNeeded]\n"
            "  /\\ messages' = [messages EXCEPT ![task] = Append(@, message)]\n"
            "  /\\ messageAuthor' = [messageAuthor EXCEPT ![message] = Agent]\n",
            "RequestInput(task, message) ==\n"
            "  /\\ taskState[task] = Working\n"
            "  /\\ messageAuthor[message] = NoAuthor\n"
            "  /\\ taskState' = [taskState EXCEPT ![task] = InputNeeded]\n"
            "  /\\ messages' = [messages EXCEPT ![task] = Append(@, message)]\n"
            "  /\\ messageAuthor' = [messageAuthor EXCEPT ![message] = Caller]\n",
        )],
    ),
    "agent_task_identity_changes": (
        "HumanAgent.tla",
        [replacement(
            "AcceptTask(task) ==\n"
            "  /\\ taskState[task] = Submitted\n"
            "  /\\ taskState' = [taskState EXCEPT ![task] = Working]\n"
            "  /\\ UNCHANGED <<taskContext, taskWorkspace, messages, messageAuthor,\n",
            "AcceptTask(task) ==\n"
            "  /\\ taskState[task] = Submitted\n"
            "  /\\ taskState' = [taskState EXCEPT ![task] = Working]\n"
            "  /\\ taskWorkspace' =\n"
            "       [taskWorkspace EXCEPT\n"
            "          ![task] = CHOOSE workspace \\in WorkspaceIds \\ {taskWorkspace[task]} : TRUE]\n"
            "  /\\ UNCHANGED <<taskContext, messages, messageAuthor,\n",
        )],
    ),
    "agent_conflict_advances_baseline": (
        "HumanAgent.tla",
        [replacement(
            "RecordApplyConflict(artifact) ==\n"
            "  /\\ artifactState[artifact] = ArtifactPublished\n"
            "  /\\ receipt[artifact] = ReceiptNone\n"
            "  /\\ receipt' = [receipt EXCEPT ![artifact] = ReceiptConflict]\n"
            "  /\\ UNCHANGED <<taskState, taskContext, taskWorkspace, messages, messageAuthor,\n"
            "                  artifactState, artifactTask, artifactBase, artifactVersion,\n"
            "                  submission, draft, baseline, dirty>>\n",
            "RecordApplyConflict(artifact) ==\n"
            "  /\\ artifactState[artifact] = ArtifactPublished\n"
            "  /\\ receipt[artifact] = ReceiptNone\n"
            "  /\\ receipt' = [receipt EXCEPT ![artifact] = ReceiptConflict]\n"
            "  /\\ baseline' =\n"
            "       [baseline EXCEPT\n"
            "          ![ArtifactWorkspace(artifact)] = artifactVersion[artifact]]\n"
            "  /\\ UNCHANGED <<taskState, taskContext, taskWorkspace, messages, messageAuthor,\n"
            "                  artifactState, artifactTask, artifactBase, artifactVersion,\n"
            "                  submission, draft, dirty>>\n",
        )],
    ),
    "agent_reply_does_not_resume": (
        "HumanAgent.tla",
        [replacement(
            "CallerReply(task, message) ==\n"
            "  /\\ taskState[task] = InputNeeded\n"
            "  /\\ messageAuthor[message] = NoAuthor\n"
            "  /\\ taskState' = [taskState EXCEPT ![task] = Working]\n",
            "CallerReply(task, message) ==\n"
            "  /\\ taskState[task] = InputNeeded\n"
            "  /\\ messageAuthor[message] = NoAuthor\n"
            "  /\\ taskState' = [taskState EXCEPT ![task] = InputNeeded]\n",
        )],
    ),
    "system_surface_collision": (
        "HumanSystem.tla",
        [replacement(
            "  /\\ agentKey = ScopedKey(AgentKind, Scope, AgentItemID)\n",
            "  /\\ agentKey = ScopedKey(LLMKind, Scope, AgentItemID)\n",
        )],
    ),
    "system_premature_baseline": (
        "HumanSystem.tla",
        [replacement(
            "ConfirmLLMReceipt ==\n"
            "  /\\ llmIntentState = \"applied\"\n"
            "  /\\ llmIntent \\in successReceipts\n",
            "ConfirmLLMReceipt ==\n"
            "  /\\ llmIntentState = \"published\"\n"
            "  \\* MUTANT: publication is mistaken for verified apply success\n",
        )],
    ),
    "system_frozen_payload_mutates_base": (
        "HumanSystem.tla",
        [replacement(
            "  /\\ llmIntentState' = \"published\"\n"
            "  /\\ llmResponse' = \"tool_calls\"\n"
            "  /\\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,\n"
            "                 agentTask, agentTerminated,\n"
            "                 llmDraft, llmDraftBase, llmCreated,\n"
            "                 llmIntent, llmIntentBase,\n",
            "  /\\ llmIntentState' = \"published\"\n"
            "  /\\ llmResponse' = \"tool_calls\"\n"
            "  /\\ llmIntentBase' = LLMVersion2\n"
            "  /\\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,\n"
            "                 agentTask, agentTerminated,\n"
            "                 llmDraft, llmDraftBase, llmCreated,\n"
            "                 llmIntent,\n",
        )],
    ),
    "system_frozen_intent_dropped_early": (
        "HumanSystem.tla",
        [replacement(
            "  /\\ llmIntentState' = \"published\"\n"
            "  /\\ llmResponse' = \"tool_calls\"\n"
            "  /\\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,\n"
            "                 agentTask, agentTerminated,\n"
            "                 llmDraft, llmDraftBase, llmCreated,\n"
            "                 llmIntent, llmIntentBase,\n",
            "  /\\ llmIntentState' = \"none\"\n"
            "  /\\ llmIntent' = NoVersion\n"
            "  /\\ llmIntentBase' = NoVersion\n"
            "  /\\ llmResponse' = \"tool_calls\"\n"
            "  /\\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,\n"
            "                 agentTask, agentTerminated,\n"
            "                 llmDraft, llmDraftBase, llmCreated,\n",
        )],
    ),
    "system_baseline_selects_old_success": (
        "HumanSystem.tla",
        [replacement(
            "  /\\ llmBaseline' = llmIntent\n",
            "  /\\ llmBaseline' =\n"
            "       IF LLMVersion1 \\in successReceipts\n"
            "       THEN LLMVersion1\n"
            "       ELSE llmIntent\n",
        )],
    ),
    "system_late_receipt_drops_newer": (
        "HumanSystem.tla",
        [replacement(
            "  /\\ IF llmDraft = llmIntent\n",
            "  /\\ IF TRUE\n",
        )],
    ),
    "system_artifact_nonatomic": (
        "HumanSystem.tla",
        [replacement(
            "  /\\ publishedArtifacts' = publishedArtifacts \\union {agentIntent}\n"
            "  \\* The authoritative final Submission and Task completion are one durable\n"
            "  \\* visibility boundary. Streaming previews are deliberately not modeled as\n"
            "  \\* published final Artifacts.\n"
            "  /\\ agentTask' = \"completed\"\n"
            "  /\\ agentTerminated' = TRUE\n",
            "  /\\ publishedArtifacts' = publishedArtifacts \\union {agentIntent}\n"
            "  \\* MUTANT: Artifact visibility is split from Task completion.\n"
            "  /\\ agentTask' = agentTask\n"
            "  /\\ agentTerminated' = agentTerminated\n",
        )],
    ),
    "system_race_agent_wrong_base_succeeds": (
        "HumanSystem.tla",
        [replacement(
            "RaceApplyAgentSuccess ==\n"
            "  /\\ workspaceVersion = agentIntentBase\n"
            "  /\\ ApplyAgentWrite\n",
            "RaceApplyAgentSuccess ==\n"
            "  \\* MUTANT: the race-only Agent CAS ignores its frozen base\n"
            "  /\\ ApplyAgentWrite\n",
        )],
    ),
}


def replace_once(text: str, old: str, new: str, mutant: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"{mutant}: expected exactly one replacement site, found {count}"
        )
    return text.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 3 or sys.argv[1] not in MUTANTS:
        names = "\n  ".join(sorted(MUTANTS))
        raise SystemExit(f"usage: {sys.argv[0]} MUTANT OUTPUT_DIR\n  {names}")

    mutant = sys.argv[1]
    output = Path(sys.argv[2]).resolve()
    source_name, edits = MUTANTS[mutant]
    module_name = "Mutant" + "".join(part.title() for part in mutant.split("_"))
    source = (ROOT / source_name).read_text(encoding="utf-8")
    source = replace_once(
        source,
        f"MODULE {Path(source_name).stem}",
        f"MODULE {module_name}",
        mutant,
    )
    for old, new in edits:
        source = replace_once(source, old, new, mutant)

    output.mkdir(parents=True, exist_ok=True)
    (output / f"{module_name}.tla").write_text(source, encoding="utf-8")
    shutil.copy2(ROOT / "HumanCommon.tla", output / "HumanCommon.tla")
    print(output / f"{module_name}.tla")


if __name__ == "__main__":
    main()
