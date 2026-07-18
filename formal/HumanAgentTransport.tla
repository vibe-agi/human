---------------------- MODULE HumanAgentTransport ----------------------
(***************************************************************************)
(* HumanAgent's worker transport and commit-time authority boundary.       *)
(*                                                                         *)
(* This is deliberately separate from HumanAgent.tla. That module owns the *)
(* public Task/Message/Artifact lifecycle; this module owns durable worker  *)
(* grants, delivery replay, and the transaction boundary where an Agent     *)
(* mutation becomes authoritative. It is also separate from HumanRuntime's  *)
(* completion-oriented session model: an Agent grant is explicitly fenced, *)
(* has no wall-clock expiry, and may span input_required/reply turns.        *)
(*                                                                         *)
(* A worker envelope carries the authenticated worker plus the complete     *)
(* authority/workspace/task identity, fence, expected Task revision, op,    *)
(* command id/digest, and transport delivery id. Prepare is deliberately    *)
(* non-authoritative: FenceGrant, CallerReply, or another committed command *)
(* may run before CommitPrepared. The current grant and revision are checked*)
(* again in the same abstract action that records both effect and command   *)
(* receipt. Exact committed-command replay is decided before those current  *)
(* checks, so a retry can be ACKed after fencing without reapplying effect.  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Authorities,
  Workspaces,
  TaskIDs,
  Workers,
  DeliveryIDs,
  CommandIDs,
  Digests,
  MaxFence,
  MaxRevision,
  MaxOutbox,
  MaxFaults

ASSUME
  /\ IsFiniteSet(Authorities) /\ Authorities # {}
  /\ IsFiniteSet(Workspaces) /\ Workspaces # {}
  /\ IsFiniteSet(TaskIDs) /\ TaskIDs # {}
  /\ IsFiniteSet(Workers) /\ Workers # {}
  /\ IsFiniteSet(DeliveryIDs) /\ DeliveryIDs # {}
  /\ IsFiniteSet(CommandIDs) /\ CommandIDs # {}
  /\ IsFiniteSet(Digests) /\ Digests # {}
  /\ MaxFence \in Nat \ {0}
  /\ MaxRevision \in Nat /\ MaxRevision >= 2
  /\ MaxOutbox \in Nat \ {0}
  /\ MaxFaults \in Nat

NoWorker     == "__no_worker__"
Submitted    == "submitted"
Working      == "working"
InputNeeded  == "input_required"
Completed    == "completed"
Canceled     == "canceled"
Failed       == "failed"
TaskStates   == {Submitted, Working, InputNeeded, Completed, Canceled, Failed}
TerminalStates == {Completed, Canceled, Failed}

AcceptOp       == "accept"
RequestInputOp == "request_input"
CompleteOp     == "complete"
Operations     == {AcceptOp, RequestInputOp, CompleteOp}

TaskRef(authority, workspace, task) ==
  [authority |-> authority, workspace |-> workspace, task |-> task]

TaskRefs ==
  {TaskRef(authority, workspace, task) :
     authority \in Authorities,
     workspace \in Workspaces,
     task \in TaskIDs}

Grant(ref, worker, fence) ==
  [authority |-> ref.authority,
   workspace |-> ref.workspace,
   task      |-> ref.task,
   worker    |-> worker,
   fence     |-> fence]

Grants ==
  {Grant(ref, worker, fence) :
     ref \in TaskRefs, worker \in Workers, fence \in 1..MaxFence}

Envelope(ref, delivery, command, digest, worker, fence, expected, op) ==
  [authority         |-> ref.authority,
   workspace         |-> ref.workspace,
   task              |-> ref.task,
   delivery          |-> delivery,
   command           |-> command,
   digest            |-> digest,
   worker            |-> worker,
   fence             |-> fence,
   expected_revision |-> expected,
   op                |-> op]

Envelopes ==
  {Envelope(ref, delivery, command, digest, worker, fence, expected, op) :
     ref \in TaskRefs,
     delivery \in DeliveryIDs,
     command \in CommandIDs,
     digest \in Digests,
     worker \in Workers,
     fence \in 1..MaxFence,
     expected \in 1..MaxRevision,
     op \in Operations}

EnvelopeTask(envelope) ==
  TaskRef(envelope.authority, envelope.workspace, envelope.task)

EnvelopeGrant(envelope) ==
  Grant(EnvelopeTask(envelope), envelope.worker, envelope.fence)

(* A delivery id belongs to the authenticated worker's durable outbox.      *)
DeliveryKey(envelope) ==
  [authority |-> envelope.authority,
   worker    |-> envelope.worker,
   delivery  |-> envelope.delivery]

(* Command ids are authority scoped in the Agent command ledger.            *)
CommandKey(envelope) ==
  [authority |-> envelope.authority, command |-> envelope.command]

(* delivery is intentionally absent: a reconnect/retry can wrap the exact   *)
(* same durable command in a fresh transport delivery.                       *)
CommandInput(envelope) ==
  [authority         |-> envelope.authority,
   workspace         |-> envelope.workspace,
   task              |-> envelope.task,
   command           |-> envelope.command,
   digest            |-> envelope.digest,
   worker            |-> envelope.worker,
   fence             |-> envelope.fence,
   expected_revision |-> envelope.expected_revision,
   op                |-> envelope.op]

CommandInputs == {CommandInput(envelope) : envelope \in Envelopes}

CommitFact(command, owner, fence, revision, state) ==
  [command  |-> command,
   owner    |-> owner,
   fence    |-> fence,
   revision |-> revision,
   state    |-> state]

CommitFacts ==
  {CommitFact(command, owner, fence, revision, state) :
     command \in CommandInputs,
     owner \in Workers \union {NoWorker},
     fence \in 0..MaxFence,
     revision \in 1..MaxRevision,
     state \in TaskStates}

VARIABLES
  (* Durable Task state and no-clock fenced authority. *)
  taskState,
  taskRevision,
  leaseOwner,
  leaseFence,
  grantPrepared,
  durableGrants,
  visibleGrants,

  (* Durable worker outbox; wire/prepare/check snapshots are volatile. *)
  outbox,
  wire,
  prepared,
  grantPrechecked,
  revisionPrechecked,

  (* Domain command receipt/effect and delivery receipt are separate. *)
  effects,
  commandReceipts,
  commitFacts,
  effectCount,
  deliveryAccepted,
  deliveryRejected,
  replayedDeliveries,
  replayedAfterFence,
  ackWire,
  nackWire,
  settled,

  (* Finite-fault environment. *)
  gatewayUp,
  workerUp,
  linkUp,
  faultsLeft

taskVars ==
  <<taskState, taskRevision, leaseOwner, leaseFence,
    grantPrepared, durableGrants, visibleGrants>>

deliveryVars ==
  <<outbox, wire, prepared, grantPrechecked, revisionPrechecked,
    effects, commandReceipts, commitFacts, effectCount,
    deliveryAccepted, deliveryRejected, replayedDeliveries,
    replayedAfterFence, ackWire, nackWire, settled>>

environmentVars == <<gatewayUp, workerUp, linkUp, faultsLeft>>

vars == <<taskVars, deliveryVars, environmentVars>>

DurableVars ==
  <<taskState, taskRevision, leaseOwner, leaseFence, durableGrants,
    visibleGrants, outbox, effects, commandReceipts, commitFacts, effectCount,
    deliveryAccepted, deliveryRejected, replayedDeliveries,
    replayedAfterFence, settled>>

CurrentGrant(ref) == Grant(ref, leaseOwner[ref], leaseFence[ref])

GrantCurrent(envelope) ==
  /\ leaseOwner[EnvelopeTask(envelope)] = envelope.worker
  /\ leaseFence[EnvelopeTask(envelope)] = envelope.fence

RevisionCurrent(envelope) ==
  taskRevision[EnvelopeTask(envelope)] = envelope.expected_revision

CommitGrantCurrent(envelope) == GrantCurrent(envelope)
CommitRevisionCurrent(envelope) == RevisionCurrent(envelope)
ReplayAuthorization(envelope) == TRUE

ReceiptsForCommand(envelope) ==
  {receipt \in commandReceipts :
     CommandKey(receipt) = CommandKey(envelope)}

ExactCommitted(envelope) ==
  CommandInput(envelope) \in commandReceipts

ConflictingCommitted(envelope) ==
  \E receipt \in commandReceipts :
    /\ CommandKey(receipt) = CommandKey(envelope)
    /\ receipt # CommandInput(envelope)

DeliveryDecided(envelope) ==
  \E decided \in deliveryAccepted \union deliveryRejected :
    DeliveryKey(decided) = DeliveryKey(envelope)

OperationEnabled(envelope) ==
  LET ref == EnvelopeTask(envelope)
  IN \/ /\ envelope.op = AcceptOp
           /\ taskState[ref] = Submitted
     \/ /\ envelope.op = RequestInputOp
           /\ taskState[ref] = Working
     \/ /\ envelope.op = CompleteOp
           /\ taskState[ref] = Working

NextTaskState(envelope) ==
  IF envelope.op = AcceptOp THEN Working
  ELSE IF envelope.op = RequestInputOp THEN InputNeeded
  ELSE Completed

TerminalOperation(envelope) == NextTaskState(envelope) \in TerminalStates

CommitAuthorized(envelope) ==
  /\ CommitGrantCurrent(envelope)
  /\ CommitRevisionCurrent(envelope)
  /\ OperationEnabled(envelope)
  /\ taskRevision[EnvelopeTask(envelope)] < MaxRevision

Init ==
  /\ taskState = [ref \in TaskRefs |-> Submitted]
  /\ taskRevision = [ref \in TaskRefs |-> 1]
  /\ leaseOwner = [ref \in TaskRefs |-> NoWorker]
  /\ leaseFence = [ref \in TaskRefs |-> 0]
  /\ grantPrepared = {}
  /\ durableGrants = {}
  /\ visibleGrants = {}
  /\ outbox = {}
  /\ wire = {}
  /\ prepared = {}
  /\ grantPrechecked = {}
  /\ revisionPrechecked = {}
  /\ effects = {}
  /\ commandReceipts = {}
  /\ commitFacts = {}
  /\ effectCount = [command \in CommandInputs |-> 0]
  /\ deliveryAccepted = {}
  /\ deliveryRejected = {}
  /\ replayedDeliveries = {}
  /\ replayedAfterFence = {}
  /\ ackWire = {}
  /\ nackWire = {}
  /\ settled = {}
  /\ gatewayUp = TRUE
  /\ workerUp = TRUE
  /\ linkUp = TRUE
  /\ faultsLeft = MaxFaults

(***************************************************************************)
(* Grant transaction. grantPrepared represents an internal precommit value; *)
(* only durableGrants may become externally visible.                        *)
(***************************************************************************)

BeginAcquire(ref, worker) ==
  /\ taskState[ref] \notin TerminalStates
  /\ leaseOwner[ref] = NoWorker
  /\ leaseFence[ref] < MaxFence
  /\ gatewayUp
  /\ LET grant == Grant(ref, worker, leaseFence[ref] + 1)
     IN /\ grant \notin grantPrepared
        /\ grantPrepared' = grantPrepared \union {grant}
  /\ UNCHANGED <<taskState, taskRevision, leaseOwner, leaseFence,
                  durableGrants, visibleGrants, deliveryVars,
                  environmentVars>>

CommitGrant(grant) ==
  LET ref == TaskRef(grant.authority, grant.workspace, grant.task)
  IN /\ grant \in grantPrepared
     /\ taskState[ref] \notin TerminalStates
     /\ leaseOwner[ref] = NoWorker
     /\ grant.fence = leaseFence[ref] + 1
     /\ gatewayUp
     /\ leaseOwner' = [leaseOwner EXCEPT ![ref] = grant.worker]
     /\ leaseFence' = [leaseFence EXCEPT ![ref] = grant.fence]
     /\ durableGrants' = durableGrants \union {grant}
     /\ grantPrepared' = grantPrepared \ {grant}
     /\ UNCHANGED <<taskState, taskRevision, visibleGrants,
                     deliveryVars, environmentVars>>

ExposeGrant(grant) ==
  /\ grant \in durableGrants \ visibleGrants
  /\ grant = CurrentGrant(TaskRef(grant.authority, grant.workspace, grant.task))
  /\ gatewayUp /\ workerUp /\ linkUp
  /\ visibleGrants' = visibleGrants \union {grant}
  /\ UNCHANGED <<taskState, taskRevision, leaseOwner, leaseFence,
                  grantPrepared, durableGrants, deliveryVars,
                  environmentVars>>

FenceGrant(grant) ==
  LET ref == TaskRef(grant.authority, grant.workspace, grant.task)
  IN /\ grant = CurrentGrant(ref)
     /\ taskState[ref] \notin TerminalStates
     /\ leaseOwner' = [leaseOwner EXCEPT ![ref] = NoWorker]
     /\ UNCHANGED <<taskState, taskRevision, leaseFence, grantPrepared,
                     durableGrants, visibleGrants, deliveryVars,
                     environmentVars>>

(***************************************************************************)
(* Durable worker outbox and volatile delivery/prepare phase.               *)
(***************************************************************************)

WorkerEnqueue(envelope) ==
  /\ EnvelopeGrant(envelope) \in visibleGrants
  /\ Cardinality(outbox) < MaxOutbox
  /\ ~DeliveryDecided(envelope)
  /\ \A existing \in outbox :
       DeliveryKey(existing) # DeliveryKey(envelope)
  /\ outbox' = outbox \union {envelope}
  /\ UNCHANGED <<taskVars, wire, prepared, grantPrechecked,
                  revisionPrechecked, effects, commandReceipts, commitFacts,
                  effectCount, deliveryAccepted, deliveryRejected,
                  replayedDeliveries, replayedAfterFence,
                  ackWire, nackWire, settled, environmentVars>>

Transmit(envelope) ==
  /\ envelope \in outbox
  /\ envelope \notin wire
  /\ gatewayUp /\ workerUp /\ linkUp
  /\ wire' = wire \union {envelope}
  /\ UNCHANGED <<taskVars, outbox, prepared, grantPrechecked,
                  revisionPrechecked, effects, commandReceipts, commitFacts,
                  effectCount, deliveryAccepted, deliveryRejected,
                  replayedDeliveries, replayedAfterFence,
                  ackWire, nackWire, settled, environmentVars>>

Prepare(envelope) ==
  /\ envelope \in wire
  /\ envelope \notin prepared
  /\ ~DeliveryDecided(envelope)
  /\ gatewayUp
  /\ prepared' = prepared \union {envelope}
  /\ grantPrechecked' =
       IF GrantCurrent(envelope)
       THEN grantPrechecked \union {envelope}
       ELSE grantPrechecked
  /\ revisionPrechecked' =
       IF RevisionCurrent(envelope)
       THEN revisionPrechecked \union {envelope}
       ELSE revisionPrechecked
  /\ UNCHANGED <<taskVars, outbox, wire, effects, commandReceipts,
                  commitFacts, effectCount, deliveryAccepted,
                  deliveryRejected, replayedDeliveries, replayedAfterFence,
                  ackWire, nackWire, settled, environmentVars>>

CommitPrepared(envelope) ==
  LET ref == EnvelopeTask(envelope)
      command == CommandInput(envelope)
      fact == CommitFact(command, leaseOwner[ref], leaseFence[ref],
                         taskRevision[ref], taskState[ref])
  IN /\ envelope \in prepared
     /\ ~ExactCommitted(envelope)
     /\ ~ConflictingCommitted(envelope)
     /\ CommitAuthorized(envelope)
     /\ taskState' = [taskState EXCEPT ![ref] = NextTaskState(envelope)]
     /\ taskRevision' = [taskRevision EXCEPT ![ref] = @ + 1]
     /\ leaseOwner' =
          IF TerminalOperation(envelope)
          THEN [leaseOwner EXCEPT ![ref] = NoWorker]
          ELSE leaseOwner
     /\ effects' = effects \union {command}
     /\ commandReceipts' = commandReceipts \union {command}
     /\ commitFacts' = commitFacts \union {fact}
     /\ effectCount' = [effectCount EXCEPT ![command] = @ + 1]
     /\ deliveryAccepted' = deliveryAccepted \union {envelope}
     /\ prepared' = prepared \ {envelope}
     /\ grantPrechecked' = grantPrechecked \ {envelope}
     /\ revisionPrechecked' = revisionPrechecked \ {envelope}
     /\ UNCHANGED <<leaseFence, grantPrepared, durableGrants, visibleGrants,
                     outbox, wire, deliveryRejected, replayedDeliveries,
                     replayedAfterFence, ackWire, nackWire, settled,
                     environmentVars>>

ReplayPrepared(envelope) ==
  /\ envelope \in prepared
  /\ ExactCommitted(envelope)
  /\ IF ReplayAuthorization(envelope)
     THEN /\ deliveryAccepted' = deliveryAccepted \union {envelope}
          /\ deliveryRejected' = deliveryRejected
          /\ replayedDeliveries' = replayedDeliveries \union {envelope}
          /\ replayedAfterFence' =
               IF GrantCurrent(envelope)
               THEN replayedAfterFence
               ELSE replayedAfterFence \union {envelope}
     ELSE /\ deliveryAccepted' = deliveryAccepted
          /\ deliveryRejected' = deliveryRejected \union {envelope}
          /\ replayedDeliveries' = replayedDeliveries \union {envelope}
          /\ replayedAfterFence' = replayedAfterFence
  /\ prepared' = prepared \ {envelope}
  /\ grantPrechecked' = grantPrechecked \ {envelope}
  /\ revisionPrechecked' = revisionPrechecked \ {envelope}
  /\ UNCHANGED <<taskVars, outbox, wire, effects, commandReceipts,
                  commitFacts, effectCount, ackWire, nackWire, settled,
                  environmentVars>>

RejectPrepared(envelope) ==
  /\ envelope \in prepared
  /\ ~ExactCommitted(envelope)
  /\ \/ ConflictingCommitted(envelope)
     \/ ~CommitAuthorized(envelope)
  /\ deliveryRejected' = deliveryRejected \union {envelope}
  /\ prepared' = prepared \ {envelope}
  /\ grantPrechecked' = grantPrechecked \ {envelope}
  /\ revisionPrechecked' = revisionPrechecked \ {envelope}
  /\ UNCHANGED <<taskVars, outbox, wire, effects, commandReceipts,
                  commitFacts, effectCount, deliveryAccepted,
                  replayedDeliveries, replayedAfterFence,
                  ackWire, nackWire, settled, environmentVars>>

ProcessPrepared(envelope) ==
  \/ CommitPrepared(envelope)
  \/ ReplayPrepared(envelope)
  \/ RejectPrepared(envelope)

(***************************************************************************)
(* Delivery ACK/NACK follows its own durable decision. It never stands in   *)
(* for the domain command receipt. Client dequeue and settled history are    *)
(* one durable action, preventing a rejected head from poisoning followers. *)
(***************************************************************************)

SendACK(envelope) ==
  /\ envelope \in deliveryAccepted \cap outbox
  /\ envelope \notin ackWire
  /\ gatewayUp /\ workerUp /\ linkUp
  /\ ackWire' = ackWire \union {envelope}
  /\ UNCHANGED <<taskVars, outbox, wire, prepared, grantPrechecked,
                  revisionPrechecked, effects, commandReceipts, commitFacts,
                  effectCount, deliveryAccepted, deliveryRejected,
                  replayedDeliveries, replayedAfterFence,
                  nackWire, settled, environmentVars>>

SendNACK(envelope) ==
  /\ envelope \in deliveryRejected \cap outbox
  /\ envelope \notin nackWire
  /\ gatewayUp /\ workerUp /\ linkUp
  /\ nackWire' = nackWire \union {envelope}
  /\ UNCHANGED <<taskVars, outbox, wire, prepared, grantPrechecked,
                  revisionPrechecked, effects, commandReceipts, commitFacts,
                  effectCount, deliveryAccepted, deliveryRejected,
                  replayedDeliveries, replayedAfterFence,
                  ackWire, settled, environmentVars>>

ConsumeACK(envelope) ==
  /\ envelope \in ackWire \cap outbox
  /\ outbox' = outbox \ {envelope}
  /\ ackWire' = ackWire \ {envelope}
  /\ settled' = settled \union {envelope}
  /\ UNCHANGED <<taskVars, wire, prepared, grantPrechecked,
                  revisionPrechecked, effects, commandReceipts, commitFacts,
                  effectCount, deliveryAccepted, deliveryRejected,
                  replayedDeliveries, replayedAfterFence,
                  nackWire, environmentVars>>

ConsumeNACK(envelope) ==
  /\ envelope \in nackWire \cap outbox
  /\ outbox' = outbox \ {envelope}
  /\ nackWire' = nackWire \ {envelope}
  /\ settled' = settled \union {envelope}
  /\ UNCHANGED <<taskVars, wire, prepared, grantPrechecked,
                  revisionPrechecked, effects, commandReceipts, commitFacts,
                  effectCount, deliveryAccepted, deliveryRejected,
                  replayedDeliveries, replayedAfterFence,
                  ackWire, environmentVars>>

(***************************************************************************)
(* Caller lifecycle actions. input_required/reply retains the same grant;    *)
(* terminal completion/cancel clears owner but never rewinds the fence.      *)
(***************************************************************************)

CallerReply(ref) ==
  /\ taskState[ref] = InputNeeded
  /\ taskRevision[ref] < MaxRevision
  /\ taskState' = [taskState EXCEPT ![ref] = Working]
  /\ taskRevision' = [taskRevision EXCEPT ![ref] = @ + 1]
  /\ UNCHANGED <<leaseOwner, leaseFence, grantPrepared, durableGrants,
                  visibleGrants, deliveryVars, environmentVars>>

CallerCancel(ref) ==
  /\ taskState[ref] \notin TerminalStates
  /\ taskRevision[ref] < MaxRevision
  /\ taskState' = [taskState EXCEPT ![ref] = Canceled]
  /\ taskRevision' = [taskRevision EXCEPT ![ref] = @ + 1]
  /\ leaseOwner' = [leaseOwner EXCEPT ![ref] = NoWorker]
  /\ UNCHANGED <<leaseFence, grantPrepared, durableGrants, visibleGrants,
                  deliveryVars, environmentVars>>

(***************************************************************************)
(* Finite loss/crash actions. Only volatile network/prepare state is lost.   *)
(***************************************************************************)

DropWire(envelope) ==
  /\ envelope \in wire
  /\ faultsLeft > 0
  /\ wire' = wire \ {envelope}
  /\ prepared' = prepared \ {envelope}
  /\ grantPrechecked' = grantPrechecked \ {envelope}
  /\ revisionPrechecked' = revisionPrechecked \ {envelope}
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<taskVars, outbox, effects, commandReceipts, commitFacts,
                  effectCount, deliveryAccepted, deliveryRejected,
                  replayedDeliveries, replayedAfterFence,
                  ackWire, nackWire, settled, gatewayUp, workerUp, linkUp>>

CrashGateway ==
  /\ gatewayUp
  /\ faultsLeft > 0
  /\ gatewayUp' = FALSE
  /\ wire' = {}
  /\ prepared' = {}
  /\ grantPrechecked' = {}
  /\ revisionPrechecked' = {}
  /\ ackWire' = {}
  /\ nackWire' = {}
  /\ grantPrepared' = {}
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<taskState, taskRevision, leaseOwner, leaseFence,
                  durableGrants, visibleGrants, outbox, effects,
                  commandReceipts, commitFacts, effectCount,
                  deliveryAccepted, deliveryRejected, replayedDeliveries,
                  replayedAfterFence, settled, workerUp, linkUp>>

RestartGateway ==
  /\ ~gatewayUp
  /\ gatewayUp' = TRUE
  /\ UNCHANGED <<taskVars, deliveryVars, workerUp, linkUp, faultsLeft>>

CrashWorker ==
  /\ workerUp
  /\ faultsLeft > 0
  /\ workerUp' = FALSE
  /\ wire' = {}
  /\ ackWire' = {}
  /\ nackWire' = {}
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<taskVars, outbox, prepared, grantPrechecked,
                  revisionPrechecked, effects, commandReceipts, commitFacts,
                  effectCount, deliveryAccepted, deliveryRejected,
                  replayedDeliveries, replayedAfterFence,
                  settled, gatewayUp, linkUp>>

RestartWorker ==
  /\ ~workerUp
  /\ workerUp' = TRUE
  /\ UNCHANGED <<taskVars, deliveryVars, gatewayUp, linkUp, faultsLeft>>

BreakLink ==
  /\ linkUp
  /\ faultsLeft > 0
  /\ linkUp' = FALSE
  /\ wire' = {}
  /\ ackWire' = {}
  /\ nackWire' = {}
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<taskVars, outbox, prepared, grantPrechecked,
                  revisionPrechecked, effects, commandReceipts, commitFacts,
                  effectCount, deliveryAccepted, deliveryRejected,
                  replayedDeliveries, replayedAfterFence,
                  settled, gatewayUp, workerUp>>

RestoreLink ==
  /\ ~linkUp
  /\ linkUp' = TRUE
  /\ UNCHANGED <<taskVars, deliveryVars, gatewayUp, workerUp, faultsLeft>>

Idle == UNCHANGED vars

Next ==
  \/ \E ref \in TaskRefs, worker \in Workers : BeginAcquire(ref, worker)
  \/ \E grant \in Grants : CommitGrant(grant)
  \/ \E grant \in Grants : ExposeGrant(grant)
  \/ \E grant \in Grants : FenceGrant(grant)
  \/ \E envelope \in Envelopes : WorkerEnqueue(envelope)
  \/ \E envelope \in Envelopes : Transmit(envelope)
  \/ \E envelope \in Envelopes : Prepare(envelope)
  \/ \E envelope \in Envelopes : ProcessPrepared(envelope)
  \/ \E envelope \in Envelopes : SendACK(envelope)
  \/ \E envelope \in Envelopes : SendNACK(envelope)
  \/ \E envelope \in Envelopes : ConsumeACK(envelope)
  \/ \E envelope \in Envelopes : ConsumeNACK(envelope)
  \/ \E ref \in TaskRefs : CallerReply(ref)
  \/ \E ref \in TaskRefs : CallerCancel(ref)
  \/ \E envelope \in Envelopes : DropWire(envelope)
  \/ CrashGateway
  \/ RestartGateway
  \/ CrashWorker
  \/ RestartWorker
  \/ BreakLink
  \/ RestoreLink
  \/ Idle

Spec == Init /\ [][Next]_vars

TransportFairness ==
  /\ \A envelope \in Envelopes :
       /\ WF_vars(Transmit(envelope))
       /\ WF_vars(Prepare(envelope))
       /\ WF_vars(ProcessPrepared(envelope))
       /\ WF_vars(SendACK(envelope))
       /\ WF_vars(SendNACK(envelope))
       /\ WF_vars(ConsumeACK(envelope))
       /\ WF_vars(ConsumeNACK(envelope))
  /\ WF_vars(RestartGateway)
  /\ WF_vars(RestartWorker)
  /\ WF_vars(RestoreLink)

LiveSpec == Spec /\ TransportFairness

(***************************************************************************)
(* Safety and liveness oracles.                                             *)
(***************************************************************************)

TypeOK ==
  /\ taskState \in [TaskRefs -> TaskStates]
  /\ taskRevision \in [TaskRefs -> 1..MaxRevision]
  /\ leaseOwner \in [TaskRefs -> Workers \union {NoWorker}]
  /\ leaseFence \in [TaskRefs -> 0..MaxFence]
  /\ grantPrepared \subseteq Grants
  /\ durableGrants \subseteq Grants
  /\ visibleGrants \subseteq Grants
  /\ outbox \subseteq Envelopes
  /\ wire \subseteq Envelopes
  /\ prepared \subseteq Envelopes
  /\ grantPrechecked \subseteq Envelopes
  /\ revisionPrechecked \subseteq Envelopes
  /\ effects \subseteq CommandInputs
  /\ commandReceipts \subseteq CommandInputs
  /\ commitFacts \subseteq CommitFacts
  /\ effectCount \in [CommandInputs -> 0..2]
  /\ deliveryAccepted \subseteq Envelopes
  /\ deliveryRejected \subseteq Envelopes
  /\ replayedDeliveries \subseteq Envelopes
  /\ replayedAfterFence \subseteq Envelopes
  /\ ackWire \subseteq Envelopes
  /\ nackWire \subseteq Envelopes
  /\ settled \subseteq Envelopes
  /\ gatewayUp \in BOOLEAN
  /\ workerUp \in BOOLEAN
  /\ linkUp \in BOOLEAN
  /\ faultsLeft \in 0..MaxFaults

VisibleGrantIsDurable == visibleGrants \subseteq durableGrants

FenceHistoryMonotone ==
  /\ \A grant \in durableGrants :
       grant.fence <= leaseFence[
         TaskRef(grant.authority, grant.workspace, grant.task)]
  /\ \A ref \in TaskRefs :
       leaseFence[ref] = 0 <=>
         {grant \in durableGrants :
            TaskRef(grant.authority, grant.workspace, grant.task) = ref} = {}

GrantGenerationUnique ==
  \A left, right \in durableGrants :
    /\ left.authority = right.authority
    /\ left.workspace = right.workspace
    /\ left.task = right.task
    /\ left.fence = right.fence
    => left = right

TerminalTaskHasNoActiveGrant ==
  \A ref \in TaskRefs :
    taskState[ref] \in TerminalStates => leaseOwner[ref] = NoWorker

CommandReceiptUnique ==
  \A left, right \in commandReceipts :
    CommandKey(left) = CommandKey(right) => left = right

WorkerEffectAuthorizedAtCommit ==
  \A command \in effects :
    \E fact \in commitFacts :
      /\ fact.command = command
      /\ fact.owner = command.worker
      /\ fact.fence = command.fence

TaskRevisionCheckedAtCommit ==
  \A command \in effects :
    \E fact \in commitFacts :
      /\ fact.command = command
      /\ fact.revision = command.expected_revision

EffectAndCommandReceiptAtomic == effects = commandReceipts

EffectAppliedExactlyOnce ==
  \A command \in CommandInputs :
    effectCount[command] = IF command \in effects THEN 1 ELSE 0

AcceptedDeliveryHasExactCommand ==
  \A envelope \in deliveryAccepted :
    CommandInput(envelope) \in commandReceipts

ExactReplayNeverRejected ==
  replayedDeliveries \cap deliveryRejected = {}

DeliveryDecisionExclusive ==
  /\ deliveryAccepted \cap deliveryRejected = {}
  /\ \A left, right \in deliveryAccepted \union deliveryRejected :
       DeliveryKey(left) = DeliveryKey(right) => left = right

ACKAfterDurableDecision ==
  /\ ackWire \subseteq deliveryAccepted
  /\ nackWire \subseteq deliveryRejected

SettledAfterDurableDecision ==
  /\ settled \subseteq deliveryAccepted \union deliveryRejected
  /\ settled \cap outbox = {}

CommitFactExact ==
  /\ \A command \in effects :
       \E fact \in commitFacts : fact.command = command
  /\ \A fact \in commitFacts : fact.command \in effects

TransportSafety ==
  /\ TypeOK
  /\ VisibleGrantIsDurable
  /\ FenceHistoryMonotone
  /\ GrantGenerationUnique
  /\ TerminalTaskHasNoActiveGrant
  /\ CommandReceiptUnique
  /\ WorkerEffectAuthorizedAtCommit
  /\ TaskRevisionCheckedAtCommit
  /\ EffectAndCommandReceiptAtomic
  /\ EffectAppliedExactlyOnce
  /\ AcceptedDeliveryHasExactCommand
  /\ ExactReplayNeverRejected
  /\ DeliveryDecisionExclusive
  /\ ACKAfterDurableDecision
  /\ SettledAfterDurableDecision
  /\ CommitFactExact

OutboxEventuallySettles ==
  \A envelope \in Envelopes : envelope \in outbox ~> envelope \in settled

ExactReplayEventuallySettles ==
  \A envelope \in Envelopes :
    /\ envelope \in outbox
    /\ CommandInput(envelope) \in commandReceipts
    ~> envelope \in settled

NoReplayAfterFenceWitness == [] (replayedAfterFence = {})
NoInputRoundWitness ==
  [] (\A ref \in TaskRefs : taskState[ref] # InputNeeded)

=============================================================================
