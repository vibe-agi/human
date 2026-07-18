--------------------------- MODULE HumanRuntime ---------------------------
(***************************************************************************)
(* Protocol-neutral runtime shared by HumanLLM and HumanAgent.             *)
(*                                                                         *)
(* This model intentionally abstracts message bodies and file trees to     *)
(* finite digests and monotonically increasing workspace versions. Network *)
(* packets are an unordered set: any packet may be dropped, packets may be *)
(* processed in any order, and an outbox entry may be transmitted again    *)
(* before its ACK/NACK arrives. That is sufficient to model drop, reorder, *)
(* and duplicate delivery without an unbounded packet sequence.            *)
(*                                                                         *)
(* Durable variables survive every crash action. wire/ackWire/nackWire and  *)
(* service/link availability are volatile. visible* variables are history  *)
(* variables: a crash cannot make an observation that already escaped cease *)
(* to have happened.                                                       *)
(***************************************************************************)
EXTENDS HumanCommon, TLC

CONSTANTS
  ModeledSurfaces,
  Scopes,
  ItemIDs,
  EventIDs,
  Digests,
  Workers,
  Capacity,
  MaxFence,
  MaxVersion,
  MaxFaults

ASSUME
  /\ ModeledSurfaces \subseteq Surfaces /\ ModeledSurfaces # {}
  /\ IsFiniteSet(Scopes) /\ Scopes # {}
  /\ IsFiniteSet(ItemIDs) /\ ItemIDs # {}
  /\ IsFiniteSet(EventIDs) /\ EventIDs # {}
  /\ IsFiniteSet(Digests) /\ Digests # {}
  /\ IsFiniteSet(Workers) /\ Workers # {}
  /\ Capacity \in Nat \ {0}
  /\ MaxFence \in Nat \ {0}
  /\ MaxVersion \in Nat \ {0}
  /\ MaxFaults \in Nat

NoWorker == "no-worker"
ASSUME NoWorker \notin Workers

Items ==
  {ScopedKey(kind, scope, itemID) :
    kind \in ModeledSurfaces, scope \in Scopes, itemID \in ItemIDs}

(* An event id carries its parent item id. The outer key still carries the  *)
(* surface and workspace scope, so ids from the two public surfaces cannot  *)
(* alias one another.                                                       *)
EventKeys ==
  {ScopedKey(kind, scope, <<itemID, eventID>>) :
    kind \in ModeledSurfaces,
    scope \in Scopes,
    itemID \in ItemIDs,
    eventID \in EventIDs}

EventItem(eventKey) ==
  ScopedKey(KeyKind(eventKey), KeyScope(eventKey), KeyID(eventKey)[1])

LeaseGrant(item, worker, fence) ==
  [item |-> item, worker |-> worker, fence |-> fence]

LeaseGrants ==
  {LeaseGrant(item, worker, fence) :
    item \in Items, worker \in Workers, fence \in 1..MaxFence}

Envelope(eventKey, digest, worker, fence) ==
  [event |-> eventKey, digest |-> digest, worker |-> worker, fence |-> fence]

Envelopes ==
  {Envelope(eventKey, digest, worker, fence) :
    eventKey \in EventKeys,
    digest \in Digests,
    worker \in Workers,
    fence \in 1..MaxFence}

EnvelopeItem(envelope) == EventItem(envelope.event)

Proposal(item, baseVersion) ==
  [item |-> item, base |-> baseVersion, next |-> baseVersion + 1]

Proposals ==
  {Proposal(item, baseVersion) :
    item \in Items, baseVersion \in 0..(MaxVersion - 1)}

ProposalScope(proposal) == KeyScope(proposal.item)

VARIABLES
  (* Durable admission and fenced lease authority. *)
  admitted,
  queue,
  leaseOwner,
  leaseFence,
  leaseGrants,
  visibleAssignments,
  retired,
  durableOutcomes,
  visibleOutcomes,

  (* Durable worker outbox and gateway receipts; wire sets are volatile. *)
  outbox,
  enqueued,
  wire,
  effects,
  receipts,
  rejections,
  ackWire,
  nackWire,
  settled,

  (* Durable workspace CAS state plus externally observed versions. *)
  workspaceVersion,
  visibleWorkspaceVersion,
  pendingChanges,
  committedChanges,
  conflictedChanges,

  (* Failure environment. Only faultsLeft is durable model bookkeeping. *)
  callerUp,
  gatewayUp,
  workerUp,
  callerLinkUp,
  workerLinkUp,
  faultsLeft

lifecycleVars ==
  <<admitted, queue, leaseOwner, leaseFence, leaseGrants,
    visibleAssignments, retired, durableOutcomes, visibleOutcomes>>

transportVars ==
  <<outbox, enqueued, wire, effects, receipts, rejections,
    ackWire, nackWire, settled>>

workspaceVars ==
  <<workspaceVersion, visibleWorkspaceVersion, pendingChanges,
    committedChanges, conflictedChanges>>

environmentVars ==
  <<callerUp, gatewayUp, workerUp, callerLinkUp, workerLinkUp, faultsLeft>>

vars == <<lifecycleVars, transportVars, workspaceVars, environmentVars>>

durableVars ==
  <<lifecycleVars, outbox, enqueued, effects, receipts, rejections, settled,
    workspaceVars>>

ActiveItems ==
  {item \in Items : leaseOwner[item] # NoWorker /\ item \notin retired}

CurrentGrant(item) ==
  LeaseGrant(item, leaseOwner[item], leaseFence[item])

ReceiptFor(eventKey) ==
  {envelope \in receipts : envelope.event = eventKey}

RejectedFor(eventKey) ==
  {envelope \in rejections : envelope.event = eventKey}

EnvironmentReady ==
  callerUp /\ gatewayUp /\ workerUp /\ callerLinkUp /\ workerLinkUp

TypeOK ==
  /\ admitted \subseteq Items
  /\ queue \subseteq Items
  /\ leaseOwner \in [Items -> Workers \union {NoWorker}]
  /\ leaseFence \in [Items -> 0..MaxFence]
  /\ leaseGrants \subseteq LeaseGrants
  /\ visibleAssignments \subseteq LeaseGrants
  /\ retired \subseteq Items
  /\ durableOutcomes \subseteq Items
  /\ visibleOutcomes \subseteq Items
  /\ outbox \subseteq Envelopes
  /\ enqueued \subseteq Envelopes
  /\ wire \subseteq Envelopes
  /\ effects \subseteq Envelopes
  /\ receipts \subseteq Envelopes
  /\ rejections \subseteq Envelopes
  /\ ackWire \subseteq Envelopes
  /\ nackWire \subseteq Envelopes
  /\ settled \subseteq Envelopes
  /\ workspaceVersion \in [Scopes -> 0..MaxVersion]
  /\ visibleWorkspaceVersion \in [Scopes -> 0..MaxVersion]
  /\ pendingChanges \subseteq Proposals
  /\ committedChanges \subseteq Proposals
  /\ conflictedChanges \subseteq Proposals
  /\ callerUp \in BOOLEAN
  /\ gatewayUp \in BOOLEAN
  /\ workerUp \in BOOLEAN
  /\ callerLinkUp \in BOOLEAN
  /\ workerLinkUp \in BOOLEAN
  /\ faultsLeft \in 0..MaxFaults

Init ==
  /\ admitted = {}
  /\ queue = {}
  /\ leaseOwner = [item \in Items |-> NoWorker]
  /\ leaseFence = [item \in Items |-> 0]
  /\ leaseGrants = {}
  /\ visibleAssignments = {}
  /\ retired = {}
  /\ durableOutcomes = {}
  /\ visibleOutcomes = {}
  /\ outbox = {}
  /\ enqueued = {}
  /\ wire = {}
  /\ effects = {}
  /\ receipts = {}
  /\ rejections = {}
  /\ ackWire = {}
  /\ nackWire = {}
  /\ settled = {}
  /\ workspaceVersion = [scope \in Scopes |-> 0]
  /\ visibleWorkspaceVersion = [scope \in Scopes |-> 0]
  /\ pendingChanges = {}
  /\ committedChanges = {}
  /\ conflictedChanges = {}
  /\ callerUp = TRUE
  /\ gatewayUp = TRUE
  /\ workerUp = TRUE
  /\ callerLinkUp = TRUE
  /\ workerLinkUp = TRUE
  /\ faultsLeft = MaxFaults

(***************************************************************************)
(* Admission, durable lease, and external visibility.                     *)
(***************************************************************************)

Admit(item) ==
  /\ item \in Items \ admitted
  /\ gatewayUp
  /\ Cardinality(admitted \ retired) < Capacity
  /\ admitted' = admitted \union {item}
  /\ queue' = queue \union {item}
  /\ UNCHANGED <<leaseOwner, leaseFence, leaseGrants, visibleAssignments,
                  retired, durableOutcomes, visibleOutcomes,
                  transportVars, workspaceVars, environmentVars>>

AcquireLease(item, worker) ==
  /\ item \in queue
  /\ item \notin retired
  /\ leaseOwner[item] = NoWorker
  /\ leaseFence[item] < MaxFence
  /\ Cardinality(ActiveItems) < Capacity
  /\ gatewayUp /\ workerUp
  /\ LET nextFence == leaseFence[item] + 1
         grant == LeaseGrant(item, worker, nextFence)
     IN /\ leaseOwner' = [leaseOwner EXCEPT ![item] = worker]
        /\ leaseFence' = [leaseFence EXCEPT ![item] = nextFence]
        /\ leaseGrants' = leaseGrants \union {grant}
  /\ queue' = queue \ {item}
  /\ UNCHANGED <<admitted, visibleAssignments, retired,
                  durableOutcomes, visibleOutcomes,
                  transportVars, workspaceVars, environmentVars>>

ExposeAssignment(grant) ==
  /\ grant \in leaseGrants \ visibleAssignments
  /\ grant = CurrentGrant(grant.item)
  /\ gatewayUp /\ workerUp /\ workerLinkUp
  /\ visibleAssignments' = visibleAssignments \union {grant}
  /\ UNCHANGED <<admitted, queue, leaseOwner, leaseFence, leaseGrants,
                  retired, durableOutcomes, visibleOutcomes,
                  transportVars, workspaceVars, environmentVars>>

(* Fencing is durable. Reassignment, if any, must acquire a strictly newer *)
(* token. Existing outbox entries keep their original token and are NACKed. *)
FenceLease(item) ==
  /\ item \in admitted \ retired
  /\ leaseOwner[item] # NoWorker
  /\ leaseFence[item] < MaxFence
  /\ leaseOwner' = [leaseOwner EXCEPT ![item] = NoWorker]
  /\ queue' = queue \union {item}
  /\ UNCHANGED <<admitted, leaseFence, leaseGrants, visibleAssignments,
                  retired, durableOutcomes, visibleOutcomes,
                  transportVars, workspaceVars, environmentVars>>

Retire(item) ==
  /\ item \in admitted \ retired
  /\ durableOutcomes' = durableOutcomes \union {item}
  /\ retired' = retired \union {item}
  /\ queue' = queue \ {item}
  /\ leaseOwner' = [leaseOwner EXCEPT ![item] = NoWorker]
  /\ UNCHANGED <<admitted, leaseFence, leaseGrants, visibleAssignments,
                  visibleOutcomes, transportVars, workspaceVars, environmentVars>>

ExposeOutcome(item) ==
  /\ item \in durableOutcomes \ visibleOutcomes
  /\ callerUp /\ gatewayUp /\ callerLinkUp
  /\ visibleOutcomes' = visibleOutcomes \union {item}
  /\ UNCHANGED <<admitted, queue, leaseOwner, leaseFence, leaseGrants,
                  visibleAssignments, retired, durableOutcomes,
                  transportVars, workspaceVars, environmentVars>>

(***************************************************************************)
(* Durable outbox, unordered lossy wire, exact receipt, and ACK/NACK.      *)
(***************************************************************************)

EnqueueEvent(envelope) ==
  /\ envelope \in Envelopes
  /\ LET item == EnvelopeItem(envelope)
         grant == LeaseGrant(item, envelope.worker, envelope.fence)
     \* A disconnected worker knows only that this grant was once delivered.
     \* It may durably enqueue after the gateway has fenced/retired the item;
     \* current authority is checked only when the gateway processes the wire.
     IN /\ grant \in visibleAssignments
  /\ envelope \notin outbox \union settled \union receipts \union rejections
  /\ workerUp
  /\ outbox' = outbox \union {envelope}
  /\ enqueued' = enqueued \union {envelope}
  /\ UNCHANGED <<wire, effects, receipts, rejections, ackWire, nackWire,
                  settled, lifecycleVars, workspaceVars, environmentVars>>

Transmit(envelope) ==
  /\ envelope \in outbox
  /\ envelope \notin wire
  /\ workerUp /\ gatewayUp /\ workerLinkUp
  /\ wire' = wire \union {envelope}
  /\ UNCHANGED <<outbox, enqueued, effects, receipts, rejections,
                  ackWire, nackWire, settled,
                  lifecycleVars, workspaceVars, environmentVars>>

DropData(envelope) ==
  /\ faultsLeft > 0
  /\ envelope \in wire
  /\ wire' = wire \ {envelope}
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<outbox, enqueued, effects, receipts, rejections,
                  ackWire, nackWire, settled,
                  lifecycleVars, workspaceVars,
                  callerUp, gatewayUp, workerUp, callerLinkUp, workerLinkUp>>

ValidCurrentEnvelope(envelope) ==
  LET item == EnvelopeItem(envelope)
  IN /\ item \notin retired
     /\ leaseOwner[item] = envelope.worker
     /\ leaseFence[item] = envelope.fence
     /\ LeaseGrant(item, envelope.worker, envelope.fence) \in leaseGrants

CommitEvent(envelope) ==
  /\ envelope \in wire
  /\ gatewayUp /\ workerLinkUp
  /\ ValidCurrentEnvelope(envelope)
  /\ ReceiptFor(envelope.event) = {}
  /\ RejectedFor(envelope.event) = {}
  (* The effect and its durable replay receipt commit atomically. *)
  /\ effects' = effects \union {envelope}
  /\ receipts' = receipts \union {envelope}
  /\ wire' = wire \ {envelope}
  /\ UNCHANGED <<outbox, enqueued, rejections, ackWire, nackWire, settled,
                  lifecycleVars, workspaceVars, environmentVars>>

ReplayExact(envelope) ==
  /\ envelope \in wire
  /\ envelope \in receipts
  /\ gatewayUp /\ workerLinkUp
  /\ wire' = wire \ {envelope}
  /\ UNCHANGED <<outbox, enqueued, effects, receipts, rejections,
                  ackWire, nackWire, settled,
                  lifecycleVars, workspaceVars, environmentVars>>

RejectEvent(envelope) ==
  /\ envelope \in wire
  /\ gatewayUp /\ workerLinkUp
  /\ envelope \notin receipts
  /\ \/ ~ValidCurrentEnvelope(envelope)
     \/ ReceiptFor(envelope.event) # {}
     \/ RejectedFor(envelope.event) # {}
  /\ rejections' = rejections \union {envelope}
  /\ wire' = wire \ {envelope}
  /\ UNCHANGED <<outbox, enqueued, effects, receipts, ackWire, nackWire, settled,
                  lifecycleVars, workspaceVars, environmentVars>>

SendAck(envelope) ==
  /\ envelope \in receipts \cap outbox
  /\ envelope \notin ackWire
  /\ gatewayUp /\ workerUp /\ workerLinkUp
  /\ ackWire' = ackWire \union {envelope}
  /\ UNCHANGED <<outbox, enqueued, wire, effects, receipts, rejections,
                  nackWire, settled,
                  lifecycleVars, workspaceVars, environmentVars>>

SendNack(envelope) ==
  /\ envelope \in rejections \cap outbox
  /\ envelope \notin nackWire
  /\ gatewayUp /\ workerUp /\ workerLinkUp
  /\ nackWire' = nackWire \union {envelope}
  /\ UNCHANGED <<outbox, enqueued, wire, effects, receipts, rejections,
                  ackWire, settled,
                  lifecycleVars, workspaceVars, environmentVars>>

DropAck(envelope) ==
  /\ faultsLeft > 0
  /\ envelope \in ackWire \union nackWire
  /\ ackWire' = ackWire \ {envelope}
  /\ nackWire' = nackWire \ {envelope}
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<outbox, enqueued, wire, effects, receipts, rejections, settled,
                  lifecycleVars, workspaceVars,
                  callerUp, gatewayUp, workerUp, callerLinkUp, workerLinkUp>>

ReceiveReply(envelope) ==
  /\ envelope \in ackWire \union nackWire
  /\ workerUp /\ gatewayUp /\ workerLinkUp
  /\ outbox' = outbox \ {envelope}
  /\ wire' = wire \ {envelope}
  /\ ackWire' = ackWire \ {envelope}
  /\ nackWire' = nackWire \ {envelope}
  /\ settled' = settled \union {envelope}
  /\ UNCHANGED <<enqueued, effects, receipts, rejections,
                  lifecycleVars, workspaceVars, environmentVars>>

(***************************************************************************)
(* Shared workspace: drafts may race, but publication is a durable CAS.    *)
(***************************************************************************)

ProposeChange(proposal) ==
  /\ proposal \in Proposals
  /\ proposal.item \in admitted \ retired
  /\ proposal.base = workspaceVersion[ProposalScope(proposal)]
  /\ proposal \notin pendingChanges \union committedChanges \union conflictedChanges
  /\ pendingChanges' = pendingChanges \union {proposal}
  /\ UNCHANGED <<workspaceVersion, visibleWorkspaceVersion,
                  committedChanges, conflictedChanges,
                  lifecycleVars, transportVars, environmentVars>>

CommitChange(proposal) ==
  /\ proposal \in pendingChanges
  /\ callerUp /\ gatewayUp /\ callerLinkUp
  /\ proposal.base = workspaceVersion[ProposalScope(proposal)]
  /\ workspaceVersion' =
       [workspaceVersion EXCEPT ![ProposalScope(proposal)] = proposal.next]
  /\ pendingChanges' = pendingChanges \ {proposal}
  /\ committedChanges' = committedChanges \union {proposal}
  /\ UNCHANGED <<visibleWorkspaceVersion, conflictedChanges,
                  lifecycleVars, transportVars, environmentVars>>

ConflictChange(proposal) ==
  /\ proposal \in pendingChanges
  /\ callerUp /\ gatewayUp /\ callerLinkUp
  /\ proposal.base # workspaceVersion[ProposalScope(proposal)]
  /\ pendingChanges' = pendingChanges \ {proposal}
  /\ conflictedChanges' = conflictedChanges \union {proposal}
  /\ UNCHANGED <<workspaceVersion, visibleWorkspaceVersion, committedChanges,
                  lifecycleVars, transportVars, environmentVars>>

ExposeWorkspace(scope) ==
  /\ scope \in Scopes
  /\ visibleWorkspaceVersion[scope] < workspaceVersion[scope]
  /\ callerUp /\ callerLinkUp
  /\ visibleWorkspaceVersion' =
       [visibleWorkspaceVersion EXCEPT ![scope] = workspaceVersion[scope]]
  /\ UNCHANGED <<workspaceVersion, pendingChanges,
                  committedChanges, conflictedChanges,
                  lifecycleVars, transportVars, environmentVars>>

(***************************************************************************)
(* Caller, gateway, worker, and both one-way links fail independently.     *)
(***************************************************************************)

CallerCrash ==
  /\ faultsLeft > 0 /\ callerUp
  /\ callerUp' = FALSE
  /\ callerLinkUp' = FALSE
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<gatewayUp, workerUp, workerLinkUp,
                  lifecycleVars, transportVars, workspaceVars>>

GatewayCrash ==
  /\ faultsLeft > 0 /\ gatewayUp
  /\ gatewayUp' = FALSE
  /\ callerLinkUp' = FALSE
  /\ workerLinkUp' = FALSE
  /\ wire' = {}
  /\ ackWire' = {}
  /\ nackWire' = {}
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<callerUp, workerUp,
                  outbox, enqueued, effects, receipts, rejections, settled,
                  lifecycleVars, workspaceVars>>

WorkerCrash ==
  /\ faultsLeft > 0 /\ workerUp
  /\ workerUp' = FALSE
  /\ workerLinkUp' = FALSE
  /\ wire' = {}
  /\ ackWire' = {}
  /\ nackWire' = {}
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<callerUp, gatewayUp, callerLinkUp,
                  outbox, enqueued, effects, receipts, rejections, settled,
                  lifecycleVars, workspaceVars>>

DropCallerLink ==
  /\ faultsLeft > 0 /\ callerLinkUp
  /\ callerLinkUp' = FALSE
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<callerUp, gatewayUp, workerUp, workerLinkUp,
                  lifecycleVars, transportVars, workspaceVars>>

DropWorkerLink ==
  /\ faultsLeft > 0 /\ workerLinkUp
  /\ workerLinkUp' = FALSE
  /\ wire' = {}
  /\ ackWire' = {}
  /\ nackWire' = {}
  /\ faultsLeft' = faultsLeft - 1
  /\ UNCHANGED <<callerUp, gatewayUp, workerUp, callerLinkUp,
                  outbox, enqueued, effects, receipts, rejections, settled,
                  lifecycleVars, workspaceVars>>

CallerRecover ==
  /\ ~callerUp
  /\ callerUp' = TRUE
  /\ UNCHANGED <<gatewayUp, workerUp, callerLinkUp, workerLinkUp, faultsLeft,
                  lifecycleVars, transportVars, workspaceVars>>

GatewayRecover ==
  /\ ~gatewayUp
  /\ gatewayUp' = TRUE
  /\ UNCHANGED <<callerUp, workerUp, callerLinkUp, workerLinkUp, faultsLeft,
                  lifecycleVars, transportVars, workspaceVars>>

WorkerRecover ==
  /\ ~workerUp
  /\ workerUp' = TRUE
  /\ UNCHANGED <<callerUp, gatewayUp, callerLinkUp, workerLinkUp, faultsLeft,
                  lifecycleVars, transportVars, workspaceVars>>

RestoreCallerLink ==
  /\ callerUp /\ gatewayUp /\ ~callerLinkUp
  /\ callerLinkUp' = TRUE
  /\ UNCHANGED <<callerUp, gatewayUp, workerUp, workerLinkUp, faultsLeft,
                  lifecycleVars, transportVars, workspaceVars>>

RestoreWorkerLink ==
  /\ workerUp /\ gatewayUp /\ ~workerLinkUp
  /\ workerLinkUp' = TRUE
  /\ UNCHANGED <<callerUp, gatewayUp, workerUp, callerLinkUp, faultsLeft,
                  lifecycleVars, transportVars, workspaceVars>>

AdmitAny == \E item \in Items : Admit(item)
AcquireLeaseAny ==
  \E item \in Items, worker \in Workers : AcquireLease(item, worker)
ExposeAssignmentAny == \E grant \in LeaseGrants : ExposeAssignment(grant)
FenceLeaseAny == \E item \in Items : FenceLease(item)
RetireAny == \E item \in Items : Retire(item)
ExposeOutcomeAny == \E item \in Items : ExposeOutcome(item)
EnqueueEventAny == \E envelope \in Envelopes : EnqueueEvent(envelope)
TransmitAny == \E envelope \in Envelopes : Transmit(envelope)
DropDataAny == \E envelope \in Envelopes : DropData(envelope)
CommitEventAny == \E envelope \in Envelopes : CommitEvent(envelope)
ReplayExactAny == \E envelope \in Envelopes : ReplayExact(envelope)
RejectEventAny == \E envelope \in Envelopes : RejectEvent(envelope)
SendAckAny == \E envelope \in Envelopes : SendAck(envelope)
SendNackAny == \E envelope \in Envelopes : SendNack(envelope)
DropAckAny == \E envelope \in Envelopes : DropAck(envelope)
ReceiveReplyAny == \E envelope \in Envelopes : ReceiveReply(envelope)
ProposeChangeAny == \E proposal \in Proposals : ProposeChange(proposal)
CommitChangeAny == \E proposal \in Proposals : CommitChange(proposal)
ConflictChangeAny == \E proposal \in Proposals : ConflictChange(proposal)
ExposeWorkspaceAny == \E scope \in Scopes : ExposeWorkspace(scope)

NormalNext ==
  \/ AdmitAny
  \/ AcquireLeaseAny
  \/ ExposeAssignmentAny
  \/ FenceLeaseAny
  \/ RetireAny
  \/ ExposeOutcomeAny
  \/ EnqueueEventAny
  \/ TransmitAny
  \/ CommitEventAny
  \/ ReplayExactAny
  \/ RejectEventAny
  \/ SendAckAny
  \/ SendNackAny
  \/ ReceiveReplyAny
  \/ ProposeChangeAny
  \/ CommitChangeAny
  \/ ConflictChangeAny
  \/ ExposeWorkspaceAny

FaultNext ==
  \/ CallerCrash
  \/ GatewayCrash
  \/ WorkerCrash
  \/ DropCallerLink
  \/ DropWorkerLink
  \/ DropDataAny
  \/ DropAckAny

RecoveryNext ==
  \/ CallerRecover
  \/ GatewayRecover
  \/ WorkerRecover
  \/ RestoreCallerLink
  \/ RestoreWorkerLink

Quiescent ==
  /\ admitted = Items
  /\ retired = Items
  /\ queue = {}
  /\ ActiveItems = {}
  /\ outbox = {}
  /\ wire = {}
  /\ ackWire = {}
  /\ nackWire = {}
  /\ pendingChanges = {}
  /\ durableOutcomes = visibleOutcomes
  /\ workspaceVersion = visibleWorkspaceVersion
  /\ EnvironmentReady
  /\ faultsLeft = 0

Terminate == Quiescent /\ UNCHANGED vars

Next == NormalNext \/ FaultNext \/ RecoveryNext \/ Terminate

Spec == Init /\ [][Next]_vars

(* Faults are bounded, so fair recovery eventually becomes continuously     *)
(* enabled. Strong fairness on transport covers links that can flap before  *)
(* the finite budget is exhausted.                                          *)
RuntimeFairness ==
  /\ WF_vars(CallerRecover)
  /\ WF_vars(GatewayRecover)
  /\ WF_vars(WorkerRecover)
  /\ WF_vars(RestoreCallerLink)
  /\ WF_vars(RestoreWorkerLink)
  /\ WF_vars(AdmitAny)
  /\ WF_vars(AcquireLeaseAny \/ RetireAny)
  /\ WF_vars(ExposeAssignmentAny)
  /\ WF_vars(RetireAny)
  /\ SF_vars(TransmitAny)
  /\ SF_vars(CommitEventAny \/ ReplayExactAny \/ RejectEventAny)
  /\ SF_vars(SendAckAny \/ SendNackAny)
  /\ SF_vars(ReceiveReplyAny)
  /\ WF_vars(CommitChangeAny \/ ConflictChangeAny)
  /\ WF_vars(ExposeOutcomeAny)
  /\ WF_vars(ExposeWorkspaceAny)

LiveSpec == Spec /\ RuntimeFairness

(***************************************************************************)
(* Safety invariants.                                                       *)
(***************************************************************************)

ScopedIsolation ==
  /\ \A eventKey \in EventKeys :
       /\ KeyKind(EventItem(eventKey)) = KeyKind(eventKey)
       /\ KeyScope(EventItem(eventKey)) = KeyScope(eventKey)
  /\ \A envelope \in outbox \union effects \union receipts \union rejections :
       EnvelopeItem(envelope) \in Items

AdmissionAndCapacityOK ==
  /\ queue \subseteq admitted \ retired
  /\ Cardinality(admitted \ retired) <= Capacity
  /\ \A item \in ActiveItems : item \in admitted
  /\ \A item \in retired : leaseOwner[item] = NoWorker

LeaseFencingOK ==
  /\ \A item \in ActiveItems :
       /\ leaseFence[item] > 0
       /\ CurrentGrant(item) \in leaseGrants
  /\ \A grant \in visibleAssignments : grant \in leaseGrants
  /\ \A envelope \in effects :
       LeaseGrant(EnvelopeItem(envelope), envelope.worker, envelope.fence)
         \in leaseGrants

DurableBeforeVisible ==
  /\ visibleAssignments \subseteq leaseGrants
  /\ visibleOutcomes \subseteq durableOutcomes
  /\ \A scope \in Scopes :
       visibleWorkspaceVersion[scope] <= workspaceVersion[scope]

ReceiptConsistency ==
  /\ effects = receipts
  /\ receipts \cap rejections = {}
  /\ \A eventKey \in EventKeys : Cardinality(ReceiptFor(eventKey)) <= 1
  /\ \A first \in receipts, second \in receipts :
       first.event = second.event => first = second

AckAfterDurable ==
  /\ ackWire \subseteq receipts
  /\ nackWire \subseteq rejections
  /\ settled \subseteq receipts \union rejections
  /\ settled \cap outbox = {}

OutboxAccounting ==
  /\ enqueued = outbox \union settled
  /\ outbox \cap settled = {}
  /\ wire \union ackWire \union nackWire \subseteq outbox
  /\ effects \union receipts \union rejections \union settled \subseteq enqueued

WorkspaceCASOK ==
  /\ pendingChanges \cap committedChanges = {}
  /\ pendingChanges \cap conflictedChanges = {}
  /\ committedChanges \cap conflictedChanges = {}
  /\ \A scope \in Scopes :
       {proposal.next :
          proposal \in
            {candidate \in committedChanges :
              ProposalScope(candidate) = scope}}
         = 1..workspaceVersion[scope]
  /\ \A first \in committedChanges, second \in committedChanges :
       /\ ProposalScope(first) = ProposalScope(second)
       /\ first.base = second.base
       => first = second
  /\ \A proposal \in conflictedChanges :
       proposal.base < workspaceVersion[ProposalScope(proposal)]

FaultPreservesDurable ==
  [][faultsLeft' < faultsLeft => durableVars' = durableVars]_vars

EffectAuthorizedAtCommit ==
  [][\A envelope \in effects' \ effects :
       /\ EnvelopeItem(envelope) \notin retired
       /\ leaseOwner[EnvelopeItem(envelope)] = envelope.worker
       /\ leaseFence[EnvelopeItem(envelope)] = envelope.fence
       /\ CurrentGrant(EnvelopeItem(envelope)) =
            LeaseGrant(EnvelopeItem(envelope), envelope.worker, envelope.fence)]_vars

(* These sets are the durable audit/replay history, not current work queues. *)
(* Ordinary actions and crashes may remove volatile packets or live leases,  *)
(* but can never erase a previously admitted identity, fence, receipt, NACK, *)
(* settlement, terminal outcome, or workspace CAS decision.                 *)
DurableHistoriesAppendOnly ==
  [][ /\ admitted \subseteq admitted'
      /\ leaseGrants \subseteq leaseGrants'
      /\ visibleAssignments \subseteq visibleAssignments'
      /\ retired \subseteq retired'
      /\ durableOutcomes \subseteq durableOutcomes'
      /\ visibleOutcomes \subseteq visibleOutcomes'
      /\ enqueued \subseteq enqueued'
      /\ effects \subseteq effects'
      /\ receipts \subseteq receipts'
      /\ rejections \subseteq rejections'
      /\ settled \subseteq settled'
      /\ committedChanges \subseteq committedChanges'
      /\ conflictedChanges \subseteq conflictedChanges' ]_vars

Safety ==
  /\ TypeOK
  /\ ScopedIsolation
  /\ AdmissionAndCapacityOK
  /\ LeaseFencingOK
  /\ DurableBeforeVisible
  /\ ReceiptConsistency
  /\ AckAfterDurable
  /\ OutboxAccounting
  /\ WorkspaceCASOK

(***************************************************************************)
(* Liveness, checked only under LiveSpec's explicit fairness assumptions.  *)
(***************************************************************************)

EnvironmentEventuallyRecovers == <>[]EnvironmentReady

(* Coverage witness used by an expected-failure configuration.  Its failure *)
(* proves that the bounded state graph really contains a simultaneous crash  *)
(* of caller, gateway, and worker; ordinary liveness is then checked from    *)
(* every such reachable state by HumanRuntimeTripleOutage.cfg.               *)
SomePartyUp == callerUp \/ gatewayUp \/ workerUp

(* A second expected-failure witness pins the retry-storm fault budget: TLC *)
(* must reach a behavior containing all five modeled fault transitions.     *)
FaultBudgetRemaining == faultsLeft > 0

OutboxEventuallySettles ==
  \A envelope \in Envelopes :
    (envelope \in outbox) ~> (envelope \in settled)

LateEventsDoNotPoison ==
  \A envelope \in Envelopes :
    (EnvelopeItem(envelope) \in retired /\ envelope \in outbox)
      ~> (envelope \in settled)

OutcomesEventuallyVisible ==
  \A item \in Items :
    (item \in durableOutcomes) ~> (item \in visibleOutcomes)

WorkspaceEventuallyResolves ==
  /\ \A proposal \in Proposals :
       (proposal \in pendingChanges)
         ~> (proposal \in committedChanges \union conflictedChanges)
  /\ \A scope \in Scopes :
       (workspaceVersion[scope] # visibleWorkspaceVersion[scope])
         ~> (workspaceVersion[scope] = visibleWorkspaceVersion[scope])

ConflictingDigestEventuallyRejected ==
  \A accepted, conflicting \in Envelopes :
    /\ accepted.event = conflicting.event
    /\ accepted.digest # conflicting.digest
    => (accepted \in receipts /\ conflicting \in enqueued)
         ~> (conflicting \in rejections)

(***************************************************************************)
(* Dedicated workspace race harness. It requires every modeled item to     *)
(* propose from base 0 before allowing the first commit, then requires the  *)
(* stale proposal to become an explicit conflict. This keeps CAS coverage  *)
(* non-vacuous without multiplying transport state.                        *)
(***************************************************************************)

WorkspaceRaceReady ==
  /\ admitted = Items
  /\ Cardinality(pendingChanges) = Cardinality(Items)
  /\ committedChanges = {}
  /\ conflictedChanges = {}

WorkspaceRaceResolved ==
  /\ Cardinality(committedChanges) = 1
  /\ Cardinality(conflictedChanges) = Cardinality(Items) - 1
  /\ pendingChanges = {}

WorkspaceRaceNext ==
  \/ (~WorkspaceRaceReady /\ committedChanges = {} /\ AdmitAny)
  \/ (~WorkspaceRaceReady /\ committedChanges = {} /\ ProposeChangeAny)
  \/ (WorkspaceRaceReady /\ CommitChangeAny)
  \/ (committedChanges # {} /\ pendingChanges # {} /\ ConflictChangeAny)
  \/ ExposeWorkspaceAny
  \/ (WorkspaceRaceResolved /\
      workspaceVersion = visibleWorkspaceVersion /\ UNCHANGED vars)

WorkspaceRaceSpec ==
  /\ Init
  /\ [][WorkspaceRaceNext]_vars
  /\ WF_vars(AdmitAny)
  /\ WF_vars(ProposeChangeAny)
  /\ WF_vars(CommitChangeAny)
  /\ WF_vars(ConflictChangeAny)
  /\ WF_vars(ExposeWorkspaceAny)

WorkspaceRaceEventuallyExplicit == <>WorkspaceRaceResolved

=============================================================================
