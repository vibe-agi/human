---------------------------- MODULE HumanAgent ----------------------------
(***************************************************************************)
(* HumanAgent is the task-oriented surface of Human.                       *)
(*                                                                         *)
(* This module deliberately does not model sockets, retries, or the shared *)
(* durable outbox. HumanCommon owns the cross-surface identity vocabulary;  *)
(* the runtime/network model composes with this lifecycle separately.       *)
(*                                                                         *)
(* Context is conversation grouping only. Workspace is an orthogonal       *)
(* correctness scope: two Contexts may intentionally operate on the same    *)
(* Workspace and therefore share one CAS baseline. A completed Task is      *)
(* never reopened; a later caller message creates a fresh Task.              *)
(* ContextIds and WorkspaceIds are opaque authority-qualified scopes, e.g.  *)
(* <<authenticated principal, external id>>. They are never untrusted bare  *)
(* tenant-local strings; API refinement must construct them after auth.      *)
(*                                                                         *)
(* Workspace contents are abstracted to monotonically numbered versions.   *)
(* Freezing records an immutable Artifact payload (base, version, digest).  *)
(* Publishing that Artifact and completing its Task are one atomic action. *)
(* Applying an Artifact is a separate caller-side concern. Only a verified *)
(* success receipt for the exact base/version may advance baseline.         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, HumanCommon

CONSTANTS ContextIds,
          WorkspaceIds,
          TaskIds,
          MessageIds,
          ArtifactIds,
          MaxVersion

ASSUME /\ ContextIds # {}
       /\ WorkspaceIds # {}
       /\ TaskIds # {}
       /\ MessageIds # {}
       /\ ArtifactIds # {}
       /\ IsFiniteSet(ContextIds)
       /\ IsFiniteSet(WorkspaceIds)
       /\ IsFiniteSet(TaskIds)
       /\ IsFiniteSet(MessageIds)
       /\ IsFiniteSet(ArtifactIds)
       /\ MaxVersion \in Nat
       /\ MaxVersion > 0

Unused       == "unused"
Submitted    == "submitted"
Working      == "working"
InputNeeded  == "input_required"
Completed    == "completed"
Canceled     == "canceled"
Rejected     == "rejected"
Failed       == "failed"

TerminalStates == {Completed, Canceled, Rejected, Failed}
ActiveStates   == {Submitted, Working, InputNeeded}
TaskStates     == {Unused} \union ActiveStates \union TerminalStates

NoContext   == "__no_context__"
NoWorkspace == "__no_workspace__"
NoTask      == "__no_task__"
NoArtifact  == "__no_artifact__"
NoAuthor    == "__no_author__"
Caller      == "caller"
Agent       == "agent"

MessageAuthors == {NoAuthor, Caller, Agent}

ArtifactAbsent    == "absent"
ArtifactFrozen    == "frozen"
ArtifactPublished == "published"
ArtifactDiscarded == "discarded"
ArtifactStates    == {ArtifactAbsent, ArtifactFrozen, ArtifactPublished,
                      ArtifactDiscarded}

ReceiptNone          == "none"
ReceiptSuccess       == "success"
ReceiptConflict      == "conflict"
ReceiptRejected      == "rejected"
ReceiptIndeterminate == "indeterminate"
ReceiptStates == {ReceiptNone, ReceiptSuccess, ReceiptConflict,
                  ReceiptRejected, ReceiptIndeterminate}

VARIABLES
  taskState,       \* TaskId -> lifecycle state
  taskContext,     \* TaskId -> display/conversation ContextId (stable)
  taskWorkspace,   \* TaskId -> correctness WorkspaceId (stable)
  messages,        \* TaskId -> append-only sequence of MessageIds
  messageAuthor,   \* MessageId -> caller/agent, immutable after append
  artifactState,   \* ArtifactId -> absent/frozen/published
  artifactTask,    \* ArtifactId -> owning TaskId, immutable after freeze
  artifactBase,    \* ArtifactId -> required confirmed base version
  artifactVersion, \* ArtifactId -> immutable submitted workspace version
  submission,      \* TaskId -> its atomically published ArtifactId
  receipt,         \* ArtifactId -> immutable caller apply decision
  draft,           \* WorkspaceId -> newest Human workspace draft
  baseline,        \* WorkspaceId -> newest verified applied version
  dirty             \* WorkspaceId -> unverified external edit exists

vars == <<taskState, taskContext, taskWorkspace, messages, messageAuthor,
          artifactState, artifactTask, artifactBase, artifactVersion,
          submission, receipt, draft, baseline, dirty>>

TaskCreated(task) == taskState[task] # Unused
TaskTerminal(task) == taskState[task] \in TerminalStates
TaskActive(task) == taskState[task] \in ActiveStates

TasksInContext(context) ==
  {task \in TaskIds : TaskCreated(task) /\ taskContext[task] = context}

TerminalTasksInContext(context) ==
  {task \in TaskIds : TaskTerminal(task) /\ taskContext[task] = context}

TasksInWorkspace(workspace) ==
  {task \in TaskIds : TaskCreated(task) /\ taskWorkspace[task] = workspace}

ArtifactsForTask(task) ==
  {artifact \in ArtifactIds :
     artifactState[artifact] # ArtifactAbsent /\ artifactTask[artifact] = task}

UsedBy(message) ==
  {task \in TaskIds : message \in SeqRange(messages[task])}

ArtifactContext(artifact) == taskContext[artifactTask[artifact]]
ArtifactWorkspace(artifact) == taskWorkspace[artifactTask[artifact]]

AgentTaskKey(task) ==
  ScopedKey(AgentKind, taskWorkspace[task], task)

AgentArtifactKey(artifact) ==
  ScopedKey(AgentKind, ArtifactWorkspace(artifact), artifact)

ArtifactDigest(artifact) ==
  <<AgentKind, artifactBase[artifact], artifactVersion[artifact], artifact>>

Init ==
  /\ taskState = [task \in TaskIds |-> Unused]
  /\ taskContext = [task \in TaskIds |-> NoContext]
  /\ taskWorkspace = [task \in TaskIds |-> NoWorkspace]
  /\ messages = [task \in TaskIds |-> <<>>]
  /\ messageAuthor = [message \in MessageIds |-> NoAuthor]
  /\ artifactState = [artifact \in ArtifactIds |-> ArtifactAbsent]
  /\ artifactTask = [artifact \in ArtifactIds |-> NoTask]
  /\ artifactBase = [artifact \in ArtifactIds |-> 0]
  /\ artifactVersion = [artifact \in ArtifactIds |-> 0]
  /\ submission = [task \in TaskIds |-> NoArtifact]
  /\ receipt = [artifact \in ArtifactIds |-> ReceiptNone]
  /\ draft = [workspace \in WorkspaceIds |-> 0]
  /\ baseline = [workspace \in WorkspaceIds |-> 0]
  /\ dirty = [workspace \in WorkspaceIds |-> FALSE]

(***************************************************************************)
(* Caller and Human task/message lifecycle.                               *)
(***************************************************************************)

CreateFirstTask(context, workspace, task, message) ==
  /\ taskState[task] = Unused
  /\ messageAuthor[message] = NoAuthor
  /\ TasksInContext(context) = {}
  /\ taskState' = [taskState EXCEPT ![task] = Submitted]
  /\ taskContext' = [taskContext EXCEPT ![task] = context]
  /\ taskWorkspace' = [taskWorkspace EXCEPT ![task] = workspace]
  /\ messages' = [messages EXCEPT ![task] = Append(@, message)]
  /\ messageAuthor' = [messageAuthor EXCEPT ![message] = Caller]
  /\ UNCHANGED <<artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline, dirty>>

CreateTaskInExistingContext(context, workspace, task, message) ==
  /\ taskState[task] = Unused
  /\ messageAuthor[message] = NoAuthor
  /\ TasksInContext(context) # {}
  /\ taskState' = [taskState EXCEPT ![task] = Submitted]
  /\ taskContext' = [taskContext EXCEPT ![task] = context]
  /\ taskWorkspace' = [taskWorkspace EXCEPT ![task] = workspace]
  /\ messages' = [messages EXCEPT ![task] = Append(@, message)]
  /\ messageAuthor' = [messageAuthor EXCEPT ![message] = Caller]
  /\ UNCHANGED <<artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline, dirty>>

AcceptTask(task) ==
  /\ taskState[task] = Submitted
  /\ taskState' = [taskState EXCEPT ![task] = Working]
  /\ UNCHANGED <<taskContext, taskWorkspace, messages, messageAuthor,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline, dirty>>

RejectTask(task) ==
  /\ taskState[task] = Submitted
  /\ taskState' = [taskState EXCEPT ![task] = Rejected]
  /\ UNCHANGED <<taskContext, taskWorkspace, messages, messageAuthor,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline, dirty>>

RequestInput(task, message) ==
  /\ taskState[task] = Working
  /\ messageAuthor[message] = NoAuthor
  /\ taskState' = [taskState EXCEPT ![task] = InputNeeded]
  /\ messages' = [messages EXCEPT ![task] = Append(@, message)]
  /\ messageAuthor' = [messageAuthor EXCEPT ![message] = Agent]
  /\ UNCHANGED <<taskContext, taskWorkspace,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline, dirty>>

CallerReply(task, message) ==
  /\ taskState[task] = InputNeeded
  /\ messageAuthor[message] = NoAuthor
  /\ taskState' = [taskState EXCEPT ![task] = Working]
  /\ messages' = [messages EXCEPT ![task] = Append(@, message)]
  /\ messageAuthor' = [messageAuthor EXCEPT ![message] = Caller]
  /\ UNCHANGED <<taskContext, taskWorkspace,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline, dirty>>

CancelTask(task) ==
  /\ TaskActive(task)
  /\ taskState' = [taskState EXCEPT ![task] = Canceled]
  /\ artifactState' =
       [artifact \in ArtifactIds |->
         IF artifactTask[artifact] = task /\
            artifactState[artifact] = ArtifactFrozen
           THEN ArtifactDiscarded
           ELSE artifactState[artifact]]
  /\ UNCHANGED <<taskContext, taskWorkspace, messages, messageAuthor,
                  artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline, dirty>>

FailTask(task) ==
  /\ taskState[task] \in {Working, InputNeeded}
  /\ taskState' = [taskState EXCEPT ![task] = Failed]
  /\ artifactState' =
       [artifact \in ArtifactIds |->
         IF artifactTask[artifact] = task /\
            artifactState[artifact] = ArtifactFrozen
           THEN ArtifactDiscarded
           ELSE artifactState[artifact]]
  /\ UNCHANGED <<taskContext, taskWorkspace, messages, messageAuthor,
                  artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline, dirty>>

(***************************************************************************)
(* Workspace draft and immutable atomic Submission/Artifact lifecycle.    *)
(***************************************************************************)

EditDraft(task) ==
  /\ taskState[task] = Working
  /\ draft[taskWorkspace[task]] < MaxVersion
  /\ draft' = [draft EXCEPT ![taskWorkspace[task]] = @ + 1]
  /\ UNCHANGED <<taskState, taskContext, taskWorkspace, messages, messageAuthor,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, baseline, dirty>>

CallerLocalEdit(workspace) ==
  /\ TasksInWorkspace(workspace) # {}
  /\ ~dirty[workspace]
  /\ dirty' = [dirty EXCEPT ![workspace] = TRUE]
  /\ UNCHANGED <<taskState, taskContext, taskWorkspace, messages, messageAuthor,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline>>

ResolveLocalEdit(workspace) ==
  /\ dirty[workspace]
  /\ dirty' = [dirty EXCEPT ![workspace] = FALSE]
  /\ UNCHANGED <<taskState, taskContext, taskWorkspace, messages, messageAuthor,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline>>

FreezeArtifact(task, artifact) ==
  /\ taskState[task] = Working
  /\ submission[task] = NoArtifact
  /\ ArtifactsForTask(task) = {}
  /\ artifactState[artifact] = ArtifactAbsent
  /\ artifactState' = [artifactState EXCEPT ![artifact] = ArtifactFrozen]
  /\ artifactTask' = [artifactTask EXCEPT ![artifact] = task]
  /\ artifactBase' = [artifactBase EXCEPT
                         ![artifact] = baseline[taskWorkspace[task]]]
  /\ artifactVersion' = [artifactVersion EXCEPT
                            ![artifact] = draft[taskWorkspace[task]]]
  /\ UNCHANGED <<taskState, taskContext, taskWorkspace, messages, messageAuthor,
                  submission, receipt, draft, baseline, dirty>>

PublishSubmission(task, artifact, message) ==
  /\ taskState[task] = Working
  /\ artifactState[artifact] = ArtifactFrozen
  /\ artifactTask[artifact] = task
  /\ submission[task] = NoArtifact
  /\ messageAuthor[message] = NoAuthor
  /\ taskState' = [taskState EXCEPT ![task] = Completed]
  /\ artifactState' = [artifactState EXCEPT
                         ![artifact] = ArtifactPublished]
  /\ submission' = [submission EXCEPT ![task] = artifact]
  /\ messages' = [messages EXCEPT ![task] = Append(@, message)]
  /\ messageAuthor' = [messageAuthor EXCEPT ![message] = Agent]
  /\ UNCHANGED <<taskContext, taskWorkspace,
                  artifactTask, artifactBase, artifactVersion,
                  receipt, draft, baseline, dirty>>

(* Content-only work is a first-class successful Task. An authoritative     *)
(* workspace Submission is optional, but when present it remains atomic.    *)
(* Any unsubmitted local draft deliberately remains unconfirmed for a later *)
(* fresh Task in this Context; completion never pretends it was applied.     *)
CompleteWithoutArtifact(task, message) ==
  /\ taskState[task] = Working
  /\ submission[task] = NoArtifact
  /\ ArtifactsForTask(task) = {}
  /\ messageAuthor[message] = NoAuthor
  /\ taskState' = [taskState EXCEPT ![task] = Completed]
  /\ messages' = [messages EXCEPT ![task] = Append(@, message)]
  /\ messageAuthor' = [messageAuthor EXCEPT ![message] = Agent]
  /\ UNCHANGED <<taskContext, taskWorkspace,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, receipt, draft, baseline, dirty>>

(***************************************************************************)
(* Caller apply decisions. The network may duplicate delivery, but the     *)
(* shared runtime presents at most one durable decision to this lifecycle. *)
(***************************************************************************)

RecordApplySuccess(artifact) ==
  LET workspace == ArtifactWorkspace(artifact)
  IN /\ artifactState[artifact] = ArtifactPublished
     /\ receipt[artifact] = ReceiptNone
     /\ ~dirty[workspace]
     /\ baseline[workspace] = artifactBase[artifact]
     /\ receipt' = [receipt EXCEPT ![artifact] = ReceiptSuccess]
     /\ baseline' = [baseline EXCEPT
                       ![workspace] = artifactVersion[artifact]]
     /\ UNCHANGED <<taskState, taskContext, taskWorkspace,
                     messages, messageAuthor,
                     artifactState, artifactTask, artifactBase,
                     artifactVersion, submission, draft, dirty>>

RecordApplyConflict(artifact) ==
  /\ artifactState[artifact] = ArtifactPublished
  /\ receipt[artifact] = ReceiptNone
  /\ receipt' = [receipt EXCEPT ![artifact] = ReceiptConflict]
  /\ UNCHANGED <<taskState, taskContext, taskWorkspace, messages, messageAuthor,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, draft, baseline, dirty>>

RecordApplyRejected(artifact) ==
  /\ artifactState[artifact] = ArtifactPublished
  /\ receipt[artifact] = ReceiptNone
  /\ receipt' = [receipt EXCEPT ![artifact] = ReceiptRejected]
  /\ UNCHANGED <<taskState, taskContext, taskWorkspace, messages, messageAuthor,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, draft, baseline, dirty>>

RecordApplyIndeterminate(artifact) ==
  /\ artifactState[artifact] = ArtifactPublished
  /\ receipt[artifact] = ReceiptNone
  /\ receipt' = [receipt EXCEPT ![artifact] = ReceiptIndeterminate]
  /\ UNCHANGED <<taskState, taskContext, taskWorkspace, messages, messageAuthor,
                  artifactState, artifactTask, artifactBase, artifactVersion,
                  submission, draft, baseline, dirty>>

CreateFirst ==
  \E context \in ContextIds, workspace \in WorkspaceIds,
     task \in TaskIds, message \in MessageIds :
    CreateFirstTask(context, workspace, task, message)

CreateInExisting ==
  \E context \in ContextIds, workspace \in WorkspaceIds,
     task \in TaskIds, message \in MessageIds :
    CreateTaskInExistingContext(context, workspace, task, message)

(* The public lifecycle permits independent Tasks to run concurrently in   *)
(* one display Context. This narrower wrapper exists only so FollowupSpec   *)
(* can force the separate "terminal Task, then fresh Task" witness.         *)
CreateFollowupAfterTerminal ==
  \E context \in ContextIds, workspace \in WorkspaceIds,
     task \in TaskIds, message \in MessageIds :
    /\ TerminalTasksInContext(context) # {}
    /\ CreateTaskInExistingContext(context, workspace, task, message)

AcceptOrReject ==
  \E task \in TaskIds : AcceptTask(task) \/ RejectTask(task)

AskForInput ==
  \E task \in TaskIds, message \in MessageIds : RequestInput(task, message)

ReplyToInput ==
  \E task \in TaskIds, message \in MessageIds : CallerReply(task, message)

CancelAny == \E task \in TaskIds : CancelTask(task)
FailAny == \E task \in TaskIds : FailTask(task)
EditAny == \E task \in TaskIds : EditDraft(task)
LocalEditAny == \E workspace \in WorkspaceIds : CallerLocalEdit(workspace)
ResolveLocalEditAny ==
  \E workspace \in WorkspaceIds : ResolveLocalEdit(workspace)

FreezeAny ==
  \E task \in TaskIds, artifact \in ArtifactIds :
    FreezeArtifact(task, artifact)

PublishAny ==
  \E task \in TaskIds, artifact \in ArtifactIds, message \in MessageIds :
    PublishSubmission(task, artifact, message)

CompleteContentAny ==
  \E task \in TaskIds, message \in MessageIds :
    CompleteWithoutArtifact(task, message)

ApplyDecisionAny ==
  \E artifact \in ArtifactIds :
    \/ RecordApplySuccess(artifact)
    \/ RecordApplyConflict(artifact)
    \/ RecordApplyRejected(artifact)
    \/ RecordApplyIndeterminate(artifact)

ApplySuccessAny ==
  \E artifact \in ArtifactIds : RecordApplySuccess(artifact)

ApplyCASConflictAny ==
  \E artifact \in ArtifactIds :
    /\ artifactState[artifact] = ArtifactPublished
    /\ receipt[artifact] = ReceiptNone
    /\ baseline[ArtifactWorkspace(artifact)] # artifactBase[artifact]
    /\ RecordApplyConflict(artifact)

AcceptAny == \E task \in TaskIds : AcceptTask(task)

Next ==
  \/ CreateFirst
  \/ CreateInExisting
  \/ AcceptOrReject
  \/ AskForInput
  \/ ReplyToInput
  \/ CancelAny
  \/ FailAny
  \/ EditAny
  \/ LocalEditAny
  \/ ResolveLocalEditAny
  \/ FreezeAny
  \/ PublishAny
  \/ CompleteContentAny
  \/ ApplyDecisionAny

AllCreatedTasksTerminal ==
  \A task \in TaskIds : ~TaskCreated(task) \/ TaskTerminal(task)

AllPublishedReceiptsDecided ==
  \A artifact \in ArtifactIds :
    artifactState[artifact] # ArtifactPublished \/
      receipt[artifact] # ReceiptNone

Quiescent ==
  /\ AllCreatedTasksTerminal
  /\ AllPublishedReceiptsDecided
  /\ \A artifact \in ArtifactIds :
       artifactState[artifact] # ArtifactFrozen

Terminating == Quiescent /\ UNCHANGED vars

FullNext == Next \/ Terminating

Spec == Init /\ [][FullNext]_vars

(* Human/caller fairness is intentionally opt-in. Safety must not depend on *)
(* people being online. Strong fairness for FailAny prevents an infinite    *)
(* input-required/reply loop from starving every terminal decision.         *)
HumanDecisionFairness ==
  /\ WF_vars(AcceptOrReject)
  /\ SF_vars(FreezeAny \/ FailAny)
  /\ SF_vars(PublishAny \/ FailAny)

CallerFairness == WF_vars(ReplyToInput \/ CancelAny)

MachineFairness == WF_vars(ApplyDecisionAny)

FairSpec ==
  Spec /\ HumanDecisionFairness /\ CallerFairness /\ MachineFairness

(* Dedicated finite harness: input loops are covered by FairSpec above. This *)
(* smaller relation reserves enough finite MessageIds to force a terminal   *)
(* Task followed by a fresh Task in the same Context.                        *)
FollowupNext ==
  \/ CreateFirst
  \/ CreateFollowupAfterTerminal
  \/ AcceptOrReject
  \/ CancelAny
  \/ FailAny
  \/ EditAny
  \/ FreezeAny
  \/ PublishAny
  \/ CompleteContentAny
  \/ ApplyDecisionAny
  \/ Terminating

FollowupSpec ==
  /\ Init
  /\ [][FollowupNext]_vars
  /\ WF_vars(CreateFirst)
  /\ WF_vars(CreateFollowupAfterTerminal)
  /\ WF_vars(AcceptOrReject)
  /\ SF_vars(FreezeAny \/ FailAny)
  /\ SF_vars(PublishAny \/ FailAny)
  /\ WF_vars(ApplyDecisionAny)

(* Forced two-round input conversation. Progress/status events are outside  *)
(* this message sequence; these six entries are caller request, Human ask,  *)
(* caller reply, Human ask, caller reply, and Human final content.           *)
ConversationCreate == CreateFirst
ConversationAccept == AcceptAny
ConversationAsk ==
  \E task \in TaskIds, message \in MessageIds :
    /\ Len(messages[task]) \in {1, 3}
    /\ RequestInput(task, message)
ConversationReply ==
  \E task \in TaskIds, message \in MessageIds :
    /\ Len(messages[task]) \in {2, 4}
    /\ CallerReply(task, message)
ConversationComplete ==
  \E task \in TaskIds, message \in MessageIds :
    /\ Len(messages[task]) = 5
    /\ CompleteWithoutArtifact(task, message)
ConversationDone ==
  \E task \in TaskIds :
    /\ taskState[task] = Completed
    /\ Len(messages[task]) = 6

ConversationTerminating == ConversationDone /\ UNCHANGED vars

ConversationNext ==
  \/ ConversationCreate
  \/ ConversationAccept
  \/ ConversationAsk
  \/ ConversationReply
  \/ ConversationComplete
  \/ ConversationTerminating

ConversationSpec ==
  /\ Init
  /\ [][ConversationNext]_vars
  /\ WF_vars(ConversationCreate)
  /\ WF_vars(ConversationAccept)
  /\ WF_vars(ConversationAsk)
  /\ WF_vars(ConversationReply)
  /\ WF_vars(ConversationComplete)

(* Context is not a scheduler or a correctness lock. This dedicated harness*)
(* forces two independent Tasks in the same Context to reach Working at the*)
(* same time. Their Workspace effects remain serialized only by CAS.       *)
ParallelContextReady ==
  /\ \A task \in TaskIds : taskState[task] = Working
  /\ \E context \in ContextIds : TasksInContext(context) = TaskIds

ParallelContextNext ==
  \/ (\lnot (\A task \in TaskIds : TaskCreated(task)) /\
      (CreateFirst \/ CreateInExisting))
  \/ ((\A task \in TaskIds : TaskCreated(task)) /\
      \lnot ParallelContextReady /\ AcceptAny)
  \/ (ParallelContextReady /\ UNCHANGED vars)

ParallelContextSpec ==
  /\ Init
  /\ [][ParallelContextNext]_vars
  /\ WF_vars(CreateFirst)
  /\ WF_vars(CreateInExisting)
  /\ WF_vars(AcceptAny)

(* Forced cross-Context workspace race. Both Tasks freeze the same non-noop *)
(* base/version before either Artifact is published. Exactly one verified   *)
(* CAS may succeed; the other must become an explicit conflict.             *)
SharedTasksCreated == \A task \in TaskIds : TaskCreated(task)
SharedTasksWorking == \A task \in TaskIds : taskState[task] = Working
SharedDraftReady == \A workspace \in WorkspaceIds : draft[workspace] = 1
SharedArtifactsFrozen ==
  \A artifact \in ArtifactIds : artifactState[artifact] = ArtifactFrozen
SharedArtifactsReadyToPublish ==
  \A artifact \in ArtifactIds :
    artifactState[artifact] \in {ArtifactFrozen, ArtifactPublished}
SharedArtifactsPublished ==
  \A artifact \in ArtifactIds : artifactState[artifact] = ArtifactPublished
SharedWorkspaceResolved ==
  /\ Cardinality(
       {artifact \in ArtifactIds : receipt[artifact] = ReceiptSuccess}) = 1
  /\ Cardinality(
       {artifact \in ArtifactIds : receipt[artifact] = ReceiptConflict}) = 1

SharedWorkspaceNext ==
  \/ (\lnot SharedTasksCreated /\ CreateFirst)
  \/ (SharedTasksCreated /\ \lnot SharedTasksWorking /\ AcceptAny)
  \/ (SharedTasksWorking /\ \lnot SharedDraftReady /\ EditAny)
  \/ (SharedDraftReady /\ \lnot SharedArtifactsFrozen /\ FreezeAny)
  \/ (SharedArtifactsReadyToPublish /\
      \lnot SharedArtifactsPublished /\ PublishAny)
  \/ (SharedArtifactsPublished /\ ApplySuccessAny)
  \/ (SharedArtifactsPublished /\ ApplyCASConflictAny)
  \/ (SharedWorkspaceResolved /\ UNCHANGED vars)

SharedWorkspaceSpec ==
  /\ Init
  /\ [][SharedWorkspaceNext]_vars
  /\ WF_vars(CreateFirst)
  /\ WF_vars(AcceptAny)
  /\ WF_vars(EditAny)
  /\ WF_vars(FreezeAny)
  /\ WF_vars(PublishAny)
  /\ WF_vars(ApplySuccessAny \/ ApplyCASConflictAny)
NoHumanFairSpec == Spec /\ MachineFairness

(***************************************************************************)
(* State invariants.                                                       *)
(***************************************************************************)

TypeOK ==
  /\ taskState \in [TaskIds -> TaskStates]
  /\ taskContext \in [TaskIds -> ContextIds \union {NoContext}]
  /\ taskWorkspace \in [TaskIds -> WorkspaceIds \union {NoWorkspace}]
  /\ messages \in [TaskIds -> Seq(MessageIds)]
  /\ messageAuthor \in [MessageIds -> MessageAuthors]
  /\ artifactState \in [ArtifactIds -> ArtifactStates]
  /\ artifactTask \in [ArtifactIds -> TaskIds \union {NoTask}]
  /\ artifactBase \in [ArtifactIds -> 0..MaxVersion]
  /\ artifactVersion \in [ArtifactIds -> 0..MaxVersion]
  /\ submission \in [TaskIds -> ArtifactIds \union {NoArtifact}]
  /\ receipt \in [ArtifactIds -> ReceiptStates]
  /\ draft \in [WorkspaceIds -> 0..MaxVersion]
  /\ baseline \in [WorkspaceIds -> 0..MaxVersion]
  /\ dirty \in [WorkspaceIds -> BOOLEAN]

AgentKeyIsolation ==
  /\ AgentKind \in Surfaces
  /\ LLMKind \in Surfaces
  /\ AgentKind # LLMKind
  /\ \A task \in TaskIds :
       TaskCreated(task) =>
         /\ KeyKind(AgentTaskKey(task)) = AgentKind
         /\ KeyScope(AgentTaskKey(task)) = taskWorkspace[task]
         /\ KeyID(AgentTaskKey(task)) = task
  /\ \A artifact \in ArtifactIds :
       artifactState[artifact] # ArtifactAbsent =>
         /\ KeyKind(AgentArtifactKey(artifact)) = AgentKind
         /\ KeyScope(AgentArtifactKey(artifact)) = ArtifactWorkspace(artifact)
         /\ KeyID(AgentArtifactKey(artifact)) = artifact

TaskIdentityWellFormed ==
  /\ \A task \in TaskIds :
       /\ (TaskCreated(task) <=> taskContext[task] \in ContextIds)
       /\ (TaskCreated(task) <=> taskWorkspace[task] \in WorkspaceIds)
       /\ (~TaskCreated(task) =>
             /\ taskContext[task] = NoContext
             /\ taskWorkspace[task] = NoWorkspace
             /\ messages[task] = <<>>
             /\ submission[task] = NoArtifact)
       /\ (TaskCreated(task) =>
             /\ Len(messages[task]) > 0
             /\ messageAuthor[Head(messages[task])] = Caller)

(* Deliberately false as a public invariant; an expected-failure harness    *)
(* keeps the reachable parallel-in-one-Context behavior from regressing.    *)
AtMostOneActiveTaskPerContext ==
  \A context \in ContextIds :
    Cardinality(
      {task \in TasksInContext(context) : TaskActive(task)}) <= 1

MessageTurnsAlternate ==
  \A task \in TaskIds :
    \A index \in DOMAIN messages[task] :
      messageAuthor[messages[task][index]] =
        IF index % 2 = 1 THEN Caller ELSE Agent

MessageHistoryWellFormed ==
  /\ \A task \in TaskIds :
       Len(messages[task]) = Cardinality(SeqRange(messages[task]))
  /\ \A message \in MessageIds :
       /\ Cardinality(UsedBy(message)) <= 1
       /\ (messageAuthor[message] # NoAuthor <=>
             Cardinality(UsedBy(message)) = 1)

ArtifactWellFormed ==
  /\ \A artifact \in ArtifactIds :
       /\ (artifactState[artifact] = ArtifactAbsent =>
             /\ artifactTask[artifact] = NoTask
             /\ artifactBase[artifact] = 0
             /\ artifactVersion[artifact] = 0
             /\ receipt[artifact] = ReceiptNone)
       /\ (artifactState[artifact] # ArtifactAbsent =>
             /\ artifactTask[artifact] \in TaskIds
             /\ TaskCreated(artifactTask[artifact])
             /\ artifactBase[artifact] <= artifactVersion[artifact]
             /\ artifactVersion[artifact] <=
                  draft[ArtifactWorkspace(artifact)])
       /\ (artifactState[artifact] = ArtifactFrozen =>
             receipt[artifact] = ReceiptNone)
       /\ (artifactState[artifact] = ArtifactDiscarded =>
             /\ receipt[artifact] = ReceiptNone
             /\ \A task \in TaskIds : submission[task] # artifact)
       /\ (artifactState[artifact] = ArtifactPublished <=>
             \E task \in TaskIds : submission[task] = artifact)
  /\ \A task \in TaskIds : Cardinality(ArtifactsForTask(task)) <= 1
  /\ \A first \in TaskIds, second \in TaskIds :
       (submission[first] # NoArtifact /\
        submission[first] = submission[second]) => first = second

TerminalArtifactsSettled ==
  \A task \in TaskIds :
    TaskTerminal(task) =>
      \A artifact \in ArtifactsForTask(task) :
        artifactState[artifact] \in {ArtifactPublished, ArtifactDiscarded}

PublicationAtomic ==
  \A task \in TaskIds :
    /\ (submission[task] # NoArtifact =>
          /\ taskState[task] = Completed
          /\ artifactState[submission[task]] = ArtifactPublished
          /\ artifactTask[submission[task]] = task)
    /\ (taskState[task] = Completed =>
          messageAuthor[messages[task][Len(messages[task])]] = Agent)

ReceiptWellFormed ==
  /\ \A artifact \in ArtifactIds :
       receipt[artifact] # ReceiptNone =>
         artifactState[artifact] = ArtifactPublished
  /\ \A artifact \in ArtifactIds :
       receipt[artifact] = ReceiptSuccess =>
         artifactVersion[artifact] <= baseline[ArtifactWorkspace(artifact)]

BaselineWithinDraft ==
  \A workspace \in WorkspaceIds : baseline[workspace] <= draft[workspace]

BaselineJustified ==
  \A workspace \in WorkspaceIds :
    baseline[workspace] = 0 \/
      \E artifact \in ArtifactIds :
        /\ artifactState[artifact] = ArtifactPublished
        /\ ArtifactWorkspace(artifact) = workspace
        /\ receipt[artifact] = ReceiptSuccess
        /\ artifactVersion[artifact] = baseline[workspace]

NoForkedWorkspaceSuccess ==
  LET successful ==
        {artifact \in ArtifactIds : receipt[artifact] = ReceiptSuccess}
  IN \A first, second \in successful :
       /\ first # second
       /\ ArtifactWorkspace(first) = ArtifactWorkspace(second)
       /\ artifactBase[first] = artifactBase[second]
       /\ artifactVersion[first] > artifactBase[first]
       /\ artifactVersion[second] > artifactBase[second]
       => FALSE

AtMostOneArtifactContextPerWorkspace ==
  LET existing ==
        {artifact \in ArtifactIds :
           artifactState[artifact] # ArtifactAbsent}
  IN \A workspace \in WorkspaceIds :
       Cardinality(
         {taskContext[artifactTask[artifact]] :
            artifact \in
              {candidate \in existing :
                 ArtifactWorkspace(candidate) = workspace}}) <= 1

(***************************************************************************)
(* Temporal safety: append-only messages, immutable payloads/receipts,     *)
(* irreversible terminal Tasks, and monotonic workspace knowledge.         *)
(***************************************************************************)

TerminalTasksImmutable ==
  [][\A task \in TaskIds :
       TaskTerminal(task) => taskState'[task] = taskState[task]]_vars

TaskIdentityImmutable ==
  [][\A task \in TaskIds :
       TaskCreated(task) =>
         /\ taskContext'[task] = taskContext[task]
         /\ taskWorkspace'[task] = taskWorkspace[task]]_vars

TerminalTaskMessagesImmutable ==
  [][\A task \in TaskIds :
       TaskTerminal(task) => messages'[task] = messages[task]]_vars

MessagesAppendOnly ==
  [][\A task \in TaskIds :
       /\ Len(messages'[task]) >= Len(messages[task])
       /\ SubSeq(messages'[task], 1, Len(messages[task])) =
            messages[task]]_vars

MessageAuthorsImmutable ==
  [][\A message \in MessageIds :
       messageAuthor[message] # NoAuthor =>
         messageAuthor'[message] = messageAuthor[message]]_vars

ArtifactPayloadImmutable ==
  [][\A artifact \in ArtifactIds :
       artifactState[artifact] # ArtifactAbsent =>
         /\ artifactTask'[artifact] = artifactTask[artifact]
         /\ artifactBase'[artifact] = artifactBase[artifact]
         /\ artifactVersion'[artifact] = artifactVersion[artifact]
         /\ <<AgentKind, artifactBase'[artifact],
               artifactVersion'[artifact], artifact>> =
              ArtifactDigest(artifact)]_vars

PublishedArtifactsImmutable ==
  [][\A artifact \in ArtifactIds :
       artifactState[artifact] = ArtifactPublished =>
         artifactState'[artifact] = ArtifactPublished]_vars

DiscardedArtifactsImmutable ==
  [][\A artifact \in ArtifactIds :
       artifactState[artifact] = ArtifactDiscarded =>
         artifactState'[artifact] = ArtifactDiscarded]_vars

ReceiptsImmutable ==
  [][\A artifact \in ArtifactIds :
       receipt[artifact] # ReceiptNone =>
         receipt'[artifact] = receipt[artifact]]_vars

ApplySuccessRequiresVerifiedCAS ==
  [][\A artifact \in ArtifactIds :
       /\ receipt[artifact] = ReceiptNone
       /\ receipt'[artifact] = ReceiptSuccess
       => LET workspace == ArtifactWorkspace(artifact)
          IN /\ artifactState[artifact] = ArtifactPublished
             /\ \lnot dirty[workspace]
             /\ baseline[workspace] = artifactBase[artifact]
             /\ baseline'[workspace] = artifactVersion[artifact]
             /\ \A other \in WorkspaceIds \ {workspace} :
                  baseline'[other] = baseline[other]]_vars

BaselineChangesOnlyOnApplySuccess ==
  [][\A workspace \in WorkspaceIds :
       baseline'[workspace] # baseline[workspace] =>
         \E artifact \in ArtifactIds :
           /\ receipt[artifact] = ReceiptNone
           /\ receipt'[artifact] = ReceiptSuccess
           /\ ArtifactWorkspace(artifact) = workspace
           /\ baseline[workspace] = artifactBase[artifact]
           /\ baseline'[workspace] = artifactVersion[artifact]]_vars

BaselineMonotone ==
  [][\A workspace \in WorkspaceIds :
       baseline'[workspace] >= baseline[workspace]]_vars

DraftMonotone ==
  [][\A workspace \in WorkspaceIds :
       draft'[workspace] >= draft[workspace]]_vars

(***************************************************************************)
(* Conditional liveness. These properties require FairSpec; NoHumanFairSpec*)
(* intentionally checks safety only because a person may remain offline.   *)
(***************************************************************************)

EventuallyAllCreatedTasksTerminal == <>[]AllCreatedTasksTerminal

EventuallyEveryTaskCreatedAndTerminal ==
  <>[](\A task \in TaskIds : TaskCreated(task) /\ TaskTerminal(task))

SharedWorkspaceEventuallyResolves == <>SharedWorkspaceResolved

ParallelContextEventuallyWorking == <>ParallelContextReady

TwoInputRoundsEventuallyComplete == <>ConversationDone

EventuallyPublishedReceiptsDecided == <>[]AllPublishedReceiptsDecided

=============================================================================
