----------------------------- MODULE HumanSystem ----------------------------
(***************************************************************************)
(* Small composition model for HumanLLM and HumanAgent sharing one runtime   *)
(* and one caller workspace.  It intentionally does not merge their public  *)
(* lifecycles.  Payloads and file trees are abstracted to immutable versions. *)
(***************************************************************************)
EXTENDS HumanCommon, Naturals, Sequences, FiniteSets

CONSTANTS Scope, LLMItemID, AgentItemID,
          BaseVersion, NoVersion, LLMVersion1, LLMVersion2, AgentVersion

LLMVersions   == {LLMVersion1, LLMVersion2}
AgentVersions == {AgentVersion}
ChangeVersions == LLMVersions \union AgentVersions
Versions == {BaseVersion, NoVersion} \union ChangeVersions

ASSUME /\ Scope # LLMItemID
       /\ Scope # AgentItemID
       /\ LLMItemID # AgentItemID
       /\ Cardinality(Versions) = 5

IntentStates == {"none", "frozen", "published", "applied", "conflict"}
LLMResponseStates == {"open"} \union LLMResponseTerminalStates
AgentTaskStates == {"working", "input_required"} \union AgentTerminalStates

VARIABLES
  workspaceVersion, writes,
  llmKey, agentKey,
  llmResponse, agentTask, agentTerminated,
  llmDraft, llmDraftBase, llmCreated,
  llmIntent, llmIntentBase, llmIntentState,
  agentDraft, agentDraftBase, agentCreated,
  agentIntent, agentIntentBase, agentIntentState,
  successReceipts, conflicts, publishedArtifacts,
  llmBaseline, agentBaseline

vars == <<workspaceVersion, writes,
          llmKey, agentKey,
          llmResponse, agentTask, agentTerminated,
          llmDraft, llmDraftBase, llmCreated,
          llmIntent, llmIntentBase, llmIntentState,
          agentDraft, agentDraftBase, agentCreated,
          agentIntent, agentIntentBase, agentIntentState,
          successReceipts, conflicts, publishedArtifacts,
          llmBaseline, agentBaseline>>

Init ==
  /\ workspaceVersion = BaseVersion
  /\ writes = <<>>
  /\ llmKey = ScopedKey(LLMKind, Scope, LLMItemID)
  /\ agentKey = ScopedKey(AgentKind, Scope, AgentItemID)
  /\ llmResponse = "open"
  /\ agentTask = "working"
  /\ agentTerminated = FALSE
  /\ llmDraft = NoVersion
  /\ llmDraftBase = NoVersion
  /\ llmCreated = {}
  /\ llmIntent = NoVersion
  /\ llmIntentBase = NoVersion
  /\ llmIntentState = "none"
  /\ agentDraft = NoVersion
  /\ agentDraftBase = NoVersion
  /\ agentCreated = {}
  /\ agentIntent = NoVersion
  /\ agentIntentBase = NoVersion
  /\ agentIntentState = "none"
  /\ successReceipts = {}
  /\ conflicts = {}
  /\ publishedArtifacts = {}
  /\ llmBaseline = BaseVersion
  /\ agentBaseline = BaseVersion

(* --------------------------- HumanLLM surface -------------------------- *)

DraftLLM(version) ==
  /\ llmResponse = "open"
  /\ version \in LLMVersions \ llmCreated
  /\ version = LLMVersion1 \/ LLMVersion1 \in llmCreated
  /\ llmDraft = NoVersion \/ llmDraft = llmIntent
  /\ llmDraft' = version
  /\ llmDraftBase' = llmBaseline
  /\ llmCreated' = llmCreated \union {version}
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

FreezeLLM ==
  /\ llmResponse = "open"
  /\ llmDraft \in LLMVersions
  /\ llmIntent = NoVersion
  /\ llmIntent' = llmDraft
  /\ llmIntentBase' = llmDraftBase
  /\ llmIntentState' = "frozen"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

PublishLLMToolCall ==
  /\ llmIntentState = "frozen"
  /\ llmIntentState' = "published"
  /\ llmResponse' = "tool_calls"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

ApplyLLMSuccess ==
  /\ llmIntentState = "published"
  /\ workspaceVersion = llmIntentBase
  /\ workspaceVersion' = llmIntent
  /\ writes' = Append(writes, <<LLMKind, llmIntentBase, llmIntent>>)
  /\ successReceipts' = successReceipts \union {llmIntent}
  /\ llmIntentState' = "applied"
  /\ UNCHANGED <<llmKey, agentKey, llmResponse, agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 conflicts, publishedArtifacts, llmBaseline, agentBaseline>>

ApplyLLMConflict ==
  /\ llmIntentState = "published"
  /\ workspaceVersion # llmIntentBase
  /\ conflicts' = conflicts \union {llmIntent}
  /\ llmIntentState' = "conflict"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

ConfirmLLMReceipt ==
  /\ llmIntentState = "applied"
  /\ llmIntent \in successReceipts
  /\ llmBaseline' = llmIntent
  /\ llmIntent' = NoVersion
  /\ llmIntentBase' = NoVersion
  /\ llmIntentState' = "none"
  /\ IF llmDraft = llmIntent
        THEN /\ llmDraft' = NoVersion
             /\ llmDraftBase' = NoVersion
        ELSE UNCHANGED <<llmDraft, llmDraftBase>>
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated, llmCreated,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 agentBaseline>>

RebaseLLMAfterConflict ==
  /\ llmIntentState = "conflict"
  /\ llmDraft \in LLMVersions
  \* The mutable draft may already be newer than the frozen conflicting
  \* delivery. Rebase that current draft; never mutate the frozen payload.
  /\ llmDraftBase' = workspaceVersion
  /\ llmIntent' = NoVersion
  /\ llmIntentBase' = NoVersion
  /\ llmIntentState' = "none"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated,
                 llmDraft, llmCreated,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

OpenNextLLMResponse ==
  /\ IsLLMResponseTerminal(llmResponse)
  /\ llmIntent = NoVersion
  /\ llmDraft \in LLMVersions
  /\ llmResponse' = "open"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

FinishLLMText ==
  /\ llmResponse = "open"
  /\ llmIntent = NoVersion
  /\ llmDraft = NoVersion
  /\ llmResponse' = "text_final"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

(* -------------------------- HumanAgent surface ------------------------- *)

DraftAgentArtifact ==
  /\ agentTask \in {"working", "input_required"}
  /\ AgentVersion \notin agentCreated
  /\ agentDraft = NoVersion
  /\ agentDraft' = AgentVersion
  /\ agentDraftBase' = agentBaseline
  /\ agentCreated' = agentCreated \union {AgentVersion}
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

FreezeAgentArtifact ==
  /\ agentTask = "working"
  /\ agentDraft = AgentVersion
  /\ agentIntent = NoVersion
  /\ agentIntent' = agentDraft
  /\ agentIntentBase' = agentDraftBase
  /\ agentIntentState' = "frozen"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

PublishAgentSubmission ==
  /\ agentIntentState = "frozen"
  /\ agentIntentState' = "published"
  /\ publishedArtifacts' = publishedArtifacts \union {agentIntent}
  \* The authoritative final Submission and Task completion are one durable
  \* visibility boundary. Streaming previews are deliberately not modeled as
  \* published final Artifacts.
  /\ agentTask' = "completed"
  /\ agentTerminated' = TRUE
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase,
                 successReceipts, conflicts, llmBaseline, agentBaseline>>

(* The write body is deliberately not a Next action. Every public action that *)
(* invokes it must first supply the workspace/base CAS guard. Keeping the race *)
(* wrapper separate lets its oracle be mutation-tested without weakening Spec. *)
ApplyAgentWrite ==
  /\ agentIntentState = "published"
  /\ workspaceVersion' = agentIntent
  /\ writes' = Append(writes, <<AgentKind, agentIntentBase, agentIntent>>)
  /\ successReceipts' = successReceipts \union {agentIntent}
  /\ agentIntentState' = "applied"
  /\ UNCHANGED <<llmKey, agentKey, llmResponse, agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase,
                 conflicts, publishedArtifacts, llmBaseline, agentBaseline>>

ApplyAgentSuccess ==
  /\ workspaceVersion = agentIntentBase
  /\ ApplyAgentWrite

ApplyAgentConflict ==
  /\ agentIntentState = "published"
  /\ workspaceVersion # agentIntentBase
  /\ conflicts' = conflicts \union {agentIntent}
  /\ agentIntentState' = "conflict"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase,
                 successReceipts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

ConfirmAgentReceipt ==
  /\ agentIntentState = "applied"
  /\ agentIntent \in successReceipts
  /\ agentBaseline' = agentIntent
  /\ agentDraft' = NoVersion
  /\ agentDraftBase' = NoVersion
  /\ agentIntent' = NoVersion
  /\ agentIntentBase' = NoVersion
  /\ agentIntentState' = "none"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentCreated, successReceipts, conflicts,
                 publishedArtifacts, llmBaseline>>

AcknowledgeAgentConflict ==
  /\ agentIntentState = "conflict"
  /\ agentDraft' = NoVersion
  /\ agentDraftBase' = NoVersion
  /\ agentIntent' = NoVersion
  /\ agentIntentBase' = NoVersion
  /\ agentIntentState' = "none"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTask, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentCreated, successReceipts, conflicts,
                 publishedArtifacts, llmBaseline, agentBaseline>>

AgentRequestsInput ==
  /\ agentTask = "working"
  /\ agentTask' = "input_required"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

AgentReply ==
  /\ agentTask = "input_required"
  /\ agentTask' = "working"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse, agentTerminated,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

CompleteAgentContent ==
  /\ agentTask = "working"
  /\ agentCreated = {}
  /\ agentDraft = NoVersion
  /\ agentIntent = NoVersion
  /\ agentTask' = "completed"
  /\ agentTerminated' = TRUE
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentDraft, agentDraftBase, agentCreated,
                 agentIntent, agentIntentBase, agentIntentState,
                 successReceipts, conflicts, publishedArtifacts,
                 llmBaseline, agentBaseline>>

CancelAgent ==
  /\ agentTask \in {"working", "input_required"}
  /\ agentIntentState \in {"none", "frozen"}
  /\ agentTask' = "canceled"
  /\ agentTerminated' = TRUE
  /\ agentDraft' = NoVersion
  /\ agentDraftBase' = NoVersion
  /\ agentIntent' = NoVersion
  /\ agentIntentBase' = NoVersion
  /\ agentIntentState' = "none"
  /\ UNCHANGED <<workspaceVersion, writes, llmKey, agentKey,
                 llmResponse,
                 llmDraft, llmDraftBase, llmCreated,
                 llmIntent, llmIntentBase, llmIntentState,
                 agentCreated, successReceipts, conflicts,
                 publishedArtifacts, llmBaseline, agentBaseline>>

Quiescent ==
  /\ IsLLMResponseTerminal(llmResponse)
  /\ IsAgentTerminal(agentTask)
  /\ llmDraft = NoVersion
  /\ llmIntent = NoVersion
  /\ agentDraft = NoVersion
  /\ agentIntent = NoVersion

Terminating == Quiescent /\ UNCHANGED vars

Next ==
  \/ \E version \in LLMVersions : DraftLLM(version)
  \/ FreezeLLM \/ PublishLLMToolCall
  \/ ApplyLLMSuccess \/ ApplyLLMConflict \/ ConfirmLLMReceipt
  \/ RebaseLLMAfterConflict \/ OpenNextLLMResponse \/ FinishLLMText
  \/ DraftAgentArtifact \/ FreezeAgentArtifact \/ PublishAgentSubmission
  \/ ApplyAgentSuccess \/ ApplyAgentConflict \/ ConfirmAgentReceipt
  \/ AcknowledgeAgentConflict
  \/ AgentRequestsInput \/ AgentReply \/ CompleteAgentContent \/ CancelAgent
  \/ Terminating

Spec == Init /\ [][Next]_vars

(* --------------------- Forced cross-surface CAS race -------------------- *)

(* These wrappers admit exactly one LLM change and one Agent artifact. Weak  *)
(* fairness on every preparation step forces both immutable intents through  *)
(* draft/freeze/publish before RaceResolveCAS may mutate the workspace.      *)
RaceDraftLLM == DraftLLM(LLMVersion1)
RaceDraftAgent == DraftAgentArtifact
RaceFreezeLLM == FreezeLLM
RaceFreezeAgent == FreezeAgentArtifact
RacePublishLLM == PublishLLMToolCall
RacePublishAgent == PublishAgentSubmission

RaceCASStates == {"published", "applied", "conflict"}

(* Before the first CAS both sides are published. After it, the winner is     *)
(* applied and the still-published loser must take the conflict transition.   *)
RaceApplying ==
  /\ llmIntentState \in RaceCASStates
  /\ agentIntentState \in RaceCASStates
  /\ \/ llmIntentState = "published"
     \/ agentIntentState = "published"

(* Race-only Agent success path. The base guard is intentionally repeated at *)
(* this boundary so a mutant can attack RaceSpec without changing Spec.      *)
RaceApplyAgentSuccess ==
  /\ workspaceVersion = agentIntentBase
  /\ ApplyAgentWrite

RaceResolveCAS ==
  /\ RaceApplying
  /\ \/ ApplyLLMSuccess
     \/ ApplyLLMConflict
     \/ RaceApplyAgentSuccess
     \/ ApplyAgentConflict

RaceResolved ==
  \/ /\ llmIntentState = "applied"
     /\ agentIntentState = "conflict"
  \/ /\ llmIntentState = "conflict"
     /\ agentIntentState = "applied"

RaceTerminating == RaceResolved /\ UNCHANGED vars

RaceNext ==
  \/ RaceDraftLLM
  \/ RaceDraftAgent
  \/ RaceFreezeLLM
  \/ RaceFreezeAgent
  \/ RacePublishLLM
  \/ RacePublishAgent
  \/ RaceResolveCAS
  \/ RaceTerminating

RaceSpec ==
  /\ Init
  /\ [][RaceNext]_vars
  /\ WF_vars(RaceDraftLLM)
  /\ WF_vars(RaceDraftAgent)
  /\ WF_vars(RaceFreezeLLM)
  /\ WF_vars(RaceFreezeAgent)
  /\ WF_vars(RacePublishLLM)
  /\ WF_vars(RacePublishAgent)
  /\ WF_vars(RaceResolveCAS)

(* ------------------------------- Safety ------------------------------- *)

TypeOK ==
  /\ workspaceVersion \in {BaseVersion} \union ChangeVersions
  /\ writes \in Seq(Surfaces \X Versions \X ChangeVersions)
  /\ llmKey \in Surfaces \X {Scope} \X {LLMItemID, AgentItemID}
  /\ agentKey \in Surfaces \X {Scope} \X {LLMItemID, AgentItemID}
  /\ llmResponse \in LLMResponseStates
  /\ agentTask \in AgentTaskStates
  /\ agentTerminated \in BOOLEAN
  /\ llmDraft \in {NoVersion} \union LLMVersions
  /\ llmDraftBase \in Versions
  /\ llmCreated \subseteq LLMVersions
  /\ llmIntent \in {NoVersion} \union LLMVersions
  /\ llmIntentBase \in Versions
  /\ llmIntentState \in IntentStates
  /\ agentDraft \in {NoVersion} \union AgentVersions
  /\ agentDraftBase \in Versions
  /\ agentCreated \subseteq AgentVersions
  /\ agentIntent \in {NoVersion} \union AgentVersions
  /\ agentIntentBase \in Versions
  /\ agentIntentState \in IntentStates
  /\ successReceipts \subseteq ChangeVersions
  /\ conflicts \subseteq ChangeVersions
  /\ publishedArtifacts \subseteq AgentVersions
  /\ llmBaseline \in {BaseVersion} \union LLMVersions
  /\ agentBaseline \in {BaseVersion} \union AgentVersions

SurfaceIsolation ==
  /\ KeyKind(llmKey) = LLMKind
  /\ KeyKind(agentKey) = AgentKind
  /\ KeyScope(llmKey) = KeyScope(agentKey)
  /\ KeyID(llmKey) # KeyID(agentKey)
  /\ llmKey # agentKey
  /\ \A index \in DOMAIN writes :
       IF writes[index][1] = LLMKind
         THEN writes[index][3] \in LLMVersions
         ELSE writes[index][3] \in AgentVersions

ValidWriteChain ==
  /\ workspaceVersion =
       IF Len(writes) = 0 THEN BaseVersion ELSE writes[Len(writes)][3]
  /\ \A index \in DOMAIN writes :
       writes[index][2] =
         IF index = 1 THEN BaseVersion ELSE writes[index - 1][3]
  /\ \A left, right \in DOMAIN writes :
       left # right => writes[left][3] # writes[right][3]

BaselineConfirmed ==
  /\ llmBaseline = BaseVersion \/ llmBaseline \in successReceipts \cap LLMVersions
  /\ agentBaseline = BaseVersion \/ agentBaseline \in successReceipts \cap AgentVersions
  \* A version may first conflict, then be explicitly rebased and succeed.
  \* Historical conflicts therefore need not be disjoint from later receipts.

FrozenPayloadImmutable ==
  /\ (llmIntentState = "none") <=> (llmIntent = NoVersion)
  /\ (agentIntentState = "none") <=> (agentIntent = NoVersion)
  /\ llmIntent # NoVersion =>
       /\ llmIntent \in llmCreated
       /\ llmIntentBase # NoVersion
  /\ agentIntent # NoVersion =>
       /\ agentIntent \in agentCreated
       /\ agentIntentBase # NoVersion
  /\ publishedArtifacts \subseteq agentCreated

ArtifactAtomic ==
  /\ AgentVersion \in publishedArtifacts => agentTask = "completed"
  /\ agentTask = "completed" /\ agentCreated # {} =>
       AgentVersion \in publishedArtifacts

TerminalStable == agentTerminated <=> IsAgentTerminal(agentTask)

PendingNewerDraftPreserved ==
  /\ LLMVersion2 \in llmCreated
  /\ LLMVersion2 \notin successReceipts
  /\ LLMVersion2 \notin conflicts
  => \/ llmDraft = LLMVersion2
     \/ llmIntent = LLMVersion2

RaceUsesOneSharedBase ==
  /\ llmCreated \subseteq {LLMVersion1}
  /\ (llmDraft # NoVersion => llmDraftBase = BaseVersion)
  /\ (llmIntent # NoVersion => llmIntentBase = BaseVersion)
  /\ (agentDraft # NoVersion => agentDraftBase = BaseVersion)
  /\ (agentIntent # NoVersion => agentIntentBase = BaseVersion)

RaceCASWaitsForBothPublications ==
  (Len(writes) > 0 \/ conflicts # {}) =>
    /\ llmIntentState \in RaceCASStates
    /\ agentIntentState \in RaceCASStates
    /\ llmIntent = LLMVersion1
    /\ agentIntent = AgentVersion

RaceCASOutcome ==
  /\ Len(writes) = Cardinality(successReceipts)
  /\ Len(writes) <= 1
  /\ Cardinality(successReceipts) <= 1
  /\ Cardinality(conflicts) <= 1
  /\ successReceipts \cap conflicts = {}
  /\ (llmIntentState = "applied" <=> LLMVersion1 \in successReceipts)
  /\ (agentIntentState = "applied" <=> AgentVersion \in successReceipts)
  /\ (llmIntentState = "conflict" <=> LLMVersion1 \in conflicts)
  /\ (agentIntentState = "conflict" <=> AgentVersion \in conflicts)
  /\ (successReceipts # {} /\ conflicts # {} <=> RaceResolved)

RaceEventuallyResolves == <>RaceResolved

WritesAppendOnly ==
  [][ /\ Len(writes') >= Len(writes)
      /\ SubSeq(writes', 1, Len(writes)) = writes ]_vars

ArtifactsAppendOnly == [][publishedArtifacts \subseteq publishedArtifacts']_vars
KeysStable == [][llmKey' = llmKey /\ agentKey' = agentKey]_vars

(* Once an intent exists, its payload version and the base captured with it  *)
(* are immutable.  The only permitted change is the explicit reset to       *)
(* NoVersion performed after a receipt, conflict acknowledgement, or cancel. *)
FrozenIntentsChangeOnlyOnReset ==
  [][ /\ (llmIntent # NoVersion /\ llmIntent' # NoVersion =>
            /\ llmIntent' = llmIntent
            /\ llmIntentBase' = llmIntentBase)
      /\ (llmIntent # NoVersion /\ llmIntent' = NoVersion =>
            llmIntentState \in {"applied", "conflict"})
      /\ (agentIntent # NoVersion /\ agentIntent' # NoVersion =>
            /\ agentIntent' = agentIntent
            /\ agentIntentBase' = agentIntentBase)
      /\ (agentIntent # NoVersion /\ agentIntent' = NoVersion =>
            \/ agentIntentState \in {"applied", "conflict"}
            \/ /\ agentIntentState = "frozen"
               /\ agentTask' = "canceled"
               /\ agentTerminated') ]_vars

(* A baseline transition is tied to the exact currently-applied intent and  *)
(* its durable success receipt.  Conversely, consuming an applied intent    *)
(* must advance to that exact version; it may not silently keep or select an *)
(* older successful version from the historical receipt set.                *)
BaselineChangesOnlyOnExactReceipt ==
  [][ /\ (llmBaseline' # llmBaseline =>
            /\ llmIntentState = "applied"
            /\ llmIntent \in successReceipts
            /\ llmBaseline' = llmIntent)
      /\ (llmIntentState = "applied" /\ llmIntentState' = "none" =>
            /\ llmIntent \in successReceipts
            /\ llmBaseline' = llmIntent)
      /\ (agentBaseline' # agentBaseline =>
            /\ agentIntentState = "applied"
            /\ agentIntent \in successReceipts
            /\ agentBaseline' = agentIntent)
      /\ (agentIntentState = "applied" /\ agentIntentState' = "none" =>
            /\ agentIntent \in successReceipts
            /\ agentBaseline' = agentIntent) ]_vars

=============================================================================
