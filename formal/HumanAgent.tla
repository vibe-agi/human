---------------------------- MODULE HumanAgent ----------------------------
(***************************************************************************)
(* Formal model of the Human Agent delivery protocol.                     *)
(* See docs/phase2-async-mode.md (section 7) for the theory.              *)
(*                                                                         *)
(* Roles:                                                                  *)
(*   - humand: single-writer authority (taskState, turns, latest, ...)    *)
(*   - Bob (worker, HUMAN transitions): Accept/RejectTask/FinishTurn/     *)
(*     CompleteTask/ConfirmRewind/RejectRewind                             *)
(*   - Caller machine side, human-mcp (MACHINE transitions):               *)
(*     CallerFetch/CallerApply*                                            *)
(*   - Caller human side (HUMAN/ADVERSARY transitions): Reply, Cancel,     *)
(*     RequestRewind, LocalEdit (adversary), ResolveConflict               *)
(*                                                                         *)
(* Abstraction: file trees are abstracted to delivery version numbers.    *)
(* Turn commits are immutable => version numbers are never reused          *)
(* (bobNext is monotonic), mirroring git content addressing.               *)
(* apply/revert on a clean workdir always succeeds (axioms A1/A2);         *)
(* on a dirty workdir an abstract runtime oracle returns verified-ok or    *)
(* explicit conflict. This model assumes, but does not verify, that oracle. *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS MaxTurns,       \* max deliveries Bob can make (model bound)
          MaxLocalEdits,  \* max adversary edits on caller workdir
          MaxRewinds      \* max rewind requests

VARIABLES
  taskState,   \* A2A task lifecycle state (authority: humand)
  turns,       \* audit: sequence of ALL versions ever delivered (append-only)
  latest,      \* current anchor: newest live delivery (0 = base)
  superseded,  \* versions rolled back by rewind (marked, never deleted)
  rewindTo,    \* target version of a pending rewind
  rewinds,     \* rewind counter (model bound)
  bobNext,     \* next delivery version (monotonic, never reused)
  applied,     \* newest version successfully applied+verified at caller
  dirty,       \* caller workdir has local edits
  conflict,    \* caller is in explicit CONFLICT state
  inflight,    \* caller has fetched a version not yet applied
  fetched,     \* the fetched version (meaningful iff inflight)
  localEdits   \* adversary edit counter (model bound)

vars == <<taskState, turns, latest, superseded, rewindTo, rewinds, bobNext,
          applied, dirty, conflict, inflight, fetched, localEdits>>

Terminal == {"completed", "canceled", "rejected", "failed"}
States   == {"submitted", "working", "input_required", "rewind_pending"}
              \union Terminal

Range(s)  == { s[i] : i \in DOMAIN s }
Delivered == {0} \union Range(turns)
Live      == Delivered \ superseded   \* versions on the current valid chain

TypeOK ==
  /\ taskState \in States
  /\ turns \in Seq(1..MaxTurns)
  /\ latest \in 0..MaxTurns
  /\ superseded \subseteq 1..MaxTurns
  /\ rewindTo \in 0..MaxTurns
  /\ rewinds \in 0..MaxRewinds
  /\ bobNext \in 1..(MaxTurns+1)
  /\ applied \in 0..MaxTurns
  /\ dirty \in BOOLEAN
  /\ conflict \in BOOLEAN
  /\ inflight \in BOOLEAN
  /\ fetched \in 0..MaxTurns
  /\ localEdits \in 0..MaxLocalEdits

Init ==
  /\ taskState = "submitted"
  /\ turns = <<>>
  /\ latest = 0
  /\ superseded = {}
  /\ rewindTo = 0
  /\ rewinds = 0
  /\ bobNext = 1
  /\ applied = 0
  /\ dirty = FALSE
  /\ conflict = FALSE
  /\ inflight = FALSE
  /\ fetched = 0
  /\ localEdits = 0

(* ------------------------- Bob: HUMAN transitions ---------------------- *)

Accept ==
  /\ taskState = "submitted"
  /\ taskState' = "working"
  /\ UNCHANGED <<turns, latest, superseded, rewindTo, rewinds, bobNext,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

RejectTask ==
  /\ taskState = "submitted"
  /\ taskState' = "rejected"
  /\ UNCHANGED <<turns, latest, superseded, rewindTo, rewinds, bobNext,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

\* Bob ends a turn: commit becomes the new live anchor. Audit is append-only.
FinishTurn ==
  /\ taskState = "working"
  /\ bobNext <= MaxTurns
  /\ turns' = Append(turns, bobNext)
  /\ latest' = bobNext
  /\ bobNext' = bobNext + 1
  /\ taskState' = "input_required"
  /\ UNCHANGED <<superseded, rewindTo, rewinds,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

CompleteTask ==
  /\ taskState \in {"working", "input_required"}
  /\ taskState' = "completed"
  /\ UNCHANGED <<turns, latest, superseded, rewindTo, rewinds, bobNext,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

\* Bob declares the task unachievable (attaches a reason in the real system).
Fail ==
  /\ taskState \in {"working", "input_required"}
  /\ taskState' = "failed"
  /\ UNCHANGED <<turns, latest, superseded, rewindTo, rewinds, bobNext,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

\* Bob confirms a rewind: anchor moves back, skipped versions are MARKED
\* superseded (audit keeps them), transactionally in one authority step.
ConfirmRewind ==
  /\ taskState = "rewind_pending"
  /\ latest' = rewindTo
  /\ superseded' = superseded \union { v \in Range(turns) : v > rewindTo }
  /\ taskState' = "input_required"
  /\ UNCHANGED <<turns, rewindTo, rewinds, bobNext,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

\* Bob holds veto power: a rewind request may be refused.
RejectRewind ==
  /\ taskState = "rewind_pending"
  /\ taskState' = "input_required"
  /\ UNCHANGED <<turns, latest, superseded, rewindTo, rewinds, bobNext,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

(* -------------------- Caller: HUMAN / ADVERSARY transitions ------------ *)

Reply ==
  /\ taskState = "input_required"
  /\ taskState' = "working"
  /\ UNCHANGED <<turns, latest, superseded, rewindTo, rewinds, bobNext,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

Cancel ==
  /\ taskState \in {"submitted", "working", "input_required", "rewind_pending"}
  /\ taskState' = "canceled"
  /\ UNCHANGED <<turns, latest, superseded, rewindTo, rewinds, bobNext,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

\* Caller may only target a version on the current live chain, strictly
\* older than the anchor.
RequestRewind ==
  /\ taskState \in {"working", "input_required"}
  /\ rewinds < MaxRewinds
  /\ \E v \in Live : /\ v < latest
                     /\ rewindTo' = v
  /\ rewinds' = rewinds + 1
  /\ taskState' = "rewind_pending"
  /\ UNCHANGED <<turns, latest, superseded, bobNext,
                 applied, dirty, conflict, inflight, fetched, localEdits>>

\* Adversary: caller edits their workdir behind our back.
LocalEdit ==
  /\ ~conflict
  /\ localEdits < MaxLocalEdits
  /\ dirty' = TRUE
  /\ localEdits' = localEdits + 1
  /\ UNCHANGED <<taskState, turns, latest, superseded, rewindTo, rewinds,
                 bobNext, applied, conflict, inflight, fetched>>

\* Human resolves an explicit conflict (e.g. stashes/commits local edits).
ResolveConflict ==
  /\ conflict
  /\ conflict' = FALSE
  /\ dirty' = FALSE
  /\ UNCHANGED <<taskState, turns, latest, superseded, rewindTo, rewinds,
                 bobNext, applied, inflight, fetched, localEdits>>

(* --------------------- Caller: MACHINE transitions --------------------- *)
\* human-mcp pulls the authoritative latest. Pull-based: stale reads are
\* possible (humand may move on after the fetch) - that is the point.

CallerFetch ==
  /\ ~inflight
  /\ ~conflict
  /\ applied # latest
  /\ inflight' = TRUE
  /\ fetched' = latest
  /\ UNCHANGED <<taskState, turns, latest, superseded, rewindTo, rewinds,
                 bobNext, applied, dirty, conflict, localEdits>>

\* Axioms A1/A2: on a clean workdir revert+apply always succeeds.
CallerApplyClean ==
  /\ inflight /\ ~conflict /\ ~dirty
  /\ applied' = fetched
  /\ inflight' = FALSE
  /\ UNCHANGED <<taskState, turns, latest, superseded, rewindTo, rewinds,
                 bobNext, dirty, conflict, fetched, localEdits>>

\* Dirty workdir, nondeterministic outcome 1: local edits do not touch
\* patched files; blob-hash verification (X-04) passes.
CallerApplyDirtyOK ==
  /\ inflight /\ ~conflict /\ dirty
  /\ applied' = fetched
  /\ inflight' = FALSE
  /\ UNCHANGED <<taskState, turns, latest, superseded, rewindTo, rewinds,
                 bobNext, dirty, conflict, fetched, localEdits>>

\* Dirty workdir, nondeterministic outcome 2: explicit conflict.
\* Fail-explicit: the workdir is NOT silently corrupted; applied unchanged.
CallerApplyDirtyFail ==
  /\ inflight /\ ~conflict /\ dirty
  /\ conflict' = TRUE
  /\ inflight' = FALSE
  /\ UNCHANGED <<taskState, turns, latest, superseded, rewindTo, rewinds,
                 bobNext, applied, dirty, fetched, localEdits>>

(* ------------------------------ Glue ----------------------------------- *)

Quiescent == /\ taskState \in Terminal
             /\ ~inflight
             /\ ~conflict
             /\ applied = latest

Terminating == Quiescent /\ UNCHANGED vars

Next ==
  \/ Accept \/ RejectTask \/ FinishTurn \/ CompleteTask \/ Fail
  \/ ConfirmRewind \/ RejectRewind
  \/ Reply \/ Cancel \/ RequestRewind \/ LocalEdit \/ ResolveConflict
  \/ CallerFetch \/ CallerApplyClean \/ CallerApplyDirtyOK \/ CallerApplyDirtyFail
  \/ Terminating

Spec == Init /\ [][Next]_vars

\* Machine-side fairness: human-mcp's automatic behavior.
MachineFairness ==
  /\ WF_vars(CallerFetch)
  /\ WF_vars(CallerApplyClean)
  /\ WF_vars(CallerApplyDirtyOK \/ CallerApplyDirtyFail)

\* Human-side fairness: people eventually act.
HumanFairness ==
  /\ WF_vars(Accept \/ RejectTask)
  /\ WF_vars(FinishTurn \/ CompleteTask)
  /\ WF_vars(CompleteTask)
  /\ WF_vars(ConfirmRewind \/ RejectRewind)
  /\ WF_vars(ResolveConflict)

FairSpec        == Spec /\ MachineFairness /\ HumanFairness
NoHumanFairSpec == Spec /\ MachineFairness

(* --------------------------- Safety invariants ------------------------- *)

\* The anchor always points at a live (never superseded) real delivery.
LatestValid == latest \in Delivered /\ latest \notin superseded

\* Caller never holds a ghost version: whatever is applied or in flight
\* was really delivered once. This does not verify the runtime apply oracle.
AppliedReal == applied \in Delivered /\ (inflight => fetched \in Delivered)

SupersededOK == superseded \subseteq Range(turns)

\* While a rewind is pending, its target stays live and strictly older.
RewindPendingOK ==
  taskState = "rewind_pending" => (rewindTo \in Live /\ rewindTo < latest)

(* --------------------------- Action properties ------------------------- *)

\* Audit is append-only: history is never rewritten or shortened.
TurnsAppendOnly ==
  [][ /\ Len(turns') >= Len(turns)
      /\ SubSeq(turns', 1, Len(turns)) = turns ]_vars

\* Rewind marks, never unmarks/deletes.
SupersededMonotone == [][ superseded \subseteq superseded' ]_vars

(* --------------------------- Liveness ---------------------------------- *)

\* The task eventually reaches a terminal state (needs HUMAN fairness).
EventuallyTerminal == <>(taskState \in Terminal)

\* Under fair automatic pulls, the recorded applied artifact version
\* eventually converges to the authoritative anchor and stays there.
EventuallyConsistent == <>[](applied = latest /\ ~conflict)

=============================================================================
