----------------------- MODULE HumanWorkerSequence ------------------------
(***************************************************************************)
(* Exact finite harness for the worker WebSocket sequence/ACK contract.     *)
(*                                                                         *)
(* Two client events are simultaneously in flight: a late event at client  *)
(* sequence 1 and a healthy follower at sequence 2. The server already has  *)
(* an Assignment frame queued before it processes either event. Processing  *)
(* the late event enqueues event_rejected(ack=1); processing the follower    *)
(* enqueues ack(ack=2). ACK watermarks are bound when frames enter the FIFO, *)
(* not when the socket writer dequeues them. The client atomically moves the*)
(* rejected event to its durable inbox while deleting the cumulative prefix.*)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

Late     == "late"
Follower == "follower"
Events   == {Late, Follower}

AssignmentFrame == "assignment"
PostRejectionAssignmentFrame == "post_rejection_assignment"
RejectionFrame  == "event_rejected"
AckFrame        == "ack"
NoFrameKind     == "none"
FrameKinds == {AssignmentFrame, PostRejectionAssignmentFrame,
                RejectionFrame, AckFrame, NoFrameKind}

ClientSeq(event) == IF event = Late THEN 1 ELSE 2
Frame(kind, ack) == [kind |-> kind, ack |-> ack]
NoFrame == Frame(NoFrameKind, 0)
Frames == {Frame(kind, ack) : kind \in FrameKinds, ack \in 0..2}

VARIABLES
  incoming,          \* server-side ordered client frames
  serverCommitted,   \* highest client seq durably decided
  serverLastQueuedAck, \* monotonic watermark held by outboundQueue
  serverQueue,       \* server outbound FIFO with bound ACK snapshots
  postRejectionAssignmentQueued,
  wireFrame,         \* one FIFO head currently delivered to the client
  effects,
  rejections,
  clientOutbox,
  clientInflight,
  clientRejectedInbox,
  clientDeleted,
  ackHistory

vars == <<incoming, serverCommitted, serverLastQueuedAck, serverQueue,
          postRejectionAssignmentQueued, wireFrame,
          effects, rejections, clientOutbox, clientInflight,
          clientRejectedInbox, clientDeleted, ackHistory>>

Init ==
  /\ incoming = <<Late, Follower>>
  /\ serverCommitted = 0
  /\ serverLastQueuedAck = 0
  \* This old frame is ahead of the future rejection. Its ack=0 must remain
  \* frozen even if both client events commit before the writer dequeues it.
  /\ serverQueue = <<Frame(AssignmentFrame, 0)>>
  /\ postRejectionAssignmentQueued = FALSE
  /\ wireFrame = NoFrame
  /\ effects = {}
  /\ rejections = {}
  /\ clientOutbox = Events
  /\ clientInflight = Events
  /\ clientRejectedInbox = {}
  /\ clientDeleted = {}
  /\ ackHistory = <<>>

ProcessLate ==
  /\ Len(incoming) > 0
  /\ Head(incoming) = Late
  /\ rejections' = rejections \union {Late}
  /\ serverCommitted' = 1
  /\ serverLastQueuedAck' = 1
  /\ serverQueue' = Append(serverQueue, Frame(RejectionFrame, 1))
  /\ incoming' = Tail(incoming)
  /\ UNCHANGED <<postRejectionAssignmentQueued, wireFrame, effects,
                  clientOutbox, clientInflight,
                  clientRejectedInbox, clientDeleted, ackHistory>>

ProcessFollower ==
  /\ Len(incoming) > 0
  /\ Head(incoming) = Follower
  /\ serverCommitted = 1
  /\ effects' = effects \union {Follower}
  /\ serverCommitted' = 2
  /\ serverLastQueuedAck' = 2
  /\ serverQueue' = Append(serverQueue, Frame(AckFrame, 2))
  /\ incoming' = Tail(incoming)
  /\ UNCHANGED <<postRejectionAssignmentQueued, wireFrame, rejections,
                  clientOutbox, clientInflight,
                  clientRejectedInbox, clientDeleted, ackHistory>>

(* A concurrent assignment producer took a stale lastCommitted snapshot of *)
(* zero after the rejection was committed. outboundQueue must promote that  *)
(* snapshot to its own last queued watermark before appending the frame.     *)
QueuePostRejectionAssignment ==
  /\ Late \in rejections
  /\ \lnot postRejectionAssignmentQueued
  /\ serverQueue' =
       Append(serverQueue,
              Frame(PostRejectionAssignmentFrame, serverLastQueuedAck))
  /\ postRejectionAssignmentQueued' = TRUE
  /\ UNCHANGED <<incoming, serverCommitted, serverLastQueuedAck, wireFrame,
                  effects, rejections, clientOutbox, clientInflight,
                  clientRejectedInbox, clientDeleted, ackHistory>>

WriteFIFOHead ==
  /\ Len(serverQueue) > 0
  /\ wireFrame = NoFrame
  /\ wireFrame' = Head(serverQueue)
  /\ serverQueue' = Tail(serverQueue)
  /\ UNCHANGED <<incoming, serverCommitted, serverLastQueuedAck,
                  postRejectionAssignmentQueued, effects, rejections,
                  clientOutbox, clientInflight, clientRejectedInbox,
                  clientDeleted, ackHistory>>

ReceiveServerFrame ==
  /\ wireFrame # NoFrame
  /\ wireFrame.ack <= serverCommitted
  /\ (wireFrame.kind = RejectionFrame => Late \in rejections)
  /\ LET acknowledged ==
           {event \in clientInflight : ClientSeq(event) <= wireFrame.ack}
     IN /\ clientOutbox' = clientOutbox \ acknowledged
        /\ clientInflight' = clientInflight \ acknowledged
        /\ clientDeleted' = clientDeleted \union acknowledged
  /\ clientRejectedInbox' =
       IF wireFrame.kind = RejectionFrame
       THEN clientRejectedInbox \union {Late}
       ELSE clientRejectedInbox
  /\ ackHistory' = Append(ackHistory, wireFrame.ack)
  /\ wireFrame' = NoFrame
  /\ UNCHANGED <<incoming, serverCommitted, serverLastQueuedAck, serverQueue,
                  postRejectionAssignmentQueued,
                  effects, rejections>>

Quiescent ==
  /\ incoming = <<>>
  /\ serverQueue = <<>>
  /\ wireFrame = NoFrame
  /\ postRejectionAssignmentQueued
  /\ clientOutbox = {}
  /\ clientInflight = {}

Terminating == Quiescent /\ UNCHANGED vars

Next ==
  \/ ProcessLate
  \/ ProcessFollower
  \/ QueuePostRejectionAssignment
  \/ WriteFIFOHead
  \/ ReceiveServerFrame
  \/ Terminating

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(ProcessLate)
  /\ WF_vars(ProcessFollower)
  /\ WF_vars(QueuePostRejectionAssignment)
  /\ WF_vars(WriteFIFOHead)
  /\ WF_vars(ReceiveServerFrame)

TypeOK ==
  /\ incoming \in Seq(Events)
  /\ serverCommitted \in 0..2
  /\ serverLastQueuedAck \in 0..2
  /\ serverQueue \in Seq(Frames)
  /\ postRejectionAssignmentQueued \in BOOLEAN
  /\ wireFrame \in Frames
  /\ effects \subseteq Events
  /\ rejections \subseteq Events
  /\ clientOutbox \subseteq Events
  /\ clientInflight \subseteq Events
  /\ clientRejectedInbox \subseteq Events
  /\ clientDeleted \subseteq Events
  /\ ackHistory \in Seq(0..2)

ServerDecisionsAreExact ==
  /\ effects \subseteq {Follower}
  /\ rejections \subseteq {Late}
  /\ serverCommitted =
       IF Follower \in effects THEN 2
       ELSE IF Late \in rejections THEN 1 ELSE 0

QueuedACKTracksDecisions ==
  /\ serverLastQueuedAck <= serverCommitted
  /\ (Late \in rejections => serverLastQueuedAck >= 1)
  /\ (Follower \in effects => serverLastQueuedAck = 2)

OutboundACKsBoundAtEnqueue ==
  /\ \A index \in DOMAIN serverQueue :
       /\ serverQueue[index].ack <= serverCommitted
       /\ (serverQueue[index].kind = AssignmentFrame =>
             serverQueue[index].ack = 0)
       /\ (serverQueue[index].kind = PostRejectionAssignmentFrame =>
             serverQueue[index].ack >= 1)
  /\ (wireFrame.kind = AssignmentFrame => wireFrame.ack = 0)
  /\ (wireFrame.kind = PostRejectionAssignmentFrame => wireFrame.ack >= 1)

OutboundACKsMonotone ==
  /\ \A left, right \in DOMAIN serverQueue :
       left < right => serverQueue[left].ack <= serverQueue[right].ack
  /\ \A left, right \in DOMAIN ackHistory :
       left < right => ackHistory[left] <= ackHistory[right]
  /\ Len(ackHistory) = 0 \/ wireFrame = NoFrame \/
       ackHistory[Len(ackHistory)] <= wireFrame.ack

NoPrematureCumulativeDelete ==
  /\ (Late \in clientDeleted => Late \in clientRejectedInbox)
  /\ (Follower \in clientDeleted => Follower \in effects)

RejectionDoesNotDeleteFollower ==
  [][ /\ Late \notin clientRejectedInbox
      /\ Late \in clientRejectedInbox'
      => /\ Follower \in clientOutbox'
         /\ Follower \in clientInflight']_vars

DeletionIsCumulativeAndDurable ==
  [][\A event \in clientDeleted' \ clientDeleted :
       /\ (event = Late => Late \in clientRejectedInbox')
       /\ (event = Follower => Follower \in effects')]_vars

SequenceEventuallyDrains ==
  <> (/\ clientOutbox = {}
      /\ clientInflight = {}
      /\ clientRejectedInbox = {Late}
      /\ effects = {Follower}
      /\ ackHistory \in {<<0, 1, 1, 2>>, <<0, 1, 2, 2>>})

=============================================================================
