// Package delegation implements the durable, single-writer authority for the
// asynchronous human-delegation protocol. Artifact bytes and metadata are
// deliberately opaque here; git and workspace operations belong to the TUI or
// caller-side adapter.
package delegation

import "time"

const (
	// MetadataKey contains authority-owned protocol projection metadata.
	MetadataKey = "https://github.com/vibe-agi/human/delegation"
	// RequestMetadataKey contains caller-owned delegation request parameters.
	RequestMetadataKey = MetadataKey + "/request"
	// GitPatchMediaType is the single wire media type for cumulative
	// delegation patch artifacts.
	GitPatchMediaType = "application/vnd.git.patch"
)

// State is the authoritative lifecycle state of one delegated task.
type State string

const (
	StateSubmitted     State = "submitted"
	StateWorking       State = "working"
	StateInputRequired State = "input-required"
	StateRewindPending State = "rewind-pending"
	StateCompleted     State = "completed"
	StateCanceled      State = "canceled"
	StateRejected      State = "rejected"
	StateFailed        State = "failed"
)

// Valid reports whether state is part of the delegation protocol.
func (state State) Valid() bool {
	switch state {
	case StateSubmitted, StateWorking, StateInputRequired, StateRewindPending,
		StateCompleted, StateCanceled, StateRejected, StateFailed:
		return true
	default:
		return false
	}
}

// IsTerminal reports whether no further task commands may be accepted.
func (state State) IsTerminal() bool {
	switch state {
	case StateCompleted, StateCanceled, StateRejected, StateFailed:
		return true
	default:
		return false
	}
}

// EventKind identifies an append-only authority event.
type EventKind string

const (
	EventTaskSubmitted   EventKind = "task.submitted"
	EventTaskAccepted    EventKind = "task.accepted"
	EventTaskRejected    EventKind = "task.rejected"
	EventTurnDelivered   EventKind = "turn.delivered"
	EventCallerReplied   EventKind = "caller.replied"
	EventMessageAppended EventKind = "message.appended"
	EventTaskCompleted   EventKind = "task.completed"
	EventTaskCanceled    EventKind = "task.canceled"
	EventTaskFailed      EventKind = "task.failed"
	EventRewindRequested EventKind = "rewind.requested"
	EventRewindConfirmed EventKind = "rewind.confirmed"
	EventRewindRejected  EventKind = "rewind.rejected"
	EventExecRequested   EventKind = "exec.requested"
	EventExecCompleted   EventKind = "exec.completed"
	EventExecDenied      EventKind = "exec.denied"
	EventExecFailed      EventKind = "exec.failed"
)

// Task is the current materialized authority state. Revision is also the
// sequence number of the most recent event, so optimistic writes and audit
// replay share one monotonic clock.
type Task struct {
	ID              string    `json:"id"`
	CallerID        string    `json:"caller_id"`
	ContextID       string    `json:"context_id"`
	State           State     `json:"state"`
	WorkerID        string    `json:"worker_id,omitempty"`
	LatestTurn      int64     `json:"latest_turn"`
	NextTurn        int64     `json:"next_turn"`
	PendingRewindTo *int64    `json:"pending_rewind_to,omitempty"`
	Revision        int64     `json:"revision"`
	Metadata        []byte    `json:"metadata,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// Turn is an immutable delivery record. Rewind never deletes or renumbers a
// turn; it may only set SupersededAtRevision once.
type Turn struct {
	TaskID               string    `json:"task_id"`
	Number               int64     `json:"number"`
	SupersededAtRevision *int64    `json:"superseded_at_revision,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
}

// Superseded reports whether this turn no longer belongs to the live chain.
func (turn Turn) Superseded() bool { return turn.SupersededAtRevision != nil }

// Artifact is the opaque cumulative delivery attached to exactly one turn.
// SHA256 is computed by the authority over Data; it is not caller supplied.
type Artifact struct {
	ID                   string    `json:"id"`
	TaskID               string    `json:"task_id"`
	TurnNumber           int64     `json:"turn_number"`
	MediaType            string    `json:"media_type"`
	Data                 []byte    `json:"data"`
	Metadata             []byte    `json:"metadata,omitempty"`
	SHA256               string    `json:"sha256"`
	SupersededAtRevision *int64    `json:"superseded_at_revision,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
}

// Superseded reports whether this artifact no longer belongs to the live chain.
func (artifact Artifact) Superseded() bool { return artifact.SupersededAtRevision != nil }

// Event is an immutable audit item. Sequence is strictly increasing per task.
type Event struct {
	TaskID     string    `json:"task_id"`
	Sequence   int64     `json:"sequence"`
	Kind       EventKind `json:"kind"`
	FromState  State     `json:"from_state,omitempty"`
	ToState    State     `json:"to_state"`
	TurnNumber *int64    `json:"turn_number,omitempty"`
	Data       []byte    `json:"data,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// Message is an immutable, idempotently keyed conversation item. Data is an
// opaque protocol payload; Sequence is the task revision at which it arrived.
type Message struct {
	TaskID    string    `json:"task_id"`
	CallerID  string    `json:"caller_id"`
	ID        string    `json:"id"`
	Sequence  int64     `json:"sequence"`
	Role      string    `json:"role"`
	Data      []byte    `json:"data"`
	SHA256    string    `json:"sha256"`
	CreatedAt time.Time `json:"created_at"`
}

// ExecStatus is the authority-owned lifecycle of one demand-side command.
// It deliberately does not alter the surrounding A2A task state.
type ExecStatus string

const (
	ExecPending   ExecStatus = "pending"
	ExecCompleted ExecStatus = "completed"
	ExecDenied    ExecStatus = "denied"
	ExecFailed    ExecStatus = "failed"
)

func (status ExecStatus) Valid() bool {
	switch status {
	case ExecPending, ExecCompleted, ExecDenied, ExecFailed:
		return true
	default:
		return false
	}
}

// ExecRequest is a durable worker-to-caller command request and its optional
// terminal result. Command execution never occurs in humand.
type ExecRequest struct {
	TaskID             string     `json:"task_id"`
	ID                 string     `json:"request_id"`
	WorkerID           string     `json:"worker_id"`
	Command            string     `json:"command"`
	CWD                string     `json:"cwd,omitempty"`
	TimeoutMS          int64      `json:"timeout_ms,omitempty"`
	Reason             string     `json:"reason"`
	Status             ExecStatus `json:"status"`
	ExitCode           *int       `json:"exit_code,omitempty"`
	Stdout             []byte     `json:"stdout,omitempty"`
	Stderr             []byte     `json:"stderr,omitempty"`
	Error              string     `json:"error,omitempty"`
	Truncated          bool       `json:"truncated,omitempty"`
	TimedOut           bool       `json:"timed_out,omitempty"`
	RequestSequence    int64      `json:"request_sequence"`
	ResolutionSequence *int64     `json:"resolution_sequence,omitempty"`
	ResolutionID       string     `json:"resolution_id,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	ResolvedAt         *time.Time `json:"resolved_at,omitempty"`
}

type CreateTaskInput struct {
	ID        string
	CallerID  string
	ContextID string
	Metadata  []byte
}

type MessageInput struct {
	ID   string
	Role string
	Data []byte
}

// CommandInput supplies the optimistic revision expected by a caller. Data is
// opaque audit context such as a reason or reply payload.
type CommandInput struct {
	TaskID           string
	ExpectedRevision int64
	Data             []byte
}

type AcceptTaskInput struct {
	CommandInput
	WorkerID string
}

type DeliverTurnInput struct {
	CommandInput
	ArtifactID        string
	ArtifactMediaType string
	ArtifactData      []byte
	ArtifactMetadata  []byte
}

type RequestRewindInput struct {
	CommandInput
	TargetTurn int64
}

type RequestExecInput struct {
	CommandInput
	WorkerID  string
	RequestID string
	Command   string
	CWD       string
	TimeoutMS int64
	Reason    string
}

type ResolveExecInput struct {
	CommandInput
	RequestID    string
	ResolutionID string
	Approved     bool
	ExitCode     int
	Stdout       []byte
	Stderr       []byte
	Error        string
	Truncated    bool
	TimedOut     bool
}

type TransitionResult struct {
	Task  Task  `json:"task"`
	Event Event `json:"event"`
}

type DeliveryResult struct {
	Task     Task     `json:"task"`
	Turn     Turn     `json:"turn"`
	Artifact Artifact `json:"artifact"`
	Event    Event    `json:"event"`
}

type MessageResult struct {
	Task    Task
	Message Message
	Event   Event
	Replay  bool
}

type ExecResult struct {
	Task    Task        `json:"task"`
	Request ExecRequest `json:"request"`
	Event   Event       `json:"event"`
	Replay  bool        `json:"replay,omitempty"`
}

// Snapshot is a transactionally consistent recovery view of one task.
type Snapshot struct {
	Task      Task          `json:"task"`
	Turns     []Turn        `json:"turns"`
	Artifacts []Artifact    `json:"artifacts"`
	Messages  []Message     `json:"messages"`
	Exec      []ExecRequest `json:"exec_requests"`
	Events    []Event       `json:"events"`
}
