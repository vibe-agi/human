// Package agent implements the durable, task-oriented HumanAgent surface.
//
// Unlike the response-oriented completion gateway, an Agent Task survives
// individual transports and may alternate between working and input_required
// multiple times. Context is display/conversation grouping only; Workspace is
// the independent correctness scope used by Artifact publication and receipt
// CAS operations.
package agent

import (
	"errors"
	"fmt"
	"time"

	"github.com/vibe-agi/human/workspace"
)

var (
	ErrInvalidArgument     = errors.New("invalid agent argument")
	ErrNotFound            = errors.New("agent task not found")
	ErrIdempotencyConflict = errors.New("agent command id reused with different input")
	ErrRevisionConflict    = errors.New("agent task revision conflict")
	ErrInvalidTransition   = errors.New("invalid agent task transition")
	ErrTerminalTask        = errors.New("agent task is terminal")
	ErrTaskConflict        = errors.New("agent task already exists")
	ErrMessageConflict     = errors.New("agent message id is already in use")
	ErrSubmissionConflict  = errors.New("agent submission id is already in use")
	ErrArtifactNotFound    = errors.New("agent artifact not found")
	ErrArtifactConflict    = errors.New("agent artifact id or task is already bound")
	ErrArtifactState       = errors.New("invalid agent artifact state")
	ErrWorkspaceConflict   = errors.New("agent workspace confirmed revision conflict")
	ErrWorkspaceNotFound   = errors.New("agent workspace head not found")
	ErrReceiptNotFound     = errors.New("agent apply receipt not found")
	ErrReceiptConflict     = errors.New("agent apply receipt is already decided differently")
	ErrLeaseNotFound       = errors.New("agent task lease not found")
	ErrLeaseUnavailable    = errors.New("agent task is already leased")
	ErrStaleLease          = errors.New("agent task lease is stale")
	ErrLeaseFenceExhausted = errors.New("agent task lease fence exhausted")
	ErrCorruptStore        = errors.New("corrupt agent store")
	ErrDatabaseInUse       = errors.New("agent database is already held by another running instance")
	ErrClosed              = errors.New("agent is closed")
)

type AuthorityID string
type ContextID string
type WorkspaceID string
type TaskID string
type MessageID string
type SubmissionID string
type CommandID string
type ArtifactID string
type ApplyReceiptID string
type WorkerID string
type LeaseFence uint64

// ContextRef is an authority-qualified display/conversation scope. Two Tasks
// in one Context are allowed to be active concurrently.
type ContextRef struct {
	Authority AuthorityID `json:"authority"`
	ID        ContextID   `json:"id"`
}

// WorkspaceRef is an authority-qualified correctness scope. Context and
// Workspace are deliberately orthogonal: different Contexts may share one
// Workspace and therefore its future Artifact CAS baseline.
type WorkspaceRef struct {
	Authority AuthorityID `json:"authority"`
	ID        WorkspaceID `json:"id"`
}

// TaskRef is the durable Agent task key. The "agent" surface discriminator is
// implicit in this package and in the SQLite table namespace; it never aliases
// a completion task key.
type TaskRef struct {
	Workspace WorkspaceRef `json:"workspace"`
	ID        TaskID       `json:"id"`
}

type ArtifactRef struct {
	Workspace WorkspaceRef `json:"workspace"`
	ID        ArtifactID   `json:"id"`
}

type TaskState string

const (
	TaskSubmitted     TaskState = "submitted"
	TaskWorking       TaskState = "working"
	TaskInputRequired TaskState = "input_required"
	TaskCompleted     TaskState = "completed"
	TaskCanceled      TaskState = "canceled"
	TaskRejected      TaskState = "rejected"
	TaskFailed        TaskState = "failed"
)

func (state TaskState) Terminal() bool {
	switch state {
	case TaskCompleted, TaskCanceled, TaskRejected, TaskFailed:
		return true
	default:
		return false
	}
}

type Author string

const (
	AuthorCaller Author = "caller"
	AuthorAgent  Author = "agent"
)

// Part is an ordered, transport-neutral content part. A2A, JSON, or another
// wire adapter must translate at the boundary instead of leaking its DTOs into
// this domain package.
type Part struct {
	MediaType string `json:"media_type"`
	Data      []byte `json:"data"`
}

type MessageInput struct {
	ID    MessageID `json:"id"`
	Parts []Part    `json:"parts"`
}

type Message struct {
	ID        MessageID `json:"id"`
	Task      TaskRef   `json:"task"`
	Sequence  uint64    `json:"sequence"`
	Author    Author    `json:"author"`
	Parts     []Part    `json:"parts"`
	CreatedAt time.Time `json:"created_at"`
}

// Submission is published atomically with its final Agent message and the
// Task's transition to completed. Artifact is added by the workspace delivery
// phase; a nil/absent Artifact is a first-class content-only result.
type Submission struct {
	ID           SubmissionID `json:"id"`
	Task         TaskRef      `json:"task"`
	FinalMessage MessageID    `json:"final_message"`
	Artifact     *ArtifactRef `json:"artifact,omitempty"`
	PublishedAt  time.Time    `json:"published_at"`
}

type Task struct {
	Ref          TaskRef      `json:"ref"`
	Context      ContextRef   `json:"context"`
	State        TaskState    `json:"state"`
	Revision     uint64       `json:"revision"`
	MessageCount uint64       `json:"message_count"`
	EventCount   uint64       `json:"event_count"`
	Artifact     *ArtifactRef `json:"artifact,omitempty"`
	Submission   *Submission  `json:"submission,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

// MaxPageSize is the hard item limit for every Agent list operation. A zero
// request limit selects this value; larger or negative limits are rejected.
const MaxPageSize = 100

// PageRequest pages append-only Message and Event streams by sequence. Next is
// always the last returned sequence, or After when the page is empty, so a
// caller can use it as the next tail-poll cursor even when HasMore is false.
type PageRequest struct {
	After uint64 `json:"after,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type MessagePage struct {
	Items   []Message `json:"items"`
	Next    uint64    `json:"next,omitempty"`
	HasMore bool      `json:"has_more"`
}

type EventPage struct {
	Items   []Event `json:"items"`
	Next    uint64  `json:"next,omitempty"`
	HasMore bool    `json:"has_more"`
}

// TaskPageCursor is a stable keyset cursor over a live view, not a snapshot.
// Context is supplied separately; the cursor cannot move a query across an
// authenticated authority boundary.
type TaskPageCursor struct {
	CreatedAt time.Time   `json:"created_at"`
	Workspace WorkspaceID `json:"workspace"`
	Task      TaskID      `json:"task"`
}

type TaskPageRequest struct {
	After *TaskPageCursor `json:"after,omitempty"`
	Limit int             `json:"limit,omitempty"`
}

type TaskPage struct {
	Items   []Task          `json:"items"`
	Next    *TaskPageCursor `json:"next,omitempty"`
	HasMore bool            `json:"has_more"`
}

// TaskQueryCursor is a stable keyset cursor over an authority-wide live view.
// Tasks are ordered by most recently updated first, then by their stable key.
// Task IDs are unique inside one authenticated Authority; Workspace remains in
// the cursor so a corrupt store cannot silently alias two correctness scopes.
type TaskQueryCursor struct {
	UpdatedAt time.Time   `json:"updated_at"`
	Workspace WorkspaceID `json:"workspace"`
	Task      TaskID      `json:"task"`
}

// TaskQuery filters an authenticated Authority's Tasks. Zero Context and State
// values mean "any". UpdatedAtOrAfter is inclusive. TotalSize in the result is
// evaluated in the same SQLite read transaction as the returned page.
type TaskQuery struct {
	Context          ContextID        `json:"context,omitempty"`
	State            TaskState        `json:"state,omitempty"`
	UpdatedAtOrAfter *time.Time       `json:"updated_at_or_after,omitempty"`
	After            *TaskQueryCursor `json:"after,omitempty"`
	Limit            int              `json:"limit,omitempty"`
}

type TaskQueryPage struct {
	Items     []Task           `json:"items"`
	Next      *TaskQueryCursor `json:"next,omitempty"`
	HasMore   bool             `json:"has_more"`
	TotalSize uint64           `json:"total_size"`
}

// TaskSnapshot couples a Task with the exact append-only Event cursor observed
// in the same database snapshot. Reading events after EventCursor cannot miss a
// concurrent commit; it either appears in the subsequent page or a later poll.
type TaskSnapshot struct {
	Task        Task   `json:"task"`
	EventCursor uint64 `json:"event_cursor"`
}

// CommandMeta supplies durable idempotency and optimistic concurrency. An
// exact retry of a committed command returns its original result snapshot even
// if the Task has advanced since then. Once committed, reusing ID with
// different input fails. A command rejected before commit does not reserve ID.
type CommandMeta struct {
	ID               CommandID `json:"id"`
	ExpectedRevision uint64    `json:"expected_revision"`
}

// LeaseGrant is the durable, fenced authority for one worker to mutate a Task.
// It deliberately has no wall-clock expiry: a dispatcher fences a disconnected
// worker explicitly, then acquires the next monotonically larger generation.
// Remote adapters must construct Worker from the authenticated principal, never
// from an untrusted request body.
type LeaseGrant struct {
	Task   TaskRef    `json:"task"`
	Worker WorkerID   `json:"worker"`
	Fence  LeaseFence `json:"fence"`
}

// LeaseAssignment is returned only after both the current grant and its
// immutable history row have committed. Dispatchers may therefore publish this
// value directly and can recover the same assignment after a process restart.
type LeaseAssignment struct {
	Grant     LeaseGrant `json:"grant"`
	Task      Task       `json:"task"`
	GrantedAt time.Time  `json:"granted_at"`
}

type LeasePageCursor struct {
	GrantedAt time.Time   `json:"granted_at"`
	Workspace WorkspaceID `json:"workspace"`
	Task      TaskID      `json:"task"`
	Fence     LeaseFence  `json:"fence"`
}

type LeasePageRequest struct {
	After *LeasePageCursor `json:"after,omitempty"`
	Limit int              `json:"limit,omitempty"`
}

type LeasePage struct {
	Items   []LeaseAssignment `json:"items"`
	Next    *LeasePageCursor  `json:"next,omitempty"`
	HasMore bool              `json:"has_more"`
}

type AcquireLeaseCommand struct {
	ID     CommandID `json:"id"`
	Task   TaskRef   `json:"task"`
	Worker WorkerID  `json:"worker"`
}

// ClaimLeaseCommand asks the durable dispatcher to select the oldest
// claimable Task within one authenticated authority. Authority and Worker must
// come from the authenticated transport principal, never an untrusted payload.
type ClaimLeaseCommand struct {
	ID        CommandID   `json:"id"`
	Authority AuthorityID `json:"authority"`
	Worker    WorkerID    `json:"worker"`
}

type FenceLeaseCommand struct {
	ID    CommandID  `json:"id"`
	Grant LeaseGrant `json:"grant"`
}

// WorkerCommandMeta couples ordinary command idempotency/revision CAS with the
// grant that must still be current at the exact SQLite commit boundary.
type WorkerCommandMeta struct {
	ID               CommandID  `json:"id"`
	ExpectedRevision uint64     `json:"expected_revision"`
	Grant            LeaseGrant `json:"grant"`
}

type CreateTaskCommand struct {
	Meta    CommandMeta  `json:"meta"`
	Task    TaskRef      `json:"task"`
	Context ContextRef   `json:"context"`
	Message MessageInput `json:"message"`
}

type TaskCommand struct {
	Meta CommandMeta `json:"meta"`
	Task TaskRef     `json:"task"`
}

type WorkerTaskCommand struct {
	Meta WorkerCommandMeta `json:"meta"`
	Task TaskRef           `json:"task"`
}

type MessageCommand struct {
	Meta    CommandMeta  `json:"meta"`
	Task    TaskRef      `json:"task"`
	Message MessageInput `json:"message"`
}

type WorkerMessageCommand struct {
	Meta    WorkerCommandMeta `json:"meta"`
	Task    TaskRef           `json:"task"`
	Message MessageInput      `json:"message"`
}

type CompleteTaskCommand struct {
	Meta       WorkerCommandMeta `json:"meta"`
	Task       TaskRef           `json:"task"`
	Submission SubmissionID      `json:"submission"`
	Message    MessageInput      `json:"message"`
	Artifact   *ArtifactRef      `json:"artifact,omitempty"`
}

type ArtifactState string

const (
	ArtifactFrozen    ArtifactState = "frozen"
	ArtifactPublished ArtifactState = "published"
	ArtifactDiscarded ArtifactState = "discarded"
)

// Artifact is immutable workspace content plus its publication state. Payload
// bytes are returned separately so ordinary Task listing remains bounded.
type Artifact struct {
	Ref            ArtifactRef        `json:"ref"`
	Task           TaskRef            `json:"task"`
	State          ArtifactState      `json:"state"`
	BaseRevision   workspace.Revision `json:"base_revision"`
	ResultRevision workspace.Revision `json:"result_revision"`
	Digest         workspace.Digest   `json:"digest"`
	PayloadDigest  workspace.Digest   `json:"payload_digest"`
	PayloadSize    int64              `json:"payload_size"`
	MediaType      string             `json:"media_type"`
	FrozenAt       time.Time          `json:"frozen_at"`
	PublishedAt    *time.Time         `json:"published_at,omitempty"`
	DiscardedAt    *time.Time         `json:"discarded_at,omitempty"`
}

type ArtifactContent struct {
	Artifact Artifact          `json:"artifact"`
	Payload  workspace.Payload `json:"payload"`
}

type FreezeArtifactCommand struct {
	Meta                 WorkerCommandMeta  `json:"meta"`
	Task                 TaskRef            `json:"task"`
	Artifact             ArtifactID         `json:"artifact"`
	ExpectedBaseRevision workspace.Revision `json:"expected_base_revision"`
	Payload              workspace.Payload  `json:"payload"`
}

type FreezeArtifactResult struct {
	Task     Task     `json:"task"`
	Artifact Artifact `json:"artifact"`
}

type RecordApplyReceiptCommand struct {
	CommandID        CommandID               `json:"command_id"`
	Receipt          ApplyReceiptID          `json:"receipt"`
	Artifact         ArtifactRef             `json:"artifact"`
	ArtifactDigest   workspace.Digest        `json:"artifact_digest"`
	BaseRevision     workspace.Revision      `json:"base_revision"`
	ResultRevision   workspace.Revision      `json:"result_revision"`
	Decision         workspace.ApplyDecision `json:"decision"`
	ObservedRevision workspace.Revision      `json:"observed_revision,omitempty"`
	Code             string                  `json:"code,omitempty"`
	Message          string                  `json:"message,omitempty"`
}

type ApplyReceipt struct {
	ID               ApplyReceiptID          `json:"id"`
	Artifact         ArtifactRef             `json:"artifact"`
	ArtifactDigest   workspace.Digest        `json:"artifact_digest"`
	BaseRevision     workspace.Revision      `json:"base_revision"`
	ResultRevision   workspace.Revision      `json:"result_revision"`
	Decision         workspace.ApplyDecision `json:"decision"`
	ObservedRevision workspace.Revision      `json:"observed_revision,omitempty"`
	Code             string                  `json:"code,omitempty"`
	Message          string                  `json:"message,omitempty"`
	RecordedAt       time.Time               `json:"recorded_at"`
}

type WorkspaceHead struct {
	Workspace         WorkspaceRef       `json:"workspace"`
	ConfirmedRevision workspace.Revision `json:"confirmed_revision"`
	UpdatedAt         time.Time          `json:"updated_at"`
}

type EventType string

const (
	EventTaskSubmitted  EventType = "task_submitted"
	EventTaskAccepted   EventType = "task_accepted"
	EventTaskRejected   EventType = "task_rejected"
	EventInputRequired  EventType = "input_required"
	EventCallerReplied  EventType = "caller_replied"
	EventTaskCanceled   EventType = "task_canceled"
	EventTaskFailed     EventType = "task_failed"
	EventTaskCompleted  EventType = "task_completed"
	EventArtifactFrozen EventType = "artifact_frozen"
)

// Event is an append-only status stream. Status/progress belongs here rather
// than in Message history, whose caller/agent turn alternation stays strict.
type Event struct {
	Task       TaskRef      `json:"task"`
	Sequence   uint64       `json:"sequence"`
	Type       EventType    `json:"type"`
	State      TaskState    `json:"state"`
	Revision   uint64       `json:"revision"`
	Message    MessageID    `json:"message,omitempty"`
	Submission SubmissionID `json:"submission,omitempty"`
	Artifact   ArtifactID   `json:"artifact,omitempty"`
	OccurredAt time.Time    `json:"occurred_at"`
}

// RevisionConflictError reports the exact failed CAS values.
type RevisionConflictError struct {
	Expected uint64
	Actual   uint64
}

func (failure *RevisionConflictError) Error() string {
	return fmt.Sprintf("%v: expected %d, current %d", ErrRevisionConflict, failure.Expected, failure.Actual)
}

func (failure *RevisionConflictError) Unwrap() error { return ErrRevisionConflict }

// TransitionError retains the operation and state while supporting errors.Is.
type TransitionError struct {
	Operation string
	State     TaskState
	Terminal  bool
}

func (failure *TransitionError) Error() string {
	if failure.Terminal {
		return fmt.Sprintf("%v: %s from %s", ErrTerminalTask, failure.Operation, failure.State)
	}
	return fmt.Sprintf("%v: %s from %s", ErrInvalidTransition, failure.Operation, failure.State)
}

func (failure *TransitionError) Unwrap() error {
	if failure.Terminal {
		return ErrTerminalTask
	}
	return ErrInvalidTransition
}
