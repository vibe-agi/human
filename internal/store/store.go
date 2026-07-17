// Package store defines persistence contracts for completion requests.
// Domain state-transition rules stay in the completion package.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

var (
	ErrNotFound             = errors.New("store record not found")
	ErrIdempotencyConflict  = errors.New("idempotency key reused with a different request")
	ErrTaskConflict         = errors.New("task identity conflicts with the stored task")
	ErrStateConflict        = errors.New("task state changed concurrently")
	ErrLeaseConflict        = errors.New("task is leased to a different worker")
	ErrToolCallConflict     = errors.New("tool call id reused with different input")
	ErrToolCallPending      = errors.New("tool call execution is still pending")
	ErrToolResultsMissing   = errors.New("one or more dispatched tool calls have no result")
	ErrTaskNotReady         = errors.New("task is not ready for another completion request")
	ErrUnrecoverableRequest = errors.New("completion request cannot be recovered")
	ErrReplayPayloadExpired = errors.New("idempotent replay payload has expired")
	ErrWorkerEventConflict  = errors.New("worker event id reused with a different payload")
)

type TaskKey struct {
	CallerID string
	TaskID   string
}

type Task struct {
	TaskKey
	WorkspaceKey     string
	CapabilityTier   completion.CapabilityTier
	Dialect          canonical.Dialect
	HarnessID        string
	HarnessVersion   string
	HarnessSessionID string
	Root             string
	ExecAllowed      bool
	State            completion.State
	LeaseOwner       string
	// RetryRequestDigest is set only after an admitted/reconciled request
	// fails before crossing its HTTP response boundary. It permits one new
	// idempotency key to retry the exact same canonical request while keeping
	// unrelated continuations out of a task whose previous turn never ran.
	RetryRequestDigest string
	Revision           int64
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type RequestKey struct {
	CallerID       string
	IdempotencyKey string
}

type Request struct {
	RequestKey
	TaskID           string
	RequestDigest    string
	CanonicalRequest canonical.Request
	Response         ResponseDecision
	ResponseComplete bool
	// RecoveryQuarantined marks a record-local recovery failure whose canonical
	// payload is no longer trusted. The request digest and immutable HTTP
	// decision remain authoritative for idempotency and exact finite replay;
	// callers must not inspect CanonicalRequest when this flag is set.
	RecoveryQuarantined bool
	CreatedAt           time.Time
	CompletedAt         *time.Time
	// PayloadPrunedAt marks an immutable idempotency tombstone. The request
	// digest and terminal response metadata remain durable, but exact replay
	// bytes are no longer available after the configured grace period.
	PayloadPrunedAt *time.Time
}

// ResponseDecision is the durable HTTP boundary for a completion request.
// StatusCode == 0 means the gateway has not yet chosen an HTTP response. A
// streaming request chooses 200 before worker dispatch; a non-streaming request
// chooses status and complete body together after its terminal worker event.
// Once set, the decision is immutable so every idempotent replay observes the
// same status, headers, and bytes.
type ResponseDecision struct {
	StatusCode  int
	ContentType string
	RetryAfter  string
	Body        []byte
}

type BeginRequestInput struct {
	Task
	IdempotencyKey   string
	RequestDigest    string
	CanonicalRequest canonical.Request
	ToolResults      []ToolResult
}

type BeginRequestResult struct {
	Task    Task
	Request Request
	Replay  bool
}

// TaskAffinity names one exact harness conversation without conflating that
// conversation with a Human task. An exact serial harness may use it to resume
// the unique non-terminal task across clarification and native tool results.
type TaskAffinity struct {
	CallerID         string
	WorkspaceKey     string
	HarnessID        string
	HarnessVersion   string
	HarnessSessionID string
}

// RecoveryIssue identifies one durable request that cannot be reconstructed.
// Recovery is deliberately fail-closed for that request while allowing other,
// independent requests in the same store to make progress.
type RecoveryIssue struct {
	RequestKey
	Dialect        canonical.Dialect
	ResponseStatus int
	// StreamMetadata is the first durable response-mode checkpoint, when one
	// exists. It is opaque to the Store and lets the gateway produce a
	// dialect-correct terminal frame without decoding the corrupt canonical
	// request.
	StreamMetadata []byte
	Err            error
}

// RecoveryQuarantine is a raw, record-local fail-closed decision. Failure is
// used when no HTTP response was committed. StreamTerminal is appended only
// when a 200/SSE boundary was already committed and the response is still
// incomplete. Implementations must not decode the canonical request while
// applying this decision.
type RecoveryQuarantine struct {
	RequestKey
	Failure        ResponseDecision
	StreamTerminal []byte
}

// RecoverySnapshot separates record-local corruption from infrastructure
// failures. Issues are quarantined by the caller; a non-nil method error means
// the store could not produce a trustworthy snapshot at all.
type RecoverySnapshot struct {
	Requests []BeginRequestResult
	Issues   []RecoveryIssue
}

type ResponseEvent struct {
	RequestKey
	Sequence    int64
	Kind        string
	EventID     string
	EventDigest string
	Data        []byte
	CreatedAt   time.Time
}

// ResponseRead is a lightweight, internally consistent view of one HTTP
// response and the events after a caller-owned cursor. Implementations must
// read ResponseComplete and Events from the same database snapshot: observing
// complete while omitting the terminal event would truncate an exact replay.
// It intentionally excludes the canonical request payload from the hot path.
type ResponseRead struct {
	RequestKey
	Response         ResponseDecision
	ResponseComplete bool
	// PayloadPruned is read in the same snapshot as Events. A streaming replay
	// must check it before making its 200 response observable, otherwise a
	// retention pass racing an earlier LookupRequest could produce a truncated
	// success instead of an explicit expired-replay response.
	PayloadPruned bool
	Events        []ResponseEvent
}

type WorkerEventReceipt struct {
	RequestKey
	EventID   string
	WorkerID  string
	Digest    string
	CreatedAt time.Time
}

// WorkerEventReceiptStore is the durable ACK oracle for the private worker
// protocol. A receipt exists only after the event's state and response wire
// effects are durable, allowing an outbox replay to be acknowledged even if
// gateway restarted after committing the event but before sending its ACK.
type WorkerEventReceiptStore interface {
	RecordWorkerEventReceipt(context.Context, RequestKey, string, string, string) (WorkerEventReceipt, error)
	LookupWorkerEventReceipt(context.Context, RequestKey, string) (WorkerEventReceipt, error)
}

type ToolExecutionKey struct {
	CallerID   string
	TaskID     string
	ToolCallID string
}

type ToolExecution struct {
	ToolExecutionKey
	RequestDigest string
	Status        string
	Result        []byte
	IsError       bool
	CreatedAt     time.Time
	CompletedAt   *time.Time
}

// ToolResult is the canonical, caller-reported outcome of one dispatched
// tool call. Result contains canonical JSON so equality remains stable across
// HTTP dialects and retries.
type ToolResult struct {
	ToolCallID string
	Result     []byte
	IsError    bool
}

type BeginToolExecutionResult struct {
	Execution ToolExecution
	Replay    bool
}

type APIToken struct {
	KeyID         string
	PrincipalType string
	SubjectID     string
	TokenHash     []byte
	CreatedAt     time.Time
	RevokedAt     *time.Time
}

// AuditMetadata contains the operational facts retained by default. It is
// deliberately unable to carry request, response, tool-result, or source
// content; those bytes belong in AuditPayload and are opt-in at the caller.
type AuditMetadata struct {
	ID           string
	CallerID     string
	WorkspaceKey string
	TaskID       string
	Dialect      canonical.Dialect
	KeyID        string
	PendingMS    int64
	GenMS        int64
	Error        string
	CreatedAt    time.Time
}

// AuditPayload is the independently retained, short-lived content associated
// with an audit record. Kind lets a record hold separate request, tool-result,
// and response payloads without mixing any of them into default metadata.
type AuditPayload struct {
	AuditID   string
	Kind      string
	Data      []byte
	CreatedAt time.Time
	ExpiresAt time.Time
}

// AuditStore separates default-on metadata from opt-in payload retention.
// Implementations must not infer or synthesize payloads while recording
// metadata.
type AuditStore interface {
	CreateAuditMetadata(context.Context, AuditMetadata) (AuditMetadata, error)
	CompleteAuditMetadata(context.Context, string, int64, int64, string) (AuditMetadata, error)
	GetAuditMetadata(context.Context, string) (AuditMetadata, error)
	StoreAuditPayload(context.Context, AuditPayload) (AuditPayload, error)
	GetAuditPayload(context.Context, string, string) (AuditPayload, error)
	PurgeExpiredAuditPayloads(context.Context, time.Time) (int64, error)
}

type TokenStore interface {
	CreateAPIToken(context.Context, APIToken) error
	FindAPITokenByHash(context.Context, []byte) (APIToken, error)
	RevokeAPIToken(context.Context, string) error
}

// CompletionStore is the durable correctness boundary for completion mode.
type CompletionStore interface {
	WorkerEventReceiptStore
	LookupRequest(context.Context, RequestKey, string) (BeginRequestResult, error)
	BeginRequest(context.Context, BeginRequestInput) (BeginRequestResult, error)
	BeginResponse(context.Context, RequestKey) (Request, error)
	CompleteNonStreamingResponse(context.Context, RequestKey, ResponseDecision) (Request, error)
	FailRequest(context.Context, RequestKey, completion.State, ResponseDecision) (Request, error)
	ListRecoverableRequests(context.Context) (RecoverySnapshot, error)
	QuarantineRecoveryRequest(context.Context, RecoveryQuarantine) error
	GetTask(context.Context, TaskKey) (Task, error)
	FindOpenHarnessTask(context.Context, TaskAffinity) (Task, error)
	TransitionTask(context.Context, TaskKey, completion.State, completion.State, string) (Task, error)
	AppendResponseEvent(context.Context, RequestKey, string, []byte) (ResponseEvent, error)
	AppendWorkerResponseEvent(context.Context, RequestKey, string, string, string, []byte) (ResponseEvent, error)
	ReadResponse(context.Context, RequestKey, int64) (ResponseRead, error)
	ListResponseEvents(context.Context, RequestKey, int64) ([]ResponseEvent, error)
	ListWorkerEventStages(context.Context, RequestKey, string) ([]ResponseEvent, error)
	CompleteRequest(context.Context, RequestKey) error
	PurgeExpiredCompletionPayloads(context.Context, time.Time) (int64, error)
	LookupToolExecution(context.Context, ToolExecutionKey) (ToolExecution, error)
	BeginToolExecution(context.Context, ToolExecutionKey, string) (BeginToolExecutionResult, error)
	CompleteToolExecution(context.Context, ToolExecutionKey, []byte, bool) (ToolExecution, error)
	Close() error
}
