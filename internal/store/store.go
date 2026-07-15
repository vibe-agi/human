// Package store defines persistence contracts shared by the completion and
// delegation modes. Domain state-transition rules stay in their mode package.
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
	ErrWorkerEventConflict  = errors.New("worker event id reused with a different payload")
)

type TaskKey struct {
	CallerID string
	TaskID   string
}

type Task struct {
	TaskKey
	WorkspaceKey   string
	CapabilityTier completion.CapabilityTier
	Dialect        canonical.Dialect
	HarnessID      string
	HarnessVersion string
	Root           string
	ExecAllowed    bool
	State          completion.State
	LeaseOwner     string
	Revision       int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
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
	CreatedAt        time.Time
	CompletedAt      *time.Time
}

// ResponseDecision is the durable HTTP boundary for a completion request.
// StatusCode == 0 means the gateway has not yet chosen between a pre-stream
// HTTP failure and a 200 streaming response. Once set, the decision is
// immutable so every idempotent replay observes the same protocol boundary.
// Body is populated only for terminal pre-stream responses.
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

type ResponseEvent struct {
	RequestKey
	Sequence    int64
	Kind        string
	EventID     string
	EventDigest string
	Data        []byte
	CreatedAt   time.Time
}

type WorkerEventReceipt struct {
	RequestKey
	EventID   string
	Digest    string
	CreatedAt time.Time
}

// WorkerEventReceiptStore is the durable ACK oracle for the private worker
// protocol. A receipt exists only after the event's state and response wire
// effects are durable, allowing an outbox replay to be acknowledged even if
// humand restarted after committing the event but before sending its ACK.
type WorkerEventReceiptStore interface {
	RecordWorkerEventReceipt(context.Context, RequestKey, string, string) (WorkerEventReceipt, error)
	LookupWorkerEventReceipt(context.Context, RequestKey, string, string) (WorkerEventReceipt, error)
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
	FailRequest(context.Context, RequestKey, completion.State, ResponseDecision) (Request, error)
	ListRecoverableRequests(context.Context) ([]BeginRequestResult, error)
	GetTask(context.Context, TaskKey) (Task, error)
	TransitionTask(context.Context, TaskKey, completion.State, completion.State, string) (Task, error)
	AppendResponseEvent(context.Context, RequestKey, string, []byte) (ResponseEvent, error)
	AppendWorkerResponseEvent(context.Context, RequestKey, string, string, string, []byte) (ResponseEvent, error)
	ListResponseEvents(context.Context, RequestKey, int64) ([]ResponseEvent, error)
	CompleteRequest(context.Context, RequestKey) error
	BeginToolExecution(context.Context, ToolExecutionKey, string) (BeginToolExecutionResult, error)
	CompleteToolExecution(context.Context, ToolExecutionKey, []byte, bool) (ToolExecution, error)
	Close() error
}
