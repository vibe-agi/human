package llm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/observe"
	"github.com/vibe-agi/human/protect"
)

var (
	ErrInvalidServiceConfig = errors.New("llm: invalid service configuration")
	ErrServiceClosed        = errors.New("llm: service is closed")
	ErrIdempotencyConflict  = errors.New("llm: idempotency key conflicts with another request")
	ErrReplayExpired        = errors.New("llm: exact replay payload has expired")
	ErrTaskConflict         = errors.New("llm: task state conflicts with the request")
	ErrWorkerUnavailable    = errors.New("llm: no worker is available")
	ErrWorkerRouteDenied    = errors.New("llm: worker routing was denied")
	// ErrWorkerRouterRequired is a deployment configuration error, not a
	// temporary capacity condition: more than one worker is connected and no
	// WorkerRouter was configured to make the selection explicit.
	ErrWorkerRouterRequired = errors.New("llm: multiple workers are connected and no WorkerRouter is configured")
	ErrResponseNotFound     = errors.New("llm: response is not found")
	ErrResponseDigest       = errors.New("llm: response digest does not match")
)

// Clock supplies all core-owned wall-clock values. Implementations must be
// safe for concurrent use and must not return the zero time.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

func (clock ClockFunc) Now() time.Time { return clock() }

// IDKind identifies one durable correctness namespace.
type IDKind string

const (
	IDTask     IDKind = "task"
	IDRequest  IDKind = "request"
	IDResponse IDKind = "response"
	IDLease    IDKind = "lease"
)

// IDSource creates stable, opaque identifiers. Implementations are borrowed,
// must be concurrency-safe until Service.Done closes, and must honor context
// cancellation. Calls happen outside Store callbacks, so an implementation may
// use an HSM or network allocator. An error aborts the operation before its new
// Task/Request identity is persisted.
type IDSource interface {
	NewID(context.Context, IDKind) (string, error)
}

// IDSourceFunc adapts a function to IDSource.
type IDSourceFunc func(context.Context, IDKind) (string, error)

func (source IDSourceFunc) NewID(ctx context.Context, kind IDKind) (string, error) {
	return source(ctx, kind)
}

// SeedSource supplies every deterministic codec seed before persistence.
// Implementations are borrowed, concurrency-safe until Service.Done, and honor
// context cancellation. It is never called from inside a Store callback. The
// resulting seed, not the source, is replayed after restart. Ownership of
// returned Entropy and Opaque buffers transfers to the core; an implementation
// must not mutate them after return (including from another goroutine).
type SeedSource interface {
	SessionSeed(context.Context, SeedContext) (SessionSeed, error)
	EventSeed(context.Context, EventSeedContext) (EventSeed, error)
}

type SeedContext struct {
	Identity CompletionIdentity
	Request  Request
}

type EventSeedContext struct {
	Identity CompletionIdentity
	Event    Event
}

// CodecRegistration binds a validated pure Codec to caller transport metadata.
// Content types are response metadata only; no HTTP listener is owned here.
type CodecRegistration struct {
	Codec                Codec
	StreamContentType    string
	AggregateContentType string
	SuccessStatus        int
}

// TaskContext carries correctness identity selected by an authenticated caller
// transport. A chat request must leave every field except CapabilityTier empty.
// For remote-tools/workspace requests, TaskID may name an existing Task or a
// new caller-selected Task; an empty TaskID asks the core to allocate one.
type TaskContext struct {
	TaskID           TaskID         `json:"task_id,omitempty"`
	WorkspaceKey     string         `json:"workspace_key,omitempty"`
	CapabilityTier   CapabilityTier `json:"capability_tier"`
	HarnessID        string         `json:"harness_id,omitempty"`
	HarnessVersion   string         `json:"harness_version,omitempty"`
	HarnessSessionID string         `json:"harness_session_id,omitempty"`
	ExecAllowed      bool           `json:"exec_allowed,omitempty"`
}

// AdmissionRequest is the transport-neutral caller boundary. CallerID must
// come from authentication; Body is borrowed only for Admit's duration.
//
// CallerAttributes carries advisory claims of the authenticated principal
// (organization, roles, entitlements) for AdmissionPolicy and WorkerRouter.
// They never enter correctness identity: the request digest, idempotency
// comparison, persistence records, and worker assignments ignore them, so an
// exact retry may carry different attributes and still replay byte-for-byte.
// The map is borrowed only for Admit's duration; the core copies the top-level
// map before each policy call and never retains the values.
type AdmissionRequest struct {
	CallerID         CallerID
	IdempotencyKey   IdempotencyKey
	CodecID          CodecID
	Body             []byte
	Task             TaskContext
	CallerAttributes map[string]any
}

type AdmissionContext struct {
	CallerID       CallerID
	IdempotencyKey IdempotencyKey
	Codec          CodecDescription
	Request        Request
	Task           TaskContext
	// CallerAttributes is an independent top-level copy per policy call; the
	// attribute values themselves are borrowed and must not be mutated.
	CallerAttributes map[string]any
}

type AdmissionPolicyDecision struct {
	Allowed    bool
	Failure    AdmissionFailure
	RetryAfter time.Duration
}

// AdmissionPolicy runs before durable admission and assignment. Input is
// borrowed for the call; implementations must not retain or mutate its maps,
// slices, or RawMessage values. Implementations must be concurrency-safe and
// honor cancellation; any error fails admission without durable effects.
//
// Admission is a required deployment choice: NewService rejects a nil policy
// instead of silently allowing every request. AdmitAll is the explicit,
// auditable allow-everything policy for deployments where transport
// authentication is the only gate.
type AdmissionPolicy interface {
	Admit(context.Context, AdmissionContext) (AdmissionPolicyDecision, error)
}

// AdmitAll returns the explicit allow-everything AdmissionPolicy. It admits
// every structurally valid, authenticated request; use it only when transport
// authentication and the ToolAuthorizer are the deployment's real gates.
func AdmitAll() AdmissionPolicy {
	return AdmissionPolicyFunc(func(context.Context, AdmissionContext) (AdmissionPolicyDecision, error) {
		return AdmissionPolicyDecision{Allowed: true}, nil
	})
}

type AdmissionPolicyFunc func(context.Context, AdmissionContext) (AdmissionPolicyDecision, error)

func (policy AdmissionPolicyFunc) Admit(ctx context.Context, input AdmissionContext) (AdmissionPolicyDecision, error) {
	return policy(ctx, input)
}

type WorkerRouteRequest struct {
	CallerID       CallerID
	IdempotencyKey IdempotencyKey
	TaskID         TaskID
	Request        Request
	Task           TaskContext
	Candidates     []WorkerID
	// CallerAttributes is an independent top-level copy per route call carrying
	// the authenticated principal's advisory claims; the attribute values are
	// borrowed and must not be mutated. Attributes never enter routing
	// correctness state: the selected WorkerID alone is persisted.
	CallerAttributes map[string]any
}

// WorkerRouter selects a stable authenticated WorkerID. Returning an empty ID
// is equivalent to ErrWorkerUnavailable. It may deliberately route to a known
// but currently disconnected worker so durable work waits for reconnection.
// The borrowed implementation must be concurrency-safe until Service.Done,
// honor context cancellation, and not retain/mutate WorkerRouteRequest values.
//
// A Router is optional only for single-worker deployments: with a nil Router
// the core routes to the sole connected worker and fails closed with
// ErrWorkerRouterRequired as soon as a second worker connects.
type WorkerRouter interface {
	RouteWorker(context.Context, WorkerRouteRequest) (WorkerID, error)
}

type WorkerRouterFunc func(context.Context, WorkerRouteRequest) (WorkerID, error)

func (router WorkerRouterFunc) RouteWorker(ctx context.Context, input WorkerRouteRequest) (WorkerID, error) {
	return router(ctx, input)
}

type ToolAuthorization struct {
	CallerID CallerID
	Task     StoreTaskRecord
	Request  Request
	Call     ToolCall
}

// ToolAuthorizer is the fail-closed authority boundary for caller-executed
// tools. Nil permits only chat-tier tool calls; other tiers require an explicit
// authorizer. Calls happen outside Store callbacks. Input is borrowed for the
// call; implementations must not retain or mutate its nested mutable values.
// Implementations must be concurrency-safe until Service.Done and honor
// cancellation. Calls are retryable protocol work and may repeat when a prior
// attempt produced no durable WorkerReceipt, so implementations must be
// reentrant and must not consume one-shot authority as an untracked side
// effect. Only a framework Fault with CodeForbidden and RetryNever is a proved
// deterministic denial and produces a terminal worker NACK. CodeUnavailable,
// cancellation, and unclassified infrastructure errors fail closed without
// settling the durable worker outbox record, allowing an exact retry.
type ToolAuthorizer interface {
	AuthorizeTool(context.Context, ToolAuthorization) error
}

type ToolAuthorizerFunc func(context.Context, ToolAuthorization) error

func (authorizer ToolAuthorizerFunc) AuthorizeTool(ctx context.Context, input ToolAuthorization) error {
	return authorizer(ctx, input)
}

// Config composes the transport-neutral HumanLLM correctness core. DeploymentID
// is a non-secret persistence namespace, not caller authority. Protection AAD
// binds each blob to its authenticated CallerID and durable Task/Request key.
//
// Store and Protector are explicit Resources: ownership transfers to
// NewService even when construction fails, while borrowed values must remain
// valid until Service.Done. Other ports are borrowed for the runtime lifetime
// and must satisfy their documented concurrency contracts. No default database,
// network transport, authentication provider, or key provider is selected here.
// Store binding is namespace identity rather than an HA lease; without external
// fencing, only one active Service may drive one Store/DeploymentID pair.
type Config struct {
	DeploymentID string
	Store        framework.Resource[Store]
	Protector    protect.Resource
	// ProtectionReadPolicy defaults to requiring sealed records whenever a
	// Protector is configured. AllowPlain is an explicit migration escape hatch;
	// it must never be enabled merely to recover from authentication failures.
	ProtectionReadPolicy ProtectionReadPolicy
	Codecs               []CodecRegistration

	Clock          Clock
	IDs            IDSource
	Seeds          SeedSource
	Router         WorkerRouter
	Admission      AdmissionPolicy
	ToolAuthorizer ToolAuthorizer
	// Observer receives telemetry events (admission outcomes, worker sessions,
	// event settlements). Nil is a no-op. Events are emitted after their
	// durable decision and outside Store callbacks; a slow or panicking
	// Observer cannot affect correctness (see the observe package contract).
	Observer observe.Observer

	AssignmentBuffer int
	// WorkerPayloadLimitBytes is the maximum transport-neutral JSON assignment
	// or worker event, including a small envelope reserve. It must be no larger
	// than the configured worker transport/client write/read limit.
	WorkerPayloadLimitBytes int64
	ReadLimitBytes          int64
	// ReleaseTimeout bounds each owned Store/Protector release independently.
	// The runtime reaches Done even if a broken custom release callback ignores
	// cancellation; such an adapter goroutine is then the adapter's leak.
	ReleaseTimeout time.Duration
}

type ProtectionReadPolicy string

const (
	ProtectionRequireSealed ProtectionReadPolicy = "require_sealed"
	ProtectionAllowPlain    ProtectionReadPolicy = "allow_plain"
)

// ResponseDecision contains caller-visible, already-unprotected bytes.
type ResponseDecision struct {
	StatusCode  int
	ContentType string
	RetryAfter  string
	Body        []byte
}

type WireEvent struct {
	Sequence uint64
	Data     []byte
}

// ResponsePage is an ordered, cursor-based exact replay projection. Cursor is
// the highest persisted event sequence inspected, including private checkpoint
// records; callers must pass it back unchanged rather than derive it from the
// visible WireEvents slice.
type ResponsePage struct {
	Identity          CompletionIdentity
	RequestDigest     StoreDigest
	Mode              ResponseMode
	DecisionCommitted bool
	Decision          ResponseDecision
	Complete          bool
	Events            []WireEvent
	Cursor            uint64
}

type ResponseQuery struct {
	CallerID       CallerID
	IdempotencyKey IdempotencyKey
	RequestDigest  StoreDigest
	After          uint64
	Limit          int
	MaxBytes       int64
}

type AdmissionResult struct {
	Identity      CompletionIdentity
	RequestDigest StoreDigest
	Replay        bool
	Response      ResponsePage
}

// CallerEndpoint is the complete transport-neutral caller surface. HTTP,
// authentication, headers, and listener ownership belong to adapters which
// construct AdmissionRequest from an authenticated principal. Implementations
// are safe for concurrent calls; operation contexts never own the endpoint
// lifetime and cancellation does not imply that an ambiguous durable commit was
// rolled back.
type CallerEndpoint interface {
	Admit(context.Context, AdmissionRequest) (AdmissionResult, error)
	ReadResponse(context.Context, ResponseQuery) (ResponsePage, error)
	WaitResponse(context.Context, ResponseQuery) (ResponsePage, error)
}

// AdmissionError is safe for a caller transport to project directly. Cause is
// retained for local classification but must not be exposed by an adapter.
type AdmissionError struct {
	Failure     AdmissionFailure
	ContentType string
	RetryAfter  string
	Body        []byte
	Cause       error
}

func (failure *AdmissionError) Error() string {
	if failure == nil {
		return "<nil>"
	}
	return failure.Failure.Message
}

func (failure *AdmissionError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Cause
}

func validateServiceContentType(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= 256 &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func validateTaskContext(input TaskContext) error {
	if input.CapabilityTier == "" {
		input.CapabilityTier = TierChat
	}
	switch input.CapabilityTier {
	case TierChat:
		if input.TaskID != "" || input.WorkspaceKey != "" || input.HarnessID != "" ||
			input.HarnessVersion != "" || input.HarnessSessionID != "" || input.ExecAllowed {
			return fmt.Errorf("%w: chat requests cannot carry a stable workspace Task", ErrInvalidServiceConfig)
		}
	case TierRemoteTools, TierWorkspace:
		if err := validateOptionalStable("task id", string(input.TaskID)); err != nil {
			return err
		}
		for name, value := range map[string]string{
			"workspace key": input.WorkspaceKey, "harness id": input.HarnessID,
			"harness version": input.HarnessVersion, "harness session id": input.HarnessSessionID,
		} {
			if value == "" || !workerStableKeyPattern.MatchString(value) {
				return fmt.Errorf("%w: %s is required and must be a stable key", ErrInvalidServiceConfig, name)
			}
		}
	default:
		return fmt.Errorf("%w: unsupported capability tier %q", ErrInvalidServiceConfig, input.CapabilityTier)
	}
	return nil
}

func validateOptionalStable(name, value string) error {
	if value != "" && !workerStableKeyPattern.MatchString(value) {
		return fmt.Errorf("%w: %s must be a stable key", ErrInvalidServiceConfig, name)
	}
	return nil
}
