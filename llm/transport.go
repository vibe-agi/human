package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/vibe-agi/human/framework"
)

const (
	// WorkerTransportContractID names the authenticated HumanLLM worker
	// transport contract. It describes completion delivery and settlement
	// semantics, not a particular wire protocol.
	WorkerTransportContractID framework.ContractID = "human.llm.worker.transport"
	// WorkerTransportContractMajor includes exact assignment redelivery,
	// commit-before-ACK, deterministic NACK, and explicit connection lifetime.
	WorkerTransportContractMajor uint16 = 1
)

var (
	// ErrWorkerTransportContractMismatch aliases the framework-wide
	// construction error so callers can classify incompatible adapters without
	// parsing a HumanLLM-specific message.
	ErrWorkerTransportContractMismatch = framework.ErrContractMismatch
	ErrWorkerTransportDescription      = errors.New("llm: invalid worker transport description")
	ErrWorkerPrincipal                 = errors.New("llm: invalid authenticated worker principal")
	ErrWorkerDelivery                  = errors.New("llm: invalid worker delivery")

	// Runtime errors below settle neither an assignment nor an event. In
	// particular, a durable worker outbox must retain an event whenever
	// CommitEvent returns an error instead of a valid ACK or NACK receipt.
	ErrWorkerTransportClosed       = errors.New("llm: worker transport is closed")
	ErrWorkerConnectionClosed      = errors.New("llm: worker connection is closed")
	ErrWorkerConnectionConflict    = errors.New("llm: worker connection conflicts with an active session")
	ErrWorkerBackpressure          = errors.New("llm: worker transport is backpressured")
	ErrWorkerDeliveryNotFound      = errors.New("llm: worker delivery is not known to this connection")
	ErrWorkerDeliveryConflict      = errors.New("llm: worker delivery identity was reused with different content")
	ErrWorkerDeliveryIndeterminate = errors.New("llm: worker delivery commit outcome is indeterminate")
)

var workerStableKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// WorkerTransportRequirements returns a fresh copy of the HumanLLM worker
// transport contract required by the core.
func WorkerTransportRequirements() framework.Requirements {
	return framework.Requirements{
		ID:    WorkerTransportContractID,
		Major: WorkerTransportContractMajor,
	}
}

// WorkerTransportDescription is immutable, non-secret adapter metadata.
// Provider identifies the implementation (for example "websocket" or
// "grpc") and Version identifies that implementation, not its wire protocol.
type WorkerTransportDescription struct {
	Contract framework.Contract
	Provider string
	Version  string
}

// Validate negotiates the base contract and validates the adapter metadata.
func (description WorkerTransportDescription) Validate() error {
	_, err := NegotiateWorkerTransport(description)
	return err
}

// NegotiateWorkerTransport validates a description and returns an independent
// frozen copy. A composition root must cache this result once; runtime behavior
// must not depend on later mutations to Description().Contract.Features.
func NegotiateWorkerTransport(
	description WorkerTransportDescription,
) (WorkerTransportDescription, error) {
	contract, err := framework.Negotiate(description.Contract, WorkerTransportRequirements())
	if err != nil {
		return WorkerTransportDescription{}, err
	}
	if !validWorkerTransportMetadata(description.Provider) ||
		!validWorkerTransportMetadata(description.Version) {
		return WorkerTransportDescription{}, ErrWorkerTransportDescription
	}
	description.Contract = contract
	return description, nil
}

// WorkerTransport authenticates remote workers and projects a wire protocol
// onto WorkerEndpoint. It owns its listeners, sessions, and background delivery
// loops, but only borrows endpoint.
//
// Start's context bounds initialization only. After Start succeeds, the
// returned runtime owns the adapter lifetime. Shutdown must stop accepting new
// workers, shut down every WorkerConnection opened through endpoint, wait for
// adapter goroutines, and then return. It must never close endpoint or any Store
// or Protector reachable through endpoint.
//
// The transport, rather than an inbound payload, constructs
// AuthenticatedWorker after authentication. It may send a wire ACK/NACK only
// from the corresponding result of WorkerConnection.CommitEvent; accepting a
// frame into an in-memory queue is not an acknowledgement boundary.
type WorkerTransport interface {
	Description() WorkerTransportDescription
	Start(context.Context, WorkerEndpoint) (WorkerTransportRuntime, error)
}

// WorkerTransportRuntime is separated from WorkerTransport so a reusable
// adapter configuration is not confused with one running instance.
type WorkerTransportRuntime interface {
	framework.Runtime
}

// ValidateWorkerTransport rejects typed-nil adapters and returns their frozen
// negotiated description. It does not start network listeners.
func ValidateWorkerTransport(transport WorkerTransport) (WorkerTransportDescription, error) {
	if isNilWorkerTransport(transport) {
		return WorkerTransportDescription{}, fmt.Errorf("%w: transport is nil", ErrWorkerTransportDescription)
	}
	return NegotiateWorkerTransport(transport.Description())
}

// WorkerSessionID identifies one live transport connection only. It changes on
// reconnect and never participates in request, lease, delivery, or outbox
// correctness identity.
type WorkerSessionID string

// AuthenticatedWorker is constructed from a verified transport principal.
// Both fields are deliberately excluded from serialized worker payloads: a
// WorkerEvent is always bound to WorkerID by the opened WorkerConnection.
type AuthenticatedWorker struct {
	WorkerID  WorkerID        `json:"-"`
	SessionID WorkerSessionID `json:"-"`
}

// Validate checks the stable principal and ephemeral connection identity.
func (principal AuthenticatedWorker) Validate() error {
	if err := validateWorkerStableKey("worker id", string(principal.WorkerID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerPrincipal, err)
	}
	if err := validateWorkerStableKey("worker session id", string(principal.SessionID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerPrincipal, err)
	}
	return nil
}

// WorkerEndpoint is the narrow HumanLLM-core surface borrowed by a transport.
// OpenWorker is called only after transport authentication and policy checks.
// Its context bounds initialization only; the returned connection has an
// independent lifecycle. The transport shuts down that connection when the
// wire session ends, but never shuts down the endpoint itself. Implementations
// are safe for concurrent OpenWorker calls. A failed call returns no live
// connection and transfers no lifecycle obligation to the caller.
type WorkerEndpoint interface {
	OpenWorker(context.Context, AuthenticatedWorker) (WorkerConnection, error)
}

// WorkerConnection binds all completion operations to one authenticated
// principal.
//
// Assignments has one sender and closer: the connection. The transport is its
// only receiver. A delivery remains owned by the endpoint until AckAssignment
// succeeds or the connection ends. The transport may call AckAssignment only
// after the remote worker has durably stored the exact assignment. A missing or
// lost ACK permits value-equivalent redelivery with the same delivery ID on
// this or a later session of the same WorkerID.
//
// CommitEvent is the only event-to-wire-settlement boundary. It returns ACK
// only after every response frame/decision caused by the event and the exact
// worker-event receipt are durable, or after exact replay proves that boundary
// was crossed earlier. A deterministic NACK proves the event had no new effect
// and terminally removes that exact outbox record. An error settles nothing;
// transient cancellation after a possibly committed write must be reconciled
// before CommitEvent may return ACK, NACK, or ErrWorkerDeliveryIndeterminate.
//
// Calls to CommitEvent on one connection are ordered. A transport must not
// dequeue the next durable outbox record before the current call settles. When
// the remote peer or endpoint is slow, it must propagate bounded backpressure
// by pausing reads rather than dropping an assignment/event, acknowledging it
// early, growing an unbounded queue, or destroying an otherwise healthy
// connection.
type WorkerConnection interface {
	framework.Runtime
	Principal() AuthenticatedWorker
	Assignments() <-chan WorkerAssignmentDelivery
	AckAssignment(context.Context, WorkerDeliveryID) error
	CommitEvent(context.Context, WorkerEventDelivery) (WorkerEventReceipt, error)
}

// WorkerNotice is a transport-level alert the worker's human must see — for
// example, the caller holding an in-flight request disconnected. It is advisory
// (a UI hint), never a correctness signal: dropping one changes no durable
// state and is always safe.
type WorkerNotice struct {
	Code      string
	Message   string
	Caller    CallerID
	TaskID    TaskID
	RequestID string
}

// WorkerNoticer is an optional WorkerConnection capability. A connection may
// surface transport-level notices (a caller disconnect, say) that a transport
// adapter forwards to the human side. It is discovered by type assertion, so a
// WorkerConnection that does not implement it simply carries no notices and
// nothing breaks. The abstraction is the product; whether and how a given
// connection detects such events is that implementation's own concern.
type WorkerNoticer interface {
	Notices() <-chan WorkerNotice
}

// WorkerDeliveryID identifies one transport delivery. It is distinct from an
// idempotency key and worker event ID, and remains stable across retransmission.
type WorkerDeliveryID string

// CompletionIdentity contains every stable routing identity for one HumanLLM
// response. TaskID may span multiple completion requests. RequestID identifies
// one admitted request; IdempotencyKey controls exact caller replay. WorkspaceKey
// is empty only for a chat-only request. Lease identity is kept separately so a
// new lease cannot silently reuse an old outbox event.
type CompletionIdentity struct {
	CallerID       CallerID       `json:"caller_id"`
	RequestID      string         `json:"request_id"`
	TaskID         TaskID         `json:"task_id"`
	WorkspaceKey   string         `json:"workspace_key,omitempty"`
	IdempotencyKey IdempotencyKey `json:"idempotency_key"`
}

// Validate checks the stable completion namespace. Session IDs, socket
// sequence numbers, and token text are intentionally absent.
func (identity CompletionIdentity) Validate() error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{"caller id", string(identity.CallerID)},
		{"request id", identity.RequestID},
		{"task id", string(identity.TaskID)},
		{"idempotency key", string(identity.IdempotencyKey)},
	} {
		if err := validateWorkerStableKey(field.name, field.value); err != nil {
			return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
		}
	}
	if identity.WorkspaceKey != "" {
		if err := validateWorkerStableKey("workspace key", identity.WorkspaceKey); err != nil {
			return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
		}
	}
	return nil
}

// WorkerLease is the stable completion ownership generation. ID changes when
// ownership is re-granted, while Owner remains the authenticated WorkerID.
// Neither value is a connection/session identity.
type WorkerLease struct {
	ID    WorkerLeaseID `json:"id"`
	Owner WorkerID      `json:"owner"`
}

func (lease WorkerLease) validate() error {
	if err := validateWorkerStableKey("worker lease id", string(lease.ID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	if err := validateWorkerStableKey("worker lease owner", string(lease.Owner)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	return nil
}

// AssignmentBoundary records which caller-visible durability boundary the core
// crossed before making an assignment observable to a transport.
type AssignmentBoundary string

const (
	// AssignmentAfterAdmission is used for aggregate/non-streaming requests: the
	// request and response mode are durable, but the one HTTP decision is not
	// committed until a terminal worker event.
	AssignmentAfterAdmission AssignmentBoundary = "admission_committed"
	// AssignmentAfterResponse is required for streaming requests: the response
	// status and deterministic stream-start frames are durable before assignment.
	AssignmentAfterResponse AssignmentBoundary = "response_committed"
)

// Assignment is a codec-neutral, response-oriented unit of Human work. It is
// not a HumanAgent Task claim: a terminal Event ends this response, while a
// later tool-result continuation is a new RequestID/IdempotencyKey that may
// retain the same TaskID and WorkspaceKey.
type Assignment struct {
	Identity CompletionIdentity `json:"identity"`
	Lease    WorkerLease        `json:"lease"`
	Boundary AssignmentBoundary `json:"boundary"`
	// Task is authenticated logical workspace/capability context. It carries no
	// caller filesystem coordinates.
	Task    TaskContext `json:"task"`
	Request Request     `json:"request"`
}

// ValidateFor verifies canonical request ownership and the admission/response
// gate. Streaming work cannot become visible at the weaker admission-only
// boundary; aggregate work cannot claim that its still-undecided terminal HTTP
// response was already committed.
func (assignment Assignment) ValidateFor(principal AuthenticatedWorker) error {
	if err := principal.Validate(); err != nil {
		return err
	}
	if err := assignment.Identity.Validate(); err != nil {
		return err
	}
	if err := assignment.Lease.validate(); err != nil {
		return err
	}
	if assignment.Lease.Owner != principal.WorkerID {
		return fmt.Errorf("%w: assignment lease belongs to another worker", ErrWorkerDelivery)
	}
	task := normalizeTaskContext(assignment.Task)
	if err := validateTaskContext(task); err != nil {
		return fmt.Errorf("%w: invalid task context: %v", ErrWorkerDelivery, err)
	}
	if task.CapabilityTier == TierChat {
		if assignment.Identity.WorkspaceKey != "" {
			return fmt.Errorf("%w: chat assignment has a workspace identity", ErrWorkerDelivery)
		}
	} else if task.TaskID != assignment.Identity.TaskID || task.WorkspaceKey != assignment.Identity.WorkspaceKey {
		return fmt.Errorf("%w: assignment task context does not match identity", ErrWorkerDelivery)
	}
	if err := assignment.Request.Validate(); err != nil {
		return fmt.Errorf("%w: invalid canonical request: %v", ErrWorkerDelivery, err)
	}
	if assignment.Request.Stream {
		if assignment.Boundary != AssignmentAfterResponse {
			return fmt.Errorf("%w: streaming assignment was visible before response commit", ErrWorkerDelivery)
		}
	} else if assignment.Boundary != AssignmentAfterAdmission {
		return fmt.Errorf("%w: aggregate assignment has an invalid response boundary", ErrWorkerDelivery)
	}
	return nil
}

// WorkerAssignmentDelivery is one ACKable delivery of a committed Assignment.
// Exact repetition with the same ID is normal across disconnects.
type WorkerAssignmentDelivery struct {
	ID         WorkerDeliveryID `json:"delivery_id"`
	Assignment Assignment       `json:"assignment"`
}

// ValidateFor checks both delivery identity and authenticated assignment
// ownership. SessionID is intentionally not compared.
func (delivery WorkerAssignmentDelivery) ValidateFor(principal AuthenticatedWorker) error {
	if err := validateWorkerStableKey("worker delivery id", string(delivery.ID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	return delivery.Assignment.ValidateFor(principal)
}

// WorkerEventDelivery is one durable worker-outbox item. Identity and LeaseID
// are repeated explicitly so a reconnect never routes by socket-local state.
// Event.WorkerID must be empty; CommitEvent binds the event to
// WorkerConnection.Principal().WorkerID.
type WorkerEventDelivery struct {
	ID       WorkerDeliveryID   `json:"delivery_id"`
	Identity CompletionIdentity `json:"identity"`
	LeaseID  WorkerLeaseID      `json:"lease_id"`
	Event    Event              `json:"event"`
}

// Validate checks the transport-neutral shape. Deterministic failures are
// valid reasons for a NACK; a transport must not close the whole connection or
// retry a known poison item forever merely because this method returns an
// error.
func (delivery WorkerEventDelivery) Validate() error {
	if err := validateWorkerStableKey("worker delivery id", string(delivery.ID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	if err := delivery.Identity.Validate(); err != nil {
		return err
	}
	if err := validateWorkerStableKey("worker lease id", string(delivery.LeaseID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	return validateTransportEvent(delivery.Event)
}

// ValidateFor additionally verifies that no worker identity arrived through
// the payload. Authorization of Identity/LeaseID to the authenticated worker is
// performed by the core at CommitEvent against its durable assignment record.
func (delivery WorkerEventDelivery) ValidateFor(principal AuthenticatedWorker) error {
	if err := principal.Validate(); err != nil {
		return err
	}
	return delivery.Validate()
}

// WorkerEventDecision is a terminal transport settlement. Both values permit
// deletion of the exact event from a durable worker outbox; a CommitEvent error
// does not.
type WorkerEventDecision string

const (
	WorkerEventACK  WorkerEventDecision = "ack"
	WorkerEventNACK WorkerEventDecision = "nack"
)

// WorkerRejectionCode is selected by WorkerEndpoint. Message is diagnostics
// only and must never be parsed for protocol behavior.
type WorkerRejectionCode string

const (
	WorkerRejectInvalid        WorkerRejectionCode = "invalid"
	WorkerRejectForbidden      WorkerRejectionCode = "forbidden"
	WorkerRejectNotFound       WorkerRejectionCode = "not_found"
	WorkerRejectStaleLease     WorkerRejectionCode = "stale_lease"
	WorkerRejectResponseClosed WorkerRejectionCode = "response_closed"
	WorkerRejectEventConflict  WorkerRejectionCode = "event_conflict"
	WorkerRejectStateConflict  WorkerRejectionCode = "state_conflict"
	WorkerRejectToolConflict   WorkerRejectionCode = "tool_conflict"
)

// WorkerEventReceipt is exact-retry stable and is the only authority for a
// wire ACK/NACK. ACK means the exact event's response output and event receipt
// are durable. NACK means the event was deterministically rejected before any
// new effect became durable.
//
// For a terminal event that arrives after a response closed, an exact event
// matching an existing durable receipt is replayed as ACK. An unknown late
// event is NACKed with WorkerRejectResponseClosed; a reused event ID with a
// different digest is NACKed with WorkerRejectEventConflict. None of those
// outcomes reopens the response or tears down unrelated work.
type WorkerEventReceipt struct {
	Delivery WorkerDeliveryID    `json:"delivery_id"`
	EventID  string              `json:"event_id"`
	Decision WorkerEventDecision `json:"decision"`
	Code     WorkerRejectionCode `json:"code,omitempty"`
	Message  string              `json:"message,omitempty"`
}

// ValidateFor prevents a cumulative, reordered, or stale wire acknowledgement
// from settling the wrong durable outbox record. A malformed event may be
// NACKed if it still has stable delivery/event IDs, but it can never be ACKed.
func (receipt WorkerEventReceipt) ValidateFor(delivery WorkerEventDelivery) error {
	if err := validateWorkerStableKey("worker delivery id", string(delivery.ID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	if err := validateWorkerEventID(delivery.Event.ID); err != nil {
		return err
	}
	if receipt.Delivery != delivery.ID || receipt.EventID != delivery.Event.ID {
		return fmt.Errorf("%w: receipt identity does not match event delivery", ErrWorkerDelivery)
	}
	switch receipt.Decision {
	case WorkerEventACK:
		if err := delivery.Validate(); err != nil {
			return err
		}
		if receipt.Code != "" || receipt.Message != "" {
			return fmt.Errorf("%w: ACK receipt contains rejection detail", ErrWorkerDelivery)
		}
	case WorkerEventNACK:
		if !validWorkerRejectionCode(receipt.Code) {
			return fmt.Errorf("%w: NACK receipt has invalid rejection code", ErrWorkerDelivery)
		}
		if len(receipt.Message) > 4096 || !utf8.ValidString(receipt.Message) ||
			strings.ContainsRune(receipt.Message, '\x00') {
			return fmt.Errorf("%w: NACK receipt message is invalid", ErrWorkerDelivery)
		}
	default:
		return fmt.Errorf("%w: invalid event receipt decision %q", ErrWorkerDelivery, receipt.Decision)
	}
	return nil
}

// WorkerConnectionError adds authenticated session context while preserving
// errors.Is/errors.As classification through Cause. Authorization failures
// which could target another worker are connection errors, never NACKs that
// erase the suspect outbox record.
type WorkerConnectionError struct {
	Principal AuthenticatedWorker
	Cause     error
}

func (failure *WorkerConnectionError) Error() string {
	if failure == nil {
		return "<nil>"
	}
	if failure.Cause == nil {
		return "llm: worker connection failed"
	}
	return fmt.Sprintf("llm: worker connection %q failed: %v", failure.Principal.SessionID, failure.Cause)
}

func (failure *WorkerConnectionError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Cause
}

// WorkerDeliveryError identifies an unsettled delivery while preserving its
// typed cause. ErrWorkerDeliveryIndeterminate requires exact replay or
// reconciliation and must never be translated into ACK or NACK by an adapter.
type WorkerDeliveryError struct {
	Delivery WorkerDeliveryID
	EventID  string
	Cause    error
}

func (failure *WorkerDeliveryError) Error() string {
	if failure == nil {
		return "<nil>"
	}
	if failure.Cause == nil {
		return fmt.Sprintf("llm: worker delivery %q failed", failure.Delivery)
	}
	return fmt.Sprintf("llm: worker delivery %q failed: %v", failure.Delivery, failure.Cause)
}

func (failure *WorkerDeliveryError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Cause
}

// CloneWorkerAssignmentDelivery and CloneWorkerEventDelivery make ownership
// explicit at asynchronous adapter boundaries. Returned slices, maps, and raw
// JSON values do not alias their inputs.
func CloneWorkerAssignmentDelivery(delivery WorkerAssignmentDelivery) WorkerAssignmentDelivery {
	delivery.Assignment.Request = cloneTransportRequest(delivery.Assignment.Request)
	return delivery
}

func CloneWorkerEventDelivery(delivery WorkerEventDelivery) WorkerEventDelivery {
	delivery.Event.ToolCalls = cloneTransportToolCalls(delivery.Event.ToolCalls)
	return delivery
}

func validateTransportEvent(event Event) error {
	if err := validateWorkerEventID(event.ID); err != nil {
		return err
	}
	if event.WorkerID != "" {
		return fmt.Errorf("%w: worker id must come from the authenticated principal", ErrWorkerDelivery)
	}
	switch event.Type {
	case EventAccepted:
		if event.Text != "" || len(event.ToolCalls) != 0 || event.ErrorCode != "" || event.Error != "" {
			return fmt.Errorf("%w: accepted event has unexpected payload", ErrWorkerDelivery)
		}
	case EventProgress:
		if event.Text == "" || len(event.ToolCalls) != 0 || event.ErrorCode != "" || event.Error != "" {
			return fmt.Errorf("%w: progress event payload is invalid", ErrWorkerDelivery)
		}
	case EventFinal, EventClarification:
		if len(event.ToolCalls) != 0 || event.ErrorCode != "" || event.Error != "" {
			return fmt.Errorf("%w: text terminal event has unexpected payload", ErrWorkerDelivery)
		}
	case EventToolCalls:
		if event.Text != "" || len(event.ToolCalls) == 0 || event.ErrorCode != "" || event.Error != "" {
			return fmt.Errorf("%w: tool_calls event payload is invalid", ErrWorkerDelivery)
		}
		seen := make(map[string]struct{}, len(event.ToolCalls))
		for _, call := range event.ToolCalls {
			if strings.TrimSpace(call.ID) == "" || call.ID != strings.TrimSpace(call.ID) {
				return fmt.Errorf("%w: tool call id is required and trimmed", ErrWorkerDelivery)
			}
			if _, duplicate := seen[call.ID]; duplicate {
				return fmt.Errorf("%w: duplicate tool call id %q", ErrWorkerDelivery, call.ID)
			}
			seen[call.ID] = struct{}{}
			if err := ValidateToolIdentity(call.Namespace, call.Name); err != nil {
				return fmt.Errorf("%w: invalid tool call %q: %v", ErrWorkerDelivery, call.ID, err)
			}
			if call.Input != nil && call.TextInput != nil {
				return fmt.Errorf("%w: tool call %q has both JSON and text input", ErrWorkerDelivery, call.ID)
			}
			if _, err := json.Marshal(call.Input); err != nil {
				return fmt.Errorf("%w: tool call %q input is not JSON: %v", ErrWorkerDelivery, call.ID, err)
			}
		}
	case EventRejected, EventExpired, EventFailed, EventUnavailable:
		if event.Text != "" || len(event.ToolCalls) != 0 ||
			(event.ErrorCode == "" && event.Error == "") {
			return fmt.Errorf("%w: failure event payload is invalid", ErrWorkerDelivery)
		}
		if event.ErrorCode != "" && !workerStableKeyPattern.MatchString(event.ErrorCode) {
			return fmt.Errorf("%w: failure event code is invalid", ErrWorkerDelivery)
		}
	default:
		return fmt.Errorf("%w: unsupported worker event %q", ErrWorkerDelivery, event.Type)
	}
	return nil
}

func validateWorkerEventID(value string) error {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 512 ||
		!utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: worker event id is required, trimmed, and at most 512 bytes", ErrWorkerDelivery)
	}
	return nil
}

func validateWorkerStableKey(name, value string) error {
	if !workerStableKeyPattern.MatchString(value) {
		return fmt.Errorf("%s is required and must be a stable key", name)
	}
	return nil
}

func validWorkerTransportMetadata(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 128 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func validWorkerRejectionCode(code WorkerRejectionCode) bool {
	switch code {
	case WorkerRejectInvalid, WorkerRejectForbidden, WorkerRejectNotFound,
		WorkerRejectStaleLease,
		WorkerRejectResponseClosed, WorkerRejectEventConflict,
		WorkerRejectStateConflict, WorkerRejectToolConflict:
		return true
	default:
		return false
	}
}

func isNilWorkerTransport(transport WorkerTransport) bool {
	if transport == nil {
		return true
	}
	value := reflect.ValueOf(transport)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func cloneTransportRequest(request Request) Request {
	request.Messages = append([]Message(nil), request.Messages...)
	for messageIndex := range request.Messages {
		blocks := append([]Block(nil), request.Messages[messageIndex].Blocks...)
		for blockIndex := range blocks {
			blocks[blockIndex].Input = cloneTransportMap(blocks[blockIndex].Input)
			blocks[blockIndex].TextInput = cloneTransportString(blocks[blockIndex].TextInput)
			blocks[blockIndex].Output = cloneTransportJSONValue(blocks[blockIndex].Output)
		}
		request.Messages[messageIndex].Blocks = blocks
	}
	request.Tools = append([]Tool(nil), request.Tools...)
	for index := range request.Tools {
		request.Tools[index].InputSchema = append(json.RawMessage(nil), request.Tools[index].InputSchema...)
		request.Tools[index].InputFormat = append(json.RawMessage(nil), request.Tools[index].InputFormat...)
	}
	request.HostedCapabilities = append([]HostedCapability(nil), request.HostedCapabilities...)
	for index := range request.HostedCapabilities {
		request.HostedCapabilities[index].Configuration = append(
			json.RawMessage(nil), request.HostedCapabilities[index].Configuration...,
		)
	}
	request.OpaqueInput = append([]OpaqueInput(nil), request.OpaqueInput...)
	if request.Metadata != nil {
		metadata := make(map[string]string, len(request.Metadata))
		for key, value := range request.Metadata {
			metadata[key] = value
		}
		request.Metadata = metadata
	}
	return request
}

func cloneTransportToolCalls(calls []ToolCall) []ToolCall {
	cloned := append([]ToolCall(nil), calls...)
	for index := range cloned {
		cloned[index].Input = cloneTransportMap(cloned[index].Input)
		cloned[index].TextInput = cloneTransportString(cloned[index].TextInput)
	}
	return cloned
}

func cloneTransportString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneTransportMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	cloned := make(map[string]any, len(value))
	for key, item := range value {
		cloned[key] = cloneTransportJSONValue(item)
	}
	return cloned
}

func cloneTransportJSONValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneTransportMap(typed)
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			cloned[index] = cloneTransportJSONValue(item)
		}
		return cloned
	case json.RawMessage:
		return append(json.RawMessage(nil), typed...)
	case []byte:
		return append([]byte(nil), typed...)
	default:
		return typed
	}
}
