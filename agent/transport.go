package agent

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/workspace"
)

const (
	// WorkerTransportContractID names the authenticated Agent worker transport
	// contract. It describes delivery semantics, not a particular wire protocol.
	WorkerTransportContractID framework.ContractID = "human.agent.worker.transport"
	// WorkerTransportContractMajor includes exact redelivery, commit-before-ACK,
	// terminal NACK, and explicit connection-lifetime semantics.
	WorkerTransportContractMajor uint16 = 1
)

var (
	// ErrWorkerTransportContractMismatch aliases the framework-wide constructor
	// error so callers can classify all incompatible adapters consistently.
	ErrWorkerTransportContractMismatch = framework.ErrContractMismatch
	ErrWorkerTransportDescription      = errors.New("invalid Agent worker transport description")
	ErrWorkerPrincipal                 = errors.New("invalid authenticated Agent worker principal")
	ErrWorkerDelivery                  = errors.New("invalid Agent worker delivery")

	// Runtime errors below are deliberately transport-neutral. Concrete adapters
	// may wrap them in WorkerConnectionError or WorkerDeliveryError.
	ErrWorkerTransportClosed       = errors.New("Agent worker transport is closed")
	ErrWorkerConnectionClosed      = errors.New("Agent worker connection is closed")
	ErrWorkerConnectionConflict    = errors.New("Agent worker connection conflicts with an active session")
	ErrWorkerDeliveryNotFound      = errors.New("Agent worker delivery is not known to this connection")
	ErrWorkerDeliveryConflict      = errors.New("Agent worker delivery identity was reused with different input")
	ErrWorkerDeliveryIndeterminate = errors.New("Agent worker delivery commit outcome is indeterminate")
)

// WorkerTransportRequirements returns the immutable contract required by the
// HumanAgent composition root.
func WorkerTransportRequirements() framework.Requirements {
	return framework.Requirements{
		ID:    WorkerTransportContractID,
		Major: WorkerTransportContractMajor,
	}
}

// WorkerTransportDescription is static, non-secret adapter metadata. Provider
// identifies an implementation (for example "websocket" or "grpc") and Version
// identifies that implementation, not its negotiated wire version.
type WorkerTransportDescription struct {
	Contract framework.Contract
	Provider string
	Version  string
}

func (description WorkerTransportDescription) Validate() error {
	if _, err := framework.Negotiate(description.Contract, WorkerTransportRequirements()); err != nil {
		return err
	}
	if !validWorkerTransportMetadata(description.Provider) ||
		!validWorkerTransportMetadata(description.Version) {
		return ErrWorkerTransportDescription
	}
	return nil
}

// WorkerTransport owns its listeners, sessions, and background delivery loops,
// but only borrows endpoint. Start must not retain ctx after initialization; the
// returned runtime owns its lifetime. Shutdown must stop admission, shut down all
// WorkerConnections opened through endpoint, and wait for adapter goroutines.
// It must never close endpoint, type-assert it to *Agent, or close dependencies
// reachable through endpoint.
type WorkerTransport interface {
	Description() WorkerTransportDescription
	Start(context.Context, WorkerEndpoint) (WorkerTransportRuntime, error)
}

// WorkerTransportRuntime is separated from WorkerTransport so a reusable
// adapter value is not itself confused with one running instance.
type WorkerTransportRuntime interface {
	framework.Runtime
}

// WorkerSessionID identifies one live connection only. It is deliberately not
// part of Task, command, lease, delivery, or idempotency identity and therefore
// changes on reconnect.
type WorkerSessionID string

// AuthenticatedWorker is constructed by a transport after authentication. No
// inbound WorkerEventDelivery contains WorkerID; WorkerEndpoint binds every
// event to this principal when it constructs the durable LeaseGrant.
type AuthenticatedWorker struct {
	Authority AuthorityID     `json:"-"`
	Worker    WorkerID        `json:"-"`
	Session   WorkerSessionID `json:"-"`
}

func (principal AuthenticatedWorker) Validate() error {
	if err := validateStable("authority id", string(principal.Authority)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerPrincipal, err)
	}
	if err := validateStable("worker id", string(principal.Worker)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerPrincipal, err)
	}
	if err := validateStable("worker session id", string(principal.Session)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerPrincipal, err)
	}
	return nil
}

// WorkerEndpoint is the narrow Agent-core surface borrowed by a transport.
// OpenWorker is called only after transport authentication and policy checks.
// Its context bounds initialization only; after success the returned connection
// has an independent lifecycle. The transport must call connection.Shutdown
// when the wire session ends, but it never shuts down the endpoint itself.
type WorkerEndpoint interface {
	OpenWorker(context.Context, AuthenticatedWorker) (WorkerConnection, error)
}

// WorkerConnection binds every operation to one authenticated principal.
//
// Assignments has one sender and closer: the connection. The transport is its
// only receiver. A delivery remains owned by the connection until
// AckAssignment succeeds or the connection ends. The transport may ACK only
// after the remote worker has durably stored the exact assignment. Lack of ACK,
// an ACK lost in transit, or reconnect permits exact value-equivalent redelivery
// with the same Delivery ID; task acceptance is a separate WorkerEvent.
//
// CommitEvent is the only event-to-ACK boundary. An ACK receipt may be returned
// only after the corresponding Agent command and its durable command receipt
// have committed (or an exact committed command was replayed). A NACK is a
// deterministic terminal rejection and also tells a durable worker outbox to
// delete that record. An error settles nothing: the outbox retains the exact
// delivery and retries or reconciles according to its error classification.
type WorkerConnection interface {
	framework.Runtime
	Principal() AuthenticatedWorker
	Assignments() <-chan WorkerAssignmentDelivery
	AckAssignment(context.Context, WorkerDeliveryID) error
	CommitEvent(context.Context, WorkerEventDelivery) (WorkerEventReceipt, error)
}

// WorkerDeliveryID identifies one transport delivery. It is intentionally
// distinct from a domain CommandID and remains stable across retransmission.
type WorkerDeliveryID string

// WorkerAssignmentDelivery contains a previously committed lease assignment.
// Repetition is normal protocol behavior and never creates a new lease fence.
type WorkerAssignmentDelivery struct {
	ID         WorkerDeliveryID `json:"delivery_id"`
	Assignment LeaseAssignment  `json:"assignment"`
}

// ValidateFor verifies both the delivery shape and its authenticated routing.
// Session is intentionally not compared because it is not correctness identity.
func (delivery WorkerAssignmentDelivery) ValidateFor(principal AuthenticatedWorker) error {
	if err := principal.Validate(); err != nil {
		return err
	}
	if err := validateStable("worker delivery id", string(delivery.ID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	if err := validateLeaseAssignment(delivery.Assignment); err != nil {
		return fmt.Errorf("%w: invalid lease assignment: %v", ErrWorkerDelivery, err)
	}
	grant := delivery.Assignment.Grant
	if grant.Worker != principal.Worker || grant.Task.Workspace.Authority != principal.Authority {
		return fmt.Errorf("%w: assignment does not belong to authenticated worker", ErrWorkerDelivery)
	}
	return nil
}

// WorkerEventKind is the closed set of Agent domain commands accepted from a
// worker. Adding a kind requires a transport-contract minor or major revision;
// adapters must never pass unknown values through as generic executable tools.
type WorkerEventKind string

const (
	WorkerEventAcceptTask     WorkerEventKind = "accept_task"
	WorkerEventRejectTask     WorkerEventKind = "reject_task"
	WorkerEventRequestInput   WorkerEventKind = "request_input"
	WorkerEventFailTask       WorkerEventKind = "fail_task"
	WorkerEventFreezeArtifact WorkerEventKind = "freeze_artifact"
	WorkerEventCompleteTask   WorkerEventKind = "complete_task"
)

// WorkerArtifactFreeze is present only for WorkerEventFreezeArtifact. Payload
// is declarative workspace data; the worker transport never applies it locally.
type WorkerArtifactFreeze struct {
	Artifact             ArtifactID         `json:"artifact"`
	ExpectedBaseRevision workspace.Revision `json:"expected_base_revision"`
	Payload              workspace.Payload  `json:"payload"`
}

// WorkerEvent is transport-neutral input to one worker-authorized Agent command.
// ID is the durable domain command/event identity; Fence and Task identify the
// lease generation. WorkerID is structurally absent and must be taken from the
// WorkerConnection principal at the commit boundary.
//
// Message is required only by request_input and complete_task. Submission and
// optional Artifact are used only by complete_task. Freeze is used only by
// freeze_artifact. Unused fields must be zero so an adapter cannot smuggle an
// ignored interpretation through different codecs.
type WorkerEvent struct {
	ID               CommandID             `json:"event_id"`
	Kind             WorkerEventKind       `json:"kind"`
	Task             TaskRef               `json:"task"`
	Fence            LeaseFence            `json:"fence"`
	ExpectedRevision uint64                `json:"expected_revision"`
	Message          *MessageInput         `json:"message,omitempty"`
	Submission       SubmissionID          `json:"submission,omitempty"`
	Artifact         *ArtifactRef          `json:"artifact,omitempty"`
	Freeze           *WorkerArtifactFreeze `json:"freeze,omitempty"`
}

// WorkerEventDelivery is the worker outbox unit. Exact retries use the same ID
// and the same Event. Reusing ID with different Event is a deterministic NACK.
type WorkerEventDelivery struct {
	ID    WorkerDeliveryID `json:"delivery_id"`
	Event WorkerEvent      `json:"event"`
}

func (delivery WorkerEventDelivery) Validate() error {
	if err := validateStable("worker delivery id", string(delivery.ID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	return delivery.Event.validate()
}

// ValidateFor additionally checks the authenticated authority. There is no
// payload WorkerID to compare or trust; CommitEvent supplies principal.Worker
// when binding Event.Fence into a LeaseGrant.
func (delivery WorkerEventDelivery) ValidateFor(principal AuthenticatedWorker) error {
	if err := principal.Validate(); err != nil {
		return err
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	if delivery.Event.Task.Workspace.Authority != principal.Authority {
		return fmt.Errorf("%w: event Task belongs to another authority", ErrWorkerDelivery)
	}
	return nil
}

func (event WorkerEvent) validate() error {
	if err := validateStable("worker event id", string(event.ID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	if err := validateTaskRef(event.Task); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	if event.Fence == 0 || event.Fence > LeaseFence(math.MaxInt64) {
		return fmt.Errorf("%w: lease fence must be in 1..MaxInt64", ErrWorkerDelivery)
	}
	if event.ExpectedRevision == 0 || event.ExpectedRevision > math.MaxInt64 {
		return fmt.Errorf("%w: expected revision must be in 1..MaxInt64", ErrWorkerDelivery)
	}

	switch event.Kind {
	case WorkerEventAcceptTask, WorkerEventRejectTask, WorkerEventFailTask:
		if event.Message != nil || event.Submission != "" || event.Artifact != nil || event.Freeze != nil {
			return fmt.Errorf("%w: %s has unexpected payload fields", ErrWorkerDelivery, event.Kind)
		}
	case WorkerEventRequestInput:
		if event.Message == nil || event.Submission != "" || event.Artifact != nil || event.Freeze != nil {
			return fmt.Errorf("%w: request_input payload shape is invalid", ErrWorkerDelivery)
		}
		if err := validateMessageInput(*event.Message); err != nil {
			return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
		}
	case WorkerEventFreezeArtifact:
		if event.Message != nil || event.Submission != "" || event.Artifact != nil || event.Freeze == nil {
			return fmt.Errorf("%w: freeze_artifact payload shape is invalid", ErrWorkerDelivery)
		}
		if err := validateStable("artifact id", string(event.Freeze.Artifact)); err != nil {
			return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
		}
		if err := validateRevision("expected base revision", event.Freeze.ExpectedBaseRevision); err != nil {
			return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
		}
		if err := validateArtifactPayloadShape(event.Freeze.Payload); err != nil {
			return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
		}
	case WorkerEventCompleteTask:
		if event.Message == nil || event.Submission == "" || event.Freeze != nil {
			return fmt.Errorf("%w: complete_task payload shape is invalid", ErrWorkerDelivery)
		}
		if err := validateMessageInput(*event.Message); err != nil {
			return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
		}
		if err := validateStable("submission id", string(event.Submission)); err != nil {
			return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
		}
		if event.Artifact != nil {
			if err := validateArtifactRef(*event.Artifact); err != nil || event.Artifact.Workspace != event.Task.Workspace {
				return fmt.Errorf("%w: completion Artifact does not belong to Task workspace", ErrWorkerDelivery)
			}
		}
	default:
		return fmt.Errorf("%w: unsupported worker event kind %q", ErrWorkerDelivery, event.Kind)
	}
	return nil
}

// WorkerEventDecision is a terminal transport settlement. Both values remove
// the exact event from a durable worker outbox; an error from CommitEvent does
// not. ACK means committed/replayed. NACK means deterministically rejected
// without accepting the requested domain transition.
type WorkerEventDecision string

const (
	WorkerEventACK  WorkerEventDecision = "ack"
	WorkerEventNACK WorkerEventDecision = "nack"
)

// WorkerRejectionCode is a stable machine code selected by WorkerEndpoint. It
// is intentionally more specific than an HTTP or WebSocket status.
type WorkerRejectionCode string

const (
	WorkerRejectInvalid         WorkerRejectionCode = "invalid"
	WorkerRejectForbidden       WorkerRejectionCode = "forbidden"
	WorkerRejectNotFound        WorkerRejectionCode = "not_found"
	WorkerRejectStaleLease      WorkerRejectionCode = "stale_lease"
	WorkerRejectRevision        WorkerRejectionCode = "revision_conflict"
	WorkerRejectState           WorkerRejectionCode = "state_conflict"
	WorkerRejectCommandConflict WorkerRejectionCode = "command_conflict"
)

// WorkerEventReceipt is exact-retry stable. ACK receipts may be emitted only at
// the domain durability boundary described by WorkerConnection.CommitEvent.
// NACK Code is safe for protocol logic; Message is diagnostic and must not be
// parsed for behavior.
type WorkerEventReceipt struct {
	Delivery WorkerDeliveryID    `json:"delivery_id"`
	Event    CommandID           `json:"event_id"`
	Decision WorkerEventDecision `json:"decision"`
	Code     WorkerRejectionCode `json:"code,omitempty"`
	Message  string              `json:"message,omitempty"`
}

// ValidateFor prevents a cumulative or reordered wire ACK from settling the
// wrong durable outbox record.
func (receipt WorkerEventReceipt) ValidateFor(delivery WorkerEventDelivery) error {
	if err := validateStable("worker delivery id", string(delivery.ID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	if receipt.Delivery != delivery.ID || receipt.Event != delivery.Event.ID {
		return fmt.Errorf("%w: receipt identity does not match event delivery", ErrWorkerDelivery)
	}
	switch receipt.Decision {
	case WorkerEventACK:
		// A malformed event can be terminally NACKed so it does not become an
		// outbox poison pill, but it can never be acknowledged as committed.
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
		if len(receipt.Message) > 4096 || !utf8.ValidString(receipt.Message) || strings.ContainsRune(receipt.Message, '\x00') {
			return fmt.Errorf("%w: NACK receipt message is invalid", ErrWorkerDelivery)
		}
	default:
		return fmt.Errorf("%w: invalid event receipt decision %q", ErrWorkerDelivery, receipt.Decision)
	}
	return nil
}

// WorkerConnectionError adds authenticated session context while preserving
// errors.Is/errors.As classification through Cause.
type WorkerConnectionError struct {
	Principal AuthenticatedWorker
	Cause     error
}

func (failure *WorkerConnectionError) Error() string {
	if failure == nil {
		return "<nil>"
	}
	if failure.Cause == nil {
		return "Agent worker connection failed"
	}
	return fmt.Sprintf("Agent worker connection %q failed: %v", failure.Principal.Session, failure.Cause)
}

func (failure *WorkerConnectionError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Cause
}

// WorkerDeliveryError identifies an unsettled delivery while preserving its
// typed cause. In particular, ErrWorkerDeliveryIndeterminate requires exact
// replay/reconciliation and must never be translated into ACK or NACK.
type WorkerDeliveryError struct {
	Delivery WorkerDeliveryID
	Event    CommandID
	Cause    error
}

func (failure *WorkerDeliveryError) Error() string {
	if failure == nil {
		return "<nil>"
	}
	if failure.Cause == nil {
		return fmt.Sprintf("Agent worker delivery %q failed", failure.Delivery)
	}
	return fmt.Sprintf("Agent worker delivery %q failed: %v", failure.Delivery, failure.Cause)
}

func (failure *WorkerDeliveryError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Cause
}

// CloneWorkerAssignmentDelivery and CloneWorkerEventDelivery make ownership
// explicit at asynchronous adapter boundaries. Returned byte slices do not
// alias their inputs.
func CloneWorkerAssignmentDelivery(delivery WorkerAssignmentDelivery) WorkerAssignmentDelivery {
	delivery.Assignment = cloneLeaseAssignment(delivery.Assignment)
	return delivery
}

func CloneWorkerEventDelivery(delivery WorkerEventDelivery) WorkerEventDelivery {
	if delivery.Event.Message != nil {
		message := cloneMessageInput(*delivery.Event.Message)
		delivery.Event.Message = &message
	}
	if delivery.Event.Artifact != nil {
		artifact := *delivery.Event.Artifact
		delivery.Event.Artifact = &artifact
	}
	if delivery.Event.Freeze != nil {
		freeze := *delivery.Event.Freeze
		freeze.Payload.Data = append([]byte(nil), delivery.Event.Freeze.Payload.Data...)
		delivery.Event.Freeze = &freeze
	}
	return delivery
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
		WorkerRejectStaleLease, WorkerRejectRevision, WorkerRejectState,
		WorkerRejectCommandConflict:
		return true
	default:
		return false
	}
}
