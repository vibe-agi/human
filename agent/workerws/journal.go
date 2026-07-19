package workerws

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

var stableWorkerIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

const (
	// JournalContractID names the durable remote-worker inbox/outbox contract.
	// Its major version includes FIFO enumeration, exact-input replay, durable
	// settlement tombstones, and atomic event-to-rejection settlement.
	JournalContractID    framework.ContractID = "human.agent.worker.journal"
	JournalContractMajor uint16               = 1

	MaxJournalPageSize = 1024
)

var (
	ErrJournalDescription   = errors.New("invalid Agent worker journal description")
	ErrJournalClosed        = errors.New("Agent worker journal is closed")
	ErrJournalNotFound      = errors.New("Agent worker journal record was not found")
	ErrJournalConflict      = errors.New("Agent worker journal binding or delivery conflicts with durable state")
	ErrJournalCorrupt       = errors.New("Agent worker journal contains a corrupt record")
	ErrJournalCommitUnknown = errors.New("Agent worker journal commit outcome is unknown")
	ErrJournalLimit         = errors.New("Agent worker journal read limit was exceeded")
)

// JournalDescription is static, non-secret adapter metadata. Provider and
// Version identify the implementation, not a database or tenant.
type JournalDescription struct {
	Contract framework.Contract
	Provider string
	Version  string
}

func (description JournalDescription) Validate() error {
	if _, err := framework.Negotiate(description.Contract, JournalRequirements()); err != nil {
		return err
	}
	if !validJournalMetadata(description.Provider) || !validJournalMetadata(description.Version) {
		return ErrJournalDescription
	}
	return nil
}

func JournalRequirements() framework.Requirements {
	return framework.Requirements{ID: JournalContractID, Major: JournalContractMajor}
}

// JournalSequence is allocated monotonically by a Journal. It establishes FIFO
// presentation within each pending collection and is never reused.
type JournalSequence uint64

// GatewayID is the durable identity of one gateway consistency domain. It is
// not a URL: changing DNS, ports, or transports does not change GatewayID.
type GatewayID string

func (identity GatewayID) Validate() error {
	if !stableWorkerIdentity.MatchString(string(identity)) {
		return fmt.Errorf("gateway id must match %s", stableWorkerIdentity.String())
	}
	return nil
}

// JournalBinding prevents a durable outbox from being replayed into another
// gateway, authority, or worker after a configuration or URL change.
type JournalBinding struct {
	Gateway   GatewayID
	Authority agent.AuthorityID
	Worker    agent.WorkerID
}

func (binding JournalBinding) Validate() error {
	if err := binding.Gateway.Validate(); err != nil {
		return err
	}
	return agent.AuthenticatedWorker{
		Authority: binding.Authority, Worker: binding.Worker, Session: "binding-validation",
	}.Validate()
}

// JournalDigest is a lowercase SHA-256 digest calculated by the official
// client over the canonical JSON representation of a public delivery/receipt.
// Journal implementations preserve it verbatim; they do not reinterpret the
// payload or choose their own equivalence relation.
type JournalDigest string

func (digest JournalDigest) Validate() error {
	if len(digest) != 64 {
		return fmt.Errorf("%w: digest must contain 64 lowercase hexadecimal characters", ErrJournalCorrupt)
	}
	for _, character := range digest {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return fmt.Errorf("%w: digest must contain 64 lowercase hexadecimal characters", ErrJournalCorrupt)
		}
	}
	return nil
}

// JournalEntryState tells PutAssignment/PutEvent whether the exact delivery is
// still pending or has a durable settlement tombstone. A settled exact replay
// is never presented or sent again. Reusing its ID with a different digest is a
// conflict even after the payload has been compacted.
type JournalEntryState string

const (
	JournalEntryPending JournalEntryState = "pending"
	JournalEntrySettled JournalEntryState = "settled"
)

type JournalAssignment struct {
	Sequence JournalSequence
	Digest   JournalDigest
	Delivery agent.WorkerAssignmentDelivery
}

type JournalEvent struct {
	Sequence JournalSequence
	Digest   JournalDigest
	Delivery agent.WorkerEventDelivery
}

// RejectedEvent is the application-facing NACK, including the exact reply or
// command that was rejected so it can be corrected after a process restart.
type RejectedEvent struct {
	Delivery agent.WorkerEventDelivery
	Receipt  agent.WorkerEventReceipt
}

// JournalRejection is the durable application-facing NACK inbox. Confirming it
// removes both payloads but retains their compact digest tombstone.
type JournalRejection struct {
	Sequence      JournalSequence
	EventDigest   JournalDigest
	ReceiptDigest JournalDigest
	RejectedEvent
}

// Journal is the persistence port borrowed or owned by Client through a
// framework.Resource. It deliberately has no Close method.
//
// Implementations take ownership of inputs before a successful return and
// return independent copies from List methods; mutable byte slices must never
// alias adapter storage or another caller. All operations other than
// Description and Bind fail closed until a durable binding exists.
//
// Every mutating call is one atomic, strictly serializable transaction. Calls
// are not implicitly retried by the implementation. A successful call is
// durable before it returns. ErrJournalCommitUnknown means the caller must
// reconcile by repeating the exact operation; it must never assume success or
// failure.
//
// PutAssignment and PutEvent allocate a non-zero monotonically increasing
// sequence for a new ID. An exact existing digest returns its current state and
// does not allocate a sequence. A different digest for any pending or settled
// ID returns ErrJournalConflict. ConfirmAssignment replaces the pending payload
// with a compact settled tombstone atomically.
//
// SettleEvent validates the pending event identity, removes it from the outbox,
// and writes a compact settled tombstone atomically. For a NACK it atomically
// moves the full event and receipt into the rejection inbox; ConfirmRejection
// later compacts both payloads. Exact repeated receipt
// settlement is a no-op; a different event or receipt digest conflicts.
//
// List methods return records with Sequence > after in strictly increasing
// order, up to limit. Callers may page without holding a transaction open.
type Journal interface {
	Description() JournalDescription
	// Bind is an idempotent durability operation. The first call permanently
	// records binding; an exact call succeeds and any different value returns
	// ErrJournalConflict. All other operations fail closed while unbound.
	Bind(context.Context, JournalBinding) error

	PutAssignment(context.Context, JournalAssignment) (JournalEntryState, error)
	ConfirmAssignment(context.Context, agent.WorkerDeliveryID) error
	ListAssignments(context.Context, JournalSequence, int) ([]JournalAssignment, error)

	PutEvent(context.Context, JournalEvent) (JournalEntryState, error)
	ListEvents(context.Context, JournalSequence, int) ([]JournalEvent, error)
	SettleEvent(context.Context, agent.WorkerEventReceipt, JournalDigest, JournalDigest) error

	ListRejections(context.Context, JournalSequence, int) ([]JournalRejection, error)
	ConfirmRejection(context.Context, agent.WorkerDeliveryID) error
}

// JournalError attaches an operation and delivery identity while preserving
// errors.Is/errors.As classification through Cause.
type JournalError struct {
	Operation string
	Delivery  agent.WorkerDeliveryID
	Cause     error
}

func (failure *JournalError) Error() string {
	if failure == nil {
		return "<nil>"
	}
	identity := ""
	if failure.Delivery != "" {
		identity = fmt.Sprintf(" delivery %q", failure.Delivery)
	}
	if failure.Cause == nil {
		return fmt.Sprintf("Agent worker journal %s%s failed", failure.Operation, identity)
	}
	return fmt.Sprintf("Agent worker journal %s%s failed: %v", failure.Operation, identity, failure.Cause)
}

func (failure *JournalError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Cause
}

func validJournalMetadata(value string) bool {
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

func validateJournalLimit(limit int) error {
	if limit < 1 || limit > MaxJournalPageSize {
		return fmt.Errorf("%w: page limit must be 1..%d", ErrJournalLimit, MaxJournalPageSize)
	}
	return nil
}
