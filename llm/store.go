package llm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/vibe-agi/human/framework"
)

// StoreContractID names the semantic persistence contract implemented by a
// HumanLLM Store. It is independent of a database product, physical schema,
// serialization format, or deployment topology.
const StoreContractID framework.ContractID = "human.llm.store"

// StoreContractMajor changes when an implementation must change its atomicity,
// ordering, snapshot, recovery, or byte-ownership behavior to remain correct.
const StoreContractMajor uint16 = 1

var (
	// ErrStoreContractMismatch aliases the framework construction error for
	// callers that classify failures at the HumanLLM Store boundary.
	ErrStoreContractMismatch = framework.ErrContractMismatch
	// ErrStoreDescription means a Store returned missing, malformed, or unsafe
	// static metadata. Descriptions must never disclose a DSN or credential.
	ErrStoreDescription = errors.New("llm: invalid store description")
	// ErrStoreRecordNotFound reports that one logical record is absent from the
	// transaction snapshot.
	ErrStoreRecordNotFound = errors.New("llm: store record not found")
	// ErrStoreConflict reports a logical uniqueness, immutability, or
	// compare-and-swap conflict. StoreConflictError identifies the rule.
	ErrStoreConflict = errors.New("llm: store conflict")
	// ErrStoreCommitUnknown is the only valid result when a Store cannot prove
	// whether an Update committed. HumanLLM must reconcile durable request or
	// worker-event identity before retrying any effects.
	ErrStoreCommitUnknown = errors.New("llm: store commit outcome is unknown")
	// ErrStoreClosed means the resource behind Store no longer accepts work.
	// Store itself deliberately has no lifecycle method.
	ErrStoreClosed = errors.New("llm: store is closed")
	// ErrStoreRecordTooLarge means a bounded materialization was refused.
	ErrStoreRecordTooLarge = errors.New("llm: store record exceeds read budget")
	// ErrStoreCorruptRecord means physical data could not be represented as the
	// typed logical record promised by this contract. It is distinct from an
	// invalid canonical payload, which HumanLLM core detects and quarantines.
	ErrStoreCorruptRecord = errors.New("llm: corrupt store record")
)

// StoreRequirements returns HumanLLM core's immutable base requirements.
// Strict serializability, atomic receipts, stable snapshots, bounded scans,
// exact byte preservation, and durable restart visibility are major-contract
// semantics, not optional features.
func StoreRequirements() framework.Requirements {
	return framework.Requirements{ID: StoreContractID, Major: StoreContractMajor}
}

// StoreDescription is static, non-secret implementation metadata. Provider
// identifies the adapter (for example "sqlite" or a Go module path); Version is
// the adapter release or schema family, not a database address.
type StoreDescription struct {
	Contract framework.Contract
	Provider string
	Version  string
}

// Validate checks the implementation metadata and semantic contract.
func (description StoreDescription) Validate() error {
	if _, err := framework.Negotiate(description.Contract, StoreRequirements()); err != nil {
		return err
	}
	if !validStoreDescriptionField(description.Provider) ||
		!validStoreDescriptionField(description.Version) {
		return ErrStoreDescription
	}
	return nil
}

func validStoreDescriptionField(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 128 ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

// StoreRecordKind identifies a logical record without exposing a table,
// collection, key prefix, or vendor schema name.
type StoreRecordKind string

const (
	StoreRecordTask           StoreRecordKind = "task"
	StoreRecordRequest        StoreRecordKind = "request"
	StoreRecordResponseEvent  StoreRecordKind = "response_event"
	StoreRecordWorkerReceipt  StoreRecordKind = "worker_receipt"
	StoreRecordToolExecution  StoreRecordKind = "tool_execution"
	StoreRecordRecoveryCursor StoreRecordKind = "recovery_cursor"
)

// StoreNotFoundError adds a logical kind and safe stable key to
// ErrStoreRecordNotFound. Implementations may return the sentinel directly
// when no safe key is available.
type StoreNotFoundError struct {
	Record StoreRecordKind
	Key    string
}

func (failure *StoreNotFoundError) Error() string {
	if failure == nil || (failure.Record == "" && failure.Key == "") {
		return ErrStoreRecordNotFound.Error()
	}
	if failure.Key == "" {
		return fmt.Sprintf("%v: %s", ErrStoreRecordNotFound, failure.Record)
	}
	return fmt.Sprintf("%v: %s %q", ErrStoreRecordNotFound, failure.Record, failure.Key)
}

func (*StoreNotFoundError) Unwrap() error { return ErrStoreRecordNotFound }

// StoreConstraint identifies one logical uniqueness or immutability rule.
// Adapter errors must use these values instead of leaking index names or
// relying on parsed vendor error text.
type StoreConstraint string

const (
	StoreConstraintTaskKey          StoreConstraint = "task_key"
	StoreConstraintOpenAffinity     StoreConstraint = "open_task_affinity"
	StoreConstraintRequestKey       StoreConstraint = "request_key"
	StoreConstraintActiveRequest    StoreConstraint = "active_request_per_task"
	StoreConstraintResponseSequence StoreConstraint = "response_event_sequence"
	StoreConstraintWorkerEvent      StoreConstraint = "worker_event_identity"
	StoreConstraintWorkerReceipt    StoreConstraint = "worker_receipt_identity"
	StoreConstraintToolCall         StoreConstraint = "tool_call_identity"
	StoreConstraintImmutableRecord  StoreConstraint = "immutable_record"
	StoreConstraintCompareAndSwap   StoreConstraint = "compare_and_swap"
)

// StoreConflictError reports a typed logical conflict and supports
// errors.Is(err, ErrStoreConflict).
type StoreConflictError struct {
	Constraint StoreConstraint
	Key        string
}

func (failure *StoreConflictError) Error() string {
	if failure == nil || (failure.Constraint == "" && failure.Key == "") {
		return ErrStoreConflict.Error()
	}
	if failure.Key == "" {
		return fmt.Sprintf("%v: %s", ErrStoreConflict, failure.Constraint)
	}
	return fmt.Sprintf("%v: %s %q", ErrStoreConflict, failure.Constraint, failure.Key)
}

func (*StoreConflictError) Unwrap() error { return ErrStoreConflict }

// StoreCommitUnknownError preserves an infrastructure cause while supporting
// errors.Is for both ErrStoreCommitUnknown and that cause.
type StoreCommitUnknownError struct {
	Cause error
}

func (failure *StoreCommitUnknownError) Error() string {
	if failure == nil || failure.Cause == nil {
		return ErrStoreCommitUnknown.Error()
	}
	return fmt.Sprintf("%v: %v", ErrStoreCommitUnknown, failure.Cause)
}

func (failure *StoreCommitUnknownError) Unwrap() []error {
	if failure == nil || failure.Cause == nil {
		return []error{ErrStoreCommitUnknown}
	}
	return []error{ErrStoreCommitUnknown, failure.Cause}
}

// StoreLimitError describes a bounded read refusal and supports
// errors.Is(err, ErrStoreRecordTooLarge).
type StoreLimitError struct {
	Record StoreRecordKind
	Limit  int64
}

func (failure *StoreLimitError) Error() string {
	if failure == nil {
		return ErrStoreRecordTooLarge.Error()
	}
	return fmt.Sprintf("%v: %s exceeds %d bytes", ErrStoreRecordTooLarge, failure.Record, failure.Limit)
}

func (*StoreLimitError) Unwrap() error { return ErrStoreRecordTooLarge }

// StoreCorruptError classifies a physically malformed logical record without
// leaking a database location or query. Cause is retained for operators.
type StoreCorruptError struct {
	Record StoreRecordKind
	Key    string
	Cause  error
}

func (failure *StoreCorruptError) Error() string {
	if failure == nil {
		return ErrStoreCorruptRecord.Error()
	}
	detail := string(failure.Record)
	if failure.Key != "" {
		detail += " " + fmt.Sprintf("%q", failure.Key)
	}
	if detail == "" {
		detail = "record"
	}
	if failure.Cause == nil {
		return fmt.Sprintf("%v: %s", ErrStoreCorruptRecord, detail)
	}
	return fmt.Sprintf("%v: %s: %v", ErrStoreCorruptRecord, detail, failure.Cause)
}

func (failure *StoreCorruptError) Unwrap() []error {
	if failure == nil || failure.Cause == nil {
		return []error{ErrStoreCorruptRecord}
	}
	return []error{ErrStoreCorruptRecord, failure.Cause}
}

// Store is the complete persistence consistency boundary for HumanLLM. Tasks,
// completion requests, canonical payloads, response wire events, durable worker
// receipts, tool execution ledgers, retention tombstones, and recovery scans
// deliberately share one Store. Splitting them across independently committed
// implementations would invalidate exact replay and ACK safety.
//
// View must call fn exactly once with one stable read snapshot. Update must call
// fn exactly once in one strictly serializable transaction. Implementations
// must never retry either callback: HumanLLM callbacks may create timestamps,
// deterministic codec seeds, and exact conflict results from their snapshot.
// A callback and its StoreView or StoreTx are valid only until the enclosing
// method returns and must not be retained or used concurrently.
//
// If an Update callback returns an error, none of its writes may become visible.
// If it returns nil, every write becomes visible atomically or none does. A
// definite commit failure returns an ordinary error and guarantees no commit. A
// result that may have committed must match ErrStoreCommitUnknown. The core then
// reconciles the request key or worker-event receipt; it must not blindly repeat
// an observable response or tool side effect.
//
// The following correctness boundaries are composed by HumanLLM inside one
// Update, rather than implemented as driver-specific commands:
//
//   - admission: optional Task creation/reconciliation plus Request insertion;
//   - pre-stream failure: retry grant plus finite immutable HTTP decision;
//   - worker event: Task transition, tool-ledger entries, exact wire events,
//     terminal response state, and the WorkerReceipt used to ACK the outbox;
//   - recovery quarantine: finite response, optional terminal wire event, Task
//     repair, and quarantine marker;
//   - retention: request tombstone, response-event deletion, and eligible
//     terminal tool-result pruning.
//
// Successful Update effects are durable across process restart. View and Update
// are safe for concurrent callers. Implementations own independent copies of
// every byte slice, map, CodecSnapshot contract feature map, and nested mutable
// value after a successful write and return caller-owned copies from reads.
//
// Store intentionally has no Close method. Composition receives it through a
// framework.Resource[Store], making borrowed versus owned lifetime and release
// explicit without contaminating this business port.
type Store interface {
	// Description is static, non-secret metadata. It must not perform I/O and
	// remains safe to call while the containing resource is being released.
	Description() StoreDescription
	View(context.Context, func(StoreView) error) error
	Update(context.Context, func(StoreTx) error) error
}
