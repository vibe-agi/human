package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/workspace"
)

// StoreContractID names the semantic contract implemented by an Agent Store.
// It is deliberately independent of any physical schema, database product, or
// serialization format.
const StoreContractID framework.ContractID = "human.agent.store"

// StoreContractMajor changes when an implementation must change its base
// transaction, query, or record semantics to remain correct. Minor revisions
// may add behavior without weakening the major contract.
const StoreContractMajor uint16 = 1

var (
	// ErrStoreContractMismatch aliases the framework-wide construction error for
	// callers that classify failures at the Agent Store boundary.
	ErrStoreContractMismatch = framework.ErrContractMismatch
	// ErrStoreDescription means a Store returned missing, malformed, or unsafe
	// implementation metadata. Description metadata must never contain a DSN,
	// credential, or another secret.
	ErrStoreDescription = errors.New("invalid agent store description")
	// ErrStoreRecordNotFound is the storage-level absence sentinel. Agent maps
	// this to the appropriate domain error for the record being loaded.
	ErrStoreRecordNotFound = errors.New("agent store record not found")
	// ErrStoreConflict reports a uniqueness or immutable-record collision. A
	// compare-and-swap miss is reported by the mutation's boolean result instead.
	ErrStoreConflict = errors.New("agent store constraint conflict")
	// ErrStoreCommitUnknown is the only valid result when a Store cannot prove
	// whether an Update committed. The caller must retry the exact Agent command;
	// its command receipt resolves the ambiguity without repeating a committed
	// transition.
	ErrStoreCommitUnknown = errors.New("agent store commit outcome is unknown")
	// ErrStoreClosed means the resource behind a Store no longer admits View or
	// Update calls. Store itself has no lifecycle method; framework.Resource owns
	// release explicitly.
	ErrStoreClosed = errors.New("agent store is closed")
	// ErrStoreRecordTooLarge means a bounded read refused to materialize a record.
	ErrStoreRecordTooLarge = errors.New("agent store record exceeds read budget")
)

// StoreRequirements returns the immutable base contract required by the Agent
// core. Atomicity, strict serializability, durable command receipts, lease
// fencing, snapshot reads, ordering, and byte ownership are all mandatory
// major-contract semantics, not optional features.
func StoreRequirements() framework.Requirements {
	return framework.Requirements{ID: StoreContractID, Major: StoreContractMajor}
}

// StoreDescription identifies an implementation without exposing its backing
// location. Provider should identify the driver (for example, "sqlite" or a Go
// module path); Version is the driver's own release version. Contract describes
// behavior, not the driver's schema version.
//
// Description values are suitable for diagnostics and audit metadata. They
// must not contain database paths, network addresses with credentials, DSNs,
// tokens, key identifiers, or tenant data.
type StoreDescription struct {
	Contract framework.Contract
	Provider string
	Version  string
}

// Validate checks the implementation metadata and exact semantic contract.
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

// StoreRecordKind identifies a logical record without exposing a table or a
// document collection. Values are stable diagnostics and conformance labels.
type StoreRecordKind string

const (
	StoreRecordCommand       StoreRecordKind = "command"
	StoreRecordTask          StoreRecordKind = "task"
	StoreRecordMessage       StoreRecordKind = "message"
	StoreRecordEvent         StoreRecordKind = "event"
	StoreRecordLeaseGrant    StoreRecordKind = "lease_grant"
	StoreRecordWorkspaceHead StoreRecordKind = "workspace_head"
	StoreRecordArtifact      StoreRecordKind = "artifact"
	StoreRecordSubmission    StoreRecordKind = "submission"
	StoreRecordApplyReceipt  StoreRecordKind = "apply_receipt"
)

// StoreNotFoundError adds a logical record kind and non-secret stable key to
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

// StoreConstraint identifies the logical uniqueness or immutability rule that
// rejected an insert. Store implementations must not expose vendor index names
// or parse error strings at the Agent boundary.
type StoreConstraint string

const (
	StoreConstraintCommandID       StoreConstraint = "command_id"
	StoreConstraintTaskKey         StoreConstraint = "task_key"
	StoreConstraintPublicTaskID    StoreConstraint = "public_task_id"
	StoreConstraintMessageID       StoreConstraint = "message_id"
	StoreConstraintMessageSequence StoreConstraint = "message_sequence"
	StoreConstraintEventSequence   StoreConstraint = "event_sequence"
	StoreConstraintLeaseFence      StoreConstraint = "lease_fence"
	StoreConstraintArtifactID      StoreConstraint = "artifact_id"
	StoreConstraintArtifactTask    StoreConstraint = "artifact_task"
	StoreConstraintSubmissionID    StoreConstraint = "submission_id"
	StoreConstraintReceiptID       StoreConstraint = "receipt_id"
)

// StoreConflictError reports a typed logical constraint collision and supports
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
// errors.Is for both ErrStoreCommitUnknown and the cause. Cause may be nil.
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
	return fmt.Sprintf(
		"%v: %s exceeds %d bytes",
		ErrStoreRecordTooLarge,
		failure.Record,
		failure.Limit,
	)
}

func (*StoreLimitError) Unwrap() error { return ErrStoreRecordTooLarge }

// Store is the complete persistence consistency boundary for HumanAgent.
// Tasks, command receipts, messages, events, lease history, Artifacts,
// submissions, apply receipts, and Workspace heads deliberately share this one
// Store. Splitting them across independently committed implementations would
// invalidate the Agent state machine.
//
// View must call fn exactly once with a stable read snapshot. Update must call
// fn exactly once in a strictly serializable transaction. Implementations must
// never retry either callback: Agent callbacks can derive timestamps and return
// precise conflict information from the snapshot they observed. A callback and
// its StoreView or StoreTx are valid only until the enclosing method returns and
// must not be retained or used concurrently.
//
// If an Update callback returns an error, none of its writes may become visible.
// If it returns nil, all writes, including the command receipt, become visible
// atomically or none do. A definite commit failure returns an ordinary error and
// guarantees no commit. A failure that may have committed must return an error
// matching ErrStoreCommitUnknown. An exact retry then observes either the whole
// command receipt and replays it, or no receipt and safely attempts the command.
//
// Successful Update effects are durable across a process restart. View and
// Update must be safe for concurrent callers. Implementations own copies of all
// byte slices after a successful write and return caller-owned copies from all
// reads; mutable aliases may not cross a transaction boundary.
//
// Store intentionally has no Close method. Construction receives it through a
// framework.Resource[Store], which makes borrowed versus owned lifetime and the
// release callback explicit without contaminating this business port.
type Store interface {
	// Description returns static, non-secret implementation metadata. It must be
	// safe before and during resource release and must not perform network or
	// storage I/O.
	Description() StoreDescription
	// View executes exactly one read-only callback against one stable snapshot.
	View(context.Context, func(StoreView) error) error
	// Update executes exactly one callback in one atomic, strictly serializable
	// transaction. See Store's commit-ambiguity contract.
	Update(context.Context, func(StoreTx) error) error
}

// StoreDigest is an opaque digest emitted and verified by the Agent core. A
// Store must preserve it byte-for-byte and must not recalculate it with a
// driver-specific serialization.
type StoreDigest string

// StoreCommandRecord is the durable idempotency receipt for one Agent command.
// Result contains the exact canonical result bytes produced by Agent; the Store
// does not decode or re-encode them. The key is (Authority, ID).
type StoreCommandRecord struct {
	Authority    AuthorityID
	ID           CommandID
	Kind         string
	IntentDigest StoreDigest
	ResultKind   string
	Result       []byte
	ResultDigest StoreDigest
	CreatedAt    time.Time
}

// StoreLeaseState is the mutable lease pointer embedded in a Task record. A
// zero Owner means unleased. Fence never decreases and identifies the latest
// immutable StoreLeaseGrantRecord even after that generation is fenced.
type StoreLeaseState struct {
	Owner WorkerID
	Fence LeaseFence
}

// StoreTaskRecord is the materialized Task aggregate plus its non-public
// current lease. ArtifactState is nil exactly when Task.Artifact is nil; Agent
// uses it to fail closed when an implementation returns a Task/Artifact state
// mismatch.
type StoreTaskRecord struct {
	Task          Task
	Lease         StoreLeaseState
	ArtifactState *ArtifactState
}

// StoreMessageRecord keeps canonical encoded Parts and their digest alongside
// message metadata. Agent decodes and verifies both on every read.
type StoreMessageRecord struct {
	ID           MessageID
	Task         TaskRef
	Sequence     uint64
	Author       Author
	EncodedParts []byte
	PartsDigest  StoreDigest
	CreatedAt    time.Time
}

// StoreEventRecord is one append-only Task revision event.
type StoreEventRecord struct {
	Event Event
}

// StoreLeaseGrantRecord is immutable history for one monotonically increasing
// Task fence. Current lease state may be empty, but committed history is never
// deleted or rewritten.
type StoreLeaseGrantRecord struct {
	Grant     LeaseGrant
	GrantedAt time.Time
}

// StoreArtifactRecord contains immutable payload identity and its mutable
// publication state. State transitions may not change payload bytes or any
// digest/revision identity field.
type StoreArtifactRecord struct {
	Content ArtifactContent
}

// StoreSubmissionRecord is written once, in the same transaction that publishes
// an optional Artifact and moves its Task to completed.
type StoreSubmissionRecord struct {
	Submission Submission
}

// StoreApplyReceiptRecord is the caller-side terminal decision for one exact
// published Artifact.
type StoreApplyReceiptRecord struct {
	Receipt ApplyReceipt
}

// StoreWorkspaceHeadRecord is the confirmed caller Workspace revision. A
// successful apply receipt advances it by compare-and-swap in the same Update.
type StoreWorkspaceHeadRecord struct {
	Head WorkspaceHead
}

// StoreCommandKey is authority-qualified because command IDs are unique only
// inside an authenticated Authority.
type StoreCommandKey struct {
	Authority AuthorityID
	ID        CommandID
}

// StoreReadLimit is a hard materialization budget for one opaque record field.
// MaxBytes must be positive. A Store must reject a larger value before returning
// the blob, using an error matching ErrStoreRecordTooLarge. It must never treat
// a zero value as unlimited.
type StoreReadLimit struct {
	MaxBytes int64
}

// StoreMessageKey is authority-qualified because message IDs are unique only
// inside an authenticated Authority.
type StoreMessageKey struct {
	Authority AuthorityID
	ID        MessageID
}

// StoreTaskContextScan selects a bounded page ordered by CreatedAt ascending,
// then Workspace ID and Task ID ascending. Limit is the physical maximum number
// of records returned; Agent normally requests one extra record to derive
// HasMore.
type StoreTaskContextScan struct {
	Context ContextRef
	After   *TaskPageCursor
	Limit   int
}

// StoreTaskAuthorityScan selects a bounded authority-wide page ordered by
// UpdatedAt descending, then Workspace ID and Task ID ascending. Filters and
// cursor have the same meaning as TaskQuery. Limit is the physical maximum;
// TotalSize counts the filtered set before applying After and must be observed
// in the same View snapshot as Records.
type StoreTaskAuthorityScan struct {
	Authority        AuthorityID
	Context          ContextID
	State            TaskState
	UpdatedAtOrAfter *time.Time
	After            *TaskQueryCursor
	Limit            int
}

// StoreTaskAuthorityResult is the raw authority query result from one View.
type StoreTaskAuthorityResult struct {
	Records   []StoreTaskRecord
	TotalSize uint64
}

// StoreMessageScan selects Task messages with Sequence greater than After in
// ascending order. ReadLimit is a hard aggregate encoded-Parts budget. A driver
// must fail with ErrStoreRecordTooLarge when the first record cannot fit; after
// at least one record fits it returns the largest contiguous prefix within the
// budget. Agent compares the last sequence with Task.MessageCount to derive
// HasMore without a driver-specific continuation flag.
type StoreMessageScan struct {
	Task      TaskRef
	After     uint64
	Limit     int
	ReadLimit StoreReadLimit
}

// StoreEventScan selects Task events with Sequence greater than After in
// ascending order.
type StoreEventScan struct {
	Task  TaskRef
	After uint64
	Limit int
}

// StoreLeaseScan selects current leases for one authenticated worker, ordered
// by GrantedAt, Workspace ID, Task ID, and Fence, all ascending.
type StoreLeaseScan struct {
	Authority AuthorityID
	Worker    WorkerID
	After     *LeasePageCursor
	Limit     int
}

// StoreView is a transaction-bound logical snapshot. It intentionally exposes
// no SQL, table, driver cursor, or transport type. Every returned record and
// byte slice belongs to the caller. Absence is reported with an error matching
// ErrStoreRecordNotFound.
type StoreView interface {
	// LookupCommand loads canonical result bytes subject to limit.
	LookupCommand(StoreCommandKey, StoreReadLimit) (StoreCommandRecord, error)
	LoadTask(TaskRef) (StoreTaskRecord, error)
	ResolveTask(AuthorityID, TaskID) (TaskRef, error)
	// LoadMessage loads canonical encoded Parts subject to limit.
	LoadMessage(StoreMessageKey, StoreReadLimit) (StoreMessageRecord, error)
	// LoadArtifact loads immutable payload bytes subject to limit.
	LoadArtifact(ArtifactRef, StoreReadLimit) (StoreArtifactRecord, error)
	LoadApplyReceipt(ArtifactRef) (StoreApplyReceiptRecord, error)
	LoadWorkspaceHead(WorkspaceRef) (StoreWorkspaceHeadRecord, error)
	LoadLeaseGrant(TaskRef, LeaseFence) (StoreLeaseGrantRecord, error)
	LoadLatestLeaseGrant(TaskRef) (StoreLeaseGrantRecord, error)
	FindClaimableTask(AuthorityID) (StoreTaskRecord, error)
	ScanContextTasks(StoreTaskContextScan) ([]StoreTaskRecord, error)
	ScanAuthorityTasks(StoreTaskAuthorityScan) (StoreTaskAuthorityResult, error)
	ScanMessages(StoreMessageScan) ([]StoreMessageRecord, error)
	ScanEvents(StoreEventScan) ([]StoreEventRecord, error)
	ScanLeases(StoreLeaseScan) ([]LeaseAssignment, error)
}

// StoreTaskCondition is the compare-and-swap precondition for a Task mutation.
// ExpectedRevision is always matched. ExpectedLease nil ignores the lease (for
// caller-authorized transitions); a non-nil zero value requires an unleased
// Task, and a non-zero value requires that exact owner/fence generation.
type StoreTaskCondition struct {
	ExpectedRevision uint64
	ExpectedLease    *StoreLeaseState
}

// StoreTaskMutation replaces one Task aggregate when Condition still matches.
// Ref, Next.Task.Ref, and any nested Task references must be identical. A false
// result from CompareAndSwapTask means no field was changed.
type StoreTaskMutation struct {
	Ref       TaskRef
	Condition StoreTaskCondition
	Next      StoreTaskRecord
}

// StoreArtifactMutation changes only publication state and its associated
// terminal timestamp without materializing the immutable payload. Exactly one
// of PublishedAt or DiscardedAt is set according to NextState. Identity,
// payload, digests, revisions, media type, Task, and FrozenAt remain unchanged.
// A false result means ExpectedState or Task no longer matches.
type StoreArtifactMutation struct {
	Ref           ArtifactRef
	Task          TaskRef
	ExpectedState ArtifactState
	NextState     ArtifactState
	PublishedAt   *time.Time
	DiscardedAt   *time.Time
}

// StoreWorkspaceHeadMutation advances a Workspace head by exact revision CAS.
// A false result means ExpectedRevision is no longer confirmed.
type StoreWorkspaceHeadMutation struct {
	Workspace        WorkspaceRef
	ExpectedRevision workspace.Revision
	Next             StoreWorkspaceHeadRecord
}

// StoreTx extends one StoreView with typed mutation primitives. These methods
// do not commit independently. All successful calls become visible together
// only when the enclosing Store.Update commits.
//
// Immutable inserts return an error matching ErrStoreConflict on any logical
// uniqueness collision. Compare-and-swap methods return (false, nil) on an
// expected-value miss and must not partially modify a record. The Agent core,
// not the Store, owns state-transition policy, command digests, timestamps,
// replay validation, and lease authorization.
type StoreTx interface {
	StoreView
	InsertCommand(StoreCommandRecord) error
	InsertTask(StoreTaskRecord) error
	CompareAndSwapTask(StoreTaskMutation) (bool, error)
	InsertMessage(StoreMessageRecord) error
	InsertEvent(StoreEventRecord) error
	InsertLeaseGrant(StoreLeaseGrantRecord) error
	InsertArtifact(StoreArtifactRecord) error
	CompareAndSwapArtifact(StoreArtifactMutation) (bool, error)
	InsertSubmission(StoreSubmissionRecord) error
	InsertWorkspaceHead(StoreWorkspaceHeadRecord) (bool, error)
	CompareAndSwapWorkspaceHead(StoreWorkspaceHeadMutation) (bool, error)
	InsertApplyReceipt(StoreApplyReceiptRecord) error
}
