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
const StoreContractMajor uint16 = 2

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
	// Update calls, or a callback-scoped StoreView/StoreTx was used after its
	// callback returned. Store itself has no lifecycle method; framework.Resource
	// owns release explicitly.
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
// A non-nil context and callback are required. A nil argument is rejected with
// ErrInvalidArgument without invoking the callback; a context already done is
// returned without invoking it. Once admitted, View and Update call the callback
// exactly once. Implementations must never retry a callback: Agent callbacks can
// derive timestamps and return precise conflict information from the snapshot
// they observed. The enclosing context supplies every primitive's cancellation
// and deadline; an implementation must not replace it with a background context.
//
// View supplies one stable read snapshot. Update supplies one atomic, strictly
// serializable read/write transaction with read-your-writes. A callback and its
// StoreView or StoreTx are valid only until the enclosing method returns and must
// not be retained or used concurrently. A primitive used outside that lifetime
// returns an error matching ErrStoreClosed and performs no work.
//
// If an Update callback returns an error, none of its writes may become visible.
// View and Update return an error matching the callback error; a rollback failure
// may be joined without hiding it.
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
// Store serializability is not a live-runtime lease. Unless the embedding host
// adds external generation/fencing coordination, exactly one active Agent may
// drive a Store correctness namespace. Two Agent instances can otherwise
// present the same durable Task to different Humans before either one commits.
//
// Store intentionally has no Close method. Construction receives it through a
// framework.Resource[Store], which makes borrowed versus owned lifetime and the
// release callback explicit without contaminating this business port.
type Store interface {
	// Description returns static, non-secret implementation metadata. It must be
	// safe before and during resource release and must not perform network or
	// storage I/O.
	Description() StoreDescription
	// View executes one callback against a stable, read-only snapshot. On a valid,
	// live invocation it calls the callback exactly once and returns an error
	// matching the callback's result. The context bounds snapshot acquisition and
	// every StoreView primitive.
	View(context.Context, func(StoreView) error) error
	// Update executes one callback in one atomic, strictly serializable transaction
	// with read-your-writes. On a valid, live invocation it calls the callback
	// exactly once. A callback error rolls back and remains matchable in the return;
	// a nil callback result enters the commit-ambiguity contract described above.
	// The context bounds transaction acquisition, every StoreTx primitive, and
	// commit, but cancellation never permits a partial commit.
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

// StoreMessageRecord keeps a versioned opaque StoredValue containing canonical
// encoded Parts and their plaintext digest alongside message metadata. Store
// implementations preserve EncodedParts byte-for-byte and never decrypt it.
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

// StoreArtifactRecord contains immutable plaintext payload identity, its
// mutable publication state, and a versioned opaque StoredValue. Artifact
// PayloadSize and PayloadDigest always describe canonical plaintext;
// EncodedPayload has an independent physical size and is preserved byte-for-byte.
// State transitions may not change EncodedPayload or any identity field.
type StoreArtifactRecord struct {
	Artifact       Artifact
	EncodedPayload []byte
}

// StoreSubmissionRecord contains only durable identity, routing, and timestamp
// metadata. Final user content lives in its referenced protected Message and
// optional Artifact; Store implementations must not duplicate that payload here.
// It is written once, in the same transaction that publishes
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
// then Workspace ID and Task ID ascending. After is an exclusive lexicographic
// cursor in that order. Limit is the physical maximum number of records returned
// and must be in 1..MaxPageSize+1; zero has no default meaning at the Store port.
// Agent normally requests one extra record to derive HasMore.
type StoreTaskContextScan struct {
	Context ContextRef
	After   *TaskPageCursor
	Limit   int
}

// StoreTaskAuthorityScan selects a bounded authority-wide page ordered by
// UpdatedAt descending, then Workspace ID and Task ID ascending. Context and
// State zero values do not filter; UpdatedAtOrAfter is inclusive. After is an
// exclusive lexicographic cursor in the mixed direction above. Limit is the
// physical maximum, must be in 1..MaxPageSize+1, and has no zero default at the
// Store port. TotalSize counts the filtered set before applying After and must
// be observed in the same View snapshot as Records.
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

// StoreMessageScan selects Task messages with Sequence strictly greater than
// After in ascending order. Limit is an item cap in 1..MaxPageSize+1. ReadLimit
// is a hard aggregate EncodedParts budget. A Store fails with
// ErrStoreRecordTooLarge when the first record cannot fit; after at least one
// record fits it returns the largest ordered contiguous prefix within the
// budget. Agent compares the last sequence with Task.MessageCount to derive
// HasMore without a driver-specific continuation flag.
type StoreMessageScan struct {
	Task      TaskRef
	After     uint64
	Limit     int
	ReadLimit StoreReadLimit
}

// StoreEventScan selects Task events with Sequence strictly greater than After
// in ascending order. Limit is an item cap in 1..MaxPageSize+1 and has no zero
// default at this port.
type StoreEventScan struct {
	Task  TaskRef
	After uint64
	Limit int
}

// StoreLeaseScan selects current (not historical) leases for one authenticated
// worker, ordered by GrantedAt, Workspace ID, Task ID, and Fence, all ascending.
// After is an exclusive lexicographic cursor in that order. Limit is an item cap
// in 1..MaxPageSize+1 and has no zero default at this port.
type StoreLeaseScan struct {
	Authority AuthorityID
	Worker    WorkerID
	After     *LeasePageCursor
	Limit     int
}

// StoreView is a transaction-bound logical snapshot. It intentionally exposes
// no SQL, table, driver cursor, or transport type. Every returned record and
// byte slice belongs to the caller. Single-record lookup, load, resolve, and
// find methods report absence with an error matching ErrStoreRecordNotFound;
// scan methods instead return a successful empty result. Every read made through
// the StoreView embedded in StoreTx observes earlier writes in that Update.
type StoreView interface {
	// LookupCommand loads the immutable command keyed by (Authority, ID). MaxBytes
	// applies to Result only; absence matches ErrStoreRecordNotFound.
	LookupCommand(StoreCommandKey, StoreReadLimit) (StoreCommandRecord, error)
	// LoadTask loads the aggregate whose Task.Ref exactly equals ref. It includes
	// the current lease and any Artifact/Submission links visible in this snapshot;
	// absence matches ErrStoreRecordNotFound.
	LoadTask(TaskRef) (StoreTaskRecord, error)
	// ResolveTask resolves the authority-wide public identity (authority, task ID)
	// to its unique Workspace-qualified TaskRef. It must never cross authority
	// boundaries; absence matches ErrStoreRecordNotFound.
	ResolveTask(AuthorityID, TaskID) (TaskRef, error)
	// LoadMessage loads the immutable message keyed by (Authority, Message ID). The
	// returned record ID and Task authority must match that key. MaxBytes applies
	// to EncodedParts; absence matches ErrStoreRecordNotFound.
	LoadMessage(StoreMessageKey, StoreReadLimit) (StoreMessageRecord, error)
	// LoadArtifact loads the Artifact whose Artifact.Ref exactly equals ref.
	// MaxBytes applies to EncodedPayload; absence matches ErrStoreRecordNotFound.
	LoadArtifact(ArtifactRef, StoreReadLimit) (StoreArtifactRecord, error)
	// LoadApplyReceipt loads the immutable terminal receipt keyed by its exact
	// ArtifactRef; absence matches ErrStoreRecordNotFound.
	LoadApplyReceipt(ArtifactRef) (StoreApplyReceiptRecord, error)
	// LoadWorkspaceHead loads the head whose WorkspaceRef exactly equals ref;
	// absence matches ErrStoreRecordNotFound.
	LoadWorkspaceHead(WorkspaceRef) (StoreWorkspaceHeadRecord, error)
	// LoadLeaseGrant loads immutable history for the exact (TaskRef, Fence) key;
	// absence matches ErrStoreRecordNotFound.
	LoadLeaseGrant(TaskRef, LeaseFence) (StoreLeaseGrantRecord, error)
	// LoadLatestLeaseGrant returns the record with the greatest Fence for ref, not
	// the record with the greatest wall-clock time. No history matches
	// ErrStoreRecordNotFound.
	LoadLatestLeaseGrant(TaskRef) (StoreLeaseGrantRecord, error)
	// FindClaimableTask returns the deterministic oldest unleased, non-terminal
	// Task in authority: CreatedAt, Workspace ID, then Task ID, all ascending. It
	// only selects; it does not reserve or mutate the Task. No candidate matches
	// ErrStoreRecordNotFound.
	FindClaimableTask(AuthorityID) (StoreTaskRecord, error)
	// ScanContextTasks returns only records in scan.Context, in the exact order and
	// exclusive-cursor semantics defined by StoreTaskContextScan. It returns at
	// most Limit records; zero matches are a successful empty result.
	ScanContextTasks(StoreTaskContextScan) ([]StoreTaskRecord, error)
	// ScanAuthorityTasks applies all filters and returns Records, ordering, and
	// TotalSize exactly as StoreTaskAuthorityScan specifies. Zero matches are a
	// successful result with TotalSize zero.
	ScanAuthorityTasks(StoreTaskAuthorityScan) (StoreTaskAuthorityResult, error)
	// ScanMessages returns at most Limit messages in the exact order, cursor, and
	// aggregate byte-budget semantics defined by StoreMessageScan. Zero matches are
	// a successful empty result.
	ScanMessages(StoreMessageScan) ([]StoreMessageRecord, error)
	// ScanEvents returns at most Limit events in the exact order and exclusive
	// cursor semantics defined by StoreEventScan. Zero matches are a successful
	// empty result.
	ScanEvents(StoreEventScan) ([]StoreEventRecord, error)
	// ScanLeases returns at most Limit assignments in StoreLeaseScan order. Each
	// item couples the current Task lease with the immutable grant at that same
	// Fence from this snapshot. Zero matches are a successful empty result.
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

// StoreTaskMutation replaces one existing Task aggregate when Condition still
// matches. Ref and Next.Task.Ref must be identical; Context and CreatedAt are
// immutable, and every nested Task/Workspace reference must remain in the same
// identity scope. ExpectedRevision must be non-zero. A false result from
// CompareAndSwapTask means no field was changed.
type StoreTaskMutation struct {
	Ref       TaskRef
	Condition StoreTaskCondition
	Next      StoreTaskRecord
}

// StoreArtifactMutation changes only publication state and its associated
// terminal timestamp without materializing the immutable payload. Exactly one
// of PublishedAt or DiscardedAt is set according to NextState. Identity,
// payload, digests, revisions, media type, Task, and FrozenAt remain unchanged.
// A false result means the record is absent or ExpectedState or Task no longer
// matches.
type StoreArtifactMutation struct {
	Ref           ArtifactRef
	Task          TaskRef
	ExpectedState ArtifactState
	NextState     ArtifactState
	PublishedAt   *time.Time
	DiscardedAt   *time.Time
}

// StoreWorkspaceHeadMutation advances a Workspace head by exact revision CAS.
// Workspace and Next.Head.Workspace must be identical. A false result means the
// head is absent or ExpectedRevision is no longer confirmed.
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
// uniqueness collision, including an exact duplicate; the typed constraint
// named below is part of the portable contract. InsertWorkspaceHead is the sole
// first-writer-wins exception and reports existence with its boolean. CAS and
// first-writer methods return (false, nil) for the documented miss and must not
// partially modify a record. The Agent core, not the Store, owns state-transition
// policy, command digests, timestamps, replay validation, and lease authorization.
type StoreTx interface {
	StoreView
	// InsertCommand inserts the immutable (Authority, ID) receipt. Any collision
	// returns ErrStoreConflict with StoreConstraintCommandID.
	InsertCommand(StoreCommandRecord) error
	// InsertTask inserts an initial Task. It must be unleased at fence zero and
	// have no pre-bound Artifact, ArtifactState, or Submission. Task.Ref is unique;
	// (Authority, Task ID) is also unique across Workspaces. Collisions use
	// StoreConstraintTaskKey and StoreConstraintPublicTaskID respectively.
	InsertTask(StoreTaskRecord) error
	// CompareAndSwapTask replaces an existing Task when ExpectedRevision and, when
	// non-nil, ExpectedLease match. A revision/lease miss returns (false, nil); an
	// absent Task matches ErrStoreRecordNotFound. Invalid or changed identity is an
	// error. Artifact and Submission links in Next are backed by their paired
	// immutable inserts in the same Update, rather than created by this primitive.
	CompareAndSwapTask(StoreTaskMutation) (bool, error)
	// InsertMessage inserts an immutable message. (Authority, Message ID) and
	// (TaskRef, Sequence) are unique; collisions use StoreConstraintMessageID and
	// StoreConstraintMessageSequence respectively.
	InsertMessage(StoreMessageRecord) error
	// InsertEvent inserts an immutable event keyed by (TaskRef, Sequence). A
	// collision uses StoreConstraintEventSequence.
	InsertEvent(StoreEventRecord) error
	// InsertLeaseGrant inserts immutable history keyed by (TaskRef, Fence). A
	// collision uses StoreConstraintLeaseFence. It does not itself change the
	// Task's current lease; core pairs it with CompareAndSwapTask in this Update.
	InsertLeaseGrant(StoreLeaseGrantRecord) error
	// InsertArtifact inserts one immutable payload identity. Artifact IDs are
	// authority-wide unique and a Task owns at most one Artifact; collisions use
	// StoreConstraintArtifactID and StoreConstraintArtifactTask respectively.
	InsertArtifact(StoreArtifactRecord) error
	// CompareAndSwapArtifact changes only the state and terminal timestamps when
	// the exact ArtifactRef, bound TaskRef, and ExpectedState match. Absence or a
	// Task/state miss returns (false, nil); identity-invalid input is an error.
	CompareAndSwapArtifact(StoreArtifactMutation) (bool, error)
	// InsertSubmission inserts the immutable, sole Submission for its Task. Its
	// (Authority, Submission ID) is also unique. Any collision returns
	// ErrStoreConflict with StoreConstraintSubmissionID.
	InsertSubmission(StoreSubmissionRecord) error
	// InsertWorkspaceHead is first-writer-wins for Head.Workspace. It returns true
	// only when it inserted the row. An existing row returns (false, nil), remains
	// unchanged even when its value differs, and must be loaded before deciding the
	// next action.
	InsertWorkspaceHead(StoreWorkspaceHeadRecord) (bool, error)
	// CompareAndSwapWorkspaceHead replaces an existing head only when
	// ExpectedRevision matches. An absent head or revision miss returns (false,
	// nil); a Workspace/Next identity mismatch is an error.
	CompareAndSwapWorkspaceHead(StoreWorkspaceHeadMutation) (bool, error)
	// InsertApplyReceipt inserts the immutable, sole receipt for its Artifact.
	// (Authority, Receipt ID) is also unique. Any collision returns
	// ErrStoreConflict with StoreConstraintReceiptID.
	InsertApplyReceipt(StoreApplyReceiptRecord) error
}
