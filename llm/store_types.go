package llm

import (
	"fmt"
	"maps"
	"time"

	"github.com/vibe-agi/human/framework"
)

// StoreDigest is an opaque core-owned digest. A Store preserves it exactly and
// never recalculates it using adapter-specific serialization.
type StoreDigest string

type CallerID string
type TaskID string
type IdempotencyKey string
type WorkerID string

// WorkerLeaseID identifies one durable HumanLLM assignment generation. It is
// not a socket/session identity and must be checked at the same commit boundary
// as LeaseOwner before accepting a worker event.
type WorkerLeaseID string
type ToolCallID string

// CapabilityTier is the authority boundary selected before admission.
type CapabilityTier string

const (
	TierChat        CapabilityTier = "chat"
	TierRemoteTools CapabilityTier = "remote_tools"
	TierWorkspace   CapabilityTier = "workspace"
)

// TaskState is HumanLLM core's reachable durable task state. The implementation
// may atomically refine across conceptual protocol steps such as admitted or
// reconciled when no crash/retry boundary can observe them; those TLA+ states
// are intentionally not imposed on third-party Store implementations.
type TaskState string

const (
	TaskLeased          TaskState = "leased"
	TaskAwaitingHuman   TaskState = "awaiting_human"
	TaskAwaitingCaller  TaskState = "awaiting_caller"
	TaskAwaitingResults TaskState = "awaiting_results"
	TaskCompleted       TaskState = "completed"
	TaskRejected        TaskState = "rejected"
	TaskExpired         TaskState = "expired"
	TaskFailed          TaskState = "failed"
)

// Valid reports whether state is part of the persisted HumanLLM vocabulary.
func (state TaskState) Valid() bool {
	switch state {
	case TaskLeased, TaskAwaitingHuman, TaskAwaitingCaller,
		TaskAwaitingResults, TaskCompleted, TaskRejected, TaskExpired, TaskFailed:
		return true
	default:
		return false
	}
}

// Terminal reports whether no new turn may be admitted for this Task.
func (state TaskState) Terminal() bool {
	switch state {
	case TaskCompleted, TaskRejected, TaskExpired, TaskFailed:
		return true
	default:
		return false
	}
}

// CodecSnapshot is the exact negotiated codec identity persisted with every
// Task and Request. Contract is a frozen snapshot of the Go port contract;
// Version and Fingerprint pin wire behavior. A Store must preserve the feature
// map as an independently owned value.
type CodecSnapshot struct {
	Contract    framework.Contract
	ID          CodecID
	Version     string
	Fingerprint CodecFingerprint
}

// NewCodecSnapshot validates a complete CodecDescription and returns the
// immutable identity that persistence and recovery must use. Runtime byte
// limits and overload policy are intentionally not part of stored identity;
// byte-producing behavior is pinned by Version and Fingerprint.
func NewCodecSnapshot(description CodecDescription) (CodecSnapshot, error) {
	negotiated, err := NegotiateCodec(description)
	if err != nil {
		return CodecSnapshot{}, err
	}
	return CodecSnapshot{
		Contract:    negotiated.Contract,
		ID:          negotiated.ID,
		Version:     negotiated.Version,
		Fingerprint: negotiated.Fingerprint,
	}, nil
}

// Validate verifies a persisted snapshot without requiring current runtime
// limits. Recovery must additionally locate a registered Codec whose freshly
// negotiated snapshot is exactly equal to this value; matching ID alone is
// insufficient.
func (snapshot CodecSnapshot) Validate() error {
	contract, err := framework.Negotiate(snapshot.Contract, RequiredCodecContract())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCodecContract, err)
	}
	if !codecIDPattern.MatchString(string(snapshot.ID)) ||
		!validVersion(snapshot.Version) || !validFingerprint(snapshot.Fingerprint) {
		return fmt.Errorf("%w: invalid persisted codec identity", ErrInvalidCodecContract)
	}
	_ = contract
	return nil
}

// Equal reports semantic identity for recovery. Feature-map insertion order and
// nil-versus-empty maps do not change a framework contract. A runtime must use
// this full comparison, never ID or Version alone, before decoding persisted
// canonical payload or reconstructing response bytes.
func (snapshot CodecSnapshot) Equal(other CodecSnapshot) bool {
	return snapshot.ID == other.ID && snapshot.Version == other.Version &&
		snapshot.Fingerprint == other.Fingerprint &&
		snapshot.Contract.ID == other.Contract.ID &&
		snapshot.Contract.Major == other.Contract.Major &&
		snapshot.Contract.Minor == other.Contract.Minor &&
		maps.Equal(snapshot.Contract.Features, other.Contract.Features)
}

type StoreTaskKey struct {
	Caller CallerID
	Task   TaskID
}

// StoreTaskRecord is the durable multi-turn completion aggregate. Codec is
// pinned for the Task lifetime; every Request must carry an equal snapshot.
type StoreTaskRecord struct {
	Key              StoreTaskKey
	WorkspaceKey     string
	CapabilityTier   CapabilityTier
	Codec            CodecSnapshot
	HarnessID        string
	HarnessVersion   string
	HarnessSessionID string
	ExecAllowed      bool
	State            TaskState
	LeaseOwner       WorkerID
	LeaseID          WorkerLeaseID
	Revision         uint64
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// StoreTaskAffinity names one exact non-chat harness conversation. Every field
// is required. Chat completions deliberately have no stable affinity and must
// never be looked up or made unique through this type: each one owns a fresh
// Task even when another completion from the same Caller remains active.
type StoreTaskAffinity struct {
	Caller           CallerID
	WorkspaceKey     string
	HarnessID        string
	HarnessVersion   string
	HarnessSessionID string
}

type StoreRequestKey struct {
	Caller         CallerID
	IdempotencyKey IdempotencyKey
}

// ResponseMode is immutable request metadata needed before canonical payload
// decoding and after retention has replaced that payload with a tombstone.
type ResponseMode string

const (
	ResponseStream    ResponseMode = "stream"
	ResponseAggregate ResponseMode = "aggregate"
)

// StoreResponseDecision is the immutable HTTP boundary for one request. A zero
// StatusCode means no response is committed. Streaming normally commits a 200
// with no Body before dispatch; aggregate mode commits status and Body together
// at the terminal event. Body is the core-owned opaque at-rest representation
// of exact wire bytes (plain or protected StoredValue framing); Store preserves
// it without interpreting or encrypting it.
type StoreResponseDecision struct {
	StatusCode  int
	ContentType string
	RetryAfter  string
	Body        []byte
}

// StoreRequestRecord owns the idempotency tombstone and exact replay material.
// RequestDigest binds the canonical plaintext represented by CanonicalPayload
// to Codec, including its full contract snapshot. CanonicalPayload itself is a
// core-owned opaque at-rest representation; Store preserves but never opens,
// encrypts, or computes a digest from it. PayloadPrunedAt
// is immutable once set, and then CanonicalPayload and Decision.Body are empty
// while identity, status metadata, receipts, and the digest remain durable.
type StoreRequestRecord struct {
	Key  StoreRequestKey
	Task StoreTaskKey
	// RequestID is the stable assignment identity. ResponseID is the exact
	// codec encoder identity. Both are allocated before dispatch and remain
	// immutable so crash recovery can rebuild the same worker assignment and
	// byte-identical response stream.
	RequestID           string
	ResponseID          string
	RequestDigest       StoreDigest
	Codec               CodecSnapshot
	Mode                ResponseMode
	CanonicalPayload    []byte
	Decision            StoreResponseDecision
	ResponseComplete    bool
	RecoveryQuarantined bool
	LastEventSequence   uint64
	Revision            uint64
	CreatedAt           time.Time
	CompletedAt         *time.Time
	PayloadPrunedAt     *time.Time
}

// StoreRequestHead is a payload-free projection for hot-path state checks,
// recovery cursors, and retention scans. Decision omits Body by construction.
type StoreRequestHead struct {
	Key                 StoreRequestKey
	Task                StoreTaskKey
	RequestID           string
	ResponseID          string
	RequestDigest       StoreDigest
	Codec               CodecSnapshot
	Mode                ResponseMode
	DecisionStatus      int
	DecisionContentType string
	DecisionRetryAfter  string
	ResponseComplete    bool
	RecoveryQuarantined bool
	LastEventSequence   uint64
	Revision            uint64
	CreatedAt           time.Time
	CompletedAt         *time.Time
	PayloadPrunedAt     *time.Time
}

// StoreResponseEventKind identifies append-only exact-replay material. A
// checkpoint represents core-owned deterministic encoder session metadata;
// wire represents exact caller-visible bytes. Data is a core-owned opaque
// at-rest representation, so Store adapters never call a Protector.
// Worker/WorkerEventID/WorkerEventDigest bind every frame produced for one
// authenticated worker event to its durable ACK receipt. All three are empty
// for session/start/quarantine material and all three are non-empty together
// for worker-produced checkpoints and wire frames.
type StoreResponseEventKind string

const (
	StoreEventCheckpoint StoreResponseEventKind = "checkpoint"
	StoreEventWire       StoreResponseEventKind = "wire"
)

type StoreResponseEventRecord struct {
	Request           StoreRequestKey
	Sequence          uint64
	Kind              StoreResponseEventKind
	Worker            WorkerID
	WorkerEventID     string
	WorkerEventDigest StoreDigest
	Data              []byte
	CreatedAt         time.Time
}

// StoreWorkerReceiptRecord is the durable ACK oracle. It may be inserted only
// in the same Update as all Task, tool-ledger, response-wire, and terminal
// effects of that worker event. Therefore observing a matching receipt proves
// those effects are durable after a crash.
type StoreWorkerReceiptRecord struct {
	Request   StoreRequestKey
	EventID   string
	Worker    WorkerID
	Digest    StoreDigest
	CreatedAt time.Time
}

type StoreToolExecutionKey struct {
	Task       StoreTaskKey
	ToolCallID ToolCallID
}

type ToolExecutionState string

const (
	ToolExecutionPending   ToolExecutionState = "pending"
	ToolExecutionCompleted ToolExecutionState = "completed"
)

// StoreToolExecutionRecord is the task-wide at-most-once ledger. InputDigest
// binds a ToolCallID to one canonical call. Result is the core-owned opaque
// at-rest representation of canonical caller-reported JSON. ResultPrunedAt distinguishes retention
// from a legitimate empty result.
type StoreToolExecutionRecord struct {
	Key            StoreToolExecutionKey
	InputDigest    StoreDigest
	State          ToolExecutionState
	Result         []byte
	IsError        bool
	Revision       uint64
	CreatedAt      time.Time
	CompletedAt    *time.Time
	ResultPrunedAt *time.Time
}

// StoreReadLimit is a hard aggregate byte budget for one method call. MaxBytes
// must be positive and counts every returned CanonicalPayload, response Body,
// response-event Data, and tool Result byte. It is never interpreted as
// unlimited. A single-record method fails with ErrStoreRecordTooLarge. A scan
// does the same when its first record cannot fit; after at least one record fits
// it returns the largest ordered contiguous prefix within the budget.
type StoreReadLimit struct {
	MaxBytes int64
}

// StoreResponseEventScan returns events in Sequence order strictly after After.
// Kinds empty means all kinds. WorkerEventID empty means all worker events.
type StoreResponseEventScan struct {
	Request       StoreRequestKey
	After         uint64
	Kinds         []StoreResponseEventKind
	WorkerEventID string
	Limit         int
	ReadLimit     StoreReadLimit
}

// StoreToolExecutionScan returns ToolCallID-ascending records strictly after
// After. State empty means both states.
type StoreToolExecutionScan struct {
	Task      StoreTaskKey
	State     ToolExecutionState
	After     ToolCallID
	Limit     int
	ReadLimit StoreReadLimit
}

// StoreRecoveryCursor orders candidates by CreatedAt, Caller, and idempotency
// key, all ascending.
type StoreRecoveryCursor struct {
	CreatedAt      time.Time
	Caller         CallerID
	IdempotencyKey IdempotencyKey
}

// StoreRecoveryScan selects every non-quarantined request whose response is
// incomplete, plus any request with a worker-associated response event lacking
// its matching receipt. The latter clause preserves recovery from adapters
// migrated from the old staged protocol; a conforming new core normally commits
// event effects and receipt atomically. Results contain Task and full Request
// from the same View snapshot in cursor order.
type StoreRecoveryScan struct {
	After     *StoreRecoveryCursor
	Limit     int
	ReadLimit StoreReadLimit
}

type StoreRecoveryRecord struct {
	Task    StoreTaskRecord
	Request StoreRequestRecord
}

// StoreRetentionCursor orders candidates by effective completion time, Caller,
// and idempotency key, all ascending.
type StoreRetentionCursor struct {
	CompletedAt    time.Time
	Caller         CallerID
	IdempotencyKey IdempotencyKey
}

// StoreRetentionScan selects complete, non-tombstoned requests whose effective
// completion time (CompletedAt, otherwise CreatedAt for malformed legacy data)
// is at or before CompletedBefore. The core must not tombstone an ordinary
// candidate with UnacknowledgedWorkerEvent=true. A RecoveryQuarantined request
// is already a durable terminal adjudication: stores report its marker as false
// so corrupt/orphan worker history cannot retain private payload forever.
type StoreRetentionScan struct {
	CompletedBefore time.Time
	After           *StoreRetentionCursor
	Limit           int
}

type StoreRetentionCandidate struct {
	Request                   StoreRequestHead
	EffectiveCompletedAt      time.Time
	UnacknowledgedWorkerEvent bool
}

// StoreView is a transaction-bound logical snapshot. It exposes no SQL,
// collection, driver cursor, or transport type. Absence matches
// ErrStoreRecordNotFound; every returned mutable value belongs to the caller.
type StoreView interface {
	LoadTask(StoreTaskKey) (StoreTaskRecord, error)
	// FindOpenTask resolves only TierRemoteTools/TierWorkspace Tasks. An empty or
	// partial affinity returns ErrStoreInvalidArgument; absence returns
	// ErrStoreRecordNotFound.
	FindOpenTask(StoreTaskAffinity) (StoreTaskRecord, error)
	FindActiveRequest(StoreTaskKey) (StoreRequestHead, error)
	LoadRequestHead(StoreRequestKey) (StoreRequestHead, error)
	LoadRequest(StoreRequestKey, StoreReadLimit) (StoreRequestRecord, error)
	LoadResponseDecision(StoreRequestKey, StoreReadLimit) (StoreResponseDecision, error)
	LoadWorkerReceipt(StoreRequestKey, string) (StoreWorkerReceiptRecord, error)
	LoadToolExecution(StoreToolExecutionKey, StoreReadLimit) (StoreToolExecutionRecord, error)
	ScanResponseEvents(StoreResponseEventScan) ([]StoreResponseEventRecord, error)
	ScanToolExecutions(StoreToolExecutionScan) ([]StoreToolExecutionRecord, error)
	ScanRecovery(StoreRecoveryScan) ([]StoreRecoveryRecord, error)
	ScanRetention(StoreRetentionScan) ([]StoreRetentionCandidate, error)
}

// StoreTaskMutation replaces one Task when ExpectedRevision still matches.
// Key and every immutable identity field in Next must equal the stored record.
// A false result means a CAS miss and changes nothing. Store does not decide
// whether Next.State is a legal HumanLLM transition.
type StoreTaskMutation struct {
	Key              StoreTaskKey
	ExpectedRevision uint64
	Next             StoreTaskRecord
}

// StoreRequestMutation replaces mutable response/recovery/retention state when
// ExpectedRevision matches. Key, Task, RequestID, ResponseID, RequestDigest,
// Codec, Mode, CreatedAt, and the pre-tombstone CanonicalPayload identity are
// immutable. Core must increment Revision exactly once for each successful
// replacement.
type StoreRequestMutation struct {
	Key              StoreRequestKey
	ExpectedRevision uint64
	Next             StoreRequestRecord
}

// StoreToolExecutionMutation completes or retention-prunes one ledger row by
// revision CAS. Key, InputDigest, and CreatedAt are immutable.
type StoreToolExecutionMutation struct {
	Key              StoreToolExecutionKey
	ExpectedRevision uint64
	Next             StoreToolExecutionRecord
}

// StoreTx extends one StoreView with typed, non-committing primitives. Immutable
// inserts return ErrStoreConflict on any collision, including an exact replay;
// HumanLLM core loads and compares the existing record to decide idempotency.
// CAS methods return (false, nil) on an expected-revision miss.
//
// DeleteTombstonedResponseEvents may delete only events owned by a Request whose
// PayloadPrunedAt is non-nil in this same transaction. It leaves worker receipts
// intact so delayed durable-outbox duplicates remain ACKable. It returns the
// number of deleted records. All retention decisions remain in core.
type StoreTx interface {
	StoreView
	InsertTask(StoreTaskRecord) error
	CompareAndSwapTask(StoreTaskMutation) (bool, error)
	InsertRequest(StoreRequestRecord) error
	CompareAndSwapRequest(StoreRequestMutation) (bool, error)
	InsertResponseEvent(StoreResponseEventRecord) error
	InsertWorkerReceipt(StoreWorkerReceiptRecord) error
	InsertToolExecution(StoreToolExecutionRecord) error
	CompareAndSwapToolExecution(StoreToolExecutionMutation) (bool, error)
	DeleteTombstonedResponseEvents(StoreRequestKey) (uint64, error)
}
