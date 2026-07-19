package humantest

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

// MemoryLLMStore is an in-memory semantic model of [llm.Store] intended only
// for tests, examples, and fault-injection harnesses. It is concurrency-safe
// and preserves the production Store contract, including serializable atomic
// updates, callback lifetimes, exact byte ownership, and read budgets. It does
// not provide process durability.
//
// Construct it with [NewMemoryLLMStore] so ownership and release remain
// explicit. Store has no Close method by design.
type MemoryLLMStore struct {
	mu      sync.RWMutex
	closed  bool
	binding *llm.StoreBinding
	state   memoryLLMState
}

type memoryLLMReceiptKey struct {
	request llm.StoreRequestKey
	eventID string
}

type memoryLLMState struct {
	tasks    map[llm.StoreTaskKey]llm.StoreTaskRecord
	requests map[llm.StoreRequestKey]llm.StoreRequestRecord
	events   map[llm.StoreRequestKey]map[uint64]llm.StoreResponseEventRecord
	receipts map[memoryLLMReceiptKey]llm.StoreWorkerReceiptRecord
	tools    map[llm.StoreToolExecutionKey]llm.StoreToolExecutionRecord
}

// NewMemoryLLMStore creates an independent, empty test Store and an idempotent
// release function. After release, View and Update return [llm.ErrStoreClosed].
func NewMemoryLLMStore() (*MemoryLLMStore, framework.ReleaseFunc) {
	store := &MemoryLLMStore{state: newMemoryLLMState()}
	var once sync.Once
	release := func(context.Context) error {
		once.Do(store.close)
		return nil
	}
	return store, release
}

func newMemoryLLMState() memoryLLMState {
	return memoryLLMState{
		tasks:    make(map[llm.StoreTaskKey]llm.StoreTaskRecord),
		requests: make(map[llm.StoreRequestKey]llm.StoreRequestRecord),
		events:   make(map[llm.StoreRequestKey]map[uint64]llm.StoreResponseEventRecord),
		receipts: make(map[memoryLLMReceiptKey]llm.StoreWorkerReceiptRecord),
		tools:    make(map[llm.StoreToolExecutionKey]llm.StoreToolExecutionRecord),
	}
}

func (*MemoryLLMStore) Description() llm.StoreDescription {
	return llm.StoreDescription{
		Contract: framework.Contract{ID: llm.StoreContractID, Major: llm.StoreContractMajor},
		Provider: "humantest-memory-model", Version: "1",
	}
}

func (store *MemoryLLMStore) Bind(ctx context.Context, binding llm.StoreBinding) error {
	if ctx == nil {
		return llm.ErrStoreInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return llm.ErrStoreClosed
	}
	if store.binding == nil {
		stored := binding
		store.binding = &stored
		return nil
	}
	if *store.binding != binding {
		return &llm.StoreConflictError{
			Constraint: llm.StoreConstraintDeploymentBinding, Key: binding.DeploymentID,
		}
	}
	return nil
}

func (store *MemoryLLMStore) View(ctx context.Context, callback func(llm.StoreView) error) error {
	if ctx == nil || callback == nil {
		return llm.ErrStoreInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return llm.ErrStoreClosed
	}
	unit := &memoryLLMUnit{state: &store.state}
	unit.active.Store(true)
	defer unit.active.Store(false)
	return callback(memoryLLMView{unit: unit})
}

func (store *MemoryLLMStore) Update(ctx context.Context, callback func(llm.StoreTx) error) error {
	if ctx == nil || callback == nil {
		return llm.ErrStoreInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return llm.ErrStoreClosed
	}
	next := store.state.clone()
	unit := &memoryLLMUnit{state: &next}
	unit.active.Store(true)
	err := callback(memoryLLMTx{memoryLLMView{unit: unit}})
	unit.active.Store(false)
	if err != nil {
		return err
	}
	store.state = next
	return nil
}

func (store *MemoryLLMStore) close() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
}

func (state memoryLLMState) clone() memoryLLMState {
	cloned := newMemoryLLMState()
	for key, record := range state.tasks {
		cloned.tasks[key] = cloneMemoryLLMTask(record)
	}
	for key, record := range state.requests {
		cloned.requests[key] = cloneMemoryLLMRequest(record)
	}
	for key, records := range state.events {
		cloned.events[key] = make(map[uint64]llm.StoreResponseEventRecord, len(records))
		for sequence, record := range records {
			cloned.events[key][sequence] = cloneMemoryLLMEvent(record)
		}
	}
	for key, record := range state.receipts {
		cloned.receipts[key] = record
	}
	for key, record := range state.tools {
		cloned.tools[key] = cloneMemoryLLMTool(record)
	}
	return cloned
}

type memoryLLMUnit struct {
	state  *memoryLLMState
	active atomic.Bool
}

func (unit *memoryLLMUnit) ensureActive() error {
	if unit == nil || unit.state == nil || !unit.active.Load() {
		return llm.ErrStoreClosed
	}
	return nil
}

type memoryLLMView struct{ unit *memoryLLMUnit }
type memoryLLMTx struct{ memoryLLMView }

var _ llm.Store = (*MemoryLLMStore)(nil)
var _ llm.StoreView = memoryLLMView{}
var _ llm.StoreTx = memoryLLMTx{}

func (view memoryLLMView) LoadTask(key llm.StoreTaskKey) (llm.StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreTaskRecord{}, err
	}
	record, ok := view.unit.state.tasks[key]
	if !ok {
		return llm.StoreTaskRecord{}, memoryLLMNotFound(llm.StoreRecordTask, key)
	}
	return cloneMemoryLLMTask(record), nil
}

func (view memoryLLMView) FindOpenTask(affinity llm.StoreTaskAffinity) (llm.StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreTaskRecord{}, err
	}
	if affinity.Caller == "" || affinity.WorkspaceKey == "" || affinity.HarnessID == "" ||
		affinity.HarnessVersion == "" || affinity.HarnessSessionID == "" {
		return llm.StoreTaskRecord{}, llm.ErrStoreInvalidArgument
	}
	for _, record := range view.unit.state.tasks {
		if !record.State.Terminal() &&
			(record.CapabilityTier == llm.TierRemoteTools || record.CapabilityTier == llm.TierWorkspace) &&
			memoryLLMAffinity(record) == affinity {
			return cloneMemoryLLMTask(record), nil
		}
	}
	return llm.StoreTaskRecord{}, memoryLLMNotFound(llm.StoreRecordTask, affinity)
}

func (view memoryLLMView) FindActiveRequest(task llm.StoreTaskKey) (llm.StoreRequestHead, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreRequestHead{}, err
	}
	for _, record := range view.unit.state.requests {
		if record.Task == task && !record.ResponseComplete {
			return memoryLLMRequestHead(record), nil
		}
	}
	return llm.StoreRequestHead{}, memoryLLMNotFound(llm.StoreRecordRequest, task)
}

func (view memoryLLMView) LoadRequestHead(key llm.StoreRequestKey) (llm.StoreRequestHead, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreRequestHead{}, err
	}
	record, ok := view.unit.state.requests[key]
	if !ok {
		return llm.StoreRequestHead{}, memoryLLMNotFound(llm.StoreRecordRequest, key)
	}
	return memoryLLMRequestHead(record), nil
}

func (view memoryLLMView) LoadRequest(key llm.StoreRequestKey, limit llm.StoreReadLimit) (llm.StoreRequestRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreRequestRecord{}, err
	}
	record, ok := view.unit.state.requests[key]
	if !ok {
		return llm.StoreRequestRecord{}, memoryLLMNotFound(llm.StoreRecordRequest, key)
	}
	if err := memoryLLMBudget(llm.StoreRecordRequest, int64(len(record.CanonicalPayload)+len(record.Decision.Body)), limit); err != nil {
		return llm.StoreRequestRecord{}, err
	}
	return cloneMemoryLLMRequest(record), nil
}

func (view memoryLLMView) LoadResponseDecision(key llm.StoreRequestKey, limit llm.StoreReadLimit) (llm.StoreResponseDecision, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreResponseDecision{}, err
	}
	record, ok := view.unit.state.requests[key]
	if !ok {
		return llm.StoreResponseDecision{}, memoryLLMNotFound(llm.StoreRecordRequest, key)
	}
	if err := memoryLLMBudget(llm.StoreRecordRequest, int64(len(record.Decision.Body)), limit); err != nil {
		return llm.StoreResponseDecision{}, err
	}
	return cloneMemoryLLMDecision(record.Decision), nil
}

func (view memoryLLMView) LoadWorkerReceipt(request llm.StoreRequestKey, eventID string) (llm.StoreWorkerReceiptRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreWorkerReceiptRecord{}, err
	}
	record, ok := view.unit.state.receipts[memoryLLMReceiptKey{request: request, eventID: eventID}]
	if !ok {
		return llm.StoreWorkerReceiptRecord{}, memoryLLMNotFound(llm.StoreRecordWorkerReceipt, eventID)
	}
	return record, nil
}

func (view memoryLLMView) LoadToolExecution(key llm.StoreToolExecutionKey, limit llm.StoreReadLimit) (llm.StoreToolExecutionRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreToolExecutionRecord{}, err
	}
	record, ok := view.unit.state.tools[key]
	if !ok {
		return llm.StoreToolExecutionRecord{}, memoryLLMNotFound(llm.StoreRecordToolExecution, key)
	}
	if err := memoryLLMBudget(llm.StoreRecordToolExecution, int64(len(record.Result)), limit); err != nil {
		return llm.StoreToolExecutionRecord{}, err
	}
	return cloneMemoryLLMTool(record), nil
}

func (view memoryLLMView) ScanResponseEvents(scan llm.StoreResponseEventScan) ([]llm.StoreResponseEventRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := memoryLLMScanArguments(scan.Limit, scan.ReadLimit); err != nil {
		return nil, err
	}
	kinds := make(map[llm.StoreResponseEventKind]bool, len(scan.Kinds))
	for _, kind := range scan.Kinds {
		kinds[kind] = true
	}
	sequences := make([]uint64, 0, len(view.unit.state.events[scan.Request]))
	for sequence, record := range view.unit.state.events[scan.Request] {
		if sequence <= scan.After || len(kinds) > 0 && !kinds[record.Kind] ||
			scan.WorkerEventID != "" && record.WorkerEventID != scan.WorkerEventID {
			continue
		}
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	result := make([]llm.StoreResponseEventRecord, 0, min(scan.Limit, len(sequences)))
	var used int64
	for _, sequence := range sequences {
		if len(result) == scan.Limit {
			break
		}
		record := view.unit.state.events[scan.Request][sequence]
		size := int64(len(record.Data))
		if size > scan.ReadLimit.MaxBytes {
			if len(result) == 0 {
				return nil, memoryLLMLimit(llm.StoreRecordResponseEvent, scan.ReadLimit.MaxBytes)
			}
			break
		}
		if used+size > scan.ReadLimit.MaxBytes {
			break
		}
		result = append(result, cloneMemoryLLMEvent(record))
		used += size
	}
	return result, nil
}

func (view memoryLLMView) ScanToolExecutions(scan llm.StoreToolExecutionScan) ([]llm.StoreToolExecutionRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := memoryLLMScanArguments(scan.Limit, scan.ReadLimit); err != nil {
		return nil, err
	}
	ids := make([]llm.ToolCallID, 0)
	for key, record := range view.unit.state.tools {
		if key.Task != scan.Task || key.ToolCallID <= scan.After || scan.State != "" && record.State != scan.State {
			continue
		}
		ids = append(ids, key.ToolCallID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]llm.StoreToolExecutionRecord, 0, min(scan.Limit, len(ids)))
	var used int64
	for _, id := range ids {
		if len(result) == scan.Limit {
			break
		}
		record := view.unit.state.tools[llm.StoreToolExecutionKey{Task: scan.Task, ToolCallID: id}]
		size := int64(len(record.Result))
		if size > scan.ReadLimit.MaxBytes {
			if len(result) == 0 {
				return nil, memoryLLMLimit(llm.StoreRecordToolExecution, scan.ReadLimit.MaxBytes)
			}
			break
		}
		if used+size > scan.ReadLimit.MaxBytes {
			break
		}
		result = append(result, cloneMemoryLLMTool(record))
		used += size
	}
	return result, nil
}

func (view memoryLLMView) ScanRecovery(scan llm.StoreRecoveryScan) ([]llm.StoreRecoveryRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := memoryLLMScanArguments(scan.Limit, scan.ReadLimit); err != nil {
		return nil, err
	}
	candidates := make([]llm.StoreRecoveryRecord, 0)
	for _, request := range view.unit.state.requests {
		// Quarantine is itself the durable recovery decision. Never resurrect it
		// merely because the corrupt history which caused quarantine still has no
		// matching receipt.
		if request.RecoveryQuarantined || request.ResponseComplete && !view.unacknowledged(request.Key) {
			continue
		}
		if scan.After != nil && !memoryLLMRecoveryAfter(request, *scan.After) {
			continue
		}
		task, ok := view.unit.state.tasks[request.Task]
		if !ok {
			continue
		}
		candidates = append(candidates, llm.StoreRecoveryRecord{
			Task: cloneMemoryLLMTask(task), Request: cloneMemoryLLMRequest(request),
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return memoryLLMRecoveryLess(candidates[i].Request, candidates[j].Request)
	})
	result := make([]llm.StoreRecoveryRecord, 0, min(scan.Limit, len(candidates)))
	var used int64
	for _, record := range candidates {
		if len(result) == scan.Limit {
			break
		}
		size := int64(len(record.Request.CanonicalPayload) + len(record.Request.Decision.Body))
		if size > scan.ReadLimit.MaxBytes {
			if len(result) == 0 {
				return nil, memoryLLMLimit(llm.StoreRecordRequest, scan.ReadLimit.MaxBytes)
			}
			break
		}
		if used+size > scan.ReadLimit.MaxBytes {
			break
		}
		result = append(result, record)
		used += size
	}
	return result, nil
}

func (view memoryLLMView) ScanRetention(scan llm.StoreRetentionScan) ([]llm.StoreRetentionCandidate, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if scan.CompletedBefore.IsZero() || scan.Limit < 1 {
		return nil, llm.ErrStoreInvalidArgument
	}
	result := make([]llm.StoreRetentionCandidate, 0)
	for _, request := range view.unit.state.requests {
		if !request.ResponseComplete || request.PayloadPrunedAt != nil {
			continue
		}
		effective := request.CreatedAt
		if request.CompletedAt != nil {
			effective = *request.CompletedAt
		}
		if effective.After(scan.CompletedBefore) || scan.After != nil && !memoryLLMRetentionAfter(request.Key, effective, *scan.After) {
			continue
		}
		result = append(result, llm.StoreRetentionCandidate{
			Request: memoryLLMRequestHead(request), EffectiveCompletedAt: effective,
			UnacknowledgedWorkerEvent: !request.RecoveryQuarantined && view.unacknowledged(request.Key),
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return memoryLLMRetentionLess(result[i], result[j])
	})
	if len(result) > scan.Limit {
		result = result[:scan.Limit]
	}
	return result, nil
}

func (view memoryLLMView) unacknowledged(request llm.StoreRequestKey) bool {
	for _, event := range view.unit.state.events[request] {
		if event.WorkerEventID == "" {
			continue
		}
		receipt, ok := view.unit.state.receipts[memoryLLMReceiptKey{request: request, eventID: event.WorkerEventID}]
		if !ok || receipt.Worker != event.Worker || receipt.Digest != event.WorkerEventDigest {
			return true
		}
	}
	return false
}

func (tx memoryLLMTx) InsertTask(record llm.StoreTaskRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if _, exists := tx.unit.state.tasks[record.Key]; exists {
		return memoryLLMConflict(llm.StoreConstraintTaskKey, record.Key)
	}
	if !record.State.Terminal() &&
		(record.CapabilityTier == llm.TierRemoteTools || record.CapabilityTier == llm.TierWorkspace) {
		for _, existing := range tx.unit.state.tasks {
			if !existing.State.Terminal() && memoryLLMAffinity(existing) == memoryLLMAffinity(record) {
				return memoryLLMConflict(llm.StoreConstraintOpenAffinity, record.Key)
			}
		}
	}
	tx.unit.state.tasks[record.Key] = cloneMemoryLLMTask(record)
	return nil
}

func (tx memoryLLMTx) CompareAndSwapTask(mutation llm.StoreTaskMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	current, exists := tx.unit.state.tasks[mutation.Key]
	if !exists {
		return false, memoryLLMNotFound(llm.StoreRecordTask, mutation.Key)
	}
	if current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if !memoryLLMTaskIdentityEqual(current, mutation.Next) || mutation.Next.Revision != current.Revision+1 {
		return false, memoryLLMConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	tx.unit.state.tasks[mutation.Key] = cloneMemoryLLMTask(mutation.Next)
	return true, nil
}

func (tx memoryLLMTx) InsertRequest(record llm.StoreRequestRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if _, exists := tx.unit.state.requests[record.Key]; exists {
		return memoryLLMConflict(llm.StoreConstraintRequestKey, record.Key)
	}
	task, exists := tx.unit.state.tasks[record.Task]
	if !exists {
		return memoryLLMNotFound(llm.StoreRecordTask, record.Task)
	}
	if !task.Codec.Equal(record.Codec) {
		return memoryLLMConflict(llm.StoreConstraintImmutableRecord, record.Key)
	}
	for _, existing := range tx.unit.state.requests {
		if existing.Task == record.Task && !existing.ResponseComplete {
			return memoryLLMConflict(llm.StoreConstraintActiveRequest, record.Task)
		}
		if existing.Key.Caller == record.Key.Caller &&
			(existing.RequestID == record.RequestID || existing.ResponseID == record.ResponseID) {
			return memoryLLMConflict(llm.StoreConstraintRequestKey, record.Key)
		}
	}
	tx.unit.state.requests[record.Key] = cloneMemoryLLMRequest(record)
	return nil
}

func (tx memoryLLMTx) CompareAndSwapRequest(mutation llm.StoreRequestMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	current, exists := tx.unit.state.requests[mutation.Key]
	if !exists {
		return false, memoryLLMNotFound(llm.StoreRecordRequest, mutation.Key)
	}
	if current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if !memoryLLMRequestIdentityEqual(current, mutation.Next) || mutation.Next.Revision != current.Revision+1 {
		return false, memoryLLMConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	if current.PayloadPrunedAt == nil {
		if mutation.Next.PayloadPrunedAt == nil && !bytes.Equal(current.CanonicalPayload, mutation.Next.CanonicalPayload) {
			return false, memoryLLMConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
		}
		if mutation.Next.PayloadPrunedAt != nil && (len(mutation.Next.CanonicalPayload) != 0 || len(mutation.Next.Decision.Body) != 0) {
			return false, memoryLLMConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
		}
	} else if mutation.Next.PayloadPrunedAt == nil || !mutation.Next.PayloadPrunedAt.Equal(*current.PayloadPrunedAt) ||
		len(mutation.Next.CanonicalPayload) != 0 || len(mutation.Next.Decision.Body) != 0 {
		return false, memoryLLMConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	tx.unit.state.requests[mutation.Key] = cloneMemoryLLMRequest(mutation.Next)
	return true, nil
}

func (tx memoryLLMTx) InsertResponseEvent(record llm.StoreResponseEventRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if _, exists := tx.unit.state.requests[record.Request]; !exists {
		return memoryLLMNotFound(llm.StoreRecordRequest, record.Request)
	}
	if (record.Worker == "") != (record.WorkerEventID == "") ||
		(record.WorkerEventID == "") != (record.WorkerEventDigest == "") {
		return llm.ErrStoreInvalidArgument
	}
	if tx.unit.state.events[record.Request] == nil {
		tx.unit.state.events[record.Request] = make(map[uint64]llm.StoreResponseEventRecord)
	}
	if _, exists := tx.unit.state.events[record.Request][record.Sequence]; exists {
		return memoryLLMConflict(llm.StoreConstraintResponseSequence, record.Sequence)
	}
	if record.WorkerEventID != "" {
		for _, existing := range tx.unit.state.events[record.Request] {
			if existing.WorkerEventID == record.WorkerEventID &&
				(existing.Worker != record.Worker || existing.WorkerEventDigest != record.WorkerEventDigest) {
				return memoryLLMConflict(llm.StoreConstraintWorkerEvent, record.WorkerEventID)
			}
		}
	}
	tx.unit.state.events[record.Request][record.Sequence] = cloneMemoryLLMEvent(record)
	return nil
}

func (tx memoryLLMTx) InsertWorkerReceipt(record llm.StoreWorkerReceiptRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if _, exists := tx.unit.state.requests[record.Request]; !exists {
		return memoryLLMNotFound(llm.StoreRecordRequest, record.Request)
	}
	key := memoryLLMReceiptKey{request: record.Request, eventID: record.EventID}
	if _, exists := tx.unit.state.receipts[key]; exists {
		return memoryLLMConflict(llm.StoreConstraintWorkerReceipt, record.EventID)
	}
	tx.unit.state.receipts[key] = record
	return nil
}

func (tx memoryLLMTx) InsertToolExecution(record llm.StoreToolExecutionRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if _, exists := tx.unit.state.tasks[record.Key.Task]; !exists {
		return memoryLLMNotFound(llm.StoreRecordTask, record.Key.Task)
	}
	if _, exists := tx.unit.state.tools[record.Key]; exists {
		return memoryLLMConflict(llm.StoreConstraintToolCall, record.Key)
	}
	tx.unit.state.tools[record.Key] = cloneMemoryLLMTool(record)
	return nil
}

func (tx memoryLLMTx) CompareAndSwapToolExecution(mutation llm.StoreToolExecutionMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	current, exists := tx.unit.state.tools[mutation.Key]
	if !exists {
		return false, memoryLLMNotFound(llm.StoreRecordToolExecution, mutation.Key)
	}
	if current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if current.Key != mutation.Next.Key || current.InputDigest != mutation.Next.InputDigest ||
		!current.CreatedAt.Equal(mutation.Next.CreatedAt) || mutation.Next.Revision != current.Revision+1 {
		return false, memoryLLMConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	tx.unit.state.tools[mutation.Key] = cloneMemoryLLMTool(mutation.Next)
	return true, nil
}

func (tx memoryLLMTx) DeleteTombstonedResponseEvents(key llm.StoreRequestKey) (uint64, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return 0, err
	}
	request, exists := tx.unit.state.requests[key]
	if !exists {
		return 0, memoryLLMNotFound(llm.StoreRecordRequest, key)
	}
	if request.PayloadPrunedAt == nil {
		return 0, memoryLLMConflict(llm.StoreConstraintCompareAndSwap, key)
	}
	count := uint64(len(tx.unit.state.events[key]))
	delete(tx.unit.state.events, key)
	return count, nil
}

func memoryLLMNotFound(kind llm.StoreRecordKind, key any) error {
	return &llm.StoreNotFoundError{Record: kind, Key: fmt.Sprint(key)}
}

func memoryLLMConflict(constraint llm.StoreConstraint, key any) error {
	return &llm.StoreConflictError{Constraint: constraint, Key: fmt.Sprint(key)}
}

func memoryLLMLimit(kind llm.StoreRecordKind, limit int64) error {
	return &llm.StoreLimitError{Record: kind, Limit: limit}
}

func memoryLLMBudget(kind llm.StoreRecordKind, size int64, limit llm.StoreReadLimit) error {
	if limit.MaxBytes < 1 || size > limit.MaxBytes {
		return memoryLLMLimit(kind, limit.MaxBytes)
	}
	return nil
}

func memoryLLMScanArguments(limit int, readLimit llm.StoreReadLimit) error {
	if limit < 1 || limit > 4096 {
		return llm.ErrStoreInvalidArgument
	}
	if readLimit.MaxBytes < 1 {
		return memoryLLMLimit(llm.StoreRecordRequest, readLimit.MaxBytes)
	}
	return nil
}

func memoryLLMAffinity(record llm.StoreTaskRecord) llm.StoreTaskAffinity {
	return llm.StoreTaskAffinity{
		Caller: record.Key.Caller, WorkspaceKey: record.WorkspaceKey,
		HarnessID: record.HarnessID, HarnessVersion: record.HarnessVersion,
		HarnessSessionID: record.HarnessSessionID,
	}
}

func memoryLLMTaskIdentityEqual(left, right llm.StoreTaskRecord) bool {
	return left.Key == right.Key && left.WorkspaceKey == right.WorkspaceKey &&
		left.CapabilityTier == right.CapabilityTier && left.Codec.Equal(right.Codec) &&
		left.HarnessID == right.HarnessID && left.HarnessVersion == right.HarnessVersion &&
		left.HarnessSessionID == right.HarnessSessionID && left.WorkspaceRoot == right.WorkspaceRoot &&
		left.ExecAllowed == right.ExecAllowed && left.CreatedAt.Equal(right.CreatedAt)
}

func memoryLLMRequestIdentityEqual(left, right llm.StoreRequestRecord) bool {
	return left.Key == right.Key && left.Task == right.Task && left.RequestID == right.RequestID &&
		left.ResponseID == right.ResponseID && left.RequestDigest == right.RequestDigest &&
		left.Codec.Equal(right.Codec) && left.Mode == right.Mode && left.CreatedAt.Equal(right.CreatedAt)
}

func memoryLLMRequestHead(record llm.StoreRequestRecord) llm.StoreRequestHead {
	return llm.StoreRequestHead{
		Key: record.Key, Task: record.Task, RequestID: record.RequestID, ResponseID: record.ResponseID,
		RequestDigest: record.RequestDigest, Codec: cloneMemoryLLMCodec(record.Codec), Mode: record.Mode,
		DecisionStatus: record.Decision.StatusCode, DecisionContentType: record.Decision.ContentType,
		DecisionRetryAfter: record.Decision.RetryAfter, ResponseComplete: record.ResponseComplete,
		RecoveryQuarantined: record.RecoveryQuarantined, LastEventSequence: record.LastEventSequence,
		Revision: record.Revision, CreatedAt: record.CreatedAt,
		CompletedAt: cloneMemoryLLMTime(record.CompletedAt), PayloadPrunedAt: cloneMemoryLLMTime(record.PayloadPrunedAt),
	}
}

func memoryLLMRecoveryLess(left, right llm.StoreRequestRecord) bool {
	if !left.CreatedAt.Equal(right.CreatedAt) {
		return left.CreatedAt.Before(right.CreatedAt)
	}
	if left.Key.Caller != right.Key.Caller {
		return left.Key.Caller < right.Key.Caller
	}
	return left.Key.IdempotencyKey < right.Key.IdempotencyKey
}

func memoryLLMRecoveryAfter(record llm.StoreRequestRecord, cursor llm.StoreRecoveryCursor) bool {
	if !record.CreatedAt.Equal(cursor.CreatedAt) {
		return record.CreatedAt.After(cursor.CreatedAt)
	}
	if record.Key.Caller != cursor.Caller {
		return record.Key.Caller > cursor.Caller
	}
	return record.Key.IdempotencyKey > cursor.IdempotencyKey
}

func memoryLLMRetentionLess(left, right llm.StoreRetentionCandidate) bool {
	if !left.EffectiveCompletedAt.Equal(right.EffectiveCompletedAt) {
		return left.EffectiveCompletedAt.Before(right.EffectiveCompletedAt)
	}
	if left.Request.Key.Caller != right.Request.Key.Caller {
		return left.Request.Key.Caller < right.Request.Key.Caller
	}
	return left.Request.Key.IdempotencyKey < right.Request.Key.IdempotencyKey
}

func memoryLLMRetentionAfter(key llm.StoreRequestKey, effective time.Time, cursor llm.StoreRetentionCursor) bool {
	if !effective.Equal(cursor.CompletedAt) {
		return effective.After(cursor.CompletedAt)
	}
	if key.Caller != cursor.Caller {
		return key.Caller > cursor.Caller
	}
	return key.IdempotencyKey > cursor.IdempotencyKey
}

func cloneMemoryLLMTask(record llm.StoreTaskRecord) llm.StoreTaskRecord {
	cloned := record
	cloned.Codec = cloneMemoryLLMCodec(record.Codec)
	return cloned
}

func cloneMemoryLLMRequest(record llm.StoreRequestRecord) llm.StoreRequestRecord {
	cloned := record
	cloned.Codec = cloneMemoryLLMCodec(record.Codec)
	cloned.CanonicalPayload = cloneMemoryLLMBytes(record.CanonicalPayload)
	cloned.Decision = cloneMemoryLLMDecision(record.Decision)
	cloned.CompletedAt = cloneMemoryLLMTime(record.CompletedAt)
	cloned.PayloadPrunedAt = cloneMemoryLLMTime(record.PayloadPrunedAt)
	return cloned
}

func cloneMemoryLLMEvent(record llm.StoreResponseEventRecord) llm.StoreResponseEventRecord {
	cloned := record
	cloned.Data = cloneMemoryLLMBytes(record.Data)
	return cloned
}

func cloneMemoryLLMTool(record llm.StoreToolExecutionRecord) llm.StoreToolExecutionRecord {
	cloned := record
	cloned.Result = cloneMemoryLLMBytes(record.Result)
	cloned.CompletedAt = cloneMemoryLLMTime(record.CompletedAt)
	cloned.ResultPrunedAt = cloneMemoryLLMTime(record.ResultPrunedAt)
	return cloned
}

func cloneMemoryLLMDecision(decision llm.StoreResponseDecision) llm.StoreResponseDecision {
	cloned := decision
	cloned.Body = cloneMemoryLLMBytes(decision.Body)
	return cloned
}

func cloneMemoryLLMCodec(codec llm.CodecSnapshot) llm.CodecSnapshot {
	cloned := codec
	cloned.Contract.Features = make(map[framework.Feature]uint16, len(codec.Contract.Features))
	for feature, version := range codec.Contract.Features {
		cloned.Contract.Features[feature] = version
	}
	return cloned
}

func cloneMemoryLLMBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte{}, value...)
}

func cloneMemoryLLMTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
