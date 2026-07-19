package customstore

import (
	"bytes"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

type customReceiptKey struct {
	request llm.StoreRequestKey
	eventID string
}

type customState struct {
	tasks    map[llm.StoreTaskKey]llm.StoreTaskRecord
	requests map[llm.StoreRequestKey]llm.StoreRequestRecord
	events   map[llm.StoreRequestKey]map[uint64]llm.StoreResponseEventRecord
	receipts map[customReceiptKey]llm.StoreWorkerReceiptRecord
	tools    map[llm.StoreToolExecutionKey]llm.StoreToolExecutionRecord
}

func newCustomState() customState {
	return customState{
		tasks:    make(map[llm.StoreTaskKey]llm.StoreTaskRecord),
		requests: make(map[llm.StoreRequestKey]llm.StoreRequestRecord),
		events:   make(map[llm.StoreRequestKey]map[uint64]llm.StoreResponseEventRecord),
		receipts: make(map[customReceiptKey]llm.StoreWorkerReceiptRecord),
		tools:    make(map[llm.StoreToolExecutionKey]llm.StoreToolExecutionRecord),
	}
}
func (state customState) clone() customState {
	cloned := newCustomState()
	for key, record := range state.tasks {
		cloned.tasks[key] = cloneCustomTask(record)
	}
	for key, record := range state.requests {
		cloned.requests[key] = cloneCustomRequest(record)
	}
	for key, records := range state.events {
		cloned.events[key] = make(map[uint64]llm.StoreResponseEventRecord, len(records))
		for sequence, record := range records {
			cloned.events[key][sequence] = cloneCustomEvent(record)
		}
	}
	for key, record := range state.receipts {
		cloned.receipts[key] = record
	}
	for key, record := range state.tools {
		cloned.tools[key] = cloneCustomTool(record)
	}
	return cloned
}

type customUnit struct {
	state  *customState
	active atomic.Bool
}

func (unit *customUnit) ensureActive() error {
	if unit == nil || unit.state == nil || !unit.active.Load() {
		return llm.ErrStoreClosed
	}
	return nil
}

type customView struct{ unit *customUnit }
type customTx struct{ customView }

var _ llm.StoreView = customView{}
var _ llm.StoreTx = customTx{}

func (view customView) LoadTask(key llm.StoreTaskKey) (llm.StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreTaskRecord{}, err
	}
	if !customValidTaskKey(key) {
		return llm.StoreTaskRecord{}, customInvalid("invalid task key")
	}
	record, ok := view.unit.state.tasks[key]
	if !ok {
		return llm.StoreTaskRecord{}, customNotFound(llm.StoreRecordTask, key)
	}
	return cloneCustomTask(record), nil
}

func (view customView) FindOpenTask(affinity llm.StoreTaskAffinity) (llm.StoreTaskRecord, error) {
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
			customAffinity(record) == affinity {
			return cloneCustomTask(record), nil
		}
	}
	return llm.StoreTaskRecord{}, customNotFound(llm.StoreRecordTask, affinity)
}

func (view customView) FindActiveRequest(task llm.StoreTaskKey) (llm.StoreRequestHead, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreRequestHead{}, err
	}
	if !customValidTaskKey(task) {
		return llm.StoreRequestHead{}, customInvalid("invalid task key")
	}
	for _, record := range view.unit.state.requests {
		if record.Task == task && !record.ResponseComplete {
			return customRequestHead(record), nil
		}
	}
	return llm.StoreRequestHead{}, customNotFound(llm.StoreRecordRequest, task)
}

func (view customView) LoadRequestHead(key llm.StoreRequestKey) (llm.StoreRequestHead, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreRequestHead{}, err
	}
	if !customValidRequestKey(key) {
		return llm.StoreRequestHead{}, customInvalid("invalid request key")
	}
	record, ok := view.unit.state.requests[key]
	if !ok {
		return llm.StoreRequestHead{}, customNotFound(llm.StoreRecordRequest, key)
	}
	return customRequestHead(record), nil
}

func (view customView) LoadRequest(key llm.StoreRequestKey, limit llm.StoreReadLimit) (llm.StoreRequestRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreRequestRecord{}, err
	}
	if !customValidRequestKey(key) {
		return llm.StoreRequestRecord{}, customInvalid("invalid request key")
	}
	if limit.MaxBytes < 1 {
		return llm.StoreRequestRecord{}, customLimit(llm.StoreRecordRequest, limit.MaxBytes)
	}
	record, ok := view.unit.state.requests[key]
	if !ok {
		return llm.StoreRequestRecord{}, customNotFound(llm.StoreRecordRequest, key)
	}
	if err := customBudget(llm.StoreRecordRequest, int64(len(record.CanonicalPayload)+len(record.Decision.Body)), limit); err != nil {
		return llm.StoreRequestRecord{}, err
	}
	return cloneCustomRequest(record), nil
}

func (view customView) LoadResponseDecision(key llm.StoreRequestKey, limit llm.StoreReadLimit) (llm.StoreResponseDecision, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreResponseDecision{}, err
	}
	if !customValidRequestKey(key) {
		return llm.StoreResponseDecision{}, customInvalid("invalid request key")
	}
	if limit.MaxBytes < 1 {
		return llm.StoreResponseDecision{}, customLimit(llm.StoreRecordRequest, limit.MaxBytes)
	}
	record, ok := view.unit.state.requests[key]
	if !ok {
		return llm.StoreResponseDecision{}, customNotFound(llm.StoreRecordRequest, key)
	}
	if err := customBudget(llm.StoreRecordRequest, int64(len(record.Decision.Body)), limit); err != nil {
		return llm.StoreResponseDecision{}, err
	}
	return cloneCustomDecision(record.Decision), nil
}

func (view customView) LoadWorkerReceipt(request llm.StoreRequestKey, eventID string) (llm.StoreWorkerReceiptRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreWorkerReceiptRecord{}, err
	}
	if !customValidRequestKey(request) || eventID == "" {
		return llm.StoreWorkerReceiptRecord{}, customInvalid("invalid worker receipt key")
	}
	record, ok := view.unit.state.receipts[customReceiptKey{request: request, eventID: eventID}]
	if !ok {
		return llm.StoreWorkerReceiptRecord{}, customNotFound(llm.StoreRecordWorkerReceipt, eventID)
	}
	return record, nil
}

func (view customView) LoadToolExecution(key llm.StoreToolExecutionKey, limit llm.StoreReadLimit) (llm.StoreToolExecutionRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreToolExecutionRecord{}, err
	}
	if !customValidToolKey(key) {
		return llm.StoreToolExecutionRecord{}, customInvalid("invalid tool execution key")
	}
	if limit.MaxBytes < 1 {
		return llm.StoreToolExecutionRecord{}, customLimit(llm.StoreRecordToolExecution, limit.MaxBytes)
	}
	record, ok := view.unit.state.tools[key]
	if !ok {
		return llm.StoreToolExecutionRecord{}, customNotFound(llm.StoreRecordToolExecution, key)
	}
	if err := customBudget(llm.StoreRecordToolExecution, int64(len(record.Result)), limit); err != nil {
		return llm.StoreToolExecutionRecord{}, err
	}
	return cloneCustomTool(record), nil
}

func (view customView) ScanResponseEvents(scan llm.StoreResponseEventScan) ([]llm.StoreResponseEventRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if !customValidRequestKey(scan.Request) {
		return nil, customInvalid("invalid response-event request key")
	}
	if err := customScanArguments(llm.StoreRecordResponseEvent, scan.Limit, scan.ReadLimit); err != nil {
		return nil, err
	}
	kinds := make(map[llm.StoreResponseEventKind]bool, len(scan.Kinds))
	for _, kind := range scan.Kinds {
		if kind != llm.StoreEventCheckpoint && kind != llm.StoreEventWire {
			return nil, customInvalid("invalid response-event kind %q", kind)
		}
		if kinds[kind] {
			return nil, customInvalid("duplicate response-event kind %q", kind)
		}
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
				return nil, customLimit(llm.StoreRecordResponseEvent, scan.ReadLimit.MaxBytes)
			}
			break
		}
		if used+size > scan.ReadLimit.MaxBytes {
			break
		}
		result = append(result, cloneCustomEvent(record))
		used += size
	}
	return result, nil
}

func (view customView) ScanToolExecutions(scan llm.StoreToolExecutionScan) ([]llm.StoreToolExecutionRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if !customValidTaskKey(scan.Task) {
		return nil, customInvalid("invalid tool-execution task key")
	}
	if scan.State != "" && scan.State != llm.ToolExecutionPending && scan.State != llm.ToolExecutionCompleted {
		return nil, customInvalid("invalid tool-execution state %q", scan.State)
	}
	if err := customScanArguments(llm.StoreRecordToolExecution, scan.Limit, scan.ReadLimit); err != nil {
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
				return nil, customLimit(llm.StoreRecordToolExecution, scan.ReadLimit.MaxBytes)
			}
			break
		}
		if used+size > scan.ReadLimit.MaxBytes {
			break
		}
		result = append(result, cloneCustomTool(record))
		used += size
	}
	return result, nil
}

func (view customView) ScanRecovery(scan llm.StoreRecoveryScan) ([]llm.StoreRecoveryRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := customScanArguments(llm.StoreRecordRequest, scan.Limit, scan.ReadLimit); err != nil {
		return nil, err
	}
	if scan.After != nil && (scan.After.CreatedAt.IsZero() || scan.After.Caller == "" || scan.After.IdempotencyKey == "") {
		return nil, customInvalid("invalid recovery cursor")
	}
	candidates := make([]llm.StoreRecoveryRecord, 0)
	for _, request := range view.unit.state.requests {
		// Quarantine is itself the durable recovery decision. Never resurrect it
		// merely because the corrupt history which caused quarantine still has no
		// matching receipt.
		if request.RecoveryQuarantined || request.ResponseComplete && !view.unacknowledged(request.Key) {
			continue
		}
		if scan.After != nil && !customRecoveryAfter(request, *scan.After) {
			continue
		}
		task, ok := view.unit.state.tasks[request.Task]
		if !ok {
			continue
		}
		candidates = append(candidates, llm.StoreRecoveryRecord{
			Task: cloneCustomTask(task), Request: cloneCustomRequest(request),
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return customRecoveryLess(candidates[i].Request, candidates[j].Request)
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
				return nil, customLimit(llm.StoreRecordRequest, scan.ReadLimit.MaxBytes)
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

func (view customView) ScanRetention(scan llm.StoreRetentionScan) ([]llm.StoreRetentionCandidate, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if scan.CompletedBefore.IsZero() || scan.Limit < 1 || scan.Limit > 4096 {
		return nil, customInvalid("invalid retention scan")
	}
	if scan.After != nil && (scan.After.CompletedAt.IsZero() || scan.After.Caller == "" || scan.After.IdempotencyKey == "") {
		return nil, customInvalid("invalid retention cursor")
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
		if effective.After(scan.CompletedBefore) || scan.After != nil && !customRetentionAfter(request.Key, effective, *scan.After) {
			continue
		}
		result = append(result, llm.StoreRetentionCandidate{
			Request: customRequestHead(request), EffectiveCompletedAt: effective,
			UnacknowledgedWorkerEvent: !request.RecoveryQuarantined && view.unacknowledged(request.Key),
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return customRetentionLess(result[i], result[j])
	})
	if len(result) > scan.Limit {
		result = result[:scan.Limit]
	}
	return result, nil
}

func (view customView) unacknowledged(request llm.StoreRequestKey) bool {
	for _, event := range view.unit.state.events[request] {
		if event.WorkerEventID == "" {
			continue
		}
		receipt, ok := view.unit.state.receipts[customReceiptKey{request: request, eventID: event.WorkerEventID}]
		if !ok || receipt.Worker != event.Worker || receipt.Digest != event.WorkerEventDigest {
			return true
		}
	}
	return false
}

func (tx customTx) InsertTask(record llm.StoreTaskRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := customValidateTask(record); err != nil {
		return err
	}
	if _, exists := tx.unit.state.tasks[record.Key]; exists {
		return customConflict(llm.StoreConstraintTaskKey, record.Key)
	}
	if !record.State.Terminal() &&
		(record.CapabilityTier == llm.TierRemoteTools || record.CapabilityTier == llm.TierWorkspace) {
		for _, existing := range tx.unit.state.tasks {
			if !existing.State.Terminal() && customAffinity(existing) == customAffinity(record) {
				return customConflict(llm.StoreConstraintOpenAffinity, record.Key)
			}
		}
	}
	tx.unit.state.tasks[record.Key] = cloneCustomTask(record)
	return nil
}

func (tx customTx) CompareAndSwapTask(mutation llm.StoreTaskMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	if !customValidTaskKey(mutation.Key) || mutation.ExpectedRevision == 0 {
		return false, customInvalid("invalid task mutation")
	}
	if err := customValidateTask(mutation.Next); err != nil {
		return false, err
	}
	current, exists := tx.unit.state.tasks[mutation.Key]
	if !exists {
		return false, customNotFound(llm.StoreRecordTask, mutation.Key)
	}
	if current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if !customTaskIdentityEqual(current, mutation.Next) || mutation.Next.Revision != current.Revision+1 {
		return false, customConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	if !mutation.Next.State.Terminal() &&
		(mutation.Next.CapabilityTier == llm.TierRemoteTools || mutation.Next.CapabilityTier == llm.TierWorkspace) {
		for key, existing := range tx.unit.state.tasks {
			if key != mutation.Key && !existing.State.Terminal() && customAffinity(existing) == customAffinity(mutation.Next) {
				return false, customConflict(llm.StoreConstraintOpenAffinity, mutation.Key)
			}
		}
	}
	tx.unit.state.tasks[mutation.Key] = cloneCustomTask(mutation.Next)
	return true, nil
}

func (tx customTx) InsertRequest(record llm.StoreRequestRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := customValidateRequest(record); err != nil {
		return err
	}
	if _, exists := tx.unit.state.requests[record.Key]; exists {
		return customConflict(llm.StoreConstraintRequestKey, record.Key)
	}
	task, exists := tx.unit.state.tasks[record.Task]
	if !exists {
		return customNotFound(llm.StoreRecordTask, record.Task)
	}
	if !task.Codec.Equal(record.Codec) {
		return customConflict(llm.StoreConstraintImmutableRecord, record.Key)
	}
	for _, existing := range tx.unit.state.requests {
		if !record.ResponseComplete && existing.Task == record.Task && !existing.ResponseComplete {
			return customConflict(llm.StoreConstraintActiveRequest, record.Task)
		}
		if existing.Key.Caller == record.Key.Caller &&
			(existing.RequestID == record.RequestID || existing.ResponseID == record.ResponseID) {
			return customConflict(llm.StoreConstraintRequestKey, record.Key)
		}
	}
	tx.unit.state.requests[record.Key] = cloneCustomRequest(record)
	return nil
}

func (tx customTx) CompareAndSwapRequest(mutation llm.StoreRequestMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	if !customValidRequestKey(mutation.Key) || mutation.ExpectedRevision == 0 {
		return false, customInvalid("invalid request mutation")
	}
	if err := customValidateRequest(mutation.Next); err != nil {
		return false, err
	}
	current, exists := tx.unit.state.requests[mutation.Key]
	if !exists {
		return false, customNotFound(llm.StoreRecordRequest, mutation.Key)
	}
	if current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if !customRequestIdentityEqual(current, mutation.Next) || mutation.Next.Revision != current.Revision+1 {
		return false, customConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	if current.PayloadPrunedAt == nil {
		if mutation.Next.PayloadPrunedAt == nil && !bytes.Equal(current.CanonicalPayload, mutation.Next.CanonicalPayload) {
			return false, customConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
		}
		if mutation.Next.PayloadPrunedAt != nil && (len(mutation.Next.CanonicalPayload) != 0 || len(mutation.Next.Decision.Body) != 0) {
			return false, customConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
		}
	} else if mutation.Next.PayloadPrunedAt == nil || !mutation.Next.PayloadPrunedAt.Equal(*current.PayloadPrunedAt) ||
		len(mutation.Next.CanonicalPayload) != 0 || len(mutation.Next.Decision.Body) != 0 {
		return false, customConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	if !mutation.Next.ResponseComplete {
		for key, existing := range tx.unit.state.requests {
			if key != mutation.Key && existing.Task == mutation.Next.Task && !existing.ResponseComplete {
				return false, customConflict(llm.StoreConstraintActiveRequest, mutation.Next.Task)
			}
		}
	}
	tx.unit.state.requests[mutation.Key] = cloneCustomRequest(mutation.Next)
	return true, nil
}

func (tx customTx) InsertResponseEvent(record llm.StoreResponseEventRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := customValidateResponseEvent(record); err != nil {
		return err
	}
	if _, exists := tx.unit.state.requests[record.Request]; !exists {
		return customNotFound(llm.StoreRecordRequest, record.Request)
	}
	if tx.unit.state.events[record.Request] == nil {
		tx.unit.state.events[record.Request] = make(map[uint64]llm.StoreResponseEventRecord)
	}
	if _, exists := tx.unit.state.events[record.Request][record.Sequence]; exists {
		return customConflict(llm.StoreConstraintResponseSequence, record.Sequence)
	}
	if record.WorkerEventID != "" {
		for _, existing := range tx.unit.state.events[record.Request] {
			if existing.WorkerEventID == record.WorkerEventID &&
				(existing.Worker != record.Worker || existing.WorkerEventDigest != record.WorkerEventDigest) {
				return customConflict(llm.StoreConstraintWorkerEvent, record.WorkerEventID)
			}
		}
	}
	tx.unit.state.events[record.Request][record.Sequence] = cloneCustomEvent(record)
	return nil
}

func (tx customTx) InsertWorkerReceipt(record llm.StoreWorkerReceiptRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := customValidateWorkerReceipt(record); err != nil {
		return err
	}
	if _, exists := tx.unit.state.requests[record.Request]; !exists {
		return customNotFound(llm.StoreRecordRequest, record.Request)
	}
	key := customReceiptKey{request: record.Request, eventID: record.EventID}
	if _, exists := tx.unit.state.receipts[key]; exists {
		return customConflict(llm.StoreConstraintWorkerReceipt, record.EventID)
	}
	tx.unit.state.receipts[key] = record
	return nil
}

func (tx customTx) InsertToolExecution(record llm.StoreToolExecutionRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := customValidateTool(record); err != nil {
		return err
	}
	if _, exists := tx.unit.state.tasks[record.Key.Task]; !exists {
		return customNotFound(llm.StoreRecordTask, record.Key.Task)
	}
	if _, exists := tx.unit.state.tools[record.Key]; exists {
		return customConflict(llm.StoreConstraintToolCall, record.Key)
	}
	tx.unit.state.tools[record.Key] = cloneCustomTool(record)
	return nil
}

func (tx customTx) CompareAndSwapToolExecution(mutation llm.StoreToolExecutionMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	if !customValidToolKey(mutation.Key) || mutation.ExpectedRevision == 0 {
		return false, customInvalid("invalid tool execution mutation")
	}
	if err := customValidateTool(mutation.Next); err != nil {
		return false, err
	}
	current, exists := tx.unit.state.tools[mutation.Key]
	if !exists {
		return false, customNotFound(llm.StoreRecordToolExecution, mutation.Key)
	}
	if current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if current.Key != mutation.Next.Key || current.InputDigest != mutation.Next.InputDigest ||
		!current.CreatedAt.Equal(mutation.Next.CreatedAt) || mutation.Next.Revision != current.Revision+1 {
		return false, customConflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	tx.unit.state.tools[mutation.Key] = cloneCustomTool(mutation.Next)
	return true, nil
}

func (tx customTx) DeleteTombstonedResponseEvents(key llm.StoreRequestKey) (uint64, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return 0, err
	}
	if !customValidRequestKey(key) {
		return 0, customInvalid("invalid request key")
	}
	request, exists := tx.unit.state.requests[key]
	if !exists {
		return 0, customNotFound(llm.StoreRecordRequest, key)
	}
	if request.PayloadPrunedAt == nil {
		return 0, customConflict(llm.StoreConstraintCompareAndSwap, key)
	}
	count := uint64(len(tx.unit.state.events[key]))
	delete(tx.unit.state.events, key)
	return count, nil
}

func customNotFound(kind llm.StoreRecordKind, key any) error {
	return &llm.StoreNotFoundError{Record: kind, Key: fmt.Sprint(key)}
}

func customInvalid(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", llm.ErrStoreInvalidArgument, fmt.Sprintf(format, arguments...))
}

func customValidTaskKey(key llm.StoreTaskKey) bool {
	return key.Caller != "" && key.Task != ""
}

func customValidRequestKey(key llm.StoreRequestKey) bool {
	return key.Caller != "" && key.IdempotencyKey != ""
}

func customValidToolKey(key llm.StoreToolExecutionKey) bool {
	return customValidTaskKey(key.Task) && key.ToolCallID != ""
}

func customValidateTask(record llm.StoreTaskRecord) error {
	if !customValidTaskKey(record.Key) || record.Revision == 0 ||
		record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() || !record.State.Valid() ||
		(record.LeaseOwner == "") != (record.LeaseID == "") {
		return customInvalid("invalid task record")
	}
	switch record.CapabilityTier {
	case llm.TierChat:
	case llm.TierRemoteTools, llm.TierWorkspace:
		if record.WorkspaceKey == "" || record.HarnessID == "" ||
			record.HarnessVersion == "" || record.HarnessSessionID == "" {
			return customInvalid("tool-capable task requires complete affinity")
		}
	default:
		return customInvalid("invalid capability tier %q", record.CapabilityTier)
	}
	if err := record.Codec.Validate(); err != nil {
		return customInvalid("invalid task codec: %v", err)
	}
	return nil
}

func customValidateRequest(record llm.StoreRequestRecord) error {
	if !customValidRequestKey(record.Key) || !customValidTaskKey(record.Task) ||
		record.Key.Caller != record.Task.Caller ||
		record.RequestID == "" || record.ResponseID == "" || record.RequestDigest == "" ||
		record.Revision == 0 || record.CreatedAt.IsZero() ||
		(record.Mode != llm.ResponseStream && record.Mode != llm.ResponseAggregate) {
		return customInvalid("invalid request record")
	}
	if err := record.Codec.Validate(); err != nil {
		return customInvalid("invalid request codec: %v", err)
	}
	if record.PayloadPrunedAt != nil && (!record.ResponseComplete ||
		len(record.CanonicalPayload) != 0 || len(record.Decision.Body) != 0) {
		return customInvalid("tombstoned request retains payload bytes")
	}
	return nil
}

func customValidateResponseEvent(record llm.StoreResponseEventRecord) error {
	if !customValidRequestKey(record.Request) || record.Sequence == 0 ||
		(record.Kind != llm.StoreEventCheckpoint && record.Kind != llm.StoreEventWire) ||
		record.CreatedAt.IsZero() ||
		(record.Worker == "") != (record.WorkerEventID == "") ||
		(record.WorkerEventID == "") != (record.WorkerEventDigest == "") {
		return customInvalid("invalid response event record")
	}
	return nil
}

func customValidateWorkerReceipt(record llm.StoreWorkerReceiptRecord) error {
	if !customValidRequestKey(record.Request) || record.EventID == "" ||
		record.Worker == "" || record.Digest == "" || record.CreatedAt.IsZero() {
		return customInvalid("invalid worker receipt record")
	}
	return nil
}

func customValidateTool(record llm.StoreToolExecutionRecord) error {
	if !customValidToolKey(record.Key) || record.InputDigest == "" || record.Revision == 0 ||
		record.CreatedAt.IsZero() ||
		(record.State != llm.ToolExecutionPending && record.State != llm.ToolExecutionCompleted) {
		return customInvalid("invalid tool execution record")
	}
	return nil
}

func customConflict(constraint llm.StoreConstraint, key any) error {
	return &llm.StoreConflictError{Constraint: constraint, Key: fmt.Sprint(key)}
}

func customLimit(kind llm.StoreRecordKind, limit int64) error {
	return &llm.StoreLimitError{Record: kind, Limit: limit}
}

func customBudget(kind llm.StoreRecordKind, size int64, limit llm.StoreReadLimit) error {
	if limit.MaxBytes < 1 || size > limit.MaxBytes {
		return customLimit(kind, limit.MaxBytes)
	}
	return nil
}

func customScanArguments(kind llm.StoreRecordKind, limit int, readLimit llm.StoreReadLimit) error {
	if limit < 1 || limit > 4096 {
		return customInvalid("scan limit must be 1..4096")
	}
	if readLimit.MaxBytes < 1 {
		return customLimit(kind, readLimit.MaxBytes)
	}
	return nil
}

func customAffinity(record llm.StoreTaskRecord) llm.StoreTaskAffinity {
	return llm.StoreTaskAffinity{
		Caller: record.Key.Caller, WorkspaceKey: record.WorkspaceKey,
		HarnessID: record.HarnessID, HarnessVersion: record.HarnessVersion,
		HarnessSessionID: record.HarnessSessionID,
	}
}

func customTaskIdentityEqual(left, right llm.StoreTaskRecord) bool {
	return left.Key == right.Key && left.WorkspaceKey == right.WorkspaceKey &&
		left.CapabilityTier == right.CapabilityTier && left.Codec.Equal(right.Codec) &&
		left.HarnessID == right.HarnessID && left.HarnessVersion == right.HarnessVersion &&
		left.HarnessSessionID == right.HarnessSessionID && left.WorkspaceRoot == right.WorkspaceRoot &&
		left.ExecAllowed == right.ExecAllowed && left.CreatedAt.Equal(right.CreatedAt)
}

func customRequestIdentityEqual(left, right llm.StoreRequestRecord) bool {
	return left.Key == right.Key && left.Task == right.Task && left.RequestID == right.RequestID &&
		left.ResponseID == right.ResponseID && left.RequestDigest == right.RequestDigest &&
		left.Codec.Equal(right.Codec) && left.Mode == right.Mode && left.CreatedAt.Equal(right.CreatedAt)
}

func customRequestHead(record llm.StoreRequestRecord) llm.StoreRequestHead {
	return llm.StoreRequestHead{
		Key: record.Key, Task: record.Task, RequestID: record.RequestID, ResponseID: record.ResponseID,
		RequestDigest: record.RequestDigest, Codec: cloneCustomCodec(record.Codec), Mode: record.Mode,
		DecisionStatus: record.Decision.StatusCode, DecisionContentType: record.Decision.ContentType,
		DecisionRetryAfter: record.Decision.RetryAfter, ResponseComplete: record.ResponseComplete,
		RecoveryQuarantined: record.RecoveryQuarantined, LastEventSequence: record.LastEventSequence,
		Revision: record.Revision, CreatedAt: record.CreatedAt,
		CompletedAt: cloneCustomTime(record.CompletedAt), PayloadPrunedAt: cloneCustomTime(record.PayloadPrunedAt),
	}
}

func customRecoveryLess(left, right llm.StoreRequestRecord) bool {
	if !left.CreatedAt.Equal(right.CreatedAt) {
		return left.CreatedAt.Before(right.CreatedAt)
	}
	if left.Key.Caller != right.Key.Caller {
		return left.Key.Caller < right.Key.Caller
	}
	return left.Key.IdempotencyKey < right.Key.IdempotencyKey
}

func customRecoveryAfter(record llm.StoreRequestRecord, cursor llm.StoreRecoveryCursor) bool {
	if !record.CreatedAt.Equal(cursor.CreatedAt) {
		return record.CreatedAt.After(cursor.CreatedAt)
	}
	if record.Key.Caller != cursor.Caller {
		return record.Key.Caller > cursor.Caller
	}
	return record.Key.IdempotencyKey > cursor.IdempotencyKey
}

func customRetentionLess(left, right llm.StoreRetentionCandidate) bool {
	if !left.EffectiveCompletedAt.Equal(right.EffectiveCompletedAt) {
		return left.EffectiveCompletedAt.Before(right.EffectiveCompletedAt)
	}
	if left.Request.Key.Caller != right.Request.Key.Caller {
		return left.Request.Key.Caller < right.Request.Key.Caller
	}
	return left.Request.Key.IdempotencyKey < right.Request.Key.IdempotencyKey
}

func customRetentionAfter(key llm.StoreRequestKey, effective time.Time, cursor llm.StoreRetentionCursor) bool {
	if !effective.Equal(cursor.CompletedAt) {
		return effective.After(cursor.CompletedAt)
	}
	if key.Caller != cursor.Caller {
		return key.Caller > cursor.Caller
	}
	return key.IdempotencyKey > cursor.IdempotencyKey
}

func cloneCustomTask(record llm.StoreTaskRecord) llm.StoreTaskRecord {
	cloned := record
	cloned.Codec = cloneCustomCodec(record.Codec)
	return cloned
}

func cloneCustomRequest(record llm.StoreRequestRecord) llm.StoreRequestRecord {
	cloned := record
	cloned.Codec = cloneCustomCodec(record.Codec)
	cloned.CanonicalPayload = cloneCustomBytes(record.CanonicalPayload)
	cloned.Decision = cloneCustomDecision(record.Decision)
	cloned.CompletedAt = cloneCustomTime(record.CompletedAt)
	cloned.PayloadPrunedAt = cloneCustomTime(record.PayloadPrunedAt)
	return cloned
}

func cloneCustomEvent(record llm.StoreResponseEventRecord) llm.StoreResponseEventRecord {
	cloned := record
	cloned.Data = cloneCustomBytes(record.Data)
	return cloned
}

func cloneCustomTool(record llm.StoreToolExecutionRecord) llm.StoreToolExecutionRecord {
	cloned := record
	cloned.Result = cloneCustomBytes(record.Result)
	cloned.CompletedAt = cloneCustomTime(record.CompletedAt)
	cloned.ResultPrunedAt = cloneCustomTime(record.ResultPrunedAt)
	return cloned
}

func cloneCustomDecision(decision llm.StoreResponseDecision) llm.StoreResponseDecision {
	cloned := decision
	cloned.Body = cloneCustomBytes(decision.Body)
	return cloned
}

func cloneCustomCodec(codec llm.CodecSnapshot) llm.CodecSnapshot {
	cloned := codec
	cloned.Contract.Features = make(map[framework.Feature]uint16, len(codec.Contract.Features))
	for feature, version := range codec.Contract.Features {
		cloned.Contract.Features[feature] = version
	}
	return cloned
}

func cloneCustomBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte{}, value...)
}

func cloneCustomTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
