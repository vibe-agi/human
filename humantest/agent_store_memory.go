package humantest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

// MemoryAgentStore is an in-memory semantic model of [agent.Store] intended
// for tests, examples, and fault-injection harnesses. It preserves atomic
// serializable updates, stable View snapshots, callback lifetimes, logical
// constraints, scan ordering, read budgets, and byte ownership. It does not
// provide real process durability; [MemoryAgentStoreImage] only models a
// committed image for deterministic lifecycle and fault-state tests.
//
// Construct it with [NewMemoryAgentStore] so ownership and release remain
// explicit. Store has no Close method by design.
type MemoryAgentStore struct {
	image  *MemoryAgentStoreImage
	handle *memoryAgentStoreHandle
}

type memoryAgentStoreHandle struct{ closed bool }

// MemoryAgentStoreImage is a test-only model of durable Agent storage media.
// Every successful Update changes the image synchronously. Releasing an open
// MemoryAgentStore closes only that runtime handle; opening the same image again
// creates a distinct Store handle over the already committed state. Abandon
// can explicitly model loss of a process-local handle without pretending that
// in-memory data survives a real process exit.
//
// An image admits at most one live Store handle, matching Agent Store's
// single-active-owner correctness boundary.
type MemoryAgentStoreImage struct {
	mu    sync.RWMutex
	state memoryAgentState
	owner *memoryAgentStoreHandle
}

// ErrMemoryAgentStoreImageInUse means an image already has a live Store handle.
var ErrMemoryAgentStoreImageInUse = errors.New("memory Agent Store image is already in use")

type memoryAgentLeaseKey struct {
	task  agent.TaskRef
	fence agent.LeaseFence
}

type memoryAgentState struct {
	commands       map[agent.StoreCommandKey]agent.StoreCommandRecord
	tasks          map[agent.TaskRef]agent.StoreTaskRecord
	messages       map[agent.StoreMessageKey]agent.StoreMessageRecord
	events         map[agent.TaskRef]map[uint64]agent.StoreEventRecord
	leaseGrants    map[memoryAgentLeaseKey]agent.StoreLeaseGrantRecord
	artifacts      map[agent.ArtifactRef]agent.StoreArtifactRecord
	submissions    map[agent.TaskRef]agent.StoreSubmissionRecord
	workspaceHeads map[agent.WorkspaceRef]agent.StoreWorkspaceHeadRecord
	applyReceipts  map[agent.ArtifactRef]agent.StoreApplyReceiptRecord
}

// NewMemoryAgentStore creates an independent, empty test Store and an
// idempotent release function. After release, View and Update return
// [agent.ErrStoreClosed].
func NewMemoryAgentStore() (*MemoryAgentStore, framework.ReleaseFunc) {
	store, release, err := NewMemoryAgentStoreImage().Open()
	if err != nil {
		panic(fmt.Sprintf("open new memory Agent Store image: %v", err))
	}
	return store, release
}

// NewMemoryAgentStoreImage creates an empty test-only durable image. Call Open
// to acquire a Store handle, then Release or Abandon it before reopening the
// committed state.
func NewMemoryAgentStoreImage() *MemoryAgentStoreImage {
	return &MemoryAgentStoreImage{state: newMemoryAgentState()}
}

// Open acquires a new runtime Store handle over this image. The returned
// release is idempotent and must run before the image can be opened again.
func (image *MemoryAgentStoreImage) Open() (*MemoryAgentStore, framework.ReleaseFunc, error) {
	if image == nil {
		return nil, nil, fmt.Errorf("%w: memory Agent Store image is required", agent.ErrInvalidArgument)
	}
	image.mu.Lock()
	defer image.mu.Unlock()
	if image.owner != nil {
		return nil, nil, ErrMemoryAgentStoreImageInUse
	}
	if image.state.commands == nil {
		image.state = newMemoryAgentState()
	}
	handle := &memoryAgentStoreHandle{}
	store := &MemoryAgentStore{image: image, handle: handle}
	image.owner = handle
	var once sync.Once
	release := func(context.Context) error {
		once.Do(store.close)
		return nil
	}
	return store, release, nil
}

// Abandon invalidates the current handle without running its Release function.
// It is a deterministic semantic model of process loss: mutations that already
// returned remain committed in image, while process-local handle state is lost.
// It does not model OS, filesystem, or hardware crash behavior.
func (image *MemoryAgentStoreImage) Abandon(store *MemoryAgentStore) error {
	if image == nil || store == nil || store.image != image || store.handle == nil {
		return agent.ErrStoreClosed
	}
	image.mu.Lock()
	defer image.mu.Unlock()
	if store.handle.closed || image.owner != store.handle {
		return agent.ErrStoreClosed
	}
	store.handle.closed = true
	image.owner = nil
	return nil
}

func newMemoryAgentState() memoryAgentState {
	return memoryAgentState{
		commands:       make(map[agent.StoreCommandKey]agent.StoreCommandRecord),
		tasks:          make(map[agent.TaskRef]agent.StoreTaskRecord),
		messages:       make(map[agent.StoreMessageKey]agent.StoreMessageRecord),
		events:         make(map[agent.TaskRef]map[uint64]agent.StoreEventRecord),
		leaseGrants:    make(map[memoryAgentLeaseKey]agent.StoreLeaseGrantRecord),
		artifacts:      make(map[agent.ArtifactRef]agent.StoreArtifactRecord),
		submissions:    make(map[agent.TaskRef]agent.StoreSubmissionRecord),
		workspaceHeads: make(map[agent.WorkspaceRef]agent.StoreWorkspaceHeadRecord),
		applyReceipts:  make(map[agent.ArtifactRef]agent.StoreApplyReceiptRecord),
	}
}

func (*MemoryAgentStore) Description() agent.StoreDescription {
	return agent.StoreDescription{
		Contract: framework.Contract{ID: agent.StoreContractID, Major: agent.StoreContractMajor},
		Provider: "humantest-memory-model",
		Version:  "1",
	}
}

func (store *MemoryAgentStore) View(ctx context.Context, callback func(agent.StoreView) error) error {
	if ctx == nil || callback == nil {
		return fmt.Errorf("%w: context and callback are required", agent.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.image == nil || store.handle == nil {
		return agent.ErrStoreClosed
	}
	store.image.mu.RLock()
	defer store.image.mu.RUnlock()
	if store.handle.closed || store.image.owner != store.handle {
		return agent.ErrStoreClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	unit := &memoryAgentUnit{state: &store.image.state}
	unit.active.Store(true)
	defer unit.active.Store(false)
	return callback(memoryAgentView{unit: unit})
}

func (store *MemoryAgentStore) Update(ctx context.Context, callback func(agent.StoreTx) error) error {
	if ctx == nil || callback == nil {
		return fmt.Errorf("%w: context and callback are required", agent.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.image == nil || store.handle == nil {
		return agent.ErrStoreClosed
	}
	store.image.mu.Lock()
	defer store.image.mu.Unlock()
	if store.handle.closed || store.image.owner != store.handle {
		return agent.ErrStoreClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	next := store.image.state.clone()
	unit := &memoryAgentUnit{state: &next}
	unit.active.Store(true)
	err := callback(memoryAgentTx{memoryAgentView{unit: unit}})
	unit.active.Store(false)
	if err != nil {
		return err
	}
	store.image.state = next
	return nil
}

func (store *MemoryAgentStore) close() {
	if store == nil || store.image == nil || store.handle == nil {
		return
	}
	store.image.mu.Lock()
	defer store.image.mu.Unlock()
	store.handle.closed = true
	if store.image.owner == store.handle {
		store.image.owner = nil
	}
}

func (state memoryAgentState) clone() memoryAgentState {
	cloned := newMemoryAgentState()
	for key, record := range state.commands {
		cloned.commands[key] = cloneMemoryAgentCommand(record)
	}
	for key, record := range state.tasks {
		cloned.tasks[key] = cloneMemoryAgentTaskRecord(record)
	}
	for key, record := range state.messages {
		cloned.messages[key] = cloneMemoryAgentMessage(record)
	}
	for task, events := range state.events {
		cloned.events[task] = make(map[uint64]agent.StoreEventRecord, len(events))
		for sequence, record := range events {
			cloned.events[task][sequence] = record
		}
	}
	for key, record := range state.leaseGrants {
		cloned.leaseGrants[key] = record
	}
	for key, record := range state.artifacts {
		cloned.artifacts[key] = cloneMemoryAgentArtifact(record)
	}
	for key, record := range state.submissions {
		cloned.submissions[key] = cloneMemoryAgentSubmissionRecord(record)
	}
	for key, record := range state.workspaceHeads {
		cloned.workspaceHeads[key] = record
	}
	for key, record := range state.applyReceipts {
		cloned.applyReceipts[key] = record
	}
	return cloned
}

type memoryAgentUnit struct {
	state  *memoryAgentState
	active atomic.Bool
}

func (unit *memoryAgentUnit) ensureActive() error {
	if unit == nil || unit.state == nil || !unit.active.Load() {
		return agent.ErrStoreClosed
	}
	return nil
}

type memoryAgentView struct{ unit *memoryAgentUnit }
type memoryAgentTx struct{ memoryAgentView }

func (view memoryAgentView) LookupCommand(key agent.StoreCommandKey, limit agent.StoreReadLimit) (agent.StoreCommandRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreCommandRecord{}, err
	}
	if err := memoryAgentReadLimit(agent.StoreRecordCommand, limit); err != nil {
		return agent.StoreCommandRecord{}, err
	}
	record, ok := view.unit.state.commands[key]
	if !ok {
		return agent.StoreCommandRecord{}, memoryAgentNotFound(agent.StoreRecordCommand, key.ID)
	}
	if int64(len(record.Result)) > limit.MaxBytes {
		return agent.StoreCommandRecord{}, memoryAgentLimit(agent.StoreRecordCommand, limit.MaxBytes)
	}
	return cloneMemoryAgentCommand(record), nil
}

func (view memoryAgentView) LoadTask(ref agent.TaskRef) (agent.StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreTaskRecord{}, err
	}
	record, ok := view.unit.state.tasks[ref]
	if !ok {
		return agent.StoreTaskRecord{}, memoryAgentNotFound(agent.StoreRecordTask, ref.ID)
	}
	return cloneMemoryAgentTaskRecord(record), nil
}

func (view memoryAgentView) ResolveTask(authority agent.AuthorityID, id agent.TaskID) (agent.TaskRef, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.TaskRef{}, err
	}
	for ref := range view.unit.state.tasks {
		if ref.Workspace.Authority == authority && ref.ID == id {
			return ref, nil
		}
	}
	return agent.TaskRef{}, memoryAgentNotFound(agent.StoreRecordTask, id)
}

func (view memoryAgentView) LoadMessage(key agent.StoreMessageKey, limit agent.StoreReadLimit) (agent.StoreMessageRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreMessageRecord{}, err
	}
	if err := memoryAgentReadLimit(agent.StoreRecordMessage, limit); err != nil {
		return agent.StoreMessageRecord{}, err
	}
	record, ok := view.unit.state.messages[key]
	if !ok {
		return agent.StoreMessageRecord{}, memoryAgentNotFound(agent.StoreRecordMessage, key.ID)
	}
	if int64(len(record.EncodedParts)) > limit.MaxBytes {
		return agent.StoreMessageRecord{}, memoryAgentLimit(agent.StoreRecordMessage, limit.MaxBytes)
	}
	return cloneMemoryAgentMessage(record), nil
}

func (view memoryAgentView) LoadArtifact(ref agent.ArtifactRef, limit agent.StoreReadLimit) (agent.StoreArtifactRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreArtifactRecord{}, err
	}
	if err := memoryAgentReadLimit(agent.StoreRecordArtifact, limit); err != nil {
		return agent.StoreArtifactRecord{}, err
	}
	record, ok := view.unit.state.artifacts[ref]
	if !ok {
		return agent.StoreArtifactRecord{}, memoryAgentNotFound(agent.StoreRecordArtifact, ref.ID)
	}
	if int64(len(record.EncodedPayload)) > limit.MaxBytes {
		return agent.StoreArtifactRecord{}, memoryAgentLimit(agent.StoreRecordArtifact, limit.MaxBytes)
	}
	return cloneMemoryAgentArtifact(record), nil
}

func (view memoryAgentView) LoadApplyReceipt(ref agent.ArtifactRef) (agent.StoreApplyReceiptRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreApplyReceiptRecord{}, err
	}
	record, ok := view.unit.state.applyReceipts[ref]
	if !ok {
		return agent.StoreApplyReceiptRecord{}, memoryAgentNotFound(agent.StoreRecordApplyReceipt, ref.ID)
	}
	return record, nil
}

func (view memoryAgentView) LoadWorkspaceHead(ref agent.WorkspaceRef) (agent.StoreWorkspaceHeadRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreWorkspaceHeadRecord{}, err
	}
	record, ok := view.unit.state.workspaceHeads[ref]
	if !ok {
		return agent.StoreWorkspaceHeadRecord{}, memoryAgentNotFound(agent.StoreRecordWorkspaceHead, ref.ID)
	}
	return record, nil
}

func (view memoryAgentView) LoadLeaseGrant(ref agent.TaskRef, fence agent.LeaseFence) (agent.StoreLeaseGrantRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreLeaseGrantRecord{}, err
	}
	record, ok := view.unit.state.leaseGrants[memoryAgentLeaseKey{task: ref, fence: fence}]
	if !ok {
		return agent.StoreLeaseGrantRecord{}, memoryAgentNotFound(agent.StoreRecordLeaseGrant, fence)
	}
	return record, nil
}

func (view memoryAgentView) LoadLatestLeaseGrant(ref agent.TaskRef) (agent.StoreLeaseGrantRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreLeaseGrantRecord{}, err
	}
	var latest agent.StoreLeaseGrantRecord
	found := false
	for key, record := range view.unit.state.leaseGrants {
		if key.task == ref && (!found || key.fence > latest.Grant.Fence) {
			latest, found = record, true
		}
	}
	if !found {
		return agent.StoreLeaseGrantRecord{}, memoryAgentNotFound(agent.StoreRecordLeaseGrant, ref.ID)
	}
	return latest, nil
}

func (view memoryAgentView) FindClaimableTask(authority agent.AuthorityID) (agent.StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreTaskRecord{}, err
	}
	list := make([]agent.StoreTaskRecord, 0)
	for _, record := range view.unit.state.tasks {
		if record.Task.Ref.Workspace.Authority == authority && record.Lease.Owner == "" && !record.Task.State.Terminal() {
			list = append(list, cloneMemoryAgentTaskRecord(record))
		}
	}
	sort.Slice(list, func(i, j int) bool { return memoryAgentTaskCreatedLess(list[i], list[j]) })
	if len(list) == 0 {
		return agent.StoreTaskRecord{}, memoryAgentNotFound(agent.StoreRecordTask, "claimable")
	}
	return list[0], nil
}

func (view memoryAgentView) ScanContextTasks(scan agent.StoreTaskContextScan) ([]agent.StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := memoryAgentScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	list := make([]agent.StoreTaskRecord, 0)
	for _, record := range view.unit.state.tasks {
		if record.Task.Context != scan.Context || !memoryAgentTaskAfterContextCursor(record.Task, scan.After) {
			continue
		}
		list = append(list, cloneMemoryAgentTaskRecord(record))
	}
	sort.Slice(list, func(i, j int) bool { return memoryAgentTaskCreatedLess(list[i], list[j]) })
	if len(list) > scan.Limit {
		list = list[:scan.Limit]
	}
	return cloneMemoryAgentTasks(list), nil
}

func (view memoryAgentView) ScanAuthorityTasks(scan agent.StoreTaskAuthorityScan) (agent.StoreTaskAuthorityResult, error) {
	if err := view.unit.ensureActive(); err != nil {
		return agent.StoreTaskAuthorityResult{}, err
	}
	if err := memoryAgentScanLimit(scan.Limit); err != nil {
		return agent.StoreTaskAuthorityResult{}, err
	}
	filtered := make([]agent.StoreTaskRecord, 0)
	for _, record := range view.unit.state.tasks {
		if record.Task.Ref.Workspace.Authority != scan.Authority ||
			(scan.Context != "" && record.Task.Context.ID != scan.Context) ||
			(scan.State != "" && record.Task.State != scan.State) ||
			(scan.UpdatedAtOrAfter != nil && record.Task.UpdatedAt.Before(scan.UpdatedAtOrAfter.UTC())) {
			continue
		}
		filtered = append(filtered, cloneMemoryAgentTaskRecord(record))
	}
	sort.Slice(filtered, func(i, j int) bool { return memoryAgentTaskUpdatedLess(filtered[i], filtered[j]) })
	total := uint64(len(filtered))
	page := make([]agent.StoreTaskRecord, 0, min(len(filtered), scan.Limit))
	for _, record := range filtered {
		if memoryAgentTaskAfterAuthorityCursor(record.Task, scan.After) {
			page = append(page, record)
			if len(page) == scan.Limit {
				break
			}
		}
	}
	return agent.StoreTaskAuthorityResult{Records: cloneMemoryAgentTasks(page), TotalSize: total}, nil
}

func (view memoryAgentView) ScanMessages(scan agent.StoreMessageScan) ([]agent.StoreMessageRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := memoryAgentScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	if err := memoryAgentReadLimit(agent.StoreRecordMessage, scan.ReadLimit); err != nil {
		return nil, err
	}
	list := make([]agent.StoreMessageRecord, 0)
	for _, record := range view.unit.state.messages {
		if record.Task == scan.Task && record.Sequence > scan.After {
			list = append(list, cloneMemoryAgentMessage(record))
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Sequence < list[j].Sequence })
	if len(list) > scan.Limit {
		list = list[:scan.Limit]
	}
	result := make([]agent.StoreMessageRecord, 0, len(list))
	var total int64
	for _, record := range list {
		size := int64(len(record.EncodedParts))
		if size > scan.ReadLimit.MaxBytes {
			if len(result) != 0 {
				break
			}
			return nil, memoryAgentLimit(agent.StoreRecordMessage, scan.ReadLimit.MaxBytes)
		}
		if total > scan.ReadLimit.MaxBytes-size {
			break
		}
		total += size
		result = append(result, record)
	}
	return cloneMemoryAgentMessages(result), nil
}

func (view memoryAgentView) ScanEvents(scan agent.StoreEventScan) ([]agent.StoreEventRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := memoryAgentScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	events := view.unit.state.events[scan.Task]
	sequences := make([]uint64, 0, len(events))
	for sequence := range events {
		if sequence > scan.After {
			sequences = append(sequences, sequence)
		}
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	if len(sequences) > scan.Limit {
		sequences = sequences[:scan.Limit]
	}
	result := make([]agent.StoreEventRecord, 0, len(sequences))
	for _, sequence := range sequences {
		result = append(result, events[sequence])
	}
	return append([]agent.StoreEventRecord(nil), result...), nil
}

func (view memoryAgentView) ScanLeases(scan agent.StoreLeaseScan) ([]agent.LeaseAssignment, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := memoryAgentScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	result := make([]agent.LeaseAssignment, 0)
	for ref, record := range view.unit.state.tasks {
		if ref.Workspace.Authority != scan.Authority || record.Lease.Owner != scan.Worker {
			continue
		}
		grant, ok := view.unit.state.leaseGrants[memoryAgentLeaseKey{task: ref, fence: record.Lease.Fence}]
		if !ok || grant.Grant.Worker != scan.Worker {
			return nil, fmt.Errorf("%w: current Agent lease differs from durable grant", agent.ErrCorruptStore)
		}
		assignment := agent.LeaseAssignment{Grant: grant.Grant, Task: cloneMemoryAgentTask(record.Task), GrantedAt: grant.GrantedAt}
		if memoryAgentLeaseAfterCursor(assignment, scan.After) {
			result = append(result, assignment)
		}
	}
	sort.Slice(result, func(i, j int) bool { return memoryAgentLeaseLess(result[i], result[j]) })
	if len(result) > scan.Limit {
		result = result[:scan.Limit]
	}
	return cloneMemoryAgentLeases(result), nil
}

func (tx memoryAgentTx) InsertCommand(record agent.StoreCommandRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	key := agent.StoreCommandKey{Authority: record.Authority, ID: record.ID}
	if _, exists := tx.unit.state.commands[key]; exists {
		return memoryAgentConflict(agent.StoreConstraintCommandID, record.ID)
	}
	tx.unit.state.commands[key] = cloneMemoryAgentCommand(record)
	return nil
}

func (tx memoryAgentTx) InsertTask(record agent.StoreTaskRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if _, exists := tx.unit.state.tasks[record.Task.Ref]; exists {
		return memoryAgentConflict(agent.StoreConstraintTaskKey, record.Task.Ref.ID)
	}
	for ref := range tx.unit.state.tasks {
		if ref.Workspace.Authority == record.Task.Ref.Workspace.Authority && ref.ID == record.Task.Ref.ID {
			return memoryAgentConflict(agent.StoreConstraintPublicTaskID, record.Task.Ref.ID)
		}
	}
	tx.unit.state.tasks[record.Task.Ref] = cloneMemoryAgentTaskRecord(record)
	return nil
}

func (tx memoryAgentTx) CompareAndSwapTask(mutation agent.StoreTaskMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	current, exists := tx.unit.state.tasks[mutation.Ref]
	if !exists {
		return false, memoryAgentNotFound(agent.StoreRecordTask, mutation.Ref.ID)
	}
	if mutation.Ref != mutation.Next.Task.Ref || mutation.Condition.ExpectedRevision == 0 {
		return false, fmt.Errorf("%w: invalid Agent Store Task mutation identity", agent.ErrInvalidArgument)
	}
	if current.Task.Revision != mutation.Condition.ExpectedRevision ||
		(mutation.Condition.ExpectedLease != nil && current.Lease != *mutation.Condition.ExpectedLease) {
		return false, nil
	}
	if current.Task.Context != mutation.Next.Task.Context || !current.Task.CreatedAt.Equal(mutation.Next.Task.CreatedAt) {
		return false, fmt.Errorf("%w: Agent Task mutation changes immutable metadata", agent.ErrInvalidArgument)
	}
	tx.unit.state.tasks[mutation.Ref] = cloneMemoryAgentTaskRecord(mutation.Next)
	return true, nil
}

func (tx memoryAgentTx) InsertMessage(record agent.StoreMessageRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	key := agent.StoreMessageKey{Authority: record.Task.Workspace.Authority, ID: record.ID}
	if _, exists := tx.unit.state.messages[key]; exists {
		return memoryAgentConflict(agent.StoreConstraintMessageID, record.ID)
	}
	for _, existing := range tx.unit.state.messages {
		if existing.Task == record.Task && existing.Sequence == record.Sequence {
			return memoryAgentConflict(agent.StoreConstraintMessageSequence, record.Sequence)
		}
	}
	tx.unit.state.messages[key] = cloneMemoryAgentMessage(record)
	return nil
}

func (tx memoryAgentTx) InsertEvent(record agent.StoreEventRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	event := record.Event
	if tx.unit.state.events[event.Task] == nil {
		tx.unit.state.events[event.Task] = make(map[uint64]agent.StoreEventRecord)
	}
	if _, exists := tx.unit.state.events[event.Task][event.Sequence]; exists {
		return memoryAgentConflict(agent.StoreConstraintEventSequence, event.Sequence)
	}
	tx.unit.state.events[event.Task][event.Sequence] = record
	return nil
}

func (tx memoryAgentTx) InsertLeaseGrant(record agent.StoreLeaseGrantRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	key := memoryAgentLeaseKey{task: record.Grant.Task, fence: record.Grant.Fence}
	if _, exists := tx.unit.state.leaseGrants[key]; exists {
		return memoryAgentConflict(agent.StoreConstraintLeaseFence, record.Grant.Fence)
	}
	tx.unit.state.leaseGrants[key] = record
	return nil
}

func (tx memoryAgentTx) InsertArtifact(record agent.StoreArtifactRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if _, exists := tx.unit.state.artifacts[record.Artifact.Ref]; exists {
		return memoryAgentConflict(agent.StoreConstraintArtifactID, record.Artifact.Ref.ID)
	}
	for _, existing := range tx.unit.state.artifacts {
		if existing.Artifact.Ref.Workspace.Authority == record.Artifact.Ref.Workspace.Authority &&
			existing.Artifact.Ref.ID == record.Artifact.Ref.ID {
			return memoryAgentConflict(agent.StoreConstraintArtifactID, record.Artifact.Ref.ID)
		}
		if existing.Artifact.Task == record.Artifact.Task {
			return memoryAgentConflict(agent.StoreConstraintArtifactTask, record.Artifact.Task.ID)
		}
	}
	tx.unit.state.artifacts[record.Artifact.Ref] = cloneMemoryAgentArtifact(record)
	return nil
}

func (tx memoryAgentTx) CompareAndSwapArtifact(mutation agent.StoreArtifactMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	if mutation.Task.Workspace != mutation.Ref.Workspace {
		return false, fmt.Errorf("%w: Agent Artifact mutation Task belongs to another Workspace", agent.ErrInvalidArgument)
	}
	current, exists := tx.unit.state.artifacts[mutation.Ref]
	if !exists || current.Artifact.Task != mutation.Task || current.Artifact.State != mutation.ExpectedState {
		return false, nil
	}
	current.Artifact.State = mutation.NextState
	current.Artifact.PublishedAt = cloneMemoryAgentTime(mutation.PublishedAt)
	current.Artifact.DiscardedAt = cloneMemoryAgentTime(mutation.DiscardedAt)
	tx.unit.state.artifacts[mutation.Ref] = current
	return true, nil
}

func (tx memoryAgentTx) InsertSubmission(record agent.StoreSubmissionRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if _, exists := tx.unit.state.submissions[record.Submission.Task]; exists {
		return memoryAgentConflict(agent.StoreConstraintSubmissionID, record.Submission.ID)
	}
	for _, existing := range tx.unit.state.submissions {
		if existing.Submission.Task.Workspace.Authority == record.Submission.Task.Workspace.Authority &&
			existing.Submission.ID == record.Submission.ID {
			return memoryAgentConflict(agent.StoreConstraintSubmissionID, record.Submission.ID)
		}
	}
	tx.unit.state.submissions[record.Submission.Task] = cloneMemoryAgentSubmissionRecord(record)
	return nil
}

func (tx memoryAgentTx) InsertWorkspaceHead(record agent.StoreWorkspaceHeadRecord) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	if _, exists := tx.unit.state.workspaceHeads[record.Head.Workspace]; exists {
		return false, nil
	}
	tx.unit.state.workspaceHeads[record.Head.Workspace] = record
	return true, nil
}

func (tx memoryAgentTx) CompareAndSwapWorkspaceHead(mutation agent.StoreWorkspaceHeadMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	if mutation.Next.Head.Workspace != mutation.Workspace {
		return false, fmt.Errorf("%w: Workspace head mutation identity mismatch", agent.ErrInvalidArgument)
	}
	current, exists := tx.unit.state.workspaceHeads[mutation.Workspace]
	if !exists || current.Head.ConfirmedRevision != mutation.ExpectedRevision {
		return false, nil
	}
	tx.unit.state.workspaceHeads[mutation.Workspace] = mutation.Next
	return true, nil
}

func (tx memoryAgentTx) InsertApplyReceipt(record agent.StoreApplyReceiptRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if _, exists := tx.unit.state.applyReceipts[record.Receipt.Artifact]; exists {
		return memoryAgentConflict(agent.StoreConstraintReceiptID, record.Receipt.ID)
	}
	for _, existing := range tx.unit.state.applyReceipts {
		if existing.Receipt.Artifact.Workspace.Authority == record.Receipt.Artifact.Workspace.Authority &&
			existing.Receipt.ID == record.Receipt.ID {
			return memoryAgentConflict(agent.StoreConstraintReceiptID, record.Receipt.ID)
		}
	}
	tx.unit.state.applyReceipts[record.Receipt.Artifact] = record
	return nil
}

func memoryAgentTaskCreatedLess(left, right agent.StoreTaskRecord) bool {
	if !left.Task.CreatedAt.Equal(right.Task.CreatedAt) {
		return left.Task.CreatedAt.Before(right.Task.CreatedAt)
	}
	if left.Task.Ref.Workspace.ID != right.Task.Ref.Workspace.ID {
		return left.Task.Ref.Workspace.ID < right.Task.Ref.Workspace.ID
	}
	return left.Task.Ref.ID < right.Task.Ref.ID
}

func memoryAgentTaskUpdatedLess(left, right agent.StoreTaskRecord) bool {
	if !left.Task.UpdatedAt.Equal(right.Task.UpdatedAt) {
		return left.Task.UpdatedAt.After(right.Task.UpdatedAt)
	}
	if left.Task.Ref.Workspace.ID != right.Task.Ref.Workspace.ID {
		return left.Task.Ref.Workspace.ID < right.Task.Ref.Workspace.ID
	}
	return left.Task.Ref.ID < right.Task.Ref.ID
}

func memoryAgentTaskAfterContextCursor(task agent.Task, cursor *agent.TaskPageCursor) bool {
	if cursor == nil {
		return true
	}
	if !task.CreatedAt.Equal(cursor.CreatedAt) {
		return task.CreatedAt.After(cursor.CreatedAt)
	}
	if task.Ref.Workspace.ID != cursor.Workspace {
		return task.Ref.Workspace.ID > cursor.Workspace
	}
	return task.Ref.ID > cursor.Task
}

func memoryAgentTaskAfterAuthorityCursor(task agent.Task, cursor *agent.TaskQueryCursor) bool {
	if cursor == nil {
		return true
	}
	if !task.UpdatedAt.Equal(cursor.UpdatedAt) {
		return task.UpdatedAt.Before(cursor.UpdatedAt)
	}
	if task.Ref.Workspace.ID != cursor.Workspace {
		return task.Ref.Workspace.ID > cursor.Workspace
	}
	return task.Ref.ID > cursor.Task
}

func memoryAgentLeaseLess(left, right agent.LeaseAssignment) bool {
	if !left.GrantedAt.Equal(right.GrantedAt) {
		return left.GrantedAt.Before(right.GrantedAt)
	}
	if left.Task.Ref.Workspace.ID != right.Task.Ref.Workspace.ID {
		return left.Task.Ref.Workspace.ID < right.Task.Ref.Workspace.ID
	}
	if left.Task.Ref.ID != right.Task.Ref.ID {
		return left.Task.Ref.ID < right.Task.Ref.ID
	}
	return left.Grant.Fence < right.Grant.Fence
}

func memoryAgentLeaseAfterCursor(assignment agent.LeaseAssignment, cursor *agent.LeasePageCursor) bool {
	if cursor == nil {
		return true
	}
	if !assignment.GrantedAt.Equal(cursor.GrantedAt) {
		return assignment.GrantedAt.After(cursor.GrantedAt)
	}
	if assignment.Task.Ref.Workspace.ID != cursor.Workspace {
		return assignment.Task.Ref.Workspace.ID > cursor.Workspace
	}
	if assignment.Task.Ref.ID != cursor.Task {
		return assignment.Task.Ref.ID > cursor.Task
	}
	return assignment.Grant.Fence > cursor.Fence
}

func memoryAgentReadLimit(kind agent.StoreRecordKind, limit agent.StoreReadLimit) error {
	if limit.MaxBytes < 1 {
		return memoryAgentLimit(kind, limit.MaxBytes)
	}
	return nil
}

func memoryAgentScanLimit(limit int) error {
	if limit < 1 || limit > agent.MaxPageSize+1 {
		return fmt.Errorf("%w: Agent Store scan limit must be 1..%d", agent.ErrInvalidArgument, agent.MaxPageSize+1)
	}
	return nil
}

func memoryAgentNotFound(kind agent.StoreRecordKind, key any) error {
	return &agent.StoreNotFoundError{Record: kind, Key: fmt.Sprint(key)}
}

func memoryAgentConflict(constraint agent.StoreConstraint, key any) error {
	return &agent.StoreConflictError{Constraint: constraint, Key: fmt.Sprint(key)}
}

func memoryAgentLimit(kind agent.StoreRecordKind, limit int64) error {
	return &agent.StoreLimitError{Record: kind, Limit: limit}
}

func cloneMemoryAgentCommand(record agent.StoreCommandRecord) agent.StoreCommandRecord {
	record.Result = bytes.Clone(record.Result)
	return record
}

func cloneMemoryAgentTask(task agent.Task) agent.Task {
	if task.Artifact != nil {
		value := *task.Artifact
		task.Artifact = &value
	}
	if task.Submission != nil {
		value := *task.Submission
		if value.Artifact != nil {
			artifact := *value.Artifact
			value.Artifact = &artifact
		}
		task.Submission = &value
	}
	return task
}

func cloneMemoryAgentTaskRecord(record agent.StoreTaskRecord) agent.StoreTaskRecord {
	record.Task = cloneMemoryAgentTask(record.Task)
	if record.ArtifactState != nil {
		value := *record.ArtifactState
		record.ArtifactState = &value
	}
	return record
}

func cloneMemoryAgentTasks(records []agent.StoreTaskRecord) []agent.StoreTaskRecord {
	cloned := make([]agent.StoreTaskRecord, len(records))
	for index := range records {
		cloned[index] = cloneMemoryAgentTaskRecord(records[index])
	}
	return cloned
}

func cloneMemoryAgentMessage(record agent.StoreMessageRecord) agent.StoreMessageRecord {
	record.EncodedParts = bytes.Clone(record.EncodedParts)
	return record
}

func cloneMemoryAgentMessages(records []agent.StoreMessageRecord) []agent.StoreMessageRecord {
	cloned := make([]agent.StoreMessageRecord, len(records))
	for index := range records {
		cloned[index] = cloneMemoryAgentMessage(records[index])
	}
	return cloned
}

func cloneMemoryAgentArtifact(record agent.StoreArtifactRecord) agent.StoreArtifactRecord {
	record.EncodedPayload = bytes.Clone(record.EncodedPayload)
	record.Artifact.PublishedAt = cloneMemoryAgentTime(record.Artifact.PublishedAt)
	record.Artifact.DiscardedAt = cloneMemoryAgentTime(record.Artifact.DiscardedAt)
	return record
}

func cloneMemoryAgentSubmissionRecord(record agent.StoreSubmissionRecord) agent.StoreSubmissionRecord {
	if record.Submission.Artifact != nil {
		value := *record.Submission.Artifact
		record.Submission.Artifact = &value
	}
	return record
}

func cloneMemoryAgentTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneMemoryAgentLeases(assignments []agent.LeaseAssignment) []agent.LeaseAssignment {
	cloned := make([]agent.LeaseAssignment, len(assignments))
	for index := range assignments {
		cloned[index] = assignments[index]
		cloned[index].Task = cloneMemoryAgentTask(assignments[index].Task)
	}
	return cloned
}

var _ agent.Store = (*MemoryAgentStore)(nil)
var _ agent.StoreView = memoryAgentView{}
var _ agent.StoreTx = memoryAgentTx{}
