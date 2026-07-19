package humantest_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
)

func TestAgentStoreConformanceSuiteAgainstMemoryModel(t *testing.T) {
	humantest.TestAgentStore(t, func(
		context.Context,
		testing.TB,
	) (agent.Store, framework.ReleaseFunc, error) {
		store := newMemoryAgentStore()
		return store, func(context.Context) error {
			store.close()
			return nil
		}, nil
	})
}

// memoryAgentStore is intentionally small and test-only. It is a semantic
// model used to execute the reusable suite before a physical provider is
// available; it is not offered as a production Store implementation.
type memoryAgentStore struct {
	mu     sync.RWMutex
	closed bool
	state  memoryAgentState
}

type memoryAgentState struct {
	commands  map[agent.StoreCommandKey]agent.StoreCommandRecord
	tasks     map[agent.TaskRef]agent.StoreTaskRecord
	messages  map[agent.StoreMessageKey]agent.StoreMessageRecord
	artifacts map[agent.ArtifactRef]agent.StoreArtifactRecord
}

func newMemoryAgentStore() *memoryAgentStore {
	return &memoryAgentStore{state: newMemoryAgentState()}
}

func newMemoryAgentState() memoryAgentState {
	return memoryAgentState{
		commands:  make(map[agent.StoreCommandKey]agent.StoreCommandRecord),
		tasks:     make(map[agent.TaskRef]agent.StoreTaskRecord),
		messages:  make(map[agent.StoreMessageKey]agent.StoreMessageRecord),
		artifacts: make(map[agent.ArtifactRef]agent.StoreArtifactRecord),
	}
}

func (store *memoryAgentStore) Description() agent.StoreDescription {
	return agent.StoreDescription{
		Contract: framework.Contract{ID: agent.StoreContractID, Major: agent.StoreContractMajor},
		Provider: "humantest-memory-model",
		Version:  "1",
	}
}

func (store *memoryAgentStore) View(ctx context.Context, callback func(agent.StoreView) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return agent.ErrStoreClosed
	}
	return callback(memoryAgentView{state: &store.state})
}

func (store *memoryAgentStore) Update(ctx context.Context, callback func(agent.StoreTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return agent.ErrStoreClosed
	}
	next := store.state.clone()
	if err := callback(memoryAgentTx{memoryAgentView{state: &next}}); err != nil {
		return err
	}
	store.state = next
	return nil
}

func (store *memoryAgentStore) close() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
}

func (state memoryAgentState) clone() memoryAgentState {
	cloned := newMemoryAgentState()
	for key, record := range state.commands {
		cloned.commands[key] = cloneCommand(record)
	}
	for key, record := range state.tasks {
		cloned.tasks[key] = cloneTask(record)
	}
	for key, record := range state.messages {
		cloned.messages[key] = cloneMessage(record)
	}
	for key, record := range state.artifacts {
		cloned.artifacts[key] = cloneArtifact(record)
	}
	return cloned
}

type memoryAgentView struct {
	state *memoryAgentState
}

func (view memoryAgentView) LookupCommand(
	key agent.StoreCommandKey,
	limit agent.StoreReadLimit,
) (agent.StoreCommandRecord, error) {
	record, ok := view.state.commands[key]
	if !ok {
		return agent.StoreCommandRecord{}, notFound(agent.StoreRecordCommand, string(key.ID))
	}
	if err := enforceLimit(agent.StoreRecordCommand, len(record.Result), limit); err != nil {
		return agent.StoreCommandRecord{}, err
	}
	return cloneCommand(record), nil
}

func (view memoryAgentView) LoadTask(ref agent.TaskRef) (agent.StoreTaskRecord, error) {
	record, ok := view.state.tasks[ref]
	if !ok {
		return agent.StoreTaskRecord{}, notFound(agent.StoreRecordTask, string(ref.ID))
	}
	return cloneTask(record), nil
}

func (view memoryAgentView) ResolveTask(authority agent.AuthorityID, id agent.TaskID) (agent.TaskRef, error) {
	for ref := range view.state.tasks {
		if ref.Workspace.Authority == authority && ref.ID == id {
			return ref, nil
		}
	}
	return agent.TaskRef{}, notFound(agent.StoreRecordTask, string(id))
}

func (view memoryAgentView) LoadMessage(
	key agent.StoreMessageKey,
	limit agent.StoreReadLimit,
) (agent.StoreMessageRecord, error) {
	record, ok := view.state.messages[key]
	if !ok {
		return agent.StoreMessageRecord{}, notFound(agent.StoreRecordMessage, string(key.ID))
	}
	if err := enforceLimit(agent.StoreRecordMessage, len(record.EncodedParts), limit); err != nil {
		return agent.StoreMessageRecord{}, err
	}
	return cloneMessage(record), nil
}

func (view memoryAgentView) LoadArtifact(
	ref agent.ArtifactRef,
	limit agent.StoreReadLimit,
) (agent.StoreArtifactRecord, error) {
	record, ok := view.state.artifacts[ref]
	if !ok {
		return agent.StoreArtifactRecord{}, notFound(agent.StoreRecordArtifact, string(ref.ID))
	}
	if err := enforceLimit(agent.StoreRecordArtifact, len(record.Content.Payload.Data), limit); err != nil {
		return agent.StoreArtifactRecord{}, err
	}
	return cloneArtifact(record), nil
}

func (memoryAgentView) LoadApplyReceipt(agent.ArtifactRef) (agent.StoreApplyReceiptRecord, error) {
	return agent.StoreApplyReceiptRecord{}, notFound(agent.StoreRecordApplyReceipt, "")
}

func (memoryAgentView) LoadWorkspaceHead(agent.WorkspaceRef) (agent.StoreWorkspaceHeadRecord, error) {
	return agent.StoreWorkspaceHeadRecord{}, notFound(agent.StoreRecordWorkspaceHead, "")
}

func (memoryAgentView) LoadLeaseGrant(agent.TaskRef, agent.LeaseFence) (agent.StoreLeaseGrantRecord, error) {
	return agent.StoreLeaseGrantRecord{}, notFound(agent.StoreRecordLeaseGrant, "")
}

func (memoryAgentView) LoadLatestLeaseGrant(agent.TaskRef) (agent.StoreLeaseGrantRecord, error) {
	return agent.StoreLeaseGrantRecord{}, notFound(agent.StoreRecordLeaseGrant, "")
}

func (memoryAgentView) FindClaimableTask(agent.AuthorityID) (agent.StoreTaskRecord, error) {
	return agent.StoreTaskRecord{}, notFound(agent.StoreRecordTask, "")
}

func (memoryAgentView) ScanContextTasks(agent.StoreTaskContextScan) ([]agent.StoreTaskRecord, error) {
	return nil, nil
}

func (memoryAgentView) ScanAuthorityTasks(agent.StoreTaskAuthorityScan) (agent.StoreTaskAuthorityResult, error) {
	return agent.StoreTaskAuthorityResult{}, nil
}

func (memoryAgentView) ScanMessages(agent.StoreMessageScan) ([]agent.StoreMessageRecord, error) {
	return nil, nil
}

func (memoryAgentView) ScanEvents(agent.StoreEventScan) ([]agent.StoreEventRecord, error) {
	return nil, nil
}

func (memoryAgentView) ScanLeases(agent.StoreLeaseScan) ([]agent.LeaseAssignment, error) {
	return nil, nil
}

type memoryAgentTx struct {
	memoryAgentView
}

func (tx memoryAgentTx) InsertCommand(record agent.StoreCommandRecord) error {
	key := agent.StoreCommandKey{Authority: record.Authority, ID: record.ID}
	if _, exists := tx.state.commands[key]; exists {
		return conflict(agent.StoreConstraintCommandID, string(record.ID))
	}
	tx.state.commands[key] = cloneCommand(record)
	return nil
}

func (tx memoryAgentTx) InsertTask(record agent.StoreTaskRecord) error {
	if _, exists := tx.state.tasks[record.Task.Ref]; exists {
		return conflict(agent.StoreConstraintTaskKey, string(record.Task.Ref.ID))
	}
	tx.state.tasks[record.Task.Ref] = cloneTask(record)
	return nil
}

func (tx memoryAgentTx) CompareAndSwapTask(mutation agent.StoreTaskMutation) (bool, error) {
	current, exists := tx.state.tasks[mutation.Ref]
	if !exists {
		return false, notFound(agent.StoreRecordTask, string(mutation.Ref.ID))
	}
	if current.Task.Revision != mutation.Condition.ExpectedRevision {
		return false, nil
	}
	if mutation.Condition.ExpectedLease != nil && current.Lease != *mutation.Condition.ExpectedLease {
		return false, nil
	}
	if mutation.Next.Task.Ref != mutation.Ref {
		return false, errors.New("memory model rejects a Task key mutation")
	}
	tx.state.tasks[mutation.Ref] = cloneTask(mutation.Next)
	return true, nil
}

func (tx memoryAgentTx) InsertMessage(record agent.StoreMessageRecord) error {
	key := agent.StoreMessageKey{Authority: record.Task.Workspace.Authority, ID: record.ID}
	if _, exists := tx.state.messages[key]; exists {
		return conflict(agent.StoreConstraintMessageID, string(record.ID))
	}
	tx.state.messages[key] = cloneMessage(record)
	return nil
}

func (memoryAgentTx) InsertEvent(agent.StoreEventRecord) error {
	return errors.New("memory model Event primitive is outside this suite version")
}

func (memoryAgentTx) InsertLeaseGrant(agent.StoreLeaseGrantRecord) error {
	return errors.New("memory model LeaseGrant primitive is outside this suite version")
}

func (tx memoryAgentTx) InsertArtifact(record agent.StoreArtifactRecord) error {
	ref := record.Content.Artifact.Ref
	if _, exists := tx.state.artifacts[ref]; exists {
		return conflict(agent.StoreConstraintArtifactID, string(ref.ID))
	}
	tx.state.artifacts[ref] = cloneArtifact(record)
	return nil
}

func (memoryAgentTx) CompareAndSwapArtifact(agent.StoreArtifactMutation) (bool, error) {
	return false, errors.New("memory model Artifact CAS primitive is outside this suite version")
}

func (memoryAgentTx) InsertSubmission(agent.StoreSubmissionRecord) error {
	return errors.New("memory model Submission primitive is outside this suite version")
}

func (memoryAgentTx) InsertWorkspaceHead(agent.StoreWorkspaceHeadRecord) (bool, error) {
	return false, errors.New("memory model WorkspaceHead primitive is outside this suite version")
}

func (memoryAgentTx) CompareAndSwapWorkspaceHead(agent.StoreWorkspaceHeadMutation) (bool, error) {
	return false, errors.New("memory model WorkspaceHead CAS primitive is outside this suite version")
}

func (memoryAgentTx) InsertApplyReceipt(agent.StoreApplyReceiptRecord) error {
	return errors.New("memory model ApplyReceipt primitive is outside this suite version")
}

func enforceLimit(kind agent.StoreRecordKind, size int, limit agent.StoreReadLimit) error {
	if limit.MaxBytes <= 0 || int64(size) > limit.MaxBytes {
		return &agent.StoreLimitError{Record: kind, Limit: limit.MaxBytes}
	}
	return nil
}

func notFound(kind agent.StoreRecordKind, key string) error {
	return &agent.StoreNotFoundError{Record: kind, Key: key}
}

func conflict(constraint agent.StoreConstraint, key string) error {
	return &agent.StoreConflictError{Constraint: constraint, Key: key}
}

func cloneCommand(record agent.StoreCommandRecord) agent.StoreCommandRecord {
	record.Result = append([]byte(nil), record.Result...)
	return record
}

func cloneTask(record agent.StoreTaskRecord) agent.StoreTaskRecord {
	if record.Task.Artifact != nil {
		value := *record.Task.Artifact
		record.Task.Artifact = &value
	}
	if record.Task.Submission != nil {
		value := *record.Task.Submission
		if value.Artifact != nil {
			artifact := *value.Artifact
			value.Artifact = &artifact
		}
		record.Task.Submission = &value
	}
	if record.ArtifactState != nil {
		value := *record.ArtifactState
		record.ArtifactState = &value
	}
	return record
}

func cloneMessage(record agent.StoreMessageRecord) agent.StoreMessageRecord {
	record.EncodedParts = append([]byte(nil), record.EncodedParts...)
	return record
}

func cloneArtifact(record agent.StoreArtifactRecord) agent.StoreArtifactRecord {
	record.Content.Payload.Data = append([]byte(nil), record.Content.Payload.Data...)
	if record.Content.Artifact.PublishedAt != nil {
		value := *record.Content.Artifact.PublishedAt
		record.Content.Artifact.PublishedAt = &value
	}
	if record.Content.Artifact.DiscardedAt != nil {
		value := *record.Content.Artifact.DiscardedAt
		record.Content.Artifact.DiscardedAt = &value
	}
	return record
}

var _ agent.Store = (*memoryAgentStore)(nil)
var _ agent.StoreView = memoryAgentView{}
var _ agent.StoreTx = memoryAgentTx{}
