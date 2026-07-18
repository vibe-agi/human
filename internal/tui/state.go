package tui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerstate"
)

const (
	workerStateDraftKind        = "tui_draft_v2"
	workerStateContinuationKind = "tui_continuation_v2"
	workerStatePendingSendKind  = "tui_pending_send_v2"
	workerStateVersion          = 2
	workerStateLoadTimeout      = 2 * time.Second
	workerStateWriteTimeout     = 5 * time.Second
	workerStateRetryMin         = 250 * time.Millisecond
	workerStateRetryMax         = 5 * time.Second
	maxPersistedInputBytes      = 8 << 20
)

// StateStore is the worker-local persistence boundary used by the TUI. The
// concrete workerstate.Store is owned and closed by the CLI, while this small
// interface keeps model recovery tests deterministic.
type StateStore interface {
	Bind(context.Context, string, string) error
	Put(context.Context, workerstate.Scope, string, json.RawMessage) (workerstate.Record, error)
	Delete(context.Context, workerstate.Scope, string) error
	List(context.Context) ([]workerstate.Record, error)
}

// WithStateStore enables durable TUI drafts and continuation recovery. A nil
// store is equivalent to leaving persistence disabled.
func WithStateStore(store StateStore) Option {
	return func(model *Model) { model.stateStore = store }
}

func (model *Model) bindAndLoadWorkerState(identity workerclient.Identity) error {
	if model.stateStore == nil {
		return nil
	}
	if strings.TrimSpace(identity.GatewayID) == "" || strings.TrimSpace(identity.WorkerID) == "" {
		return errors.New("authenticated gateway and worker identity are required")
	}
	if model.stateBound && model.stateIdentity != identity {
		return workerstate.ErrIdentityConflict
	}
	ctx, cancel := context.WithTimeout(context.Background(), workerStateLoadTimeout)
	defer cancel()
	if err := model.stateStore.Bind(ctx, identity.GatewayID, identity.WorkerID); err != nil {
		return err
	}
	model.stateBound = true
	model.stateIdentity = identity
	model.loadWorkerState()
	return nil
}

type stateRecordKey struct {
	scope workerstate.Scope
	kind  string
}

type savedStateDraft struct {
	draft     persistedDraft
	updatedAt time.Time
	revision  int64
}

type persistedDraft struct {
	Version       int             `json:"version"`
	Focus         string          `json:"focus,omitempty"`
	Reply         string          `json:"reply,omitempty"`
	Command       string          `json:"command,omitempty"`
	HasTasks      bool            `json:"has_tasks,omitempty"`
	Tasks         []persistedTask `json:"tasks,omitempty"`
	TaskSelected  int             `json:"task_selected,omitempty"`
	TaskDirty     bool            `json:"task_dirty,omitempty"`
	TaskEditing   bool            `json:"task_editing,omitempty"`
	TaskEditIndex int             `json:"task_edit_index,omitempty"`
	TaskInput     string          `json:"task_input,omitempty"`
	ToolInput     string          `json:"tool_input,omitempty"`
	ToolCallIDs   []string        `json:"tool_call_ids,omitempty"`
}

type persistedTask struct {
	Content  string          `json:"content"`
	Status   agentTaskStatus `json:"status"`
	Priority string          `json:"priority"`
}

type persistedContinuation struct {
	Version int                    `json:"version"`
	IDs     []string               `json:"tool_call_ids,omitempty"`
	Handoff bool                   `json:"handoff,omitempty"`
	Context *completion.Assignment `json:"context,omitempty"`
}

const (
	pendingSendDispositionSend    = "send"
	pendingSendDispositionRestore = "restore"
)

type persistedPendingSend struct {
	Version     int                    `json:"version"`
	Disposition string                 `json:"disposition"`
	Kind        pendingSendKind        `json:"kind"`
	Assignment  completion.Assignment  `json:"assignment"`
	Event       completion.Event       `json:"event"`
	Context     *completion.Assignment `json:"context,omitempty"`
	Draft       persistedDraft         `json:"draft"`
	Remaining   persistedDraft         `json:"remaining_draft"`
	ToolCalls   []completion.ToolCall  `json:"tool_calls,omitempty"`
}

type savedPendingSend struct {
	pending        pendingSend
	disposition    string
	updatedAt      time.Time
	intentRevision int64
}

type stateWriteOperation struct {
	key     stateRecordKey
	payload json.RawMessage
	delete  bool
}

type stateWriteResult struct {
	operation stateWriteOperation
	err       error
}

type stateRetryReady struct{}

const (
	persistedFocusTasks   = "tasks"
	persistedFocusReply   = "reply"
	persistedFocusCommand = "command"
)

func (model *Model) loadWorkerState() {
	if model.stateStore == nil || !model.stateBound || model.stateLoaded {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), workerStateLoadTimeout)
	defer cancel()
	records, listErr := model.stateStore.List(ctx)
	corruptRows := 0
	if listErr != nil {
		var corrupt *workerstate.CorruptRecordsError
		if errors.As(listErr, &corrupt) {
			corruptRows = len(corrupt.Records)
		} else {
			model.stateLoadWarning = "recovery state unavailable: " + listErr.Error()
			return
		}
	}
	model.stateLoaded = true

	continuations := make([]continuationState, 0)
	allPendingSends := make([]savedPendingSend, 0)
	decodeErrors := 0
	for _, record := range records {
		key := stateRecordKey{scope: record.Scope, kind: record.Kind}
		switch record.Kind {
		case workerStateDraftKind:
			var draft persistedDraft
			if err := json.Unmarshal(record.Payload, &draft); err != nil || validatePersistedDraft(draft) != nil {
				decodeErrors++
				continue
			}
			model.stateManaged[key] = struct{}{}
			model.stateSynced[key] = string(record.Payload)
			model.stateDrafts[key] = savedStateDraft{
				draft: sanitizePersistedDraft(draft), updatedAt: record.UpdatedAt, revision: record.Revision,
			}
		case workerStateContinuationKind:
			var persisted persistedContinuation
			if err := json.Unmarshal(record.Payload, &persisted); err != nil ||
				validatePersistedContinuation(record.Scope, persisted) != nil {
				decodeErrors++
				continue
			}
			model.stateManaged[key] = struct{}{}
			model.stateSynced[key] = string(record.Payload)
			continuations = append(continuations, continuationFromPersisted(record.Scope, persisted))
		case workerStatePendingSendKind:
			var persisted persistedPendingSend
			if err := json.Unmarshal(record.Payload, &persisted); err != nil ||
				validatePersistedPendingSend(record.Scope, persisted) != nil {
				decodeErrors++
				continue
			}
			model.stateManaged[key] = struct{}{}
			model.stateSynced[key] = string(record.Payload)
			allPendingSends = append(allPendingSends, savedPendingSend{
				pending: pendingSendFromPersisted(persisted), disposition: persisted.Disposition,
				updatedAt: record.UpdatedAt, intentRevision: record.CreatedRevision,
			})
		default:
			// A newer binary may own this kind. Leave it untouched rather than
			// treating forward-compatible state as garbage.
		}
	}
	sort.SliceStable(allPendingSends, func(left, right int) bool {
		return allPendingSends[left].intentRevision < allPendingSends[right].intentRevision
	})
	pendingSends := make([]savedPendingSend, 0, len(allPendingSends))
	for _, saved := range allPendingSends {
		model.reconcilePendingDraft(saved)
		if saved.disposition == pendingSendDispositionSend {
			pendingSends = append(pendingSends, saved)
		}
	}

	if len(model.stateDrafts) > maxRejectedDraftScopes {
		type orderedDraft struct {
			key       stateRecordKey
			updatedAt time.Time
			revision  int64
		}
		ordered := make([]orderedDraft, 0, len(model.stateDrafts))
		for key, saved := range model.stateDrafts {
			ordered = append(ordered, orderedDraft{key: key, updatedAt: saved.updatedAt, revision: saved.revision})
		}
		sort.Slice(ordered, func(left, right int) bool {
			if ordered[left].revision == ordered[right].revision {
				return stateKeyLess(ordered[left].key, ordered[right].key)
			}
			return ordered[left].revision < ordered[right].revision
		})
		overflow := len(ordered) - maxRejectedDraftScopes
		for _, saved := range ordered[:overflow] {
			delete(model.stateDrafts, saved.key)
		}
		decodeErrors += overflow
	}
	if len(continuations) > maxParkedContinuations {
		continuations = continuations[len(continuations)-maxParkedContinuations:]
		decodeErrors++
	}
	if len(continuations) > 0 {
		latest := continuations[len(continuations)-1]
		model.parkedContinuations = append([]continuationState(nil), continuations[:len(continuations)-1]...)
		model.loadContinuation(latest)
		model.lastContext = cloneAssignment(latest.context)
		model.status = "recovered unfinished continuation · waiting for the client Agent"
	}
	if len(pendingSends) > 0 {
		for _, saved := range pendingSends {
			model.recoveredSessions[saved.pending.assignment.SessionKey()] = struct{}{}
		}
		model.pending = pendingSends[0].pending
		for _, saved := range pendingSends[1:] {
			model.pendingRecoveries = append(model.pendingRecoveries, saved.pending)
		}
		model.activateRecoveredPending(model.pending)
		model.status = "recovered a locally committed response event · resuming durable outbox handoff"
	}
	if count := corruptRows + decodeErrors; count > 0 {
		model.stateLoadWarning = fmt.Sprintf("ignored %d corrupt recovery record(s)", count)
	}
}

func (model *Model) activateRecoveredPending(pending pendingSend) {
	if pending.event.Type != completion.EventProgress {
		return
	}
	assignment := pending.assignment
	model.active = &assignment
	if pending.context != nil {
		model.lastContext = cloneAssignment(pending.context)
	} else {
		model.lastContext = cloneAssignment(&assignment)
	}
	if pending.reply != "" {
		model.appendLocalText(assignment, canonical.RoleAssistant, pending.reply)
	}
	model.loadAgentTasks(assignment)
	if draft, ok := model.takePersistentDraft(assignment); ok {
		model.applyPersistentDraft(draft)
	}
	model.focus = focusReply
	model.ui.chatFollow = true
}

func (model *Model) reconcilePendingDraft(saved savedPendingSend) {
	target := stateScope(saved.pending.assignment)
	exact := stateRecordKey{scope: target, kind: workerStateDraftKind}
	var latest savedStateDraft
	found := false
	for key, draft := range model.stateDrafts {
		if key.kind != workerStateDraftKind || !recoverableDraftScope(key.scope, target) {
			continue
		}
		if !found || draft.revision > latest.revision {
			latest, found = draft, true
		}
		delete(model.stateDrafts, key)
	}

	var recovered persistedDraft
	switch {
	case found && latest.revision > saved.intentRevision:
		// This row was written after the exact intent and is a new operator tail.
		// A successful send keeps it byte-for-byte; a failed handoff restores only
		// the source pane in front of that newer tail.
		recovered = latest.draft
		if saved.disposition == pendingSendDispositionRestore {
			recovered = mergePendingSource(recovered, saved.pending)
		}
	default:
		// No draft, or a pre-intent snapshot. The pending row atomically captured
		// all non-source panes at the send boundary, so it is authoritative over a
		// stale whole-editor row.
		recovered = saved.pending.remainingDraft
		if saved.disposition == pendingSendDispositionRestore {
			recovered = mergePendingSource(recovered, saved.pending)
		}
	}
	recovered = sanitizePersistedDraft(recovered)
	if persistedDraftHasContent(recovered) {
		updatedAt := saved.updatedAt
		revision := saved.intentRevision
		if found && latest.revision > revision {
			updatedAt = latest.updatedAt
			revision = latest.revision
		}
		model.stateDrafts[exact] = savedStateDraft{draft: recovered, updatedAt: updatedAt, revision: revision}
	}
}

func mergePendingSource(tail persistedDraft, pending pendingSend) persistedDraft {
	source, _ := persistedDraftFromPending(pending)
	if tail.Version == 0 {
		tail.Version = workerStateVersion
		tail.TaskEditIndex = -1
	}
	switch pending.kind {
	case pendingReply:
		tail.Reply = joinDraftSegments(source.Reply, tail.Reply)
	case pendingCommand:
		tail.Command = joinDraftSegments(source.Command, tail.Command)
	case pendingTasks:
		if !tail.HasTasks {
			tail.HasTasks = source.HasTasks
			tail.Tasks = append([]persistedTask(nil), source.Tasks...)
			tail.TaskSelected = source.TaskSelected
			tail.TaskDirty = source.TaskDirty
			tail.TaskEditIndex = source.TaskEditIndex
		}
	case pendingAdvancedTools:
		if source.ToolInput != "" {
			if tail.ToolInput == "" {
				tail.ToolInput = source.ToolInput
			} else {
				tail.ToolInput = source.ToolInput + "\n" + tail.ToolInput
			}
			tail.ToolCallIDs = append(append([]string(nil), source.ToolCallIDs...), tail.ToolCallIDs...)
		}
	}
	if tail.Focus == "" {
		tail.Focus = pendingKindFocus(pending.kind)
	}
	return tail
}

func (model *Model) retainPendingRemainder(pending pendingSend) {
	if !persistedDraftHasContent(pending.remainingDraft) {
		return
	}
	key := stateRecordKey{scope: stateScope(pending.assignment), kind: workerStateDraftKind}
	if _, exists := model.stateDrafts[key]; exists {
		return
	}
	model.stateDrafts[key] = savedStateDraft{
		draft: sanitizePersistedDraft(pending.remainingDraft), updatedAt: time.Now().UTC(),
	}
}

func (model *Model) nextStateCommand() tea.Cmd {
	if model.stateStore == nil || !model.stateBound || model.stateWriting || model.stateRetryPending {
		return nil
	}
	desired, protected, err := model.desiredWorkerState()
	if err != nil {
		model.stateWriteWarning = "recovery state encode failed: " + err.Error()
	}
	for key := range desired {
		model.stateManaged[key] = struct{}{}
	}
	// The exact event intent is the crash-recovery authority for every editor
	// mutation below it. Never let a derived draft update overtake a newly staged
	// intent: a crash after that draft write but before the intent would silently
	// lose the pane the operator just sent.
	if model.pending.kind != pendingNone && !model.pending.durable && model.pending.event.ID != "" {
		key := pendingSendStateKey(model.pending)
		if payload, ok := desired[key]; ok && model.stateSynced[key] != string(payload) {
			operation := stateWriteOperation{key: key, payload: append(json.RawMessage(nil), payload...)}
			model.stateWriting = true
			return writeWorkerState(model.stateStore, operation)
		}
	}

	desiredKeys := sortedStateKeys(desired)
	for _, key := range desiredKeys {
		payload := desired[key]
		if model.stateSynced[key] == string(payload) {
			continue
		}
		operation := stateWriteOperation{key: key, payload: append(json.RawMessage(nil), payload...)}
		model.stateWriting = true
		return writeWorkerState(model.stateStore, operation)
	}
	managed := make(map[stateRecordKey]json.RawMessage, len(model.stateManaged))
	for key := range model.stateManaged {
		managed[key] = nil
	}
	for _, key := range sortedStateKeys(managed) {
		if _, keep := desired[key]; keep {
			continue
		}
		if _, keep := protected[key]; keep {
			continue
		}
		if _, exists := model.stateSynced[key]; !exists {
			continue
		}
		operation := stateWriteOperation{key: key, delete: true}
		model.stateWriting = true
		return writeWorkerState(model.stateStore, operation)
	}
	return nil
}

// workerStateSynchronized is the execution gate for an input gesture queued
// while a prior state transaction was in flight. nextStateCommand is called
// first and starts any missing put/delete; this predicate then proves there is
// no remaining desired-state delta before the gesture may mutate an editor.
func (model Model) workerStateSynchronized() bool {
	if model.stateStore == nil {
		return true
	}
	if !model.stateBound || model.stateWriting || model.stateRetryPending {
		return false
	}
	desired, protected, err := model.desiredWorkerState()
	if err != nil || len(protected) != 0 {
		return false
	}
	for key, payload := range desired {
		if model.stateSynced[key] != string(payload) {
			return false
		}
	}
	for key := range model.stateManaged {
		if _, keep := desired[key]; keep {
			continue
		}
		if _, exists := model.stateSynced[key]; exists {
			return false
		}
	}
	return true
}

func (model *Model) applyStateWriteResult(result stateWriteResult) tea.Cmd {
	model.stateWriting = false
	if result.err == nil {
		if result.operation.delete {
			delete(model.stateSynced, result.operation.key)
			delete(model.stateManaged, result.operation.key)
		} else {
			model.stateSynced[result.operation.key] = string(result.operation.payload)
		}
		model.stateRetryAttempt = 0
		model.stateWriteWarning = ""
		if !result.operation.delete && result.operation.key.kind == workerStatePendingSendKind &&
			model.pending.kind != pendingNone && !model.pending.durable &&
			pendingSendStateKey(model.pending) == result.operation.key {
			model.pending.durable = true
			model.status = "response event committed to recovery state · entering the durable outbox…"
			return sendPersistedEvent(model.client, model.stateStore, model.pending)
		}
		return nil
	}
	model.stateWriteWarning = "recovery state write failed; draft remains in memory: " + result.err.Error()
	model.stateRetryAttempt++
	model.stateRetryPending = true
	delay := workerStateRetryMin << min(model.stateRetryAttempt-1, 4)
	if delay > workerStateRetryMax {
		delay = workerStateRetryMax
	}
	return tea.Tick(delay, func(time.Time) tea.Msg { return stateRetryReady{} })
}

func writeWorkerState(store StateStore, operation stateWriteOperation) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), workerStateWriteTimeout)
		defer cancel()
		var err error
		if operation.delete {
			err = store.Delete(ctx, operation.key.scope, operation.key.kind)
		} else {
			_, err = store.Put(ctx, operation.key.scope, operation.key.kind, operation.payload)
		}
		return stateWriteResult{operation: operation, err: err}
	}
}

func (model Model) desiredWorkerState() (
	map[stateRecordKey]json.RawMessage,
	map[stateRecordKey]struct{},
	error,
) {
	desired := make(map[stateRecordKey]json.RawMessage)
	protected := make(map[stateRecordKey]struct{})
	var firstErr error
	put := func(key stateRecordKey, value any) {
		payload, err := json.Marshal(value)
		if err != nil {
			protected[key] = struct{}{}
			if firstErr == nil {
				firstErr = err
			}
			return
		}
		desired[key] = payload
	}

	for key, saved := range model.stateDrafts {
		put(key, saved.draft)
	}
	for _, rejected := range model.rejectedDrafts {
		if draft, ok := persistedDraftFromRejected(rejected); ok {
			key := stateRecordKey{scope: stateScope(rejected.assignment), kind: workerStateDraftKind}
			put(key, draft)
		}
	}
	if model.active != nil {
		if draft, ok := model.currentPersistedDraft(); ok {
			key := stateRecordKey{scope: stateScope(*model.active), kind: workerStateDraftKind}
			put(key, draft)
		}
	}
	if model.pending.kind != pendingNone && model.pending.event.ID != "" {
		key := pendingSendStateKey(model.pending)
		put(key, persistedPendingFromSend(model.pending, pendingSendDispositionSend))
	}
	for _, pending := range model.pendingRecoveries {
		if pending.kind == pendingNone || pending.event.ID == "" {
			continue
		}
		key := pendingSendStateKey(pending)
		put(key, persistedPendingFromSend(pending, pendingSendDispositionSend))
	}

	states := append([]continuationState(nil), model.parkedContinuations...)
	if current := model.currentContinuation(); current.origin != "" {
		states = append(states, current)
	}
	for _, state := range states {
		key := stateRecordKey{scope: scopeFromContinuation(state), kind: workerStateContinuationKind}
		put(key, persistedFromContinuation(state))
	}
	return desired, protected, firstErr
}

func (model Model) currentPersistedDraft() (persistedDraft, bool) {
	draft := persistedDraft{
		Version: workerStateVersion, Focus: persistedFocus(model.focus),
		Reply: model.replyInput, Command: model.commandInput,
	}
	if model.taskDirty || model.taskEditing {
		draft.HasTasks = true
		draft.Tasks = persistTasks(model.agentTasks)
		draft.TaskSelected = model.taskSelected
		draft.TaskDirty = model.taskDirty
		draft.TaskEditing = model.taskEditing
		draft.TaskEditIndex = model.taskEditIndex
		draft.TaskInput = model.taskInput
	}
	if model.composing {
		draft.ToolInput = model.input
		draft.ToolCallIDs = append([]string(nil), model.toolCallIDs...)
	}
	return draft, persistedDraftHasContent(draft)
}

func (model Model) remainingDraftAfterSend(kind pendingSendKind) persistedDraft {
	draft, _ := model.currentPersistedDraft()
	switch kind {
	case pendingReply:
		draft.Reply = ""
	case pendingCommand:
		draft.Command = ""
	case pendingTasks:
		draft.HasTasks = false
		draft.Tasks = nil
		draft.TaskSelected = 0
		draft.TaskDirty = false
		draft.TaskEditing = false
		draft.TaskEditIndex = -1
		draft.TaskInput = ""
	case pendingAdvancedTools:
		draft.ToolInput = ""
		draft.ToolCallIDs = nil
	}
	if !persistedDraftHasContent(draft) {
		return persistedDraft{Version: workerStateVersion, TaskEditIndex: -1}
	}
	if draft.Focus == pendingKindFocus(kind) {
		switch {
		case draft.Reply != "":
			draft.Focus = persistedFocusReply
		case draft.Command != "":
			draft.Focus = persistedFocusCommand
		default:
			draft.Focus = persistedFocusTasks
		}
	}
	return draft
}

func pendingKindFocus(kind pendingSendKind) string {
	switch kind {
	case pendingCommand:
		return persistedFocusCommand
	case pendingTasks, pendingAdvancedTools:
		return persistedFocusTasks
	default:
		return persistedFocusReply
	}
}

func persistedDraftHasContent(draft persistedDraft) bool {
	return draft.Reply != "" || draft.Command != "" || draft.HasTasks || draft.ToolInput != "" || len(draft.ToolCallIDs) > 0
}

func persistedDraftFromPending(pending pendingSend) (persistedDraft, bool) {
	draft := persistedDraft{Version: workerStateVersion, TaskEditIndex: -1}
	switch pending.kind {
	case pendingReply:
		draft.Focus = persistedFocusReply
		draft.Reply = pending.reply
	case pendingCommand:
		draft.Focus = persistedFocusCommand
		draft.Command = pending.command
	case pendingTasks:
		draft.Focus = persistedFocusTasks
		draft.HasTasks = true
		draft.Tasks = persistTasks(pending.tasks)
		draft.TaskDirty = true
		draft.TaskSelected = pending.selected
	case pendingAdvancedTools:
		draft.Focus = persistedFocusTasks
		draft.ToolInput = pending.toolInput
		draft.ToolCallIDs = append([]string(nil), pending.toolCallIDs...)
	}
	return draft, persistedDraftHasContent(draft)
}

func persistedDraftFromRejected(rejected rejectedDraftState) (persistedDraft, bool) {
	draft := persistedDraft{Version: workerStateVersion}
	if rejected.hasReply {
		draft.Reply = rejected.reply
	}
	if rejected.hasCommand {
		draft.Command = rejected.command
	}
	if rejected.hasTasks {
		draft.HasTasks = true
		draft.Tasks = persistTasks(rejected.tasks)
		draft.TaskSelected = rejected.selected
		draft.TaskDirty = true
		draft.TaskEditIndex = -1
	}
	switch rejected.kind {
	case pendingCommand:
		draft.Focus = persistedFocusCommand
	case pendingTasks:
		draft.Focus = persistedFocusTasks
	default:
		draft.Focus = persistedFocusReply
	}
	if rejected.hasTools {
		draft.ToolInput = rejected.toolInput
		draft.ToolCallIDs = append([]string(nil), rejected.toolCallIDs...)
	}
	return draft, persistedDraftHasContent(draft)
}

func (model *Model) takePersistentDraft(assignment completion.Assignment) (persistedDraft, bool) {
	target := stateScope(assignment)
	exact := stateRecordKey{scope: target, kind: workerStateDraftKind}
	if saved, ok := model.stateDrafts[exact]; ok {
		delete(model.stateDrafts, exact)
		return saved.draft, true
	}
	if target.Tier != completion.TierRemoteTools && target.Tier != completion.TierWorkspace {
		return persistedDraft{}, false
	}
	var selected stateRecordKey
	var selectedDraft savedStateDraft
	found := false
	for key, saved := range model.stateDrafts {
		if key.kind != workerStateDraftKind || !sameStableStateScope(key.scope, target) {
			continue
		}
		if !found || saved.revision > selectedDraft.revision ||
			(saved.revision == selectedDraft.revision && saved.updatedAt.After(selectedDraft.updatedAt)) {
			selected, selectedDraft, found = key, saved, true
		}
	}
	if !found {
		return persistedDraft{}, false
	}
	delete(model.stateDrafts, selected)
	return selectedDraft.draft, true
}

func (model *Model) applyPersistentDraft(draft persistedDraft) {
	model.setReplyValue(draft.Reply)
	model.setCommandValue(draft.Command)
	model.commandConfirm = ""
	if draft.HasTasks {
		model.agentTasks = restoreTasks(draft.Tasks)
		model.taskSelected = min(max(0, draft.TaskSelected), max(0, len(model.agentTasks)-1))
		model.taskDirty = draft.TaskDirty
		model.taskEditing = draft.TaskEditing
		model.taskEditIndex = draft.TaskEditIndex
		model.taskInput = draft.TaskInput
		model.taskSyncWait = false
		model.taskConflict = false
	}
	if draft.ToolInput != "" || len(draft.ToolCallIDs) > 0 {
		model.composing = true
		model.input = draft.ToolInput
		model.toolCallIDs = append([]string(nil), draft.ToolCallIDs...)
	}
	switch draft.Focus {
	case persistedFocusTasks:
		model.focus = focusTasks
	case persistedFocusCommand:
		// Focus is presentation state, not authority. Re-evaluate the current
		// assignment's declared tool and ExecAllowed boundary before restoring
		// command focus; a persisted draft can never revive old permissions.
		if _, reason := model.commandTarget(); reason == "" {
			model.focus = focusCommand
		} else {
			model.focus = focusReply
		}
	default:
		model.focus = focusReply
	}
}

func stateScope(assignment completion.Assignment) workerstate.Scope {
	tier := assignment.CapabilityTier
	if tier == "" {
		tier = completion.TierChat
	}
	return workerstate.Scope{
		CallerID: assignment.CallerID, WorkspaceKey: assignment.WorkspaceKey,
		TaskID: assignment.TaskID, SessionKey: assignment.SessionKey(), Tier: tier,
	}
}

func scopeFromContinuation(state continuationState) workerstate.Scope {
	tier := state.tier
	if tier == "" {
		tier = completion.TierChat
	}
	return workerstate.Scope{
		CallerID: state.caller, WorkspaceKey: state.workspace, TaskID: state.taskID,
		SessionKey: state.origin, Tier: tier,
	}
}

func sameStableStateScope(left, right workerstate.Scope) bool {
	return left.CallerID == right.CallerID && left.WorkspaceKey == right.WorkspaceKey &&
		left.TaskID == right.TaskID && left.Tier == right.Tier
}

func recoverableDraftScope(stored, target workerstate.Scope) bool {
	if stored == target {
		return true
	}
	if target.Tier != completion.TierRemoteTools && target.Tier != completion.TierWorkspace {
		return false
	}
	return sameStableStateScope(stored, target)
}

func persistedFromContinuation(state continuationState) persistedContinuation {
	ids := make([]string, 0, len(state.ids))
	for id := range state.ids {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return persistedContinuation{
		Version: workerStateVersion, IDs: ids, Handoff: state.handoff,
		Context: cloneAssignment(state.context),
	}
}

func pendingSendStateKey(pending pendingSend) stateRecordKey {
	return stateRecordKey{scope: stateScope(pending.assignment), kind: workerStatePendingSendKind}
}

func persistedPendingFromSend(pending pendingSend, disposition string) persistedPendingSend {
	draft, _ := persistedDraftFromPending(pending)
	return persistedPendingSend{
		Version: workerStateVersion, Disposition: disposition, Kind: pending.kind,
		Assignment: pending.assignment, Event: pending.event, Context: cloneAssignment(pending.context),
		Draft: draft, Remaining: pending.remainingDraft,
		ToolCalls: append([]completion.ToolCall(nil), pending.toolCalls...),
	}
}

func pendingSendFromPersisted(persisted persistedPendingSend) pendingSend {
	pending := pendingSend{
		kind: persisted.Kind, eventID: persisted.Event.ID,
		assignment: persisted.Assignment, context: cloneAssignment(persisted.Context),
		event: persisted.Event, durable: true, recovered: true,
		toolCalls:      append([]completion.ToolCall(nil), persisted.ToolCalls...),
		remainingDraft: sanitizePersistedDraft(persisted.Remaining),
	}
	pending.reply = persisted.Draft.Reply
	pending.command = persisted.Draft.Command
	pending.tasks = restoreTasks(persisted.Draft.Tasks)
	pending.selected = persisted.Draft.TaskSelected
	pending.toolInput = persisted.Draft.ToolInput
	pending.toolCallIDs = append([]string(nil), persisted.Draft.ToolCallIDs...)
	return pending
}

func continuationFromPersisted(scope workerstate.Scope, persisted persistedContinuation) continuationState {
	ids := make(map[string]struct{}, len(persisted.IDs))
	for _, id := range persisted.IDs {
		ids[id] = struct{}{}
	}
	return continuationState{
		caller: scope.CallerID, workspace: scope.WorkspaceKey, taskID: scope.TaskID,
		tier: scope.Tier, origin: scope.SessionKey, ids: ids, handoff: persisted.Handoff,
		context: cloneAssignment(persisted.Context),
	}
}

func validatePersistedDraft(draft persistedDraft) error {
	if draft.Version != workerStateVersion {
		return fmt.Errorf("unsupported draft version %d", draft.Version)
	}
	switch draft.Focus {
	case "", persistedFocusTasks, persistedFocusReply, persistedFocusCommand:
	default:
		return fmt.Errorf("persisted draft has invalid focus %q", draft.Focus)
	}
	if len(draft.Reply) > maxPersistedInputBytes || len(draft.Command) > maxPersistedInputBytes ||
		len(draft.TaskInput) > maxPersistedInputBytes || len(draft.ToolInput) > maxPersistedInputBytes {
		return errors.New("persisted draft input is too large")
	}
	if !utf8.ValidString(draft.Reply) || !utf8.ValidString(draft.Command) || !utf8.ValidString(draft.TaskInput) ||
		!utf8.ValidString(draft.ToolInput) {
		return errors.New("persisted draft input is not UTF-8")
	}
	if len(draft.ToolCallIDs) > maxAgentTasks {
		return errors.New("persisted tool draft has too many call ids")
	}
	for _, identifier := range draft.ToolCallIDs {
		if strings.TrimSpace(identifier) == "" {
			return errors.New("persisted tool draft has an empty call id")
		}
	}
	if len(draft.Tasks) > maxAgentTasks {
		return errors.New("persisted task list is too large")
	}
	inProgress := 0
	for index, task := range draft.Tasks {
		if strings.TrimSpace(task.Content) == "" || utf8.RuneCountInString(task.Content) > maxTaskContentRunes {
			return fmt.Errorf("persisted task %d has invalid content", index+1)
		}
		switch task.Status {
		case taskPending, taskInProgress, taskCompleted, taskCancelled:
		default:
			return fmt.Errorf("persisted task %d has invalid status", index+1)
		}
		if _, err := parsePriority(task.Priority); err != nil {
			return fmt.Errorf("persisted task %d has invalid priority", index+1)
		}
		if task.Status == taskInProgress {
			inProgress++
		}
	}
	if inProgress > 1 {
		return errors.New("persisted task list has multiple in-progress items")
	}
	if draft.TaskSelected < 0 || draft.TaskSelected > max(0, len(draft.Tasks)-1) {
		return errors.New("persisted task selection is out of range")
	}
	if draft.TaskEditing && (draft.TaskEditIndex < -1 || draft.TaskEditIndex >= len(draft.Tasks)) {
		return errors.New("persisted task editor index is out of range")
	}
	return nil
}

func validatePersistedPendingSend(scope workerstate.Scope, persisted persistedPendingSend) error {
	if persisted.Version != workerStateVersion {
		return fmt.Errorf("unsupported pending send version %d", persisted.Version)
	}
	if persisted.Disposition != pendingSendDispositionSend && persisted.Disposition != pendingSendDispositionRestore {
		return fmt.Errorf("persisted pending send has invalid disposition %q", persisted.Disposition)
	}
	switch persisted.Kind {
	case pendingReply, pendingCommand, pendingTasks, pendingAdvancedTools:
	default:
		return fmt.Errorf("persisted pending send has unsupported kind %d", persisted.Kind)
	}
	if stateScope(persisted.Assignment) != scope {
		return errors.New("persisted pending send assignment does not match its scope")
	}
	if strings.TrimSpace(persisted.Event.ID) == "" {
		return errors.New("persisted pending send has no event id")
	}
	if persisted.Context != nil && stateScope(*persisted.Context) != scope {
		return errors.New("persisted pending send context does not match its scope")
	}
	if err := validatePersistedDraft(persisted.Draft); err != nil {
		return fmt.Errorf("persisted pending send draft: %w", err)
	}
	if err := validatePersistedDraft(persisted.Remaining); err != nil {
		return fmt.Errorf("persisted pending remaining draft: %w", err)
	}
	switch persisted.Kind {
	case pendingReply:
		switch persisted.Event.Type {
		case completion.EventProgress, completion.EventFinal, completion.EventClarification:
		default:
			return fmt.Errorf("persisted reply has invalid event type %q", persisted.Event.Type)
		}
	case pendingCommand, pendingTasks, pendingAdvancedTools:
		if persisted.Event.Type != completion.EventToolCalls || len(persisted.Event.ToolCalls) == 0 {
			return errors.New("persisted tool send has no tool-call event")
		}
	}
	return nil
}

func validatePersistedContinuation(scope workerstate.Scope, persisted persistedContinuation) error {
	if persisted.Version != workerStateVersion {
		return fmt.Errorf("unsupported continuation version %d", persisted.Version)
	}
	if persisted.Handoff && len(persisted.IDs) != 0 {
		return errors.New("handoff continuation cannot also wait for tool calls")
	}
	if !persisted.Handoff && len(persisted.IDs) == 0 {
		return errors.New("continuation has no awaited tool call")
	}
	seen := make(map[string]struct{}, len(persisted.IDs))
	for _, id := range persisted.IDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("continuation has an empty tool-call id")
		}
		if _, duplicate := seen[id]; duplicate {
			return errors.New("continuation has duplicate tool-call ids")
		}
		seen[id] = struct{}{}
	}
	if persisted.Context != nil && stateScope(*persisted.Context) != scope {
		return errors.New("continuation context does not match its persisted scope")
	}
	return nil
}

func sanitizePersistedDraft(draft persistedDraft) persistedDraft {
	draft.Reply = terminalSafe(draft.Reply)
	draft.Command = terminalSafe(draft.Command)
	draft.TaskInput = terminalSafe(draft.TaskInput)
	draft.ToolInput = terminalSafe(draft.ToolInput)
	for index := range draft.Tasks {
		draft.Tasks[index].Content = terminalSafe(draft.Tasks[index].Content)
	}
	return draft
}

func persistTasks(tasks []agentTask) []persistedTask {
	persisted := make([]persistedTask, len(tasks))
	for index, task := range tasks {
		persisted[index] = persistedTask{Content: task.Content, Status: task.Status, Priority: normalizePriority(task.Priority)}
	}
	return persisted
}

func restoreTasks(tasks []persistedTask) []agentTask {
	restored := make([]agentTask, len(tasks))
	for index, task := range tasks {
		restored[index] = agentTask{Content: task.Content, Status: task.Status, Priority: task.Priority}
	}
	return restored
}

func persistedFocus(focus inputFocus) string {
	switch focus {
	case focusTasks:
		return persistedFocusTasks
	case focusCommand:
		return persistedFocusCommand
	default:
		return persistedFocusReply
	}
}

func sortedStateKeys(records map[stateRecordKey]json.RawMessage) []stateRecordKey {
	keys := make([]stateRecordKey, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		return stateKeyLess(keys[left], keys[right])
	})
	return keys
}

func stateKeyLess(left, right stateRecordKey) bool {
	valuesLeft := []string{
		left.scope.CallerID, left.scope.WorkspaceKey, left.scope.TaskID,
		left.scope.SessionKey, string(left.scope.Tier), left.kind,
	}
	valuesRight := []string{
		right.scope.CallerID, right.scope.WorkspaceKey, right.scope.TaskID,
		right.scope.SessionKey, string(right.scope.Tier), right.kind,
	}
	for index := range valuesLeft {
		if valuesLeft[index] != valuesRight[index] {
			return valuesLeft[index] < valuesRight[index]
		}
	}
	return false
}

func (model Model) visibleStatus() string {
	parts := make([]string, 0, 4)
	if model.status != "" {
		parts = append(parts, model.status)
	}
	if model.stateLoadWarning != "" {
		parts = append(parts, model.stateLoadWarning)
	}
	if model.stateWriteWarning != "" {
		parts = append(parts, model.stateWriteWarning)
	}
	if model.outboxWarning != "" {
		parts = append(parts, model.outboxWarning)
	}
	if len(parts) == 0 {
		return "ready"
	}
	return strings.Join(parts, " · ")
}
