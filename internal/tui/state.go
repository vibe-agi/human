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
	"github.com/vibe-agi/human/internal/workerstate"
)

const (
	workerStateDraftKind        = "tui_draft_v1"
	workerStateContinuationKind = "tui_continuation_v1"
	workerStateVersion          = 1
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
	Put(context.Context, workerstate.Scope, string, json.RawMessage) (workerstate.Record, error)
	Delete(context.Context, workerstate.Scope, string) error
	List(context.Context) ([]workerstate.Record, error)
}

// WithStateStore enables durable TUI drafts and continuation recovery. A nil
// store is equivalent to leaving persistence disabled.
func WithStateStore(store StateStore) Option {
	return func(model *Model) { model.stateStore = store }
}

type stateRecordKey struct {
	scope workerstate.Scope
	kind  string
}

type savedStateDraft struct {
	draft     persistedDraft
	updatedAt time.Time
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
	if model.stateStore == nil {
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

	continuations := make([]continuationState, 0)
	draftOrder := make([]stateRecordKey, 0)
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
			model.stateDrafts[key] = savedStateDraft{draft: sanitizePersistedDraft(draft), updatedAt: record.UpdatedAt}
			draftOrder = append(draftOrder, key)
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
		default:
			// A newer binary may own this kind. Leave it untouched rather than
			// treating forward-compatible state as garbage.
		}
	}

	if len(draftOrder) > maxRejectedDraftScopes {
		for _, key := range draftOrder[:len(draftOrder)-maxRejectedDraftScopes] {
			delete(model.stateDrafts, key)
		}
		decodeErrors += len(draftOrder) - maxRejectedDraftScopes
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
	if count := corruptRows + decodeErrors; count > 0 {
		model.stateLoadWarning = fmt.Sprintf("ignored %d corrupt recovery record(s)", count)
	}
}

func (model *Model) nextStateCommand() tea.Cmd {
	if model.stateStore == nil || model.stateWriting || model.stateRetryPending {
		return nil
	}
	desired, protected, err := model.desiredWorkerState()
	if err != nil {
		model.stateWriteWarning = "recovery state encode failed: " + err.Error()
	}
	for key := range desired {
		model.stateManaged[key] = struct{}{}
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
	return draft, draft.Reply != "" || draft.Command != "" || draft.HasTasks
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
	return draft, draft.Reply != "" || draft.Command != "" || draft.HasTasks
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
		if !found || saved.updatedAt.After(selectedDraft.updatedAt) {
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
		len(draft.TaskInput) > maxPersistedInputBytes {
		return errors.New("persisted draft input is too large")
	}
	if !utf8.ValidString(draft.Reply) || !utf8.ValidString(draft.Command) || !utf8.ValidString(draft.TaskInput) {
		return errors.New("persisted draft input is not UTF-8")
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
		a, b := keys[left], keys[right]
		valuesA := []string{a.scope.CallerID, a.scope.WorkspaceKey, a.scope.TaskID, a.scope.SessionKey, string(a.scope.Tier), a.kind}
		valuesB := []string{b.scope.CallerID, b.scope.WorkspaceKey, b.scope.TaskID, b.scope.SessionKey, string(b.scope.Tier), b.kind}
		for index := range valuesA {
			if valuesA[index] != valuesB[index] {
				return valuesA[index] < valuesB[index]
			}
		}
		return false
	})
	return keys
}

func (model Model) visibleStatus() string {
	parts := make([]string, 0, 3)
	if model.status != "" {
		parts = append(parts, model.status)
	}
	if model.stateLoadWarning != "" {
		parts = append(parts, model.stateLoadWarning)
	}
	if model.stateWriteWarning != "" {
		parts = append(parts, model.stateWriteWarning)
	}
	if len(parts) == 0 {
		return "ready"
	}
	return strings.Join(parts, " · ")
}
