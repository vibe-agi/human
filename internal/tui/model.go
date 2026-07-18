// Package tui contains the Bubble Tea worker interface.
package tui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/safety"
	workmirror "github.com/vibe-agi/human/internal/mirror"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerproto"
)

type Client interface {
	Messages() <-chan workerclient.Message
	SendEvent(context.Context, completion.Assignment, completion.Event) error
	ConfirmRejectedEvent(context.Context, string) error
}

type Model struct {
	client              Client
	mirrorManager       MirrorManager
	mirrors             map[string]MirrorWorkspace
	mirrorWatches       map[string]mirrorWatchState
	mirrorDirty         map[string]bool
	mirrorGeneration    map[string]uint64
	mirrorReviewing     map[string]uint64
	mirrorWatchSequence uint64
	workspaceAutoSend   bool
	assignments         []completion.Assignment
	selected            int
	active              *completion.Assignment
	lastContext         *completion.Assignment
	focus               inputFocus
	replyInput          string
	commandInput        string
	commandConfirm      string
	agentTasks          []agentTask
	taskSelected        int
	taskDirty           bool
	taskEditing         bool
	taskEditIndex       int
	taskInput           string
	taskSyncWait        bool
	taskConflict        bool
	continueCaller      string
	continueWorkspace   string
	continueTaskID      string
	continueTier        completion.CapabilityTier
	continueOrigin      string
	continueIDs         map[string]struct{}
	continueHandoff     bool
	continueContext     *completion.Assignment
	parkedContinuations []continuationState
	pending             pendingSend
	pendingRecoveries   []pendingSend
	recoveredSessions   map[string]struct{}
	rejectedDrafts      map[string]rejectedDraftState
	rejectedDraftOrder  []string
	handledRejections   map[string]struct{}
	handledRejectOrder  []string
	draftSession        string
	draftAuthority      string
	draftRejectedEvents []string
	draftRejectedKinds  map[string]pendingSendKind
	draftReplyRejected  string
	detailMode          bool
	composing           bool
	toolCallIDs         []string
	input               string
	status              string
	delivery            deliveryReview
	width               int
	height              int
	connection          connectionState
	connectionTerminal  string
	ui                  workspaceUI
	stateStore          StateStore
	stateBound          bool
	stateLoaded         bool
	stateIdentity       workerclient.Identity
	stateDrafts         map[stateRecordKey]savedStateDraft
	draftOrigins        map[stateRecordKey]string
	stateSynced         map[stateRecordKey]string
	stateManaged        map[stateRecordKey]struct{}
	stateWriting        bool
	stateRetryPending   bool
	stateRetryAttempt   int
	stateLoadWarning    string
	stateWriteWarning   string
	outboxWarning       string
	deferredSend        deferredSendIntent
	rejectionFinalizers map[string]rejectionFinalizer
	recoveryDrainPaused bool
	quitting            bool
}

type connectionState int

const (
	connectionConnected connectionState = iota
	connectionReconnecting
	connectionClosed
)

type inputFocus int

const (
	focusTasks inputFocus = iota
	focusReply
	focusCommand
)

type Option func(*Model)

// WithMirrorRoot enables the caller/workspace-scoped scratch mirror. Chat
// assignments still bypass it even when this option is configured.
func WithMirrorRoot(root string) Option {
	return func(model *Model) {
		if strings.TrimSpace(root) != "" {
			model.mirrorManager = newFilesystemMirrorManager(root)
		}
	}
}

// WithMirrorManager supplies a mirror implementation. It is useful for
// embedding the TUI and for deterministic boundary tests.
func WithMirrorManager(manager MirrorManager) Option {
	return func(model *Model) { model.mirrorManager = manager }
}

// WithWorkspaceAutoSend makes a mirror save immediately enter the existing
// fresh-review delivery path. Safety/conflict warnings on the change itself
// still stop for Human review. Adapter limitations remain visible in the
// preview, while the caller Agent retains its normal execution/permission gate.
func WithWorkspaceAutoSend(enabled bool) Option {
	return func(model *Model) { model.workspaceAutoSend = enabled }
}

const (
	toolDescriptionPreviewRunes = 80
	toolSchemaPreviewRunes      = 160
	maxRejectedDraftScopes      = 32
	maxHandledRejectionIDs      = 256
	maxParkedContinuations      = 32
	rejectionConfirmMinBackoff  = 100 * time.Millisecond
	rejectionConfirmMaxBackoff  = 5 * time.Second
)

type deliveryStage int

const (
	deliveryNone deliveryStage = iota
	deliveryReviewed
	deliveryPreviewed
	deliveryConfirming
	deliveryConfirmed
	deliverySending
	deliveryDiscarding
)

type deliveryReview struct {
	stage                deliveryStage
	sessionKey           string
	namespace            string
	changes              []workmirror.Change
	calls                []completion.ToolCall
	warnings             []string
	eventID              string
	generation           uint64
	prepareInFlight      bool
	intentWriterInFlight bool
	assignment           completion.Assignment
	context              *completion.Assignment
}

type pendingSendKind int

const (
	pendingNone pendingSendKind = iota
	pendingReply
	pendingCommand
	pendingTasks
	pendingAdvancedTools
	pendingAccept
	pendingReject
	// Keep new durable kinds appended: persisted worker-state rows encode these
	// numeric values and must never be reinterpreted by a later binary.
	pendingDelivery
)

type pendingSend struct {
	kind                   pendingSendKind
	eventID                string
	assignment             completion.Assignment
	context                *completion.Assignment
	reply                  string
	command                string
	tasks                  []agentTask
	toolInput              string
	toolCallIDs            []string
	toolCalls              []completion.ToolCall
	remainingDraft         persistedDraft
	draftOrigins           []draftOrigin
	selected               int
	automatic              bool
	event                  completion.Event
	durable                bool
	recovered              bool
	retryAttempt           int
	deliveryNamespace      string
	deliveryGeneration     uint64
	deliveryChanges        []workmirror.Change
	deliveryIntentRecorded bool
	// outboxInFlight is process-local. Once a send command captures this pending
	// payload, assignment replay may refresh UI routing but must not rewrite the
	// recovery row to a snapshot different from the durable outbox event.
	outboxInFlight bool
}

type pendingDeliveryIntentsDiscarded struct {
	eventID   string
	namespace string
	reason    string
	attempt   int
	retry     tea.Cmd
	err       error
}

type draftOrigin struct {
	key    stateRecordKey
	digest string
}

// deferredSendIntent is an in-memory user gesture waiting for the latest
// editor snapshot to finish committing. The editor remains untouched and
// frozen, so execution reads the exact draft that nextStateCommand just
// synchronized. A snapshot is also pinned to the source scope solely for the
// cancellation path: if the request dies meanwhile, its draft remains durable.
// The eventual response intent still follows the normal pending-send -> local
// outbox ordering.
type deferredSendIntent struct {
	kind        pendingSendKind
	sessionKey  string
	replyType   completion.EventType
	endResponse bool
	allowEmpty  bool
	draftKey    stateRecordKey
	draft       persistedDraft
}

type continuationState struct {
	caller    string
	workspace string
	taskID    string
	tier      completion.CapabilityTier
	origin    string
	ids       map[string]struct{}
	handoff   bool
	context   *completion.Assignment
}

// rejectedDraftState keeps one undelivered operator draft outside the lifetime
// of the HTTP session which rejected it. A later request may have a different
// SessionKey, so the draft is bound to the stable caller/workspace/task scope
// and is restored only when that exact scope is activated again.
type rejectedDraftState struct {
	assignment completion.Assignment
	authority  string
	// rejectedEventIDs is the durable, ordered provenance of source events
	// materialized into this draft. It makes replay idempotence and ordering
	// decidable across process restarts; the opaque authority hash cannot.
	rejectedEventIDs   []string
	rejectedEventKinds map[string]pendingSendKind
	kind               pendingSendKind
	hasReply           bool
	reply              string
	// replyRejected contains only the sent segments rejected by the gateway;
	// replyTail is text typed after those sends. Keeping the two pieces apart
	// lets a later rejection insert an older sent segment before the still-local
	// tail instead of overwriting it or moving it out of order.
	replyRejected string
	replyTail     string
	hasCommand    bool
	command       string
	hasTasks      bool
	tasks         []agentTask
	taskDirty     bool
	taskEditing   bool
	taskEditIndex int
	taskInput     string
	hasTools      bool
	toolInput     string
	toolCallIDs   []string
	selected      int
}

type networkMessage workerclient.Message
type networkClosed struct{}
type eventSent struct {
	eventID        string
	intentKey      stateRecordKey
	restorePayload json.RawMessage
	restorePending bool
	intentErr      error
	err            error
}
type pendingSendRetry struct {
	eventID string
	attempt int
}
type pendingRestoreRetry struct {
	eventID string
	attempt int
	sendErr error
}
type resumeDurablePendingRequest struct{ eventID string }
type rejectedEventConfirmed struct {
	eventID string
	attempt int
	err     error
}

// rejectionFinalizer keeps the rejected inbox as the crash authority until
// both local correctness stores are clean. A pending-send journal may be
// deleted in parallel with mirror cleanup, but ConfirmRejectedEvent is never
// issued until both operations have committed and any already-running mirror
// confirmation has settled.
type rejectionFinalizer struct {
	scope                     string
	pendingKey                stateRecordKey
	waitsForPendingDelete     bool
	pendingDeleted            bool
	waitsForStateSync         bool
	mirrorConfirmationPending bool
	cleanup                   tea.Cmd
	cleanupDone               bool
	cleanupInFlight           bool
	confirming                bool
	resumeRecoveries          bool
}

type mirrorIntentsDiscarded struct {
	eventID string
	attempt int
	retry   tea.Cmd
	reason  string
	err     error
}

func New(client Client, options ...Option) Model {
	model := Model{
		client: client, status: "ready",
		mirrors:             make(map[string]MirrorWorkspace),
		mirrorWatches:       make(map[string]mirrorWatchState),
		mirrorDirty:         make(map[string]bool),
		mirrorGeneration:    make(map[string]uint64),
		mirrorReviewing:     make(map[string]uint64),
		continueIDs:         make(map[string]struct{}),
		recoveredSessions:   make(map[string]struct{}),
		rejectedDrafts:      make(map[string]rejectedDraftState),
		handledRejections:   make(map[string]struct{}),
		rejectionFinalizers: make(map[string]rejectionFinalizer),
		stateDrafts:         make(map[stateRecordKey]savedStateDraft),
		draftOrigins:        make(map[stateRecordKey]string),
		stateSynced:         make(map[stateRecordKey]string),
		stateManaged:        make(map[stateRecordKey]struct{}),
		width:               80, height: 24, focus: focusTasks, taskEditIndex: -1,
		connection: connectionConnected, ui: newWorkspaceUI(),
	}
	for _, option := range options {
		option(&model)
	}
	if provider, ok := client.(interface {
		WorkerIdentity() (workerclient.Identity, bool)
	}); ok {
		if identity, ready := provider.WorkerIdentity(); ready {
			if err := model.bindAndLoadWorkerState(identity); err != nil {
				model.stateLoadWarning = "recovery state identity failed: " + err.Error()
			}
		}
	}
	return model
}

func (model Model) Init() tea.Cmd {
	commands := []tea.Cmd{waitForNetwork(model.client), tea.RequestBackgroundColor}
	if model.pending.kind != pendingNone && model.pending.durable && model.pending.event.ID != "" {
		// Init has a value receiver, so it cannot publish correctness-bearing
		// in-flight flags. Re-enter through Update before any mirror writer or
		// outbox command captures the recovered payload.
		eventID := model.pending.event.ID
		commands = append(commands, func() tea.Msg {
			return resumeDurablePendingRequest{eventID: eventID}
		})
	}
	if model.animationActive() {
		commands = append(commands, model.ui.spinner.Tick)
	}
	return tea.Batch(commands...)
}

func (model Model) Update(message tea.Msg) (updated tea.Model, command tea.Cmd) {
	wasAnimating := model.animationActive()
	defer func() {
		next, ok := updated.(Model)
		if !ok {
			return
		}
		next.syncUI()
		next.resizeUI()
		next.cancelDeferredSendIfInactive()
		stateCommand := next.nextStateCommand()
		if stateCommand != nil {
			command = tea.Batch(command, stateCommand)
		} else if next.deferredSend.kind != pendingNone && next.workerStateSynchronized() {
			var deferred tea.Cmd
			next, deferred = next.executeDeferredSend()
			command = tea.Batch(command, deferred)
			// Executing the gesture only stages the immutable response event in
			// memory. Start its pending-intent transaction before the outbox is
			// allowed to observe it.
			if intent := next.nextStateCommand(); intent != nil {
				command = tea.Batch(command, intent)
			}
		}
		if rejection := next.advanceRejectionFinalizations(); rejection != nil {
			command = tea.Batch(command, rejection)
		}
		if !wasAnimating && next.animationActive() {
			command = tea.Batch(command, next.ui.spinner.Tick)
		}
		if next.quitting && next.workerStateSynchronized() && !next.correctnessWorkInFlight() {
			command = tea.Batch(command, tea.Quit)
		}
		updated = next
	}()
	model.syncUI()
	model.resizeUI()
	if command, handled := model.ui.handleSystemMessage(message, model.animationActive()); handled {
		return model, command
	}
	switch message := message.(type) {
	case tea.WindowSizeMsg:
		if message.Width > 0 {
			model.width = message.Width
		}
		if message.Height > 0 {
			model.height = message.Height
		}
		return model, nil
	case tea.KeyboardEnhancementsMsg:
		return model, nil
	case stateWriteResult:
		stateCommand := model.applyStateWriteResult(message)
		return model, stateCommand
	case stateRetryReady:
		model.stateRetryPending = false
		return model, nil
	case pendingSendRetry:
		if model.pending.eventID != message.eventID || model.pending.kind == pendingNone ||
			(model.stateStore != nil && !model.pending.durable &&
				model.pending.kind != pendingAccept && model.pending.kind != pendingReject) {
			return model, nil
		}
		model.pending.retryAttempt = message.attempt
		if model.pending.kind == pendingDelivery {
			model.status = "retrying the exact durable file delivery…"
		} else {
			model.status = "retrying the durable local response intent…"
		}
		if model.stateStore == nil || model.pending.kind == pendingAccept || model.pending.kind == pendingReject {
			return model, sendEvent(model.client, model.pending.assignment, model.pending.event)
		}
		return model, sendPersistedEvent(model.client, model.stateStore, model.pending)
	case pendingRestoreRetry:
		if model.pending.eventID != message.eventID || model.pending.kind == pendingNone ||
			model.stateStore == nil || !model.pending.durable {
			return model, nil
		}
		model.pending.retryAttempt = message.attempt
		model.status = "retrying the durable draft-restore decision…"
		return model, persistPendingRestore(
			model.stateStore, model.pending, message.sendErr,
		)
	case resumeDurablePendingRequest:
		if model.pending.kind == pendingNone || !model.pending.durable ||
			model.pending.event.ID != message.eventID {
			return model, nil
		}
		resume := model.resumeDurablePending(model.pending)
		return model, resume
	case networkClosed:
		model.invalidateChat()
		model.connection = connectionClosed
		if model.connectionTerminal != "" {
			model.status = model.connectionTerminal
		} else {
			model.status = "worker connection closed"
		}
		return model, nil
	case networkMessage:
		model.invalidateChat()
		if message.IdentityReady != nil {
			wasBound := model.stateBound
			if err := model.bindAndLoadWorkerState(*message.IdentityReady); err != nil {
				model.connection = connectionClosed
				model.connectionTerminal = "worker recovery state identity failed: " + terminalSafe(err.Error())
				model.status = model.connectionTerminal
				return model, nil
			}
			commands := []tea.Cmd{waitForNetwork(model.client)}
			if !wasBound && model.pending.kind != pendingNone && model.pending.durable && model.pending.event.ID != "" {
				commands = append(commands, model.resumeDurablePending(model.pending))
			}
			return model, tea.Batch(commands...)
		}
		if message.OutboxQuarantine != nil {
			quarantine := message.OutboxQuarantine
			identifiers := make([]string, 0, len(quarantine.EventIDs))
			for _, identifier := range quarantine.EventIDs {
				identifier = strings.ToValidUTF8(identifier, "�")
				runes := []rune(identifier)
				if len(runes) > 128 {
					identifier = string(runes[:128]) + "…"
				}
				identifiers = append(identifiers, terminalSafe(identifier))
			}
			detail := fmt.Sprintf("%d corrupt worker outbox event(s) quarantined; healthy events continue", quarantine.Count)
			if len(identifiers) > 0 {
				detail += " · event IDs: " + strings.Join(identifiers, ", ")
			}
			if strings.TrimSpace(quarantine.Path) != "" {
				detail += " · raw records retained in " + terminalSafe(quarantine.Path)
			}
			model.outboxWarning = detail
			model.status = "worker outbox corruption isolated"
			return model, waitForNetwork(model.client)
		}
		if message.Err != nil {
			if errors.Is(message.Err, workerclient.ErrWorkerAlreadyConnected) {
				model.connection = connectionReconnecting
				model.connectionTerminal = ""
				model.status = "another Human worker currently owns this token · waiting without displacing it"
				return model, waitForNetwork(model.client)
			}
			if errors.Is(message.Err, workerclient.ErrWorkerOwnershipViolation) {
				model.connection = connectionClosed
				model.connectionTerminal = "worker event ownership violation · outbox retained; verify the token and task owner"
				model.status = model.connectionTerminal
				return model, waitForNetwork(model.client)
			}
			model.connection = connectionReconnecting
			model.status = "connection error: " + message.Err.Error()
			return model, waitForNetwork(model.client)
		}
		if message.ConnectionRestored {
			model.connection = connectionConnected
			model.connectionTerminal = ""
			model.status = "reconnected · ready"
			return model, waitForNetwork(model.client)
		}
		if message.EventRejected != nil {
			rejected := message.EventRejected
			var intentCleanup tea.Cmd
			if _, finalizing := model.rejectionFinalizers[rejected.EventID]; finalizing {
				// A live barrier is stronger than the bounded duplicate receipt cache.
				// Never let cache eviction re-enter first-delivery handling and replace
				// its writer/delete/drain obligations.
				return model, tea.Batch(
					waitForNetwork(model.client),
					model.advanceRejectionFinalization(rejected.EventID),
				)
			}
			if model.rememberHandledRejection(rejected.EventID) {
				if message.RejectedEvent != nil {
					var assignment *completion.Assignment
					sessionKey := (completion.Assignment{
						CallerID: rejected.CallerID, IdempotencyKey: rejected.IdempotencyKey,
					}).SessionKey()
					switch {
					case message.RejectedAssignment != nil && message.RejectedAssignment.SessionKey() == sessionKey:
						assignment = message.RejectedAssignment
					case model.pending.assignment.SessionKey() == sessionKey:
						assignment = &model.pending.assignment
					case model.lastContext != nil && model.lastContext.SessionKey() == sessionKey:
						assignment = model.lastContext
					}
					if assignment != nil {
						intentCleanup = model.discardIntentCommand(
							*assignment, message.RejectedEvent.ToolCalls, "replayed durable event rejection",
						)
						if rejectedMirrorEvent(*assignment, *message.RejectedEvent) && intentCleanup == nil {
							intentCleanup = unavailableIntentCleanup(
								"durable workspace delivery intent cleanup is unavailable",
							)
						}
					}
				}
				// The durable rejected inbox may replay while an earlier finalization
				// is still in flight. Repeat the idempotent cleanup-before-confirm
				// sequence, but never merge the same rejected draft twice.
				return model, tea.Batch(
					waitForNetwork(model.client),
					finalizeRejectedEvent(model.client, rejected.EventID, intentCleanup),
				)
			}
			sessionKey := (completion.Assignment{
				CallerID: rejected.CallerID, IdempotencyKey: rejected.IdempotencyKey,
			}).SessionKey()
			// A rejected event does not cover its source request. Let a gateway replay
			// re-enter Inbox (with the saved draft) instead of suppressing the only
			// correction surface as if the durable response had succeeded.
			delete(model.recoveredSessions, sessionKey)
			rejectedPending := pendingSend{}
			pendingIndex := -1 // -1 is current; non-negative values index pendingRecoveries.
			pendingMatches := model.pending.eventID == rejected.EventID &&
				model.pending.assignment.SessionKey() == sessionKey
			if pendingMatches {
				rejectedPending = model.pending
			} else {
				for index := range model.pendingRecoveries {
					candidate := model.pendingRecoveries[index]
					if candidate.eventID == rejected.EventID &&
						candidate.assignment.SessionKey() == sessionKey {
						rejectedPending = candidate
						pendingIndex = index
						pendingMatches = true
						break
					}
				}
			}
			stalePendingKey, stalePendingJournal := model.rejectedPendingJournal(
				rejected.EventID, sessionKey,
			)
			// A recovered delivery may still be executing RecordDeliveryIntents in
			// another Bubble Tea command. Capture that writer before clearing the UI
			// delivery state below; rejection cleanup must run after it settles or the
			// old writer can recreate an intent after cleanup has committed.
			mirrorWriterPending := pendingMatches && pendingIndex < 0 &&
				rejectedPending.kind == pendingDelivery &&
				model.delivery.intentWriterInFlight &&
				model.delivery.eventID == rejected.EventID
			activeMatches := model.active != nil && model.active.SessionKey() == sessionKey
			contextMatches := model.lastContext != nil && model.lastContext.SessionKey() == sessionKey
			continuationMatches := model.hasContinuationOrigin(sessionKey)
			otherPendingSameSession := !pendingMatches && model.pending.kind != pendingNone &&
				model.pending.assignment.SessionKey() == sessionKey
			var activeEditor persistedDraft
			activeEditorPresent := false
			if activeMatches {
				activeEditor, activeEditorPresent = model.currentPersistedDraft()
				if pendingMatches || otherPendingSameSession {
					// rejectedDraftFromPending already owns the pane being sent and
					// its live tail. A different event from the same session may also
					// be rejected first; its pending snapshot retains ownership of
					// that pane until its own ACK/NACK resolves. Capture only the
					// independent panes here.
					kind := rejectedPending.kind
					if otherPendingSameSession {
						kind = model.pending.kind
					}
					activeEditor = model.remainingDraftAfterSend(kind)
					activeEditorPresent = persistedDraftHasContent(activeEditor)
				}
			}
			taskSyncMatches := model.taskSyncWait && (activeMatches || continuationMatches ||
				(pendingMatches && rejectedPending.kind == pendingTasks))
			var restoredDraft *rejectedDraftState
			rejectionScope := ""
			draftEvicted := false
			draftParked := false
			mirrorRejected := false
			originalMatches := message.RejectedEvent != nil &&
				message.RejectedEvent.ID == rejected.EventID
			if pendingMatches {
				pending := rejectedPending
				rejectionScope = rejectedDraftScopeKey(pending.assignment)
				if pending.kind == pendingDelivery {
					mirrorRejected = true
				}
				if len(pending.event.ToolCalls) > 0 {
					intentCleanup = model.discardIntentCommand(
						pending.assignment, pending.event.ToolCalls, "durable event rejection",
					)
				}
				if pending.kind == pendingDelivery && intentCleanup == nil {
					intentCleanup = unavailableIntentCleanup(
						"durable workspace delivery intent cleanup is unavailable",
					)
				}
				prior, hasPrior := model.rejectedDraftForAssignment(pending.assignment)
				priorSameSession := hasPrior && prior.assignment.SessionKey() == sessionKey
				if pendingIndex < 0 && priorSameSession && originalMatches {
					// Another event from this expired stream was rejected first.
					// Keep its surgical rollback and retract only this event;
					// restoring pending.context would resurrect the earlier item.
					model.rollbackRejectedEvent(*message.RejectedEvent)
				} else if pendingIndex < 0 && pending.context != nil {
					model.lastContext = cloneAssignment(pending.context)
					model.invalidateChat()
				}
				if draft, ok := model.rejectedDraftFromPending(pending); ok {
					restoredDraft = &draft
				}
			} else if originalMatches && model.lastContext != nil &&
				model.lastContext.SessionKey() == sessionKey {
				assignment := *model.lastContext
				rejectionScope = rejectedDraftScopeKey(assignment)
				intentCleanup = model.discardIntentCommand(
					assignment, message.RejectedEvent.ToolCalls, "durable event rejection",
				)
				model.rollbackRejectedEvent(*message.RejectedEvent)
				mirrorRejected = rejectedMirrorEvent(assignment, *message.RejectedEvent)
				if draft, ok := rejectedDraftFromEvent(assignment, *message.RejectedEvent); ok {
					restoredDraft = &draft
				}
			} else if originalMatches && message.RejectedAssignment != nil &&
				message.RejectedAssignment.SessionKey() == sessionKey {
				// After a process restart there is no optimistic transcript or pending
				// send to inspect. The worker outbox therefore carries a deliberately
				// reduced assignment snapshot (routing identity, adapter and declared
				// tools, but no conversation payload) alongside the rejected event.
				assignment := *message.RejectedAssignment
				rejectionScope = rejectedDraftScopeKey(assignment)
				intentCleanup = model.discardIntentCommand(
					assignment, message.RejectedEvent.ToolCalls, "durable event rejection",
				)
				mirrorRejected = rejectedMirrorEvent(assignment, *message.RejectedEvent)
				if draft, ok := rejectedDraftFromEvent(assignment, *message.RejectedEvent); ok {
					restoredDraft = &draft
				}
			}
			if activeEditorPresent {
				editor, ok := rejectedDraftFromPersisted(*model.active, activeEditor)
				if ok {
					editor.authority = mergeDraftAuthorities(
						editor.authority, eventDraftAuthority(rejected.EventID),
					)
					if restoredDraft == nil {
						restoredDraft = &editor
					} else {
						merged := mergeRejectedDraft(restoredDraft, editor)
						restoredDraft = &merged
					}
				}
				delete(model.stateDrafts, stateRecordKey{
					scope: stateScope(*model.active), kind: workerStateDraftKind,
				})
			} else if pendingMatches && persistedDraftHasContent(rejectedPending.remainingDraft) {
				// Terminal sends clear active before their outbox command completes.
				// A NACK can therefore arrive before eventSent while every independent
				// pane exists only in the immutable pending snapshot. Materialize that
				// remainder into the rejected draft before deleting the journal.
				editor, ok := rejectedDraftFromPersisted(
					rejectedPending.assignment, rejectedPending.remainingDraft,
				)
				if ok {
					if restoredDraft == nil {
						restoredDraft = &editor
					} else {
						merged := mergeRejectedDraft(restoredDraft, editor)
						restoredDraft = &merged
					}
				}
			}
			if mirrorRejected && intentCleanup == nil {
				intentCleanup = unavailableIntentCleanup(
					"durable workspace delivery intent cleanup is unavailable",
				)
			}
			if rejectionScope == "" {
				rejectionScope = rejectedDraftScopeFromStateKey(stalePendingKey)
			}
			if rejectionScope == "" {
				rejectionScope = "session\x00" + sessionKey
			}
			model.connection = connectionConnected
			if pendingMatches {
				if pendingIndex < 0 {
					model.pending = pendingSend{}
					model.draftOrigins = make(map[stateRecordKey]string)
				} else {
					model.pendingRecoveries = append(
						model.pendingRecoveries[:pendingIndex:pendingIndex],
						model.pendingRecoveries[pendingIndex+1:]...,
					)
				}
			}
			if activeMatches {
				model.active = nil
				model.draftOrigins = make(map[stateRecordKey]string)
				model.focus = focusTasks
				model.commandConfirm = ""
				model.composing = false
				model.input = ""
				model.toolCallIDs = nil
				model.detailMode = false
			}
			if model.delivery.sessionKey == sessionKey {
				model.delivery = deliveryReview{}
			}
			if continuationMatches {
				model.removeContinuationOrigin(sessionKey)
			}
			if taskSyncMatches {
				// A task-list tool call can be locally queued before the gateway
				// reports that its HTTP session has already closed. Keep the
				// operator's list as an unsynchronized draft instead of leaving
				// the Tasks pane stuck in a false "waiting" state.
				model.taskSyncWait = false
				model.taskDirty = true
			}
			if restoredDraft != nil {
				if (pendingMatches && pendingIndex >= 0) || model.active != nil {
					// A rejection for an older/out-of-order session must not apply its
					// recovered editor over a newer active request. Keep it in the
					// scope tray; activation after the current request reaches a safe
					// boundary will merge/restore it deliberately.
					draftEvicted = model.storeRejectedDraft(*restoredDraft)
					draftParked = true
				} else {
					draftEvicted = model.installRejectedDraft(*restoredDraft)
				}
			}
			model.removeQueuedSession(sessionKey)
			reason := strings.TrimSpace(rejected.Message)
			if restoredDraft != nil {
				if draftParked {
					model.status = "response not delivered; draft saved for its task after the active request: " + terminalSafe(reason)
				} else {
					model.status = "response not delivered; draft restored: " + terminalSafe(reason)
				}
				if draftEvicted {
					model.status += " · oldest saved draft evicted at the 32-scope limit"
				}
			} else if mirrorRejected {
				model.status = "workspace delivery not accepted; re-review the mirror on the next request: " + terminalSafe(reason)
			} else {
				model.status = "response not delivered: " + terminalSafe(reason)
			}
			var finalization tea.Cmd
			if pendingMatches {
				pendingKey := pendingSendStateKey(rejectedPending)
				_, stateSynced := model.stateSynced[pendingKey]
				_, stateManaged := model.stateManaged[pendingKey]
				// durable is deliberately false while an already-existing phase=false
				// row is being replaced with phase=true. The row still exists and must
				// be deleted before the rejected inbox can be confirmed.
				waitsForDelete := model.stateStore != nil && model.stateBound &&
					(stateSynced || stateManaged)
				resumeRecoveries := pendingIndex < 0 &&
					(rejectedPending.recovered || model.recoveryDrainPaused)
				finalization = model.beginRejectionFinalization(
					rejected.EventID, rejectionScope, pendingKey, waitsForDelete, mirrorWriterPending,
					intentCleanup, resumeRecoveries,
				)
			} else if stalePendingJournal ||
				(model.recoveryDrainPaused && len(model.pendingRecoveries) > 0 &&
					(activeMatches || contextMatches || continuationMatches)) {
				finalization = model.beginRejectionFinalization(
					rejected.EventID, rejectionScope, stalePendingKey, stalePendingJournal, false,
					intentCleanup,
					model.recoveryDrainPaused && len(model.pendingRecoveries) > 0 &&
						(activeMatches || contextMatches || continuationMatches),
				)
			} else {
				finalization = model.beginRejectionFinalization(
					rejected.EventID, rejectionScope, stateRecordKey{}, false, false, intentCleanup, false,
				)
			}
			return model, tea.Batch(waitForNetwork(model.client), finalization)
		}
		if message.Assignment != nil {
			model.connection = connectionConnected
			incoming := *message.Assignment
			commands := []tea.Cmd{waitForNetwork(model.client)}
			if _, recovered := model.recoveredSessions[incoming.SessionKey()]; recovered {
				model.refreshRecoveredAssignment(incoming)
				if model.pending.kind == pendingDelivery && model.pending.recovered &&
					model.pending.assignment.SessionKey() == incoming.SessionKey() {
					if resume := model.resumePendingDelivery(model.pending); resume != nil {
						commands = append(commands, resume)
					}
				}
				model.status = "replayed request is already covered by a recovered durable response event"
				return model, tea.Batch(commands...)
			}
			if model.mirrorManager != nil && mirrorEnabled(incoming) {
				commands = append(commands, prepareMirror(model.mirrorManager, incoming))
			}
			if model.pending.kind == pendingAccept &&
				model.pending.assignment.SessionKey() == incoming.SessionKey() {
				model.pending.assignment = incoming
				model.status = "accept is still being committed locally…"
				return model, tea.Batch(commands...)
			}
			if model.delivery.stage == deliveryConfirming &&
				model.delivery.sessionKey == incoming.SessionKey() {
				model.active = &incoming
				model.status = "checking the confirmed file delivery…"
				return model, tea.Batch(commands...)
			}
			if model.delivery.stage == deliveryConfirmed &&
				model.delivery.sessionKey == incoming.SessionKey() {
				model.active = &incoming
				model.status = "confirmed file delivery is waiting for an exact outbox retry"
				return model, tea.Batch(commands...)
			}
			if model.delivery.stage == deliverySending &&
				model.delivery.sessionKey == incoming.SessionKey() {
				model.delivery.assignment = incoming
				model.status = "file delivery is committed locally · waiting for client Agent results"
				return model, tea.Batch(commands...)
			}
			if model.isSourceSessionReplay(incoming) {
				model.refreshContinuationOrigin(incoming)
				if model.continueHandoff || len(model.continueIDs) > 0 {
					model.status = "source request replayed · still waiting for the client Agent's next turn"
				} else {
					model.status = "completed source request replayed · ignored"
				}
				return model, tea.Batch(commands...)
			}
			if model.continueHandoff && model.continueTier == completion.TierChat &&
				incoming.CallerID == model.continueCaller {
				// Chat callers cannot carry a durable task identity across turns.
				// Surface their next request in Inbox instead of leaving a stale
				// automatic-resume promise on screen.
				model.clearContinuation()
			}
			model.dropParkedChatHandoffs(incoming.CallerID)
			if model.active == nil && model.matchesContinuation(incoming) &&
				model.pending.kind != pendingAccept && model.pending.kind != pendingReject {
				if model.pending.kind != pendingNone && model.pending.kind != pendingAccept &&
					model.pending.kind != pendingReject {
					// The follow-up assignment proves the prior terminal event made
					// it through the gateway even if its local command result is
					// delivered to Bubble Tea out of order.
					if model.pending.recovered && len(model.pendingRecoveries) > 0 {
						// The follow-up proves this recovered event reached the caller,
						// but the new request now owns the operator. Hold later recovered
						// events until that request reaches a terminal/handoff/tool event.
						model.recoveryDrainPaused = true
					}
					model.retainPendingRemainder(model.pending)
					model.pending = pendingSend{}
				}
				if model.delivery.stage == deliverySending {
					model.delivery = deliveryReview{}
				}
				next, acceptCommand := model.beginAccept(incoming, -1, true)
				model = next.(Model)
				if acceptCommand != nil {
					commands = append(commands, acceptCommand)
				} else if model.recoveryDrainPaused && model.pending.kind == pendingNone && model.active == nil {
					// ID allocation failed before the automatic accept entered an
					// outbox. The follow-up remains in Inbox, so no active owner exists
					// to release this pause later.
					model.recoveryDrainPaused = false
					if recovered := model.resumePendingRecovery(); recovered != nil {
						commands = append(commands, recovered)
					}
				}
				return model, tea.Batch(commands...)
			}
			if model.active != nil && model.active.SessionKey() == incoming.SessionKey() {
				model.active = &incoming
				model.delivery = deliveryReview{}
				if !model.taskDirty && !model.taskEditing {
					model.loadAgentTasks(incoming)
				}
				model.status = "reconnected · active request restored"
				return model, tea.Batch(commands...)
			}
			for index := range model.assignments {
				if model.assignments[index].SessionKey() == incoming.SessionKey() {
					model.assignments[index] = incoming
					model.status = fmt.Sprintf("%d request(s) queued", len(model.assignments))
					return model, tea.Batch(commands...)
				}
			}
			model.assignments = append(model.assignments, incoming)
			model.status = fmt.Sprintf("%d request(s) queued", len(model.assignments))
			return model, tea.Batch(commands...)
		}
		return model, waitForNetwork(model.client)
	case rejectedEventConfirmed:
		if message.err != nil {
			// Intent cleanup has already committed (or was unnecessary), while the
			// rejected inbox row remains durable. Retry only its confirmation.
			model.status += " · rejected draft is safe on disk; inbox confirmation failed: " +
				terminalSafe(message.err.Error())
			return model, confirmRejectedEventAttempt(
				model.client, message.eventID, message.attempt+1,
			)
		}
		if message.attempt > 0 {
			model.status = "rejected draft retained · durable inbox confirmation recovered"
		}
		finalization := model.finishRejectionFinalization(message.eventID)
		return model, finalization
	case tea.PasteMsg:
		if model.quitting {
			model.status = "finishing durable recovery work before exit · Ctrl+C again forces quit"
			return model, nil
		}
		if model.deferredSend.kind != pendingNone {
			model.status = "send is waiting for recovery state; input is locked · Esc cancels and keeps the draft"
			return model, nil
		}
		// Paste transports commonly encode line endings as CRLF (and some
		// terminals still send bare CR). Canonicalize those bytes before the
		// terminal-display sanitizer: turning CR into the visible glyph ␍ would
		// otherwise mutate reply text and, more seriously, the bash command sent
		// to the client Agent.
		rawContent := normalizeInputNewlines(message.Content)
		if model.composing {
			model = model.appendComposeInput(terminalSafe(rawContent))
			return model, nil
		}
		if model.active == nil {
			return model, nil
		}
		switch model.focus {
		case focusReply:
			message.Content = rawContent
			return model, model.updateReplyEditor(message)
		case focusCommand:
			message.Content = rawContent
			return model, model.updateCommandEditor(message)
		case focusTasks:
			if model.taskEditing {
				model.taskInput += terminalSafe(singleLinePaste(rawContent))
			}
			return model, nil
		default:
			return model, nil
		}
	case mirrorIntentsDiscarded:
		if message.eventID == "" {
			if message.err != nil {
				model.status += " · failed to discard unsent workspace intent (" +
					terminalSafe(message.reason) + "): " + terminalSafe(message.err.Error())
			}
			return model, nil
		}
		_, finalizingRejection := model.rejectionFinalizers[message.eventID]
		if message.err != nil {
			// ConfirmRejectedEvent must not run until this durable cleanup succeeds.
			// The workerclient inbox therefore remains the crash-recovery source
			// while bounded-backoff retries repair the local mirror provenance.
			model.status += " · rejected draft is safe on disk; workspace intent cleanup failed (" +
				terminalSafe(message.reason) + "): " + terminalSafe(message.err.Error())
			return model, discardRejectedIntentsAttempt(
				message.eventID, message.retry, message.attempt+1,
			)
		}
		if finalizingRejection {
			if message.attempt > 0 {
				model.status = "workspace intent cleanup recovered · waiting for local rejection journal…"
			}
			confirmation := model.completeRejectedIntentCleanup(message.eventID)
			return model, confirmation
		}
		if message.attempt > 0 {
			model.status = "workspace intent cleanup recovered · confirming durable rejected inbox…"
		}
		return model, confirmRejectedEvent(model.client, message.eventID)
	case pendingDeliveryIntentsDiscarded:
		if model.pending.kind != pendingDelivery || model.pending.event.ID != message.eventID ||
			model.pending.deliveryNamespace != message.namespace ||
			model.delivery.stage != deliveryDiscarding {
			return model, nil
		}
		if message.err != nil {
			model.status = "delivery not sent; preserving its recovery journal while workspace intent cleanup retries: " +
				terminalSafe(message.err.Error())
			return model, discardPendingDeliveryIntentsAttempt(
				message.eventID, message.namespace, message.reason,
				message.retry, message.attempt+1,
			)
		}
		followup := model.completePendingDeliveryFailure(message.namespace, errors.New(message.reason))
		return model, followup
	case mirrorPrepared:
		model.invalidateChat()
		if message.err != nil {
			if model.pending.kind == pendingDelivery && model.pending.durable &&
				model.pending.assignment.SessionKey() == message.sessionKey &&
				model.pending.deliveryNamespace == message.namespace &&
				model.delivery.stage == deliveryConfirming &&
				model.delivery.eventID == model.pending.event.ID {
				model.delivery.prepareInFlight = false
				followup := model.failPendingDeliveryConfirmation(message.namespace, message.err)
				return model, followup
			}
			if model.active != nil && model.active.SessionKey() == message.sessionKey {
				model.status = "mirror error: " + message.err.Error()
			}
			return model, nil
		}
		if existing := model.mirrors[message.namespace]; existing != nil &&
			model.pendingDeliveryPinsMirror(message.namespace) {
			// The exact delivery was reviewed against the cached workspace. A
			// concurrent prepare for another session in the same namespace must not
			// redirect its writer/cleanup to a different custom manager instance.
			// This pin spans the phase=true state write, where pending.durable is
			// deliberately false even though the old mirror already owns the intent.
			message.workspace = existing
		} else {
			model.mirrors[message.namespace] = message.workspace
		}
		resumingDelivery := model.pending.kind == pendingDelivery && model.pending.durable &&
			model.pending.assignment.SessionKey() == message.sessionKey &&
			model.pending.deliveryNamespace == message.namespace &&
			model.delivery.stage == deliveryConfirming &&
			model.delivery.eventID == model.pending.event.ID
		if resumingDelivery {
			model.delivery.prepareInFlight = false
		}
		if !resumingDelivery {
			model.requireMirrorReview(message.namespace)
		}
		if model.mirrorWatches == nil {
			model.mirrorWatches = make(map[string]mirrorWatchState)
		}
		commands := make([]tea.Cmd, 0, 2)
		if _, watching := model.mirrorWatches[message.namespace]; !watching {
			startID := model.nextMirrorWatchStartID()
			model.mirrorWatches[message.namespace] = mirrorWatchState{
				startID: startID, starting: true,
			}
			commands = append(commands, startMirrorWatch(message.workspace, message.namespace, startID))
		}
		summary := reconcileSummary(message.report)
		model.pruneMirrorCache()
		if resumingDelivery {
			model.status = "durable file delivery restored · rechecking its exact mirror intent…"
			commands = append(commands, model.resumePendingDelivery(model.pending))
			return model, tea.Batch(commands...)
		}
		if model.active != nil && model.active.SessionKey() == message.sessionKey {
			model.status = summary + " · checking local workspace changes…"
			if review := model.drainDirtyMirrorReview(); review != nil {
				commands = append(commands, review)
			}
		}
		return model, tea.Batch(commands...)
	case mirrorWatchStarted:
		state, exists := model.mirrorWatches[message.namespace]
		if !exists || !state.starting || state.startID != message.startID {
			if message.cancel != nil {
				message.cancel()
			}
			return model, nil
		}
		if message.err != nil {
			return model, model.scheduleMirrorWatchRecovery(
				message.namespace, state, "could not start: "+message.err.Error(),
			)
		}
		if message.events == nil {
			delete(model.mirrorWatches, message.namespace)
			return model, nil
		}
		state.events = message.events
		state.cancel = message.cancel
		state.starting = false
		state.retryPending = false
		model.mirrorWatches[message.namespace] = state
		nextWatch := waitForMirrorChange(message.namespace, message.events)
		if state.failures == 0 {
			return model, nextWatch
		}
		// A full scan after re-establishing the watcher closes the gap between
		// the last failed scan and the new fsnotify subscription.
		model.markMirrorChanged(message.namespace)
		review := model.drainDirtyMirrorReview()
		model.status = "workspace watch recovered · verifying the full workspace before delivery"
		return model, tea.Batch(nextWatch, review)
	case mirrorWatchRetryReady:
		state, exists := model.mirrorWatches[message.namespace]
		if !exists || !state.retryPending || state.startID != message.startID {
			return model, nil
		}
		workspace := model.mirrors[message.namespace]
		if workspace == nil {
			delete(model.mirrorWatches, message.namespace)
			return model, nil
		}
		state.retryPending = false
		state.starting = true
		model.mirrorWatches[message.namespace] = state
		model.status = "restarting workspace watcher…"
		return model, startMirrorWatch(workspace, message.namespace, message.startID)
	case mirrorWatchEvent:
		state, exists := model.mirrorWatches[message.namespace]
		if !exists || state.events != message.events {
			return model, nil
		}
		if !message.open {
			return model, model.scheduleMirrorWatchRecovery(
				message.namespace, state, "event stream closed",
			)
		}
		nextWatch := waitForMirrorChange(message.namespace, message.events)
		if message.event.Err != nil {
			// fsnotify errors can occupy the watcher's coalescing slot and hide
			// its later debounce notification. Treat the error itself as an
			// authoritative full-scan trigger rather than merely showing it.
			model.markMirrorChanged(message.namespace)
			review := model.drainDirtyMirrorReview()
			model.status = "workspace watch warning: " + message.event.Err.Error() +
				" · full workspace review queued"
			return model, tea.Batch(nextWatch, review)
		}
		state.failures = 0
		model.mirrorWatches[message.namespace] = state
		model.markMirrorChanged(message.namespace)
		if model.active == nil ||
			mirrorNamespace(model.active.CallerID, model.active.WorkspaceKey) != message.namespace {
			return model, nextWatch
		}
		if model.responseInFlightFor(model.active) {
			model.status = "workspace changed · queued until the current response event is committed"
			return model, nextWatch
		}
		if _, reviewing := model.mirrorReviewing[message.namespace]; reviewing {
			model.status = "workspace changed again · newest generation queued behind the current review"
			return model, nextWatch
		}
		return model, tea.Batch(nextWatch, model.drainDirtyMirrorReview())
	case mirrorReviewReady:
		model.invalidateChat()
		reviewingGeneration, reviewing := model.mirrorReviewing[message.namespace]
		if !reviewing || reviewingGeneration != message.generation {
			return model, nil
		}
		delete(model.mirrorReviewing, message.namespace)
		if model.active == nil || model.active.SessionKey() != message.sessionKey {
			return model, nil
		}
		if model.mirrorGeneration[message.namespace] != message.generation {
			model.status = "workspace changed during review · refreshing newest generation…"
			return model, model.drainDirtyMirrorReview()
		}
		if message.err != nil {
			model.delivery = deliveryReview{}
			model.status = "review failed: " + message.err.Error()
			return model, nil
		}
		model.delivery = deliveryReview{
			stage: deliveryReviewed, sessionKey: message.sessionKey,
			namespace: message.namespace, generation: message.generation,
			changes: message.changes,
		}
		delete(model.mirrorDirty, message.namespace)
		if len(message.changes) == 0 {
			model.delivery = deliveryReview{}
			if len(message.diagnostics) == 0 {
				model.status = "workspace live · no unconfirmed changes"
			} else {
				model.status = "workspace live · no deliverable changes · " +
					formatMirrorDiagnostics(message.diagnostics)
			}
		} else {
			prefix := "reviewed"
			if message.automatic {
				prefix = "workspace changed · reviewed"
			}
			model.status = fmt.Sprintf("%s %d change(s) · ctrl+p to preview", prefix, len(message.changes))
			if len(message.diagnostics) > 0 {
				model.status += " · " + formatMirrorDiagnostics(message.diagnostics)
			}
		}
		if message.automatic && model.workspaceAutoSend && len(message.changes) > 0 {
			if changesNeedHumanReview(message.changes) {
				model.status += " · warning/conflict requires Human confirmation"
				return model, nil
			}
			if len(message.diagnostics) > 0 {
				model.status += " · skipped workspace entries require Human confirmation"
				return model, nil
			}
			reviewedCount := len(model.delivery.changes)
			previewed, _ := model.previewMirrorDelivery()
			model = previewed.(Model)
			if model.delivery.stage != deliveryPreviewed {
				return model, nil
			}
			if len(model.delivery.changes) != reviewedCount {
				model.status += " · undeliverable changes remain pending; Human confirmation required"
				return model, nil
			}
			return model.confirmMirrorDelivery()
		}
		return model, nil
	case mirrorConfirmationReady:
		model.invalidateChat()
		if finalizer, finalizing := model.rejectionFinalizers[message.eventID]; finalizing {
			if finalizer.mirrorConfirmationPending {
				cleanup := model.settleRejectedMirrorConfirmation(message.eventID)
				return model, cleanup
			}
			return model, nil
		}
		if model.active == nil || model.active.SessionKey() != message.sessionKey ||
			model.pending.kind != pendingDelivery || model.pending.eventID != message.eventID ||
			model.delivery.stage != deliveryConfirming || model.delivery.sessionKey != message.sessionKey ||
			model.delivery.namespace != message.namespace ||
			model.delivery.generation != message.generation || model.delivery.eventID != message.eventID {
			return model, nil
		}
		model.delivery.intentWriterInFlight = false
		if message.err != nil {
			followup := model.failPendingDeliveryConfirmation(message.namespace, message.err)
			return model, followup
		}
		assignment := *model.active
		model.delivery.calls = append([]completion.ToolCall(nil), message.calls...)
		model.delivery.stage = deliveryConfirmed
		model.pending.deliveryIntentRecorded = true
		// The first phase=true write must already cover every editor pane visible
		// when mirror intent recording completed. startRecordedMirrorDelivery repeats
		// this freeze if the operator edits again while that write is in flight.
		model.pending.remainingDraft = sanitizePersistedDraft(
			model.remainingDraftAfterSend(pendingDelivery),
		)
		model.pending.draftOrigins = sortedDraftOriginKeys(model.draftOrigins)
		if model.stateStore != nil {
			// Record the intent phase in the same save-ahead row before the worker
			// outbox can observe the event. A restart can then distinguish a merely
			// staged delivery from one whose provenance must be preserved.
			model.pending.durable = false
			model.status = "file intent recorded · saving its durable handoff phase…"
			return model, nil
		}
		return model.sendConfirmedMirrorDelivery(assignment)
	case eventSent:
		model.invalidateChat()
		if message.intentKey.kind == workerStatePendingSendKind && len(message.restorePayload) > 0 {
			model.stateManaged[message.intentKey] = struct{}{}
			model.stateSynced[message.intentKey] = string(message.restorePayload)
		}
		if message.intentErr != nil && model.pending.eventID == message.eventID &&
			model.pending.kind != pendingNone {
			attempt := model.pending.retryAttempt + 1
			model.pending.retryAttempt = attempt
			if message.restorePending {
				model.status = "durable draft-restore decision retained; state commit will retry: " +
					terminalSafe(message.intentErr.Error())
			} else if model.pending.kind == pendingDelivery {
				model.status = "durable file delivery retained; local outbox handoff will retry: " +
					terminalSafe(message.intentErr.Error())
			} else {
				model.status = "durable response intent retained; local handoff will retry: " +
					terminalSafe(message.intentErr.Error())
			}
			delay := rejectionRetryDelay(attempt)
			if message.restorePending {
				sendErr := message.err
				return model, tea.Tick(delay, func(time.Time) tea.Msg {
					return pendingRestoreRetry{
						eventID: message.eventID, attempt: attempt, sendErr: sendErr,
					}
				})
			}
			return model, tea.Tick(delay, func(time.Time) tea.Msg {
				return pendingSendRetry{eventID: message.eventID, attempt: attempt}
			})
		}
		if message.err != nil && model.pending.eventID == message.eventID &&
			model.pending.kind != pendingNone &&
			!errors.Is(message.err, workerclient.ErrEventNotStored) &&
			!errors.Is(message.err, workerclient.ErrEventRejectionPending) &&
			!errors.Is(message.err, workerclient.ErrEventPreviouslyRejected) {
			attempt := model.pending.retryAttempt + 1
			model.pending.retryAttempt = attempt
			model.status = "local outbox outcome is uncertain; retrying the exact event ID: " +
				terminalSafe(message.err.Error())
			delay := rejectionRetryDelay(attempt)
			return model, tea.Tick(delay, func(time.Time) tea.Msg {
				return pendingSendRetry{eventID: message.eventID, attempt: attempt}
			})
		}
		if message.err != nil {
			if errors.Is(message.err, workerclient.ErrEventRejectionPending) {
				// The exact event is no longer sendable, but its body and rejection
				// remain in workerclient's durable inbox. Keep pending intact until
				// that correctness-bearing message arrives; restoring here as an
				// ordinary send failure would merge the same draft twice.
				if _, alreadyApplied := model.handledRejections[message.eventID]; !alreadyApplied {
					model.status = "event rejected · recovering draft from durable inbox…"
				}
				return model, nil
			}
			if errors.Is(message.err, workerclient.ErrEventPreviouslyRejected) &&
				model.pending.eventID == message.eventID && model.pending.kind != pendingNone {
				finalization := model.finalizePreviouslyRejectedPending()
				return model, finalization
			}
			if model.pending.eventID != "" && model.pending.eventID == message.eventID {
				pending := model.pending
				if pending.recovered {
					// A local handoff failure means this event never covered its source
					// request. Re-enable source replay and hold later recovered events
					// behind the operator's correction until it reaches a stable end.
					delete(model.recoveredSessions, pending.assignment.SessionKey())
					if len(model.pendingRecoveries) > 0 {
						model.recoveryDrainPaused = true
					}
				}
				if pending.kind == pendingDelivery && model.stateStore == nil {
					model.clearContinuation()
					assignment := pending.assignment
					model.active = &assignment
					model.lastContext = cloneAssignment(pending.context)
					model.pending.durable = false
					model.delivery.stage = deliveryConfirmed
					model.focus = focusTasks
					model.status = "confirmed delivery not queued; Enter retries the exact in-memory event: " +
						terminalSafe(message.err.Error())
					return model, nil
				}
				switch pending.kind {
				case pendingAccept:
					model.pending = pendingSend{}
					if pending.automatic {
						model.queueAssignmentOnce(pending.assignment)
					}
					model.focus = focusTasks
					model.status = "accept failed; request kept in Inbox: " + message.err.Error()
				case pendingReject:
					model.pending = pendingSend{}
					model.focus = focusTasks
					model.status = "reject failed; request kept in Inbox: " + message.err.Error()
				default:
					model = model.restorePendingSend(message.err)
				}
			} else {
				model.status = "send failed: " + message.err.Error()
			}
		} else if model.pending.eventID != "" && model.pending.eventID == message.eventID {
			pending := model.pending
			model.retainPendingRemainder(pending)
			model.draftOrigins = make(map[stateRecordKey]string)
			model.draftAuthority = mergeDraftAuthorities(
				model.draftAuthority, "after-event", pending.event.ID,
			)
			model.pending = pendingSend{}
			switch pending.kind {
			case pendingAccept:
				model.removeQueuedSession(pending.assignment.SessionKey())
				model = model.activateAssignment(pending.assignment)
				if mirrorEnabled(pending.assignment) {
					namespace := mirrorNamespace(pending.assignment.CallerID, pending.assignment.WorkspaceKey)
					if model.mirrors[namespace] != nil {
						model.requireMirrorReview(namespace)
					}
				}
			case pendingReject:
				model.removeQueuedAssignment(pending.assignment.SessionKey(), pending.selected)
				model.status = "request rejected"
			case pendingReply:
				if model.active == nil {
					if len(model.assignments) > 0 {
						model.status = fmt.Sprintf(
							"response event committed locally · %d request(s) waiting in Inbox",
							len(model.assignments),
						)
					}
					break
				}
				model.status = "stream open · continue replying or hand the turn to the Agent"
				model.ui.chatFollow = true
			case pendingCommand, pendingTasks, pendingAdvancedTools:
				if len(model.assignments) > 0 {
					model.status = fmt.Sprintf(
						"response event committed locally · %d request(s) waiting in Inbox",
						len(model.assignments),
					)
				}
			case pendingDelivery:
				count := len(pending.event.ToolCalls)
				model.delivery = deliveryReview{}
				model.ui.chatFollow = true
				model.status = fmt.Sprintf(
					"confirmed · %d file change(s) queued · waiting for client Agent result", count,
				)
			}
			followup := model.drainDirtyMirrorReview()
			var recovered tea.Cmd
			if pending.recovered {
				recovered = model.resumePendingRecovery()
			} else if model.recoveryDrainPaused && model.active == nil {
				model.recoveryDrainPaused = false
				recovered = model.resumePendingRecovery()
			}
			model.pruneMirrorCache()
			return model, tea.Batch(followup, recovered)
		}
		followup := model.drainDirtyMirrorReview()
		var recovered tea.Cmd
		if model.recoveryDrainPaused && model.pending.kind == pendingNone && model.active == nil {
			// This covers a failed automatic accept: its follow-up remains visible in
			// Inbox, while older committed recovery events are free to keep draining.
			model.recoveryDrainPaused = false
			recovered = model.resumePendingRecovery()
		}
		model.pruneMirrorCache()
		return model, tea.Batch(followup, recovered)
	case tea.KeyPressMsg:
		return model.updateKey(message)
	default:
		return model, nil
	}
}

func (model *Model) resumePendingRecovery() tea.Cmd {
	if model.recoveryDrainPaused || model.pending.kind != pendingNone || len(model.pendingRecoveries) == 0 {
		return nil
	}
	next := model.pendingRecoveries[0]
	if model.active != nil && recoveredPendingNeedsActive(next) &&
		model.active.SessionKey() != next.assignment.SessionKey() {
		// Progress and workspace delivery recovery both need the active editor/
		// review surface. Never replace a request which an earlier recovered
		// progress event has left open; terminal events may still drain headlessly.
		model.recoveryDrainPaused = true
		return nil
	}
	model.pendingRecoveries = append([]pendingSend(nil), model.pendingRecoveries[1:]...)
	if model.stateStore != nil {
		next.durable = model.pendingSendStateSynchronized(next)
	}
	model.pending = next
	model.activateRecoveredPending(next)
	model.status = "resuming the next locally committed response event…"
	return model.resumeDurablePending(next)
}

func recoveredPendingNeedsActive(pending pendingSend) bool {
	return pending.kind == pendingDelivery || pending.event.Type == completion.EventProgress
}

func (model *Model) failPendingDeliveryConfirmation(namespace string, failure error) tea.Cmd {
	pending := model.pending
	if pending.kind != pendingDelivery {
		return nil
	}
	if model.delivery.stage == deliveryDiscarding {
		return nil
	}
	model.delivery.stage = deliveryDiscarding
	model.delivery.prepareInFlight = false
	model.delivery.intentWriterInFlight = false
	cleanup := model.discardIntentCommand(
		pending.assignment, pending.event.ToolCalls,
		"delivery confirmation failed: "+failure.Error(),
	)
	if cleanup == nil {
		cleanup = unavailableIntentCleanup(
			"durable workspace delivery intent cleanup is unavailable",
		)
	}
	model.status = "delivery not sent: " + terminalSafe(failure.Error()) +
		" · removing any recorded workspace intent before restoring the request…"
	return discardPendingDeliveryIntentsAttempt(
		pending.event.ID, namespace, failure.Error(), cleanup, 0,
	)
}

func (model *Model) completePendingDeliveryFailure(namespace string, failure error) tea.Cmd {
	pending := model.pending
	if pending.kind != pendingDelivery || model.delivery.stage != deliveryDiscarding {
		return nil
	}
	model.pending = pendingSend{}
	model.delivery = deliveryReview{}
	if pending.recovered {
		// A phase=false delivery can never have reached the outbox. After its exact
		// mirror intents are gone, replay of the source request must be allowed to
		// become the correction surface instead of being suppressed as delivered.
		delete(model.recoveredSessions, pending.assignment.SessionKey())
		if len(model.pendingRecoveries) > 0 {
			model.recoveryDrainPaused = true
		}
	}
	assignment := pending.assignment
	model.active = &assignment
	if pending.context != nil {
		model.lastContext = cloneAssignment(pending.context)
	} else {
		model.lastContext = cloneAssignment(&assignment)
	}
	model.markMirrorChanged(namespace)
	followup := model.drainDirtyMirrorReview()
	model.status = "delivery not sent: " + failure.Error()
	if followup != nil {
		model.status += " · refreshing the changed workspace"
	}
	return followup
}

func (model *Model) resumeDurablePending(pending pendingSend) tea.Cmd {
	if model.stateStore != nil && !pending.durable {
		return nil
	}
	if pending.kind == pendingDelivery {
		return model.resumePendingDelivery(pending)
	}
	if model.pending.event.ID == pending.event.ID && model.pending.kind == pending.kind {
		model.pending.outboxInFlight = true
		pending.outboxInFlight = true
	}
	return sendPersistedEvent(model.client, model.stateStore, pending)
}

func (model *Model) refreshRecoveredAssignment(incoming completion.Assignment) {
	if model.pending.recovered && model.pending.assignment.SessionKey() == incoming.SessionKey() {
		// A recovered row proves only that its event had reached the local state
		// store. The same exact event may also already exist in the durable worker
		// outbox (crash after Put, before Bubble Tea observed success). Rewriting any
		// assignment byte here would make the next idempotent outbox Put conflict
		// with that indistinguishable state. The gateway's task lease is sticky, so
		// an exact replay is the only safe refresh; everything else fails closed.
		if !samePersistedAssignment(model.pending.assignment, incoming) {
			return
		}
		if model.active != nil && model.active.SessionKey() == incoming.SessionKey() {
			model.active = &incoming
		}
		if model.pending.kind == pendingDelivery &&
			model.delivery.sessionKey == incoming.SessionKey() {
			model.delivery.assignment = incoming
		}
	}
}

func samePersistedAssignment(left, right completion.Assignment) bool {
	leftPayload, leftErr := json.Marshal(left)
	rightPayload, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftPayload) == string(rightPayload)
}

func (model Model) pendingDeliveryPinsMirror(namespace string) bool {
	return model.pending.kind == pendingDelivery &&
		model.pending.event.ID != "" &&
		model.pending.deliveryNamespace == namespace &&
		model.delivery.namespace == namespace &&
		model.delivery.eventID == model.pending.event.ID &&
		model.delivery.stage >= deliveryConfirming &&
		model.delivery.stage <= deliveryDiscarding
}

func (model Model) animationActive() bool {
	return model.connection == connectionReconnecting || model.responseCommitInFlight()
}

func (model *Model) pruneMirrorCache() {
	if len(model.mirrors) == 0 {
		return
	}
	keep := make(map[string]struct{}, len(model.assignments)+4)
	keepAssignment := func(assignment *completion.Assignment) {
		if assignment == nil || assignment.CallerID == "" || assignment.WorkspaceKey == "" {
			return
		}
		keep[mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)] = struct{}{}
	}
	keepAssignment(model.active)
	for index := range model.assignments {
		keepAssignment(&model.assignments[index])
	}
	if model.pending.kind != pendingNone {
		keepAssignment(&model.pending.assignment)
	}
	if model.delivery.stage != deliveryNone {
		keepAssignment(&model.delivery.assignment)
		keepAssignment(model.delivery.context)
		if model.delivery.namespace != "" {
			keep[model.delivery.namespace] = struct{}{}
		}
	}
	if model.continueCaller != "" && model.continueWorkspace != "" {
		keep[mirrorNamespace(model.continueCaller, model.continueWorkspace)] = struct{}{}
	}
	for _, continuation := range model.parkedContinuations {
		if continuation.caller != "" && continuation.workspace != "" {
			keep[mirrorNamespace(continuation.caller, continuation.workspace)] = struct{}{}
		}
	}
	for namespace := range model.mirrors {
		if _, ok := keep[namespace]; !ok {
			delete(model.mirrors, namespace)
			delete(model.mirrorDirty, namespace)
			delete(model.mirrorGeneration, namespace)
			delete(model.mirrorReviewing, namespace)
			if watch, watched := model.mirrorWatches[namespace]; watched {
				if watch.cancel != nil {
					watch.cancel()
				}
				delete(model.mirrorWatches, namespace)
			}
		}
	}
}

func (model *Model) removeQueuedAssignment(sessionKey string, preferred int) {
	index := -1
	if preferred >= 0 && preferred < len(model.assignments) &&
		model.assignments[preferred].SessionKey() == sessionKey {
		index = preferred
	} else {
		for candidate := range model.assignments {
			if model.assignments[candidate].SessionKey() == sessionKey {
				index = candidate
				break
			}
		}
	}
	if index < 0 {
		return
	}
	model.assignments = append(model.assignments[:index], model.assignments[index+1:]...)
	if model.selected >= len(model.assignments) && model.selected > 0 {
		model.selected--
	}
}

func (model Model) updateKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	if key.Keystroke() == "ctrl+c" {
		if model.quitting {
			return model, tea.Quit
		}
		if (model.stateStore != nil && model.stateBound && !model.workerStateSynchronized()) ||
			model.correctnessWorkInFlight() {
			model.quitting = true
			model.status = "finishing durable recovery work before exit · Ctrl+C again forces quit"
			return model, nil
		}
		return model, tea.Quit
	}
	if model.quitting {
		model.status = "finishing durable recovery work before exit · Ctrl+C again forces quit"
		return model, nil
	}
	if model.deferredSend.kind != pendingNone {
		if key.Keystroke() == "esc" {
			kind := model.deferredSend.kind
			model.deferredSend = deferredSendIntent{}
			if kind == pendingCommand {
				model.commandConfirm = ""
			}
			model.status = "queued send canceled · draft retained"
		} else {
			model.status = "send is waiting for recovery state; input is locked · Esc cancels and keeps the draft"
		}
		return model, nil
	}
	if model.composing {
		switch key.Keystroke() {
		case "esc":
			model.composing = false
			model.toolCallIDs = nil
			model.input = ""
			model.invalidateChat()
			model.focus = focusReply
		case "backspace":
			model = model.backspaceComposeInput()
		case "pgup":
			model.ui.chatFollow = false
			var command tea.Cmd
			model.ui.chat, command = model.ui.chat.Update(key)
			return model, command
		case "pgdown":
			var command tea.Cmd
			model.ui.chat, command = model.ui.chat.Update(key)
			model.ui.chatFollow = model.ui.chat.AtBottom()
			return model, command
		case "enter":
			model = model.appendComposeInput("\n")
		case "ctrl+s":
			return model.sendDeclaredToolCalls()
		default:
			if key.Key().Text != "" {
				model = model.appendComposeInput(key.Key().Text)
			}
		}
		return model, nil
	}

	// Focus navigation is the only global plain-key behavior. Printable text
	// belongs to the focused editor, so replies containing a/q/t/x (including
	// IME commits and paste) are never mistaken for shortcuts.
	switch key.Keystroke() {
	case "tab":
		if model.taskEditing {
			model.status = "finish the task edit with Enter or cancel it with Esc"
			return model, nil
		}
		if model.focus == focusCommand {
			model.commandConfirm = ""
		}
		model.focus = inputFocus((int(model.focus) + 1) % 3)
		return model, nil
	case "shift+tab":
		if model.taskEditing {
			model.status = "finish the task edit with Enter or cancel it with Esc"
			return model, nil
		}
		if model.focus == focusCommand {
			model.commandConfirm = ""
		}
		model.focus = inputFocus((int(model.focus) + 2) % 3)
		return model, nil
	case "pgup":
		model.ui.chatFollow = false
		var command tea.Cmd
		model.ui.chat, command = model.ui.chat.Update(key)
		return model, command
	case "pgdown":
		var command tea.Cmd
		model.ui.chat, command = model.ui.chat.Update(key)
		model.ui.chatFollow = model.ui.chat.AtBottom()
		return model, command
	}

	switch model.focus {
	case focusReply:
		return model.updateReplyKey(key)
	case focusCommand:
		return model.updateCommandKey(key)
	default:
		return model.updateTaskKey(key)
	}
}

func (model Model) correctnessWorkInFlight() bool {
	if model.delivery.prepareInFlight || model.delivery.intentWriterInFlight ||
		model.delivery.stage == deliveryDiscarding {
		return true
	}
	for _, finalizer := range model.rejectionFinalizers {
		if finalizer.mirrorConfirmationPending || finalizer.cleanupInFlight ||
			!finalizer.cleanupDone || !finalizer.pendingDeleted || finalizer.confirming {
			return true
		}
	}
	return false
}

func (model Model) updateTaskKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	if model.taskEditing {
		return model.updateTaskEditorKey(key)
	}
	if model.delivery.stage == deliveryPreviewed {
		switch key.Keystroke() {
		case "enter":
			return model.confirmMirrorDelivery()
		case "esc":
			model.delivery = deliveryReview{}
			model.invalidateChat()
			model.status = "delivery review canceled"
			return model, nil
		}
	}
	if model.delivery.stage == deliveryConfirmed {
		switch key.Keystroke() {
		case "enter":
			if model.active == nil || model.active.SessionKey() != model.delivery.sessionKey {
				return model, nil
			}
			return model.sendConfirmedMirrorDelivery(*model.active)
		case "esc":
			model.status = "delivery was already confirmed durably · Enter retries the exact event"
			return model, nil
		}
	}
	if model.active == nil {
		return model.updateInboxKey(key)
	}

	_, taskReason := taskTargetForRequest(model.active.Request)
	if model.taskConflict {
		taskReason = "Task result conflict · editing is disabled"
	} else if model.taskSyncWait {
		taskReason = "Task update is still awaiting a client result"
	}
	switch key.Keystroke() {
	case "up", "k":
		if model.taskSelected > 0 {
			model.taskSelected--
		}
	case "down", "j":
		if model.taskSelected+1 < len(model.agentTasks) {
			model.taskSelected++
		}
	case "[":
		model.ui.chatFollow = false
		var command tea.Cmd
		model.ui.chat, command = model.ui.chat.Update(tea.KeyPressMsg{Code: tea.KeyPgUp})
		return model, command
	case "]":
		var command tea.Cmd
		model.ui.chat, command = model.ui.chat.Update(tea.KeyPressMsg{Code: tea.KeyPgDown})
		model.ui.chatFollow = model.ui.chat.AtBottom()
		return model, command
	case "enter", "e":
		if taskReason != "" {
			model.status = taskReason
		} else {
			model = model.beginTaskEdit(false)
		}
	case "n":
		if taskReason != "" {
			model.status = taskReason
		} else {
			model = model.beginTaskEdit(true)
		}
	case "space":
		if taskReason != "" {
			model.status = taskReason
		} else {
			model = model.cycleSelectedTaskStatus()
		}
	case "d":
		if taskReason != "" {
			model.status = taskReason
		} else {
			model = model.deleteSelectedTask()
		}
	case "p":
		if taskReason != "" {
			model.status = taskReason
		} else {
			model = model.cycleSelectedTaskPriority()
		}
	case "ctrl+s":
		return model.sendAgentTasks()
	case "c":
		model.focus = focusReply
	case "x":
		model.focus = focusCommand
	case "t":
		return model.openDeclaredToolComposer()
	case "v":
		model.detailMode = !model.detailMode
		model.ui.chatTop = true
		model.ui.chatFollow = false
		model.invalidateChat()
	case "R", "shift+r":
		return model.startMirrorReview()
	case "ctrl+p":
		return model.previewMirrorDelivery()
	case "esc":
		if model.delivery.stage == deliveryReviewed || model.delivery.stage == deliveryPreviewed {
			model.delivery = deliveryReview{}
			model.invalidateChat()
			model.status = "delivery review canceled"
		}
	}
	return model, nil
}

func (model Model) updateInboxKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.Keystroke() {
	case "up", "k":
		if model.selected > 0 {
			model.selected--
			model.invalidateChat()
		}
	case "down", "j":
		if model.selected+1 < len(model.assignments) {
			model.selected++
			model.invalidateChat()
		}
	case "a":
		return model.acceptSelected()
	case "enter":
		model.status = "press a to accept or r to reject the selected request"
	case "r":
		return model.rejectSelected()
	case "c":
		model.focus = focusReply
	case "x":
		model.focus = focusCommand
	case "v":
		model.detailMode = !model.detailMode
		model.ui.chatTop = true
		model.ui.chatFollow = false
		model.invalidateChat()
	}
	return model, nil
}

func (model Model) updateTaskEditorKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch key.Keystroke() {
	case "esc":
		model.taskEditing = false
		model.taskEditIndex = -1
		model.taskInput = ""
		model.status = "task edit canceled"
	case "backspace":
		model.taskInput = removeLastRune(model.taskInput)
	case "enter":
		return model.commitTaskEdit(), nil
	default:
		if key.Key().Text != "" && !strings.ContainsAny(key.Key().Text, "\r\n") {
			model.taskInput += key.Key().Text
		}
	}
	return model, nil
}

func singleLinePaste(content string) string {
	content = normalizeInputNewlines(content)
	return strings.ReplaceAll(content, "\n", " ")
}

func normalizeInputNewlines(content string) string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	return strings.ReplaceAll(content, "\r", "\n")
}

func (model Model) beginTaskEdit(create bool) Model {
	model.taskEditing = true
	model.taskInput = ""
	model.taskEditIndex = -1
	if !create && len(model.agentTasks) > 0 {
		if model.taskSelected >= len(model.agentTasks) {
			model.taskSelected = len(model.agentTasks) - 1
		}
		model.taskEditIndex = model.taskSelected
		model.taskInput = model.agentTasks[model.taskSelected].Content
	}
	if create || len(model.agentTasks) == 0 {
		model.status = "new task · type a description and press Enter"
	} else {
		model.status = "edit task · press Enter to keep the local draft"
	}
	return model
}

func (model Model) commitTaskEdit() Model {
	content := strings.TrimSpace(model.taskInput)
	if content == "" {
		model.status = "task description cannot be empty"
		return model
	}
	if model.taskEditIndex >= 0 && model.taskEditIndex < len(model.agentTasks) {
		model.agentTasks[model.taskEditIndex].Content = content
		model.taskSelected = model.taskEditIndex
	} else {
		model.agentTasks = append(model.agentTasks, agentTask{
			Content: content, Status: taskPending, Priority: "medium",
		})
		model.taskSelected = len(model.agentTasks) - 1
	}
	model.taskDirty = true
	model.taskEditing = false
	model.taskEditIndex = -1
	model.taskInput = ""
	model.status = "task draft changed · Ctrl+S syncs it to the client Agent"
	return model
}

func (model Model) cycleSelectedTaskStatus() Model {
	if len(model.agentTasks) == 0 {
		return model.beginTaskEdit(true)
	}
	if model.taskSelected >= len(model.agentTasks) {
		model.taskSelected = len(model.agentTasks) - 1
	}
	next := taskPending
	switch model.agentTasks[model.taskSelected].Status {
	case taskPending:
		next = taskInProgress
	case taskInProgress:
		next = taskCompleted
	case taskCompleted:
		next = taskPending
	}
	if next == taskInProgress {
		for index := range model.agentTasks {
			if model.agentTasks[index].Status == taskInProgress {
				model.agentTasks[index].Status = taskPending
			}
		}
	}
	model.agentTasks[model.taskSelected].Status = next
	model.taskDirty = true
	model.status = "task status changed · Ctrl+S syncs it to the client Agent"
	return model
}

func (model Model) deleteSelectedTask() Model {
	if len(model.agentTasks) == 0 {
		return model
	}
	if model.taskSelected >= len(model.agentTasks) {
		model.taskSelected = len(model.agentTasks) - 1
	}
	model.agentTasks = append(model.agentTasks[:model.taskSelected], model.agentTasks[model.taskSelected+1:]...)
	if model.taskSelected >= len(model.agentTasks) && model.taskSelected > 0 {
		model.taskSelected--
	}
	model.taskDirty = true
	model.status = "task removed from draft · Ctrl+S syncs the list"
	return model
}

func (model Model) cycleSelectedTaskPriority() Model {
	if len(model.agentTasks) == 0 {
		return model
	}
	if model.taskSelected >= len(model.agentTasks) {
		model.taskSelected = len(model.agentTasks) - 1
	}
	switch normalizePriority(model.agentTasks[model.taskSelected].Priority) {
	case "medium":
		model.agentTasks[model.taskSelected].Priority = "high"
	case "high":
		model.agentTasks[model.taskSelected].Priority = "low"
	default:
		model.agentTasks[model.taskSelected].Priority = "medium"
	}
	model.taskDirty = true
	model.status = "task priority changed · Ctrl+S syncs the list"
	return model
}

func (model Model) updateReplyKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	if model.active == nil {
		if key.Keystroke() == "esc" {
			model.focus = focusTasks
		} else if key.Key().Text != "" {
			model.status = "reply disabled · accept an Inbox request first"
		}
		return model, nil
	}
	if model.responseInFlightFor(model.active) {
		switch key.Keystroke() {
		case "enter", "ctrl+s", "ctrl+enter", "ctrl+r", "ctrl+d":
			model.status = "previous response segment is still being committed; your draft is retained"
			return model, nil
		case "esc":
			model.focus = focusTasks
			return model, nil
		default:
			return model, model.updateReplyEditor(key)
		}
	}
	switch key.Keystroke() {
	case "esc":
		model.commandConfirm = ""
		model.focus = focusTasks
	case "shift+enter", "ctrl+j":
		return model, model.updateReplyEditor(key)
	case "enter", "ctrl+s":
		return model.sendReply(completion.EventProgress, false)
	case "ctrl+enter", "ctrl+r":
		// Handoff may intentionally have an empty draft after one or more
		// streamed segments. It still closes this HTTP response and gives the
		// client Agent the next turn.
		return model.sendReplyWithOptions(completion.EventClarification, true, true)
	case "ctrl+d":
		return model.finishConversation()
	default:
		return model, model.updateReplyEditor(key)
	}
	return model, nil
}

func (model Model) updateCommandKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	_, reason := model.commandTarget()
	if reason != "" {
		if key.Keystroke() == "esc" {
			model.commandConfirm = ""
			model.focus = focusTasks
		} else if key.Key().Text != "" || key.Keystroke() == "enter" {
			model.status = reason
		}
		return model, nil
	}
	switch key.Keystroke() {
	case "esc":
		model.commandConfirm = ""
		model.focus = focusTasks
	case "shift+enter", "ctrl+j":
		model.commandConfirm = ""
		return model, model.updateCommandEditor(key)
	case "enter", "ctrl+s":
		if model.responseInFlightFor(model.active) {
			model.status = "another response event is still being committed; command draft retained"
			return model, nil
		}
		return model.sendCommand()
	default:
		model.commandConfirm = ""
		return model, model.updateCommandEditor(key)
	}
	return model, nil
}

func (model Model) responseInFlight() bool {
	return model.pending.kind != pendingNone ||
		model.delivery.stage == deliveryConfirming || model.delivery.stage == deliveryConfirmed ||
		model.delivery.stage == deliverySending
}

func (model Model) responseInFlightFor(assignment *completion.Assignment) bool {
	return model.responseInFlight() ||
		(assignment != nil && model.rejectionFinalizationInFlight(*assignment))
}

func (model Model) rejectionFinalizationInFlight(assignment completion.Assignment) bool {
	scope := rejectedDraftScopeKey(assignment)
	for _, finalizer := range model.rejectionFinalizers {
		// An empty scope can only come from an incomplete embedder/test-created
		// finalizer. Treat it as global rather than accidentally failing open.
		if finalizer.scope == "" || finalizer.scope == scope {
			return true
		}
	}
	return false
}

func (model Model) responseCommitInFlight() bool {
	return model.pending.kind != pendingNone ||
		model.delivery.stage == deliveryConfirming || model.delivery.stage == deliverySending
}

func (model *Model) requireMirrorReview(namespace string) uint64 {
	if model.mirrorDirty == nil {
		model.mirrorDirty = make(map[string]bool)
	}
	if model.mirrorGeneration == nil {
		model.mirrorGeneration = make(map[string]uint64)
	}
	generation := model.mirrorGeneration[namespace]
	if generation == 0 {
		generation = 1
		model.mirrorGeneration[namespace] = generation
	}
	model.mirrorDirty[namespace] = true
	return generation
}

func (model *Model) markMirrorChanged(namespace string) uint64 {
	if model.mirrorDirty == nil {
		model.mirrorDirty = make(map[string]bool)
	}
	if model.mirrorGeneration == nil {
		model.mirrorGeneration = make(map[string]uint64)
	}
	generation := model.mirrorGeneration[namespace] + 1
	if generation == 0 {
		generation = 1
	}
	model.mirrorGeneration[namespace] = generation
	model.mirrorDirty[namespace] = true
	return generation
}

func (model *Model) nextMirrorWatchStartID() uint64 {
	model.mirrorWatchSequence++
	if model.mirrorWatchSequence == 0 {
		model.mirrorWatchSequence = 1
	}
	return model.mirrorWatchSequence
}

func (model *Model) scheduleMirrorWatchRecovery(
	namespace string,
	state mirrorWatchState,
	reason string,
) tea.Cmd {
	if state.cancel != nil {
		state.cancel()
	}
	state.events = nil
	state.cancel = nil
	state.starting = false
	state.retryPending = true
	state.failures++
	state.startID = model.nextMirrorWatchStartID()
	model.mirrorWatches[namespace] = state

	// Any watcher failure creates an observation gap. Advance the generation
	// and force Review, which is the authoritative full-tree diff. If another
	// review or response commit is in flight, the existing generation guards
	// retain this dirty generation and drain it afterward.
	model.markMirrorChanged(namespace)
	review := model.drainDirtyMirrorReview()
	delay := mirrorWatchRetryDelay(state.failures)
	model.status = fmt.Sprintf(
		"workspace watch stopped (%s) · full review queued · retrying in %s",
		terminalSafe(reason), delay,
	)
	return tea.Batch(
		review,
		waitToRestartMirrorWatch(namespace, state.startID, delay),
	)
}

func (model *Model) beginMirrorReview(
	workspace MirrorWorkspace,
	assignment completion.Assignment,
	automatic bool,
) tea.Cmd {
	namespace := mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)
	if model.mirrorReviewing == nil {
		model.mirrorReviewing = make(map[string]uint64)
	}
	if _, reviewing := model.mirrorReviewing[namespace]; reviewing {
		return nil
	}
	generation := model.requireMirrorReview(namespace)
	model.mirrorReviewing[namespace] = generation
	if automatic {
		return reviewMirrorAutomatically(workspace, assignment, generation)
	}
	return reviewMirror(workspace, assignment, generation)
}

func (model *Model) drainDirtyMirrorReview() tea.Cmd {
	if model.active == nil || !mirrorEnabled(*model.active) || model.responseInFlightFor(model.active) {
		return nil
	}
	namespace := mirrorNamespace(model.active.CallerID, model.active.WorkspaceKey)
	if !model.mirrorDirty[namespace] {
		return nil
	}
	if _, reviewing := model.mirrorReviewing[namespace]; reviewing {
		return nil
	}
	workspace := model.mirrors[namespace]
	if workspace == nil {
		return nil
	}
	model.delivery = deliveryReview{}
	model.invalidateChat()
	model.status = "workspace changed · refreshing delivery review…"
	return model.beginMirrorReview(workspace, *model.active, true)
}

func (model Model) acceptSelected() (tea.Model, tea.Cmd) {
	if model.active != nil {
		model.status = "finish the active request before accepting another"
		return model, nil
	}
	if len(model.assignments) == 0 {
		return model, nil
	}
	assignment := model.assignments[model.selected]
	if model.responseInFlight() || model.rejectionFinalizationInFlight(assignment) {
		model.status = "another response event is still being committed"
		return model, nil
	}
	return model.beginAccept(assignment, model.selected, false)
}

func (model Model) beginAccept(assignment completion.Assignment, selected int, automatic bool) (tea.Model, tea.Cmd) {
	if model.rejectionFinalizationInFlight(assignment) {
		model.status = "the rejected response is still being finalized; this request remains in Inbox"
		if automatic {
			model.queueAssignmentOnce(assignment)
		}
		return model, nil
	}
	eventID, err := allocateEventID()
	if err != nil {
		model.status = "allocate accept event id: " + err.Error()
		if automatic {
			model.queueAssignmentOnce(assignment)
		}
		return model, nil
	}
	if automatic {
		// A continuation may have been queued while a rejection finalizer owned
		// its task scope. Once the barrier clears, its replay moves that exact
		// request from Inbox into the automatic accept transaction.
		model.removeQueuedSession(assignment.SessionKey())
	}
	event := completion.Event{ID: eventID, Type: completion.EventAccepted}
	model.pending = pendingSend{
		kind: pendingAccept, eventID: eventID, assignment: assignment,
		selected: selected, automatic: automatic, event: event,
	}
	model.focus = focusTasks
	model.status = "accepting " + terminalSafe(assignment.TaskID) + "…"
	return model, sendEvent(model.client, assignment, event)
}

func (model Model) activateAssignment(assignment completion.Assignment) Model {
	model.invalidateChat()
	sameTaskScope := sameAgentTaskScope(model.lastContext, assignment)
	draft, restoredDraft := model.rejectedDraftForAssignment(assignment)
	model.draftOrigins = make(map[stateRecordKey]string)
	persistedDraft, persistedOrigin, restoredPersistedDraft := model.takePersistentDraft(assignment)
	if restoredPersistedDraft {
		if digest := persistedDraftDigest(persistedDraft); digest != "" {
			model.draftOrigins[persistedOrigin] = digest
		}
	}
	if restoredDraft {
		if persisted, ok := persistedDraftFromRejected(draft); ok {
			if digest := persistedDraftDigest(persisted); digest != "" {
				model.draftOrigins[stateRecordKey{
					scope: stateScope(draft.assignment), kind: workerStateDraftKind,
				}] = digest
			}
		}
	}
	mergedPersistentDraft := false
	if restoredDraft && restoredPersistedDraft {
		if local, ok := rejectedDraftFromPersisted(assignment, persistedDraft); ok {
			// The parked rejection is an undelivered source; the independently
			// persisted editor is newer local work. Merge them before touching the
			// UI so apply order can never overwrite either authority on the same pane.
			draft = mergeRejectedDraft(&draft, local)
			mergedPersistentDraft = true
		}
	}
	preserveTaskDraft := sameTaskScope && (model.taskDirty || model.taskEditing) ||
		restoredDraft && draft.hasTasks || restoredPersistedDraft && persistedDraft.HasTasks
	if !sameTaskScope && !preserveTaskDraft {
		model.resetAgentTasks()
	}
	model.active = &assignment
	model.draftAuthority = assignmentDraftAuthority(assignment)
	model.rememberContext(assignment)
	// loadAgentTasks may derive a correctness-bearing pending/conflict state and
	// a diagnostic from the caller's transcript. Do not overwrite either with
	// the generic acceptance status below. An expected continuation must also
	// preserve an unsynchronized local task draft/editor in the same stable task
	// scope; reloading its older caller history here used to discard user data.
	model.status = ""
	if preserveTaskDraft {
		if restoredPersistedDraft {
			model.status = "accepted " + assignment.TaskID + " · recovered local draft and unsynced Tasks"
		} else {
			model.status = "continuation accepted · unsynced Tasks draft retained"
		}
	} else {
		model.loadAgentTasks(assignment)
	}
	taskStatus := model.status
	model.delivery = deliveryReview{}
	model.focus = focusReply
	if model.draftSession != assignment.SessionKey() && !restoredDraft && !restoredPersistedDraft {
		model.setReplyValue("")
		model.draftReplyRejected = ""
		model.draftRejectedEvents = nil
		model.draftRejectedKinds = nil
		model.setCommandValue("")
		model.composing = false
		model.input = ""
		model.toolCallIDs = nil
	}
	if restoredPersistedDraft && !mergedPersistentDraft {
		model.applyPersistentDraft(persistedDraft)
	}
	if restoredDraft {
		model.applyRejectedDraft(draft, false)
		model.deleteRejectedDraft(assignment)
	}
	model.draftSession = assignment.SessionKey()
	model.detailMode = false
	if taskStatus != "" {
		model.status = taskStatus
	} else if restoredPersistedDraft {
		model.status = "accepted " + assignment.TaskID + " · recovered local draft"
	} else {
		model.status = "accepted " + assignment.TaskID + " · Enter streams · Ctrl+R hands off · Ctrl+D ends"
	}
	model.clearContinuation()
	return model
}

func sameAgentTaskScope(previous *completion.Assignment, next completion.Assignment) bool {
	if previous == nil {
		return false
	}
	return previous.CallerID == next.CallerID &&
		previous.WorkspaceKey == next.WorkspaceKey &&
		previous.TaskID == next.TaskID &&
		previous.CapabilityTier == next.CapabilityTier
}

// sameRejectedDraftScope is stricter than the visual/task helper above.
// Chat has no durable workspace/task correctness identity, so a new Chat
// completion from the same caller must never inherit a rejected draft merely
// because it repeated an advisory task_id. Tool-capable tiers may carry the
// draft across SessionKeys using their validated stable identity; every tier
// may still merge multiple rejected events from the exact same HTTP session.
func sameRejectedDraftScope(previous *completion.Assignment, next completion.Assignment) bool {
	if previous == nil {
		return false
	}
	if previous.SessionKey() == next.SessionKey() {
		return true
	}
	if previous.CapabilityTier != completion.TierRemoteTools &&
		previous.CapabilityTier != completion.TierWorkspace {
		return false
	}
	return sameAgentTaskScope(previous, next)
}

func (model Model) rejectSelected() (tea.Model, tea.Cmd) {
	if len(model.assignments) == 0 {
		return model, nil
	}
	assignment := model.assignments[model.selected]
	if model.responseInFlight() || model.rejectionFinalizationInFlight(assignment) {
		model.status = "another response event is still being committed"
		return model, nil
	}
	eventID, err := allocateEventID()
	if err != nil {
		model.status = "allocate reject event id: " + err.Error()
		return model, nil
	}
	event := completion.Event{
		ID: eventID, Type: completion.EventRejected,
		ErrorCode: "human_rejected", Error: "human rejected the request",
	}
	model.pending = pendingSend{
		kind: pendingReject, eventID: eventID, assignment: assignment, selected: model.selected,
		event: event,
	}
	model.status = "rejecting " + terminalSafe(assignment.TaskID) + "…"
	return model, sendEvent(model.client, assignment, event)
}

func (model Model) openDeclaredToolComposer() (tea.Model, tea.Cmd) {
	if model.active == nil {
		model.status = "advanced tool input disabled · accept an Inbox request first"
		return model, nil
	}
	if len(model.active.Request.Tools) == 0 {
		model.status = "caller declared no tools for this request"
		return model, nil
	}
	callID, err := canonical.NewOpaqueID("tool_")
	if err != nil {
		model.status = "allocate tool-call id: " + err.Error()
		return model, nil
	}
	model.composing = true
	model.toolCallIDs = []string{callID}
	model.input = ""
	model.invalidateChat()
	return model, nil
}

func removeLastRune(value string) string {
	if value == "" {
		return value
	}
	runes := []rune(value)
	return string(runes[:len(runes)-1])
}

func (model Model) startMirrorReview() (tea.Model, tea.Cmd) {
	if model.active == nil {
		return model, nil
	}
	if !mirrorEnabled(*model.active) {
		model.delivery = deliveryReview{}
		if model.active.CapabilityTier == completion.TierChat || model.active.CapabilityTier == "" {
			model.status = "Chat tier has no workspace mirror"
		} else {
			model.status = "workspace delivery requires the exact human-shim adapter"
		}
		return model, nil
	}
	namespace := mirrorNamespace(model.active.CallerID, model.active.WorkspaceKey)
	workspace := model.mirrors[namespace]
	if workspace == nil {
		model.status = "mirror is still preparing; try review again"
		return model, nil
	}
	if model.responseInFlightFor(model.active) {
		model.requireMirrorReview(namespace)
		model.status = "workspace review queued until the current response event is committed"
		return model, nil
	}
	if _, reviewing := model.mirrorReviewing[namespace]; reviewing {
		model.status = "workspace review is already in progress"
		return model, nil
	}
	model.requireMirrorReview(namespace)
	model.delivery = deliveryReview{}
	model.invalidateChat()
	model.status = "reviewing mirror changes…"
	return model, model.beginMirrorReview(workspace, *model.active, false)
}

func (model Model) previewMirrorDelivery() (tea.Model, tea.Cmd) {
	if model.active == nil ||
		model.delivery.stage != deliveryReviewed ||
		model.delivery.sessionKey != model.active.SessionKey() {
		return model, nil
	}
	if len(model.delivery.changes) == 0 {
		model.status = "nothing to deliver"
		return model, nil
	}
	report, err := workmirror.BuildToolCallsForProfile(
		model.delivery.changes, model.active.Adapter, model.active.Root,
	)
	calls := report.Calls
	if err == nil && len(calls) == 0 {
		model.delivery.warnings = report.Warnings
		model.status = "preview has no deliverable changes"
		if len(report.Warnings) > 0 {
			model.status += " · " + strings.Join(report.Warnings, "; ")
		}
		model.invalidateChat()
		return model, nil
	}
	if err == nil {
		err = validateMirrorCalls(model.active.Request, calls)
	}
	var eventID string
	if err == nil {
		eventID, err = canonical.NewOpaqueID("event_")
	}
	if err == nil {
		// Fail at preview, before provenance is recorded, when the exact native
		// tool payload cannot fit the durable worker protocol. Reserve the full
		// stable-key width for WorkerID because SendEvent adds it later.
		event := completion.Event{
			ID: eventID, Type: completion.EventToolCalls,
			WorkerID: strings.Repeat("w", 128), ToolCalls: calls,
		}
		err = workerproto.ValidateEnvelopeSize(workerproto.MessageEvent, workerproto.Event{
			CallerID: model.active.CallerID, IdempotencyKey: model.active.IdempotencyKey, Event: event,
		}, workerproto.MaxWireMessageBytes)
	}
	if err != nil {
		model.delivery = deliveryReview{}
		model.status = "preview failed: " + err.Error()
		return model, nil
	}
	model.delivery.stage = deliveryPreviewed
	model.delivery.changes = report.Changes
	model.delivery.calls = calls
	model.delivery.warnings = report.Warnings
	model.delivery.eventID = eventID
	model.ui.chatTop = true
	model.ui.chatFollow = false
	model.invalidateChat()
	model.status = "preview ready · enter to confirm, esc to cancel"
	if len(report.Warnings) > 0 {
		model.status = fmt.Sprintf("preview ready with %d adapter warning(s) · review, then Enter to confirm", len(report.Warnings))
	}
	return model, nil
}

func (model Model) confirmMirrorDelivery() (tea.Model, tea.Cmd) {
	if model.active == nil ||
		model.delivery.stage != deliveryPreviewed ||
		model.delivery.sessionKey != model.active.SessionKey() {
		return model, nil
	}
	if model.responseInFlightFor(model.active) {
		model.status = "delivery retained; an earlier rejected response in this task is still being finalized"
		return model, nil
	}
	workspace := model.mirrors[model.delivery.namespace]
	if workspace == nil {
		model.delivery = deliveryReview{}
		model.status = "delivery not sent: mirror is unavailable"
		return model, nil
	}
	assignment := *model.active
	event := completion.Event{
		ID: model.delivery.eventID, Type: completion.EventToolCalls,
		ToolCalls: append([]completion.ToolCall(nil), model.delivery.calls...),
	}
	model.pending = pendingSend{
		kind: pendingDelivery, eventID: event.ID, assignment: assignment,
		context: cloneAssignment(model.lastContext), event: event,
		toolCalls:          append([]completion.ToolCall(nil), event.ToolCalls...),
		remainingDraft:     model.remainingDraftAfterSend(pendingDelivery),
		deliveryNamespace:  model.delivery.namespace,
		deliveryGeneration: model.delivery.generation,
		deliveryChanges:    cloneMirrorChanges(model.delivery.changes),
	}
	model.delivery.stage = deliveryConfirming
	model.status = "confirmed · saving the exact file delivery before mirror intent and outbox…"
	return model.commitPendingEvent(event)
}

func (model Model) sendConfirmedMirrorDelivery(assignment completion.Assignment) (tea.Model, tea.Cmd) {
	if model.pending.kind != pendingDelivery || (model.stateStore != nil && !model.pending.durable) ||
		model.pending.assignment.SessionKey() != assignment.SessionKey() ||
		model.delivery.stage != deliveryConfirmed || model.delivery.sessionKey != assignment.SessionKey() ||
		model.pending.event.ID == "" || len(model.pending.event.ToolCalls) == 0 {
		return model, nil
	}
	model.pending.deliveryIntentRecorded = true
	command := model.startRecordedMirrorDelivery()
	return model, command
}

func (model *Model) startRecordedMirrorDelivery() tea.Cmd {
	if model.pending.kind != pendingDelivery || !model.pending.deliveryIntentRecorded ||
		(model.stateStore != nil && !model.pending.durable) ||
		model.delivery.stage != deliveryConfirmed || model.active == nil ||
		model.active.SessionKey() != model.pending.assignment.SessionKey() {
		return nil
	}
	latestDraft, _ := model.currentPersistedDraft()
	latestDraft = sanitizePersistedDraft(latestDraft)
	if persistedDraftEditorDigest(latestDraft, model.pending.event.ID) !=
		persistedDraftEditorDigest(model.pending.remainingDraft, model.pending.event.ID) {
		// RecordDeliveryIntents and the phase=true worker-state write are
		// asynchronous; the operator may keep editing independent panes while they
		// run. Freeze the newest editor snapshot into the pending journal and cross
		// one more save-ahead boundary before clearing active or touching the outbox.
		model.pending.remainingDraft = latestDraft
		model.pending.draftOrigins = sortedDraftOriginKeys(model.draftOrigins)
		if model.stateStore != nil {
			model.pending.durable = false
			model.status = "file intent recorded · saving the latest editor snapshot before outbox handoff…"
			return nil
		}
	}
	assignment := model.pending.assignment
	beforeContext := cloneAssignment(model.lastContext)
	for _, call := range model.pending.event.ToolCalls {
		model.appendLocalToolCall(assignment, call)
	}
	model.expectContinuation(assignment, model.pending.event.ToolCalls)
	model.delivery.stage = deliverySending
	model.delivery.assignment = assignment
	model.delivery.context = beforeContext
	model.pending.outboxInFlight = true
	model.active = nil
	model.focus = focusTasks
	model.status = fmt.Sprintf(
		"confirmed · %d durable file tool call(s) entering the worker outbox…",
		len(model.pending.event.ToolCalls),
	)
	return sendPersistedEvent(model.client, model.stateStore, model.pending)
}

type commandTarget struct {
	name                string
	commandField        string
	cwdField            string
	cwdValue            string
	descriptionRequired bool
}

func (model Model) commandTarget() (commandTarget, string) {
	if model.active == nil {
		return commandTarget{}, "command disabled · accept an Inbox request first"
	}
	return commandTargetForAssignment(*model.active)
}

func commandTargetForAssignment(assignment completion.Assignment) (commandTarget, string) {
	for _, tool := range assignment.Request.Tools {
		if tool.Namespace != "" || tool.Name != "bash" {
			continue
		}
		if commandToolNeedsExecAuthorization(assignment, tool.Name) && !assignment.ExecAllowed {
			return commandTarget{}, "command disabled · remote exec is not authorized for this task"
		}
		target, err := declaredBashTarget(tool)
		if err != nil {
			return commandTarget{}, "bash command pane disabled: " + err.Error() + " · use [t] advanced tool input"
		}
		return target, ""
	}
	if assignment.Adapter == nil || assignment.Adapter.Exec == nil {
		return commandTarget{}, "command disabled · caller declared no bash tool"
	}
	if commandToolNeedsExecAuthorization(assignment, assignment.Adapter.Exec.Name) && !assignment.ExecAllowed {
		return commandTarget{}, "command disabled · remote exec is not authorized for this task"
	}
	execTool := assignment.Adapter.Exec
	declared := false
	for _, tool := range assignment.Request.Tools {
		if tool.Namespace == "" && tool.Name == execTool.Name {
			declared = true
			break
		}
	}
	if !declared {
		return commandTarget{}, fmt.Sprintf("command disabled · caller did not declare adapter exec tool %q", execTool.Name)
	}
	commandField := execTool.Args["command"]
	if commandField == "" {
		commandField = "command"
	}
	cwdValue := assignment.Root
	if cwdValue == "" {
		cwdValue = "/workspace"
	}
	return commandTarget{
		name: execTool.Name, commandField: commandField, cwdField: execTool.CWDField,
		cwdValue: cwdValue,
	}, ""
}

// commandToolNeedsExecAuthorization mirrors the gateway's exact adapter
// authorization rule. Outside chat tier, both privileged and unclassified
// caller-native tools require the task's explicit exec opt-in; merely declaring
// a command-shaped schema must never make the TUI offer a call the gateway will
// deterministically reject.
func commandToolNeedsExecAuthorization(assignment completion.Assignment, name string) bool {
	// Empty is the parsed/default chat tier used by embedders and older in-memory
	// assignment fixtures; wire assignments are normalized to TierChat.
	if assignment.CapabilityTier == "" || assignment.CapabilityTier == completion.TierChat {
		return false
	}
	if assignment.Adapter == nil {
		return true
	}
	authorization, classified := assignment.Adapter.AuthorizeTool(name)
	return !classified || authorization == adapter.ToolAuthorizationPrivileged
}

func declaredBashTarget(tool canonical.Tool) (commandTarget, error) {
	type property struct {
		Type string `json:"type"`
	}
	var schema struct {
		Type                 string              `json:"type"`
		Properties           map[string]property `json:"properties"`
		Required             []string            `json:"required"`
		AdditionalProperties *bool               `json:"additionalProperties"`
	}
	if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
		return commandTarget{}, errors.New("invalid input schema")
	}
	if schema.Type != "object" {
		return commandTarget{}, errors.New("input schema root is not an object")
	}
	if schema.AdditionalProperties != nil && *schema.AdditionalProperties {
		return commandTarget{}, errors.New("input schema cannot set additionalProperties=true")
	}
	wantedTypes := map[string][]string{
		"command": {"string"}, "description": {"string"}, "workdir": {"string"},
		"timeout": {"integer", "number"},
	}
	for name, definition := range schema.Properties {
		allowed, known := wantedTypes[name]
		if !known {
			return commandTarget{}, fmt.Errorf("property %q needs advanced input", name)
		}
		valid := false
		for _, expected := range allowed {
			valid = valid || definition.Type == expected
		}
		if !valid {
			return commandTarget{}, fmt.Errorf("property %q has unsupported type %q", name, definition.Type)
		}
	}
	command, ok := schema.Properties["command"]
	if !ok || command.Type != "string" {
		return commandTarget{}, errors.New("command:string property is required")
	}
	target := commandTarget{name: "bash", commandField: "command"}
	commandRequired := false
	for _, field := range schema.Required {
		if _, ok := schema.Properties[field]; !ok {
			return commandTarget{}, fmt.Errorf("required field %q has no property definition", field)
		}
		switch field {
		case "command":
			commandRequired = true
		case "description":
			target.descriptionRequired = true
		default:
			return commandTarget{}, fmt.Errorf("required field %q needs advanced input", field)
		}
	}
	if !commandRequired {
		return commandTarget{}, errors.New("command is not required by the schema")
	}
	return target, nil
}

func commandDescription(command string) string {
	line := strings.TrimSpace(strings.SplitN(command, "\n", 2)[0])
	if line == "" {
		return "Human-requested command"
	}
	return "Human: " + boundedSingleLine(line, 72)
}

func (model *Model) rememberContext(assignment completion.Assignment) {
	copy := assignment
	model.lastContext = &copy
	model.ui.chatFollow = true
	model.invalidateChat()
}

func cloneAssignment(source *completion.Assignment) *completion.Assignment {
	if source == nil {
		return nil
	}
	copy := *source
	copy.Request.Messages = append([]canonical.Message(nil), source.Request.Messages...)
	return &copy
}

func (model *Model) expectContinuation(assignment completion.Assignment, calls []completion.ToolCall) {
	model.parkCurrentContinuation()
	model.continueCaller = assignment.CallerID
	model.continueWorkspace = assignment.WorkspaceKey
	model.continueTaskID = assignment.TaskID
	model.continueTier = assignment.CapabilityTier
	model.continueOrigin = assignment.SessionKey()
	model.continueHandoff = false
	model.continueContext = cloneAssignment(model.lastContext)
	model.continueIDs = make(map[string]struct{}, len(calls))
	for _, call := range calls {
		if call.ID != "" {
			model.continueIDs[call.ID] = struct{}{}
		}
	}
}

func (model *Model) expectHandoff(assignment completion.Assignment) {
	model.parkCurrentContinuation()
	model.continueCaller = assignment.CallerID
	model.continueWorkspace = assignment.WorkspaceKey
	model.continueTaskID = assignment.TaskID
	model.continueTier = assignment.CapabilityTier
	model.continueOrigin = assignment.SessionKey()
	model.continueIDs = make(map[string]struct{})
	model.continueHandoff = true
	model.continueContext = cloneAssignment(model.lastContext)
}

func (model Model) isSourceSessionReplay(assignment completion.Assignment) bool {
	if model.active != nil {
		return false
	}
	sessionKey := assignment.SessionKey()
	if model.continueOrigin != "" && sessionKey == model.continueOrigin {
		return true
	}
	for _, continuation := range model.parkedContinuations {
		if continuation.origin == sessionKey {
			return true
		}
	}
	// A plain Final has no continuation expectation, but its request may still
	// be replayed while the local outbox ACK and gateway retirement cross. The
	// same SessionKey can only identify that original idempotent request, never
	// a new turn, so it must not re-enter Inbox after completion.
	return model.lastContext != nil && sessionKey == model.lastContext.SessionKey()
}

func (model *Model) refreshContinuationOrigin(incoming completion.Assignment) {
	if model.pending.kind != pendingNone &&
		model.pending.assignment.SessionKey() == incoming.SessionKey() {
		model.pending.assignment = incoming
	}
	if model.lastContext != nil && model.lastContext.SessionKey() == incoming.SessionKey() {
		// Preserve locally appended human text/tool calls while refreshing lease
		// and adapter metadata from the replayed assignment.
		request := model.lastContext.Request
		refreshed := incoming
		refreshed.Request = request
		model.lastContext = &refreshed
	}
	model.removeQueuedSession(incoming.SessionKey())
}

func (model *Model) removeQueuedSession(sessionKey string) {
	kept := model.assignments[:0]
	for index := range model.assignments {
		if model.assignments[index].SessionKey() != sessionKey {
			kept = append(kept, model.assignments[index])
		}
	}
	model.assignments = kept
	if len(model.assignments) == 0 {
		model.selected = 0
	} else if model.selected >= len(model.assignments) {
		model.selected = len(model.assignments) - 1
	}
}

func (model *Model) queueAssignmentOnce(assignment completion.Assignment) {
	for index := range model.assignments {
		if model.assignments[index].SessionKey() == assignment.SessionKey() {
			model.assignments[index] = assignment
			return
		}
	}
	model.assignments = append(model.assignments, assignment)
}

func (model *Model) matchesContinuation(assignment completion.Assignment) bool {
	if model.matchesCurrentContinuation(assignment) {
		return true
	}
	for index := range model.parkedContinuations {
		continuation := model.parkedContinuations[index]
		if !matchesContinuationState(continuation, assignment) {
			continue
		}
		model.parkedContinuations = append(
			model.parkedContinuations[:index], model.parkedContinuations[index+1:]...,
		)
		if model.continueOrigin != "" {
			model.parkCurrentContinuation()
		}
		model.loadContinuation(continuation)
		if model.continueContext != nil {
			model.lastContext = cloneAssignment(model.continueContext)
		}
		return true
	}
	return false
}

func (model Model) matchesCurrentContinuation(assignment completion.Assignment) bool {
	return matchesContinuationState(model.currentContinuation(), assignment)
}

func matchesContinuationState(state continuationState, assignment completion.Assignment) bool {
	if state.caller == "" || assignment.CallerID != state.caller {
		return false
	}
	// Tool-capable tiers carry the stable correctness identity across
	// completions. Chat requests do not, so handoff there is deliberately a
	// visible wait state rather than an unsafe auto-accept heuristic.
	if state.handoff {
		return assignment.CapabilityTier == state.tier &&
			assignment.CapabilityTier != completion.TierChat && assignment.CapabilityTier != "" &&
			assignment.WorkspaceKey == state.workspace &&
			assignment.TaskID == state.taskID
	}
	if len(state.ids) == 0 {
		return false
	}
	if assignment.CapabilityTier != completion.TierChat && assignment.CapabilityTier != "" &&
		(assignment.CapabilityTier != state.tier ||
			assignment.WorkspaceKey != state.workspace || assignment.TaskID != state.taskID) {
		return false
	}
	seen := make(map[string]struct{}, len(state.ids))
	for _, message := range assignment.Request.Messages {
		for _, block := range message.Blocks {
			if block.Type != canonical.BlockToolResult {
				continue
			}
			if _, ok := state.ids[block.ToolCallID]; ok {
				seen[block.ToolCallID] = struct{}{}
			}
		}
	}
	return len(seen) == len(state.ids)
}

func (model Model) currentContinuation() continuationState {
	return continuationState{
		caller: model.continueCaller, workspace: model.continueWorkspace,
		taskID: model.continueTaskID, tier: model.continueTier,
		origin: model.continueOrigin, ids: cloneIDSet(model.continueIDs),
		handoff: model.continueHandoff, context: cloneAssignment(model.continueContext),
	}
}

func (model *Model) loadContinuation(state continuationState) {
	model.continueCaller = state.caller
	model.continueWorkspace = state.workspace
	model.continueTaskID = state.taskID
	model.continueTier = state.tier
	model.continueOrigin = state.origin
	model.continueIDs = cloneIDSet(state.ids)
	model.continueHandoff = state.handoff
	model.continueContext = cloneAssignment(state.context)
}

func (model *Model) parkCurrentContinuation() {
	state := model.currentContinuation()
	if state.origin == "" {
		return
	}
	for index := range model.parkedContinuations {
		if model.parkedContinuations[index].origin == state.origin {
			model.parkedContinuations = append(
				model.parkedContinuations[:index], model.parkedContinuations[index+1:]...,
			)
			break
		}
	}
	model.parkedContinuations = append(model.parkedContinuations, state)
	if len(model.parkedContinuations) > maxParkedContinuations {
		model.parkedContinuations = append(
			[]continuationState(nil),
			model.parkedContinuations[len(model.parkedContinuations)-maxParkedContinuations:]...,
		)
	}
	model.clearContinuation()
}

func (model Model) hasContinuationOrigin(sessionKey string) bool {
	if sessionKey != "" && model.continueOrigin == sessionKey {
		return true
	}
	for _, continuation := range model.parkedContinuations {
		if continuation.origin == sessionKey {
			return true
		}
	}
	return false
}

func (model *Model) removeContinuationOrigin(sessionKey string) {
	if model.continueOrigin == sessionKey {
		model.clearContinuation()
	}
	kept := model.parkedContinuations[:0]
	for _, continuation := range model.parkedContinuations {
		if continuation.origin != sessionKey {
			kept = append(kept, continuation)
		}
	}
	model.parkedContinuations = kept
}

func (model *Model) dropParkedChatHandoffs(callerID string) {
	kept := model.parkedContinuations[:0]
	for _, continuation := range model.parkedContinuations {
		if continuation.caller == callerID && continuation.handoff &&
			(continuation.tier == completion.TierChat || continuation.tier == "") {
			continue
		}
		kept = append(kept, continuation)
	}
	model.parkedContinuations = kept
}

func cloneIDSet(source map[string]struct{}) map[string]struct{} {
	copy := make(map[string]struct{}, len(source))
	for id := range source {
		copy[id] = struct{}{}
	}
	return copy
}

func (model *Model) clearContinuation() {
	model.continueCaller = ""
	model.continueWorkspace = ""
	model.continueTaskID = ""
	model.continueTier = ""
	model.continueOrigin = ""
	model.continueIDs = make(map[string]struct{})
	model.continueHandoff = false
	model.continueContext = nil
}

func (model Model) restorePendingSend(sendErr error) Model {
	model.invalidateChat()
	pending := model.pending
	replyTail := model.replyInput
	if model.draftReplyRejected != "" {
		replyTail = replyTailAfterRejectedPrefix(model.replyInput, model.draftReplyRejected)
	}
	commandTail := model.commandInput
	assignment := pending.assignment
	model.active = &assignment
	if pending.context != nil {
		model.lastContext = cloneAssignment(pending.context)
	} else {
		model.rememberContext(assignment)
	}
	model.detailMode = false
	model.clearContinuation()
	switch pending.kind {
	case pendingReply:
		// pending.reply is the complete editor value captured for this new event;
		// it may already contain older restored prefixes. The new event supersedes
		// that boundary, so appending the old prefix would duplicate it.
		model.draftReplyRejected = pending.reply
		model.setReplyValue(joinDraftSegments(model.draftReplyRejected, replyTail))
		model.focus = focusReply
	case pendingCommand:
		model.setCommandValue(joinDraftSegments(pending.command, commandTail))
		model.commandConfirm = ""
		model.focus = focusCommand
	case pendingTasks:
		model.agentTasks = append([]agentTask(nil), pending.tasks...)
		model.taskDirty = true
		model.taskSyncWait = false
		model.focus = focusTasks
	case pendingAdvancedTools:
		model.composing = true
		model.input = pending.toolInput
		model.toolCallIDs = append([]string(nil), pending.toolCallIDs...)
		model.focus = focusTasks
	}
	model.draftAuthority = mergeDraftAuthorities(
		eventDraftAuthority(pending.event.ID), model.draftAuthority,
	)
	model.draftRejectedEvents = uniqueEventIDs(append(
		append([]string(nil), model.draftRejectedEvents...), eventIDSlice(pending.event.ID)...,
	)...)
	model.draftRejectedKinds = mergeEventKinds(
		model.draftRejectedKinds, eventKindMap(pending.event.ID, pending.kind),
	)
	model.pending = pendingSend{}
	model.draftOrigins = make(map[stateRecordKey]string)
	model.status = "send failed; draft restored: " + sendErr.Error()
	return model
}

func (model Model) rejectedDraftFromPending(pending pendingSend) (rejectedDraftState, bool) {
	draft := rejectedDraftState{
		assignment:         pending.assignment,
		authority:          eventDraftAuthority(pending.event.ID),
		rejectedEventIDs:   eventIDSlice(pending.event.ID),
		rejectedEventKinds: eventKindMap(pending.event.ID, pending.kind),
		kind:               pending.kind,
		selected:           model.taskSelected,
	}
	switch pending.kind {
	case pendingReply:
		draft.hasReply = true
		draft.replyRejected = pending.reply
		draft.replyTail = model.replyInput
		draft.reply = joinDraftSegments(draft.replyRejected, draft.replyTail)
	case pendingCommand:
		draft.hasCommand = true
		draft.command = joinDraftSegments(pending.command, model.commandInput)
	case pendingTasks:
		draft.hasTasks = true
		draft.tasks = append([]agentTask(nil), pending.tasks...)
		draft.taskDirty = true
		draft.taskEditIndex = -1
	case pendingAdvancedTools:
		draft.hasTools = true
		draft.toolInput = pending.toolInput
		rejectedEventID := pending.event.ID
		if rejectedEventID == "" {
			rejectedEventID = pending.eventID
		}
		ids, ok := replacementToolCallIDs(rejectedEventID, len(pending.toolCallIDs))
		if !ok {
			return rejectedDraftState{}, false
		}
		draft.toolCallIDs = ids
	default:
		return rejectedDraftState{}, false
	}
	return draft, true
}

func rejectedDraftFromPersisted(
	assignment completion.Assignment,
	persisted persistedDraft,
) (rejectedDraftState, bool) {
	draft := rejectedDraftState{
		assignment: assignment, authority: persisted.Authority,
		rejectedEventIDs:   append([]string(nil), persisted.RejectedEventIDs...),
		rejectedEventKinds: cloneEventKinds(persisted.RejectedEventKinds),
		taskEditIndex:      -1,
	}
	if draft.authority == "" {
		draft.authority = assignmentDraftAuthority(assignment)
	}
	switch persisted.Focus {
	case persistedFocusCommand:
		draft.kind = pendingCommand
	case persistedFocusTasks:
		draft.kind = pendingTasks
	default:
		draft.kind = pendingReply
	}
	if persisted.Reply != "" {
		draft.hasReply = true
		draft.reply = persisted.Reply
		if persisted.ReplyRejected != "" || persisted.ReplyTail != "" {
			draft.replyRejected = persisted.ReplyRejected
			draft.replyTail = persisted.ReplyTail
		} else {
			// An ordinary saved editor row has no rejected-prefix provenance, so a
			// later rejected segment belongs before this unsent local text.
			draft.replyTail = persisted.Reply
		}
	}
	if persisted.Command != "" {
		draft.hasCommand = true
		draft.command = persisted.Command
	}
	if persisted.HasTasks {
		draft.hasTasks = true
		draft.tasks = restoreTasks(persisted.Tasks)
		draft.selected = persisted.TaskSelected
		draft.taskDirty = persisted.TaskDirty
		draft.taskEditing = persisted.TaskEditing
		draft.taskEditIndex = persisted.TaskEditIndex
		draft.taskInput = persisted.TaskInput
	}
	if persisted.ToolInput != "" || len(persisted.ToolCallIDs) > 0 {
		draft.hasTools = true
		draft.toolInput = persisted.ToolInput
		draft.toolCallIDs = append([]string(nil), persisted.ToolCallIDs...)
		if persisted.Focus == persistedFocusTasks {
			draft.kind = pendingAdvancedTools
		}
	}
	return draft, draft.hasReply || draft.hasCommand || draft.hasTasks || draft.hasTools
}

func rejectedDraftFromEvent(assignment completion.Assignment, event completion.Event) (rejectedDraftState, bool) {
	draft := rejectedDraftState{
		assignment: assignment, authority: eventDraftAuthority(event.ID),
		rejectedEventIDs: eventIDSlice(event.ID), taskEditIndex: -1,
	}
	switch event.Type {
	case completion.EventProgress, completion.EventFinal, completion.EventClarification:
		text := localTextForEvent(event)
		if text == "" {
			return rejectedDraftState{}, false
		}
		draft.kind = pendingReply
		draft.hasReply = true
		draft.reply = text
		draft.replyRejected = text
		draft.rejectedEventKinds = eventKindMap(event.ID, draft.kind)
		return draft, true
	case completion.EventToolCalls:
		if len(event.ToolCalls) == 0 {
			return rejectedDraftState{}, false
		}
		// Mirror delivery is regenerated from the current scratch tree after a
		// fresh review. Reconstructing its human_* calls in the generic composer
		// would bypass that review/CAS workflow.
		if rejectedMirrorEvent(assignment, event) {
			return rejectedDraftState{}, false
		}
		if len(event.ToolCalls) == 1 {
			if pull, ok := workspacePullDraftFromCall(assignment, event.ToolCalls[0]); ok {
				draft.kind = pendingCommand
				draft.hasCommand = true
				draft.command = ":pull " + pull
				draft.rejectedEventKinds = eventKindMap(event.ID, draft.kind)
				return draft, true
			}
		}
		if target, reason := taskTargetForRequest(assignment.Request); reason == "" &&
			len(event.ToolCalls) == 1 && event.ToolCalls[0].Namespace == "" &&
			event.ToolCalls[0].Name == target.name {
			items, err := tasksFromInput(event.ToolCalls[0].Input, target)
			if err == nil {
				draft.kind = pendingTasks
				draft.hasTasks = true
				draft.tasks = items
				draft.taskDirty = true
				draft.taskEditIndex = -1
				draft.rejectedEventKinds = eventKindMap(event.ID, draft.kind)
				return draft, true
			}
		}
		if target, reason := commandTargetForAssignment(assignment); reason == "" &&
			len(event.ToolCalls) == 1 && event.ToolCalls[0].Namespace == "" &&
			event.ToolCalls[0].Name == target.name {
			if command, ok := commandDraftFromCall(target, event.ToolCalls[0]); ok {
				draft.kind = pendingCommand
				draft.hasCommand = true
				draft.command = command
				draft.rejectedEventKinds = eventKindMap(event.ID, draft.kind)
				return draft, true
			}
		}
		input, ids, ok := advancedDraftFromCalls(event.ToolCalls)
		if !ok {
			return rejectedDraftState{}, false
		}
		ids, ok = replacementToolCallIDs(event.ID, len(ids))
		if !ok {
			return rejectedDraftState{}, false
		}
		draft.kind = pendingAdvancedTools
		draft.hasTools = true
		draft.toolInput = input
		draft.toolCallIDs = ids
		draft.rejectedEventKinds = eventKindMap(event.ID, draft.kind)
		return draft, true
	default:
		return rejectedDraftState{}, false
	}
}

func commandDraftFromCall(target commandTarget, call completion.ToolCall) (string, bool) {
	command, ok := call.Input[target.commandField].(string)
	if !ok {
		return "", false
	}
	expected := map[string]string{target.commandField: command}
	if target.cwdField != "" {
		expected[target.cwdField] = target.cwdValue
	}
	if target.descriptionRequired {
		expected["description"] = commandDescription(command)
	}
	if len(call.Input) != len(expected) {
		return "", false
	}
	for field, value := range expected {
		actual, ok := call.Input[field].(string)
		if !ok || actual != value {
			return "", false
		}
	}
	return command, true
}

func workspacePullDraftFromCall(
	assignment completion.Assignment,
	call completion.ToolCall,
) (string, bool) {
	profile := assignment.Adapter
	if profile == nil || profile.Key() != adapter.OpenCodeID+"@"+adapter.OpenCodeVersion ||
		profile.Exec == nil || call.Namespace != "" || call.Name != profile.Exec.Name || len(call.Input) != 2 {
		return "", false
	}
	commandField := profile.Exec.Args["command"]
	command, commandOK := call.Input[commandField].(string)
	workdir, workdirOK := call.Input[profile.Exec.CWDField].(string)
	if !commandOK || !workdirOK || workdir != assignment.Root {
		return "", false
	}
	const prefix = "opencode debug file read --pure "
	quoted := strings.TrimPrefix(command, prefix)
	if quoted == command || len(quoted) < 2 || quoted[0] != '\'' || quoted[len(quoted)-1] != '\'' {
		return "", false
	}
	value := strings.ReplaceAll(quoted[1:len(quoted)-1], "'\"'\"'", "'")
	reencoded := "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
	if reencoded != quoted {
		return "", false
	}
	if strings.HasPrefix(value, "./-") {
		value = strings.TrimPrefix(value, "./")
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) || clean != value {
		return "", false
	}
	return clean, true
}

func rejectedMirrorEvent(assignment completion.Assignment, event completion.Event) bool {
	if event.Type != completion.EventToolCalls || len(event.ToolCalls) == 0 || !mirrorEnabled(assignment) {
		return false
	}
	for _, call := range event.ToolCalls {
		if !mirrorMutationCall(assignment, call) {
			return false
		}
	}
	return true
}

func mirrorMutationCall(assignment completion.Assignment, call completion.ToolCall) bool {
	if !mirrorEnabled(assignment) || call.Namespace != "" {
		return false
	}
	profile := assignment.Adapter
	if profile.Write != nil && profile.Write.Name == call.Name {
		return true
	}
	if profile.Edit != nil && profile.Edit.Name == call.Name {
		return true
	}
	if profile.Delete != nil && profile.Delete.Name == call.Name {
		return true
	}
	if profile.Rename != nil && profile.Rename.Name == call.Name {
		return true
	}
	return false
}

func (model Model) discardIntentCommand(
	assignment completion.Assignment,
	calls []completion.ToolCall,
	reason string,
) tea.Cmd {
	if assignment.Adapter == nil || len(calls) == 0 {
		return nil
	}
	workspace := model.mirrors[mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)]
	if workspace != nil {
		return discardMirrorIntents(workspace, calls, assignment.Adapter, reason)
	}
	if model.mirrorManager == nil || assignment.CallerID == "" || assignment.WorkspaceKey == "" {
		return nil
	}
	return func() tea.Msg {
		opened, err := model.mirrorManager.Open(assignment.CallerID, assignment.WorkspaceKey)
		if err != nil {
			return mirrorIntentsDiscarded{reason: reason, err: err}
		}
		discarder, ok := opened.(interface {
			DiscardToolIntents([]completion.ToolCall, *adapter.Profile) error
		})
		if !ok {
			return mirrorIntentsDiscarded{
				reason: reason,
				err:    errors.New("mirror does not support durable delivery intent cleanup"),
			}
		}
		return mirrorIntentsDiscarded{reason: reason, err: discarder.DiscardToolIntents(calls, assignment.Adapter)}
	}
}

func unavailableIntentCleanup(reason string) tea.Cmd {
	return func() tea.Msg {
		return mirrorIntentsDiscarded{reason: reason, err: errors.New(reason)}
	}
}

func advancedDraftFromCalls(calls []completion.ToolCall) (string, []string, bool) {
	lines := make([]string, 0, len(calls))
	ids := make([]string, 0, len(calls))
	for _, call := range calls {
		if strings.TrimSpace(call.Name) == "" || strings.TrimSpace(call.ID) == "" {
			return "", nil, false
		}
		payload, err := json.Marshal(call.Input)
		if err != nil {
			return "", nil, false
		}
		lines = append(lines, call.QualifiedName()+" "+string(payload))
		ids = append(ids, call.ID)
	}
	return strings.Join(lines, "\n"), ids, true
}

func replacementToolCallIDs(rejectedEventID string, count int) ([]string, bool) {
	if strings.TrimSpace(rejectedEventID) == "" || count <= 0 {
		return nil, false
	}
	ids := make([]string, count)
	for index := range ids {
		seed := fmt.Sprintf("human/rejected-tool-retry/v1\x00%s\x00%d", rejectedEventID, index)
		digest := sha256.Sum256([]byte(seed))
		ids[index] = "tool_retry_" + hex.EncodeToString(digest[:16])
	}
	return ids, true
}

func assignmentDraftAuthority(assignment completion.Assignment) string {
	return mergeDraftAuthorities(
		"assignment", string(assignment.CapabilityTier), assignment.CallerID,
		assignment.WorkspaceKey, assignment.TaskID, assignment.SessionKey(),
	)
}

func eventDraftAuthority(eventID string) string {
	if strings.TrimSpace(eventID) == "" {
		return ""
	}
	return mergeDraftAuthorities("event", eventID)
}

func eventIDSlice(eventID string) []string {
	if strings.TrimSpace(eventID) == "" {
		return nil
	}
	return []string{eventID}
}

func eventKindMap(eventID string, kind pendingSendKind) map[string]pendingSendKind {
	if strings.TrimSpace(eventID) == "" || kind == pendingNone {
		return nil
	}
	return map[string]pendingSendKind{eventID: kind}
}

func cloneEventKinds(source map[string]pendingSendKind) map[string]pendingSendKind {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]pendingSendKind, len(source))
	for eventID, kind := range source {
		cloned[eventID] = kind
	}
	return cloned
}

func mergeEventKinds(left, right map[string]pendingSendKind) map[string]pendingSendKind {
	merged := cloneEventKinds(left)
	if merged == nil && len(right) > 0 {
		merged = make(map[string]pendingSendKind, len(right))
	}
	for eventID, kind := range right {
		if _, exists := merged[eventID]; !exists {
			merged[eventID] = kind
		}
	}
	return merged
}

func hasRejectedKind(kinds map[string]pendingSendKind, kind pendingSendKind) bool {
	for _, candidate := range kinds {
		if candidate == kind {
			return true
		}
	}
	return false
}

func filterEventProvenance(
	eventIDs []string,
	kinds map[string]pendingSendKind,
	removed pendingSendKind,
) ([]string, map[string]pendingSendKind) {
	keptIDs := make([]string, 0, len(eventIDs))
	keptKinds := make(map[string]pendingSendKind, len(kinds))
	for _, eventID := range eventIDs {
		kind, classified := kinds[eventID]
		if classified && kind == removed {
			continue
		}
		keptIDs = append(keptIDs, eventID)
		if classified {
			keptKinds[eventID] = kind
		}
	}
	if len(keptIDs) == 0 {
		keptIDs = nil
	}
	if len(keptKinds) == 0 {
		keptKinds = nil
	}
	return keptIDs, keptKinds
}

func removeEventProvenance(
	eventIDs []string,
	kinds map[string]pendingSendKind,
	removedEventID string,
) ([]string, map[string]pendingSendKind) {
	if strings.TrimSpace(removedEventID) == "" {
		return append([]string(nil), eventIDs...), cloneEventKinds(kinds)
	}
	keptIDs := make([]string, 0, len(eventIDs))
	keptKinds := cloneEventKinds(kinds)
	delete(keptKinds, removedEventID)
	for _, eventID := range eventIDs {
		if eventID != removedEventID {
			keptIDs = append(keptIDs, eventID)
		}
	}
	if len(keptIDs) == 0 {
		keptIDs = nil
	}
	if len(keptKinds) == 0 {
		keptKinds = nil
	}
	return keptIDs, keptKinds
}

func uniqueEventIDs(groups ...string) []string {
	result := make([]string, 0, len(groups))
	seen := make(map[string]struct{}, len(groups))
	for _, eventID := range groups {
		if strings.TrimSpace(eventID) == "" {
			continue
		}
		if _, exists := seen[eventID]; exists {
			continue
		}
		seen[eventID] = struct{}{}
		result = append(result, eventID)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func hasEventID(eventIDs []string, eventID string) bool {
	for _, candidate := range eventIDs {
		if candidate == eventID {
			return true
		}
	}
	return false
}

func mergeDraftAuthorities(authorities ...string) string {
	hash := sha256.New()
	written := false
	for _, authority := range authorities {
		if authority == "" {
			continue
		}
		written = true
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(authority))
	}
	if !written {
		return ""
	}
	return "draft_" + hex.EncodeToString(hash.Sum(nil))
}

func localTextForEvent(event completion.Event) string {
	if event.Type == completion.EventProgress {
		return strings.TrimSuffix(event.Text, "\n\n")
	}
	return event.Text
}

func (model *Model) rollbackRejectedEvent(event completion.Event) {
	if model.lastContext == nil {
		return
	}
	switch event.Type {
	case completion.EventProgress, completion.EventFinal, completion.EventClarification:
		text := localTextForEvent(event)
		if text != "" {
			model.lastContext.Request.Messages = removeLastLocalText(
				model.lastContext.Request.Messages, text,
			)
		}
	case completion.EventToolCalls:
		for index := len(event.ToolCalls) - 1; index >= 0; index-- {
			model.lastContext.Request.Messages = removeLastLocalToolCall(
				model.lastContext.Request.Messages, event.ToolCalls[index].ID,
			)
		}
	}
	model.invalidateChat()
}

func removeLastLocalText(messages []canonical.Message, text string) []canonical.Message {
	for index := len(messages) - 1; index >= 0; index-- {
		message := messages[index]
		if message.Role != canonical.RoleAssistant || len(message.Blocks) != 1 ||
			message.Blocks[0].Type != canonical.BlockText || message.Blocks[0].Text != text {
			continue
		}
		return append(messages[:index:index], messages[index+1:]...)
	}
	return messages
}

func removeLastLocalToolCall(messages []canonical.Message, callID string) []canonical.Message {
	if callID == "" {
		return messages
	}
	for index := len(messages) - 1; index >= 0; index-- {
		message := messages[index]
		if message.Role != canonical.RoleAssistant || len(message.Blocks) != 1 ||
			message.Blocks[0].Type != canonical.BlockToolUse || message.Blocks[0].ToolCallID != callID {
			continue
		}
		return append(messages[:index:index], messages[index+1:]...)
	}
	return messages
}

func (model *Model) installRejectedDraft(draft rejectedDraftState) bool {
	return model.recordRejectedDraft(draft, true)
}

// storeRejectedDraft retains a rejected queued recovery without replacing the
// editor or focus owned by the currently active request.
func (model *Model) storeRejectedDraft(draft rejectedDraftState) bool {
	return model.recordRejectedDraft(draft, false)
}

func (model *Model) recordRejectedDraft(draft rejectedDraftState, apply bool) bool {
	if model.rejectedDrafts == nil {
		model.rejectedDrafts = make(map[string]rejectedDraftState)
	}
	stateKey := stateRecordKey{
		scope: stateScope(draft.assignment), kind: workerStateDraftKind,
	}
	if saved, exists := model.stateDrafts[stateKey]; exists {
		if local, ok := rejectedDraftFromPersisted(draft.assignment, saved.draft); ok {
			// One durable key must have one in-memory authority. A row with event
			// provenance already contains earlier rejected sources, so it precedes
			// the newly rejected event (and exact inbox replay is deduplicated).
			// Without provenance the row is only an unsent local remainder and the
			// newly rejected source belongs before it.
			var merged rejectedDraftState
			if hasRejectedKind(saved.draft.RejectedEventKinds, draft.kind) {
				merged = mergeRejectedDraft(&local, draft)
			} else {
				merged = mergeRejectedDraft(&draft, local)
			}
			draft = merged
		}
		delete(model.stateDrafts, stateKey)
	}
	key := rejectedDraftScopeKey(draft.assignment)
	existing, exists := model.rejectedDrafts[key]
	if apply && draft.hasReply {
		if draft.replyRejected == "" && draft.replyTail == "" {
			draft.replyRejected = draft.reply
		}
		// Once the rejected reply's local outbox write has completed, the
		// operator may already be typing the next stream segment—even while a
		// command/task/tool event is pending. Capture that unsent reply tail
		// before applying the rejected segment. Only a pending reply owns the
		// same editor tail through rejectedDraftFromPending.
		if model.pending.kind != pendingReply && draft.replyTail == "" {
			if exists && existing.hasReply {
				existing.replyTail = replyTailAfterRejectedPrefix(
					model.replyInput, existing.replyRejected,
				)
				existing.reply = joinDraftSegments(existing.replyRejected, existing.replyTail)
			} else if model.draftSession == draft.assignment.SessionKey() {
				if hasRejectedKind(model.draftRejectedKinds, pendingReply) {
					// The active editor may itself have been materialized from this
					// durable rejected inbox before the assignment replay arrived. In
					// that ordering, its rejected prefix is already represented in
					// draft; only text typed after that prefix is a new local tail.
					draft.replyTail = replyTailAfterRejectedPrefix(
						model.replyInput, model.draftReplyRejected,
					)
				} else {
					draft.replyTail = model.replyInput
				}
				draft.reply = joinDraftSegments(draft.replyRejected, draft.replyTail)
			}
		}
	}
	var existingPointer *rejectedDraftState
	if exists {
		existingPointer = &existing
	}
	copy := mergeRejectedDraft(existingPointer, draft)
	copy.tasks = append([]agentTask(nil), copy.tasks...)
	copy.toolCallIDs = append([]string(nil), copy.toolCallIDs...)
	if !exists {
		model.rejectedDraftOrder = append(model.rejectedDraftOrder, key)
	}
	model.rejectedDrafts[key] = copy
	evicted := false
	for len(model.rejectedDraftOrder) > maxRejectedDraftScopes {
		oldest := model.rejectedDraftOrder[0]
		model.rejectedDraftOrder = model.rejectedDraftOrder[1:]
		delete(model.rejectedDrafts, oldest)
		evicted = true
	}
	if apply {
		model.applyRejectedDraft(copy, true)
		model.draftSession = draft.assignment.SessionKey()
	}
	return evicted
}

func rejectedDraftScopeKey(assignment completion.Assignment) string {
	if assignment.CapabilityTier == completion.TierRemoteTools ||
		assignment.CapabilityTier == completion.TierWorkspace {
		return "task\x00" + string(assignment.CapabilityTier) + "\x00" + assignment.CallerID +
			"\x00" + assignment.WorkspaceKey + "\x00" + assignment.TaskID
	}
	return "session\x00" + assignment.SessionKey()
}

func rejectedDraftScopeFromStateKey(key stateRecordKey) string {
	scope := key.scope
	if scope.SessionKey == "" {
		return ""
	}
	if scope.Tier == completion.TierRemoteTools || scope.Tier == completion.TierWorkspace {
		return "task\x00" + string(scope.Tier) + "\x00" + scope.CallerID +
			"\x00" + scope.WorkspaceKey + "\x00" + scope.TaskID
	}
	return "session\x00" + scope.SessionKey
}

func (model Model) rejectedDraftForAssignment(
	assignment completion.Assignment,
) (rejectedDraftState, bool) {
	if model.rejectedDrafts == nil {
		return rejectedDraftState{}, false
	}
	draft, exists := model.rejectedDrafts[rejectedDraftScopeKey(assignment)]
	return draft, exists
}

func (model *Model) deleteRejectedDraft(assignment completion.Assignment) {
	key := rejectedDraftScopeKey(assignment)
	if _, exists := model.rejectedDrafts[key]; !exists {
		return
	}
	delete(model.rejectedDrafts, key)
	for index, candidate := range model.rejectedDraftOrder {
		if candidate != key {
			continue
		}
		model.rejectedDraftOrder = append(
			model.rejectedDraftOrder[:index:index], model.rejectedDraftOrder[index+1:]...,
		)
		return
	}
}

// rememberHandledRejection returns true when this exact durable inbox item was
// already applied by the current TUI process. The bounded receipt cache only
// suppresses duplicates already queued around confirmation; durable restart
// recovery remains owned by workerclient's rejected inbox.
func (model *Model) rememberHandledRejection(eventID string) bool {
	if _, exists := model.handledRejections[eventID]; exists {
		return true
	}
	if model.handledRejections == nil {
		model.handledRejections = make(map[string]struct{})
	}
	model.handledRejections[eventID] = struct{}{}
	model.handledRejectOrder = append(model.handledRejectOrder, eventID)
	for len(model.handledRejectOrder) > maxHandledRejectionIDs {
		oldest := model.handledRejectOrder[0]
		model.handledRejectOrder = model.handledRejectOrder[1:]
		delete(model.handledRejections, oldest)
	}
	return false
}

func mergeRejectedDraft(existing *rejectedDraftState, next rejectedDraftState) rejectedDraftState {
	if existing == nil || !sameRejectedDraftScope(&existing.assignment, next.assignment) {
		if next.authority == "" {
			next.authority = assignmentDraftAuthority(next.assignment)
		}
		return next
	}
	merged := *existing
	merged.assignment = next.assignment
	nextAlreadyMaterialized := len(next.rejectedEventIDs) > 0
	duplicateKinds := make(map[pendingSendKind]struct{})
	for _, eventID := range next.rejectedEventIDs {
		if !hasEventID(existing.rejectedEventIDs, eventID) {
			nextAlreadyMaterialized = false
			continue
		}
		if kind, classified := next.rejectedEventKinds[eventID]; classified {
			duplicateKinds[kind] = struct{}{}
		}
	}
	merged.rejectedEventIDs = uniqueEventIDs(append(
		append([]string(nil), existing.rejectedEventIDs...), next.rejectedEventIDs...,
	)...)
	merged.rejectedEventKinds = mergeEventKinds(existing.rejectedEventKinds, next.rejectedEventKinds)
	if nextAlreadyMaterialized {
		merged.authority = existing.authority
	} else {
		merged.authority = mergeDraftAuthorities(existing.authority, next.authority)
	}
	if merged.authority == "" {
		merged.authority = assignmentDraftAuthority(next.assignment)
	}
	merged.kind = next.kind
	if next.hasReply {
		merged.hasReply = true
		if merged.replyRejected == "" && merged.replyTail == "" {
			merged.replyRejected = merged.reply
		}
		if next.replyRejected == "" && next.replyTail == "" {
			next.replyRejected = next.reply
		}
		_, duplicateReply := duplicateKinds[pendingReply]
		if next.replyRejected != "" && !duplicateReply {
			merged.replyRejected = joinDraftSegments(merged.replyRejected, next.replyRejected)
		}
		if next.replyTail != "" {
			merged.replyTail = joinDraftSegments(merged.replyTail, next.replyTail)
		}
		merged.reply = joinDraftSegments(merged.replyRejected, merged.replyTail)
	}
	_, duplicateCommand := duplicateKinds[pendingCommand]
	if next.hasCommand && !duplicateCommand {
		merged.hasCommand = true
		merged.command = joinDraftSegments(merged.command, next.command)
	}
	_, duplicateTasks := duplicateKinds[pendingTasks]
	if next.hasTasks && !duplicateTasks {
		merged.hasTasks = true
		merged.tasks = append([]agentTask(nil), next.tasks...)
		merged.selected = next.selected
		merged.taskDirty = next.taskDirty
		merged.taskEditing = next.taskEditing
		merged.taskEditIndex = next.taskEditIndex
		merged.taskInput = next.taskInput
	}
	_, duplicateTools := duplicateKinds[pendingAdvancedTools]
	if next.hasTools && !duplicateTools {
		merged.hasTools = true
		if merged.toolInput == "" {
			merged.toolInput = next.toolInput
		} else if next.toolInput != "" {
			merged.toolInput += "\n" + next.toolInput
		}
		merged.toolCallIDs = append(append([]string(nil), merged.toolCallIDs...), next.toolCallIDs...)
	}
	return merged
}

func replyTailAfterRejectedPrefix(value, rejectedPrefix string) string {
	if rejectedPrefix == "" {
		return value
	}
	if value == rejectedPrefix {
		return ""
	}
	prefix := rejectedPrefix + "\n\n"
	if strings.HasPrefix(value, prefix) {
		return strings.TrimPrefix(value, prefix)
	}
	// The operator edited the restored prefix itself. There is no lossless way
	// to infer the old boundary, so preserve the entire editor as local text;
	// duplication is safer than silently discarding an edit.
	return value
}

func (model *Model) applyRejectedDraft(draft rejectedDraftState, keepPendingEditor bool) {
	if draft.authority != "" {
		model.draftAuthority = draft.authority
	}
	appliedSource := false
	if draft.hasReply && (!keepPendingEditor || model.pending.kind != pendingReply) {
		model.setReplyValue(draft.reply)
		model.draftReplyRejected = draft.replyRejected
		appliedSource = true
	}
	if draft.hasCommand && (!keepPendingEditor || model.pending.kind != pendingCommand) {
		model.setCommandValue(draft.command)
		model.commandConfirm = ""
		appliedSource = true
	}
	if draft.hasTasks && (!keepPendingEditor || model.pending.kind != pendingTasks) {
		model.agentTasks = append([]agentTask(nil), draft.tasks...)
		model.taskSelected = draft.selected
		if model.taskSelected >= len(model.agentTasks) {
			model.taskSelected = max(0, len(model.agentTasks)-1)
		}
		model.taskDirty = draft.taskDirty
		model.taskEditing = draft.taskEditing
		model.taskEditIndex = draft.taskEditIndex
		model.taskInput = draft.taskInput
		model.taskSyncWait = false
		model.taskConflict = false
		appliedSource = true
	}
	if draft.hasTools && (!keepPendingEditor || model.pending.kind != pendingAdvancedTools) {
		model.composing = true
		model.input = draft.toolInput
		model.toolCallIDs = append([]string(nil), draft.toolCallIDs...)
		appliedSource = true
	}
	if appliedSource {
		model.draftRejectedEvents = uniqueEventIDs(append(
			append([]string(nil), model.draftRejectedEvents...), draft.rejectedEventIDs...,
		)...)
		model.draftRejectedKinds = mergeEventKinds(model.draftRejectedKinds, draft.rejectedEventKinds)
	}
	if keepPendingEditor && model.pending.kind != pendingNone {
		return
	}
	switch draft.kind {
	case pendingReply:
		model.focus = focusReply
	case pendingCommand:
		model.focus = focusCommand
	case pendingTasks, pendingAdvancedTools:
		model.focus = focusTasks
	}
}

func joinDraftSegments(first, second string) string {
	if first == "" {
		return second
	}
	if second == "" {
		return first
	}
	return first + "\n\n" + second
}

func allocateEventID() (string, error) {
	return canonical.NewOpaqueID("event_")
}

func (model *Model) ensureLocalContext(assignment completion.Assignment) {
	if model.lastContext == nil || model.lastContext.SessionKey() != assignment.SessionKey() {
		model.rememberContext(assignment)
	}
}

func (model *Model) loadAgentTasks(assignment completion.Assignment) {
	target, reason := taskTargetForRequest(assignment.Request)
	if reason != "" {
		model.resetAgentTasks()
		return
	}
	history, err := taskHistoryFromRequest(assignment.Request, target)
	if err != nil {
		model.resetAgentTasks()
		model.status = "Tasks history ignored: " + err.Error()
		return
	}
	if history.Found {
		model.agentTasks = history.Items
	} else {
		if !history.Conflict {
			model.agentTasks = nil
		}
	}
	model.taskSelected = 0
	model.taskDirty = false
	model.taskEditing = false
	model.taskEditIndex = -1
	model.taskInput = ""
	model.taskSyncWait = history.Pending
	model.taskConflict = history.Conflict
	if history.Conflict {
		model.status = "Tasks conflict: client result did not match the task update; draft retained"
	}
}

func (model *Model) resetAgentTasks() {
	model.agentTasks = nil
	model.taskSelected = 0
	model.taskDirty = false
	model.taskEditing = false
	model.taskEditIndex = -1
	model.taskInput = ""
	model.taskSyncWait = false
	model.taskConflict = false
}

func (model Model) sendAgentTasks() (tea.Model, tea.Cmd) {
	if model.active == nil {
		model.status = "Tasks sync disabled · accept a request first"
		return model, nil
	}
	if model.blockSendWhileStateUnsettled("task edits", deferredSendIntent{kind: pendingTasks}) {
		return model, nil
	}
	if model.responseInFlightFor(model.active) {
		model.status = "another response event is still being committed; task edits retained"
		return model, nil
	}
	target, reason := taskTargetForRequest(model.active.Request)
	if reason != "" {
		model.status = reason
		return model, nil
	}
	if model.taskEditing {
		model.status = "finish the current task edit with Enter before syncing"
		return model, nil
	}
	if model.taskSyncWait {
		model.status = "Tasks sync blocked: the previous task update is still awaiting a client result"
		return model, nil
	}
	if model.taskConflict {
		model.status = "Tasks sync blocked: resolve the client result conflict first"
		return model, nil
	}
	if !model.taskDirty {
		model.status = "Tasks are already in sync"
		return model, nil
	}
	items, err := validateTaskItems(append([]agentTask(nil), model.agentTasks...), target.kind)
	if err != nil {
		model.status = "Tasks not synced: " + err.Error()
		return model, nil
	}
	callID, err := canonical.NewOpaqueID("tool_")
	if err != nil {
		model.status = "allocate task tool-call id: " + err.Error()
		return model, nil
	}
	eventID, err := allocateEventID()
	if err != nil {
		model.status = "allocate task event id: " + err.Error()
		return model, nil
	}
	assignment := *model.active
	call := completion.ToolCall{ID: callID, Name: target.name, Input: target.buildInput(items)}
	event := completion.Event{ID: eventID, Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{call}}
	beforeContext := cloneAssignment(model.lastContext)
	model.appendLocalToolCall(assignment, call)
	model.expectContinuation(assignment, []completion.ToolCall{call})
	model.pending = pendingSend{
		kind: pendingTasks, eventID: eventID, assignment: assignment, tasks: append([]agentTask(nil), items...),
		context: beforeContext, selected: model.taskSelected,
		remainingDraft: model.remainingDraftAfterSend(pendingTasks),
	}
	model.active = nil
	model.focus = focusTasks
	model.taskDirty = false
	model.taskSyncWait = true
	model.detailMode = false
	model.status = "Tasks update queued for the client Agent · waiting for its next turn"
	return model.commitPendingEvent(event)
}

func (model *Model) appendLocalText(assignment completion.Assignment, role canonical.Role, text string) {
	model.ensureLocalContext(assignment)
	model.lastContext.Request.Messages = append(model.lastContext.Request.Messages, canonical.Message{
		Role: role, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: text}},
	})
	model.invalidateChat()
}

func (model *Model) appendLocalToolCall(assignment completion.Assignment, call completion.ToolCall) {
	model.ensureLocalContext(assignment)
	model.lastContext.Request.Messages = append(model.lastContext.Request.Messages, canonical.Message{
		Role: canonical.RoleAssistant,
		Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: call.ID,
			ToolNamespace: call.Namespace, ToolName: call.Name, Input: call.Input,
		}},
	})
	model.invalidateChat()
}

func (model Model) sendCommand() (tea.Model, tea.Cmd) {
	if model.blockSendWhileStateUnsettled("command draft", deferredSendIntent{kind: pendingCommand}) {
		return model, nil
	}
	if model.responseInFlightFor(model.active) {
		model.status = "another response event is still being committed; command draft retained"
		return model, nil
	}
	if strings.TrimSpace(model.commandInput) == "" {
		model.status = "type a command before sending"
		return model, nil
	}
	target, reason := model.commandTarget()
	if reason != "" {
		model.status = reason
		return model, nil
	}
	if pullPath, pull := workspacePullPath(model.commandInput); pull {
		return model.sendWorkspacePull(pullPath, target)
	}
	decision := safety.CheckCommand(model.commandInput, true)
	if decision.Severity == safety.SeverityWarn && model.commandConfirm != model.commandInput {
		model.commandConfirm = model.commandInput
		model.status = "command warning: " + strings.Join(decision.Reasons, "; ") + " · press Enter again to send"
		return model, nil
	}
	callID, err := canonical.NewOpaqueID("tool_")
	if err != nil {
		model.status = "allocate tool-call id: " + err.Error()
		return model, nil
	}
	eventID, err := allocateEventID()
	if err != nil {
		model.status = "allocate command event id: " + err.Error()
		return model, nil
	}
	input := map[string]any{target.commandField: model.commandInput}
	if target.cwdField != "" {
		input[target.cwdField] = target.cwdValue
	}
	if target.descriptionRequired {
		input["description"] = commandDescription(model.commandInput)
	}
	assignment := *model.active
	event := completion.Event{ID: eventID, Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{{
		ID: callID, Name: target.name, Input: input,
	}}}
	beforeContext := cloneAssignment(model.lastContext)
	model.appendLocalToolCall(assignment, event.ToolCalls[0])
	model.expectContinuation(assignment, event.ToolCalls)
	model.pending = pendingSend{
		kind: pendingCommand, eventID: eventID, assignment: assignment, command: model.commandInput,
		context: beforeContext, remainingDraft: model.remainingDraftAfterSend(pendingCommand),
	}
	model.setCommandValue("")
	model.commandConfirm = ""
	model.active = nil
	model.focus = focusTasks
	model.detailMode = false
	model.status = target.name + " tool call queued · waiting for the client Agent result"
	return model.commitPendingEvent(event)
}

func workspacePullPath(command string) (string, bool) {
	trimmed := strings.TrimSpace(command)
	if trimmed == ":pull" {
		return "", true
	}
	if !strings.HasPrefix(trimmed, ":pull ") {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(trimmed, ":pull ")), true
}

func (model Model) sendWorkspacePull(relativePath string, target commandTarget) (tea.Model, tea.Cmd) {
	if model.active == nil || strings.TrimSpace(relativePath) == "" {
		model.status = "workspace pull requires a relative file path: :pull path/to/file"
		return model, nil
	}
	assignment := *model.active
	if !mirrorEnabled(assignment) || assignment.Adapter == nil ||
		assignment.Adapter.Key() != adapter.OpenCodeID+"@"+adapter.OpenCodeVersion {
		model.status = "workspace pull requires the exact OpenCode workspace adapter"
		return model, nil
	}
	if !assignment.ExecAllowed {
		model.status = "workspace pull requires X-Human-Allow-Exec and client Agent permission"
		return model, nil
	}
	namespace := mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)
	workspace := model.mirrors[namespace]
	if workspace == nil {
		model.status = "workspace mirror is still preparing"
		return model, nil
	}
	call, err := workmirror.BuildHydrationToolCallForProfile(
		relativePath, assignment.Adapter, assignment.Root,
	)
	if err == nil && call.Name != target.name {
		err = fmt.Errorf("workspace pull generated undeclared command tool %q", call.Name)
	}
	if err == nil {
		err = validateMirrorCalls(assignment.Request, []completion.ToolCall{call})
	}
	var eventID string
	if err == nil {
		eventID, err = allocateEventID()
	}
	if err == nil {
		recorder, ok := workspace.(interface {
			RecordHydrationIntent(string, completion.ToolCall, *adapter.Profile, string) error
		})
		if !ok {
			err = errors.New("workspace mirror cannot persist pull intent")
		} else {
			err = recorder.RecordHydrationIntent(relativePath, call, assignment.Adapter, assignment.Root)
		}
	}
	if err != nil {
		model.status = "workspace pull not sent: " + err.Error()
		return model, nil
	}
	event := completion.Event{
		ID: eventID, Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{call},
	}
	beforeContext := cloneAssignment(model.lastContext)
	model.appendLocalToolCall(assignment, call)
	model.expectContinuation(assignment, event.ToolCalls)
	model.pending = pendingSend{
		kind: pendingCommand, eventID: eventID, assignment: assignment,
		command: model.commandInput, context: beforeContext, toolCalls: []completion.ToolCall{call},
		remainingDraft: model.remainingDraftAfterSend(pendingCommand),
	}
	model.setCommandValue("")
	model.commandConfirm = ""
	model.active = nil
	model.focus = focusTasks
	model.detailMode = false
	model.status = "exact workspace pull queued · waiting for the client Agent result"
	return model.commitPendingEvent(event)
}

func (model Model) sendDeclaredToolCalls() (tea.Model, tea.Cmd) {
	if model.active == nil || !model.composing {
		return model, nil
	}
	if model.blockSendWhileStateUnsettled("tool draft", deferredSendIntent{kind: pendingAdvancedTools}) {
		return model, nil
	}
	if model.responseInFlightFor(model.active) {
		model.status = "another response event is still being committed; tool draft retained"
		return model, nil
	}
	calls, err := parseDeclaredToolCalls(model.active.Request, model.input, model.toolCallIDs)
	if err != nil {
		model.status = "tool calls not sent: " + err.Error()
		return model, nil
	}
	for _, call := range calls {
		if mirrorMutationCall(*model.active, call) {
			// Mapped write/edit/delete/rename calls are correctness-bearing only
			// when generated from a reviewed scratch diff with durable delivery
			// intents. Letting the generic composer emit one would have no mirror
			// provenance and a later rejection could not be recovered truthfully.
			model.status = "tool calls not sent: mapped file mutations must use Live Workspace review and preview"
			return model, nil
		}
	}
	eventID, err := allocateEventID()
	if err != nil {
		model.status = "allocate tool event id: " + err.Error()
		return model, nil
	}
	assignment := *model.active
	event := completion.Event{ID: eventID, Type: completion.EventToolCalls, ToolCalls: calls}
	beforeContext := cloneAssignment(model.lastContext)
	for _, call := range calls {
		model.appendLocalToolCall(assignment, call)
	}
	model.expectContinuation(assignment, calls)
	model.pending = pendingSend{
		kind: pendingAdvancedTools, eventID: eventID, assignment: assignment,
		context: beforeContext, toolInput: model.input, toolCallIDs: append([]string(nil), model.toolCallIDs...),
		remainingDraft: model.remainingDraftAfterSend(pendingAdvancedTools),
	}
	model.input = ""
	model.toolCallIDs = nil
	model.composing = false
	model.active = nil
	model.focus = focusTasks
	model.detailMode = false
	model.status = fmt.Sprintf("%d declared tool call(s) queued · waiting for client results", len(calls))
	return model.commitPendingEvent(event)
}

func (model Model) appendComposeInput(value string) Model {
	additionalLines := strings.Count(value, "\n")
	ids := append([]string(nil), model.toolCallIDs...)
	for range additionalLines {
		callID, err := canonical.NewOpaqueID("tool_")
		if err != nil {
			model.status = "allocate tool-call id: " + err.Error()
			return model
		}
		ids = append(ids, callID)
	}
	model.toolCallIDs = ids
	model.input += value
	model.invalidateChat()
	return model
}

func (model Model) backspaceComposeInput() Model {
	if len(model.input) == 0 {
		return model
	}
	runes := []rune(model.input)
	removed := runes[len(runes)-1]
	model.input = string(runes[:len(runes)-1])
	if removed == '\n' && len(model.toolCallIDs) > 1 {
		model.toolCallIDs = model.toolCallIDs[:len(model.toolCallIDs)-1]
	}
	model.invalidateChat()
	return model
}

func parseDeclaredToolCalls(request canonical.Request, rawInput string, callIDs []string) ([]completion.ToolCall, error) {
	lines := strings.Split(rawInput, "\n")
	calls := make([]completion.ToolCall, 0, len(lines))
	for index, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if index >= len(callIDs) {
			return nil, fmt.Errorf("line %d: tool-call id is unavailable", index+1)
		}
		call, err := parseDeclaredToolCall(request, line, callIDs[index])
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", index+1, err)
		}
		calls = append(calls, call)
	}
	if len(calls) == 0 {
		return nil, errors.New("enter at least one <tool-name> <JSON object>")
	}
	if request.ToolCallPolicy == canonical.ToolCallsSerial && len(calls) > 1 {
		return nil, errors.New("this request allows one tool call per response")
	}
	return calls, nil
}

func parseDeclaredToolCall(request canonical.Request, rawInput, callID string) (completion.ToolCall, error) {
	input := strings.TrimSpace(rawInput)
	separator := strings.IndexFunc(input, unicode.IsSpace)
	if separator <= 0 {
		return completion.ToolCall{}, errors.New("use <tool-name> <JSON object>")
	}
	qualifiedName := input[:separator]
	payload := strings.TrimSpace(input[separator:])
	var declared *canonical.Tool
	for _, tool := range request.Tools {
		if tool.QualifiedName() == qualifiedName {
			copy := tool
			declared = &copy
			break
		}
	}
	if declared == nil {
		return completion.ToolCall{}, fmt.Errorf("caller did not declare tool %q", qualifiedName)
	}
	var arguments map[string]any
	if err := json.Unmarshal([]byte(payload), &arguments); err != nil {
		return completion.ToolCall{}, fmt.Errorf("arguments must be one JSON object: %w", err)
	}
	if arguments == nil {
		return completion.ToolCall{}, errors.New("arguments must be one JSON object")
	}
	if strings.TrimSpace(callID) == "" {
		return completion.ToolCall{}, errors.New("tool-call id is unavailable")
	}
	return completion.ToolCall{
		ID: callID, Namespace: declared.Namespace, Name: declared.Name, Input: arguments,
	}, nil
}

func (model Model) sendReply(eventType completion.EventType, endResponse bool) (tea.Model, tea.Cmd) {
	return model.sendReplyWithOptions(eventType, endResponse, false)
}

func (model Model) finishConversation() (tea.Model, tea.Cmd) {
	return model.sendReplyWithOptions(completion.EventFinal, true, true)
}

func (model Model) sendReplyWithOptions(eventType completion.EventType, endResponse, allowEmpty bool) (tea.Model, tea.Cmd) {
	model.syncUI()
	if model.active == nil {
		return model, nil
	}
	if model.blockSendWhileStateUnsettled("reply draft", deferredSendIntent{
		kind: pendingReply, replyType: eventType, endResponse: endResponse, allowEmpty: allowEmpty,
	}) {
		return model, nil
	}
	if model.responseInFlightFor(model.active) {
		model.status = "previous response event is still being committed; draft retained"
		return model, nil
	}
	text := model.replyInput
	if strings.TrimSpace(text) == "" && !allowEmpty {
		model.status = "type a reply before sending"
		return model, nil
	}
	assignment := *model.active
	eventID, err := allocateEventID()
	if err != nil {
		model.status = "allocate response event id: " + err.Error()
		return model, nil
	}
	wireText := text
	if eventType == completion.EventProgress {
		// Multiple progress events are adjacent deltas in every supported wire
		// protocol. Preserve the chat-message boundary explicitly.
		wireText += "\n\n"
	}
	event := completion.Event{ID: eventID, Type: eventType, Text: wireText}
	model.pending = pendingSend{
		kind: pendingReply, eventID: eventID, assignment: assignment,
		context: cloneAssignment(model.lastContext), reply: text,
		remainingDraft: model.remainingDraftAfterSend(pendingReply),
	}
	if text != "" {
		model.appendLocalText(assignment, canonical.RoleAssistant, text)
	}
	model.setReplyValue("")
	model.draftReplyRejected = ""
	model.draftRejectedEvents = nil
	model.draftRejectedKinds = nil
	if endResponse {
		if eventType == completion.EventClarification {
			model.expectHandoff(assignment)
		} else {
			model.clearContinuation()
		}
		model.active = nil
		model.focus = focusTasks
		model.detailMode = false
		if eventType == completion.EventClarification {
			model.status = "turn handed to the client Agent · waiting here for its reply"
		} else {
			model.status = "conversation ending · response queued locally"
		}
	} else {
		model.focus = focusReply
		model.status = "message queued locally · stream stays open"
	}
	return model.commitPendingEvent(event)
}

func (model *Model) blockSendWhileStateUnsettled(draft string, intent deferredSendIntent) bool {
	if model.stateStore == nil {
		return false
	}
	switch {
	case !model.stateBound:
		model.status = "recovery state is waiting for authenticated worker identity; " + draft +
			" retained · sending remains disabled until the connection is ready"
		return true
	case model.stateWriting || model.stateRetryPending:
		if model.active == nil {
			model.status = "request is no longer active; " + draft + " retained"
			return true
		}
		if model.deferredSend.kind == pendingNone {
			intent.sessionKey = model.active.SessionKey()
			intent.draftKey = stateRecordKey{scope: stateScope(*model.active), kind: workerStateDraftKind}
			intent.draft, _ = model.currentPersistedDraft()
			model.deferredSend = intent
		}
		model.status = "send queued behind recovery-state commit; " + draft +
			" retained and input locked · Esc cancels"
		return true
	default:
		return false
	}
}

func (model Model) executeDeferredSend() (Model, tea.Cmd) {
	intent := model.deferredSend
	model.deferredSend = deferredSendIntent{}
	if intent.kind == pendingNone {
		return model, nil
	}
	if model.active == nil || model.active.SessionKey() != intent.sessionKey {
		model.retainDeferredDraft(intent)
		model.status = "queued send canceled because its request is no longer active · draft retained"
		return model, nil
	}

	var updated tea.Model
	var command tea.Cmd
	switch intent.kind {
	case pendingReply:
		updated, command = model.sendReplyWithOptions(intent.replyType, intent.endResponse, intent.allowEmpty)
	case pendingCommand:
		updated, command = model.sendCommand()
	case pendingTasks:
		updated, command = model.sendAgentTasks()
	case pendingAdvancedTools:
		updated, command = model.sendDeclaredToolCalls()
	default:
		model.status = "queued send canceled because its action is invalid · draft retained"
		return model, nil
	}
	return updated.(Model), command
}

// cancelDeferredSendIfInactive runs before nextStateCommand computes deletes.
// A network lifecycle transition may have replaced the active editor while a
// send gesture was waiting; pin the captured source draft to its original scope
// so that canceling the gesture cannot turn a previously committed delete into
// silent draft loss.
func (model *Model) cancelDeferredSendIfInactive() {
	if model.deferredSend.kind == pendingNone ||
		(model.active != nil && model.active.SessionKey() == model.deferredSend.sessionKey) {
		return
	}
	intent := model.deferredSend
	model.deferredSend = deferredSendIntent{}
	model.retainDeferredDraft(intent)
	model.status = "queued send canceled because its request is no longer active · draft retained"
}

func (model *Model) retainDeferredDraft(intent deferredSendIntent) {
	if intent.draftKey.kind != workerStateDraftKind || !persistedDraftHasContent(intent.draft) {
		return
	}
	model.stateDrafts[intent.draftKey] = savedStateDraft{
		draft: sanitizePersistedDraft(intent.draft), updatedAt: time.Now().UTC(),
	}
}

func (model Model) View() tea.View {
	model.syncUI()
	model.resizeUI()
	layout := modernWorkspaceLayout(model.width, model.height, model.focus, model.active != nil)
	view := tea.NewView(model.renderWorkspace())
	view.AltScreen = true
	view.ReportFocus = true
	// Mouse tracking prevents the terminal from handling ordinary drag
	// selection. Keyboard paging is sufficient here, so copying stays native.
	view.MouseMode = tea.MouseModeNone
	// Bubble Tea v2 always requests the basic Kitty key disambiguation that makes
	// Shift+Enter distinguishable. Alternate-key reporting adds safe key identity
	// metadata without turning ordinary text/IME input into escape-coded events.
	view.KeyboardEnhancements.ReportAlternateKeys = true
	view.WindowTitle = "Human Agent"
	if layout.width >= 50 && layout.height >= 16 {
		view.Cursor = model.workspaceCursor(layout)
	}
	return view
}

func (model Model) renderWorkspace() string {
	return model.renderModernWorkspace()
}

func (model Model) contextAssignment() *completion.Assignment {
	if model.active != nil {
		if model.lastContext != nil && model.lastContext.SessionKey() == model.active.SessionKey() {
			return model.lastContext
		}
		return model.active
	}
	if (len(model.continueIDs) > 0 || model.continueHandoff || model.delivery.stage == deliverySending) &&
		model.lastContext != nil {
		return model.lastContext
	}
	if len(model.assignments) > 0 {
		selected := model.selected
		if selected < 0 {
			selected = 0
		}
		if selected >= len(model.assignments) {
			selected = len(model.assignments) - 1
		}
		return &model.assignments[selected]
	}
	return model.lastContext
}

func (model Model) renderAgentTaskRows(width, budget int) []string {
	items := model.agentTasks
	reason := ""
	if model.active == nil && len(model.assignments) > 0 {
		assignment := model.assignments[model.selected]
		target, targetReason := taskTargetForRequest(assignment.Request)
		reason = targetReason
		if targetReason == "" {
			observed, found, err := tasksFromRequest(assignment.Request, target)
			switch {
			case err != nil:
				reason = "Task history unavailable: " + err.Error()
			case found:
				items = observed
			default:
				items = nil
				reason = "No task list observed yet"
			}
		}
	} else if model.active != nil {
		_, reason = taskTargetForRequest(model.active.Request)
		if model.taskConflict {
			reason = "Task result conflict · editing and sync are disabled"
		} else if model.taskSyncWait {
			reason = "Task update is awaiting a client result"
		}
	}
	if budget <= 1 && model.active != nil && reason == "" && !model.taskEditing {
		return renderAgentTaskList(items, model.taskSelected, 1, width, true)
	}

	footer := ""
	if model.active == nil && len(model.assignments) > 0 {
		footer = fmt.Sprintf("INBOX %d/%d · a accept · r reject · %s", model.selected+1, len(model.assignments), requestPreview(model.assignments[model.selected].Request))
	} else if model.taskSyncWait {
		footer = "Waiting for the client Agent to execute the task update…"
	} else if model.active != nil && reason == "" {
		footer = "Local edits are batched; Ctrl+S sends one task-tool call"
	}

	listBudget := budget
	if model.taskEditing {
		listBudget--
	} else {
		if reason != "" {
			listBudget--
		}
		if footer != "" {
			listBudget--
		}
	}
	if listBudget < 0 {
		listBudget = 0
	}
	var rows []string
	if reason == "" {
		rows = renderAgentTaskList(items, model.taskSelected, listBudget, width, model.active != nil)
	}
	if model.taskEditing {
		label := "new"
		if model.taskEditIndex >= 0 {
			label = "edit"
		}
		rows = append(rows, oneDisplayLine(fmt.Sprintf("%s> %s█", label, model.taskInput), width))
	} else if reason != "" {
		rows = append(rows, oneDisplayLine(reason, width))
	}
	if !model.taskEditing && footer != "" {
		rows = append(rows, oneDisplayLine(footer, width))
	}
	return rows
}

func renderAgentTaskList(items []agentTask, selected, budget, width int, editable bool) []string {
	if budget <= 0 {
		return nil
	}
	if len(items) == 0 {
		message := "No task list observed"
		if editable {
			message = "No task list observed yet · press n to create one"
		}
		return []string{oneDisplayLine(message, width)}
	}
	if selected < 0 {
		selected = 0
	}
	if selected >= len(items) {
		selected = len(items) - 1
	}
	start := selected - budget + 1
	if start < 0 {
		start = 0
	}
	end := start + budget
	if end > len(items) {
		end = len(items)
		start = end - budget
		if start < 0 {
			start = 0
		}
	}
	result := make([]string, 0, budget)
	for index := start; index < end; index++ {
		cursor := "  "
		if editable && index == selected {
			cursor = "> "
		}
		item := items[index]
		marker := map[agentTaskStatus]string{
			taskPending: "□", taskInProgress: "◐", taskCompleted: "✓", taskCancelled: "×",
		}[item.Status]
		priority := ""
		switch normalizePriority(item.Priority) {
		case "high":
			priority = "! "
		case "low":
			priority = "↓ "
		}
		result = append(result, oneDisplayLine(fmt.Sprintf("%s%s %s%s", cursor, marker, priority, item.Content), width))
	}
	return result
}

func (model Model) contextSections(assignment completion.Assignment) []string {
	var reference strings.Builder
	if len(assignment.Request.Tools) > 0 {
		reference.WriteString(renderDeclaredToolDefinitions(assignment.Request.Tools))
	}
	if model.composing {
		if tool := selectedDeclaredTool(assignment.Request.Tools, currentToolCallLine(model.input)); tool != nil {
			if reference.Len() > 0 {
				reference.WriteByte('\n')
			}
			reference.WriteString("Selected tool schema (full, paged): ")
			reference.WriteString(tool.QualifiedName())
			reference.WriteByte('\n')
			reference.Write(tool.InputSchema)
		}
	}
	var primary strings.Builder
	if hosted := renderHostedCapabilities(assignment.Request.HostedCapabilities); hosted != "" {
		primary.WriteString(hosted)
		primary.WriteByte('\n')
	}
	if len(assignment.Request.Tools) > 0 {
		primary.WriteString(renderDeclaredTools(assignment.Request.Tools))
		primary.WriteByte('\n')
	}
	primary.WriteString("Request (full, paged):\n")
	primary.WriteString(strings.TrimSpace(renderRequest(assignment.Request)))
	if review := strings.TrimSpace(renderDeliveryReview(model.delivery)); review != "" {
		primary.WriteString("\n\n")
		primary.WriteString(review)
	}
	sections := make([]string, 0, 3)
	if reference.Len() > 0 {
		sections = append(sections, reference.String())
	}
	sections = append(sections, primary.String())
	if directory := model.mirrorDirectory(assignment); directory != "" {
		sections = append(sections, "Human workspace (absolute path; edit this copy):\n"+directory)
	}
	return sections
}

func (model Model) mirrorDirectory(assignment completion.Assignment) string {
	if !mirrorEnabled(assignment) {
		return ""
	}
	workspace := model.mirrors[mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)]
	if workspace == nil || workspace.Dir() == "" {
		return ""
	}
	directory, err := filepath.Abs(workspace.Dir())
	if err != nil {
		return workspace.Dir()
	}
	return directory
}

func wrapDisplayLines(value string, width int) []string {
	if width <= 0 {
		width = 1
	}
	value = terminalSafe(value)
	logical := strings.Split(value, "\n")
	lines := make([]string, 0, len(logical))
	for _, line := range logical {
		if line == "" {
			lines = append(lines, "")
			continue
		}
		lines = append(lines, strings.Split(ansi.Hardwrap(line, width, true), "\n")...)
	}
	return lines
}

func oneDisplayLine(value string, width int) string {
	value = terminalSafe(value)
	value = strings.Join(strings.Fields(value), " ")
	if width <= 0 {
		return ""
	}
	return ansi.Truncate(value, width, "…")
}

// terminalSafe turns terminal control input into inert, visible text before it
// reaches either width calculation or Bubble Tea's renderer. Newlines remain
// structural and tabs become readable indentation; no other control byte is
// passed through.
func terminalSafe(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for _, character := range value {
		switch character {
		case '\n':
			builder.WriteByte('\n')
		case '\t':
			builder.WriteString("    ")
		case '\u007f':
			builder.WriteRune('␡')
		default:
			switch {
			case character >= 0 && character <= 0x1f:
				builder.WriteRune(rune(0x2400) + character)
			case unicode.IsControl(character) || isBidiControl(character):
				fmt.Fprintf(&builder, "⟦U+%04X⟧", character)
			default:
				builder.WriteRune(character)
			}
		}
	}
	return builder.String()
}

func isBidiControl(character rune) bool {
	switch character {
	case '\u061c', '\u200e', '\u200f',
		'\u202a', '\u202b', '\u202c', '\u202d', '\u202e',
		'\u2066', '\u2067', '\u2068', '\u2069':
		return true
	default:
		return false
	}
}

func boundedTailLines(lines []string, limit int, marker string) []string {
	if len(lines) <= limit {
		return lines
	}
	return append([]string{marker}, lines[len(lines)-limit+1:]...)
}

func renderDeliveryReview(review deliveryReview) string {
	if review.stage == deliveryNone {
		return ""
	}
	var builder strings.Builder
	builder.WriteString("\nDelivery review (not sent):\n")
	for _, warning := range review.warnings {
		fmt.Fprintf(&builder, "  ! %s\n", warning)
	}
	if len(review.changes) == 0 {
		builder.WriteString("  no changes\n")
		return builder.String()
	}
	for _, change := range review.changes {
		marker := map[workmirror.ChangeKind]string{
			workmirror.ChangeWrite: "+", workmirror.ChangeEdit: "~", workmirror.ChangeDelete: "-",
		}[change.Kind]
		fmt.Fprintf(&builder, "  %s %s", marker, change.Path)
		if len(change.Reasons) > 0 {
			fmt.Fprintf(&builder, " [%s]", strings.Join(change.Reasons, "; "))
		}
		builder.WriteByte('\n')
		if review.stage == deliveryPreviewed || review.stage == deliveryConfirming ||
			review.stage == deliveryConfirmed || review.stage == deliverySending {
			builder.WriteString(renderChangePreview(change))
		}
	}
	if review.stage == deliveryReviewed {
		builder.WriteString("  [ctrl+p] build exact tool-call preview\n")
	} else if review.stage == deliveryPreviewed {
		builder.WriteString("  [enter] confirm and send exactly this preview  [esc] cancel\n")
	} else if review.stage == deliveryConfirming {
		builder.WriteString("  recording exact delivery intent…\n")
	} else if review.stage == deliveryConfirmed {
		builder.WriteString("  [enter] retry this exact confirmed event\n")
	} else {
		builder.WriteString("  sending confirmed tool calls…\n")
	}
	return builder.String()
}

func renderDeclaredTools(tools []canonical.Tool) string {
	if len(tools) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.WriteString("Declared tools: descriptions shortened; select a tool to inspect schema\n")
	for _, tool := range tools {
		fmt.Fprintf(&builder, "  %s", tool.QualifiedName())
		if description := boundedSingleLine(tool.Description, toolDescriptionPreviewRunes); description != "" {
			fmt.Fprintf(&builder, " — %s", description)
		}
		builder.WriteByte('\n')
	}
	return builder.String()
}

func renderHostedCapabilities(capabilities []canonical.HostedCapability) string {
	if len(capabilities) == 0 {
		return ""
	}
	types := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		types = append(types, capability.Type)
	}
	return "Provider-hosted capabilities (client/provider executes; Human cannot call): " + strings.Join(types, ", ")
}

func renderDeclaredToolDefinitions(tools []canonical.Tool) string {
	if len(tools) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.WriteString("Declared tools (full descriptions; select a tool to inspect schema):\n")
	for _, tool := range tools {
		fmt.Fprintf(&builder, "  %s\n", tool.QualifiedName())
		if description := strings.TrimSpace(tool.Description); description != "" {
			fmt.Fprintf(&builder, "    description: %s\n", description)
		} else {
			builder.WriteString("    description: (none)\n")
		}
	}
	return builder.String()
}

func renderToolComposeContext(tools []canonical.Tool, input string) string {
	if tool := selectedDeclaredTool(tools, input); tool != nil {
		return fmt.Sprintf("Input schema for %s:\n%s\n", tool.QualifiedName(), schemaPreview(tool.InputSchema, toolSchemaPreviewRunes))
	}
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.QualifiedName())
	}
	return "Available tools: " + strings.Join(names, ", ") + "\n"
}

func selectedDeclaredTool(tools []canonical.Tool, input string) *canonical.Tool {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return nil
	}
	for index := range tools {
		if fields[0] == tools[index].QualifiedName() {
			return &tools[index]
		}
	}
	return nil
}

func currentToolCallLine(input string) string {
	if separator := strings.LastIndexByte(input, '\n'); separator >= 0 {
		return input[separator+1:]
	}
	return input
}

func boundedSingleLine(value string, limit int) string {
	value = terminalSafe(value)
	value = strings.Join(strings.Fields(value), " ")
	if ansi.StringWidth(value) <= limit {
		return value
	}
	return ansi.Truncate(value, limit, "…")
}

func schemaPreview(schema []byte, limit int) string {
	value := strings.TrimSpace(terminalSafe(string(schema)))
	if value == "" {
		return "(empty)"
	}
	if ansi.StringWidth(value) <= limit {
		return value
	}
	return fmt.Sprintf("%s\n… schema preview truncated; exact schema is %d bytes", ansi.Truncate(value, limit, ""), len(value))
}

func renderChangePreview(change workmirror.Change) string {
	const contentLimit = 2048
	var builder strings.Builder
	fmt.Fprintf(&builder, "    expected caller hash: %s\n", change.ExpectedSHA)
	switch change.Kind {
	case workmirror.ChangeWrite:
		fmt.Fprintf(&builder, "    new content:\n%s\n", indentPreview(change.NewContent, contentLimit))
	case workmirror.ChangeEdit:
		fmt.Fprintf(&builder, "    before:\n%s\n", indentPreview(change.OldContent, contentLimit))
		fmt.Fprintf(&builder, "    after:\n%s\n", indentPreview(change.NewContent, contentLimit))
	case workmirror.ChangeDelete:
		builder.WriteString("    delete caller file if its hash still matches\n")
	}
	return builder.String()
}

func indentPreview(content []byte, limit int) string {
	if len(content) == 0 {
		return "      (empty)"
	}
	if !isText(content) {
		return fmt.Sprintf("      (binary, %d bytes)", len(content))
	}
	total := len(content)
	truncated := len(content) > limit
	if truncated {
		content = content[:limit]
	}
	text := strings.ReplaceAll(string(content), "\n", "\n      ")
	text = "      " + text
	if truncated {
		text += fmt.Sprintf("\n      … preview truncated; exact payload is %d bytes", total)
	}
	return text
}

func isText(content []byte) bool {
	if !utf8.Valid(content) {
		return false
	}
	for _, value := range content {
		if value == 0 {
			return false
		}
	}
	return true
}

func waitForNetwork(client Client) tea.Cmd {
	return func() tea.Msg {
		message, open := <-client.Messages()
		if !open {
			return networkClosed{}
		}
		return networkMessage(message)
	}
}

func sendEvent(client Client, assignment completion.Assignment, event completion.Event) tea.Cmd {
	if event.ID == "" {
		id, err := canonical.NewOpaqueID("event_")
		if err != nil {
			return func() tea.Msg { return eventSent{err: err} }
		}
		event.ID = id
	}
	return func() tea.Msg {
		return eventSent{eventID: event.ID, err: client.SendEvent(context.Background(), assignment, event)}
	}
}

func (model Model) commitPendingEvent(event completion.Event) (tea.Model, tea.Cmd) {
	model.pending.event = event
	model.pending.eventID = event.ID
	model.pending.draftOrigins = sortedDraftOriginKeys(model.draftOrigins)
	if model.stateStore == nil {
		if model.pending.kind == pendingDelivery {
			command := model.resumePendingDelivery(model.pending)
			return model, command
		}
		return model, sendEvent(model.client, model.pending.assignment, event)
	}
	// nextStateCommand serializes this intent through the worker-state store.
	// applyStateWriteResult starts the outbox write only after that transaction
	// commits, so clearing the editor can never outrun both durable copies.
	return model, nil
}

func sendPersistedEvent(client Client, store StateStore, pending pendingSend) tea.Cmd {
	return func() tea.Msg {
		err := client.SendEvent(context.Background(), pending.assignment, pending.event)
		message := eventSent{eventID: pending.event.ID, err: err}
		if err == nil || errors.Is(err, workerclient.ErrEventRejectionPending) ||
			errors.Is(err, workerclient.ErrEventPreviouslyRejected) {
			return message
		}
		if pending.kind == pendingDelivery {
			// A reviewed file event has no ordinary editor draft to restore. Keep
			// its exact durable journal entry and retry the local outbox handoff;
			// changing IDs or calls would orphan the recorded mirror provenance.
			if store != nil {
				message.intentErr = err
			}
			return message
		}
		if !errors.Is(err, workerclient.ErrEventNotStored) {
			// Storage/transaction errors are commit-ambiguous. The exact ID may
			// already be durable even though Put returned an error, so only an
			// idempotent outbox retry may resolve this outcome.
			return message
		}
		if store == nil {
			return message
		}
		return persistPendingRestore(store, pending, err)()
	}
}

func persistPendingRestore(store StateStore, pending pendingSend, sendErr error) tea.Cmd {
	return func() tea.Msg {
		message := eventSent{
			eventID: pending.event.ID, err: sendErr, restorePending: true,
		}
		// Only a deterministic pre-outbox failure may return the source draft.
		// Publish that decision into the already-durable intent row before Bubble
		// Tea clears pending state. If Put commits but reports an error, subsequent
		// retries repeat this state write and never cross back into SendEvent.
		key := pendingSendStateKey(pending)
		persisted := persistedPendingFromSend(pending, pendingSendDispositionRestore)
		payload, marshalErr := json.Marshal(persisted)
		if marshalErr != nil {
			message.intentErr = marshalErr
			return message
		}
		ctx, cancel := context.WithTimeout(context.Background(), workerStateWriteTimeout)
		defer cancel()
		if _, stateErr := store.Put(ctx, key.scope, key.kind, payload); stateErr != nil {
			message.intentErr = stateErr
			return message
		}
		message.intentKey = key
		message.restorePayload = payload
		return message
	}
}

func confirmRejectedEvent(client Client, eventID string) tea.Cmd {
	return confirmRejectedEventAttempt(client, eventID, 0)
}

func (model *Model) beginRejectionFinalization(
	eventID string,
	scope string,
	pendingKey stateRecordKey,
	waitsForPendingDelete bool,
	mirrorConfirmationPending bool,
	cleanup tea.Cmd,
	resumeRecoveries bool,
) tea.Cmd {
	if model.rejectionFinalizers == nil {
		model.rejectionFinalizers = make(map[string]rejectionFinalizer)
	}
	model.rejectionFinalizers[eventID] = rejectionFinalizer{
		scope: scope, pendingKey: pendingKey, waitsForPendingDelete: waitsForPendingDelete,
		pendingDeleted:            !waitsForPendingDelete,
		waitsForStateSync:         model.stateStore != nil && model.stateBound,
		mirrorConfirmationPending: mirrorConfirmationPending,
		cleanup:                   cleanup, cleanupDone: cleanup == nil,
		resumeRecoveries: resumeRecoveries,
	}
	return model.advanceRejectionFinalization(eventID)
}

// rejectedPendingJournal finds the exact save-ahead row after eventSent has
// cleared its in-memory pendingSend but before the asynchronous Delete result is
// reduced. Matching both event ID and session prevents a late rejection from
// waiting on (or deleting) a newer event in the same stable task scope.
func (model Model) rejectedPendingJournal(eventID, sessionKey string) (stateRecordKey, bool) {
	if eventID == "" || sessionKey == "" {
		return stateRecordKey{}, false
	}
	for key, payload := range model.stateSynced {
		if key.kind != workerStatePendingSendKind {
			continue
		}
		var persisted persistedPendingSend
		if err := json.Unmarshal([]byte(payload), &persisted); err != nil ||
			validatePersistedPendingSend(key.scope, persisted) != nil {
			continue
		}
		if persisted.Event.ID == eventID && persisted.Assignment.SessionKey() == sessionKey {
			return key, true
		}
	}
	return stateRecordKey{}, false
}

func (model *Model) advanceRejectionFinalization(eventID string) tea.Cmd {
	finalizer, exists := model.rejectionFinalizers[eventID]
	if !exists || finalizer.mirrorConfirmationPending {
		return nil
	}
	if !finalizer.cleanupDone {
		if finalizer.cleanupInFlight {
			return nil
		}
		if finalizer.cleanup == nil {
			finalizer.cleanupDone = true
		} else {
			finalizer.cleanupInFlight = true
			model.rejectionFinalizers[eventID] = finalizer
			return discardRejectedIntentsAttempt(eventID, finalizer.cleanup, 0)
		}
	}
	if !finalizer.pendingDeleted ||
		(finalizer.waitsForStateSync && !model.workerStateSynchronized()) ||
		finalizer.confirming {
		model.rejectionFinalizers[eventID] = finalizer
		return nil
	}
	finalizer.confirming = true
	model.rejectionFinalizers[eventID] = finalizer
	return confirmRejectedEvent(model.client, eventID)
}

func (model *Model) settleRejectedMirrorConfirmation(eventID string) tea.Cmd {
	finalizer, exists := model.rejectionFinalizers[eventID]
	if !exists || !finalizer.mirrorConfirmationPending {
		return nil
	}
	finalizer.mirrorConfirmationPending = false
	// Cleanup deliberately starts only after the old RecordDeliveryIntents
	// command has returned. It therefore removes both pre-existing intents and
	// anything that command may just have recreated.
	finalizer.cleanupDone = finalizer.cleanup == nil
	finalizer.cleanupInFlight = false
	model.rejectionFinalizers[eventID] = finalizer
	return model.advanceRejectionFinalization(eventID)
}

func (model *Model) completeRejectedIntentCleanup(eventID string) tea.Cmd {
	finalizer, exists := model.rejectionFinalizers[eventID]
	if !exists {
		return confirmRejectedEvent(model.client, eventID)
	}
	finalizer.cleanupDone = true
	finalizer.cleanupInFlight = false
	model.rejectionFinalizers[eventID] = finalizer
	return model.advanceRejectionFinalization(eventID)
}

func (model *Model) completeRejectedPendingDelete(key stateRecordKey) tea.Cmd {
	commands := make([]tea.Cmd, 0, 1)
	for eventID, finalizer := range model.rejectionFinalizers {
		if !finalizer.waitsForPendingDelete || finalizer.pendingKey != key {
			continue
		}
		finalizer.pendingDeleted = true
		model.rejectionFinalizers[eventID] = finalizer
		if command := model.advanceRejectionFinalization(eventID); command != nil {
			commands = append(commands, command)
		}
	}
	if len(commands) == 0 {
		return nil
	}
	return tea.Batch(commands...)
}

func (model *Model) advanceRejectionFinalizations() tea.Cmd {
	commands := make([]tea.Cmd, 0, len(model.rejectionFinalizers))
	for eventID := range model.rejectionFinalizers {
		if command := model.advanceRejectionFinalization(eventID); command != nil {
			commands = append(commands, command)
		}
	}
	if len(commands) == 0 {
		return nil
	}
	return tea.Batch(commands...)
}

func (model *Model) finishRejectionFinalization(eventID string) tea.Cmd {
	finalizer, exists := model.rejectionFinalizers[eventID]
	if !exists {
		return nil
	}
	delete(model.rejectionFinalizers, eventID)
	if finalizer.resumeRecoveries {
		if model.pending.kind != pendingNone || model.active != nil {
			// Another operator action may have entered the local outbox while mirror
			// cleanup or rejected-inbox confirmation was retrying. Keep the drain
			// obligation until that action reaches a stable boundary.
			model.recoveryDrainPaused = true
			return nil
		}
		model.recoveryDrainPaused = false
		return model.resumePendingRecovery()
	}
	return nil
}

// finalizePreviouslyRejectedPending repairs the legacy crash window where a
// rejected-inbox tombstone could commit before the worker-state pending row was
// deleted. SendEvent then correctly refuses to resurrect the event. Treat that
// refusal as the same terminal rejection barrier: preserve any operator draft,
// remove mirror provenance, delete the stale journal, and only then perform the
// idempotent inbox confirmation before draining later recovered events.
func (model *Model) finalizePreviouslyRejectedPending() tea.Cmd {
	pending := model.pending
	if pending.kind == pendingNone || pending.event.ID == "" {
		return nil
	}
	eventID := pending.event.ID
	sessionKey := pending.assignment.SessionKey()
	pendingKey := pendingSendStateKey(pending)
	_, stateSynced := model.stateSynced[pendingKey]
	_, stateManaged := model.stateManaged[pendingKey]
	waitsForDelete := model.stateStore != nil && model.stateBound &&
		(stateSynced || stateManaged)

	var intentCleanup tea.Cmd
	if len(pending.event.ToolCalls) > 0 {
		intentCleanup = model.discardIntentCommand(
			pending.assignment, pending.event.ToolCalls, "previously rejected durable event",
		)
	}
	if pending.kind == pendingDelivery && intentCleanup == nil {
		intentCleanup = unavailableIntentCleanup(
			"durable workspace delivery intent cleanup is unavailable",
		)
	}

	if pending.context != nil {
		model.lastContext = cloneAssignment(pending.context)
		model.invalidateChat()
	}
	var remainder persistedDraft
	remainderPresent := false
	if model.active != nil && model.active.SessionKey() == sessionKey {
		remainder = model.remainingDraftAfterSend(pending.kind)
		remainderPresent = persistedDraftHasContent(remainder)
		delete(model.stateDrafts, stateRecordKey{
			scope: stateScope(*model.active), kind: workerStateDraftKind,
		})
	} else if persistedDraftHasContent(pending.remainingDraft) {
		remainder = pending.remainingDraft
		remainderPresent = true
	}
	if model.active != nil && model.active.SessionKey() == sessionKey {
		model.active = nil
		model.focus = focusTasks
		model.commandConfirm = ""
		model.composing = false
		model.input = ""
		model.toolCallIDs = nil
		model.detailMode = false
	}
	if model.delivery.sessionKey == sessionKey {
		model.delivery = deliveryReview{}
	}
	if model.hasContinuationOrigin(sessionKey) {
		model.removeContinuationOrigin(sessionKey)
	}
	if model.taskSyncWait && (pending.kind == pendingTasks || model.active == nil) {
		model.taskSyncWait = false
		model.taskDirty = pending.kind == pendingTasks
	}

	model.pending = pendingSend{}
	draftEvicted := false
	draft, hasDraft := model.rejectedDraftFromPending(pending)
	if remainderPresent {
		if editor, ok := rejectedDraftFromPersisted(pending.assignment, remainder); ok {
			if hasDraft {
				draft = mergeRejectedDraft(&draft, editor)
			} else {
				draft = editor
				hasDraft = true
			}
		}
	}
	if hasDraft {
		draftEvicted = model.installRejectedDraft(draft)
	}
	model.removeQueuedSession(sessionKey)
	delete(model.recoveredSessions, sessionKey)
	model.rememberHandledRejection(eventID)
	if pending.kind == pendingDelivery {
		model.status = "workspace delivery was already rejected; stale recovery journal is being removed"
	} else {
		model.status = "response was already rejected; draft restored and stale recovery journal is being removed"
		if draftEvicted {
			model.status += " · oldest saved draft evicted at the 32-scope limit"
		}
	}
	return model.beginRejectionFinalization(
		eventID, rejectedDraftScopeKey(pending.assignment), pendingKey, waitsForDelete, false, intentCleanup,
		pending.recovered || model.recoveryDrainPaused,
	)
}

// finalizeRejectedEvent preserves the rejected inbox as the durable recovery
// source until every local delivery/hydration intent has been removed. A crash
// after intent cleanup but before confirmation simply replays the inbox row and
// repeats the idempotent cleanup on the next TUI process.
func finalizeRejectedEvent(client Client, eventID string, intentCleanup tea.Cmd) tea.Cmd {
	if intentCleanup == nil {
		return confirmRejectedEvent(client, eventID)
	}
	return discardRejectedIntentsAttempt(eventID, intentCleanup, 0)
}

func discardRejectedIntentsAttempt(eventID string, cleanup tea.Cmd, attempt int) tea.Cmd {
	run := func() tea.Msg {
		if cleanup == nil {
			return mirrorIntentsDiscarded{
				eventID: eventID, attempt: attempt,
				err: errors.New("workspace intent cleanup is unavailable"),
			}
		}
		message := cleanup()
		result, ok := message.(mirrorIntentsDiscarded)
		if !ok {
			return mirrorIntentsDiscarded{
				eventID: eventID, attempt: attempt, retry: cleanup,
				err: fmt.Errorf("workspace intent cleanup returned %T", message),
			}
		}
		result.eventID = eventID
		result.attempt = attempt
		result.retry = cleanup
		return result
	}
	if attempt == 0 {
		return run
	}
	return tea.Tick(rejectionRetryDelay(attempt), func(time.Time) tea.Msg { return run() })
}

func discardPendingDeliveryIntentsAttempt(
	eventID string,
	namespace string,
	reason string,
	cleanup tea.Cmd,
	attempt int,
) tea.Cmd {
	run := func() tea.Msg {
		if cleanup == nil {
			return pendingDeliveryIntentsDiscarded{
				eventID: eventID, namespace: namespace, reason: reason, attempt: attempt,
				err: errors.New("workspace intent cleanup is unavailable"),
			}
		}
		message := cleanup()
		result, ok := message.(mirrorIntentsDiscarded)
		if !ok {
			return pendingDeliveryIntentsDiscarded{
				eventID: eventID, namespace: namespace, reason: reason, attempt: attempt,
				retry: cleanup, err: fmt.Errorf("workspace intent cleanup returned %T", message),
			}
		}
		return pendingDeliveryIntentsDiscarded{
			eventID: eventID, namespace: namespace, reason: reason, attempt: attempt,
			retry: cleanup, err: result.err,
		}
	}
	if attempt == 0 {
		return run
	}
	return tea.Tick(rejectionRetryDelay(attempt), func(time.Time) tea.Msg { return run() })
}

func confirmRejectedEventAttempt(client Client, eventID string, attempt int) tea.Cmd {
	confirm := func() tea.Msg {
		return rejectedEventConfirmed{
			eventID: eventID,
			attempt: attempt,
			err:     client.ConfirmRejectedEvent(context.Background(), eventID),
		}
	}
	if attempt == 0 {
		return confirm
	}
	return tea.Tick(rejectionRetryDelay(attempt), func(time.Time) tea.Msg { return confirm() })
}

func rejectionRetryDelay(attempt int) time.Duration {
	delay := rejectionConfirmMinBackoff
	for index := 1; index < attempt && delay < rejectionConfirmMaxBackoff; index++ {
		delay *= 2
		if delay > rejectionConfirmMaxBackoff {
			delay = rejectionConfirmMaxBackoff
		}
	}
	return delay
}

func requestPreview(request canonical.Request) string {
	for index := len(request.Messages) - 1; index >= 0; index-- {
		for _, block := range request.Messages[index].Blocks {
			if block.Type == canonical.BlockText {
				text := strings.ReplaceAll(block.Text, "\n", " ")
				if len([]rune(text)) > 60 {
					text = string([]rune(text)[:60]) + "…"
				}
				return text
			}
		}
	}
	return "(tool context)"
}

func renderReadableChat(request canonical.Request) string {
	var builder strings.Builder
	if hosted := renderHostedCapabilities(request.HostedCapabilities); hosted != "" {
		builder.WriteString("PROVIDER · ")
		builder.WriteString(hosted)
		builder.WriteString("\n\n")
	}
	if strings.TrimSpace(request.System) != "" {
		builder.WriteString("SYSTEM · instructions folded; press v for details\n\n")
	}
	for _, message := range request.Messages {
		label := map[canonical.Role]string{
			canonical.RoleSystem:    "SYSTEM",
			canonical.RoleUser:      "CLIENT",
			canonical.RoleAssistant: "YOU",
			canonical.RoleTool:      "TOOL",
		}[message.Role]
		if label == "" {
			label = "MESSAGE"
		}
		fmt.Fprintf(&builder, "%s\n", label)
		for _, block := range message.Blocks {
			switch block.Type {
			case canonical.BlockText:
				builder.WriteString(escapeTranscriptBody(block.Text))
				builder.WriteByte('\n')
			case canonical.BlockToolUse:
				builder.WriteString(escapeTranscriptBody(fmt.Sprintf(
					"→ %s %s", block.QualifiedToolName(), readableToolInput(block.ToolName, block.Input),
				)))
				builder.WriteByte('\n')
			case canonical.BlockToolResult:
				state := "result"
				if block.IsError {
					state = "error"
				}
				builder.WriteString(escapeTranscriptBody(fmt.Sprintf(
					"← tool %s · %s", state, shortOpaqueID(block.ToolCallID),
				)))
				builder.WriteByte('\n')
				builder.WriteString(escapeTranscriptBody(readableJSON(block.Output)))
				builder.WriteByte('\n')
			case canonical.BlockImage:
				builder.WriteString(escapeTranscriptBody("[image] " + block.ImageURL))
				builder.WriteByte('\n')
			}
		}
		builder.WriteByte('\n')
	}
	if builder.Len() == 0 {
		return "No chat messages."
	}
	return strings.TrimSpace(builder.String())
}

func escapeTranscriptBody(content string) string {
	lines := strings.Split(content, "\n")
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "CLIENT" || trimmed == "YOU" || trimmed == "TOOL" ||
			trimmed == "LOCAL WORKSPACE" ||
			strings.HasPrefix(trimmed, "SYSTEM") {
			lines[index] = "│ " + line
		}
	}
	return strings.Join(lines, "\n")
}

func readableToolInput(name string, input map[string]any) string {
	switch name {
	case "todowrite", "TodoWrite":
		if todos, ok := listLength(input["todos"]); ok {
			return fmt.Sprintf("· %d task(s)", len(todos))
		}
	case "update_plan":
		if plan, ok := listLength(input["plan"]); ok {
			return fmt.Sprintf("· %d task(s)", len(plan))
		}
	case "bash":
		if command, ok := input["command"].(string); ok {
			return "· " + boundedSingleLine(command, 96)
		}
	}
	return readableJSON(input)
}

func listLength(value any) ([]any, bool) {
	switch list := value.(type) {
	case []any:
		return list, true
	case []map[string]any:
		result := make([]any, len(list))
		return result, true
	default:
		return nil, false
	}
}

func shortOpaqueID(id string) string {
	const tail = 8
	if len(id) <= tail {
		return id
	}
	return "…" + id[len(id)-tail:]
}

func readableJSON(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(encoded)
}

func renderRequest(request canonical.Request) string {
	var builder strings.Builder
	if hosted := renderHostedCapabilities(request.HostedCapabilities); hosted != "" {
		builder.WriteString(hosted)
		builder.WriteByte('\n')
	}
	if request.System != "" {
		builder.WriteString("system:\n")
		builder.WriteString(request.System)
		builder.WriteByte('\n')
	}
	for _, message := range request.Messages {
		fmt.Fprintf(&builder, "\n%s:\n", message.Role)
		for _, block := range message.Blocks {
			switch block.Type {
			case canonical.BlockText:
				builder.WriteString(block.Text + "\n")
			case canonical.BlockToolUse:
				fmt.Fprintf(&builder, "[tool use %s %s] %v\n", block.QualifiedToolName(), block.ToolCallID, block.Input)
			case canonical.BlockToolResult:
				fmt.Fprintf(&builder, "[tool result %s] %v\n", block.ToolCallID, block.Output)
			case canonical.BlockImage:
				fmt.Fprintf(&builder, "[image] %s\n", block.ImageURL)
			}
		}
	}
	return builder.String()
}
