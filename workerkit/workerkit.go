// Package workerkit is the headless human-side domain layer of HumanLLM.
//
// It owns the accept/reply/final state machine, transcript and draft state,
// and continuation parking for tool calls — everything a human-facing UI
// needs except rendering. UIs (web, terminal, chat integrations) consume the
// command / notification / snapshot surface and hold no business state of
// their own.
//
// workerkit deliberately does NOT duplicate transport durability: the Wire
// port's implementation (the official llm/workerws.Client with its durable
// Journal, or an in-process llm.WorkerConnection) owns delivery replay across
// restarts. The StateStore persists only accepted conversations, drafts, and
// parked continuations; unaccepted inbox items are rebuilt from transport
// replay. workerkit never executes caller commands and never touches the
// caller's working tree.
package workerkit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vibe-agi/human/llm"
)

// MaxParkedContinuations bounds conversations parked on submitted tool calls.
// The cap is enforced before the tool_calls event is sent, so a full worker
// fails closed instead of accumulating unresumable state.
const MaxParkedContinuations = 32

var (
	ErrInvalidConfig         = errors.New("workerkit: invalid configuration")
	ErrClosed                = errors.New("workerkit: worker is closed")
	ErrInvalidCommand        = errors.New("workerkit: invalid command")
	ErrUnknownDelivery       = errors.New("workerkit: unknown inbox delivery")
	ErrUnknownConversation   = errors.New("workerkit: unknown conversation")
	ErrConversationTerminal  = errors.New("workerkit: conversation is terminal")
	ErrConversationNotActive = errors.New("workerkit: conversation is not active")
	ErrTooManyContinuations  = errors.New("workerkit: too many parked continuations")
	ErrNoMirror              = errors.New("workerkit: no Mirror is configured")
	ErrUnknownChange         = errors.New("workerkit: unknown review change")
)

// Config composes one Worker. Wire and State are required borrowed
// dependencies: the host keeps ownership and releases them after Shutdown.
type Config struct {
	Wire  Wire
	State StateStore
	// Mirror optionally enables the Live Workspace review/delivery surface.
	// Without it, DeliverChanges and DiscardChanges fail with ErrNoMirror.
	Mirror Mirror
	// AutoResponder optionally answers an assignment without human attention:
	// returning (text, true) confirms the assignment and delivers text as the
	// final event, and the item never reaches the inbox. Products use it for
	// caller housekeeping requests (e.g. OpenCode's tool-less title
	// generation). A failed auto-response falls back to the inbox.
	AutoResponder func(llm.WorkerAssignmentDelivery) (string, bool)
	// Clock supplies transcript timestamps. Nil uses time.Now.
	Clock func() time.Time
	// IDs allocates event and delivery identifiers. Nil uses crypto/rand. The
	// returned value must satisfy the transport's stable-key pattern.
	IDs func(kind string) (string, error)
}

// Worker is the headless human-side domain object. All methods are safe for
// concurrent use; commands are serialized internally.
type Worker struct {
	wire          Wire
	state         StateStore
	mirror        Mirror
	autoResponder func(llm.WorkerAssignmentDelivery) (string, bool)
	now           func() time.Time
	ids           func(kind string) (string, error)

	// command serializes every operation with a wire side effect (accept,
	// reject, the reply/final/tool-call family, deliver/discard, and the loop's
	// resume and auto-final branches) so a check-then-send sequence cannot
	// interleave with another. It is always acquired before mu; mu still guards
	// the short in-memory critical sections. Held across SendEvent, not across
	// Snapshot, so read-only viewers never block on a command.
	command sync.Mutex

	mu            sync.Mutex
	inbox         []InboxItem
	inboxRequests map[llm.WorkerDeliveryID]llm.Request
	pending       map[llm.WorkerDeliveryID]llm.WorkerAssignmentDelivery
	conversations map[ConversationKey]*Conversation
	review        *Review
	closed        bool

	notifications chan struct{}
	loopCtx       context.Context
	loopCancel    context.CancelFunc
	loopDone      chan struct{}
}

// Open validates the configuration, restores accepted conversations from the
// StateStore, and starts consuming the wire. ctx bounds construction only.
func Open(ctx context.Context, config Config) (*Worker, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.Wire == nil {
		return nil, fmt.Errorf("%w: Wire is required", ErrInvalidConfig)
	}
	if config.State == nil {
		return nil, fmt.Errorf("%w: StateStore is required", ErrInvalidConfig)
	}
	now := config.Clock
	if now == nil {
		now = time.Now
	}
	ids := config.IDs
	if ids == nil {
		ids = randomID
	}
	restored, err := config.State.ListConversations(ctx)
	if err != nil {
		return nil, fmt.Errorf("workerkit: restore conversations: %w", err)
	}
	loopCtx, loopCancel := context.WithCancel(context.Background())
	worker := &Worker{
		wire: config.Wire, state: config.State, mirror: config.Mirror,
		autoResponder: config.AutoResponder, now: now, ids: ids,
		inboxRequests: make(map[llm.WorkerDeliveryID]llm.Request),
		conversations: make(map[ConversationKey]*Conversation, len(restored)),
		notifications: make(chan struct{}, 1),
		loopCtx:       loopCtx, loopCancel: loopCancel, loopDone: make(chan struct{}),
	}
	for _, conversation := range restored {
		cloned := cloneConversation(conversation)
		worker.conversations[cloned.Key] = &cloned
	}
	go worker.run()
	return worker, nil
}

func randomID(kind string) (string, error) {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("workerkit: allocate %s id: %w", kind, err)
	}
	return kind + "-" + hex.EncodeToString(buffer), nil
}

// Notifications signals that Snapshot may have changed. It is coalescing: one
// pending signal covers any number of changes. UIs should re-read Snapshot on
// every receive.
func (worker *Worker) Notifications() <-chan struct{} { return worker.notifications }

// Snapshot returns a deep copy of the current state.
func (worker *Worker) Snapshot() State {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	state := State{
		Inbox:         append([]InboxItem(nil), worker.inbox...),
		Conversations: make([]Conversation, 0, len(worker.conversations)),
	}
	for _, conversation := range worker.conversations {
		state.Conversations = append(state.Conversations, cloneConversation(*conversation))
	}
	sort.Slice(state.Conversations, func(left, right int) bool {
		a, b := state.Conversations[left].Key, state.Conversations[right].Key
		if a.Caller != b.Caller {
			return a.Caller < b.Caller
		}
		return a.TaskID < b.TaskID
	})
	if worker.review != nil {
		review := *worker.review
		review.Changes = append([]Change(nil), worker.review.Changes...)
		state.Review = &review
	}
	return state
}

// Shutdown stops consuming the wire and waits for the internal loop. It does
// not shut down the borrowed Wire or StateStore.
func (worker *Worker) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is required", ErrInvalidConfig)
	}
	worker.mu.Lock()
	worker.closed = true
	worker.mu.Unlock()
	worker.loopCancel()
	select {
	case <-worker.loopDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (worker *Worker) notify() {
	select {
	case worker.notifications <- struct{}{}:
	default:
	}
}

func (worker *Worker) run() {
	defer close(worker.loopDone)
	assignments := worker.wire.Assignments()
	rejections := worker.wire.Rejections()
	var reviews <-chan Review
	if worker.mirror != nil {
		reviews = worker.mirror.Reviews()
	}
	for {
		select {
		case assignment, open := <-assignments:
			if !open {
				assignments = nil
				continue
			}
			worker.handleAssignment(assignment)
		case rejection, open := <-rejections:
			if !open {
				rejections = nil
				continue
			}
			worker.handleRejection(rejection)
		case review, open := <-reviews:
			if !open {
				reviews = nil
				continue
			}
			worker.mu.Lock()
			cloned := review
			cloned.Changes = append([]Change(nil), review.Changes...)
			worker.review = &cloned
			worker.mu.Unlock()
			worker.notify()
		case <-worker.wire.Done():
			return
		case <-worker.loopCtx.Done():
			return
		}
	}
}

func conversationKeyOf(delivery llm.WorkerAssignmentDelivery) ConversationKey {
	return ConversationKey{
		Caller: delivery.Assignment.Identity.CallerID,
		TaskID: delivery.Assignment.Identity.TaskID,
	}
}

func (worker *Worker) handleAssignment(delivery llm.WorkerAssignmentDelivery) {
	delivery = llm.CloneWorkerAssignmentDelivery(delivery)
	key := conversationKeyOf(delivery)

	// Hold the command lock across the whole assignment so a resume or
	// auto-final never interleaves with a concurrent human command.
	worker.command.Lock()
	defer worker.command.Unlock()

	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		return
	}
	// Duplicate of an item already awaiting a decision.
	for _, item := range worker.inbox {
		if item.Delivery == delivery.ID {
			worker.mu.Unlock()
			return
		}
	}
	conversation, exists := worker.conversations[key]
	if exists && conversation.Phase != PhaseTerminal {
		if conversation.Assignment.ID == delivery.ID {
			// Redelivery of the current assignment: our confirmation was lost.
			worker.mu.Unlock()
			_ = worker.wire.ConfirmAssignment(worker.loopCtx, delivery.ID)
			return
		}
		// Caller continuation for a live conversation. The resume is prepared on
		// a copy and becomes command-visible only after the conversation is
		// durably saved, the assignment is confirmed, and the mirror settled:
		// otherwise a command could bind events to a not-yet-acknowledged
		// assignment and be NACKed.
		resumed := cloneConversation(*conversation)
		worker.mu.Unlock()
		settlement := worker.resumeInto(&resumed, delivery)
		if err := worker.state.SaveConversation(worker.loopCtx, cloneConversation(resumed)); err != nil {
			// Fail closed: without a durable resume the assignment must replay
			// unconfirmed; the visible conversation was never touched.
			worker.notify()
			return
		}
		_ = worker.wire.ConfirmAssignment(worker.loopCtx, delivery.ID)
		if settlement != nil && worker.mirror != nil {
			// Settle is idempotent. A crash before this call does not re-settle
			// (the resume already cleared Delivery), but a filesystem mirror
			// reseeds the still-undelivered change into the next review from the
			// on-disk baseline, so the human re-delivers once — never a silent
			// baseline advance or loss.
			_ = worker.mirror.Settle(worker.loopCtx, *settlement)
		}
		worker.mu.Lock()
		if current, stillThere := worker.conversations[key]; stillThere {
			resumed.Draft = current.Draft
			worker.conversations[key] = &resumed
		}
		worker.mu.Unlock()
		worker.notify()
		return
	}

	// Auto-answered housekeeping never reaches the human.
	if worker.autoResponder != nil {
		if text, handled := worker.autoResponder(llm.CloneWorkerAssignmentDelivery(delivery)); handled {
			worker.mu.Unlock()
			if worker.autoFinal(delivery, text) {
				worker.notify()
				return
			}
			// Fall back to the inbox on any auto-response failure.
			worker.mu.Lock()
		}
	}

	// Fresh work (or a caller reusing a terminal task id): inbox.
	worker.inbox = append(worker.inbox, InboxItem{
		Delivery: delivery.ID, Key: key,
		Tier:       delivery.Assignment.Task.CapabilityTier,
		Preview:    requestPreview(delivery.Assignment.Request),
		ToolCount:  len(delivery.Assignment.Request.Tools),
		ReceivedAt: worker.now().UTC(),
	})
	worker.inboxRequests[delivery.ID] = delivery.Assignment.Request
	worker.pendingAssignments()[delivery.ID] = delivery
	worker.mu.Unlock()
	worker.notify()
}

// autoFinal answers one assignment with a final event. Confirm precedes send
// because a final is only valid after the assignment is accepted (the legacy
// bridge maps ConfirmAssignment to the accepted event the gateway state
// machine requires before any final; an in-process wire is order-agnostic). A
// send failure falls back to the inbox; the assignment is already accepted, so
// the transport redelivers it — on this session or, after a crash, on the next
// worker session — and the auto-responder answers again rather than losing the
// task. The inbox is a transient convenience for the crash-free case.
func (worker *Worker) autoFinal(delivery llm.WorkerAssignmentDelivery, text string) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	if err := worker.wire.ConfirmAssignment(worker.loopCtx, delivery.ID); err != nil {
		return false
	}
	event, err := worker.buildEvent(delivery, llm.Event{Type: llm.EventFinal, Text: text})
	if err != nil {
		return false
	}
	return worker.wire.SendEvent(worker.loopCtx, event) == nil
}

// pendingAssignments lazily allocates the delivery cache used by Accept and
// Reject. Guarded by worker.mu.
func (worker *Worker) pendingAssignments() map[llm.WorkerDeliveryID]llm.WorkerAssignmentDelivery {
	if worker.pending == nil {
		worker.pending = make(map[llm.WorkerDeliveryID]llm.WorkerAssignmentDelivery)
	}
	return worker.pending
}

// resumeInto folds a caller continuation into the conversation copy and
// returns the mirror settlement earned by a fully successful delivered batch.
func (worker *Worker) resumeInto(conversation *Conversation, delivery llm.WorkerAssignmentDelivery) *MirrorSettlement {
	now := worker.now().UTC()
	var settlement *MirrorSettlement
	if len(conversation.ParkedCalls) > 0 {
		results := extractToolResults(delivery.Assignment.Request)
		for _, call := range conversation.ParkedCalls {
			if result, found := results[call.ID]; found {
				entry := TranscriptEntry{
					At: now, Author: AuthorCaller, Kind: EntryToolResult,
					ToolCallID: call.ID, Text: result.text,
				}
				if result.isError {
					entry.Code = "tool_error"
				}
				conversation.Transcript = append(conversation.Transcript, entry)
			}
		}
		if pending := conversation.Delivery; pending != nil {
			allSucceeded := true
			for _, callID := range pending.CallIDs {
				result, found := results[callID]
				if !found || result.isError {
					allSucceeded = false
					break
				}
			}
			if allSucceeded {
				settlement = &MirrorSettlement{
					ChangeIDs: append([]string(nil), pending.ChangeIDs...),
					Outcome:   MirrorDelivered,
				}
			}
			// Whether it settles or failed, this batch is finished; failed
			// changes stay pending inside the mirror and reappear in review.
			conversation.Delivery = nil
		}
		conversation.ParkedCalls = nil
	} else if text := latestUserText(delivery.Assignment.Request); text != "" {
		conversation.Transcript = append(conversation.Transcript, TranscriptEntry{
			At: now, Author: AuthorCaller, Kind: EntryText, Text: text,
		})
	}
	conversation.Assignment = delivery
	conversation.Phase = PhaseActive
	conversation.UpdatedAt = now
	return settlement
}

func (worker *Worker) handleRejection(rejection Rejection) {
	// Serialize with commands: this mutates conversation phase.
	worker.command.Lock()
	defer worker.command.Unlock()
	key := ConversationKey{
		Caller: rejection.Delivery.Identity.CallerID,
		TaskID: rejection.Delivery.Identity.TaskID,
	}
	worker.mu.Lock()
	conversation, exists := worker.conversations[key]
	if exists {
		conversation.Transcript = append(conversation.Transcript, TranscriptEntry{
			At: worker.now().UTC(), Author: AuthorSystem, Kind: EntryRejected,
			Text: rejection.Receipt.Message, Code: string(rejection.Receipt.Code),
		})
		// The rejected event never reached the caller, so an optimistically
		// advanced phase (terminal after Final, parked after tool calls, or
		// awaiting-caller after Clarify) would strand the human on a
		// permanently-refused conversation. Return it to active so the human
		// can correct and resend.
		if conversation.Phase != PhaseActive {
			conversation.Phase = PhaseActive
			conversation.ParkedCalls = nil
			conversation.Delivery = nil
		}
		conversation.UpdatedAt = worker.now().UTC()
		saved := cloneConversation(*conversation)
		worker.mu.Unlock()
		// Persist before confirming so a crash cannot lose the human-visible
		// NACK; an unconfirmed rejection replays from the transport.
		if err := worker.state.SaveConversation(worker.loopCtx, saved); err != nil {
			worker.notify()
			return
		}
	} else {
		worker.mu.Unlock()
	}
	_ = worker.wire.ConfirmRejection(worker.loopCtx, rejection.Delivery.ID)
	worker.notify()
}

// Accept turns an inbox assignment into an active conversation. The
// conversation is persisted before the assignment is confirmed: a crash in
// between replays the assignment, which then re-confirms idempotently.
func (worker *Worker) Accept(ctx context.Context, delivery llm.WorkerDeliveryID) (ConversationKey, error) {
	if err := worker.checkCommand(ctx); err != nil {
		return ConversationKey{}, err
	}
	worker.command.Lock()
	defer worker.command.Unlock()
	worker.mu.Lock()
	index := worker.inboxIndexLocked(delivery)
	if index < 0 {
		worker.mu.Unlock()
		return ConversationKey{}, fmt.Errorf("%w: %s", ErrUnknownDelivery, delivery)
	}
	assignment, cached := worker.pendingAssignments()[delivery]
	if !cached {
		worker.mu.Unlock()
		return ConversationKey{}, fmt.Errorf("%w: %s", ErrUnknownDelivery, delivery)
	}
	key := conversationKeyOf(assignment)
	now := worker.now().UTC()
	conversation := Conversation{
		Key: key, Phase: PhaseActive, Assignment: assignment,
		Transcript: seedTranscript(assignment.Assignment.Request, now),
		UpdatedAt:  now,
	}
	worker.mu.Unlock()

	if err := worker.state.SaveConversation(ctx, cloneConversation(conversation)); err != nil {
		return ConversationKey{}, fmt.Errorf("workerkit: persist accepted conversation: %w", err)
	}
	if err := worker.wire.ConfirmAssignment(ctx, delivery); err != nil {
		return ConversationKey{}, fmt.Errorf("workerkit: confirm assignment: %w", err)
	}

	worker.mu.Lock()
	worker.conversations[key] = &conversation
	worker.removeInboxLocked(delivery)
	worker.mu.Unlock()
	worker.notify()
	return key, nil
}

// Reject sends a terminal rejection event for an inbox assignment and
// confirms it. No conversation is created.
func (worker *Worker) Reject(ctx context.Context, delivery llm.WorkerDeliveryID, reason string) error {
	if err := worker.checkCommand(ctx); err != nil {
		return err
	}
	worker.command.Lock()
	defer worker.command.Unlock()
	worker.mu.Lock()
	index := worker.inboxIndexLocked(delivery)
	if index < 0 {
		worker.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrUnknownDelivery, delivery)
	}
	assignment := worker.pendingAssignments()[delivery]
	worker.mu.Unlock()

	event, err := worker.buildEvent(assignment, llm.Event{Type: llm.EventRejected, Text: reason})
	if err != nil {
		return err
	}
	if err := worker.wire.SendEvent(ctx, event); err != nil {
		return fmt.Errorf("workerkit: send rejection: %w", err)
	}
	if err := worker.wire.ConfirmAssignment(ctx, delivery); err != nil {
		return fmt.Errorf("workerkit: confirm rejected assignment: %w", err)
	}
	// A prior Accept whose confirm failed can have left a durable record under
	// this key; rejecting the task must not leave that orphan to be recovered
	// as a live conversation on restart.
	key := conversationKeyOf(assignment)
	worker.mu.Lock()
	_, live := worker.conversations[key]
	worker.removeInboxLocked(delivery)
	worker.mu.Unlock()
	if !live {
		_ = worker.state.DeleteConversation(ctx, key)
	}
	worker.notify()
	return nil
}

// Reply sends a non-terminal progress segment; the human turn continues.
func (worker *Worker) Reply(ctx context.Context, key ConversationKey, text string) error {
	worker.command.Lock()
	defer worker.command.Unlock()
	return worker.sendConversationEvent(ctx, key, llm.Event{Type: llm.EventProgress, Text: text}, PhaseActive)
}

// Clarify hands the turn back to the caller with a question; the conversation
// awaits the caller's next request on the same task.
func (worker *Worker) Clarify(ctx context.Context, key ConversationKey, text string) error {
	worker.command.Lock()
	defer worker.command.Unlock()
	return worker.sendConversationEvent(ctx, key, llm.Event{Type: llm.EventClarification, Text: text}, PhaseAwaitingCaller)
}

// Final ends the conversation.
func (worker *Worker) Final(ctx context.Context, key ConversationKey, text string) error {
	worker.command.Lock()
	defer worker.command.Unlock()
	return worker.sendConversationEvent(ctx, key, llm.Event{Type: llm.EventFinal, Text: text}, PhaseTerminal)
}

// SubmitToolCalls asks the caller agent to execute tools and parks the
// conversation until the caller continues with their results.
func (worker *Worker) SubmitToolCalls(ctx context.Context, key ConversationKey, calls []llm.ToolCall) error {
	worker.command.Lock()
	defer worker.command.Unlock()
	return worker.sendToolCallBatch(ctx, key, calls, nil)
}

func (worker *Worker) sendToolCallBatch(
	ctx context.Context,
	key ConversationKey,
	calls []llm.ToolCall,
	delivery *PendingDelivery,
) error {
	if len(calls) == 0 {
		return fmt.Errorf("%w: at least one tool call is required", ErrInvalidCommand)
	}
	worker.mu.Lock()
	parked := 0
	for _, conversation := range worker.conversations {
		if conversation.Phase == PhaseAwaitingResults {
			parked++
		}
	}
	worker.mu.Unlock()
	if parked >= MaxParkedContinuations {
		return fmt.Errorf("%w: %d conversations already await tool results", ErrTooManyContinuations, parked)
	}
	cloned := make([]llm.ToolCall, len(calls))
	copy(cloned, calls)
	return worker.sendConversationEventWith(
		ctx, key, llm.Event{Type: llm.EventToolCalls, ToolCalls: cloned},
		PhaseAwaitingResults, cloned, delivery,
	)
}

// DeliverChanges projects reviewed workspace changes onto the caller's
// declared native tools and sends them as one tool-call batch. The mirror
// baseline advances only when the caller returns successful results for every
// call; a failed send or a failed result leaves the changes pending in review.
func (worker *Worker) DeliverChanges(ctx context.Context, key ConversationKey, changeIDs []string) error {
	if err := worker.checkCommand(ctx); err != nil {
		return err
	}
	if worker.mirror == nil {
		return ErrNoMirror
	}
	worker.command.Lock()
	defer worker.command.Unlock()
	if len(changeIDs) == 0 {
		return fmt.Errorf("%w: at least one change is required", ErrInvalidCommand)
	}
	worker.mu.Lock()
	if worker.review == nil {
		worker.mu.Unlock()
		return fmt.Errorf("%w: no review is available", ErrUnknownChange)
	}
	known := make(map[string]struct{}, len(worker.review.Changes))
	for _, change := range worker.review.Changes {
		known[change.ID] = struct{}{}
	}
	for _, changeID := range changeIDs {
		if _, exists := known[changeID]; !exists {
			worker.mu.Unlock()
			return fmt.Errorf("%w: %s", ErrUnknownChange, changeID)
		}
	}
	conversation, exists := worker.conversations[key]
	if !exists {
		worker.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrUnknownConversation, key)
	}
	if conversation.Phase != PhaseActive {
		phase := conversation.Phase
		worker.mu.Unlock()
		if phase == PhaseTerminal {
			return fmt.Errorf("%w: %v", ErrConversationTerminal, key)
		}
		return fmt.Errorf("%w: %v is %s", ErrConversationNotActive, key, phase)
	}
	tools := append([]llm.Tool(nil), conversation.Assignment.Assignment.Request.Tools...)
	root := conversation.Assignment.Assignment.Task.WorkspaceRoot
	worker.mu.Unlock()

	calls, err := worker.mirror.Resolve(ctx, MirrorResolve{
		ChangeIDs:     append([]string(nil), changeIDs...),
		Tools:         tools,
		WorkspaceRoot: root,
	})
	if err != nil {
		return fmt.Errorf("workerkit: resolve reviewed changes: %w", err)
	}
	if len(calls) == 0 {
		return fmt.Errorf("%w: mirror produced no tool calls", ErrInvalidCommand)
	}
	callIDs := make([]string, 0, len(calls))
	for _, call := range calls {
		callIDs = append(callIDs, call.ID)
	}
	delivery := &PendingDelivery{
		ChangeIDs: append([]string(nil), changeIDs...), CallIDs: callIDs,
	}
	if err := worker.sendToolCallBatch(ctx, key, calls, delivery); err != nil {
		// The batch never reached the transport; return the resolved changes to
		// review so they are neither invisible nor silently unsettled.
		_ = worker.mirror.Cancel(ctx, changeIDs)
		return err
	}
	return nil
}

// DiscardChanges settles reviewed changes as discarded without delivery.
func (worker *Worker) DiscardChanges(ctx context.Context, changeIDs []string) error {
	if err := worker.checkCommand(ctx); err != nil {
		return err
	}
	if worker.mirror == nil {
		return ErrNoMirror
	}
	worker.command.Lock()
	defer worker.command.Unlock()
	if len(changeIDs) == 0 {
		return fmt.Errorf("%w: at least one change is required", ErrInvalidCommand)
	}
	if err := worker.mirror.Settle(ctx, MirrorSettlement{
		ChangeIDs: append([]string(nil), changeIDs...), Outcome: MirrorDiscarded,
	}); err != nil {
		return fmt.Errorf("workerkit: discard reviewed changes: %w", err)
	}
	worker.notify()
	return nil
}

// SaveDraft persists an unsent reply for a conversation.
func (worker *Worker) SaveDraft(ctx context.Context, key ConversationKey, draft string) error {
	if err := worker.checkCommand(ctx); err != nil {
		return err
	}
	worker.command.Lock()
	defer worker.command.Unlock()
	worker.mu.Lock()
	conversation, exists := worker.conversations[key]
	if !exists {
		worker.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrUnknownConversation, key)
	}
	conversation.Draft = draft
	conversation.UpdatedAt = worker.now().UTC()
	saved := cloneConversation(*conversation)
	worker.mu.Unlock()
	if err := worker.state.SaveConversation(ctx, saved); err != nil {
		return fmt.Errorf("workerkit: persist draft: %w", err)
	}
	worker.notify()
	return nil
}

func (worker *Worker) sendConversationEvent(ctx context.Context, key ConversationKey, event llm.Event, next Phase) error {
	return worker.sendConversationEventWith(ctx, key, event, next, nil, nil)
}

func (worker *Worker) sendConversationEventWith(
	ctx context.Context,
	key ConversationKey,
	event llm.Event,
	next Phase,
	parked []llm.ToolCall,
	delivery *PendingDelivery,
) error {
	if err := worker.checkCommand(ctx); err != nil {
		return err
	}
	if event.Type != llm.EventToolCalls && strings.TrimSpace(event.Text) == "" {
		return fmt.Errorf("%w: event text is required", ErrInvalidCommand)
	}
	worker.mu.Lock()
	conversation, exists := worker.conversations[key]
	if !exists {
		worker.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrUnknownConversation, key)
	}
	switch conversation.Phase {
	case PhaseTerminal:
		worker.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrConversationTerminal, key)
	case PhaseActive:
	default:
		worker.mu.Unlock()
		return fmt.Errorf("%w: %v is %s", ErrConversationNotActive, key, conversation.Phase)
	}
	assignment := conversation.Assignment
	worker.mu.Unlock()

	wireEvent, err := worker.buildEvent(assignment, event)
	if err != nil {
		return err
	}
	if err := worker.wire.SendEvent(ctx, wireEvent); err != nil {
		return fmt.Errorf("workerkit: send %s event: %w", event.Type, err)
	}

	worker.mu.Lock()
	conversation, exists = worker.conversations[key]
	if !exists {
		worker.mu.Unlock()
		return nil
	}
	now := worker.now().UTC()
	entry := TranscriptEntry{At: now, Author: AuthorHuman, Kind: entryKindFor(event.Type), Text: event.Text}
	if event.Type == llm.EventToolCalls {
		entry.ToolCalls = parked
	}
	conversation.Transcript = append(conversation.Transcript, entry)
	conversation.Phase = next
	conversation.ParkedCalls = parked
	conversation.Delivery = delivery
	conversation.Draft = ""
	conversation.UpdatedAt = now
	saved := cloneConversation(*conversation)
	worker.mu.Unlock()

	if err := worker.state.SaveConversation(ctx, saved); err != nil {
		// The event is already durably owned by the transport; surfacing the
		// persistence failure lets the host retry SaveDraft/quiesce, while the
		// in-memory state stays coherent for the running process.
		worker.notify()
		return fmt.Errorf("workerkit: persist conversation after send: %w", err)
	}
	worker.notify()
	return nil
}

func (worker *Worker) buildEvent(assignment llm.WorkerAssignmentDelivery, event llm.Event) (llm.WorkerEventDelivery, error) {
	eventID, err := worker.ids("event")
	if err != nil {
		return llm.WorkerEventDelivery{}, err
	}
	deliveryID, err := worker.ids("delivery")
	if err != nil {
		return llm.WorkerEventDelivery{}, err
	}
	event.ID = eventID
	return llm.WorkerEventDelivery{
		ID:       llm.WorkerDeliveryID(deliveryID),
		Identity: assignment.Assignment.Identity,
		LeaseID:  assignment.Assignment.Lease.ID,
		Event:    event,
	}, nil
}

func (worker *Worker) checkCommand(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidCommand)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closed {
		return ErrClosed
	}
	return nil
}

func (worker *Worker) inboxIndexLocked(delivery llm.WorkerDeliveryID) int {
	for index, item := range worker.inbox {
		if item.Delivery == delivery {
			return index
		}
	}
	return -1
}

func (worker *Worker) removeInboxLocked(delivery llm.WorkerDeliveryID) {
	index := worker.inboxIndexLocked(delivery)
	if index >= 0 {
		worker.inbox = append(worker.inbox[:index], worker.inbox[index+1:]...)
	}
	delete(worker.inboxRequests, delivery)
	delete(worker.pendingAssignments(), delivery)
}

func entryKindFor(eventType llm.EventType) EntryKind {
	switch eventType {
	case llm.EventProgress:
		return EntryProgress
	case llm.EventClarification:
		return EntryClarification
	case llm.EventFinal:
		return EntryFinal
	case llm.EventToolCalls:
		return EntryToolCalls
	default:
		return EntryText
	}
}

func seedTranscript(request llm.Request, at time.Time) []TranscriptEntry {
	var entries []TranscriptEntry
	for _, message := range request.Messages {
		author := AuthorCaller
		if message.Role == llm.RoleAssistant {
			author = AuthorHuman
		}
		for _, block := range message.Blocks {
			if block.Type == llm.BlockText && block.Text != "" {
				entries = append(entries, TranscriptEntry{
					At: at, Author: author, Kind: EntryText, Text: block.Text,
				})
			}
		}
	}
	return entries
}

func latestUserText(request llm.Request) string {
	for index := len(request.Messages) - 1; index >= 0; index-- {
		message := request.Messages[index]
		if message.Role != llm.RoleUser {
			continue
		}
		var parts []string
		for _, block := range message.Blocks {
			if block.Type == llm.BlockText && block.Text != "" {
				parts = append(parts, block.Text)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "\n")
		}
	}
	return ""
}

type toolResult struct {
	text    string
	isError bool
}

func extractToolResults(request llm.Request) map[string]toolResult {
	results := make(map[string]toolResult)
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Type != llm.BlockToolResult || block.ToolCallID == "" {
				continue
			}
			results[block.ToolCallID] = toolResult{
				text: formatToolOutput(block.Output), isError: block.IsError,
			}
		}
	}
	return results
}

func formatToolOutput(output any) string {
	switch value := output.(type) {
	case string:
		return value
	case nil:
		return ""
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Sprintf("%v", value)
		}
		return string(encoded)
	}
}

func requestPreview(request llm.Request) string {
	text := latestUserText(request)
	const limit = 160
	if len(text) > limit {
		return text[:limit] + "…"
	}
	return text
}
