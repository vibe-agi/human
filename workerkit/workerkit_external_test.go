package workerkit_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

// fakeWire is a deterministic in-memory Wire with full call recording. It
// models the transport contract: assignments replay until confirmed, SendEvent
// is durable enqueue, NACKs arrive asynchronously on Rejections.
type fakeWire struct {
	mu           sync.Mutex
	assignments  chan llm.WorkerAssignmentDelivery
	rejections   chan workerkit.Rejection
	done         chan struct{}
	sent         []llm.WorkerEventDelivery
	confirmedA   []llm.WorkerDeliveryID
	confirmedR   []llm.WorkerDeliveryID
	sendErr      error
	confirmOrder []string
}

func newFakeWire() *fakeWire {
	return &fakeWire{
		assignments: make(chan llm.WorkerAssignmentDelivery, 64),
		rejections:  make(chan workerkit.Rejection, 64),
		done:        make(chan struct{}),
	}
}

func (wire *fakeWire) Assignments() <-chan llm.WorkerAssignmentDelivery { return wire.assignments }
func (wire *fakeWire) Rejections() <-chan workerkit.Rejection           { return wire.rejections }
func (wire *fakeWire) Done() <-chan struct{}                            { return wire.done }
func (wire *fakeWire) Err() error                                       { return nil }

func (wire *fakeWire) SendEvent(_ context.Context, delivery llm.WorkerEventDelivery) error {
	wire.mu.Lock()
	defer wire.mu.Unlock()
	if wire.sendErr != nil {
		err := wire.sendErr
		wire.sendErr = nil
		return err
	}
	wire.sent = append(wire.sent, llm.CloneWorkerEventDelivery(delivery))
	return nil
}

func (wire *fakeWire) ConfirmAssignment(_ context.Context, id llm.WorkerDeliveryID) error {
	wire.mu.Lock()
	defer wire.mu.Unlock()
	wire.confirmedA = append(wire.confirmedA, id)
	wire.confirmOrder = append(wire.confirmOrder, "assignment:"+string(id))
	return nil
}

func (wire *fakeWire) ConfirmRejection(_ context.Context, id llm.WorkerDeliveryID) error {
	wire.mu.Lock()
	defer wire.mu.Unlock()
	wire.confirmedR = append(wire.confirmedR, id)
	wire.confirmOrder = append(wire.confirmOrder, "rejection:"+string(id))
	return nil
}

func (wire *fakeWire) sentEvents() []llm.WorkerEventDelivery {
	wire.mu.Lock()
	defer wire.mu.Unlock()
	events := make([]llm.WorkerEventDelivery, len(wire.sent))
	copy(events, wire.sent)
	return events
}

func (wire *fakeWire) armSendError(err error) {
	wire.mu.Lock()
	defer wire.mu.Unlock()
	wire.sendErr = err
}

// recordingStateStore wraps the memory model and records the order of
// persistence relative to wire confirmations.
type recordingStateStore struct {
	workerkit.StateStore
	mu    sync.Mutex
	saves []string
}

func (store *recordingStateStore) SaveConversation(ctx context.Context, conversation workerkit.Conversation) error {
	store.mu.Lock()
	store.saves = append(store.saves, string(conversation.Key.TaskID))
	store.mu.Unlock()
	return store.StateStore.SaveConversation(ctx, conversation)
}

func testAssignment(task, delivery string, tier llm.CapabilityTier, request llm.Request) llm.WorkerAssignmentDelivery {
	taskContext := llm.TaskContext{CapabilityTier: tier}
	if tier != llm.TierChat {
		taskContext = llm.TaskContext{
			TaskID: llm.TaskID(task), CapabilityTier: tier,
			WorkspaceKey: "workspace-a", HarnessID: "harness-a", HarnessVersion: "v1",
			HarnessSessionID: "session-a", WorkspaceRoot: "/workspace",
		}
	}
	return llm.WorkerAssignmentDelivery{
		ID: llm.WorkerDeliveryID(delivery),
		Assignment: llm.Assignment{
			Identity: llm.CompletionIdentity{
				CallerID: "caller-a", RequestID: "request-" + delivery,
				TaskID: llm.TaskID(task), IdempotencyKey: llm.IdempotencyKey("turn-" + delivery),
			},
			Lease:    llm.WorkerLease{ID: llm.WorkerLeaseID("lease-" + delivery), Owner: "worker-a"},
			Boundary: llm.AssignmentAfterResponse,
			Task:     taskContext,
			Request:  request,
		},
	}
}

func textOnlyRequest(text string) llm.Request {
	return llm.Request{
		Model: "human", Stream: true,
		Messages: []llm.Message{{Role: llm.RoleUser, Blocks: []llm.Block{{Type: llm.BlockText, Text: text}}}},
	}
}

func openTestWorker(t *testing.T, wire workerkit.Wire, store workerkit.StateStore) *workerkit.Worker {
	t.Helper()
	worker, err := workerkit.Open(t.Context(), workerkit.Config{Wire: wire, State: store})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := worker.Shutdown(ctx); err != nil {
			t.Errorf("shutdown workerkit: %v", err)
		}
	})
	return worker
}

func waitFor(t *testing.T, worker *workerkit.Worker, condition func(workerkit.State) bool) workerkit.State {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		state := worker.Snapshot()
		if condition(state) {
			return state
		}
		select {
		case <-worker.Notifications():
		case <-deadline:
			t.Fatalf("condition not reached; state = %+v", worker.Snapshot())
		}
	}
}

func TestWorkerInboxDedupesRedeliveryAndAcceptConfirmsOnce(t *testing.T) {
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, wire, store)

	assignment := testAssignment("task-1", "delivery-1", llm.TierChat, textOnlyRequest("hello"))
	wire.assignments <- assignment
	wire.assignments <- assignment // transport redelivery of the same ID
	state := waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) > 0 })
	if len(state.Inbox) != 1 || state.Inbox[0].Delivery != "delivery-1" {
		t.Fatalf("inbox = %+v, want single deduplicated item", state.Inbox)
	}

	key, err := worker.Accept(t.Context(), "delivery-1")
	if err != nil {
		t.Fatal(err)
	}
	state = waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 0 })
	if len(state.Conversations) != 1 || state.Conversations[0].Key != key {
		t.Fatalf("conversations = %+v", state.Conversations)
	}
	if confirmed := wire.confirmedA; len(confirmed) != 1 || confirmed[0] != "delivery-1" {
		t.Fatalf("assignment confirmations = %v", confirmed)
	}
}

func TestWorkerReplyThenFinalProducesOrderedEvents(t *testing.T) {
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, wire, store)

	wire.assignments <- testAssignment("task-1", "delivery-1", llm.TierChat, textOnlyRequest("hello"))
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Reply(t.Context(), key, "working on it"); err != nil {
		t.Fatal(err)
	}
	if err := worker.Final(t.Context(), key, "done"); err != nil {
		t.Fatal(err)
	}
	events := wire.sentEvents()
	if len(events) != 2 ||
		events[0].Event.Type != llm.EventProgress || events[0].Event.Text != "working on it" ||
		events[1].Event.Type != llm.EventFinal || events[1].Event.Text != "done" {
		t.Fatalf("sent events = %+v", events)
	}
	if events[0].Identity.TaskID != "task-1" || events[0].LeaseID != "lease-delivery-1" {
		t.Fatalf("event identity = %+v", events[0])
	}
	if events[0].Event.ID == "" || events[0].Event.ID == events[1].Event.ID {
		t.Fatalf("event ids must be unique and non-empty: %+v", events)
	}
	state := waitFor(t, worker, func(state workerkit.State) bool {
		return len(state.Conversations) == 1 && state.Conversations[0].Phase == workerkit.PhaseTerminal
	})
	transcript := state.Conversations[0].Transcript
	if len(transcript) != 3 { // caller text + progress + final
		t.Fatalf("transcript = %+v", transcript)
	}

	// Terminal conversations reject further events.
	if err := worker.Reply(t.Context(), key, "late"); !errors.Is(err, workerkit.ErrConversationTerminal) {
		t.Fatalf("reply after terminal = %v", err)
	}
}

func TestWorkerRejectSendsTerminalRejectionThenConfirms(t *testing.T) {
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, wire, store)

	wire.assignments <- testAssignment("task-1", "delivery-1", llm.TierChat, textOnlyRequest("hello"))
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	if err := worker.Reject(t.Context(), "delivery-1", "not my domain"); err != nil {
		t.Fatal(err)
	}
	events := wire.sentEvents()
	if len(events) != 1 || events[0].Event.Type != llm.EventRejected {
		t.Fatalf("reject events = %+v", events)
	}
	wire.mu.Lock()
	order := append([]string(nil), wire.confirmOrder...)
	wire.mu.Unlock()
	if len(order) != 1 || order[0] != "assignment:delivery-1" {
		t.Fatalf("confirm order = %v", order)
	}
	state := waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 0 })
	if len(state.Conversations) != 0 {
		t.Fatalf("rejected assignment created a conversation: %+v", state.Conversations)
	}
}

func TestWorkerSendFailureLeavesRetryableCommand(t *testing.T) {
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, wire, store)

	wire.assignments <- testAssignment("task-1", "delivery-1", llm.TierChat, textOnlyRequest("hello"))
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-1")
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("transport unavailable")
	wire.armSendError(injected)
	if err := worker.Final(t.Context(), key, "done"); !errors.Is(err, injected) {
		t.Fatalf("failed send = %v", err)
	}
	state := worker.Snapshot()
	if state.Conversations[0].Phase == workerkit.PhaseTerminal {
		t.Fatal("failed terminal send must not mark the conversation terminal")
	}
	if err := worker.Final(t.Context(), key, "done"); err != nil {
		t.Fatal(err)
	}
	if events := wire.sentEvents(); len(events) != 1 || events[0].Event.Type != llm.EventFinal {
		t.Fatalf("events after retry = %+v", events)
	}
}

func TestWorkerToolCallsParkAndResumeWithResults(t *testing.T) {
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, wire, store)

	request := textOnlyRequest("fix the bug")
	request.Tools = []llm.Tool{{Name: "bash", InputSchema: []byte(`{"type":"object"}`)}}
	wire.assignments <- testAssignment("task-1", "delivery-1", llm.TierWorkspace, request)
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-1")
	if err != nil {
		t.Fatal(err)
	}
	calls := []llm.ToolCall{{ID: "call-1", Name: "bash", Input: map[string]any{"command": "ls"}}}
	if err := worker.SubmitToolCalls(t.Context(), key, calls); err != nil {
		t.Fatal(err)
	}
	state := waitFor(t, worker, func(state workerkit.State) bool {
		return len(state.Conversations) == 1 && state.Conversations[0].Phase == workerkit.PhaseAwaitingResults
	})
	if state.Conversations[0].Key != key {
		t.Fatalf("conversation = %+v", state.Conversations[0])
	}

	// The caller executes the tool and continues with a new assignment carrying
	// the tool result for the same task.
	continuation := textOnlyRequest("continue")
	continuation.Messages = append(continuation.Messages, llm.Message{
		Role: llm.RoleTool, Blocks: []llm.Block{{
			Type: llm.BlockToolResult, ToolCallID: "call-1", Output: map[string]any{"stdout": "main.go"},
		}},
	})
	continuation.Tools = request.Tools
	wire.assignments <- testAssignment("task-1", "delivery-2", llm.TierWorkspace, continuation)

	state = waitFor(t, worker, func(state workerkit.State) bool {
		return len(state.Conversations) == 1 && state.Conversations[0].Phase == workerkit.PhaseActive
	})
	if len(state.Inbox) != 0 {
		t.Fatalf("continuation went to inbox instead of resuming: %+v", state.Inbox)
	}
	if confirmed := wire.confirmedA; len(confirmed) != 2 || confirmed[1] != "delivery-2" {
		t.Fatalf("continuation was not auto-confirmed: %v", confirmed)
	}
	transcript := state.Conversations[0].Transcript
	last := transcript[len(transcript)-1]
	if last.Kind != workerkit.EntryToolResult || last.ToolCallID != "call-1" {
		t.Fatalf("transcript tail = %+v", last)
	}

	// Later events must bind to the continuation's lease, not the original one.
	if err := worker.Final(t.Context(), key, "all fixed"); err != nil {
		t.Fatal(err)
	}
	events := wire.sentEvents()
	final := events[len(events)-1]
	if final.LeaseID != "lease-delivery-2" || final.Identity.RequestID != "request-delivery-2" {
		t.Fatalf("final bound to stale assignment: %+v", final)
	}
}

func TestWorkerContinuationCapFailsClosed(t *testing.T) {
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, wire, store)

	for index := 0; index < workerkit.MaxParkedContinuations; index++ {
		task := fmt.Sprintf("task-%02d", index)
		delivery := fmt.Sprintf("delivery-%02d", index)
		request := textOnlyRequest("work")
		request.Tools = []llm.Tool{{Name: "bash", InputSchema: []byte(`{"type":"object"}`)}}
		wire.assignments <- testAssignment(task, delivery, llm.TierWorkspace, request)
		waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
		key, err := worker.Accept(t.Context(), llm.WorkerDeliveryID(delivery))
		if err != nil {
			t.Fatal(err)
		}
		if err := worker.SubmitToolCalls(t.Context(), key, []llm.ToolCall{{
			ID: "call-" + task, Name: "bash", Input: map[string]any{"n": index},
		}}); err != nil {
			t.Fatalf("park %d: %v", index, err)
		}
	}

	request := textOnlyRequest("one too many")
	request.Tools = []llm.Tool{{Name: "bash", InputSchema: []byte(`{"type":"object"}`)}}
	wire.assignments <- testAssignment("task-overflow", "delivery-overflow", llm.TierWorkspace, request)
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-overflow")
	if err != nil {
		t.Fatal(err)
	}
	err = worker.SubmitToolCalls(t.Context(), key, []llm.ToolCall{{
		ID: "call-overflow", Name: "bash", Input: map[string]any{},
	}})
	if !errors.Is(err, workerkit.ErrTooManyContinuations) {
		t.Fatalf("continuation overflow = %v", err)
	}
}

func TestWorkerRestartRestoresConversationsAndReplaysInbox(t *testing.T) {
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, wire, store)

	// One accepted conversation awaiting results, one unaccepted inbox item.
	request := textOnlyRequest("fix")
	request.Tools = []llm.Tool{{Name: "bash", InputSchema: []byte(`{"type":"object"}`)}}
	wire.assignments <- testAssignment("task-parked", "delivery-parked", llm.TierWorkspace, request)
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-parked")
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.SubmitToolCalls(t.Context(), key, []llm.ToolCall{{
		ID: "call-1", Name: "bash", Input: map[string]any{},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := worker.SaveDraft(t.Context(), key, "half-written reply"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := worker.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()

	// Restart: the transport replays the unconfirmed assignment; the state
	// store restores the accepted conversation and its draft.
	rewire := newFakeWire()
	rewire.assignments <- testAssignment("task-inbox", "delivery-inbox", llm.TierChat, textOnlyRequest("new"))
	reopened := openTestWorker(t, rewire, store)
	state := waitFor(t, reopened, func(state workerkit.State) bool {
		return len(state.Inbox) == 1 && len(state.Conversations) == 1
	})
	if state.Inbox[0].Delivery != "delivery-inbox" {
		t.Fatalf("replayed inbox = %+v", state.Inbox)
	}
	conversation := state.Conversations[0]
	if conversation.Key != key || conversation.Phase != workerkit.PhaseAwaitingResults ||
		conversation.Draft != "half-written reply" {
		t.Fatalf("restored conversation = %+v", conversation)
	}

	// The parked continuation still resumes after restart.
	continuation := textOnlyRequest("continue")
	continuation.Messages = append(continuation.Messages, llm.Message{
		Role: llm.RoleTool, Blocks: []llm.Block{{
			Type: llm.BlockToolResult, ToolCallID: "call-1", Output: "ok",
		}},
	})
	continuation.Tools = request.Tools
	rewire.assignments <- testAssignment("task-parked", "delivery-resume", llm.TierWorkspace, continuation)
	waitFor(t, reopened, func(state workerkit.State) bool {
		for _, conversation := range state.Conversations {
			if conversation.Key == key && conversation.Phase == workerkit.PhaseActive {
				return true
			}
		}
		return false
	})
}

func TestWorkerRejectionRecordedBeforeConfirm(t *testing.T) {
	wire := newFakeWire()
	memory, _ := workerkit.NewMemoryStateStore()
	store := &recordingStateStore{StateStore: memory}
	worker := openTestWorker(t, wire, store)

	wire.assignments <- testAssignment("task-1", "delivery-1", llm.TierChat, textOnlyRequest("hello"))
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Reply(t.Context(), key, "progress"); err != nil {
		t.Fatal(err)
	}
	sent := wire.sentEvents()[0]
	wire.rejections <- workerkit.Rejection{
		Delivery: sent,
		Receipt: llm.WorkerEventReceipt{
			Delivery: sent.ID, EventID: sent.Event.ID,
			Decision: llm.WorkerEventNACK, Code: llm.WorkerRejectInvalid, Message: "too late",
		},
	}
	state := waitFor(t, worker, func(state workerkit.State) bool {
		transcript := state.Conversations[0].Transcript
		return transcript[len(transcript)-1].Kind == workerkit.EntryRejected
	})
	_ = state
	wire.mu.Lock()
	confirmations := append([]llm.WorkerDeliveryID(nil), wire.confirmedR...)
	wire.mu.Unlock()
	if len(confirmations) != 1 || confirmations[0] != sent.ID {
		t.Fatalf("rejection confirmations = %v", confirmations)
	}
	// Persistence must precede rejection confirmation so a crash cannot lose
	// the human-visible NACK.
	store.mu.Lock()
	saves := len(store.saves)
	store.mu.Unlock()
	if saves == 0 {
		t.Fatal("rejection was confirmed without persisting the conversation")
	}
}
