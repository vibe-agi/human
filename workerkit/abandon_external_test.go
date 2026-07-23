package workerkit_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

// deliverToAwaitingResults drives a conversation to awaiting_results with one
// in-flight reviewed change, so the abandon tests start from a stuck state.
func deliverToAwaitingResults(t *testing.T, worker *workerkit.Worker, wire *fakeWire, mirror *fakeMirror, task, delivery string) workerkit.ConversationKey {
	t.Helper()
	wire.assignments <- workspaceAssignment(task, delivery)
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), llm.WorkerDeliveryID(delivery))
	if err != nil {
		t.Fatal(err)
	}
	mirror.reviews <- workerkit.Review{Generation: 1, Changes: []workerkit.Change{
		{ID: "change-1", Path: "main.go", Kind: workerkit.ChangeModify},
	}}
	waitFor(t, worker, func(state workerkit.State) bool { return state.Review != nil })
	if err := worker.DeliverChanges(t.Context(), key, []string{"change-1"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, worker, func(state workerkit.State) bool {
		for _, conversation := range state.Conversations {
			if conversation.Key == key && conversation.Phase == workerkit.PhaseAwaitingResults {
				return true
			}
		}
		return false
	})
	return key
}

func TestWorkerAbandonCancelsInFlightDeliveryAndTerminates(t *testing.T) {
	wire := newFakeWire()
	mirror := newFakeMirror()
	worker := openMirrorWorker(t, wire, mirror)

	key := deliverToAwaitingResults(t, worker, wire, mirror, "task-1", "delivery-1")

	if err := worker.Abandon(t.Context(), key); err != nil {
		t.Fatal(err)
	}

	state := worker.Snapshot()
	var found *workerkit.Conversation
	for index := range state.Conversations {
		if state.Conversations[index].Key == key {
			found = &state.Conversations[index]
		}
	}
	if found == nil || found.Phase != workerkit.PhaseTerminal {
		t.Fatalf("conversation after abandon = %+v", found)
	}
	if found.Delivery != nil || len(found.ParkedCalls) != 0 {
		t.Fatalf("abandon left in-flight state: %+v", found)
	}

	// The in-flight reviewed change returns to review via Cancel; the baseline
	// never advances (no settlement) and no new wire event is sent.
	mirror.mu.Lock()
	canceled := append([]string(nil), mirror.canceled...)
	mirror.mu.Unlock()
	if len(canceled) != 1 || canceled[0] != "change-1" {
		t.Fatalf("abandon did not cancel the in-flight change: %v", canceled)
	}
	if settled := mirror.settlements(); len(settled) != 0 {
		t.Fatalf("abandon advanced the baseline: %+v", settled)
	}
	if events := wire.sentEvents(); len(events) != 1 { // only the DeliverChanges tool_calls
		t.Fatalf("abandon sent an extra wire event: %+v", events)
	}
}

func TestWorkerAbandonedTaskReappearsInInboxOnLateContinuation(t *testing.T) {
	wire := newFakeWire()
	mirror := newFakeMirror()
	worker := openMirrorWorker(t, wire, mirror)

	key := deliverToAwaitingResults(t, worker, wire, mirror, "task-1", "delivery-1")
	if err := worker.Abandon(t.Context(), key); err != nil {
		t.Fatal(err)
	}

	// A late continuation for the same task must not resume the abandoned
	// conversation; it enters the inbox as fresh work.
	continuation := textOnlyRequest("here are the results")
	continuation.Tools = workspaceAssignment("", "").Assignment.Request.Tools
	continuation.Messages = append(continuation.Messages, llm.Message{
		Role: llm.RoleTool, Blocks: []llm.Block{{
			Type: llm.BlockToolResult, ToolCallID: "call-change-1", Output: "applied",
		}},
	})
	wire.assignments <- testAssignment("task-1", "delivery-2", llm.TierWorkspace, continuation)
	waitFor(t, worker, func(state workerkit.State) bool {
		for _, item := range state.Inbox {
			if item.Delivery == "delivery-2" {
				return true
			}
		}
		return false
	})

	state := worker.Snapshot()
	for _, conversation := range state.Conversations {
		if conversation.Key == key && conversation.Phase != workerkit.PhaseTerminal {
			t.Fatalf("abandoned conversation was silently resumed: %+v", conversation)
		}
	}
}

func TestWorkerAbandonActiveConversationAndIsIdempotent(t *testing.T) {
	wire := newFakeWire()
	mirror := newFakeMirror()
	worker := openMirrorWorker(t, wire, mirror)

	wire.assignments <- workspaceAssignment("task-1", "delivery-1")
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-1")
	if err != nil {
		t.Fatal(err)
	}

	// A stuck active conversation (its caller gone) can be abandoned locally with
	// no wire event; it becomes terminal.
	wire.notices <- workerkit.Notice{
		Code: "caller_gone", Message: "caller disconnected", Caller: key.Caller, TaskID: key.TaskID,
	}
	waitFor(t, worker, func(state workerkit.State) bool {
		return len(state.Conversations) == 1 && state.Conversations[0].CallerGone
	})
	if err := worker.Abandon(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if events := wire.sentEvents(); len(events) != 0 {
		t.Fatalf("abandon sent a wire event: %+v", events)
	}
	state := worker.Snapshot()
	if len(state.Conversations) != 1 || state.Conversations[0].Phase != workerkit.PhaseTerminal {
		t.Fatalf("phase after abandon = %+v", state.Conversations)
	}
	// Abandon on an already-terminal conversation is a no-op.
	if err := worker.Abandon(t.Context(), key); err != nil {
		t.Fatalf("abandon on terminal = %v, want nil", err)
	}
}

func TestWorkerAbandonUnknownConversation(t *testing.T) {
	wire := newFakeWire()
	worker := openMirrorWorker(t, wire, newFakeMirror())
	err := worker.Abandon(t.Context(), workerkit.ConversationKey{
		Caller: llm.CallerID("nobody"), TaskID: llm.TaskID("nope"),
	})
	if !errors.Is(err, workerkit.ErrUnknownConversation) {
		t.Fatalf("abandon unknown = %v, want ErrUnknownConversation", err)
	}
}

// failingStateStore fails SaveConversation while failSaves is set, so a test can
// prove a command that cannot persist leaves in-memory state untouched.
type failingStateStore struct {
	workerkit.StateStore
	failSaves atomic.Bool
}

func (store *failingStateStore) SaveConversation(ctx context.Context, conversation workerkit.Conversation) error {
	if store.failSaves.Load() {
		return errors.New("injected save failure")
	}
	return store.StateStore.SaveConversation(ctx, conversation)
}

func TestWorkerAbandonSaveFailureLeavesConversationRetryable(t *testing.T) {
	// A2: a failed SaveConversation during abandon must not mark the conversation
	// terminal in memory. Otherwise the caller gets an error while a later retry
	// hits the idempotency short-circuit and reports a success that never
	// persisted — the abandon is silently lost on the next restart.
	wire := newFakeWire()
	mirror := newFakeMirror()
	memory, _ := workerkit.NewMemoryStateStore()
	store := &failingStateStore{StateStore: memory}
	worker, err := workerkit.Open(t.Context(), workerkit.Config{Wire: wire, State: store, Mirror: mirror})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = worker.Shutdown(ctx)
	})

	key := deliverToAwaitingResults(t, worker, wire, mirror, "task-1", "delivery-1")

	phaseOf := func() workerkit.Phase {
		t.Helper()
		for _, conversation := range worker.Snapshot().Conversations {
			if conversation.Key == key {
				return conversation.Phase
			}
		}
		t.Fatalf("conversation %v missing", key)
		return ""
	}

	// The durable write fails: abandon must error AND leave the conversation
	// non-terminal, so the caller's error is truthful.
	store.failSaves.Store(true)
	if err := worker.Abandon(t.Context(), key); err == nil {
		t.Fatal("abandon with a failing store returned nil; want the propagated save error")
	}
	if got := phaseOf(); got != workerkit.PhaseAwaitingResults {
		t.Fatalf("after a failed save the conversation phase = %q, want awaiting_results; a false terminal hides the failure", got)
	}

	// The store recovers: the retry must actually run the abandon (not the
	// idempotency short-circuit) and now persist the terminal conversation.
	store.failSaves.Store(false)
	if err := worker.Abandon(t.Context(), key); err != nil {
		t.Fatalf("abandon retry after store recovery: %v", err)
	}
	if got := phaseOf(); got != workerkit.PhaseTerminal {
		t.Fatalf("after a successful retry the conversation phase = %q, want terminal", got)
	}

	// The durable store holds the terminal conversation, not the stale
	// awaiting_results one — proving the retry persisted rather than short-circuited.
	durable, err := memory.ListConversations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, conversation := range durable {
		if conversation.Key != key {
			continue
		}
		found = true
		if conversation.Phase != workerkit.PhaseTerminal {
			t.Fatalf("durable conversation phase = %q, want terminal", conversation.Phase)
		}
	}
	if !found {
		t.Fatalf("durable store has no conversation for %v", key)
	}
}
