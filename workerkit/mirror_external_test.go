package workerkit_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

// fakeMirror records the resolve/settle protocol so tests can assert the
// save-ahead ordering the worker must preserve.
type fakeMirror struct {
	mu       sync.Mutex
	reviews  chan workerkit.Review
	resolved [][]string
	settled  []workerkit.MirrorSettlement
	canceled []string
	calls    []llm.ToolCall
	fail     error
}

func newFakeMirror() *fakeMirror {
	return &fakeMirror{reviews: make(chan workerkit.Review, 8)}
}

func (mirror *fakeMirror) Reviews() <-chan workerkit.Review { return mirror.reviews }

func (mirror *fakeMirror) Resolve(_ context.Context, request workerkit.MirrorResolve) ([]llm.ToolCall, error) {
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	if mirror.fail != nil {
		err := mirror.fail
		mirror.fail = nil
		return nil, err
	}
	mirror.resolved = append(mirror.resolved, append([]string(nil), request.ChangeIDs...))
	if len(mirror.calls) > 0 {
		return append([]llm.ToolCall(nil), mirror.calls...), nil
	}
	calls := make([]llm.ToolCall, 0, len(request.ChangeIDs))
	for _, change := range request.ChangeIDs {
		calls = append(calls, llm.ToolCall{
			ID: "call-" + change, Name: "write", Input: map[string]any{"change": change},
		})
	}
	return calls, nil
}

func (mirror *fakeMirror) Settle(_ context.Context, settlement workerkit.MirrorSettlement) error {
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	mirror.settled = append(mirror.settled, settlement)
	return nil
}

func (mirror *fakeMirror) Cancel(_ context.Context, changeIDs []string) error {
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	mirror.canceled = append(mirror.canceled, changeIDs...)
	return nil
}

func (mirror *fakeMirror) settlements() []workerkit.MirrorSettlement {
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	return append([]workerkit.MirrorSettlement(nil), mirror.settled...)
}

func openMirrorWorker(t *testing.T, wire workerkit.Wire, mirror workerkit.Mirror) *workerkit.Worker {
	t.Helper()
	store, _ := workerkit.NewMemoryStateStore()
	worker, err := workerkit.Open(t.Context(), workerkit.Config{Wire: wire, State: store, Mirror: mirror})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*1e9)
		defer cancel()
		if err := worker.Shutdown(ctx); err != nil {
			t.Errorf("shutdown workerkit: %v", err)
		}
	})
	return worker
}

func workspaceAssignment(task, delivery string) llm.WorkerAssignmentDelivery {
	request := textOnlyRequest("fix it")
	request.Tools = []llm.Tool{
		{Name: "write", InputSchema: []byte(`{"type":"object"}`)},
		{Name: "bash", InputSchema: []byte(`{"type":"object"}`)},
	}
	return testAssignment(task, delivery, llm.TierWorkspace, request)
}

func TestWorkerMirrorReviewAppearsInSnapshot(t *testing.T) {
	wire := newFakeWire()
	mirror := newFakeMirror()
	worker := openMirrorWorker(t, wire, mirror)

	mirror.reviews <- workerkit.Review{
		Generation: 1,
		Changes: []workerkit.Change{{
			ID: "change-1", Path: "main.go", Kind: workerkit.ChangeModify, Diff: "-a\n+b\n",
		}},
	}
	state := waitFor(t, worker, func(state workerkit.State) bool {
		return state.Review != nil && state.Review.Generation == 1
	})
	if len(state.Review.Changes) != 1 || state.Review.Changes[0].ID != "change-1" {
		t.Fatalf("review = %+v", state.Review)
	}

	// A newer review replaces the older one wholesale.
	mirror.reviews <- workerkit.Review{Generation: 2}
	state = waitFor(t, worker, func(state workerkit.State) bool {
		return state.Review != nil && state.Review.Generation == 2
	})
	if len(state.Review.Changes) != 0 {
		t.Fatalf("stale changes survived: %+v", state.Review)
	}
}

func TestWorkerDeliverChangesSendsToolCallsAndSettlesOnResult(t *testing.T) {
	wire := newFakeWire()
	mirror := newFakeMirror()
	worker := openMirrorWorker(t, wire, mirror)

	wire.assignments <- workspaceAssignment("task-1", "delivery-1")
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-1")
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
	events := wire.sentEvents()
	if len(events) != 1 || events[0].Event.Type != llm.EventToolCalls ||
		events[0].Event.ToolCalls[0].ID != "call-change-1" {
		t.Fatalf("delivered events = %+v", events)
	}
	// Delivery alone must NOT settle: the baseline advances only when the
	// caller returns a successful tool result.
	if settled := mirror.settlements(); len(settled) != 0 {
		t.Fatalf("premature settlement: %+v", settled)
	}

	continuation := textOnlyRequest("continue")
	continuation.Tools = workspaceAssignment("", "").Assignment.Request.Tools
	continuation.Messages = append(continuation.Messages, llm.Message{
		Role: llm.RoleTool, Blocks: []llm.Block{{
			Type: llm.BlockToolResult, ToolCallID: "call-change-1", Output: "applied",
		}},
	})
	wire.assignments <- testAssignment("task-1", "delivery-2", llm.TierWorkspace, continuation)
	waitFor(t, worker, func(state workerkit.State) bool {
		for _, conversation := range state.Conversations {
			if conversation.Key == key && conversation.Phase == workerkit.PhaseActive {
				return true
			}
		}
		return false
	})
	settled := mirror.settlements()
	if len(settled) != 1 || settled[0].Outcome != workerkit.MirrorDelivered ||
		len(settled[0].ChangeIDs) != 1 || settled[0].ChangeIDs[0] != "change-1" {
		t.Fatalf("settlements after result = %+v", settled)
	}
}

func TestWorkerDeliverChangesFailureKeepsChangesPending(t *testing.T) {
	wire := newFakeWire()
	mirror := newFakeMirror()
	worker := openMirrorWorker(t, wire, mirror)

	wire.assignments <- workspaceAssignment("task-1", "delivery-1")
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-1")
	if err != nil {
		t.Fatal(err)
	}
	mirror.reviews <- workerkit.Review{Generation: 1, Changes: []workerkit.Change{{ID: "change-1", Path: "a"}}}
	waitFor(t, worker, func(state workerkit.State) bool { return state.Review != nil })

	injected := errors.New("send failed")
	wire.armSendError(injected)
	if err := worker.DeliverChanges(t.Context(), key, []string{"change-1"}); !errors.Is(err, injected) {
		t.Fatalf("failed delivery = %v", err)
	}
	if settled := mirror.settlements(); len(settled) != 0 {
		t.Fatalf("failed delivery settled changes: %+v", settled)
	}
	// A failed send must return the resolved change to review via Cancel, not
	// leave it invisible and unsettled.
	mirror.mu.Lock()
	canceled := append([]string(nil), mirror.canceled...)
	mirror.mu.Unlock()
	if len(canceled) != 1 || canceled[0] != "change-1" {
		t.Fatalf("failed delivery did not cancel the change: %v", canceled)
	}
	// A retry after the transport recovers succeeds and parks the conversation.
	if err := worker.DeliverChanges(t.Context(), key, []string{"change-1"}); err != nil {
		t.Fatal(err)
	}
	state := worker.Snapshot()
	if state.Conversations[0].Phase != workerkit.PhaseAwaitingResults {
		t.Fatalf("conversation phase = %s", state.Conversations[0].Phase)
	}
}

func TestWorkerDiscardChangesSettlesWithoutSending(t *testing.T) {
	wire := newFakeWire()
	mirror := newFakeMirror()
	worker := openMirrorWorker(t, wire, mirror)

	mirror.reviews <- workerkit.Review{Generation: 1, Changes: []workerkit.Change{{ID: "change-1", Path: "a"}}}
	waitFor(t, worker, func(state workerkit.State) bool { return state.Review != nil })
	if err := worker.DiscardChanges(t.Context(), []string{"change-1"}); err != nil {
		t.Fatal(err)
	}
	settled := mirror.settlements()
	if len(settled) != 1 || settled[0].Outcome != workerkit.MirrorDiscarded {
		t.Fatalf("discard settlements = %+v", settled)
	}
	if events := wire.sentEvents(); len(events) != 0 {
		t.Fatalf("discard sent events: %+v", events)
	}
}

func TestWorkerWithoutMirrorRejectsDeliverCommands(t *testing.T) {
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker, err := workerkit.Open(t.Context(), workerkit.Config{Wire: wire, State: store})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = worker.Shutdown(context.Background()) })
	err = worker.DeliverChanges(t.Context(), workerkit.ConversationKey{Caller: "c", TaskID: "t"}, []string{"x"})
	if !errors.Is(err, workerkit.ErrNoMirror) {
		t.Fatalf("deliver without mirror = %v", err)
	}
	if err := worker.DiscardChanges(t.Context(), []string{"x"}); !errors.Is(err, workerkit.ErrNoMirror) {
		t.Fatalf("discard without mirror = %v", err)
	}
}
