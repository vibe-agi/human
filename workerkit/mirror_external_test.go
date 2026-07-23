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
	scopes   []workerkit.WorkspaceScope
	settled  []workerkit.MirrorSettlement
	canceled []string
	calls    []llm.ToolCall
	fail     error
}

func newFakeMirror() *fakeMirror {
	return &fakeMirror{reviews: make(chan workerkit.Review, 8)}
}

type fakeSessionMirror struct {
	*fakeMirror
	mu           sync.Mutex
	failingPaths map[string]error
	prepared     []string
}

func newFakeSessionMirror() *fakeSessionMirror {
	return &fakeSessionMirror{
		fakeMirror:   newFakeMirror(),
		failingPaths: make(map[string]error),
	}
}

func (mirror *fakeSessionMirror) PrepareSession(
	_ context.Context,
	binding workerkit.SessionBinding,
	preferredPath string,
) (workerkit.HumanWorkspace, error) {
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	mirror.prepared = append(mirror.prepared, preferredPath)
	if err := mirror.failingPaths[preferredPath]; err != nil {
		return workerkit.HumanWorkspace{}, err
	}
	id, err := workerkit.SessionWorkspaceID(binding)
	if err != nil {
		return workerkit.HumanWorkspace{}, err
	}
	path := preferredPath
	if path == "" {
		path = "/human-base/" + id
	}
	return workerkit.HumanWorkspace{ID: id, Path: path, Available: true}, nil
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
	mirror.scopes = append(mirror.scopes, request.Scope)
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
	return openMirrorWorkerWithStore(t, wire, store, mirror)
}

func openMirrorWorkerWithStore(
	t *testing.T,
	wire workerkit.Wire,
	store workerkit.StateStore,
	mirror workerkit.Mirror,
) *workerkit.Worker {
	t.Helper()
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

func TestWorkerRestoresSelectedHumanWorkspaceAndRecoversMissingRepo(t *testing.T) {
	store, _ := workerkit.NewMemoryStateStore()

	wire := newFakeWire()
	mirror := newFakeSessionMirror()
	worker := openMirrorWorkerWithStore(t, wire, store, mirror)
	wire.assignments <- workspaceAssignment("task-workspace-restore", "delivery-workspace-restore")
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), "delivery-workspace-restore")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := worker.SetHumanWorkspace(t.Context(), key, "/human/repos/project-a")
	if err != nil {
		t.Fatal(err)
	}
	if !selected.Available || selected.Path != "/human/repos/project-a" {
		t.Fatalf("selected Human workspace = %+v", selected)
	}
	shutdownWorker(t, worker)

	// An external repo can disappear while Human is stopped (for example, a
	// removable disk or a backup restored on another host). Startup must retain
	// the selection for display and recovery instead of failing the daemon.
	missing := newFakeSessionMirror()
	missing.failingPaths["/human/repos/project-a"] = errors.New("repo is offline")
	reopened := openMirrorWorkerWithStore(t, newFakeWire(), store, missing)
	state := reopened.Snapshot()
	if len(state.Conversations) != 1 || state.Conversations[0].HumanWorkspace == nil {
		t.Fatalf("restored state = %+v", state)
	}
	restored := state.Conversations[0].HumanWorkspace
	if restored.Path != "/human/repos/project-a" || restored.Available {
		t.Fatalf("missing Human workspace = %+v", restored)
	}

	rebound, err := reopened.SetHumanWorkspace(t.Context(), key, "/human/repos/project-b")
	if err != nil {
		t.Fatal(err)
	}
	if !rebound.Available || rebound.Path != "/human/repos/project-b" {
		t.Fatalf("rebound Human workspace = %+v", rebound)
	}
	shutdownWorker(t, reopened)

	// The replacement is durable and canonicalized again on the next restart.
	thirdMirror := newFakeSessionMirror()
	third := openMirrorWorkerWithStore(t, newFakeWire(), store, thirdMirror)
	thirdState := third.Snapshot()
	if len(thirdState.Conversations) != 1 ||
		thirdState.Conversations[0].HumanWorkspace == nil ||
		thirdState.Conversations[0].HumanWorkspace.Path != "/human/repos/project-b" ||
		!thirdState.Conversations[0].HumanWorkspace.Available {
		t.Fatalf("rebound workspace after restart = %+v", thirdState)
	}
}

func shutdownWorker(t *testing.T, worker *workerkit.Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*1e9)
	defer cancel()
	if err := worker.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
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
	mirror.mu.Lock()
	scopes := append([]workerkit.WorkspaceScope(nil), mirror.scopes...)
	mirror.mu.Unlock()
	if len(scopes) != 1 || scopes[0] != (workerkit.WorkspaceScope{
		Caller: "caller-a", WorkspaceKey: "workspace-a",
	}) {
		t.Fatalf("mirror resolve scopes = %+v", scopes)
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

func TestWorkerFailedToolResultReleasesChangeForRetry(t *testing.T) {
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
	if err := worker.DeliverChanges(t.Context(), key, []string{"change-1"}); err != nil {
		t.Fatal(err)
	}

	continuation := textOnlyRequest("continue")
	continuation.Tools = workspaceAssignment("", "").Assignment.Request.Tools
	continuation.Messages = append(continuation.Messages, llm.Message{
		Role: llm.RoleTool, Blocks: []llm.Block{{
			Type: llm.BlockToolResult, ToolCallID: "call-change-1",
			Output: "write failed", IsError: true,
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

	if settled := mirror.settlements(); len(settled) != 0 {
		t.Fatalf("failed tool result advanced baseline: %+v", settled)
	}
	mirror.mu.Lock()
	canceled := append([]string(nil), mirror.canceled...)
	mirror.mu.Unlock()
	if len(canceled) != 1 || canceled[0] != "change-1" {
		t.Fatalf("failed tool result did not release change for retry: %v", canceled)
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
