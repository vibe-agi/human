package web_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
)

type fakeMirror struct {
	mu      sync.Mutex
	reviews chan workerkit.Review
	settled []workerkit.MirrorSettlement
}

func (mirror *fakeMirror) Reviews() <-chan workerkit.Review { return mirror.reviews }

func (mirror *fakeMirror) Resolve(_ context.Context, request workerkit.MirrorResolve) ([]llm.ToolCall, error) {
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

func TestWebReviewDeliverAndDiscard(t *testing.T) {
	wire := newFakeWire()
	mirror := &fakeMirror{reviews: make(chan workerkit.Review, 4)}
	store, _ := workerkit.NewMemoryStateStore()
	worker, err := workerkit.Open(t.Context(), workerkit.Config{Wire: wire, State: store, Mirror: mirror})
	if err != nil {
		t.Fatal(err)
	}
	server, err := web.New(web.Config{Worker: worker, SessionToken: testToken, Heartbeat: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	listener := httptest.NewServer(server)
	t.Cleanup(func() {
		listener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = worker.Shutdown(ctx)
	})

	// A workspace conversation to deliver into.
	assignment := chatAssignment("task-1", "delivery-1", "fix it")
	assignment.Assignment.Task = llm.TaskContext{
		TaskID: "task-1", CapabilityTier: llm.TierWorkspace, WorkspaceKey: "workspace-a",
		HarnessID: "harness-a", HarnessVersion: "v1", HarnessSessionID: "session-a",
		WorkspaceRoot: "/workspace",
	}
	assignment.Assignment.Request.Tools = []llm.Tool{{Name: "write", InputSchema: []byte(`{"type":"object"}`)}}
	wire.assignments <- assignment
	waitForState(t, listener.URL, func(state map[string]any) bool {
		inbox, _ := state["inbox"].([]any)
		return len(inbox) == 1
	})
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/accept",
		map[string]string{"delivery": "delivery-1"}), http.StatusOK)

	mirror.reviews <- workerkit.Review{Generation: 1, Changes: []workerkit.Change{
		{ID: "change-1", Path: "main.go", Kind: workerkit.ChangeModify, Diff: "-a\n+b\n"},
		{ID: "change-2", Path: "scratch.txt", Kind: workerkit.ChangeCreate, Diff: "+tmp\n"},
	}}
	waitForState(t, listener.URL, func(state map[string]any) bool {
		review, _ := state["review"].(map[string]any)
		if review == nil {
			return false
		}
		changes, _ := review["changes"].([]any)
		return len(changes) == 2
	})

	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/review/deliver", map[string]any{
		"caller": "caller-a", "task_id": "task-1", "change_ids": []string{"change-1"},
	}), http.StatusOK)
	events := wire.sentEvents()
	if len(events) != 1 || events[0].Event.Type != llm.EventToolCalls ||
		events[0].Event.ToolCalls[0].ID != "call-change-1" {
		t.Fatalf("deliver events = %+v", events)
	}

	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/review/discard", map[string]any{
		"change_ids": []string{"change-2"},
	}), http.StatusOK)
	mirror.mu.Lock()
	settled := append([]workerkit.MirrorSettlement(nil), mirror.settled...)
	mirror.mu.Unlock()
	if len(settled) != 1 || settled[0].Outcome != workerkit.MirrorDiscarded ||
		settled[0].ChangeIDs[0] != "change-2" {
		t.Fatalf("discard settlements = %+v", settled)
	}

	// Unknown change maps to 404; delivering without a mirror maps to 409 (see
	// workerkit tests); here assert the unknown-change path over HTTP.
	notFound := doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/review/deliver", map[string]any{
		"caller": "caller-a", "task_id": "task-1", "change_ids": []string{"missing"},
	}), http.StatusNotFound)
	if notFound["error"] != "not_found" {
		t.Fatalf("unknown change = %v", notFound)
	}
}
