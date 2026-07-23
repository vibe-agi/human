package web_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
	"github.com/vibe-agi/human/workerkit/fsmirror"
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

func (mirror *fakeMirror) Cancel(context.Context, []string) error { return nil }

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

func TestWebCanSwitchConversationToExistingHumanRepo(t *testing.T) {
	base := t.TempDir()
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "existing.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mirror, err := fsmirror.Open(t.Context(), fsmirror.Config{
		Root: base, Scope: workerkit.WorkspaceScope{Caller: "caller-a", WorkspaceKey: "workspace-a"},
		Build: func(change workerkit.Change, content []byte, _ workerkit.MirrorResolve) ([]llm.ToolCall, error) {
			return []llm.ToolCall{{
				ID: "call-" + change.ID, Name: "write",
				Input: map[string]any{"filePath": change.Path, "content": string(content)},
			}}, nil
		},
		Debounce: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mirror.Close() })

	wire := newFakeWire()
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

	assignment := chatAssignment("task-switch", "delivery-switch", "edit the repo")
	assignment.Assignment.Task = llm.TaskContext{
		TaskID: "task-switch", CapabilityTier: llm.TierWorkspace, WorkspaceKey: "workspace-a",
		HarnessID: "opencode", HarnessVersion: "1.17.18", HarnessSessionID: "session-switch",
	}
	assignment.Assignment.Request.Tools = []llm.Tool{{Name: "write", InputSchema: []byte(`{"type":"object"}`)}}
	wire.assignments <- assignment
	waitForState(t, listener.URL, func(state map[string]any) bool {
		inbox, _ := state["inbox"].([]any)
		return len(inbox) == 1
	})
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/accept",
		map[string]string{"delivery": "delivery-switch"}), http.StatusOK)
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/workspace", map[string]any{
		"caller": "caller-a", "task_id": "task-switch", "path": repo,
	}), http.StatusOK)
	canonicalRepo, err := filepath.EvalSymlinks(repo)
	if err != nil {
		t.Fatal(err)
	}
	state := waitForState(t, listener.URL, func(state map[string]any) bool {
		conversations, _ := state["conversations"].([]any)
		if len(conversations) != 1 {
			return false
		}
		conversation, _ := conversations[0].(map[string]any)
		workspace, _ := conversation["human_workspace"].(map[string]any)
		return workspace["path"] == canonicalRepo
	})
	conversations, _ := state["conversations"].([]any)
	conversation, _ := conversations[0].(map[string]any)
	workspace, _ := conversation["human_workspace"].(map[string]any)
	workspaceID, _ := workspace["id"].(string)
	if workspaceID == "" {
		t.Fatal("Web state omitted Human workspace id")
	}

	if err := os.WriteFile(filepath.Join(repo, "existing.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForState(t, listener.URL, func(state map[string]any) bool {
		review, _ := state["review"].(map[string]any)
		changes, _ := review["changes"].([]any)
		for _, raw := range changes {
			change, _ := raw.(map[string]any)
			if change["workspace_id"] == workspaceID && change["path"] == "existing.txt" &&
				change["kind"] == string(workerkit.ChangeModify) {
				return true
			}
		}
		return false
	})
}
