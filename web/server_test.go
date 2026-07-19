package web_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
)

const testToken = "test-session-token-0123456789"

type fakeWire struct {
	mu          sync.Mutex
	assignments chan llm.WorkerAssignmentDelivery
	rejections  chan workerkit.Rejection
	done        chan struct{}
	sent        []llm.WorkerEventDelivery
}

func newFakeWire() *fakeWire {
	return &fakeWire{
		assignments: make(chan llm.WorkerAssignmentDelivery, 16),
		rejections:  make(chan workerkit.Rejection, 16),
		done:        make(chan struct{}),
	}
}

func (wire *fakeWire) Assignments() <-chan llm.WorkerAssignmentDelivery              { return wire.assignments }
func (wire *fakeWire) Rejections() <-chan workerkit.Rejection                        { return wire.rejections }
func (wire *fakeWire) Done() <-chan struct{}                                         { return wire.done }
func (wire *fakeWire) Err() error                                                    { return nil }
func (wire *fakeWire) ConfirmAssignment(context.Context, llm.WorkerDeliveryID) error { return nil }
func (wire *fakeWire) ConfirmRejection(context.Context, llm.WorkerDeliveryID) error  { return nil }

func (wire *fakeWire) SendEvent(_ context.Context, delivery llm.WorkerEventDelivery) error {
	wire.mu.Lock()
	defer wire.mu.Unlock()
	wire.sent = append(wire.sent, llm.CloneWorkerEventDelivery(delivery))
	return nil
}

func (wire *fakeWire) sentEvents() []llm.WorkerEventDelivery {
	wire.mu.Lock()
	defer wire.mu.Unlock()
	return append([]llm.WorkerEventDelivery(nil), wire.sent...)
}

func chatAssignment(task, delivery, text string) llm.WorkerAssignmentDelivery {
	return llm.WorkerAssignmentDelivery{
		ID: llm.WorkerDeliveryID(delivery),
		Assignment: llm.Assignment{
			Identity: llm.CompletionIdentity{
				CallerID: "caller-a", RequestID: "request-" + delivery,
				TaskID: llm.TaskID(task), IdempotencyKey: llm.IdempotencyKey("turn-" + delivery),
			},
			Lease:    llm.WorkerLease{ID: llm.WorkerLeaseID("lease-" + delivery), Owner: "worker-a"},
			Boundary: llm.AssignmentAfterResponse,
			Task:     llm.TaskContext{CapabilityTier: llm.TierChat},
			Request: llm.Request{
				Model: "human", Stream: true,
				Messages: []llm.Message{{Role: llm.RoleUser, Blocks: []llm.Block{{Type: llm.BlockText, Text: text}}}},
			},
		},
	}
}

func openWebServer(t *testing.T) (*fakeWire, *workerkit.Worker, *httptest.Server) {
	t.Helper()
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker, err := workerkit.Open(t.Context(), workerkit.Config{Wire: wire, State: store})
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
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown web server: %v", err)
		}
		if err := worker.Shutdown(ctx); err != nil {
			t.Errorf("shutdown worker: %v", err)
		}
	})
	return wire, worker, listener
}

func authedRequest(t *testing.T, method, url string, body any) *http.Request {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+testToken)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return request
}

func doJSON(t *testing.T, request *http.Request, wantStatus int) map[string]any {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("status = %d body = %v, want %d", response.StatusCode, decoded, wantStatus)
	}
	return decoded
}

func waitForState(t *testing.T, base string, condition func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		state := doJSON(t, authedRequest(t, http.MethodGet, base+"/api/state", nil), http.StatusOK)
		if condition(state) {
			return state
		}
		if time.Now().After(deadline) {
			t.Fatalf("state condition not reached: %v", state)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestWebRequiresSession(t *testing.T) {
	_, _, listener := openWebServer(t)

	response, err := http.Get(listener.URL + "/api/state")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated state = %d", response.StatusCode)
	}

	// A wrong bearer token is rejected.
	request, _ := http.NewRequest(http.MethodGet, listener.URL+"/api/state", nil)
	request.Header.Set("Authorization", "Bearer wrong-token-wrong-token")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token state = %d", response.StatusCode)
	}

	// The one-time token URL sets the session cookie and redirects.
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	response, err = client.Get(listener.URL + "/?token=" + testToken)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusSeeOther {
		t.Fatalf("token exchange = %d", response.StatusCode)
	}
	var cookie *http.Cookie
	for _, candidate := range response.Cookies() {
		if candidate.Name == "human_web_session" {
			cookie = candidate
		}
	}
	if cookie == nil || cookie.Value != testToken || !cookie.HttpOnly {
		t.Fatalf("session cookie = %+v", cookie)
	}

	// The cookie authenticates both the page and the API.
	request, _ = http.NewRequest(http.MethodGet, listener.URL+"/api/state", nil)
	request.AddCookie(cookie)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("cookie state = %d", response.StatusCode)
	}
}

func TestWebAcceptReplyFinalFlow(t *testing.T) {
	wire, _, listener := openWebServer(t)
	wire.assignments <- chatAssignment("task-1", "delivery-1", "please help")

	waitForState(t, listener.URL, func(state map[string]any) bool {
		inbox, _ := state["inbox"].([]any)
		return len(inbox) == 1
	})

	accepted := doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/accept",
		map[string]string{"delivery": "delivery-1"}), http.StatusOK)
	key, _ := accepted["key"].(map[string]any)
	if key["caller"] != "caller-a" || key["task_id"] != "task-1" {
		t.Fatalf("accept key = %v", accepted)
	}

	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/reply",
		map[string]string{"caller": "caller-a", "task_id": "task-1", "text": "looking"}), http.StatusOK)
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/final",
		map[string]string{"caller": "caller-a", "task_id": "task-1", "text": "done"}), http.StatusOK)

	events := wire.sentEvents()
	if len(events) != 2 || events[0].Event.Type != llm.EventProgress || events[1].Event.Type != llm.EventFinal {
		t.Fatalf("wire events = %+v", events)
	}

	state := waitForState(t, listener.URL, func(state map[string]any) bool {
		conversations, _ := state["conversations"].([]any)
		if len(conversations) != 1 {
			return false
		}
		conversation, _ := conversations[0].(map[string]any)
		return conversation["phase"] == "terminal"
	})
	_ = state

	// Terminal conversation maps to 409 on further commands.
	response := doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/reply",
		map[string]string{"caller": "caller-a", "task_id": "task-1", "text": "late"}), http.StatusConflict)
	if response["error"] != "conflict" {
		t.Fatalf("terminal reply error = %v", response)
	}
}

func TestWebCommandErrorMapping(t *testing.T) {
	_, _, listener := openWebServer(t)

	notFound := doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/accept",
		map[string]string{"delivery": "missing"}), http.StatusNotFound)
	if notFound["error"] != "not_found" {
		t.Fatalf("accept missing = %v", notFound)
	}
	unknown := doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/final",
		map[string]string{"caller": "caller-x", "task_id": "task-x", "text": "hi"}), http.StatusNotFound)
	if unknown["error"] != "not_found" {
		t.Fatalf("final unknown = %v", unknown)
	}
	invalid := doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/accept",
		map[string]string{"unexpected": "field"}), http.StatusBadRequest)
	if invalid["error"] != "invalid_body" {
		t.Fatalf("invalid body = %v", invalid)
	}
}

func TestWebEventsStreamPushesStateUpdates(t *testing.T) {
	wire, _, listener := openWebServer(t)

	request := authedRequest(t, http.MethodGet, listener.URL+"/api/events", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK ||
		!strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("events response = %d %q", response.StatusCode, response.Header.Get("Content-Type"))
	}

	reader := bufio.NewReader(response.Body)
	readEvent := func() string {
		var data strings.Builder
		deadline := time.AfterFunc(5*time.Second, func() { response.Body.Close() })
		defer deadline.Stop()
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("read SSE: %v (got %q)", err, data.String())
			}
			line = strings.TrimRight(line, "\n")
			if after, found := strings.CutPrefix(line, "data: "); found {
				data.WriteString(after)
			}
			if line == "" && data.Len() > 0 {
				return data.String()
			}
		}
	}

	initial := readEvent()
	if !strings.Contains(initial, `"inbox":[]`) {
		t.Fatalf("initial SSE state = %q", initial)
	}
	wire.assignments <- chatAssignment("task-sse", "delivery-sse", "hello sse")
	for attempt := 0; attempt < 5; attempt++ {
		update := readEvent()
		if strings.Contains(update, "delivery-sse") {
			return
		}
	}
	t.Fatal("SSE never delivered the new assignment")
}

func TestWebToolCallsRoundTrip(t *testing.T) {
	wire, _, listener := openWebServer(t)
	assignment := chatAssignment("task-1", "delivery-1", "run something")
	assignment.Assignment.Task = llm.TaskContext{
		TaskID: "task-1", CapabilityTier: llm.TierWorkspace, WorkspaceKey: "workspace-a",
		HarnessID: "harness-a", HarnessVersion: "v1", HarnessSessionID: "session-a",
		WorkspaceRoot: "/workspace",
	}
	assignment.Assignment.Request.Tools = []llm.Tool{{Name: "bash", InputSchema: []byte(`{"type":"object"}`)}}
	wire.assignments <- assignment

	waitForState(t, listener.URL, func(state map[string]any) bool {
		inbox, _ := state["inbox"].([]any)
		return len(inbox) == 1
	})
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/accept",
		map[string]string{"delivery": "delivery-1"}), http.StatusOK)
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/tool-calls", map[string]any{
		"caller": "caller-a", "task_id": "task-1",
		"calls": []map[string]any{{"id": "call-1", "name": "bash", "input": map[string]any{"command": "ls"}}},
	}), http.StatusOK)

	events := wire.sentEvents()
	if len(events) != 1 || events[0].Event.Type != llm.EventToolCalls ||
		len(events[0].Event.ToolCalls) != 1 || events[0].Event.ToolCalls[0].ID != "call-1" {
		t.Fatalf("tool-calls wire = %+v", events)
	}
	waitForState(t, listener.URL, func(state map[string]any) bool {
		conversations, _ := state["conversations"].([]any)
		if len(conversations) != 1 {
			return false
		}
		conversation, _ := conversations[0].(map[string]any)
		return conversation["phase"] == "awaiting_results"
	})
	_ = fmt.Sprint()
}
