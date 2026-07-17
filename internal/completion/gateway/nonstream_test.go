package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/anthropic"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/dialect/responses"
	"github.com/vibe-agi/human/internal/completion/hub"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

type nonStreamingDialectCase struct {
	name string
	path string
	body func(mode string) []byte
}

func nonStreamingDialects() []nonStreamingDialectCase {
	tool := `,"tools":[{"type":"function","function":{"name":"read_file","parameters":{"type":"object"}}}]`
	return []nonStreamingDialectCase{
		{
			name: "openai_chat", path: "/v1/chat/completions",
			body: func(mode string) []byte {
				extra := ""
				if mode == "tool" {
					extra = tool
				}
				return []byte(`{"model":"human-expert","stream":false,"messages":[{"role":"user","content":"hello"}]` + extra + `}`)
			},
		},
		{
			name: "anthropic", path: "/v1/messages",
			body: func(mode string) []byte {
				extra := ""
				if mode == "tool" {
					extra = `,"tools":[{"name":"read_file","input_schema":{"type":"object"}}]`
				}
				return []byte(`{"model":"human-expert","max_tokens":256,"stream":false,"messages":[{"role":"user","content":"hello"}]` + extra + `}`)
			},
		},
		{
			name: "responses", path: "/v1/responses",
			body: func(mode string) []byte {
				extra := ""
				if mode == "tool" {
					extra = `,"tools":[{"type":"function","name":"read_file","parameters":{"type":"object"}}]`
				}
				return []byte(`{"model":"human-expert","stream":false,"input":"hello"` + extra + `}`)
			},
		},
	}
}

type nonStreamingFixture struct {
	db      *sqlite.Store
	hub     *hub.Hub
	worker  *hub.Worker
	gateway *Server
	server  *httptest.Server
	cancel  context.CancelFunc
}

func newNonStreamingFixture(t *testing.T) *nonStreamingFixture {
	t.Helper()
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "non-stream.db"))
	if err != nil {
		t.Fatal(err)
	}
	workerHub := hub.New(8)
	gateway, err := NewServer(Config{
		HeartbeatInterval: 2 * time.Millisecond, MaxPending: time.Second,
	}, database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
		"/v1/messages":         anthropic.New(),
		"/v1/responses":        responses.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancel := context.WithCancel(context.Background())
	if err := gateway.Recover(runContext); err != nil {
		cancel()
		t.Fatal(err)
	}
	worker, err := workerHub.Register("worker-aggregate")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &nonStreamingFixture{
		db: database, hub: workerHub, worker: worker, gateway: gateway,
		server: httptest.NewServer(gateway), cancel: cancel,
	}
	t.Cleanup(func() {
		fixture.server.Close()
		fixture.cancel()
		fixture.worker.Close()
		fixture.gateway.Wait()
		_ = fixture.db.Close()
	})
	return fixture
}

type nonStreamingHTTPResult struct {
	status      int
	contentType string
	retryAfter  string
	body        []byte
	err         error
}

type cancelAfterAggregateDecisionStore struct {
	storeapi.CompletionStore
	cancel context.CancelFunc
}

func (store *cancelAfterAggregateDecisionStore) CompleteNonStreamingResponse(
	ctx context.Context,
	key storeapi.RequestKey,
	decision storeapi.ResponseDecision,
) (storeapi.Request, error) {
	request, err := store.CompletionStore.CompleteNonStreamingResponse(ctx, key, decision)
	if err == nil {
		// Model process loss in the exact gap after the HTTP decision/body is
		// durable but before persistAndApplyEvent can record the worker receipt.
		store.cancel()
	}
	return request, err
}

func TestNonStreamingTextToolErrorAndExactReplay(t *testing.T) {
	for _, dialectCase := range nonStreamingDialects() {
		dialectCase := dialectCase
		for _, mode := range []string{"text", "empty", "tool", "error"} {
			mode := mode
			t.Run(dialectCase.name+"/"+mode, func(t *testing.T) {
				fixture := newNonStreamingFixture(t)
				body := dialectCase.body(mode)
				key := "aggregate-" + dialectCase.name + "-" + mode
				firstResult := make(chan nonStreamingHTTPResult, 1)
				go func() {
					firstResult <- postNonStreaming(fixture.server.URL+dialectCase.path, key, body)
				}()

				var assignment completion.Assignment
				select {
				case assignment = <-fixture.worker.Assignments:
				case <-time.After(time.Second):
					t.Fatal("non-streaming request was not dispatched")
				}
				if assignment.Request.Stream {
					t.Fatalf("assignment lost non-streaming mode: %+v", assignment.Request)
				}
				digest, err := assignment.Request.Digest()
				if err != nil {
					t.Fatal(err)
				}
				lookup, err := fixture.db.LookupRequest(context.Background(), storeapi.RequestKey{
					CallerID: assignment.CallerID, IdempotencyKey: key,
				}, digest)
				if err != nil || lookup.Request.Response.StatusCode != 0 || lookup.Request.ResponseComplete {
					t.Fatalf("HTTP decision became visible before terminal event: %+v, %v", lookup.Request, err)
				}
				if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
					ID: "accepted-" + key, Type: completion.EventAccepted, WorkerID: "worker-aggregate",
				}); err != nil {
					t.Fatal(err)
				}
				if mode == "text" {
					if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
						ID: "progress-" + key, Type: completion.EventProgress, Text: "hello ",
					}); err != nil {
						t.Fatal(err)
					}
				}
				terminal := completion.Event{ID: "terminal-" + key}
				switch mode {
				case "text":
					terminal.Type, terminal.Text = completion.EventFinal, "world"
				case "empty":
					terminal.Type = completion.EventFinal
				case "tool":
					terminal.Type = completion.EventToolCalls
					terminal.ToolCalls = []completion.ToolCall{{
						ID: "call_1", Name: "read_file", Input: map[string]any{"path": "README.md"},
					}}
				case "error":
					terminal.Type, terminal.ErrorCode, terminal.Error = completion.EventRejected, "human_rejected", "not now"
				}
				if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, terminal); err != nil {
					t.Fatal(err)
				}

				var first nonStreamingHTTPResult
				select {
				case first = <-firstResult:
				case <-time.After(time.Second):
					t.Fatal("non-streaming HTTP response did not finish")
				}
				if first.err != nil {
					t.Fatal(first.err)
				}
				wantStatus := http.StatusOK
				if mode == "error" {
					wantStatus = http.StatusConflict
				}
				if first.status != wantStatus || first.contentType != "application/json" ||
					bytes.Contains(first.body, []byte("data:")) || bytes.Contains(first.body, []byte(": ping")) {
					t.Fatalf("aggregate HTTP response = status %d type %q body %q", first.status, first.contentType, first.body)
				}
				assertNonStreamingBody(t, dialectCase.name, mode, first.body)

				replay := postNonStreaming(fixture.server.URL+dialectCase.path, key, body)
				if replay.err != nil || replay.status != first.status || replay.contentType != first.contentType ||
					replay.retryAfter != first.retryAfter || !bytes.Equal(replay.body, first.body) {
					t.Fatalf("exact replay changed:\nfirst=%+v %q\nreplay=%+v %q", first, first.body, replay, replay.body)
				}
				select {
				case duplicate := <-fixture.worker.Assignments:
					t.Fatalf("idempotent replay dispatched again: %+v", duplicate)
				case <-time.After(10 * time.Millisecond):
				}
			})
		}
	}
}

func postNonStreaming(url, key string, body []byte) nonStreamingHTTPResult {
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nonStreamingHTTPResult{err: err}
	}
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(headerIdempotencyKey, key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nonStreamingHTTPResult{err: err}
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	return nonStreamingHTTPResult{
		status: response.StatusCode, contentType: response.Header.Get("Content-Type"),
		retryAfter: response.Header.Get("Retry-After"), body: payload, err: err,
	}
}

func assertNonStreamingBody(t *testing.T, dialectName, mode string, payload []byte) {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("invalid non-streaming JSON %q: %v", payload, err)
	}
	if mode == "error" {
		if _, ok := body["error"].(map[string]any); !ok {
			t.Fatalf("error body = %#v", body)
		}
		return
	}
	switch dialectName {
	case "openai_chat":
		choice := body["choices"].([]any)[0].(map[string]any)
		message := choice["message"].(map[string]any)
		if mode == "tool" {
			if len(message["tool_calls"].([]any)) != 1 {
				t.Fatalf("Chat tool body = %#v", body)
			}
		} else {
			want := ""
			if mode == "text" {
				want = "hello world"
			}
			if message["content"] != want {
				t.Fatalf("Chat text body = %#v", body)
			}
		}
	case "anthropic":
		block := body["content"].([]any)[0].(map[string]any)
		wantType := "text"
		if mode == "tool" {
			wantType = "tool_use"
		}
		if block["type"] != wantType {
			t.Fatalf("Anthropic body = %#v", body)
		}
	case "responses":
		output := body["output"].([]any)
		if len(output) != 1 {
			t.Fatalf("Responses output = %#v", body)
		}
		wantType := "message"
		if mode == "tool" {
			wantType = "function_call"
		}
		if output[0].(map[string]any)["type"] != wantType {
			t.Fatalf("Responses body = %#v", body)
		}
	default:
		t.Fatal(fmt.Sprintf("unknown dialect %q", dialectName))
	}
}

func TestNonStreamingTerminalStatusMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		event completion.Event
		want  int
	}{
		{completion.Event{Type: completion.EventFinal}, http.StatusOK},
		{completion.Event{Type: completion.EventClarification}, http.StatusOK},
		{completion.Event{Type: completion.EventToolCalls}, http.StatusOK},
		{completion.Event{Type: completion.EventRejected}, http.StatusConflict},
		{completion.Event{Type: completion.EventExpired}, http.StatusGatewayTimeout},
		{completion.Event{Type: completion.EventUnavailable}, http.StatusServiceUnavailable},
		{completion.Event{Type: completion.EventFailed}, http.StatusInternalServerError},
	}
	for _, test := range tests {
		decision, err := nonStreamingDecision(canonical.DialectOpenAIChat, test.event, []byte(`{}`))
		if err != nil || decision.StatusCode != test.want {
			t.Fatalf("event %s = status %d, err %v", test.event.Type, decision.StatusCode, err)
		}
	}
	anthropicUnavailable, err := nonStreamingDecision(canonical.DialectAnthropic, completion.Event{
		Type: completion.EventUnavailable,
	}, []byte(`{}`))
	if err != nil || anthropicUnavailable.StatusCode != 529 {
		t.Fatalf("Anthropic unavailable = status %d, err %v", anthropicUnavailable.StatusCode, err)
	}
}

func TestNonStreamingDoesNotRequireHTTPFlusher(t *testing.T) {
	fixture := newGatewayFixture(t, true)
	body := []byte(`{"model":"human-expert","stream":false,"messages":[{"role":"user","content":"plain writer"}]}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(headerIdempotencyKey, "aggregate-no-flusher")
	recorder := &nonFlushingRecorder{}
	done := make(chan struct{})
	go func() {
		fixture.gateway.ServeHTTP(recorder, request)
		close(done)
	}()
	assignment := <-fixture.worker.Assignments
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "accepted-no-flusher", Type: completion.EventAccepted, WorkerID: "worker-1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "final-no-flusher", Type: completion.EventFinal, Text: "works",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("aggregate response did not finish on a plain ResponseWriter")
	}
	if recorder.status != http.StatusOK || recorder.Header().Get("Content-Type") != "application/json" ||
		!bytes.Contains(recorder.body.Bytes(), []byte("works")) {
		t.Fatalf("plain writer aggregate = status %d headers %v body %q", recorder.status, recorder.Header(), recorder.body.Bytes())
	}
}

func TestConcurrentNonStreamingReplayWaitsForOneTerminalDecision(t *testing.T) {
	fixture := newNonStreamingFixture(t)
	body := []byte(`{"model":"human-expert","stream":false,"messages":[{"role":"user","content":"one decision"}]}`)
	const key = "aggregate-concurrent-replay"
	results := make(chan nonStreamingHTTPResult, 2)
	for range 2 {
		go func() {
			results <- postNonStreaming(fixture.server.URL+"/v1/chat/completions", key, body)
		}()
	}
	var assignment completion.Assignment
	select {
	case assignment = <-fixture.worker.Assignments:
	case <-time.After(time.Second):
		t.Fatal("aggregate request was not dispatched")
	}
	select {
	case duplicate := <-fixture.worker.Assignments:
		t.Fatalf("concurrent duplicate was dispatched: %+v", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case early := <-results:
		t.Fatalf("aggregate response became visible before terminal decision: %+v", early)
	case <-time.After(20 * time.Millisecond):
	}
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
		ID: "accepted-concurrent", Type: completion.EventAccepted, WorkerID: "worker-aggregate",
	}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
		ID: "final-concurrent", Type: completion.EventFinal, Text: "same bytes",
	}); err != nil {
		t.Fatal(err)
	}
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.status != http.StatusOK || second.status != http.StatusOK ||
		!bytes.Equal(first.body, second.body) {
		t.Fatalf("concurrent aggregate results differ: first=%+v %q second=%+v %q", first, first.body, second, second.body)
	}
}

func TestNonStreamingRecoveryAfterDecisionBeforeTerminalReceipt(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "aggregate-recovery.db")
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	runContext, cancelRun := context.WithCancel(context.Background())
	faults := &cancelAfterAggregateDecisionStore{CompletionStore: database, cancel: cancelRun}
	firstHub := hub.New(4)
	firstGateway, err := NewServer(Config{MaxPending: time.Second}, faults, fixedAuthenticator{}, firstHub,
		adapter.NewRegistry(), map[string]dialect.Codec{"/v1/chat/completions": openai.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := firstGateway.Recover(runContext); err != nil {
		t.Fatal(err)
	}
	firstWorker, err := firstHub.Register("worker-before-crash")
	if err != nil {
		t.Fatal(err)
	}
	firstHTTP := httptest.NewServer(firstGateway)
	body := []byte(`{"model":"human-expert","stream":false,"messages":[{"role":"user","content":"survive"}]}`)
	const key = "aggregate-decision-crash"
	resultChannel := make(chan nonStreamingHTTPResult, 1)
	go func() {
		resultChannel <- postNonStreaming(firstHTTP.URL+"/v1/chat/completions", key, body)
	}()
	assignment := <-firstWorker.Assignments
	accepted := completion.Event{
		ID: "aggregate-accepted", Type: completion.EventAccepted, WorkerID: "worker-before-crash",
	}
	if err := firstHub.Publish(context.Background(), assignment.CallerID, key, accepted); err != nil {
		t.Fatal(err)
	}
	terminal := completion.Event{ID: "aggregate-final", Type: completion.EventFinal, Text: "durable result"}
	publishDone := make(chan error, 1)
	go func() {
		publishDone <- firstHub.Publish(context.Background(), assignment.CallerID, key, terminal)
	}()
	first := <-resultChannel
	select {
	case err := <-publishDone:
		if err == nil {
			t.Fatal("terminal publisher unexpectedly received a receipt before simulated crash")
		}
	case <-time.After(time.Second):
		t.Fatal("terminal publisher did not observe simulated process loss")
	}
	terminalDigest, err := workerEventDigest(terminal)
	if err != nil {
		t.Fatal(err)
	}
	requestKey := storeapi.RequestKey{CallerID: assignment.CallerID, IdempotencyKey: key}
	requestDigest, err := assignment.Request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	durable, err := database.LookupRequest(context.Background(), requestKey, requestDigest)
	if err != nil || !durable.Request.ResponseComplete || durable.Request.Response.StatusCode != http.StatusOK ||
		!bytes.Contains(durable.Request.Response.Body, []byte("durable result")) {
		t.Fatalf("durable aggregate decision = %+v, %v", durable.Request, err)
	}
	// The decision was already durable when runtime cancellation raced the HTTP
	// waiter. A fresh short read must recover those exact bytes; returning the
	// handler normally here would synthesize an unrelated empty 200.
	if first.err != nil || first.status != durable.Request.Response.StatusCode ||
		first.contentType != durable.Request.Response.ContentType ||
		first.retryAfter != durable.Request.Response.RetryAfter ||
		!bytes.Equal(first.body, durable.Request.Response.Body) {
		t.Fatalf("decision/cancellation response = %+v %q; durable = %+v",
			first, first.body, durable.Request.Response)
	}
	if _, err := database.LookupWorkerEventReceipt(context.Background(), requestKey, terminal.ID); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("terminal receipt before recovery = %v", err)
	}
	firstHTTP.Close()
	firstWorker.Close()
	firstGateway.Wait()

	recoveredHub := hub.New(4)
	recoveredGateway, err := NewServer(Config{MaxPending: time.Second}, database, fixedAuthenticator{}, recoveredHub,
		adapter.NewRegistry(), map[string]dialect.Codec{"/v1/chat/completions": openai.New()})
	if err != nil {
		t.Fatal(err)
	}
	recoveryContext, cancelRecovery := context.WithCancel(context.Background())
	defer func() {
		cancelRecovery()
		recoveredGateway.Wait()
	}()
	if err := recoveredGateway.Recover(recoveryContext); err != nil {
		t.Fatalf("recover aggregate receipt gap: %v", err)
	}
	receipt, err := database.LookupWorkerEventReceipt(context.Background(), requestKey, terminal.ID)
	if err != nil || receipt.Digest != terminalDigest {
		t.Fatalf("terminal receipt after recovery = %+v, %v", receipt, err)
	}
	recoveredHTTP := httptest.NewServer(recoveredGateway)
	defer recoveredHTTP.Close()
	replay := postNonStreaming(recoveredHTTP.URL+"/v1/chat/completions", key, body)
	if replay.err != nil || replay.status != durable.Request.Response.StatusCode ||
		replay.contentType != durable.Request.Response.ContentType ||
		replay.retryAfter != durable.Request.Response.RetryAfter ||
		!bytes.Equal(replay.body, durable.Request.Response.Body) {
		t.Fatalf("recovered replay changed durable decision: durable=%+v replay=%+v %q",
			durable.Request.Response, replay, replay.body)
	}
	recoveredWorker, err := recoveredHub.Register("worker-after-crash")
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredWorker.Close()
	select {
	case duplicate := <-recoveredWorker.Assignments:
		t.Fatalf("completed aggregate was redispatched after recovery: %+v", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestNonStreamingRuntimeStopAbortsUndecidedHTTPResponse(t *testing.T) {
	fixture := newNonStreamingFixture(t)
	result := make(chan nonStreamingHTTPResult, 1)
	go func() {
		result <- postNonStreaming(
			fixture.server.URL+"/v1/chat/completions",
			"aggregate-undecided-shutdown",
			[]byte(`{"model":"human-expert","stream":false,"messages":[{"role":"user","content":"wait"}]}`),
		)
	}()
	select {
	case <-fixture.worker.Assignments:
	case <-time.After(time.Second):
		t.Fatal("non-streaming request was not dispatched")
	}
	fixture.cancel()
	select {
	case stopped := <-result:
		if stopped.err == nil {
			t.Fatalf("undecided runtime stop returned HTTP %d, %q; want transport abort",
				stopped.status, stopped.body)
		}
	case <-time.After(time.Second):
		t.Fatal("undecided HTTP response did not abort after runtime stop")
	}
}
