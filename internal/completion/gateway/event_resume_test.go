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
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/anthropic"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/dialect/responses"
	"github.com/vibe-agi/human/internal/completion/hub"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

const (
	faultStep     = "step"
	faultApplied  = "applied"
	faultReceipt  = "receipt"
	faultComplete = "complete"
)

var errInjectedWorkerEventStage = errors.New("injected worker event stage failure")

type transientWorkerEventStore struct {
	storeapi.CompletionStore

	mu         sync.Mutex
	stage      string
	eventID    string
	remaining  int
	failures   int
	failed     chan struct{}
	failedOnce sync.Once
}

type transientHeartbeatStore struct {
	storeapi.CompletionStore
	once   sync.Once
	failed chan struct{}
}

func (store *transientHeartbeatStore) AppendResponseEvent(
	ctx context.Context,
	key storeapi.RequestKey,
	kind string,
	data []byte,
) (storeapi.ResponseEvent, error) {
	failed := false
	if kind == responseEventWire && bytes.Equal(data, []byte(": ping\n\n")) {
		store.once.Do(func() {
			failed = true
			close(store.failed)
		})
	}
	if failed {
		return storeapi.ResponseEvent{}, errInjectedWorkerEventStage
	}
	return store.CompletionStore.AppendResponseEvent(ctx, key, kind, data)
}

func (store *transientWorkerEventStore) fail(stage, eventID string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.remaining == 0 || store.stage != stage || store.eventID != "" && store.eventID != eventID {
		return false
	}
	if store.remaining > 0 {
		store.remaining--
	}
	store.failures++
	if store.failed != nil {
		store.failedOnce.Do(func() { close(store.failed) })
	}
	return true
}

func (store *transientWorkerEventStore) failureCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.failures
}

func (store *transientWorkerEventStore) allowSuccess() {
	store.mu.Lock()
	store.remaining = 0
	store.mu.Unlock()
}

func (store *transientWorkerEventStore) AppendWorkerResponseEvent(
	ctx context.Context,
	key storeapi.RequestKey,
	kind string,
	eventID string,
	digest string,
	data []byte,
) (storeapi.ResponseEvent, error) {
	if store.fail(kind, eventID) {
		return storeapi.ResponseEvent{}, errInjectedWorkerEventStage
	}
	return store.CompletionStore.AppendWorkerResponseEvent(ctx, key, kind, eventID, digest, data)
}

func (store *transientWorkerEventStore) RecordWorkerEventReceipt(
	ctx context.Context,
	key storeapi.RequestKey,
	eventID string,
	digest string,
) (storeapi.WorkerEventReceipt, error) {
	if store.fail(faultReceipt, eventID) {
		return storeapi.WorkerEventReceipt{}, errInjectedWorkerEventStage
	}
	return store.CompletionStore.RecordWorkerEventReceipt(ctx, key, eventID, digest)
}

func (store *transientWorkerEventStore) CompleteRequest(ctx context.Context, key storeapi.RequestKey) error {
	if store.fail(faultComplete, "final-retry") {
		return errInjectedWorkerEventStage
	}
	return store.CompletionStore.CompleteRequest(ctx, key)
}

type eventResponse struct {
	status int
	body   []byte
	err    error
}

func startEventRequest(
	t *testing.T,
	ctx context.Context,
	serverURL string,
	path string,
	body []byte,
	key string,
) <-chan eventResponse {
	t.Helper()
	result := make(chan eventResponse, 1)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set(headerIdempotencyKey, key)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			result <- eventResponse{err: err}
			return
		}
		data, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		result <- eventResponse{status: response.StatusCode, body: data, err: readErr}
	}()
	return result
}

func waitAssignment(t *testing.T, worker *hub.Worker) completion.Assignment {
	t.Helper()
	select {
	case assignment := <-worker.Assignments:
		return assignment
	case <-time.After(time.Second):
		t.Fatal("worker did not receive completion assignment")
		return completion.Assignment{}
	}
}

func waitEventResponse(t *testing.T, result <-chan eventResponse) eventResponse {
	t.Helper()
	select {
	case response := <-result:
		return response
	case <-time.After(3 * time.Second):
		t.Fatal("completion response did not terminate")
		return eventResponse{}
	}
}

func assertSingleWorkerEventStages(
	t *testing.T,
	database *sqlite.Store,
	key storeapi.RequestKey,
	eventID string,
) {
	t.Helper()
	events, err := database.ListResponseEvents(context.Background(), key, 0)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, event := range events {
		if event.EventID == eventID {
			counts[event.Kind]++
		}
	}
	if counts[responseEventStep] != 1 || counts[responseEventApplied] != 1 {
		t.Fatalf("worker event %q stages = %v; all events = %+v", eventID, counts, events)
	}
}

func TestWorkerEventStagesResumeOnlineAfterTransientFailure(t *testing.T) {
	for _, stage := range []string{faultApplied, faultReceipt, faultComplete} {
		stage := stage
		t.Run(stage, func(t *testing.T) {
			database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "online.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			faults := &transientWorkerEventStore{
				CompletionStore: database, stage: stage, eventID: "final-retry",
				remaining: -1, failed: make(chan struct{}),
			}
			workerHub := hub.New(2)
			worker, err := workerHub.Register("worker-online")
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			gateway, err := NewServer(Config{
				// The retry backoff crosses several heartbeat ticks. A terminal
				// stage must settle before the session select can observe them.
				HeartbeatInterval: 2 * time.Millisecond, MaxPending: time.Second,
			}, faults, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
				"/v1/chat/completions": openai.New(),
			})
			if err != nil {
				t.Fatal(err)
			}
			runContext, cancelRun := context.WithCancel(context.Background())
			defer cancelRun()
			if err := gateway.Recover(runContext); err != nil {
				t.Fatal(err)
			}
			httpServer := httptest.NewServer(gateway)
			defer httpServer.Close()
			body := chatBody("resume every stage", false)
			result := startEventRequest(t, context.Background(), httpServer.URL, "/v1/chat/completions", body, "event-stage-online")
			assignment := waitAssignment(t, worker)
			if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
				ID: "accepted-online", Type: completion.EventAccepted, WorkerID: worker.ID,
			}); err != nil {
				t.Fatal(err)
			}
			final := completion.Event{ID: "final-retry", Type: completion.EventFinal, Text: "exactly once"}
			publishResult := make(chan error, 1)
			go func() {
				publishResult <- workerHub.Publish(
					context.Background(), assignment.CallerID, assignment.IdempotencyKey, final,
				)
			}()
			select {
			case <-faults.failed:
			case <-time.After(time.Second):
				t.Fatalf("%s online stage fault was not reached", stage)
			}
			// Keep the stage pending across heartbeat ticks, then restore the
			// store without asking the worker to publish the event a second time.
			time.Sleep(15 * time.Millisecond)
			faults.allowSuccess()
			select {
			case err := <-publishResult:
				if err != nil {
					t.Fatalf("online stage resumption after %s failure: %v", stage, err)
				}
			case <-time.After(time.Second):
				t.Fatalf("online stage resumption after %s did not ACK", stage)
			}
			response := waitEventResponse(t, result)
			if response.err != nil || response.status != http.StatusOK ||
				bytes.Count(response.body, []byte(`"content":"exactly once"`)) != 1 ||
				bytes.Count(response.body, []byte("data: [DONE]")) != 1 {
				t.Fatalf("online response after %s = %d, %q, %v", stage, response.status, response.body, response.err)
			}
			key := storeapi.RequestKey{CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey}
			assertSingleWorkerEventStages(t, database, key, final.ID)
			digest, err := workerEventDigest(final)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.LookupWorkerEventReceipt(context.Background(), key, final.ID, digest); err != nil {
				t.Fatalf("terminal receipt after %s = %v", stage, err)
			}
			lookup, err := database.LookupRequest(context.Background(), key, mustRequestDigest(t, body))
			if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateCompleted {
				t.Fatalf("terminal request after %s = %+v, %v", stage, lookup, err)
			}
		})
	}
}

func TestTransientHeartbeatFailureDoesNotAbandonLiveSession(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "heartbeat.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := &transientHeartbeatStore{CompletionStore: database, failed: make(chan struct{})}
	workerHub := hub.New(2)
	worker, err := workerHub.Register("worker-heartbeat")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	gateway, err := NewServer(Config{
		HeartbeatInterval: 2 * time.Millisecond, MaxPending: time.Second,
	}, store, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	if err := gateway.Recover(runContext); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(gateway)
	defer httpServer.Close()
	body := chatBody("survive heartbeat store failure", false)
	result := startEventRequest(
		t, context.Background(), httpServer.URL, "/v1/chat/completions", body, "heartbeat-retry",
	)
	assignment := waitAssignment(t, worker)
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "heartbeat-accepted", Type: completion.EventAccepted, WorkerID: worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.failed:
	case <-time.After(time.Second):
		t.Fatal("heartbeat failure was not injected")
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "heartbeat-final", Type: completion.EventFinal, Text: "consumer remained live",
	}); err != nil {
		t.Fatalf("final event after heartbeat failure: %v", err)
	}
	response := waitEventResponse(t, result)
	if response.err != nil || response.status != http.StatusOK ||
		!bytes.Contains(response.body, []byte("consumer remained live")) ||
		!bytes.Contains(response.body, []byte("data: [DONE]")) {
		t.Fatalf("response after heartbeat failure = %d, %q, %v", response.status, response.body, response.err)
	}
}

func TestSyntheticExpiryRetriesTransientStepWithoutWorkerOutbox(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "expiry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	faults := &transientWorkerEventStore{
		CompletionStore: database, stage: faultStep, remaining: 1,
	}
	workerHub := hub.New(2)
	worker, err := workerHub.Register("worker-expiry")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour, MaxPending: 25 * time.Millisecond,
	}, faults, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	if err := gateway.Recover(runContext); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(gateway)
	defer httpServer.Close()
	body := chatBody("expire without an outbox event", false)
	result := startEventRequest(
		t, context.Background(), httpServer.URL, "/v1/chat/completions", body, "synthetic-expiry",
	)
	assignment := waitAssignment(t, worker)
	response := waitEventResponse(t, result)
	if response.err != nil || response.status != http.StatusOK ||
		!bytes.Contains(response.body, []byte("human_timeout")) ||
		!bytes.Contains(response.body, []byte("data: [DONE]")) {
		t.Fatalf("synthetic expiry response = %d, %q, %v", response.status, response.body, response.err)
	}
	if failures := faults.failureCount(); failures != 1 {
		t.Fatalf("synthetic expiry step failures = %d, want one", failures)
	}
	key := storeapi.RequestKey{CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey}
	lookup, err := database.LookupRequest(context.Background(), key, mustRequestDigest(t, body))
	if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateExpired {
		t.Fatalf("synthetic expiry durable state = %+v, %v", lookup, err)
	}
	events, err := database.ListResponseEvents(context.Background(), key, 0)
	if err != nil {
		t.Fatal(err)
	}
	var terminal completion.Event
	for _, stored := range events {
		if stored.Kind != responseEventStep {
			continue
		}
		var step persistedStep
		if err := json.Unmarshal(stored.Data, &step); err != nil {
			t.Fatal(err)
		}
		if step.Event.Type == completion.EventExpired {
			terminal = step.Event
		}
	}
	if terminal.ID == "" {
		t.Fatalf("synthetic expiry has no durable terminal event: %+v", events)
	}
	digest, err := workerEventDigest(terminal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.LookupWorkerEventReceipt(context.Background(), key, terminal.ID, digest); err != nil {
		t.Fatalf("synthetic expiry receipt = %v", err)
	}
}

func TestResponsesStepWriteFailureReusesFirstStatefulEncoding(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "responses.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	faults := &transientWorkerEventStore{
		CompletionStore: database, stage: faultStep, eventID: "responses-final", remaining: 1,
	}
	workerHub := hub.New(2)
	worker, err := workerHub.Register("worker-responses")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour, MaxPending: time.Second,
	}, faults, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/responses": responses.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	if err := gateway.Recover(runContext); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(gateway)
	defer httpServer.Close()
	body := []byte(`{"model":"human-expert","stream":true,"input":"preserve sequence"}`)
	result := startEventRequest(t, context.Background(), httpServer.URL, "/v1/responses", body, "responses-step-retry")
	assignment := waitAssignment(t, worker)
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "responses-accepted", Type: completion.EventAccepted, WorkerID: worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	final := completion.Event{ID: "responses-final", Type: completion.EventFinal, Text: "stable sequence"}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, final); err != nil {
		t.Fatalf("Responses step resumer re-encoded mutated stream: %v", err)
	}
	response := waitEventResponse(t, result)
	if response.err != nil || response.status != http.StatusOK {
		t.Fatalf("Responses retry = %d, %q, %v", response.status, response.body, response.err)
	}
	for sequence := 0; sequence <= 2; sequence++ {
		needle := []byte(fmt.Sprintf(`"sequence_number":%d`, sequence))
		if count := bytes.Count(response.body, needle); count != 1 {
			t.Fatalf("Responses sequence %d count = %d, body = %q", sequence, count, response.body)
		}
	}
	if strings.Contains(string(response.body), `"sequence_number":3`) {
		t.Fatalf("Responses encoder drifted after retry: %q", response.body)
	}
	assertSingleWorkerEventStages(t, database, storeapi.RequestKey{
		CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
	}, final.ID)
}

func TestAnthropicStepWriteFailurePreservesOpenContentBlock(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "anthropic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	faults := &transientWorkerEventStore{
		CompletionStore: database, stage: faultStep, eventID: "anthropic-final", remaining: 1,
	}
	workerHub := hub.New(2)
	worker, err := workerHub.Register("worker-anthropic")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour, MaxPending: time.Second,
	}, faults, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/messages": anthropic.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	if err := gateway.Recover(runContext); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(gateway)
	defer httpServer.Close()
	body := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"preserve block"}]}`)
	result := startEventRequest(t, context.Background(), httpServer.URL, "/v1/messages", body, "anthropic-step-retry")
	assignment := waitAssignment(t, worker)
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "anthropic-accepted", Type: completion.EventAccepted, WorkerID: worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "anthropic-progress", Type: completion.EventProgress, Text: "opening ",
	}); err != nil {
		t.Fatal(err)
	}
	final := completion.Event{ID: "anthropic-final", Type: completion.EventFinal, Text: "finished"}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, final); err != nil {
		t.Fatalf("Anthropic step resumer re-encoded mutated stream: %v", err)
	}
	response := waitEventResponse(t, result)
	if response.err != nil || response.status != http.StatusOK {
		t.Fatalf("Anthropic retry = %d, %q, %v", response.status, response.body, response.err)
	}
	checks := map[string]int{
		`event: content_block_start`: 1,
		`event: content_block_stop`:  1,
		`event: message_stop`:        1,
		`"index":0`:                  4,
	}
	for fragment, expected := range checks {
		if count := strings.Count(string(response.body), fragment); count != expected {
			t.Fatalf("Anthropic fragment %q count = %d, want %d; body = %q", fragment, count, expected, response.body)
		}
	}
	if strings.Contains(string(response.body), `"index":1`) {
		t.Fatalf("Anthropic content block index drifted after retry: %q", response.body)
	}
	assertSingleWorkerEventStages(t, database, storeapi.RequestKey{
		CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
	}, final.ID)
}

func TestRecoverResumesEveryInterruptedWorkerEventStage(t *testing.T) {
	for _, stage := range []string{faultStep, faultApplied, faultReceipt, faultComplete} {
		stage := stage
		t.Run(stage, func(t *testing.T) {
			databasePath := filepath.Join(t.TempDir(), "restart.db")
			database, err := sqlite.Open(context.Background(), databasePath)
			if err != nil {
				t.Fatal(err)
			}
			faults := &transientWorkerEventStore{
				CompletionStore: database, stage: stage, eventID: "final-retry",
				remaining: -1, failed: make(chan struct{}),
			}
			workerHub := hub.New(2)
			worker, err := workerHub.Register("worker-before-restart")
			if err != nil {
				t.Fatal(err)
			}
			gateway, err := NewServer(Config{
				HeartbeatInterval: time.Hour, MaxPending: time.Second,
			}, faults, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
				"/v1/chat/completions": openai.New(),
			})
			if err != nil {
				t.Fatal(err)
			}
			runContext, cancelRun := context.WithCancel(context.Background())
			if err := gateway.Recover(runContext); err != nil {
				t.Fatal(err)
			}
			httpServer := httptest.NewServer(gateway)
			clientContext, cancelClient := context.WithCancel(context.Background())
			body := chatBody("recover interrupted stage", false)
			result := startEventRequest(
				t, clientContext, httpServer.URL, "/v1/chat/completions", body, "event-stage-restart",
			)
			assignment := waitAssignment(t, worker)
			if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
				ID: "accepted-before-restart", Type: completion.EventAccepted, WorkerID: worker.ID,
			}); err != nil {
				t.Fatal(err)
			}
			final := completion.Event{ID: "final-retry", Type: completion.EventFinal, Text: "recovered once"}
			publishResult := make(chan error, 1)
			go func() {
				publishResult <- workerHub.Publish(
					context.Background(), assignment.CallerID, assignment.IdempotencyKey, final,
				)
			}()
			select {
			case <-faults.failed:
			case <-time.After(time.Second):
				t.Fatalf("%s stage fault was not reached", stage)
			}

			cancelRun()
			if err := <-publishResult; !errors.Is(err, context.Canceled) {
				t.Fatalf("publish interrupted at %s = %v", stage, err)
			}
			cancelClient()
			httpServer.Close()
			worker.Close()
			_ = waitEventResponse(t, result)
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}

			database, err = sqlite.Open(context.Background(), databasePath)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			restartedHub := hub.New(2)
			restarted, err := NewServer(Config{}, database, fixedAuthenticator{}, restartedHub, adapter.NewRegistry(), map[string]dialect.Codec{
				"/v1/chat/completions": openai.New(),
			})
			if err != nil {
				t.Fatal(err)
			}
			recoveryContext, cancelRecovery := context.WithCancel(context.Background())
			defer cancelRecovery()
			if err := restarted.Recover(recoveryContext); err != nil {
				t.Fatalf("Recover() after %s failure: %v", stage, err)
			}
			if stage == faultStep {
				// The failed step never reached SQLite, so recovery restores the
				// sticky assignment and the worker outbox supplies the same event.
				recoveredWorker, err := restartedHub.Register("worker-before-restart")
				if err != nil {
					t.Fatal(err)
				}
				defer recoveredWorker.Close()
				recoveredAssignment := waitAssignment(t, recoveredWorker)
				if recoveredAssignment.IdempotencyKey != assignment.IdempotencyKey {
					t.Fatalf("recovered assignment = %+v", recoveredAssignment)
				}
				if err := restartedHub.Publish(
					context.Background(), recoveredAssignment.CallerID, recoveredAssignment.IdempotencyKey, final,
				); err != nil {
					t.Fatalf("outbox replay after step failure: %v", err)
				}
			}

			key := storeapi.RequestKey{CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey}
			lookup, err := database.LookupRequest(context.Background(), key, mustRequestDigest(t, body))
			if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateCompleted {
				t.Fatalf("recovered request after %s = %+v, %v", stage, lookup, err)
			}
			digest, err := workerEventDigest(final)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.LookupWorkerEventReceipt(context.Background(), key, final.ID, digest); err != nil {
				t.Fatalf("recovered receipt after %s = %v", stage, err)
			}
			assertSingleWorkerEventStages(t, database, key, final.ID)
			recoverable, err := database.ListRecoverableRequests(context.Background())
			if err != nil || len(recoverable) != 0 {
				t.Fatalf("requests still recoverable after %s = %+v, %v", stage, recoverable, err)
			}

			replayServer := httptest.NewServer(restarted)
			defer replayServer.Close()
			replay := startEventRequest(
				t, context.Background(), replayServer.URL, "/v1/chat/completions", body, key.IdempotencyKey,
			)
			response := waitEventResponse(t, replay)
			if response.err != nil || response.status != http.StatusOK ||
				bytes.Count(response.body, []byte(`"content":"recovered once"`)) != 1 ||
				bytes.Count(response.body, []byte("data: [DONE]")) != 1 {
				t.Fatalf("recovered replay after %s = %d, %q, %v", stage, response.status, response.body, response.err)
			}
		})
	}
}

func TestResponsesStepFailureRestartRebuildsPriorStreamState(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "responses-restart.db")
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	faults := &transientWorkerEventStore{
		CompletionStore: database, stage: faultStep, eventID: "responses-final-restart",
		remaining: -1, failed: make(chan struct{}),
	}
	workerHub := hub.New(2)
	worker, err := workerHub.Register("worker-responses-restart")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour, MaxPending: time.Second,
	}, faults, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/responses": responses.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	if err := gateway.Recover(runContext); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(gateway)
	clientContext, cancelClient := context.WithCancel(context.Background())
	body := []byte(`{"model":"human-expert","stream":true,"input":"rebuild sequence"}`)
	result := startEventRequest(
		t, clientContext, httpServer.URL, "/v1/responses", body, "responses-step-restart",
	)
	assignment := waitAssignment(t, worker)
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "responses-accepted-restart", Type: completion.EventAccepted, WorkerID: worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "responses-progress-restart", Type: completion.EventProgress, Text: "before restart ",
	}); err != nil {
		t.Fatal(err)
	}
	final := completion.Event{
		ID: "responses-final-restart", Type: completion.EventFinal, Text: "after restart",
	}
	publishResult := make(chan error, 1)
	go func() {
		publishResult <- workerHub.Publish(
			context.Background(), assignment.CallerID, assignment.IdempotencyKey, final,
		)
	}()
	select {
	case <-faults.failed:
	case <-time.After(time.Second):
		t.Fatal("Responses restart step fault was not reached")
	}
	cancelRun()
	if err := <-publishResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Responses publish interrupted for restart = %v", err)
	}
	cancelClient()
	httpServer.Close()
	worker.Close()
	_ = waitEventResponse(t, result)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	// Force the restarted codec's wall clock into a different second. The
	// durable stream seed—not the new process clock—must drive created_at.
	time.Sleep(1100 * time.Millisecond)

	database, err = sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	restartedHub := hub.New(2)
	restarted, err := NewServer(Config{}, database, fixedAuthenticator{}, restartedHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/responses": responses.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	recoveryContext, cancelRecovery := context.WithCancel(context.Background())
	defer cancelRecovery()
	if err := restarted.Recover(recoveryContext); err != nil {
		t.Fatal(err)
	}
	recoveredWorker, err := restartedHub.Register("worker-responses-restart")
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredWorker.Close()
	recoveredAssignment := waitAssignment(t, recoveredWorker)
	if err := restartedHub.Publish(
		context.Background(), recoveredAssignment.CallerID, recoveredAssignment.IdempotencyKey, final,
	); err != nil {
		t.Fatalf("Responses outbox replay after restart: %v", err)
	}

	replayServer := httptest.NewServer(restarted)
	defer replayServer.Close()
	replay := startEventRequest(
		t, context.Background(), replayServer.URL, "/v1/responses", body, assignment.IdempotencyKey,
	)
	response := waitEventResponse(t, replay)
	if response.err != nil || response.status != http.StatusOK {
		t.Fatalf("Responses restart replay = %d, %q, %v", response.status, response.body, response.err)
	}
	for sequence := 0; sequence <= 3; sequence++ {
		needle := []byte(fmt.Sprintf(`"sequence_number":%d`, sequence))
		if count := bytes.Count(response.body, needle); count != 1 {
			t.Fatalf("Responses restart sequence %d count = %d, body = %q", sequence, count, response.body)
		}
	}
	if strings.Contains(string(response.body), `"sequence_number":4`) {
		t.Fatalf("Responses sequence drifted across restart: %q", response.body)
	}
	createdAt := regexp.MustCompile(`"created_at":([0-9]+)`).FindAllSubmatch(response.body, -1)
	if len(createdAt) != 2 || !bytes.Equal(createdAt[0][1], createdAt[1][1]) {
		t.Fatalf("Responses creation seed changed across restart: %q", response.body)
	}
	assertSingleWorkerEventStages(t, database, storeapi.RequestKey{
		CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
	}, final.ID)
}
