package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/hub"
	"github.com/vibe-agi/human/internal/ratelimit"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

type fixedAuthenticator struct{}

func (fixedAuthenticator) Authenticate(_ context.Context, secret string) (auth.Principal, error) {
	if secret != "hae_test" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return auth.Principal{Type: auth.PrincipalCaller, SubjectID: "caller-1", KeyID: "key-1"}, nil
}

type gatewayFixture struct {
	db       *sqlite.Store
	hub      *hub.Hub
	worker   *hub.Worker
	gateway  *Server
	server   *httptest.Server
	registry *adapter.Registry
}

func newGatewayFixture(t *testing.T, withWorker bool) *gatewayFixture {
	return newGatewayFixtureWithConfig(t, withWorker, Config{})
}

func newGatewayFixtureWithConfig(t *testing.T, withWorker bool, config Config) *gatewayFixture {
	t.Helper()
	ctx := context.Background()
	db, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	workerHub := hub.New(8)
	registry := adapter.NewRegistry()
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 5 * time.Millisecond
	}
	if config.MaxPending == 0 {
		config.MaxPending = time.Second
	}
	server, err := NewServer(config, db, fixedAuthenticator{}, workerHub, registry, map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &gatewayFixture{
		db: db, hub: workerHub, registry: registry,
		gateway: server, server: httptest.NewServer(server),
	}
	if withWorker {
		fixture.worker, err = workerHub.Register("worker-1")
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		fixture.server.Close()
		if fixture.worker != nil {
			fixture.worker.Close()
		}
		_ = db.Close()
	})
	return fixture
}

type nonFlushingRecorder struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (recorder *nonFlushingRecorder) Header() http.Header {
	if recorder.header == nil {
		recorder.header = make(http.Header)
	}
	return recorder.header
}

func (recorder *nonFlushingRecorder) WriteHeader(status int) {
	if recorder.status == 0 {
		recorder.status = status
	}
}

func (recorder *nonFlushingRecorder) Write(data []byte) (int, error) {
	if recorder.status == 0 {
		recorder.status = http.StatusOK
	}
	return recorder.body.Write(data)
}

type flushCallbackRecorder struct {
	*httptest.ResponseRecorder
	once         sync.Once
	onFirstFlush func()
}

func (recorder *flushCallbackRecorder) Flush() {
	recorder.ResponseRecorder.Flush()
	recorder.once.Do(recorder.onFirstFlush)
}

type cancelAfterResponseDecisionStore struct {
	storeapi.CompletionStore
	cancel context.CancelFunc
	once   sync.Once
}

func (store *cancelAfterResponseDecisionStore) BeginResponse(
	ctx context.Context,
	key storeapi.RequestKey,
) (storeapi.Request, error) {
	request, err := store.CompletionStore.BeginResponse(ctx, key)
	if err == nil {
		store.once.Do(store.cancel)
	}
	return request, err
}

type transientResponseListStore struct {
	storeapi.CompletionStore
	mu       sync.Mutex
	failed   bool
	allow    <-chan struct{}
	failure  chan struct{}
	listCall int
}

func (store *transientResponseListStore) ListResponseEvents(
	ctx context.Context,
	key storeapi.RequestKey,
	after int64,
) ([]storeapi.ResponseEvent, error) {
	if store.allow != nil {
		select {
		case <-store.allow:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	store.mu.Lock()
	store.listCall++
	if !store.failed {
		store.failed = true
		close(store.failure)
		store.mu.Unlock()
		return nil, errors.New("injected transient response-list failure")
	}
	store.mu.Unlock()
	return store.CompletionStore.ListResponseEvents(ctx, key, after)
}

func (store *transientResponseListStore) listCalls() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.listCall
}

func testAuditID(callerID, idempotencyKey string) string {
	sum := sha256.Sum256([]byte(callerID + "\x00" + idempotencyKey))
	return "audit_" + hex.EncodeToString(sum[:])
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

func chatBody(text string, withTool bool) []byte {
	tools := ""
	if withTool {
		tools = `,"tools":[{"type":"function","function":{"name":"read_file","parameters":{"type":"object"}}}]`
	}
	return []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"` + text + `"}]` + tools + `}`)
}

func newChatRequest(t *testing.T, fixture *gatewayFixture, body []byte, idempotencyKey string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, fixture.server.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set(headerIdempotencyKey, idempotencyKey)
	}
	return request
}

func setRemoteHeaders(request *http.Request, taskID string) {
	request.Header.Set(headerCapabilityTier, string(completion.TierRemoteTools))
	request.Header.Set(headerWorkspaceKey, "workspace")
	request.Header.Set(headerTaskID, taskID)
	request.Header.Set(headerHarnessID, "known")
	request.Header.Set(headerHarnessVersion, "1")
	request.Header.Set(headerWorkspaceRoot, "/repo")
}

func setHumanShimRemoteHeaders(request *http.Request, taskID, callerID string) {
	request.Header.Set(headerCapabilityTier, string(completion.TierRemoteTools))
	request.Header.Set(headerWorkspaceKey, "workspace")
	request.Header.Set(headerTaskID, taskID)
	request.Header.Set(headerHarnessID, adapter.HumanShimID)
	request.Header.Set(headerHarnessVersion, adapter.HumanShimVersion)
	request.Header.Set(headerWorkspaceRoot, "/repo")
	if callerID != "" {
		request.Header.Set(headerCallerID, callerID)
	}
}

func TestChatCompletionPersistsBeforeDispatchAndReplays(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixture(t, true)
	body := chatBody("diagnose", true)
	assignmentDone := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if len(assignment.Request.Tools) != 0 {
			assignmentDone <- errors.New("Chat tier exposed tools")
			return
		}
		task, err := fixture.db.GetTask(context.Background(), storeapi.TaskKey{
			CallerID: assignment.CallerID, TaskID: assignment.TaskID,
		})
		if err != nil {
			assignmentDone <- err
			return
		}
		if task.State != completion.StateAdmitted {
			assignmentDone <- errors.New("task was dispatched before admitted was durable")
			return
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventAccepted, WorkerID: "worker-1",
		}); err != nil {
			assignmentDone <- err
			return
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventProgress, Text: "checking",
		}); err != nil {
			assignmentDone <- err
			return
		}
		assignmentDone <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventFinal, Text: "fixed",
		})
	}()

	response, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "request-1"))
	if err != nil {
		t.Fatal(err)
	}
	responseBody, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(responseBody), `"content":"fixed"`) || !strings.Contains(string(responseBody), "data: [DONE]") {
		t.Fatalf("status = %d, body = %s", response.StatusCode, responseBody)
	}
	if err := <-assignmentDone; err != nil {
		t.Fatal(err)
	}
	taskID := response.Header.Get(headerTaskID)
	task, err := fixture.db.GetTask(context.Background(), storeapi.TaskKey{CallerID: "caller-1", TaskID: taskID})
	if err != nil {
		t.Fatal(err)
	}
	if task.State != completion.StateCompleted || task.LeaseOwner != "worker-1" {
		t.Fatalf("task = %+v", task)
	}
	auditID := testAuditID("caller-1", "request-1")
	metadata, err := fixture.db.GetAuditMetadata(context.Background(), auditID)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.CallerID != "caller-1" || metadata.TaskID != taskID || metadata.KeyID != "key-1" || metadata.Error != "" {
		t.Fatalf("audit metadata = %+v", metadata)
	}
	if _, err := fixture.db.GetAuditPayload(context.Background(), auditID, "request"); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("default audit retained request payload: %v", err)
	}

	fixture.worker.Close()
	fixture.worker = nil
	replay, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "request-1"))
	if err != nil {
		t.Fatal(err)
	}
	replayBody, err := io.ReadAll(replay.Body)
	replay.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if replay.StatusCode != http.StatusOK || !bytes.Equal(responseBody, replayBody) {
		t.Fatalf("replay status = %d\nfirst: %s\nreplay: %s", replay.StatusCode, responseBody, replayBody)
	}

	conflict, err := http.DefaultClient.Do(newChatRequest(t, fixture, chatBody("different", false), "request-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer conflict.Body.Close()
	if conflict.StatusCode != http.StatusConflict {
		data, _ := io.ReadAll(conflict.Body)
		t.Fatalf("conflict status = %d, body = %s", conflict.StatusCode, data)
	}
}

func TestOpenAIStreamPersistsAndFlushesHeartbeatBeforeFinal(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval: 10 * time.Millisecond, MaxPending: time.Second,
	})
	done := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventAccepted, WorkerID: "worker-1",
		}); err != nil {
			done <- err
			return
		}
		time.Sleep(45 * time.Millisecond)
		done <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventFinal, Text: "after heartbeat",
		})
	}()
	response, err := http.DefaultClient.Do(newChatRequest(t, fixture, chatBody("wait", false), "heartbeat-1"))
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(": ping\n\n")) ||
		!bytes.Contains(body, []byte("after heartbeat")) {
		t.Fatalf("heartbeat stream = %d, %q", response.StatusCode, body)
	}
}

// TestOptInGatewaySoak provides the reproducible gateway half of the P1-M0
// 10m/2h gate. It is intentionally opt-in because the decisive run must still
// use a frozen real harness outside this process. Example:
// HUMAN_GATEWAY_SOAK=2h go test -timeout=2h5m ./internal/completion/gateway -run TestOptInGatewaySoak
func TestOptInGatewaySoak(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv("HUMAN_GATEWAY_SOAK"))
	if raw == "" {
		t.Skip("set HUMAN_GATEWAY_SOAK=10m or 2h for the opt-in soak")
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid HUMAN_GATEWAY_SOAK %q: %v", raw, err)
	}
	heartbeat := 15 * time.Second
	if duration < heartbeat*2 {
		heartbeat = duration / 4
	}
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval: heartbeat, MaxPending: duration + time.Minute,
	})
	done := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventAccepted, WorkerID: "worker-1",
		}); err != nil {
			done <- err
			return
		}
		timer := time.NewTimer(duration)
		defer timer.Stop()
		<-timer.C
		done <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventFinal, Text: "soak complete",
		})
	}()
	response, err := http.DefaultClient.Do(newChatRequest(t, fixture, chatBody("soak", false), "soak-1"))
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(": ping\n\n")) || !bytes.Contains(body, []byte("soak complete")) {
		t.Fatalf("soak stream lost heartbeat/final: %q", body)
	}
}

func TestNewRequestWithoutWorkerFailsBeforeStreaming(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixture(t, false)
	response, err := http.DefaultClient.Do(newChatRequest(t, fixture, chatBody("hello", false), "request-new"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || response.Header.Get("Retry-After") == "" {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d, headers = %v, body = %s", response.StatusCode, response.Header, data)
	}
}

func TestPreStreamFailurePersistsExactHTTPReplay(t *testing.T) {
	fixture := newGatewayFixture(t, true)
	body := chatBody("writer cannot stream", false)
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set(headerIdempotencyKey, "pre-stream-failure")
	first := &nonFlushingRecorder{}
	fixture.gateway.ServeHTTP(first, request)
	firstBody := bytes.Clone(first.body.Bytes())
	if first.status != http.StatusInternalServerError ||
		!bytes.Contains(firstBody, []byte("streaming_unsupported")) ||
		bytes.Contains(firstBody, []byte("data:")) {
		t.Fatalf("first pre-stream response = %d, %q", first.status, firstBody)
	}

	digest := mustRequestDigest(t, body)
	stored, err := fixture.db.LookupRequest(context.Background(), storeapi.RequestKey{
		CallerID: "caller-1", IdempotencyKey: "pre-stream-failure",
	}, digest)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Request.Response.StatusCode != http.StatusInternalServerError ||
		!stored.Request.ResponseComplete || stored.Task.State != completion.StateFailed ||
		!bytes.Equal(stored.Request.Response.Body, firstBody) {
		t.Fatalf("durable pre-stream response = %+v, task = %+v", stored.Request, stored.Task)
	}

	fixture.worker.Close()
	fixture.worker = nil
	replay, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "pre-stream-failure"))
	if err != nil {
		t.Fatal(err)
	}
	replayBody, err := io.ReadAll(replay.Body)
	replay.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if replay.StatusCode != first.status || !bytes.Equal(replayBody, firstBody) ||
		replay.Header.Get("Content-Type") != first.Header().Get("Content-Type") {
		t.Fatalf("pre-stream replay = %d, %q; first = %d, %q", replay.StatusCode, replayBody, first.status, firstBody)
	}

	conflict, err := http.DefaultClient.Do(newChatRequest(
		t, fixture, chatBody("same key, different payload", false), "pre-stream-failure",
	))
	if err != nil {
		t.Fatal(err)
	}
	conflictBody, err := io.ReadAll(conflict.Body)
	conflict.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if conflict.StatusCode != http.StatusConflict || !bytes.Contains(conflictBody, []byte("idempotency_conflict")) {
		t.Fatalf("failed-response conflict = %d, %q", conflict.StatusCode, conflictBody)
	}
}

func TestCommitted200ClientCancellationStillDispatchesAndOnlineReplayCompletes(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "cancel-after-200.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	workerHub := hub.New(2)
	worker, err := workerHub.Register("worker-cancel")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	requestContext, cancelRequest := context.WithCancel(context.Background())
	store := &cancelAfterResponseDecisionStore{
		CompletionStore: database,
		cancel:          cancelRequest,
	}
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour, MaxPending: time.Second,
	}, store, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	body := chatBody("cancel immediately after durable 200", false)
	firstRequest := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body)).WithContext(requestContext)
	firstRequest.Header.Set("Authorization", "Bearer hae_test")
	firstRequest.Header.Set(headerIdempotencyKey, "cancel-after-200")
	firstDone := make(chan struct{})
	go func() {
		gateway.ServeHTTP(httptest.NewRecorder(), firstRequest)
		close(firstDone)
	}()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("canceled original handler did not stop output promptly")
	}

	var assignment completion.Assignment
	select {
	case assignment = <-worker.Assignments:
	case <-time.After(time.Second):
		t.Fatal("durable request was not dispatched after original client cancellation")
	}
	key := storeapi.RequestKey{CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey}
	stored, err := database.LookupRequest(context.Background(), key, mustRequestDigest(t, body))
	if err != nil || stored.Request.Response.StatusCode != http.StatusOK || stored.Request.ResponseComplete {
		t.Fatalf("committed response before online replay = %+v, err = %v", stored.Request, err)
	}

	retryRequest := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	retryRequest.Header.Set("Authorization", "Bearer hae_test")
	retryRequest.Header.Set(headerIdempotencyKey, key.IdempotencyKey)
	retry := httptest.NewRecorder()
	retryDone := make(chan struct{})
	go func() {
		gateway.ServeHTTP(retry, retryRequest)
		close(retryDone)
	}()

	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "accepted-after-client-cancel", Type: completion.EventAccepted, WorkerID: worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "final-after-client-cancel", Type: completion.EventFinal, Text: "completed for online retry",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-retryDone:
	case <-time.After(time.Second):
		t.Fatal("online retry did not observe durable completion")
	}
	if retry.Code != http.StatusOK || !bytes.Contains(retry.Body.Bytes(), []byte("completed for online retry")) ||
		!bytes.Contains(retry.Body.Bytes(), []byte("data: [DONE]")) {
		t.Fatalf("online replay = %d, %q", retry.Code, retry.Body.Bytes())
	}
	select {
	case duplicate := <-worker.Assignments:
		t.Fatalf("idempotent online retry dispatched twice: %+v", duplicate)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestTransientPostFlushListFailureDoesNotCancelDispatchAndOnlineReplayCompletes(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "transient-list.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	workerHub := hub.New(2)
	worker, err := workerHub.Register("worker-transient-list")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	allowResponseList := make(chan struct{})
	store := &transientResponseListStore{
		CompletionStore: database,
		allow:           allowResponseList,
		failure:         make(chan struct{}),
	}
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour, MaxPending: time.Second,
	}, store, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	body := chatBody("survive transient post-flush read", false)
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set(headerIdempotencyKey, "transient-post-flush-list")
	recorder := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		gateway.ServeHTTP(recorder, request)
		close(handlerDone)
	}()
	var assignment completion.Assignment
	select {
	case assignment = <-worker.Assignments:
	case <-time.After(time.Second):
		t.Fatal("post-flush list failure stranded the committed request")
	}
	if recorder.Code != http.StatusOK || !recorder.Flushed ||
		!bytes.Contains(recorder.Body.Bytes(), []byte(`"role":"assistant"`)) {
		t.Fatalf("dispatch happened before durable start flush: code=%d flushed=%t body=%q",
			recorder.Code, recorder.Flushed, recorder.Body.Bytes())
	}
	// The first fresh dispatch must not depend on a store read after the 200
	// decision. Only now allow the output poll to read, then fail it once.
	close(allowResponseList)
	select {
	case <-store.failure:
	case <-time.After(time.Second):
		t.Fatal("post-flush response-list failure was not injected")
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("failed original output handler did not return")
	}

	retryRequest := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	retryRequest.Header.Set("Authorization", "Bearer hae_test")
	retryRequest.Header.Set(headerIdempotencyKey, assignment.IdempotencyKey)
	retry := httptest.NewRecorder()
	retryDone := make(chan struct{})
	go func() {
		gateway.ServeHTTP(retry, retryRequest)
		close(retryDone)
	}()

	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "accepted-after-list-retry", Type: completion.EventAccepted, WorkerID: worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "final-after-list-retry", Type: completion.EventFinal, Text: "transient read recovered",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-retryDone:
	case <-time.After(time.Second):
		t.Fatal("online replay did not finish after transient output failure")
	}
	if retry.Code != http.StatusOK || !bytes.Contains(retry.Body.Bytes(), []byte("transient read recovered")) ||
		!bytes.Contains(retry.Body.Bytes(), []byte("data: [DONE]")) {
		t.Fatalf("online replay after transient output failure = %d, %q", retry.Code, retry.Body.Bytes())
	}
	if store.listCalls() < 2 {
		t.Fatalf("online replay did not re-read durable response: calls=%d", store.listCalls())
	}
	select {
	case duplicate := <-worker.Assignments:
		t.Fatalf("transient retry dispatched twice: %+v", duplicate)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestOnlineReplayRetriesTransientInitialListWithoutEmpty200(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "retry-initial-list.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	workerHub := hub.New(2)
	worker, err := workerHub.Register("worker-replay-list")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	newServer := func(store storeapi.CompletionStore) *Server {
		t.Helper()
		gateway, err := NewServer(Config{
			HeartbeatInterval: time.Hour, MaxPending: time.Second,
		}, store, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
			"/v1/chat/completions": openai.New(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return gateway
	}

	body := chatBody("retry the first replay read", false)
	originalServer := newServer(database)
	firstRequest := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	firstRequest.Header.Set("Authorization", "Bearer hae_test")
	firstRequest.Header.Set(headerIdempotencyKey, "retry-initial-list")
	first := httptest.NewRecorder()
	firstDone := make(chan struct{})
	go func() {
		originalServer.ServeHTTP(first, firstRequest)
		close(firstDone)
	}()

	var assignment completion.Assignment
	select {
	case assignment = <-worker.Assignments:
	case <-time.After(time.Second):
		t.Fatal("original request was not assigned")
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "accepted-before-replay-list", Type: completion.EventAccepted, WorkerID: worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "final-before-replay-list", Type: completion.EventFinal, Text: "durable replay body",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("original request did not complete")
	}
	if first.Code != http.StatusOK || !bytes.Contains(first.Body.Bytes(), []byte("durable replay body")) {
		t.Fatalf("original response = %d, %q", first.Code, first.Body.Bytes())
	}

	transientStore := &transientResponseListStore{
		CompletionStore: database,
		failure:         make(chan struct{}),
	}
	replayServer := newServer(transientStore)
	retryRequest := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	retryRequest.Header.Set("Authorization", "Bearer hae_test")
	retryRequest.Header.Set(headerIdempotencyKey, assignment.IdempotencyKey)
	retry := httptest.NewRecorder()
	retryDone := make(chan struct{})
	go func() {
		replayServer.ServeHTTP(retry, retryRequest)
		close(retryDone)
	}()

	select {
	case <-transientStore.failure:
	case <-time.After(time.Second):
		t.Fatal("initial replay response-list failure was not injected")
	}
	select {
	case <-retryDone:
		t.Fatalf("initial list failure returned an empty response: code=%d body=%q", retry.Code, retry.Body.Bytes())
	default:
	}
	select {
	case <-retryDone:
	case <-time.After(time.Second):
		t.Fatal("online replay did not recover from its initial list failure")
	}
	if retry.Code != http.StatusOK || !bytes.Equal(retry.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("replayed response = %d, %q; original = %q", retry.Code, retry.Body.Bytes(), first.Body.Bytes())
	}
	if transientStore.listCalls() < 2 {
		t.Fatalf("initial replay list was not retried: calls=%d", transientStore.listCalls())
	}
	select {
	case duplicate := <-worker.Assignments:
		t.Fatalf("online replay dispatched twice: %+v", duplicate)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestWorkerLossAfterFlushed200PersistsTerminalReplayAcrossRestart(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "worker-loss.db")
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	workerHub := hub.New(2)
	worker, err := workerHub.Register("worker-window")
	if err != nil {
		t.Fatal(err)
	}
	faults := &transientWorkerEventStore{
		CompletionStore: database, stage: faultStep, remaining: 1,
	}
	gateway, err := NewServer(Config{
		HeartbeatInterval: time.Hour, MaxPending: 250 * time.Millisecond,
	}, faults, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	body := chatBody("worker disappears after 200", false)
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set(headerIdempotencyKey, "worker-loss-window")
	recorder := &flushCallbackRecorder{ResponseRecorder: httptest.NewRecorder()}
	var statusAtFirstFlush int
	var bodyAtFirstFlush []byte
	recorder.onFirstFlush = func() {
		statusAtFirstFlush = recorder.Code
		bodyAtFirstFlush = bytes.Clone(recorder.Body.Bytes())
		worker.Close()
	}
	gateway.ServeHTTP(recorder, request)
	firstBody := bytes.Clone(recorder.Body.Bytes())
	if statusAtFirstFlush != http.StatusOK || !bytes.Contains(bodyAtFirstFlush, []byte(`"role":"assistant"`)) ||
		bytes.Contains(bodyAtFirstFlush, []byte("worker_unavailable")) {
		t.Fatalf("first flush did not establish phase B before worker loss: status=%d body=%q", statusAtFirstFlush, bodyAtFirstFlush)
	}
	if recorder.Code != http.StatusOK || !bytes.Contains(firstBody, []byte("worker_unavailable")) ||
		!bytes.Contains(firstBody, []byte("data: [DONE]")) {
		t.Fatalf("worker-loss stream = %d, %q", recorder.Code, firstBody)
	}
	key := storeapi.RequestKey{CallerID: "caller-1", IdempotencyKey: "worker-loss-window"}
	stored, err := database.LookupRequest(context.Background(), key, mustRequestDigest(t, body))
	if err != nil || stored.Request.Response.StatusCode != http.StatusOK ||
		!stored.Request.ResponseComplete || stored.Task.State != completion.StateFailed {
		t.Fatalf("durable worker-loss response = %+v, err = %v", stored, err)
	}
	if failures := faults.failureCount(); failures != 1 {
		t.Fatalf("synthetic unavailable stage retries = %d, want one injected failure", failures)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	restarted, err := NewServer(Config{}, database, fixedAuthenticator{}, hub.New(2), adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	replayRequest := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	replayRequest.Header.Set("Authorization", "Bearer hae_test")
	replayRequest.Header.Set(headerIdempotencyKey, key.IdempotencyKey)
	replay := httptest.NewRecorder()
	restarted.ServeHTTP(replay, replayRequest)
	if replay.Code != http.StatusOK || !bytes.Equal(replay.Body.Bytes(), firstBody) {
		t.Fatalf("restarted replay = %d, %q; first = %q", replay.Code, replay.Body.Bytes(), firstBody)
	}

	conflictRequest := httptest.NewRequest(
		http.MethodPost, "/v1/chat/completions", bytes.NewReader(chatBody("different after restart", false)),
	)
	conflictRequest.Header.Set("Authorization", "Bearer hae_test")
	conflictRequest.Header.Set(headerIdempotencyKey, key.IdempotencyKey)
	conflict := httptest.NewRecorder()
	restarted.ServeHTTP(conflict, conflictRequest)
	if conflict.Code != http.StatusConflict || !bytes.Contains(conflict.Body.Bytes(), []byte("idempotency_conflict")) {
		t.Fatalf("restarted conflict = %d, %q", conflict.Code, conflict.Body.Bytes())
	}
}

func TestAdmissionRateLimitRunsAfterIdempotencyReplay(t *testing.T) {
	t.Parallel()
	limiter, err := ratelimit.New(ratelimit.Config{
		RatePerSecond: 1,
		Burst:         1,
		IdleTTL:       time.Hour,
	}, fixedClock{now: time.Unix(100, 0)})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newGatewayFixtureWithConfig(t, true, Config{RateLimiter: limiter})
	body := chatBody("first", false)
	done := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventAccepted, WorkerID: "worker-1",
		}); err != nil {
			done <- err
			return
		}
		done <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventFinal, Text: "done",
		})
	}()

	first, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "limited-request"))
	if err != nil {
		t.Fatal(err)
	}
	firstBody, err := io.ReadAll(first.Body)
	first.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", first.StatusCode, firstBody)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	fixture.worker.Close()
	fixture.worker = nil

	limited, err := http.DefaultClient.Do(newChatRequest(t, fixture, chatBody("new", false), "new-request"))
	if err != nil {
		t.Fatal(err)
	}
	limitedBody, err := io.ReadAll(limited.Body)
	limited.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if limited.StatusCode != http.StatusTooManyRequests || limited.Header.Get("Retry-After") != "1" {
		t.Fatalf("limited status = %d, retry-after = %q, body = %s", limited.StatusCode, limited.Header.Get("Retry-After"), limitedBody)
	}
	if got := limited.Header.Get(headerTaskID); got != "" {
		t.Fatalf("rate-limited response crossed HTTP 200 admission boundary with task %q", got)
	}

	replay, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "limited-request"))
	if err != nil {
		t.Fatal(err)
	}
	replayBody, err := io.ReadAll(replay.Body)
	replay.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if replay.StatusCode != http.StatusOK || !bytes.Equal(firstBody, replayBody) {
		t.Fatalf("replay status = %d, body = %s", replay.StatusCode, replayBody)
	}
}

func TestUnknownHarnessDowngradesToChat(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixture(t, true)
	done := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if len(assignment.Request.Tools) != 0 || assignment.WorkspaceKey != "" {
			done <- errors.New("unknown harness retained tool capability")
			return
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{Type: completion.EventRejected, ErrorCode: "rejected", Error: "no"}); err != nil {
			done <- err
			return
		}
		done <- nil
	}()
	request := newChatRequest(t, fixture, chatBody("hello", true), "request-remote")
	request.Header.Set(headerCapabilityTier, string(completion.TierRemoteTools))
	request.Header.Set(headerWorkspaceKey, "workspace")
	request.Header.Set(headerTaskID, "task")
	request.Header.Set(headerHarnessID, "unknown")
	request.Header.Set(headerHarnessVersion, "1")
	request.Header.Set(headerWorkspaceRoot, "/repo")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDeclaredCallerMismatchRejectedBeforeAdmission(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixture(t, true)
	request := newChatRequest(t, fixture, chatBody("caller mismatch", false), "request-caller-mismatch")
	request.Header.Set(headerTaskID, "task-caller-mismatch")
	request.Header.Set(headerCallerID, "another-caller")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("caller_identity_mismatch")) {
		t.Fatalf("caller mismatch response = %d, %q", response.StatusCode, body)
	}
	if _, err := fixture.db.GetTask(context.Background(), storeapi.TaskKey{
		CallerID: "caller-1", TaskID: "task-caller-mismatch",
	}); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("caller mismatch crossed BeginRequest: %v", err)
	}
	select {
	case assignment := <-fixture.worker.Assignments:
		t.Fatalf("caller mismatch crossed worker visibility: %+v", assignment)
	default:
	}
}

func TestHumanShimRemoteToolsRequiresCallerBeforeAdmission(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixture(t, true)
	if err := fixture.registry.Register(adapter.HumanShimProfile()); err != nil {
		t.Fatal(err)
	}
	request := newChatRequest(t, fixture, chatBody("missing caller", false), "request-missing-caller")
	setHumanShimRemoteHeaders(request, "task-missing-caller", "")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusPreconditionRequired || !bytes.Contains(body, []byte("caller_identity_required")) {
		t.Fatalf("missing caller response = %d, %q", response.StatusCode, body)
	}
	if _, err := fixture.db.GetTask(context.Background(), storeapi.TaskKey{
		CallerID: "caller-1", TaskID: "task-missing-caller",
	}); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("missing caller crossed BeginRequest: %v", err)
	}
	select {
	case assignment := <-fixture.worker.Assignments:
		t.Fatalf("missing caller crossed worker visibility: %+v", assignment)
	default:
	}
}

func TestHumanShimRemoteToolsAcceptsMatchingCaller(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixture(t, true)
	if err := fixture.registry.Register(adapter.HumanShimProfile()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if assignment.CallerID != "caller-1" || assignment.TaskID != "task-matching-caller" ||
			assignment.CapabilityTier != completion.TierRemoteTools || assignment.Adapter == nil ||
			assignment.Adapter.HarnessID != adapter.HumanShimID {
			done <- fmt.Errorf("matching caller assignment = %+v", assignment)
			return
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			ID: "accepted-matching-caller", Type: completion.EventAccepted, WorkerID: fixture.worker.ID,
		}); err != nil {
			done <- err
			return
		}
		done <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			ID: "final-matching-caller", Type: completion.EventFinal, Text: "caller identity matched",
		})
	}()

	request := newChatRequest(t, fixture, chatBody("matching caller", false), "request-matching-caller")
	setHumanShimRemoteHeaders(request, "task-matching-caller", "caller-1")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("caller identity matched")) {
		t.Fatalf("matching caller response = %d, %q", response.StatusCode, body)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestKnownRemoteHarnessPreservesDeclaredTools(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixture(t, true)
	if err := fixture.registry.Register(adapter.Profile{
		HarnessID: "known", HarnessVersion: "1", Read: &adapter.Tool{Name: "read_file"}, ErrorShape: "is_error",
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if len(assignment.Request.Tools) != 1 || assignment.WorkspaceKey != "workspace" {
			done <- errors.New("known harness lost declared tools")
			return
		}
		done <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{Type: completion.EventRejected, ErrorCode: "rejected", Error: "no"})
	}()
	request := newChatRequest(t, fixture, chatBody("hello", true), "request-known")
	request.Header.Set(headerCapabilityTier, string(completion.TierRemoteTools))
	request.Header.Set(headerWorkspaceKey, "workspace")
	request.Header.Set(headerTaskID, "task-known")
	request.Header.Set(headerHarnessID, "known")
	request.Header.Set(headerHarnessVersion, "1")
	request.Header.Set(headerWorkspaceRoot, "/repo")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRemoteToolResultReconcilesAndReturnsToStickyWorker(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixture(t, true)
	if err := fixture.registry.Register(adapter.Profile{
		HarnessID: "known", HarnessVersion: "1", Read: &adapter.Tool{Name: "read_file"}, ErrorShape: "is_error",
	}); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if assignment.TaskID != "task-loop" {
			firstDone <- errors.New("first assignment used the wrong task")
			return
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventAccepted, WorkerID: "worker-1",
		}); err != nil {
			firstDone <- err
			return
		}
		firstDone <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type:      completion.EventToolCalls,
			ToolCalls: []completion.ToolCall{{ID: "read-1", Name: "read_file", Input: map[string]any{"path": "README.md"}}},
		})
	}()

	firstRequest := newChatRequest(t, fixture, chatBody("diagnose", true), "request-loop-1")
	setRemoteHeaders(firstRequest, "task-loop")
	firstResponse, err := http.DefaultClient.Do(firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	firstBody, err := io.ReadAll(firstResponse.Body)
	firstResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if firstResponse.StatusCode != http.StatusOK || !strings.Contains(string(firstBody), `"finish_reason":"tool_calls"`) {
		t.Fatalf("first response = %d, %s", firstResponse.StatusCode, firstBody)
	}
	taskKey := storeapi.TaskKey{CallerID: "caller-1", TaskID: "task-loop"}
	task, err := fixture.db.GetTask(context.Background(), taskKey)
	if err != nil || task.State != completion.StateAwaitingResults || task.LeaseOwner != "worker-1" {
		t.Fatalf("task after tool dispatch = %+v, %v", task, err)
	}

	secondDone := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		persisted, err := fixture.db.GetTask(context.Background(), taskKey)
		if err != nil {
			secondDone <- err
			return
		}
		if assignment.TaskID != "task-loop" || assignment.LeaseOwner != "worker-1" || persisted.State != completion.StateReconciled {
			secondDone <- errors.New("follow-up was not reconciled and routed to the sticky worker")
			return
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventAccepted, WorkerID: "worker-1",
		}); err != nil {
			secondDone <- err
			return
		}
		secondDone <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventFinal, Text: "README inspected",
		})
	}()
	secondBody := []byte(`{
		"model":"human-expert","stream":true,
		"messages":[
			{"role":"user","content":"diagnose"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"read-1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"README.md\"}"}}]},
			{"role":"tool","tool_call_id":"read-1","content":"contents"}
		],
		"tools":[{"type":"function","function":{"name":"read_file","parameters":{"type":"object"}}}]
	}`)
	secondRequest := newChatRequest(t, fixture, secondBody, "request-loop-2")
	setRemoteHeaders(secondRequest, "task-loop")
	secondResponse, err := http.DefaultClient.Do(secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, err := io.ReadAll(secondResponse.Body)
	secondResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if secondResponse.StatusCode != http.StatusOK || !strings.Contains(string(responseBody), `"content":"README inspected"`) {
		t.Fatalf("second response = %d, %s", secondResponse.StatusCode, responseBody)
	}
	task, err = fixture.db.GetTask(context.Background(), taskKey)
	if err != nil || task.State != completion.StateCompleted || task.LeaseOwner != "worker-1" {
		t.Fatalf("completed task = %+v, %v", task, err)
	}
}

func TestAuditPayloadRequiresExplicitOptInAndHasTTL(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval: 5 * time.Millisecond,
		MaxPending:        time.Second,
		AuditPayload:      true,
		AuditPayloadTTL:   time.Hour,
	})
	done := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventAccepted, WorkerID: "worker-1",
		}); err != nil {
			done <- err
			return
		}
		done <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventFinal, Text: "audited response",
		})
	}()
	body := chatBody("audited request", false)
	response, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "request-audit-payload"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	auditID := testAuditID("caller-1", "request-audit-payload")
	requestPayload, err := fixture.db.GetAuditPayload(context.Background(), auditID, "request")
	if err != nil {
		t.Fatal(err)
	}
	responsePayload, err := fixture.db.GetAuditPayload(context.Background(), auditID, "response")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(requestPayload.Data, body) || !strings.Contains(string(responsePayload.Data), "audited response") {
		t.Fatalf("request = %s, response = %s", requestPayload.Data, responsePayload.Data)
	}
	if ttl := requestPayload.ExpiresAt.Sub(requestPayload.CreatedAt); ttl < 59*time.Minute || ttl > 61*time.Minute {
		t.Fatalf("request payload TTL = %s", ttl)
	}
}

func TestModelsRequiresAuthentication(t *testing.T) {
	t.Parallel()
	fixture := newGatewayFixture(t, false)
	response, err := http.Get(fixture.server.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated models status = %d", response.StatusCode)
	}
	request, err := http.NewRequest(http.MethodGet, fixture.server.URL+"/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer hae_test")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(string(data), "human-expert") {
		t.Fatalf("models status = %d, body = %s", response.StatusCode, data)
	}
}
