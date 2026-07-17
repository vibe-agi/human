package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/dialect/responses"
	"github.com/vibe-agi/human/internal/completion/hub"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

const (
	recoveryCreatedAtUnix  = int64(1_700_000_000)
	recoveryAcceptedAtUnix = int64(1_700_000_001)
	recoveryFinalAtUnix    = int64(1_700_000_002)
)

type failingRecoveryTerminalStore struct {
	storeapi.CompletionStore
	err error
}

func (store failingRecoveryTerminalStore) FailRequest(
	context.Context,
	storeapi.RequestKey,
	completion.State,
	storeapi.ResponseDecision,
) (storeapi.Request, error) {
	return storeapi.Request{}, store.err
}

func checkpointChatRequest(
	t *testing.T,
	database *sqlite.Store,
	idempotencyKey string,
	accepted bool,
	workerID string,
) (canonical.Request, []byte) {
	t.Helper()
	body := chatBody("survive restart", false)
	request, err := openai.New().Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	begin, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "task-restart"},
			CapabilityTier: completion.TierChat,
			Dialect:        canonical.DialectOpenAIChat,
			LeaseOwner:     workerID,
		},
		IdempotencyKey:   idempotencyKey,
		RequestDigest:    digest,
		CanonicalRequest: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := json.Marshal(streamMetadata{
		ResponseID: "chatcmpl_restarted", Model: request.Model,
		CreatedAtUnix: recoveryCreatedAtUnix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendResponseEvent(context.Background(), begin.Request.RequestKey, responseEventStream, metadata); err != nil {
		t.Fatal(err)
	}
	stream := openai.New().NewStream("chatcmpl_restarted", request.Model, dialect.StreamSeed{
		CreatedAtUnix: recoveryCreatedAtUnix,
	})
	frames, err := stream.Start()
	if err != nil {
		t.Fatal(err)
	}
	for _, frame := range frames {
		if _, err := database.AppendResponseEvent(context.Background(), begin.Request.RequestKey, responseEventWire, frame); err != nil {
			t.Fatal(err)
		}
	}
	if accepted {
		event := completion.Event{ID: "accepted-before-restart", Type: completion.EventAccepted, WorkerID: "worker-sticky"}
		frames, done, err := stream.Encode(event, dialect.EventSeed{EncodedAtUnix: recoveryAcceptedAtUnix})
		if err != nil || done {
			t.Fatalf("encode accepted = done %v, err %v", done, err)
		}
		payload, err := json.Marshal(persistedStep{
			Event: event, Wire: bytes.Join(frames, nil), Done: false,
			EncodedAtUnix: recoveryAcceptedAtUnix,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.AppendResponseEvent(context.Background(), begin.Request.RequestKey, responseEventStep, payload); err != nil {
			t.Fatal(err)
		}
		if _, err := database.TransitionTask(context.Background(), begin.Task.TaskKey, completion.StateAdmitted, completion.StateLeased, "worker-sticky"); err != nil {
			t.Fatal(err)
		}
		if _, err := database.TransitionTask(context.Background(), begin.Task.TaskKey, completion.StateLeased, completion.StateAwaitingHuman, ""); err != nil {
			t.Fatal(err)
		}
	}
	return request, body
}

func newRecoveryServer(t *testing.T, database *sqlite.Store, workerHub *hub.Hub) (*Server, context.CancelFunc) {
	return newRecoveryServerWithConfig(t, database, workerHub, Config{})
}

func newRecoveryServerWithConfig(
	t *testing.T,
	database *sqlite.Store,
	workerHub *hub.Hub,
	config Config,
) (*Server, context.CancelFunc) {
	t.Helper()
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = time.Hour
	}
	if config.MaxPending == 0 {
		config.MaxPending = 5 * time.Second
	}
	server, err := NewServer(config, database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := server.Recover(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	return server, cancel
}

func TestRestartRecoveryPreservesStickyWorkerAndCompletesReplay(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "restart.db")
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	_, body := checkpointChatRequest(t, database, "request-restart", true, "worker-sticky")
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	workerHub := hub.New(4)
	var routeMu sync.Mutex
	routeCalls := 0
	server, cancel := newRecoveryServerWithConfig(t, database, workerHub, Config{
		WorkerRouter: workerRouterFunc(func(context.Context, WorkerRouteRequest) (string, error) {
			routeMu.Lock()
			defer routeMu.Unlock()
			routeCalls++
			return "worker-other", nil
		}),
	})
	t.Cleanup(cancel)
	routeMu.Lock()
	if routeCalls != 0 {
		t.Fatalf("recovery invoked new-task worker router %d times", routeCalls)
	}
	routeMu.Unlock()

	other, err := workerHub.Register("worker-other")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	select {
	case assignment := <-other.Assignments:
		t.Fatalf("sticky assignment leaked to another worker: %+v", assignment)
	case <-time.After(20 * time.Millisecond):
	}

	sticky, err := workerHub.Register("worker-sticky")
	if err != nil {
		t.Fatal(err)
	}
	defer sticky.Close()
	var assignment completion.Assignment
	select {
	case assignment = <-sticky.Assignments:
	case <-time.After(time.Second):
		t.Fatal("recovered assignment was not delivered to its sticky worker")
	}
	if assignment.TaskID != "task-restart" || assignment.Request.Model != "human-expert" {
		t.Fatalf("recovered assignment = %+v", assignment)
	}
	// Re-delivery commonly makes the worker repeat accepted. The recovered
	// hub remembers that acceptance and suppresses it without a state error.
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "accepted-after-restart", Type: completion.EventAccepted, WorkerID: "worker-sticky",
	}); err != nil {
		t.Fatal(err)
	}
	finalEvent := completion.Event{
		ID: "final-after-restart", Type: completion.EventFinal, Text: "recovered result",
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, finalEvent); err != nil {
		t.Fatal(err)
	}
	finalDigest, err := workerEventDigest(finalEvent)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := database.LookupWorkerEventReceipt(context.Background(), storeapi.RequestKey{
		CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
	}, finalEvent.ID)
	if err != nil || receipt.Digest != finalDigest || receipt.WorkerID != assignment.LeaseOwner {
		t.Fatalf("durable worker receipt = %v", err)
	}

	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set(headerIdempotencyKey, "request-restart")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(responseBody), "recovered result") || !strings.Contains(string(responseBody), "data: [DONE]") {
		t.Fatalf("replayed recovered response status = %d, body = %s", response.StatusCode, responseBody)
	}
	if count := bytes.Count(responseBody, []byte(`"created":1700000000`)); count != 3 {
		t.Fatalf("OpenAI stream creation seed changed across restart: count=%d body=%s", count, responseBody)
	}
	lookup, err := database.LookupRequest(context.Background(), storeapi.RequestKey{
		CallerID: "caller-1", IdempotencyKey: "request-restart",
	}, mustRequestDigest(t, body))
	if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateCompleted {
		t.Fatalf("durable recovered request = %+v, err = %v", lookup, err)
	}
}

func TestRestartRecoveryPreservesAdmittedTaskOwner(t *testing.T) {
	t.Parallel()
	database := openRecoveryDatabase(t)
	checkpointChatRequest(t, database, "request-admitted", false, "worker-any")
	workerHub := hub.New(2)
	_, cancel := newRecoveryServer(t, database, workerHub)
	t.Cleanup(cancel)
	worker, err := workerHub.Register("worker-any")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	select {
	case assignment := <-worker.Assignments:
		if assignment.LeaseOwner != "worker-any" || assignment.IdempotencyKey != "request-admitted" {
			t.Fatalf("recovered admitted assignment = %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted recovery was not assigned to the first worker")
	}
}

func TestRestartRecoveryCreatesCheckpointForPre200AdmittedRequest(t *testing.T) {
	t.Parallel()
	database := openRecoveryDatabase(t)
	request, err := openai.New().Decode(chatBody("crash before stream checkpoint", false))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	created, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "pre-200-task"},
			CapabilityTier: completion.TierChat, Dialect: canonical.DialectOpenAIChat,
			LeaseOwner: "worker-after-crash",
		},
		IdempotencyKey: "pre-200-request", RequestDigest: digest, CanonicalRequest: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	workerHub := hub.New(2)
	_, cancel := newRecoveryServer(t, database, workerHub)
	defer cancel()
	lookup, err := database.LookupRequest(context.Background(), created.Request.RequestKey, digest)
	if err != nil || lookup.Request.Response.StatusCode != http.StatusOK {
		t.Fatalf("recovered pre-200 response decision = %+v, err = %v", lookup, err)
	}
	events, err := database.ListResponseEvents(context.Background(), created.Request.RequestKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Kind != responseEventStream || events[1].Kind != responseEventWire {
		t.Fatalf("recovery-created checkpoint = %+v", events)
	}
	worker, err := workerHub.Register("worker-after-crash")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	select {
	case assignment := <-worker.Assignments:
		if assignment.TaskID != "pre-200-task" {
			t.Fatalf("pre-200 recovered assignment = %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("pre-200 admitted request was not restored")
	}
}

func TestRestartRecoveryCompletesPartialMultiFrameStreamStart(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "partial-responses-start.db")
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"model":"human-expert","stream":true,"input":"resume partial start"}`)
	canonicalRequest, err := responses.New().Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := canonicalRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	begin, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "partial-start-task"},
			CapabilityTier: completion.TierChat,
			Dialect:        canonical.DialectResponses,
			LeaseOwner:     "worker-partial-start",
		},
		IdempotencyKey:   "partial-responses-start",
		RequestDigest:    digest,
		CanonicalRequest: canonicalRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	const responseID = "resp_partial_start"
	metadata, err := json.Marshal(streamMetadata{
		ResponseID: responseID, Model: canonicalRequest.Model,
		CreatedAtUnix: recoveryCreatedAtUnix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendResponseEvent(
		context.Background(), begin.Request.RequestKey, responseEventStream, metadata,
	); err != nil {
		t.Fatal(err)
	}
	encoder := responses.New().NewStream(
		responseID, canonicalRequest.Model,
		dialect.StreamSeed{CreatedAtUnix: recoveryCreatedAtUnix},
	)
	startFrames, err := encoder.Start()
	if err != nil {
		t.Fatal(err)
	}
	if len(startFrames) != 2 {
		t.Fatalf("Responses start frame count = %d, want 2", len(startFrames))
	}
	// Model the exact crash window: response.created is durable, while the
	// following response.in_progress append never happened.
	if _, err := database.AppendResponseEvent(
		context.Background(), begin.Request.RequestKey, responseEventWire, startFrames[0],
	); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	workerHub := hub.New(2)
	gateway, err := NewServer(
		Config{HeartbeatInterval: time.Hour, MaxPending: time.Second},
		database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(),
		map[string]dialect.Codec{"/v1/responses": responses.New()},
	)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	if err := gateway.Recover(runContext); err != nil {
		cancelRun()
		_ = database.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancelRun()
		gateway.Wait()
		_ = database.Close()
	})

	events, err := database.ListResponseEvents(
		context.Background(), begin.Request.RequestKey, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].Kind != responseEventStream ||
		events[1].Kind != responseEventWire || events[2].Kind != responseEventWire ||
		!bytes.Equal(events[1].Data, startFrames[0]) ||
		!bytes.Equal(events[2].Data, startFrames[1]) {
		t.Fatalf("recovered partial Responses start = %+v", events)
	}

	worker, err := workerHub.Register("worker-partial-start")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	assignment := waitAssignment(t, worker)
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "partial-start-accepted", Type: completion.EventAccepted, WorkerID: worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "partial-start-final", Type: completion.EventFinal, Text: "recovered start",
	}); err != nil {
		t.Fatal(err)
	}

	httpServer := httptest.NewServer(gateway)
	defer httpServer.Close()
	first := waitEventResponse(t, startEventRequest(
		t, context.Background(), httpServer.URL, "/v1/responses", body, begin.Request.IdempotencyKey,
	))
	second := waitEventResponse(t, startEventRequest(
		t, context.Background(), httpServer.URL, "/v1/responses", body, begin.Request.IdempotencyKey,
	))
	if first.err != nil || first.status != http.StatusOK || second.err != nil ||
		second.status != http.StatusOK || !bytes.Equal(first.body, second.body) {
		t.Fatalf(
			"partial-start byte-stable replay = first(%d, %q, %v), second(%d, %q, %v)",
			first.status, first.body, first.err, second.status, second.body, second.err,
		)
	}
	if !bytes.HasPrefix(first.body, bytes.Join(startFrames, nil)) {
		t.Fatalf("replay does not begin with the complete durable start: %q", first.body)
	}
	for sequence := 0; sequence <= 8; sequence++ {
		needle := []byte(fmt.Sprintf(`"sequence_number":%d`, sequence))
		if count := bytes.Count(first.body, needle); count != 1 {
			t.Fatalf("recovered Responses sequence %d count = %d, body = %q", sequence, count, first.body)
		}
	}
	if bytes.Index(first.body, []byte("response.in_progress")) >
		bytes.Index(first.body, []byte("response.output_item.added")) {
		t.Fatalf("response.in_progress was replayed after output started: %q", first.body)
	}
}

func TestRestartRecoveryAfterCommitted200BeforeEnqueue(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "committed-200.db")
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	_, body := checkpointChatRequest(t, database, "request-committed-200", false, "worker-after-200-crash")
	key := storeapi.RequestKey{CallerID: "caller-1", IdempotencyKey: "request-committed-200"}
	decision, err := database.BeginResponse(context.Background(), key)
	if err != nil || decision.Response.StatusCode != http.StatusOK {
		t.Fatalf("commit 200 decision = %+v, err = %v", decision, err)
	}
	// Simulate process loss after the durable phase-B decision but before the
	// handler can Enqueue the assignment.
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	workerHub := hub.New(2)
	server, cancel := newRecoveryServer(t, database, workerHub)
	defer cancel()
	worker, err := workerHub.Register("worker-after-200-crash")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	var assignment completion.Assignment
	select {
	case assignment = <-worker.Assignments:
	case <-time.After(time.Second):
		t.Fatal("committed-200 request was not restored after restart")
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "accepted-after-200-crash", Type: completion.EventAccepted, WorkerID: worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := workerHub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
		ID: "final-after-200-crash", Type: completion.EventFinal, Text: "resumed after committed 200",
	}); err != nil {
		t.Fatal(err)
	}

	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set(headerIdempotencyKey, key.IdempotencyKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK ||
		!bytes.Contains(responseBody, []byte("resumed after committed 200")) ||
		!bytes.Contains(responseBody, []byte("data: [DONE]")) {
		t.Fatalf("committed-200 recovery response = %d, %q", response.StatusCode, responseBody)
	}
}

func TestRestartRecoveryFinishesDurableTerminalStepAfterStateCrash(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "terminal-step.db")
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	request, body := checkpointChatRequest(t, database, "request-terminal-step", true, "worker-sticky")
	stream := openai.New().NewStream("chatcmpl_restarted", request.Model, dialect.StreamSeed{
		CreatedAtUnix: recoveryCreatedAtUnix,
	})
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stream.Encode(completion.Event{
		ID: "accepted-before-restart", Type: completion.EventAccepted, WorkerID: "worker-sticky",
	}, dialect.EventSeed{EncodedAtUnix: recoveryAcceptedAtUnix}); err != nil {
		t.Fatal(err)
	}
	final := completion.Event{ID: "final-before-restart", Type: completion.EventFinal, Text: "durable before state"}
	frames, done, err := stream.Encode(final, dialect.EventSeed{EncodedAtUnix: recoveryFinalAtUnix})
	if err != nil || !done {
		t.Fatalf("encode final = done %v, err %v", done, err)
	}
	payload, err := json.Marshal(persistedStep{
		Event: final, Wire: bytes.Join(frames, nil), Done: true,
		EncodedAtUnix: recoveryFinalAtUnix,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := storeapi.RequestKey{CallerID: "caller-1", IdempotencyKey: "request-terminal-step"}
	if _, err := database.AppendResponseEvent(context.Background(), key, responseEventStep, payload); err != nil {
		t.Fatal(err)
	}
	// Model the narrow crash point after the state reached terminal but before
	// response_complete was set. ListRecoverableRequests includes this record
	// only because the exact terminal response step is already durable.
	taskKey := storeapi.TaskKey{CallerID: "caller-1", TaskID: "task-restart"}
	if _, err := database.TransitionTask(context.Background(), taskKey, completion.StateAwaitingHuman, completion.StateResponded, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionTask(context.Background(), taskKey, completion.StateResponded, completion.StateCompleted, ""); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	server, cancel := newRecoveryServer(t, database, hub.New(2))
	defer cancel()
	lookup, err := database.LookupRequest(context.Background(), key, mustRequestDigest(t, body))
	if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateCompleted {
		t.Fatalf("terminal recovery = %+v, err = %v", lookup, err)
	}

	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	httpRequest, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Authorization", "Bearer hae_test")
	httpRequest.Header.Set(headerIdempotencyKey, key.IdempotencyKey)
	response, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(responseBody), "durable before state") || !strings.Contains(string(responseBody), "data: [DONE]") {
		t.Fatalf("recovered terminal wire = %s", responseBody)
	}
}

func TestRestartRecoveryQuarantinesRequestWithoutCheckpointAndRecoversHealthyPeer(t *testing.T) {
	t.Parallel()
	database := openRecoveryDatabase(t)
	request, err := openai.New().Decode(chatBody("missing checkpoint", false))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	created, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "missing-checkpoint-task"},
			CapabilityTier: completion.TierChat, Dialect: canonical.DialectOpenAIChat,
			LeaseOwner: "checkpoint-worker",
		},
		IdempotencyKey: "missing-checkpoint-request", RequestDigest: digest, CanonicalRequest: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionTask(context.Background(), created.Task.TaskKey, completion.StateAdmitted, completion.StateLeased, "checkpoint-worker"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionTask(context.Background(), created.Task.TaskKey, completion.StateLeased, completion.StateAwaitingHuman, ""); err != nil {
		t.Fatal(err)
	}
	checkpointChatRequest(t, database, "healthy-request", false, "worker-healthy")
	workerHub := hub.New(2)
	server, err := NewServer(Config{}, database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	if err := server.Recover(runContext); err != nil {
		cancelRun()
		t.Fatalf("recovery with quarantined checkpoint-less request = %v", err)
	}
	defer func() {
		cancelRun()
		server.Wait()
	}()
	lookup, err := database.LookupRequest(
		context.Background(), created.Request.RequestKey, digest,
	)
	if err != nil {
		t.Fatalf("lookup failed-closed checkpoint-less request: %v", err)
	}
	if !lookup.Request.ResponseComplete || lookup.Request.Response.StatusCode != http.StatusOK ||
		lookup.Task.State != completion.StateFailed {
		t.Fatalf("failed-closed checkpoint-less request = %+v", lookup)
	}
	read, err := database.ReadResponse(context.Background(), created.Request.RequestKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	var terminalWire []byte
	for _, event := range read.Events {
		wire, wireErr := responseEventWireData(event)
		if wireErr != nil {
			t.Fatal(wireErr)
		}
		terminalWire = append(terminalWire, wire...)
	}
	if !bytes.Contains(terminalWire, []byte("recovery_failed")) {
		t.Fatalf("checkpoint-less request has no durable recovery failure: %s", terminalWire)
	}
	worker, err := workerHub.Register("worker-healthy")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	select {
	case assignment := <-worker.Assignments:
		if assignment.IdempotencyKey != "healthy-request" {
			t.Fatalf("assignment after quarantine = %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy request was not recovered after quarantining its peer")
	}
}

func TestRecoveryQuarantinesStreamCheckpointWithoutClockSeed(t *testing.T) {
	database := openRecoveryDatabase(t)
	body := chatBody("invalid stream seed", false)
	request, err := openai.New().Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	created, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "invalid-seed-task"},
			CapabilityTier: completion.TierChat,
			Dialect:        canonical.DialectOpenAIChat,
			LeaseOwner:     "invalid-seed-worker",
		},
		IdempotencyKey: "invalid-seed-request", RequestDigest: digest, CanonicalRequest: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	invalidMetadata, err := json.Marshal(map[string]string{
		"response_id": "chatcmpl_invalid_seed", "model": request.Model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendResponseEvent(
		context.Background(), created.Request.RequestKey, responseEventStream, invalidMetadata,
	); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	server, err := NewServer(Config{
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}, database, fixedAuthenticator{}, hub.New(2), adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Recover(context.Background()); err != nil {
		t.Fatalf("recovery with quarantined invalid stream metadata = %v", err)
	}
	lookup, err := database.LookupRequest(
		context.Background(), created.Request.RequestKey, digest,
	)
	if err != nil {
		t.Fatalf("lookup failed-closed invalid checkpoint: %v", err)
	}
	if !lookup.Request.ResponseComplete || lookup.Request.Response.StatusCode != http.StatusInternalServerError ||
		lookup.Task.State != completion.StateAdmitted ||
		!bytes.Contains(lookup.Request.Response.Body, []byte("recovery_failed")) {
		t.Fatalf("invalid checkpoint did not become durable failure: %+v", lookup)
	}
	if !strings.Contains(logs.String(), "failed closed unrecoverable completion request") ||
		!strings.Contains(logs.String(), "invalid-seed-request") {
		t.Fatalf("recovery quarantine bypassed configured logger: %s", logs.String())
	}
}

func TestRecoveryAbortsWhenUnrecoverableRequestCannotBeFailedClosed(t *testing.T) {
	database := openRecoveryDatabase(t)
	request, err := openai.New().Decode(chatBody("terminal persistence must succeed", false))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	created, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey: storeapi.TaskKey{
				CallerID: "caller-1",
				TaskID:   "failed-closed-persistence-task",
			},
			CapabilityTier: completion.TierChat,
			Dialect:        canonical.DialectOpenAIChat,
			LeaseOwner:     "failed-closed-persistence-worker",
		},
		IdempotencyKey:   "failed-closed-persistence-request",
		RequestDigest:    digest,
		CanonicalRequest: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	// A stream checkpoint without its required clock seed is trustworthy enough
	// to identify and fail the request, but cannot be resumed.
	invalidMetadata, err := json.Marshal(map[string]string{
		"response_id": "chatcmpl_failed_closed_persistence",
		"model":       request.Model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendResponseEvent(
		context.Background(), created.Request.RequestKey, responseEventStream, invalidMetadata,
	); err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected recovery terminal persistence failure")
	server, err := NewServer(
		Config{},
		failingRecoveryTerminalStore{CompletionStore: database, err: injected},
		fixedAuthenticator{},
		hub.New(2),
		adapter.NewRegistry(),
		map[string]dialect.Codec{"/v1/chat/completions": openai.New()},
	)
	if err != nil {
		t.Fatal(err)
	}
	recoverErr := server.Recover(context.Background())
	if !errors.Is(recoverErr, injected) {
		t.Fatalf("Recover() error = %v, want injected terminal persistence failure", recoverErr)
	}
	if !strings.Contains(recoverErr.Error(), created.Request.CallerID+"/"+created.Request.IdempotencyKey) {
		t.Fatalf("Recover() error lacks request identity: %v", recoverErr)
	}

	lookup, err := database.LookupRequest(
		context.Background(), created.Request.RequestKey, digest,
	)
	if err != nil {
		t.Fatal(err)
	}
	if lookup.Request.ResponseComplete || lookup.Request.Response.StatusCode != 0 {
		t.Fatalf("injected terminal failure unexpectedly became durable: %+v", lookup.Request)
	}
}

func TestRecoveryDurablyQuarantinesCorruptCanonicalBeforeStream(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "raw-quarantine.db")
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	body := chatBody("raw recovery quarantine", false)
	request, err := openai.New().Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	created, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "raw-quarantine-task"},
			CapabilityTier: completion.TierChat,
			Dialect:        canonical.DialectOpenAIChat,
			LeaseOwner:     "raw-quarantine-worker",
		},
		IdempotencyKey: "raw-quarantine-request", RequestDigest: digest,
		CanonicalRequest: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpointChatRequest(t, database, "raw-quarantine-healthy", false, "raw-quarantine-worker")
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(context.Background(), `
		UPDATE completion_requests SET canonical_request = X''
		WHERE caller_id = ? AND idempotency_key = ?`,
		created.Request.CallerID, created.Request.IdempotencyKey,
	); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	workerHub := hub.New(2)
	gateway, err := NewServer(
		Config{HeartbeatInterval: time.Hour, MaxPending: time.Second},
		database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(),
		map[string]dialect.Codec{"/v1/chat/completions": openai.New()},
	)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	if err := gateway.Recover(runContext); err != nil {
		cancelRun()
		_ = database.Close()
		t.Fatal(err)
	}
	worker, err := workerHub.Register("raw-quarantine-worker")
	if err != nil {
		cancelRun()
		_ = database.Close()
		t.Fatal(err)
	}
	select {
	case assignment := <-worker.Assignments:
		if assignment.IdempotencyKey != "raw-quarantine-healthy" {
			t.Fatalf("healthy recovery after raw quarantine = %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy peer was blocked by raw recovery quarantine")
	}
	httpServer := httptest.NewServer(gateway)
	defer func() {
		httpServer.Close()
		cancelRun()
		worker.Close()
		gateway.Wait()
		_ = database.Close()
	}()

	first := waitEventResponse(t, startEventRequest(
		t, context.Background(), httpServer.URL, "/v1/chat/completions", body,
		created.Request.IdempotencyKey,
	))
	second := waitEventResponse(t, startEventRequest(
		t, context.Background(), httpServer.URL, "/v1/chat/completions", body,
		created.Request.IdempotencyKey,
	))
	if first.err != nil || second.err != nil || first.status != http.StatusInternalServerError ||
		second.status != http.StatusInternalServerError || !bytes.Equal(first.body, second.body) ||
		!bytes.Contains(first.body, []byte("recovery_failed")) {
		t.Fatalf(
			"finite raw quarantine replay = first(%d, %q, %v), second(%d, %q, %v)",
			first.status, first.body, first.err, second.status, second.body, second.err,
		)
	}
	changed := chatBody("different request under same key", false)
	conflict := waitEventResponse(t, startEventRequest(
		t, context.Background(), httpServer.URL, "/v1/chat/completions", changed,
		created.Request.IdempotencyKey,
	))
	if conflict.err != nil || conflict.status != http.StatusConflict ||
		!bytes.Contains(conflict.body, []byte("idempotency_conflict")) {
		t.Fatalf("quarantined idempotency conflict = %d, %q, %v", conflict.status, conflict.body, conflict.err)
	}
	lookup, err := database.LookupRequest(context.Background(), created.Request.RequestKey, digest)
	if err != nil || !lookup.Request.RecoveryQuarantined || !lookup.Request.ResponseComplete {
		t.Fatalf("durable raw quarantine lookup = %+v, %v", lookup, err)
	}
}

func TestRecoveryDurablyTerminatesCorruptCanonicalAfterCommitted200(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "raw-stream-quarantine.db")
	body := []byte(`{"model":"human-expert","stream":true,"input":"raw stream quarantine"}`)
	request, err := responses.New().Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	created, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "raw-stream-task"},
			CapabilityTier: completion.TierChat,
			Dialect:        canonical.DialectResponses,
			LeaseOwner:     "raw-stream-worker",
		},
		IdempotencyKey: "raw-stream-request", RequestDigest: digest,
		CanonicalRequest: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := streamMetadata{
		ResponseID: "resp_raw_stream", Model: request.Model, CreatedAtUnix: recoveryCreatedAtUnix,
	}
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendResponseEvent(
		context.Background(), created.Request.RequestKey, responseEventStream, metadataBytes,
	); err != nil {
		t.Fatal(err)
	}
	encoder := responses.New().NewStream(
		metadata.ResponseID, metadata.Model,
		dialect.StreamSeed{CreatedAtUnix: metadata.CreatedAtUnix},
	)
	startFrames, err := encoder.Start()
	if err != nil {
		t.Fatal(err)
	}
	for _, frame := range startFrames {
		if _, err := database.AppendResponseEvent(
			context.Background(), created.Request.RequestKey, responseEventWire, frame,
		); err != nil {
			t.Fatal(err)
		}
	}
	progress := completion.Event{
		ID: "raw-stream-progress", Type: completion.EventProgress, Text: "partial",
	}
	progressFrames, done, err := encoder.Encode(
		progress, dialect.EventSeed{EncodedAtUnix: recoveryAcceptedAtUnix},
	)
	if err != nil || done {
		t.Fatalf("encode partial Responses progress = done %t, err %v", done, err)
	}
	progressPayload, err := json.Marshal(persistedStep{
		Event: progress, Wire: bytes.Join(progressFrames, nil), Done: false,
		EncodedAtUnix: recoveryAcceptedAtUnix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendResponseEvent(
		context.Background(), created.Request.RequestKey, responseEventStep, progressPayload,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendResponseEvent(
		context.Background(), created.Request.RequestKey, responseEventApplied, progressPayload,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := database.BeginResponse(context.Background(), created.Request.RequestKey); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(context.Background(), `
		UPDATE completion_requests SET canonical_request = '{'
		WHERE caller_id = ? AND idempotency_key = ?`,
		created.Request.CallerID, created.Request.IdempotencyKey,
	); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	replayOnce := func() eventResponse {
		db, openErr := sqlite.Open(context.Background(), databasePath)
		if openErr != nil {
			t.Fatal(openErr)
		}
		gateway, newErr := NewServer(
			Config{}, db, fixedAuthenticator{}, hub.New(2), adapter.NewRegistry(),
			map[string]dialect.Codec{"/v1/responses": responses.New()},
		)
		if newErr != nil {
			_ = db.Close()
			t.Fatal(newErr)
		}
		if recoverErr := gateway.Recover(context.Background()); recoverErr != nil {
			_ = db.Close()
			t.Fatal(recoverErr)
		}
		httpServer := httptest.NewServer(gateway)
		result := waitEventResponse(t, startEventRequest(
			t, context.Background(), httpServer.URL, "/v1/responses", body,
			created.Request.IdempotencyKey,
		))
		httpServer.Close()
		gateway.Wait()
		if closeErr := db.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		return result
	}
	first := replayOnce()
	second := replayOnce()
	if first.err != nil || second.err != nil || first.status != http.StatusOK ||
		second.status != http.StatusOK || !bytes.Equal(first.body, second.body) ||
		!bytes.Contains(first.body, []byte("response.failed")) ||
		!bytes.Contains(first.body, []byte("recovery_failed")) {
		t.Fatalf(
			"committed raw quarantine replay = first(%d, %q, %v), second(%d, %q, %v)",
			first.status, first.body, first.err, second.status, second.body, second.err,
		)
	}
	if !bytes.HasPrefix(first.body, bytes.Join(startFrames, nil)) {
		t.Fatalf("committed quarantine lost durable stream prefix: %q", first.body)
	}
	wantTerminalSequence := len(startFrames) + len(progressFrames)
	for sequence := 0; sequence <= wantTerminalSequence; sequence++ {
		needle := []byte(fmt.Sprintf(`"sequence_number":%d`, sequence))
		if count := bytes.Count(first.body, needle); count != 1 {
			t.Fatalf(
				"quarantined Responses sequence %d count = %d, body = %q",
				sequence, count, first.body,
			)
		}
	}
}

func TestRecoveryDrainsDurableBacklogAboveReducedQueueCapacity(t *testing.T) {
	database := openRecoveryDatabase(t)
	for index := 0; index < 3; index++ {
		request, err := openai.New().Decode(chatBody(fmt.Sprintf("backlog-%d", index), false))
		if err != nil {
			t.Fatal(err)
		}
		digest, err := request.Digest()
		if err != nil {
			t.Fatal(err)
		}
		created, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
			Task: storeapi.Task{
				TaskKey: storeapi.TaskKey{
					CallerID: "caller-1", TaskID: fmt.Sprintf("backlog-task-%d", index),
				},
				CapabilityTier: completion.TierChat, Dialect: canonical.DialectOpenAIChat,
				LeaseOwner: "worker-backlog",
			},
			IdempotencyKey: fmt.Sprintf("backlog-request-%d", index),
			RequestDigest:  digest, CanonicalRequest: request,
		})
		if err != nil {
			t.Fatal(err)
		}
		responseID := fmt.Sprintf("chatcmpl_backlog_%d", index)
		metadata, err := json.Marshal(streamMetadata{
			ResponseID: responseID, Model: request.Model, CreatedAtUnix: recoveryCreatedAtUnix,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.AppendResponseEvent(
			context.Background(), created.Request.RequestKey, responseEventStream, metadata,
		); err != nil {
			t.Fatal(err)
		}
		encoder := openai.New().NewStream(responseID, request.Model, dialect.StreamSeed{
			CreatedAtUnix: recoveryCreatedAtUnix,
		})
		frames, err := encoder.Start()
		if err != nil {
			t.Fatal(err)
		}
		for _, frame := range frames {
			if _, err := database.AppendResponseEvent(
				context.Background(), created.Request.RequestKey, responseEventWire, frame,
			); err != nil {
				t.Fatal(err)
			}
		}
	}

	workerHub := hub.New(1)
	server, err := NewServer(Config{
		HeartbeatInterval: time.Hour, MaxPending: time.Hour,
	}, database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		server.Wait()
	}()
	if err := server.Recover(runContext); err != nil {
		t.Fatalf("recovery above reduced capacity: %v", err)
	}
	worker, err := workerHub.Register("worker-backlog")
	if err != nil {
		t.Fatalf("register worker for recovered backlog: %v", err)
	}
	defer worker.Close()
	seen := make(map[string]struct{})
	for len(seen) < 3 {
		select {
		case assignment := <-worker.Assignments:
			seen[assignment.IdempotencyKey] = struct{}{}
		case <-time.After(time.Second):
			t.Fatalf("only drained recovered assignments %v", seen)
		}
	}
	if _, err := workerHub.Reserve("worker-backlog"); !errors.Is(err, hub.ErrCapacity) {
		t.Fatalf("new admission bypassed recovered backlog: %v", err)
	}
}

func openRecoveryDatabase(t *testing.T) *sqlite.Store {
	t.Helper()
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "recovery.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func mustRequestDigest(t *testing.T, body []byte) string {
	t.Helper()
	request, err := openai.New().Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
