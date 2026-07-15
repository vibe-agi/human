package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/hub"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

const (
	recoveryCreatedAtUnix  = int64(1_700_000_000)
	recoveryAcceptedAtUnix = int64(1_700_000_001)
	recoveryFinalAtUnix    = int64(1_700_000_002)
)

func checkpointChatRequest(
	t *testing.T,
	database *sqlite.Store,
	idempotencyKey string,
	accepted bool,
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
	t.Helper()
	server, err := NewServer(Config{
		HeartbeatInterval: time.Hour,
		MaxPending:        5 * time.Second,
	}, database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(), map[string]dialect.Codec{
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
	_, body := checkpointChatRequest(t, database, "request-restart", true)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	workerHub := hub.New(4)
	server, cancel := newRecoveryServer(t, database, workerHub)
	t.Cleanup(cancel)

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
	if _, err := database.LookupWorkerEventReceipt(context.Background(), storeapi.RequestKey{
		CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
	}, finalEvent.ID, finalDigest); err != nil {
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

func TestRestartRecoveryAssignsAdmittedTaskToFirstWorker(t *testing.T) {
	t.Parallel()
	database := openRecoveryDatabase(t)
	checkpointChatRequest(t, database, "request-admitted", false)
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

func TestRestartRecoveryAfterCommitted200BeforeEnqueue(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "committed-200.db")
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	_, body := checkpointChatRequest(t, database, "request-committed-200", false)
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
	request, body := checkpointChatRequest(t, database, "request-terminal-step", true)
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

func TestRestartRecoveryFailsExplicitlyForActiveLegacyRequestWithoutCheckpoint(t *testing.T) {
	t.Parallel()
	database := openRecoveryDatabase(t)
	request, err := openai.New().Decode(chatBody("legacy", false))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	created, err := database.BeginRequest(context.Background(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "legacy-task"},
			CapabilityTier: completion.TierChat, Dialect: canonical.DialectOpenAIChat,
		},
		IdempotencyKey: "legacy-request", RequestDigest: digest, CanonicalRequest: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionTask(context.Background(), created.Task.TaskKey, completion.StateAdmitted, completion.StateLeased, "legacy-worker"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionTask(context.Background(), created.Task.TaskKey, completion.StateLeased, completion.StateAwaitingHuman, ""); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(Config{}, database, fixedAuthenticator{}, hub.New(2), adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = server.Recover(context.Background())
	if !errors.Is(err, storeapi.ErrUnrecoverableRequest) || !strings.Contains(err.Error(), "legacy-request") {
		t.Fatalf("legacy recovery error = %v", err)
	}
}

func TestRecoveryFailsClosedForStreamCheckpointWithoutClockSeed(t *testing.T) {
	database := openRecoveryDatabase(t)
	body := chatBody("legacy stream seed", false)
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
			TaskKey:        storeapi.TaskKey{CallerID: "caller-1", TaskID: "legacy-seed-task"},
			CapabilityTier: completion.TierChat,
			Dialect:        canonical.DialectOpenAIChat,
		},
		IdempotencyKey: "legacy-seed-request", RequestDigest: digest, CanonicalRequest: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	legacyMetadata, err := json.Marshal(map[string]string{
		"response_id": "chatcmpl_legacy_seed", "model": request.Model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendResponseEvent(
		context.Background(), created.Request.RequestKey, responseEventStream, legacyMetadata,
	); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(Config{}, database, fixedAuthenticator{}, hub.New(2), adapter.NewRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = server.Recover(context.Background())
	if !errors.Is(err, storeapi.ErrUnrecoverableRequest) || !strings.Contains(err.Error(), "invalid stream metadata") {
		t.Fatalf("legacy stream seed recovery error = %v", err)
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
