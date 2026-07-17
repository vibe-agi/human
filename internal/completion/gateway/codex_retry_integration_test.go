package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect/responses"
	storeapi "github.com/vibe-agi/human/internal/store"
)

func openCodexResponsesRetry(
	t *testing.T,
	fixture *gatewayFixture,
	body []byte,
	metadata string,
) (*http.Response, *http.Transport) {
	t.Helper()
	request, err := http.NewRequest(
		http.MethodPost, fixture.server.URL+"/v1/responses", bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer hae_test")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "codex_exec/0.144.0")
	request.Header.Set(headerCodexTurnMetadata, metadata)
	transport := &http.Transport{DisableKeepAlives: true}
	response, err := (&http.Client{Transport: transport, Timeout: 2 * time.Second}).Do(request)
	if err != nil {
		transport.CloseIdleConnections()
		t.Fatalf("open Codex Responses retry: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		response.Body.Close()
		transport.CloseIdleConnections()
		t.Fatalf("Codex Responses retry status = %d, body = %q", response.StatusCode, payload)
	}
	return response, transport
}

func responsesRequestDigest(t *testing.T, body []byte) string {
	t.Helper()
	request, err := responses.New().Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func TestCodexResponsesFiveUnkeyedDisconnectsRecoverOneDerivedRequest(t *testing.T) {
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval: time.Hour,
		MaxPending:        5 * time.Second,
	})
	metadata := testCodexMetadata(testCodexTurnID)
	bodyA := []byte(`{
      "model":"human-expert",
      "stream":true,
      "input":"recover this Codex turn",
      "metadata":{"fixture":"redacted"},
	  "client_metadata":{"z":2,"a":1}
    }`)
	bodyB := []byte(`{"client_metadata":{"a":1,"z":2},"metadata":{"fixture":"redacted"},"input":"recover this Codex turn","stream":true,"model":"human-expert"}`)
	bodies := [][]byte{bodyA, bodyB, bodyA, bodyB, bodyA, bodyB}

	// The first socket crosses the durable HTTP boundary and disappears before
	// consuming a frame. No explicit Idempotency-Key is present.
	response, transport := openCodexResponsesRetry(t, fixture, bodies[0], metadata)
	derivedKey := response.Header.Get(headerIdempotencyKey)
	taskID := response.Header.Get(headerTaskID)
	if !bytes.HasPrefix([]byte(derivedKey), []byte(codexDerivedIdempotencyPrefix)) || taskID == "" {
		t.Fatalf("derived response identity = key %q, task %q", derivedKey, taskID)
	}
	closeCallerRetry(response, transport)

	assignment := waitAssignment(t, fixture.worker)
	if assignment.IdempotencyKey != derivedKey || assignment.TaskID != taskID ||
		assignment.Request.Dialect != canonical.DialectResponses {
		t.Fatalf("Codex assignment identity = %+v", assignment)
	}
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, derivedKey, completion.Event{
		ID: "codex-retry-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	assertNoRetryAssignment(t, fixture.worker.Assignments)

	// Four more fresh TCP connections use semantically identical JSON with a
	// different object-key order. Every response must expose the same derived
	// key and task, and none may redispatch work.
	for attempt := 1; attempt < 5; attempt++ {
		response, transport = openCodexResponsesRetry(t, fixture, bodies[attempt], metadata)
		if gotKey, gotTask := response.Header.Get(headerIdempotencyKey), response.Header.Get(headerTaskID); gotKey != derivedKey || gotTask != taskID {
			t.Fatalf("attempt %d identity = key %q task %q; want %q %q",
				attempt+1, gotKey, gotTask, derivedKey, taskID)
		}
		readCallerRetryThrough(t, response, []byte(`"type":"response.in_progress"`))
		closeCallerRetry(response, transport)
		assertNoRetryAssignment(t, fixture.worker.Assignments)
	}

	// The sixth socket stays connected. Its final bytes must equal the complete
	// durable wire transcript, proving that derived retries share the ordinary
	// exact-replay machinery rather than an approximate response cache.
	response, transport = openCodexResponsesRetry(t, fixture, bodies[5], metadata)
	if gotKey, gotTask := response.Header.Get(headerIdempotencyKey), response.Header.Get(headerTaskID); gotKey != derivedKey || gotTask != taskID {
		t.Fatalf("recovery identity = key %q task %q; want %q %q", gotKey, gotTask, derivedKey, taskID)
	}
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, derivedKey, completion.Event{
		ID: "codex-retry-final", Type: completion.EventFinal, Text: "CODEX_DERIVED_RETRY_FINAL",
	}); err != nil {
		closeCallerRetry(response, transport)
		t.Fatal(err)
	}
	finalBody, err := io.ReadAll(response.Body)
	closeCallerRetry(response, transport)
	if err != nil {
		t.Fatal(err)
	}

	requestKey := storeapi.RequestKey{CallerID: assignment.CallerID, IdempotencyKey: derivedKey}
	events, err := fixture.db.ListResponseEvents(context.Background(), requestKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	var durableWire bytes.Buffer
	for _, event := range events {
		wire, wireErr := responseEventWireData(event)
		if wireErr != nil {
			t.Fatal(wireErr)
		}
		durableWire.Write(wire)
	}
	if !bytes.Equal(finalBody, durableWire.Bytes()) {
		t.Fatalf("sixth Codex attempt was not an exact replay\nresponse: %q\ndurable:  %q",
			finalBody, durableWire.Bytes())
	}
	if count := bytes.Count(finalBody, []byte(`"type":"response.completed"`)); count != 1 {
		t.Fatalf("response.completed count = %d, body = %q", count, finalBody)
	}
	if !bytes.Contains(finalBody, []byte("CODEX_DERIVED_RETRY_FINAL")) {
		t.Fatalf("final response omitted expert text: %q", finalBody)
	}
	assertSingleWorkerEventStages(t, fixture.db, requestKey, "codex-retry-final")
	lookup, err := fixture.db.LookupRequest(context.Background(), requestKey, responsesRequestDigest(t, bodyA))
	if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateCompleted ||
		lookup.Task.TaskID != taskID {
		t.Fatalf("single durable Codex task = %+v, err = %v", lookup, err)
	}
	assertNoRetryAssignment(t, fixture.worker.Assignments)
}

func TestDirectChatCannotSelectStableTaskNamespace(t *testing.T) {
	fixture := newGatewayFixture(t, true)
	request := newChatRequest(t, fixture, chatBody("direct Basic task isolation", false), "direct-chat-key")
	request.Header.Set(headerCapabilityTier, string(completion.TierChat))
	request.Header.Set(headerTaskID, "caller-selected-task")

	workerDone := make(chan error, 1)
	go func() {
		var assignment completion.Assignment
		select {
		case assignment = <-fixture.worker.Assignments:
		case <-time.After(time.Second):
			workerDone <- errors.New("worker did not receive direct Chat assignment")
			return
		}
		if assignment.TaskID == "caller-selected-task" || assignment.TaskID == "" {
			workerDone <- fmt.Errorf("direct Chat retained caller-selected task identity %q", assignment.TaskID)
			return
		}
		if err := fixture.hub.Publish(
			context.Background(), assignment.CallerID, assignment.IdempotencyKey,
			completion.Event{ID: "direct-chat-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID},
		); err != nil {
			workerDone <- err
			return
		}
		workerDone <- fixture.hub.Publish(
			context.Background(), assignment.CallerID, assignment.IdempotencyKey,
			completion.Event{ID: "direct-chat-final", Type: completion.EventFinal, Text: "isolated"},
		)
	}()

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get(headerTaskID) == "caller-selected-task" ||
		response.Header.Get(headerTaskID) == "" {
		t.Fatalf("direct Chat response identity = status %d task %q",
			response.StatusCode, response.Header.Get(headerTaskID))
	}
	if _, err := fixture.db.GetTask(context.Background(), storeapi.TaskKey{
		CallerID: "caller-1", TaskID: "caller-selected-task",
	}); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("direct Chat caller-selected task crossed admission: %v", err)
	}
}

func TestGatewayFailsClosedOnRecognizedCodexMetadataButExplicitKeyWins(t *testing.T) {
	body := []byte(`{"model":"human-expert","stream":true,"input":"metadata gate"}`)
	newRequest := func(t *testing.T, fixture *gatewayFixture) *http.Request {
		t.Helper()
		request, err := http.NewRequest(
			http.MethodPost, fixture.server.URL+"/v1/responses", bytes.NewReader(body),
		)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer hae_test")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("User-Agent", "codex_exec/0.144.0")
		request.Header.Add(headerCodexTurnMetadata, testCodexMetadata(testCodexTurnID))
		request.Header.Add(headerCodexTurnMetadata, testCodexMetadata(testCodexTurnID))
		return request
	}

	t.Run("derived identity rejects ambiguous header", func(t *testing.T) {
		fixture := newGatewayFixture(t, true)
		response, err := http.DefaultClient.Do(newRequest(t, fixture))
		if err != nil {
			t.Fatal(err)
		}
		payload, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode != http.StatusBadRequest ||
			!bytes.Contains(payload, []byte("exactly one header value")) {
			t.Fatalf("ambiguous Codex metadata = %d, %q", response.StatusCode, payload)
		}
		assertNoRetryAssignment(t, fixture.worker.Assignments)
	})

	t.Run("caller key has absolute priority", func(t *testing.T) {
		fixture := newGatewayFixture(t, true)
		request := newRequest(t, fixture)
		request.Header.Set(headerIdempotencyKey, "explicit-codex-key")
		workerDone := make(chan error, 1)
		go func() {
			assignment := <-fixture.worker.Assignments
			if assignment.IdempotencyKey != "explicit-codex-key" {
				workerDone <- fmt.Errorf("explicit assignment key = %q", assignment.IdempotencyKey)
				return
			}
			if err := fixture.hub.Publish(
				context.Background(), assignment.CallerID, assignment.IdempotencyKey,
				completion.Event{ID: "explicit-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID},
			); err != nil {
				workerDone <- err
				return
			}
			workerDone <- fixture.hub.Publish(
				context.Background(), assignment.CallerID, assignment.IdempotencyKey,
				completion.Event{ID: "explicit-final", Type: completion.EventFinal, Text: "explicit wins"},
			)
		}()
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if err := <-workerDone; err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK ||
			response.Header.Get(headerIdempotencyKey) != "explicit-codex-key" {
			t.Fatalf("explicit Codex response = %d, key %q",
				response.StatusCode, response.Header.Get(headerIdempotencyKey))
		}
	})
}

func TestCodexDerivedIdempotencyThirtyConcurrentAndSequentialReplaysDispatchOnce(t *testing.T) {
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval: time.Hour,
		MaxPending:        5 * time.Second,
	})
	metadata := testCodexMetadata(testCodexTurnID)
	bodyA := []byte(`{"model":"human-expert","stream":true,"input":"concurrent replay","client_metadata":{"b":2,"a":1}}`)
	bodyB := []byte(`{"client_metadata":{"a":1,"b":2},"input":"concurrent replay","stream":true,"model":"human-expert"}`)

	initial, initialTransport := openCodexResponsesRetry(t, fixture, bodyA, metadata)
	key := initial.Header.Get(headerIdempotencyKey)
	taskID := initial.Header.Get(headerTaskID)
	closeCallerRetry(initial, initialTransport)
	assignment := waitAssignment(t, fixture.worker)
	if assignment.IdempotencyKey != key || assignment.TaskID != taskID {
		t.Fatalf("initial derived assignment = %+v", assignment)
	}
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
		ID: "concurrent-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID,
	}); err != nil {
		t.Fatal(err)
	}

	type openedRetry struct {
		status int
		key    string
		taskID string
		err    error
	}
	type completedRetry struct {
		body []byte
		err  error
	}
	const concurrentRetries = 30
	opened := make(chan openedRetry, concurrentRetries)
	completed := make(chan completedRetry, concurrentRetries)
	for index := 0; index < concurrentRetries; index++ {
		body := bodyA
		if index%2 != 0 {
			body = bodyB
		}
		go func() {
			request, err := http.NewRequest(
				http.MethodPost, fixture.server.URL+"/v1/responses", bytes.NewReader(body),
			)
			if err != nil {
				opened <- openedRetry{err: err}
				completed <- completedRetry{err: err}
				return
			}
			request.Header.Set("Authorization", "Bearer hae_test")
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("User-Agent", "codex_exec/0.144.0")
			request.Header.Set(headerCodexTurnMetadata, metadata)
			response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
			if err != nil {
				opened <- openedRetry{err: err}
				completed <- completedRetry{err: err}
				return
			}
			opened <- openedRetry{
				status: response.StatusCode,
				key:    response.Header.Get(headerIdempotencyKey),
				taskID: response.Header.Get(headerTaskID),
			}
			payload, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			completed <- completedRetry{body: payload, err: readErr}
		}()
	}

	for index := 0; index < concurrentRetries; index++ {
		result := <-opened
		if result.err != nil || result.status != http.StatusOK || result.key != key || result.taskID != taskID {
			t.Fatalf("concurrent retry %d opened as %+v; want status 200 key %q task %q",
				index+1, result, key, taskID)
		}
	}
	assertNoRetryAssignment(t, fixture.worker.Assignments)
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
		ID: "concurrent-final", Type: completion.EventFinal, Text: "THIRTY_REPLAYS_ONE_FINAL",
	}); err != nil {
		t.Fatal(err)
	}

	var expected []byte
	for index := 0; index < concurrentRetries; index++ {
		result := <-completed
		if result.err != nil {
			t.Fatalf("concurrent retry %d completed: %v", index+1, result.err)
		}
		if expected == nil {
			expected = result.body
		} else if !bytes.Equal(result.body, expected) {
			t.Fatalf("concurrent retry %d did not receive exact shared response", index+1)
		}
	}
	if !bytes.Contains(expected, []byte("THIRTY_REPLAYS_ONE_FINAL")) {
		t.Fatalf("concurrent response omitted final: %q", expected)
	}

	sequential, sequentialTransport := openCodexResponsesRetry(t, fixture, bodyB, metadata)
	sequentialBody, err := io.ReadAll(sequential.Body)
	closeCallerRetry(sequential, sequentialTransport)
	if err != nil {
		t.Fatal(err)
	}
	if sequential.Header.Get(headerIdempotencyKey) != key ||
		sequential.Header.Get(headerTaskID) != taskID || !bytes.Equal(sequentialBody, expected) {
		t.Fatalf("sequential replay identity/body drifted: key %q task %q body %q",
			sequential.Header.Get(headerIdempotencyKey), sequential.Header.Get(headerTaskID), sequentialBody)
	}
	assertNoRetryAssignment(t, fixture.worker.Assignments)
}

func TestCodexAutoIdempotencyKillSwitchRestoresRandomBasicRequests(t *testing.T) {
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval:           time.Hour,
		MaxPending:                  5 * time.Second,
		DisableCodexAutoIdempotency: true,
	})
	body := []byte(`{"model":"human-expert","stream":true,"input":"kill switch"}`)
	metadata := testCodexMetadata(testCodexTurnID)
	keys := make(map[string]struct{}, 2)
	tasks := make(map[string]struct{}, 2)
	for attempt := 0; attempt < 2; attempt++ {
		response, transport := openCodexResponsesRetry(t, fixture, body, metadata)
		key := response.Header.Get(headerIdempotencyKey)
		taskID := response.Header.Get(headerTaskID)
		closeCallerRetry(response, transport)
		if key == "" || strings.HasPrefix(key, codexDerivedIdempotencyPrefix) || taskID == "" {
			t.Fatalf("kill-switch attempt %d identity = key %q task %q", attempt+1, key, taskID)
		}
		if _, duplicate := keys[key]; duplicate {
			t.Fatalf("kill switch reused random key %q", key)
		}
		if _, duplicate := tasks[taskID]; duplicate {
			t.Fatalf("kill switch reused Basic task %q", taskID)
		}
		keys[key] = struct{}{}
		tasks[taskID] = struct{}{}
		assignment := waitAssignment(t, fixture.worker)
		if assignment.IdempotencyKey != key || assignment.TaskID != taskID {
			t.Fatalf("kill-switch assignment %d = %+v", attempt+1, assignment)
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
			ID: "kill-switch-accepted-" + key, Type: completion.EventAccepted, WorkerID: fixture.worker.ID,
		}); err != nil {
			t.Fatal(err)
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
			ID: "kill-switch-final-" + key, Type: completion.EventFinal, Text: "independent",
		}); err != nil {
			t.Fatal(err)
		}
	}
}
