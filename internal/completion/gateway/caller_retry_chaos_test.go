package gateway

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	storeapi "github.com/vibe-agi/human/internal/store"
)

// openCallerRetry deliberately gives every attempt a fresh transport. Closing
// its streaming body therefore tears down the TCP connection instead of
// merely abandoning a read while a pooled connection remains reusable.
func openCallerRetry(
	t *testing.T,
	fixture *gatewayFixture,
	body []byte,
	idempotencyKey string,
) (*http.Response, *http.Transport) {
	t.Helper()
	transport := &http.Transport{DisableKeepAlives: true}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	response, err := client.Do(newChatRequest(t, fixture, body, idempotencyKey))
	if err != nil {
		transport.CloseIdleConnections()
		t.Fatalf("open caller retry: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		response.Body.Close()
		transport.CloseIdleConnections()
		t.Fatalf("caller retry status = %d, body = %q", response.StatusCode, payload)
	}
	return response, transport
}

func closeCallerRetry(response *http.Response, transport *http.Transport) {
	_ = response.Body.Close()
	transport.CloseIdleConnections()
}

func readCallerRetryThrough(t *testing.T, response *http.Response, marker []byte) []byte {
	t.Helper()
	reader := bufio.NewReader(response.Body)
	var observed bytes.Buffer
	for !bytes.Contains(observed.Bytes(), marker) {
		line, err := reader.ReadBytes('\n')
		observed.Write(line)
		if err != nil {
			t.Fatalf("caller retry ended before marker %q: read %q: %v", marker, observed.Bytes(), err)
		}
	}
	return observed.Bytes()
}

func assertNoRetryAssignment(t *testing.T, workerAssignments <-chan completion.Assignment) {
	t.Helper()
	select {
	case duplicate := <-workerAssignments:
		t.Fatalf("caller retry dispatched a duplicate assignment: %+v", duplicate)
	case <-time.After(25 * time.Millisecond):
	}
}

// This is the caller-facing complement to the worker reconnect tests. Five
// separate TCP connections carrying one explicit idempotency key disappear at
// increasingly late points in one SSE response, and a sixth connection resumes
// the same durable request. Vendor-specific no-key behavior is tested separately.
func TestCallerFiveTCPDisconnectsThenExactIdempotentRecovery(t *testing.T) {
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval: time.Hour,
		MaxPending:        5 * time.Second,
	})
	body := chatBody("survive five caller disconnects", false)
	const key = "caller-five-retries"

	// Attempt 1 observes only the durable HTTP decision and closes without
	// reading an SSE frame.
	response, transport := openCallerRetry(t, fixture, body, key)
	if response.Header.Get(headerIdempotencyKey) != key {
		t.Fatalf("attempt 1 idempotency header = %q", response.Header.Get(headerIdempotencyKey))
	}
	taskID := response.Header.Get(headerTaskID)
	if taskID == "" {
		t.Fatal("attempt 1 omitted task id")
	}
	closeCallerRetry(response, transport)

	assignment := waitAssignment(t, fixture.worker)
	if assignment.IdempotencyKey != key || assignment.TaskID != taskID {
		t.Fatalf("assignment identity = %+v; want key %q task %q", assignment, key, taskID)
	}
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
		ID: "retry-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID,
	}); err != nil {
		t.Fatal(err)
	}
	assertNoRetryAssignment(t, fixture.worker.Assignments)

	// Attempt 2 consumes the initial assistant-role frame and then drops the
	// connection. It must replay the same task without another assignment.
	response, transport = openCallerRetry(t, fixture, body, key)
	if response.Header.Get(headerTaskID) != taskID {
		t.Fatalf("attempt 2 task id = %q, want %q", response.Header.Get(headerTaskID), taskID)
	}
	start := readCallerRetryThrough(t, response, []byte(`"role":"assistant"`))
	if !bytes.Contains(start, []byte("data: ")) {
		t.Fatalf("attempt 2 did not receive an SSE frame: %q", start)
	}
	closeCallerRetry(response, transport)
	assertNoRetryAssignment(t, fixture.worker.Assignments)

	progressMarkers := []string{"RETRY_PROGRESS_ONE", "RETRY_PROGRESS_TWO", "RETRY_PROGRESS_THREE"}
	for index, marker := range progressMarkers {
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
			ID:   "retry-progress-" + marker,
			Type: completion.EventProgress, Text: marker,
		}); err != nil {
			t.Fatalf("publish progress %d: %v", index+1, err)
		}
		response, transport = openCallerRetry(t, fixture, body, key)
		if response.Header.Get(headerTaskID) != taskID || response.Header.Get(headerIdempotencyKey) != key {
			t.Fatalf("attempt %d identity headers = task %q, key %q", index+3,
				response.Header.Get(headerTaskID), response.Header.Get(headerIdempotencyKey))
		}
		readCallerRetryThrough(t, response, []byte(marker))
		closeCallerRetry(response, transport)
		assertNoRetryAssignment(t, fixture.worker.Assignments)
	}

	// Attempt 6 replays every durable byte and stays connected for the sole
	// terminal event.
	response, transport = openCallerRetry(t, fixture, body, key)
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, key, completion.Event{
		ID: "retry-final", Type: completion.EventFinal, Text: "RETRY_FINAL_OK",
	}); err != nil {
		closeCallerRetry(response, transport)
		t.Fatal(err)
	}
	finalBody, err := io.ReadAll(response.Body)
	closeCallerRetry(response, transport)
	if err != nil {
		t.Fatal(err)
	}

	requestKey := storeapi.RequestKey{CallerID: assignment.CallerID, IdempotencyKey: key}
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
		t.Fatalf("sixth attempt was not an exact replay\nresponse: %q\ndurable:  %q", finalBody, durableWire.Bytes())
	}
	for _, marker := range append(progressMarkers, "RETRY_FINAL_OK") {
		if count := bytes.Count(finalBody, []byte(marker)); count != 1 {
			t.Fatalf("marker %q count = %d, response = %q", marker, count, finalBody)
		}
	}
	if count := bytes.Count(finalBody, []byte("data: [DONE]")); count != 1 {
		t.Fatalf("terminal sentinel count = %d, response = %q", count, finalBody)
	}
	assertSingleWorkerEventStages(t, fixture.db, requestKey, "retry-final")
	lookup, err := fixture.db.LookupRequest(context.Background(), requestKey, mustRequestDigest(t, body))
	if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateCompleted {
		t.Fatalf("final durable state = %+v, err = %v", lookup, err)
	}
	assertNoRetryAssignment(t, fixture.worker.Assignments)
}

// Generic Chat has no protocol signal from which the gateway can infer that
// no-key retries belong to one logical turn. Lock that limitation down
// explicitly: five disconnected POSTs are five independently dispatched and
// terminated tasks. The narrow Codex Responses turn adapter has separate tests.
func TestCallerFiveRetriesWithoutIdempotencyKeyAreIndependentRequests(t *testing.T) {
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval: time.Hour,
		MaxPending:        5 * time.Second,
	})
	body := chatBody("same bytes but no stable retry key", false)
	keys := make(map[string]struct{}, 5)
	tasks := make(map[string]struct{}, 5)

	for attempt := 1; attempt <= 5; attempt++ {
		response, transport := openCallerRetry(t, fixture, body, "")
		generatedKey := response.Header.Get(headerIdempotencyKey)
		taskID := response.Header.Get(headerTaskID)
		closeCallerRetry(response, transport)
		if generatedKey == "" || taskID == "" {
			t.Fatalf("attempt %d generated identity = key %q, task %q", attempt, generatedKey, taskID)
		}
		if _, duplicate := keys[generatedKey]; duplicate {
			t.Fatalf("attempt %d reused generated key %q", attempt, generatedKey)
		}
		if _, duplicate := tasks[taskID]; duplicate {
			t.Fatalf("attempt %d reused generated task %q", attempt, taskID)
		}
		keys[generatedKey] = struct{}{}
		tasks[taskID] = struct{}{}

		assignment := waitAssignment(t, fixture.worker)
		if assignment.IdempotencyKey != generatedKey || assignment.TaskID != taskID {
			t.Fatalf("attempt %d assignment = %+v; want key %q task %q",
				attempt, assignment, generatedKey, taskID)
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, generatedKey, completion.Event{
			ID:   "unkeyed-accepted-" + generatedKey,
			Type: completion.EventAccepted, WorkerID: fixture.worker.ID,
		}); err != nil {
			t.Fatal(err)
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, generatedKey, completion.Event{
			ID:   "unkeyed-final-" + generatedKey,
			Type: completion.EventFinal, Text: "terminated independent retry",
		}); err != nil {
			t.Fatal(err)
		}
		requestKey := storeapi.RequestKey{CallerID: assignment.CallerID, IdempotencyKey: generatedKey}
		lookup, err := fixture.db.LookupRequest(context.Background(), requestKey, mustRequestDigest(t, body))
		if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateCompleted {
			t.Fatalf("attempt %d durable state = %+v, err = %v", attempt, lookup, err)
		}
	}

	if len(keys) != 5 || len(tasks) != 5 {
		t.Fatalf("unkeyed retries = %d keys, %d tasks; want five independent requests", len(keys), len(tasks))
	}
	assertNoRetryAssignment(t, fixture.worker.Assignments)
}
