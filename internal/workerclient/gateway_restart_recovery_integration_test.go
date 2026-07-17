package workerclient

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	storeapi "github.com/vibe-agi/human/internal/store"
)

// TestGatewayRestartWithLiveWorkerRecoversPartialCallerAndOfflineOutbox fills
// the service-restart gap between the focused caller/worker retry tests and
// the full three-process reopen test. The caller has already observed partial
// SSE, gateway+SQLite are restarted, but the worker process stays alive: it must
// reconnect by itself and replay the final event written while gateway was
// offline. A same-key caller retry must then see one exact logical response.
func TestGatewayRestartWithLiveWorkerRecoversPartialCallerAndOfflineOutbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "gateway.db")
	outboxPath := filepath.Join(directory, "worker-outbox.db")
	daemon := startLiveDaemon(t, "127.0.0.1:0", databasePath)
	address := daemon.address

	caller := newChaosHTTPClient()
	defer caller.CloseIdleConnections()
	worker, err := dialWithOutbox(
		ctx, daemon.workerURL(), chaosWorkerToken, outboxPath, fastClientRuntimeConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	waitFor(t, ctx, func() bool { return worker.WorkerID() == chaosWorkerID }, "initial worker hello")

	requestBody := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"survive a live-worker gateway restart"}]}`)
	request, err := http.NewRequest(http.MethodPost, daemon.httpURL()+"/v1/chat/completions", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+chaosCallerToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", chaosRequestKey)
	response, err := caller.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("initial caller status = %d, body = %q", response.StatusCode, data)
	}

	assignment := receiveClientAssignment(t, ctx, worker.Messages())
	if assignment.CallerID != chaosCallerID || assignment.IdempotencyKey != chaosRequestKey ||
		assignment.LeaseOwner != chaosWorkerID {
		t.Fatalf("initial assignment = %+v", assignment)
	}
	if err := worker.SendEvent(ctx, assignment, completion.Event{
		ID: "accepted-before-live-worker-restart", Type: completion.EventAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(worker) == 0 }, "accepted ACK before restart")
	const progressText = "progress visible before gateway restart"
	if err := worker.SendEvent(ctx, assignment, completion.Event{
		ID: "progress-before-live-worker-restart", Type: completion.EventProgress, Text: progressText,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(worker) == 0 }, "progress ACK before restart")
	partial := readStreamThrough(t, response.Body, []byte(progressText))
	if bytes.Contains(partial, []byte("data: [DONE]")) {
		t.Fatalf("initial stream completed before restart: %q", partial)
	}

	// Stop the public listener/HTTP stream and the hijacked worker socket, then
	// close the gateway runtime and SQLite. The Client itself remains alive and
	// enters its ordinary bounded reconnect loop.
	daemon.cutNetwork(t)
	closeWorkerConnection(worker)
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	waitFor(t, ctx, func() bool {
		worker.writeMu.Lock()
		defer worker.writeMu.Unlock()
		return worker.connection == nil
	}, "live worker to observe gateway restart")
	daemon.closeCore(t)

	const finalText = "final replayed by the still-running worker"
	if err := worker.SendEvent(ctx, assignment, completion.Event{
		ID: "final-during-live-worker-restart", Type: completion.EventFinal, Text: finalText,
	}); err != nil {
		t.Fatal(err)
	}
	if count := pendingOutboxCount(worker); count != 1 {
		t.Fatalf("offline worker outbox count = %d, want 1", count)
	}
	for attempt := 1; attempt <= 5; attempt++ {
		failed := postRestartCompletion(caller, "http://"+address, requestBody)
		if failed.err == nil {
			t.Fatalf("offline caller retry %d unexpectedly returned status=%d body=%q", attempt, failed.status, failed.body)
		}
	}

	restarted := startLiveDaemon(t, address, databasePath)
	t.Cleanup(func() {
		_ = restarted.httpServer.Close()
		restarted.cancel()
		restarted.gateway.Wait()
		_ = restarted.database.Close()
	})
	recovered, sawDisconnect, sawRestore := receiveRecoveredAssignment(t, ctx, worker.Messages())
	if !sawDisconnect || !sawRestore || recovered.SessionKey() != assignment.SessionKey() ||
		recovered.LeaseOwner != chaosWorkerID {
		t.Fatalf("recovery lost=%t restored=%t assignment=%+v", sawDisconnect, sawRestore, recovered)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(worker) == 0 }, "offline final replay ACK")
	if records, err := worker.outbox.ListRejected(ctx); err != nil || len(records) != 0 {
		t.Fatalf("gateway restart rejection inbox = %d records, err=%v", len(records), err)
	}

	firstReplay := postRestartCompletion(caller, restarted.httpURL(), requestBody)
	secondReplay := postRestartCompletion(caller, restarted.httpURL(), requestBody)
	for index, replay := range []callerAttempt{firstReplay, secondReplay} {
		if replay.err != nil || replay.status != http.StatusOK ||
			bytes.Count(replay.body, []byte(progressText)) != 1 ||
			bytes.Count(replay.body, []byte(finalText)) != 1 ||
			bytes.Count(replay.body, []byte("data: [DONE]")) != 1 {
			t.Fatalf("caller replay %d = status %d body %q err %v", index+1, replay.status, replay.body, replay.err)
		}
	}
	if !bytes.Equal(firstReplay.body, secondReplay.body) {
		t.Fatalf("completed same-key replay changed bytes\nfirst=%q\nsecond=%q", firstReplay.body, secondReplay.body)
	}

	canonicalRequest, err := openai.New().Decode(requestBody)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := canonicalRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := restarted.database.LookupRequest(ctx, storeapi.RequestKey{
		CallerID: chaosCallerID, IdempotencyKey: chaosRequestKey,
	}, digest)
	if err != nil || !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateCompleted ||
		lookup.Task.LeaseOwner != chaosWorkerID || lookup.Task.TaskID != assignment.TaskID {
		t.Fatalf("gateway restart durable state = %+v, err=%v", lookup, err)
	}
	assertGatewayRestartExactlyOnce(t, databasePath, assignment.TaskID)
}

func readStreamThrough(t *testing.T, body io.Reader, marker []byte) []byte {
	t.Helper()
	reader := bufio.NewReader(body)
	var observed bytes.Buffer
	for !bytes.Contains(observed.Bytes(), marker) {
		line, err := reader.ReadBytes('\n')
		observed.Write(line)
		if err != nil {
			t.Fatalf("stream ended before marker %q: body=%q err=%v", marker, observed.Bytes(), err)
		}
	}
	return observed.Bytes()
}

func receiveRecoveredAssignment(
	t *testing.T,
	ctx context.Context,
	messages <-chan Message,
) (completion.Assignment, bool, bool) {
	t.Helper()
	var sawDisconnect, sawRestore bool
	for {
		select {
		case message, open := <-messages:
			if !open {
				t.Fatal("live worker stopped during gateway restart")
			}
			if message.Err != nil {
				if errors.Is(message.Err, ErrWorkerAuthentication) || errors.Is(message.Err, ErrWorkerHandshake) {
					t.Fatalf("transient gateway restart became permanent: %v", message.Err)
				}
				sawDisconnect = true
			}
			if message.ConnectionRestored {
				sawRestore = true
			}
			if message.Assignment != nil {
				return *message.Assignment, sawDisconnect, sawRestore
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for live worker gateway recovery")
		}
	}
}

func postRestartCompletion(client *http.Client, baseURL string, body []byte) callerAttempt {
	request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return callerAttempt{err: err}
	}
	request.Header.Set("Authorization", "Bearer "+chaosCallerToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", chaosRequestKey)
	response, err := client.Do(request)
	if err != nil {
		return callerAttempt{err: err}
	}
	data, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr == nil {
		readErr = closeErr
	}
	return callerAttempt{status: response.StatusCode, body: data, err: readErr}
}

func assertGatewayRestartExactlyOnce(t *testing.T, databasePath, taskID string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		t.Fatal(err)
	}
	var requests, tasks, receipts, distinctReceipts int
	if err := database.QueryRow(`
		SELECT COUNT(*) FROM completion_requests
		WHERE caller_id = ? AND idempotency_key = ?`, chaosCallerID, chaosRequestKey,
	).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow(`
		SELECT COUNT(*) FROM completion_tasks
		WHERE caller_id = ? AND task_id = ?`, chaosCallerID, taskID,
	).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow(`
		SELECT COUNT(*), COUNT(DISTINCT event_id)
		FROM completion_worker_event_receipts
		WHERE caller_id = ? AND idempotency_key = ?`, chaosCallerID, chaosRequestKey,
	).Scan(&receipts, &distinctReceipts); err != nil {
		t.Fatal(err)
	}
	if requests != 1 || tasks != 1 || receipts != 3 || distinctReceipts != 3 {
		t.Fatalf("restart durable counts requests/tasks/receipts/distinct = %d/%d/%d/%d, want 1/1/3/3",
			requests, tasks, receipts, distinctReceipts)
	}
}
