package workerclient

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/gateway"
	"github.com/vibe-agi/human/internal/completion/hub"
	"github.com/vibe-agi/human/internal/ratelimit"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/store/sqlite"
	"github.com/vibe-agi/human/internal/workerws"
)

const (
	chaosCallerToken     = "hae_caller_three_party_recovery"
	chaosWorkerToken     = "hae_worker_three_party_recovery"
	chaosCallerID        = "caller-three-party-recovery"
	chaosWorkerID        = "worker-three-party-recovery"
	chaosUntrustedTaskID = "untrusted-basic-task-header"
	chaosRequestKey      = "request-three-party-recovery"
)

type threePartyAuthenticator struct{}

func (threePartyAuthenticator) AuthenticateRequest(request *http.Request) (auth.Principal, error) {
	token := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	switch token {
	case chaosCallerToken:
		return auth.Principal{
			Type: auth.PrincipalCaller, SubjectID: chaosCallerID, KeyID: "key-caller-recovery",
		}, nil
	case chaosWorkerToken:
		return auth.Principal{
			Type: auth.PrincipalWorker, SubjectID: chaosWorkerID, KeyID: "key-worker-recovery",
		}, nil
	default:
		return auth.Principal{}, auth.ErrUnauthorized
	}
}

// liveDaemon is intentionally assembled from the production gateway,
// worker-WebSocket server, TCP listener, and on-disk SQLite store. It provides
// a crash boundary without relying on httptest's changing port or in-memory
// state, so a worker outbox namespace and caller retry really target the same
// endpoint after restart.
type liveDaemon struct {
	address        string
	database       *sqlite.Store
	gateway        *gateway.Server
	httpServer     *http.Server
	cancel         context.CancelFunc
	serveDone      chan error
	activeHandlers atomic.Int64
}

func startLiveDaemon(t *testing.T, address, databasePath string) *liveDaemon {
	t.Helper()
	ctx := context.Background()
	database, err := sqlite.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	workerHub := hub.New(8)
	authenticator := threePartyAuthenticator{}
	workerServer, err := workerws.New(workerws.Config{
		PingInterval: 100 * time.Millisecond,
		// Keep failure detection quick without treating a healthy connection as
		// half-open when the race detector or parallel package tests briefly
		// delay the pong goroutine.
		PingTimeout: time.Second,
	}, authenticator, workerHub, database)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	gatewayServer, err := gateway.NewServer(gateway.Config{
		HeartbeatInterval: time.Hour,
		MaxPending:        30 * time.Second,
		RateLimit: ratelimit.Config{
			RatePerSecond: 10_000,
			Burst:         10_000,
		},
	}, database, authenticator, workerHub, adapter.NewDefaultRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
	})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	runContext, cancel := context.WithCancel(context.Background())
	if err := gatewayServer.Recover(runContext); err != nil {
		cancel()
		_ = database.Close()
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		cancel()
		gatewayServer.Wait()
		_ = database.Close()
		t.Fatal(err)
	}
	instance := &liveDaemon{
		address: listener.Addr().String(), database: database, gateway: gatewayServer,
		cancel: cancel, serveDone: make(chan error, 1),
	}
	mux := http.NewServeMux()
	mux.Handle("/internal/v1/worker/ws", workerServer)
	mux.Handle("/", gatewayServer)
	tracked := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		instance.activeHandlers.Add(1)
		defer instance.activeHandlers.Add(-1)
		mux.ServeHTTP(response, request)
	})
	instance.httpServer = &http.Server{
		Handler: tracked,
		// These production-like timeouts do not participate in the injected
		// outage, but keep a failed test from leaking a half-open connection.
		ReadHeaderTimeout: time.Second,
		IdleTimeout:       time.Second,
	}
	go func() {
		instance.serveDone <- instance.httpServer.Serve(listener)
	}()
	return instance
}

func (daemon *liveDaemon) httpURL() string { return "http://" + daemon.address }
func (daemon *liveDaemon) workerURL() string {
	return "ws://" + daemon.address + "/internal/v1/worker/ws"
}

// cutNetwork models the service/caller half of an abrupt loss: the listener
// and active HTTP streams disappear without an application-level terminal
// frame. The test cuts the hijacked worker TCP socket separately because
// net/http.Server.Close deliberately does not own hijacked connections.
func (daemon *liveDaemon) cutNetwork(t *testing.T) {
	t.Helper()
	if err := daemon.httpServer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-daemon.serveDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("serve after injected outage: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP server did not stop after injected outage")
	}
}

func (daemon *liveDaemon) closeCore(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for daemon.activeHandlers.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d network handlers remained after outage", daemon.activeHandlers.Load())
		}
		time.Sleep(time.Millisecond)
	}
	daemon.cancel()
	daemon.gateway.Wait()
	if err := daemon.database.Close(); err != nil {
		t.Fatal(err)
	}
}

type callerAttempt struct {
	status int
	body   []byte
	err    error
}

func postChaosCompletion(client *http.Client, baseURL string) callerAttempt {
	return postChaosCompletionStarted(client, baseURL, nil)
}

func postChaosCompletionStarted(
	client *http.Client,
	baseURL string,
	started chan<- int,
) callerAttempt {
	body := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"survive a three-party outage"}]}`)
	request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return callerAttempt{err: err}
	}
	request.Header.Set("Authorization", "Bearer "+chaosCallerToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Human-Task-Id", chaosUntrustedTaskID)
	request.Header.Set("Idempotency-Key", chaosRequestKey)
	response, err := client.Do(request)
	if err != nil {
		return callerAttempt{err: err}
	}
	if started != nil {
		started <- response.StatusCode
	}
	payload, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr == nil {
		readErr = closeErr
	}
	return callerAttempt{status: response.StatusCode, body: payload, err: readErr}
}

func newChaosHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   10 * time.Second,
	}
}

// TestThreePartyOutageRecoversExactlyOnce covers the failure mode which a long
// healthy soak cannot: caller stream loss, gateway+SQLite restart, and worker
// process loss all overlap. The final human event is produced while gateway is
// offline, survives a worker process reopen, and resolves five concurrent
// same-key caller retries through one recovered request/session.
func TestThreePartyOutageRecoversExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "gateway.db")
	outboxPath := filepath.Join(directory, "worker-outbox.db")
	daemon := startLiveDaemon(t, "127.0.0.1:0", databasePath)
	address := daemon.address
	caller := newChaosHTTPClient()
	defer caller.CloseIdleConnections()

	worker, err := DialWithOutbox(ctx, daemon.workerURL(), chaosWorkerToken, outboxPath)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return worker.WorkerID() == chaosWorkerID }, "initial worker hello")

	firstResult := make(chan callerAttempt, 1)
	go func() { firstResult <- postChaosCompletion(caller, daemon.httpURL()) }()
	assignment := receiveClientAssignment(t, ctx, worker.Messages())
	if assignment.CallerID != chaosCallerID || assignment.TaskID == "" || assignment.TaskID == chaosUntrustedTaskID ||
		assignment.IdempotencyKey != chaosRequestKey || assignment.LeaseOwner != chaosWorkerID {
		t.Fatalf("initial assignment = %+v", assignment)
	}
	if err := worker.SendEvent(ctx, assignment, completion.Event{
		ID: "accepted-before-three-party-outage", Type: completion.EventAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	// An empty outbox proves that Accepted crossed the server's durable receipt
	// boundary before the crash. Recovery must therefore restore awaiting_human
	// with the same sticky worker rather than re-admitting a new task.
	waitFor(t, ctx, func() bool { return pendingOutboxCount(worker) == 0 }, "durable accepted ACK")

	daemon.cutNetwork(t)
	// WebSockets are hijacked connections, so net/http.Server.Close cannot see
	// or close them. Cutting this actual client TCP socket is the worker-side
	// half of the injected network partition (and makes the handler unwind just
	// as it would after a proxy/NAT reset).
	worker.writeMu.Lock()
	connection := worker.connection
	if connection != nil {
		_ = connection.CloseNow()
	}
	worker.writeMu.Unlock()
	waitFor(t, ctx, func() bool {
		worker.writeMu.Lock()
		defer worker.writeMu.Unlock()
		return worker.connection == nil
	}, "worker to observe gateway outage")
	select {
	case first := <-firstResult:
		if first.status != http.StatusOK || bytes.Contains(first.body, []byte("[DONE]")) ||
			bytes.Contains(first.body, []byte("survived exactly once")) {
			t.Fatalf("interrupted caller unexpectedly completed: status=%d body=%q err=%v", first.status, first.body, first.err)
		}
	case <-ctx.Done():
		t.Fatal("interrupted caller did not observe the gateway outage")
	}

	// The human finishes while both caller and gateway are offline. SendEvent's
	// success is only the local durable boundary; closing the Client immediately
	// afterwards models the third process disappearing before any reconnect.
	if err := worker.SendEvent(ctx, assignment, completion.Event{
		ID: "final-during-three-party-outage", Type: completion.EventFinal,
		Text: "survived exactly once",
	}); err != nil {
		t.Fatal(err)
	}
	if count := pendingOutboxCount(worker); count != 1 {
		t.Fatalf("offline terminal outbox count = %d, want 1", count)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
	daemon.closeCore(t)

	// Same-key retries while the service is unavailable must be observable as
	// transport failures, not accidental new admissions. Every attempt uses the
	// exact same caller/idempotency identity and canonical request; Basic ignores
	// the untrusted task header and recovers the server-generated task instead.
	for attempt := 1; attempt <= 5; attempt++ {
		failed := postChaosCompletion(caller, "http://"+address)
		if failed.err == nil {
			t.Fatalf("offline caller retry %d unexpectedly returned status=%d body=%q", attempt, failed.status, failed.body)
		}
	}

	// Deliberately restore in the hardest order: caller retries first, then the
	// gateway/store, and the sticky worker process last. Five duplicate streams
	// wait concurrently; none may create or dispatch a second logical request.
	restarted := startLiveDaemon(t, address, databasePath)
	t.Cleanup(func() {
		_ = restarted.httpServer.Close()
		restarted.cancel()
		restarted.gateway.Wait()
		_ = restarted.database.Close()
	})
	const retryCount = 5
	retries := make(chan callerAttempt, retryCount)
	startedRetries := make(chan int, retryCount)
	for index := 0; index < retryCount; index++ {
		go func() {
			retries <- postChaosCompletionStarted(caller, restarted.httpURL(), startedRetries)
		}()
	}
	// Require every caller to cross the recovered durable 200 boundary before
	// the worker is brought back. Completion is impossible at this point.
	for index := 0; index < retryCount; index++ {
		select {
		case status := <-startedRetries:
			if status != http.StatusOK {
				t.Fatalf("recovered retry %d status before worker = %d", index+1, status)
			}
		case <-ctx.Done():
			t.Fatalf("retry %d did not cross durable 200 before worker recovery", index+1)
		}
	}
	select {
	case early := <-retries:
		t.Fatalf("caller retry completed before worker recovery: status=%d body=%q err=%v", early.status, early.body, early.err)
	default:
	}

	reopened, err := DialWithOutbox(ctx, restarted.workerURL(), chaosWorkerToken, outboxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	waitFor(t, ctx, func() bool { return reopened.WorkerID() == chaosWorkerID }, "reopened worker hello")
	recoveredAssignment := receiveClientAssignment(t, ctx, reopened.Messages())
	if recoveredAssignment.SessionKey() != assignment.SessionKey() || recoveredAssignment.LeaseOwner != chaosWorkerID {
		t.Fatalf("recovered assignment = %+v, initial = %+v", recoveredAssignment, assignment)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(reopened) == 0 }, "replayed terminal event ACK")
	if records, err := reopened.outbox.ListRejected(ctx); err != nil || len(records) != 0 {
		t.Fatalf("recovered worker rejection inbox = %d records, err=%v", len(records), err)
	}

	var replay []byte
	for index := 0; index < retryCount; index++ {
		select {
		case result := <-retries:
			if result.err != nil || result.status != http.StatusOK ||
				bytes.Count(result.body, []byte("survived exactly once")) != 1 ||
				bytes.Count(result.body, []byte("data: [DONE]")) != 1 {
				t.Fatalf("recovered caller retry %d: status=%d body=%q err=%v", index+1, result.status, result.body, result.err)
			}
			if replay == nil {
				replay = result.body
			} else if !bytes.Equal(replay, result.body) {
				t.Fatalf("same-key recovered retries changed bytes\nfirst=%q\nnext=%q", replay, result.body)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for recovered caller retry %d", index+1)
		}
	}
	completedReplay := postChaosCompletion(caller, restarted.httpURL())
	if completedReplay.err != nil || completedReplay.status != http.StatusOK || !bytes.Equal(replay, completedReplay.body) {
		t.Fatalf("completed replay changed: status=%d body=%q err=%v", completedReplay.status, completedReplay.body, completedReplay.err)
	}

	requestBody := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"survive a three-party outage"}]}`)
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
	if err != nil {
		t.Fatal(err)
	}
	if !lookup.Request.ResponseComplete || lookup.Task.State != completion.StateCompleted ||
		lookup.Task.LeaseOwner != chaosWorkerID {
		t.Fatalf("recovered durable state = task %+v request %+v", lookup.Task, lookup.Request)
	}
	assertThreePartyExactlyOnce(t, databasePath, assignment.TaskID)

	// A recovered assignment is a single logical delivery. Give the socket a
	// short scheduling window and reject any duplicate that a same-key retry or
	// outbox replay might have created.
	select {
	case message := <-reopened.Messages():
		if message.Assignment != nil {
			t.Fatalf("duplicate recovered assignment = %+v", *message.Assignment)
		}
		if message.Err != nil {
			t.Fatalf("worker connection failed after recovery: %v", message.Err)
		}
	case <-time.After(30 * time.Millisecond):
	}
}

func assertThreePartyExactlyOnce(t *testing.T, databasePath, taskID string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		t.Fatal(err)
	}
	var requestCount, completedCount int
	if err := database.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(response_complete), 0)
		FROM completion_requests
		WHERE caller_id = ? AND idempotency_key = ?`, chaosCallerID, chaosRequestKey,
	).Scan(&requestCount, &completedCount); err != nil {
		t.Fatal(err)
	}
	if requestCount != 1 || completedCount != 1 {
		t.Fatalf("durable requests = %d, completed = %d, want 1/1", requestCount, completedCount)
	}
	var taskCount int
	var taskState, leaseOwner string
	if err := database.QueryRow(`
		SELECT COUNT(*), MIN(state), MIN(lease_owner)
		FROM completion_tasks
		WHERE caller_id = ? AND task_id = ?`, chaosCallerID, taskID,
	).Scan(&taskCount, &taskState, &leaseOwner); err != nil {
		t.Fatal(err)
	}
	if taskCount != 1 || taskState != string(completion.StateCompleted) || leaseOwner != chaosWorkerID {
		t.Fatalf("durable task count/state/owner = %d/%q/%q", taskCount, taskState, leaseOwner)
	}
	var receiptCount, distinctReceiptCount int
	if err := database.QueryRow(`
		SELECT COUNT(*), COUNT(DISTINCT event_id)
		FROM completion_worker_event_receipts
		WHERE caller_id = ? AND idempotency_key = ?`, chaosCallerID, chaosRequestKey,
	).Scan(&receiptCount, &distinctReceiptCount); err != nil {
		t.Fatal(err)
	}
	if receiptCount != 2 || distinctReceiptCount != 2 {
		t.Fatalf("worker event receipts = %d rows/%d distinct, want 2/2", receiptCount, distinctReceiptCount)
	}
	var finalStageCount, finalStageKinds int
	if err := database.QueryRow(`
		SELECT COUNT(*), COUNT(DISTINCT kind)
		FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ? AND event_id = ?`,
		chaosCallerID, chaosRequestKey, "final-during-three-party-outage",
	).Scan(&finalStageCount, &finalStageKinds); err != nil {
		t.Fatal(err)
	}
	if finalStageCount != 2 || finalStageKinds != 2 {
		t.Fatalf("terminal durable stages = %d rows/%d kinds, want step+applied exactly once", finalStageCount, finalStageKinds)
	}
	var terminalReceiptCount int
	if err := database.QueryRow(`
		SELECT COUNT(*)
		FROM completion_worker_event_receipts
		WHERE caller_id = ? AND idempotency_key = ? AND event_id = ?`,
		chaosCallerID, chaosRequestKey, "final-during-three-party-outage",
	).Scan(&terminalReceiptCount); err != nil {
		t.Fatal(err)
	}
	if terminalReceiptCount != 1 {
		t.Fatalf("terminal receipts = %d, want exactly 1", terminalReceiptCount)
	}
}
