package workerclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/hub"
	"github.com/vibe-agi/human/internal/workerproto"
	"github.com/vibe-agi/human/internal/workerws"
)

type integrationAuthenticator struct{}

type ownershipIntegrationAuthenticator struct{}

func (integrationAuthenticator) AuthenticateRequest(request *http.Request) (auth.Principal, error) {
	token := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	if token != "hae_worker_integration" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return auth.Principal{
		Type:      auth.PrincipalWorker,
		SubjectID: "worker-integration",
		KeyID:     "key-integration",
	}, nil
}

func (ownershipIntegrationAuthenticator) AuthenticateRequest(request *http.Request) (auth.Principal, error) {
	token := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	if token != "hae_worker_intruder" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return auth.Principal{
		Type: auth.PrincipalWorker, SubjectID: "worker-intruder", KeyID: "key-intruder",
	}, nil
}

func TestWorkerClientServerRoundTrip(t *testing.T) {
	workerHub := hub.New(2)
	server, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	client, err := DialWithOutbox(ctx, websocketURL(httpServer.URL), "hae_worker_integration", filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, ctx, func() bool {
		return client.WorkerID() == "worker-integration" && outgoingSequence(client) == 1
	}, "worker hello and acknowledgement")

	reservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID:       "caller-integration",
		WorkspaceKey:   "workspace-integration",
		TaskID:         "task-integration",
		IdempotencyKey: "request-integration",
	})
	if err != nil {
		t.Fatal(err)
	}

	var assignment completion.Assignment
	select {
	case message, open := <-client.Messages():
		if !open {
			t.Fatal("worker client closed before receiving assignment")
		}
		if message.Err != nil {
			t.Fatalf("worker client error: %v", message.Err)
		}
		if message.Assignment == nil {
			t.Fatal("worker client delivered an empty assignment")
		}
		assignment = *message.Assignment
	case <-ctx.Done():
		t.Fatal("timed out waiting for assignment")
	}
	if assignment.TaskID != "task-integration" || assignment.LeaseOwner != "worker-integration" {
		t.Fatalf("assignment = %+v", assignment)
	}
	waitFor(t, ctx, func() bool {
		return client.serverSeq.Load() == 2 && outgoingSequence(client) == 2
	}, "assignment acknowledgement")

	if err := client.SendEvent(ctx, assignment, completion.Event{Type: completion.EventAccepted}); err != nil {
		t.Fatal(err)
	}
	accepted := receiveEvent(t, ctx, events)
	if accepted.Type != completion.EventAccepted || accepted.WorkerID != "worker-integration" {
		t.Fatalf("accepted event = %+v", accepted)
	}
	waitFor(t, ctx, func() bool {
		return client.serverSeq.Load() == 3 && outgoingSequence(client) == 4
	}, "accepted-event acknowledgement")

	if err := client.SendEvent(ctx, assignment, completion.Event{Type: completion.EventProgress, Text: "working"}); err != nil {
		t.Fatal(err)
	}
	progress := receiveEvent(t, ctx, events)
	if progress.Type != completion.EventProgress || progress.Text != "working" {
		t.Fatalf("progress event = %+v", progress)
	}
	waitFor(t, ctx, func() bool {
		return client.serverSeq.Load() == 4 && outgoingSequence(client) == 6
	}, "progress-event acknowledgement")

	if err := client.SendEvent(ctx, assignment, completion.Event{Type: completion.EventFinal, Text: "done"}); err != nil {
		t.Fatal(err)
	}
	final := receiveEvent(t, ctx, events)
	if final.Type != completion.EventFinal || final.Text != "done" {
		t.Fatalf("final event = %+v", final)
	}
	waitFor(t, ctx, func() bool {
		return client.serverSeq.Load() == 5 && outgoingSequence(client) == 8
	}, "final-event acknowledgement")

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case message, open := <-client.Messages():
			if !open {
				waitFor(t, ctx, func() bool {
					reservation, reserveErr := workerHub.Reserve("worker-integration")
					if reserveErr == nil {
						reservation.Release()
					}
					return errors.Is(reserveErr, hub.ErrNoWorker)
				}, "worker unregister after normal close")
				return
			}
			if message.Err != nil {
				t.Fatalf("normal close delivered an error: %v", message.Err)
			}
			t.Fatalf("unexpected message during close: %+v", message)
		case <-ctx.Done():
			t.Fatal("timed out waiting for normal close")
		}
	}
}

func TestSecondWorkerInstanceWaitsWithoutDisplacingThenRecovers(t *testing.T) {
	workerHub := hub.New(2)
	server, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	url := websocketURL(httpServer.URL)
	incumbent, err := DialWithOutbox(ctx, url, "hae_worker_integration", filepath.Join(t.TempDir(), "first.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = incumbent.Close() })
	waitFor(t, ctx, func() bool { return incumbent.WorkerID() == "worker-integration" }, "incumbent hello")

	var attempts atomic.Int32
	runtime := defaultClientRuntimeConfig()
	runtime.reconnectMin = 10 * time.Millisecond
	runtime.reconnectMax = 20 * time.Millisecond
	runtime.observeDial = func() { attempts.Add(1) }
	challenger, err := dialWithOutbox(
		ctx, url, "hae_worker_integration", filepath.Join(t.TempDir(), "second.db"), runtime,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = challenger.Close() })

	var conflict error
	for conflict == nil {
		select {
		case message, open := <-challenger.Messages():
			if !open {
				t.Fatal("challenger closed without an actionable conflict")
			}
			if errors.Is(message.Err, ErrWorkerAlreadyConnected) {
				conflict = message.Err
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for worker-token conflict")
		}
	}
	reservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatalf("challenger displaced incumbent: %v", err)
	}
	reservation.Release()
	if err := incumbent.Close(); err != nil {
		t.Fatal(err)
	}
	for challenger.WorkerID() != "worker-integration" {
		select {
		case message, open := <-challenger.Messages():
			if !open {
				t.Fatal("waiting challenger stopped before the incumbent disconnected")
			}
			if message.Err != nil && !errors.Is(message.Err, ErrWorkerAlreadyConnected) {
				t.Fatalf("challenger recovery error: %v", message.Err)
			}
		case <-ctx.Done():
			t.Fatal("challenger did not acquire the worker after incumbent disconnect")
		}
	}
	if attempts.Load() < 2 {
		t.Fatalf("challenger dial attempts = %d, want a bounded retry", attempts.Load())
	}
}

func TestWorkerOwnershipViolationStopsReconnectWithoutAcknowledgingOutbox(t *testing.T) {
	workerHub := hub.New(2)
	assignment := completion.Assignment{
		CallerID: "caller-owner", TaskID: "task-owner", IdempotencyKey: "request-owner",
		LeaseOwner: "worker-owner",
	}
	if _, err := workerHub.Restore(assignment, hub.RestoreOptions{WorkerID: "worker-owner"}); err != nil {
		t.Fatal(err)
	}
	server, err := workerws.New(workerws.Config{}, ownershipIntegrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	var attempts atomic.Int32
	runtime := defaultClientRuntimeConfig()
	runtime.reconnectMin = 10 * time.Millisecond
	runtime.reconnectMax = 20 * time.Millisecond
	runtime.observeDial = func() { attempts.Add(1) }
	client, err := dialWithOutbox(
		ctx, websocketURL(httpServer.URL), "hae_worker_intruder",
		filepath.Join(t.TempDir(), "ownership.db"), runtime,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-intruder" }, "intruder hello")

	if err := client.SendEvent(ctx, assignment, completion.Event{
		ID: "cross-worker-event", Type: completion.EventAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	var terminal error
	for terminal == nil {
		select {
		case message, open := <-client.Messages():
			if !open {
				t.Fatal("ownership violation closed without an actionable error")
			}
			if errors.Is(message.Err, ErrWorkerOwnershipViolation) {
				terminal = message.Err
			}
		case <-ctx.Done():
			t.Fatal("ownership violation did not stop the client")
		}
	}
	select {
	case <-client.done:
	case <-ctx.Done():
		t.Fatal("ownership-violating client did not terminate")
	}
	time.Sleep(80 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("ownership-violating client dial attempts = %d, want one terminal attempt", got)
	}
	if got := pendingOutboxCount(client); got != 1 {
		t.Fatalf("ownership violation acknowledged an unauthorized event: pending=%d, want 1", got)
	}
}

func TestWorkerClientRejectsInvalidToken(t *testing.T) {
	workerHub := hub.New(1)
	server, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := DialWithOutbox(ctx, websocketURL(httpServer.URL), "wrong-token", filepath.Join(t.TempDir(), "outbox.db"))
	if err == nil {
		_ = client.Close()
		t.Fatal("worker client connected with an invalid token")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("authentication error = %v", err)
	}
	if !errors.Is(err, ErrWorkerAuthentication) {
		t.Fatalf("authentication error does not expose permanent classification: %v", err)
	}
}

func TestWorkerClientInitialTransientFailuresRecoverAfterFiveAttempts(t *testing.T) {
	workerHub := hub.New(2)
	workerServer, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	httpServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if attempts.Add(1) <= 5 {
			http.Error(response, "gateway is starting", http.StatusServiceUnavailable)
			return
		}
		workerServer.ServeHTTP(response, request)
	}))
	t.Cleanup(httpServer.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime := fastClientRuntimeConfig()
	client, err := dialWithOutbox(
		ctx, websocketURL(httpServer.URL), "hae_worker_integration",
		filepath.Join(t.TempDir(), "outbox.db"), runtime,
	)
	if err != nil {
		t.Fatalf("transient initial dial escaped to CLI: %v", err)
	}
	defer client.Close()

	var sawInitialFailure, sawRestore bool
	for !sawInitialFailure || !sawRestore {
		select {
		case message := <-client.Messages():
			if message.ConnectionRestored {
				sawRestore = true
			}
			if message.Err != nil {
				if errors.Is(message.Err, ErrWorkerAuthentication) {
					t.Fatalf("503 was classified as permanent: %v", message.Err)
				}
				sawInitialFailure = true
			}
		case <-ctx.Done():
			t.Fatalf("worker did not recover after %d attempts", attempts.Load())
		}
	}
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-integration" }, "worker hello after cold retries")
	if got := attempts.Load(); got < 6 {
		t.Fatalf("dial attempts = %d, want at least 6", got)
	}

	reservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller-cold-retry", TaskID: "task-cold-retry", IdempotencyKey: "request-cold-retry",
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveClientAssignment(t, ctx, client.Messages())
	if err := client.SendEvent(ctx, assignment, completion.Event{
		ID: "accepted-cold-retry", Type: completion.EventAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	if event := receiveEvent(t, ctx, events); event.ID != "accepted-cold-retry" {
		t.Fatalf("accepted event after cold retry = %+v", event)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(client) == 0 }, "cold-retry event ACK")
	if err := workerHub.Abort(assignment.CallerID, assignment.IdempotencyKey); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerClientInitialConnectionRefusedRetriesUntilGatewayStarts(t *testing.T) {
	workerHub := hub.New(1)
	workerServer, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	runtime := fastClientRuntimeConfig()
	runtime.observeDial = func() { attempts.Add(1) }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := dialWithOutbox(
		ctx, "ws://"+address+"/internal/v1/worker/ws", "hae_worker_integration",
		filepath.Join(t.TempDir(), "outbox.db"), runtime,
	)
	if err != nil {
		t.Fatalf("connection refused escaped to CLI: %v", err)
	}
	var liveServer *http.Server
	var liveListener net.Listener
	t.Cleanup(func() {
		_ = client.Close()
		if liveServer != nil {
			_ = liveServer.Close()
		}
		if liveListener != nil {
			_ = liveListener.Close()
		}
	})
	// The reconnect loop is serial. Observing attempt six means the first five
	// dials have all completed with connection-refused before the listener exists.
	waitFor(t, ctx, func() bool { return attempts.Load() >= 6 }, "five refused connection failures")

	liveListener, err = net.Listen("tcp4", address)
	if err != nil {
		t.Fatalf("start gateway after refused attempts: %v", err)
	}
	liveServer = &http.Server{Handler: workerServer}
	go func() { _ = liveServer.Serve(liveListener) }()
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-integration" }, "gateway startup recovery")
	if got := attempts.Load(); got < 6 {
		t.Fatalf("dial attempts = %d, want at least 6 (five failures and one recovery)", got)
	}

	var sawFailure, sawRestore bool
	for !sawFailure || !sawRestore {
		select {
		case message := <-client.Messages():
			sawFailure = sawFailure || message.Err != nil
			sawRestore = sawRestore || message.ConnectionRestored
		case <-ctx.Done():
			t.Fatalf("cold-start recovery messages: failure=%t restored=%t", sawFailure, sawRestore)
		}
	}
	reservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller-refused", TaskID: "task-refused", IdempotencyKey: "request-refused",
	}); err != nil {
		t.Fatal(err)
	}
	assignment := receiveClientAssignment(t, ctx, client.Messages())
	if assignment.TaskID != "task-refused" {
		t.Fatalf("assignment after gateway startup = %+v", assignment)
	}
	if err := workerHub.Abort(assignment.CallerID, assignment.IdempotencyKey); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerClientCloseCancelsInitialOfflineRetries(t *testing.T) {
	var attempts atomic.Int32
	httpServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(response, "still offline", http.StatusServiceUnavailable)
	}))
	defer httpServer.Close()

	runtime := fastClientRuntimeConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := dialWithOutbox(
		ctx, websocketURL(httpServer.URL), "token",
		filepath.Join(t.TempDir(), "outbox.db"), runtime,
	)
	if err != nil {
		t.Fatalf("transient offline dial = %v", err)
	}
	waitFor(t, ctx, func() bool { return attempts.Load() >= 3 }, "offline reconnect attempts")
	started := time.Now()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("cancel offline reconnect took %s", elapsed)
	}
	stoppedAt := attempts.Load()
	time.Sleep(5 * runtime.reconnectMax)
	if got := attempts.Load(); got != stoppedAt {
		t.Fatalf("offline reconnect continued after Close: before=%d after=%d", stoppedAt, got)
	}
	for {
		select {
		case _, open := <-client.Messages():
			if !open {
				return
			}
		case <-ctx.Done():
			t.Fatal("message channel remained open after Close")
		}
	}
}

func TestWorkerClientPermanentHTTPAuthenticationFailuresDoNotRetry(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var attempts atomic.Int32
			httpServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				http.Error(response, "credential rejected", status)
			}))
			defer httpServer.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			client, err := dialWithOutbox(
				ctx, websocketURL(httpServer.URL), "invalid",
				filepath.Join(t.TempDir(), "outbox.db"), fastClientRuntimeConfig(),
			)
			if client != nil || !errors.Is(err, ErrWorkerAuthentication) {
				if client != nil {
					_ = client.Close()
				}
				t.Fatalf("client=%v error=%v, want permanent authentication error", client, err)
			}
			time.Sleep(100 * time.Millisecond)
			if got := attempts.Load(); got != 1 {
				t.Fatalf("permanent HTTP %d attempts = %d, want 1", status, got)
			}
		})
	}
}

func TestWorkerClientWrongHTTPRouteDoesNotRetryForever(t *testing.T) {
	var attempts atomic.Int32
	httpServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(response, "wrong route", http.StatusNotFound)
	}))
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := dialWithOutbox(
		ctx, websocketURL(httpServer.URL)+"/wrong-worker-route", "token",
		filepath.Join(t.TempDir(), "outbox.db"), fastClientRuntimeConfig(),
	)
	if client != nil || !errors.Is(err, ErrWorkerHandshake) {
		if client != nil {
			_ = client.Close()
		}
		t.Fatalf("client=%v error=%v, want permanent handshake error", client, err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("wrong-route attempts = %d, want 1", got)
	}
}

func TestWorkerClientStopsReconnectWhenCredentialIsRevoked(t *testing.T) {
	workerHub := hub.New(1)
	workerServer, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	var revoked atomic.Bool
	httpServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		attempts.Add(1)
		if revoked.Load() {
			http.Error(response, "credential revoked", http.StatusUnauthorized)
			return
		}
		workerServer.ServeHTTP(response, request)
	}))
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := dialWithOutbox(
		ctx, websocketURL(httpServer.URL), "hae_worker_integration",
		filepath.Join(t.TempDir(), "outbox.db"), fastClientRuntimeConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-integration" }, "initial worker hello")

	revoked.Store(true)
	closeWorkerConnection(client)
	var terminal error
	for terminal == nil {
		select {
		case message := <-client.Messages():
			if errors.Is(message.Err, ErrWorkerAuthentication) {
				terminal = message.Err
			}
		case <-ctx.Done():
			t.Fatalf("revoked credential did not stop reconnect: attempts=%d", attempts.Load())
		}
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close() }()
	for {
		select {
		case _, open := <-client.Messages():
			if !open {
				goto channelClosed
			}
		case <-ctx.Done():
			t.Fatal("message channel remained open after terminal authentication failure")
		}
	}

channelClosed:
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close racing terminal authentication = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Close deadlocked with terminal authentication shutdown")
	}
	stoppedAt := attempts.Load()
	time.Sleep(5 * fastClientRuntimeConfig().reconnectMax)
	if got := attempts.Load(); got != stoppedAt || got != 2 {
		t.Fatalf("reconnect continued after permanent authentication failure: before=%d after=%d", stoppedAt, got)
	}
}

func TestWorkerClientReconnectsAndReceivesActiveAssignmentAgain(t *testing.T) {
	workerHub := hub.New(2)
	server, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := DialWithOutbox(ctx, websocketURL(httpServer.URL), "hae_worker_integration", filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-integration" }, "initial hello")
	reservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task-reconnect", IdempotencyKey: "request-reconnect",
	})
	if err != nil {
		t.Fatal(err)
	}
	first := receiveClientAssignment(t, ctx, client.Messages())
	if first.TaskID != "task-reconnect" {
		t.Fatalf("first assignment = %+v", first)
	}

	client.writeMu.Lock()
	connection := client.connection
	_ = connection.CloseNow()
	client.writeMu.Unlock()
	var redelivered completion.Assignment
	sawDisconnect := false
	sawReconnect := false
	for redelivered.TaskID == "" {
		select {
		case message, open := <-client.Messages():
			if !open {
				t.Fatal("client stopped instead of reconnecting")
			}
			if message.Err != nil {
				sawDisconnect = true
				continue
			}
			if message.ConnectionRestored {
				sawReconnect = true
				continue
			}
			if message.Assignment != nil {
				redelivered = *message.Assignment
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for reconnect assignment")
		}
	}
	if !sawDisconnect || !sawReconnect || redelivered.TaskID != first.TaskID {
		t.Fatalf("disconnect=%t, reconnect=%t, redelivered=%+v", sawDisconnect, sawReconnect, redelivered)
	}
	if err := client.SendEvent(ctx, redelivered, completion.Event{ID: "accept-reconnect", Type: completion.EventAccepted}); err != nil {
		t.Fatal(err)
	}
	if event := receiveEvent(t, ctx, events); event.ID != "accept-reconnect" {
		t.Fatalf("accepted = %+v", event)
	}
	if err := client.SendEvent(ctx, redelivered, completion.Event{ID: "final-reconnect", Type: completion.EventFinal, Text: "done"}); err != nil {
		t.Fatal(err)
	}
	if event := receiveEvent(t, ctx, events); event.ID != "final-reconnect" {
		t.Fatalf("final = %+v", event)
	}
}

func TestWorkerClientFiveFlapsPreserveAssignmentOutboxAndACKs(t *testing.T) {
	workerHub := hub.New(2)
	server, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := dialWithOutbox(
		ctx, websocketURL(httpServer.URL), "hae_worker_integration",
		filepath.Join(t.TempDir(), "outbox.db"), fastClientRuntimeConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-integration" }, "initial worker hello")

	reservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller-five-flaps", TaskID: "task-five-flaps", IdempotencyKey: "request-five-flaps",
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveClientAssignment(t, ctx, client.Messages())
	if err := client.SendEvent(ctx, assignment, completion.Event{
		ID: "accepted-five-flaps", Type: completion.EventAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	if event := receiveEvent(t, ctx, events); event.ID != "accepted-five-flaps" {
		t.Fatalf("accepted event = %+v", event)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(client) == 0 }, "accepted event ACK")

	for flap := 1; flap <= 5; flap++ {
		closeWorkerConnection(client)
		waitFor(t, ctx, func() bool {
			client.writeMu.Lock()
			defer client.writeMu.Unlock()
			return client.connection == nil
		}, fmt.Sprintf("flap %d disconnect", flap))

		eventID := fmt.Sprintf("progress-after-flap-%d", flap)
		if err := client.SendEvent(ctx, assignment, completion.Event{
			ID: eventID, Type: completion.EventProgress, Text: fmt.Sprintf("recovered %d", flap),
		}); err != nil {
			t.Fatalf("persist event during flap %d: %v", flap, err)
		}
		if got := pendingOutboxCount(client); got != 1 {
			t.Fatalf("flap %d durable outbox count = %d, want 1", flap, got)
		}

		var redelivered completion.Assignment
		var sawLost, sawRestored bool
		for redelivered.TaskID == "" {
			select {
			case message, open := <-client.Messages():
				if !open {
					t.Fatalf("client closed during flap %d", flap)
				}
				if message.Err != nil {
					if errors.Is(message.Err, ErrWorkerAuthentication) {
						t.Fatalf("flap %d became permanent: %v", flap, message.Err)
					}
					sawLost = true
				}
				if message.ConnectionRestored {
					sawRestored = true
				}
				if message.Assignment != nil {
					redelivered = *message.Assignment
				}
			case <-ctx.Done():
				t.Fatalf("flap %d did not recover", flap)
			}
		}
		if !sawLost || !sawRestored || redelivered.SessionKey() != assignment.SessionKey() {
			t.Fatalf(
				"flap %d: lost=%t restored=%t assignment=%+v",
				flap, sawLost, sawRestored, redelivered,
			)
		}
		if event := receiveEvent(t, ctx, events); event.ID != eventID {
			t.Fatalf("flap %d replayed event = %+v", flap, event)
		}
		waitFor(t, ctx, func() bool { return pendingOutboxCount(client) == 0 }, fmt.Sprintf("flap %d ACK", flap))
		assignment = redelivered
	}

	if err := client.SendEvent(ctx, assignment, completion.Event{
		ID: "final-after-five-flaps", Type: completion.EventFinal, Text: "done",
	}); err != nil {
		t.Fatal(err)
	}
	if event := receiveEvent(t, ctx, events); event.ID != "final-after-five-flaps" {
		t.Fatalf("final event = %+v", event)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(client) == 0 }, "final event ACK")
	select {
	case duplicate := <-events:
		t.Fatalf("flap replay reached completion consumer twice: %+v", duplicate.Event)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestWorkerClientKeepaliveRecoversFromPeerThatStopsReading(t *testing.T) {
	workerHub := hub.New(1)
	workerServer, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	releaseBlackhole := make(chan struct{})
	httpServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if attempts.Add(1) == 1 {
			connection, acceptErr := websocket.Accept(response, request, nil)
			if acceptErr != nil {
				return
			}
			defer connection.CloseNow()
			// Deliberately never read: the TCP/WebSocket remains open, but the
			// worker's ping receives no pong and must force the reconnect path.
			<-releaseBlackhole
			return
		}
		workerServer.ServeHTTP(response, request)
	}))
	t.Cleanup(func() {
		close(releaseBlackhole)
		httpServer.Close()
	})

	runtime := fastClientRuntimeConfig()
	runtime.keepaliveInterval = 20 * time.Millisecond
	runtime.keepaliveTimeout = 30 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := dialWithOutbox(
		ctx, websocketURL(httpServer.URL), "hae_worker_integration",
		filepath.Join(t.TempDir(), "outbox.db"), runtime,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, ctx, func() bool {
		return attempts.Load() >= 2 && client.WorkerID() == "worker-integration"
	}, "keepalive-driven reconnect")

	var sawLost, sawRestored bool
	for !sawLost || !sawRestored {
		select {
		case message := <-client.Messages():
			sawLost = sawLost || message.Err != nil
			sawRestored = sawRestored || message.ConnectionRestored
		case <-ctx.Done():
			t.Fatalf("keepalive reconnect messages: lost=%t restored=%t", sawLost, sawRestored)
		}
	}
}

func TestWorkerClientReceivesLargeAssignmentBeforeAndAfterReconnect(t *testing.T) {
	workerHub := hub.New(2)
	server, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := DialWithOutbox(ctx, websocketURL(httpServer.URL), "hae_worker_integration", filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-integration" }, "initial hello")

	largeDescription := strings.Repeat("workspace-aware editing tool documentation; ", 1_024)
	if len(largeDescription) <= 32<<10 {
		t.Fatalf("large assignment fixture is only %d bytes", len(largeDescription))
	}
	reservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID:       "caller-large",
		WorkspaceKey:   "workspace-large",
		TaskID:         "task-large",
		IdempotencyKey: "request-large",
		Request: canonical.Request{
			Dialect: canonical.DialectOpenAIChat,
			Model:   "human",
			Stream:  true,
			Messages: []canonical.Message{{
				Role:   canonical.RoleUser,
				Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "inspect the workspace"}},
			}},
			Tools: []canonical.Tool{{
				Name:        "edit_file",
				Description: largeDescription,
				InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`),
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	first := receiveClientAssignment(t, ctx, client.Messages())
	if got := first.Request.Tools[0].Description; got != largeDescription {
		t.Fatalf("initial large tool description length = %d, want %d", len(got), len(largeDescription))
	}

	client.writeMu.Lock()
	connection := client.connection
	_ = connection.CloseNow()
	client.writeMu.Unlock()
	var redelivered completion.Assignment
	for redelivered.TaskID == "" {
		select {
		case message, open := <-client.Messages():
			if !open {
				t.Fatal("client stopped instead of reconnecting")
			}
			if message.Assignment != nil {
				redelivered = *message.Assignment
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for large assignment after reconnect")
		}
	}
	if got := redelivered.Request.Tools[0].Description; got != largeDescription {
		t.Fatalf("redelivered large tool description length = %d, want %d", len(got), len(largeDescription))
	}
	if err := client.SendEvent(ctx, redelivered, completion.Event{ID: "final-large", Type: completion.EventFinal, Text: "done"}); err != nil {
		t.Fatal(err)
	}
	if event := receiveEvent(t, ctx, events); event.ID != "final-large" {
		t.Fatalf("final event = %+v", event)
	}
}

func TestWorkerClientBacksOffWhenConnectionsFailImmediately(t *testing.T) {
	attempts := make(chan time.Time, 16)
	httpServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(response, request, nil)
		if err != nil {
			return
		}
		select {
		case attempts <- time.Now():
		default:
		}
		_ = connection.CloseNow()
	}))
	t.Cleanup(httpServer.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	client, err := DialWithOutbox(ctx, websocketURL(httpServer.URL), "unused", filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	observed := make([]time.Time, 0, 4)
	for len(observed) < cap(observed) {
		select {
		case attempt := <-attempts:
			observed = append(observed, attempt)
		case <-ctx.Done():
			t.Fatalf("saw %d connection attempts, want 4", len(observed))
		}
	}
	if gap := observed[2].Sub(observed[1]); gap < 150*time.Millisecond {
		t.Fatalf("second immediate-failure retry gap = %s, want exponential backoff near 200ms", gap)
	}
	if gap := observed[3].Sub(observed[2]); gap < 300*time.Millisecond {
		t.Fatalf("third immediate-failure retry gap = %s, want exponential backoff near 400ms", gap)
	}
}

func TestWorkerClientReplaysTerminalEventAfterACKLoss(t *testing.T) {
	workerHub := hub.New(2)
	server, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := DialWithOutbox(ctx, websocketURL(httpServer.URL), "hae_worker_integration", filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-integration" }, "worker hello")
	reservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task-ack-loss", IdempotencyKey: "request-ack-loss",
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveClientAssignment(t, ctx, client.Messages())
	if err := client.SendEvent(ctx, assignment, completion.Event{
		ID: "accepted-ack-loss", Type: completion.EventAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	if event := receiveEvent(t, ctx, events); event.ID != "accepted-ack-loss" {
		t.Fatalf("accepted event = %+v", event)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(client) == 0 }, "accepted event ACK")

	if err := client.SendEvent(ctx, assignment, completion.Event{
		ID: "final-ack-loss", Type: completion.EventFinal, Text: "durable final",
	}); err != nil {
		t.Fatal(err)
	}
	var final *hub.Delivery
	select {
	case final = <-events:
	case <-ctx.Done():
		t.Fatal("terminal event was not delivered")
	}
	client.writeMu.Lock()
	connection := client.connection
	if connection != nil {
		_ = connection.CloseNow()
	}
	client.writeMu.Unlock()
	// gateway has durably committed the terminal event, but its corresponding
	// ACK is now guaranteed to be lost with the old socket.
	final.Commit(nil)
	waitFor(t, ctx, func() bool { return pendingOutboxCount(client) == 0 }, "terminal event replay ACK")
	select {
	case duplicate := <-events:
		t.Fatalf("ACK-loss replay reached the completion processor twice: %+v", duplicate)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestWorkerClientOutboxSurvivesClientProcessReopen(t *testing.T) {
	workerHub := hub.New(2)
	server, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	outboxPath := filepath.Join(t.TempDir(), "worker-outbox.db")
	client, err := DialWithOutbox(ctx, websocketURL(httpServer.URL), "hae_worker_integration", outboxPath)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-integration" }, "first worker hello")
	reservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task-process-reopen", IdempotencyKey: "request-process-reopen",
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveClientAssignment(t, ctx, client.Messages())
	client.writeMu.Lock()
	connection := client.connection
	if connection != nil {
		_ = connection.CloseNow()
	}
	client.writeMu.Unlock()
	waitFor(t, ctx, func() bool {
		client.writeMu.Lock()
		defer client.writeMu.Unlock()
		return client.connection == nil
	}, "first client disconnect")
	event := completion.Event{ID: "accepted-after-process-reopen", Type: completion.EventAccepted}
	if err := client.SendEvent(ctx, assignment, event); err != nil {
		t.Fatalf("SendEvent after socket loss = %v", err)
	}
	if pendingOutboxCount(client) != 1 {
		t.Fatal("SendEvent returned before its event was durable in the outbox")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool {
		reservation, reserveErr := workerHub.Reserve("worker-integration")
		if reserveErr == nil {
			reservation.Release()
		}
		return errors.Is(reserveErr, hub.ErrNoWorker)
	}, "first worker process unregister")

	reopened, err := DialWithOutbox(ctx, websocketURL(httpServer.URL), "hae_worker_integration", outboxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	redelivered := receiveClientAssignment(t, ctx, reopened.Messages())
	if redelivered.SessionKey() != assignment.SessionKey() {
		t.Fatalf("redelivered assignment = %+v", redelivered)
	}
	select {
	case delivery := <-events:
		if delivery.ID != event.ID {
			t.Fatalf("reopened outbox event = %+v", delivery.Event)
		}
		delivery.Commit(nil)
	case <-ctx.Done():
		t.Fatal("reopened process did not replay its durable event")
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(reopened) == 0 }, "reopened event ACK")
}

func TestExpiredSessionRejectsLateOutboxEventAndContinuesWithLiveWork(t *testing.T) {
	workerHub := hub.New(2)
	server, err := workerws.New(workerws.Config{}, integrationAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := DialWithOutbox(
		ctx, websocketURL(httpServer.URL), "hae_worker_integration",
		filepath.Join(t.TempDir(), "worker-outbox.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, ctx, func() bool { return client.WorkerID() == "worker-integration" }, "worker hello")

	expiredReservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := expiredReservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "expired-task", IdempotencyKey: "expired-request",
	}); err != nil {
		t.Fatal(err)
	}
	expired := receiveClientAssignment(t, ctx, client.Messages())
	if err := workerHub.Abort(expired.CallerID, expired.IdempotencyKey); err != nil {
		t.Fatal(err)
	}

	liveReservation, err := workerHub.Reserve("worker-integration")
	if err != nil {
		t.Fatal(err)
	}
	liveEvents, err := liveReservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "live-task", IdempotencyKey: "live-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	live := receiveClientAssignment(t, ctx, client.Messages())

	client.writeMu.Lock()
	connectionBefore := client.connection
	client.writeMu.Unlock()
	if err := client.SendEvent(ctx, expired, completion.Event{
		ID: "late-expired-final", Type: completion.EventFinal, Text: "too late",
	}); err != nil {
		t.Fatal(err)
	}
	if err := client.SendEvent(ctx, live, completion.Event{
		ID: "live-accepted", Type: completion.EventAccepted,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case delivery := <-liveEvents:
		if delivery.ID != "live-accepted" {
			t.Fatalf("live event behind stale outbox head = %+v", delivery.Event)
		}
		delivery.Commit(nil)
	case <-ctx.Done():
		t.Fatal("late expired event poisoned delivery of later live work")
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(client) == 0 }, "rejected and live outbox events to resolve")

	var rejection *workerproto.EventRejected
	var rejectedEvent *completion.Event
	for rejection == nil {
		select {
		case message := <-client.Messages():
			rejection = message.EventRejected
			rejectedEvent = message.RejectedEvent
		case <-ctx.Done():
			t.Fatal("client did not surface the late event rejection")
		}
	}
	if rejection.CallerID != expired.CallerID ||
		rejection.IdempotencyKey != expired.IdempotencyKey ||
		rejection.EventID != "late-expired-final" {
		t.Fatalf("event rejection = %+v", rejection)
	}
	if rejectedEvent == nil || rejectedEvent.ID != "late-expired-final" ||
		rejectedEvent.Type != completion.EventFinal || rejectedEvent.Text != "too late" {
		t.Fatalf("rejected durable event = %+v", rejectedEvent)
	}
	client.writeMu.Lock()
	connectionAfter := client.connection
	client.writeMu.Unlock()
	if connectionAfter == nil || connectionAfter != connectionBefore {
		t.Fatal("business rejection reconnected the healthy worker WebSocket")
	}
	if err := workerHub.Abort(live.CallerID, live.IdempotencyKey); err != nil {
		t.Fatal(err)
	}
}

func websocketURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func fastClientRuntimeConfig() clientRuntimeConfig {
	runtime := defaultClientRuntimeConfig()
	runtime.reconnectMin = 10 * time.Millisecond
	runtime.reconnectMax = 40 * time.Millisecond
	runtime.reconnectResetAfter = 200 * time.Millisecond
	runtime.dialTimeout = 500 * time.Millisecond
	return runtime
}

func closeWorkerConnection(client *Client) {
	client.writeMu.Lock()
	connection := client.connection
	client.writeMu.Unlock()
	if connection != nil {
		_ = connection.CloseNow()
	}
}

func outgoingSequence(client *Client) uint64 {
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	return client.clientSeq
}

func pendingOutboxCount(client *Client) int {
	records, err := client.outbox.List(context.Background())
	if err != nil {
		return -1
	}
	return len(records)
}

func receiveEvent(t *testing.T, ctx context.Context, events <-chan *hub.Delivery) completion.Event {
	t.Helper()
	select {
	case delivery := <-events:
		delivery.Commit(nil)
		return delivery.Event
	case <-ctx.Done():
		t.Fatal("timed out waiting for worker event")
		return completion.Event{}
	}
}

func receiveClientAssignment(t *testing.T, ctx context.Context, messages <-chan Message) completion.Assignment {
	t.Helper()
	for {
		select {
		case message, open := <-messages:
			if !open {
				t.Fatal("worker client closed")
			}
			if message.Err != nil {
				t.Fatalf("worker client error: %v", message.Err)
			}
			if message.Assignment != nil {
				return *message.Assignment
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for assignment")
		}
	}
}

func waitFor(t *testing.T, ctx context.Context, condition func() bool, description string) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}
