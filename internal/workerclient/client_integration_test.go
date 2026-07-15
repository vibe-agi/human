package workerclient

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/hub"
	"github.com/vibe-agi/human/internal/workerws"
)

type integrationAuthenticator struct{}

func (integrationAuthenticator) Authenticate(_ context.Context, token string) (auth.Principal, error) {
	if token != "hae_worker_integration" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return auth.Principal{
		Type:      auth.PrincipalWorker,
		SubjectID: "worker-integration",
		KeyID:     "key-integration",
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
			if message.Assignment != nil {
				redelivered = *message.Assignment
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for reconnect assignment")
		}
	}
	if !sawDisconnect || redelivered.TaskID != first.TaskID {
		t.Fatalf("disconnect=%t, redelivered=%+v", sawDisconnect, redelivered)
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
	// humand has durably committed the terminal event, but its corresponding
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

func websocketURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
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
