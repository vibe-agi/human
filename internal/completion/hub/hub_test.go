package hub

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
)

func publishAndReceive(
	t *testing.T,
	hub *Hub,
	events <-chan *Delivery,
	callerID string,
	idempotencyKey string,
	event completion.Event,
) *Delivery {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		result <- hub.Publish(context.Background(), callerID, idempotencyKey, event)
	}()
	var delivery *Delivery
	select {
	case delivery = <-events:
	case <-time.After(time.Second):
		t.Fatal("worker event was not delivered")
	}
	delivery.Commit(nil)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	return delivery
}

func TestHubAdmissionAndStickyWorker(t *testing.T) {
	t.Parallel()
	hub := New(1)
	if _, err := hub.Reserve(""); !errors.Is(err, ErrNoWorker) {
		t.Fatalf("Reserve without worker error = %v", err)
	}
	worker, err := hub.Register("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	reservation, err := hub.Reserve("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	events, err := reservation.Enqueue(assignment)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Reserve(""); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	select {
	case delivered := <-worker.Assignments:
		if delivered.LeaseOwner != "worker-1" || delivered.TaskID != "task" {
			t.Fatalf("assignment = %+v", delivered)
		}
	case <-time.After(time.Second):
		t.Fatal("assignment was not delivered")
	}
	if event := publishAndReceive(t, hub, events, "caller", "request", completion.Event{Type: completion.EventAccepted, WorkerID: "worker-1"}); event.Type != completion.EventAccepted {
		t.Fatalf("event = %+v", event)
	}
	if event := publishAndReceive(t, hub, events, "caller", "request", completion.Event{Type: completion.EventFinal, Text: "done"}); event.Type != completion.EventFinal {
		t.Fatalf("event = %+v", event)
	}
	second, err := hub.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
}

func TestWorkerDisconnectKeepsAndRedeliversActiveSession(t *testing.T) {
	t.Parallel()
	hub := New(2)
	worker, err := hub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := hub.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{CallerID: "c", TaskID: "t", IdempotencyKey: "r"})
	if err != nil {
		t.Fatal(err)
	}
	worker.Close()
	select {
	case event := <-events:
		t.Fatalf("disconnect terminated live session: %+v", event)
	case <-time.After(20 * time.Millisecond):
	}
	reconnected, err := hub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	defer reconnected.Close()
	select {
	case assignment := <-reconnected.Assignments:
		if assignment.TaskID != "t" || assignment.LeaseOwner != "worker" {
			t.Fatalf("redelivered assignment = %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("active assignment was not redelivered")
	}
	if event := publishAndReceive(t, hub, events, "c", "r", completion.Event{ID: "accept-1", Type: completion.EventAccepted, WorkerID: "worker"}); event.Type != completion.EventAccepted {
		t.Fatalf("accepted event = %+v", event)
	}
	if err := hub.Publish(context.Background(), "c", "r", completion.Event{ID: "accept-2", Type: completion.EventAccepted, WorkerID: "worker"}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf("duplicate accept was delivered: %+v", event)
	case <-time.After(20 * time.Millisecond):
	}
	if event := publishAndReceive(t, hub, events, "c", "r", completion.Event{ID: "final-1", Type: completion.EventFinal, Text: "done"}); event.Type != completion.EventFinal {
		t.Fatalf("final event = %+v", event)
	}
}

func TestWorkerEventIDIsIdempotent(t *testing.T) {
	t.Parallel()
	hub := New(1)
	worker, err := hub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	reservation, _ := hub.Reserve("worker")
	events, err := reservation.Enqueue(completion.Assignment{CallerID: "c", TaskID: "t", IdempotencyKey: "r"})
	if err != nil {
		t.Fatal(err)
	}
	<-worker.Assignments
	event := completion.Event{ID: "progress-1", Type: completion.EventProgress, Text: "same"}
	first := make(chan error, 1)
	go func() { first <- hub.Publish(context.Background(), "c", "r", event) }()
	received := <-events
	received.Commit(nil)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := hub.Publish(context.Background(), "c", "r", event); err != nil {
		t.Fatal(err)
	}
	if received.ID != "progress-1" {
		t.Fatalf("received = %+v", received)
	}
	select {
	case duplicate := <-events:
		t.Fatalf("duplicate event delivered: %+v", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
	_ = hub.Abort("c", "r")
}

func TestConcurrentDuplicateWaitsForOriginalDurableCommit(t *testing.T) {
	t.Parallel()
	hub := New(1)
	worker, err := hub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	reservation, err := hub.Reserve("worker")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{CallerID: "c", TaskID: "t", IdempotencyKey: "r"})
	if err != nil {
		t.Fatal(err)
	}
	<-worker.Assignments
	event := completion.Event{ID: "same-inflight", Type: completion.EventProgress, Text: "durable first"}
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- hub.Publish(context.Background(), "c", "r", event) }()
	delivery := <-events
	go func() { second <- hub.Publish(context.Background(), "c", "r", event) }()
	select {
	case err := <-second:
		t.Fatalf("duplicate was acknowledged before durable commit: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	delivery.Commit(nil)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	select {
	case duplicate := <-events:
		t.Fatalf("duplicate event reached consumer: %+v", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
	_ = hub.Abort("c", "r")
}

func TestReservationRelease(t *testing.T) {
	t.Parallel()
	hub := New(1)
	worker, err := hub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	reservation, err := hub.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	reservation.Release()
	if _, err := reservation.Enqueue(completion.Assignment{}); !errors.Is(err, ErrReservationUsed) {
		t.Fatalf("used reservation error = %v", err)
	}
	other, err := hub.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	other.Release()
}

func TestAbortReleasesCapacity(t *testing.T) {
	t.Parallel()
	hub := New(1)
	worker, err := hub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	reservation, err := hub.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	_, err = reservation.Enqueue(completion.Assignment{CallerID: "c", IdempotencyKey: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if err := hub.Abort("c", "r"); err != nil {
		t.Fatal(err)
	}
	next, err := hub.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	next.Release()
}

func TestTerminalAndAbortRetireLiveSessions(t *testing.T) {
	t.Parallel()
	hub := New(64)
	worker, err := hub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)

	terminalCount := hub.maxRetired + 32
	for index := range terminalCount {
		requestID := fmt.Sprintf("terminal-%d", index)
		reservation, err := hub.Reserve("worker")
		if err != nil {
			t.Fatal(err)
		}
		events, err := reservation.Enqueue(completion.Assignment{
			CallerID: "caller", TaskID: requestID, IdempotencyKey: requestID,
		})
		if err != nil {
			t.Fatal(err)
		}
		<-worker.Assignments
		publishAndReceive(t, hub, events, "caller", requestID, completion.Event{
			ID: "final-" + requestID, Type: completion.EventFinal, Text: "done",
		})
	}

	const abortedCount = 32
	for index := range abortedCount {
		requestID := fmt.Sprintf("aborted-%d", index)
		reservation, err := hub.Reserve("worker")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reservation.Enqueue(completion.Assignment{
			CallerID: "caller", TaskID: requestID, IdempotencyKey: requestID,
		}); err != nil {
			t.Fatal(err)
		}
		<-worker.Assignments
		if err := hub.Abort("caller", requestID); err != nil {
			t.Fatal(err)
		}
	}

	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.active != 0 || len(hub.sessions) != 0 {
		t.Fatalf("retained live state: active=%d sessions=%d", hub.active, len(hub.sessions))
	}
	if len(hub.retired) != hub.maxRetired || len(hub.order) != hub.maxRetired {
		t.Fatalf("terminal receipt bound: receipts=%d order=%d limit=%d", len(hub.retired), len(hub.order), hub.maxRetired)
	}
}

func TestRetiredTerminalReceiptAcknowledgesOnlyExactReplay(t *testing.T) {
	t.Parallel()
	hub := New(1)
	worker, err := hub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	reservation, err := hub.Reserve("worker")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	<-worker.Assignments
	final := completion.Event{ID: "final", Type: completion.EventFinal, Text: "done"}
	publishAndReceive(t, hub, events, "caller", "request", final)

	if err := hub.Publish(context.Background(), "caller", "request", final); err != nil {
		t.Fatalf("exact terminal replay = %v", err)
	}
	changed := final
	changed.Text = "changed"
	if err := hub.Publish(context.Background(), "caller", "request", changed); !errors.Is(err, ErrEventConflict) {
		t.Fatalf("changed terminal replay = %v", err)
	}
	changed.ID = "different"
	if err := hub.Publish(context.Background(), "caller", "request", changed); !errors.Is(err, ErrSessionMissing) {
		t.Fatalf("different terminal event = %v", err)
	}
}

func TestAbortRacingDurableTerminalCommitKeepsCompactReceipt(t *testing.T) {
	t.Parallel()
	hub := New(1)
	worker, err := hub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	reservation, err := hub.Reserve("worker")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	<-worker.Assignments
	final := completion.Event{ID: "final-race", Type: completion.EventFinal, Text: "done"}
	result := make(chan error, 1)
	go func() {
		result <- hub.Publish(context.Background(), "caller", "request", final)
	}()
	delivery := <-events
	if err := hub.Abort("caller", "request"); err != nil {
		t.Fatal(err)
	}
	delivery.Commit(nil)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := hub.Publish(context.Background(), "caller", "request", final); err != nil {
		t.Fatalf("terminal replay after abort race = %v", err)
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.active != 0 || len(hub.sessions) != 0 || len(hub.retired) != 1 {
		t.Fatalf("abort race state: active=%d sessions=%d receipts=%d", hub.active, len(hub.sessions), len(hub.retired))
	}
}
