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

func TestRegisterReplacesSameWorkerWithoutOldCloseUnregisteringReplacement(t *testing.T) {
	t.Parallel()
	workerHub := New(2)
	oldWorker, err := workerHub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := workerHub.Reserve("worker")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	if assignment := <-oldWorker.Assignments; assignment.TaskID != "task" {
		t.Fatalf("old assignment = %+v", assignment)
	}

	replacement, err := workerHub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replacement.Close)
	select {
	case _, open := <-oldWorker.Assignments:
		if open {
			t.Fatal("superseded worker assignment channel remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("superseded worker assignment channel did not close")
	}
	select {
	case assignment := <-replacement.Assignments:
		if assignment.TaskID != "task" || assignment.LeaseOwner != "worker" {
			t.Fatalf("replacement assignment = %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("active assignment was not redelivered to replacement")
	}

	// This is the ordering used by the two HTTP handlers: Register replaces
	// first, then the old handler's deferred Close runs.
	oldWorker.Close()
	probe, err := workerHub.Reserve("worker")
	if err != nil {
		t.Fatalf("old Close unregistered replacement: %v", err)
	}
	probe.Release()
	publishAndReceive(t, workerHub, events, "caller", "request", completion.Event{
		ID: "final", Type: completion.EventFinal, Text: "done",
	})
}

func TestPublishFromEnforcesWorkerOwnershipAndOverridesAcceptedIdentity(t *testing.T) {
	t.Parallel()
	workerHub := New(2)
	owner, err := workerHub.Register("owner")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	intruder, err := workerHub.Register("intruder")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intruder.Close)
	reservation, err := workerHub.Reserve("owner")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	<-owner.Assignments

	forged := completion.Event{
		ID: "forged", Type: completion.EventAccepted, WorkerID: "owner",
	}
	if err := workerHub.PublishFrom(
		context.Background(), "intruder", "caller", "request", forged,
	); !errors.Is(err, ErrWorkerOwnership) {
		t.Fatalf("intruder publish error = %v", err)
	}
	select {
	case event := <-events:
		t.Fatalf("intruder event was delivered: %+v", event.Event)
	case <-time.After(20 * time.Millisecond):
	}

	result := make(chan error, 1)
	go func() {
		result <- workerHub.PublishFrom(
			context.Background(), "owner", "caller", "request",
			completion.Event{ID: "accepted", Type: completion.EventAccepted, WorkerID: "intruder"},
		)
	}()
	delivery := <-events
	if delivery.WorkerID != "owner" {
		t.Fatalf("accepted worker id = %q, want authenticated owner", delivery.WorkerID)
	}
	delivery.Commit(nil)
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	final := completion.Event{ID: "final", Type: completion.EventFinal, Text: "done"}
	go func() {
		result <- workerHub.PublishFrom(context.Background(), "owner", "caller", "request", final)
	}()
	delivery = <-events
	delivery.Commit(nil)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := workerHub.PublishFrom(
		context.Background(), "intruder", "caller", "request", final,
	); !errors.Is(err, ErrWorkerOwnership) {
		t.Fatalf("intruder terminal replay error = %v", err)
	}
	if err := workerHub.PublishFrom(
		context.Background(), "owner", "caller", "request", final,
	); err != nil {
		t.Fatalf("owner terminal replay error = %v", err)
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

func TestAbortRetainsOwnerWithoutInventingLateEventReceipt(t *testing.T) {
	t.Parallel()
	workerHub := New(2)
	owner, err := workerHub.Register("owner")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	intruder, err := workerHub.Register("intruder")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intruder.Close)

	reservation, err := workerHub.Reserve("owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "expired", IdempotencyKey: "request",
	}); err != nil {
		t.Fatal(err)
	}
	<-owner.Assignments
	if err := workerHub.Abort("caller", "request"); err != nil {
		t.Fatal(err)
	}

	late := completion.Event{ID: "late-final", Type: completion.EventFinal, Text: "too late"}
	if err := workerHub.AuthorizePublisher("owner", "caller", "request"); err != nil {
		t.Fatalf("retired owner authorization = %v", err)
	}
	if err := workerHub.PublishFrom(
		context.Background(), "owner", "caller", "request", late,
	); !errors.Is(err, ErrSessionMissing) {
		t.Fatalf("late owner event = %v, want explicit rejection", err)
	}
	if err := workerHub.AuthorizePublisher("intruder", "caller", "request"); !errors.Is(err, ErrWorkerOwnership) {
		t.Fatalf("retired intruder authorization = %v", err)
	}
	if err := workerHub.PublishFrom(
		context.Background(), "intruder", "caller", "request", late,
	); !errors.Is(err, ErrWorkerOwnership) {
		t.Fatalf("retired intruder publish = %v", err)
	}
}

func TestCapacityOneAbortDiscardsQueuedAssignment(t *testing.T) {
	t.Parallel()
	workerHub := New(1)
	worker, err := workerHub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)

	first, err := workerHub.Reserve("worker")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "stale", IdempotencyKey: "stale",
	}); err != nil {
		t.Fatal(err)
	}
	// Do not receive the first assignment. This reproduces the capacity-one
	// stale queue slot which used to make the next Enqueue block under Hub.mu.
	if err := workerHub.Abort("caller", "stale"); err != nil {
		t.Fatal(err)
	}

	second, err := workerHub.Reserve("worker")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "live", IdempotencyKey: "live",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case assignment := <-worker.Assignments:
		if assignment.TaskID != "live" {
			t.Fatalf("received stale assignment after abort: %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("live assignment was blocked behind retired session")
	}
	if err := workerHub.Abort("caller", "live"); err != nil {
		t.Fatal(err)
	}
}

func TestCapacityOneRestoreDiscardsRetiredAssignment(t *testing.T) {
	t.Parallel()
	workerHub := New(1)
	worker, err := workerHub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)

	if _, err := workerHub.Restore(completion.Assignment{
		CallerID: "caller", TaskID: "stale", IdempotencyKey: "stale",
	}, RestoreOptions{WorkerID: "worker"}); err != nil {
		t.Fatal(err)
	}
	if err := workerHub.Abort("caller", "stale"); err != nil {
		t.Fatal(err)
	}
	if _, err := workerHub.Restore(completion.Assignment{
		CallerID: "caller", TaskID: "live", IdempotencyKey: "live",
	}, RestoreOptions{WorkerID: "worker"}); err != nil {
		t.Fatal(err)
	}
	select {
	case assignment := <-worker.Assignments:
		if assignment.TaskID != "live" {
			t.Fatalf("received retired restored assignment: %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("live restored assignment was blocked")
	}
	if err := workerHub.Abort("caller", "live"); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryMayExceedConfiguredCapacityAndDrainsBeforeNewAdmission(t *testing.T) {
	t.Parallel()
	workerHub := New(1)
	for _, request := range []string{"first", "second"} {
		if _, err := workerHub.Restore(completion.Assignment{
			CallerID: "caller", TaskID: request, IdempotencyKey: request,
		}, RestoreOptions{}); err != nil {
			t.Fatalf("restore %s above configured capacity: %v", request, err)
		}
	}
	if _, err := workerHub.Reserve(""); !errors.Is(err, ErrNoWorker) {
		t.Fatalf("reserve before worker = %v", err)
	}
	worker, err := workerHub.Register("worker")
	if err != nil {
		t.Fatalf("register worker for over-capacity recovery: %v", err)
	}
	t.Cleanup(worker.Close)
	drained := make(map[string]struct{})
	for len(drained) < 2 {
		select {
		case assignment := <-worker.Assignments:
			if assignment.IdempotencyKey != "first" && assignment.IdempotencyKey != "second" {
				t.Fatalf("unexpected recovery assignment = %q", assignment.IdempotencyKey)
			}
			drained[assignment.IdempotencyKey] = struct{}{}
		case <-time.After(time.Second):
			t.Fatalf("recovery assignments did not drain: %v", drained)
		}
	}
	if _, err := workerHub.Reserve("worker"); !errors.Is(err, ErrCapacity) {
		t.Fatalf("new admission while recovered backlog exceeds limit = %v", err)
	}
	if err := workerHub.Abort("caller", "first"); err != nil {
		t.Fatal(err)
	}
	if _, err := workerHub.Reserve("worker"); !errors.Is(err, ErrCapacity) {
		t.Fatalf("new admission at configured limit = %v", err)
	}
	if err := workerHub.Abort("caller", "second"); err != nil {
		t.Fatal(err)
	}
	reservation, err := workerHub.Reserve("worker")
	if err != nil {
		t.Fatalf("admission did not recover after backlog drained: %v", err)
	}
	reservation.Release()
}

func TestWorkerPendingAssignmentsStayCapacityBoundedAcrossAbortChurn(t *testing.T) {
	t.Parallel()
	workerHub := New(1)
	worker, err := workerHub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)

	const attempts = 100
	for index := range attempts {
		requestID := fmt.Sprintf("request-%d", index)
		reservation, err := workerHub.Reserve("worker")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reservation.Enqueue(completion.Assignment{
			CallerID: "caller", TaskID: requestID, IdempotencyKey: requestID,
		}); err != nil {
			t.Fatal(err)
		}
		if index != attempts-1 {
			if err := workerHub.Abort("caller", requestID); err != nil {
				t.Fatal(err)
			}
		}
	}

	workerHub.mu.Lock()
	state := workerHub.workers["worker"]
	workerHub.mu.Unlock()
	state.mu.Lock()
	pending := len(state.pending)
	state.mu.Unlock()
	if pending > workerHub.capacity {
		t.Fatalf("pending assignments = %d, capacity = %d", pending, workerHub.capacity)
	}

	select {
	case assignment := <-worker.Assignments:
		if assignment.TaskID != "request-99" {
			t.Fatalf("received stale assignment after churn: %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("last live assignment was not delivered")
	}
	if err := workerHub.Abort("caller", "request-99"); err != nil {
		t.Fatal(err)
	}
}

func TestAbortUnblocksPublishersWaitingToSendOrCommit(t *testing.T) {
	t.Parallel()
	workerHub := New(1)
	worker, err := workerHub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	reservation, err := workerHub.Reserve("worker")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = events // Deliberately leave the 32-slot delivery channel undrained.

	const publishers = 40
	results := make(chan error, publishers)
	for index := range publishers {
		go func() {
			results <- workerHub.Publish(context.Background(), "caller", "request", completion.Event{
				ID: fmt.Sprintf("progress-%d", index), Type: completion.EventProgress, Text: "pending",
			})
		}()
	}
	deadline := time.Now().Add(time.Second)
	for {
		workerHub.mu.Lock()
		active := workerHub.sessions["caller\x00request"]
		pending := 0
		if active != nil {
			pending = len(active.pending)
		}
		workerHub.mu.Unlock()
		if pending == publishers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("registered publishers = %d, want %d", pending, publishers)
		}
		time.Sleep(time.Millisecond)
	}

	if err := workerHub.Abort("caller", "request"); err != nil {
		t.Fatal(err)
	}
	for range publishers {
		select {
		case err := <-results:
			if !errors.Is(err, ErrSessionMissing) {
				t.Fatalf("publisher retirement error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("publisher remained blocked after abort")
		}
	}
}

func TestDurableTerminalRetiresOtherPendingPublishers(t *testing.T) {
	t.Parallel()
	workerHub := New(1)
	worker, err := workerHub.Register("worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	reservation, err := workerHub.Reserve("worker")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	})
	if err != nil {
		t.Fatal(err)
	}

	progressResult := make(chan error, 1)
	go func() {
		progressResult <- workerHub.Publish(context.Background(), "caller", "request", completion.Event{
			ID: "progress", Type: completion.EventProgress, Text: "pending",
		})
	}()
	progress := <-events

	finalResult := make(chan error, 1)
	go func() {
		finalResult <- workerHub.Publish(context.Background(), "caller", "request", completion.Event{
			ID: "final", Type: completion.EventFinal, Text: "done",
		})
	}()
	final := <-events
	final.Commit(nil)
	if err := <-finalResult; err != nil {
		t.Fatal(err)
	}
	if err := <-progressResult; !errors.Is(err, ErrSessionMissing) {
		t.Fatalf("pending progress retirement error = %v", err)
	}
	progress.Commit(nil) // A late consumer commit is harmless.

	next, err := workerHub.Reserve("worker")
	if err != nil {
		t.Fatalf("terminal event did not release capacity: %v", err)
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

func TestDurableTerminalCommitWinsLaterAbortAndKeepsCompactReceipt(t *testing.T) {
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
	delivery.Commit(nil)
	// Once Commit(nil) wins Delivery.once, a concurrent abort may retire the
	// live session but cannot turn a durably handled terminal event into a
	// failure. Publish still installs the compact exact-replay receipt.
	if err := hub.Abort("caller", "request"); err != nil && !errors.Is(err, ErrSessionMissing) {
		t.Fatal(err)
	}
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
