package llm_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
)

// TestServiceDurableFaultMatrix runs the same lifecycle failures against both
// the semantic in-memory Store model and the official SQLite adapter. Keeping
// the scenarios here transport-neutral makes Store/recovery regressions fail
// independently of WebSocket or HTTP retry policy.
func TestServiceDurableFaultMatrix(t *testing.T) {
	backends := []struct {
		name string
		open func(*testing.T) llm.Store
	}{
		{name: "memory", open: openFaultMatrixMemoryStore},
		{name: "sqlite", open: openFaultMatrixSQLiteStore},
	}
	scenarios := []struct {
		name string
		run  func(*testing.T, llm.Store)
	}{
		{name: "caller_wait_cancel_exact_retry", run: faultCallerWaitCancelExactRetry},
		{name: "worker_disconnect_before_assignment_ack", run: faultWorkerDisconnectBeforeAssignmentACK},
		{name: "event_ack_loss_exact_replay", run: faultEventACKLossExactReplay},
		{name: "service_restart_redelivers_assignment", run: faultServiceRestartRedeliversAssignment},
		{name: "poison_nack_does_not_block_follower", run: faultPoisonNACKDoesNotBlockFollower},
		{name: "caller_worker_service_offline_converges", run: faultCallerWorkerServiceOfflineConverges},
	}

	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			for _, scenario := range scenarios {
				scenario := scenario
				t.Run(scenario.name, func(t *testing.T) {
					scenario.run(t, backend.open(t))
				})
			}
		})
	}
}

func faultCallerWaitCancelExactRetry(t *testing.T, store llm.Store) {
	service := newFaultMatrixService(t, store)
	worker := openTestWorker(t, service, "worker-a", "wait-session")
	result, assignment := admitFaultMatrixRequest(t, service, worker, "fault-wait", true)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	query := faultMatrixQuery(result, 0)
	initial, err := service.ReadResponse(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	query.After = initial.Cursor

	waitCtx, cancel := context.WithCancel(context.Background())
	waited := make(chan error, 1)
	go func() {
		_, waitErr := service.WaitResponse(waitCtx, query)
		waited <- waitErr
	}()
	select {
	case err := <-waited:
		t.Fatalf("WaitResponse returned before disconnect: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-waited:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled WaitResponse = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled WaitResponse did not return")
	}

	progress := workerDelivery(assignment, "delivery-wait-progress", llm.Event{
		ID: "event-wait-progress", Type: llm.EventProgress, Text: "durable",
	})
	assertWorkerACK(t, worker, progress)
	firstRetry, err := service.WaitResponse(t.Context(), query)
	if err != nil {
		t.Fatalf("retry WaitResponse: %v", err)
	}
	secondRetry, err := service.ReadResponse(t.Context(), query)
	if err != nil {
		t.Fatalf("repeat exact read: %v", err)
	}
	assertExactResponsePage(t, firstRetry, secondRetry)
	if firstRetry.Complete || len(firstRetry.Events) != 1 || string(firstRetry.Events[0].Data) != "progress:durable\n" {
		t.Fatalf("retried response = %+v", firstRetry)
	}

	final := workerDelivery(assignment, "delivery-wait-final", llm.Event{
		ID: "event-wait-final", Type: llm.EventFinal, Text: "done",
	})
	assertWorkerACK(t, worker, final)
	tail, err := service.WaitResponse(t.Context(), faultMatrixQuery(result, firstRetry.Cursor))
	if err != nil || !tail.Complete || len(tail.Events) != 1 || string(tail.Events[0].Data) != "final:done\n" {
		t.Fatalf("terminal retry = %+v, %v", tail, err)
	}
}

func faultWorkerDisconnectBeforeAssignmentACK(t *testing.T, store llm.Store) {
	service := newFaultMatrixService(t, store)
	firstWorker := openTestWorker(t, service, "worker-a", "assignment-before")
	result, first := admitFaultMatrixRequest(t, service, firstWorker, "fault-assignment", true)
	shutdownRuntime(t, firstWorker)

	secondWorker := openTestWorker(t, service, "worker-a", "assignment-after")
	redelivered := receiveServiceAssignment(t, secondWorker)
	assertExactAssignment(t, first, redelivered)
	if err := secondWorker.AckAssignment(t.Context(), redelivered.ID); err != nil {
		t.Fatalf("ACK redelivered assignment: %v", err)
	}
	assertWorkerACK(t, secondWorker, workerDelivery(redelivered, "delivery-redelivery-final", llm.Event{
		ID: "event-redelivery-final", Type: llm.EventFinal, Text: "done",
	}))
	assertFaultMatrixComplete(t, service, result, []string{"start\n", "final:done\n"})
}

func faultEventACKLossExactReplay(t *testing.T, store llm.Store) {
	service := newFaultMatrixService(t, store)
	firstWorker := openTestWorker(t, service, "worker-a", "event-before")
	result, assignment := admitFaultMatrixRequest(t, service, firstWorker, "fault-event", true)
	if err := firstWorker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	delivery := workerDelivery(assignment, "delivery-lost-event-ack", llm.Event{
		ID: "event-lost-ack", Type: llm.EventProgress, Text: "once",
	})
	// The worker-side ACK is deliberately ignored, as if the connection vanished
	// after the durable commit but before the receipt reached its outbox.
	assertWorkerACK(t, firstWorker, delivery)
	beforeReplay, err := service.ReadResponse(t.Context(), faultMatrixQuery(result, 0))
	if err != nil {
		t.Fatal(err)
	}
	shutdownRuntime(t, firstWorker)

	secondWorker := openTestWorker(t, service, "worker-a", "event-after")
	assertWorkerACK(t, secondWorker, delivery)
	afterReplay, err := service.ReadResponse(t.Context(), faultMatrixQuery(result, 0))
	if err != nil {
		t.Fatal(err)
	}
	assertExactResponsePage(t, beforeReplay, afterReplay)
	if len(afterReplay.Events) != 2 {
		t.Fatalf("exact event replay duplicated wire output: %+v", afterReplay.Events)
	}
	assertWorkerACK(t, secondWorker, workerDelivery(assignment, "delivery-after-replay", llm.Event{
		ID: "event-after-replay", Type: llm.EventFinal, Text: "done",
	}))
	assertFaultMatrixComplete(t, service, result, []string{"start\n", "progress:once\n", "final:done\n"})
}

func faultServiceRestartRedeliversAssignment(t *testing.T, store llm.Store) {
	service := newFaultMatrixService(t, store)
	worker := openTestWorker(t, service, "worker-a", "restart-before")
	result, first := admitFaultMatrixRequest(t, service, worker, "fault-restart", false)
	shutdownRuntime(t, service)

	service = newFaultMatrixService(t, store)
	worker = openTestWorker(t, service, "worker-a", "restart-after")
	redelivered := receiveServiceAssignment(t, worker)
	assertExactAssignment(t, first, redelivered)
	if err := worker.AckAssignment(t.Context(), redelivered.ID); err != nil {
		t.Fatal(err)
	}
	assertWorkerACK(t, worker, workerDelivery(redelivered, "delivery-restart-final", llm.Event{
		ID: "event-restart-final", Type: llm.EventFinal, Text: "recovered",
	}))
	page, err := service.ReadResponse(t.Context(), faultMatrixQuery(result, 0))
	if err != nil || !page.Complete || page.Mode != llm.ResponseAggregate || len(page.Decision.Body) == 0 {
		t.Fatalf("recovered aggregate response = %+v, %v", page, err)
	}
}

func faultPoisonNACKDoesNotBlockFollower(t *testing.T, store llm.Store) {
	service := newFaultMatrixService(t, store)
	worker := openTestWorker(t, service, "worker-a", "poison-session")
	result, assignment := admitFaultMatrixRequest(t, service, worker, "fault-poison", true)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	poison := workerDelivery(assignment, "delivery-poison", llm.Event{
		ID: "event-poison", Type: llm.EventToolCalls,
		ToolCalls: []llm.ToolCall{{ID: "call-poison", Name: "undeclared_tool", Input: map[string]any{}}},
	})
	receipt, err := worker.CommitEvent(t.Context(), poison)
	if err != nil || receipt.Decision != llm.WorkerEventNACK || receipt.Code != llm.WorkerRejectForbidden {
		t.Fatalf("poison settlement = %+v, %v", receipt, err)
	}
	assertWorkerACK(t, worker, workerDelivery(assignment, "delivery-poison-follower", llm.Event{
		ID: "event-poison-follower", Type: llm.EventFinal, Text: "healthy",
	}))
	assertFaultMatrixComplete(t, service, result, []string{"start\n", "final:healthy\n"})
}

func faultCallerWorkerServiceOfflineConverges(t *testing.T, store llm.Store) {
	service := newFaultMatrixService(t, store)
	worker := openTestWorker(t, service, "worker-a", "combined-before")
	result, assignment := admitFaultMatrixRequest(t, service, worker, "fault-combined", true)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	initial, err := service.ReadResponse(t.Context(), faultMatrixQuery(result, 0))
	if err != nil {
		t.Fatal(err)
	}

	callerCtx, callerDisconnect := context.WithCancel(context.Background())
	waited := make(chan error, 1)
	go func() {
		_, waitErr := service.WaitResponse(callerCtx, faultMatrixQuery(result, initial.Cursor))
		waited <- waitErr
	}()
	callerDisconnect()
	if err := <-waited; !errors.Is(err, context.Canceled) {
		t.Fatalf("combined caller disconnect = %v", err)
	}

	progress := workerDelivery(assignment, "delivery-combined-progress", llm.Event{
		ID: "event-combined-progress", Type: llm.EventProgress, Text: "checkpoint",
	})
	// Commit succeeds, but both worker receipt delivery and caller observation are
	// treated as lost before every process goes offline.
	assertWorkerACK(t, worker, progress)
	shutdownRuntime(t, worker)
	shutdownRuntime(t, service)

	service = newFaultMatrixService(t, store)
	worker = openTestWorker(t, service, "worker-a", "combined-after")
	assertWorkerACK(t, worker, progress)
	assertWorkerACK(t, worker, workerDelivery(assignment, "delivery-combined-final", llm.Event{
		ID: "event-combined-final", Type: llm.EventFinal, Text: "converged",
	}))

	recovered, err := service.WaitResponse(t.Context(), faultMatrixQuery(result, initial.Cursor))
	if err != nil || !recovered.Complete {
		t.Fatalf("combined recovery = %+v, %v", recovered, err)
	}
	if len(recovered.Events) != 2 || string(recovered.Events[0].Data) != "progress:checkpoint\n" ||
		string(recovered.Events[1].Data) != "final:converged\n" {
		t.Fatalf("combined recovery wire = %+v", recovered.Events)
	}
	replayed, err := service.ReadResponse(t.Context(), faultMatrixQuery(result, initial.Cursor))
	if err != nil {
		t.Fatal(err)
	}
	assertExactResponsePage(t, recovered, replayed)
}

func openFaultMatrixMemoryStore(t *testing.T) llm.Store {
	t.Helper()
	store, release := humantest.NewMemoryLLMStore()
	t.Cleanup(func() {
		if err := release(context.Background()); err != nil {
			t.Errorf("release memory Store: %v", err)
		}
	})
	return store
}

func openFaultMatrixSQLiteStore(t *testing.T) llm.Store {
	t.Helper()
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{
		Path: filepath.Join(t.TempDir(), "fault-matrix.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := resource.Release(context.Background()); err != nil {
			t.Errorf("release SQLite Store: %v", err)
		}
	})
	return store
}

func newFaultMatrixService(t *testing.T, store llm.Store) *llm.Service {
	t.Helper()
	return newTestService(t, framework.Borrow[llm.Store](store), nil)
}

func admitFaultMatrixRequest(
	t *testing.T,
	service *llm.Service,
	worker llm.WorkerConnection,
	key llm.IdempotencyKey,
	stream bool,
) (llm.AdmissionResult, llm.WorkerAssignmentDelivery) {
	t.Helper()
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: key, CodecID: testCodecID,
		Body: mustJSON(t, testRequest(stream, string(key))),
	})
	if err != nil {
		t.Fatalf("admit %q: %v", key, err)
	}
	return result, receiveServiceAssignment(t, worker)
}

func faultMatrixQuery(result llm.AdmissionResult, after uint64) llm.ResponseQuery {
	return llm.ResponseQuery{
		CallerID: result.Identity.CallerID, IdempotencyKey: result.Identity.IdempotencyKey,
		RequestDigest: result.RequestDigest, After: after,
	}
}

func assertWorkerACK(t *testing.T, worker llm.WorkerConnection, delivery llm.WorkerEventDelivery) {
	t.Helper()
	receipt, err := worker.CommitEvent(t.Context(), delivery)
	if err != nil || receipt.Decision != llm.WorkerEventACK || receipt.Delivery != delivery.ID ||
		receipt.EventID != delivery.Event.ID {
		t.Fatalf("worker event %q = %+v, %v", delivery.ID, receipt, err)
	}
}

func assertExactAssignment(t *testing.T, first, second llm.WorkerAssignmentDelivery) {
	t.Helper()
	firstWire, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondWire, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstWire) != string(secondWire) {
		t.Fatalf("assignment replay changed:\nfirst  %s\nsecond %s", firstWire, secondWire)
	}
}

func assertExactResponsePage(t *testing.T, first, second llm.ResponsePage) {
	t.Helper()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("response replay changed:\nfirst  %+v\nsecond %+v", first, second)
	}
}

func assertFaultMatrixComplete(
	t *testing.T,
	service *llm.Service,
	result llm.AdmissionResult,
	want []string,
) {
	t.Helper()
	page, err := service.ReadResponse(t.Context(), faultMatrixQuery(result, 0))
	if err != nil || !page.Complete || len(page.Events) != len(want) {
		t.Fatalf("complete response = %+v, %v", page, err)
	}
	for index := range want {
		if string(page.Events[index].Data) != want[index] {
			t.Fatalf("wire event %d = %q, want %q", index, page.Events[index].Data, want[index])
		}
	}
}
