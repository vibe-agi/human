package llm_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/observe"
)

// recordingObserver collects events for assertions.
type recordingObserver struct {
	mu     sync.Mutex
	events []observe.Event
}

func (observer *recordingObserver) Observe(event observe.Event) {
	observer.mu.Lock()
	observer.events = append(observer.events, event)
	observer.mu.Unlock()
}

func (observer *recordingObserver) kinds() map[observe.Kind]int {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	counts := make(map[observe.Kind]int)
	for _, event := range observer.events {
		counts[event.Kind]++
	}
	return counts
}

// TestServiceEmitsObserverEvents proves the core reports the lifecycle events
// an operator needs: worker connect, admission admitted, event settled.
func TestServiceEmitsObserverEvents(t *testing.T) {
	observer := &recordingObserver{}
	service := openTestServiceOptions(t, filepath.Join(t.TempDir(), "observe.db"), nil, 0,
		&stepClock{next: baseObserveTime()})
	// openTestServiceOptions does not take an observer; reopen with one via the
	// exported config path instead.
	_ = service

	full := newObserverService(t, observer)
	worker := openTestWorker(t, full, "worker-a", "session-a")
	result, err := full.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "turn-obs", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "observe me")),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-final", llm.Event{
		ID: "event-final", Type: llm.EventFinal, Text: "done",
	}))
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("commit final = %+v, %v", receipt, err)
	}
	_ = result

	counts := observer.kinds()
	if counts[observe.KindWorkerConnected] < 1 {
		t.Fatalf("no worker_connected event: %v", counts)
	}
	if counts[observe.KindAdmissionAdmitted] < 1 {
		t.Fatalf("no admission_admitted event: %v", counts)
	}
	if counts[observe.KindWorkerEventSettled] < 1 {
		t.Fatalf("no worker_event_settled event: %v", counts)
	}
}
