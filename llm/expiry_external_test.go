package llm_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
)

func TestServiceRunExpiryCompletesUnacceptedAssignment(t *testing.T) {
	service := openTestService(t, filepath.Join(t.TempDir(), "expiry.db"), nil)
	worker := openTestWorker(t, service, "worker-a", "session-a")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "expiry-a", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "please answer")),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	// Deliberately do not ACK: the host deadline covers queue/delivery time as
	// well as time after a human accepts the assignment.
	sweep, err := service.RunExpiry(t.Context(), llm.ExpiryPolicy{
		PendingBefore: time.Unix(1_900_000_000, 0).UTC(), BatchSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sweep.Scanned != 1 || sweep.Expired != 1 || sweep.Deferred != 0 {
		t.Fatalf("expiry result = %+v", sweep)
	}
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "expiry-a", RequestDigest: result.RequestDigest,
	})
	if err != nil || !page.Complete || page.Mode != llm.ResponseStream || len(page.Events) != 2 {
		t.Fatalf("expired response = %+v, %v", page, err)
	}
	if string(page.Events[1].Data) != "expired:\n" {
		t.Fatalf("expiry frame = %q", page.Events[1].Data)
	}
	late, err := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-late", llm.Event{
		ID: "event-late", Type: llm.EventFinal, Text: "too late",
	}))
	if err != nil || late.Decision != llm.WorkerEventNACK || late.Code != llm.WorkerRejectResponseClosed {
		t.Fatalf("late human event = %+v, %v", late, err)
	}
	again, err := service.RunExpiry(t.Context(), llm.ExpiryPolicy{
		PendingBefore: time.Unix(1_900_000_000, 0).UTC(),
	})
	if err != nil || again.Expired != 0 {
		t.Fatalf("idempotent expiry = %+v, %v", again, err)
	}
}
