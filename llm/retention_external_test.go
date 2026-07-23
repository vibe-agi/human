package llm_test

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
)

func TestServiceRunRetentionCreatesReplayTombstone(t *testing.T) {
	service := openTestService(t, filepath.Join(t.TempDir(), "retention.db"), nil)
	worker := openTestWorker(t, service, "worker-a", "session-a")
	request := testRequest(true, "private request")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "retention-a", CodecID: testCodecID,
		Body: mustJSON(t, request),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	final := workerDelivery(assignment, "delivery-final", llm.Event{
		ID: "event-final", Type: llm.EventFinal, Text: "private response",
	})
	if receipt, err := worker.CommitEvent(t.Context(), final); err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("final = %+v, %v", receipt, err)
	}

	sweep, err := service.RunRetention(t.Context(), llm.RetentionPolicy{
		CompletedBefore: time.Unix(1_900_000_000, 0).UTC(), BatchSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sweep.RequestsPruned != 1 || sweep.EventsDeleted == 0 {
		t.Fatalf("retention result = %+v", sweep)
	}
	_, err = service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "retention-a", RequestDigest: result.RequestDigest,
	})
	if !errors.Is(err, llm.ErrReplayExpired) {
		t.Fatalf("read after retention = %v", err)
	}
	_, err = service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "retention-a", CodecID: testCodecID,
		Body: mustJSON(t, request),
	})
	if !errors.Is(err, llm.ErrReplayExpired) {
		t.Fatalf("exact replay after retention = %v", err)
	}
	_, err = service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "retention-a", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "conflict")),
	})
	if !errors.Is(err, llm.ErrIdempotencyConflict) {
		t.Fatalf("digest conflict after retention = %v", err)
	}
	second, err := service.RunRetention(t.Context(), llm.RetentionPolicy{
		CompletedBefore: time.Unix(1_900_000_000, 0).UTC(),
	})
	if err != nil || second.RequestsPruned != 0 {
		t.Fatalf("idempotent retention = %+v, %v", second, err)
	}
}
