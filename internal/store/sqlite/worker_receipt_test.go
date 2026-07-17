package sqlite

import (
	"context"
	"errors"
	"testing"

	storeapi "github.com/vibe-agi/human/internal/store"
)

func TestWorkerEventReceiptIsDurableIdempotentAndConflictSafe(t *testing.T) {
	t.Parallel()
	database := openTestStore(t)
	input := requestInput()
	if _, err := database.BeginRequest(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	key := storeapi.RequestKey{CallerID: input.CallerID, IdempotencyKey: input.IdempotencyKey}
	receipt, err := database.RecordWorkerEventReceipt(context.Background(), key, "event-1", "worker-1", "digest-1")
	if err != nil {
		t.Fatal(err)
	}
	if receipt.EventID != "event-1" || receipt.WorkerID != "worker-1" || receipt.Digest != "digest-1" {
		t.Fatalf("receipt = %+v", receipt)
	}
	replayed, err := database.RecordWorkerEventReceipt(context.Background(), key, "event-1", "worker-1", "digest-1")
	if err != nil || replayed.CreatedAt != receipt.CreatedAt {
		t.Fatalf("receipt replay = %+v, %v", replayed, err)
	}
	if _, err := database.LookupWorkerEventReceipt(context.Background(), key, "event-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.LookupWorkerEventReceipt(context.Background(), key, "missing"); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("missing receipt error = %v", err)
	}
	if _, err := database.RecordWorkerEventReceipt(context.Background(), key, "event-1", "worker-1", "changed"); !errors.Is(err, storeapi.ErrWorkerEventConflict) {
		t.Fatalf("conflicting receipt error = %v", err)
	}
	if _, err := database.RecordWorkerEventReceipt(context.Background(), key, "event-1", "worker-2", "digest-1"); !errors.Is(err, storeapi.ErrWorkerEventConflict) {
		t.Fatalf("cross-worker receipt error = %v", err)
	}
}
