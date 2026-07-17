package workerclient

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerproto"
)

func TestSendEventRejectsOversizeBeforeOutboxCommit(t *testing.T) {
	ctx := context.Background()
	outbox, err := openDurableOutbox(
		ctx, filepath.Join(t.TempDir(), "outbox.db"), "wss://gateway.example/worker", "worker-token",
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = outbox.Close() })
	client := &Client{outbox: outbox, workerID: "worker-limit"}
	assignment := completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	}
	event := completion.Event{
		ID: "event-too-large", Type: completion.EventFinal,
		Text: strings.Repeat("x", int(workerproto.MaxWireMessageBytes)),
	}
	if err := client.SendEvent(ctx, assignment, event); !errors.Is(err, workerproto.ErrMessageTooLarge) {
		t.Fatalf("oversize SendEvent error = %v", err)
	}
	records, err := outbox.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("oversize event reached durable outbox: %+v", records)
	}
}

func TestWorkerEventWireLimitExactBoundary(t *testing.T) {
	assignment := completion.Assignment{CallerID: "caller", IdempotencyKey: "request"}
	event := completion.Event{ID: "event", Type: completion.EventProgress, WorkerID: "worker", Text: "progress"}
	message := workerproto.Event{
		CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey, Event: event,
	}
	size, err := workerproto.EnvelopeWireSize(workerproto.MessageEvent, message)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWorkerEventSize(assignment, event, size); err != nil {
		t.Fatalf("exact boundary rejected: %v", err)
	}
	if err := validateWorkerEventSize(assignment, event, size-1); !errors.Is(err, workerproto.ErrMessageTooLarge) {
		t.Fatalf("one byte over boundary error = %v", err)
	}
}
