package workerclient

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
)

func TestDurableOutboxPersistsExactCommandAndScopesCredential(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "outbox.db")
	command := workerproto.Command{
		EventID: "event-1", Kind: workerproto.CommandAccept,
		TaskID: "task-1", ExpectedRevision: 1,
	}
	outbox, err := openDurableOutbox(ctx, path, "wss://example.test/worker", "token-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Put(ctx, command); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Put(ctx, command); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	changed := command
	changed.ExpectedRevision = 2
	if _, err := outbox.Put(ctx, changed); !errors.Is(err, delegation.ErrIdempotencyConflict) {
		t.Fatalf("changed event error = %v", err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}

	outbox, err = openDurableOutbox(ctx, path, "wss://example.test/worker", "token-1")
	if err != nil {
		t.Fatal(err)
	}
	records, err := outbox.List(ctx)
	if err != nil || len(records) != 1 || records[0].Command.EventID != command.EventID {
		t.Fatalf("records = %#v, error = %v", records, err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}

	other, err := openDurableOutbox(ctx, path, "wss://example.test/worker", "token-2")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	records, err = other.List(ctx)
	if err != nil || len(records) != 0 {
		t.Fatalf("other credential records = %#v, error = %v", records, err)
	}
}
