package workerclient

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
)

func TestDurableOutboxSurvivesReopenAndSeparatesCredentials(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "private", "worker-outbox.db")
	ctx := context.Background()
	outbox, err := openDurableOutbox(ctx, path, "wss://gateway.example/worker", "hae_secret_token")
	if err != nil {
		t.Fatal(err)
	}
	assignment := completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	}
	event := completion.Event{ID: "event-persisted", Type: completion.EventFinal, Text: "sensitive response body"}
	if _, err := outbox.Put(ctx, assignment, event); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("outbox mode = %o", info.Mode().Perm())
	}
	directory, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if directory.Mode().Perm() != 0o700 {
		t.Fatalf("outbox directory mode = %o", directory.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "hae_secret_token") {
		t.Fatal("outbox stored the bearer token")
	}

	outbox, err = openDurableOutbox(ctx, path, "wss://gateway.example/worker", "hae_secret_token")
	if err != nil {
		t.Fatal(err)
	}
	records, err := outbox.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].EventID != event.ID || records[0].Message.Event.Text != event.Text {
		t.Fatalf("reopened outbox = %+v", records)
	}
	otherCredential, err := openDurableOutbox(ctx, path, "wss://gateway.example/worker", "hae_other_token")
	if err != nil {
		t.Fatal(err)
	}
	records, err = otherCredential.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("another credential could see pending events: %+v", records)
	}
	if err := otherCredential.Close(); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Delete(ctx, event.ID); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), event.Text) {
		t.Fatal("secure deletion left acknowledged response content in the outbox file")
	}
}

func TestDurableOutboxEventIDIsIdempotentAndConflictSafe(t *testing.T) {
	t.Parallel()
	outbox, err := openDurableOutbox(context.Background(), filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	event := completion.Event{ID: "event-one", Type: completion.EventProgress, Text: "same"}
	if _, err := outbox.Put(context.Background(), assignment, event); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Put(context.Background(), assignment, event); err != nil {
		t.Fatalf("idempotent Put = %v", err)
	}
	changed := event
	changed.Text = "different"
	if _, err := outbox.Put(context.Background(), assignment, changed); !errors.Is(err, errOutboxConflict) {
		t.Fatalf("conflicting Put error = %v", err)
	}
	if err := outbox.Delete(context.Background(), event.ID); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Delete(context.Background(), event.ID); err != nil {
		t.Fatalf("duplicate Delete = %v", err)
	}
}

func TestDuplicateACKDeletesOutboxExactlyOnce(t *testing.T) {
	t.Parallel()
	outbox, err := openDurableOutbox(context.Background(), filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	event := completion.Event{ID: "event-acked", Type: completion.EventProgress, Text: "work"}
	if _, err := outbox.Put(context.Background(), assignment, event); err != nil {
		t.Fatal(err)
	}
	client := &Client{
		outbox: outbox, inflight: map[string]uint64{event.ID: 7},
	}
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsSeen <- client.acknowledge(context.Background(), 7)
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	records, err := outbox.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("acknowledged records = %+v", records)
	}
}
