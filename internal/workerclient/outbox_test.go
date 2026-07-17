package workerclient

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/workerproto"
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

func TestDurableOutboxRejectsUnversionedSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "outbox.db")
	outbox, err := openDurableOutbox(ctx, path, "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.db.ExecContext(ctx, `DROP TABLE worker_outbox_schema`); err != nil {
		outbox.Close()
		t.Fatal(err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openDurableOutbox(ctx, path, "ws://gateway", "token"); !errors.Is(err, errUnsupportedOutboxSchema) {
		t.Fatalf("open unversioned outbox error = %v", err)
	}
}

func TestDurableOutboxQuarantinesOneCorruptRowWithoutBlockingHealthyEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "outbox.db")
	outbox, err := openDurableOutbox(ctx, path, "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	for _, event := range []completion.Event{
		{ID: "event-before", Type: completion.EventProgress, Text: "before"},
		{ID: "event-corrupt", Type: completion.EventProgress, Text: "corrupt me"},
		{ID: "event-after", Type: completion.EventFinal, Text: "after"},
	} {
		if _, err := outbox.Put(ctx, assignment, event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := outbox.db.ExecContext(ctx, `
		UPDATE worker_outbox
		SET payload = x'7b'
		WHERE namespace = ? AND event_id = 'event-corrupt'`, outbox.namespace); err != nil {
		t.Fatal(err)
	}

	records, err := outbox.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].EventID != "event-before" || records[1].EventID != "event-after" {
		t.Fatalf("healthy outbox records = %+v", records)
	}
	summary, err := outbox.QuarantineSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Count != 1 || len(summary.EventIDs) != 1 || summary.EventIDs[0] != "event-corrupt" || summary.Path != path {
		t.Fatalf("quarantine summary = %+v", summary)
	}
	var rawPayload []byte
	if err := outbox.db.QueryRowContext(ctx, `
		SELECT payload
		FROM worker_outbox_quarantine
		WHERE namespace = ? AND event_id = 'event-corrupt'`, outbox.namespace).Scan(&rawPayload); err != nil {
		t.Fatal(err)
	}
	if string(rawPayload) != "{" {
		t.Fatalf("quarantined raw payload = %q", rawPayload)
	}
	if _, err := outbox.Put(ctx, assignment, completion.Event{
		ID: "event-corrupt", Type: completion.EventProgress, Text: "do not reuse",
	}); !errors.Is(err, ErrEventQuarantined) {
		t.Fatalf("quarantined event reuse error = %v", err)
	}
}

func TestClientFlushReportsPersistentOutboxQuarantineOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	outbox, err := openDurableOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	event := completion.Event{ID: "event-corrupt", Type: completion.EventProgress, Text: "corrupt me"}
	if _, err := outbox.Put(ctx, assignment, event); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.db.ExecContext(ctx, `
		UPDATE worker_outbox SET assignment = x'7b'
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, event.ID); err != nil {
		t.Fatal(err)
	}
	client := &Client{outbox: outbox, messages: make(chan Message, 1)}
	if err := client.flush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-client.messages:
		if message.OutboxQuarantine == nil || message.OutboxQuarantine.Count != 1 {
			t.Fatalf("quarantine message = %+v", message)
		}
	default:
		t.Fatal("flush did not surface quarantined outbox row")
	}
	if err := client.flush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-client.messages:
		t.Fatalf("unchanged quarantine was reported twice: %+v", message)
	default:
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

func TestDurableOutboxLookupReturnsExactEventWithoutDeletingIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	outbox, err := openDurableOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	event := completion.Event{ID: "event-lookup", Type: completion.EventProgress, Text: "draft body\n\n"}
	if _, err := outbox.Put(ctx, assignment, event); err != nil {
		t.Fatal(err)
	}
	record, found, err := outbox.Lookup(ctx, event.ID)
	if err != nil || !found {
		t.Fatalf("Lookup = %+v, %t, %v", record, found, err)
	}
	if record.Message.CallerID != assignment.CallerID ||
		record.Message.IdempotencyKey != assignment.IdempotencyKey || record.TaskID != assignment.TaskID ||
		record.Message.Event.ID != event.ID || record.Message.Event.Text != event.Text {
		t.Fatalf("looked-up record = %+v", record)
	}
	if records, err := outbox.List(ctx); err != nil || len(records) != 1 {
		t.Fatalf("Lookup mutated outbox = %+v, %v", records, err)
	}
	if record, found, err := outbox.Lookup(ctx, "missing"); err != nil || found || record.EventID != "" {
		t.Fatalf("missing Lookup = %+v, %t, %v", record, found, err)
	}
}

func TestDurableOutboxDeleteManyIsAtomic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	outbox, err := openDurableOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	for _, event := range []completion.Event{
		{ID: "event-delete-first", Type: completion.EventProgress, Text: "first"},
		{ID: "event-delete-second", Type: completion.EventProgress, Text: "second"},
	} {
		if _, err := outbox.Put(ctx, assignment, event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := outbox.db.ExecContext(ctx, `
		CREATE TRIGGER fail_second_outbox_delete
		BEFORE DELETE ON worker_outbox
		WHEN OLD.event_id = 'event-delete-second'
		BEGIN
		  SELECT RAISE(ABORT, 'injected delete failure');
		END;`); err != nil {
		t.Fatal(err)
	}
	ids := []string{"event-delete-first", "event-delete-second"}
	if err := outbox.DeleteMany(ctx, ids); err == nil {
		t.Fatal("DeleteMany succeeded through injected second-row failure")
	}
	records, err := outbox.List(ctx)
	if err != nil || len(records) != 2 {
		t.Fatalf("failed DeleteMany partially removed outbox rows: %+v, %v", records, err)
	}
	client := &Client{outbox: outbox, clientSeq: 2, inflight: map[string]uint64{
		"event-delete-first": 1, "event-delete-second": 2,
	}}
	if err := client.acknowledge(ctx, 2); err == nil {
		t.Fatal("cumulative ACK succeeded through injected batch failure")
	}
	if len(client.inflight) != 2 {
		t.Fatalf("failed cumulative ACK partially mutated inflight: %+v", client.inflight)
	}
	if _, err := outbox.db.ExecContext(ctx, `DROP TRIGGER fail_second_outbox_delete`); err != nil {
		t.Fatal(err)
	}
	if err := client.acknowledge(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if len(client.inflight) != 0 {
		t.Fatalf("successful cumulative ACK left inflight: %+v", client.inflight)
	}
	if records, err := outbox.List(ctx); err != nil || len(records) != 0 {
		t.Fatalf("successful DeleteMany left rows: %+v, %v", records, err)
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
		outbox: outbox, clientSeq: 7, inflight: map[string]uint64{event.ID: 7},
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

func TestRejectedEventMovesOnlyItsExactOutboxRecord(t *testing.T) {
	t.Parallel()
	outbox, err := openDurableOutbox(
		context.Background(), filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	}
	for _, event := range []completion.Event{
		{ID: "stale-event", Type: completion.EventFinal, Text: "late"},
		{ID: "live-event", Type: completion.EventProgress, Text: "keep"},
	} {
		if _, err := outbox.Put(context.Background(), assignment, event); err != nil {
			t.Fatal(err)
		}
	}
	client := &Client{
		outbox: outbox, clientSeq: 8,
		inflight: map[string]uint64{
			"stale-event": 7,
			"live-event":  8,
		},
	}
	rejection := &workerproto.EventRejected{
		CallerID: "caller", IdempotencyKey: "request", EventID: "stale-event",
		Message: "session expired",
	}
	rejected, found, err := client.rejectAndAcknowledge(context.Background(), 7, rejection)
	if err != nil {
		t.Fatal(err)
	}
	if !found || rejected.EventID != "stale-event" || rejected.Message.Event.Text != "late" {
		t.Fatalf("moved rejected event = %+v, found=%t", rejected, found)
	}
	records, err := outbox.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].EventID != "live-event" {
		t.Fatalf("outbox after exact rejection = %+v", records)
	}
	rejectedRecords, err := outbox.ListRejected(context.Background())
	if err != nil || len(rejectedRecords) != 1 || rejectedRecords[0].EventID != "stale-event" {
		t.Fatalf("rejected inbox = %+v, %v", rejectedRecords, err)
	}
	if _, exists := client.inflight["stale-event"]; exists {
		t.Fatal("rejected event remained in the inflight map")
	}
	if sequence := client.inflight["live-event"]; sequence != 8 {
		t.Fatalf("unrelated inflight event sequence = %d", sequence)
	}
}

func TestRejectAndAcknowledgeRollsBackMoveAndCumulativeACKTogether(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	outbox, err := openDurableOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	for _, event := range []completion.Event{
		{ID: "event-earlier", Type: completion.EventProgress, Text: "earlier"},
		{ID: "event-rejected", Type: completion.EventFinal, Text: "late"},
	} {
		if _, err := outbox.Put(ctx, assignment, event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := outbox.db.ExecContext(ctx, `
		CREATE TRIGGER fail_rejection_delete
		BEFORE DELETE ON worker_outbox
		WHEN OLD.event_id = 'event-rejected'
		BEGIN
		  SELECT RAISE(ABORT, 'injected rejection transaction failure');
		END;`); err != nil {
		t.Fatal(err)
	}
	client := &Client{outbox: outbox, clientSeq: 2, inflight: map[string]uint64{
		"event-earlier": 1, "event-rejected": 2,
	}}
	rejection := &workerproto.EventRejected{
		CallerID: "caller", IdempotencyKey: "request", EventID: "event-rejected",
		Message: "expired",
	}
	if _, _, err := client.rejectAndAcknowledge(ctx, 2, rejection); err == nil {
		t.Fatal("rejection transaction succeeded through injected delete failure")
	}
	if records, err := outbox.List(ctx); err != nil || len(records) != 2 {
		t.Fatalf("failed rejection transaction mutated outbox = %+v, %v", records, err)
	}
	if records, err := outbox.ListRejected(ctx); err != nil || len(records) != 0 {
		t.Fatalf("failed rejection transaction left inbox row = %+v, %v", records, err)
	}
	if len(client.inflight) != 2 {
		t.Fatalf("failed rejection transaction mutated inflight = %+v", client.inflight)
	}
	if _, err := outbox.db.ExecContext(ctx, `DROP TRIGGER fail_rejection_delete`); err != nil {
		t.Fatal(err)
	}
	if _, found, err := client.rejectAndAcknowledge(ctx, 2, rejection); err != nil || !found {
		t.Fatalf("successful rejection transaction = found %t, err %v", found, err)
	}
	if records, err := outbox.List(ctx); err != nil || len(records) != 0 {
		t.Fatalf("successful rejection transaction left outbox = %+v, %v", records, err)
	}
	if records, err := outbox.ListRejected(ctx); err != nil || len(records) != 1 {
		t.Fatalf("successful rejection transaction inbox = %+v, %v", records, err)
	}
}

func TestRejectedInboxSurvivesReopenWithRedactedAssignmentAndStableOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "outbox.db")
	outbox, err := openDurableOutbox(ctx, path, "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	assignment := completion.Assignment{
		CallerID: "caller", WorkspaceKey: "workspace", TaskID: "task", IdempotencyKey: "request",
		LeaseOwner: "worker", CapabilityTier: completion.TierWorkspace,
		HarnessID: "opencode", HarnessVersion: "1", Root: "/workspace", ExecAllowed: true,
		Adapter: &adapter.Profile{HarnessID: "opencode", HarnessVersion: "1", ErrorShape: "text"},
		Request: canonical.Request{
			Dialect: canonical.DialectOpenAIChat, Model: "human", Stream: true,
			System: "sensitive system prompt",
			Messages: []canonical.Message{{
				Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "sensitive transcript"}},
			}},
			Tools: []canonical.Tool{{
				Name: "human_write_file", Description: "write workspace file",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`),
			}},
			Metadata: map[string]string{"secret": "sensitive metadata"},
		},
	}
	client := &Client{outbox: outbox, inflight: make(map[string]uint64)}
	for index, eventID := range []string{"rejected-first", "rejected-second"} {
		event := completion.Event{ID: eventID, Type: completion.EventProgress, Text: eventID}
		if _, err := outbox.Put(ctx, assignment, event); err != nil {
			t.Fatal(err)
		}
		sequence := uint64(index + 1)
		client.clientSeq = sequence
		client.inflight[eventID] = sequence
		rejection := &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: eventID, Message: "expired",
		}
		if _, found, err := client.rejectAndAcknowledge(ctx, sequence, rejection); err != nil || !found {
			t.Fatalf("move %s = found %t, err %v", eventID, found, err)
		}
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}
	outbox, err = openDurableOutbox(ctx, path, "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	records, err := outbox.ListRejected(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].EventID != "rejected-first" ||
		records[1].EventID != "rejected-second" || records[0].InboxSequence >= records[1].InboxSequence {
		t.Fatalf("reopened rejected order = %+v", records)
	}
	snapshot := records[0].Assignment
	if snapshot.WorkspaceKey != assignment.WorkspaceKey || snapshot.CapabilityTier != completion.TierWorkspace ||
		snapshot.Adapter == nil || snapshot.Adapter.HarnessID != "opencode" || !snapshot.ExecAllowed ||
		len(snapshot.Request.Tools) != 1 || snapshot.Request.Tools[0].Name != "human_write_file" {
		t.Fatalf("reopened assignment snapshot = %+v", snapshot)
	}
	if snapshot.Request.System != "" || len(snapshot.Request.Messages) != 0 || len(snapshot.Request.Metadata) != 0 {
		t.Fatalf("assignment snapshot retained sensitive request content = %+v", snapshot.Request)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"sensitive system prompt", "sensitive transcript", "sensitive metadata"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("durable assignment snapshot retained %q", secret)
		}
	}
}

func TestRejectionFailsClosedForUnknownUnsentAndACKBehindEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	outbox, err := openDurableOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	for _, event := range []completion.Event{
		{ID: "earlier", Type: completion.EventProgress, Text: "earlier"},
		{ID: "target", Type: completion.EventFinal, Text: "target"},
	} {
		if _, err := outbox.Put(ctx, assignment, event); err != nil {
			t.Fatal(err)
		}
	}
	rejection := func(eventID string) *workerproto.EventRejected {
		return &workerproto.EventRejected{
			CallerID: "caller", IdempotencyKey: "request", EventID: eventID,
			Message: "expired",
		}
	}
	client := &Client{outbox: outbox, clientSeq: 5, inflight: map[string]uint64{"earlier": 1}}
	if _, _, err := client.rejectAndAcknowledge(ctx, 1, rejection("unknown")); !errors.Is(err, errRejectedUnknown) {
		t.Fatalf("unknown rejection error = %v", err)
	}
	if _, exists := client.inflight["earlier"]; !exists {
		t.Fatal("unknown rejection applied the same-frame cumulative ACK")
	}
	if _, _, err := client.rejectAndAcknowledge(ctx, 0, rejection("target")); !errors.Is(err, errRejectedNotSent) {
		t.Fatalf("unsent rejection error = %v", err)
	}
	client.inflight["target"] = 5
	if _, _, err := client.rejectAndAcknowledge(ctx, 4, rejection("target")); !errors.Is(err, errRejectedAckBehind) {
		t.Fatalf("ACK-behind rejection error = %v", err)
	}
	if records, err := outbox.List(ctx); err != nil || len(records) != 2 {
		t.Fatalf("invalid rejection mutated outbox = %+v, %v", records, err)
	}
	if records, err := outbox.ListRejected(ctx); err != nil || len(records) != 0 {
		t.Fatalf("invalid rejection populated inbox = %+v, %v", records, err)
	}
}

func TestFutureServerACKFailsClosedWithoutDeletingDurableEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	outbox, err := openDurableOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	for _, event := range []completion.Event{
		{ID: "first", Type: completion.EventProgress, Text: "first"},
		{ID: "target", Type: completion.EventFinal, Text: "target"},
	} {
		if _, err := outbox.Put(ctx, assignment, event); err != nil {
			t.Fatal(err)
		}
	}
	client := &Client{
		outbox: outbox, clientSeq: 2,
		inflight: map[string]uint64{"first": 1, "target": 2},
	}
	if err := client.acknowledge(ctx, 3); !errors.Is(err, errServerACKAhead) {
		t.Fatalf("future ordinary ACK error = %v", err)
	}
	rejection := &workerproto.EventRejected{
		CallerID: "caller", IdempotencyKey: "request", EventID: "target",
		Message: "expired",
	}
	if _, _, err := client.rejectAndAcknowledge(ctx, 3, rejection); !errors.Is(err, errServerACKAhead) {
		t.Fatalf("future rejection ACK error = %v", err)
	}
	if len(client.inflight) != 2 {
		t.Fatalf("future ACK mutated inflight = %+v", client.inflight)
	}
	if records, err := outbox.List(ctx); err != nil || len(records) != 2 {
		t.Fatalf("future ACK mutated outbox = %+v, %v", records, err)
	}
	if records, err := outbox.ListRejected(ctx); err != nil || len(records) != 0 {
		t.Fatalf("future ACK populated rejected inbox = %+v, %v", records, err)
	}
}

func TestDuplicateRejectionAndConfirmationTombstoneAreIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	outbox, err := openDurableOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	event := completion.Event{ID: "rejected", Type: completion.EventFinal, Text: "late"}
	if _, err := outbox.Put(ctx, assignment, event); err != nil {
		t.Fatal(err)
	}
	client := &Client{outbox: outbox, clientSeq: 1, inflight: map[string]uint64{event.ID: 1}}
	rejection := &workerproto.EventRejected{
		CallerID: "caller", IdempotencyKey: "request", EventID: event.ID,
		Message: "expired",
	}
	if _, found, err := client.rejectAndAcknowledge(ctx, 1, rejection); err != nil || !found {
		t.Fatalf("first rejection = found %t, err %v", found, err)
	}
	if _, found, err := client.rejectAndAcknowledge(ctx, 1, rejection); err != nil || !found {
		t.Fatalf("duplicate pending rejection = found %t, err %v", found, err)
	}
	if records, err := outbox.ListRejected(ctx); err != nil || len(records) != 1 {
		t.Fatalf("duplicate pending inbox = %+v, %v", records, err)
	}
	if _, err := outbox.Put(ctx, assignment, event); !errors.Is(err, ErrEventRejectionPending) {
		t.Fatalf("pending rejected local retry error = %v", err)
	}
	if err := client.flush(ctx); err != nil {
		t.Fatalf("ordinary flush attempted to resend rejected inbox row: %v", err)
	}
	if err := client.ConfirmRejectedEvent(ctx, event.ID); err != nil {
		t.Fatal(err)
	}
	if _, found, err := client.rejectAndAcknowledge(ctx, 1, rejection); err != nil || found {
		t.Fatalf("confirmed duplicate rejection = found %t, err %v", found, err)
	}
	if records, err := outbox.ListRejected(ctx); err != nil || len(records) != 0 {
		t.Fatalf("confirmed duplicate recreated inbox = %+v, %v", records, err)
	}
	if _, err := outbox.Put(ctx, assignment, event); !errors.Is(err, ErrEventPreviouslyRejected) {
		t.Fatalf("confirmed rejected local retry error = %v", err)
	}
	if records, err := outbox.List(ctx); err != nil || len(records) != 0 {
		t.Fatalf("confirmed event reentered send outbox = %+v, %v", records, err)
	}
	changed := event
	changed.Text = "different"
	if _, err := outbox.Put(ctx, assignment, changed); !errors.Is(err, errOutboxConflict) {
		t.Fatalf("conflicting tombstoned event error = %v", err)
	}
	changedAssignment := assignment
	changedAssignment.TaskID = "different-task"
	if _, err := outbox.Put(ctx, changedAssignment, event); !errors.Is(err, errOutboxConflict) {
		t.Fatalf("conflicting tombstoned assignment error = %v", err)
	}
}

func TestRejectedConfirmationRollbackKeepsPayloadAndSuccessfulRetryErasesIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "outbox.db")
	outbox, err := openDurableOutbox(ctx, path, "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	const secret = "UNIQUE_REJECTED_PAYLOAD_MUST_BE_ERASED_7f3a1c"
	event := completion.Event{ID: "confirm-rollback", Type: completion.EventFinal, Text: secret}
	if _, err := outbox.Put(ctx, assignment, event); err != nil {
		t.Fatal(err)
	}
	client := &Client{
		outbox: outbox, clientSeq: 1, inflight: map[string]uint64{event.ID: 1},
	}
	rejection := &workerproto.EventRejected{
		CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
		EventID: event.ID,
	}
	if _, found, err := client.rejectAndAcknowledge(ctx, 1, rejection); err != nil || !found {
		t.Fatalf("seed rejection = found %t, err %v", found, err)
	}
	if _, err := outbox.db.ExecContext(ctx, `
		CREATE TRIGGER fail_rejected_confirmation_delete
		BEFORE DELETE ON worker_rejected_inbox
		WHEN OLD.event_id = 'confirm-rollback'
		BEGIN
		  SELECT RAISE(ABORT, 'injected confirmation delete failure');
		END;`); err != nil {
		t.Fatal(err)
	}
	if err := client.ConfirmRejectedEvent(ctx, event.ID); err == nil {
		t.Fatal("confirmation succeeded through injected inbox delete failure")
	}
	if records, err := outbox.ListRejected(ctx); err != nil || len(records) != 1 ||
		records[0].Message.Event.Text != secret {
		t.Fatalf("failed confirmation lost rejected payload = %+v, %v", records, err)
	}
	var tombstones int
	if err := outbox.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM worker_rejected_confirmed
		WHERE namespace = ? AND event_id = ?`, outbox.namespace, event.ID).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if tombstones != 0 {
		t.Fatalf("failed confirmation committed %d tombstones", tombstones)
	}
	if _, err := outbox.db.ExecContext(ctx, `DROP TRIGGER fail_rejected_confirmation_delete`); err != nil {
		t.Fatal(err)
	}
	if err := client.ConfirmRejectedEvent(ctx, event.ID); err != nil {
		t.Fatal(err)
	}
	if records, err := outbox.ListRejected(ctx); err != nil || len(records) != 0 {
		t.Fatalf("successful confirmation retained inbox payload = %+v, %v", records, err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatal("secure_delete left the confirmed rejected payload in the SQLite file")
	}
}

func TestRejectedDispatcherOffersOnlyOldestUntilConfirmedAndReplaysOnReconnect(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox, err := openDurableOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{
		CallerID: "caller", WorkspaceKey: "workspace", TaskID: "task",
		IdempotencyKey: "request", CapabilityTier: completion.TierWorkspace,
	}
	seed := &Client{outbox: outbox, inflight: make(map[string]uint64)}
	for index, eventID := range []string{"first", "second"} {
		if _, err := outbox.Put(ctx, assignment, completion.Event{
			ID: eventID, Type: completion.EventProgress, Text: eventID,
		}); err != nil {
			t.Fatal(err)
		}
		sequence := uint64(index + 1)
		seed.clientSeq = sequence
		seed.inflight[eventID] = sequence
		if _, found, err := seed.rejectAndAcknowledge(ctx, sequence, &workerproto.EventRejected{
			CallerID: "caller", IdempotencyKey: "request", EventID: eventID,
			Message: "expired",
		}); err != nil || !found {
			t.Fatalf("seed rejection %s = found %t, err %v", eventID, found, err)
		}
	}
	client := &Client{
		outbox: outbox, messages: make(chan Message, 8), rejectedWake: make(chan struct{}, 1),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.rejectedLoop(ctx)
	}()
	client.signalRejectedReplay(true)
	receive := func(want string) Message {
		t.Helper()
		select {
		case message := <-client.messages:
			if message.EventRejected == nil || message.EventRejected.EventID != want {
				t.Fatalf("rejected offer = %+v, want %s", message, want)
			}
			return message
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for rejected offer %s", want)
			return Message{}
		}
	}
	first := receive("first")
	if first.RejectedAssignment == nil || first.RejectedAssignment.WorkspaceKey != "workspace" {
		t.Fatalf("replayed rejected assignment = %+v", first.RejectedAssignment)
	}
	select {
	case next := <-client.messages:
		t.Fatalf("dispatcher offered a second unconfirmed rejection: %+v", next)
	case <-time.After(30 * time.Millisecond):
	}
	client.signalRejectedReplay(true)
	receive("first")
	select {
	case next := <-client.messages:
		t.Fatalf("reconnect replay bypassed the oldest rejection: %+v", next)
	case <-time.After(30 * time.Millisecond):
	}
	if err := client.ConfirmRejectedEvent(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	receive("second")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rejected dispatcher did not stop")
	}
}

func TestOldestRejectedDoesNotDecodeLaterCorruptBacklogRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	outbox, err := openDurableOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"), "ws://gateway", "token")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	assignment := completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"}
	client := &Client{outbox: outbox, inflight: make(map[string]uint64)}
	for index, eventID := range []string{"healthy-head", "corrupt-tail"} {
		if _, err := outbox.Put(ctx, assignment, completion.Event{
			ID: eventID, Type: completion.EventProgress, Text: eventID,
		}); err != nil {
			t.Fatal(err)
		}
		sequence := uint64(index + 1)
		client.clientSeq = sequence
		client.inflight[eventID] = sequence
		if _, found, err := client.rejectAndAcknowledge(ctx, sequence, &workerproto.EventRejected{
			CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey,
			EventID: eventID,
		}); err != nil || !found {
			t.Fatalf("seed rejection %s = found %t, err %v", eventID, found, err)
		}
	}
	if _, err := outbox.db.ExecContext(ctx, `
		UPDATE worker_rejected_inbox SET assignment = X'00'
		WHERE namespace = ? AND event_id = 'corrupt-tail'`, outbox.namespace); err != nil {
		t.Fatal(err)
	}
	record, found, err := outbox.OldestRejected(ctx)
	if err != nil || !found || record.EventID != "healthy-head" {
		t.Fatalf("oldest rejected = %+v, found %t, err %v", record, found, err)
	}
}
