package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/workerws"
)

func TestJournalConformance(t *testing.T) {
	humantest.TestLLMWorkerJournal(t, func(ctx context.Context, test testing.TB) (
		workerws.Journal,
		framework.ReleaseFunc,
		error,
	) {
		resource, err := Open(ctx, Config{Path: filepath.Join(test.TempDir(), "journal.db")})
		if err != nil {
			return nil, nil, err
		}
		journal, err := resource.Value()
		if err != nil {
			_ = resource.Release(context.Background())
			return nil, nil, err
		}
		return journal, resource.Release, nil
	})
}

func TestFilePermissionsOwnerLockAndStrictSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	resource, err := Open(t.Context(), Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("database permissions = %o, want 600", permissions)
	}
	journalPort, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	binding := workerws.JournalBinding{Gateway: "gateway-a", Worker: "worker-a"}
	if err := journalPort.Bind(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), Config{Path: path}); !errors.Is(err, ErrDatabaseInUse) {
		t.Fatalf("second live owner error = %v, want ErrDatabaseInUse", err)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	reopenedPort, _ := reopened.Value()
	if err := reopenedPort.Bind(t.Context(), binding); err != nil {
		t.Fatalf("exact durable binding after reopen: %v", err)
	}
	if err := reopenedPort.Bind(t.Context(), workerws.JournalBinding{Gateway: "gateway-b", Worker: "worker-a"}); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("rebound Journal error = %v, want ErrJournalConflict", err)
	}
	if err := reopened.Release(t.Context()); err != nil {
		t.Fatal(err)
	}

	foreign := filepath.Join(t.TempDir(), "foreign.db")
	foreignResource, err := Open(t.Context(), Config{Path: foreign})
	if err != nil {
		t.Fatal(err)
	}
	value, err := foreignResource.Value()
	if err != nil {
		t.Fatal(err)
	}
	underlying := value.(*journal)
	if _, err := underlying.database.ExecContext(t.Context(), "CREATE TABLE foreign_table(value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := foreignResource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), Config{Path: foreign}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("foreign schema error = %v, want ErrUnsupportedSchema", err)
	}
}

func TestPayloadCeilingCoversEveryHardWireValidDelivery(t *testing.T) {
	if maxJournalPayloadBytes < int(workerws.MaxWireMessageBytes) {
		t.Fatalf("Journal payload ceiling = %d, hard wire ceiling = %d", maxJournalPayloadBytes, workerws.MaxWireMessageBytes)
	}
}

func TestQuotaRejectsNewPayloadButAllowsSettlement(t *testing.T) {
	resource, err := Open(t.Context(), Config{
		Path: filepath.Join(t.TempDir(), "journal.db"), MaxPendingRecords: 1, MaxPendingBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Release(context.Background())
	journal, _ := resource.Value()
	if err := journal.Bind(t.Context(), workerws.JournalBinding{Gateway: "gateway-a", Worker: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	first := sqliteTestEvent("delivery-a")
	if state, err := journal.PutEvent(t.Context(), first); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("first event = %q, %v", state, err)
	}
	// Exact replay is checked before quota and remains valid.
	if state, err := journal.PutEvent(t.Context(), first); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact replay at quota = %q, %v", state, err)
	}
	if _, err := journal.PutEvent(t.Context(), sqliteTestEvent("delivery-b")); !errors.Is(err, workerws.ErrJournalLimit) {
		t.Fatalf("new event over quota error = %v, want ErrJournalLimit", err)
	}
	receipt := llm.WorkerEventReceipt{
		Delivery: first.Delivery.ID, EventID: first.Delivery.Event.ID, Decision: llm.WorkerEventACK,
	}
	if err := journal.SettleEvent(t.Context(), receipt, first.Digest, sqliteDigest(receipt)); err != nil {
		t.Fatalf("settle while at quota: %v", err)
	}
	second := sqliteTestEvent("delivery-b")
	if _, err := journal.PutEvent(t.Context(), second); err != nil {
		t.Fatalf("put after settlement: %v", err)
	}
	nack := llm.WorkerEventReceipt{
		Delivery: second.Delivery.ID, EventID: second.Delivery.Event.ID,
		Decision: llm.WorkerEventNACK, Code: llm.WorkerRejectInvalid, Message: "quota fixture",
	}
	if err := journal.SettleEvent(t.Context(), nack, second.Digest, sqliteDigest(nack)); err != nil {
		t.Fatalf("NACK settlement while at quota: %v", err)
	}
	if _, err := journal.PutEvent(t.Context(), sqliteTestEvent("delivery-c")); !errors.Is(err, workerws.ErrJournalLimit) {
		t.Fatalf("new event while rejection inbox consumes quota = %v, want ErrJournalLimit", err)
	}
	if err := journal.ConfirmRejection(t.Context(), second.Delivery.ID); err != nil {
		t.Fatalf("confirm rejection while at quota: %v", err)
	}
	if _, err := journal.PutEvent(t.Context(), sqliteTestEvent("delivery-c")); err != nil {
		t.Fatalf("put after rejection compaction: %v", err)
	}
}

func TestCommitUnknownCanBeReconciledByExactScan(t *testing.T) {
	resource, err := Open(t.Context(), Config{Path: filepath.Join(t.TempDir(), "journal.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Release(context.Background())
	port, _ := resource.Value()
	underlying := port.(*journal)
	if err := port.Bind(t.Context(), workerws.JournalBinding{Gateway: "gateway-a", Worker: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	originalCommit := underlying.commitTx
	underlying.commitTx = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return errors.New("simulated lost commit acknowledgement")
	}
	record := sqliteTestEvent("delivery-unknown")
	if _, err := port.PutEvent(t.Context(), record); !errors.Is(err, workerws.ErrJournalCommitUnknown) {
		t.Fatalf("PutEvent commit-unknown error = %v", err)
	}
	underlying.commitTx = originalCommit
	listed, err := port.ListEvents(t.Context(), 0, 10)
	if err != nil || len(listed) != 1 || listed[0].Delivery.ID != record.Delivery.ID {
		t.Fatalf("scan after commit-unknown = %#v, %v", listed, err)
	}
	if state, err := port.PutEvent(t.Context(), record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact reconcile = %q, %v", state, err)
	}
}

func TestByteQuotaMeasuresEncodedDurablePayload(t *testing.T) {
	first := sqliteTestEvent("delivery-byte-a")
	encoded, err := json.Marshal(first.Delivery)
	if err != nil {
		t.Fatal(err)
	}
	resource, err := Open(t.Context(), Config{
		Path:              filepath.Join(t.TempDir(), "journal.db"),
		MaxPendingRecords: 10, MaxPendingBytes: int64(len(encoded)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Release(context.Background())
	journal, _ := resource.Value()
	if err := journal.Bind(t.Context(), workerws.JournalBinding{Gateway: "gateway-a", Worker: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.PutEvent(t.Context(), first); err != nil {
		t.Fatalf("payload exactly at byte quota: %v", err)
	}
	if _, err := journal.PutEvent(t.Context(), sqliteTestEvent("delivery-byte-b")); !errors.Is(err, workerws.ErrJournalLimit) {
		t.Fatalf("payload over byte quota error = %v, want ErrJournalLimit", err)
	}
}

func sqliteTestEvent(id llm.WorkerDeliveryID) workerws.JournalEvent {
	delivery := llm.WorkerEventDelivery{
		ID: id,
		Identity: llm.CompletionIdentity{
			CallerID: "caller-a", RequestID: "request-a", TaskID: "task-a",
			WorkspaceKey: "workspace-a", IdempotencyKey: "key-a",
		},
		LeaseID: "lease-a",
		Event:   llm.Event{ID: "event-" + string(id), Type: llm.EventProgress, Text: "working"},
	}
	return workerws.JournalEvent{Digest: sqliteDigest(delivery), Delivery: delivery}
}

func sqliteDigest(value any) workerws.JournalDigest {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(encoded)
	return workerws.JournalDigest(hex.EncodeToString(sum[:]))
}
