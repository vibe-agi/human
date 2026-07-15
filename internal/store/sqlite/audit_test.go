package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion/canonical"
	storeapi "github.com/vibe-agi/human/internal/store"
)

func auditMetadata() storeapi.AuditMetadata {
	return storeapi.AuditMetadata{
		ID:           "audit-1",
		CallerID:     "caller-1",
		WorkspaceKey: "workspace-1",
		TaskID:       "task-1",
		Dialect:      canonical.DialectOpenAIChat,
		KeyID:        "key-id-not-secret",
		PendingMS:    12,
		GenMS:        34,
	}
}

func TestAuditMetadataDoesNotCreatePayloadByDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	now := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	db.now = func() time.Time { return now }

	created, err := db.CreateAuditMetadata(ctx, auditMetadata())
	if err != nil {
		t.Fatal(err)
	}
	if !created.CreatedAt.Equal(now) {
		t.Fatalf("CreatedAt = %s, want %s", created.CreatedAt, now)
	}
	got, err := db.GetAuditMetadata(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got != created {
		t.Fatalf("GetAuditMetadata() = %+v, want %+v", got, created)
	}
	if _, err := db.GetAuditPayload(ctx, created.ID, "request"); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("default payload lookup error = %v, want ErrNotFound", err)
	}
}

func TestAuditPayloadIsIndependentAndPurgedByExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)
	metadata := auditMetadata()
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	now := base.Add(24 * time.Hour)
	db.now = func() time.Time { return now }
	if _, err := db.CreateAuditMetadata(ctx, metadata); err != nil {
		t.Fatal(err)
	}

	payloads := []storeapi.AuditPayload{
		{
			AuditID: metadata.ID, Kind: "request", Data: []byte(`{"prompt":"private"}`),
			CreatedAt: base, ExpiresAt: base.Add(7 * 24 * time.Hour),
		},
		{
			AuditID: metadata.ID, Kind: "response", Data: []byte(`{"answer":"private"}`),
			CreatedAt: base, ExpiresAt: base.Add(14 * 24 * time.Hour),
		},
	}
	for _, payload := range payloads {
		created, err := db.StoreAuditPayload(ctx, payload)
		if err != nil {
			t.Fatal(err)
		}
		payload.Data[0] = 'X'
		if created.Data[0] == 'X' {
			t.Fatal("StoreAuditPayload returned aliased data")
		}
	}

	request, err := db.GetAuditPayload(ctx, metadata.ID, "request")
	if err != nil {
		t.Fatal(err)
	}
	if string(request.Data) != `{"prompt":"private"}` {
		t.Fatalf("request payload = %q", request.Data)
	}
	now = base.Add(7 * 24 * time.Hour)
	if _, err := db.GetAuditPayload(ctx, metadata.ID, "request"); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("expired payload remained readable: %v", err)
	}
	purged, err := db.PurgeExpiredAuditPayloads(ctx, base.Add(7*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Fatalf("purged = %d, want 1", purged)
	}
	if _, err := db.GetAuditPayload(ctx, metadata.ID, "request"); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("expired payload lookup error = %v", err)
	}
	if _, err := db.GetAuditPayload(ctx, metadata.ID, "response"); err != nil {
		t.Fatalf("unexpired payload was purged: %v", err)
	}
	if _, err := db.GetAuditMetadata(ctx, metadata.ID); err != nil {
		t.Fatalf("payload purge removed metadata: %v", err)
	}
}

func TestAuditValidationAndForeignKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestStore(t)

	invalid := auditMetadata()
	invalid.PendingMS = -1
	if _, err := db.CreateAuditMetadata(ctx, invalid); err == nil {
		t.Fatal("negative duration accepted")
	}
	if _, err := db.StoreAuditPayload(ctx, storeapi.AuditPayload{
		AuditID: "missing", Kind: "request", Data: []byte("x"),
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}); err == nil {
		t.Fatal("payload without metadata accepted")
	}
	if _, err := db.PurgeExpiredAuditPayloads(ctx, time.Time{}); err == nil {
		t.Fatal("zero purge cutoff accepted")
	}
}

func TestOpenAddsAuditTablesToPreAuditDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "old-human.db")
	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.ExecContext(ctx, `
		CREATE TABLE api_tokens (
		  key_id TEXT PRIMARY KEY,
		  principal_type TEXT NOT NULL CHECK(principal_type IN ('caller', 'worker')),
		  subject_id TEXT NOT NULL,
		  token_hash BLOB NOT NULL UNIQUE,
		  created_at INTEGER NOT NULL,
		  revoked_at INTEGER
		)`); err != nil {
		old.Close()
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.CreateAuditMetadata(ctx, auditMetadata()); err != nil {
		t.Fatalf("audit metadata after migration: %v", err)
	}
}
