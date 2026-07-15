package auth

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

func TestIssueAuthenticateAndRevoke(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "auth.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	service := NewService(db)
	issued, err := service.Issue(ctx, PrincipalCaller, "caller-1")
	if err != nil {
		t.Fatal(err)
	}
	if issued.KeyID == "" || issued.Secret == "" {
		t.Fatalf("issued = %+v", issued)
	}
	principal, err := service.Authenticate(ctx, issued.Secret)
	if err != nil {
		t.Fatal(err)
	}
	if principal.Type != PrincipalCaller || principal.SubjectID != "caller-1" || principal.KeyID != issued.KeyID {
		t.Fatalf("principal = %+v", principal)
	}
	if err := service.Revoke(ctx, issued.KeyID); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Authenticate(ctx, issued.Secret); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked token error = %v", err)
	}
	if err := service.Revoke(ctx, "missing"); !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("missing revoke error = %v", err)
	}
}

func TestRejectsMalformedToken(t *testing.T) {
	t.Parallel()
	service := NewService(nil)
	if _, err := service.Authenticate(context.Background(), "not-a-token"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("malformed token error = %v", err)
	}
}
