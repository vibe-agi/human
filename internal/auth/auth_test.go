package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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

type cookieAuthenticator struct {
}

func (authenticator *cookieAuthenticator) AuthenticateRequest(request *http.Request) (Principal, error) {
	cookie, err := request.Cookie("session")
	if err != nil || cookie.Value != "valid-session" {
		return Principal{}, ErrUnauthorized
	}
	return Principal{Type: PrincipalCaller, SubjectID: "tenant-user", KeyID: "session-key"}, nil
}

func TestAuthenticateRequestDelegatesCompleteRequest(t *testing.T) {
	t.Parallel()
	authenticator := &cookieAuthenticator{}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	request.AddCookie(&http.Cookie{Name: "session", Value: "valid-session"})

	var requestAuthenticator Authenticator = authenticator
	principal, err := requestAuthenticator.AuthenticateRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if principal.SubjectID != "tenant-user" {
		t.Fatalf("principal = %+v", principal)
	}
}

func TestServiceAuthenticateRequestAcceptsCurrentTokenHeaders(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "auth.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	service := NewService(db)
	issued, err := service.Issue(ctx, PrincipalCaller, "caller")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name          string
		authorization string
		apiKey        string
		wantErr       error
	}{
		{name: "bearer", authorization: "bearer " + issued.Secret},
		{name: "api key", apiKey: issued.Secret},
		{name: "empty bearer", authorization: "Bearer ", apiKey: issued.Secret, wantErr: ErrUnauthorized},
		{name: "wrong scheme", authorization: "Basic abc", wantErr: ErrUnauthorized},
		{name: "wrong scheme never uses api key", authorization: "Basic abc", apiKey: issued.Secret, wantErr: ErrUnauthorized},
		{name: "missing", wantErr: ErrUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set("Authorization", test.authorization)
			request.Header.Set("X-Api-Key", test.apiKey)
			principal, err := service.AuthenticateRequest(request)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if test.wantErr == nil && (principal.SubjectID != "caller" || principal.KeyID != issued.KeyID) {
				t.Fatalf("principal = %+v", principal)
			}
		})
	}
}
