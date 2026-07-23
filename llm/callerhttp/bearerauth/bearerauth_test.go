package bearerauth_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm/callerhttp/bearerauth"
)

func authenticator(t *testing.T) *bearerauth.Authenticator {
	t.Helper()
	auth, err := bearerauth.New("s3cr3t-token", "caller-local")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return auth
}

func withAuth(header string) *http.Request {
	request, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if header != "" {
		request.Header.Set("Authorization", header)
	}
	return request
}

func withAPIKey(value string) *http.Request {
	request := withAuth("")
	request.Header.Set("X-Api-Key", value)
	return request
}

func TestValidTokenAuthenticates(t *testing.T) {
	identity, err := authenticator(t).AuthenticateCaller(context.Background(), withAuth("Bearer s3cr3t-token"))
	if err != nil {
		t.Fatalf("valid token: %v", err)
	}
	if identity.CallerID != "caller-local" {
		t.Fatalf("caller = %q, want caller-local", identity.CallerID)
	}
}

func TestAnthropicAPIKeyAuthenticates(t *testing.T) {
	identity, err := authenticator(t).AuthenticateCaller(context.Background(), withAPIKey("s3cr3t-token"))
	if err != nil {
		t.Fatalf("valid X-Api-Key: %v", err)
	}
	if identity.CallerID != "caller-local" {
		t.Fatalf("caller = %q, want caller-local", identity.CallerID)
	}
}

func TestConflictingCredentialHeadersFailClosed(t *testing.T) {
	request := withAuth("Bearer s3cr3t-token")
	request.Header.Set("X-Api-Key", "different")
	if _, err := authenticator(t).AuthenticateCaller(context.Background(), request); err == nil {
		t.Fatal("conflicting Authorization and X-Api-Key must fail")
	}
}

func TestRejectionsAreTerminalUnauthenticated(t *testing.T) {
	cases := map[string]string{
		"wrong token": "Bearer wrong-token",
		"missing":     "",
		"not bearer":  "Basic s3cr3t-token",
		"empty token": "Bearer ",
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := authenticator(t).AuthenticateCaller(context.Background(), withAuth(header))
			if err == nil {
				t.Fatal("expected rejection")
			}
			code, retry, ok := framework.FaultInfo(err)
			if !ok || code != framework.CodeUnauthenticated || retry != framework.RetryNever {
				t.Fatalf("fault = %v/%v (ok=%v), want unauthenticated/never", code, retry, ok)
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := bearerauth.New("", "caller"); err == nil {
		t.Fatal("empty token must error")
	}
	if _, err := bearerauth.New("token", ""); err == nil {
		t.Fatal("empty callerID must error")
	}
}
