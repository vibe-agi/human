package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	internalauth "github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	completiongateway "github.com/vibe-agi/human/internal/completion/gateway"
)

func TestPublicGatewayRequiresExplicitDatabaseIdentity(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	if config.DatabasePath != "" {
		t.Fatalf("public default selected shared database %q", config.DatabasePath)
	}
	if _, err := Open(context.Background(), config); err == nil || !strings.Contains(err.Error(), "database path is required") {
		t.Fatalf("open without explicit database error = %v", err)
	}
}

func TestCustomCookieAuthenticationAndPrincipalBoundaries(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	config.Authenticator = AuthenticatorFunc(func(request *http.Request) (Principal, error) {
		cookie, err := request.Cookie("human_session")
		if err != nil {
			return Principal{}, ErrUnauthorized
		}
		switch cookie.Value {
		case "caller-session":
			return Principal{Type: PrincipalCaller, SubjectID: "tenant-user"}, nil
		case "worker-session":
			return Principal{Type: PrincipalWorker, SubjectID: "expert-1"}, nil
		default:
			return Principal{}, ErrUnauthorized
		}
	})

	server, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close gateway: %v", err)
		}
	})

	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.AddCookie(&http.Cookie{Name: "human_session", Value: "caller-session"})
	response := httptest.NewRecorder()
	server.ModelHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("models status = %d, body = %s", response.Code, response.Body.String())
	}

	// A worker identity is authenticated but cannot use either model API route.
	request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.AddCookie(&http.Cookie{Name: "human_session", Value: "worker-session"})
	response = httptest.NewRecorder()
	server.ModelHandler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("worker on models route status = %d, want 401", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	request.AddCookie(&http.Cookie{Name: "human_session", Value: "worker-session"})
	response = httptest.NewRecorder()
	server.ModelHandler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("worker on model route status = %d, want 401", response.Code)
	}

	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	workerURL := "ws" + httpServer.URL[len("http"):] + WorkerPath
	callerHeader := http.Header{"Cookie": {"human_session=caller-session"}}
	connection, dialResponse, err := websocket.Dial(
		context.Background(), workerURL, &websocket.DialOptions{HTTPHeader: callerHeader},
	)
	if connection != nil {
		_ = connection.CloseNow()
	}
	if err == nil || dialResponse == nil || dialResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("caller worker dial err = %v, response = %#v", err, dialResponse)
	}
	_ = dialResponse.Body.Close()

	workerHeader := http.Header{"Cookie": {"human_session=worker-session"}}
	workerHeader.Set("X-Human-Worker-Instance", "test-worker-instance")
	connection, dialResponse, err = websocket.Dial(
		context.Background(), workerURL, &websocket.DialOptions{HTTPHeader: workerHeader},
	)
	if err != nil {
		if dialResponse != nil {
			_ = dialResponse.Body.Close()
		}
		t.Fatalf("worker dial: %v", err)
	}
	if err := connection.Close(websocket.StatusNormalClosure, "test complete"); err != nil {
		t.Fatalf("close worker socket: %v", err)
	}
}

func TestBuiltInTokenIssueAuthenticateAndRevoke(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	server, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close gateway: %v", err)
		}
	})

	issued, err := server.Issue(context.Background(), PrincipalCaller, "caller-1")
	if err != nil {
		t.Fatal(err)
	}
	if issued.KeyID == "" || issued.Secret == "" {
		t.Fatalf("issued token = %+v", issued)
	}

	models := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		request.Header.Set("Authorization", "Bearer "+issued.Secret)
		response := httptest.NewRecorder()
		server.ModelHandler().ServeHTTP(response, request)
		return response
	}
	if response := models(); response.Code != http.StatusOK {
		t.Fatalf("models before revoke status = %d, body = %s", response.Code, response.Body.String())
	}
	if err := server.Revoke(context.Background(), issued.KeyID); err != nil {
		t.Fatal(err)
	}
	if response := models(); response.Code != http.StatusUnauthorized {
		t.Fatalf("models after revoke status = %d, want 401", response.Code)
	}
}

func TestBuiltInTokenValidationAndConditionalRevoke(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	server, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	issued, err := server.Issue(context.Background(), PrincipalWorker, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := server.ValidateToken(context.Background(), issued.Secret)
	if err != nil {
		t.Fatal(err)
	}
	if principal.Type != PrincipalWorker || principal.SubjectID != "worker-1" || principal.KeyID != issued.KeyID {
		t.Fatalf("principal = %+v", principal)
	}

	if err := server.RevokeToken(context.Background(), issued.Secret, "key_unrelated"); !errors.Is(err, ErrCredentialMismatch) {
		t.Fatalf("mismatched conditional revoke error = %v", err)
	}
	if _, err := server.ValidateToken(context.Background(), issued.Secret); err != nil {
		t.Fatalf("mismatched revoke invalidated token: %v", err)
	}
	if err := server.RevokeToken(context.Background(), issued.Secret, issued.KeyID); err != nil {
		t.Fatal(err)
	}
	if _, err := server.ValidateToken(context.Background(), issued.Secret); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked token validation error = %v", err)
	}
	if _, err := server.Issue(context.Background(), PrincipalCaller, "tenant/user"); err == nil {
		t.Fatal("Issue accepted a subject outside the durable-key grammar")
	}
}

func TestPreparedTokenIsInactiveUntilIdempotentActivation(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	server, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	prepared, err := server.PrepareToken(PrincipalCaller, "prepared-caller")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.ValidateToken(context.Background(), prepared.Secret); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("prepared token authenticated before activation: %v", err)
	}
	weak := prepared
	weak.Secret = "hae_predictable"
	if err := server.ActivateToken(context.Background(), weak); err == nil {
		t.Fatal("ActivateToken accepted caller-chosen low-entropy material")
	}
	if err := server.ActivateToken(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if err := server.ActivateToken(context.Background(), prepared); err != nil {
		t.Fatalf("replayed activation: %v", err)
	}
	principal, err := server.ValidateToken(context.Background(), prepared.Secret)
	if err != nil {
		t.Fatal(err)
	}
	if principal.Type != prepared.Type || principal.SubjectID != prepared.SubjectID || principal.KeyID != prepared.KeyID {
		t.Fatalf("activated principal = %+v, prepared = %+v", principal, prepared)
	}

	conflict := prepared
	conflict.SubjectID = "different-caller"
	if err := server.ActivateToken(context.Background(), conflict); err == nil {
		t.Fatal("conflicting replay unexpectedly activated")
	}
}

func TestCustomAuthenticatorOwnsTokenAdministration(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	config.Authenticator = AuthenticatorFunc(func(*http.Request) (Principal, error) {
		return Principal{}, ErrUnauthorized
	})
	server, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	if _, err := server.Issue(context.Background(), PrincipalCaller, "caller"); !errors.Is(err, ErrBuiltInAuthDisabled) {
		t.Fatalf("issue error = %v", err)
	}
	if _, err := server.PrepareToken(PrincipalCaller, "caller"); !errors.Is(err, ErrBuiltInAuthDisabled) {
		t.Fatalf("prepare error = %v", err)
	}
	if err := server.ActivateToken(context.Background(), PreparedToken{}); !errors.Is(err, ErrBuiltInAuthDisabled) {
		t.Fatalf("activate error = %v", err)
	}
	if err := server.Revoke(context.Background(), "key"); !errors.Is(err, ErrBuiltInAuthDisabled) {
		t.Fatalf("revoke error = %v", err)
	}
	if _, err := server.ValidateToken(context.Background(), "secret"); !errors.Is(err, ErrBuiltInAuthDisabled) {
		t.Fatalf("validate token error = %v", err)
	}
	if err := server.RevokeToken(context.Background(), "secret", "key"); !errors.Is(err, ErrBuiltInAuthDisabled) {
		t.Fatalf("conditional revoke error = %v", err)
	}
}

type pointerAuthenticator struct{}

func (*pointerAuthenticator) AuthenticateRequest(*http.Request) (Principal, error) {
	panic("typed-nil authenticator was called")
}

func TestOpenRejectsTypedNilAuthenticator(t *testing.T) {
	t.Parallel()
	for _, authenticator := range []Authenticator{
		AuthenticatorFunc(nil),
		(*pointerAuthenticator)(nil),
	} {
		config := DefaultConfig()
		config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
		config.Authenticator = authenticator
		server, err := Open(context.Background(), config)
		if server != nil {
			_ = server.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "typed nil") {
			t.Fatalf("Open typed-nil authenticator error = %v", err)
		}
	}
}

type pointerWorkerRouter struct{}

func (*pointerWorkerRouter) RouteWorker(context.Context, WorkerRouteRequest) (string, error) {
	panic("typed-nil worker router was called")
}

func TestOpenRejectsTypedNilWorkerRouter(t *testing.T) {
	t.Parallel()
	for _, router := range []WorkerRouter{
		WorkerRouterFunc(nil),
		(*pointerWorkerRouter)(nil),
	} {
		config := DefaultConfig()
		config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
		config.WorkerRouter = router
		server, err := Open(context.Background(), config)
		if server != nil {
			_ = server.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "typed nil") {
			t.Fatalf("Open typed-nil worker router error = %v", err)
		}
	}
}

func TestPublicWorkerRouterReceivesRequestWithoutInternalTypes(t *testing.T) {
	t.Parallel()
	called := false
	router := publicWorkerRouter{router: WorkerRouterFunc(func(ctx context.Context, request WorkerRouteRequest) (string, error) {
		called = true
		body, err := io.ReadAll(request.Request.Body)
		if err != nil {
			t.Fatal(err)
		}
		if ctx != request.Request.Context() || request.Caller != (Principal{
			Type: PrincipalCaller, SubjectID: "tenant-a", KeyID: "key-a",
		}) || request.Model != "human-expert" || request.CapabilityTier != CapabilityWorkspace ||
			request.WorkspaceKey != "workspace-a" || request.TaskID != "task-a" ||
			request.IdempotencyKey != "request-a" ||
			request.HarnessID != "codex" || request.HarnessVersion != "1" ||
			request.HarnessSessionID != "session-a" || request.WorkspaceRoot != "/repo" ||
			!request.ExecAllowed ||
			string(body) != `{"model":"human-expert"}` {
			t.Fatalf("public worker route request = %+v, body = %s", request, body)
		}
		return "worker-a", nil
	})}
	httpRequest := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"human-expert"}`))
	workerID, err := router.RouteWorker(httpRequest.Context(), completiongateway.WorkerRouteRequest{
		HTTPRequest: httpRequest,
		Caller: internalauth.Principal{
			Type: internalauth.PrincipalCaller, SubjectID: "tenant-a", KeyID: "key-a",
		},
		Model: "human-expert", Tier: completion.TierWorkspace,
		Identity: completion.RoutingIdentity{
			WorkspaceKey: "workspace-a", TaskID: "task-a", IdempotencyKey: "request-a",
			HarnessID: "codex", HarnessVersion: "1", HarnessSessionID: "session-a", Root: "/repo", ExecAllowed: true,
		},
	})
	if err != nil || workerID != "worker-a" || !called {
		t.Fatalf("public router result = %q, %v, called=%v", workerID, err, called)
	}

	denied := publicWorkerRouter{router: WorkerRouterFunc(func(context.Context, WorkerRouteRequest) (string, error) {
		return "", fmt.Errorf("tenant policy: %w", ErrWorkerRouteDenied)
	})}
	if _, err := denied.RouteWorker(httpRequest.Context(), completiongateway.WorkerRouteRequest{
		Caller: internalauth.Principal{Type: internalauth.PrincipalCaller, SubjectID: "tenant-a", KeyID: "key-a"},
	}); !errors.Is(err, completiongateway.ErrWorkerRouteDenied) {
		t.Fatalf("public route denial mapping = %v", err)
	}
}

func TestConfiguredLoggerReceivesCompletionGatewayDiagnostics(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	config := DefaultConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	config.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	config.WorkerRouter = WorkerRouterFunc(func(context.Context, WorkerRouteRequest) (string, error) {
		return "", errors.New("routing backend failed")
	})
	server, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	issued, err := server.Issue(context.Background(), PrincipalCaller, "caller-logger")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"route"}]}`),
	)
	request.Header.Set("Authorization", "Bearer "+issued.Secret)
	request.Header.Set("Idempotency-Key", "logger-route-request")
	response := httptest.NewRecorder()
	server.ModelHandler().ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), "worker_route_error") {
		t.Fatalf("router error response = %d, %s", response.Code, response.Body.String())
	}
	if !strings.Contains(logs.String(), "route new completion task") || !strings.Contains(logs.String(), "routing backend failed") {
		t.Fatalf("configured logger did not receive completion diagnostic: %s", logs.String())
	}
}

func TestCustomPrincipalRequiresStableCorrectnessIdentifiers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		principal Principal
		wantOK    bool
	}{
		{name: "stable derived key", principal: Principal{Type: PrincipalCaller, SubjectID: "tenant-user"}, wantOK: true},
		{name: "stable explicit key", principal: Principal{Type: PrincipalWorker, SubjectID: "expert-1", KeyID: "session:one"}, wantOK: true},
		{name: "slash in subject", principal: Principal{Type: PrincipalCaller, SubjectID: "tenant/user"}},
		{name: "control in subject", principal: Principal{Type: PrincipalCaller, SubjectID: "tenant\nuser"}},
		{name: "subject too long", principal: Principal{Type: PrincipalCaller, SubjectID: strings.Repeat("a", 129)}},
		{name: "space in key", principal: Principal{Type: PrincipalCaller, SubjectID: "tenant", KeyID: "session one"}},
		{name: "key too long", principal: Principal{Type: PrincipalCaller, SubjectID: "tenant", KeyID: strings.Repeat("k", 129)}},
		{name: "invalid type", principal: Principal{Type: "admin", SubjectID: "tenant"}},
	}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator := requestAuthenticator{custom: AuthenticatorFunc(func(*http.Request) (Principal, error) {
				return test.principal, nil
			})}
			got, err := authenticator.AuthenticateRequest(request)
			if !test.wantOK {
				if !errors.Is(err, ErrUnauthorized) {
					t.Fatalf("error = %v, want ErrUnauthorized", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.KeyID == "" || len(got.KeyID) > 128 {
				t.Fatalf("derived/explicit key ID = %q", got.KeyID)
			}
		})
	}
}

func TestWorkerWireBudgetIsUnifiedAndBounded(t *testing.T) {
	t.Parallel()
	const smaller int64 = 1 << 20

	fromGateway, err := (Config{DatabasePath: ":memory:", MaxWorkerMessageBytes: smaller}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if fromGateway.MaxWorkerMessageBytes != smaller || fromGateway.Worker.ReadLimit != smaller {
		t.Fatalf(
			"gateway-only wire budget = max %d read %d, want %d",
			fromGateway.MaxWorkerMessageBytes, fromGateway.Worker.ReadLimit, smaller,
		)
	}
	fromTransport, err := (Config{DatabasePath: ":memory:", Worker: WorkerTransportConfig{ReadLimit: smaller}}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if fromTransport.MaxWorkerMessageBytes != smaller || fromTransport.Worker.ReadLimit != smaller {
		t.Fatalf(
			"transport-only wire budget = max %d read %d, want %d",
			fromTransport.MaxWorkerMessageBytes, fromTransport.Worker.ReadLimit, smaller,
		)
	}

	for _, testCase := range []struct {
		name   string
		config Config
	}{
		{
			name: "gateway accepts more than transport",
			config: Config{
				MaxWorkerMessageBytes: 2 << 20,
				Worker:                WorkerTransportConfig{ReadLimit: 1 << 20},
			},
		},
		{
			name: "transport accepts more than gateway",
			config: Config{
				MaxWorkerMessageBytes: 1 << 20,
				Worker:                WorkerTransportConfig{ReadLimit: 2 << 20},
			},
		},
		{
			name: "both exceed official protocol client",
			config: Config{
				MaxWorkerMessageBytes: 16 << 20,
				Worker:                WorkerTransportConfig{ReadLimit: 16 << 20},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			testCase.config.DatabasePath = filepath.Join(t.TempDir(), "must-not-open.db")
			server, openErr := Open(context.Background(), testCase.config)
			if server != nil {
				_ = server.Close()
			}
			if openErr == nil {
				t.Fatal("Open accepted incompatible worker wire budgets")
			}
		})
	}
}

func TestStreamWriteTimeoutDefaultsAndRejectsNegativeDuration(t *testing.T) {
	t.Parallel()
	configured, err := (Config{DatabasePath: ":memory:"}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if configured.StreamWriteTimeout != 10*time.Second {
		t.Fatalf("stream write timeout = %v, want 10s", configured.StreamWriteTimeout)
	}
	if _, err := (Config{DatabasePath: ":memory:", StreamWriteTimeout: -time.Second}).withDefaults(); err == nil ||
		!strings.Contains(err.Error(), "stream write timeout must be positive") {
		t.Fatalf("negative stream write timeout error = %v", err)
	}
}
