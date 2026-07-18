package local

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/gateway"
	"github.com/vibe-agi/human/internal/userdata"
	"github.com/vibe-agi/human/worker"
)

func TestDefaultConfigScopesPrivateStoresOutsideWorkspace(t *testing.T) {
	dataRoot := t.TempDir()
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := userdata.ResolveGitWorkspace(".")
	if err != nil {
		t.Fatal(err)
	}
	wantGateway, err := userdata.WorkspacePath("local", workspace, "gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	wantOutbox, err := userdata.WorkspacePath("local", workspace, "worker-outbox.db")
	if err != nil {
		t.Fatal(err)
	}
	wantState, err := userdata.WorkspacePath("local", workspace, "worker-state.db")
	if err != nil {
		t.Fatal(err)
	}
	if config.Gateway.DatabasePath != wantGateway || config.Worker.OutboxPath != wantOutbox || config.Worker.StatePath != wantState {
		t.Fatalf("local private defaults = %q / %q / %q, want %q / %q / %q",
			config.Gateway.DatabasePath, config.Worker.OutboxPath, config.Worker.StatePath,
			wantGateway, wantOutbox, wantState)
	}
	if config.IssuedCredentialPolicy != IssuedCredentialsRevokeOnClose {
		t.Fatalf("default issued credential policy = %d, want revoke-on-close", config.IssuedCredentialPolicy)
	}
}

func TestLocalOutboxScopeUsesCanonicalDurableGatewayIdentity(t *testing.T) {
	realParent := filepath.Join(t.TempDir(), "real")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasParent := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatal(err)
	}
	realScope, err := localOutboxScope(filepath.Join(realParent, "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	aliasScope, err := localOutboxScope(filepath.Join(aliasParent, "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	if realScope == "" || realScope != aliasScope {
		t.Fatalf("canonical local scopes = %q and %q", realScope, aliasScope)
	}
	otherScope, err := localOutboxScope(filepath.Join(realParent, "other.db"))
	if err != nil {
		t.Fatal(err)
	}
	if otherScope == realScope {
		t.Fatal("different gateway databases selected the same local outbox scope")
	}
	memoryScope, err := localOutboxScope(":memory:")
	if err != nil || memoryScope != "" {
		t.Fatalf("memory gateway scope = %q, %v", memoryScope, err)
	}
}

func TestOpenConnectsWorkerServesCallerAndClosesIdempotently(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	privateRoot := filepath.Join(root, "private")
	if err := os.Mkdir(privateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	config := Config{
		Gateway: gateway.Config{DatabasePath: filepath.Join(root, "gateway.db")},
		Worker: worker.Config{
			MirrorRoot: filepath.Join(root, "mirror"),
			OutboxPath: filepath.Join(privateRoot, "outbox.db"),
			StatePath:  filepath.Join(privateRoot, "state.db"),
		},
		ListenAddress: "127.0.0.1:0",
		CallerSubject: "test-caller",
		WorkerSubject: "test-worker",
	}

	instance, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = instance.Close()
		}
	})
	if instance.Worker() == nil || instance.Model() == nil || instance.Gateway() == nil {
		t.Fatal("local instance did not expose all embedded components")
	}
	if instance.CallerToken() == "" {
		t.Fatal("caller token is empty")
	}
	credentials := instance.Credentials()
	if !credentials.NewlyIssued || credentials.CallerToken != instance.CallerToken() || credentials.WorkerToken == "" ||
		credentials.CallerKeyID == "" || credentials.WorkerKeyID == "" {
		t.Fatal("Open did not return a complete newly-issued credential pair")
	}
	if !strings.HasPrefix(instance.BaseURL(), "http://127.0.0.1:") {
		t.Fatalf("base URL = %q, want an IPv4 loopback URL", instance.BaseURL())
	}

	modelsRequest, err := http.NewRequest(http.MethodGet, instance.BaseURL()+"/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	modelsRequest.Header.Set("Authorization", "Bearer "+instance.CallerToken())
	modelsResponse, err := http.DefaultClient.Do(modelsRequest)
	if err != nil {
		t.Fatal(err)
	}
	modelsBody, readErr := io.ReadAll(modelsResponse.Body)
	_ = modelsResponse.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if modelsResponse.StatusCode != http.StatusOK {
		t.Fatalf("models status = %d, body = %s", modelsResponse.StatusCode, modelsBody)
	}

	// A streamed request is admitted only after the gateway reserves a live
	// worker. Reaching 200 therefore exercises the in-process worker WebSocket,
	// not merely the public HTTP listener.
	requestContext, cancelRequest := context.WithTimeout(context.Background(), 3*time.Second)
	body := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"ping"}]}`)
	chatRequest, err := http.NewRequestWithContext(
		requestContext, http.MethodPost, instance.BaseURL()+"/v1/chat/completions", bytes.NewReader(body),
	)
	if err != nil {
		cancelRequest()
		t.Fatal(err)
	}
	chatRequest.Header.Set("Authorization", "Bearer "+instance.CallerToken())
	chatRequest.Header.Set("Content-Type", "application/json")
	chatRequest.Header.Set("Idempotency-Key", "local-worker-connected")
	chatResponse, err := http.DefaultClient.Do(chatRequest)
	if err != nil {
		cancelRequest()
		t.Fatal(err)
	}
	if chatResponse.StatusCode != http.StatusOK {
		failureBody, _ := io.ReadAll(chatResponse.Body)
		_ = chatResponse.Body.Close()
		cancelRequest()
		t.Fatalf("chat status = %d, body = %s", chatResponse.StatusCode, failureBody)
	}
	_ = chatResponse.Body.Close()
	cancelRequest()

	if err := instance.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	closed = true
	if err := instance.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	select {
	case <-instance.serveDone:
	default:
		t.Fatal("HTTP serving goroutine is still running after Close")
	}

	client := http.Client{Timeout: time.Second}
	if response, err := client.Get(instance.BaseURL() + "/v1/models"); err == nil {
		_ = response.Body.Close()
		t.Fatalf("HTTP listener still accepted requests after Close: %s", response.Status)
	}

	databaseBytes, err := os.ReadFile(config.Gateway.DatabasePath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(databaseBytes, []byte(instance.CallerToken())) {
		t.Fatal("gateway database persisted the plaintext caller token")
	}
	if bytes.Contains(databaseBytes, []byte(credentials.WorkerToken)) {
		t.Fatal("gateway database persisted the plaintext worker token")
	}

	reopened, err := gateway.Open(context.Background(), config.Gateway)
	if err != nil {
		t.Fatalf("reopen gateway after Local.Close: %v", err)
	}
	defer reopened.Close()
	for label, secret := range map[string]string{
		"caller": credentials.CallerToken,
		"worker": credentials.WorkerToken,
	} {
		if _, err := reopened.ValidateToken(context.Background(), secret); !errors.Is(err, gateway.ErrUnauthorized) {
			t.Fatalf("default %s credential after Local.Close = %v, want revoked", label, err)
		}
	}
}

func TestOpenExplicitlyPreservesAndReusesExistingCredentials(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	privateRoot := filepath.Join(root, "private")
	if err := os.Mkdir(privateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	config := Config{
		Gateway: gateway.Config{DatabasePath: filepath.Join(root, "gateway.db")},
		Worker: worker.Config{
			MirrorRoot: filepath.Join(root, "mirror"),
			OutboxPath: filepath.Join(privateRoot, "outbox.db"),
			StatePath:  filepath.Join(privateRoot, "state.db"),
		},
		ListenAddress:          "127.0.0.1:0",
		CallerSubject:          "stable-caller",
		WorkerSubject:          "stable-worker",
		IssuedCredentialPolicy: IssuedCredentialsPreserve,
	}
	first, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	credentials := first.Credentials()
	if !credentials.NewlyIssued {
		t.Fatal("first Open did not mark issued credentials")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	config.ExistingCallerToken = credentials.CallerToken
	config.ExistingWorkerToken = credentials.WorkerToken
	config.ExistingCallerKeyID = credentials.CallerKeyID
	config.ExistingWorkerKeyID = credentials.WorkerKeyID
	second, err := Open(context.Background(), config)
	if err != nil {
		t.Fatalf("reuse credentials: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if got := second.Credentials(); got.NewlyIssued || got.CallerToken != credentials.CallerToken || got.WorkerToken != credentials.WorkerToken ||
		got.CallerKeyID != credentials.CallerKeyID || got.WorkerKeyID != credentials.WorkerKeyID {
		t.Fatal("Open did not return the configured existing credential pair")
	}

	request, err := http.NewRequest(http.MethodGet, second.BaseURL()+"/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+credentials.CallerToken)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("reused caller token status = %s", response.Status)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}

	withoutKeyIDs := config
	withoutKeyIDs.ExistingCallerKeyID = ""
	withoutKeyIDs.ExistingWorkerKeyID = ""
	third, err := Open(context.Background(), withoutKeyIDs)
	if err != nil {
		t.Fatalf("reuse credentials without persisted key IDs: %v", err)
	}
	if got := third.Credentials(); got.CallerKeyID != credentials.CallerKeyID || got.WorkerKeyID != credentials.WorkerKeyID {
		_ = third.Close()
		t.Fatalf("derived credential key IDs = %+v", got)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}

	wrongCallerSubject := config
	wrongCallerSubject.CallerSubject = "different-caller"
	if opened, err := Open(context.Background(), wrongCallerSubject); err == nil {
		_ = opened.Close()
		t.Fatal("Open accepted a caller token bound to a different subject")
	}

	wrongWorkerSubject := config
	wrongWorkerSubject.WorkerSubject = "different-worker"
	if opened, err := Open(context.Background(), wrongWorkerSubject); err == nil {
		_ = opened.Close()
		t.Fatal("Open accepted a worker token bound to a different subject")
	}

	wrongCallerKey := config
	wrongCallerKey.ExistingCallerKeyID = "key_unrelated"
	if opened, err := Open(context.Background(), wrongCallerKey); err == nil {
		_ = opened.Close()
		t.Fatal("Open accepted a caller token with a different persisted key ID")
	}

	invalidCaller := config
	invalidCaller.ExistingCallerToken = credentials.WorkerToken
	if opened, err := Open(context.Background(), invalidCaller); err == nil {
		_ = opened.Close()
		t.Fatal("Open accepted a worker credential as the existing caller token")
	}

	invalidWorker := config
	invalidWorker.ExistingWorkerToken = credentials.CallerToken
	if opened, err := Open(context.Background(), invalidWorker); err == nil {
		_ = opened.Close()
		t.Fatal("Open accepted a caller credential as the existing worker token")
	}
}

func TestOpenUsesCredentialProviderBeforeStartingWorker(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	privateRoot := filepath.Join(root, "private")
	if err := os.Mkdir(privateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	providerCalled := false
	config := Config{
		Gateway: gateway.Config{DatabasePath: filepath.Join(root, "gateway.db")},
		Worker: worker.Config{
			MirrorRoot: filepath.Join(root, "mirror"),
			OutboxPath: filepath.Join(privateRoot, "outbox.db"),
			StatePath:  filepath.Join(privateRoot, "state.db"),
		},
		ListenAddress: "127.0.0.1:0",
		CallerSubject: "provided-caller",
		WorkerSubject: "provided-worker",
	}
	config.CredentialProvider = func(ctx context.Context, server *gateway.Server) (Credentials, error) {
		providerCalled = true
		caller, err := server.Issue(ctx, gateway.PrincipalCaller, config.CallerSubject)
		if err != nil {
			return Credentials{}, err
		}
		workerToken, err := server.Issue(ctx, gateway.PrincipalWorker, config.WorkerSubject)
		if err != nil {
			return Credentials{}, err
		}
		return Credentials{
			CallerToken: caller.Secret, WorkerToken: workerToken.Secret,
			CallerKeyID: caller.KeyID, WorkerKeyID: workerToken.KeyID,
		}, nil
	}

	instance, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if !providerCalled {
		t.Fatal("credential provider was not called")
	}
	if got := instance.Credentials(); got.NewlyIssued || got.CallerToken == "" || got.WorkerToken == "" {
		t.Fatalf("provided credentials = %+v", got)
	}
	provided := instance.Credentials()
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := gateway.Open(context.Background(), config.Gateway)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for label, secret := range map[string]string{
		"caller": provided.CallerToken,
		"worker": provided.WorkerToken,
	} {
		if _, err := reopened.ValidateToken(context.Background(), secret); err != nil {
			t.Fatalf("provider-owned %s credential was revoked: %v", label, err)
		}
	}
}

func TestOpenRejectsCustomAuthenticatorAndNonLoopbackListener(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	base := Config{
		Gateway: gateway.Config{DatabasePath: filepath.Join(root, "gateway.db")},
		Worker: worker.Config{
			MirrorRoot:   filepath.Join(root, "mirror"),
			OutboxPath:   filepath.Join(root, "outbox.db"),
			DisableState: true,
		},
	}
	customAuth := base
	customAuth.Gateway.Authenticator = gateway.AuthenticatorFunc(func(*http.Request) (gateway.Principal, error) {
		return gateway.Principal{}, gateway.ErrUnauthorized
	})
	if instance, err := Open(context.Background(), customAuth); err == nil {
		_ = instance.Close()
		t.Fatal("Open accepted a custom authenticator in auto-token mode")
	}

	partialCredentials := base
	partialCredentials.ExistingCallerToken = "caller-only"
	if instance, err := Open(context.Background(), partialCredentials); err == nil {
		_ = instance.Close()
		t.Fatal("Open accepted only one existing credential")
	}

	invalidPolicy := base
	invalidPolicy.IssuedCredentialPolicy = IssuedCredentialPolicy(255)
	if instance, err := Open(context.Background(), invalidPolicy); err == nil {
		_ = instance.Close()
		t.Fatal("Open accepted an invalid issued credential policy")
	}

	nonLoopback := base
	nonLoopback.ListenAddress = "0.0.0.0:0"
	if instance, err := Open(context.Background(), nonLoopback); err == nil {
		_ = instance.Close()
		t.Fatal("Open accepted a non-loopback listener")
	}
}

func TestDefaultConfigUsesEmbeddableComponentDefaults(t *testing.T) {
	t.Parallel()
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.ListenAddress != DefaultListenAddress {
		t.Fatalf("listen address = %q", config.ListenAddress)
	}
	if config.Gateway.Authenticator != nil || config.Worker.Token != "" {
		t.Fatal("default local config unexpectedly contains external auth or plaintext credentials")
	}
	for label, path := range map[string]string{
		"mirror": config.Worker.MirrorRoot,
		"outbox": config.Worker.OutboxPath,
		"state":  config.Worker.StatePath,
	} {
		if !filepath.IsAbs(path) {
			t.Fatalf("%s path = %q, want absolute", label, path)
		}
	}
}

func TestFailedOpenRollbackRevokesCallerIssuedBeforeWorker(t *testing.T) {
	t.Parallel()
	gatewayConfig := gateway.DefaultConfig()
	gatewayConfig.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	gatewayServer, err := gateway.Open(context.Background(), gatewayConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gatewayServer.Close() })
	caller, err := gatewayServer.Issue(context.Background(), gateway.PrincipalCaller, "rollback-caller")
	if err != nil {
		t.Fatal(err)
	}

	partial := &Local{
		gateway: gatewayServer,
		credentials: Credentials{
			CallerToken: caller.Secret,
			CallerKeyID: caller.KeyID,
		},
		issuedDuringOpen: true,
		shutdownTimeout:  time.Second,
	}
	if err := partial.revokeNewCredentials(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer "+caller.Secret)
	response := httptest.NewRecorder()
	gatewayServer.ModelHandler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("partially issued caller status = %d, want 401 after rollback", response.Code)
	}
}

func TestOpenRejectsPendingOfflineRestoreJournal(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "gateway.db")
	if err := os.WriteFile(databasePath+".restore-journal.json", []byte("pending"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := Config{}
	config.Gateway.DatabasePath = databasePath
	if _, err := Open(context.Background(), config); err == nil || !strings.Contains(err.Error(), "interrupted offline restore") {
		t.Fatalf("pending restore open error = %v", err)
	}
}
