package harnessresolver_test

// Integration coverage: the basic resolver + bearer authenticator wired into the
// real public composition local.Open will use — llm.Service + callerhttp + an
// in-process worker (no WebSocket). It proves the port impls resolve and gate a
// request end-to-end, so the local wiring is a mechanical translation of this.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/builtin"
	"github.com/vibe-agi/human/llm/callerhttp"
	"github.com/vibe-agi/human/llm/callerhttp/bearerauth"
	"github.com/vibe-agi/human/llm/callerhttp/harnessresolver"
	"github.com/vibe-agi/human/workerkit"
)

func TestResolverAndAuthDriveWorkspaceAssignment(t *testing.T) {
	ctx := t.Context()

	store, releaseStore := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = releaseStore(context.Background()) })
	service, err := llm.NewService(ctx, llm.Config{
		DeploymentID: "harnessresolver-integration",
		Store:        framework.Borrow[llm.Store](store),
		Codecs:       builtin.Registrations(),
		Admission:    llm.AdmitAll(),
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
		ToolAuthorizer: llm.ToolAuthorizerFunc(func(context.Context, llm.ToolAuthorization) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = service.Shutdown(shutdownCtx)
	})

	resolver, err := harnessresolver.New(harnessresolver.Config{
		WorkspaceKey: workspaceKey, ExecAllowed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := bearerauth.New("local-token", "caller-local")
	if err != nil {
		t.Fatal(err)
	}
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: authenticator, Resolver: resolver, Routes: callerhttp.BuiltinRoutes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(ctx, service)
	if err != nil {
		t.Fatal(err)
	}
	callerServer := httptest.NewServer(transport)
	t.Cleanup(func() {
		callerServer.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtime.Shutdown(shutdownCtx)
	})

	// The human side runs in-process: no worker token, no WebSocket.
	connection, err := service.OpenWorker(ctx, llm.AuthenticatedWorker{WorkerID: "worker-a", SessionID: "wsess"})
	if err != nil {
		t.Fatal(err)
	}
	stateStore, releaseState := workerkit.NewMemoryStateStore()
	t.Cleanup(func() { _ = releaseState(context.Background()) })
	worker, err := workerkit.Open(ctx, workerkit.Config{Wire: workerkit.WrapConnection(connection), State: stateStore})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = worker.Shutdown(shutdownCtx)
	})

	send := func(token, session string) (*http.Response, error) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, callerServer.URL+"/v1/chat/completions", strings.NewReader(toolBody))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("User-Agent", opencodeUA)
		request.Header.Set("X-Session-Id", session)
		return http.DefaultClient.Do(request)
	}

	// A wrong token is a terminal 401 before the request is admitted.
	badResponse, err := send("wrong-token", "ses_abc")
	if err != nil {
		t.Fatal(err)
	}
	if badResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, want 401", badResponse.StatusCode)
	}
	_ = badResponse.Body.Close()

	// A valid OpenCode workspace request reaches the human worker as a Workspace
	// turn — the resolver classified it and the affinity flowed through the core.
	go func() {
		if response, doErr := send("local-token", "ses_abc"); doErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	var tier llm.CapabilityTier
	for time.Now().Before(deadline) {
		if inbox := worker.Snapshot().Inbox; len(inbox) > 0 {
			tier = inbox[0].Tier
			break
		}
		select {
		case <-worker.Notifications():
		case <-time.After(50 * time.Millisecond):
		}
	}
	if tier != llm.TierWorkspace {
		t.Fatalf("worker inbox tier = %q, want workspace (resolver classification did not flow through)", tier)
	}
}
