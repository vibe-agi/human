package web_test

// The first real Claude Code door: the actual claude CLI speaks Anthropic
// Messages to the EMBEDDED public kernel while the human side is operated
// through the web HTTP API. Basic/Chat text loop only; captured wire facts
// this door encodes: POST /v1/messages?beta=true, x-api-key authentication,
// no Idempotency-Key (identity derived from the exact body), system as a
// block array, top-level thinking/context_management fields, and a
// metadata.user_id JSON carrying the session id.
//
//	HUMAN_REAL_CLAUDE_E2E=1 go test ./web -run TestRealClaudeCodeWebBasicLoop -count=1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/builtin"
	"github.com/vibe-agi/human/llm/callerhttp"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
)

func TestRealClaudeCodeWebBasicLoop(t *testing.T) {
	if os.Getenv("HUMAN_REAL_CLAUDE_E2E") != "1" {
		t.Skip("set HUMAN_REAL_CLAUDE_E2E=1 to run the installed Claude Code CLI")
	}
	binary, err := exec.LookPath("claude")
	if err != nil {
		t.Skip("claude is not installed")
	}

	store, releaseStore := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = releaseStore(context.Background()) })
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "claude-web-door",
		Store:        framework.Borrow[llm.Store](store),
		Codecs:       builtin.Registrations(),
		Admission:    llm.AdmitAll(),
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = service.Shutdown(ctx)
	})

	// Claude Code sends no Idempotency-Key; its Stainless SDK retries the
	// exact body (X-Stainless-Retry-Count changes, the payload does not), so
	// the body digest is the retry identity.
	resolver := callerhttp.ResolveFunc(func(_ context.Context, request callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
		digest := sha256.Sum256(request.Body)
		return callerhttp.Resolution{
			IdempotencyKey: llm.IdempotencyKey("cc-" + hex.EncodeToString(digest[:16])),
			Task:           llm.TaskContext{CapabilityTier: llm.TierChat},
		}, nil
	})
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: callerhttp.AuthenticateFunc(func(_ context.Context, request *http.Request) (callerhttp.Identity, error) {
			if request.Header.Get("x-api-key") == "" {
				return callerhttp.Identity{}, &framework.Fault{
					Code: framework.CodeUnauthenticated, Retry: framework.RetryNever,
				}
			}
			return callerhttp.Identity{CallerID: "caller-claude"}, nil
		}),
		Resolver: resolver,
		Routes:   callerhttp.BuiltinRoutes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), service)
	if err != nil {
		t.Fatal(err)
	}
	callerServer := httptest.NewServer(transport)
	t.Cleanup(func() {
		callerServer.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtime.Shutdown(ctx)
	})

	connection, err := service.OpenWorker(t.Context(), llm.AuthenticatedWorker{
		WorkerID: "worker-a", SessionID: "session-claude-door",
	})
	if err != nil {
		t.Fatal(err)
	}
	stateStore, _ := workerkit.NewMemoryStateStore()
	worker, err := workerkit.Open(t.Context(), workerkit.Config{
		Wire: workerkit.WrapConnection(connection), State: stateStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	webServer, err := web.New(web.Config{Worker: worker, SessionToken: testToken})
	if err != nil {
		t.Fatal(err)
	}
	webListener := httptest.NewServer(webServer)
	t.Cleanup(func() {
		webListener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = webServer.Shutdown(ctx)
		_ = worker.Shutdown(ctx)
	})

	const finalAnswer = "CLAUDE-WEB-DOOR-FINAL: the human answered through the browser API"
	var handled atomic.Int64
	operatorCtx, stopOperator := context.WithCancel(context.Background())
	defer stopOperator()
	go func() {
		for {
			select {
			case <-operatorCtx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			state := doJSON(t, authedRequest(t, http.MethodGet, webListener.URL+"/api/state", nil), http.StatusOK)
			inbox, _ := state["inbox"].([]any)
			for _, raw := range inbox {
				item, _ := raw.(map[string]any)
				delivery, _ := item["delivery"].(string)
				accepted := doJSON(t, authedRequest(t, http.MethodPost, webListener.URL+"/api/accept",
					map[string]string{"delivery": delivery}), http.StatusOK)
				key, _ := accepted["key"].(map[string]any)
				doJSON(t, authedRequest(t, http.MethodPost, webListener.URL+"/api/final", map[string]any{
					"caller": key["caller"], "task_id": key["task_id"], "text": finalAnswer,
				}), http.StatusOK)
				handled.Add(1)
			}
		}
	}()

	workspace := t.TempDir()
	configDir := filepath.Join(t.TempDir(), "claude-config")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-p", "Relay the Human expert's final answer verbatim.",
		"--model", "claude-sonnet-4-5")
	command.Dir = workspace
	command.Env = append(os.Environ(),
		"ANTHROPIC_BASE_URL="+callerServer.URL,
		"ANTHROPIC_API_KEY=hae_test",
		"CLAUDE_CONFIG_DIR="+configDir,
		"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1",
	)
	output, runErr := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("real Claude Code timed out: %v\n%s", ctx.Err(), output)
	}
	if runErr != nil {
		t.Fatalf("real Claude Code failed: %v\n%s", runErr, output)
	}
	stopOperator()
	if !strings.Contains(string(output), "CLAUDE-WEB-DOOR-FINAL") {
		t.Fatalf("Claude Code output lacks the web-delivered final:\n%s", output)
	}
	if handled.Load() < 1 {
		t.Fatalf("web operator handled %d conversations, want at least 1", handled.Load())
	}
}
