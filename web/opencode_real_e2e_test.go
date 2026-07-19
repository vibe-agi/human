package web_test

// The M2 web door: a real OpenCode CLI drives the EMBEDDED public kernel
// (llm.Service + callerhttp, no legacy gateway) while the human side is
// operated exclusively through the web HTTP API — the exact composition a
// third-party embedder runs. Basic/Chat tier text loop only; the Live
// Workspace door arrives with M3/M4.
//
//	HUMAN_REAL_OPENCODE_E2E=1 go test ./web -run TestRealOpenCodeWebBasicLoop -count=1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

const openCodeVersion = "1.17.18"

func TestRealOpenCodeWebBasicLoop(t *testing.T) {
	if os.Getenv("HUMAN_REAL_OPENCODE_E2E") != "1" {
		t.Skip("set HUMAN_REAL_OPENCODE_E2E=1 to run the installed OpenCode CLI")
	}
	binary, err := exec.LookPath("opencode")
	if err != nil {
		t.Skip("opencode is not installed")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != openCodeVersion {
		t.Skipf("requires opencode %s; got %q (%v)", openCodeVersion, strings.TrimSpace(string(version)), err)
	}

	// --- Embedded public kernel: store → service → HTTP caller transport.
	store, releaseStore := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = releaseStore(context.Background()) })
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "web-real-door",
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

	// Host-side resolver: OpenCode does not send Idempotency-Key, so the host
	// derives a stable per-request key from its X-Session-Id and the exact body.
	resolver := callerhttp.ResolveFunc(func(_ context.Context, request callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
		digest := sha256.Sum256(append([]byte(request.Header.Get("X-Session-Id")+"\x00"), request.Body...))
		return callerhttp.Resolution{
			IdempotencyKey: llm.IdempotencyKey("oc-" + hex.EncodeToString(digest[:16])),
			Task:           llm.TaskContext{CapabilityTier: llm.TierChat},
		}, nil
	})
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: callerhttp.AuthenticateFunc(func(context.Context, *http.Request) (callerhttp.Identity, error) {
			return callerhttp.Identity{CallerID: "caller-opencode"}, nil
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

	// --- Human side: workerkit over the in-process wire, operated via web API.
	connection, err := service.OpenWorker(t.Context(), llm.AuthenticatedWorker{
		WorkerID: "worker-a", SessionID: "session-web-door",
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

	// The human operator, scripted entirely through the web HTTP API: accept
	// every inbox item and deliver a final answer.
	const finalAnswer = "WEB-DOOR-FINAL: the embedded kernel web loop is complete"
	var handled atomic.Int64
	operatorCtx, stopOperator := context.WithCancel(context.Background())
	defer stopOperator()
	operatorDone := make(chan error, 1)
	go func() {
		operatorDone <- func() error {
			for {
				select {
				case <-operatorCtx.Done():
					return nil
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
					doJSON(t, authedRequest(t, http.MethodPost, webListener.URL+"/api/final", map[string]string{
						"caller": fmt.Sprint(key["caller"]), "task_id": fmt.Sprint(key["task_id"]),
						"text": finalAnswer,
					}), http.StatusOK)
					handled.Add(1)
				}
			}
		}()
	}()

	// --- Real OpenCode CLI pointed at the embedded kernel (Basic/Chat tier).
	workspace := t.TempDir()
	configHome := filepath.Join(t.TempDir(), "config")
	dataHome := filepath.Join(t.TempDir(), "data")
	for _, directory := range []string{configHome, dataHome} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	config := map[string]any{
		"$schema": "https://opencode.ai/config.json",
		"provider": map[string]any{"human-web": map[string]any{
			"npm": "@ai-sdk/openai-compatible", "name": "Human Web Door",
			"options": map[string]any{
				"baseURL": callerServer.URL + "/v1", "apiKey": "hae_test",
			},
			"models": map[string]any{"human": map[string]any{
				"name": "Human", "limit": map[string]int{"context": 100000, "output": 4096},
			}},
		}},
	}
	payload, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "opencode.json"), append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "run", "--pure", "--auto", "--format", "json",
		"--model", "human-web/human", "--dir", workspace,
		"Relay the Human expert's final answer verbatim.")
	command.Dir = workspace
	command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+configHome, "XDG_DATA_HOME="+dataHome)
	output, runErr := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("real OpenCode timed out: %v\n%s", ctx.Err(), output)
	}
	if runErr != nil {
		t.Fatalf("real OpenCode failed: %v\n%s", runErr, output)
	}
	stopOperator()
	if err := <-operatorDone; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(output), "WEB-DOOR-FINAL") {
		t.Fatalf("OpenCode output does not contain the web-delivered final answer:\n%s", output)
	}
	if handled.Load() < 1 {
		t.Fatalf("web operator handled %d conversations, want at least 1", handled.Load())
	}
}
