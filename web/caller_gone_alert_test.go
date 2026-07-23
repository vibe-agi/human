package web_test

// Scenario B: when a caller's socket goes away while the human holds the task,
// the human must see a "caller disconnected" alert instead of replying into the
// void. The signal flows through the ports that scenario B fills in:
//
//	caller disconnect (callerhttp ctx.Err)
//	  -> service.ReportCallerGone
//	  -> serviceWorkerConnection (WorkerNoticer)
//	  -> connectionWire (workerkit.NoticeSource)
//	  -> workerkit State.Alerts (web shows it)

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"github.com/vibe-agi/human/workerkit"
)

func TestCallerDisconnectSurfacesNoticeToHuman(t *testing.T) {
	ctx := t.Context()

	store, releaseStore := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = releaseStore(context.Background()) })
	service, err := llm.NewService(ctx, llm.Config{
		DeploymentID: "caller-gone-alert",
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

	const session = "ses_gone_abcdefghij0123456789"
	resolver := callerhttp.ResolveFunc(func(_ context.Context, request callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
		resolution := callerhttp.Resolution{Task: llm.TaskContext{CapabilityTier: llm.TierChat}}
		var probe struct {
			Tools []json.RawMessage `json:"tools"`
		}
		sessionID := request.Header.Get("X-Session-Id")
		if json.Unmarshal(request.Body, &probe) == nil && len(probe.Tools) > 0 && sessionID != "" {
			resolution.Task = llm.TaskContext{
				CapabilityTier: llm.TierWorkspace, WorkspaceKey: "ws-gone",
				HarnessID: "opencode", HarnessVersion: "1.17.18",
				HarnessSessionID: sessionID, ExecAllowed: true,
			}
		}
		digest := sha256.Sum256(append([]byte(sessionID+"\x00"), request.Body...))
		resolution.IdempotencyKey = llm.IdempotencyKey("k-" + hex.EncodeToString(digest[:16]))
		return resolution, nil
	})
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: callerhttp.AuthenticateFunc(func(context.Context, *http.Request) (callerhttp.Identity, error) {
			return callerhttp.Identity{CallerID: "caller-oc"}, nil
		}),
		Resolver: resolver,
		Routes:   callerhttp.BuiltinRoutes(),
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

	connection, err := service.OpenWorker(ctx, llm.AuthenticatedWorker{WorkerID: "worker-a", SessionID: "wsess"})
	if err != nil {
		t.Fatal(err)
	}
	stateStore, releaseState := workerkit.NewMemoryStateStore()
	t.Cleanup(func() { _ = releaseState(context.Background()) })
	worker, err := workerkit.Open(ctx, workerkit.Config{
		Wire: workerkit.WrapConnection(connection), State: stateStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = worker.Shutdown(shutdownCtx)
	})

	// caller: an in-flight workspace completion that parks awaiting the human.
	callerCtx, disconnect := context.WithCancel(ctx)
	defer disconnect()
	go func() {
		body := `{"model":"human","stream":true,"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"turn one"}]}`
		request, _ := http.NewRequestWithContext(callerCtx, http.MethodPost, callerServer.URL+"/v1/chat/completions", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer hae_test")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Session-Id", session)
		if response, doErr := http.DefaultClient.Do(request); doErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}()

	// human accepts → holds the task.
	deadline := time.Now().Add(5 * time.Second)
	var delivery llm.WorkerDeliveryID
	for time.Now().Before(deadline) {
		if inbox := worker.Snapshot().Inbox; len(inbox) > 0 {
			delivery = inbox[0].Delivery
			break
		}
		select {
		case <-worker.Notifications():
		case <-time.After(100 * time.Millisecond):
		}
	}
	if delivery == "" {
		t.Fatal("caller never reached the worker inbox")
	}
	if _, err := worker.Accept(ctx, delivery); err != nil {
		t.Fatalf("accept: %v", err)
	}

	// caller disconnects mid-flight.
	disconnect()

	// the human must see a caller-gone alert.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, alert := range worker.Snapshot().Alerts {
			if alert.Code == "caller_gone" {
				return // GREEN: the disconnect surfaced to the human.
			}
		}
		select {
		case <-worker.Notifications():
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("human never saw a caller_gone alert after the caller disconnected")
}
