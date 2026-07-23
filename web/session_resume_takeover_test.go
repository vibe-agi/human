package web_test

// Scenario C (RED until the availability fix): a caller's in-flight completion
// is abandoned (its socket goes away) while the human holds the task; then the
// SAME session resumes with a NEW top-level turn. The resuming completion must
// take over the existing task, not be rejected with a reconciliation conflict.
//
// Today planAdmissionTask hits its default branch (task is active, held by the
// human for the now-gone caller) and returns ErrTaskConflict → the resuming
// caller is rejected. Per the availability charter, a fresh completion carries
// full context and should preempt the dead in-flight request. This gate encodes
// that target and is RED until the takeover lands.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func TestWorkspaceSessionResumeTakesOverAbandonedTask(t *testing.T) {
	// Scenario C: a resuming session's new turn takes over the task an abandoned
	// caller still holds — the stale in-flight request is superseded at commit —
	// instead of HTTP 409 task_conflict. Formally AdmitPreemptDetached, proven
	// safe + live in HumanLLM.tla.
	ctx := t.Context()

	store, releaseStore := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = releaseStore(context.Background()) })
	service, err := llm.NewService(ctx, llm.Config{
		DeploymentID: "session-resume-takeover",
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

	const session = "ses_resume_abcdefghij0123456789"
	// Workspace tier for tool-declaring requests keyed by session; idempotency is
	// hash(session,body) so a new turn (new body) is a new request, not a retry.
	resolver := callerhttp.ResolveFunc(func(_ context.Context, request callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
		resolution := callerhttp.Resolution{Task: llm.TaskContext{CapabilityTier: llm.TierChat}}
		var probe struct {
			Tools []json.RawMessage `json:"tools"`
		}
		sessionID := request.Header.Get("X-Session-Id")
		if json.Unmarshal(request.Body, &probe) == nil && len(probe.Tools) > 0 && sessionID != "" {
			resolution.Task = llm.TaskContext{
				CapabilityTier: llm.TierWorkspace, WorkspaceKey: "ws-resume",
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

	const toolBody = `{"model":"human","stream":true,"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":%q}]}`
	send := func(reqCtx context.Context, turn string) (*http.Response, error) {
		body := strings.NewReader(fmt.Sprintf(toolBody, turn))
		request, _ := http.NewRequestWithContext(reqCtx, http.MethodPost, callerServer.URL+"/v1/chat/completions", body)
		request.Header.Set("Authorization", "Bearer hae_test")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Session-Id", session)
		return http.DefaultClient.Do(request)
	}

	// caller A: an in-flight workspace completion that parks awaiting the human.
	callerACtx, abandonCallerA := context.WithCancel(ctx)
	defer abandonCallerA()
	callerAResult := make(chan string, 1)
	go func() {
		response, doErr := send(callerACtx, "turn one")
		if doErr != nil {
			callerAResult <- "error: " + doErr.Error()
			return
		}
		if response.StatusCode != http.StatusOK {
			snippet, _ := io.ReadAll(io.LimitReader(response.Body, 600))
			callerAResult <- fmt.Sprintf("HTTP %d: %s", response.StatusCode, snippet)
			_ = response.Body.Close()
			return
		}
		callerAResult <- "HTTP 200 (parked)"
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}()

	// The human accepts A → the task is active, held for caller A.
	deadline := time.Now().Add(5 * time.Second)
	var deliveryA llm.WorkerDeliveryID
	for time.Now().Before(deadline) {
		if inbox := worker.Snapshot().Inbox; len(inbox) > 0 {
			deliveryA = inbox[0].Delivery
			break
		}
		select {
		case <-worker.Notifications():
		case <-time.After(100 * time.Millisecond):
		}
	}
	if deliveryA == "" {
		select {
		case msg := <-callerAResult:
			t.Fatalf("caller A never reached the worker inbox; caller A returned %s", msg)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("caller A never reached the worker inbox; caller A still blocked with no HTTP status")
		}
	}
	if _, err := worker.Accept(ctx, deliveryA); err != nil {
		t.Fatalf("accept caller A: %v", err)
	}

	// caller A is abandoned mid-flight (socket gone).
	abandonCallerA()
	time.Sleep(300 * time.Millisecond)

	// caller B: the session resumes with a NEW turn. It must take over the task.
	resumeCtx, cancelResume := context.WithTimeout(ctx, 3*time.Second)
	defer cancelResume()
	response, resumeErr := send(resumeCtx, "turn two, resumed after reconnect")
	if resumeErr != nil {
		// No quick HTTP status = the resume was admitted and is now parked
		// awaiting the human = takeover succeeded. This is the GREEN outcome.
		t.Logf("caller B parked awaiting the human (takeover path): %v", resumeErr)
		return
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
	if response.StatusCode != http.StatusOK {
		t.Fatalf("RED: session resume was rejected with HTTP %d instead of taking over the abandoned task:\n%s",
			response.StatusCode, payload)
	}
}
