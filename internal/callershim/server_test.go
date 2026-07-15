package callershim

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/callerfs"
)

func newShimServer(t *testing.T, gatewayURL string, allowExec bool) (*httptest.Server, string) {
	t.Helper()
	workspace := t.TempDir()
	shim, ledger := newShimHandler(t, gatewayURL, allowExec, workspace, filepath.Join(t.TempDir(), "ledger.db"), "task-default")
	t.Cleanup(func() { _ = ledger.Close() })
	httpServer := httptest.NewServer(shim)
	t.Cleanup(httpServer.Close)
	return httpServer, workspace
}

func newShimHandler(
	t *testing.T,
	gatewayURL string,
	allowExec bool,
	workspace, ledgerDSN, taskID string,
) (*Server, *SQLiteLedger) {
	t.Helper()
	return newShimHandlerWithCallerToken(t, gatewayURL, "caller-secret", allowExec, workspace, ledgerDSN, taskID)
}

func newShimHandlerWithCallerToken(
	t *testing.T,
	gatewayURL, callerToken string,
	allowExec bool,
	workspace, ledgerDSN, taskID string,
) (*Server, *SQLiteLedger) {
	t.Helper()
	root, err := callerfs.OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenSQLiteLedger(context.Background(), ledgerDSN)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutor(ExecutorConfig{Root: root, Ledger: ledger, ExecEnabled: allowExec})
	if err != nil {
		_ = ledger.Close()
		t.Fatal(err)
	}
	shim, err := NewServer(ServerConfig{
		GatewayURL: gatewayURL, CallerToken: callerToken, ToolToken: "tool-secret",
		CallerID: "caller-1", WorkspaceKey: "workspace-1", WorkspaceRoot: workspace, TaskID: taskID,
		AllowExec: allowExec, Executor: executor,
	})
	if err != nil {
		_ = ledger.Close()
		t.Fatal(err)
	}
	return shim, ledger
}

func TestProxyInjectsStableIdentityAndStreamsGatewayResponse(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		clone := request.Clone(context.Background())
		clone.Body = http.NoBody
		requests <- clone
		response.Header().Set("Content-Type", "text/event-stream")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("data: first\n\n"))
		if flusher, ok := response.(http.Flusher); ok {
			flusher.Flush()
		}
		_, _ = response.Write([]byte("data: [DONE]\n\n"))
	}))
	t.Cleanup(upstream.Close)
	shim, workspace := newShimServer(t, upstream.URL, false)
	body := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"hello"}]}`)

	const (
		taskID = "task-default"
		key    = "request-exact-retry"
	)
	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequest(http.MethodPost, shim.URL+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer malicious")
		request.Header.Set("X-Api-Key", "malicious-api-key")
		request.Header.Set(headerCapabilityTier, "workspace")
		request.Header.Set(headerCallerID, "spoofed-caller")
		request.Header.Set(headerWorkspaceKey, "spoofed-workspace")
		request.Header.Set(headerWorkspaceRoot, "/outside")
		request.Header.Set(headerHarnessID, "spoofed-harness")
		request.Header.Set(headerHarnessVersion, "999")
		request.Header.Set(headerAllowExec, "true")
		request.Header.Set("X-Human-Future-Identity", "spoofed")
		request.Header.Set(headerTaskID, taskID)
		request.Header.Set(headerIdempotencyKey, key)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(data), "[DONE]") {
			t.Fatalf("proxy response = %d, %q, %v", response.StatusCode, data, err)
		}
		if response.Header.Get(headerTaskID) != taskID || response.Header.Get(headerIdempotencyKey) != key {
			t.Fatalf("proxy response identity = task %q, key %q", response.Header.Get(headerTaskID), response.Header.Get(headerIdempotencyKey))
		}
		captured := <-requests
		if captured.Header.Get("Authorization") != "Bearer caller-secret" ||
			captured.Header.Get("X-Api-Key") != "" ||
			captured.Header.Get("X-Human-Future-Identity") != "" ||
			captured.Header.Get(headerCapabilityTier) != "remote_tools" ||
			captured.Header.Get(headerCallerID) != "caller-1" ||
			len(captured.Header.Values(headerCallerID)) != 1 ||
			captured.Header.Get(headerWorkspaceKey) != "workspace-1" ||
			captured.Header.Get(headerTaskID) != taskID ||
			captured.Header.Get(headerHarnessID) != "human-shim" ||
			captured.Header.Get(headerHarnessVersion) != "1" ||
			captured.Header.Get(headerWorkspaceRoot) != workspace ||
			captured.Header.Get(headerAllowExec) != "false" ||
			captured.Header.Get(headerIdempotencyKey) != key {
			t.Fatalf("upstream headers = %v", captured.Header)
		}
	}
}

func TestProxyDoesNotFoldIndependentRequestsWithIdenticalBodies(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		clone := request.Clone(context.Background())
		clone.Body = http.NoBody
		requests <- clone
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("data: [DONE]\n\n"))
	}))
	t.Cleanup(upstream.Close)
	shim, _ := newShimServer(t, upstream.URL, false)
	body := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"same"}]}`)

	for _, key := range []string{"request-independent-1", "request-independent-2"} {
		request, err := http.NewRequest(http.MethodPost, shim.URL+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set(headerTaskID, "task-default")
		request.Header.Set(headerIdempotencyKey, key)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("proxy response = %d, %v", response.StatusCode, readErr)
		}
		captured := <-requests
		if got := captured.Header.Get(headerIdempotencyKey); got != key {
			t.Fatalf("identical request key = %q, want %q", got, key)
		}
	}
}

func TestProxyFailsClosedWithoutExplicitStableIdentity(t *testing.T) {
	t.Parallel()
	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		response.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	body := []byte(`{"model":"human-expert","messages":[{"role":"user","content":"hello"}]}`)

	t.Run("missing idempotency key", func(t *testing.T) {
		shim, _ := newShimServer(t, upstream.URL, false)
		request, _ := http.NewRequest(http.MethodPost, shim.URL+"/v1/chat/completions", bytes.NewReader(body))
		request.Header.Set(headerTaskID, "task-default")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusPreconditionRequired {
			t.Fatalf("missing idempotency status = %d", response.StatusCode)
		}
	})

	t.Run("mismatched task id", func(t *testing.T) {
		shim, _ := newShimServer(t, upstream.URL, false)
		request, _ := http.NewRequest(http.MethodPost, shim.URL+"/v1/chat/completions", bytes.NewReader(body))
		request.Header.Set(headerIdempotencyKey, "request-explicit")
		request.Header.Set(headerTaskID, "task-not-bound-to-shim")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusPreconditionRequired {
			t.Fatalf("mismatched task status = %d", response.StatusCode)
		}
	})

	if upstreamCalls != 0 {
		t.Fatalf("identity-less requests reached upstream %d times", upstreamCalls)
	}
}

func TestToolEndpointAuthenticatesAndReplaysThroughDurableExecutor(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(upstream.Close)
	shim, workspace := newShimServer(t, upstream.URL, false)
	tool := ToolRequest{
		CallerID: "spoofed-caller-1", TaskID: "spoofed-task-1", ToolCallID: "write-1", Name: "human_write_file",
		Input: json.RawMessage(`{"path":"file.txt","content":"once","expected_sha256":"absent"}`),
	}
	payload, _ := json.Marshal(tool)

	unauthorized, err := http.Post(shim.URL+"/internal/v1/tools/execute", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.StatusCode)
	}

	call := func(requestPayload []byte) ToolResponse {
		request, _ := http.NewRequest(http.MethodPost, shim.URL+"/internal/v1/tools/execute", bytes.NewReader(requestPayload))
		request.Header.Set("Authorization", "Bearer tool-secret")
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var result ToolResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK || result.IsError {
			t.Fatalf("tool response = %d, %+v", response.StatusCode, result)
		}
		return result
	}
	call(payload)
	if err := os.WriteFile(filepath.Join(workspace, "file.txt"), []byte("caller changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool.CallerID = "spoofed-caller-2"
	tool.TaskID = "spoofed-task-2"
	forgedReplay, _ := json.Marshal(tool)
	call(forgedReplay)
	content, err := os.ReadFile(filepath.Join(workspace, "file.txt"))
	if err != nil || string(content) != "caller changed" {
		t.Fatalf("replayed tool changed workspace: %q, %v", content, err)
	}
}

func TestToolLedgerNamespaceSurvivesShimRestartAndCallerTokenRotation(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(upstream.Close)
	workspace := t.TempDir()
	ledgerDSN := filepath.Join(t.TempDir(), "ledger.db")
	tool := ToolRequest{
		CallerID: "untrusted-caller-before", TaskID: "untrusted-task-before",
		ToolCallID: "write-across-restart", Name: "human_write_file",
		Input: json.RawMessage(`{"path":"restart.txt","content":"once","expected_sha256":"absent"}`),
	}

	call := func(t *testing.T, serverURL string, request ToolRequest) ToolResponse {
		t.Helper()
		payload, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		httpRequest, err := http.NewRequest(http.MethodPost, serverURL+"/internal/v1/tools/execute", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		httpRequest.Header.Set("Authorization", "Bearer tool-secret")
		httpRequest.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(httpRequest)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var result ToolResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK || result.IsError {
			t.Fatalf("tool response = %d, %+v", response.StatusCode, result)
		}
		return result
	}

	firstShim, firstLedger := newShimHandlerWithCallerToken(
		t, upstream.URL, "caller-token-before", false, workspace, ledgerDSN, "task-stable",
	)
	firstServer := httptest.NewServer(firstShim)
	call(t, firstServer.URL, tool)
	firstServer.Close()
	if err := firstLedger.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(workspace, "restart.txt")
	if err := os.WriteFile(path, []byte("caller changed after restart"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool.CallerID = "untrusted-caller-after"
	tool.TaskID = "untrusted-task-after"
	secondShim, secondLedger := newShimHandlerWithCallerToken(
		t, upstream.URL, "caller-token-after", false, workspace, ledgerDSN, "task-stable",
	)
	t.Cleanup(func() { _ = secondLedger.Close() })
	secondServer := httptest.NewServer(secondShim)
	t.Cleanup(secondServer.Close)
	call(t, secondServer.URL, tool)

	content, err := os.ReadFile(path)
	if err != nil || string(content) != "caller changed after restart" {
		t.Fatalf("restart replay executed twice: %q, %v", content, err)
	}
}

func TestToolSchemaOmitsExecUntilExplicitlyEnabled(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(upstream.Close)
	shim, _ := newShimServer(t, upstream.URL, false)
	request, _ := http.NewRequest(http.MethodGet, shim.URL+"/internal/v1/tools/schema", nil)
	request.Header.Set("Authorization", "Bearer tool-secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || strings.Contains(string(data), "human_exec") || !strings.Contains(string(data), "human_edit_file") {
		t.Fatalf("schema status = %d, body = %s", response.StatusCode, data)
	}
}
