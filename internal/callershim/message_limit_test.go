package callershim

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vibe-agi/human/internal/workerproto"
)

func TestCompletionProxyBodyLimitBoundary(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		upstreamCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		response.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	workspace := t.TempDir()
	shim, ledger := newShimHandler(
		t, upstream.URL, false, workspace, filepath.Join(t.TempDir(), "ledger.db"), "task-limit",
	)
	t.Cleanup(func() { _ = ledger.Close() })
	if shim.config.MaxBodyBytes != workerproto.MaxWireMessageBytes {
		t.Fatalf("default shim body limit = %d, want %d", shim.config.MaxBodyBytes, workerproto.MaxWireMessageBytes)
	}
	const limit int64 = 256
	shim.config.MaxBodyBytes = limit
	server := httptest.NewServer(shim)
	t.Cleanup(server.Close)

	do := func(body []byte, key string) (int, string) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set(headerTaskID, "task-limit")
		request.Header.Set(headerIdempotencyKey, key)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		return response.StatusCode, string(data)
	}

	exact := append([]byte(`{}`), []byte(strings.Repeat(" ", int(limit)-2))...)
	if status, body := do(exact, "request-exact-limit"); status != http.StatusOK {
		t.Fatalf("exact-limit request = %d, %q", status, body)
	}
	over := append(exact, ' ')
	if status, body := do(over, "request-over-limit"); status != http.StatusRequestEntityTooLarge ||
		!strings.Contains(body, "worker wire limit") {
		t.Fatalf("over-limit request = %d, %q", status, body)
	}
	if calls := upstreamCalls.Load(); calls != 1 {
		t.Fatalf("upstream calls = %d, want only exact-limit request", calls)
	}
}
