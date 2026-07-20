package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/gateway"
)

// Claude Code probes count_tokens on startup; a 404 surfaces as "model may
// not exist". The endpoint must be caller-authenticated and return a
// plausible estimate.
func TestCountTokensEndpointIsAuthenticatedAndEstimates(t *testing.T) {
	config := gateway.DefaultConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	server, err := gateway.Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	caller, err := server.Issue(context.Background(), gateway.PrincipalCaller, "count-caller")
	if err != nil {
		t.Fatal(err)
	}
	worker, err := server.Issue(context.Background(), gateway.PrincipalWorker, "count-worker")
	if err != nil {
		t.Fatal(err)
	}
	listener := httptest.NewServer(server)
	t.Cleanup(listener.Close)

	body := []byte(`{"model":"claude-x","system":[{"type":"text","text":"be helpful"}],` +
		`"messages":[{"role":"user","content":"count me please"}],"tools":[]}`)
	do := func(token string) (*http.Response, map[string]any) {
		request, _ := http.NewRequest(http.MethodPost, listener.URL+"/v1/messages/count_tokens", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		if token != "" {
			request.Header.Set("x-api-key", token)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var decoded map[string]any
		_ = json.NewDecoder(response.Body).Decode(&decoded)
		return response, decoded
	}

	response, decoded := do(caller.Secret)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("caller count_tokens = %d (%v)", response.StatusCode, decoded)
	}
	tokens, _ := decoded["input_tokens"].(float64)
	if tokens < 1 {
		t.Fatalf("input_tokens = %v, want >= 1", decoded)
	}

	if response, decoded = do(""); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated count_tokens = %d (%v)", response.StatusCode, decoded)
	}
	if response, decoded = do(worker.Secret); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("worker-token count_tokens = %d (%v)", response.StatusCode, decoded)
	}
}
