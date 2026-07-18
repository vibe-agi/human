package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/hub"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

func TestHealthEndpointsSeparateLivenessReadinessAndWorkerAvailability(t *testing.T) {
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	workerHub := hub.New(4)
	server, err := NewServer(
		Config{}, database, fixedAuthenticator{}, workerHub, adapter.NewRegistry(),
		map[string]dialect.Codec{"/v1/chat/completions": openai.New()},
	)
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		server.Wait()
		_ = database.Close()
	})

	live := recordHealth(t, server, "/livez")
	assertHealth(t, live, http.StatusOK, "ok", "unchecked", false, 0, false)
	readyBeforeRecovery := recordHealth(t, server, "/readyz")
	assertHealth(t, readyBeforeRecovery, http.StatusServiceUnavailable, "not_ready", "ok", false, 0, false)
	healthBeforeRecovery := recordHealth(t, server, "/healthz")
	if readyBeforeRecovery.Code != healthBeforeRecovery.Code || readyBeforeRecovery.Body.String() != healthBeforeRecovery.Body.String() {
		t.Fatalf("healthz did not preserve readyz semantics: ready=%d %s health=%d %s",
			readyBeforeRecovery.Code, readyBeforeRecovery.Body.String(), healthBeforeRecovery.Code, healthBeforeRecovery.Body.String())
	}

	if err := server.Recover(runContext); err != nil {
		t.Fatal(err)
	}
	assertHealth(t, recordHealth(t, server, "/readyz"), http.StatusOK, "ok", "ok", true, 0, false)

	worker, err := workerHub.Register("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	assertHealth(t, recordHealth(t, server, "/readyz"), http.StatusOK, "ok", "ok", true, 1, true)
	worker.Close()

	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	assertHealth(t, recordHealth(t, server, "/readyz"), http.StatusServiceUnavailable, "not_ready", "error", true, 0, false)
	assertHealth(t, recordHealth(t, server, "/healthz"), http.StatusServiceUnavailable, "not_ready", "error", true, 0, false)
	assertHealth(t, recordHealth(t, server, "/livez"), http.StatusOK, "ok", "unchecked", true, 0, false)
}

func recordHealth(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("%s Content-Type = %q", path, contentType)
	}
	if cacheControl := response.Header().Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("%s Cache-Control = %q", path, cacheControl)
	}
	return response
}

func assertHealth(
	t *testing.T,
	response *httptest.ResponseRecorder,
	status int,
	statusText string,
	databaseStatus string,
	recoveryComplete bool,
	onlineWorkers int,
	hasOnline bool,
) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("health HTTP status = %d, want %d; body=%s", response.Code, status, response.Body.String())
	}
	var payload healthResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode health response %q: %v", response.Body.String(), err)
	}
	if payload.Status != statusText || payload.Database.Status != databaseStatus ||
		payload.Recovery.Complete != recoveryComplete || payload.Workers.Online != onlineWorkers ||
		payload.Workers.HasOnline != hasOnline {
		t.Fatalf("health response = %+v", payload)
	}
}
