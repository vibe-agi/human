package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/vibe-agi/human/internal/workerproto"
)

type publicHealthResponse struct {
	Status   string `json:"status"`
	Database struct {
		Status string `json:"status"`
	} `json:"database"`
	Recovery struct {
		Complete bool `json:"complete"`
	} `json:"recovery"`
	Workers struct {
		Online    int  `json:"online"`
		HasOnline bool `json:"has_online"`
	} `json:"workers"`
}

func TestPublicHandlersExposeUnauthenticatedOperationalHealth(t *testing.T) {
	config := DefaultConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	server, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	var connection *websocket.Conn
	t.Cleanup(func() {
		if connection != nil {
			_ = connection.Close(websocket.StatusNormalClosure, "test complete")
		}
		httpServer.Close()
		if err := server.Close(); err != nil {
			t.Errorf("close gateway: %v", err)
		}
	})

	for name, handler := range map[string]http.Handler{
		"ServeHTTP":    server,
		"ModelHandler": server.ModelHandler(),
	} {
		t.Run(name, func(t *testing.T) {
			live := publicHealth(t, handler, "/livez")
			if live.Status != "ok" || live.Database.Status != "unchecked" || !live.Recovery.Complete {
				t.Fatalf("live response = %+v", live)
			}
			ready := publicHealth(t, handler, "/readyz")
			if ready.Status != "ok" || ready.Database.Status != "ok" || !ready.Recovery.Complete ||
				ready.Workers.Online != 0 || ready.Workers.HasOnline {
				t.Fatalf("ready response without worker = %+v", ready)
			}
			health := publicHealth(t, handler, "/healthz")
			if health != ready {
				t.Fatalf("healthz = %+v, readyz = %+v", health, ready)
			}
		})
	}

	issued, err := server.Issue(context.Background(), PrincipalWorker, "health-worker")
	if err != nil {
		t.Fatal(err)
	}
	workerURL := "ws" + httpServer.URL[len("http"):] + WorkerPath
	header := http.Header{"Authorization": {"Bearer " + issued.Secret}}
	header.Set(workerproto.WorkerInstanceHeader, "health-worker-instance")
	connection, response, err := websocket.Dial(
		context.Background(), workerURL, &websocket.DialOptions{HTTPHeader: header},
	)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		t.Fatalf("connect worker: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		ready := publicHealth(t, server.ModelHandler(), "/readyz")
		if ready.Workers.Online == 1 && ready.Workers.HasOnline {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not become observable: %+v", ready)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPublicReadinessFailsAfterDatabaseRuntimeFailure(t *testing.T) {
	config := DefaultConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	server, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.database.Close(); err != nil {
		t.Fatal(err)
	}

	for name, handler := range map[string]http.Handler{
		"ServeHTTP":    server,
		"ModelHandler": server.ModelHandler(),
	} {
		t.Run(name, func(t *testing.T) {
			for _, path := range []string{"/readyz", "/healthz"} {
				request := httptest.NewRequest(http.MethodGet, path, nil)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusServiceUnavailable {
					t.Fatalf("GET %s = HTTP %d, want 503; body=%s", path, response.Code, response.Body.String())
				}
				var payload publicHealthResponse
				if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
					t.Fatal(err)
				}
				if payload.Status != "not_ready" || payload.Database.Status != "error" || !payload.Recovery.Complete {
					t.Fatalf("GET %s response = %+v", path, payload)
				}
			}
			live := publicHealth(t, handler, "/livez")
			if live.Status != "ok" || live.Database.Status != "unchecked" {
				t.Fatalf("live response after database failure = %+v", live)
			}
		})
	}
}

func publicHealth(t *testing.T, handler http.Handler, path string) publicHealthResponse {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET %s = HTTP %d, body=%s", path, response.Code, response.Body.String())
	}
	var payload publicHealthResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode GET %s response %q: %v", path, response.Body.String(), err)
	}
	return payload
}
