package humandcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()
	command := New()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs(args)
	err := command.ExecuteContext(context.Background())
	return output.String(), err
}

func TestMigrateCreatesDatabase(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "nested", "human.db")
	if _, err := execute(t, "--db", databasePath, "migrate"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(databasePath); err != nil {
		t.Fatalf("database was not created: %v", err)
	}
}

func TestTokenIssueAndRevoke(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "human.db")
	output, err := execute(t, "--db", databasePath, "token", "issue", "--type", "worker", "--subject", "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	var issued map[string]string
	if err := json.Unmarshal([]byte(output), &issued); err != nil {
		t.Fatalf("decode issue output %q: %v", output, err)
	}
	database, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	service := auth.NewService(database)
	principal, err := service.Authenticate(context.Background(), issued["token"])
	if err != nil {
		t.Fatal(err)
	}
	if principal.Type != auth.PrincipalWorker || principal.SubjectID != "worker-1" {
		t.Fatalf("principal = %+v", principal)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := execute(t, "--db", databasePath, "token", "revoke", "--key-id", issued["key_id"]); err != nil {
		t.Fatal(err)
	}
	database, err = sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := auth.NewService(database).Authenticate(context.Background(), issued["token"]); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("revoked token error = %v", err)
	}
}

func TestExplicitConfigFile(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "from-config.db")
	configPath := filepath.Join(directory, "human.toml")
	if err := os.WriteFile(configPath, []byte("db = \""+databasePath+"\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := execute(t, "--config", configPath, "migrate"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(databasePath); err != nil {
		t.Fatalf("configured database was not created: %v", err)
	}
}

func TestVersion(t *testing.T) {
	t.Parallel()
	output, err := execute(t, "version")
	if err != nil {
		t.Fatal(err)
	}
	if output != Version+"\n" {
		t.Fatalf("version output = %q", output)
	}
}

func TestServeFlagsAreCompletionOnly(t *testing.T) {
	t.Parallel()
	serve, _, err := New().Find([]string{"serve"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"listen", "queue-capacity", "heartbeat", "max-pending", "rate-limit",
		"rate-burst", "audit-payload", "audit-payload-ttl", "shutdown-timeout",
	} {
		if serve.Flags().Lookup(name) == nil {
			t.Fatalf("completion serve flag %q is not registered", name)
		}
	}
	for _, name := range []string{"a2a-base-url", "remote-exec"} {
		if serve.Flags().Lookup(name) != nil {
			t.Fatalf("removed serve flag %q is still registered", name)
		}
	}
}

func TestServeRoutesCompletionOnly(t *testing.T) {
	t.Parallel()
	completionWorker := http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("X-Human-Route", "completion-worker")
		response.WriteHeader(http.StatusNoContent)
	})
	gateway := http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("X-Human-Route", "gateway")
		response.WriteHeader(http.StatusNoContent)
	})
	mux := http.NewServeMux()
	registerServeRoutes(mux, completionWorker, gateway)
	server := httptest.NewServer(mux)
	defer server.Close()

	response, err := http.Get(server.URL + "/internal/v1/worker/ws")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent || response.Header.Get("X-Human-Route") != "completion-worker" {
		t.Fatalf("completion route = %d / %q", response.StatusCode, response.Header.Get("X-Human-Route"))
	}

	for _, path := range []string{"/v1/chat/completions", "/v1/messages", "/v1/responses"} {
		response, err = http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNoContent || response.Header.Get("X-Human-Route") != "gateway" {
			t.Fatalf("gateway route %q = %d / %q", path, response.StatusCode, response.Header.Get("X-Human-Route"))
		}
	}
}
