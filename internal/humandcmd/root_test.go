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
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
	delegationworkerws "github.com/vibe-agi/human/internal/delegation/workerws"
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

func TestServeRoutesDelegationWorkerWebSocketWithWorkerAuth(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "human.db")
	database, err := sqlite.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	delegationStore, err := delegation.OpenSQLite(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer delegationStore.Close()
	if _, err := delegationStore.CreateTask(ctx, delegation.CreateTaskInput{
		ID: "route-task", CallerID: "caller-1",
	}); err != nil {
		t.Fatal(err)
	}
	tokens := auth.NewService(database)
	workerToken, err := tokens.Issue(ctx, auth.PrincipalWorker, "route-worker")
	if err != nil {
		t.Fatal(err)
	}
	callerToken, err := tokens.Issue(ctx, auth.PrincipalCaller, "caller-1")
	if err != nil {
		t.Fatal(err)
	}
	delegationWorker, err := delegationworkerws.New(
		delegationworkerws.Config{SnapshotPoll: 10 * time.Millisecond}, tokens, delegationStore,
	)
	if err != nil {
		t.Fatal(err)
	}

	completionWorker := http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("X-Human-Route", "completion-worker")
		response.WriteHeader(http.StatusNoContent)
	})
	a2a := http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("X-Human-Route", "a2a")
		response.WriteHeader(http.StatusNoContent)
	})
	gateway := http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("X-Human-Route", "gateway")
		response.WriteHeader(http.StatusNoContent)
	})
	mux := http.NewServeMux()
	registerServeRoutes(mux, completionWorker, delegationWorker, a2a, gateway)
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

	websocketURL := "ws" + strings.TrimPrefix(server.URL, "http") + delegationworkerws.DefaultPath
	connection, response, err := websocket.Dial(ctx, websocketURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + callerToken.Secret}},
	})
	if connection != nil {
		connection.CloseNow()
	}
	if response != nil {
		defer response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("caller delegation-worker dial response = %#v, error = %v", response, err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	connection, _, err = websocket.Dial(dialCtx, websocketURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + workerToken.Secret}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	var helloEnvelope workerproto.Envelope
	if err := wsjson.Read(dialCtx, connection, &helloEnvelope); err != nil {
		t.Fatal(err)
	}
	if helloEnvelope.Type != workerproto.MessageHello {
		t.Fatalf("first delegation message = %q", helloEnvelope.Type)
	}
	var hello workerproto.Hello
	if err := json.Unmarshal(helloEnvelope.Payload, &hello); err != nil {
		t.Fatal(err)
	}
	if hello.WorkerID != "route-worker" {
		t.Fatalf("worker identity = %q", hello.WorkerID)
	}
	var snapshotEnvelope workerproto.Envelope
	if err := wsjson.Read(dialCtx, connection, &snapshotEnvelope); err != nil {
		t.Fatal(err)
	}
	if snapshotEnvelope.Type != workerproto.MessageSnapshot {
		t.Fatalf("second delegation message = %q", snapshotEnvelope.Type)
	}
	var snapshot workerproto.Snapshot
	if err := json.Unmarshal(snapshotEnvelope.Payload, &snapshot); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Validate(); err != nil || snapshot.Snapshot.Task.ID != "route-task" ||
		snapshot.Reason != workerproto.ReasonRecovery {
		t.Fatalf("delegation snapshot = %#v, error = %v", snapshot, err)
	}
}
