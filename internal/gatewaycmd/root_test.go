package gatewaycmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()
	command := NewGatewayCommand()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs(args)
	err := command.ExecuteContext(context.Background())
	return output.String(), err
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

func TestTokenAdministrationRejectsUnstableIdentifiers(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "human.db")
	if _, err := execute(t, "--db", databasePath, "token", "issue", "--subject", "tenant/user"); err == nil ||
		!strings.Contains(err.Error(), "stable key") {
		t.Fatalf("unstable subject error = %v", err)
	}
	if _, err := execute(t, "--db", databasePath, "token", "revoke", "--key-id", "../key"); err == nil ||
		!strings.Contains(err.Error(), "stable key") {
		t.Fatalf("unstable key error = %v", err)
	}
}

func TestServeFlagsAreCompletionOnly(t *testing.T) {
	t.Parallel()
	serve := NewGatewayCommand()
	for _, name := range []string{
		"listen", "queue-capacity", "heartbeat", "stream-write-timeout", "max-pending", "disable-codex-auto-idempotency", "rate-limit",
		"rate-burst", "audit-payload", "audit-payload-ttl", "replay-payload-grace", "shutdown-timeout",
	} {
		if serve.Flags().Lookup(name) == nil {
			t.Fatalf("completion serve flag %q is not registered", name)
		}
	}
	if got := serve.Flags().Lookup("replay-payload-grace").DefValue; got != "24h0m0s" {
		t.Fatalf("replay payload grace default = %q", got)
	}
	if got := serve.Flags().Lookup("stream-write-timeout").DefValue; got != "10s" {
		t.Fatalf("stream write timeout default = %q", got)
	}
	for _, name := range []string{"a2a-base-url", "remote-exec"} {
		if serve.Flags().Lookup(name) != nil {
			t.Fatalf("removed serve flag %q is still registered", name)
		}
	}
}

func TestHumanGatewayFormRunsDirectlyAndKeepsTokenCommands(t *testing.T) {
	t.Parallel()
	command := NewGatewayCommand()
	if command.RunE == nil {
		t.Fatal("gateway command does not run the server directly")
	}
	if command.Flag("listen") == nil || command.Flag("db") == nil {
		t.Fatal("gateway command is missing serve flags")
	}
	for _, path := range [][]string{{"token", "issue"}, {"token", "revoke"}} {
		if _, _, err := command.Find(path); err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
	}
}

func TestServeRejectsNonPositiveReplayPayloadGrace(t *testing.T) {
	t.Parallel()
	_, err := execute(t, "--db", filepath.Join(t.TempDir(), "human.db"),
		"--replay-payload-grace", "0s")
	if err == nil || !strings.Contains(err.Error(), "replay payload grace must be positive") {
		t.Fatalf("non-positive replay payload grace error = %v", err)
	}
}
