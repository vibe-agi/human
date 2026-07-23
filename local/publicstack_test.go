package local

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
)

const publicStackOpencodeUA = "opencode/1.17.18 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"

func publicStackWorkspaceBody(text string) string {
	return fmt.Sprintf(
		`{"model":"human","stream":true,"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":%q}]}`,
		text,
	)
}

// TestOpenPublicStackServesOpenCodeWorkspace proves the migrated composition: the
// caller endpoint (callerhttp + basic resolver/authenticator) and the in-process
// worker share one llm.Service, no gateway or worker WebSocket involved. A valid
// OpenCode workspace completion reaches the human inbox as a Workspace turn; a
// wrong caller token is rejected.
func TestOpenPublicStackServesOpenCodeWorkspace(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	privateRoot := filepath.Join(root, "private")
	if err := os.Mkdir(privateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	config := Config{
		HumanWorkspaceRoot: filepath.Join(root, "mirror"),
		Public:             PublicStackConfig{DatabasePath: filepath.Join(root, "store.db")},
		ListenAddress:      "127.0.0.1:0",
		WebListenAddress:   "127.0.0.1:0",
		WebStatePath:       filepath.Join(privateRoot, "workerkit-state.db"),
		ShutdownTimeout:    5 * time.Second,
	}
	instance, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	if info, err := os.Stat(config.HumanWorkspaceRoot); err != nil || !info.IsDir() {
		t.Fatalf("exact Human mirror was not created: info=%v error=%v", info, err)
	}
	if _, err := os.Stat(filepath.Join(config.HumanWorkspaceRoot, "web")); !os.IsNotExist(err) {
		t.Fatalf("local stack created an implicit web mirror child: %v", err)
	}

	if instance.WebWorker() == nil {
		t.Fatal("public stack did not compose the workerkit human side")
	}
	if instance.credentials.CallerToken == "" || instance.baseURL == "" {
		t.Fatalf("public stack did not expose a caller token/url: token=%q url=%q", instance.credentials.CallerToken, instance.baseURL)
	}
	send := func(token string) (*http.Response, error) {
		request, _ := http.NewRequest(http.MethodPost, instance.baseURL+"/v1/chat/completions", strings.NewReader(publicStackWorkspaceBody("fix the bug")))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("User-Agent", publicStackOpencodeUA)
		request.Header.Set("X-Session-Id", "ses_smoke_1234567890")
		return http.DefaultClient.Do(request)
	}

	// Wrong token → terminal 401.
	badResponse, err := send("wrong-token")
	if err != nil {
		t.Fatal(err)
	}
	if badResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d, want 401", badResponse.StatusCode)
	}
	_ = badResponse.Body.Close()

	// Valid OpenCode workspace completion parks awaiting the human; it must reach
	// the inbox as a Workspace turn.
	go func() {
		if response, doErr := send(instance.credentials.CallerToken); doErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}()

	worker := instance.WebWorker()
	deadline := time.Now().Add(5 * time.Second)
	var tier llm.CapabilityTier
	for time.Now().Before(deadline) {
		if inbox := worker.Snapshot().Inbox; len(inbox) > 0 {
			tier = inbox[0].Tier
			break
		}
		select {
		case <-worker.Notifications():
		case <-time.After(50 * time.Millisecond):
		}
	}
	if tier != llm.TierWorkspace {
		t.Fatalf("worker inbox tier = %q, want workspace", tier)
	}
}
