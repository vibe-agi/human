package local

// The migrated-product door: real OpenCode 1.17.18 against the REAL local
// product in web mode — the public llm.Service/callerhttp composition, its
// in-process workerkit connection, and the human side operated exclusively
// through the web HTTP API. No TUI process is involved.
//
//	HUMAN_REAL_OPENCODE_E2E=1 go test ./local -run TestRealOpenCodeLocalWebMode -count=1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRealOpenCodeLocalWebMode(t *testing.T) {
	if os.Getenv("HUMAN_REAL_OPENCODE_E2E") != "1" {
		t.Skip("set HUMAN_REAL_OPENCODE_E2E=1 to run the installed OpenCode CLI")
	}
	binary, err := exec.LookPath("opencode")
	if err != nil {
		t.Skip("opencode is not installed")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != "1.17.18" {
		t.Skipf("requires opencode 1.17.18; got %q (%v)", strings.TrimSpace(string(version)), err)
	}

	root := t.TempDir()
	privateRoot := filepath.Join(root, "private")
	if err := os.Mkdir(privateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	instance, err := Open(context.Background(), Config{
		Public:             PublicStackConfig{DatabasePath: filepath.Join(root, "store.db")},
		HumanWorkspaceRoot: filepath.Join(root, "mirror"),
		ListenAddress:      "127.0.0.1:0",
		WebListenAddress:   "127.0.0.1:0",
		WebStatePath:       filepath.Join(privateRoot, "workerkit-state.db"),
		CallerSubject:      "door-caller",
		WorkerSubject:      "door-worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	webURL := instance.WebURL()
	base := webURL[:strings.Index(webURL, "/?token=")]
	token := webURL[strings.Index(webURL, "?token=")+len("?token="):]
	webJSON := func(method, path string, body any) (map[string]any, int) {
		var reader io.Reader
		if body != nil {
			encoded, _ := json.Marshal(body)
			reader = bytes.NewReader(encoded)
		}
		request, _ := http.NewRequest(method, base+path, reader)
		request.Header.Set("Authorization", "Bearer "+token)
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return nil, 0
		}
		defer response.Body.Close()
		var decoded map[string]any
		_ = json.NewDecoder(response.Body).Decode(&decoded)
		return decoded, response.StatusCode
	}

	const finalAnswer = "LOCAL-WEB-DOOR-FINAL: the migrated product loop is complete"
	var handled atomic.Int64
	operatorCtx, stopOperator := context.WithCancel(context.Background())
	defer stopOperator()
	go func() {
		for {
			select {
			case <-operatorCtx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			state, status := webJSON(http.MethodGet, "/api/state", nil)
			if status != http.StatusOK {
				continue
			}
			inbox, _ := state["inbox"].([]any)
			for _, raw := range inbox {
				item, _ := raw.(map[string]any)
				accepted, status := webJSON(http.MethodPost, "/api/accept",
					map[string]any{"delivery": item["delivery"]})
				if status != http.StatusOK {
					continue
				}
				key, _ := accepted["key"].(map[string]any)
				webJSON(http.MethodPost, "/api/final", map[string]any{
					"caller": fmt.Sprint(key["caller"]), "task_id": fmt.Sprint(key["task_id"]),
					"text": finalAnswer,
				})
				handled.Add(1)
			}
		}
	}()

	workspace := t.TempDir()
	configHome := filepath.Join(t.TempDir(), "config")
	dataHome := filepath.Join(t.TempDir(), "data")
	for _, directory := range []string{configHome, dataHome} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	providerConfig := map[string]any{
		"$schema": "https://opencode.ai/config.json",
		"provider": map[string]any{"human-local": map[string]any{
			"npm": "@ai-sdk/openai-compatible", "name": "Human Local Web",
			"options": map[string]any{
				"baseURL": instance.BaseURL() + "/v1", "apiKey": instance.CallerToken(),
			},
			"models": map[string]any{"human-expert": map[string]any{
				"name": "Human Expert", "limit": map[string]int{"context": 100000, "output": 4096},
			}},
		}},
	}
	payload, err := json.MarshalIndent(providerConfig, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "opencode.json"), append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "run", "--pure", "--auto", "--format", "json",
		"--model", "human-local/human-expert", "--dir", workspace,
		"Relay the Human expert's final answer verbatim.")
	command.Dir = workspace
	command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+configHome, "XDG_DATA_HOME="+dataHome)
	output, runErr := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("real OpenCode timed out: %v\n%s", ctx.Err(), output)
	}
	if runErr != nil {
		t.Fatalf("real OpenCode failed: %v\n%s", runErr, output)
	}
	if !strings.Contains(string(output), "LOCAL-WEB-DOOR-FINAL") {
		t.Fatalf("OpenCode output lacks the web-delivered final:\n%s", output)
	}
	firstHandled := handled.Load()
	if firstHandled < 1 {
		t.Fatalf("web operator handled %d conversations, want at least 1", firstHandled)
	}

	// Second top-level user turn in the SAME OpenCode session must reach the
	// human again and complete through the web API.
	sessionID := ""
	for _, line := range strings.Split(string(output), "\n") {
		var event struct {
			SessionID string `json:"sessionID"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.SessionID != "" {
			sessionID = event.SessionID
			break
		}
	}
	if sessionID == "" {
		t.Fatalf("no session id in OpenCode output:\n%s", output)
	}
	second := exec.CommandContext(ctx, binary, "run", "--pure", "--auto", "--format", "json",
		"--model", "human-local/human-expert", "--dir", workspace, "--session", sessionID,
		"Handle this second top-level user turn in the same session.")
	second.Dir = workspace
	second.Env = command.Env
	secondOutput, secondErr := second.CombinedOutput()
	if secondErr != nil || ctx.Err() != nil {
		t.Fatalf("second OpenCode turn failed: %v %v\n%s", secondErr, ctx.Err(), secondOutput)
	}
	stopOperator()
	if !strings.Contains(string(secondOutput), "LOCAL-WEB-DOOR-FINAL") {
		t.Fatalf("second turn lacks the web-delivered final:\n%s", secondOutput)
	}
	if handled.Load() <= firstHandled {
		t.Fatalf("second turn did not reach the web operator (handled %d)", handled.Load())
	}
}
