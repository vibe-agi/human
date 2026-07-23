package local

// Real current-Codex release door: Codex speaks Responses to the public local
// stack while the scripted human uses only the web JSON API. The first Human
// turn issues Codex's declared exec_command tool; Codex executes it in its own
// workspace and returns function_call_output on the same turn affinity; the
// human then delivers the final response.
//
//	HUMAN_REAL_CODEX_E2E=1 go test ./local \
//	  -run TestRealCodexLocalPublicStackToolLoop -count=1 -v

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const publicStackCodexVersion = "codex-cli 0.145.0"

func TestRealCodexLocalPublicStackToolLoop(t *testing.T) {
	if os.Getenv("HUMAN_REAL_CODEX_E2E") != "1" {
		t.Skip("set HUMAN_REAL_CODEX_E2E=1 to run the installed Codex CLI")
	}
	binary, err := exec.LookPath("codex")
	if err != nil {
		t.Skip("codex is not installed")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != publicStackCodexVersion {
		t.Skipf("requires %s; got %q (%v)", publicStackCodexVersion, strings.TrimSpace(string(version)), err)
	}

	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	privateRoot := filepath.Join(root, "private")
	for _, directory := range []string{workspace, privateRoot} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	instance, err := Open(context.Background(), Config{
		HumanWorkspaceRoot: filepath.Join(root, "mirror"),
		Public: PublicStackConfig{
			DatabasePath: filepath.Join(privateRoot, "store.db"), MaxPending: 45 * time.Second,
		},
		ListenAddress:    "127.0.0.1:0",
		WebListenAddress: "127.0.0.1:0",
		WebStatePath:     filepath.Join(privateRoot, "workerkit-state.db"),
		ShutdownTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	api := webAPIForLocal(t, instance, &http.Client{Timeout: 5 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	operatorDone := make(chan error, 1)
	go runCodexWebOperator(ctx, api, workspace, operatorDone)

	codexHome := filepath.Join(privateRoot, "codex-home")
	if err := os.Mkdir(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	lastMessage := filepath.Join(privateRoot, "last-message.txt")
	provider := fmt.Sprintf(
		`model_providers.human_e2e={ name = "Human E2E", base_url = %q, env_key = "HUMAN_CODEX_API_KEY", wire_api = "responses", request_max_retries = 0, stream_max_retries = 0, stream_idle_timeout_ms = 10000 }`,
		instance.BaseURL()+"/v1",
	)
	command := exec.CommandContext(ctx, binary,
		"--ask-for-approval", "never",
		"exec", "--ignore-user-config", "--ephemeral", "--skip-git-repo-check",
		"--json", "--color", "never", "--sandbox", "read-only", "--cd", workspace,
		"--model", "human-expert", "--config", `model_provider="human_e2e"`,
		"--config", provider, "--config", `web_search="cached"`,
		"--output-last-message", lastMessage,
		"Run the available command tool once, then report the Human model's final response.",
	)
	command.Dir = workspace
	command.Env = publicCodexEnvironment(codexHome, instance.CallerToken())
	output, runErr := command.CombinedOutput()
	if runErr != nil || ctx.Err() != nil {
		cancel()
		select {
		case operatorErr := <-operatorDone:
			if operatorErr != nil && !errors.Is(operatorErr, context.Canceled) {
				t.Logf("web operator: %v", operatorErr)
			}
		default:
		}
		t.Fatalf("real Codex public-stack run failed: %v / %v\n%s", runErr, ctx.Err(), output)
	}
	select {
	case operatorErr := <-operatorDone:
		if operatorErr != nil {
			t.Fatal(operatorErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Codex exited before the web operator observed the tool continuation")
	}
	final, err := os.ReadFile(lastMessage)
	if err != nil || strings.TrimSpace(string(final)) != "CODEX-PUBLIC-HUMAN-FINAL" {
		t.Fatalf("Codex final = %q, read error %v\n%s", final, err, output)
	}
	if !strings.Contains(string(output), "CODEX-PUBLIC-HUMAN-FINAL") {
		t.Fatalf("Codex JSON output omitted the Human final:\n%s", output)
	}
	if _, err := os.Stat(filepath.Join(codexHome, "config.toml")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Codex wrote isolated user configuration: %v", err)
	}
}

func runCodexWebOperator(
	ctx context.Context,
	api localWebAPI,
	workspace string,
	done chan<- error,
) {
	const callID = "call_codex_public_exec"
	var caller, task string
	toolSent := false
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case <-ticker.C:
		}
		state, status, err := api.call(ctx, http.MethodGet, "/api/state", nil)
		if err != nil {
			continue
		}
		if status != http.StatusOK {
			done <- fmt.Errorf("web state status = %d", status)
			return
		}
		if caller == "" {
			inbox, _ := state["inbox"].([]any)
			if len(inbox) == 0 {
				continue
			}
			item, _ := inbox[0].(map[string]any)
			accepted, acceptedStatus, err := api.call(ctx, http.MethodPost, "/api/accept",
				map[string]any{"delivery": item["delivery"]})
			if err != nil || acceptedStatus != http.StatusOK {
				done <- fmt.Errorf("accept Codex assignment: status=%d err=%v", acceptedStatus, err)
				return
			}
			key, _ := accepted["key"].(map[string]any)
			caller, task = fmt.Sprint(key["caller"]), fmt.Sprint(key["task_id"])
			if caller == "" || task == "" {
				done <- fmt.Errorf("accept returned invalid conversation key: %v", accepted)
				return
			}
		}
		if !toolSent {
			_, toolStatus, err := api.call(ctx, http.MethodPost, "/api/tool-calls", map[string]any{
				"caller": caller, "task_id": task,
				"calls": []map[string]any{{
					"id": callID, "name": "exec_command",
					"input": map[string]any{
						"cmd": "printf CODEX_PUBLIC_TOOL_OK", "workdir": workspace,
						"yield_time_ms": 1000, "max_output_tokens": 1000,
					},
				}},
			})
			if err != nil || toolStatus != http.StatusOK {
				done <- fmt.Errorf("submit Codex tool call: status=%d err=%v", toolStatus, err)
				return
			}
			toolSent = true
			continue
		}
		if !webStateHasToolResult(state, caller, task, callID, "CODEX_PUBLIC_TOOL_OK") {
			continue
		}
		_, finalStatus, err := api.call(ctx, http.MethodPost, "/api/final", map[string]any{
			"caller": caller, "task_id": task, "text": "CODEX-PUBLIC-HUMAN-FINAL",
		})
		if err != nil || finalStatus != http.StatusOK {
			done <- fmt.Errorf("send Codex final: status=%d err=%v", finalStatus, err)
			return
		}
		done <- nil
		return
	}
}

func webStateHasToolResult(state map[string]any, caller, task, callID, marker string) bool {
	conversations, _ := state["conversations"].([]any)
	for _, raw := range conversations {
		conversation, _ := raw.(map[string]any)
		key, _ := conversation["key"].(map[string]any)
		if fmt.Sprint(key["caller"]) != caller || fmt.Sprint(key["task_id"]) != task {
			continue
		}
		transcript, _ := conversation["transcript"].([]any)
		for _, rawEntry := range transcript {
			entry, _ := rawEntry.(map[string]any)
			if entry["kind"] == "tool_result" && entry["tool_call_id"] == callID &&
				strings.Contains(fmt.Sprint(entry["text"]), marker) {
				return true
			}
		}
	}
	return false
}

func publicCodexEnvironment(codexHome, token string) []string {
	environment := make([]string, 0, len(os.Environ())+4)
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		switch name {
		case "CODEX_HOME", "HUMAN_CODEX_API_KEY", "OPENAI_API_KEY", "CODEX_API_KEY":
			continue
		default:
			environment = append(environment, value)
		}
	}
	return append(environment,
		"CODEX_HOME="+codexHome, "HUMAN_CODEX_API_KEY="+token,
		"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost",
	)
}
