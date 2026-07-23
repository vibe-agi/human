package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

const realCodexVersion = "codex-cli 0.145.0"

// TestRealCodexResponsesToolLoop is an opt-in release gate for the exact
// installed Codex CLI. It uses only command-line overrides and an empty
// CODEX_HOME: no user configuration or credentials are copied into the run.
//
//	HUMAN_REAL_CODEX_E2E=1 go test ./internal/completion/gateway \
//	  -run TestRealCodexResponsesToolLoop -count=1 -v
func TestRealCodexResponsesToolLoop(t *testing.T) {
	if os.Getenv("HUMAN_REAL_CODEX_E2E") != "1" {
		t.Skip("set HUMAN_REAL_CODEX_E2E=1 to run the installed Codex CLI")
	}
	binary, err := exec.LookPath("codex")
	if err != nil {
		t.Skip("codex is not installed")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != realCodexVersion {
		t.Skipf("requires %s; got %q (%v)", realCodexVersion, strings.TrimSpace(string(version)), err)
	}

	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval: 100 * time.Millisecond,
		MaxPending:        30 * time.Second,
	})
	workspace := t.TempDir()
	codexHome := t.TempDir()
	lastMessage := filepath.Join(t.TempDir(), "last-message.txt")

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	type observation struct {
		firstKey          string
		secondKey         string
		namespaceFunction string
		toolCallID        string
	}
	type workerResult struct {
		observation observation
		err         error
	}
	workerDone := make(chan workerResult, 1)
	go func() {
		observed := observation{}
		fail := func(err error) {
			workerDone <- workerResult{observation: observed, err: err}
			cancel()
		}

		first, err := waitForCodexAssignment(ctx, fixture)
		if err != nil {
			fail(err)
			return
		}
		observed.firstKey = first.IdempotencyKey
		if first.Request.ToolCallPolicy != canonical.ToolCallsSerial {
			fail(fmt.Errorf("Codex tool-call policy = %q, want serial", first.Request.ToolCallPolicy))
			return
		}
		var execTool canonical.Tool
		for _, tool := range first.Request.Tools {
			switch {
			case tool.Namespace == "" && tool.Name == "exec_command":
				execTool = tool
			case tool.Namespace != "" && observed.namespaceFunction == "":
				observed.namespaceFunction = tool.QualifiedName()
			}
			if tool.Namespace == "" && tool.Name == "web_search" {
				fail(errors.New("Codex web_search was exposed as a caller-executed function"))
				return
			}
		}
		if execTool.Name == "" {
			fail(errors.New("Codex request declared no ordinary exec_command function"))
			return
		}
		if observed.namespaceFunction == "" {
			fail(errors.New("Codex request declared no namespaced function"))
			return
		}
		hasHostedSearch := false
		for _, capability := range first.Request.HostedCapabilities {
			if capability.Type == "web_search" {
				hasHostedSearch = true
				break
			}
		}
		if !hasHostedSearch {
			fail(errors.New("Codex request did not preserve web_search as a hosted capability"))
			return
		}

		observed.toolCallID = "call_codex_native_exec"
		if err := fixture.hub.Publish(ctx, first.CallerID, first.IdempotencyKey, completion.Event{
			ID: "codex_real_accepted_1", Type: completion.EventAccepted, WorkerID: fixture.worker.ID,
		}); err != nil {
			fail(fmt.Errorf("accept first Codex assignment: %w", err))
			return
		}
		if err := fixture.hub.Publish(ctx, first.CallerID, first.IdempotencyKey, completion.Event{
			ID: "codex_real_tool_1", Type: completion.EventToolCalls,
			ToolCalls: []completion.ToolCall{{
				ID: observed.toolCallID, Name: execTool.Name,
				Input: map[string]any{
					"cmd":               "printf CODEX_NATIVE_TOOL_OK",
					"workdir":           workspace,
					"yield_time_ms":     1000,
					"max_output_tokens": 1000,
				},
			}},
		}); err != nil {
			fail(fmt.Errorf("send Codex tool call: %w", err))
			return
		}

		second, err := waitForCodexAssignment(ctx, fixture)
		if err != nil {
			fail(err)
			return
		}
		observed.secondKey = second.IdempotencyKey
		if observed.secondKey == observed.firstKey {
			fail(errors.New("Codex tool result continuation reused the first request key"))
			return
		}
		result, ok := canonicalToolResult(second.Request, observed.toolCallID)
		if !ok {
			fail(fmt.Errorf("Codex continuation omitted tool result %q", observed.toolCallID))
			return
		}
		resultJSON, err := json.Marshal(result.Output)
		if err != nil || !strings.Contains(string(resultJSON), "CODEX_NATIVE_TOOL_OK") {
			fail(fmt.Errorf("Codex continuation tool result omitted marker (type %T, marshal error %v)", result.Output, err))
			return
		}
		if err := publishAcceptedAndFinal(fixture, second, "CODEX_HUMAN_FINAL"); err != nil {
			fail(fmt.Errorf("finish Codex continuation: %w", err))
			return
		}
		workerDone <- workerResult{observation: observed}
	}()

	provider := fmt.Sprintf(
		`model_providers.human_e2e={ name = "Human E2E", base_url = %q, env_key = "HUMAN_CODEX_API_KEY", wire_api = "responses", request_max_retries = 0, stream_max_retries = 0, stream_idle_timeout_ms = 10000 }`,
		fixture.server.URL+"/v1",
	)
	command := exec.CommandContext(ctx, binary,
		"--ask-for-approval", "never",
		"exec",
		"--ignore-user-config",
		"--ephemeral",
		"--skip-git-repo-check",
		"--json",
		"--color", "never",
		"--sandbox", "read-only",
		"--cd", workspace,
		"--model", "human-expert",
		"--config", `model_provider="human_e2e"`,
		"--config", provider,
		"--config", `web_search="cached"`,
		"--output-last-message", lastMessage,
		"Run the available command tool once, then report the Human model's final response.",
	)
	command.Dir = workspace
	command.Env = codexGateEnvironment(codexHome)
	output, runErr := command.CombinedOutput()
	if runErr != nil {
		select {
		case result := <-workerDone:
			if result.err != nil {
				t.Fatalf("real Codex worker assertion failed: %v", result.err)
			}
		default:
		}
		t.Fatalf("real Codex failed: %v (output bytes=%d, event types=%v)\n%s",
			runErr, len(output), codexEventTypes(output), output)
	}

	var result workerResult
	select {
	case result = <-workerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Codex exited before the worker observed its tool-result continuation")
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.observation.firstKey == "" || result.observation.secondKey == "" ||
		result.observation.namespaceFunction == "" || result.observation.toolCallID == "" {
		t.Fatalf("incomplete Codex observation: %+v", result.observation)
	}
	final, err := os.ReadFile(lastMessage)
	if err != nil || strings.TrimSpace(string(final)) != "CODEX_HUMAN_FINAL" {
		t.Fatalf("Codex last message did not contain the Human final (read error %v, bytes=%d)", err, len(final))
	}
	if !strings.Contains(string(output), "CODEX_HUMAN_FINAL") {
		t.Fatalf("Codex JSON output omitted the Human final (bytes=%d, event types=%v)", len(output), codexEventTypes(output))
	}
	if _, err := os.Stat(filepath.Join(codexHome, "config.toml")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Codex gate wrote a config file into its isolated CODEX_HOME: %v", err)
	}
	t.Logf(
		"real Codex completed 2 Responses assignments: ordinary=exec_command namespace=%s hosted=web_search final=CODEX_HUMAN_FINAL",
		result.observation.namespaceFunction,
	)
}

func waitForCodexAssignment(ctx context.Context, fixture *gatewayFixture) (completion.Assignment, error) {
	select {
	case assignment, open := <-fixture.worker.Assignments:
		if !open {
			return completion.Assignment{}, errors.New("worker assignment stream closed")
		}
		return assignment, nil
	case <-ctx.Done():
		return completion.Assignment{}, fmt.Errorf("wait for Codex assignment: %w", ctx.Err())
	}
}

func codexGateEnvironment(codexHome string) []string {
	environment := make([]string, 0, len(os.Environ())+2)
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		switch name {
		case "CODEX_HOME", "HUMAN_CODEX_API_KEY", "OPENAI_API_KEY", "CODEX_API_KEY":
			continue
		default:
			environment = append(environment, value)
		}
	}
	return append(environment, "CODEX_HOME="+codexHome, "HUMAN_CODEX_API_KEY=hae_test")
}

func codexEventTypes(output []byte) []string {
	types := make([]string, 0, 8)
	seen := make(map[string]struct{})
	for _, line := range strings.Split(string(output), "\n") {
		var event struct {
			Type string `json:"type"`
		}
		if json.Unmarshal([]byte(line), &event) != nil || event.Type == "" {
			continue
		}
		if _, duplicate := seen[event.Type]; duplicate {
			continue
		}
		seen[event.Type] = struct{}{}
		types = append(types, event.Type)
	}
	return types
}
