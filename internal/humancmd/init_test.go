package humancmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/userdata"
)

func TestInitOpenCodePrintsCompleteSecretFreeProvider(t *testing.T) {
	payload, err := generateOpenCodeConfig(".", defaultOpenCodeBaseURL)
	if err != nil {
		t.Fatal(err)
	}
	var config openCodeConfig
	if err := json.Unmarshal(payload, &config); err != nil {
		t.Fatalf("generated output is not JSON/JSONC: %v\n%s", err, payload)
	}
	provider, ok := config.Provider["human"]
	if !ok || len(config.Provider) != 1 {
		t.Fatalf("providers = %+v", config.Provider)
	}
	if config.Schema != "https://opencode.ai/config.json" || provider.NPM != "@ai-sdk/openai-compatible" {
		t.Fatalf("provider envelope = %+v", config)
	}
	if provider.Options.BaseURL != defaultOpenCodeBaseURL || provider.Options.APIKey != "{env:HUMAN_CALLER_TOKEN}" || provider.Options.Timeout {
		t.Fatalf("provider options = %+v", provider.Options)
	}
	workspace, err := userdata.ResolveGitWorkspace(".")
	if err != nil {
		t.Fatal(err)
	}
	workspaceKey, err := userdata.WorkspaceKey(workspace)
	if err != nil {
		t.Fatal(err)
	}
	wantHeaders := map[string]string{
		"X-Human-Capability-Tier": "workspace",
		"X-Human-Workspace-Key":   workspaceKey,
		"X-Human-Harness-Id":      adapter.OpenCodeID,
		"X-Human-Harness-Version": adapter.OpenCodeVersion,
		"X-Human-Workspace-Root":  workspace,
		"X-Human-Allow-Exec":      "true",
	}
	if len(provider.Options.Headers) != len(wantHeaders) {
		t.Fatalf("headers = %+v", provider.Options.Headers)
	}
	for name, want := range wantHeaders {
		if got := provider.Options.Headers[name]; got != want {
			t.Fatalf("header %s = %q, want %q", name, got, want)
		}
	}
	if strings.Contains(workspaceKey, filepath.Base(workspace)) {
		t.Fatalf("workspace key reveals workspace name: %q", workspaceKey)
	}
	if !bytes.Equal(payload, mustGenerateOpenCodeConfig(t, ".", defaultOpenCodeBaseURL)) {
		t.Fatal("generated provider JSON is not deterministic")
	}
}

func TestInitOpenCodeWritesAtomicallyAndRequiresForce(t *testing.T) {
	dataRoot := t.TempDir()
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	outputPath := filepath.Join(dataRoot, "opencode.jsonc")

	run := func(args ...string) (string, error) {
		command := newInitOpenCodeCommand()
		var output bytes.Buffer
		command.SetOut(&output)
		command.SetErr(&output)
		command.SetArgs(args)
		err := command.ExecuteContext(context.Background())
		return output.String(), err
	}
	output, err := run("--workspace", ".", "--output", outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, outputPath) {
		t.Fatalf("write confirmation = %q", output)
	}
	first, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, mustGenerateOpenCodeConfig(t, ".", defaultOpenCodeBaseURL)) {
		t.Fatalf("written config differs from stdout config:\n%s", first)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(outputPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("output mode = %o, want 600", info.Mode().Perm())
		}
	}
	if _, err := run("--workspace", ".", "--output", outputPath); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("no-clobber error = %v", err)
	}
	const replacementURL = "http://127.0.0.1:29080/v1"
	if _, err := run("--workspace", ".", "--base-url", replacementURL, "--output", outputPath, "--force"); err != nil {
		t.Fatal(err)
	}
	replacement, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(replacement, []byte(replacementURL)) || bytes.Equal(first, replacement) {
		t.Fatalf("forced replacement was not published: %s", replacement)
	}
	entries, err := os.ReadDir(dataRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".human-opencode-") {
			t.Fatalf("temporary output was not cleaned: %s", entry.Name())
		}
	}
}

func TestInitOpenCodeDefaultsToStdout(t *testing.T) {
	command := newInitOpenCodeCommand()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"--workspace", "."})
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), mustGenerateOpenCodeConfig(t, ".", defaultOpenCodeBaseURL)) {
		t.Fatalf("stdout provider config = %s", output.Bytes())
	}
}

func TestInitOpenCodeRequiresTLSForRemoteBearerEndpoint(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"http://human.example/v1",
		"http://192.0.2.10:8080/v1",
	} {
		if _, err := generateOpenCodeConfig(".", value); err == nil || !strings.Contains(err.Error(), "must use https") {
			t.Fatalf("remote plaintext base URL %q error = %v", value, err)
		}
	}
	for _, value := range []string{
		"http://localhost:19080/v1",
		"http://127.0.0.1:19080/v1",
		"http://[::1]:19080/v1",
		"https://human.example/v1",
	} {
		if _, err := generateOpenCodeConfig(".", value); err != nil {
			t.Fatalf("safe base URL %q: %v", value, err)
		}
	}
}

func mustGenerateOpenCodeConfig(t *testing.T, workspace, baseURL string) []byte {
	t.Helper()
	payload, err := generateOpenCodeConfig(workspace, baseURL)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
