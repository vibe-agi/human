package local_test

// End-to-end door: REAL OpenCode 1.17.18 (Workspace profile) as the caller,
// the REAL local human product, and a REAL browser (Playwright) driving the web
// UI as a scripted human. Unlike the API-only door, the human side is operated
// through the actual page, so this observes the full loop a real operator sees.
//
// Scenario A (this file): accept -> progress -> final, asserting the caller
// receives the human's final. Workspace tier is what keeps the tool loop
// continuous; scenarios B (caller disconnect) and C (session resume) are added
// next as red gates that drive the availability fixes.
//
//	HUMAN_REAL_OPENCODE_BROWSER_E2E=1 go test ./local \
//	  -run TestRealOpenCodeWorkspaceBrowserFinal -count=1 -v

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/local"
)

func TestRealOpenCodeWorkspaceBrowserFinal(t *testing.T) {
	if os.Getenv("HUMAN_REAL_OPENCODE_BROWSER_E2E") != "1" {
		t.Skip("set HUMAN_REAL_OPENCODE_BROWSER_E2E=1 to run the browser end-to-end door")
	}
	opencodeBinary, err := exec.LookPath("opencode")
	if err != nil {
		t.Skip("opencode is not installed")
	}
	version, err := exec.Command(opencodeBinary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != "1.17.18" {
		t.Skipf("requires opencode 1.17.18; got %q (%v)", strings.TrimSpace(string(version)), err)
	}
	nodeBinary, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}

	root := t.TempDir()
	privateRoot := filepath.Join(root, "private")
	if err := os.Mkdir(privateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	instance, err := local.Open(context.Background(), local.Config{
		Public:             local.PublicStackConfig{DatabasePath: filepath.Join(root, "store.db")},
		HumanWorkspaceRoot: filepath.Join(root, "mirror"),
		ListenAddress:      "127.0.0.1:0",
		WebListenAddress:   "127.0.0.1:0",
		WebStatePath:       filepath.Join(privateRoot, "workerkit-state.db"),
		CallerSubject:      "browser-caller",
		WorkerSubject:      "browser-worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	webURL := instance.WebURL()
	base := webURL[:strings.Index(webURL, "/?token=")]
	token := webURL[strings.Index(webURL, "?token=")+len("?token="):]

	// Workspace-tier OpenCode config: the X-Human-Capability-Tier=workspace
	// header is what routes the caller through resumeOpenCodeTask, so a tool
	// loop continues on the same task instead of forking a fresh one.
	workspace := t.TempDir()
	config := map[string]any{
		"$schema": "https://opencode.ai/config.json",
		"provider": map[string]any{"human": map[string]any{
			"npm": "@ai-sdk/openai-compatible", "name": "Human Agent (local)",
			"options": map[string]any{
				"baseURL": instance.BaseURL() + "/v1", "apiKey": instance.CallerToken(),
				"headers": map[string]string{
					"X-Human-Capability-Tier": "workspace",
					"X-Human-Workspace-Key":   "workspace-browser-e2e",
					"X-Human-Harness-Id":      "opencode",
					"X-Human-Harness-Version": "1.17.18",
					"X-Human-Allow-Exec":      "true",
				},
			},
			"models": map[string]any{"human-expert": map[string]any{
				"name": "Human Expert", "limit": map[string]int{"context": 100000, "output": 4096},
			}},
		}},
	}
	payload, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "opencode.json"), append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const finalToken = "WORKSPACE-BROWSER-DOOR"

	// OpenCode caller in the background: it sends a completion and blocks for the
	// human's answer.
	type openCodeResult struct {
		output []byte
		err    error
	}
	// Isolate OpenCode's config/data so it loads THIS test's workspace provider,
	// not the user's global ~/.config/opencode (which points at a fixed port).
	configHome := filepath.Join(t.TempDir(), "config")
	dataHome := filepath.Join(t.TempDir(), "data")
	for _, dir := range []string{configHome, dataHome} {
		if mkErr := os.MkdirAll(dir, 0o700); mkErr != nil {
			t.Fatal(mkErr)
		}
	}
	// Loopback must bypass the environment's HTTP_PROXY (e.g. a local Clash on
	// 127.0.0.1:7890) — OpenCode's bun runtime honors proxy env and would route
	// the loopback completion through it, getting a 502 Bad Gateway. node/go
	// ignore proxy env, so only the real caller needs this.
	callerEnv := append(os.Environ(),
		"XDG_CONFIG_HOME="+configHome, "XDG_DATA_HOME="+dataHome,
		"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost")
	callerDone := make(chan openCodeResult, 1)
	go func() {
		command := exec.CommandContext(ctx, opencodeBinary, "run", "--pure", "--auto",
			"--print-logs", "--log-level", "DEBUG", "--format", "json",
			"--model", "human/human-expert", "--dir", workspace,
			"How do I safely roll back a bad production deploy? Answer through the human console.")
		command.Dir = workspace
		command.Env = callerEnv
		output, err := command.CombinedOutput()
		callerDone <- openCodeResult{output, err}
	}()

	// Playwright human operating the real web UI.
	e2eDir, err := filepath.Abs(filepath.Join("..", "web", "e2e"))
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(filepath.Join(e2eDir, "node_modules", "playwright")); statErr != nil {
		install := exec.Command("npm", "install", "--no-audit", "--no-fund")
		install.Dir = e2eDir
		if out, installErr := install.CombinedOutput(); installErr != nil {
			t.Fatalf("npm install playwright: %v\n%s", installErr, out)
		}
	}
	driver := exec.CommandContext(ctx, nodeBinary, "human-e2e.mjs")
	driver.Dir = e2eDir
	driver.Env = append(os.Environ(),
		"WEB_URL="+base, "WEB_TOKEN="+token, "FINAL_TOKEN="+finalToken)
	driverOutput, driverErr := driver.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("playwright human timed out: %v\n%s", ctx.Err(), driverOutput)
	}
	if driverErr != nil || !strings.Contains(string(driverOutput), "human-e2e-ok") {
		cancel()
		caller := <-callerDone
		t.Fatalf("playwright human failed: %v\n%s\n--- opencode output ---\n%s\n--- opencode err: %v",
			driverErr, driverOutput, caller.output, caller.err)
	}

	caller := <-callerDone
	if caller.err != nil {
		t.Fatalf("real OpenCode failed: %v\n%s", caller.err, caller.output)
	}
	if !strings.Contains(string(caller.output), finalToken) {
		t.Fatalf("OpenCode caller never received the human's final:\n%s", caller.output)
	}
}
