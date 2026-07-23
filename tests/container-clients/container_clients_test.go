package containerclients

// Testcontainers product door for the three supported coding clients. The
// clients run in a pinned Linux image and call the host's public Local stack;
// Playwright and Chromium run headlessly in a second pinned Linux image and
// operate the real Web DOM. The browser-side human asks an OpenAI-compatible
// host LLM what to type through a short-lived authenticated relay.
//
// The external LLM credential is intentionally never passed to Docker. Set:
//
//   HUMAN_TESTCONTAINERS_E2E=1
//   HUMAN_TEST_LLM_API_KEY=...
//
// Optional settings are HUMAN_TEST_LLM_BASE_URL (default
// http://127.0.0.1:23333), HUMAN_TEST_LLM_MODEL (default dashscope:glm-5), and
// HUMAN_TEST_CLIENT_IMAGE and HUMAN_TEST_BROWSER_IMAGE (use already-built
// replacement images).

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	testcontainers "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	humanlocal "github.com/vibe-agi/human/local"
)

const (
	containerE2EGate         = "HUMAN_TESTCONTAINERS_E2E"
	containerLLMKeyEnv       = "HUMAN_TEST_LLM_API_KEY"
	containerLLMBaseEnv      = "HUMAN_TEST_LLM_BASE_URL"
	containerLLMModelEnv     = "HUMAN_TEST_LLM_MODEL"
	containerImageEnv        = "HUMAN_TEST_CLIENT_IMAGE"
	containerBrowserImageEnv = "HUMAN_TEST_BROWSER_IMAGE"
	containerFakeLLMEnv      = "HUMAN_TEST_LLM_FAKE"
	defaultContainerLLMURL   = "http://127.0.0.1:23333"
	defaultContainerModel    = "dashscope:glm-5"

	containerClaudeMarker    = "HUMAN-CONTAINER-CLAUDE-OK"
	containerOpenCodeMarker  = "HUMAN-CONTAINER-OPENCODE-OK"
	containerCodexMarker     = "HUMAN-CONTAINER-CODEX-OK"
	containerCodexTool       = "HUMAN-CONTAINER-CODEX-TOOL-OK"
	containerPreflight       = "HUMAN-CONTAINER-PREFLIGHT-OK"
	containerClaudeResume    = "HUMAN-CONTAINER-CLAUDE-RESUME-OK"
	containerOpenCodeResume  = "HUMAN-CONTAINER-OPENCODE-RESUME-OK"
	containerCodexResume     = "HUMAN-CONTAINER-CODEX-RESUME-OK"
	containerClaudeTool      = "HUMAN-CONTAINER-CLAUDE-TOOL-OK"
	containerClaudeToolFail  = "HUMAN-CONTAINER-CLAUDE-TOOL-FAILURE-OK"
	containerOpenCodeTool    = "HUMAN-CONTAINER-OPENCODE-TOOL-OK"
	containerOpenCodeFail    = "HUMAN-CONTAINER-OPENCODE-TOOL-FAILURE-OK"
	containerWorkspace       = "HUMAN-CONTAINER-WORKSPACE-OK"
	containerToolFailure     = "HUMAN-CONTAINER-TOOL-FAILURE-OK"
	containerToolError       = "HUMAN-CONTAINER-EXPECTED-TOOL-ERROR"
	containerClaudeReject    = "HUMAN-CONTAINER-CLAUDE-REJECT-OK"
	containerOpenCodeReject  = "HUMAN-CONTAINER-OPENCODE-REJECT-OK"
	containerCodexReject     = "HUMAN-CONTAINER-CODEX-REJECT-OK"
	containerClaudeTask      = "HUMAN-CONTAINER-CLAUDE-TASK-OK"
	containerOpenCodeTask    = "HUMAN-CONTAINER-OPENCODE-TASK-OK"
	containerCodexTask       = "HUMAN-CONTAINER-CODEX-TASK-OK"
	containerClaudeWorkspace = "HUMAN-CONTAINER-CLAUDE-WORKSPACE-OK"
	containerCodexWorkspace  = "HUMAN-CONTAINER-CODEX-WORKSPACE-OK"
	containerClaudePartial   = "HUMAN-CONTAINER-CLAUDE-PARTIAL-SSE-OK"
	containerCodexPartial    = "HUMAN-CONTAINER-CODEX-PARTIAL-SSE-OK"
	containerCodexModel      = "human-expert"
)

var containerMarkerPattern = regexp.MustCompile(`HUMAN-CONTAINER-[A-Z0-9-]+-OK`)

func TestContainerAgentCLIsViaLLMWebHuman(t *testing.T) {
	if os.Getenv(containerE2EGate) != "1" {
		t.Skip("set HUMAN_TESTCONTAINERS_E2E=1 to run the pinned client containers")
	}

	humanLLM := newContainerHumanLLM(t)
	preflightContainerHumanLLM(t, humanLLM, containerPreflight)

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize container test root: %v", err)
	}
	workspace := filepath.Join(root, "workspace")
	humanWorkspaceRoot := filepath.Join(root, "human-workspaces")
	privateRoot := filepath.Join(root, "private")
	claudeWorkspace := filepath.Join(workspace, "claude")
	openCodeWorkspace := filepath.Join(workspace, "opencode")
	codexWorkspace := filepath.Join(workspace, "codex")
	containerHome := filepath.Join(workspace, ".container-home")
	for _, directory := range []string{
		workspace, humanWorkspaceRoot, privateRoot, claudeWorkspace, openCodeWorkspace, codexWorkspace,
		containerHome, filepath.Join(containerHome, "claude"),
		filepath.Join(containerHome, "xdg-config"), filepath.Join(containerHome, "xdg-data"),
		filepath.Join(containerHome, "codex"),
	} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	instance, err := humanlocal.Open(t.Context(), humanlocal.Config{
		HumanWorkspaceRoot: humanWorkspaceRoot,
		Public: humanlocal.PublicStackConfig{
			DatabasePath:       filepath.Join(privateRoot, "store.db"),
			CallerWriteTimeout: 30 * time.Second,
			CallerHeartbeat:    2 * time.Second,
			MaxPending:         5 * time.Minute,
		},
		ListenAddress:    "127.0.0.1:0",
		WebListenAddress: "127.0.0.1:0",
		WebStatePath:     filepath.Join(privateRoot, "workerkit-state.db"),
		ShutdownTimeout:  10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := instance.Close(); err != nil {
			t.Errorf("close local container test instance: %v", err)
		}
	})

	modelPort, err := loopbackPort(instance.BaseURL())
	if err != nil {
		t.Fatal(err)
	}
	containerModelBase := fmt.Sprintf("http://host.testcontainers.internal:%d", modelPort)
	if err := writeContainerOpenCodeConfig(openCodeWorkspace, containerModelBase); err != nil {
		t.Fatal(err)
	}
	codexModelCatalog, err := writeContainerCodexModelCatalog(workspace)
	if err != nil {
		t.Fatal(err)
	}
	codexCatalogConfig := fmt.Sprintf("model_catalog_json=%q", codexModelCatalog)
	webAPI := webAPIForLocal(t, instance)
	webPort, err := loopbackPort(webAPI.base)
	if err != nil {
		t.Fatal(err)
	}
	replyRelay := newContainerReplyRelay(t, humanLLM)
	relayPort, err := loopbackPort(replyRelay.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	claudeFault := newContainerPartialSSEProxy(t, instance.BaseURL(), "/v1/messages",
		"Reviewing the request for "+containerClaudePartial+".")
	defer claudeFault.Close()
	codexFault := newContainerPartialSSEProxy(t, instance.BaseURL(), "/v1/responses",
		"Reviewing the request for "+containerCodexPartial+".")
	defer codexFault.Close()
	claudeFaultPort, err := loopbackPort(claudeFault.URL)
	if err != nil {
		t.Fatal(err)
	}
	codexFaultPort, err := loopbackPort(codexFault.URL)
	if err != nil {
		t.Fatal(err)
	}

	container, err := startRealClientsContainer(t.Context(), realClientsContainerConfig{
		modelPort: modelPort, modelBaseURL: containerModelBase,
		callerToken: instance.CallerToken(), workspace: workspace, home: containerHome,
		extraHostPorts: []int{claudeFaultPort, codexFaultPort},
	})
	if err != nil {
		t.Fatalf("start real-client container: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := container.Terminate(ctx); err != nil {
			t.Errorf("terminate real-client container: %v", err)
		}
	})

	assertContainerClientVersions(t, container)

	browserHumanWorkspaceRoot := humanWorkspaceRoot
	browserContainer, err := startBrowserHumanContainer(t.Context(), webPort, relayPort, browserHumanWorkspaceRoot,
		claudeFaultPort, codexFaultPort)
	if err != nil {
		t.Fatalf("start browser-human container: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := browserContainer.Terminate(ctx); err != nil {
			t.Errorf("terminate browser-human container: %v", err)
		}
	})
	assertBrowserVersion(t, browserContainer)
	assertContainerFilesystemIsolation(t, container, browserContainer, workspace, browserHumanWorkspaceRoot)

	browserConfig := containerBrowserConfig{
		webURL:     fmt.Sprintf("http://host.testcontainers.internal:%d", webPort),
		webToken:   webAPI.token,
		replyURL:   fmt.Sprintf("http://host.testcontainers.internal:%d/reply", relayPort),
		replyToken: replyRelay.token,
	}
	codexProvider := fmt.Sprintf(
		`model_providers.human_e2e={ name = "Human E2E", base_url = %q, env_key = "HUMAN_CODEX_API_KEY", wire_api = "responses", request_max_retries = 0, stream_max_retries = 0, stream_idle_timeout_ms = 60000 }`,
		containerModelBase+"/v1",
	)
	const claudeSession = "123e4567-e89b-42d3-a456-426614174217"

	if !t.Run("claude-anthropic-messages", func(t *testing.T) {
		prompt := "Compatibility probe: reply with exactly " + containerClaudeMarker + " and nothing else."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			claudeWorkspace, []string{
				"claude", "-p", prompt, "--model", "claude-sonnet-4-5",
				"--session-id", claudeSession, "--output-format", "json",
			}, containerDialogueScenario{marker: containerClaudeMarker}, instance.CallerToken())
		if !strings.Contains(output, containerClaudeMarker) {
			t.Fatalf("Claude output omitted the LLM-human final:\n%s", safeSnippet(output, 16<<10))
		}
	}) {
		return
	}
	if !t.Run("claude-close-reopen-resume", func(t *testing.T) {
		prompt := "After reopening this saved Claude session, reply with exactly " + containerClaudeResume + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			claudeWorkspace, []string{
				"claude", "-p", "--resume", claudeSession, prompt,
				"--model", "claude-sonnet-4-5", "--output-format", "json",
			}, containerDialogueScenario{marker: containerClaudeResume}, instance.CallerToken())
		if !strings.Contains(output, containerClaudeResume) {
			t.Fatalf("reopened Claude session omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
	}) {
		return
	}
	if !t.Run("claude-tool-success", func(t *testing.T) {
		prompt := "Run the Human's Bash tool request, then relay a final containing " + containerClaudeTool + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			claudeWorkspace, []string{
				"claude", "-p", prompt, "--model", "claude-sonnet-4-5",
				"--dangerously-skip-permissions", "--output-format", "json",
			}, containerDialogueScenario{
				marker: containerClaudeTool, toolMarker: containerClaudeTool,
				toolProfile: "claude",
			}, instance.CallerToken())
		assertToolSuccess(t, "Claude", output, containerClaudeTool, claudeWorkspace)
	}) {
		return
	}
	if !t.Run("claude-tool-failure-recovers-to-final", func(t *testing.T) {
		prompt := "Run the Human's Bash tool request even though it will fail, then relay a final containing " + containerClaudeToolFail + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			claudeWorkspace, []string{
				"claude", "-p", prompt, "--model", "claude-sonnet-4-5",
				"--dangerously-skip-permissions", "--output-format", "json",
			}, containerDialogueScenario{
				marker: containerClaudeToolFail, toolMarker: containerToolError,
				toolProfile: "claude", toolCommand: failingToolCommand(),
			}, instance.CallerToken())
		if !strings.Contains(output, containerClaudeToolFail) {
			t.Fatalf("Claude did not recover from the expected failed tool result:\n%s", safeSnippet(output, 16<<10))
		}
	}) {
		return
	}
	if !t.Run("claude-native-task-lifecycle", func(t *testing.T) {
		prompt := "Execute the Human's TaskCreate, TaskUpdate, and TaskList requests in order, then relay a final containing " + containerClaudeTask + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			claudeWorkspace, []string{
				"claude", "-p", prompt, "--model", "claude-sonnet-4-5",
				"--dangerously-skip-permissions", "--output-format", "stream-json", "--verbose",
			}, containerDialogueScenario{
				marker: containerClaudeTask, action: "tasks", taskMarker: containerClaudeTask,
				toolProfile: "claude",
			}, instance.CallerToken())
		if !strings.Contains(output, containerClaudeTask) {
			t.Fatalf("Claude task turn omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
		for _, tool := range []string{"TaskCreate", "TaskUpdate", "TaskList"} {
			if !strings.Contains(output, `"name":"`+tool+`"`) &&
				!strings.Contains(output, `"name": "`+tool+`"`) {
				t.Fatalf("Claude task lifecycle output shows no native %s call:\n%s",
					tool, safeSnippet(output, 16<<10))
			}
		}
	}) {
		return
	}
	if !t.Run("claude-workspace-create-then-native-edit", func(t *testing.T) {
		prompt := "Apply every workspace change the Human delivers through native tools, then relay exactly " +
			containerClaudeWorkspace + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			claudeWorkspace, []string{
				"claude", "-p", prompt, "--model", "claude-sonnet-4-5",
				"--dangerously-skip-permissions", "--output-format", "stream-json", "--verbose",
			}, containerDialogueScenario{
				marker: containerClaudeWorkspace, action: "workspace",
				workspacePath: "claude-workspace.txt", workspaceFirst: "claude-workspace-v1\n",
				workspaceSecond: "claude-workspace-v2\n",
			}, instance.CallerToken())
		if !strings.Contains(output, containerClaudeWorkspace) {
			t.Fatalf("Claude workspace turn omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
		for _, tool := range []string{"Write", "Edit"} {
			if !strings.Contains(output, `"name":"`+tool+`"`) &&
				!strings.Contains(output, `"name": "`+tool+`"`) {
				t.Fatalf("Claude workspace output shows no native %s call:\n%s", tool, safeSnippet(output, 16<<10))
			}
		}
		content, err := os.ReadFile(filepath.Join(claudeWorkspace, "claude-workspace.txt"))
		if err != nil || string(content) != "claude-workspace-v2\n" {
			t.Fatalf("Claude caller workspace file = %q, read error %v\nClaude output:\n%s",
				content, err, safeSnippet(output, 16<<10))
		}
	}) {
		return
	}
	if !t.Run("claude-partial-sse-exact-retry", func(t *testing.T) {
		const session = "323e4567-e89b-42d3-a456-426614174217"
		prompt := "Survive one interrupted partial stream and relay exactly " + containerClaudePartial + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			claudeWorkspace, []string{
				"claude", "-p", prompt, "--model", "claude-sonnet-4-5", "--session-id", session,
				"--output-format", "stream-json", "--verbose",
			}, containerDialogueScenario{
				marker: containerClaudePartial,
				clientEnvironment: []string{
					"ANTHROPIC_BASE_URL=" + fmt.Sprintf("http://host.testcontainers.internal:%d", claudeFaultPort),
				},
				faultSyncURL:   fmt.Sprintf("http://host.testcontainers.internal:%d/__human_fault/wait", claudeFaultPort),
				faultSyncToken: claudeFault.token,
			}, instance.CallerToken())
		if !strings.Contains(output, containerClaudePartial) {
			t.Fatalf("Claude partial-SSE retry omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
		claudeFault.assertProfiledRetry(t, session)
	}) {
		return
	}

	var openCodeSession string
	if !t.Run("opencode-chat-completions", func(t *testing.T) {
		prompt := "Compatibility probe: reply with exactly " + containerOpenCodeMarker + " and nothing else."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			openCodeWorkspace, []string{
				"opencode", "run", "--pure", "--auto", "--format", "json",
				"--model", "human-local/human-expert", "--dir", openCodeWorkspace, prompt,
			}, containerDialogueScenario{marker: containerOpenCodeMarker}, instance.CallerToken())
		if !strings.Contains(output, containerOpenCodeMarker) {
			t.Fatalf("OpenCode output omitted the LLM-human final:\n%s", safeSnippet(output, 16<<10))
		}
		openCodeSession = openCodeSessionID(output)
		if openCodeSession == "" {
			t.Fatalf("OpenCode output omitted its session id:\n%s", safeSnippet(output, 16<<10))
		}
	}) {
		return
	}
	if !t.Run("opencode-close-reopen-resume", func(t *testing.T) {
		prompt := "After reopening this OpenCode session, relay exactly " + containerOpenCodeResume + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			openCodeWorkspace, []string{
				"opencode", "run", "--pure", "--auto", "--format", "json",
				"--model", "human-local/human-expert", "--dir", openCodeWorkspace,
				"--session", openCodeSession, prompt,
			}, containerDialogueScenario{marker: containerOpenCodeResume}, instance.CallerToken())
		if !strings.Contains(output, containerOpenCodeResume) {
			t.Fatalf("reopened OpenCode session omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
	}) {
		return
	}
	if !t.Run("opencode-tool-success", func(t *testing.T) {
		prompt := "Run the Human's bash tool request, then relay a final containing " + containerOpenCodeTool + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			openCodeWorkspace, []string{
				"opencode", "run", "--pure", "--auto", "--format", "json",
				"--model", "human-local/human-expert", "--dir", openCodeWorkspace, prompt,
			}, containerDialogueScenario{
				marker: containerOpenCodeTool, toolMarker: containerOpenCodeTool,
				toolProfile: "opencode",
			}, instance.CallerToken())
		assertToolSuccess(t, "OpenCode", output, containerOpenCodeTool, openCodeWorkspace)
	}) {
		return
	}
	if !t.Run("opencode-tool-failure-recovers-to-final", func(t *testing.T) {
		prompt := "Run the Human's bash tool request even though it will fail, then relay a final containing " + containerOpenCodeFail + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			openCodeWorkspace, []string{
				"opencode", "run", "--pure", "--auto", "--format", "json",
				"--model", "human-local/human-expert", "--dir", openCodeWorkspace, prompt,
			}, containerDialogueScenario{
				marker: containerOpenCodeFail, toolMarker: containerToolError,
				toolProfile: "opencode", toolCommand: failingToolCommand(),
			}, instance.CallerToken())
		if !strings.Contains(output, containerOpenCodeFail) {
			t.Fatalf("OpenCode did not recover from the expected failed tool result:\n%s", safeSnippet(output, 16<<10))
		}
	}) {
		return
	}
	if !t.Run("opencode-native-todo-lifecycle", func(t *testing.T) {
		prompt := "Execute every Human todowrite lifecycle request, then relay a final containing " + containerOpenCodeTask + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			openCodeWorkspace, []string{
				"opencode", "run", "--pure", "--auto", "--format", "json",
				"--model", "human-local/human-expert", "--dir", openCodeWorkspace, prompt,
			}, containerDialogueScenario{
				marker: containerOpenCodeTask, action: "tasks", taskMarker: containerOpenCodeTask,
				toolProfile: "opencode",
			}, instance.CallerToken())
		if !strings.Contains(output, containerOpenCodeTask) {
			t.Fatalf("OpenCode task turn omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
	}) {
		return
	}

	if !t.Run("codex-responses-and-caller-tool", func(t *testing.T) {
		lastMessage := filepath.Join(codexWorkspace, "last-message.txt")
		prompt := "The Human may ask you to run one command. Run it, then relay exactly the Human's final answer " +
			containerCodexMarker + "."
		// Codex's Linux bwrap sandbox needs unprivileged user namespaces, which
		// ordinary Docker containers intentionally lack. The container is the
		// sandbox here: it receives only this test's temporary bind mount and the
		// ephemeral Human caller token.
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			codexWorkspace, []string{
				"codex", "exec", "--ignore-user-config", "--dangerously-bypass-approvals-and-sandbox",
				"--skip-git-repo-check", "--json", "--color", "never",
				"--cd", codexWorkspace, "--model", containerCodexModel, "--config", `model_provider="human_e2e"`,
				"--config", codexProvider, "--config", codexCatalogConfig, "--config", `web_search="cached"`,
				"--output-last-message", lastMessage, prompt,
			}, containerDialogueScenario{
				marker: containerCodexMarker, toolMarker: containerCodexTool,
				toolProfile: "codex",
			}, instance.CallerToken())
		if !strings.Contains(output, containerCodexMarker) {
			t.Fatalf("Codex output omitted the LLM-human final:\n%s", safeSnippet(output, 16<<10))
		}
		final, err := os.ReadFile(lastMessage)
		if err != nil || !strings.Contains(string(final), containerCodexMarker) {
			t.Fatalf("Codex last message = %q, read error %v", final, err)
		}
		proof, err := os.ReadFile(filepath.Join(codexWorkspace, "tool-proof.txt"))
		if err != nil || string(proof) != containerCodexTool {
			t.Fatalf("caller-workspace tool proof = %q, read error %v", proof, err)
		}
	}) {
		return
	}
	if !t.Run("codex-close-reopen-resume", func(t *testing.T) {
		lastMessage := filepath.Join(codexWorkspace, "resumed-last-message.txt")
		prompt := "After reopening this Codex thread, relay exactly " + containerCodexResume + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			codexWorkspace, []string{
				"codex", "exec", "resume", "--last", "--ignore-user-config",
				"--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--json",
				"--model", containerCodexModel, "--config", `model_provider="human_e2e"`,
				"--config", codexProvider, "--config", codexCatalogConfig, "--config", `web_search="cached"`,
				"--output-last-message", lastMessage, prompt,
			}, containerDialogueScenario{marker: containerCodexResume}, instance.CallerToken())
		if !strings.Contains(output, containerCodexResume) {
			t.Fatalf("reopened Codex thread omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
		final, err := os.ReadFile(lastMessage)
		if err != nil || !strings.Contains(string(final), containerCodexResume) {
			t.Fatalf("resumed Codex last message = %q, read error %v", final, err)
		}
	}) {
		return
	}

	if !t.Run("codex-workspace-create-then-native-apply-patch", func(t *testing.T) {
		prompt := "Apply every workspace change the Human delivers through the native apply_patch tool, then relay exactly " +
			containerCodexWorkspace + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			codexWorkspace, []string{
				"codex", "exec", "--ignore-user-config", "--ephemeral",
				"--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--json",
				"--color", "never", "--cd", codexWorkspace, "--model", containerCodexModel,
				"--config", `model_provider="human_e2e"`, "--config", codexProvider,
				"--config", codexCatalogConfig, "--config", `web_search="cached"`, prompt,
			}, containerDialogueScenario{
				marker: containerCodexWorkspace, action: "workspace",
				workspacePath: "codex-workspace.txt", workspaceFirst: "codex-workspace-v1\n",
				workspaceSecond: "codex-workspace-v2\n",
			}, instance.CallerToken())
		if !strings.Contains(output, containerCodexWorkspace) {
			t.Fatalf("Codex workspace turn omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
		content, err := os.ReadFile(filepath.Join(codexWorkspace, "codex-workspace.txt"))
		if err != nil || string(content) != "codex-workspace-v2\n" {
			t.Fatalf("Codex caller workspace file = %q, read error %v\nCodex output:\n%s",
				content, err, safeSnippet(output, 16<<10))
		}
	}) {
		return
	}

	if !t.Run("opencode-workspace-create-then-native-edit", func(t *testing.T) {
		prompt := "Apply every workspace change the Human delivers through native tools, then relay exactly " +
			containerWorkspace + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			openCodeWorkspace, []string{
				"opencode", "run", "--pure", "--auto", "--format", "json",
				"--model", "human-local/human-expert", "--dir", openCodeWorkspace, prompt,
			}, containerDialogueScenario{marker: containerWorkspace, action: "workspace"}, instance.CallerToken())
		if !strings.Contains(output, containerWorkspace) {
			t.Fatalf("OpenCode workspace turn omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
		for _, tool := range []string{"write", "edit"} {
			if !strings.Contains(output, `"tool":"`+tool+`"`) &&
				!strings.Contains(output, `"tool": "`+tool+`"`) {
				t.Fatalf("OpenCode workspace output shows no native %s call:\n%s", tool, safeSnippet(output, 16<<10))
			}
		}
		content, err := os.ReadFile(filepath.Join(openCodeWorkspace, "browser-workspace.txt"))
		if err != nil || string(content) != "workspace-v2\n" {
			t.Fatalf("caller workspace file = %q, read error %v\nOpenCode output:\n%s",
				content, err, safeSnippet(output, 16<<10))
		}
	}) {
		return
	}

	if !t.Run("codex-failed-tool-result-recovers-to-final", func(t *testing.T) {
		prompt := "The Human will ask you to run one command that is expected to fail. Run it once, then relay exactly " +
			containerToolFailure + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			codexWorkspace, []string{
				"codex", "exec", "--ignore-user-config", "--ephemeral", "--dangerously-bypass-approvals-and-sandbox",
				"--skip-git-repo-check", "--json", "--color", "never", "--cd", codexWorkspace,
				"--model", containerCodexModel, "--config", `model_provider="human_e2e"`,
				"--config", codexProvider, "--config", codexCatalogConfig, "--config", `web_search="cached"`, prompt,
			}, containerDialogueScenario{
				marker: containerToolFailure, toolMarker: containerToolError,
				toolProfile: "codex", toolCommand: failingToolCommand(),
			}, instance.CallerToken())
		if !strings.Contains(output, containerToolFailure) {
			t.Fatalf("Codex did not recover from the expected failed tool result:\n%s", safeSnippet(output, 16<<10))
		}
	}) {
		return
	}
	if !t.Run("codex-native-plan-lifecycle", func(t *testing.T) {
		prompt := "Execute every Human update_plan lifecycle request, then relay exactly " + containerCodexTask + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			codexWorkspace, []string{
				"codex", "exec", "--ignore-user-config", "--ephemeral",
				"--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--json",
				"--color", "never", "--cd", codexWorkspace, "--model", containerCodexModel,
				"--config", `model_provider="human_e2e"`, "--config", codexProvider,
				"--config", codexCatalogConfig, "--config", `web_search="cached"`, prompt,
			}, containerDialogueScenario{
				marker: containerCodexTask, action: "tasks", taskMarker: containerCodexTask,
				toolProfile: "codex",
			}, instance.CallerToken())
		if !strings.Contains(output, containerCodexTask) {
			t.Fatalf("Codex plan turn omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
	}) {
		return
	}
	if !t.Run("codex-partial-sse-profiled-retry", func(t *testing.T) {
		t.Cleanup(func() {
			if diagnostic := codexFault.retryDiagnostic(); diagnostic != "" {
				t.Log(diagnostic)
			}
		})
		partialProvider := fmt.Sprintf(
			`model_providers.human_partial={ name = "Human Partial", base_url = %q, env_key = "HUMAN_CODEX_API_KEY", wire_api = "responses", request_max_retries = 0, stream_max_retries = 2, stream_idle_timeout_ms = 60000 }`,
			fmt.Sprintf("http://host.testcontainers.internal:%d/v1", codexFaultPort),
		)
		prompt := "Survive one interrupted partial stream and relay exactly " + containerCodexPartial + "."
		output := runContainerDialogue(t, container, browserContainer, browserConfig,
			codexWorkspace, []string{
				"codex", "exec", "--ignore-user-config", "--ephemeral",
				"--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--json",
				"--color", "never", "--cd", codexWorkspace, "--model", containerCodexModel,
				"--config", `model_provider="human_partial"`, "--config", partialProvider,
				"--config", codexCatalogConfig, "--config", `web_search="cached"`, prompt,
			}, containerDialogueScenario{
				marker:         containerCodexPartial,
				faultSyncURL:   fmt.Sprintf("http://host.testcontainers.internal:%d/__human_fault/wait", codexFaultPort),
				faultSyncToken: codexFault.token,
			}, instance.CallerToken())
		if !strings.Contains(output, containerCodexPartial) {
			t.Fatalf("Codex partial-SSE retry omitted the Human final:\n%s", safeSnippet(output, 16<<10))
		}
		codexFault.assertProfiledRetry(t, "")
		if os.Getenv("HUMAN_TEST_TRACE") == "1" {
			t.Logf("Codex declared tools:\n%s", codexFault.firstRequestToolDiagnostic())
		}
	}) {
		return
	}

	rejections := []struct {
		name      string
		marker    string
		workspace string
		command   []string
	}{
		{
			name: "claude-reject", marker: containerClaudeReject, workspace: claudeWorkspace,
			command: []string{"claude", "-p", "Request " + containerClaudeReject,
				"--model", "claude-sonnet-4-5", "--output-format", "json"},
		},
		{
			name: "opencode-reject", marker: containerOpenCodeReject, workspace: openCodeWorkspace,
			command: []string{"opencode", "run", "--pure", "--auto", "--format", "json",
				"--model", "human-local/human-expert", "--dir", openCodeWorkspace,
				"Request " + containerOpenCodeReject},
		},
		{
			name: "codex-reject", marker: containerCodexReject, workspace: codexWorkspace,
			command: []string{"codex", "exec", "--ignore-user-config", "--ephemeral",
				"--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--json",
				"--color", "never", "--cd", codexWorkspace, "--model", containerCodexModel,
				"--config", `model_provider="human_e2e"`, "--config", codexProvider,
				"--config", codexCatalogConfig, "--config", `web_search="cached"`, "Request " + containerCodexReject},
		},
	}
	for _, rejection := range rejections {
		rejection := rejection
		if !t.Run(rejection.name, func(t *testing.T) {
			// A rejection arrives inside an already-200 streaming response. Real
			// clients differ on whether they exit, retry, or wait after that error,
			// so bound the process while the browser rejects every visible retry.
			// Some CLIs leave a helper process holding the Docker exec stdout pipe
			// after SIGTERM. Escalate after a short grace period so an expected
			// unhappy path cannot strand the whole product gate.
			bounded := append([]string{"timeout", "--kill-after=5s", "25s"}, rejection.command...)
			output := runContainerDialogue(t, container, browserContainer, browserConfig,
				rejection.workspace, bounded, containerDialogueScenario{
					marker: rejection.marker, action: "reject", wantClientError: true,
				}, instance.CallerToken())
			if strings.Contains(output, rejection.marker) {
				t.Fatalf("rejected client received a successful marker:\n%s", safeSnippet(output, 16<<10))
			}
		}) {
			return
		}
	}
}

type realClientsContainerConfig struct {
	modelPort      int
	modelBaseURL   string
	callerToken    string
	workspace      string
	home           string
	extraHostPorts []int
}

func startRealClientsContainer(ctx context.Context, config realClientsContainerConfig) (testcontainers.Container, error) {
	var buildLog tailWriter
	request := testcontainers.ContainerRequest{
		Cmd:             []string{"sleep", "infinity"},
		HostAccessPorts: append([]int{config.modelPort}, config.extraHostPorts...),
		ConfigModifier: func(containerConfig *mobycontainer.Config) {
			containerConfig.User = currentContainerUser()
		},
		HostConfigModifier: func(hostConfig *mobycontainer.HostConfig) {
			hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{
				Type: mount.TypeBind, Source: config.workspace, Target: config.workspace,
			})
		},
		Env: map[string]string{
			"HOME":               config.home,
			"HUMAN_CALLER_TOKEN": config.callerToken,
			"ANTHROPIC_BASE_URL": config.modelBaseURL,
			"ANTHROPIC_API_KEY":  config.callerToken,
			"CLAUDE_CONFIG_DIR":  filepath.Join(config.home, "claude"),
			"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
			"DISABLE_AUTOUPDATER":                      "1",
			"XDG_CONFIG_HOME":                          filepath.Join(config.home, "xdg-config"),
			"XDG_DATA_HOME":                            filepath.Join(config.home, "xdg-data"),
			"CODEX_HOME":                               filepath.Join(config.home, "codex"),
			"HUMAN_CODEX_API_KEY":                      config.callerToken,
			"NO_PROXY":                                 "host.testcontainers.internal,127.0.0.1,localhost",
			"no_proxy":                                 "host.testcontainers.internal,127.0.0.1,localhost",
			"CI":                                       "1",
		},
	}
	if image := strings.TrimSpace(os.Getenv(containerImageEnv)); image != "" {
		request.Image = image
	} else {
		request.FromDockerfile = testcontainers.FromDockerfile{
			Context: realClientsDockerContext(), Dockerfile: "Dockerfile", BuildLogWriter: &buildLog,
		}
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	if err != nil && buildLog.Len() != 0 {
		return nil, fmt.Errorf("%w\nDocker build log tail:\n%s", err, buildLog.String())
	}
	return container, err
}

func startBrowserHumanContainer(
	ctx context.Context,
	webPort, relayPort int,
	humanWorkspaceRoot string,
	extraHostPorts ...int,
) (testcontainers.Container, error) {
	var buildLog tailWriter
	request := testcontainers.ContainerRequest{
		Cmd:             []string{"sleep", "infinity"},
		HostAccessPorts: append([]int{webPort, relayPort}, extraHostPorts...),
		Env: map[string]string{
			"NO_PROXY": "host.testcontainers.internal,127.0.0.1,localhost",
			"no_proxy": "host.testcontainers.internal,127.0.0.1,localhost",
			"CI":       "1",
		},
		HostConfigModifier: func(hostConfig *mobycontainer.HostConfig) {
			hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{
				Type: mount.TypeBind, Source: humanWorkspaceRoot, Target: humanWorkspaceRoot,
			})
		},
	}
	if image := strings.TrimSpace(os.Getenv(containerBrowserImageEnv)); image != "" {
		request.Image = image
	} else {
		request.FromDockerfile = testcontainers.FromDockerfile{
			Context: browserHumanDockerContext(), Dockerfile: "Dockerfile", BuildLogWriter: &buildLog,
		}
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	if err != nil && buildLog.Len() != 0 {
		return nil, fmt.Errorf("%w\nDocker build log tail:\n%s", err, buildLog.String())
	}
	return container, err
}

// tailWriter keeps Docker diagnostics bounded. Image construction has no
// runtime credentials, but successful multi-hundred-line package logs should
// not make the normal test output unreadable.
type tailWriter struct {
	mu     sync.Mutex
	buffer []byte
}

func (writer *tailWriter) Write(input []byte) (int, error) {
	written := len(input)
	writer.mu.Lock()
	defer writer.mu.Unlock()
	const limit = 32 << 10
	if len(input) >= limit {
		writer.buffer = append(writer.buffer[:0], input[len(input)-limit:]...)
		return written, nil
	}
	writer.buffer = append(writer.buffer, input...)
	if overflow := len(writer.buffer) - limit; overflow > 0 {
		copy(writer.buffer, writer.buffer[overflow:])
		writer.buffer = writer.buffer[:limit]
	}
	return written, nil
}

func (writer *tailWriter) Len() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return len(writer.buffer)
}

func (writer *tailWriter) String() string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return string(writer.buffer)
}

func realClientsDockerContext() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return filepath.Join("..", "..", "testdata", "real-clients")
	}
	return filepath.Join(filepath.Dir(filename), "..", "..", "testdata", "real-clients")
}

func browserHumanDockerContext() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return filepath.Join("..", "..", "testdata", "browser-human")
	}
	return filepath.Join(filepath.Dir(filename), "..", "..", "testdata", "browser-human")
}

func currentContainerUser() string {
	current, err := user.Current()
	if err != nil {
		return ""
	}
	if _, err := strconv.ParseUint(current.Uid, 10, 32); err != nil {
		return ""
	}
	if _, err := strconv.ParseUint(current.Gid, 10, 32); err != nil {
		return ""
	}
	return current.Uid + ":" + current.Gid
}

func assertContainerClientVersions(t *testing.T, container testcontainers.Container) {
	t.Helper()
	checks := []struct {
		command []string
		want    string
	}{
		{[]string{"claude", "--version"}, "2.1.217 (Claude Code)"},
		{[]string{"opencode", "--version"}, "1.17.18"},
		{[]string{"codex", "--version"}, "codex-cli 0.145.0"},
	}
	for _, check := range checks {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		output, err := execInContainer(ctx, container, "/tmp", check.command)
		cancel()
		if err != nil || strings.TrimSpace(output) != check.want {
			t.Fatalf("container client version %q = %q, error %v", check.command[0], output, err)
		}
	}
}

func assertBrowserVersion(t *testing.T, container testcontainers.Container) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	output, err := execInContainer(ctx, container, "/opt/human-browser", []string{"npx", "playwright", "--version"})
	if err != nil || strings.TrimSpace(output) != "Version 1.61.1" {
		t.Fatalf("container Playwright version = %q, error %v", output, err)
	}
}

func assertContainerFilesystemIsolation(
	t *testing.T,
	callerContainer, browserContainer testcontainers.Container,
	callerWorkspace, humanWorkspaceRoot string,
) {
	t.Helper()
	check := func(role string, container testcontainers.Container, owned, foreign string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		output, err := execInContainer(ctx, container, "/", []string{
			"sh", "-ceu",
			`test -d "$1"; test ! -e "$2"`,
			"human-filesystem-isolation", owned, foreign,
		})
		if err != nil {
			t.Fatalf("%s filesystem boundary failed: %v\n%s", role, err, output)
		}
	}
	check("Agent-user container", callerContainer, callerWorkspace, humanWorkspaceRoot)
	check("Human-browser container", browserContainer, humanWorkspaceRoot, callerWorkspace)
}

type containerExecResult struct {
	role   string
	output string
	err    error
}

type containerBrowserConfig struct {
	webURL     string
	webToken   string
	replyURL   string
	replyToken string
}

type containerDialogueScenario struct {
	marker            string
	action            string
	toolMarker        string
	toolProfile       string
	toolCommand       string
	taskMarker        string
	workspacePath     string
	workspaceFirst    string
	workspaceSecond   string
	clientEnvironment []string
	faultSyncURL      string
	faultSyncToken    string
	wantClientError   bool
}

func runContainerDialogue(
	t *testing.T,
	clientContainer testcontainers.Container,
	browserContainer testcontainers.Container,
	browserConfig containerBrowserConfig,
	workingDirectory string,
	command []string,
	scenario containerDialogueScenario,
	callerToken string,
) string {
	t.Helper()
	action := scenario.action
	if action == "" {
		action = "final"
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	results := make(chan containerExecResult, 2)
	go func() {
		output, err := execInContainerWithEnv(ctx, clientContainer, workingDirectory, command,
			scenario.clientEnvironment)
		results <- containerExecResult{role: "caller client", output: output, err: err}
	}()
	go func() {
		output, err := runBrowserHuman(ctx, browserContainer, browserConfig, scenario)
		results <- containerExecResult{role: "browser human", output: output, err: err}
	}()

	completed := make(map[string]containerExecResult, 2)
	collecting := true
	for len(completed) < 2 && collecting {
		select {
		case result := <-results:
			completed[result.role] = result
			if result.err != nil && (result.role != "caller client" || !scenario.wantClientError) {
				cancel()
			}
		case <-ctx.Done():
			if len(completed) == 0 {
				t.Fatalf("container dialogue timed out: %v", ctx.Err())
			}
			// A failed peer cancels the shared context. Preserve both sides' output
			// when possible; the caller trace is often what explains a DOM timeout.
			select {
			case result := <-results:
				completed[result.role] = result
			case <-time.After(5 * time.Second):
				collecting = false
			}
		}
	}
	redactions := []string{callerToken, browserConfig.webToken, browserConfig.replyToken}
	var failures []string
	for _, role := range []string{"browser human", "caller client"} {
		result, ok := completed[role]
		if !ok {
			failures = append(failures, role+" did not exit after its peer failed")
			continue
		}
		output := redactContainerOutput(result.output, redactions...)
		if result.err != nil && (role != "caller client" || !scenario.wantClientError) {
			failures = append(failures, fmt.Sprintf("%s failed: %v\n%s",
				role, result.err, safeSnippet(output, 16<<10)))
		}
	}
	if len(failures) != 0 {
		for _, role := range []string{"browser human", "caller client"} {
			if result, ok := completed[role]; ok && result.err == nil {
				failures = append(failures, fmt.Sprintf("%s output:\n%s", role,
					safeSnippet(redactContainerOutput(result.output, redactions...), 16<<10)))
			}
		}
		t.Fatal(strings.Join(failures, "\n\n"))
	}
	if scenario.wantClientError && completed["caller client"].err == nil {
		t.Fatalf("caller client unexpectedly succeeded after %s:\n%s", action,
			safeSnippet(redactContainerOutput(completed["caller client"].output, redactions...), 16<<10))
	}
	browserOutput := completed["browser human"].output
	if os.Getenv("HUMAN_TEST_TRACE") == "1" {
		t.Logf("browser-human trace:\n%s", safeSnippet(
			redactContainerOutput(browserOutput, redactions...), 64<<10,
		))
	}
	if !strings.Contains(browserOutput, "browser-human-ok:"+action+":"+scenario.marker) {
		t.Fatalf("browser human did not finish the marked DOM conversation:\n%s",
			safeSnippet(redactContainerOutput(browserOutput, redactions...), 16<<10))
	}
	return redactContainerOutput(completed["caller client"].output, redactions...)
}

func runBrowserHuman(
	ctx context.Context,
	browserContainer testcontainers.Container,
	browserConfig containerBrowserConfig,
	scenario containerDialogueScenario,
) (string, error) {
	action := scenario.action
	if action == "" {
		action = "final"
	}
	environment := []string{
		"WEB_URL=" + browserConfig.webURL,
		"WEB_TOKEN=" + browserConfig.webToken,
		"REPLY_URL=" + browserConfig.replyURL,
		"REPLY_TOKEN=" + browserConfig.replyToken,
		"FINAL_MARKER=" + scenario.marker,
		"HUMAN_ACTION=" + action,
		"TOOL_MARKER=" + scenario.toolMarker,
		"TOOL_PROFILE=" + scenario.toolProfile,
		"TOOL_COMMAND=" + scenario.toolCommand,
		"TASK_MARKER=" + scenario.taskMarker,
		"WORKSPACE_PATH=" + scenario.workspacePath,
		"WORKSPACE_FIRST=" + scenario.workspaceFirst,
		"WORKSPACE_SECOND=" + scenario.workspaceSecond,
		"FAULT_SYNC_URL=" + scenario.faultSyncURL,
		"FAULT_SYNC_TOKEN=" + scenario.faultSyncToken,
	}
	return execInContainerWithEnv(ctx, browserContainer, "/opt/human-browser",
		[]string{"node", "human-operator.mjs"}, environment)
}

func execInContainer(
	ctx context.Context,
	container testcontainers.Container,
	workingDirectory string,
	command []string,
) (string, error) {
	return execInContainerWithEnv(ctx, container, workingDirectory, command, nil)
}

func execInContainerWithEnv(
	ctx context.Context,
	container testcontainers.Container,
	workingDirectory string,
	command []string,
	environment []string,
) (string, error) {
	options := []tcexec.ProcessOption{tcexec.Multiplexed(), tcexec.WithWorkingDir(workingDirectory)}
	if len(environment) != 0 {
		sort.Strings(environment)
		options = append(options, tcexec.WithEnv(environment))
	}
	exitCode, output, err := container.Exec(ctx, command, options...)
	if err != nil {
		return "", err
	}
	encoded, err := io.ReadAll(io.LimitReader(output, (8<<20)+1))
	if err != nil {
		return "", err
	}
	if len(encoded) > 8<<20 {
		return string(encoded[:8<<20]), errors.New("container client output exceeds 8 MiB")
	}
	if exitCode != 0 {
		return string(encoded), fmt.Errorf("container command exited with status %d", exitCode)
	}
	return string(encoded), nil
}

func redactContainerOutput(output string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			output = strings.ReplaceAll(output, secret, "[REDACTED]")
		}
	}
	return output
}

func writeContainerOpenCodeConfig(workspace, modelBaseURL string) error {
	config := map[string]any{
		"$schema": "https://opencode.ai/config.json",
		"provider": map[string]any{"human-local": map[string]any{
			"npm": "@ai-sdk/openai-compatible", "name": "Human Local Container",
			"options": map[string]any{
				"baseURL": modelBaseURL + "/v1", "apiKey": "{env:HUMAN_CALLER_TOKEN}",
			},
			"models": map[string]any{"human-expert": map[string]any{
				"name": "Human Expert", "limit": map[string]int{"context": 100000, "output": 4096},
			}},
		}},
	}
	encoded, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(workspace, "opencode.json"), append(encoded, '\n'), 0o600)
}

// writeContainerCodexModelCatalog makes the pinned Codex client expose its
// native freeform apply_patch tool even though the hermetic container has no
// OpenAI account with which to refresh the remote model catalog.
func writeContainerCodexModelCatalog(workspace string) (string, error) {
	path := filepath.Join(workspace, "codex-model-catalog.json")
	catalog := map[string]any{
		"models": []any{map[string]any{
			"slug":                         containerCodexModel,
			"display_name":                 "Human Expert",
			"description":                  "HumanLLM Testcontainers model",
			"default_reasoning_level":      "low",
			"supported_reasoning_levels":   []any{map[string]any{"effort": "low", "description": "Test effort"}},
			"shell_type":                   "shell_command",
			"visibility":                   "none",
			"supported_in_api":             true,
			"priority":                     1,
			"availability_nux":             nil,
			"upgrade":                      nil,
			"base_instructions":            "You are a coding agent. Use caller tools exactly when the Human requests them.",
			"default_reasoning_summary":    "auto",
			"support_verbosity":            false,
			"default_verbosity":            nil,
			"apply_patch_tool_type":        "freeform",
			"web_search_tool_type":         "text",
			"truncation_policy":            map[string]any{"mode": "bytes", "limit": 10000},
			"supports_parallel_tool_calls": true,
			"context_window":               100000,
			"max_context_window":           100000,
			"experimental_supported_tools": []any{},
			"input_modalities":             []string{"text", "image"},
		}},
	}
	encoded, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, append(encoded, '\n'), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

// containerReplyRelay lets the browser-side operator ask the configured LLM
// what to type without putting the external API key in a container. Both the
// relay credential and its loopback listener live only for this test.
type containerReplyRelay struct {
	server *httptest.Server
	token  string
}

func newContainerReplyRelay(t *testing.T, humanLLM *containerHumanLLM) *containerReplyRelay {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("generate browser reply relay credential: %v", err)
	}
	relay := &containerReplyRelay{token: fmt.Sprintf("%x", random)}
	relay.server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/reply" {
			http.NotFound(response, request)
			return
		}
		provided := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
		if len(provided) != len(relay.token) || subtle.ConstantTimeCompare([]byte(provided), []byte(relay.token)) != 1 {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		request.Body = http.MaxBytesReader(response, request.Body, 64<<10)
		var body struct {
			Prompt string `json:"prompt"`
		}
		decoder := json.NewDecoder(request.Body)
		if err := decoder.Decode(&body); err != nil || strings.TrimSpace(body.Prompt) == "" {
			http.Error(response, "invalid request", http.StatusBadRequest)
			return
		}
		answer, err := humanLLM.complete(request.Context(), body.Prompt)
		if err != nil {
			http.Error(response, "human LLM failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]string{"answer": answer})
	}))
	t.Cleanup(relay.server.Close)
	return relay
}

var errContainerPartialSSEDrop = errors.New("injected container partial SSE drop")

type containerPartialAttemptContextKey struct{}

type containerPartialAttempt struct {
	digest         [sha256.Size]byte
	body           []byte
	identity       string
	idempotencyKey string
	statusCode     int
}

// containerPartialSSEProxy severs exactly one real client stream after a full
// progress frame, then observes the client's version-profiled retry. Its /wait
// endpoint is an authenticated, loopback-only barrier used by Chromium so the
// human does not send the final event before recovery has actually happened.
type containerPartialSSEProxy struct {
	*httptest.Server
	token      string
	targetPath string
	marker     []byte

	mu         sync.Mutex
	attempts   []*containerPartialAttempt
	dropped    bool
	replayed   *containerPartialAttempt
	dropOnce   chan struct{}
	replayOnce chan struct{}
}

func newContainerPartialSSEProxy(t *testing.T, upstream, targetPath, marker string) *containerPartialSSEProxy {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("generate partial-SSE synchronization credential: %v", err)
	}
	state := &containerPartialSSEProxy{
		token: fmt.Sprintf("%x", random), targetPath: targetPath, marker: []byte(marker),
		dropOnce: make(chan struct{}), replayOnce: make(chan struct{}),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.FlushInterval = -1
	proxy.ErrorLog = log.New(io.Discard, "", 0)
	proxy.ModifyResponse = func(response *http.Response) error {
		attempt, _ := response.Request.Context().Value(containerPartialAttemptContextKey{}).(*containerPartialAttempt)
		if attempt == nil {
			return nil
		}
		state.recordResponse(attempt, response.StatusCode, response.Header.Get("Idempotency-Key"))
		if response.StatusCode == http.StatusOK &&
			strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") && state.canDrop(attempt) {
			response.Body = &containerAbortAfterMarker{
				ReadCloser: response.Body, attempt: attempt, state: state,
			}
		}
		return nil
	}
	handler := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/__human_fault/wait" {
			state.serveWait(response, request)
			return
		}
		if request.Method == http.MethodPost && request.URL.Path == targetPath {
			body, readErr := io.ReadAll(io.LimitReader(request.Body, (8<<20)+1))
			if readErr != nil || len(body) > 8<<20 {
				http.Error(response, "fault proxy could not read bounded request", http.StatusBadGateway)
				return
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			request.ContentLength = int64(len(body))
			identity := request.Header.Get("X-Claude-Code-Session-Id")
			if targetPath == "/v1/responses" {
				identity = request.Header.Get("X-Codex-Turn-Metadata")
			}
			attempt := state.recordAttempt(body, identity)
			request = request.WithContext(context.WithValue(request.Context(), containerPartialAttemptContextKey{}, attempt))
		}
		proxy.ServeHTTP(response, request)
	})
	state.Server = httptest.NewServer(handler)
	return state
}

func (proxy *containerPartialSSEProxy) recordAttempt(body []byte, identity string) *containerPartialAttempt {
	attempt := &containerPartialAttempt{
		digest: sha256.Sum256(body), body: append([]byte(nil), body...), identity: identity,
	}
	proxy.mu.Lock()
	proxy.attempts = append(proxy.attempts, attempt)
	proxy.mu.Unlock()
	return attempt
}

func (proxy *containerPartialSSEProxy) recordResponse(attempt *containerPartialAttempt, status int, key string) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	attempt.statusCode = status
	attempt.idempotencyKey = key
	equivalent := false
	if len(proxy.attempts) > 1 {
		first := proxy.attempts[0]
		equivalent = attempt.digest == first.digest
		if proxy.targetPath == "/v1/responses" {
			equivalent = codexRetryBodiesEquivalent(first.body, attempt.body)
		}
	}
	if proxy.dropped && proxy.replayed == nil && equivalent &&
		attempt.identity == proxy.attempts[0].identity {
		proxy.replayed = attempt
		close(proxy.replayOnce)
	}
}

func (proxy *containerPartialSSEProxy) canDrop(attempt *containerPartialAttempt) bool {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	return !proxy.dropped && len(proxy.attempts) > 0 && attempt == proxy.attempts[0]
}

func (proxy *containerPartialSSEProxy) markDropped(attempt *containerPartialAttempt) bool {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if proxy.dropped || len(proxy.attempts) == 0 || attempt != proxy.attempts[0] {
		return false
	}
	proxy.dropped = true
	close(proxy.dropOnce)
	return true
}

func (proxy *containerPartialSSEProxy) serveWait(response http.ResponseWriter, request *http.Request) {
	provided := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if request.Method != http.MethodGet || len(provided) != len(proxy.token) ||
		subtle.ConstantTimeCompare([]byte(provided), []byte(proxy.token)) != 1 {
		http.Error(response, "unauthorized", http.StatusUnauthorized)
		return
	}
	timer := time.NewTimer(45 * time.Second)
	defer timer.Stop()
	select {
	case <-proxy.replayOnce:
		response.WriteHeader(http.StatusNoContent)
	case <-request.Context().Done():
	case <-timer.C:
		http.Error(response, "caller did not retry the interrupted stream", http.StatusGatewayTimeout)
	}
}

func (proxy *containerPartialSSEProxy) assertProfiledRetry(t *testing.T, wantIdentity string) {
	t.Helper()
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if !proxy.dropped || proxy.replayed == nil || len(proxy.attempts) != 2 {
		t.Fatalf("partial-SSE attempts=%d dropped=%t replayed=%t; want one drop plus one exact retry",
			len(proxy.attempts), proxy.dropped, proxy.replayed != nil)
	}
	first, replay := proxy.attempts[0], proxy.attempts[1]
	bodyEquivalent := first.digest == replay.digest
	if proxy.targetPath == "/v1/responses" {
		bodyEquivalent = codexRetryBodiesEquivalent(first.body, replay.body)
	}
	if !bodyEquivalent || first.identity == "" || first.identity != replay.identity {
		t.Fatalf("partial-SSE retry changed non-profiled body fields or identity: first=%q replay=%q",
			first.identity, replay.identity)
	}
	if wantIdentity != "" && first.identity != wantIdentity {
		t.Fatalf("partial-SSE identity = %q, want %q", first.identity, wantIdentity)
	}
	if first.idempotencyKey == "" || first.idempotencyKey != replay.idempotencyKey ||
		first.statusCode != http.StatusOK || replay.statusCode != http.StatusOK {
		t.Fatalf("partial-SSE replay response identity/status = %q/%d then %q/%d",
			first.idempotencyKey, first.statusCode, replay.idempotencyKey, replay.statusCode)
	}
}

func (proxy *containerPartialSSEProxy) firstRequestToolDiagnostic() string {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if len(proxy.attempts) == 0 {
		return "no captured request"
	}
	var request struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(proxy.attempts[0].body, &request); err != nil {
		return "decode request: " + err.Error()
	}
	encoded, err := json.MarshalIndent(request.Tools, "", "  ")
	if err != nil {
		return "encode tools: " + err.Error()
	}
	return safeSnippet(string(encoded), 64<<10)
}

// Codex 0.145.0 may reorder client_metadata and has also been observed to drop
// only its redundant client_metadata.session_id on a stream retry. The
// canonical X-Codex-Turn-Metadata (including session_id, thread_id and turn_id)
// remains byte-identical, as do all model inputs and tools. Treat only that
// captured, version-specific normalization as equivalent; any other body drift
// fails the gate.
func codexRetryBodiesEquivalent(first, second []byte) bool {
	changed := changedJSONFields(first, second)
	if len(changed) == 0 {
		return true
	}
	if len(changed) != 1 || changed[0] != "client_metadata" {
		return false
	}
	var leftTop, rightTop map[string]json.RawMessage
	if json.Unmarshal(first, &leftTop) != nil || json.Unmarshal(second, &rightTop) != nil {
		return false
	}
	var left, right map[string]string
	if json.Unmarshal(leftTop["client_metadata"], &left) != nil ||
		json.Unmarshal(rightTop["client_metadata"], &right) != nil {
		return false
	}
	if mapsEqual(left, right) {
		return true
	}
	if left["session_id"] == "" {
		return false
	}
	delete(left, "session_id")
	if _, exists := right["session_id"]; exists {
		return false
	}
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func (proxy *containerPartialSSEProxy) retryDiagnostic() string {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if len(proxy.attempts) == 2 && proxy.dropped && proxy.replayed != nil {
		return ""
	}
	var details strings.Builder
	fmt.Fprintf(&details, "partial-SSE diagnostic: attempts=%d dropped=%t replayed=%t",
		len(proxy.attempts), proxy.dropped, proxy.replayed != nil)
	for index, attempt := range proxy.attempts {
		fmt.Fprintf(&details, "\n  attempt %d digest=%x identity=%q key=%q status=%d",
			index+1, attempt.digest[:8], attempt.identity, attempt.idempotencyKey, attempt.statusCode)
	}
	if len(proxy.attempts) > 1 {
		fmt.Fprintf(&details, "\n  changed top-level fields: %v",
			changedJSONFields(proxy.attempts[0].body, proxy.attempts[1].body))
		var first, second map[string]json.RawMessage
		if json.Unmarshal(proxy.attempts[0].body, &first) == nil &&
			json.Unmarshal(proxy.attempts[1].body, &second) == nil &&
			!bytes.Equal(first["client_metadata"], second["client_metadata"]) {
			fmt.Fprintf(&details, "\n  client_metadata: %s -> %s",
				first["client_metadata"], second["client_metadata"])
		}
	}
	return details.String()
}

func changedJSONFields(first, second []byte) []string {
	var left, right map[string]json.RawMessage
	if json.Unmarshal(first, &left) != nil || json.Unmarshal(second, &right) != nil {
		return []string{"<invalid-json>"}
	}
	keys := make(map[string]struct{}, len(left)+len(right))
	for key := range left {
		keys[key] = struct{}{}
	}
	for key := range right {
		keys[key] = struct{}{}
	}
	var changed []string
	for key := range keys {
		if !jsonRawEqual(left[key], right[key]) {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

func jsonRawEqual(left, right json.RawMessage) bool {
	if bytes.Equal(left, right) {
		return true
	}
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

type containerAbortAfterMarker struct {
	io.ReadCloser
	attempt *containerPartialAttempt
	state   *containerPartialSSEProxy
	tail    []byte
	seen    bool
	abort   bool
}

func (reader *containerAbortAfterMarker) Read(buffer []byte) (int, error) {
	if reader.abort {
		if reader.state.markDropped(reader.attempt) {
			return 0, errContainerPartialSSEDrop
		}
		reader.abort = false
	}
	n, err := reader.ReadCloser.Read(buffer)
	if n == 0 {
		return n, err
	}
	combined := append(append([]byte(nil), reader.tail...), buffer[:n]...)
	if !reader.seen {
		markerAt := bytes.Index(combined, reader.state.marker)
		if markerAt < 0 {
			keep := len(reader.state.marker) - 1
			if keep > len(combined) {
				keep = len(combined)
			}
			reader.tail = append(reader.tail[:0], combined[len(combined)-keep:]...)
			return n, nil
		}
		reader.seen = true
		combined = combined[markerAt+len(reader.state.marker):]
	}
	if bytes.Contains(combined, []byte("\n\n")) {
		reader.abort = true
	}
	keep := 1
	if keep > len(combined) {
		keep = len(combined)
	}
	reader.tail = append(reader.tail[:0], combined[len(combined)-keep:]...)
	return n, nil
}

// localWebAPI contains only the public browser endpoint and its ephemeral login
// token. All conversation actions happen in Chromium through the actual DOM.
type localWebAPI struct {
	base  string
	token string
}

func webAPIForLocal(t *testing.T, instance *humanlocal.Local) localWebAPI {
	t.Helper()
	webURL := instance.WebURL()
	const separator = "/?token="
	index := strings.Index(webURL, separator)
	if index < 0 {
		t.Fatalf("local web URL has no login token")
	}
	return localWebAPI{base: webURL[:index], token: webURL[index+len(separator):]}
}

type containerHumanLLM struct {
	endpoint string
	apiKey   string
	model    string
	client   *http.Client
}

func newContainerHumanLLM(t *testing.T) *containerHumanLLM {
	t.Helper()
	baseURL := strings.TrimSpace(os.Getenv(containerLLMBaseEnv))
	apiKey := strings.TrimSpace(os.Getenv(containerLLMKeyEnv))
	model := strings.TrimSpace(os.Getenv(containerLLMModelEnv))
	if os.Getenv(containerFakeLLMEnv) == "1" {
		server := httptest.NewServer(http.HandlerFunc(fakeContainerLLMHandler))
		t.Cleanup(server.Close)
		baseURL, apiKey, model = server.URL, "test-only", "test-only"
	} else if apiKey == "" {
		t.Fatalf("%s=1 requires %s; the secret is read only by the host test process", containerE2EGate, containerLLMKeyEnv)
	}
	if baseURL == "" {
		baseURL = defaultContainerLLMURL
	}
	if model == "" {
		model = defaultContainerModel
	}
	endpoint, err := containerChatCompletionsURL(baseURL)
	if err != nil {
		t.Fatalf("invalid %s: %v", containerLLMBaseEnv, err)
	}
	return &containerHumanLLM{
		endpoint: endpoint, apiKey: apiKey, model: model,
		client: &http.Client{Timeout: 90 * time.Second},
	}
}

func preflightContainerHumanLLM(t *testing.T, humanLLM *containerHumanLLM, marker string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	preflight, err := humanLLM.complete(ctx, "Reply with exactly "+marker+" and nothing else.")
	if err != nil {
		t.Fatalf("host human LLM preflight failed: %v", err)
	}
	if markerFromText(preflight) != marker {
		t.Fatalf("host human LLM preflight returned an unexpected answer: %q", safeSnippet(preflight, 256))
	}
}

func (llmClient *containerHumanLLM) complete(ctx context.Context, prompt string) (string, error) {
	requestBody := map[string]any{
		"model": llmClient.model,
		"messages": []map[string]string{
			{
				"role": "system",
				"content": "You are the human operator behind a Human model endpoint. Answer the caller directly and concisely. " +
					"If the request contains a token beginning HUMAN-CONTAINER- and ending -OK, include that exact token in the final answer.",
			},
			{"role": "user", "content": prompt},
		},
		"temperature": 0,
		"max_tokens":  256,
		"stream":      false,
	}
	encoded, err := json.Marshal(requestBody)
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, llmClient.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+llmClient.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := llmClient.client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return "", err
	}
	if len(body) > 1<<20 {
		return "", errors.New("host human LLM response exceeds 1 MiB")
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("host human LLM returned HTTP %d: %s", response.StatusCode, safeSnippet(string(body), 512))
	}
	var decoded struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", fmt.Errorf("decode host human LLM response: %w", err)
	}
	if len(decoded.Choices) == 0 || strings.TrimSpace(decoded.Choices[0].Message.Content) == "" {
		return "", errors.New("host human LLM returned no message content")
	}
	return decoded.Choices[0].Message.Content, nil
}

func containerChatCompletionsURL(baseURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", errors.New("base URL must be an absolute HTTP(S) URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("base URL must not contain credentials, query, or fragment")
	}
	path := strings.TrimRight(parsed.Path, "/")
	switch {
	case strings.HasSuffix(path, "/v1/chat/completions"):
	case strings.HasSuffix(path, "/v1"):
		path += "/chat/completions"
	default:
		path += "/v1/chat/completions"
	}
	parsed.Path = path
	return parsed.String(), nil
}

func fakeContainerLLMHandler(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != "/v1/chat/completions" {
		http.NotFound(response, request)
		return
	}
	var body struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		http.Error(response, "invalid request", http.StatusBadRequest)
		return
	}
	var prompt string
	for _, message := range body.Messages {
		prompt += "\n" + message.Content
	}
	marker := markerFromText(prompt)
	if marker == "" {
		marker = "test-only human answer"
	}
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(map[string]any{
		"choices": []map[string]any{{"message": map[string]string{"role": "assistant", "content": marker}}},
	})
}

func loopbackPort(baseURL string) (int, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return 0, err
	}
	_, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		return 0, fmt.Errorf("parse local model address: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("local model address has an invalid port")
	}
	return port, nil
}

func markerFromText(text string) string {
	return containerMarkerPattern.FindString(text)
}

func openCodeSessionID(output string) string {
	for _, line := range strings.Split(output, "\n") {
		var event struct {
			SessionID string `json:"sessionID"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.SessionID != "" {
			return event.SessionID
		}
	}
	return ""
}

func failingToolCommand() string {
	return "printf " + containerToolError + " >&2; exit 7"
}

func assertToolSuccess(t *testing.T, client, output, marker, workspace string) {
	t.Helper()
	if !strings.Contains(output, marker) {
		t.Fatalf("%s output omitted the Human final after a successful tool call:\n%s",
			client, safeSnippet(output, 16<<10))
	}
	proof, err := os.ReadFile(filepath.Join(workspace, "tool-proof.txt"))
	if err != nil || string(proof) != marker {
		t.Fatalf("%s caller-workspace tool proof = %q, read error %v", client, proof, err)
	}
}

func safeSnippet(text string, limit int) string {
	text = strings.Map(func(character rune) rune {
		if character == '\n' || character == '\r' || character == '\t' || character >= 0x20 {
			return character
		}
		return -1
	}, text)
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "...[truncated]"
}

func TestContainerChatCompletionsURL(t *testing.T) {
	tests := map[string]string{
		"http://127.0.0.1:23333":                       "http://127.0.0.1:23333/v1/chat/completions",
		"https://llm.example.test/proxy/v1":            "https://llm.example.test/proxy/v1/chat/completions",
		"https://llm.example.test/v1/chat/completions": "https://llm.example.test/v1/chat/completions",
	}
	for input, want := range tests {
		got, err := containerChatCompletionsURL(input)
		if err != nil || got != want {
			t.Fatalf("containerChatCompletionsURL(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	for _, input := range []string{"", "file:///tmp/llm", "https://secret@example.test", "https://example.test?token=x"} {
		if _, err := containerChatCompletionsURL(input); err == nil {
			t.Fatalf("containerChatCompletionsURL(%q) accepted an unsafe URL", input)
		}
	}
}

func TestCodexRetryBodiesEquivalent(t *testing.T) {
	base := []byte(`{"model":"human","input":"hello","client_metadata":{"session_id":"s","turn_id":"t","x-codex-turn-metadata":"wire"}}`)
	tests := []struct {
		name string
		body string
		want bool
	}{
		{name: "map order only", body: `{"client_metadata":{"x-codex-turn-metadata":"wire","turn_id":"t","session_id":"s"},"input":"hello","model":"human"}`, want: true},
		{name: "captured retry omission", body: `{"model":"human","input":"hello","client_metadata":{"turn_id":"t","x-codex-turn-metadata":"wire"}}`, want: true},
		{name: "model input drift", body: `{"model":"human","input":"changed","client_metadata":{"turn_id":"t","x-codex-turn-metadata":"wire"}}`},
		{name: "metadata drift", body: `{"model":"human","input":"hello","client_metadata":{"turn_id":"different","x-codex-turn-metadata":"wire"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := codexRetryBodiesEquivalent(base, []byte(test.body)); got != test.want {
				t.Fatalf("codexRetryBodiesEquivalent = %t, want %t", got, test.want)
			}
		})
	}
}
