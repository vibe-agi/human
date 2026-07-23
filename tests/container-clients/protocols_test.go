package containerclients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	humanlocal "github.com/vibe-agi/human/local"
)

// TestContainerProtocolsViaLLMWebHuman is the direct-protocol product door.
// Unlike the CLI door, the caller is deliberately just the model API wire: all
// three advertised dialects are sent straight to Local in aggregate and stream
// modes. The human side is still the real Playwright DOM backed by the configured
// host LLM; no test helper accepts or completes an assignment through JSON APIs.
func TestContainerProtocolsViaLLMWebHuman(t *testing.T) {
	if os.Getenv(containerE2EGate) != "1" {
		t.Skip("set HUMAN_TESTCONTAINERS_E2E=1 to run the direct-protocol container door")
	}

	humanLLM := newContainerHumanLLM(t)
	preflightContainerHumanLLM(t, humanLLM, "HUMAN-CONTAINER-PROTOCOL-PREFLIGHT-OK")

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize protocol test root: %v", err)
	}
	privateRoot := filepath.Join(root, "private")
	workspace := filepath.Join(root, "workspace")
	humanWorkspaceRoot := filepath.Join(root, "human-workspaces")
	for _, directory := range []string{privateRoot, workspace, humanWorkspaceRoot} {
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
			MaxPending:         2 * time.Minute,
		},
		ListenAddress:       "127.0.0.1:0",
		WebListenAddress:    "127.0.0.1:0",
		WebStatePath:        filepath.Join(privateRoot, "workerkit-state.db"),
		WebDisableAutoTitle: true,
		ShutdownTimeout:     10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := instance.Close(); err != nil {
			t.Errorf("close direct-protocol Local instance: %v", err)
		}
	})

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
	browserContainer, err := startBrowserHumanContainer(t.Context(), webPort, relayPort, humanWorkspaceRoot)
	if err != nil {
		t.Fatalf("start direct-protocol browser human: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := browserContainer.Terminate(ctx); err != nil {
			t.Errorf("terminate direct-protocol browser human: %v", err)
		}
	})
	assertBrowserVersion(t, browserContainer)
	browserConfig := containerBrowserConfig{
		webURL:     fmt.Sprintf("http://host.testcontainers.internal:%d", webPort),
		webToken:   webAPI.token,
		replyURL:   fmt.Sprintf("http://host.testcontainers.internal:%d/reply", relayPort),
		replyToken: replyRelay.token,
	}

	protocols := []directProtocol{
		{name: "anthropic-messages", path: "/v1/messages", anthropic: true},
		{name: "openai-chat-completions", path: "/v1/chat/completions"},
		{name: "openai-responses", path: "/v1/responses", responses: true},
	}
	for _, protocol := range protocols {
		protocol := protocol
		for _, stream := range []bool{false, true} {
			stream := stream
			mode := "aggregate"
			if stream {
				mode = "stream"
			}
			t.Run(protocol.name+"/"+mode, func(t *testing.T) {
				marker := "HUMAN-CONTAINER-PROTOCOL-" +
					strings.ToUpper(protocol.name) + "-" +
					strings.ToUpper(mode) + "-OK"
				output := runDirectProtocolDialogue(t, browserContainer, browserConfig,
					containerDialogueScenario{marker: marker}, instance.CallerToken(), func(ctx context.Context) (string, error) {
						return protocol.call(ctx, instance.BaseURL(), instance.CallerToken(), marker, stream)
					})
				if !strings.Contains(output, marker) {
					t.Fatalf("%s %s response omitted the LLM-human marker:\n%s",
						protocol.name, mode, safeSnippet(output, 16<<10))
				}
			})
		}
	}
}

type directProtocol struct {
	name      string
	path      string
	anthropic bool
	responses bool
}

func (protocol directProtocol) call(
	ctx context.Context,
	baseURL, callerToken, marker string,
	stream bool,
) (string, error) {
	prompt := "Answer through the Human Web operator and include exactly " + marker + "."
	var body string
	switch {
	case protocol.anthropic:
		body = fmt.Sprintf(`{"model":"human-expert","max_tokens":256,"stream":%t,"messages":[{"role":"user","content":%q}]}`,
			stream, prompt)
	case protocol.responses:
		body = fmt.Sprintf(`{"model":"human-expert","stream":%t,"input":%q}`, stream, prompt)
	default:
		body = fmt.Sprintf(`{"model":"human-expert","stream":%t,"messages":[{"role":"user","content":%q}]}`,
			stream, prompt)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+protocol.path, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "protocol-"+strings.ToLower(marker))
	if protocol.anthropic {
		request.Header.Set("X-Api-Key", callerToken)
		request.Header.Set("Anthropic-Version", "2023-06-01")
	} else {
		request.Header.Set("Authorization", "Bearer "+callerToken)
	}
	response, err := (&http.Client{Timeout: 90 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
	if err != nil {
		return "", err
	}
	if len(payload) > 8<<20 {
		return string(payload[:8<<20]), errors.New("direct protocol response exceeds 8 MiB")
	}
	if response.StatusCode != http.StatusOK {
		return string(payload), fmt.Errorf("direct protocol returned HTTP %d", response.StatusCode)
	}
	return string(payload), nil
}

func runDirectProtocolDialogue(
	t *testing.T,
	browserContainer testcontainers.Container,
	browserConfig containerBrowserConfig,
	scenario containerDialogueScenario,
	callerToken string,
	caller func(context.Context) (string, error),
) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	results := make(chan containerExecResult, 2)
	go func() {
		output, err := caller(ctx)
		results <- containerExecResult{role: "protocol caller", output: output, err: err}
	}()
	go func() {
		output, err := runBrowserHuman(ctx, browserContainer, browserConfig, scenario)
		results <- containerExecResult{role: "browser human", output: output, err: err}
	}()

	completed := make(map[string]containerExecResult, 2)
	for len(completed) != 2 {
		select {
		case result := <-results:
			completed[result.role] = result
			if result.err != nil {
				cancel()
			}
		case <-ctx.Done():
			t.Fatalf("direct protocol dialogue timed out: %v", ctx.Err())
		}
	}
	redactions := []string{callerToken, browserConfig.webToken, browserConfig.replyToken}
	for _, role := range []string{"browser human", "protocol caller"} {
		result := completed[role]
		if result.err != nil {
			t.Fatalf("%s failed: %v\n%s", role, result.err,
				safeSnippet(redactContainerOutput(result.output, redactions...), 16<<10))
		}
	}
	browserOutput := completed["browser human"].output
	if !strings.Contains(browserOutput, "browser-human-ok:final:"+scenario.marker) {
		t.Fatalf("browser human did not complete direct protocol call:\n%s",
			safeSnippet(redactContainerOutput(browserOutput, redactions...), 16<<10))
	}
	return redactContainerOutput(completed["protocol caller"].output, redactions...)
}
