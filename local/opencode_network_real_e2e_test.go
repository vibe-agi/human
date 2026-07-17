package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/gateway"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/worker"
)

// TestRealOpenCodeRecoversAcrossNetworkFaultMatrix is the real-client transport
// matrix. A controlled reverse proxy aborts the first Workspace request before
// downstream response headers, after the stream-start frame, or after a complete
// Human progress frame. OpenCode must retry the byte-identical request, the
// gateway must replay the same durable response rather than enqueueing a second
// assignment, and the original Human turn must finish without duplicate text.
//
// It is opt-in because it requires the exact captured OpenCode binary:
//
//	HUMAN_REAL_OPENCODE_NETWORK_E2E=1 go test ./local \
//	  -run TestRealOpenCodeRecoversAcrossNetworkFaultMatrix -count=1 -v
func TestRealOpenCodeRecoversAcrossNetworkFaultMatrix(t *testing.T) {
	if os.Getenv("HUMAN_REAL_OPENCODE_NETWORK_E2E") != "1" {
		t.Skip("set HUMAN_REAL_OPENCODE_NETWORK_E2E=1 to run the installed OpenCode CLI through a fault proxy")
	}
	binary, err := exec.LookPath("opencode")
	if err != nil {
		t.Skip("opencode is not installed")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != adapter.OpenCodeVersion {
		t.Skipf("requires opencode %s; got %q (%v)", adapter.OpenCodeVersion, strings.TrimSpace(string(version)), err)
	}

	scenarios := []realOpenCodeNetworkScenario{
		{name: "before downstream response headers", point: faultBeforeResponseHeaders},
		{name: "after stream start frame", point: faultAfterStreamStart},
		{name: "after Human progress frame", point: faultAfterHumanProgress},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			runRealOpenCodeNetworkScenario(t, binary, scenario)
		})
	}
}

type realOpenCodeNetworkFaultPoint int

const (
	faultBeforeResponseHeaders realOpenCodeNetworkFaultPoint = iota
	faultAfterStreamStart
	faultAfterHumanProgress
)

type realOpenCodeNetworkScenario struct {
	name  string
	point realOpenCodeNetworkFaultPoint
}

func runRealOpenCodeNetworkScenario(t *testing.T, binary string, scenario realOpenCodeNetworkScenario) {
	t.Helper()

	root := t.TempDir()
	callerWorkspace := filepath.Join(root, "caller-workspace")
	mirrorRoot := filepath.Join(root, "human-mirrors")
	privateRoot := filepath.Join(root, "private")
	configHome := filepath.Join(root, "opencode-config")
	dataHome := filepath.Join(root, "opencode-data")
	for _, directory := range []string{callerWorkspace, mirrorRoot, privateRoot, configHome, dataHome} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	runContext, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	instance, err := Open(runContext, Config{
		Gateway: gateway.Config{
			DatabasePath:      filepath.Join(privateRoot, "gateway.db"),
			HeartbeatInterval: 100 * time.Millisecond,
			MaxPending:        60 * time.Second,
		},
		Worker: worker.Config{
			MirrorRoot: mirrorRoot,
			OutboxPath: filepath.Join(privateRoot, "worker-outbox.db"),
			StatePath:  filepath.Join(privateRoot, "worker-state.db"),
		},
		ListenAddress: "127.0.0.1:0",
		CallerSubject: "network-e2e-caller",
		WorkerSubject: "network-e2e-worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := instance.Close(); err != nil {
			t.Errorf("close local instance: %v", err)
		}
	})

	scenarioID := strings.ReplaceAll(scenario.name, " ", "-")
	progressFragment := "Human progress survived " + scenarioID + " exactly once"
	faultProxy := newOpenCodeNetworkFaultProxy(t, instance.BaseURL(), scenario, progressFragment)
	defer faultProxy.Close()
	writeRealOpenCodeNetworkConfig(t, callerWorkspace, faultProxy.URL, instance.CallerToken())

	inputReader, inputWriter := io.Pipe()
	defer inputWriter.Close()
	probe := newTUIViewProbe()
	program := tea.NewProgram(
		tuiObservedModel{inner: instance.Model(), probe: probe},
		tea.WithContext(runContext),
		tea.WithInput(inputReader),
		tea.WithOutput(io.Discard),
		tea.WithoutRenderer(),
		tea.WithoutSignals(),
	)
	programDone := make(chan error, 1)
	go func() {
		_, runErr := program.Run()
		programDone <- runErr
	}()
	program.Send(tea.WindowSizeMsg{Width: 160, Height: 48})
	t.Cleanup(func() {
		program.Quit()
		select {
		case err := <-programDone:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, tea.ErrProgramKilled) {
				t.Errorf("stop TUI program: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("TUI program did not stop")
		}
	})

	command := exec.CommandContext(runContext, binary, "run", "--pure", "--auto", "--format", "json",
		"--model", "human-network-e2e/human", "--dir", callerWorkspace,
		"Let the Human operator drive this turn. Wait for streamed replies and finish only when the Human ends the response.")
	command.Dir = callerWorkspace
	command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+configHome, "XDG_DATA_HOME="+dataHome)
	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		output, runErr := command.CombinedOutput()
		commandDone <- commandResult{output: output, err: runErr}
	}()

	// OpenCode may issue a finite auxiliary Chat request beside its Workspace
	// turn. Finish auxiliaries normally. The proxy classifies the main request by
	// its declared native bash tool, so no auxiliary response can consume the
	// single injected failure.
	for attempts := 0; ; attempts++ {
		if attempts >= 4 {
			t.Fatalf("main OpenCode Workspace request never became active\n%s", probe.view())
		}
		probe.waitContains(t, runContext, "INBOX")
		acceptRealOpenCodeRequest(t, runContext, probe, inputWriter)
		if strings.Contains(probe.view(), "bash · runs on client Agent") {
			break
		}
		writeTUIInput(t, inputWriter, "OpenCode auxiliary request complete")
		probe.waitContains(t, runContext, "OpenCode auxiliary request complete")
		_, beforeAuxiliaryFinal := probe.snapshot()
		writeTUIInput(t, inputWriter, "\x04")
		probe.waitAfterAny(t, runContext, beforeAuxiliaryFinal, "INBOX", "IDLE")
	}

	if scenario.point != faultAfterHumanProgress {
		faultProxy.waitForDrop(t, runContext)
		faultProxy.waitForReplay(t, runContext)
	}

	writeTUIInput(t, inputWriter, progressFragment)
	probe.waitContains(t, runContext, progressFragment)
	_, beforeFirstReply := probe.snapshot()
	writeTUIInput(t, inputWriter, "\r")
	probe.waitAfterContains(t, runContext, beforeFirstReply, "stream open")
	if scenario.point == faultAfterHumanProgress {
		faultProxy.waitForDrop(t, runContext)
		faultProxy.waitForReplay(t, runContext)
	}

	// A transport retry is response replay only. It must not produce another
	// Inbox item or move the sole active Human turn out of its streaming state.
	if count := strings.Count(probe.view(), progressFragment); count != 1 {
		t.Fatalf("TUI displayed the Human progress fragment %d times; want exactly once\n%s", count, probe.view())
	}
	if strings.Contains(probe.view(), "INBOX 1") || strings.Contains(probe.view(), "Inbox 1/1") {
		t.Fatalf("transport retry enqueued a duplicate assignment\n%s", probe.view())
	}

	finalFragment := "network retry completed " + scenarioID + " in the original Human turn"
	writeTUIInput(t, inputWriter, finalFragment)
	probe.waitContains(t, runContext, finalFragment)
	writeTUIInput(t, inputWriter, "\x04")

	var result commandResult
	select {
	case result = <-commandDone:
	case <-runContext.Done():
		t.Fatalf("real OpenCode network recovery timed out: %v\n%s", runContext.Err(), probe.view())
	}
	if result.err != nil {
		t.Fatalf("real OpenCode network recovery failed: %v\n%s\nTUI:\n%s", result.err, result.output, probe.view())
	}
	faultProxy.assertExactReplay(t)
	output := string(result.output)
	for _, marker := range []string{progressFragment, finalFragment} {
		if count := strings.Count(output, marker); count != 1 {
			t.Fatalf("OpenCode output contained %q %d times; want exactly once:\n%s", marker, count, output)
		}
	}
}

func acceptRealOpenCodeRequest(
	t *testing.T,
	ctx context.Context,
	probe *tuiViewProbe,
	input io.Writer,
) {
	t.Helper()
	for retries := 0; retries < 5; retries++ {
		_, beforeAccept := probe.snapshot()
		writeTUIInput(t, input, "a")
		probe.waitAfter(t, ctx, beforeAccept, func(content string) bool {
			return strings.Contains(content, "HUMAN TURN") ||
				strings.Contains(content, "another response event is still being committed")
		}, "HUMAN TURN or completion of the preceding response event")
		if strings.Contains(probe.view(), "HUMAN TURN") {
			return
		}
		_, blockedRevision := probe.snapshot()
		probe.waitAfter(t, ctx, blockedRevision, func(content string) bool {
			return strings.Contains(content, "INBOX") &&
				!strings.Contains(content, "another response event is still being committed")
		}, "the preceding response event to finish committing")
	}
	t.Fatalf("OpenCode request remained blocked by a preceding response event\n%s", probe.view())
}

func writeRealOpenCodeNetworkConfig(t *testing.T, workspace, baseURL, callerToken string) {
	t.Helper()
	config := map[string]any{
		"$schema": "https://opencode.ai/config.json",
		"provider": map[string]any{"human-network-e2e": map[string]any{
			"npm": "@ai-sdk/openai-compatible", "name": "Human Network E2E",
			"options": map[string]any{
				"baseURL": baseURL + "/v1", "apiKey": callerToken,
				"headers": map[string]string{
					"X-Human-Capability-Tier": "workspace",
					"X-Human-Workspace-Key":   "opencode-network-e2e",
					"X-Human-Harness-Id":      adapter.OpenCodeID,
					"X-Human-Harness-Version": adapter.OpenCodeVersion,
					"X-Human-Workspace-Root":  workspace,
					"X-Human-Allow-Exec":      "true",
				},
			},
			"models": map[string]any{"human": map[string]any{
				"name": "Human", "limit": map[string]int{"context": 100000, "output": 4096},
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
}

var errInjectedPartialSSEDrop = errors.New("injected partial SSE connection drop")

type faultProxyAttemptContextKey struct{}

type faultProxyAttempt struct {
	digest         [sha256.Size]byte
	sessionID      string
	mainWorkspace  bool
	idempotencyKey string
	statusCode     int
}

type openCodeNetworkFaultState struct {
	point  realOpenCodeNetworkFaultPoint
	marker []byte

	mu             sync.Mutex
	attempts       []*faultProxyAttempt
	dropped        *faultProxyAttempt
	replayed       *faultProxyAttempt
	dropObserved   chan struct{}
	replayObserved chan struct{}
}

type openCodeNetworkFaultProxy struct {
	*httptest.Server
	state *openCodeNetworkFaultState
}

func newOpenCodeNetworkFaultProxy(
	t *testing.T,
	upstream string,
	scenario realOpenCodeNetworkScenario,
	progressMarker string,
) *openCodeNetworkFaultProxy {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	marker := []byte(progressMarker)
	if scenario.point == faultAfterStreamStart {
		marker = []byte(`"role":"assistant"`)
	}
	state := &openCodeNetworkFaultState{
		point:          scenario.point,
		marker:         marker,
		dropObserved:   make(chan struct{}),
		replayObserved: make(chan struct{}),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.FlushInterval = -1
	proxy.ErrorLog = log.New(io.Discard, "", 0)
	proxy.ModifyResponse = func(response *http.Response) error {
		attempt, _ := response.Request.Context().Value(faultProxyAttemptContextKey{}).(*faultProxyAttempt)
		if attempt == nil {
			return nil
		}
		state.recordResponse(attempt, response.StatusCode, response.Header.Get("Idempotency-Key"))
		if attempt.mainWorkspace && state.point != faultBeforeResponseHeaders &&
			response.StatusCode == http.StatusOK &&
			strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") && state.injectionAvailable() {
			response.Body = &abortAfterMarkerReadCloser{ReadCloser: response.Body, attempt: attempt, state: state}
		}
		return nil
	}
	handler := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost && request.URL.Path == "/v1/chat/completions" {
			body, readErr := io.ReadAll(request.Body)
			if readErr != nil {
				http.Error(response, "fault proxy could not read request", http.StatusBadGateway)
				return
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			request.ContentLength = int64(len(body))
			attempt := state.recordAttempt(
				sha256.Sum256(body), request.Header.Get("X-Session-Id"), isMainOpenCodeWorkspaceRequest(body),
			)
			request = request.WithContext(context.WithValue(request.Context(), faultProxyAttemptContextKey{}, attempt))
			if attempt.mainWorkspace && state.point == faultBeforeResponseHeaders && state.injectionAvailable() {
				dropBeforeDownstreamHeaders(response, request, proxy, state, attempt)
				return
			}
		}
		proxy.ServeHTTP(response, request)
	})
	server := httptest.NewServer(handler)
	return &openCodeNetworkFaultProxy{Server: server, state: state}
}

func isMainOpenCodeWorkspaceRequest(body []byte) bool {
	var request struct {
		Tools []struct {
			Name     string `json:"name"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return false
	}
	for _, tool := range request.Tools {
		if tool.Name == "bash" || tool.Function.Name == "bash" {
			return true
		}
	}
	return false
}

func dropBeforeDownstreamHeaders(
	response http.ResponseWriter,
	request *http.Request,
	proxy *httputil.ReverseProxy,
	state *openCodeNetworkFaultState,
	attempt *faultProxyAttempt,
) {
	upstreamRequest := request.Clone(request.Context())
	proxy.Director(upstreamRequest)
	upstreamRequest.RequestURI = ""
	transport := proxy.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	upstreamResponse, err := transport.RoundTrip(upstreamRequest)
	if err != nil {
		http.Error(response, "fault proxy could not reach upstream", http.StatusBadGateway)
		return
	}
	state.recordResponse(attempt, upstreamResponse.StatusCode, upstreamResponse.Header.Get("Idempotency-Key"))
	hijacker, ok := response.(http.Hijacker)
	if !ok {
		_ = upstreamResponse.Body.Close()
		http.Error(response, "fault proxy cannot close downstream connection", http.StatusInternalServerError)
		return
	}
	connection, _, err := hijacker.Hijack()
	if err != nil {
		_ = upstreamResponse.Body.Close()
		http.Error(response, "fault proxy could not close downstream connection", http.StatusInternalServerError)
		return
	}
	state.markDropped(attempt)
	_ = connection.Close()
	_ = upstreamResponse.Body.Close()
}

func (state *openCodeNetworkFaultState) recordAttempt(
	digest [sha256.Size]byte,
	sessionID string,
	mainWorkspace bool,
) *faultProxyAttempt {
	attempt := &faultProxyAttempt{digest: digest, sessionID: sessionID, mainWorkspace: mainWorkspace}
	state.mu.Lock()
	state.attempts = append(state.attempts, attempt)
	state.mu.Unlock()
	return attempt
}

func (state *openCodeNetworkFaultState) recordResponse(attempt *faultProxyAttempt, status int, idempotencyKey string) {
	state.mu.Lock()
	attempt.statusCode = status
	attempt.idempotencyKey = idempotencyKey
	if state.dropped != nil && state.replayed == nil && attempt != state.dropped && attempt.digest == state.dropped.digest {
		state.replayed = attempt
		close(state.replayObserved)
	}
	state.mu.Unlock()
}

func (state *openCodeNetworkFaultState) markDropped(attempt *faultProxyAttempt) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped != nil {
		return false
	}
	state.dropped = attempt
	close(state.dropObserved)
	return true
}

func (state *openCodeNetworkFaultState) injectionAvailable() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.dropped == nil
}

func (proxy *openCodeNetworkFaultProxy) waitForDrop(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-proxy.state.dropObserved:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for injected network drop: %v", ctx.Err())
	}
}

func (proxy *openCodeNetworkFaultProxy) waitForReplay(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-proxy.state.replayObserved:
	case <-ctx.Done():
		t.Fatalf("OpenCode did not retry the dropped model request: %v", ctx.Err())
	}
}

func (proxy *openCodeNetworkFaultProxy) assertExactReplay(t *testing.T) {
	t.Helper()
	state := proxy.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped == nil || state.replayed == nil {
		t.Fatal("fault proxy did not observe both the dropped response and its replay")
	}
	if state.dropped.digest != state.replayed.digest {
		t.Fatal("OpenCode retry body differed from the dropped request")
	}
	if state.dropped.sessionID == "" || state.replayed.sessionID == "" {
		t.Fatal("OpenCode omitted X-Session-Id on the dropped request or replay")
	}
	if state.dropped.sessionID != state.replayed.sessionID {
		t.Fatalf("OpenCode retry changed X-Session-Id: %q != %q", state.dropped.sessionID, state.replayed.sessionID)
	}
	if state.dropped.idempotencyKey == "" || state.replayed.idempotencyKey == "" {
		t.Fatal("gateway omitted the durable idempotency key on drop or replay")
	}
	if state.dropped.idempotencyKey != state.replayed.idempotencyKey {
		t.Fatalf("gateway replay changed idempotency key: %q != %q", state.dropped.idempotencyKey, state.replayed.idempotencyKey)
	}
	if state.dropped.statusCode != http.StatusOK || state.replayed.statusCode != http.StatusOK {
		t.Fatalf("drop/replay statuses = %d/%d; want 200/200", state.dropped.statusCode, state.replayed.statusCode)
	}
	matchingAttempts := 0
	for _, attempt := range state.attempts {
		if attempt.digest == state.dropped.digest {
			matchingAttempts++
		}
	}
	if matchingAttempts != 2 {
		t.Fatalf("OpenCode sent the dropped semantic request %d times; want initial request plus one replay", matchingAttempts)
	}
}

type abortAfterMarkerReadCloser struct {
	io.ReadCloser
	attempt    *faultProxyAttempt
	state      *openCodeNetworkFaultState
	tail       []byte
	seenMarker bool
	abort      bool
}

func (reader *abortAfterMarkerReadCloser) Read(buffer []byte) (int, error) {
	if reader.abort {
		if reader.state.markDropped(reader.attempt) {
			return 0, errInjectedPartialSSEDrop
		}
		reader.abort = false
	}
	n, err := reader.ReadCloser.Read(buffer)
	if n == 0 {
		return n, err
	}
	combined := append(append([]byte(nil), reader.tail...), buffer[:n]...)
	if !reader.seenMarker {
		markerAt := bytes.Index(combined, reader.state.marker)
		if markerAt < 0 {
			keep := len(reader.state.marker) - 1
			if keep > len(combined) {
				keep = len(combined)
			}
			reader.tail = append(reader.tail[:0], combined[len(combined)-keep:]...)
			return n, nil
		}
		reader.seenMarker = true
		combined = combined[markerAt+len(reader.state.marker):]
	}
	if bytes.Contains(combined, []byte("\n\n")) {
		reader.abort = true
	}
	// One trailing newline is sufficient to recognize a frame boundary split
	// across two upstream reads after the marker.
	keep := 1
	if keep > len(combined) {
		keep = len(combined)
	}
	reader.tail = append(reader.tail[:0], combined[len(combined)-keep:]...)
	return n, nil
}
