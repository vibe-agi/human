package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/gateway"
	"github.com/vibe-agi/human/internal/completion/adapter"
)

// TestRealOpenCodeRecoversAcrossNetworkFaultMatrix is the real-client transport
// matrix. A controlled reverse proxy aborts the Workspace request before
// downstream response headers, after the stream-start frame, or after a complete
// Human progress frame. Each point is repeated one to five times according to
// HUMAN_REAL_OPENCODE_NETWORK_DROPS. OpenCode must retry the byte-identical request, the
// gateway must replay the same durable response rather than enqueueing a second
// assignment, and the original Human turn must finish without duplicate text.
//
// The human side is driven exclusively through the local web HTTP API.
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
	drops := 1
	if raw := strings.TrimSpace(os.Getenv("HUMAN_REAL_OPENCODE_NETWORK_DROPS")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 1 || parsed > 5 {
			t.Fatalf("HUMAN_REAL_OPENCODE_NETWORK_DROPS must be an integer from 1 through 5; got %q", raw)
		}
		drops = parsed
	}

	scenarios := []realOpenCodeNetworkScenario{
		{name: "before downstream response headers", point: faultBeforeResponseHeaders, drops: drops},
		{name: "after stream start frame", point: faultAfterStreamStart, drops: drops},
		{name: "after Human progress frame", point: faultAfterHumanProgress, drops: drops},
	}
	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
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
	drops int
}

func runRealOpenCodeNetworkScenario(t *testing.T, binary string, scenario realOpenCodeNetworkScenario) {
	t.Helper()

	root := t.TempDir()
	callerWorkspace := filepath.Join(root, "caller-workspace")
	mirrorRoot := filepath.Join(root, "human-mirrors")
	privateRoot := filepath.Join(root, "private")
	home := filepath.Join(root, "opencode-home")
	configHome := filepath.Join(root, "opencode-config")
	dataHome := filepath.Join(root, "opencode-data")
	stateHome := filepath.Join(root, "opencode-state")
	cacheHome := filepath.Join(root, "opencode-cache")
	temporaryHome := filepath.Join(root, "opencode-tmp")
	for _, directory := range []string{
		callerWorkspace, mirrorRoot, privateRoot, home, configHome, dataHome, stateHome, cacheHome, temporaryHome,
	} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	// OpenCode backs off between transport retries. Keep the normal one-drop gate
	// fast, but give an explicitly requested five-drop probe enough response time
	// to test the client retry ceiling instead of accidentally testing our short
	// fixture deadline.
	runTimeout := 75*time.Second + time.Duration(scenario.drops-1)*30*time.Second
	maxPending := runTimeout - 15*time.Second
	runContext, cancel := context.WithTimeout(context.Background(), runTimeout)
	defer cancel()
	instance, err := Open(runContext, Config{
		Gateway: gateway.Config{
			DatabasePath:      filepath.Join(privateRoot, "gateway.db"),
			HeartbeatInterval: 100 * time.Millisecond,
			MaxPending:        maxPending,
		},
		Worker: WorkerPaths{
			MirrorRoot: mirrorRoot,
			OutboxPath: filepath.Join(privateRoot, "worker-outbox.db"),
		},
		ListenAddress:    "127.0.0.1:0",
		WebListenAddress: "127.0.0.1:0",
		WebStatePath:     filepath.Join(privateRoot, "workerkit-state.db"),
		CallerSubject:    "network-e2e-caller",
		WorkerSubject:    "network-e2e-worker",
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

	// The human side is the web HTTP API: parse the login URL into base and
	// bearer token, then drive the operator through /api/state, /api/accept,
	// /api/reply, and /api/final.
	webURL := instance.WebURL()
	base := webURL[:strings.Index(webURL, "/?token=")]
	token := webURL[strings.Index(webURL, "?token=")+len("?token="):]
	webJSON := func(method, path string, body any) (map[string]any, int) {
		var reader io.Reader
		if body != nil {
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			reader = bytes.NewReader(encoded)
		}
		request, err := http.NewRequestWithContext(runContext, method, base+path, reader)
		if err != nil {
			t.Fatal(err)
		}
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
	webState := func() map[string]any {
		state, status := webJSON(http.MethodGet, "/api/state", nil)
		if status != http.StatusOK {
			t.Fatalf("GET /api/state = %d (%v)", status, state)
		}
		return state
	}
	waitWebState := func(what string, ready func(state map[string]any) bool) map[string]any {
		t.Helper()
		var last map[string]any
		for {
			state, status := webJSON(http.MethodGet, "/api/state", nil)
			if status == http.StatusOK {
				last = state
				if ready(state) {
					return state
				}
			}
			select {
			case <-runContext.Done():
				t.Fatalf("timed out waiting for %s: %v\nlast web state: %v", what, runContext.Err(), last)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}

	command := exec.CommandContext(runContext, binary, "run", "--pure", "--auto", "--format", "json",
		"--model", "human-network-e2e/human", "--dir", callerWorkspace,
		"Let the Human operator drive this turn. Wait for streamed replies and finish only when the Human ends the response.")
	command.Dir = callerWorkspace
	command.Env = append(os.Environ(),
		"HOME="+home,
		"XDG_CONFIG_HOME="+configHome,
		"XDG_DATA_HOME="+dataHome,
		"XDG_STATE_HOME="+stateHome,
		"XDG_CACHE_HOME="+cacheHome,
		"TMPDIR="+temporaryHome,
	)
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
	// turn. Local web mode auto-answers tool-less chat requests with a derived
	// title (auto-title), so auxiliaries never reach the inbox; there is no
	// auxiliary handling step here. The main Workspace request is classified by
	// its declared native bash tool, so it arrives with tool_count > 0 and no
	// auxiliary response can consume the single injected failure.
	var delivery string
	waitWebState("the main OpenCode Workspace assignment in the inbox", func(state map[string]any) bool {
		for _, item := range webInboxItems(state) {
			if count, _ := item["tool_count"].(float64); count > 0 {
				delivery, _ = item["delivery"].(string)
				return delivery != ""
			}
		}
		return false
	})

	// Accept may transiently conflict while a preceding response event is still
	// being committed; retry until the conversation is ours.
	var callerID, taskID string
	for {
		accepted, status := webJSON(http.MethodPost, "/api/accept", map[string]any{"delivery": delivery})
		if status == http.StatusOK {
			key, _ := accepted["key"].(map[string]any)
			callerID = fmt.Sprint(key["caller"])
			taskID = fmt.Sprint(key["task_id"])
			break
		}
		select {
		case <-runContext.Done():
			t.Fatalf("accepting the OpenCode Workspace assignment never succeeded: %v (last accept = %d, %v)",
				runContext.Err(), status, accepted)
		case <-time.After(100 * time.Millisecond):
		}
	}

	if scenario.point != faultAfterHumanProgress {
		faultProxy.waitForDrop(t, runContext)
		faultProxy.waitForReplay(t, runContext)
	}

	if reply, status := webJSON(http.MethodPost, "/api/reply", map[string]any{
		"caller": callerID, "task_id": taskID, "text": progressFragment,
	}); status != http.StatusOK {
		t.Fatalf("POST /api/reply = %d (%v)", status, reply)
	}
	waitWebState("the Human progress reply in the transcript", func(state map[string]any) bool {
		return webTranscriptFragmentCount(state, progressFragment) >= 1
	})
	if scenario.point == faultAfterHumanProgress {
		faultProxy.waitForDrop(t, runContext)
		faultProxy.waitForReplay(t, runContext)
	}

	// A transport retry is response replay only. It must not produce another
	// Inbox item, another conversation, or a duplicate of the sole Human
	// progress entry.
	state := webState()
	if count := webTranscriptFragmentCount(state, progressFragment); count != 1 {
		t.Fatalf("web transcript contained the Human progress fragment %d times; want exactly once\n%v", count, state)
	}
	if items := webInboxItems(state); len(items) != 0 {
		t.Fatalf("transport retry enqueued a duplicate assignment\n%v", items)
	}
	if conversations := webConversations(state); len(conversations) != 1 {
		t.Fatalf("transport retry produced %d conversations; want exactly one\n%v", len(conversations), state)
	}

	finalFragment := "network retry completed " + scenarioID + " in the original Human turn"
	if final, status := webJSON(http.MethodPost, "/api/final", map[string]any{
		"caller": callerID, "task_id": taskID, "text": finalFragment,
	}); status != http.StatusOK {
		t.Fatalf("POST /api/final = %d (%v)", status, final)
	}

	var result commandResult
	select {
	case result = <-commandDone:
	case <-runContext.Done():
		lastState, _ := webJSON(http.MethodGet, "/api/state", nil)
		t.Fatalf("real OpenCode network recovery timed out: %v\nweb state: %v", runContext.Err(), lastState)
	}
	if result.err != nil {
		t.Fatalf("real OpenCode network recovery failed: %v\n%s", result.err, result.output)
	}
	faultProxy.assertExactReplay(t)
	output := string(result.output)
	for _, marker := range []string{progressFragment, finalFragment} {
		if count := strings.Count(output, marker); count != 1 {
			t.Fatalf("OpenCode output contained %q %d times; want exactly once:\n%s", marker, count, output)
		}
	}
}

// webInboxItems extracts the inbox rows from a decoded /api/state payload.
func webInboxItems(state map[string]any) []map[string]any {
	raw, _ := state["inbox"].([]any)
	items := make([]map[string]any, 0, len(raw))
	for _, entry := range raw {
		if item, ok := entry.(map[string]any); ok {
			items = append(items, item)
		}
	}
	return items
}

// webConversations extracts the conversation rows from a decoded /api/state
// payload.
func webConversations(state map[string]any) []map[string]any {
	raw, _ := state["conversations"].([]any)
	conversations := make([]map[string]any, 0, len(raw))
	for _, entry := range raw {
		if conversation, ok := entry.(map[string]any); ok {
			conversations = append(conversations, conversation)
		}
	}
	return conversations
}

// webTranscriptFragmentCount counts occurrences of fragment across every
// transcript entry of every conversation in a decoded /api/state payload.
func webTranscriptFragmentCount(state map[string]any, fragment string) int {
	count := 0
	for _, conversation := range webConversations(state) {
		transcript, _ := conversation["transcript"].([]any)
		for _, entry := range transcript {
			entryMap, _ := entry.(map[string]any)
			text, _ := entryMap["text"].(string)
			count += strings.Count(text, fragment)
		}
	}
	return count
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
	point      realOpenCodeNetworkFaultPoint
	marker     []byte
	dropTarget int

	mu             sync.Mutex
	attempts       []*faultProxyAttempt
	dropped        []*faultProxyAttempt
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
		dropTarget:     scenario.drops,
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
	if len(state.dropped) == state.dropTarget && state.replayed == nil &&
		attempt.digest == state.dropped[0].digest {
		state.replayed = attempt
		close(state.replayObserved)
	}
	state.mu.Unlock()
}

func (state *openCodeNetworkFaultState) markDropped(attempt *faultProxyAttempt) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.dropped) >= state.dropTarget {
		return false
	}
	for _, dropped := range state.dropped {
		if dropped == attempt {
			return false
		}
	}
	state.dropped = append(state.dropped, attempt)
	if len(state.dropped) == state.dropTarget {
		close(state.dropObserved)
	}
	return true
}

func (state *openCodeNetworkFaultState) injectionAvailable() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	return len(state.dropped) < state.dropTarget
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
	if len(state.dropped) != state.dropTarget || state.replayed == nil {
		t.Fatalf("fault proxy observed %d/%d dropped responses and replay=%t", len(state.dropped), state.dropTarget, state.replayed != nil)
	}
	first := state.dropped[0]
	for index, dropped := range state.dropped {
		if dropped.digest != first.digest || dropped.digest != state.replayed.digest {
			t.Fatalf("OpenCode retry body differed at dropped attempt %d", index+1)
		}
		if dropped.sessionID == "" || state.replayed.sessionID == "" {
			t.Fatal("OpenCode omitted X-Session-Id on a dropped request or replay")
		}
		if dropped.sessionID != first.sessionID || dropped.sessionID != state.replayed.sessionID {
			t.Fatalf("OpenCode retry changed X-Session-Id at dropped attempt %d", index+1)
		}
		if dropped.idempotencyKey == "" || state.replayed.idempotencyKey == "" {
			t.Fatal("gateway omitted the durable idempotency key on a drop or replay")
		}
		if dropped.idempotencyKey != first.idempotencyKey || dropped.idempotencyKey != state.replayed.idempotencyKey {
			t.Fatalf("gateway replay changed idempotency key at dropped attempt %d", index+1)
		}
		if dropped.statusCode != http.StatusOK || state.replayed.statusCode != http.StatusOK {
			t.Fatalf("drop/replay statuses at attempt %d = %d/%d; want 200/200", index+1, dropped.statusCode, state.replayed.statusCode)
		}
	}
	matchingAttempts := 0
	for _, attempt := range state.attempts {
		if attempt.digest == first.digest {
			matchingAttempts++
		}
	}
	wantAttempts := state.dropTarget + 1
	if matchingAttempts != wantAttempts {
		t.Fatalf("OpenCode sent the dropped semantic request %d times; want %d drops plus one successful replay", matchingAttempts, state.dropTarget)
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
