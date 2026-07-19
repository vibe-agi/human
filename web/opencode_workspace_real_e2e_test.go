package web_test

// The M4 workspace web door: real OpenCode 1.17.18 in WORKSPACE tier against
// the embedded public kernel, with the human side operated exclusively through
// the web API and the Live Workspace mirror:
//
//	accept → progress → :pull equivalent (bash + base64 exact bytes) →
//	seed mirror → human edit → review → deliver as native write →
//	result continuation → bash + todowrite batch → final
//
//	HUMAN_REAL_OPENCODE_E2E=1 go test ./web -run TestRealOpenCodeWorkspaceWebDoor -count=1

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/builtin"
	"github.com/vibe-agi/human/llm/callerhttp"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
	"github.com/vibe-agi/human/workerkit/fsmirror"
)

var _ = fsnotify.Create // keep the mirror's watcher dependency explicit here

func TestRealOpenCodeWorkspaceWebDoor(t *testing.T) {
	if os.Getenv("HUMAN_REAL_OPENCODE_E2E") != "1" {
		t.Skip("set HUMAN_REAL_OPENCODE_E2E=1 to run the installed OpenCode CLI")
	}
	binary, err := exec.LookPath("opencode")
	if err != nil {
		t.Skip("opencode is not installed")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != openCodeVersion {
		t.Skipf("requires opencode %s; got %q (%v)", openCodeVersion, strings.TrimSpace(string(version)), err)
	}

	workspace := t.TempDir()
	const before = "version from the client Agent\n"
	const editedSuffix = "edited via the web door\n"
	if err := os.WriteFile(filepath.Join(workspace, "native.txt"), []byte(before), 0o600); err != nil {
		t.Fatal(err)
	}

	// --- Embedded kernel with a workspace-aware host resolver: tool-declaring
	// requests join the workspace task scope keyed by OpenCode's X-Session-Id;
	// tool-less auxiliary requests (title/summary) are isolated as chat.
	store, releaseStore := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = releaseStore(context.Background()) })
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "workspace-web-door",
		Store:        framework.Borrow[llm.Store](store),
		Codecs:       builtin.Registrations(),
		Admission:    llm.AdmitAll(),
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
		ToolAuthorizer: llm.ToolAuthorizerFunc(func(context.Context, llm.ToolAuthorization) error {
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = service.Shutdown(ctx)
	})
	resolver := callerhttp.ResolveFunc(func(_ context.Context, request callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
		digest := sha256.Sum256(append([]byte(request.Header.Get("X-Session-Id")+"\x00"), request.Body...))
		resolution := callerhttp.Resolution{
			IdempotencyKey: llm.IdempotencyKey("oc-" + hex.EncodeToString(digest[:16])),
			Task:           llm.TaskContext{CapabilityTier: llm.TierChat},
		}
		var probe struct {
			Tools []json.RawMessage `json:"tools"`
		}
		session := request.Header.Get("X-Session-Id")
		if json.Unmarshal(request.Body, &probe) == nil && len(probe.Tools) > 0 && session != "" {
			resolution.Task = llm.TaskContext{
				CapabilityTier: llm.TierWorkspace, WorkspaceKey: "workspace-web-door",
				HarnessID: "opencode", HarnessVersion: "1.17.18",
				HarnessSessionID: session, WorkspaceRoot: workspace, ExecAllowed: true,
			}
		}
		return resolution, nil
	})
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: callerhttp.AuthenticateFunc(func(context.Context, *http.Request) (callerhttp.Identity, error) {
			return callerhttp.Identity{CallerID: "caller-opencode"}, nil
		}),
		Resolver: resolver,
		Routes:   callerhttp.BuiltinRoutes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), service)
	if err != nil {
		t.Fatal(err)
	}
	callerServer := httptest.NewServer(transport)
	t.Cleanup(func() {
		callerServer.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtime.Shutdown(ctx)
	})

	// --- Human side: workerkit + filesystem mirror + web, all over public API.
	mirrorRoot := t.TempDir()
	mirror, err := fsmirror.Open(t.Context(), fsmirror.Config{
		Root: mirrorRoot, Debounce: 100 * time.Millisecond,
		BaselineFile: filepath.Join(t.TempDir(), "baseline.json"),
		Build: fsmirror.OpenCodeWriteBuilder(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mirror.Close() })
	connection, err := service.OpenWorker(t.Context(), llm.AuthenticatedWorker{
		WorkerID: "worker-a", SessionID: "session-workspace-door",
	})
	if err != nil {
		t.Fatal(err)
	}
	stateStore, _ := workerkit.NewMemoryStateStore()
	worker, err := workerkit.Open(t.Context(), workerkit.Config{
		Wire: workerkit.WrapConnection(connection), State: stateStore, Mirror: mirror,
	})
	if err != nil {
		t.Fatal(err)
	}
	webServer, err := web.New(web.Config{Worker: worker, SessionToken: testToken})
	if err != nil {
		t.Fatal(err)
	}
	webListener := httptest.NewServer(webServer)
	t.Cleanup(func() {
		webListener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = webServer.Shutdown(ctx)
		_ = worker.Shutdown(ctx)
	})

	// --- The scripted human operator, entirely through the web HTTP API.
	const finalAnswer = "WORKSPACE-WEB-DOOR-FINAL: native loop complete"
	operatorCtx, stopOperator := context.WithCancel(context.Background())
	defer stopOperator()
	operatorErr := make(chan error, 1)
	go func() {
		operatorErr <- runWorkspaceOperator(operatorCtx, webListener.URL, mirrorRoot, workspace, editedSuffix, finalAnswer)
	}()

	// --- Real OpenCode, workspace tier, one turn covering the whole loop.
	configHome := filepath.Join(t.TempDir(), "config")
	dataHome := filepath.Join(t.TempDir(), "data")
	for _, directory := range []string{configHome, dataHome} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	config := map[string]any{
		"$schema": "https://opencode.ai/config.json",
		"provider": map[string]any{"human-web": map[string]any{
			"npm": "@ai-sdk/openai-compatible", "name": "Human Web Door",
			"options": map[string]any{"baseURL": callerServer.URL + "/v1", "apiKey": "hae_test"},
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
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "run", "--pure", "--auto", "--format", "json",
		"--model", "human-web/human", "--dir", workspace,
		"Use the Human response and complete the requested native workspace tool loop.")
	command.Dir = workspace
	command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+configHome, "XDG_DATA_HOME="+dataHome)
	output, runErr := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("real OpenCode timed out: %v\n%s", ctx.Err(), output)
	}
	if runErr != nil {
		t.Fatalf("real OpenCode failed: %v\n%s", runErr, output)
	}
	stopOperator()
	if err := <-operatorErr; err != nil {
		t.Fatalf("web operator: %v\n%s", err, output)
	}

	content, err := os.ReadFile(filepath.Join(workspace, "native.txt"))
	if err != nil || string(content) != before+editedSuffix {
		t.Fatalf("native.txt = %q, %v (want pulled bytes + web edit)\n%s", content, err, output)
	}
	text := string(output)
	if !strings.Contains(text, "WORKSPACE-WEB-DOOR-FINAL") {
		t.Fatalf("OpenCode output lacks the final answer:\n%s", text)
	}
	for _, tool := range []string{"write", "bash", "todowrite"} {
		if !strings.Contains(text, `"tool":"`+tool+`"`) && !strings.Contains(text, `"tool": "`+tool+`"`) {
			t.Fatalf("OpenCode output shows no native %s activity:\n%s", tool, text)
		}
	}
}

// runWorkspaceOperator is the human, scripted over the web API: it accepts the
// workspace conversation, pulls exact bytes through the caller's bash gate,
// edits the mirror, delivers the reviewed change as a native write, then runs
// a bash+todowrite batch and finals. Auxiliary chat requests are answered
// immediately.
func runWorkspaceOperator(
	ctx context.Context,
	base, mirrorRoot, workspace, editedSuffix, finalAnswer string,
) error {
	type key struct{ caller, task string }
	var main *key
	stage := "accept"
	pullCallID := "call-pull-native"
	var pulled []byte

	post := func(path string, body map[string]any) (map[string]any, int) {
		encoded, _ := json.Marshal(body)
		request, _ := http.NewRequest(http.MethodPost, base+path, strings.NewReader(string(encoded)))
		request.Header.Set("Authorization", "Bearer "+testToken)
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return nil, 0
		}
		defer response.Body.Close()
		var decoded map[string]any
		_ = json.NewDecoder(response.Body).Decode(&decoded)
		return decoded, response.StatusCode
	}
	getState := func() map[string]any {
		request, _ := http.NewRequest(http.MethodGet, base+"/api/state", nil)
		request.Header.Set("Authorization", "Bearer "+testToken)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return nil
		}
		defer response.Body.Close()
		var decoded map[string]any
		_ = json.NewDecoder(response.Body).Decode(&decoded)
		return decoded
	}

	for {
		select {
		case <-ctx.Done():
			if stage != "done" {
				return fmt.Errorf("operator stopped in stage %s", stage)
			}
			return nil
		case <-time.After(100 * time.Millisecond):
		}
		state := getState()
		if state == nil {
			continue
		}

		// Answer auxiliary chat requests (no declared tools) immediately.
		inbox, _ := state["inbox"].([]any)
		for _, raw := range inbox {
			item, _ := raw.(map[string]any)
			tier, _ := item["tier"].(string)
			delivery, _ := item["delivery"].(string)
			if tier == string(llm.TierChat) {
				accepted, status := post("/api/accept", map[string]any{"delivery": delivery})
				if status != http.StatusOK {
					continue
				}
				acceptedKey, _ := accepted["key"].(map[string]any)
				post("/api/final", map[string]any{
					"caller": fmt.Sprint(acceptedKey["caller"]), "task_id": fmt.Sprint(acceptedKey["task_id"]),
					"text": "Workspace Web Door",
				})
			} else if stage == "accept" {
				accepted, status := post("/api/accept", map[string]any{"delivery": delivery})
				if status != http.StatusOK {
					return fmt.Errorf("accept failed: %v", accepted)
				}
				acceptedKey, _ := accepted["key"].(map[string]any)
				main = &key{caller: fmt.Sprint(acceptedKey["caller"]), task: fmt.Sprint(acceptedKey["task_id"])}
				post("/api/reply", map[string]any{"caller": main.caller, "task_id": main.task, "text": "inspecting the workspace"})
				post("/api/tool-calls", map[string]any{
					"caller": main.caller, "task_id": main.task,
					"calls": []map[string]any{{
						"id": pullCallID, "name": "bash",
						"input": map[string]any{
							"command":     "opencode debug file read --pure native.txt",
							"description": "pull exact bytes of native.txt",
							"workdir":     workspace,
						},
					}},
				})
				stage = "await-pull"
			}
		}
		if main == nil {
			continue
		}
		conversation := findConversation(state, main.caller, main.task)
		if conversation == nil {
			continue
		}
		phase, _ := conversation["phase"].(string)

		switch stage {
		case "await-pull":
			if phase != "active" {
				continue
			}
			// `opencode debug file read --pure` returns an envelope of the exact
			// bytes: {"content":"<base64>","encoding":"base64","mime":...}.
			result := transcriptToolResult(conversation, pullCallID)
			var envelope struct {
				Content  string `json:"content"`
				Encoding string `json:"encoding"`
			}
			start := strings.Index(result, "{")
			end := strings.LastIndex(result, "}")
			if start < 0 || end <= start {
				return fmt.Errorf("pull result has no envelope: %q", result)
			}
			if err := json.Unmarshal([]byte(result[start:end+1]), &envelope); err != nil ||
				envelope.Encoding != "base64" {
				return fmt.Errorf("pull envelope undecodable: %q (%v)", result, err)
			}
			decoded, err := base64.StdEncoding.DecodeString(envelope.Content)
			if err != nil || len(decoded) == 0 {
				return fmt.Errorf("pull content undecodable: %q (%v)", envelope.Content, err)
			}
			pulled = decoded
			// Seed the mirror with the exact pulled bytes plus the human edit.
			if err := os.WriteFile(filepath.Join(mirrorRoot, "native.txt"),
				append(append([]byte(nil), pulled...), []byte(editedSuffix)...), 0o600); err != nil {
				return err
			}
			stage = "await-review"
		case "await-review":
			review, _ := state["review"].(map[string]any)
			changes, _ := review["changes"].([]any)
			if len(changes) == 0 {
				continue
			}
			change, _ := changes[0].(map[string]any)
			_, status := post("/api/review/deliver", map[string]any{
				"caller": main.caller, "task_id": main.task,
				"change_ids": []string{fmt.Sprint(change["id"])},
			})
			if status != http.StatusOK {
				continue
			}
			stage = "await-write"
		case "await-write":
			if phase != "active" {
				continue
			}
			post("/api/tool-calls", map[string]any{
				"caller": main.caller, "task_id": main.task,
				"calls": []map[string]any{
					{"id": "call-verify", "name": "bash", "input": map[string]any{
						"command": "printf 'command-ok:%s' \"$(cat native.txt)\"", "workdir": workspace,
						"description": "verify the delivered write",
					}},
					{"id": "call-todos", "name": "todowrite", "input": map[string]any{
						"todos": []map[string]any{{
							"id": "todo-1", "content": "workspace web door verified",
							"status": "completed", "priority": "medium",
						}},
					}},
				},
			})
			stage = "await-batch"
		case "await-batch":
			if phase != "active" {
				continue
			}
			verify := transcriptToolResult(conversation, "call-verify")
			if !strings.Contains(verify, "command-ok:") {
				return fmt.Errorf("bash verification result = %q", verify)
			}
			post("/api/final", map[string]any{"caller": main.caller, "task_id": main.task, "text": finalAnswer})
			stage = "done"
		}
	}
}

func findConversation(state map[string]any, caller, task string) map[string]any {
	conversations, _ := state["conversations"].([]any)
	for _, raw := range conversations {
		conversation, _ := raw.(map[string]any)
		keyView, _ := conversation["key"].(map[string]any)
		if fmt.Sprint(keyView["caller"]) == caller && fmt.Sprint(keyView["task_id"]) == task {
			return conversation
		}
	}
	return nil
}

func transcriptToolResult(conversation map[string]any, callID string) string {
	transcript, _ := conversation["transcript"].([]any)
	for index := len(transcript) - 1; index >= 0; index-- {
		entry, _ := transcript[index].(map[string]any)
		if fmt.Sprint(entry["kind"]) == "tool_result" && fmt.Sprint(entry["tool_call_id"]) == callID {
			return fmt.Sprint(entry["text"])
		}
	}
	return ""
}
