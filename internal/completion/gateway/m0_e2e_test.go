package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/callershim"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/ratelimit"
)

// TestM0TwentyRemoteToolLoops is the executable M0 contract smoke test:
// the real gateway and caller shim complete read -> edit -> exec -> final
// twenty times, while duplicate HTTP requests and tool executions remain
// idempotent. It intentionally uses the project-owned exact adapter rather
// than inferring capabilities from a tool schema.
func TestM0TwentyRemoteToolLoops(t *testing.T) {
	fixture := newGatewayFixtureWithConfig(t, true, Config{RateLimit: ratelimit.Config{
		RatePerSecond: 10_000, Burst: 10_000,
	}})
	if err := fixture.registry.Register(adapter.HumanShimProfile()); err != nil {
		t.Fatal(err)
	}

	workspace := t.TempDir()
	root, err := callerfs.OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := callershim.OpenSQLiteLedger(context.Background(), filepath.Join(t.TempDir(), "caller-ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ledger.Close() })
	executor, err := callershim.NewExecutor(callershim.ExecutorConfig{
		Root: root, Ledger: ledger, ExecEnabled: true, DefaultTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	for cycle := 0; cycle < 20; cycle++ {
		taskID := fmt.Sprintf("task-m0-%02d", cycle)
		shim, err := callershim.NewServer(callershim.ServerConfig{
			GatewayURL: fixture.server.URL, CallerToken: "hae_test", ToolToken: "tool-test",
			CallerID: "caller-1", WorkspaceKey: "workspace-m0", WorkspaceRoot: workspace, TaskID: taskID,
			AllowExec: true, Executor: executor,
		})
		if err != nil {
			t.Fatal(err)
		}
		shimServer := httptest.NewServer(shim)
		runM0Loop(t, fixture, shimServer.URL, workspace, cycle)
		shimServer.Close()
	}
}

func runM0Loop(t *testing.T, fixture *gatewayFixture, shimURL, workspace string, cycle int) {
	t.Helper()
	taskID := fmt.Sprintf("task-m0-%02d", cycle)
	fileName := fmt.Sprintf("source-%02d.txt", cycle)
	execName := fmt.Sprintf("exec-%02d.log", cycle)
	initial := fmt.Sprintf("before-%02d", cycle)
	final := fmt.Sprintf("after-%02d", cycle)
	if err := os.WriteFile(filepath.Join(workspace, fileName), []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}

	messages := []map[string]any{{"role": "user", "content": "inspect, edit, verify"}}
	readID := fmt.Sprintf("read-%02d", cycle)
	readInput := map[string]any{"path": "/workspace/" + fileName}
	firstBody := m0ChatBody(t, messages)
	first := m0Completion(t, fixture, shimURL, taskID, "m0-read-"+taskID, firstBody, completion.Event{
		ID: "event-" + readID, Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{ID: readID, Name: "human_read_file", Input: readInput}},
	})
	if !bytes.Contains(first, []byte(readID)) {
		t.Fatalf("cycle %d read response did not contain tool call: %s", cycle, first)
	}
	readResult := m0ExecuteTwice(t, shimURL, callershim.ToolRequest{
		CallerID: "caller-1", TaskID: taskID, ToolCallID: readID, Name: "human_read_file",
		Input: m0Raw(t, readInput),
	})
	readContent := m0ContentMap(t, readResult)
	fingerprint, _ := readContent["sha256"].(string)
	if fingerprint == "" || readContent["content"] != initial {
		t.Fatalf("cycle %d read result = %+v", cycle, readResult)
	}
	messages = appendToolExchange(t, messages, readID, "human_read_file", readInput, readResult)

	editID := fmt.Sprintf("edit-%02d", cycle)
	editInput := map[string]any{
		"path": "/workspace/" + fileName, "old_string": initial,
		"new_string": final, "expected_sha256": fingerprint,
	}
	secondBody := m0ChatBody(t, messages)
	second := m0Completion(t, fixture, shimURL, taskID, "m0-edit-"+taskID, secondBody, completion.Event{
		ID: "event-" + editID, Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{ID: editID, Name: "human_edit_file", Input: editInput}},
	})
	if !bytes.Contains(second, []byte(editID)) {
		t.Fatalf("cycle %d edit response did not contain tool call: %s", cycle, second)
	}
	editResult := m0ExecuteTwice(t, shimURL, callershim.ToolRequest{
		CallerID: "caller-1", TaskID: taskID, ToolCallID: editID, Name: "human_edit_file",
		Input: m0Raw(t, editInput),
	})
	content, err := os.ReadFile(filepath.Join(workspace, fileName))
	if err != nil || string(content) != final {
		t.Fatalf("cycle %d CAS edit = %q, %v", cycle, content, err)
	}
	messages = appendToolExchange(t, messages, editID, "human_edit_file", editInput, editResult)

	execID := fmt.Sprintf("exec-%02d", cycle)
	execInput := map[string]any{"command": "printf x >> " + execName, "timeout_ms": 1000}
	thirdBody := m0ChatBody(t, messages)
	third := m0Completion(t, fixture, shimURL, taskID, "m0-exec-"+taskID, thirdBody, completion.Event{
		ID: "event-" + execID, Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{ID: execID, Name: "human_exec", Input: execInput}},
	})
	if !bytes.Contains(third, []byte(execID)) {
		t.Fatalf("cycle %d exec response did not contain tool call: %s", cycle, third)
	}
	execResult := m0ExecuteTwice(t, shimURL, callershim.ToolRequest{
		CallerID: "caller-1", TaskID: taskID, ToolCallID: execID, Name: "human_exec",
		Input: m0Raw(t, execInput),
	})
	execContent, err := os.ReadFile(filepath.Join(workspace, execName))
	if err != nil || string(execContent) != "x" {
		t.Fatalf("cycle %d duplicate exec was not exactly-once: %q, %v", cycle, execContent, err)
	}
	messages = appendToolExchange(t, messages, execID, "human_exec", execInput, execResult)

	finalBody := m0ChatBody(t, messages)
	key := "m0-final-" + taskID
	finalResponse := m0Completion(t, fixture, shimURL, taskID, key, finalBody, completion.Event{
		ID: "event-final-" + taskID, Type: completion.EventFinal, Text: "verified " + fileName,
	})
	replay := m0PostCompletion(t, shimURL, taskID, key, finalBody)
	if !bytes.Equal(finalResponse, replay) {
		t.Fatalf("cycle %d completion replay changed\nfirst: %s\nreplay: %s", cycle, finalResponse, replay)
	}
	select {
	case unexpected := <-fixture.worker.Assignments:
		t.Fatalf("cycle %d idempotent replay was dispatched again: %+v", cycle, unexpected)
	case <-time.After(5 * time.Millisecond):
	}
}

func m0Completion(
	t *testing.T,
	fixture *gatewayFixture,
	shimURL, taskID, key string,
	body []byte,
	event completion.Event,
) []byte {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if assignment.TaskID != taskID || assignment.IdempotencyKey != key || assignment.LeaseOwner != "worker-1" {
			done <- fmt.Errorf("unexpected assignment: %+v", assignment)
			return
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			ID: "accept-" + key, Type: completion.EventAccepted, WorkerID: "worker-1",
		}); err != nil {
			done <- err
			return
		}
		done <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, event)
	}()
	response := m0PostCompletion(t, shimURL, taskID, key, body)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return response
}

func m0PostCompletion(t *testing.T, shimURL, taskID, key string, body []byte) []byte {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, shimURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(headerTaskID, taskID)
	request.Header.Set(headerIdempotencyKey, key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !bytes.Contains(payload, []byte("data: [DONE]")) {
		t.Fatalf("completion status = %d, body = %s", response.StatusCode, payload)
	}
	return payload
}

func m0ExecuteTwice(t *testing.T, shimURL string, tool callershim.ToolRequest) callershim.ToolResponse {
	t.Helper()
	payload, err := json.Marshal(tool)
	if err != nil {
		t.Fatal(err)
	}
	var first callershim.ToolResponse
	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequest(http.MethodPost, shimURL+"/internal/v1/tools/execute", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer tool-test")
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var result callershim.ToolResponse
		decodeErr := json.NewDecoder(response.Body).Decode(&result)
		response.Body.Close()
		if decodeErr != nil || response.StatusCode != http.StatusOK || result.IsError {
			t.Fatalf("tool %s attempt %d = status %d, %+v, %v", tool.ToolCallID, attempt, response.StatusCode, result, decodeErr)
		}
		if attempt == 0 {
			first = result
		} else if !reflect.DeepEqual(first, result) {
			t.Fatalf("tool %s replay changed: first=%+v replay=%+v", tool.ToolCallID, first, result)
		}
	}
	return first
}

func m0ChatBody(t *testing.T, messages []map[string]any) []byte {
	t.Helper()
	names := []string{"human_read_file", "human_edit_file", "human_exec"}
	tools := make([]map[string]any, 0, len(names))
	for _, name := range names {
		tools = append(tools, map[string]any{
			"type": "function", "function": map[string]any{
				"name": name, "parameters": map[string]any{"type": "object"},
			},
		})
	}
	payload, err := json.Marshal(map[string]any{
		"model": "human-expert", "stream": true, "messages": messages, "tools": tools,
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func appendToolExchange(
	t *testing.T,
	messages []map[string]any,
	id, name string,
	input map[string]any,
	result callershim.ToolResponse,
) []map[string]any {
	t.Helper()
	arguments, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	resultPayload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return append(messages,
		map[string]any{
			"role": "assistant", "content": nil,
			"tool_calls": []map[string]any{{
				"id": id, "type": "function",
				"function": map[string]any{"name": name, "arguments": string(arguments)},
			}},
		},
		map[string]any{"role": "tool", "tool_call_id": id, "content": string(resultPayload)},
	)
}

func m0Raw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func m0ContentMap(t *testing.T, response callershim.ToolResponse) map[string]any {
	t.Helper()
	content, ok := response.Content.(map[string]any)
	if !ok {
		t.Fatalf("tool response content is %T: %+v", response.Content, response.Content)
	}
	return content
}
