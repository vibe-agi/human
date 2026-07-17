package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	workspacemirror "github.com/vibe-agi/human/internal/mirror"
)

// TestRealOpenCodeNativeWorkspaceLoop is skipped on builders without the exact
// captured CLI. It remains in the ordinary test binary so a release machine
// can run the real harness gate with no separate script:
//
//	HUMAN_REAL_OPENCODE_E2E=1 go test ./internal/completion/gateway \
//	  -run TestRealOpenCodeNativeWorkspaceLoop -count=1 -v
func TestRealOpenCodeNativeWorkspaceLoop(t *testing.T) {
	if os.Getenv("HUMAN_REAL_OPENCODE_E2E") != "1" {
		t.Skip("set HUMAN_REAL_OPENCODE_E2E=1 to run the installed OpenCode CLI")
	}
	binary, err := exec.LookPath("opencode")
	if err != nil {
		t.Skip("opencode is not installed")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != adapter.OpenCodeVersion {
		t.Skipf("requires opencode %s; got %q (%v)", adapter.OpenCodeVersion, strings.TrimSpace(string(version)), err)
	}

	fixture := newGatewayFixtureWithConfig(t, true, Config{
		HeartbeatInterval: 100 * time.Millisecond,
		MaxPending:        30 * time.Second,
	})
	if err := fixture.registry.Register(adapter.OpenCode11718Profile()); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	const before = "version from the client Agent\n"
	const after = "version edited in the Human workspace\n"
	if err := os.WriteFile(filepath.Join(workspace, "native.txt"), []byte(before), 0o600); err != nil {
		t.Fatal(err)
	}
	humanWorkspace, err := workspacemirror.Open(t.TempDir(), "caller-e2e", "opencode-real-e2e")
	if err != nil {
		t.Fatal(err)
	}
	configHome := filepath.Join(t.TempDir(), "config")
	dataHome := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(configHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dataHome, 0o700); err != nil {
		t.Fatal(err)
	}
	config := map[string]any{
		"$schema": "https://opencode.ai/config.json",
		"provider": map[string]any{"human-e2e": map[string]any{
			"npm": "@ai-sdk/openai-compatible", "name": "Human E2E",
			"options": map[string]any{
				"baseURL": fixture.server.URL + "/v1", "apiKey": "hae_test",
				"headers": map[string]string{
					headerCapabilityTier: string(completion.TierWorkspace),
					headerWorkspaceKey:   "opencode-real-e2e",
					headerHarnessID:      adapter.OpenCodeID,
					headerHarnessVersion: adapter.OpenCodeVersion,
					headerWorkspaceRoot:  workspace,
					headerAllowExec:      "true",
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

	type observed struct {
		mainTask    string
		firstKey    string
		secondKey   string
		thirdKey    string
		fourthKey   string
		nextTask    string
		nextKey     string
		pullCallID  string
		toolCallID  string
		auxiliary   int
		editResult  string
		bashResult  string
		tasksResult string
		assignments int
	}
	workerDone := make(chan observed, 1)
	workerErr := make(chan error, 1)
	go func() {
		state := observed{}
		for state.nextKey == "" {
			var assignment completion.Assignment
			select {
			case assignment = <-fixture.worker.Assignments:
			case <-time.After(25 * time.Second):
				workerErr <- fmt.Errorf("timed out waiting for OpenCode assignment after %+v", state)
				return
			}
			state.assignments++
			if !hasCanonicalTool(assignment.Request, "edit") {
				if assignment.CapabilityTier != completion.TierChat || assignment.TaskID == "" ||
					strings.HasPrefix(assignment.TaskID, "ses_") {
					workerErr <- fmt.Errorf("auxiliary request was not isolated Chat: %+v", assignment)
					return
				}
				state.auxiliary++
				if err := publishAcceptedAndFinal(fixture, assignment, "OpenCode E2E"); err != nil {
					workerErr <- err
					return
				}
				continue
			}
			if assignment.CapabilityTier != completion.TierWorkspace || assignment.Adapter == nil ||
				assignment.Adapter.Key() != adapter.OpenCodeID+"@"+adapter.OpenCodeVersion ||
				assignment.Root != workspace || !strings.HasPrefix(assignment.TaskID, openCodeTaskPrefix) ||
				!strings.HasPrefix(assignment.IdempotencyKey, openCodeDerivedKeyPrefix) {
				workerErr <- fmt.Errorf("main OpenCode assignment has wrong identity: %+v", assignment)
				return
			}
			if state.fourthKey != "" {
				if assignment.TaskID == state.mainTask || assignment.IdempotencyKey == state.fourthKey {
					workerErr <- fmt.Errorf("new user turn reused terminal task/request: %+v", assignment)
					return
				}
				state.nextTask = assignment.TaskID
				state.nextKey = assignment.IdempotencyKey
				if err := publishAcceptedAndFinal(fixture, assignment, "second user turn complete"); err != nil {
					workerErr <- err
					return
				}
				continue
			}
			if state.pullCallID == "" {
				state.mainTask = assignment.TaskID
				state.firstKey = assignment.IdempotencyKey
				call, err := workspacemirror.BuildHydrationToolCallForProfile(
					"native.txt", assignment.Adapter, workspace,
				)
				if err != nil {
					workerErr <- err
					return
				}
				if err := humanWorkspace.RecordHydrationIntent(
					"native.txt", call, assignment.Adapter, workspace,
				); err != nil {
					workerErr <- err
					return
				}
				state.pullCallID = call.ID
				if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
					completion.Event{ID: "real-open-pull-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID}); err != nil {
					workerErr <- err
					return
				}
				if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
					completion.Event{ID: "real-open-pull", Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{call}}); err != nil {
					workerErr <- err
					return
				}
				continue
			}
			if assignment.TaskID != state.mainTask || assignment.IdempotencyKey == state.firstKey {
				workerErr <- fmt.Errorf("continuation identity changed incorrectly: first=%s/%s next=%s/%s",
					state.mainTask, state.firstKey, assignment.TaskID, assignment.IdempotencyKey)
				return
			}
			pullResult, pulled := canonicalToolResult(assignment.Request, state.pullCallID)
			if !pulled {
				workerErr <- fmt.Errorf("OpenCode continuation omitted workspace pull result %s", state.pullCallID)
				return
			}
			if _, ok := pullResult.Output.(string); !ok {
				workerErr <- fmt.Errorf("OpenCode workspace pull result type = %T", pullResult.Output)
				return
			}
			if state.toolCallID == "" {
				state.secondKey = assignment.IdempotencyKey
				reconciled, err := humanWorkspace.ReconcileRequestForProfile(
					assignment.Request, assignment.Adapter, workspace,
				)
				if err != nil || len(reconciled.Confirmed) != 1 || reconciled.Confirmed[0] != state.pullCallID ||
					len(reconciled.Failed) != 0 {
					workerErr <- fmt.Errorf("OpenCode workspace pull did not hydrate the Human mirror: %+v (%v)", reconciled, err)
					return
				}
				pulledContent, err := os.ReadFile(filepath.Join(humanWorkspace.Dir(), "native.txt"))
				if err != nil || string(pulledContent) != before {
					workerErr <- fmt.Errorf("Human mirror pull = %q (%v)", pulledContent, err)
					return
				}
				if err := os.WriteFile(filepath.Join(humanWorkspace.Dir(), "native.txt"), []byte(after), 0o600); err != nil {
					workerErr <- err
					return
				}
				changes, err := humanWorkspace.Review()
				if err != nil {
					workerErr <- err
					return
				}
				report, err := workspacemirror.BuildToolCallsForProfile(changes, assignment.Adapter, workspace)
				if err != nil {
					workerErr <- err
					return
				}
				if len(report.Calls) != 1 || report.Calls[0].Name != "edit" || len(report.Warnings) == 0 {
					workerErr <- fmt.Errorf("Human mirror did not produce the exact OpenCode edit contract: %+v", report)
					return
				}
				if err := humanWorkspace.RecordDeliveryIntents(
					changes, report.Calls, assignment.Adapter, workspace,
				); err != nil {
					workerErr <- err
					return
				}
				state.toolCallID = report.Calls[0].ID
				if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
					completion.Event{ID: "real-open-edit-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID}); err != nil {
					workerErr <- err
					return
				}
				if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
					completion.Event{ID: "real-open-edit", Type: completion.EventToolCalls, ToolCalls: report.Calls}); err != nil {
					workerErr <- err
					return
				}
				continue
			}
			editResult, edited := canonicalToolResult(assignment.Request, state.toolCallID)
			if !edited {
				workerErr <- fmt.Errorf("OpenCode continuation omitted native edit result %s", state.toolCallID)
				return
			}
			text, ok := editResult.Output.(string)
			if !ok {
				workerErr <- fmt.Errorf("OpenCode native edit result type = %T", editResult.Output)
				return
			}
			state.editResult = strings.TrimSpace(text)

			bashResult, ranCommand := canonicalToolResult(assignment.Request, "tool_native_bash")
			tasksResult, updatedTasks := canonicalToolResult(assignment.Request, "tool_native_tasks")
			if !ranCommand && !updatedTasks {
				state.thirdKey = assignment.IdempotencyKey
				reconciled, err := humanWorkspace.ReconcileRequestForProfile(
					assignment.Request, assignment.Adapter, workspace,
				)
				if err != nil || len(reconciled.Confirmed) != 1 || reconciled.Confirmed[0] != state.toolCallID ||
					len(reconciled.Failed) != 0 {
					workerErr <- fmt.Errorf("OpenCode edit result did not reconcile the Human baseline: %+v (%v)", reconciled, err)
					return
				}
				remaining, err := humanWorkspace.Review()
				if err != nil || len(remaining) != 0 {
					workerErr <- fmt.Errorf("Human mirror remained dirty after OpenCode result: %+v (%v)", remaining, err)
					return
				}
				if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
					completion.Event{ID: "real-open-tools-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID}); err != nil {
					workerErr <- err
					return
				}
				if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
					completion.Event{ID: "real-open-command-tasks", Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{
						{ID: "tool_native_bash", Name: "bash", Input: map[string]any{
							"command": "printf 'command-ok:' && wc -c < native.txt",
							"workdir": workspace, "description": "Verify the Human workspace edit",
						}},
						{ID: "tool_native_tasks", Name: "todowrite", Input: map[string]any{
							"todos": []map[string]any{{
								"content": "Apply the Human workspace edit", "status": "completed", "priority": "high",
							}},
						}},
					}}); err != nil {
					workerErr <- err
					return
				}
				continue
			}
			if !ranCommand || !updatedTasks || assignment.IdempotencyKey == state.thirdKey {
				workerErr <- fmt.Errorf("OpenCode did not return command and task results together: bash=%t tasks=%t", ranCommand, updatedTasks)
				return
			}
			bashText, bashOK := bashResult.Output.(string)
			tasksText, tasksOK := tasksResult.Output.(string)
			if !bashOK || !tasksOK {
				workerErr <- fmt.Errorf("OpenCode command/task result types = %T/%T", bashResult.Output, tasksResult.Output)
				return
			}
			state.bashResult = strings.TrimSpace(bashText)
			state.tasksResult = strings.TrimSpace(tasksText)
			state.fourthKey = assignment.IdempotencyKey
			if err := publishAcceptedAndFinal(fixture, assignment, "native workspace loop complete"); err != nil {
				workerErr <- err
				return
			}
		}
		workerDone <- state
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "run", "--pure", "--auto", "--format", "json",
		"--model", "human-e2e/human", "--dir", workspace,
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
	sessionID, err := openCodeSessionIDFromJSONLines(output)
	if err != nil {
		t.Fatalf("read real OpenCode session id: %v\n%s", err, output)
	}
	secondCommand := exec.CommandContext(ctx, binary, "run", "--pure", "--auto", "--format", "json",
		"--model", "human-e2e/human", "--dir", workspace, "--session", sessionID,
		"Handle this second top-level user turn in the same OpenCode session.")
	secondCommand.Dir = workspace
	secondCommand.Env = command.Env
	secondOutput, secondRunErr := secondCommand.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("second real OpenCode turn timed out: %v\n%s", ctx.Err(), secondOutput)
	}
	if secondRunErr != nil {
		t.Fatalf("second real OpenCode turn failed: %v\n%s", secondRunErr, secondOutput)
	}
	select {
	case err := <-workerErr:
		t.Fatal(err)
	case state := <-workerDone:
		if state.editResult != "Edit applied successfully." || !strings.Contains(state.bashResult, "command-ok:") ||
			state.tasksResult == "" || state.mainTask == "" || state.firstKey == "" || state.secondKey == "" ||
			state.thirdKey == "" || state.fourthKey == "" || state.nextTask == "" || state.nextKey == "" ||
			state.pullCallID == "" || state.toolCallID == "" || state.assignments < 5 {
			t.Fatalf("observed OpenCode loop = %+v", state)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OpenCode exited before the worker observed the completed continuation")
	}
	content, err := os.ReadFile(filepath.Join(workspace, "native.txt"))
	if err != nil || string(content) != after {
		t.Fatalf("native edit result = %q, %v\n%s", content, err, output)
	}
	if !strings.Contains(string(output), "native workspace loop complete") ||
		!strings.Contains(string(output), "native.txt") ||
		(!strings.Contains(string(output), `"tool":"edit"`) &&
			!strings.Contains(string(output), `"tool": "edit"`)) ||
		(!strings.Contains(string(output), `"tool":"bash"`) &&
			!strings.Contains(string(output), `"tool": "bash"`)) ||
		(!strings.Contains(string(output), `"tool":"todowrite"`) &&
			!strings.Contains(string(output), `"tool": "todowrite"`)) {
		t.Fatalf("OpenCode output did not expose native tool/final activity:\n%s", output)
	}
	if !strings.Contains(string(secondOutput), "second user turn complete") {
		t.Fatalf("second OpenCode user turn did not complete:\n%s", secondOutput)
	}
}

func openCodeSessionIDFromJSONLines(output []byte) (string, error) {
	for _, line := range strings.Split(string(output), "\n") {
		var event struct {
			SessionID string `json:"sessionID"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.SessionID != "" {
			return event.SessionID, nil
		}
	}
	return "", errors.New("OpenCode JSON output contained no sessionID")
}

func hasCanonicalTool(request canonical.Request, name string) bool {
	for _, tool := range request.Tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}

func canonicalToolResult(request canonical.Request, id string) (canonical.Block, bool) {
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Type == canonical.BlockToolResult && block.ToolCallID == id {
				return block, true
			}
		}
	}
	return canonical.Block{}, false
}

func publishAcceptedAndFinal(fixture *gatewayFixture, assignment completion.Assignment, text string) error {
	if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
		completion.Event{ID: "accepted_" + assignment.IdempotencyKey, Type: completion.EventAccepted, WorkerID: fixture.worker.ID}); err != nil {
		return err
	}
	return fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
		completion.Event{ID: "final_" + assignment.IdempotencyKey, Type: completion.EventFinal, Text: text})
}
