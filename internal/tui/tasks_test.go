package tui

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion/canonical"
)

const openCodeTodoWriteSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "description": "Update the task list shown by the client Agent.",
  "required": ["todos"],
  "properties": {
    "todos": {
      "description": "The complete replacement task list.",
      "items": {
        "required": ["content", "status", "priority"],
        "properties": {
          "priority": {
            "description": "Execution priority.",
            "enum": ["high", "medium", "low"],
            "type": "string"
          },
          "content": {
            "minLength": 1,
            "description": "Task text.",
            "type": "string"
          },
          "status": {
            "enum": ["pending", "in_progress", "completed", "cancelled"],
            "description": "Current task state.",
            "type": "string"
          }
        },
        "type": "object"
      },
      "type": "array"
    }
  },
  "type": "object"
}`

const claudeTodoWriteSchema = `{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "todos": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "content": {"type": "string", "description": "Imperative task"},
          "status": {"type": "string", "enum": ["pending", "in_progress", "completed"]},
          "activeForm": {"type": "string", "description": "Present-progress form"}
        },
        "required": ["content", "status", "activeForm"]
      }
    }
  },
  "required": ["todos"]
}`

const codexUpdatePlanSchema = `{
  "additionalProperties": false,
  "required": ["plan"],
  "type": "object",
  "properties": {
    "plan": {
      "items": {
        "required": ["step", "status"],
        "additionalProperties": false,
        "type": "object",
        "properties": {
          "status": {"enum": ["pending", "in_progress", "completed"], "type": "string"},
          "step": {"description": "Plan step", "type": "string"}
        }
      },
      "type": "array"
    },
    "explanation": {"description": "Why the plan changed", "type": "string"}
  }
}`

func taskTool(name, schema string) canonical.Tool {
	return canonical.Tool{Name: name, InputSchema: json.RawMessage(schema)}
}

func TestTaskTargetMatchesRealOpenCodeSchemaWithoutAdditionalProperties(t *testing.T) {
	t.Parallel()

	target, reason := taskTargetForRequest(canonical.Request{Tools: []canonical.Tool{
		{
			Name:        "todowrite",
			Description: "Annotations and JSON key order do not change the wire contract.",
			InputSchema: json.RawMessage(openCodeTodoWriteSchema),
		},
	}})

	if reason != "" {
		t.Fatalf("compatible OpenCode schema rejected: %s", reason)
	}
	if target != (taskTarget{name: "todowrite", kind: taskTargetOpenCode}) {
		t.Fatalf("target = %+v", target)
	}
}

func TestTaskTargetDoesNotMistakeNamespacedLeafNameForBuiltinTaskTool(t *testing.T) {
	t.Parallel()
	namespaced := taskTool("update_plan", codexUpdatePlanSchema)
	namespaced.Namespace = "multi_agent_v1"
	if _, reason := taskTargetForRequest(canonical.Request{Tools: []canonical.Tool{namespaced}}); !strings.Contains(reason, "no compatible") {
		t.Fatalf("namespaced lookalike enabled Tasks: %q", reason)
	}
	topLevel := taskTool("update_plan", codexUpdatePlanSchema)
	target, reason := taskTargetForRequest(canonical.Request{Tools: []canonical.Tool{namespaced, topLevel}})
	if reason != "" || target.name != "update_plan" || target.kind != taskTargetCodex {
		t.Fatalf("top-level task tool hidden by namespaced lookalike: target=%+v reason=%q", target, reason)
	}
}

func TestTaskTargetRejectsUnsupportedOrAmbiguousContracts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tools      []canonical.Tool
		wantReason string
	}{
		{
			name:       "wrong tool name",
			tools:      []canonical.Tool{taskTool("todo_write", openCodeTodoWriteSchema)},
			wantReason: "no compatible task-list tool",
		},
		{
			name: "missing required task field",
			tools: []canonical.Tool{taskTool("todowrite", `{
              "type":"object", "properties":{"todos":{"type":"array", "items":{
                "type":"object", "properties":{"content":{"type":"string"},"status":{"type":"string"},"priority":{"type":"string"}},
                "required":["content","status"]
              }}}, "required":["todos"]
            }`)},
			wantReason: `field "priority" must be required`,
		},
		{
			name: "wrong field type",
			tools: []canonical.Tool{taskTool("todowrite", `{
              "type":"object", "properties":{"todos":{"type":"array", "items":{
                "type":"object", "properties":{"content":{"type":"string"},"status":{"type":"string"},"priority":{"type":"integer"}},
                "required":["content","status","priority"]
              }}}, "required":["todos"]
            }`)},
			wantReason: `task field "priority" must be a string`,
		},
		{
			name: "status enum drift",
			tools: []canonical.Tool{taskTool("todowrite", `{
              "type":"object", "properties":{"todos":{"type":"array", "items":{
                "type":"object", "properties":{"content":{"type":"string"},"status":{"type":"string","enum":["pending","done"]},"priority":{"type":"string","enum":["high","medium","low"]}},
                "required":["content","status","priority"]
              }}}, "required":["todos"]
            }`)},
			wantReason: "unsupported enum",
		},
		{
			name: "unknown item field",
			tools: []canonical.Tool{taskTool("todowrite", `{
              "type":"object", "properties":{"todos":{"type":"array", "items":{
                "type":"object", "properties":{"content":{"type":"string"},"status":{"type":"string"},"priority":{"type":"string"},"id":{"type":"string"}},
                "required":["content","status","priority"]
              }}}, "required":["todos"]
            }`)},
			wantReason: `unknown property "id"`,
		},
		{
			name: "referenced schema",
			tools: []canonical.Tool{taskTool("todowrite", `{
              "type":"object", "$ref":"#/$defs/task-list", "properties":{}, "required":[]
            }`)},
			wantReason: "referenced schemas need an explicit adapter",
		},
		{
			name: "two compatible candidates",
			tools: []canonical.Tool{
				taskTool("todowrite", openCodeTodoWriteSchema),
				taskTool("TodoWrite", claudeTodoWriteSchema),
			},
			wantReason: "multiple compatible task tools",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			target, reason := taskTargetForRequest(canonical.Request{Tools: test.tools})
			if target != (taskTarget{}) {
				t.Fatalf("unexpected target = %+v", target)
			}
			if !strings.Contains(reason, test.wantReason) {
				t.Fatalf("reason = %q, want substring %q", reason, test.wantReason)
			}
		})
	}
}

func TestTaskTargetMatchesStrictClaudeAndCodexContracts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tool       canonical.Tool
		wantTarget taskTarget
	}{
		{
			name:       "Claude TodoWrite",
			tool:       taskTool("TodoWrite", claudeTodoWriteSchema),
			wantTarget: taskTarget{name: "TodoWrite", kind: taskTargetClaude},
		},
		{
			name:       "Codex update_plan",
			tool:       taskTool("update_plan", codexUpdatePlanSchema),
			wantTarget: taskTarget{name: "update_plan", kind: taskTargetCodex},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			target, reason := taskTargetForRequest(canonical.Request{Tools: []canonical.Tool{test.tool}})
			if reason != "" || target != test.wantTarget {
				t.Fatalf("target = %+v, reason = %q", target, reason)
			}
		})
	}

	rejections := []struct {
		name       string
		tool       canonical.Tool
		wantReason string
	}{
		{
			name: "Claude activeForm must be required",
			tool: taskTool("TodoWrite", `{
              "type":"object", "properties":{"todos":{"type":"array", "items":{
                "type":"object", "properties":{"content":{"type":"string"},"status":{"type":"string"},"activeForm":{"type":"string"}},
                "required":["content","status"]
              }}}, "required":["todos"]
            }`),
			wantReason: `field "activeForm" must be required`,
		},
		{
			name: "Codex explanation must be a string",
			tool: taskTool("update_plan", `{
              "type":"object", "properties":{
                "explanation":{"type":"array"},
                "plan":{"type":"array", "items":{"type":"object", "properties":{"step":{"type":"string"},"status":{"type":"string"}}, "required":["step","status"]}}
              }, "required":["plan"]
            }`),
			wantReason: "explanation must be a string",
		},
	}
	for _, test := range rejections {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, reason := taskTargetForRequest(canonical.Request{Tools: []canonical.Tool{test.tool}})
			if !strings.Contains(reason, test.wantReason) {
				t.Fatalf("reason = %q, want substring %q", reason, test.wantReason)
			}
		})
	}
}

func TestTaskTargetsEncodeStatusesPrioritiesUnicodeAndOrder(t *testing.T) {
	t.Parallel()

	openCodeItems := []agentTask{
		{Content: "修 P1：安全与挂起类", Status: taskCompleted, Priority: "high"},
		{Content: "検証 🧪", Status: taskInProgress, Priority: "low"},
		{Content: "保持顺序", Status: taskPending, Priority: "medium"},
		{Content: "废弃旧方案", Status: taskCancelled, Priority: "high"},
	}
	openCodeTarget := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	wantOpenCode := map[string]any{"todos": []map[string]any{
		{"content": "修 P1：安全与挂起类", "status": "completed", "priority": "high"},
		{"content": "検証 🧪", "status": "in_progress", "priority": "low"},
		{"content": "保持顺序", "status": "pending", "priority": "medium"},
		{"content": "废弃旧方案", "status": "cancelled", "priority": "high"},
	}}
	assertTaskInput(t, openCodeTarget, openCodeItems, wantOpenCode)

	portableItems := openCodeItems[:3]
	claudeTarget := taskTarget{name: "TodoWrite", kind: taskTargetClaude}
	wantClaude := map[string]any{"todos": []map[string]any{
		{"content": "修 P1：安全与挂起类", "status": "completed", "activeForm": "修 P1：安全与挂起类"},
		{"content": "検証 🧪", "status": "in_progress", "activeForm": "検証 🧪"},
		{"content": "保持顺序", "status": "pending", "activeForm": "保持顺序"},
	}}
	assertTaskInput(t, claudeTarget, portableItems, wantClaude)

	codexTarget := taskTarget{name: "update_plan", kind: taskTargetCodex}
	wantCodex := map[string]any{
		"plan": []map[string]any{
			{"step": "修 P1：安全与挂起类", "status": "completed"},
			{"step": "検証 🧪", "status": "in_progress"},
			{"step": "保持顺序", "status": "pending"},
		},
	}
	assertTaskInput(t, codexTarget, portableItems, wantCodex)
}

func assertTaskInput(t *testing.T, target taskTarget, items []agentTask, want map[string]any) {
	t.Helper()
	input := target.buildInput(items)
	if !reflect.DeepEqual(input, want) {
		t.Fatalf("%s input = %#v, want %#v", target.name, input, want)
	}
	decoded, err := tasksFromInput(input, target)
	if err != nil {
		t.Fatalf("decode generated %s input: %v", target.name, err)
	}
	wantDecoded := append([]agentTask(nil), items...)
	if target.kind != taskTargetOpenCode {
		for index := range wantDecoded {
			wantDecoded[index].Priority = "medium"
		}
	}
	if !reflect.DeepEqual(decoded, wantDecoded) {
		t.Fatalf("decoded %s tasks = %#v, want %#v", target.name, decoded, wantDecoded)
	}
}

func TestTaskHistoryPairsAndUsesLatestSuccessfulUpdate(t *testing.T) {
	t.Parallel()
	target := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	first := []agentTask{{Content: "first", Status: taskCompleted, Priority: "low"}}
	latest := []agentTask{{Content: "最新任务 🧭", Status: taskInProgress, Priority: "high"}}

	history, err := taskHistoryFromRequest(taskRequest(
		taskUse("call-1", target, first),
		taskResult(t, "call-1", first, false),
		taskUse("call-2", target, latest),
		taskResult(t, "call-2", latest, false),
	), target)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	assertTaskHistory(t, history, latest, true, false, false)
}

func TestTaskHistoryFallsBackAfterFailedOrMalformedResult(t *testing.T) {
	t.Parallel()
	target := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	confirmed := []agentTask{{Content: "confirmed", Status: taskCompleted, Priority: "medium"}}
	candidate := []agentTask{{Content: "candidate", Status: taskPending, Priority: "high"}}

	tests := []struct {
		name   string
		result canonical.Block
	}{
		{
			name:   "explicit tool failure",
			result: canonical.Block{Type: canonical.BlockToolResult, ToolCallID: "call-new", Output: "permission denied", IsError: true},
		},
		{
			name:   "malformed success output",
			result: canonical.Block{Type: canonical.BlockToolResult, ToolCallID: "call-new", Output: "not JSON", IsError: false},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			history, err := taskHistoryFromRequest(taskRequest(
				taskUse("call-old", target, confirmed),
				taskResult(t, "call-old", confirmed, false),
				taskUse("call-new", target, candidate),
				test.result,
			), target)
			if err != nil {
				t.Fatalf("read history: %v", err)
			}
			assertTaskHistory(t, history, confirmed, true, false, false)
		})
	}
}

func TestTaskHistoryReturnsLatestUnpairedCallAsPending(t *testing.T) {
	t.Parallel()
	target := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	confirmed := []agentTask{{Content: "confirmed", Status: taskCompleted, Priority: "medium"}}
	pending := []agentTask{{Content: "awaiting client Agent", Status: taskInProgress, Priority: "high"}}

	history, err := taskHistoryFromRequest(taskRequest(
		taskUse("call-old", target, confirmed),
		taskResult(t, "call-old", confirmed, false),
		taskUse("call-pending", target, pending),
	), target)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	assertTaskHistory(t, history, pending, true, true, false)
}

func TestTaskHistoryRejectsMalformedCallInput(t *testing.T) {
	t.Parallel()
	target := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	request := taskRequest(canonical.Block{
		Type: canonical.BlockToolUse, ToolName: target.name, ToolCallID: "call-bad",
		Input: map[string]any{"todos": "not an array"},
	})

	_, err := taskHistoryFromRequest(request, target)
	if err == nil || !strings.Contains(err.Error(), "read todowrite call call-bad") {
		t.Fatalf("error = %v", err)
	}
}

func TestTaskHistoryFlagsResultDivergenceAndKeepsConfirmedFallback(t *testing.T) {
	t.Parallel()
	target := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	confirmed := []agentTask{{Content: "confirmed", Status: taskCompleted, Priority: "low"}}
	candidate := []agentTask{{Content: "candidate", Status: taskInProgress, Priority: "high"}}

	history, err := taskHistoryFromRequest(taskRequest(
		taskUse("call-old", target, confirmed),
		taskResult(t, "call-old", confirmed, false),
		taskUse("call-new", target, candidate),
		taskResult(t, "call-new", confirmed, false),
	), target)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	assertTaskHistory(t, history, confirmed, true, false, true)
}

func TestTaskHistoryConflictIsDerivedFromRetainedHistory(t *testing.T) {
	t.Parallel()
	target := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	divergentCall := []agentTask{{Content: "divergent", Status: taskPending, Priority: "low"}}
	divergentResult := []agentTask{{Content: "different", Status: taskCompleted, Priority: "high"}}
	latest := []agentTask{{Content: "latest", Status: taskInProgress, Priority: "medium"}}

	history, err := taskHistoryFromRequest(taskRequest(
		taskUse("call-divergent", target, divergentCall),
		taskResult(t, "call-divergent", divergentResult, false),
		taskUse("call-latest", target, latest),
		taskResult(t, "call-latest", latest, false),
	), target)
	if err != nil {
		t.Fatalf("read retained conflict history: %v", err)
	}
	assertTaskHistory(t, history, latest, true, false, true)

	compressed, err := taskHistoryFromRequest(taskRequest(
		taskUse("call-latest", target, latest),
		taskResult(t, "call-latest", latest, false),
	), target)
	if err != nil {
		t.Fatalf("read compressed history: %v", err)
	}
	assertTaskHistory(t, compressed, latest, true, false, false)
}

func TestTaskHistoryDoesNotPairWrongToolCallID(t *testing.T) {
	t.Parallel()
	target := taskTarget{name: "todowrite", kind: taskTargetOpenCode}
	pending := []agentTask{{Content: "still pending", Status: taskPending, Priority: "medium"}}

	history, err := taskHistoryFromRequest(taskRequest(
		taskUse("call-right", target, pending),
		taskResult(t, "call-wrong", pending, false),
	), target)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	assertTaskHistory(t, history, pending, true, true, false)
}

func taskRequest(blocks ...canonical.Block) canonical.Request {
	return canonical.Request{Messages: []canonical.Message{{Role: canonical.RoleAssistant, Blocks: blocks}}}
}

func taskUse(id string, target taskTarget, items []agentTask) canonical.Block {
	return canonical.Block{
		Type: canonical.BlockToolUse, ToolName: target.name, ToolCallID: id,
		Input: target.buildInput(items),
	}
}

func taskResult(t *testing.T, id string, items []agentTask, isError bool) canonical.Block {
	t.Helper()
	encoded, err := json.Marshal(taskTarget{name: "todowrite", kind: taskTargetOpenCode}.buildInput(items)["todos"])
	if err != nil {
		t.Fatalf("encode task result: %v", err)
	}
	return canonical.Block{Type: canonical.BlockToolResult, ToolCallID: id, Output: string(encoded), IsError: isError}
}

func assertTaskHistory(t *testing.T, got taskHistory, want []agentTask, found, pending, conflict bool) {
	t.Helper()
	if !reflect.DeepEqual(got.Items, want) || got.Found != found || got.Pending != pending || got.Conflict != conflict {
		t.Fatalf("history = %+v, want items=%+v found=%t pending=%t conflict=%t", got, want, found, pending, conflict)
	}
}
