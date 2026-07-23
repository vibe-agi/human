package web

import (
	"encoding/json"
	"testing"

	"github.com/vibe-agi/human/llm"
)

func TestOfficialPlanProfilesRequireExactHarnessAndBehavioralSchemas(t *testing.T) {
	t.Parallel()
	resolver := OfficialPlanProfiles()
	tests := []struct {
		name     string
		task     llm.TaskContext
		tools    []llm.Tool
		wantID   string
		wantKind PlanKind
	}{
		{
			name: "opencode", task: llm.TaskContext{HarnessID: "opencode", HarnessVersion: "1.17.18"},
			tools:  []llm.Tool{testPlanTool("todowrite", openCodeTodoSchema)},
			wantID: "opencode@1.17.18/todowrite", wantKind: PlanKindList,
		},
		{
			name: "codex", task: llm.TaskContext{HarnessID: "codex", HarnessVersion: "0.145.0"},
			tools:  []llm.Tool{testPlanTool("update_plan", codexPlanSchema)},
			wantID: "codex@0.145.0/update_plan", wantKind: PlanKindList,
		},
		{
			name: "claude", task: llm.TaskContext{HarnessID: "claude-code", HarnessVersion: "2.1.217"},
			tools: []llm.Tool{
				testPlanTool("TaskList", claudeTaskListSchema),
				testPlanTool("TaskCreate", claudeTaskCreateSchema),
				testPlanTool("TaskUpdate", claudeTaskUpdateSchema),
			},
			wantID: "claude-code@2.1.217/tasks", wantKind: PlanKindTaskLifecycle,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			profile, ok := resolver.ResolvePlanProfile(test.task, llm.Request{Tools: test.tools})
			if !ok || profile.ID != test.wantID || profile.Kind != test.wantKind || profile.Validate() != nil {
				t.Fatalf("profile = %#v, ok=%t", profile, ok)
			}
		})
	}
}

func TestOfficialPlanProfilesIgnoreAnnotationsButFailClosedOnSchemaDrift(t *testing.T) {
	t.Parallel()
	resolver := OfficialPlanProfiles()
	task := llm.TaskContext{HarnessID: "codex", HarnessVersion: "0.145.0"}

	var annotated map[string]any
	if err := json.Unmarshal([]byte(codexPlanSchema), &annotated); err != nil {
		t.Fatal(err)
	}
	annotated["description"] = "wording may change without changing behavior"
	properties := annotated["properties"].(map[string]any)
	properties["plan"].(map[string]any)["description"] = "another annotation"
	encoded, err := json.Marshal(annotated)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := resolver.ResolvePlanProfile(task, llm.Request{Tools: []llm.Tool{{
		Name: "update_plan", InputSchema: encoded,
	}}}); !ok {
		t.Fatal("description-only schema change disabled the exact profile")
	}

	properties["plan"].(map[string]any)["minItems"] = float64(1)
	drifted, err := json.Marshal(annotated)
	if err != nil {
		t.Fatal(err)
	}
	if profile, ok := resolver.ResolvePlanProfile(task, llm.Request{Tools: []llm.Tool{{
		Name: "update_plan", InputSchema: drifted,
	}}}); ok {
		t.Fatalf("behavioral schema drift selected profile %#v", profile)
	}

	wrongVersion := task
	wrongVersion.HarnessVersion = "0.145.1"
	if profile, ok := resolver.ResolvePlanProfile(wrongVersion, llm.Request{Tools: []llm.Tool{
		testPlanTool("update_plan", codexPlanSchema),
	}}); ok {
		t.Fatalf("unknown harness version selected profile %#v", profile)
	}
}

func TestPlanProfileValidation(t *testing.T) {
	t.Parallel()
	valid := PlanProfile{
		ID: "custom@1/plan", Kind: PlanKindList, ToolName: "sync_plan",
		ItemsField: "items", ContentField: "text", StatusField: "state",
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.CreateTool = "create"
	if err := invalid.Validate(); err == nil {
		t.Fatal("mixed list/lifecycle profile was accepted")
	}
}

func testPlanTool(name, schema string) llm.Tool {
	return llm.Tool{Name: name, InputSchema: json.RawMessage(schema)}
}
