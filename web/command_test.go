package web

import (
	"encoding/json"
	"testing"

	"github.com/vibe-agi/human/llm"
)

func TestOfficialCommandProfilesRequireExactHarnessAndBehavioralSchema(t *testing.T) {
	t.Parallel()
	resolver := OfficialCommandProfiles()
	tests := []struct {
		name        string
		task        llm.TaskContext
		tool        llm.Tool
		wantID      string
		wantTool    string
		wantCommand string
	}{
		{
			name: "claude", task: llm.TaskContext{HarnessID: "claude-code", HarnessVersion: "2.1.217"},
			tool:   testPlanTool("Bash", claudeBashSchema),
			wantID: "claude-code@2.1.217/Bash", wantTool: "Bash", wantCommand: "command",
		},
		{
			name: "opencode", task: llm.TaskContext{HarnessID: "opencode", HarnessVersion: "1.17.18"},
			tool:   testPlanTool("bash", openCodeBashSchema),
			wantID: "opencode@1.17.18/bash", wantTool: "bash", wantCommand: "command",
		},
		{
			name: "codex", task: llm.TaskContext{HarnessID: "codex", HarnessVersion: "0.145.0"},
			tool:   testPlanTool("exec_command", codexExecCommandSchema),
			wantID: "codex@0.145.0/exec_command", wantTool: "exec_command", wantCommand: "cmd",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			profile, ok := resolver.ResolveCommandProfile(test.task, llm.Request{Tools: []llm.Tool{test.tool}})
			if !ok || profile.ID != test.wantID || profile.ToolName != test.wantTool ||
				profile.CommandField != test.wantCommand || profile.Validate() != nil {
				t.Fatalf("profile = %#v, ok=%t", profile, ok)
			}
		})
	}
}

func TestOfficialCommandProfilesIgnoreAnnotationsButFailClosedOnDrift(t *testing.T) {
	t.Parallel()
	resolver := OfficialCommandProfiles()
	task := llm.TaskContext{HarnessID: "codex", HarnessVersion: "0.145.0"}

	var annotated map[string]any
	if err := json.Unmarshal([]byte(codexExecCommandSchema), &annotated); err != nil {
		t.Fatal(err)
	}
	annotated["description"] = "annotation-only change"
	properties := annotated["properties"].(map[string]any)
	properties["cmd"].(map[string]any)["description"] = "annotation-only field change"
	encoded, err := json.Marshal(annotated)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := resolver.ResolveCommandProfile(task, llm.Request{Tools: []llm.Tool{{
		Name: "exec_command", InputSchema: encoded,
	}}}); !ok {
		t.Fatal("description-only schema change disabled the exact command profile")
	}

	properties["cmd"].(map[string]any)["minLength"] = float64(1)
	drifted, err := json.Marshal(annotated)
	if err != nil {
		t.Fatal(err)
	}
	if profile, ok := resolver.ResolveCommandProfile(task, llm.Request{Tools: []llm.Tool{{
		Name: "exec_command", InputSchema: drifted,
	}}}); ok {
		t.Fatalf("behavioral schema drift selected profile %#v", profile)
	}

	wrongVersion := task
	wrongVersion.HarnessVersion = "0.145.1"
	if profile, ok := resolver.ResolveCommandProfile(wrongVersion, llm.Request{Tools: []llm.Tool{
		testPlanTool("exec_command", codexExecCommandSchema),
	}}); ok {
		t.Fatalf("unknown harness version selected profile %#v", profile)
	}
}

func TestCommandProfileValidation(t *testing.T) {
	t.Parallel()
	valid := CommandProfile{
		ID: "custom@1/exec", ToolName: "run", CommandField: "script",
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.CommandField = ""
	if err := invalid.Validate(); err == nil {
		t.Fatal("command profile without a command field was accepted")
	}
	invalid = valid
	invalid.DescriptionField = invalid.CommandField
	if err := invalid.Validate(); err == nil {
		t.Fatal("command profile with colliding fields was accepted")
	}
}
