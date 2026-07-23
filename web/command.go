package web

import (
	"fmt"
	"strings"

	"github.com/vibe-agi/human/llm"
)

// CommandProfile describes the safe subset needed by the reference Web command
// editor. It only constructs a caller-side tool call; Human never executes the
// command in the worker or browser environment.
type CommandProfile struct {
	ID               string `json:"id"`
	ToolName         string `json:"tool_name"`
	CommandField     string `json:"command_field"`
	DescriptionField string `json:"description_field,omitempty"`
}

func (profile CommandProfile) Validate() error {
	valid := func(value string) bool {
		return value != "" && value == strings.TrimSpace(value) && !strings.Contains(value, "::")
	}
	if strings.TrimSpace(profile.ID) == "" || profile.ID != strings.TrimSpace(profile.ID) ||
		!valid(profile.ToolName) || !valid(profile.CommandField) {
		return fmt.Errorf("web: command profile is incomplete")
	}
	if profile.DescriptionField != "" && !valid(profile.DescriptionField) {
		return fmt.Errorf("web: command description field is invalid")
	}
	fields := []string{profile.CommandField}
	for _, optional := range []string{profile.DescriptionField} {
		if optional == "" {
			continue
		}
		for _, existing := range fields {
			if optional == existing {
				return fmt.Errorf("web: command profile fields must be distinct")
			}
		}
		fields = append(fields, optional)
	}
	return nil
}

// CommandProfileResolver maps an exact authenticated harness request onto the
// reference Web command editor. It has the same borrowing/concurrency contract
// as PlanProfileResolver and fails closed by returning ok=false.
type CommandProfileResolver interface {
	ResolveCommandProfile(llm.TaskContext, llm.Request) (profile CommandProfile, ok bool)
}

type CommandProfileResolverFunc func(llm.TaskContext, llm.Request) (CommandProfile, bool)

func (function CommandProfileResolverFunc) ResolveCommandProfile(
	task llm.TaskContext,
	request llm.Request,
) (CommandProfile, bool) {
	return function(task, request)
}

// OfficialCommandProfiles returns the exact profiles exercised by the real
// client gates. Command tools use the Agent process's current workspace; the
// Human browser never receives or selects a caller filesystem path.
func OfficialCommandProfiles() CommandProfileResolver {
	return CommandProfileResolverFunc(resolveOfficialCommandProfile)
}

func resolveOfficialCommandProfile(task llm.TaskContext, request llm.Request) (CommandProfile, bool) {
	var profile CommandProfile
	switch task.HarnessID + "@" + task.HarnessVersion {
	case "opencode@1.17.18":
		if !requestHasExactTool(request, "bash", openCodeBashSchema) {
			return CommandProfile{}, false
		}
		profile = CommandProfile{
			ID: "opencode@1.17.18/bash", ToolName: "bash", CommandField: "command",
		}
	case "claude-code@2.1.217":
		if !requestHasExactTool(request, "Bash", claudeBashSchema) {
			return CommandProfile{}, false
		}
		profile = CommandProfile{
			ID: "claude-code@2.1.217/Bash", ToolName: "Bash", CommandField: "command",
			DescriptionField: "description",
		}
	case "codex@0.145.0":
		if !requestHasExactTool(request, "exec_command", codexExecCommandSchema) {
			return CommandProfile{}, false
		}
		profile = CommandProfile{
			ID: "codex@0.145.0/exec_command", ToolName: "exec_command", CommandField: "cmd",
		}
	default:
		return CommandProfile{}, false
	}
	if profile.Validate() != nil {
		return CommandProfile{}, false
	}
	return profile, true
}

func nilCommandProfileResolver(resolver CommandProfileResolver) bool {
	return nilInterfaceValue(resolver)
}

const claudeBashSchema = `{
  "$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{
    "command":{"type":"string"},"timeout":{"type":"number"},"description":{"type":"string"},
    "run_in_background":{"type":"boolean"},"dangerouslyDisableSandbox":{"type":"boolean"}
  },"required":["command"],"additionalProperties":false
}`

const openCodeBashSchema = `{
  "$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{
    "command":{"type":"string"},
    "timeout":{"minimum":-9007199254740991,"exclusiveMinimum":0,"type":"integer","maximum":9007199254740991},
    "workdir":{"type":"string"}
  },"required":["command"]
}`

const codexExecCommandSchema = `{
  "type":"object","properties":{
    "cmd":{"type":"string"},"justification":{"type":"string"},"login":{"type":"boolean"},
    "max_output_tokens":{"type":"number"},"prefix_rule":{"type":"array","items":{"type":"string"}},
    "sandbox_permissions":{"type":"string","enum":["use_default","require_escalated"]},
    "shell":{"type":"string"},"tty":{"type":"boolean"},"workdir":{"type":"string"},
    "yield_time_ms":{"type":"number"}
  },"required":["cmd"],"additionalProperties":false
}`
