package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/vibe-agi/human/llm"
)

// PlanKind identifies the interaction model rendered by the reference Web UI.
// List profiles replace the caller's complete plan in one tool call. Task
// lifecycle profiles create and update individually identified caller tasks.
type PlanKind string

const (
	PlanKindList          PlanKind = "list"
	PlanKindTaskLifecycle PlanKind = "task_lifecycle"
)

// PlanProfile is the normalized browser contract selected from one exact
// harness request. It contains no authority: the HumanLLM core still checks the
// submitted call against the caller's declaration and ToolAuthorizer.
type PlanProfile struct {
	ID              string   `json:"id"`
	Kind            PlanKind `json:"kind"`
	ToolName        string   `json:"tool_name,omitempty"`
	ItemsField      string   `json:"items_field,omitempty"`
	ContentField    string   `json:"content_field,omitempty"`
	StatusField     string   `json:"status_field,omitempty"`
	PriorityField   string   `json:"priority_field,omitempty"`
	DefaultPriority string   `json:"default_priority,omitempty"`
	CreateTool      string   `json:"create_tool,omitempty"`
	UpdateTool      string   `json:"update_tool,omitempty"`
	ListTool        string   `json:"list_tool,omitempty"`
}

// Validate rejects ambiguous UI profiles. A custom resolver may return only
// these closed, generic interaction shapes; products needing different task
// semantics can replace the Web human-side implementation entirely.
func (profile PlanProfile) Validate() error {
	if strings.TrimSpace(profile.ID) == "" || profile.ID != strings.TrimSpace(profile.ID) {
		return fmt.Errorf("web: plan profile id is required")
	}
	validField := func(value string) bool {
		return value != "" && value == strings.TrimSpace(value) && !strings.Contains(value, "::")
	}
	switch profile.Kind {
	case PlanKindList:
		if !validField(profile.ToolName) || !validField(profile.ItemsField) ||
			!validField(profile.ContentField) || !validField(profile.StatusField) ||
			profile.CreateTool != "" || profile.UpdateTool != "" || profile.ListTool != "" {
			return fmt.Errorf("web: list plan profile is incomplete")
		}
		if profile.PriorityField == "" && profile.DefaultPriority != "" {
			return fmt.Errorf("web: default priority requires a priority field")
		}
		if profile.PriorityField != "" && !validField(profile.PriorityField) {
			return fmt.Errorf("web: list plan priority field is invalid")
		}
	case PlanKindTaskLifecycle:
		if !validField(profile.CreateTool) || !validField(profile.UpdateTool) || !validField(profile.ListTool) ||
			profile.ToolName != "" || profile.ItemsField != "" || profile.ContentField != "" ||
			profile.StatusField != "" || profile.PriorityField != "" || profile.DefaultPriority != "" {
			return fmt.Errorf("web: task lifecycle plan profile is incomplete")
		}
	default:
		return fmt.Errorf("web: unsupported plan profile kind %q", profile.Kind)
	}
	return nil
}

// PlanProfileResolver maps an exact authenticated harness task plus its current
// caller-declared tools onto a normalized Web plan surface. Implementations are
// borrowed for the Server lifetime, called concurrently, and must not mutate or
// retain the input values. Returning ok=false makes the dedicated panel fail
// closed while leaving the generic declared-tool editor available.
type PlanProfileResolver interface {
	ResolvePlanProfile(llm.TaskContext, llm.Request) (profile PlanProfile, ok bool)
}

type PlanProfileResolverFunc func(llm.TaskContext, llm.Request) (PlanProfile, bool)

func (function PlanProfileResolverFunc) ResolvePlanProfile(
	task llm.TaskContext,
	request llm.Request,
) (PlanProfile, bool) {
	return function(task, request)
}

// OfficialPlanProfiles returns the exact profiles proven by the repository's
// real Claude Code, OpenCode, and Codex container gates. Behavioral JSON Schema
// is matched exactly after removing annotations such as descriptions; a new
// field, constraint, or enum value therefore fails closed until reviewed.
func OfficialPlanProfiles() PlanProfileResolver {
	return PlanProfileResolverFunc(resolveOfficialPlanProfile)
}

func resolveOfficialPlanProfile(task llm.TaskContext, request llm.Request) (PlanProfile, bool) {
	var profile PlanProfile
	switch task.HarnessID + "@" + task.HarnessVersion {
	case "opencode@1.17.18":
		if !requestHasExactTool(request, "todowrite", openCodeTodoSchema) {
			return PlanProfile{}, false
		}
		profile = PlanProfile{
			ID: "opencode@1.17.18/todowrite", Kind: PlanKindList, ToolName: "todowrite",
			ItemsField: "todos", ContentField: "content", StatusField: "status",
			PriorityField: "priority", DefaultPriority: "medium",
		}
	case "codex@0.145.0":
		if !requestHasExactTool(request, "update_plan", codexPlanSchema) {
			return PlanProfile{}, false
		}
		profile = PlanProfile{
			ID: "codex@0.145.0/update_plan", Kind: PlanKindList, ToolName: "update_plan",
			ItemsField: "plan", ContentField: "step", StatusField: "status",
		}
	case "claude-code@2.1.217":
		if !requestHasExactTool(request, "TaskCreate", claudeTaskCreateSchema) ||
			!requestHasExactTool(request, "TaskUpdate", claudeTaskUpdateSchema) ||
			!requestHasExactTool(request, "TaskList", claudeTaskListSchema) {
			return PlanProfile{}, false
		}
		profile = PlanProfile{
			ID: "claude-code@2.1.217/tasks", Kind: PlanKindTaskLifecycle,
			CreateTool: "TaskCreate", UpdateTool: "TaskUpdate", ListTool: "TaskList",
		}
	default:
		return PlanProfile{}, false
	}
	if profile.Validate() != nil {
		return PlanProfile{}, false
	}
	return profile, true
}

func requestHasExactTool(request llm.Request, name, expected string) bool {
	found := false
	for _, tool := range request.Tools {
		if tool.Namespace != "" || tool.Name != name {
			continue
		}
		if found || !behavioralSchemaEqual(tool.InputSchema, []byte(expected)) {
			return false
		}
		found = true
	}
	return found
}

func behavioralSchemaEqual(actual, expected []byte) bool {
	decode := func(encoded []byte) (any, bool) {
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, false
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return nil, false
		}
		return normalizeBehavioralSchema(value, ""), true
	}
	left, leftOK := decode(actual)
	right, rightOK := decode(expected)
	return leftOK && rightOK && reflect.DeepEqual(left, right)
}

func normalizeBehavioralSchema(value any, parent string) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, child := range typed {
			switch key {
			case "$schema", "description", "title", "default", "examples":
				continue
			default:
				result[key] = normalizeBehavioralSchema(child, key)
			}
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index := range typed {
			result[index] = normalizeBehavioralSchema(typed[index], parent)
		}
		if parent == "required" || parent == "enum" || parent == "anyOf" || parent == "oneOf" || parent == "allOf" {
			sort.Slice(result, func(left, right int) bool {
				leftJSON, _ := json.Marshal(result[left])
				rightJSON, _ := json.Marshal(result[right])
				return bytes.Compare(leftJSON, rightJSON) < 0
			})
		}
		return result
	default:
		return value
	}
}

func nilPlanProfileResolver(resolver PlanProfileResolver) bool {
	return nilInterfaceValue(resolver)
}

func nilInterfaceValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

const openCodeTodoSchema = `{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "type":"object",
  "properties":{"todos":{"type":"array","items":{"type":"object","properties":{
    "content":{"type":"string"},"status":{"type":"string"},"priority":{"type":"string"}
  },"required":["content","status","priority"]}}},
  "required":["todos"]
}`

const codexPlanSchema = `{
  "type":"object","properties":{
    "explanation":{"type":"string"},
    "plan":{"type":"array","items":{"type":"object","properties":{
      "status":{"type":"string","enum":["pending","in_progress","completed"]},
      "step":{"type":"string"}
    },"required":["step","status"],"additionalProperties":false}}
  },"required":["plan"],"additionalProperties":false
}`

const claudeTaskCreateSchema = `{
  "$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{
    "subject":{"type":"string"},"description":{"type":"string"},"activeForm":{"type":"string"},
    "metadata":{"type":"object","propertyNames":{"type":"string"},"additionalProperties":{}}
  },"required":["subject","description"],"additionalProperties":false
}`

const claudeTaskUpdateSchema = `{
  "$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{
    "taskId":{"type":"string"},"subject":{"type":"string"},"description":{"type":"string"},
    "activeForm":{"type":"string"},
    "status":{"anyOf":[{"type":"string","enum":["pending","in_progress","completed"]},{"type":"string","const":"deleted"}]},
    "addBlocks":{"type":"array","items":{"type":"string"}},
    "addBlockedBy":{"type":"array","items":{"type":"string"}},"owner":{"type":"string"},
    "metadata":{"type":"object","propertyNames":{"type":"string"},"additionalProperties":{}}
  },"required":["taskId"],"additionalProperties":false
}`

const claudeTaskListSchema = `{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "type":"object","properties":{},"additionalProperties":false
}`
