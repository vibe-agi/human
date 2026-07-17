package tui

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/vibe-agi/human/internal/completion/canonical"
)

const (
	maxAgentTasks       = 100
	maxTaskContentRunes = 4096
)

type agentTaskStatus string

const (
	taskPending    agentTaskStatus = "pending"
	taskInProgress agentTaskStatus = "in_progress"
	taskCompleted  agentTaskStatus = "completed"
	taskCancelled  agentTaskStatus = "cancelled"
)

type agentTask struct {
	Content  string
	Status   agentTaskStatus
	Priority string
}

type taskTargetKind int

const (
	taskTargetOpenCode taskTargetKind = iota
	taskTargetClaude
	taskTargetCodex
)

type taskTarget struct {
	name string
	kind taskTargetKind
}

type taskHistory struct {
	Items    []agentTask
	Found    bool
	Pending  bool
	Conflict bool
}

type taskSchema struct {
	Type                 string                `json:"type"`
	Properties           map[string]taskSchema `json:"properties"`
	Required             []string              `json:"required"`
	Items                *taskSchema           `json:"items"`
	Enum                 []string              `json:"enum"`
	AdditionalProperties json.RawMessage       `json:"additionalProperties"`
	Ref                  string                `json:"$ref"`
	AnyOf                []json.RawMessage     `json:"anyOf"`
	OneOf                []json.RawMessage     `json:"oneOf"`
	AllOf                []json.RawMessage     `json:"allOf"`
}

// taskTargetForRequest enables the structured Tasks pane only for a caller
// tool whose complete JSON shape is understood. The caller still owns the
// tool and its permissions; this matcher only replaces hand-written JSON with
// a purpose-built editor.
func taskTargetForRequest(request canonical.Request) (taskTarget, string) {
	var matches []taskTarget
	var namedError string
	for _, tool := range request.Tools {
		// The built-in Tasks projection is an exact adapter for top-level caller
		// functions. A namespaced function with the same leaf name is a distinct
		// contract and remains available through advanced tool input only.
		if tool.Namespace != "" {
			continue
		}
		kind, candidate := taskKindForName(tool.Name)
		if !candidate {
			continue
		}
		if err := validateTaskToolSchema(tool, kind); err != nil {
			namedError = fmt.Sprintf("Tasks disabled: %s schema is not supported: %v", tool.Name, err)
			continue
		}
		matches = append(matches, taskTarget{name: tool.Name, kind: kind})
	}
	if len(matches) > 1 {
		return taskTarget{}, "Tasks disabled: caller declared multiple compatible task tools"
	}
	if len(matches) == 1 {
		return matches[0], ""
	}
	if namedError != "" {
		return taskTarget{}, namedError + " · use [t] advanced tool input"
	}
	return taskTarget{}, "Tasks unavailable: caller declared no compatible task-list tool"
}

func taskKindForName(name string) (taskTargetKind, bool) {
	switch name {
	case "todowrite":
		return taskTargetOpenCode, true
	case "TodoWrite":
		return taskTargetClaude, true
	case "update_plan":
		return taskTargetCodex, true
	default:
		return 0, false
	}
}

func validateTaskToolSchema(tool canonical.Tool, kind taskTargetKind) error {
	var schema taskSchema
	if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
		return errors.New("invalid JSON schema")
	}
	if schema.Type != "object" {
		return errors.New("root must be an object")
	}
	if err := rejectAmbiguousSchema(schema); err != nil {
		return err
	}
	switch kind {
	case taskTargetOpenCode:
		if err := validateListSchema(schema, "todos", []string{"content", "status", "priority"}); err != nil {
			return err
		}
		item := schema.Properties["todos"].Items
		if err := validateStringEnum(item.Properties["status"], []string{"pending", "in_progress", "completed", "cancelled"}); err != nil {
			return fmt.Errorf("status: %w", err)
		}
		if err := validateStringEnum(item.Properties["priority"], []string{"high", "medium", "low"}); err != nil {
			return fmt.Errorf("priority: %w", err)
		}
		return nil
	case taskTargetClaude:
		if err := validateListSchema(schema, "todos", []string{"content", "status", "activeForm"}); err != nil {
			return err
		}
		return validateStringEnum(schema.Properties["todos"].Items.Properties["status"], []string{
			"pending", "in_progress", "completed",
		})
	case taskTargetCodex:
		if err := requireKnownProperties(schema, []string{"explanation", "plan"}, []string{"plan"}); err != nil {
			return err
		}
		if explanation, ok := schema.Properties["explanation"]; ok && explanation.Type != "string" {
			return errors.New("explanation must be a string")
		}
		if err := validateArrayItems(schema.Properties["plan"], []string{"step", "status"}); err != nil {
			return err
		}
		return validateStringEnum(schema.Properties["plan"].Items.Properties["status"], []string{
			"pending", "in_progress", "completed",
		})
	default:
		return errors.New("unknown task adapter")
	}
}

// An omitted enum accepts every string, including all values the editor emits.
// If a caller supplies an enum, require the exact supported set so a schema
// drift cannot make us emit an invalid value or silently discard a new state.
func validateStringEnum(schema taskSchema, expected []string) error {
	if len(schema.Enum) == 0 {
		return nil
	}
	actual := make(map[string]struct{}, len(schema.Enum))
	for _, value := range schema.Enum {
		actual[value] = struct{}{}
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("unsupported enum %v", schema.Enum)
	}
	for _, value := range expected {
		if _, ok := actual[value]; !ok {
			return fmt.Errorf("unsupported enum %v", schema.Enum)
		}
	}
	return nil
}

func validateListSchema(schema taskSchema, field string, itemFields []string) error {
	if err := requireKnownProperties(schema, []string{field}, []string{field}); err != nil {
		return err
	}
	return validateArrayItems(schema.Properties[field], itemFields)
}

func validateArrayItems(array taskSchema, itemFields []string) error {
	if err := rejectAmbiguousSchema(array); err != nil {
		return err
	}
	if array.Type != "array" || array.Items == nil || array.Items.Type != "object" {
		return errors.New("task list must be an array of objects")
	}
	if err := rejectAmbiguousSchema(*array.Items); err != nil {
		return err
	}
	if err := requireKnownProperties(*array.Items, itemFields, itemFields); err != nil {
		return err
	}
	for _, field := range itemFields {
		if array.Items.Properties[field].Type != "string" {
			return fmt.Errorf("task field %q must be a string", field)
		}
	}
	return nil
}

func requireKnownProperties(schema taskSchema, allowed, required []string) error {
	allowedSet := make(map[string]bool, len(allowed))
	for _, field := range allowed {
		allowedSet[field] = true
	}
	for field := range schema.Properties {
		if !allowedSet[field] {
			return fmt.Errorf("unknown property %q", field)
		}
	}
	requiredSet := make(map[string]bool, len(schema.Required))
	for _, field := range schema.Required {
		if !allowedSet[field] {
			return fmt.Errorf("unknown required field %q", field)
		}
		requiredSet[field] = true
	}
	for _, field := range required {
		if !requiredSet[field] {
			return fmt.Errorf("field %q must be required", field)
		}
		if _, ok := schema.Properties[field]; !ok {
			return fmt.Errorf("required field %q has no property", field)
		}
	}
	return nil
}

func rejectAmbiguousSchema(schema taskSchema) error {
	if schema.Ref != "" || len(schema.AnyOf) > 0 || len(schema.OneOf) > 0 || len(schema.AllOf) > 0 {
		return errors.New("composed or referenced schemas need an explicit adapter")
	}
	if len(schema.AdditionalProperties) == 0 {
		return nil
	}
	var allowed bool
	if err := json.Unmarshal(schema.AdditionalProperties, &allowed); err != nil || allowed {
		return errors.New("additional properties must be absent or false")
	}
	return nil
}

func tasksFromRequest(request canonical.Request, target taskTarget) ([]agentTask, bool, error) {
	history, err := taskHistoryFromRequest(request, target)
	return history.Items, history.Found, err
}

func taskHistoryFromRequest(request canonical.Request, target taskTarget) (taskHistory, error) {
	type pendingCall struct {
		items []agentTask
		order int
	}
	pending := make(map[string]pendingCall)
	var confirmed []agentTask
	confirmedFound := false
	conflict := false
	order := 0
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			switch block.Type {
			case canonical.BlockToolUse:
				if block.ToolNamespace != "" || block.ToolName != target.name {
					continue
				}
				items, err := tasksFromInput(block.Input, target)
				if err != nil {
					return taskHistory{}, fmt.Errorf("read %s call %s: %w", target.name, block.ToolCallID, err)
				}
				order++
				pending[block.ToolCallID] = pendingCall{items: items, order: order}
			case canonical.BlockToolResult:
				call, ok := pending[block.ToolCallID]
				if !ok {
					continue
				}
				delete(pending, block.ToolCallID)
				if block.IsError {
					continue
				}
				if target.kind == taskTargetOpenCode {
					resultItems, err := openCodeTasksFromResult(block.Output)
					if err != nil {
						continue
					}
					if !reflect.DeepEqual(call.items, resultItems) {
						conflict = true
						continue
					}
				}
				confirmed = call.items
				confirmedFound = true
			}
		}
	}

	latestOrder := -1
	var latestPending []agentTask
	for _, call := range pending {
		if call.order > latestOrder {
			latestOrder = call.order
			latestPending = call.items
		}
	}
	if latestOrder >= 0 {
		return taskHistory{Items: latestPending, Found: true, Pending: true, Conflict: conflict}, nil
	}
	return taskHistory{Items: confirmed, Found: confirmedFound, Conflict: conflict}, nil
}

func openCodeTasksFromResult(output any) ([]agentTask, error) {
	var raw []byte
	if text, ok := output.(string); ok {
		raw = []byte(text)
	} else {
		var err error
		raw, err = json.Marshal(output)
		if err != nil {
			return nil, err
		}
	}
	var todos []struct {
		Content  string `json:"content"`
		Status   string `json:"status"`
		Priority string `json:"priority"`
	}
	if err := json.Unmarshal(raw, &todos); err != nil {
		return nil, err
	}
	items := make([]agentTask, 0, len(todos))
	for _, todo := range todos {
		status, err := parseTaskStatus(todo.Status, taskTargetOpenCode)
		if err != nil {
			return nil, err
		}
		priority, err := parsePriority(todo.Priority)
		if err != nil {
			return nil, err
		}
		items = append(items, agentTask{Content: todo.Content, Status: status, Priority: priority})
	}
	return validateTaskItems(items, taskTargetOpenCode)
}

func tasksFromInput(input map[string]any, target taskTarget) ([]agentTask, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	switch target.kind {
	case taskTargetOpenCode:
		if _, ok := input["todos"]; !ok {
			return nil, errors.New("todos is missing")
		}
		var payload struct {
			Todos []struct {
				Content  string `json:"content"`
				Status   string `json:"status"`
				Priority string `json:"priority"`
			} `json:"todos"`
		}
		if err := json.Unmarshal(encoded, &payload); err != nil {
			return nil, err
		}
		items := make([]agentTask, 0, len(payload.Todos))
		for _, todo := range payload.Todos {
			status, err := parseTaskStatus(todo.Status, target.kind)
			if err != nil {
				return nil, err
			}
			priority, err := parsePriority(todo.Priority)
			if err != nil {
				return nil, err
			}
			items = append(items, agentTask{Content: todo.Content, Status: status, Priority: priority})
		}
		return validateTaskItems(items, target.kind)
	case taskTargetClaude:
		if _, ok := input["todos"]; !ok {
			return nil, errors.New("todos is missing")
		}
		var payload struct {
			Todos []struct {
				Content string `json:"content"`
				Status  string `json:"status"`
			} `json:"todos"`
		}
		if err := json.Unmarshal(encoded, &payload); err != nil {
			return nil, err
		}
		items := make([]agentTask, 0, len(payload.Todos))
		for _, todo := range payload.Todos {
			status, err := parseTaskStatus(todo.Status, target.kind)
			if err != nil {
				return nil, err
			}
			items = append(items, agentTask{Content: todo.Content, Status: status, Priority: "medium"})
		}
		return validateTaskItems(items, target.kind)
	case taskTargetCodex:
		if _, ok := input["plan"]; !ok {
			return nil, errors.New("plan is missing")
		}
		var payload struct {
			Plan []struct {
				Step   string `json:"step"`
				Status string `json:"status"`
			} `json:"plan"`
		}
		if err := json.Unmarshal(encoded, &payload); err != nil {
			return nil, err
		}
		items := make([]agentTask, 0, len(payload.Plan))
		for _, item := range payload.Plan {
			status, err := parseTaskStatus(item.Status, target.kind)
			if err != nil {
				return nil, err
			}
			items = append(items, agentTask{Content: item.Step, Status: status, Priority: "medium"})
		}
		return validateTaskItems(items, target.kind)
	default:
		return nil, errors.New("unknown task adapter")
	}
}

func validateTaskItems(items []agentTask, kind taskTargetKind) ([]agentTask, error) {
	if len(items) > maxAgentTasks {
		return nil, fmt.Errorf("task list exceeds %d items", maxAgentTasks)
	}
	inProgress := 0
	for index := range items {
		items[index].Content = strings.TrimSpace(items[index].Content)
		if items[index].Content == "" {
			return nil, fmt.Errorf("task %d has empty content", index+1)
		}
		if utf8.RuneCountInString(items[index].Content) > maxTaskContentRunes {
			return nil, fmt.Errorf("task %d content is too long", index+1)
		}
		if _, err := parseTaskStatus(string(items[index].Status), kind); err != nil {
			return nil, fmt.Errorf("task %d: %w", index+1, err)
		}
		if kind == taskTargetOpenCode {
			if _, err := parsePriority(items[index].Priority); err != nil {
				return nil, fmt.Errorf("task %d: %w", index+1, err)
			}
		}
		if items[index].Status == taskInProgress {
			inProgress++
		}
	}
	if inProgress > 1 {
		return nil, errors.New("at most one task may be in_progress")
	}
	return items, nil
}

func parseTaskStatus(status string, kind taskTargetKind) (agentTaskStatus, error) {
	parsed := agentTaskStatus(status)
	switch parsed {
	case taskPending, taskInProgress, taskCompleted:
		return parsed, nil
	case taskCancelled:
		if kind == taskTargetOpenCode {
			return parsed, nil
		}
	}
	return "", fmt.Errorf("unsupported task status %q", status)
}

func parsePriority(priority string) (string, error) {
	switch priority {
	case "high", "medium", "low":
		return priority, nil
	default:
		return "", fmt.Errorf("unsupported task priority %q", priority)
	}
}

func (target taskTarget) buildInput(items []agentTask) map[string]any {
	switch target.kind {
	case taskTargetOpenCode:
		todos := make([]map[string]any, 0, len(items))
		for _, item := range items {
			todos = append(todos, map[string]any{
				"content": item.Content, "status": string(item.Status), "priority": normalizePriority(item.Priority),
			})
		}
		return map[string]any{"todos": todos}
	case taskTargetClaude:
		todos := make([]map[string]any, 0, len(items))
		for _, item := range items {
			todos = append(todos, map[string]any{
				"content": item.Content, "status": string(item.Status), "activeForm": item.Content,
			})
		}
		return map[string]any{"todos": todos}
	case taskTargetCodex:
		plan := make([]map[string]any, 0, len(items))
		for _, item := range items {
			plan = append(plan, map[string]any{"step": item.Content, "status": string(item.Status)})
		}
		// explanation is optional in Codex's contract. Omitting it keeps the
		// adapter valid if a compatible client exposes only the required plan.
		return map[string]any{"plan": plan}
	default:
		return map[string]any{}
	}
}

func normalizePriority(priority string) string {
	if parsed, err := parsePriority(priority); err == nil {
		return parsed
	}
	return "medium"
}
