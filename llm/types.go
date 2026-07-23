// Package llm defines the public, dialect-neutral contracts used by HumanLLM.
//
// The package deliberately owns its canonical message and event types. A
// custom codec can therefore implement the public ports without importing an
// internal package or depending on a built-in OpenAI/Anthropic dialect.
package llm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Role is the canonical author of a message after a wire request is decoded.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// BlockType identifies one canonical message content block.
type BlockType string

const (
	BlockText       BlockType = "text"
	BlockImage      BlockType = "image"
	BlockToolUse    BlockType = "tool_use"
	BlockToolResult BlockType = "tool_result"
)

// Block is a dialect-neutral message content block.
type Block struct {
	Type       BlockType `json:"type"`
	Text       string    `json:"text,omitempty"`
	ImageURL   string    `json:"image_url,omitempty"`
	ToolCallID string    `json:"tool_call_id,omitempty"`

	// ToolNamespace and ToolName form one correctness identity. Consumers must
	// never match a namespaced tool on Name alone.
	ToolNamespace string         `json:"tool_namespace,omitempty"`
	ToolName      string         `json:"tool_name,omitempty"`
	Input         map[string]any `json:"input,omitempty"`
	// TextInput carries the opaque freeform payload of a text-input tool call.
	// It is a pointer so an intentionally empty payload remains distinguishable
	// from a JSON-input tool call.
	TextInput *string `json:"text_input,omitempty"`
	Output    any     `json:"output,omitempty"`
	IsError   bool    `json:"is_error,omitempty"`
}

// Message is one canonical conversation message.
type Message struct {
	Role   Role    `json:"role"`
	Blocks []Block `json:"blocks"`
}

// ToolInputKind identifies how a caller-executed tool receives its input.
// The zero value is JSON for backwards compatibility with function tools.
type ToolInputKind string

const (
	ToolInputJSON ToolInputKind = ""
	ToolInputText ToolInputKind = "text"
)

// Tool is a caller-executed operation made available to HumanLLM. JSON-input
// tools map to ordinary OpenAI/Anthropic functions. Text-input tools map to
// Responses custom tools such as Codex's freeform apply_patch.
type Tool struct {
	Namespace   string          `json:"namespace,omitempty"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputKind   ToolInputKind   `json:"input_kind,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
	// InputFormat preserves a provider's validated text format contract (for
	// example a Responses grammar object) as part of request identity.
	InputFormat json.RawMessage `json:"input_format,omitempty"`
}

// ToolCallPolicy is the caller's explicit scheduling contract for one model
// response. An empty value means the wire protocol did not express a policy.
type ToolCallPolicy string

const (
	// ToolCallsDisabled means the caller explicitly prohibited tool use for this
	// response, even if its provider request carried tool definitions.
	ToolCallsDisabled ToolCallPolicy = "disabled"
	ToolCallsSerial   ToolCallPolicy = "serial"
	ToolCallsParallel ToolCallPolicy = "parallel"
)

// HostedCapability records a provider-executed tool that must participate in
// exact request identity but is not offered as a caller-executed function.
type HostedCapability struct {
	Type          string          `json:"type"`
	Configuration json.RawMessage `json:"configuration"`
}

// OpaqueInput records a fingerprint of provider-private request state. The raw
// private bytes are not part of the Human transcript.
type OpaqueInput struct {
	Type   string `json:"type"`
	SHA256 string `json:"sha256"`
}

// Request is the canonical output of Codec.Decode. Codec identity is not a
// field of Request: the runtime must bind the request to the negotiated codec
// ID, version, fingerprint, and contract version in its persistence record.
type Request struct {
	Model              string             `json:"model"`
	Stream             bool               `json:"stream"`
	System             string             `json:"system,omitempty"`
	Messages           []Message          `json:"messages"`
	Tools              []Tool             `json:"tools,omitempty"`
	ToolCallPolicy     ToolCallPolicy     `json:"tool_call_policy,omitempty"`
	HostedCapabilities []HostedCapability `json:"hosted_capabilities,omitempty"`
	OpaqueInput        []OpaqueInput      `json:"opaque_input,omitempty"`
	Metadata           map[string]string  `json:"metadata,omitempty"`
}

// Validate checks the dialect-neutral invariants required by HumanLLM core.
// A codec may impose additional wire-specific constraints during Decode.
func (request Request) Validate() error {
	if strings.TrimSpace(request.Model) == "" {
		return errors.New("llm: model is required")
	}
	if len(request.Messages) == 0 {
		return errors.New("llm: at least one message is required")
	}
	switch request.ToolCallPolicy {
	case "", ToolCallsDisabled, ToolCallsSerial, ToolCallsParallel:
	default:
		return fmt.Errorf("llm: unsupported tool-call policy %q", request.ToolCallPolicy)
	}

	type toolIdentity struct{ namespace, name string }
	toolNames := make(map[toolIdentity]struct{}, len(request.Tools))
	for _, tool := range request.Tools {
		if err := ValidateToolIdentity(tool.Namespace, tool.Name); err != nil {
			return fmt.Errorf("llm: invalid tool identity: %w", err)
		}
		identity := toolIdentity{namespace: tool.Namespace, name: tool.Name}
		if _, exists := toolNames[identity]; exists {
			return fmt.Errorf("llm: duplicate tool %q", QualifiedToolName(tool.Namespace, tool.Name))
		}
		toolNames[identity] = struct{}{}
		switch tool.InputKind {
		case ToolInputJSON:
			if !json.Valid(tool.InputSchema) {
				return fmt.Errorf("llm: tool %q has invalid input schema", tool.QualifiedName())
			}
			if len(tool.InputFormat) != 0 {
				return fmt.Errorf("llm: JSON-input tool %q has a text input format", tool.QualifiedName())
			}
		case ToolInputText:
			if len(tool.InputSchema) != 0 {
				return fmt.Errorf("llm: text-input tool %q has an input schema", tool.QualifiedName())
			}
			if len(tool.InputFormat) != 0 && !json.Valid(tool.InputFormat) {
				return fmt.Errorf("llm: text-input tool %q has invalid input format", tool.QualifiedName())
			}
		default:
			return fmt.Errorf("llm: tool %q has unsupported input kind %q", tool.QualifiedName(), tool.InputKind)
		}
	}
	for index, capability := range request.HostedCapabilities {
		if !validTrimmed(capability.Type) {
			return fmt.Errorf("llm: hosted capability %d has invalid type", index)
		}
		if !json.Valid(capability.Configuration) {
			return fmt.Errorf("llm: hosted capability %q has invalid configuration", capability.Type)
		}
	}
	for index, item := range request.OpaqueInput {
		if !validTrimmed(item.Type) {
			return fmt.Errorf("llm: opaque input %d has invalid type", index)
		}
		decoded, err := hex.DecodeString(item.SHA256)
		if err != nil || len(decoded) != sha256.Size {
			return fmt.Errorf("llm: opaque input %q has invalid SHA-256 fingerprint", item.Type)
		}
	}
	for messageIndex, message := range request.Messages {
		switch message.Role {
		case RoleSystem, RoleUser, RoleAssistant, RoleTool:
		default:
			return fmt.Errorf("llm: message %d has invalid role %q", messageIndex, message.Role)
		}
		if len(message.Blocks) == 0 {
			return fmt.Errorf("llm: message %d has no content blocks", messageIndex)
		}
		for blockIndex, block := range message.Blocks {
			if err := block.Validate(); err != nil {
				return fmt.Errorf("llm: message %d block %d: %w", messageIndex, blockIndex, err)
			}
		}
	}
	return nil
}

// Digest returns the stable digest of the canonical request. Persistence and
// idempotency code must additionally bind this digest to the negotiated codec
// identity because the same wire bytes may decode differently under another
// codec version.
func (request Request) Digest() (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("llm: marshal canonical request: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// Validate checks one canonical block.
func (block Block) Validate() error {
	switch block.Type {
	case BlockText:
		if block.Text == "" {
			return errors.New("text block is empty")
		}
	case BlockImage:
		if strings.TrimSpace(block.ImageURL) == "" {
			return errors.New("image URL is required")
		}
	case BlockToolUse:
		if block.ToolCallID == "" {
			return errors.New("tool use requires an id")
		}
		if err := ValidateToolIdentity(block.ToolNamespace, block.ToolName); err != nil {
			return fmt.Errorf("invalid tool use identity: %w", err)
		}
		if block.Input != nil && block.TextInput != nil {
			return errors.New("tool use cannot have both JSON and text input")
		}
	case BlockToolResult:
		if block.ToolCallID == "" {
			return errors.New("tool result requires a tool call id")
		}
	default:
		return fmt.Errorf("unsupported block type %q", block.Type)
	}
	return nil
}

// QualifiedToolName returns the reversible human-facing spelling of a tool.
func QualifiedToolName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "::" + name
}

// ValidateToolIdentity ensures QualifiedToolName remains reversible without
// imposing a provider-specific character set.
func ValidateToolIdentity(namespace, name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("tool name is required")
	}
	if name != strings.TrimSpace(name) {
		return errors.New("tool name has surrounding whitespace")
	}
	if namespace != strings.TrimSpace(namespace) {
		return errors.New("tool namespace has surrounding whitespace")
	}
	if strings.Contains(namespace, "::") || strings.Contains(name, "::") {
		return errors.New(`tool namespace and name cannot contain "::"`)
	}
	return nil
}

// QualifiedName returns the complete tool identity.
func (tool Tool) QualifiedName() string {
	return QualifiedToolName(tool.Namespace, tool.Name)
}

// QualifiedToolName returns the complete identity of a tool-use block.
func (block Block) QualifiedToolName() string {
	return QualifiedToolName(block.ToolNamespace, block.ToolName)
}

// EventType is a canonical Human response event.
type EventType string

const (
	EventAccepted      EventType = "accepted"
	EventProgress      EventType = "progress"
	EventFinal         EventType = "final"
	EventClarification EventType = "clarification"
	EventToolCalls     EventType = "tool_calls"
	EventRejected      EventType = "rejected"
	EventExpired       EventType = "expired"
	EventFailed        EventType = "failed"
	EventUnavailable   EventType = "unavailable"
)

// ToolCall requests one caller-side tool invocation.
type ToolCall struct {
	ID        string         `json:"id"`
	Namespace string         `json:"namespace,omitempty"`
	Name      string         `json:"name"`
	Input     map[string]any `json:"input,omitempty"`
	TextInput *string        `json:"text_input,omitempty"`
}

// QualifiedName returns the complete tool identity.
func (call ToolCall) QualifiedName() string {
	return QualifiedToolName(call.Namespace, call.Name)
}

// Event is the dialect-neutral input consumed by an Encoder.
type Event struct {
	ID        string     `json:"id,omitempty"`
	Type      EventType  `json:"type"`
	WorkerID  string     `json:"worker_id,omitempty"`
	Text      string     `json:"text,omitempty"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	ErrorCode string     `json:"error_code,omitempty"`
	Error     string     `json:"error,omitempty"`
}

// EndsResponse reports whether the event is terminal for this model response.
func (event Event) EndsResponse() bool {
	switch event.Type {
	case EventFinal, EventClarification, EventToolCalls, EventRejected,
		EventExpired, EventFailed, EventUnavailable:
		return true
	default:
		return false
	}
}

func validTrimmed(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}
