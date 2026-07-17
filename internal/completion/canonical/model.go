// Package canonical defines the dialect-neutral model used by completion APIs.
package canonical

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Dialect string

const (
	DialectOpenAIChat Dialect = "openai_chat"
	DialectAnthropic  Dialect = "anthropic_messages"
	DialectResponses  Dialect = "openai_responses"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type BlockType string

const (
	BlockText       BlockType = "text"
	BlockImage      BlockType = "image"
	BlockToolUse    BlockType = "tool_use"
	BlockToolResult BlockType = "tool_result"
)

type Block struct {
	Type       BlockType `json:"type"`
	Text       string    `json:"text,omitempty"`
	ImageURL   string    `json:"image_url,omitempty"`
	ToolCallID string    `json:"tool_call_id,omitempty"`
	// ToolNamespace is empty for an ordinary function and names the owning
	// Responses namespace for a namespaced function. Namespace and name are a
	// single correctness identity; callers must never match on ToolName alone.
	ToolNamespace string         `json:"tool_namespace,omitempty"`
	ToolName      string         `json:"tool_name,omitempty"`
	Input         map[string]any `json:"input,omitempty"`
	Output        any            `json:"output,omitempty"`
	IsError       bool           `json:"is_error,omitempty"`
}

type Message struct {
	Role   Role    `json:"role"`
	Blocks []Block `json:"blocks"`
}

type Tool struct {
	Namespace   string          `json:"namespace,omitempty"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// ToolCallPolicy is the caller's explicit scheduling contract for one model
// response. An empty value means the dialect did not express a policy.
type ToolCallPolicy string

const (
	ToolCallsSerial   ToolCallPolicy = "serial"
	ToolCallsParallel ToolCallPolicy = "parallel"
)

// HostedCapability records a provider-executed Responses tool without
// presenting it to Human as a caller-executed function. Configuration keeps
// the complete validated wire object so request identity remains exact.
type HostedCapability struct {
	Type          string          `json:"type"`
	Configuration json.RawMessage `json:"configuration"`
}

// OpaqueInput records only the fingerprint of provider state required for exact
// request identity. Raw provider-private bytes are neither projected into the
// human transcript nor delivered/persisted with worker state.
type OpaqueInput struct {
	Type   string `json:"type"`
	SHA256 string `json:"sha256"`
}

type Request struct {
	Dialect            Dialect            `json:"dialect"`
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

func (request Request) Validate() error {
	switch request.Dialect {
	case DialectOpenAIChat, DialectAnthropic, DialectResponses:
	default:
		return fmt.Errorf("unsupported dialect %q", request.Dialect)
	}
	if strings.TrimSpace(request.Model) == "" {
		return errors.New("model is required")
	}
	if len(request.Messages) == 0 {
		return errors.New("at least one message is required")
	}
	switch request.ToolCallPolicy {
	case "", ToolCallsSerial, ToolCallsParallel:
	default:
		return fmt.Errorf("unsupported tool-call policy %q", request.ToolCallPolicy)
	}
	type toolIdentity struct{ namespace, name string }
	toolNames := make(map[toolIdentity]struct{}, len(request.Tools))
	for _, tool := range request.Tools {
		if err := ValidateToolIdentity(tool.Namespace, tool.Name); err != nil {
			return fmt.Errorf("invalid tool identity: %w", err)
		}
		identity := toolIdentity{namespace: tool.Namespace, name: tool.Name}
		if _, exists := toolNames[identity]; exists {
			return fmt.Errorf("duplicate tool %q", QualifiedToolName(tool.Namespace, tool.Name))
		}
		toolNames[identity] = struct{}{}
		if !json.Valid(tool.InputSchema) {
			return fmt.Errorf("tool %q has invalid input schema", QualifiedToolName(tool.Namespace, tool.Name))
		}
	}
	for index, capability := range request.HostedCapabilities {
		if strings.TrimSpace(capability.Type) == "" || capability.Type != strings.TrimSpace(capability.Type) {
			return fmt.Errorf("hosted capability %d has no type", index)
		}
		if !json.Valid(capability.Configuration) {
			return fmt.Errorf("hosted capability %q has invalid configuration", capability.Type)
		}
	}
	for index, item := range request.OpaqueInput {
		if strings.TrimSpace(item.Type) == "" || item.Type != strings.TrimSpace(item.Type) {
			return fmt.Errorf("opaque input %d has no type", index)
		}
		decoded, err := hex.DecodeString(item.SHA256)
		if err != nil || len(decoded) != sha256.Size {
			return fmt.Errorf("opaque input %q has invalid SHA-256 fingerprint", item.Type)
		}
	}
	for messageIndex, message := range request.Messages {
		switch message.Role {
		case RoleSystem, RoleUser, RoleAssistant, RoleTool:
		default:
			return fmt.Errorf("message %d has invalid role %q", messageIndex, message.Role)
		}
		if len(message.Blocks) == 0 {
			return fmt.Errorf("message %d has no content blocks", messageIndex)
		}
		for blockIndex, block := range message.Blocks {
			if err := block.Validate(); err != nil {
				return fmt.Errorf("message %d block %d: %w", messageIndex, blockIndex, err)
			}
		}
	}
	return nil
}

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
			return errors.New("tool use requires id and name")
		}
		if err := ValidateToolIdentity(block.ToolNamespace, block.ToolName); err != nil {
			return fmt.Errorf("invalid tool use identity: %w", err)
		}
	case BlockToolResult:
		if block.ToolCallID == "" {
			return errors.New("tool result requires tool call id")
		}
	default:
		return fmt.Errorf("unsupported block type %q", block.Type)
	}
	return nil
}

// QualifiedToolName is the unambiguous human-facing spelling used by the TUI.
// Responses wire encoding continues to carry namespace and name separately.
func QualifiedToolName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "::" + name
}

// ValidateToolIdentity keeps QualifiedToolName reversible without imposing a
// provider-specific character regex on otherwise valid tool identifiers.
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

func (tool Tool) QualifiedName() string {
	return QualifiedToolName(tool.Namespace, tool.Name)
}

func (block Block) QualifiedToolName() string {
	return QualifiedToolName(block.ToolNamespace, block.ToolName)
}

// Digest returns the stable digest used to detect reuse of an idempotency key
// with a different canonical request.
func (request Request) Digest() (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("marshal canonical request: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func NewOpaqueID(prefix string) (string, error) {
	var raw [18]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return prefix + base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
