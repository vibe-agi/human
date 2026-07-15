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
	Type       BlockType      `json:"type"`
	Text       string         `json:"text,omitempty"`
	ImageURL   string         `json:"image_url,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	ToolName   string         `json:"tool_name,omitempty"`
	Input      map[string]any `json:"input,omitempty"`
	Output     any            `json:"output,omitempty"`
	IsError    bool           `json:"is_error,omitempty"`
}

type Message struct {
	Role   Role    `json:"role"`
	Blocks []Block `json:"blocks"`
}

type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type Request struct {
	Dialect  Dialect           `json:"dialect"`
	Model    string            `json:"model"`
	Stream   bool              `json:"stream"`
	System   string            `json:"system,omitempty"`
	Messages []Message         `json:"messages"`
	Tools    []Tool            `json:"tools,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
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
	toolNames := make(map[string]struct{}, len(request.Tools))
	for _, tool := range request.Tools {
		if strings.TrimSpace(tool.Name) == "" {
			return errors.New("tool name is required")
		}
		if _, exists := toolNames[tool.Name]; exists {
			return fmt.Errorf("duplicate tool %q", tool.Name)
		}
		toolNames[tool.Name] = struct{}{}
		if !json.Valid(tool.InputSchema) {
			return fmt.Errorf("tool %q has invalid input schema", tool.Name)
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
		if block.ToolCallID == "" || block.ToolName == "" {
			return errors.New("tool use requires id and name")
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
