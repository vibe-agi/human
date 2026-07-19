// Package openai implements the OpenAI Chat Completions wire dialect.
package openai

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

type Codec struct {
	now func() time.Time
}

var _ dialect.Codec = Codec{}

func New() Codec {
	return Codec{now: time.Now}
}

func (Codec) Dialect() canonical.Dialect {
	return canonical.DialectOpenAIChat
}

type chatRequest struct {
	Model             string          `json:"model"`
	Stream            bool            `json:"stream"`
	Messages          []chatMessage   `json:"messages"`
	Tools             []chatTool      `json:"tools"`
	ToolChoice        json.RawMessage `json:"tool_choice"`
	ResponseFormat    json.RawMessage `json:"response_format"`
	ParallelToolCalls *bool           `json:"parallel_tool_calls"`
}

type chatMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCallID string          `json:"tool_call_id"`
	ToolCalls  []chatToolCall  `json:"tool_calls"`
}

type chatTool struct {
	Type     string       `json:"type"`
	Function chatFunction `json:"function"`
}

type chatFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type chatToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func (Codec) Decode(payload []byte) (canonical.Request, error) {
	var wire chatRequest
	if err := dialect.DecodeJSON(payload, &wire); err != nil {
		return canonical.Request{}, fmt.Errorf("decode OpenAI chat request: %w", err)
	}
	if err := validateToolChoice(wire.ToolChoice); err != nil {
		return canonical.Request{}, err
	}
	if err := validateResponseFormat(wire.ResponseFormat); err != nil {
		return canonical.Request{}, err
	}
	if wire.ParallelToolCalls != nil && !*wire.ParallelToolCalls {
		return canonical.Request{}, errors.New("parallel_tool_calls=false is not supported")
	}
	request := canonical.Request{
		Dialect: canonical.DialectOpenAIChat,
		Model:   wire.Model,
		Stream:  wire.Stream,
	}
	for index, message := range wire.Messages {
		role, err := parseRole(message.Role)
		if err != nil {
			return canonical.Request{}, fmt.Errorf("message %d: %w", index, err)
		}
		blocks, err := parseContent(message.Content)
		if err != nil {
			return canonical.Request{}, fmt.Errorf("message %d content: %w", index, err)
		}
		if role == canonical.RoleTool {
			if message.ToolCallID == "" {
				return canonical.Request{}, fmt.Errorf("message %d: tool_call_id is required", index)
			}
			var output any
			if len(blocks) == 1 && blocks[0].Type == canonical.BlockText {
				output = blocks[0].Text
			} else {
				output = blocks
			}
			blocks = []canonical.Block{{Type: canonical.BlockToolResult, ToolCallID: message.ToolCallID, Output: output}}
		}
		for callIndex, call := range message.ToolCalls {
			if call.Type != "" && call.Type != "function" {
				return canonical.Request{}, fmt.Errorf("message %d tool call %d: unsupported type %q", index, callIndex, call.Type)
			}
			input, err := decodeToolArguments(call.Function.Arguments)
			if err != nil {
				return canonical.Request{}, fmt.Errorf("message %d tool call %d arguments: %w", index, callIndex, err)
			}
			blocks = append(blocks, canonical.Block{
				Type: canonical.BlockToolUse, ToolCallID: call.ID,
				ToolName: call.Function.Name, Input: input,
			})
		}
		if role == canonical.RoleSystem {
			for _, block := range blocks {
				if block.Type != canonical.BlockText {
					return canonical.Request{}, fmt.Errorf("message %d: system content must be text", index)
				}
				if request.System != "" {
					request.System += "\n"
				}
				request.System += block.Text
			}
			continue
		}
		// Model APIs may include an explicit empty assistant turn (notably after
		// an empty final). It carries no canonical information; dropping it keeps
		// the next full-history request valid without weakening user validation.
		if role == canonical.RoleAssistant && len(blocks) == 0 {
			continue
		}
		request.Messages = append(request.Messages, canonical.Message{Role: role, Blocks: blocks})
	}
	for index, tool := range wire.Tools {
		if tool.Type != "function" {
			return canonical.Request{}, fmt.Errorf("tool %d: unsupported type %q", index, tool.Type)
		}
		request.Tools = append(request.Tools, canonical.Tool{
			Name: tool.Function.Name, Description: tool.Function.Description,
			InputSchema: objectSchemaOrDefault(tool.Function.Parameters),
		})
	}
	if err := request.Validate(); err != nil {
		return canonical.Request{}, err
	}
	return request, nil
}

func validateToolChoice(raw json.RawMessage) error {
	if isNullJSON(raw) {
		return nil
	}
	var choice string
	if err := json.Unmarshal(raw, &choice); err != nil || choice != "auto" {
		return errors.New("tool_choice is unsupported; omit it or use \"auto\"")
	}
	return nil
}

func validateResponseFormat(raw json.RawMessage) error {
	if isNullJSON(raw) {
		return nil
	}
	var format struct {
		Type string `json:"type"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&format); err != nil {
		return fmt.Errorf("response_format: %w", err)
	}
	if format.Type != "text" {
		return errors.New("structured response_format is not supported; omit it or use type \"text\"")
	}
	return nil
}

func decodeToolArguments(value string) (map[string]any, error) {
	if strings.TrimSpace(value) == "" || strings.TrimSpace(value) == "null" {
		return map[string]any{}, nil
	}
	var input map[string]any
	if err := dialect.DecodeJSON([]byte(value), &input); err != nil {
		return nil, err
	}
	if input == nil {
		input = map[string]any{}
	}
	return input, nil
}

func objectSchemaOrDefault(raw json.RawMessage) json.RawMessage {
	if isNullJSON(raw) {
		return json.RawMessage(`{"type":"object","properties":{}}`)
	}
	return raw
}

func isNullJSON(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func parseRole(value string) (canonical.Role, error) {
	switch value {
	case "system", "developer":
		return canonical.RoleSystem, nil
	case "user":
		return canonical.RoleUser, nil
	case "assistant":
		return canonical.RoleAssistant, nil
	case "tool":
		return canonical.RoleTool, nil
	default:
		return "", fmt.Errorf("unsupported role %q", value)
	}
}

type contentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	ImageURL struct {
		URL string `json:"url"`
	} `json:"image_url"`
}

func parseContent(raw json.RawMessage) ([]canonical.Block, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if text == "" {
			return nil, nil
		}
		return []canonical.Block{{Type: canonical.BlockText, Text: text}}, nil
	}
	var parts []contentPart
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, err
	}
	blocks := make([]canonical.Block, 0, len(parts))
	for _, part := range parts {
		switch part.Type {
		case "text":
			if part.Text != "" {
				blocks = append(blocks, canonical.Block{Type: canonical.BlockText, Text: part.Text})
			}
		case "image_url":
			blocks = append(blocks, canonical.Block{Type: canonical.BlockImage, ImageURL: part.ImageURL.URL})
		default:
			return nil, fmt.Errorf("unsupported content part %q", part.Type)
		}
	}
	return blocks, nil
}

func (codec Codec) NewStream(responseID, model string, seeds ...dialect.StreamSeed) dialect.Stream {
	created := codec.now().Unix()
	if len(seeds) != 0 && seeds[0].CreatedAtUnix > 0 {
		created = seeds[0].CreatedAtUnix
	}
	return &stream{responseID: responseID, model: model, created: created}
}

func (Codec) AdmissionError(status int, code, message string) []byte {
	payload, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    errorType(status),
			"code":    code,
		},
	})
	return payload
}

func (Codec) OverloadedStatus() int {
	return http.StatusServiceUnavailable
}

func errorType(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "authentication_error"
	case http.StatusTooManyRequests:
		return "rate_limit_error"
	case http.StatusConflict:
		return "idempotency_error"
	default:
		if status >= 500 {
			return "server_error"
		}
		return "invalid_request_error"
	}
}

type stream struct {
	responseID string
	model      string
	created    int64
	started    bool
	done       bool
}

func (stream *stream) Start() ([][]byte, error) {
	if stream.started {
		return nil, errors.New("OpenAI stream already started")
	}
	stream.started = true
	frame, err := stream.chunk(map[string]any{"role": "assistant"}, nil)
	if err != nil {
		return nil, err
	}
	return [][]byte{frame}, nil
}

func (*stream) Heartbeat() []byte {
	return []byte(": ping\n\n")
}

func (stream *stream) Encode(event completion.Event, _ ...dialect.EventSeed) ([][]byte, bool, error) {
	if !stream.started {
		return nil, false, errors.New("OpenAI stream has not started")
	}
	if stream.done {
		return nil, true, errors.New("OpenAI stream is complete")
	}
	switch event.Type {
	case completion.EventAccepted:
		return nil, false, nil
	case completion.EventProgress:
		frame, err := stream.chunk(map[string]any{"content": event.Text}, nil)
		return frames(frame), false, err
	case completion.EventFinal, completion.EventClarification:
		var result [][]byte
		if event.Text != "" {
			frame, err := stream.chunk(map[string]any{"content": event.Text}, nil)
			if err != nil {
				return nil, false, err
			}
			result = append(result, frame)
		}
		finish, err := stream.chunk(map[string]any{}, stringPointer("stop"))
		if err != nil {
			return nil, false, err
		}
		stream.done = true
		return append(result, finish, []byte("data: [DONE]\n\n")), true, nil
	case completion.EventToolCalls:
		calls := make([]map[string]any, 0, len(event.ToolCalls))
		for index, call := range event.ToolCalls {
			arguments, err := marshalToolArguments(call.Input)
			if err != nil {
				return nil, false, fmt.Errorf("marshal tool call %q arguments: %w", call.ID, err)
			}
			calls = append(calls, map[string]any{
				"index": index,
				"id":    call.ID,
				"type":  "function",
				"function": map[string]any{
					"name": call.Name, "arguments": string(arguments),
				},
			})
		}
		frame, err := stream.chunk(map[string]any{"tool_calls": calls}, nil)
		if err != nil {
			return nil, false, err
		}
		finish, err := stream.chunk(map[string]any{}, stringPointer("tool_calls"))
		if err != nil {
			return nil, false, err
		}
		stream.done = true
		return [][]byte{frame, finish, []byte("data: [DONE]\n\n")}, true, nil
	case completion.EventRejected, completion.EventExpired, completion.EventFailed, completion.EventUnavailable:
		payload, err := json.Marshal(map[string]any{
			"error": map[string]any{
				"type": "human_agent_error", "code": event.ErrorCode, "message": event.Error,
			},
		})
		if err != nil {
			return nil, false, err
		}
		stream.done = true
		return [][]byte{sse(payload), []byte("data: [DONE]\n\n")}, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported completion event %q", event.Type)
	}
}

func marshalToolArguments(input map[string]any) ([]byte, error) {
	if input == nil {
		input = map[string]any{}
	}
	return json.Marshal(input)
}

func (stream *stream) chunk(delta map[string]any, finishReason *string) ([]byte, error) {
	payload, err := json.Marshal(map[string]any{
		"id":      stream.responseID,
		"object":  "chat.completion.chunk",
		"created": stream.created,
		"model":   stream.model,
		"choices": []map[string]any{{
			"index": 0, "delta": delta, "finish_reason": finishReason,
		}},
	})
	if err != nil {
		return nil, err
	}
	return sse(payload), nil
}

func sse(payload []byte) []byte {
	return []byte("data: " + strings.TrimSpace(string(payload)) + "\n\n")
}

func stringPointer(value string) *string {
	return &value
}

func frames(frame []byte) [][]byte {
	if frame == nil {
		return nil
	}
	return [][]byte{frame}
}
