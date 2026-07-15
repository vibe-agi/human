// Package responses implements the OpenAI Responses API wire dialect.
package responses

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
	return canonical.DialectResponses
}

type responsesRequest struct {
	Model        string            `json:"model"`
	Stream       bool              `json:"stream"`
	Instructions json.RawMessage   `json:"instructions"`
	Input        json.RawMessage   `json:"input"`
	Tools        []responseTool    `json:"tools"`
	Metadata     map[string]string `json:"metadata"`
}

type responseTool struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type inputItem struct {
	Type      string          `json:"type"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	ID        string          `json:"id"`
	CallID    string          `json:"call_id"`
	Name      string          `json:"name"`
	Arguments string          `json:"arguments"`
	Output    json.RawMessage `json:"output"`
}

type contentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	ImageURL string `json:"image_url"`
}

func (Codec) Decode(payload []byte) (canonical.Request, error) {
	var wire responsesRequest
	if err := json.Unmarshal(payload, &wire); err != nil {
		return canonical.Request{}, fmt.Errorf("decode OpenAI Responses request: %w", err)
	}
	if !wire.Stream {
		return canonical.Request{}, dialect.ErrUnsupportedNonStreaming
	}

	instructions, err := parseInstructions(wire.Instructions)
	if err != nil {
		return canonical.Request{}, fmt.Errorf("instructions: %w", err)
	}
	request := canonical.Request{
		Dialect:  canonical.DialectResponses,
		Model:    wire.Model,
		Stream:   true,
		System:   instructions,
		Metadata: wire.Metadata,
	}
	if err := appendInput(&request, wire.Input); err != nil {
		return canonical.Request{}, fmt.Errorf("input: %w", err)
	}
	for index, tool := range wire.Tools {
		if tool.Type != "function" {
			return canonical.Request{}, fmt.Errorf("tool %d: unsupported type %q", index, tool.Type)
		}
		request.Tools = append(request.Tools, canonical.Tool{
			Name:        tool.Name,
			Description: tool.Description,
			InputSchema: tool.Parameters,
		})
	}
	if err := request.Validate(); err != nil {
		return canonical.Request{}, err
	}
	return request, nil
}

func parseInstructions(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", errors.New("only string instructions are supported")
	}
	return value, nil
}

func appendInput(request *canonical.Request, raw json.RawMessage) error {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return errors.New("input is required")
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if text == "" {
			return errors.New("input text is empty")
		}
		request.Messages = append(request.Messages, canonical.Message{
			Role: canonical.RoleUser,
			Blocks: []canonical.Block{{
				Type: canonical.BlockText,
				Text: text,
			}},
		})
		return nil
	}

	var items []inputItem
	if err := json.Unmarshal(raw, &items); err != nil {
		return err
	}
	for index, item := range items {
		if err := appendItem(request, item); err != nil {
			return fmt.Errorf("item %d: %w", index, err)
		}
	}
	return nil
}

func appendItem(request *canonical.Request, item inputItem) error {
	switch item.Type {
	case "", "message":
		role, system, err := parseRole(item.Role)
		if err != nil {
			return err
		}
		blocks, err := parseContent(item.Content)
		if err != nil {
			return fmt.Errorf("content: %w", err)
		}
		if system {
			for _, block := range blocks {
				if block.Type != canonical.BlockText {
					return errors.New("system and developer messages must contain only text")
				}
				appendSystem(request, block.Text)
			}
			return nil
		}
		request.Messages = append(request.Messages, canonical.Message{Role: role, Blocks: blocks})
		return nil
	case "function_call":
		if strings.TrimSpace(item.CallID) == "" {
			return errors.New("function_call requires call_id")
		}
		if strings.TrimSpace(item.Name) == "" {
			return errors.New("function_call requires name")
		}
		var arguments map[string]any
		if err := json.Unmarshal([]byte(item.Arguments), &arguments); err != nil {
			return fmt.Errorf("function_call arguments: %w", err)
		}
		request.Messages = append(request.Messages, canonical.Message{
			Role: canonical.RoleAssistant,
			Blocks: []canonical.Block{{
				Type:       canonical.BlockToolUse,
				ToolCallID: item.CallID,
				ToolName:   item.Name,
				Input:      arguments,
			}},
		})
		return nil
	case "function_call_output":
		if strings.TrimSpace(item.CallID) == "" {
			return errors.New("function_call_output requires call_id")
		}
		if len(item.Output) == 0 {
			return errors.New("function_call_output requires output")
		}
		var output any
		if err := json.Unmarshal(item.Output, &output); err != nil {
			return fmt.Errorf("function_call_output output: %w", err)
		}
		request.Messages = append(request.Messages, canonical.Message{
			Role: canonical.RoleTool,
			Blocks: []canonical.Block{{
				Type:       canonical.BlockToolResult,
				ToolCallID: item.CallID,
				Output:     output,
			}},
		})
		return nil
	default:
		return fmt.Errorf("unsupported item type %q", item.Type)
	}
}

func parseRole(value string) (role canonical.Role, system bool, err error) {
	switch value {
	case "user":
		return canonical.RoleUser, false, nil
	case "assistant":
		return canonical.RoleAssistant, false, nil
	case "system", "developer":
		return canonical.RoleSystem, true, nil
	default:
		return "", false, fmt.Errorf("unsupported role %q", value)
	}
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
	for index, part := range parts {
		switch part.Type {
		case "input_text", "output_text":
			blocks = append(blocks, canonical.Block{Type: canonical.BlockText, Text: part.Text})
		case "input_image":
			blocks = append(blocks, canonical.Block{Type: canonical.BlockImage, ImageURL: part.ImageURL})
		default:
			return nil, fmt.Errorf("part %d: unsupported type %q", index, part.Type)
		}
	}
	return blocks, nil
}

func appendSystem(request *canonical.Request, text string) {
	if request.System != "" {
		request.System += "\n"
	}
	request.System += text
}

func (codec Codec) NewStream(responseID, model string, seeds ...dialect.StreamSeed) dialect.Stream {
	created := codec.now().Unix()
	if len(seeds) != 0 && seeds[0].CreatedAtUnix > 0 {
		created = seeds[0].CreatedAtUnix
	}
	return &stream{
		responseID: responseID,
		model:      model,
		created:    created,
		now:        codec.now,
	}
}

func (Codec) AdmissionError(status int, code, message string) []byte {
	payload, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    errorType(status),
			"code":    code,
			"param":   nil,
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
	now        func() time.Time
	sequence   int
	started    bool
	done       bool
	text       strings.Builder
}

func (stream *stream) Start() ([][]byte, error) {
	if stream.started {
		return nil, errors.New("Responses stream already started")
	}
	stream.started = true
	frame, err := stream.event("response.created", map[string]any{
		"type":     "response.created",
		"response": stream.response("in_progress", nil, nil),
	})
	if err != nil {
		return nil, err
	}
	return [][]byte{frame}, nil
}

func (*stream) Heartbeat() []byte {
	return []byte(": ping\n\n")
}

func (stream *stream) Encode(event completion.Event, seeds ...dialect.EventSeed) ([][]byte, bool, error) {
	if !stream.started {
		return nil, false, errors.New("Responses stream has not started")
	}
	if stream.done {
		return nil, true, errors.New("Responses stream is complete")
	}

	switch event.Type {
	case completion.EventAccepted:
		return nil, false, nil
	case completion.EventProgress:
		frame, err := stream.textDelta(event.Text)
		return singleFrame(frame), false, err
	case completion.EventFinal, completion.EventClarification:
		var frames [][]byte
		if event.Text != "" {
			frame, err := stream.textDelta(event.Text)
			if err != nil {
				return nil, false, err
			}
			frames = append(frames, frame)
		}
		output := []any{}
		if stream.text.Len() != 0 {
			output = append(output, stream.messageOutput())
		}
		completed, err := stream.complete(output, encodedAtUnix(stream.now, seeds))
		if err != nil {
			return nil, false, err
		}
		stream.done = true
		return append(frames, completed), true, nil
	case completion.EventToolCalls:
		var frames [][]byte
		output := make([]any, 0, len(event.ToolCalls)+1)
		if stream.text.Len() != 0 {
			output = append(output, stream.messageOutput())
		}
		for _, call := range event.ToolCalls {
			arguments, err := json.Marshal(call.Input)
			if err != nil {
				return nil, false, fmt.Errorf("marshal tool call %q arguments: %w", call.ID, err)
			}
			itemID := functionItemID(call.ID)
			index := len(output)
			frame, err := stream.event("response.function_call_arguments.done", map[string]any{
				"type":         "response.function_call_arguments.done",
				"item_id":      itemID,
				"name":         call.Name,
				"output_index": index,
				"arguments":    string(arguments),
			})
			if err != nil {
				return nil, false, err
			}
			frames = append(frames, frame)
			output = append(output, map[string]any{
				"id":        itemID,
				"type":      "function_call",
				"status":    "completed",
				"call_id":   call.ID,
				"name":      call.Name,
				"arguments": string(arguments),
			})
		}
		completed, err := stream.complete(output, encodedAtUnix(stream.now, seeds))
		if err != nil {
			return nil, false, err
		}
		stream.done = true
		return append(frames, completed), true, nil
	case completion.EventRejected, completion.EventExpired, completion.EventFailed, completion.EventUnavailable:
		message := event.Error
		if message == "" {
			message = event.ErrorCode
		}
		frame, err := stream.event("error", map[string]any{
			"type":    "error",
			"code":    nullableString(event.ErrorCode),
			"message": message,
			"param":   nil,
		})
		if err != nil {
			return nil, false, err
		}
		stream.done = true
		return [][]byte{frame}, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported completion event %q", event.Type)
	}
}

func (stream *stream) textDelta(text string) ([]byte, error) {
	if text == "" {
		return nil, nil
	}
	stream.text.WriteString(text)
	return stream.event("response.output_text.delta", map[string]any{
		"type":          "response.output_text.delta",
		"item_id":       messageItemID(stream.responseID),
		"output_index":  0,
		"content_index": 0,
		"delta":         text,
		"logprobs":      []any{},
	})
}

func (stream *stream) complete(output []any, completedAt int64) ([]byte, error) {
	return stream.event("response.completed", map[string]any{
		"type":     "response.completed",
		"response": stream.response("completed", output, completedAt),
	})
}

func encodedAtUnix(now func() time.Time, seeds []dialect.EventSeed) int64 {
	if len(seeds) != 0 && seeds[0].EncodedAtUnix > 0 {
		return seeds[0].EncodedAtUnix
	}
	return now().Unix()
}

func (stream *stream) response(status string, output []any, completedAt any) map[string]any {
	if output == nil {
		output = []any{}
	}
	return map[string]any{
		"id":                   stream.responseID,
		"object":               "response",
		"created_at":           stream.created,
		"status":               status,
		"completed_at":         completedAt,
		"error":                nil,
		"incomplete_details":   nil,
		"instructions":         nil,
		"max_output_tokens":    nil,
		"model":                stream.model,
		"output":               output,
		"parallel_tool_calls":  true,
		"previous_response_id": nil,
		"reasoning": map[string]any{
			"effort": nil, "summary": nil,
		},
		"store":       false,
		"temperature": nil,
		"text": map[string]any{
			"format": map[string]string{"type": "text"},
		},
		"tool_choice": "auto",
		"tools":       []any{},
		"top_p":       nil,
		"truncation":  "disabled",
		"usage":       nil,
		"metadata":    map[string]string{},
	}
}

func (stream *stream) messageOutput() map[string]any {
	return map[string]any{
		"id":     messageItemID(stream.responseID),
		"type":   "message",
		"status": "completed",
		"role":   "assistant",
		"content": []any{map[string]any{
			"type":        "output_text",
			"text":        stream.text.String(),
			"annotations": []any{},
			"logprobs":    []any{},
		}},
	}
}

func (stream *stream) event(name string, payload map[string]any) ([]byte, error) {
	payload["sequence_number"] = stream.sequence
	stream.sequence++
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return namedSSE(name, encoded), nil
}

func namedSSE(name string, payload []byte) []byte {
	return []byte("event: " + name + "\ndata: " + strings.TrimSpace(string(payload)) + "\n\n")
}

func messageItemID(responseID string) string {
	return "msg_" + responseID
}

func functionItemID(callID string) string {
	return "fc_" + callID
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func singleFrame(frame []byte) [][]byte {
	if frame == nil {
		return nil
	}
	return [][]byte{frame}
}
