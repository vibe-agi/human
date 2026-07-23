// Package anthropic implements the Anthropic Messages wire dialect.
package anthropic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

const statusOverloaded = 529

type Codec struct{}

var _ dialect.Codec = Codec{}

func New() Codec {
	return Codec{}
}

func (Codec) Dialect() canonical.Dialect {
	return canonical.DialectAnthropic
}

type messagesRequest struct {
	Model               string             `json:"model"`
	MaxTokens           *int64             `json:"max_tokens"`
	Stream              bool               `json:"stream"`
	System              json.RawMessage    `json:"system"`
	Messages            []anthropicMessage `json:"messages"`
	Tools               []anthropicTool    `json:"tools"`
	Metadata            map[string]string  `json:"metadata"`
	ToolChoice          json.RawMessage    `json:"tool_choice"`
	Temperature         *float64           `json:"temperature"`
	TopK                *int64             `json:"top_k"`
	TopP                *float64           `json:"top_p"`
	StopSequences       []string           `json:"stop_sequences"`
	Thinking            json.RawMessage    `json:"thinking"`
	ContextManagement   json.RawMessage    `json:"context_management"`
	OutputConfig        json.RawMessage    `json:"output_config"`
	OutputFormat        json.RawMessage    `json:"output_format"`
	Container           json.RawMessage    `json:"container"`
	CacheControl        json.RawMessage    `json:"cache_control"`
	Diagnostics         json.RawMessage    `json:"diagnostics"`
	Fallbacks           []json.RawMessage  `json:"fallbacks"`
	MCPServers          []json.RawMessage  `json:"mcp_servers"`
	FallbackCreditToken string             `json:"fallback_credit_token"`
	InferenceGeo        string             `json:"inference_geo"`
	ServiceTier         string             `json:"service_tier"`
	Speed               string             `json:"speed"`
}

type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type contentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Source    imageSource     `json:"source"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     map[string]any  `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
	IsError   bool            `json:"is_error"`
}

type imageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
	URL       string `json:"url"`
}

func (Codec) Decode(payload []byte) (canonical.Request, error) {
	var wire messagesRequest
	if err := dialect.DecodeJSONStrict(payload, &wire); err != nil {
		return canonical.Request{}, fmt.Errorf("decode Anthropic Messages request: %w", err)
	}
	if err := validateRequestControls(wire); err != nil {
		return canonical.Request{}, err
	}
	toolCallPolicy, err := parseToolChoice(wire.ToolChoice)
	if err != nil {
		return canonical.Request{}, err
	}

	system, err := parseSystem(wire.System)
	if err != nil {
		return canonical.Request{}, fmt.Errorf("system: %w", err)
	}
	request := canonical.Request{
		Dialect:        canonical.DialectAnthropic,
		Model:          wire.Model,
		Stream:         wire.Stream,
		System:         system,
		Metadata:       wire.Metadata,
		ToolCallPolicy: toolCallPolicy,
	}
	for index, message := range wire.Messages {
		role, err := parseRole(message.Role)
		if err != nil {
			return canonical.Request{}, fmt.Errorf("message %d: %w", index, err)
		}
		blocks, err := parseContent(message.Content, role)
		if err != nil {
			return canonical.Request{}, fmt.Errorf("message %d content: %w", index, err)
		}
		if role == canonical.RoleAssistant && len(blocks) == 0 {
			continue
		}
		request.Messages = append(request.Messages, canonical.Message{Role: role, Blocks: blocks})
	}
	for index, tool := range wire.Tools {
		if strings.TrimSpace(tool.Name) == "" {
			return canonical.Request{}, fmt.Errorf("tool %d: name is required", index)
		}
		request.Tools = append(request.Tools, canonical.Tool{
			Name:        tool.Name,
			Description: tool.Description,
			InputSchema: tool.InputSchema,
		})
	}
	if err := request.Validate(); err != nil {
		return canonical.Request{}, err
	}
	if request.ToolCallPolicy == canonical.ToolCallsDisabled {
		// tool_choice:none is a capability boundary, not merely a generation hint.
		// Do not expose otherwise valid tool definitions to the Human worker.
		request.Tools = nil
	}
	return request, nil
}

// validateRequestControls classifies every field in the current stable and
// beta Messages envelopes. Provider scheduling/sampling hints have no meaning
// for a human model, but are still type/range checked before becoming explicit
// no-ops. Stateful provider features and output-shape guarantees are rejected
// because silently pretending to implement them would be unsafe.
func validateRequestControls(wire messagesRequest) error {
	if wire.MaxTokens != nil && *wire.MaxTokens < 0 {
		return errors.New("max_tokens must be non-negative")
	}
	if wire.Temperature != nil && (*wire.Temperature < 0 || *wire.Temperature > 1) {
		return errors.New("temperature must be between 0 and 1")
	}
	if wire.TopK != nil && *wire.TopK < 0 {
		return errors.New("top_k must be non-negative")
	}
	if wire.TopP != nil && (*wire.TopP < 0 || *wire.TopP > 1) {
		return errors.New("top_p must be between 0 and 1")
	}
	// stop_sequences is a token-generation control. A human response has no
	// provider tokenizer, so a well-typed list is accepted as an explicit no-op.
	for _, object := range []struct {
		name string
		raw  json.RawMessage
	}{
		{name: "thinking", raw: wire.Thinking},
		{name: "context_management", raw: wire.ContextManagement},
		{name: "cache_control", raw: wire.CacheControl},
		{name: "diagnostics", raw: wire.Diagnostics},
	} {
		if err := validateJSONObject(object.name, object.raw); err != nil {
			return err
		}
	}
	if err := validateOutputConfig(wire.OutputConfig); err != nil {
		return err
	}
	if !isNullJSON(wire.OutputFormat) {
		return errors.New("structured output_format is not supported")
	}
	if !isNullJSON(wire.Container) {
		return errors.New("container continuation is not supported")
	}
	if wire.FallbackCreditToken != "" || len(wire.Fallbacks) != 0 {
		return errors.New("model fallbacks are not supported")
	}
	if len(wire.MCPServers) != 0 {
		return errors.New("provider-hosted MCP servers are not supported")
	}
	if wire.ServiceTier != "" && wire.ServiceTier != "auto" && wire.ServiceTier != "standard_only" {
		return fmt.Errorf("unsupported service_tier %q", wire.ServiceTier)
	}
	if wire.Speed != "" && wire.Speed != "standard" && wire.Speed != "fast" {
		return fmt.Errorf("unsupported speed %q", wire.Speed)
	}
	return nil
}

func validateJSONObject(name string, raw json.RawMessage) error {
	if isNullJSON(raw) {
		return nil
	}
	var value map[string]any
	if err := dialect.DecodeJSON(raw, &value); err != nil || value == nil {
		return fmt.Errorf("%s must be a JSON object", name)
	}
	return nil
}

func validateOutputConfig(raw json.RawMessage) error {
	if isNullJSON(raw) {
		return nil
	}
	var config struct {
		Effort     string          `json:"effort"`
		Format     json.RawMessage `json:"format"`
		TaskBudget json.RawMessage `json:"task_budget"`
	}
	if err := dialect.DecodeJSONStrict(raw, &config); err != nil {
		return fmt.Errorf("output_config: %w", err)
	}
	if config.Effort != "" {
		switch config.Effort {
		case "low", "medium", "high", "xhigh", "max":
		default:
			return fmt.Errorf("unsupported output_config.effort %q", config.Effort)
		}
	}
	if !isNullJSON(config.Format) {
		return errors.New("structured output_config.format is not supported")
	}
	if err := validateJSONObject("output_config.task_budget", config.TaskBudget); err != nil {
		return err
	}
	return nil
}

func isNullJSON(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func parseToolChoice(raw json.RawMessage) (canonical.ToolCallPolicy, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}
	var stringChoice string
	if err := json.Unmarshal(trimmed, &stringChoice); err == nil {
		switch stringChoice {
		case "auto":
			return "", nil
		case "none":
			return canonical.ToolCallsDisabled, nil
		}
		return "", errors.New("tool_choice is unsupported; omit it or use auto/none")
	}
	var choice struct {
		Type                   string `json:"type"`
		Name                   string `json:"name"`
		DisableParallelToolUse bool   `json:"disable_parallel_tool_use"`
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&choice); err != nil {
		return "", fmt.Errorf("tool_choice: %w", err)
	}
	if choice.Name != "" {
		return "", errors.New("specific tool_choice is not supported")
	}
	if choice.Type == "none" {
		return canonical.ToolCallsDisabled, nil
	}
	if choice.Type != "auto" {
		return "", errors.New("tool_choice is unsupported; omit it or use auto/none")
	}
	if choice.DisableParallelToolUse {
		return canonical.ToolCallsSerial, nil
	}
	return canonical.ToolCallsParallel, nil
}

func parseRole(value string) (canonical.Role, error) {
	switch value {
	case "user":
		return canonical.RoleUser, nil
	case "assistant":
		return canonical.RoleAssistant, nil
	default:
		return "", fmt.Errorf("unsupported role %q", value)
	}
}

func parseSystem(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return "", err
	}
	parts := make([]string, 0, len(blocks))
	for index, block := range blocks {
		if block.Type != "text" {
			return "", fmt.Errorf("block %d: unsupported type %q", index, block.Type)
		}
		parts = append(parts, block.Text)
	}
	return strings.Join(parts, "\n"), nil
}

func parseContent(raw json.RawMessage, role canonical.Role) ([]canonical.Block, error) {
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
	var wireBlocks []contentBlock
	if err := dialect.DecodeJSON(raw, &wireBlocks); err != nil {
		return nil, err
	}
	blocks := make([]canonical.Block, 0, len(wireBlocks))
	for index, wire := range wireBlocks {
		if wire.Type == "text" && wire.Text == "" {
			continue
		}
		block, err := parseBlock(wire, role)
		if err != nil {
			return nil, fmt.Errorf("block %d: %w", index, err)
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func parseBlock(wire contentBlock, role canonical.Role) (canonical.Block, error) {
	switch wire.Type {
	case "text":
		return canonical.Block{Type: canonical.BlockText, Text: wire.Text}, nil
	case "image":
		imageURL, err := parseImageSource(wire.Source)
		if err != nil {
			return canonical.Block{}, err
		}
		return canonical.Block{Type: canonical.BlockImage, ImageURL: imageURL}, nil
	case "tool_use":
		if role != canonical.RoleAssistant {
			return canonical.Block{}, errors.New("tool_use requires assistant role")
		}
		input := wire.Input
		if input == nil {
			input = map[string]any{}
		}
		return canonical.Block{
			Type:       canonical.BlockToolUse,
			ToolCallID: wire.ID,
			ToolName:   wire.Name,
			Input:      input,
		}, nil
	case "tool_result":
		if role != canonical.RoleUser {
			return canonical.Block{}, errors.New("tool_result requires user role")
		}
		output, err := parseToolResult(wire.Content)
		if err != nil {
			return canonical.Block{}, fmt.Errorf("tool_result content: %w", err)
		}
		return canonical.Block{
			Type:       canonical.BlockToolResult,
			ToolCallID: wire.ToolUseID,
			Output:     output,
			IsError:    wire.IsError,
		}, nil
	default:
		return canonical.Block{}, fmt.Errorf("unsupported content block %q", wire.Type)
	}
}

func parseImageSource(source imageSource) (string, error) {
	switch source.Type {
	case "base64":
		if source.MediaType == "" || source.Data == "" {
			return "", errors.New("base64 image requires media_type and data")
		}
		return "data:" + source.MediaType + ";base64," + source.Data, nil
	case "url":
		if strings.TrimSpace(source.URL) == "" {
			return "", errors.New("URL image requires url")
		}
		return source.URL, nil
	default:
		return "", fmt.Errorf("unsupported image source %q", source.Type)
	}
}

func parseToolResult(raw json.RawMessage) (any, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	blocks, err := parseContent(raw, canonical.RoleUser)
	if err != nil {
		return nil, err
	}
	for _, block := range blocks {
		if block.Type != canonical.BlockText && block.Type != canonical.BlockImage {
			return nil, fmt.Errorf("unsupported nested block %q", block.Type)
		}
	}
	return blocks, nil
}

func (Codec) NewStream(responseID, model string, _ ...dialect.StreamSeed) dialect.Stream {
	return &stream{responseID: responseID, model: model}
}

func (Codec) AdmissionError(status int, code, message string) []byte {
	payload, _ := json.Marshal(map[string]any{
		"type": "error",
		"error": map[string]any{
			"type": anthropicErrorType(status, code), "message": message,
		},
	})
	return payload
}

func (Codec) OverloadedStatus() int {
	return statusOverloaded
}

func anthropicErrorType(status int, code string) string {
	switch status {
	case http.StatusBadRequest, http.StatusConflict:
		return "invalid_request_error"
	case http.StatusUnauthorized:
		return "authentication_error"
	case http.StatusForbidden:
		return "permission_error"
	case http.StatusNotFound:
		return "not_found_error"
	case http.StatusRequestEntityTooLarge:
		return "request_too_large"
	case http.StatusTooManyRequests:
		return "rate_limit_error"
	case http.StatusServiceUnavailable, statusOverloaded:
		return "overloaded_error"
	default:
		if code == "overloaded_error" {
			return code
		}
		if status >= 500 {
			return "api_error"
		}
		return "invalid_request_error"
	}
}

type stream struct {
	responseID string
	model      string
	started    bool
	done       bool
	nextIndex  int
	textOpen   bool
}

func (stream *stream) Start() ([][]byte, error) {
	if stream.started {
		return nil, errors.New("Anthropic stream already started")
	}
	stream.started = true
	frame, err := namedSSE("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": stream.responseID, "type": "message", "role": "assistant",
			"content": []any{}, "model": stream.model, "stop_reason": nil,
			"stop_sequence": nil, "stop_details": nil, "container": nil,
			"usage": anthropicUsage(),
		},
	})
	if err != nil {
		return nil, err
	}
	return [][]byte{frame}, nil
}

func anthropicUsage() map[string]any {
	return map[string]any{
		"input_tokens": 0, "output_tokens": 0,
		"cache_creation_input_tokens": 0, "cache_read_input_tokens": 0,
		"cache_creation": map[string]int{
			"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 0,
		},
		"server_tool_use":       map[string]int{"web_search_requests": 0, "web_fetch_requests": 0},
		"output_tokens_details": map[string]int{"thinking_tokens": 0},
		"service_tier":          "standard",
		"inference_geo":         "not_applicable",
	}
}

func (*stream) Heartbeat() []byte {
	frame, _ := namedSSE("ping", map[string]any{"type": "ping"})
	return frame
}

func (stream *stream) Encode(event completion.Event, _ ...dialect.EventSeed) ([][]byte, bool, error) {
	if !stream.started {
		return nil, false, errors.New("Anthropic stream has not started")
	}
	if stream.done {
		return nil, true, errors.New("Anthropic stream is complete")
	}

	switch event.Type {
	case completion.EventAccepted:
		return nil, false, nil
	case completion.EventProgress:
		frames, err := stream.appendText(event.Text)
		return frames, false, err
	case completion.EventFinal, completion.EventClarification:
		frames, err := stream.appendText(event.Text)
		if err != nil {
			return nil, false, err
		}
		if !stream.textOpen {
			started, err := stream.openText()
			if err != nil {
				return nil, false, err
			}
			frames = append(frames, started)
		}
		closed, err := stream.closeText()
		if err != nil {
			return nil, false, err
		}
		frames = append(frames, closed)
		ending, err := stream.finish("end_turn")
		if err != nil {
			return nil, false, err
		}
		stream.done = true
		return append(frames, ending...), true, nil
	case completion.EventToolCalls:
		var frames [][]byte
		if stream.textOpen {
			closed, err := stream.closeText()
			if err != nil {
				return nil, false, err
			}
			frames = append(frames, closed)
		}
		for _, call := range event.ToolCalls {
			if call.TextInput != nil {
				return nil, false, fmt.Errorf("Anthropic tool call %q cannot use text input", call.ID)
			}
			toolFrames, err := stream.toolUse(call)
			if err != nil {
				return nil, false, err
			}
			frames = append(frames, toolFrames...)
		}
		ending, err := stream.finish("tool_use")
		if err != nil {
			return nil, false, err
		}
		stream.done = true
		return append(frames, ending...), true, nil
	case completion.EventRejected, completion.EventExpired, completion.EventFailed, completion.EventUnavailable:
		errorType := "api_error"
		if event.Type == completion.EventUnavailable {
			errorType = "overloaded_error"
		}
		message := event.Error
		if message == "" {
			message = event.ErrorCode
		}
		if message == "" {
			message = "human agent request failed"
		}
		frame, err := namedSSE("error", map[string]any{
			"type": "error", "error": map[string]string{"type": errorType, "message": message},
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

func (stream *stream) appendText(text string) ([][]byte, error) {
	if text == "" {
		return nil, nil
	}
	var frames [][]byte
	if !stream.textOpen {
		frame, err := stream.openText()
		if err != nil {
			return nil, err
		}
		frames = append(frames, frame)
	}
	delta, err := namedSSE("content_block_delta", map[string]any{
		"type": "content_block_delta", "index": stream.nextIndex,
		"delta": map[string]string{"type": "text_delta", "text": text},
	})
	if err != nil {
		return nil, err
	}
	return append(frames, delta), nil
}

func (stream *stream) openText() ([]byte, error) {
	frame, err := namedSSE("content_block_start", map[string]any{
		"type": "content_block_start", "index": stream.nextIndex,
		"content_block": map[string]string{"type": "text", "text": ""},
	})
	if err == nil {
		stream.textOpen = true
	}
	return frame, err
}

func (stream *stream) closeText() ([]byte, error) {
	frame, err := namedSSE("content_block_stop", map[string]any{
		"type": "content_block_stop", "index": stream.nextIndex,
	})
	if err == nil {
		stream.textOpen = false
		stream.nextIndex++
	}
	return frame, err
}

func (stream *stream) toolUse(call completion.ToolCall) ([][]byte, error) {
	input := call.Input
	if input == nil {
		input = map[string]any{}
	}
	partialJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal tool call %q input: %w", call.ID, err)
	}
	index := stream.nextIndex
	start, err := namedSSE("content_block_start", map[string]any{
		"type": "content_block_start", "index": index,
		"content_block": map[string]any{
			"type": "tool_use", "id": call.ID, "name": call.Name, "input": map[string]any{},
		},
	})
	if err != nil {
		return nil, err
	}
	delta, err := namedSSE("content_block_delta", map[string]any{
		"type": "content_block_delta", "index": index,
		"delta": map[string]string{"type": "input_json_delta", "partial_json": string(partialJSON)},
	})
	if err != nil {
		return nil, err
	}
	stop, err := namedSSE("content_block_stop", map[string]any{
		"type": "content_block_stop", "index": index,
	})
	if err != nil {
		return nil, err
	}
	stream.nextIndex++
	return [][]byte{start, delta, stop}, nil
}

func (stream *stream) finish(reason string) ([][]byte, error) {
	delta, err := namedSSE("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": reason, "stop_sequence": nil},
		"usage": map[string]int{"output_tokens": 0},
	})
	if err != nil {
		return nil, err
	}
	stop, err := namedSSE("message_stop", map[string]any{"type": "message_stop"})
	if err != nil {
		return nil, err
	}
	return [][]byte{delta, stop}, nil
}

func namedSSE(name string, payload any) ([]byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return []byte("event: " + name + "\ndata: " + strings.TrimSpace(string(encoded)) + "\n\n"), nil
}
