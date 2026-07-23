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
	Model             string            `json:"model"`
	Stream            bool              `json:"stream"`
	Messages          []chatMessage     `json:"messages"`
	Tools             []chatTool        `json:"tools"`
	ToolChoice        json.RawMessage   `json:"tool_choice"`
	ResponseFormat    json.RawMessage   `json:"response_format"`
	ParallelToolCalls *bool             `json:"parallel_tool_calls"`
	FrequencyPenalty  *float64          `json:"frequency_penalty"`
	Logprobs          *bool             `json:"logprobs"`
	MaxCompletion     *int64            `json:"max_completion_tokens"`
	MaxTokens         *int64            `json:"max_tokens"`
	N                 *int64            `json:"n"`
	PresencePenalty   *float64          `json:"presence_penalty"`
	Seed              *int64            `json:"seed"`
	Store             *bool             `json:"store"`
	Temperature       *float64          `json:"temperature"`
	TopLogprobs       *int64            `json:"top_logprobs"`
	TopP              *float64          `json:"top_p"`
	PromptCacheKey    string            `json:"prompt_cache_key"`
	SafetyIdentifier  string            `json:"safety_identifier"`
	User              string            `json:"user"`
	Audio             json.RawMessage   `json:"audio"`
	LogitBias         map[string]int64  `json:"logit_bias"`
	Metadata          map[string]string `json:"metadata"`
	Modalities        []string          `json:"modalities"`
	PromptRetention   string            `json:"prompt_cache_retention"`
	ReasoningEffort   string            `json:"reasoning_effort"`
	ServiceTier       string            `json:"service_tier"`
	Stop              json.RawMessage   `json:"stop"`
	StreamOptions     json.RawMessage   `json:"stream_options"`
	Verbosity         string            `json:"verbosity"`
	FunctionCall      json.RawMessage   `json:"function_call"`
	Functions         []chatFunction    `json:"functions"`
	Prediction        json.RawMessage   `json:"prediction"`
	WebSearchOptions  json.RawMessage   `json:"web_search_options"`
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
	if err := dialect.DecodeJSONStrict(payload, &wire); err != nil {
		return canonical.Request{}, fmt.Errorf("decode OpenAI chat request: %w", err)
	}
	if err := validateChatControls(wire); err != nil {
		return canonical.Request{}, err
	}
	toolChoicePolicy, err := parseToolChoice(wire.ToolChoice)
	if err != nil {
		return canonical.Request{}, err
	}
	if err := validateResponseFormat(wire.ResponseFormat); err != nil {
		return canonical.Request{}, err
	}
	request := canonical.Request{
		Dialect:        canonical.DialectOpenAIChat,
		Model:          wire.Model,
		Stream:         wire.Stream,
		Metadata:       wire.Metadata,
		ToolCallPolicy: toolChoicePolicy,
	}
	if wire.ParallelToolCalls != nil && toolChoicePolicy != canonical.ToolCallsDisabled {
		request.ToolCallPolicy = canonical.ToolCallsParallel
		if !*wire.ParallelToolCalls {
			request.ToolCallPolicy = canonical.ToolCallsSerial
		}
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
	for index, function := range wire.Functions {
		if strings.TrimSpace(function.Name) == "" {
			return canonical.Request{}, fmt.Errorf("legacy function %d: name is required", index)
		}
		request.Tools = append(request.Tools, canonical.Tool{
			Name: function.Name, Description: function.Description,
			InputSchema: objectSchemaOrDefault(function.Parameters),
		})
	}
	if err := request.Validate(); err != nil {
		return canonical.Request{}, err
	}
	if request.ToolCallPolicy == canonical.ToolCallsDisabled {
		request.Tools = nil
	}
	return request, nil
}

func validateChatControls(wire chatRequest) error {
	if wire.FrequencyPenalty != nil && (*wire.FrequencyPenalty < -2 || *wire.FrequencyPenalty > 2) {
		return errors.New("frequency_penalty must be between -2 and 2")
	}
	if wire.PresencePenalty != nil && (*wire.PresencePenalty < -2 || *wire.PresencePenalty > 2) {
		return errors.New("presence_penalty must be between -2 and 2")
	}
	if wire.Temperature != nil && (*wire.Temperature < 0 || *wire.Temperature > 2) {
		return errors.New("temperature must be between 0 and 2")
	}
	if wire.TopP != nil && (*wire.TopP < 0 || *wire.TopP > 1) {
		return errors.New("top_p must be between 0 and 1")
	}
	for _, limit := range []struct {
		name  string
		value *int64
	}{
		{name: "max_completion_tokens", value: wire.MaxCompletion},
		{name: "max_tokens", value: wire.MaxTokens},
	} {
		if limit.value != nil && *limit.value < 0 {
			return fmt.Errorf("%s must be non-negative", limit.name)
		}
	}
	if wire.N != nil && *wire.N != 1 {
		return errors.New("n must be 1; the human model returns one choice")
	}
	if wire.Logprobs != nil && *wire.Logprobs {
		return errors.New("logprobs are not supported")
	}
	if wire.TopLogprobs != nil && *wire.TopLogprobs != 0 {
		return errors.New("top_logprobs are not supported")
	}
	if wire.Store != nil && *wire.Store {
		return errors.New("store=true is not supported")
	}
	if !isNullJSON(wire.Audio) || !isNullJSON(wire.Prediction) || !isNullJSON(wire.WebSearchOptions) {
		return errors.New("audio, predicted output, and provider-hosted web search are not supported")
	}
	if len(wire.LogitBias) != 0 {
		return errors.New("logit_bias is not supported")
	}
	if len(wire.Modalities) != 0 && (len(wire.Modalities) != 1 || wire.Modalities[0] != "text") {
		return errors.New("only the text output modality is supported")
	}
	if err := validateStop(wire.Stop); err != nil {
		return err
	}
	if err := validateStreamOptions(wire.StreamOptions); err != nil {
		return err
	}
	if err := validateLegacyFunctionCall(wire.FunctionCall); err != nil {
		return err
	}
	if wire.PromptRetention != "" && wire.PromptRetention != "in_memory" && wire.PromptRetention != "24h" {
		return fmt.Errorf("unsupported prompt_cache_retention %q", wire.PromptRetention)
	}
	if wire.ReasoningEffort != "" {
		switch wire.ReasoningEffort {
		case "none", "minimal", "low", "medium", "high", "xhigh", "max":
		default:
			return fmt.Errorf("unsupported reasoning_effort %q", wire.ReasoningEffort)
		}
	}
	if wire.ServiceTier != "" {
		switch wire.ServiceTier {
		case "auto", "default", "flex", "scale", "priority":
		default:
			return fmt.Errorf("unsupported service_tier %q", wire.ServiceTier)
		}
	}
	if wire.Verbosity != "" && wire.Verbosity != "low" && wire.Verbosity != "medium" && wire.Verbosity != "high" {
		return fmt.Errorf("unsupported verbosity %q", wire.Verbosity)
	}
	return nil
}

func validateStop(raw json.RawMessage) error {
	if isNullJSON(raw) {
		return nil
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return errors.New("stop must be a string or an array of strings")
	}
	return nil
}

func validateStreamOptions(raw json.RawMessage) error {
	if isNullJSON(raw) {
		return nil
	}
	var options struct {
		IncludeObfuscation *bool `json:"include_obfuscation"`
		IncludeUsage       *bool `json:"include_usage"`
	}
	if err := dialect.DecodeJSONStrict(raw, &options); err != nil {
		return fmt.Errorf("stream_options: %w", err)
	}
	// Both options affect provider telemetry only. Human emits unobfuscated SSE
	// and zero/omitted token usage, so accepting them is an explicit no-op.
	return nil
}

func validateLegacyFunctionCall(raw json.RawMessage) error {
	if isNullJSON(raw) {
		return nil
	}
	var mode string
	if err := json.Unmarshal(raw, &mode); err == nil && mode == "auto" {
		return nil
	}
	return errors.New("legacy function_call is unsupported; omit it or use \"auto\"")
}

func parseToolChoice(raw json.RawMessage) (canonical.ToolCallPolicy, error) {
	if isNullJSON(raw) {
		return "", nil
	}
	var choice string
	if err := json.Unmarshal(raw, &choice); err == nil {
		switch choice {
		case "auto":
			return "", nil
		case "none":
			return canonical.ToolCallsDisabled, nil
		}
	}
	return "", errors.New("tool_choice is unsupported; omit it or use \"auto\"/\"none\"")
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
			if call.TextInput != nil {
				return nil, false, fmt.Errorf("OpenAI Chat tool call %q cannot use text input", call.ID)
			}
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
	var usage any
	if finishReason != nil {
		// A human model has no provider tokenizer. Terminal chunks still expose a
		// well-formed zero usage object so SDK callers which request stream usage
		// receive a deterministic answer without a dialect-specific session seed.
		usage = completionUsage()
	}
	payload, err := json.Marshal(map[string]any{
		"id": stream.responseID, "object": "chat.completion.chunk",
		"created": stream.created, "model": stream.model,
		"service_tier": nil, "system_fingerprint": nil, "usage": usage,
		"choices": []map[string]any{{
			"index": 0, "delta": delta, "finish_reason": finishReason, "logprobs": nil,
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
