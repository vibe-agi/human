// Package responses implements the OpenAI Responses API wire dialect.
package responses

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	Model                string            `json:"model"`
	Stream               bool              `json:"stream"`
	Instructions         json.RawMessage   `json:"instructions"`
	Input                json.RawMessage   `json:"input"`
	Tools                []json.RawMessage `json:"tools"`
	Metadata             map[string]string `json:"metadata"`
	ToolChoice           json.RawMessage   `json:"tool_choice"`
	Text                 json.RawMessage   `json:"text"`
	PreviousResponseID   json.RawMessage   `json:"previous_response_id"`
	ParallelToolCalls    *bool             `json:"parallel_tool_calls"`
	Reasoning            json.RawMessage   `json:"reasoning"`
	Store                *bool             `json:"store"`
	Include              json.RawMessage   `json:"include"`
	MaxOutputTokens      *int64            `json:"max_output_tokens"`
	ClientMetadata       json.RawMessage   `json:"client_metadata"`
	PromptCacheKey       json.RawMessage   `json:"prompt_cache_key"`
	Background           *bool             `json:"background"`
	MaxToolCalls         *int64            `json:"max_tool_calls"`
	Temperature          *float64          `json:"temperature"`
	TopLogprobs          *int64            `json:"top_logprobs"`
	TopP                 *float64          `json:"top_p"`
	SafetyIdentifier     string            `json:"safety_identifier"`
	User                 string            `json:"user"`
	ContextManagement    []json.RawMessage `json:"context_management"`
	Conversation         json.RawMessage   `json:"conversation"`
	Prompt               json.RawMessage   `json:"prompt"`
	PromptCacheRetention string            `json:"prompt_cache_retention"`
	ServiceTier          string            `json:"service_tier"`
	StreamOptions        json.RawMessage   `json:"stream_options"`
	Truncation           string            `json:"truncation"`
}

type responseTool struct {
	Type         string          `json:"type"`
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	Parameters   json.RawMessage `json:"parameters"`
	Format       json.RawMessage `json:"format"`
	DeferLoading *bool           `json:"defer_loading"`
	Tools        []responseTool  `json:"tools"`
}

type inputItem struct {
	Type      string          `json:"type"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	ID        string          `json:"id"`
	CallID    string          `json:"call_id"`
	Namespace string          `json:"namespace"`
	Name      string          `json:"name"`
	Arguments string          `json:"arguments"`
	Input     string          `json:"input"`
	Output    json.RawMessage `json:"output"`
}

type contentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	ImageURL string `json:"image_url"`
}

func (Codec) Decode(payload []byte) (canonical.Request, error) {
	var wire responsesRequest
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return canonical.Request{}, fmt.Errorf("decode OpenAI Responses request: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return canonical.Request{}, fmt.Errorf("decode OpenAI Responses request: %w", err)
	}
	toolChoicePolicy, err := parseToolChoice(wire.ToolChoice)
	if err != nil {
		return canonical.Request{}, err
	}
	if err := validateTextFormat(wire.Text); err != nil {
		return canonical.Request{}, err
	}
	if !isNullJSON(wire.PreviousResponseID) {
		return canonical.Request{}, errors.New("previous_response_id is not supported")
	}
	if err := validateTopLevelControls(wire); err != nil {
		return canonical.Request{}, err
	}
	instructions, err := parseInstructions(wire.Instructions)
	if err != nil {
		return canonical.Request{}, fmt.Errorf("instructions: %w", err)
	}
	request := canonical.Request{
		Dialect:        canonical.DialectResponses,
		Model:          wire.Model,
		Stream:         wire.Stream,
		System:         instructions,
		Metadata:       wire.Metadata,
		ToolCallPolicy: toolChoicePolicy,
	}
	if wire.ParallelToolCalls != nil && toolChoicePolicy != canonical.ToolCallsDisabled {
		request.ToolCallPolicy = canonical.ToolCallsParallel
		if !*wire.ParallelToolCalls {
			request.ToolCallPolicy = canonical.ToolCallsSerial
		}
	}
	if err := appendInput(&request, wire.Input); err != nil {
		return canonical.Request{}, fmt.Errorf("input: %w", err)
	}
	for index, rawTool := range wire.Tools {
		if err := appendResponseTool(&request, rawTool); err != nil {
			return canonical.Request{}, fmt.Errorf("tool %d: %w", index, err)
		}
	}
	if err := request.Validate(); err != nil {
		return canonical.Request{}, err
	}
	if request.ToolCallPolicy == canonical.ToolCallsDisabled {
		request.Tools = nil
	}
	return request, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

// validateTopLevelControls makes every accepted modern Codex field deliberate.
// Sampling, token-budget, cache, service-tier and reasoning options are
// provider hints with no analogue for a human model. They are type/range
// checked and deliberately omitted from canonical assignment. Stateful
// provider features and output-shape guarantees remain fail-closed.
func validateTopLevelControls(wire responsesRequest) error {
	if wire.Background != nil && *wire.Background {
		return errors.New("background=true is not supported")
	}
	if wire.Store != nil && *wire.Store {
		return errors.New("store=true is not supported; omit store or use false")
	}
	if !isNullJSON(wire.Include) {
		var include []string
		if err := json.Unmarshal(wire.Include, &include); err != nil {
			return fmt.Errorf("include: %w", err)
		}
		for index, item := range include {
			if item != "reasoning.encrypted_content" || index != 0 {
				return fmt.Errorf("unsupported include value: %q", include)
			}
		}
	}
	if wire.MaxOutputTokens != nil && *wire.MaxOutputTokens < 0 {
		return errors.New("max_output_tokens must be non-negative")
	}
	if wire.MaxToolCalls != nil && *wire.MaxToolCalls < 0 {
		return errors.New("max_tool_calls must be non-negative")
	}
	if wire.Temperature != nil && (*wire.Temperature < 0 || *wire.Temperature > 2) {
		return errors.New("temperature must be between 0 and 2")
	}
	if wire.TopP != nil && (*wire.TopP < 0 || *wire.TopP > 1) {
		return errors.New("top_p must be between 0 and 1")
	}
	if wire.TopLogprobs != nil && *wire.TopLogprobs != 0 {
		return errors.New("top_logprobs are not supported")
	}
	if !isNullJSON(wire.ClientMetadata) {
		var metadata map[string]any
		if err := json.Unmarshal(wire.ClientMetadata, &metadata); err != nil || metadata == nil {
			return errors.New("client_metadata must be a JSON object")
		}
	}
	if !isNullJSON(wire.PromptCacheKey) {
		var key string
		if err := json.Unmarshal(wire.PromptCacheKey, &key); err != nil {
			return errors.New("prompt_cache_key must be a string")
		}
	}
	if !isNullJSON(wire.Reasoning) {
		var controls struct {
			Effort          string `json:"effort"`
			GenerateSummary string `json:"generate_summary"`
			Summary         string `json:"summary"`
		}
		decoder := json.NewDecoder(bytes.NewReader(wire.Reasoning))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&controls); err != nil {
			return fmt.Errorf("reasoning: %w", err)
		}
		if err := ensureJSONEOF(decoder); err != nil {
			return fmt.Errorf("reasoning: %w", err)
		}
		if controls.Effort != "" {
			switch controls.Effort {
			case "none", "minimal", "low", "medium", "high", "xhigh", "max":
			default:
				return fmt.Errorf("unsupported reasoning.effort %q", controls.Effort)
			}
		}
		for _, summary := range []struct {
			name  string
			value string
		}{
			{name: "generate_summary", value: controls.GenerateSummary},
			{name: "summary", value: controls.Summary},
		} {
			if summary.value != "" && summary.value != "auto" && summary.value != "concise" && summary.value != "detailed" {
				return fmt.Errorf("unsupported reasoning.%s %q", summary.name, summary.value)
			}
		}
	}
	if len(wire.ContextManagement) != 0 {
		return errors.New("context_management is not supported")
	}
	if !isNullJSON(wire.Conversation) {
		return errors.New("conversation continuation is not supported")
	}
	if !isNullJSON(wire.Prompt) {
		return errors.New("server-side prompt templates are not supported")
	}
	if wire.PromptCacheRetention != "" && wire.PromptCacheRetention != "in_memory" && wire.PromptCacheRetention != "24h" {
		return fmt.Errorf("unsupported prompt_cache_retention %q", wire.PromptCacheRetention)
	}
	if wire.ServiceTier != "" {
		switch wire.ServiceTier {
		case "auto", "default", "flex", "scale", "priority":
		default:
			return fmt.Errorf("unsupported service_tier %q", wire.ServiceTier)
		}
	}
	if wire.Truncation != "" && wire.Truncation != "auto" && wire.Truncation != "disabled" {
		return fmt.Errorf("unsupported truncation %q", wire.Truncation)
	}
	if !isNullJSON(wire.StreamOptions) {
		var options struct {
			IncludeObfuscation *bool `json:"include_obfuscation"`
		}
		decoder := json.NewDecoder(bytes.NewReader(wire.StreamOptions))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&options); err != nil {
			return fmt.Errorf("stream_options: %w", err)
		}
		if err := ensureJSONEOF(decoder); err != nil {
			return fmt.Errorf("stream_options: %w", err)
		}
	}
	return nil
}

func appendOpaqueFingerprint(request *canonical.Request, itemType string, raw json.RawMessage) error {
	if !json.Valid(raw) {
		return errors.New("opaque provider state is invalid JSON")
	}
	digest := sha256.Sum256(raw)
	request.OpaqueInput = append(request.OpaqueInput, canonical.OpaqueInput{
		Type: itemType, SHA256: fmt.Sprintf("%x", digest),
	})
	return nil
}

func appendResponseTool(request *canonical.Request, raw json.RawMessage) error {
	var tool responseTool
	if err := json.Unmarshal(raw, &tool); err != nil {
		return err
	}
	switch tool.Type {
	case "function":
		return appendFunctionTool(request, "", tool)
	case "custom":
		return appendCustomTool(request, "", tool)
	case "namespace":
		if strings.TrimSpace(tool.Name) == "" {
			return errors.New("namespace requires name")
		}
		if len(tool.Tools) == 0 {
			return fmt.Errorf("namespace %q contains no functions", tool.Name)
		}
		for index, nested := range tool.Tools {
			if nested.Type != "function" && nested.Type != "custom" {
				return fmt.Errorf("namespace %q tool %d: unsupported type %q", tool.Name, index, nested.Type)
			}
			var err error
			if nested.Type == "function" {
				err = appendFunctionTool(request, tool.Name, nested)
			} else {
				err = appendCustomTool(request, tool.Name, nested)
			}
			if err != nil {
				return fmt.Errorf("namespace %q tool %d: %w", tool.Name, index, err)
			}
		}
		return nil
	case "web_search":
		request.HostedCapabilities = append(request.HostedCapabilities, canonical.HostedCapability{
			Type:          tool.Type,
			Configuration: bytes.Clone(raw),
		})
		return nil
	default:
		return fmt.Errorf("unsupported type %q", tool.Type)
	}
}

func appendCustomTool(request *canonical.Request, namespace string, tool responseTool) error {
	if strings.TrimSpace(tool.Name) == "" {
		return errors.New("custom tool requires name")
	}
	if tool.DeferLoading != nil && *tool.DeferLoading {
		return fmt.Errorf("custom tool %q: defer_loading=true is not supported", tool.Name)
	}
	format, err := validateCustomToolFormat(tool.Format)
	if err != nil {
		return fmt.Errorf("custom tool %q format: %w", tool.Name, err)
	}
	request.Tools = append(request.Tools, canonical.Tool{
		Namespace:   namespace,
		Name:        tool.Name,
		Description: tool.Description,
		InputKind:   canonical.ToolInputText,
		InputFormat: format,
	})
	return nil
}

func validateCustomToolFormat(raw json.RawMessage) (json.RawMessage, error) {
	if isNullJSON(raw) {
		return nil, nil
	}
	var format struct {
		Type       string `json:"type"`
		Definition string `json:"definition"`
		Syntax     string `json:"syntax"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&format); err != nil {
		return nil, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	switch format.Type {
	case "text":
		if format.Definition != "" || format.Syntax != "" {
			return nil, errors.New("text format cannot contain grammar fields")
		}
	case "grammar":
		if format.Definition == "" {
			return nil, errors.New("grammar definition is required")
		}
		if format.Syntax != "lark" && format.Syntax != "regex" {
			return nil, errors.New(`grammar syntax must be "lark" or "regex"`)
		}
	default:
		return nil, fmt.Errorf("unsupported format type %q", format.Type)
	}
	return bytes.Clone(raw), nil
}

func appendFunctionTool(request *canonical.Request, namespace string, tool responseTool) error {
	if strings.TrimSpace(tool.Name) == "" {
		return errors.New("function requires name")
	}
	request.Tools = append(request.Tools, canonical.Tool{
		Namespace:   namespace,
		Name:        tool.Name,
		Description: tool.Description,
		InputSchema: objectSchemaOrDefault(tool.Parameters),
	})
	return nil
}

func objectSchemaOrDefault(raw json.RawMessage) json.RawMessage {
	if isNullJSON(raw) {
		return json.RawMessage(`{"type":"object","properties":{}}`)
	}
	return raw
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

func validateTextFormat(raw json.RawMessage) error {
	if isNullJSON(raw) {
		return nil
	}
	var textOptions struct {
		Format    json.RawMessage `json:"format"`
		Verbosity string          `json:"verbosity"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&textOptions); err != nil {
		return fmt.Errorf("text: %w", err)
	}
	switch textOptions.Verbosity {
	case "", "low", "medium", "high":
	default:
		return fmt.Errorf("text.verbosity: unsupported value %q", textOptions.Verbosity)
	}
	if isNullJSON(textOptions.Format) {
		return nil
	}
	var format struct {
		Type string `json:"type"`
	}
	decoder = json.NewDecoder(bytes.NewReader(textOptions.Format))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&format); err != nil {
		return fmt.Errorf("text.format: %w", err)
	}
	if format.Type != "text" {
		return errors.New("structured text.format is not supported; omit it or use type \"text\"")
	}
	return nil
}

func isNullJSON(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
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

	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return err
	}
	for index, rawItem := range items {
		var item inputItem
		if err := json.Unmarshal(rawItem, &item); err != nil {
			return fmt.Errorf("item %d: %w", index, err)
		}
		if err := appendItem(request, item, rawItem); err != nil {
			return fmt.Errorf("item %d: %w", index, err)
		}
	}
	return nil
}

func appendItem(request *canonical.Request, item inputItem, raw json.RawMessage) error {
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
		if role == canonical.RoleAssistant && len(blocks) == 0 {
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
		arguments, err := decodeToolArguments(item.Arguments)
		if err != nil {
			return fmt.Errorf("function_call arguments: %w", err)
		}
		request.Messages = append(request.Messages, canonical.Message{
			Role: canonical.RoleAssistant,
			Blocks: []canonical.Block{{
				Type:          canonical.BlockToolUse,
				ToolCallID:    item.CallID,
				ToolNamespace: item.Namespace,
				ToolName:      item.Name,
				Input:         arguments,
			}},
		})
		return nil
	case "custom_tool_call":
		if strings.TrimSpace(item.CallID) == "" {
			return errors.New("custom_tool_call requires call_id")
		}
		if strings.TrimSpace(item.Name) == "" {
			return errors.New("custom_tool_call requires name")
		}
		input := item.Input
		request.Messages = append(request.Messages, canonical.Message{
			Role: canonical.RoleAssistant,
			Blocks: []canonical.Block{{
				Type:          canonical.BlockToolUse,
				ToolCallID:    item.CallID,
				ToolNamespace: item.Namespace,
				ToolName:      item.Name,
				TextInput:     &input,
			}},
		})
		return nil
	case "reasoning":
		return appendOpaqueFingerprint(request, item.Type, raw)
	case "function_call_output", "custom_tool_call_output":
		if strings.TrimSpace(item.CallID) == "" {
			return fmt.Errorf("%s requires call_id", item.Type)
		}
		if len(item.Output) == 0 {
			return fmt.Errorf("%s requires output", item.Type)
		}
		var output any
		if err := dialect.DecodeJSON(item.Output, &output); err != nil {
			return fmt.Errorf("%s output: %w", item.Type, err)
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
			if part.Text != "" {
				blocks = append(blocks, canonical.Block{Type: canonical.BlockText, Text: part.Text})
			}
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
	parallelToolCalls := true
	toolChoice := "auto"
	if len(seeds) != 0 && seeds[0].CreatedAtUnix > 0 {
		created = seeds[0].CreatedAtUnix
	}
	if len(seeds) != 0 && seeds[0].ToolCallPolicy == canonical.ToolCallsSerial {
		parallelToolCalls = false
	}
	if len(seeds) != 0 && seeds[0].ToolCallPolicy == canonical.ToolCallsDisabled {
		parallelToolCalls = false
		toolChoice = "none"
	}
	return &stream{
		responseID:        responseID,
		model:             model,
		created:           created,
		now:               codec.now,
		parallelToolCalls: parallelToolCalls,
		toolChoice:        toolChoice,
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
	responseID        string
	model             string
	created           int64
	now               func() time.Time
	sequence          int
	started           bool
	done              bool
	text              strings.Builder
	textOpen          bool
	parallelToolCalls bool
	toolChoice        string
}

func (stream *stream) Start() ([][]byte, error) {
	if stream.started {
		return nil, errors.New("Responses stream already started")
	}
	stream.started = true
	created, err := stream.event("response.created", map[string]any{
		"type":     "response.created",
		"response": stream.response("in_progress", nil, nil),
	})
	if err != nil {
		return nil, err
	}
	inProgress, err := stream.event("response.in_progress", map[string]any{
		"type":     "response.in_progress",
		"response": stream.response("in_progress", nil, nil),
	})
	if err != nil {
		return nil, err
	}
	return [][]byte{created, inProgress}, nil
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
		frames, err := stream.appendText(event.Text)
		return frames, false, err
	case completion.EventFinal, completion.EventClarification:
		frames, err := stream.appendText(event.Text)
		if err != nil {
			return nil, false, err
		}
		// A successful response always contributes one assistant message, even
		// when its text is empty. Otherwise an empty final produces output=[] and
		// the caller loses the assistant turn on the next round trip.
		if !stream.textOpen {
			opened, openErr := stream.openText()
			if openErr != nil {
				return nil, false, openErr
			}
			frames = append(frames, opened...)
		}
		closing, closeErr := stream.closeText()
		if closeErr != nil {
			return nil, false, closeErr
		}
		frames = append(frames, closing...)
		output := []any{stream.messageOutput()}
		completed, err := stream.complete(output, encodedAtUnix(stream.now, seeds))
		if err != nil {
			return nil, false, err
		}
		stream.done = true
		return append(frames, completed), true, nil
	case completion.EventToolCalls:
		var frames [][]byte
		output := make([]any, 0, len(event.ToolCalls)+1)
		if stream.textOpen {
			closing, err := stream.closeText()
			if err != nil {
				return nil, false, err
			}
			frames = append(frames, closing...)
			output = append(output, stream.messageOutput())
		}
		for _, call := range event.ToolCalls {
			if call.TextInput != nil {
				itemID := customItemID(call.ID)
				index := len(output)
				added, err := stream.event("response.output_item.added", map[string]any{
					"type":         "response.output_item.added",
					"output_index": index,
					"item":         stream.customOutput(call, "", "in_progress"),
				})
				if err != nil {
					return nil, false, err
				}
				delta, err := stream.event("response.custom_tool_call_input.delta", map[string]any{
					"type":         "response.custom_tool_call_input.delta",
					"item_id":      itemID,
					"output_index": index,
					"delta":        *call.TextInput,
				})
				if err != nil {
					return nil, false, err
				}
				inputDone, err := stream.event("response.custom_tool_call_input.done", map[string]any{
					"type":         "response.custom_tool_call_input.done",
					"item_id":      itemID,
					"output_index": index,
					"input":        *call.TextInput,
				})
				if err != nil {
					return nil, false, err
				}
				item := stream.customOutput(call, *call.TextInput, "completed")
				itemDone, err := stream.event("response.output_item.done", map[string]any{
					"type":         "response.output_item.done",
					"output_index": index,
					"item":         item,
				})
				if err != nil {
					return nil, false, err
				}
				frames = append(frames, added, delta, inputDone, itemDone)
				output = append(output, item)
				continue
			}
			arguments, err := marshalToolArguments(call.Input)
			if err != nil {
				return nil, false, fmt.Errorf("marshal tool call %q arguments: %w", call.ID, err)
			}
			itemID := functionItemID(call.ID)
			index := len(output)
			added, err := stream.event("response.output_item.added", map[string]any{
				"type":         "response.output_item.added",
				"output_index": index,
				"item":         stream.functionOutput(call, "", "in_progress"),
			})
			if err != nil {
				return nil, false, err
			}
			delta, err := stream.event("response.function_call_arguments.delta", map[string]any{
				"type":         "response.function_call_arguments.delta",
				"item_id":      itemID,
				"output_index": index,
				"delta":        string(arguments),
			})
			if err != nil {
				return nil, false, err
			}
			argumentsDonePayload := map[string]any{
				"type":         "response.function_call_arguments.done",
				"item_id":      itemID,
				"name":         call.Name,
				"output_index": index,
				"arguments":    string(arguments),
			}
			if call.Namespace != "" {
				argumentsDonePayload["namespace"] = call.Namespace
			}
			argumentsDone, err := stream.event("response.function_call_arguments.done", argumentsDonePayload)
			if err != nil {
				return nil, false, err
			}
			item := stream.functionOutput(call, string(arguments), "completed")
			itemDone, err := stream.event("response.output_item.done", map[string]any{
				"type":         "response.output_item.done",
				"output_index": index,
				"item":         item,
			})
			if err != nil {
				return nil, false, err
			}
			frames = append(frames, added, delta, argumentsDone, itemDone)
			output = append(output, item)
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
		if message == "" {
			message = "human agent request failed"
		}
		// ResponseError.code is a closed Responses API enum. Keep our more
		// specific durable error in the message/event record, but never leak an
		// internal code such as human_timeout onto the public wire.
		frame, err := stream.fail("server_error", message)
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
		opened, err := stream.openText()
		if err != nil {
			return nil, err
		}
		frames = append(frames, opened...)
	}
	stream.text.WriteString(text)
	delta, err := stream.event("response.output_text.delta", map[string]any{
		"type":          "response.output_text.delta",
		"item_id":       messageItemID(stream.responseID),
		"output_index":  0,
		"content_index": 0,
		"delta":         text,
		"logprobs":      []any{},
	})
	if err != nil {
		return nil, err
	}
	return append(frames, delta), nil
}

func (stream *stream) openText() ([][]byte, error) {
	itemID := messageItemID(stream.responseID)
	added, err := stream.event("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": 0,
		"item":         stream.messageOutputInProgress(),
	})
	if err != nil {
		return nil, err
	}
	part, err := stream.event("response.content_part.added", map[string]any{
		"type":          "response.content_part.added",
		"item_id":       itemID,
		"output_index":  0,
		"content_index": 0,
		"part":          outputTextPart(""),
	})
	if err != nil {
		return nil, err
	}
	stream.textOpen = true
	return [][]byte{added, part}, nil
}

func (stream *stream) closeText() ([][]byte, error) {
	if !stream.textOpen {
		return nil, nil
	}
	itemID := messageItemID(stream.responseID)
	text := stream.text.String()
	done, err := stream.event("response.output_text.done", map[string]any{
		"type":          "response.output_text.done",
		"item_id":       itemID,
		"output_index":  0,
		"content_index": 0,
		"text":          text,
		"logprobs":      []any{},
	})
	if err != nil {
		return nil, err
	}
	partDone, err := stream.event("response.content_part.done", map[string]any{
		"type":          "response.content_part.done",
		"item_id":       itemID,
		"output_index":  0,
		"content_index": 0,
		"part":          outputTextPart(text),
	})
	if err != nil {
		return nil, err
	}
	itemDone, err := stream.event("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": 0,
		"item":         stream.messageOutput(),
	})
	if err != nil {
		return nil, err
	}
	stream.textOpen = false
	return [][]byte{done, partDone, itemDone}, nil
}

func (stream *stream) complete(output []any, completedAt int64) ([]byte, error) {
	return stream.event("response.completed", map[string]any{
		"type":     "response.completed",
		"response": stream.response("completed", output, completedAt),
	})
}

func (stream *stream) fail(code, message string) ([]byte, error) {
	var output []any
	if stream.textOpen {
		output = []any{stream.incompleteMessageOutput()}
	}
	response := stream.response("failed", output, nil)
	response["error"] = map[string]any{
		"code":    code,
		"message": message,
	}
	return stream.event("response.failed", map[string]any{
		"type":     "response.failed",
		"response": response,
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
	var usage any
	if status == "completed" {
		usage = responsesUsage()
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
		"parallel_tool_calls":  stream.parallelToolCalls,
		"previous_response_id": nil,
		"reasoning": map[string]any{
			"effort": nil, "summary": nil,
		},
		"store":       false,
		"temperature": nil,
		"text": map[string]any{
			"format": map[string]string{"type": "text"},
		},
		"tool_choice": stream.toolChoice,
		"tools":       []any{},
		"top_p":       nil,
		"truncation":  "disabled",
		"usage":       usage,
		"metadata":    map[string]string{},
	}
}

func responsesUsage() map[string]any {
	return map[string]any{
		"input_tokens": 0, "output_tokens": 0, "total_tokens": 0,
		"input_tokens_details":  map[string]int{"cached_tokens": 0},
		"output_tokens_details": map[string]int{"reasoning_tokens": 0},
	}
}

func (stream *stream) messageOutput() map[string]any {
	return map[string]any{
		"id":      messageItemID(stream.responseID),
		"type":    "message",
		"status":  "completed",
		"role":    "assistant",
		"content": []any{outputTextPart(stream.text.String())},
	}
}

func (stream *stream) messageOutputInProgress() map[string]any {
	return map[string]any{
		"id":      messageItemID(stream.responseID),
		"type":    "message",
		"status":  "in_progress",
		"role":    "assistant",
		"content": []any{},
	}
}

func (stream *stream) incompleteMessageOutput() map[string]any {
	return map[string]any{
		"id":      messageItemID(stream.responseID),
		"type":    "message",
		"status":  "incomplete",
		"role":    "assistant",
		"content": []any{outputTextPart(stream.text.String())},
	}
}

func (stream *stream) functionOutput(call completion.ToolCall, arguments, status string) map[string]any {
	item := map[string]any{
		"id":        functionItemID(call.ID),
		"type":      "function_call",
		"status":    status,
		"call_id":   call.ID,
		"name":      call.Name,
		"arguments": arguments,
	}
	if call.Namespace != "" {
		item["namespace"] = call.Namespace
	}
	return item
}

func (stream *stream) customOutput(call completion.ToolCall, input, status string) map[string]any {
	item := map[string]any{
		"id":      customItemID(call.ID),
		"type":    "custom_tool_call",
		"status":  status,
		"call_id": call.ID,
		"name":    call.Name,
		"input":   input,
	}
	if call.Namespace != "" {
		item["namespace"] = call.Namespace
	}
	return item
}

func outputTextPart(text string) map[string]any {
	return map[string]any{
		"type":        "output_text",
		"text":        text,
		"annotations": []any{},
		"logprobs":    []any{},
	}
}

func marshalToolArguments(input map[string]any) ([]byte, error) {
	if input == nil {
		input = map[string]any{}
	}
	return json.Marshal(input)
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

func customItemID(callID string) string {
	return "ctc_" + callID
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
