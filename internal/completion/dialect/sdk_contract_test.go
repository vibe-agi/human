package dialect_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	anthropicoption "github.com/anthropics/anthropic-sdk-go/option"
	openaisdk "github.com/openai/openai-go/v3"
	openaioption "github.com/openai/openai-go/v3/option"
	openairesponses "github.com/openai/openai-go/v3/responses"
	openaishared "github.com/openai/openai-go/v3/shared"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
	anthropicdialect "github.com/vibe-agi/human/internal/completion/dialect/anthropic"
	openaidialect "github.com/vibe-agi/human/internal/completion/dialect/openai"
	responsesdialect "github.com/vibe-agi/human/internal/completion/dialect/responses"
	"github.com/vibe-agi/human/llm/callerhttp"
)

func TestAnthropicSDKMessagesContract(t *testing.T) {
	t.Parallel()
	var sawToolResult atomic.Bool
	server := newSDKCodecServer(t, "/v1/messages", anthropicdialect.New(), func(request canonical.Request) {
		inspectSDKToolResult(t, &sawToolResult, request)
		if request.ToolCallPolicy == canonical.ToolCallsDisabled {
			if len(request.Tools) != 0 {
				t.Errorf("Anthropic SDK disabled tools = %+v", request.Tools)
			}
			return
		}
		if len(request.Tools) != 1 || request.Tools[0].Name != "read_file" {
			t.Errorf("Anthropic SDK tools = %+v", request.Tools)
		}
		if request.ToolCallPolicy != canonical.ToolCallsSerial {
			t.Errorf("Anthropic SDK tool policy = %q", request.ToolCallPolicy)
		}
	})
	defer server.Close()

	disabledThinking := anthropic.NewThinkingConfigDisabledParam()
	params := anthropic.MessageNewParams{
		MaxTokens: 1024,
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock("inspect README.md")),
		},
		Model:       anthropic.Model("human-expert"),
		Temperature: anthropic.Float(0.2),
		TopP:        anthropic.Float(0.9),
		Thinking: anthropic.ThinkingConfigParamUnion{
			OfDisabled: &disabledThinking,
		},
		OutputConfig: anthropic.OutputConfigParam{Effort: anthropic.OutputConfigEffortLow},
		ServiceTier:  anthropic.MessageNewParamsServiceTierAuto,
		ToolChoice: anthropic.ToolChoiceUnionParam{OfAuto: &anthropic.ToolChoiceAutoParam{
			DisableParallelToolUse: anthropic.Bool(true),
		}},
		StopSequences: []string{
			"<human-stop>",
		},
		Tools: []anthropic.ToolUnionParam{{
			OfTool: &anthropic.ToolParam{
				Name:        "read_file",
				Description: anthropic.String("Read a workspace file"),
				InputSchema: anthropic.ToolInputSchemaParam{
					Properties: map[string]any{"path": map[string]string{"type": "string"}},
					Required:   []string{"path"},
				},
			},
		}},
	}
	client := anthropic.NewClient(
		anthropicoption.WithAPIKey("sdk-contract"),
		anthropicoption.WithBaseURL(server.URL+"/"),
	)

	message, err := client.Messages.New(context.Background(), params)
	if err != nil {
		t.Fatalf("Anthropic SDK aggregate: %v", err)
	}
	if len(message.Content) != 1 {
		t.Fatalf("Anthropic SDK aggregate content = %+v", message.Content)
	}
	if !message.Usage.JSON.CacheCreationInputTokens.Valid() || !message.Usage.JSON.OutputTokensDetails.Valid() {
		t.Fatalf("Anthropic SDK usage shape = %+v", message.Usage)
	}
	call := message.Content[0].AsToolUse()
	if call.ID != "call-sdk" || call.Name != "read_file" {
		t.Fatalf("Anthropic SDK tool call = %+v", call)
	}
	continuation := params
	continuation.Messages = append(
		append([]anthropic.MessageParam{}, params.Messages...),
		message.ToParam(),
		anthropic.NewUserMessage(anthropic.NewToolResultBlock(call.ID, "file contents", false)),
	)
	reply, err := client.Messages.New(
		context.Background(), continuation,
		anthropicoption.WithHeader("X-Human-SDK-Terminal", "text"),
	)
	if err != nil || len(reply.Content) != 1 || reply.Content[0].AsText().Text != "sdk text" || !sawToolResult.Load() {
		t.Fatalf("Anthropic SDK tool_result continuation = %+v, saw=%t, %v", reply, sawToolResult.Load(), err)
	}

	stream := client.Messages.NewStreaming(context.Background(), params)
	var text strings.Builder
	for stream.Next() {
		switch event := stream.Current().AsAny().(type) {
		case anthropic.ContentBlockDeltaEvent:
			if delta, ok := event.Delta.AsAny().(anthropic.TextDelta); ok {
				text.WriteString(delta.Text)
			}
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("Anthropic SDK stream: %v", err)
	}
	if text.String() != "sdk text" {
		t.Fatalf("Anthropic SDK stream text = %q", text.String())
	}

	toolStream := client.Messages.NewStreaming(
		context.Background(), params,
		anthropicoption.WithHeader("X-Human-SDK-Terminal", "tool"),
	)
	var accumulated anthropic.Message
	for toolStream.Next() {
		if err := accumulated.Accumulate(toolStream.Current()); err != nil {
			t.Fatalf("accumulate Anthropic SDK tool stream: %v", err)
		}
	}
	if err := toolStream.Err(); err != nil {
		t.Fatalf("Anthropic SDK tool stream: %v", err)
	}
	if len(accumulated.Content) != 1 {
		t.Fatalf("Anthropic SDK accumulated tool stream = %+v", accumulated)
	}
	streamCall := accumulated.Content[0].AsToolUse()
	if streamCall.ID != "call-sdk" || streamCall.Name != "read_file" || string(streamCall.Input) != `{"path":"README.md"}` {
		t.Fatalf("Anthropic SDK accumulated stream tool call = %+v", streamCall)
	}
	none := anthropic.NewToolChoiceNoneParam()
	noneParams := params
	noneParams.ToolChoice = anthropic.ToolChoiceUnionParam{OfNone: &none}
	message, err = client.Messages.New(
		context.Background(), noneParams,
		anthropicoption.WithHeader("X-Human-SDK-Terminal", "text"),
	)
	if err != nil || message.Content[0].AsText().Text != "sdk text" {
		t.Fatalf("Anthropic SDK tool_choice:none = %+v, %v", message, err)
	}

	countServer := httptest.NewServer(callerhttp.CountTokensHandler())
	defer countServer.Close()
	countClient := anthropic.NewClient(
		anthropicoption.WithAPIKey("sdk-contract"),
		anthropicoption.WithBaseURL(countServer.URL+"/"),
	)
	count, err := countClient.Messages.CountTokens(context.Background(), anthropic.MessageCountTokensParams{
		Messages: params.Messages,
		Model:    params.Model,
	})
	if err != nil || count.InputTokens < 1 {
		t.Fatalf("Anthropic SDK count_tokens = %+v, %v", count, err)
	}
}

func TestOpenAISDKChatCompletionsContract(t *testing.T) {
	t.Parallel()
	var sawToolResult atomic.Bool
	server := newSDKCodecServer(t, "/v1/chat/completions", openaidialect.New(), func(request canonical.Request) {
		inspectSDKToolResult(t, &sawToolResult, request)
		if request.ToolCallPolicy == canonical.ToolCallsDisabled {
			if len(request.Tools) != 0 {
				t.Errorf("OpenAI SDK Chat disabled tools = %+v", request.Tools)
			}
			return
		}
		if len(request.Tools) != 1 || request.Tools[0].Name != "read_file" {
			t.Errorf("OpenAI SDK Chat tools = %+v", request.Tools)
		}
		if request.ToolCallPolicy != canonical.ToolCallsSerial {
			t.Errorf("OpenAI SDK Chat tool policy = %q", request.ToolCallPolicy)
		}
	})
	defer server.Close()

	params := openaisdk.ChatCompletionNewParams{
		Model:               openaisdk.ChatModel("human-expert"),
		Messages:            []openaisdk.ChatCompletionMessageParamUnion{openaisdk.UserMessage("inspect README.md")},
		MaxCompletionTokens: openaisdk.Int(1024),
		ParallelToolCalls:   openaisdk.Bool(false),
		Temperature:         openaisdk.Float(0.2),
		ReasoningEffort:     openaisdk.ReasoningEffortLow,
		StreamOptions: openaisdk.ChatCompletionStreamOptionsParam{
			IncludeObfuscation: openaisdk.Bool(false),
			IncludeUsage:       openaisdk.Bool(true),
		},
		Tools: []openaisdk.ChatCompletionToolUnionParam{
			openaisdk.ChatCompletionFunctionTool(openaisdk.FunctionDefinitionParam{
				Name:        "read_file",
				Description: openaisdk.String("Read a workspace file"),
				Parameters: openaisdk.FunctionParameters{
					"type":       "object",
					"properties": map[string]any{"path": map[string]string{"type": "string"}},
					"required":   []string{"path"},
				},
			}),
		},
	}
	client := openaisdk.NewClient(
		openaioption.WithAPIKey("sdk-contract"),
		openaioption.WithBaseURL(server.URL+"/v1/"),
	)

	message, err := client.Chat.Completions.New(context.Background(), params)
	if err != nil {
		t.Fatalf("OpenAI SDK Chat aggregate: %v", err)
	}
	if len(message.Choices) != 1 || len(message.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("OpenAI SDK Chat aggregate = %+v", message)
	}
	if !message.JSON.Usage.Valid() || message.Usage.TotalTokens != 0 {
		t.Fatalf("OpenAI SDK Chat usage = %+v", message.Usage)
	}
	call := message.Choices[0].Message.ToolCalls[0].AsFunction()
	if call.ID != "call-sdk" || call.Function.Name != "read_file" {
		t.Fatalf("OpenAI SDK Chat tool call = %+v", call)
	}
	continuation := params
	continuation.Messages = append(
		append([]openaisdk.ChatCompletionMessageParamUnion{}, params.Messages...),
		message.Choices[0].Message.ToParam(),
		openaisdk.ToolMessage("file contents", call.ID),
	)
	reply, err := client.Chat.Completions.New(
		context.Background(), continuation,
		openaioption.WithHeader("X-Human-SDK-Terminal", "text"),
	)
	if err != nil || len(reply.Choices) != 1 || reply.Choices[0].Message.Content != "sdk text" || !sawToolResult.Load() {
		t.Fatalf("OpenAI SDK Chat tool result continuation = %+v, saw=%t, %v", reply, sawToolResult.Load(), err)
	}

	stream := client.Chat.Completions.NewStreaming(context.Background(), params)
	var text strings.Builder
	terminalUsage := false
	for stream.Next() {
		chunk := stream.Current()
		if len(chunk.Choices) != 0 {
			text.WriteString(chunk.Choices[0].Delta.Content)
		}
		terminalUsage = terminalUsage || chunk.JSON.Usage.Valid()
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("OpenAI SDK Chat stream: %v", err)
	}
	if text.String() != "sdk text" {
		t.Fatalf("OpenAI SDK Chat stream text = %q", text.String())
	}
	if !terminalUsage {
		t.Fatal("OpenAI SDK Chat stream omitted terminal usage")
	}

	toolStream := client.Chat.Completions.NewStreaming(
		context.Background(), params,
		openaioption.WithHeader("X-Human-SDK-Terminal", "tool"),
	)
	var accumulated openaisdk.ChatCompletionAccumulator
	for toolStream.Next() {
		if !accumulated.AddChunk(toolStream.Current()) {
			t.Fatal("OpenAI SDK Chat accumulator rejected a tool stream chunk")
		}
	}
	if err := toolStream.Err(); err != nil {
		t.Fatalf("OpenAI SDK Chat tool stream: %v", err)
	}
	if len(accumulated.Choices) != 1 || len(accumulated.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("OpenAI SDK Chat accumulated tool stream = %+v", accumulated.ChatCompletion)
	}
	streamCall := accumulated.Choices[0].Message.ToolCalls[0]
	if streamCall.ID != "call-sdk" || streamCall.Function.Name != "read_file" || streamCall.Function.Arguments != `{"path":"README.md"}` {
		t.Fatalf("OpenAI SDK Chat accumulated stream tool call = %+v", streamCall)
	}
	noneParams := params
	noneParams.ToolChoice = openaisdk.ChatCompletionToolChoiceOptionUnionParam{
		OfAuto: openaisdk.String("none"),
	}
	message, err = client.Chat.Completions.New(
		context.Background(), noneParams,
		openaioption.WithHeader("X-Human-SDK-Terminal", "text"),
	)
	if err != nil || len(message.Choices) != 1 || message.Choices[0].Message.Content != "sdk text" {
		t.Fatalf("OpenAI SDK Chat tool_choice:none = %+v, %v", message, err)
	}
}

func TestOpenAISDKResponsesContract(t *testing.T) {
	t.Parallel()
	var sawToolResult atomic.Bool
	server := newSDKCodecServer(t, "/v1/responses", responsesdialect.New(), func(request canonical.Request) {
		inspectSDKToolResult(t, &sawToolResult, request)
		if request.ToolCallPolicy == canonical.ToolCallsDisabled {
			if len(request.Tools) != 0 {
				t.Errorf("OpenAI SDK Responses disabled tools = %+v", request.Tools)
			}
			return
		}
		if len(request.Tools) != 1 || request.Tools[0].Name != "read_file" {
			t.Errorf("OpenAI SDK Responses tools = %+v", request.Tools)
		}
		if request.ToolCallPolicy != canonical.ToolCallsSerial {
			t.Errorf("OpenAI SDK Responses tool policy = %q", request.ToolCallPolicy)
		}
	})
	defer server.Close()

	params := openairesponses.ResponseNewParams{
		Model:             openaisdk.ResponsesModel("human-expert"),
		Input:             openairesponses.ResponseNewParamsInputUnion{OfString: openaisdk.String("inspect README.md")},
		MaxOutputTokens:   openaisdk.Int(1024),
		ParallelToolCalls: openaisdk.Bool(false),
		Store:             openaisdk.Bool(false),
		Temperature:       openaisdk.Float(0.2),
		PromptCacheKey:    openaisdk.String("sdk-contract"),
		Reasoning: openaisdk.ReasoningParam{
			Effort:  openaisdk.ReasoningEffortLow,
			Summary: openaisdk.ReasoningSummaryAuto,
		},
		ServiceTier: openairesponses.ResponseNewParamsServiceTierAuto,
		StreamOptions: openairesponses.ResponseNewParamsStreamOptions{
			IncludeObfuscation: openaisdk.Bool(false),
		},
		Truncation: openairesponses.ResponseNewParamsTruncationDisabled,
		Tools: []openairesponses.ToolUnionParam{{
			OfFunction: &openairesponses.FunctionToolParam{
				Name:        "read_file",
				Description: openaisdk.String("Read a workspace file"),
				Strict:      openaisdk.Bool(true),
				Parameters: map[string]any{
					"type":       "object",
					"properties": map[string]any{"path": map[string]string{"type": "string"}},
					"required":   []string{"path"},
				},
			},
		}},
	}
	client := openaisdk.NewClient(
		openaioption.WithAPIKey("sdk-contract"),
		openaioption.WithBaseURL(server.URL+"/v1/"),
	)

	response, err := client.Responses.New(context.Background(), params)
	if err != nil {
		t.Fatalf("OpenAI SDK Responses aggregate: %v", err)
	}
	if len(response.Output) != 1 {
		t.Fatalf("OpenAI SDK Responses aggregate = %+v", response)
	}
	if !response.JSON.Usage.Valid() || response.Usage.TotalTokens != 0 {
		t.Fatalf("OpenAI SDK Responses usage = %+v", response.Usage)
	}
	call := response.Output[0].AsFunctionCall()
	if call.CallID != "call-sdk" || call.Name != "read_file" {
		t.Fatalf("OpenAI SDK Responses tool call = %+v", call)
	}
	continuation := params
	continuation.Input = openairesponses.ResponseNewParamsInputUnion{
		OfInputItemList: openairesponses.ResponseInputParam{
			openairesponses.ResponseInputItemParamOfFunctionCall(call.Arguments, call.CallID, call.Name),
			openairesponses.ResponseInputItemParamOfFunctionCallOutput(call.CallID, "file contents"),
		},
	}
	reply, err := client.Responses.New(
		context.Background(), continuation,
		openaioption.WithHeader("X-Human-SDK-Terminal", "text"),
	)
	if err != nil || reply.OutputText() != "sdk text" || !sawToolResult.Load() {
		t.Fatalf("OpenAI SDK Responses function_call_output continuation = %+v, saw=%t, %v", reply, sawToolResult.Load(), err)
	}

	stream := client.Responses.NewStreaming(context.Background(), params)
	var text strings.Builder
	for stream.Next() {
		event := stream.Current()
		if event.Type == "response.output_text.delta" {
			text.WriteString(event.Delta)
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("OpenAI SDK Responses stream: %v", err)
	}
	if text.String() != "sdk text" {
		t.Fatalf("OpenAI SDK Responses stream text = %q", text.String())
	}

	toolStream := client.Responses.NewStreaming(
		context.Background(), params,
		openaioption.WithHeader("X-Human-SDK-Terminal", "tool"),
	)
	var streamCall openairesponses.ResponseFunctionToolCall
	for toolStream.Next() {
		event := toolStream.Current()
		if event.Type == "response.output_item.done" {
			streamCall = event.Item.AsFunctionCall()
		}
	}
	if err := toolStream.Err(); err != nil {
		t.Fatalf("OpenAI SDK Responses tool stream: %v", err)
	}
	if streamCall.CallID != "call-sdk" || streamCall.Name != "read_file" || streamCall.Arguments != `{"path":"README.md"}` {
		t.Fatalf("OpenAI SDK Responses stream tool call = %+v", streamCall)
	}
	noneParams := params
	noneParams.ToolChoice = openairesponses.ResponseNewParamsToolChoiceUnion{
		OfToolChoiceMode: openaisdk.Opt(openairesponses.ToolChoiceOptionsNone),
	}
	response, err = client.Responses.New(
		context.Background(), noneParams,
		openaioption.WithHeader("X-Human-SDK-Terminal", "text"),
	)
	if err != nil || response.OutputText() != "sdk text" ||
		response.ToolChoice.OfToolChoiceMode != openairesponses.ToolChoiceOptionsNone {
		t.Fatalf("OpenAI SDK Responses tool_choice:none = %+v, %v", response, err)
	}
}

func TestOpenAISDKResponsesCustomToolContract(t *testing.T) {
	t.Parallel()
	const patch = "*** Begin Patch\n*** Add File: hello.txt\n+hello\n*** End Patch"
	var sawToolResult atomic.Bool
	server := newSDKCodecServer(t, "/v1/responses", responsesdialect.New(), func(request canonical.Request) {
		inspectSDKToolResult(t, &sawToolResult, request)
		if len(request.Tools) != 1 {
			t.Errorf("OpenAI SDK Responses custom tools = %+v", request.Tools)
			return
		}
		tool := request.Tools[0]
		var format struct {
			Type       string `json:"type"`
			Definition string `json:"definition"`
			Syntax     string `json:"syntax"`
		}
		formatErr := json.Unmarshal(tool.InputFormat, &format)
		if tool.Name != "apply_patch" || tool.InputKind != canonical.ToolInputText ||
			formatErr != nil || format.Type != "grammar" ||
			format.Definition != "start: PATCH" || format.Syntax != "lark" {
			t.Errorf("OpenAI SDK Responses custom tool = %+v", tool)
		}
		for _, message := range request.Messages {
			for _, block := range message.Blocks {
				if block.Type == canonical.BlockToolUse &&
					(block.TextInput == nil || *block.TextInput != patch) {
					t.Errorf("OpenAI SDK Responses custom call input = %+v", block)
				}
			}
		}
	})
	defer server.Close()

	custom := openairesponses.CustomToolParam{
		Name:        "apply_patch",
		Description: openaisdk.String("Apply a patch to the workspace"),
		Format:      openaishared.CustomToolInputFormatParamOfGrammar("start: PATCH", "lark"),
	}
	params := openairesponses.ResponseNewParams{
		Model: openaisdk.ResponsesModel("human-expert"),
		Input: openairesponses.ResponseNewParamsInputUnion{
			OfString: openaisdk.String("create hello.txt"),
		},
		Tools: []openairesponses.ToolUnionParam{{OfCustom: &custom}},
	}
	client := openaisdk.NewClient(
		openaioption.WithAPIKey("sdk-contract"),
		openaioption.WithBaseURL(server.URL+"/v1/"),
	)

	response, err := client.Responses.New(context.Background(), params)
	if err != nil {
		t.Fatalf("OpenAI SDK Responses custom aggregate: %v", err)
	}
	if len(response.Output) != 1 {
		t.Fatalf("OpenAI SDK Responses custom aggregate = %+v", response)
	}
	call := response.Output[0].AsCustomToolCall()
	if call.CallID != "call-sdk" || call.Name != "apply_patch" || call.Input != patch {
		t.Fatalf("OpenAI SDK Responses custom call = %+v", call)
	}

	continuation := params
	continuation.Input = openairesponses.ResponseNewParamsInputUnion{
		OfInputItemList: openairesponses.ResponseInputParam{
			openairesponses.ResponseInputItemParamOfCustomToolCall(call.CallID, call.Input, call.Name),
			openairesponses.ResponseInputItemParamOfCustomToolCallOutput(call.CallID, "file contents"),
		},
	}
	reply, err := client.Responses.New(
		context.Background(), continuation,
		openaioption.WithHeader("X-Human-SDK-Terminal", "text"),
	)
	if err != nil || reply.OutputText() != "sdk text" || !sawToolResult.Load() {
		t.Fatalf("OpenAI SDK Responses custom continuation = %+v, saw=%t, %v", reply, sawToolResult.Load(), err)
	}

	stream := client.Responses.NewStreaming(
		context.Background(), params,
		openaioption.WithHeader("X-Human-SDK-Terminal", "tool"),
	)
	var streamCall openairesponses.ResponseCustomToolCall
	sawDelta := false
	sawDone := false
	for stream.Next() {
		event := stream.Current()
		switch event.Type {
		case "response.custom_tool_call_input.delta":
			sawDelta = event.Delta == patch
		case "response.custom_tool_call_input.done":
			sawDone = event.Input == patch
		case "response.output_item.done":
			streamCall = event.Item.AsCustomToolCall()
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("OpenAI SDK Responses custom stream: %v", err)
	}
	if !sawDelta || !sawDone || streamCall.CallID != "call-sdk" ||
		streamCall.Name != "apply_patch" || streamCall.Input != patch {
		t.Fatalf(
			"OpenAI SDK Responses custom stream = call %+v, delta=%t, done=%t",
			streamCall, sawDelta, sawDone,
		)
	}
}

func TestOfficialSDKErrorContracts(t *testing.T) {
	t.Run("anthropic", func(t *testing.T) {
		t.Parallel()
		codec := anthropicdialect.New()
		server := newSDKErrorServer(t, codec.OverloadedStatus(), codec.AdmissionError(
			codec.OverloadedStatus(), "overloaded_error", "no human is available",
		))
		defer server.Close()
		client := anthropic.NewClient(
			anthropicoption.WithAPIKey("sdk-contract"),
			anthropicoption.WithBaseURL(server.URL+"/"),
			anthropicoption.WithMaxRetries(0),
		)
		_, err := client.Messages.New(context.Background(), anthropic.MessageNewParams{
			MaxTokens: 1,
			Messages:  []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock("hello"))},
			Model:     anthropic.Model("human-expert"),
		})
		var apiError *anthropic.Error
		if !errors.As(err, &apiError) || apiError.StatusCode != codec.OverloadedStatus() || string(apiError.Type()) != "overloaded_error" {
			t.Fatalf("Anthropic SDK error = %#v (%v)", apiError, err)
		}
	})

	t.Run("openai", func(t *testing.T) {
		t.Parallel()
		codec := openaidialect.New()
		server := newSDKErrorServer(t, http.StatusTooManyRequests, codec.AdmissionError(
			http.StatusTooManyRequests, "rate_limit_exceeded", "no human is available",
		))
		defer server.Close()
		client := openaisdk.NewClient(
			openaioption.WithAPIKey("sdk-contract"),
			openaioption.WithBaseURL(server.URL+"/v1/"),
			openaioption.WithMaxRetries(0),
		)
		_, err := client.Chat.Completions.New(context.Background(), openaisdk.ChatCompletionNewParams{
			Model:    openaisdk.ChatModel("human-expert"),
			Messages: []openaisdk.ChatCompletionMessageParamUnion{openaisdk.UserMessage("hello")},
		})
		var apiError *openaisdk.Error
		if !errors.As(err, &apiError) || apiError.StatusCode != http.StatusTooManyRequests ||
			apiError.Type != "rate_limit_error" || apiError.Code != "rate_limit_exceeded" ||
			apiError.Message != "no human is available" {
			t.Fatalf("OpenAI SDK error = %#v (%v)", apiError, err)
		}
	})
}

func newSDKCodecServer(
	t *testing.T,
	path string,
	codec dialect.Codec,
	inspect func(canonical.Request),
) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != path {
			http.Error(response, "unexpected SDK route", http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		decoded, err := codec.Decode(body)
		if err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		inspect(decoded)

		var encoder dialect.Encoder
		seed := dialect.StreamSeed{CreatedAtUnix: 1, ToolCallPolicy: decoded.ToolCallPolicy}
		if decoded.Stream {
			encoder = codec.NewStream("response-sdk", decoded.Model, seed)
			response.Header().Set("Content-Type", "text/event-stream")
		} else {
			encoder = codec.NewAggregate("response-sdk", decoded.Model, seed)
			response.Header().Set("Content-Type", "application/json")
		}
		frames, err := encoder.Start()
		if err != nil {
			http.Error(response, err.Error(), http.StatusInternalServerError)
			return
		}
		terminal := completion.Event{Type: completion.EventFinal, Text: "sdk text"}
		if request.Header.Get("X-Human-SDK-Terminal") != "text" &&
			(!decoded.Stream || request.Header.Get("X-Human-SDK-Terminal") == "tool") {
			call := completion.ToolCall{
				ID: "call-sdk", Name: "read_file", Input: map[string]any{"path": "README.md"},
			}
			if len(decoded.Tools) == 1 && decoded.Tools[0].InputKind == canonical.ToolInputText {
				input := "*** Begin Patch\n*** Add File: hello.txt\n+hello\n*** End Patch"
				call = completion.ToolCall{
					ID: "call-sdk", Name: decoded.Tools[0].Name, TextInput: &input,
				}
			}
			terminal = completion.Event{Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{call}}
		}
		terminalFrames, done, err := encoder.Encode(terminal, dialect.EventSeed{EncodedAtUnix: 2})
		if err != nil || !done {
			http.Error(response, "codec did not complete", http.StatusInternalServerError)
			return
		}
		for _, frame := range append(frames, terminalFrames...) {
			if _, err := response.Write(frame); err != nil {
				return
			}
		}
	}))
}

func inspectSDKToolResult(t *testing.T, saw *atomic.Bool, request canonical.Request) {
	t.Helper()
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Type != canonical.BlockToolResult {
				continue
			}
			if block.ToolCallID != "call-sdk" || !sdkToolOutputIsFileContents(block.Output) {
				t.Errorf("official SDK tool result = %+v", block)
				return
			}
			saw.Store(true)
		}
	}
}

func sdkToolOutputIsFileContents(output any) bool {
	if text, ok := output.(string); ok {
		return text == "file contents"
	}
	blocks, ok := output.([]canonical.Block)
	return ok && len(blocks) == 1 && blocks[0].Type == canonical.BlockText && blocks[0].Text == "file contents"
}

func newSDKErrorServer(t *testing.T, status int, body []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(status)
		_, _ = response.Write(body)
	}))
}
