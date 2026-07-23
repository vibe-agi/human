package openai

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

func TestDecodeChatRequest(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
  "model":"human-expert",
  "stream":true,
  "messages":[
    {"role":"system","content":"be precise"},
    {"role":"user","content":[
      {"type":"text","text":"inspect"},
      {"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}
    ]},
    {"role":"assistant","content":null,"tool_calls":[{
      "id":"call-1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"/workspace/a\"}"}
    }]},
    {"role":"tool","tool_call_id":"call-1","content":"contents"}
  ],
  "tools":[{"type":"function","function":{
    "name":"read_file","description":"read","parameters":{"type":"object"}
  }}]
}`)
	request, err := New().Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if request.Dialect != canonical.DialectOpenAIChat || request.System != "be precise" || len(request.Messages) != 3 {
		t.Fatalf("request = %+v", request)
	}
	if got := request.Messages[1].Blocks[0]; got.Type != canonical.BlockToolUse || got.ToolCallID != "call-1" {
		t.Fatalf("tool use = %+v", got)
	}
	if got := request.Messages[2].Blocks[0]; got.Type != canonical.BlockToolResult || got.Output != "contents" {
		t.Fatalf("tool result = %+v", got)
	}
}

func TestDecodeChatAcceptsDeveloperInstructions(t *testing.T) {
	t.Parallel()
	request, err := New().Decode([]byte(`{
  "model":"human-expert",
  "stream":true,
  "messages":[
    {"role":"developer","content":"follow the repository instructions"},
    {"role":"user","content":"inspect the workspace"}
  ]
}`))
	if err != nil {
		t.Fatal(err)
	}
	if request.System != "follow the repository instructions" || len(request.Messages) != 1 ||
		request.Messages[0].Role != canonical.RoleUser {
		t.Fatalf("request = %+v", request)
	}
}

func TestDecodeChatMultipleToolResultsRemainIndependent(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
  "model":"human-expert",
  "stream":true,
  "messages":[
    {"role":"user","content":"inspect both files"},
    {"role":"assistant","content":null,"tool_calls":[
      {"id":"call-read-a","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"/workspace/a\"}"}},
      {"id":"call-read-b","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"/workspace/b\"}"}}
    ]},
    {"role":"tool","tool_call_id":"call-read-a","content":"contents-a"},
    {"role":"tool","tool_call_id":"call-read-b","content":"contents-b"}
  ],
  "tools":[{"type":"function","function":{"name":"read_file","parameters":{"type":"object"}}}]
}`)
	request, err := New().Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Messages) != 4 {
		t.Fatalf("messages = %+v", request.Messages)
	}
	toolUses := request.Messages[1].Blocks
	if len(toolUses) != 2 || toolUses[0].ToolCallID != "call-read-a" || toolUses[1].ToolCallID != "call-read-b" {
		t.Fatalf("tool uses = %+v", toolUses)
	}
	firstResult := request.Messages[2].Blocks
	secondResult := request.Messages[3].Blocks
	if len(firstResult) != 1 || firstResult[0].Type != canonical.BlockToolResult ||
		firstResult[0].ToolCallID != "call-read-a" || firstResult[0].Output != "contents-a" {
		t.Fatalf("first tool result = %+v", firstResult)
	}
	if len(secondResult) != 1 || secondResult[0].Type != canonical.BlockToolResult ||
		secondResult[0].ToolCallID != "call-read-b" || secondResult[0].Output != "contents-b" {
		t.Fatalf("second tool result = %+v", secondResult)
	}
}

func TestDecodeAcceptsExplicitNoopFieldsRejectsUnknownAndSupportsNonStreaming(t *testing.T) {
	t.Parallel()
	codec := New()
	if _, err := codec.Decode([]byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hello"}],"temperature":0.1}`)); err != nil {
		t.Fatalf("documented no-op field rejected: %v", err)
	}
	if _, err := codec.Decode([]byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hello"}],"future_control":true}`)); err == nil {
		t.Fatal("unknown top-level control accepted")
	}
	request, err := codec.Decode([]byte(`{"model":"m","stream":false,"messages":[{"role":"user","content":"hello"}]}`))
	if err != nil || request.Stream {
		t.Fatalf("non-streaming request = %+v, %v", request, err)
	}
}

func TestDecodeChatAcceptsDefaultControlsAndNormalizesEmptyTools(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
  "model":"m","stream":true,
  "messages":[
    {"role":"user","content":"hello"},
    {"role":"assistant","content":null,"tool_calls":[
      {"id":"call-empty","type":"function","function":{"name":"empty","arguments":"null"}}
    ]}
  ],
  "tools":[{"type":"function","function":{"name":"empty"}}],
  "tool_choice":"auto","response_format":{"type":"text"},"parallel_tool_calls":true
}`)
	request, err := New().Decode(payload)
	if err != nil {
		t.Fatalf("Decode() rejected supported defaults: %v", err)
	}
	if got := request.Messages[1].Blocks[0].Input; got == nil || len(got) != 0 {
		t.Fatalf("empty tool arguments = %#v, want non-nil empty object", got)
	}
	if got := string(request.Tools[0].InputSchema); got != `{"type":"object","properties":{}}` {
		t.Fatalf("default tool schema = %s", got)
	}
	serial, err := New().Decode([]byte(`{
  "model":"m","stream":true,"parallel_tool_calls":false,
  "messages":[{"role":"user","content":"hello"}],
  "tools":[{"type":"function","function":{"name":"empty"}}]
}`))
	if err != nil || serial.ToolCallPolicy != canonical.ToolCallsSerial {
		t.Fatalf("serial tool-call policy = %q, %v", serial.ToolCallPolicy, err)
	}
	disabled, err := New().Decode([]byte(`{
	  "model":"m","stream":true,"tool_choice":"none",
	  "messages":[{"role":"user","content":"hello"}],
	  "tools":[{"type":"function","function":{"name":"empty"}}]
	}`))
	if err != nil || disabled.ToolCallPolicy != canonical.ToolCallsDisabled || len(disabled.Tools) != 0 {
		t.Fatalf("disabled tool-call policy = %q, tools=%+v, %v", disabled.ToolCallPolicy, disabled.Tools, err)
	}
}

func TestDecodeChatRejectsUnsupportedOutputControls(t *testing.T) {
	t.Parallel()
	codec := New()
	tests := []struct {
		name  string
		field string
	}{
		{name: "required tool choice", field: `"tool_choice":"required"`},
		{name: "specific tool choice", field: `"tool_choice":{"type":"function","function":{"name":"read_file"}}`},
		{name: "JSON object response", field: `"response_format":{"type":"json_object"}`},
		{name: "JSON schema response", field: `"response_format":{"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object"}}}`},
		{name: "text response with hidden schema", field: `"response_format":{"type":"text","json_schema":{"name":"answer","schema":{"type":"object"}}}`},
		{name: "text response with unknown control", field: `"response_format":{"type":"text","future_control":true}`},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			payload := `{"model":"m","stream":true,"messages":[{"role":"user","content":"hello"}],` + test.field + `}`
			if _, err := codec.Decode([]byte(payload)); err == nil {
				t.Fatal("Decode() accepted unsupported control")
			}
		})
	}
}

func TestOpenAITextStream(t *testing.T) {
	t.Parallel()
	codec := Codec{now: func() time.Time { return time.Unix(100, 0) }}
	stream := codec.NewStream("response-1", "human-expert")
	start, err := stream.Start()
	if err != nil {
		t.Fatal(err)
	}
	if len(start) != 1 || !strings.Contains(string(start[0]), `"role":"assistant"`) {
		t.Fatalf("start = %q", start)
	}
	progress, done, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "working"})
	if err != nil || done || !strings.Contains(string(progress[0]), `"content":"working"`) {
		t.Fatalf("progress = %q, %v, %v", progress, done, err)
	}
	final, done, err := stream.Encode(completion.Event{Type: completion.EventFinal, Text: "fixed"})
	if err != nil || !done || len(final) != 3 || string(final[2]) != "data: [DONE]\n\n" {
		t.Fatalf("final = %q, %v, %v", final, done, err)
	}
}

func TestOpenAIToolStream(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("response-1", "human-expert")
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	frames, done, err := stream.Encode(completion.Event{
		Type:      completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{ID: "call-1", Name: "read_file", Input: map[string]any{"path": "/workspace/a"}}},
	})
	if err != nil || !done {
		t.Fatalf("Encode() = %q, %v, %v", frames, done, err)
	}
	if !strings.Contains(string(frames[0]), `"finish_reason":null`) || !strings.Contains(string(frames[1]), `"finish_reason":"tool_calls"`) {
		t.Fatalf("tool frames = %q", frames)
	}
	line := strings.TrimSpace(strings.TrimPrefix(string(frames[0]), "data: "))
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("invalid tool SSE JSON: %v", err)
	}
}

func TestOpenAINilToolInputEncodedAsObject(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("response-empty", "human-expert")
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	frames, done, err := stream.Encode(completion.Event{
		Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{
			ID: "call-empty", Name: "no_arguments", Input: nil,
		}},
	})
	if err != nil || !done || len(frames) != 3 {
		t.Fatalf("Encode() = %q, %v, %v", frames, done, err)
	}
	if !strings.Contains(string(frames[0]), `"arguments":"{}"`) {
		t.Fatalf("nil tool input frame = %q", frames[0])
	}
}

func TestOpenAIAdmissionServerErrorType(t *testing.T) {
	t.Parallel()
	for _, status := range []int{500, 503, 599} {
		if got := string(New().AdmissionError(status, "server_failure", "failed")); !strings.Contains(got, `"type":"server_error"`) {
			t.Fatalf("status %d admission error = %s", status, got)
		}
	}
}

func TestOpenAIProgressThenMultipleToolCalls(t *testing.T) {
	t.Parallel()
	type functionDelta struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	type toolCallDelta struct {
		Index    int           `json:"index"`
		ID       string        `json:"id"`
		Type     string        `json:"type"`
		Function functionDelta `json:"function"`
	}
	type streamDelta struct {
		Content   string          `json:"content"`
		ToolCalls []toolCallDelta `json:"tool_calls"`
	}
	type streamChoice struct {
		Index        int         `json:"index"`
		Delta        streamDelta `json:"delta"`
		FinishReason *string     `json:"finish_reason"`
	}
	type streamChunk struct {
		ID      string         `json:"id"`
		Object  string         `json:"object"`
		Created int64          `json:"created"`
		Model   string         `json:"model"`
		Choices []streamChoice `json:"choices"`
	}
	decodeFrame := func(frame []byte) streamChunk {
		t.Helper()
		line := strings.TrimSpace(string(frame))
		if !strings.HasPrefix(line, "data: ") {
			t.Fatalf("SSE frame = %q", frame)
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &chunk); err != nil {
			t.Fatalf("decode SSE chunk: %v", err)
		}
		if chunk.ID != "response-multi" || chunk.Object != "chat.completion.chunk" ||
			chunk.Created != 123 || chunk.Model != "human-expert" || len(chunk.Choices) != 1 || chunk.Choices[0].Index != 0 {
			t.Fatalf("chunk envelope = %+v", chunk)
		}
		return chunk
	}

	codec := Codec{now: func() time.Time { return time.Unix(123, 0) }}
	stream := codec.NewStream("response-multi", "human-expert")
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	progressFrames, done, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "checking both tools"})
	if err != nil || done || len(progressFrames) != 1 {
		t.Fatalf("progress Encode() = %q, %v, %v", progressFrames, done, err)
	}
	progress := decodeFrame(progressFrames[0]).Choices[0]
	if progress.Delta.Content != "checking both tools" || len(progress.Delta.ToolCalls) != 0 || progress.FinishReason != nil {
		t.Fatalf("progress choice = %+v", progress)
	}

	calls := []completion.ToolCall{
		{ID: "call-read", Name: "read_file", Input: map[string]any{"path": "/workspace/a.go", "line": 12}},
		{ID: "call-search", Name: "search", Input: map[string]any{"query": "needle", "include": []any{"*.go", "*.md"}}},
	}
	frames, done, err := stream.Encode(completion.Event{Type: completion.EventToolCalls, ToolCalls: calls})
	if err != nil || !done || len(frames) != 3 {
		t.Fatalf("tool Encode() = %q, %v, %v", frames, done, err)
	}
	toolChoice := decodeFrame(frames[0]).Choices[0]
	if toolChoice.FinishReason != nil || len(toolChoice.Delta.ToolCalls) != len(calls) {
		t.Fatalf("tool choice = %+v", toolChoice)
	}
	for index, expected := range calls {
		actual := toolChoice.Delta.ToolCalls[index]
		if actual.Index != index || actual.ID != expected.ID || actual.Type != "function" || actual.Function.Name != expected.Name {
			t.Fatalf("tool call %d = %+v", index, actual)
		}
		arguments, marshalErr := json.Marshal(expected.Input)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if actual.Function.Arguments != string(arguments) {
			t.Fatalf("tool call %d arguments = %q, want %q", index, actual.Function.Arguments, arguments)
		}
	}
	finish := decodeFrame(frames[1]).Choices[0]
	if finish.FinishReason == nil || *finish.FinishReason != "tool_calls" || len(finish.Delta.ToolCalls) != 0 {
		t.Fatalf("finish choice = %+v", finish)
	}
	if string(frames[2]) != "data: [DONE]\n\n" {
		t.Fatalf("done frame = %q", frames[2])
	}
}

func TestOpenAIStreamError(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("response-1", "human-expert")
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	frames, done, err := stream.Encode(completion.Event{Type: completion.EventRejected, ErrorCode: "rejected", Error: "human rejected task"})
	if err != nil || !done || !strings.Contains(string(frames[0]), `"code":"rejected"`) {
		t.Fatalf("error frames = %q, %v, %v", frames, done, err)
	}
}
