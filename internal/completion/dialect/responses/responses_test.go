package responses

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

func TestDecodeResponsesRequest(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
  "model":"human-expert",
  "stream":true,
  "instructions":"be precise",
  "metadata":{"caller":"caller-1"},
  "input":[
    {"type":"message","role":"developer","content":"show evidence"},
    {"type":"message","role":"user","content":[
      {"type":"input_text","text":"inspect"},
      {"type":"input_image","image_url":"data:image/png;base64,AA=="}
    ]},
    {"type":"function_call","id":"fc-1","call_id":"call-1","name":"read_file","arguments":"{\"path\":\"/workspace/a\"}"},
    {"type":"function_call_output","call_id":"call-1","output":"contents"},
    {"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}
  ],
  "tools":[{
    "type":"function","name":"read_file","description":"read a file",
    "parameters":{"type":"object","properties":{"path":{"type":"string"}}},
    "strict":true
  }]
}`)

	request, err := New().Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if request.Dialect != canonical.DialectResponses || request.Model != "human-expert" || !request.Stream {
		t.Fatalf("request identity = %+v", request)
	}
	if request.System != "be precise\nshow evidence" || request.Metadata["caller"] != "caller-1" {
		t.Fatalf("system/metadata = %q / %+v", request.System, request.Metadata)
	}
	if len(request.Messages) != 4 {
		t.Fatalf("messages = %+v", request.Messages)
	}
	if got := request.Messages[0].Blocks[1]; got.Type != canonical.BlockImage || got.ImageURL != "data:image/png;base64,AA==" {
		t.Fatalf("image = %+v", got)
	}
	if got := request.Messages[1].Blocks[0]; got.Type != canonical.BlockToolUse || got.ToolCallID != "call-1" || got.ToolName != "read_file" {
		t.Fatalf("function call = %+v", got)
	}
	if got := request.Messages[2].Blocks[0]; got.Type != canonical.BlockToolResult || got.ToolCallID != "call-1" || got.Output != "contents" {
		t.Fatalf("function output = %+v", got)
	}
	if len(request.Tools) != 1 || request.Tools[0].Name != "read_file" || !json.Valid(request.Tools[0].InputSchema) {
		t.Fatalf("tools = %+v", request.Tools)
	}
}

func TestDecodeStringInputAndStructuredFunctionOutput(t *testing.T) {
	t.Parallel()
	request, err := New().Decode([]byte(`{
  "model":"human-expert","stream":true,"input":"hello"
}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Messages) != 1 || request.Messages[0].Blocks[0].Text != "hello" {
		t.Fatalf("request = %+v", request)
	}

	request, err = New().Decode([]byte(`{
  "model":"human-expert","stream":true,
  "input":[
    {"type":"message","role":"user","content":"continue"},
    {"type":"function_call_output","call_id":"call-1","output":{"ok":true}}
  ]
}`))
	if err != nil {
		t.Fatal(err)
	}
	output, ok := request.Messages[1].Blocks[0].Output.(map[string]any)
	if !ok || output["ok"] != true {
		t.Fatalf("structured output = %#v", request.Messages[1].Blocks[0].Output)
	}
}

func TestDecodeResponsesRejectsUnsupportedShapes(t *testing.T) {
	t.Parallel()
	codec := New()
	nonStreaming, err := codec.Decode([]byte(`{"model":"m","stream":false,"input":"hello"}`))
	if err != nil || nonStreaming.Stream {
		t.Fatalf("non-streaming request = %+v, %v", nonStreaming, err)
	}
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{
			name:    "non string instructions",
			payload: `{"model":"m","stream":true,"instructions":[{"role":"developer","content":"x"}],"input":"hello"}`,
		},
		{
			name:    "missing function output call id",
			payload: `{"model":"m","stream":true,"input":[{"type":"function_call_output","output":"x"}]}`,
		},
		{
			name:    "required tool choice",
			payload: `{"model":"m","stream":true,"input":"hello","tool_choice":"required"}`,
		},
		{
			name:    "specific tool choice",
			payload: `{"model":"m","stream":true,"input":"hello","tool_choice":{"type":"function","name":"read_file"}}`,
		},
		{
			name:    "structured text format",
			payload: `{"model":"m","stream":true,"input":"hello","text":{"format":{"type":"json_schema","name":"answer","schema":{"type":"object"}}}}`,
		},
		{
			name:    "text format with hidden schema",
			payload: `{"model":"m","stream":true,"input":"hello","text":{"format":{"type":"text","schema":{"type":"object"}}}}`,
		},
		{
			name:    "unknown text control",
			payload: `{"model":"m","stream":true,"input":"hello","text":{"future_control":true}}`,
		},
		{
			name:    "unknown top level control",
			payload: `{"model":"m","stream":true,"input":"hello","future_control":true}`,
		},
		{
			name:    "previous response",
			payload: `{"model":"m","stream":true,"input":"hello","previous_response_id":"resp_previous"}`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := codec.Decode([]byte(test.payload))
			if err == nil {
				t.Fatal("Decode() succeeded")
			}
			if test.wantErr != nil && err != test.wantErr {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestDecodeResponsesAllowsDefaultOutputControls(t *testing.T) {
	t.Parallel()
	_, err := New().Decode([]byte(`{
  "model":"m","stream":true,"input":"hello",
  "tool_choice":"auto","text":{"format":{"type":"text"},"verbosity":"medium"},
  "previous_response_id":null,"parallel_tool_calls":true
}`))
	if err != nil {
		t.Fatalf("Decode() rejected supported defaults: %v", err)
	}
}

func TestDecodeResponsesDisablesTools(t *testing.T) {
	t.Parallel()
	request, err := New().Decode([]byte(`{
	  "model":"m","stream":true,"input":"hello","tool_choice":"none",
	  "tools":[{"type":"function","name":"read_file","parameters":{"type":"object"}}]
	}`))
	if err != nil || request.ToolCallPolicy != canonical.ToolCallsDisabled || len(request.Tools) != 0 {
		t.Fatalf("disabled Responses tools = %q, tools=%+v, %v", request.ToolCallPolicy, request.Tools, err)
	}
	stream := New().NewStream("resp-disabled", "m", dialect.StreamSeed{
		CreatedAtUnix: 1, ToolCallPolicy: canonical.ToolCallsDisabled,
	})
	frames, err := stream.Start()
	if err != nil {
		t.Fatal(err)
	}
	created := assertEvent(t, frames[0], "response.created", 0)
	response := created["response"].(map[string]any)
	if response["tool_choice"] != "none" || response["parallel_tool_calls"] != false {
		t.Fatalf("disabled Responses projection = %+v", response)
	}
}

func TestResponsesTextStream(t *testing.T) {
	t.Parallel()
	times := []time.Time{time.Unix(100, 0), time.Unix(101, 0)}
	next := 0
	codec := Codec{now: func() time.Time {
		value := times[next]
		if next < len(times)-1 {
			next++
		}
		return value
	}}
	stream := codec.NewStream("resp-1", "human-expert")

	start, err := stream.Start()
	if err != nil {
		t.Fatal(err)
	}
	if len(start) != 2 {
		t.Fatalf("start frames = %d, want 2", len(start))
	}
	created := assertEvent(t, start[0], "response.created", 0)
	response := created["response"].(map[string]any)
	if response["id"] != "resp-1" || response["status"] != "in_progress" || response["created_at"] != float64(100) {
		t.Fatalf("created response = %#v", response)
	}
	assertEvent(t, start[1], "response.in_progress", 1)
	if got := string(stream.Heartbeat()); got != ": ping\n\n" {
		t.Fatalf("heartbeat = %q", got)
	}

	progress, done, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "working"})
	if err != nil || done || len(progress) != 3 {
		t.Fatalf("progress = %q, %v, %v", progress, done, err)
	}
	added := assertEvent(t, progress[0], "response.output_item.added", 2)
	item := added["item"].(map[string]any)
	if item["id"] != "msg_resp-1" || item["status"] != "in_progress" || len(item["content"].([]any)) != 0 {
		t.Fatalf("added output item = %#v", item)
	}
	partAdded := assertEvent(t, progress[1], "response.content_part.added", 3)
	part := partAdded["part"].(map[string]any)
	if part["type"] != "output_text" || part["text"] != "" {
		t.Fatalf("added content part = %#v", partAdded)
	}
	delta := assertEvent(t, progress[2], "response.output_text.delta", 4)
	if delta["delta"] != "working" || delta["item_id"] != "msg_resp-1" {
		t.Fatalf("delta = %#v", delta)
	}

	final, done, err := stream.Encode(completion.Event{Type: completion.EventFinal, Text: " done"})
	if err != nil || !done || len(final) != 5 {
		t.Fatalf("final = %q, %v, %v", final, done, err)
	}
	assertEvent(t, final[0], "response.output_text.delta", 5)
	textDone := assertEvent(t, final[1], "response.output_text.done", 6)
	if textDone["text"] != "working done" {
		t.Fatalf("text done = %#v", textDone)
	}
	assertEvent(t, final[2], "response.content_part.done", 7)
	itemDone := assertEvent(t, final[3], "response.output_item.done", 8)
	if itemDone["item"].(map[string]any)["status"] != "completed" {
		t.Fatalf("item done = %#v", itemDone)
	}
	completed := assertEvent(t, final[4], "response.completed", 9)
	response = completed["response"].(map[string]any)
	if response["status"] != "completed" || response["completed_at"] != float64(101) {
		t.Fatalf("completed response = %#v", response)
	}
	output := response["output"].([]any)
	message := output[0].(map[string]any)
	content := message["content"].([]any)[0].(map[string]any)
	if content["text"] != "working done" {
		t.Fatalf("completed output = %#v", output)
	}
}

func TestResponsesToolStream(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("resp-1", "human-expert")
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "checking"}); err != nil {
		t.Fatal(err)
	}
	frames, done, err := stream.Encode(completion.Event{
		Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{
			{ID: "call-1", Name: "read_file", Input: map[string]any{"path": "/workspace/a"}},
			{ID: "call-2", Name: "search", Input: map[string]any{"query": "needle"}},
		},
	})
	if err != nil || !done || len(frames) != 12 {
		t.Fatalf("tool frames = %q, %v, %v", frames, done, err)
	}
	assertEvent(t, frames[0], "response.output_text.done", 5)
	assertEvent(t, frames[1], "response.content_part.done", 6)
	assertEvent(t, frames[2], "response.output_item.done", 7)
	firstAdded := assertEvent(t, frames[3], "response.output_item.added", 8)
	if firstAdded["item"].(map[string]any)["status"] != "in_progress" {
		t.Fatalf("first added tool = %#v", firstAdded)
	}
	firstDelta := assertEvent(t, frames[4], "response.function_call_arguments.delta", 9)
	first := assertEvent(t, frames[5], "response.function_call_arguments.done", 10)
	assertEvent(t, frames[6], "response.output_item.done", 11)
	assertEvent(t, frames[7], "response.output_item.added", 12)
	secondDelta := assertEvent(t, frames[8], "response.function_call_arguments.delta", 13)
	second := assertEvent(t, frames[9], "response.function_call_arguments.done", 14)
	assertEvent(t, frames[10], "response.output_item.done", 15)
	if first["item_id"] != "fc_call-1" || first["name"] != "read_file" || first["output_index"] != float64(1) {
		t.Fatalf("first tool event = %#v", first)
	}
	if firstDelta["delta"] != first["arguments"] {
		t.Fatalf("first tool delta/done = %#v / %#v", firstDelta, first)
	}
	if second["output_index"] != float64(2) || !strings.Contains(second["arguments"].(string), `"query":"needle"`) ||
		secondDelta["delta"] != second["arguments"] {
		t.Fatalf("second tool event = %#v", second)
	}
	completed := assertEvent(t, frames[11], "response.completed", 16)
	response := completed["response"].(map[string]any)
	output := response["output"].([]any)
	if len(output) != 3 {
		t.Fatalf("completed output = %#v", output)
	}
	call := output[1].(map[string]any)
	if call["type"] != "function_call" || call["call_id"] != "call-1" || call["id"] != "fc_call-1" {
		t.Fatalf("completed function call = %#v", call)
	}
}

func TestResponsesErrorAndAdmissionError(t *testing.T) {
	t.Parallel()
	codec := New()
	admission := string(codec.AdmissionError(503, "capacity", "overloaded"))
	if codec.OverloadedStatus() != 503 || !strings.Contains(admission, `"type":"server_error"`) {
		t.Fatalf("admission = %q, status = %d", admission, codec.OverloadedStatus())
	}

	stream := codec.NewStream("resp-1", "human-expert")
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	frames, done, err := stream.Encode(completion.Event{
		Type: completion.EventRejected, ErrorCode: "human_rejected", Error: "human rejected task",
	})
	if err != nil || !done || len(frames) != 1 {
		t.Fatalf("error frames = %q, %v, %v", frames, done, err)
	}
	payload := assertEvent(t, frames[0], "response.failed", 2)
	response := payload["response"].(map[string]any)
	responseError := response["error"].(map[string]any)
	if response["status"] != "failed" || response["completed_at"] != nil ||
		responseError["code"] != "server_error" || responseError["message"] != "human rejected task" {
		t.Fatalf("failed response = %#v", response)
	}
}

func TestResponsesStreamStateGuards(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("resp-1", "m")
	if _, _, err := stream.Encode(completion.Event{Type: completion.EventFinal, Text: "done"}); err == nil {
		t.Fatal("encode before start succeeded")
	}
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Start(); err == nil {
		t.Fatal("second start succeeded")
	}
	if _, _, err := stream.Encode(completion.Event{Type: completion.EventFinal, Text: "done"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "late"}); err == nil {
		t.Fatal("encode after completion succeeded")
	}
}

func assertEvent(t *testing.T, frame []byte, eventName string, sequence int) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(string(frame)), "\n")
	if len(lines) != 2 || lines[0] != "event: "+eventName || !strings.HasPrefix(lines[1], "data: ") {
		t.Fatalf("invalid SSE frame = %q", frame)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &payload); err != nil {
		t.Fatalf("invalid SSE JSON: %v", err)
	}
	if payload["type"] != eventName || payload["sequence_number"] != float64(sequence) {
		t.Fatalf("event = %#v; want type %q sequence %d", payload, eventName, sequence)
	}
	return payload
}
