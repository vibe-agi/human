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
  }],
  "temperature":0.1
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
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{
			name:    "non streaming",
			payload: `{"model":"m","stream":false,"input":"hello"}`,
			wantErr: dialect.ErrUnsupportedNonStreaming,
		},
		{
			name:    "non string instructions",
			payload: `{"model":"m","stream":true,"instructions":[{"role":"developer","content":"x"}],"input":"hello"}`,
		},
		{
			name:    "built in tool",
			payload: `{"model":"m","stream":true,"input":"hello","tools":[{"type":"web_search"}]}`,
		},
		{
			name:    "missing function output call id",
			payload: `{"model":"m","stream":true,"input":[{"type":"function_call_output","output":"x"}]}`,
		},
		{
			name:    "unsupported input item",
			payload: `{"model":"m","stream":true,"input":[{"type":"reasoning","summary":[]}]}`,
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
	created := assertEvent(t, start[0], "response.created", 0)
	response := created["response"].(map[string]any)
	if response["id"] != "resp-1" || response["status"] != "in_progress" || response["created_at"] != float64(100) {
		t.Fatalf("created response = %#v", response)
	}
	if got := string(stream.Heartbeat()); got != ": ping\n\n" {
		t.Fatalf("heartbeat = %q", got)
	}

	progress, done, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "working"})
	if err != nil || done || len(progress) != 1 {
		t.Fatalf("progress = %q, %v, %v", progress, done, err)
	}
	delta := assertEvent(t, progress[0], "response.output_text.delta", 1)
	if delta["delta"] != "working" || delta["item_id"] != "msg_resp-1" {
		t.Fatalf("delta = %#v", delta)
	}

	final, done, err := stream.Encode(completion.Event{Type: completion.EventFinal, Text: " done"})
	if err != nil || !done || len(final) != 2 {
		t.Fatalf("final = %q, %v, %v", final, done, err)
	}
	assertEvent(t, final[0], "response.output_text.delta", 2)
	completed := assertEvent(t, final[1], "response.completed", 3)
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
	if err != nil || !done || len(frames) != 3 {
		t.Fatalf("tool frames = %q, %v, %v", frames, done, err)
	}
	first := assertEvent(t, frames[0], "response.function_call_arguments.done", 2)
	second := assertEvent(t, frames[1], "response.function_call_arguments.done", 3)
	if first["item_id"] != "fc_call-1" || first["name"] != "read_file" || first["output_index"] != float64(1) {
		t.Fatalf("first tool event = %#v", first)
	}
	if second["output_index"] != float64(2) || !strings.Contains(second["arguments"].(string), `"query":"needle"`) {
		t.Fatalf("second tool event = %#v", second)
	}
	completed := assertEvent(t, frames[2], "response.completed", 4)
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
	payload := assertEvent(t, frames[0], "error", 1)
	if payload["code"] != "human_rejected" || payload["message"] != "human rejected task" || payload["param"] != nil {
		t.Fatalf("error = %#v", payload)
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
