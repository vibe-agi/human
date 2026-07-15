package anthropic

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

func TestDecodeMessagesRequest(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
  "model":"human-expert",
  "max_tokens":4096,
  "stream":true,
  "system":[{"type":"text","text":"be precise"},{"type":"text","text":"show evidence"}],
  "metadata":{"user_id":"caller-1"},
  "messages":[
    {"role":"user","content":[
      {"type":"text","text":"inspect"},
      {"type":"image","source":{"type":"base64","media_type":"image/png","data":"AA=="}}
    ]},
    {"role":"assistant","content":[
      {"type":"tool_use","id":"toolu-1","name":"read_file","input":{"path":"/workspace/a"}}
    ]},
    {"role":"user","content":[
      {"type":"tool_result","tool_use_id":"toolu-1","content":"contents","is_error":false}
    ]}
  ],
  "tools":[{"name":"read_file","description":"read","input_schema":{"type":"object"}}],
  "temperature":0.1
}`)
	request, err := New().Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if request.Dialect != canonical.DialectAnthropic || request.System != "be precise\nshow evidence" {
		t.Fatalf("request = %+v", request)
	}
	if request.Metadata["user_id"] != "caller-1" || len(request.Messages) != 3 {
		t.Fatalf("metadata/messages = %+v / %d", request.Metadata, len(request.Messages))
	}
	if got := request.Messages[0].Blocks[1]; got.Type != canonical.BlockImage || got.ImageURL != "data:image/png;base64,AA==" {
		t.Fatalf("image = %+v", got)
	}
	if got := request.Messages[1].Blocks[0]; got.Type != canonical.BlockToolUse || got.ToolCallID != "toolu-1" || got.ToolName != "read_file" {
		t.Fatalf("tool use = %+v", got)
	}
	if got := request.Messages[2].Blocks[0]; got.Type != canonical.BlockToolResult || got.ToolCallID != "toolu-1" || got.Output != "contents" {
		t.Fatalf("tool result = %+v", got)
	}
	if len(request.Tools) != 1 || string(request.Tools[0].InputSchema) != `{"type":"object"}` {
		t.Fatalf("tools = %+v", request.Tools)
	}
}

func TestDecodeURLImageAndNestedToolResult(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
  "model":"human-expert","stream":true,
  "messages":[
    {"role":"user","content":[{"type":"image","source":{"type":"url","url":"https://example.com/a.png"}}]},
    {"role":"assistant","content":[{"type":"tool_use","id":"toolu-1","name":"inspect","input":{}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu-1","is_error":true,"content":[
      {"type":"text","text":"failed"},
      {"type":"image","source":{"type":"url","url":"https://example.com/error.png"}}
    ]}]}
  ]
}`)
	request, err := New().Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got := request.Messages[0].Blocks[0].ImageURL; got != "https://example.com/a.png" {
		t.Fatalf("image URL = %q", got)
	}
	result := request.Messages[2].Blocks[0]
	blocks, ok := result.Output.([]canonical.Block)
	if !ok || !result.IsError || len(blocks) != 2 || blocks[1].Type != canonical.BlockImage {
		t.Fatalf("nested result = %#v", result)
	}
}

func TestDecodeRejectsNonStreamingAndInvalidRoles(t *testing.T) {
	t.Parallel()
	codec := New()
	if _, err := codec.Decode([]byte(`{"model":"m","stream":false,"messages":[{"role":"user","content":"hello"}]}`)); err != dialect.ErrUnsupportedNonStreaming {
		t.Fatalf("non-streaming error = %v", err)
	}
	if _, err := codec.Decode([]byte(`{"model":"m","stream":true,"messages":[{"role":"system","content":"hello"}]}`)); err == nil {
		t.Fatal("system message role accepted")
	}
	if _, err := codec.Decode([]byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":[{"type":"tool_use","id":"x","name":"read","input":{}}]}]}`)); err == nil {
		t.Fatal("user tool_use accepted")
	}
}

func TestAnthropicTextStream(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("msg-1", "human-expert")
	start, err := stream.Start()
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, start[0], "message_start", "message_start")
	if !strings.Contains(string(start[0]), `"id":"msg-1"`) || !strings.Contains(string(start[0]), `"content":[]`) {
		t.Fatalf("start = %q", start)
	}

	progress, done, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "working"})
	if err != nil || done || len(progress) != 2 {
		t.Fatalf("progress = %q, %v, %v", progress, done, err)
	}
	assertEvent(t, progress[0], "content_block_start", "content_block_start")
	assertEvent(t, progress[1], "content_block_delta", "content_block_delta")
	if !strings.Contains(string(progress[1]), `"type":"text_delta"`) || !strings.Contains(string(progress[1]), `"text":"working"`) {
		t.Fatalf("progress delta = %q", progress[1])
	}

	final, done, err := stream.Encode(completion.Event{Type: completion.EventFinal, Text: " done"})
	if err != nil || !done || len(final) != 4 {
		t.Fatalf("final = %q, %v, %v", final, done, err)
	}
	assertEvent(t, final[0], "content_block_delta", "content_block_delta")
	assertEvent(t, final[1], "content_block_stop", "content_block_stop")
	assertEvent(t, final[2], "message_delta", "message_delta")
	assertEvent(t, final[3], "message_stop", "message_stop")
	if !strings.Contains(string(final[2]), `"stop_reason":"end_turn"`) {
		t.Fatalf("message delta = %q", final[2])
	}
}

func TestAnthropicToolUseStream(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("msg-1", "human-expert")
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	frames, done, err := stream.Encode(completion.Event{
		Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{
			{ID: "toolu-1", Name: "read_file", Input: map[string]any{"path": "/workspace/a"}},
			{ID: "toolu-2", Name: "search", Input: map[string]any{"query": "needle"}},
		},
	})
	if err != nil || !done || len(frames) != 8 {
		t.Fatalf("tool frames = %q, %v, %v", frames, done, err)
	}
	assertEvent(t, frames[0], "content_block_start", "content_block_start")
	assertEvent(t, frames[1], "content_block_delta", "content_block_delta")
	assertEvent(t, frames[2], "content_block_stop", "content_block_stop")
	if !strings.Contains(string(frames[0]), `"type":"tool_use"`) || !strings.Contains(string(frames[1]), `"type":"input_json_delta"`) {
		t.Fatalf("first tool = %q / %q", frames[0], frames[1])
	}
	if !strings.Contains(string(frames[6]), `"stop_reason":"tool_use"`) {
		t.Fatalf("message delta = %q", frames[6])
	}
	for _, index := range []struct {
		frame int
		want  string
	}{{0, `"index":0`}, {3, `"index":1`}} {
		if !strings.Contains(string(frames[index.frame]), index.want) {
			t.Fatalf("frame %d = %q", index.frame, frames[index.frame])
		}
	}
}

func TestAnthropicErrorHeartbeatAndAdmissionError(t *testing.T) {
	t.Parallel()
	codec := New()
	if got := string(codec.NewStream("msg-1", "m").Heartbeat()); got != "event: ping\ndata: {\"type\":\"ping\"}\n\n" {
		t.Fatalf("heartbeat = %q", got)
	}
	admission := string(codec.AdmissionError(529, "capacity", "overloaded"))
	if codec.OverloadedStatus() != 529 || !strings.Contains(admission, `"type":"overloaded_error"`) {
		t.Fatalf("admission error = %q, status = %d", admission, codec.OverloadedStatus())
	}

	stream := codec.NewStream("msg-1", "m")
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	frames, done, err := stream.Encode(completion.Event{
		Type: completion.EventRejected, ErrorCode: "human_rejected", Error: "human rejected task",
	})
	if err != nil || !done || len(frames) != 1 {
		t.Fatalf("error = %q, %v, %v", frames, done, err)
	}
	assertEvent(t, frames[0], "error", "error")
	if !strings.Contains(string(frames[0]), `"type":"api_error"`) || !strings.Contains(string(frames[0]), `"message":"human rejected task"`) {
		t.Fatalf("error frame = %q", frames[0])
	}
}

func TestAnthropicStreamStateGuards(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("msg-1", "m")
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
		t.Fatal("encode after stop succeeded")
	}
}

func assertEvent(t *testing.T, frame []byte, eventName, payloadType string) {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(string(frame)), "\n")
	if len(lines) != 2 || lines[0] != "event: "+eventName || !strings.HasPrefix(lines[1], "data: ") {
		t.Fatalf("invalid SSE frame = %q", frame)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &payload); err != nil {
		t.Fatalf("invalid SSE JSON: %v", err)
	}
	if payload["type"] != payloadType {
		t.Fatalf("payload type = %#v; frame = %q", payload["type"], frame)
	}
}
