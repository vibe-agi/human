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

func TestDecodeAllowsForwardCompatibleFieldsAndRejectsNonStreaming(t *testing.T) {
	t.Parallel()
	codec := New()
	if _, err := codec.Decode([]byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hello"}],"temperature":0.1}`)); err != nil {
		t.Fatalf("forward-compatible field rejected: %v", err)
	}
	if _, err := codec.Decode([]byte(`{"model":"m","stream":false,"messages":[]}`)); err == nil {
		t.Fatal("non-streaming request accepted")
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
