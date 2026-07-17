package anthropic

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

func TestAggregateTextToolErrorAndExactRebuild(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		events     []completion.Event
		assertBody func(*testing.T, map[string]any)
	}{
		{
			name: "text",
			events: []completion.Event{
				{Type: completion.EventProgress, Text: "hello "},
				{Type: completion.EventFinal, Text: "world"},
			},
			assertBody: func(t *testing.T, body map[string]any) {
				content := body["content"].([]any)
				if content[0].(map[string]any)["text"] != "hello world" || body["stop_reason"] != "end_turn" {
					t.Fatalf("text body = %#v", body)
				}
			},
		},
		{
			name:   "empty final",
			events: []completion.Event{{Type: completion.EventFinal}},
			assertBody: func(t *testing.T, body map[string]any) {
				block := body["content"].([]any)[0].(map[string]any)
				if block["type"] != "text" || block["text"] != "" {
					t.Fatalf("empty final body = %#v", body)
				}
			},
		},
		{
			name: "tool",
			events: []completion.Event{{Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{{
				ID: "call_1", Name: "read_file", Input: map[string]any{"path": "README.md"},
			}}}},
			assertBody: func(t *testing.T, body map[string]any) {
				block := body["content"].([]any)[0].(map[string]any)
				if body["stop_reason"] != "tool_use" || block["type"] != "tool_use" || block["id"] != "call_1" {
					t.Fatalf("tool body = %#v", body)
				}
			},
		},
		{
			name:   "error",
			events: []completion.Event{{Type: completion.EventUnavailable, ErrorCode: "worker_unavailable", Error: "gone"}},
			assertBody: func(t *testing.T, body map[string]any) {
				errorBody := body["error"].(map[string]any)
				if body["type"] != "error" || errorBody["type"] != "overloaded_error" {
					t.Fatalf("error body = %#v", body)
				}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first := encodeAnthropicAggregate(t, test.events)
			second := encodeAnthropicAggregate(t, test.events)
			if !bytes.Equal(first, second) {
				t.Fatalf("aggregate rebuild changed bytes:\n%s\n%s", first, second)
			}
			var body map[string]any
			if err := json.Unmarshal(first, &body); err != nil {
				t.Fatalf("invalid aggregate JSON %q: %v", first, err)
			}
			test.assertBody(t, body)
		})
	}
}

func encodeAnthropicAggregate(t *testing.T, events []completion.Event) []byte {
	t.Helper()
	aggregate := New().NewAggregate("msg_fixed", "human-expert", dialect.StreamSeed{CreatedAtUnix: 123})
	start, err := aggregate.Start()
	if err != nil || len(start) != 0 || len(aggregate.Heartbeat()) != 0 {
		t.Fatalf("aggregate start = %q, heartbeat=%q, err=%v", start, aggregate.Heartbeat(), err)
	}
	var body []byte
	for index, event := range events {
		frames, done, err := aggregate.Encode(event, dialect.EventSeed{EncodedAtUnix: int64(200 + index)})
		if err != nil {
			t.Fatalf("event %d: %v", index, err)
		}
		if index != len(events)-1 && (done || len(frames) != 0) {
			t.Fatalf("event %d leaked aggregate bytes: done=%v frames=%q", index, done, frames)
		}
		if index == len(events)-1 {
			if !done || len(frames) != 1 {
				t.Fatalf("terminal = done=%v frames=%q", done, frames)
			}
			body = frames[0]
		}
	}
	return body
}

func TestEmptyAggregateAssistantRoundTripsInNextRequest(t *testing.T) {
	t.Parallel()
	request, err := New().Decode([]byte(`{
  "model":"human-expert","stream":false,
  "messages":[
    {"role":"user","content":"hello"},
    {"role":"assistant","content":[{"type":"text","text":""}]},
    {"role":"user","content":"continue"}
  ]
}`))
	if err != nil || len(request.Messages) != 2 {
		t.Fatalf("empty assistant round trip = %+v, %v", request.Messages, err)
	}
}
