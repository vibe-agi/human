package openai

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

func TestAggregateTextToolErrorAndExactRebuild(t *testing.T) {
	t.Parallel()
	codec := Codec{now: func() time.Time { return time.Unix(999, 0) }}
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
				choice := body["choices"].([]any)[0].(map[string]any)
				message := choice["message"].(map[string]any)
				if message["content"] != "hello world" || choice["finish_reason"] != "stop" {
					t.Fatalf("text body = %#v", body)
				}
			},
		},
		{
			name:   "empty final",
			events: []completion.Event{{Type: completion.EventFinal}},
			assertBody: func(t *testing.T, body map[string]any) {
				message := body["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)
				if message["content"] != "" {
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
				choice := body["choices"].([]any)[0].(map[string]any)
				message := choice["message"].(map[string]any)
				calls := message["tool_calls"].([]any)
				function := calls[0].(map[string]any)["function"].(map[string]any)
				if choice["finish_reason"] != "tool_calls" || function["arguments"] != `{"path":"README.md"}` {
					t.Fatalf("tool body = %#v", body)
				}
			},
		},
		{
			name:   "error",
			events: []completion.Event{{Type: completion.EventExpired, ErrorCode: "human_timeout", Error: "too slow"}},
			assertBody: func(t *testing.T, body map[string]any) {
				if body["error"].(map[string]any)["message"] != "too slow" {
					t.Fatalf("error body = %#v", body)
				}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first := encodeOpenAIAggregate(t, codec, test.events)
			second := encodeOpenAIAggregate(t, codec, test.events)
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

func encodeOpenAIAggregate(t *testing.T, codec Codec, events []completion.Event) []byte {
	t.Helper()
	aggregate := codec.NewAggregate("chatcmpl_fixed", "human-expert", dialect.StreamSeed{CreatedAtUnix: 123})
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
    {"role":"assistant","content":""},
    {"role":"user","content":"continue"}
  ]
}`))
	if err != nil || len(request.Messages) != 2 {
		t.Fatalf("empty assistant round trip = %+v, %v", request.Messages, err)
	}
}
