package responses

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
				message := body["output"].([]any)[0].(map[string]any)
				part := message["content"].([]any)[0].(map[string]any)
				if body["status"] != "completed" || part["text"] != "hello world" || body["completed_at"] != float64(201) {
					t.Fatalf("text body = %#v", body)
				}
			},
		},
		{
			name:   "empty final",
			events: []completion.Event{{Type: completion.EventFinal}},
			assertBody: func(t *testing.T, body map[string]any) {
				message := body["output"].([]any)[0].(map[string]any)
				part := message["content"].([]any)[0].(map[string]any)
				if part["type"] != "output_text" || part["text"] != "" {
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
				call := body["output"].([]any)[0].(map[string]any)
				if call["type"] != "function_call" || call["call_id"] != "call_1" || call["arguments"] != `{"path":"README.md"}` {
					t.Fatalf("tool body = %#v", body)
				}
			},
		},
		{
			name:   "error",
			events: []completion.Event{{Type: completion.EventFailed, ErrorCode: "worker_failed", Error: "gone"}},
			assertBody: func(t *testing.T, body map[string]any) {
				if body["error"].(map[string]any)["message"] != "gone" {
					t.Fatalf("error body = %#v", body)
				}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first := encodeResponsesAggregate(t, codec, test.events)
			second := encodeResponsesAggregate(t, codec, test.events)
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

func encodeResponsesAggregate(t *testing.T, codec Codec, events []completion.Event) []byte {
	t.Helper()
	aggregate := codec.NewAggregate("resp_fixed", "human-expert", dialect.StreamSeed{CreatedAtUnix: 123})
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
  "input":[
    {"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]},
    {"type":"message","role":"assistant","content":[{"type":"output_text","text":""}]},
    {"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}
  ]
}`))
	if err != nil || len(request.Messages) != 2 {
		t.Fatalf("empty assistant round trip = %+v, %v", request.Messages, err)
	}
}
