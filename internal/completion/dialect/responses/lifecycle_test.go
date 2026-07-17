package responses

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

func TestResponsesTextLifecycleFeedsAccumulator(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("resp-accumulate", "human-expert")
	start, err := stream.Start()
	if err != nil {
		t.Fatal(err)
	}
	progress, done, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "work"})
	if err != nil || done {
		t.Fatalf("progress = %q, %v, %v", progress, done, err)
	}
	final, done, err := stream.Encode(completion.Event{Type: completion.EventFinal, Text: " done"})
	if err != nil || !done {
		t.Fatalf("final = %q, %v, %v", final, done, err)
	}

	frames := append(append(start, progress...), final...)
	wantTypes := []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.content_part.added",
		"response.output_text.delta",
		"response.output_text.delta",
		"response.output_text.done",
		"response.content_part.done",
		"response.output_item.done",
		"response.completed",
	}
	if len(frames) != len(wantTypes) {
		t.Fatalf("frames = %d, want %d: %q", len(frames), len(wantTypes), frames)
	}

	var accumulated strings.Builder
	itemAdded, partAdded, textDone, partDone, itemDone := false, false, false, false, false
	for index, frame := range frames {
		payload := decodeLifecycleFrame(t, frame)
		if payload["type"] != wantTypes[index] || payload["sequence_number"] != float64(index) {
			t.Fatalf("event %d = %#v, want %q", index, payload, wantTypes[index])
		}
		switch wantTypes[index] {
		case "response.output_item.added":
			if itemAdded {
				t.Fatal("text item added twice")
			}
			item := lifecycleMap(t, payload["item"])
			if item["id"] != "msg_resp-accumulate" || item["status"] != "in_progress" {
				t.Fatalf("added item = %#v", item)
			}
			itemAdded = true
		case "response.content_part.added":
			if !itemAdded || partAdded {
				t.Fatalf("content part added out of order: item=%v part=%v", itemAdded, partAdded)
			}
			partAdded = true
		case "response.output_text.delta":
			if !partAdded || textDone {
				t.Fatalf("text delta outside open part: part=%v done=%v", partAdded, textDone)
			}
			accumulated.WriteString(payload["delta"].(string))
		case "response.output_text.done":
			if payload["text"] != accumulated.String() {
				t.Fatalf("text done = %q, accumulated %q", payload["text"], accumulated.String())
			}
			textDone = true
		case "response.content_part.done":
			part := lifecycleMap(t, payload["part"])
			if !textDone || part["text"] != accumulated.String() {
				t.Fatalf("part done = %#v after textDone=%v", part, textDone)
			}
			partDone = true
		case "response.output_item.done":
			item := lifecycleMap(t, payload["item"])
			content := lifecycleSlice(t, item["content"])
			if !partDone || lifecycleMap(t, content[0])["text"] != accumulated.String() {
				t.Fatalf("item done = %#v after partDone=%v", item, partDone)
			}
			itemDone = true
		case "response.completed":
			response := lifecycleMap(t, payload["response"])
			output := lifecycleSlice(t, response["output"])
			content := lifecycleSlice(t, lifecycleMap(t, output[0])["content"])
			if !itemDone || lifecycleMap(t, content[0])["text"] != accumulated.String() {
				t.Fatalf("completed response = %#v after itemDone=%v", response, itemDone)
			}
		}
	}
	if accumulated.String() != "work done" {
		t.Fatalf("accumulated text = %q", accumulated.String())
	}
}

func TestResponsesToolLifecycleFeedsAccumulatorAndNormalizesNilInput(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("resp-tool-accumulate", "human-expert")
	start, err := stream.Start()
	if err != nil {
		t.Fatal(err)
	}
	final, done, err := stream.Encode(completion.Event{
		Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{
			ID: "call-empty", Name: "no_arguments", Input: nil,
		}},
	})
	if err != nil || !done {
		t.Fatalf("tool final = %q, %v, %v", final, done, err)
	}
	frames := append(start, final...)
	wantTypes := []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.function_call_arguments.delta",
		"response.function_call_arguments.done",
		"response.output_item.done",
		"response.completed",
	}
	if len(frames) != len(wantTypes) {
		t.Fatalf("frames = %d, want %d: %q", len(frames), len(wantTypes), frames)
	}

	var arguments strings.Builder
	itemAdded, argumentsDone, itemDone := false, false, false
	for index, frame := range frames {
		payload := decodeLifecycleFrame(t, frame)
		if payload["type"] != wantTypes[index] || payload["sequence_number"] != float64(index) {
			t.Fatalf("event %d = %#v, want %q", index, payload, wantTypes[index])
		}
		switch wantTypes[index] {
		case "response.output_item.added":
			item := lifecycleMap(t, payload["item"])
			if item["arguments"] != "" || item["status"] != "in_progress" {
				t.Fatalf("added function item = %#v", item)
			}
			itemAdded = true
		case "response.function_call_arguments.delta":
			if !itemAdded || argumentsDone {
				t.Fatalf("arguments delta out of order")
			}
			arguments.WriteString(payload["delta"].(string))
		case "response.function_call_arguments.done":
			if payload["arguments"] != arguments.String() {
				t.Fatalf("arguments done = %q, accumulated %q", payload["arguments"], arguments.String())
			}
			argumentsDone = true
		case "response.output_item.done":
			item := lifecycleMap(t, payload["item"])
			if !argumentsDone || item["arguments"] != arguments.String() || item["status"] != "completed" {
				t.Fatalf("function item done = %#v", item)
			}
			itemDone = true
		case "response.completed":
			response := lifecycleMap(t, payload["response"])
			output := lifecycleSlice(t, response["output"])
			if !itemDone || lifecycleMap(t, output[0])["arguments"] != arguments.String() {
				t.Fatalf("completed response = %#v", response)
			}
		}
	}
	if arguments.String() != "{}" {
		t.Fatalf("nil tool input arguments = %q, want {}", arguments.String())
	}
}

func TestResponsesLifecycleReplayIsByteStable(t *testing.T) {
	t.Parallel()
	encode := func() []byte {
		t.Helper()
		stream := New().NewStream(
			"resp-stable", "human-expert", dialect.StreamSeed{CreatedAtUnix: 123},
		)
		frames, err := stream.Start()
		if err != nil {
			t.Fatal(err)
		}
		progress, done, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "checking "})
		if err != nil || done {
			t.Fatalf("progress = %q, %v, %v", progress, done, err)
		}
		frames = append(frames, progress...)
		terminal, done, err := stream.Encode(completion.Event{
			Type: completion.EventToolCalls,
			ToolCalls: []completion.ToolCall{
				{ID: "call-a", Name: "read_file", Input: map[string]any{"path": "/workspace/a"}},
				{ID: "call-b", Name: "no_arguments", Input: nil},
			},
		}, dialect.EventSeed{EncodedAtUnix: 456})
		if err != nil || !done {
			t.Fatalf("terminal = %q, %v, %v", terminal, done, err)
		}
		return bytes.Join(append(frames, terminal...), nil)
	}

	first := encode()
	second := encode()
	if !bytes.Equal(first, second) {
		t.Fatalf("replayed lifecycle differs\nfirst:  %s\nsecond: %s", first, second)
	}
	for sequence := 0; sequence <= 16; sequence++ {
		needle := []byte(`"sequence_number":` + strconv.Itoa(sequence) + `,`)
		if count := bytes.Count(first, needle); count != 1 {
			t.Fatalf("sequence %d count = %d in %s", sequence, count, first)
		}
	}
}

func TestResponsesEmptyFinalLifecycleGoldenAndReplayStable(t *testing.T) {
	t.Parallel()
	encode := func() ([][]byte, []byte) {
		t.Helper()
		stream := Codec{now: func() time.Time { return time.Unix(999, 0) }}.NewStream(
			"resp-empty", "human-expert", dialect.StreamSeed{CreatedAtUnix: 123},
		)
		frames, err := stream.Start()
		if err != nil {
			t.Fatal(err)
		}
		terminal, done, err := stream.Encode(
			completion.Event{Type: completion.EventFinal},
			dialect.EventSeed{EncodedAtUnix: 456},
		)
		if err != nil || !done {
			t.Fatalf("empty final = %q, %v, %v", terminal, done, err)
		}
		frames = append(frames, terminal...)
		return frames, bytes.Join(frames, nil)
	}

	frames, first := encode()
	_, second := encode()
	if !bytes.Equal(first, second) {
		t.Fatalf("empty-final replay differs\nfirst:  %s\nsecond: %s", first, second)
	}
	assertResponsesStreamGolden(t, first, "testdata/empty_final_stream.golden.sse")
	wantTypes := []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.content_part.added",
		"response.output_text.done",
		"response.content_part.done",
		"response.output_item.done",
		"response.completed",
	}
	if len(frames) != len(wantTypes) {
		t.Fatalf("empty-final frame count = %d, want %d", len(frames), len(wantTypes))
	}
	for index, frame := range frames {
		payload := decodeLifecycleFrame(t, frame)
		if payload["type"] != wantTypes[index] || payload["sequence_number"] != float64(index) {
			t.Fatalf("empty-final event %d = %#v, want %q", index, payload, wantTypes[index])
		}
	}
	completed := lifecycleMap(t, decodeLifecycleFrame(t, frames[len(frames)-1])["response"])
	output := lifecycleSlice(t, completed["output"])
	message := lifecycleMap(t, output[0])
	content := lifecycleSlice(t, message["content"])
	if message["role"] != "assistant" || message["status"] != "completed" ||
		lifecycleMap(t, content[0])["text"] != "" || completed["completed_at"] != float64(456) {
		t.Fatalf("empty-final completed response = %#v", completed)
	}
}

func TestResponsesFailedLifecycleGoldenAndReplayStable(t *testing.T) {
	t.Parallel()
	encode := func() ([][]byte, []byte) {
		t.Helper()
		stream := Codec{now: func() time.Time { return time.Unix(999, 0) }}.NewStream(
			"resp-failed", "human-expert", dialect.StreamSeed{CreatedAtUnix: 123},
		)
		frames, err := stream.Start()
		if err != nil {
			t.Fatal(err)
		}
		progress, done, err := stream.Encode(completion.Event{Type: completion.EventProgress, Text: "partial"})
		if err != nil || done {
			t.Fatalf("failed progress = %q, %v, %v", progress, done, err)
		}
		frames = append(frames, progress...)
		terminal, done, err := stream.Encode(completion.Event{
			Type: completion.EventExpired, ErrorCode: "human_timeout", Error: "expert timed out",
		}, dialect.EventSeed{EncodedAtUnix: 456})
		if err != nil || !done {
			t.Fatalf("failed terminal = %q, %v, %v", terminal, done, err)
		}
		frames = append(frames, terminal...)
		return frames, bytes.Join(frames, nil)
	}

	frames, first := encode()
	_, second := encode()
	if !bytes.Equal(first, second) {
		t.Fatalf("failed replay differs\nfirst:  %s\nsecond: %s", first, second)
	}
	assertResponsesStreamGolden(t, first, "testdata/failed_stream.golden.sse")
	wantTypes := []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.content_part.added",
		"response.output_text.delta",
		"response.failed",
	}
	if len(frames) != len(wantTypes) {
		t.Fatalf("failed frame count = %d, want %d", len(frames), len(wantTypes))
	}
	for index, frame := range frames {
		payload := decodeLifecycleFrame(t, frame)
		if payload["type"] != wantTypes[index] || payload["sequence_number"] != float64(index) {
			t.Fatalf("failed event %d = %#v, want %q", index, payload, wantTypes[index])
		}
	}
	failed := lifecycleMap(t, decodeLifecycleFrame(t, frames[len(frames)-1])["response"])
	responseError := lifecycleMap(t, failed["error"])
	output := lifecycleSlice(t, failed["output"])
	message := lifecycleMap(t, output[0])
	content := lifecycleSlice(t, message["content"])
	if failed["status"] != "failed" || failed["completed_at"] != nil ||
		responseError["code"] != "server_error" || responseError["message"] != "expert timed out" ||
		message["status"] != "incomplete" || lifecycleMap(t, content[0])["text"] != "partial" {
		t.Fatalf("failed response = %#v", failed)
	}
	if bytes.Contains(first, []byte("event: error")) || bytes.Contains(first, []byte("response.completed")) {
		t.Fatalf("failed transcript used a non-failure terminal: %s", first)
	}
}

func TestResponsesFailedLifecycleDefaultsError(t *testing.T) {
	t.Parallel()
	stream := New().NewStream("resp-failed-default", "human-expert")
	if _, err := stream.Start(); err != nil {
		t.Fatal(err)
	}
	frames, done, err := stream.Encode(completion.Event{Type: completion.EventFailed})
	if err != nil || !done || len(frames) != 1 {
		t.Fatalf("default failure = %q, %v, %v", frames, done, err)
	}
	failed := lifecycleMap(t, decodeLifecycleFrame(t, frames[0])["response"])
	responseError := lifecycleMap(t, failed["error"])
	if responseError["code"] != "server_error" || responseError["message"] != "human agent request failed" {
		t.Fatalf("default response error = %#v", responseError)
	}
	if output, ok := failed["output"].([]any); !ok || len(output) != 0 {
		t.Fatalf("default failure output = %#v", failed["output"])
	}
}

func assertResponsesStreamGolden(t *testing.T, actual []byte, path string) {
	t.Helper()
	want := readFixture(t, path)
	// Keep the fixture itself at one trailing newline so repository whitespace
	// checks stay clean; the second newline is the SSE frame terminator.
	want = append(want, '\n')
	if !bytes.Equal(actual, want) {
		t.Fatalf("Responses stream golden mismatch for %s\nactual:\n%s\nwant:\n%s", path, actual, want)
	}
}

func decodeLifecycleFrame(t *testing.T, frame []byte) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(string(frame)), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "event: ") || !strings.HasPrefix(lines[1], "data: ") {
		t.Fatalf("invalid SSE frame = %q", frame)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &payload); err != nil {
		t.Fatalf("invalid SSE JSON: %v", err)
	}
	if lines[0] != "event: "+payload["type"].(string) {
		t.Fatalf("SSE event/payload mismatch = %q / %#v", lines[0], payload)
	}
	return payload
}

func lifecycleMap(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value = %#v, want object", value)
	}
	return result
}

func lifecycleSlice(t *testing.T, value any) []any {
	t.Helper()
	result, ok := value.([]any)
	if !ok || len(result) == 0 {
		t.Fatalf("value = %#v, want non-empty array", value)
	}
	return result
}
