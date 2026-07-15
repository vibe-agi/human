package anthropic

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

func TestGoldenAnthropicToolsCanonicalAndStreamContract(t *testing.T) {
	t.Parallel()
	payload := readFixture(t, "testdata/tools_request.json")
	assertRedactedFixture(t, payload)
	codec := New()
	request, err := codec.Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalFixture(t, request, "testdata/tools_canonical.golden.json")
	if request.Metadata["user_id"] != "caller_fixture_01" {
		t.Fatalf("metadata user_id = %q", request.Metadata["user_id"])
	}
	call, result := fixtureToolPair(t, request)
	if call.ToolCallID != result.ToolCallID || call.ToolCallID != "toolu_fixture_read_01" {
		t.Fatalf("tool identity use/result = %q / %q", call.ToolCallID, result.ToolCallID)
	}
	assertStableDigest(t, request)

	stream := codec.NewStream("anthropic_fixture_01", request.Model)
	start, err := stream.Start()
	if err != nil || len(start) != 1 {
		t.Fatalf("start = %q, %v", start, err)
	}
	started := decodeAnthropicFixtureFrame(t, start[0], "message_start")
	message := fixtureMap(t, started["message"])
	if message["id"] != "anthropic_fixture_01" || message["model"] != request.Model {
		t.Fatalf("message identity = %#v", message)
	}

	frames, done, err := stream.Encode(completion.Event{
		Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{
			ID: call.ToolCallID, Name: call.ToolName, Input: call.Input,
		}},
	})
	if err != nil || !done || len(frames) != 5 {
		t.Fatalf("tool stream = %q, %v, %v", frames, done, err)
	}
	blockStart := decodeAnthropicFixtureFrame(t, frames[0], "content_block_start")
	contentBlock := fixtureMap(t, blockStart["content_block"])
	if contentBlock["id"] != call.ToolCallID || contentBlock["name"] != call.ToolName ||
		contentBlock["type"] != "tool_use" {
		t.Fatalf("encoded tool identity = %#v", contentBlock)
	}
	delta := decodeAnthropicFixtureFrame(t, frames[1], "content_block_delta")
	deltaBody := fixtureMap(t, delta["delta"])
	partialJSON, ok := deltaBody["partial_json"].(string)
	if !ok {
		t.Fatalf("partial_json = %#v", deltaBody["partial_json"])
	}
	var arguments map[string]any
	if err := json.Unmarshal([]byte(partialJSON), &arguments); err != nil ||
		!reflect.DeepEqual(arguments, call.Input) {
		t.Fatalf("encoded tool arguments = %#v, %v", arguments, err)
	}
	decodeAnthropicFixtureFrame(t, frames[2], "content_block_stop")
	messageDelta := decodeAnthropicFixtureFrame(t, frames[3], "message_delta")
	if fixtureMap(t, messageDelta["delta"])["stop_reason"] != "tool_use" {
		t.Fatalf("message delta = %#v", messageDelta)
	}
	decodeAnthropicFixtureFrame(t, frames[4], "message_stop")
}

func readFixture(t *testing.T, path string) []byte {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func assertCanonicalFixture(t *testing.T, request canonical.Request, path string) {
	t.Helper()
	actual, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	want := readFixture(t, path)
	var actualJSON, wantJSON any
	if err := json.Unmarshal(actual, &actualJSON); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &wantJSON); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actualJSON, wantJSON) {
		pretty, _ := json.MarshalIndent(actualJSON, "", "  ")
		t.Fatalf("canonical fixture mismatch\nactual: %s\nwant: %s", pretty, want)
	}
}

func fixtureToolPair(t *testing.T, request canonical.Request) (canonical.Block, canonical.Block) {
	t.Helper()
	var call, result canonical.Block
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			switch block.Type {
			case canonical.BlockToolUse:
				call = block
			case canonical.BlockToolResult:
				result = block
			}
		}
	}
	if call.ToolCallID == "" || result.ToolCallID == "" {
		t.Fatalf("fixture lacks tool use/result: %+v", request.Messages)
	}
	return call, result
}

func assertStableDigest(t *testing.T, request canonical.Request) {
	t.Helper()
	first, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second, err := request.Digest()
	if err != nil || first != second {
		t.Fatalf("canonical digest = %q / %q, %v", first, second, err)
	}
}

func assertRedactedFixture(t *testing.T, payload []byte) {
	t.Helper()
	for _, forbidden := range []string{"/Users/", "/home/", "Bearer ", "sk-", "BEGIN PRIVATE KEY", "@example."} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("fixture contains non-redacted marker %q", forbidden)
		}
	}
}

func decodeAnthropicFixtureFrame(t *testing.T, frame []byte, eventName string) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(string(frame)), "\n")
	if len(lines) != 2 || lines[0] != "event: "+eventName || !strings.HasPrefix(lines[1], "data: ") {
		t.Fatalf("invalid %s SSE frame = %q", eventName, frame)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &payload); err != nil {
		t.Fatalf("invalid %s SSE JSON: %v", eventName, err)
	}
	return payload
}

func fixtureMap(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value is %T, want object", value)
	}
	return result
}
