package responses

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

func TestGoldenResponsesToolsCanonicalAndStreamContract(t *testing.T) {
	t.Parallel()
	payload := readFixture(t, "testdata/tools_request.json")
	assertRedactedFixture(t, payload)
	codec := Codec{now: func() time.Time { return time.Unix(1_700_000_000, 0) }}
	request, err := codec.Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalFixture(t, request, "testdata/tools_canonical.golden.json")
	for key, want := range map[string]string{
		"caller_id": "caller_fixture_01", "workspace_key": "workspace_fixture_01",
		"task_id": "task_fixture_01", "idempotency_key": "idem_fixture_01",
	} {
		if request.Metadata[key] != want {
			t.Fatalf("metadata[%q] = %q, want %q", key, request.Metadata[key], want)
		}
	}
	call, result := fixtureToolPair(t, request)
	if call.ToolCallID != result.ToolCallID || call.ToolCallID != "call_fixture_read_01" {
		t.Fatalf("tool identity use/result = %q / %q", call.ToolCallID, result.ToolCallID)
	}
	assertStableDigest(t, request)

	stream := codec.NewStream("responses_fixture_01", request.Model)
	start, err := stream.Start()
	if err != nil || len(start) != 2 {
		t.Fatalf("start = %q, %v", start, err)
	}
	created := assertEvent(t, start[0], "response.created", 0)
	createdResponse := fixtureMap(t, created["response"])
	if createdResponse["id"] != "responses_fixture_01" || createdResponse["model"] != request.Model {
		t.Fatalf("created response identity = %#v", createdResponse)
	}
	assertEvent(t, start[1], "response.in_progress", 1)

	frames, done, err := stream.Encode(completion.Event{
		Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{{
			ID: call.ToolCallID, Name: call.ToolName, Input: call.Input,
		}},
	})
	if err != nil || !done || len(frames) != 5 {
		t.Fatalf("tool stream = %q, %v, %v", frames, done, err)
	}
	added := assertEvent(t, frames[0], "response.output_item.added", 2)
	if item := fixtureMap(t, added["item"]); item["status"] != "in_progress" || item["arguments"] != "" {
		t.Fatalf("added tool = %#v", item)
	}
	delta := assertEvent(t, frames[1], "response.function_call_arguments.delta", 3)
	encodedCall := assertEvent(t, frames[2], "response.function_call_arguments.done", 4)
	if encodedCall["item_id"] != "fc_"+call.ToolCallID || encodedCall["name"] != call.ToolName {
		t.Fatalf("encoded tool identity = %#v", encodedCall)
	}
	var arguments map[string]any
	encodedArguments, ok := encodedCall["arguments"].(string)
	if !ok {
		t.Fatalf("arguments = %#v", encodedCall["arguments"])
	}
	if err := json.Unmarshal([]byte(encodedArguments), &arguments); err != nil ||
		!reflect.DeepEqual(arguments, call.Input) {
		t.Fatalf("encoded tool arguments = %#v, %v", arguments, err)
	}
	if delta["delta"] != encodedArguments {
		t.Fatalf("tool argument delta = %#v, want %q", delta, encodedArguments)
	}
	doneItem := assertEvent(t, frames[3], "response.output_item.done", 5)
	if item := fixtureMap(t, doneItem["item"]); item["status"] != "completed" || item["arguments"] != encodedArguments {
		t.Fatalf("done tool = %#v", item)
	}
	completed := assertEvent(t, frames[4], "response.completed", 6)
	response := fixtureMap(t, completed["response"])
	output := fixtureSlice(t, response["output"])
	function := fixtureMap(t, output[0])
	if function["id"] != "fc_"+call.ToolCallID || function["call_id"] != call.ToolCallID ||
		function["name"] != call.ToolName {
		t.Fatalf("completed tool identity = %#v", function)
	}
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

func fixtureMap(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value is %T, want object", value)
	}
	return result
}

func fixtureSlice(t *testing.T, value any) []any {
	t.Helper()
	result, ok := value.([]any)
	if !ok || len(result) == 0 {
		t.Fatalf("value is %#v, want non-empty array", value)
	}
	return result
}
