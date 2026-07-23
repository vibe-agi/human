package responses

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

func TestDecodeModernCodexToolsReasoningAndSerialPolicy(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
	"model":"human-expert","stream":true,"parallel_tool_calls":false,
	"store":false,"include":["reasoning.encrypted_content"],"client_metadata":{"origin":"codex"},
	"prompt_cache_key":"cache-key","reasoning":{"summary":"auto"},"max_output_tokens":null,
  "input":[
    {"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"private-provider-state"},
    {"type":"message","role":"user","content":[{"type":"input_text","text":"inspect"}]},
    {"type":"function_call","call_id":"call_ns","namespace":"multi_agent_v1","name":"spawn_agent","arguments":"{\"task\":\"audit\"}"}
  ],
  "tools":[
    {"type":"function","name":"read","parameters":{"type":"object"}},
    {"type":"namespace","name":"multi_agent_v1","description":"agent controls","tools":[
      {"type":"function","name":"spawn_agent","parameters":{"type":"object"},"strict":false},
      {"type":"function","name":"read","parameters":{"type":"object"},"strict":false}
    ]},
    {"type":"web_search","external_web_access":true}
  ]
}`)
	request, err := New().Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if request.ToolCallPolicy != canonical.ToolCallsSerial {
		t.Fatalf("tool-call policy = %q", request.ToolCallPolicy)
	}
	if len(request.Tools) != 3 || request.Tools[0].QualifiedName() != "read" ||
		request.Tools[1].QualifiedName() != "multi_agent_v1::spawn_agent" ||
		request.Tools[2].QualifiedName() != "multi_agent_v1::read" {
		t.Fatalf("callable tools = %+v", request.Tools)
	}
	if len(request.HostedCapabilities) != 1 || request.HostedCapabilities[0].Type != "web_search" ||
		!strings.Contains(string(request.HostedCapabilities[0].Configuration), `"external_web_access":true`) {
		t.Fatalf("hosted capabilities = %+v", request.HostedCapabilities)
	}
	if len(request.OpaqueInput) != 1 || request.OpaqueInput[0].Type != "reasoning" ||
		len(request.OpaqueInput[0].SHA256) != 64 {
		t.Fatalf("opaque input = %+v", request.OpaqueInput)
	}
	canonicalJSON, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonicalJSON), "private-provider-state") {
		t.Fatalf("raw reasoning state crossed the canonical worker boundary: %s", canonicalJSON)
	}
	redecoded, err := New().Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if request.OpaqueInput[0].SHA256 != redecoded.OpaqueInput[0].SHA256 {
		t.Fatal("identical opaque reasoning bytes produced an unstable fingerprint")
	}
	if len(request.Messages) != 2 || request.Messages[0].Blocks[0].Text != "inspect" {
		t.Fatalf("human transcript messages = %+v", request.Messages)
	}
	call := request.Messages[1].Blocks[0]
	if call.Type != canonical.BlockToolUse || call.ToolNamespace != "multi_agent_v1" ||
		call.ToolName != "spawn_agent" || call.QualifiedToolName() != "multi_agent_v1::spawn_agent" {
		t.Fatalf("namespaced history call = %+v", call)
	}

	otherPayload := []byte(strings.Replace(string(payload), "private-provider-state", "other-provider-state", 1))
	other, err := New().Decode(otherPayload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(request.Messages, other.Messages) {
		t.Fatal("opaque reasoning state leaked into canonical transcript")
	}
	if request.OpaqueInput[0].SHA256 == other.OpaqueInput[0].SHA256 {
		t.Fatal("distinct opaque reasoning bytes produced the same fingerprint")
	}
	firstDigest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := other.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest == secondDigest {
		t.Fatal("distinct opaque reasoning items produced the same request digest")
	}
}

func TestTopLevelReasoningAcceptsOfficialHintsAndRejectsUnknownControls(t *testing.T) {
	t.Parallel()
	for _, reasoning := range []string{
		`{"summary":"auto"}`,
		`{"effort":"medium","summary":"detailed"}`,
		`{"effort":"none","generate_summary":"concise"}`,
	} {
		if _, err := New().Decode([]byte(`{
  "model":"m","stream":true,"input":"hello","reasoning":` + reasoning + `
}`)); err != nil {
			t.Fatalf("official reasoning hint %s rejected: %v", reasoning, err)
		}
	}
	if _, err := New().Decode([]byte(`{
  "model":"m","stream":true,"input":"hello",
  "reasoning":{"summary":"auto","future_control":true}
}`)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown top-level reasoning error = %v", err)
	}
}

func TestTopLevelBehaviorControlsFailClosed(t *testing.T) {
	t.Parallel()
	tests := []string{
		`"background":true`,
		`"store":true`,
		`"include":["unsupported.provider_state"]`,
		`"include":["reasoning.encrypted_content","reasoning.encrypted_content"]`,
		`"max_output_tokens":-1`,
		`"temperature":2.1`,
		`"top_logprobs":1`,
		`"conversation":"conv_1"`,
		`"prompt":{"id":"pmpt_1"}`,
		`"client_metadata":"not-an-object"`,
		`"prompt_cache_key":{"bad":true}`,
		`"reasoning":"medium"`,
		`"reasoning":{"summary":"verbose"}`,
	}
	for _, control := range tests {
		control := control
		t.Run(control, func(t *testing.T) {
			t.Parallel()
			payload := `{"model":"m","stream":true,"input":"hello",` + control + `}`
			if _, err := New().Decode([]byte(payload)); err == nil {
				t.Fatalf("unsupported control accepted: %s", payload)
			}
		})
	}
}

func TestNamespacedFunctionRoundTripsThroughStreamAndAggregate(t *testing.T) {
	t.Parallel()
	call := completion.ToolCall{
		ID: "call_ns", Namespace: "multi_agent_v1", Name: "spawn_agent",
		Input: map[string]any{"task": "audit"},
	}
	seed := dialect.StreamSeed{CreatedAtUnix: 123, ToolCallPolicy: canonical.ToolCallsSerial}

	stream := New().NewStream("resp_ns", "human-expert", seed)
	start, err := stream.Start()
	if err != nil {
		t.Fatal(err)
	}
	created := assertEvent(t, start[0], "response.created", 0)
	if created["response"].(map[string]any)["parallel_tool_calls"] != false {
		t.Fatalf("serial stream start = %#v", created)
	}
	frames, done, err := stream.Encode(completion.Event{
		Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{call},
	})
	if err != nil || !done {
		t.Fatalf("stream encode = done=%v err=%v", done, err)
	}
	added := assertEvent(t, frames[0], "response.output_item.added", 2)
	if item := added["item"].(map[string]any); item["namespace"] != "multi_agent_v1" || item["name"] != "spawn_agent" {
		t.Fatalf("stream namespaced item = %#v", item)
	}
	argumentsDone := assertEvent(t, frames[2], "response.function_call_arguments.done", 4)
	if argumentsDone["namespace"] != "multi_agent_v1" || argumentsDone["name"] != "spawn_agent" {
		t.Fatalf("stream namespaced arguments done = %#v", argumentsDone)
	}
	completed := assertEvent(t, frames[len(frames)-1], "response.completed", 6)
	response := completed["response"].(map[string]any)
	item := response["output"].([]any)[0].(map[string]any)
	if response["parallel_tool_calls"] != false || item["namespace"] != "multi_agent_v1" {
		t.Fatalf("stream completed response = %#v", response)
	}

	aggregate := New().NewAggregate("resp_ns", "human-expert", seed)
	if _, err := aggregate.Start(); err != nil {
		t.Fatal(err)
	}
	bodies, done, err := aggregate.Encode(completion.Event{
		Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{call},
	})
	if err != nil || !done || len(bodies) != 1 {
		t.Fatalf("aggregate encode = %#v done=%v err=%v", bodies, done, err)
	}
	var body map[string]any
	if err := json.Unmarshal(bodies[0], &body); err != nil {
		t.Fatal(err)
	}
	aggregateCall := body["output"].([]any)[0].(map[string]any)
	if body["parallel_tool_calls"] != false || aggregateCall["namespace"] != "multi_agent_v1" ||
		aggregateCall["name"] != "spawn_agent" {
		t.Fatalf("aggregate namespaced response = %#v", body)
	}
}

func TestNamespaceRejectsNonFunctionNestedTool(t *testing.T) {
	t.Parallel()
	_, err := New().Decode([]byte(`{
  "model":"m","stream":true,"input":"hello",
  "tools":[{"type":"namespace","name":"bad","tools":[{"type":"web_search"}]}]
}`))
	if err == nil || !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("nested hosted tool error = %v", err)
	}
}
