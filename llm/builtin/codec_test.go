package builtin_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/builtin"
)

type codecCase struct {
	name       string
	id         llm.CodecID
	newCodec   func() llm.Codec
	payload    string
	model      string
	overloaded int
}

func builtinCodecCases() []codecCase {
	return []codecCase{
		{
			name: "OpenAI Chat", id: "openai.chat", newCodec: builtin.OpenAIChat,
			model: "gpt-human",
			payload: `{
				"model":"gpt-human","stream":true,
				"messages":[{"role":"developer","content":"be exact"},{"role":"user","content":"hello"}],
				"tools":[{"type":"function","function":{"name":"lookup","description":"look up","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}}]
			}`,
			overloaded: 503,
		},
		{
			name: "Anthropic Messages", id: "anthropic.messages", newCodec: builtin.AnthropicMessages,
			model: "claude-human",
			payload: `{
				"model":"claude-human","stream":true,"system":"be exact",
				"messages":[{"role":"user","content":"hello"}],
				"tools":[{"name":"lookup","description":"look up","input_schema":{"type":"object","properties":{"q":{"type":"string"}}}}]
			}`,
			overloaded: 529,
		},
		{
			name: "OpenAI Responses", id: "openai.responses", newCodec: builtin.OpenAIResponses,
			model: "codex-human",
			payload: `{
				"model":"codex-human","stream":true,"instructions":"be exact","input":"hello",
				"parallel_tool_calls":false,
				"tools":[{"type":"function","name":"lookup","description":"look up","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}]
			}`,
			overloaded: 503,
		},
	}
}

func TestRegistrationsAreCompleteAndFresh(t *testing.T) {
	first := builtin.Registrations()
	second := builtin.Registrations()
	want := []llm.CodecID{"openai.chat", "openai.responses", "anthropic.messages"}
	if len(first) != len(want) || len(second) != len(want) {
		t.Fatalf("registration lengths = %d/%d, want %d", len(first), len(second), len(want))
	}
	for index, id := range want {
		firstDescription, err := llm.ValidateCodec(first[index].Codec)
		if err != nil {
			t.Fatalf("first registration %d: %v", index, err)
		}
		secondDescription, err := llm.ValidateCodec(second[index].Codec)
		if err != nil {
			t.Fatalf("second registration %d: %v", index, err)
		}
		if firstDescription.ID != id || secondDescription.ID != id {
			t.Fatalf("registration %d IDs = %q/%q, want %q", index, firstDescription.ID, secondDescription.ID, id)
		}
		if first[index].Codec == second[index].Codec {
			t.Fatalf("registration %d reused a mutable Codec instance", index)
		}
		if first[index].StreamContentType != "text/event-stream" ||
			first[index].AggregateContentType != "application/json" || first[index].SuccessStatus != 200 {
			t.Fatalf("registration %d metadata = %+v", index, first[index])
		}
	}
	first[0] = llm.CodecRegistration{}
	if later := builtin.Registrations(); later[0].Codec == nil {
		t.Fatal("Registrations reused a mutable slice")
	}
}

func TestBuiltinsDecodeToOwnedPublicRequests(t *testing.T) {
	for _, test := range builtinCodecCases() {
		t.Run(test.name, func(t *testing.T) {
			codec := test.newCodec()
			description, err := llm.ValidateCodec(codec)
			if err != nil {
				t.Fatalf("ValidateCodec: %v", err)
			}
			if description.ID != test.id || description.Version == "" ||
				description.Fingerprint == "" || description.OverloadedStatus != test.overloaded {
				t.Fatalf("unexpected description: %+v", description)
			}

			body := []byte(test.payload)
			request, err := codec.Decode(body)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if err := request.Validate(); err != nil {
				t.Fatalf("decoded public Request is invalid: %v", err)
			}
			if request.Model != test.model || len(request.Messages) != 1 ||
				request.Messages[0].Role != llm.RoleUser || request.Messages[0].Blocks[0].Text != "hello" {
				t.Fatalf("unexpected public request: %+v", request)
			}
			if len(request.Tools) != 1 || request.Tools[0].Name != "lookup" {
				t.Fatalf("tools = %+v", request.Tools)
			}
			if test.id == "openai.responses" && request.ToolCallPolicy != llm.ToolCallsSerial {
				t.Fatalf("Responses tool policy = %q, want serial", request.ToolCallPolicy)
			}

			// Decode borrows the wire body. Destroying it after return must not
			// change any nested public value.
			for index := range body {
				body[index] = 'x'
			}
			if request.Messages[0].Blocks[0].Text != "hello" ||
				!json.Valid(request.Tools[0].InputSchema) ||
				!bytes.Contains(request.Tools[0].InputSchema, []byte(`"type":"object"`)) {
				t.Fatalf("decoded request aliases its input body: %+v", request)
			}

			// A second decode is independent of mutations to the first public
			// result, including RawMessage and nested map storage.
			request.Tools[0].InputSchema[0] = 'x'
			if request.Metadata != nil {
				request.Metadata["mutated"] = "yes"
			}
			second, err := codec.Decode([]byte(test.payload))
			if err != nil {
				t.Fatalf("second Decode: %v", err)
			}
			if !json.Valid(second.Tools[0].InputSchema) || second.Metadata["mutated"] != "" {
				t.Fatalf("decoded requests share mutable storage: %+v", second)
			}
		})
	}
}

func TestBuiltinsRebuildStreamAndAggregateByteExactly(t *testing.T) {
	events := []llm.Event{
		{ID: "evt-1", Type: llm.EventAccepted, WorkerID: "human-1"},
		{ID: "evt-2", Type: llm.EventProgress, WorkerID: "human-1", Text: "hel"},
		{ID: "evt-3", Type: llm.EventFinal, WorkerID: "human-1", Text: "lo"},
	}
	seeds := []llm.EventSeed{
		{EncodedAtUnix: 1_700_000_101},
		{EncodedAtUnix: 1_700_000_102},
		{EncodedAtUnix: 1_700_000_103},
	}

	for _, test := range builtinCodecCases() {
		for _, aggregate := range []bool{false, true} {
			mode := "stream"
			if aggregate {
				mode = "aggregate"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				first := encodeTranscript(t, test.newCodec(), aggregate, test.model, events, seeds)
				second := encodeTranscript(t, test.newCodec(), aggregate, test.model, events, seeds)
				if !reflect.DeepEqual(first, second) {
					t.Fatalf("reconstructed output differs\nfirst:  %q\nsecond: %q", first, second)
				}
				if len(first) == 0 {
					t.Fatal("encoder returned no observable output")
				}

				// Mutating transferred output cannot affect a newly reconstructed
				// encoder, which also catches shared static frame buffers.
				pristine := cloneFrames(first)
				first[0][0] ^= 0xff
				third := encodeTranscript(t, test.newCodec(), aggregate, test.model, events, seeds)
				if !reflect.DeepEqual(pristine, third) {
					t.Fatal("encoder output aliases mutable storage across reconstruction")
				}
			})
		}
	}
}

func TestBuiltinsEncodeToolCall(t *testing.T) {
	for _, test := range builtinCodecCases() {
		t.Run(test.name, func(t *testing.T) {
			codec := test.newCodec()
			encoder, err := codec.NewStream(testSession(test.model))
			if err != nil {
				t.Fatalf("NewStream: %v", err)
			}
			if _, err := encoder.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			frames, done, err := encoder.Encode(llm.Event{
				ID: "evt-tool", Type: llm.EventToolCalls, WorkerID: "human-1",
				ToolCalls: []llm.ToolCall{{
					ID: "call-1", Name: "lookup",
					Input: map[string]any{"q": "weather", "nested": map[string]any{"n": float64(1)}},
				}},
			}, llm.EventSeed{EncodedAtUnix: 1_700_000_211})
			if err != nil || !done {
				t.Fatalf("Encode(tool call): done=%v err=%v", done, err)
			}
			wire := string(bytes.Join(frames, nil))
			for _, required := range []string{"call-1", "lookup", "weather"} {
				if !strings.Contains(wire, required) {
					t.Fatalf("tool wire does not contain %q: %s", required, wire)
				}
			}
		})
	}
}

func TestBuiltinsRebuildToolCallByteExactly(t *testing.T) {
	events := []llm.Event{{
		ID: "evt-tool-replay", Type: llm.EventToolCalls, WorkerID: "human-1",
		ToolCalls: []llm.ToolCall{{
			ID: "call-replay", Name: "lookup",
			Input: map[string]any{"q": "weather", "nested": map[string]any{"n": float64(1)}},
		}},
	}}
	seeds := []llm.EventSeed{{EncodedAtUnix: 1_700_000_212}}
	for _, test := range builtinCodecCases() {
		for _, aggregate := range []bool{false, true} {
			mode := "stream"
			if aggregate {
				mode = "aggregate"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				first := encodeTranscript(t, test.newCodec(), aggregate, test.model, events, seeds)
				second := encodeTranscript(t, test.newCodec(), aggregate, test.model, events, seeds)
				if !reflect.DeepEqual(first, second) {
					t.Fatalf("reconstructed tool output differs\nfirst:  %q\nsecond: %q", first, second)
				}
			})
		}
	}
}

func TestResponsesStreamLifecycleAndEmptyFinal(t *testing.T) {
	codec := builtin.OpenAIResponses()
	encoder, err := codec.NewStream(testSession("codex-human"))
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	start, err := encoder.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	startWire := string(bytes.Join(start, nil))
	for _, eventName := range []string{"response.created", "response.in_progress"} {
		if !strings.Contains(startWire, "event: "+eventName) {
			t.Fatalf("start misses %s: %s", eventName, startWire)
		}
	}
	if !strings.Contains(startWire, `"created_at":1700000001`) {
		t.Fatalf("Responses start did not use durable created-at seed: %s", startWire)
	}

	frames, done, err := encoder.Encode(
		llm.Event{ID: "evt-empty", Type: llm.EventFinal, WorkerID: "human-1"},
		llm.EventSeed{EncodedAtUnix: 1_700_000_311},
	)
	if err != nil || !done {
		t.Fatalf("Encode(empty final): done=%v err=%v", done, err)
	}
	wire := string(bytes.Join(frames, nil))
	for _, eventName := range []string{
		"response.output_item.added",
		"response.content_part.added",
		"response.output_text.done",
		"response.content_part.done",
		"response.output_item.done",
		"response.completed",
	} {
		if !strings.Contains(wire, "event: "+eventName) {
			t.Fatalf("empty final misses %s: %s", eventName, wire)
		}
	}
	if !strings.Contains(wire, `"output":[{`) || !strings.Contains(wire, `"text":""`) ||
		!strings.Contains(wire, `"completed_at":1700000311`) {
		t.Fatalf("empty final lost assistant turn or durable event time: %s", wire)
	}
}

func TestOpenAIChatUsesDurableCreatedAtSeed(t *testing.T) {
	encoder, err := builtin.OpenAIChat().NewStream(testSession("gpt-human"))
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	frames, err := encoder.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	wire := string(bytes.Join(frames, nil))
	if !strings.Contains(wire, `"created":1700000001`) {
		t.Fatalf("Chat start did not use durable created-at seed: %s", wire)
	}
}

func TestResponsesToolLifecycle(t *testing.T) {
	encoder, err := builtin.OpenAIResponses().NewStream(testSession("codex-human"))
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if _, err := encoder.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	frames, done, err := encoder.Encode(llm.Event{
		Type: llm.EventToolCalls,
		ToolCalls: []llm.ToolCall{{
			ID: "call-9", Namespace: "workspace", Name: "edit",
			Input: map[string]any{"path": "README.md"},
		}},
	}, llm.EventSeed{EncodedAtUnix: 1_700_000_411})
	if err != nil || !done {
		t.Fatalf("Encode: done=%v err=%v", done, err)
	}
	wire := string(bytes.Join(frames, nil))
	for _, eventName := range []string{
		"response.output_item.added",
		"response.function_call_arguments.delta",
		"response.function_call_arguments.done",
		"response.output_item.done",
		"response.completed",
	} {
		if !strings.Contains(wire, "event: "+eventName) {
			t.Fatalf("tool lifecycle misses %s: %s", eventName, wire)
		}
	}
	if !strings.Contains(wire, `"namespace":"workspace"`) ||
		!strings.Contains(wire, `"completed_at":1700000411`) {
		t.Fatalf("tool lifecycle loses namespace or durable time: %s", wire)
	}
}

func TestBuiltinsAdmissionErrorsAreValidatedAndDeterministic(t *testing.T) {
	failure := llm.AdmissionFailure{Status: 503, Code: "worker_unavailable", Message: "human unavailable"}
	for _, test := range builtinCodecCases() {
		t.Run(test.name, func(t *testing.T) {
			codec := test.newCodec()
			first, err := codec.AdmissionError(failure)
			if err != nil {
				t.Fatalf("AdmissionError: %v", err)
			}
			second, err := codec.AdmissionError(failure)
			if err != nil {
				t.Fatalf("second AdmissionError: %v", err)
			}
			if !bytes.Equal(first, second) || !json.Valid(first) ||
				!bytes.Contains(first, []byte("human unavailable")) {
				t.Fatalf("invalid/non-deterministic admission body: %q / %q", first, second)
			}
			if err := codec.Description().Limits.CheckAdmissionError(first); err != nil {
				t.Fatalf("admission output exceeds descriptor: %v", err)
			}
			if _, err := codec.AdmissionError(llm.AdmissionFailure{
				Status: 200, Code: "not_an_error", Message: "bad",
			}); !errors.Is(err, llm.ErrInvalidCodecContract) {
				t.Fatalf("invalid failure error = %v", err)
			}
		})
	}
}

func TestBuiltinsRejectUnpersistedOrUnknownSeeds(t *testing.T) {
	for _, test := range builtinCodecCases() {
		t.Run(test.name, func(t *testing.T) {
			for name, mutate := range map[string]func(*llm.EncoderSession){
				"missing created time": func(session *llm.EncoderSession) { session.Seed.CreatedAtUnix = 0 },
				"session entropy":      func(session *llm.EncoderSession) { session.Seed.Entropy = []byte{1} },
				"session opaque":       func(session *llm.EncoderSession) { session.Seed.Opaque = json.RawMessage(`null`) },
			} {
				t.Run(name, func(t *testing.T) {
					session := testSession(test.model)
					mutate(&session)
					if _, err := test.newCodec().NewStream(session); !errors.Is(err, llm.ErrInvalidCodecContract) {
						t.Fatalf("NewStream error = %v", err)
					}
				})
			}

			for name, seed := range map[string]llm.EventSeed{
				"missing encoded time": {},
				"event entropy":        {EncodedAtUnix: 1, Entropy: []byte{1}},
				"event opaque":         {EncodedAtUnix: 1, Opaque: json.RawMessage(`null`)},
			} {
				t.Run(name, func(t *testing.T) {
					encoder, err := test.newCodec().NewStream(testSession(test.model))
					if err != nil {
						t.Fatalf("NewStream: %v", err)
					}
					if _, err := encoder.Start(); err != nil {
						t.Fatalf("Start: %v", err)
					}
					if _, _, err := encoder.Encode(llm.Event{Type: llm.EventAccepted}, seed); !errors.Is(err, llm.ErrInvalidCodecContract) {
						t.Fatalf("Encode error = %v", err)
					}
				})
			}
		})
	}
}

func encodeTranscript(
	t *testing.T,
	codec llm.Codec,
	aggregate bool,
	model string,
	events []llm.Event,
	seeds []llm.EventSeed,
) [][]byte {
	t.Helper()
	if len(events) != len(seeds) {
		t.Fatal("test fixture event/seed mismatch")
	}
	description, err := llm.ValidateCodec(codec)
	if err != nil {
		t.Fatalf("ValidateCodec: %v", err)
	}
	var encoder llm.Encoder
	if aggregate {
		encoder, err = codec.NewAggregate(testSession(model))
	} else {
		encoder, err = codec.NewStream(testSession(model))
	}
	if err != nil {
		t.Fatalf("construct encoder: %v", err)
	}
	start, err := encoder.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if aggregate {
		if err := description.Limits.CheckAggregateFrames(start, false); err != nil {
			t.Fatalf("aggregate start: %v", err)
		}
	} else if err := description.Limits.CheckStreamFrames(start); err != nil {
		t.Fatalf("stream start: %v", err)
	}
	output := cloneFrames(start)
	for index, event := range events {
		frames, done, err := encoder.Encode(event, seeds[index])
		if err != nil {
			t.Fatalf("Encode event %d (%s): %v", index, event.Type, err)
		}
		if aggregate {
			if err := description.Limits.CheckAggregateFrames(frames, done); err != nil {
				t.Fatalf("aggregate event %d: %v", index, err)
			}
		} else if err := description.Limits.CheckStreamFrames(frames); err != nil {
			t.Fatalf("stream event %d: %v", index, err)
		}
		output = append(output, cloneFrames(frames)...)
		if done != (index == len(events)-1) {
			t.Fatalf("event %d done=%v", index, done)
		}
	}
	return output
}

func testSession(model string) llm.EncoderSession {
	return llm.EncoderSession{
		ResponseID: "response-stable-1",
		Model:      model,
		Seed: llm.SessionSeed{
			CreatedAtUnix:  1_700_000_001,
			ToolCallPolicy: llm.ToolCallsParallel,
		},
	}
}

func cloneFrames(frames [][]byte) [][]byte {
	cloned := make([][]byte, len(frames))
	for index, frame := range frames {
		cloned[index] = bytes.Clone(frame)
	}
	return cloned
}

func TestPublicAPIConstructorsHaveStableDistinctIdentity(t *testing.T) {
	seen := make(map[llm.CodecID]llm.CodecFingerprint)
	for _, test := range builtinCodecCases() {
		description, err := llm.ValidateCodec(test.newCodec())
		if err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		if previous, exists := seen[description.ID]; exists {
			t.Fatalf("duplicate codec id %q (%s and %s)", description.ID, previous, description.Fingerprint)
		}
		seen[description.ID] = description.Fingerprint
	}
}

func TestToolCallInputPreservesIntegersBeyondFloat64Precision(t *testing.T) {
	const exact = "9007199254740993"
	for _, test := range builtinCodecCases() {
		t.Run(test.name, func(t *testing.T) {
			frames := encodeTranscript(t, test.newCodec(), false, test.model, []llm.Event{{
				ID: "event-big-integer", Type: llm.EventToolCalls,
				ToolCalls: []llm.ToolCall{{
					ID: "call-big-integer", Name: "calculate",
					Input: map[string]any{"value": json.Number(exact)},
				}},
			}}, []llm.EventSeed{{EncodedAtUnix: 1_700_000_002}})
			if output := string(bytes.Join(frames, nil)); !strings.Contains(output, exact) {
				t.Fatalf("encoded tool input lost exact integer: %s", output)
			}
		})
	}
}

func TestBuiltinsDecodeToolJSONPreservesIntegersBeyondFloat64Precision(t *testing.T) {
	const exact = "9007199254740993"
	tests := []struct {
		name       string
		codec      llm.Codec
		payload    string
		inputAt    int
		outputAt   int
		wantOutput bool
	}{
		{
			name: "OpenAI Chat arguments", codec: builtin.OpenAIChat(), inputAt: 1, outputAt: -1,
			payload: `{
				"model":"gpt-human","stream":true,
				"messages":[
					{"role":"user","content":"calculate"},
					{"role":"assistant","tool_calls":[{"id":"call-big","type":"function","function":{"name":"calculate","arguments":"{\"value\":9007199254740993}"}}]}
				],
				"tools":[{"type":"function","function":{"name":"calculate","parameters":{"type":"object"}}}]
			}`,
		},
		{
			name: "Anthropic tool use", codec: builtin.AnthropicMessages(), inputAt: 1, outputAt: -1,
			payload: `{
				"model":"claude-human","stream":true,
				"messages":[
					{"role":"user","content":"calculate"},
					{"role":"assistant","content":[{"type":"tool_use","id":"call-big","name":"calculate","input":{"value":9007199254740993}}]}
				],
				"tools":[{"name":"calculate","input_schema":{"type":"object"}}]
			}`,
		},
		{
			name: "OpenAI Responses arguments and output", codec: builtin.OpenAIResponses(),
			inputAt: 0, outputAt: 1, wantOutput: true,
			payload: `{
				"model":"codex-human","stream":true,
				"input":[
					{"type":"function_call","call_id":"call-big","name":"calculate","arguments":"{\"value\":9007199254740993}"},
					{"type":"function_call_output","call_id":"call-big","output":{"value":9007199254740993}}
				],
				"tools":[{"type":"function","name":"calculate","parameters":{"type":"object"}}]
			}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := test.codec.Decode([]byte(test.payload))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			input := request.Messages[test.inputAt].Blocks[0].Input["value"]
			if number, ok := input.(json.Number); !ok || number.String() != exact {
				t.Fatalf("tool input value = %T(%v), want json.Number(%s)", input, input, exact)
			}
			if test.wantOutput {
				output := request.Messages[test.outputAt].Blocks[0].Output.(map[string]any)["value"]
				if number, ok := output.(json.Number); !ok || number.String() != exact {
					t.Fatalf("tool output value = %T(%v), want json.Number(%s)", output, output, exact)
				}
			}
		})
	}
}
