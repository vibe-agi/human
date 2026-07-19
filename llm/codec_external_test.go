package llm_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

type exampleCodec struct{}

var _ llm.Codec = (*exampleCodec)(nil)

func (*exampleCodec) Description() llm.CodecDescription {
	return llm.CodecDescription{
		Contract: framework.Contract{
			ID:       llm.CodecContractID,
			Major:    llm.CodecContractMajor,
			Minor:    2,
			Features: map[framework.Feature]uint16{"example.trace": 1},
		},
		ID:          "example.messages",
		Version:     "2026.07.1",
		Fingerprint: llm.Fingerprint([]byte("example-codec/2026.07.1\nformat=v1")),
		Limits: llm.CodecLimits{
			MaxRequestBytes:        1 << 20,
			MaxStreamFrameBytes:    1 << 16,
			MaxStreamFramesPerStep: 4,
			MaxAggregateBytes:      1 << 20,
			MaxAdmissionErrorBytes: 4096,
		},
		OverloadedStatus: 529,
	}
}

func (*exampleCodec) Decode(body []byte) (llm.Request, error) {
	var wire struct {
		Model  string `json:"model"`
		Input  string `json:"input"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return llm.Request{}, err
	}
	request := llm.Request{
		Model:  wire.Model,
		Stream: wire.Stream,
		Messages: []llm.Message{{
			Role:   llm.RoleUser,
			Blocks: []llm.Block{{Type: llm.BlockText, Text: wire.Input}},
		}},
	}
	return request, request.Validate()
}

func (*exampleCodec) NewStream(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return newExampleEncoder("stream", session), nil
}

func (*exampleCodec) NewAggregate(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return newExampleEncoder("aggregate", session), nil
}

func (*exampleCodec) AdmissionError(failure llm.AdmissionFailure) ([]byte, error) {
	if err := failure.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"error": map[string]any{"status": failure.Status, "code": failure.Code, "message": failure.Message},
	})
}

type exampleEncoder struct {
	mode       string
	responseID string
	model      string
	created    int64
	entropy    []byte
	started    bool
	done       bool
	text       string
}

func newExampleEncoder(mode string, session llm.EncoderSession) *exampleEncoder {
	return &exampleEncoder{
		mode:       mode,
		responseID: session.ResponseID,
		model:      session.Model,
		created:    session.Seed.CreatedAtUnix,
		entropy:    bytes.Clone(session.Seed.Entropy),
	}
}

func (encoder *exampleEncoder) Start() ([][]byte, error) {
	if encoder.started {
		return nil, errors.New("already started")
	}
	encoder.started = true
	if encoder.mode == "aggregate" {
		return nil, nil
	}
	return [][]byte{[]byte(fmt.Sprintf(
		"start:%s:%s:%d:%x", encoder.responseID, encoder.model, encoder.created, encoder.entropy,
	))}, nil
}

func (encoder *exampleEncoder) Encode(event llm.Event, seed llm.EventSeed) ([][]byte, bool, error) {
	if !encoder.started || encoder.done {
		return nil, encoder.done, errors.New("invalid encoder state")
	}
	if err := seed.Validate(); err != nil {
		return nil, false, err
	}
	encoder.text += event.Text
	if encoder.mode == "aggregate" && !event.EndsResponse() {
		return nil, false, nil
	}
	payload := []byte(fmt.Sprintf("event:%s:%d:%x:%s", event.Type, seed.EncodedAtUnix, seed.Entropy, encoder.text))
	encoder.done = event.EndsResponse()
	return [][]byte{payload}, encoder.done, nil
}

func TestExternalPackageCanImplementCodecAndReplayExactly(t *testing.T) {
	codec := &exampleCodec{}
	description, err := llm.ValidateCodec(codec)
	if err != nil {
		t.Fatalf("ValidateCodec: %v", err)
	}
	if description.ID != "example.messages" || description.OverloadedStatus != 529 {
		t.Fatalf("unexpected description: %+v", description)
	}

	body := []byte(`{"model":"human-expert","input":"review this","stream":true}`)
	request, err := codec.Decode(body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for index := range body {
		body[index] = 'x'
	}
	if got := request.Messages[0].Blocks[0].Text; got != "review this" {
		t.Fatalf("Decode retained input buffer: got %q", got)
	}

	session := llm.EncoderSession{
		ResponseID: "response-1",
		Model:      request.Model,
		Seed: llm.SessionSeed{
			CreatedAtUnix: 1_750_000_123,
			Entropy:       []byte{0x01, 0x02, 0x03},
			Opaque:        json.RawMessage(`{"revision":7}`),
		},
	}
	events := []struct {
		event llm.Event
		seed  llm.EventSeed
	}{
		{llm.Event{Type: llm.EventProgress, Text: "hel"}, llm.EventSeed{EncodedAtUnix: 1_750_000_124}},
		{llm.Event{Type: llm.EventFinal, Text: "lo"}, llm.EventSeed{EncodedAtUnix: 1_750_000_125, Entropy: []byte{0x09}}},
	}
	first := runStream(t, codec, session, events)
	second := runStream(t, codec, session, events)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("reconstruction was not byte-exact:\nfirst  %q\nsecond %q", first, second)
	}

	first[0][0] = 'X'
	third := runStream(t, codec, session, events)
	if third[0][0] == 'X' {
		t.Fatal("encoder reused a caller-owned output buffer")
	}
}

func runStream(
	t *testing.T,
	codec llm.Codec,
	session llm.EncoderSession,
	events []struct {
		event llm.Event
		seed  llm.EventSeed
	},
) [][]byte {
	t.Helper()
	encoder, err := codec.NewStream(session)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	frames, err := encoder.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	all := cloneFrames(frames)
	for index, item := range events {
		frames, done, err := encoder.Encode(item.event, item.seed)
		if err != nil {
			t.Fatalf("Encode %d: %v", index, err)
		}
		if err := (&exampleCodec{}).Description().Limits.CheckStreamFrames(frames); err != nil {
			t.Fatalf("CheckStreamFrames %d: %v", index, err)
		}
		all = append(all, cloneFrames(frames)...)
		if done != item.event.EndsResponse() {
			t.Fatalf("Encode %d done=%v, event terminal=%v", index, done, item.event.EndsResponse())
		}
	}
	return all
}

func cloneFrames(frames [][]byte) [][]byte {
	cloned := make([][]byte, len(frames))
	for index, frame := range frames {
		cloned[index] = bytes.Clone(frame)
	}
	return cloned
}

func TestNegotiateCodecFreezesContractAndRejectsTypedNil(t *testing.T) {
	codec := &exampleCodec{}
	description := codec.Description()
	frozen, err := llm.NegotiateCodec(description)
	if err != nil {
		t.Fatalf("NegotiateCodec: %v", err)
	}
	description.Contract.Features["example.trace"] = 99
	if got := frozen.Contract.Features["example.trace"]; got != 1 {
		t.Fatalf("negotiated feature map aliases provider: got %d", got)
	}

	var typedNil *exampleCodec
	if _, err := llm.ValidateCodec(typedNil); !errors.Is(err, llm.ErrInvalidCodecContract) {
		t.Fatalf("typed nil error = %v", err)
	}
}

func TestNegotiateCodecRejectsUnstableIdentityAndUnsafeLimits(t *testing.T) {
	valid := (&exampleCodec{}).Description()
	tests := []struct {
		name   string
		mutate func(*llm.CodecDescription)
	}{
		{"contract", func(value *llm.CodecDescription) { value.Contract.Major++ }},
		{"id", func(value *llm.CodecDescription) { value.ID = "deployment token" }},
		{"version", func(value *llm.CodecDescription) { value.Version = " mutable " }},
		{"fingerprint", func(value *llm.CodecDescription) { value.Fingerprint = "sha256:not-a-digest" }},
		{"request limit", func(value *llm.CodecDescription) { value.Limits.MaxRequestBytes = 0 }},
		{"frame count", func(value *llm.CodecDescription) { value.Limits.MaxStreamFramesPerStep = -1 }},
		{"overload status", func(value *llm.CodecDescription) { value.OverloadedStatus = 200 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			description := valid
			test.mutate(&description)
			_, err := llm.NegotiateCodec(description)
			if !errors.Is(err, llm.ErrInvalidCodecContract) {
				t.Fatalf("NegotiateCodec error = %v", err)
			}
		})
	}
}

func TestCodecLimitsEnforceModeSpecificOutput(t *testing.T) {
	limits := llm.CodecLimits{
		MaxRequestBytes:        4,
		MaxStreamFrameBytes:    3,
		MaxStreamFramesPerStep: 2,
		MaxAggregateBytes:      5,
		MaxAdmissionErrorBytes: 6,
	}
	checks := []struct {
		name string
		err  error
	}{
		{"request", limits.CheckRequestSize(5)},
		{"stream frame", limits.CheckStreamFrames([][]byte{[]byte("four")})},
		{"stream count", limits.CheckStreamFrames([][]byte{{}, {}, {}})},
		{"early aggregate", limits.CheckAggregateFrames([][]byte{[]byte("body")}, false)},
		{"missing aggregate", limits.CheckAggregateFrames(nil, true)},
		{"large aggregate", limits.CheckAggregateFrames([][]byte{[]byte("123456")}, true)},
		{"admission", limits.CheckAdmissionError([]byte("1234567"))},
	}
	for _, check := range checks {
		if !errors.Is(check.err, llm.ErrInvalidCodecContract) {
			t.Errorf("%s error = %v", check.name, check.err)
		}
	}
	if err := limits.CheckRequestSize(4); err != nil {
		t.Fatalf("boundary request: %v", err)
	}
	if err := limits.CheckStreamFrames([][]byte{[]byte("one"), []byte("two")}); err != nil {
		t.Fatalf("boundary stream: %v", err)
	}
	if err := limits.CheckAggregateFrames([][]byte{[]byte("12345")}, true); err != nil {
		t.Fatalf("boundary aggregate: %v", err)
	}
	if err := (llm.CodecLimits{}).CheckRequestSize(0); !errors.Is(err, llm.ErrInvalidCodecContract) {
		t.Fatalf("unnegotiated zero limits passed: %v", err)
	}
}

func TestRequestDigestIsDeterministicAndCodecNeutral(t *testing.T) {
	request := llm.Request{
		Model: "human-expert",
		Messages: []llm.Message{{
			Role:   llm.RoleUser,
			Blocks: []llm.Block{{Type: llm.BlockText, Text: "inspect workspace"}},
		}},
		Tools:    []llm.Tool{{Name: "read", InputSchema: json.RawMessage(`{"type":"object"}`)}},
		Metadata: map[string]string{"z": "last", "a": "first"},
	}
	first, err := request.Digest()
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	second, err := request.Digest()
	if err != nil {
		t.Fatalf("Digest again: %v", err)
	}
	if first != second || len(first) != 64 {
		t.Fatalf("unstable digest: first=%q second=%q", first, second)
	}
	if err := (llm.Request{Model: "human-expert"}).Validate(); err == nil {
		t.Fatal("request without messages passed validation")
	}
}

func TestAdmissionFailureAndAggregateAreUsableExternally(t *testing.T) {
	codec := &exampleCodec{}
	failure := llm.AdmissionFailure{Status: 503, Code: "capacity", Message: "try again"}
	body, err := codec.AdmissionError(failure)
	if err != nil {
		t.Fatalf("AdmissionError: %v", err)
	}
	if err := codec.Description().Limits.CheckAdmissionError(body); err != nil {
		t.Fatalf("CheckAdmissionError: %v", err)
	}

	session := llm.EncoderSession{ResponseID: "r", Model: "m", Seed: llm.SessionSeed{CreatedAtUnix: 1}}
	encoder, err := codec.NewAggregate(session)
	if err != nil {
		t.Fatalf("NewAggregate: %v", err)
	}
	frames, err := encoder.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := codec.Description().Limits.CheckAggregateFrames(frames, false); err != nil {
		t.Fatalf("aggregate Start output: %v", err)
	}
	frames, done, err := encoder.Encode(llm.Event{Type: llm.EventFinal, Text: "done"}, llm.EventSeed{EncodedAtUnix: 2})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := codec.Description().Limits.CheckAggregateFrames(frames, done); err != nil {
		t.Fatalf("aggregate terminal output: %v", err)
	}
}
