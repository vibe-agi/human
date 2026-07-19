package llm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/vibe-agi/human/framework"
)

const (
	// CodecContractID is the public protocol implemented by HumanLLM codecs.
	CodecContractID framework.ContractID = "human.llm.codec"
	// CodecContractMajor is the exact base contract third-party codecs declare.
	CodecContractMajor uint16 = 1

	maximumCodecBytes    int64 = 128 << 20
	maximumFramesPerStep       = 4096
	maximumSeedBytes     int64 = 64 << 10
)

var codecIDPattern = regexp.MustCompile(`^[a-z][a-z0-9._/-]{0,127}$`)

// ErrInvalidCodecContract means a codec descriptor or encoder output violates
// the public Codec contract.
var ErrInvalidCodecContract = errors.New("llm: invalid codec contract")

// CodecID is a stable logical wire-protocol identity, for example
// "openai.chat". It must not contain deployment-specific values.
type CodecID string

// CodecFingerprint is a SHA-256 pin for every implementation and configuration
// input that can affect decoded canonical values or encoded bytes.
type CodecFingerprint string

// Fingerprint returns the canonical fingerprint spelling for a codec manifest.
// A manifest should include implementation source/build identity and all
// byte-affecting configuration; hashing configuration alone is insufficient.
func Fingerprint(manifest []byte) CodecFingerprint {
	sum := sha256.Sum256(manifest)
	return CodecFingerprint("sha256:" + hex.EncodeToString(sum[:]))
}

// CodecLimits are hard maxima advertised by a codec. The runtime checks the
// request before Decode and every returned byte slice after an encoder call.
// A codec must fail rather than return output above its own advertised limits.
type CodecLimits struct {
	MaxRequestBytes        int64
	MaxStreamFrameBytes    int64
	MaxStreamFramesPerStep int
	MaxAggregateBytes      int64
	MaxAdmissionErrorBytes int64
}

// CodecDescription is immutable for the lifetime of a registered codec.
//
// ID identifies the logical protocol. Version is an immutable, human-readable
// release identifier: an (ID, Version) pair must never be reused for different
// behavior. Fingerprint pins the exact byte-affecting implementation and
// configuration. Contract versions describe this Go port, not the wire API.
type CodecDescription struct {
	Contract         framework.Contract
	ID               CodecID
	Version          string
	Fingerprint      CodecFingerprint
	Limits           CodecLimits
	OverloadedStatus int
}

// RequiredCodecContract returns a fresh copy of HumanLLM's base requirements.
// Exact replay, explicit deterministic seeds, and both encoder modes are base
// major-version semantics rather than optional features.
func RequiredCodecContract() framework.Requirements {
	return framework.Requirements{ID: CodecContractID, Major: CodecContractMajor}
}

// NegotiateCodec validates a descriptor, negotiates its framework contract,
// and returns an independent frozen copy. Applications should do this exactly
// once during composition and persist the returned identity with every request.
func NegotiateCodec(description CodecDescription) (CodecDescription, error) {
	contract, err := framework.Negotiate(description.Contract, RequiredCodecContract())
	if err != nil {
		return CodecDescription{}, fmt.Errorf("%w: %w", ErrInvalidCodecContract, err)
	}
	if !codecIDPattern.MatchString(string(description.ID)) {
		return CodecDescription{}, fmt.Errorf("%w: invalid codec id %q", ErrInvalidCodecContract, description.ID)
	}
	if !validVersion(description.Version) {
		return CodecDescription{}, fmt.Errorf("%w: invalid codec version %q", ErrInvalidCodecContract, description.Version)
	}
	if !validFingerprint(description.Fingerprint) {
		return CodecDescription{}, fmt.Errorf("%w: invalid codec fingerprint", ErrInvalidCodecContract)
	}
	if err := validateLimits(description.Limits); err != nil {
		return CodecDescription{}, err
	}
	if !validAdmissionStatus(description.OverloadedStatus) {
		return CodecDescription{}, fmt.Errorf(
			"%w: overload status %d is not 429 or 5xx",
			ErrInvalidCodecContract, description.OverloadedStatus,
		)
	}
	description.Contract = contract
	return description, nil
}

// Codec is a pure wire projection port. Implementations are borrowed until
// Service.Done and must allow concurrent calls to every Codec method. They may
// parse and encode a custom model API, but must not perform transport,
// persistence, authentication, clock, random, or other external I/O through
// this interface. Description must always report the same value. Decode and
// AdmissionError must be deterministic functions of their arguments.
//
// Decode borrows body only for the duration of the call. It must neither mutate
// nor retain body, and the returned Request must own all mutable slices, maps,
// and RawMessage values and pass Request.Validate. AdmissionError transfers
// ownership of its returned body to the caller. Encoder factory arguments are
// likewise borrowed; an encoder must copy any mutable value it retains.
// A registered Codec is borrowed for the Service lifetime and every method may
// be called concurrently. Each returned Encoder, however, belongs to exactly
// one response and is driven serially; encoders need not support concurrent
// Start/Encode calls. Any method error fails the current request/event and must
// not hide externally visible side effects because codecs are pure.
type Codec interface {
	Description() CodecDescription
	Decode(body []byte) (Request, error)
	NewStream(session EncoderSession) (Encoder, error)
	NewAggregate(session EncoderSession) (Encoder, error)
	AdmissionError(failure AdmissionFailure) ([]byte, error)
}

// ValidateCodec negotiates a codec's descriptor. A typed-nil implementation is
// rejected before any method call. The returned descriptor, not later calls to
// Description, is the immutable value a runtime must cache and persist.
func ValidateCodec(codec Codec) (CodecDescription, error) {
	if isNilCodec(codec) {
		return CodecDescription{}, fmt.Errorf("%w: codec is nil", ErrInvalidCodecContract)
	}
	return NegotiateCodec(codec.Description())
}

// AdmissionFailure is a safe, already-classified pre-response failure. Message
// may be exposed to the caller; a codec must not inspect or reveal an underlying
// implementation error through AdmissionError.
type AdmissionFailure struct {
	Status  int
	Code    string
	Message string
}

// Validate checks the transport-neutral admission error input.
func (failure AdmissionFailure) Validate() error {
	if failure.Status < 400 || failure.Status > 599 {
		return fmt.Errorf("%w: admission status %d is not an error", ErrInvalidCodecContract, failure.Status)
	}
	if !validToken(failure.Code) {
		return fmt.Errorf("%w: invalid admission error code %q", ErrInvalidCodecContract, failure.Code)
	}
	if strings.TrimSpace(failure.Message) == "" {
		return fmt.Errorf("%w: admission error message is empty", ErrInvalidCodecContract)
	}
	if len(failure.Message) > 64<<10 {
		return fmt.Errorf("%w: admission error message is too large", ErrInvalidCodecContract)
	}
	return nil
}

// EncoderSession contains every per-response value that may influence encoded
// bytes. It must be durable before Start output becomes observable.
type EncoderSession struct {
	ResponseID string
	Model      string
	Seed       SessionSeed
}

// SessionSeed supplies deterministic clock and entropy inputs. Opaque is a
// codec-owned JSON value for additional deterministic inputs. The runtime owns
// and persists all three fields; a codec must never fill missing values from a
// clock or random source.
type SessionSeed struct {
	CreatedAtUnix int64           `json:"created_at_unix"`
	Entropy       []byte          `json:"entropy,omitempty"`
	Opaque        json.RawMessage `json:"opaque,omitempty"`

	ToolCallPolicy ToolCallPolicy `json:"tool_call_policy,omitempty"`
}

// Validate checks the mode-independent encoder inputs. A codec must additionally
// reject a missing seed field that its wire representation requires.
func (session EncoderSession) Validate() error {
	if !validStableValue(session.ResponseID, 512) {
		return fmt.Errorf("%w: response id is required", ErrInvalidCodecContract)
	}
	if !validStableValue(session.Model, 512) {
		return fmt.Errorf("%w: model is required", ErrInvalidCodecContract)
	}
	if session.Seed.CreatedAtUnix <= 0 {
		return fmt.Errorf("%w: positive created-at seed is required", ErrInvalidCodecContract)
	}
	if int64(len(session.Seed.Entropy))+int64(len(session.Seed.Opaque)) > maximumSeedBytes {
		return fmt.Errorf("%w: session seed is too large", ErrInvalidCodecContract)
	}
	switch session.Seed.ToolCallPolicy {
	case "", ToolCallsSerial, ToolCallsParallel:
	default:
		return fmt.Errorf("%w: unsupported tool-call policy %q", ErrInvalidCodecContract, session.Seed.ToolCallPolicy)
	}
	if len(session.Seed.Opaque) != 0 && !json.Valid(session.Seed.Opaque) {
		return fmt.Errorf("%w: session seed opaque value is not JSON", ErrInvalidCodecContract)
	}
	return nil
}

// EventSeed supplies every event-local clock, entropy, or codec-specific input
// that may affect observable bytes. It must be persisted with the event before
// those bytes become visible.
type EventSeed struct {
	EncodedAtUnix int64           `json:"encoded_at_unix"`
	Entropy       []byte          `json:"entropy,omitempty"`
	Opaque        json.RawMessage `json:"opaque,omitempty"`
}

// Validate checks the generic event seed representation.
func (seed EventSeed) Validate() error {
	if int64(len(seed.Entropy))+int64(len(seed.Opaque)) > maximumSeedBytes {
		return fmt.Errorf("%w: event seed is too large", ErrInvalidCodecContract)
	}
	if len(seed.Opaque) != 0 && !json.Valid(seed.Opaque) {
		return fmt.Errorf("%w: event seed opaque value is not JSON", ErrInvalidCodecContract)
	}
	return nil
}

// Encoder is a single-session deterministic state machine. A runtime creates a
// fresh instance per response and calls its methods sequentially: Start exactly
// once, then Encode in durable event order until done. It is not required to be
// concurrency-safe and must never be reused for another response.
//
// Start and Encode return newly owned buffers. After return, the caller may
// mutate or retain them and the encoder must never mutate them again. Inputs are
// borrowed for the call only. Given the same codec descriptor, EncoderSession,
// ordered Events, and EventSeeds, a reconstructed encoder must return byte-for-
// byte identical outputs without consulting a clock, random source, or I/O.
//
// For a stream encoder, Start and each Encode return zero or more frames. For
// an aggregate encoder, Start and non-terminal Encode calls return no bytes,
// and the terminal Encode returns exactly one body. Transport-level keepalive
// is deliberately outside this deterministic response projection.
type Encoder interface {
	Start() (frames [][]byte, err error)
	Encode(event Event, seed EventSeed) (frames [][]byte, done bool, err error)
}

// CheckRequestSize enforces a codec's advertised decode input limit.
func (limits CodecLimits) CheckRequestSize(size int64) error {
	if err := validateLimits(limits); err != nil {
		return err
	}
	if size < 0 || size > limits.MaxRequestBytes {
		return fmt.Errorf("%w: request bytes %d exceed limit %d", ErrInvalidCodecContract, size, limits.MaxRequestBytes)
	}
	return nil
}

// CheckStreamFrames enforces the advertised per-call stream limits. Buffer
// ownership is a behavioral requirement verified by codec conformance tests.
func (limits CodecLimits) CheckStreamFrames(frames [][]byte) error {
	if err := validateLimits(limits); err != nil {
		return err
	}
	if len(frames) > limits.MaxStreamFramesPerStep {
		return fmt.Errorf(
			"%w: stream emitted %d frames, limit %d",
			ErrInvalidCodecContract, len(frames), limits.MaxStreamFramesPerStep,
		)
	}
	for index, frame := range frames {
		if int64(len(frame)) > limits.MaxStreamFrameBytes {
			return fmt.Errorf(
				"%w: stream frame %d has %d bytes, limit %d",
				ErrInvalidCodecContract, index, len(frame), limits.MaxStreamFrameBytes,
			)
		}
	}
	return nil
}

// CheckAggregateFrames enforces aggregate visibility and size invariants.
func (limits CodecLimits) CheckAggregateFrames(frames [][]byte, done bool) error {
	if err := validateLimits(limits); err != nil {
		return err
	}
	if !done && len(frames) != 0 {
		return fmt.Errorf("%w: aggregate emitted bytes before terminal event", ErrInvalidCodecContract)
	}
	if done && len(frames) != 1 {
		return fmt.Errorf("%w: terminal aggregate emitted %d bodies, want 1", ErrInvalidCodecContract, len(frames))
	}
	if len(frames) == 1 && int64(len(frames[0])) > limits.MaxAggregateBytes {
		return fmt.Errorf(
			"%w: aggregate body has %d bytes, limit %d",
			ErrInvalidCodecContract, len(frames[0]), limits.MaxAggregateBytes,
		)
	}
	return nil
}

// CheckAdmissionError enforces the advertised admission-error body limit.
func (limits CodecLimits) CheckAdmissionError(body []byte) error {
	if err := validateLimits(limits); err != nil {
		return err
	}
	if int64(len(body)) > limits.MaxAdmissionErrorBytes {
		return fmt.Errorf(
			"%w: admission error has %d bytes, limit %d",
			ErrInvalidCodecContract, len(body), limits.MaxAdmissionErrorBytes,
		)
	}
	return nil
}

func validateLimits(limits CodecLimits) error {
	if limits.MaxRequestBytes <= 0 || limits.MaxStreamFrameBytes <= 0 ||
		limits.MaxStreamFramesPerStep <= 0 || limits.MaxAggregateBytes <= 0 ||
		limits.MaxAdmissionErrorBytes <= 0 {
		return fmt.Errorf("%w: every codec byte/count limit must be positive", ErrInvalidCodecContract)
	}
	if limits.MaxRequestBytes > maximumCodecBytes ||
		limits.MaxStreamFrameBytes > maximumCodecBytes ||
		limits.MaxAggregateBytes > maximumCodecBytes ||
		limits.MaxAdmissionErrorBytes > maximumCodecBytes ||
		limits.MaxStreamFramesPerStep > maximumFramesPerStep {
		return fmt.Errorf("%w: codec limit exceeds framework maximum", ErrInvalidCodecContract)
	}
	return nil
}

func validFingerprint(fingerprint CodecFingerprint) bool {
	const prefix = "sha256:"
	value := string(fingerprint)
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+sha256.Size*2 {
		return false
	}
	digest := value[len(prefix):]
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == sha256.Size && digest == strings.ToLower(digest)
}

func validVersion(version string) bool {
	if version == "" || version != strings.TrimSpace(version) || len(version) > 128 {
		return false
	}
	for _, character := range version {
		if character < 0x21 || character == 0x7f {
			return false
		}
	}
	return true
}

func validAdmissionStatus(status int) bool {
	return status == 429 || status >= 500 && status <= 599
}

func validToken(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func validStableValue(value string, maximum int) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func isNilCodec(codec Codec) bool {
	if codec == nil {
		return true
	}
	value := reflect.ValueOf(codec)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
