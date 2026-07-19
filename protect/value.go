package protect

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	StoredValueVersion  uint16 = 1
	MaxStoredValueBytes int64  = MaxStoredEnvelopeBytes + (1 << 20)
)

var ErrInvalidStoredValue = errors.New("invalid stored protected value")

type StoredValueMode string

const (
	StoredValuePlain  StoredValueMode = "plain"
	StoredValueSealed StoredValueMode = "sealed"
)

// StoredValue is the logical, version-independent representation of one opaque
// persistence field. Mode is always explicit: runtimes never guess protection
// from a magic byte prefix. Nil preserves nil versus non-nil empty plaintext.
// Plain and Envelope are caller-owned and must be defensively copied when
// retained across an adapter boundary.
type StoredValue struct {
	Mode     StoredValueMode
	Nil      bool
	Plain    []byte
	Envelope *Envelope
}

// NewPlainStoredValue copies plaintext into an explicitly unprotected value.
// It exists so deployments can turn protection on without making old plain
// records ambiguous or unreadable.
func NewPlainStoredValue(plaintext []byte) StoredValue {
	return StoredValue{
		Mode: StoredValuePlain, Nil: plaintext == nil,
		Plain: appendExact(plaintext),
	}
}

// NewSealedStoredValue copies a validated Envelope and records whether the
// authenticated plaintext was nil. Opened plaintext must match that marker.
func NewSealedStoredValue(plaintextNil bool, envelope Envelope) (StoredValue, error) {
	if err := ValidateEnvelope(envelope); err != nil {
		return StoredValue{}, fmt.Errorf("%w: sealed envelope is invalid", ErrInvalidStoredValue)
	}
	copyOfEnvelope := CloneEnvelope(envelope)
	return StoredValue{Mode: StoredValueSealed, Nil: plaintextNil, Envelope: &copyOfEnvelope}, nil
}

// ValidateStoredValue checks the logical mode/shape without performing any
// cryptographic operation.
func ValidateStoredValue(value StoredValue) error {
	switch value.Mode {
	case StoredValuePlain:
		if value.Envelope != nil || value.Nil != (value.Plain == nil) || len(value.Plain) > maximumDataBytes {
			return fmt.Errorf("%w: plain value shape is invalid", ErrInvalidStoredValue)
		}
	case StoredValueSealed:
		if value.Plain != nil || value.Envelope == nil {
			return fmt.Errorf("%w: sealed value shape is invalid", ErrInvalidStoredValue)
		}
		if err := ValidateEnvelope(*value.Envelope); err != nil {
			return fmt.Errorf("%w: sealed envelope is invalid", ErrInvalidStoredValue)
		}
	default:
		return fmt.Errorf("%w: mode is unsupported", ErrInvalidStoredValue)
	}
	return nil
}

// CloneStoredValue returns a deep copy safe to retain across Store and
// Protector boundaries.
func CloneStoredValue(value StoredValue) StoredValue {
	value.Plain = appendExact(value.Plain)
	if value.Envelope != nil {
		envelope := CloneEnvelope(*value.Envelope)
		value.Envelope = &envelope
	}
	return value
}

// MarshalStoredValue encodes a deterministic, versioned persistence frame.
// Call it after Seal and before entering Store.Update; the Store callback only
// needs to persist the returned opaque bytes.
func MarshalStoredValue(value StoredValue) ([]byte, error) {
	if err := ValidateStoredValue(value); err != nil {
		return nil, err
	}
	wire := storedValueV1{
		Version: StoredValueVersion, Mode: value.Mode, Nil: value.Nil,
		Plain: base64.StdEncoding.EncodeToString(value.Plain), Envelope: json.RawMessage("null"),
	}
	if value.Mode == StoredValueSealed {
		encodedEnvelope, err := MarshalEnvelope(*value.Envelope)
		if err != nil {
			return nil, fmt.Errorf("%w: encode sealed envelope", ErrInvalidStoredValue)
		}
		wire.Envelope = encodedEnvelope
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("%w: encode framing", ErrInvalidStoredValue)
	}
	if int64(len(encoded)) > MaxStoredValueBytes {
		return nil, fmt.Errorf("%w: encoded framing exceeds hard limit", ErrInvalidStoredValue)
	}
	return encoded, nil
}

// UnmarshalStoredValue strictly decodes MarshalStoredValue output. It performs
// no Open operation; callers leave Store.View with these copied bytes, decode
// them, and only then call Protector.Open.
func UnmarshalStoredValue(encoded []byte) (StoredValue, error) {
	return UnmarshalStoredValueLimited(encoded, MaxStoredValueBytes)
}

// UnmarshalStoredValueLimited applies a caller-selected admission limit no
// larger than MaxStoredValueBytes.
func UnmarshalStoredValueLimited(encoded []byte, maxBytes int64) (StoredValue, error) {
	if maxBytes < 1 || maxBytes > MaxStoredValueBytes {
		return StoredValue{}, fmt.Errorf("%w: decode limit is out of range", ErrInvalidStoredValue)
	}
	if len(encoded) == 0 || int64(len(encoded)) > maxBytes {
		return StoredValue{}, fmt.Errorf("%w: encoded framing exceeds admission limit", ErrInvalidStoredValue)
	}
	if !utf8.Valid(encoded) {
		return StoredValue{}, fmt.Errorf("%w: framing is not valid UTF-8", ErrInvalidStoredValue)
	}
	wire, err := decodeStoredValueV1(encoded)
	if err != nil {
		return StoredValue{}, err
	}
	if wire.Version != StoredValueVersion {
		return StoredValue{}, fmt.Errorf("%w: unsupported framing version", ErrInvalidStoredValue)
	}

	var value StoredValue
	switch wire.Mode {
	case StoredValuePlain:
		if !bytes.Equal(bytes.TrimSpace(wire.Envelope), []byte("null")) {
			return StoredValue{}, fmt.Errorf("%w: plain value carries an envelope", ErrInvalidStoredValue)
		}
		plain, err := decodeCanonicalValueBytes(wire.Plain, wire.Nil)
		if err != nil {
			return StoredValue{}, err
		}
		value = StoredValue{Mode: StoredValuePlain, Nil: wire.Nil, Plain: plain}
	case StoredValueSealed:
		if wire.Plain != "" || bytes.Equal(bytes.TrimSpace(wire.Envelope), []byte("null")) {
			return StoredValue{}, fmt.Errorf("%w: sealed value shape is invalid", ErrInvalidStoredValue)
		}
		envelope, err := UnmarshalEnvelopeLimited(wire.Envelope, min(int64(len(wire.Envelope)), MaxStoredEnvelopeBytes))
		if err != nil {
			return StoredValue{}, fmt.Errorf("%w: sealed envelope framing is invalid", ErrInvalidStoredValue)
		}
		value = StoredValue{Mode: StoredValueSealed, Nil: wire.Nil, Envelope: &envelope}
	default:
		return StoredValue{}, fmt.Errorf("%w: mode is unsupported", ErrInvalidStoredValue)
	}
	if err := ValidateStoredValue(value); err != nil {
		return StoredValue{}, err
	}
	return value, nil
}

type storedValueV1 struct {
	Version  uint16          `json:"version"`
	Mode     StoredValueMode `json:"mode"`
	Nil      bool            `json:"nil"`
	Plain    string          `json:"plain"`
	Envelope json.RawMessage `json:"envelope"`
}

func decodeStoredValueV1(encoded []byte) (storedValueV1, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return storedValueV1{}, fmt.Errorf("%w: framing must be one object", ErrInvalidStoredValue)
	}
	var wire storedValueV1
	seen := make(map[string]bool, 5)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return storedValueV1{}, fmt.Errorf("%w: decode field name", ErrInvalidStoredValue)
		}
		key, ok := keyToken.(string)
		if !ok {
			return storedValueV1{}, fmt.Errorf("%w: field name is not a string", ErrInvalidStoredValue)
		}
		if seen[key] {
			return storedValueV1{}, fmt.Errorf("%w: duplicate field", ErrInvalidStoredValue)
		}
		seen[key] = true
		var decodeErr error
		switch key {
		case "version":
			decodeErr = decoder.Decode(&wire.Version)
		case "mode":
			decodeErr = decoder.Decode(&wire.Mode)
		case "nil":
			decodeErr = decoder.Decode(&wire.Nil)
		case "plain":
			decodeErr = decoder.Decode(&wire.Plain)
		case "envelope":
			decodeErr = decoder.Decode(&wire.Envelope)
		default:
			return storedValueV1{}, fmt.Errorf("%w: unknown field", ErrInvalidStoredValue)
		}
		if decodeErr != nil {
			return storedValueV1{}, fmt.Errorf("%w: invalid field value", ErrInvalidStoredValue)
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return storedValueV1{}, fmt.Errorf("%w: framing object is incomplete", ErrInvalidStoredValue)
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return storedValueV1{}, fmt.Errorf("%w: trailing data", ErrInvalidStoredValue)
	}
	for _, required := range []string{"version", "mode", "nil", "plain", "envelope"} {
		if !seen[required] {
			return storedValueV1{}, fmt.Errorf("%w: required field is missing", ErrInvalidStoredValue)
		}
	}
	return wire, nil
}

func decodeCanonicalValueBytes(encoded string, isNil bool) ([]byte, error) {
	if isNil {
		if encoded != "" {
			return nil, fmt.Errorf("%w: nil plain value carries bytes", ErrInvalidStoredValue)
		}
		return nil, nil
	}
	length := base64.StdEncoding.DecodedLen(len(encoded))
	decoded := make([]byte, length)
	written, err := base64.StdEncoding.Strict().Decode(decoded, []byte(encoded))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid plain byte encoding", ErrInvalidStoredValue)
	}
	decoded = decoded[:written]
	if base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, fmt.Errorf("%w: plain byte encoding is not canonical", ErrInvalidStoredValue)
	}
	return decoded, nil
}

func appendExact(value []byte) []byte {
	if value == nil {
		return nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result
}
