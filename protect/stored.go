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
	// StoredEnvelopeVersion is the strict, provider-neutral persistence framing
	// version emitted by MarshalEnvelope.
	StoredEnvelopeVersion uint16 = 1
	// MaxStoredEnvelopeBytes bounds the base64-expanded JSON representation of a
	// maximum-size Envelope plus its metadata. Providers normally impose a much
	// smaller Description.MaxEnvelopeBytes before this framing is reached.
	MaxStoredEnvelopeBytes int64 = 172 << 20
)

// ErrInvalidStoredEnvelope reports malformed, unsupported, ambiguous, or
// oversized persistence framing. It is distinct from ErrAuthentication: no
// cryptographic interpretation has happened yet.
var ErrInvalidStoredEnvelope = errors.New("invalid stored protection envelope")

// MarshalEnvelope emits deterministic, versioned persistence bytes. The
// explicit nil flags preserve nil versus non-nil empty byte slices exactly.
//
// Human cores call Seal and MarshalEnvelope before entering a Store.Update
// callback, then store only the returned opaque bytes inside the callback. They
// copy those bytes from Store.View and call UnmarshalEnvelope and Open only
// after the callback returns. This keeps remote KMS/provider I/O outside Store
// transactions while leaving custom Store implementations encryption-agnostic.
func MarshalEnvelope(envelope Envelope) ([]byte, error) {
	if err := ValidateEnvelope(envelope); err != nil {
		return nil, fmt.Errorf("%w: envelope validation failed", ErrInvalidStoredEnvelope)
	}
	wire := storedEnvelopeV1{
		Version:  StoredEnvelopeVersion,
		Provider: envelope.Provider, Format: envelope.Format,
		KeyID: envelope.KeyID, KeyVersion: envelope.KeyVersion,
		NonceNil: envelope.Nonce == nil, Nonce: base64.StdEncoding.EncodeToString(envelope.Nonce),
		DataNil: envelope.Data == nil, Data: base64.StdEncoding.EncodeToString(envelope.Data),
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("%w: encode framing", ErrInvalidStoredEnvelope)
	}
	if int64(len(encoded)) > MaxStoredEnvelopeBytes {
		return nil, fmt.Errorf("%w: encoded framing exceeds hard limit", ErrInvalidStoredEnvelope)
	}
	return encoded, nil
}

// UnmarshalEnvelope strictly decodes MarshalEnvelope output. Unknown or
// duplicate fields, unsupported versions, trailing values, inconsistent
// nil/empty markers, and oversized inputs are rejected fail-closed.
func UnmarshalEnvelope(encoded []byte) (Envelope, error) {
	return UnmarshalEnvelopeLimited(encoded, MaxStoredEnvelopeBytes)
}

// UnmarshalEnvelopeLimited applies a caller-selected admission limit no larger
// than MaxStoredEnvelopeBytes. It is useful when a runtime's negotiated
// Description is substantially smaller than the framework hard limit.
func UnmarshalEnvelopeLimited(encoded []byte, maxBytes int64) (Envelope, error) {
	if maxBytes < 1 || maxBytes > MaxStoredEnvelopeBytes {
		return Envelope{}, fmt.Errorf("%w: decode limit is out of range", ErrInvalidStoredEnvelope)
	}
	if len(encoded) == 0 || int64(len(encoded)) > maxBytes {
		return Envelope{}, fmt.Errorf("%w: encoded framing exceeds admission limit", ErrInvalidStoredEnvelope)
	}
	if !utf8.Valid(encoded) {
		return Envelope{}, fmt.Errorf("%w: framing is not valid UTF-8", ErrInvalidStoredEnvelope)
	}
	wire, err := decodeStoredEnvelopeV1(encoded)
	if err != nil {
		return Envelope{}, err
	}
	if wire.Version != StoredEnvelopeVersion {
		return Envelope{}, fmt.Errorf("%w: unsupported framing version", ErrInvalidStoredEnvelope)
	}
	nonce, err := decodeStoredBytes(wire.Nonce, wire.NonceNil)
	if err != nil {
		return Envelope{}, err
	}
	data, err := decodeStoredBytes(wire.Data, wire.DataNil)
	if err != nil {
		return Envelope{}, err
	}
	envelope := Envelope{
		Provider: wire.Provider, Format: wire.Format,
		KeyID: wire.KeyID, KeyVersion: wire.KeyVersion,
		Nonce: nonce, Data: data,
	}
	if err := ValidateEnvelope(envelope); err != nil {
		return Envelope{}, fmt.Errorf("%w: decoded envelope validation failed", ErrInvalidStoredEnvelope)
	}
	return envelope, nil
}

type storedEnvelopeV1 struct {
	Version    uint16 `json:"version"`
	Provider   string `json:"provider"`
	Format     string `json:"format"`
	KeyID      string `json:"key_id"`
	KeyVersion string `json:"key_version"`
	NonceNil   bool   `json:"nonce_nil"`
	Nonce      string `json:"nonce"`
	DataNil    bool   `json:"data_nil"`
	Data       string `json:"data"`
}

func decodeStoredEnvelopeV1(encoded []byte) (storedEnvelopeV1, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return storedEnvelopeV1{}, fmt.Errorf("%w: framing must be one object", ErrInvalidStoredEnvelope)
	}
	var wire storedEnvelopeV1
	seen := make(map[string]bool, 9)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return storedEnvelopeV1{}, fmt.Errorf("%w: decode field name", ErrInvalidStoredEnvelope)
		}
		key, ok := keyToken.(string)
		if !ok {
			return storedEnvelopeV1{}, fmt.Errorf("%w: field name is not a string", ErrInvalidStoredEnvelope)
		}
		if seen[key] {
			return storedEnvelopeV1{}, fmt.Errorf("%w: duplicate field", ErrInvalidStoredEnvelope)
		}
		seen[key] = true
		var decodeErr error
		switch key {
		case "version":
			decodeErr = decoder.Decode(&wire.Version)
		case "provider":
			decodeErr = decoder.Decode(&wire.Provider)
		case "format":
			decodeErr = decoder.Decode(&wire.Format)
		case "key_id":
			decodeErr = decoder.Decode(&wire.KeyID)
		case "key_version":
			decodeErr = decoder.Decode(&wire.KeyVersion)
		case "nonce_nil":
			decodeErr = decoder.Decode(&wire.NonceNil)
		case "nonce":
			decodeErr = decoder.Decode(&wire.Nonce)
		case "data_nil":
			decodeErr = decoder.Decode(&wire.DataNil)
		case "data":
			decodeErr = decoder.Decode(&wire.Data)
		default:
			return storedEnvelopeV1{}, fmt.Errorf("%w: unknown field", ErrInvalidStoredEnvelope)
		}
		if decodeErr != nil {
			return storedEnvelopeV1{}, fmt.Errorf("%w: invalid field value", ErrInvalidStoredEnvelope)
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return storedEnvelopeV1{}, fmt.Errorf("%w: framing object is incomplete", ErrInvalidStoredEnvelope)
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return storedEnvelopeV1{}, fmt.Errorf("%w: trailing data", ErrInvalidStoredEnvelope)
	}
	for _, required := range []string{
		"version", "provider", "format", "key_id", "key_version",
		"nonce_nil", "nonce", "data_nil", "data",
	} {
		if !seen[required] {
			return storedEnvelopeV1{}, fmt.Errorf("%w: required field is missing", ErrInvalidStoredEnvelope)
		}
	}
	return wire, nil
}

func decodeStoredBytes(encoded string, isNil bool) ([]byte, error) {
	if isNil {
		if encoded != "" {
			return nil, fmt.Errorf("%w: nil marker carries bytes", ErrInvalidStoredEnvelope)
		}
		return nil, nil
	}
	length := base64.StdEncoding.DecodedLen(len(encoded))
	decoded := make([]byte, length)
	written, err := base64.StdEncoding.Strict().Decode(decoded, []byte(encoded))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid byte encoding", ErrInvalidStoredEnvelope)
	}
	decoded = decoded[:written]
	// encoding/base64 deliberately ignores CR and LF even in Strict mode.
	// Re-encoding closes that malleability and admits only the one spelling
	// emitted by MarshalEnvelope.
	if base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, fmt.Errorf("%w: byte encoding is not canonical", ErrInvalidStoredEnvelope)
	}
	return decoded, nil
}
