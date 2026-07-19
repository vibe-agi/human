// Package protect defines the provider-neutral payload protection port used by
// Human runtimes. Authentication and API-token hashing are separate concerns.
package protect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/vibe-agi/human/framework"
)

const (
	ContractID       framework.ContractID = "human.protector"
	ContractMajor    uint16               = 1
	maximumDataBytes                      = 128 << 20
)

var (
	ErrInvalidBinding      = errors.New("invalid protection binding")
	ErrInvalidEnvelope     = errors.New("invalid protection envelope")
	ErrAuthentication      = errors.New("protected payload authentication failed")
	ErrKeyUnavailable      = errors.New("protection key is unavailable")
	ErrProviderUnavailable = errors.New("protection provider is temporarily unavailable")
)

var tokenPattern = regexp.MustCompile(`^[a-z][a-z0-9._/-]{0,127}$`)

// Binding is canonical associated data supplied by the Human core. A
// Protector must cryptographically bind every field to the ciphertext. Moving
// an Envelope between authorities, records, fields, schemas, or purposes must
// make Open fail authentication.
type Binding struct {
	Component  string `json:"component"`
	Purpose    string `json:"purpose"`
	Authority  string `json:"authority"`
	Namespace  string `json:"namespace"`
	RecordType string `json:"record_type"`
	RecordID   string `json:"record_id"`
	Field      string `json:"field"`
	Schema     uint32 `json:"schema"`
}

// AAD returns the stable JSON encoding that conformance adapters use as
// associated data. It first validates the complete binding.
func (binding Binding) AAD() ([]byte, error) {
	if err := ValidateBinding(binding); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(binding)
	if err != nil {
		return nil, fmt.Errorf("%w: encode canonical binding: %v", ErrInvalidBinding, err)
	}
	return encoded, nil
}

// Envelope is self-describing encrypted payload state. Provider and Format
// identify the adapter/wire format; KeyID and KeyVersion select old keys after
// rotation. Nonce and Data are opaque bytes and must be defensively copied by
// both callers and implementations.
type Envelope struct {
	Provider   string `json:"provider"`
	Format     string `json:"format"`
	KeyID      string `json:"key_id"`
	KeyVersion string `json:"key_version"`
	Nonce      []byte `json:"nonce,omitempty"`
	Data       []byte `json:"data"`
}

// Description is frozen at runtime construction. MaxPlaintextBytes and
// MaxEnvelopeBytes are hard admission limits, not hints.
type Description struct {
	Contract          framework.Contract
	Provider          string
	Format            string
	MaxPlaintextBytes int64
	MaxEnvelopeBytes  int64
}

// Protector seals and opens payloads. Implementations must not retain input or
// return buffers. KMS/network work belongs outside Store transaction callbacks.
type Protector interface {
	Describe(context.Context) (Description, error)
	Seal(context.Context, Binding, []byte) (Envelope, error)
	Open(context.Context, Binding, Envelope) ([]byte, error)
}

// Rewrapper optionally rotates an Envelope without changing its plaintext
// identity. The returned envelope must still open under the exact same Binding.
type Rewrapper interface {
	Rewrap(context.Context, Binding, Envelope) (Envelope, error)
}

// Requirements returns the immutable base contract required by Human cores.
func Requirements() framework.Requirements {
	return framework.Requirements{ID: ContractID, Major: ContractMajor}
}

func ValidateBinding(binding Binding) error {
	for label, value := range map[string]string{
		"component":   binding.Component,
		"purpose":     binding.Purpose,
		"record type": binding.RecordType,
		"field":       binding.Field,
	} {
		if !tokenPattern.MatchString(value) {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidBinding, label)
		}
	}
	for label, value := range map[string]string{
		"authority": binding.Authority,
		"namespace": binding.Namespace,
		"record id": binding.RecordID,
	} {
		if value == "" || len(value) > 512 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidBinding, label)
		}
	}
	if binding.Schema == 0 {
		return fmt.Errorf("%w: schema must be positive", ErrInvalidBinding)
	}
	return nil
}

func ValidateDescription(description Description) error {
	if _, err := framework.Negotiate(description.Contract, Requirements()); err != nil {
		return err
	}
	if !tokenPattern.MatchString(description.Provider) || !tokenPattern.MatchString(description.Format) {
		return errors.New("protect: provider and format must be stable lowercase identifiers")
	}
	if description.MaxPlaintextBytes < 1 || description.MaxPlaintextBytes > maximumDataBytes ||
		description.MaxEnvelopeBytes < description.MaxPlaintextBytes || description.MaxEnvelopeBytes > maximumDataBytes {
		return errors.New("protect: invalid plaintext or envelope byte limit")
	}
	return nil
}

func ValidateEnvelope(envelope Envelope) error {
	if !tokenPattern.MatchString(envelope.Provider) || !tokenPattern.MatchString(envelope.Format) {
		return fmt.Errorf("%w: provider or format is invalid", ErrInvalidEnvelope)
	}
	for label, value := range map[string]string{
		"key id":      envelope.KeyID,
		"key version": envelope.KeyVersion,
	} {
		if value == "" || len(value) > 512 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidEnvelope, label)
		}
	}
	if len(envelope.Nonce) > 1024 || len(envelope.Data) == 0 || len(envelope.Data) > maximumDataBytes {
		return fmt.Errorf("%w: nonce or ciphertext size is invalid", ErrInvalidEnvelope)
	}
	return nil
}

// CloneEnvelope returns a deep copy safe to retain across adapter boundaries.
func CloneEnvelope(envelope Envelope) Envelope {
	envelope.Nonce = append([]byte(nil), envelope.Nonce...)
	envelope.Data = append([]byte(nil), envelope.Data...)
	return envelope
}
