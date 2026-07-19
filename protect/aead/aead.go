// Package aead provides Human's local, in-memory AES-256-GCM Protector.
//
// It is the convenient single-process default, not a key-management system.
// Key material is supplied by the application, copied into locked lifecycle
// state, never persisted by this package, and wiped from that copy on release.
// Applications that use a KMS or HSM should implement protect.Protector instead.
package aead

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/protect"
)

const (
	// Provider and Format are persisted in every envelope. They are constants so
	// an application cannot accidentally reinterpret ciphertext under another
	// implementation after a configuration change.
	Provider = "human.local-aead"
	Format   = "aes-256-gcm-v1"

	KeySize                  = 32
	defaultMaxPlaintextBytes = int64(16 << 20)
	maximumEnvelopeBytes     = int64(128 << 20)
	nonceBytes               = 12
	tagBytes                 = 16
	framingBytes             = 1
	maximumPlaintextBytes    = maximumEnvelopeBytes - nonceBytes - tagBytes - framingBytes
)

var (
	ErrInvalidConfig = errors.New("local AEAD Protector configuration is invalid")
	ErrClosed        = errors.New("local AEAD Protector is closed")
)

// KeyRef is the durable identity of one key version. Both fields are written to
// Envelope metadata; changing either field without re-encryption invalidates
// authentication even when two references happen to use identical material.
type KeyRef struct {
	ID      string
	Version string
}

// Key supplies one AES-256 key version. Material must contain exactly 32 bytes.
// Open copies Material before returning, so callers may wipe or reuse Config
// after construction. IDs and versions are operational metadata, never secrets.
type Key struct {
	ID       string
	Version  string
	Material []byte
}

// Config defines a local versioned keyring. Keys may contain old versions for
// reads; Active selects the only version used by Seal and Rewrap. Key material
// remains application configuration and is never written into an Envelope.
type Config struct {
	Active            KeyRef
	Keys              []Key
	MaxPlaintextBytes int64
}

// Open copies and validates the complete keyring and returns it as an owned
// resource. ctx bounds construction only. Reconstructing Open with the same
// versions can open already-persisted envelopes; adding a new Active version
// rotates writes without invalidating reads of old versions.
func Open(ctx context.Context, config Config) (protect.Resource, error) {
	if ctx == nil {
		return protect.Resource{}, fmt.Errorf("%w: context is required", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return protect.Resource{}, err
	}
	resolved, err := resolve(config)
	if err != nil {
		return protect.Resource{}, err
	}
	resource, err := framework.Own[protect.Protector](resolved, func(context.Context) error {
		resolved.close()
		return nil
	})
	if err != nil {
		resolved.close()
		return protect.Resource{}, fmt.Errorf("own local AEAD Protector resource: %w", err)
	}
	return resource, nil
}

type keyring struct {
	mu          sync.RWMutex
	closed      bool
	active      KeyRef
	keys        map[KeyRef][]byte
	description protect.Description
}

func resolve(config Config) (*keyring, error) {
	maximum := config.MaxPlaintextBytes
	if maximum == 0 {
		maximum = defaultMaxPlaintextBytes
	}
	if maximum < 1 || maximum > maximumPlaintextBytes {
		return nil, fmt.Errorf("%w: plaintext byte limit is out of range", ErrInvalidConfig)
	}
	if err := validateRef(config.Active); err != nil {
		return nil, err
	}
	if len(config.Keys) == 0 {
		return nil, fmt.Errorf("%w: at least one key version is required", ErrInvalidConfig)
	}

	keys := make(map[KeyRef][]byte, len(config.Keys))
	wipeKeys := true
	defer func() {
		if wipeKeys {
			for _, material := range keys {
				wipe(material)
			}
		}
	}()
	for index, configured := range config.Keys {
		ref := KeyRef{ID: configured.ID, Version: configured.Version}
		if err := validateRef(ref); err != nil {
			return nil, fmt.Errorf("%w: key entry %d has invalid metadata", ErrInvalidConfig, index)
		}
		if len(configured.Material) != KeySize {
			return nil, fmt.Errorf("%w: key entry %d must contain 32 bytes", ErrInvalidConfig, index)
		}
		if _, duplicate := keys[ref]; duplicate {
			return nil, fmt.Errorf("%w: duplicate key reference", ErrInvalidConfig)
		}
		keys[ref] = append([]byte(nil), configured.Material...)
	}
	if _, exists := keys[config.Active]; !exists {
		return nil, fmt.Errorf("%w: active key reference is unavailable", ErrInvalidConfig)
	}

	description := protect.Description{
		Contract:          framework.Contract{ID: protect.ContractID, Major: protect.ContractMajor},
		Provider:          Provider,
		Format:            Format,
		MaxPlaintextBytes: maximum,
		MaxEnvelopeBytes:  maximum + nonceBytes + tagBytes + framingBytes,
	}
	if err := protect.ValidateDescription(description); err != nil {
		return nil, fmt.Errorf("%w: description: %v", ErrInvalidConfig, err)
	}
	wipeKeys = false
	return &keyring{active: config.Active, keys: keys, description: description}, nil
}

func validateRef(ref KeyRef) error {
	probe := protect.Envelope{
		Provider: Provider, Format: Format,
		KeyID: ref.ID, KeyVersion: ref.Version,
		Nonce: make([]byte, nonceBytes), Data: make([]byte, tagBytes+framingBytes),
	}
	if err := protect.ValidateEnvelope(probe); err != nil {
		return fmt.Errorf("%w: key reference metadata is invalid", ErrInvalidConfig)
	}
	return nil
}

func (keyring *keyring) Describe(ctx context.Context) (protect.Description, error) {
	if err := contextError(ctx); err != nil {
		return protect.Description{}, err
	}
	keyring.mu.RLock()
	defer keyring.mu.RUnlock()
	if keyring.closed {
		return protect.Description{}, ErrClosed
	}
	return keyring.description, nil
}

func (keyring *keyring) Seal(ctx context.Context, binding protect.Binding, plaintext []byte) (protect.Envelope, error) {
	if err := contextError(ctx); err != nil {
		return protect.Envelope{}, err
	}
	if _, err := binding.AAD(); err != nil {
		return protect.Envelope{}, err
	}
	if int64(len(plaintext)) > keyring.maxPlaintextBytes() {
		return protect.Envelope{}, fmt.Errorf("%w: plaintext exceeds configured limit", protect.ErrInvalidEnvelope)
	}

	active, material, err := keyring.activeMaterial()
	if err != nil {
		return protect.Envelope{}, err
	}
	defer wipe(material)
	primitive, err := newAEAD(material)
	if err != nil {
		return protect.Envelope{}, err
	}
	nonce := make([]byte, primitive.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return protect.Envelope{}, fmt.Errorf("%w: random nonce generation failed", protect.ErrProviderUnavailable)
	}
	if err := contextError(ctx); err != nil {
		return protect.Envelope{}, err
	}

	framed := make([]byte, framingBytes+len(plaintext))
	if plaintext == nil {
		framed[0] = 0
	} else {
		framed[0] = 1
		copy(framed[1:], plaintext)
	}
	defer wipe(framed)
	aad, err := associatedData(binding, active)
	if err != nil {
		return protect.Envelope{}, err
	}
	ciphertext := primitive.Seal(nil, nonce, framed, aad)
	return protect.Envelope{
		Provider: Provider, Format: Format,
		KeyID: active.ID, KeyVersion: active.Version,
		Nonce: nonce, Data: ciphertext,
	}, nil
}

func (keyring *keyring) Open(ctx context.Context, binding protect.Binding, envelope protect.Envelope) ([]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if _, err := binding.AAD(); err != nil {
		return nil, err
	}
	copyOfEnvelope := protect.CloneEnvelope(envelope)
	if err := protect.ValidateEnvelope(copyOfEnvelope); err != nil {
		return nil, err
	}
	if copyOfEnvelope.Provider != Provider || copyOfEnvelope.Format != Format {
		return nil, fmt.Errorf("%w: provider or format does not match adapter", protect.ErrInvalidEnvelope)
	}
	if int64(len(copyOfEnvelope.Nonce)+len(copyOfEnvelope.Data)) > keyring.maxEnvelopeBytes() {
		return nil, fmt.Errorf("%w: ciphertext exceeds configured limit", protect.ErrInvalidEnvelope)
	}
	if len(copyOfEnvelope.Nonce) != nonceBytes || len(copyOfEnvelope.Data) < tagBytes+framingBytes {
		return nil, fmt.Errorf("%w: nonce or ciphertext shape is invalid", protect.ErrInvalidEnvelope)
	}

	ref := KeyRef{ID: copyOfEnvelope.KeyID, Version: copyOfEnvelope.KeyVersion}
	material, err := keyring.material(ref)
	if err != nil {
		return nil, err
	}
	defer wipe(material)
	primitive, err := newAEAD(material)
	if err != nil {
		return nil, err
	}
	aad, err := associatedData(binding, ref)
	if err != nil {
		return nil, err
	}
	framed, err := primitive.Open(nil, copyOfEnvelope.Nonce, copyOfEnvelope.Data, aad)
	if err != nil {
		return nil, protect.ErrAuthentication
	}
	defer wipe(framed)
	if len(framed) < framingBytes || int64(len(framed)-framingBytes) > keyring.maxPlaintextBytes() {
		return nil, fmt.Errorf("%w: authenticated payload framing is invalid", protect.ErrInvalidEnvelope)
	}
	switch framed[0] {
	case 0:
		if len(framed) != framingBytes {
			return nil, fmt.Errorf("%w: authenticated nil payload framing is invalid", protect.ErrInvalidEnvelope)
		}
		return nil, nil
	case 1:
		plaintext := make([]byte, len(framed)-framingBytes)
		copy(plaintext, framed[framingBytes:])
		return plaintext, nil
	default:
		return nil, fmt.Errorf("%w: authenticated payload marker is invalid", protect.ErrInvalidEnvelope)
	}
}

// Rewrap authenticates and decrypts an existing envelope, then seals it with
// the current Active key. It never persists either plaintext or key material.
func (keyring *keyring) Rewrap(ctx context.Context, binding protect.Binding, envelope protect.Envelope) (protect.Envelope, error) {
	plaintext, err := keyring.Open(ctx, binding, envelope)
	if err != nil {
		return protect.Envelope{}, err
	}
	defer wipe(plaintext)
	return keyring.Seal(ctx, binding, plaintext)
}

func (keyring *keyring) activeMaterial() (KeyRef, []byte, error) {
	keyring.mu.RLock()
	defer keyring.mu.RUnlock()
	if keyring.closed {
		return KeyRef{}, nil, ErrClosed
	}
	return keyring.active, append([]byte(nil), keyring.keys[keyring.active]...), nil
}

func (keyring *keyring) material(ref KeyRef) ([]byte, error) {
	keyring.mu.RLock()
	defer keyring.mu.RUnlock()
	if keyring.closed {
		return nil, ErrClosed
	}
	material, exists := keyring.keys[ref]
	if !exists {
		return nil, protect.ErrKeyUnavailable
	}
	return append([]byte(nil), material...), nil
}

func (keyring *keyring) maxPlaintextBytes() int64 {
	keyring.mu.RLock()
	defer keyring.mu.RUnlock()
	return keyring.description.MaxPlaintextBytes
}

func (keyring *keyring) maxEnvelopeBytes() int64 {
	keyring.mu.RLock()
	defer keyring.mu.RUnlock()
	return keyring.description.MaxEnvelopeBytes
}

func (keyring *keyring) close() {
	keyring.mu.Lock()
	defer keyring.mu.Unlock()
	if keyring.closed {
		return
	}
	keyring.closed = true
	for ref, material := range keyring.keys {
		wipe(material)
		delete(keyring.keys, ref)
	}
	keyring.active = KeyRef{}
}

func newAEAD(material []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(material)
	if err != nil {
		return nil, fmt.Errorf("%w: initialize AES-256", protect.ErrProviderUnavailable)
	}
	primitive, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: initialize GCM", protect.ErrProviderUnavailable)
	}
	return primitive, nil
}

type aadDocument struct {
	Provider   string          `json:"provider"`
	Format     string          `json:"format"`
	KeyID      string          `json:"key_id"`
	KeyVersion string          `json:"key_version"`
	Binding    json.RawMessage `json:"binding"`
}

func associatedData(binding protect.Binding, ref KeyRef) ([]byte, error) {
	bindingAAD, err := binding.AAD()
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(aadDocument{
		Provider: Provider, Format: Format,
		KeyID: ref.ID, KeyVersion: ref.Version,
		Binding: bindingAAD,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: encode associated data", protect.ErrInvalidBinding)
	}
	return encoded, nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", protect.ErrProviderUnavailable)
	}
	return ctx.Err()
}

func wipe(data []byte) {
	clear(data)
}

var _ protect.Protector = (*keyring)(nil)
var _ protect.Rewrapper = (*keyring)(nil)
