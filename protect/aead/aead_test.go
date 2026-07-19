package aead_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/protect"
	"github.com/vibe-agi/human/protect/aead"
)

func TestProtectorConformance(t *testing.T) {
	humantest.TestProtector(t, func(ctx context.Context, _ testing.TB) (humantest.ProtectorFixture, error) {
		oldMaterial := bytes.Repeat([]byte{'o'}, aead.KeySize)
		newMaterial := bytes.Repeat([]byte{'n'}, aead.KeySize)
		old := aead.Key{ID: "payload-key", Version: "v1", Material: oldMaterial}
		current := aead.Key{ID: "payload-key", Version: "v2", Material: newMaterial}
		initialConfig := aead.Config{
			Active: aead.KeyRef{ID: old.ID, Version: old.Version}, Keys: []aead.Key{old},
		}
		rotatedConfig := aead.Config{
			Active: aead.KeyRef{ID: current.ID, Version: current.Version}, Keys: []aead.Key{old, current},
		}
		currentConfig := aead.Config{
			Active: aead.KeyRef{ID: current.ID, Version: current.Version}, Keys: []aead.Key{current},
		}
		initial, err := aead.Open(ctx, initialConfig)
		if err != nil {
			return humantest.ProtectorFixture{}, err
		}
		return humantest.ProtectorFixture{
			Initial: initial,
			Reopen: func(ctx context.Context) (protect.Resource, error) {
				return aead.Open(ctx, initialConfig)
			},
			Rotate: func(ctx context.Context) (protect.Resource, error) {
				return aead.Open(ctx, rotatedConfig)
			},
			CurrentOnly: func(ctx context.Context) (protect.Resource, error) {
				return aead.Open(ctx, currentConfig)
			},
			DiagnosticSecrets: []string{
				string(oldMaterial), string(newMaterial),
				hex.EncodeToString(oldMaterial), hex.EncodeToString(newMaterial),
				base64.StdEncoding.EncodeToString(oldMaterial), base64.StdEncoding.EncodeToString(newMaterial),
			},
		}, nil
	})
}

func TestOpenCopiesConfigurationAndReleaseClosesRetainedValue(t *testing.T) {
	material := bytes.Repeat([]byte{0x7a}, aead.KeySize)
	config := aead.Config{
		Active: aead.KeyRef{ID: "local-key", Version: "v1"},
		Keys:   []aead.Key{{ID: "local-key", Version: "v1", Material: material}},
	}
	resource, err := aead.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	clear(material)
	binding := binding()
	envelope, err := protector.Seal(t.Context(), binding, []byte("copied-key"))
	if err != nil {
		t.Fatalf("Seal after caller key wipe: %v", err)
	}
	if payload, err := protector.Open(t.Context(), binding, envelope); err != nil || string(payload) != "copied-key" {
		t.Fatalf("Open = %q / %v", payload, err)
	}
	if err := resource.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := protector.Describe(t.Context()); !errors.Is(err, aead.ErrClosed) {
		t.Fatalf("retained Describe after release error = %v, want ErrClosed", err)
	}
	if _, err := protector.Seal(t.Context(), binding, []byte("x")); !errors.Is(err, aead.ErrClosed) {
		t.Fatalf("retained Seal after release error = %v, want ErrClosed", err)
	}
	if _, err := protector.Open(t.Context(), binding, envelope); !errors.Is(err, aead.ErrClosed) {
		t.Fatalf("retained Open after release error = %v, want ErrClosed", err)
	}
}

func TestSealUsesFreshNonce(t *testing.T) {
	resource := mustOpen(t, validConfig())
	protector, _ := resource.Value()
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	first, err := protector.Seal(t.Context(), binding(), []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := protector.Seal(t.Context(), binding(), []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first.Nonce, second.Nonce) || bytes.Equal(first.Data, second.Data) {
		t.Fatal("two Seal operations reused nonce or ciphertext")
	}
}

func TestKeyReferenceMetadataIsAuthenticatedEvenWhenMaterialMatches(t *testing.T) {
	material := bytes.Repeat([]byte{0x33}, aead.KeySize)
	resource := mustOpen(t, aead.Config{
		Active: aead.KeyRef{ID: "payload-key", Version: "v1"},
		Keys: []aead.Key{
			{ID: "payload-key", Version: "v1", Material: material},
			{ID: "payload-key", Version: "v2", Material: material},
		},
	})
	protector, _ := resource.Value()
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	envelope, err := protector.Seal(t.Context(), binding(), []byte("bound"))
	if err != nil {
		t.Fatal(err)
	}
	envelope.KeyVersion = "v2"
	if _, err := protector.Open(t.Context(), binding(), envelope); !errors.Is(err, protect.ErrAuthentication) {
		t.Fatalf("Open with substituted same-material key reference error = %v, want ErrAuthentication", err)
	}
}

func TestOpenRejectsInvalidConfigurationWithoutDisclosingMaterial(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	cases := map[string]aead.Config{
		"no_keys": {
			Active: aead.KeyRef{ID: "key", Version: "v1"},
		},
		"wrong_size": {
			Active: aead.KeyRef{ID: "key", Version: "v1"},
			Keys:   []aead.Key{{ID: "key", Version: "v1", Material: secret[:31]}},
		},
		"duplicate": {
			Active: aead.KeyRef{ID: "key", Version: "v1"},
			Keys: []aead.Key{
				{ID: "key", Version: "v1", Material: secret},
				{ID: "key", Version: "v1", Material: secret},
			},
		},
		"missing_active": {
			Active: aead.KeyRef{ID: "key", Version: "v2"},
			Keys:   []aead.Key{{ID: "key", Version: "v1", Material: secret}},
		},
		"invalid_metadata": {
			Active: aead.KeyRef{ID: "key\nsecret", Version: "v1"},
			Keys:   []aead.Key{{ID: "key\nsecret", Version: "v1", Material: secret}},
		},
		"invalid_limit": {
			Active:            aead.KeyRef{ID: "key", Version: "v1"},
			Keys:              []aead.Key{{ID: "key", Version: "v1", Material: secret}},
			MaxPlaintextBytes: -1,
		},
	}
	for name, config := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := aead.Open(t.Context(), config)
			if !errors.Is(err, aead.ErrInvalidConfig) {
				t.Fatalf("Open error = %v, want ErrInvalidConfig", err)
			}
			for _, forbidden := range []string{
				string(secret), hex.EncodeToString(secret), base64.StdEncoding.EncodeToString(secret),
			} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("configuration error disclosed key material: %v", err)
				}
			}
		})
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := aead.Open(cancelled, validConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Open error = %v, want context.Canceled", err)
	}
}

func validConfig() aead.Config {
	return aead.Config{
		Active: aead.KeyRef{ID: "local-key", Version: "v1"},
		Keys: []aead.Key{{
			ID: "local-key", Version: "v1", Material: bytes.Repeat([]byte{0x42}, aead.KeySize),
		}},
	}
}

func mustOpen(t *testing.T, config aead.Config) protect.Resource {
	t.Helper()
	resource, err := aead.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	return resource
}

func binding() protect.Binding {
	return protect.Binding{
		Component: "human-llm", Purpose: "payload-at-rest",
		Authority: "tenant", Namespace: "workspace", RecordType: "request",
		RecordID: "request", Field: "canonical-request", Schema: 1,
	}
}
