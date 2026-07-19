package protect

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func storedTestEnvelope(nonce []byte) Envelope {
	return Envelope{
		Provider: "test-provider", Format: "test-format-v1",
		KeyID: "test-key", KeyVersion: "v1",
		Nonce: nonce, Data: []byte{1, 2, 3, 4},
	}
}

func TestStoredEnvelopeRoundTripIsDeterministicAndIndependent(t *testing.T) {
	for _, nonce := range [][]byte{nil, {}, {1, 2, 3}} {
		envelope := storedTestEnvelope(nonce)
		first, err := MarshalEnvelope(envelope)
		if err != nil {
			t.Fatal(err)
		}
		second, err := MarshalEnvelope(envelope)
		if err != nil || !bytes.Equal(first, second) {
			t.Fatalf("MarshalEnvelope is not deterministic: %q / %q / %v", first, second, err)
		}
		decoded, err := UnmarshalEnvelope(first)
		if err != nil {
			t.Fatal(err)
		}
		if (decoded.Nonce == nil) != (nonce == nil) || !bytes.Equal(decoded.Nonce, nonce) {
			t.Fatalf("nonce shape = %#v, want %#v", decoded.Nonce, nonce)
		}
		if !bytes.Equal(decoded.Data, envelope.Data) {
			t.Fatalf("data = %x, want %x", decoded.Data, envelope.Data)
		}
		decoded.Data[0] = 0xff
		first[len(first)-2] ^= 1
		if envelope.Data[0] != 1 {
			t.Fatal("stored framing aliases caller Envelope bytes")
		}
	}
}

func TestStoredEnvelopeStrictlyRejectsAmbiguousFraming(t *testing.T) {
	valid, err := MarshalEnvelope(storedTestEnvelope([]byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty":         nil,
		"invalid_utf8":  {0xff},
		"unknown":       bytes.Replace(valid, []byte(`"data":`), []byte(`"unknown":0,"data":`), 1),
		"duplicate":     bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"trailing":      append(append([]byte(nil), valid...), []byte(` {}`)...),
		"wrong_version": bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":2`), 1),
		"missing":       bytes.Replace(valid, []byte(`"provider":"test-provider",`), nil, 1),
		"nil_mismatch":  bytes.Replace(valid, []byte(`"nonce_nil":false`), []byte(`"nonce_nil":true`), 1),
		"base64_newline": bytes.Replace(
			valid, []byte(`"data":"AQIDBA=="`), []byte(`"data":"AQID\nBA=="`), 1,
		),
	}
	for name, encoded := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalEnvelope(encoded); !errors.Is(err, ErrInvalidStoredEnvelope) {
				t.Fatalf("UnmarshalEnvelope error = %v, want ErrInvalidStoredEnvelope", err)
			}
		})
	}
	if _, err := UnmarshalEnvelopeLimited(valid, int64(len(valid)-1)); !errors.Is(err, ErrInvalidStoredEnvelope) {
		t.Fatalf("oversize error = %v, want ErrInvalidStoredEnvelope", err)
	}
	if _, err := UnmarshalEnvelopeLimited(valid, MaxStoredEnvelopeBytes+1); !errors.Is(err, ErrInvalidStoredEnvelope) {
		t.Fatalf("invalid limit error = %v, want ErrInvalidStoredEnvelope", err)
	}
}

func TestStoredEnvelopeErrorsDoNotEchoInput(t *testing.T) {
	secret := "plaintext-must-not-be-echoed-by-strict-decoder"
	encoded := []byte(`{"version":1,"` + secret + `":true}`)
	_, err := UnmarshalEnvelope(encoded)
	if !errors.Is(err, ErrInvalidStoredEnvelope) {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error disclosed input: %v", err)
	}
}
