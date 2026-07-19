package protect

import (
	"bytes"
	"errors"
	"testing"
)

func TestStoredValuePlainRoundTripPreservesNilEmptyAndOwnership(t *testing.T) {
	for _, plaintext := range [][]byte{nil, {}, {1, 2, 3}} {
		value := NewPlainStoredValue(plaintext)
		encoded, err := MarshalStoredValue(value)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := UnmarshalStoredValue(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Mode != StoredValuePlain || decoded.Nil != (plaintext == nil) ||
			(decoded.Plain == nil) != (plaintext == nil) || !bytes.Equal(decoded.Plain, plaintext) {
			t.Fatalf("decoded plain value = %#v, want %#v", decoded, plaintext)
		}
		if len(decoded.Plain) > 0 {
			decoded.Plain[0] ^= 0xff
		}
		if len(plaintext) > 0 && plaintext[0] != 1 {
			t.Fatal("decoded StoredValue aliases caller plaintext")
		}
	}
}

func TestStoredValueSealedRoundTripAndDeepCopy(t *testing.T) {
	envelope := storedTestEnvelope([]byte{7, 8, 9})
	value, err := NewSealedStoredValue(false, envelope)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Data[0] = 0xff
	encoded, err := MarshalStoredValue(value)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalStoredValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Mode != StoredValueSealed || decoded.Nil || decoded.Envelope == nil || decoded.Envelope.Data[0] != 1 {
		t.Fatalf("decoded sealed value = %#v", decoded)
	}
	cloned := CloneStoredValue(decoded)
	decoded.Envelope.Data[0] = 0xee
	if cloned.Envelope.Data[0] != 1 {
		t.Fatal("CloneStoredValue aliases Envelope bytes")
	}
}

func TestStoredValueRejectsAmbiguousAndNonCanonicalFraming(t *testing.T) {
	valid, err := MarshalStoredValue(NewPlainStoredValue([]byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"unknown":       bytes.Replace(valid, []byte(`"plain":`), []byte(`"unknown":0,"plain":`), 1),
		"duplicate":     bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"trailing":      append(append([]byte(nil), valid...), []byte(` {}`)...),
		"wrong_version": bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":2`), 1),
		"missing":       bytes.Replace(valid, []byte(`"mode":"plain",`), nil, 1),
		"unknown_mode":  bytes.Replace(valid, []byte(`"mode":"plain"`), []byte(`"mode":"magic"`), 1),
		"plain_envelope": bytes.Replace(valid, []byte(`"envelope":null`),
			[]byte(`"envelope":{"version":1}`), 1),
		"noncanonical_base64": bytes.Replace(valid, []byte(`"plain":"AQ=="`), []byte(`"plain":"AQ==\n"`), 1),
		"invalid_utf8":        {0xff},
	}
	for name, encoded := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalStoredValue(encoded); !errors.Is(err, ErrInvalidStoredValue) {
				t.Fatalf("UnmarshalStoredValue error = %v, want ErrInvalidStoredValue", err)
			}
		})
	}
	if _, err := UnmarshalStoredValueLimited(valid, int64(len(valid)-1)); !errors.Is(err, ErrInvalidStoredValue) {
		t.Fatalf("oversize error = %v, want ErrInvalidStoredValue", err)
	}
}

func TestStoredValueRejectsModeShapeMismatch(t *testing.T) {
	envelope := storedTestEnvelope(nil)
	cases := []StoredValue{
		{Mode: StoredValuePlain, Nil: true, Plain: []byte{}},
		{Mode: StoredValuePlain, Plain: []byte{}, Envelope: &envelope},
		{Mode: StoredValueSealed, Plain: []byte{}, Envelope: &envelope},
		{Mode: StoredValueSealed},
		{Mode: "magic", Plain: []byte{}},
	}
	for index, value := range cases {
		if err := ValidateStoredValue(value); !errors.Is(err, ErrInvalidStoredValue) {
			t.Fatalf("case %d validation error = %v", index, err)
		}
	}
}
