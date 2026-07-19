package protect

import (
	"bytes"
	"errors"
	"testing"

	"github.com/vibe-agi/human/framework"
)

func validBinding() Binding {
	return Binding{
		Component: "human-agent", Purpose: "payload-at-rest",
		Authority: "tenant-a", Namespace: "workspace-a", RecordType: "artifact",
		RecordID: "artifact-a", Field: "payload", Schema: 1,
	}
}

func TestBindingAADIsStableAndCoversEveryField(t *testing.T) {
	binding := validBinding()
	first, err := binding.AAD()
	if err != nil {
		t.Fatal(err)
	}
	second, err := binding.AAD()
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("AAD is not deterministic: %q / %q / %v", first, second, err)
	}
	changed := binding
	changed.Authority = "tenant-b"
	third, err := changed.AAD()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, third) {
		t.Fatal("authority did not participate in AAD")
	}
}

func TestBindingAndEnvelopeValidationFailClosed(t *testing.T) {
	binding := validBinding()
	binding.Schema = 0
	if err := ValidateBinding(binding); !errors.Is(err, ErrInvalidBinding) {
		t.Fatalf("invalid binding error = %v", err)
	}
	envelope := Envelope{Provider: "kms", Format: "aead-v1", KeyID: "key", KeyVersion: "1"}
	if err := ValidateEnvelope(envelope); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("empty ciphertext error = %v", err)
	}
}

func TestDescriptionNegotiatesBaseContract(t *testing.T) {
	description := Description{
		Contract: framework.Contract{ID: ContractID, Major: ContractMajor},
		Provider: "local", Format: "aead-v1",
		MaxPlaintextBytes: 1024, MaxEnvelopeBytes: 2048,
	}
	if err := ValidateDescription(description); err != nil {
		t.Fatalf("ValidateDescription: %v", err)
	}
	description.Contract.Major++
	if err := ValidateDescription(description); !errors.Is(err, framework.ErrContractMismatch) {
		t.Fatalf("major drift error = %v", err)
	}
}

func TestCloneEnvelopeDoesNotAliasBytes(t *testing.T) {
	original := Envelope{Nonce: []byte{1}, Data: []byte{2}}
	cloned := CloneEnvelope(original)
	cloned.Nonce[0], cloned.Data[0] = 3, 4
	if original.Nonce[0] != 1 || original.Data[0] != 2 {
		t.Fatal("CloneEnvelope aliases source bytes")
	}
}
