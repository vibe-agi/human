package llm_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

type callerTransportStub struct {
	description llm.CallerTransportDescription
}

func (transport *callerTransportStub) Description() llm.CallerTransportDescription {
	return transport.description
}

func (*callerTransportStub) Start(
	context.Context,
	llm.CallerEndpoint,
) (llm.CallerTransportRuntime, error) {
	return nil, nil
}

func validCallerTransportDescription() llm.CallerTransportDescription {
	return llm.CallerTransportDescription{
		Contract: framework.Contract{
			ID: llm.CallerTransportContractID, Major: llm.CallerTransportContractMajor,
			Features: map[framework.Feature]uint16{"custom.feature": 1},
		},
		Provider: "contract-test",
		Version:  "1",
	}
}

func TestCallerTransportDescriptionNegotiatesFrozenContract(t *testing.T) {
	description := validCallerTransportDescription()
	negotiated, err := llm.NegotiateCallerTransport(description)
	if err != nil {
		t.Fatalf("negotiate valid caller transport: %v", err)
	}
	description.Contract.Features["custom.feature"] = 2
	if got := negotiated.Contract.Features["custom.feature"]; got != 1 {
		t.Fatalf("negotiated feature mutated through provider map: got %d", got)
	}

	description = validCallerTransportDescription()
	description.Contract.Major++
	if err := description.Validate(); !errors.Is(err, llm.ErrCallerTransportContractMismatch) {
		t.Fatalf("major mismatch error = %v", err)
	}
	for _, provider := range []string{"", " http", "grpc\nsecret=value"} {
		description = validCallerTransportDescription()
		description.Provider = provider
		if err := description.Validate(); !errors.Is(err, llm.ErrCallerTransportDescription) {
			t.Fatalf("provider %q error = %v", provider, err)
		}
	}
}

func TestValidateCallerTransportRejectsNilAndTypedNil(t *testing.T) {
	if _, err := llm.ValidateCallerTransport(nil); !errors.Is(err, llm.ErrCallerTransportDescription) {
		t.Fatalf("nil transport error = %v", err)
	}
	var typedNil *callerTransportStub
	if _, err := llm.ValidateCallerTransport(typedNil); !errors.Is(err, llm.ErrCallerTransportDescription) {
		t.Fatalf("typed-nil transport error = %v", err)
	}

	transport := &callerTransportStub{description: validCallerTransportDescription()}
	if _, err := llm.ValidateCallerTransport(transport); err != nil {
		t.Fatalf("valid transport: %v", err)
	}
}

var _ llm.CallerTransport = (*callerTransportStub)(nil)
