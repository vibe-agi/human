package framework

import (
	"errors"
	"testing"
)

func TestNegotiateFreezesSatisfiedContract(t *testing.T) {
	provided := Contract{
		ID: "human.agent-store", Major: 1, Minor: 3,
		Features: map[Feature]uint16{"snapshot-cursor": 2},
	}
	negotiated, err := Negotiate(provided, Requirements{
		ID: "human.agent-store", Major: 1, MinimumMinor: 2,
		Features: map[Feature]uint16{"snapshot-cursor": 1},
	})
	if err != nil {
		t.Fatalf("Negotiate: %v", err)
	}
	provided.Features["snapshot-cursor"] = 99
	if negotiated.Features["snapshot-cursor"] != 2 {
		t.Fatal("negotiated contract aliases provider feature map")
	}
}

func TestNegotiateRejectsMissingCapabilityAndMajorDrift(t *testing.T) {
	provided := Contract{ID: "human.agent-store", Major: 2, Minor: 0}
	_, err := Negotiate(provided, Requirements{ID: "human.agent-store", Major: 1})
	if !errors.Is(err, ErrContractMismatch) {
		t.Fatalf("major mismatch error = %v", err)
	}
	provided.Major = 1
	_, err = Negotiate(provided, Requirements{
		ID: "human.agent-store", Major: 1,
		Features: map[Feature]uint16{"snapshot-cursor": 1},
	})
	if !errors.Is(err, ErrContractMismatch) {
		t.Fatalf("feature mismatch error = %v", err)
	}
}
