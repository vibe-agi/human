package workerproto

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateEnvelopeSizeUsesExactEncoderBoundary(t *testing.T) {
	payload := Event{CallerID: "caller", IdempotencyKey: "request"}
	payload.Event.Text = strings.Repeat("<", 128)
	size, err := EnvelopeWireSize(MessageEvent, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateEnvelopeSize(MessageEvent, payload, size); err != nil {
		t.Fatalf("exact boundary rejected: %v", err)
	}
	if err := ValidateEnvelopeSize(MessageEvent, payload, size-1); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("one byte over boundary error = %v", err)
	}
}
