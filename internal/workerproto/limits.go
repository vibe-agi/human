package workerproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// MaxWireMessageBytes is the single application-message budget for the
// private worker WebSocket in both directions. HTTP completion bodies use the
// same value as their coarse read bound, then the gateway performs an exact
// assignment-envelope check before durable admission.
const MaxWireMessageBytes int64 = 8 << 20

var ErrMessageTooLarge = errors.New("worker protocol message exceeds the wire limit")

// EnvelopeWireSize returns the largest encoded size of an envelope carrying
// payload. wsjson.Write uses json.Encoder, including its trailing newline.
// Maximal sequence and ACK values make this safe for every point in a long-
// lived connection rather than only for the next frame.
func EnvelopeWireSize(messageType MessageType, payload any) (int64, error) {
	envelope, err := NewEnvelope(messageType, payload)
	if err != nil {
		return 0, err
	}
	envelope.Seq = math.MaxUint64
	envelope.Ack = math.MaxUint64
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(envelope); err != nil {
		return 0, fmt.Errorf("encode worker protocol envelope: %w", err)
	}
	return int64(encoded.Len()), nil
}

func ValidateEnvelopeSize(messageType MessageType, payload any, limit int64) error {
	if limit <= 0 {
		return errors.New("worker protocol wire limit must be positive")
	}
	size, err := EnvelopeWireSize(messageType, payload)
	if err != nil {
		return err
	}
	if size > limit {
		return fmt.Errorf("%w: encoded=%d limit=%d", ErrMessageTooLarge, size, limit)
	}
	return nil
}
