// Package workerproto defines the private, versioned humand-to-TUI protocol.
package workerproto

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vibe-agi/human/internal/completion"
)

const Version = "1"

type MessageType string

const (
	MessageHello      MessageType = "hello"
	MessageAssignment MessageType = "assignment"
	MessageEvent      MessageType = "event"
	MessageAck        MessageType = "ack"
	MessageError      MessageType = "error"
)

type Envelope struct {
	Version string          `json:"version"`
	Seq     uint64          `json:"seq"`
	Ack     uint64          `json:"ack"`
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func NewEnvelope(messageType MessageType, payload any) (Envelope, error) {
	var raw json.RawMessage
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return Envelope{}, err
		}
		raw = encoded
	}
	return Envelope{Version: Version, Type: messageType, Payload: raw}, nil
}

func (envelope Envelope) Validate() error {
	if envelope.Version != Version {
		return fmt.Errorf("unsupported worker protocol version %q", envelope.Version)
	}
	switch envelope.Type {
	case MessageHello, MessageAssignment, MessageEvent, MessageAck, MessageError:
	default:
		return fmt.Errorf("unsupported worker message type %q", envelope.Type)
	}
	if envelope.Seq == 0 {
		return errors.New("worker message seq must be positive")
	}
	return nil
}

type Hello struct {
	WorkerID string `json:"worker_id"`
}

type Event struct {
	CallerID       string           `json:"caller_id"`
	IdempotencyKey string           `json:"idempotency_key"`
	Event          completion.Event `json:"event"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
