// Package workerws implements the official WebSocket adapter for the public
// HumanLLM WorkerTransport port. The wire protocol contains only completion
// assignment and event settlement messages; HumanAgent task/job messages are a
// separate protocol in agent/workerws.
package workerws

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	// ProtocolVersion is bumped for incompatible wire changes. It is independent
	// from llm.WorkerTransportContractMajor, which versions the public Go port.
	ProtocolVersion = "1"
	// SessionHeader is generated afresh by a remote worker for every dial. The
	// value is diagnostic connection identity only and never enters completion,
	// lease, delivery, or outbox correctness identity.
	SessionHeader = "X-Human-LLM-Worker-Session"
)

type messageType string

const (
	messageHello         messageType = "hello"
	messageAssignment    messageType = "assignment"
	messageAssignmentACK messageType = "assignment_ack"
	messageEvent         messageType = "event"
	messageEventReceipt  messageType = "event_receipt"
)

type envelope struct {
	Version string          `json:"version"`
	Type    messageType     `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func newEnvelope(kind messageType, payload any) (envelope, error) {
	var raw json.RawMessage
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return envelope{}, err
		}
		raw = encoded
	}
	return envelope{Version: ProtocolVersion, Type: kind, Payload: raw}, nil
}

func (message envelope) validateInbound(allowed ...messageType) error {
	if message.Version != ProtocolVersion {
		return fmt.Errorf("unsupported HumanLLM worker protocol version %q", message.Version)
	}
	for _, kind := range allowed {
		if message.Type == kind {
			return nil
		}
	}
	return fmt.Errorf("unexpected HumanLLM worker message type %q", message.Type)
}

func decodePayload[T any](message envelope) (T, error) {
	var value T
	if len(message.Payload) == 0 {
		return value, errors.New("HumanLLM worker message payload is required")
	}
	if err := decodeStrictJSON(message.Payload, &value); err != nil {
		return value, fmt.Errorf("decode HumanLLM worker %s payload: %w", message.Type, err)
	}
	return value, nil
}

func decodeStrictJSON(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	// Canonical tool inputs are JSON values. UseNumber preserves integers and
	// exponent spellings which float64 would round before core/journal digesting.
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

type hello struct {
	Gateway string `json:"gateway"`
	Worker  string `json:"worker"`
	Session string `json:"session"`
}
