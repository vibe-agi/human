package llm

import (
	"context"
	"errors"
	"fmt"

	"github.com/vibe-agi/human/protect"
)

const persistedBlobVersion = 1

var errPersistedPayloadLimit = errors.New("llm: persisted payload exceeds the configured read limit")

type payloadBinding struct {
	caller     CallerID
	namespace  string
	recordType string
	recordID   string
	field      string
}

func (service *Service) sealPayload(
	ctx context.Context,
	binding payloadBinding,
	plaintext []byte,
) ([]byte, error) {
	var framed protect.StoredValue
	if service.protector == nil {
		framed = protect.NewPlainStoredValue(plaintext)
	} else {
		if err := service.validateProtectorPlaintext(plaintext); err != nil {
			return nil, fmt.Errorf("seal HumanLLM persisted payload: %w", err)
		}
		aad, err := service.protectionBinding(binding)
		if err != nil {
			return nil, err
		}
		envelope, err := service.protector.Seal(ctx, aad, clonePayloadBytes(plaintext))
		if err != nil {
			return nil, fmt.Errorf("seal HumanLLM persisted payload: %w", err)
		}
		if err := service.validateProtectorEnvelope(envelope); err != nil {
			return nil, fmt.Errorf("seal HumanLLM persisted payload: %w", err)
		}
		framed, err = protect.NewSealedStoredValue(plaintext == nil, envelope)
		if err != nil {
			return nil, fmt.Errorf("seal HumanLLM persisted payload: %w", err)
		}
	}
	encoded, err := protect.MarshalStoredValue(framed)
	if err != nil {
		return nil, fmt.Errorf("encode HumanLLM persisted payload: %w", err)
	}
	if service.readLimitBytes < 1 || int64(len(encoded)) > service.readLimitBytes {
		return nil, errPersistedPayloadLimit
	}
	return encoded, nil
}

func (service *Service) openPayload(
	ctx context.Context,
	binding payloadBinding,
	encoded []byte,
) ([]byte, error) {
	framed, err := protect.UnmarshalStoredValueLimited(encoded, service.readLimitBytes)
	if err != nil {
		return nil, fmt.Errorf("decode HumanLLM persisted payload: %w", err)
	}
	switch framed.Mode {
	case protect.StoredValuePlain:
		if service.protector != nil && service.protectionReadPolicy != ProtectionAllowPlain {
			return nil, fmt.Errorf("HumanLLM protected deployment refuses an unsealed persisted value")
		}
		if framed.Nil {
			return nil, nil
		}
		plaintext := make([]byte, len(framed.Plain))
		copy(plaintext, framed.Plain)
		return plaintext, nil
	case protect.StoredValueSealed:
		if service.protector == nil {
			return nil, fmt.Errorf("sealed HumanLLM persisted payload requires a Protector")
		}
		if err := service.validateProtectorEnvelope(*framed.Envelope); err != nil {
			return nil, fmt.Errorf("open HumanLLM persisted payload: %w", err)
		}
		aad, err := service.protectionBinding(binding)
		if err != nil {
			return nil, err
		}
		plaintext, err := service.protector.Open(ctx, aad, protect.CloneEnvelope(*framed.Envelope))
		if err != nil {
			return nil, fmt.Errorf("open HumanLLM persisted payload: %w", err)
		}
		if err := service.validateProtectorPlaintext(plaintext); err != nil {
			return nil, fmt.Errorf("open HumanLLM persisted payload: %w", err)
		}
		if (plaintext == nil) != framed.Nil {
			return nil, fmt.Errorf("HumanLLM persisted payload nil marker is not authenticated consistently")
		}
		return clonePayloadBytes(plaintext), nil
	default:
		return nil, fmt.Errorf("unsupported HumanLLM persisted payload mode %q", framed.Mode)
	}
}

func (service *Service) validateProtectorPlaintext(plaintext []byte) error {
	maximum := service.protectorDescription.MaxPlaintextBytes
	if maximum < 1 || service.readLimitBytes < 1 || int64(len(plaintext)) > maximum ||
		int64(len(plaintext)) > service.readLimitBytes {
		return errors.Join(
			errPersistedPayloadLimit,
			fmt.Errorf("%w: plaintext exceeds the frozen Protector/core limit", protect.ErrInvalidEnvelope),
		)
	}
	return nil
}

func (service *Service) validateProtectorEnvelope(envelope protect.Envelope) error {
	if err := protect.ValidateEnvelope(envelope); err != nil {
		return err
	}
	description := service.protectorDescription
	if envelope.Provider != description.Provider || envelope.Format != description.Format {
		return fmt.Errorf("%w: provider or format differs from the frozen Protector description", protect.ErrInvalidEnvelope)
	}
	dataBytes := int64(len(envelope.Data))
	nonceBytes := int64(len(envelope.Nonce))
	if description.MaxEnvelopeBytes < 1 || dataBytes > description.MaxEnvelopeBytes ||
		nonceBytes > description.MaxEnvelopeBytes-dataBytes {
		return fmt.Errorf("%w: envelope exceeds the frozen Protector limit", protect.ErrInvalidEnvelope)
	}
	return nil
}

func clonePayloadBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

func (service *Service) protectionBinding(input payloadBinding) (protect.Binding, error) {
	binding := protect.Binding{
		Component:  "human.llm",
		Purpose:    "persistence",
		Authority:  string(input.caller),
		Namespace:  service.deploymentID + "/" + input.namespace,
		RecordType: input.recordType,
		RecordID:   input.recordID,
		Field:      input.field,
		Schema:     persistedBlobVersion,
	}
	if err := protect.ValidateBinding(binding); err != nil {
		return protect.Binding{}, fmt.Errorf("construct HumanLLM protection binding: %w", err)
	}
	return binding, nil
}

func requestPayloadBinding(key StoreRequestKey, requestID string) payloadBinding {
	return payloadBinding{
		caller: key.Caller, namespace: string(key.IdempotencyKey),
		recordType: "request", recordID: requestID, field: "canonical-payload",
	}
}

func responseEventBinding(key StoreRequestKey, requestID string, sequence uint64) payloadBinding {
	return payloadBinding{
		caller: key.Caller, namespace: string(key.IdempotencyKey), recordType: "response-event",
		recordID: fmt.Sprintf("%s/%d", requestID, sequence), field: "data",
	}
}

func responseBodyBinding(key StoreRequestKey, requestID string) payloadBinding {
	return payloadBinding{
		caller: key.Caller, namespace: string(key.IdempotencyKey),
		recordType: "request", recordID: requestID, field: "decision-body",
	}
}

func toolResultBinding(key StoreToolExecutionKey) payloadBinding {
	return payloadBinding{
		caller: key.Task.Caller, namespace: string(key.Task.Task), recordType: "tool-execution",
		recordID: string(key.ToolCallID), field: "result",
	}
}
