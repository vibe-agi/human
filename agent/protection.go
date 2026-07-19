package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vibe-agi/human/protect"
	"github.com/vibe-agi/human/workspace"
)

const (
	// agentProtectionSchema versions the authenticated record binding. It is
	// intentionally independent of StoredValue's wire version: changing JSON
	// framing must not silently reinterpret the AAD domain, and vice versa.
	agentProtectionSchema  uint32 = 1
	maxStoredArtifactBytes        = protect.MaxStoredValueBytes
)

type preparedMessageContent struct {
	encoded []byte
	digest  StoreDigest
}

func (agent *Agent) prepareMessageContent(
	ctx context.Context,
	task TaskRef,
	message MessageInput,
) (preparedMessageContent, error) {
	plaintext, err := json.Marshal(message.Parts)
	if err != nil {
		return preparedMessageContent{}, fmt.Errorf("encode Agent message: %w", err)
	}
	digest, err := contentDigest(message.Parts)
	if err != nil {
		return preparedMessageContent{}, err
	}
	encoded, err := agent.sealStoredValue(ctx, messageProtectionBinding(task, message.ID), plaintext)
	if err != nil {
		return preparedMessageContent{}, fmt.Errorf("protect Agent message content: %w", err)
	}
	if len(encoded) == 0 || len(encoded) > maxPageBytes {
		return preparedMessageContent{}, fmt.Errorf("%w: protected Agent message exceeds storage budget", ErrInvalidArgument)
	}
	return preparedMessageContent{encoded: encoded, digest: StoreDigest(digest)}, nil
}

func (agent *Agent) messageFromStoreRecord(ctx context.Context, record StoreMessageRecord) (Message, error) {
	message := Message{
		ID: record.ID, Task: record.Task, Sequence: record.Sequence,
		Author: record.Author, CreatedAt: record.CreatedAt,
	}
	if len(record.EncodedParts) == 0 || len(record.EncodedParts) > maxPageBytes {
		return Message{}, fmt.Errorf("%w: Agent message has invalid encoded content", ErrCorruptStore)
	}
	plaintext, err := agent.openStoredValue(
		ctx, messageProtectionBinding(record.Task, record.ID), record.EncodedParts, maxPageBytes,
	)
	if err != nil {
		return Message{}, fmt.Errorf("open Agent message content: %w", err)
	}
	if plaintext == nil || json.Unmarshal(plaintext, &message.Parts) != nil {
		return Message{}, fmt.Errorf("%w: decode Agent message content", ErrCorruptStore)
	}
	actual, err := contentDigest(message.Parts)
	if err != nil || StoreDigest(actual) != record.PartsDigest {
		return Message{}, fmt.Errorf("%w: Agent message content digest mismatch", ErrCorruptStore)
	}
	if err := validateStoredMessage(message); err != nil {
		return Message{}, err
	}
	return cloneMessage(message), nil
}

func (agent *Agent) prepareArtifactPayload(
	ctx context.Context,
	artifact Artifact,
	payload workspace.Payload,
) ([]byte, error) {
	encoded, err := agent.sealStoredValue(ctx, artifactProtectionBinding(artifact), payload.Data)
	if err != nil {
		return nil, fmt.Errorf("protect Agent Artifact payload: %w", err)
	}
	if len(encoded) == 0 || int64(len(encoded)) > maxStoredArtifactBytes {
		return nil, fmt.Errorf("%w: protected Agent Artifact exceeds storage budget", ErrInvalidArgument)
	}
	return encoded, nil
}

func (agent *Agent) artifactContentFromStoreRecord(
	ctx context.Context,
	record StoreArtifactRecord,
) (ArtifactContent, error) {
	if len(record.EncodedPayload) == 0 || int64(len(record.EncodedPayload)) > maxStoredArtifactBytes {
		return ArtifactContent{}, fmt.Errorf("%w: Agent Artifact has invalid encoded payload", ErrCorruptStore)
	}
	plaintext, err := agent.openStoredValue(
		ctx, artifactProtectionBinding(record.Artifact), record.EncodedPayload, maxStoredArtifactBytes,
	)
	if err != nil {
		return ArtifactContent{}, fmt.Errorf("open Agent Artifact payload: %w", err)
	}
	content := ArtifactContent{
		Artifact: record.Artifact,
		Payload:  workspace.Payload{MediaType: record.Artifact.MediaType, Data: plaintext},
	}
	if err := validateStoredArtifact(content); err != nil {
		return ArtifactContent{}, err
	}
	return cloneArtifactContent(content), nil
}

func (agent *Agent) sealStoredValue(
	ctx context.Context,
	binding protect.Binding,
	plaintext []byte,
) ([]byte, error) {
	if agent.protector == nil {
		return protect.MarshalStoredValue(protect.NewPlainStoredValue(plaintext))
	}
	if int64(len(plaintext)) > agent.protectorDescription.MaxPlaintextBytes {
		return nil, fmt.Errorf(
			"%w: plaintext exceeds Protector limit of %d bytes",
			ErrInvalidArgument, agent.protectorDescription.MaxPlaintextBytes,
		)
	}
	envelope, err := agent.protector.Seal(ctx, binding, plaintext)
	if err != nil {
		return nil, err
	}
	if err := agent.validateProtectorEnvelope(envelope); err != nil {
		return nil, err
	}
	value, err := protect.NewSealedStoredValue(plaintext == nil, envelope)
	if err != nil {
		return nil, err
	}
	return protect.MarshalStoredValue(value)
}

func (agent *Agent) openStoredValue(
	ctx context.Context,
	binding protect.Binding,
	encoded []byte,
	maxBytes int64,
) ([]byte, error) {
	value, err := agent.validateStoredValueShape(encoded, maxBytes)
	if err != nil {
		return nil, err
	}
	switch value.Mode {
	case protect.StoredValuePlain:
		return appendExactAgentBytes(value.Plain), nil
	case protect.StoredValueSealed:
		plaintext, err := agent.protector.Open(ctx, binding, protect.CloneEnvelope(*value.Envelope))
		if err != nil {
			if errors.Is(err, protect.ErrAuthentication) || errors.Is(err, protect.ErrInvalidEnvelope) {
				return nil, errors.Join(ErrCorruptStore, err)
			}
			return nil, err
		}
		if (plaintext == nil) != value.Nil {
			return nil, fmt.Errorf("%w: protected payload nil marker mismatch", ErrCorruptStore)
		}
		if int64(len(plaintext)) > agent.protectorDescription.MaxPlaintextBytes {
			return nil, fmt.Errorf(
				"%w: Protector returned plaintext above its declared limit of %d bytes",
				ErrCorruptStore, agent.protectorDescription.MaxPlaintextBytes,
			)
		}
		return appendExactAgentBytes(plaintext), nil
	default:
		return nil, fmt.Errorf("%w: unsupported protected payload mode", ErrCorruptStore)
	}
}

// validateStoredValueShape checks persisted framing and the immutable Protector
// capability without invoking KMS/provider I/O. Exact command replay uses this
// path so an already committed metadata receipt remains available even when an
// old key or the provider is temporarily unavailable.
func (agent *Agent) validateStoredValueShape(encoded []byte, maxBytes int64) (protect.StoredValue, error) {
	value, err := protect.UnmarshalStoredValueLimited(encoded, maxBytes)
	if err != nil {
		return protect.StoredValue{}, errors.Join(ErrCorruptStore, err)
	}
	switch value.Mode {
	case protect.StoredValuePlain:
		if agent.protector != nil && !agent.allowPlaintext {
			return protect.StoredValue{}, errors.Join(ErrCorruptStore, ErrProtectionDowngrade)
		}
	case protect.StoredValueSealed:
		if agent.protector == nil || value.Envelope == nil {
			return protect.StoredValue{}, fmt.Errorf(
				"%w: sealed payload requires its configured Protector", ErrCorruptStore,
			)
		}
		if err := agent.validateProtectorEnvelope(*value.Envelope); err != nil {
			return protect.StoredValue{}, errors.Join(ErrCorruptStore, err)
		}
	default:
		return protect.StoredValue{}, fmt.Errorf("%w: unsupported protected payload mode", ErrCorruptStore)
	}
	return value, nil
}

func (agent *Agent) validateProtectorEnvelope(envelope protect.Envelope) error {
	if err := protect.ValidateEnvelope(envelope); err != nil {
		return err
	}
	description := agent.protectorDescription
	if envelope.Provider != description.Provider || envelope.Format != description.Format {
		return fmt.Errorf(
			"%w: Protector returned provider/format %q/%q, want %q/%q",
			protect.ErrInvalidEnvelope, envelope.Provider, envelope.Format,
			description.Provider, description.Format,
		)
	}
	envelopeBytes := int64(len(envelope.Nonce)) + int64(len(envelope.Data))
	if envelopeBytes > description.MaxEnvelopeBytes {
		return fmt.Errorf(
			"%w: envelope exceeds Protector limit of %d bytes",
			protect.ErrInvalidEnvelope, description.MaxEnvelopeBytes,
		)
	}
	return nil
}

func messageProtectionBinding(task TaskRef, message MessageID) protect.Binding {
	return protect.Binding{
		Component: "human.agent", Purpose: "persistence",
		Authority:  string(task.Workspace.Authority),
		Namespace:  string(task.Workspace.ID) + "/" + string(task.ID),
		RecordType: "message", RecordID: string(message), Field: "parts",
		Schema: agentProtectionSchema,
	}
}

func artifactProtectionBinding(artifact Artifact) protect.Binding {
	return protect.Binding{
		Component: "human.agent", Purpose: "persistence",
		Authority: string(artifact.Ref.Workspace.Authority),
		Namespace: string(artifact.Ref.Workspace.ID) + "/" + string(artifact.Task.ID),
		// Digest binds the payload's stable semantic identity (record/task,
		// revisions, payload digest, and media type). Store-owned lifecycle metadata
		// such as FrozenAt remains protected by the Store contract, not by this AAD.
		RecordType: "artifact", RecordID: string(artifact.Ref.ID) + "/" + string(artifact.Digest), Field: "payload",
		Schema: agentProtectionSchema,
	}
}

func appendExactAgentBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result
}
