package a2a

import (
	"bytes"
	"errors"
	"fmt"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/workspace"
)

// ErrInvalidApplyArtifact reports an Artifact or journal result that cannot be
// mapped to Human's exact caller-side apply protocol.
var ErrInvalidApplyArtifact = errors.New("a2a: invalid Human workspace Artifact")

// DecodeApplyArtifact converts one negotiated Human workspace Artifact into
// the exact caller-side CAS intent. Authority must come from the caller's
// authenticated account configuration, never from Artifact metadata.
//
// Human workspace Artifacts deliberately carry their payload as Raw even for
// text and JSON media types. That preserves the exact bytes bound by
// payloadDigest instead of normalizing whitespace, Unicode, or object order.
func DecodeApplyArtifact(authority agent.AuthorityID, artifact *sdk.Artifact) (workspace.ApplyIntent, error) {
	if authority == "" || artifact == nil || artifact.ID == "" {
		return workspace.ApplyIntent{}, fmt.Errorf("%w: authority, Artifact, and artifactId are required", ErrInvalidApplyArtifact)
	}
	if !containsString(artifact.Extensions, WorkspaceExtensionURI) {
		return workspace.ApplyIntent{}, fmt.Errorf("%w: workspace extension is not declared", ErrInvalidApplyArtifact)
	}
	metadata, ok := artifact.Metadata[WorkspaceExtensionURI].(map[string]any)
	if !ok {
		return workspace.ApplyIntent{}, fmt.Errorf("%w: workspace metadata is missing", ErrInvalidApplyArtifact)
	}
	workspaceID, okWorkspace := metadataString(metadata, "workspaceId")
	artifactDigest, okArtifactDigest := metadataString(metadata, "artifactDigest")
	payloadDigest, okPayloadDigest := metadataString(metadata, "payloadDigest")
	baseRevision, okBase := metadataString(metadata, "baseRevision")
	resultRevision, okResult := metadataString(metadata, "resultRevision")
	if !okWorkspace || !okArtifactDigest || !okPayloadDigest || !okBase || !okResult {
		return workspace.ApplyIntent{}, fmt.Errorf("%w: workspace metadata fields are missing or invalid", ErrInvalidApplyArtifact)
	}
	if len(artifact.Parts) != 1 || artifact.Parts[0] == nil {
		return workspace.ApplyIntent{}, fmt.Errorf("%w: exactly one payload Part is required", ErrInvalidApplyArtifact)
	}
	part := artifact.Parts[0]
	if part.Filename != "" || len(part.Metadata) != 0 {
		return workspace.ApplyIntent{}, fmt.Errorf("%w: payload Part filename and metadata are not supported", ErrInvalidApplyArtifact)
	}
	raw, ok := part.Content.(sdk.Raw)
	if !ok || len(raw) == 0 || part.MediaType == "" {
		return workspace.ApplyIntent{}, fmt.Errorf("%w: payload Part must contain exact Raw bytes and mediaType", ErrInvalidApplyArtifact)
	}
	payload := workspace.Payload{MediaType: part.MediaType, Data: bytes.Clone(raw)}
	intent := workspace.ApplyIntent{
		Identity: workspace.ApplyIdentity{
			Authority: string(authority), Workspace: workspaceID, Artifact: string(artifact.ID),
		},
		ArtifactDigest: workspace.Digest(artifactDigest),
		PayloadDigest:  workspace.Digest(payloadDigest),
		BaseRevision:   workspace.Revision(baseRevision),
		ResultRevision: workspace.Revision(resultRevision),
		Payload:        payload,
	}
	if workspace.DigestPayload(payload) != intent.PayloadDigest {
		return workspace.ApplyIntent{}, fmt.Errorf("%w: payload bytes do not match payloadDigest", ErrInvalidApplyArtifact)
	}
	return intent, nil
}

// NewApplyReceiptRequest maps a terminal caller-side journal record to the
// authenticated A2A apply-receipt extension. commandID and receiptID are stable
// caller-generated idempotency identities and must be reused on HTTP retry.
func NewApplyReceiptRequest(
	commandID string,
	receiptID string,
	result workspace.ApplyResult,
) (RecordApplyReceiptRequest, error) {
	record := result.Record
	if commandID == "" || receiptID == "" || !record.State.Terminal() || record.Outcome == nil {
		return RecordApplyReceiptRequest{}, fmt.Errorf(
			"%w: commandId, receiptId, and a terminal apply outcome are required",
			ErrInvalidApplyArtifact,
		)
	}
	return RecordApplyReceiptRequest{
		CommandID: commandID, ReceiptID: receiptID,
		ArtifactID: string(record.Intent.Identity.Artifact), ArtifactDigest: record.Intent.ArtifactDigest,
		BaseRevision: record.Intent.BaseRevision, ResultRevision: record.Intent.ResultRevision,
		Decision: record.Outcome.Decision, ObservedRevision: record.Outcome.ObservedRevision,
		Code: record.Outcome.Code, Message: record.Outcome.Message,
	}, nil
}

func metadataString(metadata map[string]any, key string) (string, bool) {
	value, ok := metadata[key].(string)
	return value, ok && value != ""
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
