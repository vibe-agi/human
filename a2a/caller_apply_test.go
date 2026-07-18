package a2a

import (
	"bytes"
	"errors"
	"testing"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/workspace"
)

func TestDecodeApplyArtifactPreservesExactRawPayload(t *testing.T) {
	payload := workspace.Payload{
		MediaType: "application/json",
		Data:      []byte("{\n  \"z\": 1, \"a\": 2\n}\n"),
	}
	digest := workspace.DigestPayload(payload)
	part := sdk.NewRawPart(payload.Data)
	part.MediaType = payload.MediaType
	artifact := &sdk.Artifact{
		ID: "artifact-a", Extensions: []string{WorkspaceExtensionURI},
		Parts: sdk.ContentParts{part},
		Metadata: map[string]any{WorkspaceExtensionURI: map[string]any{
			"workspaceId": "workspace-a", "artifactDigest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"payloadDigest": string(digest), "baseRevision": "revision-a", "resultRevision": "revision-b",
		}},
	}
	intent, err := DecodeApplyArtifact("authority-a", artifact)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(intent.Payload.Data, payload.Data) || intent.Payload.MediaType != payload.MediaType ||
		intent.PayloadDigest != digest || intent.Identity.Authority != "authority-a" ||
		intent.Identity.Workspace != "workspace-a" || intent.Identity.Artifact != "artifact-a" {
		t.Fatalf("decoded intent = %#v", intent)
	}
	artifact.Parts[0].Content.(sdk.Raw)[0] = 'X'
	if bytes.Equal(intent.Payload.Data, artifact.Parts[0].Content.(sdk.Raw)) {
		t.Fatal("decoded intent aliases wire payload")
	}
}

func TestDecodeApplyArtifactRejectsSemanticOrDigestRoundTrip(t *testing.T) {
	data := sdk.NewDataPart(map[string]any{"a": float64(1)})
	data.MediaType = "application/json"
	artifact := &sdk.Artifact{
		ID: "artifact-a", Extensions: []string{WorkspaceExtensionURI}, Parts: sdk.ContentParts{data},
		Metadata: map[string]any{WorkspaceExtensionURI: map[string]any{
			"workspaceId": "workspace-a", "artifactDigest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"payloadDigest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"baseRevision":  "revision-a", "resultRevision": "revision-b",
		}},
	}
	if _, err := DecodeApplyArtifact("authority-a", artifact); !errors.Is(err, ErrInvalidApplyArtifact) {
		t.Fatalf("Data payload error = %v", err)
	}

	raw := sdk.NewRawPart([]byte("actual"))
	raw.MediaType = "application/octet-stream"
	artifact.Parts = sdk.ContentParts{raw}
	if _, err := DecodeApplyArtifact("authority-a", artifact); !errors.Is(err, ErrInvalidApplyArtifact) {
		t.Fatalf("digest mismatch error = %v", err)
	}
}

func TestNewApplyReceiptRequestRequiresTerminalJournalRecord(t *testing.T) {
	intent := workspace.ApplyIntent{
		Identity:       workspace.ApplyIdentity{Authority: "authority-a", Workspace: "workspace-a", Artifact: "artifact-a"},
		ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PayloadDigest:  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		BaseRevision:   "revision-a", ResultRevision: "revision-b",
	}
	completed := time.Now().UTC()
	result := workspace.ApplyResult{Record: workspace.ApplyRecord{
		Intent: intent, State: workspace.ApplyStateSuccess,
		Outcome:     &workspace.CASOutcome{Decision: workspace.ApplySuccess, ObservedRevision: "revision-b"},
		CompletedAt: &completed,
	}}
	request, err := NewApplyReceiptRequest("command-a", "receipt-a", result)
	if err != nil {
		t.Fatal(err)
	}
	if request.ArtifactID != "artifact-a" || request.Decision != workspace.ApplySuccess ||
		request.ObservedRevision != "revision-b" || request.CommandID != "command-a" || request.ReceiptID != "receipt-a" {
		t.Fatalf("receipt request = %#v", request)
	}
	result.Record.State = workspace.ApplyStatePending
	result.Record.Outcome = nil
	if _, err := NewApplyReceiptRequest("command-a", "receipt-a", result); !errors.Is(err, ErrInvalidApplyArtifact) {
		t.Fatalf("pending result error = %v", err)
	}
}
