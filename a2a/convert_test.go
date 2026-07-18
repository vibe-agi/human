package a2a

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/workspace"
)

func TestA2AMessagePartOneofRoundTripsCanonicalCombinations(t *testing.T) {
	tests := []struct {
		name string
		part *sdk.Part
		kind string
	}{
		{name: "text default", part: sdk.NewTextPart("hello"), kind: "text"},
		{
			name: "text explicit", part: &sdk.Part{
				Content: sdk.Text("# heading"), MediaType: "text/markdown; charset=utf-8",
			}, kind: "text",
		},
		{name: "raw default", part: sdk.NewRawPart([]byte{0, 1, 0xff}), kind: "raw"},
		{
			name: "raw explicit", part: &sdk.Part{
				Content: sdk.Raw([]byte("opaque")), MediaType: "image/png",
			}, kind: "raw",
		},
		{name: "data default", part: sdk.NewDataPart(map[string]any{"b": 2, "a": "one"}), kind: "data"},
		{
			name: "data explicit", part: &sdk.Part{
				Content:   sdk.Data{Value: []any{true, nil, "value"}},
				MediaType: "application/json; charset=utf-8",
			}, kind: "data",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first, err := fromA2AMessage(&sdk.Message{
				ID: "message-one", Role: sdk.MessageRoleUser,
				Parts: sdk.ContentParts{test.part},
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(first.Parts) != 1 {
				t.Fatalf("parts = %d, want 1", len(first.Parts))
			}
			outbound := toA2APart(first.Parts[0].MediaType, first.Parts[0].Data)
			if got := a2aPartKind(outbound); got != test.kind {
				t.Fatalf("outbound oneof = %q, want %q", got, test.kind)
			}
			second, err := fromA2AMessage(&sdk.Message{
				ID: "message-one", Role: sdk.MessageRoleUser,
				Parts: sdk.ContentParts{outbound},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !sameParts(first.Parts, second.Parts) {
				t.Fatalf("transport-neutral parts changed:\n first=%#v\nsecond=%#v", first.Parts, second.Parts)
			}
		})
	}
}

func TestA2AMessagePartRejectsOneofMediaTypeAmbiguity(t *testing.T) {
	tests := []struct {
		name string
		part *sdk.Part
	}{
		{
			name: "text with binary media type",
			part: &sdk.Part{Content: sdk.Text("hello"), MediaType: "application/octet-stream"},
		},
		{
			name: "raw with text media type",
			part: &sdk.Part{Content: sdk.Raw("hello"), MediaType: "text/plain"},
		},
		{
			name: "raw with json media type",
			part: &sdk.Part{Content: sdk.Raw(`{"exact": true}`), MediaType: "application/json"},
		},
		{
			name: "data with non-json media type",
			part: &sdk.Part{Content: sdk.Data{Value: map[string]any{"ok": true}}, MediaType: "application/cbor"},
		},
		{
			name: "surrounding media type whitespace",
			part: &sdk.Part{Content: sdk.Text("hello"), MediaType: " text/plain"},
		},
		{
			name: "invalid utf8 text",
			part: &sdk.Part{Content: sdk.Text(string([]byte{0xff})), MediaType: "text/plain"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := fromA2AMessage(&sdk.Message{
				ID: "message-one", Role: sdk.MessageRoleUser,
				Parts: sdk.ContentParts{test.part},
			})
			if err == nil {
				t.Fatal("ambiguous part was accepted")
			}
		})
	}
}

func TestA2AArtifactPreservesExactPayloadBytesAndDeclaresActiveExtension(t *testing.T) {
	payload := []byte("{\n  \"z\": 1, \"a\": [2, 3]\n}\n")
	content := agent.ArtifactContent{
		Artifact: agent.Artifact{
			Ref: agent.ArtifactRef{
				Workspace: agent.WorkspaceRef{Authority: "authority-a", ID: "workspace-a"},
				ID:        "artifact-a",
			},
			Digest:         "sha256:artifact",
			PayloadDigest:  "sha256:payload",
			BaseRevision:   "sha256:base",
			ResultRevision: "sha256:result",
		},
		Payload: workspace.Payload{MediaType: "application/json", Data: payload},
	}

	plain := toA2AArtifact(content, false)
	if len(plain.Extensions) != 0 || len(plain.Metadata) != 0 {
		t.Fatalf("inactive extension leaked: extensions=%#v metadata=%#v", plain.Extensions, plain.Metadata)
	}
	assertExactRawArtifactPayload(t, plain, payload)

	active := toA2AArtifact(content, true)
	if !reflect.DeepEqual(active.Extensions, []string{WorkspaceExtensionURI}) {
		t.Fatalf("artifact extensions = %#v", active.Extensions)
	}
	if _, ok := active.Metadata[WorkspaceExtensionURI]; !ok {
		t.Fatalf("artifact metadata = %#v", active.Metadata)
	}
	assertExactRawArtifactPayload(t, active, payload)

	wire, err := json.Marshal(active)
	if err != nil {
		t.Fatal(err)
	}
	var decoded sdk.Artifact
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	assertExactRawArtifactPayload(t, &decoded, payload)
}

func TestWorkspaceMetadataRequiresActivatedExtension(t *testing.T) {
	task := agent.Task{
		Ref: agent.TaskRef{
			Workspace: agent.WorkspaceRef{Authority: "authority-a", ID: "workspace-a"},
			ID:        "task-a",
		},
		Context:   agent.ContextRef{Authority: "authority-a", ID: "context-a"},
		State:     agent.TaskSubmitted,
		UpdatedAt: time.Unix(1, 0).UTC(),
	}
	handler := &requestHandler{}

	requestedOnly, _ := a2asrv.NewCallContext(context.Background(), a2asrv.NewServiceParams(map[string][]string{
		sdk.SvcParamExtensions: {WorkspaceExtensionURI},
	}))
	withoutActivation, err := handler.convertTask(requestedOnly, task, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(withoutActivation.Metadata) != 0 {
		t.Fatalf("requested but inactive extension leaked metadata: %#v", withoutActivation.Metadata)
	}

	activeContext, call := a2asrv.NewCallContext(context.Background(), a2asrv.NewServiceParams(map[string][]string{
		sdk.SvcParamExtensions: {WorkspaceExtensionURI},
	}))
	extension := sdk.AgentExtension{URI: WorkspaceExtensionURI}
	call.Extensions().Activate(&extension)
	withActivation, err := handler.convertTask(activeContext, task, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := withActivation.Metadata[WorkspaceExtensionURI]; !ok {
		t.Fatalf("active extension metadata = %#v", withActivation.Metadata)
	}
}

func assertExactRawArtifactPayload(t *testing.T, artifact *sdk.Artifact, want []byte) {
	t.Helper()
	if len(artifact.Parts) != 1 {
		t.Fatalf("artifact parts = %d, want 1", len(artifact.Parts))
	}
	raw, ok := artifact.Parts[0].Content.(sdk.Raw)
	if !ok {
		t.Fatalf("artifact part oneof = %T, want a2a.Raw", artifact.Parts[0].Content)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("artifact payload changed:\n got %q\nwant %q", raw, want)
	}
	if sha256.Sum256(raw) != sha256.Sum256(want) {
		t.Fatal("artifact exact-byte digest changed")
	}
	if artifact.Parts[0].MediaType != "application/json" {
		t.Fatalf("artifact media type = %q", artifact.Parts[0].MediaType)
	}
}

func a2aPartKind(part *sdk.Part) string {
	switch part.Content.(type) {
	case sdk.Text:
		return "text"
	case sdk.Raw:
		return "raw"
	case sdk.Data:
		return "data"
	case sdk.URL:
		return "url"
	default:
		return "unknown"
	}
}
