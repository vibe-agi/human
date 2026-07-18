package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/workspace"
)

func TestA2AArtifactApplyJournalReceiptVerticalSlice(t *testing.T) {
	service := openA2ATestAgent(t)
	content, completed := publishA2ATestArtifact(t, service)
	handler, err := NewHandler(Config{
		Agent: service,
		Card: testAgentCard(
			sdk.AgentExtension{URI: WorkspaceExtensionURI},
			sdk.AgentExtension{URI: ApplyReceiptExtensionURI},
		),
		Authenticate: func(context.Context, *http.Request) (Principal, error) {
			return Principal{Authority: "authority-a", Subject: "caller-a"}, nil
		},
		ResolveWorkspace: func(context.Context, Principal, *sdk.SendMessageRequest) (agent.WorkspaceID, error) {
			return "workspace-a", nil
		},
		AuthorizeApplyReceipt: func(_ context.Context, principal Principal, task agent.Task, request *RecordApplyReceiptRequest) error {
			if principal.Subject != "caller-a" || task.Ref != completed.Ref || request.ArtifactID != string(content.Artifact.Ref.ID) {
				return sdk.ErrUnauthorized
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	get := httptest.NewRequest(http.MethodGet, "/tasks/"+string(completed.Ref.ID), nil)
	get.Header.Set(sdk.SvcParamVersion, string(sdk.Version))
	get.Header.Set(sdk.SvcParamExtensions, WorkspaceExtensionURI)
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("get Task status = %d, body = %s", getResponse.Code, getResponse.Body.String())
	}
	var task sdk.Task
	if err := json.Unmarshal(getResponse.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode Task response: %v: %s", err, getResponse.Body.String())
	}
	if len(task.Artifacts) != 1 {
		t.Fatalf("Task response = %#v", task)
	}
	intent, err := DecodeApplyArtifact("authority-a", task.Artifacts[0])
	if err != nil {
		t.Fatal(err)
	}

	journal, err := workspace.OpenSQLiteApplyJournal(context.Background(), workspace.ApplyJournalConfig{
		DatabasePath: filepath.Join(t.TempDir(), "apply.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Errorf("close apply journal: %v", err)
		}
	})
	applyCalls := 0
	applier := workspace.CASApplierFunc(func(_ context.Context, got workspace.ApplyIntent) (workspace.CASOutcome, error) {
		applyCalls++
		if got.Identity != intent.Identity || !bytes.Equal(got.Payload.Data, content.Payload.Data) {
			t.Fatalf("CAS intent = %#v", got)
		}
		return workspace.CASOutcome{
			Decision: workspace.ApplySuccess, ObservedRevision: got.ResultRevision,
		}, nil
	})
	result, err := journal.Apply(context.Background(), intent, applier)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := journal.Apply(context.Background(), intent, applier)
	if err != nil || !replay.Replay || applyCalls != 1 {
		t.Fatalf("apply replay = %#v, calls=%d, err=%v", replay, applyCalls, err)
	}
	receipt, err := NewApplyReceiptRequest("record-vertical", "receipt-vertical", result)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		post := httptest.NewRequest(
			http.MethodPost,
			"/tasks/"+string(completed.Ref.ID)+":recordApplyReceipt",
			bytes.NewReader(payload),
		)
		post.Header.Set(sdk.SvcParamVersion, string(sdk.Version))
		post.Header.Set(sdk.SvcParamExtensions, ApplyReceiptExtensionURI)
		post.Header.Set("Content-Type", "application/a2a+json")
		postResponse := httptest.NewRecorder()
		handler.ServeHTTP(postResponse, post)
		if postResponse.Code != http.StatusOK {
			t.Fatalf("receipt attempt %d status = %d, body = %s", attempt, postResponse.Code, postResponse.Body.String())
		}
	}
	head, err := service.GetWorkspaceHead(context.Background(), completed.Ref.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if head.ConfirmedRevision != content.Artifact.ResultRevision {
		t.Fatalf("confirmed revision = %q, want %q", head.ConfirmedRevision, content.Artifact.ResultRevision)
	}
}
