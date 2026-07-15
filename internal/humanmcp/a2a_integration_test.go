package humanmcp

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/a2aadapter"
)

func TestOfficialA2AClientServerDelegationRoundTrip(t *testing.T) {
	ctx := context.Background()
	baseCommit := strings.Repeat("1", 40)
	deliveryCommit := strings.Repeat("2", 40)
	blob := strings.Repeat("3", 40)
	store, err := delegation.OpenSQLite(ctx, filepath.Join(t.TempDir(), "delegation.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	testServer := httptest.NewUnstartedServer(nil)
	baseURL := "http://" + testServer.Listener.Addr().String()
	adapter, err := a2aadapter.NewServer(a2aadapter.ServerConfig{
		Authority: store, Authenticator: a2aadapter.StaticBearerTokens{"caller-token": "caller-1"},
		BaseURL: baseURL, Version: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	testServer.Config.Handler = adapter.Handler()
	testServer.Start()
	t.Cleanup(testServer.Close)
	authority, err := NewA2AAuthority(ctx, A2AConfig{
		BaseURL: baseURL, BearerToken: "caller-token", HTTPClient: testServer.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := authority.Delegate(ctx, DelegateInput{
		Prompt: "change the readme", BaseCommit: baseCommit, WorkspaceDigest: "tree",
		IdempotencyKey: "message-1",
	})
	if err != nil || created.State != StateSubmitted || created.Revision != 1 {
		t.Fatalf("created = %+v, %v", created, err)
	}
	stored, err := store.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := store.AcceptTask(ctx, delegation.AcceptTaskInput{
		CommandInput: delegation.CommandInput{TaskID: stored.ID, ExpectedRevision: stored.Revision},
		WorkerID:     "worker-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	artifactMetadata, err := json.Marshal(map[string]any{
		"schema": "human-agent.git-patch.v1", "task_id": created.ID, "turn": 1,
		"base_commit": baseCommit, "commit": deliveryCommit, "incremental_patch": []byte("incremental"),
		"files": []map[string]string{{"path": "README.md", "blob_sha": blob, "mode": "100644"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeliverTurn(ctx, delegation.DeliverTurnInput{
		CommandInput: delegation.CommandInput{TaskID: created.ID, ExpectedRevision: accepted.Task.Revision},
		ArtifactID:   "artifact-1", ArtifactMediaType: delegation.GitPatchMediaType,
		ArtifactData: []byte("cumulative"), ArtifactMetadata: artifactMetadata,
	}); err != nil {
		t.Fatal(err)
	}
	delivered, err := authority.GetTask(ctx, created.ID)
	if err != nil || delivered.State != StateInputRequired || delivered.Artifact == nil ||
		string(delivered.Artifact.CumulativePatch) != "cumulative" ||
		string(delivered.Artifact.IncrementalPatch) != "incremental" {
		t.Fatalf("delivered = %+v, %v", delivered, err)
	}
	listed, err := authority.ListTasks(ctx)
	if err != nil || len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("listed = %+v, %v", listed, err)
	}
	replied, err := authority.Reply(ctx, created.ID, "please continue", "message-2")
	if err != nil || replied.State != StateWorking {
		t.Fatalf("replied = %+v, %v", replied, err)
	}
	canceled, err := authority.Cancel(ctx, created.ID, "stop", "cancel-1")
	if err != nil || canceled.State != StateCanceled {
		t.Fatalf("canceled = %+v, %v", canceled, err)
	}
	// Repeat cancellation reconciles the terminal state instead of issuing a
	// second non-idempotent protocol transition.
	if replay, err := authority.Cancel(ctx, created.ID, "stop", "cancel-1"); err != nil || replay.State != StateCanceled {
		t.Fatalf("cancel replay = %+v, %v", replay, err)
	}
}
