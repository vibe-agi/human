package agent_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	. "github.com/vibe-agi/human/agent"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	agentsqlite "github.com/vibe-agi/human/agent/sqlite"
	"github.com/vibe-agi/human/workspace"
)

func TestOpenCreatesPrivateDatabaseParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "private", "agent.db")
	resource, err := agentsqlite.Open(context.Background(), agentsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := resource.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("database parent is not a directory: %s", info.Mode())
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o700 {
		t.Fatalf("database parent mode = %04o, want 0700", info.Mode().Perm())
	}
}

func TestOpenRejectsOldAgentSchemaFingerprint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		CREATE TABLE human_schema (
		  component TEXT PRIMARY KEY,
		  version INTEGER NOT NULL,
		  fingerprint TEXT NOT NULL
		);
		INSERT INTO human_schema (component, version, fingerprint)
		VALUES ('agent', 1, 'human-agent-v1-20260718d');
	`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	resource, err := agentsqlite.Open(context.Background(), agentsqlite.Config{Path: path})
	if err == nil {
		_ = resource.Release(context.Background())
		t.Fatal("SQLite adapter accepted an old clean-break schema fingerprint")
	}
	if !errors.Is(err, agentsqlite.ErrUnsupportedSchema) {
		t.Fatalf("old schema error = %v", err)
	}
}

func TestCommandReplayRejectsValidResultTampering(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "context", "workspace", "task")
	command := createCommand("create", contextRef, taskRef, "message", "start")
	if _, err := service.CreateTask(ctx, command); err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	if err := service.database.QueryRowContext(ctx, `
		SELECT result FROM agent_commands
		WHERE authority_id = ? AND command_id = ?`, taskRef.Workspace.Authority, command.Meta.ID,
	).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var forged Task
	if err := json.Unmarshal(encoded, &forged); err != nil {
		t.Fatal(err)
	}
	forged.Revision = 2
	forged.EventCount = 2
	encoded, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.database.ExecContext(ctx, `
		UPDATE agent_commands SET result = ?
		WHERE authority_id = ? AND command_id = ?`, encoded,
		taskRef.Workspace.Authority, command.Meta.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTask(ctx, command); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("tampered command replay error = %v", err)
	}
}

func TestHistoryPagesRejectSequenceGaps(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "context", "workspace", "task")
	task := createWorkingTask(t, service, contextRef, taskRef, "history")
	task, err := service.RequestInput(ctx, WorkerMessageCommand{
		Meta: workerMeta(t, service, taskRef, "ask", task.Revision), Task: taskRef,
		Message: textMessage("question", "which environment?"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReplyTask(ctx, MessageCommand{
		Meta: CommandMeta{ID: "reply", ExpectedRevision: task.Revision}, Task: taskRef,
		Message: textMessage("answer", "staging"),
	}); err != nil {
		t.Fatal(err)
	}
	messages, err := service.ListMessages(ctx, taskRef, PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if messages.HasMore || messages.Next != 3 {
		t.Fatalf("terminal message cursor = %#v", messages)
	}
	events, err := service.ReadEvents(ctx, taskRef, PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if events.HasMore || events.Next != 4 {
		t.Fatalf("terminal event cursor = %#v", events)
	}
	if _, err := service.database.ExecContext(ctx, `
		UPDATE agent_messages SET author = 'agent'
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND sequence = 1`,
		taskRef.Workspace.Authority, taskRef.Workspace.ID, taskRef.ID,
	); err == nil {
		t.Fatal("message role parity corruption passed SQLite constraint")
	}

	if _, err := service.database.ExecContext(ctx, `
		DELETE FROM agent_messages
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND sequence = 2`,
		taskRef.Workspace.Authority, taskRef.Workspace.ID, taskRef.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListMessages(ctx, taskRef, PageRequest{}); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("gapped message history error = %v", err)
	}
	if _, err := service.database.ExecContext(ctx, `
		DELETE FROM agent_events
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND sequence = 2`,
		taskRef.Workspace.Authority, taskRef.Workspace.ID, taskRef.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReadEvents(ctx, taskRef, PageRequest{}); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("gapped event history error = %v", err)
	}
}

func TestClockRollbackCannotCommitUnreadableState(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	clock := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return clock }
	contextRef, taskRef := refs("tenant-a", "context", "workspace", "task")
	created, err := service.CreateTask(ctx, createCommand("create", contextRef, taskRef, "message", "start"))
	if err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(-time.Hour)
	grant := acquireTestLease(t, service, taskRef)
	working, err := service.AcceptTask(ctx, WorkerTaskCommand{
		Meta: WorkerCommandMeta{ID: "accept", ExpectedRevision: created.Revision, Grant: grant}, Task: taskRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	if working.UpdatedAt.Before(created.UpdatedAt) {
		t.Fatalf("clock rollback regressed task timestamp: %s < %s", working.UpdatedAt, created.UpdatedAt)
	}
	if _, err := service.GetTask(ctx, taskRef); err != nil {
		t.Fatalf("read after clock rollback: %v", err)
	}
	frozen, err := service.FreezeArtifact(ctx, freezeCommand(
		t, service, "clock", working, "clock-base", []byte(`{"edit":"clock"}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	artifactRef := frozen.Artifact.Ref
	if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: workerMeta(t, service, taskRef, "complete", frozen.Task.Revision),
		Task: taskRef, Submission: "submission", Artifact: &artifactRef,
		Message: textMessage("final", "done"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RecordApplyReceipt(ctx, RecordApplyReceiptCommand{
		CommandID: "receipt-command", Receipt: "receipt", Artifact: artifactRef,
		ArtifactDigest: frozen.Artifact.Digest, BaseRevision: frozen.Artifact.BaseRevision,
		ResultRevision: frozen.Artifact.ResultRevision, ObservedRevision: frozen.Artifact.ResultRevision,
		Decision: workspace.ApplySuccess,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetArtifact(ctx, artifactRef); err != nil {
		t.Fatalf("read Artifact after clock rollback: %v", err)
	}
	if _, err := service.GetApplyReceipt(ctx, artifactRef); err != nil {
		t.Fatalf("read receipt after clock rollback: %v", err)
	}
}
