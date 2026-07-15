package humanmcp

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/a2aadapter"
	delegationworker "github.com/vibe-agi/human/internal/delegation/worker"
	"github.com/vibe-agi/human/internal/patchapply"
	"github.com/vibe-agi/human/internal/worktree"
)

func TestMCPA2AWorkerGitApplyTwoTurnEndToEnd(t *testing.T) {
	ctx := context.Background()
	origin := filepath.Join(t.TempDir(), "origin.git")
	gitMCP(t, "", "init", "--bare", "-q", origin)
	seed := filepath.Join(t.TempDir(), "seed")
	gitMCP(t, "", "clone", "-q", origin, seed)
	gitMCP(t, seed, "config", "user.name", "Seed")
	gitMCP(t, seed, "config", "user.email", "seed@example.test")
	if err := os.WriteFile(filepath.Join(seed, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitMCP(t, seed, "add", "README.md")
	gitMCP(t, seed, "commit", "-q", "-m", "base")
	gitMCP(t, seed, "push", "-q", "origin", "HEAD")
	base := strings.TrimSpace(gitMCP(t, seed, "rev-parse", "HEAD"))
	caller := filepath.Join(t.TempDir(), "caller")
	producer := filepath.Join(t.TempDir(), "producer")
	gitMCP(t, "", "clone", "-q", origin, caller)
	gitMCP(t, "", "clone", "-q", origin, producer)
	gitMCP(t, producer, "config", "user.name", "Human")
	gitMCP(t, producer, "config", "user.email", "human@example.test")

	store, err := delegation.OpenSQLite(ctx, filepath.Join(t.TempDir(), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	httpServer := httptest.NewUnstartedServer(nil)
	baseURL := "http://" + httpServer.Listener.Addr().String()
	a2aServer, err := a2aadapter.NewServer(a2aadapter.ServerConfig{
		Authority: store, Authenticator: a2aadapter.StaticBearerTokens{"caller-token": "caller-1"},
		BaseURL: baseURL, Version: "test", RemoteExec: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer.Config.Handler = a2aServer.Handler()
	httpServer.Start()
	t.Cleanup(httpServer.Close)
	authority, err := NewA2AAuthority(ctx, A2AConfig{
		BaseURL: baseURL, BearerToken: "caller-token", HTTPClient: httpServer.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	applyEngine, err := patchapply.Open(patchapply.Config{
		Repository: caller, StateRoot: filepath.Join(t.TempDir(), "apply-state"), MergirafPath: "-",
	})
	if err != nil {
		t.Fatal(err)
	}
	execRunner := &fakeExecRunner{outcome: ExecOutcome{ExitCode: 0, Stdout: []byte("verified\n")}}
	mcpServer, err := NewServer(Config{
		Authority: authority, PatchApply: applyEngine, ExecRunner: execRunner, Version: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	mcpClient := connectTestClient(t, mcpServer)
	delegated := callTool(t, mcpClient, "human_delegate", map[string]any{
		"prompt": "update the README", "base_commit": base, "idempotency_key": "delegate-1",
	})
	if delegated.IsError {
		t.Fatalf("human_delegate = %+v", delegated)
	}
	tasks, err := store.ListTasks(ctx, "caller-1")
	if err != nil || len(tasks) != 1 {
		t.Fatalf("authority tasks = %+v, %v", tasks, err)
	}
	taskID := tasks[0].ID
	worktrees, err := worktree.Open(worktree.Config{
		Repository: producer, WorktreeRoot: filepath.Join(t.TempDir(), "worktrees"),
		AuthorName: "Human", AuthorEmail: "human@example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := delegationworker.New(delegationworker.Config{
		Authority: store, Worktrees: worktrees, WorkerID: "worker-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := worker.Accept(ctx, delegationworker.AcceptInput{
		TaskID: taskID, ExpectedRevision: tasks[0].Revision, BaseCommit: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	requestedExec, err := store.RequestExec(ctx, delegation.RequestExecInput{
		CommandInput: delegation.CommandInput{TaskID: taskID, ExpectedRevision: accepted.Task.Revision},
		WorkerID:     "worker-1", RequestID: "exec-e2e-1", Command: "go test ./...",
		Reason: "verify before delivery", TimeoutMS: 30_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	pendingExec := callTool(t, mcpClient, "human_exec_pending", map[string]any{"task_id": taskID})
	if pendingExec.IsError {
		t.Fatalf("human_exec_pending = %+v", pendingExec)
	}
	approveArgs := map[string]any{
		"task_id": taskID, "request_id": "exec-e2e-1", "idempotency_key": "approve-e2e-1",
	}
	approvedExec := callTool(t, mcpClient, "human_exec_approve", approveArgs)
	if approvedExec.IsError || execRunner.calls != 1 {
		t.Fatalf("human_exec_approve = %+v, calls=%d", approvedExec, execRunner.calls)
	}
	execRequests, err := store.ListExecRequests(ctx, taskID)
	if err != nil || len(execRequests) != 1 || execRequests[0].Status != delegation.ExecCompleted ||
		string(execRequests[0].Stdout) != "verified\n" {
		t.Fatalf("authority exec result = %+v, %v", execRequests, err)
	}
	revisionAfterExec := execRequests[0].ResolutionSequence
	approvedReplay := callTool(t, mcpClient, "human_exec_approve", approveArgs)
	if approvedReplay.IsError || execRunner.calls != 1 {
		t.Fatalf("human_exec_approve replay = %+v, calls=%d", approvedReplay, execRunner.calls)
	}
	currentAfterExec, err := store.GetTask(ctx, taskID)
	if err != nil || revisionAfterExec == nil || currentAfterExec.Revision != *revisionAfterExec ||
		currentAfterExec.Revision != requestedExec.Task.Revision+1 {
		t.Fatalf("exec replay advanced authority = %+v, request=%+v, %v", currentAfterExec, requestedExec, err)
	}
	if err := os.WriteFile(filepath.Join(accepted.Worktree.Path, "README.md"), []byte("turn one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := worker.Deliver(ctx, delegationworker.DeliverInput{
		TaskID: taskID, ExpectedRevision: currentAfterExec.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Task.State != delegation.StateInputRequired {
		t.Fatalf("first delivery state = %s", first.Task.State)
	}
	firstStatus := callTool(t, mcpClient, "human_status", map[string]any{"task_id": taskID})
	firstCAS := statusArtifactCAS(t, firstStatus, 1)
	result := callTool(t, mcpClient, "human_result", map[string]any{
		"task_id": taskID, "apply": true, "expected_turn": 1,
		"expected_sha256": firstCAS,
	})
	if result.IsError {
		t.Fatalf("first human_result = %+v", result)
	}
	assertMCPFile(t, filepath.Join(caller, "README.md"), "turn one\n")

	reply := callTool(t, mcpClient, "human_reply", map[string]any{
		"task_id": taskID, "message": "make one more change", "idempotency_key": "reply-1",
	})
	if reply.IsError {
		t.Fatalf("human_reply = %+v", reply)
	}
	working, err := store.GetTask(ctx, taskID)
	if err != nil || working.State != delegation.StateWorking {
		t.Fatalf("working task = %+v, %v", working, err)
	}
	if err := os.WriteFile(filepath.Join(accepted.Worktree.Path, "README.md"), []byte("turn two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := worker.Deliver(ctx, delegationworker.DeliverInput{
		TaskID: taskID, ExpectedRevision: working.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondStatus := callTool(t, mcpClient, "human_status", map[string]any{"task_id": taskID})
	secondCAS := statusArtifactCAS(t, secondStatus, 2)
	result = callTool(t, mcpClient, "human_result", map[string]any{
		"task_id": taskID, "apply": true, "expected_turn": 2,
		"expected_sha256": secondCAS,
	})
	if result.IsError {
		t.Fatalf("second human_result = %+v", result)
	}
	assertMCPFile(t, filepath.Join(caller, "README.md"), "turn two\n")
	completed, err := store.CompleteTask(ctx, delegation.CommandInput{
		TaskID: taskID, ExpectedRevision: second.Task.Revision,
	})
	if err != nil || completed.Task.State != delegation.StateCompleted {
		t.Fatalf("completed = %+v, %v", completed, err)
	}
	status := callTool(t, mcpClient, "human_status", map[string]any{"task_id": taskID})
	if status.IsError {
		t.Fatalf("completed human_status = %+v", status)
	}
}

func statusArtifactCAS(t *testing.T, callResult *mcp.CallToolResult, expectedTurn int) string {
	t.Helper()
	if callResult == nil || callResult.IsError {
		t.Fatalf("human_status result = %#v", callResult)
	}
	payload, err := json.Marshal(callResult.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var output taskOutput
	if err := json.Unmarshal(payload, &output); err != nil {
		t.Fatalf("decode human_status structured output: %v", err)
	}
	if output.Task.Artifact == nil || output.Task.Artifact.Turn != expectedTurn || output.Task.Artifact.SHA256 == "" {
		t.Fatalf("human_status artifact CAS = %+v", output.Task.Artifact)
	}
	return output.Task.Artifact.SHA256
}

func assertMCPFile(t *testing.T, path, want string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil || string(content) != want {
		t.Fatalf("%s = %q, %v; want %q", path, content, err, want)
	}
}
