package humanmcp

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/vibe-agi/human/internal/patchapply"
	"github.com/vibe-agi/human/internal/worktree"
)

type fakeAuthority struct {
	tasks        map[string]Task
	delegated    DelegateInput
	reply        string
	replyKey     string
	cancel       string
	cancelKey    string
	resolve      ExecResolutionInput
	resolveCalls int
	delegateErr  error
}

func (authority *fakeAuthority) Delegate(_ context.Context, input DelegateInput) (Task, error) {
	if authority.delegateErr != nil {
		return Task{}, authority.delegateErr
	}
	authority.delegated = input
	task := Task{ID: "task-1", State: StateSubmitted, Revision: 1}
	if authority.tasks == nil {
		authority.tasks = make(map[string]Task)
	}
	authority.tasks[task.ID] = task
	return task, nil
}

func (authority *fakeAuthority) GetTask(_ context.Context, id string) (Task, error) {
	task, ok := authority.tasks[id]
	if !ok {
		return Task{}, errors.New("not found")
	}
	return task, nil
}

func (authority *fakeAuthority) ListTasks(context.Context) ([]Task, error) {
	result := make([]Task, 0, len(authority.tasks))
	for _, task := range authority.tasks {
		result = append(result, task)
	}
	return result, nil
}

func (authority *fakeAuthority) Reply(_ context.Context, id, message, idempotencyKey string) (Task, error) {
	authority.reply = message
	authority.replyKey = idempotencyKey
	task := authority.tasks[id]
	task.State = StateWorking
	authority.tasks[id] = task
	return task, nil
}

func (authority *fakeAuthority) Cancel(_ context.Context, id, reason, idempotencyKey string) (Task, error) {
	authority.cancel = reason
	authority.cancelKey = idempotencyKey
	task := authority.tasks[id]
	task.State = StateCanceled
	authority.tasks[id] = task
	return task, nil
}

func (authority *fakeAuthority) ResolveExec(_ context.Context, id string, input ExecResolutionInput) (Task, error) {
	task, ok := authority.tasks[id]
	if !ok {
		return Task{}, errors.New("not found")
	}
	for index := range task.ExecRequests {
		request := &task.ExecRequests[index]
		if request.ID != input.RequestID {
			continue
		}
		if request.Status != ExecPending {
			if request.ResolutionID == input.ResolutionID {
				return task, nil
			}
			return Task{}, errors.New("idempotency conflict")
		}
		authority.resolve = input
		authority.resolveCalls++
		request.ResolutionID = input.ResolutionID
		request.ExitCode = new(int)
		*request.ExitCode = input.ExitCode
		request.StdoutBase64 = base64.StdEncoding.EncodeToString(input.Stdout)
		request.StderrBase64 = base64.StdEncoding.EncodeToString(input.Stderr)
		request.Error = input.Error
		request.Truncated = input.Truncated
		request.TimedOut = input.TimedOut
		if !input.Approved {
			request.Status = ExecDenied
			request.ExitCode = nil
		} else if input.Error != "" || input.TimedOut {
			request.Status = ExecFailed
		} else {
			request.Status = ExecCompleted
		}
		task.Revision++
		authority.tasks[id] = task
		return task, nil
	}
	return Task{}, errors.New("exec request not found")
}

type fakeExecRunner struct {
	calls   int
	outcome ExecOutcome
}

func (runner *fakeExecRunner) Execute(context.Context, ExecRequest) (ExecOutcome, error) {
	runner.calls++
	return runner.outcome, nil
}

func TestOfficialMCPToolsDelegateStatusReplyCancelAndList(t *testing.T) {
	authority := &fakeAuthority{}
	server, err := NewServer(Config{Authority: authority})
	if err != nil {
		t.Fatal(err)
	}
	client := connectTestClient(t, server)
	delegate := callTool(t, client, "human_delegate", map[string]any{
		"prompt": "fix it", "base_commit": "abc", "reference_task_ids": []string{"old", "old", ""},
		"idempotency_key": "delegate-1",
	})
	if delegate.IsError || authority.delegated.Prompt != "fix it" || len(authority.delegated.ReferenceTaskIDs) != 1 {
		t.Fatalf("delegate = %+v, input = %+v", delegate, authority.delegated)
	}
	for _, call := range []struct {
		name string
		args map[string]any
	}{
		{"human_status", map[string]any{"task_id": "task-1"}},
		{"human_tasks", map[string]any{}},
		{"human_reply", map[string]any{"task_id": "task-1", "message": "continue", "idempotency_key": "reply-1"}},
		{"human_cancel", map[string]any{"task_id": "task-1", "reason": "stop", "idempotency_key": "cancel-1"}},
	} {
		result := callTool(t, client, call.name, call.args)
		if result.IsError {
			t.Fatalf("%s = %+v", call.name, result)
		}
	}
	if authority.delegated.IdempotencyKey != "delegate-1" || authority.replyKey != "reply-1" ||
		authority.cancelKey != "cancel-1" || authority.reply != "continue" || authority.cancel != "stop" {
		t.Fatalf("delegate=%q reply=%q/%q cancel=%q/%q", authority.delegated.IdempotencyKey,
			authority.reply, authority.replyKey, authority.cancel, authority.cancelKey)
	}
}

func TestMutationToolsRequireIdempotencyKey(t *testing.T) {
	authority := &fakeAuthority{tasks: map[string]Task{
		"task-1": {ID: "task-1", State: StateWorking, Revision: 1},
	}}
	server, err := NewServer(Config{Authority: authority})
	if err != nil {
		t.Fatal(err)
	}
	client := connectTestClient(t, server)
	for _, call := range []struct {
		name string
		args map[string]any
	}{
		{"human_delegate", map[string]any{"prompt": "fix it"}},
		{"human_reply", map[string]any{"task_id": "task-1", "message": "continue"}},
		{"human_cancel", map[string]any{"task_id": "task-1", "reason": "stop"}},
	} {
		result := callTool(t, client, call.name, call.args)
		if !result.IsError {
			t.Fatalf("%s without idempotency key unexpectedly succeeded: %+v", call.name, result)
		}
	}
	if authority.delegated.Prompt != "" || authority.reply != "" || authority.cancel != "" {
		t.Fatalf("mutation reached authority without idempotency key: %+v", authority)
	}
}

func TestRemoteExecToolsAreDefaultClosedAndApprovalIsExactlyOnce(t *testing.T) {
	authority := &fakeAuthority{tasks: map[string]Task{
		"task-exec": {
			ID: "task-exec", State: StateWorking, Revision: 2,
			ExecRequests: []ExecRequest{{
				TaskID: "task-exec", ID: "exec-1", Command: "printf approved",
				Reason: "verify the change", Status: ExecPending,
			}},
		},
	}}
	closedServer, err := NewServer(Config{Authority: authority})
	if err != nil {
		t.Fatal(err)
	}
	closedClient := connectTestClient(t, closedServer)
	listed, err := closedClient.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range listed.Tools {
		if strings.HasPrefix(tool.Name, "human_exec_") {
			t.Fatalf("default-closed server advertised %q", tool.Name)
		}
	}

	runner := &fakeExecRunner{outcome: ExecOutcome{ExitCode: 0, Stdout: []byte("approved")}}
	server, err := NewServer(Config{Authority: authority, ExecRunner: runner})
	if err != nil {
		t.Fatal(err)
	}
	client := connectTestClient(t, server)
	pending := callTool(t, client, "human_exec_pending", map[string]any{"task_id": "task-exec"})
	if pending.IsError {
		t.Fatalf("human_exec_pending = %+v", pending)
	}
	args := map[string]any{
		"task_id": "task-exec", "request_id": "exec-1", "idempotency_key": "approve-1",
	}
	first := callTool(t, client, "human_exec_approve", args)
	if first.IsError || runner.calls != 1 || authority.resolveCalls != 1 ||
		authority.resolve.ResolutionID != "approve-1" || string(authority.resolve.Stdout) != "approved" {
		t.Fatalf("first approval = %+v, runner=%d resolve=%+v/%d",
			first, runner.calls, authority.resolve, authority.resolveCalls)
	}
	replay := callTool(t, client, "human_exec_approve", args)
	if replay.IsError || runner.calls != 1 || authority.resolveCalls != 1 {
		t.Fatalf("approval replay reran side effect: result=%+v runner=%d resolve=%d",
			replay, runner.calls, authority.resolveCalls)
	}
	changedDecision := callTool(t, client, "human_exec_deny", map[string]any{
		"task_id": "task-exec", "request_id": "exec-1", "idempotency_key": "approve-1", "reason": "changed",
	})
	if !changedDecision.IsError {
		t.Fatalf("same key changed from approve to deny: %+v", changedDecision)
	}
}

func TestHumanResultAppliesAndVerifiesCumulativeArtifact(t *testing.T) {
	authority, applyEngine, artifact, caller := resultFixture(t)
	authority.tasks = map[string]Task{"task-apply": {
		ID: "task-apply", State: StateInputRequired, Revision: 3, LatestTurn: 1, Artifact: &artifact,
	}}
	server, err := NewServer(Config{Authority: authority, PatchApply: applyEngine})
	if err != nil {
		t.Fatal(err)
	}
	client := connectTestClient(t, server)
	digest := artifactSHA256(&artifact)
	missingCAS := callTool(t, client, "human_result", map[string]any{"task_id": "task-apply", "apply": true})
	if !missingCAS.IsError {
		t.Fatalf("human_result without CAS unexpectedly succeeded: %+v", missingCAS)
	}
	staleCAS := callTool(t, client, "human_result", map[string]any{
		"task_id": "task-apply", "apply": true, "expected_turn": 2, "expected_sha256": digest,
	})
	if !staleCAS.IsError {
		t.Fatalf("human_result with stale turn unexpectedly succeeded: %+v", staleCAS)
	}
	badDigest := callTool(t, client, "human_result", map[string]any{
		"task_id": "task-apply", "apply": true, "expected_turn": 1, "expected_sha256": strings.Repeat("0", 64),
	})
	if !badDigest.IsError {
		t.Fatalf("human_result with stale digest unexpectedly succeeded: %+v", badDigest)
	}
	assertFileContent(t, filepath.Join(caller, "README.md"), "base\n")
	applyArgs := map[string]any{
		"task_id": "task-apply", "apply": true, "expected_turn": 1, "expected_sha256": digest,
	}
	result := callTool(t, client, "human_result", applyArgs)
	if result.IsError {
		t.Fatalf("human_result = %+v", result)
	}
	content, err := os.ReadFile(filepath.Join(caller, "README.md"))
	if err != nil || string(content) != "delivered\n" {
		t.Fatalf("applied README = %q, %v", content, err)
	}
	replay := callTool(t, client, "human_result", applyArgs)
	if replay.IsError {
		t.Fatalf("human_result replay = %+v", replay)
	}
}

func TestArtifactCASCoversIncrementalPatchAndFileOracle(t *testing.T) {
	artifact := Artifact{
		ID: "artifact-1", TaskID: "task-1", Turn: 2,
		BaseCommit: strings.Repeat("1", 40), Commit: strings.Repeat("2", 40),
		CumulativePatch: []byte("cumulative"), IncrementalPatch: []byte("incremental"),
		Files: []File{{Path: "README.md", BlobSHA: strings.Repeat("3", 40), Mode: "100644"}},
	}
	baseline := artifactSHA256(&artifact)
	artifact.IncrementalPatch = []byte("tampered incremental")
	if artifactSHA256(&artifact) == baseline {
		t.Fatal("incremental patch was not covered by artifact CAS")
	}
	artifact.IncrementalPatch = []byte("incremental")
	artifact.Files[0].BlobSHA = strings.Repeat("4", 40)
	if artifactSHA256(&artifact) == baseline {
		t.Fatal("file oracle was not covered by artifact CAS")
	}
	artifact.Files[0].BlobSHA = strings.Repeat("3", 40)
	artifact.Files[0].Mode = "100755"
	if artifactSHA256(&artifact) == baseline {
		t.Fatal("file mode oracle was not covered by artifact CAS")
	}
}

func TestToolBusinessErrorsAreVisibleToMCPCaller(t *testing.T) {
	authority := &fakeAuthority{delegateErr: errors.New("authority offline")}
	server, err := NewServer(Config{Authority: authority})
	if err != nil {
		t.Fatal(err)
	}
	client := connectTestClient(t, server)
	result := callTool(t, client, "human_delegate", map[string]any{
		"prompt": "help", "idempotency_key": "delegate-error-1",
	})
	if !result.IsError || len(result.Content) == 0 || !strings.Contains(result.Content[0].(*mcp.TextContent).Text, "authority offline") {
		t.Fatalf("business error result = %+v", result)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil || string(content) != want {
		t.Fatalf("%s = %q, %v; want %q", path, content, err, want)
	}
}

func connectTestClient(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Close()
		_ = serverSession.Wait()
	})
	return clientSession
}

func callTool(t *testing.T, client *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	result, err := client.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func resultFixture(t *testing.T) (*fakeAuthority, *patchapply.Engine, Artifact, string) {
	t.Helper()
	origin := filepath.Join(t.TempDir(), "origin.git")
	gitMCP(t, "", "init", "--bare", "-q", origin)
	seed := filepath.Join(t.TempDir(), "seed")
	gitMCP(t, "", "clone", "-q", origin, seed)
	gitMCP(t, seed, "config", "user.name", "Test")
	gitMCP(t, seed, "config", "user.email", "test@example.test")
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
	worker, err := worktree.Open(worktree.Config{Repository: producer, WorktreeRoot: filepath.Join(t.TempDir(), "wt")})
	if err != nil {
		t.Fatal(err)
	}
	task, err := worker.Create(context.Background(), "task-apply", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("delivered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	delivered, err := worker.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	files := make([]File, len(delivered.Files))
	for index, file := range delivered.Files {
		files[index] = File{Path: file.Path, BlobSHA: file.BlobSHA, Mode: file.Mode}
	}
	applyEngine, err := patchapply.Open(patchapply.Config{Repository: caller, StateRoot: filepath.Join(t.TempDir(), "state")})
	if err != nil {
		t.Fatal(err)
	}
	if err := applyEngine.Bind(context.Background(), task.ID, base); err != nil {
		t.Fatal(err)
	}
	return &fakeAuthority{}, applyEngine, Artifact{
		TaskID: task.ID, Turn: 1, BaseCommit: delivered.BaseCommit, Commit: delivered.Commit,
		CumulativePatch: delivered.CumulativePatch, IncrementalPatch: delivered.IncrementalPatch, Files: files,
	}, caller
}

func gitMCP(t *testing.T, directory string, args ...string) string {
	t.Helper()
	var command *exec.Cmd
	if directory == "" {
		command = exec.Command("git", args...)
	} else {
		command = exec.Command("git", append([]string{"-C", directory}, args...)...)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}
