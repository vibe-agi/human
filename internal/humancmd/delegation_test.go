package humancmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/delegation"
	delegationworkerws "github.com/vibe-agi/human/internal/delegation/workerws"
)

type delegationCLIAuth struct{}

func (delegationCLIAuth) Authenticate(_ context.Context, token string) (auth.Principal, error) {
	if token != "worker-token" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return auth.Principal{Type: auth.PrincipalWorker, SubjectID: "cli-worker"}, nil
}

func TestDelegationCLIRecoverAcceptDeliverRewindCompleteAndReject(t *testing.T) {
	ctx := context.Background()
	store, err := delegation.OpenSQLite(ctx, filepath.Join(t.TempDir(), "delegation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	repository, base := newDelegationCLIRepository(t)
	metadata, err := json.Marshal(map[string]any{
		delegation.RequestMetadataKey: map[string]any{"baseCommit": base},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, taskID := range []string{"work-task", "reject-task"} {
		if _, err := store.CreateTask(ctx, delegation.CreateTaskInput{
			ID: taskID, CallerID: "caller-1", Metadata: metadata,
		}); err != nil {
			t.Fatal(err)
		}
	}
	workerServer, err := delegationworkerws.New(
		delegationworkerws.Config{SnapshotPoll: 10 * time.Millisecond, RemoteExec: true}, delegationCLIAuth{}, store,
	)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(workerServer)
	defer server.Close()
	outbox := filepath.Join(t.TempDir(), "delegation-outbox.db")
	worktreeRoot := filepath.Join(t.TempDir(), "worktrees")
	common := []string{
		"delegation",
		"--gateway", "ws" + strings.TrimPrefix(server.URL, "http") + delegationworkerws.DefaultPath,
		"--token", "worker-token",
		"--outbox", outbox,
		"--repository", repository,
		"--worktree-root", worktreeRoot,
		"--author-name", "CLI Human",
		"--author-email", "cli-human@example.test",
		"--timeout", "5s",
	}

	output, err := executeHumanCLI(ctx, append(common, "tasks", "--settle", "50ms")...)
	if err != nil {
		t.Fatal(err)
	}
	var recovered struct {
		WorkerID string               `json:"worker_id"`
		Tasks    []delegationTaskView `json:"tasks"`
	}
	if err := json.Unmarshal([]byte(output), &recovered); err != nil {
		t.Fatalf("decode tasks %q: %v", output, err)
	}
	if recovered.WorkerID != "cli-worker" || len(recovered.Tasks) != 2 ||
		recovered.Tasks[0].BaseCommit != base {
		t.Fatalf("recovered = %#v", recovered)
	}

	output, err = executeHumanCLI(ctx, append(common, "accept", "work-task")...)
	if err != nil {
		t.Fatal(err)
	}
	var accepted struct {
		Task struct {
			State    delegation.State `json:"state"`
			WorkerID string           `json:"worker_id"`
		} `json:"task"`
		Worktree struct {
			Path string `json:"path"`
		} `json:"worktree"`
	}
	if err := json.Unmarshal([]byte(output), &accepted); err != nil {
		t.Fatalf("decode accept %q: %v", output, err)
	}
	if accepted.Task.State != delegation.StateWorking || accepted.Task.WorkerID != "cli-worker" ||
		accepted.Worktree.Path != filepath.Join(worktreeRoot, "work-task") {
		t.Fatalf("accepted = %#v", accepted)
	}
	execArgs := append(common, "exec", "work-task",
		"--request-id", "cli-exec-1",
		"--command", "git status --short",
		"--cwd", ".",
		"--command-timeout", "1500ms",
		"--reason", "inspect the delegated workspace")
	output, err = executeHumanCLI(ctx, execArgs...)
	if err != nil {
		t.Fatal(err)
	}
	var execResult delegation.ExecResult
	if err := json.Unmarshal([]byte(output), &execResult); err != nil {
		t.Fatalf("decode exec %q: %v", output, err)
	}
	if execResult.Replay || execResult.Task.State != delegation.StateWorking || execResult.Task.Revision != 3 ||
		execResult.Request.WorkerID != "cli-worker" || execResult.Request.Command != "git status --short" ||
		execResult.Request.CWD != "." || execResult.Request.TimeoutMS != 1500 ||
		execResult.Request.Reason != "inspect the delegated workspace" {
		t.Fatalf("exec result = %#v", execResult)
	}
	// A fresh CLI process uses a new transport event, while the explicit
	// request id makes the authority request itself replay-safe.
	output, err = executeHumanCLI(ctx, execArgs...)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(output), &execResult); err != nil {
		t.Fatalf("decode replayed exec %q: %v", output, err)
	}
	requests, err := store.ListExecRequests(ctx, "work-task")
	if err != nil || !execResult.Replay || execResult.Task.Revision != 3 || len(requests) != 1 {
		t.Fatalf("replayed exec = %#v, requests = %#v, error = %v", execResult, requests, err)
	}
	if _, err := store.ResolveExec(ctx, delegation.ResolveExecInput{
		CommandInput: delegation.CommandInput{TaskID: "work-task", ExpectedRevision: execResult.Task.Revision},
		RequestID:    "cli-exec-1", ResolutionID: "cli-deny-1", Error: "not needed for this fixture",
	}); err != nil {
		t.Fatal(err)
	}
	readme := filepath.Join(accepted.Worktree.Path, "README.md")
	if err := os.WriteFile(readme, []byte("turn one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := executeHumanCLI(ctx, append(common, "deliver", "work-task", "--note", "first turn")...); err != nil {
		t.Fatal(err)
	}
	first, err := store.GetArtifact(ctx, "work-task", 1)
	if err != nil || !bytes.Contains(first.Data, []byte("turn one")) {
		t.Fatalf("first artifact = %#v, error = %v", first, err)
	}
	current, err := store.GetTask(ctx, "work-task")
	if err != nil {
		t.Fatal(err)
	}
	replied, err := store.AppendMessage(ctx,
		delegation.CommandInput{TaskID: "work-task", ExpectedRevision: current.Revision},
		delegation.MessageInput{ID: "reply-1", Role: "user", Data: []byte(`{"text":"continue"}`)})
	if err != nil || replied.Task.State != delegation.StateWorking {
		t.Fatalf("reply = %#v, error = %v", replied, err)
	}
	if err := os.WriteFile(readme, []byte("turn two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := executeHumanCLI(ctx, append(common, "deliver", "work-task")...); err != nil {
		t.Fatal(err)
	}
	current, err = store.GetTask(ctx, "work-task")
	if err != nil {
		t.Fatal(err)
	}
	requested, err := store.RequestRewind(ctx, delegation.RequestRewindInput{
		CommandInput: delegation.CommandInput{TaskID: "work-task", ExpectedRevision: current.Revision},
		TargetTurn:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executeHumanCLI(ctx, append(common, "rewind", "confirm", "work-task", "--to-turn", "1")...); err != nil {
		t.Fatal(err)
	}
	rewound, err := store.GetTask(ctx, "work-task")
	if err != nil || rewound.Revision != requested.Task.Revision+1 || rewound.LatestTurn != 1 ||
		rewound.State != delegation.StateInputRequired {
		t.Fatalf("rewound = %#v, error = %v", rewound, err)
	}
	content, err := os.ReadFile(readme)
	if err != nil || string(content) != "turn one\n" {
		t.Fatalf("rewound content = %q, error = %v", content, err)
	}
	if _, err := executeHumanCLI(ctx, append(common, "complete", "work-task", "--reason", "done")...); err != nil {
		t.Fatal(err)
	}
	completed, err := store.GetTask(ctx, "work-task")
	if err != nil || completed.State != delegation.StateCompleted {
		t.Fatalf("completed = %#v, error = %v", completed, err)
	}
	if _, err := os.Stat(accepted.Worktree.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed worktree was not archived: %v", err)
	}
	if keep := strings.TrimSpace(gitCLI(t, repository, "show-ref", "--hash", "refs/human/keep/work-task/completed")); keep == "" {
		t.Fatal("completed worktree has no durable keep ref")
	}

	if _, err := executeHumanCLI(ctx, append(common, "reject", "reject-task", "--reason", "no capacity")...); err != nil {
		t.Fatal(err)
	}
	rejected, err := store.GetTask(ctx, "reject-task")
	if err != nil || rejected.State != delegation.StateRejected {
		t.Fatalf("rejected = %#v, error = %v", rejected, err)
	}
}

func TestDelegationCLIConfigurationAndMetadata(t *testing.T) {
	t.Parallel()
	command := New()
	delegationCommand, _, err := command.Find([]string{"delegation"})
	if err != nil {
		t.Fatal(err)
	}
	if flag := delegationCommand.PersistentFlags().Lookup("gateway"); flag == nil || flag.DefValue != defaultDelegationGateway {
		t.Fatalf("delegation gateway flag = %#v", flag)
	}
	for _, path := range [][]string{{"tasks"}, {"accept"}, {"deliver"}, {"exec"}, {"reject"}, {"complete"}, {"fail"}, {"rewind", "confirm"}, {"rewind", "reject"}} {
		if _, _, err := delegationCommand.Find(path); err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
	}
	metadata, err := json.Marshal(map[string]any{
		delegation.RequestMetadataKey: map[string]any{"baseCommit": "abc123"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := taskBaseCommit(metadata); got != "abc123" {
		t.Fatalf("base commit = %q", got)
	}
	if got := taskBaseCommit([]byte(`{"invalid":true}`)); got != "" {
		t.Fatalf("unexpected base commit = %q", got)
	}
}

func executeHumanCLI(ctx context.Context, args ...string) (string, error) {
	command := New()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs(args)
	err := command.ExecuteContext(ctx)
	return output.String(), err
}

func newDelegationCLIRepository(t *testing.T) (string, string) {
	t.Helper()
	repository := filepath.Join(t.TempDir(), "repository")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	gitCLI(t, repository, "init", "-q")
	gitCLI(t, repository, "config", "user.name", "CLI Test")
	gitCLI(t, repository, "config", "user.email", "cli@example.test")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitCLI(t, repository, "add", "README.md")
	gitCLI(t, repository, "commit", "-q", "-m", "base")
	return repository, strings.TrimSpace(gitCLI(t, repository, "rev-parse", "HEAD"))
}

func gitCLI(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return string(output)
}
