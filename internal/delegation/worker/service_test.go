package worker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/worktree"
)

type testEnvironment struct {
	store        *delegation.Store
	engine       *worktree.Engine
	config       worktree.Config
	repository   string
	worktreeRoot string
	base         string
}

func newTestEnvironment(t *testing.T) testEnvironment {
	t.Helper()
	ctx := context.Background()
	store, err := delegation.OpenSQLite(ctx, filepath.Join(t.TempDir(), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	repository := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	git(t, repository, "init", "-q")
	git(t, repository, "config", "user.name", "Test Human")
	git(t, repository, "config", "user.email", "human@example.test")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, repository, "add", "README.md")
	git(t, repository, "commit", "-q", "-m", "base")
	base := strings.TrimSpace(git(t, repository, "rev-parse", "HEAD"))
	worktreeRoot := filepath.Join(t.TempDir(), "worktrees")
	config := worktree.Config{
		Repository: repository, WorktreeRoot: worktreeRoot,
		AuthorName: "Test Human", AuthorEmail: "human@example.test",
	}
	engine, err := worktree.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	return testEnvironment{
		store: store, engine: engine, config: config, repository: repository,
		worktreeRoot: worktreeRoot, base: base,
	}
}

func newService(t *testing.T, authority Authority, engine Worktrees) *Service {
	t.Helper()
	service, err := New(Config{Authority: authority, Worktrees: engine, WorkerID: "worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func createTask(t *testing.T, environment testEnvironment, id string) delegation.Task {
	t.Helper()
	created, err := environment.store.CreateTask(context.Background(), delegation.CreateTaskInput{
		ID: id, CallerID: "caller-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return created.Task
}

func acceptTask(t *testing.T, environment testEnvironment, id string) (*Service, AcceptResult) {
	t.Helper()
	created := createTask(t, environment, id)
	service := newService(t, environment.store, environment.engine)
	accepted, err := service.Accept(context.Background(), AcceptInput{
		TaskID: id, ExpectedRevision: created.Revision, BaseCommit: environment.base,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, accepted
}

func TestServiceEndToEndDeliverRewindAndMonotonicBranch(t *testing.T) {
	environment := newTestEnvironment(t)
	ctx := context.Background()
	service, accepted := acceptTask(t, environment, "task-e2e")
	if accepted.Task.State != delegation.StateWorking || accepted.Worktree.NextTurn != 1 {
		t.Fatalf("accepted = %+v", accepted)
	}

	writeFile(t, accepted.Worktree.Path, "README.md", "turn one\n")
	first, err := service.Deliver(ctx, DeliverInput{
		TaskID: accepted.Task.ID, ExpectedRevision: accepted.Task.Revision, Data: []byte("first"),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertFullIndexArtifact(t, first, 1, environment.base)
	if first.Task.State != delegation.StateInputRequired || first.Task.LatestTurn != 1 {
		t.Fatalf("first task = %+v", first.Task)
	}

	replied, err := environment.store.Reply(ctx, delegation.CommandInput{
		TaskID: first.Task.ID, ExpectedRevision: first.Task.Revision, Data: []byte("continue"),
	})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, accepted.Worktree.Path, "README.md", "turn two\n")
	second, err := service.Deliver(ctx, DeliverInput{
		TaskID: replied.Task.ID, ExpectedRevision: replied.Task.Revision, Data: []byte("second"),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertFullIndexArtifact(t, second, 2, environment.base)

	requested, err := environment.store.RequestRewind(ctx, delegation.RequestRewindInput{
		CommandInput: delegation.CommandInput{
			TaskID: second.Task.ID, ExpectedRevision: second.Task.Revision, Data: []byte("back to one"),
		},
		TargetTurn: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	rewound, err := service.ConfirmRewind(ctx, ConfirmRewindInput{
		TaskID: requested.Task.ID, ExpectedRevision: requested.Task.Revision, TargetTurn: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rewound.Task.LatestTurn != 1 || rewound.Worktree.LatestTurn != 1 || rewound.Worktree.NextTurn != 3 ||
		head(t, accepted.Worktree.Path) != first.WorktreeArtifact.Commit {
		t.Fatalf("rewound = %+v", rewound)
	}
	if _, err := service.Deliver(ctx, DeliverInput{
		TaskID: rewound.Task.ID, ExpectedRevision: requested.Task.Revision,
	}); !errors.Is(err, delegation.ErrRevisionConflict) {
		t.Fatalf("rewind confirmation was mistaken for delivery replay: %v", err)
	}

	replied, err = environment.store.Reply(ctx, delegation.CommandInput{
		TaskID: rewound.Task.ID, ExpectedRevision: rewound.Task.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, accepted.Worktree.Path, "README.md", "branched turn three\n")
	third, err := service.Deliver(ctx, DeliverInput{
		TaskID: replied.Task.ID, ExpectedRevision: replied.Task.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if third.WorktreeArtifact.Turn != 3 || third.WorktreeArtifact.PreviousCommit != first.WorktreeArtifact.Commit ||
		!bytes.Contains(third.StoredArtifact.Data, []byte("branched turn three")) {
		t.Fatalf("post-rewind delivery = %+v", third)
	}
	turns, err := environment.store.ListTurns(ctx, third.Task.ID)
	if err != nil || len(turns) != 3 || !turns[1].Superseded() || turns[2].Superseded() {
		t.Fatalf("authority turns = %+v, %v", turns, err)
	}

	requested, err = environment.store.RequestRewind(ctx, delegation.RequestRewindInput{
		CommandInput: delegation.CommandInput{TaskID: third.Task.ID, ExpectedRevision: third.Task.Revision},
		TargetTurn:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	toBase, err := service.ConfirmRewind(ctx, ConfirmRewindInput{
		TaskID: requested.Task.ID, ExpectedRevision: requested.Task.Revision, TargetTurn: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if toBase.Task.LatestTurn != 0 || toBase.Worktree.LatestTurn != 0 ||
		toBase.Worktree.NextTurn != 4 || head(t, accepted.Worktree.Path) != environment.base {
		t.Fatalf("base rewind = %+v, head=%s", toBase, head(t, accepted.Worktree.Path))
	}
}

func TestAcceptCASFailureCleansWorktreeStatePathAndBranch(t *testing.T) {
	environment := newTestEnvironment(t)
	created := createTask(t, environment, "task-accept-fail")
	injected := errors.New("injected accept CAS failure")
	faults := &faultAuthority{store: environment.store}
	faults.accept = func(context.Context, delegation.AcceptTaskInput) (delegation.TransitionResult, error) {
		return delegation.TransitionResult{}, injected
	}
	service := newService(t, faults, environment.engine)
	if _, err := service.Accept(context.Background(), AcceptInput{
		TaskID: created.ID, ExpectedRevision: created.Revision, BaseCommit: environment.base,
	}); !errors.Is(err, injected) {
		t.Fatalf("accept error = %v", err)
	}
	if _, err := environment.engine.Load(created.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discarded worktree state error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(environment.worktreeRoot, created.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discarded worktree path error = %v", err)
	}
	if gitSucceeds(environment.repository, "show-ref", "--verify", "refs/heads/human/"+created.ID) {
		t.Fatal("rejected accept left its branch behind")
	}
	stored, err := environment.store.GetTask(context.Background(), created.ID)
	if err != nil || stored.State != delegation.StateSubmitted || stored.Revision != created.Revision {
		t.Fatalf("authority after failed accept = %+v, %v", stored, err)
	}
	// Cleanup is complete enough that the exact same accept can be retried.
	if _, err := newService(t, environment.store, environment.engine).Accept(context.Background(), AcceptInput{
		TaskID: created.ID, ExpectedRevision: created.Revision, BaseCommit: environment.base,
	}); err != nil {
		t.Fatalf("accept retry = %v", err)
	}
}

func TestAcceptTreatsLostSuccessResponseAsReplay(t *testing.T) {
	environment := newTestEnvironment(t)
	created := createTask(t, environment, "task-accept-response")
	faults := &faultAuthority{store: environment.store}
	faults.accept = func(ctx context.Context, input delegation.AcceptTaskInput) (delegation.TransitionResult, error) {
		result, err := environment.store.AcceptTask(ctx, input)
		if err != nil {
			return result, err
		}
		return delegation.TransitionResult{}, errors.New("accept response lost")
	}
	result, err := newService(t, faults, environment.engine).Accept(context.Background(), AcceptInput{
		TaskID: created.ID, ExpectedRevision: created.Revision, BaseCommit: environment.base,
	})
	if err != nil || !result.Replay || result.Task.State != delegation.StateWorking {
		t.Fatalf("lost accept response = %+v, %v", result, err)
	}
	if _, err := environment.engine.Load(created.ID); err != nil {
		t.Fatalf("accepted worktree was cleaned: %v", err)
	}
}

func TestDeliverRetriesCommittedTurnAfterAuthorityFailureAndRestart(t *testing.T) {
	environment := newTestEnvironment(t)
	_, accepted := acceptTask(t, environment, "task-deliver-retry")
	writeFile(t, accepted.Worktree.Path, "README.md", "committed once\n")
	injected := errors.New("injected authority transaction failure")
	faults := &faultAuthority{store: environment.store}
	faults.deliver = func(context.Context, delegation.DeliverTurnInput) (delegation.DeliveryResult, error) {
		return delegation.DeliveryResult{}, injected
	}
	service := newService(t, faults, environment.engine)
	input := DeliverInput{TaskID: accepted.Task.ID, ExpectedRevision: accepted.Task.Revision}
	if _, err := service.Deliver(context.Background(), input); !errors.Is(err, injected) {
		t.Fatalf("first delivery error = %v", err)
	}
	local, err := environment.engine.Load(accepted.Task.ID)
	if err != nil || local.LatestTurn != 1 || local.NextTurn != 2 {
		t.Fatalf("local committed state = %+v, %v", local, err)
	}
	if commits := strings.TrimSpace(git(t, local.Path, "rev-list", "--count", environment.base+"..HEAD")); commits != "1" {
		t.Fatalf("commit count after failure = %s", commits)
	}
	stored, err := environment.store.GetTask(context.Background(), accepted.Task.ID)
	if err != nil || stored.State != delegation.StateWorking || stored.LatestTurn != 0 {
		t.Fatalf("authority after failed delivery = %+v, %v", stored, err)
	}

	// Simulate a worker process restart. CurrentArtifact must rebuild the same
	// payload from persisted task state and HEAD, without a second commit.
	reopened, err := worktree.Open(environment.config)
	if err != nil {
		t.Fatal(err)
	}
	retry := newService(t, environment.store, reopened)
	delivered, err := retry.Deliver(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !delivered.Replay || delivered.WorktreeArtifact.Commit != local.LatestCommit || delivered.Task.LatestTurn != 1 {
		t.Fatalf("delivery retry = %+v", delivered)
	}
	if commits := strings.TrimSpace(git(t, local.Path, "rev-list", "--count", environment.base+"..HEAD")); commits != "1" {
		t.Fatalf("commit count after retry = %s", commits)
	}
}

func TestDeliverTreatsLostSuccessResponseAsReplay(t *testing.T) {
	environment := newTestEnvironment(t)
	_, accepted := acceptTask(t, environment, "task-deliver-response")
	writeFile(t, accepted.Worktree.Path, "README.md", "authority committed\n")
	injected := errors.New("response lost after authority commit")
	faults := &faultAuthority{store: environment.store}
	faults.deliver = func(ctx context.Context, input delegation.DeliverTurnInput) (delegation.DeliveryResult, error) {
		result, err := environment.store.DeliverTurn(ctx, input)
		if err != nil {
			return result, err
		}
		return delegation.DeliveryResult{}, injected
	}
	result, err := newService(t, faults, environment.engine).Deliver(context.Background(), DeliverInput{
		TaskID: accepted.Task.ID, ExpectedRevision: accepted.Task.Revision,
	})
	if err != nil || !result.Replay || result.Task.State != delegation.StateInputRequired {
		t.Fatalf("lost response reconciliation = %+v, %v", result, err)
	}
	turns, err := environment.store.ListTurns(context.Background(), accepted.Task.ID)
	if err != nil || len(turns) != 1 {
		t.Fatalf("turns = %+v, %v", turns, err)
	}
}

func TestCompletePreflightArchivesAndRetriesCleanupAfterAuthorityCommit(t *testing.T) {
	environment := newTestEnvironment(t)
	service, accepted := acceptTask(t, environment, "task-complete")
	writeFile(t, accepted.Worktree.Path, "README.md", "final\n")
	delivered, err := service.Deliver(context.Background(), DeliverInput{
		TaskID: accepted.Task.ID, ExpectedRevision: accepted.Task.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("archive unavailable")
	failingWorktrees := &faultWorktrees{Worktrees: environment.engine, completeErr: injected}
	failingService := newService(t, environment.store, failingWorktrees)
	input := CompleteInput{TaskID: delivered.Task.ID, ExpectedRevision: delivered.Task.Revision}
	if _, err := failingService.Complete(context.Background(), input); !errors.Is(err, injected) {
		t.Fatalf("cleanup failure = %v", err)
	}
	authoritative, err := environment.store.GetTask(context.Background(), delivered.Task.ID)
	if err != nil || authoritative.State != delegation.StateCompleted {
		t.Fatalf("authority was not durably completed: %+v, %v", authoritative, err)
	}
	result, err := service.Complete(context.Background(), input)
	if err != nil || !result.Replay || result.Task.State != delegation.StateCompleted || result.Archive.Commit == "" {
		t.Fatalf("completion cleanup retry = %+v, %v", result, err)
	}
	if _, err := environment.engine.Load(delivered.Task.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed active worktree state = %v", err)
	}
}

func TestCompleteDirtyPreflightDoesNotMakeAuthorityTerminal(t *testing.T) {
	environment := newTestEnvironment(t)
	service, accepted := acceptTask(t, environment, "task-complete-dirty")
	writeFile(t, accepted.Worktree.Path, "README.md", "uncommitted\n")
	if _, err := service.Complete(context.Background(), CompleteInput{
		TaskID: accepted.Task.ID, ExpectedRevision: accepted.Task.Revision,
	}); !errors.Is(err, worktree.ErrDirtyWorktree) {
		t.Fatalf("dirty complete error = %v", err)
	}
	authoritative, err := environment.store.GetTask(context.Background(), accepted.Task.ID)
	if err != nil || authoritative.State != delegation.StateWorking || authoritative.Revision != accepted.Task.Revision {
		t.Fatalf("dirty preflight changed authority = %+v, %v", authoritative, err)
	}
}

func TestCompleteReconcilesLostAuthorityResponseBeforeArchiving(t *testing.T) {
	environment := newTestEnvironment(t)
	_, accepted := acceptTask(t, environment, "task-complete-response")
	faults := &faultAuthority{store: environment.store}
	faults.complete = func(ctx context.Context, input delegation.CommandInput) (delegation.TransitionResult, error) {
		result, err := environment.store.CompleteTask(ctx, input)
		if err != nil {
			return result, err
		}
		return delegation.TransitionResult{}, errors.New("completion response lost")
	}
	result, err := newService(t, faults, environment.engine).Complete(context.Background(), CompleteInput{
		TaskID: accepted.Task.ID, ExpectedRevision: accepted.Task.Revision,
	})
	if err != nil || !result.Replay || result.Task.State != delegation.StateCompleted || result.Archive.Commit != environment.base {
		t.Fatalf("lost complete response = %+v, %v", result, err)
	}
}

func TestConfirmRewindCASFailureRestoresBackupAndWIP(t *testing.T) {
	environment := newTestEnvironment(t)
	service, accepted := acceptTask(t, environment, "task-rewind-cas")
	ctx := context.Background()
	writeFile(t, accepted.Worktree.Path, "README.md", "turn one\n")
	first, err := service.Deliver(ctx, DeliverInput{TaskID: accepted.Task.ID, ExpectedRevision: accepted.Task.Revision})
	if err != nil {
		t.Fatal(err)
	}
	replied, err := environment.store.Reply(ctx, delegation.CommandInput{
		TaskID: first.Task.ID, ExpectedRevision: first.Task.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, accepted.Worktree.Path, "README.md", "turn two\n")
	second, err := service.Deliver(ctx, DeliverInput{TaskID: replied.Task.ID, ExpectedRevision: replied.Task.Revision})
	if err != nil {
		t.Fatal(err)
	}
	requested, err := environment.store.RequestRewind(ctx, delegation.RequestRewindInput{
		CommandInput: delegation.CommandInput{TaskID: second.Task.ID, ExpectedRevision: second.Task.Revision},
		TargetTurn:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	originalHead := head(t, accepted.Worktree.Path)
	writeFile(t, accepted.Worktree.Path, "README.md", "tracked WIP\n")
	writeFile(t, accepted.Worktree.Path, "scratch.txt", "untracked WIP\n")
	originalStatus := git(t, accepted.Worktree.Path, "status", "--porcelain=v1", "--untracked-files=all")

	injected := delegation.ErrRevisionConflict
	faults := &faultAuthority{store: environment.store}
	faults.confirm = func(context.Context, delegation.CommandInput) (delegation.TransitionResult, error) {
		return delegation.TransitionResult{}, injected
	}
	failedService := newService(t, faults, environment.engine)
	input := ConfirmRewindInput{
		TaskID: requested.Task.ID, ExpectedRevision: requested.Task.Revision, TargetTurn: 1,
	}
	if _, err := failedService.ConfirmRewind(ctx, input); !errors.Is(err, injected) {
		t.Fatalf("confirm rewind error = %v", err)
	}
	local, err := environment.engine.Load(accepted.Task.ID)
	if err != nil || local.LatestTurn != 2 || local.NextTurn != 3 || head(t, local.Path) != originalHead {
		t.Fatalf("restored local task = %+v, %v", local, err)
	}
	if content := readFile(t, local.Path, "README.md"); content != "tracked WIP\n" {
		t.Fatalf("tracked WIP after restore = %q", content)
	}
	if content := readFile(t, local.Path, "scratch.txt"); content != "untracked WIP\n" {
		t.Fatalf("untracked WIP after restore = %q", content)
	}
	if status := git(t, local.Path, "status", "--porcelain=v1", "--untracked-files=all"); status != originalStatus {
		t.Fatalf("status after restore = %q, want %q", status, originalStatus)
	}
	stored, err := environment.store.GetTask(ctx, requested.Task.ID)
	if err != nil || stored.State != delegation.StateRewindPending || stored.Revision != requested.Task.Revision {
		t.Fatalf("authority after CAS failure = %+v, %v", stored, err)
	}
	// The compensated task can retry the exact pending confirmation.
	confirmed, err := service.ConfirmRewind(ctx, input)
	if err != nil || confirmed.Task.LatestTurn != 1 || head(t, local.Path) != first.WorktreeArtifact.Commit {
		t.Fatalf("rewind retry = %+v, %v", confirmed, err)
	}
}

func TestConfirmRewindTreatsLostSuccessResponseAsReplay(t *testing.T) {
	environment := newTestEnvironment(t)
	service, accepted := acceptTask(t, environment, "task-rewind-response")
	ctx := context.Background()
	writeFile(t, accepted.Worktree.Path, "README.md", "turn one\n")
	delivered, err := service.Deliver(ctx, DeliverInput{TaskID: accepted.Task.ID, ExpectedRevision: accepted.Task.Revision})
	if err != nil {
		t.Fatal(err)
	}
	requested, err := environment.store.RequestRewind(ctx, delegation.RequestRewindInput{
		CommandInput: delegation.CommandInput{TaskID: delivered.Task.ID, ExpectedRevision: delivered.Task.Revision},
		TargetTurn:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	faults := &faultAuthority{store: environment.store}
	faults.confirm = func(ctx context.Context, input delegation.CommandInput) (delegation.TransitionResult, error) {
		result, err := environment.store.ConfirmRewind(ctx, input)
		if err != nil {
			return result, err
		}
		return delegation.TransitionResult{}, errors.New("rewind response lost")
	}
	result, err := newService(t, faults, environment.engine).ConfirmRewind(ctx, ConfirmRewindInput{
		TaskID: requested.Task.ID, ExpectedRevision: requested.Task.Revision, TargetTurn: 0,
	})
	if err != nil || !result.Replay || result.Task.LatestTurn != 0 || head(t, result.Worktree.Path) != environment.base {
		t.Fatalf("lost rewind response = %+v, %v", result, err)
	}
}

func assertFullIndexArtifact(t *testing.T, result DeliverResult, turn int, base string) {
	t.Helper()
	if result.StoredArtifact.MediaType != delegation.GitPatchMediaType || result.WorktreeArtifact.Turn != turn ||
		!bytes.Equal(result.StoredArtifact.Data, result.WorktreeArtifact.CumulativePatch) ||
		!bytes.Contains(result.StoredArtifact.Data, []byte("index ")) {
		t.Fatalf("artifact = %+v", result)
	}
	metadata, err := DecodeArtifactMetadata(result.StoredArtifact.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Turn != turn || metadata.TaskID != result.Task.ID || metadata.BaseCommit != base ||
		metadata.Commit != result.WorktreeArtifact.Commit ||
		!bytes.Equal(metadata.IncrementalPatch, result.WorktreeArtifact.IncrementalPatch) ||
		len(metadata.Files) == 0 || metadata.Files[0].Mode == "" {
		t.Fatalf("metadata = %+v", metadata)
	}
}

type faultAuthority struct {
	store    *delegation.Store
	accept   func(context.Context, delegation.AcceptTaskInput) (delegation.TransitionResult, error)
	deliver  func(context.Context, delegation.DeliverTurnInput) (delegation.DeliveryResult, error)
	complete func(context.Context, delegation.CommandInput) (delegation.TransitionResult, error)
	confirm  func(context.Context, delegation.CommandInput) (delegation.TransitionResult, error)
}

type faultWorktrees struct {
	Worktrees
	completeErr error
}

func (worktrees *faultWorktrees) Complete(context.Context, string) (worktree.ArchiveReceipt, error) {
	return worktree.ArchiveReceipt{}, worktrees.completeErr
}

func (authority *faultAuthority) GetTask(ctx context.Context, id string) (delegation.Task, error) {
	return authority.store.GetTask(ctx, id)
}

func (authority *faultAuthority) GetArtifact(ctx context.Context, id string, turn int64) (delegation.Artifact, error) {
	return authority.store.GetArtifact(ctx, id, turn)
}

func (authority *faultAuthority) AcceptTask(ctx context.Context, input delegation.AcceptTaskInput) (delegation.TransitionResult, error) {
	if authority.accept != nil {
		return authority.accept(ctx, input)
	}
	return authority.store.AcceptTask(ctx, input)
}

func (authority *faultAuthority) DeliverTurn(ctx context.Context, input delegation.DeliverTurnInput) (delegation.DeliveryResult, error) {
	if authority.deliver != nil {
		return authority.deliver(ctx, input)
	}
	return authority.store.DeliverTurn(ctx, input)
}

func (authority *faultAuthority) CompleteTask(ctx context.Context, input delegation.CommandInput) (delegation.TransitionResult, error) {
	if authority.complete != nil {
		return authority.complete(ctx, input)
	}
	return authority.store.CompleteTask(ctx, input)
}

func (authority *faultAuthority) ConfirmRewind(ctx context.Context, input delegation.CommandInput) (delegation.TransitionResult, error) {
	if authority.confirm != nil {
		return authority.confirm(ctx, input)
	}
	return authority.store.ConfirmRewind(ctx, input)
}

func writeFile(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, root, relative string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func head(t *testing.T, root string) string {
	t.Helper()
	return strings.TrimSpace(git(t, root, "rev-parse", "HEAD"))
}

func git(t *testing.T, root string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func gitSucceeds(root string, args ...string) bool {
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	return command.Run() == nil
}
