package patchapply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/worktree"
)

type applyFixture struct {
	caller   string
	producer string
	engine   *Engine
	worker   *worktree.Engine
	task     worktree.Task
	base     string
}

func newApplyFixture(t *testing.T) *applyFixture {
	t.Helper()
	origin := filepath.Join(t.TempDir(), "origin.git")
	gitRun(t, "", "init", "--bare", "-q", origin)
	seed := filepath.Join(t.TempDir(), "seed")
	gitRun(t, "", "clone", "-q", origin, seed)
	gitRun(t, seed, "config", "user.name", "Test")
	gitRun(t, seed, "config", "user.email", "test@example.test")
	if err := os.WriteFile(filepath.Join(seed, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, seed, "add", "README.md")
	gitRun(t, seed, "commit", "-q", "-m", "base")
	gitRun(t, seed, "push", "-q", "origin", "HEAD")
	base := strings.TrimSpace(gitRun(t, seed, "rev-parse", "HEAD"))
	caller := filepath.Join(t.TempDir(), "caller")
	producer := filepath.Join(t.TempDir(), "producer")
	gitRun(t, "", "clone", "-q", origin, caller)
	gitRun(t, "", "clone", "-q", origin, producer)
	gitRun(t, caller, "config", "user.name", "Caller")
	gitRun(t, caller, "config", "user.email", "caller@example.test")
	gitRun(t, producer, "config", "user.name", "Human")
	gitRun(t, producer, "config", "user.email", "human@example.test")
	worker, err := worktree.Open(worktree.Config{
		Repository: producer, WorktreeRoot: filepath.Join(t.TempDir(), "worktrees"),
		AuthorName: "Human", AuthorEmail: "human@example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	task, err := worker.Create(context.Background(), "apply-task", base)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := Open(Config{Repository: caller, StateRoot: filepath.Join(t.TempDir(), "apply-state")})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Bind(context.Background(), task.ID, base); err != nil {
		t.Fatal(err)
	}
	return &applyFixture{caller: caller, producer: producer, engine: engine, worker: worker, task: task, base: base}
}

func (fixture *applyFixture) artifact(t *testing.T, turn int, content string) Artifact {
	t.Helper()
	if err := os.WriteFile(filepath.Join(fixture.task.Path, "README.md"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	delivered, err := fixture.worker.CommitTurn(context.Background(), fixture.task.ID, turn)
	if err != nil {
		t.Fatal(err)
	}
	files := make([]File, len(delivered.Files))
	for index, file := range delivered.Files {
		files[index] = File{Path: file.Path, BlobSHA: file.BlobSHA, Mode: file.Mode}
	}
	return Artifact{
		TaskID: delivered.TaskID, Turn: delivered.Turn, BaseCommit: delivered.BaseCommit,
		Commit: delivered.Commit, CumulativePatch: delivered.CumulativePatch,
		IncrementalPatch: delivered.IncrementalPatch, Files: files,
	}
}

func TestReplaceApplyHashOracleAndReplay(t *testing.T) {
	fixture := newApplyFixture(t)
	ctx := context.Background()
	first := fixture.artifact(t, 1, "one\n")
	result, err := fixture.engine.Apply(ctx, first)
	if err != nil || result.Level != 1 || result.Replay {
		t.Fatalf("first apply = %+v, %v", result, err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "one\n")
	replay, err := fixture.engine.Apply(ctx, first)
	if err != nil || !replay.Replay {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	if err := os.WriteFile(filepath.Join(fixture.caller, "README.md"), []byte("drift\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.engine.Apply(ctx, first); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("drifted replay error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(fixture.caller, "README.md"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	second := fixture.artifact(t, 2, "two\n")
	result, err = fixture.engine.Apply(ctx, second)
	if err != nil || result.Level != 1 {
		var failure *ApplyError
		if errors.As(err, &failure) {
			t.Fatalf("replace apply = %+v, %v: %+v", result, err, failure.Conflicts)
		}
		t.Fatalf("replace apply = %+v, %v", result, err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "two\n")

	bad := fixture.artifact(t, 3, "three\n")
	bad.Files[0].BlobSHA = strings.Repeat("0", len(bad.Files[0].BlobSHA))
	if _, err := fixture.engine.Apply(ctx, bad); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("bad oracle error = %v", err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "two\n")
}

func TestReplaceArtifactCanReturnExactlyToBaseWithEmptyCumulativePatch(t *testing.T) {
	fixture := newApplyFixture(t)
	ctx := context.Background()
	first := fixture.artifact(t, 1, "changed\n")
	if _, err := fixture.engine.Apply(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := fixture.artifact(t, 2, "base\n")
	if len(second.CumulativePatch) != 0 || len(second.IncrementalPatch) == 0 || len(second.Files) != 0 {
		t.Fatalf("return-to-base artifact = cumulative %d, incremental %d, files %+v",
			len(second.CumulativePatch), len(second.IncrementalPatch), second.Files)
	}
	result, err := fixture.engine.Apply(ctx, second)
	if err != nil || result.Level != 1 {
		t.Fatalf("return-to-base apply = %+v, %v", result, err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "base\n")
	if len(result.Files) != 1 || result.Files[0].Path != "README.md" || result.Files[0].Mode != gitModeRegular {
		t.Fatalf("expanded base oracle = %+v", result.Files)
	}
}

func TestExecutableModeDeliveryIsVerified(t *testing.T) {
	fixture := newApplyFixture(t)
	if err := os.Chmod(filepath.Join(fixture.task.Path, "README.md"), 0o700); err != nil {
		t.Fatal(err)
	}
	artifact := fixture.artifact(t, 1, "executable\n")
	if len(artifact.Files) != 1 || artifact.Files[0].Mode != gitModeExecutable {
		t.Fatalf("executable artifact oracle = %+v", artifact.Files)
	}
	if _, err := fixture.engine.Apply(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(fixture.caller, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("executable delivery mode = %s", info.Mode())
	}
}

func TestPersistedFileOracleWithoutModeFailsClosed(t *testing.T) {
	fixture := newApplyFixture(t)
	artifact := fixture.artifact(t, 1, "one\n")
	if _, err := fixture.engine.Apply(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}
	stored, exists, err := fixture.engine.loadState(artifact.TaskID)
	if err != nil || !exists || len(stored.Files) != 1 {
		t.Fatalf("load applied state = %+v, %t, %v", stored, exists, err)
	}
	stored.Files[0].Mode = ""
	if err := fixture.engine.saveState(stored); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.engine.Apply(context.Background(), artifact); err == nil ||
		!strings.Contains(err.Error(), "invalid persisted apply state file metadata") {
		t.Fatalf("mode-less persisted oracle error = %v", err)
	}
}

func TestCommittedPreviousArtifactFallsBackToIncremental(t *testing.T) {
	fixture := newApplyFixture(t)
	ctx := context.Background()
	first := fixture.artifact(t, 1, "one\n")
	if _, err := fixture.engine.Apply(ctx, first); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.caller, "add", "README.md")
	gitRun(t, fixture.caller, "commit", "-q", "-m", "caller accepted turn one")
	second := fixture.artifact(t, 2, "two\n")
	result, err := fixture.engine.Apply(ctx, second)
	if err != nil || result.Level != 2 {
		t.Fatalf("incremental fallback = %+v, %v", result, err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "two\n")
}

func TestTouchedCallerChangeFailsWithoutMutation(t *testing.T) {
	fixture := newApplyFixture(t)
	artifact := fixture.artifact(t, 1, "human\n")
	if err := os.WriteFile(filepath.Join(fixture.caller, "README.md"), []byte("caller\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.engine.Apply(context.Background(), artifact); !errors.Is(err, ErrDirtyTouchedPaths) {
		t.Fatalf("dirty apply error = %v", err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "caller\n")
}

func TestPatchPathsMustExactlyMatchArtifactFileOracle(t *testing.T) {
	t.Run("underreported", func(t *testing.T) {
		fixture := newApplyFixture(t)
		artifact := fixture.artifact(t, 1, "human\n")
		artifact.Files = nil
		if _, err := fixture.engine.Apply(context.Background(), artifact); !errors.Is(err, ErrArtifactPaths) {
			t.Fatalf("underreported paths error = %v", err)
		}
		assertFile(t, filepath.Join(fixture.caller, "README.md"), "base\n")
	})
	t.Run("overreported", func(t *testing.T) {
		fixture := newApplyFixture(t)
		artifact := fixture.artifact(t, 1, "human\n")
		artifact.Files = append(artifact.Files, File{Path: "extra.txt", BlobSHA: strings.Repeat("0", 40), Mode: gitModeRegular})
		if _, err := fixture.engine.Apply(context.Background(), artifact); !errors.Is(err, ErrArtifactPaths) {
			t.Fatalf("overreported paths error = %v", err)
		}
		assertFile(t, filepath.Join(fixture.caller, "README.md"), "base\n")
	})
}

func TestRenamePatchCannotHideDeletedEndpoint(t *testing.T) {
	fixture := newApplyFixture(t)
	gitRun(t, fixture.task.Path, "mv", "README.md", "RENAMED.md")
	gitRun(t, fixture.task.Path, "commit", "-q", "-m", "rename")
	commit := strings.TrimSpace(gitRun(t, fixture.task.Path, "rev-parse", "HEAD"))
	patch := []byte(gitRun(t, fixture.task.Path,
		"diff", "--find-renames=100%", "--full-index", "--binary", fixture.base+"..."+commit))
	if !bytes.Contains(patch, []byte("rename from README.md")) {
		t.Fatalf("fixture did not produce a rename patch:\n%s", patch)
	}
	line := strings.Fields(gitRunLiteral(t, fixture.task.Path, "ls-tree", commit, "--", "RENAMED.md"))
	if len(line) < 3 {
		t.Fatalf("renamed blob metadata = %v", line)
	}
	artifact := Artifact{
		TaskID: fixture.task.ID, Turn: 1, BaseCommit: fixture.base, Commit: commit,
		CumulativePatch: patch, IncrementalPatch: patch,
		Files: []File{{Path: "RENAMED.md", BlobSHA: line[2], Mode: line[0]}},
	}
	if _, err := fixture.engine.Apply(context.Background(), artifact); !errors.Is(err, ErrArtifactPaths) {
		t.Fatalf("rename patch error = %v", err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "base\n")
	if _, err := os.Lstat(filepath.Join(fixture.caller, "RENAMED.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rename destination appeared after rejection: %v", err)
	}
	if staged := gitRun(t, fixture.caller, "diff", "--cached", "--name-only"); staged != "" {
		t.Fatalf("rename rejection changed index: %q", staged)
	}
}

func TestCrossPathPatchWithoutRenameHeadersCannotHideSource(t *testing.T) {
	fixture := newApplyFixture(t)
	baseBlob := strings.TrimSpace(gitRun(t, fixture.task.Path, "rev-parse", fixture.base+":README.md"))
	if err := os.Rename(filepath.Join(fixture.task.Path, "README.md"), filepath.Join(fixture.task.Path, "MOVED.md")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.task.Path, "MOVED.md"), []byte("moved\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.task.Path, "add", "-A")
	gitRun(t, fixture.task.Path, "commit", "-q", "-m", "cross path")
	commit := strings.TrimSpace(gitRun(t, fixture.task.Path, "rev-parse", "HEAD"))
	newBlob := strings.TrimSpace(gitRun(t, fixture.task.Path, "rev-parse", commit+":MOVED.md"))
	patch := []byte(fmt.Sprintf(
		"diff --git a/README.md b/MOVED.md\nindex %s..%s 100644\n--- a/README.md\n+++ b/MOVED.md\n@@ -1 +1 @@\n-base\n+moved\n",
		baseBlob, newBlob,
	))
	if bytes.Contains(patch, []byte("rename from")) || bytes.Contains(patch, []byte("rename to")) {
		t.Fatal("fixture unexpectedly contains extended rename headers")
	}
	artifact := Artifact{
		TaskID: fixture.task.ID, Turn: 1, BaseCommit: fixture.base, Commit: commit,
		CumulativePatch: patch, IncrementalPatch: patch,
		Files: []File{{Path: "MOVED.md", BlobSHA: newBlob, Mode: gitModeRegular}},
	}
	if _, err := fixture.engine.Apply(context.Background(), artifact); !errors.Is(err, ErrArtifactPaths) {
		t.Fatalf("cross-path patch error = %v", err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "base\n")
	if _, err := os.Lstat(filepath.Join(fixture.caller, "MOVED.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cross-path destination appeared after rejection: %v", err)
	}
}

func TestArtifactPathspecMagicIsAlwaysLiteral(t *testing.T) {
	fixture := newApplyFixture(t)
	name := ":(glob)**"
	if err := os.WriteFile(filepath.Join(fixture.task.Path, name), []byte("literal\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRunLiteral(t, fixture.task.Path, "add", "--", name)
	gitRunLiteral(t, fixture.task.Path, "commit", "-q", "-m", "literal pathspec")
	commit := strings.TrimSpace(gitRunLiteral(t, fixture.task.Path, "rev-parse", "HEAD"))
	patch := []byte(gitRunLiteral(t, fixture.task.Path,
		"diff", "--no-renames", "--full-index", "--binary", fixture.base+"..."+commit))
	line := strings.Fields(gitRunLiteral(t, fixture.task.Path, "ls-tree", commit, "--", name))
	if len(line) < 3 {
		t.Fatalf("literal blob metadata = %v", line)
	}
	artifact := Artifact{
		TaskID: fixture.task.ID, Turn: 1, BaseCommit: fixture.base, Commit: commit,
		CumulativePatch: patch, IncrementalPatch: patch,
		Files: []File{{Path: name, BlobSHA: line[2], Mode: line[0]}},
	}
	stateRoot := filepath.Join(t.TempDir(), "literal-state")
	engine, err := Open(Config{
		Repository: fixture.caller, StateRoot: stateRoot, DirtyCommit: true, MergirafPath: "-",
		DirtyCommitName: "Caller", DirtyCommitEmail: "caller@example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Bind(context.Background(), fixture.task.ID, fixture.base); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.caller, "README.md"), []byte("unrelated dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	headBefore := strings.TrimSpace(gitRun(t, fixture.caller, "rev-parse", "HEAD"))
	if _, err := engine.Apply(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}
	assertFile(t, filepath.Join(fixture.caller, name), "literal\n")
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "unrelated dirty\n")
	headAfter := strings.TrimSpace(gitRun(t, fixture.caller, "rev-parse", "HEAD"))
	if headAfter != headBefore {
		t.Fatalf("unrelated dirty file was committed: %s -> %s", headBefore, headAfter)
	}
}

func TestIntermediateSymlinkIsRejectedWithoutOutsideMutation(t *testing.T) {
	fixture := newApplyFixture(t)
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(outsideFile, []byte("outside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(outsideFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(fixture.caller, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(fixture.task.Path, "link"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.task.Path, "link", "secret.txt"), []byte("human\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	delivered, err := fixture.worker.CommitTurn(context.Background(), fixture.task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	files := make([]File, len(delivered.Files))
	for index, file := range delivered.Files {
		files[index] = File{Path: file.Path, BlobSHA: file.BlobSHA, Mode: file.Mode}
	}
	artifact := Artifact{
		TaskID: delivered.TaskID, Turn: delivered.Turn, BaseCommit: delivered.BaseCommit,
		Commit: delivered.Commit, CumulativePatch: delivered.CumulativePatch,
		IncrementalPatch: delivered.IncrementalPatch, Files: files,
	}
	if _, err := fixture.engine.Apply(context.Background(), artifact); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("symlink boundary error = %v", err)
	}
	after, err := os.Stat(outsideFile)
	if err != nil {
		t.Fatal(err)
	}
	assertFile(t, outsideFile, "outside\n")
	if !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("outside mtime changed: %s -> %s", before.ModTime(), after.ModTime())
	}
}

func TestStagedTouchedPathIsRejectedWithoutClearingIndex(t *testing.T) {
	fixture := newApplyFixture(t)
	artifact := fixture.artifact(t, 1, "human\n")
	if err := os.WriteFile(filepath.Join(fixture.caller, "README.md"), []byte("staged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.caller, "add", "README.md")
	before := gitRun(t, fixture.caller, "diff", "--cached", "--", "README.md")
	if _, err := fixture.engine.Apply(context.Background(), artifact); !errors.Is(err, ErrDirtyTouchedPaths) {
		t.Fatalf("staged path error = %v", err)
	}
	after := gitRun(t, fixture.caller, "diff", "--cached", "--", "README.md")
	if before == "" || after != before {
		t.Fatalf("staged diff changed\nbefore=%q\nafter=%q", before, after)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "staged\n")
}

func TestDirtyCommitPreservesStagedRenameAndUnrelatedIndex(t *testing.T) {
	fixture := newApplyFixture(t)
	artifact := fixture.artifact(t, 1, "human\n")
	engine, err := Open(Config{
		Repository: fixture.caller, StateRoot: filepath.Join(t.TempDir(), "dirty-state"),
		DirtyCommit: true, DirtyCommitName: "Caller", DirtyCommitEmail: "caller@example.test", MergirafPath: "-",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Bind(context.Background(), fixture.task.ID, fixture.base); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.caller, "unrelated.txt"), []byte("staged unrelated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.caller, "add", "unrelated.txt")
	gitRun(t, fixture.caller, "mv", "README.md", "caller-renamed.md")
	if _, err := engine.Apply(context.Background(), artifact); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete/modify conflict = %v; want ErrConflict", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.caller, "README.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("conflicting apply resurrected renamed source: %v", err)
	}
	assertFile(t, filepath.Join(fixture.caller, "caller-renamed.md"), "base\n")
	committedRename := gitRun(t, fixture.caller, "show", "HEAD:caller-renamed.md")
	if committedRename != "base\n" {
		t.Fatalf("preserved rename content = %q", committedRename)
	}
	if _, err := exec.Command("git", "-C", fixture.caller, "show", "HEAD:README.md").CombinedOutput(); err == nil {
		t.Fatal("preservation commit retained the renamed source")
	}
	staged := strings.Fields(gitRun(t, fixture.caller, "diff", "--cached", "--name-only"))
	if len(staged) != 1 || staged[0] != "unrelated.txt" {
		t.Fatalf("unrelated index was not preserved exactly: %v", staged)
	}
	if unstaged := gitRun(t, fixture.caller, "diff", "--name-only"); unstaged != "" {
		t.Fatalf("conflict rollback left unstaged pollution: %q", unstaged)
	}
}

func TestIndependentEnginesSerializeAndReplayTheSameArtifact(t *testing.T) {
	fixture := newApplyFixture(t)
	artifact := fixture.artifact(t, 1, "human\n")
	second, err := Open(Config{
		Repository: fixture.caller, StateRoot: fixture.engine.stateRoot, MergirafPath: "-",
	})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan Result, 2)
	errors := make(chan error, 2)
	var wait sync.WaitGroup
	for _, engine := range []*Engine{fixture.engine, second} {
		wait.Add(1)
		go func(engine *Engine) {
			defer wait.Done()
			<-start
			result, err := engine.Apply(context.Background(), artifact)
			results <- result
			errors <- err
		}(engine)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent apply error = %v", err)
		}
	}
	var applied, replayed int
	for result := range results {
		if result.Replay {
			replayed++
		} else if result.Level == 1 {
			applied++
		}
	}
	if applied != 1 || replayed != 1 {
		t.Fatalf("concurrent outcomes: applied=%d replayed=%d", applied, replayed)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "human\n")
}

func TestSharedLedgerSerializesBindingsAcrossLinkedWorktrees(t *testing.T) {
	fixture := newApplyFixture(t)
	linked := filepath.Join(t.TempDir(), "linked")
	gitRun(t, fixture.caller, "worktree", "add", "--detach", "-q", linked, fixture.base)
	sharedState := filepath.Join(t.TempDir(), "shared-ledger")
	first, err := Open(Config{Repository: fixture.caller, StateRoot: sharedState, MergirafPath: "-"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(Config{Repository: linked, StateRoot: sharedState, MergirafPath: "-"})
	if err != nil {
		t.Fatal(err)
	}
	if first.stateLockPath != second.stateLockPath || first.repositoryLockPath == second.repositoryLockPath {
		t.Fatalf("lock namespaces: first=%q/%q second=%q/%q",
			first.stateLockPath, first.repositoryLockPath, second.stateLockPath, second.repositoryLockPath)
	}
	start := make(chan struct{})
	errorsChannel := make(chan error, 2)
	for _, engine := range []*Engine{first, second} {
		go func(engine *Engine) {
			<-start
			errorsChannel <- engine.Bind(context.Background(), "shared-binding", fixture.base)
		}(engine)
	}
	close(start)
	var succeeded, rejected int
	for range 2 {
		err := <-errorsChannel
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrTaskUnbound):
			rejected++
		default:
			t.Fatalf("linked worktree bind error = %v", err)
		}
	}
	if succeeded != 1 || rejected != 1 {
		t.Fatalf("linked worktree binding outcomes: success=%d rejected=%d", succeeded, rejected)
	}
}

func TestCanceledStructuredMergeStillRollsBackWorktreeAndIndex(t *testing.T) {
	fixture := newApplyFixture(t)
	mergiraf := filepath.Join(t.TempDir(), "mergiraf")
	marker := filepath.Join(t.TempDir(), "entered")
	script := "#!/bin/sh\n: > " + shellQuote(marker) + "\nwhile :; do :; done\n"
	if err := os.WriteFile(mergiraf, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	engine, err := Open(Config{
		Repository: fixture.caller, StateRoot: filepath.Join(t.TempDir(), "apply-state"), MergirafPath: mergiraf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Bind(context.Background(), fixture.task.ID, fixture.base); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.caller, "README.md"), []byte("caller\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.caller, "add", "README.md")
	gitRun(t, fixture.caller, "commit", "-q", "-m", "caller diverged")
	artifact := fixture.artifact(t, 1, "human\n")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := engine.Apply(ctx, artifact)
		result <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("structured merge was not entered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-result; err == nil {
		t.Fatal("canceled structured merge unexpectedly succeeded")
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "caller\n")
	if unmerged := gitRun(t, fixture.caller, "diff", "--name-only", "--diff-filter=U"); unmerged != "" {
		t.Fatalf("unmerged index entries remained after rollback: %q", unmerged)
	}
	if staged := gitRun(t, fixture.caller, "diff", "--cached", "--name-only"); staged != "" {
		t.Fatalf("staged entries remained after rollback: %q", staged)
	}
}

func TestIncrementalCannotMutatePathThatReturnedToBaseWithoutOracle(t *testing.T) {
	fixture := newApplyFixture(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(fixture.task.Path, "other.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first := fixture.artifact(t, 1, "one\n")
	if _, err := fixture.engine.Apply(ctx, first); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.caller, "add", "README.md", "other.txt")
	gitRun(t, fixture.caller, "commit", "-q", "-m", "accepted first turn")

	if err := os.WriteFile(filepath.Join(fixture.task.Path, "other.txt"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	honest := fixture.artifact(t, 2, "base\n")
	if len(honest.Files) != 1 || honest.Files[0].Path != "other.txt" {
		t.Fatalf("honest turn should omit base-restored README: %+v", honest.Files)
	}
	gitRun(t, fixture.task.Path, "reset", "--hard", "-q", first.Commit)
	if err := os.WriteFile(filepath.Join(fixture.task.Path, "README.md"), []byte("evil\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.task.Path, "other.txt"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.task.Path, "add", "-A")
	gitRun(t, fixture.task.Path, "commit", "-q", "-m", "tampered incremental")
	maliciousCommit := strings.TrimSpace(gitRun(t, fixture.task.Path, "rev-parse", "HEAD"))
	honest.IncrementalPatch = []byte(gitRun(t, fixture.task.Path,
		"diff", "--no-renames", "--full-index", "--binary", first.Commit+".."+maliciousCommit))
	gitRun(t, fixture.task.Path, "reset", "--hard", "-q", honest.Commit)
	if _, err := fixture.engine.Apply(ctx, honest); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("tampered incremental error = %v", err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "one\n")
	assertFile(t, filepath.Join(fixture.caller, "other.txt"), "one\n")
}

func TestIncrementalCannotHideExecutableBitOnPathThatReturnedToBase(t *testing.T) {
	fixture := newApplyFixture(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(fixture.task.Path, "other.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first := fixture.artifact(t, 1, "one\n")
	if _, err := fixture.engine.Apply(ctx, first); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.caller, "add", "README.md", "other.txt")
	gitRun(t, fixture.caller, "commit", "-q", "-m", "accepted first turn")

	if err := os.WriteFile(filepath.Join(fixture.task.Path, "other.txt"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	honest := fixture.artifact(t, 2, "base\n")
	if len(honest.Files) != 1 || honest.Files[0].Path != "other.txt" {
		t.Fatalf("honest turn should omit base-restored README: %+v", honest.Files)
	}
	gitRun(t, fixture.task.Path, "reset", "--hard", "-q", first.Commit)
	if err := os.WriteFile(filepath.Join(fixture.task.Path, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(fixture.task.Path, "README.md"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.task.Path, "other.txt"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.task.Path, "add", "-A")
	gitRun(t, fixture.task.Path, "commit", "-q", "-m", "tampered incremental mode")
	maliciousCommit := strings.TrimSpace(gitRun(t, fixture.task.Path, "rev-parse", "HEAD"))
	honest.IncrementalPatch = []byte(gitRun(t, fixture.task.Path,
		"diff", "--no-renames", "--full-index", "--binary", first.Commit+".."+maliciousCommit))
	gitRun(t, fixture.task.Path, "reset", "--hard", "-q", honest.Commit)
	if _, err := fixture.engine.Apply(ctx, honest); !errors.Is(err, ErrModeMismatch) {
		t.Fatalf("tampered incremental mode error = %v", err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "one\n")
	assertFile(t, filepath.Join(fixture.caller, "other.txt"), "one\n")
	info, err := os.Stat(filepath.Join(fixture.caller, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 != 0 {
		t.Fatalf("mode mismatch rollback left README executable: %s", info.Mode())
	}
}

func TestTaskMustBeBoundToCallerRepositoryAndBase(t *testing.T) {
	fixture := newApplyFixture(t)
	artifact := fixture.artifact(t, 1, "human\n")
	unbound, err := Open(Config{
		Repository: fixture.caller, StateRoot: filepath.Join(t.TempDir(), "unbound"), MergirafPath: "-",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unbound.Apply(context.Background(), artifact); !errors.Is(err, ErrTaskUnbound) {
		t.Fatalf("unbound task error = %v", err)
	}
	if err := unbound.Bind(context.Background(), artifact.TaskID, strings.Repeat("0", 40)); err == nil {
		t.Fatal("unknown base commit was bound")
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "base\n")
}

func TestRevisionOptionInjectionIsRejectedWithoutOutsideWrite(t *testing.T) {
	fixture := newApplyFixture(t)
	artifact := fixture.artifact(t, 1, "human\n")
	outside := t.TempDir()
	malicious := "--output=" + filepath.Join(outside, "escaped")
	artifact.Commit = malicious
	if _, err := fixture.engine.Apply(context.Background(), artifact); err == nil {
		t.Fatal("option-shaped artifact commit was accepted")
	}
	if err := fixture.engine.ValidateBase(context.Background(), malicious); err == nil {
		t.Fatal("option-shaped base commit was accepted")
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("revision parsing wrote outside the repository: %+v", entries)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "base\n")
}

func TestStructuredMergeIsAuditedAnnotatedAndHashChecked(t *testing.T) {
	fixture := newApplyFixture(t)
	mergiraf := filepath.Join(t.TempDir(), "mergiraf")
	// The fake exercises the integration boundary deterministically by choosing
	// the delivered side. A real mergiraf may synthesize a combined candidate;
	// the same artifact hash oracle below still decides whether it is accepted.
	if err := os.WriteFile(mergiraf, []byte("#!/bin/sh\ncp \"$7\" \"$6\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	stateRoot := filepath.Join(t.TempDir(), "apply-state")
	engine, err := Open(Config{
		Repository: fixture.caller, StateRoot: stateRoot, MergirafPath: mergiraf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Bind(context.Background(), fixture.task.ID, fixture.base); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.caller, "README.md"), []byte("caller\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.caller, "add", "README.md")
	gitRun(t, fixture.caller, "commit", "-q", "-m", "caller diverged")

	artifact := fixture.artifact(t, 1, "human\n")
	result, err := engine.Apply(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	if result.Level != 3 || len(result.ResolvedConflicts) != 1 || result.ResolvedConflicts[0] != "README.md" {
		t.Fatalf("structured result = %+v", result)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "human\n")
	audit := filepath.Join(stateRoot, "conflicts", artifact.TaskID, "turn-1", "README.md.conflict")
	content, err := os.ReadFile(audit)
	if err != nil || !strings.Contains(string(content), "<<<<<<<") {
		t.Fatalf("conflict audit = %q, %v", content, err)
	}
	assertFile(t, filepath.Join(stateRoot, "conflicts", artifact.TaskID, "turn-1", "README.md.merged"), "human\n")
}

func TestStructuredMergeCandidateCannotBypassArtifactHashOracle(t *testing.T) {
	fixture := newApplyFixture(t)
	mergiraf := filepath.Join(t.TempDir(), "mergiraf")
	if err := os.WriteFile(mergiraf, []byte("#!/bin/sh\nprintf 'caller and human\\n' > \"$6\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	stateRoot := filepath.Join(t.TempDir(), "apply-state")
	engine, err := Open(Config{
		Repository: fixture.caller, StateRoot: stateRoot, MergirafPath: mergiraf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Bind(context.Background(), fixture.task.ID, fixture.base); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.caller, "README.md"), []byte("caller\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, fixture.caller, "add", "README.md")
	gitRun(t, fixture.caller, "commit", "-q", "-m", "caller diverged")
	artifact := fixture.artifact(t, 1, "human\n")
	if _, err := engine.Apply(context.Background(), artifact); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("structured hash mismatch error = %v", err)
	}
	assertFile(t, filepath.Join(fixture.caller, "README.md"), "caller\n")
	assertFile(t, filepath.Join(stateRoot, "conflicts", artifact.TaskID, "turn-1", "README.md.merged"), "caller and human\n")
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil || string(content) != want {
		t.Fatalf("%s = %q, %v; want %q", path, content, err, want)
	}
}

func gitRun(t *testing.T, directory string, args ...string) string {
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

func gitRunLiteral(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"--literal-pathspecs", "-C", directory}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git --literal-pathspecs %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
