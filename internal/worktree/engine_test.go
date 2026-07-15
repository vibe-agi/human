package worktree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestEngine(t *testing.T) (*Engine, string, string) {
	t.Helper()
	repository := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, repository, "init", "-q")
	gitTest(t, repository, "config", "user.name", "Test Human")
	gitTest(t, repository, "config", "user.email", "human@example.test")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, repository, "add", "README.md")
	gitTest(t, repository, "commit", "-q", "-m", "base")
	base := strings.TrimSpace(gitTest(t, repository, "rev-parse", "HEAD"))
	engine, err := Open(Config{
		Repository: repository, WorktreeRoot: filepath.Join(t.TempDir(), "worktrees"),
		AuthorName: "Test Human", AuthorEmail: "human@example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return engine, repository, base
}

func TestTaskWorktreeTurnsArtifactsAndRewind(t *testing.T) {
	engine, repository, base := newTestEngine(t)
	ctx := context.Background()
	task, err := engine.Create(ctx, "task-1", base)
	if err != nil {
		t.Fatal(err)
	}
	if task.Path == repository || task.BaseCommit != base || task.Branch != "human/task-1" {
		t.Fatalf("task = %+v", task)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("turn one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "new.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := engine.CommitTurn(ctx, task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(first.CumulativePatch, []byte("turn one")) || !bytes.Contains(first.CumulativePatch, []byte("new.txt")) ||
		!bytes.Contains(first.CumulativePatch, []byte("index ")) || len(first.Files) != 2 {
		t.Fatalf("first artifact = %+v\n%s", first, first.CumulativePatch)
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "refs/human/keep/task-1/turn-1")); got != first.Commit {
		t.Fatalf("first keep ref = %s, want %s", got, first.Commit)
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "refs/human/keep/task-1/turn-1-previous")); got != base {
		t.Fatalf("first previous ref = %s, want %s", got, base)
	}
	rebuilt, err := engine.CurrentArtifact(ctx, task.ID, 1)
	if err != nil || rebuilt.Commit != first.Commit ||
		!bytes.Equal(rebuilt.CumulativePatch, first.CumulativePatch) ||
		!bytes.Equal(rebuilt.IncrementalPatch, first.IncrementalPatch) {
		t.Fatalf("rebuilt first artifact = %+v, %v", rebuilt, err)
	}

	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("turn two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "untracked.txt"), []byte("wip survives\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wip, err := engine.SnapshotWIP(ctx, task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := gitTest(t, task.Path, "show", wip+":untracked.txt"); got != "wip survives\n" {
		t.Fatalf("WIP snapshot lost untracked content: %q", got)
	}
	second, err := engine.CommitTurn(ctx, task.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(second.CumulativePatch, []byte("turn two")) || bytes.Contains(second.IncrementalPatch, []byte("base\n")) ||
		bytes.Contains(second.IncrementalPatch, []byte("new.txt")) {
		t.Fatalf("second artifact cumulative/incremental mismatch\n%s\n%s", second.CumulativePatch, second.IncrementalPatch)
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "refs/human/keep/task-1/turn-2")); got != second.Commit {
		t.Fatalf("second keep ref = %s, want %s", got, second.Commit)
	}

	if err := os.WriteFile(filepath.Join(task.Path, "scratch.tmp"), []byte("do not clean"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Rewind(ctx, task.ID, 1, base); err == nil {
		t.Fatal("rewind accepted a commit that does not match the pinned turn")
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "HEAD")); got != second.Commit {
		t.Fatalf("rejected rewind changed HEAD to %s", got)
	}
	rewound, err := engine.Rewind(ctx, task.ID, 1, first.Commit)
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(task.Path, "README.md"))
	if err != nil || string(content) != "turn one\n" {
		t.Fatalf("rewound README = %q, %v", content, err)
	}
	if _, err := os.Stat(filepath.Join(task.Path, "scratch.tmp")); err != nil {
		t.Fatalf("rewind cleaned untracked file: %v", err)
	}
	if rewound.LatestTurn != 1 || rewound.NextTurn != 3 || rewound.LatestCommit != first.Commit ||
		rewound.LatestPreviousCommit != base {
		t.Fatalf("rewound task = %+v", rewound)
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "refs/human/backup/task-1/turn-2")); got != second.Commit {
		t.Fatalf("backup ref = %s, want %s", got, second.Commit)
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "refs/human/keep/task-1/turn-2")); got != second.Commit {
		t.Fatalf("keep ref = %s, want %s", got, second.Commit)
	}
	// Legacy state omitted NextTurn. Its rewind keep ref must prevent turn 2
	// from being reused after migration.
	statePath := engine.statePath(task.ID)
	payload, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var legacy map[string]any
	if err := json.Unmarshal(payload, &legacy); err != nil {
		t.Fatal(err)
	}
	delete(legacy, "next_turn")
	payload, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if migrated, err := engine.Load(task.ID); err != nil || migrated.NextTurn != 3 {
		t.Fatalf("legacy rewind migration = %+v, %v", migrated, err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("branched turn three\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	third, err := engine.CommitTurn(ctx, task.ID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if third.Turn != 3 || third.PreviousCommit != first.Commit || !bytes.Contains(third.CumulativePatch, []byte("branched turn three")) {
		t.Fatalf("post-rewind artifact = %+v", third)
	}
}

func TestArtifactSafetyScanBlocksCredentialAndLargeFile(t *testing.T) {
	engine, _, base := newTestEngine(t)
	engine.maxFileBytes = 1 << 20
	task, err := engine.Create(context.Background(), "safe-task", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "key.pem"), []byte("-----BEGIN "+"PRIVATE KEY-----\nsecret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.CommitTurn(context.Background(), task.ID, 1); !errors.Is(err, ErrUnsafeChange) {
		t.Fatalf("credential commit error = %v", err)
	}
	if status := gitTest(t, task.Path, "diff", "--cached", "--name-only"); status != "" {
		t.Fatalf("failed scan left index staged: %q", status)
	}

	largeEngine, _, largeBase := newTestEngine(t)
	largeEngine.maxFileBytes = 8
	largeTask, err := largeEngine.Create(context.Background(), "large-task", largeBase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(largeTask.Path, "large.bin"), bytes.Repeat([]byte{'x'}, 9), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := largeEngine.CommitTurn(context.Background(), largeTask.ID, 1); !errors.Is(err, ErrUnsafeChange) {
		t.Fatalf("large file commit error = %v", err)
	}
}

func TestSharedRemoteIsExplicitAndFetchesExactBase(t *testing.T) {
	upstream := filepath.Join(t.TempDir(), "upstream")
	if err := os.MkdirAll(upstream, 0o700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, upstream, "init", "-q")
	gitTest(t, upstream, "config", "user.name", "Upstream")
	gitTest(t, upstream, "config", "user.email", "upstream@example.test")
	if err := os.WriteFile(filepath.Join(upstream, "base.txt"), []byte("remote base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, upstream, "add", "base.txt")
	gitTest(t, upstream, "commit", "-q", "-m", "remote base")
	base := strings.TrimSpace(gitTest(t, upstream, "rev-parse", "HEAD"))

	receiver := filepath.Join(t.TempDir(), "receiver")
	if err := os.MkdirAll(receiver, 0o700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, receiver, "init", "-q")
	gitTest(t, receiver, "config", "user.name", "Receiver")
	gitTest(t, receiver, "config", "user.email", "receiver@example.test")
	gitTest(t, receiver, "remote", "add", "origin", upstream)

	localOnly, err := Open(Config{Repository: receiver, WorktreeRoot: filepath.Join(t.TempDir(), "local-only")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := localOnly.Create(context.Background(), "must-not-guess", base); !errors.Is(err, ErrInvalidBase) {
		t.Fatalf("local-only create error = %v, want ErrInvalidBase", err)
	}

	shared, err := Open(Config{
		Repository: receiver, WorktreeRoot: filepath.Join(t.TempDir(), "shared"), SharedRemote: "origin",
	})
	if err != nil {
		t.Fatal(err)
	}
	task, err := shared.Create(context.Background(), "fetched", strings.ToUpper(base))
	if err != nil {
		t.Fatal(err)
	}
	if task.BaseCommit != base || task.SharedRemote != "origin" {
		t.Fatalf("fetched task = %+v", task)
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "HEAD")); got != base {
		t.Fatalf("fetched worktree HEAD = %s, want %s", got, base)
	}
	if _, err := Open(Config{Repository: receiver, SharedRemote: "--upload-pack=malicious"}); !errors.Is(err, ErrInvalidRemote) {
		t.Fatalf("option-like shared remote error = %v", err)
	}
}

func TestSetupHookIsExplicitAndFailureCleansUp(t *testing.T) {
	engine, repository, base := newTestEngine(t)
	defaultTask, err := engine.Create(context.Background(), "default-setup", base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(defaultTask.Path, "setup.marker")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("default-disabled setup marker stat = %v", err)
	}

	explicit, err := Open(Config{
		Repository: repository, WorktreeRoot: filepath.Join(t.TempDir(), "explicit-setup"),
		SetupHook: []string{"sh", "-c", `printf '%s\n%s\n' "$HUMAN_TASK_ID" "$HUMAN_BASE_COMMIT" > setup.marker`},
	})
	if err != nil {
		t.Fatal(err)
	}
	explicitTask, err := explicit.Create(context.Background(), "explicit-setup", base)
	if err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(explicitTask.Path, "setup.marker"))
	if err != nil {
		t.Fatal(err)
	}
	if string(marker) != "explicit-setup\n"+base+"\n" {
		t.Fatalf("setup marker = %q", marker)
	}

	failing, err := Open(Config{
		Repository: repository, WorktreeRoot: filepath.Join(t.TempDir(), "failing-setup"),
		SetupHook: []string{"sh", "-c", "printf failed >&2; exit 7"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.Create(context.Background(), "failed-setup", base); !errors.Is(err, ErrSetupHook) {
		t.Fatalf("failing setup error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(failing.worktreeRoot, "failed-setup")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed setup worktree stat = %v", err)
	}
	command := exec.Command("git", "-C", repository, "show-ref", "--verify", "--quiet", "refs/heads/human/failed-setup")
	if err := command.Run(); err == nil {
		t.Fatal("failed setup left its branch behind")
	}
}

func TestCommitTurnRecoversCommittedIntentAndReplays(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "recover-turn", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("recovered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, task.Path, "add", "-A")
	task.PendingTurn = 1
	task.PendingPreviousCommit = base
	task.PendingHead = base
	task.PendingAutoCommit = true
	if err := engine.saveTask(task); err != nil {
		t.Fatal(err)
	}
	gitTest(t, task.Path, "commit", "--no-verify", "-q", "-m", "human: task-recover-turn turn-1")
	committed := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "HEAD"))

	artifact, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Commit != committed || !bytes.Contains(artifact.CumulativePatch, []byte("recovered")) {
		t.Fatalf("recovered artifact = %+v", artifact)
	}
	loaded, err := engine.Load(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.LatestTurn != 1 || loaded.NextTurn != 2 || loaded.PendingTurn != 0 || loaded.LatestCommit != committed {
		t.Fatalf("recovered state = %+v", loaded)
	}
	replayed, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil || replayed.Commit != artifact.Commit || !bytes.Equal(replayed.CumulativePatch, artifact.CumulativePatch) {
		t.Fatalf("replayed artifact = %+v, %v", replayed, err)
	}
}

func TestCommitTurnAdoptsHumanCommitChainAndKeepsIncrementalAnchor(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "human-commits", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("first delivered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(task.Path, "human-a.txt"), []byte("human A\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, task.Path, "add", "human-a.txt")
	gitTest(t, task.Path, "commit", "-q", "-m", "human manual A")
	if err := os.WriteFile(filepath.Join(task.Path, "human-b.txt"), []byte("human B\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, task.Path, "add", "human-b.txt")
	gitTest(t, task.Path, "commit", "-q", "-m", "human manual B")
	humanHead := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "HEAD"))

	second, err := engine.CommitTurn(context.Background(), task.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if second.Commit != humanHead || second.PreviousCommit != first.Commit ||
		!bytes.Contains(second.IncrementalPatch, []byte("human A")) ||
		!bytes.Contains(second.IncrementalPatch, []byte("human B")) ||
		bytes.Contains(second.IncrementalPatch, []byte("first delivered")) {
		t.Fatalf("human commit artifact = %+v\n%s", second, second.IncrementalPatch)
	}
	loaded, err := engine.Load(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.LatestCommit != humanHead || loaded.LatestPreviousCommit != first.Commit {
		t.Fatalf("human commit state = %+v", loaded)
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "refs/human/keep/human-commits/turn-2-previous")); got != first.Commit {
		t.Fatalf("human commit previous ref = %s, want %s", got, first.Commit)
	}
	rebuilt, err := engine.CurrentArtifact(context.Background(), task.ID, 2)
	if err != nil || !bytes.Equal(rebuilt.IncrementalPatch, second.IncrementalPatch) {
		t.Fatalf("rebuilt human commit artifact = %+v, %v", rebuilt, err)
	}
	replayed, err := engine.CommitTurn(context.Background(), task.ID, 2)
	if err != nil || !bytes.Equal(replayed.IncrementalPatch, second.IncrementalPatch) {
		t.Fatalf("replayed human commit artifact = %+v, %v", replayed, err)
	}

	if err := os.WriteFile(filepath.Join(task.Path, "human-c.txt"), []byte("human C\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, task.Path, "add", "human-c.txt")
	gitTest(t, task.Path, "commit", "-q", "-m", "human manual C")
	manualParent := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "HEAD"))
	if err := os.WriteFile(filepath.Join(task.Path, "automatic-tail.txt"), []byte("automatic tail\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	third, err := engine.CommitTurn(context.Background(), task.ID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if third.PreviousCommit != second.Commit || third.Commit == manualParent ||
		strings.TrimSpace(gitTest(t, task.Path, "rev-parse", third.Commit+"^1")) != manualParent ||
		!bytes.Contains(third.IncrementalPatch, []byte("human C")) ||
		!bytes.Contains(third.IncrementalPatch, []byte("automatic tail")) {
		t.Fatalf("human+automatic turn artifact = %+v\n%s", third, third.IncrementalPatch)
	}
}

func TestTaskOperationsRejectDetachedOrDifferentBranch(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "branch-guard", base)
	if err != nil {
		t.Fatal(err)
	}
	gitTest(t, task.Path, "checkout", "--detach", "-q")
	if err := os.WriteFile(filepath.Join(task.Path, "detached.txt"), []byte("must not deliver\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.CommitTurn(context.Background(), task.ID, 1); !errors.Is(err, ErrRecovery) {
		t.Fatalf("detached worktree commit error = %v", err)
	}
	if got := gitTest(t, task.Path, "status", "--porcelain=v1"); !strings.Contains(got, "detached.txt") {
		t.Fatalf("detached rejection lost work: %q", got)
	}
}

func TestRecoveredCommitIsRescannedFailClosed(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "recover-secret", base)
	if err != nil {
		t.Fatal(err)
	}
	// Assemble the fake token at runtime so repository secret scanners do not
	// mistake the test fixture itself for a live credential.
	if err := os.WriteFile(filepath.Join(task.Path, "secret.txt"), []byte("ghp_"+"abcdefghijklmnopqrstuvwxyzABCDEFGHIJ"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, task.Path, "add", "-A")
	task.PendingTurn = 1
	task.PendingPreviousCommit = base
	task.PendingHead = base
	task.PendingAutoCommit = true
	if err := engine.saveTask(task); err != nil {
		t.Fatal(err)
	}
	gitTest(t, task.Path, "commit", "--no-verify", "-q", "-m", "human: task-recover-secret turn-1")
	if _, err := engine.CurrentArtifact(context.Background(), task.ID, 1); !errors.Is(err, ErrUnsafeChange) {
		t.Fatalf("recovered secret scan error = %v", err)
	}
	persisted, err := engine.loadTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.PendingTurn != 1 || persisted.LatestTurn != 0 {
		t.Fatalf("unsafe recovered commit advanced state: %+v", persisted)
	}
}

func TestPendingRewindRecoversPreResetWIPAfterCrash(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "recover-rewind", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("turn one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("turn two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := engine.CommitTurn(context.Background(), task.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	task, err = engine.Load(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("unsaved WIP\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "untracked.wip"), []byte("untracked survives\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wip, err := engine.snapshotWIP(context.Background(), task, task.NextTurn)
	if err != nil {
		t.Fatal(err)
	}
	backupRef := "refs/human/backup/recover-rewind/turn-2"
	gitTest(t, task.Path, "update-ref", backupRef, second.Commit)
	receipt := RewindReceipt{
		TaskID: task.ID, FromTurn: 2, ToTurn: 1,
		FromCommit: second.Commit, ToCommit: first.Commit, WIPCommit: wip, BackupRef: backupRef,
	}
	task.PendingRewind = &receipt
	if err := engine.saveTask(task); err != nil {
		t.Fatal(err)
	}
	// Simulate process death after reset --hard and before the final state
	// rename. The authority still describes turn 2 at this point.
	gitTest(t, task.Path, "reset", "--hard", first.Commit)

	recovered, err := engine.Load(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.LatestTurn != 2 || recovered.LatestCommit != second.Commit || recovered.PendingRewind != nil {
		t.Fatalf("recovered rewind state = %+v", recovered)
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "HEAD")); got != second.Commit {
		t.Fatalf("recovered rewind HEAD = %s, want %s", got, second.Commit)
	}
	content, err := os.ReadFile(filepath.Join(task.Path, "README.md"))
	if err != nil || string(content) != "unsaved WIP\n" {
		t.Fatalf("recovered tracked WIP = %q, %v", content, err)
	}
	content, err = os.ReadFile(filepath.Join(task.Path, "untracked.wip"))
	if err != nil || string(content) != "untracked survives\n" {
		t.Fatalf("recovered untracked WIP = %q, %v", content, err)
	}
}

func TestRevisionInputsCannotBecomeGitOptions(t *testing.T) {
	engine, _, base := newTestEngine(t)
	outside := filepath.Join(t.TempDir(), "outside")
	if _, err := engine.Create(context.Background(), "bad-base", "--output="+outside); !errors.Is(err, ErrInvalidBase) {
		t.Fatalf("malicious base error = %v", err)
	}
	if _, err := os.Stat(outside); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("malicious base caused external side effect: %v", err)
	}

	task, err := engine.Create(context.Background(), "safe-revision", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := engine.CommitTurn(context.Background(), task.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	malicious := "--output=" + outside
	if _, err := engine.Rewind(context.Background(), task.ID, 1, malicious); err == nil {
		t.Fatal("malicious rewind revision was accepted")
	}
	if _, err := os.Stat(outside); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("malicious rewind caused external side effect: %v", err)
	}
	if got := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "HEAD")); got != second.Commit || got == first.Commit {
		t.Fatalf("malicious rewind changed HEAD to %s", got)
	}
}

func TestPathspecMagicFilenameIsAlwaysLiteral(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "literal-path", base)
	if err != nil {
		t.Fatal(err)
	}
	magic := ":(glob)**"
	magicContent := []byte("only the magic filename\n")
	if err := os.WriteFile(filepath.Join(task.Path, magic), magicContent, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "ordinary.txt"), []byte("ordinary\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact.Files) != 2 {
		t.Fatalf("artifact files = %+v", artifact.Files)
	}
	wantSHA := strings.TrimSpace(gitTest(t, task.Path, "hash-object", "--", magic))
	var gotSHA string
	for _, file := range artifact.Files {
		if file.Path == magic {
			gotSHA = file.BlobSHA
		}
	}
	if gotSHA != wantSHA {
		t.Fatalf("magic path blob = %q, want %q; files=%+v", gotSHA, wantSHA, artifact.Files)
	}
	if !bytes.Contains(artifact.CumulativePatch, []byte("a/:(glob)**")) ||
		!bytes.Contains(artifact.CumulativePatch, magicContent) {
		t.Fatalf("magic path missing from patch:\n%s", artifact.CumulativePatch)
	}
}

func TestRenameIsEmittedAsLiteralDeleteAndAdd(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "rename-path", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(task.Path, "README.md"), filepath.Join(task.Path, "renamed.md")); err != nil {
		t.Fatal(err)
	}
	artifact, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(artifact.CumulativePatch, []byte("rename from")) || bytes.Contains(artifact.CumulativePatch, []byte("rename to")) {
		t.Fatalf("artifact unexpectedly encoded rename/copy metadata:\n%s", artifact.CumulativePatch)
	}
	if len(artifact.Files) != 2 || artifact.Files[0] != (File{Path: "README.md", BlobSHA: "deleted", Mode: gitModeDeleted}) ||
		artifact.Files[1].Path != "renamed.md" || artifact.Files[1].BlobSHA == "deleted" ||
		artifact.Files[1].Mode != gitModeRegular {
		t.Fatalf("rename files = %+v", artifact.Files)
	}
}

func TestBinaryArtifactIsFullIndexAndSelfContained(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "binary-artifact", base)
	if err != nil {
		t.Fatal(err)
	}
	binary := make([]byte, 4<<10)
	for index := range binary {
		binary[index] = byte((index*31 + 17) % 251)
	}
	binary[0] = 0
	if err := os.WriteFile(filepath.Join(task.Path, "asset.bin"), binary, 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(artifact.CumulativePatch, []byte("GIT binary patch")) ||
		!bytes.Contains(artifact.CumulativePatch, []byte("new file mode")) ||
		!bytes.Contains(artifact.CumulativePatch, []byte("index 0000000000000000000000000000000000000000..")) {
		t.Fatalf("binary artifact is not a full-index binary patch:\n%s", artifact.CumulativePatch)
	}
	if len(artifact.Files) != 1 || artifact.Files[0].Path != "asset.bin" || artifact.Files[0].BlobSHA == "deleted" ||
		artifact.Files[0].Mode != gitModeRegular {
		t.Fatalf("binary artifact files = %+v", artifact.Files)
	}
}

func TestArtifactOracleIncludesExecutableMode(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "executable-mode", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(task.Path, "README.md"), 0o700); err != nil {
		t.Fatal(err)
	}
	artifact, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact.Files) != 1 || artifact.Files[0].Path != "README.md" ||
		artifact.Files[0].Mode != gitModeExecutable {
		t.Fatalf("executable artifact files = %+v", artifact.Files)
	}
	if !bytes.Contains(artifact.CumulativePatch, []byte("old mode 100644")) ||
		!bytes.Contains(artifact.CumulativePatch, []byte("new mode 100755")) {
		t.Fatalf("executable mode missing from patch:\n%s", artifact.CumulativePatch)
	}
}

func TestSymlinkArtifactFailsClosedBeforeCommit(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "symlink-mode", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("README.md", filepath.Join(task.Path, "link")); err != nil {
		t.Fatal(err)
	}
	headBefore := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "HEAD"))
	if _, err := engine.CommitTurn(context.Background(), task.ID, 1); !errors.Is(err, ErrUnsafeChange) {
		t.Fatalf("symlink artifact error = %v", err)
	}
	if headAfter := strings.TrimSpace(gitTest(t, task.Path, "rev-parse", "HEAD")); headAfter != headBefore {
		t.Fatalf("symlink rejection committed HEAD: %s -> %s", headBefore, headAfter)
	}
}

func TestCompleteArchivesAndRetainsBranchAndCommit(t *testing.T) {
	engine, repository, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "complete-task", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "README.md"), []byte("complete\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := engine.CommitTurn(context.Background(), task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := engine.ValidateComplete(context.Background(), task.ID)
	if err != nil || validated.ID != task.ID || validated.LatestCommit != artifact.Commit {
		t.Fatalf("completion preflight = %+v, %v", validated, err)
	}
	receipt, err := engine.Complete(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit != artifact.Commit || receipt.Branch != task.Branch || receipt.KeepRef != "refs/human/keep/complete-task/completed" {
		t.Fatalf("archive receipt = %+v", receipt)
	}
	if _, err := os.Stat(task.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed worktree stat = %v", err)
	}
	if _, err := os.Stat(receipt.StatePath); err != nil {
		t.Fatalf("archive state: %v", err)
	}
	if got := strings.TrimSpace(gitTest(t, repository, "rev-parse", receipt.KeepRef)); got != artifact.Commit {
		t.Fatalf("completion keep ref = %s", got)
	}
	if got := strings.TrimSpace(gitTest(t, repository, "rev-parse", "refs/heads/"+task.Branch)); got != artifact.Commit {
		t.Fatalf("retained branch = %s", got)
	}
	replayed, err := engine.Complete(context.Background(), task.ID)
	if err != nil || replayed != receipt {
		t.Fatalf("idempotent complete = %+v, %v", replayed, err)
	}
	if _, err := engine.ValidateComplete(context.Background(), task.ID); err == nil {
		t.Fatal("archived task passed active completion preflight")
	}
}

func TestValidateCompleteIsReadOnlyAndRejectsDirtyDivergentOrMissingWorktree(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "validate-complete", base)
	if err != nil {
		t.Fatal(err)
	}
	stateBefore, err := os.ReadFile(engine.statePath(task.ID))
	if err != nil {
		t.Fatal(err)
	}
	if validated, err := engine.ValidateComplete(context.Background(), task.ID); err != nil || validated.ID != task.ID {
		t.Fatalf("clean completion preflight = %+v, %v", validated, err)
	}
	stateAfter, err := os.ReadFile(engine.statePath(task.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stateAfter, stateBefore) {
		t.Fatal("completion preflight mutated active task state")
	}
	if _, err := os.Stat(engine.archivePath(task.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completion preflight wrote archive state: %v", err)
	}

	if err := os.WriteFile(filepath.Join(task.Path, "dirty.txt"), []byte("dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ValidateComplete(context.Background(), task.ID); !errors.Is(err, ErrDirtyWorktree) {
		t.Fatalf("dirty completion preflight error = %v", err)
	}
	if err := os.Remove(filepath.Join(task.Path, "dirty.txt")); err != nil {
		t.Fatal(err)
	}
	gitTest(t, task.Path, "commit", "--allow-empty", "-q", "-m", "divergent")
	if _, err := engine.ValidateComplete(context.Background(), task.ID); !errors.Is(err, ErrRecovery) {
		t.Fatalf("divergent completion preflight error = %v", err)
	}
	gitTest(t, task.Path, "reset", "--hard", "-q", base)
	if err := os.RemoveAll(task.Path); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ValidateComplete(context.Background(), task.ID); !errors.Is(err, ErrRecovery) {
		t.Fatalf("missing worktree completion preflight error = %v", err)
	}
}

func TestRepositoryProcessLockHonorsContext(t *testing.T) {
	first, repository, _ := newTestEngine(t)
	second, err := Open(Config{Repository: repository, WorktreeRoot: first.worktreeRoot})
	if err != nil {
		t.Fatal(err)
	}
	releaseFirst, err := first.acquireProcessLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if _, err := second.acquireProcessLock(ctx); !errors.Is(err, context.DeadlineExceeded) {
		releaseFirst()
		t.Fatalf("contended process lock error = %v", err)
	}
	releaseFirst()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	releaseSecond, err := second.acquireProcessLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond()
}

func TestRemoveRefusesDirtyWorktree(t *testing.T) {
	engine, _, base := newTestEngine(t)
	task, err := engine.Create(context.Background(), "dirty-task", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(task.Path, "dirty.txt"), []byte("dirty"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := engine.Remove(context.Background(), task.ID); !errors.Is(err, ErrDirtyWorktree) {
		t.Fatalf("remove dirty error = %v", err)
	}
}

func gitTest(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}
