// Package worktree owns the git operations performed on the human worker's
// machine for asynchronous delegation tasks. The daemon treats artifacts as
// opaque bytes and never imports this package.
package worktree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidTaskID = errors.New("worktree task id is not a stable key")
	ErrInvalidBase   = errors.New("worktree base commit must be a full object id")
	ErrInvalidRemote = errors.New("shared remote must be explicitly configured")
	ErrSetupHook     = errors.New("configured worktree setup hook failed")
	ErrUnsafeChange  = errors.New("staged changes failed artifact safety scan")
	ErrDirtyWorktree = errors.New("task worktree contains uncommitted changes")
	ErrRecovery      = errors.New("worktree turn recovery is ambiguous")
)

var (
	taskKey          = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$`)
	fullObjectID     = regexp.MustCompile(`^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$`)
	awsAccessKey     = regexp.MustCompile(`AKIA[0-9A-Z]{16}`)
	githubToken      = regexp.MustCompile(`gh[pousr]_[A-Za-z0-9_]{30,}`)
	slackToken       = regexp.MustCompile(`xox[baprs]-[A-Za-z0-9-]{20,}`)
	privateKeyMarker = regexp.MustCompile(`-----BEGIN (?:OPENSSH |RSA |EC |DSA )?PRIVATE KEY-----`)
)

type Config struct {
	Repository   string
	WorktreeRoot string
	// SharedRemote is an explicit configured git remote name. When non-empty,
	// Create must fetch the requested immutable base from this exact remote.
	// The engine never guesses origin or any other configured remote.
	SharedRemote string
	// SetupHook is an explicit argv vector executed in a newly-created
	// worktree. An empty vector disables setup; no repository file or shell
	// convention is discovered automatically.
	SetupHook    []string
	MaxFileBytes int64
	AuthorName   string
	AuthorEmail  string
}

type Engine struct {
	mu           sync.Mutex
	repository   string
	worktreeRoot string
	stateRoot    string
	archiveRoot  string
	lockPath     string
	sharedRemote string
	setupHook    []string
	maxFileBytes int64
	authorName   string
	authorEmail  string
}

type Task struct {
	ID           string `json:"id"`
	BaseCommit   string `json:"base_commit"`
	SharedRemote string `json:"shared_remote,omitempty"`
	Branch       string `json:"branch"`
	Path         string `json:"path"`
	LatestTurn   int    `json:"latest_turn"`
	NextTurn     int    `json:"next_turn"`
	LatestCommit string `json:"latest_commit,omitempty"`
	// LatestPreviousCommit is the previous delivered anchor, which may differ
	// from LatestCommit^1 when a turn contains several human commits or a merge.
	LatestPreviousCommit string `json:"latest_previous_commit,omitempty"`
	// PendingTurn is persisted before git commit. On restart, HEAD together
	// with the two pending anchors makes the commit/state crash window
	// unambiguous and recoverable. PendingPreviousCommit is the previous
	// delivered turn; PendingHead is HEAD before an optional automatic commit.
	PendingTurn           int            `json:"pending_turn,omitempty"`
	PendingPreviousCommit string         `json:"pending_previous_commit,omitempty"`
	PendingHead           string         `json:"pending_head,omitempty"`
	PendingAutoCommit     bool           `json:"pending_auto_commit,omitempty"`
	PendingRewind         *RewindReceipt `json:"pending_rewind,omitempty"`
}

type File struct {
	Path    string `json:"path"`
	BlobSHA string `json:"blob_sha"`
	Mode    string `json:"mode"`
}

const (
	gitModeDeleted    = "000000"
	gitModeRegular    = "100644"
	gitModeExecutable = "100755"
)

type Artifact struct {
	TaskID           string `json:"task_id"`
	Turn             int    `json:"turn"`
	BaseCommit       string `json:"base_commit"`
	Commit           string `json:"commit"`
	PreviousCommit   string `json:"previous_commit,omitempty"`
	CumulativePatch  []byte `json:"-"`
	IncrementalPatch []byte `json:"-"`
	Files            []File `json:"files"`
}

// RewindReceipt is the durable-enough compensation token for the authority
// CAS that follows a local rewind. WIPCommit captures tracked and untracked
// content; FromCommit is also pinned by refs/human/backup/... before reset.
type RewindReceipt struct {
	TaskID     string `json:"task_id"`
	FromTurn   int    `json:"from_turn"`
	ToTurn     int    `json:"to_turn"`
	FromCommit string `json:"from_commit"`
	ToCommit   string `json:"to_commit"`
	WIPCommit  string `json:"wip_commit"`
	BackupRef  string `json:"backup_ref"`
}

// ArchiveReceipt identifies the refs and state retained after a completed
// patch-delivery task has had its worktree safely removed.
type ArchiveReceipt struct {
	TaskID    string `json:"task_id"`
	Commit    string `json:"commit"`
	Branch    string `json:"branch"`
	KeepRef   string `json:"keep_ref"`
	StatePath string `json:"state_path"`
}

type archivedTask struct {
	Task      Task   `json:"task"`
	KeepRef   string `json:"keep_ref"`
	Completed bool   `json:"completed"`
}

func Open(config Config) (*Engine, error) {
	repository, err := filepath.Abs(strings.TrimSpace(config.Repository))
	if err != nil || strings.TrimSpace(config.Repository) == "" {
		return nil, errors.New("repository path is required")
	}
	worktreeRoot := strings.TrimSpace(config.WorktreeRoot)
	if worktreeRoot == "" {
		worktreeRoot = filepath.Join(repository, ".human", "wt")
	}
	worktreeRoot, err = filepath.Abs(worktreeRoot)
	if err != nil {
		return nil, err
	}
	if config.MaxFileBytes <= 0 {
		config.MaxFileBytes = 16 << 20
	}
	sharedRemote := strings.TrimSpace(config.SharedRemote)
	if strings.ContainsRune(sharedRemote, 0) || strings.HasPrefix(sharedRemote, "-") {
		return nil, ErrInvalidRemote
	}
	setupHook := append([]string(nil), config.SetupHook...)
	for _, argument := range setupHook {
		if strings.ContainsRune(argument, 0) {
			return nil, errors.New("setup hook argument contains NUL")
		}
	}
	if len(setupHook) > 0 && strings.TrimSpace(setupHook[0]) == "" {
		return nil, errors.New("setup hook executable is empty")
	}
	engine := &Engine{
		repository: repository, worktreeRoot: worktreeRoot,
		stateRoot: filepath.Join(worktreeRoot, ".state"), archiveRoot: filepath.Join(worktreeRoot, ".archive"),
		sharedRemote: sharedRemote, setupHook: setupHook, maxFileBytes: config.MaxFileBytes,
		authorName: strings.TrimSpace(config.AuthorName), authorEmail: strings.TrimSpace(config.AuthorEmail),
	}
	if _, err := engine.git(context.Background(), repository, "rev-parse", "--git-dir"); err != nil {
		return nil, fmt.Errorf("repository is not a git worktree: %w", err)
	}
	commonDirectory, err := engine.git(
		context.Background(), repository, "rev-parse", "--path-format=absolute", "--git-common-dir",
	)
	if err != nil {
		return nil, fmt.Errorf("resolve common git directory: %w", err)
	}
	commonPath := strings.TrimSpace(string(commonDirectory))
	if !filepath.IsAbs(commonPath) {
		commonPath = filepath.Join(repository, commonPath)
	}
	commonPath, err = filepath.Abs(commonPath)
	if err != nil {
		return nil, fmt.Errorf("normalize common git directory: %w", err)
	}
	lockRoot := filepath.Join(commonPath, "human-agent")
	if err := os.MkdirAll(lockRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create worktree lock directory: %w", err)
	}
	engine.lockPath = filepath.Join(lockRoot, "worktree.lock")
	if sharedRemote != "" {
		if _, err := engine.git(context.Background(), repository, "remote", "get-url", "--", sharedRemote); err != nil {
			return nil, fmt.Errorf("%w: %q is not a configured git remote: %v", ErrInvalidRemote, sharedRemote, err)
		}
	}
	if err := os.MkdirAll(engine.stateRoot, 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(engine.archiveRoot, 0o700); err != nil {
		return nil, err
	}
	return engine, nil
}

func (engine *Engine) Create(ctx context.Context, taskID, baseCommit string) (Task, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(ctx)
	if err != nil {
		return Task{}, err
	}
	defer release()
	if !taskKey.MatchString(taskID) {
		return Task{}, ErrInvalidTaskID
	}
	base, err := engine.resolveBase(ctx, baseCommit)
	if err != nil {
		return Task{}, err
	}
	if _, err := os.Stat(engine.statePath(taskID)); err == nil {
		return Task{}, fmt.Errorf("task %q already exists", taskID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Task{}, err
	}
	path := filepath.Join(engine.worktreeRoot, taskID)
	if _, err := os.Stat(path); err == nil {
		return Task{}, fmt.Errorf("worktree path %s already exists", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Task{}, err
	}
	if err := os.MkdirAll(engine.worktreeRoot, 0o700); err != nil {
		return Task{}, err
	}
	branch := "human/" + taskID
	if _, err := engine.git(ctx, engine.repository, "show-ref", "--verify", "--quiet", "refs/heads/"+branch); err == nil {
		return Task{}, fmt.Errorf("branch %q already exists", branch)
	}
	if _, err := engine.git(ctx, engine.repository, "worktree", "add", "-b", branch, path, base); err != nil {
		return Task{}, fmt.Errorf("create task worktree: %w", err)
	}
	cleanup := func(cause error) error {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		var cleanupErrors []error
		if _, cleanupErr := engine.git(cleanupCtx, engine.repository, "worktree", "remove", "--force", path); cleanupErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove failed setup worktree: %w", cleanupErr))
		}
		if _, cleanupErr := engine.git(cleanupCtx, engine.repository, "branch", "-D", branch); cleanupErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove failed setup branch: %w", cleanupErr))
		}
		return errors.Join(append([]error{cause}, cleanupErrors...)...)
	}
	if err := engine.runSetupHook(ctx, taskID, path, base); err != nil {
		return Task{}, cleanup(err)
	}
	task := Task{
		ID: taskID, BaseCommit: base, SharedRemote: engine.sharedRemote,
		Branch: branch, Path: path, NextTurn: 1,
	}
	if err := engine.saveTask(task); err != nil {
		return Task{}, cleanup(err)
	}
	return task, nil
}

func (engine *Engine) Load(taskID string) (Task, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(context.Background())
	if err != nil {
		return Task{}, err
	}
	defer release()
	task, _, err := engine.loadTaskForOperation(context.Background(), taskID)
	return task, err
}

func (engine *Engine) SnapshotWIP(ctx context.Context, taskID string, sequence int) (string, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	task, _, err := engine.loadTaskForOperation(ctx, taskID)
	if err != nil {
		return "", err
	}
	return engine.snapshotWIP(ctx, task, sequence)
}

func (engine *Engine) CommitTurn(ctx context.Context, taskID string, turn int) (Artifact, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(ctx)
	if err != nil {
		return Artifact{}, err
	}
	defer release()
	task, recovered, err := engine.loadTaskForOperation(ctx, taskID)
	if err != nil {
		return Artifact{}, err
	}
	if recovered != nil && recovered.Turn == turn {
		return *recovered, nil
	}
	// DeliverTurn retries are idempotent even after the final state rename was
	// acknowledged locally but its response was lost.
	if turn > 0 && turn == task.LatestTurn && strings.TrimSpace(task.LatestCommit) != "" {
		return engine.buildArtifact(ctx, task, turn, task.LatestCommit, task.LatestPreviousCommit)
	}
	if turn != task.NextTurn || turn < 1 {
		return Artifact{}, fmt.Errorf("turn must use monotonic next turn %d", task.NextTurn)
	}
	headBeforeCommit, err := engine.resolveCommit(ctx, task.Path, "HEAD")
	if err != nil {
		return Artifact{}, err
	}
	previousDelivered := task.BaseCommit
	if task.LatestTurn > 0 {
		previousDelivered = task.LatestCommit
	}
	if headBeforeCommit != previousDelivered {
		ancestor, err := engine.isAncestor(ctx, task.Path, previousDelivered, headBeforeCommit)
		if err != nil {
			return Artifact{}, fmt.Errorf("verify human commit ancestry: %w", err)
		}
		if !ancestor {
			return Artifact{}, fmt.Errorf(
				"%w: HEAD %s is not a descendant of persisted anchor %s",
				ErrRecovery,
				headBeforeCommit,
				previousDelivered,
			)
		}
		// Human-created commits are part of this turn. Scan their immutable
		// cumulative content before adding an automatic tail commit.
		if err := engine.scanCommit(ctx, task, headBeforeCommit); err != nil {
			return Artifact{}, err
		}
	}
	if _, err := engine.git(ctx, task.Path, "add", "-A"); err != nil {
		return Artifact{}, fmt.Errorf("stage task changes: %w", err)
	}
	if err := engine.scanStaged(ctx, task); err != nil {
		return Artifact{}, engine.resetIndexAfterFailure(ctx, task, err)
	}
	staged, err := engine.git(ctx, task.Path, "diff", "--cached", "--name-only", "-z")
	if err != nil {
		return Artifact{}, fmt.Errorf("inspect staged task changes: %w", err)
	}
	autoCommit := len(staged) != 0
	if !autoCommit && headBeforeCommit == previousDelivered {
		return Artifact{}, errors.New("turn has no changes")
	}
	task.PendingTurn = turn
	task.PendingPreviousCommit = previousDelivered
	task.PendingHead = headBeforeCommit
	task.PendingAutoCommit = autoCommit
	if err := engine.saveTask(task); err != nil {
		return Artifact{}, fmt.Errorf("persist turn commit intent: %w", err)
	}
	if !autoCommit {
		finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		_, artifact, err := engine.finishPendingTurn(finalizeCtx, task)
		if err != nil {
			return Artifact{}, err
		}
		if artifact == nil {
			return Artifact{}, fmt.Errorf("%w: human commit turn was not finalized", ErrRecovery)
		}
		return *artifact, nil
	}
	args := []string{"-c", "commit.gpgSign=false"}
	if engine.authorName != "" {
		args = append(args, "-c", "user.name="+engine.authorName)
	}
	if engine.authorEmail != "" {
		args = append(args, "-c", "user.email="+engine.authorEmail)
	}
	args = append(args, "commit", "--no-verify", "-m", fmt.Sprintf("human: task-%s turn-%d", task.ID, turn))
	if _, err := engine.git(ctx, task.Path, args...); err != nil {
		commitErr := fmt.Errorf("commit task turn: %w", err)
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		recoveredTask, artifact, recoveryErr := engine.finishPendingTurn(recoveryCtx, task)
		if recoveryErr == nil && artifact != nil {
			return *artifact, nil
		}
		if recoveryErr == nil {
			_ = recoveredTask
			return Artifact{}, commitErr
		}
		return Artifact{}, errors.Join(commitErr, fmt.Errorf("recover failed turn commit: %w", recoveryErr))
	}
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	_, artifact, err := engine.finishPendingTurn(finalizeCtx, task)
	if err != nil {
		return Artifact{}, err
	}
	if artifact == nil {
		return Artifact{}, fmt.Errorf("%w: committed turn did not advance HEAD", ErrRecovery)
	}
	return *artifact, nil
}

// CurrentArtifact deterministically rebuilds the most recently committed turn.
// It closes the crash/retry window where git and local task state advanced but
// the authority did not persist (or did not acknowledge) DeliverTurn.
func (engine *Engine) CurrentArtifact(ctx context.Context, taskID string, turn int) (Artifact, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(ctx)
	if err != nil {
		return Artifact{}, err
	}
	defer release()
	task, recovered, err := engine.loadTaskForOperation(ctx, taskID)
	if err != nil {
		return Artifact{}, err
	}
	if recovered != nil && recovered.Turn == turn {
		return *recovered, nil
	}
	if turn < 1 || task.LatestTurn != turn || strings.TrimSpace(task.LatestCommit) == "" {
		return Artifact{}, fmt.Errorf("turn %d is not the current local artifact", turn)
	}
	commit, err := engine.resolveCommit(ctx, task.Path, "HEAD")
	if err != nil {
		return Artifact{}, err
	}
	if commit != task.LatestCommit {
		return Artifact{}, fmt.Errorf("worktree HEAD %s differs from persisted turn commit %s", commit, task.LatestCommit)
	}
	return engine.buildArtifact(ctx, task, turn, commit, task.LatestPreviousCommit)
}

func (engine *Engine) Rewind(ctx context.Context, taskID string, toTurn int, toCommit string) (Task, error) {
	task, _, err := engine.RewindWithReceipt(ctx, taskID, toTurn, toCommit)
	return task, err
}

// RewindWithReceipt snapshots all current content, pins the committed HEAD,
// and resets to an older turn. Callers must keep the receipt until their
// authority confirms the rewind; RestoreRewind compensates a failed CAS.
func (engine *Engine) RewindWithReceipt(
	ctx context.Context,
	taskID string,
	toTurn int,
	toCommit string,
) (Task, RewindReceipt, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(ctx)
	if err != nil {
		return Task{}, RewindReceipt{}, err
	}
	defer release()
	task, _, err := engine.loadTaskForOperation(ctx, taskID)
	if err != nil {
		return Task{}, RewindReceipt{}, err
	}
	if toTurn < 0 || toTurn >= task.LatestTurn {
		return Task{}, RewindReceipt{}, fmt.Errorf("rewind target turn %d must precede latest turn %d", toTurn, task.LatestTurn)
	}
	target, err := engine.resolveCommit(ctx, task.Path, toCommit)
	if err != nil {
		return Task{}, RewindReceipt{}, err
	}
	expectedTarget := task.BaseCommit
	targetPrevious := ""
	if toTurn > 0 {
		expectedTarget, err = engine.resolveCommit(
			ctx,
			task.Path,
			fmt.Sprintf("refs/human/keep/%s/turn-%d", task.ID, toTurn),
		)
		if err != nil {
			return Task{}, RewindReceipt{}, fmt.Errorf("resolve pinned rewind turn %d: %w", toTurn, err)
		}
		targetPrevious, err = engine.resolveCommit(
			ctx,
			task.Path,
			fmt.Sprintf("refs/human/keep/%s/turn-%d-previous", task.ID, toTurn),
		)
		if err != nil {
			return Task{}, RewindReceipt{}, fmt.Errorf("resolve previous anchor for rewind turn %d: %w", toTurn, err)
		}
	}
	if target != expectedTarget {
		return Task{}, RewindReceipt{}, fmt.Errorf(
			"rewind target commit %s differs from pinned turn %d commit %s",
			target,
			toTurn,
			expectedTarget,
		)
	}
	current, err := engine.resolveCommit(ctx, task.Path, "HEAD")
	if err != nil {
		return Task{}, RewindReceipt{}, err
	}
	if task.LatestCommit != "" && current != task.LatestCommit {
		return Task{}, RewindReceipt{}, fmt.Errorf("worktree HEAD %s differs from persisted turn commit %s", current, task.LatestCommit)
	}
	wip, err := engine.snapshotWIP(ctx, task, task.NextTurn)
	if err != nil {
		return Task{}, RewindReceipt{}, fmt.Errorf("snapshot before rewind: %w", err)
	}
	backupRef := fmt.Sprintf("refs/human/backup/%s/turn-%d", task.ID, task.LatestTurn)
	keepRef := fmt.Sprintf("refs/human/keep/%s/turn-%d", task.ID, task.LatestTurn)
	if _, err := engine.git(ctx, task.Path, "update-ref", backupRef, current); err != nil {
		return Task{}, RewindReceipt{}, fmt.Errorf("write rewind backup ref: %w", err)
	}
	if _, err := engine.git(ctx, task.Path, "update-ref", keepRef, current); err != nil {
		return Task{}, RewindReceipt{}, fmt.Errorf("write rewind keep ref: %w", err)
	}
	receipt := RewindReceipt{
		TaskID: task.ID, FromTurn: task.LatestTurn, ToTurn: toTurn,
		FromCommit: current, ToCommit: target, WIPCommit: wip, BackupRef: backupRef,
	}
	task.PendingRewind = &receipt
	if err := engine.saveTask(task); err != nil {
		return Task{}, RewindReceipt{}, fmt.Errorf("persist rewind intent: %w", err)
	}
	if _, err := engine.git(ctx, task.Path, "reset", "--hard", target); err != nil {
		resetErr := fmt.Errorf("rewind task branch: %w", err)
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if _, recoveryErr := engine.recoverPendingRewind(recoveryCtx, task); recoveryErr != nil {
			return Task{}, RewindReceipt{}, errors.Join(resetErr, recoveryErr)
		}
		return Task{}, RewindReceipt{}, resetErr
	}
	task.LatestTurn = toTurn
	task.LatestCommit = target
	task.LatestPreviousCommit = targetPrevious
	task.PendingRewind = nil
	if err := engine.saveTask(task); err != nil {
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if _, restoreErr := engine.restoreRewindLocked(recoveryCtx, task, receipt); restoreErr != nil {
			return Task{}, RewindReceipt{}, errors.Join(err, fmt.Errorf("restore rewind after state failure: %w", restoreErr))
		}
		return Task{}, RewindReceipt{}, err
	}
	return task, receipt, nil
}

// RestoreRewind restores the exact pre-rewind committed anchor and materializes
// the WIP snapshot as ordinary working-tree changes. It is intentionally valid
// only while the task still matches the receipt's rewind target.
func (engine *Engine) RestoreRewind(ctx context.Context, receipt RewindReceipt) (Task, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(ctx)
	if err != nil {
		return Task{}, err
	}
	defer release()
	task, _, err := engine.loadTaskForOperation(ctx, receipt.TaskID)
	if err != nil {
		return Task{}, err
	}
	return engine.restoreRewindLocked(ctx, task, receipt)
}

// ValidateComplete performs the caller-visible completion preflight without
// pinning refs, writing archive state, or removing the worktree. It deliberately
// does not recover pending operations: a completion decision must be based on a
// stable active-task snapshot.
func (engine *Engine) ValidateComplete(ctx context.Context, taskID string) (Task, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(ctx)
	if err != nil {
		return Task{}, err
	}
	defer release()
	if !taskKey.MatchString(taskID) {
		return Task{}, ErrInvalidTaskID
	}
	task, err := engine.loadTask(taskID)
	if err != nil {
		return Task{}, fmt.Errorf("load active task for completion: %w", err)
	}
	if _, err := engine.validateCompleteTask(ctx, task); err != nil {
		return Task{}, err
	}
	return task, nil
}

func (engine *Engine) validateCompleteTask(ctx context.Context, task Task) (string, error) {
	if task.PendingTurn != 0 || task.PendingRewind != nil {
		return "", fmt.Errorf("%w: task has a pending operation", ErrRecovery)
	}
	info, err := os.Stat(task.Path)
	if errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("%w: active task worktree is missing", ErrRecovery)
	}
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%w: active task worktree is not a directory", ErrRecovery)
	}
	status, err := engine.git(ctx, task.Path, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return "", err
	}
	if len(bytes.TrimSpace(status)) != 0 {
		return "", ErrDirtyWorktree
	}
	commit, err := engine.resolveCommit(ctx, task.Path, "HEAD")
	if err != nil {
		return "", err
	}
	expected := task.BaseCommit
	if task.LatestTurn > 0 {
		expected = task.LatestCommit
	}
	if commit != expected {
		return "", fmt.Errorf("%w: final HEAD %s differs from task anchor %s", ErrRecovery, commit, expected)
	}
	return commit, nil
}

// Complete implements the default patch-delivery cleanup: it refuses dirty or
// divergent worktrees, pins the final commit, retains the human/<task> branch,
// writes an archive record, and only then removes the worktree. It never merges
// or pushes, so the same change cannot flow back through two independent paths.
func (engine *Engine) Complete(ctx context.Context, taskID string) (ArchiveReceipt, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(ctx)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	defer release()
	if !taskKey.MatchString(taskID) {
		return ArchiveReceipt{}, ErrInvalidTaskID
	}
	statePath := engine.statePath(taskID)
	if _, err := os.Stat(statePath); errors.Is(err, os.ErrNotExist) {
		archived, loadErr := engine.loadArchivedTask(taskID)
		if loadErr != nil {
			return ArchiveReceipt{}, loadErr
		}
		if !archived.Completed {
			return ArchiveReceipt{}, fmt.Errorf("%w: archive exists but completion was not finalized", ErrRecovery)
		}
		return engine.verifiedArchiveReceipt(ctx, archived)
	} else if err != nil {
		return ArchiveReceipt{}, err
	}
	task, err := engine.loadTask(taskID)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	if _, err := os.Stat(task.Path); errors.Is(err, os.ErrNotExist) {
		archived, loadErr := engine.loadArchivedTask(taskID)
		if loadErr != nil {
			return ArchiveReceipt{}, fmt.Errorf("%w: worktree disappeared without a prepared archive: %v", ErrRecovery, loadErr)
		}
		commit, resolveErr := engine.resolveCommit(ctx, engine.repository, archived.KeepRef)
		expected := archived.Task.BaseCommit
		if archived.Task.LatestTurn > 0 {
			expected = archived.Task.LatestCommit
		}
		if resolveErr != nil || commit != expected {
			return ArchiveReceipt{}, fmt.Errorf("%w: prepared archive keep ref is invalid", ErrRecovery)
		}
		archived.Completed = true
		if err := engine.saveArchivedTask(archived); err != nil {
			return ArchiveReceipt{}, err
		}
		if err := os.Remove(statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return ArchiveReceipt{}, err
		}
		return engine.verifiedArchiveReceipt(ctx, archived)
	} else if err != nil {
		return ArchiveReceipt{}, err
	}
	task, _, err = engine.loadTaskForOperation(ctx, taskID)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	commit, err := engine.validateCompleteTask(ctx, task)
	if err != nil {
		return ArchiveReceipt{}, err
	}
	keepRef := fmt.Sprintf("refs/human/keep/%s/completed", task.ID)
	if _, err := engine.git(ctx, task.Path, "update-ref", keepRef, commit); err != nil {
		return ArchiveReceipt{}, fmt.Errorf("pin completed task: %w", err)
	}
	archived := archivedTask{Task: task, KeepRef: keepRef}
	if err := engine.saveArchivedTask(archived); err != nil {
		return ArchiveReceipt{}, fmt.Errorf("prepare task archive: %w", err)
	}
	if _, err := engine.git(ctx, engine.repository, "worktree", "remove", task.Path); err != nil {
		return ArchiveReceipt{}, fmt.Errorf("remove completed task worktree: %w", err)
	}
	archived.Completed = true
	if err := engine.saveArchivedTask(archived); err != nil {
		return ArchiveReceipt{}, fmt.Errorf("finalize task archive: %w", err)
	}
	if err := os.Remove(statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ArchiveReceipt{}, fmt.Errorf("remove active task state: %w", err)
	}
	return engine.verifiedArchiveReceipt(ctx, archived)
}

// Remove is retained as a compatibility wrapper for callers that only need an
// error result. Completed work is archived rather than destructively deleted.
func (engine *Engine) Remove(ctx context.Context, taskID string) error {
	_, err := engine.Complete(ctx, taskID)
	return err
}

// DiscardCreated removes a pristine, never-delivered worktree and its branch.
// It is the compensation step when authority AcceptTask loses its revision CAS.
func (engine *Engine) DiscardCreated(ctx context.Context, taskID string) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := engine.acquireProcessLock(ctx)
	if err != nil {
		return err
	}
	defer release()
	task, _, err := engine.loadTaskForOperation(ctx, taskID)
	if err != nil {
		return err
	}
	if task.LatestTurn != 0 || task.NextTurn != 1 {
		return fmt.Errorf("cannot discard task %q after a delivered turn", taskID)
	}
	current, err := engine.resolveCommit(ctx, task.Path, "HEAD")
	if err != nil {
		return err
	}
	status, err := engine.git(ctx, task.Path, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return err
	}
	if current != task.BaseCommit || len(bytes.TrimSpace(status)) != 0 {
		return ErrDirtyWorktree
	}
	if _, err := engine.git(ctx, engine.repository, "worktree", "remove", task.Path); err != nil {
		return fmt.Errorf("remove rejected task worktree: %w", err)
	}
	if _, err := engine.git(ctx, engine.repository, "branch", "-D", task.Branch); err != nil {
		return fmt.Errorf("remove rejected task branch: %w", err)
	}
	return os.Remove(engine.statePath(taskID))
}

func (engine *Engine) snapshotWIP(ctx context.Context, task Task, sequence int) (string, error) {
	if sequence < 1 {
		return "", errors.New("WIP sequence must be positive")
	}
	temporary, err := os.CreateTemp(engine.stateRoot, ".index-*")
	if err != nil {
		return "", err
	}
	indexPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(indexPath); err != nil {
		return "", err
	}
	defer os.Remove(indexPath)
	env := append(os.Environ(), "GIT_INDEX_FILE="+indexPath)
	if _, err := engine.gitEnv(ctx, task.Path, env, "read-tree", "HEAD"); err != nil {
		return "", err
	}
	if _, err := engine.gitEnv(ctx, task.Path, env, "add", "-A"); err != nil {
		return "", err
	}
	treeBytes, err := engine.gitEnv(ctx, task.Path, env, "write-tree")
	if err != nil {
		return "", err
	}
	parent, err := engine.resolveCommit(ctx, task.Path, "HEAD")
	if err != nil {
		return "", err
	}
	message := fmt.Sprintf("human: task-%s wip-%d", task.ID, sequence)
	commitBytes, err := engine.gitEnv(ctx, task.Path, engine.authorEnv(env), "commit-tree", strings.TrimSpace(string(treeBytes)), "-p", parent, "-m", message)
	if err != nil {
		return "", err
	}
	commit := strings.TrimSpace(string(commitBytes))
	ref := fmt.Sprintf("refs/human/wip/%s/%d", task.ID, sequence)
	if _, err := engine.git(ctx, task.Path, "update-ref", ref, commit); err != nil {
		return "", err
	}
	return commit, nil
}

func (engine *Engine) restoreRewindLocked(
	ctx context.Context,
	task Task,
	receipt RewindReceipt,
) (Task, error) {
	if receipt.TaskID != task.ID || receipt.FromTurn <= receipt.ToTurn ||
		task.LatestTurn != receipt.ToTurn || task.LatestCommit != receipt.ToCommit ||
		strings.TrimSpace(receipt.WIPCommit) == "" || strings.TrimSpace(receipt.BackupRef) == "" {
		return Task{}, errors.New("rewind receipt does not match current task state")
	}
	backup, err := engine.resolveCommit(ctx, task.Path, receipt.BackupRef)
	if err != nil {
		return Task{}, fmt.Errorf("resolve rewind backup: %w", err)
	}
	if backup != receipt.FromCommit {
		return Task{}, errors.New("rewind backup ref differs from receipt")
	}
	previous, err := engine.resolveCommit(
		ctx,
		task.Path,
		fmt.Sprintf("refs/human/keep/%s/turn-%d-previous", task.ID, receipt.FromTurn),
	)
	if err != nil {
		return Task{}, fmt.Errorf("resolve rewind source previous anchor: %w", err)
	}
	wip, err := engine.resolveCommit(ctx, task.Path, receipt.WIPCommit)
	if err != nil {
		return Task{}, fmt.Errorf("resolve rewind WIP snapshot: %w", err)
	}
	if wip != receipt.WIPCommit {
		return Task{}, errors.New("rewind WIP ref differs from receipt")
	}
	if _, err := engine.git(ctx, task.Path, "reset", "--hard", wip); err != nil {
		return Task{}, fmt.Errorf("restore rewind WIP tree: %w", err)
	}
	if _, err := engine.git(ctx, task.Path, "reset", "--mixed", backup); err != nil {
		return Task{}, fmt.Errorf("restore rewind committed anchor: %w", err)
	}
	task.LatestTurn = receipt.FromTurn
	task.LatestCommit = backup
	task.LatestPreviousCommit = previous
	task.PendingRewind = nil
	if err := engine.saveTask(task); err != nil {
		return Task{}, err
	}
	return task, nil
}

func (engine *Engine) buildArtifact(
	ctx context.Context,
	task Task,
	turn int,
	commit string,
	previous string,
) (Artifact, error) {
	// Re-scan the immutable committed tree before every initial delivery and
	// replay. This keeps crash recovery fail-closed instead of trusting that a
	// pre-commit scan happened in the previous process.
	if err := engine.scanCommit(ctx, task, commit); err != nil {
		return Artifact{}, err
	}
	cumulative, err := engine.git(ctx, task.Path, "diff", "--no-renames", "--full-index", "--binary", task.BaseCommit+"..."+commit)
	if err != nil {
		return Artifact{}, fmt.Errorf("build cumulative patch: %w", err)
	}
	incremental, err := engine.git(ctx, task.Path, "diff", "--no-renames", "--full-index", "--binary", previous+"..."+commit)
	if err != nil {
		return Artifact{}, fmt.Errorf("build incremental patch: %w", err)
	}
	files, err := engine.artifactFiles(ctx, task, commit)
	if err != nil {
		return Artifact{}, err
	}
	return Artifact{
		TaskID: task.ID, Turn: turn, BaseCommit: task.BaseCommit, Commit: commit,
		PreviousCommit: previous, CumulativePatch: cumulative, IncrementalPatch: incremental, Files: files,
	}, nil
}

func (engine *Engine) scanStaged(ctx context.Context, task Task) error {
	output, err := engine.git(ctx, task.Path, "diff", "--cached", "--no-renames", "--name-only", "--diff-filter=ACMRT", "-z")
	if err != nil {
		return err
	}
	return engine.scanObjects(ctx, task, ":", output)
}

func (engine *Engine) scanCommit(ctx context.Context, task Task, commit string) error {
	output, err := engine.git(
		ctx,
		task.Path,
		"diff", "--no-renames", "--name-only", "--diff-filter=ACMRT", "-z", task.BaseCommit+"..."+commit,
	)
	if err != nil {
		return fmt.Errorf("list committed artifact files: %w", err)
	}
	return engine.scanObjects(ctx, task, commit+":", output)
}

func (engine *Engine) scanObjects(ctx context.Context, task Task, objectPrefix string, paths []byte) error {
	for _, rawPath := range bytes.Split(paths, []byte{0}) {
		if len(rawPath) == 0 {
			continue
		}
		path := string(rawPath)
		lowerPath := strings.ToLower(filepath.ToSlash(path))
		if strings.Contains(lowerPath, "/.git/") || strings.HasPrefix(lowerPath, ".git/") || lowerPath == ".git" {
			return fmt.Errorf("%w: git administrative path %s", ErrUnsafeChange, path)
		}
		if err := engine.validateRegularObjectMode(ctx, task.Path, objectPrefix, path); err != nil {
			return err
		}
		object := objectPrefix + path
		objectType, err := engine.git(ctx, task.Path, "cat-file", "-t", object)
		if err != nil {
			return fmt.Errorf("%w: inspect object type for %s: %v", ErrUnsafeChange, path, err)
		}
		if strings.TrimSpace(string(objectType)) != "blob" {
			return fmt.Errorf("%w: unsupported non-blob change at %s", ErrUnsafeChange, path)
		}
		sizeBytes, err := engine.git(ctx, task.Path, "cat-file", "-s", object)
		if err != nil {
			return fmt.Errorf("%w: inspect object size for %s: %v", ErrUnsafeChange, path, err)
		}
		size, err := strconv.ParseInt(strings.TrimSpace(string(sizeBytes)), 10, 64)
		if err != nil || size < 0 {
			return fmt.Errorf("%w: invalid object size for %s", ErrUnsafeChange, path)
		}
		if size > engine.maxFileBytes {
			return fmt.Errorf("%w: %s exceeds %d bytes", ErrUnsafeChange, path, engine.maxFileBytes)
		}
		content, err := engine.git(ctx, task.Path, "show", object)
		if err != nil {
			return fmt.Errorf("%w: inspect content for %s: %v", ErrUnsafeChange, path, err)
		}
		if int64(len(content)) != size {
			return fmt.Errorf("%w: object size changed while scanning %s", ErrUnsafeChange, path)
		}
		if privateKeyMarker.Match(content) || awsAccessKey.Match(content) ||
			githubToken.Match(content) || slackToken.Match(content) {
			return fmt.Errorf("%w: possible credential in %s", ErrUnsafeChange, path)
		}
	}
	return nil
}

func (engine *Engine) validateRegularObjectMode(
	ctx context.Context,
	directory string,
	objectPrefix string,
	path string,
) error {
	if objectPrefix == ":" {
		output, err := engine.git(ctx, directory, "ls-files", "--stage", "-z", "--", path)
		if err != nil {
			return fmt.Errorf("%w: inspect staged mode for %s: %v", ErrUnsafeChange, path, err)
		}
		fields, exists, err := parseSinglePathRecord(output, path)
		if err != nil || !exists || len(fields) != 3 || fields[2] != "0" || !fullObjectID.MatchString(fields[1]) {
			return fmt.Errorf("%w: invalid staged entry for %s", ErrUnsafeChange, path)
		}
		if fields[0] != gitModeRegular && fields[0] != gitModeExecutable {
			return fmt.Errorf("%w: unsupported Git mode %s at %s", ErrUnsafeChange, fields[0], path)
		}
		return nil
	}
	commit := strings.TrimSuffix(objectPrefix, ":")
	file, exists, err := engine.treeFile(ctx, directory, commit, path)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: committed entry disappeared for %s", ErrUnsafeChange, path)
	}
	if file.Mode != gitModeRegular && file.Mode != gitModeExecutable {
		return fmt.Errorf("%w: unsupported Git mode %s at %s", ErrUnsafeChange, file.Mode, path)
	}
	return nil
}

func (engine *Engine) artifactFiles(ctx context.Context, task Task, commit string) ([]File, error) {
	changed, err := engine.git(ctx, task.Path, "diff", "--no-renames", "--name-only", "-z", task.BaseCommit+"..."+commit)
	if err != nil {
		return nil, err
	}
	var files []File
	for _, rawPath := range bytes.Split(changed, []byte{0}) {
		if len(rawPath) == 0 {
			continue
		}
		path := string(rawPath)
		file, exists, err := engine.treeFile(ctx, task.Path, commit, path)
		if err != nil {
			return nil, err
		}
		if !exists {
			file = File{Path: filepath.ToSlash(path), BlobSHA: "deleted", Mode: gitModeDeleted}
		}
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func (engine *Engine) treeFile(
	ctx context.Context,
	directory string,
	commit string,
	path string,
) (File, bool, error) {
	output, err := engine.git(ctx, directory, "ls-tree", "-z", "--full-name", commit, "--", path)
	if err != nil {
		return File{}, false, err
	}
	fields, exists, err := parseSinglePathRecord(output, path)
	if err != nil || !exists {
		return File{}, exists, err
	}
	if len(fields) != 3 || fields[1] != "blob" || !fullObjectID.MatchString(fields[2]) {
		return File{}, false, fmt.Errorf("%w: invalid tree entry for %s", ErrUnsafeChange, path)
	}
	if fields[0] != gitModeRegular && fields[0] != gitModeExecutable {
		return File{}, false, fmt.Errorf("%w: unsupported Git mode %s at %s", ErrUnsafeChange, fields[0], path)
	}
	return File{Path: filepath.ToSlash(path), BlobSHA: strings.ToLower(fields[2]), Mode: fields[0]}, true, nil
}

func parseSinglePathRecord(output []byte, path string) ([]string, bool, error) {
	if len(output) == 0 {
		return nil, false, nil
	}
	if output[len(output)-1] != 0 || bytes.IndexByte(output[:len(output)-1], 0) >= 0 {
		return nil, false, fmt.Errorf("path %s resolved to more than one Git entry", path)
	}
	record := output[:len(output)-1]
	tab := bytes.IndexByte(record, '\t')
	if tab < 0 || string(record[tab+1:]) != path {
		return nil, false, fmt.Errorf("path %s resolved ambiguously", path)
	}
	return strings.Fields(string(record[:tab])), true, nil
}

func (engine *Engine) resolveBase(ctx context.Context, baseCommit string) (string, error) {
	baseCommit = strings.TrimSpace(baseCommit)
	if !fullObjectID.MatchString(baseCommit) {
		return "", ErrInvalidBase
	}
	baseCommit = strings.ToLower(baseCommit)
	if engine.sharedRemote != "" {
		if _, err := engine.git(
			ctx,
			engine.repository,
			"fetch", "--no-tags", "--no-write-fetch-head", "--recurse-submodules=no", "--", engine.sharedRemote, baseCommit,
		); err != nil {
			return "", fmt.Errorf("fetch base %s from explicit shared remote %q: %w", baseCommit, engine.sharedRemote, err)
		}
	}
	resolved, err := engine.resolveCommit(ctx, engine.repository, baseCommit)
	if err != nil {
		if engine.sharedRemote == "" {
			return "", fmt.Errorf("%w: base is not local and shared remote is disabled: %v", ErrInvalidBase, err)
		}
		return "", err
	}
	if resolved != baseCommit {
		return "", fmt.Errorf("%w: resolved base %s differs from requested %s", ErrInvalidBase, resolved, baseCommit)
	}
	return resolved, nil
}

func (engine *Engine) runSetupHook(ctx context.Context, taskID, path, baseCommit string) error {
	if len(engine.setupHook) == 0 {
		return nil
	}
	command := exec.CommandContext(ctx, engine.setupHook[0], engine.setupHook[1:]...)
	command.Dir = path
	command.Env = append(
		os.Environ(),
		"HUMAN_TASK_ID="+taskID,
		"HUMAN_BASE_COMMIT="+baseCommit,
		"HUMAN_WORKTREE="+path,
	)
	var output cappedBuffer
	output.limit = 64 << 10
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Run(); err != nil {
		message := strings.TrimSpace(output.String())
		if message != "" {
			return fmt.Errorf("%w: %v: %s", ErrSetupHook, err, message)
		}
		return fmt.Errorf("%w: %v", ErrSetupHook, err)
	}
	return nil
}

func (engine *Engine) resolveCommit(ctx context.Context, directory, revision string) (string, error) {
	if strings.TrimSpace(revision) == "" {
		revision = "HEAD"
	}
	output, err := engine.git(ctx, directory, "rev-parse", "--verify", "--end-of-options", revision+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve commit %q: %w", revision, err)
	}
	resolved := strings.TrimSpace(string(output))
	if !fullObjectID.MatchString(resolved) || resolved != strings.ToLower(resolved) {
		return "", fmt.Errorf("%w: git returned an invalid object id", ErrRecovery)
	}
	return resolved, nil
}

func (engine *Engine) isAncestor(ctx context.Context, directory, ancestor, descendant string) (bool, error) {
	if !fullObjectID.MatchString(ancestor) || !fullObjectID.MatchString(descendant) {
		return false, ErrRecovery
	}
	_, err := engine.git(ctx, directory, "merge-base", "--is-ancestor", ancestor, descendant)
	if err == nil {
		return true, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) && exitError.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

type cappedBuffer struct {
	bytes.Buffer
	limit int
}

func (buffer *cappedBuffer) Write(payload []byte) (int, error) {
	consumed := len(payload)
	remaining := buffer.limit - buffer.Len()
	if remaining > 0 {
		if remaining < len(payload) {
			payload = payload[:remaining]
		}
		_, _ = buffer.Buffer.Write(payload)
	}
	return consumed, nil
}

func (engine *Engine) authorEnv(base []string) []string {
	env := append([]string(nil), base...)
	if engine.authorName != "" {
		env = append(env, "GIT_AUTHOR_NAME="+engine.authorName, "GIT_COMMITTER_NAME="+engine.authorName)
	}
	if engine.authorEmail != "" {
		env = append(env, "GIT_AUTHOR_EMAIL="+engine.authorEmail, "GIT_COMMITTER_EMAIL="+engine.authorEmail)
	}
	return env
}

func (engine *Engine) loadTaskForOperation(ctx context.Context, taskID string) (Task, *Artifact, error) {
	task, err := engine.loadTask(taskID)
	if err != nil {
		return Task{}, nil, err
	}
	if err := engine.verifyTaskBranch(ctx, task); err != nil {
		return Task{}, nil, err
	}
	if task.PendingRewind != nil {
		task, err = engine.recoverPendingRewind(ctx, task)
		if err != nil {
			return Task{}, nil, err
		}
	}
	if task.PendingTurn == 0 {
		return task, nil, nil
	}
	return engine.finishPendingTurn(ctx, task)
}

func (engine *Engine) verifyTaskBranch(ctx context.Context, task Task) error {
	branch, err := engine.git(ctx, task.Path, "symbolic-ref", "--quiet", "HEAD")
	if err != nil {
		return fmt.Errorf("%w: task worktree is detached or unavailable: %v", ErrRecovery, err)
	}
	expected := "refs/heads/" + task.Branch
	if strings.TrimSpace(string(branch)) != expected {
		return fmt.Errorf("%w: task worktree is on %q, want %q", ErrRecovery, strings.TrimSpace(string(branch)), expected)
	}
	return nil
}

func (engine *Engine) recoverPendingRewind(ctx context.Context, task Task) (Task, error) {
	receipt := task.PendingRewind
	if receipt == nil {
		return task, nil
	}
	if task.PendingTurn != 0 || receipt.TaskID != task.ID || receipt.FromTurn <= receipt.ToTurn ||
		receipt.FromTurn != task.LatestTurn || receipt.FromCommit != task.LatestCommit ||
		!fullObjectID.MatchString(receipt.FromCommit) || !fullObjectID.MatchString(receipt.ToCommit) ||
		!fullObjectID.MatchString(receipt.WIPCommit) ||
		receipt.BackupRef != fmt.Sprintf("refs/human/backup/%s/turn-%d", task.ID, receipt.FromTurn) {
		return Task{}, fmt.Errorf("%w: invalid pending rewind state", ErrRecovery)
	}
	head, err := engine.resolveCommit(ctx, task.Path, "HEAD")
	if err != nil {
		return Task{}, err
	}
	if head != receipt.FromCommit && head != receipt.ToCommit {
		return Task{}, fmt.Errorf("%w: pending rewind HEAD %s is neither source nor target", ErrRecovery, head)
	}
	backup, err := engine.resolveCommit(ctx, task.Path, receipt.BackupRef)
	if err != nil || backup != receipt.FromCommit {
		return Task{}, fmt.Errorf("%w: pending rewind backup ref is invalid", ErrRecovery)
	}
	wipRef := fmt.Sprintf("refs/human/wip/%s/%d", task.ID, task.NextTurn)
	wip, err := engine.resolveCommit(ctx, task.Path, wipRef)
	if err != nil || wip != receipt.WIPCommit {
		return Task{}, fmt.Errorf("%w: pending rewind WIP ref is invalid", ErrRecovery)
	}
	expectedTarget := task.BaseCommit
	if receipt.ToTurn > 0 {
		expectedTarget, err = engine.resolveCommit(
			ctx,
			task.Path,
			fmt.Sprintf("refs/human/keep/%s/turn-%d", task.ID, receipt.ToTurn),
		)
		if err != nil {
			return Task{}, fmt.Errorf("%w: pending rewind target ref is invalid", ErrRecovery)
		}
	}
	if expectedTarget != receipt.ToCommit {
		return Task{}, fmt.Errorf("%w: pending rewind target does not match pinned turn", ErrRecovery)
	}
	// The authority has not observed a result while the intent is still
	// pending. Restore the exact pre-rewind worktree and let its command retry.
	if _, err := engine.git(ctx, task.Path, "reset", "--hard", receipt.WIPCommit); err != nil {
		return Task{}, fmt.Errorf("recover pending rewind WIP: %w", err)
	}
	if _, err := engine.git(ctx, task.Path, "reset", "--mixed", receipt.FromCommit); err != nil {
		return Task{}, fmt.Errorf("recover pending rewind anchor: %w", err)
	}
	task.PendingRewind = nil
	if err := engine.saveTask(task); err != nil {
		return Task{}, fmt.Errorf("clear recovered rewind intent: %w", err)
	}
	return task, nil
}

func (engine *Engine) finishPendingTurn(ctx context.Context, task Task) (Task, *Artifact, error) {
	if task.PendingTurn < 1 || task.PendingTurn != task.NextTurn ||
		!fullObjectID.MatchString(task.PendingPreviousCommit) ||
		!fullObjectID.MatchString(task.PendingHead) {
		return Task{}, nil, fmt.Errorf("%w: invalid pending turn state", ErrRecovery)
	}
	expectedPrevious := task.BaseCommit
	if task.LatestTurn > 0 {
		expectedPrevious = task.LatestCommit
	}
	if task.PendingPreviousCommit != expectedPrevious {
		return Task{}, nil, fmt.Errorf(
			"%w: pending previous commit %s differs from persisted anchor %s",
			ErrRecovery,
			task.PendingPreviousCommit,
			expectedPrevious,
		)
	}
	ancestor, err := engine.isAncestor(ctx, task.Path, task.PendingPreviousCommit, task.PendingHead)
	if err != nil {
		return Task{}, nil, fmt.Errorf("verify pending turn ancestry: %w", err)
	}
	if !ancestor {
		return Task{}, nil, fmt.Errorf("%w: pending turn head is not descended from prior delivery", ErrRecovery)
	}
	head, err := engine.resolveCommit(ctx, task.Path, "HEAD")
	if err != nil {
		return Task{}, nil, err
	}
	if task.PendingAutoCommit && head == task.PendingHead {
		task.PendingTurn = 0
		task.PendingPreviousCommit = ""
		task.PendingHead = ""
		task.PendingAutoCommit = false
		if err := engine.saveTask(task); err != nil {
			return Task{}, nil, fmt.Errorf("clear uncommitted turn intent: %w", err)
		}
		return task, nil, nil
	}
	if task.PendingAutoCommit {
		parents, err := engine.git(ctx, task.Path, "rev-list", "--parents", "-n", "1", head)
		if err != nil {
			return Task{}, nil, err
		}
		parentFields := strings.Fields(string(parents))
		if len(parentFields) != 2 || parentFields[0] != head || parentFields[1] != task.PendingHead {
			return Task{}, nil, fmt.Errorf(
				"%w: automatic turn HEAD %s is not a single-parent child of %s",
				ErrRecovery,
				head,
				task.PendingHead,
			)
		}
		subject, err := engine.git(ctx, task.Path, "show", "-s", "--format=%s", head)
		if err != nil {
			return Task{}, nil, err
		}
		expectedSubject := fmt.Sprintf("human: task-%s turn-%d", task.ID, task.PendingTurn)
		if strings.TrimSpace(string(subject)) != expectedSubject {
			return Task{}, nil, fmt.Errorf("%w: pending turn commit subject does not match", ErrRecovery)
		}
	} else if head != task.PendingHead || head == task.PendingPreviousCommit {
		return Task{}, nil, fmt.Errorf("%w: human commit turn HEAD differs from persisted intent", ErrRecovery)
	}
	artifact, err := engine.buildArtifact(
		ctx,
		task,
		task.PendingTurn,
		head,
		task.PendingPreviousCommit,
	)
	if err != nil {
		return Task{}, nil, err
	}
	keepRef := fmt.Sprintf("refs/human/keep/%s/turn-%d", task.ID, task.PendingTurn)
	if _, err := engine.git(ctx, task.Path, "update-ref", keepRef, head); err != nil {
		return Task{}, nil, fmt.Errorf("pin delivered turn commit: %w", err)
	}
	previousRef := fmt.Sprintf("refs/human/keep/%s/turn-%d-previous", task.ID, task.PendingTurn)
	if _, err := engine.git(ctx, task.Path, "update-ref", previousRef, task.PendingPreviousCommit); err != nil {
		return Task{}, nil, fmt.Errorf("pin previous delivered turn commit: %w", err)
	}
	task.LatestTurn = task.PendingTurn
	task.NextTurn = task.PendingTurn + 1
	task.LatestCommit = head
	task.LatestPreviousCommit = task.PendingPreviousCommit
	task.PendingTurn = 0
	task.PendingPreviousCommit = ""
	task.PendingHead = ""
	task.PendingAutoCommit = false
	if err := engine.saveTask(task); err != nil {
		return Task{}, nil, fmt.Errorf("persist committed turn: %w", err)
	}
	return task, &artifact, nil
}

func (engine *Engine) resetIndexAfterFailure(ctx context.Context, task Task, cause error) error {
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if _, err := engine.git(recoveryCtx, task.Path, "reset", "--mixed", "HEAD"); err != nil {
		return errors.Join(cause, fmt.Errorf("restore index after failed artifact scan: %w", err))
	}
	return cause
}

func (engine *Engine) statePath(taskID string) string {
	return filepath.Join(engine.stateRoot, taskID+".json")
}

func (engine *Engine) loadTask(taskID string) (Task, error) {
	if !taskKey.MatchString(taskID) {
		return Task{}, ErrInvalidTaskID
	}
	payload, err := os.ReadFile(engine.statePath(taskID))
	if err != nil {
		return Task{}, err
	}
	var task Task
	if err := json.Unmarshal(payload, &task); err != nil {
		return Task{}, err
	}
	if task.ID != taskID || task.Path != filepath.Join(engine.worktreeRoot, taskID) || task.Branch != "human/"+taskID {
		return Task{}, errors.New("worktree task state failed namespace validation")
	}
	// State files created before monotonic turn allocation stored only the live
	// anchor. Rewind backup/keep refs retain the highest removed turn, so use
	// those refs to migrate without ever reusing an authority turn number.
	if task.NextTurn == 0 {
		next, err := engine.legacyNextTurn(task)
		if err != nil {
			return Task{}, err
		}
		task.NextTurn = next
	}
	if task.LatestTurn < 0 || task.NextTurn <= task.LatestTurn ||
		!fullObjectID.MatchString(task.BaseCommit) ||
		(task.LatestTurn > 0 && (!fullObjectID.MatchString(task.LatestCommit) || !fullObjectID.MatchString(task.LatestPreviousCommit))) ||
		(task.LatestTurn == 0 && task.LatestPreviousCommit != "") ||
		(task.PendingTurn == 0 && (task.PendingPreviousCommit != "" || task.PendingHead != "" || task.PendingAutoCommit)) ||
		(task.PendingTurn > 0 && (task.PendingTurn != task.NextTurn ||
			!fullObjectID.MatchString(task.PendingPreviousCommit) || !fullObjectID.MatchString(task.PendingHead))) ||
		(task.PendingTurn > 0 && task.PendingRewind != nil) ||
		strings.ContainsRune(task.SharedRemote, 0) || strings.HasPrefix(task.SharedRemote, "-") {
		return Task{}, errors.New("worktree task state has invalid turn/commit fields")
	}
	return task, nil
}

func (engine *Engine) legacyNextTurn(task Task) (int, error) {
	maximum := task.LatestTurn
	output, err := engine.git(
		context.Background(), task.Path, "for-each-ref", "--format=%(refname)",
		"refs/human/keep/"+task.ID+"/", "refs/human/backup/"+task.ID+"/",
	)
	if err != nil {
		return 0, fmt.Errorf("inspect legacy task turn refs: %w", err)
	}
	for _, ref := range strings.Fields(string(output)) {
		index := strings.LastIndex(ref, "/turn-")
		if index < 0 {
			continue
		}
		turn, err := strconv.Atoi(ref[index+len("/turn-"):])
		if err == nil && turn > maximum {
			maximum = turn
		}
	}
	return maximum + 1, nil
}

func (engine *Engine) saveTask(task Task) error {
	return saveJSONAtomic(engine.stateRoot, engine.statePath(task.ID), task)
}

func (engine *Engine) archivePath(taskID string) string {
	return filepath.Join(engine.archiveRoot, taskID+".json")
}

func (engine *Engine) saveArchivedTask(archived archivedTask) error {
	return saveJSONAtomic(engine.archiveRoot, engine.archivePath(archived.Task.ID), archived)
}

func (engine *Engine) loadArchivedTask(taskID string) (archivedTask, error) {
	if !taskKey.MatchString(taskID) {
		return archivedTask{}, ErrInvalidTaskID
	}
	payload, err := os.ReadFile(engine.archivePath(taskID))
	if err != nil {
		return archivedTask{}, err
	}
	var archived archivedTask
	if err := json.Unmarshal(payload, &archived); err != nil {
		return archivedTask{}, err
	}
	if archived.Task.ID != taskID || archived.Task.Branch != "human/"+taskID ||
		archived.Task.Path != filepath.Join(engine.worktreeRoot, taskID) ||
		archived.KeepRef != "refs/human/keep/"+taskID+"/completed" ||
		!fullObjectID.MatchString(archived.Task.BaseCommit) ||
		archived.Task.LatestTurn < 0 || archived.Task.NextTurn <= archived.Task.LatestTurn ||
		(archived.Task.LatestTurn > 0 && (!fullObjectID.MatchString(archived.Task.LatestCommit) ||
			!fullObjectID.MatchString(archived.Task.LatestPreviousCommit))) ||
		archived.Task.PendingTurn != 0 || archived.Task.PendingPreviousCommit != "" || archived.Task.PendingHead != "" ||
		archived.Task.PendingAutoCommit ||
		archived.Task.PendingRewind != nil {
		return archivedTask{}, errors.New("worktree archive failed namespace validation")
	}
	return archived, nil
}

func (engine *Engine) verifiedArchiveReceipt(ctx context.Context, archived archivedTask) (ArchiveReceipt, error) {
	receipt := engine.archiveReceipt(archived)
	commit, err := engine.resolveCommit(ctx, engine.repository, archived.KeepRef)
	if err != nil || commit != receipt.Commit {
		return ArchiveReceipt{}, fmt.Errorf("%w: completed archive keep ref is invalid", ErrRecovery)
	}
	branch, err := engine.resolveCommit(ctx, engine.repository, "refs/heads/"+archived.Task.Branch)
	if err != nil || branch != receipt.Commit {
		return ArchiveReceipt{}, fmt.Errorf("%w: completed archive branch is missing or divergent", ErrRecovery)
	}
	return receipt, nil
}

func (engine *Engine) archiveReceipt(archived archivedTask) ArchiveReceipt {
	commit := archived.Task.BaseCommit
	if archived.Task.LatestTurn > 0 {
		commit = archived.Task.LatestCommit
	}
	return ArchiveReceipt{
		TaskID: archived.Task.ID, Commit: commit, Branch: archived.Task.Branch,
		KeepRef: archived.KeepRef, StatePath: engine.archivePath(archived.Task.ID),
	}
}

func saveJSONAtomic(root, destination string, value any) error {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(root, ".record-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(append(payload, '\n')); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, destination); err != nil {
		return err
	}
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (engine *Engine) acquireProcessLock(ctx context.Context) (func(), error) {
	file, err := os.OpenFile(engine.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open worktree process lock: %w", err)
	}
	for {
		acquired, lockErr := lockFileNonblocking(file)
		if lockErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("acquire worktree process lock: %w", lockErr)
		}
		if acquired {
			var once sync.Once
			return func() {
				once.Do(func() {
					_ = unlockFile(file)
					_ = file.Close()
				})
			}, nil
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			_ = file.Close()
			return nil, fmt.Errorf("wait for worktree process lock: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func (engine *Engine) git(ctx context.Context, directory string, args ...string) ([]byte, error) {
	return engine.gitEnv(ctx, directory, nil, args...)
}

func (engine *Engine) gitEnv(ctx context.Context, directory string, env []string, args ...string) ([]byte, error) {
	// Every path passed by this package is a literal repository-relative name.
	// `--` alone does not disable pathspec magic such as :(glob), so make the
	// literal rule global and impossible for individual call sites to forget.
	command := exec.CommandContext(ctx, "git", append([]string{"--literal-pathspecs", "-C", directory}, args...)...)
	if env != nil {
		command.Env = env
	}
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err != nil {
		return stdout.Bytes(), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
