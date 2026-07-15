// Package patchapply applies replace-semantics delegation artifacts on the
// caller's machine. It never reports success until every delivered file hash
// matches the artifact oracle.
package patchapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrConflict          = errors.New("artifact could not be applied without conflict")
	ErrHashMismatch      = errors.New("applied artifact failed file hash verification")
	ErrModeMismatch      = errors.New("applied artifact failed Git mode verification")
	ErrArtifactReplay    = errors.New("artifact turn was reused with different content")
	ErrDirtyTouchedPaths = errors.New("caller has uncommitted changes on artifact paths")
	ErrArtifactPaths     = errors.New("artifact patch paths do not match file oracle metadata")
	ErrUnsafePath        = errors.New("artifact path crosses a symbolic link or non-directory")
	ErrTaskUnbound       = errors.New("delegation task is not bound to this caller workspace")
)

var taskKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$`)

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
	CumulativePatch  []byte `json:"-"`
	IncrementalPatch []byte `json:"-"`
	Files            []File `json:"files"`
}

type Config struct {
	Repository       string
	StateRoot        string
	DirtyCommit      bool
	DirtyCommitName  string
	DirtyCommitEmail string
	// MergirafPath overrides PATH discovery. Set it to "-" to disable the
	// optional structured-merge level even when mergiraf is installed.
	MergirafPath string
}

type Engine struct {
	mu                 sync.Mutex
	repository         string
	stateRoot          string
	stateLockPath      string
	repositoryLockPath string
	dirtyCommit        bool
	dirtyCommitName    string
	dirtyCommitEmail   string
	mergirafPath       string
	repositoryID       string
}

type Result struct {
	TaskID            string
	Turn              int
	Level             int
	Replay            bool
	Files             []File
	ResolvedConflicts []string
}

type Conflict struct {
	Path   string `json:"path"`
	Base   string `json:"base,omitempty"`
	Ours   string `json:"ours,omitempty"`
	Theirs string `json:"theirs,omitempty"`
	Reason string `json:"reason"`
}

type ApplyError struct {
	Cause     error
	Conflicts []Conflict
}

func (failure *ApplyError) Error() string {
	if len(failure.Conflicts) == 0 {
		return failure.Cause.Error()
	}
	return fmt.Sprintf("%v: %d path(s)", failure.Cause, len(failure.Conflicts))
}

func (failure *ApplyError) Unwrap() error { return failure.Cause }

type state struct {
	TaskID          string `json:"task_id"`
	RepositoryID    string `json:"repository_id"`
	BaseCommit      string `json:"base_commit"`
	Turn            int    `json:"turn"`
	Digest          string `json:"digest"`
	CumulativePatch []byte `json:"cumulative_patch"`
	Files           []File `json:"files"`
}

type pathSnapshot struct {
	path    string
	content []byte
	mode    fs.FileMode
	exists  bool
}

func Open(config Config) (*Engine, error) {
	if strings.TrimSpace(config.Repository) == "" {
		return nil, errors.New("caller repository is required")
	}
	requested, err := filepath.Abs(config.Repository)
	if err != nil {
		return nil, err
	}
	repositoryOutput, err := runGitAt(context.Background(), requested, nil, "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, fmt.Errorf("caller workspace is not a git repository: %w", err)
	}
	repository, err := filepath.EvalSymlinks(strings.TrimSpace(string(repositoryOutput)))
	if err != nil {
		return nil, fmt.Errorf("resolve caller repository root: %w", err)
	}
	commonOutput, err := runGitAt(context.Background(), repository, nil, "rev-parse", "--git-common-dir")
	if err != nil {
		return nil, fmt.Errorf("resolve caller git common directory: %w", err)
	}
	commonDirectory := strings.TrimSpace(string(commonOutput))
	if !filepath.IsAbs(commonDirectory) {
		commonDirectory = filepath.Join(repository, commonDirectory)
	}
	commonDirectory, err = filepath.EvalSymlinks(commonDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve caller git common directory: %w", err)
	}
	identityDigest := sha256.Sum256([]byte(commonDirectory + "\x00" + repository))
	lockDirectory := filepath.Join(commonDirectory, "human-agent", "locks")
	stateRoot := strings.TrimSpace(config.StateRoot)
	if stateRoot == "" {
		stateRoot = filepath.Join(commonDirectory, "human-agent", "apply")
	}
	stateRoot, err = filepath.Abs(stateRoot)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		return nil, err
	}
	stateRoot, err = filepath.EvalSymlinks(stateRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve apply state root: %w", err)
	}
	engine := &Engine{
		repository: repository, stateRoot: stateRoot, dirtyCommit: config.DirtyCommit,
		stateLockPath:      filepath.Join(stateRoot, ".ledger.lock"),
		repositoryLockPath: filepath.Join(lockDirectory, hex.EncodeToString(identityDigest[:])+".lock"),
		dirtyCommitName:    strings.TrimSpace(config.DirtyCommitName),
		dirtyCommitEmail:   strings.TrimSpace(config.DirtyCommitEmail),
		repositoryID:       hex.EncodeToString(identityDigest[:]),
	}
	configuredMergiraf := strings.TrimSpace(config.MergirafPath)
	switch configuredMergiraf {
	case "-":
	case "":
		engine.mergirafPath, _ = exec.LookPath("mergiraf")
	default:
		path, lookErr := exec.LookPath(configuredMergiraf)
		if lookErr != nil {
			return nil, fmt.Errorf("find configured mergiraf: %w", lookErr)
		}
		engine.mergirafPath = path
	}
	if err := os.MkdirAll(lockDirectory, 0o700); err != nil {
		return nil, err
	}
	return engine, nil
}

func runGitAt(ctx context.Context, repository string, stdin []byte, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", repository, "--literal-pathspecs"}, args...)...)
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// ValidateBase verifies that a shared-remote delegation base is a commit in
// this caller repository. Patch-only mode needs a different tree-fingerprint
// binding and is intentionally not inferred from a merely clean apply.
func (engine *Engine) ValidateBase(ctx context.Context, baseCommit string) error {
	_, err := engine.resolveBase(ctx, baseCommit)
	return err
}

// Bind durably associates a task with this exact caller worktree and base
// before any artifact can be applied. This prevents a caller-scoped A2A token
// from applying another workspace's clean-looking task by task ID alone.
func (engine *Engine) Bind(ctx context.Context, taskID, baseCommit string) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := acquireProcessLocks(ctx, engine.stateLockPath)
	if err != nil {
		return fmt.Errorf("lock caller worktree: %w", err)
	}
	defer release()
	if !taskKey.MatchString(strings.TrimSpace(taskID)) {
		return errors.New("valid task id is required")
	}
	resolvedBase, err := engine.resolveBase(ctx, baseCommit)
	if err != nil {
		return err
	}
	stored, exists, err := engine.loadState(taskID)
	if err != nil {
		return err
	}
	if exists {
		if stored.RepositoryID != engine.repositoryID || stored.BaseCommit != resolvedBase {
			return fmt.Errorf("%w: task binding differs from caller repository or base", ErrTaskUnbound)
		}
		return nil
	}
	return engine.saveState(state{
		TaskID: taskID, RepositoryID: engine.repositoryID, BaseCommit: resolvedBase,
		Files: []File{}, CumulativePatch: []byte{},
	})
}

func (engine *Engine) resolveBase(ctx context.Context, baseCommit string) (string, error) {
	baseCommit = strings.TrimSpace(baseCommit)
	if !validObjectID(baseCommit) {
		return "", errors.New("base commit must be a full lowercase Git object ID")
	}
	output, err := engine.git(ctx, nil, "rev-parse", "--verify", "--end-of-options", baseCommit+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("delegation base is not a commit in this caller repository: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

func (engine *Engine) Apply(ctx context.Context, artifact Artifact) (Result, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	release, err := acquireProcessLocks(ctx, engine.stateLockPath, engine.repositoryLockPath)
	if err != nil {
		return Result{}, fmt.Errorf("lock caller worktree: %w", err)
	}
	defer release()
	if err := validateArtifact(artifact); err != nil {
		return Result{}, err
	}
	digest := artifactDigest(artifact)
	previous, exists, err := engine.loadState(artifact.TaskID)
	if err != nil {
		return Result{}, err
	}
	if !exists || previous.RepositoryID != engine.repositoryID {
		return Result{}, ErrTaskUnbound
	}
	resolvedBase, err := engine.resolveBase(ctx, artifact.BaseCommit)
	if err != nil {
		return Result{}, err
	}
	if previous.BaseCommit != resolvedBase {
		return Result{}, fmt.Errorf("%w: artifact base differs from task binding", ErrTaskUnbound)
	}
	hasPrevious := previous.Turn > 0
	if artifact.Turn <= previous.Turn {
		if artifact.Turn == previous.Turn && digest == previous.Digest {
			root, err := os.OpenRoot(engine.repository)
			if err != nil {
				return Result{}, err
			}
			defer root.Close()
			if err := engine.verifyFiles(ctx, root, previous.Files); err != nil {
				return Result{}, err
			}
			return Result{TaskID: artifact.TaskID, Turn: artifact.Turn, Replay: true, Files: previous.Files}, nil
		}
		return Result{}, ErrArtifactReplay
	}
	if artifact.Turn != previous.Turn+1 {
		return Result{}, fmt.Errorf("artifact turn %d does not follow %d", artifact.Turn, previous.Turn)
	}

	paths, err := engine.validatedArtifactPaths(ctx, artifact, previous)
	if err != nil {
		return Result{}, err
	}
	expectedFiles, err := engine.expectedFiles(ctx, artifact.Files, previous.Files, resolvedBase)
	if err != nil {
		return Result{}, err
	}
	root, err := os.OpenRoot(engine.repository)
	if err != nil {
		return Result{}, err
	}
	defer root.Close()
	if err := ensureSafePaths(root, paths); err != nil {
		return Result{}, err
	}
	dirty, err := engine.dirtyIntersection(ctx, paths)
	if err != nil {
		return Result{}, err
	}
	previousCommitted := false
	staged, err := engine.stagedIntersection(ctx, paths)
	if err != nil {
		return Result{}, err
	}
	if len(staged) > 0 {
		if !engine.dirtyCommit {
			return Result{}, &ApplyError{
				Cause:     ErrDirtyTouchedPaths,
				Conflicts: pathReasons(staged, "caller path is staged; apply will not rewrite the index"),
			}
		}
		dirty = unionPaths(dirty, staged)
		if err := engine.commitDirty(ctx, artifact.TaskID, dirty); err != nil {
			return Result{}, fmt.Errorf("commit staged caller changes before apply: %w", err)
		}
		previousCommitted = hasPrevious
		dirty = nil
	}
	if hasPrevious {
		if verifyErr := engine.verifyFiles(ctx, root, previous.Files); verifyErr != nil {
			if !engine.dirtyCommit || len(dirty) == 0 {
				conflictPaths := dirty
				if len(conflictPaths) == 0 {
					conflictPaths = paths
				}
				return Result{}, &ApplyError{Cause: ErrDirtyTouchedPaths, Conflicts: pathReasons(conflictPaths, verifyErr.Error())}
			}
			if err := engine.commitDirty(ctx, artifact.TaskID, dirty); err != nil {
				return Result{}, fmt.Errorf("commit caller changes before apply: %w", err)
			}
			previousCommitted = true
			dirty = nil
		} else {
			managed := make(map[string]struct{}, len(previous.Files))
			for _, file := range previous.Files {
				managed[filepath.ToSlash(filepath.Clean(filepath.FromSlash(file.Path)))] = struct{}{}
			}
			var external []string
			managedDirty := false
			for _, path := range dirty {
				if _, ok := managed[path]; ok {
					managedDirty = true
					continue
				}
				external = append(external, path)
			}
			dirty = external
			previousCommitted = !managedDirty
		}
	}
	if len(dirty) > 0 {
		if !engine.dirtyCommit {
			return Result{}, &ApplyError{Cause: ErrDirtyTouchedPaths, Conflicts: pathReasons(dirty, "caller path has uncommitted changes")}
		}
		if err := engine.commitDirty(ctx, artifact.TaskID, dirty); err != nil {
			return Result{}, fmt.Errorf("commit caller changes before apply: %w", err)
		}
	}
	snapshots, err := engine.snapshotPaths(root, paths)
	if err != nil {
		return Result{}, err
	}

	level := -1
	var resolvedConflicts []string
	var applyErrors []string
	var applyCauses []error
	if hasPrevious && !previousCommitted && len(previous.CumulativePatch) > 0 {
		if err := engine.applyPatch(ctx, previous.CumulativePatch, true, false); err == nil {
			if len(artifact.CumulativePatch) == 0 {
				level = 1
			} else if resolved, err := engine.applyPatchWithStructuredMerge(ctx, artifact, artifact.CumulativePatch); err == nil {
				if len(resolved) > 0 {
					level, resolvedConflicts = 3, resolved
				} else {
					level = 1
				}
			} else {
				applyErrors = append(applyErrors, "replace cumulative: "+err.Error())
				applyCauses = append(applyCauses, err)
				if restoreErr := engine.restorePaths(ctx, root, snapshots); restoreErr != nil {
					return Result{}, errors.Join(err, fmt.Errorf("rollback failed: %w", restoreErr))
				}
			}
		} else {
			applyErrors = append(applyErrors, "revert previous cumulative: "+err.Error())
			applyCauses = append(applyCauses, err)
		}
	}
	if level < 0 && hasPrevious && len(artifact.IncrementalPatch) > 0 {
		if resolved, err := engine.applyPatchWithStructuredMerge(ctx, artifact, artifact.IncrementalPatch); err == nil {
			if len(resolved) > 0 {
				level, resolvedConflicts = 3, resolved
			} else {
				level = 2
			}
		} else {
			applyErrors = append(applyErrors, "apply incremental: "+err.Error())
			applyCauses = append(applyCauses, err)
			if restoreErr := engine.restorePaths(ctx, root, snapshots); restoreErr != nil {
				return Result{}, errors.Join(err, fmt.Errorf("rollback failed: %w", restoreErr))
			}
		}
	}
	if level < 0 && !hasPrevious {
		if len(artifact.CumulativePatch) == 0 {
			level = 1
		} else if resolved, err := engine.applyPatchWithStructuredMerge(ctx, artifact, artifact.CumulativePatch); err == nil {
			if len(resolved) > 0 {
				level, resolvedConflicts = 3, resolved
			} else {
				level = 1
			}
		} else {
			applyErrors = append(applyErrors, "apply initial cumulative: "+err.Error())
			applyCauses = append(applyCauses, err)
			if restoreErr := engine.restorePaths(ctx, root, snapshots); restoreErr != nil {
				return Result{}, errors.Join(err, fmt.Errorf("rollback failed: %w", restoreErr))
			}
		}
	}
	if level < 0 {
		conflicts := engine.conflictDetails(ctx, root, artifact, paths, strings.Join(applyErrors, "; "))
		causes := append([]error{ErrConflict}, applyCauses...)
		return Result{}, &ApplyError{Cause: errors.Join(causes...), Conflicts: conflicts}
	}
	if err := engine.unstagePaths(ctx, paths); err != nil {
		if restoreErr := engine.restorePaths(ctx, root, snapshots); restoreErr != nil {
			return Result{}, fmt.Errorf("unstage applied artifact: %w; rollback failed: %v", err, restoreErr)
		}
		return Result{}, fmt.Errorf("unstage applied artifact: %w", err)
	}
	if err := engine.verifyFiles(ctx, root, expectedFiles); err != nil {
		if restoreErr := engine.restorePaths(ctx, root, snapshots); restoreErr != nil {
			return Result{}, fmt.Errorf("%w; rollback failed: %v", err, restoreErr)
		}
		return Result{}, err
	}
	next := state{
		TaskID: artifact.TaskID, RepositoryID: engine.repositoryID, BaseCommit: resolvedBase,
		Turn: artifact.Turn, Digest: digest,
		CumulativePatch: bytes.Clone(artifact.CumulativePatch), Files: expectedFiles,
	}
	if err := engine.saveState(next); err != nil {
		if restoreErr := engine.restorePaths(ctx, root, snapshots); restoreErr != nil {
			return Result{}, fmt.Errorf("persist apply state: %w; rollback failed: %v", err, restoreErr)
		}
		return Result{}, fmt.Errorf("persist apply state: %w", err)
	}
	return Result{
		TaskID: artifact.TaskID, Turn: artifact.Turn, Level: level, Files: expectedFiles,
		ResolvedConflicts: resolvedConflicts,
	}, nil
}

func (engine *Engine) unstagePaths(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	args := append([]string{"reset", "--quiet", "--"}, paths...)
	_, err := engine.git(ctx, nil, args...)
	return err
}

func validateArtifact(artifact Artifact) error {
	if !taskKey.MatchString(artifact.TaskID) || artifact.Turn < 1 ||
		!validObjectID(artifact.BaseCommit) || !validObjectID(artifact.Commit) {
		return errors.New("task, positive turn, and commits are required")
	}
	seen := make(map[string]struct{}, len(artifact.Files))
	for _, file := range artifact.Files {
		canonical, err := canonicalArtifactPath(file.Path)
		if err != nil || !validFileOracle(file) {
			return fmt.Errorf("invalid artifact file metadata for %q", file.Path)
		}
		if _, duplicate := seen[canonical]; duplicate {
			return fmt.Errorf("duplicate artifact file %q", canonical)
		}
		seen[canonical] = struct{}{}
	}
	return nil
}

func validFileOracle(file File) bool {
	if file.BlobSHA == "deleted" || file.Mode == gitModeDeleted {
		return file.BlobSHA == "deleted" && file.Mode == gitModeDeleted
	}
	return validObjectID(file.BlobSHA) && (file.Mode == gitModeRegular || file.Mode == gitModeExecutable)
}

func validObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			if digit < 'a' || digit > 'f' {
				return false
			}
		}
	}
	return true
}

func artifactDigest(artifact Artifact) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(artifact.TaskID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(fmt.Sprintf("%d\x00%s\x00%s\x00", artifact.Turn, artifact.BaseCommit, artifact.Commit)))
	_, _ = hash.Write(artifact.CumulativePatch)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(artifact.IncrementalPatch)
	for _, file := range artifact.Files {
		_, _ = hash.Write([]byte("\x00" + file.Path + "\x00" + file.BlobSHA + "\x00" + file.Mode))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func (engine *Engine) validatedArtifactPaths(
	ctx context.Context,
	artifact Artifact,
	previous state,
) ([]string, error) {
	declared := filePaths(artifact.Files)
	cumulative, err := engine.patchPaths(ctx, artifact.CumulativePatch)
	if err != nil {
		return nil, fmt.Errorf("inspect cumulative artifact paths: %w", err)
	}
	if !slices.Equal(declared, cumulative) {
		return nil, fmt.Errorf(
			"%w: declared=%v cumulative=%v", ErrArtifactPaths, declared, cumulative,
		)
	}
	paths := unionPaths(declared, filePaths(previous.Files))
	if len(artifact.IncrementalPatch) > 0 {
		incremental, err := engine.patchPaths(ctx, artifact.IncrementalPatch)
		if err != nil {
			return nil, fmt.Errorf("inspect incremental artifact paths: %w", err)
		}
		for _, path := range incremental {
			if _, found := slices.BinarySearch(paths, path); !found {
				return nil, fmt.Errorf("%w: incremental path %q is outside cumulative oracle", ErrArtifactPaths, path)
			}
		}
	}
	return paths, nil
}

// expectedFiles expands the current cumulative oracle with every previously
// managed path that returned to the bound base. Incremental fallback may touch
// those paths even though they disappeared from base...HEAD and current Files.
func (engine *Engine) expectedFiles(
	ctx context.Context,
	current []File,
	previous []File,
	baseCommit string,
) ([]File, error) {
	byPath := make(map[string]File, len(current)+len(previous))
	for _, file := range current {
		canonical, err := canonicalArtifactPath(file.Path)
		if err != nil {
			return nil, err
		}
		file.Path = canonical
		byPath[canonical] = file
	}
	for _, file := range previous {
		canonical, err := canonicalArtifactPath(file.Path)
		if err != nil {
			return nil, err
		}
		if _, currentPath := byPath[canonical]; currentPath {
			continue
		}
		baseFile, err := engine.baseFileOracle(ctx, baseCommit, canonical)
		if err != nil {
			return nil, err
		}
		byPath[canonical] = baseFile
	}
	files := make([]File, 0, len(byPath))
	for _, file := range byPath {
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func (engine *Engine) baseFileOracle(ctx context.Context, baseCommit, path string) (File, error) {
	output, err := engine.git(ctx, nil, "ls-tree", "-z", "--full-name", baseCommit, "--", path)
	if err != nil {
		return File{}, fmt.Errorf("inspect bound base path %s: %w", path, err)
	}
	if len(output) == 0 {
		return File{Path: path, BlobSHA: "deleted", Mode: gitModeDeleted}, nil
	}
	if output[len(output)-1] != 0 || bytes.IndexByte(output[:len(output)-1], 0) >= 0 {
		return File{}, fmt.Errorf("bound base path %s did not resolve to one entry", path)
	}
	entry := output[:len(output)-1]
	tab := bytes.IndexByte(entry, '\t')
	if tab < 0 || string(entry[tab+1:]) != path {
		return File{}, fmt.Errorf("bound base path %s resolved ambiguously", path)
	}
	fields := strings.Fields(string(entry[:tab]))
	if len(fields) != 3 || (fields[0] != "100644" && fields[0] != "100755") ||
		fields[1] != "blob" || !validObjectID(fields[2]) {
		return File{}, fmt.Errorf("bound base path %s is not a regular blob", path)
	}
	return File{Path: path, BlobSHA: fields[2], Mode: fields[0]}, nil
}

func filePaths(files []File) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, filepath.ToSlash(filepath.Clean(filepath.FromSlash(file.Path))))
	}
	sort.Strings(paths)
	return paths
}

func unionPaths(groups ...[]string) []string {
	set := make(map[string]struct{})
	for _, group := range groups {
		for _, path := range group {
			set[path] = struct{}{}
		}
	}
	paths := make([]string, 0, len(set))
	for path := range set {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// patchPaths delegates patch parsing to git in both directions. git apply's
// forward numstat can report only the destination of a cross-path patch while
// reverse numstat reports its source; their union is the complete mutation
// boundary for dirty checks, snapshot, rollback, and file oracles.
func (engine *Engine) patchPaths(ctx context.Context, patch []byte) ([]string, error) {
	if len(patch) == 0 {
		return []string{}, nil
	}
	forward, err := engine.numstatPaths(ctx, patch, false)
	if err != nil {
		return nil, err
	}
	reverse, err := engine.numstatPaths(ctx, patch, true)
	if err != nil {
		return nil, err
	}
	return unionPaths(forward, reverse), nil
}

func (engine *Engine) numstatPaths(ctx context.Context, patch []byte, reverse bool) ([]string, error) {
	args := []string{"apply"}
	if reverse {
		args = append(args, "--reverse")
	}
	args = append(args, "--numstat", "-z")
	output, err := runGitAt(ctx, engine.repository, patch, args...)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{})
	for offset := 0; offset < len(output); {
		for offset < len(output) && output[offset] == '\n' {
			offset++
		}
		if offset == len(output) {
			break
		}
		firstTab := bytes.IndexByte(output[offset:], '\t')
		if firstTab < 1 {
			return nil, errors.New("malformed git apply numstat added count")
		}
		firstTab += offset
		secondTab := bytes.IndexByte(output[firstTab+1:], '\t')
		if secondTab < 1 {
			return nil, errors.New("malformed git apply numstat deleted count")
		}
		secondTab += firstTab + 1
		if !validNumstatCount(output[offset:firstTab]) || !validNumstatCount(output[firstTab+1:secondTab]) {
			return nil, errors.New("malformed git apply numstat counts")
		}
		offset = secondTab + 1
		if offset >= len(output) {
			return nil, errors.New("malformed git apply numstat path")
		}
		if output[offset] == 0 {
			offset++
			for range 2 {
				path, next, err := nulField(output, offset)
				if err != nil {
					return nil, err
				}
				canonical, err := canonicalArtifactPath(path)
				if err != nil {
					return nil, err
				}
				set[canonical] = struct{}{}
				offset = next
			}
			continue
		}
		path, next, err := nulField(output, offset)
		if err != nil {
			return nil, err
		}
		canonical, err := canonicalArtifactPath(path)
		if err != nil {
			return nil, err
		}
		set[canonical] = struct{}{}
		offset = next
	}
	paths := make([]string, 0, len(set))
	for path := range set {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

func validNumstatCount(value []byte) bool {
	if bytes.Equal(value, []byte("-")) {
		return true
	}
	if len(value) == 0 {
		return false
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func nulField(data []byte, offset int) (string, int, error) {
	end := bytes.IndexByte(data[offset:], 0)
	if end < 0 {
		return "", 0, errors.New("malformed git apply numstat NUL field")
	}
	end += offset
	if end == offset {
		return "", 0, errors.New("git apply numstat contains an empty path")
	}
	return string(data[offset:end]), end + 1, nil
}

func canonicalArtifactPath(path string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(path))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: invalid patch path %q", ErrArtifactPaths, path)
	}
	canonical := filepath.ToSlash(clean)
	lower := strings.ToLower(canonical)
	if lower == ".git" || strings.HasPrefix(lower, ".git/") || strings.Contains(lower, "/.git/") {
		return "", fmt.Errorf("%w: git administrative path %q", ErrArtifactPaths, path)
	}
	return canonical, nil
}

func (engine *Engine) dirtyIntersection(ctx context.Context, paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	output, err := engine.git(ctx, nil, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return nil, err
	}
	touched := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		touched[path] = struct{}{}
	}
	set := make(map[string]struct{})
	entries := bytes.Split(output, []byte{0})
	for index := 0; index < len(entries); index++ {
		entry := entries[index]
		if len(entry) < 4 {
			continue
		}
		group := []string{string(entry[3:])}
		if (strings.Contains(string(entry[:2]), "R") || strings.Contains(string(entry[:2]), "C")) &&
			index+1 < len(entries) && len(entries[index+1]) > 0 {
			index++
			group = append(group, string(entries[index]))
		}
		canonical := make([]string, 0, len(group))
		intersects := false
		for _, path := range group {
			path, err := canonicalArtifactPath(path)
			if err != nil {
				return nil, err
			}
			canonical = append(canonical, path)
			if _, ok := touched[path]; ok {
				intersects = true
			}
		}
		if !intersects {
			continue
		}
		for _, path := range canonical {
			set[path] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for path := range set {
		result = append(result, path)
	}
	sort.Strings(result)
	return result, nil
}

func (engine *Engine) stagedIntersection(ctx context.Context, paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	output, err := engine.git(ctx, nil, "diff", "--cached", "--no-renames", "--name-only", "-z")
	if err != nil {
		return nil, err
	}
	touched := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		touched[path] = struct{}{}
	}
	var result []string
	for _, raw := range bytes.Split(output, []byte{0}) {
		if len(raw) == 0 {
			continue
		}
		path, err := canonicalArtifactPath(string(raw))
		if err != nil {
			return nil, err
		}
		if _, ok := touched[path]; ok {
			result = append(result, path)
		}
	}
	sort.Strings(result)
	return result, nil
}

func (engine *Engine) commitDirty(ctx context.Context, taskID string, paths []string) error {
	if len(paths) == 0 {
		return errors.New("dirty commit requires at least one path")
	}
	temporary, err := os.CreateTemp(engine.stateRoot, ".dirty-index-*")
	if err != nil {
		return err
	}
	indexPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Remove(indexPath); err != nil {
		return err
	}
	defer os.Remove(indexPath)
	environment := append(os.Environ(), "GIT_INDEX_FILE="+indexPath)
	if _, err := engine.git(ctx, environment, "read-tree", "HEAD"); err != nil {
		return fmt.Errorf("seed dirty commit index: %w", err)
	}
	addArgs := append([]string{"add", "-A", "--"}, paths...)
	if _, err := engine.git(ctx, environment, addArgs...); err != nil {
		return fmt.Errorf("stage dirty commit snapshot: %w", err)
	}
	treeOutput, err := engine.git(ctx, environment, "write-tree")
	if err != nil {
		return fmt.Errorf("write dirty commit tree: %w", err)
	}
	parentOutput, err := engine.git(ctx, nil, "rev-parse", "--verify", "--end-of-options", "HEAD^{commit}")
	if err != nil {
		return err
	}
	parent := strings.TrimSpace(string(parentOutput))
	commitEnvironment := append([]string(nil), environment...)
	if engine.dirtyCommitName != "" {
		commitEnvironment = append(commitEnvironment,
			"GIT_AUTHOR_NAME="+engine.dirtyCommitName, "GIT_COMMITTER_NAME="+engine.dirtyCommitName)
	}
	if engine.dirtyCommitEmail != "" {
		commitEnvironment = append(commitEnvironment,
			"GIT_AUTHOR_EMAIL="+engine.dirtyCommitEmail, "GIT_COMMITTER_EMAIL="+engine.dirtyCommitEmail)
	}
	message := "human: preserve caller changes before task-" + taskID
	commitOutput, err := engine.git(ctx, commitEnvironment,
		"commit-tree", strings.TrimSpace(string(treeOutput)), "-p", parent, "-m", message)
	if err != nil {
		return fmt.Errorf("create dirty preservation commit: %w", err)
	}
	commit := strings.TrimSpace(string(commitOutput))
	if !validObjectID(commit) {
		return errors.New("dirty preservation commit returned an invalid object id")
	}
	if _, err := engine.git(ctx, nil, "update-ref", "HEAD", commit, parent); err != nil {
		return fmt.Errorf("advance caller HEAD to dirty preservation commit: %w", err)
	}
	resetArgs := append([]string{"reset", "--quiet", "--"}, paths...)
	if _, err := engine.git(ctx, nil, resetArgs...); err != nil {
		_, rollbackErr := engine.git(context.WithoutCancel(ctx), nil, "update-ref", "HEAD", parent, commit)
		if rollbackErr != nil {
			return errors.Join(err, fmt.Errorf("rollback dirty preservation HEAD: %w", rollbackErr))
		}
		return fmt.Errorf("align caller index after dirty preservation: %w", err)
	}
	return nil
}

func ensureSafePaths(root *os.Root, paths []string) error {
	for _, path := range paths {
		parts := strings.Split(filepath.ToSlash(path), "/")
		for index := range parts {
			prefix := filepath.FromSlash(strings.Join(parts[:index+1], "/"))
			info, err := root.Lstat(prefix)
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			if err != nil {
				return fmt.Errorf("inspect artifact path %s: %w", path, err)
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("%w: %s contains symlink component %s", ErrUnsafePath, path, filepath.ToSlash(prefix))
			}
			if index < len(parts)-1 && !info.IsDir() {
				return fmt.Errorf("%w: %s has non-directory component %s", ErrUnsafePath, path, filepath.ToSlash(prefix))
			}
			if index == len(parts)-1 && !info.Mode().IsRegular() {
				return fmt.Errorf("%w: %s is not a regular file", ErrUnsafePath, path)
			}
		}
	}
	return nil
}

func (engine *Engine) snapshotPaths(root *os.Root, paths []string) ([]pathSnapshot, error) {
	snapshots := make([]pathSnapshot, 0, len(paths))
	for _, relative := range paths {
		name := filepath.FromSlash(relative)
		info, err := root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			snapshots = append(snapshots, pathSnapshot{path: relative})
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("artifact path %s is not a regular file", relative)
		}
		content, err := root.ReadFile(name)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, pathSnapshot{path: relative, content: content, mode: info.Mode(), exists: true})
	}
	return snapshots, nil
}

func (engine *Engine) restorePaths(ctx context.Context, root *os.Root, snapshots []pathSnapshot) error {
	rollbackContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	paths := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		paths = append(paths, snapshot.path)
	}
	if len(paths) > 0 {
		args := append([]string{"reset", "--quiet", "--"}, paths...)
		if _, err := engine.git(rollbackContext, nil, args...); err != nil {
			return err
		}
	}
	// Remove exact patched leaves shallow-first. This safely removes a symlink
	// created by a failed patch before considering any descendant path.
	ordered := append([]pathSnapshot(nil), snapshots...)
	sort.Slice(ordered, func(i, j int) bool {
		leftDepth := strings.Count(filepath.ToSlash(ordered[i].path), "/")
		rightDepth := strings.Count(filepath.ToSlash(ordered[j].path), "/")
		if leftDepth == rightDepth {
			return ordered[i].path < ordered[j].path
		}
		return leftDepth < rightDepth
	})
	for _, snapshot := range ordered {
		name := filepath.FromSlash(snapshot.path)
		info, err := root.Lstat(name)
		if err == nil {
			if info.IsDir() {
				return fmt.Errorf("rollback target %s unexpectedly became a directory", snapshot.path)
			}
			if err := root.Remove(name); err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for _, snapshot := range snapshots {
		if !snapshot.exists {
			continue
		}
		name := filepath.FromSlash(snapshot.path)
		if err := root.MkdirAll(filepath.Dir(name), 0o700); err != nil {
			return err
		}
		if err := root.WriteFile(name, snapshot.content, snapshot.mode.Perm()); err != nil {
			return err
		}
	}
	return nil
}

func (engine *Engine) applyPatch(ctx context.Context, patch []byte, reverse, threeWay bool) error {
	args := []string{"apply", "--whitespace=nowarn"}
	if reverse {
		args = append(args, "--reverse")
	}
	if threeWay {
		checkArgs := append(append([]string(nil), args...), "--check")
		if err := engine.runGitApply(ctx, patch, checkArgs); err == nil {
			return engine.runGitApply(ctx, patch, args)
		}
		args = append(args, "--3way")
	}
	return engine.runGitApply(ctx, patch, args)
}

// applyPatchWithStructuredMerge first uses git's exact/three-way machinery.
// A failed three-way apply intentionally leaves unmerged index stages behind;
// when mergiraf is available those immutable base/ours/theirs blobs are the
// inputs to the optional syntax-aware level. The caller-visible worktree is
// still verified against the artifact blob oracle before success is reported.
func (engine *Engine) applyPatchWithStructuredMerge(
	ctx context.Context,
	artifact Artifact,
	patch []byte,
) ([]string, error) {
	applyErr := engine.applyPatch(ctx, patch, false, true)
	if applyErr == nil {
		return nil, nil
	}
	if engine.mergirafPath == "" {
		return nil, applyErr
	}
	resolved, mergeErr := engine.resolveUnmergedWithMergiraf(ctx, artifact)
	if mergeErr != nil {
		return nil, fmt.Errorf("%w; structured merge: %w", applyErr, mergeErr)
	}
	if len(resolved) == 0 {
		return nil, applyErr
	}
	return resolved, nil
}

func (engine *Engine) resolveUnmergedWithMergiraf(
	ctx context.Context,
	artifact Artifact,
) ([]string, error) {
	output, err := engine.git(ctx, nil, "diff", "--name-only", "--diff-filter=U", "-z")
	if err != nil {
		return nil, err
	}
	entries := bytes.Split(output, []byte{0})
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if len(entry) == 0 {
			continue
		}
		path := filepath.ToSlash(filepath.Clean(filepath.FromSlash(string(entry))))
		if path == "." || path == ".." || strings.HasPrefix(path, "../") || filepath.IsAbs(path) {
			return nil, fmt.Errorf("invalid unmerged path %q", entry)
		}
		paths = append(paths, path)
	}
	if len(paths) == 0 {
		return nil, errors.New("git did not leave three-way conflict stages")
	}
	sort.Strings(paths)
	for _, path := range paths {
		if err := engine.resolvePathWithMergiraf(ctx, artifact, path); err != nil {
			return nil, err
		}
	}
	return paths, nil
}

func (engine *Engine) resolvePathWithMergiraf(
	ctx context.Context,
	artifact Artifact,
	path string,
) error {
	root, err := os.OpenRoot(engine.repository)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := ensureSafePaths(root, []string{path}); err != nil {
		return err
	}
	base, baseErr := engine.git(ctx, nil, "show", ":1:"+path)
	ours, oursErr := engine.git(ctx, nil, "show", ":2:"+path)
	theirs, theirsErr := engine.git(ctx, nil, "show", ":3:"+path)
	if baseErr != nil || oursErr != nil || theirsErr != nil {
		return fmt.Errorf("%s lacks regular stage 1/2/3 blobs", path)
	}
	if bytes.IndexByte(base, 0) >= 0 || bytes.IndexByte(ours, 0) >= 0 || bytes.IndexByte(theirs, 0) >= 0 {
		return fmt.Errorf("%s is binary", path)
	}
	temporaryRoot, err := os.MkdirTemp(engine.stateRoot, ".mergiraf-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporaryRoot)
	basePath := filepath.Join(temporaryRoot, "base")
	oursPath := filepath.Join(temporaryRoot, "ours")
	theirsPath := filepath.Join(temporaryRoot, "theirs")
	for file, content := range map[string][]byte{basePath: base, oursPath: ours, theirsPath: theirs} {
		if err := os.WriteFile(file, content, 0o600); err != nil {
			return err
		}
	}
	command := exec.CommandContext(
		ctx, engine.mergirafPath, "merge", "--git", "--path-name", path,
		basePath, oursPath, theirsPath,
	)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("mergiraf merge %s: %w: %s", path, err, strings.TrimSpace(stderr.String()))
	}
	merged, err := os.ReadFile(oursPath)
	if err != nil {
		return err
	}
	conflicted, err := root.ReadFile(filepath.FromSlash(path))
	if err != nil {
		return fmt.Errorf("read git conflict for %s: %w", path, err)
	}
	if _, err := engine.saveConflictAudit(artifact, path, ".conflict", conflicted); err != nil {
		return fmt.Errorf("save conflict audit for %s: %w", path, err)
	}
	candidatePath, err := engine.saveConflictAudit(artifact, path, ".merged", merged)
	if err != nil {
		return fmt.Errorf("save structured merge candidate for %s: %w", path, err)
	}
	expected, ok := artifactFile(artifact.Files, path)
	if !ok || expected.BlobSHA == "deleted" {
		return fmt.Errorf("%w: structured merge candidate %s has no live file oracle", ErrHashMismatch, candidatePath)
	}
	actual, err := engine.hashBytes(ctx, merged)
	if err != nil {
		return err
	}
	if actual != expected.BlobSHA {
		return fmt.Errorf(
			"%w: structured merge candidate for %s got %s want %s; review %s",
			ErrHashMismatch, path, actual, expected.BlobSHA, candidatePath,
		)
	}
	name := filepath.FromSlash(path)
	info, err := root.Lstat(name)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: structured merge target %s is not regular", ErrUnsafePath, path)
	}
	if err := root.WriteFile(name, merged, info.Mode().Perm()); err != nil {
		return err
	}
	if _, err := engine.git(ctx, nil, "add", "--", path); err != nil {
		return fmt.Errorf("stage structured merge %s: %w", path, err)
	}
	return nil
}

func (engine *Engine) saveConflictAudit(
	artifact Artifact,
	path string,
	suffix string,
	content []byte,
) (string, error) {
	directory := filepath.Join(
		engine.stateRoot, "conflicts", artifact.TaskID, fmt.Sprintf("turn-%d", artifact.Turn),
		filepath.Dir(filepath.FromSlash(path)),
	)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	target := filepath.Join(directory, filepath.Base(filepath.FromSlash(path))+suffix)
	temporary, err := os.CreateTemp(directory, ".conflict-*")
	if err != nil {
		return "", err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return "", err
	}
	if _, err := temporary.Write(content); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(name, target); err != nil {
		return "", err
	}
	return target, nil
}

func artifactFile(files []File, path string) (File, bool) {
	for _, file := range files {
		if filepath.ToSlash(filepath.Clean(filepath.FromSlash(file.Path))) == path {
			return file, true
		}
	}
	return File{}, false
}

func (engine *Engine) hashBytes(ctx context.Context, content []byte) (string, error) {
	command := exec.CommandContext(ctx, "git", "-C", engine.repository, "--literal-pathspecs", "hash-object", "--stdin")
	command.Stdin = bytes.NewReader(content)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("git hash-object --stdin: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

func (engine *Engine) runGitApply(ctx context.Context, patch []byte, args []string) error {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", engine.repository, "--literal-pathspecs"}, args...)...)
	command.Stdin = bytes.NewReader(patch)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func (engine *Engine) verifyFiles(ctx context.Context, root *os.Root, files []File) error {
	if err := ensureSafePaths(root, filePaths(files)); err != nil {
		return err
	}
	for _, file := range files {
		name := filepath.FromSlash(file.Path)
		if file.BlobSHA == "deleted" {
			if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: %s should be deleted", ErrHashMismatch, file.Path)
			}
			continue
		}
		info, err := root.Lstat(name)
		if err != nil {
			return fmt.Errorf("%w: inspect %s: %v", ErrModeMismatch, file.Path, err)
		}
		actualMode := gitModeRegular
		if info.Mode().Perm()&0o111 != 0 {
			actualMode = gitModeExecutable
		}
		if actualMode != file.Mode {
			return fmt.Errorf("%w: %s got %s want %s", ErrModeMismatch, file.Path, actualMode, file.Mode)
		}
		content, err := root.ReadFile(name)
		if err != nil {
			return fmt.Errorf("%w: read %s: %v", ErrHashMismatch, file.Path, err)
		}
		actual, err := engine.hashBytes(ctx, content)
		if err != nil {
			return fmt.Errorf("%w: hash %s: %v", ErrHashMismatch, file.Path, err)
		}
		if actual != file.BlobSHA {
			return fmt.Errorf("%w: %s got %s want %s", ErrHashMismatch, file.Path, actual, file.BlobSHA)
		}
	}
	return nil
}

func (engine *Engine) conflictDetails(
	ctx context.Context,
	root *os.Root,
	artifact Artifact,
	paths []string,
	reason string,
) []Conflict {
	conflicts := make([]Conflict, 0, len(paths))
	for _, path := range paths {
		ours, _ := root.ReadFile(filepath.FromSlash(path))
		base, _ := engine.git(ctx, nil, "show", artifact.BaseCommit+":"+path)
		theirs, _ := engine.git(ctx, nil, "show", artifact.Commit+":"+path)
		conflicts = append(conflicts, Conflict{
			Path: path, Base: printable(base), Ours: printable(ours), Theirs: printable(theirs), Reason: reason,
		})
	}
	return conflicts
}

func printable(content []byte) string {
	if bytes.IndexByte(content, 0) >= 0 {
		return "<binary>"
	}
	return string(content)
}

func pathReasons(paths []string, reason string) []Conflict {
	result := make([]Conflict, 0, len(paths))
	for _, path := range paths {
		result = append(result, Conflict{Path: path, Reason: reason})
	}
	return result
}

func (engine *Engine) statePath(taskID string) string {
	return filepath.Join(engine.stateRoot, taskID+".json")
}

func (engine *Engine) loadState(taskID string) (state, bool, error) {
	payload, err := os.ReadFile(engine.statePath(taskID))
	if errors.Is(err, os.ErrNotExist) {
		return state{}, false, nil
	}
	if err != nil {
		return state{}, false, err
	}
	var stored state
	if err := json.Unmarshal(payload, &stored); err != nil {
		return state{}, false, err
	}
	if stored.TaskID != taskID || stored.RepositoryID == "" || stored.BaseCommit == "" || stored.Turn < 0 {
		return state{}, false, errors.New("invalid persisted apply state")
	}
	if stored.Turn == 0 {
		if stored.Digest != "" || len(stored.CumulativePatch) != 0 || len(stored.Files) != 0 {
			return state{}, false, errors.New("invalid persisted task binding")
		}
	} else if stored.Digest == "" || len(stored.CumulativePatch) == 0 {
		return state{}, false, errors.New("invalid persisted applied artifact state")
	}
	seen := make(map[string]struct{}, len(stored.Files))
	for _, file := range stored.Files {
		canonical, err := canonicalArtifactPath(file.Path)
		if err != nil || canonical != file.Path || !validFileOracle(file) {
			return state{}, false, fmt.Errorf("invalid persisted apply state file metadata for %q", file.Path)
		}
		if _, duplicate := seen[canonical]; duplicate {
			return state{}, false, fmt.Errorf("duplicate persisted apply state file %q", canonical)
		}
		seen[canonical] = struct{}{}
	}
	return stored, true, nil
}

func (engine *Engine) saveState(stored state) error {
	payload, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(engine.stateRoot, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(engine.stateRoot, ".apply-*")
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
	return os.Rename(name, engine.statePath(stored.TaskID))
}

func (engine *Engine) git(ctx context.Context, env []string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", engine.repository, "--literal-pathspecs"}, args...)...)
	if env != nil {
		command.Env = env
	}
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
