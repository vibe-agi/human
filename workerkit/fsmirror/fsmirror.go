// Package fsmirror is the official filesystem Mirror adapter for workerkit.
//
// It watches a human-owned mirror directory, publishes a complete review of
// every difference against the delivered baseline after each debounced save,
// and projects confirmed changes onto caller-declared native tools through a
// host-supplied builder. The builder is the harness-mapping seam: until the
// Harness SPI exists, the host decides how a changed file becomes an exact
// native write/edit call.
//
// Semantics relative to the frozen TUI mirror, stated honestly:
//
//   - The baseline advances only on Settle(MirrorDelivered), i.e. after the
//     caller returned successful results — never at send time.
//   - Resolve reads the file bytes at resolve time and freezes them for the
//     eventual baseline advance; a save between review and resolve delivers
//     the newer bytes.
//   - With BaselineFile configured, the baseline survives process restart, so
//     undelivered edits reappear in review after a crash. Without it the
//     baseline re-seeds from the current tree at Open and pre-restart edits
//     are treated as already delivered.
//   - Symlinks and non-regular files are skipped per path and surfaced as
//     warnings, never followed.
package fsmirror

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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/fsnotify/fsnotify"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

// BuildFunc maps one confirmed change onto native tool calls. content is the
// exact file bytes frozen at resolve time (nil for a delete). Change.Path is a
// project-relative path: native tools resolve it against the Agent user's own
// cwd, which is intentionally unknown to the Human host.
type BuildFunc func(change workerkit.Change, content []byte, request workerkit.MirrorResolve) ([]llm.ToolCall, error)

// BuildSnapshot is the byte-exact input for builders that map modifications
// onto native compare-and-replace tools. Before is the delivered baseline and
// After is the frozen mirror content (nil for a delete). The builder owns these
// slices and may retain or modify them.
type BuildSnapshot struct {
	Change workerkit.Change
	Before []byte
	After  []byte
}

// SnapshotBuildFunc is the richer builder extension point. Config accepts it
// as an alternative to the legacy BuildFunc so existing library users keep
// their implementation while native edit profiles can receive old/new bytes.
type SnapshotBuildFunc func(snapshot BuildSnapshot, request workerkit.MirrorResolve) ([]llm.ToolCall, error)

// NativeProfile binds one exact harness version to its native file-tool
// projection. Profiles are immutable after registry construction.
type NativeProfile struct {
	HarnessID      string
	HarnessVersion string
	Build          SnapshotBuildFunc
}

// NativeBuilderRegistry dispatches byte-exact snapshots by authenticated
// harness identity. It never guesses a profile from overlapping tool names.
type NativeBuilderRegistry struct {
	builders map[string]SnapshotBuildFunc
}

// NewNativeBuilderRegistry validates and freezes exact native profiles.
func NewNativeBuilderRegistry(profiles ...NativeProfile) (*NativeBuilderRegistry, error) {
	registry := &NativeBuilderRegistry{builders: make(map[string]SnapshotBuildFunc, len(profiles))}
	for _, profile := range profiles {
		id := strings.TrimSpace(profile.HarnessID)
		version := strings.TrimSpace(profile.HarnessVersion)
		if id == "" || version == "" || id != profile.HarnessID || version != profile.HarnessVersion ||
			strings.Contains(id, "@") || strings.Contains(version, "@") || profile.Build == nil {
			return nil, fmt.Errorf("%w: native profile identity and builder are required", ErrConfig)
		}
		key := id + "@" + version
		if _, exists := registry.builders[key]; exists {
			return nil, fmt.Errorf("%w: duplicate native profile %s", ErrConfig, key)
		}
		registry.builders[key] = profile.Build
	}
	if len(registry.builders) == 0 {
		return nil, fmt.Errorf("%w: at least one native profile is required", ErrConfig)
	}
	return registry, nil
}

// Build dispatches one snapshot to its exact native profile.
func (registry *NativeBuilderRegistry) Build(
	snapshot BuildSnapshot,
	request workerkit.MirrorResolve,
) ([]llm.ToolCall, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: native builder registry is nil", ErrConfig)
	}
	key := request.HarnessID + "@" + request.HarnessVersion
	builder, exists := registry.builders[key]
	if !exists {
		return nil, fmt.Errorf("fsmirror: no native file profile for %s", key)
	}
	return builder(snapshot, request)
}

// OpenCodeWriteBuilder maps creates and modifies onto OpenCode's native
// write tool (whole-file content, project-relative filePath). Deletes are not
// mapped.
func OpenCodeWriteBuilder() BuildFunc {
	return func(change workerkit.Change, content []byte, request workerkit.MirrorResolve) ([]llm.ToolCall, error) {
		if change.Kind == workerkit.ChangeDelete {
			return nil, fmt.Errorf("fsmirror: delete has no mapped native tool")
		}
		declared := false
		for _, tool := range request.Tools {
			if tool.Namespace == "" && tool.Name == "write" {
				declared = true
			}
		}
		if !declared {
			return nil, fmt.Errorf("fsmirror: caller did not declare a write tool")
		}
		return []llm.ToolCall{{
			ID: "call-" + change.ID, Name: "write",
			Input: map[string]any{
				"filePath": change.Path,
				"content":  string(content),
			},
		}}, nil
	}
}

// OpenCodeNativeBuilder maps creates to OpenCode's whole-file write tool and
// modifications to its exact edit tool. OpenCode 1.17.18 declares no native
// delete capability, so deletes fail closed instead of being smuggled through
// a shell command.
func OpenCodeNativeBuilder() SnapshotBuildFunc {
	return func(snapshot BuildSnapshot, request workerkit.MirrorResolve) ([]llm.ToolCall, error) {
		change := snapshot.Change
		declared := func(name string) bool {
			for _, tool := range request.Tools {
				if tool.Namespace == "" && tool.Name == name {
					return true
				}
			}
			return false
		}
		path := change.Path
		call := llm.ToolCall{ID: "call-" + change.ID}
		switch change.Kind {
		case workerkit.ChangeCreate:
			if !declared("write") {
				return nil, fmt.Errorf("fsmirror: caller did not declare a write tool")
			}
			call.Name = "write"
			call.Input = map[string]any{"filePath": path, "content": string(snapshot.After)}
		case workerkit.ChangeModify:
			if !declared("edit") {
				return nil, fmt.Errorf("fsmirror: caller did not declare an edit tool")
			}
			call.Name = "edit"
			call.Input = map[string]any{
				"filePath": path, "oldString": string(snapshot.Before), "newString": string(snapshot.After),
			}
		case workerkit.ChangeDelete:
			return nil, fmt.Errorf("fsmirror: delete has no mapped native tool")
		default:
			return nil, fmt.Errorf("fsmirror: unsupported change kind %q", change.Kind)
		}
		return []llm.ToolCall{call}, nil
	}
}

// ClaudeCodeNativeBuilder maps creates and modifications onto the exact
// Claude Code 2.1.217 Write/Edit schemas. Deletes fail closed because that
// version declares no native file-delete tool.
func ClaudeCodeNativeBuilder() SnapshotBuildFunc {
	return func(snapshot BuildSnapshot, request workerkit.MirrorResolve) ([]llm.ToolCall, error) {
		change := snapshot.Change
		declared := func(name string) bool {
			for _, tool := range request.Tools {
				if tool.Namespace == "" && tool.Name == name {
					return true
				}
			}
			return false
		}
		path := change.Path
		call := llm.ToolCall{ID: "call-" + change.ID}
		switch change.Kind {
		case workerkit.ChangeCreate:
			if !declared("Write") {
				return nil, fmt.Errorf("fsmirror: caller did not declare a Write tool")
			}
			call.Name = "Write"
			call.Input = map[string]any{"file_path": path, "content": string(snapshot.After)}
		case workerkit.ChangeModify:
			if !declared("Edit") {
				return nil, fmt.Errorf("fsmirror: caller did not declare an Edit tool")
			}
			call.Name = "Edit"
			call.Input = map[string]any{
				"file_path": path, "old_string": string(snapshot.Before), "new_string": string(snapshot.After),
			}
		case workerkit.ChangeDelete:
			return nil, fmt.Errorf("fsmirror: delete has no mapped native tool")
		default:
			return nil, fmt.Errorf("fsmirror: unsupported change kind %q", change.Kind)
		}
		return []llm.ToolCall{call}, nil
	}
}

// CodexApplyPatchBuilder maps byte-exact text snapshots onto Codex 0.145.0's
// Responses custom/freeform apply_patch tool. Paths stay project-relative.
// Codex apply_patch normalizes non-empty files to a trailing newline, so this
// builder fails closed when it cannot preserve the reviewed bytes exactly.
func CodexApplyPatchBuilder() SnapshotBuildFunc {
	return func(snapshot BuildSnapshot, request workerkit.MirrorResolve) ([]llm.ToolCall, error) {
		var declared bool
		for _, tool := range request.Tools {
			if tool.Namespace == "" && tool.Name == "apply_patch" &&
				tool.InputKind == llm.ToolInputText {
				declared = true
				break
			}
		}
		if !declared {
			return nil, fmt.Errorf("fsmirror: caller did not declare a text-input apply_patch tool")
		}
		path := filepath.ToSlash(snapshot.Change.Path)
		if path == "" || strings.ContainsAny(path, "\r\n") {
			return nil, fmt.Errorf("fsmirror: path cannot be represented by apply_patch")
		}
		var patch strings.Builder
		patch.WriteString("*** Begin Patch\n")
		switch snapshot.Change.Kind {
		case workerkit.ChangeCreate:
			if err := validateApplyPatchText(snapshot.After, "reviewed content"); err != nil {
				return nil, err
			}
			if len(snapshot.After) == 0 {
				return nil, fmt.Errorf("fsmirror: Codex apply_patch cannot create an empty file exactly")
			}
			if !bytes.HasSuffix(snapshot.After, []byte("\n")) {
				return nil, fmt.Errorf("fsmirror: Codex apply_patch requires created text to end with a newline")
			}
			patch.WriteString("*** Add File: ")
			patch.WriteString(path)
			patch.WriteByte('\n')
			writePatchLines(&patch, '+', snapshot.After)
		case workerkit.ChangeModify:
			if err := validateApplyPatchText(snapshot.Before, "baseline"); err != nil {
				return nil, err
			}
			if err := validateApplyPatchText(snapshot.After, "reviewed content"); err != nil {
				return nil, err
			}
			if len(snapshot.After) != 0 && !bytes.HasSuffix(snapshot.After, []byte("\n")) {
				return nil, fmt.Errorf("fsmirror: Codex apply_patch requires modified text to end with a newline")
			}
			patch.WriteString("*** Update File: ")
			patch.WriteString(path)
			patch.WriteString("\n@@\n")
			writePatchLines(&patch, '-', snapshot.Before)
			writePatchLines(&patch, '+', snapshot.After)
			patch.WriteString("*** End of File\n")
		case workerkit.ChangeDelete:
			patch.WriteString("*** Delete File: ")
			patch.WriteString(path)
			patch.WriteByte('\n')
		default:
			return nil, fmt.Errorf("fsmirror: unsupported change kind %q", snapshot.Change.Kind)
		}
		patch.WriteString("*** End Patch")
		input := patch.String()
		return []llm.ToolCall{{
			ID: "call-" + snapshot.Change.ID, Name: "apply_patch", TextInput: &input,
		}}, nil
	}
}

func validateApplyPatchText(content []byte, label string) error {
	if !utf8.Valid(content) || bytes.IndexByte(content, 0) >= 0 ||
		bytes.IndexByte(content, '\r') >= 0 {
		return fmt.Errorf("fsmirror: %s is not portable UTF-8 text for Codex apply_patch", label)
	}
	return nil
}

func writePatchLines(destination *strings.Builder, prefix byte, content []byte) {
	if len(content) == 0 {
		return
	}
	for _, line := range strings.Split(strings.TrimSuffix(string(content), "\n"), "\n") {
		destination.WriteByte(prefix)
		destination.WriteString(line)
		destination.WriteByte('\n')
	}
}

var (
	ErrConfig = errors.New("fsmirror: invalid configuration")
	ErrClosed = errors.New("fsmirror: mirror is closed")
)

// Config composes one filesystem mirror.
type Config struct {
	// Root is the Human-owned base workspace. Each harness session receives one
	// stable child directory beneath it. Root must exist.
	Root string
	// Scope is the exact authenticated Agent-user workspace this Human-owned
	// directory may deliver into.
	Scope workerkit.WorkspaceScope
	// Build projects a confirmed change onto native tool calls.
	Build BuildFunc
	// BuildSnapshot is the byte-exact builder variant used by native edit
	// profiles. Exactly one of Build and BuildSnapshot is required.
	BuildSnapshot SnapshotBuildFunc
	// BaselineFile optionally persists the delivered baseline (outside Root)
	// so undelivered edits survive a process restart.
	BaselineFile string
	// Debounce coalesces filesystem events before a fresh full review. Zero
	// selects 250ms.
	Debounce time.Duration
	// MaxFileBytes bounds one reviewed file. Zero selects 8 MiB; larger files
	// are skipped with a per-path warning.
	MaxFileBytes int64
}

// Mirror is the filesystem implementation of workerkit.Mirror.
type Mirror struct {
	root          string
	scope         workerkit.WorkspaceScope
	build         BuildFunc
	buildSnapshot SnapshotBuildFunc
	baselineFile  string
	debounce      time.Duration
	maxFileBytes  int64

	watcher *fsnotify.Watcher
	reviews chan workerkit.Review
	watchMu sync.Mutex

	mu       sync.Mutex
	closed   bool
	baseline map[string][]byte // workspace id/project path -> delivered content
	// baselineRoots records which canonical Human directory each persisted
	// baseline belongs to. A directory switch always seeds a fresh baseline.
	baselineRoots  map[string]string
	legacyBaseline bool
	inflight       map[string][]byte // workspace id/project path -> resolved content
	// inflightByID keeps settlement bookkeeping stable across review
	// republication: a resolved change stays settleable until Settle.
	inflightByID map[string]inflightRecord
	generation   uint64
	lastReview   map[string]reviewRecord // change id -> review/storage record
	sessions     map[string]sessionRecord

	done chan struct{}
}

var _ workerkit.Mirror = (*Mirror)(nil)
var _ workerkit.SessionMirror = (*Mirror)(nil)

// Open scans Root, loads the persisted baseline when configured, publishes an
// initial review, and starts watching. ctx bounds construction only.
func Open(ctx context.Context, config Config) (*Mirror, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if (config.Build == nil) == (config.BuildSnapshot == nil) {
		return nil, fmt.Errorf("%w: exactly one of Build and BuildSnapshot is required", ErrConfig)
	}
	if err := config.Scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConfig, err)
	}
	root, err := filepath.Abs(strings.TrimSpace(config.Root))
	if err != nil || config.Root == "" {
		return nil, fmt.Errorf("%w: Root is required", ErrConfig)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve Root: %v", ErrConfig, err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("%w: Root must be an existing directory", ErrConfig)
	}
	if config.BaselineFile != "" {
		absolute, err := filepath.Abs(config.BaselineFile)
		if err != nil {
			return nil, fmt.Errorf("%w: BaselineFile: %v", ErrConfig, err)
		}
		if strings.HasPrefix(absolute+string(filepath.Separator), root+string(filepath.Separator)) {
			return nil, fmt.Errorf("%w: BaselineFile must live outside Root", ErrConfig)
		}
		config.BaselineFile = absolute
	}
	debounce := config.Debounce
	if debounce == 0 {
		debounce = 250 * time.Millisecond
	}
	maxFileBytes := config.MaxFileBytes
	if maxFileBytes == 0 {
		maxFileBytes = 8 << 20
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("fsmirror: start watcher: %w", err)
	}
	mirror := &Mirror{
		root: root, scope: config.Scope,
		build: config.Build, buildSnapshot: config.BuildSnapshot, baselineFile: config.BaselineFile,
		debounce: debounce, maxFileBytes: maxFileBytes,
		watcher: watcher, reviews: make(chan workerkit.Review, 1),
		inflight:     make(map[string][]byte),
		inflightByID: make(map[string]inflightRecord),
		lastReview:   make(map[string]reviewRecord),
		sessions:     make(map[string]sessionRecord),
		done:         make(chan struct{}),
	}
	baseline, roots, legacy, err := mirror.loadBaseline()
	if err != nil {
		_ = watcher.Close()
		return nil, err
	}
	if baseline == nil {
		baseline = make(map[string][]byte)
	}
	if roots == nil {
		roots = make(map[string]string)
	}
	mirror.baseline = baseline
	mirror.baselineRoots = roots
	mirror.legacyBaseline = legacy
	go mirror.run()
	mirror.publish()
	return mirror, nil
}

// Reviews implements workerkit.Mirror.
func (mirror *Mirror) Reviews() <-chan workerkit.Review { return mirror.reviews }

// PrepareSession creates (or reopens) the stable Human-side child directory
// for one harness session. It is idempotent across resumes and restarts.
func (mirror *Mirror) PrepareSession(
	ctx context.Context,
	binding workerkit.SessionBinding,
	preferredPath string,
) (workerkit.HumanWorkspace, error) {
	if ctx == nil {
		return workerkit.HumanWorkspace{}, fmt.Errorf("%w: context is required", ErrConfig)
	}
	if err := ctx.Err(); err != nil {
		return workerkit.HumanWorkspace{}, err
	}
	if binding.Scope != mirror.scope {
		return workerkit.HumanWorkspace{}, fmt.Errorf(
			"%w: mirror is bound to %s/%s, session is %s/%s",
			workerkit.ErrMirrorScopeMismatch,
			mirror.scope.Caller, mirror.scope.WorkspaceKey,
			binding.Scope.Caller, binding.Scope.WorkspaceKey,
		)
	}
	id, err := workerkit.SessionWorkspaceID(binding)
	if err != nil {
		return workerkit.HumanWorkspace{}, fmt.Errorf("%w: %v", ErrConfig, err)
	}
	path, isDefault, err := mirror.resolveSessionPath(id, preferredPath)
	if err != nil {
		return workerkit.HumanWorkspace{}, err
	}
	current, _, err := mirror.scanWorkspace(id, path)
	if err != nil {
		return workerkit.HumanWorkspace{}, err
	}

	mirror.mu.Lock()
	if mirror.closed {
		mirror.mu.Unlock()
		return workerkit.HumanWorkspace{}, ErrClosed
	}
	if existing, found := mirror.sessions[id]; found && existing.binding != binding {
		mirror.mu.Unlock()
		return workerkit.HumanWorkspace{}, fmt.Errorf("%w: session workspace collision", ErrConfig)
	}
	persistedRoot := mirror.baselineRoots[id]
	preserveLegacy := mirror.legacyBaseline && isDefault && persistedRoot == "" &&
		mirror.hasWorkspaceBaselineLocked(id)
	reseed := persistedRoot != path && !preserveLegacy
	oldBaseline := cloneFileMap(mirror.baseline)
	oldRoots := cloneStringMap(mirror.baselineRoots)
	oldSession, hadSession := mirror.sessions[id]
	mirror.sessions[id] = sessionRecord{binding: binding, path: path}
	if reseed {
		mirror.clearWorkspaceStateLocked(id)
		for storagePath, content := range current {
			mirror.baseline[storagePath] = content
		}
	}
	mirror.baselineRoots[id] = path
	if err := mirror.persistBaselineLocked(); err != nil {
		mirror.baseline = oldBaseline
		mirror.baselineRoots = oldRoots
		if hadSession {
			mirror.sessions[id] = oldSession
		} else {
			delete(mirror.sessions, id)
		}
		mirror.mu.Unlock()
		return workerkit.HumanWorkspace{}, err
	}
	mirror.legacyBaseline = false
	mirror.mu.Unlock()

	if err := mirror.reconcileWatches(); err != nil {
		return workerkit.HumanWorkspace{}, fmt.Errorf("fsmirror: watch session workspace: %w", err)
	}
	mirror.publish()
	return workerkit.HumanWorkspace{ID: id, Path: path, Available: true}, nil
}

func (mirror *Mirror) resolveSessionPath(id, preferredPath string) (string, bool, error) {
	preferredPath = strings.TrimSpace(preferredPath)
	isDefault := preferredPath == ""
	path := preferredPath
	if isDefault {
		path = filepath.Join(mirror.root, id)
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return "", false, fmt.Errorf("fsmirror: create default session workspace: %w", err)
		}
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", false, fmt.Errorf("%w: resolve Human workspace: %v", ErrConfig, err)
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", false, fmt.Errorf("%w: Human workspace must be an existing directory: %v", ErrConfig, err)
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.IsDir() {
		return "", false, fmt.Errorf("%w: Human workspace must be an existing directory", ErrConfig)
	}
	return canonical, isDefault, nil
}

// Close stops the watcher and closes the review stream.
func (mirror *Mirror) Close() error {
	mirror.mu.Lock()
	if mirror.closed {
		mirror.mu.Unlock()
		return nil
	}
	mirror.closed = true
	mirror.mu.Unlock()
	err := mirror.watcher.Close()
	<-mirror.done
	return err
}

func (mirror *Mirror) run() {
	defer close(mirror.done)
	var timer *time.Timer
	var timerC <-chan time.Time
	for {
		select {
		case event, open := <-mirror.watcher.Events:
			if !open {
				return
			}
			if mirror.ignored(event.Name) {
				continue
			}
			if event.Op&fsnotify.Create != 0 {
				if info, err := os.Lstat(event.Name); err == nil && info.IsDir() {
					// Walk the new subtree, not just its root: mkdir -p a/b can
					// create b before a's watch is installed, so b's own create
					// event is missed. Re-adding every descendant (Add is
					// idempotent) closes that race.
					_ = mirror.addTree(event.Name)
				}
			}
			if timer == nil {
				timer = time.NewTimer(mirror.debounce)
				timerC = timer.C
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(mirror.debounce)
			}
		case <-timerC:
			timer = nil
			timerC = nil
			mirror.publish()
		case _, open := <-mirror.watcher.Errors:
			if !open {
				return
			}
		}
	}
}

func (mirror *Mirror) ignored(path string) bool {
	for _, segment := range strings.Split(filepath.ToSlash(filepath.Clean(path)), "/") {
		if segment == ".git" {
			return true
		}
	}
	return false
}

// addTree adds root and every directory under it to the watcher, ignoring
// .git. Add is idempotent, so re-adding already-watched directories is safe.
func (mirror *Mirror) addTree(root string) error {
	mirror.watchMu.Lock()
	defer mirror.watchMu.Unlock()
	return mirror.addTreeLocked(root)
}

func (mirror *Mirror) addTreeLocked(root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if mirror.ignored(path) && path != root {
				return filepath.SkipDir
			}
			return mirror.watcher.Add(path)
		}
		return nil
	})
}

func (mirror *Mirror) reconcileWatches() error {
	mirror.mu.Lock()
	roots := make(map[string]struct{}, len(mirror.sessions))
	for _, session := range mirror.sessions {
		roots[session.path] = struct{}{}
	}
	closed := mirror.closed
	mirror.mu.Unlock()
	if closed {
		return ErrClosed
	}
	ordered := make([]string, 0, len(roots))
	for root := range roots {
		ordered = append(ordered, root)
	}
	sort.Strings(ordered)

	mirror.watchMu.Lock()
	defer mirror.watchMu.Unlock()
	for _, watched := range mirror.watcher.WatchList() {
		if err := mirror.watcher.Remove(watched); err != nil &&
			!errors.Is(err, fsnotify.ErrNonExistentWatch) {
			return err
		}
	}
	for _, root := range ordered {
		if err := mirror.addTreeLocked(root); err != nil {
			return err
		}
	}
	return nil
}

// scan reads every prepared session tree. Storage keys include the workspace
// id; paths exposed through Change are stripped back to project-relative.
func (mirror *Mirror) scan() (map[string][]byte, map[string]string, error) {
	files := make(map[string][]byte)
	warnings := make(map[string]string)
	ids := make([]string, 0, len(mirror.sessions))
	for id := range mirror.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		sessionFiles, sessionWarnings, err := mirror.scanWorkspace(id, mirror.sessions[id].path)
		if err != nil {
			return nil, nil, err
		}
		for path, content := range sessionFiles {
			files[path] = content
		}
		for path, warning := range sessionWarnings {
			warnings[path] = warning
		}
	}
	return files, warnings, nil
}

func (mirror *Mirror) scanWorkspace(
	workspaceID string,
	root string,
) (map[string][]byte, map[string]string, error) {
	files := make(map[string][]byte)
	warnings := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if mirror.ignored(path) {
			if entry.IsDir() && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		storagePath := workspaceID + "/" + relative
		if entry.Type()&fs.ModeSymlink != 0 || !entry.Type().IsRegular() {
			warnings[storagePath] = "skipped: not a regular file"
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			warnings[storagePath] = "skipped: " + err.Error()
			return nil
		}
		if info.Size() > mirror.maxFileBytes {
			warnings[storagePath] = fmt.Sprintf("skipped: %d bytes exceeds the review limit", info.Size())
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			warnings[storagePath] = "skipped: " + err.Error()
			return nil
		}
		files[storagePath] = content
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("fsmirror: scan Human workspace %s: %w", workspaceID, err)
	}
	return files, warnings, nil
}

// publish computes a fresh complete review against the baseline and replaces
// the pending one.
func (mirror *Mirror) publish() {
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	if mirror.closed {
		return
	}
	current, warnings, err := mirror.scan()
	if err != nil {
		return
	}
	changes := make([]workerkit.Change, 0)
	paths := make(map[string]struct{}, len(current)+len(mirror.baseline))
	for path := range current {
		paths[path] = struct{}{}
	}
	for path := range mirror.baseline {
		paths[path] = struct{}{}
	}
	ordered := make([]string, 0, len(paths))
	for path := range paths {
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	mirror.lastReview = make(map[string]reviewRecord)
	for _, storagePath := range ordered {
		workspaceID, projectPath, knownSession := mirror.projectPath(storagePath)
		if !knownSession {
			continue
		}
		content, exists := current[storagePath]
		baseline, hadBaseline := mirror.baseline[storagePath]
		if inflight, isInflight := mirror.inflight[storagePath]; isInflight && exists && bytesEqual(content, inflight) {
			// Being delivered right now; it reappears only if it changes again
			// or the delivery fails and the batch is left unsettled.
			continue
		}
		var change workerkit.Change
		switch {
		case exists && !hadBaseline:
			change = workerkit.Change{
				WorkspaceID: workspaceID, Path: projectPath,
				Kind: workerkit.ChangeCreate, Diff: lineDiff(nil, content),
			}
		case !exists && hadBaseline:
			change = workerkit.Change{
				WorkspaceID: workspaceID, Path: projectPath,
				Kind: workerkit.ChangeDelete, Diff: lineDiff(baseline, nil),
			}
		case exists && hadBaseline && !bytesEqual(content, baseline):
			change = workerkit.Change{
				WorkspaceID: workspaceID, Path: projectPath,
				Kind: workerkit.ChangeModify, Diff: lineDiff(baseline, content),
			}
		default:
			continue
		}
		change.ID = changeID(storagePath, content)
		change.Warning = warnings[storagePath]
		changes = append(changes, change)
		mirror.lastReview[change.ID] = reviewRecord{change: change, storagePath: storagePath}
	}
	for storagePath, warning := range warnings {
		workspaceID, projectPath, knownSession := mirror.projectPath(storagePath)
		if !knownSession {
			continue
		}
		if _, seen := current[storagePath]; !seen {
			if _, hadBaseline := mirror.baseline[storagePath]; !hadBaseline {
				change := workerkit.Change{
					ID: changeID(storagePath, nil), WorkspaceID: workspaceID,
					Path: projectPath, Kind: workerkit.ChangeModify, Warning: warning,
				}
				changes = append(changes, change)
				mirror.lastReview[change.ID] = reviewRecord{change: change, storagePath: storagePath}
			}
		}
	}
	mirror.generation++
	review := workerkit.Review{Generation: mirror.generation, Changes: changes}
	select {
	case mirror.reviews <- review:
	default:
		select {
		case <-mirror.reviews:
		default:
		}
		mirror.reviews <- review
	}
}

func (mirror *Mirror) projectPath(storagePath string) (string, string, bool) {
	parts := strings.SplitN(filepath.ToSlash(storagePath), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	if _, known := mirror.sessions[parts[0]]; !known {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// Resolve implements workerkit.Mirror: it freezes the current bytes of each
// selected change and builds native tool calls for them.
func (mirror *Mirror) Resolve(ctx context.Context, request workerkit.MirrorResolve) ([]llm.ToolCall, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Scope != mirror.scope {
		return nil, fmt.Errorf("%w: mirror is bound to %s/%s, request is %s/%s",
			workerkit.ErrMirrorScopeMismatch,
			mirror.scope.Caller, mirror.scope.WorkspaceKey,
			request.Scope.Caller, request.Scope.WorkspaceKey)
	}
	mirror.mu.Lock()
	if mirror.closed {
		mirror.mu.Unlock()
		return nil, ErrClosed
	}
	session, known := mirror.sessions[request.Workspace.ID]
	if request.Workspace.ID == "" || !known ||
		filepath.Clean(request.Workspace.Path) != session.path {
		mirror.mu.Unlock()
		return nil, fmt.Errorf("%w: Human workspace does not belong to this mirror", workerkit.ErrMirrorScopeMismatch)
	}
	var calls []llm.ToolCall
	frozen := make(map[string][]byte, len(request.ChangeIDs))
	fail := func(err error) ([]llm.ToolCall, error) {
		mirror.mu.Unlock()
		return nil, err
	}
	for _, id := range request.ChangeIDs {
		record, known := mirror.lastReview[id]
		if !known {
			return fail(fmt.Errorf("%w: %s", workerkit.ErrUnknownChange, id))
		}
		change := record.change
		if change.WorkspaceID != request.Workspace.ID {
			return fail(fmt.Errorf("%w: change %s belongs to another Human workspace",
				workerkit.ErrMirrorScopeMismatch, id))
		}
		if change.Warning != "" {
			return fail(fmt.Errorf("fsmirror: change %s is not deliverable: %s", change.Path, change.Warning))
		}
		var content []byte
		if change.Kind != workerkit.ChangeDelete {
			absolute, err := mirror.absolutePathLocked(record.storagePath)
			if err != nil {
				return fail(err)
			}
			read, err := os.ReadFile(absolute)
			if err != nil {
				return fail(fmt.Errorf("fsmirror: read %s: %w", change.Path, err))
			}
			content = read
		}
		var built []llm.ToolCall
		var err error
		if mirror.buildSnapshot != nil {
			built, err = mirror.buildSnapshot(BuildSnapshot{
				Change: change,
				Before: bytes.Clone(mirror.baseline[record.storagePath]),
				After:  bytes.Clone(content),
			}, request)
		} else {
			built, err = mirror.build(change, content, request)
		}
		if err != nil {
			return fail(fmt.Errorf("fsmirror: build calls for %s: %w", change.Path, err))
		}
		calls = append(calls, built...)
		frozen[record.storagePath] = content
		mirror.inflightByID[id] = inflightRecord{
			path: record.storagePath, kind: change.Kind, content: content,
		}
	}
	for path, content := range frozen {
		mirror.inflight[path] = content
	}
	mirror.mu.Unlock()
	// Republish so in-flight changes leave the review immediately.
	mirror.publish()
	return calls, nil
}

type inflightRecord struct {
	path    string
	kind    workerkit.ChangeKind
	content []byte
}

type reviewRecord struct {
	change      workerkit.Change
	storagePath string
}

// Cancel implements workerkit.Mirror: it releases resolved-but-unsent changes
// so they reappear in the next review without advancing the baseline.
func (mirror *Mirror) Cancel(ctx context.Context, changeIDs []string) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrConfig)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	mirror.mu.Lock()
	if mirror.closed {
		mirror.mu.Unlock()
		return ErrClosed
	}
	for _, id := range changeIDs {
		if record, resolved := mirror.inflightByID[id]; resolved {
			delete(mirror.inflight, record.path)
			delete(mirror.inflightByID, id)
		}
	}
	mirror.mu.Unlock()
	mirror.publish()
	return nil
}

// Settle implements workerkit.Mirror.
func (mirror *Mirror) Settle(ctx context.Context, settlement workerkit.MirrorSettlement) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrConfig)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	mirror.mu.Lock()
	if mirror.closed {
		mirror.mu.Unlock()
		return ErrClosed
	}
	for _, id := range settlement.ChangeIDs {
		record, resolved := mirror.inflightByID[id]
		if !resolved {
			// Discarding an unresolved change is legal; anything else missing is
			// an idempotent repeat of an already settled batch.
			if settlement.Outcome == workerkit.MirrorDiscarded {
				if review, known := mirror.lastReview[id]; known {
					record = inflightRecord{path: review.storagePath, kind: review.change.Kind}
					resolved = true
				}
			}
			if !resolved {
				continue
			}
		}
		switch settlement.Outcome {
		case workerkit.MirrorDelivered:
			if record.kind == workerkit.ChangeDelete || record.content == nil {
				delete(mirror.baseline, record.path)
			} else {
				mirror.baseline[record.path] = record.content
			}
		case workerkit.MirrorDiscarded:
			// Dropping a change accepts the current mirror content as baseline so
			// it stops appearing without being delivered. A warning-only change
			// (oversize or non-regular, skipped by scan) must not be read whole
			// into memory/baseline — drop it from the baseline instead.
			absolute, pathErr := mirror.absolutePathLocked(record.path)
			if pathErr != nil {
				delete(mirror.baseline, record.path)
				break
			}
			info, statErr := os.Lstat(absolute)
			if statErr != nil || !info.Mode().IsRegular() || info.Size() > mirror.maxFileBytes {
				delete(mirror.baseline, record.path)
			} else if content, readErr := os.ReadFile(absolute); readErr != nil {
				delete(mirror.baseline, record.path)
			} else {
				mirror.baseline[record.path] = content
			}
		}
		delete(mirror.inflight, record.path)
		delete(mirror.inflightByID, id)
		delete(mirror.lastReview, id)
	}
	err := mirror.persistBaselineLocked()
	mirror.mu.Unlock()
	mirror.publish()
	return err
}

type persistedBaseline struct {
	Version uint8             `json:"version"`
	Roots   map[string]string `json:"roots"`
	Files   map[string][]byte `json:"files"`
}

const persistedBaselineVersion = 2

func (mirror *Mirror) loadBaseline() (map[string][]byte, map[string]string, bool, error) {
	if mirror.baselineFile == "" {
		return nil, nil, false, nil
	}
	payload, err := os.ReadFile(mirror.baselineFile)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("fsmirror: read baseline: %w", err)
	}
	var state persistedBaseline
	if json.Unmarshal(payload, &state) == nil && state.Version != 0 {
		if state.Version != persistedBaselineVersion || state.Files == nil || state.Roots == nil {
			return nil, nil, false, fmt.Errorf("fsmirror: unsupported baseline version %d", state.Version)
		}
		return state.Files, state.Roots, false, nil
	}
	var encoded map[string][]byte
	if err := json.Unmarshal(payload, &encoded); err != nil {
		return nil, nil, false, fmt.Errorf("fsmirror: decode baseline: %w", err)
	}
	return encoded, make(map[string]string), true, nil
}

func (mirror *Mirror) persistBaselineLocked() error {
	if mirror.baselineFile == "" {
		return nil
	}
	payload, err := json.Marshal(persistedBaseline{
		Version: persistedBaselineVersion,
		Roots:   mirror.baselineRoots,
		Files:   mirror.baseline,
	})
	if err != nil {
		return fmt.Errorf("fsmirror: encode baseline: %w", err)
	}
	staging := mirror.baselineFile + ".staging"
	if err := os.WriteFile(staging, payload, 0o600); err != nil {
		return fmt.Errorf("fsmirror: stage baseline: %w", err)
	}
	if err := os.Rename(staging, mirror.baselineFile); err != nil {
		return fmt.Errorf("fsmirror: install baseline: %w", err)
	}
	return nil
}

type sessionRecord struct {
	binding workerkit.SessionBinding
	path    string
}

func (mirror *Mirror) absolutePathLocked(storagePath string) (string, error) {
	workspaceID, projectPath, known := mirror.projectPath(storagePath)
	if !known {
		return "", fmt.Errorf("%w: unknown Human workspace path", workerkit.ErrMirrorScopeMismatch)
	}
	session := mirror.sessions[workspaceID]
	absolute := filepath.Join(session.path, filepath.FromSlash(projectPath))
	relative, err := filepath.Rel(session.path, absolute)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: Human workspace path escapes its root", workerkit.ErrMirrorScopeMismatch)
	}
	return absolute, nil
}

func (mirror *Mirror) hasWorkspaceBaselineLocked(workspaceID string) bool {
	prefix := workspaceID + "/"
	for path := range mirror.baseline {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (mirror *Mirror) clearWorkspaceStateLocked(workspaceID string) {
	prefix := workspaceID + "/"
	for path := range mirror.baseline {
		if strings.HasPrefix(path, prefix) {
			delete(mirror.baseline, path)
		}
	}
	for path := range mirror.inflight {
		if strings.HasPrefix(path, prefix) {
			delete(mirror.inflight, path)
		}
	}
	for id, record := range mirror.inflightByID {
		if strings.HasPrefix(record.path, prefix) {
			delete(mirror.inflightByID, id)
		}
	}
	for id, record := range mirror.lastReview {
		if strings.HasPrefix(record.storagePath, prefix) {
			delete(mirror.lastReview, id)
		}
	}
}

func cloneFileMap(input map[string][]byte) map[string][]byte {
	output := make(map[string][]byte, len(input))
	for key, value := range input {
		output[key] = bytes.Clone(value)
	}
	return output
}

func cloneStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func changeID(path string, content []byte) string {
	sum := sha256.New()
	sum.Write([]byte(path))
	sum.Write([]byte{0})
	sum.Write(content)
	return "chg-" + hex.EncodeToString(sum.Sum(nil))[:16]
}

func bytesEqual(left, right []byte) bool {
	return string(left) == string(right)
}

// lineDiff renders a compact common-prefix/suffix line diff for review
// display. It is presentation only; delivery always uses exact bytes.
func lineDiff(before, after []byte) string {
	beforeLines := splitLines(before)
	afterLines := splitLines(after)
	prefix := 0
	for prefix < len(beforeLines) && prefix < len(afterLines) && beforeLines[prefix] == afterLines[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(beforeLines)-prefix && suffix < len(afterLines)-prefix &&
		beforeLines[len(beforeLines)-1-suffix] == afterLines[len(afterLines)-1-suffix] {
		suffix++
	}
	var builder strings.Builder
	const contextCap = 200
	emit := func(prefix string, lines []string) {
		for index, line := range lines {
			if index >= contextCap {
				fmt.Fprintf(&builder, "%s… (%d more lines)\n", prefix, len(lines)-index)
				return
			}
			builder.WriteString(prefix)
			builder.WriteString(line)
			builder.WriteByte('\n')
		}
	}
	emit("-", beforeLines[prefix:len(beforeLines)-suffix])
	emit("+", afterLines[prefix:len(afterLines)-suffix])
	return builder.String()
}

func splitLines(content []byte) []string {
	if len(content) == 0 {
		return nil
	}
	return strings.Split(strings.TrimSuffix(string(content), "\n"), "\n")
}
