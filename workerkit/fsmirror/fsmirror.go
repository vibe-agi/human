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

	"github.com/fsnotify/fsnotify"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

// BuildFunc maps one confirmed change onto native tool calls. content is the
// exact file bytes frozen at resolve time (nil for a delete). The builder must
// only use tools declared in request.Tools; request.WorkspaceRoot is the
// caller's absolute workspace root for building native absolute paths.
type BuildFunc func(change workerkit.Change, content []byte, request workerkit.MirrorResolve) ([]llm.ToolCall, error)

// OpenCodeWriteBuilder maps creates and modifies onto OpenCode's native
// write tool (whole-file content, absolute filePath). Deletes are not mapped.
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
		if request.WorkspaceRoot == "" {
			return nil, fmt.Errorf("fsmirror: assignment has no workspace root")
		}
		return []llm.ToolCall{{
			ID: "call-" + change.ID, Name: "write",
			Input: map[string]any{
				"filePath": filepath.Join(request.WorkspaceRoot, filepath.FromSlash(change.Path)),
				"content":  string(content),
			},
		}}, nil
	}
}

var (
	ErrConfig = errors.New("fsmirror: invalid configuration")
	ErrClosed = errors.New("fsmirror: mirror is closed")
)

// Config composes one filesystem mirror.
type Config struct {
	// Root is the human-owned mirror directory. It must exist.
	Root string
	// Build projects a confirmed change onto native tool calls.
	Build BuildFunc
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
	root         string
	build        BuildFunc
	baselineFile string
	debounce     time.Duration
	maxFileBytes int64

	watcher *fsnotify.Watcher
	reviews chan workerkit.Review

	mu       sync.Mutex
	closed   bool
	baseline map[string][]byte // path -> delivered content
	inflight map[string][]byte // path -> resolved content awaiting settlement
	// inflightByID keeps settlement bookkeeping stable across review
	// republication: a resolved change stays settleable until Settle.
	inflightByID map[string]inflightRecord
	generation   uint64
	lastReview   map[string]workerkit.Change // change id -> change

	done chan struct{}
}

var _ workerkit.Mirror = (*Mirror)(nil)

// Open scans Root, loads the persisted baseline when configured, publishes an
// initial review, and starts watching. ctx bounds construction only.
func Open(ctx context.Context, config Config) (*Mirror, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.Build == nil {
		return nil, fmt.Errorf("%w: Build is required", ErrConfig)
	}
	root, err := filepath.Abs(strings.TrimSpace(config.Root))
	if err != nil || config.Root == "" {
		return nil, fmt.Errorf("%w: Root is required", ErrConfig)
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
		root: root, build: config.Build, baselineFile: config.BaselineFile,
		debounce: debounce, maxFileBytes: maxFileBytes,
		watcher: watcher, reviews: make(chan workerkit.Review, 1),
		inflight:     make(map[string][]byte),
		inflightByID: make(map[string]inflightRecord),
		lastReview:   make(map[string]workerkit.Change),
		done:         make(chan struct{}),
	}
	baseline, err := mirror.loadBaseline()
	if err != nil {
		_ = watcher.Close()
		return nil, err
	}
	if baseline == nil {
		// No persisted baseline: the current tree is the delivered state.
		baseline, _, err = mirror.scan()
		if err != nil {
			_ = watcher.Close()
			return nil, err
		}
	}
	mirror.baseline = baseline
	if err := mirror.watchTree(); err != nil {
		_ = watcher.Close()
		return nil, err
	}
	go mirror.run()
	mirror.publish()
	return mirror, nil
}

// Reviews implements workerkit.Mirror.
func (mirror *Mirror) Reviews() <-chan workerkit.Review { return mirror.reviews }

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
	relative, err := filepath.Rel(mirror.root, path)
	if err != nil {
		return true
	}
	for _, segment := range strings.Split(filepath.ToSlash(relative), "/") {
		if segment == ".git" {
			return true
		}
	}
	return false
}

// addTree adds root and every directory under it to the watcher, ignoring
// .git. Add is idempotent, so re-adding already-watched directories is safe.
func (mirror *Mirror) addTree(root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if mirror.ignored(path) && path != mirror.root {
				return filepath.SkipDir
			}
			return mirror.watcher.Add(path)
		}
		return nil
	})
}

func (mirror *Mirror) watchTree() error {
	return filepath.WalkDir(mirror.root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if mirror.ignored(path) && path != mirror.root {
				return filepath.SkipDir
			}
			return mirror.watcher.Add(path)
		}
		return nil
	})
}

// scan reads the current regular-file tree. It returns content by relative
// path and per-path warnings for skipped entries.
func (mirror *Mirror) scan() (map[string][]byte, map[string]string, error) {
	files := make(map[string][]byte)
	warnings := make(map[string]string)
	err := filepath.WalkDir(mirror.root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if mirror.ignored(path) {
			if entry.IsDir() && path != mirror.root {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(mirror.root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if entry.Type()&fs.ModeSymlink != 0 || !entry.Type().IsRegular() {
			warnings[relative] = "skipped: not a regular file"
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			warnings[relative] = "skipped: " + err.Error()
			return nil
		}
		if info.Size() > mirror.maxFileBytes {
			warnings[relative] = fmt.Sprintf("skipped: %d bytes exceeds the review limit", info.Size())
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			warnings[relative] = "skipped: " + err.Error()
			return nil
		}
		files[relative] = content
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("fsmirror: scan mirror: %w", err)
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
	mirror.lastReview = make(map[string]workerkit.Change)
	for _, path := range ordered {
		content, exists := current[path]
		baseline, hadBaseline := mirror.baseline[path]
		if inflight, isInflight := mirror.inflight[path]; isInflight && exists && bytesEqual(content, inflight) {
			// Being delivered right now; it reappears only if it changes again
			// or the delivery fails and the batch is left unsettled.
			continue
		}
		var change workerkit.Change
		switch {
		case exists && !hadBaseline:
			change = workerkit.Change{Path: path, Kind: workerkit.ChangeCreate, Diff: lineDiff(nil, content)}
		case !exists && hadBaseline:
			change = workerkit.Change{Path: path, Kind: workerkit.ChangeDelete, Diff: lineDiff(baseline, nil)}
		case exists && hadBaseline && !bytesEqual(content, baseline):
			change = workerkit.Change{Path: path, Kind: workerkit.ChangeModify, Diff: lineDiff(baseline, content)}
		default:
			continue
		}
		change.ID = changeID(path, content)
		change.Warning = warnings[path]
		changes = append(changes, change)
		mirror.lastReview[change.ID] = change
	}
	for path, warning := range warnings {
		if _, seen := current[path]; !seen {
			if _, hadBaseline := mirror.baseline[path]; !hadBaseline {
				change := workerkit.Change{
					ID: changeID(path, nil), Path: path, Kind: workerkit.ChangeModify, Warning: warning,
				}
				changes = append(changes, change)
				mirror.lastReview[change.ID] = change
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

// Resolve implements workerkit.Mirror: it freezes the current bytes of each
// selected change and builds native tool calls for them.
func (mirror *Mirror) Resolve(ctx context.Context, request workerkit.MirrorResolve) ([]llm.ToolCall, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mirror.mu.Lock()
	if mirror.closed {
		mirror.mu.Unlock()
		return nil, ErrClosed
	}
	var calls []llm.ToolCall
	frozen := make(map[string][]byte, len(request.ChangeIDs))
	fail := func(err error) ([]llm.ToolCall, error) {
		mirror.mu.Unlock()
		return nil, err
	}
	for _, id := range request.ChangeIDs {
		change, known := mirror.lastReview[id]
		if !known {
			return fail(fmt.Errorf("%w: %s", workerkit.ErrUnknownChange, id))
		}
		if change.Warning != "" {
			return fail(fmt.Errorf("fsmirror: change %s is not deliverable: %s", change.Path, change.Warning))
		}
		var content []byte
		if change.Kind != workerkit.ChangeDelete {
			read, err := os.ReadFile(filepath.Join(mirror.root, filepath.FromSlash(change.Path)))
			if err != nil {
				return fail(fmt.Errorf("fsmirror: read %s: %w", change.Path, err))
			}
			content = read
		}
		built, err := mirror.build(change, content, request)
		if err != nil {
			return fail(fmt.Errorf("fsmirror: build calls for %s: %w", change.Path, err))
		}
		calls = append(calls, built...)
		frozen[change.Path] = content
		mirror.inflightByID[id] = inflightRecord{
			path: change.Path, kind: change.Kind, content: content,
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
				if change, known := mirror.lastReview[id]; known {
					record = inflightRecord{path: change.Path, kind: change.Kind}
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
			absolute := filepath.Join(mirror.root, filepath.FromSlash(record.path))
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

func (mirror *Mirror) loadBaseline() (map[string][]byte, error) {
	if mirror.baselineFile == "" {
		return nil, nil
	}
	payload, err := os.ReadFile(mirror.baselineFile)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fsmirror: read baseline: %w", err)
	}
	var encoded map[string][]byte
	if err := json.Unmarshal(payload, &encoded); err != nil {
		return nil, fmt.Errorf("fsmirror: decode baseline: %w", err)
	}
	return encoded, nil
}

func (mirror *Mirror) persistBaselineLocked() error {
	if mirror.baselineFile == "" {
		return nil
	}
	payload, err := json.Marshal(mirror.baseline)
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
