// Package mirror manages the worker-side scratch copy used by completion
// mode. The caller workspace remains authoritative; every outgoing mutation
// carries the last caller-confirmed content hash.
package mirror

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion/safety"
)

var (
	ErrUnsafeNamespace = errors.New("mirror namespace is not a stable key")
	ErrMirrorEscape    = errors.New("mirror path escapes workspace")
	ErrBaselineDrift   = errors.New("caller result hash does not match mirror content")
)

var stableKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type ChangeKind string

const (
	ChangeWrite  ChangeKind = "write"
	ChangeEdit   ChangeKind = "edit"
	ChangeDelete ChangeKind = "delete"
)

type Change struct {
	Kind        ChangeKind
	Path        string
	OldContent  []byte
	NewContent  []byte
	ExpectedSHA string
	Warning     safety.Severity
	Reasons     []string
}

// ReviewDiagnostic describes a scratch-tree entry which Review deliberately
// did not turn into a caller mutation. Unsupported entries are isolated per
// path so one build artifact cannot block unrelated source edits, but they
// remain visible to the Human instead of disappearing silently.
type ReviewDiagnostic struct {
	Path   string
	Reason string
}

type baselineEntry struct {
	Fingerprint string `json:"fingerprint"`
	Blob        string `json:"blob"`
}

type deliveryIntent struct {
	ProfileKey      string        `json:"profile_key"`
	ToolName        string        `json:"tool_name"`
	Path            string        `json:"path"`
	Kind            ChangeKind    `json:"kind"`
	CallDigest      string        `json:"call_digest"`
	BaseFingerprint string        `json:"base_fingerprint"`
	Delivered       baselineEntry `json:"delivered,omitempty"`
	Deleted         bool          `json:"deleted,omitempty"`
}

type hydrationIntent struct {
	ProfileKey string `json:"profile_key"`
	ToolName   string `json:"tool_name"`
	Path       string `json:"path"`
	CallDigest string `json:"call_digest"`
}

type baselineState struct {
	Entries    map[string]baselineEntry   `json:"entries"`
	Results    map[string]string          `json:"results,omitempty"`
	Warnings   map[string]string          `json:"warnings,omitempty"`
	Deliveries map[string]deliveryIntent  `json:"deliveries,omitempty"`
	Hydrations map[string]hydrationIntent `json:"hydrations,omitempty"`
}

type ReconcileReport struct {
	Confirmed []string
	Failed    map[string]string
	Warnings  []string
}

type Workspace struct {
	mu          sync.Mutex
	reconcileMu sync.Mutex
	dir         string
	stateDir    string
	state       baselineState
	maxFile     int64
	diagnostics []ReviewDiagnostic
}

func Open(root, callerID, workspaceKey string) (*Workspace, error) {
	if !stableKey.MatchString(callerID) || !stableKey.MatchString(workspaceKey) {
		return nil, ErrUnsafeNamespace
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(absolute, callerID, workspaceKey)
	stateDir := filepath.Join(absolute, ".human-state", callerID, workspaceKey)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(stateDir, "blobs"), 0o700); err != nil {
		return nil, err
	}
	workspace := &Workspace{
		dir: dir, stateDir: stateDir, maxFile: 16 << 20,
		state: baselineState{
			Entries: make(map[string]baselineEntry), Results: make(map[string]string),
			Warnings: make(map[string]string), Deliveries: make(map[string]deliveryIntent),
			Hydrations: make(map[string]hydrationIntent),
		},
	}
	if err := workspace.load(); err != nil {
		return nil, err
	}
	return workspace, nil
}

func (workspace *Workspace) Dir() string { return workspace.dir }

func (workspace *Workspace) Hydrate(path string, content []byte, fingerprint string) error {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	relative, target, err := workspace.resolve(path)
	if err != nil {
		return err
	}
	if int64(len(content)) > workspace.maxFile {
		return errors.New("mirror file exceeds maximum size")
	}
	if callerfs.Fingerprint(content) != fingerprint {
		return ErrBaselineDrift
	}
	entry, err := workspace.storeBlob(content, fingerprint)
	if err != nil {
		return err
	}
	current, readErr := os.ReadFile(target)
	currentExists := readErr == nil
	if readErr != nil && !errors.Is(readErr, fs.ErrNotExist) {
		return readErr
	}
	previous, tracked := workspace.state.Entries[relative]
	localDirty := currentExists
	if tracked {
		localDirty = !currentExists || callerfs.Fingerprint(current) != previous.Fingerprint
	} else if currentExists && callerfs.Fingerprint(current) == fingerprint {
		localDirty = false
	}
	if localDirty {
		if tracked && previous.Fingerprint == fingerprint {
			// A repeated read of the same caller baseline must never erase a local
			// draft. The result is still safe to mark reconciled by the caller.
			return nil
		}
		workspace.state.Entries[relative] = entry
		workspace.state.Warnings[relative] =
			"caller content changed while the Human workspace had an unconfirmed draft; merge before sending"
		return workspace.save()
	}
	if err := atomicWrite(target, content, 0o600); err != nil {
		return err
	}
	workspace.state.Entries[relative] = entry
	delete(workspace.state.Warnings, relative)
	return workspace.save()
}

// Confirm applies a successful caller tool result to the baseline. It never
// trusts the result hash alone: the worker mirror must currently hash to the
// same value, otherwise the change remains unconfirmed.
func (workspace *Workspace) Confirm(path, fingerprint string, deleted bool) error {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	relative, target, err := workspace.resolve(path)
	if err != nil {
		return err
	}
	if deleted {
		if _, err := os.Lstat(target); err == nil {
			return ErrBaselineDrift
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		delete(workspace.state.Entries, relative)
		delete(workspace.state.Warnings, relative)
		return workspace.save()
	}
	content, err := os.ReadFile(target)
	if err != nil {
		return err
	}
	if callerfs.Fingerprint(content) != fingerprint {
		return ErrBaselineDrift
	}
	entry, err := workspace.storeBlob(content, fingerprint)
	if err != nil {
		return err
	}
	workspace.state.Entries[relative] = entry
	delete(workspace.state.Warnings, relative)
	return workspace.save()
}

// ConfirmRename reconciles a caller-confirmed rename into both the scratch
// tree and its baseline. A caller-side rename may arrive while the scratch
// tree still has the old path, so treating it as an already-completed local
// delete followed by a write would drift forever. We only remove/move that
// source when it still matches the last confirmed baseline; an unconfirmed
// local edit therefore remains intact and the result fails closed.
func (workspace *Workspace) ConfirmRename(from, to, fingerprint string) error {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()

	fromRelative, fromTarget, err := workspace.resolve(from)
	if err != nil {
		return err
	}
	toRelative, toTarget, err := workspace.resolve(to)
	if err != nil {
		return err
	}
	if fromRelative == toRelative || fingerprint == "" {
		return ErrBaselineDrift
	}

	fromContent, fromErr := os.ReadFile(fromTarget)
	fromExists := fromErr == nil
	if fromErr != nil && !errors.Is(fromErr, fs.ErrNotExist) {
		return fromErr
	}
	if fromExists {
		entry, tracked := workspace.state.Entries[fromRelative]
		if !tracked || callerfs.Fingerprint(fromContent) != entry.Fingerprint ||
			entry.Fingerprint != fingerprint {
			return ErrBaselineDrift
		}
	}

	toContent, toErr := os.ReadFile(toTarget)
	toExists := toErr == nil
	if toErr != nil && !errors.Is(toErr, fs.ErrNotExist) {
		return toErr
	}
	if toExists && callerfs.Fingerprint(toContent) != fingerprint {
		return ErrBaselineDrift
	}
	if !fromExists && !toExists {
		return ErrBaselineDrift
	}
	if fromExists && toExists {
		fromInfo, err := os.Stat(fromTarget)
		if err != nil {
			return err
		}
		toInfo, err := os.Stat(toTarget)
		if err != nil {
			return err
		}
		// On a case-insensitive filesystem two differently spelled paths can
		// resolve to the same directory entry. Hard links have the same shape.
		// Removing the source in either case could also remove the requested
		// destination, so leave both the scratch tree and baseline untouched.
		if os.SameFile(fromInfo, toInfo) {
			return ErrBaselineDrift
		}
	}

	switch {
	case fromExists && !toExists:
		// Publish a complete, separate destination inode with an atomic
		// no-replace operation. If the process stops before removing the source,
		// replay observes two matching files and safely finishes reconciliation.
		if err := atomicWriteExclusive(toTarget, fromContent, 0o600); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return ErrBaselineDrift
			}
			return err
		}
		toContent, err = os.ReadFile(toTarget)
		if err != nil || callerfs.Fingerprint(toContent) != fingerprint {
			if err != nil {
				return err
			}
			return ErrBaselineDrift
		}
		if err := os.Remove(fromTarget); err != nil {
			return err
		}
	case fromExists && toExists:
		// Both paths can legitimately remain after a crash between publishing
		// the destination and removing the source. They are distinct inodes and
		// both hashes are confirmed, so removing only the source is safe.
		if err := os.Remove(fromTarget); err != nil {
			return err
		}
	}

	confirmed, err := os.ReadFile(toTarget)
	if err != nil || callerfs.Fingerprint(confirmed) != fingerprint {
		// The destination may have been changed by an editor after our first
		// verification. Never remove that potentially external content. Restore
		// the confirmed source under a no-replace rule and leave reconciliation
		// pending instead.
		if fromExists {
			_ = atomicWriteExclusive(fromTarget, fromContent, 0o600)
		}
		if err != nil {
			return err
		}
		return ErrBaselineDrift
	}
	entry, err := workspace.storeBlob(confirmed, fingerprint)
	if err != nil {
		return err
	}
	delete(workspace.state.Entries, fromRelative)
	delete(workspace.state.Warnings, fromRelative)
	workspace.state.Entries[toRelative] = entry
	delete(workspace.state.Warnings, toRelative)
	return workspace.save()
}

func (workspace *Workspace) markResult(id, digest string) error {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	workspace.state.Results[id] = digest
	delete(workspace.state.Deliveries, id)
	delete(workspace.state.Hydrations, id)
	return workspace.save()
}

func decodeToolResponse(output any, blockError bool) (map[string]any, bool, string, error) {
	if text, ok := output.(string); ok {
		var decoded any
		if err := json.Unmarshal([]byte(text), &decoded); err != nil {
			return nil, blockError, text, fmt.Errorf("decode human shim result: %w", err)
		}
		output = decoded
	}
	payload, err := json.Marshal(output)
	if err != nil {
		return nil, false, "", err
	}
	var envelope struct {
		Content   any    `json:"content"`
		IsError   bool   `json:"is_error"`
		ErrorCode string `json:"error_code"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, false, "", err
	}
	if envelope.IsError || blockError {
		return nil, true, fmt.Sprint(envelope.Content), nil
	}
	contentPayload, err := json.Marshal(envelope.Content)
	if err != nil {
		return nil, false, "", err
	}
	var content map[string]any
	if err := json.Unmarshal(contentPayload, &content); err != nil {
		return nil, false, "", errors.New("human shim result content is not an object")
	}
	return content, false, "", nil
}

func decodeMirrorContent(content, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "utf-8", "utf8":
		return []byte(content), nil
	case "base64":
		return base64.StdEncoding.DecodeString(content)
	default:
		return nil, fmt.Errorf("unsupported mirror content encoding %q", encoding)
	}
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func (workspace *Workspace) Review() ([]Change, error) {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	current := make(map[string][]byte)
	skipped := make(map[string]string)
	err := filepath.WalkDir(workspace.dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path == workspace.dir {
				return walkErr
			}
			relative, relErr := filepath.Rel(workspace.dir, path)
			if relErr != nil {
				return walkErr
			}
			relative = filepath.ToSlash(relative)
			skipped[relative] = "cannot inspect mirror entry: " + walkErr.Error()
			if entry != nil && entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if path == workspace.dir {
			return nil
		}
		relative, err := filepath.Rel(workspace.dir, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if entry.IsDir() {
			if strings.EqualFold(entry.Name(), ".git") {
				return fs.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			skipped[relative] = "symbolic links are not reviewed or delivered"
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			skipped[relative] = "cannot inspect mirror file: " + err.Error()
			return nil
		}
		if !info.Mode().IsRegular() {
			skipped[relative] = "non-regular files are not reviewed or delivered"
			return nil
		}
		if info.Size() > workspace.maxFile {
			skipped[relative] = fmt.Sprintf(
				"file size %d exceeds the %d-byte mirror review limit", info.Size(), workspace.maxFile,
			)
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			skipped[relative] = "cannot read mirror file: " + err.Error()
			return nil
		}
		if int64(len(data)) > workspace.maxFile {
			// The file may have grown after Info. Apply the same per-file
			// isolation to the bytes actually read.
			skipped[relative] = fmt.Sprintf(
				"file exceeds the %d-byte mirror review limit", workspace.maxFile,
			)
			return nil
		}
		current[relative] = data
		return nil
	})
	if err != nil {
		workspace.diagnostics = nil
		return nil, err
	}
	workspace.diagnostics = workspace.diagnostics[:0]
	for path, reason := range skipped {
		workspace.diagnostics = append(workspace.diagnostics, ReviewDiagnostic{Path: path, Reason: reason})
	}
	sort.Slice(workspace.diagnostics, func(i, j int) bool {
		return workspace.diagnostics[i].Path < workspace.diagnostics[j].Path
	})
	var changes []Change
	for path, entry := range workspace.state.Entries {
		if mirrorPathSkipped(path, skipped) {
			// A tracked file replaced by an unsupported node is not a delete.
			// Keep its confirmed baseline intact until the Human restores a
			// regular, reviewable file or explicitly removes the path.
			continue
		}
		content, exists := current[path]
		if !exists {
			changes = append(changes, workspace.changeWithWarning(Change{
				Kind: ChangeDelete, Path: path, ExpectedSHA: entry.Fingerprint,
			}))
			continue
		}
		if callerfs.Fingerprint(content) == entry.Fingerprint {
			delete(current, path)
			continue
		}
		oldContent, err := os.ReadFile(filepath.Join(workspace.stateDir, "blobs", entry.Blob))
		if err != nil {
			return nil, fmt.Errorf("load mirror baseline %s: %w", path, err)
		}
		if callerfs.Fingerprint(oldContent) != entry.Fingerprint {
			return nil, fmt.Errorf("%w: corrupted baseline for %s", ErrBaselineDrift, path)
		}
		changes = append(changes, workspace.changeWithWarning(Change{
			Kind: ChangeEdit, Path: path, OldContent: oldContent,
			NewContent: content, ExpectedSHA: entry.Fingerprint,
		}))
		delete(current, path)
	}
	for path, content := range current {
		changes = append(changes, workspace.changeWithWarning(Change{
			Kind: ChangeWrite, Path: path, NewContent: content,
			ExpectedSHA: callerfs.AbsentFingerprint,
		}))
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	return changes, nil
}

func mirrorPathSkipped(path string, skipped map[string]string) bool {
	for ignored := range skipped {
		if path == ignored || strings.HasPrefix(path, strings.TrimSuffix(ignored, "/")+"/") {
			return true
		}
	}
	return false
}

// ReviewDiagnostics returns the per-file exclusions from the most recent
// Review. The copy keeps callers from mutating Workspace state.
func (workspace *Workspace) ReviewDiagnostics() []ReviewDiagnostic {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	return append([]ReviewDiagnostic(nil), workspace.diagnostics...)
}

func (workspace *Workspace) resolve(path string) (string, string, error) {
	path = filepath.ToSlash(strings.TrimSpace(path))
	path = strings.TrimPrefix(path, "/workspace/")
	if path == "" || path == "/workspace" || filepath.IsAbs(path) {
		return "", "", ErrMirrorEscape
	}
	clean := filepath.Clean(filepath.FromSlash(path))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", "", ErrMirrorEscape
	}
	target := filepath.Join(workspace.dir, clean)
	relative, err := filepath.Rel(workspace.dir, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", "", ErrMirrorEscape
	}
	return filepath.ToSlash(clean), target, nil
}

func (workspace *Workspace) storeBlob(content []byte, fingerprint string) (baselineEntry, error) {
	name := strings.TrimPrefix(fingerprint, "sha256:")
	if name == fingerprint || len(name) != sha256.Size*2 {
		return baselineEntry{}, errors.New("invalid baseline fingerprint")
	}
	if _, err := hex.DecodeString(name); err != nil {
		return baselineEntry{}, errors.New("invalid baseline fingerprint")
	}
	path := filepath.Join(workspace.stateDir, "blobs", name)
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		if err := atomicWrite(path, content, 0o600); err != nil {
			return baselineEntry{}, err
		}
	} else if err != nil {
		return baselineEntry{}, err
	}
	return baselineEntry{Fingerprint: fingerprint, Blob: name}, nil
}

func (workspace *Workspace) load() error {
	payload, err := os.ReadFile(filepath.Join(workspace.stateDir, "baseline.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, &workspace.state); err != nil {
		return fmt.Errorf("decode mirror baseline: %w", err)
	}
	if workspace.state.Entries == nil {
		workspace.state.Entries = make(map[string]baselineEntry)
	}
	if workspace.state.Results == nil {
		workspace.state.Results = make(map[string]string)
	}
	if workspace.state.Warnings == nil {
		workspace.state.Warnings = make(map[string]string)
	}
	if workspace.state.Deliveries == nil {
		workspace.state.Deliveries = make(map[string]deliveryIntent)
	}
	if workspace.state.Hydrations == nil {
		workspace.state.Hydrations = make(map[string]hydrationIntent)
	}
	return nil
}

func (workspace *Workspace) save() error {
	payload, err := json.MarshalIndent(workspace.state, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(workspace.stateDir, "baseline.json"), append(payload, '\n'), 0o600)
}

func atomicWrite(path string, content []byte, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".human-mirror-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
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
	return os.Rename(name, path)
}

// atomicWriteExclusive publishes complete content without replacing an
// existing destination. The temporary file and destination intentionally have
// a different inode from any source file so crash recovery can distinguish a
// duplicate copy from a case-insensitive path alias.
func atomicWriteExclusive(path string, content []byte, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".human-mirror-rename-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
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
	return os.Link(name, path)
}

func (workspace *Workspace) changeWithWarning(change Change) Change {
	operation := safety.OperationWrite
	if change.Kind == ChangeDelete {
		operation = safety.OperationDelete
	}
	decision, err := safety.CheckVirtualPath("/workspace/"+filepath.ToSlash(change.Path), operation)
	if err != nil {
		change.Warning = safety.SeverityBlock
		change.Reasons = []string{err.Error()}
		return change
	}
	change.Warning = decision.Severity
	change.Reasons = decision.Reasons
	if warning := strings.TrimSpace(workspace.state.Warnings[change.Path]); warning != "" {
		if change.Warning == safety.SeverityAllow {
			change.Warning = safety.SeverityWarn
		}
		change.Reasons = append(change.Reasons, warning)
	}
	return change
}
