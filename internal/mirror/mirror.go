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
	"unicode/utf8"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
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

type baselineEntry struct {
	Fingerprint string `json:"fingerprint"`
	Blob        string `json:"blob"`
}

type baselineState struct {
	Entries map[string]baselineEntry `json:"entries"`
	Results map[string]string        `json:"results,omitempty"`
}

type ReconcileReport struct {
	Confirmed []string
	Failed    map[string]string
}

type Workspace struct {
	mu          sync.Mutex
	reconcileMu sync.Mutex
	dir         string
	stateDir    string
	state       baselineState
	maxFile     int64
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
		state: baselineState{Entries: make(map[string]baselineEntry), Results: make(map[string]string)},
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
	if err := atomicWrite(target, content, 0o600); err != nil {
		return err
	}
	entry, err := workspace.storeBlob(content, fingerprint)
	if err != nil {
		return err
	}
	workspace.state.Entries[relative] = entry
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
	return workspace.save()
}

// ReconcileRequest consumes previously unseen caller tool results from the
// full conversation history. Processed IDs are persisted because harnesses
// resend the entire history on every completion; without this ledger an old
// read result could overwrite newer worker edits.
func (workspace *Workspace) ReconcileRequest(request canonical.Request) (ReconcileReport, error) {
	workspace.reconcileMu.Lock()
	defer workspace.reconcileMu.Unlock()
	uses := make(map[string]canonical.Block)
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Type == canonical.BlockToolUse {
				uses[block.ToolCallID] = block
			}
		}
	}
	report := ReconcileReport{Failed: make(map[string]string)}
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Type != canonical.BlockToolResult {
				continue
			}
			use, known := uses[block.ToolCallID]
			if !known || !strings.HasPrefix(use.ToolName, "human_") {
				continue
			}
			encoded, err := json.Marshal(block)
			if err != nil {
				return report, err
			}
			sum := sha256.Sum256(encoded)
			digest := hex.EncodeToString(sum[:])
			workspace.mu.Lock()
			previous, processed := workspace.state.Results[block.ToolCallID]
			workspace.mu.Unlock()
			if processed {
				if previous != digest {
					return report, fmt.Errorf("tool result %s changed after reconciliation", block.ToolCallID)
				}
				continue
			}
			content, isError, errorMessage, err := decodeToolResponse(block.Output, block.IsError)
			if err != nil {
				report.Failed[block.ToolCallID] = err.Error()
				continue
			}
			if isError {
				if errorMessage == "" {
					errorMessage = "caller tool returned an error"
				}
				report.Failed[block.ToolCallID] = errorMessage
				if err := workspace.markResult(block.ToolCallID, digest); err != nil {
					return report, err
				}
				continue
			}
			if err := workspace.applyToolResult(use, content); err != nil {
				report.Failed[block.ToolCallID] = err.Error()
				continue
			}
			if err := workspace.markResult(block.ToolCallID, digest); err != nil {
				return report, err
			}
			report.Confirmed = append(report.Confirmed, block.ToolCallID)
		}
	}
	return report, nil
}

func (workspace *Workspace) applyToolResult(use canonical.Block, content map[string]any) error {
	path := stringValue(content["path"])
	if path == "" {
		path = stringValue(use.Input["path"])
	}
	switch use.ToolName {
	case "human_read_file":
		fingerprint := stringValue(content["sha256"])
		encoding := stringValue(content["encoding"])
		data, err := decodeMirrorContent(stringValue(content["content"]), encoding)
		if err != nil {
			return err
		}
		return workspace.Hydrate(path, data, fingerprint)
	case "human_write_file", "human_edit_file":
		return workspace.Confirm(path, stringValue(content["sha256"]), false)
	case "human_delete_file":
		return workspace.Confirm(path, "", true)
	case "human_rename_file":
		from := stringValue(content["from"])
		to := stringValue(content["to"])
		if from == "" {
			from = stringValue(use.Input["from"])
		}
		if to == "" {
			to = stringValue(use.Input["to"])
		}
		if err := workspace.Confirm(from, "", true); err != nil {
			return err
		}
		return workspace.Confirm(to, stringValue(content["sha256"]), false)
	case "human_search", "human_exec":
		return nil
	default:
		return fmt.Errorf("unsupported human shim result %q", use.ToolName)
	}
}

func (workspace *Workspace) markResult(id, digest string) error {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	workspace.state.Results[id] = digest
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
	err := filepath.WalkDir(workspace.dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
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
			return fmt.Errorf("%w: symlink %s", ErrMirrorEscape, relative)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > workspace.maxFile {
			return fmt.Errorf("mirror file %s exceeds maximum size", relative)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		current[relative] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	var changes []Change
	for path, entry := range workspace.state.Entries {
		content, exists := current[path]
		if !exists {
			changes = append(changes, changeWithWarning(Change{
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
		changes = append(changes, changeWithWarning(Change{
			Kind: ChangeEdit, Path: path, OldContent: oldContent,
			NewContent: content, ExpectedSHA: entry.Fingerprint,
		}))
		delete(current, path)
	}
	for path, content := range current {
		changes = append(changes, changeWithWarning(Change{
			Kind: ChangeWrite, Path: path, NewContent: content,
			ExpectedSHA: callerfs.AbsentFingerprint,
		}))
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	return changes, nil
}

func BuildToolCalls(changes []Change) ([]completion.ToolCall, error) {
	calls := make([]completion.ToolCall, 0, len(changes))
	for _, change := range changes {
		if change.Warning == safety.SeverityBlock {
			return nil, fmt.Errorf("blocked mirror change %s: %s", change.Path, strings.Join(change.Reasons, "; "))
		}
		id, err := canonical.NewOpaqueID("tool_")
		if err != nil {
			return nil, err
		}
		virtualPath := "/workspace/" + filepath.ToSlash(change.Path)
		call := completion.ToolCall{ID: id}
		switch change.Kind {
		case ChangeEdit:
			if utf8.Valid(change.OldContent) && utf8.Valid(change.NewContent) {
				call.Name = "human_edit_file"
				call.Input = map[string]any{
					"path": virtualPath, "old_string": string(change.OldContent),
					"new_string": string(change.NewContent), "expected_sha256": change.ExpectedSHA,
				}
			} else {
				call.Name = "human_write_file"
				call.Input = map[string]any{
					"path": virtualPath, "content": base64.StdEncoding.EncodeToString(change.NewContent),
					"encoding": "base64", "expected_sha256": change.ExpectedSHA,
				}
			}
		case ChangeWrite:
			call.Name = "human_write_file"
			encoding := "utf-8"
			content := string(change.NewContent)
			if !utf8.Valid(change.NewContent) {
				encoding = "base64"
				content = base64.StdEncoding.EncodeToString(change.NewContent)
			}
			call.Input = map[string]any{
				"path": virtualPath, "content": content, "encoding": encoding,
				"expected_sha256": callerfs.AbsentFingerprint,
			}
		case ChangeDelete:
			call.Name = "human_delete_file"
			call.Input = map[string]any{"path": virtualPath, "expected_sha256": change.ExpectedSHA}
		default:
			return nil, fmt.Errorf("unknown mirror change kind %q", change.Kind)
		}
		calls = append(calls, call)
	}
	return calls, nil
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

func changeWithWarning(change Change) Change {
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
	return change
}
