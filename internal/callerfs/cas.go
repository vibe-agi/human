// Package callerfs enforces the caller-side filesystem boundary and CAS edits.
package callerfs

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"golang.org/x/text/unicode/norm"
)

const AbsentFingerprint = "absent"

var (
	ErrOutsideRoot        = errors.New("path resolves outside caller workspace root")
	ErrPreconditionFailed = errors.New("file content precondition failed")
	ErrGitWriteForbidden  = errors.New("writes under .git are forbidden")
	ErrEditMatch          = errors.New("edit old content must match exactly once")
	ErrDestinationExists  = errors.New("rename destination already exists")
	ErrFileTooLarge       = errors.New("file exceeds configured read limit")
)

type SearchMatch struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	Text string `json:"text"`
}

type SearchSkip struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type SearchReport struct {
	Matches []SearchMatch `json:"matches"`
	Skipped []SearchSkip  `json:"skipped,omitempty"`
}

type Root struct {
	real string
}

// mutationLocks serializes callerfs mutations by their resolved real paths.
// The table is process-wide so separately constructed Roots for the same
// workspace still coordinate. Entries are reference counted and removed once
// the last holder or waiter releases them.
var mutationLocks pathLockTable

type pathLockTable struct {
	mu      sync.Mutex
	entries map[string]*pathLock
}

type pathLock struct {
	mu   sync.Mutex
	refs int
}

func (table *pathLockTable) lock(paths ...string) func() {
	normalized := make([]string, 0, len(paths))
	for _, path := range paths {
		path = canonicalLockKey(path)
		if path != "" {
			normalized = append(normalized, path)
		}
	}
	sort.Strings(normalized)
	unique := normalized[:0]
	for _, path := range normalized {
		if len(unique) == 0 || unique[len(unique)-1] != path {
			unique = append(unique, path)
		}
	}

	table.mu.Lock()
	if table.entries == nil {
		table.entries = make(map[string]*pathLock)
	}
	locks := make([]*pathLock, len(unique))
	for index, path := range unique {
		entry := table.entries[path]
		if entry == nil {
			entry = &pathLock{}
			table.entries[path] = entry
		}
		entry.refs++
		locks[index] = entry
	}
	table.mu.Unlock()

	// Every caller acquires multi-path operations in the same path order, so a
	// pair of opposing renames cannot deadlock.
	for _, entry := range locks {
		entry.mu.Lock()
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			for index := len(locks) - 1; index >= 0; index-- {
				locks[index].mu.Unlock()
			}
			table.mu.Lock()
			defer table.mu.Unlock()
			for index, path := range unique {
				entry := locks[index]
				entry.refs--
				if entry.refs == 0 {
					delete(table.entries, path)
				}
			}
		})
	}
}

// canonicalLockKey intentionally over-serializes case and normalization
// variants. They may be distinct files on a case-sensitive filesystem, but on
// the default macOS/Windows filesystems they can name the same inode; treating
// them as one lock is the conservative correctness choice on every platform.
func canonicalLockKey(path string) string {
	return norm.NFC.String(strings.ToLower(filepath.Clean(path)))
}

func OpenRoot(root string) (*Root, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("make workspace root absolute: %w", err)
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace root: %w", err)
	}
	info, err := os.Stat(real)
	if err != nil {
		return nil, fmt.Errorf("stat workspace root: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("workspace root is not a directory")
	}
	return &Root{real: real}, nil
}

func (root *Root) Path() string {
	return root.real
}

func (root *Root) ResolveDirectory(relative string) (string, error) {
	if clean := strings.TrimSpace(relative); clean == "" || clean == "." || clean == "/workspace" {
		return root.real, nil
	}
	resolved, err := root.resolve(relative, false)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("caller cwd is not a directory")
	}
	return resolved, nil
}

// ReadFileLimited reads at most maxBytes from a caller file. Callers must pass
// their actual response budget so an oversized immutable tool result cannot
// poison the next completion request.
func (root *Root) ReadFileLimited(relative string, maxBytes int64) ([]byte, string, error) {
	if maxBytes <= 0 {
		return nil, "", errors.New("caller file read limit must be positive")
	}
	resolved, err := root.resolve(relative, false)
	if err != nil {
		return nil, "", err
	}
	file, err := os.Open(resolved)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	if info, statErr := file.Stat(); statErr != nil {
		return nil, "", statErr
	} else if info.Size() > maxBytes {
		return nil, "", fmt.Errorf("%w: %s is %d bytes (limit %d)",
			ErrFileTooLarge, filepath.ToSlash(relative), info.Size(), maxBytes)
	}
	reader := io.LimitReader(file, maxBytes+1)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, "", err
	}
	if maxBytes > 0 && int64(len(data)) > maxBytes {
		return nil, "", fmt.Errorf("%w: %s grew beyond limit %d while reading",
			ErrFileTooLarge, filepath.ToSlash(relative), maxBytes)
	}
	return data, Fingerprint(data), nil
}

func (root *Root) WriteFileCAS(relative, expected string, content []byte, mode fs.FileMode) (string, error) {
	resolved, unlock, err := root.lockResolvedPath(relative, true)
	if err != nil {
		return "", err
	}
	defer unlock()
	return root.writeFileCASLocked(relative, resolved, expected, content, mode)
}

func (root *Root) writeFileCASLocked(relative, resolved, expected string, content []byte, mode fs.FileMode) (string, error) {
	if err := verifyExpected(resolved, expected, true); err != nil {
		return "", err
	}
	if mode == 0 {
		mode = 0o600
	}
	parent := filepath.Dir(resolved)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", fmt.Errorf("create caller file parent: %w", err)
	}
	// Resolve the parent again after creation to catch a symlink swap before
	// placing the temporary file. Platform-specific openat hardening remains a
	// later defense for hostile local races.
	verified, err := root.verifyLockedPath(relative, resolved, true)
	if err != nil {
		return "", err
	}
	resolved = verified
	temporary, err := os.CreateTemp(filepath.Dir(resolved), ".human-cas-*")
	if err != nil {
		return "", fmt.Errorf("create caller CAS temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(mode.Perm()); err != nil {
		temporary.Close()
		return "", fmt.Errorf("chmod caller CAS temporary file: %w", err)
	}
	if _, err := temporary.Write(content); err != nil {
		temporary.Close()
		return "", fmt.Errorf("write caller CAS temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return "", fmt.Errorf("sync caller CAS temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close caller CAS temporary file: %w", err)
	}
	// Recheck immediately before replacement. The path lock makes this
	// linearizable against other callerfs mutations in this process. An
	// arbitrary external editor can still race the following Rename; callerfs
	// deliberately does not claim a cross-process locking protocol.
	verified, err = root.verifyLockedPath(relative, resolved, true)
	if err != nil {
		return "", err
	}
	if err := verifyExpected(verified, expected, true); err != nil {
		return "", err
	}
	if err := os.Rename(temporaryName, resolved); err != nil {
		return "", fmt.Errorf("replace caller file: %w", err)
	}
	return Fingerprint(content), nil
}

func (root *Root) EditFileCAS(relative, expected string, oldContent, newContent []byte) (string, error) {
	resolved, unlock, err := root.lockResolvedPath(relative, true)
	if err != nil {
		return "", err
	}
	defer unlock()
	current, err := os.ReadFile(resolved)
	if errors.Is(err, fs.ErrNotExist) {
		return "", ErrPreconditionFailed
	}
	if err != nil {
		return "", err
	}
	if Fingerprint(current) != expected {
		return "", ErrPreconditionFailed
	}
	if len(oldContent) == 0 || bytes.Count(current, oldContent) != 1 {
		return "", ErrEditMatch
	}
	updated := bytes.Replace(current, oldContent, newContent, 1)
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	return root.writeFileCASLocked(relative, resolved, expected, updated, info.Mode())
}

func (root *Root) DeleteFileCAS(relative, expected string) error {
	resolved, unlock, err := root.lockResolvedPath(relative, true)
	if err != nil {
		return err
	}
	defer unlock()
	if err := verifyExpected(resolved, expected, false); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(resolved), ".human-delete-*")
	if err != nil {
		return fmt.Errorf("reserve caller delete quarantine: %w", err)
	}
	temporaryName := temporary.Name()
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Remove(temporaryName); err != nil {
		return err
	}
	verified, err := root.verifyLockedPath(relative, resolved, true)
	if err != nil {
		return err
	}
	if err := verifyExpected(verified, expected, false); err != nil {
		return err
	}
	if err := os.Rename(resolved, temporaryName); err != nil {
		return fmt.Errorf("quarantine caller file: %w", err)
	}
	restore := true
	defer func() {
		if restore {
			_ = os.Rename(temporaryName, resolved)
		}
	}()
	quarantined, err := os.ReadFile(temporaryName)
	if err != nil {
		return err
	}
	if Fingerprint(quarantined) != expected {
		return ErrPreconditionFailed
	}
	if err := os.Remove(temporaryName); err != nil {
		return fmt.Errorf("delete quarantined caller file: %w", err)
	}
	restore = false
	return nil
}

func (root *Root) RenameFileCAS(from, to, expected string) error {
	source, destination, unlock, err := root.lockResolvedPair(from, to)
	if err != nil {
		return err
	}
	defer unlock()
	if err := verifyExpected(source, expected, false); err != nil {
		return err
	}
	if err := verifyDestinationAbsent(destination); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	verifiedSource, err := root.verifyLockedPath(from, source, true)
	if err != nil {
		return err
	}
	verifiedDestination, err := root.verifyLockedPath(to, destination, true)
	if err != nil {
		return err
	}
	// Recheck both sides immediately before the rename. Stable two-path locking
	// prevents another callerfs mutation from changing either side here.
	if err := verifyExpected(verifiedSource, expected, false); err != nil {
		return err
	}
	if err := verifyDestinationAbsent(verifiedDestination); err != nil {
		return err
	}
	if err := os.Rename(verifiedSource, verifiedDestination); err != nil {
		return fmt.Errorf("rename caller file: %w", err)
	}
	moved, err := os.ReadFile(verifiedDestination)
	if err != nil || Fingerprint(moved) != expected {
		_ = os.Rename(verifiedDestination, verifiedSource)
		if err != nil {
			return err
		}
		return ErrPreconditionFailed
	}
	return nil
}

// SearchDetailed is a best-effort workspace search with explicit per-file
// diagnostics so protocol layers can disclose why generated/binary files were
// skipped.
func (root *Root) SearchDetailed(relative, query string, maxResults int) (SearchReport, error) {
	if strings.TrimSpace(query) == "" {
		return SearchReport{}, errors.New("search query is required")
	}
	if maxResults <= 0 {
		maxResults = 100
	}
	start := root.real
	if clean := strings.TrimSpace(relative); clean != "" && clean != "." && clean != "/workspace" {
		var err error
		start, err = root.resolve(clean, false)
		if err != nil {
			return SearchReport{}, err
		}
	}
	report := SearchReport{}
	walkErr := filepath.WalkDir(start, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if len(report.Matches) >= maxResults {
			return fs.SkipAll
		}
		if entry.IsDir() {
			if entry.Name() == ".git" && path != start {
				return fs.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		relativePath, err := filepath.Rel(root.real, path)
		if err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			report.Skipped = append(report.Skipped, SearchSkip{
				Path: filepath.ToSlash(relativePath), Reason: err.Error(),
			})
			return nil
		}
		scanner := bufio.NewScanner(file)
		buffer := make([]byte, 64*1024)
		scanner.Buffer(buffer, 4<<20)
		line := 0
		for scanner.Scan() {
			line++
			if strings.Contains(scanner.Text(), query) {
				report.Matches = append(report.Matches, SearchMatch{Path: filepath.ToSlash(relativePath), Line: line, Text: scanner.Text()})
				if len(report.Matches) >= maxResults {
					break
				}
			}
		}
		scanErr := scanner.Err()
		_ = file.Close()
		if scanErr != nil {
			// Search is best-effort across a heterogeneous workspace. A
			// generated bundle with a line beyond Scanner's bounded token size,
			// a transient read error, or an unreadable binary must not suppress
			// matches from every other file.
			report.Skipped = append(report.Skipped, SearchSkip{
				Path: filepath.ToSlash(relativePath), Reason: scanErr.Error(),
			})
			return nil
		}
		return nil
	})
	if walkErr != nil {
		return report, walkErr
	}
	return report, nil
}

func Fingerprint(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func verifyExpected(path, expected string, allowAbsent bool) error {
	content, err := os.ReadFile(path)
	switch {
	case err == nil:
		if Fingerprint(content) != expected {
			return ErrPreconditionFailed
		}
		return nil
	case errors.Is(err, fs.ErrNotExist):
		if allowAbsent && expected == AbsentFingerprint {
			return nil
		}
		return ErrPreconditionFailed
	default:
		return fmt.Errorf("read current caller file: %w", err)
	}
}

func verifyDestinationAbsent(path string) error {
	if _, err := os.Lstat(path); err == nil {
		return ErrDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (root *Root) lockResolvedPath(relative string, forWrite bool) (string, func(), error) {
	for {
		resolved, err := root.resolve(relative, forWrite)
		if err != nil {
			return "", nil, err
		}
		unlock := mutationLocks.lock(resolved)
		verified, err := root.resolve(relative, forWrite)
		if err != nil {
			unlock()
			return "", nil, err
		}
		if filepath.Clean(verified) == filepath.Clean(resolved) {
			return verified, unlock, nil
		}
		// The resolved real path changed while this caller waited. Retry with the
		// new identity rather than mutating under the wrong path lock.
		unlock()
	}
}

func (root *Root) lockResolvedPair(from, to string) (string, string, func(), error) {
	for {
		source, err := root.resolve(from, true)
		if err != nil {
			return "", "", nil, err
		}
		destination, err := root.resolve(to, true)
		if err != nil {
			return "", "", nil, err
		}
		unlock := mutationLocks.lock(source, destination)
		verifiedSource, sourceErr := root.resolve(from, true)
		verifiedDestination, destinationErr := root.resolve(to, true)
		if sourceErr != nil || destinationErr != nil {
			unlock()
			if sourceErr != nil {
				return "", "", nil, sourceErr
			}
			return "", "", nil, destinationErr
		}
		if filepath.Clean(verifiedSource) == filepath.Clean(source) && filepath.Clean(verifiedDestination) == filepath.Clean(destination) {
			return verifiedSource, verifiedDestination, unlock, nil
		}
		unlock()
	}
}

func (root *Root) verifyLockedPath(relative, locked string, forWrite bool) (string, error) {
	resolved, err := root.resolve(relative, forWrite)
	if err != nil {
		return "", err
	}
	if filepath.Clean(resolved) != filepath.Clean(locked) {
		return "", ErrPreconditionFailed
	}
	return resolved, nil
}

func (root *Root) resolve(relative string, forWrite bool) (string, error) {
	if filepath.IsAbs(relative) {
		return "", ErrOutsideRoot
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == "" || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", ErrOutsideRoot
	}
	segments := strings.Split(strings.ToLower(filepath.ToSlash(clean)), "/")
	if forWrite {
		for _, segment := range segments {
			if segment == ".git" {
				return "", ErrGitWriteForbidden
			}
		}
	}
	candidate := filepath.Join(root.real, clean)
	real, err := filepath.EvalSymlinks(candidate)
	if errors.Is(err, fs.ErrNotExist) {
		parent, parentErr := filepath.EvalSymlinks(filepath.Dir(candidate))
		if parentErr != nil {
			// Walk upward to the nearest existing parent, then append the still
			// missing suffix without trusting any symlink in the existing prefix.
			parent, parentErr = resolveNearestParent(filepath.Dir(candidate))
			if parentErr != nil {
				return "", parentErr
			}
		}
		real = filepath.Join(parent, filepath.Base(candidate))
	} else if err != nil {
		return "", fmt.Errorf("resolve caller path: %w", err)
	}
	if !within(root.real, real) {
		return "", ErrOutsideRoot
	}
	// Enforce the repository-metadata boundary on the resolved identity too.
	// A purely lexical check is insufficient because an otherwise innocent
	// workspace path may be an internal symlink to .git (for example
	// "git-alias/hooks/pre-commit"). Such a path still resolves inside the
	// workspace, but mutating it can install executable hooks or corrupt the
	// repository. Keep the lexical check above for early rejection and repeat
	// it here on the canonical path for symlink aliases.
	if forWrite && isGitMetadataPath(root.real, real) {
		return "", ErrGitWriteForbidden
	}
	return real, nil
}

func isGitMetadataPath(root, resolved string) bool {
	relative, err := filepath.Rel(root, resolved)
	if err != nil {
		return false
	}
	for _, segment := range strings.Split(strings.ToLower(filepath.ToSlash(filepath.Clean(relative))), "/") {
		if segment == ".git" {
			return true
		}
	}
	return false
}

func resolveNearestParent(candidate string) (string, error) {
	missing := make([]string, 0, 4)
	current := candidate
	for {
		real, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				real = filepath.Join(real, missing[index])
			}
			return real, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("resolve caller parent: %w", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fs.ErrNotExist
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func within(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}
