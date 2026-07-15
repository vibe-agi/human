// Package callerfs enforces the caller-side filesystem boundary and CAS edits.
package callerfs

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const AbsentFingerprint = "absent"

var (
	ErrOutsideRoot        = errors.New("path resolves outside caller workspace root")
	ErrPreconditionFailed = errors.New("file content precondition failed")
	ErrGitWriteForbidden  = errors.New("writes under .git are forbidden")
	ErrEditMatch          = errors.New("edit old content must match exactly once")
	ErrDestinationExists  = errors.New("rename destination already exists")
)

type SearchMatch struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	Text string `json:"text"`
}

type Root struct {
	real string
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

func (root *Root) ReadFile(relative string) ([]byte, string, error) {
	resolved, err := root.resolve(relative, false)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, "", err
	}
	return data, Fingerprint(data), nil
}

func (root *Root) WriteFileCAS(relative, expected string, content []byte, mode fs.FileMode) (string, error) {
	resolved, err := root.resolve(relative, true)
	if err != nil {
		return "", err
	}
	current, err := os.ReadFile(resolved)
	switch {
	case err == nil:
		if Fingerprint(current) != expected {
			return "", ErrPreconditionFailed
		}
	case errors.Is(err, fs.ErrNotExist):
		if expected != AbsentFingerprint {
			return "", ErrPreconditionFailed
		}
	default:
		return "", fmt.Errorf("read current caller file: %w", err)
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
	verified, err := root.resolve(relative, true)
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
	if err := os.Rename(temporaryName, resolved); err != nil {
		return "", fmt.Errorf("replace caller file: %w", err)
	}
	return Fingerprint(content), nil
}

func (root *Root) EditFileCAS(relative, expected string, oldContent, newContent []byte) (string, error) {
	current, fingerprint, err := root.ReadFile(relative)
	if err != nil {
		return "", err
	}
	if fingerprint != expected {
		return "", ErrPreconditionFailed
	}
	if len(oldContent) == 0 || bytes.Count(current, oldContent) != 1 {
		return "", ErrEditMatch
	}
	updated := bytes.Replace(current, oldContent, newContent, 1)
	resolved, err := root.resolve(relative, false)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	return root.WriteFileCAS(relative, expected, updated, info.Mode())
}

func (root *Root) DeleteFileCAS(relative, expected string) error {
	resolved, err := root.resolve(relative, true)
	if err != nil {
		return err
	}
	current, err := os.ReadFile(resolved)
	if err != nil {
		return err
	}
	if Fingerprint(current) != expected {
		return ErrPreconditionFailed
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
	source, err := root.resolve(from, true)
	if err != nil {
		return err
	}
	destination, err := root.resolve(to, true)
	if err != nil {
		return err
	}
	content, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if Fingerprint(content) != expected {
		return ErrPreconditionFailed
	}
	if _, err := os.Lstat(destination); err == nil {
		return ErrDestinationExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	verified, err := root.resolve(to, true)
	if err != nil {
		return err
	}
	if err := os.Rename(source, verified); err != nil {
		return fmt.Errorf("rename caller file: %w", err)
	}
	moved, err := os.ReadFile(verified)
	if err != nil || Fingerprint(moved) != expected {
		_ = os.Rename(verified, source)
		if err != nil {
			return err
		}
		return ErrPreconditionFailed
	}
	return nil
}

func (root *Root) Search(relative, query string, maxResults int) ([]SearchMatch, error) {
	if strings.TrimSpace(query) == "" {
		return nil, errors.New("search query is required")
	}
	if maxResults <= 0 {
		maxResults = 100
	}
	start := root.real
	if clean := strings.TrimSpace(relative); clean != "" && clean != "." && clean != "/workspace" {
		var err error
		start, err = root.resolve(clean, false)
		if err != nil {
			return nil, err
		}
	}
	var matches []SearchMatch
	walkErr := filepath.WalkDir(start, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if len(matches) >= maxResults {
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
			return nil
		}
		scanner := bufio.NewScanner(file)
		buffer := make([]byte, 64*1024)
		scanner.Buffer(buffer, 4<<20)
		line := 0
		for scanner.Scan() {
			line++
			if strings.Contains(scanner.Text(), query) {
				matches = append(matches, SearchMatch{Path: filepath.ToSlash(relativePath), Line: line, Text: scanner.Text()})
				if len(matches) >= maxResults {
					break
				}
			}
		}
		scanErr := scanner.Err()
		closeErr := file.Close()
		if scanErr != nil {
			return scanErr
		}
		return closeErr
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return matches, nil
}

func Fingerprint(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
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
	return real, nil
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
