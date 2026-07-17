// Package userdata resolves Human's private, per-user durable state.
//
// Product defaults must never be relative to the process working directory:
// that directory is commonly a customer's Git workspace. Callers may still
// supply explicit paths when they deliberately want a different layout.
package userdata

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const dataHomeEnvironment = "HUMAN_DATA_HOME"

// Root returns the platform user-data directory dedicated to Human. An
// absolute HUMAN_DATA_HOME overrides the platform convention, primarily for
// managed installations and hermetic tests.
func Root() (string, error) {
	if override := strings.TrimSpace(os.Getenv(dataHomeEnvironment)); override != "" {
		if !filepath.IsAbs(override) {
			return "", fmt.Errorf("%s must be an absolute path", dataHomeEnvironment)
		}
		return filepath.Clean(override), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve Human user-data home: %w", err)
	}
	return platformRoot(runtime.GOOS, home, os.Getenv)
}

// Path resolves a private product path below Root. Empty, absolute, dot, and
// parent-traversal elements are rejected so defaults cannot escape the product
// directory accidentally.
func Path(elements ...string) (string, error) {
	root, err := Root()
	if err != nil {
		return "", err
	}
	return joinBelow(root, elements...)
}

// ResolveDirectory returns the real path of an existing directory. It is used
// before hashing workspace identity so symlink aliases share durable state.
func ResolveDirectory(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("workspace directory is required")
	}
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", fmt.Errorf("resolve workspace directory: %w", err)
	}
	real, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve workspace directory symlinks: %w", err)
	}
	info, err := os.Stat(real)
	if err != nil {
		return "", fmt.Errorf("inspect workspace directory: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("workspace path %s is not a directory", real)
	}
	return filepath.Clean(real), nil
}

// ResolveGitWorkspace resolves value to its real path and then walks upward to
// the nearest Git worktree root. A directory without a .git marker is itself a
// valid workspace root. Both .git directories and worktree .git files count.
func ResolveGitWorkspace(value string) (string, error) {
	real, err := ResolveDirectory(value)
	if err != nil {
		return "", err
	}
	for candidate := real; ; candidate = filepath.Dir(candidate) {
		marker := filepath.Join(candidate, ".git")
		info, statErr := os.Lstat(marker)
		switch {
		case statErr == nil:
			if info.IsDir() || info.Mode().IsRegular() {
				return candidate, nil
			}
			return "", fmt.Errorf("Git workspace marker %s must be a regular file or directory", marker)
		case !errors.Is(statErr, os.ErrNotExist):
			return "", fmt.Errorf("inspect Git workspace marker: %w", statErr)
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return real, nil
		}
	}
}

// WorkspacePath returns a private path scoped to the real workspace identity.
// The full SHA-256 digest avoids leaking customer path names into directory
// listings and makes independently selected workspaces collision-resistant.
func WorkspacePath(component, realWorkspace string, elements ...string) (string, error) {
	if strings.TrimSpace(component) == "" {
		return "", errors.New("user-data component is required")
	}
	scope, err := workspaceDigest(realWorkspace)
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(elements)+3)
	parts = append(parts, component, "workspaces", scope)
	parts = append(parts, elements...)
	return Path(parts...)
}

// WorkspaceKey returns a stable, non-reversible correctness key for a real
// workspace path. The caller must first resolve symlinks and select its desired
// workspace root (normally with ResolveGitWorkspace). The digest deliberately
// keeps customer path names out of generated Agent configuration.
func WorkspaceKey(realWorkspace string) (string, error) {
	digest, err := workspaceDigest(realWorkspace)
	if err != nil {
		return "", err
	}
	return "workspace-" + digest, nil
}

func workspaceDigest(realWorkspace string) (string, error) {
	identity, err := canonicalWorkspaceIdentity(realWorkspace)
	if err != nil {
		return "", err
	}
	return digestWorkspaceIdentity(identity), nil
}

func workspaceDigestForOS(goos, realWorkspace string) (string, error) {
	identity, err := normalizeWorkspaceIdentity(realWorkspace, goos == "windows")
	if err != nil {
		return "", err
	}
	return digestWorkspaceIdentity(identity), nil
}

// canonicalWorkspaceIdentity normalizes each existing path component according
// to the filesystem that contains that component. A read-only alternate-case
// lookup proves case-insensitivity before folding; this avoids conflating two
// real directories on case-sensitive APFS volumes while still making the usual
// macOS casing aliases share durable state. Lstat deliberately prevents a
// same-target symlink from being mistaken for case-insensitive lookup.
func canonicalWorkspaceIdentity(realWorkspace string) (string, error) {
	if !filepath.IsAbs(realWorkspace) {
		return "", errors.New("workspace identity path must be absolute")
	}
	clean := filepath.Clean(realWorkspace)
	info, err := os.Lstat(clean)
	if err != nil {
		return "", fmt.Errorf("inspect workspace identity path: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("workspace identity path %s is not a directory", clean)
	}

	volume := filepath.VolumeName(clean)
	root := volume + string(filepath.Separator)
	if volume == "" {
		root = string(filepath.Separator)
	}
	remainder := strings.TrimPrefix(clean, root)
	components := []string(nil)
	if remainder != "" {
		components = strings.Split(remainder, string(filepath.Separator))
	}

	// Drive letters and UNC volume names are outside any directory whose case
	// behavior can be probed. Windows treats that namespace case-insensitively;
	// all ordinary path components below it are still detected individually.
	identityRoot := normalizeWorkspaceComponent(volume, runtime.GOOS == "windows") + string(filepath.Separator)
	if volume == "" {
		identityRoot = string(filepath.Separator)
	}
	identity := identityRoot
	actualParent := root
	for _, component := range components {
		actual := filepath.Join(actualParent, component)
		componentInfo, statErr := os.Lstat(actual)
		if statErr != nil {
			return "", fmt.Errorf("inspect workspace identity component %s: %w", actual, statErr)
		}
		caseInsensitive, detectErr := workspaceComponentCaseInsensitive(actualParent, component, componentInfo)
		if detectErr != nil {
			return "", detectErr
		}
		identity = filepath.Join(identity, normalizeWorkspaceComponent(component, caseInsensitive))
		actualParent = actual
	}
	return filepath.Clean(identity), nil
}

func workspaceComponentCaseInsensitive(parent, component string, original os.FileInfo) (bool, error) {
	alternate, ok := alternateWorkspaceComponentCase(component)
	if !ok {
		return false, nil
	}
	alternatePath := filepath.Join(parent, alternate)
	alternateInfo, err := os.Lstat(alternatePath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("probe workspace filesystem case behavior at %s: %w", alternatePath, err)
	}
	return os.SameFile(original, alternateInfo), nil
}

func alternateWorkspaceComponentCase(value string) (string, bool) {
	for offset, current := range value {
		replacement := unicode.ToUpper(current)
		if replacement == current {
			replacement = unicode.ToLower(current)
		}
		if replacement == current {
			continue
		}
		var result strings.Builder
		result.Grow(len(value) - utf8.RuneLen(current) + utf8.RuneLen(replacement))
		result.WriteString(value[:offset])
		result.WriteRune(replacement)
		result.WriteString(value[offset+utf8.RuneLen(current):])
		return result.String(), true
	}
	return "", false
}

func normalizeWorkspaceIdentity(realWorkspace string, foldCase bool) (string, error) {
	if !filepath.IsAbs(realWorkspace) {
		return "", errors.New("workspace identity path must be absolute")
	}
	return normalizeWorkspaceComponent(filepath.Clean(realWorkspace), foldCase), nil
}

func normalizeWorkspaceComponent(value string, foldCase bool) string {
	value = norm.NFC.String(value)
	if foldCase {
		value = norm.NFC.String(cases.Fold().String(value))
	}
	return value
}

func digestWorkspaceIdentity(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func platformRoot(goos, home string, getenv func(string) string) (string, error) {
	if !filepath.IsAbs(home) {
		return "", errors.New("user home directory must be absolute")
	}
	switch goos {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "Human"), nil
	case "windows":
		if local := strings.TrimSpace(getenv("LOCALAPPDATA")); local != "" && filepath.IsAbs(local) {
			return filepath.Join(local, "Human"), nil
		}
		return filepath.Join(home, "AppData", "Local", "Human"), nil
	default:
		if data := strings.TrimSpace(getenv("XDG_DATA_HOME")); data != "" && filepath.IsAbs(data) {
			return filepath.Join(data, "human"), nil
		}
		return filepath.Join(home, ".local", "share", "human"), nil
	}
}

func joinBelow(root string, elements ...string) (string, error) {
	parts := make([]string, 0, len(elements)+1)
	parts = append(parts, root)
	for _, element := range elements {
		element = strings.TrimSpace(element)
		clean := filepath.Clean(element)
		if element == "" || filepath.IsAbs(element) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("invalid user-data path element %q", element)
		}
		parts = append(parts, clean)
	}
	joined := filepath.Join(parts...)
	relative, err := filepath.Rel(root, joined)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("user-data path escapes product root")
	}
	return joined, nil
}
