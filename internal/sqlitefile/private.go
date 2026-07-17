// Package sqlitefile secures file-backed SQLite databases before a driver
// opens them. It deliberately leaves caller-owned parent directory modes alone.
package sqlitefile

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const privateFileMode = 0o600

// Location is the single canonical interpretation of a SQLite DSN used by
// filesystem permission and ownership-lock boundaries.
type Location struct {
	Path         string
	FileBacked   bool
	Memory       bool
	SharedMemory bool
	openDSN      string
	create       bool
}

// OpenDSN returns the canonical DSN that must be passed to sql.Open. For a
// file-backed database it names Location.Path with an absolute path while
// retaining the original URI query byte-for-byte. In-memory DSNs are returned
// unchanged so named and shared-cache memory semantics are preserved.
func (location Location) OpenDSN() string {
	return location.openDSN
}

// PreparePrivate creates a file-backed SQLite database with mode 0600, or
// tightens an existing regular database to that mode. In-memory DSNs need no
// filesystem preparation. Parent directories must already exist; their modes
// are never changed.
//
// The returned Location is the only value callers should use for sql.Open and
// related owner locks. This keeps the permission check, lock, and actual driver
// open on one canonical absolute path.
func PreparePrivate(dsn, purpose string) (Location, error) {
	location, err := Resolve(dsn)
	if err != nil {
		return Location{}, fmt.Errorf("resolve %s: %w", purpose, err)
	}
	if !location.FileBacked {
		return location, nil
	}
	path := location.Path
	if strings.TrimSpace(purpose) == "" {
		purpose = "SQLite database"
	}

	info, err := os.Lstat(path)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return Location{}, fmt.Errorf("%s %s must be a regular file, not a symlink or special file", purpose, path)
		}
		if err := tightenExisting(path, purpose, info); err != nil {
			return Location{}, err
		}
		return location, nil
	case !errors.Is(err, os.ErrNotExist):
		return Location{}, fmt.Errorf("inspect %s %s: %w", purpose, path, err)
	case !location.create:
		return Location{}, fmt.Errorf("open %s %s: %w", purpose, path, os.ErrNotExist)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, privateFileMode)
	if errors.Is(err, os.ErrExist) {
		// Another opener won the creation race. Re-run all existing-file checks
		// rather than following whatever appeared at the path.
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return Location{}, fmt.Errorf("inspect concurrently created %s %s: %w", purpose, path, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return Location{}, fmt.Errorf("%s %s must be a regular file, not a symlink or special file", purpose, path)
		}
		if err := tightenExisting(path, purpose, info); err != nil {
			return Location{}, err
		}
		return location, nil
	}
	if err != nil {
		return Location{}, fmt.Errorf("create %s %s: %w", purpose, path, err)
	}
	defer file.Close()
	if err := file.Chmod(privateFileMode); err != nil {
		return Location{}, fmt.Errorf("secure %s %s: %w", purpose, path, err)
	}
	if err := verifyPrivateRegular(file, purpose, path); err != nil {
		return Location{}, err
	}
	if err := file.Close(); err != nil {
		return Location{}, fmt.Errorf("close %s %s: %w", purpose, path, err)
	}
	return location, nil
}

func tightenExisting(path, purpose string, expected os.FileInfo) error {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open %s %s to secure permissions: %w", purpose, path, err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect opened %s %s: %w", purpose, path, err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(expected, opened) {
		return fmt.Errorf("%s %s changed while securing permissions", purpose, path)
	}
	if err := file.Chmod(privateFileMode); err != nil {
		return fmt.Errorf("secure %s %s: %w", purpose, path, err)
	}
	return verifyPrivateRegular(file, purpose, path)
}

func verifyPrivateRegular(file *os.File, purpose, path string) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("verify %s %s: %w", purpose, path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s %s is not a regular file", purpose, path)
	}
	if hasMultipleLinks(info) {
		return fmt.Errorf("%s %s must not have multiple hard links", purpose, path)
	}
	// Windows' os.FileMode does not represent an ACL. Chmod still removes the
	// writable bit when requested, but a Unix-style 0600 assertion is meaningless.
	if runtime.GOOS != "windows" && info.Mode().Perm() != privateFileMode {
		return fmt.Errorf("%s %s has mode %04o after securing, want 0600", purpose, path, info.Mode().Perm())
	}
	return nil
}

// Resolve maps a SQLite DSN to one canonical filesystem path. The same result
// must be used by every permission and single-owner boundary so encoded URI,
// relative-path, and symlinked-parent aliases cannot acquire separate locks.
func Resolve(dsn string) (Location, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return Location{}, errors.New("SQLite DSN is required")
	}
	if dsn == ":memory:" {
		return Location{Memory: true, openDSN: dsn}, nil
	}

	path := dsn
	create := true
	var parsedURI *url.URL
	if strings.HasPrefix(strings.ToLower(dsn), "file:") {
		parsed, parseErr := url.Parse(dsn)
		if parseErr != nil {
			return Location{}, fmt.Errorf("parse SQLite URI: %w", parseErr)
		}
		query := parsed.Query()
		mode := strings.ToLower(query.Get("mode"))
		if mode == "memory" || parsed.Opaque == ":memory:" || parsed.Path == ":memory:" {
			return Location{
				Memory:       true,
				SharedMemory: strings.EqualFold(query.Get("cache"), "shared"),
				openDSN:      dsn,
			}, nil
		}
		if parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
			return Location{}, fmt.Errorf("SQLite URI has unsupported host %q", parsed.Host)
		}
		switch mode {
		case "", "rwc":
			create = true
		case "ro", "rw":
			create = false
		default:
			return Location{}, fmt.Errorf("SQLite URI has unsupported mode %q", mode)
		}
		switch {
		case parsed.Opaque != "":
			var err error
			path, err = url.PathUnescape(parsed.Opaque)
			if err != nil {
				return Location{}, fmt.Errorf("decode SQLite URI path: %w", err)
			}
		case parsed.Path != "":
			// net/url already unescapes hierarchical URL paths into Path. A
			// second PathUnescape would corrupt literal percent-encoded names.
			path = parsed.Path
		default:
			return Location{}, errors.New("file-backed SQLite URI requires a path")
		}
		path = filepath.FromSlash(path)
		if runtime.GOOS == "windows" && len(path) >= 3 && path[0] == filepath.Separator && path[2] == ':' {
			path = path[1:]
		}
		parsedURI = parsed
	}

	absolute, err := filepath.Abs(path)
	if err != nil {
		return Location{}, fmt.Errorf("resolve SQLite database path: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return Location{}, fmt.Errorf("resolve SQLite database directory: %w", err)
	}
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return Location{}, fmt.Errorf("inspect resolved SQLite database directory: %w", err)
	}
	if !parentInfo.IsDir() {
		return Location{}, errors.New("resolved SQLite database parent is not a directory")
	}
	if runtime.GOOS != "windows" && parentInfo.Mode().Perm()&0o022 != 0 {
		return Location{}, fmt.Errorf(
			"resolved SQLite database directory %s must not be group- or world-writable (got %04o)",
			parent, parentInfo.Mode().Perm(),
		)
	}
	canonicalPath := filepath.Join(parent, filepath.Base(absolute))
	openDSN := canonicalPath
	if parsedURI != nil {
		openDSN = canonicalFileURI(canonicalPath, parsedURI)
	}
	return Location{
		Path: canonicalPath, FileBacked: true, openDSN: openDSN, create: create,
	}, nil
}

func canonicalFileURI(path string, original *url.URL) string {
	uriPath := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && len(uriPath) >= 2 && uriPath[1] == ':' {
		uriPath = "/" + uriPath
	}
	canonical := &url.URL{
		Scheme:      "file",
		User:        original.User,
		Host:        original.Host,
		Path:        uriPath,
		ForceQuery:  original.ForceQuery,
		RawQuery:    original.RawQuery,
		Fragment:    original.Fragment,
		RawFragment: original.RawFragment,
	}
	return canonical.String()
}
