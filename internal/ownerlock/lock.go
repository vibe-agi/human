// Package ownerlock provides a process-lifetime, cross-process ownership lock
// for a canonical file-backed SQLite location. The lock file is deliberately
// persistent: unlinking it on Close could let a third process lock a new inode
// while another process still owns the old one.
package ownerlock

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/vibe-agi/human/internal/sqlitefile"
)

var ErrInUse = errors.New("SQLite database is already held by another live owner")

// Lock holds ownership until Close. A nil lock is returned for an independent
// in-memory database, which has no filesystem identity to share.
type Lock struct {
	file      *os.File
	path      string
	closeOnce sync.Once
	closeErr  error
}

var localOwners = struct {
	sync.Mutex
	paths map[string]struct{}
}{paths: make(map[string]struct{})}

// Path returns the persistent lock-file identity for location.
func Path(location sqlitefile.Location) (string, bool, error) {
	if location.SharedMemory {
		return "", false, errors.New("shared in-memory SQLite ownership is unsupported; use an independent :memory: database or a file-backed database")
	}
	if !location.FileBacked {
		return "", false, nil
	}
	return location.Path + ".owner.lock", true, nil
}

// Acquire takes the non-blocking exclusive ownership lock for location.
func Acquire(location sqlitefile.Location, description string) (*Lock, error) {
	lockPath, fileBacked, err := Path(location)
	if err != nil {
		return nil, err
	}
	if !fileBacked {
		return nil, nil
	}
	description = strings.TrimSpace(description)
	if description == "" {
		description = "SQLite database"
	}

	localOwners.Lock()
	if _, exists := localOwners.paths[lockPath]; exists {
		localOwners.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrInUse, lockPath)
	}
	localOwners.paths[lockPath] = struct{}{}
	localOwners.Unlock()
	releaseLocal := true
	defer func() {
		if releaseLocal {
			releaseLocalOwner(lockPath)
		}
	}()

	var expected os.FileInfo
	if info, statErr := os.Lstat(lockPath); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%s owner lock %s must be a regular file", description, lockPath)
		}
		expected = info
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect %s owner lock %s: %w", description, lockPath, statErr)
	}
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open %s owner lock %s: %w", description, lockPath, err)
	}
	closeFile := true
	defer func() {
		if closeFile {
			_ = file.Close()
		}
	}()
	// OpenFile follows a symlink created between the first Lstat and open. Verify
	// the descriptor before chmod: otherwise a raced symlink or a pre-positioned
	// hard link could make this process change an unrelated file's permissions
	// even though acquisition later failed. The platform check also requires one
	// filesystem link (and rejects Windows reparse points).
	if err := validateStableLockFile(file, lockPath, expected, false); err != nil {
		return nil, fmt.Errorf("%s owner lock %s is not a private stable regular file: %w", description, lockPath, err)
	}
	if err := file.Chmod(0o600); err != nil {
		return nil, fmt.Errorf("secure %s owner lock %s: %w", description, lockPath, err)
	}
	if err := validateStableLockFile(file, lockPath, expected, true); err != nil {
		return nil, fmt.Errorf("%s owner lock %s changed while securing it: %w", description, lockPath, err)
	}
	if err := lockFile(file); err != nil {
		if errors.Is(err, ErrInUse) {
			return nil, fmt.Errorf("%w: %s", ErrInUse, lockPath)
		}
		return nil, fmt.Errorf("lock %s owner file %s: %w", description, lockPath, err)
	}
	if err := validateStableLockFile(file, lockPath, expected, true); err != nil {
		_ = unlockFile(file)
		return nil, fmt.Errorf("%s owner lock %s changed while locking it: %w", description, lockPath, err)
	}

	closeFile = false
	releaseLocal = false
	return &Lock{file: file, path: lockPath}, nil
}

func validateStableLockFile(file *os.File, path string, expected os.FileInfo, requirePrivate bool) error {
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	if !opened.Mode().IsRegular() {
		return errors.New("opened descriptor is not a regular file")
	}
	if err := validatePlatformLockFile(file); err != nil {
		return err
	}
	linked, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if linked.Mode()&os.ModeSymlink != 0 || !linked.Mode().IsRegular() || !os.SameFile(linked, opened) {
		return errors.New("directory entry does not identify the opened regular file")
	}
	if expected != nil && !os.SameFile(expected, opened) {
		return errors.New("owner lock was replaced while opening")
	}
	if requirePrivate && runtime.GOOS != "windows" && opened.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("permissions are %04o, want 0600", opened.Mode().Perm())
	}
	return nil
}

// Close releases ownership after the caller has closed the database itself.
func (lock *Lock) Close() error {
	if lock == nil {
		return nil
	}
	lock.closeOnce.Do(func() {
		unlockErr := unlockFile(lock.file)
		closeErr := lock.file.Close()
		releaseLocalOwner(lock.path)
		lock.closeErr = errors.Join(unlockErr, closeErr)
	})
	return lock.closeErr
}

func releaseLocalOwner(path string) {
	localOwners.Lock()
	delete(localOwners.paths, path)
	localOwners.Unlock()
}
