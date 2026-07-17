package callershim

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// ledgerOwnerLock makes the process boundary used by recoverPending real.
// The lock file is deliberately persistent: unlinking it on Close could let a
// third process lock a new inode while a second process still owns the old one.
type ledgerOwnerLock struct {
	file      *os.File
	path      string
	closeOnce sync.Once
	closeErr  error
}

var localLedgerOwners = struct {
	sync.Mutex
	paths map[string]struct{}
}{paths: make(map[string]struct{})}

func acquireLedgerOwnerLock(dsn string) (*ledgerOwnerLock, error) {
	lockPath, fileBacked, err := ledgerOwnerLockPath(dsn)
	if err != nil {
		return nil, err
	}
	if !fileBacked {
		return nil, nil
	}

	localLedgerOwners.Lock()
	if _, exists := localLedgerOwners.paths[lockPath]; exists {
		localLedgerOwners.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrLedgerInUse, lockPath)
	}
	localLedgerOwners.paths[lockPath] = struct{}{}
	localLedgerOwners.Unlock()
	releaseLocal := true
	defer func() {
		if releaseLocal {
			releaseLocalLedgerOwner(lockPath)
		}
	}()

	if info, statErr := os.Lstat(lockPath); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("caller ledger owner lock %s must be a regular file", lockPath)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect caller ledger owner lock %s: %w", lockPath, statErr)
	}
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open caller ledger owner lock %s: %w", lockPath, err)
	}
	closeFile := true
	defer func() {
		if closeFile {
			_ = file.Close()
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return nil, fmt.Errorf("secure caller ledger owner lock %s: %w", lockPath, err)
	}
	if info, err := file.Stat(); err != nil {
		return nil, fmt.Errorf("inspect opened caller ledger owner lock %s: %w", lockPath, err)
	} else if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("caller ledger owner lock %s is not a private regular file", lockPath)
	}
	if err := lockLedgerOwnerFile(file); err != nil {
		if errors.Is(err, ErrLedgerInUse) {
			return nil, fmt.Errorf("%w: %s", ErrLedgerInUse, lockPath)
		}
		return nil, fmt.Errorf("lock caller ledger owner file %s: %w", lockPath, err)
	}

	closeFile = false
	releaseLocal = false
	return &ledgerOwnerLock{file: file, path: lockPath}, nil
}

func (lock *ledgerOwnerLock) Close() error {
	lock.closeOnce.Do(func() {
		unlockErr := unlockLedgerOwnerFile(lock.file)
		closeErr := lock.file.Close()
		releaseLocalLedgerOwner(lock.path)
		lock.closeErr = errors.Join(unlockErr, closeErr)
	})
	return lock.closeErr
}

func releaseLocalLedgerOwner(path string) {
	localLedgerOwners.Lock()
	delete(localLedgerOwners.paths, path)
	localLedgerOwners.Unlock()
}

func ledgerOwnerLockPath(dsn string) (string, bool, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == ":memory:" {
		return "", false, nil
	}

	databasePath := dsn
	if strings.HasPrefix(strings.ToLower(dsn), "file:") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return "", false, fmt.Errorf("parse caller ledger SQLite URI: %w", err)
		}
		query := parsed.Query()
		memoryMode := strings.EqualFold(query.Get("mode"), "memory")
		if strings.EqualFold(query.Get("cache"), "shared") &&
			(memoryMode || parsed.Opaque == ":memory:" || parsed.Path == ":memory:") {
			return "", false, errors.New("shared in-memory caller ledgers are unsupported; use an independent :memory: database or a file-backed ledger")
		}
		if memoryMode || parsed.Opaque == ":memory:" || parsed.Path == ":memory:" {
			return "", false, nil
		}
		if parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
			return "", false, fmt.Errorf("caller ledger SQLite URI has unsupported host %q", parsed.Host)
		}
		switch {
		case parsed.Opaque != "":
			databasePath, err = url.PathUnescape(parsed.Opaque)
			if err != nil {
				return "", false, fmt.Errorf("decode caller ledger SQLite URI path: %w", err)
			}
		case parsed.Path != "":
			databasePath = parsed.Path
		default:
			return "", false, errors.New("file-backed caller ledger SQLite URI requires a path")
		}
		databasePath = filepath.FromSlash(databasePath)
		if runtime.GOOS == "windows" && len(databasePath) >= 3 &&
			databasePath[0] == filepath.Separator && databasePath[2] == ':' {
			databasePath = databasePath[1:]
		}
	}

	absolute, err := filepath.Abs(databasePath)
	if err != nil {
		return "", false, fmt.Errorf("resolve caller ledger path for owner lock: %w", err)
	}
	if resolved, resolveErr := filepath.EvalSymlinks(absolute); resolveErr == nil {
		absolute = resolved
	} else if !errors.Is(resolveErr, os.ErrNotExist) {
		return "", false, fmt.Errorf("resolve caller ledger path for owner lock: %w", resolveErr)
	} else {
		resolvedParent, parentErr := filepath.EvalSymlinks(filepath.Dir(absolute))
		if parentErr != nil {
			return "", false, fmt.Errorf("resolve caller ledger directory for owner lock: %w", parentErr)
		}
		absolute = filepath.Join(resolvedParent, filepath.Base(absolute))
	}
	return absolute + ".owner.lock", true, nil
}
