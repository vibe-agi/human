package callershim

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/vibe-agi/human/internal/sqlitefile"
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

func acquireLedgerOwnerLock(location sqlitefile.Location) (*ledgerOwnerLock, error) {
	lockPath, fileBacked, err := ledgerOwnerLockPath(location)
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
	} else if !info.Mode().IsRegular() ||
		(runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0) {
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

func ledgerOwnerLockPath(location sqlitefile.Location) (string, bool, error) {
	if location.SharedMemory {
		return "", false, errors.New("shared in-memory caller ledgers are unsupported; use an independent :memory: database or a file-backed ledger")
	}
	if !location.FileBacked {
		return "", false, nil
	}
	return location.Path + ".owner.lock", true, nil
}
