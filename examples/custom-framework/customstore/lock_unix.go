//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package customstore

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func acquireSnapshotLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("custom Store: open ownership lock: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("custom Store: secure ownership lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errors.New("custom Store: snapshot is locked by another process")
		}
		return nil, fmt.Errorf("custom Store: acquire ownership lock: %w", err)
	}
	return file, nil
}

func releaseSnapshotLock(file *os.File) error {
	if file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	if unlockErr != nil {
		unlockErr = fmt.Errorf("custom Store: release ownership lock: %w", unlockErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("custom Store: close ownership lock: %w", closeErr)
	}
	return errors.Join(unlockErr, closeErr)
}
