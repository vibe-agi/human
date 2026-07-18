//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package ownerlock

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockFile(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return ErrInUse
	}
	return err
}

func unlockFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}

func validatePlatformLockFile(file *os.File) error {
	var info unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &info); err != nil {
		return fmt.Errorf("inspect owner lock descriptor: %w", err)
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG {
		return errors.New("owner lock descriptor is not a regular file")
	}
	if info.Nlink != 1 {
		return fmt.Errorf("owner lock has %d filesystem links, want 1", info.Nlink)
	}
	return nil
}
