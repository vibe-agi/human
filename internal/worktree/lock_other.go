//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package worktree

import (
	"errors"
	"os"
)

func lockFileNonblocking(_ *os.File) (bool, error) {
	return false, errors.New("worktree process locking is unsupported on this platform")
}

func unlockFile(_ *os.File) error {
	return nil
}
