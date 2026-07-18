//go:build !windows && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package ownerlock

import (
	"errors"
	"os"
)

func lockFile(*os.File) error {
	return errors.New("file-backed SQLite ownership locking is unsupported on this platform")
}

func unlockFile(*os.File) error { return nil }

func validatePlatformLockFile(*os.File) error { return nil }
