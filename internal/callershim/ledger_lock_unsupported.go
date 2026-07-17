//go:build !windows && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package callershim

import (
	"errors"
	"os"
)

func lockLedgerOwnerFile(*os.File) error {
	return errors.New("file-backed caller ledger owner locking is unsupported on this platform")
}

func unlockLedgerOwnerFile(*os.File) error { return nil }
