package callershim

import (
	"errors"
	"fmt"

	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
)

type ledgerOwnerLock = ownerlock.Lock

func acquireLedgerOwnerLock(location sqlitefile.Location) (*ledgerOwnerLock, error) {
	lock, err := ownerlock.Acquire(location, "caller ledger")
	if errors.Is(err, ownerlock.ErrInUse) {
		return nil, fmt.Errorf("%w: %v", ErrLedgerInUse, err)
	}
	return lock, err
}

func ledgerOwnerLockPath(location sqlitefile.Location) (string, bool, error) {
	return ownerlock.Path(location)
}
