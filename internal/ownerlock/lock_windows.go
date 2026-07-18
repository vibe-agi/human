//go:build windows

package ownerlock

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func lockFile(file *os.File) error {
	overlapped := new(windows.Overlapped)
	err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, overlapped,
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return ErrInUse
	}
	return err
}

func unlockFile(file *os.File) error {
	overlapped := new(windows.Overlapped)
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, overlapped)
}

func validatePlatformLockFile(file *os.File) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return fmt.Errorf("inspect owner lock descriptor: %w", err)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("owner lock descriptor is a reparse point")
	}
	if info.NumberOfLinks != 1 {
		return fmt.Errorf("owner lock has %d filesystem links, want 1", info.NumberOfLinks)
	}
	return nil
}
