//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package customstore

import (
	"errors"
	"os"
)

func acquireSnapshotLock(string) (*os.File, error) {
	return nil, errors.New("custom Store: OS ownership locks are unsupported on this platform")
}

func releaseSnapshotLock(*os.File) error { return nil }
