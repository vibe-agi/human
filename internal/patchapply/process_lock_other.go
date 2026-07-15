//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package patchapply

import (
	"context"
	"errors"
)

func acquireProcessLock(context.Context, string) (func(), error) {
	return nil, errors.New("cross-process caller-worktree locking is unsupported on this platform")
}

func acquireProcessLocks(context.Context, ...string) (func(), error) {
	return nil, errors.New("cross-process caller-worktree locking is unsupported on this platform")
}
