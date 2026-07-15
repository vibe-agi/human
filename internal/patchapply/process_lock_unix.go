//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package patchapply

import (
	"context"
	"errors"
	"os"
	"slices"
	"time"

	"golang.org/x/sys/unix"
)

// acquireProcessLock serializes caller-worktree and apply-ledger mutation
// across independent human-mcp processes while preserving cancellation.
func acquireProcessLock(ctx context.Context, path string) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
				_ = file.Close()
			}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			_ = file.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func acquireProcessLocks(ctx context.Context, paths ...string) (func(), error) {
	paths = append([]string(nil), paths...)
	slices.Sort(paths)
	paths = slices.Compact(paths)
	releases := make([]func(), 0, len(paths))
	for _, path := range paths {
		release, err := acquireProcessLock(ctx, path)
		if err != nil {
			for index := len(releases) - 1; index >= 0; index-- {
				releases[index]()
			}
			return nil, err
		}
		releases = append(releases, release)
	}
	return func() {
		for index := len(releases) - 1; index >= 0; index-- {
			releases[index]()
		}
	}, nil
}
