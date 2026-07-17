package mirror

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

const defaultWatchDebounce = 150 * time.Millisecond

// WatchEvent reports that the scratch tree may have changed. Events are
// deliberately coalesced: Review remains the source of truth and computes the
// complete diff against the persisted caller baseline.
type WatchEvent struct {
	Err error
}

// Watch starts a recursive, debounced scratch-tree watcher. The returned
// stream closes with ctx. A newly-created directory is registered before the
// change notification is emitted, while a later Review provides a full-scan
// fallback for editor rename patterns and coalesced filesystem events.
func (workspace *Workspace) Watch(ctx context.Context, debounce time.Duration) (<-chan WatchEvent, error) {
	if ctx == nil {
		return nil, errors.New("mirror watch context is required")
	}
	if debounce <= 0 {
		debounce = defaultWatchDebounce
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := addWatchTree(watcher, workspace.dir); err != nil {
		watcher.Close()
		return nil, err
	}
	events := make(chan WatchEvent, 1)
	go runWatch(ctx, watcher, workspace.dir, debounce, events)
	return events, nil
}

func runWatch(
	ctx context.Context,
	watcher *fsnotify.Watcher,
	root string,
	debounce time.Duration,
	destination chan<- WatchEvent,
) {
	defer watcher.Close()
	defer close(destination)
	var timer *time.Timer
	var timerChannel <-chan time.Time
	reset := func() {
		if timer == nil {
			timer = time.NewTimer(debounce)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(debounce)
		}
		timerChannel = timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case event, open := <-watcher.Events:
			if !open {
				return
			}
			if !watchPathAllowed(root, event.Name) {
				continue
			}
			if event.Op&fsnotify.Create != 0 {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					_ = addWatchTree(watcher, event.Name)
				}
			}
			if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) != 0 {
				reset()
			}
		case err, open := <-watcher.Errors:
			if !open {
				return
			}
			select {
			case destination <- WatchEvent{Err: err}:
			default:
			}
		case <-timerChannel:
			timerChannel = nil
			select {
			case destination <- WatchEvent{}:
			default:
			}
		}
	}
}

func addWatchTree(watcher *fsnotify.Watcher, root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		if path != root && strings.EqualFold(entry.Name(), ".git") {
			return fs.SkipDir
		}
		return watcher.Add(path)
	})
}

func watchPathAllowed(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return false
	}
	for _, part := range strings.Split(filepath.ToSlash(relative), "/") {
		if strings.EqualFold(part, ".git") {
			return false
		}
	}
	return true
}
