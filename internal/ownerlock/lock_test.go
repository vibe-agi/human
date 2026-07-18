package ownerlock

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/vibe-agi/human/internal/sqlitefile"
)

const (
	ownerLockHelperEnv  = "HUMAN_OWNER_LOCK_HELPER"
	ownerLockHelperPath = "HUMAN_OWNER_LOCK_PATH"
)

func TestAcquireIsExclusiveAndReleases(t *testing.T) {
	location, err := sqlitefile.PreparePrivate(filepath.Join(t.TempDir(), "state.db"), "test database")
	if err != nil {
		t.Fatal(err)
	}
	first, err := Acquire(location, "test database")
	if err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(location, "test database")
	if second != nil {
		_ = second.Close()
		t.Fatal("second owner acquired the same database")
	}
	if !errors.Is(err, ErrInUse) {
		t.Fatalf("second owner error = %v, want ErrInUse", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Acquire(location, "test database")
	if err != nil {
		t.Fatalf("acquire after Close: %v", err)
	}
	defer reopened.Close()

	if runtime.GOOS != "windows" {
		lockPath, fileBacked, err := Path(location)
		if err != nil || !fileBacked {
			t.Fatalf("lock path = %q, %v, %v", lockPath, fileBacked, err)
		}
		info, err := os.Stat(lockPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("owner lock mode = %04o, want 0600", info.Mode().Perm())
		}
	}
}

func TestAcquireIsExclusiveAcrossProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	location, err := sqlitefile.PreparePrivate(path, "test database")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := Acquire(location, "test database")
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestOwnerLockHelperProcess$")
	command.Env = append(os.Environ(), ownerLockHelperEnv+"=1", ownerLockHelperPath+"="+path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("cross-process lock helper: %v\n%s", err, output)
	}
}

func TestOwnerLockHelperProcess(t *testing.T) {
	if os.Getenv(ownerLockHelperEnv) != "1" {
		return
	}
	location, err := sqlitefile.PreparePrivate(os.Getenv(ownerLockHelperPath), "helper database")
	if err != nil {
		t.Fatal(err)
	}
	lock, err := Acquire(location, "helper database")
	if lock != nil {
		_ = lock.Close()
		t.Fatal("helper acquired an already-owned database")
	}
	if !errors.Is(err, ErrInUse) {
		t.Fatalf("helper error = %v, want ErrInUse", err)
	}
}

func TestIndependentMemoryLocationsNeedNoOwnerLock(t *testing.T) {
	for _, dsn := range []string{":memory:", "file:private-owner?mode=memory&cache=private"} {
		location, err := sqlitefile.PreparePrivate(dsn, "memory database")
		if err != nil {
			t.Fatal(err)
		}
		first, err := Acquire(location, "memory database")
		if err != nil || first != nil {
			t.Fatalf("first memory lock = %v, %v", first, err)
		}
		second, err := Acquire(location, "memory database")
		if err != nil || second != nil {
			t.Fatalf("second memory lock = %v, %v", second, err)
		}
	}
}

func TestSharedMemoryLocationIsRejected(t *testing.T) {
	location, err := sqlitefile.PreparePrivate("file:shared-owner?mode=memory&cache=shared", "shared database")
	if err != nil {
		t.Fatal(err)
	}
	if lock, err := Acquire(location, "shared database"); lock != nil || err == nil {
		if lock != nil {
			_ = lock.Close()
		}
		t.Fatalf("shared memory lock = %v, %v", lock, err)
	}
}

func TestAcquireRejectsHardLinkedOwnerFileWithoutChangingTarget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("ordinary Windows users cannot create hard links in all CI environments")
	}
	directory := t.TempDir()
	target := filepath.Join(directory, "unrelated")
	if err := os.WriteFile(target, []byte("do not touch"), 0o644); err != nil {
		t.Fatal(err)
	}
	location, err := sqlitefile.PreparePrivate(filepath.Join(directory, "state.db"), "test database")
	if err != nil {
		t.Fatal(err)
	}
	lockPath, _, err := Path(location)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Link(target, lockPath); err != nil {
		t.Fatal(err)
	}
	lock, err := Acquire(location, "test database")
	if lock != nil {
		_ = lock.Close()
		t.Fatal("acquired an owner lock through a hard link")
	}
	if err == nil {
		t.Fatal("hard-linked owner lock was accepted")
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("unrelated target mode = %04o, want 0644", info.Mode().Perm())
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "do not touch" {
		t.Fatalf("unrelated target content = %q", data)
	}
}

func TestAcquireRejectsSymlinkOwnerFileWithoutChangingTarget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("ordinary Windows users cannot create symlinks in all CI environments")
	}
	directory := t.TempDir()
	target := filepath.Join(directory, "unrelated")
	if err := os.WriteFile(target, []byte("do not touch"), 0o644); err != nil {
		t.Fatal(err)
	}
	location, err := sqlitefile.PreparePrivate(filepath.Join(directory, "state.db"), "test database")
	if err != nil {
		t.Fatal(err)
	}
	lockPath, _, err := Path(location)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, lockPath); err != nil {
		t.Fatal(err)
	}
	lock, err := Acquire(location, "test database")
	if lock != nil {
		_ = lock.Close()
		t.Fatal("acquired an owner lock through a symlink")
	}
	if err == nil {
		t.Fatal("symlink owner lock was accepted")
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("unrelated target mode = %04o, want 0644", info.Mode().Perm())
	}
}
