package callershim

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

const (
	ledgerLockHelperEnv  = "HUMAN_CALLER_LEDGER_LOCK_HELPER"
	ledgerLockHelperPath = "HUMAN_CALLER_LEDGER_LOCK_PATH"
)

func TestFileLedgerRejectsSecondOwnerBeforePendingRecovery(t *testing.T) {
	path := t.TempDir() + string(os.PathSeparator) + "caller-ledger.db"
	first, err := OpenSQLiteLedger(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	key := ExecutionKey{CallerID: "caller-lock", TaskID: "task-lock", ToolCallID: "call-lock"}
	if _, err := first.Begin(context.Background(), key, "digest-lock"); err != nil {
		t.Fatal(err)
	}

	second, err := OpenSQLiteLedger(context.Background(), path)
	if second != nil {
		_ = second.Close()
		t.Fatal("second owner opened the live caller ledger")
	}
	if !errors.Is(err, ErrLedgerInUse) || !strings.Contains(err.Error(), ".owner.lock") {
		t.Fatalf("second owner error = %v, want explicit owner-lock conflict", err)
	}

	// The failed opener must not reach recoverPending and turn a live execution
	// into an indeterminate terminal while its actual owner is still running.
	pending, err := first.Begin(context.Background(), key, "digest-lock")
	if err != nil || !pending.Replay || pending.Execution.Status != "pending" {
		t.Fatalf("live pending after rejected owner = %+v, %v", pending, err)
	}
	lockPath, fileBacked, err := ledgerOwnerLockPath(path)
	if err != nil || !fileBacked {
		t.Fatalf("owner lock path = %q, %v, %v", lockPath, fileBacked, err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(lockPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("owner lock mode = %o, want 0600", info.Mode().Perm())
		}
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteLedger(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen after owner Close: %v", err)
	}
	defer reopened.Close()
	recovered, err := reopened.Begin(context.Background(), key, "digest-lock")
	if err != nil || !recovered.Replay || recovered.Execution.Status != "indeterminate" {
		t.Fatalf("recovered previous-owner pending = %+v, %v", recovered, err)
	}
}

func TestFileLedgerOwnerLockIsCrossProcess(t *testing.T) {
	path := t.TempDir() + string(os.PathSeparator) + "caller-ledger.db"
	owner, err := OpenSQLiteLedger(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestLedgerOwnerLockHelperProcess$")
	command.Env = append(os.Environ(), ledgerLockHelperEnv+"=1", ledgerLockHelperPath+"="+path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("cross-process lock helper: %v\n%s", err, output)
	}
}

func TestLedgerOwnerLockHelperProcess(t *testing.T) {
	if os.Getenv(ledgerLockHelperEnv) != "1" {
		return
	}
	ledger, err := OpenSQLiteLedger(context.Background(), os.Getenv(ledgerLockHelperPath))
	if ledger != nil {
		_ = ledger.Close()
		t.Fatal("helper process acquired an already-owned caller ledger")
	}
	if !errors.Is(err, ErrLedgerInUse) {
		t.Fatalf("helper process open error = %v, want owner-lock conflict", err)
	}
}

func TestIndependentMemoryLedgersDoNotRequireOwnerLock(t *testing.T) {
	for _, dsn := range []string{":memory:", "file:private-ledger?mode=memory&cache=private"} {
		t.Run(dsn, func(t *testing.T) {
			first, err := OpenSQLiteLedger(context.Background(), dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			second, err := OpenSQLiteLedger(context.Background(), dsn)
			if err != nil {
				t.Fatalf("second independent memory ledger: %v", err)
			}
			defer second.Close()
			if first.ownerLock != nil || second.ownerLock != nil {
				t.Fatal("independent memory ledger acquired a file owner lock")
			}
		})
	}
}

func TestSharedMemoryLedgerIsRejected(t *testing.T) {
	ledger, err := OpenSQLiteLedger(
		context.Background(), "file:shared-ledger?mode=memory&cache=shared",
	)
	if ledger != nil {
		_ = ledger.Close()
		t.Fatal("shared in-memory ledger opened without an enforceable owner boundary")
	}
	if err == nil || !strings.Contains(err.Error(), "shared in-memory") {
		t.Fatalf("shared in-memory ledger error = %v", err)
	}
}
