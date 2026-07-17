package callershim

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLedgerSchemaInitializesAndReopensCurrentDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")

	ledger, err := OpenSQLiteLedger(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var version int
	var fingerprint string
	if err := database.QueryRowContext(ctx, `
		SELECT version, fingerprint
		FROM caller_ledger_schema
		WHERE component = 'caller-ledger'`).Scan(&version, &fingerprint); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if version != ledgerSchemaVersion || fingerprint != ledgerSchemaFingerprint {
		t.Fatalf("schema marker = %d %q, want %d %q",
			version, fingerprint, ledgerSchemaVersion, ledgerSchemaFingerprint)
	}

	ledger, err = OpenSQLiteLedger(ctx, path)
	if err != nil {
		t.Fatalf("reopen current schema: %v", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerUsesPrivateFileWithoutChangingCallerDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows file modes are ACL-backed")
	}
	parent := filepath.Join(t.TempDir(), "caller-owned")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "ledger.db")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}

	ledger, err := OpenSQLiteLedger(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	assertCallerLedgerMode(t, path, 0o600)
	assertCallerLedgerMode(t, parent, 0o755)
}

func assertCallerLedgerMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %04o, want %04o", path, got, want)
	}
}

func TestLedgerSchemaRejectsUnpublishedDatabaseShapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ddl  string
	}{
		{
			name: "unmarked database",
			ddl: `CREATE TABLE caller_tool_executions (
				caller_id TEXT NOT NULL,
				task_id TEXT NOT NULL,
				tool_call_id TEXT NOT NULL,
				request_digest TEXT NOT NULL
			);`,
		},
		{
			name: "previous clean-break version",
			ddl: `
				CREATE TABLE caller_ledger_schema (
				  component TEXT PRIMARY KEY,
				  version INTEGER NOT NULL,
				  fingerprint TEXT NOT NULL
				);
				INSERT INTO caller_ledger_schema (component, version, fingerprint)
				VALUES ('caller-ledger', 1, 'human-caller-ledger-v1-20260717');`,
		},
		{
			name: "marker without version",
			ddl: `
				CREATE TABLE caller_ledger_schema (
				  component TEXT PRIMARY KEY,
				  fingerprint TEXT NOT NULL
				);
				INSERT INTO caller_ledger_schema (component, fingerprint)
				VALUES ('caller-ledger', 'human-caller-ledger-v2-20260717');`,
		},
		{
			name: "wrong version",
			ddl: `
				CREATE TABLE caller_ledger_schema (
				  component TEXT PRIMARY KEY,
				  version INTEGER NOT NULL,
				  fingerprint TEXT NOT NULL
				);
				INSERT INTO caller_ledger_schema (component, version, fingerprint)
				VALUES ('caller-ledger', 3, 'human-caller-ledger-v2-20260717');`,
		},
		{
			name: "wrong fingerprint",
			ddl: `
				CREATE TABLE caller_ledger_schema (
				  component TEXT PRIMARY KEY,
				  version INTEGER NOT NULL,
				  fingerprint TEXT NOT NULL
				);
				INSERT INTO caller_ledger_schema (component, version, fingerprint)
				VALUES ('caller-ledger', 2, 'different-shape');`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "ledger.db")
			createLedgerDatabase(t, path, test.ddl)

			ledger, err := OpenSQLiteLedger(context.Background(), path)
			if ledger != nil {
				_ = ledger.Close()
				t.Fatal("OpenSQLiteLedger returned a ledger for an unsupported schema")
			}
			if !errors.Is(err, errUnsupportedLedgerSchema) {
				t.Fatalf("OpenSQLiteLedger error = %T %v, want unsupported schema", err, err)
			}
			if !strings.Contains(err.Error(), "recreate database") {
				t.Fatalf("OpenSQLiteLedger error does not explain recovery: %v", err)
			}
		})
	}
}

func createLedgerDatabase(t *testing.T, path, ddl string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(ddl); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}
