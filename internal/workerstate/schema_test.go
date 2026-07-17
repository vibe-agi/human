package workerstate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkerStateSchemaInitializesAndReopensCurrentDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := privateWorkerStatePath(t)

	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
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
		FROM worker_state_schema
		WHERE component = 'worker-state'`).Scan(&version, &fingerprint); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if version != workerStateSchemaVersion || fingerprint != workerStateSchemaFingerprint {
		t.Fatalf("schema marker = %d %q, want %d %q",
			version, fingerprint, workerStateSchemaVersion, workerStateSchemaFingerprint)
	}

	store, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen current schema: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerStateSchemaRejectsUnpublishedDatabaseShapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ddl  string
	}{
		{
			name: "unmarked database",
			ddl: `CREATE TABLE worker_state (
				caller_id TEXT NOT NULL,
				session_key TEXT NOT NULL,
				kind TEXT NOT NULL,
				payload BLOB NOT NULL
			);`,
		},
		{
			name: "marker without version",
			ddl: `
				CREATE TABLE worker_state_schema (
				  component TEXT PRIMARY KEY,
				  fingerprint TEXT NOT NULL
				);
				INSERT INTO worker_state_schema (component, fingerprint)
				VALUES ('worker-state', 'human-worker-state-v1-20260717');`,
		},
		{
			name: "wrong version",
			ddl: `
				CREATE TABLE worker_state_schema (
				  component TEXT PRIMARY KEY,
				  version INTEGER NOT NULL,
				  fingerprint TEXT NOT NULL
				);
				INSERT INTO worker_state_schema (component, version, fingerprint)
				VALUES ('worker-state', 2, 'human-worker-state-v1-20260717');`,
		},
		{
			name: "wrong fingerprint",
			ddl: `
				CREATE TABLE worker_state_schema (
				  component TEXT PRIMARY KEY,
				  version INTEGER NOT NULL,
				  fingerprint TEXT NOT NULL
				);
				INSERT INTO worker_state_schema (component, version, fingerprint)
				VALUES ('worker-state', 1, 'different-shape');`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := privateWorkerStatePath(t)
			createWorkerStateDatabase(t, path, test.ddl)

			store, err := Open(context.Background(), path)
			if store != nil {
				_ = store.Close()
				t.Fatal("Open returned a store for an unsupported schema")
			}
			if !errors.Is(err, errUnsupportedWorkerStateSchema) {
				t.Fatalf("Open error = %T %v, want unsupported schema", err, err)
			}
			if !strings.Contains(err.Error(), "recreate database") {
				t.Fatalf("Open error does not explain recovery: %v", err)
			}
		})
	}
}

func privateWorkerStatePath(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, "state.db")
}

func createWorkerStateDatabase(t *testing.T, path, ddl string) {
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
