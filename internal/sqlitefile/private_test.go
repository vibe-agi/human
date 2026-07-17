package sqlitefile

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestPreparePrivateCreatesAndTightensWithoutChangingParent(t *testing.T) {
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

	created := filepath.Join(parent, "created.db")
	if _, err := PreparePrivate(created, "test database"); err != nil {
		t.Fatal(err)
	}
	assertMode(t, created, 0o600)
	assertMode(t, parent, 0o755)

	existing := filepath.Join(parent, "existing.db")
	if err := os.WriteFile(existing, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(existing, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := PreparePrivate(existing, "test database"); err != nil {
		t.Fatal(err)
	}
	assertMode(t, existing, 0o600)
	assertMode(t, parent, 0o755)
}

func TestPreparePrivateSupportsFileURIAndMemory(t *testing.T) {
	memory, err := PreparePrivate(":memory:", "test database")
	if err != nil {
		t.Fatal(err)
	}
	if memory.OpenDSN() != ":memory:" {
		t.Fatalf("memory open DSN = %q", memory.OpenDSN())
	}
	namedMemoryDSN := "file:private?mode=memory&cache=private"
	namedMemory, err := PreparePrivate(namedMemoryDSN, "test database")
	if err != nil {
		t.Fatal(err)
	}
	if namedMemory.OpenDSN() != namedMemoryDSN {
		t.Fatalf("named memory open DSN = %q, want exact %q", namedMemory.OpenDSN(), namedMemoryDSN)
	}

	parent := t.TempDir()
	path := filepath.Join(parent, "space name.db")
	uri := "file:" + strings.ReplaceAll(filepath.ToSlash(path), " ", "%20") + "?mode=rwc"
	prepared, err := PreparePrivate(uri, "test database")
	if err != nil {
		t.Fatal(err)
	}
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		t.Fatal(err)
	}
	wantPreparedPath := filepath.Join(resolvedParent, filepath.Base(path))
	if prepared.Path != wantPreparedPath {
		t.Fatalf("prepared URI path = %q, want %q", prepared.Path, wantPreparedPath)
	}
	if runtime.GOOS != "windows" {
		assertMode(t, path, 0o600)
	}
	plain, err := Resolve(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Resolve(uri)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Path != encoded.Path || !plain.FileBacked || !encoded.FileBacked {
		t.Fatalf("plain and encoded URI locations = %+v and %+v", plain, encoded)
	}
}

func TestPreparePrivateRejectsSymlinkAndMissingReadOnlyDatabase(t *testing.T) {
	parent := t.TempDir()
	missing := filepath.Join(parent, "missing.db")
	if _, err := PreparePrivate("file:"+filepath.ToSlash(missing)+"?mode=ro", "test database"); err == nil {
		t.Fatal("read-only missing database was created")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("missing database stat = %v, want not exist", err)
	}

	if runtime.GOOS == "windows" {
		t.Skip("symlink creation commonly requires extra Windows privileges")
	}
	target := filepath.Join(parent, "target.db")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link.db")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := PreparePrivate(link, "test database"); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink error = %v", err)
	}
}

func TestPreparePrivateRejectsDatabaseWithMultipleHardLinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("portable Windows FileInfo does not expose hard-link count")
	}
	parent := t.TempDir()
	first := filepath.Join(parent, "first.db")
	second := filepath.Join(parent, "second.db")
	if err := os.WriteFile(first, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(first, second); err != nil {
		t.Fatal(err)
	}
	if _, err := PreparePrivate(first, "test database"); err == nil || !strings.Contains(err.Error(), "hard links") {
		t.Fatalf("hard-linked database error = %v", err)
	}
}

func TestResolveRejectsWritableResolvedParent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows directory permissions are ACL-backed")
	}
	for _, mode := range []os.FileMode{0o720, 0o702, 0o722} {
		t.Run(mode.String(), func(t *testing.T) {
			parent := filepath.Join(t.TempDir(), "writable")
			if err := os.Mkdir(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(parent, mode); err != nil {
				t.Fatal(err)
			}
			_, err := Resolve(filepath.Join(parent, "state.db"))
			if err == nil || !strings.Contains(err.Error(), "group- or world-writable") {
				t.Fatalf("Resolve parent mode %04o error = %v", mode, err)
			}
		})
	}

	writableTarget := filepath.Join(t.TempDir(), "writable-target")
	if err := os.Mkdir(writableTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(writableTarget, 0o777); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "apparently-private-alias")
	if err := os.Symlink(writableTarget, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve(filepath.Join(alias, "state.db")); err == nil ||
		!strings.Contains(err.Error(), "group- or world-writable") {
		t.Fatalf("Resolve writable symlink target error = %v", err)
	}
}

func TestPreparedOpenDSNCanonicalizesPathAndPreservesURIOptions(t *testing.T) {
	realParent := filepath.Join(t.TempDir(), "real parent")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasParent := filepath.Join(t.TempDir(), "alias parent")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink creation unavailable: %v", err)
		}
		t.Fatal(err)
	}
	databasePath := filepath.Join(aliasParent, "option database.db")
	uriPath := filepath.ToSlash(databasePath)
	if runtime.GOOS == "windows" {
		uriPath = "/" + uriPath
	}
	rawQuery := "mode=rwc&_pragma=foreign_keys%281%29&_time_format=sqlite"
	dsn := (&url.URL{Scheme: "file", Path: uriPath, RawQuery: rawQuery}).String()

	location, err := PreparePrivate(dsn, "option database")
	if err != nil {
		t.Fatal(err)
	}
	resolvedRealParent, err := filepath.EvalSymlinks(realParent)
	if err != nil {
		t.Fatal(err)
	}
	wantPath := filepath.Join(resolvedRealParent, "option database.db")
	if location.Path != wantPath {
		t.Fatalf("canonical path = %q, want %q", location.Path, wantPath)
	}
	parsed, err := url.Parse(location.OpenDSN())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.RawQuery != rawQuery {
		t.Fatalf("canonical DSN query = %q, want byte-identical %q", parsed.RawQuery, rawQuery)
	}

	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var foreignKeys int
	if err := database.QueryRowContext(context.Background(), "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatal(err)
	}
	if foreignKeys != 1 {
		t.Fatalf("preserved URI _pragma set foreign_keys = %d, want 1", foreignKeys)
	}
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("canonical database path was not opened: %v", err)
	}
}

func TestPreparedRelativeDSNOpensOriginalAbsolutePathAfterChdir(t *testing.T) {
	firstWorkingDirectory := t.TempDir()
	secondWorkingDirectory := t.TempDir()
	t.Chdir(firstWorkingDirectory)

	location, err := PreparePrivate("relative.db", "relative database")
	if err != nil {
		t.Fatal(err)
	}
	wantPath, err := filepath.EvalSymlinks(firstWorkingDirectory)
	if err != nil {
		t.Fatal(err)
	}
	wantPath = filepath.Join(wantPath, "relative.db")
	if location.Path != wantPath || location.OpenDSN() != wantPath {
		t.Fatalf("relative location = %+v, open DSN %q; want %q", location, location.OpenDSN(), wantPath)
	}

	if err := os.Chdir(secondWorkingDirectory); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(context.Background(), "CREATE TABLE canonical_open (id INTEGER)"); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(secondWorkingDirectory, "relative.db")); !os.IsNotExist(err) {
		t.Fatalf("raw relative DSN was reopened in the new working directory: %v", err)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %04o, want %04o", path, got, want)
	}
}
