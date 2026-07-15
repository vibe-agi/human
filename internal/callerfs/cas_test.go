package callerfs

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestWriteFileCAS(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	root, err := OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	first, err := root.WriteFileCAS("src/main.go", AbsentFingerprint, []byte("first"), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if first != Fingerprint([]byte("first")) {
		t.Fatalf("fingerprint = %q", first)
	}
	if _, err := root.WriteFileCAS("src/main.go", AbsentFingerprint, []byte("overwrite"), 0o644); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale create error = %v", err)
	}
	second, err := root.WriteFileCAS("src/main.go", first, []byte("second"), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	content, fingerprint, err := root.ReadFile("src/main.go")
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "second" || fingerprint != second {
		t.Fatalf("content = %q, fingerprint = %q", content, fingerprint)
	}
}

func TestRootRejectsTraversalAndGitWrites(t *testing.T) {
	t.Parallel()
	root, err := OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.WriteFileCAS("../outside", AbsentFingerprint, nil, 0); !errors.Is(err, ErrOutsideRoot) {
		t.Fatalf("traversal error = %v", err)
	}
	if _, err := root.WriteFileCAS(".git/hooks/pre-commit", AbsentFingerprint, nil, 0); !errors.Is(err, ErrGitWriteForbidden) {
		t.Fatalf("git write error = %v", err)
	}
}

func TestRootRejectsEscapingSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink privileges vary on Windows")
	}
	t.Parallel()
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(workspace, "escape")); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.WriteFileCAS("escape/file", AbsentFingerprint, []byte("no"), 0o600); !errors.Is(err, ErrOutsideRoot) {
		t.Fatalf("escaping symlink error = %v", err)
	}
}

func TestRootAllowsInternalSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink privileges vary on Windows")
	}
	t.Parallel()
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "real"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(workspace, "real"), filepath.Join(workspace, "link")); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.WriteFileCAS("link/file", AbsentFingerprint, []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(workspace, "real", "file"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "ok" {
		t.Fatalf("content = %q", content)
	}
}

func TestEditDeleteRenameAndSearchUseContentPreconditions(t *testing.T) {
	t.Parallel()
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	original := []byte("alpha\nneedle one\nomega\n")
	if err := os.WriteFile(filepath.Join(workspace, "src", "a.txt"), original, 0o640); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := Fingerprint(original)
	updatedFingerprint, err := root.EditFileCAS("src/a.txt", fingerprint, []byte("needle one"), []byte("needle two"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.EditFileCAS("src/a.txt", fingerprint, []byte("needle two"), []byte("bad")); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale edit error = %v", err)
	}
	matches, err := root.Search(".", "needle two", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].Path != "src/a.txt" || matches[0].Line != 2 {
		t.Fatalf("matches = %+v", matches)
	}
	if err := root.RenameFileCAS("src/a.txt", "src/b.txt", "wrong"); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale rename error = %v", err)
	}
	if err := root.RenameFileCAS("src/a.txt", "src/b.txt", updatedFingerprint); err != nil {
		t.Fatal(err)
	}
	if err := root.DeleteFileCAS("src/b.txt", "wrong"); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale delete error = %v", err)
	}
	if err := root.DeleteFileCAS("src/b.txt", updatedFingerprint); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "src", "b.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted file stat error = %v", err)
	}
}

func TestEditRequiresExactlyOneOldContentMatch(t *testing.T) {
	t.Parallel()
	workspace := t.TempDir()
	content := []byte("same same")
	if err := os.WriteFile(filepath.Join(workspace, "file"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.EditFileCAS("file", Fingerprint(content), []byte("same"), []byte("new")); !errors.Is(err, ErrEditMatch) {
		t.Fatalf("ambiguous edit error = %v", err)
	}
}
