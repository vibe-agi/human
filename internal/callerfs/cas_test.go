package callerfs

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReadFileLimitedRejectsOversizedContentWithoutReadingItIntoResult(t *testing.T) {
	t.Parallel()
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "large.bin"), []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := root.ReadFileLimited("large.bin", 8); !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("bounded read error = %v", err)
	}
	if _, _, err := root.ReadFileLimited("large.bin", 0); err == nil ||
		!strings.Contains(err.Error(), "limit must be positive") {
		t.Fatalf("non-positive read limit error = %v", err)
	}
	content, fingerprint, err := root.ReadFileLimited("large.bin", 10)
	if err != nil || string(content) != "0123456789" || fingerprint != Fingerprint(content) {
		t.Fatalf("exact-limit read = %q, %q, %v", content, fingerprint, err)
	}
}

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
	content, fingerprint, err := root.ReadFileLimited("src/main.go", 64)
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

func TestRootRejectsGitWritesThroughInternalSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink privileges vary on Windows")
	}
	t.Parallel()
	workspace := t.TempDir()
	gitDirectory := filepath.Join(workspace, ".git")
	if err := os.MkdirAll(filepath.Join(gitDirectory, "hooks"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(gitDirectory, filepath.Join(workspace, "git-alias")); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}

	config := []byte("[core]\n")
	configPath := filepath.Join(gitDirectory, "config")
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}
	expected := Fingerprint(config)
	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "write",
			run: func() error {
				_, err := root.WriteFileCAS("git-alias/hooks/pre-commit", AbsentFingerprint, []byte("#!/bin/sh\n"), 0o700)
				return err
			},
		},
		{
			name: "edit",
			run: func() error {
				_, err := root.EditFileCAS("git-alias/config", expected, []byte("core"), []byte("evil"))
				return err
			},
		},
		{
			name: "delete",
			run: func() error {
				return root.DeleteFileCAS("git-alias/config", expected)
			},
		},
		{
			name: "rename into git",
			run: func() error {
				source := []byte("hook")
				if err := os.WriteFile(filepath.Join(workspace, "source"), source, 0o600); err != nil {
					return err
				}
				return root.RenameFileCAS("source", "git-alias/hooks/pre-commit", Fingerprint(source))
			},
		},
		{
			name: "rename out of git",
			run: func() error {
				return root.RenameFileCAS("git-alias/config", "stolen-config", expected)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(configPath, config, 0o600); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{
				filepath.Join(gitDirectory, "hooks", "pre-commit"),
				filepath.Join(workspace, "source"),
				filepath.Join(workspace, "stolen-config"),
			} {
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
			}
			if err := test.run(); !errors.Is(err, ErrGitWriteForbidden) {
				t.Fatalf("git symlink mutation error = %v", err)
			}
		})
	}
	if content, err := os.ReadFile(configPath); err != nil || string(content) != string(config) {
		t.Fatalf("git config changed after rejected mutations: %q, %v", content, err)
	}
	if _, err := os.Stat(filepath.Join(gitDirectory, "hooks", "pre-commit")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("git hook exists after rejected mutations: %v", err)
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
	report, err := root.SearchDetailed(".", "needle two", 10)
	if err != nil {
		t.Fatal(err)
	}
	matches := report.Matches
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

func TestSearchSkipsOversizedSingleLineWithoutAbortingOtherFiles(t *testing.T) {
	t.Parallel()
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "bundle.js"),
		[]byte(strings.Repeat("x", (4<<20)+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "source.go"), []byte("first\nneedle\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	report, err := root.SearchDetailed(".", "needle", 10)
	if err != nil {
		t.Fatal(err)
	}
	matches := report.Matches
	if len(matches) != 1 || matches[0].Path != "source.go" || matches[0].Line != 2 {
		t.Fatalf("matches after oversized line = %+v", matches)
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Path != "bundle.js" ||
		!strings.Contains(report.Skipped[0].Reason, "token too long") {
		t.Fatalf("oversized line diagnostics = %+v", report.Skipped)
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

func TestConcurrentWritesWithSamePreconditionHaveOneWinner(t *testing.T) {
	t.Parallel()
	workspace := t.TempDir()
	original := []byte("original")
	if err := os.WriteFile(filepath.Join(workspace, "shared.txt"), original, 0o600); err != nil {
		t.Fatal(err)
	}
	// Use independently opened roots to verify the process-wide path lock, not
	// just coordination through one Root instance.
	roots := make([]*Root, 16)
	for index := range roots {
		root, err := OpenRoot(workspace)
		if err != nil {
			t.Fatal(err)
		}
		roots[index] = root
	}

	type result struct {
		content []byte
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, len(roots))
	for index, root := range roots {
		content := []byte("writer-" + string(rune('a'+index)))
		go func() {
			<-start
			_, err := root.WriteFileCAS("shared.txt", Fingerprint(original), content, 0o600)
			results <- result{content: content, err: err}
		}()
	}
	close(start)

	winners := make([]byte, 0)
	for range roots {
		outcome := <-results
		switch {
		case outcome.err == nil:
			if winners != nil && len(winners) != 0 {
				t.Fatalf("more than one concurrent write succeeded")
			}
			winners = outcome.content
		case errors.Is(outcome.err, ErrPreconditionFailed):
		default:
			t.Fatalf("concurrent write error = %v", outcome.err)
		}
	}
	if len(winners) == 0 {
		t.Fatal("no concurrent write succeeded")
	}
	got, err := os.ReadFile(filepath.Join(workspace, "shared.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(winners) {
		t.Fatalf("final content = %q, winning content = %q", got, winners)
	}
}

func TestConcurrentMutationKindsSharePathLock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*Root, string) error
	}{
		{
			name: "edit",
			run: func(root *Root, expected string) error {
				_, err := root.EditFileCAS("shared.txt", expected, []byte("original"), []byte("edited"))
				return err
			},
		},
		{
			name: "delete",
			run: func(root *Root, expected string) error {
				return root.DeleteFileCAS("shared.txt", expected)
			},
		},
		{
			name: "rename",
			run: func(root *Root, expected string) error {
				return root.RenameFileCAS("shared.txt", "moved.txt", expected)
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			workspace := t.TempDir()
			original := []byte("original")
			if err := os.WriteFile(filepath.Join(workspace, "shared.txt"), original, 0o600); err != nil {
				t.Fatal(err)
			}
			writerRoot, err := OpenRoot(workspace)
			if err != nil {
				t.Fatal(err)
			}
			otherRoot, err := OpenRoot(workspace)
			if err != nil {
				t.Fatal(err)
			}
			expected := Fingerprint(original)
			start := make(chan struct{})
			results := make(chan error, 2)
			go func() {
				<-start
				_, err := writerRoot.WriteFileCAS("shared.txt", expected, []byte("written"), 0o600)
				results <- err
			}()
			go func() {
				<-start
				results <- test.run(otherRoot, expected)
			}()
			close(start)

			first, second := <-results, <-results
			if (first == nil) == (second == nil) {
				t.Fatalf("errors = (%v, %v), want exactly one success", first, second)
			}
			loser := first
			if loser == nil {
				loser = second
			}
			if !errors.Is(loser, ErrPreconditionFailed) {
				t.Fatalf("losing mutation error = %v, want ErrPreconditionFailed", loser)
			}
		})
	}
}

func TestConcurrentRenamesOfSameSourceHaveOneWinner(t *testing.T) {
	t.Parallel()
	workspace := t.TempDir()
	original := []byte("original")
	if err := os.WriteFile(filepath.Join(workspace, "source.txt"), original, 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	expected := Fingerprint(original)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, destination := range []string{"first.txt", "second.txt"} {
		destination := destination
		go func() {
			<-start
			results <- root.RenameFileCAS("source.txt", destination, expected)
		}()
	}
	close(start)
	first, second := <-results, <-results
	if (first == nil) == (second == nil) {
		t.Fatalf("errors = (%v, %v), want exactly one success", first, second)
	}
	loser := first
	if loser == nil {
		loser = second
	}
	if !errors.Is(loser, ErrPreconditionFailed) {
		t.Fatalf("losing rename error = %v, want ErrPreconditionFailed", loser)
	}
}

func TestPathLockTableOrdersAndReclaimsEntries(t *testing.T) {
	t.Parallel()
	var table pathLockTable
	firstUnlock := table.lock("b", "a", "a")

	acquired := make(chan struct{})
	release := make(chan struct{})
	go func() {
		secondUnlock := table.lock("a")
		close(acquired)
		<-release
		secondUnlock()
	}()

	// Wait until the second caller is registered as a waiter. Its reference
	// must keep the entry alive while the first caller releases it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		table.mu.Lock()
		entry := table.entries[filepath.Clean("a")]
		registered := entry != nil && entry.refs == 2
		table.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second path lock did not register")
		}
		runtime.Gosched()
	}
	firstUnlock()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("second path lock did not acquire after release")
	}
	close(release)

	deadline = time.Now().Add(5 * time.Second)
	for {
		table.mu.Lock()
		remaining := len(table.entries)
		table.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("path lock table retained %d entries", remaining)
		}
		runtime.Gosched()
	}
	// Unlock functions are idempotent so deferred cleanup cannot corrupt refs.
	firstUnlock()
}

func TestOpposingPathLockSetsDoNotDeadlock(t *testing.T) {
	t.Parallel()
	var table pathLockTable
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	for _, paths := range [][2]string{{"a", "b"}, {"b", "a"}} {
		paths := paths
		go func() {
			defer wait.Done()
			<-start
			unlock := table.lock(paths[0], paths[1])
			unlock()
			done <- struct{}{}
		}()
	}
	close(start)
	waitDone := make(chan struct{})
	go func() {
		wait.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("opposing path lock sets deadlocked")
	}
	if len(done) != 2 {
		t.Fatalf("completed callers = %d, want 2", len(done))
	}
}

func TestPathLockAliasesShareCaseAndUnicodeNormalizationKey(t *testing.T) {
	t.Parallel()
	var table pathLockTable
	firstUnlock := table.lock(filepath.Join("Workspace", "CAFÉ.txt"))
	defer firstUnlock()

	acquired := make(chan func(), 1)
	go func() {
		// The second spelling uses lower case and a decomposed acute accent.
		acquired <- table.lock(filepath.Join("workspace", "cafe\u0301.txt"))
	}()
	select {
	case unlock := <-acquired:
		unlock()
		t.Fatal("case/normalization alias acquired a distinct path lock")
	case <-time.After(30 * time.Millisecond):
	}

	firstUnlock()
	select {
	case unlock := <-acquired:
		unlock()
	case <-time.After(time.Second):
		t.Fatal("alias path lock did not acquire after release")
	}
}
