package humancmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
	workerkitsqlite "github.com/vibe-agi/human/workerkit/sqlite"
)

func newPublicBackupFixture(t *testing.T) publicLocalBackupPaths {
	t.Helper()
	root := t.TempDir()
	paths := publicLocalBackupPaths{
		WorkspaceRoot: t.TempDir(), WorkspaceKey: "workspace-test",
		ServiceDB:       filepath.Join(root, "private", "store.db"),
		Credentials:     filepath.Join(root, "private", "credentials.json"),
		StateDB:         filepath.Join(root, "private", "workerkit-state.db"),
		MirrorBaseline:  filepath.Join(root, "private", "workerkit-mirror-baseline.json"),
		MirrorWorkspace: filepath.Join(root, "mirror"),
		CallerID:        "local-caller", WorkerID: "local-worker",
	}
	if err := os.MkdirAll(filepath.Dir(paths.ServiceDB), 0o700); err != nil {
		t.Fatal(err)
	}
	serviceResource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: paths.ServiceDB})
	if err != nil {
		t.Fatal(err)
	}
	if err := serviceResource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	stateResource, err := workerkitsqlite.Open(t.Context(), workerkitsqlite.Config{Path: paths.StateDB})
	if err != nil {
		t.Fatal(err)
	}
	if err := stateResource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := writePublicCredentials(paths.Credentials, "original-caller-secret"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(paths.MirrorWorkspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.MirrorWorkspace, "draft.txt"), []byte("original\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.MirrorBaseline, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return paths
}

func TestPublicLocalBackupVerifyRestoreRoundTrip(t *testing.T) {
	paths := newPublicBackupFixture(t)

	archive := filepath.Join(t.TempDir(), "public-local.tar.gz")
	summary, err := createPublicLocalBackup(context.Background(), paths, archive, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Manifest.Version != publicLocalBackupVersion || !summary.Manifest.StateEnabled ||
		!summary.Manifest.BaselineEnabled {
		t.Fatalf("manifest = %+v", summary.Manifest)
	}
	extracted := t.TempDir()
	verified, err := extractAndVerifyPublicLocalBackup(context.Background(), archive, extracted)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Manifest.WorkspaceScope != paths.WorkspaceKey || verified.Bytes == 0 {
		t.Fatalf("verified = %+v", verified)
	}

	if err := writePublicCredentials(paths.Credentials, "mutated-caller-secret"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.MirrorWorkspace, "draft.txt"), []byte("mutated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.MirrorWorkspace, "stale.txt"), []byte("remove\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.MirrorBaseline, []byte("{\"mutated\":true}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	locks, err := acquireStoppedPublicLocal(paths, false)
	if err != nil {
		t.Fatal(err)
	}
	_, restoreErr := restorePublicLocalBackup(context.Background(), paths, archive, true, false)
	closeErr := locks.Close()
	if restoreErr != nil || closeErr != nil {
		t.Fatalf("restore=%v close=%v", restoreErr, closeErr)
	}
	token, found, err := readPublicCredentials(paths.Credentials)
	if err != nil || !found || token != "original-caller-secret" {
		t.Fatalf("restored credential = %q found=%v err=%v", token, found, err)
	}
	draft, err := os.ReadFile(filepath.Join(paths.MirrorWorkspace, "draft.txt"))
	if err != nil || string(draft) != "original\n" {
		t.Fatalf("restored draft = %q, %v", draft, err)
	}
	if _, err := os.Lstat(filepath.Join(paths.MirrorWorkspace, "stale.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale mirror file survived restore: %v", err)
	}
	baseline, err := os.ReadFile(paths.MirrorBaseline)
	if err != nil || string(baseline) != "{}\n" {
		t.Fatalf("restored baseline = %q, %v", baseline, err)
	}
	if _, err := os.Lstat(localRestoreJournalPath(paths.ServiceDB)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore journal remains: %v", err)
	}
}

func TestPublicLocalBackupIsPrivateAtomicAndSkipsSymlinks(t *testing.T) {
	paths := newPublicBackupFixture(t)
	if runtime.GOOS != "windows" {
		if err := os.Symlink(paths.Credentials, filepath.Join(paths.MirrorWorkspace, "credential-link")); err != nil {
			t.Fatal(err)
		}
	}
	archive := filepath.Join(t.TempDir(), "backup.tar.gz")
	summary, err := createPublicLocalBackup(t.Context(), paths, archive, false)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(archive)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("archive mode = %o, want 0600", info.Mode().Perm())
	}
	if runtime.GOOS != "windows" {
		if len(summary.Manifest.Skipped) != 1 || summary.Manifest.Skipped[0].Path != "mirror/workspace/credential-link" {
			t.Fatalf("skipped = %+v", summary.Manifest.Skipped)
		}
	}
	if _, err := createPublicLocalBackup(t.Context(), paths, archive, false); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("second backup without force = %v", err)
	}
	if _, err := createPublicLocalBackup(t.Context(), paths, archive, true); err != nil {
		t.Fatalf("forced atomic replacement: %v", err)
	}
}

func TestPublicLocalBackupRejectsLiveOwnerAndUnsafeArchive(t *testing.T) {
	paths := newPublicBackupFixture(t)
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: paths.ServiceDB})
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := createPublicLocalBackup(t.Context(), paths, archive, false); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("backup while service owns database = %v", err)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	summary, err := createPublicLocalBackup(t.Context(), paths, archive, false)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(archive, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("trailing-data")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := extractAndVerifyPublicLocalBackup(t.Context(), archive, t.TempDir()); err == nil || !strings.Contains(err.Error(), "second gzip member or trailing data") {
		t.Fatalf("verify archive with trailing data = %v", err)
	}

	manifest := summary.Manifest
	manifest.Entries = append([]localBackupManifestEntry(nil), manifest.Entries...)
	manifest.Entries[0].Path = "../escape"
	if err := validatePublicLocalManifest(manifest); err == nil || !strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("unsafe manifest validation = %v", err)
	}
}

func TestPublicLocalRestoreResumesAfterPartialInstall(t *testing.T) {
	paths := newPublicBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := createPublicLocalBackup(t.Context(), paths, archive, false); err != nil {
		t.Fatal(err)
	}
	extracted := t.TempDir()
	summary, err := extractAndVerifyPublicLocalBackup(t.Context(), archive, extracted)
	if err != nil {
		t.Fatal(err)
	}
	if err := writePublicCredentials(paths.Credentials, "mutated-secret"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.MirrorWorkspace, "draft.txt"), []byte("mutated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	journal, err := preparePublicLocalRestore(t.Context(), paths, summary.Manifest, extracted, true)
	if err != nil {
		t.Fatal(err)
	}
	journalPath := localRestoreJournalPath(paths.ServiceDB)
	published, err := writeLocalRestoreJournal(journalPath, journal)
	if err != nil || !published {
		t.Fatalf("publish journal = %v, %v", published, err)
	}
	// Emulate SIGKILL after two durable component renames. Resume must treat
	// those entries idempotently and finish the remaining set.
	for _, entry := range journal.Entries[:2] {
		if err := installLocalRestoreEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := resumePublicLocalRestore(t.Context(), paths); err != nil {
		t.Fatal(err)
	}
	token, found, err := readPublicCredentials(paths.Credentials)
	if err != nil || !found || token != "original-caller-secret" {
		t.Fatalf("resumed credential = %q found=%v err=%v", token, found, err)
	}
	draft, err := os.ReadFile(filepath.Join(paths.MirrorWorkspace, "draft.txt"))
	if err != nil || string(draft) != "original\n" {
		t.Fatalf("resumed mirror = %q err=%v", draft, err)
	}
	if _, err := os.Lstat(journalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore journal remains: %v", err)
	}
}

func TestPublicLocalRestoreJournalFailsClosedOnIdentityOrPathTampering(t *testing.T) {
	paths := newPublicBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := createPublicLocalBackup(t.Context(), paths, archive, false); err != nil {
		t.Fatal(err)
	}
	extracted := t.TempDir()
	summary, err := extractAndVerifyPublicLocalBackup(t.Context(), archive, extracted)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := preparePublicLocalRestore(t.Context(), paths, summary.Manifest, extracted, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { removeLocalRestoreStages(journal) })
	if err := validatePublicLocalRestoreJournal(paths, journal); err != nil {
		t.Fatalf("valid journal: %v", err)
	}
	tests := map[string]func(*localRestoreJournal){
		"deployment": func(candidate *localRestoreJournal) { candidate.DeploymentID = "attacker" },
		"caller":     func(candidate *localRestoreJournal) { candidate.CallerID = "attacker" },
		"target": func(candidate *localRestoreJournal) {
			candidate.Entries[0].Target = filepath.Join(t.TempDir(), "escape.db")
		},
		"duplicate": func(candidate *localRestoreJournal) {
			candidate.Entries = append(candidate.Entries, candidate.Entries[0])
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := journal
			candidate.Entries = append([]localRestoreJournalEntry(nil), journal.Entries...)
			mutate(&candidate)
			if err := validatePublicLocalRestoreJournal(paths, candidate); err == nil {
				t.Fatal("tampered restore journal was accepted")
			}
		})
	}
}
