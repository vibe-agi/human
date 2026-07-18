package humancmd

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerstate"
)

const testLocalGatewayIdentity = "scope:0000000000000000000000000000000000000000000000000000000000000000"

func TestLocalBackupVerifyRestoreRoundTrip(t *testing.T) {
	paths := newLocalBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "local.tar.gz")
	if _, err := createLocalBackup(context.Background(), paths, archive, false); err != nil {
		t.Fatal(err)
	}
	verification := t.TempDir()
	summary, err := extractAndVerifyLocalBackup(context.Background(), archive, verification)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Manifest.Format != localBackupFormat || !summary.Manifest.StateEnabled {
		t.Fatalf("manifest = %+v", summary.Manifest)
	}

	for _, database := range []string{paths.GatewayDB, paths.OutboxDB, paths.StateDB} {
		writeSQLiteValue(t, database, "mutated")
	}
	if err := writeLocalCredentials(paths.Credentials, localCredentialFile{
		Version: localCredentialVersion,
		Active: &localCredentialPair{
			Caller: localCredential{Type: "caller", SubjectID: "other", KeyID: "new-c", Secret: "new-c-secret"},
			Worker: localCredential{Type: "worker", SubjectID: "other", KeyID: "new-w", Secret: "new-w-secret"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.MirrorRoot, paths.CallerSubject, "workspace-one", "draft.txt"), []byte("mutated"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, sidecar := range []string{paths.GatewayDB + "-wal", paths.OutboxDB + "-journal", paths.StateDB + "-shm"} {
		if err := os.WriteFile(sidecar, []byte("stale sidecar"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	lock, err := acquireStoppedLocal(paths, false)
	if err != nil {
		t.Fatal(err)
	}
	_, restoreErr := restoreLocalBackup(context.Background(), paths, archive, true, false)
	closeErr := lock.Close()
	if restoreErr != nil || closeErr != nil {
		t.Fatalf("restore=%v close=%v", restoreErr, closeErr)
	}
	for _, database := range []string{paths.GatewayDB, paths.OutboxDB, paths.StateDB} {
		if got := readSQLiteValue(t, database); got != "original" {
			t.Fatalf("restored %s = %q", database, got)
		}
	}
	credentials, found, err := readLocalCredentials(paths.Credentials)
	if err != nil || !found || credentials.Active == nil || credentials.Active.Caller.SubjectID != "caller" {
		t.Fatalf("restored credentials = %+v found=%v err=%v", credentials, found, err)
	}
	draft, err := os.ReadFile(filepath.Join(paths.MirrorRoot, paths.CallerSubject, "workspace-one", "draft.txt"))
	if err != nil || string(draft) != "draft\n" {
		t.Fatalf("restored draft = %q, %v", draft, err)
	}
	baseline, err := os.ReadFile(filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject, "workspace-one", "baseline.json"))
	if err != nil || string(baseline) != "{}\n" {
		t.Fatalf("restored baseline = %q, %v", baseline, err)
	}
	if _, err := os.Lstat(localRestoreJournalPath(paths.GatewayDB)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore journal remains: %v", err)
	}
	for _, sidecar := range []string{paths.GatewayDB + "-wal", paths.OutboxDB + "-journal", paths.StateDB + "-shm"} {
		if _, err := os.Lstat(sidecar); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale SQLite sidecar remains at %s: %v", sidecar, err)
		}
	}
}

func TestLocalBackupRejectsLiveOwner(t *testing.T) {
	paths := newLocalBackupFixture(t)
	location, err := sqlitefile.Resolve(paths.GatewayDB)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := ownerlock.Acquire(location, "running local fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	_, err = createLocalBackup(context.Background(), paths, filepath.Join(t.TempDir(), "backup.tar.gz"), false)
	if err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("live backup error = %v", err)
	}
}

func TestLocalBackupRejectsArchiveDerivedStatePathCollisions(t *testing.T) {
	paths := newLocalBackupFixture(t)
	candidates := map[string]string{
		"restore-journal": localRestoreJournalPath(paths.GatewayDB),
	}
	for label, database := range map[string]string{
		"gateway": paths.GatewayDB, "outbox": paths.OutboxDB, "state": paths.StateDB,
	} {
		for _, suffix := range []string{"journal", "wal", "shm"} {
			candidates[label+"-"+suffix] = database + "-" + suffix
		}
		location, err := sqlitefile.Resolve(database)
		if err != nil {
			t.Fatal(err)
		}
		lockPath, fileBacked, err := ownerlock.Path(location)
		if err != nil || !fileBacked {
			t.Fatalf("resolve %s owner lock: fileBacked=%v err=%v", label, fileBacked, err)
		}
		candidates[label+"-owner-lock"] = lockPath
	}
	for name, archivePath := range candidates {
		t.Run(name, func(t *testing.T) {
			if err := rejectArchiveStateCollision(paths, archivePath); err == nil || !strings.Contains(err.Error(), "collide") {
				t.Fatalf("derived archive collision error = %v", err)
			}
		})
	}
	if err := rejectArchiveStateCollision(paths, filepath.Join(t.TempDir(), "local.tar.gz")); err != nil {
		t.Fatalf("ordinary archive rejected: %v", err)
	}
}

func TestOfflineBackupAndRestoreLockAllWorkerDatabases(t *testing.T) {
	for _, heldStore := range []string{"outbox", "state"} {
		t.Run(heldStore, func(t *testing.T) {
			paths := newLocalBackupFixture(t)
			databasePath := paths.OutboxDB
			if heldStore == "state" {
				databasePath = paths.StateDB
			}
			location, err := sqlitefile.Resolve(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := ownerlock.Acquire(location, "running independent worker "+heldStore)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := createLocalBackup(context.Background(), paths, filepath.Join(t.TempDir(), "backup.tar.gz"), false); err == nil || !strings.Contains(err.Error(), "in use") {
				_ = owner.Close()
				t.Fatalf("backup with live %s owner = %v", heldStore, err)
			}
			if _, err := acquireStoppedLocal(paths, true); err == nil || !strings.Contains(err.Error(), "in use") {
				_ = owner.Close()
				t.Fatalf("restore lock acquisition with live %s owner = %v", heldStore, err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			locks, err := acquireStoppedLocal(paths, false)
			if err != nil {
				t.Fatalf("failed lock acquisition leaked an earlier lock: %v", err)
			}
			if err := locks.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSQLiteBackupSnapshotIncludesCommittedWAL(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.db")
	database, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE fixture(value TEXT); INSERT INTO fixture(value) VALUES ('from-wal')`); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source + "-wal"); err != nil {
		t.Fatalf("fixture did not create a WAL sidecar: %v", err)
	}
	destination := filepath.Join(directory, "snapshot", "copy.db")
	if err := snapshotSQLite(context.Background(), source, destination, "WAL fixture"); err != nil {
		t.Fatal(err)
	}
	copyDB, err := sql.Open("sqlite", sqliteReadWriteDSN(destination))
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	var got string
	if err := copyDB.QueryRow(`SELECT value FROM fixture`).Scan(&got); err != nil || got != "from-wal" {
		t.Fatalf("snapshot WAL value = %q, %v", got, err)
	}
}

func TestLocalBackupRecordsMirrorSymlinkWithoutFollowingIt(t *testing.T) {
	paths := newLocalBackupFixture(t)
	link := filepath.Join(paths.MirrorRoot, paths.CallerSubject, "workspace-one", "outside-link")
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skip("creating symlinks requires an optional Windows privilege")
		}
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "local.tar.gz")
	summary, err := createLocalBackup(context.Background(), paths, archive, false)
	if err != nil {
		t.Fatal(err)
	}
	want := "mirror/workspaces/workspace-one/outside-link"
	if len(summary.Manifest.Skipped) != 1 || summary.Manifest.Skipped[0].Path != want {
		t.Fatalf("skipped = %+v, want %s", summary.Manifest.Skipped, want)
	}
	extracted := t.TempDir()
	if _, err := extractAndVerifyLocalBackup(context.Background(), archive, extracted); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(extracted, filepath.FromSlash(want))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink was restored into verified tree: %v", err)
	}
}

func TestLocalBackupDetectsCorruptionAndPathTraversal(t *testing.T) {
	paths := newLocalBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "local.tar.gz")
	if _, err := createLocalBackup(context.Background(), paths, archive, false); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	payload[len(payload)/2] ^= 0xff
	corrupt := filepath.Join(t.TempDir(), "corrupt.tar.gz")
	if err := os.WriteFile(corrupt, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := extractAndVerifyLocalBackup(context.Background(), corrupt, t.TempDir()); err == nil {
		t.Fatal("corrupt backup verified")
	}
	trailingPayload, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	trailingPayload = append(trailingPayload, []byte("unexpected trailing bytes")...)
	trailingArchive := filepath.Join(t.TempDir(), "trailing.tar.gz")
	if err := os.WriteFile(trailingArchive, trailingPayload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := extractAndVerifyLocalBackup(context.Background(), trailingArchive, t.TempDir()); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing archive error = %v", err)
	}

	manifest := localBackupManifest{
		Format: localBackupFormat, Version: localBackupVersion, CreatedAt: time.Now().UTC(),
		WorkspaceScope: "workspace-test", GatewayIdentity: testLocalGatewayIdentity,
		CallerSubject: "caller", WorkerSubject: "worker",
		Entries: []localBackupManifestEntry{{Path: "../escape", Type: "file", SHA256: strings.Repeat("0", 64), Mode: 0o600}},
	}
	traversal := filepath.Join(t.TempDir(), "traversal.tar.gz")
	writeManifestOnlyBackup(t, traversal, manifest)
	if _, err := extractAndVerifyLocalBackup(context.Background(), traversal, t.TempDir()); err == nil || !strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("path traversal error = %v", err)
	}
}

func TestLocalBackupManifestRejectsCoreSemanticSubstitution(t *testing.T) {
	manifest := localBackupManifest{
		Format: localBackupFormat, Version: localBackupVersion, CreatedAt: time.Now().UTC(),
		WorkspaceScope: "workspace-test", GatewayIdentity: testLocalGatewayIdentity,
		CallerSubject: "caller", WorkerSubject: "worker",
		Entries: []localBackupManifestEntry{
			{Path: "credentials", Type: "directory", Mode: 0o700},
			{Path: "credentials/credentials.json", Type: "file", SHA256: strings.Repeat("0", 64), Mode: 0o600},
			{Path: "gateway", Type: "directory", Mode: 0o700},
			// The exact core path cannot masquerade as a non-SQLite payload.
			{Path: "gateway/gateway.db", Type: "file", SHA256: strings.Repeat("0", 64), Mode: 0o600},
			{Path: "worker", Type: "directory", Mode: 0o700},
			{Path: "worker/worker-outbox.db", Type: "file", SHA256: strings.Repeat("0", 64), Mode: 0o600, SQLite: true},
			{Path: "mirror", Type: "directory", Mode: 0o700},
			{Path: "mirror/workspaces", Type: "directory", Mode: 0o700},
			{Path: "mirror/state", Type: "directory", Mode: 0o700},
		},
	}
	if err := validateLocalBackupManifest(manifest); err == nil || !strings.Contains(err.Error(), "gateway") {
		t.Fatalf("substituted core manifest error = %v", err)
	}
}

func TestLocalBackupVerifyRejectsCredentialGatewayMismatch(t *testing.T) {
	paths := newLocalBackupFixture(t)
	validArchive := filepath.Join(t.TempDir(), "valid.tar.gz")
	if _, err := createLocalBackup(context.Background(), paths, validArchive, false); err != nil {
		t.Fatal(err)
	}
	stage := t.TempDir()
	summary, err := extractAndVerifyLocalBackup(context.Background(), validArchive, stage)
	if err != nil {
		t.Fatal(err)
	}
	mismatched := localCredentialFile{
		Version: localCredentialVersion,
		Active: &localCredentialPair{
			Caller: localCredential{Type: "caller", SubjectID: "caller", KeyID: "missing-c", Secret: "missing-c-secret"},
			Worker: localCredential{Type: "worker", SubjectID: "worker", KeyID: "missing-w", Secret: "missing-w-secret"},
		},
	}
	if err := writeLocalCredentials(filepath.Join(stage, "credentials", "credentials.json"), mismatched); err != nil {
		t.Fatal(err)
	}
	manifest, _, err := buildLocalBackupManifest(stage, paths, summary.Manifest.Skipped)
	if err != nil {
		t.Fatal(err)
	}
	badArchive := filepath.Join(t.TempDir(), "mismatch.tar.gz")
	if err := writeLocalBackupArchive(stage, manifest, badArchive, false); err != nil {
		t.Fatal(err)
	}
	if _, err := extractAndVerifyLocalBackup(context.Background(), badArchive, t.TempDir()); err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("credential/gateway mismatch error = %v", err)
	}
}

func TestLocalBackupVerifyRejectsWorkerStateIdentityMismatch(t *testing.T) {
	paths := newLocalBackupFixture(t)
	validArchive := filepath.Join(t.TempDir(), "valid.tar.gz")
	if _, err := createLocalBackup(context.Background(), paths, validArchive, false); err != nil {
		t.Fatal(err)
	}
	stage := t.TempDir()
	summary, err := extractAndVerifyLocalBackup(context.Background(), validArchive, stage)
	if err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", filepath.Join(stage, "worker", "worker-state.db"))
	if err != nil {
		t.Fatal(err)
	}
	wrongIdentity := "scope:" + strings.Repeat("1", sha256.Size*2)
	_, updateErr := database.Exec(`UPDATE worker_state SET gateway_id = ?`, wrongIdentity)
	closeErr := database.Close()
	if updateErr != nil || closeErr != nil {
		t.Fatalf("mutate worker state identity: %v", errors.Join(updateErr, closeErr))
	}
	manifest, _, err := buildLocalBackupManifest(stage, paths, summary.Manifest.Skipped)
	if err != nil {
		t.Fatal(err)
	}
	badArchive := filepath.Join(t.TempDir(), "worker-state-mismatch.tar.gz")
	if err := writeLocalBackupArchive(stage, manifest, badArchive, false); err != nil {
		t.Fatal(err)
	}
	if _, err := extractAndVerifyLocalBackup(context.Background(), badArchive, t.TempDir()); err == nil ||
		!strings.Contains(err.Error(), "worker state identity") {
		t.Fatalf("worker-state identity mismatch error = %v", err)
	}
}

func TestLocalRestoreRebindsWorkerStateAcrossGatewayDatabasePaths(t *testing.T) {
	source := newLocalBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "local.tar.gz")
	if _, err := createLocalBackup(context.Background(), source, archive, false); err != nil {
		t.Fatal(err)
	}
	destination := newLocalBackupFixture(t)
	sourceIdentity, err := localGatewayIdentity(source.GatewayDB)
	if err != nil {
		t.Fatal(err)
	}
	destinationIdentity, err := localGatewayIdentity(destination.GatewayDB)
	if err != nil {
		t.Fatal(err)
	}
	if sourceIdentity == destinationIdentity {
		t.Fatal("fixture gateway identities unexpectedly match")
	}
	locks, err := acquireStoppedLocal(destination, false)
	if err != nil {
		t.Fatal(err)
	}
	_, restoreErr := restoreLocalBackup(context.Background(), destination, archive, true, false)
	closeErr := locks.Close()
	if restoreErr != nil || closeErr != nil {
		t.Fatalf("cross-path restore=%v close=%v", restoreErr, closeErr)
	}
	if err := workerclient.RebindOutboxIdentity(
		context.Background(), destination.OutboxDB,
		destinationIdentity, destinationIdentity, destination.WorkerSubject,
	); err != nil {
		t.Fatalf("restored worker identity was not rebound to destination: %v", err)
	}
	if err := workerstate.RebindIdentity(
		context.Background(), destination.StateDB,
		destinationIdentity, destinationIdentity, destination.WorkerSubject,
	); err != nil {
		t.Fatalf("restored TUI state identity was not rebound to destination: %v", err)
	}
}

func TestLocalRestoreResumesAfterComponentRenameBoundary(t *testing.T) {
	paths := newLocalBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "local.tar.gz")
	if _, err := createLocalBackup(context.Background(), paths, archive, false); err != nil {
		t.Fatal(err)
	}
	for _, database := range []string{paths.GatewayDB, paths.OutboxDB, paths.StateDB} {
		writeSQLiteValue(t, database, "before-resume")
	}
	extracted := t.TempDir()
	summary, err := extractAndVerifyLocalBackup(context.Background(), archive, extracted)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := prepareLocalRestore(context.Background(), paths, summary.Manifest, extracted, true)
	if err != nil {
		t.Fatal(err)
	}
	journalPath := localRestoreJournalPath(paths.GatewayDB)
	if _, err := writeLocalRestoreJournal(journalPath, journal); err != nil {
		t.Fatal(err)
	}
	if err := installLocalRestoreEntry(journal.Entries[0]); err != nil {
		t.Fatal(err)
	}
	if err := rejectPendingLocalRestore(paths.GatewayDB); err == nil {
		t.Fatal("local startup did not reject pending restore")
	}
	if err := resumeLocalRestore(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	for _, database := range []string{paths.GatewayDB, paths.OutboxDB, paths.StateDB} {
		if got := readSQLiteValue(t, database); got != "original" {
			t.Fatalf("resumed %s = %q", database, got)
		}
	}
}

func TestLocalRestoreForwardRenameBoundariesAreIdempotentForEveryComponent(t *testing.T) {
	for _, boundary := range []string{"previous-parked", "new-installed"} {
		t.Run(boundary, func(t *testing.T) {
			paths, journal, journalPath := prepareRestoreBoundaryFixture(t)
			if _, err := writeLocalRestoreJournal(journalPath, journal); err != nil {
				t.Fatal(err)
			}
			for _, entry := range journal.Entries {
				if entry.HadPrevious {
					if err := os.Rename(entry.Target, entry.Previous); err != nil {
						t.Fatalf("park %s: %v", entry.ID, err)
					}
				}
				if boundary == "new-installed" && entry.Present {
					if err := os.Rename(entry.Staged, entry.Target); err != nil {
						t.Fatalf("install %s: %v", entry.ID, err)
					}
				}
				if err := installLocalRestoreEntry(entry); err != nil {
					t.Fatalf("finish %s after %s: %v", entry.ID, boundary, err)
				}
				if err := installLocalRestoreEntry(entry); err != nil {
					t.Fatalf("repeat %s after %s: %v", entry.ID, boundary, err)
				}
			}
			if err := resumeLocalRestore(context.Background(), paths); err != nil {
				t.Fatal(err)
			}
			assertRestoreCommitted(t, paths, journal)
		})
	}
}

func TestLocalRestoreRollbackRenameBoundariesAreIdempotentForEveryComponent(t *testing.T) {
	for _, boundary := range []string{"new-removed", "previous-restored"} {
		t.Run(boundary, func(t *testing.T) {
			paths, journal, journalPath := prepareRestoreBoundaryFixture(t)
			if _, err := writeLocalRestoreJournal(journalPath, journal); err != nil {
				t.Fatal(err)
			}
			for _, entry := range journal.Entries {
				if err := installLocalRestoreEntry(entry); err != nil {
					t.Fatalf("install %s: %v", entry.ID, err)
				}
			}
			journal.Phase = localRestorePhaseRollingBack
			if err := replaceLocalRestoreJournal(journalPath, journal); err != nil {
				t.Fatal(err)
			}
			for index := len(journal.Entries) - 1; index >= 0; index-- {
				entry := journal.Entries[index]
				if _, err := os.Lstat(entry.Previous); err == nil {
					if err := os.RemoveAll(entry.Target); err != nil {
						t.Fatalf("remove installed %s: %v", entry.ID, err)
					}
					if boundary == "previous-restored" {
						if err := os.Rename(entry.Previous, entry.Target); err != nil {
							t.Fatalf("restore previous %s: %v", entry.ID, err)
						}
					}
				} else if !entry.HadPrevious && entry.Present {
					if err := os.RemoveAll(entry.Target); err != nil {
						t.Fatalf("remove new %s: %v", entry.ID, err)
					}
				}
				if err := rollbackLocalRestoreEntry(entry); err != nil {
					t.Fatalf("finish rollback %s after %s: %v", entry.ID, boundary, err)
				}
				if err := rollbackLocalRestoreEntry(entry); err != nil {
					t.Fatalf("repeat rollback %s after %s: %v", entry.ID, boundary, err)
				}
			}
			if err := resumeLocalRestore(context.Background(), paths); err != nil {
				t.Fatal(err)
			}
			assertRestoreRolledBack(t, paths, journal)
		})
	}
}

func TestLocalRestoreResumeContinuesPartialRollback(t *testing.T) {
	paths, journal, journalPath := prepareRestoreBoundaryFixture(t)
	if _, err := writeLocalRestoreJournal(journalPath, journal); err != nil {
		t.Fatal(err)
	}
	for _, entry := range journal.Entries {
		if err := installLocalRestoreEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	journal.Phase = localRestorePhaseRollingBack
	if err := replaceLocalRestoreJournal(journalPath, journal); err != nil {
		t.Fatal(err)
	}
	for index := len(journal.Entries) - 1; index >= len(journal.Entries)/2; index-- {
		if err := rollbackLocalRestoreEntry(journal.Entries[index]); err != nil {
			t.Fatal(err)
		}
	}
	if err := resumeLocalRestore(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	assertRestoreRolledBack(t, paths, journal)
}

func TestLocalRestoreResumeContinuesCommittedCleanup(t *testing.T) {
	paths, journal, journalPath := prepareRestoreBoundaryFixture(t)
	if _, err := writeLocalRestoreJournal(journalPath, journal); err != nil {
		t.Fatal(err)
	}
	for _, entry := range journal.Entries {
		if err := installLocalRestoreEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateInstalledLocalRestore(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	for _, entry := range journal.Entries {
		if err := os.RemoveAll(entry.Previous); err != nil {
			t.Fatal(err)
		}
		if err := installLocalRestoreEntry(entry); err != nil {
			t.Fatalf("resume %s after prior cleanup: %v", entry.ID, err)
		}
	}
	if err := resumeLocalRestore(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	assertRestoreCommitted(t, paths, journal)
}

func TestLocalRestoreValidationFailureTransitionsToRollback(t *testing.T) {
	paths, journal, journalPath := prepareRestoreBoundaryFixture(t)
	if _, err := writeLocalRestoreJournal(journalPath, journal); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := commitLocalRestore(ctx, journalPath, journal); err == nil {
		t.Fatal("canceled installed validation unexpectedly committed")
	}
	assertRestoreRolledBack(t, paths, journal)
}

func TestLocalRestoreJournalPostPublishSyncFailureKeepsResumeData(t *testing.T) {
	paths, journal, journalPath := prepareRestoreBoundaryFixture(t)
	wantErr := errors.New("injected directory sync failure")
	published, err := writeLocalRestoreJournalWithSync(journalPath, journal, func(string) error { return wantErr })
	if !published || !errors.Is(err, wantErr) {
		t.Fatalf("published=%v err=%v", published, err)
	}
	for _, entry := range journal.Entries {
		if !entry.Present {
			continue
		}
		if _, err := os.Lstat(entry.Staged); err != nil {
			t.Fatalf("published journal lost staged %s: %v", entry.ID, err)
		}
	}
	if err := resumeLocalRestore(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	assertRestoreCommitted(t, paths, journal)
}

func TestLocalRestoreRollbackPhaseReplaceFailureRemainsRecoverable(t *testing.T) {
	paths, journal, journalPath := prepareRestoreBoundaryFixture(t)
	if _, err := writeLocalRestoreJournal(journalPath, journal); err != nil {
		t.Fatal(err)
	}
	for _, entry := range journal.Entries {
		if err := installLocalRestoreEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	journal.Phase = localRestorePhaseRollingBack
	wantErr := errors.New("injected rollback phase sync failure")
	if err := replaceLocalRestoreJournalWithSync(journalPath, journal, func(string) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("phase replace error = %v", err)
	}
	if err := resumeLocalRestore(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	assertRestoreRolledBack(t, paths, journal)
}

func TestPrepareLocalRestoreCleansCurrentEntryAfterValidationFailure(t *testing.T) {
	paths := newLocalBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "local.tar.gz")
	if _, err := createLocalBackup(context.Background(), paths, archive, false); err != nil {
		t.Fatal(err)
	}
	extracted := t.TempDir()
	summary, err := extractAndVerifyLocalBackup(context.Background(), archive, extracted)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(extracted, "gateway", "gateway.db"), []byte("not a SQLite database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareLocalRestore(context.Background(), paths, summary.Manifest, extracted, true); err == nil {
		t.Fatal("corrupt staged gateway unexpectedly prepared")
	}
	entries, err := os.ReadDir(filepath.Dir(paths.GatewayDB))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".human-restore-") {
			t.Fatalf("failed prepare leaked staging path %s", entry.Name())
		}
	}
}

func TestLocalRestoreJournalRejectsSemanticDowngrade(t *testing.T) {
	paths := newLocalBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "local.tar.gz")
	if _, err := createLocalBackup(context.Background(), paths, archive, false); err != nil {
		t.Fatal(err)
	}
	extracted := t.TempDir()
	summary, err := extractAndVerifyLocalBackup(context.Background(), archive, extracted)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := prepareLocalRestore(context.Background(), paths, summary.Manifest, extracted, true)
	if err != nil {
		t.Fatal(err)
	}
	defer removeLocalRestoreStages(journal)
	journal.Entries[0].SQLite = false
	if err := validateLocalRestoreJournal(paths, journal); err == nil || !strings.Contains(err.Error(), "semantics") {
		t.Fatalf("journal semantic downgrade error = %v", err)
	}
}

func TestLocalRestoreRefusesNonemptyTargetsWithoutForce(t *testing.T) {
	paths := newLocalBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "local.tar.gz")
	if _, err := createLocalBackup(context.Background(), paths, archive, false); err != nil {
		t.Fatal(err)
	}
	lock, err := acquireStoppedLocal(paths, false)
	if err != nil {
		t.Fatal(err)
	}
	_, restoreErr := restoreLocalBackup(context.Background(), paths, archive, false, false)
	_ = lock.Close()
	if restoreErr == nil || !strings.Contains(restoreErr.Error(), "non-empty") {
		t.Fatalf("non-force restore error = %v", restoreErr)
	}
}

func prepareRestoreBoundaryFixture(t *testing.T) (localBackupPaths, localRestoreJournal, string) {
	t.Helper()
	paths := newLocalBackupFixture(t)
	archive := filepath.Join(t.TempDir(), "local.tar.gz")
	if _, err := createLocalBackup(context.Background(), paths, archive, false); err != nil {
		t.Fatal(err)
	}
	for _, database := range []string{paths.GatewayDB, paths.OutboxDB, paths.StateDB} {
		writeSQLiteValue(t, database, "before-resume")
		for _, suffix := range []string{"journal", "wal", "shm"} {
			if err := os.WriteFile(database+"-"+suffix, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.WriteFile(
		filepath.Join(paths.MirrorRoot, paths.CallerSubject, "workspace-one", "draft.txt"),
		[]byte("before-resume\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject, "workspace-one", "baseline.json"),
		[]byte("{\"before\":true}\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	extracted := t.TempDir()
	summary, err := extractAndVerifyLocalBackup(context.Background(), archive, extracted)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := prepareLocalRestore(context.Background(), paths, summary.Manifest, extracted, true)
	if err != nil {
		t.Fatal(err)
	}
	return paths, journal, localRestoreJournalPath(paths.GatewayDB)
}

func assertRestoreCommitted(t *testing.T, paths localBackupPaths, journal localRestoreJournal) {
	t.Helper()
	for _, database := range []string{paths.GatewayDB, paths.OutboxDB, paths.StateDB} {
		if got := readSQLiteValue(t, database); got != "original" {
			t.Fatalf("committed %s = %q", database, got)
		}
		for _, suffix := range []string{"journal", "wal", "shm"} {
			if _, err := os.Lstat(database + "-" + suffix); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("committed sidecar %s-%s remains: %v", database, suffix, err)
			}
		}
	}
	if _, err := os.Lstat(localRestoreJournalPath(paths.GatewayDB)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("committed restore journal remains: %v", err)
	}
	assertRestoreScratchRemoved(t, journal)
}

func assertRestoreRolledBack(t *testing.T, paths localBackupPaths, journal localRestoreJournal) {
	t.Helper()
	for _, database := range []string{paths.GatewayDB, paths.OutboxDB, paths.StateDB} {
		for _, suffix := range []string{"journal", "wal", "shm"} {
			payload, err := os.ReadFile(database + "-" + suffix)
			if err != nil || len(payload) != 0 {
				t.Fatalf("rolled back sidecar %s-%s = %q, %v", database, suffix, payload, err)
			}
		}
		if got := readSQLiteValue(t, database); got != "before-resume" {
			t.Fatalf("rolled back %s = %q", database, got)
		}
	}
	if _, err := os.Lstat(localRestoreJournalPath(paths.GatewayDB)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rolled back restore journal remains: %v", err)
	}
	assertRestoreScratchRemoved(t, journal)
}

func assertRestoreScratchRemoved(t *testing.T, journal localRestoreJournal) {
	t.Helper()
	for _, entry := range journal.Entries {
		for _, scratch := range []string{entry.Staged, entry.Previous} {
			if scratch == "" {
				continue
			}
			if _, err := os.Lstat(scratch); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("restore scratch path remains at %s: %v", scratch, err)
			}
		}
	}
}

func newLocalBackupFixture(t *testing.T) localBackupPaths {
	t.Helper()
	root := t.TempDir()
	data := filepath.Join(root, "data")
	if err := os.Mkdir(data, 0o700); err != nil {
		t.Fatal(err)
	}
	paths := localBackupPaths{
		WorkspaceRoot: filepath.Join(root, "workspace"), WorkspaceKey: "workspace-test",
		GatewayDB: filepath.Join(data, "gateway.db"), Credentials: filepath.Join(data, "credentials.json"),
		OutboxDB: filepath.Join(data, "outbox.db"), StateDB: filepath.Join(data, "state.db"),
		MirrorRoot: filepath.Join(root, "mirror"), CallerSubject: "caller", WorkerSubject: "worker",
	}
	if err := os.Mkdir(paths.WorkspaceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	createSQLiteFixture(t, paths.GatewayDB, "original")
	createWorkerOutboxFixture(t, paths, "original")
	createWorkerStateFixture(t, paths, "original")
	if err := writeLocalCredentials(paths.Credentials, testCredentialState()); err != nil {
		t.Fatal(err)
	}
	bindFixtureCredentials(t, paths.GatewayDB, testCredentialState())
	worktree := filepath.Join(paths.MirrorRoot, paths.CallerSubject, "workspace-one")
	state := filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject, "workspace-one")
	if err := os.MkdirAll(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "draft.txt"), []byte("draft\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "baseline.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return paths
}

func createWorkerOutboxFixture(t *testing.T, paths localBackupPaths, value string) {
	t.Helper()
	if err := os.WriteFile(paths.OutboxDB, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	gatewayIdentity, err := localGatewayIdentity(paths.GatewayDB)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerclient.RebindOutboxIdentity(
		context.Background(), paths.OutboxDB,
		gatewayIdentity, gatewayIdentity, paths.WorkerSubject,
	); err != nil {
		t.Fatal(err)
	}
	namespaceDigest := sha256.Sum256([]byte(gatewayIdentity + "\x00" + paths.WorkerSubject))
	database, err := sql.Open("sqlite", paths.OutboxDB)
	if err != nil {
		t.Fatal(err)
	}
	_, tableErr := database.Exec(`CREATE TABLE fixture (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`)
	_, valueErr := database.Exec(`INSERT INTO fixture(id, value) VALUES(1, ?)`, value)
	_, identityErr := database.Exec(`INSERT INTO worker_rejected_confirmed(
			namespace, event_id, event_digest, assignment_digest, rejection_digest, confirmed_at
		) VALUES(?, 'fixture-event', 'event', 'assignment', 'rejection', 1)`,
		fmt.Sprintf("%x", namespaceDigest[:]))
	closeErr := database.Close()
	if tableErr != nil || valueErr != nil || identityErr != nil || closeErr != nil {
		t.Fatalf("create worker outbox fixture: %v", errors.Join(tableErr, valueErr, identityErr, closeErr))
	}
	if err := os.Chmod(paths.OutboxDB, 0o600); err != nil {
		t.Fatal(err)
	}
}

func createWorkerStateFixture(t *testing.T, paths localBackupPaths, value string) {
	t.Helper()
	gatewayIdentity, err := localGatewayIdentity(paths.GatewayDB)
	if err != nil {
		t.Fatal(err)
	}
	store, err := workerstate.Open(context.Background(), paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Bind(context.Background(), gatewayIdentity, paths.WorkerSubject); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	_, putErr := store.Put(context.Background(), workerstate.Scope{
		CallerID: paths.CallerSubject, SessionKey: "fixture-session", Tier: completion.TierChat,
	}, "fixture", json.RawMessage(`{"value":"state"}`))
	closeErr := store.Close()
	if putErr != nil || closeErr != nil {
		t.Fatalf("create worker state fixture: %v", errors.Join(putErr, closeErr))
	}
	database, err := sql.Open("sqlite", paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	_, setupErr := database.Exec(`
		CREATE TABLE fixture (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO fixture(id, value) VALUES(1, ?)`, value)
	closeErr = database.Close()
	if setupErr != nil || closeErr != nil {
		t.Fatalf("add worker state fixture value: %v", errors.Join(setupErr, closeErr))
	}
	if err := os.Chmod(paths.StateDB, 0o600); err != nil {
		t.Fatal(err)
	}
}

func bindFixtureCredentials(t *testing.T, filename string, credentials localCredentialFile) {
	t.Helper()
	database, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`CREATE TABLE api_tokens (
		key_id TEXT PRIMARY KEY, principal_type TEXT NOT NULL, subject_id TEXT NOT NULL,
		token_hash BLOB NOT NULL UNIQUE, created_at INTEGER NOT NULL, revoked_at INTEGER
	)`); err != nil {
		t.Fatal(err)
	}
	for _, credential := range []localCredential{credentials.Active.Caller, credentials.Active.Worker} {
		digest := sha256.Sum256([]byte(credential.Secret))
		if _, err := database.Exec(`INSERT INTO api_tokens(key_id, principal_type, subject_id, token_hash, created_at) VALUES(?, ?, ?, ?, 1)`,
			credential.KeyID, credential.Type, credential.SubjectID, digest[:]); err != nil {
			t.Fatal(err)
		}
	}
}

func createSQLiteFixture(t *testing.T, filename, value string) {
	t.Helper()
	database, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE fixture (id INTEGER PRIMARY KEY, value TEXT NOT NULL); INSERT INTO fixture(id, value) VALUES(1, ?)`, value); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filename, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeSQLiteValue(t *testing.T, filename, value string) {
	t.Helper()
	database, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`UPDATE fixture SET value = ? WHERE id = 1`, value); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func readSQLiteValue(t *testing.T, filename string) string {
	t.Helper()
	database, err := sql.Open("sqlite", sqliteReadWriteDSN(filename))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var value string
	if err := database.QueryRow(`SELECT value FROM fixture WHERE id = 1`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func writeManifestOnlyBackup(t *testing.T, filename string, manifest localBackupManifest) {
	t.Helper()
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	gzipWriter := gzip.NewWriter(file)
	tarWriter := tar.NewWriter(gzipWriter)
	payload, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.WriteHeader(&tar.Header{Name: localBackupManifestPath, Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(tarWriter.Close(), gzipWriter.Close(), file.Close()); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(filename, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
