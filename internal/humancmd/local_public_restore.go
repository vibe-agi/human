package humancmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/google/uuid"
)

const publicRestoreIdentity = "public:human-local"

func restorePublicLocalBackup(
	ctx context.Context,
	paths publicLocalBackupPaths,
	archivePath string,
	force bool,
	acceptWorkspaceMismatch bool,
) (publicLocalBackupManifest, error) {
	if err := validatePublicLocalPathSet(paths); err != nil {
		return publicLocalBackupManifest{}, err
	}
	if err := rejectPublicArchiveCollision(paths, archivePath); err != nil {
		return publicLocalBackupManifest{}, err
	}
	extracted, err := os.MkdirTemp("", "human-local-restore-verify-*")
	if err != nil {
		return publicLocalBackupManifest{}, err
	}
	defer os.RemoveAll(extracted)
	summary, err := extractAndVerifyPublicLocalBackup(ctx, archivePath, extracted)
	if err != nil {
		return publicLocalBackupManifest{}, err
	}
	manifest := summary.Manifest
	if manifest.WorkspaceScope != paths.WorkspaceKey && !acceptWorkspaceMismatch {
		return publicLocalBackupManifest{}, fmt.Errorf(
			"backup belongs to workspace scope %s, current scope is %s; use --accept-workspace-mismatch only after verifying the destination",
			manifest.WorkspaceScope, paths.WorkspaceKey,
		)
	}
	if manifest.CallerID != paths.CallerID || manifest.WorkerID != paths.WorkerID {
		return publicLocalBackupManifest{}, fmt.Errorf(
			"backup principal identities (%q, %q) do not match configured local identities (%q, %q)",
			manifest.CallerID, manifest.WorkerID, paths.CallerID, paths.WorkerID,
		)
	}
	if manifest.StateEnabled && paths.StateDB == "" {
		return publicLocalBackupManifest{}, errors.New("backup contains worker state; configure a non-empty --state-db destination")
	}
	journal, err := preparePublicLocalRestore(ctx, paths, manifest, extracted, force)
	if err != nil {
		return publicLocalBackupManifest{}, err
	}
	journalPath := localRestoreJournalPath(paths.ServiceDB)
	published, err := writeLocalRestoreJournal(journalPath, journal)
	if err != nil {
		if !published {
			removeLocalRestoreStages(journal)
		}
		return publicLocalBackupManifest{}, err
	}
	if err := commitPublicLocalRestore(ctx, journalPath, journal); err != nil {
		return publicLocalBackupManifest{}, fmt.Errorf(
			"commit local restore (run `human local restore --workspace . --resume` after correcting the reported condition): %w", err,
		)
	}
	if _, found, err := readPublicCredentials(paths.Credentials); err != nil || !found {
		if err == nil {
			err = errors.New("restored caller credential is missing")
		}
		return publicLocalBackupManifest{}, err
	}
	return manifest, nil
}

func preparePublicLocalRestore(
	ctx context.Context,
	paths publicLocalBackupPaths,
	manifest publicLocalBackupManifest,
	extracted string,
	force bool,
) (localRestoreJournal, error) {
	txID := uuid.NewString()
	journal := localRestoreJournal{
		Version: localRestoreJournalVersion, Phase: localRestorePhaseInstalling,
		TransactionID: txID, WorkspaceScope: paths.WorkspaceKey,
		DeploymentID: publicRestoreIdentity, CallerID: paths.CallerID, WorkerID: paths.WorkerID,
	}
	type component struct {
		id, kind, logical, target string
		present, sqlite, secret   bool
	}
	components := []component{
		{"service", "file", "service/service.db", paths.ServiceDB, true, true, false},
		{"credentials", "file", "credentials/credentials.json", paths.Credentials, true, false, true},
		{"mirror-workspace", "directory", "mirror/workspace", paths.MirrorWorkspace, true, false, false},
	}
	if paths.StateDB != "" {
		components = append(components,
			component{"state", "file", "worker/worker-state.db", paths.StateDB, manifest.StateEnabled, manifest.StateEnabled, false},
			component{"baseline", "file", "mirror/baseline.json", paths.MirrorBaseline, manifest.BaselineEnabled, false, false},
		)
	}
	for _, database := range []struct{ id, path string }{{"service", paths.ServiceDB}, {"state", paths.StateDB}} {
		if database.path == "" {
			continue
		}
		for _, suffix := range []string{"journal", "wal", "shm"} {
			components = append(components, component{
				id: database.id + "-" + suffix, kind: "file", target: database.path + "-" + suffix,
			})
		}
	}
	for _, item := range components {
		if err := os.MkdirAll(filepath.Dir(item.target), 0o700); err != nil {
			removeLocalRestoreStages(journal)
			return localRestoreJournal{}, err
		}
		if item.secret && runtime.GOOS != "windows" {
			info, err := os.Stat(filepath.Dir(item.target))
			if err != nil || info.Mode().Perm()&0o077 != 0 {
				removeLocalRestoreStages(journal)
				return localRestoreJournal{}, fmt.Errorf("credential restore directory %s must have mode 0700", filepath.Dir(item.target))
			}
		}
		hadPrevious, nonempty, err := inspectRestoreTarget(item.target, item.kind)
		if err != nil {
			removeLocalRestoreStages(journal)
			return localRestoreJournal{}, err
		}
		if nonempty && !force {
			removeLocalRestoreStages(journal)
			return localRestoreJournal{}, fmt.Errorf(
				"restore target %s is non-empty; inspect it and use --force to replace the complete local state set", item.target,
			)
		}
		base := filepath.Base(item.target)
		entry := localRestoreJournalEntry{
			ID: item.id, Kind: item.kind, Target: item.target,
			Previous: filepath.Join(filepath.Dir(item.target), ".human-restore-"+txID+"-old-"+base),
			Present:  item.present, HadPrevious: hadPrevious, SQLite: item.sqlite, Credentials: item.secret,
		}
		if item.present {
			entry.Staged = filepath.Join(filepath.Dir(item.target), ".human-restore-"+txID+"-new-"+base)
		}
		journal.Entries = append(journal.Entries, entry)
		current := &journal.Entries[len(journal.Entries)-1]
		if !item.present {
			continue
		}
		source := filepath.Join(extracted, filepath.FromSlash(item.logical))
		if item.kind == "file" {
			err = copyStableRegular(source, current.Staged, 0o600)
			if err == nil {
				current.Digest, err = digestFile(current.Staged)
			}
		} else {
			err = copyRestoreDirectory(source, current.Staged)
			if err == nil {
				current.Digest, err = digestRestoreTree(current.Staged)
			}
		}
		if err != nil {
			removeLocalRestoreStages(journal)
			return localRestoreJournal{}, fmt.Errorf("stage restored %s: %w", item.id, err)
		}
		if current.SQLite {
			if err := quickCheckSQLitePath(ctx, current.Staged, "staged restored "+item.id); err != nil {
				removeLocalRestoreStages(journal)
				return localRestoreJournal{}, err
			}
		}
		if current.Credentials {
			if _, found, err := readPublicCredentials(current.Staged); err != nil || !found {
				removeLocalRestoreStages(journal)
				if err == nil {
					err = errors.New("caller credential is missing")
				}
				return localRestoreJournal{}, err
			}
		}
		if err := syncLocalRestoreStage(*current); err != nil {
			removeLocalRestoreStages(journal)
			return localRestoreJournal{}, err
		}
	}
	if err := validatePublicLocalRestoreJournal(paths, journal); err != nil {
		removeLocalRestoreStages(journal)
		return localRestoreJournal{}, err
	}
	return journal, nil
}

func publicRestoreAllowedTargets(paths publicLocalBackupPaths) map[string]string {
	allowed := map[string]string{
		"service": paths.ServiceDB, "credentials": paths.Credentials, "mirror-workspace": paths.MirrorWorkspace,
	}
	if paths.StateDB != "" {
		allowed["state"] = paths.StateDB
		allowed["baseline"] = paths.MirrorBaseline
	}
	for _, database := range []struct{ id, path string }{{"service", paths.ServiceDB}, {"state", paths.StateDB}} {
		if database.path == "" {
			continue
		}
		for _, suffix := range []string{"journal", "wal", "shm"} {
			allowed[database.id+"-"+suffix] = database.path + "-" + suffix
		}
	}
	return allowed
}

func validatePublicLocalRestoreJournal(paths publicLocalBackupPaths, journal localRestoreJournal) error {
	if journal.Version != localRestoreJournalVersion || !localRestoreTransactionID.MatchString(journal.TransactionID) {
		return errors.New("local restore journal has an unsupported version or transaction ID")
	}
	if journal.Phase != localRestorePhaseInstalling && journal.Phase != localRestorePhaseRollingBack {
		return fmt.Errorf("local restore journal has invalid phase %q", journal.Phase)
	}
	if journal.WorkspaceScope != paths.WorkspaceKey || journal.DeploymentID != publicRestoreIdentity ||
		journal.CallerID != paths.CallerID || journal.WorkerID != paths.WorkerID {
		return errors.New("local restore journal does not belong to the selected public local workspace")
	}
	allowed := publicRestoreAllowedTargets(paths)
	seen := make(map[string]struct{}, len(journal.Entries))
	for _, entry := range journal.Entries {
		target, ok := allowed[entry.ID]
		if !ok || target != entry.Target {
			return fmt.Errorf("local restore journal has an unexpected target for %s", entry.ID)
		}
		if _, duplicate := seen[entry.ID]; duplicate {
			return fmt.Errorf("local restore journal repeats component %s", entry.ID)
		}
		seen[entry.ID] = struct{}{}
		base := filepath.Base(target)
		expectedPrevious := filepath.Join(filepath.Dir(target), ".human-restore-"+journal.TransactionID+"-old-"+base)
		expectedStaged := filepath.Join(filepath.Dir(target), ".human-restore-"+journal.TransactionID+"-new-"+base)
		if entry.Previous != expectedPrevious || entry.Present && entry.Staged != expectedStaged || !entry.Present && entry.Staged != "" {
			return fmt.Errorf("local restore journal staging paths for %s are invalid", entry.ID)
		}
		if entry.Kind != "file" && entry.Kind != "directory" {
			return fmt.Errorf("local restore journal kind for %s is invalid", entry.ID)
		}
		if entry.Present {
			if len(entry.Digest) != sha256.Size*2 {
				return fmt.Errorf("local restore journal digest for %s is invalid", entry.ID)
			}
			if _, err := hex.DecodeString(entry.Digest); err != nil {
				return fmt.Errorf("local restore journal digest for %s is invalid", entry.ID)
			}
		} else if entry.Digest != "" {
			return fmt.Errorf("absent restore component %s must not carry a digest", entry.ID)
		}
		if err := validatePublicRestoreEntrySemantics(entry); err != nil {
			return err
		}
		for _, candidate := range []string{entry.Target, entry.Staged, entry.Previous} {
			if candidate != "" {
				if err := validateRestorePathShape(entry, candidate); err != nil {
					return err
				}
			}
		}
	}
	for id := range allowed {
		if _, found := seen[id]; !found {
			return fmt.Errorf("local restore journal is missing component %s", id)
		}
	}
	return nil
}

func validatePublicRestoreEntrySemantics(entry localRestoreJournalEntry) error {
	valid := false
	switch entry.ID {
	case "service":
		valid = entry.Kind == "file" && entry.Present && entry.SQLite && !entry.Credentials
	case "credentials":
		valid = entry.Kind == "file" && entry.Present && !entry.SQLite && entry.Credentials
	case "mirror-workspace":
		valid = entry.Kind == "directory" && entry.Present && !entry.SQLite && !entry.Credentials
	case "state":
		valid = entry.Kind == "file" && entry.SQLite == entry.Present && !entry.Credentials
	case "baseline":
		valid = entry.Kind == "file" && !entry.SQLite && !entry.Credentials
	default:
		for _, prefix := range []string{"service-", "state-"} {
			if strings.HasPrefix(entry.ID, prefix) {
				suffix := strings.TrimPrefix(entry.ID, prefix)
				valid = (suffix == "journal" || suffix == "wal" || suffix == "shm") &&
					entry.Kind == "file" && !entry.Present && !entry.SQLite && !entry.Credentials
			}
		}
	}
	if !valid {
		return fmt.Errorf("local restore journal component %s has invalid semantics", entry.ID)
	}
	return nil
}

func commitPublicLocalRestore(ctx context.Context, journalPath string, journal localRestoreJournal) error {
	if journal.Phase != localRestorePhaseInstalling {
		return fmt.Errorf("cannot commit local restore journal in phase %q", journal.Phase)
	}
	for _, entry := range journal.Entries {
		if err := installLocalRestoreEntry(entry); err != nil {
			return err
		}
	}
	if err := validateInstalledPublicLocalRestore(ctx, journal); err != nil {
		journal.Phase = localRestorePhaseRollingBack
		if phaseErr := replaceLocalRestoreJournal(journalPath, journal); phaseErr != nil {
			return errors.Join(err, phaseErr)
		}
		return errors.Join(err, rollbackLocalRestore(journalPath, journal))
	}
	for _, entry := range journal.Entries {
		if err := os.RemoveAll(entry.Previous); err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(entry.Target)); err != nil {
			return err
		}
	}
	if err := os.Remove(journalPath); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(journalPath))
}

func validateInstalledPublicLocalRestore(ctx context.Context, journal localRestoreJournal) error {
	for _, entry := range journal.Entries {
		if !entry.Present {
			if _, err := os.Lstat(entry.Target); !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("restore component %s should be absent", entry.ID)
			}
			continue
		}
		if !matchesRestoreDigest(entry, entry.Target) {
			return fmt.Errorf("installed restore component %s failed its content digest", entry.ID)
		}
		if entry.SQLite {
			if err := quickCheckSQLitePath(ctx, entry.Target, "installed restored "+entry.ID); err != nil {
				return err
			}
		}
		if entry.Credentials {
			if _, found, err := readPublicCredentials(entry.Target); err != nil || !found {
				if err == nil {
					err = errors.New("caller credential is missing")
				}
				return err
			}
		}
	}
	return nil
}

func resumePublicLocalRestore(ctx context.Context, paths publicLocalBackupPaths) error {
	journalPath := localRestoreJournalPath(paths.ServiceDB)
	journal, err := readLocalRestoreJournal(journalPath)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("there is no interrupted local restore to resume")
	}
	if err != nil {
		return err
	}
	if err := validatePublicLocalRestoreJournal(paths, journal); err != nil {
		return err
	}
	switch journal.Phase {
	case localRestorePhaseInstalling:
		return commitPublicLocalRestore(ctx, journalPath, journal)
	case localRestorePhaseRollingBack:
		return rollbackLocalRestore(journalPath, journal)
	default:
		return fmt.Errorf("local restore journal has invalid phase %q", journal.Phase)
	}
}
