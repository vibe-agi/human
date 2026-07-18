package humancmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	localRestoreJournalVersion   = 3
	localRestorePhaseInstalling  = "installing"
	localRestorePhaseRollingBack = "rolling_back"
)

var localRestoreTransactionID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type localRestoreJournal struct {
	Version         int                        `json:"version"`
	Phase           string                     `json:"phase"`
	TransactionID   string                     `json:"transaction_id"`
	WorkspaceScope  string                     `json:"workspace_scope"`
	GatewayIdentity string                     `json:"gateway_identity"`
	CallerSubject   string                     `json:"caller_subject"`
	WorkerSubject   string                     `json:"worker_subject"`
	Entries         []localRestoreJournalEntry `json:"entries"`
}

type localRestoreJournalEntry struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Target      string `json:"target"`
	Staged      string `json:"staged,omitempty"`
	Previous    string `json:"previous"`
	Present     bool   `json:"present"`
	HadPrevious bool   `json:"had_previous"`
	Digest      string `json:"digest,omitempty"`
	SQLite      bool   `json:"sqlite,omitempty"`
	Credentials bool   `json:"credentials,omitempty"`
}

func newLocalRestoreCommand(settings *viper.Viper) *cobra.Command {
	var input string
	var force bool
	var resume bool
	var acceptWorkspaceMismatch bool
	command := &cobra.Command{
		Use:   "restore",
		Short: "restore a verified offline backup as one crash-recoverable local state set",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			paths, err := resolveLocalBackupPaths(settings)
			if err != nil {
				return err
			}
			locks, err := acquireStoppedLocal(paths, true)
			if err != nil {
				return err
			}
			defer locks.Close()
			if resume {
				if strings.TrimSpace(input) != "" || force || acceptWorkspaceMismatch {
					return errors.New("--resume cannot be combined with --input, --force, or --accept-workspace-mismatch")
				}
				if err := resumeLocalRestore(command.Context(), paths); err != nil {
					return err
				}
				_, err = fmt.Fprintln(command.OutOrStdout(), "Human local restore recovery completed")
				return err
			}
			if strings.TrimSpace(input) == "" {
				return errors.New("backup input is required; use --input FILE (or --resume after an interrupted restore)")
			}
			if err := rejectPendingLocalRestore(paths.GatewayDB); err != nil {
				return err
			}
			archivePath, err := resolvePrivatePath(input, "backup input")
			if err != nil {
				return err
			}
			manifest, err := restoreLocalBackup(command.Context(), paths, archivePath, force, acceptWorkspaceMismatch)
			if err != nil {
				return err
			}
			if manifest.WorkspaceScope != paths.WorkspaceKey {
				if _, err := fmt.Fprintln(command.OutOrStdout(),
					"WARNING: transport identity was rebound, but archived in-flight task workspace/root identities were preserved; review them before continuing old file operations.",
				); err != nil {
					return err
				}
			}
			_, err = fmt.Fprintf(command.OutOrStdout(),
				"Human local backup restored\narchive: %s\ncreated: %s\nfiles: %d\n",
				archivePath, manifest.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), countManifestFiles(manifest))
			return err
		},
	}
	command.Flags().StringVarP(&input, "input", "i", "", "source .tar.gz archive")
	command.Flags().BoolVar(&force, "force", false, "replace non-empty local databases, credentials, and selected mirror workspace")
	command.Flags().BoolVar(&resume, "resume", false, "complete an interrupted restore from its durable journal")
	command.Flags().BoolVar(&acceptWorkspaceMismatch, "accept-workspace-mismatch", false,
		"acknowledge a verified same-workspace relocation; archived in-flight workspace/root identities are not rewritten")
	return command
}

func restoreLocalBackup(ctx context.Context, paths localBackupPaths, archivePath string, force, acceptWorkspaceMismatch bool) (localBackupManifest, error) {
	if err := validateLocalBackupPathSet(paths); err != nil {
		return localBackupManifest{}, err
	}
	if err := rejectArchiveStateCollision(paths, archivePath); err != nil {
		return localBackupManifest{}, err
	}
	extracted, err := os.MkdirTemp("", "human-local-restore-verify-*")
	if err != nil {
		return localBackupManifest{}, fmt.Errorf("create restore verification directory: %w", err)
	}
	defer os.RemoveAll(extracted)
	summary, err := extractAndVerifyLocalBackup(ctx, archivePath, extracted)
	if err != nil {
		return localBackupManifest{}, err
	}
	manifest := summary.Manifest
	if manifest.WorkspaceScope != paths.WorkspaceKey && !acceptWorkspaceMismatch {
		return localBackupManifest{}, fmt.Errorf(
			"backup belongs to workspace scope %s, current scope is %s; use --accept-workspace-mismatch only after verifying the destination",
			manifest.WorkspaceScope, paths.WorkspaceKey,
		)
	}
	if manifest.CallerSubject != paths.CallerSubject {
		return localBackupManifest{}, fmt.Errorf(
			"backup caller subject %q does not match configured local caller subject %q",
			manifest.CallerSubject, paths.CallerSubject,
		)
	}
	if manifest.WorkerSubject != paths.WorkerSubject {
		return localBackupManifest{}, fmt.Errorf(
			"backup worker subject %q does not match configured local worker subject %q",
			manifest.WorkerSubject, paths.WorkerSubject,
		)
	}
	if manifest.StateEnabled && paths.StateDB == "" {
		return localBackupManifest{}, errors.New("backup contains worker state; configure a non-empty --state-db destination")
	}

	journal, err := prepareLocalRestore(ctx, paths, manifest, extracted, force)
	if err != nil {
		return localBackupManifest{}, err
	}
	journalPath := localRestoreJournalPath(paths.GatewayDB)
	published, err := writeLocalRestoreJournal(journalPath, journal)
	if err != nil {
		if !published {
			removeLocalRestoreStages(journal)
		}
		return localBackupManifest{}, err
	}
	if err := commitLocalRestore(ctx, journalPath, journal); err != nil {
		return localBackupManifest{}, fmt.Errorf("commit local restore (run `human local restore --workspace . --resume` after correcting the reported condition): %w", err)
	}
	if _, found, err := readLocalCredentials(paths.Credentials); err != nil || !found {
		if err == nil {
			err = errors.New("restored credentials are missing")
		}
		return localBackupManifest{}, err
	}
	return manifest, nil
}

func prepareLocalRestore(ctx context.Context, paths localBackupPaths, manifest localBackupManifest, extracted string, force bool) (localRestoreJournal, error) {
	targetGatewayIdentity, err := localGatewayIdentity(paths.GatewayDB)
	if err != nil {
		return localRestoreJournal{}, fmt.Errorf("resolve restore gateway identity: %w", err)
	}
	if err := rebindLocalWorkerBinding(
		ctx,
		filepath.Join(extracted, "worker", "worker-outbox.db"),
		filepath.Join(extracted, "worker", "worker-state.db"),
		manifest.StateEnabled,
		manifest.GatewayIdentity, targetGatewayIdentity, manifest.WorkerSubject,
	); err != nil {
		return localRestoreJournal{}, fmt.Errorf("prepare restored worker identity: %w", err)
	}
	txID := uuid.NewString()
	journal := localRestoreJournal{
		Version: localRestoreJournalVersion, Phase: localRestorePhaseInstalling, TransactionID: txID,
		WorkspaceScope: paths.WorkspaceKey, GatewayIdentity: targetGatewayIdentity,
		CallerSubject: paths.CallerSubject, WorkerSubject: paths.WorkerSubject,
	}
	components := []struct {
		id, kind, logical, target    string
		present, sqlite, credentials bool
	}{
		{"gateway", "file", "gateway/gateway.db", paths.GatewayDB, true, true, false},
		{"credentials", "file", "credentials/credentials.json", paths.Credentials, true, false, true},
		{"outbox", "file", "worker/worker-outbox.db", paths.OutboxDB, true, true, false},
		{
			"mirror-workspace", "directory", "mirror/workspace",
			filepath.Join(paths.MirrorRoot, paths.CallerSubject, paths.WorkspaceKey), true, false, false,
		},
		{
			"mirror-state", "directory", "mirror/state",
			filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject, paths.WorkspaceKey), true, false, false,
		},
	}
	if paths.StateDB != "" {
		components = append(components, struct {
			id, kind, logical, target    string
			present, sqlite, credentials bool
		}{"state", "file", "worker/worker-state.db", paths.StateDB, manifest.StateEnabled, manifest.StateEnabled, false})
	}
	databaseTargets := []struct{ id, target string }{{"gateway", paths.GatewayDB}, {"outbox", paths.OutboxDB}}
	if paths.StateDB != "" {
		databaseTargets = append(databaseTargets, struct{ id, target string }{"state", paths.StateDB})
	}
	for _, database := range databaseTargets {
		for _, suffix := range []string{"journal", "wal", "shm"} {
			components = append(components, struct {
				id, kind, logical, target    string
				present, sqlite, credentials bool
			}{database.id + "-" + suffix, "file", "", database.target + "-" + suffix, false, false, false})
		}
	}

	for _, component := range components {
		var targetDirectoryErr error
		switch component.id {
		case "mirror-workspace", "mirror-state":
			targetDirectoryErr = ensureLocalMirrorCallerRoot(filepath.Dir(component.target))
		default:
			targetDirectoryErr = os.MkdirAll(filepath.Dir(component.target), 0o700)
		}
		if targetDirectoryErr != nil {
			removeLocalRestoreStages(journal)
			return localRestoreJournal{}, fmt.Errorf("create restore target directory: %w", targetDirectoryErr)
		}
		if component.credentials && runtime.GOOS != "windows" {
			info, err := os.Stat(filepath.Dir(component.target))
			if err != nil || info.Mode().Perm()&0o077 != 0 {
				removeLocalRestoreStages(journal)
				return localRestoreJournal{}, fmt.Errorf("credential restore directory %s must have mode 0700", filepath.Dir(component.target))
			}
		}
		hadPrevious, nonempty, err := inspectRestoreTarget(component.target, component.kind)
		if err != nil {
			removeLocalRestoreStages(journal)
			return localRestoreJournal{}, err
		}
		if nonempty && !force {
			removeLocalRestoreStages(journal)
			return localRestoreJournal{}, fmt.Errorf("restore target %s is non-empty; inspect it and use --force to replace the complete local state set", component.target)
		}
		base := filepath.Base(component.target)
		entry := localRestoreJournalEntry{
			ID: component.id, Kind: component.kind, Target: component.target,
			Previous: filepath.Join(filepath.Dir(component.target), ".human-restore-"+txID+"-old-"+base),
			Present:  component.present, HadPrevious: hadPrevious,
			SQLite: component.sqlite, Credentials: component.credentials,
		}
		if component.present {
			entry.Staged = filepath.Join(filepath.Dir(component.target), ".human-restore-"+txID+"-new-"+base)
		}
		journal.Entries = append(journal.Entries, entry)
		current := &journal.Entries[len(journal.Entries)-1]
		if component.present {
			source := filepath.Join(extracted, filepath.FromSlash(component.logical))
			switch component.kind {
			case "file":
				mode := fs.FileMode(0o600)
				if err := copyStableRegular(source, current.Staged, mode); err != nil {
					removeLocalRestoreStages(journal)
					return localRestoreJournal{}, fmt.Errorf("stage restored %s: %w", component.id, err)
				}
				current.Digest, err = digestFile(current.Staged)
			case "directory":
				err = copyRestoreDirectory(source, current.Staged)
				if err == nil {
					current.Digest, err = digestRestoreTree(current.Staged)
				}
			}
			if err != nil {
				removeLocalRestoreStages(journal)
				return localRestoreJournal{}, fmt.Errorf("stage restored %s: %w", component.id, err)
			}
			if current.SQLite {
				if err := quickCheckSQLitePath(ctx, current.Staged, "staged restored "+component.id); err != nil {
					removeLocalRestoreStages(journal)
					return localRestoreJournal{}, err
				}
			}
			if current.Credentials {
				if _, found, err := readLocalCredentials(current.Staged); err != nil || !found {
					removeLocalRestoreStages(journal)
					if err == nil {
						err = errors.New("credential journal is missing")
					}
					return localRestoreJournal{}, fmt.Errorf("validate staged restored credentials: %w", err)
				}
			}
			if err := syncLocalRestoreStage(*current); err != nil {
				removeLocalRestoreStages(journal)
				return localRestoreJournal{}, fmt.Errorf("sync staged restored %s: %w", component.id, err)
			}
		}
	}
	if err := validateLocalRestoreJournal(paths, journal); err != nil {
		removeLocalRestoreStages(journal)
		return localRestoreJournal{}, err
	}
	return journal, nil
}

func inspectRestoreTarget(target, kind string) (exists, nonempty bool, err error) {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("inspect restore target %s: %w", target, err)
	}
	switch kind {
	case "file":
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return true, true, fmt.Errorf("restore file target %s is a symlink or special file", target)
		}
		return true, info.Size() > 0, nil
	case "directory":
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return true, true, fmt.Errorf("restore directory target %s is a symlink or special file", target)
		}
		entries, err := os.ReadDir(target)
		return true, len(entries) > 0, err
	default:
		return false, false, fmt.Errorf("unknown restore target kind %q", kind)
	}
}

func copyRestoreDirectory(source, destination string) error {
	info, err := os.Lstat(source)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("restored directory source is missing or unsafe")
	}
	if err := os.Mkdir(destination, info.Mode().Perm()); err != nil {
		return err
	}
	return filepath.WalkDir(source, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if current == source {
			return nil
		}
		relative, err := filepath.Rel(source, current)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || (!info.IsDir() && !info.Mode().IsRegular()) {
			return fmt.Errorf("verified backup extraction unexpectedly contains a special node at %s", relative)
		}
		if info.IsDir() {
			return os.Mkdir(target, info.Mode().Perm())
		}
		return copyStableRegular(current, target, info.Mode().Perm())
	})
}

func digestRestoreTree(root string) (string, error) {
	type item struct {
		path, kind, digest string
		size               int64
		mode               fs.FileMode
	}
	var items []item
	err := filepath.WalkDir(root, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if current == root {
			return nil
		}
		relative, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		value := item{path: filepath.ToSlash(relative), mode: info.Mode().Perm()}
		if info.IsDir() {
			value.kind = "directory"
		} else if info.Mode().IsRegular() {
			value.kind = "file"
			value.size = info.Size()
			value.digest, err = digestFile(current)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("restore tree contains special path %s", relative)
		}
		items = append(items, value)
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].path < items[j].path })
	hasher := sha256.New()
	for _, item := range items {
		fmt.Fprintf(hasher, "%s\x00%s\x00%o\x00%d\x00%s\n", item.kind, item.path, item.mode, item.size, item.digest)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func createLocalRestoreJournalTemporary(directory string, journal localRestoreJournal) (string, error) {
	payload, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode local restore journal: %w", err)
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(directory, ".human-local-restore-journal-*")
	if err != nil {
		return "", fmt.Errorf("create local restore journal: %w", err)
	}
	temporaryPath := temporary.Name()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	keep = true
	return temporaryPath, nil
}

func writeLocalRestoreJournal(filename string, journal localRestoreJournal) (bool, error) {
	return writeLocalRestoreJournalWithSync(filename, journal, syncDirectory)
}

func writeLocalRestoreJournalWithSync(
	filename string,
	journal localRestoreJournal,
	syncDirectoryFn func(string) error,
) (bool, error) {
	directory := filepath.Dir(filename)
	temporaryPath, err := createLocalRestoreJournalTemporary(directory, journal)
	if err != nil {
		return false, err
	}
	defer os.Remove(temporaryPath)
	if err := os.Link(temporaryPath, filename); err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, errors.New("a local restore journal already exists; run `human local restore --resume`")
		}
		return false, fmt.Errorf("publish local restore journal: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return true, err
	}
	if err := syncDirectoryFn(directory); err != nil {
		return true, err
	}
	return true, nil
}

func replaceLocalRestoreJournal(filename string, journal localRestoreJournal) error {
	return replaceLocalRestoreJournalWithSync(filename, journal, syncDirectory)
}

func replaceLocalRestoreJournalWithSync(
	filename string,
	journal localRestoreJournal,
	syncDirectoryFn func(string) error,
) error {
	directory := filepath.Dir(filename)
	temporaryPath, err := createLocalRestoreJournalTemporary(directory, journal)
	if err != nil {
		return err
	}
	defer os.Remove(temporaryPath)
	if err := replaceFileAtomically(temporaryPath, filename); err != nil {
		return fmt.Errorf("replace local restore journal: %w", err)
	}
	return syncDirectoryFn(directory)
}

func readLocalRestoreJournal(filename string) (localRestoreJournal, error) {
	const maxJournalBytes = 1 << 20
	info, err := os.Lstat(filename)
	if err != nil {
		return localRestoreJournal{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > maxJournalBytes {
		return localRestoreJournal{}, errors.New("local restore journal is not a bounded regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return localRestoreJournal{}, errors.New("local restore journal must have mode 0600")
	}
	file, err := os.Open(filename)
	if err != nil {
		return localRestoreJournal{}, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() != info.Size() {
		return localRestoreJournal{}, errors.New("local restore journal changed while opening")
	}
	decoder := json.NewDecoder(io.LimitReader(file, maxJournalBytes+1))
	decoder.DisallowUnknownFields()
	var journal localRestoreJournal
	if err := decoder.Decode(&journal); err != nil {
		return localRestoreJournal{}, fmt.Errorf("decode local restore journal: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return localRestoreJournal{}, errors.New("local restore journal contains trailing data")
	}
	return journal, nil
}

func commitLocalRestore(ctx context.Context, journalPath string, journal localRestoreJournal) error {
	if journal.Phase != localRestorePhaseInstalling {
		return fmt.Errorf("cannot commit local restore journal in phase %q", journal.Phase)
	}
	for _, entry := range journal.Entries {
		if err := installLocalRestoreEntry(entry); err != nil {
			return err
		}
	}
	if err := validateInstalledLocalRestore(ctx, journal); err != nil {
		journal.Phase = localRestorePhaseRollingBack
		if phaseErr := replaceLocalRestoreJournal(journalPath, journal); phaseErr != nil {
			return errors.Join(err, fmt.Errorf("record local restore rollback phase: %w", phaseErr))
		}
		rollbackErr := rollbackLocalRestore(journalPath, journal)
		return errors.Join(err, rollbackErr)
	}
	for _, entry := range journal.Entries {
		if err := os.RemoveAll(entry.Previous); err != nil {
			return fmt.Errorf("remove prior restore component %s: %w", entry.ID, err)
		}
		if err := syncDirectory(filepath.Dir(entry.Target)); err != nil {
			return err
		}
	}
	if err := os.Remove(journalPath); err != nil {
		return fmt.Errorf("remove committed local restore journal: %w", err)
	}
	return syncDirectory(filepath.Dir(journalPath))
}

func installLocalRestoreEntry(entry localRestoreJournalEntry) error {
	if !entry.Present {
		if _, err := os.Lstat(entry.Previous); err == nil {
			if _, targetErr := os.Lstat(entry.Target); !errors.Is(targetErr, os.ErrNotExist) {
				return fmt.Errorf("restore component %s has both previous and target paths", entry.ID)
			}
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, err := os.Lstat(entry.Target); err == nil {
			if !entry.HadPrevious {
				return fmt.Errorf("new data appeared at absent restore target %s", entry.Target)
			}
			if err := os.Rename(entry.Target, entry.Previous); err != nil {
				return fmt.Errorf("park previous restore component %s: %w", entry.ID, err)
			}
			return syncDirectory(filepath.Dir(entry.Target))
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}

	_, stageErr := os.Lstat(entry.Staged)
	stageExists := stageErr == nil
	if stageErr != nil && !errors.Is(stageErr, os.ErrNotExist) {
		return stageErr
	}
	_, previousErr := os.Lstat(entry.Previous)
	previousExists := previousErr == nil
	if previousErr != nil && !errors.Is(previousErr, os.ErrNotExist) {
		return previousErr
	}
	_, targetErr := os.Lstat(entry.Target)
	targetExists := targetErr == nil
	if targetErr != nil && !errors.Is(targetErr, os.ErrNotExist) {
		return targetErr
	}

	if stageExists {
		if targetExists && !previousExists {
			if entry.HadPrevious {
				if err := os.Rename(entry.Target, entry.Previous); err != nil {
					return fmt.Errorf("park previous restore component %s: %w", entry.ID, err)
				}
				targetExists = false
				previousExists = true
			} else if matchesRestoreDigest(entry, entry.Target) {
				if err := os.RemoveAll(entry.Staged); err != nil {
					return err
				}
				return nil
			} else {
				return fmt.Errorf("new data appeared at restore target %s", entry.Target)
			}
		}
		if targetExists && previousExists {
			if !matchesRestoreDigest(entry, entry.Target) {
				return fmt.Errorf("restore target %s does not match staged data", entry.Target)
			}
			return os.RemoveAll(entry.Staged)
		}
		if err := os.Rename(entry.Staged, entry.Target); err != nil {
			return fmt.Errorf("install restore component %s: %w", entry.ID, err)
		}
		return syncDirectory(filepath.Dir(entry.Target))
	}
	if !targetExists || !matchesRestoreDigest(entry, entry.Target) {
		return fmt.Errorf("restore component %s lost both its staged and installed data", entry.ID)
	}
	return nil
}

func matchesRestoreDigest(entry localRestoreJournalEntry, filename string) bool {
	var digest string
	var err error
	if entry.Kind == "file" {
		digest, err = digestFile(filename)
	} else {
		digest, err = digestRestoreTree(filename)
	}
	return err == nil && digest == entry.Digest
}

func validateInstalledLocalRestore(ctx context.Context, journal localRestoreJournal) error {
	var gatewayPath, credentialPath string
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
			credentialPath = entry.Target
			if _, found, err := readLocalCredentials(entry.Target); err != nil || !found {
				if err == nil {
					err = errors.New("credential journal is missing")
				}
				return fmt.Errorf("validate installed restored credentials: %w", err)
			}
		}
		if entry.ID == "gateway" {
			gatewayPath = entry.Target
		}
	}
	if err := validateLocalCredentialBinding(ctx, gatewayPath, credentialPath, journal.CallerSubject, journal.WorkerSubject); err != nil {
		return fmt.Errorf("validate installed gateway/credential binding: %w", err)
	}
	return nil
}

func rollbackLocalRestore(journalPath string, journal localRestoreJournal) error {
	var errs []error
	for index := len(journal.Entries) - 1; index >= 0; index-- {
		entry := journal.Entries[index]
		if err := rollbackLocalRestoreEntry(entry); err != nil {
			errs = append(errs, fmt.Errorf("roll back restore component %s: %w", entry.ID, err))
		}
	}
	if len(errs) == 0 {
		if err := os.Remove(journalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		} else if err := syncDirectory(filepath.Dir(journalPath)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func rollbackLocalRestoreEntry(entry localRestoreJournalEntry) error {
	_, previousErr := os.Lstat(entry.Previous)
	if previousErr != nil && !errors.Is(previousErr, os.ErrNotExist) {
		return previousErr
	}
	previousExists := previousErr == nil
	stagedExists := false
	if entry.Staged != "" {
		_, stagedErr := os.Lstat(entry.Staged)
		if stagedErr != nil && !errors.Is(stagedErr, os.ErrNotExist) {
			return stagedErr
		}
		stagedExists = stagedErr == nil
	}
	if previousExists {
		if err := os.RemoveAll(entry.Target); err != nil {
			return err
		}
		if err := os.Rename(entry.Previous, entry.Target); err != nil {
			return err
		}
	} else if !entry.HadPrevious && entry.Present && !stagedExists {
		if err := os.RemoveAll(entry.Target); err != nil {
			return err
		}
	}
	if entry.Staged != "" {
		if err := os.RemoveAll(entry.Staged); err != nil {
			return err
		}
	}
	return syncDirectory(filepath.Dir(entry.Target))
}

func resumeLocalRestore(ctx context.Context, paths localBackupPaths) error {
	journalPath := localRestoreJournalPath(paths.GatewayDB)
	journal, err := readLocalRestoreJournal(journalPath)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("there is no interrupted local restore to resume")
	}
	if err != nil {
		return err
	}
	if err := validateLocalRestoreJournal(paths, journal); err != nil {
		return err
	}
	switch journal.Phase {
	case localRestorePhaseInstalling:
		return commitLocalRestore(ctx, journalPath, journal)
	case localRestorePhaseRollingBack:
		return rollbackLocalRestore(journalPath, journal)
	default:
		return fmt.Errorf("local restore journal has invalid phase %q", journal.Phase)
	}
}

func validateLocalRestoreJournal(paths localBackupPaths, journal localRestoreJournal) error {
	if journal.Version != localRestoreJournalVersion || !localRestoreTransactionID.MatchString(journal.TransactionID) {
		return errors.New("local restore journal has an unsupported version or transaction ID")
	}
	if journal.Phase != localRestorePhaseInstalling && journal.Phase != localRestorePhaseRollingBack {
		return fmt.Errorf("local restore journal has invalid phase %q", journal.Phase)
	}
	expectedGatewayIdentity, err := localGatewayIdentity(paths.GatewayDB)
	if err != nil {
		return fmt.Errorf("resolve local restore journal gateway identity: %w", err)
	}
	if journal.WorkspaceScope != paths.WorkspaceKey || journal.GatewayIdentity != expectedGatewayIdentity ||
		journal.CallerSubject != paths.CallerSubject || journal.WorkerSubject != paths.WorkerSubject {
		return errors.New("local restore journal does not belong to the selected workspace and caller")
	}
	for _, callerRoot := range []string{
		filepath.Join(paths.MirrorRoot, paths.CallerSubject),
		filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject),
	} {
		if err := validateLocalMirrorCallerRoot(callerRoot, false); err != nil {
			return err
		}
	}
	allowed := map[string]string{
		"gateway": paths.GatewayDB, "credentials": paths.Credentials, "outbox": paths.OutboxDB,
		"mirror-workspace": filepath.Join(paths.MirrorRoot, paths.CallerSubject, paths.WorkspaceKey),
		"mirror-state":     filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject, paths.WorkspaceKey),
	}
	if paths.StateDB != "" {
		allowed["state"] = paths.StateDB
	}
	for id, database := range map[string]string{"gateway": paths.GatewayDB, "outbox": paths.OutboxDB, "state": paths.StateDB} {
		if database == "" {
			continue
		}
		for _, suffix := range []string{"journal", "wal", "shm"} {
			allowed[id+"-"+suffix] = database + "-" + suffix
		}
	}
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
		if entry.Previous != expectedPrevious || (entry.Present && entry.Staged != filepath.Join(filepath.Dir(target), ".human-restore-"+journal.TransactionID+"-new-"+base)) || (!entry.Present && entry.Staged != "") {
			return fmt.Errorf("local restore journal staging paths for %s are invalid", entry.ID)
		}
		if entry.Kind != "file" && entry.Kind != "directory" {
			return fmt.Errorf("local restore journal kind for %s is invalid", entry.ID)
		}
		if entry.Present && len(entry.Digest) != sha256.Size*2 {
			return fmt.Errorf("local restore journal digest for %s is invalid", entry.ID)
		}
		if entry.Present {
			if _, err := hex.DecodeString(entry.Digest); err != nil {
				return fmt.Errorf("local restore journal digest for %s is invalid", entry.ID)
			}
		} else if entry.Digest != "" {
			return fmt.Errorf("absent restore component %s must not carry a digest", entry.ID)
		}
		if err := validateRestoreEntrySemantics(entry); err != nil {
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
		if _, ok := seen[id]; !ok {
			return fmt.Errorf("local restore journal is missing component %s", id)
		}
	}
	return nil
}

func validateRestoreEntrySemantics(entry localRestoreJournalEntry) error {
	valid := false
	switch entry.ID {
	case "gateway", "outbox":
		valid = entry.Kind == "file" && entry.Present && entry.SQLite && !entry.Credentials
	case "credentials":
		valid = entry.Kind == "file" && entry.Present && !entry.SQLite && entry.Credentials
	case "mirror-workspace", "mirror-state":
		valid = entry.Kind == "directory" && entry.Present && !entry.SQLite && !entry.Credentials
	case "state":
		valid = entry.Kind == "file" && entry.SQLite == entry.Present && !entry.Credentials
	default:
		for _, prefix := range []string{"gateway-", "outbox-", "state-"} {
			if strings.HasPrefix(entry.ID, prefix) {
				suffix := strings.TrimPrefix(entry.ID, prefix)
				valid = (suffix == "journal" || suffix == "wal" || suffix == "shm") &&
					entry.Kind == "file" && !entry.Present && !entry.SQLite && !entry.Credentials
				break
			}
		}
	}
	if !valid {
		return fmt.Errorf("local restore journal component %s has invalid kind/presence/security semantics", entry.ID)
	}
	return nil
}

func syncLocalRestoreStage(entry localRestoreJournalEntry) error {
	if !entry.Present {
		return nil
	}
	if entry.Kind == "file" {
		return syncDirectory(filepath.Dir(entry.Staged))
	}
	var directories []string
	err := filepath.WalkDir(entry.Staged, func(current string, item fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if item.IsDir() {
			directories = append(directories, current)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(directories, func(i, j int) bool { return len(directories[i]) > len(directories[j]) })
	for _, directory := range directories {
		if err := syncDirectory(directory); err != nil {
			return err
		}
	}
	return syncDirectory(filepath.Dir(entry.Staged))
}

func validateRestorePathShape(entry localRestoreJournalEntry, candidate string) error {
	info, err := os.Lstat(candidate)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect restore component %s path %s: %w", entry.ID, candidate, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("restore component %s path %s is a symbolic link", entry.ID, candidate)
	}
	if entry.Kind == "file" {
		if !info.Mode().IsRegular() || runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			return fmt.Errorf("restore component %s path %s must be a mode-0600 regular file", entry.ID, candidate)
		}
		return nil
	}
	if !info.IsDir() || runtime.GOOS != "windows" && info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("restore component %s path %s must be a non-writable-by-others directory", entry.ID, candidate)
	}
	return nil
}

func removeLocalRestoreStages(journal localRestoreJournal) {
	for _, entry := range journal.Entries {
		if entry.Staged != "" {
			_ = os.RemoveAll(entry.Staged)
		}
	}
}

func localRestoreJournalPath(gatewayDatabase string) string {
	return gatewayDatabase + ".restore-journal.json"
}

func rejectPendingLocalRestore(gatewayDatabase string) error {
	journalPath := localRestoreJournalPath(gatewayDatabase)
	if _, err := os.Lstat(journalPath); err == nil {
		return fmt.Errorf("an interrupted local restore is pending at %s; keep Human local stopped and run `human local restore --workspace . --resume`", journalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect local restore journal: %w", err)
	}
	return nil
}
