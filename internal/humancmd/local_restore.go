package humancmd

import (
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

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	localRestoreJournalVersion   = 4
	localRestorePhaseInstalling  = "installing"
	localRestorePhaseRollingBack = "rolling_back"
)

var localRestoreTransactionID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// localRestoreJournal is an internal, crash-recovery record. Version 4 names
// identities after the public Service deployment instead of the retired
// gateway credential-pair implementation.
type localRestoreJournal struct {
	Version        int                        `json:"version"`
	Phase          string                     `json:"phase"`
	TransactionID  string                     `json:"transaction_id"`
	WorkspaceScope string                     `json:"workspace_scope"`
	DeploymentID   string                     `json:"deployment_id"`
	CallerID       string                     `json:"caller_id"`
	WorkerID       string                     `json:"worker_id"`
	Entries        []localRestoreJournalEntry `json:"entries"`
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
			paths, err := resolvePublicLocalBackupPaths(settings)
			if err != nil {
				return err
			}
			locks, err := acquireStoppedPublicLocal(paths, true)
			if err != nil {
				return err
			}
			defer locks.Close()
			if resume {
				if strings.TrimSpace(input) != "" || force || acceptWorkspaceMismatch {
					return errors.New("--resume cannot be combined with --input, --force, or --accept-workspace-mismatch")
				}
				if err := resumePublicLocalRestore(command.Context(), paths); err != nil {
					return err
				}
				_, err = fmt.Fprintln(command.OutOrStdout(), "Human local restore recovery completed")
				return err
			}
			if strings.TrimSpace(input) == "" {
				return errors.New("backup input is required; use --input FILE (or --resume after an interrupted restore)")
			}
			if err := rejectPendingLocalRestore(paths.ServiceDB); err != nil {
				return err
			}
			archivePath, err := resolvePrivatePath(input, "backup input")
			if err != nil {
				return err
			}
			manifest, err := restorePublicLocalBackup(command.Context(), paths, archivePath, force, acceptWorkspaceMismatch)
			if err != nil {
				return err
			}
			if manifest.WorkspaceScope != paths.WorkspaceKey {
				if _, err := fmt.Fprintln(command.OutOrStdout(),
					"WARNING: archived in-flight task workspace/root identities were preserved; review them before continuing old file operations.",
				); err != nil {
					return err
				}
			}
			_, err = fmt.Fprintf(command.OutOrStdout(),
				"Human local backup restored\narchive: %s\ncreated: %s\nfiles: %d\n",
				archivePath, manifest.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), countPublicManifestFiles(manifest))
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

func writeLocalRestoreJournalWithSync(filename string, journal localRestoreJournal, syncDirectoryFn func(string) error) (bool, error) {
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

func replaceLocalRestoreJournalWithSync(filename string, journal localRestoreJournal, syncDirectoryFn func(string) error) error {
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

func localRestoreJournalPath(serviceDatabase string) string {
	return serviceDatabase + ".restore-journal.json"
}

func rejectPendingLocalRestore(serviceDatabase string) error {
	journalPath := localRestoreJournalPath(serviceDatabase)
	if _, err := os.Lstat(journalPath); err == nil {
		return fmt.Errorf("an interrupted local restore is pending at %s; keep Human local stopped and run `human local restore --workspace . --resume`", journalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect local restore journal: %w", err)
	}
	return nil
}
