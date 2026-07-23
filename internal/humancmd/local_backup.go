package humancmd

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	_ "modernc.org/sqlite"
)

// These bounds are shared by the public v3 archive reader and writer. They
// deliberately cap both allocation and extraction work before any restore data
// can reach a configured destination.
const (
	maxLocalBackupManifestSize = 4 << 20
	maxLocalBackupEntries      = 100_000
	maxLocalBackupPathBytes    = 1024
	maxLocalBackupReasonBytes  = 1024
	maxLocalBackupExtractBytes = 64 << 30
)

type localBackupManifestEntry struct {
	Path   string `json:"path"`
	Type   string `json:"type"`
	SHA256 string `json:"sha256,omitempty"`
	Size   int64  `json:"size"`
	Mode   uint32 `json:"mode"`
	SQLite bool   `json:"sqlite,omitempty"`
}

type localBackupSkipped struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

func newLocalBackupCommand(settings *viper.Viper) *cobra.Command {
	var output string
	var force bool
	command := &cobra.Command{
		Use:   "backup",
		Short: "create a verified offline backup of this local workspace scope",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			paths, err := resolvePublicLocalBackupPaths(settings)
			if err != nil {
				return err
			}
			if strings.TrimSpace(output) == "" {
				return errors.New("backup output is required; use --output FILE")
			}
			archivePath, err := resolvePrivatePath(output, "backup output")
			if err != nil {
				return err
			}
			summary, err := createPublicLocalBackup(command.Context(), paths, archivePath, force)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.OutOrStdout(),
				"Human local backup created\narchive: %s\nfiles: %d\nbytes: %d\nskipped mirror nodes: %d\n",
				archivePath, countPublicManifestFiles(summary.Manifest), summary.Bytes, len(summary.Manifest.Skipped))
			return err
		},
	}
	command.Flags().StringVarP(&output, "output", "o", "", "destination .tar.gz archive")
	command.Flags().BoolVar(&force, "force", false, "atomically replace an existing archive")
	return command
}

func newLocalVerifyBackupCommand() *cobra.Command {
	var input string
	command := &cobra.Command{
		Use:   "verify-backup",
		Short: "verify a Human local backup without restoring it",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if strings.TrimSpace(input) == "" {
				return errors.New("backup input is required; use --input FILE")
			}
			archivePath, err := resolvePrivatePath(input, "backup input")
			if err != nil {
				return err
			}
			temporary, err := os.MkdirTemp("", "human-local-backup-verify-*")
			if err != nil {
				return fmt.Errorf("create backup verification directory: %w", err)
			}
			defer os.RemoveAll(temporary)
			summary, err := extractAndVerifyPublicLocalBackup(command.Context(), archivePath, temporary)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.OutOrStdout(),
				"Human local backup is structurally valid and internally consistent\nwarning: verification does not authenticate the archive source\nformat: %s v%d\ncreated: %s\nfiles: %d\nbytes: %d\nskipped mirror nodes: %d\n",
				summary.Manifest.Format, summary.Manifest.Version,
				summary.Manifest.CreatedAt.UTC().Format(time.RFC3339Nano),
				countPublicManifestFiles(summary.Manifest), summary.Bytes, len(summary.Manifest.Skipped))
			return err
		},
	}
	command.Flags().StringVarP(&input, "input", "i", "", "source .tar.gz archive")
	return command
}

type localOfflineLocks struct {
	locks []*ownerlock.Lock
}

func (locks *localOfflineLocks) Close() error {
	if locks == nil {
		return nil
	}
	var errs []error
	for index := len(locks.locks) - 1; index >= 0; index-- {
		errs = append(errs, locks.locks[index].Close())
	}
	locks.locks = nil
	return errors.Join(errs...)
}

func snapshotSQLite(ctx context.Context, source, destination, label string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("%s must be a regular file", label)
	}
	location, err := sqlitefile.PreparePrivate(source, label)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return fmt.Errorf("create %s snapshot directory: %w", label, err)
	}
	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		return fmt.Errorf("open %s for backup: %w", label, err)
	}
	database.SetMaxOpenConns(1)
	closeWith := func(operationErr error) error { return errors.Join(operationErr, database.Close()) }
	if err := quickCheckOpenDatabase(ctx, database, label+" before backup"); err != nil {
		return closeWith(err)
	}
	if _, err := os.Lstat(destination); err == nil {
		return closeWith(fmt.Errorf("internal backup snapshot %s already exists", destination))
	} else if !errors.Is(err, os.ErrNotExist) {
		return closeWith(err)
	}
	statement := "VACUUM INTO '" + strings.ReplaceAll(destination, "'", "''") + "'"
	if _, err := database.ExecContext(ctx, statement); err != nil {
		return closeWith(fmt.Errorf("snapshot %s with SQLite: %w", label, err))
	}
	if err := database.Close(); err != nil {
		return fmt.Errorf("close %s after backup: %w", label, err)
	}
	if err := os.Chmod(destination, 0o600); err != nil {
		return fmt.Errorf("secure %s snapshot: %w", label, err)
	}
	if err := quickCheckSQLitePath(ctx, destination, label+" snapshot"); err != nil {
		return err
	}
	return syncFile(destination)
}

func quickCheckSQLitePath(ctx context.Context, databasePath, label string) error {
	database, err := sql.Open("sqlite", sqliteReadWriteDSN(databasePath))
	if err != nil {
		return fmt.Errorf("open %s for integrity check: %w", label, err)
	}
	database.SetMaxOpenConns(1)
	checkErr := quickCheckOpenDatabase(ctx, database, label)
	return errors.Join(checkErr, database.Close())
}

func quickCheckOpenDatabase(ctx context.Context, database *sql.DB, label string) error {
	rows, err := database.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		return fmt.Errorf("run SQLite quick_check on %s: %w", label, err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return fmt.Errorf("read SQLite quick_check for %s: %w", label, err)
		}
		count++
		if result != "ok" {
			return fmt.Errorf("SQLite quick_check failed for %s: %s", label, result)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("finish SQLite quick_check for %s: %w", label, err)
	}
	if count != 1 {
		return fmt.Errorf("SQLite quick_check for %s returned %d rows, want exactly one ok", label, count)
	}
	return nil
}

func sqliteReadWriteDSN(databasePath string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(databasePath), RawQuery: "mode=rw"}).String()
}

func copyStableRegular(source, destination string, mode fs.FileMode) error {
	before, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return errors.New("source is not a regular file")
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	opened, err := input.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return errors.New("source changed while opening")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	copyErr := func() error {
		if _, err := io.Copy(output, input); err != nil {
			return err
		}
		return output.Sync()
	}()
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(destination)
		return errors.Join(copyErr, closeErr)
	}
	after, err := input.Stat()
	if err != nil || after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		_ = os.Remove(destination)
		return errors.New("source changed while copying")
	}
	return nil
}

func copyMirrorTree(source, destination, logical string, skipped *[]localBackupSkipped) error {
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return fmt.Errorf("create mirror backup root: %w", err)
	}
	rootInfo, err := os.Lstat(source)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect mirror backup root %s: %w", source, err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return fmt.Errorf("mirror backup root %s must be a directory, not a symlink or special file", source)
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
		archivePath := path.Join(logical, filepath.ToSlash(relative))
		info, err := entry.Info()
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			*skipped = append(*skipped, localBackupSkipped{Path: archivePath, Reason: "symbolic link is not part of the deliverable mirror correctness tree"})
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		case info.IsDir():
			return os.MkdirAll(target, info.Mode().Perm())
		case info.Mode().IsRegular():
			if err := copyStableRegular(current, target, info.Mode().Perm()); err != nil {
				return fmt.Errorf("copy mirror file %s: %w", current, err)
			}
			return nil
		default:
			*skipped = append(*skipped, localBackupSkipped{Path: archivePath, Reason: "special file is not part of the deliverable mirror correctness tree"})
			return nil
		}
	})
}

func portableFilesystemPath(value string) (string, error) {
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	return cases.Fold().String(norm.NFC.String(filepath.Clean(absolute))), nil
}

func pathAtOrBelow(root, candidate string) (bool, error) {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return false, err
	}
	candidateAbsolute, err := filepath.Abs(candidate)
	if err != nil {
		return false, err
	}
	relative, err := filepath.Rel(rootAbsolute, candidateAbsolute)
	if err != nil {
		return false, err
	}
	return relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)), nil
}

func validateArchivePath(value string) error {
	if value == "" || len(value) > maxLocalBackupPathBytes || value == "." || strings.Contains(value, "\\") || strings.ContainsRune(value, '\x00') ||
		!fs.ValidPath(value) || path.Clean(value) != value || strings.HasPrefix(value, "/") {
		return fmt.Errorf("backup path %q is unsafe", value)
	}
	return nil
}

func secureArchiveTarget(root, logical string) (string, error) {
	if err := validateArchivePath(logical); err != nil {
		return "", err
	}
	target := filepath.Join(root, filepath.FromSlash(logical))
	relative, err := filepath.Rel(root, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("backup path %q escapes extraction root", logical)
	}
	return target, nil
}

func digestFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	_, copyErr := io.Copy(hasher, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return "", errors.Join(copyErr, closeErr)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func syncFile(filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(syncErr, closeErr)
}

func syncDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	syncErr := handle.Sync()
	closeErr := handle.Close()
	return errors.Join(syncErr, closeErr)
}
