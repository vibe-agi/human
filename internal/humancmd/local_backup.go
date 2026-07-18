package humancmd

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"github.com/vibe-agi/human/internal/userdata"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerstate"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	_ "modernc.org/sqlite"
)

const (
	localBackupFormat          = "human-local-backup"
	localBackupVersion         = 2
	localBackupManifestPath    = "manifest.json"
	maxLocalBackupManifestSize = 4 << 20
	maxLocalBackupEntries      = 100_000
	maxLocalBackupPathBytes    = 1024
	maxLocalBackupReasonBytes  = 1024
	maxLocalBackupExtractBytes = 64 << 30
)

type localBackupPaths struct {
	WorkspaceRoot string
	WorkspaceKey  string
	GatewayDB     string
	Credentials   string
	MirrorRoot    string
	OutboxDB      string
	StateDB       string
	CallerSubject string
	WorkerSubject string
}

type localBackupManifest struct {
	Format          string                     `json:"format"`
	Version         int                        `json:"version"`
	CreatedAt       time.Time                  `json:"created_at"`
	WorkspaceScope  string                     `json:"workspace_scope"`
	GatewayIdentity string                     `json:"gateway_identity"`
	CallerSubject   string                     `json:"caller_subject"`
	WorkerSubject   string                     `json:"worker_subject"`
	StateEnabled    bool                       `json:"state_enabled"`
	Entries         []localBackupManifestEntry `json:"entries"`
	Skipped         []localBackupSkipped       `json:"skipped,omitempty"`
}

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

type localBackupSummary struct {
	Manifest localBackupManifest
	Bytes    int64
}

func newLocalBackupCommand(settings *viper.Viper) *cobra.Command {
	var output string
	var force bool
	command := &cobra.Command{
		Use:   "backup",
		Short: "create a verified offline backup of this local workspace scope",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			paths, err := resolveLocalBackupPaths(settings)
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
			summary, err := createLocalBackup(command.Context(), paths, archivePath, force)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.OutOrStdout(),
				"Human local backup created\narchive: %s\nfiles: %d\nbytes: %d\nskipped mirror nodes: %d\n",
				archivePath, countManifestFiles(summary.Manifest), summary.Bytes, len(summary.Manifest.Skipped))
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
			summary, err := extractAndVerifyLocalBackup(command.Context(), archivePath, temporary)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.OutOrStdout(),
				"Human local backup is structurally valid and internally consistent\nwarning: verification does not authenticate the archive source\nformat: %s v%d\ncreated: %s\nfiles: %d\nbytes: %d\nskipped mirror nodes: %d\n",
				summary.Manifest.Format, summary.Manifest.Version,
				summary.Manifest.CreatedAt.UTC().Format(time.RFC3339Nano),
				countManifestFiles(summary.Manifest), summary.Bytes, len(summary.Manifest.Skipped))
			return err
		},
	}
	command.Flags().StringVarP(&input, "input", "i", "", "source .tar.gz archive")
	return command
}

func resolveLocalBackupPaths(settings *viper.Viper) (localBackupPaths, error) {
	workspaceRoot, err := resolveLocalWorkspace(settings.GetString("local.workspace"))
	if err != nil {
		return localBackupPaths{}, err
	}
	workspaceKey, err := userdata.WorkspaceKey(workspaceRoot)
	if err != nil {
		return localBackupPaths{}, fmt.Errorf("resolve local backup workspace identity: %w", err)
	}
	resolveWorkspace := func(value, description, name string) (string, error) {
		return resolveWorkspaceDataPath(value, description, "local", workspaceRoot, name)
	}
	databasePath, err := resolveWorkspace(settings.GetString("local.db"), "local database", "gateway.db")
	if err != nil {
		return localBackupPaths{}, err
	}
	credentialPath, err := resolveWorkspace(settings.GetString("local.credentials"), "local credential file", "credentials.json")
	if err != nil {
		return localBackupPaths{}, err
	}
	outboxPath, err := resolveWorkspace(settings.GetString("local.outbox"), "local worker outbox", "worker-outbox.db")
	if err != nil {
		return localBackupPaths{}, err
	}
	statePath := ""
	if strings.TrimSpace(settings.GetString("local.state_db")) != "" {
		statePath, err = resolveWorkspace(settings.GetString("local.state_db"), "local worker state database", "worker-state.db")
		if err != nil {
			return localBackupPaths{}, err
		}
	}
	mirrorRoot, err := resolveMirrorRoot(settings.GetString("local.mirror_root"))
	if err != nil {
		return localBackupPaths{}, err
	}
	callerSubject := strings.TrimSpace(settings.GetString("local.caller_subject"))
	if !completion.IsStableKey(callerSubject) {
		return localBackupPaths{}, errors.New("local caller subject must be a stable key")
	}
	workerSubject := strings.TrimSpace(settings.GetString("local.worker_subject"))
	if !completion.IsStableKey(workerSubject) {
		return localBackupPaths{}, errors.New("local worker subject must be a stable key")
	}
	paths := localBackupPaths{
		WorkspaceRoot: workspaceRoot, WorkspaceKey: workspaceKey,
		GatewayDB: databasePath, Credentials: credentialPath, MirrorRoot: mirrorRoot,
		OutboxDB: outboxPath, StateDB: statePath, CallerSubject: callerSubject, WorkerSubject: workerSubject,
	}
	if err := validateLocalBackupPathSet(paths); err != nil {
		return localBackupPaths{}, err
	}
	return paths, nil
}

func createLocalBackup(ctx context.Context, paths localBackupPaths, archivePath string, force bool) (localBackupSummary, error) {
	if ctx == nil {
		return localBackupSummary{}, errors.New("create local backup: context is required")
	}
	if err := validateLocalBackupPathSet(paths); err != nil {
		return localBackupSummary{}, err
	}
	if err := rejectArchiveStateCollision(paths, archivePath); err != nil {
		return localBackupSummary{}, err
	}
	locks, err := acquireStoppedLocal(paths, false)
	if err != nil {
		return localBackupSummary{}, err
	}
	defer locks.Close()
	if err := rejectPendingLocalRestore(paths.GatewayDB); err != nil {
		return localBackupSummary{}, err
	}
	if credentials, found, err := readLocalCredentials(paths.Credentials); err != nil {
		return localBackupSummary{}, err
	} else if !found {
		return localBackupSummary{}, errors.New("local credentials do not exist; start and stop `human local` before backing up")
	} else if credentials.Active == nil {
		return localBackupSummary{}, errors.New("local credential rotation is incomplete; start and stop `human local` to recover it before backing up")
	}

	stage, err := os.MkdirTemp("", "human-local-backup-stage-*")
	if err != nil {
		return localBackupSummary{}, fmt.Errorf("create local backup staging directory: %w", err)
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0o700); err != nil {
		return localBackupSummary{}, fmt.Errorf("secure local backup staging directory: %w", err)
	}

	for _, item := range []struct {
		source, logical, label string
	}{
		{paths.GatewayDB, "gateway/gateway.db", "gateway database"},
		{paths.OutboxDB, "worker/worker-outbox.db", "worker outbox database"},
	} {
		if err := snapshotSQLite(ctx, item.source, filepath.Join(stage, filepath.FromSlash(item.logical)), item.label); err != nil {
			return localBackupSummary{}, err
		}
	}
	if paths.StateDB != "" {
		if err := snapshotSQLite(ctx, paths.StateDB, filepath.Join(stage, "worker", "worker-state.db"), "worker state database"); err != nil {
			return localBackupSummary{}, err
		}
	}
	if err := copyStableRegular(paths.Credentials, filepath.Join(stage, "credentials", "credentials.json"), 0o600); err != nil {
		return localBackupSummary{}, fmt.Errorf("stage local credentials: %w", err)
	}
	if err := validateLocalCredentialBinding(ctx, filepath.Join(stage, "gateway", "gateway.db"), filepath.Join(stage, "credentials", "credentials.json"), paths.CallerSubject, paths.WorkerSubject); err != nil {
		return localBackupSummary{}, fmt.Errorf("validate local backup credential binding: %w", err)
	}
	gatewayIdentity, err := localGatewayIdentity(paths.GatewayDB)
	if err != nil {
		return localBackupSummary{}, fmt.Errorf("resolve local backup gateway identity: %w", err)
	}
	if err := validateLocalWorkerBinding(
		ctx,
		filepath.Join(stage, "worker", "worker-outbox.db"),
		filepath.Join(stage, "worker", "worker-state.db"),
		paths.StateDB != "", gatewayIdentity, paths.WorkerSubject,
	); err != nil {
		return localBackupSummary{}, fmt.Errorf("validate local backup worker identity: %w", err)
	}

	var skipped []localBackupSkipped
	for _, tree := range []struct {
		callerRoot, source, destination, logical string
	}{
		{
			filepath.Join(paths.MirrorRoot, paths.CallerSubject),
			filepath.Join(paths.MirrorRoot, paths.CallerSubject, paths.WorkspaceKey),
			filepath.Join(stage, "mirror", "workspace"),
			"mirror/workspace",
		},
		{
			filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject),
			filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject, paths.WorkspaceKey),
			filepath.Join(stage, "mirror", "state"),
			"mirror/state",
		},
	} {
		if err := validateLocalMirrorCallerRoot(tree.callerRoot, true); err != nil {
			return localBackupSummary{}, err
		}
		if err := copyMirrorTree(tree.source, tree.destination, tree.logical, &skipped); err != nil {
			return localBackupSummary{}, err
		}
	}

	manifest, bytesCount, err := buildLocalBackupManifest(stage, paths, skipped)
	if err != nil {
		return localBackupSummary{}, err
	}
	if err := writeLocalBackupArchive(stage, manifest, archivePath, force); err != nil {
		return localBackupSummary{}, err
	}
	return localBackupSummary{Manifest: manifest, Bytes: bytesCount}, nil
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

func acquireStoppedLocal(paths localBackupPaths, createParents bool) (*localOfflineLocks, error) {
	type candidate struct {
		label    string
		location sqlitefile.Location
	}
	databasePaths := []struct{ label, path string }{
		{"gateway database", paths.GatewayDB},
		{"worker outbox database", paths.OutboxDB},
	}
	if paths.StateDB != "" {
		databasePaths = append(databasePaths, struct{ label, path string }{"worker state database", paths.StateDB})
	}
	candidates := make([]candidate, 0, len(databasePaths))
	for _, database := range databasePaths {
		if createParents {
			if err := os.MkdirAll(filepath.Dir(database.path), 0o700); err != nil {
				return nil, fmt.Errorf("create %s directory for restore: %w", database.label, err)
			}
		} else {
			info, err := os.Lstat(database.path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil, fmt.Errorf("%s does not exist; start and stop `human local` first", database.label)
				}
				return nil, fmt.Errorf("inspect %s: %w", database.label, err)
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return nil, fmt.Errorf("%s must be a regular file", database.label)
			}
		}
		location, err := sqlitefile.Resolve(database.path)
		if err != nil {
			return nil, fmt.Errorf("resolve %s for offline operation: %w", database.label, err)
		}
		if !location.FileBacked {
			return nil, fmt.Errorf("local backup and restore require a file-backed %s", database.label)
		}
		candidates = append(candidates, candidate{label: database.label, location: location})
	}
	// A caller can configure the three stores in different directories. Always
	// take their canonical identities in one order so concurrent administration
	// cannot create an AB/BA deadlock.
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].location.Path < candidates[j].location.Path })
	for index := 1; index < len(candidates); index++ {
		if candidates[index-1].location.Path == candidates[index].location.Path {
			return nil, fmt.Errorf("%s and %s resolve to the same SQLite database", candidates[index-1].label, candidates[index].label)
		}
	}
	held := &localOfflineLocks{}
	for _, candidate := range candidates {
		lock, err := ownerlock.Acquire(candidate.location, "local offline "+candidate.label)
		if err != nil {
			_ = held.Close()
			if errors.Is(err, ownerlock.ErrInUse) {
				return nil, fmt.Errorf("%s is in use; stop Human local and every worker using this local state before backup or restore", candidate.label)
			}
			return nil, err
		}
		held.locks = append(held.locks, lock)
	}
	return held, nil
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
	closeWith := func(operationErr error) error {
		return errors.Join(operationErr, database.Close())
	}
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

func sqliteReadOnlyDSN(databasePath string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(databasePath), RawQuery: "mode=ro"}).String()
}

func validateLocalCredentialBinding(ctx context.Context, gatewayDatabase, credentialPath, callerSubject, workerSubject string) error {
	credentials, found, err := readLocalCredentials(credentialPath)
	if err != nil {
		return err
	}
	if !found || credentials.Active == nil {
		return errors.New("credential journal has no active caller/worker pair")
	}
	if credentials.Active.Caller.SubjectID != callerSubject || credentials.Active.Worker.SubjectID != workerSubject {
		return fmt.Errorf(
			"active credential subjects (%q, %q) do not match backup identities (%q, %q)",
			credentials.Active.Caller.SubjectID, credentials.Active.Worker.SubjectID, callerSubject, workerSubject,
		)
	}
	database, err := sql.Open("sqlite", sqliteReadOnlyDSN(gatewayDatabase))
	if err != nil {
		return fmt.Errorf("open gateway database for credential binding check: %w", err)
	}
	database.SetMaxOpenConns(1)
	defer database.Close()
	for _, credential := range []localCredential{credentials.Active.Caller, credentials.Active.Worker} {
		digest := sha256.Sum256([]byte(credential.Secret))
		var keyID, principalType, subjectID string
		var revokedAt sql.NullInt64
		if err := database.QueryRowContext(ctx, `
			SELECT key_id, principal_type, subject_id, revoked_at
			FROM api_tokens
			WHERE token_hash = ?`, digest[:]).Scan(&keyID, &principalType, &subjectID, &revokedAt); err != nil {
			return fmt.Errorf("active %s token is not present in the gateway snapshot: %w", credential.Type, err)
		}
		if revokedAt.Valid || keyID != credential.KeyID || principalType != string(credential.Type) || subjectID != credential.SubjectID {
			return fmt.Errorf("active %s token does not match an unrevoked gateway principal", credential.Type)
		}
	}
	return nil
}

func localGatewayIdentity(databasePath string) (string, error) {
	logicalScope, err := localGatewayLogicalScope(databasePath)
	if err != nil {
		return "", err
	}
	return workerclient.GatewayIdentity("", logicalScope)
}

func localGatewayLogicalScope(databasePath string) (string, error) {
	location, err := sqlitefile.Resolve(databasePath)
	if err != nil {
		return "", err
	}
	if !location.FileBacked {
		return "", errors.New("local gateway identity requires a file-backed gateway database")
	}
	pathDigest := sha256.Sum256([]byte(location.Path))
	return "human-local-gateway:" + hex.EncodeToString(pathDigest[:]), nil
}

func validateLocalWorkerBinding(
	ctx context.Context,
	outboxPath, statePath string,
	stateEnabled bool,
	gatewayIdentity, workerSubject string,
) (returnErr error) {
	defer func() {
		returnErr = errors.Join(returnErr, removePrivateStagingOwnerLocks(outboxPath, statePath, stateEnabled))
	}()
	if err := workerclient.RebindOutboxIdentity(
		ctx, outboxPath, gatewayIdentity, gatewayIdentity, workerSubject,
	); err != nil {
		return fmt.Errorf("validate worker outbox identity: %w", err)
	}
	if !stateEnabled {
		return nil
	}
	if err := workerstate.RebindIdentity(
		ctx, statePath, gatewayIdentity, gatewayIdentity, workerSubject,
	); err != nil {
		return fmt.Errorf("validate worker state identity: %w", err)
	}
	return nil
}

func rebindLocalWorkerBinding(
	ctx context.Context,
	outboxPath, statePath string,
	stateEnabled bool,
	oldGatewayIdentity, newGatewayIdentity, workerSubject string,
) (returnErr error) {
	defer func() {
		returnErr = errors.Join(returnErr, removePrivateStagingOwnerLocks(outboxPath, statePath, stateEnabled))
	}()
	if err := workerclient.RebindOutboxIdentity(
		ctx, outboxPath, oldGatewayIdentity, newGatewayIdentity, workerSubject,
	); err != nil {
		return fmt.Errorf("rebind worker outbox identity: %w", err)
	}
	if !stateEnabled {
		return nil
	}
	if err := workerstate.RebindIdentity(
		ctx, statePath, oldGatewayIdentity, newGatewayIdentity, workerSubject,
	); err != nil {
		// Both databases are private extraction copies. If the second transaction
		// fails, the caller discards the whole extraction directory; no live state
		// has been partially published.
		return fmt.Errorf("rebind worker state identity: %w", err)
	}
	return nil
}

// The generic store locks are deliberately persistent for live databases.
// These two files live in a freshly-created mode-0700 staging directory known
// only to this process, so after Rebind has closed the stores there can be no
// third owner racing an unlink. Never call this helper on a configured live DB.
func removePrivateStagingOwnerLocks(outboxPath, statePath string, stateEnabled bool) error {
	paths := []string{outboxPath}
	if stateEnabled {
		paths = append(paths, statePath)
	}
	var errs []error
	for _, databasePath := range paths {
		location, err := sqlitefile.Resolve(databasePath)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		lockPath, fileBacked, err := ownerlock.Path(location)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !fileBacked {
			continue
		}
		if err := os.Remove(lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove private staging owner lock %s: %w", lockPath, err))
		}
	}
	return errors.Join(errs...)
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
		if err := output.Sync(); err != nil {
			return err
		}
		return nil
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

func validateLocalMirrorCallerRoot(root string, allowMissing bool) error {
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) && allowMissing {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect local mirror caller root %s: %w", root, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("local mirror caller root %s must be a directory, not a symlink or special file", root)
	}
	return nil
}

func ensureLocalMirrorCallerRoot(root string) error {
	if err := os.MkdirAll(filepath.Dir(root), 0o700); err != nil {
		return fmt.Errorf("create local mirror caller parent: %w", err)
	}
	if err := os.Mkdir(root, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create local mirror caller root: %w", err)
	}
	return validateLocalMirrorCallerRoot(root, false)
}

func buildLocalBackupManifest(stage string, paths localBackupPaths, skipped []localBackupSkipped) (localBackupManifest, int64, error) {
	gatewayIdentity, err := localGatewayIdentity(paths.GatewayDB)
	if err != nil {
		return localBackupManifest{}, 0, fmt.Errorf("resolve backup gateway identity: %w", err)
	}
	manifest := localBackupManifest{
		Format: localBackupFormat, Version: localBackupVersion, CreatedAt: time.Now().UTC(),
		WorkspaceScope: paths.WorkspaceKey, GatewayIdentity: gatewayIdentity, CallerSubject: paths.CallerSubject,
		WorkerSubject: paths.WorkerSubject,
		StateEnabled:  paths.StateDB != "", Skipped: append([]localBackupSkipped(nil), skipped...),
	}
	var total int64
	err = filepath.WalkDir(stage, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if current == stage {
			return nil
		}
		relative, err := filepath.Rel(stage, current)
		if err != nil {
			return err
		}
		logical := filepath.ToSlash(relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		item := localBackupManifestEntry{Path: logical, Mode: uint32(info.Mode().Perm())}
		if logical == "gateway/gateway.db" || logical == "credentials/credentials.json" ||
			logical == "worker/worker-outbox.db" || logical == "worker/worker-state.db" {
			item.Mode = 0o600
		}
		switch {
		case info.IsDir():
			item.Type = "directory"
		case info.Mode().IsRegular():
			item.Type = "file"
			item.Size = info.Size()
			digest, err := digestFile(current)
			if err != nil {
				return err
			}
			item.SHA256 = digest
			item.SQLite = logical == "gateway/gateway.db" || logical == "worker/worker-outbox.db" || logical == "worker/worker-state.db"
			total += item.Size
		default:
			return fmt.Errorf("backup staging path %s is not a regular file or directory", logical)
		}
		manifest.Entries = append(manifest.Entries, item)
		return nil
	})
	if err != nil {
		return localBackupManifest{}, 0, fmt.Errorf("enumerate local backup staging tree: %w", err)
	}
	sort.Slice(manifest.Entries, func(i, j int) bool { return manifest.Entries[i].Path < manifest.Entries[j].Path })
	sort.Slice(manifest.Skipped, func(i, j int) bool { return manifest.Skipped[i].Path < manifest.Skipped[j].Path })
	if err := validateLocalBackupManifest(manifest); err != nil {
		return localBackupManifest{}, 0, err
	}
	return manifest, total, nil
}

func writeLocalBackupArchive(stage string, manifest localBackupManifest, archivePath string, force bool) error {
	directory := filepath.Dir(archivePath)
	if info, err := os.Stat(directory); err != nil {
		return fmt.Errorf("inspect backup output directory: %w", err)
	} else if !info.IsDir() {
		return errors.New("backup output parent is not a directory")
	}
	if !force {
		if _, err := os.Lstat(archivePath); err == nil {
			return fmt.Errorf("backup archive %s already exists; use --force to replace it", archivePath)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect backup output: %w", err)
		}
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode backup manifest: %w", err)
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(directory, ".human-local-backup-*")
	if err != nil {
		return fmt.Errorf("create temporary backup archive: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure temporary backup archive: %w", err)
	}
	gzipWriter := gzip.NewWriter(temporary)
	tarWriter := tar.NewWriter(gzipWriter)
	writeErr := tarWriter.WriteHeader(&tar.Header{
		Name: localBackupManifestPath, Typeflag: tar.TypeReg, Mode: 0o600,
		Size: int64(len(payload)), ModTime: manifest.CreatedAt,
	})
	if writeErr == nil {
		_, writeErr = tarWriter.Write(payload)
	}
	for _, entry := range manifest.Entries {
		if writeErr != nil {
			break
		}
		header := &tar.Header{Name: entry.Path, Mode: int64(entry.Mode), ModTime: manifest.CreatedAt}
		if entry.Type == "directory" {
			header.Typeflag = tar.TypeDir
		} else {
			header.Typeflag = tar.TypeReg
			header.Size = entry.Size
		}
		writeErr = tarWriter.WriteHeader(header)
		if writeErr == nil && entry.Type == "file" {
			file, err := os.Open(filepath.Join(stage, filepath.FromSlash(entry.Path)))
			if err != nil {
				writeErr = err
			} else {
				_, writeErr = io.CopyN(tarWriter, file, entry.Size)
				writeErr = errors.Join(writeErr, file.Close())
			}
		}
	}
	writeErr = errors.Join(writeErr, tarWriter.Close(), gzipWriter.Close())
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	writeErr = errors.Join(writeErr, temporary.Close())
	if writeErr != nil {
		return fmt.Errorf("write local backup archive: %w", writeErr)
	}
	if force {
		err = replaceFileAtomically(temporaryPath, archivePath)
	} else {
		err = os.Link(temporaryPath, archivePath)
	}
	if err != nil {
		if !force && errors.Is(err, os.ErrExist) {
			return fmt.Errorf("backup archive %s already exists; use --force to replace it", archivePath)
		}
		return fmt.Errorf("publish backup archive atomically: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove temporary backup archive link: %w", err)
	}
	return syncDirectory(directory)
}

func extractAndVerifyLocalBackup(ctx context.Context, archivePath, destination string) (localBackupSummary, error) {
	archiveInfo, err := os.Lstat(archivePath)
	if err != nil {
		return localBackupSummary{}, fmt.Errorf("inspect backup archive: %w", err)
	}
	if archiveInfo.Mode()&os.ModeSymlink != 0 || !archiveInfo.Mode().IsRegular() {
		return localBackupSummary{}, errors.New("backup archive must be a regular file, not a symlink or special file")
	}
	if runtime.GOOS != "windows" && archiveInfo.Mode().Perm()&0o077 != 0 {
		return localBackupSummary{}, errors.New("backup archive contains credentials and must not be accessible by group or others (chmod 600)")
	}
	file, err := os.Open(archivePath)
	if err != nil {
		return localBackupSummary{}, fmt.Errorf("open backup archive: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(archiveInfo, opened) {
		return localBackupSummary{}, errors.New("backup archive changed while opening")
	}
	buffered := bufio.NewReader(file)
	gzipReader, err := gzip.NewReader(buffered)
	if err != nil {
		return localBackupSummary{}, fmt.Errorf("open backup gzip stream: %w", err)
	}
	gzipReader.Multistream(false)
	tarReader := tar.NewReader(gzipReader)
	manifestHeader, err := tarReader.Next()
	if err != nil {
		return localBackupSummary{}, fmt.Errorf("read backup manifest header: %w", err)
	}
	if manifestHeader.Name != localBackupManifestPath || manifestHeader.Typeflag != tar.TypeReg ||
		manifestHeader.Size <= 0 || manifestHeader.Size > maxLocalBackupManifestSize {
		return localBackupSummary{}, errors.New("backup must begin with a bounded regular manifest.json")
	}
	manifestPayload, err := io.ReadAll(io.LimitReader(tarReader, maxLocalBackupManifestSize+1))
	if err != nil || int64(len(manifestPayload)) != manifestHeader.Size {
		return localBackupSummary{}, errors.New("backup manifest is truncated")
	}
	decoder := json.NewDecoder(strings.NewReader(string(manifestPayload)))
	decoder.DisallowUnknownFields()
	var manifest localBackupManifest
	if err := decoder.Decode(&manifest); err != nil {
		return localBackupSummary{}, fmt.Errorf("decode backup manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return localBackupSummary{}, errors.New("backup manifest contains trailing JSON")
	}
	if err := validateLocalBackupManifest(manifest); err != nil {
		return localBackupSummary{}, err
	}
	expected := make(map[string]localBackupManifestEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		expected[entry.Path] = entry
	}
	seen := make(map[string]struct{}, len(expected))
	var total int64
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return localBackupSummary{}, fmt.Errorf("read backup entry: %w", err)
		}
		entry, ok := expected[header.Name]
		if !ok {
			return localBackupSummary{}, fmt.Errorf("backup contains unmanifested path %q", header.Name)
		}
		if _, duplicate := seen[header.Name]; duplicate {
			return localBackupSummary{}, fmt.Errorf("backup contains duplicate path %q", header.Name)
		}
		seen[header.Name] = struct{}{}
		if uint32(header.Mode)&0o777 != entry.Mode {
			return localBackupSummary{}, fmt.Errorf("backup mode for %s does not match manifest", entry.Path)
		}
		target, err := secureArchiveTarget(destination, entry.Path)
		if err != nil {
			return localBackupSummary{}, err
		}
		switch entry.Type {
		case "directory":
			if header.Typeflag != tar.TypeDir || header.Size != 0 {
				return localBackupSummary{}, fmt.Errorf("backup directory %s has an invalid tar header", entry.Path)
			}
			if err := os.MkdirAll(target, fs.FileMode(entry.Mode)); err != nil {
				return localBackupSummary{}, err
			}
		case "file":
			if header.Typeflag != tar.TypeReg || header.Size != entry.Size {
				return localBackupSummary{}, fmt.Errorf("backup file %s has an invalid tar header", entry.Path)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return localBackupSummary{}, err
			}
			output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fs.FileMode(entry.Mode))
			if err != nil {
				return localBackupSummary{}, err
			}
			hasher := sha256.New()
			written, copyErr := io.CopyN(io.MultiWriter(output, hasher), tarReader, entry.Size)
			syncErr := output.Sync()
			closeErr := output.Close()
			if copyErr != nil || syncErr != nil || closeErr != nil || written != entry.Size {
				return localBackupSummary{}, fmt.Errorf("extract backup file %s: %w", entry.Path, errors.Join(copyErr, syncErr, closeErr))
			}
			if hex.EncodeToString(hasher.Sum(nil)) != entry.SHA256 {
				return localBackupSummary{}, fmt.Errorf("backup checksum mismatch for %s", entry.Path)
			}
			total += entry.Size
			if total > maxLocalBackupExtractBytes {
				return localBackupSummary{}, fmt.Errorf("backup exceeds the %d-byte extraction limit", maxLocalBackupExtractBytes)
			}
		}
	}
	if len(seen) != len(expected) {
		for name := range expected {
			if _, ok := seen[name]; !ok {
				return localBackupSummary{}, fmt.Errorf("backup is missing manifested path %s", name)
			}
		}
	}
	// tar stops at its zero blocks before gzip necessarily reads the compressed
	// trailer. Drain to gzip EOF so its CRC/size checksum is actually verified,
	// then reject a second gzip member or any trailing bytes.
	if _, err := io.Copy(io.Discard, gzipReader); err != nil {
		return localBackupSummary{}, fmt.Errorf("verify backup gzip checksum: %w", err)
	}
	if err := gzipReader.Close(); err != nil {
		return localBackupSummary{}, fmt.Errorf("verify backup gzip checksum: %w", err)
	}
	if _, err := buffered.Peek(1); !errors.Is(err, io.EOF) {
		if err == nil {
			return localBackupSummary{}, errors.New("backup contains a second gzip member or trailing data")
		}
		return localBackupSummary{}, fmt.Errorf("inspect backup trailing data: %w", err)
	}
	for _, entry := range manifest.Entries {
		if entry.SQLite {
			if err := quickCheckSQLitePath(ctx, filepath.Join(destination, filepath.FromSlash(entry.Path)), "backup "+entry.Path); err != nil {
				return localBackupSummary{}, err
			}
		}
	}
	if err := validateLocalCredentialBinding(ctx,
		filepath.Join(destination, "gateway", "gateway.db"),
		filepath.Join(destination, "credentials", "credentials.json"),
		manifest.CallerSubject, manifest.WorkerSubject,
	); err != nil {
		return localBackupSummary{}, fmt.Errorf("validate backup credential binding: %w", err)
	}
	if err := validateLocalWorkerBinding(
		ctx,
		filepath.Join(destination, "worker", "worker-outbox.db"),
		filepath.Join(destination, "worker", "worker-state.db"),
		manifest.StateEnabled, manifest.GatewayIdentity, manifest.WorkerSubject,
	); err != nil {
		return localBackupSummary{}, fmt.Errorf("validate backup worker identity: %w", err)
	}
	return localBackupSummary{Manifest: manifest, Bytes: total}, nil
}

func validateLocalBackupManifest(manifest localBackupManifest) error {
	if manifest.Format != localBackupFormat || manifest.Version != localBackupVersion {
		return fmt.Errorf("unsupported local backup format %q version %d", manifest.Format, manifest.Version)
	}
	if manifest.CreatedAt.IsZero() || manifest.CreatedAt.Location() != time.UTC {
		return errors.New("backup manifest created_at must be a UTC timestamp")
	}
	if !strings.HasPrefix(manifest.WorkspaceScope, "workspace-") || !completion.IsStableKey(manifest.CallerSubject) || !completion.IsStableKey(manifest.WorkerSubject) {
		return errors.New("backup manifest has an invalid workspace or caller identity")
	}
	if !validLocalGatewayIdentity(manifest.GatewayIdentity) {
		return errors.New("backup manifest has an invalid local gateway identity")
	}
	if len(manifest.Entries) == 0 {
		return errors.New("backup manifest has no entries")
	}
	if len(manifest.Entries) > maxLocalBackupEntries || len(manifest.Skipped) > maxLocalBackupEntries {
		return fmt.Errorf("backup manifest exceeds the %d-entry limit", maxLocalBackupEntries)
	}
	required := map[string]string{
		"gateway": "gateway/gateway.db", "credentials": "credentials/credentials.json",
		"outbox": "worker/worker-outbox.db", "mirror-workspace": "mirror/workspace",
		"mirror-state": "mirror/state",
	}
	if manifest.StateEnabled {
		required["state"] = "worker/worker-state.db"
	}
	seen := make(map[string]struct{}, len(manifest.Entries))
	portable := make(map[string]string, len(manifest.Entries))
	var declaredBytes int64
	for _, entry := range manifest.Entries {
		if err := validateArchivePath(entry.Path); err != nil {
			return err
		}
		if _, duplicate := seen[entry.Path]; duplicate {
			return fmt.Errorf("backup manifest repeats path %q", entry.Path)
		}
		seen[entry.Path] = struct{}{}
		key := cases.Fold().String(norm.NFC.String(entry.Path))
		if previous, collision := portable[key]; collision {
			return fmt.Errorf("backup paths %q and %q collide on a portable filesystem", previous, entry.Path)
		}
		portable[key] = entry.Path
		if entry.Mode > 0o777 {
			return fmt.Errorf("backup entry %s has invalid permission mode", entry.Path)
		}
		switch entry.Type {
		case "directory":
			if entry.Size != 0 || entry.SHA256 != "" || entry.SQLite {
				return fmt.Errorf("backup directory %s has invalid metadata", entry.Path)
			}
		case "file":
			if entry.Size < 0 || len(entry.SHA256) != sha256.Size*2 {
				return fmt.Errorf("backup file %s has invalid size or checksum", entry.Path)
			}
			if _, err := hex.DecodeString(entry.SHA256); err != nil {
				return fmt.Errorf("backup file %s has an invalid checksum", entry.Path)
			}
			if entry.Size > maxLocalBackupExtractBytes-declaredBytes {
				return fmt.Errorf("backup manifest exceeds the %d-byte extraction limit", maxLocalBackupExtractBytes)
			}
			declaredBytes += entry.Size
		default:
			return fmt.Errorf("backup entry %s has unsupported type %q", entry.Path, entry.Type)
		}
		if !allowedLocalBackupPath(entry.Path) {
			return fmt.Errorf("backup manifest path %s is outside the fixed local backup layout", entry.Path)
		}
	}
	if !manifest.StateEnabled {
		if _, present := seen["worker/worker-state.db"]; present {
			return errors.New("backup has worker state while state_enabled is false")
		}
	}
	for component, requiredPath := range required {
		entry, found := manifestEntryByPath(manifest, requiredPath)
		if !found {
			return fmt.Errorf("backup manifest is missing required path %s", requiredPath)
		}
		switch component {
		case "gateway", "outbox", "state":
			if entry.Type != "file" || !entry.SQLite || entry.Mode != 0o600 {
				return fmt.Errorf("backup %s component must be one mode-0600 SQLite file", component)
			}
		case "credentials":
			if entry.Type != "file" || entry.SQLite || entry.Mode != 0o600 {
				return errors.New("backup credentials component must be one mode-0600 non-SQLite file")
			}
		case "mirror-workspace", "mirror-state":
			if entry.Type != "directory" || entry.SQLite {
				return fmt.Errorf("backup %s component must be a directory", component)
			}
		}
	}
	for _, entry := range manifest.Entries {
		if entry.SQLite && entry.Path != "gateway/gateway.db" && entry.Path != "worker/worker-outbox.db" && entry.Path != "worker/worker-state.db" {
			return fmt.Errorf("backup path %s is not an allowed SQLite component", entry.Path)
		}
	}
	skippedPortable := make(map[string]string, len(manifest.Skipped))
	for _, skipped := range manifest.Skipped {
		if err := validateArchivePath(skipped.Path); err != nil {
			return fmt.Errorf("invalid skipped mirror path: %w", err)
		}
		if len(skipped.Reason) > maxLocalBackupReasonBytes ||
			(!strings.HasPrefix(skipped.Path, "mirror/workspace/") && !strings.HasPrefix(skipped.Path, "mirror/state/")) ||
			strings.TrimSpace(skipped.Reason) == "" {
			return errors.New("backup skipped entries must be explained mirror paths")
		}
		if _, included := seen[skipped.Path]; included {
			return fmt.Errorf("backup path %s is both included and skipped", skipped.Path)
		}
		key := cases.Fold().String(norm.NFC.String(skipped.Path))
		if included, collision := portable[key]; collision {
			return fmt.Errorf("skipped backup path %q collides with included path %q", skipped.Path, included)
		}
		if previous, duplicate := skippedPortable[key]; duplicate {
			return fmt.Errorf("skipped backup paths %q and %q collide", previous, skipped.Path)
		}
		skippedPortable[key] = skipped.Path
	}
	return nil
}

func validLocalGatewayIdentity(value string) bool {
	const prefix = "scope:"
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, prefix))
	return err == nil
}

func validateLocalBackupPathSet(paths localBackupPaths) error {
	files := map[string]string{
		"gateway database": paths.GatewayDB, "credential journal": paths.Credentials,
		"worker outbox": paths.OutboxDB,
	}
	if paths.StateDB != "" {
		files["worker state"] = paths.StateDB
	}
	seen := make(map[string]string, len(files))
	worktree := filepath.Join(paths.MirrorRoot, paths.CallerSubject)
	baseline := filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject)
	for label, filename := range files {
		key, err := portableFilesystemPath(filename)
		if err != nil {
			return fmt.Errorf("resolve %s backup identity: %w", label, err)
		}
		if previous, duplicate := seen[key]; duplicate {
			return fmt.Errorf("local backup paths for %s and %s resolve to the same file", previous, label)
		}
		seen[key] = label
		for _, mirrorTarget := range []string{worktree, baseline} {
			inside, err := pathAtOrBelow(mirrorTarget, filename)
			if err != nil {
				return err
			}
			if inside {
				return fmt.Errorf("%s must not be stored inside the local mirror correctness tree", label)
			}
		}
	}
	return nil
}

func rejectArchiveStateCollision(paths localBackupPaths, archivePath string) error {
	archiveKey, err := portableFilesystemPath(archivePath)
	if err != nil {
		return err
	}
	reserved := map[string]string{
		"gateway database": paths.GatewayDB, "credential journal": paths.Credentials,
		"worker outbox": paths.OutboxDB, "worker state": paths.StateDB,
		"local restore journal": localRestoreJournalPath(paths.GatewayDB),
	}
	for label, database := range map[string]string{
		"gateway database": paths.GatewayDB, "worker outbox": paths.OutboxDB, "worker state": paths.StateDB,
	} {
		if database == "" {
			continue
		}
		for suffix, description := range map[string]string{
			"journal": "rollback journal", "wal": "WAL sidecar", "shm": "shared-memory sidecar",
		} {
			reserved[label+" "+description] = database + "-" + suffix
		}
		location, err := sqlitefile.Resolve(database)
		if err != nil {
			return fmt.Errorf("resolve %s owner lock collision: %w", label, err)
		}
		lockPath, fileBacked, err := ownerlock.Path(location)
		if err != nil {
			return fmt.Errorf("resolve %s owner lock collision: %w", label, err)
		}
		if fileBacked {
			reserved[label+" owner lock"] = lockPath
		}
	}
	for label, filename := range reserved {
		if filename == "" {
			continue
		}
		key, err := portableFilesystemPath(filename)
		if err != nil {
			return err
		}
		if key == archiveKey {
			return fmt.Errorf("backup archive must not collide with the %s", label)
		}
	}
	for _, mirrorTarget := range []string{
		filepath.Join(paths.MirrorRoot, paths.CallerSubject),
		filepath.Join(paths.MirrorRoot, ".human-state", paths.CallerSubject),
	} {
		inside, err := pathAtOrBelow(mirrorTarget, archivePath)
		if err != nil {
			return err
		}
		if inside {
			return errors.New("backup archive must be outside the local mirror correctness tree")
		}
	}
	return nil
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

func allowedLocalBackupPath(value string) bool {
	switch value {
	case "gateway", "gateway/gateway.db", "credentials", "credentials/credentials.json",
		"worker", "worker/worker-outbox.db", "worker/worker-state.db",
		"mirror", "mirror/workspace", "mirror/state":
		return true
	default:
		return strings.HasPrefix(value, "mirror/workspace/") || strings.HasPrefix(value, "mirror/state/")
	}
}

func manifestEntryByPath(manifest localBackupManifest, wanted string) (localBackupManifestEntry, bool) {
	for _, entry := range manifest.Entries {
		if entry.Path == wanted {
			return entry, true
		}
	}
	return localBackupManifestEntry{}, false
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

func countManifestFiles(manifest localBackupManifest) int {
	count := 0
	for _, entry := range manifest.Entries {
		if entry.Type == "file" {
			count++
		}
	}
	return count
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
