package humancmd

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
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
	"time"

	"github.com/spf13/viper"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"github.com/vibe-agi/human/internal/userdata"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
	workerkitsqlite "github.com/vibe-agi/human/workerkit/sqlite"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	publicLocalBackupFormat       = "human-local-backup"
	publicLocalBackupVersion      = 3
	publicLocalBackupManifestPath = "manifest.json"
	publicLocalDeploymentID       = "human-local"
)

var publicLocalStableKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type publicLocalBackupPaths struct {
	WorkspaceRoot   string
	WorkspaceKey    string
	ServiceDB       string
	Credentials     string
	MirrorWorkspace string
	MirrorBaseline  string
	StateDB         string
	CallerID        string
	WorkerID        string
}

type publicLocalBackupManifest struct {
	Format          string                     `json:"format"`
	Version         int                        `json:"version"`
	CreatedAt       time.Time                  `json:"created_at"`
	DeploymentID    string                     `json:"deployment_id"`
	WorkspaceScope  string                     `json:"workspace_scope"`
	CallerID        string                     `json:"caller_id"`
	WorkerID        string                     `json:"worker_id"`
	StateEnabled    bool                       `json:"state_enabled"`
	BaselineEnabled bool                       `json:"baseline_enabled"`
	Entries         []localBackupManifestEntry `json:"entries"`
	Skipped         []localBackupSkipped       `json:"skipped,omitempty"`
}

type publicLocalBackupSummary struct {
	Manifest publicLocalBackupManifest
	Bytes    int64
}

func resolvePublicLocalBackupPaths(settings *viper.Viper) (publicLocalBackupPaths, error) {
	workspaceRoot, err := resolveLocalWorkspace(settings.GetString("local.workspace"))
	if err != nil {
		return publicLocalBackupPaths{}, err
	}
	workspaceKey, err := userdata.WorkspaceKey(workspaceRoot)
	if err != nil {
		return publicLocalBackupPaths{}, fmt.Errorf("resolve local backup workspace identity: %w", err)
	}
	resolveWorkspace := func(value, description, name string) (string, error) {
		return resolveWorkspaceDataPath(value, description, "local", workspaceRoot, name)
	}
	serviceDB, err := resolveWorkspace(settings.GetString("local.db"), "local service database", "store.db")
	if err != nil {
		return publicLocalBackupPaths{}, err
	}
	credentials, err := resolveWorkspace(settings.GetString("local.credentials"), "local credential file", "credentials.json")
	if err != nil {
		return publicLocalBackupPaths{}, err
	}
	stateDB := ""
	baseline := ""
	if strings.TrimSpace(settings.GetString("local.state_db")) != "" {
		stateDB, err = resolveWorkspace(settings.GetString("local.state_db"), "local worker state database", "workerkit-state.db")
		if err != nil {
			return publicLocalBackupPaths{}, err
		}
		baseline = filepath.Join(filepath.Dir(stateDB), "workerkit-mirror-baseline.json")
	}
	callerID := strings.TrimSpace(settings.GetString("local.caller_subject"))
	workerID := strings.TrimSpace(settings.GetString("local.worker_subject"))
	if !publicLocalStableKey.MatchString(callerID) || !publicLocalStableKey.MatchString(workerID) {
		return publicLocalBackupPaths{}, errors.New("local caller and worker identities must be stable keys")
	}
	paths := publicLocalBackupPaths{
		WorkspaceRoot: workspaceRoot, WorkspaceKey: workspaceKey,
		ServiceDB: serviceDB, Credentials: credentials,
		MirrorWorkspace: workspaceRoot, MirrorBaseline: baseline,
		StateDB: stateDB, CallerID: callerID, WorkerID: workerID,
	}
	if err := validatePublicLocalPathSet(paths); err != nil {
		return publicLocalBackupPaths{}, err
	}
	return paths, nil
}

func validatePublicLocalPathSet(paths publicLocalBackupPaths) error {
	if !strings.HasPrefix(paths.WorkspaceKey, "workspace-") ||
		!publicLocalStableKey.MatchString(paths.CallerID) || !publicLocalStableKey.MatchString(paths.WorkerID) {
		return errors.New("public local backup has invalid workspace or principal identity")
	}
	files := map[string]string{
		"service database": paths.ServiceDB, "credential file": paths.Credentials,
	}
	if paths.StateDB != "" {
		files["worker state database"] = paths.StateDB
		files["mirror baseline"] = paths.MirrorBaseline
	}
	seen := make(map[string]string, len(files))
	for label, filename := range files {
		key, err := portableFilesystemPath(filename)
		if err != nil {
			return fmt.Errorf("resolve %s identity: %w", label, err)
		}
		if previous, duplicate := seen[key]; duplicate {
			return fmt.Errorf("local paths for %s and %s resolve to the same file", previous, label)
		}
		seen[key] = label
		inside, err := pathAtOrBelow(paths.MirrorWorkspace, filename)
		if err != nil {
			return err
		}
		if inside {
			return fmt.Errorf("%s must not be stored inside the mirror workspace", label)
		}
	}
	return nil
}

func rejectPublicArchiveCollision(paths publicLocalBackupPaths, archivePath string) error {
	archiveKey, err := portableFilesystemPath(archivePath)
	if err != nil {
		return err
	}
	reserved := map[string]string{
		"service database": paths.ServiceDB, "credential file": paths.Credentials,
		"worker state database": paths.StateDB, "mirror baseline": paths.MirrorBaseline,
		"restore journal": localRestoreJournalPath(paths.ServiceDB),
	}
	for label, database := range map[string]string{"service database": paths.ServiceDB, "worker state database": paths.StateDB} {
		if database == "" {
			continue
		}
		for _, suffix := range []string{"journal", "wal", "shm"} {
			reserved[label+" "+suffix] = database + "-" + suffix
		}
		location, err := sqlitefile.Resolve(database)
		if err != nil {
			return err
		}
		lockPath, backed, err := ownerlock.Path(location)
		if err != nil {
			return err
		}
		if backed {
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
	inside, err := pathAtOrBelow(paths.MirrorWorkspace, archivePath)
	if err != nil {
		return err
	}
	if inside {
		return errors.New("backup archive must be outside the mirror workspace")
	}
	return nil
}

func acquireStoppedPublicLocal(paths publicLocalBackupPaths, createParents bool) (*localOfflineLocks, error) {
	type candidate struct {
		label    string
		location sqlitefile.Location
	}
	inputs := []struct{ label, path string }{{"service database", paths.ServiceDB}}
	if paths.StateDB != "" {
		inputs = append(inputs, struct{ label, path string }{"worker state database", paths.StateDB})
	}
	var candidates []candidate
	for _, input := range inputs {
		if createParents {
			if err := os.MkdirAll(filepath.Dir(input.path), 0o700); err != nil {
				return nil, err
			}
		} else {
			info, err := os.Lstat(input.path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil, fmt.Errorf("%s does not exist; start and stop `human local` first", input.label)
				}
				return nil, err
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return nil, fmt.Errorf("%s must be a regular file", input.label)
			}
		}
		location, err := sqlitefile.Resolve(input.path)
		if err != nil {
			return nil, fmt.Errorf("resolve file-backed %s: %w", input.label, err)
		}
		if !location.FileBacked {
			return nil, fmt.Errorf("local backup and restore require a file-backed %s", input.label)
		}
		candidates = append(candidates, candidate{input.label, location})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].location.Path < candidates[j].location.Path })
	for index := 1; index < len(candidates); index++ {
		if candidates[index-1].location.Path == candidates[index].location.Path {
			return nil, errors.New("service and worker state databases resolve to the same file")
		}
	}
	held := &localOfflineLocks{}
	for _, candidate := range candidates {
		lock, err := ownerlock.Acquire(candidate.location, "local offline "+candidate.label)
		if err != nil {
			_ = held.Close()
			if errors.Is(err, ownerlock.ErrInUse) {
				return nil, fmt.Errorf("%s is in use; stop Human local before backup or restore", candidate.label)
			}
			return nil, err
		}
		held.locks = append(held.locks, lock)
	}
	return held, nil
}

func createPublicLocalBackup(ctx context.Context, paths publicLocalBackupPaths, archivePath string, force bool) (publicLocalBackupSummary, error) {
	if ctx == nil {
		return publicLocalBackupSummary{}, errors.New("create local backup: context is required")
	}
	if err := validatePublicLocalPathSet(paths); err != nil {
		return publicLocalBackupSummary{}, err
	}
	if err := rejectPublicArchiveCollision(paths, archivePath); err != nil {
		return publicLocalBackupSummary{}, err
	}
	locks, err := acquireStoppedPublicLocal(paths, false)
	if err != nil {
		return publicLocalBackupSummary{}, err
	}
	defer locks.Close()
	if err := rejectPendingLocalRestore(paths.ServiceDB); err != nil {
		return publicLocalBackupSummary{}, err
	}
	if _, found, err := readPublicCredentials(paths.Credentials); err != nil {
		return publicLocalBackupSummary{}, err
	} else if !found {
		return publicLocalBackupSummary{}, errors.New("local credentials do not exist; start and stop `human local` before backing up")
	}
	stage, err := os.MkdirTemp("", "human-local-backup-stage-*")
	if err != nil {
		return publicLocalBackupSummary{}, err
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0o700); err != nil {
		return publicLocalBackupSummary{}, err
	}
	if err := snapshotSQLite(ctx, paths.ServiceDB, filepath.Join(stage, "service", "service.db"), "service database"); err != nil {
		return publicLocalBackupSummary{}, err
	}
	if paths.StateDB != "" {
		if err := snapshotSQLite(ctx, paths.StateDB, filepath.Join(stage, "worker", "worker-state.db"), "worker state database"); err != nil {
			return publicLocalBackupSummary{}, err
		}
	}
	if err := copyStableRegular(paths.Credentials, filepath.Join(stage, "credentials", "credentials.json"), 0o600); err != nil {
		return publicLocalBackupSummary{}, fmt.Errorf("stage local credentials: %w", err)
	}
	if err := validateStagedPublicLocalDatabases(ctx, stage, paths.StateDB != ""); err != nil {
		return publicLocalBackupSummary{}, err
	}
	if _, found, err := readPublicCredentials(filepath.Join(stage, "credentials", "credentials.json")); err != nil || !found {
		if err == nil {
			err = errors.New("staged caller credential is missing")
		}
		return publicLocalBackupSummary{}, err
	}
	var skipped []localBackupSkipped
	if err := copyMirrorTree(paths.MirrorWorkspace, filepath.Join(stage, "mirror", "workspace"), "mirror/workspace", &skipped); err != nil {
		return publicLocalBackupSummary{}, err
	}
	baselineEnabled := false
	if paths.MirrorBaseline != "" {
		if _, err := os.Lstat(paths.MirrorBaseline); err == nil {
			if err := copyStableRegular(paths.MirrorBaseline, filepath.Join(stage, "mirror", "baseline.json"), 0o600); err != nil {
				return publicLocalBackupSummary{}, fmt.Errorf("stage mirror baseline: %w", err)
			}
			baselineEnabled = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return publicLocalBackupSummary{}, err
		}
	}
	manifest, bytesCount, err := buildPublicLocalManifest(stage, paths, baselineEnabled, skipped)
	if err != nil {
		return publicLocalBackupSummary{}, err
	}
	if err := writePublicLocalBackupArchive(stage, manifest, archivePath, force); err != nil {
		return publicLocalBackupSummary{}, err
	}
	return publicLocalBackupSummary{Manifest: manifest, Bytes: bytesCount}, nil
}

func validateStagedPublicLocalDatabases(ctx context.Context, stage string, stateEnabled bool) error {
	servicePath := filepath.Join(stage, "service", "service.db")
	serviceResource, err := llmsqlite.Open(ctx, llmsqlite.Config{Path: servicePath})
	if err != nil {
		return fmt.Errorf("validate staged service schema: %w", err)
	}
	if err := serviceResource.Release(context.Background()); err != nil {
		return fmt.Errorf("close staged service schema: %w", err)
	}
	if err := removeStagedSQLiteRuntimeFiles(servicePath); err != nil {
		return err
	}
	if !stateEnabled {
		return nil
	}
	statePath := filepath.Join(stage, "worker", "worker-state.db")
	stateResource, err := workerkitsqlite.Open(ctx, workerkitsqlite.Config{Path: statePath})
	if err != nil {
		return fmt.Errorf("validate staged worker state schema: %w", err)
	}
	if err := stateResource.Release(context.Background()); err != nil {
		return fmt.Errorf("close staged worker state schema: %w", err)
	}
	return removeStagedSQLiteRuntimeFiles(statePath)
}

func removeStagedSQLiteRuntimeFiles(database string) error {
	location, err := sqlitefile.Resolve(database)
	if err != nil {
		return err
	}
	lockPath, backed, err := ownerlock.Path(location)
	if err != nil {
		return err
	}
	artifacts := []string{database + "-journal", database + "-wal", database + "-shm"}
	if backed {
		artifacts = append(artifacts, lockPath)
	}
	for _, artifact := range artifacts {
		if err := os.Remove(artifact); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove staged SQLite runtime file %s: %w", artifact, err)
		}
	}
	return nil
}

func buildPublicLocalManifest(stage string, paths publicLocalBackupPaths, baseline bool, skipped []localBackupSkipped) (publicLocalBackupManifest, int64, error) {
	manifest := publicLocalBackupManifest{
		Format: publicLocalBackupFormat, Version: publicLocalBackupVersion, CreatedAt: time.Now().UTC(),
		DeploymentID: publicLocalDeploymentID, WorkspaceScope: paths.WorkspaceKey,
		CallerID: paths.CallerID, WorkerID: paths.WorkerID,
		StateEnabled: paths.StateDB != "", BaselineEnabled: baseline,
		Skipped: append([]localBackupSkipped(nil), skipped...),
	}
	var total int64
	err := filepath.WalkDir(stage, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || current == stage {
			return walkErr
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
		switch {
		case info.IsDir():
			item.Type = "directory"
		case info.Mode().IsRegular():
			item.Type, item.Size = "file", info.Size()
			item.SHA256, err = digestFile(current)
			if err != nil {
				return err
			}
			item.SQLite = logical == "service/service.db" || logical == "worker/worker-state.db"
			if logical == "service/service.db" || logical == "worker/worker-state.db" ||
				logical == "credentials/credentials.json" || logical == "mirror/baseline.json" {
				item.Mode = 0o600
			}
			total += item.Size
		default:
			return fmt.Errorf("backup staging path %s is not a regular file or directory", logical)
		}
		manifest.Entries = append(manifest.Entries, item)
		return nil
	})
	if err != nil {
		return publicLocalBackupManifest{}, 0, err
	}
	sort.Slice(manifest.Entries, func(i, j int) bool { return manifest.Entries[i].Path < manifest.Entries[j].Path })
	sort.Slice(manifest.Skipped, func(i, j int) bool { return manifest.Skipped[i].Path < manifest.Skipped[j].Path })
	if err := validatePublicLocalManifest(manifest); err != nil {
		return publicLocalBackupManifest{}, 0, err
	}
	return manifest, total, nil
}

func validatePublicLocalManifest(manifest publicLocalBackupManifest) error {
	if manifest.Format != publicLocalBackupFormat || manifest.Version != publicLocalBackupVersion {
		return fmt.Errorf("unsupported local backup format %q version %d", manifest.Format, manifest.Version)
	}
	if manifest.CreatedAt.IsZero() || manifest.CreatedAt.Location() != time.UTC ||
		manifest.DeploymentID != publicLocalDeploymentID || !strings.HasPrefix(manifest.WorkspaceScope, "workspace-") ||
		!publicLocalStableKey.MatchString(manifest.CallerID) || !publicLocalStableKey.MatchString(manifest.WorkerID) {
		return errors.New("backup manifest has invalid deployment, workspace, or principal identity")
	}
	if len(manifest.Entries) == 0 || len(manifest.Entries) > maxLocalBackupEntries || len(manifest.Skipped) > maxLocalBackupEntries {
		return errors.New("backup manifest has an invalid entry count")
	}
	required := map[string]string{
		"service/service.db": "sqlite", "credentials/credentials.json": "secret", "mirror/workspace": "directory",
	}
	if manifest.StateEnabled {
		required["worker/worker-state.db"] = "sqlite"
	}
	if manifest.BaselineEnabled {
		required["mirror/baseline.json"] = "secret"
	}
	seen := make(map[string]struct{}, len(manifest.Entries))
	portable := make(map[string]string, len(manifest.Entries))
	var declared int64
	for _, entry := range manifest.Entries {
		if err := validateArchivePath(entry.Path); err != nil || !allowedPublicLocalBackupPath(entry.Path) {
			if err != nil {
				return err
			}
			return fmt.Errorf("backup path %s is outside the fixed public-local layout", entry.Path)
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
			return fmt.Errorf("backup entry %s has invalid mode", entry.Path)
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
			if entry.Size > maxLocalBackupExtractBytes-declared {
				return errors.New("backup exceeds the extraction byte limit")
			}
			declared += entry.Size
		default:
			return fmt.Errorf("backup entry %s has unsupported type %q", entry.Path, entry.Type)
		}
	}
	if !manifest.StateEnabled {
		if _, ok := seen["worker/worker-state.db"]; ok {
			return errors.New("backup has worker state while state_enabled is false")
		}
	}
	if !manifest.BaselineEnabled {
		if _, ok := seen["mirror/baseline.json"]; ok {
			return errors.New("backup has a mirror baseline while baseline_enabled is false")
		}
	}
	for wanted, kind := range required {
		entry, found := publicManifestEntry(manifest, wanted)
		if !found {
			return fmt.Errorf("backup manifest is missing required path %s", wanted)
		}
		switch kind {
		case "sqlite":
			if entry.Type != "file" || !entry.SQLite || entry.Mode != 0o600 {
				return fmt.Errorf("backup %s must be a mode-0600 SQLite file", wanted)
			}
		case "secret":
			if entry.Type != "file" || entry.SQLite || entry.Mode != 0o600 {
				return fmt.Errorf("backup %s must be a mode-0600 regular file", wanted)
			}
		case "directory":
			if entry.Type != "directory" || entry.SQLite {
				return fmt.Errorf("backup %s must be a directory", wanted)
			}
		}
	}
	for _, entry := range manifest.Entries {
		if entry.SQLite && entry.Path != "service/service.db" && entry.Path != "worker/worker-state.db" {
			return fmt.Errorf("backup path %s is not an allowed SQLite component", entry.Path)
		}
	}
	for _, skipped := range manifest.Skipped {
		if err := validateArchivePath(skipped.Path); err != nil ||
			!strings.HasPrefix(skipped.Path, "mirror/workspace/") || strings.TrimSpace(skipped.Reason) == "" ||
			len(skipped.Reason) > maxLocalBackupReasonBytes {
			return errors.New("backup skipped entries must be explained mirror/workspace paths")
		}
		if _, included := seen[skipped.Path]; included {
			return fmt.Errorf("backup path %s is both included and skipped", skipped.Path)
		}
	}
	return nil
}

func allowedPublicLocalBackupPath(value string) bool {
	switch value {
	case "service", "service/service.db", "credentials", "credentials/credentials.json",
		"worker", "worker/worker-state.db", "mirror", "mirror/workspace", "mirror/baseline.json":
		return true
	default:
		return strings.HasPrefix(value, "mirror/workspace/")
	}
}

func publicManifestEntry(manifest publicLocalBackupManifest, wanted string) (localBackupManifestEntry, bool) {
	for _, entry := range manifest.Entries {
		if entry.Path == wanted {
			return entry, true
		}
	}
	return localBackupManifestEntry{}, false
}

func countPublicManifestFiles(manifest publicLocalBackupManifest) int {
	count := 0
	for _, entry := range manifest.Entries {
		if entry.Type == "file" {
			count++
		}
	}
	return count
}

func writePublicLocalBackupArchive(stage string, manifest publicLocalBackupManifest, archivePath string, force bool) error {
	directory := filepath.Dir(archivePath)
	if info, err := os.Stat(directory); err != nil || !info.IsDir() {
		return fmt.Errorf("backup output parent is not a directory: %w", err)
	}
	if !force {
		if _, err := os.Lstat(archivePath); err == nil {
			return fmt.Errorf("backup archive %s already exists; use --force to replace it", archivePath)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(directory, ".human-local-backup-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	gzipWriter := gzip.NewWriter(temporary)
	tarWriter := tar.NewWriter(gzipWriter)
	writeErr := tarWriter.WriteHeader(&tar.Header{
		Name: publicLocalBackupManifestPath, Typeflag: tar.TypeReg, Mode: 0o600,
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
			header.Typeflag, header.Size = tar.TypeReg, entry.Size
		}
		writeErr = tarWriter.WriteHeader(header)
		if writeErr == nil && entry.Type == "file" {
			file, openErr := os.Open(filepath.Join(stage, filepath.FromSlash(entry.Path)))
			if openErr != nil {
				writeErr = openErr
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
		return fmt.Errorf("publish backup archive atomically: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(directory)
}

func extractAndVerifyPublicLocalBackup(ctx context.Context, archivePath, destination string) (publicLocalBackupSummary, error) {
	info, err := os.Lstat(archivePath)
	if err != nil {
		return publicLocalBackupSummary{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return publicLocalBackupSummary{}, errors.New("backup archive must be a regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return publicLocalBackupSummary{}, errors.New("backup archive contains credentials and must have mode 0600")
	}
	file, err := os.Open(archivePath)
	if err != nil {
		return publicLocalBackupSummary{}, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return publicLocalBackupSummary{}, errors.New("backup archive changed while opening")
	}
	buffered := bufio.NewReader(file)
	gzipReader, err := gzip.NewReader(buffered)
	if err != nil {
		return publicLocalBackupSummary{}, err
	}
	gzipReader.Multistream(false)
	tarReader := tar.NewReader(gzipReader)
	header, err := tarReader.Next()
	if err != nil || header.Name != publicLocalBackupManifestPath || header.Typeflag != tar.TypeReg ||
		header.Size <= 0 || header.Size > maxLocalBackupManifestSize {
		return publicLocalBackupSummary{}, errors.New("backup must begin with a bounded regular manifest.json")
	}
	payload, err := io.ReadAll(io.LimitReader(tarReader, maxLocalBackupManifestSize+1))
	if err != nil || int64(len(payload)) != header.Size {
		return publicLocalBackupSummary{}, errors.New("backup manifest is truncated")
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	var manifest publicLocalBackupManifest
	if err := decoder.Decode(&manifest); err != nil {
		return publicLocalBackupSummary{}, fmt.Errorf("decode backup manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return publicLocalBackupSummary{}, errors.New("backup manifest contains trailing JSON")
	}
	if err := validatePublicLocalManifest(manifest); err != nil {
		return publicLocalBackupSummary{}, err
	}
	expected := make(map[string]localBackupManifestEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		expected[entry.Path] = entry
	}
	seen := make(map[string]struct{}, len(expected))
	var total int64
	for {
		header, err = tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return publicLocalBackupSummary{}, err
		}
		entry, ok := expected[header.Name]
		if !ok {
			return publicLocalBackupSummary{}, fmt.Errorf("backup contains unmanifested path %q", header.Name)
		}
		if _, duplicate := seen[header.Name]; duplicate {
			return publicLocalBackupSummary{}, fmt.Errorf("backup contains duplicate path %q", header.Name)
		}
		seen[header.Name] = struct{}{}
		if uint32(header.Mode)&0o777 != entry.Mode {
			return publicLocalBackupSummary{}, fmt.Errorf("backup mode for %s does not match manifest", entry.Path)
		}
		target, err := secureArchiveTarget(destination, entry.Path)
		if err != nil {
			return publicLocalBackupSummary{}, err
		}
		switch entry.Type {
		case "directory":
			if header.Typeflag != tar.TypeDir || header.Size != 0 {
				return publicLocalBackupSummary{}, fmt.Errorf("backup directory %s has an invalid header", entry.Path)
			}
			if err := os.MkdirAll(target, fs.FileMode(entry.Mode)); err != nil {
				return publicLocalBackupSummary{}, err
			}
		case "file":
			if header.Typeflag != tar.TypeReg || header.Size != entry.Size {
				return publicLocalBackupSummary{}, fmt.Errorf("backup file %s has an invalid header", entry.Path)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return publicLocalBackupSummary{}, err
			}
			output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fs.FileMode(entry.Mode))
			if err != nil {
				return publicLocalBackupSummary{}, err
			}
			hasher := sha256.New()
			written, copyErr := io.CopyN(io.MultiWriter(output, hasher), tarReader, entry.Size)
			closeErr := errors.Join(output.Sync(), output.Close())
			if copyErr != nil || closeErr != nil || written != entry.Size {
				return publicLocalBackupSummary{}, errors.Join(copyErr, closeErr)
			}
			if hex.EncodeToString(hasher.Sum(nil)) != entry.SHA256 {
				return publicLocalBackupSummary{}, fmt.Errorf("backup checksum mismatch for %s", entry.Path)
			}
			total += entry.Size
			if total > maxLocalBackupExtractBytes {
				return publicLocalBackupSummary{}, errors.New("backup exceeds the extraction byte limit")
			}
		}
	}
	if len(seen) != len(expected) {
		return publicLocalBackupSummary{}, errors.New("backup is missing a manifested path")
	}
	if _, err := io.Copy(io.Discard, gzipReader); err != nil {
		return publicLocalBackupSummary{}, err
	}
	if err := gzipReader.Close(); err != nil {
		return publicLocalBackupSummary{}, err
	}
	if _, err := buffered.Peek(1); !errors.Is(err, io.EOF) {
		return publicLocalBackupSummary{}, errors.New("backup contains a second gzip member or trailing data")
	}
	servicePath := filepath.Join(destination, "service", "service.db")
	if err := quickCheckSQLitePath(ctx, servicePath, "backup service database"); err != nil {
		return publicLocalBackupSummary{}, err
	}
	serviceResource, err := llmsqlite.Open(ctx, llmsqlite.Config{Path: servicePath})
	if err != nil {
		return publicLocalBackupSummary{}, fmt.Errorf("validate backup service schema: %w", err)
	}
	if err := serviceResource.Release(context.Background()); err != nil {
		return publicLocalBackupSummary{}, err
	}
	if manifest.StateEnabled {
		statePath := filepath.Join(destination, "worker", "worker-state.db")
		if err := quickCheckSQLitePath(ctx, statePath, "backup worker state database"); err != nil {
			return publicLocalBackupSummary{}, err
		}
		stateResource, err := workerkitsqlite.Open(ctx, workerkitsqlite.Config{Path: statePath})
		if err != nil {
			return publicLocalBackupSummary{}, fmt.Errorf("validate backup worker state schema: %w", err)
		}
		if err := stateResource.Release(context.Background()); err != nil {
			return publicLocalBackupSummary{}, err
		}
	}
	if _, found, err := readPublicCredentials(filepath.Join(destination, "credentials", "credentials.json")); err != nil || !found {
		if err == nil {
			err = errors.New("backup caller credential is missing")
		}
		return publicLocalBackupSummary{}, err
	}
	return publicLocalBackupSummary{Manifest: manifest, Bytes: total}, nil
}
