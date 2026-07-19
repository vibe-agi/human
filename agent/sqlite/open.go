package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	_ "modernc.org/sqlite"
)

// ErrDatabaseInUse means another live resource owns the same SQLite file.
var ErrDatabaseInUse = errors.New("agent/sqlite: database is already in use")

// Config selects one dedicated HumanAgent SQLite persistence identity.
// Path may be an ordinary filesystem path or an independent :memory: database.
// Shared-memory SQLite DSNs are rejected because they cannot provide the
// adapter's single-owner lifecycle guarantee.
type Config struct {
	Path string
}

// Open constructs an owned SQLite Store resource. The returned Resource must
// either be passed to an Agent as owned configuration or released explicitly.
// ctx bounds construction only and is not retained.
func Open(ctx context.Context, config Config) (framework.Resource[agent.Store], error) {
	if ctx == nil {
		return framework.Resource[agent.Store]{}, fmt.Errorf("%w: context is required", agent.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return framework.Resource[agent.Store]{}, err
	}
	resolvedPath, err := resolveDatabasePath(config.Path)
	if err != nil {
		return framework.Resource[agent.Store]{}, err
	}
	location, err := sqlitefile.PreparePrivate(resolvedPath, "HumanAgent database")
	if err != nil {
		return framework.Resource[agent.Store]{}, err
	}
	owner, err := ownerlock.Acquire(location, "HumanAgent database")
	if err != nil {
		if errors.Is(err, ownerlock.ErrInUse) {
			return framework.Resource[agent.Store]{}, fmt.Errorf("%w: %v", ErrDatabaseInUse, err)
		}
		return framework.Resource[agent.Store]{}, err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner {
			_ = owner.Close()
		}
	}()

	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		return framework.Resource[agent.Store]{}, fmt.Errorf("open HumanAgent sqlite: %w", err)
	}
	closeDatabase := true
	defer func() {
		if closeDatabase {
			_ = database.Close()
		}
	}()
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode = DELETE",
		"PRAGMA synchronous = FULL",
		"PRAGMA secure_delete = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := database.ExecContext(ctx, pragma); err != nil {
			return framework.Resource[agent.Store]{}, fmt.Errorf("configure HumanAgent sqlite: %w", err)
		}
	}
	if err := requireCurrentOrEmptySchema(ctx, database); err != nil {
		return framework.Resource[agent.Store]{}, err
	}
	if _, err := database.ExecContext(ctx, agentSchema); err != nil {
		return framework.Resource[agent.Store]{}, fmt.Errorf("initialize HumanAgent sqlite schema: %w", err)
	}

	store := newSQLiteStore(database)
	resource, err := framework.Own[agent.Store](store, func(context.Context) error {
		store.close()
		return errors.Join(database.Close(), owner.Close())
	})
	if err != nil {
		return framework.Resource[agent.Store]{}, fmt.Errorf("own HumanAgent sqlite Store: %w", err)
	}
	closeDatabase = false
	releaseOwner = false
	return resource, nil
}

func resolveDatabasePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("%w: HumanAgent SQLite path is required", agent.ErrInvalidArgument)
	}
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve HumanAgent database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", fmt.Errorf("create HumanAgent database directory: %w", err)
	}
	return absolute, nil
}
