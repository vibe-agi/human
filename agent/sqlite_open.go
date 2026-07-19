package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	_ "modernc.org/sqlite"
)

// OpenSQLiteStore is the low-level composition primitive behind the official
// agent/sqlite adapter and Agent's DatabasePath convenience branch. It opens a
// dedicated, single-owner SQLite persistence domain and returns it as an owned
// Resource. Releasing the Resource first stops Store admission, then closes the
// database and releases its cross-process ownership lock.
//
// Applications should normally call sqlite.Open from the agent/sqlite package.
// This function is public because that package must depend on agent's Store
// contract and Go packages cannot otherwise share the in-package Store bridge
// without either an import cycle or exposing *sql.DB. It intentionally exposes
// neither the database handle nor its canonical DSN.
//
// ctx bounds construction only and is not retained after this function returns.
func OpenSQLiteStore(ctx context.Context, path string) (framework.Resource[Store], error) {
	if ctx == nil {
		return framework.Resource[Store]{}, fmt.Errorf("%w: context is required", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return framework.Resource[Store]{}, err
	}
	resolvedPath, err := resolveAgentDatabasePath(path)
	if err != nil {
		return framework.Resource[Store]{}, err
	}
	location, err := sqlitefile.PreparePrivate(resolvedPath, "HumanAgent database")
	if err != nil {
		return framework.Resource[Store]{}, err
	}
	owner, err := ownerlock.Acquire(location, "HumanAgent database")
	if err != nil {
		if errors.Is(err, ownerlock.ErrInUse) {
			return framework.Resource[Store]{}, fmt.Errorf("%w: %v", ErrDatabaseInUse, err)
		}
		return framework.Resource[Store]{}, err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner {
			_ = owner.Close()
		}
	}()

	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		return framework.Resource[Store]{}, fmt.Errorf("open HumanAgent sqlite: %w", err)
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
			return framework.Resource[Store]{}, fmt.Errorf("configure HumanAgent sqlite: %w", err)
		}
	}
	if err := requireCurrentOrEmptySchema(ctx, database); err != nil {
		return framework.Resource[Store]{}, err
	}
	if _, err := database.ExecContext(ctx, agentSchema); err != nil {
		return framework.Resource[Store]{}, fmt.Errorf("initialize HumanAgent sqlite schema: %w", err)
	}

	store := newSQLiteStore(database)
	resource, err := framework.Own[Store](store, func(context.Context) error {
		store.close()
		return errors.Join(database.Close(), owner.Close())
	})
	if err != nil {
		return framework.Resource[Store]{}, fmt.Errorf("own HumanAgent sqlite Store: %w", err)
	}
	closeDatabase = false
	releaseOwner = false
	return resource, nil
}

func resolveAgentDatabasePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("%w: HumanAgent SQLite path is required", ErrInvalidArgument)
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
