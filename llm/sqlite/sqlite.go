// Package sqlite is HumanLLM's official single-process SQLite Store adapter.
// It is one official implementation of llm.Store, not a requirement imposed on
// applications: custom databases implement the same public port and run the
// same humantest conformance suite.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"github.com/vibe-agi/human/llm"
	_ "modernc.org/sqlite"
)

var (
	// ErrInvalidConfig means Open was given no usable independent database.
	ErrInvalidConfig = errors.New("llm/sqlite: invalid configuration")
	// ErrDatabaseInUse means another live resource owns the same file identity.
	ErrDatabaseInUse = errors.New("llm/sqlite: database is already in use")
)

// Config selects one dedicated HumanLLM persistence identity. Path may be an
// ordinary filesystem path, a file: URI, or :memory:. Shared-memory databases
// are rejected because they cannot satisfy the adapter's single-owner contract.
type Config struct {
	Path string
}

// Open constructs an owned Store resource. ctx bounds construction only. The
// caller must transfer the Resource to a HumanLLM runtime or release it.
func Open(ctx context.Context, config Config) (framework.Resource[llm.Store], error) {
	if ctx == nil {
		return framework.Resource[llm.Store]{}, fmt.Errorf("%w: context is required", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return framework.Resource[llm.Store]{}, err
	}
	path, err := resolvePath(config.Path)
	if err != nil {
		return framework.Resource[llm.Store]{}, err
	}
	location, err := sqlitefile.PreparePrivate(path, "HumanLLM database")
	if err != nil {
		return framework.Resource[llm.Store]{}, err
	}
	owner, err := ownerlock.Acquire(location, "HumanLLM database")
	if err != nil {
		if errors.Is(err, ownerlock.ErrInUse) {
			return framework.Resource[llm.Store]{}, fmt.Errorf("%w: %v", ErrDatabaseInUse, err)
		}
		return framework.Resource[llm.Store]{}, err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner {
			_ = owner.Close()
		}
	}()

	database, err := sql.Open("sqlite", location.OpenDSN())
	if err != nil {
		return framework.Resource[llm.Store]{}, fmt.Errorf("open HumanLLM SQLite: %w", err)
	}
	closeDatabase := true
	defer func() {
		if closeDatabase {
			_ = database.Close()
		}
	}()
	// One connection gives callback transactions a simple strict serial order.
	// WAL still makes crash recovery and future read-pool evolution explicit.
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if location.FileBacked {
		var mode string
		if err := database.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
			return framework.Resource[llm.Store]{}, fmt.Errorf("enable HumanLLM SQLite WAL: %w", err)
		}
		if !strings.EqualFold(mode, "wal") {
			return framework.Resource[llm.Store]{}, fmt.Errorf("enable HumanLLM SQLite WAL: got %q", mode)
		}
	}
	for _, pragma := range []string{
		"PRAGMA synchronous = FULL",
		"PRAGMA secure_delete = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := database.ExecContext(ctx, pragma); err != nil {
			return framework.Resource[llm.Store]{}, fmt.Errorf("configure HumanLLM SQLite: %w", err)
		}
	}
	if err := requireCurrentOrEmptySchema(ctx, database); err != nil {
		return framework.Resource[llm.Store]{}, err
	}
	if _, err := database.ExecContext(ctx, schema); err != nil {
		return framework.Resource[llm.Store]{}, fmt.Errorf("initialize HumanLLM SQLite schema: %w", err)
	}

	store := &store{database: database}
	resource, err := framework.Own[llm.Store](store, func(context.Context) error {
		store.closed.Store(true)
		return errors.Join(database.Close(), owner.Close())
	})
	if err != nil {
		return framework.Resource[llm.Store]{}, fmt.Errorf("own HumanLLM SQLite Store: %w", err)
	}
	closeDatabase = false
	releaseOwner = false
	return resource, nil
}

func resolvePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("%w: SQLite path is required", ErrInvalidConfig)
	}
	if path == ":memory:" || strings.HasPrefix(strings.ToLower(path), "file:") {
		return path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve HumanLLM SQLite path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", fmt.Errorf("create HumanLLM SQLite directory: %w", err)
	}
	return absolute, nil
}

type store struct {
	database *sql.DB
	closed   atomic.Bool
}

var _ llm.Store = (*store)(nil)
