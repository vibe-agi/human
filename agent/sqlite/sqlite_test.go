package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/agent"
	agentsqlite "github.com/vibe-agi/human/agent/sqlite"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
)

func TestStoreConformance(t *testing.T) {
	humantest.TestAgentStore(t, func(
		ctx context.Context,
		test testing.TB,
	) (agent.Store, framework.ReleaseFunc, error) {
		resource, err := agentsqlite.Open(ctx, agentsqlite.Config{
			Path: filepath.Join(test.TempDir(), "agent.db"),
		})
		if err != nil {
			return nil, nil, err
		}
		store, err := resource.Value()
		if err != nil {
			_ = resource.Release(context.Background())
			return nil, nil, err
		}
		return store, resource.Release, nil
	})
}

func TestOwnedResourceReleaseAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	resource, err := agentsqlite.Open(t.Context(), agentsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if !resource.Owned() {
		t.Fatal("SQLite adapter returned a borrowed Resource")
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	if got := store.Description().Provider; got != "sqlite" {
		t.Fatalf("Store provider = %q, want sqlite", got)
	}

	if second, err := agentsqlite.Open(t.Context(), agentsqlite.Config{Path: path}); !errors.Is(err, agentsqlite.ErrDatabaseInUse) {
		if err == nil {
			_ = second.Release(t.Context())
		}
		t.Fatalf("second Open error = %v, want ErrDatabaseInUse", err)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := resource.Release(t.Context()); err != nil {
		t.Fatalf("idempotent Resource release: %v", err)
	}
	if _, err := resource.Value(); !errors.Is(err, framework.ErrResourceReleased) {
		t.Fatalf("Value after release = %v, want ErrResourceReleased", err)
	}
	if err := store.View(t.Context(), func(agent.StoreView) error { return nil }); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("retained Store after release = %v, want ErrStoreClosed", err)
	}

	reopened, err := agentsqlite.Open(t.Context(), agentsqlite.Config{Path: path})
	if err != nil {
		t.Fatalf("reopen released SQLite Store: %v", err)
	}
	if err := reopened.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAgentConsumesOwnedSQLiteStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	resource, err := agentsqlite.Open(t.Context(), agentsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	composedConfig := agent.DefaultConfig()
	composedConfig.Store = resource
	composed, err := agent.New(t.Context(), composedConfig)
	if err != nil {
		_ = resource.Release(t.Context())
		t.Fatalf("compose Agent with SQLite Store resource: %v", err)
	}
	if err := composed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := resource.Value(); !errors.Is(err, framework.ErrResourceReleased) {
		t.Fatalf("Agent did not release owned SQLite Store: %v", err)
	}

	reopened, err := agentsqlite.Open(t.Context(), agentsqlite.Config{Path: path})
	if err != nil {
		t.Fatalf("adapter Open after Agent Close: %v", err)
	}
	if err := reopened.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRequiresPathAndLiveContext(t *testing.T) {
	if _, err := agentsqlite.Open(t.Context(), agentsqlite.Config{}); !errors.Is(err, agent.ErrInvalidArgument) {
		t.Fatalf("Open empty path = %v, want ErrInvalidArgument", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := agentsqlite.Open(ctx, agentsqlite.Config{Path: filepath.Join(t.TempDir(), "agent.db")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open cancelled context = %v, want context.Canceled", err)
	}
}
