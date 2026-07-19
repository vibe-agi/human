package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vibe-agi/human/agent"
	agentsqlite "github.com/vibe-agi/human/agent/sqlite"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/workspace"
)

const (
	agentStoreCrashChildEnv = "HUMAN_AGENT_SQLITE_STORE_CRASH_CHILD"
	agentStoreCrashPathEnv  = "HUMAN_AGENT_SQLITE_STORE_CRASH_PATH"
	agentStoreCrashExitCode = 95
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

func TestStoreRecoversCommittedTransactionAfterAbruptProcessExit(t *testing.T) {
	if os.Getenv(agentStoreCrashChildEnv) == "1" {
		if err := seedAgentStoreBeforeCrash(os.Getenv(agentStoreCrashPathEnv)); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(agentStoreCrashExitCode + 1)
		}
		os.Exit(agentStoreCrashExitCode) // intentionally bypass Resource.Release
	}

	path := filepath.Join(t.TempDir(), "agent-crash.db")
	command := exec.Command(os.Args[0], "-test.run=^TestStoreRecoversCommittedTransactionAfterAbruptProcessExit$", "-test.count=1")
	command.Env = append(os.Environ(), agentStoreCrashChildEnv+"=1", agentStoreCrashPathEnv+"="+path)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != agentStoreCrashExitCode {
		t.Fatalf("crash child = %v (exit %d), want %d; output:\n%s", err, agentStoreProcessExitCode(exit), agentStoreCrashExitCode, output)
	}

	resource, err := agentsqlite.Open(t.Context(), agentsqlite.Config{Path: path})
	if err != nil {
		t.Fatalf("open Agent Store after process crash: %v", err)
	}
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	want := agentCrashWorkspaceHead()
	if err := store.View(t.Context(), func(view agent.StoreView) error {
		got, err := view.LoadWorkspaceHead(want.Head.Workspace)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("workspace head after crash = %#v, want %#v", got, want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func seedAgentStoreBeforeCrash(path string) error {
	resource, err := agentsqlite.Open(context.Background(), agentsqlite.Config{Path: path})
	if err != nil {
		return err
	}
	store, err := resource.Value()
	if err != nil {
		return err
	}
	record := agentCrashWorkspaceHead()
	return store.Update(context.Background(), func(tx agent.StoreTx) error {
		_, err := tx.InsertWorkspaceHead(record)
		return err
	})
}

func agentCrashWorkspaceHead() agent.StoreWorkspaceHeadRecord {
	return agent.StoreWorkspaceHeadRecord{Head: agent.WorkspaceHead{
		Workspace:         agent.WorkspaceRef{Authority: "authority-crash", ID: "workspace-crash"},
		ConfirmedRevision: workspace.Revision("revision-crash"),
		UpdatedAt:         time.Date(2026, 7, 19, 12, 0, 0, 123, time.UTC),
	}}
}

func agentStoreProcessExitCode(err *exec.ExitError) int {
	if err == nil {
		return 0
	}
	return err.ExitCode()
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
