package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/workerkit"
	statesqlite "github.com/vibe-agi/human/workerkit/sqlite"
)

func TestStateStoreConformance(t *testing.T) {
	humantest.TestWorkerStateStore(t, func(
		ctx context.Context,
		test testing.TB,
	) (workerkit.StateStore, framework.ReleaseFunc, error) {
		resource, err := statesqlite.Open(ctx, statesqlite.Config{
			Path: filepath.Join(test.TempDir(), "state.db"),
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

func TestStateStoreReleaseAndReopenKeepsConversations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	first, err := statesqlite.Open(t.Context(), statesqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	store, err := first.Value()
	if err != nil {
		t.Fatal(err)
	}
	saved := workerkit.Conversation{
		Key:   workerkit.ConversationKey{Caller: "caller-a", TaskID: "task-1"},
		Phase: workerkit.PhaseAwaitingResults,
		Draft: "survives restart",
	}
	if err := store.SaveConversation(t.Context(), saved); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	second, err := statesqlite.Open(t.Context(), statesqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Release(context.Background()) })
	reopened, err := second.Value()
	if err != nil {
		t.Fatal(err)
	}
	listed, err := reopened.ListConversations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Key != saved.Key ||
		listed[0].Phase != workerkit.PhaseAwaitingResults || listed[0].Draft != "survives restart" {
		t.Fatalf("reopened conversations = %+v", listed)
	}
}

func TestStateStoreSingleOwnerAndForeignSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	first, err := statesqlite.Open(t.Context(), statesqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Release(context.Background()) })
	if _, err := statesqlite.Open(t.Context(), statesqlite.Config{Path: path}); !errors.Is(err, statesqlite.ErrDatabaseInUse) {
		t.Fatalf("second live owner = %v, want ErrDatabaseInUse", err)
	}
}
