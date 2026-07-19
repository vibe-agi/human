package humantest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/workspace"
)

func TestAgentStoreConformanceSuiteAgainstMemoryModel(t *testing.T) {
	humantest.TestAgentStore(t, func(
		context.Context,
		testing.TB,
	) (agent.Store, framework.ReleaseFunc, error) {
		store, release := humantest.NewMemoryAgentStore()
		return store, release, nil
	})
}

func TestMemoryAgentStoreImageReopensCommittedState(t *testing.T) {
	image := humantest.NewMemoryAgentStoreImage()
	first, releaseFirst, err := image.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := image.Open(); !errors.Is(err, humantest.ErrMemoryAgentStoreImageInUse) {
		t.Fatalf("open live image = %v, want ErrMemoryAgentStoreImageInUse", err)
	}
	alias := *first

	record := agent.StoreWorkspaceHeadRecord{
		Head: agent.WorkspaceHead{
			Workspace:         agent.WorkspaceRef{Authority: "authority-image", ID: "workspace-image"},
			ConfirmedRevision: workspace.Revision("revision-image"),
			UpdatedAt:         time.Unix(1_720_000_000, 0).UTC(),
		},
	}
	if err := first.Update(t.Context(), func(tx agent.StoreTx) error {
		_, err := tx.InsertWorkspaceHead(record)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := releaseFirst(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := first.View(t.Context(), func(agent.StoreView) error { return nil }); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("released first handle = %v, want ErrStoreClosed", err)
	}
	if err := alias.Update(t.Context(), func(agent.StoreTx) error { return nil }); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("copied alias after release = %v, want ErrStoreClosed", err)
	}

	second, releaseSecond, err := image.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := releaseSecond(context.Background()); err != nil {
			t.Errorf("release reopened memory Agent Store: %v", err)
		}
	})
	if second == first {
		t.Fatal("reopening an image reused the released Store runtime handle")
	}
	if err := alias.View(t.Context(), func(agent.StoreView) error { return nil }); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("copied alias after image reopen = %v, want ErrStoreClosed", err)
	}
	if err := second.View(t.Context(), func(view agent.StoreView) error {
		loaded, err := view.LoadWorkspaceHead(record.Head.Workspace)
		if err != nil {
			return err
		}
		if loaded != record {
			t.Fatalf("reopened record = %#v, want %#v", loaded, record)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryAgentStoreImageAbandonInvalidatesGeneration(t *testing.T) {
	image := humantest.NewMemoryAgentStoreImage()
	first, releaseFirst, err := image.Open()
	if err != nil {
		t.Fatal(err)
	}
	alias := *first
	if err := image.Abandon(first); err != nil {
		t.Fatal(err)
	}
	if err := alias.View(t.Context(), func(agent.StoreView) error { return nil }); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("abandoned alias = %v, want ErrStoreClosed", err)
	}
	second, releaseSecond, err := image.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = releaseSecond(context.Background()) })
	if err := releaseFirst(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := image.Open(); !errors.Is(err, humantest.ErrMemoryAgentStoreImageInUse) {
		t.Fatalf("late old release changed new owner: %v", err)
	}
	if err := second.View(t.Context(), func(agent.StoreView) error { return nil }); err != nil {
		t.Fatalf("new generation after late old release: %v", err)
	}
}
