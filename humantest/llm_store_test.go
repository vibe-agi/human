package humantest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
)

func TestLLMStoreConformanceSuiteAgainstMemoryModel(t *testing.T) {
	humantest.TestLLMStore(t, func(
		context.Context,
		testing.TB,
	) (llm.Store, framework.ReleaseFunc, error) {
		store, release := humantest.NewMemoryLLMStore()
		return store, release, nil
	})
}

func TestMemoryLLMStoreReleaseIsIdempotent(t *testing.T) {
	store, release := humantest.NewMemoryLLMStore()
	if store == nil || release == nil {
		t.Fatal("NewMemoryLLMStore returned a nil Store or release function")
	}
	if err := release(t.Context()); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := release(t.Context()); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if err := store.View(t.Context(), func(llm.StoreView) error { return nil }); err != llm.ErrStoreClosed {
		t.Fatalf("View after release error = %v, want ErrStoreClosed", err)
	}
	if err := store.Update(t.Context(), func(llm.StoreTx) error { return nil }); err != llm.ErrStoreClosed {
		t.Fatalf("Update after release error = %v, want ErrStoreClosed", err)
	}
}

func TestMemoryLLMStoreImageReopensCommittedStateWithFreshHandle(t *testing.T) {
	image := humantest.NewMemoryLLMStoreImage()
	first, releaseFirst := image.Open()
	alias := *first
	binding := llm.StoreBinding{DeploymentID: "restartable-memory-image"}
	if err := first.Bind(t.Context(), binding); err != nil {
		t.Fatalf("bind first handle: %v", err)
	}
	if err := releaseFirst(t.Context()); err != nil {
		t.Fatalf("release first handle: %v", err)
	}
	if err := first.View(t.Context(), func(llm.StoreView) error { return nil }); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("first handle after release = %v, want ErrStoreClosed", err)
	}
	if err := alias.Update(t.Context(), func(llm.StoreTx) error { return nil }); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("copied alias after release = %v, want ErrStoreClosed", err)
	}

	second, releaseSecond := image.Open()
	t.Cleanup(func() {
		if err := releaseSecond(context.Background()); err != nil {
			t.Errorf("release second handle: %v", err)
		}
	})
	if first == second {
		t.Fatal("image reopen returned the released process handle")
	}
	if err := alias.View(t.Context(), func(llm.StoreView) error { return nil }); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("copied alias after image reopen = %v, want ErrStoreClosed", err)
	}
	if err := second.Bind(t.Context(), binding); err != nil {
		t.Fatalf("reopened handle lost exact deployment binding: %v", err)
	}
	if err := second.Bind(t.Context(), llm.StoreBinding{DeploymentID: "different-deployment"}); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("reopened handle lost conflicting deployment binding: %v", err)
	}
}

func TestMemoryLLMStoreImageAbandonPreservesCommittedBinding(t *testing.T) {
	image := humantest.NewMemoryLLMStoreImage()
	first, releaseFirst := image.Open()
	binding := llm.StoreBinding{DeploymentID: "abandoned-memory-image"}
	if err := first.Bind(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	alias := *first
	if err := image.Abandon(first); err != nil {
		t.Fatal(err)
	}
	if err := alias.View(t.Context(), func(llm.StoreView) error { return nil }); !errors.Is(err, llm.ErrStoreClosed) {
		t.Fatalf("abandoned alias = %v, want ErrStoreClosed", err)
	}
	second, releaseSecond := image.Open()
	t.Cleanup(func() { _ = releaseSecond(context.Background()) })
	if err := releaseFirst(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := second.Bind(t.Context(), binding); err != nil {
		t.Fatalf("binding after abandoned generation: %v", err)
	}
}
