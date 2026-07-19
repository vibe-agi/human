package humantest_test

import (
	"context"
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
