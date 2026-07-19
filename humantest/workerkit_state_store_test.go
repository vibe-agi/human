package humantest_test

import (
	"context"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/workerkit"
)

func TestWorkerStateStoreConformanceAgainstMemoryModel(t *testing.T) {
	humantest.TestWorkerStateStore(t, func(
		context.Context,
		testing.TB,
	) (workerkit.StateStore, framework.ReleaseFunc, error) {
		store, release := workerkit.NewMemoryStateStore()
		return store, release, nil
	})
}
