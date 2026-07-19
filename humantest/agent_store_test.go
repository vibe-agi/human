package humantest_test

import (
	"context"
	"testing"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
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
