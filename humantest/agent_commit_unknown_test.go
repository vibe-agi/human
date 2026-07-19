package humantest_test

import (
	"context"
	"testing"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
)

// The Agent commit-unknown reconciliation kit must hold against the reference
// in-memory model; the same call is the entry point for third-party Stores.
func TestAgentCommitUnknownReconciliationAgainstMemoryModel(t *testing.T) {
	humantest.TestAgentCommitUnknownReconciliation(t, func(
		context.Context,
		testing.TB,
	) (agent.Store, framework.ReleaseFunc, error) {
		store, release := humantest.NewMemoryAgentStore()
		return store, release, nil
	})
}
