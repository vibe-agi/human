package humantest_test

import (
	"context"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
)

// The commit-unknown reconciliation kit must hold against the reference
// in-memory model; the same call is the entry point for third-party Stores.
func TestLLMServiceCommitUnknownReconciliationAgainstMemoryModel(t *testing.T) {
	humantest.TestLLMServiceCommitUnknownReconciliation(t, func(
		context.Context,
		testing.TB,
	) (llm.Store, framework.ReleaseFunc, error) {
		store, release := humantest.NewMemoryLLMStore()
		return store, release, nil
	})
}
