package humantest_test

import (
	"context"
	"testing"

	"github.com/vibe-agi/human/agent/workerws"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
)

func TestAgentWorkerJournalConformanceSuiteAgainstMemoryModel(t *testing.T) {
	humantest.TestAgentWorkerJournal(t, func(
		context.Context,
		testing.TB,
	) (workerws.Journal, framework.ReleaseFunc, error) {
		journal, release := humantest.NewMemoryAgentWorkerJournal()
		return journal, release, nil
	})
}
