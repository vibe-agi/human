package humantest_test

import (
	"context"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm/workerws"
)

func TestLLMWorkerJournalConformanceSuiteAgainstMemoryModel(t *testing.T) {
	humantest.TestLLMWorkerJournal(t, func(
		context.Context,
		testing.TB,
	) (workerws.Journal, framework.ReleaseFunc, error) {
		journal, release := humantest.NewMemoryLLMWorkerJournal()
		return journal, release, nil
	})
}

func TestLLMWorkerJournalConformanceSuiteAgainstShortPageMemoryModel(t *testing.T) {
	humantest.TestLLMWorkerJournal(t, func(
		context.Context,
		testing.TB,
	) (workerws.Journal, framework.ReleaseFunc, error) {
		journal, release := humantest.NewMemoryLLMWorkerJournal()
		return humantest.ShortPageLLMWorkerJournal{Journal: journal, PageSize: 1}, release, nil
	})
}
