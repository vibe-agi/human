package humantest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vibe-agi/human/agent/workerws"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
)

func TestMemoryAgentWorkerJournalCopiedHandleClosesWithOwner(t *testing.T) {
	image := humantest.NewMemoryAgentWorkerJournalImage()
	journal, release, err := image.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Bind(t.Context(), workerws.JournalBinding{
		Gateway: "gateway-copy", Authority: "authority-copy", Worker: "worker-copy",
	}); err != nil {
		t.Fatal(err)
	}
	alias := *journal
	if err := release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := alias.ListAssignments(t.Context(), 0, 1); !errors.Is(err, workerws.ErrJournalClosed) {
		t.Fatalf("copied alias after release = %v, want ErrJournalClosed", err)
	}
	reopened, releaseReopened, err := image.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = releaseReopened(context.Background()) })
	if reopened == journal {
		t.Fatal("image reopen returned released Journal handle")
	}
	if _, err := alias.ListAssignments(t.Context(), 0, 1); !errors.Is(err, workerws.ErrJournalClosed) {
		t.Fatalf("copied alias after image reopen = %v, want ErrJournalClosed", err)
	}
}

func TestMemoryAgentWorkerJournalAbandonDoesNotLetLateReleaseCloseNewOwner(t *testing.T) {
	image := humantest.NewMemoryAgentWorkerJournalImage()
	first, releaseFirst, err := image.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	alias := *first
	if err := image.Abandon(first); err != nil {
		t.Fatal(err)
	}
	if _, err := alias.ListAssignments(t.Context(), 0, 1); !errors.Is(err, workerws.ErrJournalClosed) {
		t.Fatalf("abandoned alias = %v, want ErrJournalClosed", err)
	}
	second, releaseSecond, err := image.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = releaseSecond(context.Background()) })
	if err := releaseFirst(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := image.Open(t.Context()); err == nil {
		t.Fatal("late old release cleared the new Journal owner")
	}
	if err := second.Bind(t.Context(), workerws.JournalBinding{
		Gateway: "gateway-abandon", Authority: "authority-abandon", Worker: "worker-abandon",
	}); err != nil {
		t.Fatalf("new Journal generation after late old release: %v", err)
	}
}

func TestAgentWorkerJournalConformanceSuiteAgainstMemoryModel(t *testing.T) {
	humantest.TestAgentWorkerJournal(t, func(
		context.Context,
		testing.TB,
	) (workerws.Journal, framework.ReleaseFunc, error) {
		journal, release := humantest.NewMemoryAgentWorkerJournal()
		return journal, release, nil
	})
}

func TestAgentWorkerJournalRecoverySuiteAgainstMemoryImage(t *testing.T) {
	humantest.TestAgentWorkerJournalRecovery(t, func(
		_ context.Context,
		_ testing.TB,
	) (humantest.AgentWorkerJournalRecoveryOpener, error) {
		image := humantest.NewMemoryAgentWorkerJournalImage()
		return func(ctx context.Context) (
			workerws.Journal,
			framework.ReleaseFunc,
			error,
		) {
			return image.Open(ctx)
		}, nil
	})
}
