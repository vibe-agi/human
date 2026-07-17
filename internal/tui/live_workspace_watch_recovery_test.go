package tui

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	workmirror "github.com/vibe-agi/human/internal/mirror"
)

func TestLiveWorkspaceWatchErrorForcesAuthoritativeReview(t *testing.T) {
	model, _, namespace, events := liveWorkspaceGenerationFixture(t, false)

	updated, watchCommands := model.Update(mirrorWatchEvent{
		namespace: namespace,
		events:    events,
		event:     workmirror.WatchEvent{Err: errors.New("fsnotify overflow")},
		open:      true,
	})
	model = updated.(Model)
	generation := model.mirrorGeneration[namespace]
	if watchCommands == nil || generation == 0 || !model.mirrorDirty[namespace] ||
		model.mirrorReviewing[namespace] != generation {
		t.Fatalf("watch error did not schedule a full review: %+v", model)
	}
	if !strings.Contains(model.status, "full workspace review queued") {
		t.Fatalf("watch error status is not actionable: %q", model.status)
	}

	result := reviewMirrorAutomatically(model.mirrors[namespace], *model.active, generation)()
	updated, _ = model.Update(result)
	model = updated.(Model)
	if model.delivery.stage != deliveryReviewed || model.delivery.generation != generation ||
		len(model.delivery.changes) != 1 {
		t.Fatalf("authoritative review was not installed after watch error: %+v", model.delivery)
	}
}

func TestLiveWorkspaceClosedWatchBacksOffRecoversAndRejectsStaleWork(t *testing.T) {
	workspace := &watchRecoveryWorkspace{
		attempts: []watchRecoveryAttempt{
			{err: errors.New("watch start failed one")},
			{err: errors.New("watch start failed two")},
			{events: make(chan workmirror.WatchEvent)},
		},
	}
	assignment := workspaceAssignment(canonical.Request{})
	namespace := mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey)
	initialEvents := make(chan workmirror.WatchEvent)
	model := New(newFakeClient())
	model.active = &assignment
	model.rememberContext(assignment)
	model.mirrors[namespace] = workspace
	model.mirrorWatches[namespace] = mirrorWatchState{events: initialEvents}

	updated, _ := model.Update(mirrorWatchEvent{
		namespace: namespace, events: initialEvents, open: false,
	})
	model = updated.(Model)
	firstGeneration := model.mirrorGeneration[namespace]
	firstRetryID := model.mirrorWatches[namespace].startID
	if firstGeneration == 0 || model.mirrorWatches[namespace].failures != 1 ||
		!model.mirrorWatches[namespace].retryPending ||
		!strings.Contains(model.status, "event stream closed") {
		t.Fatalf("closed watch did not enter bounded recovery: %+v", model.mirrorWatches[namespace])
	}

	// The first two restart attempts fail. Each deterministic failure advances
	// the retry token and the bounded exponential delay instead of spinning.
	for failure := 1; failure <= 2; failure++ {
		state := model.mirrorWatches[namespace]
		updated, start := model.Update(mirrorWatchRetryReady{
			namespace: namespace, startID: state.startID,
		})
		model = updated.(Model)
		if start == nil || !model.mirrorWatches[namespace].starting {
			t.Fatalf("retry %d did not start watcher: %+v", failure, model.mirrorWatches[namespace])
		}
		updated, _ = model.Update(start())
		model = updated.(Model)
		wantFailures := failure + 1
		if model.mirrorWatches[namespace].failures != wantFailures ||
			!model.mirrorWatches[namespace].retryPending {
			t.Fatalf("retry %d failure did not back off: %+v", failure, model.mirrorWatches[namespace])
		}
	}
	if got, want := mirrorWatchRetryDelay(1), 100*time.Millisecond; got != want {
		t.Fatalf("first retry delay = %s, want %s", got, want)
	}
	if got, want := mirrorWatchRetryDelay(3), 400*time.Millisecond; got != want {
		t.Fatalf("third retry delay = %s, want %s", got, want)
	}
	if got := mirrorWatchRetryDelay(100); got != mirrorWatchRetryMax {
		t.Fatalf("retry delay did not cap: %s", got)
	}

	// A timer from an older recovery generation cannot start another watcher.
	before := workspace.watchCalls()
	updated, staleStart := model.Update(mirrorWatchRetryReady{
		namespace: namespace, startID: firstRetryID,
	})
	model = updated.(Model)
	if staleStart != nil || workspace.watchCalls() != before {
		t.Fatal("stale retry generation started a duplicate watcher")
	}

	state := model.mirrorWatches[namespace]
	updated, start := model.Update(mirrorWatchRetryReady{
		namespace: namespace, startID: state.startID,
	})
	model = updated.(Model)
	updated, followup := model.Update(start())
	model = updated.(Model)
	state = model.mirrorWatches[namespace]
	if state.events != workspace.attempts[2].events || state.starting || state.retryPending ||
		state.failures != 3 || followup == nil ||
		!strings.Contains(model.status, "watch recovered") {
		t.Fatalf("watch did not recover with a gap-closing scan: state=%+v status=%q", state, model.status)
	}
	defer state.cancel()

	// The review started at the first failure is stale after retries and
	// recovery. It must schedule exactly the latest generation instead of
	// clearing dirty state or installing an obsolete result.
	staleResult := reviewMirrorAutomatically(workspace, assignment, firstGeneration)()
	updated, latestReview := model.Update(staleResult)
	model = updated.(Model)
	latestGeneration := model.mirrorGeneration[namespace]
	if latestReview == nil || latestGeneration <= firstGeneration ||
		model.mirrorReviewing[namespace] != latestGeneration || model.delivery.stage != deliveryNone {
		t.Fatalf("stale recovery review displaced latest generation: %+v", model)
	}
	updated, _ = model.Update(latestReview())
	model = updated.(Model)
	if model.mirrorDirty[namespace] || len(model.mirrorReviewing) != 0 {
		t.Fatalf("latest recovery scan did not settle watcher state: %+v", model)
	}
}

func TestLiveWorkspaceRepeatedWatchErrorsDoNotDuplicateAutoDelivery(t *testing.T) {
	model, _, namespace, events := liveWorkspaceGenerationFixture(t, true)
	client := model.client.(*fakeClient)

	updated, _ := model.Update(mirrorWatchEvent{
		namespace: namespace, events: events,
		event: workmirror.WatchEvent{Err: errors.New("overflow one")}, open: true,
	})
	model = updated.(Model)
	staleGeneration := model.mirrorGeneration[namespace]
	staleResult := reviewMirrorAutomatically(model.mirrors[namespace], *model.active, staleGeneration)()

	updated, _ = model.Update(mirrorWatchEvent{
		namespace: namespace, events: events,
		event: workmirror.WatchEvent{Err: errors.New("overflow two")}, open: true,
	})
	model = updated.(Model)
	updated, latestReview := model.Update(staleResult)
	model = updated.(Model)
	if latestReview == nil || model.delivery.stage != deliveryNone {
		t.Fatalf("stale error review was installed or latest scan was not queued: %+v", model)
	}

	updated, confirm := model.Update(latestReview())
	model = updated.(Model)
	if confirm == nil || model.delivery.stage != deliveryConfirming {
		t.Fatalf("latest full scan did not enter one auto-delivery: %+v", model.delivery)
	}
	updated, send := model.Update(commandResult(t, confirm))
	model = updated.(Model)
	updated, _ = model.Update(commandResult(t, send))
	model = updated.(Model)
	if len(client.events) != 1 || model.delivery.stage != deliveryNone {
		t.Fatalf("auto-delivery count = %d, state=%+v", len(client.events), model.delivery)
	}

	updated, duplicate := model.Update(staleResult)
	model = updated.(Model)
	if duplicate != nil || len(client.events) != 1 {
		t.Fatalf("duplicate stale review emitted another delivery: command=%v events=%d", duplicate != nil, len(client.events))
	}
}

type watchRecoveryAttempt struct {
	events chan workmirror.WatchEvent
	err    error
}

type watchRecoveryWorkspace struct {
	mu       sync.Mutex
	attempts []watchRecoveryAttempt
	next     int
}

func (workspace *watchRecoveryWorkspace) Dir() string { return "" }

func (workspace *watchRecoveryWorkspace) ReconcileRequestForProfile(
	canonical.Request,
	*adapter.Profile,
	string,
) (workmirror.ReconcileReport, error) {
	return workmirror.ReconcileReport{}, nil
}

func (workspace *watchRecoveryWorkspace) Review() ([]workmirror.Change, error) { return nil, nil }

func (workspace *watchRecoveryWorkspace) Watch(
	ctx context.Context,
	_ time.Duration,
) (<-chan workmirror.WatchEvent, error) {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	if workspace.next >= len(workspace.attempts) {
		return nil, errors.New("unexpected extra watch attempt")
	}
	attempt := workspace.attempts[workspace.next]
	workspace.next++
	return attempt.events, attempt.err
}

func (workspace *watchRecoveryWorkspace) watchCalls() int {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	return workspace.next
}
