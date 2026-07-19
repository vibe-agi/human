package humantest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/workspace"
)

// WorkspaceStoreFactory opens a new, empty workspace Store for one
// conformance subtest. Every invocation must return an independent Store.
// release must be non-nil, idempotent, and release every resource opened by the
// factory. TestWorkspaceStore always invokes it during cleanup and may invoke
// it earlier to verify the released Store's error classification.
//
// A provider normally exposes a test like:
//
//	func TestStoreConformance(t *testing.T) {
//		humantest.TestWorkspaceStore(t, func(ctx context.Context, t testing.TB) (
//			workspace.Store, framework.ReleaseFunc, error,
//		) {
//			return openTestStore(ctx, t.TempDir())
//		})
//	}
//
// The context bounds construction and the subtest and must not be retained.
// release receives a fresh context so a cancelled operation cannot prevent
// deterministic cleanup.
type WorkspaceStoreFactory func(
	context.Context,
	testing.TB,
) (workspace.Store, framework.ReleaseFunc, error)

// TestWorkspaceStore runs the mandatory black-box conformance suite for a
// workspace Store. It verifies validation and absence, durable-before-effect
// visibility, exact replay, conflicting identity rejection, callback failure
// containment, concurrent at-most-once CAS, byte ownership, contract metadata,
// and post-release error classification.
//
// Passing this suite is necessary but does not replace a provider's physical
// durability, process-crash, migration, and infrastructure fault tests. In
// particular, recovered pending records require a provider-specific test that
// reopens the same physical namespace after a simulated crash.
func TestWorkspaceStore(t *testing.T, factory WorkspaceStoreFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("workspace Store conformance factory is nil")
	}

	tests := []struct {
		name string
		run  func(context.Context, *testing.T, workspace.Store, framework.ReleaseFunc)
	}{
		{"description_and_contract", adaptWorkspaceStoreTest(testWorkspaceStoreDescription)},
		{"lookup_absence_and_validation", adaptWorkspaceStoreTest(testWorkspaceStoreLookupAndValidation)},
		{"persist_before_cas_and_exact_replay", adaptWorkspaceStoreTest(testWorkspaceStoreExactReplay)},
		{"different_intent_conflicts", adaptWorkspaceStoreTest(testWorkspaceStoreIntentConflict)},
		{"callback_failures_are_indeterminate", adaptWorkspaceStoreTest(testWorkspaceStoreCallbackFailures)},
		{"concurrent_exact_apply_executes_once", adaptWorkspaceStoreTest(testWorkspaceStoreConcurrentApply)},
		{"byte_ownership", adaptWorkspaceStoreTest(testWorkspaceStoreByteOwnership)},
		{"released_store_is_closed", testWorkspaceStoreReleased},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			t.Cleanup(cancel)

			store, release, err := factory(ctx, t)
			if release != nil {
				t.Cleanup(func() {
					releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer releaseCancel()
					if err := release(releaseCtx); err != nil {
						t.Errorf("release workspace Store: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatalf("open fresh workspace Store: %v", err)
			}
			if store == nil {
				t.Fatal("factory returned a nil workspace Store")
			}
			if release == nil {
				t.Fatal("factory returned a nil release function")
			}

			test.run(ctx, t, store, release)
		})
	}
}

func adaptWorkspaceStoreTest(
	test func(context.Context, *testing.T, workspace.Store),
) func(context.Context, *testing.T, workspace.Store, framework.ReleaseFunc) {
	return func(ctx context.Context, t *testing.T, store workspace.Store, _ framework.ReleaseFunc) {
		test(ctx, t, store)
	}
}

func testWorkspaceStoreDescription(_ context.Context, t *testing.T, store workspace.Store) {
	t.Helper()
	first := store.Description()
	if err := first.Validate(); err != nil {
		t.Fatalf("Description does not satisfy workspace Store contract: %v", err)
	}
	second := store.Description()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Description is not static:\nfirst:  %#v\nsecond: %#v", first, second)
	}
	if _, err := framework.Negotiate(first.Contract, workspace.StoreRequirements()); err != nil {
		t.Fatalf("Description contract negotiation: %v", err)
	}
}

func testWorkspaceStoreLookupAndValidation(ctx context.Context, t *testing.T, store workspace.Store) {
	t.Helper()
	identity := workspaceConformanceIdentity("absent")
	if _, err := store.Lookup(nil, identity); !errors.Is(err, workspace.ErrInvalidApply) {
		t.Fatalf("Lookup nil context error = %v, want ErrInvalidApply", err)
	}
	if _, err := store.Lookup(ctx, identity); !errors.Is(err, workspace.ErrApplyNotFound) {
		t.Fatalf("Lookup absent error = %v, want ErrApplyNotFound", err)
	}

	invalidIdentity := identity
	invalidIdentity.Artifact = ""
	if _, err := store.Lookup(ctx, invalidIdentity); !errors.Is(err, workspace.ErrInvalidApply) {
		t.Fatalf("Lookup invalid identity error = %v, want ErrInvalidApply", err)
	}

	invalidIntent := workspaceConformanceIntent("invalid", "payload")
	invalidIntent.Payload.Data[0] = 'X' // Deliberately leave PayloadDigest stale.
	var calls atomic.Int32
	applier := workspace.CASApplierFunc(func(context.Context, workspace.ApplyIntent) (workspace.CASOutcome, error) {
		calls.Add(1)
		return workspace.CASOutcome{}, nil
	})
	if _, err := store.Apply(nil, workspaceConformanceIntent("nil-context", "payload"), applier); !errors.Is(err, workspace.ErrInvalidApply) {
		t.Fatalf("Apply nil context error = %v, want ErrInvalidApply", err)
	}
	_, err := store.Apply(ctx, invalidIntent, applier)
	if !errors.Is(err, workspace.ErrInvalidApply) {
		t.Fatalf("Apply invalid intent error = %v, want ErrInvalidApply", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("invalid intent invoked CAS %d times, want zero", got)
	}
	if _, err := store.Lookup(ctx, invalidIntent.Identity); !errors.Is(err, workspace.ErrApplyNotFound) {
		t.Fatalf("invalid intent was persisted: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	canceledIntent := workspaceConformanceIntent("canceled", "payload")
	if _, err := store.Apply(canceled, canceledIntent, applier); !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply canceled context error = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("invalid/canceled calls invoked CAS %d times, want zero", got)
	}
	if _, err := store.Lookup(ctx, canceledIntent.Identity); !errors.Is(err, workspace.ErrApplyNotFound) {
		t.Fatalf("canceled intent was persisted: %v", err)
	}
}

func testWorkspaceStoreExactReplay(ctx context.Context, t *testing.T, store workspace.Store) {
	t.Helper()
	intent := workspaceConformanceIntent("exact", "exact payload")
	var calls atomic.Int32
	first, err := store.Apply(ctx, intent, workspace.CASApplierFunc(func(_ context.Context, received workspace.ApplyIntent) (workspace.CASOutcome, error) {
		calls.Add(1)
		pending, lookupErr := store.Lookup(context.Background(), intent.Identity)
		if lookupErr != nil {
			return workspace.CASOutcome{}, fmt.Errorf("lookup pending intent from CAS callback: %w", lookupErr)
		}
		if pending.State != workspace.ApplyStatePending || pending.Outcome != nil || pending.CompletedAt != nil {
			return workspace.CASOutcome{}, fmt.Errorf("CAS callback observed non-pending record: %#v", pending)
		}
		if !reflect.DeepEqual(received, intent) {
			return workspace.CASOutcome{}, fmt.Errorf("CAS callback intent differs from submitted intent")
		}
		return workspace.CASOutcome{
			Decision:         workspace.ApplySuccess,
			ObservedRevision: intent.ResultRevision,
			Code:             "applied",
			Message:          "workspace CAS committed",
		}, nil
	}))
	if err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	assertWorkspaceTerminal(t, first.Record, workspace.ApplyStateSuccess, workspace.ApplySuccess)
	if first.Replay {
		t.Fatal("first Apply was reported as replay")
	}

	replayed, err := store.Apply(ctx, workspace.CloneApplyIntent(intent), workspace.CASApplierFunc(func(context.Context, workspace.ApplyIntent) (workspace.CASOutcome, error) {
		calls.Add(1)
		return workspace.CASOutcome{}, errors.New("exact replay invoked CAS")
	}))
	if err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if !replayed.Replay {
		t.Fatal("exact retry was not reported as replay")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("exact retry CAS calls = %d, want one total", got)
	}
	if !reflect.DeepEqual(replayed.Record, first.Record) {
		t.Fatalf("exact replay changed durable record:\nfirst:  %#v\nreplay: %#v", first.Record, replayed.Record)
	}

	lookedUp, err := store.Lookup(ctx, intent.Identity)
	if err != nil {
		t.Fatalf("Lookup terminal record: %v", err)
	}
	if !reflect.DeepEqual(lookedUp, first.Record) {
		t.Fatalf("Lookup differs from Apply result:\napply:  %#v\nlookup: %#v", first.Record, lookedUp)
	}
}

func testWorkspaceStoreIntentConflict(ctx context.Context, t *testing.T, store workspace.Store) {
	t.Helper()
	firstIntent := workspaceConformanceIntent("conflict", "first payload")
	if _, err := store.Apply(ctx, firstIntent, workspace.CASApplierFunc(func(context.Context, workspace.ApplyIntent) (workspace.CASOutcome, error) {
		return workspace.CASOutcome{Decision: workspace.ApplyRejected, Code: "policy", Message: "not allowed"}, nil
	})); err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	conflicting := workspaceConformanceIntent("conflict", "different payload")
	var calls atomic.Int32
	_, err := store.Apply(ctx, conflicting, workspace.CASApplierFunc(func(context.Context, workspace.ApplyIntent) (workspace.CASOutcome, error) {
		calls.Add(1)
		return workspace.CASOutcome{}, nil
	}))
	if !errors.Is(err, workspace.ErrApplyIntentConflict) {
		t.Fatalf("different intent error = %v, want ErrApplyIntentConflict", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("different intent invoked CAS %d times, want zero", got)
	}
	stored, err := store.Lookup(ctx, firstIntent.Identity)
	if err != nil {
		t.Fatalf("Lookup original intent: %v", err)
	}
	if !reflect.DeepEqual(stored.Intent, firstIntent) {
		t.Fatalf("conflicting retry replaced original intent:\nwant: %#v\ngot:  %#v", firstIntent, stored.Intent)
	}
}

func testWorkspaceStoreCallbackFailures(ctx context.Context, t *testing.T, store workspace.Store) {
	t.Helper()
	tests := []struct {
		name    string
		applier workspace.CASApplier
	}{
		{
			name: "callback_error",
			applier: workspace.CASApplierFunc(func(context.Context, workspace.ApplyIntent) (workspace.CASOutcome, error) {
				return workspace.CASOutcome{}, errors.New("external result is unknown")
			}),
		},
		{
			name: "invalid_outcome",
			applier: workspace.CASApplierFunc(func(_ context.Context, intent workspace.ApplyIntent) (workspace.CASOutcome, error) {
				return workspace.CASOutcome{Decision: workspace.ApplySuccess, ObservedRevision: intent.BaseRevision}, nil
			}),
		},
	}

	for _, test := range tests {
		intent := workspaceConformanceIntent("failure-"+test.name, "payload")
		result, err := store.Apply(ctx, intent, test.applier)
		if err != nil {
			t.Fatalf("%s Apply: %v", test.name, err)
		}
		assertWorkspaceTerminal(t, result.Record, workspace.ApplyStateIndeterminate, workspace.ApplyIndeterminate)
		if result.Replay {
			t.Fatalf("%s first Apply was reported as replay", test.name)
		}

		var replayCalls atomic.Int32
		replayed, err := store.Apply(ctx, intent, workspace.CASApplierFunc(func(context.Context, workspace.ApplyIntent) (workspace.CASOutcome, error) {
			replayCalls.Add(1)
			return workspace.CASOutcome{}, nil
		}))
		if err != nil {
			t.Fatalf("%s exact replay: %v", test.name, err)
		}
		if !replayed.Replay || replayCalls.Load() != 0 {
			t.Fatalf("%s replay = %#v; CAS calls=%d", test.name, replayed, replayCalls.Load())
		}
		if !reflect.DeepEqual(replayed.Record, result.Record) {
			t.Fatalf("%s replay changed indeterminate record", test.name)
		}
	}
}

func testWorkspaceStoreConcurrentApply(ctx context.Context, t *testing.T, store workspace.Store) {
	t.Helper()
	intent := workspaceConformanceIntent("concurrent", "payload")
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCallbacks := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCallbacks()
	var calls atomic.Int32
	applier := workspace.CASApplierFunc(func(context.Context, workspace.ApplyIntent) (workspace.CASOutcome, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return workspace.CASOutcome{Decision: workspace.ApplySuccess, ObservedRevision: intent.ResultRevision}, nil
	})

	const count = 32
	start := make(chan struct{})
	results := make([]workspace.ApplyResult, count)
	errs := make([]error, count)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(count)
	done.Add(count)
	for index := range count {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			results[index], errs[index] = store.Apply(ctx, intent, applier)
		}()
	}
	ready.Wait()
	close(start)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("CAS callback did not start: %v", ctx.Err())
	}
	releaseCallbacks()
	done.Wait()

	firsts := 0
	for index := range count {
		if errs[index] != nil {
			t.Fatalf("concurrent Apply %d: %v", index, errs[index])
		}
		assertWorkspaceTerminal(t, results[index].Record, workspace.ApplyStateSuccess, workspace.ApplySuccess)
		if !results[index].Replay {
			firsts++
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent Apply invoked CAS %d times, want one", got)
	}
	if firsts != 1 {
		t.Fatalf("concurrent Apply reported %d first executions, want one", firsts)
	}
}

func testWorkspaceStoreByteOwnership(ctx context.Context, t *testing.T, store workspace.Store) {
	t.Helper()
	intent := workspaceConformanceIntent("bytes", "owned payload")
	expected := workspace.CloneApplyIntent(intent)
	result, err := store.Apply(ctx, intent, workspace.CASApplierFunc(func(_ context.Context, received workspace.ApplyIntent) (workspace.CASOutcome, error) {
		received.Payload.Data[0] = 'X'
		return workspace.CASOutcome{Decision: workspace.ApplyRejected, Code: "test"}, nil
	}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(intent, expected) {
		t.Fatalf("CAS callback mutated caller-owned intent:\nwant: %#v\ngot:  %#v", expected, intent)
	}
	if !reflect.DeepEqual(result.Record.Intent, expected) {
		t.Fatalf("CAS callback mutated durable intent:\nwant: %#v\ngot:  %#v", expected, result.Record.Intent)
	}

	intent.Payload.Data[0] = 'Y'
	result.Record.Intent.Payload.Data[1] = 'Z'
	if result.Record.Outcome != nil {
		result.Record.Outcome.Code = "mutated-result"
	}
	if result.Record.CompletedAt != nil {
		*result.Record.CompletedAt = time.Time{}
	}

	firstLookup, err := store.Lookup(ctx, expected.Identity)
	if err != nil {
		t.Fatalf("Lookup after caller mutations: %v", err)
	}
	if !reflect.DeepEqual(firstLookup.Intent, expected) {
		t.Fatalf("Store retained caller-owned byte alias:\nwant: %#v\ngot:  %#v", expected, firstLookup.Intent)
	}
	if firstLookup.Outcome == nil || firstLookup.Outcome.Code != "test" || firstLookup.CompletedAt == nil || firstLookup.CompletedAt.IsZero() {
		t.Fatalf("Store retained returned pointer alias: %#v", firstLookup)
	}

	firstLookup.Intent.Payload.Data[2] = 'Q'
	firstLookup.Outcome.Message = "mutated lookup"
	*firstLookup.CompletedAt = time.Time{}
	secondLookup, err := store.Lookup(ctx, expected.Identity)
	if err != nil {
		t.Fatalf("second Lookup: %v", err)
	}
	if !reflect.DeepEqual(secondLookup.Intent, expected) || secondLookup.Outcome == nil ||
		secondLookup.Outcome.Message != "" || secondLookup.CompletedAt == nil || secondLookup.CompletedAt.IsZero() {
		t.Fatalf("Lookup returned mutable aliases into Store: %#v", secondLookup)
	}
}

func testWorkspaceStoreReleased(
	ctx context.Context,
	t *testing.T,
	store workspace.Store,
	release framework.ReleaseFunc,
) {
	t.Helper()
	before := store.Description()
	if err := release(ctx); err != nil {
		t.Fatalf("release workspace Store: %v", err)
	}
	if err := release(ctx); err != nil {
		t.Fatalf("idempotent release workspace Store: %v", err)
	}
	if after := store.Description(); !reflect.DeepEqual(after, before) {
		t.Fatalf("Description changed after release:\nbefore: %#v\nafter:  %#v", before, after)
	}
	identity := workspaceConformanceIdentity("released")
	if _, err := store.Lookup(ctx, identity); !errors.Is(err, workspace.ErrStoreClosed) {
		t.Fatalf("Lookup after release error = %v, want ErrStoreClosed", err)
	}
	intent := workspaceConformanceIntent("released", "payload")
	var calls atomic.Int32
	_, err := store.Apply(ctx, intent, workspace.CASApplierFunc(func(context.Context, workspace.ApplyIntent) (workspace.CASOutcome, error) {
		calls.Add(1)
		return workspace.CASOutcome{}, nil
	}))
	if !errors.Is(err, workspace.ErrStoreClosed) {
		t.Fatalf("Apply after release error = %v, want ErrStoreClosed", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("Apply after release invoked CAS %d times, want zero", got)
	}
}

func workspaceConformanceIdentity(artifact string) workspace.ApplyIdentity {
	return workspace.ApplyIdentity{
		Authority: "humantest-authority",
		Workspace: "humantest-workspace",
		Artifact:  artifact,
	}
}

func workspaceConformanceIntent(artifact, data string) workspace.ApplyIntent {
	payload := workspace.Payload{MediaType: "application/json", Data: []byte(data)}
	return workspace.ApplyIntent{
		Identity:       workspaceConformanceIdentity(artifact),
		ArtifactDigest: workspace.Digest("sha256:" + strings.Repeat("a", 64)),
		PayloadDigest:  workspace.DigestPayload(payload),
		BaseRevision:   workspace.Revision("base:" + artifact),
		ResultRevision: workspace.Revision("result:" + artifact),
		Payload:        payload,
	}
}

func assertWorkspaceTerminal(t *testing.T, record workspace.ApplyRecord, state workspace.ApplyState, decision workspace.ApplyDecision) {
	t.Helper()
	if record.State != state || !record.State.Terminal() || record.Outcome == nil ||
		record.Outcome.Decision != decision || record.CompletedAt == nil || record.CompletedAt.IsZero() {
		t.Fatalf("unexpected terminal workspace record: %#v", record)
	}
	if err := workspace.ValidateApplyIntent(record.Intent); err != nil {
		t.Fatalf("terminal record contains invalid intent: %v", err)
	}
	_, digest, err := workspace.CanonicalApplyIntent(record.Intent)
	if err != nil {
		t.Fatalf("canonicalize terminal intent: %v", err)
	}
	if record.IntentDigest != digest {
		t.Fatalf("terminal intent digest = %q, want %q", record.IntentDigest, digest)
	}
	if err := workspace.ValidateCASOutcome(*record.Outcome, record.Intent); err != nil {
		t.Fatalf("terminal record contains invalid outcome: %v", err)
	}
}
