package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/vibe-agi/human/workspace"
	_ "modernc.org/sqlite"
)

func TestSQLiteApplyJournalPersistsBeforeCASAndReplaysExactly(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	journal := openTestApplyJournal(t, filepath.Join(t.TempDir(), "apply.db"))
	intent := testApplyIntent("artifact-1", "first payload")
	callbackCalls := 0
	result, err := journal.Apply(ctx, intent, CASApplierFunc(func(_ context.Context, received ApplyIntent) (CASOutcome, error) {
		callbackCalls++
		pending, lookupErr := journal.Lookup(context.Background(), intent.Identity)
		if lookupErr != nil {
			t.Fatalf("lookup pending apply from callback: %v", lookupErr)
		}
		if pending.State != ApplyStatePending || pending.Outcome != nil || pending.CompletedAt != nil {
			t.Fatalf("callback observed non-pending journal record: %#v", pending)
		}
		// The applier receives an isolated copy and cannot mutate durable intent.
		received.Payload.Data[0] = 'X'
		// A request cancellation after the external effect must not cancel the
		// bounded terminal commit.
		cancel()
		return CASOutcome{
			Decision: ApplySuccess, ObservedRevision: intent.ResultRevision,
			Code: "applied", Message: "caller CAS committed",
		}, nil
	}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if result.Replay || result.Record.State != ApplyStateSuccess || result.Record.Outcome == nil ||
		result.Record.Outcome.ObservedRevision != intent.ResultRevision || result.Record.CompletedAt == nil {
		t.Fatalf("unexpected first result: %#v", result)
	}
	if string(result.Record.Intent.Payload.Data) != "first payload" {
		t.Fatalf("applier mutated recorded payload: %q", result.Record.Intent.Payload.Data)
	}

	replayed, err := journal.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
		t.Fatal("exact replay invoked CAS applier")
		return CASOutcome{}, nil
	}))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replayed.Replay || replayed.Record.State != ApplyStateSuccess || callbackCalls != 1 {
		t.Fatalf("unexpected replay result %#v; callback calls=%d", replayed, callbackCalls)
	}
	if replayed.Record.IntentDigest != result.Record.IntentDigest ||
		!replayed.Record.CreatedAt.Equal(result.Record.CreatedAt) ||
		!replayed.Record.CompletedAt.Equal(*result.Record.CompletedAt) {
		t.Fatalf("replay did not preserve durable record: first=%#v replay=%#v", result.Record, replayed.Record)
	}

	conflicting := testApplyIntent("artifact-1", "different payload")
	_, err = journal.Apply(context.Background(), conflicting, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
		t.Fatal("conflicting retry invoked CAS applier")
		return CASOutcome{}, nil
	}))
	if !errors.Is(err, ErrApplyIntentConflict) {
		t.Fatalf("conflicting retry error = %v, want ErrApplyIntentConflict", err)
	}
}

func TestSQLiteApplyJournalCallbackFailuresAreTerminalIndeterminate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		apply   CASApplierFunc
		code    string
		message string
	}{
		{
			name: "callback error",
			apply: func(context.Context, ApplyIntent) (CASOutcome, error) {
				return CASOutcome{}, errors.New("write may have happened\x00after disconnect")
			},
			code: "cas_callback_error", message: "write may have happened�after disconnect",
		},
		{
			name: "invalid success observation",
			apply: func(_ context.Context, intent ApplyIntent) (CASOutcome, error) {
				return CASOutcome{Decision: ApplySuccess, ObservedRevision: intent.BaseRevision}, nil
			},
			code: "invalid_cas_result", message: "CAS applier returned an invalid result",
		},
	}
	for index, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			journal := openTestApplyJournal(t, filepath.Join(t.TempDir(), "apply.db"))
			intent := testApplyIntent(fmt.Sprintf("artifact-%d", index+1), "payload")
			result, err := journal.Apply(context.Background(), intent, test.apply)
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
			if result.Replay || result.Record.State != ApplyStateIndeterminate || result.Record.Outcome == nil {
				t.Fatalf("unexpected result: %#v", result)
			}
			if result.Record.Outcome.Decision != ApplyIndeterminate || result.Record.Outcome.Code != test.code ||
				result.Record.Outcome.Message != test.message {
				t.Fatalf("unexpected indeterminate outcome: %#v", result.Record.Outcome)
			}
			calls := 0
			replayed, err := journal.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
				calls++
				return CASOutcome{}, nil
			}))
			if err != nil || !replayed.Replay || calls != 0 {
				t.Fatalf("indeterminate replay = %#v, %v; callback calls=%d", replayed, err, calls)
			}
		})
	}
}

func TestSQLiteApplyJournalRecoveryNeverReexecutesPendingCAS(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "apply.db")
	journal := openTestApplyJournal(t, path)
	intent := testApplyIntent("artifact-crash", "durable payload")
	encoded, digest, err := CanonicalApplyIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	pending, replay, err := journal.begin(context.Background(), intent, encoded, digest)
	if err != nil || replay || pending.State != ApplyStatePending {
		t.Fatalf("persist pending = %#v, replay=%t, err=%v", pending, replay, err)
	}
	// The process may have executed CAS here and stopped before recording it.
	// Reopening must not infer that it is safe to run again.
	if err := journal.close(context.Background()); err != nil {
		t.Fatalf("close crashed owner: %v", err)
	}

	reopened := openTestApplyJournal(t, path)
	recovered, err := reopened.Lookup(context.Background(), intent.Identity)
	if err != nil {
		t.Fatalf("lookup recovered: %v", err)
	}
	if recovered.State != ApplyStateIndeterminate || recovered.Outcome == nil ||
		recovered.Outcome.Code != "recovered_pending" || recovered.CompletedAt == nil {
		t.Fatalf("unexpected recovered record: %#v", recovered)
	}
	calls := 0
	result, err := reopened.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
		calls++
		return CASOutcome{}, nil
	}))
	if err != nil || !result.Replay || calls != 0 {
		t.Fatalf("recovered replay = %#v, %v; callback calls=%d", result, err, calls)
	}
}

func TestSQLiteApplyJournalPostCASCommitGapRecoversIndeterminate(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "apply.db")
	journal := openTestApplyJournal(t, path)
	intent := testApplyIntent("artifact-commit-gap", "payload")
	var effects atomic.Int32
	result, err := journal.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
		effects.Add(1)
		// Model a process/database loss after the external side effect but before
		// the detached terminal commit.
		if closeErr := journal.database.Close(); closeErr != nil {
			t.Fatalf("close database in callback: %v", closeErr)
		}
		return CASOutcome{Decision: ApplySuccess, ObservedRevision: intent.ResultRevision}, nil
	}))
	if err == nil || result.Record.State != ApplyStatePending || effects.Load() != 1 {
		t.Fatalf("commit-gap result = %#v, %v; effects=%d", result, err, effects.Load())
	}
	if err := journal.close(context.Background()); err != nil {
		t.Fatalf("close journal after database loss: %v", err)
	}

	reopened := openTestApplyJournal(t, path)
	replayed, err := reopened.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
		effects.Add(1)
		return CASOutcome{}, nil
	}))
	if err != nil || !replayed.Replay || replayed.Record.State != ApplyStateIndeterminate || effects.Load() != 1 {
		t.Fatalf("post-gap recovery = %#v, %v; effects=%d", replayed, err, effects.Load())
	}
}

func TestSQLiteApplyJournalPendingRetryBecomesIndeterminate(t *testing.T) {
	t.Parallel()
	journal := openTestApplyJournal(t, filepath.Join(t.TempDir(), "apply.db"))
	intent := testApplyIntent("artifact-pending", "payload")
	encoded, digest, err := CanonicalApplyIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.begin(context.Background(), intent, encoded, digest); err != nil {
		t.Fatalf("begin: %v", err)
	}
	calls := 0
	result, err := journal.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
		calls++
		return CASOutcome{}, nil
	}))
	if err != nil || !result.Replay || result.Record.State != ApplyStateIndeterminate ||
		result.Record.Outcome == nil || result.Record.Outcome.Code != "unresolved_pending" || calls != 0 {
		t.Fatalf("pending retry = %#v, %v; calls=%d", result, err, calls)
	}
}

func TestSQLiteApplyJournalConcurrentExactApplyExecutesOnce(t *testing.T) {
	t.Parallel()
	journal := openTestApplyJournal(t, filepath.Join(t.TempDir(), "apply.db"))
	intent := testApplyIntent("artifact-race", "payload")
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	applier := CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return CASOutcome{Decision: ApplySuccess, ObservedRevision: intent.ResultRevision}, nil
	})

	const goroutines = 48
	start := make(chan struct{})
	results := make(chan ApplyResult, goroutines)
	errorsFound := make(chan error, goroutines)
	var ready sync.WaitGroup
	ready.Add(goroutines)
	for range goroutines {
		go func() {
			ready.Done()
			<-start
			result, err := journal.Apply(context.Background(), intent, applier)
			results <- result
			errorsFound <- err
		}()
	}
	ready.Wait()
	close(start)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("CAS callback did not start")
	}
	close(release)

	firstEffects := 0
	for range goroutines {
		if err := <-errorsFound; err != nil {
			t.Fatalf("concurrent apply: %v", err)
		}
		result := <-results
		if !result.Replay {
			firstEffects++
		}
		if result.Record.State != ApplyStateSuccess || result.Record.Outcome == nil ||
			result.Record.Outcome.ObservedRevision != intent.ResultRevision {
			t.Fatalf("unexpected concurrent result: %#v", result)
		}
	}
	if firstEffects != 1 || calls.Load() != 1 {
		t.Fatalf("first effects=%d, callback calls=%d; want one each", firstEffects, calls.Load())
	}
}

func TestSQLiteApplyJournalCloseDoesNotDeadlockReentrantLookup(t *testing.T) {
	journal := openTestApplyJournal(t, filepath.Join(t.TempDir(), "apply.db"))
	intent := testApplyIntent("artifact-close-reentry", "payload")
	callbackEntered := make(chan struct{})
	allowLookup := make(chan struct{})
	lookupResult := make(chan error, 1)
	applyResult := make(chan error, 1)
	go func() {
		_, err := journal.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
			close(callbackEntered)
			<-allowLookup
			_, lookupErr := journal.Lookup(context.Background(), intent.Identity)
			lookupResult <- lookupErr
			return CASOutcome{Decision: ApplySuccess, ObservedRevision: intent.ResultRevision}, nil
		}))
		applyResult <- err
	}()
	<-callbackEntered
	closeResult := make(chan error, 1)
	go func() { closeResult <- journal.close(context.Background()) }()
	waitForApplyJournalClosed(t, journal)
	close(allowLookup)
	select {
	case err := <-lookupResult:
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("reentrant Lookup after Close started = %v, want ErrStoreClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reentrant Lookup deadlocked behind Close")
	}
	select {
	case err := <-applyResult:
		if err != nil {
			t.Fatalf("Apply after reentrant Lookup: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Apply did not finish")
	}
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not finish")
	}
}

func TestSQLiteStoreReleaseContextBoundsBlockedApply(t *testing.T) {
	resource, err := Open(context.Background(), Config{
		Path:          filepath.Join(t.TempDir(), "apply.db"),
		CommitTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	journal := value.(*applyJournal)
	intent := testApplyIntent("artifact-bounded-release", "payload")
	callbackEntered := make(chan struct{})
	unblock := make(chan struct{})
	var unblockOnce sync.Once
	t.Cleanup(func() {
		unblockOnce.Do(func() { close(unblock) })
		_ = journal.close(context.Background())
	})
	applyResult := make(chan error, 1)
	go func() {
		_, applyErr := journal.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
			close(callbackEntered)
			<-unblock
			return CASOutcome{Decision: ApplySuccess, ObservedRevision: intent.ResultRevision}, nil
		}))
		applyResult <- applyErr
	}()
	select {
	case <-callbackEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("CAS callback did not start")
	}

	releaseContext, cancelRelease := context.WithTimeout(context.Background(), 20*time.Millisecond)
	started := time.Now()
	err = resource.Release(releaseContext)
	cancelRelease()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Release blocked Apply = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Release ignored context for %v", elapsed)
	}
	if _, err := journal.Lookup(context.Background(), intent.Identity); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Lookup after Release admission stop = %v, want ErrStoreClosed", err)
	}

	unblockOnce.Do(func() { close(unblock) })
	select {
	case err := <-applyResult:
		if err != nil {
			t.Fatalf("admitted Apply after release timeout: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("admitted Apply did not finish")
	}
	if err := journal.close(context.Background()); err != nil {
		t.Fatalf("background Store close: %v", err)
	}
}

func TestSQLiteApplyJournalSameIdentityWaitHonorsContext(t *testing.T) {
	journal := openTestApplyJournal(t, filepath.Join(t.TempDir(), "apply.db"))
	intent := testApplyIntent("artifact-cancel-wait", "payload")
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := journal.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
			close(callbackEntered)
			<-releaseCallback
			return CASOutcome{Decision: ApplySuccess, ObservedRevision: intent.ResultRevision}, nil
		}))
		firstDone <- err
	}()
	<-callbackEntered

	waitContext, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := journal.Apply(waitContext, intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
		t.Fatal("waiting exact retry invoked CAS callback")
		return CASOutcome{}, nil
	}))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting exact retry error = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("context cancellation took %v", elapsed)
	}
	close(releaseCallback)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Apply: %v", err)
	}
}

func waitForApplyJournalClosed(t *testing.T, journal *applyJournal) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		journal.lifecycle.Lock()
		closed := journal.closed
		journal.lifecycle.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Close did not mark journal closed")
}

func TestSQLiteApplyJournalSingleOwnerAndCleanBreakSchema(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "apply.db")
	journal := openTestApplyJournal(t, path)
	if _, err := Open(context.Background(), Config{Path: path}); !errors.Is(err, ErrInUse) {
		t.Fatalf("second owner error = %v, want ErrInUse", err)
	}
	if err := journal.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reopened := openTestApplyJournal(t, path)
	if err := reopened.close(context.Background()); err != nil {
		t.Fatal(err)
	}

	foreignPath := filepath.Join(t.TempDir(), "foreign.db")
	database, err := sql.Open("sqlite", foreignPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE old_schema (value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), Config{Path: foreignPath}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("foreign schema error = %v, want clean-break rejection", err)
	}
}

func TestSQLiteApplyJournalRejectsInvalidIntentBeforePersistence(t *testing.T) {
	t.Parallel()
	journal := openTestApplyJournal(t, filepath.Join(t.TempDir(), "apply.db"))
	intent := testApplyIntent("artifact-invalid", "payload")
	intent.Payload.Data[0] = 'X'
	calls := 0
	_, err := journal.Apply(context.Background(), intent, CASApplierFunc(func(context.Context, ApplyIntent) (CASOutcome, error) {
		calls++
		return CASOutcome{}, nil
	}))
	if !errors.Is(err, ErrInvalidApply) || calls != 0 {
		t.Fatalf("invalid intent error=%v, calls=%d", err, calls)
	}
	if _, err := journal.Lookup(context.Background(), intent.Identity); !errors.Is(err, ErrApplyNotFound) {
		t.Fatalf("invalid intent was persisted: %v", err)
	}
}

func openTestApplyJournal(t *testing.T, path string) *applyJournal {
	t.Helper()
	resource, err := Open(context.Background(), Config{
		Path:          path,
		CommitTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("open apply journal: %v", err)
	}
	value, err := resource.Value()
	if err != nil {
		t.Fatalf("read apply journal resource: %v", err)
	}
	journal, ok := value.(*applyJournal)
	if !ok {
		t.Fatalf("apply journal resource type = %T", value)
	}
	t.Cleanup(func() {
		if err := resource.Release(context.Background()); err != nil {
			t.Errorf("close apply journal: %v", err)
		}
	})
	return journal
}

func testApplyIntent(artifact, data string) ApplyIntent {
	payload := Payload{MediaType: "application/json", Data: []byte(data)}
	return ApplyIntent{
		Identity:       ApplyIdentity{Authority: "caller-1", Workspace: "workspace-1", Artifact: artifact},
		ArtifactDigest: digestBytes([]byte("artifact:" + artifact)),
		PayloadDigest:  DigestPayload(payload),
		BaseRevision:   Revision("base:initial"),
		ResultRevision: Revision("result:" + artifact),
		Payload:        payload,
	}
}
