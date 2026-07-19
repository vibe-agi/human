package llm_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/builtin"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
	"github.com/vibe-agi/human/protect"
)

type regressionServiceOptions struct {
	codecs      []llm.CodecRegistration
	clock       llm.Clock
	ids         llm.IDSource
	seeds       llm.SeedSource
	router      llm.WorkerRouter
	admission   llm.AdmissionPolicy
	authorizer  llm.ToolAuthorizer
	readLimit   int64
	workerLimit int64
}

func newRegressionService(
	t *testing.T,
	resource framework.Resource[llm.Store],
	options regressionServiceOptions,
) *llm.Service {
	t.Helper()
	if len(options.codecs) == 0 {
		options.codecs = []llm.CodecRegistration{{
			Codec: testCodec{}, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		}}
	}
	if options.clock == nil {
		options.clock = &stepClock{next: time.Unix(1_900_000_000, 0)}
	}
	if options.ids == nil {
		options.ids = &sequenceIDs{}
	}
	if options.router == nil {
		options.router = llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		})
	}
	if options.authorizer == nil {
		options.authorizer = llm.ToolAuthorizerFunc(func(context.Context, llm.ToolAuthorization) error { return nil })
	}
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "regression-deployment", Store: resource,
		Codecs: options.codecs, Clock: options.clock, IDs: options.ids, Seeds: options.seeds,
		Router: options.router, Admission: options.admission, ToolAuthorizer: options.authorizer,
		ReadLimitBytes: options.readLimit, WorkerPayloadLimitBytes: options.workerLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-service.Done():
		default:
			shutdownRuntime(t, service)
		}
	})
	return service
}

func openBorrowedRegressionStore(t *testing.T) (llm.Store, framework.Resource[llm.Store]) {
	t.Helper()
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: filepath.Join(t.TempDir(), "service.db")})
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := resource.Release(context.Background()); err != nil {
			t.Errorf("release SQLite: %v", err)
		}
	})
	return store, framework.Borrow[llm.Store](store)
}

func TestServiceAdmissionCommitUnknownRestoresAssignmentWithoutRestart(t *testing.T) {
	store, borrowed := openBorrowedRegressionStore(t)
	wrapper := &commitUnknownStore{Store: store}
	service := newRegressionService(t, framework.Borrow[llm.Store](wrapper), regressionServiceOptions{})
	worker := openTestWorker(t, service, "worker-a", "session-a")
	wrapper.unknown.Store(true)
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "unknown-admission", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "committed")),
	})
	if err != nil || !result.Replay {
		t.Fatalf("commit-unknown admission = %+v, %v", result, err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if assignment.Assignment.Identity != result.Identity {
		t.Fatalf("assignment identity = %+v, want %+v", assignment.Assignment.Identity, result.Identity)
	}
	assertNoServiceAssignment(t, worker, 150*time.Millisecond)
	_ = borrowed
}

func TestServiceAdmissionCommitUnknownIgnoresCanceledOperationContext(t *testing.T) {
	store, _ := openBorrowedRegressionStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	wrapper := &unknownAfterCommitStore{Store: store, afterCommit: cancel}
	service := newRegressionService(t, framework.Borrow[llm.Store](wrapper), regressionServiceOptions{})
	worker := openTestWorker(t, service, "worker-a", "session-a")
	wrapper.unknown.Store(true)
	result, err := service.Admit(ctx, llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "unknown-canceled", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "committed despite cancellation")),
	})
	if err != nil || !result.Replay || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("canceled commit-unknown admission = %+v, %v (ctx=%v)", result, err, ctx.Err())
	}
	if assignment := receiveServiceAssignment(t, worker); assignment.Assignment.Identity != result.Identity {
		t.Fatalf("assignment identity = %+v, want %+v", assignment.Assignment.Identity, result.Identity)
	}
}

func TestServiceExactRetryRepairsAssignmentAfterUnknownReconcileOutage(t *testing.T) {
	store, _ := openBorrowedRegressionStore(t)
	wrapper := &transientUnknownStore{Store: store}
	service := newRegressionService(t, framework.Borrow[llm.Store](wrapper), regressionServiceOptions{})
	worker := openTestWorker(t, service, "worker-a", "session-a")
	request := llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "unknown-outage", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "repair me")),
	}
	wrapper.arm(1)
	if _, err := service.Admit(t.Context(), request); err == nil {
		t.Fatal("first admission unexpectedly reconciled through injected Store outage")
	}
	assertNoServiceAssignment(t, worker, 100*time.Millisecond)
	replay, err := service.Admit(t.Context(), request)
	if err != nil || !replay.Replay {
		t.Fatalf("exact retry = %+v, %v", replay, err)
	}
	if assignment := receiveServiceAssignment(t, worker); assignment.Assignment.Identity != replay.Identity {
		t.Fatalf("repaired assignment identity = %+v, want %+v", assignment.Assignment.Identity, replay.Identity)
	}
	assertNoServiceAssignment(t, worker, 150*time.Millisecond)
}

func TestServiceExactWorkerReceiptSurvivesSmallerReadAndWorkerLimits(t *testing.T) {
	store, borrowed := openBorrowedRegressionStore(t)
	service := newRegressionService(t, borrowed, regressionServiceOptions{})
	worker := openTestWorker(t, service, "worker-a", "session-before")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "large-receipt", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, string(make([]byte, 12<<10)))),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	delivery := workerDelivery(assignment, "delivery-large-final", llm.Event{
		ID: "event-large-final", Type: llm.EventFinal, Text: string(make([]byte, 12<<10)),
	})
	if receipt, commitErr := worker.CommitEvent(t.Context(), delivery); commitErr != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("first commit = %+v, %v", receipt, commitErr)
	}
	shutdownRuntime(t, service)

	service = newRegressionService(t, framework.Borrow[llm.Store](store), regressionServiceOptions{
		readLimit: 8 << 10, workerLimit: 8 << 10,
	})
	worker = openTestWorker(t, service, "worker-a", "session-after")
	receipt, err := worker.CommitEvent(t.Context(), delivery)
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("exact receipt under smaller limits = %+v, %v (digest=%s)", receipt, err, result.RequestDigest)
	}
}

func TestServiceRecoveryQuarantinesAssignmentOverCurrentWorkerLimit(t *testing.T) {
	store, borrowed := openBorrowedRegressionStore(t)
	service := newRegressionService(t, borrowed, regressionServiceOptions{})
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "large-assignment", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, string(make([]byte, 12<<10)))),
	})
	if err != nil {
		t.Fatal(err)
	}
	shutdownRuntime(t, service)

	service = newRegressionService(t, framework.Borrow[llm.Store](store), regressionServiceOptions{workerLimit: 8 << 10})
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "large-assignment", RequestDigest: result.RequestDigest,
	})
	if err != nil || !page.Complete {
		t.Fatalf("quarantined response = %+v, %v", page, err)
	}
	worker := openTestWorker(t, service, "worker-a", "session-after")
	assertNoServiceAssignment(t, worker, 150*time.Millisecond)
}

func TestServiceRecoveryQuarantinesRecordOverLoweredReadLimitAndRestoresOthers(t *testing.T) {
	store, borrowed := openBorrowedRegressionStore(t)
	service := newRegressionService(t, borrowed, regressionServiceOptions{})
	large, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-large", IdempotencyKey: "large-read", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, string(make([]byte, 12<<10)))),
	})
	if err != nil {
		t.Fatal(err)
	}
	small, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-small", IdempotencyKey: "small-read", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "small")),
	})
	if err != nil {
		t.Fatal(err)
	}
	shutdownRuntime(t, service)

	service = newRegressionService(t, framework.Borrow[llm.Store](store), regressionServiceOptions{readLimit: 8 << 10})
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-large", IdempotencyKey: "large-read", RequestDigest: large.RequestDigest,
	})
	if err != nil || !page.Complete {
		t.Fatalf("oversized recovery was not finite: %+v, %v", page, err)
	}
	worker := openTestWorker(t, service, "worker-a", "session-after")
	recovered := receiveServiceAssignment(t, worker)
	if recovered.Assignment.Identity != small.Identity {
		t.Fatalf("recovered assignment = %+v, want small %+v", recovered.Assignment.Identity, small.Identity)
	}
}

func TestServiceMemoryRecoveryQuarantinesRecordOverLoweredReadLimitAndRestoresOthers(t *testing.T) {
	store, release := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = release(context.Background()) })
	service := newRegressionService(t, framework.Borrow[llm.Store](store), regressionServiceOptions{})
	large, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-large", IdempotencyKey: "large-memory-read", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, string(make([]byte, 12<<10)))),
	})
	if err != nil {
		t.Fatal(err)
	}
	small, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-small", IdempotencyKey: "small-memory-read", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "small")),
	})
	if err != nil {
		t.Fatal(err)
	}
	shutdownRuntime(t, service)

	service = newRegressionService(t, framework.Borrow[llm.Store](store), regressionServiceOptions{readLimit: 8 << 10})
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-large", IdempotencyKey: "large-memory-read", RequestDigest: large.RequestDigest,
	})
	if err != nil || !page.Complete {
		t.Fatalf("memory oversized recovery was not finite: %+v, %v", page, err)
	}
	worker := openTestWorker(t, service, "worker-a", "session-memory-after")
	recovered := receiveServiceAssignment(t, worker)
	if recovered.Assignment.Identity != small.Identity {
		t.Fatalf("memory recovered assignment = %+v, want small %+v", recovered.Assignment.Identity, small.Identity)
	}
}

func TestServiceReconcilesStoreDeploymentBindingCommitUnknown(t *testing.T) {
	store, release := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = release(context.Background()) })
	wrapper := &bindingCommitUnknownStore{Store: store}
	wrapper.unknown.Store(true)
	service := newRegressionService(t, framework.Borrow[llm.Store](wrapper), regressionServiceOptions{})
	if calls := wrapper.calls.Load(); calls != 2 {
		t.Fatalf("Store Bind calls = %d, want initial plus exact reconciliation", calls)
	}
	if err := store.Bind(t.Context(), llm.StoreBinding{DeploymentID: "other-deployment"}); !errors.Is(err, llm.ErrStoreConflict) {
		t.Fatalf("service did not permanently bind Store deployment: %v", err)
	}
	shutdownRuntime(t, service)
}

func TestServiceStaticConfigurationFailureDoesNotBindStore(t *testing.T) {
	store, release := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = release(context.Background()) })
	_, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "failed-deployment", Store: framework.Borrow[llm.Store](store),
		// Missing Codecs is deliberately invalid before the durable binding point.
	})
	if !errors.Is(err, llm.ErrInvalidServiceConfig) {
		t.Fatalf("invalid static configuration error = %v", err)
	}
	if err := store.Bind(t.Context(), llm.StoreBinding{DeploymentID: "healthy-deployment"}); err != nil {
		t.Fatalf("failed static configuration permanently bound Store: %v", err)
	}
}

func TestServiceRejectsAndReleasesOwnedNilProtector(t *testing.T) {
	store, releaseStore := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = releaseStore(context.Background()) })
	released := atomic.Bool{}
	var provider protect.Protector
	resource, err := framework.Own[protect.Protector](provider, func(context.Context) error {
		released.Store(true)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = llm.NewService(t.Context(), llm.Config{
		DeploymentID: "owned-nil-protector", Store: framework.Borrow[llm.Store](store), Protector: resource,
		Codecs: []llm.CodecRegistration{{
			Codec: testCodec{}, StreamContentType: "text/event-stream", AggregateContentType: "application/json",
		}},
	})
	if !errors.Is(err, llm.ErrInvalidServiceConfig) {
		t.Fatalf("owned nil Protector error = %v, want ErrInvalidServiceConfig", err)
	}
	if !released.Load() {
		t.Fatal("owned nil Protector was not released after constructor failure")
	}
	if err := store.Bind(t.Context(), llm.StoreBinding{DeploymentID: "usable-after-owned-nil"}); err != nil {
		t.Fatalf("owned nil Protector failure reached durable Store binding: %v", err)
	}
}

func TestServiceWorkerCommitUnknownWakesWaiterAndExactReplayCleansAssignment(t *testing.T) {
	store, _ := openBorrowedRegressionStore(t)
	wrapper := &transientUnknownStore{Store: store}
	service := newRegressionService(t, framework.Borrow[llm.Store](wrapper), regressionServiceOptions{})
	worker := openTestWorker(t, service, "worker-a", "session-a")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "unknown-event", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "wait")),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	initial, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "unknown-event", RequestDigest: result.RequestDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan llm.ResponsePage, 1)
	waitErr := make(chan error, 1)
	go func() {
		page, waitError := service.WaitResponse(context.Background(), llm.ResponseQuery{
			CallerID: "caller-a", IdempotencyKey: "unknown-event", RequestDigest: result.RequestDigest,
			After: initial.Cursor,
		})
		if waitError != nil {
			waitErr <- waitError
			return
		}
		waited <- page
	}()
	delivery := workerDelivery(assignment, "delivery-unknown-final", llm.Event{
		ID: "event-unknown-final", Type: llm.EventFinal, Text: "done",
	})
	wrapper.arm(1)
	if _, err := worker.CommitEvent(t.Context(), delivery); err == nil {
		t.Fatal("first worker commit unexpectedly reconciled through injected Store outage")
	}
	select {
	case page := <-waited:
		if !page.Complete {
			t.Fatalf("woken response is not complete: %+v", page)
		}
	case err := <-waitErr:
		t.Fatalf("WaitResponse: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitResponse was not woken after commit-unknown")
	}
	receipt, err := worker.CommitEvent(t.Context(), delivery)
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("exact outbox replay = %+v, %v", receipt, err)
	}
	assertNoServiceAssignment(t, worker, 150*time.Millisecond)
}

func TestServiceConcurrentBlankTaskAffinityHasOneCleanWinner(t *testing.T) {
	store, borrowed := openBorrowedRegressionStore(t)
	ids := newTaskBarrierIDs(2)
	service := newRegressionService(t, borrowed, regressionServiceOptions{ids: ids})
	worker := openTestWorker(t, service, "worker-a", "session-a")
	task := testRemoteTask()
	type outcome struct {
		key    llm.IdempotencyKey
		result llm.AdmissionResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	for _, key := range []llm.IdempotencyKey{"affinity-a", "affinity-b"} {
		key := key
		go func() {
			result, err := service.Admit(context.Background(), llm.AdmissionRequest{
				CallerID: "caller-a", IdempotencyKey: key, CodecID: testCodecID,
				Body: mustJSON(t, testRequest(true, "same affinity")), Task: task,
			})
			outcomes <- outcome{key: key, result: result, err: err}
		}()
	}
	first, second := <-outcomes, <-outcomes
	var winner, loser outcome
	if first.err == nil && second.err != nil {
		winner, loser = first, second
	} else if second.err == nil && first.err != nil {
		winner, loser = second, first
	} else {
		t.Fatalf("concurrent outcomes = (%+v, %v), (%+v, %v)", first.result, first.err, second.result, second.err)
	}
	var admission *llm.AdmissionError
	if !errors.As(loser.err, &admission) || admission.Failure.Status != 409 {
		t.Fatalf("loser error = %v, want durable 409 conflict", loser.err)
	}
	allocated := ids.taskIDs()
	if len(allocated) != 2 {
		t.Fatalf("allocated Task IDs = %v", allocated)
	}
	loserTask := allocated[0]
	if loserTask == winner.result.Identity.TaskID {
		loserTask = allocated[1]
	}
	if loserTask == winner.result.Identity.TaskID {
		t.Fatalf("could not identify loser Task from %v and winner %q", allocated, winner.result.Identity.TaskID)
	}
	err := store.View(t.Context(), func(view llm.StoreView) error {
		if _, loadErr := view.LoadRequestHead(llm.StoreRequestKey{
			Caller: "caller-a", IdempotencyKey: loser.key,
		}); !errors.Is(loadErr, llm.ErrStoreRecordNotFound) {
			return fmt.Errorf("loser request exists: %w", loadErr)
		}
		if _, loadErr := view.LoadTask(llm.StoreTaskKey{Caller: "caller-a", Task: loserTask}); !errors.Is(loadErr, llm.ErrStoreRecordNotFound) {
			return fmt.Errorf("loser Task exists: %w", loadErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := receiveServiceAssignment(t, worker)
	if assignment.Assignment.Identity != winner.result.Identity {
		t.Fatalf("winner assignment = %+v, want %+v", assignment.Assignment.Identity, winner.result.Identity)
	}
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	if receipt, commitErr := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-clarify", llm.Event{
		ID: "event-clarify", Type: llm.EventClarification, Text: "continue",
	})); commitErr != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("clarification = %+v, %v", receipt, commitErr)
	}
	retry, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: loser.key, CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "same affinity")), Task: task,
	})
	if err != nil || retry.Identity.TaskID != winner.result.Identity.TaskID {
		t.Fatalf("loser exact retry = %+v, %v; want Task %q", retry, err, winner.result.Identity.TaskID)
	}
	if next := receiveServiceAssignment(t, worker); next.Assignment.Identity != retry.Identity {
		t.Fatalf("retry assignment = %+v, want %+v", next.Assignment.Identity, retry.Identity)
	}
}

func TestServiceAggregateTerminalStatusMapsFailureKinds(t *testing.T) {
	service := newRegressionService(t, mustRegressionStore(t), regressionServiceOptions{})
	worker := openTestWorker(t, service, "worker-a", "session-a")
	for index, test := range []struct {
		event llm.EventType
		want  int
	}{
		{event: llm.EventRejected, want: 409},
		{event: llm.EventExpired, want: 504},
		{event: llm.EventFailed, want: 500},
		{event: llm.EventUnavailable, want: 503},
	} {
		key := llm.IdempotencyKey(fmt.Sprintf("aggregate-failure-%d", index))
		result, err := service.Admit(t.Context(), llm.AdmissionRequest{
			CallerID: "caller-a", IdempotencyKey: key, CodecID: testCodecID,
			Body: mustJSON(t, testRequest(false, "fail")),
		})
		if err != nil {
			t.Fatal(err)
		}
		assignment := receiveServiceAssignment(t, worker)
		if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
			t.Fatal(err)
		}
		receipt, err := worker.CommitEvent(t.Context(), workerDelivery(assignment,
			llm.WorkerDeliveryID(fmt.Sprintf("delivery-failure-%d", index)), llm.Event{
				ID: fmt.Sprintf("event-failure-%d", index), Type: test.event,
				ErrorCode: "human_failure", Error: "human failed",
			}))
		if err != nil || receipt.Decision != llm.WorkerEventACK {
			t.Fatalf("commit %s = %+v, %v", test.event, receipt, err)
		}
		page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
			CallerID: "caller-a", IdempotencyKey: key, RequestDigest: result.RequestDigest,
		})
		if err != nil || !page.Complete || page.Mode != llm.ResponseAggregate ||
			page.Decision.StatusCode != test.want || len(page.Decision.Body) == 0 {
			t.Fatalf("aggregate %s = %+v, %v; want status %d", test.event, page, err, test.want)
		}
	}
}

func TestServiceBuiltinsKeepStream200AndMapAggregateFailure500(t *testing.T) {
	tests := []struct {
		name    string
		codec   llm.Codec
		payload func(bool) []byte
	}{
		{
			name: "OpenAI Chat", codec: builtin.OpenAIChat(),
			payload: func(stream bool) []byte {
				return []byte(fmt.Sprintf(`{"model":"human","stream":%t,"messages":[{"role":"user","content":"help"}]}`, stream))
			},
		},
		{
			name: "Anthropic Messages", codec: builtin.AnthropicMessages(),
			payload: func(stream bool) []byte {
				return []byte(fmt.Sprintf(`{"model":"human","stream":%t,"messages":[{"role":"user","content":"help"}]}`, stream))
			},
		},
		{
			name: "OpenAI Responses", codec: builtin.OpenAIResponses(),
			payload: func(stream bool) []byte {
				return []byte(fmt.Sprintf(`{"model":"human","stream":%t,"input":"help"}`, stream))
			},
		},
	}
	registrations := make([]llm.CodecRegistration, 0, len(tests))
	for _, test := range tests {
		registrations = append(registrations, llm.CodecRegistration{
			Codec: test.codec, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		})
	}
	service := newRegressionService(t, mustRegressionStore(t), regressionServiceOptions{codecs: registrations})
	worker := openTestWorker(t, service, "worker-a", "session-a")
	for index, test := range tests {
		for _, stream := range []bool{false, true} {
			mode := "aggregate"
			if stream {
				mode = "stream"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				description := test.codec.Description()
				key := llm.IdempotencyKey(fmt.Sprintf("builtin-%d-%t", index, stream))
				result, err := service.Admit(t.Context(), llm.AdmissionRequest{
					CallerID: "caller-a", IdempotencyKey: key, CodecID: description.ID,
					Body: test.payload(stream),
				})
				if err != nil {
					t.Fatal(err)
				}
				if stream && result.Response.Decision.StatusCode != 200 {
					t.Fatalf("stream admission status = %d", result.Response.Decision.StatusCode)
				}
				assignment := receiveServiceAssignment(t, worker)
				if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
					t.Fatal(err)
				}
				receipt, err := worker.CommitEvent(t.Context(), workerDelivery(assignment,
					llm.WorkerDeliveryID(fmt.Sprintf("delivery-builtin-%d-%t", index, stream)), llm.Event{
						ID: fmt.Sprintf("event-builtin-%d-%t", index, stream), Type: llm.EventFailed,
						ErrorCode: "human_failure", Error: "human failed",
					}))
				if err != nil || receipt.Decision != llm.WorkerEventACK {
					t.Fatalf("commit failed = %+v, %v", receipt, err)
				}
				page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
					CallerID: "caller-a", IdempotencyKey: key, RequestDigest: result.RequestDigest,
				})
				if err != nil || !page.Complete || page.Decision.StatusCode != map[bool]int{false: 500, true: 200}[stream] {
					t.Fatalf("terminal response = %+v, %v", page, err)
				}
			})
		}
	}
}

func TestServiceRejectsBodySuppressingSuccessStatuses(t *testing.T) {
	store, borrowed := openBorrowedRegressionStore(t)
	for _, test := range []struct {
		status int
		valid  bool
	}{{status: 201, valid: true}, {status: 202, valid: true}, {status: 204}, {status: 205}} {
		t.Run(fmt.Sprint(test.status), func(t *testing.T) {
			service, err := llm.NewService(t.Context(), llm.Config{
				DeploymentID: "status-test", Store: borrowed,
				Codecs: []llm.CodecRegistration{{
					Codec: testCodec{}, StreamContentType: "text/event-stream",
					AggregateContentType: "application/json", SuccessStatus: test.status,
				}},
			})
			if test.valid {
				if err != nil {
					t.Fatalf("SuccessStatus %d error = %v", test.status, err)
				}
				shutdownRuntime(t, service)
				return
			}
			if !errors.Is(err, llm.ErrInvalidServiceConfig) {
				t.Fatalf("SuccessStatus %d error = %v", test.status, err)
			}
		})
	}
	_ = store
}

func TestServiceCanonicalizesOwnedSeedsAndLargeIntegersAcrossRestart(t *testing.T) {
	const exact = "9007199254740993"
	store, borrowed := openBorrowedRegressionStore(t)
	seeds := &retainedSeedSource{
		sessionEntropy: []byte{1, 2, 3},
		sessionOpaque:  json.RawMessage(` { "z" : 1, "a" : 9007199254740993 } `),
		eventEntropy:   []byte{4, 5, 6},
		eventOpaque:    json.RawMessage(` { "b" : 2, "a" : 9007199254740993 } `),
	}
	registration := llm.CodecRegistration{
		Codec: seedEchoCodec{}, StreamContentType: "text/event-stream",
		AggregateContentType: "application/json", SuccessStatus: 200,
	}
	service := newRegressionService(t, borrowed, regressionServiceOptions{
		codecs: []llm.CodecRegistration{registration}, seeds: seeds,
	})
	worker := openTestWorker(t, service, "worker-a", "session-before")
	request := testRequest(true, "seeded")
	request.Tools = []llm.Tool{{Name: "calculate", InputSchema: json.RawMessage(`{"type":"object"}`)}}
	request.Messages = append(request.Messages, llm.Message{
		Role: llm.RoleAssistant,
		Blocks: []llm.Block{{
			Type: llm.BlockToolUse, ToolCallID: "history-call", ToolName: "calculate",
			Input: map[string]any{"value": json.Number(exact)},
		}},
	})
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "seed-restart", CodecID: seedEchoCodecID,
		Body: mustJSON(t, request),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	assertExactHistoryInteger(t, assignment.Assignment.Request, exact)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	if receipt, commitErr := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-seed-progress", llm.Event{
		ID: "event-seed-progress", Type: llm.EventProgress, Text: "working",
	})); commitErr != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("seeded progress = %+v, %v", receipt, commitErr)
	}
	before, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "seed-restart", RequestDigest: result.RequestDigest,
	})
	if err != nil || before.Complete {
		t.Fatalf("before restart = %+v, %v", before, err)
	}
	if output := string(bytes.Join(wireEventBytes(before.Events), nil)); !bytes.Contains([]byte(output), []byte(`{"a":9007199254740993,"z":1}`)) ||
		!bytes.Contains([]byte(output), []byte(`{"a":9007199254740993,"b":2}`)) {
		t.Fatalf("seed output was not canonical/exact: %q", output)
	}
	seeds.destroyBorrowedBuffers()
	shutdownRuntime(t, service)

	service = newRegressionService(t, framework.Borrow[llm.Store](store), regressionServiceOptions{
		codecs: []llm.CodecRegistration{registration}, seeds: failingSeedSource{},
	})
	worker = openTestWorker(t, service, "worker-a", "session-after")
	after, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "seed-restart", RequestDigest: result.RequestDigest,
	})
	if err != nil || after.Complete || !equalWireEvents(before.Events, after.Events) {
		t.Fatalf("after restart = %+v, %v; before=%+v", after, err, before)
	}
	replay, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "seed-restart", CodecID: seedEchoCodecID,
		Body: mustJSON(t, request),
	})
	if err != nil || !replay.Replay || replay.Response.Complete {
		t.Fatalf("large-integer exact replay after restart = %+v, %v", replay, err)
	}
	assertNoServiceAssignment(t, worker, 100*time.Millisecond)
}

func TestServiceHooksCannotMutateCanonicalOrDurableInputs(t *testing.T) {
	store, borrowed := openBorrowedRegressionStore(t)
	var policyCalls atomic.Int64
	policy := llm.AdmissionPolicyFunc(func(_ context.Context, input llm.AdmissionContext) (llm.AdmissionPolicyDecision, error) {
		if got := input.Codec.Contract.Features[hookCodecFeature]; got != 1 {
			return llm.AdmissionPolicyDecision{}, fmt.Errorf("feature version = %d, want 1", got)
		}
		policyCalls.Add(1)
		input.Codec.Contract.Features[hookCodecFeature] = 99
		mutable := input.Request.Messages[1].Blocks[0].Input
		mutable["typed_map"].(map[string]any)["value"] = "policy-mutated"
		mutable["typed_slice"].([]any)[0] = "policy-mutated"
		mutable["typed_struct"].(map[string]any)["value"] = "policy-mutated"
		return llm.AdmissionPolicyDecision{Allowed: true}, nil
	})
	authorizer := llm.ToolAuthorizerFunc(func(_ context.Context, input llm.ToolAuthorization) error {
		input.Task.Codec.Contract.Features[hookCodecFeature] = 77
		input.Request.Messages[1].Blocks[0].Input["typed_map"].(map[string]any)["value"] = "auth-mutated"
		input.Call.Input["nested"].(map[string]any)["value"] = "auth-mutated"
		input.Call.Input["items"].([]any)[0] = "auth-mutated"
		return nil
	})
	registration := llm.CodecRegistration{
		Codec: hookCodec{}, StreamContentType: "text/event-stream",
		AggregateContentType: "application/json", SuccessStatus: 200,
	}
	service := newRegressionService(t, borrowed, regressionServiceOptions{
		codecs: []llm.CodecRegistration{registration}, admission: policy, authorizer: authorizer,
	})
	worker := openTestWorker(t, service, "worker-a", "session-a")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "hook-a", CodecID: hookCodecID,
		Body: []byte(`{"stream":false}`), Task: testRemoteTask(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	assertHookRequestUnmutated(t, assignment.Assignment.Request)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-hook", llm.Event{
		ID: "event-hook", Type: llm.EventToolCalls,
		ToolCalls: []llm.ToolCall{{
			ID: "call-hook", Name: "calculate",
			Input: map[string]any{
				"nested": map[string]string{"value": "original"},
				"items":  []string{"original"},
			},
		}},
	}))
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("tool event after malicious authorizer = %+v, %v", receipt, err)
	}
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "hook-a", RequestDigest: result.RequestDigest,
	})
	if err != nil || !page.Complete || bytes.Contains(page.Decision.Body, []byte("mutated")) ||
		!bytes.Contains(page.Decision.Body, []byte(`"value":"original"`)) {
		t.Fatalf("aggregate output was mutated by hook: %s (%v)", page.Decision.Body, err)
	}
	var task llm.StoreTaskRecord
	err = store.View(t.Context(), func(view llm.StoreView) error {
		loaded, loadErr := view.LoadTask(llm.StoreTaskKey{Caller: "caller-a", Task: result.Identity.TaskID})
		task = loaded
		return loadErr
	})
	if err != nil || task.Codec.Contract.Features[hookCodecFeature] != 1 {
		t.Fatalf("durable Task Codec was mutated: %+v, %v", task.Codec, err)
	}
	if policyCalls.Load() != 1 {
		t.Fatalf("policy calls = %d", policyCalls.Load())
	}
}

func TestServiceWorkerToolEventLargeIntegerExactReplayAfterRestart(t *testing.T) {
	const exact = "9007199254740993"
	store, borrowed := openBorrowedRegressionStore(t)
	service := newRegressionService(t, borrowed, regressionServiceOptions{})
	worker := openTestWorker(t, service, "worker-a", "session-before")
	request := testRequest(true, "calculate")
	request.Tools = []llm.Tool{{Name: "calculate", InputSchema: json.RawMessage(`{"type":"object"}`)}}
	_, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "tool-integer", CodecID: testCodecID,
		Body: mustJSON(t, request),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	delivery := workerDelivery(assignment, "delivery-tool-integer", llm.Event{
		ID: "event-tool-integer", Type: llm.EventToolCalls,
		ToolCalls: []llm.ToolCall{{
			ID: "call-integer", Name: "calculate",
			Input: map[string]any{"value": json.Number(exact)},
		}},
	})
	if receipt, commitErr := worker.CommitEvent(t.Context(), delivery); commitErr != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("initial tool event = %+v, %v", receipt, commitErr)
	}
	shutdownRuntime(t, service)

	service = newRegressionService(t, framework.Borrow[llm.Store](store), regressionServiceOptions{})
	worker = openTestWorker(t, service, "worker-a", "session-after")
	receipt, err := worker.CommitEvent(t.Context(), delivery)
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("tool event exact replay after restart = %+v, %v", receipt, err)
	}
}

func TestServiceSQLiteRecoverySelectsWrongWorkerReceiptForQuarantine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrong-worker.db")
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	service := newRegressionService(t, framework.Borrow[llm.Store](store), regressionServiceOptions{})
	worker := openTestWorker(t, service, "worker-a", "session-before")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "wrong-worker", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "finish")),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	if receipt, commitErr := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-final", llm.Event{
		ID: "event-final", Type: llm.EventFinal, Text: "done",
	})); commitErr != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("commit final = %+v, %v", receipt, commitErr)
	}
	shutdownRuntime(t, service)

	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE llm_worker_receipts SET worker_id = 'worker-other'
		WHERE caller_id = 'caller-a' AND idempotency_key = 'wrong-worker' AND event_id = 'event-final'`); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	service = newRegressionService(t, framework.Borrow[llm.Store](store), regressionServiceOptions{})
	var head llm.StoreRequestHead
	err = store.View(t.Context(), func(view llm.StoreView) error {
		loaded, loadErr := view.LoadRequestHead(llm.StoreRequestKey{
			Caller: "caller-a", IdempotencyKey: "wrong-worker",
		})
		head = loaded
		return loadErr
	})
	if err != nil || !head.RecoveryQuarantined {
		t.Fatalf("wrong-worker receipt was not selected/quarantined: %+v, %v", head, err)
	}
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "wrong-worker", RequestDigest: result.RequestDigest,
	})
	if err != nil || !page.Complete {
		t.Fatalf("quarantined completed response = %+v, %v", page, err)
	}
}

func assertNoServiceAssignment(t *testing.T, worker llm.WorkerConnection, wait time.Duration) {
	t.Helper()
	select {
	case assignment := <-worker.Assignments():
		t.Fatalf("unexpected assignment: %+v", assignment)
	case <-time.After(wait):
	}
}

func wireEventBytes(events []llm.WireEvent) [][]byte {
	result := make([][]byte, len(events))
	for index := range events {
		result[index] = events[index].Data
	}
	return result
}

func equalWireEvents(left, right []llm.WireEvent) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Sequence != right[index].Sequence || !bytes.Equal(left[index].Data, right[index].Data) {
			return false
		}
	}
	return true
}

func assertExactHistoryInteger(t *testing.T, request llm.Request, exact string) {
	t.Helper()
	value := request.Messages[1].Blocks[0].Input["value"]
	if number, ok := value.(json.Number); !ok || number.String() != exact {
		t.Fatalf("history integer = %T(%v), want json.Number(%s)", value, value, exact)
	}
}

func assertHookRequestUnmutated(t *testing.T, request llm.Request) {
	t.Helper()
	input := request.Messages[1].Blocks[0].Input
	for name, value := range map[string]any{
		"typed_map":    input["typed_map"].(map[string]any)["value"],
		"typed_slice":  input["typed_slice"].([]any)[0],
		"typed_struct": input["typed_struct"].(map[string]any)["value"],
	} {
		if value != "original" {
			t.Fatalf("%s = %v, want original", name, value)
		}
	}
}

const seedEchoCodecID llm.CodecID = "test.seed-echo"

type seedEchoCodec struct{}

func (seedEchoCodec) Description() llm.CodecDescription {
	description := testCodec{}.Description()
	description.ID = seedEchoCodecID
	description.Fingerprint = llm.Fingerprint([]byte("seed-echo-v1"))
	return description
}

func (seedEchoCodec) Decode(body []byte) (llm.Request, error) {
	var request llm.Request
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		return llm.Request{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return llm.Request{}, errors.New("trailing request JSON")
	}
	return request, nil
}

func (seedEchoCodec) NewStream(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &seedEchoEncoder{stream: true, session: session.Seed}, nil
}

func (seedEchoCodec) NewAggregate(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &seedEchoEncoder{session: session.Seed}, nil
}

func (seedEchoCodec) AdmissionError(failure llm.AdmissionFailure) ([]byte, error) {
	return json.Marshal(failure)
}

type seedEchoEncoder struct {
	stream  bool
	session llm.SessionSeed
}

func (encoder *seedEchoEncoder) Start() ([][]byte, error) {
	if !encoder.stream {
		return nil, nil
	}
	return [][]byte{[]byte(fmt.Sprintf("session:%s:%x\n", encoder.session.Opaque, encoder.session.Entropy))}, nil
}

func (encoder *seedEchoEncoder) Encode(event llm.Event, seed llm.EventSeed) ([][]byte, bool, error) {
	done := event.EndsResponse()
	frame := []byte(fmt.Sprintf("%s:%s:%s:%x\n", event.Type, event.Text, seed.Opaque, seed.Entropy))
	if encoder.stream {
		return [][]byte{frame}, done, nil
	}
	if !done {
		return nil, false, nil
	}
	return [][]byte{frame}, true, nil
}

type retainedSeedSource struct {
	sessionEntropy []byte
	sessionOpaque  json.RawMessage
	eventEntropy   []byte
	eventOpaque    json.RawMessage
}

func (source *retainedSeedSource) SessionSeed(context.Context, llm.SeedContext) (llm.SessionSeed, error) {
	return llm.SessionSeed{
		CreatedAtUnix: 1_900_000_001, Entropy: source.sessionEntropy, Opaque: source.sessionOpaque,
	}, nil
}

func (source *retainedSeedSource) EventSeed(context.Context, llm.EventSeedContext) (llm.EventSeed, error) {
	return llm.EventSeed{
		EncodedAtUnix: 1_900_000_002, Entropy: source.eventEntropy, Opaque: source.eventOpaque,
	}, nil
}

func (source *retainedSeedSource) destroyBorrowedBuffers() {
	for _, value := range [][]byte{
		source.sessionEntropy, source.sessionOpaque, source.eventEntropy, source.eventOpaque,
	} {
		for index := range value {
			value[index] = 'x'
		}
	}
}

type failingSeedSource struct{}

func (failingSeedSource) SessionSeed(context.Context, llm.SeedContext) (llm.SessionSeed, error) {
	return llm.SessionSeed{}, errors.New("recovery unexpectedly requested a fresh session seed")
}

func (failingSeedSource) EventSeed(context.Context, llm.EventSeedContext) (llm.EventSeed, error) {
	return llm.EventSeed{}, errors.New("recovery unexpectedly requested a fresh event seed")
}

const (
	hookCodecID      llm.CodecID       = "test.hook-isolation"
	hookCodecFeature framework.Feature = "human.test.feature"
)

type hookValue struct {
	Value string `json:"value"`
}

type hookCodec struct{}

func (hookCodec) Description() llm.CodecDescription {
	description := testCodec{}.Description()
	description.ID = hookCodecID
	description.Fingerprint = llm.Fingerprint([]byte("hook-isolation-v1"))
	description.Contract.Features = map[framework.Feature]uint16{hookCodecFeature: 1}
	return description
}

func (hookCodec) Decode(body []byte) (llm.Request, error) {
	var wire struct {
		Stream bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return llm.Request{}, err
	}
	return llm.Request{
		Model: "human", Stream: wire.Stream,
		Messages: []llm.Message{
			{Role: llm.RoleUser, Blocks: []llm.Block{{Type: llm.BlockText, Text: "calculate"}}},
			{Role: llm.RoleAssistant, Blocks: []llm.Block{{
				Type: llm.BlockToolUse, ToolCallID: "history-call", ToolName: "calculate",
				Input: map[string]any{
					"typed_map":    map[string]string{"value": "original"},
					"typed_slice":  []string{"original"},
					"typed_struct": &hookValue{Value: "original"},
				},
			}}},
		},
		Tools: []llm.Tool{{Name: "calculate", InputSchema: json.RawMessage(`{"type":"object"}`)}},
	}, nil
}

func (hookCodec) NewStream(session llm.EncoderSession) (llm.Encoder, error) {
	return testCodec{}.NewStream(session)
}

func (hookCodec) NewAggregate(session llm.EncoderSession) (llm.Encoder, error) {
	return testCodec{}.NewAggregate(session)
}

func (hookCodec) AdmissionError(failure llm.AdmissionFailure) ([]byte, error) {
	return testCodec{}.AdmissionError(failure)
}

func mustRegressionStore(t *testing.T) framework.Resource[llm.Store] {
	t.Helper()
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: filepath.Join(t.TempDir(), "owned.db")})
	if err != nil {
		t.Fatal(err)
	}
	return resource
}

type bindingCommitUnknownStore struct {
	llm.Store
	unknown atomic.Bool
	calls   atomic.Int32
}

func (store *bindingCommitUnknownStore) Bind(ctx context.Context, binding llm.StoreBinding) error {
	store.calls.Add(1)
	err := store.Store.Bind(ctx, binding)
	if err == nil && store.unknown.Swap(false) {
		return &llm.StoreCommitUnknownError{Cause: errors.New("injected lost binding acknowledgement")}
	}
	return err
}

type unknownAfterCommitStore struct {
	llm.Store
	unknown     atomic.Bool
	afterCommit func()
}

func (store *unknownAfterCommitStore) Update(ctx context.Context, callback func(llm.StoreTx) error) error {
	err := store.Store.Update(ctx, callback)
	if err == nil && store.unknown.Swap(false) {
		store.afterCommit()
		return &llm.StoreCommitUnknownError{Cause: errors.New("injected lost commit acknowledgement")}
	}
	return err
}

var errInjectedStoreOutage = errors.New("injected temporary Store outage")

type transientUnknownStore struct {
	llm.Store
	unknownNext atomic.Bool
	failViews   atomic.Int64
	mu          sync.Mutex
}

type taskBarrierIDs struct {
	needed    uint64
	taskSeen  atomic.Uint64
	next      atomic.Uint64
	gate      chan struct{}
	once      sync.Once
	mu        sync.Mutex
	allocated []llm.TaskID
}

func newTaskBarrierIDs(needed uint64) *taskBarrierIDs {
	return &taskBarrierIDs{needed: needed, gate: make(chan struct{})}
}

func (source *taskBarrierIDs) NewID(ctx context.Context, kind llm.IDKind) (string, error) {
	value := fmt.Sprintf("%s-%08d", kind, source.next.Add(1))
	if kind == llm.IDTask {
		source.mu.Lock()
		source.allocated = append(source.allocated, llm.TaskID(value))
		source.mu.Unlock()
		if source.taskSeen.Add(1) == source.needed {
			source.once.Do(func() { close(source.gate) })
		}
		select {
		case <-source.gate:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return value, nil
}

func (source *taskBarrierIDs) taskIDs() []llm.TaskID {
	source.mu.Lock()
	defer source.mu.Unlock()
	return append([]llm.TaskID(nil), source.allocated...)
}

func (store *transientUnknownStore) arm(failedViews int64) {
	store.failViews.Store(0)
	store.unknownNext.Store(true)
	store.mu.Lock()
	store.failViews.Store(-failedViews)
	store.mu.Unlock()
}

func (store *transientUnknownStore) Update(ctx context.Context, callback func(llm.StoreTx) error) error {
	err := store.Store.Update(ctx, callback)
	if err == nil && store.unknownNext.Swap(false) {
		store.mu.Lock()
		pending := -store.failViews.Load()
		store.failViews.Store(pending)
		store.mu.Unlock()
		return &llm.StoreCommitUnknownError{Cause: errors.New("injected lost commit acknowledgement")}
	}
	return err
}

func (store *transientUnknownStore) View(ctx context.Context, callback func(llm.StoreView) error) error {
	for {
		remaining := store.failViews.Load()
		if remaining <= 0 {
			return store.Store.View(ctx, callback)
		}
		if store.failViews.CompareAndSwap(remaining, remaining-1) {
			return errInjectedStoreOutage
		}
	}
}

func jsonNumber(value string) json.Number { return json.Number(value) }

func formatRegressionValue(value any) string { return fmt.Sprint(value) }
