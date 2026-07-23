package llm_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
	"github.com/vibe-agi/human/observe"
)

func TestServiceStreamingReplayAndWorkerReceipts(t *testing.T) {
	service := openTestService(t, filepath.Join(t.TempDir(), "stream.db"), nil)
	worker := openTestWorker(t, service, "worker-a", "session-a")
	request := testRequest(true, "hello")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "turn-a", CodecID: testCodecID,
		Body: mustJSON(t, request),
	})
	if err != nil {
		t.Fatalf("admit stream: %v", err)
	}
	if result.Replay || result.Response.Mode != llm.ResponseStream ||
		!result.Response.DecisionCommitted || result.Response.Decision.StatusCode != 200 {
		t.Fatalf("unexpected admission result: %+v", result)
	}
	assignment := receiveServiceAssignment(t, worker)
	if assignment.Assignment.Identity != result.Identity {
		t.Fatalf("assignment identity = %+v, want %+v", assignment.Assignment.Identity, result.Identity)
	}
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatalf("ack assignment: %v", err)
	}
	progress := workerDelivery(assignment, "delivery-progress", llm.Event{
		ID: "event-progress", Type: llm.EventProgress, Text: "working",
	})
	receipt, err := worker.CommitEvent(t.Context(), progress)
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("commit progress = %+v, %v", receipt, err)
	}
	replayedReceipt, err := worker.CommitEvent(t.Context(), progress)
	if err != nil || replayedReceipt.Decision != llm.WorkerEventACK {
		t.Fatalf("replay progress = %+v, %v", replayedReceipt, err)
	}
	final := workerDelivery(assignment, "delivery-final", llm.Event{
		ID: "event-final", Type: llm.EventFinal, Text: "done",
	})
	if receipt, err = worker.CommitEvent(t.Context(), final); err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("commit final = %+v, %v", receipt, err)
	}
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "turn-a", RequestDigest: result.RequestDigest,
	})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if page.Mode != llm.ResponseStream || !page.Complete || len(page.Events) != 3 {
		t.Fatalf("response = %+v, want start/progress/final", page)
	}
	if string(page.Events[0].Data) != "start\n" || string(page.Events[1].Data) != "progress:working\n" ||
		string(page.Events[2].Data) != "final:done\n" {
		t.Fatalf("wire events = %#v", page.Events)
	}
	var paged []llm.WireEvent
	var cursor uint64
	for turn := 0; turn < 16; turn++ {
		chunk, readErr := service.ReadResponse(t.Context(), llm.ResponseQuery{
			CallerID: "caller-a", IdempotencyKey: "turn-a", RequestDigest: result.RequestDigest,
			After: cursor, Limit: 1,
		})
		if readErr != nil {
			t.Fatal(readErr)
		}
		if chunk.Complete && chunk.Cursor != page.Cursor {
			t.Fatalf("stream prefix reported complete at cursor %d of %d", chunk.Cursor, page.Cursor)
		}
		paged = append(paged, chunk.Events...)
		if chunk.Complete {
			cursor = chunk.Cursor
			break
		}
		if chunk.Cursor <= cursor {
			t.Fatalf("stream pagination did not advance beyond %d", cursor)
		}
		cursor = chunk.Cursor
	}
	if cursor != page.Cursor || len(paged) != len(page.Events) {
		t.Fatalf("paged stream cursor/events = %d/%d, want %d/%d", cursor, len(paged), page.Cursor, len(page.Events))
	}
	for index := range paged {
		if paged[index].Sequence != page.Events[index].Sequence ||
			string(paged[index].Data) != string(page.Events[index].Data) {
			t.Fatalf("paged wire %d = %+v, want %+v", index, paged[index], page.Events[index])
		}
	}
	emptyTail, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "turn-a", RequestDigest: result.RequestDigest,
		After: cursor, Limit: 1,
	})
	if err != nil || !emptyTail.Complete || emptyTail.Cursor != cursor || len(emptyTail.Events) != 0 {
		t.Fatalf("empty terminal tail = %+v, %v", emptyTail, err)
	}
	replay, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "turn-a", CodecID: testCodecID,
		Body: mustJSON(t, request),
	})
	if err != nil || !replay.Replay || replay.Identity != result.Identity ||
		replay.Response.Mode != llm.ResponseStream {
		t.Fatalf("exact replay = %+v, %v", replay, err)
	}
	conflicting := testRequest(true, "different")
	_, err = service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "turn-a", CodecID: testCodecID,
		Body: mustJSON(t, conflicting),
	})
	if !errors.Is(err, llm.ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay error = %v", err)
	}
}

func TestServiceWaitResponseWakesOnDurableProgressAndCancels(t *testing.T) {
	service := openTestService(t, filepath.Join(t.TempDir(), "wait.db"), nil)
	worker := openTestWorker(t, service, "worker-a", "session-a")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "turn-wait", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "hello")),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	initial, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "turn-wait", RequestDigest: result.RequestDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	type waitResult struct {
		page llm.ResponsePage
		err  error
	}
	waited := make(chan waitResult, 1)
	go func() {
		page, waitErr := service.WaitResponse(context.Background(), llm.ResponseQuery{
			CallerID: "caller-a", IdempotencyKey: "turn-wait", RequestDigest: result.RequestDigest,
			After: initial.Cursor,
		})
		waited <- waitResult{page: page, err: waitErr}
	}()
	receipt, err := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-progress", llm.Event{
		ID: "event-progress", Type: llm.EventProgress, Text: "working",
	}))
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("commit progress = %+v, %v", receipt, err)
	}
	select {
	case outcome := <-waited:
		if outcome.err != nil || outcome.page.Cursor <= initial.Cursor || len(outcome.page.Events) != 1 {
			t.Fatalf("wait response = %+v, %v", outcome.page, outcome.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitResponse did not observe durable progress")
	}

	current, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "turn-wait", RequestDigest: result.RequestDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = service.WaitResponse(canceled, llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "turn-wait", RequestDigest: result.RequestDigest,
		After: current.Cursor,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled WaitResponse = %v", err)
	}
}

func TestServiceAssignmentRedeliveryAcrossRestartAndAggregate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	service := openTestService(t, path, nil)
	worker := openTestWorker(t, service, "worker-a", "session-before")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "aggregate-a", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(false, "aggregate")),
	})
	if err != nil {
		t.Fatalf("admit aggregate: %v", err)
	}
	if result.Response.Mode != llm.ResponseAggregate || result.Response.DecisionCommitted {
		t.Fatalf("aggregate admission projection = %+v", result.Response)
	}
	first := receiveServiceAssignment(t, worker)
	shutdownRuntime(t, service)

	service = openTestService(t, path, nil)
	worker = openTestWorker(t, service, "worker-a", "session-after")
	redelivered := receiveServiceAssignment(t, worker)
	if redelivered.ID != first.ID || redelivered.Assignment.Identity != first.Assignment.Identity {
		t.Fatalf("redelivery changed identity: before=%+v after=%+v", first, redelivered)
	}
	if err := worker.AckAssignment(t.Context(), redelivered.ID); err != nil {
		t.Fatalf("ack redelivery: %v", err)
	}
	receipt, err := worker.CommitEvent(t.Context(), workerDelivery(redelivered, "delivery-final", llm.Event{
		ID: "event-final", Type: llm.EventFinal, Text: "packaged",
	}))
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("commit aggregate = %+v, %v", receipt, err)
	}
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "aggregate-a", RequestDigest: result.RequestDigest,
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("read aggregate: %v", err)
	}
	if page.Mode != llm.ResponseAggregate || !page.Complete || page.Decision.StatusCode != 200 ||
		len(page.Decision.Body) == 0 || len(page.Events) != 0 {
		t.Fatalf("aggregate response = %+v", page)
	}
	replay, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "aggregate-a", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(false, "aggregate")),
	})
	if err != nil || !replay.Replay || replay.Response.Mode != llm.ResponseAggregate ||
		!replay.Response.Complete {
		t.Fatalf("aggregate exact replay = %+v, %v", replay, err)
	}
}

func TestServiceCommitUnknownConvergesForAssignmentAndEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unknown.db")
	base, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := base.Value()
	if err != nil {
		t.Fatal(err)
	}
	wrapper := &commitUnknownStore{Store: inner}
	resource, err := framework.Own[llm.Store](wrapper, base.Release)
	if err != nil {
		t.Fatal(err)
	}
	service := newTestService(t, resource, nil)
	worker := openTestWorker(t, service, "worker-a", "session-a")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "unknown-a", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "unknown")),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	wrapper.unknown.Store(true)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatalf("commit-unknown assignment ACK did not reconcile: %v", err)
	}
	wrapper.unknown.Store(true)
	receipt, err := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-final", llm.Event{
		ID: "event-final", Type: llm.EventFinal, Text: "settled",
	}))
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("commit-unknown event did not reconcile: %+v, %v", receipt, err)
	}
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "unknown-a", RequestDigest: result.RequestDigest,
	})
	if err != nil || !page.Complete {
		t.Fatalf("response after reconciliation = %+v, %v", page, err)
	}
}

func TestServiceContinuationUsesAffinityAndCompletesEveryToolResult(t *testing.T) {
	service := openTestServiceWithLimit(t, filepath.Join(t.TempDir(), "tools.db"), 4096)
	worker := openTestWorker(t, service, "worker-a", "session-a")
	request := testRequest(true, "tools")
	for index := 0; index < 24; index++ {
		request.Tools = append(request.Tools, llm.Tool{
			Name: fmt.Sprintf("tool_%02d", index), InputSchema: json.RawMessage(`{"type":"object"}`),
		})
	}
	task := testRemoteTask()
	first, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "tools-a", CodecID: testCodecID,
		Body: mustJSON(t, request), Task: task,
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	calls := make([]llm.ToolCall, 0, len(request.Tools))
	for index, tool := range request.Tools {
		calls = append(calls, llm.ToolCall{ID: fmt.Sprintf("call_%02d", index), Name: tool.Name, Input: map[string]any{"n": index}})
	}
	receipt, err := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-tools", llm.Event{
		ID: "event-tools", Type: llm.EventToolCalls, ToolCalls: calls,
	}))
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("tool calls = %+v, %v", receipt, err)
	}
	continuation := testRequest(true, "continue")
	for index := range calls {
		continuation.Messages = append(continuation.Messages, llm.Message{
			Role: llm.RoleTool, Blocks: []llm.Block{{
				Type: llm.BlockToolResult, ToolCallID: calls[index].ID,
				Output: map[string]any{"value": index},
			}},
		})
	}
	second, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "tools-b", CodecID: testCodecID,
		Body: mustJSON(t, continuation), Task: task,
	})
	if err != nil {
		t.Fatalf("admit continuation: %v", err)
	}
	if second.Identity.TaskID != first.Identity.TaskID {
		t.Fatalf("affinity continuation task = %q, want %q", second.Identity.TaskID, first.Identity.TaskID)
	}
}

func TestServiceEnforcesToolCallPolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		policy llm.ToolCallPolicy
		tools  []llm.Tool
		calls  []llm.ToolCall
		code   llm.WorkerRejectionCode
	}{
		{
			name: "disabled", policy: llm.ToolCallsDisabled, code: llm.WorkerRejectForbidden,
			calls: []llm.ToolCall{{ID: "call-a", Name: "tool_a", Input: map[string]any{}}},
		},
		{
			name: "serial", policy: llm.ToolCallsSerial, code: llm.WorkerRejectInvalid,
			calls: []llm.ToolCall{
				{ID: "call-a", Name: "tool_a", Input: map[string]any{}},
				{ID: "call-b", Name: "tool_b", Input: map[string]any{}},
			},
		},
		{
			name: "json-tool-rejects-text", code: llm.WorkerRejectInvalid,
			tools: []llm.Tool{{Name: "tool_a", InputSchema: json.RawMessage(`{"type":"object"}`)}},
			calls: []llm.ToolCall{{ID: "call-a", Name: "tool_a", TextInput: testStringPointer("freeform")}},
		},
		{
			name: "text-tool-requires-text", code: llm.WorkerRejectInvalid,
			tools: []llm.Tool{{Name: "tool_a", InputKind: llm.ToolInputText}},
			calls: []llm.ToolCall{{ID: "call-a", Name: "tool_a", Input: map[string]any{}}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := openTestService(t, filepath.Join(t.TempDir(), "policy.db"), nil)
			worker := openTestWorker(t, service, "worker-a", "session-a")
			request := testRequest(true, "policy")
			request.ToolCallPolicy = test.policy
			request.Tools = test.tools
			if len(request.Tools) == 0 {
				request.Tools = []llm.Tool{
					{Name: "tool_a", InputSchema: json.RawMessage(`{"type":"object"}`)},
					{Name: "tool_b", InputSchema: json.RawMessage(`{"type":"object"}`)},
				}
			}
			_, err := service.Admit(t.Context(), llm.AdmissionRequest{
				CallerID: "caller-a", IdempotencyKey: llm.IdempotencyKey("policy-" + test.name),
				CodecID: testCodecID, Body: mustJSON(t, request),
			})
			if err != nil {
				t.Fatal(err)
			}
			assignment := receiveServiceAssignment(t, worker)
			if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
				t.Fatal(err)
			}
			receipt, err := worker.CommitEvent(t.Context(), workerDelivery(
				assignment, llm.WorkerDeliveryID("delivery-"+test.name), llm.Event{
					ID: "event-" + test.name, Type: llm.EventToolCalls, ToolCalls: test.calls,
				},
			))
			if err != nil || receipt.Decision != llm.WorkerEventNACK || receipt.Code != test.code {
				t.Fatalf("policy settlement = %+v, %v; want NACK %q", receipt, err, test.code)
			}
		})
	}
}

func testStringPointer(value string) *string { return &value }

func TestServiceShutdownSynchronouslyClosesAdmission(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	policy := llm.AdmissionPolicyFunc(func(ctx context.Context, _ llm.AdmissionContext) (llm.AdmissionPolicyDecision, error) {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return llm.AdmissionPolicyDecision{Allowed: true}, nil
		case <-ctx.Done():
			return llm.AdmissionPolicyDecision{}, ctx.Err()
		}
	})
	service := openTestService(t, filepath.Join(t.TempDir(), "shutdown.db"), policy)
	firstDone := make(chan error, 1)
	go func() {
		_, err := service.Admit(context.Background(), llm.AdmissionRequest{
			CallerID: "caller-a", IdempotencyKey: "shutdown-a", CodecID: testCodecID,
			Body: mustJSON(t, testRequest(true, "first")),
		})
		firstDone <- err
	}()
	<-entered
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- service.Shutdown(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for {
		attemptCtx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
		_, err := service.Admit(attemptCtx, llm.AdmissionRequest{
			CallerID: "caller-a", IdempotencyKey: "shutdown-b", CodecID: testCodecID,
			Body: mustJSON(t, testRequest(true, "second")),
		})
		cancel()
		if errors.Is(err, llm.ErrServiceClosed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admission remained open during Shutdown: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	<-firstDone
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}

func TestServiceOfflineQueueHasStableFIFO(t *testing.T) {
	clock := &stepClock{next: time.Unix(1_900_000_000, 0)}
	service := openTestServiceOptions(t, filepath.Join(t.TempDir(), "fifo.db"), nil, 0, clock)
	for index := 0; index < 8; index++ {
		_, err := service.Admit(t.Context(), llm.AdmissionRequest{
			CallerID:       llm.CallerID(fmt.Sprintf("caller-%02d", index)),
			IdempotencyKey: llm.IdempotencyKey(fmt.Sprintf("fifo-%02d", index)),
			CodecID:        testCodecID, Body: mustJSON(t, testRequest(true, fmt.Sprintf("%02d", index))),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	worker := openTestWorker(t, service, "worker-a", "session-a")
	for index := 0; index < 8; index++ {
		assignment := receiveServiceAssignment(t, worker)
		want := llm.IdempotencyKey(fmt.Sprintf("fifo-%02d", index))
		if assignment.Assignment.Identity.IdempotencyKey != want {
			t.Fatalf("FIFO item %d = %q, want %q", index, assignment.Assignment.Identity.IdempotencyKey, want)
		}
		if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestServiceLateOldLeaseSettlesWithoutPoisoningConnection(t *testing.T) {
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: filepath.Join(t.TempDir(), "lease.db")})
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	service := newTestService(t, framework.Borrow[llm.Store](store), nil)
	workerA := openTestWorker(t, service, "worker-a", "session-a")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "lease-a", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "lease")),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, workerA)
	if err := workerA.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	committed := workerDelivery(assignment, "delivery-progress", llm.Event{
		ID: "event-progress", Type: llm.EventProgress, Text: "from-a",
	})
	if receipt, err := workerA.CommitEvent(t.Context(), committed); err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("initial A event = %+v, %v", receipt, err)
	}
	var leaseB llm.WorkerLeaseID = "lease-worker-b"
	err = store.Update(t.Context(), func(tx llm.StoreTx) error {
		task, loadErr := tx.LoadTask(llm.StoreTaskKey{Caller: "caller-a", Task: result.Identity.TaskID})
		if loadErr != nil {
			return loadErr
		}
		next := task
		next.LeaseOwner, next.LeaseID = "worker-b", leaseB
		next.Revision++
		next.UpdatedAt = next.UpdatedAt.Add(time.Second)
		changed, changeErr := tx.CompareAndSwapTask(llm.StoreTaskMutation{
			Key: task.Key, ExpectedRevision: task.Revision, Next: next,
		})
		if changeErr != nil {
			return changeErr
		}
		if !changed {
			return errors.New("lease mutation lost CAS")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := workerA.CommitEvent(t.Context(), committed); err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("exact A receipt replay after re-lease = %+v, %v", receipt, err)
	}
	late := workerDelivery(assignment, "delivery-late", llm.Event{
		ID: "event-late", Type: llm.EventProgress, Text: "late-a",
	})
	if receipt, err := workerA.CommitEvent(t.Context(), late); err != nil ||
		receipt.Decision != llm.WorkerEventNACK || receipt.Code != llm.WorkerRejectStaleLease {
		t.Fatalf("late A event = %+v, %v", receipt, err)
	}
	workerB := openTestWorker(t, service, "worker-b", "session-b")
	final := llm.WorkerEventDelivery{
		ID: "delivery-b-final", Identity: assignment.Assignment.Identity, LeaseID: leaseB,
		Event: llm.Event{ID: "event-b-final", Type: llm.EventFinal, Text: "from-b"},
	}
	if receipt, err := workerB.CommitEvent(t.Context(), final); err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("B event after stale A NACK = %+v, %v", receipt, err)
	}
}

func TestServiceRecoveryQuarantinesBadRecordAndRestoresGoodRecord(t *testing.T) {
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: filepath.Join(t.TempDir(), "quarantine.db")})
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	service := newTestService(t, framework.Borrow[llm.Store](store), nil)
	bad, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-bad", IdempotencyKey: "recovery-bad", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "bad")),
	})
	if err != nil {
		t.Fatal(err)
	}
	good, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-good", IdempotencyKey: "recovery-good", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "good")),
	})
	if err != nil {
		t.Fatal(err)
	}
	shutdownRuntime(t, service)
	err = store.Update(t.Context(), func(tx llm.StoreTx) error {
		task, loadErr := tx.LoadTask(llm.StoreTaskKey{Caller: "caller-bad", Task: bad.Identity.TaskID})
		if loadErr != nil {
			return loadErr
		}
		next := task
		next.State = llm.TaskState("corrupt-state")
		next.Revision++
		next.UpdatedAt = next.UpdatedAt.Add(time.Second)
		changed, changeErr := tx.CompareAndSwapTask(llm.StoreTaskMutation{
			Key: task.Key, ExpectedRevision: task.Revision, Next: next,
		})
		if changeErr != nil {
			return changeErr
		}
		if !changed {
			return errors.New("corrupt-state mutation lost CAS")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	service = newTestService(t, framework.Borrow[llm.Store](store), nil)
	badPage, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-bad", IdempotencyKey: "recovery-bad", RequestDigest: bad.RequestDigest,
	})
	if err != nil {
		t.Fatalf("read quarantined response: %v", err)
	}
	if !badPage.Complete || len(badPage.Events) < 2 {
		t.Fatalf("quarantined response is not finite: %+v", badPage)
	}
	worker := openTestWorker(t, service, "worker-a", "session-recovered")
	recovered := receiveServiceAssignment(t, worker)
	if recovered.Assignment.Identity != good.Identity {
		t.Fatalf("recovered assignment = %+v, want good %+v", recovered.Assignment.Identity, good.Identity)
	}
}

func TestServiceRecoveryQuarantinesWorkerEventReceiptFaultsWithoutDuplicateReplay(t *testing.T) {
	for _, test := range []struct {
		name  string
		fault recoveryReceiptFault
		code  llm.WorkerRejectionCode
	}{
		{name: "missing", fault: receiptMissing, code: llm.WorkerRejectResponseClosed},
		{name: "wrong-digest", fault: receiptWrongDigest, code: llm.WorkerRejectEventConflict},
		{name: "wrong-worker", fault: receiptWrongWorker, code: llm.WorkerRejectEventConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: filepath.Join(t.TempDir(), "receipt.db")})
			if err != nil {
				t.Fatal(err)
			}
			store, err := resource.Value()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = resource.Release(context.Background()) })
			service := newTestService(t, framework.Borrow[llm.Store](store), nil)
			worker := openTestWorker(t, service, "worker-a", "session-before")
			result, err := service.Admit(t.Context(), llm.AdmissionRequest{
				CallerID: "caller-a", IdempotencyKey: "receipt-a", CodecID: testCodecID,
				Body: mustJSON(t, testRequest(true, "receipt")),
			})
			if err != nil {
				t.Fatal(err)
			}
			assignment := receiveServiceAssignment(t, worker)
			if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
				t.Fatal(err)
			}
			progress := workerDelivery(assignment, "delivery-progress", llm.Event{
				ID: "event-progress", Type: llm.EventProgress, Text: "working",
			})
			if receipt, commitErr := worker.CommitEvent(t.Context(), progress); commitErr != nil ||
				receipt.Decision != llm.WorkerEventACK {
				t.Fatalf("commit progress = %+v, %v", receipt, commitErr)
			}
			shutdownRuntime(t, service)

			key := llm.StoreRequestKey{Caller: "caller-a", IdempotencyKey: "receipt-a"}
			var recovery llm.StoreRecoveryRecord
			err = store.View(t.Context(), func(view llm.StoreView) error {
				request, loadErr := view.LoadRequest(key, llm.StoreReadLimit{MaxBytes: 64 << 20})
				if loadErr != nil {
					return loadErr
				}
				task, loadErr := view.LoadTask(request.Task)
				recovery = llm.StoreRecoveryRecord{Request: request, Task: task}
				return loadErr
			})
			if err != nil {
				t.Fatal(err)
			}
			faulted := &recoveryReceiptFaultStore{
				Store: store, recovery: recovery, eventID: "event-progress", fault: test.fault,
			}
			faulted.forceRecovery.Store(true)
			service = newTestService(t, framework.Borrow[llm.Store](faulted), nil)
			page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
				CallerID: "caller-a", IdempotencyKey: "receipt-a", RequestDigest: result.RequestDigest,
			})
			if err != nil || !page.Complete {
				t.Fatalf("quarantined response = %+v, %v", page, err)
			}
			progressFrames := 0
			for _, event := range page.Events {
				if string(event.Data) == "progress:working\n" {
					progressFrames++
				}
			}
			if progressFrames != 1 {
				t.Fatalf("durable progress frames = %d, response=%+v", progressFrames, page)
			}
			worker = openTestWorker(t, service, "worker-a", "session-after")
			receipt, err := worker.CommitEvent(t.Context(), progress)
			if err != nil || receipt.Decision != llm.WorkerEventNACK || receipt.Code != test.code {
				t.Fatalf("orphan exact replay = %+v, %v", receipt, err)
			}
			after, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
				CallerID: "caller-a", IdempotencyKey: "receipt-a", RequestDigest: result.RequestDigest,
			})
			if err != nil || len(after.Events) != len(page.Events) {
				t.Fatalf("orphan replay changed response = %+v, %v", after, err)
			}

			// Quarantine is a durable adjudication, not a synthetic receipt. Retention
			// may now erase corrupt history; a delayed durable-outbox event must still
			// settle with the same deterministic NACK from the request head.
			shutdownRuntime(t, service)
			prunedAt := time.Unix(1_900_000_000, 0).UTC()
			err = store.Update(t.Context(), func(tx llm.StoreTx) error {
				current, loadErr := tx.LoadRequest(key, llm.StoreReadLimit{MaxBytes: 64 << 20})
				if loadErr != nil {
					return loadErr
				}
				next := current
				next.CanonicalPayload = nil
				next.Decision.Body = nil
				next.PayloadPrunedAt = &prunedAt
				next.Revision++
				changed, changeErr := tx.CompareAndSwapRequest(llm.StoreRequestMutation{
					Key: key, ExpectedRevision: current.Revision, Next: next,
				})
				if changeErr != nil || !changed {
					return errors.Join(changeErr, llm.ErrStoreConflict)
				}
				_, deleteErr := tx.DeleteTombstonedResponseEvents(key)
				return deleteErr
			})
			if err != nil {
				t.Fatalf("retention tombstone after quarantine: %v", err)
			}
			faulted.forceRecovery.Store(false)
			service = newTestService(t, framework.Borrow[llm.Store](faulted), nil)
			worker = openTestWorker(t, service, "worker-a", "session-after-retention")
			receipt, err = worker.CommitEvent(t.Context(), progress)
			if err != nil || receipt.Decision != llm.WorkerEventNACK || receipt.Code != test.code {
				t.Fatalf("late event after quarantine retention = %+v, %v", receipt, err)
			}
		})
	}
}

func TestServiceRejectsOversizeAssignmentBeforePersistence(t *testing.T) {
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: filepath.Join(t.TempDir(), "assignment-limit.db")})
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "test-deployment", Store: resource,
		Codecs: []llm.CodecRegistration{{
			Codec: testCodec{}, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json",
		}},
		Clock: &stepClock{next: time.Unix(1_800_000_000, 0)}, IDs: &sequenceIDs{},
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
		Admission:               llm.AdmitAll(),
		WorkerPayloadLimitBytes: 8 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownRuntime(t, service) })
	worker := openTestWorker(t, service, "worker-a", "session-a")
	small, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "small-a", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "small")),
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := worker.CommitEvent(t.Context(), workerDelivery(assignment, "delivery-large", llm.Event{
		ID: "event-large", Type: llm.EventProgress, Text: strings.Repeat("x", 6<<10),
	}))
	if err != nil || receipt.Decision != llm.WorkerEventNACK || receipt.Code != llm.WorkerRejectInvalid {
		t.Fatalf("oversize worker event = %+v, %v", receipt, err)
	}
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: "small-a", RequestDigest: small.RequestDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Complete {
		t.Fatalf("oversize worker event changed durable response: %+v", page)
	}

	_, err = service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "oversize-a", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, strings.Repeat("x", 6<<10))),
	})
	var admission *llm.AdmissionError
	if !errors.As(err, &admission) || admission.Failure.Status != 413 {
		t.Fatalf("oversize assignment error = %v", err)
	}
	storeErr := store.View(t.Context(), func(view llm.StoreView) error {
		_, loadErr := view.LoadRequestHead(llm.StoreRequestKey{Caller: "caller-a", IdempotencyKey: "oversize-a"})
		return loadErr
	})
	if !errors.Is(storeErr, llm.ErrStoreRecordNotFound) {
		t.Fatalf("oversize assignment was persisted: %v", storeErr)
	}

}

const testCodecID llm.CodecID = "test.service"

type testCodec struct{}

func (testCodec) Description() llm.CodecDescription {
	return llm.CodecDescription{
		Contract: framework.Contract{ID: llm.CodecContractID, Major: llm.CodecContractMajor},
		ID:       testCodecID, Version: "1", Fingerprint: llm.Fingerprint([]byte("service-test-codec-v1")),
		Limits: llm.CodecLimits{
			MaxRequestBytes: 8 << 20, MaxStreamFrameBytes: 1 << 20,
			MaxStreamFramesPerStep: 16, MaxAggregateBytes: 8 << 20,
			MaxAdmissionErrorBytes: 1 << 20,
		},
		OverloadedStatus: 503,
	}
}

func (testCodec) Decode(body []byte) (llm.Request, error) {
	var request llm.Request
	err := json.Unmarshal(body, &request)
	return request, err
}

func (testCodec) NewStream(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &testEncoder{stream: true}, nil
}

func (testCodec) NewAggregate(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &testEncoder{}, nil
}

func (testCodec) AdmissionError(failure llm.AdmissionFailure) ([]byte, error) {
	return json.Marshal(failure)
}

type testEncoder struct {
	stream bool
	events []llm.Event
}

func (encoder *testEncoder) Start() ([][]byte, error) {
	if encoder.stream {
		return [][]byte{[]byte("start\n")}, nil
	}
	return nil, nil
}

func (encoder *testEncoder) Encode(event llm.Event, _ llm.EventSeed) ([][]byte, bool, error) {
	encoder.events = append(encoder.events, event)
	done := event.EndsResponse()
	if encoder.stream {
		return [][]byte{[]byte(fmt.Sprintf("%s:%s\n", event.Type, event.Text))}, done, nil
	}
	if !done {
		return nil, false, nil
	}
	body, err := json.Marshal(encoder.events)
	return [][]byte{body}, true, err
}

type stepClock struct {
	mu   sync.Mutex
	next time.Time
}

func (clock *stepClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	result := clock.next
	clock.next = clock.next.Add(time.Millisecond)
	return result
}

type sequenceIDs struct{ next atomic.Uint64 }

func (source *sequenceIDs) NewID(_ context.Context, kind llm.IDKind) (string, error) {
	return fmt.Sprintf("%s-%08d", kind, source.next.Add(1)), nil
}

type commitUnknownStore struct {
	llm.Store
	unknown atomic.Bool
}

func (store *commitUnknownStore) Update(ctx context.Context, callback func(llm.StoreTx) error) error {
	err := store.Store.Update(ctx, callback)
	if err == nil && store.unknown.Swap(false) {
		return &llm.StoreCommitUnknownError{Cause: errors.New("injected lost commit acknowledgement")}
	}
	return err
}

type recoveryReceiptFault uint8

const (
	receiptMissing recoveryReceiptFault = iota + 1
	receiptWrongDigest
	receiptWrongWorker
)

type recoveryReceiptFaultStore struct {
	llm.Store
	recovery      llm.StoreRecoveryRecord
	eventID       string
	fault         recoveryReceiptFault
	forceRecovery atomic.Bool
}

func (store *recoveryReceiptFaultStore) View(
	ctx context.Context,
	callback func(llm.StoreView) error,
) error {
	return store.Store.View(ctx, func(view llm.StoreView) error {
		return callback(recoveryReceiptFaultView{StoreView: view, store: store})
	})
}

type recoveryReceiptFaultView struct {
	llm.StoreView
	store *recoveryReceiptFaultStore
}

func (view recoveryReceiptFaultView) ScanRecovery(scan llm.StoreRecoveryScan) ([]llm.StoreRecoveryRecord, error) {
	if !view.store.forceRecovery.Load() {
		return view.StoreView.ScanRecovery(scan)
	}
	if scan.After != nil {
		return nil, nil
	}
	return []llm.StoreRecoveryRecord{view.store.recovery}, nil
}

func (view recoveryReceiptFaultView) LoadWorkerReceipt(
	key llm.StoreRequestKey,
	eventID string,
) (llm.StoreWorkerReceiptRecord, error) {
	receipt, err := view.StoreView.LoadWorkerReceipt(key, eventID)
	if err != nil || eventID != view.store.eventID {
		return receipt, err
	}
	switch view.store.fault {
	case receiptMissing:
		return llm.StoreWorkerReceiptRecord{}, llm.ErrStoreRecordNotFound
	case receiptWrongDigest:
		receipt.Digest = llm.StoreDigest(strings.Repeat("0", 64))
	case receiptWrongWorker:
		receipt.Worker = "worker-other"
	}
	return receipt, nil
}

func openTestService(t *testing.T, path string, policy llm.AdmissionPolicy) *llm.Service {
	return openTestServiceOptions(t, path, policy, 0, &stepClock{next: time.Unix(1_800_000_000, 0)})
}

func openTestServiceWithLimit(t *testing.T, path string, limit int64) *llm.Service {
	return openTestServiceOptions(t, path, nil, limit, &stepClock{next: time.Unix(1_800_000_000, 0)})
}

func openTestServiceOptions(t *testing.T, path string, policy llm.AdmissionPolicy, limit int64, clock llm.Clock) *llm.Service {
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	return newTestServiceWithOptions(t, resource, policy, limit, clock)
}

// baseObserveTime is a fixed, deterministic clock origin for observer tests, so
// emitted event timestamps are stable across runs.
func baseObserveTime() time.Time { return time.Unix(1_800_000_000, 0) }

// newObserverService opens a service wired to an observer, so a test can assert
// on the telemetry the core emits (worker sessions, admission outcomes, settled
// events). The observer is advisory and never affects correctness.
func newObserverService(t *testing.T, observer observe.Observer) *llm.Service {
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{
		Path: filepath.Join(t.TempDir(), "observe.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "test-observe", Store: resource,
		Codecs: []llm.CodecRegistration{{
			Codec: testCodec{}, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		}},
		Clock: &stepClock{next: baseObserveTime()}, IDs: &sequenceIDs{},
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
		Admission:      llm.AdmitAll(),
		ToolAuthorizer: llm.ToolAuthorizerFunc(func(context.Context, llm.ToolAuthorization) error { return nil }),
		Observer:       observer,
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

func newTestService(t *testing.T, resource framework.Resource[llm.Store], policy llm.AdmissionPolicy) *llm.Service {
	return newTestServiceWithOptions(t, resource, policy, 0, &stepClock{next: time.Unix(1_800_000_000, 0)})
}

func newTestServiceWithOptions(
	t *testing.T,
	resource framework.Resource[llm.Store],
	policy llm.AdmissionPolicy,
	limit int64,
	clock llm.Clock,
) *llm.Service {
	if policy == nil {
		policy = llm.AdmitAll()
	}
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "test-deployment", Store: resource,
		Codecs: []llm.CodecRegistration{{
			Codec: testCodec{}, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		}},
		Clock: clock, IDs: &sequenceIDs{},
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
		Admission:      policy,
		ToolAuthorizer: llm.ToolAuthorizerFunc(func(context.Context, llm.ToolAuthorization) error { return nil }),
		ReadLimitBytes: limit,
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

func openTestWorker(t *testing.T, service *llm.Service, worker llm.WorkerID, session llm.WorkerSessionID) llm.WorkerConnection {
	connection, err := service.OpenWorker(t.Context(), llm.AuthenticatedWorker{WorkerID: worker, SessionID: session})
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func receiveServiceAssignment(t *testing.T, worker llm.WorkerConnection) llm.WorkerAssignmentDelivery {
	t.Helper()
	select {
	case assignment, ok := <-worker.Assignments():
		if !ok {
			t.Fatal("assignment channel closed")
		}
		return assignment
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for assignment")
		return llm.WorkerAssignmentDelivery{}
	}
}

func workerDelivery(assignment llm.WorkerAssignmentDelivery, id llm.WorkerDeliveryID, event llm.Event) llm.WorkerEventDelivery {
	return llm.WorkerEventDelivery{
		ID: id, Identity: assignment.Assignment.Identity,
		LeaseID: assignment.Assignment.Lease.ID, Event: event,
	}
}

func testRequest(stream bool, text string) llm.Request {
	return llm.Request{
		Model: "human", Stream: stream,
		Messages: []llm.Message{{Role: llm.RoleUser, Blocks: []llm.Block{{Type: llm.BlockText, Text: text}}}},
	}
}

func testRemoteTask() llm.TaskContext {
	return llm.TaskContext{
		WorkspaceKey: "workspace-a", CapabilityTier: llm.TierWorkspace,
		HarnessID: "harness-a", HarnessVersion: "v1", HarnessSessionID: "session-a",
		ExecAllowed: true,
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func shutdownRuntime(t *testing.T, runtime framework.Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown runtime: %v", err)
	}
}
