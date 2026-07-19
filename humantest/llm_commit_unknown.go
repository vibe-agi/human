package humantest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

// CommitUnknownLLMStore wraps an llm.Store and injects the most dangerous
// Store outcome a driver can produce: llm.ErrStoreCommitUnknown, the "this
// commit may or may not be durable" classification that forces the HumanLLM
// core to reconcile by durable identity instead of blindly retrying.
//
// Two one-shot failure modes model the two real ambiguities:
//
//   - ArmCommittedUnknown: the next Update commits through the wrapped Store
//     and then reports commit-unknown, as when a database applied the
//     transaction but the acknowledgement was lost.
//   - ArmLostUnknown: the next Update performs no write at all and reports
//     commit-unknown, as when the connection died before the commit reached
//     the database.
//
// The wrapper is safe for concurrent use and composes with any conforming
// Store, so third-party adapters can rehearse ambiguous commits against the
// real core via TestLLMServiceCommitUnknownReconciliation.
type CommitUnknownLLMStore struct {
	inner     llm.Store
	committed atomic.Bool
	lost      atomic.Bool
}

// WrapCommitUnknownLLMStore borrows inner; the caller keeps ownership and
// releases it after the wrapper is no longer used.
func WrapCommitUnknownLLMStore(inner llm.Store) *CommitUnknownLLMStore {
	return &CommitUnknownLLMStore{inner: inner}
}

// ArmCommittedUnknown makes exactly one subsequent Update commit durably and
// then report llm.ErrStoreCommitUnknown.
func (store *CommitUnknownLLMStore) ArmCommittedUnknown() { store.committed.Store(true) }

// ArmLostUnknown makes exactly one subsequent Update skip the inner commit
// entirely and report llm.ErrStoreCommitUnknown.
func (store *CommitUnknownLLMStore) ArmLostUnknown() { store.lost.Store(true) }

func (store *CommitUnknownLLMStore) Description() llm.StoreDescription {
	return store.inner.Description()
}

func (store *CommitUnknownLLMStore) Bind(ctx context.Context, binding llm.StoreBinding) error {
	return store.inner.Bind(ctx, binding)
}

func (store *CommitUnknownLLMStore) View(ctx context.Context, callback func(llm.StoreView) error) error {
	return store.inner.View(ctx, callback)
}

func (store *CommitUnknownLLMStore) Update(ctx context.Context, callback func(llm.StoreTx) error) error {
	if store.lost.Swap(false) {
		return &llm.StoreCommitUnknownError{Cause: errors.New("humantest: injected pre-commit connection loss")}
	}
	err := store.inner.Update(ctx, callback)
	if err == nil && store.committed.Swap(false) {
		return &llm.StoreCommitUnknownError{Cause: errors.New("humantest: injected lost commit acknowledgement")}
	}
	return err
}

var _ llm.Store = (*CommitUnknownLLMStore)(nil)

// TestLLMServiceCommitUnknownReconciliation drives a real HumanLLM Service
// over a factory-provided Store while injecting ambiguous commits, proving
// that the Store's durable identity, replay, and recovery reads support the
// core's reconciliation contract:
//
//   - an admission whose commit acknowledgement was lost reconciles to the
//     already-committed request, delivers exactly one assignment, and replays
//     the exact retry;
//   - an admission whose commit was genuinely lost fails without durable
//     effects, and the exact retry then succeeds with exactly one assignment;
//   - a worker event whose commit acknowledgement was lost reconciles to an
//     ACK receipt and completes the response exactly once.
//
// Passing complements TestLLMStore; it does not replace adapter-specific
// crash, durability, and infrastructure fault-injection tests.
func TestLLMServiceCommitUnknownReconciliation(t *testing.T, factory LLMStoreFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("HumanLLM Store conformance factory is nil")
	}

	t.Run("committed_unknown_admission_reconciles_to_exact_replay", func(t *testing.T) {
		service, wrapper, worker := openCommitUnknownService(t, factory, "committed-admission")
		wrapper.ArmCommittedUnknown()
		request := commitUnknownAdmission("turn-committed")
		result, err := service.Admit(t.Context(), request)
		if err != nil {
			t.Fatalf("commit-unknown admission did not reconcile: %v", err)
		}
		assignment := receiveCommitUnknownAssignment(t, worker)
		if assignment.Assignment.Identity != result.Identity {
			t.Fatalf("assignment identity = %+v, want %+v", assignment.Assignment.Identity, result.Identity)
		}
		assertNoCommitUnknownAssignment(t, worker)
		replay, err := service.Admit(t.Context(), request)
		if err != nil || !replay.Replay || replay.Identity != result.Identity {
			t.Fatalf("exact retry after reconciliation = %+v, %v", replay, err)
		}
		assertNoCommitUnknownAssignment(t, worker)
		settleCommitUnknownResponse(t, service, worker, assignment, request)
	})

	t.Run("lost_unknown_admission_fails_then_exact_retry_succeeds", func(t *testing.T) {
		service, wrapper, worker := openCommitUnknownService(t, factory, "lost-admission")
		wrapper.ArmLostUnknown()
		request := commitUnknownAdmission("turn-lost")
		if _, err := service.Admit(t.Context(), request); err == nil {
			t.Fatal("lost commit-unknown admission reported success without durable state")
		}
		assertNoCommitUnknownAssignment(t, worker)
		result, err := service.Admit(t.Context(), request)
		if err != nil {
			t.Fatalf("exact retry after lost commit: %v", err)
		}
		assignment := receiveCommitUnknownAssignment(t, worker)
		if assignment.Assignment.Identity != result.Identity {
			t.Fatalf("assignment identity = %+v, want %+v", assignment.Assignment.Identity, result.Identity)
		}
		assertNoCommitUnknownAssignment(t, worker)
		settleCommitUnknownResponse(t, service, worker, assignment, request)
	})

	t.Run("committed_unknown_worker_event_reconciles_receipt", func(t *testing.T) {
		service, wrapper, worker := openCommitUnknownService(t, factory, "committed-event")
		request := commitUnknownAdmission("turn-event")
		result, err := service.Admit(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		assignment := receiveCommitUnknownAssignment(t, worker)
		if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
			t.Fatal(err)
		}
		wrapper.ArmCommittedUnknown()
		final := llm.WorkerEventDelivery{
			ID: "delivery-final", Identity: assignment.Assignment.Identity,
			LeaseID: assignment.Assignment.Lease.ID,
			Event:   llm.Event{ID: "event-final", Type: llm.EventFinal, Text: "settled"},
		}
		receipt, err := worker.CommitEvent(t.Context(), final)
		if err != nil || receipt.Decision != llm.WorkerEventACK {
			t.Fatalf("commit-unknown worker event did not reconcile: %+v, %v", receipt, err)
		}
		replayed, err := worker.CommitEvent(t.Context(), final)
		if err != nil || replayed.Decision != llm.WorkerEventACK {
			t.Fatalf("worker event replay after reconciliation = %+v, %v", replayed, err)
		}
		page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
			CallerID: request.CallerID, IdempotencyKey: request.IdempotencyKey,
			RequestDigest: result.RequestDigest,
		})
		if err != nil || !page.Complete {
			t.Fatalf("response after event reconciliation = %+v, %v", page, err)
		}
	})
}

func openCommitUnknownService(
	t *testing.T,
	factory LLMStoreFactory,
	deployment string,
) (*llm.Service, *CommitUnknownLLMStore, llm.WorkerConnection) {
	t.Helper()
	inner, release, err := factory(t.Context(), t)
	if err != nil {
		t.Fatalf("open HumanLLM Store: %v", err)
	}
	if inner == nil || release == nil {
		t.Fatal("HumanLLM Store factory returned nil store or release")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := release(ctx); err != nil {
			t.Errorf("release HumanLLM Store: %v", err)
		}
	})
	wrapper := WrapCommitUnknownLLMStore(inner)
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "commit-unknown-" + deployment,
		Store:        framework.Borrow[llm.Store](wrapper),
		Codecs: []llm.CodecRegistration{{
			Codec: commitUnknownCodec{}, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		}},
		Admission: llm.AdmitAll(),
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
	})
	if err != nil {
		t.Fatalf("start HumanLLM Service: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := service.Shutdown(ctx); err != nil {
			t.Errorf("shutdown HumanLLM Service: %v", err)
		}
	})
	worker, err := service.OpenWorker(t.Context(), llm.AuthenticatedWorker{
		WorkerID: "worker-a", SessionID: "session-a",
	})
	if err != nil {
		t.Fatalf("open worker connection: %v", err)
	}
	return service, wrapper, worker
}

func commitUnknownAdmission(key llm.IdempotencyKey) llm.AdmissionRequest {
	body, _ := json.Marshal(llm.Request{
		Model: "human", Stream: true,
		Messages: []llm.Message{{
			Role: llm.RoleUser, Blocks: []llm.Block{{Type: llm.BlockText, Text: string(key)}},
		}},
	})
	return llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: key,
		CodecID: commitUnknownCodecID, Body: body,
	}
}

func receiveCommitUnknownAssignment(t *testing.T, worker llm.WorkerConnection) llm.WorkerAssignmentDelivery {
	t.Helper()
	select {
	case assignment, open := <-worker.Assignments():
		if !open {
			t.Fatal("assignment channel closed")
		}
		return assignment
	case <-time.After(5 * time.Second):
		t.Fatal("no assignment was delivered")
	}
	panic("unreachable")
}

func assertNoCommitUnknownAssignment(t *testing.T, worker llm.WorkerConnection) {
	t.Helper()
	select {
	case assignment, open := <-worker.Assignments():
		if open {
			t.Fatalf("unexpected duplicate assignment %+v", assignment)
		}
		t.Fatal("assignment channel closed")
	case <-time.After(150 * time.Millisecond):
	}
}

func settleCommitUnknownResponse(
	t *testing.T,
	service *llm.Service,
	worker llm.WorkerConnection,
	assignment llm.WorkerAssignmentDelivery,
	request llm.AdmissionRequest,
) {
	t.Helper()
	if err := worker.AckAssignment(t.Context(), assignment.ID); err != nil {
		t.Fatalf("ack assignment: %v", err)
	}
	receipt, err := worker.CommitEvent(t.Context(), llm.WorkerEventDelivery{
		ID: "delivery-final", Identity: assignment.Assignment.Identity,
		LeaseID: assignment.Assignment.Lease.ID,
		Event:   llm.Event{ID: "event-final", Type: llm.EventFinal, Text: "done"},
	})
	if err != nil || receipt.Decision != llm.WorkerEventACK {
		t.Fatalf("final worker event = %+v, %v", receipt, err)
	}
	result, err := service.Admit(t.Context(), request)
	if err != nil || !result.Replay || !result.Response.Complete {
		t.Fatalf("terminal exact replay = %+v, %v", result, err)
	}
}

const commitUnknownCodecID llm.CodecID = "humantest.commit-unknown"

type commitUnknownCodec struct{}

func (commitUnknownCodec) Description() llm.CodecDescription {
	return llm.CodecDescription{
		Contract: framework.Contract{ID: llm.CodecContractID, Major: llm.CodecContractMajor},
		ID:       commitUnknownCodecID, Version: "1",
		Fingerprint: llm.Fingerprint([]byte("humantest-commit-unknown-v1")),
		Limits: llm.CodecLimits{
			MaxRequestBytes: 1 << 20, MaxStreamFrameBytes: 1 << 20,
			MaxStreamFramesPerStep: 16, MaxAggregateBytes: 1 << 20,
			MaxAdmissionErrorBytes: 1 << 20,
		},
		OverloadedStatus: 503,
	}
}

func (commitUnknownCodec) Decode(body []byte) (llm.Request, error) {
	var request llm.Request
	err := json.Unmarshal(body, &request)
	return request, err
}

func (commitUnknownCodec) NewStream(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &commitUnknownEncoder{stream: true}, nil
}

func (commitUnknownCodec) NewAggregate(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &commitUnknownEncoder{}, nil
}

func (commitUnknownCodec) AdmissionError(failure llm.AdmissionFailure) ([]byte, error) {
	return json.Marshal(failure)
}

type commitUnknownEncoder struct {
	stream bool
	events []llm.Event
}

func (encoder *commitUnknownEncoder) Start() ([][]byte, error) {
	if encoder.stream {
		return [][]byte{[]byte("start\n")}, nil
	}
	return nil, nil
}

func (encoder *commitUnknownEncoder) Encode(event llm.Event, _ llm.EventSeed) ([][]byte, bool, error) {
	encoder.events = append(encoder.events, event)
	done := event.EndsResponse()
	if encoder.stream {
		return [][]byte{fmt.Appendf(nil, "%s:%s\n", event.Type, event.Text)}, done, nil
	}
	if !done {
		return nil, false, nil
	}
	body, err := json.Marshal(encoder.events)
	return [][]byte{body}, true, err
}
