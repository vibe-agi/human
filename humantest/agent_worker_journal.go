package humantest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/agent/workerws"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/workspace"
)

// AgentWorkerJournalFactory opens a new, empty Agent worker Journal for one
// conformance subtest. Every invocation must return an independent Journal.
// release must be non-nil and release every resource opened by the factory.
// The suite invokes it exactly once, including the close-behavior subtest.
//
// A provider normally exposes a test like:
//
//	func TestJournalConformance(t *testing.T) {
//		humantest.TestAgentWorkerJournal(t, func(ctx context.Context, t testing.TB) (
//			workerws.Journal, framework.ReleaseFunc, error,
//		) {
//			return openTestJournal(ctx, t.TempDir())
//		})
//	}
//
// The context bounds construction and the subtest and must not be retained.
// release receives a fresh context so cancellation of an operation cannot
// prevent deterministic cleanup.
type AgentWorkerJournalFactory func(
	context.Context,
	testing.TB,
) (workerws.Journal, framework.ReleaseFunc, error)

// TestAgentWorkerJournal runs the mandatory black-box conformance suite for
// durable Agent worker inbox/outbox journals. Passing it verifies the public
// contract, but does not replace provider-specific crash, durability, schema,
// permission, commit-ambiguity, and infrastructure tests.
func TestAgentWorkerJournal(t *testing.T, factory AgentWorkerJournalFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("Agent worker Journal conformance factory is nil")
	}

	tests := []struct {
		name string
		run  func(context.Context, *testing.T, workerws.Journal, framework.ReleaseFunc)
	}{
		{"description_and_contract", testAgentWorkerJournalDescription},
		{"binding_and_fail_closed_namespace", testAgentWorkerJournalBinding},
		{"assignment_exact_replay_and_tombstone", testAgentWorkerJournalAssignments},
		{"fifo_paging_and_byte_ownership", testAgentWorkerJournalFIFOAndBytes},
		{"event_ack_settlement", testAgentWorkerJournalACK},
		{"event_nack_atomic_rejection", testAgentWorkerJournalNACK},
		{"concurrent_sequence_allocation", testAgentWorkerJournalConcurrentSequences},
		{"validation_limits_and_context", testAgentWorkerJournalValidation},
		{"release_closes_journal", testAgentWorkerJournalClose},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			t.Cleanup(cancel)

			journal, release, err := factory(ctx, t)
			if err != nil {
				t.Fatalf("open fresh Agent worker Journal: %v", err)
			}
			if journal == nil {
				t.Fatal("factory returned a nil Agent worker Journal")
			}
			if release == nil {
				t.Fatal("factory returned a nil release function")
			}

			var releaseMu sync.Mutex
			released := false
			releaseOnce := func(releaseCtx context.Context) error {
				releaseMu.Lock()
				defer releaseMu.Unlock()
				if released {
					return errors.New("humantest: Agent worker Journal released more than once")
				}
				released = true
				return release(releaseCtx)
			}
			t.Cleanup(func() {
				releaseMu.Lock()
				alreadyReleased := released
				releaseMu.Unlock()
				if alreadyReleased {
					return
				}
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer releaseCancel()
				if err := releaseOnce(releaseCtx); err != nil {
					t.Errorf("release Agent worker Journal: %v", err)
				}
			})

			test.run(ctx, t, journal, releaseOnce)
		})
	}
}

// MemoryAgentWorkerJournal is one open handle onto a
// MemoryAgentWorkerJournalImage. It has no Close method because handle lifetime
// is represented by the framework.ReleaseFunc returned when it is opened.
type MemoryAgentWorkerJournal struct {
	memoryAgentWorkerJournalHandle
}

// The unexported embedding preserves concise access to the image state inside
// this semantic model without exposing a replaceable image pointer on a live
// handle.
type memoryAgentWorkerJournalHandle struct {
	*MemoryAgentWorkerJournalImage
	lifecycle *memoryAgentWorkerJournalLifecycle
}

type memoryAgentWorkerJournalLifecycle struct{ closed bool }

// MemoryAgentWorkerJournalImage is a concurrency-safe semantic durable image
// for tests. It deliberately follows the same binding, FIFO, exact-replay,
// compaction, and byte-ownership rules as durable providers. Closing a handle
// leaves the image intact, so recovery tests can open a fresh handle after
// simulated loss of process-local handle state. This does not model physical
// process or storage durability.
//
// At most one handle may be open at a time. The image is a test model, not a
// production persistence adapter.
type MemoryAgentWorkerJournalImage struct {
	mu          sync.Mutex
	owner       *memoryAgentWorkerJournalLifecycle
	binding     *workerws.JournalBinding
	next        workerws.JournalSequence
	assignments map[agent.WorkerDeliveryID]*memoryJournalAssignment
	events      map[agent.WorkerDeliveryID]*memoryJournalEvent
	rejections  map[agent.WorkerDeliveryID]*memoryJournalRejection
}

type memoryJournalAssignment struct {
	digest   workerws.JournalDigest
	sequence workerws.JournalSequence
	pending  *agent.WorkerAssignmentDelivery
}

type memoryJournalEvent struct {
	digest        workerws.JournalDigest
	sequence      workerws.JournalSequence
	pending       *agent.WorkerEventDelivery
	receiptDigest workerws.JournalDigest
}

type memoryJournalRejection struct {
	sequence      workerws.JournalSequence
	eventDigest   workerws.JournalDigest
	receiptDigest workerws.JournalDigest
	pending       *workerws.RejectedEvent
}

// NewMemoryAgentWorkerJournalImage creates an independent, initially unbound
// durable image with no open handle.
func NewMemoryAgentWorkerJournalImage() *MemoryAgentWorkerJournalImage {
	return &MemoryAgentWorkerJournalImage{
		assignments: make(map[agent.WorkerDeliveryID]*memoryJournalAssignment),
		events:      make(map[agent.WorkerDeliveryID]*memoryJournalEvent),
		rejections:  make(map[agent.WorkerDeliveryID]*memoryJournalRejection),
	}
}

// Open returns the sole live handle onto image. Successful Journal mutations
// update image before they return; releasing the handle only relinquishes
// ownership and never flushes otherwise-volatile state.
func (image *MemoryAgentWorkerJournalImage) Open(
	ctx context.Context,
) (*MemoryAgentWorkerJournal, framework.ReleaseFunc, error) {
	if ctx == nil {
		return nil, nil, errors.New("open memory Agent worker Journal: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if image == nil {
		return nil, nil, errors.New("open memory Agent worker Journal: image is required")
	}
	image.mu.Lock()
	defer image.mu.Unlock()
	if image.owner != nil {
		return nil, nil, errors.New("open memory Agent worker Journal: image already has an open handle")
	}
	lifecycle := &memoryAgentWorkerJournalLifecycle{}
	journal := &MemoryAgentWorkerJournal{
		memoryAgentWorkerJournalHandle: memoryAgentWorkerJournalHandle{
			MemoryAgentWorkerJournalImage: image,
			lifecycle:                     lifecycle,
		},
	}
	image.owner = lifecycle
	var once sync.Once
	release := func(context.Context) error {
		once.Do(func() {
			image.mu.Lock()
			lifecycle.closed = true
			if image.owner == lifecycle {
				image.owner = nil
			}
			image.mu.Unlock()
		})
		return nil
	}
	return journal, release, nil
}

// Abandon invalidates journal without invoking its Release function. It is a
// deterministic crash semantic for tests, not physical durability evidence.
// A late Release for this generation cannot close a newer image owner.
func (image *MemoryAgentWorkerJournalImage) Abandon(journal *MemoryAgentWorkerJournal) error {
	if image == nil || journal == nil ||
		journal.MemoryAgentWorkerJournalImage != image || journal.lifecycle == nil {
		return workerws.ErrJournalClosed
	}
	image.mu.Lock()
	defer image.mu.Unlock()
	if journal.lifecycle.closed || image.owner != journal.lifecycle {
		return workerws.ErrJournalClosed
	}
	journal.lifecycle.closed = true
	image.owner = nil
	return nil
}

// NewMemoryAgentWorkerJournal creates an independent image, opens its first
// handle, and returns that handle with an idempotent release function.
func NewMemoryAgentWorkerJournal() (*MemoryAgentWorkerJournal, framework.ReleaseFunc) {
	journal, release, err := NewMemoryAgentWorkerJournalImage().Open(context.Background())
	if err != nil {
		panic(fmt.Sprintf("open fresh memory Agent worker Journal: %v", err))
	}
	return journal, release
}

func (*MemoryAgentWorkerJournal) Description() workerws.JournalDescription {
	return workerws.JournalDescription{
		Contract: framework.Contract{ID: workerws.JournalContractID, Major: workerws.JournalContractMajor},
		Provider: "humantest-memory", Version: "1",
	}
}

func (journal *MemoryAgentWorkerJournal) Bind(ctx context.Context, binding workerws.JournalBinding) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.contextAndOpen(ctx); err != nil {
		return err
	}
	if err := binding.Validate(); err != nil {
		return errors.Join(workerws.ErrJournalCorrupt, err)
	}
	if journal.binding == nil {
		copy := binding
		journal.binding = &copy
		return nil
	}
	if *journal.binding != binding {
		return workerws.ErrJournalConflict
	}
	return nil
}

func (journal *MemoryAgentWorkerJournal) PutAssignment(
	ctx context.Context,
	record workerws.JournalAssignment,
) (workerws.JournalEntryState, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return "", err
	}
	if err := journal.validateAssignment(record); err != nil {
		return "", err
	}
	id := record.Delivery.ID
	if current := journal.assignments[id]; current != nil {
		if current.digest != record.Digest {
			return "", workerws.ErrJournalConflict
		}
		if current.pending == nil {
			return workerws.JournalEntrySettled, nil
		}
		return workerws.JournalEntryPending, nil
	}
	sequence, err := journal.allocateSequence()
	if err != nil {
		return "", err
	}
	delivery := agent.CloneWorkerAssignmentDelivery(record.Delivery)
	journal.assignments[id] = &memoryJournalAssignment{
		digest: record.Digest, sequence: sequence, pending: &delivery,
	}
	return workerws.JournalEntryPending, nil
}

func (journal *MemoryAgentWorkerJournal) ConfirmAssignment(ctx context.Context, id agent.WorkerDeliveryID) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return err
	}
	if !validJournalDeliveryID(id) {
		return workerws.ErrJournalCorrupt
	}
	current := journal.assignments[id]
	if current == nil {
		return workerws.ErrJournalNotFound
	}
	current.pending = nil
	return nil
}

func (journal *MemoryAgentWorkerJournal) ListAssignments(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalAssignment, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateMemoryJournalLimit(limit); err != nil {
		return nil, err
	}
	result := make([]workerws.JournalAssignment, 0, min(limit, len(journal.assignments)))
	for _, current := range journal.assignments {
		if current.pending == nil || current.sequence <= after {
			continue
		}
		result = append(result, workerws.JournalAssignment{
			Sequence: current.sequence, Digest: current.digest,
			Delivery: agent.CloneWorkerAssignmentDelivery(*current.pending),
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Sequence < result[right].Sequence })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (journal *MemoryAgentWorkerJournal) PutEvent(
	ctx context.Context,
	record workerws.JournalEvent,
) (workerws.JournalEntryState, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return "", err
	}
	if err := journal.validateEvent(record); err != nil {
		return "", err
	}
	id := record.Delivery.ID
	if current := journal.events[id]; current != nil {
		if current.digest != record.Digest {
			return "", workerws.ErrJournalConflict
		}
		if current.pending == nil {
			return workerws.JournalEntrySettled, nil
		}
		return workerws.JournalEntryPending, nil
	}
	sequence, err := journal.allocateSequence()
	if err != nil {
		return "", err
	}
	delivery := agent.CloneWorkerEventDelivery(record.Delivery)
	journal.events[id] = &memoryJournalEvent{
		digest: record.Digest, sequence: sequence, pending: &delivery,
	}
	return workerws.JournalEntryPending, nil
}

func (journal *MemoryAgentWorkerJournal) ListEvents(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalEvent, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateMemoryJournalLimit(limit); err != nil {
		return nil, err
	}
	result := make([]workerws.JournalEvent, 0, min(limit, len(journal.events)))
	for _, current := range journal.events {
		if current.pending == nil || current.sequence <= after {
			continue
		}
		result = append(result, workerws.JournalEvent{
			Sequence: current.sequence, Digest: current.digest,
			Delivery: agent.CloneWorkerEventDelivery(*current.pending),
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Sequence < result[right].Sequence })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (journal *MemoryAgentWorkerJournal) SettleEvent(
	ctx context.Context,
	receipt agent.WorkerEventReceipt,
	eventDigest workerws.JournalDigest,
	receiptDigest workerws.JournalDigest,
) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return err
	}
	if err := eventDigest.Validate(); err != nil {
		return err
	}
	if err := receiptDigest.Validate(); err != nil {
		return err
	}
	current := journal.events[receipt.Delivery]
	if current == nil {
		return workerws.ErrJournalNotFound
	}
	if current.digest != eventDigest {
		return workerws.ErrJournalConflict
	}
	if current.pending == nil {
		if current.receiptDigest != receiptDigest {
			return workerws.ErrJournalConflict
		}
		return nil
	}
	if err := receipt.ValidateFor(*current.pending); err != nil {
		return errors.Join(workerws.ErrJournalCorrupt, err)
	}
	delivery := agent.CloneWorkerEventDelivery(*current.pending)
	current.pending = nil
	current.receiptDigest = receiptDigest
	if receipt.Decision == agent.WorkerEventNACK {
		sequence, sequenceErr := journal.allocateSequence()
		if sequenceErr != nil {
			// Allocation is checked before any practically reachable overflow. Keep
			// the semantic model atomic even at that artificial boundary.
			current.pending = &delivery
			current.receiptDigest = ""
			return sequenceErr
		}
		rejected := workerws.RejectedEvent{Delivery: delivery, Receipt: receipt}
		journal.rejections[receipt.Delivery] = &memoryJournalRejection{
			sequence: sequence, eventDigest: eventDigest, receiptDigest: receiptDigest, pending: &rejected,
		}
	}
	return nil
}

func (journal *MemoryAgentWorkerJournal) ListRejections(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalRejection, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateMemoryJournalLimit(limit); err != nil {
		return nil, err
	}
	result := make([]workerws.JournalRejection, 0, min(limit, len(journal.rejections)))
	for _, current := range journal.rejections {
		if current.pending == nil || current.sequence <= after {
			continue
		}
		result = append(result, workerws.JournalRejection{
			Sequence: current.sequence, EventDigest: current.eventDigest, ReceiptDigest: current.receiptDigest,
			RejectedEvent: workerws.RejectedEvent{
				Delivery: agent.CloneWorkerEventDelivery(current.pending.Delivery),
				Receipt:  current.pending.Receipt,
			},
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Sequence < result[right].Sequence })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (journal *MemoryAgentWorkerJournal) ConfirmRejection(ctx context.Context, id agent.WorkerDeliveryID) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return err
	}
	if !validJournalDeliveryID(id) {
		return workerws.ErrJournalCorrupt
	}
	current := journal.rejections[id]
	if current == nil {
		return workerws.ErrJournalNotFound
	}
	current.pending = nil
	return nil
}

func (journal *MemoryAgentWorkerJournal) contextAndOpen(ctx context.Context) error {
	if ctx == nil {
		return errors.Join(workerws.ErrJournalCorrupt, errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if journal.lifecycle == nil || journal.lifecycle.closed {
		return workerws.ErrJournalClosed
	}
	return nil
}

func (journal *MemoryAgentWorkerJournal) ready(ctx context.Context) error {
	if err := journal.contextAndOpen(ctx); err != nil {
		return err
	}
	if journal.binding == nil {
		return workerws.ErrJournalCorrupt
	}
	return nil
}

func (journal *MemoryAgentWorkerJournal) principal() agent.AuthenticatedWorker {
	return agent.AuthenticatedWorker{
		Authority: journal.binding.Authority,
		Worker:    journal.binding.Worker,
		Session:   "humantest-journal-validation",
	}
}

func (journal *MemoryAgentWorkerJournal) validateAssignment(record workerws.JournalAssignment) error {
	if record.Sequence != 0 {
		return errors.Join(workerws.ErrJournalCorrupt, errors.New("assignment sequence must be allocated by Journal"))
	}
	if err := record.Digest.Validate(); err != nil {
		return err
	}
	if err := record.Delivery.ValidateFor(journal.principal()); err != nil {
		return errors.Join(workerws.ErrJournalCorrupt, err)
	}
	return nil
}

func (journal *MemoryAgentWorkerJournal) validateEvent(record workerws.JournalEvent) error {
	if record.Sequence != 0 {
		return errors.Join(workerws.ErrJournalCorrupt, errors.New("event sequence must be allocated by Journal"))
	}
	if err := record.Digest.Validate(); err != nil {
		return err
	}
	if err := record.Delivery.ValidateFor(journal.principal()); err != nil {
		return errors.Join(workerws.ErrJournalCorrupt, err)
	}
	return nil
}

func (journal *MemoryAgentWorkerJournal) allocateSequence() (workerws.JournalSequence, error) {
	if journal.next == workerws.JournalSequence(^uint64(0)) {
		return 0, workerws.ErrJournalLimit
	}
	journal.next++
	if journal.next == 0 {
		return 0, workerws.ErrJournalLimit
	}
	return journal.next, nil
}

func validateMemoryJournalLimit(limit int) error {
	if limit < 1 || limit > workerws.MaxJournalPageSize {
		return fmt.Errorf("%w: page limit must be 1..%d", workerws.ErrJournalLimit, workerws.MaxJournalPageSize)
	}
	return nil
}

func validJournalDeliveryID(id agent.WorkerDeliveryID) bool {
	value := string(id)
	if value == "" || len(value) > 128 || !utf8.ValidString(value) || value != strings.TrimSpace(value) {
		return false
	}
	for index, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || (index > 0 && strings.ContainsRune("._:-", character)) {
			continue
		}
		return false
	}
	return true
}

func memoryJournalDigest(value any) (workerws.JournalDigest, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return workerws.JournalDigest(hex.EncodeToString(digest[:])), nil
}

func testAgentWorkerJournalDescription(
	_ context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	t.Helper()
	first := journal.Description()
	if err := first.Validate(); err != nil {
		t.Fatalf("Description does not satisfy Agent worker Journal contract: %v", err)
	}
	second := journal.Description()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Description is not static:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

func testAgentWorkerJournalBinding(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	t.Helper()
	assignment := conformanceJournalAssignment("prebind-assignment")
	event := conformanceJournalEvent("prebind-event", agent.WorkerEventAcceptTask, nil)
	ack := conformanceJournalReceipt(event.Delivery, agent.WorkerEventACK)
	eventDigest := mustJournalDigest(t, event.Delivery)
	receiptDigest := mustJournalDigest(t, ack)
	prebind := []struct {
		name string
		call func() error
	}{
		{"put assignment", func() error { _, err := journal.PutAssignment(ctx, assignment); return err }},
		{"confirm assignment", func() error { return journal.ConfirmAssignment(ctx, assignment.Delivery.ID) }},
		{"list assignments", func() error { _, err := journal.ListAssignments(ctx, 0, 1); return err }},
		{"put event", func() error { _, err := journal.PutEvent(ctx, event); return err }},
		{"list events", func() error { _, err := journal.ListEvents(ctx, 0, 1); return err }},
		{"settle event", func() error { return journal.SettleEvent(ctx, ack, eventDigest, receiptDigest) }},
		{"list rejections", func() error { _, err := journal.ListRejections(ctx, 0, 1); return err }},
		{"confirm rejection", func() error { return journal.ConfirmRejection(ctx, event.Delivery.ID) }},
	}
	for _, operation := range prebind {
		if err := operation.call(); !errors.Is(err, workerws.ErrJournalCorrupt) {
			t.Fatalf("%s before Bind error = %v, want ErrJournalCorrupt", operation.name, err)
		}
	}

	invalid := conformanceJournalBinding()
	invalid.Gateway = ""
	if err := journal.Bind(ctx, invalid); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("invalid Bind error = %v, want ErrJournalCorrupt", err)
	}
	binding := conformanceJournalBinding()
	if err := journal.Bind(ctx, binding); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := journal.Bind(ctx, binding); err != nil {
		t.Fatalf("exact Bind replay: %v", err)
	}
	variants := []workerws.JournalBinding{
		{Gateway: "gateway-b", Authority: binding.Authority, Worker: binding.Worker},
		{Gateway: binding.Gateway, Authority: "authority-b", Worker: binding.Worker},
		{Gateway: binding.Gateway, Authority: binding.Authority, Worker: "worker-b"},
	}
	for _, variant := range variants {
		if err := journal.Bind(ctx, variant); !errors.Is(err, workerws.ErrJournalConflict) {
			t.Fatalf("divergent Bind(%#v) error = %v, want ErrJournalConflict", variant, err)
		}
	}
	// A conflicting Bind must not mutate the first correctness namespace.
	if err := journal.Bind(ctx, binding); err != nil {
		t.Fatalf("original Bind after conflict: %v", err)
	}
}

func testAgentWorkerJournalAssignments(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindConformanceJournal(ctx, t, journal)
	record := conformanceJournalAssignment("assignment-exact")
	if state, err := journal.PutAssignment(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutAssignment new = (%q, %v), want pending", state, err)
	}
	if state, err := journal.PutAssignment(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutAssignment exact pending replay = (%q, %v), want pending", state, err)
	}
	listed, err := journal.ListAssignments(ctx, 0, 10)
	if err != nil || len(listed) != 1 || listed[0].Sequence == 0 || listed[0].Digest != record.Digest ||
		!reflect.DeepEqual(listed[0].Delivery, record.Delivery) {
		t.Fatalf("ListAssignments after exact replay = %#v, %v", listed, err)
	}

	divergent := record
	divergent.Delivery.Assignment.Task.Revision++
	divergent.Delivery.Assignment.Task.EventCount++
	divergent.Digest = mustJournalDigest(t, divergent.Delivery)
	if _, err := journal.PutAssignment(ctx, divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("PutAssignment divergent replay error = %v, want ErrJournalConflict", err)
	}
	if err := journal.ConfirmAssignment(ctx, record.Delivery.ID); err != nil {
		t.Fatalf("ConfirmAssignment: %v", err)
	}
	if err := journal.ConfirmAssignment(ctx, record.Delivery.ID); err != nil {
		t.Fatalf("idempotent ConfirmAssignment: %v", err)
	}
	listed, err = journal.ListAssignments(ctx, 0, 10)
	if err != nil || len(listed) != 0 {
		t.Fatalf("settled assignment remained visible: %#v, %v", listed, err)
	}
	if state, err := journal.PutAssignment(ctx, record); err != nil || state != workerws.JournalEntrySettled {
		t.Fatalf("PutAssignment exact tombstone replay = (%q, %v), want settled", state, err)
	}
	if _, err := journal.PutAssignment(ctx, divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("PutAssignment divergent tombstone replay error = %v, want ErrJournalConflict", err)
	}
	if err := journal.ConfirmAssignment(ctx, "missing-assignment"); !errors.Is(err, workerws.ErrJournalNotFound) {
		t.Fatalf("ConfirmAssignment missing error = %v, want ErrJournalNotFound", err)
	}
}

func testAgentWorkerJournalFIFOAndBytes(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindConformanceJournal(ctx, t, journal)
	for index := 0; index < 5; index++ {
		record := conformanceJournalAssignment(agent.WorkerDeliveryID(fmt.Sprintf("assignment-page-%d", index)))
		if _, err := journal.PutAssignment(ctx, record); err != nil {
			t.Fatalf("PutAssignment %d: %v", index, err)
		}
	}
	first, err := journal.ListAssignments(ctx, 0, 2)
	if err != nil || len(first) != 2 {
		t.Fatalf("first assignment page = %#v, %v", first, err)
	}
	second, err := journal.ListAssignments(ctx, first[len(first)-1].Sequence, 2)
	if err != nil || len(second) != 2 {
		t.Fatalf("second assignment page = %#v, %v", second, err)
	}
	third, err := journal.ListAssignments(ctx, second[len(second)-1].Sequence, 2)
	if err != nil || len(third) != 1 {
		t.Fatalf("third assignment page = %#v, %v", third, err)
	}
	allAssignments := append(append(first, second...), third...)
	assertStrictJournalSequences(t, assignmentSequences(allAssignments))
	if trailing, err := journal.ListAssignments(ctx, third[0].Sequence, 2); err != nil || len(trailing) != 0 {
		t.Fatalf("assignment page after tail = %#v, %v", trailing, err)
	}

	payload := []byte("owned payload")
	event := conformanceJournalEvent("event-owned-bytes", agent.WorkerEventFreezeArtifact, payload)
	want := agent.CloneWorkerEventDelivery(event.Delivery)
	if _, err := journal.PutEvent(ctx, event); err != nil {
		t.Fatalf("PutEvent with bytes: %v", err)
	}
	payload[0] = 'X'
	event.Delivery.Event.Freeze.Payload.Data[1] = 'Y'
	listed, err := journal.ListEvents(ctx, 0, 10)
	if err != nil || len(listed) != 1 || !reflect.DeepEqual(listed[0].Delivery, want) {
		t.Fatalf("Journal retained caller-owned bytes: got %#v, want %#v, err %v", listed, want, err)
	}
	listed[0].Delivery.Event.Freeze.Payload.Data[0] = 'Z'
	again, err := journal.ListEvents(ctx, 0, 10)
	if err != nil || len(again) != 1 || !reflect.DeepEqual(again[0].Delivery, want) {
		t.Fatalf("ListEvents returned aliased bytes: got %#v, want %#v, err %v", again, want, err)
	}
}

func testAgentWorkerJournalACK(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindConformanceJournal(ctx, t, journal)
	record := conformanceJournalEvent("event-ack", agent.WorkerEventAcceptTask, nil)
	if state, err := journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutEvent new = (%q, %v), want pending", state, err)
	}
	if state, err := journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutEvent exact pending replay = (%q, %v), want pending", state, err)
	}
	receipt := conformanceJournalReceipt(record.Delivery, agent.WorkerEventACK)
	receiptDigest := mustJournalDigest(t, receipt)
	if err := journal.SettleEvent(ctx, receipt, record.Digest, receiptDigest); err != nil {
		t.Fatalf("SettleEvent ACK: %v", err)
	}
	if err := journal.SettleEvent(ctx, receipt, record.Digest, receiptDigest); err != nil {
		t.Fatalf("exact repeated ACK settlement: %v", err)
	}
	if events, err := journal.ListEvents(ctx, 0, 10); err != nil || len(events) != 0 {
		t.Fatalf("ACK-settled event remained visible: %#v, %v", events, err)
	}
	if rejections, err := journal.ListRejections(ctx, 0, 10); err != nil || len(rejections) != 0 {
		t.Fatalf("ACK created a rejection: %#v, %v", rejections, err)
	}
	if state, err := journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntrySettled {
		t.Fatalf("PutEvent exact ACK tombstone replay = (%q, %v), want settled", state, err)
	}
	if err := journal.SettleEvent(ctx, receipt, differentJournalDigest(record.Digest), receiptDigest); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("SettleEvent divergent event digest error = %v, want ErrJournalConflict", err)
	}
	divergentReceipt := conformanceJournalReceipt(record.Delivery, agent.WorkerEventNACK)
	divergentReceiptDigest := mustJournalDigest(t, divergentReceipt)
	if err := journal.SettleEvent(ctx, divergentReceipt, record.Digest, divergentReceiptDigest); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("SettleEvent divergent receipt error = %v, want ErrJournalConflict", err)
	}
}

func testAgentWorkerJournalNACK(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindConformanceJournal(ctx, t, journal)
	record := conformanceJournalEvent("event-nack", agent.WorkerEventFreezeArtifact, []byte("rejected payload"))
	if _, err := journal.PutEvent(ctx, record); err != nil {
		t.Fatalf("PutEvent before NACK: %v", err)
	}
	receipt := conformanceJournalReceipt(record.Delivery, agent.WorkerEventNACK)
	receiptDigest := mustJournalDigest(t, receipt)
	if err := journal.SettleEvent(ctx, receipt, record.Digest, receiptDigest); err != nil {
		t.Fatalf("SettleEvent NACK: %v", err)
	}
	if events, err := journal.ListEvents(ctx, 0, 10); err != nil || len(events) != 0 {
		t.Fatalf("NACK did not atomically remove event: %#v, %v", events, err)
	}
	rejections, err := journal.ListRejections(ctx, 0, 10)
	if err != nil || len(rejections) != 1 {
		t.Fatalf("NACK did not atomically create rejection: %#v, %v", rejections, err)
	}
	got := rejections[0]
	if got.Sequence == 0 || got.EventDigest != record.Digest || got.ReceiptDigest != receiptDigest ||
		!reflect.DeepEqual(got.Delivery, record.Delivery) || got.Receipt != receipt {
		t.Fatalf("rejection = %#v, want event %#v and receipt %#v", got, record, receipt)
	}
	got.Delivery.Event.Freeze.Payload.Data[0] = 'X'
	again, err := journal.ListRejections(ctx, 0, 10)
	if err != nil || len(again) != 1 || !reflect.DeepEqual(again[0].Delivery, record.Delivery) {
		t.Fatalf("ListRejections returned aliased bytes: %#v, %v", again, err)
	}
	if err := journal.ConfirmRejection(ctx, record.Delivery.ID); err != nil {
		t.Fatalf("ConfirmRejection: %v", err)
	}
	if err := journal.ConfirmRejection(ctx, record.Delivery.ID); err != nil {
		t.Fatalf("idempotent ConfirmRejection: %v", err)
	}
	if rejections, err := journal.ListRejections(ctx, 0, 10); err != nil || len(rejections) != 0 {
		t.Fatalf("confirmed rejection remained visible: %#v, %v", rejections, err)
	}
	if err := journal.SettleEvent(ctx, receipt, record.Digest, receiptDigest); err != nil {
		t.Fatalf("exact settlement after rejection compaction: %v", err)
	}
	if rejections, err := journal.ListRejections(ctx, 0, 10); err != nil || len(rejections) != 0 {
		t.Fatalf("exact settlement recreated compacted rejection: %#v, %v", rejections, err)
	}
	if state, err := journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntrySettled {
		t.Fatalf("PutEvent exact NACK tombstone replay = (%q, %v), want settled", state, err)
	}
	if err := journal.ConfirmRejection(ctx, "missing-rejection"); !errors.Is(err, workerws.ErrJournalNotFound) {
		t.Fatalf("ConfirmRejection missing error = %v, want ErrJournalNotFound", err)
	}
}

func testAgentWorkerJournalConcurrentSequences(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindConformanceJournal(ctx, t, journal)
	const count = 48
	start := make(chan struct{})
	errorsByIndex := make(chan error, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			if index%2 == 0 {
				record := conformanceJournalAssignment(agent.WorkerDeliveryID(fmt.Sprintf("concurrent-assignment-%03d", index)))
				_, err := journal.PutAssignment(ctx, record)
				errorsByIndex <- err
				return
			}
			record := conformanceJournalEvent(agent.WorkerDeliveryID(fmt.Sprintf("concurrent-event-%03d", index)), agent.WorkerEventAcceptTask, nil)
			_, err := journal.PutEvent(ctx, record)
			errorsByIndex <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsByIndex)
	for err := range errorsByIndex {
		if err != nil {
			t.Fatalf("concurrent Put: %v", err)
		}
	}
	assignments, err := journal.ListAssignments(ctx, 0, workerws.MaxJournalPageSize)
	if err != nil {
		t.Fatalf("ListAssignments after concurrent Put: %v", err)
	}
	events, err := journal.ListEvents(ctx, 0, workerws.MaxJournalPageSize)
	if err != nil {
		t.Fatalf("ListEvents after concurrent Put: %v", err)
	}
	if len(assignments)+len(events) != count {
		t.Fatalf("concurrent Put count = %d, want %d", len(assignments)+len(events), count)
	}
	assertStrictJournalSequences(t, assignmentSequences(assignments))
	assertStrictJournalSequences(t, eventSequences(events))
}

func testAgentWorkerJournalValidation(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := journal.Bind(cancelled, conformanceJournalBinding()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Bind cancelled context error = %v, want context.Canceled", err)
	}
	bindConformanceJournal(ctx, t, journal)

	assignment := conformanceJournalAssignment("validation-assignment")
	withSequence := assignment
	withSequence.Sequence = 7
	if _, err := journal.PutAssignment(ctx, withSequence); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("PutAssignment caller sequence error = %v, want ErrJournalCorrupt", err)
	}
	badDigest := assignment
	badDigest.Digest = "not-a-digest"
	if _, err := journal.PutAssignment(ctx, badDigest); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("PutAssignment invalid digest error = %v, want ErrJournalCorrupt", err)
	}
	invalidAssignment := assignment
	invalidAssignment.Delivery.ID = ""
	invalidAssignment.Digest = mustJournalDigest(t, invalidAssignment.Delivery)
	if _, err := journal.PutAssignment(ctx, invalidAssignment); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("PutAssignment invalid delivery error = %v, want ErrJournalCorrupt", err)
	}

	event := conformanceJournalEvent("validation-event", agent.WorkerEventAcceptTask, nil)
	withEventSequence := event
	withEventSequence.Sequence = 9
	if _, err := journal.PutEvent(ctx, withEventSequence); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("PutEvent caller sequence error = %v, want ErrJournalCorrupt", err)
	}
	if _, err := journal.PutEvent(cancelled, event); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutEvent cancelled context error = %v, want context.Canceled", err)
	}
	for _, limit := range []int{0, -1, workerws.MaxJournalPageSize + 1} {
		if _, err := journal.ListAssignments(ctx, 0, limit); !errors.Is(err, workerws.ErrJournalLimit) {
			t.Fatalf("ListAssignments limit %d error = %v, want ErrJournalLimit", limit, err)
		}
		if _, err := journal.ListEvents(ctx, 0, limit); !errors.Is(err, workerws.ErrJournalLimit) {
			t.Fatalf("ListEvents limit %d error = %v, want ErrJournalLimit", limit, err)
		}
		if _, err := journal.ListRejections(ctx, 0, limit); !errors.Is(err, workerws.ErrJournalLimit) {
			t.Fatalf("ListRejections limit %d error = %v, want ErrJournalLimit", limit, err)
		}
	}
}

func testAgentWorkerJournalClose(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	release framework.ReleaseFunc,
) {
	bindConformanceJournal(ctx, t, journal)
	description := journal.Description()
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := release(releaseCtx); err != nil {
		t.Fatalf("release Agent worker Journal: %v", err)
	}
	if !reflect.DeepEqual(journal.Description(), description) {
		t.Fatal("Description changed after release")
	}
	assignment := conformanceJournalAssignment("closed-assignment")
	event := conformanceJournalEvent("closed-event", agent.WorkerEventAcceptTask, nil)
	receipt := conformanceJournalReceipt(event.Delivery, agent.WorkerEventACK)
	eventDigest := mustJournalDigest(t, event.Delivery)
	receiptDigest := mustJournalDigest(t, receipt)
	closed := []struct {
		name string
		call func() error
	}{
		{"bind", func() error { return journal.Bind(ctx, conformanceJournalBinding()) }},
		{"put assignment", func() error { _, err := journal.PutAssignment(ctx, assignment); return err }},
		{"confirm assignment", func() error { return journal.ConfirmAssignment(ctx, assignment.Delivery.ID) }},
		{"list assignments", func() error { _, err := journal.ListAssignments(ctx, 0, 1); return err }},
		{"put event", func() error { _, err := journal.PutEvent(ctx, event); return err }},
		{"list events", func() error { _, err := journal.ListEvents(ctx, 0, 1); return err }},
		{"settle event", func() error { return journal.SettleEvent(ctx, receipt, eventDigest, receiptDigest) }},
		{"list rejections", func() error { _, err := journal.ListRejections(ctx, 0, 1); return err }},
		{"confirm rejection", func() error { return journal.ConfirmRejection(ctx, event.Delivery.ID) }},
	}
	for _, operation := range closed {
		if err := operation.call(); !errors.Is(err, workerws.ErrJournalClosed) {
			t.Fatalf("%s after release error = %v, want ErrJournalClosed", operation.name, err)
		}
	}
}

func conformanceJournalBinding() workerws.JournalBinding {
	return workerws.JournalBinding{Gateway: "gateway-a", Authority: "authority-a", Worker: "worker-a"}
}

func bindConformanceJournal(ctx context.Context, t *testing.T, journal workerws.Journal) {
	t.Helper()
	if err := journal.Bind(ctx, conformanceJournalBinding()); err != nil {
		t.Fatalf("Bind Agent worker Journal: %v", err)
	}
}

func conformanceJournalAssignment(id agent.WorkerDeliveryID) workerws.JournalAssignment {
	now := time.Date(2026, 7, 19, 6, 7, 8, 9, time.UTC)
	ref := conformanceJournalTaskRef()
	delivery := agent.WorkerAssignmentDelivery{
		ID: id,
		Assignment: agent.LeaseAssignment{
			Grant: agent.LeaseGrant{Task: ref, Worker: "worker-a", Fence: 1},
			Task: agent.Task{
				Ref: ref, Context: agent.ContextRef{Authority: "authority-a", ID: "context-a"},
				State: agent.TaskSubmitted, Revision: 1, MessageCount: 1, EventCount: 1,
				CreatedAt: now, UpdatedAt: now,
			},
			GrantedAt: now.Add(time.Nanosecond),
		},
	}
	digest, err := memoryJournalDigest(delivery)
	if err != nil {
		panic(err)
	}
	return workerws.JournalAssignment{Digest: digest, Delivery: delivery}
}

func conformanceJournalEvent(
	id agent.WorkerDeliveryID,
	kind agent.WorkerEventKind,
	payload []byte,
) workerws.JournalEvent {
	event := agent.WorkerEvent{
		ID: agent.CommandID("command-" + id), Kind: kind,
		Task: conformanceJournalTaskRef(), Fence: 1, ExpectedRevision: 1,
	}
	if kind == agent.WorkerEventFreezeArtifact {
		event.Freeze = &agent.WorkerArtifactFreeze{
			Artifact: "artifact-a", ExpectedBaseRevision: workspace.Revision("revision-a"),
			Payload: workspace.Payload{MediaType: "application/octet-stream", Data: payload},
		}
	}
	delivery := agent.WorkerEventDelivery{ID: id, Event: event}
	digest, err := memoryJournalDigest(delivery)
	if err != nil {
		panic(err)
	}
	return workerws.JournalEvent{Digest: digest, Delivery: delivery}
}

func conformanceJournalReceipt(
	delivery agent.WorkerEventDelivery,
	decision agent.WorkerEventDecision,
) agent.WorkerEventReceipt {
	receipt := agent.WorkerEventReceipt{
		Delivery: delivery.ID, Event: delivery.Event.ID, Decision: decision,
	}
	if decision == agent.WorkerEventNACK {
		receipt.Code = agent.WorkerRejectInvalid
		receipt.Message = "rejected by conformance endpoint"
	}
	return receipt
}

func conformanceJournalTaskRef() agent.TaskRef {
	return agent.TaskRef{
		Workspace: agent.WorkspaceRef{Authority: "authority-a", ID: "workspace-a"},
		ID:        "task-a",
	}
}

func mustJournalDigest(t *testing.T, value any) workerws.JournalDigest {
	t.Helper()
	digest, err := memoryJournalDigest(value)
	if err != nil {
		t.Fatalf("digest Agent worker Journal value: %v", err)
	}
	return digest
}

func differentJournalDigest(digest workerws.JournalDigest) workerws.JournalDigest {
	if digest[0] == '0' {
		return "1" + digest[1:]
	}
	return "0" + digest[1:]
}

func assignmentSequences(records []workerws.JournalAssignment) []workerws.JournalSequence {
	result := make([]workerws.JournalSequence, len(records))
	for index := range records {
		result[index] = records[index].Sequence
	}
	return result
}

func eventSequences(records []workerws.JournalEvent) []workerws.JournalSequence {
	result := make([]workerws.JournalSequence, len(records))
	for index := range records {
		result[index] = records[index].Sequence
	}
	return result
}

func assertStrictJournalSequences(t *testing.T, sequences []workerws.JournalSequence) {
	t.Helper()
	for index, sequence := range sequences {
		if sequence == 0 || (index > 0 && sequence <= sequences[index-1]) {
			t.Fatalf("Journal sequences are not non-zero and strictly increasing: %#v", sequences)
		}
	}
}

var _ workerws.Journal = (*MemoryAgentWorkerJournal)(nil)
