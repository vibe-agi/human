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

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/workerws"
)

// LLMWorkerJournalFactory opens a new, empty HumanLLM worker Journal for one
// conformance subtest. Every invocation must return an independent Journal.
// release must be non-nil and release every resource opened by the factory.
// The suite invokes it exactly once, including the close-behavior subtest.
//
// A provider normally exposes a test like:
//
//	func TestJournalConformance(t *testing.T) {
//		humantest.TestLLMWorkerJournal(t, func(ctx context.Context, t testing.TB) (
//			workerws.Journal, framework.ReleaseFunc, error,
//		) {
//			return openTestJournal(ctx, t.TempDir())
//		})
//	}
//
// The context bounds construction and the subtest and must not be retained.
// release receives a fresh context so cancellation of an operation cannot
// prevent deterministic cleanup.
type LLMWorkerJournalFactory func(
	context.Context,
	testing.TB,
) (workerws.Journal, framework.ReleaseFunc, error)

// ShortPageLLMWorkerJournal is a fault-injection adapter for Journal
// consumers. It deliberately returns fewer records than requested while
// preserving the wrapped cursor semantics. The Journal contract promises only
// "up to limit", so consumers must scan until an empty page rather than treating
// a short page as EOF. A non-positive PageSize leaves the caller's limit intact.
type ShortPageLLMWorkerJournal struct {
	workerws.Journal
	PageSize int
}

func (journal ShortPageLLMWorkerJournal) ListAssignments(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalAssignment, error) {
	return journal.Journal.ListAssignments(ctx, after, journal.cap(limit))
}

func (journal ShortPageLLMWorkerJournal) ListEvents(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalEvent, error) {
	return journal.Journal.ListEvents(ctx, after, journal.cap(limit))
}

func (journal ShortPageLLMWorkerJournal) ListRejections(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalRejection, error) {
	return journal.Journal.ListRejections(ctx, after, journal.cap(limit))
}

func (journal ShortPageLLMWorkerJournal) cap(limit int) int {
	// Preserve invalid caller input so the wrapped Journal still enforces the
	// public limit contract instead of accidentally laundering it into a valid
	// short page.
	if journal.PageSize > 0 && limit > journal.PageSize && limit <= workerws.MaxJournalPageSize {
		return journal.PageSize
	}
	return limit
}

// TestLLMWorkerJournal runs the mandatory black-box conformance suite for
// durable HumanLLM worker inbox/outbox journals. Passing it verifies the public
// contract, but does not replace provider-specific crash, durability, schema,
// permission, commit-ambiguity, and infrastructure tests.
func TestLLMWorkerJournal(t *testing.T, factory LLMWorkerJournalFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("HumanLLM worker Journal conformance factory is nil")
	}

	tests := []struct {
		name string
		run  func(context.Context, *testing.T, workerws.Journal, framework.ReleaseFunc)
	}{
		{"description_and_contract", testLLMWorkerJournalDescription},
		{"binding_and_fail_closed_namespace", testLLMWorkerJournalBinding},
		{"assignment_exact_replay_and_tombstone", testLLMWorkerJournalAssignments},
		{"fifo_paging_and_byte_ownership", testLLMWorkerJournalFIFOAndBytes},
		{"event_and_rejection_fifo_paging", testLLMWorkerJournalEventAndRejectionPaging},
		{"event_ack_settlement", testLLMWorkerJournalACK},
		{"event_nack_atomic_rejection", testLLMWorkerJournalNACK},
		{"concurrent_sequence_allocation", testLLMWorkerJournalConcurrentSequences},
		{"validation_limits_and_context", testLLMWorkerJournalValidation},
		{"release_closes_journal", testLLMWorkerJournalClose},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			t.Cleanup(cancel)

			journal, release, err := factory(ctx, t)
			if err != nil {
				t.Fatalf("open fresh HumanLLM worker Journal: %v", err)
			}
			if journal == nil {
				t.Fatal("factory returned a nil HumanLLM worker Journal")
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
					return errors.New("humantest: HumanLLM worker Journal released more than once")
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
					t.Errorf("release HumanLLM worker Journal: %v", err)
				}
			})

			test.run(ctx, t, journal, releaseOnce)
		})
	}
}

// MemoryLLMWorkerJournal is one open handle onto a
// MemoryLLMWorkerJournalImage. It has no Close method because handle lifetime is
// represented by the framework.ReleaseFunc returned when it is opened.
type MemoryLLMWorkerJournal struct {
	memoryLLMWorkerJournalHandle
}

// The unexported embedding preserves concise access to the image state inside
// this semantic model without exposing a replaceable image pointer on a live
// handle.
type memoryLLMWorkerJournalHandle struct {
	*MemoryLLMWorkerJournalImage
	lifecycle *memoryLLMWorkerJournalLifecycle
}

type memoryLLMWorkerJournalLifecycle struct{ closed bool }

// MemoryLLMWorkerJournalImage is a concurrency-safe semantic durable image for
// tests. It deliberately follows the same binding, FIFO, exact-replay,
// compaction, and byte-ownership rules as durable providers. Closing a handle
// leaves the image intact, so recovery tests can open a fresh handle after
// simulated loss of process-local handle state. This does not model physical
// process or storage durability.
//
// At most one handle may be open at a time. The image is a test model, not a
// production persistence adapter.
type MemoryLLMWorkerJournalImage struct {
	mu          sync.Mutex
	owner       *memoryLLMWorkerJournalLifecycle
	binding     *workerws.JournalBinding
	next        workerws.JournalSequence
	assignments map[llm.WorkerDeliveryID]*memoryLLMJournalAssignment
	events      map[llm.WorkerDeliveryID]*memoryLLMJournalEvent
	rejections  map[llm.WorkerDeliveryID]*memoryLLMJournalRejection
}

type memoryLLMJournalAssignment struct {
	digest   workerws.JournalDigest
	sequence workerws.JournalSequence
	pending  *llm.WorkerAssignmentDelivery
}

type memoryLLMJournalEvent struct {
	digest        workerws.JournalDigest
	sequence      workerws.JournalSequence
	pending       *llm.WorkerEventDelivery
	receiptDigest workerws.JournalDigest
}

type memoryLLMJournalRejection struct {
	sequence      workerws.JournalSequence
	eventDigest   workerws.JournalDigest
	receiptDigest workerws.JournalDigest
	pending       *workerws.RejectedEvent
}

// NewMemoryLLMWorkerJournalImage creates an independent, initially unbound
// durable image with no open handle.
func NewMemoryLLMWorkerJournalImage() *MemoryLLMWorkerJournalImage {
	return &MemoryLLMWorkerJournalImage{
		assignments: make(map[llm.WorkerDeliveryID]*memoryLLMJournalAssignment),
		events:      make(map[llm.WorkerDeliveryID]*memoryLLMJournalEvent),
		rejections:  make(map[llm.WorkerDeliveryID]*memoryLLMJournalRejection),
	}
}

// Open returns the sole live handle onto image. Successful Journal mutations
// update image before they return; releasing the handle only relinquishes
// ownership and never flushes otherwise-volatile state.
func (image *MemoryLLMWorkerJournalImage) Open(
	ctx context.Context,
) (*MemoryLLMWorkerJournal, framework.ReleaseFunc, error) {
	if ctx == nil {
		return nil, nil, errors.New("open memory HumanLLM worker Journal: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if image == nil {
		return nil, nil, errors.New("open memory HumanLLM worker Journal: image is required")
	}
	image.mu.Lock()
	defer image.mu.Unlock()
	if image.owner != nil {
		return nil, nil, errors.New("open memory HumanLLM worker Journal: image already has an open handle")
	}
	lifecycle := &memoryLLMWorkerJournalLifecycle{}
	journal := &MemoryLLMWorkerJournal{
		memoryLLMWorkerJournalHandle: memoryLLMWorkerJournalHandle{
			MemoryLLMWorkerJournalImage: image,
			lifecycle:                   lifecycle,
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
func (image *MemoryLLMWorkerJournalImage) Abandon(journal *MemoryLLMWorkerJournal) error {
	if image == nil || journal == nil ||
		journal.MemoryLLMWorkerJournalImage != image || journal.lifecycle == nil {
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

// NewMemoryLLMWorkerJournal creates an independent image, opens its first
// handle, and returns that handle with an idempotent release function.
func NewMemoryLLMWorkerJournal() (*MemoryLLMWorkerJournal, framework.ReleaseFunc) {
	journal, release, err := NewMemoryLLMWorkerJournalImage().Open(context.Background())
	if err != nil {
		panic(fmt.Sprintf("open fresh memory HumanLLM worker Journal: %v", err))
	}
	return journal, release
}

func (*MemoryLLMWorkerJournal) Description() workerws.JournalDescription {
	return workerws.JournalDescription{
		Contract: framework.Contract{ID: workerws.JournalContractID, Major: workerws.JournalContractMajor},
		Provider: "humantest-memory", Version: "1",
	}
}

func (journal *MemoryLLMWorkerJournal) Bind(ctx context.Context, binding workerws.JournalBinding) error {
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

func (journal *MemoryLLMWorkerJournal) PutAssignment(
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
	delivery := llm.CloneWorkerAssignmentDelivery(record.Delivery)
	journal.assignments[id] = &memoryLLMJournalAssignment{
		digest: record.Digest, sequence: sequence, pending: &delivery,
	}
	return workerws.JournalEntryPending, nil
}

func (journal *MemoryLLMWorkerJournal) ConfirmAssignment(ctx context.Context, id llm.WorkerDeliveryID) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return err
	}
	if !validLLMJournalDeliveryID(id) {
		return workerws.ErrJournalCorrupt
	}
	current := journal.assignments[id]
	if current == nil {
		return workerws.ErrJournalNotFound
	}
	current.pending = nil
	return nil
}

func (journal *MemoryLLMWorkerJournal) ListAssignments(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalAssignment, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateMemoryLLMJournalLimit(limit); err != nil {
		return nil, err
	}
	result := make([]workerws.JournalAssignment, 0, min(limit, len(journal.assignments)))
	for _, current := range journal.assignments {
		if current.pending == nil || current.sequence <= after {
			continue
		}
		result = append(result, workerws.JournalAssignment{
			Sequence: current.sequence, Digest: current.digest,
			Delivery: llm.CloneWorkerAssignmentDelivery(*current.pending),
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Sequence < result[right].Sequence })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (journal *MemoryLLMWorkerJournal) PutEvent(
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
	delivery := llm.CloneWorkerEventDelivery(record.Delivery)
	journal.events[id] = &memoryLLMJournalEvent{
		digest: record.Digest, sequence: sequence, pending: &delivery,
	}
	return workerws.JournalEntryPending, nil
}

func (journal *MemoryLLMWorkerJournal) ListEvents(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalEvent, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateMemoryLLMJournalLimit(limit); err != nil {
		return nil, err
	}
	result := make([]workerws.JournalEvent, 0, min(limit, len(journal.events)))
	for _, current := range journal.events {
		if current.pending == nil || current.sequence <= after {
			continue
		}
		result = append(result, workerws.JournalEvent{
			Sequence: current.sequence, Digest: current.digest,
			Delivery: llm.CloneWorkerEventDelivery(*current.pending),
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Sequence < result[right].Sequence })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (journal *MemoryLLMWorkerJournal) SettleEvent(
	ctx context.Context,
	receipt llm.WorkerEventReceipt,
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
	delivery := llm.CloneWorkerEventDelivery(*current.pending)
	current.pending = nil
	current.receiptDigest = receiptDigest
	if receipt.Decision == llm.WorkerEventNACK {
		sequence, sequenceErr := journal.allocateSequence()
		if sequenceErr != nil {
			// Allocation is checked before any practically reachable overflow. Keep
			// the semantic model atomic even at that artificial boundary.
			current.pending = &delivery
			current.receiptDigest = ""
			return sequenceErr
		}
		rejected := workerws.RejectedEvent{Delivery: delivery, Receipt: receipt}
		journal.rejections[receipt.Delivery] = &memoryLLMJournalRejection{
			sequence: sequence, eventDigest: eventDigest, receiptDigest: receiptDigest, pending: &rejected,
		}
	}
	return nil
}

func (journal *MemoryLLMWorkerJournal) ListRejections(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalRejection, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateMemoryLLMJournalLimit(limit); err != nil {
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
				Delivery: llm.CloneWorkerEventDelivery(current.pending.Delivery),
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

func (journal *MemoryLLMWorkerJournal) ConfirmRejection(ctx context.Context, id llm.WorkerDeliveryID) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return err
	}
	if !validLLMJournalDeliveryID(id) {
		return workerws.ErrJournalCorrupt
	}
	current := journal.rejections[id]
	if current == nil {
		return workerws.ErrJournalNotFound
	}
	current.pending = nil
	return nil
}

func (journal *MemoryLLMWorkerJournal) contextAndOpen(ctx context.Context) error {
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

func (journal *MemoryLLMWorkerJournal) ready(ctx context.Context) error {
	if err := journal.contextAndOpen(ctx); err != nil {
		return err
	}
	if journal.binding == nil {
		return workerws.ErrJournalCorrupt
	}
	return nil
}

func (journal *MemoryLLMWorkerJournal) principal() llm.AuthenticatedWorker {
	return llm.AuthenticatedWorker{
		WorkerID:  journal.binding.Worker,
		SessionID: "humantest-journal-validation",
	}
}

func (journal *MemoryLLMWorkerJournal) validateAssignment(record workerws.JournalAssignment) error {
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

func (journal *MemoryLLMWorkerJournal) validateEvent(record workerws.JournalEvent) error {
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

func (journal *MemoryLLMWorkerJournal) allocateSequence() (workerws.JournalSequence, error) {
	if journal.next == workerws.JournalSequence(^uint64(0)) {
		return 0, workerws.ErrJournalLimit
	}
	journal.next++
	if journal.next == 0 {
		return 0, workerws.ErrJournalLimit
	}
	return journal.next, nil
}

func validateMemoryLLMJournalLimit(limit int) error {
	if limit < 1 || limit > workerws.MaxJournalPageSize {
		return fmt.Errorf("%w: page limit must be 1..%d", workerws.ErrJournalLimit, workerws.MaxJournalPageSize)
	}
	return nil
}

func validLLMJournalDeliveryID(id llm.WorkerDeliveryID) bool {
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

func memoryLLMJournalDigest(value any) (workerws.JournalDigest, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return workerws.JournalDigest(hex.EncodeToString(digest[:])), nil
}

func testLLMWorkerJournalDescription(
	_ context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	t.Helper()
	first := journal.Description()
	if err := first.Validate(); err != nil {
		t.Fatalf("Description does not satisfy HumanLLM worker Journal contract: %v", err)
	}
	second := journal.Description()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Description is not static:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

func testLLMWorkerJournalBinding(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	t.Helper()
	assignment := llmConformanceJournalAssignment("prebind-assignment")
	event := llmConformanceJournalEvent("prebind-event", llm.EventAccepted, nil)
	ack := llmConformanceJournalReceipt(event.Delivery, llm.WorkerEventACK)
	eventDigest := mustLLMJournalDigest(t, event.Delivery)
	receiptDigest := mustLLMJournalDigest(t, ack)
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

	invalid := llmConformanceJournalBinding()
	invalid.Gateway = ""
	if err := journal.Bind(ctx, invalid); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("invalid Bind error = %v, want ErrJournalCorrupt", err)
	}
	binding := llmConformanceJournalBinding()
	if err := journal.Bind(ctx, binding); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := journal.Bind(ctx, binding); err != nil {
		t.Fatalf("exact Bind replay: %v", err)
	}
	variants := []workerws.JournalBinding{
		{Gateway: "gateway-b", Worker: binding.Worker},
		{Gateway: binding.Gateway, Worker: "worker-b"},
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

func testLLMWorkerJournalAssignments(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindLLMConformanceJournal(ctx, t, journal)
	record := llmConformanceJournalAssignment("assignment-exact")
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
	divergent.Delivery = llm.CloneWorkerAssignmentDelivery(record.Delivery)
	divergent.Delivery.Assignment.Request.Metadata["revision"] = "2"
	divergent.Digest = mustLLMJournalDigest(t, divergent.Delivery)
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

func testLLMWorkerJournalFIFOAndBytes(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindLLMConformanceJournal(ctx, t, journal)
	for index := 0; index < 5; index++ {
		record := llmConformanceJournalAssignment(llm.WorkerDeliveryID(fmt.Sprintf("assignment-page-%d", index)))
		if _, err := journal.PutAssignment(ctx, record); err != nil {
			t.Fatalf("PutAssignment %d: %v", index, err)
		}
	}
	var assignmentAfter workerws.JournalSequence
	var allAssignments []workerws.JournalAssignment
	for {
		page, err := journal.ListAssignments(ctx, assignmentAfter, 2)
		if err != nil {
			t.Fatalf("ListAssignments page after %d: %v", assignmentAfter, err)
		}
		if len(page) > 2 {
			t.Fatalf("assignment page length = %d, exceeds requested limit 2", len(page))
		}
		if len(page) == 0 {
			break
		}
		allAssignments = append(allAssignments, page...)
		assignmentAfter = page[len(page)-1].Sequence
		if len(allAssignments) > 5 {
			t.Fatalf("assignment cursor repeated or invented records: %#v", allAssignments)
		}
	}
	if len(allAssignments) != 5 {
		t.Fatalf("assignment paging returned %d records, want 5", len(allAssignments))
	}
	assertStrictLLMJournalSequences(t, llmJournalAssignmentSequences(allAssignments))
	if trailing, err := journal.ListAssignments(ctx, assignmentAfter, 2); err != nil || len(trailing) != 0 {
		t.Fatalf("assignment page after tail = %#v, %v", trailing, err)
	}
	ownedAssignment := llmConformanceJournalAssignment("assignment-owned-bytes")
	ownedAssignment.Delivery.Assignment.Request.Tools = []llm.Tool{{
		Namespace: "human", Name: "write_file", InputSchema: json.RawMessage(`{"type":"object"}`),
	}}
	ownedAssignment.Digest = mustLLMJournalDigest(t, ownedAssignment.Delivery)
	wantAssignment := llm.CloneWorkerAssignmentDelivery(ownedAssignment.Delivery)
	if _, err := journal.PutAssignment(ctx, ownedAssignment); err != nil {
		t.Fatalf("PutAssignment with raw JSON: %v", err)
	}
	ownedAssignment.Delivery.Assignment.Request.Tools[0].InputSchema[0] = 'X'
	ownedAssignment.Delivery.Assignment.Request.Metadata["revision"] = "changed"
	listedAssignments, err := journal.ListAssignments(ctx, assignmentAfter, 10)
	if err != nil || len(listedAssignments) != 1 ||
		!reflect.DeepEqual(listedAssignments[0].Delivery, wantAssignment) {
		t.Fatalf("Journal retained caller-owned assignment bytes: got %#v, want %#v, err %v", listedAssignments, wantAssignment, err)
	}
	listedAssignments[0].Delivery.Assignment.Request.Tools[0].InputSchema[0] = 'Y'
	againAssignments, err := journal.ListAssignments(ctx, assignmentAfter, 10)
	if err != nil || len(againAssignments) != 1 ||
		!reflect.DeepEqual(againAssignments[0].Delivery, wantAssignment) {
		t.Fatalf("ListAssignments returned aliased bytes: got %#v, want %#v, err %v", againAssignments, wantAssignment, err)
	}

	payload := []byte("owned payload")
	event := llmConformanceJournalEvent("event-owned-bytes", llm.EventToolCalls, payload)
	want := llm.CloneWorkerEventDelivery(event.Delivery)
	if _, err := journal.PutEvent(ctx, event); err != nil {
		t.Fatalf("PutEvent with bytes: %v", err)
	}
	payload[0] = 'X'
	event.Delivery.Event.ToolCalls[0].Input["payload"] = "changed"
	listed, err := journal.ListEvents(ctx, 0, 10)
	if err != nil || len(listed) != 1 || !reflect.DeepEqual(listed[0].Delivery, want) {
		t.Fatalf("Journal retained caller-owned bytes: got %#v, want %#v, err %v", listed, want, err)
	}
	listed[0].Delivery.Event.ToolCalls[0].Input["payload"] = "changed-again"
	again, err := journal.ListEvents(ctx, 0, 10)
	if err != nil || len(again) != 1 || !reflect.DeepEqual(again[0].Delivery, want) {
		t.Fatalf("ListEvents returned aliased bytes: got %#v, want %#v, err %v", again, want, err)
	}
}

func testLLMWorkerJournalEventAndRejectionPaging(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindLLMConformanceJournal(ctx, t, journal)
	events := make([]workerws.JournalEvent, 5)
	for index := range events {
		events[index] = llmConformanceJournalEvent(
			llm.WorkerDeliveryID(fmt.Sprintf("event-page-%d", index)),
			llm.EventProgress,
			nil,
		)
		if _, err := journal.PutEvent(ctx, events[index]); err != nil {
			t.Fatalf("PutEvent page fixture %d: %v", index, err)
		}
	}
	var eventAfter workerws.JournalSequence
	var allEvents []workerws.JournalEvent
	for {
		page, err := journal.ListEvents(ctx, eventAfter, 2)
		if err != nil {
			t.Fatalf("ListEvents page after %d: %v", eventAfter, err)
		}
		if len(page) > 2 {
			t.Fatalf("event page length = %d, exceeds requested limit 2", len(page))
		}
		if len(page) == 0 {
			break
		}
		allEvents = append(allEvents, page...)
		eventAfter = page[len(page)-1].Sequence
		if len(allEvents) > len(events) {
			t.Fatalf("event cursor repeated or invented records: %#v", allEvents)
		}
	}
	if len(allEvents) != len(events) {
		t.Fatalf("event paging returned %d records, want %d", len(allEvents), len(events))
	}
	assertStrictLLMJournalSequences(t, llmJournalEventSequences(allEvents))
	for index, record := range allEvents {
		if want := events[index].Delivery.ID; record.Delivery.ID != want {
			t.Fatalf("event page item %d = %q, want %q", index, record.Delivery.ID, want)
		}
	}
	if trailing, err := journal.ListEvents(ctx, eventAfter, 2); err != nil || len(trailing) != 0 {
		t.Fatalf("event page after tail = %#v, %v", trailing, err)
	}

	// Move every event into the rejection inbox. Its sequence is allocated at
	// settlement time, so rejection paging must have its own strict cursor rather
	// than inheriting event insertion order accidentally.
	for _, record := range events {
		receipt := llmConformanceJournalReceipt(record.Delivery, llm.WorkerEventNACK)
		if err := journal.SettleEvent(ctx, receipt, record.Digest, mustLLMJournalDigest(t, receipt)); err != nil {
			t.Fatalf("SettleEvent NACK page fixture %q: %v", record.Delivery.ID, err)
		}
	}
	var rejectionAfter workerws.JournalSequence
	var allRejections []workerws.JournalRejection
	for {
		page, err := journal.ListRejections(ctx, rejectionAfter, 2)
		if err != nil {
			t.Fatalf("ListRejections page after %d: %v", rejectionAfter, err)
		}
		if len(page) > 2 {
			t.Fatalf("rejection page length = %d, exceeds requested limit 2", len(page))
		}
		if len(page) == 0 {
			break
		}
		allRejections = append(allRejections, page...)
		rejectionAfter = page[len(page)-1].Sequence
		if len(allRejections) > len(events) {
			t.Fatalf("rejection cursor repeated or invented records: %#v", allRejections)
		}
	}
	if len(allRejections) != len(events) {
		t.Fatalf("rejection paging returned %d records, want %d", len(allRejections), len(events))
	}
	assertStrictLLMJournalSequences(t, llmJournalRejectionSequences(allRejections))
	for index, record := range allRejections {
		if want := events[index].Delivery.ID; record.Delivery.ID != want {
			t.Fatalf("rejection page item %d = %q, want %q", index, record.Delivery.ID, want)
		}
	}
	if trailing, err := journal.ListRejections(ctx, rejectionAfter, 2); err != nil || len(trailing) != 0 {
		t.Fatalf("rejection page after tail = %#v, %v", trailing, err)
	}
}

func testLLMWorkerJournalACK(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindLLMConformanceJournal(ctx, t, journal)
	record := llmConformanceJournalEvent("event-ack", llm.EventAccepted, nil)
	if state, err := journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutEvent new = (%q, %v), want pending", state, err)
	}
	if state, err := journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutEvent exact pending replay = (%q, %v), want pending", state, err)
	}
	assertLLMWorkerJournalEventReplayConflicts(ctx, t, journal, record, "pending")
	receipt := llmConformanceJournalReceipt(record.Delivery, llm.WorkerEventACK)
	receiptDigest := mustLLMJournalDigest(t, receipt)
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
	assertLLMWorkerJournalEventReplayConflicts(ctx, t, journal, record, "ACK-settled")
	if err := journal.SettleEvent(ctx, receipt, differentLLMJournalDigest(record.Digest), receiptDigest); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("SettleEvent divergent event digest error = %v, want ErrJournalConflict", err)
	}
	divergentReceipt := llmConformanceJournalReceipt(record.Delivery, llm.WorkerEventNACK)
	divergentReceiptDigest := mustLLMJournalDigest(t, divergentReceipt)
	if err := journal.SettleEvent(ctx, divergentReceipt, record.Digest, divergentReceiptDigest); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("SettleEvent divergent receipt error = %v, want ErrJournalConflict", err)
	}
}

func testLLMWorkerJournalNACK(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindLLMConformanceJournal(ctx, t, journal)
	record := llmConformanceJournalEvent("event-nack", llm.EventToolCalls, []byte("rejected payload"))
	if _, err := journal.PutEvent(ctx, record); err != nil {
		t.Fatalf("PutEvent before NACK: %v", err)
	}
	assertLLMWorkerJournalEventReplayConflicts(ctx, t, journal, record, "pending before NACK")
	receipt := llmConformanceJournalReceipt(record.Delivery, llm.WorkerEventNACK)
	receiptDigest := mustLLMJournalDigest(t, receipt)
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
	got.Delivery.Event.ToolCalls[0].Input["payload"] = "changed"
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
	assertLLMWorkerJournalEventReplayConflicts(ctx, t, journal, record, "NACK-settled")
	if err := journal.ConfirmRejection(ctx, "missing-rejection"); !errors.Is(err, workerws.ErrJournalNotFound) {
		t.Fatalf("ConfirmRejection missing error = %v, want ErrJournalNotFound", err)
	}
}

func testLLMWorkerJournalConcurrentSequences(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	bindLLMConformanceJournal(ctx, t, journal)
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
				record := llmConformanceJournalAssignment(llm.WorkerDeliveryID(fmt.Sprintf("concurrent-assignment-%03d", index)))
				_, err := journal.PutAssignment(ctx, record)
				errorsByIndex <- err
				return
			}
			record := llmConformanceJournalEvent(llm.WorkerDeliveryID(fmt.Sprintf("concurrent-event-%03d", index)), llm.EventAccepted, nil)
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
	var assignments []workerws.JournalAssignment
	var assignmentAfter workerws.JournalSequence
	for {
		page, err := journal.ListAssignments(ctx, assignmentAfter, workerws.MaxJournalPageSize)
		if err != nil {
			t.Fatalf("ListAssignments after concurrent Put: %v", err)
		}
		if len(page) == 0 {
			break
		}
		assignments = append(assignments, page...)
		assignmentAfter = page[len(page)-1].Sequence
		if len(assignments) > count {
			t.Fatalf("concurrent assignment cursor repeated or invented records: %#v", assignments)
		}
	}
	var events []workerws.JournalEvent
	var eventAfter workerws.JournalSequence
	for {
		page, err := journal.ListEvents(ctx, eventAfter, workerws.MaxJournalPageSize)
		if err != nil {
			t.Fatalf("ListEvents after concurrent Put: %v", err)
		}
		if len(page) == 0 {
			break
		}
		events = append(events, page...)
		eventAfter = page[len(page)-1].Sequence
		if len(events) > count {
			t.Fatalf("concurrent event cursor repeated or invented records: %#v", events)
		}
	}
	if len(assignments)+len(events) != count {
		t.Fatalf("concurrent Put count = %d, want %d", len(assignments)+len(events), count)
	}
	assertStrictLLMJournalSequences(t, llmJournalAssignmentSequences(assignments))
	assertStrictLLMJournalSequences(t, llmJournalEventSequences(events))
	allSequences := append(llmJournalAssignmentSequences(assignments), llmJournalEventSequences(events)...)
	sort.Slice(allSequences, func(left, right int) bool { return allSequences[left] < allSequences[right] })
	assertStrictLLMJournalSequences(t, allSequences)
}

func testLLMWorkerJournalValidation(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	_ framework.ReleaseFunc,
) {
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := journal.Bind(cancelled, llmConformanceJournalBinding()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Bind cancelled context error = %v, want context.Canceled", err)
	}
	bindLLMConformanceJournal(ctx, t, journal)

	assignment := llmConformanceJournalAssignment("validation-assignment")
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
	invalidAssignment.Digest = mustLLMJournalDigest(t, invalidAssignment.Delivery)
	if _, err := journal.PutAssignment(ctx, invalidAssignment); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("PutAssignment invalid delivery error = %v, want ErrJournalCorrupt", err)
	}

	event := llmConformanceJournalEvent("validation-event", llm.EventAccepted, nil)
	withEventSequence := event
	withEventSequence.Sequence = 9
	if _, err := journal.PutEvent(ctx, withEventSequence); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("PutEvent caller sequence error = %v, want ErrJournalCorrupt", err)
	}
	if _, err := journal.PutEvent(cancelled, event); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutEvent cancelled context error = %v, want context.Canceled", err)
	}
	badEventDigest := event
	badEventDigest.Digest = "not-a-digest"
	if _, err := journal.PutEvent(ctx, badEventDigest); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("PutEvent invalid digest error = %v, want ErrJournalCorrupt", err)
	}
	invalidEvent := event
	invalidEvent.Delivery.ID = ""
	invalidEvent.Digest = mustLLMJournalDigest(t, invalidEvent.Delivery)
	if _, err := journal.PutEvent(ctx, invalidEvent); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("PutEvent invalid delivery error = %v, want ErrJournalCorrupt", err)
	}
	if _, err := journal.PutEvent(ctx, event); err != nil {
		t.Fatalf("PutEvent settlement validation fixture: %v", err)
	}
	validReceipt := llmConformanceJournalReceipt(event.Delivery, llm.WorkerEventACK)
	if err := journal.SettleEvent(ctx, validReceipt, "not-a-digest", mustLLMJournalDigest(t, validReceipt)); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("SettleEvent invalid event digest error = %v, want ErrJournalCorrupt", err)
	}
	if err := journal.SettleEvent(ctx, validReceipt, event.Digest, "not-a-digest"); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("SettleEvent invalid receipt digest error = %v, want ErrJournalCorrupt", err)
	}
	invalidReceipt := validReceipt
	invalidReceipt.Code = llm.WorkerRejectInvalid
	if err := journal.SettleEvent(
		ctx,
		invalidReceipt,
		event.Digest,
		mustLLMJournalDigest(t, invalidReceipt),
	); !errors.Is(err, workerws.ErrJournalCorrupt) {
		t.Fatalf("SettleEvent invalid receipt shape error = %v, want ErrJournalCorrupt", err)
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

func testLLMWorkerJournalClose(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	release framework.ReleaseFunc,
) {
	bindLLMConformanceJournal(ctx, t, journal)
	description := journal.Description()
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := release(releaseCtx); err != nil {
		t.Fatalf("release HumanLLM worker Journal: %v", err)
	}
	if !reflect.DeepEqual(journal.Description(), description) {
		t.Fatal("Description changed after release")
	}
	assignment := llmConformanceJournalAssignment("closed-assignment")
	event := llmConformanceJournalEvent("closed-event", llm.EventAccepted, nil)
	receipt := llmConformanceJournalReceipt(event.Delivery, llm.WorkerEventACK)
	eventDigest := mustLLMJournalDigest(t, event.Delivery)
	receiptDigest := mustLLMJournalDigest(t, receipt)
	closed := []struct {
		name string
		call func() error
	}{
		{"bind", func() error { return journal.Bind(ctx, llmConformanceJournalBinding()) }},
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

func llmConformanceJournalBinding() workerws.JournalBinding {
	return workerws.JournalBinding{Gateway: "gateway-a", Worker: "worker-a"}
}

func bindLLMConformanceJournal(ctx context.Context, t *testing.T, journal workerws.Journal) {
	t.Helper()
	if err := journal.Bind(ctx, llmConformanceJournalBinding()); err != nil {
		t.Fatalf("Bind HumanLLM worker Journal: %v", err)
	}
}

func llmConformanceJournalAssignment(id llm.WorkerDeliveryID) workerws.JournalAssignment {
	delivery := llm.WorkerAssignmentDelivery{
		ID: id,
		Assignment: llm.Assignment{
			Identity: llm.CompletionIdentity{
				CallerID: "caller-a", RequestID: "request-a", TaskID: "task-a",
				WorkspaceKey: "workspace-a", IdempotencyKey: "idempotency-a",
			},
			Lease:    llm.WorkerLease{ID: "lease-a", Owner: "worker-a"},
			Boundary: llm.AssignmentAfterResponse,
			Task: llm.TaskContext{
				TaskID: "task-a", WorkspaceKey: "workspace-a", CapabilityTier: llm.TierWorkspace,
				HarnessID: "harness-a", HarnessVersion: "1", HarnessSessionID: "session-a",
			},
			Request: llm.Request{
				Model: "human", Stream: true,
				Messages: []llm.Message{{
					Role:   llm.RoleUser,
					Blocks: []llm.Block{{Type: llm.BlockText, Text: "Please inspect the workspace."}},
				}},
				Metadata: map[string]string{"revision": "1"},
			},
		},
	}
	digest, err := memoryLLMJournalDigest(delivery)
	if err != nil {
		panic(err)
	}
	return workerws.JournalAssignment{Digest: digest, Delivery: delivery}
}

func llmConformanceJournalEvent(
	id llm.WorkerDeliveryID,
	kind llm.EventType,
	payload []byte,
) workerws.JournalEvent {
	event := llm.Event{ID: "event-" + string(id), Type: kind}
	switch kind {
	case llm.EventAccepted:
	case llm.EventProgress:
		event.Text = "working"
	case llm.EventFinal:
		event.Text = "done"
	case llm.EventToolCalls:
		event.ToolCalls = []llm.ToolCall{{
			ID: "tool-" + string(id), Namespace: "human", Name: "write_file",
			Input: map[string]any{
				"payload": string(payload), "large_integer": json.Number("18446744073709551615"),
			},
		}}
	}
	delivery := llm.WorkerEventDelivery{
		ID: id,
		Identity: llm.CompletionIdentity{
			CallerID: "caller-a", RequestID: "request-a", TaskID: "task-a",
			WorkspaceKey: "workspace-a", IdempotencyKey: "idempotency-a",
		},
		LeaseID: "lease-a", Event: event,
	}
	digest, err := memoryLLMJournalDigest(delivery)
	if err != nil {
		panic(err)
	}
	return workerws.JournalEvent{Digest: digest, Delivery: delivery}
}

func llmConformanceJournalReceipt(
	delivery llm.WorkerEventDelivery,
	decision llm.WorkerEventDecision,
) llm.WorkerEventReceipt {
	receipt := llm.WorkerEventReceipt{
		Delivery: delivery.ID, EventID: delivery.Event.ID, Decision: decision,
	}
	if decision == llm.WorkerEventNACK {
		receipt.Code = llm.WorkerRejectInvalid
		receipt.Message = "rejected by conformance endpoint"
	}
	return receipt
}

func mustLLMJournalDigest(t *testing.T, value any) workerws.JournalDigest {
	t.Helper()
	digest, err := memoryLLMJournalDigest(value)
	if err != nil {
		t.Fatalf("digest HumanLLM worker Journal value: %v", err)
	}
	return digest
}

func differentLLMJournalDigest(digest workerws.JournalDigest) workerws.JournalDigest {
	if digest[0] == '0' {
		return "1" + digest[1:]
	}
	return "0" + digest[1:]
}

func assertLLMWorkerJournalEventReplayConflicts(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	record workerws.JournalEvent,
	phase string,
) {
	t.Helper()
	samePayloadDifferentDigest := record
	samePayloadDifferentDigest.Digest = differentLLMJournalDigest(record.Digest)
	if _, err := journal.PutEvent(ctx, samePayloadDifferentDigest); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("PutEvent %s replay with different digest error = %v, want ErrJournalConflict", phase, err)
	}

	differentPayload := record
	differentPayload.Delivery = llm.CloneWorkerEventDelivery(record.Delivery)
	differentPayload.Delivery.Event.ID += "-changed"
	differentPayload.Digest = mustLLMJournalDigest(t, differentPayload.Delivery)
	if _, err := journal.PutEvent(ctx, differentPayload); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("PutEvent %s replay with different payload error = %v, want ErrJournalConflict", phase, err)
	}
}

func llmJournalAssignmentSequences(records []workerws.JournalAssignment) []workerws.JournalSequence {
	result := make([]workerws.JournalSequence, len(records))
	for index := range records {
		result[index] = records[index].Sequence
	}
	return result
}

func llmJournalEventSequences(records []workerws.JournalEvent) []workerws.JournalSequence {
	result := make([]workerws.JournalSequence, len(records))
	for index := range records {
		result[index] = records[index].Sequence
	}
	return result
}

func llmJournalRejectionSequences(records []workerws.JournalRejection) []workerws.JournalSequence {
	result := make([]workerws.JournalSequence, len(records))
	for index := range records {
		result[index] = records[index].Sequence
	}
	return result
}

func assertStrictLLMJournalSequences(t *testing.T, sequences []workerws.JournalSequence) {
	t.Helper()
	for index, sequence := range sequences {
		if sequence == 0 || (index > 0 && sequence <= sequences[index-1]) {
			t.Fatalf("Journal sequences are not non-zero and strictly increasing: %#v", sequences)
		}
	}
}

var _ workerws.Journal = (*MemoryLLMWorkerJournal)(nil)
var _ workerws.Journal = ShortPageLLMWorkerJournal{}
