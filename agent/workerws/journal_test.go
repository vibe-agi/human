package workerws

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

type memoryJournal struct {
	mu                         sync.Mutex
	next                       JournalSequence
	closed                     bool
	binding                    *JournalBinding
	assignments                map[agent.WorkerDeliveryID]*memoryAssignment
	events                     map[agent.WorkerDeliveryID]*memoryEvent
	rejections                 map[agent.WorkerDeliveryID]JournalRejection
	confirmedRejections        map[agent.WorkerDeliveryID]JournalDigest
	putAssignmentCommitUnknown bool
	settleCommitUnknown        bool
}

type memoryAssignment struct {
	digest  JournalDigest
	pending *JournalAssignment
}

type memoryEvent struct {
	digest        JournalDigest
	pending       *JournalEvent
	receiptDigest JournalDigest
}

func newMemoryJournal() *memoryJournal {
	return &memoryJournal{
		assignments:         make(map[agent.WorkerDeliveryID]*memoryAssignment),
		events:              make(map[agent.WorkerDeliveryID]*memoryEvent),
		rejections:          make(map[agent.WorkerDeliveryID]JournalRejection),
		confirmedRejections: make(map[agent.WorkerDeliveryID]JournalDigest),
	}
}

func (*memoryJournal) Description() JournalDescription {
	return JournalDescription{
		Contract: framework.Contract{ID: JournalContractID, Major: JournalContractMajor},
		Provider: "test-memory", Version: "1",
	}
}

func (journal *memoryJournal) Bind(ctx context.Context, binding JournalBinding) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if journal.closed {
		return ErrJournalClosed
	}
	if err := binding.Validate(); err != nil {
		return errors.Join(ErrJournalCorrupt, err)
	}
	if journal.binding == nil {
		copy := binding
		journal.binding = &copy
		return nil
	}
	if *journal.binding != binding {
		return ErrJournalConflict
	}
	return nil
}

func (journal *memoryJournal) PutAssignment(ctx context.Context, record JournalAssignment) (JournalEntryState, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return "", err
	}
	id := record.Delivery.ID
	if current := journal.assignments[id]; current != nil {
		if current.digest != record.Digest {
			return "", ErrJournalConflict
		}
		if current.pending == nil {
			return JournalEntrySettled, nil
		}
		return JournalEntryPending, nil
	}
	journal.next++
	record.Sequence = journal.next
	record.Delivery = agent.CloneWorkerAssignmentDelivery(record.Delivery)
	journal.assignments[id] = &memoryAssignment{digest: record.Digest, pending: &record}
	if journal.putAssignmentCommitUnknown {
		journal.putAssignmentCommitUnknown = false
		return "", ErrJournalCommitUnknown
	}
	return JournalEntryPending, nil
}

func (journal *memoryJournal) ConfirmAssignment(ctx context.Context, id agent.WorkerDeliveryID) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return err
	}
	current := journal.assignments[id]
	if current == nil {
		return ErrJournalNotFound
	}
	current.pending = nil
	return nil
}

func (journal *memoryJournal) ListAssignments(ctx context.Context, after JournalSequence, limit int) ([]JournalAssignment, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateJournalLimit(limit); err != nil {
		return nil, err
	}
	result := make([]JournalAssignment, 0)
	for _, current := range journal.assignments {
		if current.pending != nil && current.pending.Sequence > after {
			record := *current.pending
			record.Delivery = agent.CloneWorkerAssignmentDelivery(record.Delivery)
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, k int) bool { return result[i].Sequence < result[k].Sequence })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (journal *memoryJournal) PutEvent(ctx context.Context, record JournalEvent) (JournalEntryState, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return "", err
	}
	id := record.Delivery.ID
	if current := journal.events[id]; current != nil {
		if current.digest != record.Digest {
			return "", ErrJournalConflict
		}
		if current.pending == nil {
			return JournalEntrySettled, nil
		}
		return JournalEntryPending, nil
	}
	journal.next++
	record.Sequence = journal.next
	record.Delivery = agent.CloneWorkerEventDelivery(record.Delivery)
	journal.events[id] = &memoryEvent{digest: record.Digest, pending: &record}
	return JournalEntryPending, nil
}

func (journal *memoryJournal) ListEvents(ctx context.Context, after JournalSequence, limit int) ([]JournalEvent, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateJournalLimit(limit); err != nil {
		return nil, err
	}
	result := make([]JournalEvent, 0)
	for _, current := range journal.events {
		if current.pending != nil && current.pending.Sequence > after {
			record := *current.pending
			record.Delivery = agent.CloneWorkerEventDelivery(record.Delivery)
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, k int) bool { return result[i].Sequence < result[k].Sequence })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (journal *memoryJournal) SettleEvent(
	ctx context.Context,
	receipt agent.WorkerEventReceipt,
	eventDigest JournalDigest,
	receiptDigest JournalDigest,
) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return err
	}
	current := journal.events[receipt.Delivery]
	if current == nil {
		return ErrJournalNotFound
	}
	if current.digest != eventDigest {
		return ErrJournalConflict
	}
	if current.pending == nil {
		if current.receiptDigest != receiptDigest {
			return ErrJournalConflict
		}
		return nil
	}
	if err := receipt.ValidateFor(current.pending.Delivery); err != nil {
		return errors.Join(ErrJournalCorrupt, err)
	}
	delivery := agent.CloneWorkerEventDelivery(current.pending.Delivery)
	current.pending = nil
	current.receiptDigest = receiptDigest
	if receipt.Decision == agent.WorkerEventNACK {
		journal.next++
		journal.rejections[receipt.Delivery] = JournalRejection{
			Sequence: journal.next, EventDigest: eventDigest, ReceiptDigest: receiptDigest,
			RejectedEvent: RejectedEvent{Delivery: delivery, Receipt: receipt},
		}
	}
	if journal.settleCommitUnknown {
		journal.settleCommitUnknown = false
		return ErrJournalCommitUnknown
	}
	return nil
}

func (journal *memoryJournal) ListRejections(ctx context.Context, after JournalSequence, limit int) ([]JournalRejection, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateJournalLimit(limit); err != nil {
		return nil, err
	}
	result := make([]JournalRejection, 0)
	for _, record := range journal.rejections {
		if record.Sequence > after {
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, k int) bool { return result[i].Sequence < result[k].Sequence })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (journal *memoryJournal) ConfirmRejection(ctx context.Context, id agent.WorkerDeliveryID) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ready(ctx); err != nil {
		return err
	}
	record, ok := journal.rejections[id]
	if !ok {
		if _, confirmed := journal.confirmedRejections[id]; confirmed {
			return nil
		}
		return ErrJournalNotFound
	}
	delete(journal.rejections, id)
	journal.confirmedRejections[id] = record.ReceiptDigest
	return nil
}

func (journal *memoryJournal) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if journal.closed {
		return ErrJournalClosed
	}
	if journal.binding == nil {
		return ErrJournalCorrupt
	}
	return nil
}

func (journal *memoryJournal) close() {
	journal.mu.Lock()
	journal.closed = true
	journal.mu.Unlock()
}

var _ Journal = (*memoryJournal)(nil)
