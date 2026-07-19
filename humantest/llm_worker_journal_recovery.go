package humantest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/workerws"
)

// LLMWorkerJournalRecoveryOpener opens one live Journal handle onto the same
// durable domain on every call. A caller must release the current handle before
// opening the next one. Implementations commonly close and reopen one database
// path; semantic fakes can reopen a MemoryLLMWorkerJournalImage.
type LLMWorkerJournalRecoveryOpener func(
	context.Context,
) (workerws.Journal, framework.ReleaseFunc, error)

// LLMWorkerJournalRecoveryFactory creates a fresh durable domain for one
// recovery subtest and returns an opener for that domain. The setup context and
// testing handle must not be retained by the returned opener.
type LLMWorkerJournalRecoveryFactory func(
	context.Context,
	testing.TB,
) (LLMWorkerJournalRecoveryOpener, error)

// TestLLMWorkerJournalRecovery runs the mandatory handle release/reopen matrix
// for a HumanLLM worker inbox/outbox Journal. The exact same scenarios are
// intended for official adapters, third-party adapters, and semantic fakes:
// assignment ACK loss, event receipt loss, and NACK poison settlement without
// blocking FIFO followers.
//
// Every restart releases the old handle before reopening the same durable
// domain. This suite verifies recovered contents, conflicts, and sequence
// continuity; it cannot prove that an adapter did not flush only during
// Release. Durable adapters must add a test-owned process kill/exit test.
func TestLLMWorkerJournalRecovery(t *testing.T, factory LLMWorkerJournalRecoveryFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("HumanLLM worker Journal recovery factory is nil")
	}
	tests := []struct {
		name string
		run  func(context.Context, *testing.T, *llmWorkerJournalRecoveryHarness)
	}{
		{"assignment_ack_loss_reopen_replays_exactly", testLLMJournalAssignmentRecovery},
		{"event_receipt_loss_reopen_replays_exactly", testLLMJournalEventRecovery},
		{"nack_poison_settlement_preserves_follower_fifo", testLLMJournalNACKRecovery},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			t.Cleanup(cancel)
			opener, err := factory(ctx, t)
			if err != nil {
				t.Fatalf("create HumanLLM worker Journal recovery domain: %v", err)
			}
			if opener == nil {
				t.Fatal("factory returned a nil HumanLLM worker Journal recovery opener")
			}
			harness := &llmWorkerJournalRecoveryHarness{t: t, ctx: ctx, open: opener}
			harness.reopen(false)
			t.Cleanup(harness.cleanup)
			bindLLMConformanceJournal(ctx, t, harness.journal)
			test.run(ctx, t, harness)
		})
	}
}

type llmWorkerJournalRecoveryHarness struct {
	t       *testing.T
	ctx     context.Context
	open    LLMWorkerJournalRecoveryOpener
	journal workerws.Journal
	release framework.ReleaseFunc
}

func (harness *llmWorkerJournalRecoveryHarness) reopen(requireBinding bool) {
	harness.t.Helper()
	if harness.release != nil {
		harness.releaseCurrent()
	}
	journal, release, err := harness.open(harness.ctx)
	if err != nil {
		harness.t.Fatalf("reopen HumanLLM worker Journal recovery domain: %v", err)
	}
	if journal == nil || release == nil {
		if release != nil {
			_ = release(context.Background())
		}
		harness.t.Fatal("recovery opener returned a nil HumanLLM worker Journal or release function")
	}
	harness.journal, harness.release = journal, release
	if requireBinding {
		// Listing without rebinding proves that binding belongs to the durable
		// image rather than the discarded process-local handle.
		if _, err := journal.ListAssignments(harness.ctx, 0, 1); err != nil {
			harness.t.Fatalf("durable HumanLLM worker Journal binding after reopen: %v", err)
		}
		if err := journal.Bind(harness.ctx, llmConformanceJournalBinding()); err != nil {
			harness.t.Fatalf("exact HumanLLM worker Journal rebind after reopen: %v", err)
		}
	}
}

func (harness *llmWorkerJournalRecoveryHarness) releaseCurrent() {
	harness.t.Helper()
	release := harness.release
	harness.release = nil
	harness.journal = nil
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := release(ctx); err != nil {
		harness.t.Fatalf("release HumanLLM worker Journal before reopen: %v", err)
	}
}

func (harness *llmWorkerJournalRecoveryHarness) cleanup() {
	if harness.release == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := harness.release(ctx); err != nil {
		harness.t.Errorf("release HumanLLM worker Journal recovery handle: %v", err)
	}
	harness.release = nil
	harness.journal = nil
}

func testLLMJournalAssignmentRecovery(
	ctx context.Context,
	t *testing.T,
	harness *llmWorkerJournalRecoveryHarness,
) {
	record := llmConformanceJournalAssignment("assignment-ack-loss")
	if state, err := harness.journal.PutAssignment(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutAssignment before lost ACK = (%q, %v), want pending", state, err)
	}
	want := mustLLMRecoveryAssignments(ctx, t, harness.journal, 1)
	harness.reopen(true)
	got := mustLLMRecoveryAssignments(ctx, t, harness.journal, 1)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assignment after ACK-loss reopen = %#v, want exact %#v", got, want)
	}
	divergent := record
	divergent.Delivery = llm.CloneWorkerAssignmentDelivery(record.Delivery)
	divergent.Delivery.Assignment.Request.Metadata["revision"] = "changed"
	divergent.Digest = mustLLMJournalDigest(t, divergent.Delivery)
	if _, err := harness.journal.PutAssignment(ctx, divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent assignment after reopen = %v, want ErrJournalConflict", err)
	}
	if state, err := harness.journal.PutAssignment(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact assignment retry after reopen = (%q, %v), want pending", state, err)
	}
	follower := llmConformanceJournalAssignment("assignment-after-reopen")
	if state, err := harness.journal.PutAssignment(ctx, follower); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("new assignment after reopen = (%q, %v), want pending", state, err)
	}
	withFollower := mustLLMRecoveryAssignments(ctx, t, harness.journal, 2)
	if withFollower[1].Sequence <= want[0].Sequence || !reflect.DeepEqual(withFollower[1].Delivery, follower.Delivery) {
		t.Fatalf("assignment sequence after reopen = %#v, want exact follower after sequence %d", withFollower[1], want[0].Sequence)
	}
	if err := harness.journal.ConfirmAssignment(ctx, record.Delivery.ID); err != nil {
		t.Fatalf("ConfirmAssignment after replay = %v", err)
	}
	if err := harness.journal.ConfirmAssignment(ctx, follower.Delivery.ID); err != nil {
		t.Fatalf("ConfirmAssignment follower after reopen = %v", err)
	}
	harness.reopen(true)
	mustLLMRecoveryAssignments(ctx, t, harness.journal, 0)
	if state, err := harness.journal.PutAssignment(ctx, record); err != nil || state != workerws.JournalEntrySettled {
		t.Fatalf("settled assignment retry after reopen = (%q, %v), want settled", state, err)
	}
	if _, err := harness.journal.PutAssignment(ctx, divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent settled assignment after reopen = %v, want ErrJournalConflict", err)
	}
}

func testLLMJournalEventRecovery(
	ctx context.Context,
	t *testing.T,
	harness *llmWorkerJournalRecoveryHarness,
) {
	record := llmConformanceJournalEvent("event-receipt-loss", llm.EventProgress, nil)
	if state, err := harness.journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutEvent before lost receipt = (%q, %v), want pending", state, err)
	}
	want := mustLLMRecoveryEvents(ctx, t, harness.journal, 1)
	harness.reopen(true)
	got := mustLLMRecoveryEvents(ctx, t, harness.journal, 1)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event after receipt-loss reopen = %#v, want exact %#v", got, want)
	}
	divergent := record
	divergent.Delivery = llm.CloneWorkerEventDelivery(record.Delivery)
	divergent.Delivery.Event.ID += "-changed"
	divergent.Digest = mustLLMJournalDigest(t, divergent.Delivery)
	if _, err := harness.journal.PutEvent(ctx, divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent event after reopen = %v, want ErrJournalConflict", err)
	}
	if state, err := harness.journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact event retry after reopen = (%q, %v), want pending", state, err)
	}
	follower := llmConformanceJournalEvent("event-after-reopen", llm.EventProgress, nil)
	if state, err := harness.journal.PutEvent(ctx, follower); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("new event after reopen = (%q, %v), want pending", state, err)
	}
	withFollower := mustLLMRecoveryEvents(ctx, t, harness.journal, 2)
	if withFollower[1].Sequence <= want[0].Sequence || !reflect.DeepEqual(withFollower[1].Delivery, follower.Delivery) {
		t.Fatalf("event sequence after reopen = %#v, want exact follower after sequence %d", withFollower[1], want[0].Sequence)
	}
	receipt := llmConformanceJournalReceipt(record.Delivery, llm.WorkerEventACK)
	receiptDigest := mustLLMJournalDigest(t, receipt)
	if err := harness.journal.SettleEvent(ctx, receipt, record.Digest, receiptDigest); err != nil {
		t.Fatalf("SettleEvent ACK after replay = %v", err)
	}
	followerReceipt := llmConformanceJournalReceipt(follower.Delivery, llm.WorkerEventACK)
	if err := harness.journal.SettleEvent(
		ctx, followerReceipt, follower.Digest, mustLLMJournalDigest(t, followerReceipt),
	); err != nil {
		t.Fatalf("SettleEvent follower after reopen = %v", err)
	}
	harness.reopen(true)
	mustLLMRecoveryEvents(ctx, t, harness.journal, 0)
	if state, err := harness.journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntrySettled {
		t.Fatalf("settled event retry after reopen = (%q, %v), want settled", state, err)
	}
	if err := harness.journal.SettleEvent(ctx, receipt, record.Digest, receiptDigest); err != nil {
		t.Fatalf("exact ACK receipt retry after reopen = %v", err)
	}
	if _, err := harness.journal.PutEvent(ctx, divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent settled event after reopen = %v, want ErrJournalConflict", err)
	}
}

func testLLMJournalNACKRecovery(
	ctx context.Context,
	t *testing.T,
	harness *llmWorkerJournalRecoveryHarness,
) {
	poison := llmConformanceJournalEvent("event-poison", llm.EventProgress, nil)
	followerA := llmConformanceJournalEvent("event-follower-a", llm.EventProgress, nil)
	followerB := llmConformanceJournalEvent("event-follower-b", llm.EventProgress, nil)
	for _, record := range []workerws.JournalEvent{poison, followerA, followerB} {
		if state, err := harness.journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
			t.Fatalf("PutEvent %q = (%q, %v), want pending", record.Delivery.ID, state, err)
		}
	}
	receipt := llmConformanceJournalReceipt(poison.Delivery, llm.WorkerEventNACK)
	if err := harness.journal.SettleEvent(
		ctx, receipt, poison.Digest, mustLLMJournalDigest(t, receipt),
	); err != nil {
		t.Fatalf("settle poison NACK = %v", err)
	}
	wantEvents := mustLLMRecoveryEvents(ctx, t, harness.journal, 2)
	wantRejections, err := harness.journal.ListRejections(ctx, 0, 10)
	if err != nil || len(wantRejections) != 1 {
		t.Fatalf("ListRejections before poison reopen = %#v, %v", wantRejections, err)
	}
	harness.reopen(true)
	events := mustLLMRecoveryEvents(ctx, t, harness.journal, 2)
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("followers after poison NACK = %#v, want exact FIFO %#v", events, wantEvents)
	}
	rejections, err := harness.journal.ListRejections(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ListRejections after poison NACK reopen: %v", err)
	}
	if !reflect.DeepEqual(rejections, wantRejections) {
		t.Fatalf("poison rejection after reopen = %#v, want exact %#v", rejections, wantRejections)
	}
	if state, err := harness.journal.PutEvent(ctx, poison); err != nil || state != workerws.JournalEntrySettled {
		t.Fatalf("poison exact retry after NACK = (%q, %v), want settled", state, err)
	}
	ackA := llmConformanceJournalReceipt(followerA.Delivery, llm.WorkerEventACK)
	if err := harness.journal.SettleEvent(ctx, ackA, followerA.Digest, mustLLMJournalDigest(t, ackA)); err != nil {
		t.Fatalf("settle first follower = %v", err)
	}
	if err := harness.journal.ConfirmRejection(ctx, poison.Delivery.ID); err != nil {
		t.Fatalf("confirm poison rejection = %v", err)
	}
	harness.reopen(true)
	events = mustLLMRecoveryEvents(ctx, t, harness.journal, 1)
	if events[0].Delivery.ID != followerB.Delivery.ID {
		t.Fatalf("remaining follower after reopen = %q, want %q", events[0].Delivery.ID, followerB.Delivery.ID)
	}
	rejections, err = harness.journal.ListRejections(ctx, 0, 10)
	if err != nil || len(rejections) != 0 {
		t.Fatalf("rejections after confirmation = %#v, %v, want empty", rejections, err)
	}
	ackB := llmConformanceJournalReceipt(followerB.Delivery, llm.WorkerEventACK)
	if err := harness.journal.SettleEvent(ctx, ackB, followerB.Digest, mustLLMJournalDigest(t, ackB)); err != nil {
		t.Fatalf("settle second follower = %v", err)
	}
	harness.reopen(true)
	mustLLMRecoveryEvents(ctx, t, harness.journal, 0)
}

func mustLLMRecoveryAssignments(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	want int,
) []workerws.JournalAssignment {
	t.Helper()
	records, err := journal.ListAssignments(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ListAssignments during recovery: %v", err)
	}
	if len(records) != want {
		t.Fatalf("pending assignments = %d, want %d: %#v", len(records), want, records)
	}
	return records
}

func mustLLMRecoveryEvents(
	ctx context.Context,
	t *testing.T,
	journal workerws.Journal,
	want int,
) []workerws.JournalEvent {
	t.Helper()
	records, err := journal.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ListEvents during recovery: %v", err)
	}
	if len(records) != want {
		t.Fatalf("pending events = %d, want %d: %#v", len(records), want, records)
	}
	return records
}
