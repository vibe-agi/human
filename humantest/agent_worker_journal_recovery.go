package humantest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/agent/workerws"
	"github.com/vibe-agi/human/framework"
)

// AgentWorkerJournalRecoveryOpener opens one live Journal handle onto the same
// durable domain on every call. A caller must release the current handle before
// opening the next one. Implementations commonly close and reopen one database
// path; semantic fakes can reopen a MemoryAgentWorkerJournalImage.
type AgentWorkerJournalRecoveryOpener func(
	context.Context,
) (workerws.Journal, framework.ReleaseFunc, error)

// AgentWorkerJournalRecoveryFactory creates a fresh durable domain for one
// recovery subtest and returns an opener for that domain. The setup context and
// testing handle must not be retained by the returned opener.
type AgentWorkerJournalRecoveryFactory func(
	context.Context,
	testing.TB,
) (AgentWorkerJournalRecoveryOpener, error)

// TestAgentWorkerJournalRecovery runs the mandatory handle release/reopen
// matrix for an Agent worker inbox/outbox Journal. The exact same scenarios are
// intended for official adapters, third-party adapters, and semantic fakes:
// assignment ACK loss, event receipt loss, and NACK poison settlement without
// blocking FIFO followers.
//
// Every restart releases the old handle before reopening the same durable
// domain. This suite verifies recovered contents, conflicts, and sequence
// continuity; it cannot prove that an adapter did not flush only during
// Release. Durable adapters must add a test-owned process kill/exit test.
func TestAgentWorkerJournalRecovery(t *testing.T, factory AgentWorkerJournalRecoveryFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("Agent worker Journal recovery factory is nil")
	}
	tests := []struct {
		name string
		run  func(context.Context, *testing.T, *agentWorkerJournalRecoveryHarness)
	}{
		{"assignment_ack_loss_reopen_replays_exactly", testAgentJournalAssignmentRecovery},
		{"event_receipt_loss_reopen_replays_exactly", testAgentJournalEventRecovery},
		{"nack_poison_settlement_preserves_follower_fifo", testAgentJournalNACKRecovery},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			t.Cleanup(cancel)
			opener, err := factory(ctx, t)
			if err != nil {
				t.Fatalf("create Agent worker Journal recovery domain: %v", err)
			}
			if opener == nil {
				t.Fatal("factory returned a nil Agent worker Journal recovery opener")
			}
			harness := &agentWorkerJournalRecoveryHarness{t: t, ctx: ctx, open: opener}
			harness.reopen(false)
			t.Cleanup(harness.cleanup)
			bindConformanceJournal(ctx, t, harness.journal)
			test.run(ctx, t, harness)
		})
	}
}

type agentWorkerJournalRecoveryHarness struct {
	t       *testing.T
	ctx     context.Context
	open    AgentWorkerJournalRecoveryOpener
	journal workerws.Journal
	release framework.ReleaseFunc
}

func (harness *agentWorkerJournalRecoveryHarness) reopen(requireBinding bool) {
	harness.t.Helper()
	if harness.release != nil {
		harness.releaseCurrent()
	}
	journal, release, err := harness.open(harness.ctx)
	if err != nil {
		harness.t.Fatalf("reopen Agent worker Journal recovery domain: %v", err)
	}
	if journal == nil || release == nil {
		if release != nil {
			_ = release(context.Background())
		}
		harness.t.Fatal("recovery opener returned a nil Agent worker Journal or release function")
	}
	harness.journal, harness.release = journal, release
	if requireBinding {
		// Listing without rebinding proves that binding belongs to the durable
		// image rather than the discarded process-local handle.
		if _, err := journal.ListAssignments(harness.ctx, 0, 1); err != nil {
			harness.t.Fatalf("durable Agent worker Journal binding after reopen: %v", err)
		}
		if err := journal.Bind(harness.ctx, conformanceJournalBinding()); err != nil {
			harness.t.Fatalf("exact Agent worker Journal rebind after reopen: %v", err)
		}
	}
}

func (harness *agentWorkerJournalRecoveryHarness) releaseCurrent() {
	harness.t.Helper()
	release := harness.release
	harness.release = nil
	harness.journal = nil
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := release(ctx); err != nil {
		harness.t.Fatalf("release Agent worker Journal before reopen: %v", err)
	}
}

func (harness *agentWorkerJournalRecoveryHarness) cleanup() {
	if harness.release == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := harness.release(ctx); err != nil {
		harness.t.Errorf("release Agent worker Journal recovery handle: %v", err)
	}
	harness.release = nil
	harness.journal = nil
}

func testAgentJournalAssignmentRecovery(
	ctx context.Context,
	t *testing.T,
	harness *agentWorkerJournalRecoveryHarness,
) {
	record := conformanceJournalAssignment("assignment-ack-loss")
	if state, err := harness.journal.PutAssignment(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutAssignment before lost ACK = (%q, %v), want pending", state, err)
	}
	want := mustAgentRecoveryAssignments(ctx, t, harness.journal, 1)
	harness.reopen(true)
	got := mustAgentRecoveryAssignments(ctx, t, harness.journal, 1)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assignment after ACK-loss reopen = %#v, want exact %#v", got, want)
	}
	divergent := record
	divergent.Delivery = agent.CloneWorkerAssignmentDelivery(record.Delivery)
	divergent.Delivery.Assignment.Task.Revision++
	divergent.Delivery.Assignment.Task.EventCount++
	divergent.Digest = mustJournalDigest(t, divergent.Delivery)
	if _, err := harness.journal.PutAssignment(ctx, divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent assignment after reopen = %v, want ErrJournalConflict", err)
	}
	if state, err := harness.journal.PutAssignment(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact assignment retry after reopen = (%q, %v), want pending", state, err)
	}
	follower := conformanceJournalAssignment("assignment-after-reopen")
	if state, err := harness.journal.PutAssignment(ctx, follower); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("new assignment after reopen = (%q, %v), want pending", state, err)
	}
	withFollower := mustAgentRecoveryAssignments(ctx, t, harness.journal, 2)
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
	mustAgentRecoveryAssignments(ctx, t, harness.journal, 0)
	if state, err := harness.journal.PutAssignment(ctx, record); err != nil || state != workerws.JournalEntrySettled {
		t.Fatalf("settled assignment retry after reopen = (%q, %v), want settled", state, err)
	}
	if _, err := harness.journal.PutAssignment(ctx, divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent settled assignment after reopen = %v, want ErrJournalConflict", err)
	}
}

func testAgentJournalEventRecovery(
	ctx context.Context,
	t *testing.T,
	harness *agentWorkerJournalRecoveryHarness,
) {
	record := conformanceJournalEvent("event-receipt-loss", agent.WorkerEventAcceptTask, nil)
	if state, err := harness.journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("PutEvent before lost receipt = (%q, %v), want pending", state, err)
	}
	want := mustAgentRecoveryEvents(ctx, t, harness.journal, 1)
	harness.reopen(true)
	got := mustAgentRecoveryEvents(ctx, t, harness.journal, 1)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event after receipt-loss reopen = %#v, want exact %#v", got, want)
	}
	divergent := record
	divergent.Delivery = agent.CloneWorkerEventDelivery(record.Delivery)
	divergent.Delivery.Event.ID += "-changed"
	divergent.Digest = mustJournalDigest(t, divergent.Delivery)
	if _, err := harness.journal.PutEvent(ctx, divergent); !errors.Is(err, workerws.ErrJournalConflict) {
		t.Fatalf("divergent event after reopen = %v, want ErrJournalConflict", err)
	}
	if state, err := harness.journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("exact event retry after reopen = (%q, %v), want pending", state, err)
	}
	follower := conformanceJournalEvent("event-after-reopen", agent.WorkerEventAcceptTask, nil)
	if state, err := harness.journal.PutEvent(ctx, follower); err != nil || state != workerws.JournalEntryPending {
		t.Fatalf("new event after reopen = (%q, %v), want pending", state, err)
	}
	withFollower := mustAgentRecoveryEvents(ctx, t, harness.journal, 2)
	if withFollower[1].Sequence <= want[0].Sequence || !reflect.DeepEqual(withFollower[1].Delivery, follower.Delivery) {
		t.Fatalf("event sequence after reopen = %#v, want exact follower after sequence %d", withFollower[1], want[0].Sequence)
	}
	receipt := conformanceJournalReceipt(record.Delivery, agent.WorkerEventACK)
	receiptDigest := mustJournalDigest(t, receipt)
	if err := harness.journal.SettleEvent(ctx, receipt, record.Digest, receiptDigest); err != nil {
		t.Fatalf("SettleEvent ACK after replay = %v", err)
	}
	followerReceipt := conformanceJournalReceipt(follower.Delivery, agent.WorkerEventACK)
	if err := harness.journal.SettleEvent(
		ctx, followerReceipt, follower.Digest, mustJournalDigest(t, followerReceipt),
	); err != nil {
		t.Fatalf("SettleEvent follower after reopen = %v", err)
	}
	harness.reopen(true)
	mustAgentRecoveryEvents(ctx, t, harness.journal, 0)
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

func testAgentJournalNACKRecovery(
	ctx context.Context,
	t *testing.T,
	harness *agentWorkerJournalRecoveryHarness,
) {
	poison := conformanceJournalEvent("event-poison", agent.WorkerEventAcceptTask, nil)
	followerA := conformanceJournalEvent("event-follower-a", agent.WorkerEventAcceptTask, nil)
	followerB := conformanceJournalEvent("event-follower-b", agent.WorkerEventAcceptTask, nil)
	for _, record := range []workerws.JournalEvent{poison, followerA, followerB} {
		if state, err := harness.journal.PutEvent(ctx, record); err != nil || state != workerws.JournalEntryPending {
			t.Fatalf("PutEvent %q = (%q, %v), want pending", record.Delivery.ID, state, err)
		}
	}
	receipt := conformanceJournalReceipt(poison.Delivery, agent.WorkerEventNACK)
	if err := harness.journal.SettleEvent(
		ctx, receipt, poison.Digest, mustJournalDigest(t, receipt),
	); err != nil {
		t.Fatalf("settle poison NACK = %v", err)
	}
	wantEvents := mustAgentRecoveryEvents(ctx, t, harness.journal, 2)
	wantRejections, err := harness.journal.ListRejections(ctx, 0, 10)
	if err != nil || len(wantRejections) != 1 {
		t.Fatalf("ListRejections before poison reopen = %#v, %v", wantRejections, err)
	}
	harness.reopen(true)
	events := mustAgentRecoveryEvents(ctx, t, harness.journal, 2)
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
	ackA := conformanceJournalReceipt(followerA.Delivery, agent.WorkerEventACK)
	if err := harness.journal.SettleEvent(ctx, ackA, followerA.Digest, mustJournalDigest(t, ackA)); err != nil {
		t.Fatalf("settle first follower = %v", err)
	}
	if err := harness.journal.ConfirmRejection(ctx, poison.Delivery.ID); err != nil {
		t.Fatalf("confirm poison rejection = %v", err)
	}
	harness.reopen(true)
	events = mustAgentRecoveryEvents(ctx, t, harness.journal, 1)
	if events[0].Delivery.ID != followerB.Delivery.ID {
		t.Fatalf("remaining follower after reopen = %q, want %q", events[0].Delivery.ID, followerB.Delivery.ID)
	}
	rejections, err = harness.journal.ListRejections(ctx, 0, 10)
	if err != nil || len(rejections) != 0 {
		t.Fatalf("rejections after confirmation = %#v, %v, want empty", rejections, err)
	}
	ackB := conformanceJournalReceipt(followerB.Delivery, agent.WorkerEventACK)
	if err := harness.journal.SettleEvent(ctx, ackB, followerB.Digest, mustJournalDigest(t, ackB)); err != nil {
		t.Fatalf("settle second follower = %v", err)
	}
	harness.reopen(true)
	mustAgentRecoveryEvents(ctx, t, harness.journal, 0)
}

func mustAgentRecoveryAssignments(
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

func mustAgentRecoveryEvents(
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
