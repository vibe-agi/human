package humantest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/workspace"
)

func testAgentStoreContextAndCallbackLifetime(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	var heldView agent.StoreView
	if err := store.View(ctx, func(view agent.StoreView) error {
		heldView = view
		return nil
	}); err != nil {
		t.Fatalf("capture View: %v", err)
	}
	if _, err := heldView.LoadTask(agent.TaskRef{}); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("View used after callback = %v, want ErrStoreClosed", err)
	}

	var heldTx agent.StoreTx
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		heldTx = tx
		return nil
	}); err != nil {
		t.Fatalf("capture Tx: %v", err)
	}
	if err := heldTx.InsertCommand(conformanceCommand("expired-tx", []byte("result"))); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("Tx used after callback = %v, want ErrStoreClosed", err)
	}

	var calls atomic.Int32
	if err := store.View(nil, func(agent.StoreView) error { calls.Add(1); return nil }); !errors.Is(err, agent.ErrInvalidArgument) {
		t.Fatalf("View nil context = %v, want ErrInvalidArgument", err)
	}
	if err := store.Update(nil, func(agent.StoreTx) error { calls.Add(1); return nil }); !errors.Is(err, agent.ErrInvalidArgument) {
		t.Fatalf("Update nil context = %v, want ErrInvalidArgument", err)
	}
	if err := store.View(ctx, nil); !errors.Is(err, agent.ErrInvalidArgument) {
		t.Fatalf("View nil callback = %v, want ErrInvalidArgument", err)
	}
	if err := store.Update(ctx, nil); !errors.Is(err, agent.ErrInvalidArgument) {
		t.Fatalf("Update nil callback = %v, want ErrInvalidArgument", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.View(cancelled, func(agent.StoreView) error { calls.Add(1); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("View canceled context = %v, want context.Canceled", err)
	}
	if err := store.Update(cancelled, func(agent.StoreTx) error { calls.Add(1); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Update canceled context = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("invalid/canceled operations invoked callback %d times", got)
	}
}

func testAgentStoreReleased(
	ctx context.Context,
	t *testing.T,
	store agent.Store,
	release framework.ReleaseFunc,
) {
	t.Helper()
	before := store.Description()
	if err := before.Validate(); err != nil {
		t.Fatalf("Description before release: %v", err)
	}
	if err := release(ctx); err != nil {
		t.Fatalf("release Agent Store: %v", err)
	}
	if err := release(ctx); err != nil {
		t.Fatalf("idempotent release Agent Store: %v", err)
	}
	after := store.Description()
	if err := after.Validate(); err != nil {
		t.Fatalf("Description after release: %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("Description changed after release:\nbefore: %#v\nafter:  %#v", before, after)
	}
	var calls atomic.Int32
	if err := store.View(ctx, func(agent.StoreView) error {
		calls.Add(1)
		return nil
	}); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("View after release = %v, want ErrStoreClosed", err)
	}
	if err := store.Update(ctx, func(agent.StoreTx) error {
		calls.Add(1)
		return nil
	}); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("Update after release = %v, want ErrStoreClosed", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("post-release operations invoked callback %d times, want zero", got)
	}
}

func testAgentStoreEmptyReadsAndLimits(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	ref := conformanceTask("missing-task", 1).Task.Ref
	artifact := agent.ArtifactRef{Workspace: ref.Workspace, ID: "missing-artifact"}
	err := store.View(ctx, func(view agent.StoreView) error {
		reads := []struct {
			name string
			read func() error
		}{
			{"command", func() error {
				_, err := view.LookupCommand(agent.StoreCommandKey{Authority: ref.Workspace.Authority, ID: "missing"}, generousReadLimit())
				return err
			}},
			{"task", func() error { _, err := view.LoadTask(ref); return err }},
			{"resolve_task", func() error { _, err := view.ResolveTask(ref.Workspace.Authority, ref.ID); return err }},
			{"message", func() error {
				_, err := view.LoadMessage(agent.StoreMessageKey{Authority: ref.Workspace.Authority, ID: "missing"}, generousReadLimit())
				return err
			}},
			{"artifact", func() error { _, err := view.LoadArtifact(artifact, generousReadLimit()); return err }},
			{"apply_receipt", func() error { _, err := view.LoadApplyReceipt(artifact); return err }},
			{"workspace_head", func() error { _, err := view.LoadWorkspaceHead(ref.Workspace); return err }},
			{"lease_grant", func() error { _, err := view.LoadLeaseGrant(ref, 1); return err }},
			{"latest_lease_grant", func() error { _, err := view.LoadLatestLeaseGrant(ref); return err }},
			{"claimable_task", func() error { _, err := view.FindClaimableTask(ref.Workspace.Authority); return err }},
		}
		for _, read := range reads {
			if err := read.read(); !errors.Is(err, agent.ErrStoreRecordNotFound) {
				return fmt.Errorf("empty %s read = %v, want ErrStoreRecordNotFound", read.name, err)
			}
		}
		contextTasks, err := view.ScanContextTasks(agent.StoreTaskContextScan{Context: agent.ContextRef{Authority: ref.Workspace.Authority, ID: "context"}, Limit: 1})
		if err != nil || len(contextTasks) != 0 {
			return fmt.Errorf("empty context scan = %#v, %v", contextTasks, err)
		}
		authorityTasks, err := view.ScanAuthorityTasks(agent.StoreTaskAuthorityScan{Authority: ref.Workspace.Authority, Limit: 1})
		if err != nil || len(authorityTasks.Records) != 0 || authorityTasks.TotalSize != 0 {
			return fmt.Errorf("empty authority scan = %#v, %v", authorityTasks, err)
		}
		messages, err := view.ScanMessages(agent.StoreMessageScan{Task: ref, Limit: 1, ReadLimit: generousReadLimit()})
		if err != nil || len(messages) != 0 {
			return fmt.Errorf("empty message scan = %#v, %v", messages, err)
		}
		events, err := view.ScanEvents(agent.StoreEventScan{Task: ref, Limit: 1})
		if err != nil || len(events) != 0 {
			return fmt.Errorf("empty event scan = %#v, %v", events, err)
		}
		leases, err := view.ScanLeases(agent.StoreLeaseScan{Authority: ref.Workspace.Authority, Worker: "worker", Limit: 1})
		if err != nil || len(leases) != 0 {
			return fmt.Errorf("empty lease scan = %#v, %v", leases, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(ctx, func(view agent.StoreView) error {
		invalid := []func() error{
			func() error { _, err := view.ScanContextTasks(agent.StoreTaskContextScan{Limit: 0}); return err },
			func() error {
				_, err := view.ScanAuthorityTasks(agent.StoreTaskAuthorityScan{Limit: agent.MaxPageSize + 2})
				return err
			},
			func() error {
				_, err := view.ScanMessages(agent.StoreMessageScan{Limit: 0, ReadLimit: generousReadLimit()})
				return err
			},
			func() error {
				_, err := view.ScanEvents(agent.StoreEventScan{Limit: agent.MaxPageSize + 2})
				return err
			},
			func() error { _, err := view.ScanLeases(agent.StoreLeaseScan{Limit: 0}); return err },
		}
		for index, scan := range invalid {
			if err := scan(); !errors.Is(err, agent.ErrInvalidArgument) {
				return fmt.Errorf("invalid scan %d = %v, want ErrInvalidArgument", index, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testAgentStoreTaskScans(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	contextRef := agent.ContextRef{Authority: "humantest-authority", ID: "scan-context"}
	records := []agent.StoreTaskRecord{
		conformanceTaskForScan("workspace-b", "task-b", contextRef, agent.TaskSubmitted, 1, 3),
		conformanceTaskForScan("workspace-a", "task-z", contextRef, agent.TaskWorking, 1, 3),
		conformanceTaskForScan("workspace-a", "task-a", contextRef, agent.TaskSubmitted, 2, 4),
		conformanceTaskForScan("workspace-c", "task-other-context", agent.ContextRef{Authority: contextRef.Authority, ID: "other-context"}, agent.TaskSubmitted, 1, 5),
		conformanceTaskForScan("workspace-x", "task-other-authority", agent.ContextRef{Authority: "other-authority", ID: contextRef.ID}, agent.TaskSubmitted, 1, 9),
	}
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		for _, record := range records {
			if err := tx.InsertTask(record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("insert scan tasks: %v", err)
	}

	err := store.View(ctx, func(view agent.StoreView) error {
		resolved, err := view.ResolveTask(contextRef.Authority, "task-a")
		if err != nil || resolved != records[2].Task.Ref {
			return fmt.Errorf("ResolveTask = %#v, %v; want %#v", resolved, err, records[2].Task.Ref)
		}
		first, err := view.ScanContextTasks(agent.StoreTaskContextScan{Context: contextRef, Limit: 2})
		if err != nil {
			return err
		}
		wantFirst := []agent.StoreTaskRecord{records[1], records[0]}
		if !reflect.DeepEqual(first, wantFirst) {
			return fmt.Errorf("first context page = %#v, want %#v", first, wantFirst)
		}
		cursor := agent.TaskPageCursor{CreatedAt: first[1].Task.CreatedAt, Workspace: first[1].Task.Ref.Workspace.ID, Task: first[1].Task.Ref.ID}
		second, err := view.ScanContextTasks(agent.StoreTaskContextScan{Context: contextRef, After: &cursor, Limit: 2})
		if err != nil || !reflect.DeepEqual(second, []agent.StoreTaskRecord{records[2]}) {
			return fmt.Errorf("second context page = %#v, %v", second, err)
		}

		page, err := view.ScanAuthorityTasks(agent.StoreTaskAuthorityScan{Authority: contextRef.Authority, Limit: 2})
		if err != nil {
			return err
		}
		wantAuthority := []agent.StoreTaskRecord{records[3], records[2]}
		if page.TotalSize != 4 || !reflect.DeepEqual(page.Records, wantAuthority) {
			return fmt.Errorf("first authority page = %#v, want records %#v total 4", page, wantAuthority)
		}
		after := agent.TaskQueryCursor{UpdatedAt: page.Records[1].Task.UpdatedAt, Workspace: page.Records[1].Task.Ref.Workspace.ID, Task: page.Records[1].Task.Ref.ID}
		page, err = view.ScanAuthorityTasks(agent.StoreTaskAuthorityScan{Authority: contextRef.Authority, After: &after, Limit: 3})
		if err != nil {
			return err
		}
		wantAuthority = []agent.StoreTaskRecord{records[1], records[0]}
		if page.TotalSize != 4 || !reflect.DeepEqual(page.Records, wantAuthority) {
			return fmt.Errorf("second authority page = %#v, want records %#v total 4", page, wantAuthority)
		}
		threshold := conformanceTime(3)
		filtered, err := view.ScanAuthorityTasks(agent.StoreTaskAuthorityScan{
			Authority: contextRef.Authority, Context: contextRef.ID, State: agent.TaskSubmitted,
			UpdatedAtOrAfter: &threshold, Limit: 5,
		})
		if err != nil || filtered.TotalSize != 2 || !reflect.DeepEqual(filtered.Records, []agent.StoreTaskRecord{records[2], records[0]}) {
			return fmt.Errorf("filtered authority page = %#v, %v", filtered, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testAgentStoreMessageAndEventScans(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	task := conformanceTask("stream-task", 1)
	messages := []agent.StoreMessageRecord{
		conformanceMessage(task.Task.Ref, "message-1", 1, agent.AuthorCaller, []byte("aa")),
		conformanceMessage(task.Task.Ref, "message-2", 2, agent.AuthorAgent, []byte("bb")),
		conformanceMessage(task.Task.Ref, "message-3", 3, agent.AuthorCaller, []byte("123456")),
	}
	events := []agent.StoreEventRecord{
		conformanceEvent(task.Task.Ref, 3, agent.EventCallerReplied, agent.TaskWorking),
		conformanceEvent(task.Task.Ref, 1, agent.EventTaskSubmitted, agent.TaskSubmitted),
		conformanceEvent(task.Task.Ref, 2, agent.EventTaskAccepted, agent.TaskWorking),
	}
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		for _, message := range messages {
			if err := tx.InsertMessage(message); err != nil {
				return err
			}
		}
		for _, event := range events {
			if err := tx.InsertEvent(event); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("insert message/event streams: %v", err)
	}

	err := store.View(ctx, func(view agent.StoreView) error {
		page, err := view.ScanMessages(agent.StoreMessageScan{
			Task: task.Task.Ref, Limit: 3, ReadLimit: agent.StoreReadLimit{MaxBytes: 5},
		})
		if err != nil || !reflect.DeepEqual(page, messages[:2]) {
			return fmt.Errorf("budgeted message prefix = %#v, %v; want %#v", page, err, messages[:2])
		}
		page, err = view.ScanMessages(agent.StoreMessageScan{
			Task: task.Task.Ref, After: 1, Limit: 1, ReadLimit: generousReadLimit(),
		})
		if err != nil || !reflect.DeepEqual(page, messages[1:2]) {
			return fmt.Errorf("paged messages = %#v, %v; want %#v", page, err, messages[1:2])
		}
		page[0].EncodedParts[0] ^= 0xff
		again, err := view.LoadMessage(agent.StoreMessageKey{Authority: task.Task.Ref.Workspace.Authority, ID: messages[1].ID}, generousReadLimit())
		if err != nil || !bytesEqual(again.EncodedParts, messages[1].EncodedParts) {
			return fmt.Errorf("message scan returned aliased bytes: %#v, %v", again, err)
		}
		if _, err := view.ScanMessages(agent.StoreMessageScan{
			Task: task.Task.Ref, After: 2, Limit: 1, ReadLimit: agent.StoreReadLimit{MaxBytes: 5},
		}); !errors.Is(err, agent.ErrStoreRecordTooLarge) {
			return fmt.Errorf("first oversized message = %v, want ErrStoreRecordTooLarge", err)
		}
		eventPage, err := view.ScanEvents(agent.StoreEventScan{Task: task.Task.Ref, After: 1, Limit: 2})
		wantEvents := []agent.StoreEventRecord{events[2], events[0]}
		if err != nil || !reflect.DeepEqual(eventPage, wantEvents) {
			return fmt.Errorf("event page = %#v, %v; want %#v", eventPage, err, wantEvents)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testAgentStoreLeases(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	authority := agent.AuthorityID("humantest-authority")
	first := conformanceTaskForScan("lease-b", "lease-b", agent.ContextRef{Authority: authority, ID: "leases"}, agent.TaskSubmitted, 1, 1)
	second := conformanceTaskForScan("lease-a", "lease-z", agent.ContextRef{Authority: authority, ID: "leases"}, agent.TaskSubmitted, 1, 1)
	otherWorker := conformanceTaskForScan("lease-c", "lease-c", agent.ContextRef{Authority: authority, ID: "leases"}, agent.TaskSubmitted, 1, 1)
	claimOld := conformanceTaskForScan("claim-b", "claim-b", agent.ContextRef{Authority: authority, ID: "claim"}, agent.TaskSubmitted, 2, 2)
	claimNew := conformanceTaskForScan("claim-a", "claim-a", agent.ContextRef{Authority: authority, ID: "claim"}, agent.TaskSubmitted, 3, 3)
	terminal := conformanceTaskForScan("terminal", "terminal", agent.ContextRef{Authority: authority, ID: "claim"}, agent.TaskCompleted, 1, 1)
	records := []agent.StoreTaskRecord{first, second, otherWorker, claimOld, claimNew, terminal}
	grants := []agent.StoreLeaseGrantRecord{
		{Grant: agent.LeaseGrant{Task: first.Task.Ref, Worker: "worker-a", Fence: 1}, GrantedAt: conformanceTime(10)},
		{Grant: agent.LeaseGrant{Task: first.Task.Ref, Worker: "worker-a", Fence: 2}, GrantedAt: conformanceTime(11)},
		{Grant: agent.LeaseGrant{Task: second.Task.Ref, Worker: "worker-a", Fence: 1}, GrantedAt: conformanceTime(11)},
		{Grant: agent.LeaseGrant{Task: otherWorker.Task.Ref, Worker: "worker-b", Fence: 1}, GrantedAt: conformanceTime(9)},
	}
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		for _, record := range records {
			if err := tx.InsertTask(record); err != nil {
				return err
			}
		}
		for _, grant := range grants {
			if err := tx.InsertLeaseGrant(grant); err != nil {
				return err
			}
		}
		lease := func(record agent.StoreTaskRecord, owner agent.WorkerID, fence agent.LeaseFence) error {
			next := record
			next.Task.Revision++
			next.Task.EventCount++
			next.Task.UpdatedAt = conformanceTime(12)
			next.Lease = agent.StoreLeaseState{Owner: owner, Fence: fence}
			changed, err := tx.CompareAndSwapTask(agent.StoreTaskMutation{
				Ref: record.Task.Ref, Condition: agent.StoreTaskCondition{ExpectedRevision: record.Task.Revision}, Next: next,
			})
			if err != nil {
				return err
			}
			if !changed {
				return errors.New("lease Task CAS returned false")
			}
			return nil
		}
		if err := lease(first, "worker-a", 2); err != nil {
			return err
		}
		if err := lease(second, "worker-a", 1); err != nil {
			return err
		}
		return lease(otherWorker, "worker-b", 1)
	}); err != nil {
		t.Fatalf("insert lease fixtures: %v", err)
	}

	err := store.View(ctx, func(view agent.StoreView) error {
		exact, err := view.LoadLeaseGrant(first.Task.Ref, 1)
		if err != nil || !reflect.DeepEqual(exact, grants[0]) {
			return fmt.Errorf("exact lease grant = %#v, %v; want %#v", exact, err, grants[0])
		}
		latest, err := view.LoadLatestLeaseGrant(first.Task.Ref)
		if err != nil || !reflect.DeepEqual(latest, grants[1]) {
			return fmt.Errorf("latest lease grant = %#v, %v; want %#v", latest, err, grants[1])
		}
		claimable, err := view.FindClaimableTask(authority)
		if err != nil || claimable.Task.Ref != claimOld.Task.Ref {
			return fmt.Errorf("claimable Task = %#v, %v; want %v", claimable, err, claimOld.Task.Ref)
		}
		page, err := view.ScanLeases(agent.StoreLeaseScan{Authority: authority, Worker: "worker-a", Limit: 1})
		if err != nil || len(page) != 1 || page[0].Grant != grants[2].Grant {
			return fmt.Errorf("first lease page = %#v, %v; want second Task", page, err)
		}
		cursor := agent.LeasePageCursor{GrantedAt: page[0].GrantedAt, Workspace: page[0].Task.Ref.Workspace.ID, Task: page[0].Task.Ref.ID, Fence: page[0].Grant.Fence}
		page, err = view.ScanLeases(agent.StoreLeaseScan{Authority: authority, Worker: "worker-a", After: &cursor, Limit: 2})
		if err != nil || len(page) != 1 || page[0].Grant != grants[1].Grant {
			return fmt.Errorf("second lease page = %#v, %v; want first Task latest grant", page, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testAgentStoreArtifactSubmissionReceipt(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	task := conformanceTask("artifact-flow-task", 1)
	initialMessage := conformanceMessage(task.Task.Ref, "artifact-initial-message", 1, agent.AuthorCaller, []byte("initial"))
	finalMessage := conformanceMessage(task.Task.Ref, "artifact-final-message", 2, agent.AuthorAgent, []byte("final"))
	artifact := conformanceArtifact(task.Task.Ref, "artifact-flow", []byte("opaque-artifact"))
	linkedTask := task
	linkedTask.Task.State = agent.TaskWorking
	linkedTask.Task.Revision = 2
	linkedTask.Task.EventCount = 2
	linkedTask.Task.UpdatedAt = conformanceTime(5)
	linkedTask.Task.Artifact = &artifact.Artifact.Ref
	frozen := agent.ArtifactFrozen
	linkedTask.ArtifactState = &frozen
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertMessage(initialMessage); err != nil {
			return err
		}
		if err := tx.InsertArtifact(artifact); err != nil {
			return err
		}
		changed, err := tx.CompareAndSwapTask(agent.StoreTaskMutation{
			Ref: task.Task.Ref, Condition: agent.StoreTaskCondition{ExpectedRevision: 1}, Next: linkedTask,
		})
		if err != nil {
			return err
		}
		if !changed {
			return errors.New("link Artifact Task CAS returned false")
		}
		return nil
	}); err != nil {
		t.Fatalf("insert Artifact flow fixtures: %v", err)
	}

	publishedAt := conformanceTime(8)
	submission := agent.Submission{
		ID: "artifact-submission", Task: task.Task.Ref, FinalMessage: finalMessage.ID,
		Artifact: &artifact.Artifact.Ref, PublishedAt: publishedAt,
	}
	completedTask := linkedTask
	completedTask.Task.State = agent.TaskCompleted
	completedTask.Task.Revision = 3
	completedTask.Task.EventCount = 3
	completedTask.Task.MessageCount = 2
	completedTask.Task.UpdatedAt = publishedAt
	completedTask.Task.Submission = &submission
	published := agent.ArtifactPublished
	completedTask.ArtifactState = &published
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		changed, err := tx.CompareAndSwapArtifact(agent.StoreArtifactMutation{
			Ref: artifact.Artifact.Ref, Task: artifact.Artifact.Task,
			ExpectedState: agent.ArtifactPublished, NextState: agent.ArtifactDiscarded,
			DiscardedAt: &publishedAt,
		})
		if err != nil || changed {
			return fmt.Errorf("stale Artifact CAS = %v, %v; want false, nil", changed, err)
		}
		changed, err = tx.CompareAndSwapArtifact(agent.StoreArtifactMutation{
			Ref: artifact.Artifact.Ref, Task: agent.TaskRef{Workspace: artifact.Artifact.Task.Workspace, ID: "wrong-task"},
			ExpectedState: agent.ArtifactFrozen, NextState: agent.ArtifactPublished, PublishedAt: &publishedAt,
		})
		if err != nil || changed {
			return fmt.Errorf("wrong-Task Artifact CAS = %v, %v; want false, nil", changed, err)
		}
		changed, err = tx.CompareAndSwapArtifact(agent.StoreArtifactMutation{
			Ref: artifact.Artifact.Ref, Task: artifact.Artifact.Task,
			ExpectedState: agent.ArtifactFrozen, NextState: agent.ArtifactPublished, PublishedAt: &publishedAt,
		})
		if err != nil || !changed {
			return fmt.Errorf("matching Artifact CAS = %v, %v; want true, nil", changed, err)
		}
		changed, err = tx.CompareAndSwapTask(agent.StoreTaskMutation{
			Ref: task.Task.Ref, Condition: agent.StoreTaskCondition{ExpectedRevision: 2}, Next: completedTask,
		})
		if err != nil || !changed {
			return fmt.Errorf("complete Artifact Task CAS = %v, %v; want true, nil", changed, err)
		}
		if err := tx.InsertMessage(finalMessage); err != nil {
			return err
		}
		if err := tx.InsertSubmission(agent.StoreSubmissionRecord{Submission: submission}); err != nil {
			return err
		}
		receipt := conformanceReceipt(artifact.Artifact, "artifact-receipt", workspace.ApplySuccess)
		return tx.InsertApplyReceipt(receipt)
	}); err != nil {
		t.Fatalf("publish Artifact, submission, and receipt: %v", err)
	}

	err := store.View(ctx, func(view agent.StoreView) error {
		stored, err := view.LoadArtifact(artifact.Artifact.Ref, generousReadLimit())
		if err != nil {
			return err
		}
		want := artifact
		want.Artifact.State = agent.ArtifactPublished
		want.Artifact.PublishedAt = &publishedAt
		if !reflect.DeepEqual(stored, want) {
			return fmt.Errorf("published Artifact = %#v, want %#v", stored, want)
		}
		storedTask, err := view.LoadTask(task.Task.Ref)
		if err != nil || !reflect.DeepEqual(storedTask, completedTask) {
			return fmt.Errorf("completed Task aggregate = %#v, %v; want %#v", storedTask, err, completedTask)
		}
		receipt, err := view.LoadApplyReceipt(artifact.Artifact.Ref)
		wantReceipt := conformanceReceipt(artifact.Artifact, "artifact-receipt", workspace.ApplySuccess)
		if err != nil || !reflect.DeepEqual(receipt, wantReceipt) {
			return fmt.Errorf("apply receipt = %#v, %v; want %#v", receipt, err, wantReceipt)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testAgentStoreWorkspaceHead(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	ref := agent.WorkspaceRef{Authority: "humantest-authority", ID: "head-workspace"}
	initial := agent.StoreWorkspaceHeadRecord{Head: agent.WorkspaceHead{Workspace: ref, ConfirmedRevision: "revision-1", UpdatedAt: conformanceTime(1)}}
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		inserted, err := tx.InsertWorkspaceHead(initial)
		if err != nil || !inserted {
			return fmt.Errorf("initial Workspace head insert = %v, %v", inserted, err)
		}
		inserted, err = tx.InsertWorkspaceHead(agent.StoreWorkspaceHeadRecord{Head: agent.WorkspaceHead{Workspace: ref, ConfirmedRevision: "replacement", UpdatedAt: conformanceTime(2)}})
		if err != nil || inserted {
			return fmt.Errorf("duplicate Workspace head insert = %v, %v", inserted, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	next := agent.StoreWorkspaceHeadRecord{Head: agent.WorkspaceHead{Workspace: ref, ConfirmedRevision: "revision-2", UpdatedAt: conformanceTime(2)}}
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		changed, err := tx.CompareAndSwapWorkspaceHead(agent.StoreWorkspaceHeadMutation{Workspace: ref, ExpectedRevision: "stale", Next: next})
		if err != nil || changed {
			return fmt.Errorf("stale Workspace CAS = %v, %v", changed, err)
		}
		changed, err = tx.CompareAndSwapWorkspaceHead(agent.StoreWorkspaceHeadMutation{Workspace: ref, ExpectedRevision: initial.Head.ConfirmedRevision, Next: next})
		if err != nil || !changed {
			return fmt.Errorf("matching Workspace CAS = %v, %v", changed, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err := store.View(ctx, func(view agent.StoreView) error {
		got, err := view.LoadWorkspaceHead(ref)
		if err != nil || !reflect.DeepEqual(got, next) {
			return fmt.Errorf("Workspace head = %#v, %v; want %#v", got, err, next)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testAgentStoreLogicalConstraints(ctx context.Context, t *testing.T, store agent.Store) {
	t.Helper()
	command := conformanceCommand("constraint-command", []byte("result"))
	task := conformanceTask("constraint-task", 1)
	message := conformanceMessage(task.Task.Ref, "constraint-message", 1, agent.AuthorCaller, []byte("message"))
	event := conformanceEvent(task.Task.Ref, 1, agent.EventTaskSubmitted, agent.TaskSubmitted)
	grant := agent.StoreLeaseGrantRecord{Grant: agent.LeaseGrant{Task: task.Task.Ref, Worker: "worker", Fence: 1}, GrantedAt: conformanceTime(2)}
	artifact := conformanceArtifact(task.Task.Ref, "constraint-artifact", []byte("artifact"))
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		if err := tx.InsertCommand(command); err != nil {
			return err
		}
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		if err := tx.InsertMessage(message); err != nil {
			return err
		}
		if err := tx.InsertEvent(event); err != nil {
			return err
		}
		if err := tx.InsertLeaseGrant(grant); err != nil {
			return err
		}
		return tx.InsertArtifact(artifact)
	}); err != nil {
		t.Fatalf("insert constraint fixtures: %v", err)
	}

	assertUpdateConflict := func(label string, want agent.StoreConstraint, update func(agent.StoreTx) error) {
		t.Helper()
		err := store.Update(ctx, update)
		assertAgentStoreConstraint(t, label, err, want)
	}
	assertUpdateConflict("command ID", agent.StoreConstraintCommandID, func(tx agent.StoreTx) error { return tx.InsertCommand(command) })
	assertUpdateConflict("Task key", agent.StoreConstraintTaskKey, func(tx agent.StoreTx) error { return tx.InsertTask(task) })
	publicCollision := conformanceTask("constraint-task", 1)
	publicCollision.Task.Ref.Workspace.ID = "other-workspace"
	assertUpdateConflict("public Task ID", agent.StoreConstraintPublicTaskID, func(tx agent.StoreTx) error { return tx.InsertTask(publicCollision) })
	assertUpdateConflict("message ID", agent.StoreConstraintMessageID, func(tx agent.StoreTx) error { return tx.InsertMessage(message) })
	sequenceCollision := conformanceMessage(task.Task.Ref, "other-message", 1, agent.AuthorCaller, []byte("other"))
	assertUpdateConflict("message sequence", agent.StoreConstraintMessageSequence, func(tx agent.StoreTx) error { return tx.InsertMessage(sequenceCollision) })
	assertUpdateConflict("event sequence", agent.StoreConstraintEventSequence, func(tx agent.StoreTx) error { return tx.InsertEvent(event) })
	assertUpdateConflict("lease fence", agent.StoreConstraintLeaseFence, func(tx agent.StoreTx) error { return tx.InsertLeaseGrant(grant) })
	assertUpdateConflict("Artifact ID", agent.StoreConstraintArtifactID, func(tx agent.StoreTx) error { return tx.InsertArtifact(artifact) })
	artifactTaskCollision := conformanceArtifact(task.Task.Ref, "other-artifact", []byte("other"))
	assertUpdateConflict("Artifact Task", agent.StoreConstraintArtifactTask, func(tx agent.StoreTx) error { return tx.InsertArtifact(artifactTaskCollision) })

	otherTask := conformanceTask("constraint-other-task", 1)
	otherTask.Task.Ref.Workspace.ID = "constraint-other-workspace"
	otherMessage := conformanceMessage(otherTask.Task.Ref, "constraint-other-message", 1, agent.AuthorCaller, []byte("other"))
	if err := store.Update(ctx, func(tx agent.StoreTx) error {
		if err := tx.InsertTask(otherTask); err != nil {
			return err
		}
		return tx.InsertMessage(otherMessage)
	}); err != nil {
		t.Fatal(err)
	}
	artifactIDCollision := conformanceArtifact(otherTask.Task.Ref, artifact.Artifact.Ref.ID, []byte("other"))
	assertUpdateConflict("authority Artifact ID", agent.StoreConstraintArtifactID, func(tx agent.StoreTx) error { return tx.InsertArtifact(artifactIDCollision) })

	submission := agent.StoreSubmissionRecord{Submission: agent.Submission{ID: "constraint-submission", Task: task.Task.Ref, FinalMessage: message.ID, PublishedAt: conformanceTime(3)}}
	if err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertSubmission(submission) }); err != nil {
		t.Fatal(err)
	}
	assertUpdateConflict("submission ID", agent.StoreConstraintSubmissionID, func(tx agent.StoreTx) error { return tx.InsertSubmission(submission) })

	receipt := conformanceReceipt(artifact.Artifact, "constraint-receipt", workspace.ApplyConflict)
	if err := store.Update(ctx, func(tx agent.StoreTx) error { return tx.InsertApplyReceipt(receipt) }); err != nil {
		t.Fatal(err)
	}
	assertUpdateConflict("receipt ID", agent.StoreConstraintReceiptID, func(tx agent.StoreTx) error { return tx.InsertApplyReceipt(receipt) })
}

func conformanceTaskForScan(
	workspaceID agent.WorkspaceID,
	taskID agent.TaskID,
	contextRef agent.ContextRef,
	state agent.TaskState,
	createdSecond int64,
	updatedSecond int64,
) agent.StoreTaskRecord {
	return agent.StoreTaskRecord{Task: agent.Task{
		Ref:     agent.TaskRef{Workspace: agent.WorkspaceRef{Authority: contextRef.Authority, ID: workspaceID}, ID: taskID},
		Context: contextRef, State: state, Revision: 1, MessageCount: 1, EventCount: 1,
		CreatedAt: conformanceTime(createdSecond), UpdatedAt: conformanceTime(updatedSecond),
	}}
}

func conformanceMessage(ref agent.TaskRef, id agent.MessageID, sequence uint64, author agent.Author, encoded []byte) agent.StoreMessageRecord {
	return agent.StoreMessageRecord{
		ID: id, Task: ref, Sequence: sequence, Author: author,
		EncodedParts: append([]byte(nil), encoded...), PartsDigest: agent.StoreDigest(fmt.Sprintf("digest-%s", id)),
		CreatedAt: conformanceTime(int64(sequence) + 1),
	}
}

func conformanceEvent(ref agent.TaskRef, sequence uint64, eventType agent.EventType, state agent.TaskState) agent.StoreEventRecord {
	return agent.StoreEventRecord{Event: agent.Event{
		Task: ref, Sequence: sequence, Type: eventType, State: state, Revision: sequence,
		OccurredAt: conformanceTime(int64(sequence) + 1),
	}}
}

func conformanceArtifact(ref agent.TaskRef, id agent.ArtifactID, payload []byte) agent.StoreArtifactRecord {
	return agent.StoreArtifactRecord{Artifact: agent.Artifact{
		Ref: agent.ArtifactRef{Workspace: ref.Workspace, ID: id}, Task: ref,
		State: agent.ArtifactFrozen, BaseRevision: "base-revision", ResultRevision: workspace.Revision("result-" + string(id)),
		Digest: workspace.Digest("artifact-digest-" + string(id)), PayloadDigest: workspace.Digest("payload-digest-" + string(id)),
		PayloadSize: int64(len(payload)), MediaType: "application/octet-stream", FrozenAt: conformanceTime(4),
	}, EncodedPayload: append([]byte(nil), payload...)}
}

func conformanceReceipt(artifact agent.Artifact, id agent.ApplyReceiptID, decision workspace.ApplyDecision) agent.StoreApplyReceiptRecord {
	return agent.StoreApplyReceiptRecord{Receipt: agent.ApplyReceipt{
		ID: id, Artifact: artifact.Ref, ArtifactDigest: artifact.Digest,
		BaseRevision: artifact.BaseRevision, ResultRevision: artifact.ResultRevision,
		Decision: decision, ObservedRevision: artifact.ResultRevision,
		Code: "humantest", Message: "conformance", RecordedAt: conformanceTime(9),
	}}
}

func assertAgentStoreConstraint(t *testing.T, label string, err error, want agent.StoreConstraint) {
	t.Helper()
	if !errors.Is(err, agent.ErrStoreConflict) {
		t.Fatalf("%s conflict = %v, want ErrStoreConflict", label, err)
	}
	var conflict *agent.StoreConflictError
	if !errors.As(err, &conflict) || conflict.Constraint != want {
		t.Fatalf("%s conflict = %#v, want constraint %q", label, conflict, want)
	}
}

func bytesEqual(left, right []byte) bool {
	return reflect.DeepEqual(left, right)
}
