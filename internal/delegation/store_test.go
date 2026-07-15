package delegation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
)

func TestLifecyclePersistsOpaqueArtifactAndMonotonicEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))

	created, err := store.CreateTask(ctx, CreateTaskInput{
		ID: "task-1", CallerID: "caller-1", Metadata: []byte(`{"project":"alpha"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, created.Task, StateSubmitted, 1, 0, 1)
	if created.Event.Sequence != created.Task.Revision || created.Event.Kind != EventTaskSubmitted {
		t.Fatalf("create event = %+v", created.Event)
	}

	accepted, err := store.AcceptTask(ctx, AcceptTaskInput{
		CommandInput: command(created.Task, "accepted"), WorkerID: "worker-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, accepted.Task, StateWorking, 2, 0, 1)
	if accepted.Task.WorkerID != "worker-1" {
		t.Fatalf("worker id = %q", accepted.Task.WorkerID)
	}

	patch := []byte("opaque cumulative patch v1")
	metadata := []byte(`{"base_commit":"abc","turn":1}`)
	deliveryInput := DeliverTurnInput{
		CommandInput:      command(accepted.Task, "first delivery"),
		ArtifactID:        "artifact-1",
		ArtifactMediaType: "application/vnd.git.patch",
		ArtifactData:      patch,
		ArtifactMetadata:  metadata,
	}
	delivered, err := store.DeliverTurn(ctx, deliveryInput)
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, delivered.Task, StateInputRequired, 3, 1, 2)
	if delivered.Turn.Number != 1 || delivered.Event.TurnNumber == nil || *delivered.Event.TurnNumber != 1 {
		t.Fatalf("delivery turn/event = %+v / %+v", delivered.Turn, delivered.Event)
	}
	digest := sha256.Sum256(patch)
	if delivered.Artifact.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("artifact digest = %q", delivered.Artifact.SHA256)
	}
	if _, err := store.DeliverTurn(ctx, deliveryInput); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("replayed delivery error = %v", err)
	}
	if turns, err := store.ListTurns(ctx, created.Task.ID); err != nil || len(turns) != 1 {
		t.Fatalf("turns after replay = %+v, err=%v", turns, err)
	}
	// The authority owns its bytes; caller mutation after return cannot rewrite
	// the stored artifact or metadata.
	patch[0] = 'X'
	metadata[2] = 'X'
	storedArtifact, err := store.GetArtifact(ctx, "task-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(storedArtifact.Data) != "opaque cumulative patch v1" ||
		string(storedArtifact.Metadata) != `{"base_commit":"abc","turn":1}` {
		t.Fatalf("stored opaque artifact mutated: data=%q metadata=%q",
			storedArtifact.Data, storedArtifact.Metadata)
	}

	replied, err := store.Reply(ctx, command(delivered.Task, "please continue"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.DeliverTurn(ctx, DeliverTurnInput{
		CommandInput:      command(replied.Task, "second delivery"),
		ArtifactID:        "artifact-2",
		ArtifactMediaType: "application/octet-stream",
		ArtifactData:      []byte("opaque cumulative patch v2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := store.CompleteTask(ctx, command(second.Task, "done"))
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, completed.Task, StateCompleted, 6, 2, 3)

	events, err := store.ListEvents(ctx, "task-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	wantKinds := []EventKind{
		EventTaskSubmitted, EventTaskAccepted, EventTurnDelivered,
		EventCallerReplied, EventTurnDelivered, EventTaskCompleted,
	}
	if len(events) != len(wantKinds) {
		t.Fatalf("events len = %d, want %d: %+v", len(events), len(wantKinds), events)
	}
	for i, event := range events {
		if event.Sequence != int64(i+1) || event.Kind != wantKinds[i] {
			t.Fatalf("event[%d] = %+v, want sequence=%d kind=%q", i, event, i+1, wantKinds[i])
		}
	}
	if recoverable, err := store.ListRecoverableTasks(ctx); err != nil || len(recoverable) != 0 {
		t.Fatalf("recoverable terminal tasks = %+v, err=%v", recoverable, err)
	}
	if _, err := store.Reply(ctx, command(completed.Task, "too late")); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal Reply() error = %v", err)
	}
}

func TestConfirmedRewindSupersedesMonotonicallyAndNeverReusesTurns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	task := createAcceptedTask(t, ctx, store, "task-rewind")

	for turn := int64(1); turn <= 3; turn++ {
		delivery, err := store.DeliverTurn(ctx, DeliverTurnInput{
			CommandInput:      command(task, "deliver"),
			ArtifactID:        "artifact-" + string(rune('0'+turn)),
			ArtifactMediaType: "application/octet-stream",
			ArtifactData:      []byte{byte(turn)},
		})
		if err != nil {
			t.Fatalf("deliver turn %d: %v", turn, err)
		}
		task = delivery.Task
		if turn < 3 {
			replied, err := store.Reply(ctx, command(task, "continue"))
			if err != nil {
				t.Fatal(err)
			}
			task = replied.Task
		}
	}

	requested, err := store.RequestRewind(ctx, RequestRewindInput{
		CommandInput: command(task, "rewind to one"), TargetTurn: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, requested.Task, StateRewindPending, 8, 3, 4)
	if requested.Task.PendingRewindTo == nil || *requested.Task.PendingRewindTo != 1 {
		t.Fatalf("pending rewind = %+v", requested.Task.PendingRewindTo)
	}
	if _, err := store.DeliverTurn(ctx, DeliverTurnInput{
		CommandInput: command(requested.Task, "invalid"), ArtifactID: "invalid",
		ArtifactMediaType: "application/octet-stream",
	}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("delivery while rewind pending error = %v", err)
	}

	confirmed, err := store.ConfirmRewind(ctx, command(requested.Task, "confirmed"))
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, confirmed.Task, StateInputRequired, 9, 1, 4)
	assertSupersededTurns(t, ctx, store, task.ID, map[int64]int64{2: 9, 3: 9})
	latest, err := store.LatestArtifact(ctx, task.ID)
	if err != nil || latest.TurnNumber != 1 {
		t.Fatalf("latest artifact after rewind = %+v, err=%v", latest, err)
	}

	replied, err := store.Reply(ctx, command(confirmed.Task, "branch"))
	if err != nil {
		t.Fatal(err)
	}
	branched, err := store.DeliverTurn(ctx, DeliverTurnInput{
		CommandInput:      command(replied.Task, "new branch"),
		ArtifactID:        "artifact-4",
		ArtifactMediaType: "application/octet-stream",
		ArtifactData:      []byte{4},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, branched.Task, StateInputRequired, 11, 4, 5)

	if _, err := store.RequestRewind(ctx, RequestRewindInput{
		CommandInput: command(branched.Task, "illegal old branch"), TargetTurn: 2,
	}); !errors.Is(err, ErrInvalidRewind) {
		t.Fatalf("rewind to superseded turn error = %v", err)
	}
	afterInvalid, err := store.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, afterInvalid, StateInputRequired, 11, 4, 5)

	toBase, err := store.RequestRewind(ctx, RequestRewindInput{
		CommandInput: command(afterInvalid, "rewind to base"), TargetTurn: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	baseConfirmed, err := store.ConfirmRewind(ctx, command(toBase.Task, "confirmed"))
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, baseConfirmed.Task, StateInputRequired, 13, 0, 5)
	// Earlier supersession revisions are never overwritten; newly removed live
	// turns receive the revision of this rewind.
	assertSupersededTurns(t, ctx, store, task.ID, map[int64]int64{1: 13, 2: 9, 3: 9, 4: 13})
	if _, err := store.LatestArtifact(ctx, task.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LatestArtifact() at base error = %v", err)
	}

	events, err := store.ListEvents(ctx, task.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != int(baseConfirmed.Task.Revision) {
		t.Fatalf("event count = %d, revision = %d", len(events), baseConfirmed.Task.Revision)
	}
	for i, event := range events {
		if event.Sequence != int64(i+1) {
			t.Fatalf("event sequence gap at %d: %+v", i, event)
		}
	}
}

func TestArtifactConflictRollsBackTurnTaskAndEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	first := createAcceptedTask(t, ctx, store, "task-a")
	if _, err := store.DeliverTurn(ctx, DeliverTurnInput{
		CommandInput: command(first, "first"), ArtifactID: "shared-artifact-id",
		ArtifactMediaType: "application/octet-stream",
	}); err != nil {
		t.Fatal(err)
	}

	second := createAcceptedTask(t, ctx, store, "task-b")
	if _, err := store.DeliverTurn(ctx, DeliverTurnInput{
		CommandInput: command(second, "collision"), ArtifactID: "shared-artifact-id",
		ArtifactMediaType: "application/octet-stream",
	}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate artifact error = %v", err)
	}

	unchanged, err := store.GetTask(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, unchanged, StateWorking, 2, 0, 1)
	turns, err := store.ListTurns(ctx, second.ID)
	if err != nil || len(turns) != 0 {
		t.Fatalf("rolled-back turns = %+v, err=%v", turns, err)
	}
	events, err := store.ListEvents(ctx, second.ID, 0)
	if err != nil || len(events) != 2 {
		t.Fatalf("rolled-back events = %+v, err=%v", events, err)
	}

	success, err := store.DeliverTurn(ctx, DeliverTurnInput{
		CommandInput: command(unchanged, "retry"), ArtifactID: "task-b-artifact",
		ArtifactMediaType: "application/octet-stream",
	})
	if err != nil {
		t.Fatal(err)
	}
	if success.Turn.Number != 1 {
		t.Fatalf("turn number consumed by rolled-back transaction: %d", success.Turn.Number)
	}
}

func TestRejectedRewindPreservesAnchorAndLiveHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	task := createAcceptedTask(t, ctx, store, "task-reject-rewind")
	for turn := 1; turn <= 2; turn++ {
		delivery, err := store.DeliverTurn(ctx, DeliverTurnInput{
			CommandInput: command(task, "deliver"), ArtifactID: "reject-artifact-" + string(rune('0'+turn)),
			ArtifactMediaType: "application/octet-stream",
		})
		if err != nil {
			t.Fatal(err)
		}
		task = delivery.Task
		if turn == 1 {
			replied, err := store.Reply(ctx, command(task, "continue"))
			if err != nil {
				t.Fatal(err)
			}
			task = replied.Task
		}
	}
	requested, err := store.RequestRewind(ctx, RequestRewindInput{
		CommandInput: command(task, "request base"), TargetTurn: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := store.RejectRewind(ctx, command(requested.Task, "side effects prevent rewind"))
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, rejected.Task, StateInputRequired, 7, 2, 3)
	if rejected.Task.PendingRewindTo != nil || rejected.Event.Kind != EventRewindRejected {
		t.Fatalf("rejected rewind result = %+v", rejected)
	}
	turns, err := store.ListTurns(ctx, task.ID)
	if err != nil || len(turns) != 2 || turns[0].Superseded() || turns[1].Superseded() {
		t.Fatalf("turns after rejected rewind = %+v, err=%v", turns, err)
	}
	latest, err := store.LatestArtifact(ctx, task.ID)
	if err != nil || latest.TurnNumber != 2 {
		t.Fatalf("latest after rejected rewind = %+v, err=%v", latest, err)
	}
	canceled, err := store.CancelTask(ctx, command(rejected.Task, "caller canceled"))
	if err != nil {
		t.Fatal(err)
	}
	if !canceled.Task.State.IsTerminal() || canceled.Task.State != StateCanceled {
		t.Fatalf("canceled task = %+v", canceled.Task)
	}
}

func TestRejectAndFailAreTerminal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))

	created, err := store.CreateTask(ctx, CreateTaskInput{ID: "task-rejected", CallerID: "caller"})
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := store.RejectTask(ctx, command(created.Task, "cannot accept"))
	if err != nil {
		t.Fatal(err)
	}
	if rejected.Task.State != StateRejected || !rejected.Task.State.IsTerminal() {
		t.Fatalf("rejected task = %+v", rejected.Task)
	}

	working := createAcceptedTask(t, ctx, store, "task-failed")
	failed, err := store.FailTask(ctx, command(working, "unachievable"))
	if err != nil {
		t.Fatal(err)
	}
	if failed.Task.State != StateFailed || !failed.Task.State.IsTerminal() ||
		failed.Event.Kind != EventTaskFailed {
		t.Fatalf("failed task = %+v / %+v", failed.Task, failed.Event)
	}
	if State("unknown").Valid() || State("unknown").IsTerminal() {
		t.Fatal("unknown state reported as valid or terminal")
	}
}

func TestPendingRewindRecoversAcrossReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "delegation.db")
	store, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	task := createAcceptedTask(t, ctx, store, "task-recover")
	for turn := 1; turn <= 2; turn++ {
		delivery, err := store.DeliverTurn(ctx, DeliverTurnInput{
			CommandInput: command(task, "deliver"), ArtifactID: "recover-artifact-" + string(rune('0'+turn)),
			ArtifactMediaType: "application/octet-stream",
		})
		if err != nil {
			t.Fatal(err)
		}
		task = delivery.Task
		if turn == 1 {
			replied, err := store.Reply(ctx, command(task, "continue"))
			if err != nil {
				t.Fatal(err)
			}
			task = replied.Task
		}
	}
	pending, err := store.RequestRewind(ctx, RequestRewindInput{
		CommandInput: command(task, "base please"), TargetTurn: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recoverable, err := reopened.ListRecoverableTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 1 || recoverable[0].State != StateRewindPending ||
		recoverable[0].PendingRewindTo == nil || *recoverable[0].PendingRewindTo != 0 ||
		recoverable[0].Revision != pending.Task.Revision {
		t.Fatalf("recovered tasks = %+v", recoverable)
	}
	snapshot, err := reopened.LoadSnapshot(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Task.Revision != pending.Task.Revision || len(snapshot.Turns) != 2 ||
		len(snapshot.Artifacts) != 2 || len(snapshot.Events) != int(pending.Task.Revision) {
		t.Fatalf("recovery snapshot = %+v", snapshot)
	}
	confirmed, err := reopened.ConfirmRewind(ctx, command(recoverable[0], "resume confirm"))
	if err != nil {
		t.Fatal(err)
	}
	assertTask(t, confirmed.Task, StateInputRequired, 7, 0, 3)
	assertSupersededTurns(t, ctx, reopened, task.ID, map[int64]int64{1: 7, 2: 7})
	events, err := reopened.ListEvents(ctx, task.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 7 || events[6].Kind != EventRewindConfirmed {
		t.Fatalf("events after recovery = %+v", events)
	}
}

func TestExpectedRevisionAllowsOnlyOneRacingCommand(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	created, err := store.CreateTask(ctx, CreateTaskInput{ID: "task-race", CallerID: "caller"})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errorsCh := make(chan error, 2)
	go func() {
		<-start
		_, err := store.AcceptTask(ctx, AcceptTaskInput{
			CommandInput: command(created.Task, "accept"), WorkerID: "worker",
		})
		errorsCh <- err
	}()
	go func() {
		<-start
		_, err := store.RejectTask(ctx, command(created.Task, "reject"))
		errorsCh <- err
	}()
	close(start)
	err1, err2 := <-errorsCh, <-errorsCh
	successes, conflicts := 0, 0
	for _, err := range []error{err1, err2} {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrRevisionConflict):
			conflicts++
		default:
			t.Fatalf("racing command error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("racing results: successes=%d conflicts=%d errors=(%v, %v)",
			successes, conflicts, err1, err2)
	}
	task, err := store.GetTask(ctx, created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Revision != 2 || (task.State != StateWorking && task.State != StateRejected) {
		t.Fatalf("task after race = %+v", task)
	}
	events, err := store.ListEvents(ctx, task.ID, 0)
	if err != nil || len(events) != 2 {
		t.Fatalf("events after race = %+v, err=%v", events, err)
	}
}

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := OpenSQLite(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func createAcceptedTask(t *testing.T, ctx context.Context, store *Store, taskID string) Task {
	t.Helper()
	created, err := store.CreateTask(ctx, CreateTaskInput{ID: taskID, CallerID: "caller"})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := store.AcceptTask(ctx, AcceptTaskInput{
		CommandInput: command(created.Task, "accept"), WorkerID: "worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	return accepted.Task
}

func command(task Task, data string) CommandInput {
	return CommandInput{TaskID: task.ID, ExpectedRevision: task.Revision, Data: []byte(data)}
}

func assertTask(t *testing.T, task Task, state State, revision, latest, next int64) {
	t.Helper()
	if task.State != state || task.Revision != revision || task.LatestTurn != latest || task.NextTurn != next {
		t.Fatalf("task = %+v, want state=%q revision=%d latest=%d next=%d",
			task, state, revision, latest, next)
	}
}

func assertSupersededTurns(
	t *testing.T,
	ctx context.Context,
	store *Store,
	taskID string,
	want map[int64]int64,
) {
	t.Helper()
	turns, err := store.ListTurns(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	for _, turn := range turns {
		revision, ok := want[turn.Number]
		artifact, err := store.GetArtifact(ctx, turn.TaskID, turn.Number)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			if turn.Superseded() || artifact.Superseded() {
				t.Fatalf("turn/artifact %d unexpectedly superseded at %+v/%+v",
					turn.Number, turn.SupersededAtRevision, artifact.SupersededAtRevision)
			}
			continue
		}
		if turn.SupersededAtRevision == nil || *turn.SupersededAtRevision != revision {
			t.Fatalf("turn %d supersession = %+v, want %d", turn.Number, turn.SupersededAtRevision, revision)
		}
		if artifact.SupersededAtRevision == nil || *artifact.SupersededAtRevision != revision {
			t.Fatalf("artifact %d supersession = %+v, want %d",
				turn.Number, artifact.SupersededAtRevision, revision)
		}
	}
}
