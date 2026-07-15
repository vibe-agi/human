package delegation

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestCreateTaskWithMessageIsCallerScopedAndIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	message := MessageInput{ID: "message-1", Role: "user", Data: []byte(`{"text":"help"}`)}

	created, err := store.CreateTaskWithMessage(ctx, CreateTaskInput{
		ID: "task-original", CallerID: "caller-a", ContextID: "context-a",
		Metadata: []byte(`{"request":"one"}`),
	}, message)
	if err != nil {
		t.Fatal(err)
	}
	if created.Replay || created.Task.State != StateSubmitted || created.Task.Revision != 1 {
		t.Fatalf("created result = %+v", created)
	}

	replayed, err := store.CreateTaskWithMessage(ctx, CreateTaskInput{
		ID: "different-provisional-id", CallerID: "caller-a", ContextID: "different-context",
	}, message)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replay || replayed.Task.ID != created.Task.ID ||
		replayed.Task.ContextID != "context-a" || replayed.Task.Revision != 1 {
		t.Fatalf("replayed result = %+v", replayed)
	}
	if _, err := store.CreateTaskWithMessage(ctx, CreateTaskInput{
		ID: "task-conflict", CallerID: "caller-a",
	}, MessageInput{ID: message.ID, Role: message.Role, Data: []byte("different")}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("mismatched replay error = %v", err)
	}

	// The key is scoped to the caller, so another caller can use the same
	// protocol message ID without gaining access to caller-a's task.
	other, err := store.CreateTaskWithMessage(ctx, CreateTaskInput{
		ID: "task-other", CallerID: "caller-b",
	}, message)
	if err != nil {
		t.Fatal(err)
	}
	if other.Task.ID != "task-other" || other.Replay {
		t.Fatalf("other caller result = %+v", other)
	}
}

func TestAppendMessageResumesAndStaleExactRetryDoesNotDuplicate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	created, err := store.CreateTaskWithMessage(ctx, CreateTaskInput{
		ID: "task", CallerID: "caller", ContextID: "context",
	}, MessageInput{ID: "message-1", Role: "user", Data: []byte("initial")})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := store.AcceptTask(ctx, AcceptTaskInput{
		CommandInput: CommandInput{TaskID: created.Task.ID, ExpectedRevision: 1}, WorkerID: "worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	delivered, err := store.DeliverTurn(ctx, DeliverTurnInput{
		CommandInput: CommandInput{TaskID: created.Task.ID, ExpectedRevision: accepted.Task.Revision},
		ArtifactID:   "artifact-1", ArtifactMediaType: "text/plain", ArtifactData: []byte("result"),
	})
	if err != nil {
		t.Fatal(err)
	}
	input := MessageInput{ID: "message-2", Role: "user", Data: []byte("continue")}
	appended, err := store.AppendMessage(ctx, CommandInput{
		TaskID: created.Task.ID, ExpectedRevision: delivered.Task.Revision,
	}, input)
	if err != nil {
		t.Fatal(err)
	}
	if appended.Task.State != StateWorking || appended.Event.Kind != EventCallerReplied || appended.Replay {
		t.Fatalf("append result = %+v", appended)
	}

	// Exact idempotency is checked before revision CAS so a lost successful
	// response can be retried with the original, now-stale revision.
	replayed, err := store.AppendMessage(ctx, CommandInput{
		TaskID: created.Task.ID, ExpectedRevision: delivered.Task.Revision,
	}, input)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replay || replayed.Task.Revision != appended.Task.Revision {
		t.Fatalf("append replay = %+v", replayed)
	}
	snapshot, err := store.LoadSnapshot(ctx, created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Messages) != 2 || len(snapshot.Events) != int(snapshot.Task.Revision) {
		t.Fatalf("snapshot messages/events = %d/%d at revision %d",
			len(snapshot.Messages), len(snapshot.Events), snapshot.Task.Revision)
	}
}

func TestConcurrentInitialMessageCreatesExactlyOneTask(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "delegation.db"))
	message := MessageInput{ID: "message-race", Role: "user", Data: []byte("same")}
	start := make(chan struct{})
	results := make(chan MessageResult, 2)
	errorsCh := make(chan error, 2)
	var wait sync.WaitGroup
	for _, taskID := range []string{"task-a", "task-b"} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := store.CreateTaskWithMessage(ctx, CreateTaskInput{
				ID: taskID, CallerID: "caller",
			}, message)
			results <- result
			errorsCh <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var taskID string
	replays := 0
	for result := range results {
		if taskID == "" {
			taskID = result.Task.ID
		} else if result.Task.ID != taskID {
			t.Fatalf("racing task IDs = %q and %q", taskID, result.Task.ID)
		}
		if result.Replay {
			replays++
		}
	}
	if replays != 1 {
		t.Fatalf("replay count = %d, want 1", replays)
	}
	tasks, err := store.ListTasks(ctx, "caller")
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks = %+v, err=%v", tasks, err)
	}
}
