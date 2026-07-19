package agent_test

import (
	"context"
	"database/sql"
	"errors"
	. "github.com/vibe-agi/human/agent"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	agentsqlite "github.com/vibe-agi/human/agent/sqlite"
)

type testAgent struct {
	*Agent
	store    Store
	database *sql.DB
	now      func() time.Time
}

func openTestAgent(t *testing.T) (*testAgent, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent.db")
	return openTestAgentWithConfig(t, path, DefaultConfig()), path
}

func openTestAgentWithConfig(t *testing.T, path string, config Config) *testAgent {
	t.Helper()
	resource, err := agentsqlite.Open(t.Context(), agentsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		_ = resource.Release(context.Background())
		t.Fatal(err)
	}
	wrapper := &testAgent{store: store, now: time.Now}
	config.Store = resource
	config.Clock = func() time.Time { return wrapper.now() }
	service, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	wrapper.Agent = service
	wrapper.database, err = sql.Open("sqlite", path)
	if err != nil {
		_ = service.Close()
		t.Fatal(err)
	}
	wrapper.database.SetMaxOpenConns(1)
	wrapper.database.SetMaxIdleConns(1)
	if _, err := wrapper.database.ExecContext(t.Context(), "PRAGMA foreign_keys = ON"); err != nil {
		_ = service.Close()
		_ = wrapper.database.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := wrapper.Agent.Close(); err != nil {
			t.Errorf("close Agent: %v", err)
		}
		if err := wrapper.database.Close(); err != nil {
			t.Errorf("close Agent test database: %v", err)
		}
	})
	return wrapper
}

func openSQLiteStoreBridge(t *testing.T) Store {
	t.Helper()
	resource, err := agentsqlite.Open(t.Context(), agentsqlite.Config{
		Path: filepath.Join(t.TempDir(), "agent-store.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		_ = resource.Release(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := resource.Release(context.Background()); err != nil {
			t.Errorf("release Agent test Store: %v", err)
		}
	})
	return store
}

func refs(authority, contextID, workspaceID, taskID string) (ContextRef, TaskRef) {
	authorityID := AuthorityID(authority)
	return ContextRef{Authority: authorityID, ID: ContextID(contextID)}, TaskRef{
		Workspace: WorkspaceRef{Authority: authorityID, ID: WorkspaceID(workspaceID)},
		ID:        TaskID(taskID),
	}
}

func textMessage(id, text string) MessageInput {
	return MessageInput{ID: MessageID(id), Parts: []Part{{MediaType: "text/plain", Data: []byte(text)}}}
}

func unixNano(value time.Time) int64 { return value.UnixNano() }

func createCommand(commandID string, contextRef ContextRef, taskRef TaskRef, messageID, text string) CreateTaskCommand {
	return CreateTaskCommand{
		Meta: CommandMeta{ID: CommandID(commandID)}, Task: taskRef, Context: contextRef,
		Message: textMessage(messageID, text),
	}
}

func acquireTestLease(t *testing.T, service interface {
	AcquireLease(context.Context, AcquireLeaseCommand) (LeaseAssignment, error)
}, task TaskRef) LeaseGrant {
	t.Helper()
	assignment, err := service.AcquireLease(context.Background(), AcquireLeaseCommand{
		ID:   CommandID("lease-" + string(task.Workspace.ID) + "-" + string(task.ID)),
		Task: task, Worker: "worker-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return assignment.Grant
}

func workerMeta(t *testing.T, service interface {
	GetLease(context.Context, TaskRef) (LeaseAssignment, error)
}, task TaskRef, id CommandID, revision uint64) WorkerCommandMeta {
	t.Helper()
	assignment, err := service.GetLease(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	return WorkerCommandMeta{ID: id, ExpectedRevision: revision, Grant: assignment.Grant}
}

func TestDurableTwoRoundConversationAndFreshFollowup(t *testing.T) {
	service, path := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "conversation-1", "workspace-1", "task-1")

	task, err := service.CreateTask(ctx, createCommand("cmd-create-1", contextRef, taskRef, "message-1", "please investigate"))
	if err != nil {
		t.Fatal(err)
	}
	if task.State != TaskSubmitted || task.Revision != 1 || task.MessageCount != 1 {
		t.Fatalf("created task = %#v", task)
	}
	grant := acquireTestLease(t, service, taskRef)
	task, err = service.AcceptTask(ctx, WorkerTaskCommand{
		Meta: WorkerCommandMeta{ID: "cmd-accept-1", ExpectedRevision: 1, Grant: grant}, Task: taskRef,
	})
	if err != nil {
		t.Fatal(err)
	}

	steps := []struct {
		request bool
		id      string
		message string
		text    string
	}{
		{true, "cmd-ask-1", "message-2", "which environment?"},
		{false, "cmd-reply-1", "message-3", "staging"},
		{true, "cmd-ask-2", "message-4", "may I restart it?"},
		{false, "cmd-reply-2", "message-5", "yes"},
	}
	for _, step := range steps {
		if step.request {
			task, err = service.RequestInput(ctx, WorkerMessageCommand{
				Meta: WorkerCommandMeta{ID: CommandID(step.id), ExpectedRevision: task.Revision, Grant: grant},
				Task: taskRef, Message: textMessage(step.message, step.text),
			})
			if err == nil && task.State != TaskInputRequired {
				t.Fatalf("request state = %s", task.State)
			}
		} else {
			task, err = service.ReplyTask(ctx, MessageCommand{
				Meta: CommandMeta{ID: CommandID(step.id), ExpectedRevision: task.Revision},
				Task: taskRef, Message: textMessage(step.message, step.text),
			})
			if err == nil && task.State != TaskWorking {
				t.Fatalf("reply state = %s", task.State)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	complete := CompleteTaskCommand{
		Meta: WorkerCommandMeta{ID: "cmd-complete-1", ExpectedRevision: task.Revision, Grant: grant},
		Task: taskRef, Submission: "submission-1",
		Message: textMessage("message-6", "staging recovered"),
	}
	task, err = service.CompleteTask(ctx, complete)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != TaskCompleted || task.Revision != 7 || task.MessageCount != 6 || task.EventCount != 7 {
		t.Fatalf("completed task = %#v", task)
	}
	if task.Submission == nil || task.Submission.ID != "submission-1" || task.Submission.FinalMessage != "message-6" {
		t.Fatalf("submission = %#v", task.Submission)
	}

	messagePage, err := service.ListMessages(ctx, taskRef, PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	messages := messagePage.Items
	if len(messages) != 6 {
		t.Fatalf("messages = %d, want 6", len(messages))
	}
	for index, message := range messages {
		want := AuthorCaller
		if index%2 == 1 {
			want = AuthorAgent
		}
		if message.Author != want || message.Sequence != uint64(index+1) {
			t.Fatalf("message %d = %#v, want author %s", index, message, want)
		}
	}
	eventPage, err := service.ReadEvents(ctx, taskRef, PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	events := eventPage.Items
	if len(events) != 7 || events[len(events)-1].Type != EventTaskCompleted {
		t.Fatalf("events = %#v", events)
	}

	_, err = service.RequestInput(ctx, WorkerMessageCommand{
		Meta: WorkerCommandMeta{ID: "cmd-after-terminal", ExpectedRevision: task.Revision, Grant: grant},
		Task: taskRef, Message: textMessage("message-after-terminal", "should fail"),
	})
	if !errors.Is(err, ErrStaleLease) {
		t.Fatalf("terminal request error = %v", err)
	}

	// Reopen the same SQLite state and verify that the whole terminal boundary,
	// including the final message and Submission, survived together.
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestAgentWithConfig(t, path, DefaultConfig())
	recovered, err := reopened.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered, task) {
		t.Fatalf("recovered task differs:\n got %#v\nwant %#v", recovered, task)
	}

	followupRef := TaskRef{Workspace: taskRef.Workspace, ID: "task-2"}
	followup, err := reopened.CreateTask(ctx, createCommand(
		"cmd-create-2", contextRef, followupRef, "message-7", "verify the fix",
	))
	if err != nil {
		t.Fatal(err)
	}
	if followup.State != TaskSubmitted {
		t.Fatalf("followup state = %s", followup.State)
	}
	taskPage, err := reopened.ListTasks(ctx, contextRef, TaskPageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	tasks := taskPage.Items
	if len(tasks) != 2 {
		t.Fatalf("tasks in context = %d, want 2", len(tasks))
	}
}

func TestCommandReplayAndRevisionCAS(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "conversation-1", "workspace-1", "task-1")
	create := createCommand("cmd-create", contextRef, taskRef, "message-1", "start")
	first, err := service.CreateTask(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.CreateTask(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replay, first) {
		t.Fatalf("create replay differs:\n got %#v\nwant %#v", replay, first)
	}
	conflicting := create
	conflicting.Message = textMessage("message-1", "different")
	if _, err := service.CreateTask(ctx, conflicting); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay error = %v", err)
	}

	grant := acquireTestLease(t, service, taskRef)
	commands := []WorkerTaskCommand{
		{Meta: WorkerCommandMeta{ID: "cmd-accept-a", ExpectedRevision: 1, Grant: grant}, Task: taskRef},
		{Meta: WorkerCommandMeta{ID: "cmd-accept-b", ExpectedRevision: 1, Grant: grant}, Task: taskRef},
	}
	var wait sync.WaitGroup
	errorsSeen := make(chan error, len(commands))
	for _, command := range commands {
		command := command
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := service.AcceptTask(ctx, command)
			errorsSeen <- err
		}()
	}
	wait.Wait()
	close(errorsSeen)
	var successes, conflicts int
	for err := range errorsSeen {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrRevisionConflict):
			conflicts++
		default:
			t.Fatalf("concurrent accept error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	task, err := service.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != TaskWorking || task.Revision != 2 || task.EventCount != 2 {
		t.Fatalf("task after CAS = %#v", task)
	}
}

func TestSameContextParallelTasksAndAuthorityIsolation(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextA, taskA := refs("tenant-a", "shared-context", "workspace", "task")
	_, taskB := refs("tenant-a", "shared-context", "workspace", "task-b")
	contextOther, taskOther := refs("tenant-b", "shared-context", "workspace", "task")

	commands := []CreateTaskCommand{
		createCommand("create-a", contextA, taskA, "message-a", "A"),
		createCommand("create-b", contextA, taskB, "message-b", "B"),
		createCommand("create-other", contextOther, taskOther, "message-a", "other authority"),
	}
	for _, command := range commands {
		if _, err := service.CreateTask(ctx, command); err != nil {
			t.Fatal(err)
		}
	}
	for index, ref := range []TaskRef{taskA, taskB} {
		grant := acquireTestLease(t, service, ref)
		if _, err := service.AcceptTask(ctx, WorkerTaskCommand{
			Meta: WorkerCommandMeta{ID: CommandID("accept-" + string(rune('a'+index))), ExpectedRevision: 1, Grant: grant}, Task: ref,
		}); err != nil {
			t.Fatal(err)
		}
	}
	taskPage, err := service.ListTasks(ctx, contextA, TaskPageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	tasks := taskPage.Items
	if len(tasks) != 2 || tasks[0].State != TaskWorking || tasks[1].State != TaskWorking {
		t.Fatalf("parallel context tasks = %#v", tasks)
	}
	otherPage, err := service.ListTasks(ctx, contextOther, TaskPageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	otherTasks := otherPage.Items
	if len(otherTasks) != 1 || otherTasks[0].Ref != taskOther {
		t.Fatalf("other authority tasks = %#v", otherTasks)
	}
}

func TestMessageAndSubmissionFailuresRollbackWholeTransition(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, firstRef := refs("tenant-a", "context", "workspace", "task-a")
	_, secondRef := refs("tenant-a", "context", "workspace", "task-b")
	for _, command := range []CreateTaskCommand{
		createCommand("create-a", contextRef, firstRef, "initial-a", "A"),
		createCommand("create-b", contextRef, secondRef, "initial-b", "B"),
	} {
		if _, err := service.CreateTask(ctx, command); err != nil {
			t.Fatal(err)
		}
	}
	for index, ref := range []TaskRef{firstRef, secondRef} {
		grant := acquireTestLease(t, service, ref)
		if _, err := service.AcceptTask(ctx, WorkerTaskCommand{
			Meta: WorkerCommandMeta{ID: CommandID("accept-" + string(rune('a'+index))), ExpectedRevision: 1, Grant: grant}, Task: ref,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := service.RequestInput(ctx, WorkerMessageCommand{
		Meta: workerMeta(t, service, firstRef, "ask-a", 2), Task: firstRef,
		Message: textMessage("shared-message", "question"),
	})
	if err != nil || first.State != TaskInputRequired {
		t.Fatalf("first input request = %#v, %v", first, err)
	}
	if _, err := service.RequestInput(ctx, WorkerMessageCommand{
		Meta: workerMeta(t, service, secondRef, "ask-b", 2), Task: secondRef,
		Message: textMessage("shared-message", "another question"),
	}); !errors.Is(err, ErrMessageConflict) {
		t.Fatalf("duplicate message error = %v", err)
	}
	second, err := service.GetTask(ctx, secondRef)
	if err != nil {
		t.Fatal(err)
	}
	if second.State != TaskWorking || second.Revision != 2 || second.MessageCount != 1 || second.EventCount != 2 {
		t.Fatalf("message failure leaked partial transition: %#v", second)
	}

	// Finish A with a submission ID, then prove B's conflicting submission rolls
	// back its task update and final message together.
	first, err = service.ReplyTask(ctx, MessageCommand{
		Meta: CommandMeta{ID: "reply-a", ExpectedRevision: first.Revision}, Task: firstRef,
		Message: textMessage("reply-a-message", "answer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: workerMeta(t, service, firstRef, "complete-a", first.Revision), Task: firstRef,
		Submission: "shared-submission", Message: textMessage("final-a", "done A"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CompleteTask(ctx, CompleteTaskCommand{
		Meta: workerMeta(t, service, secondRef, "complete-b", second.Revision), Task: secondRef,
		Submission: "shared-submission", Message: textMessage("final-b", "done B"),
	}); !errors.Is(err, ErrSubmissionConflict) {
		t.Fatalf("duplicate submission error = %v", err)
	}
	second, err = service.GetTask(ctx, secondRef)
	if err != nil {
		t.Fatal(err)
	}
	if second.State != TaskWorking || second.Revision != 2 || second.MessageCount != 1 || second.EventCount != 2 || second.Submission != nil {
		t.Fatalf("submission failure leaked partial transition: %#v", second)
	}
	messagePage, err := service.ListMessages(ctx, secondRef, PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	messages := messagePage.Items
	if len(messages) != 1 {
		t.Fatalf("rolled-back final message remained: %#v", messages)
	}
}

func TestTransitionsOwnerLockAndClose(t *testing.T) {
	service, path := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("tenant-a", "context", "workspace", "task")
	if _, err := service.CreateTask(ctx, createCommand("create", contextRef, taskRef, "message", "start")); err != nil {
		t.Fatal(err)
	}
	grant := acquireTestLease(t, service, taskRef)
	if _, err := service.RequestInput(ctx, WorkerMessageCommand{
		Meta: WorkerCommandMeta{ID: "bad-request", ExpectedRevision: 1, Grant: grant}, Task: taskRef,
		Message: textMessage("bad-message", "not working yet"),
	}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("invalid transition error = %v", err)
	}
	unchanged, err := service.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Revision != 1 || unchanged.MessageCount != 1 || unchanged.EventCount != 1 {
		t.Fatalf("invalid transition changed task: %#v", unchanged)
	}

	if second, err := agentsqlite.Open(ctx, agentsqlite.Config{Path: path}); !errors.Is(err, agentsqlite.ErrDatabaseInUse) {
		if err == nil {
			_ = second.Release(ctx)
		}
		t.Fatalf("second SQLite owner error = %v, want ErrDatabaseInUse", err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := service.GetTask(ctx, taskRef); !errors.Is(err, ErrClosed) {
		t.Fatalf("query closed Agent error = %v", err)
	}
	reopened := openTestAgentWithConfig(t, path, DefaultConfig())
	if _, err := reopened.GetTask(ctx, taskRef); err != nil {
		t.Fatal(err)
	}
}
