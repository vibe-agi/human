package agent_test

import (
	"context"
	"errors"
	. "github.com/vibe-agi/human/agent"
	"testing"
	"time"
)

func TestResolveTaskUsesAuthenticatedAuthorityAndUniquePublicID(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, ref := refs("authority-a", "context-a", "workspace-a", "task-public")
	createQueryTask(t, service, contextRef, ref, "create-a", "message-a")

	resolved, err := service.ResolveTask(ctx, ref.Workspace.Authority, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != ref {
		t.Fatalf("resolved = %#v, want %#v", resolved, ref)
	}
	if _, err := service.ResolveTask(ctx, "authority-b", ref.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-authority resolve error = %v, want ErrNotFound", err)
	}

	otherWorkspace := ref
	otherWorkspace.Workspace.ID = "workspace-b"
	_, err = service.CreateTask(ctx, CreateTaskCommand{
		Meta: CommandMeta{ID: "create-duplicate"}, Task: otherWorkspace, Context: contextRef,
		Message: textMessage("message-duplicate", "duplicate"),
	})
	if !errors.Is(err, ErrTaskConflict) {
		t.Fatalf("duplicate public task id error = %v, want ErrTaskConflict", err)
	}

	otherContext, otherAuthority := refs("authority-b", "context-b", "workspace-b", "task-public")
	createQueryTask(t, service, otherContext, otherAuthority, "create-b", "message-b")
	resolved, err = service.ResolveTask(ctx, otherAuthority.Workspace.Authority, otherAuthority.ID)
	if err != nil || resolved != otherAuthority {
		t.Fatalf("other authority resolve = %#v, %v", resolved, err)
	}
}

func TestListAuthorityTasksFiltersAndPaginatesUpdatedOrder(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	base := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	clock := base
	service.now = func() time.Time { return clock }

	contextOne, first := refs("authority-a", "context-one", "workspace-a", "task-a")
	createQueryTask(t, service, contextOne, first, "create-a", "message-a")
	clock = base.Add(time.Second)
	contextTwo, second := refs("authority-a", "context-two", "workspace-b", "task-b")
	createQueryTask(t, service, contextTwo, second, "create-b", "message-b")
	clock = base.Add(2 * time.Second)
	_, third := refs("authority-a", "context-one", "workspace-c", "task-c")
	createQueryTask(t, service, contextOne, third, "create-c", "message-c")
	clock = base.Add(3 * time.Second)
	if _, err := service.CancelTask(ctx, TaskCommand{
		Meta: CommandMeta{ID: "cancel-c", ExpectedRevision: 1}, Task: third,
	}); err != nil {
		t.Fatal(err)
	}
	otherContext, other := refs("authority-b", "context-other", "workspace-z", "task-z")
	createQueryTask(t, service, otherContext, other, "create-z", "message-z")

	page, err := service.ListAuthorityTasks(ctx, "authority-a", TaskQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if page.TotalSize != 3 || !page.HasMore || page.Next == nil {
		t.Fatalf("first page metadata = %#v", page)
	}
	assertTaskIDs(t, page.Items, "task-c", "task-b")
	last, err := service.ListAuthorityTasks(ctx, "authority-a", TaskQuery{After: page.Next, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if last.TotalSize != 3 || last.HasMore || last.Next != nil {
		t.Fatalf("last page metadata = %#v", last)
	}
	assertTaskIDs(t, last.Items, "task-a")

	contextPage, err := service.ListAuthorityTasks(ctx, "authority-a", TaskQuery{Context: "context-one"})
	if err != nil {
		t.Fatal(err)
	}
	if contextPage.TotalSize != 2 {
		t.Fatalf("context total = %d, want 2", contextPage.TotalSize)
	}
	assertTaskIDs(t, contextPage.Items, "task-c", "task-a")

	statePage, err := service.ListAuthorityTasks(ctx, "authority-a", TaskQuery{State: TaskSubmitted})
	if err != nil {
		t.Fatal(err)
	}
	if statePage.TotalSize != 2 {
		t.Fatalf("state total = %d, want 2", statePage.TotalSize)
	}
	assertTaskIDs(t, statePage.Items, "task-b", "task-a")

	after := base.Add(time.Second)
	updatedPage, err := service.ListAuthorityTasks(ctx, "authority-a", TaskQuery{UpdatedAtOrAfter: &after})
	if err != nil {
		t.Fatal(err)
	}
	if updatedPage.TotalSize != 2 {
		t.Fatalf("updated total = %d, want 2", updatedPage.TotalSize)
	}
	assertTaskIDs(t, updatedPage.Items, "task-c", "task-b")
}

func TestTaskSnapshotProvidesNoGapEventCursor(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, ref := refs("authority-a", "context-a", "workspace-a", "task-snapshot")
	created := createQueryTask(t, service, contextRef, ref, "create-snapshot", "message-snapshot")
	snapshot, err := service.SnapshotTask(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Task != created || snapshot.EventCursor != created.EventCount {
		t.Fatalf("snapshot = %#v, want task %#v cursor %d", snapshot, created, created.EventCount)
	}
	if _, err := service.CancelTask(ctx, TaskCommand{
		Meta: CommandMeta{ID: "cancel-snapshot", ExpectedRevision: created.Revision}, Task: ref,
	}); err != nil {
		t.Fatal(err)
	}
	events, err := service.ReadEvents(ctx, ref, PageRequest{After: snapshot.EventCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Items) != 1 || events.Items[0].Type != EventTaskCanceled || events.Items[0].Sequence != snapshot.EventCursor+1 {
		t.Fatalf("events after snapshot = %#v", events)
	}
}

func createQueryTask(
	t *testing.T,
	service interface {
		CreateTask(context.Context, CreateTaskCommand) (Task, error)
	},
	contextRef ContextRef,
	ref TaskRef,
	commandID,
	messageID string,
) Task {
	t.Helper()
	task, err := service.CreateTask(context.Background(), CreateTaskCommand{
		Meta: CommandMeta{ID: CommandID(commandID)}, Task: ref, Context: contextRef,
		Message: textMessage(messageID, "query task"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func assertTaskIDs(t *testing.T, tasks []Task, want ...TaskID) {
	t.Helper()
	if len(tasks) != len(want) {
		t.Fatalf("task count = %d, want %d: %#v", len(tasks), len(want), tasks)
	}
	for index := range want {
		if tasks[index].Ref.ID != want[index] {
			t.Fatalf("task[%d] = %q, want %q", index, tasks[index].Ref.ID, want[index])
		}
	}
}
