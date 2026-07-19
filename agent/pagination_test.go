package agent_test

import (
	"context"
	"errors"
	. "github.com/vibe-agi/human/agent"
	"testing"
)

func TestAgentPagesUnknownTasksAndNilContext(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, firstRef := refs("tenant-a", "context", "workspace", "task-a")
	refsToCreate := []TaskRef{firstRef}
	for _, id := range []TaskID{"task-b", "task-c"} {
		refsToCreate = append(refsToCreate, TaskRef{Workspace: firstRef.Workspace, ID: id})
	}
	for index, ref := range refsToCreate {
		if _, err := service.CreateTask(ctx, createCommand(
			"create-"+string(rune('a'+index)), contextRef, ref,
			"message-"+string(rune('a'+index)), "start",
		)); err != nil {
			t.Fatal(err)
		}
	}
	firstPage, err := service.ListTasks(ctx, contextRef, TaskPageRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Items) != 2 || !firstPage.HasMore || firstPage.Next == nil {
		t.Fatalf("first task page = %#v", firstPage)
	}
	secondPage, err := service.ListTasks(ctx, contextRef, TaskPageRequest{After: firstPage.Next, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Items) != 1 || secondPage.HasMore {
		t.Fatalf("second task page = %#v", secondPage)
	}

	grant := acquireTestLease(t, service, firstRef)
	working, err := service.AcceptTask(ctx, WorkerTaskCommand{
		Meta: WorkerCommandMeta{ID: "accept-a", ExpectedRevision: 1, Grant: grant}, Task: firstRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	asked, err := service.RequestInput(ctx, WorkerMessageCommand{
		Meta: WorkerCommandMeta{ID: "ask-a", ExpectedRevision: working.Revision, Grant: grant}, Task: firstRef,
		Message: textMessage("question-a", "which environment?"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReplyTask(ctx, MessageCommand{
		Meta: CommandMeta{ID: "reply-a", ExpectedRevision: asked.Revision}, Task: firstRef,
		Message: textMessage("reply-a", "staging"),
	}); err != nil {
		t.Fatal(err)
	}
	messagePage, err := service.ListMessages(ctx, firstRef, PageRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(messagePage.Items) != 1 || !messagePage.HasMore || messagePage.Next != 1 {
		t.Fatalf("message page = %#v", messagePage)
	}
	eventPage, err := service.ReadEvents(ctx, firstRef, PageRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(eventPage.Items) != 2 || !eventPage.HasMore || eventPage.Next != 2 {
		t.Fatalf("event page = %#v", eventPage)
	}

	unknown := TaskRef{Workspace: firstRef.Workspace, ID: "unknown"}
	if _, err := service.ListMessages(ctx, unknown, PageRequest{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown message history error = %v", err)
	}
	if _, err := service.ReadEvents(ctx, unknown, PageRequest{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown event history error = %v", err)
	}
	if _, err := service.ListTasks(ctx, contextRef, TaskPageRequest{Limit: MaxPageSize + 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized task page error = %v", err)
	}
	if _, err := service.GetTask(nil, firstRef); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil context error = %v", err)
	}
}
