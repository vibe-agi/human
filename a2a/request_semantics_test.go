package a2a

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
)

func TestInitialMessageReplayDoesNotRerunWorkspaceResolver(t *testing.T) {
	service := openA2ATestAgent(t)
	resolverCalls := 0
	handler := newRequestHandler(Config{
		Agent: service, Card: testAgentCard(),
		ResolveWorkspace: func(context.Context, Principal, *sdk.SendMessageRequest) (agent.WorkspaceID, error) {
			resolverCalls++
			if resolverCalls != 1 {
				return "", errors.New("resolver is unavailable")
			}
			return "workspace-a", nil
		},
	}).(*requestHandler)
	ctx := withPrincipal(context.Background(), Principal{Authority: "authority-a", Subject: "caller-a"})
	request := &sdk.SendMessageRequest{
		Config: &sdk.SendMessageConfig{ReturnImmediately: true},
		Message: &sdk.Message{
			ID: "resolver-replay", Role: sdk.MessageRoleUser,
			Parts: sdk.ContentParts{sdk.NewTextPart("create once")},
		},
	}

	first, err := handler.SendMessage(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := handler.SendMessage(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if resolverCalls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolverCalls)
	}
	if first.(*sdk.Task).ID != second.(*sdk.Task).ID {
		t.Fatalf("replayed task id = %q, want %q", second.(*sdk.Task).ID, first.(*sdk.Task).ID)
	}
}

func TestInitialMessageReplayRequiresExactEffectiveContext(t *testing.T) {
	generated := string(deterministicID("a2a-context", "authority-a", "context-exact"))
	tests := []struct {
		name         string
		firstContext string
		retryContext string
		wantError    bool
	}{
		{name: "explicit then omitted", firstContext: "explicit-context", wantError: true},
		{name: "omitted then different explicit", retryContext: "explicit-context", wantError: true},
		{name: "omitted then generated explicit", retryContext: generated},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := openA2ATestAgent(t)
			handler := newRequestHandler(Config{
				Agent: service, Card: testAgentCard(),
				ResolveWorkspace: func(context.Context, Principal, *sdk.SendMessageRequest) (agent.WorkspaceID, error) {
					return "workspace-a", nil
				},
			}).(*requestHandler)
			ctx := withPrincipal(context.Background(), Principal{Authority: "authority-a", Subject: "caller-a"})
			message := func(contextID string) *sdk.SendMessageRequest {
				return &sdk.SendMessageRequest{
					Config: &sdk.SendMessageConfig{ReturnImmediately: true},
					Message: &sdk.Message{
						ID: "context-exact", ContextID: contextID, Role: sdk.MessageRoleUser,
						Parts: sdk.ContentParts{sdk.NewTextPart("same body")},
					},
				}
			}
			if _, err := handler.SendMessage(ctx, message(test.firstContext)); err != nil {
				t.Fatal(err)
			}
			_, err := handler.SendMessage(ctx, message(test.retryContext))
			if test.wantError {
				if !errors.Is(err, sdk.ErrInvalidParams) {
					t.Fatalf("retry error = %v, want invalid params", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSubscribeStopsAtInputRequiredAndContinuationUsesNewStream(t *testing.T) {
	service := openA2ATestAgent(t)
	ctx := withPrincipal(context.Background(), Principal{Authority: "authority-a", Subject: "caller-a"})
	ref := createA2ATestTask(t, service, "subscribe-across-input")
	grant := acquireAndRequestInput(t, service, ref, "worker-a", "subscribe-across-input")
	handler := newRequestHandler(Config{Agent: service, Card: testAgentCard(), PollInterval: time.Millisecond}).(*requestHandler)

	var firstStates []sdk.TaskState
	for event, err := range handler.SubscribeToTask(ctx, &sdk.SubscribeToTaskRequest{ID: sdk.TaskID(ref.ID)}) {
		if err != nil {
			t.Fatal(err)
		}
		switch value := event.(type) {
		case *sdk.Task:
			firstStates = append(firstStates, value.Status.State)
		case *sdk.TaskStatusUpdateEvent:
			firstStates = append(firstStates, value.Status.State)
		}
	}
	if want := []sdk.TaskState{sdk.TaskStateInputRequired}; fmt.Sprint(firstStates) != fmt.Sprint(want) {
		t.Fatalf("first stream states = %v, want %v", firstStates, want)
	}

	current, err := service.GetTask(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	working, err := service.ReplyTask(ctx, agent.MessageCommand{
		Meta: agent.CommandMeta{ID: "subscribe-reply", ExpectedRevision: current.Revision}, Task: ref,
		Message: agent.MessageInput{ID: "subscribe-caller-message", Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("continue")}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	var continuedStates []sdk.TaskState
	completed := false
	for event, err := range handler.SubscribeToTask(ctx, &sdk.SubscribeToTaskRequest{ID: sdk.TaskID(ref.ID)}) {
		if err != nil {
			t.Fatal(err)
		}
		switch value := event.(type) {
		case *sdk.Task:
			continuedStates = append(continuedStates, value.Status.State)
			if !completed {
				completed = true
				if _, err := service.CompleteTask(ctx, agent.CompleteTaskCommand{
					Meta: agent.WorkerCommandMeta{ID: "subscribe-complete", ExpectedRevision: working.Revision, Grant: grant},
					Task: ref, Submission: "subscribe-submission",
					Message: agent.MessageInput{ID: "subscribe-final-message", Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("done")}}},
				}); err != nil {
					t.Fatal(err)
				}
			}
		case *sdk.TaskStatusUpdateEvent:
			continuedStates = append(continuedStates, value.Status.State)
		}
	}
	want := []sdk.TaskState{sdk.TaskStateWorking, sdk.TaskStateCompleted}
	if fmt.Sprint(continuedStates) != fmt.Sprint(want) {
		t.Fatalf("continuation states = %v, want %v", continuedStates, want)
	}
}

func TestHistoryLengthRejectsOnlyNegativeAndClampsStorageQuery(t *testing.T) {
	negative := -1
	if _, err := normalizeHistoryLength(&negative); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("negative history error = %v, want invalid params", err)
	}
	large := agent.MaxPageSize + 10_000
	normalized, err := normalizeHistoryLength(&large)
	if err != nil {
		t.Fatal(err)
	}
	if normalized == nil || *normalized != agent.MaxPageSize {
		t.Fatalf("normalized history = %v, want %d", normalized, agent.MaxPageSize)
	}

	service := openA2ATestAgent(t)
	ref := createA2ATestTask(t, service, "history-clamp")
	handler := newRequestHandler(Config{Agent: service, Card: testAgentCard()}).(*requestHandler)
	ctx := withPrincipal(context.Background(), Principal{Authority: ref.Workspace.Authority, Subject: "caller-a"})
	task, err := handler.GetTask(ctx, &sdk.GetTaskRequest{ID: sdk.TaskID(ref.ID), HistoryLength: &large})
	if err != nil {
		t.Fatal(err)
	}
	if len(task.History) != 1 {
		t.Fatalf("history size = %d, want 1", len(task.History))
	}
}

func TestListTasksTimestampFilterIsInclusive(t *testing.T) {
	service := openA2ATestAgent(t)
	ref := createA2ATestTask(t, service, "timestamp-inclusive")
	stored, err := service.GetTask(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	handler := newRequestHandler(Config{Agent: service, Card: testAgentCard()}).(*requestHandler)
	ctx := withPrincipal(context.Background(), Principal{Authority: ref.Workspace.Authority, Subject: "caller-a"})
	page, err := handler.ListTasks(ctx, &sdk.ListTasksRequest{StatusTimestampAfter: &stored.UpdatedAt})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tasks) != 1 || page.Tasks[0].ID != sdk.TaskID(ref.ID) {
		t.Fatalf("inclusive timestamp page = %#v", page.Tasks)
	}
}

func TestListTasksAuthRequiredIsAValidatedEmptyProjection(t *testing.T) {
	service := openA2ATestAgent(t)
	handler := newRequestHandler(Config{Agent: service, Card: testAgentCard()}).(*requestHandler)
	ctx := withPrincipal(context.Background(), Principal{Authority: "authority-a", Subject: "caller-a"})
	largeHistory := agent.MaxPageSize + 1_000
	token, err := encodeTaskCursor(agent.TaskQueryCursor{
		UpdatedAt: time.Unix(123, 456).UTC(), Workspace: "workspace-a", Task: "task-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := handler.ListTasks(ctx, &sdk.ListTasksRequest{
		PageSize: 7, PageToken: token, Status: sdk.TaskStateAuthRequired, HistoryLength: &largeHistory,
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.PageSize != 7 || page.TotalSize != 0 || len(page.Tasks) != 0 || page.Tasks == nil || page.NextPageToken != "" {
		t.Fatalf("AUTH_REQUIRED page = %#v", page)
	}

	negative := -1
	if _, err := handler.ListTasks(ctx, &sdk.ListTasksRequest{
		Status: sdk.TaskStateAuthRequired, HistoryLength: &negative,
	}); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("negative history error = %v, want invalid params", err)
	}
	if _, err := handler.ListTasks(ctx, &sdk.ListTasksRequest{
		Status: sdk.TaskStateAuthRequired, PageToken: "not-a-cursor",
	}); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("invalid cursor error = %v, want invalid params", err)
	}
	if _, err := handler.ListTasks(ctx, &sdk.ListTasksRequest{
		Status: sdk.TaskStateAuthRequired, ContextID: "bad context",
	}); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("invalid context error = %v, want invalid params", err)
	}
	zero := time.Time{}
	if _, err := handler.ListTasks(ctx, &sdk.ListTasksRequest{
		Status: sdk.TaskStateAuthRequired, StatusTimestampAfter: &zero,
	}); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("invalid timestamp error = %v, want invalid params", err)
	}
}

func TestCancelTaskRetriesRevisionConflictAndUsesLatestRevision(t *testing.T) {
	ref := agent.TaskRef{Workspace: agent.WorkspaceRef{Authority: "authority-a", ID: "workspace-a"}, ID: "cancel-race"}
	loads := []agent.Task{
		{Ref: ref, State: agent.TaskWorking, Revision: 2},
		{Ref: ref, State: agent.TaskInputRequired, Revision: 3},
	}
	loadIndex := 0
	load := func(context.Context, agent.TaskRef) (agent.Task, error) {
		if loadIndex >= len(loads) {
			return agent.Task{}, errors.New("unexpected load")
		}
		result := loads[loadIndex]
		loadIndex++
		return result, nil
	}
	var revisions []uint64
	cancel := func(_ context.Context, command agent.TaskCommand) (agent.Task, error) {
		revisions = append(revisions, command.Meta.ExpectedRevision)
		if len(revisions) == 1 {
			return agent.Task{}, &agent.RevisionConflictError{Expected: 2, Actual: 3}
		}
		return agent.Task{Ref: ref, State: agent.TaskCanceled, Revision: 4}, nil
	}

	canceled, err := cancelTaskWithRetry(context.Background(), ref, "cancel-command", load, cancel)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.State != agent.TaskCanceled || fmt.Sprint(revisions) != "[2 3]" {
		t.Fatalf("canceled = %#v, attempted revisions = %v", canceled, revisions)
	}
}

func TestCancelTaskReportsTerminalStateAccurately(t *testing.T) {
	ref := agent.TaskRef{Workspace: agent.WorkspaceRef{Authority: "authority-a", ID: "workspace-a"}, ID: "cancel-terminal"}
	terminal := agent.Task{Ref: ref, State: agent.TaskCompleted, Revision: 4}
	_, err := cancelTaskWithRetry(
		context.Background(), ref, "cancel-terminal",
		func(context.Context, agent.TaskRef) (agent.Task, error) { return terminal, nil },
		func(context.Context, agent.TaskCommand) (agent.Task, error) {
			t.Fatal("terminal task reached cancel mutation")
			return agent.Task{}, nil
		},
	)
	if !errors.Is(err, sdk.ErrTaskNotCancelable) {
		t.Fatalf("terminal cancel error = %v, want task not cancelable", err)
	}

	canceled := terminal
	canceled.State = agent.TaskCanceled
	got, err := cancelTaskWithRetry(
		context.Background(), ref, "cancel-terminal",
		func(context.Context, agent.TaskRef) (agent.Task, error) { return canceled, nil },
		func(context.Context, agent.TaskCommand) (agent.Task, error) {
			t.Fatal("already canceled task reached cancel mutation")
			return agent.Task{}, nil
		},
	)
	if err != nil || got.State != agent.TaskCanceled {
		t.Fatalf("already canceled result = %#v, %v", got, err)
	}
}

func TestRequestErrorMappingsPreserveSemantics(t *testing.T) {
	transition := &agent.TransitionError{Operation: "reply_task", State: agent.TaskWorking}
	if err := mapAgentError(transition); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("transition error = %v, want invalid params", err)
	}
	if err := mapResolverError(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("resolver cancellation = %v", err)
	}
	typed := sdk.NewError(sdk.ErrUnauthorized, "workspace is forbidden")
	if got := mapResolverError(fmt.Errorf("resolve: %w", typed)); got != typed {
		t.Fatalf("typed resolver error = %#v, want original %#v", got, typed)
	}
	internal := mapResolverError(errors.New("secret storage failure"))
	if !errors.Is(internal, sdk.ErrInternalError) {
		t.Fatalf("internal resolver error = %v, want internal", internal)
	}
	if internal.Error() == "secret storage failure" {
		t.Fatalf("internal resolver error leaked implementation message: %v", internal)
	}
}

func createA2ATestTask(t *testing.T, service *agent.Agent, suffix string) agent.TaskRef {
	t.Helper()
	ref := agent.TaskRef{
		Workspace: agent.WorkspaceRef{Authority: "authority-a", ID: "workspace-a"},
		ID:        agent.TaskID("task-" + suffix),
	}
	_, err := service.CreateTask(context.Background(), agent.CreateTaskCommand{
		Meta:    agent.CommandMeta{ID: agent.CommandID("create-" + suffix)},
		Task:    ref,
		Context: agent.ContextRef{Authority: "authority-a", ID: agent.ContextID("context-" + suffix)},
		Message: agent.MessageInput{
			ID:    agent.MessageID("message-" + suffix),
			Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("start")}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
