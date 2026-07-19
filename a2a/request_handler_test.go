package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
	agentsqlite "github.com/vibe-agi/human/agent/sqlite"
	"github.com/vibe-agi/human/workspace"
)

func TestHTTPCreateTaskUsesAuthenticatedAuthorityAndReplaysExactly(t *testing.T) {
	service := openA2ATestAgent(t)
	handler := newA2ATestHTTPHandler(t, service)
	request := &sdk.SendMessageRequest{
		Tenant: "untrusted-tenant",
		Config: &sdk.SendMessageConfig{ReturnImmediately: true},
		Message: &sdk.Message{
			ID: "message-create", Role: sdk.MessageRoleUser,
			Parts: sdk.ContentParts{sdk.NewTextPart("please investigate")},
		},
	}
	first := sendA2ARequest(t, handler, "authority-a", request)
	firstTask, ok := first.Event.(*sdk.Task)
	if !ok {
		t.Fatalf("result = %T, want *a2a.Task", first.Event)
	}
	if firstTask.ID == "" || firstTask.ContextID == "" || firstTask.Status.State != sdk.TaskStateSubmitted {
		t.Fatalf("created task = %#v", firstTask)
	}
	ref, err := service.ResolveTask(context.Background(), "authority-a", agent.TaskID(firstTask.ID))
	if err != nil {
		t.Fatal(err)
	}
	if ref.Workspace.ID != "workspace-a" {
		t.Fatalf("workspace = %q, want workspace-a", ref.Workspace.ID)
	}
	if _, err := service.ResolveTask(context.Background(), "untrusted-tenant", agent.TaskID(firstTask.ID)); !errors.Is(err, agent.ErrNotFound) {
		t.Fatalf("tenant became authority: %v", err)
	}

	second := sendA2ARequest(t, handler, "authority-a", request)
	secondTask, ok := second.Event.(*sdk.Task)
	if !ok || secondTask.ID != firstTask.ID || secondTask.ContextID != firstTask.ContextID {
		t.Fatalf("replayed task = %#v, want identity %#v", second.Event, firstTask)
	}
	page, err := service.ListAuthorityTasks(context.Background(), "authority-a", agent.TaskQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if page.TotalSize != 1 || len(page.Items) != 1 {
		t.Fatalf("task page after retry = %#v", page)
	}
}

func TestContinuationRetryReturnsCurrentTaskWithoutDuplicatingMessage(t *testing.T) {
	service := openA2ATestAgent(t)
	handler := newA2ATestHTTPHandler(t, service)
	createdResponse := sendA2ARequest(t, handler, "authority-a", &sdk.SendMessageRequest{
		Config: &sdk.SendMessageConfig{ReturnImmediately: true},
		Message: &sdk.Message{
			ID: "message-create", Role: sdk.MessageRoleUser,
			Parts: sdk.ContentParts{sdk.NewTextPart("start")},
		},
	})
	created := createdResponse.Event.(*sdk.Task)
	ref, err := service.ResolveTask(context.Background(), "authority-a", agent.TaskID(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	grant := acquireAndRequestInput(t, service, ref, "worker-a", "first")

	continuation := &sdk.SendMessageRequest{
		Config: &sdk.SendMessageConfig{ReturnImmediately: true},
		Message: &sdk.Message{
			ID: "message-reply", TaskID: created.ID, ContextID: created.ContextID,
			Role: sdk.MessageRoleUser, Parts: sdk.ContentParts{sdk.NewTextPart("continue")},
		},
	}
	firstReply := sendA2ARequest(t, handler, "authority-a", continuation).Event.(*sdk.Task)
	if firstReply.Status.State != sdk.TaskStateWorking {
		t.Fatalf("reply state = %q, want working", firstReply.Status.State)
	}
	current, err := service.GetTask(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RequestInput(context.Background(), agent.WorkerMessageCommand{
		Meta: agent.WorkerCommandMeta{ID: "ask-second", ExpectedRevision: current.Revision, Grant: grant},
		Task: ref, Message: agent.MessageInput{
			ID: "agent-question-second", Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("more?")}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	replayed := sendA2ARequest(t, handler, "authority-a", continuation).Event.(*sdk.Task)
	if replayed.Status.State != sdk.TaskStateInputRequired {
		t.Fatalf("old message retry returned %q, want current input-required", replayed.Status.State)
	}
	stored, err := service.GetTask(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if stored.MessageCount != 4 {
		t.Fatalf("message count = %d, want 4", stored.MessageCount)
	}
}

func TestTerminalContinuationReplaysCommittedMessageAndRejectsNewMessage(t *testing.T) {
	service := openA2ATestAgent(t)
	ctx := withPrincipal(context.Background(), Principal{Authority: "authority-a", Subject: "caller-a"})
	ref := createA2ATestTask(t, service, "terminal-continuation")
	grant := acquireAndRequestInput(t, service, ref, "worker-a", "terminal-continuation")
	handler := newRequestHandler(Config{Agent: service, Card: testAgentCard()}).(*requestHandler)
	current, err := service.GetTask(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	reply := &sdk.SendMessageRequest{Message: &sdk.Message{
		ID: "terminal-reply", TaskID: sdk.TaskID(ref.ID), ContextID: string(current.Context.ID),
		Role: sdk.MessageRoleUser, Parts: sdk.ContentParts{sdk.NewTextPart("continue")},
	}}
	working, err := handler.acceptMessage(ctx, reply)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := service.CompleteTask(ctx, agent.CompleteTaskCommand{
		Meta: agent.WorkerCommandMeta{ID: "terminal-complete", ExpectedRevision: working.Revision, Grant: grant},
		Task: ref, Submission: "terminal-submission",
		Message: agent.MessageInput{ID: "terminal-final", Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("done")}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	replayed, err := handler.acceptMessage(ctx, reply)
	if err != nil {
		t.Fatalf("exact terminal replay: %v", err)
	}
	if replayed.State != agent.TaskCompleted || replayed.Revision != completed.Revision {
		t.Fatalf("terminal replay = %#v, want completed revision %d", replayed, completed.Revision)
	}

	newMessage := *reply
	newMessage.Message = &sdk.Message{
		ID: "terminal-new-message", TaskID: sdk.TaskID(ref.ID), ContextID: string(current.Context.ID),
		Role: sdk.MessageRoleUser, Parts: sdk.ContentParts{sdk.NewTextPart("one more thing")},
	}
	if _, err := handler.acceptMessage(ctx, &newMessage); !errors.Is(err, sdk.ErrUnsupportedOperation) {
		t.Fatalf("new terminal continuation error = %v, want unsupported operation", err)
	}
}

func TestStreamingMessageTailsDurableEventsFromInitialCursor(t *testing.T) {
	service := openA2ATestAgent(t)
	handler := newRequestHandler(Config{
		Agent: service, Card: testAgentCard(), PollInterval: time.Millisecond,
		ResolveWorkspace: func(context.Context, Principal, *sdk.SendMessageRequest) (agent.WorkspaceID, error) {
			return "workspace-a", nil
		},
	}).(*requestHandler)
	ctx := withPrincipal(context.Background(), Principal{Authority: "authority-a", Subject: "subject-a"})
	request := &sdk.SendMessageRequest{Message: &sdk.Message{
		ID: "message-stream", Role: sdk.MessageRoleUser,
		Parts: sdk.ContentParts{sdk.NewTextPart("stream this")},
	}}
	var states []sdk.TaskState
	var taskID sdk.TaskID
	for event, err := range handler.SendStreamingMessage(ctx, request) {
		if err != nil {
			t.Fatal(err)
		}
		switch value := event.(type) {
		case *sdk.Task:
			taskID = value.ID
			states = append(states, value.Status.State)
			ref, err := service.ResolveTask(ctx, "authority-a", agent.TaskID(taskID))
			if err != nil {
				t.Fatal(err)
			}
			acquireAndRequestInput(t, service, ref, "worker-a", "stream")
		case *sdk.TaskStatusUpdateEvent:
			states = append(states, value.Status.State)
		default:
			t.Fatalf("unexpected stream event %T", event)
		}
	}
	want := []sdk.TaskState{sdk.TaskStateSubmitted, sdk.TaskStateWorking, sdk.TaskStateInputRequired}
	if len(states) != len(want) {
		t.Fatalf("states = %#v, want %#v", states, want)
	}
	for index := range want {
		if states[index] != want[index] {
			t.Fatalf("states = %#v, want %#v", states, want)
		}
	}
}

func TestApplyReceiptExtensionAuthorizesAndAdvancesExactWorkspaceHead(t *testing.T) {
	service := openA2ATestAgent(t)
	content, task := publishA2ATestArtifact(t, service)
	authorized := 0
	handler, err := NewHandler(Config{
		Agent: service,
		Card:  testAgentCard(sdk.AgentExtension{URI: ApplyReceiptExtensionURI}),
		Authenticate: func(context.Context, *http.Request) (Principal, error) {
			return Principal{Authority: "authority-a", Subject: "caller-a"}, nil
		},
		ResolveWorkspace: func(context.Context, Principal, *sdk.SendMessageRequest) (agent.WorkspaceID, error) {
			return "workspace-a", nil
		},
		AuthorizeApplyReceipt: func(_ context.Context, principal Principal, got agent.Task, request *RecordApplyReceiptRequest) error {
			authorized++
			if principal.Subject != "caller-a" || got.Ref != task.Ref || request.ArtifactID != string(content.Artifact.Ref.ID) {
				return sdk.ErrUnauthorized
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := RecordApplyReceiptRequest{
		CommandID: "record-apply", ReceiptID: "receipt-a",
		ArtifactID: string(content.Artifact.Ref.ID), ArtifactDigest: content.Artifact.Digest,
		BaseRevision: content.Artifact.BaseRevision, ResultRevision: content.Artifact.ResultRevision,
		Decision: workspace.ApplySuccess, ObservedRevision: content.Artifact.ResultRevision,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		request := httptest.NewRequest(
			http.MethodPost, "/tasks/"+string(task.Ref.ID)+":recordApplyReceipt", bytes.NewReader(encoded),
		)
		request.Header.Set(sdk.SvcParamVersion, string(sdk.Version))
		request.Header.Set(sdk.SvcParamExtensions, ApplyReceiptExtensionURI)
		request.Header.Set("Content-Type", "application/a2a+json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("attempt %d status = %d, body = %s", attempt, response.Code, response.Body.String())
		}
		var result RecordApplyReceiptResponse
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Decision != workspace.ApplySuccess || result.ObservedRevision != content.Artifact.ResultRevision {
			t.Fatalf("receipt response = %#v", result)
		}
	}
	if authorized != 2 {
		t.Fatalf("authorization calls = %d, want 2", authorized)
	}
	head, err := service.GetWorkspaceHead(context.Background(), task.Ref.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if head.ConfirmedRevision != content.Artifact.ResultRevision {
		t.Fatalf("confirmed revision = %q, want %q", head.ConfirmedRevision, content.Artifact.ResultRevision)
	}
}

func openA2ATestAgent(t *testing.T) *agent.Agent {
	t.Helper()
	store, err := agentsqlite.Open(t.Context(), agentsqlite.Config{
		Path: filepath.Join(t.TempDir(), "agent.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	config := agent.DefaultConfig()
	config.Store = store
	service, err := agent.Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Errorf("close Agent: %v", err)
		}
	})
	return service
}

func newA2ATestHTTPHandler(t *testing.T, service *agent.Agent) http.Handler {
	t.Helper()
	handler, err := NewHandler(Config{
		Agent: service,
		Card:  testAgentCard(),
		Authenticate: func(_ context.Context, request *http.Request) (Principal, error) {
			authority := request.Header.Get("Authorization")
			if authority == "" {
				return Principal{}, sdk.ErrUnauthenticated
			}
			return Principal{Authority: agent.AuthorityID(authority), Subject: "test-subject"}, nil
		},
		ResolveWorkspace: func(_ context.Context, principal Principal, _ *sdk.SendMessageRequest) (agent.WorkspaceID, error) {
			if principal.Authority != "authority-a" {
				return "", errors.New("unknown authority")
			}
			return "workspace-a", nil
		},
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func testAgentCard(extensions ...sdk.AgentExtension) *sdk.AgentCard {
	return &sdk.AgentCard{
		Name: "Human Agent", Description: "test", Version: "test",
		SupportedInterfaces: []*sdk.AgentInterface{sdk.NewAgentInterface("http://example.test", sdk.TransportProtocolHTTPJSON)},
		DefaultInputModes:   []string{"text/plain"},
		DefaultOutputModes:  []string{"text/plain", "application/json"},
		Capabilities:        sdk.AgentCapabilities{Streaming: true, Extensions: extensions},
		Skills: []sdk.AgentSkill{{
			ID: "human-collaboration", Name: "Human collaboration",
			Description: "Delegate a durable task to a human collaborator.",
			Tags:        []string{"human"},
		}},
	}
}

func sendA2ARequest(
	t *testing.T,
	handler http.Handler,
	authority string,
	request *sdk.SendMessageRequest,
) sdk.StreamResponse {
	t.Helper()
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, "/message:send", bytes.NewReader(payload))
	httpRequest.Header.Set("Authorization", authority)
	httpRequest.Header.Set(sdk.SvcParamVersion, string(sdk.Version))
	httpRequest.Header.Set("Content-Type", "application/a2a+json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httpRequest)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "application/a2a+json" {
		t.Fatalf("content type = %q", got)
	}
	var result sdk.StreamResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v: %s", err, response.Body.String())
	}
	return result
}

func acquireAndRequestInput(
	t *testing.T,
	service *agent.Agent,
	ref agent.TaskRef,
	worker,
	suffix string,
) agent.LeaseGrant {
	t.Helper()
	assignment, err := service.AcquireLease(context.Background(), agent.AcquireLeaseCommand{
		ID: agent.CommandID("lease-" + suffix), Task: ref, Worker: agent.WorkerID(worker),
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := service.AcceptTask(context.Background(), agent.WorkerTaskCommand{
		Meta: agent.WorkerCommandMeta{ID: agent.CommandID("accept-" + suffix), ExpectedRevision: assignment.Task.Revision, Grant: assignment.Grant},
		Task: ref,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RequestInput(context.Background(), agent.WorkerMessageCommand{
		Meta: agent.WorkerCommandMeta{ID: agent.CommandID("ask-" + suffix), ExpectedRevision: accepted.Revision, Grant: assignment.Grant},
		Task: ref, Message: agent.MessageInput{
			ID:    agent.MessageID("agent-question-" + suffix),
			Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("more information")}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	return assignment.Grant
}

func publishA2ATestArtifact(t *testing.T, service *agent.Agent) (agent.ArtifactContent, agent.Task) {
	t.Helper()
	ctx := context.Background()
	ref := agent.TaskRef{
		Workspace: agent.WorkspaceRef{Authority: "authority-a", ID: "workspace-a"}, ID: "task-artifact",
	}
	created, err := service.CreateTask(ctx, agent.CreateTaskCommand{
		Meta: agent.CommandMeta{ID: "create-artifact"}, Task: ref,
		Context: agent.ContextRef{Authority: "authority-a", ID: "context-a"},
		Message: agent.MessageInput{ID: "artifact-request", Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("build")}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := service.AcquireLease(ctx, agent.AcquireLeaseCommand{ID: "lease-artifact", Task: ref, Worker: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	working, err := service.AcceptTask(ctx, agent.WorkerTaskCommand{
		Meta: agent.WorkerCommandMeta{ID: "accept-artifact", ExpectedRevision: created.Revision, Grant: assignment.Grant}, Task: ref,
	})
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := service.FreezeArtifact(ctx, agent.FreezeArtifactCommand{
		Meta: agent.WorkerCommandMeta{ID: "freeze-artifact", ExpectedRevision: working.Revision, Grant: assignment.Grant},
		Task: ref, Artifact: "artifact-a", ExpectedBaseRevision: "revision-base",
		Payload: workspace.Payload{MediaType: "application/json", Data: []byte(`{"path":"README.md"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	artifactRef := frozen.Artifact.Ref
	completed, err := service.CompleteTask(ctx, agent.CompleteTaskCommand{
		Meta: agent.WorkerCommandMeta{ID: "complete-artifact", ExpectedRevision: frozen.Task.Revision, Grant: assignment.Grant},
		Task: ref, Submission: "submission-a", Artifact: &artifactRef,
		Message: agent.MessageInput{ID: "artifact-final", Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("done")}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	content, err := service.GetArtifact(ctx, artifactRef)
	if err != nil {
		t.Fatal(err)
	}
	return content, completed
}
