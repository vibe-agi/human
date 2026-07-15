package a2aadapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/vibe-agi/human/internal/delegation"
)

func TestAgentCardAndBearerBoundary(t *testing.T) {
	t.Parallel()
	environment := newProtocolEnvironment(t)

	response, err := http.Get(environment.server.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("agent card status = %d", response.StatusCode)
	}
	var card a2a.AgentCard
	if err := json.NewDecoder(response.Body).Decode(&card); err != nil {
		t.Fatal(err)
	}
	if len(card.SupportedInterfaces) != 2 ||
		card.SupportedInterfaces[0].ProtocolBinding != a2a.TransportProtocolJSONRPC ||
		card.SupportedInterfaces[0].URL != "https://human.example/root"+DefaultJSONRPCPath ||
		card.SupportedInterfaces[1].ProtocolBinding != a2a.TransportProtocolHTTPJSON ||
		card.SupportedInterfaces[1].URL != "https://human.example/root"+DefaultHTTPJSONPath {
		t.Fatalf("supported interfaces = %+v", card.SupportedInterfaces)
	}
	if card.Capabilities.Streaming || card.Capabilities.PushNotifications || card.Capabilities.ExtendedAgentCard {
		t.Fatalf("advertised optional capabilities = %+v", card.Capabilities)
	}
	if len(card.Capabilities.Extensions) != 1 || card.Capabilities.Extensions[0].URI != MetadataKey ||
		len(card.DefaultOutputModes) != 1 || card.DefaultOutputModes[0] != delegation.GitPatchMediaType {
		t.Fatalf("delegation extension/output contract = %+v / %+v",
			card.Capabilities.Extensions, card.DefaultOutputModes)
	}
	if _, advertised := card.Capabilities.Extensions[0].Params["remoteExec"]; advertised {
		t.Fatal("default Agent Card advertised disabled remote execution")
	}
	enabled, err := NewServer(ServerConfig{
		Authority: environment.store, Authenticator: StaticBearerTokens{"alice-token": "caller-alice"},
		BaseURL: "https://human.example/root", Version: "test", RemoteExec: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	params := enabled.AgentCard().Capabilities.Extensions[0].Params
	if params["remoteExec"] != "explicit-caller-approval" {
		t.Fatalf("enabled remote exec declaration = %#v", params["remoteExec"])
	}
	scheme, ok := card.SecuritySchemes[BearerSchemeName].(a2a.HTTPAuthSecurityScheme)
	if !ok || scheme.Scheme != "Bearer" || len(card.SecurityRequirements) != 1 {
		t.Fatalf("bearer security declaration = %#v / %#v", card.SecuritySchemes, card.SecurityRequirements)
	}

	request, err := http.NewRequest(http.MethodPost, environment.server.URL+DefaultJSONRPCPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized ||
		response.Header.Get("WWW-Authenticate") != `Bearer realm="human-a2a"` {
		t.Fatalf("unauthenticated response = %d, challenge=%q",
			response.StatusCode, response.Header.Get("WWW-Authenticate"))
	}
}

func TestJSONRPCDelegationLifecycleAndOwnerIsolation(t *testing.T) {
	t.Parallel()
	environment := newProtocolEnvironment(t)
	ctx := context.Background()
	requestMetadata := map[string]any{
		"workspaceId": "workspace-1",
		"threadId":    "thread-1",
	}
	request := newSendRequest("message-1", "please investigate")
	request.Message.ContextID = "context-1"
	request.Metadata = map[string]any{
		RequestMetadataKey: requestMetadata,
		MetadataKey:        map[string]any{"state": "caller-spoof"},
	}
	started := time.Now()
	result, err := environment.jsonRPC.SendMessage(ctx, environment.alice, request)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("initial message did not return promptly")
	}
	task := requireTaskResult(t, result)
	if task.ID == "" || task.ContextID != "context-1" ||
		task.Status.State != a2a.TaskStateSubmitted || len(task.History) != 1 {
		t.Fatalf("initial task = %+v", task)
	}
	requestView := requireMetadataMap(t, task.Metadata, RequestMetadataKey)
	if requestView["workspaceId"] != "workspace-1" || requestView["threadId"] != "thread-1" {
		t.Fatalf("request metadata round-trip = %#v", requestView)
	}
	authorityView := requireMetadataMap(t, task.Metadata, MetadataKey)
	if authorityView["revision"] != float64(1) {
		t.Fatalf("authority metadata = %#v", authorityView)
	}
	if _, spoofed := authorityView["state"]; spoofed {
		t.Fatalf("caller wrote authority namespace: %#v", authorityView)
	}

	// A lost initial response is safe to retry: messageId is caller-scoped and
	// atomically binds the original generated task.
	replayed, err := environment.jsonRPC.SendMessage(ctx, environment.alice, request)
	if err != nil {
		t.Fatal(err)
	}
	if replayTask := requireTaskResult(t, replayed); replayTask.ID != task.ID {
		t.Fatalf("idempotent retry task = %q, want %q", replayTask.ID, task.ID)
	}
	conflicting := newSendRequest("message-1", "please investigate")
	conflicting.Message.ContextID = "context-1"
	conflicting.Metadata = map[string]any{
		RequestMetadataKey: map[string]any{"workspaceId": "different"},
	}
	if _, err := environment.jsonRPC.SendMessage(ctx, environment.alice, conflicting); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("mismatched idempotency parameters error = %v", err)
	}
	owned, err := environment.store.ListTasks(ctx, "caller-alice")
	if err != nil || len(owned) != 1 {
		t.Fatalf("authority tasks = %+v, err=%v", owned, err)
	}

	if _, err := environment.jsonRPC.GetTask(ctx, environment.bob,
		&a2a.GetTaskRequest{ID: task.ID}); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("cross-owner get error = %v", err)
	}
	bobList, err := environment.jsonRPC.ListTasks(ctx, environment.bob, &a2a.ListTasksRequest{})
	if err != nil || bobList.TotalSize != 0 || len(bobList.Tasks) != 0 {
		t.Fatalf("cross-owner list = %+v, err=%v", bobList, err)
	}
	if _, err := environment.jsonRPC.CancelTask(ctx, environment.bob,
		&a2a.CancelTaskRequest{ID: task.ID}); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("cross-owner cancel error = %v", err)
	}

	stored, err := environment.store.GetTask(ctx, string(task.ID))
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := environment.store.AcceptTask(ctx, delegation.AcceptTaskInput{
		CommandInput: delegation.CommandInput{TaskID: stored.ID, ExpectedRevision: stored.Revision},
		WorkerID:     "human-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = environment.store.DeliverTurn(ctx, delegation.DeliverTurnInput{
		CommandInput: delegation.CommandInput{
			TaskID: taskID(task), ExpectedRevision: accepted.Task.Revision,
		},
		ArtifactID: "artifact-1", ArtifactMediaType: delegation.GitPatchMediaType,
		ArtifactData:     []byte("cumulative-v1"),
		ArtifactMetadata: []byte(`{"humanField":"kept","` + MetadataKey + `":{"replace":false}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := environment.jsonRPC.GetTask(ctx, environment.alice,
		&a2a.GetTaskRequest{ID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	assertArtifactProjection(t, projected, "artifact-1", delegation.GitPatchMediaType, "cumulative-v1", 1)
	if projected.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("first delivery state = %s", projected.Status.State)
	}
	if projected.Artifacts[0].Metadata["humanField"] != "kept" {
		t.Fatalf("artifact custom metadata = %#v", projected.Artifacts[0].Metadata)
	}

	continuation := newSendRequest("message-2", "continue with fixes")
	continuation.Message.TaskID = task.ID
	continuation.Message.ContextID = task.ContextID
	continuedResult, err := environment.jsonRPC.SendMessage(ctx, environment.alice, continuation)
	if err != nil {
		t.Fatal(err)
	}
	continued := requireTaskResult(t, continuedResult)
	if continued.Status.State != a2a.TaskStateWorking || len(continued.History) != 2 {
		t.Fatalf("continued task = %+v", continued)
	}
	// The same continuation remains idempotent even after its original revision
	// has advanced.
	if _, err := environment.jsonRPC.SendMessage(ctx, environment.alice, continuation); err != nil {
		t.Fatalf("continuation replay: %v", err)
	}

	second, err := environment.store.DeliverTurn(ctx, delegation.DeliverTurnInput{
		CommandInput: delegation.CommandInput{
			TaskID: taskID(task), ExpectedRevision: continuedRevision(t, continued),
		},
		ArtifactID: "artifact-2", ArtifactMediaType: delegation.GitPatchMediaType,
		ArtifactData: []byte("cumulative-v2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	projected, err = environment.jsonRPC.GetTask(ctx, environment.alice,
		&a2a.GetTaskRequest{ID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	assertArtifactProjection(t, projected, "artifact-2", delegation.GitPatchMediaType, "cumulative-v2", 2)
	if len(projected.Artifacts) != 1 {
		t.Fatalf("cumulative replace artifact count = %d", len(projected.Artifacts))
	}

	pending, err := environment.store.RequestRewind(ctx, delegation.RequestRewindInput{
		CommandInput: delegation.CommandInput{
			TaskID: taskID(task), ExpectedRevision: second.Task.Revision,
		},
		TargetTurn: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	projected, err = environment.jsonRPC.GetTask(ctx, environment.alice,
		&a2a.GetTaskRequest{ID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if projected.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("rewind-pending leaked as nonstandard state: %s", projected.Status.State)
	}
	authorityView = requireMetadataMap(t, projected.Metadata, MetadataKey)
	if authorityView["state"] != string(delegation.StateRewindPending) || authorityView["rewindTo"] != float64(1) {
		t.Fatalf("rewind metadata = %#v", authorityView)
	}
	if pending.Task.Revision != int64(authorityView["revision"].(float64)) {
		t.Fatalf("rewind revision = %d, metadata=%#v", pending.Task.Revision, authorityView)
	}

	filtered, err := environment.jsonRPC.ListTasks(ctx, environment.alice, &a2a.ListTasksRequest{
		ContextID: task.ContextID, Status: a2a.TaskStateInputRequired,
	})
	if err != nil || filtered.TotalSize != 1 || len(filtered.Tasks) != 1 {
		t.Fatalf("filtered list = %+v, err=%v", filtered, err)
	}
	if len(filtered.Tasks[0].Artifacts) != 0 {
		t.Fatalf("list included artifacts without opt-in: %+v", filtered.Tasks[0].Artifacts)
	}
}

func TestPushConfigIsRejectedInsteadOfSilentlyIgnored(t *testing.T) {
	t.Parallel()
	environment := newProtocolEnvironment(t)
	request := newSendRequest("message-push", "notify me")
	request.Config = &a2a.SendMessageConfig{PushConfig: &a2a.PushConfig{}}
	if _, err := environment.jsonRPC.SendMessage(
		context.Background(), environment.alice, request,
	); !errors.Is(err, a2a.ErrPushNotificationNotSupported) {
		t.Fatalf("push config error = %v", err)
	}
	tasks, err := environment.store.ListTasks(context.Background(), "caller-alice")
	if err != nil || len(tasks) != 0 {
		t.Fatalf("tasks after rejected push config = %+v, err=%v", tasks, err)
	}
	request = newSendRequest("message-wrong-mode", "wrong output")
	request.Config = &a2a.SendMessageConfig{AcceptedOutputModes: []string{"image/png"}}
	if _, err := environment.jsonRPC.SendMessage(
		context.Background(), environment.alice, request,
	); !errors.Is(err, a2a.ErrUnsupportedContentType) {
		t.Fatalf("unsupported output mode error = %v", err)
	}
}

func TestJSONRPCListPaginationAndCancel(t *testing.T) {
	t.Parallel()
	environment := newProtocolEnvironment(t)
	ctx := context.Background()
	first := sendNewTask(t, ctx, environment.jsonRPC, environment.alice, "message-a", "one")
	second := sendNewTask(t, ctx, environment.jsonRPC, environment.alice, "message-b", "two")

	pageOne, err := environment.jsonRPC.ListTasks(ctx, environment.alice,
		&a2a.ListTasksRequest{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if pageOne.TotalSize != 2 || len(pageOne.Tasks) != 1 || pageOne.NextPageToken == "" {
		t.Fatalf("first page = %+v", pageOne)
	}
	pageTwo, err := environment.jsonRPC.ListTasks(ctx, environment.alice, &a2a.ListTasksRequest{
		PageSize: 1, PageToken: pageOne.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if pageTwo.TotalSize != 2 || len(pageTwo.Tasks) != 1 || pageTwo.NextPageToken != "" ||
		pageTwo.Tasks[0].ID == pageOne.Tasks[0].ID {
		t.Fatalf("second page = %+v", pageTwo)
	}

	canceled, err := environment.jsonRPC.CancelTask(ctx, environment.alice,
		&a2a.CancelTaskRequest{ID: second.ID, Metadata: map[string]any{"reason": "stop"}})
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("canceled state = %s", canceled.Status.State)
	}
	if _, err := environment.jsonRPC.CancelTask(ctx, environment.alice,
		&a2a.CancelTaskRequest{ID: second.ID}); !errors.Is(err, a2a.ErrTaskNotCancelable) {
		t.Fatalf("repeat cancel error = %v", err)
	}
	unchanged, err := environment.jsonRPC.GetTask(ctx, environment.alice, &a2a.GetTaskRequest{ID: first.ID})
	if err != nil || unchanged.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("uncanceled task = %+v, err=%v", unchanged, err)
	}
}

func TestHTTPJSONOfficialTransport(t *testing.T) {
	t.Parallel()
	environment := newProtocolEnvironment(t)
	ctx := context.Background()
	task := sendNewTask(t, ctx, environment.httpJSON, environment.alice,
		"message-rest", "through HTTP+JSON")
	got, err := environment.httpJSON.GetTask(ctx, environment.alice, &a2a.GetTaskRequest{ID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != task.ID || got.ContextID != task.ContextID || got.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("HTTP+JSON get = %+v", got)
	}
	listed, err := environment.httpJSON.ListTasks(ctx, environment.alice, &a2a.ListTasksRequest{})
	if err != nil || listed.TotalSize != 1 || len(listed.Tasks) != 1 {
		t.Fatalf("HTTP+JSON list = %+v, err=%v", listed, err)
	}
	canceled, err := environment.httpJSON.CancelTask(ctx, environment.alice,
		&a2a.CancelTaskRequest{ID: task.ID})
	if err != nil || canceled.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("HTTP+JSON cancel = %+v, err=%v", canceled, err)
	}
}

func TestRemoteExecRoundTripThroughOfficialTransports(t *testing.T) {
	t.Parallel()
	transports := []struct {
		name            string
		selectTransport func(protocolEnvironment) a2aclient.Transport
	}{
		{name: "json-rpc", selectTransport: func(environment protocolEnvironment) a2aclient.Transport {
			return environment.jsonRPC
		}},
		{name: "http-json", selectTransport: func(environment protocolEnvironment) a2aclient.Transport {
			return environment.httpJSON
		}},
	}
	for _, testCase := range transports {
		t.Run(testCase.name, func(t *testing.T) {
			environment := newProtocolEnvironmentWithRemoteExec(t, true)
			transport := testCase.selectTransport(environment)
			ctx := context.Background()
			task := sendNewTask(t, ctx, transport, environment.alice,
				"exec-delegate-"+testCase.name, "diagnose the attached device")
			stored, err := environment.store.GetTask(ctx, string(task.ID))
			if err != nil {
				t.Fatal(err)
			}
			accepted, err := environment.store.AcceptTask(ctx, delegation.AcceptTaskInput{
				CommandInput: delegation.CommandInput{TaskID: stored.ID, ExpectedRevision: stored.Revision},
				WorkerID:     "human-worker",
			})
			if err != nil {
				t.Fatal(err)
			}
			requested, err := environment.store.RequestExec(ctx, delegation.RequestExecInput{
				CommandInput: delegation.CommandInput{
					TaskID: stored.ID, ExpectedRevision: accepted.Task.Revision,
				},
				WorkerID: "human-worker", RequestID: "exec-devices", Command: "adb devices -l",
				CWD: ".", TimeoutMS: 30_000, Reason: "inspect connected device state",
			})
			if err != nil {
				t.Fatal(err)
			}
			if requested.Task.State != delegation.StateWorking || requested.Request.Status != delegation.ExecPending {
				t.Fatalf("requested exec = %+v", requested)
			}

			pending, err := transport.GetTask(ctx, environment.alice, &a2a.GetTaskRequest{ID: task.ID})
			if err != nil {
				t.Fatal(err)
			}
			if pending.Status.State != a2a.TaskStateWorking {
				t.Fatalf("pending exec changed task state to %s", pending.Status.State)
			}
			pendingView := requireExecRequestView(t, pending, 0)
			if pendingView["request_id"] != "exec-devices" || pendingView["command"] != "adb devices -l" ||
				pendingView["status"] != string(delegation.ExecPending) {
				t.Fatalf("pending exec projection = %#v", pendingView)
			}
			if revision := continuedRevision(t, pending); revision != requested.Task.Revision {
				t.Fatalf("pending projection revision = %d, want %d", revision, requested.Task.Revision)
			}
			if _, err := transport.GetTask(ctx, environment.bob, &a2a.GetTaskRequest{ID: task.ID}); !errors.Is(err, a2a.ErrTaskNotFound) {
				t.Fatalf("cross-owner pending exec get error = %v", err)
			}
			missingApproval := newExecResolutionRequest(task, "exec-devices", "exec-resolution-missing-approval", nil, nil)
			delete(missingApproval.Metadata[RequestMetadataKey].(map[string]any), "approved")
			if _, err := transport.SendMessage(ctx, environment.alice, missingApproval); !errors.Is(err, a2a.ErrInvalidParams) {
				t.Fatalf("implicit command approval error = %v", err)
			}
			stillPending, err := environment.store.GetTask(ctx, stored.ID)
			if err != nil || stillPending.Revision != requested.Task.Revision {
				t.Fatalf("invalid approval mutated task = %+v, %v", stillPending, err)
			}

			stdout := []byte{0, 'd', 'e', 'v', 'i', 'c', 'e', '\n'}
			stderr := []byte("adb warning\n")
			resolution := newExecResolutionRequest(task, "exec-devices", "exec-resolution-1", stdout, stderr)
			resolvedResult, err := transport.SendMessage(ctx, environment.alice, resolution)
			if err != nil {
				t.Fatal(err)
			}
			resolvedTask := requireTaskResult(t, resolvedResult)
			if resolvedTask.Status.State != a2a.TaskStateWorking ||
				continuedRevision(t, resolvedTask) != requested.Task.Revision+1 || len(resolvedTask.History) != 1 {
				t.Fatalf("resolved A2A task = %+v", resolvedTask)
			}
			resolvedSnapshot, err := environment.store.LoadSnapshot(ctx, stored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if resolvedSnapshot.Task.State != delegation.StateWorking || len(resolvedSnapshot.Exec) != 1 ||
				resolvedSnapshot.Exec[0].Status != delegation.ExecCompleted ||
				resolvedSnapshot.Exec[0].ExitCode == nil || *resolvedSnapshot.Exec[0].ExitCode != 0 ||
				!bytes.Equal(resolvedSnapshot.Exec[0].Stdout, stdout) ||
				!bytes.Equal(resolvedSnapshot.Exec[0].Stderr, stderr) {
				t.Fatalf("resolved authority snapshot = %+v", resolvedSnapshot)
			}
			resolvedRevision := resolvedSnapshot.Task.Revision
			resolvedEvents := len(resolvedSnapshot.Events)

			// A lost response can replay the exact A2A request without another
			// revision or event, even though the supplied expected revision is now old.
			replayed, err := transport.SendMessage(ctx, environment.alice, resolution)
			if err != nil {
				t.Fatal(err)
			}
			if continuedRevision(t, requireTaskResult(t, replayed)) != resolvedRevision {
				t.Fatal("exact exec resolution retry changed projected revision")
			}
			afterReplay, err := environment.store.LoadSnapshot(ctx, stored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if afterReplay.Task.Revision != resolvedRevision || len(afterReplay.Events) != resolvedEvents {
				t.Fatalf("exact retry mutated authority: revision=%d events=%d", afterReplay.Task.Revision, len(afterReplay.Events))
			}

			changed := newExecResolutionRequest(task, "exec-devices", "exec-resolution-1", []byte("changed"), stderr)
			if _, err := transport.SendMessage(ctx, environment.alice, changed); !errors.Is(err, a2a.ErrInvalidParams) {
				t.Fatalf("changed resolution with same id/key error = %v", err)
			}
			if _, err := transport.SendMessage(ctx, environment.bob, resolution); !errors.Is(err, a2a.ErrTaskNotFound) {
				t.Fatalf("cross-owner exec resolution error = %v", err)
			}
			missing := newExecResolutionRequest(task, "exec-never-requested", "exec-resolution-missing", nil, nil)
			if _, err := transport.SendMessage(ctx, environment.alice, missing); !errors.Is(err, a2a.ErrInvalidParams) {
				t.Fatalf("forged unrequested exec resolution error = %v", err)
			}
			finalSnapshot, err := environment.store.LoadSnapshot(ctx, stored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if finalSnapshot.Task.State != delegation.StateWorking || finalSnapshot.Task.Revision != resolvedRevision ||
				len(finalSnapshot.Events) != resolvedEvents {
				t.Fatalf("rejected exec attempts mutated task = %+v", finalSnapshot.Task)
			}
		})
	}
}

func TestRemoteExecDisabledRejectsResolutionAndHidesProjection(t *testing.T) {
	t.Parallel()
	environment := newProtocolEnvironment(t)
	ctx := context.Background()
	task := sendNewTask(t, ctx, environment.jsonRPC, environment.alice,
		"exec-disabled-delegate", "do not expose remote exec")
	stored, err := environment.store.GetTask(ctx, string(task.ID))
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := environment.store.AcceptTask(ctx, delegation.AcceptTaskInput{
		CommandInput: delegation.CommandInput{TaskID: stored.ID, ExpectedRevision: stored.Revision},
		WorkerID:     "human-worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	requested, err := environment.store.RequestExec(ctx, delegation.RequestExecInput{
		CommandInput: delegation.CommandInput{TaskID: stored.ID, ExpectedRevision: accepted.Task.Revision},
		WorkerID:     "human-worker", RequestID: "disabled-request", Command: "pwd", Reason: "verify cwd",
	})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := environment.jsonRPC.GetTask(ctx, environment.alice, &a2a.GetTaskRequest{ID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	authorityView := requireMetadataMap(t, projected.Metadata, MetadataKey)
	if _, exposed := authorityView["execRequests"]; exposed {
		t.Fatalf("disabled remote exec projection leaked requests: %#v", authorityView)
	}
	resolution := newExecResolutionRequest(task, "disabled-request", "disabled-resolution", []byte("cwd"), nil)
	if _, err := environment.jsonRPC.SendMessage(ctx, environment.alice, resolution); !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Fatalf("disabled remote exec resolution error = %v", err)
	}
	after, err := environment.store.LoadSnapshot(ctx, stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Task.Revision != requested.Task.Revision || after.Task.State != delegation.StateWorking ||
		len(after.Exec) != 1 || after.Exec[0].Status != delegation.ExecPending {
		t.Fatalf("disabled resolution mutated authority = %+v", after)
	}
}

func TestConcurrentProtocolRetryCreatesOneTask(t *testing.T) {
	t.Parallel()
	environment := newProtocolEnvironment(t)
	ctx := context.Background()
	request := newSendRequest("same-message", "same body")
	start := make(chan struct{})
	results := make(chan a2a.TaskID, 8)
	errorsCh := make(chan error, 8)
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := environment.jsonRPC.SendMessage(ctx, environment.alice, request)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- requireTaskResult(t, result).ID
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	var taskID a2a.TaskID
	for resultID := range results {
		if taskID == "" {
			taskID = resultID
		} else if resultID != taskID {
			t.Fatalf("concurrent protocol task IDs = %q and %q", taskID, resultID)
		}
	}
	tasks, err := environment.store.ListTasks(ctx, "caller-alice")
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks after protocol retry = %+v, err=%v", tasks, err)
	}
}

type protocolEnvironment struct {
	store    *delegation.Store
	server   *httptest.Server
	jsonRPC  a2aclient.Transport
	httpJSON a2aclient.Transport
	alice    a2aclient.ServiceParams
	bob      a2aclient.ServiceParams
}

func newProtocolEnvironment(t *testing.T) protocolEnvironment {
	return newProtocolEnvironmentWithRemoteExec(t, false)
}

func newProtocolEnvironmentWithRemoteExec(t *testing.T, remoteExec bool) protocolEnvironment {
	t.Helper()
	store, err := delegation.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "delegation.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	adapter, err := NewServer(ServerConfig{
		Authority: store, Authenticator: StaticBearerTokens{
			"alice-token": "caller-alice", "bob-token": "caller-bob",
		},
		BaseURL: "https://human.example/root", Version: "test", RemoteExec: remoteExec,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(adapter.Handler())
	t.Cleanup(server.Close)
	restURL, err := url.Parse(server.URL + DefaultHTTPJSONPath)
	if err != nil {
		t.Fatal(err)
	}
	return protocolEnvironment{
		store: store, server: server,
		jsonRPC:  a2aclient.NewJSONRPCTransport(server.URL+DefaultJSONRPCPath, server.Client()),
		httpJSON: a2aclient.NewRESTTransport(restURL, server.Client()),
		alice:    bearerParams("alice-token"), bob: bearerParams("bob-token"),
	}
}

func bearerParams(token string) a2aclient.ServiceParams {
	return a2aclient.ServiceParams{"Authorization": {"Bearer " + token}}
}

func newSendRequest(messageID string, text string) *a2a.SendMessageRequest {
	return &a2a.SendMessageRequest{Message: &a2a.Message{
		ID: messageID, Role: a2a.MessageRoleUser,
		Parts: a2a.ContentParts{a2a.NewTextPart(text)},
	}}
}

func newExecResolutionRequest(
	task *a2a.Task,
	requestID string,
	resolutionID string,
	stdout []byte,
	stderr []byte,
) *a2a.SendMessageRequest {
	request := newSendRequest(resolutionID, "caller command result")
	request.Message.TaskID = task.ID
	request.Message.ContextID = task.ContextID
	request.Metadata = map[string]any{
		RequestMetadataKey: map[string]any{
			"intent": "command_result", "requestId": requestID, "resolutionId": resolutionID,
			"approved": true, "exitCode": 0,
			"stdoutBase64": base64.StdEncoding.EncodeToString(stdout),
			"stderrBase64": base64.StdEncoding.EncodeToString(stderr),
			"error":        "", "truncated": false, "timedOut": false,
		},
	}
	return request
}

func sendNewTask(
	t *testing.T,
	ctx context.Context,
	transport a2aclient.Transport,
	params a2aclient.ServiceParams,
	messageID string,
	text string,
) *a2a.Task {
	t.Helper()
	result, err := transport.SendMessage(ctx, params, newSendRequest(messageID, text))
	if err != nil {
		t.Fatal(err)
	}
	return requireTaskResult(t, result)
}

func requireTaskResult(t *testing.T, result a2a.SendMessageResult) *a2a.Task {
	t.Helper()
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("send result type = %T", result)
	}
	return task
}

func requireMetadataMap(t *testing.T, metadata map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := metadata[key].(map[string]any)
	if !ok {
		t.Fatalf("metadata[%q] = %#v", key, metadata[key])
	}
	return value
}

func requireExecRequestView(t *testing.T, task *a2a.Task, index int) map[string]any {
	t.Helper()
	authority := requireMetadataMap(t, task.Metadata, MetadataKey)
	requests, ok := authority["execRequests"].([]any)
	if !ok || index < 0 || index >= len(requests) {
		t.Fatalf("execRequests = %#v", authority["execRequests"])
	}
	request, ok := requests[index].(map[string]any)
	if !ok {
		t.Fatalf("execRequests[%d] = %#v", index, requests[index])
	}
	return request
}

func assertArtifactProjection(
	t *testing.T,
	task *a2a.Task,
	artifactID string,
	mediaType string,
	data string,
	turn int64,
) {
	t.Helper()
	if len(task.Artifacts) != 1 {
		t.Fatalf("artifacts = %+v", task.Artifacts)
	}
	artifact := task.Artifacts[0]
	if string(artifact.ID) != artifactID || len(artifact.Parts) != 1 ||
		artifact.Parts[0].MediaType != mediaType || string(artifact.Parts[0].Raw()) != data {
		t.Fatalf("artifact projection = %+v", artifact)
	}
	metadata := requireMetadataMap(t, artifact.Metadata, MetadataKey)
	if metadata["turn"] != float64(turn) || metadata["replace"] != true || metadata["sha256"] == "" {
		t.Fatalf("artifact authority metadata = %#v", metadata)
	}
}

func continuedRevision(t *testing.T, task *a2a.Task) int64 {
	t.Helper()
	metadata := requireMetadataMap(t, task.Metadata, MetadataKey)
	revision, ok := metadata["revision"].(float64)
	if !ok {
		t.Fatalf("task revision metadata = %#v", metadata)
	}
	return int64(revision)
}

func taskID(task *a2a.Task) string { return string(task.ID) }
