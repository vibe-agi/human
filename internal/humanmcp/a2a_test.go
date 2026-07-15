package humanmcp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/internal/delegation"
)

type fakeProtocolClient struct {
	send   func(context.Context, *a2a.SendMessageRequest) (a2a.SendMessageResult, error)
	get    func(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error)
	list   func(context.Context, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error)
	cancel func(context.Context, *a2a.CancelTaskRequest) (*a2a.Task, error)
}

func (client *fakeProtocolClient) SendMessage(
	ctx context.Context,
	request *a2a.SendMessageRequest,
) (a2a.SendMessageResult, error) {
	return client.send(ctx, request)
}

func (client *fakeProtocolClient) GetTask(
	ctx context.Context,
	request *a2a.GetTaskRequest,
) (*a2a.Task, error) {
	return client.get(ctx, request)
}

func (client *fakeProtocolClient) ListTasks(
	ctx context.Context,
	request *a2a.ListTasksRequest,
) (*a2a.ListTasksResponse, error) {
	return client.list(ctx, request)
}

func (client *fakeProtocolClient) CancelTask(
	ctx context.Context,
	request *a2a.CancelTaskRequest,
) (*a2a.Task, error) {
	return client.cancel(ctx, request)
}

func remoteTask(id string, state a2a.TaskState, revision, latestTurn int64) *a2a.Task {
	return &a2a.Task{
		ID: a2a.TaskID(id), ContextID: "context-1", Status: a2a.TaskStatus{State: state},
		Metadata: map[string]any{delegation.MetadataKey: map[string]any{
			"revision": revision, "latestTurn": latestTurn, "nextTurn": latestTurn + 1,
		}},
	}
}

func remoteExecRequest(taskID, requestID string) delegation.ExecRequest {
	return delegation.ExecRequest{
		TaskID: taskID, ID: requestID, WorkerID: "worker-1", Command: "go test ./...",
		CWD: "subdir", TimeoutMS: 30_000, Reason: "verify the fix",
		Status: delegation.ExecPending, RequestSequence: 2,
		CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
}

func resolvedRemoteExecRequest(taskID, requestID, resolutionID string) delegation.ExecRequest {
	request := remoteExecRequest(taskID, requestID)
	exitCode := 0
	resolutionSequence := int64(3)
	resolvedAt := request.CreatedAt.Add(time.Second)
	request.Status = delegation.ExecCompleted
	request.ExitCode = &exitCode
	request.Stdout = []byte("ok\x00output")
	request.Stderr = []byte("warning")
	request.Truncated = true
	request.ResolutionID = resolutionID
	request.ResolutionSequence = &resolutionSequence
	request.ResolvedAt = &resolvedAt
	return request
}

func TestA2ADelegateUsesStableMessageContract(t *testing.T) {
	client := &fakeProtocolClient{}
	client.send = func(_ context.Context, request *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
		if request.Message.ID != "operation-1" || len(request.Message.Parts) != 1 ||
			request.Message.Parts[0].Text() != "fix the bug" || !request.Config.ReturnImmediately {
			t.Fatalf("send request = %+v", request)
		}
		if len(request.Message.ReferenceTasks) != 2 || request.Message.ReferenceTasks[0] != "old-1" {
			t.Fatalf("references = %+v", request.Message.ReferenceTasks)
		}
		metadata, ok := request.Metadata[delegation.RequestMetadataKey].(map[string]any)
		if !ok || metadata["baseCommit"] != "abc" || metadata["workspaceDigest"] != "tree" {
			t.Fatalf("request metadata = %#v", request.Metadata)
		}
		return remoteTask("task-1", a2a.TaskStateSubmitted, 1, 0), nil
	}
	authority := newA2AAuthority(client)
	task, err := authority.Delegate(context.Background(), DelegateInput{
		Prompt: " fix the bug ", BaseCommit: "abc", WorkspaceDigest: "tree",
		ReferenceTaskIDs: []string{"old-1", "old-1", "old-2"}, IdempotencyKey: "operation-1",
	})
	if err != nil || task.ID != "task-1" || task.State != StateSubmitted || task.Revision != 1 {
		t.Fatalf("delegate = %+v, %v", task, err)
	}
}

func TestA2AMutationsRequireStableIdempotencyKeysBeforeNetwork(t *testing.T) {
	authority := newA2AAuthority(&fakeProtocolClient{})
	if _, err := authority.Delegate(context.Background(), DelegateInput{Prompt: "fix it"}); err == nil || !strings.Contains(err.Error(), "idempotency key is required") {
		t.Fatalf("delegate without idempotency key error = %v", err)
	}
	if _, err := authority.Reply(context.Background(), "task-1", "continue", ""); err == nil || !strings.Contains(err.Error(), "idempotency key is required") {
		t.Fatalf("reply without idempotency key error = %v", err)
	}
	if _, err := authority.Cancel(context.Background(), "task-1", "stop", ""); err == nil || !strings.Contains(err.Error(), "idempotency key is required") {
		t.Fatalf("cancel without idempotency key error = %v", err)
	}
}

func TestA2ARejectsNonLoopbackPlaintextHTTPByDefault(t *testing.T) {
	_, err := NewA2AAuthority(context.Background(), A2AConfig{
		BaseURL: "http://192.0.2.1:8080", BearerToken: "secret",
	})
	if err == nil || !strings.Contains(err.Error(), "plaintext A2A HTTP") {
		t.Fatalf("non-loopback plaintext error = %v", err)
	}

	base, err := url.Parse("https://gateway.example")
	if err != nil {
		t.Fatal(err)
	}
	card := &a2a.AgentCard{SupportedInterfaces: []*a2a.AgentInterface{{URL: "http://api.example/a2a"}}}
	if err := validateCardInterfaces(base, card, true, false); err == nil || !strings.Contains(err.Error(), "plaintext HTTP") {
		t.Fatalf("insecure advertised interface error = %v", err)
	}
	if err := validateCardInterfaces(base, card, true, true); err != nil {
		t.Fatalf("explicit insecure opt-in rejected: %v", err)
	}
}

func TestA2ATaskArtifactMappingVerifiesTransportAndFileOracle(t *testing.T) {
	patch := []byte("diff --git a/README.md b/README.md\n")
	digest := sha256.Sum256(patch)
	remote := remoteTask("task-1", a2a.TaskStateInputRequired, 4, 2)
	part := a2a.NewRawPart(patch)
	part.MediaType = delegation.GitPatchMediaType
	remote.Artifacts = []*a2a.Artifact{{
		ID: "artifact-2", Parts: a2a.ContentParts{part},
		Metadata: map[string]any{
			"schema": "human-agent.git-patch.v1", "task_id": "task-1", "turn": 2,
			"base_commit": strings.Repeat("1", 40), "commit": strings.Repeat("2", 40), "incremental_patch": []byte("incremental"),
			"files": []map[string]any{{"path": "README.md", "blob_sha": strings.Repeat("3", 40), "mode": "100644"}},
			delegation.MetadataKey: map[string]any{
				"turn": 2, "sha256": hex.EncodeToString(digest[:]), "replace": true,
			},
		},
	}}
	task, err := fromA2ATask(remote)
	if err != nil {
		t.Fatal(err)
	}
	if task.Artifact == nil || task.Artifact.Turn != 2 || string(task.Artifact.CumulativePatch) != string(patch) ||
		string(task.Artifact.IncrementalPatch) != "incremental" || task.Artifact.Files[0].Path != "README.md" {
		t.Fatalf("mapped task = %+v", task)
	}
	artifactMetadata := remote.Artifacts[0].Metadata
	files := artifactMetadata["files"].([]map[string]any)
	files[0]["mode"] = "120000"
	if _, err := fromA2ATask(remote); err == nil || !strings.Contains(err.Error(), "file oracle") {
		t.Fatalf("unsupported mode error = %v", err)
	}
	files[0]["mode"] = "100644"
	integrity := artifactMetadata[delegation.MetadataKey].(map[string]any)
	integrity["sha256"] = "bad"
	if _, err := fromA2ATask(remote); err == nil {
		t.Fatal("corrupt artifact hash was accepted")
	}
	integrity["sha256"] = hex.EncodeToString(digest[:])
	artifactMetadata["schema"] = "human-agent.git-patch.v0"
	if _, err := fromA2ATask(remote); err == nil {
		t.Fatal("unknown artifact schema was accepted")
	}
	artifactMetadata["schema"] = "human-agent.git-patch.v1"
	artifactMetadata["turn"] = 1
	integrity["turn"] = 1
	if _, err := fromA2ATask(remote); err == nil || !strings.Contains(err.Error(), "authoritative latest turn") {
		t.Fatalf("stale artifact turn error = %v", err)
	}
}

func TestA2AArtifactAllowsEmptyCumulativeReplaceAtBase(t *testing.T) {
	digest := sha256.Sum256(nil)
	remote := remoteTask("task-empty", a2a.TaskStateInputRequired, 3, 1)
	part := a2a.NewRawPart([]byte{})
	part.MediaType = delegation.GitPatchMediaType
	remote.Artifacts = []*a2a.Artifact{{
		ID: "artifact-empty", Parts: a2a.ContentParts{part},
		Metadata: map[string]any{
			"schema": "human-agent.git-patch.v1", "task_id": "task-empty", "turn": 1,
			"base_commit": strings.Repeat("1", 40), "commit": strings.Repeat("2", 40),
			"incremental_patch": []byte{}, "files": []map[string]any{},
			delegation.MetadataKey: map[string]any{
				"turn": 1, "sha256": hex.EncodeToString(digest[:]), "replace": true,
			},
		},
	}}
	task, err := fromA2ATask(remote)
	if err != nil || task.Artifact == nil || len(task.Artifact.CumulativePatch) != 0 || len(task.Artifact.Files) != 0 {
		t.Fatalf("empty cumulative artifact = %+v, %v", task.Artifact, err)
	}
}

func TestA2ATaskProjectsExecRequestsWithIndependentResultData(t *testing.T) {
	pending := remoteExecRequest("task-1", "exec-1")
	completed := resolvedRemoteExecRequest("task-1", "exec-2", "resolution-2")
	completed.RequestSequence = 3
	resolutionSequence := int64(4)
	completed.ResolutionSequence = &resolutionSequence
	remote := remoteTask("task-1", a2a.TaskStateWorking, 4, 0)
	remote.Metadata[delegation.MetadataKey].(map[string]any)["execRequests"] = []delegation.ExecRequest{pending, completed}

	task, err := fromA2ATask(remote)
	if err != nil {
		t.Fatal(err)
	}
	if len(task.ExecRequests) != 2 || task.ExecRequests[0].Status != ExecPending ||
		task.ExecRequests[1].Status != ExecCompleted || task.ExecRequests[1].ExitCode == nil ||
		*task.ExecRequests[1].ExitCode != 0 ||
		task.ExecRequests[1].StdoutBase64 != base64.StdEncoding.EncodeToString([]byte("ok\x00output")) ||
		task.ExecRequests[1].StderrBase64 != base64.StdEncoding.EncodeToString([]byte("warning")) ||
		task.ExecRequests[1].ResolutionID != "resolution-2" {
		t.Fatalf("mapped exec requests = %+v", task.ExecRequests)
	}
	completed.Stdout[0] = 'X'
	*completed.ExitCode = 99
	if task.ExecRequests[1].StdoutBase64 != base64.StdEncoding.EncodeToString([]byte("ok\x00output")) ||
		*task.ExecRequests[1].ExitCode != 0 {
		t.Fatal("mapped exec result aliases authority wire memory")
	}
}

func TestA2ATaskRejectsInvalidExecAuthorityMetadata(t *testing.T) {
	tests := []struct {
		name     string
		requests func() []delegation.ExecRequest
	}{
		{"wrong task", func() []delegation.ExecRequest {
			request := remoteExecRequest("task-2", "exec-1")
			return []delegation.ExecRequest{request}
		}},
		{"duplicate request", func() []delegation.ExecRequest {
			request := remoteExecRequest("task-1", "exec-1")
			return []delegation.ExecRequest{request, request}
		}},
		{"unknown status", func() []delegation.ExecRequest {
			request := remoteExecRequest("task-1", "exec-1")
			request.Status = delegation.ExecStatus("unknown")
			return []delegation.ExecRequest{request}
		}},
		{"pending has output", func() []delegation.ExecRequest {
			request := remoteExecRequest("task-1", "exec-1")
			request.Stdout = []byte("must not exist")
			return []delegation.ExecRequest{request}
		}},
		{"completed lacks resolution", func() []delegation.ExecRequest {
			request := remoteExecRequest("task-1", "exec-1")
			exitCode := 0
			request.Status = delegation.ExecCompleted
			request.ExitCode = &exitCode
			return []delegation.ExecRequest{request}
		}},
		{"failed lacks failure", func() []delegation.ExecRequest {
			request := resolvedRemoteExecRequest("task-1", "exec-1", "resolution-1")
			request.Status = delegation.ExecFailed
			request.Error = ""
			request.TimedOut = false
			return []delegation.ExecRequest{request}
		}},
		{"denied has output", func() []delegation.ExecRequest {
			request := resolvedRemoteExecRequest("task-1", "exec-1", "resolution-1")
			request.Status = delegation.ExecDenied
			request.ExitCode = nil
			request.Error = "denied"
			return []delegation.ExecRequest{request}
		}},
		{"duplicate resolution", func() []delegation.ExecRequest {
			first := resolvedRemoteExecRequest("task-1", "exec-1", "resolution-1")
			second := resolvedRemoteExecRequest("task-1", "exec-2", "resolution-1")
			return []delegation.ExecRequest{first, second}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			remote := remoteTask("task-1", a2a.TaskStateWorking, 4, 0)
			remote.Metadata[delegation.MetadataKey].(map[string]any)["execRequests"] = test.requests()
			if _, err := fromA2ATask(remote); err == nil {
				t.Fatal("invalid exec authority metadata was accepted")
			}
		})
	}
}

func TestA2AResolveExecMapsCommandResultContract(t *testing.T) {
	client := &fakeProtocolClient{}
	client.get = func(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error) {
		remote := remoteTask("task-1", a2a.TaskStateWorking, 2, 0)
		remote.Metadata[delegation.MetadataKey].(map[string]any)["execRequests"] = []delegation.ExecRequest{
			remoteExecRequest("task-1", "exec-1"),
		}
		return remote, nil
	}
	client.send = func(_ context.Context, request *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
		if request.Message.ID != "resolution-1" || request.Message.TaskID != "task-1" ||
			request.Message.ContextID != "context-1" || request.Message.Parts[0].Text() != "command result" ||
			request.Config == nil || !request.Config.ReturnImmediately {
			t.Fatalf("command result message = %+v", request)
		}
		metadata, ok := request.Metadata[delegation.RequestMetadataKey].(map[string]any)
		if !ok || metadata["intent"] != "command_result" || metadata["requestId"] != "exec-1" ||
			metadata["resolutionId"] != "resolution-1" || metadata["approved"] != true ||
			metadata["exitCode"] != 7 || metadata["stdoutBase64"] != base64.StdEncoding.EncodeToString([]byte("stdout\x00")) ||
			metadata["stderrBase64"] != base64.StdEncoding.EncodeToString([]byte("stderr")) ||
			metadata["error"] != "failed" || metadata["truncated"] != true || metadata["timedOut"] != false {
			t.Fatalf("command result metadata = %#v", request.Metadata)
		}
		resolved := remoteTask("task-1", a2a.TaskStateWorking, 3, 0)
		result := resolvedRemoteExecRequest("task-1", "exec-1", "resolution-1")
		exitCode := 7
		result.ExitCode = &exitCode
		result.Stdout = []byte("stdout\x00")
		result.Stderr = []byte("stderr")
		result.Error = "failed"
		result.Status = delegation.ExecFailed
		result.Truncated = true
		resolved.Metadata[delegation.MetadataKey].(map[string]any)["execRequests"] = []delegation.ExecRequest{result}
		return resolved, nil
	}

	task, err := newA2AAuthority(client).ResolveExec(context.Background(), "task-1", ExecResolutionInput{
		RequestID: "exec-1", ResolutionID: "resolution-1", Approved: true,
		ExitCode: 7, Stdout: []byte("stdout\x00"), Stderr: []byte("stderr"), Error: "failed", Truncated: true,
	})
	if err != nil || task.ID != "task-1" || len(task.ExecRequests) != 1 ||
		task.ExecRequests[0].Status != ExecFailed {
		t.Fatalf("resolve command = %+v, %v", task, err)
	}
}

func TestA2AResolveExecReconcilesLostResponseAndReplaysLocally(t *testing.T) {
	gets, sends := 0, 0
	client := &fakeProtocolClient{}
	client.get = func(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error) {
		gets++
		remote := remoteTask("task-1", a2a.TaskStateWorking, int64(gets+1), 0)
		request := remoteExecRequest("task-1", "exec-1")
		if gets > 1 {
			request = resolvedRemoteExecRequest("task-1", "exec-1", "resolution-1")
		}
		remote.Metadata[delegation.MetadataKey].(map[string]any)["execRequests"] = []delegation.ExecRequest{request}
		return remote, nil
	}
	client.send = func(context.Context, *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
		sends++
		return nil, errors.New("response lost")
	}
	authority := newA2AAuthority(client)
	input := ExecResolutionInput{
		RequestID: "exec-1", ResolutionID: "resolution-1", Approved: true,
		Stdout: []byte("ok\x00output"), Stderr: []byte("warning"), Truncated: true,
	}
	first, err := authority.ResolveExec(context.Background(), "task-1", input)
	if err != nil || first.ExecRequests[0].Status != ExecCompleted || gets != 2 || sends != 1 {
		t.Fatalf("lost response reconciliation = %+v, %v, gets=%d sends=%d", first, err, gets, sends)
	}
	second, err := authority.ResolveExec(context.Background(), "task-1", input)
	if err != nil || second.ExecRequests[0].ResolutionID != "resolution-1" || gets != 3 || sends != 1 {
		t.Fatalf("local replay = %+v, %v, gets=%d sends=%d", second, err, gets, sends)
	}
}

func TestA2ARewindStateAndCancelLostResponseReconcile(t *testing.T) {
	remote := remoteTask("task-1", a2a.TaskStateInputRequired, 3, 0)
	remote.Metadata[delegation.MetadataKey].(map[string]any)["state"] = "rewind-pending"
	mapped, err := fromA2ATask(remote)
	if err != nil || mapped.State != StateRewindPending {
		t.Fatalf("rewind mapping = %+v, %v", mapped, err)
	}

	gets := 0
	client := &fakeProtocolClient{}
	client.get = func(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error) {
		gets++
		if gets == 1 {
			return remoteTask("task-1", a2a.TaskStateWorking, 2, 0), nil
		}
		return remoteTask("task-1", a2a.TaskStateCanceled, 3, 0), nil
	}
	client.cancel = func(context.Context, *a2a.CancelTaskRequest) (*a2a.Task, error) {
		return nil, errors.New("response lost")
	}
	task, err := newA2AAuthority(client).Cancel(context.Background(), "task-1", "stop", "cancel-1")
	if err != nil || task.State != StateCanceled || gets != 2 {
		t.Fatalf("cancel reconciliation = %+v, %v, gets=%d", task, err, gets)
	}
}

func TestA2AExistingTaskOperationsRejectMisdirectedTaskResponses(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		client := &fakeProtocolClient{get: func(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error) {
			return remoteTask("task-other", a2a.TaskStateWorking, 1, 0), nil
		}}
		if _, err := newA2AAuthority(client).GetTask(context.Background(), "task-1"); err == nil ||
			!strings.Contains(err.Error(), "instead of requested task") {
			t.Fatalf("misdirected get error = %v", err)
		}
	})

	t.Run("reply", func(t *testing.T) {
		client := &fakeProtocolClient{}
		client.get = func(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error) {
			return remoteTask("task-1", a2a.TaskStateWorking, 1, 0), nil
		}
		client.send = func(context.Context, *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
			return remoteTask("task-other", a2a.TaskStateWorking, 2, 0), nil
		}
		if _, err := newA2AAuthority(client).Reply(context.Background(), "task-1", "continue", "reply-1"); err == nil ||
			!strings.Contains(err.Error(), "instead of requested task") {
			t.Fatalf("misdirected reply error = %v", err)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		client := &fakeProtocolClient{}
		client.get = func(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error) {
			return remoteTask("task-1", a2a.TaskStateWorking, 1, 0), nil
		}
		client.cancel = func(context.Context, *a2a.CancelTaskRequest) (*a2a.Task, error) {
			return remoteTask("task-other", a2a.TaskStateCanceled, 2, 0), nil
		}
		if _, err := newA2AAuthority(client).Cancel(context.Background(), "task-1", "stop", "cancel-1"); err == nil ||
			!strings.Contains(err.Error(), "instead of requested task") {
			t.Fatalf("misdirected cancel error = %v", err)
		}
	})

	t.Run("resolve", func(t *testing.T) {
		client := &fakeProtocolClient{}
		client.get = func(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error) {
			remote := remoteTask("task-1", a2a.TaskStateWorking, 2, 0)
			remote.Metadata[delegation.MetadataKey].(map[string]any)["execRequests"] = []delegation.ExecRequest{
				remoteExecRequest("task-1", "exec-1"),
			}
			return remote, nil
		}
		client.send = func(context.Context, *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
			return remoteTask("task-other", a2a.TaskStateWorking, 3, 0), nil
		}
		if _, err := newA2AAuthority(client).ResolveExec(context.Background(), "task-1", ExecResolutionInput{
			RequestID: "exec-1", ResolutionID: "resolution-1", Approved: true,
		}); err == nil || !strings.Contains(err.Error(), "instead of requested task") {
			t.Fatalf("misdirected resolve error = %v", err)
		}
	})
}

func TestA2AResolveExecDoesNotSendWhenOwnedTaskLookupFails(t *testing.T) {
	sends := 0
	client := &fakeProtocolClient{}
	client.get = func(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error) {
		return nil, errors.New("task not found for caller")
	}
	client.send = func(context.Context, *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
		sends++
		return nil, nil
	}
	_, err := newA2AAuthority(client).ResolveExec(context.Background(), "task-1", ExecResolutionInput{
		RequestID: "exec-1", ResolutionID: "resolution-1", Approved: true,
	})
	if err == nil || sends != 0 {
		t.Fatalf("owner lookup failure = %v, sends=%d", err, sends)
	}
}
