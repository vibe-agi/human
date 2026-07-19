package workerkit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

const integrationCodecID llm.CodecID = "workerkit.integration"

type integrationCodec struct{}

func (integrationCodec) Description() llm.CodecDescription {
	return llm.CodecDescription{
		Contract: framework.Contract{ID: llm.CodecContractID, Major: llm.CodecContractMajor},
		ID:       integrationCodecID, Version: "1",
		Fingerprint: llm.Fingerprint([]byte("workerkit-integration-v1")),
		Limits: llm.CodecLimits{
			MaxRequestBytes: 1 << 20, MaxStreamFrameBytes: 1 << 20,
			MaxStreamFramesPerStep: 16, MaxAggregateBytes: 1 << 20,
			MaxAdmissionErrorBytes: 1 << 20,
		},
		OverloadedStatus: 503,
	}
}

func (integrationCodec) Decode(body []byte) (llm.Request, error) {
	var request llm.Request
	err := json.Unmarshal(body, &request)
	return request, err
}

func (integrationCodec) NewStream(session llm.EncoderSession) (llm.Encoder, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	return &integrationEncoder{}, nil
}

func (integrationCodec) NewAggregate(session llm.EncoderSession) (llm.Encoder, error) {
	return (integrationCodec{}).NewStream(session)
}

func (integrationCodec) AdmissionError(failure llm.AdmissionFailure) ([]byte, error) {
	return json.Marshal(failure)
}

type integrationEncoder struct{}

func (*integrationEncoder) Start() ([][]byte, error) { return [][]byte{[]byte("start\n")}, nil }

func (*integrationEncoder) Encode(event llm.Event, _ llm.EventSeed) ([][]byte, bool, error) {
	return [][]byte{fmt.Appendf(nil, "%s:%s\n", event.Type, event.Text)}, event.EndsResponse(), nil
}

func openIntegrationService(t *testing.T) *llm.Service {
	t.Helper()
	store, release := humantest.NewMemoryLLMStore()
	t.Cleanup(func() { _ = release(context.Background()) })
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "workerkit-integration",
		Store:        framework.Borrow[llm.Store](store),
		Codecs: []llm.CodecRegistration{{
			Codec: integrationCodec{}, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		}},
		Admission: llm.AdmitAll(),
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return "worker-a", nil
		}),
		ToolAuthorizer: llm.ToolAuthorizerFunc(func(context.Context, llm.ToolAuthorization) error {
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		select {
		case <-service.Done():
		default:
			if err := service.Shutdown(ctx); err != nil {
				t.Errorf("shutdown service: %v", err)
			}
		}
	})
	return service
}

func admitIntegration(t *testing.T, service *llm.Service, key string, request llm.Request, task llm.TaskContext) llm.AdmissionResult {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	result, admitErr := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: llm.IdempotencyKey(key),
		CodecID: integrationCodecID, Body: body, Task: task,
	})
	if admitErr != nil {
		t.Fatalf("admit %s: %v", key, admitErr)
	}
	return result
}

func readIntegrationResponse(t *testing.T, service *llm.Service, key string, digest llm.StoreDigest) llm.ResponsePage {
	t.Helper()
	page, err := service.ReadResponse(t.Context(), llm.ResponseQuery{
		CallerID: "caller-a", IdempotencyKey: llm.IdempotencyKey(key), RequestDigest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	return page
}

// TestWorkerAgainstRealServiceChatLoop proves the in-process adapter drives a
// real HumanLLM core through accept → progress → final, with the caller
// observing every durable event.
func TestWorkerAgainstRealServiceChatLoop(t *testing.T) {
	service := openIntegrationService(t)
	connection, err := service.OpenWorker(t.Context(), llm.AuthenticatedWorker{
		WorkerID: "worker-a", SessionID: "session-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, workerkit.WrapConnection(connection), store)

	result := admitIntegration(t, service, "chat-1", textOnlyRequest("please help"), llm.TaskContext{})
	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), worker.Snapshot().Inbox[0].Delivery)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Reply(t.Context(), key, "looking"); err != nil {
		t.Fatal(err)
	}
	if err := worker.Final(t.Context(), key, "answer"); err != nil {
		t.Fatal(err)
	}
	page := readIntegrationResponse(t, service, "chat-1", result.RequestDigest)
	if !page.Complete || len(page.Events) != 3 {
		t.Fatalf("caller response = %+v", page)
	}
	if string(page.Events[1].Data) != "progress:looking\n" || string(page.Events[2].Data) != "final:answer\n" {
		t.Fatalf("caller wire = %q %q", page.Events[1].Data, page.Events[2].Data)
	}
}

// TestWorkerAgainstRealServiceToolContinuation proves the full workspace-tier
// tool loop: tool calls end the response, the caller's continuation resumes
// the same conversation via task affinity, and the final answer lands on the
// continued request.
func TestWorkerAgainstRealServiceToolContinuation(t *testing.T) {
	service := openIntegrationService(t)
	connection, err := service.OpenWorker(t.Context(), llm.AuthenticatedWorker{
		WorkerID: "worker-a", SessionID: "session-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, workerkit.WrapConnection(connection), store)

	task := llm.TaskContext{
		CapabilityTier: llm.TierWorkspace, WorkspaceKey: "workspace-a",
		HarnessID: "harness-a", HarnessVersion: "v1", HarnessSessionID: "session-1",
		WorkspaceRoot: "/workspace", ExecAllowed: true,
	}
	request := textOnlyRequest("fix the bug")
	request.Tools = []llm.Tool{{Name: "bash", InputSchema: []byte(`{"type":"object"}`)}}
	first := admitIntegration(t, service, "tools-1", request, task)

	waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
	key, err := worker.Accept(t.Context(), worker.Snapshot().Inbox[0].Delivery)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.SubmitToolCalls(t.Context(), key, []llm.ToolCall{{
		ID: "call-1", Name: "bash", Input: map[string]any{"command": "ls"},
	}}); err != nil {
		t.Fatal(err)
	}
	firstPage := readIntegrationResponse(t, service, "tools-1", first.RequestDigest)
	if !firstPage.Complete {
		t.Fatalf("tool_calls did not end the first response: %+v", firstPage)
	}
	waitFor(t, worker, func(state workerkit.State) bool {
		return len(state.Conversations) == 1 && state.Conversations[0].Phase == workerkit.PhaseAwaitingResults
	})

	continuation := textOnlyRequest("continue")
	continuation.Tools = request.Tools
	continuation.Messages = append(continuation.Messages, llm.Message{
		Role: llm.RoleTool, Blocks: []llm.Block{{
			Type: llm.BlockToolResult, ToolCallID: "call-1", Output: map[string]any{"stdout": "main.go"},
		}},
	})
	second := admitIntegration(t, service, "tools-2", continuation, task)
	if second.Identity.TaskID != first.Identity.TaskID {
		t.Fatalf("continuation task affinity broke: %+v vs %+v", second.Identity, first.Identity)
	}

	state := waitFor(t, worker, func(state workerkit.State) bool {
		return len(state.Conversations) == 1 && state.Conversations[0].Phase == workerkit.PhaseActive
	})
	transcript := state.Conversations[0].Transcript
	if transcript[len(transcript)-1].Kind != workerkit.EntryToolResult {
		t.Fatalf("resume did not record the tool result: %+v", transcript)
	}
	if err := worker.Final(t.Context(), key, "fixed"); err != nil {
		t.Fatal(err)
	}
	secondPage := readIntegrationResponse(t, service, "tools-2", second.RequestDigest)
	if !secondPage.Complete {
		t.Fatalf("continuation response incomplete: %+v", secondPage)
	}
	if string(secondPage.Events[len(secondPage.Events)-1].Data) != "final:fixed\n" {
		t.Fatalf("continuation wire tail = %q", secondPage.Events[len(secondPage.Events)-1].Data)
	}
}
