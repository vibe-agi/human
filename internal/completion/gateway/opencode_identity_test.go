package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
)

func TestOpenCodeWorkspaceHeadersProduceStableNativeAssignmentAndRetry(t *testing.T) {
	fixture := newGatewayFixture(t, true)
	if err := fixture.registry.Register(adapter.OpenCode11718Profile()); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{
      "model":"human-expert","stream":true,
      "messages":[{"role":"user","content":"edit the fixture"}],
      "tools":[
        {"type":"function","function":{"name":"edit","parameters":{"type":"object","properties":{"filePath":{"type":"string"},"oldString":{"type":"string"},"newString":{"type":"string"}},"required":["filePath","oldString","newString"]}}},
        {"type":"function","function":{"name":"bash","parameters":{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}}}
      ]
    }`)
	newRequest := func() *http.Request {
		request := newChatRequest(t, fixture, body, "")
		request.Header.Set("User-Agent", "opencode/1.17.18 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14")
		request.Header.Set(headerCapabilityTier, string(completion.TierWorkspace))
		request.Header.Set(headerWorkspaceKey, "repo-fixture")
		request.Header.Set(headerHarnessID, adapter.OpenCodeID)
		request.Header.Set(headerHarnessVersion, adapter.OpenCodeVersion)
		request.Header.Set(headerWorkspaceRoot, "/repo/fixture")
		request.Header.Set(headerOpenCodeSessionID, "ses_fixture_123")
		request.Header.Set(headerOpenCodeSessionAffinity, "ses_fixture_123")
		return request
	}

	done := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if !strings.HasPrefix(assignment.TaskID, openCodeTaskPrefix) || assignment.WorkspaceKey != "repo-fixture" ||
			assignment.CapabilityTier != completion.TierWorkspace || assignment.Root != "/repo/fixture" ||
			assignment.Adapter == nil || assignment.Adapter.Key() != adapter.OpenCodeID+"@"+adapter.OpenCodeVersion ||
			!strings.HasPrefix(assignment.IdempotencyKey, openCodeDerivedKeyPrefix) {
			done <- fmt.Errorf("OpenCode assignment identity = %+v", assignment)
			return
		}
		if len(assignment.Request.Tools) != 2 || assignment.Request.Tools[0].Name != "edit" {
			done <- fmt.Errorf("native tools were not preserved: %+v", assignment.Request.Tools)
			return
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
			completion.Event{ID: "accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID}); err != nil {
			done <- err
			return
		}
		done <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey,
			completion.Event{ID: "final", Type: completion.EventFinal, Text: "ready"})
	}()

	response, err := http.DefaultClient.Do(newRequest())
	if err != nil {
		t.Fatal(err)
	}
	first, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("first OpenCode response = %d, %q, %v", response.StatusCode, first, readErr)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := response.Header.Get(headerTaskID); !strings.HasPrefix(got, openCodeTaskPrefix) {
		t.Fatalf("response task id = %q", got)
	}
	if !strings.HasPrefix(response.Header.Get(headerIdempotencyKey), openCodeDerivedKeyPrefix) {
		t.Fatalf("response idempotency key = %q", response.Header.Get(headerIdempotencyKey))
	}

	replay, err := http.DefaultClient.Do(newRequest())
	if err != nil {
		t.Fatal(err)
	}
	second, readErr := io.ReadAll(replay.Body)
	replay.Body.Close()
	if readErr != nil || !bytes.Equal(second, first) {
		t.Fatalf("OpenCode retry was not exact: first=%q second=%q err=%v", first, second, readErr)
	}
	select {
	case duplicate := <-fixture.worker.Assignments:
		t.Fatalf("OpenCode retry dispatched duplicate: %+v", duplicate)
	default:
	}
}

func TestOpenCodeSessionCompletesTaskIdentityWithoutGuessingOtherHarnesses(t *testing.T) {
	header := make(http.Header)
	header.Set(headerOpenCodeSessionID, "ses_stable_123")
	header.Set(headerOpenCodeSessionAffinity, "ses_stable_123")
	identity := completion.RoutingIdentity{
		CallerID: "caller", WorkspaceKey: "workspace", IdempotencyKey: "request", HarnessID: adapter.OpenCodeID,
		HarnessVersion: adapter.OpenCodeVersion, Root: "/repo",
	}
	request := canonical.Request{
		Dialect: canonical.DialectOpenAIChat, Model: "human-expert",
		Messages: []canonical.Message{{Role: canonical.RoleUser, Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "first turn"}}}},
	}
	if err := completeOpenCodeIdentity(&identity, header, "opencode/1.17.18 ai-sdk/runtime", request); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(identity.TaskID, openCodeTaskPrefix) || identity.HarnessSessionID != "ses_stable_123" {
		t.Fatalf("completed identity = %+v", identity)
	}
	if err := identity.Validate(completion.TierWorkspace); err != nil {
		t.Fatalf("completed identity is invalid: %v", err)
	}

	other := completion.RoutingIdentity{HarnessID: "other", HarnessVersion: adapter.OpenCodeVersion}
	if err := completeOpenCodeIdentity(&other, header, "opencode/1.17.18 ai-sdk/runtime", request); err != nil || other.TaskID != "" {
		t.Fatalf("unrecognized harness was modified: %+v, %v", other, err)
	}
	newer := completion.RoutingIdentity{HarnessID: adapter.OpenCodeID, HarnessVersion: "1.17.19"}
	if err := completeOpenCodeIdentity(&newer, header, "opencode/1.17.19 ai-sdk/runtime", request); err != nil || newer.TaskID != "" {
		t.Fatalf("unregistered version was modified: %+v, %v", newer, err)
	}
}

func TestOpenCodeTurnTaskIdentitySpansToolResultsButNotTheNextUserTurn(t *testing.T) {
	first := canonical.Request{
		Dialect: canonical.DialectOpenAIChat, Model: "human-expert",
		Messages: []canonical.Message{{
			Role:   canonical.RoleUser,
			Blocks: []canonical.Block{{Type: canonical.BlockText, Text: "same words"}},
		}},
	}
	continuation := first
	continuation.Messages = append(append([]canonical.Message(nil), first.Messages...),
		canonical.Message{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockToolUse, ToolCallID: "edit-1", ToolName: "edit",
			Input: map[string]any{"filePath": "/repo/a", "oldString": "a", "newString": "b"},
		}}},
		canonical.Message{Role: canonical.RoleTool, Blocks: []canonical.Block{{
			Type: canonical.BlockToolResult, ToolCallID: "edit-1", Output: "Edit applied successfully.",
		}}},
	)
	nextTurn := continuation
	nextTurn.Messages = append(append([]canonical.Message(nil), continuation.Messages...),
		canonical.Message{Role: canonical.RoleAssistant, Blocks: []canonical.Block{{
			Type: canonical.BlockText, Text: "done",
		}}},
		canonical.Message{Role: canonical.RoleUser, Blocks: []canonical.Block{{
			Type: canonical.BlockText, Text: "same words",
		}}},
	)

	firstID, err := openCodeTurnTaskID("ses_one", first)
	if err != nil {
		t.Fatal(err)
	}
	continuationID, err := openCodeTurnTaskID("ses_one", continuation)
	if err != nil {
		t.Fatal(err)
	}
	nextID, err := openCodeTurnTaskID("ses_one", nextTurn)
	if err != nil {
		t.Fatal(err)
	}
	otherSessionID, err := openCodeTurnTaskID("ses_two", first)
	if err != nil {
		t.Fatal(err)
	}
	if firstID != continuationID {
		t.Fatalf("tool continuation changed task: %s != %s", firstID, continuationID)
	}
	if nextID == firstID {
		t.Fatal("a repeated user message in the next turn reused the terminal task")
	}
	if otherSessionID == firstID {
		t.Fatal("different OpenCode sessions shared a task")
	}
}

func TestOpenCodeSessionIdentityRejectsAmbiguity(t *testing.T) {
	identity := func() completion.RoutingIdentity {
		return completion.RoutingIdentity{HarnessID: adapter.OpenCodeID, HarnessVersion: adapter.OpenCodeVersion}
	}
	t.Run("duplicate session", func(t *testing.T) {
		header := make(http.Header)
		header.Add(headerOpenCodeSessionID, "ses_one")
		header.Add(headerOpenCodeSessionID, "ses_two")
		value := identity()
		if err := completeOpenCodeIdentity(&value, header, "opencode/1.17.18 runtime", canonical.Request{}); err == nil {
			t.Fatal("expected duplicate session id rejection")
		}
	})
	t.Run("affinity mismatch", func(t *testing.T) {
		header := make(http.Header)
		header.Set(headerOpenCodeSessionID, "ses_one")
		header.Set(headerOpenCodeSessionAffinity, "ses_two")
		value := identity()
		if err := completeOpenCodeIdentity(&value, header, "opencode/1.17.18 runtime", canonical.Request{}); err == nil {
			t.Fatal("expected affinity mismatch rejection")
		}
	})
	t.Run("explicit task bypass", func(t *testing.T) {
		header := make(http.Header)
		header.Set(headerOpenCodeSessionID, "ses_one")
		value := identity()
		value.TaskID = "caller-selected-task"
		if err := completeOpenCodeIdentity(&value, header, "opencode/1.17.18 runtime", canonical.Request{}); err == nil ||
			!strings.Contains(err.Error(), "must be omitted") {
			t.Fatalf("explicit task bypass error = %v", err)
		}
	})
	t.Run("missing session", func(t *testing.T) {
		value := identity()
		if err := completeOpenCodeIdentity(&value, make(http.Header), "opencode/1.17.18 runtime", canonical.Request{}); err == nil ||
			!strings.Contains(err.Error(), "requires X-Session-Id") {
			t.Fatalf("missing session error = %v", err)
		}
	})
	t.Run("wrong user agent", func(t *testing.T) {
		header := make(http.Header)
		header.Set(headerOpenCodeSessionID, "ses_one")
		value := identity()
		if err := completeOpenCodeIdentity(&value, header, "lookalike/1.17.18", canonical.Request{}); err == nil ||
			!strings.Contains(err.Error(), "User-Agent") {
			t.Fatalf("wrong user-agent error = %v", err)
		}
	})
}

func TestOpenCodeExactProfileRejectsIdentityBypassAtHTTPBoundary(t *testing.T) {
	fixture := newGatewayFixture(t, true)
	if err := fixture.registry.Register(adapter.OpenCode11718Profile()); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{
	  "model":"human-expert","stream":true,
	  "messages":[{"role":"user","content":"edit"}],
	  "tools":[{"type":"function","function":{"name":"edit","parameters":{"type":"object"}}}]
	}`)
	tests := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{name: "missing session", mutate: func(*http.Request) {}},
		{name: "caller task", mutate: func(request *http.Request) {
			request.Header.Set(headerOpenCodeSessionID, "ses_bypass")
			request.Header.Set(headerTaskID, "caller-selected-task")
		}},
		{name: "wrong user agent", mutate: func(request *http.Request) {
			request.Header.Set(headerOpenCodeSessionID, "ses_bypass")
			request.Header.Set("User-Agent", "lookalike/1.17.18")
		}},
		{name: "ambiguous session value", mutate: func(request *http.Request) {
			request.Header.Set(headerOpenCodeSessionID, "ses_one,ses_two")
		}},
		{name: "oversized session value", mutate: func(request *http.Request) {
			request.Header.Set(headerOpenCodeSessionID, strings.Repeat("s", 129))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := newChatRequest(t, fixture, body, "")
			request.Header.Set("User-Agent", "opencode/1.17.18 runtime")
			request.Header.Set(headerCapabilityTier, string(completion.TierWorkspace))
			request.Header.Set(headerWorkspaceKey, "identity-bypass")
			request.Header.Set(headerHarnessID, adapter.OpenCodeID)
			request.Header.Set(headerHarnessVersion, adapter.OpenCodeVersion)
			request.Header.Set(headerWorkspaceRoot, "/repo")
			test.mutate(request)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			payload, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil || response.StatusCode != http.StatusBadRequest ||
				!bytes.Contains(payload, []byte(`"code":"invalid_request"`)) {
				t.Fatalf("response = %d %q, %v", response.StatusCode, payload, readErr)
			}
		})
	}
}

func TestOpenCodeClarificationResumesOneTaskAndRetryAfterTransition(t *testing.T) {
	fixture := newGatewayFixture(t, true)
	if err := fixture.registry.Register(adapter.OpenCode11718Profile()); err != nil {
		t.Fatal(err)
	}
	tool := `{"type":"function","function":{"name":"edit","parameters":{"type":"object"}}}`
	firstBody := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"change it"}],"tools":[` + tool + `]}`)
	followBody := []byte(`{"model":"human-expert","stream":true,"messages":[` +
		`{"role":"user","content":"change it"},` +
		`{"role":"assistant","content":"Which file?"},` +
		`{"role":"user","content":"a.txt"}],"tools":[` + tool + `]}`)
	toolResultBody := []byte(`{
	  "model":"human-expert","stream":true,
	  "messages":[
	    {"role":"user","content":"change it"},
	    {"role":"assistant","content":"Which file?"},
	    {"role":"user","content":"a.txt"},
	    {"role":"assistant","content":null,"tool_calls":[{
	      "id":"tool_handoff_edit","type":"function",
	      "function":{"name":"edit","arguments":"{\"filePath\":\"/repo/a.txt\",\"oldString\":\"old\",\"newString\":\"new\"}"}
	    }]},
	    {"role":"tool","tool_call_id":"tool_handoff_edit","content":"Edit applied successfully."}
	  ],
	  "tools":[{"type":"function","function":{"name":"edit","parameters":{"type":"object"}}}]
	}`)
	nextBody := []byte(`{
	  "model":"human-expert","stream":true,
	  "messages":[
	    {"role":"user","content":"change it"},
	    {"role":"assistant","content":"Which file?"},
	    {"role":"user","content":"a.txt"},
	    {"role":"assistant","content":null,"tool_calls":[{
	      "id":"tool_handoff_edit","type":"function",
	      "function":{"name":"edit","arguments":"{\"filePath\":\"/repo/a.txt\",\"oldString\":\"old\",\"newString\":\"new\"}"}
	    }]},
	    {"role":"tool","tool_call_id":"tool_handoff_edit","content":"Edit applied successfully."},
	    {"role":"assistant","content":"done"},
	    {"role":"user","content":"now b.txt"}
	  ],
	  "tools":[{"type":"function","function":{"name":"edit","parameters":{"type":"object"}}}]
	}`)
	type result struct {
		status int
		body   []byte
		err    error
	}
	post := func(body []byte) <-chan result {
		done := make(chan result, 1)
		go func() {
			request := newChatRequest(t, fixture, body, "")
			request.Header.Set("User-Agent", "opencode/1.17.18 runtime")
			request.Header.Set(headerCapabilityTier, string(completion.TierWorkspace))
			request.Header.Set(headerWorkspaceKey, "handoff-workspace")
			request.Header.Set(headerHarnessID, adapter.OpenCodeID)
			request.Header.Set(headerHarnessVersion, adapter.OpenCodeVersion)
			request.Header.Set(headerWorkspaceRoot, "/repo")
			request.Header.Set(headerOpenCodeSessionID, "ses_handoff_retry")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				done <- result{err: err}
				return
			}
			payload, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			done <- result{status: response.StatusCode, body: payload, err: readErr}
		}()
		return done
	}
	receive := func(channel <-chan result) result {
		t.Helper()
		select {
		case value := <-channel:
			return value
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for OpenCode response")
			return result{}
		}
	}
	receiveAssignment := func() completion.Assignment {
		t.Helper()
		select {
		case value := <-fixture.worker.Assignments:
			return value
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for OpenCode assignment")
			return completion.Assignment{}
		}
	}

	firstResponse := post(firstBody)
	first := receiveAssignment()
	if first.HarnessSessionID != "ses_handoff_retry" {
		t.Fatalf("first harness session = %q", first.HarnessSessionID)
	}
	if err := fixture.hub.Publish(context.Background(), first.CallerID, first.IdempotencyKey,
		completion.Event{ID: "handoff-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.hub.Publish(context.Background(), first.CallerID, first.IdempotencyKey,
		completion.Event{ID: "handoff-question", Type: completion.EventClarification, Text: "Which file?"}); err != nil {
		t.Fatal(err)
	}
	if response := receive(firstResponse); response.err != nil || response.status != http.StatusOK {
		t.Fatalf("clarification response = %d %q, %v", response.status, response.body, response.err)
	}

	followResponse := post(followBody)
	follow := receiveAssignment()
	if follow.TaskID != first.TaskID || follow.IdempotencyKey == first.IdempotencyKey {
		t.Fatalf("handoff continuation identity = %+v, first = %+v", follow, first)
	}
	// Start an exact transport retry only after BeginRequest has already moved
	// the old task out of awaiting_caller. Its key must still bind the harness
	// session/body and replay this request rather than derive a second task.
	retryResponse := post(followBody)
	select {
	case duplicate := <-fixture.worker.Assignments:
		t.Fatalf("handoff retry dispatched a second assignment: %+v", duplicate)
	case <-time.After(30 * time.Millisecond):
	}
	if err := fixture.hub.Publish(context.Background(), follow.CallerID, follow.IdempotencyKey,
		completion.Event{ID: "handoff-follow-accepted", Type: completion.EventAccepted, WorkerID: fixture.worker.ID}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.hub.Publish(context.Background(), follow.CallerID, follow.IdempotencyKey,
		completion.Event{ID: "handoff-edit", Type: completion.EventToolCalls, ToolCalls: []completion.ToolCall{{
			ID: "tool_handoff_edit", Name: "edit", Input: map[string]any{
				"filePath": "/repo/a.txt", "oldString": "old", "newString": "new",
			},
		}}}); err != nil {
		t.Fatal(err)
	}
	primary := receive(followResponse)
	retry := receive(retryResponse)
	if primary.err != nil || retry.err != nil || primary.status != http.StatusOK ||
		retry.status != http.StatusOK || !bytes.Equal(primary.body, retry.body) {
		t.Fatalf("handoff retry mismatch: primary=%d %q %v retry=%d %q %v",
			primary.status, primary.body, primary.err, retry.status, retry.body, retry.err)
	}
	select {
	case duplicate := <-fixture.worker.Assignments:
		t.Fatalf("handoff retry left a duplicate assignment: %+v", duplicate)
	default:
	}
	toolResultResponse := post(toolResultBody)
	var toolContinuation completion.Assignment
	select {
	case toolContinuation = <-fixture.worker.Assignments:
	case response := <-toolResultResponse:
		t.Fatalf("tool-result continuation was rejected before assignment: %d %q, %v",
			response.status, response.body, response.err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for tool-result continuation")
	}
	if toolContinuation.TaskID != first.TaskID || toolContinuation.IdempotencyKey == follow.IdempotencyKey {
		t.Fatalf("handoff tool-result continuation = %+v, first = %+v", toolContinuation, first)
	}
	if err := publishAcceptedAndFinal(fixture, toolContinuation, "done"); err != nil {
		t.Fatal(err)
	}
	if response := receive(toolResultResponse); response.err != nil || response.status != http.StatusOK {
		t.Fatalf("tool-result continuation response = %d %q, %v", response.status, response.body, response.err)
	}

	nextResponse := post(nextBody)
	next := receiveAssignment()
	if next.TaskID == first.TaskID || next.HarnessSessionID != first.HarnessSessionID {
		t.Fatalf("new top-level turn identity = %+v, old = %+v", next, first)
	}
	if err := publishAcceptedAndFinal(fixture, next, "next done"); err != nil {
		t.Fatal(err)
	}
	if response := receive(nextResponse); response.err != nil || response.status != http.StatusOK {
		t.Fatalf("next-turn response = %d %q, %v", response.status, response.body, response.err)
	}
}

func TestToollessOpenCodeAuxiliaryRequestDoesNotJoinWorkspaceTask(t *testing.T) {
	profile := adapter.OpenCode11718Profile()
	request, err := openai.New().Decode([]byte(`{
	  "model":"human-expert","stream":true,
	  "messages":[{"role":"user","content":"Generate a title"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !isOpenCodeAuxiliaryRequest(profile, request) {
		t.Fatal("tool-less OpenCode auxiliary request was not isolated")
	}
	request.Tools = []canonical.Tool{{Name: "write", InputSchema: []byte(`{"type":"object"}`)}}
	if isOpenCodeAuxiliaryRequest(profile, request) {
		t.Fatal("native OpenCode tool request was isolated from its Workspace task")
	}
	request.Tools = []canonical.Tool{{Name: "lookalike_write", InputSchema: []byte(`{"type":"object"}`)}}
	if isOpenCodeAuxiliaryRequest(profile, request) {
		t.Fatal("caller-declared custom tool was isolated from its Workspace task")
	}
	other := adapter.Profile{HarnessID: "other", HarnessVersion: "1"}
	request.Tools = nil
	if isOpenCodeAuxiliaryRequest(other, request) {
		t.Fatal("generic tool-less Remote request was treated as an OpenCode auxiliary request")
	}
}

func TestOpenCodeDerivedKeyDeduplicatesRetryButSeparatesTurnsAndCallers(t *testing.T) {
	bodyA := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"one"}]}`)
	bodyAReordered := []byte(`{"messages":[{"content":"one","role":"user"}],"stream":true,"model":"human-expert"}`)
	bodyB := []byte(`{"model":"human-expert","stream":true,"messages":[{"role":"user","content":"one"},{"role":"assistant","content":"next"}]}`)
	decode := func(body []byte) completionRequest {
		t.Helper()
		request, err := openai.New().Decode(body)
		if err != nil {
			t.Fatal(err)
		}
		return completionRequest{request: request, body: body}
	}
	first := decode(bodyA)
	reordered := decode(bodyAReordered)
	second := decode(bodyB)
	identity := completion.RoutingIdentity{
		CallerID: "caller", WorkspaceKey: "workspace", TaskID: "ses_one",
		HarnessID: adapter.OpenCodeID, HarnessVersion: adapter.OpenCodeVersion,
		HarnessSessionID: "ses_one",
	}
	resolve := func(value completionRequest, id completion.RoutingIdentity) string {
		t.Helper()
		key, err := resolveOpenCodeIdempotencyKey("", id, completion.TierWorkspace,
			"opencode/1.17.18 ai-sdk/runtime", value.request, value.body)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(key, openCodeDerivedKeyPrefix) {
			t.Fatalf("derived key = %q", key)
		}
		return key
	}
	key := resolve(first, identity)
	resolvedHandoff := identity
	resolvedHandoff.TaskID = "previous-awaiting-caller-task"
	if got := resolve(first, resolvedHandoff); got != key {
		t.Fatalf("handoff task resolution changed retry key = %q, want %q", got, key)
	}
	if got := resolve(reordered, identity); got != key {
		t.Fatalf("semantic retry key = %q, want %q", got, key)
	}
	if got := resolve(second, identity); got == key {
		t.Fatal("distinct turn reused retry key")
	}
	other := identity
	other.CallerID = "other"
	if got := resolve(first, other); got == key {
		t.Fatal("other caller reused retry key")
	}
	otherSession := identity
	otherSession.HarnessSessionID = "ses_two"
	if got := resolve(first, otherSession); got == key {
		t.Fatal("other OpenCode session reused retry key")
	}
	explicit, err := resolveOpenCodeIdempotencyKey("caller-key", identity, completion.TierWorkspace,
		"opencode/1.17.18 ai-sdk/runtime", first.request, first.body)
	if err != nil || explicit != "caller-key" {
		t.Fatalf("explicit key = %q, %v", explicit, err)
	}
}

type completionRequest struct {
	request canonical.Request
	body    []byte
}
