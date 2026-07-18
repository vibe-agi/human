package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	sdka2a "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/vibe-agi/human/agent"
)

const testExtensionURI = "https://vibe-agi.example/extensions/workspace/v1"

func TestPublicAgentCardBypassesProtocolGuard(t *testing.T) {
	authCalls := 0
	config := testHandlerConfig(t, func(context.Context, *http.Request) (Principal, error) {
		authCalls++
		return Principal{}, sdka2a.ErrUnauthenticated
	})
	handler := newHTTPHandler(config, &fakeRequestHandler{})

	request := httptest.NewRequest(http.MethodGet, a2asrv.WellKnownAgentCardPath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("Agent Card status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if authCalls != 0 {
		t.Fatalf("Authenticate calls = %d, want 0 for public Agent Card", authCalls)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Agent Card Content-Type = %q, want application/json", got)
	}
	var card sdka2a.AgentCard
	if err := json.Unmarshal(response.Body.Bytes(), &card); err != nil {
		t.Fatalf("decode Agent Card: %v", err)
	}
	if card.Name != config.Card.Name {
		t.Fatalf("Agent Card name = %q, want %q", card.Name, config.Card.Name)
	}
}

func TestProtocolGuardAuthenticatesNormalizesAndActivatesExtension(t *testing.T) {
	var observed Principal
	var sdkUser *a2asrv.User
	fake := &fakeRequestHandler{
		sendMessage: func(ctx context.Context, _ *sdka2a.SendMessageRequest) (sdka2a.SendMessageResult, error) {
			var ok bool
			observed, ok = PrincipalFromContext(ctx)
			if !ok {
				t.Fatal("authenticated Principal missing from request context")
			}
			call, ok := a2asrv.CallContextFrom(ctx)
			if !ok {
				t.Fatal("SDK CallContext missing")
			}
			sdkUser = call.User
			values, ok := call.ServiceParams().Get(sdka2a.SvcParamExtensions)
			if !ok || len(values) != 2 || values[0] != "https://unknown.example/v1" || values[1] != testExtensionURI {
				t.Fatalf("normalized extension values = %#v", values)
			}
			extensions, ok := a2asrv.ExtensionsFrom(ctx)
			declared := sdka2a.AgentExtension{URI: testExtensionURI}
			if !ok || !extensions.Active(&declared) {
				t.Fatal("supported requested extension was not activated in SDK CallContext")
			}
			return testTask(), nil
		},
	}
	config := testHandlerConfig(t, testAuthenticator)
	handler := newHTTPHandler(config, fake)
	request := sendRequest(t, "/message:send", 0)
	request.Header.Set(sdka2a.SvcParamExtensions, " https://unknown.example/v1, "+testExtensionURI)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("SendMessage status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if observed.Authority != "authority-1" || observed.Subject != "subject-1" {
		t.Fatalf("Principal = %#v", observed)
	}
	if sdkUser == nil || !sdkUser.Authenticated || sdkUser.Name != "subject-1" {
		t.Fatalf("SDK user = %#v", sdkUser)
	}
	if got := response.Header().Get("Content-Type"); got != "application/a2a+json" {
		t.Fatalf("Content-Type = %q, want application/a2a+json", got)
	}
	if got := response.Header().Get(sdka2a.SvcParamVersion); got != string(sdka2a.Version) {
		t.Fatalf("A2A-Version response = %q", got)
	}
	if got := response.Header().Get(sdka2a.SvcParamExtensions); got != testExtensionURI {
		t.Fatalf("activated extensions = %q, want %q", got, testExtensionURI)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &wire); err != nil || wire["task"] == nil {
		t.Fatalf("SendMessage body is not a task response: %s (err=%v)", response.Body.String(), err)
	}
}

func TestProtocolGuardRejectsBeforeDispatch(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*http.Request)
		wantStatus int
		wantReason string
	}{
		{
			name: "unauthenticated",
			mutate: func(request *http.Request) {
				request.Header.Del("Authorization")
			},
			wantStatus: http.StatusUnauthorized,
			wantReason: "UNAUTHENTICATED",
		},
		{
			name: "missing version",
			mutate: func(request *http.Request) {
				request.Header.Del(sdka2a.SvcParamVersion)
			},
			wantStatus: http.StatusBadRequest,
			wantReason: "VERSION_NOT_SUPPORTED",
		},
		{
			name: "wrong version",
			mutate: func(request *http.Request) {
				request.Header.Set(sdka2a.SvcParamVersion, "0.3")
			},
			wantStatus: http.StatusBadRequest,
			wantReason: "VERSION_NOT_SUPPORTED",
		},
		{
			name: "missing required extension",
			mutate: func(request *http.Request) {
				request.Header.Del(sdka2a.SvcParamExtensions)
			},
			wantStatus: http.StatusBadRequest,
			wantReason: "EXTENSION_SUPPORT_REQUIRED",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			fake := &fakeRequestHandler{sendMessage: func(context.Context, *sdka2a.SendMessageRequest) (sdka2a.SendMessageResult, error) {
				calls++
				return testTask(), nil
			}}
			handler := newHTTPHandler(testHandlerConfig(t, testAuthenticator), fake)
			request := sendRequest(t, "/message:send", 0)
			test.mutate(request)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			if calls != 0 {
				t.Fatalf("request handler calls = %d, want 0", calls)
			}
			if got := protocolErrorReason(t, response.Body.Bytes()); got != test.wantReason {
				t.Fatalf("error reason = %q, want %q", got, test.wantReason)
			}
			if got := response.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("Content-Type = %q", got)
			}
		})
	}
}

func TestProtocolGuardEnforcesBodyLimit(t *testing.T) {
	calls := 0
	config := testHandlerConfig(t, testAuthenticator)
	config.MaxRequestBytes = 32
	handler := newHTTPHandler(config, &fakeRequestHandler{sendMessage: func(context.Context, *sdka2a.SendMessageRequest) (sdka2a.SendMessageResult, error) {
		calls++
		return testTask(), nil
	}})
	request := sendRequest(t, "/message:send", 128)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%s", response.Code, response.Body.String())
	}
	if calls != 0 {
		t.Fatalf("request handler calls = %d, want 0", calls)
	}
}

func TestProtocolRejectsAmbiguousJSONBeforeDispatch(t *testing.T) {
	valid, err := json.Marshal(sdka2a.SendMessageRequest{Message: sdka2a.NewMessage(
		sdka2a.MessageRoleUser, sdka2a.NewTextPart("hello"),
	)})
	if err != nil {
		t.Fatal(err)
	}
	invalidUTF8 := append([]byte(nil), valid...)
	marker := bytes.Index(invalidUTF8, []byte("hello"))
	if marker < 0 {
		t.Fatalf("valid fixture does not contain text: %s", valid)
	}
	invalidUTF8[marker] = 0xff
	tests := []struct {
		name string
		body []byte
	}{
		{name: "second top-level value", body: append(append([]byte(nil), valid...), []byte(`{}`)...)},
		{name: "duplicate nested field", body: []byte(`{"message":{"messageId":"first","messageId":"second","role":"ROLE_USER","parts":[{"text":"hello"}]}}`)},
		{name: "invalid UTF-8", body: invalidUTF8},
		{name: "non-object request", body: []byte(`null`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			fake := &fakeRequestHandler{sendMessage: func(context.Context, *sdka2a.SendMessageRequest) (sdka2a.SendMessageResult, error) {
				calls++
				return testTask(), nil
			}}
			handler := newHTTPHandler(testHandlerConfig(t, testAuthenticator), fake)
			request := authenticatedRequest(t, http.MethodPost, "/message:send", bytes.NewReader(test.body))
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
			}
			if calls != 0 {
				t.Fatalf("SendMessage calls = %d, want 0", calls)
			}
		})
	}
}

func TestProtocolRejectsConflictingKnownQueryValues(t *testing.T) {
	calls := 0
	fake := &fakeRequestHandler{getTask: func(context.Context, *sdka2a.GetTaskRequest) (*sdka2a.Task, error) {
		calls++
		return testTask(), nil
	}}
	handler := newHTTPHandler(testHandlerConfig(t, testAuthenticator), fake)
	request := authenticatedRequest(t, http.MethodGet, "/tasks/task-1?historyLength=1&historyLength=not-an-int", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
	}
	if calls != 0 {
		t.Fatalf("GetTask calls = %d, want 0", calls)
	}
}

func TestSubscribeAcceptsBothPublishedVersionOneBindingsAndPreservesSSE(t *testing.T) {
	subscribeCalls := 0
	fake := &fakeRequestHandler{subscribe: func(_ context.Context, request *sdka2a.SubscribeToTaskRequest) iter.Seq2[sdka2a.Event, error] {
		subscribeCalls++
		if request.ID != "task-1" {
			t.Fatalf("Subscribe task ID = %q", request.ID)
		}
		return func(yield func(sdka2a.Event, error) bool) {
			yield(testTask(), nil)
		}
	}}
	handler := newHTTPHandler(testHandlerConfig(t, testAuthenticator), fake)
	request := authenticatedRequest(t, http.MethodGet, "/tasks/task-1:subscribe", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("GET subscribe status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if subscribeCalls != 1 {
		t.Fatalf("Subscribe calls = %d, want 1", subscribeCalls)
	}
	if got := response.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("SSE Content-Type = %q", got)
	}
	if !strings.Contains(response.Body.String(), `"task"`) {
		t.Fatalf("SSE body does not contain initial Task: %s", response.Body.String())
	}

	post := authenticatedRequest(t, http.MethodPost, "/tasks/task-1:subscribe", nil)
	postResponse := httptest.NewRecorder()
	handler.ServeHTTP(postResponse, post)
	if postResponse.Code != http.StatusOK {
		t.Fatalf("POST subscribe status = %d, want 200; body=%s", postResponse.Code, postResponse.Body.String())
	}
	if subscribeCalls != 2 {
		t.Fatalf("Subscribe calls = %d, want 2", subscribeCalls)
	}
}

func TestApplyReceiptInterceptorDoesNotCaptureStandardTaskActions(t *testing.T) {
	cancelCalls := 0
	fake := &fakeRequestHandler{cancelTask: func(_ context.Context, request *sdka2a.CancelTaskRequest) (*sdka2a.Task, error) {
		cancelCalls++
		if request.ID != "task-1" {
			t.Fatalf("Cancel task ID = %q", request.ID)
		}
		task := testTask()
		task.Status.State = sdka2a.TaskStateCanceled
		return task, nil
	}}
	config := testHandlerConfig(t, testAuthenticator)
	config.Config.AuthorizeApplyReceipt = func(context.Context, Principal, agent.Task, *RecordApplyReceiptRequest) error {
		return nil
	}
	handler := newHTTPHandler(config, fake)
	request := authenticatedRequest(t, http.MethodPost, "/tasks/task-1:cancel", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if cancelCalls != 1 {
		t.Fatalf("Cancel calls = %d, want 1", cancelCalls)
	}
}

func TestVersionMayBeSuppliedAsQueryParameter(t *testing.T) {
	fake := &fakeRequestHandler{getTask: func(context.Context, *sdka2a.GetTaskRequest) (*sdka2a.Task, error) {
		return testTask(), nil
	}}
	handler := newHTTPHandler(testHandlerConfig(t, testAuthenticator), fake)
	request := authenticatedRequest(t, http.MethodGet, "/tasks/task-1?A2A-Version=1.0", nil)
	request.Header.Del(sdka2a.SvcParamVersion)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("query-version status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
}

func TestProtocolNormalizesOfficialSDKListTimestampParameter(t *testing.T) {
	want := time.Date(2026, 7, 18, 12, 34, 56, 0, time.UTC)
	listCalls := 0
	fake := &fakeRequestHandler{listTasks: func(ctx context.Context, request *sdka2a.ListTasksRequest) (*sdka2a.ListTasksResponse, error) {
		listCalls++
		// The official v2.3.1 server also passes **time.Time to a parser that
		// handles only *time.Time, so the DTO remains nil. The protocol guard
		// carries the normalized value in the authenticated request context for
		// Human's real requestHandler to consume.
		if request.StatusTimestampAfter != nil {
			t.Fatalf("upstream SDK unexpectedly populated status timestamp = %v", request.StatusTimestampAfter)
		}
		got, ok := listTimestampFromContext(ctx)
		if !ok || !got.Equal(want) {
			t.Fatalf("normalized context timestamp = %v, %v; want %v, true", got, ok, want)
		}
		return &sdka2a.ListTasksResponse{Tasks: []*sdka2a.Task{}}, nil
	}}
	handler := newHTTPHandler(testHandlerConfig(t, testAuthenticator), fake)
	request := authenticatedRequest(t, http.MethodGet, "/tasks?lastUpdatedAfter="+want.Format(time.RFC3339), nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if listCalls != 1 {
		t.Fatalf("ListTasks calls = %d, want 1", listCalls)
	}
}

func TestOfficialRESTClientPreservesTypedTaskNotFoundError(t *testing.T) {
	handler := newHTTPHandler(testHandlerConfig(t, testAuthenticator), &fakeRequestHandler{})
	server := httptest.NewServer(handler)
	defer server.Close()
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	transport := a2aclient.NewRESTTransport(baseURL, server.Client())
	defer transport.Destroy()
	params := a2aclient.ServiceParams{
		"Authorization":           {"Bearer valid"},
		sdka2a.SvcParamVersion:    {string(sdka2a.Version)},
		sdka2a.SvcParamExtensions: {testExtensionURI},
	}

	_, err = transport.GetTask(context.Background(), params, &sdka2a.GetTaskRequest{ID: "missing-task"})
	if !errors.Is(err, sdka2a.ErrTaskNotFound) {
		t.Fatalf("official REST client error = %v, want task not found", err)
	}
}

func testHandlerConfig(t *testing.T, authenticate AuthenticateFunc) handlerConfig {
	t.Helper()
	config := Config{
		Agent: &agent.Agent{},
		Card: &sdka2a.AgentCard{
			Name:        "Human Agent",
			Description: "test",
			Version:     "test",
			Capabilities: sdka2a.AgentCapabilities{
				Streaming:  true,
				Extensions: []sdka2a.AgentExtension{{URI: testExtensionURI, Required: true}},
			},
			SupportedInterfaces: []*sdka2a.AgentInterface{
				sdka2a.NewAgentInterface("http://example.test", sdka2a.TransportProtocolHTTPJSON),
			},
			DefaultInputModes:  []string{"text/plain"},
			DefaultOutputModes: []string{"text/plain", "application/json"},
			Skills: []sdka2a.AgentSkill{{
				ID: "human-collaboration", Name: "Human collaboration",
				Description: "Delegate a durable task to a human collaborator.",
				Tags:        []string{"human"},
			}},
		},
		Authenticate: authenticate,
		ResolveWorkspace: func(context.Context, Principal, *sdka2a.SendMessageRequest) (agent.WorkspaceID, error) {
			return "workspace-1", nil
		},
	}
	checked, err := checkConfig(config)
	if err != nil {
		t.Fatalf("checkConfig: %v", err)
	}
	return checked
}

func testAuthenticator(_ context.Context, request *http.Request) (Principal, error) {
	if request.Header.Get("Authorization") != "Bearer valid" {
		return Principal{}, sdka2a.ErrUnauthenticated
	}
	return Principal{
		Authority: "authority-1",
		Subject:   "subject-1",
		Attributes: map[string]any{
			"role": "caller",
		},
	}, nil
}

func sendRequest(t *testing.T, path string, padding int) *http.Request {
	t.Helper()
	payload, err := json.Marshal(sdka2a.SendMessageRequest{Message: sdka2a.NewMessage(
		sdka2a.MessageRoleUser,
		sdka2a.NewTextPart("hello"+strings.Repeat("x", padding)),
	)})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return authenticatedRequest(t, http.MethodPost, path, bytes.NewReader(payload))
}

func authenticatedRequest(t *testing.T, method, path string, body io.Reader) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, path, body)
	request.Header.Set("Authorization", "Bearer valid")
	request.Header.Set(sdka2a.SvcParamVersion, string(sdka2a.Version))
	request.Header.Set(sdka2a.SvcParamExtensions, testExtensionURI)
	if body != nil {
		request.Header.Set("Content-Type", "application/a2a+json")
	}
	return request
}

func testTask() *sdka2a.Task {
	return &sdka2a.Task{
		ID:        "task-1",
		ContextID: "context-1",
		Status:    sdka2a.TaskStatus{State: sdka2a.TaskStateSubmitted},
	}
}

func protocolErrorReason(t *testing.T, encoded []byte) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Details []map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("decode protocol error: %v; body=%s", err, encoded)
	}
	for _, detail := range envelope.Error.Details {
		if reason, ok := detail["reason"].(string); ok {
			return reason
		}
	}
	return ""
}

type fakeRequestHandler struct {
	getTask     func(context.Context, *sdka2a.GetTaskRequest) (*sdka2a.Task, error)
	listTasks   func(context.Context, *sdka2a.ListTasksRequest) (*sdka2a.ListTasksResponse, error)
	cancelTask  func(context.Context, *sdka2a.CancelTaskRequest) (*sdka2a.Task, error)
	sendMessage func(context.Context, *sdka2a.SendMessageRequest) (sdka2a.SendMessageResult, error)
	subscribe   func(context.Context, *sdka2a.SubscribeToTaskRequest) iter.Seq2[sdka2a.Event, error]
}

var _ a2asrv.RequestHandler = (*fakeRequestHandler)(nil)

func (handler *fakeRequestHandler) GetTask(ctx context.Context, request *sdka2a.GetTaskRequest) (*sdka2a.Task, error) {
	if handler.getTask != nil {
		return handler.getTask(ctx, request)
	}
	return nil, sdka2a.ErrTaskNotFound
}

func (handler *fakeRequestHandler) ListTasks(ctx context.Context, request *sdka2a.ListTasksRequest) (*sdka2a.ListTasksResponse, error) {
	if handler.listTasks != nil {
		return handler.listTasks(ctx, request)
	}
	return nil, sdka2a.ErrUnsupportedOperation
}

func (handler *fakeRequestHandler) CancelTask(ctx context.Context, request *sdka2a.CancelTaskRequest) (*sdka2a.Task, error) {
	if handler.cancelTask != nil {
		return handler.cancelTask(ctx, request)
	}
	return nil, sdka2a.ErrUnsupportedOperation
}

func (handler *fakeRequestHandler) SendMessage(ctx context.Context, request *sdka2a.SendMessageRequest) (sdka2a.SendMessageResult, error) {
	if handler.sendMessage != nil {
		return handler.sendMessage(ctx, request)
	}
	return nil, sdka2a.ErrUnsupportedOperation
}

func (handler *fakeRequestHandler) SubscribeToTask(ctx context.Context, request *sdka2a.SubscribeToTaskRequest) iter.Seq2[sdka2a.Event, error] {
	if handler.subscribe != nil {
		return handler.subscribe(ctx, request)
	}
	return errorSequence(sdka2a.ErrUnsupportedOperation)
}

func (*fakeRequestHandler) SendStreamingMessage(context.Context, *sdka2a.SendMessageRequest) iter.Seq2[sdka2a.Event, error] {
	return errorSequence(sdka2a.ErrUnsupportedOperation)
}

func (*fakeRequestHandler) GetTaskPushConfig(context.Context, *sdka2a.GetTaskPushConfigRequest) (*sdka2a.PushConfig, error) {
	return nil, sdka2a.ErrPushNotificationNotSupported
}

func (*fakeRequestHandler) ListTaskPushConfigs(context.Context, *sdka2a.ListTaskPushConfigRequest) (*sdka2a.ListTaskPushConfigResponse, error) {
	return nil, sdka2a.ErrPushNotificationNotSupported
}

func (*fakeRequestHandler) CreateTaskPushConfig(context.Context, *sdka2a.PushConfig) (*sdka2a.PushConfig, error) {
	return nil, sdka2a.ErrPushNotificationNotSupported
}

func (*fakeRequestHandler) DeleteTaskPushConfig(context.Context, *sdka2a.DeleteTaskPushConfigRequest) error {
	return sdka2a.ErrPushNotificationNotSupported
}

func (*fakeRequestHandler) GetExtendedAgentCard(context.Context, *sdka2a.GetExtendedAgentCardRequest) (*sdka2a.AgentCard, error) {
	return nil, sdka2a.ErrUnsupportedOperation
}

func errorSequence(err error) iter.Seq2[sdka2a.Event, error] {
	return func(yield func(sdka2a.Event, error) bool) {
		yield(nil, err)
	}
}

func TestAuthenticationErrorsDoNotLeakImplementationMessages(t *testing.T) {
	handler := newHTTPHandler(testHandlerConfig(t, func(context.Context, *http.Request) (Principal, error) {
		return Principal{}, errors.New("secret database lookup failed")
	}), &fakeRequestHandler{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, sendRequest(t, "/message:send", 0))
	if strings.Contains(response.Body.String(), "secret") {
		t.Fatalf("authentication error leaked: %s", response.Body.String())
	}
}
