package callerhttp_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/callerhttp"
)

const testCodec llm.CodecID = "test.http"

func TestTransportProjectsExactStreamAndIdentity(t *testing.T) {
	identity := completionIdentity("stream-key", "task-stream", "workspace-a")
	endpoint := &fakeEndpoint{}
	endpoint.admit = func(_ context.Context, request llm.AdmissionRequest) (llm.AdmissionResult, error) {
		if request.CallerID != "caller-a" || request.IdempotencyKey != "stream-key" ||
			request.CodecID != testCodec || string(request.Body) != `{"stream":true}` ||
			request.Task.CapabilityTier != llm.TierWorkspace || request.Task.WorkspaceKey != "workspace-a" {
			t.Fatalf("unexpected admission request: %+v body=%q", request, request.Body)
		}
		return admission(identity, llm.ResponsePage{
			Mode: llm.ResponseStream, DecisionCommitted: true,
			Decision: llm.ResponseDecision{StatusCode: 202, ContentType: "text/event-stream", RetryAfter: "3"},
			Events:   []llm.WireEvent{{Sequence: 2, Data: []byte("first\n")}}, Cursor: 2,
		}), nil
	}
	endpoint.wait = func(_ context.Context, query llm.ResponseQuery) (llm.ResponsePage, error) {
		if query.After != 2 || query.CallerID != identity.CallerID ||
			query.IdempotencyKey != identity.IdempotencyKey || query.RequestDigest != testDigest {
			t.Fatalf("unexpected response query: %+v", query)
		}
		return responsePage(identity, llm.ResponseStream, true,
			llm.ResponseDecision{StatusCode: 202, ContentType: "text/event-stream", RetryAfter: "3"},
			[]llm.WireEvent{{Sequence: 4, Data: []byte("second\n")}}, 4), nil
	}

	transport, runtime, server := startTransport(t, endpoint, callerhttp.Config{
		Authenticator: fixedAuth("caller-a"),
		Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/v1/responses", CodecID: testCodec}},
	})
	_ = transport
	defer shutdownTransport(t, runtime, server)

	request := mustRequest(t, http.MethodPost, server.URL+"/v1/responses", `{"stream":true}`)
	setWorkspaceHeaders(request, "stream-key")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 202 || string(body) != "first\nsecond\n" {
		t.Fatalf("stream response: status=%d body=%q", response.StatusCode, body)
	}
	if response.Header.Get("Content-Type") != "text/event-stream" || response.Header.Get("Retry-After") != "3" ||
		response.Header.Get(callerhttp.HeaderIdempotencyKey) != "stream-key" ||
		response.Header.Get(callerhttp.HeaderTaskID) != "task-stream" ||
		response.Header.Get(callerhttp.HeaderRequestID) != "request-stream-key" ||
		response.Header.Get(callerhttp.HeaderWorkspaceKey) != "workspace-a" {
		t.Fatalf("unexpected response headers: %v", response.Header)
	}
}

func TestTransportWaitsForAggregateDecisionAndWritesBodyOnce(t *testing.T) {
	identity := completionIdentity("aggregate-key", "task-aggregate", "")
	endpoint := &fakeEndpoint{}
	endpoint.admit = func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
		return admission(identity, llm.ResponsePage{
			Mode: llm.ResponseAggregate, Cursor: 1,
		}), nil
	}
	endpoint.wait = func(_ context.Context, query llm.ResponseQuery) (llm.ResponsePage, error) {
		if query.After != 1 {
			t.Fatalf("aggregate cursor = %d", query.After)
		}
		return responsePage(identity, llm.ResponseAggregate, true,
			llm.ResponseDecision{StatusCode: 201, ContentType: "application/json", Body: []byte(`{"ok":true}`)},
			nil, 2), nil
	}
	_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
		Authenticator: fixedAuth("caller-a"),
		Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	})
	defer shutdownTransport(t, runtime, server)

	request := mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`)
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "aggregate-key")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 201 || response.Header.Get("Content-Type") != "application/json" ||
		string(body) != `{"ok":true}` {
		t.Fatalf("aggregate response: status=%d headers=%v body=%q", response.StatusCode, response.Header, body)
	}
}

func TestTransportFollowsPrivateCursorsAcrossSingleRecordStreamPages(t *testing.T) {
	identity := completionIdentity("paged-key", "task-paged", "")
	endpoint := &fakeEndpoint{}
	endpoint.admit = func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
		return admission(identity, responsePage(identity, llm.ResponseStream, false,
			llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"}, nil, 1)), nil
	}
	endpoint.wait = func(_ context.Context, query llm.ResponseQuery) (llm.ResponsePage, error) {
		if query.Limit != 1 {
			t.Fatalf("page limit = %d, want 1", query.Limit)
		}
		decision := llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"}
		switch query.After {
		case 1:
			return responsePage(identity, llm.ResponseStream, false, decision,
				[]llm.WireEvent{{Sequence: 2, Data: []byte("a")}}, 2), nil
		case 2:
			// Sequence 3 is a private encoder checkpoint. The transport must
			// advance the opaque cursor even though there is no wire event.
			return responsePage(identity, llm.ResponseStream, false, decision, nil, 3), nil
		case 3:
			return responsePage(identity, llm.ResponseStream, true, decision,
				[]llm.WireEvent{{Sequence: 4, Data: []byte("b")}}, 4), nil
		default:
			return llm.ResponsePage{}, errors.New("unexpected cursor")
		}
	}
	_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
		Authenticator: fixedAuth("caller-a"), PageLimit: 1,
		Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	})
	defer shutdownTransport(t, runtime, server)
	request := mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`)
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "paged-key")
	response := doRequest(t, request)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ab" {
		t.Fatalf("paged response body = %q", body)
	}
}

func TestTransportProjectsOnlySafeAdmissionError(t *testing.T) {
	secret := errors.New("database password is secret")
	endpoint := &fakeEndpoint{admit: func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
		return llm.AdmissionResult{}, &llm.AdmissionError{
			Failure:     llm.AdmissionFailure{Status: 503, Code: "busy", Message: "Human worker is busy"},
			ContentType: "application/json", RetryAfter: "7", Body: []byte(`{"error":"busy"}`), Cause: secret,
		}
	}}
	_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
		Authenticator: fixedAuth("caller-a"),
		Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	})
	defer shutdownTransport(t, runtime, server)

	request := mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`)
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "error-key")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 503 || response.Header.Get("Retry-After") != "7" ||
		response.Header.Get("Content-Type") != "application/json" || string(body) != `{"error":"busy"}` {
		t.Fatalf("safe admission response: status=%d headers=%v body=%q", response.StatusCode, response.Header, body)
	}
	if bytes.Contains(body, []byte("password")) {
		t.Fatalf("AdmissionError cause leaked: %q", body)
	}
}

func TestTransportAuthResolverRouteAndBodyFailuresAreFinite(t *testing.T) {
	t.Run("authentication", func(t *testing.T) {
		endpoint := &fakeEndpoint{}
		_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
			Authenticator: callerhttp.AuthenticateFunc(func(context.Context, *http.Request) (callerhttp.Identity, error) {
				return callerhttp.Identity{}, errors.New("secret auth detail")
			}),
			Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
		})
		defer shutdownTransport(t, runtime, server)
		response := doRequest(t, mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`))
		if response.StatusCode != http.StatusServiceUnavailable || endpoint.admitCalls.Load() != 0 {
			t.Fatalf("auth response=%d calls=%d", response.StatusCode, endpoint.admitCalls.Load())
		}
		_ = response.Body.Close()
	})

	t.Run("resolver", func(t *testing.T) {
		endpoint := &fakeEndpoint{}
		var observed atomic.Bool
		_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
			Authenticator: fixedAuth("caller-a"),
			Resolver: callerhttp.ResolveFunc(func(_ context.Context, input callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
				if input.CallerID == "caller-a" && input.Route.CodecID == testCodec && string(input.Body) == "payload" {
					observed.Store(true)
				}
				return callerhttp.Resolution{}, errors.New("private resolver detail")
			}),
			Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
		})
		defer shutdownTransport(t, runtime, server)
		response := doRequest(t, mustRequest(t, http.MethodPost, server.URL+"/chat", "payload"))
		if response.StatusCode != http.StatusServiceUnavailable || !observed.Load() || endpoint.admitCalls.Load() != 0 {
			t.Fatalf("resolver response=%d observed=%v calls=%d", response.StatusCode, observed.Load(), endpoint.admitCalls.Load())
		}
		_ = response.Body.Close()
	})

	t.Run("body limit", func(t *testing.T) {
		endpoint := &fakeEndpoint{}
		var resolverCalls atomic.Int64
		_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
			Authenticator: fixedAuth("caller-a"), MaxBodyBytes: 4,
			Resolver: callerhttp.ResolveFunc(func(context.Context, callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
				resolverCalls.Add(1)
				return callerhttp.Resolution{}, nil
			}),
			Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
		})
		defer shutdownTransport(t, runtime, server)
		response := doRequest(t, mustRequest(t, http.MethodPost, server.URL+"/chat", "12345"))
		if response.StatusCode != http.StatusRequestEntityTooLarge || resolverCalls.Load() != 0 || endpoint.admitCalls.Load() != 0 {
			t.Fatalf("body response=%d resolver=%d admit=%d", response.StatusCode, resolverCalls.Load(), endpoint.admitCalls.Load())
		}
		_ = response.Body.Close()
	})

	t.Run("explicit route", func(t *testing.T) {
		endpoint := &fakeEndpoint{}
		_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
			Authenticator: fixedAuth("caller-a"),
			Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
		})
		defer shutdownTransport(t, runtime, server)
		response := doRequest(t, mustRequest(t, http.MethodGet, server.URL+"/chat", ""))
		if response.StatusCode != http.StatusMethodNotAllowed || response.Header.Get("Allow") != http.MethodPost {
			t.Fatalf("method response=%d headers=%v", response.StatusCode, response.Header)
		}
		_ = response.Body.Close()
		response = doRequest(t, mustRequest(t, http.MethodPost, server.URL+"/other", `{}`))
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("path response=%d", response.StatusCode)
		}
		_ = response.Body.Close()
	})
}

func TestTransportClassifiesAuthenticatorFaultsWithoutLeakingCause(t *testing.T) {
	secret := errors.New("secret identity-provider detail")
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{
			name: "unauthenticated",
			err: framework.NewFault(
				framework.CodeUnauthenticated, framework.RetryNever, "do not expose", secret,
			),
			status: http.StatusUnauthorized,
		},
		{
			name: "forbidden",
			err: framework.NewFault(
				framework.CodeForbidden, framework.RetryNever, "do not expose", secret,
			),
			status: http.StatusForbidden,
		},
		{
			name: "provider unavailable",
			err: framework.NewFault(
				framework.CodeUnavailable, framework.RetryBackoff, "do not expose", secret,
			),
			status: http.StatusServiceUnavailable,
		},
		{
			name: "typed provider fault overrides wrapped invalid sentinel",
			err: framework.NewFault(
				framework.CodeUnavailable, framework.RetryBackoff, "do not expose",
				errors.Join(callerhttp.ErrResolution, secret),
			),
			status: http.StatusServiceUnavailable,
		},
		{name: "unclassified infrastructure error", err: secret, status: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := &fakeEndpoint{}
			_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
				Authenticator: callerhttp.AuthenticateFunc(func(context.Context, *http.Request) (callerhttp.Identity, error) {
					return callerhttp.Identity{}, test.err
				}),
				Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
			})
			defer shutdownTransport(t, runtime, server)
			response := doRequest(t, mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`))
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != test.status || endpoint.admitCalls.Load() != 0 {
				t.Fatalf("auth response=%d calls=%d, want %d/0", response.StatusCode, endpoint.admitCalls.Load(), test.status)
			}
			if bytes.Contains(body, []byte(secret.Error())) || bytes.Contains(body, []byte("do not expose")) {
				t.Fatalf("authentication response leaked private diagnostics: %q", body)
			}
		})
	}
}

func TestTransportClassifiesResolverFaultsWithoutLeakingCause(t *testing.T) {
	secret := errors.New("secret workspace-router detail")
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{name: "built-in invalid sentinel", err: callerhttp.ErrResolution, status: http.StatusBadRequest},
		{
			name: "typed invalid",
			err: framework.NewFault(
				framework.CodeInvalid, framework.RetryNever, "do not expose", secret,
			),
			status: http.StatusBadRequest,
		},
		{
			name: "forbidden",
			err: framework.NewFault(
				framework.CodeForbidden, framework.RetryNever, "do not expose", secret,
			),
			status: http.StatusForbidden,
		},
		{
			name: "provider unavailable",
			err: framework.NewFault(
				framework.CodeUnavailable, framework.RetryBackoff, "do not expose", secret,
			),
			status: http.StatusServiceUnavailable,
		},
		{name: "unclassified infrastructure error", err: secret, status: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := &fakeEndpoint{}
			_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
				Authenticator: fixedAuth("caller-a"),
				Resolver: callerhttp.ResolveFunc(func(context.Context, callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
					return callerhttp.Resolution{}, test.err
				}),
				Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
			})
			defer shutdownTransport(t, runtime, server)
			response := doRequest(t, mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`))
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != test.status || endpoint.admitCalls.Load() != 0 {
				t.Fatalf("resolver response=%d calls=%d, want %d/0", response.StatusCode, endpoint.admitCalls.Load(), test.status)
			}
			if bytes.Contains(body, []byte(secret.Error())) || bytes.Contains(body, []byte("do not expose")) {
				t.Fatalf("resolver response leaked private diagnostics: %q", body)
			}
		})
	}
}

func TestCustomPortsCannotMutateCoreRequestSnapshot(t *testing.T) {
	identity := completionIdentity("immutable-key", "task-immutable", "")
	endpoint := &fakeEndpoint{admit: func(_ context.Context, request llm.AdmissionRequest) (llm.AdmissionResult, error) {
		if string(request.Body) != "original" {
			t.Fatalf("core body was mutated by an extension: %q", request.Body)
		}
		return admission(identity, responsePage(identity, llm.ResponseAggregate, true,
			llm.ResponseDecision{StatusCode: 200, ContentType: "application/json", Body: []byte(`{}`)}, nil, 2)), nil
	}}
	transport, runtime, server := startTransport(t, endpoint, callerhttp.Config{
		Authenticator: callerhttp.AuthenticateFunc(func(_ context.Context, request *http.Request) (callerhttp.Identity, error) {
			consumed, err := io.ReadAll(request.Body)
			if err != nil || string(consumed) != "original" {
				t.Fatalf("auth body snapshot=%q err=%v", consumed, err)
			}
			request.Header.Set("X-Test-Mutation", "auth")
			return callerhttp.Identity{CallerID: "caller-a"}, nil
		}),
		Resolver: callerhttp.ResolveFunc(func(_ context.Context, input callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
			if input.Header.Get("X-Test-Mutation") != "original" || string(input.Body) != "original" {
				t.Fatalf("resolver did not receive the original snapshot: headers=%v body=%q", input.Header, input.Body)
			}
			input.Header.Set("X-Test-Mutation", "resolver")
			input.Body[0] = 'X'
			return callerhttp.Resolution{
				IdempotencyKey: "immutable-key", Task: llm.TaskContext{CapabilityTier: llm.TierChat},
			}, nil
		}),
		Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	})
	_ = transport
	defer shutdownTransport(t, runtime, server)
	request := mustRequest(t, http.MethodPost, server.URL+"/chat", "original")
	request.Header.Set("X-Test-Mutation", "original")
	response := doRequest(t, request)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("immutable snapshot response=%d", response.StatusCode)
	}
}

func TestHeaderResolverKeepsChatEmptyAndRemoteFailClosed(t *testing.T) {
	resolver := callerhttp.HeaderResolver{}
	chat := make(http.Header)
	chat.Set(callerhttp.HeaderIdempotencyKey, "chat-key")
	resolved, err := resolver.ResolveRequest(t.Context(), callerhttp.ResolutionRequest{CallerID: "caller-a", Header: chat})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Task.CapabilityTier != llm.TierChat || resolved.Task.TaskID != "" || resolved.Task.WorkspaceKey != "" {
		t.Fatalf("chat task was contaminated: %+v", resolved.Task)
	}
	chat.Set(callerhttp.HeaderWorkspaceKey, "workspace-a")
	if _, err := resolver.ResolveRequest(t.Context(), callerhttp.ResolutionRequest{CallerID: "caller-a", Header: chat}); !errors.Is(err, callerhttp.ErrResolution) {
		t.Fatalf("chat workspace error = %v", err)
	}

	remote := make(http.Header)
	remote.Set(callerhttp.HeaderIdempotencyKey, "remote-key")
	remote.Set(callerhttp.HeaderCapabilityTier, string(llm.TierRemoteTools))
	remote.Set(callerhttp.HeaderWorkspaceKey, "workspace-a")
	remote.Set(callerhttp.HeaderHarnessID, "custom")
	remote.Set(callerhttp.HeaderHarnessVersion, "1.0")
	if _, err := resolver.ResolveRequest(t.Context(), callerhttp.ResolutionRequest{CallerID: "caller-a", Header: remote}); !errors.Is(err, callerhttp.ErrResolution) {
		t.Fatalf("missing session error = %v", err)
	}
	remote.Set(callerhttp.HeaderHarnessSession, "session-a")
	remote.Set(callerhttp.HeaderTaskID, "task-a")
	remote.Set(callerhttp.HeaderAllowExec, "true")
	resolved, err = resolver.ResolveRequest(t.Context(), callerhttp.ResolutionRequest{CallerID: "caller-a", Header: remote})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Task.CapabilityTier != llm.TierRemoteTools || resolved.Task.WorkspaceKey != "workspace-a" ||
		resolved.Task.TaskID != "task-a" || !resolved.Task.ExecAllowed {
		t.Fatalf("remote task = %+v", resolved.Task)
	}
}

func TestTransportClearsOwnedOptionalResponseHeaders(t *testing.T) {
	identity := completionIdentity("headers-key", "task-headers", "")
	endpoint := &fakeEndpoint{admit: func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
		return admission(identity, responsePage(identity, llm.ResponseAggregate, true,
			llm.ResponseDecision{StatusCode: 200, ContentType: "application/json", Body: []byte(`{}`)}, nil, 2)), nil
	}}
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: fixedAuth("caller-a"),
		Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Retry-After", "poison")
		response.Header().Set(callerhttp.HeaderWorkspaceKey, "poison")
		transport.ServeHTTP(response, request)
	}))
	defer shutdownTransport(t, runtime, server)

	request := mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`)
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "headers-key")
	response := doRequest(t, request)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if values := response.Header.Values("Retry-After"); len(values) != 0 {
		t.Fatalf("stale Retry-After survived: %v", values)
	}
	if values := response.Header.Values(callerhttp.HeaderWorkspaceKey); len(values) != 0 {
		t.Fatalf("stale workspace identity survived: %v", values)
	}
}

func TestTransportClearsStaleRetryAfterOnAdmissionError(t *testing.T) {
	endpoint := &fakeEndpoint{admit: func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
		return llm.AdmissionResult{}, &llm.AdmissionError{
			Failure:     llm.AdmissionFailure{Status: 503, Code: "busy", Message: "busy"},
			ContentType: "application/json", Body: []byte(`{"error":"busy"}`),
		}
	}}
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: fixedAuth("caller-a"),
		Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Retry-After", "poison")
		transport.ServeHTTP(response, request)
	}))
	defer shutdownTransport(t, runtime, server)
	request := mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`)
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "admission-header-key")
	response := doRequest(t, request)
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if values := response.Header.Values("Retry-After"); len(values) != 0 {
		t.Fatalf("stale Retry-After survived: %v", values)
	}
}

func TestTransportRejectsEndpointPagesBeyondConfiguredBudgetBeforeCommit(t *testing.T) {
	tests := []struct {
		name   string
		config callerhttp.Config
		mode   llm.ResponseMode
		page   func(llm.CompletionIdentity) llm.ResponsePage
	}{
		{
			name:   "event count",
			config: callerhttp.Config{PageLimit: 1},
			mode:   llm.ResponseStream,
			page: func(identity llm.CompletionIdentity) llm.ResponsePage {
				return responsePage(identity, llm.ResponseStream, false,
					llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"},
					[]llm.WireEvent{{Sequence: 2, Data: []byte("a")}, {Sequence: 3, Data: []byte("b")}}, 3)
			},
		},
		{
			name:   "aggregate bytes",
			config: callerhttp.Config{PageMaxBytes: 3},
			mode:   llm.ResponseAggregate,
			page: func(identity llm.CompletionIdentity) llm.ResponsePage {
				return responsePage(identity, llm.ResponseAggregate, true,
					llm.ResponseDecision{StatusCode: 200, ContentType: "application/json", Body: []byte("1234")},
					nil, 2)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := llm.IdempotencyKey("budget-" + strings.ReplaceAll(test.name, " ", "-"))
			identity := completionIdentity(key, "task-budget", "")
			endpoint := &fakeEndpoint{}
			endpoint.admit = func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
				return admission(identity, responsePage(identity, test.mode, false, llm.ResponseDecision{}, nil, 1)), nil
			}
			endpoint.wait = func(_ context.Context, query llm.ResponseQuery) (llm.ResponsePage, error) {
				if query.After != 1 {
					t.Fatalf("cursor = %d", query.After)
				}
				return test.page(identity), nil
			}
			test.config.Authenticator = fixedAuth("caller-a")
			test.config.Routes = []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}}
			_, runtime, server := startTransport(t, endpoint, test.config)
			defer shutdownTransport(t, runtime, server)
			request := mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`)
			request.Header.Set(callerhttp.HeaderIdempotencyKey, string(key))
			response := doRequest(t, request)
			defer response.Body.Close()
			if response.StatusCode != http.StatusInternalServerError {
				t.Fatalf("status = %d", response.StatusCode)
			}
		})
	}
}

func TestCallerDisconnectDoesNotCancelDurableJobAndExactRetryReplays(t *testing.T) {
	identity := completionIdentity("replay-key", "task-replay", "")
	start := llm.WireEvent{Sequence: 2, Data: []byte("start\n")}
	finish := llm.WireEvent{Sequence: 4, Data: []byte("finish\n")}
	firstWaitCanceled := make(chan struct{})
	allowReplay := make(chan struct{})
	var admissions atomic.Int64
	endpoint := &fakeEndpoint{}
	endpoint.admit = func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
		admissions.Add(1)
		return admission(identity, responsePage(identity, llm.ResponseStream, false,
			llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"},
			[]llm.WireEvent{start}, 2)), nil
	}
	endpoint.wait = func(ctx context.Context, query llm.ResponseQuery) (llm.ResponsePage, error) {
		select {
		case <-allowReplay:
			return responsePage(identity, llm.ResponseStream, true,
				llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"},
				[]llm.WireEvent{finish}, 4), nil
		default:
		}
		<-ctx.Done()
		select {
		case <-firstWaitCanceled:
		default:
			close(firstWaitCanceled)
		}
		return llm.ResponsePage{}, ctx.Err()
	}
	_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
		Authenticator: fixedAuth("caller-a"),
		Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	})
	defer shutdownTransport(t, runtime, server)

	ctx, cancel := context.WithCancel(t.Context())
	request := mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`)
	request = request.WithContext(ctx)
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "replay-key")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(start.Data))
	if _, err := io.ReadFull(response.Body, buffer); err != nil || !bytes.Equal(buffer, start.Data) {
		t.Fatalf("first stream prefix=%q err=%v", buffer, err)
	}
	cancel()
	_ = response.Body.Close()
	select {
	case <-firstWaitCanceled:
	case <-time.After(time.Second):
		t.Fatal("caller disconnect did not cancel only the active response wait")
	}
	close(allowReplay)

	retry := mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`)
	retry.Header.Set(callerhttp.HeaderIdempotencyKey, "replay-key")
	replayed := doRequest(t, retry)
	defer replayed.Body.Close()
	body, err := io.ReadAll(replayed.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "start\nfinish\n" || admissions.Load() != 2 {
		t.Fatalf("replay body=%q admissions=%d", body, admissions.Load())
	}
}

func TestRuntimeOwnsHandlersButNeverBorrowedEndpoint(t *testing.T) {
	waitEntered := make(chan struct{})
	endpoint := &fakeEndpoint{}
	identity := completionIdentity("shutdown-key", "task-shutdown", "")
	endpoint.admit = func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
		return admission(identity, responsePage(identity, llm.ResponseStream, false,
			llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"}, nil, 1)), nil
	}
	endpoint.wait = func(ctx context.Context, _ llm.ResponseQuery) (llm.ResponsePage, error) {
		select {
		case <-waitEntered:
		default:
			close(waitEntered)
		}
		<-ctx.Done()
		return llm.ResponsePage{}, ctx.Err()
	}
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: fixedAuth("caller-a"),
		Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	})
	if err != nil {
		t.Fatal(err)
	}
	initialization, cancelInitialization := context.WithCancel(t.Context())
	runtime, err := transport.Start(initialization, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	cancelInitialization()
	select {
	case <-runtime.Done():
		t.Fatal("runtime retained the initialization context")
	default:
	}
	server := httptest.NewServer(transport)
	request := mustRequest(t, http.MethodPost, server.URL+"/chat", `{}`)
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "shutdown-key")
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}()
	select {
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("handler did not reach WaitResponse")
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runtime.Done():
	case <-time.After(time.Second):
		t.Fatal("runtime Done did not close")
	}
	select {
	case <-clientDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not join the active handler")
	}
	if endpoint.shutdownCalls.Load() != 0 {
		t.Fatal("caller transport shut down its borrowed endpoint")
	}
	server.Close()
}

func TestShutdownInterruptsHalfOpenRequestBodyWithoutOwningListener(t *testing.T) {
	endpoint := &fakeEndpoint{}
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: fixedAuth("caller-a"), ReadTimeout: time.Minute,
		Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	body := newBlockingBody()
	// ResponseRecorder deliberately exposes no ResponseController deadline
	// methods. Closing Request.Body is therefore the only way for the adapter to
	// interrupt this half-open read without owning the listener.
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "http://human.test/chat", nil)
	request.Body = body
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "half-open-key")
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		transport.ServeHTTP(response, request)
	}()
	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("handler did not block in request body read")
	}
	shutdownCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not interrupt the half-open body read")
	}
	if endpoint.admitCalls.Load() != 0 {
		t.Fatal("half-open request reached durable admission")
	}
}

func TestConfigurationRejectsTypedNilAndFreezesRoutes(t *testing.T) {
	var typedAuth *pointerAuth
	if _, err := callerhttp.New(callerhttp.Config{
		Authenticator: typedAuth,
		Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	}); !errors.Is(err, callerhttp.ErrInvalidConfiguration) {
		t.Fatalf("typed auth error = %v", err)
	}
	var typedResolver *pointerResolver
	if _, err := callerhttp.New(callerhttp.Config{
		Authenticator: fixedAuth("caller-a"), Resolver: typedResolver,
		Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
	}); !errors.Is(err, callerhttp.ErrInvalidConfiguration) {
		t.Fatalf("typed resolver error = %v", err)
	}
	for name, config := range map[string]callerhttp.Config{
		"page count": {
			Authenticator: fixedAuth("caller-a"), PageLimit: callerhttp.MaxResponsePageLimit + 1,
			Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
		},
		"page bytes": {
			Authenticator: fixedAuth("caller-a"), PageMaxBytes: callerhttp.MaxResponsePageBytes + 1,
			Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: testCodec}},
		},
		"codec id": {
			Authenticator: fixedAuth("caller-a"),
			Routes:        []callerhttp.Route{{Method: http.MethodPost, Path: "/chat", CodecID: "Invalid"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := callerhttp.New(config); !errors.Is(err, callerhttp.ErrInvalidConfiguration) {
				t.Fatalf("configuration error = %v", err)
			}
		})
	}
	routes := []callerhttp.Route{{Method: http.MethodPost, Path: "/original", CodecID: testCodec}}
	transport, err := callerhttp.New(callerhttp.Config{Authenticator: fixedAuth("caller-a"), Routes: routes})
	if err != nil {
		t.Fatal(err)
	}
	routes[0].Path = "/mutated"
	description, err := llm.ValidateCallerTransport(transport)
	if err != nil || description.Provider != "http" || description.Version != "1" {
		t.Fatalf("description=%+v err=%v", description, err)
	}
	var typedEndpoint *fakeEndpoint
	if _, err := transport.Start(t.Context(), typedEndpoint); !errors.Is(err, callerhttp.ErrInvalidConfiguration) {
		t.Fatalf("typed endpoint error = %v", err)
	}
	endpoint := &fakeEndpoint{admit: terminalAggregate("frozen-key")}
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(transport)
	defer shutdownTransport(t, runtime, server)
	request := mustRequest(t, http.MethodPost, server.URL+"/original", `{}`)
	request.Header.Set(callerhttp.HeaderIdempotencyKey, "frozen-key")
	response := doRequest(t, request)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("frozen route status=%d", response.StatusCode)
	}
	if _, err := transport.Start(t.Context(), endpoint); !errors.Is(err, callerhttp.ErrAlreadyStarted) {
		t.Fatalf("second Start error = %v", err)
	}
}

type fakeEndpoint struct {
	admit func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error)
	read  func(context.Context, llm.ResponseQuery) (llm.ResponsePage, error)
	wait  func(context.Context, llm.ResponseQuery) (llm.ResponsePage, error)

	admitCalls    atomic.Int64
	shutdownCalls atomic.Int64
}

func (endpoint *fakeEndpoint) Admit(ctx context.Context, request llm.AdmissionRequest) (llm.AdmissionResult, error) {
	endpoint.admitCalls.Add(1)
	if endpoint.admit == nil {
		return llm.AdmissionResult{}, errors.New("unexpected Admit")
	}
	return endpoint.admit(ctx, request)
}

func (endpoint *fakeEndpoint) ReadResponse(ctx context.Context, query llm.ResponseQuery) (llm.ResponsePage, error) {
	if endpoint.read == nil {
		return llm.ResponsePage{}, errors.New("unexpected ReadResponse")
	}
	return endpoint.read(ctx, query)
}

func (endpoint *fakeEndpoint) WaitResponse(ctx context.Context, query llm.ResponseQuery) (llm.ResponsePage, error) {
	if endpoint.wait == nil {
		return llm.ResponsePage{}, errors.New("unexpected WaitResponse")
	}
	return endpoint.wait(ctx, query)
}

// Shutdown is deliberately outside llm.CallerEndpoint. The test asserts that
// the borrowing adapter never discovers or invokes it through type assertion.
func (endpoint *fakeEndpoint) Shutdown(context.Context) error {
	endpoint.shutdownCalls.Add(1)
	return nil
}

type pointerAuth struct{}

func (*pointerAuth) AuthenticateCaller(context.Context, *http.Request) (callerhttp.Identity, error) {
	return callerhttp.Identity{CallerID: "caller-a"}, nil
}

type pointerResolver struct{}

func (*pointerResolver) ResolveRequest(context.Context, callerhttp.ResolutionRequest) (callerhttp.Resolution, error) {
	return callerhttp.Resolution{IdempotencyKey: "key", Task: llm.TaskContext{CapabilityTier: llm.TierChat}}, nil
}

type blockingBody struct {
	started chan struct{}
	release chan struct{}
	start   sync.Once
	stop    sync.Once
}

func newBlockingBody() *blockingBody {
	return &blockingBody{started: make(chan struct{}), release: make(chan struct{})}
}

func (body *blockingBody) Read([]byte) (int, error) {
	body.start.Do(func() { close(body.started) })
	<-body.release
	return 0, errors.New("request body read interrupted")
}

func (body *blockingBody) Close() error {
	body.unblock()
	return nil
}

func (body *blockingBody) unblock() {
	body.stop.Do(func() { close(body.release) })
}

func fixedAuth(caller llm.CallerID) callerhttp.Authenticator {
	return callerhttp.AuthenticateFunc(func(context.Context, *http.Request) (callerhttp.Identity, error) {
		return callerhttp.Identity{CallerID: caller}, nil
	})
}

func completionIdentity(key, task llm.IdempotencyKey, workspace string) llm.CompletionIdentity {
	return llm.CompletionIdentity{
		CallerID: "caller-a", RequestID: "request-" + string(key), TaskID: llm.TaskID(task),
		WorkspaceKey: workspace, IdempotencyKey: key,
	}
}

const testDigest llm.StoreDigest = "digest-a"

func admission(identity llm.CompletionIdentity, page llm.ResponsePage) llm.AdmissionResult {
	page.Identity = identity
	page.RequestDigest = testDigest
	return llm.AdmissionResult{Identity: identity, RequestDigest: testDigest, Response: page}
}

func responsePage(
	identity llm.CompletionIdentity,
	mode llm.ResponseMode,
	complete bool,
	decision llm.ResponseDecision,
	events []llm.WireEvent,
	cursor uint64,
) llm.ResponsePage {
	return llm.ResponsePage{
		Identity: identity, RequestDigest: testDigest, Mode: mode,
		DecisionCommitted: decision.StatusCode != 0, Decision: decision,
		Complete: complete, Events: events, Cursor: cursor,
	}
}

func terminalAggregate(key llm.IdempotencyKey) func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
	return func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
		identity := completionIdentity(key, "task-"+key, "")
		return admission(identity, responsePage(identity, llm.ResponseAggregate, true,
			llm.ResponseDecision{StatusCode: 200, ContentType: "application/json", Body: []byte(`{}`)}, nil, 2)), nil
	}
}

func setWorkspaceHeaders(request *http.Request, key string) {
	request.Header.Set(callerhttp.HeaderIdempotencyKey, key)
	request.Header.Set(callerhttp.HeaderCapabilityTier, string(llm.TierWorkspace))
	request.Header.Set(callerhttp.HeaderWorkspaceKey, "workspace-a")
	request.Header.Set(callerhttp.HeaderHarnessID, "test-harness")
	request.Header.Set(callerhttp.HeaderHarnessVersion, "1.0")
	request.Header.Set(callerhttp.HeaderHarnessSession, "session-a")
}

func startTransport(
	t *testing.T,
	endpoint llm.CallerEndpoint,
	config callerhttp.Config,
) (*callerhttp.Transport, llm.CallerTransportRuntime, *httptest.Server) {
	t.Helper()
	transport, err := callerhttp.New(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return transport, runtime, httptest.NewServer(transport)
}

func shutdownTransport(t *testing.T, runtime llm.CallerTransportRuntime, server *httptest.Server) {
	t.Helper()
	server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		t.Errorf("shutdown transport: %v", err)
	}
}

func mustRequest(t *testing.T, method, url, body string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func doRequest(t *testing.T, request *http.Request) *http.Response {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
