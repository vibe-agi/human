package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
)

var errUnexpectedEndpointCall = errors.New("unexpected caller endpoint call")

type scriptedCallerEndpoint struct {
	admit func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error)
	read  func(context.Context, llm.ResponseQuery) (llm.ResponsePage, error)
	wait  func(context.Context, llm.ResponseQuery) (llm.ResponsePage, error)
}

func (endpoint *scriptedCallerEndpoint) Admit(
	ctx context.Context,
	request llm.AdmissionRequest,
) (llm.AdmissionResult, error) {
	if endpoint.admit == nil {
		return llm.AdmissionResult{}, errUnexpectedEndpointCall
	}
	return endpoint.admit(ctx, request)
}

func (endpoint *scriptedCallerEndpoint) ReadResponse(
	ctx context.Context,
	query llm.ResponseQuery,
) (llm.ResponsePage, error) {
	if endpoint.read == nil {
		return llm.ResponsePage{}, errUnexpectedEndpointCall
	}
	return endpoint.read(ctx, query)
}

func (endpoint *scriptedCallerEndpoint) WaitResponse(
	ctx context.Context,
	query llm.ResponseQuery,
) (llm.ResponsePage, error) {
	if endpoint.wait == nil {
		return llm.ResponsePage{}, errUnexpectedEndpointCall
	}
	return endpoint.wait(ctx, query)
}

func startTestTransport(
	t *testing.T,
	auth authenticator,
	endpoint llm.CallerEndpoint,
) (*inProcessTransport, llm.CallerTransportRuntime) {
	t.Helper()
	transport := newInProcessTransport(auth)
	runtime, err := transport.Start(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Shutdown(ctx); err != nil {
			t.Errorf("shutdown custom caller transport: %v", err)
		}
	})
	return transport, runtime
}

func testCall(key llm.IdempotencyKey) call {
	return call{
		Token: "correct-token", IdempotencyKey: key, CodecID: "test.codec",
		Body: []byte(`{"messages":[{"role":"user","content":"hello"}]}`),
		Task: llm.TaskContext{CapabilityTier: llm.TierChat},
	}
}

func testIdentity(key llm.IdempotencyKey) llm.CompletionIdentity {
	return llm.CompletionIdentity{
		CallerID: "caller-a", RequestID: "request-a", TaskID: "task-a",
		IdempotencyKey: key,
	}
}

func TestCustomFrameworkExample(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := run(ctx, &output); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{
		"audit store.bind",
		"audit store.view",
		"audit store.update",
		"audit protector.seal",
		"audit protector.open",
		"HumanLLM status=200",
		"Hello from a Human worker.",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("output does not contain %q:\n%s", want, text)
		}
	}
}

func TestInProcessTransportAuthenticatesBeforeEndpoint(t *testing.T) {
	var reached atomic.Int64
	endpoint := &scriptedCallerEndpoint{
		admit: func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
			reached.Add(1)
			return llm.AdmissionResult{}, errUnexpectedEndpointCall
		},
		read: func(context.Context, llm.ResponseQuery) (llm.ResponsePage, error) {
			reached.Add(1)
			return llm.ResponsePage{}, errUnexpectedEndpointCall
		},
		wait: func(context.Context, llm.ResponseQuery) (llm.ResponsePage, error) {
			reached.Add(1)
			return llm.ResponsePage{}, errUnexpectedEndpointCall
		},
	}
	transport, _ := startTestTransport(
		t,
		newTokenAuthenticator("correct-token", "caller-a"),
		endpoint,
	)
	request := testCall("wrong-token-request")
	request.Token = "wrong-token"
	if _, err := transport.Call(t.Context(), request); !errors.Is(err, errAuthentication) {
		t.Fatalf("Call error = %v, want errAuthentication", err)
	}
	if got := reached.Load(); got != 0 {
		t.Fatalf("endpoint calls after failed authentication = %d, want 0", got)
	}
}

func TestInProcessTransportProjectsAdmissionErrorWithoutCause(t *testing.T) {
	secret := errors.New("database password=do-not-expose")
	body := []byte(`{"error":{"code":"busy"}}`)
	endpoint := &scriptedCallerEndpoint{
		admit: func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
			return llm.AdmissionResult{}, fmt.Errorf("private endpoint wrapper: %w", &llm.AdmissionError{
				Failure: llm.AdmissionFailure{
					Status: 429, Code: "worker_busy", Message: "Human worker is busy",
				},
				ContentType: "application/json", RetryAfter: "7", Body: body, Cause: secret,
			})
		},
	}
	transport, _ := startTestTransport(
		t,
		newTokenAuthenticator("correct-token", "caller-a"),
		endpoint,
	)
	result, err := transport.Call(t.Context(), testCall("safe-admission-error"))
	if err != nil {
		t.Fatalf("Call error = %v, want projected result", err)
	}
	want := callResult{
		StatusCode: 429, ContentType: "application/json", RetryAfter: "7",
		Body: []byte(`{"error":{"code":"busy"}}`),
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("projected result = %+v, want %+v", result, want)
	}
	body[0] = '!'
	if !bytes.Equal(result.Body, want.Body) {
		t.Fatalf("projected body aliases endpoint memory: got %q, want %q", result.Body, want.Body)
	}
}

func TestInProcessTransportCollapsesUnsafeEndpointErrors(t *testing.T) {
	secret := errors.New("postgres://admin:secret@internal")
	tests := []struct {
		name string
		err  error
	}{
		{name: "unknown error", err: secret},
		{
			name: "malformed admission error",
			err: &llm.AdmissionError{
				Failure: llm.AdmissionFailure{
					Status: 503, Code: "internal_error", Message: "temporarily unavailable",
				},
				ContentType: "text/plain\r\nX-Secret: yes", Body: []byte("safe"), Cause: secret,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := &scriptedCallerEndpoint{
				admit: func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
					return llm.AdmissionResult{}, test.err
				},
			}
			transport, _ := startTestTransport(
				t,
				newTokenAuthenticator("correct-token", "caller-a"),
				endpoint,
			)
			_, err := transport.Call(t.Context(), testCall("unsafe-endpoint-error"))
			if err != errEndpointFailure {
				t.Fatalf("Call error = %v, want fixed errEndpointFailure", err)
			}
			if errors.Is(err, secret) || strings.Contains(err.Error(), "secret") ||
				strings.Contains(err.Error(), "X-Secret") {
				t.Fatalf("Call exposed endpoint error details: %v", err)
			}
		})
	}
}

func TestInProcessTransportCollapsesPostAdmissionEndpointError(t *testing.T) {
	key := llm.IdempotencyKey("post-admission-error")
	identity := testIdentity(key)
	digest := llm.StoreDigest("sha256:post-admission-error")
	secret := errors.New("queue shard password=secret")
	endpoint := &scriptedCallerEndpoint{
		admit: func(context.Context, llm.AdmissionRequest) (llm.AdmissionResult, error) {
			return llm.AdmissionResult{
				Identity: identity, RequestDigest: digest,
				Response: llm.ResponsePage{
					Identity: identity, RequestDigest: digest, Mode: llm.ResponseStream,
					DecisionCommitted: true,
					Decision: llm.ResponseDecision{
						StatusCode: 200, ContentType: "text/event-stream",
					},
				},
			}, nil
		},
		wait: func(context.Context, llm.ResponseQuery) (llm.ResponsePage, error) {
			return llm.ResponsePage{}, secret
		},
	}
	transport, _ := startTestTransport(
		t,
		newTokenAuthenticator("correct-token", identity.CallerID),
		endpoint,
	)
	_, err := transport.Call(t.Context(), testCall(key))
	if err != errEndpointFailure {
		t.Fatalf("Call error = %v, want fixed errEndpointFailure", err)
	}
	if errors.Is(err, secret) || strings.Contains(err.Error(), "password") {
		t.Fatalf("Call exposed post-admission endpoint error details: %v", err)
	}
}

func TestInProcessTransportPreservesStreamFrameOrder(t *testing.T) {
	key := llm.IdempotencyKey("stream-request")
	identity := testIdentity(key)
	digest := llm.StoreDigest("sha256:stream-request")
	decision := llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"}
	wantFrames := [][]byte{
		[]byte("data: one\n\n"),
		[]byte("data: two\n\n"),
		[]byte("data: three\n\n"),
		[]byte("data: done\n\n"),
	}
	var observedAfter []uint64
	endpoint := &scriptedCallerEndpoint{
		admit: func(_ context.Context, request llm.AdmissionRequest) (llm.AdmissionResult, error) {
			if request.CallerID != identity.CallerID || request.IdempotencyKey != key {
				return llm.AdmissionResult{}, errors.New("unexpected authenticated admission")
			}
			return llm.AdmissionResult{
				Identity: identity, RequestDigest: digest,
				Response: llm.ResponsePage{
					Identity: identity, RequestDigest: digest, Mode: llm.ResponseStream,
					DecisionCommitted: true, Decision: decision,
					Events: []llm.WireEvent{{Sequence: 1, Data: wantFrames[0]}}, Cursor: 1,
				},
			}, nil
		},
		wait: func(_ context.Context, query llm.ResponseQuery) (llm.ResponsePage, error) {
			observedAfter = append(observedAfter, query.After)
			switch query.After {
			case 1:
				return llm.ResponsePage{
					Identity: identity, RequestDigest: digest, Mode: llm.ResponseStream,
					DecisionCommitted: true, Decision: decision,
					Events: []llm.WireEvent{
						{Sequence: 2, Data: wantFrames[1]},
						{Sequence: 3, Data: wantFrames[2]},
					},
					Cursor: 3,
				}, nil
			case 3:
				return llm.ResponsePage{
					Identity: identity, RequestDigest: digest, Mode: llm.ResponseStream,
					DecisionCommitted: true, Decision: decision,
					Complete: true,
					Events:   []llm.WireEvent{{Sequence: 4, Data: wantFrames[3]}}, Cursor: 4,
				}, nil
			default:
				return llm.ResponsePage{}, errors.New("unexpected response cursor")
			}
		},
	}
	transport, _ := startTestTransport(
		t,
		newTokenAuthenticator("correct-token", identity.CallerID),
		endpoint,
	)
	result, err := transport.Call(t.Context(), testCall(key))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Frames, wantFrames) {
		t.Fatalf("frames = %q, want %q", result.Frames, wantFrames)
	}
	if !reflect.DeepEqual(observedAfter, []uint64{1, 3}) {
		t.Fatalf("WaitResponse cursors = %v, want [1 3]", observedAfter)
	}
	if result.Identity != identity || result.RequestDigest != digest || result.StatusCode != 200 {
		t.Fatalf("result identity/decision changed: %+v", result)
	}
}

type retryCallerEndpoint struct {
	identity llm.CompletionIdentity
	digest   llm.StoreDigest
	frame    []byte

	mu          sync.Mutex
	created     bool
	complete    bool
	admissions  int
	creations   int
	waitEntered chan struct{}
	waitOnce    sync.Once
}

func newRetryCallerEndpoint(key llm.IdempotencyKey) *retryCallerEndpoint {
	return &retryCallerEndpoint{
		identity: testIdentity(key), digest: "sha256:retry-request",
		frame: []byte("data: exact-replay\n\n"), waitEntered: make(chan struct{}),
	}
}

func (endpoint *retryCallerEndpoint) Admit(
	ctx context.Context,
	request llm.AdmissionRequest,
) (llm.AdmissionResult, error) {
	if err := ctx.Err(); err != nil {
		return llm.AdmissionResult{}, err
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if request.CallerID != endpoint.identity.CallerID ||
		request.IdempotencyKey != endpoint.identity.IdempotencyKey {
		return llm.AdmissionResult{}, errors.New("admission authority changed")
	}
	endpoint.admissions++
	replay := endpoint.created
	if !endpoint.created {
		endpoint.created = true
		endpoint.creations++
	}
	page := llm.ResponsePage{
		Identity: endpoint.identity, RequestDigest: endpoint.digest, Mode: llm.ResponseStream,
		DecisionCommitted: true,
		Decision:          llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"},
	}
	if endpoint.complete {
		page.Complete = true
		page.Cursor = 1
		page.Events = []llm.WireEvent{{Sequence: 1, Data: append([]byte(nil), endpoint.frame...)}}
	}
	return llm.AdmissionResult{
		Identity: endpoint.identity, RequestDigest: endpoint.digest,
		Replay: replay, Response: page,
	}, nil
}

func (endpoint *retryCallerEndpoint) ReadResponse(
	context.Context,
	llm.ResponseQuery,
) (llm.ResponsePage, error) {
	return llm.ResponsePage{}, errUnexpectedEndpointCall
}

func (endpoint *retryCallerEndpoint) WaitResponse(
	ctx context.Context,
	_ llm.ResponseQuery,
) (llm.ResponsePage, error) {
	endpoint.waitOnce.Do(func() { close(endpoint.waitEntered) })
	<-ctx.Done()
	return llm.ResponsePage{}, ctx.Err()
}

func (endpoint *retryCallerEndpoint) finish() {
	endpoint.mu.Lock()
	endpoint.complete = true
	endpoint.mu.Unlock()
}

func (endpoint *retryCallerEndpoint) counts() (admissions, creations int) {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	return endpoint.admissions, endpoint.creations
}

func TestInProcessTransportCallerCancelAllowsExactRetry(t *testing.T) {
	key := llm.IdempotencyKey("retry-request")
	endpoint := newRetryCallerEndpoint(key)
	transport, _ := startTestTransport(
		t,
		newTokenAuthenticator("correct-token", endpoint.identity.CallerID),
		endpoint,
	)
	request := testCall(key)
	type outcome struct {
		result callResult
		err    error
	}
	first := make(chan outcome, 1)
	callContext, cancelCall := context.WithCancel(t.Context())
	defer cancelCall()
	go func() {
		result, err := transport.Call(callContext, request)
		first <- outcome{result: result, err: err}
	}()
	select {
	case <-endpoint.waitEntered:
	case <-time.After(time.Second):
		t.Fatal("first call did not reach response wait")
	}
	cancelCall()
	select {
	case result := <-first:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("canceled Call error = %v, want context.Canceled", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled call did not return")
	}

	// The endpoint models the durable work finishing after the caller has gone.
	// Reusing the same idempotency key must observe that one request, byte for byte.
	endpoint.finish()
	second, err := transport.Call(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	third, err := transport.Call(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second, third) {
		t.Fatalf("exact retry changed result:\nsecond=%+v\nthird=%+v", second, third)
	}
	if !reflect.DeepEqual(second.Frames, [][]byte{endpoint.frame}) {
		t.Fatalf("retry frames = %q, want %q", second.Frames, endpoint.frame)
	}
	if admissions, creations := endpoint.counts(); admissions != 3 || creations != 1 {
		t.Fatalf("admissions/creations = %d/%d, want 3/1", admissions, creations)
	}
}

type blockingCallerEndpoint struct {
	identity llm.CompletionIdentity
	digest   llm.StoreDigest

	waitEntered  chan struct{}
	waitCanceled chan struct{}
	releaseWait  chan struct{}
	enterOnce    sync.Once
	cancelOnce   sync.Once
	releaseOnce  sync.Once
	shutdowns    atomic.Int64
}

func newBlockingCallerEndpoint(key llm.IdempotencyKey) *blockingCallerEndpoint {
	return &blockingCallerEndpoint{
		identity: testIdentity(key), digest: "sha256:shutdown-request",
		waitEntered: make(chan struct{}), waitCanceled: make(chan struct{}),
		releaseWait: make(chan struct{}),
	}
}

func (endpoint *blockingCallerEndpoint) Admit(
	ctx context.Context,
	request llm.AdmissionRequest,
) (llm.AdmissionResult, error) {
	if err := ctx.Err(); err != nil {
		return llm.AdmissionResult{}, err
	}
	if request.CallerID != endpoint.identity.CallerID ||
		request.IdempotencyKey != endpoint.identity.IdempotencyKey {
		return llm.AdmissionResult{}, errors.New("admission authority changed")
	}
	return llm.AdmissionResult{
		Identity: endpoint.identity, RequestDigest: endpoint.digest,
		Response: llm.ResponsePage{
			Identity: endpoint.identity, RequestDigest: endpoint.digest,
			Mode:              llm.ResponseStream,
			DecisionCommitted: true,
			Decision:          llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"},
		},
	}, nil
}

func (endpoint *blockingCallerEndpoint) ReadResponse(
	context.Context,
	llm.ResponseQuery,
) (llm.ResponsePage, error) {
	return llm.ResponsePage{}, errUnexpectedEndpointCall
}

func (endpoint *blockingCallerEndpoint) WaitResponse(
	ctx context.Context,
	_ llm.ResponseQuery,
) (llm.ResponsePage, error) {
	endpoint.enterOnce.Do(func() { close(endpoint.waitEntered) })
	<-ctx.Done()
	endpoint.cancelOnce.Do(func() { close(endpoint.waitCanceled) })
	<-endpoint.releaseWait
	return llm.ResponsePage{}, ctx.Err()
}

// Shutdown is deliberately not part of llm.CallerEndpoint. It makes accidental
// ownership expansion observable if the custom transport ever type-asserts it.
func (endpoint *blockingCallerEndpoint) Shutdown(context.Context) error {
	endpoint.shutdowns.Add(1)
	return nil
}

func (endpoint *blockingCallerEndpoint) release() {
	endpoint.releaseOnce.Do(func() { close(endpoint.releaseWait) })
}

func TestInProcessTransportShutdownCancelsAndDrainsWithoutClosingEndpoint(t *testing.T) {
	key := llm.IdempotencyKey("shutdown-request")
	endpoint := newBlockingCallerEndpoint(key)
	t.Cleanup(endpoint.release)
	transport, runtime := startTestTransport(
		t,
		newTokenAuthenticator("correct-token", endpoint.identity.CallerID),
		endpoint,
	)
	callResult := make(chan error, 1)
	go func() {
		_, err := transport.Call(context.Background(), testCall(key))
		callResult <- err
	}()
	select {
	case <-endpoint.waitEntered:
	case <-time.After(time.Second):
		t.Fatal("active call did not reach response wait")
	}

	shutdownResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownResult <- runtime.Shutdown(ctx)
	}()
	select {
	case <-endpoint.waitCanceled:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not cancel the active endpoint operation")
	}
	select {
	case err := <-shutdownResult:
		t.Fatalf("Shutdown returned before the active call drained: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	endpoint.release()
	select {
	case err := <-callResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active Call error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active call did not drain")
	}
	select {
	case err := <-shutdownResult:
		if err != nil {
			t.Fatalf("Shutdown error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after the active call drained")
	}
	if got := endpoint.shutdowns.Load(); got != 0 {
		t.Fatalf("borrowed endpoint Shutdown calls = %d, want 0", got)
	}
	if _, err := endpoint.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: endpoint.identity.CallerID, IdempotencyKey: key,
	}); err != nil {
		t.Fatalf("borrowed endpoint was not usable after transport shutdown: %v", err)
	}
	if _, err := transport.Call(t.Context(), testCall(key)); !errors.Is(err, errTransportClosed) {
		t.Fatalf("Call after Shutdown error = %v, want errTransportClosed", err)
	}
}

func TestInProcessTransportRejectsBrokenResponsePages(t *testing.T) {
	identity := llm.CompletionIdentity{
		CallerID: "caller-a", RequestID: "request-a", TaskID: "task-a",
		IdempotencyKey: "key-a",
	}
	digest := llm.StoreDigest("sha256:example")
	streamDecision := llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"}
	committedStream := responseState{
		seen: true, mode: llm.ResponseStream, cursor: 1,
		decisionCommitted: true, decision: streamDecision,
	}
	tests := []struct {
		name  string
		state responseState
		page  llm.ResponsePage
	}{
		{
			name: "identity changed",
			page: llm.ResponsePage{
				Identity: llm.CompletionIdentity{
					CallerID: "caller-b", RequestID: "request-a", TaskID: "task-a",
					IdempotencyKey: "key-a",
				},
				RequestDigest: digest, Mode: llm.ResponseAggregate,
			},
		},
		{
			name: "wait made no progress",
			state: responseState{
				seen: true, mode: llm.ResponseAggregate, cursor: 3,
			},
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest,
				Mode: llm.ResponseAggregate, Cursor: 3,
			},
		},
		{
			name: "event is not after cursor",
			state: responseState{
				seen: true, mode: llm.ResponseStream, cursor: 2,
				decisionCommitted: true, decision: streamDecision,
			},
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest,
				Mode: llm.ResponseStream, DecisionCommitted: true,
				Decision: streamDecision, Cursor: 3,
				Events: []llm.WireEvent{{Sequence: 2, Data: []byte("stale")}},
			},
		},
		{
			name: "complete without decision",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest,
				Mode: llm.ResponseAggregate, Complete: true,
			},
		},
		{
			name: "events before decision",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest, Mode: llm.ResponseStream,
				Cursor: 1, Events: []llm.WireEvent{{Sequence: 1, Data: []byte("early")}},
			},
		},
		{
			name: "uncommitted decision payload",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest, Mode: llm.ResponseStream,
				Decision: llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"},
			},
		},
		{
			name: "invalid decision status",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest, Mode: llm.ResponseAggregate,
				DecisionCommitted: true, Complete: true,
				Decision: llm.ResponseDecision{StatusCode: http.StatusNoContent, ContentType: "application/json"},
			},
		},
		{
			name: "invalid decision metadata",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest, Mode: llm.ResponseAggregate,
				DecisionCommitted: true, Complete: true,
				Decision: llm.ResponseDecision{StatusCode: 500, ContentType: "text/plain\r\nX-Forged: yes"},
			},
		},
		{
			name: "aggregate contains events",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest, Mode: llm.ResponseAggregate,
				DecisionCommitted: true, Complete: true,
				Decision: llm.ResponseDecision{StatusCode: 200, ContentType: "application/json", Body: []byte("{}")},
				Cursor:   1, Events: []llm.WireEvent{{Sequence: 1, Data: []byte("not aggregate")}},
			},
		},
		{
			name: "aggregate decision before completion",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest, Mode: llm.ResponseAggregate,
				DecisionCommitted: true,
				Decision:          llm.ResponseDecision{StatusCode: 200, ContentType: "application/json", Body: []byte("{}")},
			},
		},
		{
			name: "stream decision contains body",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest, Mode: llm.ResponseStream,
				DecisionCommitted: true,
				Decision: llm.ResponseDecision{
					StatusCode: 200, ContentType: "text/event-stream", Body: []byte("not a stream frame"),
				},
			},
		},
		{
			name:  "later decision changed",
			state: committedStream,
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest, Mode: llm.ResponseStream,
				DecisionCommitted: true, Complete: true, Cursor: 2,
				Decision: llm.ResponseDecision{StatusCode: 500, ContentType: "text/event-stream"},
			},
		},
		{
			name:  "later decision disappeared",
			state: committedStream,
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest, Mode: llm.ResponseStream,
				Cursor: 2,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validatePage(test.page, identity, digest, test.state)
			if !errors.Is(err, errEndpointProtocol) {
				t.Fatalf("validatePage error = %v, want errEndpointProtocol", err)
			}
		})
	}
}
