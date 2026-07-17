package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	storeapi "github.com/vibe-agi/human/internal/store"
)

type scriptedResponseStore struct {
	storeapi.CompletionStore
	read          func(context.Context, storeapi.RequestKey, int64) (storeapi.ResponseRead, error)
	beginResponse func(context.Context, storeapi.RequestKey) (storeapi.Request, error)
	failRequest   func(context.Context, storeapi.RequestKey, completion.State, storeapi.ResponseDecision) (storeapi.Request, error)
}

type failingWriteRecorder struct {
	*httptest.ResponseRecorder
	err error
}

func (recorder *failingWriteRecorder) Write([]byte) (int, error) {
	return 0, recorder.err
}

func (store scriptedResponseStore) ReadResponse(
	ctx context.Context,
	key storeapi.RequestKey,
	after int64,
) (storeapi.ResponseRead, error) {
	return store.read(ctx, key, after)
}

func (store scriptedResponseStore) BeginResponse(
	ctx context.Context,
	key storeapi.RequestKey,
) (storeapi.Request, error) {
	return store.beginResponse(ctx, key)
}

func (store scriptedResponseStore) FailRequest(
	ctx context.Context,
	key storeapi.RequestKey,
	expected completion.State,
	decision storeapi.ResponseDecision,
) (storeapi.Request, error) {
	return store.failRequest(ctx, key, expected, decision)
}

func TestResponseNotifierBroadcastsOnlyToMatchingLiveSubscribers(t *testing.T) {
	t.Parallel()
	var notifier responseNotifier
	firstKey := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "first"}
	secondKey := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "second"}
	firstA, cancelFirstA := notifier.subscribe(firstKey)
	defer cancelFirstA()
	firstB, cancelFirstB := notifier.subscribe(firstKey)
	defer cancelFirstB()
	second, cancelSecond := notifier.subscribe(secondKey)
	defer cancelSecond()

	notifier.notify(firstKey)
	for name, wakeup := range map[string]<-chan struct{}{"first A": firstA, "first B": firstB} {
		select {
		case <-wakeup:
		default:
			t.Fatalf("%s subscriber was not notified", name)
		}
	}
	select {
	case <-second:
		t.Fatal("unrelated response subscriber was notified")
	default:
	}
}

func TestResponseNotifierCleanupIsIdempotentAndDoesNotRetainSignals(t *testing.T) {
	t.Parallel()
	var notifier responseNotifier
	key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "request"}
	wakeup, cancel := notifier.subscribe(key)
	cancel()
	cancel()
	if len(notifier.waiters) != 0 {
		t.Fatalf("cleaned notifier retained waiters: %#v", notifier.waiters)
	}
	notifier.notify(key)
	select {
	case <-wakeup:
		t.Fatal("cleaned subscriber received a later signal")
	default:
	}
}

func TestResponseNotifierBuffersCommitBetweenSubscribeAndRead(t *testing.T) {
	t.Parallel()
	var notifier responseNotifier
	key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "request"}
	wakeup, cancel := notifier.subscribe(key)
	defer cancel()
	notifier.notify(key)
	select {
	case <-wakeup:
	default:
		t.Fatal("commit signal was lost before subscriber began waiting")
	}
}

func TestAwaitResponseDecisionSleepsUntilNotification(t *testing.T) {
	t.Parallel()
	key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "request"}
	var mu sync.Mutex
	status := 0
	reads := 0
	firstRead := make(chan struct{}, 1)
	server := &Server{}
	server.store = scriptedResponseStore{read: func(
		_ context.Context,
		gotKey storeapi.RequestKey,
		_ int64,
	) (storeapi.ResponseRead, error) {
		mu.Lock()
		defer mu.Unlock()
		reads++
		if reads == 1 {
			firstRead <- struct{}{}
		}
		return storeapi.ResponseRead{
			RequestKey: gotKey,
			Response:   storeapi.ResponseDecision{StatusCode: status, ContentType: "text/event-stream"},
		}, nil
	}}
	lookup := storeapi.BeginRequestResult{Request: storeapi.Request{RequestKey: key}}
	type result struct {
		lookup storeapi.BeginRequestResult
		err    error
	}
	completed := make(chan result, 1)
	go func() {
		decided, err := server.awaitResponseDecision(context.Background(), lookup)
		completed <- result{lookup: decided, err: err}
	}()
	select {
	case <-firstRead:
	case <-time.After(time.Second):
		t.Fatal("response waiter did not read its initial snapshot")
	}
	mu.Lock()
	status = 200
	mu.Unlock()
	server.responses.notify(key)
	select {
	case result := <-completed:
		if result.err != nil || result.lookup.Request.Response.StatusCode != 200 {
			t.Fatalf("notified decision = %+v, %v", result.lookup, result.err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("durable response notification did not wake the waiter")
	}
	mu.Lock()
	defer mu.Unlock()
	if reads != 2 {
		t.Fatalf("response decision reads = %d, want 2", reads)
	}
}

func TestAwaitResponseDecisionDoesNotPollWhileIdle(t *testing.T) {
	t.Parallel()
	key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "idle"}
	reads := 0
	server := &Server{store: scriptedResponseStore{read: func(
		_ context.Context,
		gotKey storeapi.RequestKey,
		_ int64,
	) (storeapi.ResponseRead, error) {
		reads++
		return storeapi.ResponseRead{RequestKey: gotKey}, nil
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := server.awaitResponseDecision(ctx, storeapi.BeginRequestResult{
		Request: storeapi.Request{RequestKey: key},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("idle response wait error = %v", err)
	}
	if reads != 1 {
		t.Fatalf("idle response wait performed %d reads, want 1", reads)
	}
}

func TestAwaitResponseDecisionStopsWhenGatewayRuntimeStops(t *testing.T) {
	t.Parallel()
	key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "shutdown"}
	read := make(chan struct{}, 1)
	runtimeContext, cancelRuntime := context.WithCancel(context.Background())
	server := &Server{
		runContext: runtimeContext,
		store: scriptedResponseStore{read: func(
			_ context.Context,
			gotKey storeapi.RequestKey,
			_ int64,
		) (storeapi.ResponseRead, error) {
			read <- struct{}{}
			return storeapi.ResponseRead{RequestKey: gotKey}, nil
		}},
	}
	completed := make(chan error, 1)
	go func() {
		_, err := server.awaitResponseDecision(context.Background(), storeapi.BeginRequestResult{
			Request: storeapi.Request{RequestKey: key},
		})
		completed <- err
	}()
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Fatal("response waiter did not start")
	}
	cancelRuntime()
	select {
	case err := <-completed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime cancellation error = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("response waiter survived gateway runtime cancellation")
	}
}

func TestBeginResponseAcceptsDurableDecisionAfterAmbiguousStoreError(t *testing.T) {
	t.Parallel()
	key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "begin-ambiguous"}
	original := storeapi.Request{
		RequestKey: key,
		CanonicalRequest: canonical.Request{
			Dialect: canonical.DialectOpenAIChat, Model: "human-expert", Stream: true,
		},
	}
	server := &Server{store: scriptedResponseStore{
		beginResponse: func(context.Context, storeapi.RequestKey) (storeapi.Request, error) {
			return storeapi.Request{}, errors.New("injected error after commit")
		},
		read: func(context.Context, storeapi.RequestKey, int64) (storeapi.ResponseRead, error) {
			return storeapi.ResponseRead{
				RequestKey: key,
				Response: storeapi.ResponseDecision{
					StatusCode: http.StatusOK, ContentType: "text/event-stream",
				},
			}, nil
		},
	}}
	decided, err := server.beginResponse(context.Background(), original)
	if err != nil || decided.Response.StatusCode != http.StatusOK ||
		decided.CanonicalRequest.Model != original.CanonicalRequest.Model {
		t.Fatalf("ambiguous BeginResponse() = %+v, %v", decided, err)
	}
}

func TestFailRequestAcceptsExactDurableDecisionAfterAmbiguousStoreError(t *testing.T) {
	t.Parallel()
	key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "fail-ambiguous"}
	decision := storeapi.ResponseDecision{
		StatusCode: http.StatusInternalServerError, ContentType: "application/json",
		Body: []byte(`{"error":"durable"}`),
	}
	server := &Server{store: scriptedResponseStore{
		failRequest: func(
			context.Context, storeapi.RequestKey, completion.State, storeapi.ResponseDecision,
		) (storeapi.Request, error) {
			return storeapi.Request{}, errors.New("injected error after commit")
		},
		read: func(context.Context, storeapi.RequestKey, int64) (storeapi.ResponseRead, error) {
			return storeapi.ResponseRead{
				RequestKey: key, Response: decision, ResponseComplete: true,
			}, nil
		},
	}}
	failed, err := server.failRequest(context.Background(), key, completion.StateAdmitted, decision)
	if err != nil || !failed.ResponseComplete || !responseDecisionsEqual(failed.Response, decision) {
		t.Fatalf("ambiguous FailRequest() = %+v, %v", failed, err)
	}
}

func TestServerWaitTracksCompletionBackgroundWork(t *testing.T) {
	t.Parallel()
	server := &Server{}
	started := make(chan struct{})
	release := make(chan struct{})
	server.startBackground(func() {
		close(started)
		<-release
	})
	<-started
	waited := make(chan struct{})
	go func() {
		server.Wait()
		close(waited)
	}()
	select {
	case <-waited:
		t.Fatal("Server.Wait returned while background work was live")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("Server.Wait did not return after background work stopped")
	}
}

func TestCompleteResponseSnapshotWritesTerminalFrameBeforeReturning(t *testing.T) {
	t.Parallel()
	key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "complete"}
	payload, err := json.Marshal(persistedStep{Wire: []byte("terminal-wire")})
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{store: scriptedResponseStore{read: func(
		_ context.Context,
		gotKey storeapi.RequestKey,
		_ int64,
	) (storeapi.ResponseRead, error) {
		return storeapi.ResponseRead{
			RequestKey:       gotKey,
			ResponseComplete: true,
			Events: []storeapi.ResponseEvent{{
				RequestKey: gotKey, Sequence: 7, Kind: responseEventApplied, Data: payload,
			}},
		}, nil
	}}}
	request := httptest.NewRequest("GET", "/", nil)
	recorder := httptest.NewRecorder()
	server.continueStreamingResponse(
		recorder,
		request,
		storeapi.BeginRequestResult{Request: storeapi.Request{RequestKey: key}},
		streamReplayCursor{flush: http.NewResponseController(recorder).Flush, started: true},
	)
	if recorder.Body.String() != "terminal-wire" {
		t.Fatalf("complete response omitted terminal frame: %q", recorder.Body.String())
	}
}

func TestContinueStreamingResponseStopsOnWriteOrFlushFailure(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		writeError bool
	}{
		{name: "write", writeError: true},
		{name: "flush"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: test.name}
			reads := 0
			server := &Server{store: scriptedResponseStore{read: func(
				context.Context, storeapi.RequestKey, int64,
			) (storeapi.ResponseRead, error) {
				reads++
				return storeapi.ResponseRead{Events: []storeapi.ResponseEvent{{
					RequestKey: key, Sequence: 1, Kind: responseEventWire, Data: []byte("wire"),
				}}}, nil
			}}}
			base := httptest.NewRecorder()
			var response http.ResponseWriter = base
			if test.writeError {
				response = &failingWriteRecorder{
					ResponseRecorder: base, err: errors.New("injected write failure"),
				}
			}
			flushes := 0
			server.continueStreamingResponse(
				response,
				httptest.NewRequest(http.MethodGet, "/", nil),
				storeapi.BeginRequestResult{Request: storeapi.Request{RequestKey: key}},
				streamReplayCursor{started: true, flush: func() error {
					flushes++
					if !test.writeError {
						return errors.New("injected flush failure")
					}
					return nil
				}},
			)
			if reads != 1 {
				t.Fatalf("response reads after %s failure = %d, want 1", test.name, reads)
			}
			if test.writeError && flushes != 0 {
				t.Fatalf("flush called %d times after write failure", flushes)
			}
			if !test.writeError && flushes != 1 {
				t.Fatalf("flush calls = %d, want 1", flushes)
			}
		})
	}
}

func TestBeginStreamingReplayReturnsStartedCursorOnWriteFailure(t *testing.T) {
	t.Parallel()
	key := storeapi.RequestKey{CallerID: "caller", IdempotencyKey: "replay-write"}
	server := &Server{store: scriptedResponseStore{read: func(
		context.Context, storeapi.RequestKey, int64,
	) (storeapi.ResponseRead, error) {
		return storeapi.ResponseRead{
			RequestKey: key,
			Response: storeapi.ResponseDecision{
				StatusCode: http.StatusOK, ContentType: "text/event-stream",
			},
			Events: []storeapi.ResponseEvent{{
				RequestKey: key, Sequence: 1, Kind: responseEventWire, Data: []byte("wire"),
			}},
		}, nil
	}}}
	response := &failingWriteRecorder{
		ResponseRecorder: httptest.NewRecorder(), err: errors.New("injected write failure"),
	}
	cursor, err := server.beginStreamingResponse(context.Background(), response, storeapi.BeginRequestResult{
		Task: storeapi.Task{TaskKey: storeapi.TaskKey{CallerID: "caller", TaskID: "task"}},
		Request: storeapi.Request{
			RequestKey: key, Response: storeapi.ResponseDecision{StatusCode: http.StatusOK},
		},
	})
	if err == nil || !cursor.started {
		t.Fatalf("begin replay write failure = cursor %+v, error %v", cursor, err)
	}
}
