package gateway

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	storeapi "github.com/vibe-agi/human/internal/store"
)

const (
	responseWaitFallback       = 5 * time.Second
	durableDecisionReadTimeout = 500 * time.Millisecond
)

// responseNotifier wakes HTTP handlers after this process commits response
// state. It is deliberately only a latency optimization: subscribers always
// re-read the durable Store after waking, and callers retain a low-frequency
// timer fallback for a missed in-process notification or a future commit made
// by another process. Crash continuity comes from durable replay and recovery.
type responseNotifier struct {
	mu      sync.Mutex
	waiters map[storeapi.RequestKey]map[chan struct{}]struct{}
}

// subscribe must be called before reading the Store. That ordering closes the
// lost-wakeup window: a commit between subscription and the read leaves a
// buffered signal, while a commit before subscription is visible in the read.
func (notifier *responseNotifier) subscribe(key storeapi.RequestKey) (<-chan struct{}, func()) {
	wakeup := make(chan struct{}, 1)
	notifier.mu.Lock()
	if notifier.waiters == nil {
		notifier.waiters = make(map[storeapi.RequestKey]map[chan struct{}]struct{})
	}
	group := notifier.waiters[key]
	if group == nil {
		group = make(map[chan struct{}]struct{})
		notifier.waiters[key] = group
	}
	group[wakeup] = struct{}{}
	notifier.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			notifier.mu.Lock()
			if group := notifier.waiters[key]; group != nil {
				delete(group, wakeup)
				if len(group) == 0 {
					delete(notifier.waiters, key)
				}
			}
			notifier.mu.Unlock()
		})
	}
	return wakeup, cancel
}

func (notifier *responseNotifier) notify(key storeapi.RequestKey) {
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	for wakeup := range notifier.waiters[key] {
		select {
		case wakeup <- struct{}{}:
		default:
		}
	}
}

func waitForResponseChange(ctx context.Context, wakeup <-chan struct{}) error {
	timer := time.NewTimer(responseWaitFallback)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wakeup:
		return nil
	case <-timer.C:
		return nil
	}
}

func (server *Server) responseReadContext(requestContext context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(requestContext)
	runtimeContext := server.runtimeContext()
	stopRuntimeCancel := context.AfterFunc(runtimeContext, cancel)
	if runtimeContext.Err() != nil {
		cancel()
	}
	return ctx, func() {
		stopRuntimeCancel()
		cancel()
	}
}

// readDurableDecisionAfterError resolves the ambiguous edge shared by context
// cancellation and Store methods whose transaction may have committed before
// returning an error. It intentionally uses a fresh, short-lived context: the
// request or daemon context which failed is no longer a useful read oracle.
func (server *Server) readDurableDecisionAfterError(
	key storeapi.RequestKey,
) (storeapi.ResponseRead, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), durableDecisionReadTimeout)
	defer cancel()
	read, err := server.store.ReadResponse(ctx, key, math.MaxInt64)
	if err != nil || read.PayloadPruned || read.Response.StatusCode == 0 {
		return storeapi.ResponseRead{}, false
	}
	return read, true
}

func (server *Server) appendResponseEvent(
	ctx context.Context,
	key storeapi.RequestKey,
	kind string,
	data []byte,
) (storeapi.ResponseEvent, error) {
	event, err := server.store.AppendResponseEvent(ctx, key, kind, data)
	if err == nil {
		server.responses.notify(key)
	}
	return event, err
}

func (server *Server) appendWorkerResponseEvent(
	ctx context.Context,
	key storeapi.RequestKey,
	kind string,
	eventID string,
	eventDigest string,
	data []byte,
) (storeapi.ResponseEvent, error) {
	event, err := server.store.AppendWorkerResponseEvent(ctx, key, kind, eventID, eventDigest, data)
	if err == nil {
		server.responses.notify(key)
	}
	return event, err
}

func (server *Server) beginResponse(
	ctx context.Context,
	request storeapi.Request,
) (storeapi.Request, error) {
	stored, err := server.store.BeginResponse(ctx, request.RequestKey)
	if err != nil {
		if read, ok := server.readDurableDecisionAfterError(request.RequestKey); ok &&
			read.Response.StatusCode == 200 && read.Response.ContentType == "text/event-stream" {
			request.Response = read.Response
			request.ResponseComplete = read.ResponseComplete
			server.responses.notify(request.RequestKey)
			return request, nil
		}
		return storeapi.Request{}, err
	}
	request = stored
	server.responses.notify(request.RequestKey)
	return request, nil
}

func (server *Server) completeNonStreamingResponse(
	ctx context.Context,
	key storeapi.RequestKey,
	decision storeapi.ResponseDecision,
) (storeapi.Request, error) {
	request, err := server.store.CompleteNonStreamingResponse(ctx, key, decision)
	if err == nil {
		server.responses.notify(key)
	}
	return request, err
}

func (server *Server) completeRequest(ctx context.Context, key storeapi.RequestKey) error {
	err := server.store.CompleteRequest(ctx, key)
	if err == nil {
		server.responses.notify(key)
	}
	return err
}

func (server *Server) failRequest(
	ctx context.Context,
	key storeapi.RequestKey,
	expected completion.State,
	decision storeapi.ResponseDecision,
) (storeapi.Request, error) {
	request, err := server.store.FailRequest(ctx, key, expected, decision)
	if err != nil {
		if read, ok := server.readDurableDecisionAfterError(key); ok &&
			read.ResponseComplete && responseDecisionsEqual(read.Response, decision) {
			server.responses.notify(key)
			return storeapi.Request{
				RequestKey: key, Response: read.Response, ResponseComplete: true,
			}, nil
		}
		return storeapi.Request{}, err
	}
	server.responses.notify(key)
	return request, nil
}
