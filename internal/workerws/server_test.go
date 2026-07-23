package workerws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/hub"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/workerproto"
)

type workerAuthenticator struct{}

type multipleWorkerAuthenticator struct{}

type reservedWorkerAuthenticator struct{}

type receiptOracle struct {
	eventID  string
	workerID string
	digest   string
}

func workerHeaders(token string, instanceIDs ...string) http.Header {
	instanceID := "test-worker-instance"
	if len(instanceIDs) > 0 {
		instanceID = instanceIDs[0]
	}
	return http.Header{
		"Authorization":                  []string{"Bearer " + token},
		workerproto.WorkerInstanceHeader: []string{instanceID},
	}
}

func (oracle receiptOracle) RecordWorkerEventReceipt(
	context.Context, storeapi.RequestKey, string, string, string,
) (storeapi.WorkerEventReceipt, error) {
	return storeapi.WorkerEventReceipt{}, errors.New("receipt oracle is read-only")
}

func (oracle receiptOracle) LookupWorkerEventReceipt(
	_ context.Context,
	key storeapi.RequestKey,
	eventID string,
) (storeapi.WorkerEventReceipt, error) {
	if eventID != oracle.eventID {
		return storeapi.WorkerEventReceipt{}, storeapi.ErrNotFound
	}
	return storeapi.WorkerEventReceipt{
		RequestKey: key, EventID: eventID, WorkerID: oracle.workerID, Digest: oracle.digest,
	}, nil
}

func (workerAuthenticator) AuthenticateRequest(request *http.Request) (auth.Principal, error) {
	token := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	if token != "hae_worker" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return auth.Principal{Type: auth.PrincipalWorker, SubjectID: "worker-1", KeyID: "key-worker"}, nil
}

func (multipleWorkerAuthenticator) AuthenticateRequest(request *http.Request) (auth.Principal, error) {
	token := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	subjects := map[string]string{
		"hae_owner":    "worker-owner",
		"hae_intruder": "worker-intruder",
	}
	subject, ok := subjects[token]
	if !ok {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return auth.Principal{
		Type: auth.PrincipalWorker, SubjectID: subject, KeyID: "key-" + subject,
	}, nil
}

func (reservedWorkerAuthenticator) AuthenticateRequest(*http.Request) (auth.Principal, error) {
	return auth.Principal{
		Type: auth.PrincipalWorker, SubjectID: workerproto.GatewayEventOwner, KeyID: "reserved",
	}, nil
}

func TestWorkerWebSocketAssignmentAndEvents(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(2)
	server, err := New(Config{}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), &websocket.DialOptions{
		HTTPHeader: workerHeaders("hae_worker"),
	})
	if err != nil {
		if response != nil {
			t.Fatalf("Dial() status = %d, error = %v", response.StatusCode, err)
		}
		t.Fatal(err)
	}
	defer connection.CloseNow()
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &hello); err != nil {
		t.Fatal(err)
	}
	if hello.Type != workerproto.MessageHello || hello.Seq != 1 {
		t.Fatalf("hello = %+v", hello)
	}
	reservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{CallerID: "caller", TaskID: "task", IdempotencyKey: "request"})
	if err != nil {
		t.Fatal(err)
	}
	var assignment workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &assignment); err != nil {
		t.Fatal(err)
	}
	if assignment.Type != workerproto.MessageAssignment || assignment.Seq != 2 {
		t.Fatalf("assignment = %+v", assignment)
	}
	accepted, err := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "request",
		Event: completion.Event{ID: "accepted-1", Type: completion.EventAccepted, WorkerID: "worker-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted.Seq = 1
	if err := wsjson.Write(ctx, connection, accepted); err != nil {
		t.Fatal(err)
	}
	if event := <-events; event.Type != completion.EventAccepted {
		t.Fatalf("accepted event = %+v", event)
	} else {
		event.Commit(nil)
	}
	var ack workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Type != workerproto.MessageAck || ack.Ack != 1 {
		t.Fatalf("ack = %+v", ack)
	}
	final, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "request",
		Event: completion.Event{ID: "final-1", Type: completion.EventFinal, Text: "done"},
	})
	final.Seq = 2
	if err := wsjson.Write(ctx, connection, final); err != nil {
		t.Fatal(err)
	}
	if event := <-events; event.Type != completion.EventFinal {
		t.Fatalf("final event = %+v", event)
	} else {
		event.Commit(nil)
	}
}

func TestWorkerWebSocketRequiresWorkerToken(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(1)
	server, err := New(Config{}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
	if err == nil {
		t.Fatal("unauthenticated worker connected")
	}
	if response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("response = %#v, error = %v", response, err)
	}
}

func TestWorkerWebSocketRejectsReservedGatewayReceiptOwner(t *testing.T) {
	t.Parallel()
	server, err := New(Config{}, reservedWorkerAuthenticator{}, hub.New(1))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, response, err := websocket.Dial(
		ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"),
		&websocket.DialOptions{HTTPHeader: workerHeaders("reserved")},
	)
	if connection != nil {
		_ = connection.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("reserved worker owner dial err = %v, response = %#v", err, response)
	}
	_ = response.Body.Close()
}

func TestWorkerWebSocketRequiresStableInstanceIdentity(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(1)
	server, err := New(Config{}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer hae_worker"}},
	})
	if err == nil {
		t.Fatal("worker without an instance id connected")
	}
	if response == nil || response.StatusCode != http.StatusPreconditionRequired {
		t.Fatalf("response = %#v, error = %v", response, err)
	}
}

func TestWorkerSequenceGapClosesConnection(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(1)
	server, err := New(Config{}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization":                  []string{"Bearer hae_worker"},
			workerproto.WorkerInstanceHeader: []string{"worker-rejection-test"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &hello); err != nil {
		t.Fatal(err)
	}
	gap, _ := workerproto.NewEnvelope(workerproto.MessageAck, nil)
	gap.Seq = 2
	if err := wsjson.Write(ctx, connection, gap); err != nil {
		t.Fatal(err)
	}
	var reply workerproto.Envelope
	err = wsjson.Read(ctx, connection, &reply)
	closed := websocket.CloseStatus(err) != -1 || errors.Is(err, io.EOF)
	if err == nil || errors.Is(err, context.DeadlineExceeded) || !closed {
		t.Fatalf("sequence gap did not close connection: %v", err)
	}
}

func TestReadLoopOutboundBackpressureStopsOnCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	outbound := newOutboundQueue(1)
	if !outbound.enqueue(ctx, workerproto.Envelope{Type: workerproto.MessageAck}, 0) {
		t.Fatal("failed to fill outbound queue")
	}
	returned := make(chan bool, 1)
	go func() {
		returned <- outbound.enqueue(ctx, workerproto.Envelope{Type: workerproto.MessageAck}, 0)
	}()

	select {
	case <-returned:
		t.Fatal("full outbound queue did not apply backpressure")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case sent := <-returned:
		if sent {
			t.Fatal("canceled outbound send reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("read-loop outbound send leaked after cancellation")
	}
}

func TestWriterFailureCancelsFullOutboundAndUnregistersWorker(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(128)
	server, err := New(Config{PingInterval: time.Hour}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	outboundReady := make(chan *outboundQueue, 1)
	server.observeOutbound = func(outbound *outboundQueue) {
		outboundReady <- outbound
	}
	writerBlocked := make(chan struct{})
	failWriter := make(chan struct{})
	server.writeEnvelope = func(
		ctx context.Context,
		connection *websocket.Conn,
		envelope workerproto.Envelope,
	) error {
		if envelope.Type == workerproto.MessageHello {
			return wsjson.Write(ctx, connection, envelope)
		}
		select {
		case <-writerBlocked:
			return errors.New("writer reached a second assignment while the first was blocked")
		default:
			close(writerBlocked)
		}
		select {
		case <-failWriter:
			return errors.New("injected worker socket write failure")
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(
		ctx,
		"ws"+strings.TrimPrefix(httpServer.URL, "http"),
		&websocket.DialOptions{HTTPHeader: workerHeaders("hae_worker")},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &hello); err != nil {
		t.Fatal(err)
	}
	outbound := <-outboundReady

	enqueue := func(id string) {
		t.Helper()
		reservation, reserveErr := workerHub.Reserve("worker-1")
		if reserveErr != nil {
			t.Fatal(reserveErr)
		}
		if _, enqueueErr := reservation.Enqueue(completion.Assignment{
			CallerID: "caller", TaskID: "task-" + id, IdempotencyKey: "request-" + id,
		}); enqueueErr != nil {
			t.Fatal(enqueueErr)
		}
	}

	enqueue("writer")
	select {
	case <-writerBlocked:
	case <-ctx.Done():
		t.Fatal("writer did not block on the first assignment")
	}
	for index := 0; index < cap(outbound.frames); index++ {
		enqueue(fmt.Sprintf("queued-%d", index))
	}
	deadline := time.Now().Add(time.Second)
	for len(outbound.frames) != cap(outbound.frames) {
		if time.Now().After(deadline) {
			t.Fatalf("outbound queue did not fill: len=%d cap=%d", len(outbound.frames), cap(outbound.frames))
		}
		time.Sleep(time.Millisecond)
	}

	// One more assignment makes the handler itself block in enqueue while it
	// holds queue.mu. A writer error must cancel that enqueue directly; relying
	// on the handler to receive errorsChannel would deadlock here.
	enqueue("blocked")
	deadline = time.Now().Add(time.Second)
	for outbound.mu.TryLock() {
		outbound.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("handler did not block behind the full outbound queue")
		}
		time.Sleep(time.Millisecond)
	}
	close(failWriter)

	deadline = time.Now().Add(time.Second)
	for {
		probe, reserveErr := workerHub.Reserve("")
		if errors.Is(reserveErr, hub.ErrNoWorker) {
			break
		}
		if reserveErr != nil {
			t.Fatalf("reserve after writer failure: %v", reserveErr)
		}
		probe.Release()
		if time.Now().After(deadline) {
			t.Fatal("failed writer left its worker registered")
		}
		time.Sleep(time.Millisecond)
	}
	if !outbound.mu.TryLock() {
		t.Fatal("canceled outbound enqueue retained queue lock")
	}
	outbound.mu.Unlock()

	for index := 0; index < cap(outbound.frames); index++ {
		_ = workerHub.Abort("caller", fmt.Sprintf("request-queued-%d", index))
	}
	_ = workerHub.Abort("caller", "request-writer")
	_ = workerHub.Abort("caller", "request-blocked")
}

func TestBacklogFrameCannotAcknowledgeRejectionBeforeRejectionFrame(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbound := newOutboundQueue(1)
	var lastCommitted atomic.Uint64
	lastCommitted.Store(4)
	backlog, _ := workerproto.NewEnvelope(workerproto.MessageAssignment, completion.Assignment{TaskID: "backlog"})
	if !outbound.enqueue(ctx, backlog, lastCommitted.Load()) {
		t.Fatal("enqueue backlog assignment")
	}
	rejected, _ := workerproto.NewEnvelope(workerproto.MessageEventRejected, workerproto.EventRejected{
		CallerID: "caller", IdempotencyKey: "expired", EventID: "late",
	})
	queued := make(chan bool, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		queued <- enqueueRejection(ctx, outbound, rejected, 5, &lastCommitted)
	}()
	<-started
	deadline := time.Now().Add(time.Second)
	for outbound.mu.TryLock() {
		outbound.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("rejection sender did not block on the full outbound queue")
		}
		time.Sleep(time.Millisecond)
	}

	// The full queue keeps rejection behind the old assignment. Its ACK must
	// not become globally visible while that semantic frame is still blocked.
	select {
	case <-queued:
		t.Fatal("rejection unexpectedly bypassed backlog")
	default:
	}
	if got := lastCommitted.Load(); got != 4 {
		t.Fatalf("committed watermark before rejection enqueue = %d, want 4", got)
	}
	first := <-outbound.frames
	if first.Type != workerproto.MessageAssignment || first.Ack != 4 {
		t.Fatalf("backlog frame = %+v, want assignment with old ACK 4", first)
	}
	select {
	case ok := <-queued:
		if !ok {
			t.Fatal("rejection enqueue failed")
		}
	case <-time.After(time.Second):
		t.Fatal("rejection did not enqueue after backlog drained")
	}
	if got := lastCommitted.Load(); got != 5 {
		t.Fatalf("committed watermark after rejection enqueue = %d, want 5", got)
	}
	second := <-outbound.frames
	if second.Type != workerproto.MessageEventRejected || second.Ack != 5 {
		t.Fatalf("rejection frame = %+v, want event_rejected with ACK 5", second)
	}
	var rejection workerproto.EventRejected
	if err := json.Unmarshal(second.Payload, &rejection); err != nil {
		t.Fatal(err)
	}
	if rejection.EventID != "late" {
		t.Fatalf("rejection payload = %+v", rejection)
	}

	// A producer holding an older concurrent snapshot may enqueue later. The
	// queue promotes it to the established watermark instead of regressing ACK.
	later, _ := workerproto.NewEnvelope(workerproto.MessageAssignment, completion.Assignment{TaskID: "later"})
	if !outbound.enqueue(ctx, later, 4) {
		t.Fatal("enqueue later assignment")
	}
	third := <-outbound.frames
	if third.Ack != 5 {
		t.Fatalf("later stale snapshot ACK = %d, want monotonic 5", third.Ack)
	}
}

func TestWorkerEventIsNotAcknowledgedBeforeDurableCommit(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(3)
	server, err := New(Config{}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), &websocket.DialOptions{
		HTTPHeader: workerHeaders("hae_worker"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &hello); err != nil {
		t.Fatal(err)
	}
	firstReservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	firstEvents, err := firstReservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task-one", IdempotencyKey: "request-one",
	})
	if err != nil {
		t.Fatal(err)
	}
	var firstAssignment workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &firstAssignment); err != nil {
		t.Fatal(err)
	}
	eventEnvelope, err := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "request-one",
		Event: completion.Event{ID: "event-durable-ack", Type: completion.EventAccepted, WorkerID: "worker-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	eventEnvelope.Seq = 1
	if err := wsjson.Write(ctx, connection, eventEnvelope); err != nil {
		t.Fatal(err)
	}
	var delivery *hub.Delivery
	select {
	case delivery = <-firstEvents:
	case <-ctx.Done():
		t.Fatal("event did not reach durable processor")
	}

	// Force an unrelated outbound frame while Publish is blocked on Commit.
	// Its cumulative ACK must stay at zero until the event is durable.
	secondReservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secondReservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task-two", IdempotencyKey: "request-two",
	}); err != nil {
		t.Fatal(err)
	}
	var unrelated workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &unrelated); err != nil {
		t.Fatal(err)
	}
	if unrelated.Type != workerproto.MessageAssignment || unrelated.Ack != 0 {
		t.Fatalf("pre-commit outbound frame = %+v", unrelated)
	}
	delivery.Commit(nil)
	var ack workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Type != workerproto.MessageAck || ack.Ack != eventEnvelope.Seq {
		t.Fatalf("post-commit ACK = %+v", ack)
	}
}

func TestDeterministicallyRejectedEventDoesNotPoisonConnection(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(2)
	server, err := New(Config{}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization":                  []string{"Bearer hae_worker"},
			workerproto.WorkerInstanceHeader: []string{"worker-rejection-test"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &hello); err != nil {
		t.Fatal(err)
	}

	badReservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	badEvents, err := badReservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "bad-task", IdempotencyKey: "bad-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	var badAssignment workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &badAssignment); err != nil {
		t.Fatal(err)
	}
	bad, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "bad-request",
		Event: completion.Event{ID: "bad-event", Type: completion.EventToolCalls},
	})
	bad.Seq = 1
	if err := wsjson.Write(ctx, connection, bad); err != nil {
		t.Fatal(err)
	}
	delivery := <-badEvents
	delivery.Commit(hub.RejectEvent(errors.New("tool call is not declared")))
	var rejected workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &rejected); err != nil {
		t.Fatal(err)
	}
	if rejected.Type != workerproto.MessageEventRejected || rejected.Ack != bad.Seq {
		t.Fatalf("rejected frame = %+v", rejected)
	}
	var rejection workerproto.EventRejected
	if err := json.Unmarshal(rejected.Payload, &rejection); err != nil {
		t.Fatal(err)
	}
	if rejection.EventID != "bad-event" {
		t.Fatalf("rejection = %+v", rejection)
	}
	if err := workerHub.Abort("caller", "bad-request"); err != nil {
		t.Fatal(err)
	}

	goodReservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatalf("rejected event kept capacity: %v", err)
	}
	goodEvents, err := goodReservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "good-task", IdempotencyKey: "good-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	var goodAssignment workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &goodAssignment); err != nil {
		t.Fatalf("connection died after event rejection: %v", err)
	}
	good, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "good-request",
		Event: completion.Event{ID: "good-event", Type: completion.EventAccepted},
	})
	good.Seq = 2
	if err := wsjson.Write(ctx, connection, good); err != nil {
		t.Fatal(err)
	}
	goodDelivery := <-goodEvents
	goodDelivery.Commit(nil)
	var ack workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Type != workerproto.MessageAck || ack.Ack != good.Seq {
		t.Fatalf("healthy event ACK = %+v", ack)
	}
	_ = workerHub.Abort("caller", "good-request")
}

func TestCommittedEventReplayIsAcknowledgedWithoutLiveHubSession(t *testing.T) {
	t.Parallel()
	event := completion.Event{
		ID: "event-from-outbox", Type: completion.EventFinal,
		WorkerID: "worker-1", Text: "already durable",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	workerHub := hub.New(1)
	server, err := New(Config{}, workerAuthenticator{}, workerHub, receiptOracle{
		eventID: event.ID, workerID: "worker-1", digest: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), &websocket.DialOptions{
		HTTPHeader: workerHeaders("hae_worker"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &hello); err != nil {
		t.Fatal(err)
	}
	replay, err := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "completed-request", Event: event,
	})
	if err != nil {
		t.Fatal(err)
	}
	replay.Seq = 1
	if err := wsjson.Write(ctx, connection, replay); err != nil {
		t.Fatal(err)
	}
	var ack workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Type != workerproto.MessageAck || ack.Ack != replay.Seq {
		t.Fatalf("durable replay ACK = %+v", ack)
	}
	changed := event
	changed.Text = "conflicting payload"
	conflict, err := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "completed-request", Event: changed,
	})
	if err != nil {
		t.Fatal(err)
	}
	conflict.Seq = 2
	if err := wsjson.Write(ctx, connection, conflict); err != nil {
		t.Fatal(err)
	}
	var rejected workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &rejected); err != nil {
		t.Fatal(err)
	}
	if rejected.Type != workerproto.MessageEventRejected || rejected.Ack != conflict.Seq {
		t.Fatalf("conflicting durable event rejection = %+v", rejected)
	}
	var rejection workerproto.EventRejected
	if err := json.Unmarshal(rejected.Payload, &rejection); err != nil {
		t.Fatal(err)
	}
	if rejection.EventID != event.ID {
		t.Fatalf("conflicting durable event rejection payload = %+v", rejection)
	}

	// A conflicting receipt is a rejection of this one outbox record, not a
	// connection failure. Prove a later independent live event still commits on
	// the same socket instead of waiting behind a reconnecting poison pill.
	reservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "live-after-receipt-conflict",
		IdempotencyKey: "live-after-receipt-conflict",
	})
	if err != nil {
		t.Fatal(err)
	}
	var assignment workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &assignment); err != nil {
		t.Fatalf("same connection did not receive later assignment: %v", err)
	}
	live, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "live-after-receipt-conflict",
		Event: completion.Event{ID: "live-accepted", Type: completion.EventAccepted},
	})
	live.Seq = 3
	if err := wsjson.Write(ctx, connection, live); err != nil {
		t.Fatal(err)
	}
	delivery := <-events
	delivery.Commit(nil)
	if err := wsjson.Read(ctx, connection, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Type != workerproto.MessageAck || ack.Ack != live.Seq {
		t.Fatalf("later live event ACK = %+v", ack)
	}
	_ = workerHub.Abort("caller", "live-after-receipt-conflict")
}

func TestRetiredEventConflictIsRejectedWithoutPoisoningConnection(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(2)
	server, err := New(Config{}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(
		ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"),
		&websocket.DialOptions{HTTPHeader: workerHeaders("hae_worker")},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &hello); err != nil {
		t.Fatal(err)
	}

	reservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "terminal-task", IdempotencyKey: "terminal-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	var assignment workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &assignment); err != nil {
		t.Fatal(err)
	}
	terminalEvent := completion.Event{ID: "terminal-event", Type: completion.EventFinal, Text: "original"}
	terminal, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "terminal-request", Event: terminalEvent,
	})
	terminal.Seq = 1
	if err := wsjson.Write(ctx, connection, terminal); err != nil {
		t.Fatal(err)
	}
	delivery := <-events
	delivery.Commit(nil)
	var ack workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &ack); err != nil {
		t.Fatal(err)
	}

	changed := terminalEvent
	changed.Text = "conflicting retry"
	conflict, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "terminal-request", Event: changed,
	})
	conflict.Seq = 2
	if err := wsjson.Write(ctx, connection, conflict); err != nil {
		t.Fatal(err)
	}
	var rejected workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &rejected); err != nil {
		t.Fatal(err)
	}
	if rejected.Type != workerproto.MessageEventRejected || rejected.Ack != conflict.Seq {
		t.Fatalf("retired conflict rejection = %+v", rejected)
	}
	var rejection workerproto.EventRejected
	if err := json.Unmarshal(rejected.Payload, &rejection); err != nil {
		t.Fatal(err)
	}
	if rejection.EventID != terminalEvent.ID {
		t.Fatalf("retired conflict payload = %+v", rejection)
	}

	liveReservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	liveEvents, err := liveReservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "later-task", IdempotencyKey: "later-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := wsjson.Read(ctx, connection, &assignment); err != nil {
		t.Fatalf("retired conflict killed connection: %v", err)
	}
	live, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "later-request",
		Event: completion.Event{ID: "later-accepted", Type: completion.EventAccepted},
	})
	live.Seq = 3
	if err := wsjson.Write(ctx, connection, live); err != nil {
		t.Fatal(err)
	}
	liveDelivery := <-liveEvents
	liveDelivery.Commit(nil)
	if err := wsjson.Read(ctx, connection, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Type != workerproto.MessageAck || ack.Ack != live.Seq {
		t.Fatalf("later live event ACK = %+v", ack)
	}
	_ = workerHub.Abort("caller", "later-request")
}

func TestAuthenticatedWorkerCannotPublishToAnotherWorkersSession(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(2)
	// Make the durable oracle deliberately claim the intruder's forged event is
	// committed. Live session ownership still has to reject it before lookup.
	forgedReceipt := completion.Event{
		ID: "forged", Type: completion.EventAccepted, WorkerID: "worker-intruder",
	}
	payload, err := json.Marshal(forgedReceipt)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	server, err := New(Config{}, multipleWorkerAuthenticator{}, workerHub, receiptOracle{
		eventID: forgedReceipt.ID, workerID: "worker-intruder", digest: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	owner, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: workerHeaders("hae_owner"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.CloseNow()
	intruder, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: workerHeaders("hae_intruder"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer intruder.CloseNow()
	for _, connection := range []*websocket.Conn{owner, intruder} {
		var hello workerproto.Envelope
		if err := wsjson.Read(ctx, connection, &hello); err != nil {
			t.Fatal(err)
		}
	}

	reservation, err := workerHub.Reserve("worker-owner")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	var assignment workerproto.Envelope
	if err := wsjson.Read(ctx, owner, &assignment); err != nil {
		t.Fatal(err)
	}

	forged, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "request",
		Event: completion.Event{
			ID: "forged", Type: completion.EventAccepted, WorkerID: "worker-owner",
		},
	})
	forged.Seq = 1
	if err := wsjson.Write(ctx, intruder, forged); err != nil {
		t.Fatal(err)
	}
	var reply workerproto.Envelope
	readErr := wsjson.Read(ctx, intruder, &reply)
	if readErr == nil {
		t.Fatalf("cross-worker event was acknowledged: %+v", reply)
	}
	var closeError websocket.CloseError
	if !errors.As(readErr, &closeError) || closeError.Code != websocket.StatusPolicyViolation ||
		closeError.Reason != workerproto.WorkerOwnershipViolationReason {
		t.Fatalf("cross-worker close = %v", readErr)
	}
	select {
	case delivery := <-events:
		t.Fatalf("cross-worker event reached completion processor: %+v", delivery.Event)
	case <-time.After(20 * time.Millisecond):
	}

	accepted, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "request",
		Event: completion.Event{
			ID: "accepted", Type: completion.EventAccepted, WorkerID: "worker-intruder",
		},
	})
	accepted.Seq = 1
	if err := wsjson.Write(ctx, owner, accepted); err != nil {
		t.Fatal(err)
	}
	delivery := <-events
	if delivery.WorkerID != "worker-owner" {
		t.Fatalf("accepted identity = %q, want authenticated owner", delivery.WorkerID)
	}
	delivery.Commit(nil)
	var ack workerproto.Envelope
	if err := wsjson.Read(ctx, owner, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Ack != accepted.Seq {
		t.Fatalf("accepted ACK = %+v", ack)
	}
	if err := workerHub.Abort("caller", "request"); err != nil {
		t.Fatal(err)
	}
}

func TestCommittedReceiptReplayIsBoundToAuthenticatedWorker(t *testing.T) {
	t.Parallel()
	event := completion.Event{
		ID: "owner-terminal", Type: completion.EventFinal,
		WorkerID: "worker-owner", Text: "done",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	workerHub := hub.New(2)
	server, err := New(Config{}, multipleWorkerAuthenticator{}, workerHub, receiptOracle{
		eventID: event.ID, workerID: "worker-owner", digest: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(
		ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"),
		&websocket.DialOptions{HTTPHeader: workerHeaders("hae_intruder")},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &hello); err != nil {
		t.Fatal(err)
	}
	// Even a forged body owner cannot reproduce the owner's receipt digest:
	// readLoop overwrites it with worker-intruder before lookup.
	replay, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "completed", Event: event,
	})
	replay.Seq = 1
	if err := wsjson.Write(ctx, connection, replay); err != nil {
		t.Fatal(err)
	}
	var reply workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &reply); err == nil {
		t.Fatalf("cross-worker durable replay was acknowledged: %+v", reply)
	}
}

func TestSameWorkerConnectionReplacementKeepsNewRegistration(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(2)
	server, err := New(Config{}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	dial := func() *websocket.Conn {
		connection, _, dialErr := websocket.Dial(ctx, url, &websocket.DialOptions{
			HTTPHeader: workerHeaders("hae_worker"),
		})
		if dialErr != nil {
			t.Fatal(dialErr)
		}
		var hello workerproto.Envelope
		if readErr := wsjson.Read(ctx, connection, &hello); readErr != nil {
			t.Fatal(readErr)
		}
		return connection
	}
	oldConnection := dial()
	defer oldConnection.CloseNow()
	reservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	var firstAssignment workerproto.Envelope
	if err := wsjson.Read(ctx, oldConnection, &firstAssignment); err != nil {
		t.Fatal(err)
	}

	newConnection := dial()
	defer newConnection.CloseNow()
	var redelivered workerproto.Envelope
	if err := wsjson.Read(ctx, newConnection, &redelivered); err != nil {
		t.Fatal(err)
	}
	if redelivered.Type != workerproto.MessageAssignment {
		t.Fatalf("replacement frame = %+v", redelivered)
	}
	var oldReply workerproto.Envelope
	if err := wsjson.Read(ctx, oldConnection, &oldReply); err == nil {
		t.Fatalf("superseded connection remained active: %+v", oldReply)
	}
	probe, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatalf("old connection cleanup unregistered replacement: %v", err)
	}
	probe.Release()

	final, _ := workerproto.NewEnvelope(workerproto.MessageEvent, workerproto.Event{
		CallerID: "caller", IdempotencyKey: "request",
		Event: completion.Event{ID: "final", Type: completion.EventFinal, Text: "done"},
	})
	final.Seq = 1
	if err := wsjson.Write(ctx, newConnection, final); err != nil {
		t.Fatal(err)
	}
	delivery := <-events
	delivery.Commit(nil)
	var ack workerproto.Envelope
	if err := wsjson.Read(ctx, newConnection, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Ack != final.Seq {
		t.Fatalf("replacement ACK = %+v", ack)
	}
}

func TestDifferentWorkerInstanceCannotSupersedeActiveConnection(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(2)
	server, err := New(Config{}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	first, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: workerHeaders("hae_worker", "worker-instance-a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseNow()
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, first, &hello); err != nil {
		t.Fatal(err)
	}

	second, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: workerHeaders("hae_worker", "worker-instance-b"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseNow()
	var rejected workerproto.Envelope
	err = wsjson.Read(ctx, second, &rejected)
	var closeError websocket.CloseError
	if !errors.As(err, &closeError) || closeError.Code != websocket.StatusPolicyViolation ||
		closeError.Reason != workerproto.WorkerInstanceConflictReason {
		t.Fatalf("second worker close = %v", err)
	}

	reservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatalf("incumbent worker was displaced: %v", err)
	}
	events, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "task", IdempotencyKey: "still-live",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer workerHub.Abort("caller", "still-live")
	var assignment workerproto.Envelope
	if err := wsjson.Read(ctx, first, &assignment); err != nil {
		t.Fatalf("incumbent worker stopped receiving assignments: %v", err)
	}
	if assignment.Type != workerproto.MessageAssignment {
		t.Fatalf("incumbent frame = %+v", assignment)
	}
	_ = events
}

func TestKeepaliveUsesControlFramesAndClosesUnresponsivePeer(t *testing.T) {
	t.Parallel()
	workerHub := hub.New(1)
	server, err := New(Config{
		PingInterval: 10 * time.Millisecond,
		// A 20ms timeout made the responsive half of this test depend on the
		// scheduler while the full race suite was busy. Keep the interval short
		// but give a real reader enough time to process its pong under load.
		PingTimeout: 500 * time.Millisecond,
	}, workerAuthenticator{}, workerHub)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")

	responsive, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: workerHeaders("hae_worker"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var hello workerproto.Envelope
	if err := wsjson.Read(ctx, responsive, &hello); err != nil {
		t.Fatal(err)
	}
	assignmentRead := make(chan workerproto.Envelope, 1)
	readError := make(chan error, 1)
	go func() {
		var envelope workerproto.Envelope
		if readErr := wsjson.Read(ctx, responsive, &envelope); readErr != nil {
			readError <- readErr
			return
		}
		assignmentRead <- envelope
	}()
	time.Sleep(100 * time.Millisecond)
	reservation, err := workerHub.Reserve("worker-1")
	if err != nil {
		t.Fatalf("responsive worker failed keepalive: %v", err)
	}
	if _, err := reservation.Enqueue(completion.Assignment{
		CallerID: "caller", TaskID: "healthy", IdempotencyKey: "healthy",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case envelope := <-assignmentRead:
		if envelope.Type != workerproto.MessageAssignment || envelope.Seq != hello.Seq+1 {
			t.Fatalf("post-ping assignment = %+v, hello = %+v", envelope, hello)
		}
	case err := <-readError:
		t.Fatalf("responsive worker connection closed: %v", err)
	case <-ctx.Done():
		t.Fatal("responsive worker did not receive assignment")
	}
	_ = workerHub.Abort("caller", "healthy")
	_ = responsive.CloseNow()

	// Let the old handler unregister before opening the deliberately idle peer.
	deadline := time.Now().Add(time.Second)
	for {
		probe, reserveErr := workerHub.Reserve("worker-1")
		if errors.Is(reserveErr, hub.ErrNoWorker) {
			break
		}
		if reserveErr == nil {
			probe.Release()
		}
		if time.Now().After(deadline) {
			t.Fatal("responsive worker did not unregister")
		}
		time.Sleep(time.Millisecond)
	}

	idle, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: workerHeaders("hae_worker"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer idle.CloseNow()
	if err := wsjson.Read(ctx, idle, &hello); err != nil {
		t.Fatal(err)
	}
	// With no Reader active, coder/websocket cannot process the ping and send
	// its pong. Poll the externally visible registration instead of guessing a
	// scheduler-sensitive sleep; the server must still retire it within the
	// configured bounded timeout.
	deadline = time.Now().Add(2 * time.Second)
	for {
		probe, reserveErr := workerHub.Reserve("worker-1")
		if errors.Is(reserveErr, hub.ErrNoWorker) {
			break
		}
		if reserveErr == nil {
			probe.Release()
		}
		if time.Now().After(deadline) {
			t.Fatal("unresponsive worker remained registered after keepalive timeout")
		}
		time.Sleep(time.Millisecond)
	}
	var reply workerproto.Envelope
	if err := wsjson.Read(ctx, idle, &reply); err == nil {
		t.Fatalf("unresponsive peer remained connected: %+v", reply)
	}
}
