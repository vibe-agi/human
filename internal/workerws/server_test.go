package workerws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

type receiptOracle struct {
	eventID string
	digest  string
}

func (oracle receiptOracle) RecordWorkerEventReceipt(
	context.Context, storeapi.RequestKey, string, string,
) (storeapi.WorkerEventReceipt, error) {
	return storeapi.WorkerEventReceipt{}, errors.New("receipt oracle is read-only")
}

func (oracle receiptOracle) LookupWorkerEventReceipt(
	_ context.Context,
	key storeapi.RequestKey,
	eventID string,
	digest string,
) (storeapi.WorkerEventReceipt, error) {
	if eventID != oracle.eventID {
		return storeapi.WorkerEventReceipt{}, storeapi.ErrNotFound
	}
	if digest != oracle.digest {
		return storeapi.WorkerEventReceipt{}, storeapi.ErrWorkerEventConflict
	}
	return storeapi.WorkerEventReceipt{RequestKey: key, EventID: eventID, Digest: digest}, nil
}

func (workerAuthenticator) Authenticate(_ context.Context, token string) (auth.Principal, error) {
	if token != "hae_worker" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return auth.Principal{Type: auth.PrincipalWorker, SubjectID: "worker-1", KeyID: "key-worker"}, nil
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
		HTTPHeader: http.Header{"Authorization": []string{"Bearer hae_worker"}},
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
		HTTPHeader: http.Header{"Authorization": []string{"Bearer hae_worker"}},
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
	if err == nil || !errors.Is(err, context.DeadlineExceeded) && websocket.CloseStatus(err) == -1 {
		t.Fatalf("sequence gap did not close connection: %v", err)
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
		HTTPHeader: http.Header{"Authorization": []string{"Bearer hae_worker"}},
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

func TestCommittedEventReplayIsAcknowledgedWithoutLiveHubSession(t *testing.T) {
	t.Parallel()
	event := completion.Event{ID: "event-from-outbox", Type: completion.EventFinal, Text: "already durable"}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	workerHub := hub.New(1)
	server, err := New(Config{}, workerAuthenticator{}, workerHub, receiptOracle{
		eventID: event.ID, digest: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer hae_worker"}},
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
	var reply workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &reply); err == nil {
		t.Fatalf("conflicting durable event id was acknowledged: %+v", reply)
	}
}
