// Package workerws exposes the private WebSocket transport used by human TUI.
package workerws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/hub"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/workerproto"
)

type Config struct {
	ReadLimit    int64
	WriteTimeout time.Duration
	PingInterval time.Duration
	PingTimeout  time.Duration
}

func (config Config) withDefaults() Config {
	if config.ReadLimit <= 0 {
		config.ReadLimit = workerproto.MaxWireMessageBytes
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.PingInterval <= 0 {
		config.PingInterval = 30 * time.Second
	}
	if config.PingTimeout <= 0 {
		config.PingTimeout = 10 * time.Second
	}
	return config
}

type Server struct {
	config   Config
	auth     auth.Authenticator
	hub      *hub.Hub
	receipts storeapi.WorkerEventReceiptStore

	// writeEnvelope and observeOutbound are narrow test seams for deterministic
	// transport-failure coverage. Production always uses wsjson.Write and leaves
	// observeOutbound nil.
	writeEnvelope   func(context.Context, *websocket.Conn, workerproto.Envelope) error
	observeOutbound func(*outboundQueue)
}

// outboundQueue binds each cumulative ACK to a frame when that frame enters
// the FIFO, not later when the socket writer happens to dequeue it. Without
// this boundary, an old assignment already ahead of an event_rejected frame
// could absorb the rejection's future ACK and make the client delete its
// outbox record before learning that the event was rejected.
type outboundQueue struct {
	mu      sync.Mutex
	frames  chan workerproto.Envelope
	lastAck uint64
}

func newOutboundQueue(capacity int) *outboundQueue {
	return &outboundQueue{frames: make(chan workerproto.Envelope, capacity)}
}

func (queue *outboundQueue) enqueue(
	ctx context.Context,
	envelope workerproto.Envelope,
	ack uint64,
) bool {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	// Concurrent producers can snapshot lastCommitted in either order. Keep
	// the wire watermark monotonic while preserving FIFO: promotion is safe
	// because the frame is necessarily queued after the frame which introduced
	// lastAck (notably event_rejected).
	if ack < queue.lastAck {
		ack = queue.lastAck
	}
	envelope.Ack = ack
	select {
	case queue.frames <- envelope:
		queue.lastAck = ack
		return true
	case <-ctx.Done():
		return false
	}
}

func enqueueRejection(
	ctx context.Context,
	queue *outboundQueue,
	envelope workerproto.Envelope,
	sequence uint64,
	lastCommitted *atomic.Uint64,
) bool {
	if !queue.enqueue(ctx, envelope, sequence) {
		return false
	}
	lastCommitted.Store(sequence)
	return true
}

func New(
	config Config,
	authenticator auth.Authenticator,
	workerHub *hub.Hub,
	receiptStores ...storeapi.WorkerEventReceiptStore,
) (*Server, error) {
	if authenticator == nil || workerHub == nil {
		return nil, errors.New("worker authenticator and hub are required")
	}
	if len(receiptStores) > 1 {
		return nil, errors.New("at most one worker event receipt store is allowed")
	}
	server := &Server{
		config: config.withDefaults(), auth: authenticator, hub: workerHub,
		writeEnvelope: func(ctx context.Context, connection *websocket.Conn, envelope workerproto.Envelope) error {
			return wsjson.Write(ctx, connection, envelope)
		},
	}
	if len(receiptStores) == 1 {
		server.receipts = receiptStores[0]
	}
	return server, nil
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	principal, err := server.authenticate(request)
	if err != nil || principal.Type != auth.PrincipalWorker ||
		principal.SubjectID == workerproto.GatewayEventOwner {
		http.Error(response, "worker token required", http.StatusUnauthorized)
		return
	}
	instanceID := strings.TrimSpace(request.Header.Get(workerproto.WorkerInstanceHeader))
	if !completion.IsStableKey(instanceID) {
		http.Error(response, "stable worker instance id required", http.StatusPreconditionRequired)
		return
	}
	connection, err := websocket.Accept(response, request, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		return
	}
	defer connection.CloseNow()
	connection.SetReadLimit(server.config.ReadLimit)

	worker, err := server.hub.RegisterInstance(principal.SubjectID, instanceID)
	if err != nil {
		reason := err.Error()
		if errors.Is(err, hub.ErrWorkerConnected) {
			reason = workerproto.WorkerInstanceConflictReason
		}
		_ = connection.Close(websocket.StatusPolicyViolation, reason)
		return
	}
	defer worker.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbound := newOutboundQueue(64)
	if server.observeOutbound != nil {
		server.observeOutbound(outbound)
	}
	errorsChannel := make(chan error, 3)
	var failOnce sync.Once
	var failureMu sync.Mutex
	var failureErr error
	loadFailure := func() error {
		failureMu.Lock()
		defer failureMu.Unlock()
		return failureErr
	}
	fail := func(err error) {
		failOnce.Do(func() {
			failureMu.Lock()
			failureErr = err
			failureMu.Unlock()
			// Cancellation is the primary failure signal. In particular, it
			// releases producers blocked behind a full outbound queue even when
			// this handler is itself one of those producers and cannot receive
			// from errorsChannel.
			cancel()
			if err == nil {
				return
			}
			select {
			case errorsChannel <- err:
			default:
			}
		})
	}
	var lastIncoming atomic.Uint64
	var lastCommitted atomic.Uint64

	hello, _ := workerproto.NewEnvelope(workerproto.MessageHello, workerproto.Hello{WorkerID: worker.ID})
	if !outbound.enqueue(ctx, hello, lastCommitted.Load()) {
		return
	}
	go server.writeLoop(ctx, connection, outbound.frames, fail)
	go server.readLoop(
		ctx, connection, principal.SubjectID, outbound,
		&lastIncoming, &lastCommitted, fail,
	)
	go server.keepaliveLoop(ctx, connection, fail)

	for {
		select {
		case <-ctx.Done():
			if err := loadFailure(); err != nil {
				_ = connection.Close(websocket.StatusPolicyViolation, err.Error())
			}
			return
		case err := <-errorsChannel:
			if err != nil {
				_ = connection.Close(websocket.StatusPolicyViolation, err.Error())
			}
			return
		case assignment, open := <-worker.Assignments:
			if !open {
				return
			}
			if err := workerproto.ValidateEnvelopeSize(
				workerproto.MessageAssignment, assignment, server.config.ReadLimit,
			); err != nil {
				fail(err)
				return
			}
			envelope, err := workerproto.NewEnvelope(workerproto.MessageAssignment, assignment)
			if err != nil {
				return
			}
			if !outbound.enqueue(ctx, envelope, lastCommitted.Load()) {
				return
			}
		}
	}
}

// keepaliveLoop uses WebSocket control frames, not worker-protocol envelopes,
// so health checks do not consume or acknowledge application sequence
// numbers. Ping waits for a pong read by readLoop and therefore bounds the
// lifetime of a half-open connection.
func (server *Server) keepaliveLoop(
	ctx context.Context,
	connection *websocket.Conn,
	fail func(error),
) {
	ticker := time.NewTicker(server.config.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pingContext, cancel := context.WithTimeout(ctx, server.config.PingTimeout)
		err := connection.Ping(pingContext)
		cancel()
		if err == nil {
			continue
		}
		fail(fmt.Errorf("worker keepalive failed: %w", err))
		return
	}
}

func (server *Server) writeLoop(
	ctx context.Context,
	connection *websocket.Conn,
	outbound <-chan workerproto.Envelope,
	fail func(error),
) {
	var sequence uint64
	for {
		select {
		case <-ctx.Done():
			return
		case envelope := <-outbound:
			sequence++
			envelope.Version = workerproto.Version
			envelope.Seq = sequence
			writeContext, cancel := context.WithTimeout(ctx, server.config.WriteTimeout)
			err := server.writeEnvelope(writeContext, connection, envelope)
			cancel()
			if err != nil {
				fail(err)
				return
			}
		}
	}
}

func (server *Server) readLoop(
	ctx context.Context,
	connection *websocket.Conn,
	workerID string,
	outbound *outboundQueue,
	lastIncoming *atomic.Uint64,
	lastCommitted *atomic.Uint64,
	fail func(error),
) {
	for {
		var envelope workerproto.Envelope
		if err := wsjson.Read(ctx, connection, &envelope); err != nil {
			fail(err)
			return
		}
		if err := envelope.Validate(); err != nil {
			fail(err)
			return
		}
		last := lastIncoming.Load()
		if envelope.Seq <= last {
			ack, _ := workerproto.NewEnvelope(workerproto.MessageAck, nil)
			if !outbound.enqueue(ctx, ack, lastCommitted.Load()) {
				return
			}
			continue
		}
		if envelope.Seq != last+1 {
			fail(errors.New("worker message sequence gap"))
			return
		}
		lastIncoming.Store(envelope.Seq)
		switch envelope.Type {
		case workerproto.MessageAck:
			lastCommitted.Store(envelope.Seq)
			continue
		case workerproto.MessageEvent:
			var event workerproto.Event
			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				fail(err)
				return
			}
			if strings.TrimSpace(event.Event.ID) == "" {
				fail(errors.New("worker event id is required"))
				return
			}
			// Bind every receipt digest to the authenticated connection subject.
			// This also overwrites Accepted.WorkerID before the event reaches the
			// state machine, so the JSON body is never an ownership oracle.
			event.Event.WorkerID = workerID
			authorizationErr := server.hub.AuthorizePublisher(
				workerID, event.CallerID, event.IdempotencyKey,
			)
			if authorizationErr != nil && !errors.Is(authorizationErr, hub.ErrSessionMissing) {
				if errors.Is(authorizationErr, hub.ErrWorkerOwnership) {
					// This is the authoritative pre-publish identity gate. Unlike a
					// stale-session or event-content conflict, acknowledging it would
					// let one principal erase evidence that it targeted another
					// worker's session. Close with a machine-stable terminal reason so
					// the official client does not turn it into a reconnect storm.
					fail(errors.New(workerproto.WorkerOwnershipViolationReason))
				} else {
					fail(authorizationErr)
				}
				return
			}
			committed, err := server.eventAlreadyCommitted(ctx, workerID, event)
			if err != nil {
				if errors.Is(err, hub.ErrWorkerOwnership) {
					fail(errors.New(workerproto.WorkerOwnershipViolationReason))
					return
				}
				if isRejectableReceiptError(err) {
					if !enqueueEventRejection(ctx, outbound, event, envelope.Seq, lastCommitted, err.Error()) {
						return
					}
					continue
				}
				fail(err)
				return
			}
			if !committed {
				if err := server.hub.PublishFrom(
					ctx, workerID, event.CallerID, event.IdempotencyKey, event.Event,
				); err != nil {
					if !isRejectablePublishError(err) {
						fail(err)
						return
					}
					message := err.Error()
					if errors.Is(err, hub.ErrSessionMissing) {
						message = "completion session is already closed"
					}
					if !enqueueEventRejection(ctx, outbound, event, envelope.Seq, lastCommitted, message) {
						return
					}
					continue
				}
			}
			lastCommitted.Store(envelope.Seq)
			ack, _ := workerproto.NewEnvelope(workerproto.MessageAck, nil)
			if !outbound.enqueue(ctx, ack, envelope.Seq) {
				return
			}
		default:
			fail(errors.New("worker may only send ack or event messages"))
			return
		}
	}
}

// isRejectableReceiptError is deliberately narrower than a generic storage
// error classification. A digest conflict returned by this pre-publish
// lookup proves another payload with the same event id already has its full
// durable receipt; the incoming variant has made no state or response effect.
func isRejectableReceiptError(err error) bool {
	return errors.Is(err, storeapi.ErrWorkerEventConflict)
}

// isRejectablePublishError is used only after AuthorizePublisher has either
// bound the connection principal to the session or reported no session. Each
// listed result is returned before the incoming event has an effect:
//
//   - missing sessions are stale outbox records;
//   - ErrEventRejected is emitted by the processor only before its first
//     durable step;
//   - retired digest conflicts compare against an already committed terminal;
//   - ownership here can only arise from a race after the authoritative
//     pre-publish check. An immediate principal mismatch is terminated above.
func isRejectablePublishError(err error) bool {
	return errors.Is(err, hub.ErrSessionMissing) ||
		errors.Is(err, hub.ErrEventRejected) ||
		errors.Is(err, hub.ErrEventConflict) ||
		errors.Is(err, hub.ErrWorkerOwnership)
}

func enqueueEventRejection(
	ctx context.Context,
	outbound *outboundQueue,
	event workerproto.Event,
	sequence uint64,
	lastCommitted *atomic.Uint64,
	message string,
) bool {
	rejected, err := workerproto.NewEnvelope(
		workerproto.MessageEventRejected,
		workerproto.EventRejected{
			CallerID: event.CallerID, IdempotencyKey: event.IdempotencyKey,
			EventID: event.Event.ID, Message: message,
		},
	)
	if err != nil {
		return false
	}
	// Queue the semantic rejection before publishing its cumulative ACK
	// watermark. Frames already in front retain the old snapshot; frames behind
	// cannot overtake this FIFO entry.
	return enqueueRejection(ctx, outbound, rejected, sequence, lastCommitted)
}

func (server *Server) eventAlreadyCommitted(
	ctx context.Context,
	workerID string,
	event workerproto.Event,
) (bool, error) {
	if server.receipts == nil || event.Event.ID == "" {
		return false, nil
	}
	payload, err := json.Marshal(event.Event)
	if err != nil {
		return false, fmt.Errorf("marshal worker event receipt digest: %w", err)
	}
	sum := sha256.Sum256(payload)
	receipt, err := server.receipts.LookupWorkerEventReceipt(ctx, storeapi.RequestKey{
		CallerID: event.CallerID, IdempotencyKey: event.IdempotencyKey,
	}, event.Event.ID)
	switch {
	case errors.Is(err, storeapi.ErrNotFound):
		return false, nil
	case err != nil:
		return false, err
	}
	if receipt.WorkerID != workerID {
		return false, hub.ErrWorkerOwnership
	}
	if receipt.Digest != hex.EncodeToString(sum[:]) {
		return false, storeapi.ErrWorkerEventConflict
	}
	return true, nil
}

func (server *Server) authenticate(request *http.Request) (auth.Principal, error) {
	return server.auth.AuthenticateRequest(request)
}

// Keep the completion import anchored here so protocol evolution cannot
// silently change assignment ownership semantics.
var _ = completion.Assignment{}
