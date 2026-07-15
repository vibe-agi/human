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
}

func (config Config) withDefaults() Config {
	if config.ReadLimit <= 0 {
		config.ReadLimit = 8 << 20
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 10 * time.Second
	}
	return config
}

type Server struct {
	config   Config
	auth     auth.Authenticator
	hub      *hub.Hub
	receipts storeapi.WorkerEventReceiptStore
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
	server := &Server{config: config.withDefaults(), auth: authenticator, hub: workerHub}
	if len(receiptStores) == 1 {
		server.receipts = receiptStores[0]
	}
	return server, nil
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	principal, err := server.authenticate(request)
	if err != nil || principal.Type != auth.PrincipalWorker {
		http.Error(response, "worker token required", http.StatusUnauthorized)
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

	worker, err := server.hub.Register(principal.SubjectID)
	if err != nil {
		_ = connection.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}
	defer worker.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbound := make(chan workerproto.Envelope, 64)
	errorsChannel := make(chan error, 2)
	var lastIncoming atomic.Uint64
	var lastCommitted atomic.Uint64

	hello, _ := workerproto.NewEnvelope(workerproto.MessageHello, workerproto.Hello{WorkerID: worker.ID})
	outbound <- hello
	go server.writeLoop(ctx, connection, outbound, &lastCommitted, errorsChannel)
	go server.readLoop(ctx, connection, outbound, &lastIncoming, &lastCommitted, errorsChannel)

	for {
		select {
		case <-ctx.Done():
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
			envelope, err := workerproto.NewEnvelope(workerproto.MessageAssignment, assignment)
			if err != nil {
				return
			}
			select {
			case outbound <- envelope:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (server *Server) writeLoop(
	ctx context.Context,
	connection *websocket.Conn,
	outbound <-chan workerproto.Envelope,
	lastCommitted *atomic.Uint64,
	errorsChannel chan<- error,
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
			envelope.Ack = lastCommitted.Load()
			writeContext, cancel := context.WithTimeout(ctx, server.config.WriteTimeout)
			err := wsjson.Write(writeContext, connection, envelope)
			cancel()
			if err != nil {
				select {
				case errorsChannel <- err:
				default:
				}
				return
			}
		}
	}
}

func (server *Server) readLoop(
	ctx context.Context,
	connection *websocket.Conn,
	outbound chan<- workerproto.Envelope,
	lastIncoming *atomic.Uint64,
	lastCommitted *atomic.Uint64,
	errorsChannel chan<- error,
) {
	for {
		var envelope workerproto.Envelope
		if err := wsjson.Read(ctx, connection, &envelope); err != nil {
			select {
			case errorsChannel <- err:
			default:
			}
			return
		}
		if err := envelope.Validate(); err != nil {
			errorsChannel <- err
			return
		}
		last := lastIncoming.Load()
		if envelope.Seq <= last {
			ack, _ := workerproto.NewEnvelope(workerproto.MessageAck, nil)
			outbound <- ack
			continue
		}
		if envelope.Seq != last+1 {
			errorsChannel <- errors.New("worker message sequence gap")
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
				errorsChannel <- err
				return
			}
			if strings.TrimSpace(event.Event.ID) == "" {
				errorsChannel <- errors.New("worker event id is required")
				return
			}
			committed, err := server.eventAlreadyCommitted(ctx, event)
			if err != nil {
				errorsChannel <- err
				return
			}
			if !committed {
				if err := server.hub.Publish(ctx, event.CallerID, event.IdempotencyKey, event.Event); err != nil {
					errorsChannel <- err
					return
				}
			}
			lastCommitted.Store(envelope.Seq)
			ack, _ := workerproto.NewEnvelope(workerproto.MessageAck, nil)
			outbound <- ack
		default:
			errorsChannel <- errors.New("worker may only send ack or event messages")
			return
		}
	}
}

func (server *Server) eventAlreadyCommitted(ctx context.Context, event workerproto.Event) (bool, error) {
	if server.receipts == nil || event.Event.ID == "" {
		return false, nil
	}
	payload, err := json.Marshal(event.Event)
	if err != nil {
		return false, fmt.Errorf("marshal worker event receipt digest: %w", err)
	}
	sum := sha256.Sum256(payload)
	_, err = server.receipts.LookupWorkerEventReceipt(ctx, storeapi.RequestKey{
		CallerID: event.CallerID, IdempotencyKey: event.IdempotencyKey,
	}, event.Event.ID, hex.EncodeToString(sum[:]))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, storeapi.ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

func (server *Server) authenticate(request *http.Request) (auth.Principal, error) {
	authorization := request.Header.Get("Authorization")
	if !strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return server.auth.Authenticate(request.Context(), strings.TrimSpace(authorization[len("Bearer "):]))
}

// Keep the completion import anchored here so protocol evolution cannot
// silently change assignment ownership semantics.
var _ = completion.Assignment{}
