package workerws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

// The Agent domain permits a 64 MiB Artifact payload. JSON base64 expansion
// plus the surrounding event must still fit the official transport ceiling.
const defaultReadLimit int64 = 96 << 20

var (
	ErrNotStarted           = errors.New("Agent worker WebSocket transport is not started")
	ErrAlreadyStarted       = errors.New("Agent worker WebSocket transport is already started")
	ErrAuthentication       = errors.New("Agent worker WebSocket authentication failed")
	ErrInvalidConfiguration = errors.New("invalid Agent worker WebSocket configuration")
)

// Identity is the durable worker identity returned by Authenticator. Session
// identity comes from the transport handshake and is deliberately absent.
type Identity struct {
	Authority agent.AuthorityID
	Worker    agent.WorkerID
}

// Authenticator belongs to this HTTP adapter, not the Agent domain. A host can
// inspect mTLS, cookies, bearer tokens, or its own session and return canonical
// authority/worker identity. Implementations must not trust identity fields in
// a WebSocket message body.
type Authenticator interface {
	AuthenticateWorker(context.Context, *http.Request) (Identity, error)
}

type AuthenticateFunc func(context.Context, *http.Request) (Identity, error)

func (function AuthenticateFunc) AuthenticateWorker(
	ctx context.Context,
	request *http.Request,
) (Identity, error) {
	return function(ctx, request)
}

// Config controls one official WebSocket adapter. The embedding application
// owns the HTTP server/listener and mounts Transport as a handler.
type Config struct {
	Authenticator  Authenticator
	ReadLimit      int64
	WriteTimeout   time.Duration
	PingInterval   time.Duration
	PingTimeout    time.Duration
	OriginPatterns []string
}

func (config Config) withDefaults() (Config, error) {
	if nilAuthenticator(config.Authenticator) {
		return Config{}, fmt.Errorf("%w: authenticator is required", ErrInvalidConfiguration)
	}
	if config.ReadLimit == 0 {
		config.ReadLimit = defaultReadLimit
	}
	if config.ReadLimit < 1024 || config.ReadLimit > defaultReadLimit {
		return Config{}, fmt.Errorf("%w: read limit must be 1024..%d", ErrInvalidConfiguration, defaultReadLimit)
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.PingInterval == 0 {
		config.PingInterval = 30 * time.Second
	}
	if config.PingTimeout == 0 {
		config.PingTimeout = 10 * time.Second
	}
	if config.WriteTimeout <= 0 || config.PingInterval <= 0 || config.PingTimeout <= 0 {
		return Config{}, fmt.Errorf("%w: transport durations must be positive", ErrInvalidConfiguration)
	}
	config.OriginPatterns = append([]string(nil), config.OriginPatterns...)
	return config, nil
}

// Transport is both a WorkerTransport provider and the handler mounted by the
// host. Start binds exactly one running endpoint; shutdown is performed on the
// returned runtime. A fresh Transport value is required for a later run.
type Transport struct {
	config Config

	mu      sync.RWMutex
	runtime *runtime
	started bool
}

var _ agent.WorkerTransport = (*Transport)(nil)
var _ http.Handler = (*Transport)(nil)

func New(config Config) (*Transport, error) {
	resolved, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Transport{config: resolved}, nil
}

func (*Transport) Description() agent.WorkerTransportDescription {
	return agent.WorkerTransportDescription{
		Contract: framework.Contract{
			ID: agent.WorkerTransportContractID, Major: agent.WorkerTransportContractMajor,
		},
		Provider: "websocket",
		Version:  "1",
	}
}

func (transport *Transport) Start(
	ctx context.Context,
	endpoint agent.WorkerEndpoint,
) (agent.WorkerTransportRuntime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalidConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nilEndpoint(endpoint) {
		return nil, fmt.Errorf("%w: endpoint is required", ErrInvalidConfiguration)
	}
	if err := transport.Description().Validate(); err != nil {
		return nil, err
	}

	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.started {
		return nil, ErrAlreadyStarted
	}
	transport.started = true
	lifecycle, cancel := context.WithCancelCause(context.Background())
	running := &runtime{
		config: transport.config, endpoint: endpoint,
		lifecycle: lifecycle, cancel: cancel,
		drained: make(chan struct{}), done: make(chan struct{}),
		connections: make(map[*websocket.Conn]struct{}),
	}
	transport.runtime = running
	return running, nil
}

func (transport *Transport) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	transport.mu.RLock()
	running := transport.runtime
	transport.mu.RUnlock()
	if running == nil {
		http.Error(response, ErrNotStarted.Error(), http.StatusServiceUnavailable)
		return
	}
	running.serveHTTP(response, request)
}

type runtime struct {
	config   Config
	endpoint agent.WorkerEndpoint

	lifecycle context.Context
	cancel    context.CancelCauseFunc
	done      chan struct{}

	mu          sync.Mutex
	closing     bool
	connections map[*websocket.Conn]struct{}
	handlers    sync.WaitGroup
	drained     chan struct{}
	drainOnce   sync.Once
	finish      sync.Once
	err         error
}

var _ agent.WorkerTransportRuntime = (*runtime)(nil)

func (running *runtime) Done() <-chan struct{} { return running.done }

func (running *runtime) Err() error {
	select {
	case <-running.done:
		return running.err
	default:
		return nil
	}
}

func (running *runtime) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is required", ErrInvalidConfiguration)
	}
	running.mu.Lock()
	var connections []*websocket.Conn
	if !running.closing {
		running.closing = true
		running.cancel(agent.ErrWorkerTransportClosed)
		for connection := range running.connections {
			connections = append(connections, connection)
		}
	}
	running.mu.Unlock()
	// Cancellation is the graceful signal; CloseNow is the bounded fallback
	// which also releases a half-open peer without making Shutdown wait for the
	// WebSocket close handshake outside the caller's context budget.
	for _, connection := range connections {
		connection.CloseNow()
	}

	running.drainOnce.Do(func() {
		go func() {
			running.handlers.Wait()
			close(running.drained)
		}()
	})
	select {
	case <-running.drained:
	case <-ctx.Done():
		return ctx.Err()
	}
	running.finish.Do(func() { close(running.done) })
	return running.err
}

func (running *runtime) serveHTTP(response http.ResponseWriter, request *http.Request) {
	if !running.beginHandler() {
		http.Error(response, agent.ErrWorkerTransportClosed.Error(), http.StatusServiceUnavailable)
		return
	}
	defer running.handlers.Done()

	identity, err := running.config.Authenticator.AuthenticateWorker(request.Context(), request)
	if err != nil {
		http.Error(response, ErrAuthentication.Error(), http.StatusUnauthorized)
		return
	}
	session := strings.TrimSpace(request.Header.Get(SessionHeader))
	principal := agent.AuthenticatedWorker{
		Authority: identity.Authority,
		Worker:    identity.Worker,
		Session:   agent.WorkerSessionID(session),
	}
	if err := principal.Validate(); err != nil {
		http.Error(response, ErrAuthentication.Error(), http.StatusUnauthorized)
		return
	}

	connection, err := websocket.Accept(response, request, &websocket.AcceptOptions{
		OriginPatterns:  running.config.OriginPatterns,
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		return
	}
	connection.SetReadLimit(running.config.ReadLimit)
	if !running.track(connection) {
		_ = connection.Close(websocket.StatusGoingAway, "Agent worker transport is shutting down")
		return
	}
	defer func() {
		running.untrack(connection)
		connection.CloseNow()
	}()

	coreConnection, err := running.endpoint.OpenWorker(request.Context(), principal)
	if err != nil {
		_ = connection.Close(websocket.StatusPolicyViolation, safeConnectionMessage(err))
		return
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), running.config.WriteTimeout)
		defer cancel()
		_ = coreConnection.Shutdown(shutdownCtx)
	}()

	ctx, cancel := context.WithCancelCause(running.lifecycle)
	defer cancel(nil)
	incoming := make(chan envelope)
	readErrors := make(chan error, 1)
	go readLoop(ctx, connection, incoming, readErrors)
	go running.keepalive(ctx, connection, readErrors)

	if err := running.write(ctx, connection, messageHello, hello{
		Authority: string(principal.Authority), Worker: string(principal.Worker), Session: string(principal.Session),
	}); err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-coreConnection.Done():
			_ = connection.Close(websocket.StatusPolicyViolation, safeConnectionMessage(coreConnection.Err()))
			return
		case err := <-readErrors:
			if err != nil {
				cancel(err)
			}
			return
		case assignment, open := <-coreConnection.Assignments():
			if !open {
				return
			}
			if err := assignment.ValidateFor(principal); err != nil {
				_ = connection.Close(websocket.StatusInternalError, "invalid assignment from Agent endpoint")
				return
			}
			if err := running.write(ctx, connection, messageAssignment, assignment); err != nil {
				return
			}
		case message := <-incoming:
			if !running.handleInbound(ctx, connection, coreConnection, principal, message) {
				return
			}
		}
	}
}

func (running *runtime) handleInbound(
	ctx context.Context,
	connection *websocket.Conn,
	core agent.WorkerConnection,
	principal agent.AuthenticatedWorker,
	message envelope,
) bool {
	if err := message.validateInbound(messageAssignmentACK, messageEvent); err != nil {
		_ = connection.Close(websocket.StatusUnsupportedData, "invalid Agent worker message")
		return false
	}
	switch message.Type {
	case messageAssignmentACK:
		var delivery agent.WorkerDeliveryID
		if err := decodeStrictJSON(message.Payload, &delivery); err != nil || strings.TrimSpace(string(delivery)) == "" {
			_ = connection.Close(websocket.StatusUnsupportedData, "invalid assignment acknowledgement")
			return false
		}
		if err := core.AckAssignment(ctx, delivery); err != nil {
			_ = connection.Close(websocket.StatusPolicyViolation, safeConnectionMessage(err))
			return false
		}
		return true
	case messageEvent:
		delivery, err := decodePayload[agent.WorkerEventDelivery](message)
		if err != nil {
			_ = connection.Close(websocket.StatusUnsupportedData, "invalid Agent worker event")
			return false
		}
		// ValidateFor is a cheap wire gate. The endpoint repeats it at the durable
		// commit boundary so a custom transport cannot bypass authenticated scope.
		if err := delivery.ValidateFor(principal); err != nil {
			receipt := agent.WorkerEventReceipt{
				Delivery: delivery.ID, Event: delivery.Event.ID,
				Decision: agent.WorkerEventNACK, Code: agent.WorkerRejectInvalid,
				Message: "invalid worker event",
			}
			return running.write(ctx, connection, messageEventReceipt, receipt) == nil
		}
		receipt, err := core.CommitEvent(ctx, delivery)
		if err != nil {
			// No receipt is sent for an unsettled commit. Closing forces exact
			// redelivery from the remote durable outbox after reconnect.
			_ = connection.Close(websocket.StatusTryAgainLater, "worker event commit is unsettled")
			return false
		}
		if err := receipt.ValidateFor(delivery); err != nil {
			_ = connection.Close(websocket.StatusInternalError, "invalid receipt from Agent endpoint")
			return false
		}
		return running.write(ctx, connection, messageEventReceipt, receipt) == nil
	default:
		return false
	}
}

func (running *runtime) beginHandler() bool {
	running.mu.Lock()
	defer running.mu.Unlock()
	if running.closing {
		return false
	}
	running.handlers.Add(1)
	return true
}

func (running *runtime) track(connection *websocket.Conn) bool {
	running.mu.Lock()
	defer running.mu.Unlock()
	if running.closing {
		return false
	}
	running.connections[connection] = struct{}{}
	return true
}

func (running *runtime) untrack(connection *websocket.Conn) {
	running.mu.Lock()
	delete(running.connections, connection)
	running.mu.Unlock()
}

func (running *runtime) write(
	ctx context.Context,
	connection *websocket.Conn,
	kind messageType,
	payload any,
) error {
	message, err := newEnvelope(kind, payload)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, running.config.WriteTimeout)
	defer cancel()
	return wsjson.Write(writeCtx, connection, message)
}

func readLoop(
	ctx context.Context,
	connection *websocket.Conn,
	incoming chan<- envelope,
	failures chan<- error,
) {
	for {
		messageType, encoded, err := connection.Read(ctx)
		if err != nil {
			select {
			case failures <- err:
			default:
			}
			return
		}
		if messageType != websocket.MessageText {
			select {
			case failures <- errors.New("Agent worker protocol requires text JSON messages"):
			default:
			}
			return
		}
		var message envelope
		if err := decodeStrictJSON(encoded, &message); err != nil {
			select {
			case failures <- err:
			default:
			}
			return
		}
		select {
		case incoming <- message:
		case <-ctx.Done():
			return
		}
	}
}

func (running *runtime) keepalive(
	ctx context.Context,
	connection *websocket.Conn,
	failures chan<- error,
) {
	ticker := time.NewTicker(running.config.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pingCtx, cancel := context.WithTimeout(ctx, running.config.PingTimeout)
		err := connection.Ping(pingCtx)
		cancel()
		if err != nil {
			select {
			case failures <- err:
			default:
			}
			return
		}
	}
}

func safeConnectionMessage(err error) string {
	switch {
	case errors.Is(err, agent.ErrWorkerConnectionConflict):
		return "worker connection conflict"
	case errors.Is(err, agent.ErrWorkerTransportClosed), errors.Is(err, agent.ErrWorkerConnectionClosed):
		return "worker connection closed"
	default:
		return "worker connection failed"
	}
}

func nilAuthenticator(authenticator Authenticator) bool {
	if authenticator == nil {
		return true
	}
	value := reflect.ValueOf(authenticator)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func nilEndpoint(endpoint agent.WorkerEndpoint) bool {
	if endpoint == nil {
		return true
	}
	value := reflect.ValueOf(endpoint)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
