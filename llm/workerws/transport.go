package workerws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

const (
	// MaxWireMessageBytes is the hard ceiling accepted by the official adapter.
	// Hosts may configure a lower limit. The default covers the existing 8 MiB
	// HTTP request budget plus canonical JSON expansion without creating an
	// unbounded WebSocket allocation surface.
	MaxWireMessageBytes int64 = 64 << 20
	defaultReadLimit          = 16 << 20
)

var (
	ErrNotStarted           = errors.New("HumanLLM worker WebSocket transport is not started")
	ErrAlreadyStarted       = errors.New("HumanLLM worker WebSocket transport is already started")
	ErrAuthentication       = errors.New("HumanLLM worker WebSocket authentication failed")
	ErrInvalidConfiguration = errors.New("invalid HumanLLM worker WebSocket configuration")
)

var stableGatewayID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// GatewayID is a stable deployment identity, not a URL. DNS, ports, and
// transports may change without changing the durable gateway identity shown in
// the hello frame.
type GatewayID string

func (identity GatewayID) Validate() error {
	if !stableGatewayID.MatchString(string(identity)) {
		return fmt.Errorf("gateway id must match %s", stableGatewayID.String())
	}
	return nil
}

// Identity is the durable worker identity returned by Authenticator. Session
// identity comes from the transport handshake and is deliberately absent.
type Identity struct {
	Worker llm.WorkerID
}

// Authenticator belongs to this HTTP adapter, not to the HumanLLM domain. An
// embedding host may inspect mTLS, bearer tokens, cookies, or its own user
// system. Worker identity is never trusted from a WebSocket JSON body.
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
// owns the HTTP listener/server and mounts Transport as an http.Handler.
type Config struct {
	GatewayID      GatewayID
	Authenticator  Authenticator
	ReadLimit      int64
	WriteTimeout   time.Duration
	PingInterval   time.Duration
	PingTimeout    time.Duration
	OriginPatterns []string
}

func (config Config) withDefaults() (Config, error) {
	if err := config.GatewayID.Validate(); err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrInvalidConfiguration, err)
	}
	if nilAuthenticator(config.Authenticator) {
		return Config{}, fmt.Errorf("%w: authenticator is required", ErrInvalidConfiguration)
	}
	if config.ReadLimit == 0 {
		config.ReadLimit = defaultReadLimit
	}
	if config.ReadLimit < 1024 || config.ReadLimit > MaxWireMessageBytes {
		return Config{}, fmt.Errorf(
			"%w: read limit must be 1024..%d",
			ErrInvalidConfiguration,
			MaxWireMessageBytes,
		)
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

// Transport is both the reusable WorkerTransport provider and the HTTP handler
// mounted by a host. Start binds exactly one running endpoint; shutdown is
// performed on the returned runtime. A fresh Transport is required for a later
// run so stale sessions can never leak across composition lifetimes.
type Transport struct {
	config Config

	mu      sync.RWMutex
	runtime *runtime
	started bool
}

var _ llm.WorkerTransport = (*Transport)(nil)
var _ http.Handler = (*Transport)(nil)

func New(config Config) (*Transport, error) {
	resolved, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Transport{config: resolved}, nil
}

func (*Transport) Description() llm.WorkerTransportDescription {
	return llm.WorkerTransportDescription{
		Contract: framework.Contract{
			ID: llm.WorkerTransportContractID, Major: llm.WorkerTransportContractMajor,
		},
		Provider: "websocket",
		Version:  "1",
	}
}

func (transport *Transport) Start(
	ctx context.Context,
	endpoint llm.WorkerEndpoint,
) (llm.WorkerTransportRuntime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalidConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nilEndpoint(endpoint) {
		return nil, fmt.Errorf("%w: endpoint is required", ErrInvalidConfiguration)
	}
	if _, err := llm.ValidateWorkerTransport(transport); err != nil {
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
		config:      transport.config,
		endpoint:    endpoint,
		lifecycle:   lifecycle,
		cancel:      cancel,
		done:        make(chan struct{}),
		drained:     make(chan struct{}),
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
	endpoint llm.WorkerEndpoint

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

var _ llm.WorkerTransportRuntime = (*runtime)(nil)

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
		// Admission closes before lifecycle cancellation. Every handler which was
		// already admitted is tracked by handlers and must finish before Done.
		running.closing = true
		running.cancel(llm.ErrWorkerTransportClosed)
		for connection := range running.connections {
			connections = append(connections, connection)
		}
	}
	running.mu.Unlock()

	// CloseNow is the bounded release for half-open peers. A delivery interrupted
	// here receives no synthetic ACK/NACK; the worker retains and replays it.
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
		http.Error(response, llm.ErrWorkerTransportClosed.Error(), http.StatusServiceUnavailable)
		return
	}
	defer running.handlers.Done()

	// Bind authentication and endpoint initialization to both the HTTP peer and
	// adapter lifetime. Shutdown can therefore drain a handler stuck in a custom
	// authenticator as long as that implementation respects its context.
	handlerCtx, cancelHandler := context.WithCancelCause(request.Context())
	stopLifecycleCancel := context.AfterFunc(running.lifecycle, func() {
		cancelHandler(context.Cause(running.lifecycle))
	})
	defer func() {
		stopLifecycleCancel()
		cancelHandler(nil)
	}()
	request = request.WithContext(handlerCtx)

	identity, err := running.config.Authenticator.AuthenticateWorker(handlerCtx, request)
	if err != nil {
		http.Error(response, ErrAuthentication.Error(), http.StatusUnauthorized)
		return
	}
	values := request.Header.Values(SessionHeader)
	if len(values) != 1 {
		http.Error(response, ErrAuthentication.Error(), http.StatusUnauthorized)
		return
	}
	principal := llm.AuthenticatedWorker{
		WorkerID:  identity.Worker,
		SessionID: llm.WorkerSessionID(values[0]),
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
		_ = connection.Close(websocket.StatusGoingAway, "HumanLLM worker transport is shutting down")
		return
	}
	defer running.untrack(connection)
	writer := &connectionWriter{
		connection:   connection,
		writeTimeout: running.config.WriteTimeout,
	}

	coreConnection, err := running.endpoint.OpenWorker(handlerCtx, principal)
	if err != nil {
		_ = connection.Close(websocket.StatusPolicyViolation, safeConnectionMessage(err))
		return
	}
	if nilConnection(coreConnection) || coreConnection.Principal() != principal {
		if !nilConnection(coreConnection) {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), running.config.WriteTimeout)
			_ = coreConnection.Shutdown(shutdownCtx)
			cancel()
		}
		_ = connection.Close(websocket.StatusInternalError, "invalid HumanLLM endpoint connection")
		return
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), running.config.WriteTimeout)
		defer cancel()
		_ = coreConnection.Shutdown(shutdownCtx)
	}()

	ctx, cancel := context.WithCancelCause(handlerCtx)
	var loops sync.WaitGroup
	defer func() {
		cancel(nil)
		connection.CloseNow()
		loops.Wait()
	}()
	incoming := make(chan envelope)
	readErrors := make(chan error, 1)
	loops.Add(2)
	go func() {
		defer loops.Done()
		readLoop(ctx, connection, incoming, readErrors)
	}()
	go func() {
		defer loops.Done()
		running.keepalive(ctx, writer, readErrors)
	}()

	if err := writer.write(ctx, messageHello, hello{
		Gateway: string(running.config.GatewayID),
		Worker:  string(principal.WorkerID),
		Session: string(principal.SessionID),
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
				_ = connection.Close(websocket.StatusInternalError, "invalid assignment from HumanLLM endpoint")
				return
			}
			if err := writer.write(ctx, messageAssignment, assignment); err != nil {
				return
			}
		case message := <-incoming:
			if !running.handleInbound(ctx, connection, writer, coreConnection, principal, message) {
				return
			}
		}
	}
}

func (running *runtime) handleInbound(
	ctx context.Context,
	connection *websocket.Conn,
	writer *connectionWriter,
	core llm.WorkerConnection,
	principal llm.AuthenticatedWorker,
	message envelope,
) bool {
	if err := message.validateInbound(messageAssignmentACK, messageEvent); err != nil {
		_ = connection.Close(websocket.StatusUnsupportedData, "invalid HumanLLM worker message")
		return false
	}
	switch message.Type {
	case messageAssignmentACK:
		var delivery llm.WorkerDeliveryID
		if err := decodeStrictJSON(message.Payload, &delivery); err != nil ||
			strings.TrimSpace(string(delivery)) == "" || string(delivery) != strings.TrimSpace(string(delivery)) {
			_ = connection.Close(websocket.StatusUnsupportedData, "invalid assignment acknowledgement")
			return false
		}
		// Reaching this branch is the only assignment settlement boundary. Merely
		// writing the assignment frame never calls AckAssignment.
		if err := core.AckAssignment(ctx, delivery); err != nil {
			_ = connection.Close(websocket.StatusPolicyViolation, safeConnectionMessage(err))
			return false
		}
		return true
	case messageEvent:
		delivery, err := decodePayload[llm.WorkerEventDelivery](message)
		if err != nil {
			_ = connection.Close(websocket.StatusUnsupportedData, "invalid HumanLLM worker event")
			return false
		}
		// Shape-invalid poison with stable receipt identity receives a deterministic
		// NACK and cannot block a durable outbox head forever. If even delivery/event
		// identity is malformed, no exact settlement can be expressed, so fail closed.
		if err := delivery.ValidateFor(principal); err != nil {
			receipt := llm.WorkerEventReceipt{
				Delivery: delivery.ID,
				EventID:  delivery.Event.ID,
				Decision: llm.WorkerEventNACK,
				Code:     llm.WorkerRejectInvalid,
				Message:  "invalid worker event",
			}
			if receipt.ValidateFor(delivery) != nil {
				_ = connection.Close(websocket.StatusUnsupportedData, "invalid HumanLLM worker event identity")
				return false
			}
			return writer.write(ctx, messageEventReceipt, receipt) == nil
		}
		receipt, err := core.CommitEvent(ctx, delivery)
		if err != nil {
			// An error settles nothing. Closing forces exact durable redelivery; the
			// adapter must never invent a receipt for a commit-unknown boundary.
			_ = connection.Close(websocket.StatusTryAgainLater, "worker event commit is unsettled")
			return false
		}
		if err := receipt.ValidateFor(delivery); err != nil {
			_ = connection.Close(websocket.StatusInternalError, "invalid receipt from HumanLLM endpoint")
			return false
		}
		return writer.write(ctx, messageEventReceipt, receipt) == nil
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

type connectionWriter struct {
	connection   *websocket.Conn
	writeTimeout time.Duration
	mu           sync.Mutex
}

func (writer *connectionWriter) write(ctx context.Context, kind messageType, payload any) error {
	message, err := newEnvelope(kind, payload)
	if err != nil {
		return err
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writeCtx, cancel := context.WithTimeout(ctx, writer.writeTimeout)
	defer cancel()
	return wsjson.Write(writeCtx, writer.connection, message)
}

func (writer *connectionWriter) ping(ctx context.Context, timeout time.Duration) error {
	// coder/websocket permits Ping and Write concurrently, but this adapter uses
	// a stricter single-writer invariant. Data and control frames therefore have
	// one bounded ordering point independent of the concrete WebSocket library.
	writer.mu.Lock()
	defer writer.mu.Unlock()
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return writer.connection.Ping(pingCtx)
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
			reportFailure(failures, err)
			return
		}
		if messageType != websocket.MessageText {
			reportFailure(failures, errors.New("HumanLLM worker protocol requires text JSON messages"))
			return
		}
		var message envelope
		if err := decodeStrictJSON(encoded, &message); err != nil {
			reportFailure(failures, err)
			return
		}
		// The unbuffered handoff is intentional: only one CommitEvent may be in
		// flight, and socket reads pause while its durable settlement is unresolved.
		select {
		case incoming <- message:
		case <-ctx.Done():
			return
		}
	}
}

func (running *runtime) keepalive(
	ctx context.Context,
	writer *connectionWriter,
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
		if err := writer.ping(ctx, running.config.PingTimeout); err != nil {
			reportFailure(failures, err)
			return
		}
	}
}

func reportFailure(failures chan<- error, err error) {
	select {
	case failures <- err:
	default:
	}
}

func safeConnectionMessage(err error) string {
	switch {
	case errors.Is(err, llm.ErrWorkerConnectionConflict):
		return "worker connection conflict"
	case errors.Is(err, llm.ErrWorkerTransportClosed), errors.Is(err, llm.ErrWorkerConnectionClosed):
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

func nilEndpoint(endpoint llm.WorkerEndpoint) bool {
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

func nilConnection(connection llm.WorkerConnection) bool {
	if connection == nil {
		return true
	}
	value := reflect.ValueOf(connection)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
