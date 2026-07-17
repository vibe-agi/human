// Package workerclient implements the human TUI side of the private worker WS.
package workerclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/workerproto"
)

var (
	errWorkerConnectionUnavailable = errors.New("worker connection is not available")
	errServerACKAhead              = errors.New("server ACK exceeds the last client sequence")
	// ErrWorkerAuthentication is returned when the gateway has conclusively
	// rejected the worker credential. Retrying the same credential cannot heal
	// a 401/403/407, so both initial dial and reconnect treat it as terminal.
	ErrWorkerAuthentication = errors.New("gateway rejected worker credentials")
	// ErrWorkerHandshake is returned for a conclusive non-retryable HTTP or
	// WebSocket handshake response (for example a wrong 404 endpoint).
	ErrWorkerHandshake = errors.New("gateway permanently rejected worker handshake")
	// ErrWorkerAlreadyConnected means another Human worker process currently
	// owns the same authenticated subject. The incumbent is never displaced;
	// this client waits with bounded backoff so it can recover automatically
	// when the incumbent was merely a half-open connection.
	ErrWorkerAlreadyConnected = errors.New("worker token is already active in another Human worker")
	// ErrWorkerOwnershipViolation is terminal for this Client instance. The
	// authenticated worker tried to publish to another worker's completion;
	// retrying the same durable outbox head cannot change that authorization
	// decision and would only flap the connection.
	ErrWorkerOwnershipViolation = errors.New("worker event targets another worker's completion")
)

const (
	// workerConnectionReadLimit matches the worker WebSocket server default.
	// Assignments may contain the complete request and tool schemas, so coder's
	// 32 KiB default is too small for real agent traffic.
	workerConnectionReadLimit  int64 = workerproto.MaxWireMessageBytes
	reconnectBackoffMin              = 100 * time.Millisecond
	reconnectBackoffMax              = 5 * time.Second
	reconnectBackoffResetAfter       = 30 * time.Second
	workerDialTimeout                = 10 * time.Second
	workerKeepaliveInterval          = 30 * time.Second
	workerKeepaliveTimeout           = 10 * time.Second
)

type clientRuntimeConfig struct {
	reconnectMin        time.Duration
	reconnectMax        time.Duration
	reconnectResetAfter time.Duration
	dialTimeout         time.Duration
	keepaliveInterval   time.Duration
	keepaliveTimeout    time.Duration
	observeDial         func()
}

func defaultClientRuntimeConfig() clientRuntimeConfig {
	return clientRuntimeConfig{
		reconnectMin:        reconnectBackoffMin,
		reconnectMax:        reconnectBackoffMax,
		reconnectResetAfter: reconnectBackoffResetAfter,
		dialTimeout:         workerDialTimeout,
		keepaliveInterval:   workerKeepaliveInterval,
		keepaliveTimeout:    workerKeepaliveTimeout,
	}
}

func (runtime clientRuntimeConfig) withDefaults() clientRuntimeConfig {
	defaults := defaultClientRuntimeConfig()
	if runtime.reconnectMin <= 0 {
		runtime.reconnectMin = defaults.reconnectMin
	}
	if runtime.reconnectMax < runtime.reconnectMin {
		runtime.reconnectMax = defaults.reconnectMax
		if runtime.reconnectMax < runtime.reconnectMin {
			runtime.reconnectMax = runtime.reconnectMin
		}
	}
	if runtime.reconnectResetAfter <= 0 {
		runtime.reconnectResetAfter = defaults.reconnectResetAfter
	}
	if runtime.dialTimeout <= 0 {
		runtime.dialTimeout = defaults.dialTimeout
	}
	if runtime.keepaliveInterval <= 0 {
		runtime.keepaliveInterval = defaults.keepaliveInterval
	}
	if runtime.keepaliveTimeout <= 0 {
		runtime.keepaliveTimeout = defaults.keepaliveTimeout
	}
	return runtime
}

type Message struct {
	Assignment         *completion.Assignment
	EventRejected      *workerproto.EventRejected
	RejectedEvent      *completion.Event
	RejectedAssignment *completion.Assignment
	RejectedTaskID     string
	ConnectionRestored bool
	Err                error
}

type Client struct {
	connection         *websocket.Conn
	cancel             context.CancelFunc
	messages           chan Message
	writeMu            sync.Mutex
	clientSeq          uint64
	serverSeq          atomic.Uint64
	inflight           map[string]uint64
	workerMu           sync.RWMutex
	workerID           string
	closing            atomic.Bool
	closeOnce          sync.Once
	wait               sync.WaitGroup
	done               chan struct{}
	wake               chan struct{}
	rejectedWake       chan struct{}
	rejectedGeneration atomic.Uint64
	url                string
	token              string
	instanceID         string
	outbox             *durableOutbox
	runtime            clientRuntimeConfig
	closeErr           error
}

// Dial uses the private default outbox. Call DialWithOutbox when embedding the
// client or when the path comes from explicit configuration.
func Dial(ctx context.Context, url, token string) (*Client, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve worker outbox home: %w", err)
	}
	return DialWithOutbox(ctx, url, token, filepath.Join(home, ".human", "worker-outbox.db"))
}

// DialWithOutbox binds a worker to a durable event outbox and starts its
// connection lifecycle. A transient initial failure (including connection
// refused, timeout, 429, or 5xx) returns a live Client which reconnects in the
// background; a malformed endpoint or conclusive handshake/authentication
// rejection returns an error immediately. The bearer token is never stored;
// only a SHA-256 namespace derived from the endpoint and token is written so
// another credential cannot replay events.
func DialWithOutbox(ctx context.Context, url, token, outboxPath string) (*Client, error) {
	return dialWithOutbox(ctx, url, token, outboxPath, defaultClientRuntimeConfig())
}

func dialWithOutbox(
	ctx context.Context,
	url string,
	token string,
	outboxPath string,
	runtime clientRuntimeConfig,
) (*Client, error) {
	if err := validateWorkerEndpoint(url); err != nil {
		return nil, err
	}
	runtime = runtime.withDefaults()
	instanceID, err := canonical.NewOpaqueID("worker_")
	if err != nil {
		return nil, fmt.Errorf("create worker instance identity: %w", err)
	}
	outbox, err := openDurableOutbox(ctx, outboxPath, url, token)
	if err != nil {
		return nil, err
	}
	if runtime.observeDial != nil {
		runtime.observeDial()
	}
	connection, dialErr := dialWorkerConnection(ctx, url, token, instanceID, runtime.dialTimeout)
	if dialErr != nil && (ctx.Err() != nil || isPermanentWorkerDialError(dialErr)) {
		_ = outbox.Close()
		return nil, dialErr
	}
	runContext, cancel := context.WithCancel(context.Background())
	client := &Client{
		connection:   connection,
		cancel:       cancel,
		messages:     make(chan Message, 64),
		inflight:     make(map[string]uint64),
		done:         make(chan struct{}),
		wake:         make(chan struct{}, 1),
		rejectedWake: make(chan struct{}, 1),
		url:          url,
		token:        token,
		instanceID:   instanceID,
		outbox:       outbox,
		runtime:      runtime,
	}
	if dialErr != nil {
		client.deliver(runContext, Message{
			Err: fmt.Errorf("worker connection unavailable; reconnecting: %w", dialErr),
		})
	}
	client.wait.Add(4)
	go func() {
		defer client.wait.Done()
		client.run(runContext, connection)
	}()
	go func() {
		defer client.wait.Done()
		client.flushLoop(runContext)
	}()
	go func() {
		defer client.wait.Done()
		client.rejectedLoop(runContext)
	}()
	go func() {
		defer client.wait.Done()
		client.keepaliveLoop(runContext)
	}()
	go func() {
		client.wait.Wait()
		close(client.messages)
		close(client.done)
	}()
	client.signalFlush()
	client.signalRejectedReplay(true)
	return client, nil
}

func validateWorkerEndpoint(value string) error {
	parsed, err := neturl.Parse(value)
	if err != nil {
		return fmt.Errorf("parse worker gateway URL: %w", err)
	}
	switch parsed.Scheme {
	case "ws", "wss", "http", "https":
	default:
		return fmt.Errorf("worker gateway URL uses unsupported scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return errors.New("worker gateway URL requires a host")
	}
	return nil
}

func (client *Client) Messages() <-chan Message {
	return client.messages
}

func (client *Client) WorkerID() string {
	client.workerMu.RLock()
	defer client.workerMu.RUnlock()
	return client.workerID
}

// ConfirmRejectedEvent atomically replaces one durable inbox record with a
// payload-free digest tombstone. Until this explicit acknowledgement succeeds,
// the client may replay the rejection after process restart or reconnection;
// afterwards, a duplicate server rejection cannot recreate it.
func (client *Client) ConfirmRejectedEvent(ctx context.Context, eventID string) error {
	if err := client.outbox.DeleteRejected(ctx, eventID); err != nil {
		return err
	}
	client.signalRejectedReplay(false)
	return nil
}

// SendEvent returns success once the exact event is committed to the local
// send outbox. Network delivery and ACK may happen later. An exact ID already
// resolved as rejected returns ErrEventPreviouslyRejected instead of a false
// success, because it will not be sent again.
func (client *Client) SendEvent(ctx context.Context, assignment completion.Assignment, event completion.Event) error {
	if event.ID == "" {
		var err error
		event.ID, err = canonical.NewOpaqueID("event_")
		if err != nil {
			return err
		}
	}
	workerID := client.WorkerID()
	if workerID == "" {
		return errors.New("worker identity is not established")
	}
	// Keep the durable outbox payload bound to the server-issued identity for
	// every event type. gateway still overwrites this field from authentication;
	// the client copy makes retries byte-stable across ACK loss.
	event.WorkerID = workerID
	if err := validateWorkerEventSize(assignment, event, workerproto.MaxWireMessageBytes); err != nil {
		return err
	}
	if _, err := client.outbox.Put(ctx, assignment, event); err != nil {
		return err
	}
	client.signalFlush()
	return nil
}

func validateWorkerEventSize(assignment completion.Assignment, event completion.Event, limit int64) error {
	message := workerproto.Event{
		CallerID: assignment.CallerID, IdempotencyKey: assignment.IdempotencyKey, Event: event,
	}
	if err := workerproto.ValidateEnvelopeSize(workerproto.MessageEvent, message, limit); err != nil {
		return fmt.Errorf("worker event cannot be delivered: %w", err)
	}
	return nil
}

func (client *Client) Close() error {
	client.closeOnce.Do(func() {
		client.closing.Store(true)
		client.cancel()
		client.writeMu.Lock()
		connection := client.connection
		client.connection = nil
		client.writeMu.Unlock()
		if connection != nil {
			client.closeErr = connection.Close(websocket.StatusNormalClosure, "human TUI closed")
		}
		<-client.done
		if err := client.outbox.Close(); client.closeErr == nil {
			client.closeErr = err
		}
	})
	return client.closeErr
}

func (client *Client) run(ctx context.Context, connection *websocket.Conn) {
	backoff := client.runtime.reconnectMin
	var connectedAt time.Time
	announceRestored := connection == nil
	if connection != nil {
		connectedAt = time.Now()
	}
	for {
		if connection != nil {
			err := client.readConnection(ctx, connection, announceRestored)
			if client.closing.Load() || ctx.Err() != nil {
				return
			}
			instanceConflict := isWorkerInstanceConflict(err)
			if isWorkerOwnershipViolation(err) {
				client.deliver(ctx, Message{Err: fmt.Errorf("%w: %v", ErrWorkerOwnershipViolation, err)})
				client.cancel()
				return
			}
			if time.Since(connectedAt) >= client.runtime.reconnectResetAfter {
				backoff = client.runtime.reconnectMin
			}
			client.writeMu.Lock()
			if client.connection == connection {
				client.connection = nil
				client.inflight = make(map[string]uint64)
			}
			client.writeMu.Unlock()
			if instanceConflict {
				client.deliver(ctx, Message{Err: fmt.Errorf("%w; waiting to reconnect: %v", ErrWorkerAlreadyConnected, err)})
			} else {
				client.deliver(ctx, Message{Err: fmt.Errorf("worker connection lost; reconnecting: %w", err)})
			}
			_ = connection.CloseNow()
			connection = nil
			announceRestored = true
		}
		for connection == nil {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if client.runtime.observeDial != nil {
				client.runtime.observeDial()
			}
			candidate, dialErr := dialWorkerConnection(
				ctx, client.url, client.token, client.instanceID, client.runtime.dialTimeout,
			)
			if dialErr != nil {
				if isPermanentWorkerDialError(dialErr) {
					client.deliver(ctx, Message{
						Err: fmt.Errorf("worker reconnect stopped: %w", dialErr),
					})
					// Stop outbox and rejected-inbox retries as well. The lifecycle
					// watcher closes Messages; Close remains responsible for SQLite.
					client.cancel()
					return
				}
				backoff = nextReconnectBackoff(backoff, client.runtime.reconnectMax)
				continue
			}
			backoff = nextReconnectBackoff(backoff, client.runtime.reconnectMax)
			client.writeMu.Lock()
			if client.closing.Load() || ctx.Err() != nil {
				client.writeMu.Unlock()
				_ = candidate.CloseNow()
				return
			}
			client.connection = candidate
			client.clientSeq = 0
			client.serverSeq.Store(0)
			client.inflight = make(map[string]uint64)
			client.writeMu.Unlock()
			connection = candidate
			connectedAt = time.Now()
		}
	}
}

func dialWorkerConnection(
	ctx context.Context,
	url string,
	token string,
	instanceID string,
	timeout time.Duration,
) (*websocket.Conn, error) {
	dialContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connection, response, err := websocket.Dial(dialContext, url, &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization":                  []string{"Bearer " + token},
			workerproto.WorkerInstanceHeader: []string{instanceID},
		},
	})
	if err != nil {
		if response != nil {
			status := response.StatusCode
			switch status {
			case http.StatusUnauthorized, http.StatusForbidden, http.StatusProxyAuthRequired:
				return nil, fmt.Errorf("%w (HTTP %d): %v", ErrWorkerAuthentication, status, err)
			case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooEarly, http.StatusTooManyRequests:
				// The same endpoint and credential may recover without operator
				// intervention, so keep these on the transient reconnect path.
			default:
				if status < 500 {
					return nil, fmt.Errorf("%w (HTTP %d): %v", ErrWorkerHandshake, status, err)
				}
			}
		}
		return nil, err
	}
	connection.SetReadLimit(workerConnectionReadLimit)
	return connection, nil
}

func isWorkerInstanceConflict(err error) bool {
	var closeError websocket.CloseError
	return errors.As(err, &closeError) && closeError.Code == websocket.StatusPolicyViolation &&
		strings.HasPrefix(closeError.Reason, "worker_instance_conflict:")
}

func isWorkerOwnershipViolation(err error) bool {
	var closeError websocket.CloseError
	return errors.As(err, &closeError) && closeError.Code == websocket.StatusPolicyViolation &&
		strings.HasPrefix(closeError.Reason, "worker_ownership_violation:")
}

func isPermanentWorkerDialError(err error) bool {
	return errors.Is(err, ErrWorkerAuthentication) || errors.Is(err, ErrWorkerHandshake)
}

func nextReconnectBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum {
		return maximum
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

// keepaliveLoop gives the worker its own bound for detecting a one-way network
// black hole. The gateway also pings, but a dropped gateway-to-worker path can
// hide its close frame from this process indefinitely. Ping is safe alongside
// the single wsjson reader and closes the connection on timeout, which lets run
// use the ordinary durable reconnect/replay path.
func (client *Client) keepaliveLoop(ctx context.Context) {
	ticker := time.NewTicker(client.runtime.keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		client.writeMu.Lock()
		connection := client.connection
		client.writeMu.Unlock()
		if connection == nil {
			continue
		}
		pingContext, cancel := context.WithTimeout(ctx, client.runtime.keepaliveTimeout)
		err := connection.Ping(pingContext)
		cancel()
		if err != nil {
			// A ping may time out just as run replaces a failed connection.
			// Close only the still-current generation; never let an old health
			// check interfere with a newly authenticated socket.
			client.writeMu.Lock()
			if client.connection == connection {
				_ = connection.CloseNow()
			}
			client.writeMu.Unlock()
		}
	}
}

func (client *Client) readConnection(ctx context.Context, connection *websocket.Conn, announceRestored bool) error {
	restoredPending := announceRestored
	for {
		var envelope workerproto.Envelope
		if err := wsjson.Read(ctx, connection, &envelope); err != nil {
			return err
		}
		if err := envelope.Validate(); err != nil {
			return err
		}
		last := client.serverSeq.Load()
		if envelope.Seq > last+1 {
			return errors.New("server worker-protocol sequence gap")
		}
		var rejection *workerproto.EventRejected
		if envelope.Type == workerproto.MessageEventRejected {
			var decoded workerproto.EventRejected
			if err := json.Unmarshal(envelope.Payload, &decoded); err != nil {
				return err
			}
			rejection = &decoded
			if decoded.CallerID == "" || decoded.IdempotencyKey == "" || decoded.EventID == "" {
				return errors.New("invalid worker event rejection")
			}
			// Moving the target outbox row and deleting every event covered by
			// this frame's cumulative ACK must commit as one transaction. Once
			// durable, notification is decoupled from the socket reader and may
			// be replayed until the UI explicitly confirms it.
			_, found, err := client.rejectAndAcknowledge(ctx, envelope.Ack, &decoded)
			if err != nil {
				return err
			}
			if found {
				client.signalRejectedReplay(false)
			}
		}
		if rejection == nil {
			if err := client.acknowledge(ctx, envelope.Ack); err != nil {
				return err
			}
		}
		if envelope.Seq <= last {
			if err := client.sendAck(ctx); err != nil {
				return err
			}
			continue
		}
		client.serverSeq.Store(envelope.Seq)
		switch envelope.Type {
		case workerproto.MessageHello:
			var hello workerproto.Hello
			if err := json.Unmarshal(envelope.Payload, &hello); err != nil {
				return err
			}
			client.workerMu.Lock()
			client.workerID = hello.WorkerID
			client.workerMu.Unlock()
			if restoredPending {
				restoredPending = false
				client.deliver(ctx, Message{ConnectionRestored: true})
				client.signalRejectedReplay(true)
				client.signalFlush()
			}
		case workerproto.MessageAssignment:
			var assignment completion.Assignment
			if err := json.Unmarshal(envelope.Payload, &assignment); err != nil {
				return err
			}
			client.deliver(ctx, Message{Assignment: &assignment})
		case workerproto.MessageAck:
		case workerproto.MessageEventRejected:
		default:
			return errors.New("unexpected server worker message")
		}
		if err := client.sendAck(ctx); err != nil {
			return err
		}
	}
}

func (client *Client) rejectAndAcknowledge(
	ctx context.Context,
	sequence uint64,
	rejection *workerproto.EventRejected,
) (rejectedRecord, bool, error) {
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if sequence > client.clientSeq {
		return rejectedRecord{}, false, errServerACKAhead
	}
	sentAt, inflight := client.inflight[rejection.EventID]
	if inflight && sequence < sentAt {
		return rejectedRecord{}, false, errRejectedAckBehind
	}
	eventIDs := client.acknowledgedEventIDsLocked(sequence)
	record, found, err := client.outbox.RejectAndAcknowledge(ctx, *rejection, eventIDs, inflight)
	if err != nil {
		return rejectedRecord{}, false, err
	}
	for _, eventID := range eventIDs {
		delete(client.inflight, eventID)
	}
	delete(client.inflight, rejection.EventID)
	return record, found, nil
}

func (client *Client) acknowledge(ctx context.Context, sequence uint64) error {
	if sequence == 0 {
		return nil
	}
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if sequence > client.clientSeq {
		return errServerACKAhead
	}
	eventIDs := client.acknowledgedEventIDsLocked(sequence)
	if err := client.outbox.DeleteMany(ctx, eventIDs); err != nil {
		return err
	}
	for _, eventID := range eventIDs {
		delete(client.inflight, eventID)
	}
	return nil
}

func (client *Client) acknowledgedEventIDsLocked(sequence uint64) []string {
	eventIDs := make([]string, 0, len(client.inflight))
	for eventID, sentAt := range client.inflight {
		if sentAt <= sequence {
			eventIDs = append(eventIDs, eventID)
		}
	}
	return eventIDs
}

func (client *Client) sendAck(ctx context.Context) error {
	envelope, _ := workerproto.NewEnvelope(workerproto.MessageAck, nil)
	return client.write(ctx, envelope)
}

func (client *Client) write(ctx context.Context, envelope workerproto.Envelope) error {
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if client.connection == nil {
		return errWorkerConnectionUnavailable
	}
	client.clientSeq++
	envelope.Version = workerproto.Version
	envelope.Seq = client.clientSeq
	envelope.Ack = client.serverSeq.Load()
	writeContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return wsjson.Write(writeContext, client.connection, envelope)
}

func (client *Client) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	reportedFailure := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-client.wake:
		case <-ticker.C:
		}
		err := client.flush(ctx)
		if err == nil {
			reportedFailure = false
			continue
		}
		if ctx.Err() == nil && !errors.Is(err, errWorkerConnectionUnavailable) && !reportedFailure {
			reportedFailure = true
			client.deliver(ctx, Message{Err: fmt.Errorf("worker outbox delivery failed; retrying: %w", err)})
		}
	}
}

func (client *Client) flush(ctx context.Context) error {
	records, err := client.outbox.List(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := client.sendOutboxRecord(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

// rejectedLoop treats the durable inbox as the notification source. It offers
// only the oldest rejection, reoffers that record after reconnect, and does
// not expose the next row until the UI confirms the first. A blocked UI cannot
// stall the WebSocket reader or delay its protocol ACK.
func (client *Client) rejectedLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var offeredEventID string
	var generation uint64
	reportedFailure := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-client.rejectedWake:
		case <-ticker.C:
		}
		currentGeneration := client.rejectedGeneration.Load()
		if currentGeneration != generation {
			generation = currentGeneration
			offeredEventID = ""
		}
		record, found, err := client.outbox.OldestRejected(ctx)
		if err != nil {
			if ctx.Err() == nil && !reportedFailure {
				reportedFailure = true
				client.deliver(ctx, Message{Err: fmt.Errorf("worker rejected inbox replay failed; retrying: %w", err)})
			}
			continue
		}
		reportedFailure = false
		if !found {
			offeredEventID = ""
			continue
		}
		// Offer exactly one oldest record. The durable inbox, not channel
		// capacity, is the queue; the next record is withheld until the UI has
		// installed this one and ConfirmRejectedEvent commits its tombstone.
		if offeredEventID == record.EventID {
			continue
		}
		rejection := record.Rejection
		event := record.Message.Event
		assignment := record.Assignment
		client.deliver(ctx, Message{
			EventRejected: &rejection, RejectedEvent: &event,
			RejectedAssignment: &assignment, RejectedTaskID: record.TaskID,
		})
		if ctx.Err() != nil {
			return
		}
		offeredEventID = record.EventID
	}
}

func (client *Client) sendOutboxRecord(ctx context.Context, record outboxRecord) error {
	envelope, err := workerproto.NewEnvelope(workerproto.MessageEvent, record.Message)
	if err != nil {
		return err
	}
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if _, sent := client.inflight[record.EventID]; sent {
		return nil
	}
	if client.connection == nil {
		return errWorkerConnectionUnavailable
	}
	connection := client.connection
	client.clientSeq++
	envelope.Version = workerproto.Version
	envelope.Seq = client.clientSeq
	envelope.Ack = client.serverSeq.Load()
	client.inflight[record.EventID] = envelope.Seq
	writeContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = wsjson.Write(writeContext, connection, envelope)
	cancel()
	if err != nil {
		delete(client.inflight, record.EventID)
		_ = connection.CloseNow()
		return err
	}
	return nil
}

func (client *Client) signalFlush() {
	select {
	case client.wake <- struct{}{}:
	default:
	}
}

func (client *Client) signalRejectedReplay(replayAll bool) {
	if replayAll {
		client.rejectedGeneration.Add(1)
	}
	select {
	case client.rejectedWake <- struct{}{}:
	default:
	}
}

func (client *Client) deliver(ctx context.Context, message Message) {
	if message.Assignment != nil || message.EventRejected != nil {
		// Assignments and per-event rejections are correctness-bearing: losing
		// either leaves work leased to this worker or leaves the UI operating on
		// a session the gateway has already closed. Apply backpressure instead of
		// silently discarding either state transition when the UI is briefly
		// behind.
		select {
		case client.messages <- message:
		case <-ctx.Done():
		}
		return
	}

	// Connection status and repeated delivery errors are advisory. They are
	// best-effort so a stalled UI cannot block reconnect or outbox progress.
	select {
	case client.messages <- message:
	default:
	}
}
