// Package hub provides the in-process worker queue used by gateway. Durable
// task and response state remains in Store; the hub only owns live delivery.
package hub

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/vibe-agi/human/internal/completion"
)

var (
	ErrNoWorker        = errors.New("no eligible human worker is online")
	ErrCapacity        = errors.New("human worker queue is at capacity")
	ErrReservationUsed = errors.New("admission reservation was already used")
	ErrSessionExists   = errors.New("completion session already exists")
	ErrSessionMissing  = errors.New("completion session does not exist")
	ErrEventConflict   = errors.New("worker event id was reused with different content")
	ErrWorkerOwnership = errors.New("worker does not own completion session")
	ErrWorkerConnected = errors.New("another worker instance is already connected for this identity")
	// ErrEventRejected marks a syntactically valid, authenticated event which
	// the completion processor deterministically refused. Transports may NACK
	// exactly this durable outbox item without dropping the worker connection.
	ErrEventRejected = errors.New("worker event was rejected")
)

// RejectEvent preserves a stable transport classification without exposing
// completion/store internals to the private WebSocket package.
func RejectEvent(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrEventRejected, err)
}

type Worker struct {
	ID          string
	Assignments <-chan completion.Assignment
	hub         *Hub
	state       *workerState
	once        sync.Once
}

func (worker *Worker) Close() {
	worker.once.Do(func() { worker.hub.unregister(worker.ID, worker.state) })
}

type queuedAssignment struct {
	assignment completion.Assignment
	retired    <-chan struct{}
}

type workerState struct {
	mu          sync.Mutex
	instanceID  string
	pending     []queuedAssignment
	capacity    int
	notify      chan struct{}
	done        chan struct{}
	assignments chan completion.Assignment
	closeOnce   sync.Once
}

type session struct {
	workerID        string
	assignment      completion.Assignment
	events          chan *Delivery
	terminal        bool
	accepted        bool
	terminalPending bool
	seen            map[string]struct{}
	inflight        map[string]*Delivery
	pending         map[*Delivery]struct{}
	retired         chan struct{}
}

func newWorkerState(capacity int, instanceID string) *workerState {
	state := &workerState{
		capacity:    capacity,
		instanceID:  instanceID,
		notify:      make(chan struct{}, 1),
		done:        make(chan struct{}),
		assignments: make(chan completion.Assignment),
	}
	go state.dispatch()
	return state
}

// enqueue only appends to an in-memory list and never waits for the worker to
// receive. This keeps worker backpressure local to the worker dispatcher
// instead of allowing it to hold Hub.mu and freeze unrelated admissions.
func (state *workerState) enqueue(assignment completion.Assignment, retired <-chan struct{}) bool {
	state.mu.Lock()
	select {
	case <-state.done:
		state.mu.Unlock()
		return false
	default:
	}
	// Sessions count against Hub.capacity from admission until retirement.
	// Compact canceled entries before enforcing the same bound here, keeping
	// this queue bounded even if aborts run faster than the dispatcher.
	kept := state.pending[:0]
	for _, queued := range state.pending {
		select {
		case <-queued.retired:
			continue
		default:
			kept = append(kept, queued)
		}
	}
	for index := len(kept); index < len(state.pending); index++ {
		state.pending[index] = queuedAssignment{}
	}
	state.pending = kept
	if len(state.pending) >= state.capacity {
		state.mu.Unlock()
		return false
	}
	state.pending = append(state.pending, queuedAssignment{assignment: assignment, retired: retired})
	state.mu.Unlock()
	select {
	case state.notify <- struct{}{}:
	default:
	}
	return true
}

func (state *workerState) dispatch() {
	defer close(state.assignments)
	for {
		queued, ok := state.next()
		if !ok {
			return
		}
		// A retired session may still have an entry in pending. Its retirement
		// channel lets the dispatcher discard it even while no worker receiver
		// is ready, so stale assignments cannot consume future capacity.
		select {
		case <-queued.retired:
			continue
		default:
		}
		select {
		case state.assignments <- queued.assignment:
		case <-queued.retired:
		case <-state.done:
			return
		}
	}
}

func (state *workerState) next() (queuedAssignment, bool) {
	for {
		state.mu.Lock()
		if len(state.pending) != 0 {
			queued := state.pending[0]
			state.pending[0] = queuedAssignment{}
			state.pending = state.pending[1:]
			state.mu.Unlock()
			return queued, true
		}
		state.mu.Unlock()
		select {
		case <-state.notify:
		case <-state.done:
			return queuedAssignment{}, false
		}
	}
}

func (state *workerState) close() {
	state.closeOnce.Do(func() { close(state.done) })
}

func (state *workerState) ensureCapacity(capacity int) {
	state.mu.Lock()
	if capacity > state.capacity {
		state.capacity = capacity
	}
	state.mu.Unlock()
}

func newSession(workerID string, assignment completion.Assignment, accepted bool) *session {
	return &session{
		workerID: workerID, assignment: assignment,
		events: make(chan *Delivery, 32), accepted: accepted,
		seen: make(map[string]struct{}), inflight: make(map[string]*Delivery),
		pending: make(map[*Delivery]struct{}), retired: make(chan struct{}),
	}
}

// terminalReceipt is bounded, compact retired-session metadata. workerID is
// always retained so an expired or otherwise aborted session remains bound to
// its authenticated owner. eventID and digest are populated only for a
// durably committed terminal worker event, where they additionally close the
// socket-ACK-loss window for deployments without a durable receipt store.
// No assignment or request payload is retained.
type terminalReceipt struct {
	eventID  string
	digest   [sha256.Size]byte
	workerID string
}

// Delivery is a worker event whose acknowledgement is held until the
// completion processor has durably handled it. Commit must be called exactly
// once by the consumer. This keeps the WebSocket ACK behind persistence
// instead of merely acknowledging an in-memory channel send.
type Delivery struct {
	completion.Event
	done chan struct{}
	err  error
	once sync.Once
}

func (delivery *Delivery) Commit(err error) {
	delivery.once.Do(func() {
		delivery.err = err
		close(delivery.done)
	})

}

func (delivery *Delivery) wait() error {
	<-delivery.done
	return delivery.err
}

// RestoreOptions describes ownership already made durable before gateway
// restarted. An empty WorkerID leaves an admitted task available to the first
// worker that registers; a non-empty WorkerID preserves the sticky lease.
type RestoreOptions struct {
	WorkerID     string
	Accepted     bool
	SeenEventIDs []string
}

type Hub struct {
	mu            sync.Mutex
	capacity      int
	reserved      int
	active        int
	workers       map[string]*workerState
	onlineWorkers atomic.Int64
	sessions      map[string]*session
	retired       map[string]terminalReceipt
	order         []string
	maxRetired    int
}

// Snapshot is the dependency-free worker state exposed by gateway health
// endpoints. Worker availability is intentionally observational: a gateway
// with no connected worker remains ready to accept health and administrative
// traffic even though model admission will report worker_unavailable.
type Snapshot struct {
	OnlineWorkers int
	HasWorker     bool
}

func New(capacity int) *Hub {
	if capacity < 1 {
		capacity = 1
	}
	maxRetired := capacity * 4
	if maxRetired < 64 {
		maxRetired = 64
	}
	return &Hub{
		capacity:   capacity,
		workers:    make(map[string]*workerState),
		sessions:   make(map[string]*session),
		retired:    make(map[string]terminalReceipt),
		maxRetired: maxRetired,
	}
}

// Snapshot returns a point-in-time count of authenticated live workers.
func (hub *Hub) Snapshot() Snapshot {
	online := int(hub.onlineWorkers.Load())
	return Snapshot{OnlineWorkers: online, HasWorker: online > 0}
}

func (hub *Hub) Register(workerID string) (*Worker, error) {
	// In-process users that do not reconnect can use one stable identifier for
	// both the authenticated worker and its process instance. The WebSocket
	// boundary always supplies an explicit process instance.
	return hub.RegisterInstance(workerID, workerID)
}

// RegisterInstance allows a reconnect from the same running worker to replace
// its half-open transport, but rejects a different process sharing the token.
// This prevents two TUIs from continuously superseding one another.
func (hub *Hub) RegisterInstance(workerID, instanceID string) (*Worker, error) {
	if workerID == "" {
		return nil, errors.New("worker id is required")
	}
	if !completion.IsStableKey(instanceID) {
		return nil, errors.New("stable worker instance id is required")
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	// A restart may recover more durable sessions than today's configured
	// admission limit (for example after an operator lowers queue-capacity).
	// Those sessions must still be drainable; new Reserve calls remain blocked
	// until active falls below the configured limit.
	previous := hub.workers[workerID]
	if previous != nil && previous.instanceID != instanceID {
		return nil, ErrWorkerConnected
	}
	state := newWorkerState(max(hub.capacity, hub.active), instanceID)
	hub.workers[workerID] = state
	for _, active := range hub.sessions {
		if active.workerID == "" && !active.terminal {
			active.workerID = workerID
			active.assignment.LeaseOwner = workerID
		}
		if active.workerID == workerID && !active.terminal {
			if !state.enqueue(active.assignment, active.retired) {
				if previous == nil {
					delete(hub.workers, workerID)
				} else {
					hub.workers[workerID] = previous
				}
				state.close()
				return nil, ErrNoWorker
			}
		}
	}
	// A reconnect with the same authenticated subject supersedes the old live
	// transport. Closing its state makes the old assignment channel terminate;
	// Worker.Close uses state identity, so the old handler cannot unregister
	// this replacement when its deferred cleanup runs.
	if previous != nil {
		previous.close()
	} else {
		hub.onlineWorkers.Add(1)
	}
	return &Worker{ID: workerID, Assignments: state.assignments, hub: hub, state: state}, nil
}

type Reservation struct {
	hub      *Hub
	workerID string
	used     bool
}

func (hub *Hub) Reserve(preferredWorker string) (*Reservation, error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	workerID, err := hub.chooseWorker(preferredWorker)
	if err != nil {
		return nil, err
	}
	if hub.active+hub.reserved >= hub.capacity {
		return nil, ErrCapacity
	}
	hub.reserved++
	return &Reservation{hub: hub, workerID: workerID}, nil
}

func (reservation *Reservation) WorkerID() string {
	return reservation.workerID
}

func (reservation *Reservation) Release() {
	hub := reservation.hub
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if reservation.used {
		return
	}
	reservation.used = true
	hub.reserved--
}

func (reservation *Reservation) Enqueue(assignment completion.Assignment) (<-chan *Delivery, error) {
	hub := reservation.hub
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if reservation.used {
		return nil, ErrReservationUsed
	}
	reservation.used = true
	hub.reserved--
	state, exists := hub.workers[reservation.workerID]
	if !exists {
		return nil, ErrNoWorker
	}
	key := assignment.SessionKey()
	if _, exists := hub.sessions[key]; exists {
		return nil, ErrSessionExists
	}
	if _, retired := hub.retired[key]; retired {
		return nil, ErrSessionExists
	}
	assignment.LeaseOwner = reservation.workerID
	session := newSession(reservation.workerID, assignment, false)
	hub.sessions[key] = session
	hub.active++
	if !state.enqueue(assignment, session.retired) {
		hub.retireSessionLocked(key, session, ErrNoWorker)
		return nil, ErrNoWorker
	}
	return session.events, nil
}

// Restore reconstructs an incomplete live session without requiring its
// worker to be online yet. Restored sessions count against capacity because
// they were admitted durably before the restart.
func (hub *Hub) Restore(assignment completion.Assignment, options RestoreOptions) (<-chan *Delivery, error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	key := assignment.SessionKey()
	if _, exists := hub.sessions[key]; exists {
		return nil, ErrSessionExists
	}
	if _, retired := hub.retired[key]; retired {
		return nil, ErrSessionExists
	}
	workerID := options.WorkerID
	if workerID == "" && len(hub.workers) != 0 {
		var err error
		workerID, err = hub.chooseWorker("")
		if err != nil {
			return nil, err
		}
	}
	assignment.LeaseOwner = workerID
	restored := newSession(workerID, assignment, options.Accepted)
	for _, id := range options.SeenEventIDs {
		if id != "" {
			restored.seen[id] = struct{}{}
		}
	}
	hub.sessions[key] = restored
	hub.active++
	if state, online := hub.workers[workerID]; online {
		state.ensureCapacity(hub.active)
		if !state.enqueue(assignment, restored.retired) {
			hub.retireSessionLocked(key, restored, ErrNoWorker)
			return nil, ErrNoWorker
		}
	}
	return restored.events, nil
}

// Publish is the trusted in-process publishing path retained for completion
// processors and tests which do not model a network principal. Worker
// transports must use PublishFrom so caller-controlled routing keys can never
// cross worker ownership boundaries.
func (hub *Hub) Publish(ctx context.Context, callerID, idempotencyKey string, event completion.Event) error {
	return hub.publish(ctx, "", callerID, idempotencyKey, event)
}

// PublishFrom publishes an event on behalf of an authenticated worker
// subject. Ownership is checked before duplicate or terminal-receipt replay,
// and Accepted.WorkerID is always derived from that subject rather than the
// worker-supplied JSON body.
func (hub *Hub) PublishFrom(
	ctx context.Context,
	workerID string,
	callerID string,
	idempotencyKey string,
	event completion.Event,
) error {
	if workerID == "" {
		return ErrWorkerOwnership
	}
	return hub.publish(ctx, workerID, callerID, idempotencyKey, event)
}

// AuthorizePublisher checks live or recently retired in-memory ownership
// without publishing an event. Worker transports call this before consulting
// a durable receipt, so duplicate ACKs cannot bypass an available session
// owner check. ErrSessionMissing means only that durable recovery metadata is
// required to decide an ACK-loss replay.
func (hub *Hub) AuthorizePublisher(workerID, callerID, idempotencyKey string) error {
	if workerID == "" {
		return ErrWorkerOwnership
	}
	key := callerID + "\x00" + idempotencyKey
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if active, exists := hub.sessions[key]; exists {
		if active.workerID != workerID {
			return ErrWorkerOwnership
		}
		return nil
	}
	if receipt, exists := hub.retired[key]; exists {
		if receipt.workerID != workerID {
			return ErrWorkerOwnership
		}
		return nil
	}
	return ErrSessionMissing
}

func (hub *Hub) publish(
	ctx context.Context,
	workerID string,
	callerID string,
	idempotencyKey string,
	event completion.Event,
) error {
	key := callerID + "\x00" + idempotencyKey
	hub.mu.Lock()
	active, exists := hub.sessions[key]
	if !exists {
		if receipt, retired := hub.retired[key]; retired {
			if workerID != "" && workerID != receipt.workerID {
				hub.mu.Unlock()
				return ErrWorkerOwnership
			}
			if event.ID != "" && event.ID == receipt.eventID {
				digest, err := terminalEventDigest(event)
				if err != nil {
					hub.mu.Unlock()
					return err
				}
				if digest != receipt.digest {
					hub.mu.Unlock()
					return ErrEventConflict
				}
				hub.mu.Unlock()
				return nil
			}
		}
		hub.mu.Unlock()
		return ErrSessionMissing
	}
	if workerID != "" && workerID != active.workerID {
		hub.mu.Unlock()
		return ErrWorkerOwnership
	}
	if event.ID != "" {
		if _, duplicate := active.seen[event.ID]; duplicate {
			hub.mu.Unlock()
			return nil
		}
		if pending, duplicate := active.inflight[event.ID]; duplicate {
			hub.mu.Unlock()
			// Never wait for durable processing while holding Hub.mu. Session
			// retirement commits every pending delivery, so this wait is bounded
			// by either the consumer or retirement.
			return pending.wait()
		}
	}
	if active.terminal || active.terminalPending {
		hub.mu.Unlock()
		return ErrSessionMissing
	}
	if event.Type == completion.EventAccepted {
		if workerID != "" {
			event.WorkerID = workerID
		} else if event.WorkerID == "" {
			event.WorkerID = active.workerID
		} else if event.WorkerID != active.workerID {
			hub.mu.Unlock()
			return errors.New("assignment accepted by the wrong worker")
		}
		if active.accepted {
			if event.ID != "" {
				active.seen[event.ID] = struct{}{}
			}
			hub.mu.Unlock()
			return nil
		}
	}
	terminal := event.EndsResponse()
	if terminal {
		active.terminalPending = true
	}
	delivery := &Delivery{Event: event, done: make(chan struct{})}
	active.pending[delivery] = struct{}{}
	if event.ID != "" {
		active.inflight[event.ID] = delivery
	}
	events := active.events
	retired := active.retired
	hub.mu.Unlock()

	select {
	case events <- delivery:
	case <-retired:
		// retireSessionLocked committed the delivery with its retirement
		// error. Do not close events: a concurrent sender could otherwise panic.
	case <-ctx.Done():
		delivery.Commit(ctx.Err())
	}

	// Once delivered, wait for durable processing even if the WebSocket
	// context is canceled. The consumer owns completion while the session is
	// live; retirement owns completion after the consumer exits.
	err := delivery.wait()
	hub.mu.Lock()
	delete(active.pending, delivery)
	if event.ID != "" {
		if active.inflight[event.ID] == delivery {
			delete(active.inflight, event.ID)
		}
		if err == nil {
			active.seen[event.ID] = struct{}{}
		}
	}
	current := hub.sessions[key] == active && !active.terminal
	if err != nil {
		if terminal && current {
			active.terminalPending = false
		}
		hub.mu.Unlock()
		return err
	}
	if event.Type == completion.EventAccepted {
		active.accepted = true
	}
	if terminal {
		active.terminalPending = false
		if current {
			hub.retireSessionLocked(key, active, ErrSessionMissing)
		}
		// The durable response log and worker-event receipt are the replay
		// oracles once Commit(nil) returns. Retaining the live session here
		// would keep the full canonical request and event maps forever.
		hub.rememberTerminal(key, active.workerID, event)
	}
	hub.mu.Unlock()
	return nil
}

func (hub *Hub) rememberTerminal(key string, workerID string, event completion.Event) {
	if event.ID == "" {
		return
	}
	digest, err := terminalEventDigest(event)
	if err != nil {
		return
	}
	if _, exists := hub.retired[key]; exists {
		hub.retired[key] = terminalReceipt{eventID: event.ID, digest: digest, workerID: workerID}
		return
	}
	hub.retired[key] = terminalReceipt{eventID: event.ID, digest: digest, workerID: workerID}
	hub.order = append(hub.order, key)
	for len(hub.order) > hub.maxRetired {
		oldest := hub.order[0]
		hub.order = hub.order[1:]
		delete(hub.retired, oldest)
	}
}

// rememberRetiredOwner keeps enough metadata to reject a late event from the
// wrong worker after a server-generated terminal transition. It intentionally
// does not invent an event receipt: an arbitrary late event has not been
// durably applied and must be explicitly rejected by the worker transport,
// never acknowledged as an exact replay.
func (hub *Hub) rememberRetiredOwner(key string, workerID string) {
	if _, exists := hub.retired[key]; exists {
		return
	}
	hub.retired[key] = terminalReceipt{workerID: workerID}
	hub.order = append(hub.order, key)
	for len(hub.order) > hub.maxRetired {
		oldest := hub.order[0]
		hub.order = hub.order[1:]
		delete(hub.retired, oldest)
	}
}

func terminalEventDigest(event completion.Event) ([sha256.Size]byte, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("marshal terminal worker event: %w", err)
	}
	return sha256.Sum256(payload), nil
}

// retireSessionLocked removes a live session, releases capacity, cancels any
// assignment that has not reached a worker yet, and completes every publisher
// that could otherwise be stuck sending to events or waiting for Commit.
//
// events is intentionally never closed: publishers may already hold the
// channel outside Hub.mu, and closing it would turn a retirement race into a
// send-on-closed-channel panic.
func (hub *Hub) retireSessionLocked(key string, active *session, err error) bool {
	if hub.sessions[key] != active || active.terminal {
		return false
	}
	if err == nil {
		err = ErrSessionMissing
	}
	active.terminal = true
	active.terminalPending = false
	delete(hub.sessions, key)
	hub.active--
	close(active.retired)
	for delivery := range active.pending {
		delivery.Commit(err)
	}
	return true
}

// Abort releases live capacity without requiring a worker event. Durable
// session processing uses it for server-generated expiry and fatal teardown;
// an HTTP caller disconnect alone does not abort a durable completion.
func (hub *Hub) Abort(callerID, idempotencyKey string) error {
	key := callerID + "\x00" + idempotencyKey
	hub.mu.Lock()
	defer hub.mu.Unlock()
	session, exists := hub.sessions[key]
	if !exists || session.terminal {
		return ErrSessionMissing
	}
	hub.retireSessionLocked(key, session, ErrSessionMissing)
	hub.rememberRetiredOwner(key, session.workerID)
	return nil
}

func (hub *Hub) unregister(workerID string, registered *workerState) {
	hub.mu.Lock()
	state, exists := hub.workers[workerID]
	if !exists || state != registered {
		hub.mu.Unlock()
		registered.close()
		return
	}
	delete(hub.workers, workerID)
	hub.onlineWorkers.Add(-1)
	state.close()
	hub.mu.Unlock()
}

func (hub *Hub) chooseWorker(preferred string) (string, error) {
	if preferred != "" {
		if _, exists := hub.workers[preferred]; !exists {
			return "", ErrNoWorker
		}
		return preferred, nil
	}
	if len(hub.workers) == 0 {
		return "", ErrNoWorker
	}
	ids := make([]string, 0, len(hub.workers))
	for id := range hub.workers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids[0], nil
}
