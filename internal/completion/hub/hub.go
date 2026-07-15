// Package hub provides the in-process worker queue used by humand. Durable
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

	"github.com/vibe-agi/human/internal/completion"
)

var (
	ErrNoWorker        = errors.New("no eligible human worker is online")
	ErrCapacity        = errors.New("human worker queue is at capacity")
	ErrReservationUsed = errors.New("admission reservation was already used")
	ErrSessionExists   = errors.New("completion session already exists")
	ErrSessionMissing  = errors.New("completion session does not exist")
	ErrEventConflict   = errors.New("worker event id was reused with different content")
)

type Worker struct {
	ID          string
	Assignments <-chan completion.Assignment
	hub         *Hub
	once        sync.Once
}

func (worker *Worker) Close() {
	worker.once.Do(func() { worker.hub.unregister(worker.ID) })
}

type workerState struct {
	queue chan completion.Assignment
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
}

// terminalReceipt is the bounded, in-memory fallback for deployments that do
// not attach a durable WorkerEventReceiptStore to the worker transport. It
// retains no assignment or request payload. Production replay still uses the
// durable receipt store; this compact receipt only closes the socket-ACK-loss
// window without making live sessions grow forever.
type terminalReceipt struct {
	eventID string
	digest  [sha256.Size]byte
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

// RestoreOptions describes ownership already made durable before humand
// restarted. An empty WorkerID leaves an admitted task available to the first
// worker that registers; a non-empty WorkerID preserves the sticky lease.
type RestoreOptions struct {
	WorkerID     string
	Accepted     bool
	SeenEventIDs []string
}

type Hub struct {
	mu         sync.Mutex
	capacity   int
	reserved   int
	active     int
	workers    map[string]*workerState
	sessions   map[string]*session
	retired    map[string]terminalReceipt
	order      []string
	maxRetired int
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

func (hub *Hub) Register(workerID string) (*Worker, error) {
	if workerID == "" {
		return nil, errors.New("worker id is required")
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if _, exists := hub.workers[workerID]; exists {
		return nil, fmt.Errorf("worker %q is already registered", workerID)
	}
	state := &workerState{queue: make(chan completion.Assignment, hub.capacity)}
	hub.workers[workerID] = state
	for _, active := range hub.sessions {
		if active.workerID == "" && !active.terminal {
			active.workerID = workerID
			active.assignment.LeaseOwner = workerID
		}
		if active.workerID == workerID && !active.terminal {
			state.queue <- active.assignment
		}
	}
	return &Worker{ID: workerID, Assignments: state.queue, hub: hub}, nil
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
	session := &session{
		workerID: reservation.workerID, assignment: assignment,
		events: make(chan *Delivery, 32), seen: make(map[string]struct{}), inflight: make(map[string]*Delivery),
	}
	hub.sessions[key] = session
	hub.active++
	state.queue <- assignment
	return session.events, nil
}

// Restore reconstructs an incomplete live session without requiring its
// worker to be online yet. Restored sessions count against capacity because
// they were admitted durably before the restart.
func (hub *Hub) Restore(assignment completion.Assignment, options RestoreOptions) (<-chan *Delivery, error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.active+hub.reserved >= hub.capacity {
		return nil, ErrCapacity
	}
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
	restored := &session{
		workerID: workerID, assignment: assignment,
		events: make(chan *Delivery, 32), accepted: options.Accepted,
		seen: make(map[string]struct{}), inflight: make(map[string]*Delivery),
	}
	for _, id := range options.SeenEventIDs {
		if id != "" {
			restored.seen[id] = struct{}{}
		}
	}
	hub.sessions[key] = restored
	hub.active++
	if state, online := hub.workers[workerID]; online {
		state.queue <- assignment
	}
	return restored.events, nil
}

func (hub *Hub) Publish(ctx context.Context, callerID, idempotencyKey string, event completion.Event) error {
	key := callerID + "\x00" + idempotencyKey
	hub.mu.Lock()
	session, exists := hub.sessions[key]
	if !exists {
		if receipt, retired := hub.retired[key]; retired && event.ID != "" && event.ID == receipt.eventID {
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
		hub.mu.Unlock()
		return ErrSessionMissing
	}
	if event.ID != "" {
		if _, duplicate := session.seen[event.ID]; duplicate {
			hub.mu.Unlock()
			return nil
		}
		if pending, duplicate := session.inflight[event.ID]; duplicate {
			hub.mu.Unlock()
			return pending.wait()
		}
	}
	if session.terminal || session.terminalPending {
		if event.ID != "" {
			delete(session.inflight, event.ID)
		}
		hub.mu.Unlock()
		return ErrSessionMissing
	}
	if event.Type == completion.EventAccepted {
		if event.WorkerID == "" {
			event.WorkerID = session.workerID
		} else if event.WorkerID != session.workerID {
			hub.mu.Unlock()
			return errors.New("assignment accepted by the wrong worker")
		}
		if session.accepted {
			if event.ID != "" {
				delete(session.inflight, event.ID)
				session.seen[event.ID] = struct{}{}
			}
			hub.mu.Unlock()
			return nil
		}
	}
	terminal := event.EndsResponse()
	if terminal {
		session.terminalPending = true
	}
	delivery := &Delivery{Event: event, done: make(chan struct{})}
	if event.ID != "" {
		session.inflight[event.ID] = delivery
	}
	events := session.events
	hub.mu.Unlock()

	select {
	case events <- delivery:
	case <-ctx.Done():
		delivery.Commit(ctx.Err())
		hub.mu.Lock()
		if event.ID != "" {
			delete(session.inflight, event.ID)
		}
		if terminal {
			session.terminalPending = false
		}
		hub.mu.Unlock()
		return ctx.Err()
	}

	// Once delivered, wait for durable processing even if the WebSocket
	// context is canceled. The consumer owns completion of every delivery.
	err := delivery.wait()
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if event.ID != "" {
		delete(session.inflight, event.ID)
		if err == nil {
			session.seen[event.ID] = struct{}{}
		}
	}
	if err != nil {
		if terminal {
			session.terminalPending = false
		}
		return err
	}
	if event.Type == completion.EventAccepted {
		session.accepted = true
	}
	if terminal {
		session.terminalPending = false
		if !session.terminal {
			session.terminal = true
			hub.active--
		}
		// The durable response log and worker-event receipt are the replay
		// oracles once Commit(nil) returns. Retaining the live session here
		// would keep the full canonical request and event maps forever.
		hub.rememberTerminal(key, event)
		delete(hub.sessions, key)
	}
	return nil
}

func (hub *Hub) rememberTerminal(key string, event completion.Event) {
	if event.ID == "" {
		return
	}
	digest, err := terminalEventDigest(event)
	if err != nil {
		return
	}
	if _, exists := hub.retired[key]; exists {
		hub.retired[key] = terminalReceipt{eventID: event.ID, digest: digest}
		return
	}
	hub.retired[key] = terminalReceipt{eventID: event.ID, digest: digest}
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
	session.terminal = true
	hub.active--
	delete(hub.sessions, key)
	return nil
}

func (hub *Hub) unregister(workerID string) {
	hub.mu.Lock()
	state, exists := hub.workers[workerID]
	if !exists {
		hub.mu.Unlock()
		return
	}
	delete(hub.workers, workerID)
	close(state.queue)
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
