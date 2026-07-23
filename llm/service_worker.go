package llm

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/vibe-agi/human/observe"
)

type serviceWorkerConnection struct {
	service    *Service
	principal  AuthenticatedWorker
	ctx        context.Context
	cancel     context.CancelCauseFunc
	done       chan struct{}
	deliveries chan WorkerAssignmentDelivery
	notices    chan WorkerNotice
	wake       chan struct{}

	mu       sync.Mutex
	commitMu sync.Mutex
	sent     map[WorkerDeliveryID]struct{}
	acked    map[WorkerDeliveryID]struct{}
	err      error
	stopOnce sync.Once
}

var _ WorkerConnection = (*serviceWorkerConnection)(nil)
var _ WorkerNoticer = (*serviceWorkerConnection)(nil)

func (service *Service) OpenWorker(ctx context.Context, principal AuthenticatedWorker) (WorkerConnection, error) {
	end, err := service.beginOperation()
	if err != nil {
		return nil, err
	}
	defer end()
	if ctx == nil {
		return nil, ErrWorkerPrincipal
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := principal.Validate(); err != nil {
		return nil, err
	}
	lifecycle, cancel := context.WithCancelCause(context.Background())
	connection := &serviceWorkerConnection{
		service: service, principal: principal, ctx: lifecycle, cancel: cancel,
		done: make(chan struct{}), deliveries: make(chan WorkerAssignmentDelivery, service.assignmentBuffer),
		notices: make(chan WorkerNotice, 8),
		wake:    make(chan struct{}, 1), sent: make(map[WorkerDeliveryID]struct{}),
		acked: make(map[WorkerDeliveryID]struct{}),
	}
	service.mu.Lock()
	if service.closing {
		service.mu.Unlock()
		cancel(ErrServiceClosed)
		return nil, ErrServiceClosed
	}
	if _, exists := service.connections[principal.WorkerID]; exists {
		service.mu.Unlock()
		cancel(ErrWorkerConnectionConflict)
		return nil, ErrWorkerConnectionConflict
	}
	service.connections[principal.WorkerID] = connection
	service.mu.Unlock()
	go connection.dispatch()
	connection.signal()
	observe.Emit(service.observer, observe.Event{
		Kind: observe.KindWorkerConnected, Worker: string(principal.WorkerID),
	})
	return connection, nil
}

func (connection *serviceWorkerConnection) Principal() AuthenticatedWorker {
	return connection.principal
}

func (connection *serviceWorkerConnection) Assignments() <-chan WorkerAssignmentDelivery {
	return connection.deliveries
}

// Notices implements the optional WorkerNoticer capability: transport-level
// alerts (e.g. a caller disconnect) the human should see. The channel is never
// closed — consumers stop via Done() — so a late emit can never send on a
// closed channel.
func (connection *serviceWorkerConnection) Notices() <-chan WorkerNotice {
	return connection.notices
}

// emitNotice offers an advisory notice without ever blocking: a full buffer or
// a stopped connection simply drops it (a UI hint, not durable state).
func (connection *serviceWorkerConnection) emitNotice(notice WorkerNotice) {
	select {
	case connection.notices <- notice:
	case <-connection.ctx.Done():
	default:
	}
}

func (connection *serviceWorkerConnection) Done() <-chan struct{} { return connection.done }

func (connection *serviceWorkerConnection) Err() error {
	select {
	case <-connection.done:
		connection.mu.Lock()
		defer connection.mu.Unlock()
		return connection.err
	default:
		return nil
	}
}

func (connection *serviceWorkerConnection) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("llm: worker shutdown context is required")
	}
	connection.stop(nil)
	select {
	case <-connection.done:
		return connection.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (connection *serviceWorkerConnection) stop(cause error) {
	connection.stopOnce.Do(func() {
		connection.mu.Lock()
		connection.err = cause
		connection.mu.Unlock()
		connection.service.mu.Lock()
		if connection.service.connections[connection.principal.WorkerID] == connection {
			delete(connection.service.connections, connection.principal.WorkerID)
		}
		connection.service.mu.Unlock()
		connection.cancel(cause)
		observe.Emit(connection.service.observer, observe.Event{
			Kind: observe.KindWorkerDisconnected, Worker: string(connection.principal.WorkerID), Err: cause,
		})
	})
}

func (connection *serviceWorkerConnection) dispatch() {
	defer close(connection.done)
	defer close(connection.deliveries)
	for {
		delivery, ok := connection.nextDelivery()
		if ok {
			connection.mu.Lock()
			connection.sent[delivery.ID] = struct{}{}
			connection.mu.Unlock()
			select {
			case connection.deliveries <- CloneWorkerAssignmentDelivery(delivery):
				continue
			case <-connection.ctx.Done():
				return
			}
		}
		select {
		case <-connection.wake:
		case <-connection.ctx.Done():
			return
		}
	}
}

func (connection *serviceWorkerConnection) nextDelivery() (WorkerAssignmentDelivery, bool) {
	connection.service.mu.Lock()
	defer connection.service.mu.Unlock()
	pending := connection.service.pending[connection.principal.WorkerID]
	if len(pending) == 0 {
		return WorkerAssignmentDelivery{}, false
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	ordered := make([]*assignmentState, 0, len(pending))
	for _, assignment := range pending {
		ordered = append(ordered, assignment)
	}
	sort.Slice(ordered, func(left, right int) bool {
		if !ordered[left].createdAt.Equal(ordered[right].createdAt) {
			return ordered[left].createdAt.Before(ordered[right].createdAt)
		}
		if ordered[left].request.Caller != ordered[right].request.Caller {
			return ordered[left].request.Caller < ordered[right].request.Caller
		}
		return ordered[left].request.IdempotencyKey < ordered[right].request.IdempotencyKey
	})
	for _, assignment := range ordered {
		id := assignment.delivery.ID
		if _, sent := connection.sent[id]; sent {
			continue
		}
		return assignment.delivery, true
	}
	return WorkerAssignmentDelivery{}, false
}

func (connection *serviceWorkerConnection) signal() {
	select {
	case connection.wake <- struct{}{}:
	default:
	}
}

func (connection *serviceWorkerConnection) AckAssignment(ctx context.Context, delivery WorkerDeliveryID) error {
	return connection.service.ackAssignment(ctx, connection, delivery)
}

func (connection *serviceWorkerConnection) CommitEvent(
	ctx context.Context,
	delivery WorkerEventDelivery,
) (WorkerEventReceipt, error) {
	connection.commitMu.Lock()
	defer connection.commitMu.Unlock()
	receipt, err := connection.service.commitWorkerEvent(ctx, connection, delivery)
	if err == nil {
		detail := "ack"
		if receipt.Decision == WorkerEventNACK {
			detail = string(receipt.Code)
		}
		observe.Emit(connection.service.observer, observe.Event{
			Kind:   observe.KindWorkerEventSettled,
			Caller: string(delivery.Identity.CallerID), Task: string(delivery.Identity.TaskID),
			Worker: string(connection.principal.WorkerID), Detail: detail,
		})
	}
	return receipt, err
}

func (service *Service) completeAssignment(
	connection *serviceWorkerConnection,
	requestID string,
	lease WorkerLeaseID,
) {
	id := stableAssignmentDeliveryID(requestID, lease)
	workerID := connection.principal.WorkerID
	service.mu.Lock()
	delete(service.assignments, id)
	delete(service.pending[workerID], id)
	active := service.connections[workerID]
	service.mu.Unlock()
	// Internal host decisions (for example RunExpiry) use a synthetic
	// connection bearing the durable lease owner. Clear delivery bookkeeping on
	// the actual live connection as well, so an expired assignment cannot leak
	// in its sent/acked maps or be re-delivered.
	for _, target := range []*serviceWorkerConnection{connection, active} {
		if target == nil {
			continue
		}
		target.mu.Lock()
		delete(target.sent, id)
		delete(target.acked, id)
		target.mu.Unlock()
		if target == active {
			target.signal()
		}
		if target == connection && target == active {
			break
		}
	}
}
