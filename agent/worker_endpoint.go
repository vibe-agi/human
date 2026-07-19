package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	defaultWorkerPollInterval       = 500 * time.Millisecond
	defaultWorkerRedeliveryInterval = 5 * time.Second
	defaultWorkerAssignmentBuffer   = 64
	maximumWorkerAssignmentBuffer   = 4096
	workerAssignmentTombstoneLimit  = 4096
	workerEventReceiptLimit         = 4096
)

var ErrWorkerEndpointConfig = errors.New("invalid Agent worker endpoint configuration")

// WorkerEndpointConfig controls the official in-process bridge between an
// Agent core and any WorkerTransport. PollInterval discovers newly submitted
// tasks and caller-side Task updates. RedeliveryInterval repeats an assignment
// until the transport reports that the remote worker durably stored it.
type WorkerEndpointConfig struct {
	PollInterval       time.Duration
	RedeliveryInterval time.Duration
	AssignmentBuffer   int
}

func DefaultWorkerEndpointConfig() WorkerEndpointConfig {
	return WorkerEndpointConfig{
		PollInterval:       defaultWorkerPollInterval,
		RedeliveryInterval: defaultWorkerRedeliveryInterval,
		AssignmentBuffer:   defaultWorkerAssignmentBuffer,
	}
}

func (config WorkerEndpointConfig) withDefaults() (WorkerEndpointConfig, error) {
	if config.PollInterval == 0 {
		config.PollInterval = defaultWorkerPollInterval
	}
	if config.RedeliveryInterval == 0 {
		config.RedeliveryInterval = defaultWorkerRedeliveryInterval
	}
	if config.AssignmentBuffer == 0 {
		config.AssignmentBuffer = defaultWorkerAssignmentBuffer
	}
	if config.PollInterval < time.Millisecond || config.PollInterval > 10*time.Minute {
		return WorkerEndpointConfig{}, fmt.Errorf(
			"%w: poll interval must be between 1ms and 10m", ErrWorkerEndpointConfig,
		)
	}
	if config.RedeliveryInterval < config.PollInterval || config.RedeliveryInterval > time.Hour {
		return WorkerEndpointConfig{}, fmt.Errorf(
			"%w: redelivery interval must be between poll interval and 1h", ErrWorkerEndpointConfig,
		)
	}
	if config.AssignmentBuffer < 1 || config.AssignmentBuffer > maximumWorkerAssignmentBuffer {
		return WorkerEndpointConfig{}, fmt.Errorf(
			"%w: assignment buffer must be in 1..%d",
			ErrWorkerEndpointConfig, maximumWorkerAssignmentBuffer,
		)
	}
	return config, nil
}

// AgentWorkerEndpoint is the official transport-neutral WorkerEndpoint. It
// borrows Agent: neither the endpoint nor any connection shuts the Agent down.
// One live session is allowed per authenticated authority/worker pair. A second
// session is rejected with ErrWorkerConnectionConflict instead of silently
// superseding the first and causing a reconnect fight.
type AgentWorkerEndpoint struct {
	agent  *Agent
	config WorkerEndpointConfig

	mu     sync.Mutex
	active map[workerEndpointKey]*agentWorkerConnection
}

type workerEndpointKey struct {
	authority AuthorityID
	worker    WorkerID
}

// NewWorkerEndpoint validates and borrows service. ctx bounds construction
// only; successful connections own independent Runtime lifecycles.
func NewWorkerEndpoint(
	ctx context.Context,
	service *Agent,
	config WorkerEndpointConfig,
) (*AgentWorkerEndpoint, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrWorkerEndpointConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	if service == nil {
		return nil, fmt.Errorf("%w: Agent is required", ErrWorkerEndpointConfig)
	}
	_, release, err := service.acquireStore()
	if err != nil {
		return nil, fmt.Errorf("%w: Agent is unavailable: %v", ErrWorkerEndpointConfig, err)
	}
	release()
	return &AgentWorkerEndpoint{
		agent: service, config: config,
		active: make(map[workerEndpointKey]*agentWorkerConnection),
	}, nil
}

var _ WorkerEndpoint = (*AgentWorkerEndpoint)(nil)

func (endpoint *AgentWorkerEndpoint) OpenWorker(
	ctx context.Context,
	principal AuthenticatedWorker,
) (WorkerConnection, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrWorkerPrincipal)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if endpoint == nil || endpoint.agent == nil {
		return nil, ErrWorkerTransportClosed
	}
	if err := principal.Validate(); err != nil {
		return nil, err
	}
	if _, release, err := endpoint.agent.acquireStore(); err != nil {
		return nil, &WorkerConnectionError{Principal: principal, Cause: errors.Join(ErrWorkerConnectionClosed, err)}
	} else {
		release()
	}

	key := workerEndpointKey{authority: principal.Authority, worker: principal.Worker}
	connection := newAgentWorkerConnection(endpoint, key, principal)
	endpoint.mu.Lock()
	if current := endpoint.active[key]; current != nil {
		select {
		case <-current.Done():
			delete(endpoint.active, key)
		default:
			endpoint.mu.Unlock()
			return nil, &WorkerConnectionError{
				Principal: principal,
				Cause:     ErrWorkerConnectionConflict,
			}
		}
	}
	endpoint.active[key] = connection
	endpoint.mu.Unlock()
	go connection.run()
	return connection, nil
}

func (endpoint *AgentWorkerEndpoint) remove(
	key workerEndpointKey,
	connection *agentWorkerConnection,
) {
	endpoint.mu.Lock()
	if endpoint.active[key] == connection {
		delete(endpoint.active, key)
	}
	endpoint.mu.Unlock()
}

type agentWorkerConnection struct {
	endpoint  *AgentWorkerEndpoint
	key       workerEndpointKey
	principal AuthenticatedWorker
	ctx       context.Context
	cancel    context.CancelFunc

	assignments chan WorkerAssignmentDelivery
	wake        chan struct{}
	done        chan struct{}
	stopOnce    sync.Once
	drainOnce   sync.Once

	lifecycleMu sync.Mutex
	closing     bool
	active      uint64
	drained     chan struct{}

	errMu sync.RWMutex
	err   error

	assignmentMu sync.Mutex
	assignment   map[WorkerDeliveryID]*workerAssignmentState
	retired      map[WorkerDeliveryID]struct{}
	retiredOrder []WorkerDeliveryID

	commitMu     sync.Mutex
	receipts     map[WorkerDeliveryID]workerEventReceiptState
	receiptOrder []WorkerDeliveryID
	claimID      CommandID
}

type workerAssignmentState struct {
	delivery WorkerAssignmentDelivery
	lastSent time.Time
	acked    bool
}

type workerEventReceiptState struct {
	digest  [sha256.Size]byte
	receipt WorkerEventReceipt
}

func newAgentWorkerConnection(
	endpoint *AgentWorkerEndpoint,
	key workerEndpointKey,
	principal AuthenticatedWorker,
) *agentWorkerConnection {
	ctx, cancel := context.WithCancel(context.Background())
	return &agentWorkerConnection{
		endpoint: endpoint, key: key, principal: principal,
		ctx: ctx, cancel: cancel,
		assignments: make(chan WorkerAssignmentDelivery, endpoint.config.AssignmentBuffer),
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
		drained:     make(chan struct{}),
		assignment:  make(map[WorkerDeliveryID]*workerAssignmentState),
		retired:     make(map[WorkerDeliveryID]struct{}),
		receipts:    make(map[WorkerDeliveryID]workerEventReceiptState),
	}
}

var _ WorkerConnection = (*agentWorkerConnection)(nil)

func (connection *agentWorkerConnection) Principal() AuthenticatedWorker {
	if connection == nil {
		return AuthenticatedWorker{}
	}
	return connection.principal
}

func (connection *agentWorkerConnection) Assignments() <-chan WorkerAssignmentDelivery {
	if connection == nil {
		closed := make(chan WorkerAssignmentDelivery)
		close(closed)
		return closed
	}
	return connection.assignments
}

func (connection *agentWorkerConnection) Done() <-chan struct{} {
	if connection == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return connection.done
}

func (connection *agentWorkerConnection) Err() error {
	if connection == nil {
		return nil
	}
	select {
	case <-connection.done:
		connection.errMu.RLock()
		defer connection.errMu.RUnlock()
		return connection.err
	default:
		return nil
	}
}

func (connection *agentWorkerConnection) Shutdown(ctx context.Context) error {
	if connection == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidArgument)
	}
	connection.beginShutdown()
	select {
	case <-connection.done:
		return connection.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (connection *agentWorkerConnection) run() {
	terminal := connection.loop()
	if terminal != nil && !errors.Is(terminal, context.Canceled) {
		connection.errMu.Lock()
		connection.err = &WorkerConnectionError{
			Principal: connection.principal,
			Cause:     terminal,
		}
		connection.errMu.Unlock()
	}
	connection.beginShutdown()
	close(connection.assignments)
	<-connection.drained
	connection.endpoint.remove(connection.key, connection)
	close(connection.done)
}

func (connection *agentWorkerConnection) beginShutdown() {
	connection.stopOnce.Do(func() {
		connection.lifecycleMu.Lock()
		connection.closing = true
		if connection.active == 0 {
			connection.drainOnce.Do(func() { close(connection.drained) })
		}
		connection.lifecycleMu.Unlock()
		connection.cancel()
	})
}

func (connection *agentWorkerConnection) beginOperation() error {
	connection.lifecycleMu.Lock()
	defer connection.lifecycleMu.Unlock()
	if connection.closing {
		return ErrWorkerConnectionClosed
	}
	connection.active++
	return nil
}

func (connection *agentWorkerConnection) endOperation() {
	connection.lifecycleMu.Lock()
	if connection.active > 0 {
		connection.active--
	}
	if connection.closing && connection.active == 0 {
		connection.drainOnce.Do(func() { close(connection.drained) })
	}
	connection.lifecycleMu.Unlock()
}

func (connection *agentWorkerConnection) loop() error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-connection.ctx.Done():
			return nil
		case <-connection.wake:
		case <-timer.C:
		}

		if err := connection.synchronizeAssignments(); connection.fatalBackgroundError(err) {
			return err
		}
		if err := connection.claimTask(); connection.fatalBackgroundError(err) {
			return err
		}
		timer.Reset(connection.endpoint.config.PollInterval)
	}
}

func (connection *agentWorkerConnection) fatalBackgroundError(err error) bool {
	if err == nil {
		return false
	}
	if connection.ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	return errors.Is(err, ErrClosed) || errors.Is(err, ErrCorruptStore) ||
		errors.Is(err, ErrInvalidArgument) || errors.Is(err, ErrIdempotencyConflict)
}

func (connection *agentWorkerConnection) synchronizeAssignments() error {
	current := make(map[WorkerDeliveryID]struct{})
	var after *LeasePageCursor
	for {
		page, err := connection.endpoint.agent.ListLeases(
			connection.ctx,
			connection.principal.Authority,
			connection.principal.Worker,
			LeasePageRequest{After: after, Limit: MaxPageSize},
		)
		if err != nil {
			return err
		}
		for _, assignment := range page.Items {
			delivery, err := newWorkerAssignmentDelivery(assignment)
			if err != nil {
				return err
			}
			current[delivery.ID] = struct{}{}
			if err := connection.publishAssignment(delivery, false); err != nil {
				return err
			}
		}
		if !page.HasMore {
			break
		}
		if page.Next == nil {
			return fmt.Errorf("%w: Agent lease page omitted continuation", ErrCorruptStore)
		}
		next := *page.Next
		after = &next
	}
	connection.retireAssignmentsExcept(current)
	return nil
}

func (connection *agentWorkerConnection) claimTask() error {
	if connection.claimID == "" {
		id, err := newWorkerEndpointCommandID("claim_")
		if err != nil {
			return err
		}
		connection.claimID = id
	}
	command := ClaimLeaseCommand{
		ID:        connection.claimID,
		Authority: connection.principal.Authority,
		Worker:    connection.principal.Worker,
	}
	assignment, err := connection.endpoint.agent.ClaimLease(connection.ctx, command)
	if err != nil {
		if errors.Is(err, ErrLeaseNotFound) || errors.Is(err, ErrLeaseUnavailable) {
			return nil
		}
		return err
	}
	connection.claimID = ""
	delivery, err := newWorkerAssignmentDelivery(assignment)
	if err != nil {
		return err
	}
	return connection.publishAssignment(delivery, true)
}

func (connection *agentWorkerConnection) publishAssignment(
	delivery WorkerAssignmentDelivery,
	force bool,
) error {
	if err := delivery.ValidateFor(connection.principal); err != nil {
		return err
	}
	now := time.Now()
	connection.assignmentMu.Lock()
	state := connection.assignment[delivery.ID]
	if state == nil {
		state = &workerAssignmentState{delivery: CloneWorkerAssignmentDelivery(delivery)}
		connection.assignment[delivery.ID] = state
	} else if !reflect.DeepEqual(state.delivery, delivery) {
		connection.assignmentMu.Unlock()
		return &WorkerDeliveryError{Delivery: delivery.ID, Cause: ErrWorkerDeliveryConflict}
	}
	if state.acked || (!force && !state.lastSent.IsZero() && now.Sub(state.lastSent) < connection.endpoint.config.RedeliveryInterval) {
		connection.assignmentMu.Unlock()
		return nil
	}
	state.lastSent = now
	queued := CloneWorkerAssignmentDelivery(state.delivery)
	connection.assignmentMu.Unlock()

	select {
	case connection.assignments <- queued:
		return nil
	case <-connection.ctx.Done():
		return connection.ctx.Err()
	}
}

func (connection *agentWorkerConnection) AckAssignment(
	ctx context.Context,
	deliveryID WorkerDeliveryID,
) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrWorkerDelivery)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateStable("worker delivery id", string(deliveryID)); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerDelivery, err)
	}
	if err := connection.beginOperation(); err != nil {
		return err
	}
	defer connection.endOperation()
	connection.assignmentMu.Lock()
	defer connection.assignmentMu.Unlock()
	if state := connection.assignment[deliveryID]; state != nil {
		state.acked = true
		return nil
	}
	if _, ok := connection.retired[deliveryID]; ok {
		return nil
	}
	return &WorkerDeliveryError{Delivery: deliveryID, Cause: ErrWorkerDeliveryNotFound}
}

func (connection *agentWorkerConnection) retireAssignmentsExcept(
	current map[WorkerDeliveryID]struct{},
) {
	connection.assignmentMu.Lock()
	defer connection.assignmentMu.Unlock()
	for id := range connection.assignment {
		if _, ok := current[id]; ok {
			continue
		}
		delete(connection.assignment, id)
		connection.rememberRetiredLocked(id)
	}
}

func (connection *agentWorkerConnection) retireTask(ref TaskRef, fence LeaseFence) {
	connection.assignmentMu.Lock()
	defer connection.assignmentMu.Unlock()
	for id, state := range connection.assignment {
		grant := state.delivery.Assignment.Grant
		if grant.Task == ref && grant.Fence == fence {
			delete(connection.assignment, id)
			connection.rememberRetiredLocked(id)
		}
	}
}

func (connection *agentWorkerConnection) rememberRetiredLocked(id WorkerDeliveryID) {
	if _, exists := connection.retired[id]; exists {
		return
	}
	connection.retired[id] = struct{}{}
	connection.retiredOrder = append(connection.retiredOrder, id)
	if len(connection.retiredOrder) <= workerAssignmentTombstoneLimit {
		return
	}
	oldest := connection.retiredOrder[0]
	connection.retiredOrder[0] = ""
	connection.retiredOrder = connection.retiredOrder[1:]
	delete(connection.retired, oldest)
}

func (connection *agentWorkerConnection) CommitEvent(
	ctx context.Context,
	delivery WorkerEventDelivery,
) (WorkerEventReceipt, error) {
	if ctx == nil {
		return WorkerEventReceipt{}, fmt.Errorf("%w: context is required", ErrWorkerDelivery)
	}
	if err := ctx.Err(); err != nil {
		return WorkerEventReceipt{}, err
	}
	if err := connection.beginOperation(); err != nil {
		return WorkerEventReceipt{}, err
	}
	defer connection.endOperation()
	delivery = CloneWorkerEventDelivery(delivery)
	digest, err := workerEventDeliveryDigest(delivery)
	if err != nil {
		return WorkerEventReceipt{}, &WorkerDeliveryError{
			Delivery: delivery.ID, Event: delivery.Event.ID, Cause: err,
		}
	}

	connection.commitMu.Lock()
	defer connection.commitMu.Unlock()
	if previous, ok := connection.receipts[delivery.ID]; ok {
		if previous.digest == digest {
			return previous.receipt, nil
		}
		return workerNACK(delivery, WorkerRejectCommandConflict, "delivery id was reused with different input"), nil
	}

	if err := delivery.Validate(); err != nil {
		receipt := workerNACK(delivery, WorkerRejectInvalid, "worker event is invalid")
		connection.rememberEventReceipt(delivery.ID, digest, receipt)
		return receipt, nil
	}
	if delivery.Event.Task.Workspace.Authority != connection.principal.Authority {
		receipt := workerNACK(delivery, WorkerRejectForbidden, "worker event belongs to another authority")
		connection.rememberEventReceipt(delivery.ID, digest, receipt)
		return receipt, nil
	}

	result, err := connection.applyEvent(ctx, delivery.Event)
	if err == nil {
		receipt := WorkerEventReceipt{
			Delivery: delivery.ID,
			Event:    delivery.Event.ID,
			Decision: WorkerEventACK,
		}
		connection.rememberEventReceipt(delivery.ID, digest, receipt)
		if result.State.Terminal() {
			connection.retireTask(delivery.Event.Task, delivery.Event.Fence)
		}
		connection.wakeAssignments()
		return receipt, nil
	}
	if errors.Is(err, ErrStoreCommitUnknown) {
		return WorkerEventReceipt{}, &WorkerDeliveryError{
			Delivery: delivery.ID,
			Event:    delivery.Event.ID,
			Cause:    errors.Join(ErrWorkerDeliveryIndeterminate, err),
		}
	}
	if code, message, rejected := classifyWorkerEventRejection(err); rejected {
		receipt := workerNACK(delivery, code, message)
		connection.rememberEventReceipt(delivery.ID, digest, receipt)
		connection.wakeAssignments()
		return receipt, nil
	}
	return WorkerEventReceipt{}, &WorkerDeliveryError{
		Delivery: delivery.ID,
		Event:    delivery.Event.ID,
		Cause:    err,
	}
}

func (connection *agentWorkerConnection) rememberEventReceipt(
	id WorkerDeliveryID,
	digest [sha256.Size]byte,
	receipt WorkerEventReceipt,
) {
	if _, exists := connection.receipts[id]; !exists {
		connection.receiptOrder = append(connection.receiptOrder, id)
	}
	connection.receipts[id] = workerEventReceiptState{digest: digest, receipt: receipt}
	if len(connection.receiptOrder) <= workerEventReceiptLimit {
		return
	}
	oldest := connection.receiptOrder[0]
	connection.receiptOrder[0] = ""
	connection.receiptOrder = connection.receiptOrder[1:]
	delete(connection.receipts, oldest)
}

func (connection *agentWorkerConnection) applyEvent(
	ctx context.Context,
	event WorkerEvent,
) (Task, error) {
	grant := LeaseGrant{
		Task: event.Task, Worker: connection.principal.Worker, Fence: event.Fence,
	}
	meta := WorkerCommandMeta{
		ID: event.ID, ExpectedRevision: event.ExpectedRevision, Grant: grant,
	}
	switch event.Kind {
	case WorkerEventAcceptTask:
		return connection.endpoint.agent.AcceptTask(ctx, WorkerTaskCommand{Meta: meta, Task: event.Task})
	case WorkerEventRejectTask:
		return connection.endpoint.agent.RejectTask(ctx, WorkerTaskCommand{Meta: meta, Task: event.Task})
	case WorkerEventRequestInput:
		return connection.endpoint.agent.RequestInput(ctx, WorkerMessageCommand{
			Meta: meta, Task: event.Task, Message: *event.Message,
		})
	case WorkerEventFailTask:
		return connection.endpoint.agent.FailTask(ctx, WorkerTaskCommand{Meta: meta, Task: event.Task})
	case WorkerEventFreezeArtifact:
		frozen, err := connection.endpoint.agent.FreezeArtifact(ctx, FreezeArtifactCommand{
			Meta: meta, Task: event.Task,
			Artifact:             event.Freeze.Artifact,
			ExpectedBaseRevision: event.Freeze.ExpectedBaseRevision,
			Payload:              event.Freeze.Payload,
		})
		return frozen.Task, err
	case WorkerEventCompleteTask:
		return connection.endpoint.agent.CompleteTask(ctx, CompleteTaskCommand{
			Meta: meta, Task: event.Task,
			Submission: event.Submission,
			Message:    *event.Message,
			Artifact:   event.Artifact,
		})
	default:
		return Task{}, ErrInvalidArgument
	}
}

func (connection *agentWorkerConnection) wakeAssignments() {
	select {
	case connection.wake <- struct{}{}:
	default:
	}
}

func classifyWorkerEventRejection(err error) (WorkerRejectionCode, string, bool) {
	switch {
	case errors.Is(err, ErrInvalidArgument):
		return WorkerRejectInvalid, "worker event is invalid", true
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrArtifactNotFound),
		errors.Is(err, ErrLeaseNotFound), errors.Is(err, ErrWorkspaceNotFound):
		return WorkerRejectNotFound, "worker event target was not found", true
	case errors.Is(err, ErrStaleLease):
		return WorkerRejectStaleLease, "worker lease generation is stale", true
	case errors.Is(err, ErrRevisionConflict):
		return WorkerRejectRevision, "task revision does not match", true
	case errors.Is(err, ErrIdempotencyConflict), errors.Is(err, ErrTaskConflict),
		errors.Is(err, ErrMessageConflict), errors.Is(err, ErrSubmissionConflict),
		errors.Is(err, ErrReceiptConflict):
		return WorkerRejectCommandConflict, "command identity conflicts with committed input", true
	case errors.Is(err, ErrInvalidTransition), errors.Is(err, ErrTerminalTask),
		errors.Is(err, ErrArtifactConflict), errors.Is(err, ErrArtifactState),
		errors.Is(err, ErrWorkspaceConflict), errors.Is(err, ErrLeaseUnavailable),
		errors.Is(err, ErrLeaseFenceExhausted):
		return WorkerRejectState, "task state does not allow this worker event", true
	default:
		return "", "", false
	}
}

func workerNACK(
	delivery WorkerEventDelivery,
	code WorkerRejectionCode,
	message string,
) WorkerEventReceipt {
	return WorkerEventReceipt{
		Delivery: delivery.ID,
		Event:    delivery.Event.ID,
		Decision: WorkerEventNACK,
		Code:     code,
		Message:  message,
	}
}

func newWorkerAssignmentDelivery(assignment LeaseAssignment) (WorkerAssignmentDelivery, error) {
	encoded, err := json.Marshal(assignment)
	if err != nil {
		return WorkerAssignmentDelivery{}, fmt.Errorf("encode Agent lease assignment identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return WorkerAssignmentDelivery{
		ID:         WorkerDeliveryID("assignment_" + hex.EncodeToString(digest[:])),
		Assignment: cloneLeaseAssignment(assignment),
	}, nil
}

func newWorkerEndpointCommandID(prefix string) (CommandID, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("allocate Agent worker command identity: %w", err)
	}
	return CommandID(prefix + hex.EncodeToString(random[:])), nil
}

func workerEventDeliveryDigest(delivery WorkerEventDelivery) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(delivery.Event)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode worker event identity: %w", err)
	}
	return sha256.Sum256(encoded), nil
}
