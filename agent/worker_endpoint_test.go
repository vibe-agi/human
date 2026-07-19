package agent

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/workspace"
)

func TestWorkerEndpointClaimsRecoversAndDoesNotFenceOnDisconnect(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef, taskRef := refs("authority-a", "context-a", "workspace-a", "task-a")
	if _, err := service.CreateTask(ctx, createCommand(
		"create-a", contextRef, taskRef, "message-a", "investigate",
	)); err != nil {
		t.Fatal(err)
	}
	endpoint := openTestWorkerEndpoint(t, service)
	principal := AuthenticatedWorker{Authority: "authority-a", Worker: "worker-a", Session: "session-a"}
	connection := openTestWorkerConnection(t, endpoint, principal)

	conflict := principal
	conflict.Session = "session-conflict"
	if _, err := endpoint.OpenWorker(ctx, conflict); !errors.Is(err, ErrWorkerConnectionConflict) {
		t.Fatalf("parallel worker session error = %v", err)
	}

	first := receiveWorkerAssignment(t, connection, taskRef, 1)
	if first.Assignment.Grant.Worker != principal.Worker {
		t.Fatalf("claimed worker = %q", first.Assignment.Grant.Worker)
	}
	grant := first.Assignment.Grant
	if err := connection.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	assertWorkerConnectionClosed(t, connection)

	stillCurrent, err := service.GetLease(ctx, taskRef)
	if err != nil {
		t.Fatalf("disconnect fenced lease: %v", err)
	}
	if stillCurrent.Grant != grant {
		t.Fatalf("lease changed on disconnect: got %#v want %#v", stillCurrent.Grant, grant)
	}

	reconnectedPrincipal := principal
	reconnectedPrincipal.Session = "session-b"
	reconnected := openTestWorkerConnection(t, endpoint, reconnectedPrincipal)
	replayed := receiveWorkerAssignment(t, reconnected, taskRef, 1)
	if replayed.ID != first.ID || !reflect.DeepEqual(replayed.Assignment, first.Assignment) {
		t.Fatalf("recovered assignment differs:\nfirst=%#v\nreplayed=%#v", first, replayed)
	}
	if err := reconnected.AckAssignment(ctx, replayed.ID); err != nil {
		t.Fatal(err)
	}
	if err := reconnected.AckAssignment(ctx, replayed.ID); err != nil {
		t.Fatalf("assignment ACK is not idempotent: %v", err)
	}
	select {
	case duplicate := <-reconnected.Assignments():
		t.Fatalf("ACKed current assignment was redelivered: %#v", duplicate)
	case <-time.After(4 * testWorkerPollInterval):
	}
	accepted := workerEventFor("reconnect-accept", WorkerEventAcceptTask, grant, 1)
	assertWorkerACK(t, reconnected, accepted)
	updated := receiveWorkerAssignment(t, reconnected, taskRef, 2)
	if updated.ID == replayed.ID {
		t.Fatal("Task revision update reused the previous assignment delivery identity")
	}
	if err := reconnected.AckAssignment(ctx, updated.ID); err != nil {
		t.Fatal(err)
	}
	if err := reconnected.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	// The endpoint borrows Agent. Closing every connection must not close it.
	_, anotherTask := refs("authority-a", "context-a", "workspace-a", "task-after-disconnect")
	if _, err := service.CreateTask(ctx, createCommand(
		"create-after-disconnect", contextRef, anotherTask, "message-after-disconnect", "still alive",
	)); err != nil {
		t.Fatalf("endpoint owned/closed borrowed Agent: %v", err)
	}
}

func TestWorkerEndpointMapsAllWorkerEventsAndReplaysCommittedACK(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef := ContextRef{Authority: "authority-events", ID: "context-events"}
	refsByName := make(map[string]TaskRef)
	grants := make(map[string]LeaseGrant)
	for _, name := range []string{"complete", "reject", "fail"} {
		ref := TaskRef{
			Workspace: WorkspaceRef{Authority: contextRef.Authority, ID: WorkspaceID("workspace-" + name)},
			ID:        TaskID("task-" + name),
		}
		if _, err := service.CreateTask(ctx, createCommand(
			"create-"+name, contextRef, ref, "message-create-"+name, "work "+name,
		)); err != nil {
			t.Fatal(err)
		}
		assignment, err := service.AcquireLease(ctx, AcquireLeaseCommand{
			ID: "lease-" + CommandID(name), Task: ref, Worker: "worker-events",
		})
		if err != nil {
			t.Fatal(err)
		}
		refsByName[name] = ref
		grants[name] = assignment.Grant
	}

	endpoint := openTestWorkerEndpoint(t, service)
	principal := AuthenticatedWorker{
		Authority: contextRef.Authority, Worker: "worker-events", Session: "session-events",
	}
	connection := openTestWorkerConnection(t, endpoint, principal)
	for range refsByName {
		assignment := receiveAnyWorkerAssignment(t, connection)
		if err := connection.AckAssignment(ctx, assignment.ID); err != nil {
			t.Fatal(err)
		}
	}

	completeRef := refsByName["complete"]
	completeGrant := grants["complete"]
	accept := workerEventFor("accept-complete", WorkerEventAcceptTask, completeGrant, 1)
	assertWorkerACK(t, connection, accept)
	// Same delivery exact replay is served from the connection receipt.
	if replay, err := connection.CommitEvent(ctx, accept); err != nil || replay.Decision != WorkerEventACK {
		t.Fatalf("same-connection ACK replay = %#v, %v", replay, err)
	}

	request := workerEventFor("request-complete", WorkerEventRequestInput, completeGrant, 2)
	request.Event.Message = workerEndpointMessage("message-request", "choose an option")
	assertWorkerACK(t, connection, request)
	if _, err := service.ReplyTask(ctx, MessageCommand{
		Meta:    CommandMeta{ID: "caller-reply", ExpectedRevision: 3},
		Task:    completeRef,
		Message: *workerEndpointMessage("message-caller-reply", "option A"),
	}); err != nil {
		t.Fatal(err)
	}

	freeze := workerEventFor("freeze-complete", WorkerEventFreezeArtifact, completeGrant, 4)
	freeze.Event.Freeze = &WorkerArtifactFreeze{
		Artifact:             "artifact-complete",
		ExpectedBaseRevision: "base-complete",
		Payload: workspace.Payload{
			MediaType: "text/plain",
			Data:      []byte("workspace patch"),
		},
	}
	assertWorkerACK(t, connection, freeze)
	artifactRef := ArtifactRef{Workspace: completeRef.Workspace, ID: "artifact-complete"}
	complete := workerEventFor("complete-complete", WorkerEventCompleteTask, completeGrant, 5)
	complete.Event.Message = workerEndpointMessage("message-complete", "finished")
	complete.Event.Submission = "submission-complete"
	complete.Event.Artifact = &artifactRef
	assertWorkerACK(t, connection, complete)

	reject := workerEventFor("reject-task", WorkerEventRejectTask, grants["reject"], 1)
	assertWorkerACK(t, connection, reject)
	acceptFail := workerEventFor("accept-fail", WorkerEventAcceptTask, grants["fail"], 1)
	assertWorkerACK(t, connection, acceptFail)
	fail := workerEventFor("fail-task", WorkerEventFailTask, grants["fail"], 2)
	assertWorkerACK(t, connection, fail)

	for name, want := range map[string]TaskState{
		"complete": TaskCompleted,
		"reject":   TaskRejected,
		"fail":     TaskFailed,
	} {
		task, err := service.GetTask(ctx, refsByName[name])
		if err != nil || task.State != want {
			t.Fatalf("%s task = %#v, %v; want %s", name, task, err, want)
		}
	}

	if err := connection.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	principal.Session = "session-events-reconnect"
	reconnected := openTestWorkerConnection(t, endpoint, principal)
	// Connection memory is gone, so this proves ACK comes from Agent's durable
	// command receipt even though the Task has advanced to terminal.
	if replay, err := reconnected.CommitEvent(ctx, accept); err != nil || replay.Decision != WorkerEventACK {
		t.Fatalf("cross-reconnect committed ACK replay = %#v, %v", replay, err)
	}
	implementation := reconnected.(*agentWorkerConnection)
	digest, err := workerEventDeliveryDigest(accept)
	if err != nil {
		t.Fatal(err)
	}
	implementation.commitMu.Lock()
	for index := 0; index <= workerEventReceiptLimit; index++ {
		id := WorkerDeliveryID("cache-fill-" + strconv.Itoa(index))
		implementation.rememberEventReceipt(id, digest, WorkerEventReceipt{
			Delivery: id, Event: accept.Event.ID, Decision: WorkerEventACK,
		})
	}
	cacheSize := len(implementation.receipts)
	_, originalStillCached := implementation.receipts[accept.ID]
	implementation.commitMu.Unlock()
	if cacheSize != workerEventReceiptLimit || originalStillCached {
		t.Fatalf("receipt cache size=%d original_cached=%t", cacheSize, originalStillCached)
	}
	if replay, err := reconnected.CommitEvent(ctx, accept); err != nil || replay.Decision != WorkerEventACK {
		t.Fatalf("durable ACK replay after cache eviction = %#v, %v", replay, err)
	}
	crossReconnectConflict := accept
	crossReconnectConflict.ID = "delivery-cross-reconnect-conflict"
	crossReconnectConflict.Event.Kind = WorkerEventFailTask
	crossReconnectConflict.Event.ExpectedRevision = 2
	assertWorkerNACK(t, reconnected, crossReconnectConflict, WorkerRejectCommandConflict)
	if err := reconnected.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerEndpointNACKsDeterministicFailuresWithoutClosingConnection(t *testing.T) {
	service, _ := openTestAgent(t)
	ctx := context.Background()
	contextRef := ContextRef{Authority: "authority-nack", ID: "context-nack"}
	createLeased := func(name string) (TaskRef, LeaseGrant) {
		t.Helper()
		ref := TaskRef{
			Workspace: WorkspaceRef{Authority: contextRef.Authority, ID: WorkspaceID("workspace-" + name)},
			ID:        TaskID("task-" + name),
		}
		if _, err := service.CreateTask(ctx, createCommand(
			"create-"+name, contextRef, ref, "message-"+name, name,
		)); err != nil {
			t.Fatal(err)
		}
		assignment, err := service.AcquireLease(ctx, AcquireLeaseCommand{
			ID: "lease-" + CommandID(name), Task: ref, Worker: "worker-nack",
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref, assignment.Grant
	}
	_, staleGrant := createLeased("stale")
	_, revisionGrant := createLeased("revision")
	_, stateGrant := createLeased("state")
	_, conflictGrant := createLeased("conflict")
	if err := service.FenceLease(ctx, FenceLeaseCommand{ID: "fence-stale", Grant: staleGrant}); err != nil {
		t.Fatal(err)
	}

	endpoint := openTestWorkerEndpoint(t, service)
	principal := AuthenticatedWorker{
		Authority: contextRef.Authority, Worker: "worker-nack", Session: "session-nack",
	}
	connection := openTestWorkerConnection(t, endpoint, principal)

	invalid := workerEventFor("invalid", WorkerEventKind("unknown"), revisionGrant, 1)
	assertWorkerNACK(t, connection, invalid, WorkerRejectInvalid)
	forbidden := invalid
	forbidden.ID = "delivery-forbidden"
	forbidden.Event.ID = "event-forbidden"
	forbidden.Event.Kind = WorkerEventAcceptTask
	forbidden.Event.Task.Workspace.Authority = "another-authority"
	assertWorkerNACK(t, connection, forbidden, WorkerRejectForbidden)
	notFoundGrant := LeaseGrant{
		Task: TaskRef{
			Workspace: WorkspaceRef{Authority: contextRef.Authority, ID: "workspace-missing"},
			ID:        "task-missing",
		},
		Worker: principal.Worker,
		Fence:  1,
	}
	missing := workerEventFor("missing", WorkerEventAcceptTask, notFoundGrant, 1)
	assertWorkerNACK(t, connection, missing, WorkerRejectNotFound)
	stale := workerEventFor("stale", WorkerEventAcceptTask, staleGrant, 1)
	assertWorkerNACK(t, connection, stale, WorkerRejectStaleLease)
	revision := workerEventFor("revision", WorkerEventAcceptTask, revisionGrant, 99)
	assertWorkerNACK(t, connection, revision, WorkerRejectRevision)
	state := workerEventFor("state", WorkerEventRequestInput, stateGrant, 1)
	state.Event.Message = workerEndpointMessage("message-state", "too early")
	assertWorkerNACK(t, connection, state, WorkerRejectState)

	accepted := workerEventFor("accepted-conflict", WorkerEventAcceptTask, conflictGrant, 1)
	assertWorkerACK(t, connection, accepted)
	commandConflict := workerEventFor("different-delivery", WorkerEventFailTask, conflictGrant, 2)
	commandConflict.Event.ID = accepted.Event.ID
	assertWorkerNACK(t, connection, commandConflict, WorkerRejectCommandConflict)
	deliveryConflict := accepted
	deliveryConflict.Event.ID = "different-event"
	deliveryConflict.Event.Kind = WorkerEventFailTask
	deliveryConflict.Event.ExpectedRevision = 2
	assertWorkerNACK(t, connection, deliveryConflict, WorkerRejectCommandConflict)

	select {
	case <-connection.Done():
		t.Fatalf("deterministic NACK closed connection: %v", connection.Err())
	default:
	}
	if err := connection.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerEndpointLeavesAmbiguousTransientAndCanceledEventsUnsettled(t *testing.T) {
	base := openSQLiteStoreBridge(t)
	faults := newWorkerEndpointFaultStore(base)
	config := DefaultConfig()
	config.Store = framework.Borrow[Store](faults)
	service, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown fault Agent: %v", err)
		}
	})

	ctx := context.Background()
	contextRef, taskRef := refs("authority-fault", "context-fault", "workspace-fault", "task-fault")
	if _, err := service.CreateTask(ctx, createCommand(
		"create-fault", contextRef, taskRef, "message-fault", "fault test",
	)); err != nil {
		t.Fatal(err)
	}
	assignment, err := service.AcquireLease(ctx, AcquireLeaseCommand{
		ID: "lease-fault", Task: taskRef, Worker: "worker-fault",
	})
	if err != nil {
		t.Fatal(err)
	}
	transientRef := TaskRef{
		Workspace: WorkspaceRef{Authority: "authority-fault", ID: "workspace-transient"},
		ID:        "task-transient",
	}
	if _, err := service.CreateTask(ctx, createCommand(
		"create-transient", contextRef, transientRef, "message-transient", "retry",
	)); err != nil {
		t.Fatal(err)
	}
	transientLease, err := service.AcquireLease(ctx, AcquireLeaseCommand{
		ID: "lease-transient", Task: transientRef, Worker: "worker-fault",
	})
	if err != nil {
		t.Fatal(err)
	}
	drainRef := TaskRef{
		Workspace: WorkspaceRef{Authority: "authority-fault", ID: "workspace-drain"},
		ID:        "task-drain",
	}
	if _, err := service.CreateTask(ctx, createCommand(
		"create-drain", contextRef, drainRef, "message-drain", "drain",
	)); err != nil {
		t.Fatal(err)
	}
	drainLease, err := service.AcquireLease(ctx, AcquireLeaseCommand{
		ID: "lease-drain", Task: drainRef, Worker: "worker-fault",
	})
	if err != nil {
		t.Fatal(err)
	}
	faults.drainUpdates()
	endpointConfig := DefaultWorkerEndpointConfig()
	endpointConfig.PollInterval = 10 * time.Minute
	endpointConfig.RedeliveryInterval = 10 * time.Minute
	endpoint, err := NewWorkerEndpoint(ctx, service, endpointConfig)
	if err != nil {
		t.Fatal(err)
	}
	connection := openTestWorkerConnection(t, endpoint, AuthenticatedWorker{
		Authority: "authority-fault", Worker: "worker-fault", Session: "session-fault",
	})
	_ = receiveWorkerAssignment(t, connection, taskRef, 1)
	// Wait until the initial no-work ClaimLease transaction has finished so the
	// next injected Update belongs deterministically to CommitEvent.
	faults.waitUpdate(t)

	event := workerEventFor("ambiguous", WorkerEventAcceptTask, assignment.Grant, 1)
	faults.failNext(&StoreCommitUnknownError{Cause: errors.New("injected commit ambiguity")})
	if receipt, err := connection.CommitEvent(ctx, event); receipt != (WorkerEventReceipt{}) ||
		!errors.Is(err, ErrWorkerDeliveryIndeterminate) || !errors.Is(err, ErrStoreCommitUnknown) {
		t.Fatalf("ambiguous commit = %#v, %v", receipt, err)
	}
	if task, err := service.GetTask(ctx, taskRef); err != nil || task.State != TaskSubmitted {
		t.Fatalf("injected pre-commit ambiguity mutated task: %#v, %v", task, err)
	}
	assertWorkerACK(t, connection, event)

	temporary := errors.New("temporary storage outage")
	faults.failNext(temporary)
	transient := workerEventFor("transient", WorkerEventAcceptTask, transientLease.Grant, 1)
	if receipt, err := connection.CommitEvent(ctx, transient); receipt != (WorkerEventReceipt{}) || !errors.Is(err, temporary) {
		t.Fatalf("transient commit = %#v, %v", receipt, err)
	}
	assertWorkerACK(t, connection, transient)

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	canceled := workerEventFor("canceled", WorkerEventFailTask, transientLease.Grant, 2)
	if receipt, err := connection.CommitEvent(canceledCtx, canceled); receipt != (WorkerEventReceipt{}) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled commit = %#v, %v", receipt, err)
	}
	assertWorkerACK(t, connection, canceled)

	// Connection shutdown stops new calls but drains an already admitted domain
	// transaction before Done closes and before the transport may report an ACK.
	drainEvent := workerEventFor("drain", WorkerEventAcceptTask, drainLease.Grant, 1)
	drainContext := context.WithValue(ctx, workerEndpointBlockContextKey{}, true)
	commitResult := make(chan struct {
		receipt WorkerEventReceipt
		err     error
	}, 1)
	go func() {
		receipt, err := connection.CommitEvent(drainContext, drainEvent)
		commitResult <- struct {
			receipt WorkerEventReceipt
			err     error
		}{receipt: receipt, err: err}
	}()
	faults.waitBlocked(t)
	short, cancelShort := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancelShort()
	if err := connection.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown did not wait for in-flight commit: %v", err)
	}
	faults.unblock()
	result := <-commitResult
	if result.err != nil || result.receipt.Decision != WorkerEventACK {
		t.Fatalf("drained commit = %#v, %v", result.receipt, result.err)
	}
	if err := connection.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerEndpointRepeatedOpenShutdownHasNoStuckSession(t *testing.T) {
	service, _ := openTestAgent(t)
	endpoint := openTestWorkerEndpoint(t, service)
	ctx := context.Background()
	for index := 0; index < 50; index++ {
		connection, err := endpoint.OpenWorker(ctx, AuthenticatedWorker{
			Authority: "authority-cycle",
			Worker:    "worker-cycle",
			Session:   WorkerSessionID("session-" + strconv.Itoa(index)),
		})
		if err != nil {
			t.Fatalf("open cycle %d: %v", index, err)
		}
		if err := connection.Shutdown(ctx); err != nil {
			t.Fatalf("shutdown cycle %d: %v", index, err)
		}
		assertWorkerConnectionClosed(t, connection)
	}
}

const testWorkerPollInterval = 5 * time.Millisecond

func openTestWorkerEndpoint(t *testing.T, service *Agent) *AgentWorkerEndpoint {
	t.Helper()
	config := DefaultWorkerEndpointConfig()
	config.PollInterval = testWorkerPollInterval
	config.RedeliveryInterval = 25 * time.Millisecond
	config.AssignmentBuffer = 64
	endpoint, err := NewWorkerEndpoint(context.Background(), service, config)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint
}

func openTestWorkerConnection(
	t *testing.T,
	endpoint *AgentWorkerEndpoint,
	principal AuthenticatedWorker,
) WorkerConnection {
	t.Helper()
	connection, err := endpoint.OpenWorker(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-connection.Done():
		default:
			if err := connection.Shutdown(context.Background()); err != nil {
				t.Errorf("shutdown worker connection: %v", err)
			}
		}
	})
	return connection
}

func receiveWorkerAssignment(
	t *testing.T,
	connection WorkerConnection,
	ref TaskRef,
	revision uint64,
) WorkerAssignmentDelivery {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case delivery, ok := <-connection.Assignments():
			if !ok {
				t.Fatalf("worker connection ended before assignment: %v", connection.Err())
			}
			if delivery.Assignment.Grant.Task == ref && delivery.Assignment.Task.Revision == revision {
				return delivery
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for Task %v revision %d", ref, revision)
		}
	}
}

func receiveAnyWorkerAssignment(t *testing.T, connection WorkerConnection) WorkerAssignmentDelivery {
	t.Helper()
	select {
	case delivery, ok := <-connection.Assignments():
		if !ok {
			t.Fatalf("worker connection ended before assignment: %v", connection.Err())
		}
		return delivery
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker assignment")
		return WorkerAssignmentDelivery{}
	}
}

func assertWorkerConnectionClosed(t *testing.T, connection WorkerConnection) {
	t.Helper()
	select {
	case <-connection.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("worker connection did not stop")
	}
	select {
	case _, ok := <-connection.Assignments():
		if ok {
			t.Fatal("assignment channel remained open after connection Done")
		}
	case <-time.After(time.Second):
		t.Fatal("assignment channel did not close")
	}
}

func workerEventFor(
	id string,
	kind WorkerEventKind,
	grant LeaseGrant,
	revision uint64,
) WorkerEventDelivery {
	return WorkerEventDelivery{
		ID: WorkerDeliveryID("delivery-" + id),
		Event: WorkerEvent{
			ID: CommandID("event-" + id), Kind: kind,
			Task: grant.Task, Fence: grant.Fence, ExpectedRevision: revision,
		},
	}
}

func workerEndpointMessage(id, text string) *MessageInput {
	return &MessageInput{
		ID:    MessageID(id),
		Parts: []Part{{MediaType: "text/plain", Data: []byte(text)}},
	}
}

func assertWorkerACK(t *testing.T, connection WorkerConnection, delivery WorkerEventDelivery) {
	t.Helper()
	receipt, err := connection.CommitEvent(context.Background(), delivery)
	if err != nil {
		t.Fatalf("commit %s: %v", delivery.Event.Kind, err)
	}
	if receipt.Decision != WorkerEventACK {
		t.Fatalf("commit %s receipt = %#v", delivery.Event.Kind, receipt)
	}
	if err := receipt.ValidateFor(delivery); err != nil {
		t.Fatalf("invalid ACK receipt: %v", err)
	}
}

func assertWorkerNACK(
	t *testing.T,
	connection WorkerConnection,
	delivery WorkerEventDelivery,
	code WorkerRejectionCode,
) {
	t.Helper()
	receipt, err := connection.CommitEvent(context.Background(), delivery)
	if err != nil {
		t.Fatalf("commit rejected %s: %v", delivery.Event.Kind, err)
	}
	if receipt.Decision != WorkerEventNACK || receipt.Code != code {
		t.Fatalf("NACK = %#v, want code %s", receipt, code)
	}
	if err := receipt.ValidateFor(delivery); err != nil {
		t.Fatalf("invalid NACK receipt: %v", err)
	}
}

type workerEndpointFaultStore struct {
	Store
	mu      sync.Mutex
	next    error
	updates chan struct{}
	blocked chan struct{}
	release chan struct{}
}

type workerEndpointBlockContextKey struct{}

func newWorkerEndpointFaultStore(store Store) *workerEndpointFaultStore {
	return &workerEndpointFaultStore{
		Store: store, updates: make(chan struct{}, 128),
		blocked: make(chan struct{}, 1), release: make(chan struct{}),
	}
}

func (store *workerEndpointFaultStore) Update(ctx context.Context, callback func(StoreTx) error) error {
	store.mu.Lock()
	failure := store.next
	store.next = nil
	store.mu.Unlock()
	select {
	case store.updates <- struct{}{}:
	default:
	}
	if failure != nil {
		return failure
	}
	if block, _ := ctx.Value(workerEndpointBlockContextKey{}).(bool); block {
		select {
		case store.blocked <- struct{}{}:
		default:
		}
		select {
		case <-store.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return store.Store.Update(ctx, callback)
}

func (store *workerEndpointFaultStore) failNext(err error) {
	store.mu.Lock()
	store.next = err
	store.mu.Unlock()
}

func (store *workerEndpointFaultStore) drainUpdates() {
	for {
		select {
		case <-store.updates:
		default:
			return
		}
	}
}

func (store *workerEndpointFaultStore) waitUpdate(t *testing.T) {
	t.Helper()
	select {
	case <-store.updates:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for endpoint background claim")
	}
}

func (store *workerEndpointFaultStore) waitBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-store.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocked endpoint commit")
	}
}

func (store *workerEndpointFaultStore) unblock() { close(store.release) }
