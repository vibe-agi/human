package agent_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	. "github.com/vibe-agi/human/agent"
	agentsqlite "github.com/vibe-agi/human/agent/sqlite"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
)

// TestAgentDurableFaultMatrix runs the same transport-neutral failure
// scenarios against the official SQLite adapter and the humantest durable-image
// model. A scenario may only depend on the public Store and WorkerEndpoint
// contracts; backend-specific setup is confined to agentFaultBackend.
func TestAgentDurableFaultMatrix(t *testing.T) {
	backends := []struct {
		name string
		open func(*testing.T) *agentFaultBackend
	}{
		{name: "memory_image", open: openMemoryAgentFaultBackend},
		{name: "sqlite", open: openSQLiteAgentFaultBackend},
	}
	scenarios := []struct {
		name string
		run  func(*testing.T, *agentFaultBackend)
	}{
		{name: "disconnect_before_assignment_ack_redelivers_exactly", run: agentFaultAssignmentRedelivery},
		{name: "committed_event_ack_loss_replays_without_duplicate", run: agentFaultEventACKReplay},
		{name: "agent_and_store_restart_recovers_lease_assignment_and_receipt", run: agentFaultStoreRestart},
		{name: "poison_nack_does_not_block_follower", run: agentFaultPoisonFollower},
	}

	for _, backendCase := range backends {
		backendCase := backendCase
		t.Run(backendCase.name, func(t *testing.T) {
			for _, scenario := range scenarios {
				scenario := scenario
				t.Run(scenario.name, func(t *testing.T) {
					backend := backendCase.open(t)
					defer backend.close(t)
					scenario.run(t, backend)
				})
			}
		})
	}
}

type agentFaultBackend struct {
	store   Store
	release framework.ReleaseFunc
	reopen  func(*testing.T) (Store, framework.ReleaseFunc)
	abandon func(*testing.T, Store)
}

func openMemoryAgentFaultBackend(t *testing.T) *agentFaultBackend {
	t.Helper()
	image := humantest.NewMemoryAgentStoreImage()
	backend := &agentFaultBackend{
		abandon: func(t *testing.T, store Store) {
			t.Helper()
			concrete, ok := store.(*humantest.MemoryAgentStore)
			if !ok {
				t.Fatalf("memory Agent Store type = %T", store)
			}
			if err := image.Abandon(concrete); err != nil {
				t.Fatalf("abandon memory Agent Store handle: %v", err)
			}
		},
		reopen: func(t *testing.T) (Store, framework.ReleaseFunc) {
			t.Helper()
			store, release, err := image.Open()
			if err != nil {
				t.Fatalf("open memory Agent Store image: %v", err)
			}
			return store, release
		},
	}
	backend.open(t)
	return backend
}

func openSQLiteAgentFaultBackend(t *testing.T) *agentFaultBackend {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent-fault.db")
	backend := &agentFaultBackend{
		reopen: func(t *testing.T) (Store, framework.ReleaseFunc) {
			t.Helper()
			resource, err := agentsqlite.Open(t.Context(), agentsqlite.Config{Path: path})
			if err != nil {
				t.Fatalf("open SQLite Agent Store: %v", err)
			}
			store, err := resource.Value()
			if err != nil {
				_ = resource.Release(context.Background())
				t.Fatalf("inspect SQLite Agent Store resource: %v", err)
			}
			return store, resource.Release
		},
	}
	backend.open(t)
	return backend
}

func (backend *agentFaultBackend) open(t *testing.T) {
	t.Helper()
	backend.store, backend.release = backend.reopen(t)
}

func (backend *agentFaultBackend) restart(t *testing.T) {
	t.Helper()
	oldStore, oldRelease := backend.store, backend.release
	if backend.abandon != nil {
		backend.abandon(t, oldStore)
	} else if err := oldRelease(context.Background()); err != nil {
		t.Fatalf("release Agent Store before restart: %v", err)
	}
	backend.store, backend.release = nil, nil
	backend.open(t)
	if backend.abandon != nil {
		// A process-loss model must also tolerate delayed cleanup for the dead
		// generation without invalidating the newly opened owner.
		if err := oldRelease(context.Background()); err != nil {
			t.Fatalf("late release of abandoned Agent Store handle: %v", err)
		}
	}
}

func (backend *agentFaultBackend) close(t *testing.T) {
	t.Helper()
	if backend.release == nil {
		return
	}
	if err := backend.release(context.Background()); err != nil {
		t.Errorf("release Agent Store: %v", err)
	}
	backend.store, backend.release = nil, nil
}

func openAgentFaultService(t *testing.T, backend *agentFaultBackend) *Agent {
	t.Helper()
	config := DefaultConfig()
	config.Store = framework.Borrow[Store](backend.store)
	service, err := New(t.Context(), config)
	if err != nil {
		t.Fatalf("open Agent over fault backend: %v", err)
	}
	return service
}

func openAgentFaultEndpoint(t *testing.T, service *Agent) *AgentWorkerEndpoint {
	t.Helper()
	config := DefaultWorkerEndpointConfig()
	config.PollInterval = 5 * time.Millisecond
	config.RedeliveryInterval = 250 * time.Millisecond
	config.AssignmentBuffer = 64
	endpoint, err := NewWorkerEndpoint(t.Context(), service, config)
	if err != nil {
		t.Fatalf("open Agent worker endpoint: %v", err)
	}
	return endpoint
}

func shutdownAgentFaultService(t *testing.T, service *Agent) {
	t.Helper()
	if service == nil {
		return
	}
	if err := service.Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown Agent fault service: %v", err)
	}
}

func agentFaultPrincipal(authority AuthorityID, session WorkerSessionID) AuthenticatedWorker {
	return AuthenticatedWorker{Authority: authority, Worker: "worker-fault-matrix", Session: session}
}

func openAgentFaultConnection(
	t *testing.T,
	endpoint *AgentWorkerEndpoint,
	principal AuthenticatedWorker,
) WorkerConnection {
	t.Helper()
	connection, err := endpoint.OpenWorker(t.Context(), principal)
	if err != nil {
		t.Fatalf("open Agent worker connection: %v", err)
	}
	return connection
}

func shutdownAgentFaultConnection(t *testing.T, connection WorkerConnection) {
	t.Helper()
	if connection == nil {
		return
	}
	if err := connection.Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown Agent worker connection: %v", err)
	}
}

func agentFaultAssignmentRedelivery(t *testing.T, backend *agentFaultBackend) {
	service := openAgentFaultService(t, backend)
	defer shutdownAgentFaultService(t, service)
	ctx := t.Context()
	contextRef, taskRef := refs("authority-assignment", "context-assignment", "workspace-assignment", "task-assignment")
	if _, err := service.CreateTask(ctx, createCommand(
		"create-assignment", contextRef, taskRef, "message-assignment", "redeliver",
	)); err != nil {
		t.Fatal(err)
	}

	endpoint := openAgentFaultEndpoint(t, service)
	firstConnection := openAgentFaultConnection(t, endpoint, agentFaultPrincipal(contextRef.Authority, "session-assignment-1"))
	first := receiveWorkerAssignment(t, firstConnection, taskRef, 1)
	shutdownAgentFaultConnection(t, firstConnection) // no assignment ACK reached the endpoint

	secondConnection := openAgentFaultConnection(t, endpoint, agentFaultPrincipal(contextRef.Authority, "session-assignment-2"))
	defer shutdownAgentFaultConnection(t, secondConnection)
	replayed := receiveWorkerAssignment(t, secondConnection, taskRef, 1)
	if first.ID != replayed.ID || !reflect.DeepEqual(first.Assignment, replayed.Assignment) {
		t.Fatalf("assignment redelivery changed:\nfirst=%#v\nreplayed=%#v", first, replayed)
	}
	if err := secondConnection.AckAssignment(ctx, replayed.ID); err != nil {
		t.Fatal(err)
	}
}

func agentFaultEventACKReplay(t *testing.T, backend *agentFaultBackend) {
	service := openAgentFaultService(t, backend)
	defer shutdownAgentFaultService(t, service)
	ctx := t.Context()
	contextRef, taskRef := refs("authority-event", "context-event", "workspace-event", "task-event")
	if _, err := service.CreateTask(ctx, createCommand(
		"create-event", contextRef, taskRef, "message-event", "commit once",
	)); err != nil {
		t.Fatal(err)
	}

	endpoint := openAgentFaultEndpoint(t, service)
	firstConnection := openAgentFaultConnection(t, endpoint, agentFaultPrincipal(contextRef.Authority, "session-event-1"))
	assignment := receiveWorkerAssignment(t, firstConnection, taskRef, 1)
	if err := firstConnection.AckAssignment(ctx, assignment.ID); err != nil {
		t.Fatal(err)
	}
	event := workerEventFor("ack-loss", WorkerEventAcceptTask, assignment.Assignment.Grant, 1)
	firstReceipt, err := firstConnection.CommitEvent(ctx, event)
	if err != nil || firstReceipt.Decision != WorkerEventACK {
		t.Fatalf("first committed event = %#v, %v", firstReceipt, err)
	}
	committed, err := service.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	shutdownAgentFaultConnection(t, firstConnection) // model loss of the returned ACK

	secondConnection := openAgentFaultConnection(t, endpoint, agentFaultPrincipal(contextRef.Authority, "session-event-2"))
	defer shutdownAgentFaultConnection(t, secondConnection)
	replayedReceipt, err := secondConnection.CommitEvent(ctx, event)
	if err != nil || !reflect.DeepEqual(replayedReceipt, firstReceipt) {
		t.Fatalf("committed ACK replay = %#v, %v; want %#v", replayedReceipt, err, firstReceipt)
	}
	afterReplay, err := service.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterReplay, committed) {
		t.Fatalf("ACK replay duplicated domain transition:\nbefore=%#v\nafter=%#v", committed, afterReplay)
	}
}

func agentFaultStoreRestart(t *testing.T, backend *agentFaultBackend) {
	ctx := t.Context()
	service := openAgentFaultService(t, backend)
	defer shutdownAgentFaultService(t, service)
	contextRef, leaseTask := refs("authority-restart", "context-restart", "workspace-lease", "task-lease")
	_, receiptTask := refs("authority-restart", "context-restart", "workspace-receipt", "task-receipt")
	for _, input := range []struct {
		id   string
		ref  TaskRef
		text string
	}{
		{id: "lease", ref: leaseTask, text: "recover assignment"},
		{id: "receipt", ref: receiptTask, text: "recover receipt"},
	} {
		if _, err := service.CreateTask(ctx, createCommand(
			"create-restart-"+input.id, contextRef, input.ref,
			"message-restart-"+input.id, input.text,
		)); err != nil {
			t.Fatal(err)
		}
	}
	principal := agentFaultPrincipal(contextRef.Authority, "session-restart-1")
	leaseAssignment, err := service.AcquireLease(ctx, AcquireLeaseCommand{
		ID: "acquire-restart-lease", Task: leaseTask, Worker: principal.Worker,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiptAssignment, err := service.AcquireLease(ctx, AcquireLeaseCommand{
		ID: "acquire-restart-receipt", Task: receiptTask, Worker: principal.Worker,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := openAgentFaultEndpoint(t, service)
	connection := openAgentFaultConnection(t, endpoint, principal)
	defer shutdownAgentFaultConnection(t, connection)
	deliveries := receiveAgentFaultAssignments(t, connection, leaseTask, receiptTask)
	leaseDelivery := deliveries[leaseTask]
	if !reflect.DeepEqual(leaseDelivery.Assignment, leaseAssignment) {
		t.Fatalf("lease assignment delivery = %#v, want %#v", leaseDelivery.Assignment, leaseAssignment)
	}
	receiptDelivery := deliveries[receiptTask]
	if !reflect.DeepEqual(receiptDelivery.Assignment, receiptAssignment) {
		t.Fatalf("receipt assignment delivery = %#v, want %#v", receiptDelivery.Assignment, receiptAssignment)
	}
	if err := connection.AckAssignment(ctx, receiptDelivery.ID); err != nil {
		t.Fatal(err)
	}
	event := workerEventFor("restart-receipt", WorkerEventAcceptTask, receiptAssignment.Grant, 1)
	committedReceipt, err := connection.CommitEvent(ctx, event)
	if err != nil || committedReceipt.Decision != WorkerEventACK {
		t.Fatalf("commit before restart = %#v, %v", committedReceipt, err)
	}
	committedTask, err := service.GetTask(ctx, receiptTask)
	if err != nil {
		t.Fatal(err)
	}
	shutdownAgentFaultConnection(t, connection)
	shutdownAgentFaultService(t, service)
	backend.restart(t) // Memory abandons the handle; SQLite release/reopen is complemented by a subprocess crash test.

	recoveredService := openAgentFaultService(t, backend)
	defer shutdownAgentFaultService(t, recoveredService)
	recoveredEndpoint := openAgentFaultEndpoint(t, recoveredService)
	principal.Session = "session-restart-2"
	recoveredConnection := openAgentFaultConnection(t, recoveredEndpoint, principal)
	defer shutdownAgentFaultConnection(t, recoveredConnection)

	recoveredLease, err := recoveredService.GetLease(ctx, leaseTask)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recoveredLease, leaseAssignment) {
		t.Fatalf("lease changed across Store restart:\nbefore=%#v\nafter=%#v", leaseAssignment, recoveredLease)
	}
	replayedAssignment := receiveWorkerAssignment(t, recoveredConnection, leaseTask, 1)
	if replayedAssignment.ID != leaseDelivery.ID || !reflect.DeepEqual(replayedAssignment.Assignment, leaseDelivery.Assignment) {
		t.Fatalf("assignment changed across Store restart:\nbefore=%#v\nafter=%#v", leaseDelivery, replayedAssignment)
	}

	replayedReceipt, err := recoveredConnection.CommitEvent(ctx, event)
	if err != nil || !reflect.DeepEqual(replayedReceipt, committedReceipt) {
		t.Fatalf("command receipt after Store restart = %#v, %v; want %#v", replayedReceipt, err, committedReceipt)
	}
	afterReplay, err := recoveredService.GetTask(ctx, receiptTask)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterReplay, committedTask) {
		t.Fatalf("restart receipt replay duplicated transition:\nbefore=%#v\nafter=%#v", committedTask, afterReplay)
	}
}

func receiveAgentFaultAssignments(
	t *testing.T,
	connection WorkerConnection,
	refs ...TaskRef,
) map[TaskRef]WorkerAssignmentDelivery {
	t.Helper()
	wanted := make(map[TaskRef]struct{}, len(refs))
	for _, ref := range refs {
		wanted[ref] = struct{}{}
	}
	found := make(map[TaskRef]WorkerAssignmentDelivery, len(refs))
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for len(found) < len(wanted) {
		select {
		case delivery, ok := <-connection.Assignments():
			if !ok {
				t.Fatalf("worker connection ended before assignments: %v", connection.Err())
			}
			if _, ok := wanted[delivery.Assignment.Grant.Task]; ok {
				found[delivery.Assignment.Grant.Task] = delivery
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d Agent assignments; received %d", len(wanted), len(found))
		}
	}
	return found
}

func agentFaultPoisonFollower(t *testing.T, backend *agentFaultBackend) {
	service := openAgentFaultService(t, backend)
	defer shutdownAgentFaultService(t, service)
	ctx := t.Context()
	contextRef, taskRef := refs("authority-poison", "context-poison", "workspace-poison", "task-poison")
	if _, err := service.CreateTask(ctx, createCommand(
		"create-poison", contextRef, taskRef, "message-poison", "reject then continue",
	)); err != nil {
		t.Fatal(err)
	}
	endpoint := openAgentFaultEndpoint(t, service)
	connection := openAgentFaultConnection(t, endpoint, agentFaultPrincipal(contextRef.Authority, "session-poison"))
	defer shutdownAgentFaultConnection(t, connection)
	assignment := receiveWorkerAssignment(t, connection, taskRef, 1)
	if err := connection.AckAssignment(ctx, assignment.ID); err != nil {
		t.Fatal(err)
	}

	poison := workerEventFor("poison", WorkerEventKind("unknown"), assignment.Assignment.Grant, 1)
	poisonReceipt, err := connection.CommitEvent(ctx, poison)
	if err != nil || poisonReceipt.Decision != WorkerEventNACK || poisonReceipt.Code != WorkerRejectInvalid {
		t.Fatalf("poison receipt = %#v, %v", poisonReceipt, err)
	}
	follower := workerEventFor("poison-follower", WorkerEventAcceptTask, assignment.Assignment.Grant, 1)
	followerReceipt, err := connection.CommitEvent(ctx, follower)
	if err != nil || followerReceipt.Decision != WorkerEventACK {
		t.Fatalf("follower after poison = %#v, %v", followerReceipt, err)
	}
	task, err := service.GetTask(ctx, taskRef)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != TaskWorking || task.Revision != 2 {
		t.Fatalf("follower did not advance Task: %#v", task)
	}
	select {
	case <-connection.Done():
		t.Fatalf("poison NACK closed worker connection: %v", connection.Err())
	default:
	}
}
