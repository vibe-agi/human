package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	. "github.com/vibe-agi/human/agent"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/workspace"
)

type workerTransportContractStub struct{}

func (workerTransportContractStub) Description() WorkerTransportDescription {
	return WorkerTransportDescription{
		Contract: framework.Contract{
			ID: WorkerTransportContractID, Major: WorkerTransportContractMajor,
		},
		Provider: "contract-test",
		Version:  "1",
	}
}

func (workerTransportContractStub) Start(
	_ context.Context,
	_ WorkerEndpoint,
) (WorkerTransportRuntime, error) {
	return workerTransportRuntimeStub{done: closedWorkerTransportTestChannel()}, nil
}

type workerTransportRuntimeStub struct {
	done <-chan struct{}
}

func (runtime workerTransportRuntimeStub) Done() <-chan struct{}  { return runtime.done }
func (workerTransportRuntimeStub) Err() error                     { return nil }
func (workerTransportRuntimeStub) Shutdown(context.Context) error { return nil }

type workerEndpointContractStub struct {
	connection WorkerConnection
}

func (endpoint workerEndpointContractStub) OpenWorker(
	_ context.Context,
	_ AuthenticatedWorker,
) (WorkerConnection, error) {
	return endpoint.connection, nil
}

type workerConnectionContractStub struct {
	principal   AuthenticatedWorker
	assignments <-chan WorkerAssignmentDelivery
	done        <-chan struct{}
}

func (connection workerConnectionContractStub) Principal() AuthenticatedWorker {
	return connection.principal
}
func (connection workerConnectionContractStub) Assignments() <-chan WorkerAssignmentDelivery {
	return connection.assignments
}
func (workerConnectionContractStub) AckAssignment(context.Context, WorkerDeliveryID) error {
	return nil
}
func (_ workerConnectionContractStub) CommitEvent(
	_ context.Context,
	delivery WorkerEventDelivery,
) (WorkerEventReceipt, error) {
	return WorkerEventReceipt{
		Delivery: delivery.ID,
		Event:    delivery.Event.ID,
		Decision: WorkerEventACK,
	}, nil
}
func (connection workerConnectionContractStub) Done() <-chan struct{} { return connection.done }
func (workerConnectionContractStub) Err() error                       { return nil }
func (workerConnectionContractStub) Shutdown(context.Context) error   { return nil }

var (
	_ WorkerTransport        = workerTransportContractStub{}
	_ WorkerTransportRuntime = workerTransportRuntimeStub{}
	_ WorkerEndpoint         = workerEndpointContractStub{}
	_ WorkerConnection       = workerConnectionContractStub{}
)

func TestWorkerTransportDescriptionNegotiatesContract(t *testing.T) {
	description := workerTransportContractStub{}.Description()
	if err := description.Validate(); err != nil {
		t.Fatalf("valid WorkerTransport description: %v", err)
	}
	description.Contract.Major++
	if err := description.Validate(); !errors.Is(err, ErrWorkerTransportContractMismatch) {
		t.Fatalf("major mismatch error = %v", err)
	}

	description = workerTransportContractStub{}.Description()
	for _, provider := range []string{"", " websocket", "grpc\npassword=secret"} {
		description.Provider = provider
		if err := description.Validate(); !errors.Is(err, ErrWorkerTransportDescription) {
			t.Fatalf("provider %q error = %v", provider, err)
		}
	}
}

func TestAuthenticatedWorkerIsOutOfBandAndRequired(t *testing.T) {
	principal := workerTransportTestPrincipal()
	if err := principal.Validate(); err != nil {
		t.Fatalf("valid authenticated principal: %v", err)
	}
	encoded, err := json.Marshal(principal)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "{}" {
		t.Fatalf("authenticated principal leaked into a payload: %s", encoded)
	}

	for name, mutate := range map[string]func(*AuthenticatedWorker){
		"authority": func(value *AuthenticatedWorker) { value.Authority = "" },
		"worker":    func(value *AuthenticatedWorker) { value.Worker = "" },
		"session":   func(value *AuthenticatedWorker) { value.Session = "" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := principal
			mutate(&invalid)
			if err := invalid.Validate(); !errors.Is(err, ErrWorkerPrincipal) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	eventType := reflect.TypeOf(WorkerEvent{})
	for index := 0; index < eventType.NumField(); index++ {
		field := eventType.Field(index)
		if field.Type == reflect.TypeOf(WorkerID("")) || strings.Contains(strings.ToLower(field.Name), "worker") {
			t.Fatalf("WorkerEvent accepts payload worker identity through field %s", field.Name)
		}
	}
}

func TestWorkerAssignmentDeliveryIsSessionIndependentAndRepeatable(t *testing.T) {
	principal := workerTransportTestPrincipal()
	delivery := workerTransportTestAssignment()
	if err := delivery.ValidateFor(principal); err != nil {
		t.Fatalf("valid assignment: %v", err)
	}
	reconnected := principal
	reconnected.Session = "session-b"
	if err := delivery.ValidateFor(reconnected); err != nil {
		t.Fatalf("same assignment after reconnect: %v", err)
	}

	wrongWorker := principal
	wrongWorker.Worker = "worker-b"
	if err := delivery.ValidateFor(wrongWorker); !errors.Is(err, ErrWorkerDelivery) {
		t.Fatalf("wrong-worker error = %v", err)
	}

	cloned := CloneWorkerAssignmentDelivery(delivery)
	cloned.Assignment.Task.Context.ID = "changed-context"
	if delivery.Assignment.Task.Context.ID == cloned.Assignment.Task.Context.ID {
		t.Fatal("assignment clone aliases Task data")
	}
}

func TestWorkerEventPayloadShapesAndAuthorityBinding(t *testing.T) {
	principal := workerTransportTestPrincipal()
	base := workerTransportTestEvent()
	if err := base.ValidateFor(principal); err != nil {
		t.Fatalf("valid accept event: %v", err)
	}

	requestInput := base
	requestInput.ID = "delivery-request"
	requestInput.Event.ID = "event-request"
	requestInput.Event.Kind = WorkerEventRequestInput
	message := MessageInput{
		ID:    "message-request",
		Parts: []Part{{MediaType: "text/plain", Data: []byte("need a decision")}},
	}
	requestInput.Event.Message = &message
	if err := requestInput.ValidateFor(principal); err != nil {
		t.Fatalf("valid request-input event: %v", err)
	}

	freeze := base
	freeze.ID = "delivery-freeze"
	freeze.Event.ID = "event-freeze"
	freeze.Event.Kind = WorkerEventFreezeArtifact
	freeze.Event.Freeze = &WorkerArtifactFreeze{
		Artifact:             "artifact-a",
		ExpectedBaseRevision: "revision-a",
		Payload: workspace.Payload{
			MediaType: "application/json",
			Data:      []byte(`{"change":"value"}`),
		},
	}
	if err := freeze.ValidateFor(principal); err != nil {
		t.Fatalf("valid freeze event: %v", err)
	}

	complete := base
	complete.ID = "delivery-complete"
	complete.Event.ID = "event-complete"
	complete.Event.Kind = WorkerEventCompleteTask
	complete.Event.Submission = "submission-a"
	complete.Event.Message = &MessageInput{
		ID:    "message-complete",
		Parts: []Part{{MediaType: "text/plain", Data: []byte("done")}},
	}
	if err := complete.ValidateFor(principal); err != nil {
		t.Fatalf("valid completion event: %v", err)
	}

	malformed := requestInput
	malformed.Event.Submission = "smuggled-field"
	if err := malformed.Validate(); !errors.Is(err, ErrWorkerDelivery) {
		t.Fatalf("unexpected-field error = %v", err)
	}

	wrongAuthority := base
	wrongAuthority.Event.Task.Workspace.Authority = "authority-b"
	if err := wrongAuthority.ValidateFor(principal); !errors.Is(err, ErrWorkerDelivery) {
		t.Fatalf("wrong-authority error = %v", err)
	}
}

func TestWorkerEventCloneOwnsNestedBytes(t *testing.T) {
	delivery := workerTransportTestEvent()
	delivery.Event.Kind = WorkerEventFreezeArtifact
	delivery.Event.Freeze = &WorkerArtifactFreeze{
		Artifact: "artifact-a", ExpectedBaseRevision: "revision-a",
		Payload: workspace.Payload{MediaType: "application/json", Data: []byte("payload")},
	}
	cloned := CloneWorkerEventDelivery(delivery)
	cloned.Event.Freeze.Payload.Data[0] = 'P'
	if string(delivery.Event.Freeze.Payload.Data) != "payload" {
		t.Fatalf("freeze payload aliases clone: %q", delivery.Event.Freeze.Payload.Data)
	}

	message := MessageInput{ID: "message-a", Parts: []Part{{MediaType: "text/plain", Data: []byte("reply")}}}
	delivery.Event.Kind = WorkerEventRequestInput
	delivery.Event.Freeze = nil
	delivery.Event.Message = &message
	cloned = CloneWorkerEventDelivery(delivery)
	cloned.Event.Message.Parts[0].Data[0] = 'R'
	if string(delivery.Event.Message.Parts[0].Data) != "reply" {
		t.Fatalf("message payload aliases clone: %q", delivery.Event.Message.Parts[0].Data)
	}
}

func TestWorkerEventReceiptSeparatesACKNACKAndUnsettledError(t *testing.T) {
	delivery := workerTransportTestEvent()
	ack := WorkerEventReceipt{
		Delivery: delivery.ID, Event: delivery.Event.ID, Decision: WorkerEventACK,
	}
	if err := ack.ValidateFor(delivery); err != nil {
		t.Fatalf("valid ACK: %v", err)
	}

	nack := WorkerEventReceipt{
		Delivery: delivery.ID, Event: delivery.Event.ID, Decision: WorkerEventNACK,
		Code: WorkerRejectStaleLease, Message: "lease generation was fenced",
	}
	if err := nack.ValidateFor(delivery); err != nil {
		t.Fatalf("valid NACK: %v", err)
	}

	invalidACK := ack
	invalidACK.Code = WorkerRejectInvalid
	if err := invalidACK.ValidateFor(delivery); !errors.Is(err, ErrWorkerDelivery) {
		t.Fatalf("ACK-with-code error = %v", err)
	}
	invalidNACK := nack
	invalidNACK.Code = ""
	if err := invalidNACK.ValidateFor(delivery); !errors.Is(err, ErrWorkerDelivery) {
		t.Fatalf("NACK-without-code error = %v", err)
	}
	wrong := ack
	wrong.Delivery = "another-delivery"
	if err := wrong.ValidateFor(delivery); !errors.Is(err, ErrWorkerDelivery) {
		t.Fatalf("misdirected ACK error = %v", err)
	}
	poison := delivery
	poison.Event.Kind = "unknown_future_event"
	poisonNACK := WorkerEventReceipt{
		Delivery: poison.ID, Event: poison.Event.ID, Decision: WorkerEventNACK,
		Code: WorkerRejectInvalid, Message: "unsupported event kind",
	}
	if err := poisonNACK.ValidateFor(poison); err != nil {
		t.Fatalf("deterministic poison-event NACK must settle its outbox record: %v", err)
	}
	poisonACK := poisonNACK
	poisonACK.Decision = WorkerEventACK
	poisonACK.Code = ""
	poisonACK.Message = ""
	if err := poisonACK.ValidateFor(poison); !errors.Is(err, ErrWorkerDelivery) {
		t.Fatalf("malformed event was accepted by ACK validation: %v", err)
	}

	uncertain := &WorkerDeliveryError{
		Delivery: delivery.ID,
		Event:    delivery.Event.ID,
		Cause:    ErrWorkerDeliveryIndeterminate,
	}
	if !errors.Is(uncertain, ErrWorkerDeliveryIndeterminate) {
		t.Fatalf("typed unsettled error lost classification: %v", uncertain)
	}
	connectionFailure := &WorkerConnectionError{
		Principal: workerTransportTestPrincipal(), Cause: ErrWorkerConnectionClosed,
	}
	if !errors.Is(connectionFailure, ErrWorkerConnectionClosed) {
		t.Fatalf("typed connection error lost classification: %v", connectionFailure)
	}
}

func workerTransportTestPrincipal() AuthenticatedWorker {
	return AuthenticatedWorker{
		Authority: "authority-a",
		Worker:    "worker-a",
		Session:   "session-a",
	}
}

func workerTransportTestEvent() WorkerEventDelivery {
	return WorkerEventDelivery{
		ID: "delivery-a",
		Event: WorkerEvent{
			ID:               "event-a",
			Kind:             WorkerEventAcceptTask,
			Task:             workerTransportTestTaskRef(),
			Fence:            1,
			ExpectedRevision: 1,
		},
	}
}

func workerTransportTestAssignment() WorkerAssignmentDelivery {
	now := time.Date(2026, 7, 19, 6, 7, 8, 9, time.UTC)
	ref := workerTransportTestTaskRef()
	return WorkerAssignmentDelivery{
		ID: "assignment-a",
		Assignment: LeaseAssignment{
			Grant: LeaseGrant{Task: ref, Worker: "worker-a", Fence: 1},
			Task: Task{
				Ref: ref,
				Context: ContextRef{
					Authority: ref.Workspace.Authority,
					ID:        "context-a",
				},
				State: TaskSubmitted, Revision: 1, MessageCount: 1, EventCount: 1,
				CreatedAt: now, UpdatedAt: now,
			},
			GrantedAt: now.Add(time.Nanosecond),
		},
	}
}

func workerTransportTestTaskRef() TaskRef {
	return TaskRef{
		Workspace: WorkspaceRef{Authority: "authority-a", ID: "workspace-a"},
		ID:        "task-a",
	}
}

func closedWorkerTransportTestChannel() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
