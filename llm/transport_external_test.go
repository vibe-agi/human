package llm_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

type fakeLLMWorkerTransport struct{}

func (*fakeLLMWorkerTransport) Description() llm.WorkerTransportDescription {
	return llm.WorkerTransportDescription{
		Contract: framework.Contract{
			ID:       llm.WorkerTransportContractID,
			Major:    llm.WorkerTransportContractMajor,
			Minor:    2,
			Features: map[framework.Feature]uint16{"fake.trace": 1},
		},
		Provider: "external-fake",
		Version:  "2026.07.1",
	}
}

func (*fakeLLMWorkerTransport) Start(
	ctx context.Context,
	endpoint llm.WorkerEndpoint,
) (llm.WorkerTransportRuntime, error) {
	if ctx == nil || endpoint == nil {
		return nil, errors.New("fake transport requires initialization dependencies")
	}
	return newFakeTransportRuntime(), nil
}

type fakeTransportRuntime struct {
	done chan struct{}
	once sync.Once
}

func newFakeTransportRuntime() *fakeTransportRuntime {
	return &fakeTransportRuntime{done: make(chan struct{})}
}

func (runtime *fakeTransportRuntime) Done() <-chan struct{} { return runtime.done }
func (*fakeTransportRuntime) Err() error                    { return nil }
func (runtime *fakeTransportRuntime) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("shutdown context is required")
	}
	runtime.once.Do(func() { close(runtime.done) })
	return nil
}

var (
	_ llm.WorkerTransport        = (*fakeLLMWorkerTransport)(nil)
	_ llm.WorkerTransportRuntime = (*fakeTransportRuntime)(nil)
)

func TestExternalWorkerTransportNegotiatesAndOwnsOnlyItsRuntime(t *testing.T) {
	transport := &fakeLLMWorkerTransport{}
	description, err := llm.ValidateWorkerTransport(transport)
	if err != nil {
		t.Fatalf("ValidateWorkerTransport: %v", err)
	}
	if description.Provider != "external-fake" || description.Version != "2026.07.1" {
		t.Fatalf("unexpected description: %+v", description)
	}
	original := transport.Description()
	frozen, err := llm.NegotiateWorkerTransport(original)
	if err != nil {
		t.Fatalf("NegotiateWorkerTransport: %v", err)
	}
	original.Contract.Features["fake.trace"] = 99
	if got := frozen.Contract.Features["fake.trace"]; got != 1 {
		t.Fatalf("negotiated feature map aliases provider: got %d", got)
	}

	var typedNil *fakeLLMWorkerTransport
	if _, err := llm.ValidateWorkerTransport(typedNil); !errors.Is(err, llm.ErrWorkerTransportDescription) {
		t.Fatalf("typed-nil error = %v", err)
	}

	invalid := transport.Description()
	invalid.Contract.Major++
	if _, err := llm.NegotiateWorkerTransport(invalid); !errors.Is(err, llm.ErrWorkerTransportContractMismatch) {
		t.Fatalf("contract mismatch = %v", err)
	}
	invalid = transport.Description()
	invalid.Provider = " grpc\nsecret"
	if _, err := llm.NegotiateWorkerTransport(invalid); !errors.Is(err, llm.ErrWorkerTransportDescription) {
		t.Fatalf("invalid provider = %v", err)
	}

	endpoint := newFakeLLMWorkerEndpoint(testAssignment("runtime", true))
	initialization, cancelInitialization := context.WithCancel(context.Background())
	runtime, err := transport.Start(initialization, endpoint)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancelInitialization()
	select {
	case <-runtime.Done():
		t.Fatal("transport retained its constructor context as runtime lifetime")
	default:
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-runtime.Done():
	case <-time.After(time.Second):
		t.Fatal("transport runtime did not finish")
	}
	if endpoint.closed {
		t.Fatal("transport runtime shut down its borrowed endpoint")
	}
}

func TestAssignmentBoundaryPrincipalAndReconnectIdentity(t *testing.T) {
	principal := testPrincipal("session-a")
	if err := principal.Validate(); err != nil {
		t.Fatalf("valid principal: %v", err)
	}
	encoded, err := json.Marshal(principal)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "{}" {
		t.Fatalf("authenticated principal leaked into payload: %s", encoded)
	}

	stream := testAssignment("stream", true)
	if err := stream.ValidateFor(principal); err != nil {
		t.Fatalf("valid stream assignment: %v", err)
	}
	reconnected := principal
	reconnected.SessionID = "session-b"
	if err := stream.ValidateFor(reconnected); err != nil {
		t.Fatalf("assignment became session-bound after reconnect: %v", err)
	}

	early := stream
	early.Assignment.Boundary = llm.AssignmentAfterAdmission
	if err := early.ValidateFor(principal); !errors.Is(err, llm.ErrWorkerDelivery) {
		t.Fatalf("early stream assignment error = %v", err)
	}
	aggregate := testAssignment("aggregate", false)
	if err := aggregate.ValidateFor(principal); err != nil {
		t.Fatalf("valid aggregate assignment: %v", err)
	}
	aggregate.Assignment.Boundary = llm.AssignmentAfterResponse
	if err := aggregate.ValidateFor(principal); !errors.Is(err, llm.ErrWorkerDelivery) {
		t.Fatalf("false aggregate response boundary error = %v", err)
	}

	wrongWorker := principal
	wrongWorker.WorkerID = "worker-b"
	if err := stream.ValidateFor(wrongWorker); !errors.Is(err, llm.ErrWorkerDelivery) {
		t.Fatalf("wrong-worker assignment error = %v", err)
	}

	cloned := llm.CloneWorkerAssignmentDelivery(stream)
	cloned.Assignment.Request.Messages[0].Blocks[0].Text = "changed"
	cloned.Assignment.Request.Metadata["scope"] = "changed"
	if got := stream.Assignment.Request.Messages[0].Blocks[0].Text; got != "inspect workspace" {
		t.Fatalf("assignment clone aliases message: %q", got)
	}
	if got := stream.Assignment.Request.Metadata["scope"]; got != "stream" {
		t.Fatalf("assignment clone aliases metadata: %q", got)
	}
}

func TestWorkerEventReceiptBindsPrincipalAndSettlesPoison(t *testing.T) {
	delivery := testEvent("healthy", llm.EventProgress)
	if err := delivery.ValidateFor(testPrincipal("session-a")); err != nil {
		t.Fatalf("valid event: %v", err)
	}

	forged := delivery
	forged.Event.WorkerID = "worker-from-json"
	if err := forged.ValidateFor(testPrincipal("session-a")); !errors.Is(err, llm.ErrWorkerDelivery) {
		t.Fatalf("payload worker identity error = %v", err)
	}

	ack := llm.WorkerEventReceipt{
		Delivery: delivery.ID,
		EventID:  delivery.Event.ID,
		Decision: llm.WorkerEventACK,
	}
	if err := ack.ValidateFor(delivery); err != nil {
		t.Fatalf("valid ACK: %v", err)
	}
	nack := llm.WorkerEventReceipt{
		Delivery: delivery.ID,
		EventID:  delivery.Event.ID,
		Decision: llm.WorkerEventNACK,
		Code:     llm.WorkerRejectStateConflict,
		Message:  "response is no longer awaiting Human input",
	}
	if err := nack.ValidateFor(delivery); err != nil {
		t.Fatalf("valid NACK: %v", err)
	}

	poison := delivery
	poison.ID = "delivery-poison"
	poison.Event.ID = "event-poison"
	poison.Event.Type = "unknown_future_event"
	poisonNACK := llm.WorkerEventReceipt{
		Delivery: poison.ID,
		EventID:  poison.Event.ID,
		Decision: llm.WorkerEventNACK,
		Code:     llm.WorkerRejectInvalid,
		Message:  "unsupported event kind",
	}
	if err := poisonNACK.ValidateFor(poison); err != nil {
		t.Fatalf("deterministic poison NACK must settle its outbox record: %v", err)
	}
	poisonACK := poisonNACK
	poisonACK.Decision = llm.WorkerEventACK
	poisonACK.Code = ""
	poisonACK.Message = ""
	if err := poisonACK.ValidateFor(poison); !errors.Is(err, llm.ErrWorkerDelivery) {
		t.Fatalf("malformed event was accepted by ACK validation: %v", err)
	}

	wrong := ack
	wrong.Delivery = "delivery-other"
	if err := wrong.ValidateFor(delivery); !errors.Is(err, llm.ErrWorkerDelivery) {
		t.Fatalf("misdirected ACK error = %v", err)
	}
	uncertain := &llm.WorkerDeliveryError{
		Delivery: delivery.ID,
		EventID:  delivery.Event.ID,
		Cause:    llm.ErrWorkerDeliveryIndeterminate,
	}
	if !errors.Is(uncertain, llm.ErrWorkerDeliveryIndeterminate) {
		t.Fatalf("delivery error lost classification: %v", uncertain)
	}
	connectionFailure := &llm.WorkerConnectionError{
		Principal: testPrincipal("session-a"),
		Cause:     llm.ErrWorkerConnectionClosed,
	}
	if !errors.Is(connectionFailure, llm.ErrWorkerConnectionClosed) {
		t.Fatalf("connection error lost classification: %v", connectionFailure)
	}

	toolDelivery := testEvent("clone", llm.EventToolCalls)
	toolDelivery.Event.ToolCalls[0].Input["nested"] = map[string]any{
		"parts": []any{"a", "b"},
	}
	cloned := llm.CloneWorkerEventDelivery(toolDelivery)
	nested := cloned.Event.ToolCalls[0].Input["nested"].(map[string]any)
	nested["parts"].([]any)[0] = "changed"
	originalNested := toolDelivery.Event.ToolCalls[0].Input["nested"].(map[string]any)
	if got := originalNested["parts"].([]any)[0]; got != "a" {
		t.Fatalf("event clone aliases nested tool input: %v", got)
	}
}

func TestExternalFakeDisconnectReplayLateTerminalAndNoPoisonHead(t *testing.T) {
	assignment := testAssignment("replay", true)
	endpoint := newFakeLLMWorkerEndpoint(assignment)

	first, err := endpoint.OpenWorker(context.Background(), testPrincipal("session-a"))
	if err != nil {
		t.Fatalf("open first worker: %v", err)
	}
	received := receiveAssignment(t, first)
	if !reflect.DeepEqual(received, assignment) {
		t.Fatalf("first assignment changed:\n got  %+v\n want %+v", received, assignment)
	}
	// Disconnect before assignment ACK. The exact delivery must remain pending.
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown first connection: %v", err)
	}

	second, err := endpoint.OpenWorker(context.Background(), testPrincipal("session-b"))
	if err != nil {
		t.Fatalf("open reconnected worker: %v", err)
	}
	replayed := receiveAssignment(t, second)
	if !reflect.DeepEqual(replayed, assignment) {
		t.Fatalf("reconnected assignment was not exact replay")
	}
	if err := second.AckAssignment(context.Background(), replayed.ID); err != nil {
		t.Fatalf("ACK durable assignment: %v", err)
	}

	outbox := &fakeWorkerOutbox{}
	staleLease := testEvent("stale-lease", llm.EventProgress)
	staleLease.LeaseID = "lease-replaced"
	outbox.put(staleLease)
	receipts, err := outbox.flush(context.Background(), second)
	if err != nil {
		t.Fatalf("settle stale lease: %v", err)
	}
	if len(receipts) != 1 || receipts[0].Decision != llm.WorkerEventNACK ||
		receipts[0].Code != llm.WorkerRejectStaleLease {
		t.Fatalf("stale lease receipt = %+v", receipts)
	}
	if endpoint.durable("event-stale-lease") {
		t.Fatal("old lease generation was ACKed as a durable response effect")
	}

	poison := testEvent("poison", llm.EventProgress)
	poison.Event.Type = "unknown_future_event"
	healthy := testEvent("healthy", llm.EventProgress)
	outbox.put(poison)
	outbox.put(healthy)
	receipts, err = outbox.flush(context.Background(), second)
	if err != nil {
		t.Fatalf("flush poison and follower: %v", err)
	}
	if len(receipts) != 2 || receipts[0].Decision != llm.WorkerEventNACK ||
		receipts[0].Code != llm.WorkerRejectInvalid || receipts[1].Decision != llm.WorkerEventACK {
		t.Fatalf("poison/follower receipts = %+v", receipts)
	}
	if outbox.len() != 0 {
		t.Fatalf("settled poison blocked outbox follower: %d records remain", outbox.len())
	}
	if !endpoint.durable("event-healthy") {
		t.Fatal("healthy ACK was returned before response wire plus receipt were durable")
	}
	if endpoint.durable("event-poison") {
		t.Fatal("deterministically rejected poison produced a durable response effect")
	}

	// The terminal event commits, but the connection drops before the worker
	// observes its ACK. Its outbox record must survive and exact replay must ACK.
	final := testEvent("final", llm.EventFinal)
	endpoint.dropACKOnce("event-final")
	outbox.put(final)
	if _, err := outbox.flush(context.Background(), second); !errors.Is(err, llm.ErrWorkerConnectionClosed) {
		t.Fatalf("lost terminal ACK error = %v", err)
	}
	if outbox.len() != 1 || !endpoint.durable("event-final") {
		t.Fatalf("ACK-loss boundary: outbox=%d durable=%v", outbox.len(), endpoint.durable("event-final"))
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown second connection: %v", err)
	}

	third, err := endpoint.OpenWorker(context.Background(), testPrincipal("session-c"))
	if err != nil {
		t.Fatalf("open third worker: %v", err)
	}
	receipts, err = outbox.flush(context.Background(), third)
	if err != nil {
		t.Fatalf("replay terminal after ACK loss: %v", err)
	}
	if len(receipts) != 1 || receipts[0].Decision != llm.WorkerEventACK || outbox.len() != 0 {
		t.Fatalf("terminal replay settlement = %+v; outbox=%d", receipts, outbox.len())
	}

	late := testEvent("late", llm.EventFinal)
	outbox.put(late)
	receipts, err = outbox.flush(context.Background(), third)
	if err != nil {
		t.Fatalf("settle late terminal: %v", err)
	}
	if len(receipts) != 1 || receipts[0].Decision != llm.WorkerEventNACK ||
		receipts[0].Code != llm.WorkerRejectResponseClosed || outbox.len() != 0 {
		t.Fatalf("late terminal settlement = %+v; outbox=%d", receipts, outbox.len())
	}
	if endpoint.durable("event-late") {
		t.Fatal("late terminal event reopened the closed response")
	}

	conflict := final
	conflict.ID = "delivery-final-conflict"
	conflict.Event.Text = "different terminal payload"
	outbox.put(conflict)
	receipts, err = outbox.flush(context.Background(), third)
	if err != nil {
		t.Fatalf("settle divergent terminal replay: %v", err)
	}
	if len(receipts) != 1 || receipts[0].Code != llm.WorkerRejectEventConflict {
		t.Fatalf("divergent terminal receipt = %+v", receipts)
	}
}

func TestExternalFakeUsesBoundedAssignmentBackpressureWithoutDrop(t *testing.T) {
	firstAssignment := testAssignment("queue-a", true)
	secondAssignment := testAssignment("queue-b", true)
	endpoint := newFakeLLMWorkerEndpoint(firstAssignment, secondAssignment)
	connection, err := endpoint.OpenWorker(context.Background(), testPrincipal("session-a"))
	if err != nil {
		t.Fatal(err)
	}
	if got := cap(connection.Assignments()); got != 1 {
		t.Fatalf("fake assignment window = %d, want bounded window 1", got)
	}
	first := receiveAssignment(t, connection)
	select {
	case unexpected := <-connection.Assignments():
		t.Fatalf("next assignment bypassed unacknowledged head: %+v", unexpected)
	default:
	}
	if err := connection.AckAssignment(context.Background(), first.ID); err != nil {
		t.Fatalf("ack first assignment: %v", err)
	}
	second := receiveAssignment(t, connection)
	if second.ID != secondAssignment.ID {
		t.Fatalf("backpressured follower = %q, want %q", second.ID, secondAssignment.ID)
	}
	if err := connection.AckAssignment(context.Background(), second.ID); err != nil {
		t.Fatalf("ack second assignment: %v", err)
	}
	select {
	case unexpected := <-connection.Assignments():
		t.Fatalf("assignment was duplicated after ACK: %+v", unexpected)
	default:
	}
}

type fakeLLMWorkerEndpoint struct {
	mu              sync.Mutex
	assignments     []llm.WorkerAssignmentDelivery
	assignmentACKed map[llm.WorkerDeliveryID]bool
	committed       map[string]fakeEventCommit
	closedResponses map[string]bool
	dropACK         map[string]bool
	droppedACK      map[string]bool
	active          *fakeLLMWorkerConnection
	closed          bool
}

type fakeEventCommit struct {
	digest  string
	wire    []byte
	receipt bool
}

func newFakeLLMWorkerEndpoint(assignments ...llm.WorkerAssignmentDelivery) *fakeLLMWorkerEndpoint {
	cloned := make([]llm.WorkerAssignmentDelivery, len(assignments))
	for index, assignment := range assignments {
		cloned[index] = llm.CloneWorkerAssignmentDelivery(assignment)
	}
	return &fakeLLMWorkerEndpoint{
		assignments: cloned, assignmentACKed: make(map[llm.WorkerDeliveryID]bool),
		committed: make(map[string]fakeEventCommit), closedResponses: make(map[string]bool),
		dropACK: make(map[string]bool), droppedACK: make(map[string]bool),
	}
}

func (endpoint *fakeLLMWorkerEndpoint) OpenWorker(
	ctx context.Context,
	principal llm.AuthenticatedWorker,
) (llm.WorkerConnection, error) {
	if ctx == nil {
		return nil, errors.New("initialization context is required")
	}
	if err := principal.Validate(); err != nil {
		return nil, err
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.closed {
		return nil, llm.ErrWorkerTransportClosed
	}
	if endpoint.active != nil && !endpoint.active.isDone() {
		return nil, llm.ErrWorkerConnectionConflict
	}
	connection := &fakeLLMWorkerConnection{
		endpoint: endpoint, principal: principal,
		assignments: make(chan llm.WorkerAssignmentDelivery, 1),
		done:        make(chan struct{}),
	}
	endpoint.active = connection
	endpoint.offerNextAssignmentLocked(connection)
	return connection, nil
}

func (endpoint *fakeLLMWorkerEndpoint) offerNextAssignmentLocked(connection *fakeLLMWorkerConnection) {
	for _, assignment := range endpoint.assignments {
		if endpoint.assignmentACKed[assignment.ID] ||
			assignment.Assignment.Lease.Owner != connection.principal.WorkerID {
			continue
		}
		// The bounded connection window is allowed to stay full. Never block
		// while holding endpoint.mu and never drop the durable source record; a
		// legitimate ACK follows a receive and therefore frees this one slot.
		select {
		case connection.assignments <- llm.CloneWorkerAssignmentDelivery(assignment):
		default:
		}
		return
	}
}

func (endpoint *fakeLLMWorkerEndpoint) ackAssignment(
	connection *fakeLLMWorkerConnection,
	deliveryID llm.WorkerDeliveryID,
) error {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.active != connection || connection.isDone() {
		return llm.ErrWorkerConnectionClosed
	}
	for _, assignment := range endpoint.assignments {
		if endpoint.assignmentACKed[assignment.ID] ||
			assignment.Assignment.Lease.Owner != connection.principal.WorkerID {
			continue
		}
		if assignment.ID != deliveryID {
			return llm.ErrWorkerDeliveryNotFound
		}
		endpoint.assignmentACKed[deliveryID] = true
		endpoint.offerNextAssignmentLocked(connection)
		return nil
	}
	return llm.ErrWorkerDeliveryNotFound
}

func (endpoint *fakeLLMWorkerEndpoint) commitEvent(
	connection *fakeLLMWorkerConnection,
	delivery llm.WorkerEventDelivery,
) (llm.WorkerEventReceipt, error) {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.active != connection || connection.isDone() {
		return llm.WorkerEventReceipt{}, llm.ErrWorkerConnectionClosed
	}
	assignment, found := endpoint.assignmentFor(delivery.Identity)
	if !found || assignment.Assignment.Lease.Owner != connection.principal.WorkerID {
		return llm.WorkerEventReceipt{}, &llm.WorkerConnectionError{
			Principal: connection.principal,
			Cause:     llm.ErrWorkerDeliveryNotFound,
		}
	}
	if assignment.Assignment.Lease.ID != delivery.LeaseID {
		return fakeNACK(delivery, llm.WorkerRejectStaleLease, "worker lease generation is stale"), nil
	}

	digest, err := eventDigest(delivery.Event)
	if err != nil {
		return llm.WorkerEventReceipt{}, err
	}
	key := completionKey(delivery.Identity)
	commitKey := key + "\x00" + delivery.Event.ID
	if previous, exists := endpoint.committed[commitKey]; exists {
		if previous.digest != digest {
			return fakeNACK(delivery, llm.WorkerRejectEventConflict, "event id has different content"), nil
		}
		return fakeACK(delivery), nil
	}
	if endpoint.closedResponses[key] {
		return fakeNACK(delivery, llm.WorkerRejectResponseClosed, "completion response is already closed"), nil
	}
	if err := delivery.ValidateFor(connection.principal); err != nil {
		return fakeNACK(delivery, llm.WorkerRejectInvalid, err.Error()), nil
	}

	// This is the fake core's commit boundary: response wire first, exact event
	// receipt second, then and only then may ACK escape this method.
	wire, err := json.Marshal(delivery.Event)
	if err != nil {
		return llm.WorkerEventReceipt{}, err
	}
	endpoint.committed[commitKey] = fakeEventCommit{
		digest: digest, wire: append([]byte(nil), wire...), receipt: true,
	}
	if delivery.Event.EndsResponse() {
		endpoint.closedResponses[key] = true
	}
	if endpoint.dropACK[delivery.Event.ID] && !endpoint.droppedACK[delivery.Event.ID] {
		endpoint.droppedACK[delivery.Event.ID] = true
		return llm.WorkerEventReceipt{}, &llm.WorkerDeliveryError{
			Delivery: delivery.ID,
			EventID:  delivery.Event.ID,
			Cause:    llm.ErrWorkerConnectionClosed,
		}
	}
	return fakeACK(delivery), nil
}

func (endpoint *fakeLLMWorkerEndpoint) assignmentFor(
	identity llm.CompletionIdentity,
) (llm.WorkerAssignmentDelivery, bool) {
	for _, assignment := range endpoint.assignments {
		if reflect.DeepEqual(assignment.Assignment.Identity, identity) {
			return assignment, true
		}
	}
	return llm.WorkerAssignmentDelivery{}, false
}

func (endpoint *fakeLLMWorkerEndpoint) dropACKOnce(eventID string) {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	endpoint.dropACK[eventID] = true
}

func (endpoint *fakeLLMWorkerEndpoint) durable(eventID string) bool {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	for key, commit := range endpoint.committed {
		if len(key) >= len(eventID) && key[len(key)-len(eventID):] == eventID {
			return len(commit.wire) != 0 && commit.receipt
		}
	}
	return false
}

type fakeLLMWorkerConnection struct {
	endpoint    *fakeLLMWorkerEndpoint
	principal   llm.AuthenticatedWorker
	assignments chan llm.WorkerAssignmentDelivery
	done        chan struct{}
	once        sync.Once
}

func (connection *fakeLLMWorkerConnection) Principal() llm.AuthenticatedWorker {
	return connection.principal
}
func (connection *fakeLLMWorkerConnection) Assignments() <-chan llm.WorkerAssignmentDelivery {
	return connection.assignments
}
func (connection *fakeLLMWorkerConnection) AckAssignment(
	ctx context.Context,
	deliveryID llm.WorkerDeliveryID,
) error {
	if ctx == nil {
		return errors.New("ACK context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return connection.endpoint.ackAssignment(connection, deliveryID)
}
func (connection *fakeLLMWorkerConnection) CommitEvent(
	ctx context.Context,
	delivery llm.WorkerEventDelivery,
) (llm.WorkerEventReceipt, error) {
	if ctx == nil {
		return llm.WorkerEventReceipt{}, errors.New("commit context is required")
	}
	if err := ctx.Err(); err != nil {
		return llm.WorkerEventReceipt{}, err
	}
	return connection.endpoint.commitEvent(connection, llm.CloneWorkerEventDelivery(delivery))
}
func (connection *fakeLLMWorkerConnection) Done() <-chan struct{} { return connection.done }
func (*fakeLLMWorkerConnection) Err() error                       { return nil }
func (connection *fakeLLMWorkerConnection) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("shutdown context is required")
	}
	connection.once.Do(func() {
		close(connection.done)
		connection.endpoint.mu.Lock()
		if connection.endpoint.active == connection {
			connection.endpoint.active = nil
		}
		close(connection.assignments)
		connection.endpoint.mu.Unlock()
	})
	return nil
}
func (connection *fakeLLMWorkerConnection) isDone() bool {
	select {
	case <-connection.done:
		return true
	default:
		return false
	}
}

var (
	_ llm.WorkerEndpoint   = (*fakeLLMWorkerEndpoint)(nil)
	_ llm.WorkerConnection = (*fakeLLMWorkerConnection)(nil)
)

type fakeWorkerOutbox struct {
	deliveries []llm.WorkerEventDelivery
}

func (outbox *fakeWorkerOutbox) put(delivery llm.WorkerEventDelivery) {
	outbox.deliveries = append(outbox.deliveries, llm.CloneWorkerEventDelivery(delivery))
}

func (outbox *fakeWorkerOutbox) flush(
	ctx context.Context,
	connection llm.WorkerConnection,
) ([]llm.WorkerEventReceipt, error) {
	var receipts []llm.WorkerEventReceipt
	for len(outbox.deliveries) != 0 {
		delivery := outbox.deliveries[0]
		receipt, err := connection.CommitEvent(ctx, delivery)
		if err != nil {
			return receipts, err
		}
		if err := receipt.ValidateFor(delivery); err != nil {
			return receipts, fmt.Errorf("validate fake settlement: %w", err)
		}
		receipts = append(receipts, receipt)
		outbox.deliveries = outbox.deliveries[1:]
	}
	return receipts, nil
}

func (outbox *fakeWorkerOutbox) len() int { return len(outbox.deliveries) }

func fakeACK(delivery llm.WorkerEventDelivery) llm.WorkerEventReceipt {
	return llm.WorkerEventReceipt{
		Delivery: delivery.ID,
		EventID:  delivery.Event.ID,
		Decision: llm.WorkerEventACK,
	}
}

func fakeNACK(
	delivery llm.WorkerEventDelivery,
	code llm.WorkerRejectionCode,
	message string,
) llm.WorkerEventReceipt {
	return llm.WorkerEventReceipt{
		Delivery: delivery.ID,
		EventID:  delivery.Event.ID,
		Decision: llm.WorkerEventNACK,
		Code:     code,
		Message:  message,
	}
}

func eventDigest(event llm.Event) (string, error) {
	encoded, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func completionKey(identity llm.CompletionIdentity) string {
	return string(identity.CallerID) + "\x00" + identity.RequestID + "\x00" +
		string(identity.IdempotencyKey)
}

func testPrincipal(session string) llm.AuthenticatedWorker {
	return llm.AuthenticatedWorker{WorkerID: "worker-a", SessionID: llm.WorkerSessionID(session)}
}

func testAssignment(suffix string, stream bool) llm.WorkerAssignmentDelivery {
	boundary := llm.AssignmentAfterAdmission
	if stream {
		boundary = llm.AssignmentAfterResponse
	}
	return llm.WorkerAssignmentDelivery{
		ID: llm.WorkerDeliveryID("assignment-" + suffix),
		Assignment: llm.Assignment{
			Identity: llm.CompletionIdentity{
				CallerID:       "caller-a",
				RequestID:      "request-" + suffix,
				TaskID:         llm.TaskID("task-" + suffix),
				WorkspaceKey:   "workspace-a",
				IdempotencyKey: llm.IdempotencyKey("idempotency-" + suffix),
			},
			Lease: llm.WorkerLease{
				ID: llm.WorkerLeaseID("lease-" + suffix), Owner: "worker-a",
			},
			Boundary: boundary,
			Request: llm.Request{
				Model:  "human-expert",
				Stream: stream,
				Messages: []llm.Message{{
					Role: llm.RoleUser,
					Blocks: []llm.Block{{
						Type: llm.BlockText, Text: "inspect workspace",
					}},
				}},
				Metadata: map[string]string{"scope": suffix},
			},
		},
	}
}

func testEvent(suffix string, eventType llm.EventType) llm.WorkerEventDelivery {
	assignment := testAssignment("replay", true)
	event := llm.Event{ID: "event-" + suffix, Type: eventType}
	switch eventType {
	case llm.EventAccepted:
	case llm.EventProgress, llm.EventFinal, llm.EventClarification:
		event.Text = suffix + " text"
	case llm.EventToolCalls:
		event.ToolCalls = []llm.ToolCall{{
			ID: "call-" + suffix, Name: "read_file", Input: map[string]any{"path": "README.md"},
		}}
	case llm.EventRejected, llm.EventExpired, llm.EventFailed, llm.EventUnavailable:
		event.ErrorCode = "human_" + suffix
		event.Error = suffix + " failure"
	}
	return llm.WorkerEventDelivery{
		ID:       llm.WorkerDeliveryID("delivery-" + suffix),
		Identity: assignment.Assignment.Identity,
		LeaseID:  assignment.Assignment.Lease.ID,
		Event:    event,
	}
}

func receiveAssignment(t *testing.T, connection llm.WorkerConnection) llm.WorkerAssignmentDelivery {
	t.Helper()
	select {
	case assignment := <-connection.Assignments():
		return assignment
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for assignment")
		return llm.WorkerAssignmentDelivery{}
	}
}
