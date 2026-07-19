package workerkit

import (
	"context"
	"sync"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/workerws"
)

// Rejection is the application-facing NACK: the exact event that was rejected
// together with the deterministic receipt.
type Rejection struct {
	Delivery llm.WorkerEventDelivery
	Receipt  llm.WorkerEventReceipt
}

// Wire is the transport port between workerkit and a HumanLLM deployment.
//
// The transport, not workerkit, owns delivery durability: SendEvent returns
// only after the event is durably owned by the transport (a remote client's
// Journal/outbox, or a synchronous in-process commit), and the transport
// replays unconfirmed assignments and unconfirmed rejections after a process
// restart. workerkit therefore never persists its own copy of in-flight
// deliveries; duplicating that state would create a second source of truth.
//
// Assignments and Rejections have workerkit as their only receiver. ACKs are
// silent; failures surface either as a SendEvent error (nothing was accepted)
// or as an asynchronous Rejection (a deterministic NACK).
type Wire interface {
	Assignments() <-chan llm.WorkerAssignmentDelivery
	Rejections() <-chan Rejection
	SendEvent(context.Context, llm.WorkerEventDelivery) error
	ConfirmAssignment(context.Context, llm.WorkerDeliveryID) error
	ConfirmRejection(context.Context, llm.WorkerDeliveryID) error
	Done() <-chan struct{}
	Err() error
}

// WrapClient adapts the official durable remote client to Wire. The client
// remains caller-owned: shut it down after the workerkit Worker.
func WrapClient(client *workerws.Client) Wire {
	adapter := &clientWire{client: client, rejections: make(chan Rejection, 64)}
	go adapter.pump()
	return adapter
}

type clientWire struct {
	client     *workerws.Client
	rejections chan Rejection
}

func (wire *clientWire) pump() {
	defer close(wire.rejections)
	for {
		select {
		case rejected, open := <-wire.client.Rejections():
			if !open {
				return
			}
			select {
			case wire.rejections <- Rejection{Delivery: rejected.Delivery, Receipt: rejected.Receipt}:
			case <-wire.client.Done():
				return
			}
		case <-wire.client.Done():
			return
		}
	}
}

func (wire *clientWire) Assignments() <-chan llm.WorkerAssignmentDelivery {
	return wire.client.Assignments()
}
func (wire *clientWire) Rejections() <-chan Rejection { return wire.rejections }
func (wire *clientWire) Done() <-chan struct{}        { return wire.client.Done() }
func (wire *clientWire) Err() error                   { return wire.client.Err() }

func (wire *clientWire) SendEvent(ctx context.Context, delivery llm.WorkerEventDelivery) error {
	return wire.client.SendEvent(ctx, delivery)
}

func (wire *clientWire) ConfirmAssignment(ctx context.Context, id llm.WorkerDeliveryID) error {
	return wire.client.ConfirmAssignment(ctx, id)
}

func (wire *clientWire) ConfirmRejection(ctx context.Context, id llm.WorkerDeliveryID) error {
	return wire.client.ConfirmRejection(ctx, id)
}

// WrapConnection adapts an in-process llm.WorkerConnection (an embedded
// llm.Service worker endpoint) to Wire. CommitEvent settles synchronously:
// SendEvent returns nil for an ACK, surfaces a deterministic NACK on the
// Rejections channel, and returns the error for anything indeterminate so the
// command can be retried with the same event identity.
//
// This adapter has no durable outbox of its own: an in-process worker shares
// the service's lifetime, and the service redelivers pending assignments on
// reconnect exactly as it does for remote workers.
func WrapConnection(connection llm.WorkerConnection) Wire {
	return &connectionWire{connection: connection, rejections: make(chan Rejection, 64)}
}

type connectionWire struct {
	connection llm.WorkerConnection
	mu         sync.Mutex
	rejections chan Rejection
}

func (wire *connectionWire) Assignments() <-chan llm.WorkerAssignmentDelivery {
	return wire.connection.Assignments()
}
func (wire *connectionWire) Rejections() <-chan Rejection { return wire.rejections }
func (wire *connectionWire) Done() <-chan struct{}        { return wire.connection.Done() }
func (wire *connectionWire) Err() error                   { return wire.connection.Err() }

func (wire *connectionWire) SendEvent(ctx context.Context, delivery llm.WorkerEventDelivery) error {
	receipt, err := wire.connection.CommitEvent(ctx, delivery)
	if err != nil {
		return err
	}
	if receipt.Decision == llm.WorkerEventNACK {
		wire.mu.Lock()
		defer wire.mu.Unlock()
		select {
		case wire.rejections <- Rejection{Delivery: llm.CloneWorkerEventDelivery(delivery), Receipt: receipt}:
		default:
			// A full buffer means the application stopped consuming; the NACK is
			// deterministic and the conversation will observe it on the next
			// command failure, so dropping here cannot lose correctness state.
		}
	}
	return nil
}

func (wire *connectionWire) ConfirmAssignment(ctx context.Context, id llm.WorkerDeliveryID) error {
	return wire.connection.AckAssignment(ctx, id)
}

func (wire *connectionWire) ConfirmRejection(context.Context, llm.WorkerDeliveryID) error {
	// The in-process adapter has no durable rejection inbox to compact.
	return nil
}
