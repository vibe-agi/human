// Package workerbridge adapts the frozen legacy worker WebSocket client to
// the public workerkit.Wire port, so the shipped products (human local,
// human worker) can run the web human side against the existing gateway
// while the caller half of dual-stack convergence is still pending.
//
// The bridge is deliberately thin and deterministic:
//
//   - Legacy assignments have no transport delivery ID; the bridge derives a
//     stable one from the caller/idempotency identity, so gateway redelivery
//     after reconnect deduplicates inside workerkit.
//   - The legacy dialect has no assignment ACK; ConfirmAssignment is a no-op
//     and the gateway's redelivery-on-reconnect remains the recovery path.
//   - SendEvent binds the event to the remembered legacy assignment and the
//     durable workerclient outbox; the outbox, not workerkit, owns replay.
//   - Rejections surface the legacy NACK inbox; ConfirmRejection maps to the
//     durable ConfirmRejectedEvent keyed by the bridged delivery ID, which the
//     bridge sets to the rejected event ID.
package workerbridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

// Config selects the legacy gateway endpoint and the durable outbox identity.
type Config struct {
	URL         string
	Token       string
	OutboxPath  string
	OutboxScope string
}

// Bridge implements workerkit.Wire over the legacy worker WebSocket dialect.
type Bridge struct {
	client *workerclient.Client

	assignments chan llm.WorkerAssignmentDelivery
	rejections  chan workerkit.Rejection
	done        chan struct{}

	mu         sync.Mutex
	byKey      map[string]completion.Assignment // caller\x00idempotency -> legacy assignment
	byDelivery map[llm.WorkerDeliveryID]string  // bridged delivery id -> session key
	settled    map[string]bool                  // session keys that already carried an event
	err        error
	closeone   sync.Once
}

var _ workerkit.Wire = (*Bridge)(nil)

// Dial connects the legacy worker client and starts the conversion pump. ctx
// bounds construction only; Close owns the runtime lifetime.
func Dial(ctx context.Context, config Config) (*Bridge, error) {
	client, err := workerclient.DialWithOutboxScope(
		ctx, config.URL, config.Token, config.OutboxPath, config.OutboxScope,
	)
	if err != nil {
		return nil, fmt.Errorf("workerbridge: dial legacy worker: %w", err)
	}
	bridge := &Bridge{
		client:      client,
		assignments: make(chan llm.WorkerAssignmentDelivery, 64),
		rejections:  make(chan workerkit.Rejection, 64),
		done:        make(chan struct{}),
		byKey:       make(map[string]completion.Assignment),
		byDelivery:  make(map[llm.WorkerDeliveryID]string),
		settled:     make(map[string]bool),
	}
	go bridge.pump()
	return bridge, nil
}

// Close shuts the underlying legacy client down and ends the pump.
func (bridge *Bridge) Close() error {
	var err error
	bridge.closeone.Do(func() {
		err = bridge.client.Close()
	})
	<-bridge.done
	return err
}

func (bridge *Bridge) Assignments() <-chan llm.WorkerAssignmentDelivery { return bridge.assignments }
func (bridge *Bridge) Rejections() <-chan workerkit.Rejection           { return bridge.rejections }
func (bridge *Bridge) Done() <-chan struct{}                            { return bridge.done }

func (bridge *Bridge) Err() error {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return bridge.err
}

func (bridge *Bridge) pump() {
	defer close(bridge.done)
	for message := range bridge.client.Messages() {
		switch {
		case message.Assignment != nil:
			legacy := *message.Assignment
			delivery, err := bridgeAssignment(legacy)
			if err != nil {
				// A malformed legacy assignment cannot be represented; record and
				// skip rather than wedging the channel.
				bridge.recordErr(err)
				continue
			}
			bridge.mu.Lock()
			bridge.byKey[legacy.SessionKey()] = legacy
			bridge.byDelivery[delivery.ID] = legacy.SessionKey()
			bridge.mu.Unlock()
			bridge.assignments <- delivery
		case message.EventRejected != nil:
			rejection, ok := bridge.bridgeRejection(message)
			if ok {
				bridge.rejections <- rejection
			}
		case message.Err != nil:
			bridge.recordErr(message.Err)
		}
	}
}

func (bridge *Bridge) recordErr(err error) {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	bridge.err = errors.Join(bridge.err, err)
}

func (bridge *Bridge) SendEvent(ctx context.Context, delivery llm.WorkerEventDelivery) error {
	key := string(delivery.Identity.CallerID) + "\x00" + string(delivery.Identity.IdempotencyKey)
	bridge.mu.Lock()
	assignment, known := bridge.byKey[key]
	bridge.settled[key] = true
	bridge.mu.Unlock()
	if !known {
		return fmt.Errorf("workerbridge: no legacy assignment for %s/%s",
			delivery.Identity.CallerID, delivery.Identity.IdempotencyKey)
	}
	event := completion.Event{
		ID:   delivery.Event.ID,
		Type: completion.EventType(delivery.Event.Type),
		Text: delivery.Event.Text,
	}
	for _, call := range delivery.Event.ToolCalls {
		event.ToolCalls = append(event.ToolCalls, completion.ToolCall{
			ID: call.ID, Namespace: call.Namespace, Name: call.Name, Input: call.Input,
		})
	}
	return bridge.client.SendEvent(ctx, assignment, event)
}

// ConfirmAssignment maps workerkit's accept to the legacy accepted event: the
// legacy state machine admits progress/final only after "accepted". The event
// is sent once per completion; a session that already carried an event (e.g. a
// rejection sent before confirmation) is left untouched.
func (bridge *Bridge) ConfirmAssignment(ctx context.Context, id llm.WorkerDeliveryID) error {
	bridge.mu.Lock()
	key, known := bridge.byDelivery[id]
	if !known || bridge.settled[key] {
		bridge.mu.Unlock()
		return nil
	}
	assignment := bridge.byKey[key]
	bridge.settled[key] = true
	bridge.mu.Unlock()
	return bridge.client.SendEvent(ctx, assignment, completion.Event{Type: completion.EventAccepted})
}

func (bridge *Bridge) ConfirmRejection(ctx context.Context, id llm.WorkerDeliveryID) error {
	// Bridged rejection delivery IDs are the rejected event IDs.
	return bridge.client.ConfirmRejectedEvent(ctx, string(id))
}

// bridgeAssignment converts the legacy DTO into the public transport shape.
// The canonical request models are JSON-isomorphic by construction; the
// legacy dialect discriminator is intentionally dropped.
func bridgeAssignment(legacy completion.Assignment) (llm.WorkerAssignmentDelivery, error) {
	encoded, err := json.Marshal(legacy.Request)
	if err != nil {
		return llm.WorkerAssignmentDelivery{}, fmt.Errorf("workerbridge: encode legacy request: %w", err)
	}
	var request llm.Request
	if err := json.Unmarshal(encoded, &request); err != nil {
		return llm.WorkerAssignmentDelivery{}, fmt.Errorf("workerbridge: decode bridged request: %w", err)
	}
	identity := stableIdentity(legacy.CallerID, legacy.IdempotencyKey)
	return llm.WorkerAssignmentDelivery{
		ID: llm.WorkerDeliveryID("bridge-" + identity),
		Assignment: llm.Assignment{
			Identity: llm.CompletionIdentity{
				CallerID:       llm.CallerID(legacy.CallerID),
				RequestID:      "bridge-req-" + identity,
				TaskID:         llm.TaskID(legacy.TaskID),
				WorkspaceKey:   legacy.WorkspaceKey,
				IdempotencyKey: llm.IdempotencyKey(legacy.IdempotencyKey),
			},
			Lease: llm.WorkerLease{
				ID:    llm.WorkerLeaseID("bridge-lease-" + identity),
				Owner: llm.WorkerID(legacy.LeaseOwner),
			},
			Boundary: llm.AssignmentAfterResponse,
			Task: llm.TaskContext{
				TaskID:           llm.TaskID(legacy.TaskID),
				WorkspaceKey:     legacy.WorkspaceKey,
				CapabilityTier:   llm.CapabilityTier(legacy.CapabilityTier),
				HarnessID:        legacy.HarnessID,
				HarnessVersion:   legacy.HarnessVersion,
				HarnessSessionID: legacy.HarnessSessionID,
				WorkspaceRoot:    legacy.Root,
				ExecAllowed:      legacy.ExecAllowed,
			},
			Request: request,
		},
	}, nil
}

func (bridge *Bridge) bridgeRejection(message workerclient.Message) (workerkit.Rejection, bool) {
	rejected := message.EventRejected
	var event llm.Event
	if message.RejectedEvent != nil {
		event = llm.Event{
			ID:   message.RejectedEvent.ID,
			Type: llm.EventType(message.RejectedEvent.Type),
			Text: message.RejectedEvent.Text,
		}
		for _, call := range message.RejectedEvent.ToolCalls {
			event.ToolCalls = append(event.ToolCalls, llm.ToolCall{
				ID: call.ID, Namespace: call.Namespace, Name: call.Name, Input: call.Input,
			})
		}
	} else {
		event = llm.Event{ID: rejected.EventID, Type: llm.EventProgress}
	}
	identity := stableIdentity(rejected.CallerID, rejected.IdempotencyKey)
	taskID := message.RejectedTaskID
	if message.RejectedAssignment != nil {
		taskID = message.RejectedAssignment.TaskID
	}
	delivery := llm.WorkerEventDelivery{
		ID: llm.WorkerDeliveryID(rejected.EventID),
		Identity: llm.CompletionIdentity{
			CallerID:       llm.CallerID(rejected.CallerID),
			RequestID:      "bridge-req-" + identity,
			TaskID:         llm.TaskID(taskID),
			IdempotencyKey: llm.IdempotencyKey(rejected.IdempotencyKey),
		},
		LeaseID: llm.WorkerLeaseID("bridge-lease-" + identity),
		Event:   event,
	}
	return workerkit.Rejection{
		Delivery: delivery,
		Receipt: llm.WorkerEventReceipt{
			Delivery: delivery.ID, EventID: rejected.EventID,
			Decision: llm.WorkerEventNACK, Code: llm.WorkerRejectInvalid,
			Message: rejected.Message,
		},
	}, true
}

func stableIdentity(caller, idempotency string) string {
	digest := sha256.Sum256([]byte(caller + "\x00" + idempotency))
	return hex.EncodeToString(digest[:12])
}
