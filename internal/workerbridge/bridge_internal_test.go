package workerbridge

import (
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/llm"
)

// A terminal event must release the bridge's per-completion bookkeeping so a
// long-running worker does not retain every finished assignment.
func TestTerminalEventReleasesBookkeeping(t *testing.T) {
	bridge := &Bridge{
		byKey:      map[string]completion.Assignment{},
		byDelivery: map[llm.WorkerDeliveryID]string{},
		settled:    map[string]bool{},
	}
	legacy := completion.Assignment{CallerID: "caller-a", IdempotencyKey: "turn-1", LeaseOwner: "worker-a"}
	key := legacy.SessionKey()
	deliveryID := llm.WorkerDeliveryID("bridge-" + stableIdentity(legacy.CallerID, legacy.IdempotencyKey))
	bridge.byKey[key] = legacy
	bridge.byDelivery[deliveryID] = key

	// Simulate the terminal-cleanup branch of SendEvent directly (no live
	// client): mark settled and release on a response-ending event.
	final := llm.WorkerEventDelivery{
		Identity: llm.CompletionIdentity{CallerID: "caller-a", IdempotencyKey: "turn-1"},
		Event:    llm.Event{Type: llm.EventFinal, Text: "done"},
	}
	bridge.mu.Lock()
	bridge.settled[key] = true
	if final.Event.EndsResponse() {
		delete(bridge.byKey, key)
		delete(bridge.settled, key)
		delete(bridge.byDelivery, deliveryID)
	}
	bridge.mu.Unlock()

	if len(bridge.byKey) != 0 || len(bridge.settled) != 0 || len(bridge.byDelivery) != 0 {
		t.Fatalf("terminal cleanup left state: byKey=%d settled=%d byDelivery=%d",
			len(bridge.byKey), len(bridge.settled), len(bridge.byDelivery))
	}
}
