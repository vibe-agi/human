package workerkit_test

import (
	"context"
	"sync"
	"testing"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

// TestConcurrentFinalAndReplyDoNotBothLand proves the command lock serializes
// wire-side-effect commands: a Final and a Reply racing on the same active
// conversation cannot both send, and the conversation cannot revert from
// terminal back to active.
func TestConcurrentFinalAndReplyDoNotBothLand(t *testing.T) {
	for attempt := 0; attempt < 40; attempt++ {
		wire := newFakeWire()
		store, _ := workerkit.NewMemoryStateStore()
		worker := openTestWorker(t, wire, store)
		wire.assignments <- testAssignment("task-1", "delivery-1", llm.TierChat, textOnlyRequest("hello"))
		waitFor(t, worker, func(state workerkit.State) bool { return len(state.Inbox) == 1 })
		key, err := worker.Accept(t.Context(), "delivery-1")
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var finalErr, replyErr error
		go func() { defer wg.Done(); finalErr = worker.Final(context.Background(), key, "done") }()
		go func() { defer wg.Done(); replyErr = worker.Reply(context.Background(), key, "wait") }()
		wg.Wait()

		// Exactly one ordering is valid: Final first (Reply then rejected as
		// terminal) or Reply first (Final succeeds after). Both succeeding, or
		// the terminal conversation reverting to active, is the bug.
		state := worker.Snapshot()
		if len(state.Conversations) != 1 {
			t.Fatalf("attempt %d: conversations = %d", attempt, len(state.Conversations))
		}
		phase := state.Conversations[0].Phase
		events := wire.sentEvents()
		finalCount := 0
		for _, event := range events {
			if event.Event.Type == llm.EventFinal {
				finalCount++
			}
		}
		if finalErr == nil && replyErr == nil {
			// Both returned success only if Reply preceded Final; then phase is
			// terminal and exactly one final was sent.
			if phase != workerkit.PhaseTerminal || finalCount != 1 {
				t.Fatalf("attempt %d: both succeeded but phase=%s finals=%d events=%d",
					attempt, phase, finalCount, len(events))
			}
			continue
		}
		// If one failed, the survivor is Final (terminal) — Reply can never win
		// over an already-terminal conversation and leave it active.
		if finalErr == nil && phase != workerkit.PhaseTerminal {
			t.Fatalf("attempt %d: final succeeded but phase=%s", attempt, phase)
		}
		if finalCount > 1 {
			t.Fatalf("attempt %d: %d final events sent", attempt, finalCount)
		}
		_ = worker.Shutdown(context.Background())
	}
}

// TestConcurrentSubmitToolCallsRespectContinuationCap proves the parked cap is
// not a check-then-act TOCTOU: many concurrent SubmitToolCalls on distinct
// conversations never exceed MaxParkedContinuations parked conversations.
func TestConcurrentSubmitToolCallsRespectContinuationCap(t *testing.T) {
	wire := newFakeWire()
	store, _ := workerkit.NewMemoryStateStore()
	worker := openTestWorker(t, wire, store)

	const attempts = workerkit.MaxParkedContinuations + 16
	keys := make([]workerkit.ConversationKey, 0, attempts)
	for index := 0; index < attempts; index++ {
		task := "task-" + string(rune('a'+index%26)) + string(rune('0'+index/26))
		delivery := "delivery-" + task
		request := textOnlyRequest("work")
		request.Tools = []llm.Tool{{Name: "bash", InputSchema: []byte(`{"type":"object"}`)}}
		wire.assignments <- testAssignment(task, delivery, llm.TierWorkspace, request)
		waitFor(t, worker, func(state workerkit.State) bool {
			for _, item := range state.Inbox {
				if item.Delivery == llm.WorkerDeliveryID(delivery) {
					return true
				}
			}
			return false
		})
		key, err := worker.Accept(t.Context(), llm.WorkerDeliveryID(delivery))
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	for index, key := range keys {
		wg.Add(1)
		go func(index int, key workerkit.ConversationKey) {
			defer wg.Done()
			err := worker.SubmitToolCalls(context.Background(), key, []llm.ToolCall{{
				ID: "call", Name: "bash", Input: map[string]any{"n": index},
			}})
			if err == nil {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}(index, key)
	}
	wg.Wait()

	if accepted > workerkit.MaxParkedContinuations {
		t.Fatalf("parked cap breached: %d accepted, cap %d", accepted, workerkit.MaxParkedContinuations)
	}
	parked := 0
	for _, conversation := range worker.Snapshot().Conversations {
		if conversation.Phase == workerkit.PhaseAwaitingResults {
			parked++
		}
	}
	if parked > workerkit.MaxParkedContinuations {
		t.Fatalf("parked conversations = %d, cap %d", parked, workerkit.MaxParkedContinuations)
	}
}
