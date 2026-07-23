package web_test

import (
	"net/http"
	"testing"

	"github.com/vibe-agi/human/workerkit"
)

func TestWebAbandonTerminatesStuckConversation(t *testing.T) {
	wire, _, listener := openWebServer(t)
	wire.assignments <- chatAssignment("task-1", "delivery-1", "help me debug")
	waitForState(t, listener.URL, func(state map[string]any) bool {
		inbox, _ := state["inbox"].([]any)
		return len(inbox) == 1
	})
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/accept",
		map[string]string{"delivery": "delivery-1"}), http.StatusOK)

	// Hand the turn back with a question → awaiting_caller, a state that can get
	// stuck if the caller never replies.
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/clarify",
		map[string]string{"caller": "caller-a", "task_id": "task-1", "text": "which environment?"}), http.StatusOK)

	// Abandon the stuck conversation through the endpoint (no text, no wire event).
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/abandon",
		map[string]string{"caller": "caller-a", "task_id": "task-1"}), http.StatusOK)

	waitForState(t, listener.URL, func(state map[string]any) bool {
		conversations, _ := state["conversations"].([]any)
		for _, raw := range conversations {
			conversation, _ := raw.(map[string]any)
			key, _ := conversation["key"].(map[string]any)
			if key["task_id"] == "task-1" {
				return conversation["phase"] == "terminal"
			}
		}
		return false
	})
}

func TestWebAbandonActiveSucceedsAndUnknownIs404(t *testing.T) {
	wire, _, listener := openWebServer(t)
	wire.assignments <- chatAssignment("task-1", "delivery-1", "help")
	waitForState(t, listener.URL, func(state map[string]any) bool {
		inbox, _ := state["inbox"].([]any)
		return len(inbox) == 1
	})
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/accept",
		map[string]string{"delivery": "delivery-1"}), http.StatusOK)

	// A live active conversation cannot be abandoned until its exact caller is
	// known to be gone.
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/abandon",
		map[string]string{"caller": "caller-a", "task_id": "task-1"}), http.StatusConflict)
	wire.notices <- workerkit.Notice{
		Code: "caller_gone", Message: "caller disconnected", Caller: "caller-a", TaskID: "task-1",
	}
	waitForState(t, listener.URL, func(state map[string]any) bool {
		alerts, _ := state["alerts"].([]any)
		return len(alerts) == 1
	})
	// A stuck active conversation with a detached caller can be abandoned.
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/abandon",
		map[string]string{"caller": "caller-a", "task_id": "task-1"}), http.StatusOK)

	// An unknown conversation is a 404.
	doJSON(t, authedRequest(t, http.MethodPost, listener.URL+"/api/abandon",
		map[string]string{"caller": "nobody", "task_id": "nope"}), http.StatusNotFound)
}
