package completion

import (
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

type EventType string

const (
	EventAccepted      EventType = "accepted"
	EventProgress      EventType = "progress"
	EventFinal         EventType = "final"
	EventClarification EventType = "clarification"
	EventToolCalls     EventType = "tool_calls"
	EventRejected      EventType = "rejected"
	EventExpired       EventType = "expired"
	EventFailed        EventType = "failed"
	EventUnavailable   EventType = "unavailable"
)

type ToolCall struct {
	ID    string         `json:"id"`
	Name  string         `json:"name"`
	Input map[string]any `json:"input"`
}

type Event struct {
	ID        string     `json:"id,omitempty"`
	Type      EventType  `json:"type"`
	WorkerID  string     `json:"worker_id,omitempty"`
	Text      string     `json:"text,omitempty"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	ErrorCode string     `json:"error_code,omitempty"`
	Error     string     `json:"error,omitempty"`
}

func (event Event) EndsResponse() bool {
	switch event.Type {
	case EventFinal, EventClarification, EventToolCalls, EventRejected,
		EventExpired, EventFailed, EventUnavailable:
		return true
	default:
		return false
	}
}

type Assignment struct {
	CallerID       string            `json:"caller_id"`
	WorkspaceKey   string            `json:"workspace_key,omitempty"`
	TaskID         string            `json:"task_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	LeaseOwner     string            `json:"lease_owner"`
	CapabilityTier CapabilityTier    `json:"capability_tier"`
	HarnessID      string            `json:"harness_id,omitempty"`
	HarnessVersion string            `json:"harness_version,omitempty"`
	Root           string            `json:"root,omitempty"`
	ExecAllowed    bool              `json:"exec_allowed,omitempty"`
	Adapter        *adapter.Profile  `json:"adapter,omitempty"`
	Request        canonical.Request `json:"request"`
}

func (assignment Assignment) SessionKey() string {
	return assignment.CallerID + "\x00" + assignment.IdempotencyKey
}
