package workerkit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vibe-agi/human/llm"
)

// ConversationKey is the durable identity of one accepted conversation. It is
// the completion task scope: every assignment of the same caller task resumes
// the same conversation.
type ConversationKey struct {
	Caller llm.CallerID `json:"caller"`
	TaskID llm.TaskID   `json:"task_id"`
}

func (key ConversationKey) validate() error {
	if key.Caller == "" || key.TaskID == "" {
		return fmt.Errorf("%w: conversation key requires caller and task", ErrInvalidCommand)
	}
	return nil
}

// Phase is the human-side lifecycle of one conversation. It mirrors, but does
// not replace, the core task state machine: the core remains the correctness
// authority.
type Phase string

const (
	// PhaseActive accepts Reply/Clarify/Final/SubmitToolCalls commands.
	PhaseActive Phase = "active"
	// PhaseAwaitingResults is parked on submitted tool calls; the matching
	// caller continuation resumes it.
	PhaseAwaitingResults Phase = "awaiting_results"
	// PhaseAwaitingCaller follows a clarification; the caller's next request on
	// the same task resumes it.
	PhaseAwaitingCaller Phase = "awaiting_caller"
	// PhaseTerminal follows a final; the conversation accepts no more events.
	PhaseTerminal Phase = "terminal"
)

// EntryKind classifies one transcript entry.
type EntryKind string

const (
	EntryText          EntryKind = "text"
	EntryProgress      EntryKind = "progress"
	EntryClarification EntryKind = "clarification"
	EntryFinal         EntryKind = "final"
	EntryToolCalls     EntryKind = "tool_calls"
	EntryToolResult    EntryKind = "tool_result"
	EntryRejected      EntryKind = "rejected"
)

// Author identifies who produced a transcript entry.
type Author string

const (
	AuthorCaller Author = "caller"
	AuthorHuman  Author = "human"
	AuthorSystem Author = "system"
)

// TranscriptEntry is display state, not correctness state: the durable outbox
// and the core's response events remain the correctness record.
type TranscriptEntry struct {
	At         time.Time      `json:"at"`
	Author     Author         `json:"author"`
	Kind       EntryKind      `json:"kind"`
	Text       string         `json:"text,omitempty"`
	ToolCalls  []llm.ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	Code       string         `json:"code,omitempty"`
}

// Conversation is the persisted record of one accepted task-scoped exchange.
// Assignment is the current (latest) assignment; events always bind to its
// identity and lease.
type Conversation struct {
	Key         ConversationKey              `json:"key"`
	Phase       Phase                        `json:"phase"`
	Assignment  llm.WorkerAssignmentDelivery `json:"assignment"`
	Transcript  []TranscriptEntry            `json:"transcript"`
	ParkedCalls []llm.ToolCall               `json:"parked_calls,omitempty"`
	// Delivery is the in-flight reviewed batch, settled when every call of the
	// batch returns a successful result.
	Delivery *PendingDelivery `json:"delivery,omitempty"`
	Draft    string           `json:"draft,omitempty"`
	// HumanWorkspace is this conversation's Human-host working directory. It
	// never describes or exposes the Agent user's filesystem.
	HumanWorkspace *HumanWorkspace `json:"human_workspace,omitempty"`
	// CallerGone is an advisory, persisted operator-safety flag. It permits a
	// locally active conversation to be abandoned only after the transport has
	// identified that exact task's caller as detached. Awaiting-caller/results
	// conversations remain abandonable without it.
	CallerGone bool      `json:"caller_gone,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// StateStore persists accepted conversations across worker restarts. It is
// deliberately tiny: workerkit serializes all access (single writer), records
// are whole-value upserts, and the store never interprets their content.
//
// Unaccepted inbox items are intentionally NOT stored here: an unconfirmed
// assignment is replayed by the transport after restart, so persisting it
// twice would create a second source of truth.
//
// Implementations own independent copies of every value after a successful
// SaveConversation and return caller-owned copies from ListConversations.
// Deleting an absent key is a no-op. Implementations must not retry callbacks
// and must be safe for use from one goroutine at a time.
type StateStore interface {
	SaveConversation(context.Context, Conversation) error
	DeleteConversation(context.Context, ConversationKey) error
	// ListConversations returns all conversations ordered by Key (caller, then
	// task) for deterministic recovery.
	ListConversations(context.Context) ([]Conversation, error)
}

// AlertStore is the optional durable extension for human-visible notices.
// Keeping it separate preserves the small StateStore contract for custom
// embedders, while the reference memory and SQLite implementations provide it
// so production alerts survive process restart and dismissal is durable.
type AlertStore interface {
	SaveAlert(context.Context, Notice) error
	DeleteAlert(context.Context, uint64) error
	ListAlerts(context.Context) ([]Notice, error)
}

// InboxItem is one assignment awaiting an accept/reject decision. It is not
// persisted by workerkit; the transport replays unconfirmed assignments.
type InboxItem struct {
	Delivery llm.WorkerDeliveryID
	Key      ConversationKey
	Tier     llm.CapabilityTier
	Preview  string
	// ToolCount is the number of caller-declared tools. Zero usually marks an
	// auxiliary request (e.g. OpenCode title/summary generation) rather than
	// the main conversation turn.
	ToolCount  int
	ReceivedAt time.Time
}

// Notice is a human-visible alert (e.g. a quarantined outbox row whose reply
// will never be delivered). Seq is stable for dismissal.
type Notice struct {
	Seq       uint64       `json:"seq"`
	At        time.Time    `json:"at"`
	Code      string       `json:"code"`
	Message   string       `json:"message"`
	Caller    llm.CallerID `json:"caller,omitempty"`
	TaskID    llm.TaskID   `json:"task_id,omitempty"`
	RequestID string       `json:"request_id,omitempty"`
}

// Validate checks the portable human-alert contract implemented by optional
// AlertStores. A notice may be global or identify one exact caller task; a
// half-populated identity is rejected so safety actions can never target an
// ambiguous conversation.
func (notice Notice) Validate() error {
	if notice.Seq == 0 || notice.At.IsZero() {
		return fmt.Errorf("%w: alert sequence and timestamp are required", ErrInvalidCommand)
	}
	code := strings.TrimSpace(notice.Code)
	if code == "" || code != notice.Code || len(code) > 128 || !utf8.ValidString(code) {
		return fmt.Errorf("%w: alert code is invalid", ErrInvalidCommand)
	}
	message := strings.TrimSpace(notice.Message)
	if message == "" || len(notice.Message) > maxTranscriptTextBytes || !utf8.ValidString(notice.Message) {
		return fmt.Errorf("%w: alert message is invalid", ErrInvalidCommand)
	}
	if (notice.Caller == "") != (notice.TaskID == "") {
		return fmt.Errorf("%w: alert caller and task must be supplied together", ErrInvalidCommand)
	}
	if notice.RequestID != "" && (notice.Caller == "" || strings.TrimSpace(notice.RequestID) != notice.RequestID) {
		return fmt.Errorf("%w: alert request id requires an exact caller task", ErrInvalidCommand)
	}
	return nil
}

// State is a coherent snapshot for UIs. All values are deep copies; mutating
// them never affects the worker.
type State struct {
	Inbox         []InboxItem
	Conversations []Conversation
	// Review is the latest complete Live Workspace review, or nil without a
	// configured Mirror (or before its first publication).
	Review *Review
	// Alerts are undismissed human-visible notices, oldest first.
	Alerts []Notice
}

func cloneConversation(conversation Conversation) Conversation {
	encoded, err := json.Marshal(conversation)
	if err != nil {
		// Conversation is a closed set of JSON-safe fields; failure here is a
		// programming error surfaced loudly rather than silently shared state.
		panic(fmt.Sprintf("workerkit: conversation is not JSON-safe: %v", err))
	}
	var cloned Conversation
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		panic(fmt.Sprintf("workerkit: conversation clone failed: %v", err))
	}
	return cloned
}
