package workerkit

import (
	"context"

	"github.com/vibe-agi/human/llm"
)

// ChangeKind classifies one reviewed workspace change.
type ChangeKind string

const (
	ChangeCreate ChangeKind = "create"
	ChangeModify ChangeKind = "modify"
	ChangeDelete ChangeKind = "delete"
)

// Change is one reviewable difference between the human's mirror and the last
// delivered baseline. Diff is display text; the mirror owns the exact bytes.
type Change struct {
	ID      string     `json:"id"`
	Path    string     `json:"path"`
	Kind    ChangeKind `json:"kind"`
	Diff    string     `json:"diff,omitempty"`
	Warning string     `json:"warning,omitempty"`
}

// Review is one complete, self-consistent view of every pending change. Each
// review replaces the previous one wholesale; Generation increases with every
// publication.
type Review struct {
	Generation uint64   `json:"generation"`
	Changes    []Change `json:"changes"`
}

// MirrorResolve asks the mirror to project selected changes onto the caller's
// declared native tools. Tools are the caller-declared tools of the current
// assignment; the mirror (or its host-supplied builder) must only produce
// calls for tools in this list.
type MirrorResolve struct {
	ChangeIDs []string
	Tools     []llm.Tool
}

// MirrorOutcome is the terminal fate of a reviewed change set.
type MirrorOutcome string

const (
	// MirrorDelivered means the caller agent returned successful tool results
	// for every call built from these changes; the mirror advances its baseline.
	MirrorDelivered MirrorOutcome = "delivered"
	// MirrorDiscarded means the human dropped the changes without delivery.
	MirrorDiscarded MirrorOutcome = "discarded"
)

// MirrorSettlement finalizes a resolved change set. Settle must be idempotent
// for an exact repeat.
type MirrorSettlement struct {
	ChangeIDs []string
	Outcome   MirrorOutcome
}

// Mirror is the Live Workspace port. The mirror owns the human's working copy,
// change detection, and the byte-exact content behind each change; workerkit
// owns when changes are delivered and settled:
//
//   - Reviews delivers complete replacement reviews (coalescing is the
//     implementation's choice); workerkit keeps only the latest.
//   - Resolve builds native tool calls for selected changes. It has no side
//     effects: a crash after Resolve but before the event is durably owned by
//     the transport must leave the changes pending in the next review.
//   - Settle(MirrorDelivered) is called only after the caller returned
//     successful results for every call of the batch — never at send time —
//     so the baseline advances exactly with the caller's working tree.
//     Settle(MirrorDiscarded) drops changes without delivery.
//
// A change that was resolved but never settled must reappear in a later
// review after restart; the mirror, not workerkit, is the durability owner of
// review state.
type Mirror interface {
	Reviews() <-chan Review
	Resolve(context.Context, MirrorResolve) ([]llm.ToolCall, error)
	Settle(context.Context, MirrorSettlement) error
}

// PendingDelivery records one in-flight reviewed batch on a conversation: the
// tool calls sent and the change IDs to settle when every call succeeds.
type PendingDelivery struct {
	ChangeIDs []string `json:"change_ids"`
	CallIDs   []string `json:"call_ids"`
}
