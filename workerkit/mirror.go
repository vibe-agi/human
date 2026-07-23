package workerkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"

	"github.com/vibe-agi/human/llm"
)

var workspaceScopeKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// WorkspaceScope binds one Human-owned mirror to one authenticated Agent-user
// workspace. It is deliberately opaque: it routes changes but does not imply
// that the Human machine can resolve or mount the caller's directory.
type WorkspaceScope struct {
	Caller       llm.CallerID `json:"caller"`
	WorkspaceKey string       `json:"workspace_key"`
}

// Validate verifies the stable routing identity of a mirror scope.
func (scope WorkspaceScope) Validate() error {
	if !workspaceScopeKey.MatchString(string(scope.Caller)) ||
		!workspaceScopeKey.MatchString(scope.WorkspaceKey) {
		return fmt.Errorf("%w: mirror scope requires stable caller and workspace keys", ErrInvalidConfig)
	}
	return nil
}

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
	ID          string     `json:"id"`
	WorkspaceID string     `json:"workspace_id"`
	Path        string     `json:"path"`
	Kind        ChangeKind `json:"kind"`
	Diff        string     `json:"diff,omitempty"`
	Warning     string     `json:"warning,omitempty"`
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
	// Scope must match the scope bound to this Human mirror.
	Scope WorkspaceScope
	// Workspace identifies the Human-side session directory selected for this
	// conversation. Change paths passed to builders are relative to this
	// directory and therefore relative to the Agent user's logical project.
	Workspace HumanWorkspace
	// HarnessID and HarnessVersion select an exact, versioned native tool
	// contract. A stock multi-harness mirror must fail closed when no matching
	// builder is registered; tool names alone are not sufficient authority.
	HarnessID      string
	HarnessVersion string
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
//   - Resolve builds native tool calls for selected changes and freezes their
//     exact bytes for the eventual baseline advance, hiding them from review
//     while in flight. If the delivery is never sent, workerkit calls Cancel
//     to return them to review; a crash before the event is durably owned by
//     the transport leaves them for the mirror to reseed into a later review.
//   - Cancel returns resolved-but-unsent changes to review without advancing
//     the baseline. It is idempotent and a no-op for unknown ids.
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
	Cancel(context.Context, []string) error
	Settle(context.Context, MirrorSettlement) error
}

// SessionBinding is the portable identity from which a Human-side session
// workspace is prepared. It deliberately contains no Agent filesystem path.
type SessionBinding struct {
	Scope            WorkspaceScope
	HarnessID        string
	HarnessVersion   string
	HarnessSessionID string
}

// HumanWorkspace is one session's Human-owned working directory. ID is safe to
// expose in reviews and persists across restarts; Path is meaningful only on
// the Human host.
type HumanWorkspace struct {
	ID        string `json:"id"`
	Path      string `json:"path"`
	Available bool   `json:"available"`
}

// SessionMirror is the optional per-conversation workspace extension. The
// reference filesystem Mirror implements it; custom Mirrors may omit it when
// they do not expose a Human working directory.
type SessionMirror interface {
	Mirror
	// PrepareSession binds the session to preferredPath. An empty path selects
	// the implementation's default beneath its Human base workspace. A
	// non-empty path is an explicit Human choice and must already exist.
	PrepareSession(context.Context, SessionBinding, string) (HumanWorkspace, error)
}

// SessionWorkspaceID returns a portable, stable directory name derived from
// the authenticated conversation identity. Raw harness session identifiers
// are never used as filesystem names.
func SessionWorkspaceID(binding SessionBinding) (string, error) {
	if err := binding.Scope.Validate(); err != nil {
		return "", err
	}
	if !workspaceScopeKey.MatchString(binding.HarnessID) ||
		!workspaceScopeKey.MatchString(binding.HarnessVersion) ||
		!workspaceScopeKey.MatchString(binding.HarnessSessionID) {
		return "", fmt.Errorf("%w: session binding requires stable harness identity", ErrInvalidConfig)
	}
	sum := sha256.New()
	for _, value := range []string{
		string(binding.Scope.Caller), binding.Scope.WorkspaceKey,
		binding.HarnessID, binding.HarnessVersion, binding.HarnessSessionID,
	} {
		sum.Write([]byte(value))
		sum.Write([]byte{0})
	}
	return "session-" + hex.EncodeToString(sum.Sum(nil))[:20], nil
}

// PendingDelivery records one in-flight reviewed batch on a conversation: the
// tool calls sent and the change IDs to settle when every call succeeds.
type PendingDelivery struct {
	ChangeIDs []string `json:"change_ids"`
	CallIDs   []string `json:"call_ids"`
}
