package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	storeapi "github.com/vibe-agi/human/internal/store"
)

const (
	headerOpenCodeSessionID       = "X-Session-Id"
	headerOpenCodeSessionAffinity = "X-Session-Affinity"
	openCodeRetryKeyDomain        = "human:auto:opencode-turn:v1"
	openCodeDerivedKeyPrefix      = "auto:opencode-turn:v1:"
	openCodeTaskKeyDomain         = "human:auto:opencode-task:v1"
	openCodeTaskPrefix            = "opencode-task:v1:"
)

// completeOpenCodeIdentity maps the exact session identity emitted by the
// versioned OpenCode adapter plus the current user-turn boundary into Human's
// durable task namespace. X-Session-Id spans many user turns, while one Human
// task spans only the completion/tool-result loop for one turn. This is an
// explicit adapter contract, not user-agent or prompt inference: callers must
// still opt in with the exact harness id/version and workspace headers.
func completeOpenCodeIdentity(
	identity *completion.RoutingIdentity,
	header http.Header,
	userAgent string,
	request canonical.Request,
) error {
	if identity == nil || identity.HarnessID != adapter.OpenCodeID ||
		identity.HarnessVersion != adapter.OpenCodeVersion {
		return nil
	}
	if !strings.HasPrefix(userAgent, "opencode/"+adapter.OpenCodeVersion+" ") {
		return errors.New("exact OpenCode workspace profile requires the captured OpenCode User-Agent")
	}
	if identity.TaskID != "" {
		return errors.New("exact OpenCode workspace profile derives task identity; X-Human-Task-Id must be omitted")
	}
	values := header.Values(headerOpenCodeSessionID)
	if len(values) == 0 {
		return errors.New("exact OpenCode workspace profile requires X-Session-Id")
	}
	if len(values) != 1 || strings.TrimSpace(values[0]) == "" {
		return errors.New("OpenCode session identity must use exactly one non-empty X-Session-Id value")
	}
	sessionID := strings.TrimSpace(values[0])
	if !completion.IsStableKey(sessionID) {
		return errors.New("OpenCode X-Session-Id must be a stable key")
	}
	if affinities := header.Values(headerOpenCodeSessionAffinity); len(affinities) > 1 ||
		(len(affinities) == 1 && strings.TrimSpace(affinities[0]) != sessionID) {
		return errors.New("OpenCode X-Session-Affinity must match X-Session-Id")
	}
	taskID, err := openCodeTurnTaskID(sessionID, request)
	if err != nil {
		return err
	}
	identity.TaskID = taskID
	identity.HarnessSessionID = sessionID
	return nil
}

// resumeOpenCodeTask preserves one serial Human task across clarification and
// native tool-result completions. A harness session can contain many terminal
// tasks over time, but at most one non-terminal task; only a final/rejected/
// expired/failed task releases the affinity for the next top-level user turn.
func resumeOpenCodeTask(
	ctx context.Context,
	store storeapi.CompletionStore,
	identity *completion.RoutingIdentity,
) error {
	if identity == nil || identity.HarnessSessionID == "" || identity.TaskID == "" ||
		identity.HarnessID != adapter.OpenCodeID || identity.HarnessVersion != adapter.OpenCodeVersion {
		return nil
	}
	task, err := store.FindOpenHarnessTask(ctx, storeapi.TaskAffinity{
		CallerID: identity.CallerID, WorkspaceKey: identity.WorkspaceKey,
		HarnessID: identity.HarnessID, HarnessVersion: identity.HarnessVersion,
		HarnessSessionID: identity.HarnessSessionID,
	})
	if errors.Is(err, storeapi.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	identity.TaskID = task.TaskID
	return nil
}

func openCodeTurnTaskID(sessionID string, request canonical.Request) (string, error) {
	if request.Dialect != canonical.DialectOpenAIChat {
		return "", errors.New("OpenCode turn identity requires OpenAI Chat canonical input")
	}
	lastUser := -1
	for index := len(request.Messages) - 1; index >= 0; index-- {
		if request.Messages[index].Role == canonical.RoleUser {
			lastUser = index
			break
		}
	}
	if lastUser < 0 {
		return "", errors.New("OpenCode turn identity requires a user message")
	}
	anchor := struct {
		Domain   string              `json:"domain"`
		Session  string              `json:"session"`
		Model    string              `json:"model"`
		System   string              `json:"system,omitempty"`
		Messages []canonical.Message `json:"messages"`
	}{
		Domain: openCodeTaskKeyDomain, Session: sessionID,
		Model: request.Model, System: request.System,
		Messages: request.Messages[:lastUser+1],
	}
	payload, err := json.Marshal(anchor)
	if err != nil {
		return "", fmt.Errorf("encode OpenCode turn identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return openCodeTaskPrefix + hex.EncodeToString(digest[:]), nil
}

// resolveOpenCodeIdempotencyKey deduplicates transport retries within one
// OpenCode session without collapsing distinct turns. Exact semantic request
// equality is part of the key, so a later completion in the same session gets
// a new key as soon as its full history or options change. The key binds the
// harness session rather than the resolved task: an awaiting-caller lookup may
// legitimately change task selection between two concurrent transport retries,
// but it must never change their request identity.
func resolveOpenCodeIdempotencyKey(
	explicitKey string,
	identity completion.RoutingIdentity,
	tier completion.CapabilityTier,
	userAgent string,
	request canonical.Request,
	rawBody []byte,
) (string, error) {
	if explicitKey != "" {
		return explicitKey, nil
	}
	if identity.CallerID == "" || identity.TaskID == "" || identity.HarnessSessionID == "" ||
		identity.HarnessID != adapter.OpenCodeID || identity.HarnessVersion != adapter.OpenCodeVersion ||
		(tier != completion.TierRemoteTools && tier != completion.TierWorkspace) ||
		request.Dialect != canonical.DialectOpenAIChat ||
		!strings.HasPrefix(userAgent, "opencode/"+adapter.OpenCodeVersion+" ") {
		return "", nil
	}
	canonicalDigest, err := request.Digest()
	if err != nil {
		return "", errors.New("OpenCode request has no canonical digest")
	}
	semanticBodyDigest, err := jsonSemanticDigest(rawBody)
	if err != nil {
		return "", errors.New("OpenCode request body is not unambiguous JSON")
	}
	digest := sha256.New()
	writeDigestField(digest, openCodeRetryKeyDomain)
	writeDigestField(digest, identity.CallerID)
	writeDigestField(digest, identity.WorkspaceKey)
	writeDigestField(digest, identity.HarnessSessionID)
	writeDigestField(digest, canonicalDigest)
	writeDigestField(digest, hex.EncodeToString(semanticBodyDigest[:]))
	return openCodeDerivedKeyPrefix + hex.EncodeToString(digest.Sum(nil)), nil
}
