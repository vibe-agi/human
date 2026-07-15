package completion

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// CapabilityTier controls which caller-side capabilities a request may use.
// A request never acquires a more capable tier by heuristic inspection.
type CapabilityTier string

const (
	TierChat        CapabilityTier = "chat"
	TierRemoteTools CapabilityTier = "remote_tools"
	TierWorkspace   CapabilityTier = "workspace"
)

var stableKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// RoutingIdentity is the correctness namespace for a completion request.
// CallerID comes from authentication; the caller shim supplies the other
// stable identifiers for tool-capable tiers on every completion request.
type RoutingIdentity struct {
	CallerID       string
	WorkspaceKey   string
	TaskID         string
	IdempotencyKey string
	HarnessID      string
	HarnessVersion string
	Root           string
	ExecAllowed    bool
}

func ParseCapabilityTier(value string) (CapabilityTier, error) {
	switch tier := CapabilityTier(strings.ToLower(strings.TrimSpace(value))); tier {
	case "", TierChat:
		return TierChat, nil
	case TierRemoteTools:
		return TierRemoteTools, nil
	case TierWorkspace:
		return TierWorkspace, nil
	default:
		return "", fmt.Errorf("unknown capability tier %q", value)
	}
}

func (id RoutingIdentity) Validate(tier CapabilityTier) error {
	if !validStableKey(id.CallerID) {
		return errors.New("caller_id is required and must be a stable key")
	}
	if tier == TierChat {
		return nil
	}
	fields := []struct {
		name  string
		value string
	}{
		{"workspace_key", id.WorkspaceKey},
		{"task_id", id.TaskID},
		{"idempotency_key", id.IdempotencyKey},
		{"harness_id", id.HarnessID},
		{"harness_version", id.HarnessVersion},
	}
	for _, field := range fields {
		if !validStableKey(field.value) {
			return fmt.Errorf("%s is required and must be a stable key", field.name)
		}
	}
	if strings.TrimSpace(id.Root) == "" {
		return errors.New("workspace root is required")
	}
	return nil
}

func (id RoutingIdentity) Namespace() string {
	return id.CallerID + "/" + id.WorkspaceKey + "/" + id.TaskID
}

func validStableKey(value string) bool {
	return stableKeyPattern.MatchString(value)
}
