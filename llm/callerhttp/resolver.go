package callerhttp

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/vibe-agi/human/llm"
)

const (
	HeaderCapabilityTier = "X-Human-Capability-Tier"
	HeaderHarnessID      = "X-Human-Harness-Id"
	HeaderHarnessVersion = "X-Human-Harness-Version"
	HeaderHarnessSession = "X-Human-Harness-Session-Id"
	HeaderAllowExec      = "X-Human-Allow-Exec"
)

// ResolutionRequest is a borrowed, immutable-by-contract view of the
// authenticated HTTP request. CallerID is already authenticated, Route is the
// exact configured route, and Body contains the bytes which will be passed to
// llm.CallerEndpoint.Admit.
type ResolutionRequest struct {
	CallerID llm.CallerID
	Route    Route
	Header   http.Header
	Body     []byte
}

// Resolution selects durable retry and Task identity. It cannot replace the
// authenticated caller or the Codec selected by Route.
type Resolution struct {
	IdempotencyKey llm.IdempotencyKey
	Task           llm.TaskContext
}

// RequestResolver allows a host to map its own trusted session/workspace
// system onto the transport-neutral HumanLLM identity boundary. It is borrowed
// until the transport runtime reaches Done, called concurrently, and must honor
// context cancellation. ResolutionRequest and all nested values are borrowed
// for the call and must not be retained or mutated. Invalid caller input may
// return an error matching ErrResolution or a framework CodeInvalid/RetryNever
// Fault. Authorization denial uses CodeForbidden/RetryNever; temporary routing
// or identity-store failures use CodeUnavailable/RetryBackoff. Unclassified
// errors are treated as infrastructure failures rather than client mistakes.
type RequestResolver interface {
	ResolveRequest(context.Context, ResolutionRequest) (Resolution, error)
}

type ResolveFunc func(context.Context, ResolutionRequest) (Resolution, error)

func (function ResolveFunc) ResolveRequest(
	ctx context.Context,
	request ResolutionRequest,
) (Resolution, error) {
	return function(ctx, request)
}

// HeaderResolver is the official strict, heuristic-free resolver. Chat is the
// default capability tier and rejects every workspace/Task header. Remote
// tools and workspace tiers require a complete X-Human-* affinity tuple.
type HeaderResolver struct{}

func (HeaderResolver) ResolveRequest(
	ctx context.Context,
	request ResolutionRequest,
) (Resolution, error) {
	if ctx == nil {
		return Resolution{}, fmt.Errorf("%w: context is required", ErrResolution)
	}
	if err := ctx.Err(); err != nil {
		return Resolution{}, err
	}
	idempotency, err := requiredHeader(request.Header, HeaderIdempotencyKey)
	if err != nil || !stableIdentity.MatchString(idempotency) {
		return Resolution{}, fmt.Errorf("%w: %s is required and must be a stable key", ErrResolution, HeaderIdempotencyKey)
	}
	tierValue, present, err := optionalHeader(request.Header, HeaderCapabilityTier)
	if err != nil {
		return Resolution{}, err
	}
	if !present {
		tierValue = string(llm.TierChat)
	}
	task := llm.TaskContext{CapabilityTier: llm.CapabilityTier(tierValue)}
	switch task.CapabilityTier {
	case llm.TierChat:
		for _, header := range taskHeaders() {
			if len(request.Header.Values(header)) != 0 {
				return Resolution{}, fmt.Errorf("%w: chat requests cannot carry %s", ErrResolution, header)
			}
		}
	case llm.TierRemoteTools, llm.TierWorkspace:
		workspace, err := requiredStableHeader(request.Header, HeaderWorkspaceKey)
		if err != nil {
			return Resolution{}, err
		}
		harness, err := requiredStableHeader(request.Header, HeaderHarnessID)
		if err != nil {
			return Resolution{}, err
		}
		version, err := requiredStableHeader(request.Header, HeaderHarnessVersion)
		if err != nil {
			return Resolution{}, err
		}
		session, err := requiredStableHeader(request.Header, HeaderHarnessSession)
		if err != nil {
			return Resolution{}, err
		}
		task.WorkspaceKey = workspace
		task.HarnessID = harness
		task.HarnessVersion = version
		task.HarnessSessionID = session
		if value, exists, optionalErr := optionalHeader(request.Header, HeaderTaskID); optionalErr != nil {
			return Resolution{}, optionalErr
		} else if exists {
			if !stableIdentity.MatchString(value) {
				return Resolution{}, fmt.Errorf("%w: %s must be a stable key", ErrResolution, HeaderTaskID)
			}
			task.TaskID = llm.TaskID(value)
		}
		if value, exists, optionalErr := optionalHeader(request.Header, HeaderAllowExec); optionalErr != nil {
			return Resolution{}, optionalErr
		} else if exists {
			allowed, parseErr := strconv.ParseBool(value)
			if parseErr != nil || (value != "true" && value != "false") {
				return Resolution{}, fmt.Errorf("%w: %s must be true or false", ErrResolution, HeaderAllowExec)
			}
			task.ExecAllowed = allowed
		}
	default:
		return Resolution{}, fmt.Errorf("%w: unsupported capability tier", ErrResolution)
	}
	return Resolution{IdempotencyKey: llm.IdempotencyKey(idempotency), Task: task}, nil
}

func taskHeaders() []string {
	return []string{
		HeaderTaskID, HeaderWorkspaceKey, HeaderHarnessID, HeaderHarnessVersion,
		HeaderHarnessSession, HeaderAllowExec,
	}
}

func requiredStableHeader(header http.Header, name string) (string, error) {
	value, err := requiredHeader(header, name)
	if err != nil || !stableIdentity.MatchString(value) {
		return "", fmt.Errorf("%w: %s is required and must be a stable key", ErrResolution, name)
	}
	return value, nil
}

func requiredHeader(header http.Header, name string) (string, error) {
	value, present, err := optionalHeader(header, name)
	if err != nil {
		return "", err
	}
	if !present || value == "" {
		return "", fmt.Errorf("%w: %s is required", ErrResolution, name)
	}
	return value, nil
}

func optionalHeader(header http.Header, name string) (string, bool, error) {
	values := header.Values(name)
	if len(values) == 0 {
		return "", false, nil
	}
	if len(values) != 1 || values[0] == "" || values[0] != strings.TrimSpace(values[0]) ||
		strings.ContainsAny(values[0], "\x00\r\n") {
		return "", false, fmt.Errorf("%w: %s must appear exactly once", ErrResolution, name)
	}
	return values[0], true, nil
}

func sortedUnique(values []string) []string {
	sort.Strings(values)
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}
