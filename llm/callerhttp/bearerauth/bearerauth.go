// Package bearerauth is HumanLLM's basic caller Authenticator: it accepts a
// single shared token using the OpenAI Authorization bearer convention or the
// Anthropic X-Api-Key convention, and attributes every accepted request to one
// fixed CallerID. It is the loopback default for a single-tenant deployment.
//
// It is a basic default, not the product: the product is the
// callerhttp.Authenticator port, which a multi-tenant host implements against
// their own JWT, mTLS, or upstream-context identity system.
package bearerauth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/callerhttp"
)

// Authenticator validates a model-API token against one configured secret in
// constant time and returns a fixed caller identity.
type Authenticator struct {
	tokenSum [sha256.Size]byte
	callerID llm.CallerID
}

var _ callerhttp.Authenticator = (*Authenticator)(nil)

// New binds the shared token and the caller identity it authenticates.
func New(token string, callerID llm.CallerID) (*Authenticator, error) {
	if token == "" {
		return nil, errors.New("bearerauth: token is required")
	}
	if callerID == "" {
		return nil, errors.New("bearerauth: callerID is required")
	}
	return &Authenticator{tokenSum: sha256.Sum256([]byte(token)), callerID: callerID}, nil
}

// AuthenticateCaller accepts either the exact Authorization bearer token or the
// exact X-Api-Key. Supplying both is allowed only when their values agree;
// ambiguous duplicate or conflicting credentials fail closed. Comparisons are
// over fixed-size digests so neither token value nor length leaks through timing.
func (authenticator *Authenticator) AuthenticateCaller(
	_ context.Context,
	request *http.Request,
) (callerhttp.Identity, error) {
	presented, ok := presentedToken(request.Header)
	if !ok {
		return callerhttp.Identity{}, framework.NewFault(
			framework.CodeUnauthenticated, framework.RetryNever, "a model API token is required", nil,
		)
	}
	presentedSum := sha256.Sum256([]byte(presented))
	if subtle.ConstantTimeCompare(presentedSum[:], authenticator.tokenSum[:]) != 1 {
		return callerhttp.Identity{}, framework.NewFault(
			framework.CodeUnauthenticated, framework.RetryNever, "invalid bearer token", nil,
		)
	}
	return callerhttp.Identity{CallerID: authenticator.callerID}, nil
}

func presentedToken(header http.Header) (string, bool) {
	var bearer, apiKey string
	if values := header.Values("Authorization"); len(values) > 1 {
		return "", false
	} else if len(values) == 1 {
		var ok bool
		bearer, ok = bearerToken(values[0])
		if !ok {
			return "", false
		}
	}
	if values := header.Values("X-Api-Key"); len(values) > 1 {
		return "", false
	} else if len(values) == 1 {
		apiKey = strings.TrimSpace(values[0])
		if apiKey == "" {
			return "", false
		}
	}
	switch {
	case bearer == "" && apiKey == "":
		return "", false
	case bearer != "" && apiKey != "":
		bearerSum := sha256.Sum256([]byte(bearer))
		apiKeySum := sha256.Sum256([]byte(apiKey))
		if subtle.ConstantTimeCompare(bearerSum[:], apiKeySum[:]) != 1 {
			return "", false
		}
		return bearer, true
	case bearer != "":
		return bearer, true
	default:
		return apiKey, true
	}
}

// bearerToken extracts the token from an "Authorization: Bearer <token>" header.
func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return "", false
	}
	return token, true
}
