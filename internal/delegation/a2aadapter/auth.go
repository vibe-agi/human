// Package a2aadapter exposes the delegation authority through the official A2A
// Go SDK protocol types and HTTP transports.
package a2aadapter

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

var ErrInvalidBearerToken = errors.New("invalid bearer token")

// BearerAuthenticator resolves a bearer token to the stable delegation caller
// identity used for ownership checks and persistence.
type BearerAuthenticator interface {
	AuthenticateBearer(context.Context, string) (string, error)
}

// BearerAuthenticatorFunc adapts a function to BearerAuthenticator.
type BearerAuthenticatorFunc func(context.Context, string) (string, error)

func (fn BearerAuthenticatorFunc) AuthenticateBearer(ctx context.Context, token string) (string, error) {
	return fn(ctx, token)
}

// StaticBearerTokens is intended for tests and small single-instance setups.
// Production deployments can inject a hash-backed BearerAuthenticator.
// The map must not be mutated while requests are being served.
type StaticBearerTokens map[string]string

func (tokens StaticBearerTokens) AuthenticateBearer(_ context.Context, token string) (string, error) {
	for candidate, callerID := range tokens {
		if subtle.ConstantTimeCompare([]byte(candidate), []byte(token)) == 1 &&
			strings.TrimSpace(callerID) != "" {
			return callerID, nil
		}
	}
	return "", ErrInvalidBearerToken
}

type callerContextKey struct{}

func callerFromContext(ctx context.Context) (string, bool) {
	callerID, ok := ctx.Value(callerContextKey{}).(string)
	return callerID, ok && callerID != ""
}

func bearerMiddleware(authenticator BearerAuthenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		scheme, token, ok := strings.Cut(strings.TrimSpace(request.Header.Get("Authorization")), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
			writeUnauthorized(writer)
			return
		}
		callerID, err := authenticator.AuthenticateBearer(request.Context(), strings.TrimSpace(token))
		if err != nil || strings.TrimSpace(callerID) == "" {
			writeUnauthorized(writer)
			return
		}
		ctx := context.WithValue(request.Context(), callerContextKey{}, strings.TrimSpace(callerID))
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

func writeUnauthorized(writer http.ResponseWriter) {
	writer.Header().Set("WWW-Authenticate", `Bearer realm="human-a2a"`)
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusUnauthorized)
	_, _ = writer.Write([]byte(`{"error":"unauthenticated"}`))
}
