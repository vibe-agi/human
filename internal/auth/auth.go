// Package auth issues and verifies caller API tokens without storing secrets.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	storeapi "github.com/vibe-agi/human/internal/store"
)

var ErrUnauthorized = errors.New("invalid or revoked API token")

type Principal struct {
	Type      PrincipalType
	SubjectID string
	KeyID     string
}

type PrincipalType string

const (
	PrincipalCaller PrincipalType = "caller"
	PrincipalWorker PrincipalType = "worker"
)

type Authenticator interface {
	AuthenticateRequest(*http.Request) (Principal, error)
}

// AuthenticateRequest accepts the current model-provider authentication
// headers. Bearer is used by OpenAI-compatible clients; Anthropic-compatible
// clients use X-Api-Key. Any other Authorization scheme is rejected rather
// than being ambiguously combined with X-Api-Key.
func (service *Service) AuthenticateRequest(request *http.Request) (Principal, error) {
	if service == nil || request == nil {
		return Principal{}, ErrUnauthorized
	}

	secret := strings.TrimSpace(request.Header.Get("X-Api-Key"))
	authorization := request.Header.Get("Authorization")
	if strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
		secret = strings.TrimSpace(authorization[len("Bearer "):])
	} else if strings.TrimSpace(authorization) != "" {
		return Principal{}, ErrUnauthorized
	}
	if secret == "" {
		return Principal{}, ErrUnauthorized
	}
	return service.Authenticate(request.Context(), secret)
}

type IssuedToken struct {
	KeyID  string
	Secret string
}

type PreparedToken struct {
	KeyID         string
	Secret        string
	PrincipalType PrincipalType
	SubjectID     string
}

type Service struct {
	store storeapi.TokenStore
	now   func() time.Time
}

func NewService(store storeapi.TokenStore) *Service {
	return &Service{store: store, now: time.Now}
}

// Prepare creates a credential without making it authenticatable. Callers can
// durably persist the returned secret before Activate commits its hash. This
// split is what makes external credential rotation recoverable across a crash.
func Prepare(principalType PrincipalType, subjectID string) (PreparedToken, error) {
	if principalType != PrincipalCaller && principalType != PrincipalWorker {
		return PreparedToken{}, errors.New("principal type must be caller or worker")
	}
	if strings.TrimSpace(subjectID) == "" {
		return PreparedToken{}, errors.New("subject id is required")
	}
	keyID, err := randomString("key_", 12)
	if err != nil {
		return PreparedToken{}, err
	}
	secret, err := randomString("hae_", 32)
	if err != nil {
		return PreparedToken{}, err
	}
	return PreparedToken{
		KeyID: keyID, Secret: secret, PrincipalType: principalType, SubjectID: subjectID,
	}, nil
}

// Activate commits a prepared credential. It is idempotent for the exact same
// key, secret, type, and subject so a journal can safely replay after a crash.
func (service *Service) Activate(ctx context.Context, prepared PreparedToken) error {
	if prepared.PrincipalType != PrincipalCaller && prepared.PrincipalType != PrincipalWorker {
		return errors.New("principal type must be caller or worker")
	}
	if strings.TrimSpace(prepared.SubjectID) == "" {
		return errors.New("subject id is required")
	}
	if err := validatePreparedValue(prepared.KeyID, "key_", 12); err != nil {
		return fmt.Errorf("prepared key ID: %w", err)
	}
	if err := validatePreparedValue(prepared.Secret, "hae_", 32); err != nil {
		return fmt.Errorf("prepared token secret: %w", err)
	}
	digest := sha256.Sum256([]byte(prepared.Secret))
	if err := service.store.CreateAPIToken(ctx, storeapi.APIToken{
		KeyID: prepared.KeyID, PrincipalType: string(prepared.PrincipalType), SubjectID: prepared.SubjectID,
		TokenHash: digest[:], CreatedAt: service.now().UTC(),
	}); err != nil {
		// The insert may have committed before the caller observed an error, or
		// this may be an ordinary journal replay. Accept only an exact match;
		// key/hash collisions and conflicting principals remain hard errors.
		principal, authenticateErr := service.Authenticate(ctx, prepared.Secret)
		if authenticateErr == nil && principal.KeyID == prepared.KeyID &&
			principal.Type == prepared.PrincipalType && principal.SubjectID == prepared.SubjectID {
			return nil
		}
		return err
	}
	return nil
}

func validatePreparedValue(value, prefix string, bytesCount int) error {
	if !strings.HasPrefix(value, prefix) {
		return fmt.Errorf("must start with %q", prefix)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, prefix))
	if err != nil || len(raw) != bytesCount {
		return fmt.Errorf("must contain %d bytes of URL-safe randomness", bytesCount)
	}
	return nil
}

func (service *Service) Issue(ctx context.Context, principalType PrincipalType, subjectID string) (IssuedToken, error) {
	prepared, err := Prepare(principalType, subjectID)
	if err != nil {
		return IssuedToken{}, err
	}
	if err := service.Activate(ctx, prepared); err != nil {
		return IssuedToken{}, err
	}
	return IssuedToken{KeyID: prepared.KeyID, Secret: prepared.Secret}, nil
}

func (service *Service) Authenticate(ctx context.Context, secret string) (Principal, error) {
	if !strings.HasPrefix(secret, "hae_") {
		return Principal{}, ErrUnauthorized
	}
	digest := sha256.Sum256([]byte(secret))
	token, err := service.store.FindAPITokenByHash(ctx, digest[:])
	if err != nil {
		if errors.Is(err, storeapi.ErrNotFound) {
			return Principal{}, ErrUnauthorized
		}
		return Principal{}, err
	}
	if token.RevokedAt != nil {
		return Principal{}, ErrUnauthorized
	}
	return Principal{Type: PrincipalType(token.PrincipalType), SubjectID: token.SubjectID, KeyID: token.KeyID}, nil
}

func (service *Service) Revoke(ctx context.Context, keyID string) error {
	return service.store.RevokeAPIToken(ctx, keyID)
}

func randomString(prefix string, bytesCount int) (string, error) {
	raw := make([]byte, bytesCount)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("read token randomness: %w", err)
	}
	return prefix + base64.RawURLEncoding.EncodeToString(raw), nil
}
