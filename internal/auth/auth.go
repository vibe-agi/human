// Package auth issues and verifies caller API tokens without storing secrets.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
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
	Authenticate(context.Context, string) (Principal, error)
}

type IssuedToken struct {
	KeyID  string
	Secret string
}

type Service struct {
	store storeapi.TokenStore
	now   func() time.Time
}

func NewService(store storeapi.TokenStore) *Service {
	return &Service{store: store, now: time.Now}
}

func (service *Service) Issue(ctx context.Context, principalType PrincipalType, subjectID string) (IssuedToken, error) {
	if principalType != PrincipalCaller && principalType != PrincipalWorker {
		return IssuedToken{}, errors.New("principal type must be caller or worker")
	}
	if strings.TrimSpace(subjectID) == "" {
		return IssuedToken{}, errors.New("subject id is required")
	}
	keyID, err := randomString("key_", 12)
	if err != nil {
		return IssuedToken{}, err
	}
	secret, err := randomString("hae_", 32)
	if err != nil {
		return IssuedToken{}, err
	}
	digest := sha256.Sum256([]byte(secret))
	if err := service.store.CreateAPIToken(ctx, storeapi.APIToken{
		KeyID: keyID, PrincipalType: string(principalType), SubjectID: subjectID,
		TokenHash: digest[:], CreatedAt: service.now().UTC(),
	}); err != nil {
		return IssuedToken{}, err
	}
	return IssuedToken{KeyID: keyID, Secret: secret}, nil
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
