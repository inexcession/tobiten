package storage

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// SessionRepository persists and queries credential-bound server-side sessions.
type SessionRepository interface {
	CreateSession(ctx context.Context, session *domain.Session) error
	GetSessionByTokenHash(ctx context.Context, tokenHash string) (*domain.Session, error)
	UpdateSessionLastUsed(ctx context.Context, id string, lastUsedAt time.Time) error
	RevokeSession(ctx context.Context, id string, revokedAt time.Time) error
	RevokeSessionsByCredentialID(ctx context.Context, credentialID string, revokedAt time.Time) error
	DeleteSession(ctx context.Context, id string) error
	DeleteSessionsByCredentialID(ctx context.Context, credentialID string) error
}
