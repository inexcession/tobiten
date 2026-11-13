package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SessionRepository implements storage.SessionRepository using PostgreSQL and sqlc.
type SessionRepository struct {
	pool *pgxpool.Pool
	q    *Queries
}

// NewSessionRepository creates a new SessionRepository backed by a pgxpool.Pool.
func NewSessionRepository(pool *pgxpool.Pool) *SessionRepository {
	return &SessionRepository{
		pool: pool,
		q:    New(pool),
	}
}

var _ storage.SessionRepository = (*SessionRepository)(nil)

// CreateSession inserts a new session record after validating domain invariants.
func (r *SessionRepository) CreateSession(ctx context.Context, session *domain.Session) error {
	if err := session.Validate(); err != nil {
		return err
	}
	createdAt := session.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	row, err := r.q.CreateSession(ctx, CreateSessionParams{
		ID:                   session.ID,
		UserID:               session.UserID,
		CredentialID:         session.CredentialID,
		Kind:                 string(session.Kind),
		TokenHash:            session.TokenHash,
		CsrfTokenHash:        session.CSRFTokenHash,
		CredentialHashDigest: session.CredentialHashDigest,
		CreatedAt:            createdAt,
		LastUsedAt:           session.LastUsedAt,
		ExpiresAt:            session.ExpiresAt,
		RevokedAt:            session.RevokedAt,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "23505":
				return storage.ErrAlreadyExists
			case "23503":
				return storage.ErrNotFound
			}
		}
		return fmt.Errorf("creating session %q: %w", session.ID, err)
	}
	*session = sessionFromRow(row)
	return nil
}

// GetSessionByTokenHash fetches a session by its hashed opaque bearer/cookie token.
func (r *SessionRepository) GetSessionByTokenHash(ctx context.Context, tokenHash string) (*domain.Session, error) {
	row, err := r.q.GetSessionByTokenHash(ctx, tokenHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("getting session by token hash: %w", err)
	}
	s := sessionFromRow(row)
	return &s, nil
}

// UpdateSessionLastUsed records last activity timestamp on the session row.
func (r *SessionRepository) UpdateSessionLastUsed(ctx context.Context, id string, lastUsedAt time.Time) error {
	ts := lastUsedAt.UTC()
	rows, err := r.q.UpdateSessionLastUsed(ctx, UpdateSessionLastUsedParams{
		ID:         id,
		LastUsedAt: &ts,
	})
	if err != nil {
		return fmt.Errorf("updating session last_used_at %q: %w", id, err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// RevokeSession marks a session revoked if not already revoked.
func (r *SessionRepository) RevokeSession(ctx context.Context, id string, revokedAt time.Time) error {
	ts := revokedAt.UTC()
	rows, err := r.q.RevokeSession(ctx, RevokeSessionParams{
		ID:        id,
		RevokedAt: &ts,
	})
	if err != nil {
		return fmt.Errorf("revoking session %q: %w", id, err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// RevokeSessionsByCredentialID revokes all unrevoked sessions derived from credentialID.
func (r *SessionRepository) RevokeSessionsByCredentialID(ctx context.Context, credentialID string, revokedAt time.Time) error {
	ts := revokedAt.UTC()
	if _, err := r.q.RevokeSessionsByCredentialID(ctx, RevokeSessionsByCredentialIDParams{
		CredentialID: credentialID,
		RevokedAt:    &ts,
	}); err != nil {
		return fmt.Errorf("revoking sessions for credential %q: %w", credentialID, err)
	}
	return nil
}

// DeleteSession hard-deletes a session row by ID.
func (r *SessionRepository) DeleteSession(ctx context.Context, id string) error {
	rows, err := r.q.DeleteSession(ctx, id)
	if err != nil {
		return fmt.Errorf("deleting session %q: %w", id, err)
	}
	if rows == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// DeleteSessionsByCredentialID removes all sessions derived from credentialID.
func (r *SessionRepository) DeleteSessionsByCredentialID(ctx context.Context, credentialID string) error {
	if _, err := r.q.DeleteSessionsByCredentialID(ctx, credentialID); err != nil {
		return fmt.Errorf("deleting sessions for credential %q: %w", credentialID, err)
	}
	return nil
}

func sessionFromRow(row Session) domain.Session {
	return domain.Session{
		ID:                   row.ID,
		UserID:               row.UserID,
		CredentialID:         row.CredentialID,
		Kind:                 domain.SessionKind(row.Kind),
		TokenHash:            row.TokenHash,
		CSRFTokenHash:        row.CsrfTokenHash,
		CredentialHashDigest: row.CredentialHashDigest,
		CreatedAt:            row.CreatedAt,
		LastUsedAt:           row.LastUsedAt,
		ExpiresAt:            row.ExpiresAt,
		RevokedAt:            row.RevokedAt,
	}
}
