package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// SessionKind identifies which protocol or browser boundary owns a session row.
type SessionKind string

const (
	// SessionKindNative identifies a first-party tobiten native SPA/API session.
	SessionKindNative SessionKind = "NATIVE"
	// SessionKindKomga identifies a Komga-compatible adapter session.
	SessionKindKomga SessionKind = "KOMGA"
	// SessionKindKomgaRememberMe identifies a Komga-compatible remember-me session.
	SessionKindKomgaRememberMe SessionKind = "KOMGA_REMEMBER_ME"
)

// IsValid reports whether k is a supported SessionKind.
func (k SessionKind) IsValid() bool {
	switch k {
	case SessionKindNative, SessionKindKomga, SessionKindKomgaRememberMe:
		return true
	default:
		return false
	}
}

// Session represents an authenticated server-side session bound to an originating credential.
type Session struct {
	ID                   string
	UserID               string
	CredentialID         string
	Kind                 SessionKind
	TokenHash            string
	CSRFTokenHash        string
	CredentialHashDigest string
	CreatedAt            time.Time
	LastUsedAt           *time.Time
	ExpiresAt            time.Time
	RevokedAt            *time.Time
}

// IsRevoked reports whether the session has been explicitly revoked.
func (s Session) IsRevoked() bool {
	return s.RevokedAt != nil
}

// IsExpired reports whether the session has reached or passed its expiration time.
func (s Session) IsExpired(now time.Time) bool {
	return !now.Before(s.ExpiresAt)
}

// Validate verifies required fields on s before persistence.
func (s *Session) Validate() error {
	if s == nil {
		return errors.New("session cannot be nil")
	}
	if strings.TrimSpace(s.ID) == "" {
		return errors.New("session id cannot be empty")
	}
	if strings.TrimSpace(s.UserID) == "" {
		return errors.New("session user id cannot be empty")
	}
	if strings.TrimSpace(s.CredentialID) == "" {
		return errors.New("session credential id cannot be empty")
	}
	if !s.Kind.IsValid() {
		return fmt.Errorf("invalid session kind %q", s.Kind)
	}
	if strings.TrimSpace(s.TokenHash) == "" {
		return errors.New("session token hash cannot be empty")
	}
	if strings.TrimSpace(s.CSRFTokenHash) == "" {
		return errors.New("session csrf token hash cannot be empty")
	}
	if s.ExpiresAt.IsZero() {
		return errors.New("session expires_at cannot be zero")
	}
	return nil
}
