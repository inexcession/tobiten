package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
)

const (
	// NativeSessionCookieName is the HTTP cookie carrying the native tobiten session token.
	NativeSessionCookieName = app.NativeSessionCookieName
	// DefaultSessionTTL is the default validity window for native login sessions.
	DefaultSessionTTL = 7 * 24 * time.Hour
	// DefaultSessionLastUsedInterval throttles last_used_at persistence writes on session reads.
	DefaultSessionLastUsedInterval = 5 * time.Minute

	sessionTokenByteLen = 32
	csrfTokenByteLen    = 32
)

// SessionCredentialStore defines the minimal read methods required to verify credential
// and live role state for session creation and validation.
type SessionCredentialStore interface {
	GetUserByID(ctx context.Context, id string) (*domain.User, error)
	GetCredentialByID(ctx context.Context, id string) (*domain.Credential, error)
	ListUserRoles(ctx context.Context, userID string) ([]domain.Role, error)
}

// SessionOptions configures native session lifetime and clock behavior.
type SessionOptions struct {
	TTL              time.Duration
	LastUsedInterval time.Duration
	Now              func() time.Time
}

// HashOpaqueToken computes the lowercase SHA-256 hex digest of an opaque token or secret hash.
func HashOpaqueToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// SessionService authenticates password logins via BasicAuthenticator and mints
// credential-bound native sessions with CSRF tokens.
type SessionService struct {
	basic    *BasicAuthenticator
	creds    SessionCredentialStore
	sessions storage.SessionRepository
	ttl      time.Duration
	now      func() time.Time
}

var _ app.SessionService = (*SessionService)(nil)

// NewSessionService constructs a SessionService reusing the provided BasicAuthenticator,
// credential reader, and session repository.
func NewSessionService(
	basic *BasicAuthenticator,
	creds SessionCredentialStore,
	sessions storage.SessionRepository,
	opts SessionOptions,
) (*SessionService, error) {
	if basic == nil {
		return nil, errors.New("basic authenticator is required")
	}
	if creds == nil {
		return nil, errors.New("credential store is required")
	}
	if sessions == nil {
		return nil, errors.New("session repository is required")
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = DefaultSessionTTL
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &SessionService{
		basic:    basic,
		creds:    creds,
		sessions: sessions,
		ttl:      ttl,
		now:      nowFn,
	}, nil
}

// Login verifies username/password credentials (using stable peer-IP throttling and bounded Argon2id verification)
// and creates a credential-bound native session returning opaque session and CSRF tokens.
func (s *SessionService) Login(ctx context.Context, req app.LoginRequest) (*app.LoginResult, error) {
	if s == nil {
		return nil, errors.New("session service is nil")
	}

	clientIP := extractClientIP(req.RemoteAddr)
	principal, err := s.basic.AuthenticateBasic(ctx, req.Username, req.Password, clientIP)
	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidCredentials):
			return nil, fmt.Errorf("%w: %w", app.ErrUnauthorized, err)
		case errors.Is(err, ErrAuthThrottled):
			return nil, fmt.Errorf("%w: %w", app.ErrTooManyRequests, err)
		case errors.Is(err, ErrAuthBusy):
			return nil, fmt.Errorf("%w: %w", app.ErrServiceBusy, err)
		default:
			return nil, err
		}
	}
	if principal == nil || principal.CredentialID == "" {
		return nil, fmt.Errorf("%w: %w", app.ErrUnauthorized, ErrInvalidCredentials)
	}

	now := s.now().UTC()
	cred, err := s.creds.GetCredentialByID(ctx, principal.CredentialID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrInvalidCredentials
		}
		return nil, fmt.Errorf("loading originating credential: %w", err)
	}
	if cred.UserID != principal.User.ID || cred.IsRevoked() || cred.IsExpired(now) || !cred.Kind.IsValid() {
		return nil, ErrInvalidCredentials
	}

	var credHashDigest string
	if cred.SecretHash != nil {
		credHashDigest = HashOpaqueToken(*cred.SecretHash)
	}

	sessionID, err := domain.NewID()
	if err != nil {
		return nil, fmt.Errorf("generating session id: %w", err)
	}
	sessionToken, err := generateOpaqueToken(sessionTokenByteLen)
	if err != nil {
		return nil, fmt.Errorf("generating session token: %w", err)
	}
	csrfToken, err := generateOpaqueToken(csrfTokenByteLen)
	if err != nil {
		return nil, fmt.Errorf("generating csrf token: %w", err)
	}

	expiresAt := now.Add(s.ttl)
	lastUsed := now
	sess := &domain.Session{
		ID:                   sessionID,
		UserID:               principal.User.ID,
		CredentialID:         cred.ID,
		Kind:                 domain.SessionKindNative,
		TokenHash:            HashOpaqueToken(sessionToken),
		CSRFTokenHash:        HashOpaqueToken(csrfToken),
		CredentialHashDigest: credHashDigest,
		CreatedAt:            now,
		LastUsedAt:           &lastUsed,
		ExpiresAt:            expiresAt,
	}
	if err := s.sessions.CreateSession(ctx, sess); err != nil {
		return nil, fmt.Errorf("persisting native session: %w", err)
	}

	return &app.LoginResult{
		Principal:    principal,
		SessionToken: sessionToken,
		CSRFToken:    csrfToken,
		ExpiresAt:    expiresAt,
	}, nil
}

// SessionAuthenticator authenticates native session cookies in CredentialTierCookie.
// Komga adapter cookies (KOMGA-SESSION, komga-remember-me) are explicitly never accepted here.
type SessionAuthenticator struct {
	creds            SessionCredentialStore
	sessions         storage.SessionRepository
	lastUsedInterval time.Duration
	now              func() time.Time
}

var (
	_ app.SessionAuthenticator   = (*SessionAuthenticator)(nil)
	_ module.TieredAuthenticator = (*SessionAuthenticator)(nil)
)

// NewSessionAuthenticator constructs a SessionAuthenticator backed by persistent credentials and sessions.
func NewSessionAuthenticator(
	creds SessionCredentialStore,
	sessions storage.SessionRepository,
	opts SessionOptions,
) (*SessionAuthenticator, error) {
	if creds == nil {
		return nil, errors.New("credential store is required")
	}
	if sessions == nil {
		return nil, errors.New("session repository is required")
	}
	lastUsedInterval := opts.LastUsedInterval
	if lastUsedInterval == 0 {
		lastUsedInterval = DefaultSessionLastUsedInterval
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &SessionAuthenticator{
		creds:            creds,
		sessions:         sessions,
		lastUsedInterval: lastUsedInterval,
		now:              nowFn,
	}, nil
}

// Name returns the stable identifier for this authenticator.
func (a *SessionAuthenticator) Name() string {
	return "native-session"
}

// Tier declares cookie credential precedence (ADR-0006 §3).
func (a *SessionAuthenticator) Tier() module.CredentialTier {
	return module.CredentialTierCookie
}

// Authenticate inspects the request for a single native session cookie and verifies
// session + originating credential + live role state.
func (a *SessionAuthenticator) Authenticate(r *http.Request) (module.AuthResult, error) {
	count := countCookiesNamed(r, NativeSessionCookieName)
	if count == 0 {
		return module.AuthPass(), nil
	}
	if count > 1 {
		return module.AuthResult{}, ErrInvalidCredentials
	}
	cookie, err := r.Cookie(NativeSessionCookieName)
	if err != nil || cookie == nil || cookie.Value == "" || hasControlOrSpace(cookie.Value) {
		return module.AuthResult{}, ErrInvalidCredentials
	}

	principal, csrfHash, err := a.AuthenticateSession(r.Context(), cookie.Value)
	if err != nil {
		return module.AuthResult{}, err
	}
	return module.AuthSuccessCookie(principal, csrfHash), nil
}

// AuthenticateSession validates an opaque native session token and resolves the live Principal
// and the session's bound CSRF token SHA-256 hex digest.
func (a *SessionAuthenticator) AuthenticateSession(ctx context.Context, token string) (*domain.Principal, string, error) {
	if a == nil || token == "" || hasControlOrSpace(token) {
		return nil, "", ErrInvalidCredentials
	}

	now := a.now().UTC()
	session, err := a.sessions.GetSessionByTokenHash(ctx, HashOpaqueToken(token))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, "", ErrInvalidCredentials
		}
		return nil, "", fmt.Errorf("looking up session: %w", err)
	}
	if session.Kind != domain.SessionKindNative || session.IsRevoked() || session.IsExpired(now) {
		return nil, "", ErrInvalidCredentials
	}

	cred, err := a.creds.GetCredentialByID(ctx, session.CredentialID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, "", ErrInvalidCredentials
		}
		return nil, "", fmt.Errorf("looking up session credential: %w", err)
	}
	if cred.UserID != session.UserID || cred.IsRevoked() || cred.IsExpired(now) || !matchesCredentialDigest(cred, session.CredentialHashDigest) {
		return nil, "", ErrInvalidCredentials
	}

	user, err := a.creds.GetUserByID(ctx, cred.UserID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, "", ErrInvalidCredentials
		}
		return nil, "", fmt.Errorf("looking up session user: %w", err)
	}

	roles, err := a.creds.ListUserRoles(ctx, user.ID)
	if err != nil {
		return nil, "", fmt.Errorf("listing user roles: %w", err)
	}

	if a.lastUsedInterval < 0 || session.LastUsedAt == nil || now.Sub(*session.LastUsedAt) >= a.lastUsedInterval {
		if err := a.sessions.UpdateSessionLastUsed(ctx, session.ID, now); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, "", ErrInvalidCredentials
			}
			return nil, "", fmt.Errorf("updating session last_used_at: %w", err)
		}
	}

	principal := domain.NewPrincipal(*user, roles, cred.Kind, cred.ID, cred.Scopes)
	return principal, session.CSRFTokenHash, nil
}

func matchesCredentialDigest(cred *domain.Credential, expectedDigest string) bool {
	if cred == nil {
		return false
	}
	switch cred.Kind {
	case domain.CredentialKindPassword, domain.CredentialKindPAT:
		if cred.SecretHash == nil || expectedDigest == "" {
			return false
		}
		actual := HashOpaqueToken(*cred.SecretHash)
		return subtle.ConstantTimeCompare([]byte(actual), []byte(expectedDigest)) == 1
	case domain.CredentialKindOIDCLink:
		if expectedDigest == "" {
			return cred.SecretHash == nil
		}
		if cred.SecretHash == nil {
			return false
		}
		actual := HashOpaqueToken(*cred.SecretHash)
		return subtle.ConstantTimeCompare([]byte(actual), []byte(expectedDigest)) == 1
	default:
		return false
	}
}

func countCookiesNamed(r *http.Request, name string) int {
	if r == nil {
		return 0
	}
	count := 0
	for _, c := range r.Cookies() {
		if c.Name == name {
			count++
		}
	}
	return count
}

func generateOpaqueToken(numBytes int) (string, error) {
	buf := make([]byte, numBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
