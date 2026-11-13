package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
)

const (
	// PATPrefix is the canonical prefix for Tobiten Personal Access Tokens (ADR-0006 §3).
	PATPrefix = "tobiten_pat_"

	patSecretRandomBytes = 32
	maxPATIDBytes        = 128
)

// PATStore defines the persistence lookups and updates required by PATAuthenticator.
// *postgres.UserRepository and storage.UserRepository satisfy this interface.
type PATStore interface {
	GetCredentialByID(ctx context.Context, id string) (*domain.Credential, error)
	GetUserByID(ctx context.Context, id string) (*domain.User, error)
	ListUserRoles(ctx context.Context, userID string) ([]domain.Role, error)
	UpdateCredentialLastUsed(ctx context.Context, id string, lastUsedAt time.Time) error
}

// PATOptions configures caching, failure throttling, and hashing concurrency for PATAuthenticator.
type PATOptions struct {
	Logger *slog.Logger

	// Bounded verification cache (keyed to exact presented secret + current credential/hash identity).
	DisableCache    bool
	CacheMaxEntries int
	CacheTTL        time.Duration

	// Per-IP and per-credential-ID failure throttling (cooldown only; never locks out).
	DisableThrottle bool
	IPMaxFailures   int
	CredMaxFailures int
	FailureWindow   time.Duration
	BaseCooldown    time.Duration
	MaxCooldown     time.Duration
	ThrottleMaxKeys int

	// Aggregate concurrent Argon2id verification bounds.
	Limiter        *HashLimiter
	LimiterOptions HashLimiterOptions

	// Optional test hooks.
	Now        func() time.Time
	VerifyFunc func(secret, encodedHash string) (bool, error)
}

// PATAuthenticator implements Personal Access Token authentication via Authorization: Bearer
// and X-API-Key headers using the tobiten_pat_<id>_<secret> format (ADR-0006 §3, ADR-0018 S1).
type PATAuthenticator struct {
	store        PATStore
	rt           *authRuntime
	ipThrottle   *boundedThrottleMap
	credThrottle *boundedThrottleMap
}

func (o PATOptions) runtimeConfig() authRuntimeConfig {
	limOpts := o.LimiterOptions
	if limOpts.VerifyFunc == nil {
		limOpts.VerifyFunc = o.VerifyFunc
	}
	return authRuntimeConfig{
		Logger:              o.Logger,
		DisableCache:        o.DisableCache,
		CacheMaxEntries:     o.CacheMaxEntries,
		CacheTTL:            o.CacheTTL,
		DisableThrottle:     o.DisableThrottle,
		IPMaxFailures:       o.IPMaxFailures,
		IDMaxFailures:       o.CredMaxFailures,
		FailureWindow:       o.FailureWindow,
		BaseCooldown:        o.BaseCooldown,
		MaxCooldown:         o.MaxCooldown,
		ThrottleMaxKeys:     o.ThrottleMaxKeys,
		IPThrottleTag:       "throttle-pat-ip",
		IDThrottleTag:       "throttle-pat-cred",
		Limiter:             o.Limiter,
		MaxConcurrentHashes: limOpts.MaxConcurrentHashes,
		MaxHashQueue:        limOpts.MaxHashQueue,
		HashWaitTimeout:     limOpts.HashWaitTimeout,
		Now:                 o.Now,
		VerifyFunc:          limOpts.VerifyFunc,
	}
}

// NewPATAuthenticator constructs a PATAuthenticator backed by store.
func NewPATAuthenticator(store PATStore, opts PATOptions) (*PATAuthenticator, error) {
	if store == nil {
		return nil, errors.New("pat authenticator store is required")
	}

	rt, err := newAuthRuntime(opts.runtimeConfig())
	if err != nil {
		return nil, err
	}

	return &PATAuthenticator{
		store:        store,
		rt:           rt,
		ipThrottle:   rt.throttles.ipThrottle,
		credThrottle: rt.throttles.idThrottle,
	}, nil
}

// Name returns the canonical authenticator name.
func (a *PATAuthenticator) Name() string {
	return "pat"
}

// Tier returns CredentialTierExplicit per ADR-0006 §3 request precedence.
func (a *PATAuthenticator) Tier() module.CredentialTier {
	return module.CredentialTierExplicit
}

// Authenticate implements module.Authenticator for Bearer and X-API-Key PAT credentials.
func (a *PATAuthenticator) Authenticate(r *http.Request) (module.AuthResult, error) {
	token, hasPAT, err := parsePATHeaders(r)
	if !hasPAT {
		return module.AuthPass(), nil
	}

	clientIP := extractClientIP(r.RemoteAddr)
	if err != nil {
		now := a.rt.now()
		if a.rt.throttles.isIPThrottled(clientIP, now) {
			return module.AuthResult{Handled: true}, ErrAuthThrottled
		}
		a.rt.throttles.recordIPFailure(clientIP, now)
		return module.AuthResult{Handled: true}, err
	}

	principal, err := a.AuthenticatePAT(r.Context(), token, clientIP)
	if err != nil {
		return module.AuthResult{Handled: true}, err
	}
	return module.AuthSuccess(principal), nil
}

// AuthenticatePAT parses and verifies a PAT of the form tobiten_pat_<id>_<secret>,
// validates live credential and role state, updates last_used_at after verification,
// and resolves a scope-capped domain.Principal.
func (a *PATAuthenticator) AuthenticatePAT(ctx context.Context, token, clientIP string) (*domain.Principal, error) {
	credID, secret, ok := ParsePAT(token)
	if !ok {
		now := a.rt.now()
		if a.rt.throttles.isIPThrottled(clientIP, now) {
			return nil, ErrAuthThrottled
		}
		a.rt.throttles.recordIPFailure(clientIP, now)
		return nil, ErrInvalidCredentials
	}

	now := a.rt.now()
	if a.rt.throttles.isThrottled(clientIP, credID, now) {
		a.logThrottled(ctx, clientIP, credID)
		return nil, ErrAuthThrottled
	}

	cred, err := a.store.GetCredentialByID(ctx, credID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if _, vErr := a.rt.verifyWithBounds(ctx, clientIP, credID, secret, dummyArgon2idHash); vErr != nil {
				return nil, vErr
			}
			a.rt.throttles.recordFailure(clientIP, credID, a.rt.now())
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	if cred.ID != credID || !isUsablePATCredential(cred, now) {
		a.rt.throttles.recordFailure(clientIP, credID, now)
		return nil, ErrInvalidCredentials
	}

	cacheKey := a.computeCacheKey(cred.UserID, cred.ID, *cred.SecretHash, secret)
	if a.rt.cache == nil || !a.rt.cache.get(cacheKey, now) {
		match, vErr := a.rt.verifyWithBounds(ctx, clientIP, cred.ID, secret, *cred.SecretHash)
		if vErr != nil {
			return nil, vErr
		}
		if !match {
			a.rt.throttles.recordFailure(clientIP, cred.ID, a.rt.now())
			return nil, ErrInvalidCredentials
		}
		if a.rt.cache != nil {
			a.rt.cache.put(cacheKey, a.rt.now().Add(a.rt.cacheTTL))
		}
	}

	a.rt.throttles.resetID(cred.ID)

	user, err := a.store.GetUserByID(ctx, cred.UserID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	roles, err := a.store.ListUserRoles(ctx, cred.UserID)
	if err != nil {
		return nil, err
	}

	if err := a.store.UpdateCredentialLastUsed(ctx, cred.ID, a.rt.now().UTC()); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	return domain.NewPrincipal(*user, roles, cred.Kind, cred.ID, cred.Scopes), nil
}

// ClearCache removes all cached PAT verification entries.
func (a *PATAuthenticator) ClearCache() {
	a.rt.clearCache()
}

func isUsablePATCredential(cred *domain.Credential, now time.Time) bool {
	if cred == nil || cred.ID == "" || cred.UserID == "" {
		return false
	}
	if cred.Kind != domain.CredentialKindPAT {
		return false
	}
	if len(cred.Scopes) == 0 {
		return false
	}
	for _, scope := range cred.Scopes {
		if !scope.IsValid() {
			return false
		}
	}
	if cred.IsRevoked() || cred.IsExpired(now) {
		return false
	}
	if cred.SecretHash == nil || *cred.SecretHash == "" {
		return false
	}
	if cred.HashScheme == nil || *cred.HashScheme != HashSchemeArgon2id {
		return false
	}
	return true
}

func (a *PATAuthenticator) logThrottled(ctx context.Context, clientIP, credID string) {
	if a.rt.logger != nil {
		a.rt.logger.WarnContext(ctx, "pat authentication throttled",
			slog.String("client_ip", detachBoundedLogAttr(clientIP)),
			slog.String("credential_id", detachBoundedLogAttr(credID)),
		)
	}
}

func (a *PATAuthenticator) computeCacheKey(userID, credID, secretHash, secret string) [32]byte {
	return hmacLengthPrefixed(a.rt.hmacSecret[:], "pat-cache", userID, credID, secretHash, secret)
}

func parsePATHeaders(r *http.Request) (token string, hasPAT bool, err error) {
	var tokens []string
	malformed := false

	for _, hdr := range r.Header.Values("Authorization") {
		scheme, rest, found := strings.Cut(strings.TrimSpace(hdr), " ")
		if !strings.EqualFold(scheme, "Bearer") {
			continue
		}
		hasPAT = true
		if !found {
			malformed = true
			continue
		}
		val := strings.TrimSpace(rest)
		if val == "" {
			malformed = true
			continue
		}
		tokens = append(tokens, val)
	}

	for _, hdr := range r.Header.Values("X-API-Key") {
		hasPAT = true
		val := strings.TrimSpace(hdr)
		if val == "" {
			malformed = true
			continue
		}
		tokens = append(tokens, val)
	}

	if !hasPAT {
		return "", false, nil
	}
	if malformed || len(tokens) == 0 {
		return "", true, ErrInvalidCredentials
	}

	first := tokens[0]
	for _, tok := range tokens[1:] {
		if subtle.ConstantTimeCompare([]byte(first), []byte(tok)) != 1 {
			return "", true, ErrInvalidCredentials
		}
	}

	return first, true, nil
}

// ParsePAT parses a token in tobiten_pat_<id>_<secret> format.
// The id segment is delimited by the first underscore after PATPrefix, allowing secret
// portions (such as base64url strings) to contain underscores.
func ParsePAT(token string) (credID, secret string, ok bool) {
	if !strings.HasPrefix(token, PATPrefix) {
		return "", "", false
	}
	rest := token[len(PATPrefix):]
	id, sec, found := strings.Cut(rest, "_")
	if !found || id == "" || sec == "" || len(id) > maxPATIDBytes {
		return "", "", false
	}
	if hasControlOrSpace(id) || hasControlOrSpace(sec) {
		return "", "", false
	}
	return id, sec, true
}

// FormatPAT formats a credential ID and secret into the canonical tobiten_pat_<id>_<secret> token.
func FormatPAT(id, secret string) (string, error) {
	if id == "" || len(id) > maxPATIDBytes || strings.ContainsRune(id, '_') || hasControlOrSpace(id) {
		return "", errors.New("invalid pat credential id")
	}
	if secret == "" || hasControlOrSpace(secret) {
		return "", errors.New("invalid pat secret")
	}
	return PATPrefix + id + "_" + secret, nil
}

// GeneratePATSecret generates a cryptographically random URL-safe PAT secret.
func GeneratePATSecret() (string, error) {
	var raw [patSecretRandomBytes]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", fmt.Errorf("generating pat secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

// NewPAT generates a new random secret for credID, formats the full PAT token,
// and returns the token paired with its Argon2id PHC secret hash.
func NewPAT(credID string) (token string, secretHash string, err error) {
	secret, err := GeneratePATSecret()
	if err != nil {
		return "", "", err
	}
	token, err = FormatPAT(credID, secret)
	if err != nil {
		return "", "", err
	}
	secretHash, err = HashPassword(secret)
	if err != nil {
		return "", "", err
	}
	return token, secretHash, nil
}

func hasControlOrSpace(s string) bool {
	for i := range len(s) {
		b := s[i]
		if b <= 0x20 || b == 0x7f {
			return true
		}
	}
	return false
}
