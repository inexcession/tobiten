package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
)

// BasicStore defines the persistence lookups required by BasicAuthenticator.
// *postgres.UserRepository and storage.UserRepository satisfy this interface.
type BasicStore interface {
	GetUserByUsername(ctx context.Context, username string) (*domain.User, error)
	GetActivePasswordCredentialByUserID(ctx context.Context, userID string) (*domain.Credential, error)
	ListUserRoles(ctx context.Context, userID string) ([]domain.Role, error)
}

// BasicOptions configures caching, failure throttling, and hashing concurrency for BasicAuthenticator.
type BasicOptions struct {
	Logger *slog.Logger

	// Bounded verification cache (keyed to exact presented credentials + current credential/hash identity).
	DisableCache    bool
	CacheMaxEntries int
	CacheTTL        time.Duration

	// Per-IP and per-user failure throttling (cooldown only; never locks out accounts).
	DisableThrottle bool
	IPMaxFailures   int
	UserMaxFailures int
	FailureWindow   time.Duration
	BaseCooldown    time.Duration
	MaxCooldown     time.Duration
	ThrottleMaxKeys int

	// Aggregate concurrent Argon2id verification bounds.
	Limiter             *HashLimiter
	MaxConcurrentHashes int
	MaxHashQueue        int
	HashWaitTimeout     time.Duration

	// Optional test hooks.
	Now        func() time.Time
	VerifyFunc func(password, encodedHash string) (bool, error)
}

// BasicAuthenticator implements HTTP Basic authentication against PASSWORD credentials
// with per-IP and per-user failure throttling, bounded concurrent Argon2id hashing,
// and a bounded verification cache bound to exact credential and hash identity (ADR-0006 §3).
type BasicAuthenticator struct {
	store        BasicStore
	rt           *authRuntime
	ipThrottle   *boundedThrottleMap
	userThrottle *boundedThrottleMap
}

func (o BasicOptions) runtimeConfig() authRuntimeConfig {
	return authRuntimeConfig{
		Logger:              o.Logger,
		DisableCache:        o.DisableCache,
		CacheMaxEntries:     o.CacheMaxEntries,
		CacheTTL:            o.CacheTTL,
		DisableThrottle:     o.DisableThrottle,
		IPMaxFailures:       o.IPMaxFailures,
		IDMaxFailures:       o.UserMaxFailures,
		FailureWindow:       o.FailureWindow,
		BaseCooldown:        o.BaseCooldown,
		MaxCooldown:         o.MaxCooldown,
		ThrottleMaxKeys:     o.ThrottleMaxKeys,
		IPThrottleTag:       "throttle-ip",
		IDThrottleTag:       "throttle-user",
		Limiter:             o.Limiter,
		MaxConcurrentHashes: o.MaxConcurrentHashes,
		MaxHashQueue:        o.MaxHashQueue,
		HashWaitTimeout:     o.HashWaitTimeout,
		Now:                 o.Now,
		VerifyFunc:          o.VerifyFunc,
	}
}

// NewBasicAuthenticator constructs a BasicAuthenticator backed by store.
func NewBasicAuthenticator(store BasicStore, opts BasicOptions) (*BasicAuthenticator, error) {
	if store == nil {
		return nil, errors.New("basic authenticator store is required")
	}

	rt, err := newAuthRuntime(opts.runtimeConfig())
	if err != nil {
		return nil, err
	}

	return &BasicAuthenticator{
		store:        store,
		rt:           rt,
		ipThrottle:   rt.throttles.ipThrottle,
		userThrottle: rt.throttles.idThrottle,
	}, nil
}

// Name returns the canonical authenticator name.
func (a *BasicAuthenticator) Name() string {
	return "basic"
}

// Authenticate implements module.Authenticator for HTTP Basic credentials.
func (a *BasicAuthenticator) Authenticate(r *http.Request) (module.AuthResult, error) {
	username, password, hasBasic, err := parseBasicAuthHeader(r)
	if !hasBasic {
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

	principal, err := a.AuthenticateBasic(r.Context(), username, password, clientIP)
	if err != nil {
		return module.AuthResult{Handled: true}, err
	}
	return module.AuthSuccess(principal), nil
}

// AuthenticateBasic verifies username/password credentials and resolves a Principal with live roles.
func (a *BasicAuthenticator) AuthenticateBasic(ctx context.Context, username, password, clientIP string) (*domain.Principal, error) {
	if username == "" || password == "" || strings.ContainsRune(username, 0) || strings.ContainsRune(password, 0) {
		a.rt.throttles.recordFailure(clientIP, "", a.rt.now())
		return nil, ErrInvalidCredentials
	}

	now := a.rt.now()
	if a.rt.throttles.isThrottled(clientIP, username, now) {
		a.logThrottled(ctx, clientIP, username)
		return nil, ErrAuthThrottled
	}

	user, err := a.store.GetUserByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if _, vErr := a.rt.verifyWithBounds(ctx, clientIP, username, password, dummyArgon2idHash); vErr != nil {
				return nil, vErr
			}
			a.rt.throttles.recordFailure(clientIP, username, a.rt.now())
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	cred, err := a.store.GetActivePasswordCredentialByUserID(ctx, user.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if _, vErr := a.rt.verifyWithBounds(ctx, clientIP, username, password, dummyArgon2idHash); vErr != nil {
				return nil, vErr
			}
			a.rt.throttles.recordFailure(clientIP, username, a.rt.now())
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	if !isUsablePasswordCredential(cred, now) {
		a.rt.throttles.recordFailure(clientIP, username, now)
		return nil, ErrInvalidCredentials
	}

	cacheKey := a.computeCacheKey(user.ID, username, cred.ID, *cred.SecretHash, password)
	if a.rt.cache == nil || !a.rt.cache.get(cacheKey, now) {
		match, vErr := a.rt.verifyWithBounds(ctx, clientIP, username, password, *cred.SecretHash)
		if vErr != nil {
			return nil, vErr
		}
		if !match {
			a.rt.throttles.recordFailure(clientIP, username, a.rt.now())
			return nil, ErrInvalidCredentials
		}
		if a.rt.cache != nil {
			a.rt.cache.put(cacheKey, a.rt.now().Add(a.rt.cacheTTL))
		}
	}

	a.rt.throttles.resetID(username)

	roles, err := a.store.ListUserRoles(ctx, user.ID)
	if err != nil {
		return nil, err
	}

	return domain.NewPrincipal(*user, roles, cred.Kind, cred.ID, cred.Scopes), nil
}

// ClearCache removes all cached password verification entries.
func (a *BasicAuthenticator) ClearCache() {
	a.rt.clearCache()
}

func isUsablePasswordCredential(cred *domain.Credential, now time.Time) bool {
	if cred == nil {
		return false
	}
	if cred.Kind != domain.CredentialKindPassword {
		return false
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

func (a *BasicAuthenticator) logThrottled(ctx context.Context, clientIP, username string) {
	if a.rt.logger != nil {
		a.rt.logger.WarnContext(ctx, "basic authentication throttled",
			slog.String("client_ip", detachBoundedLogAttr(clientIP)),
			slog.String("username", detachBoundedLogAttr(username)),
		)
	}
}

func (a *BasicAuthenticator) computeCacheKey(userID, username, credID, secretHash, password string) [32]byte {
	return hmacLengthPrefixed(a.rt.hmacSecret[:], "cache", userID, username, credID, secretHash, password)
}

func parseBasicAuthHeader(r *http.Request) (username, password string, hasBasic bool, err error) {
	var basicPayload string
	basicCount := 0
	for _, hdr := range r.Header.Values("Authorization") {
		scheme, rest, found := strings.Cut(strings.TrimSpace(hdr), " ")
		if !strings.EqualFold(scheme, "Basic") {
			continue
		}
		basicCount++
		if !found {
			return "", "", true, ErrInvalidCredentials
		}
		basicPayload = strings.TrimSpace(rest)
	}

	if basicCount == 0 {
		return "", "", false, nil
	}
	if basicCount > 1 || basicPayload == "" {
		return "", "", true, ErrInvalidCredentials
	}

	decoded, decErr := base64.StdEncoding.DecodeString(basicPayload)
	if decErr != nil {
		return "", "", true, ErrInvalidCredentials
	}

	user, pass, ok := strings.Cut(string(decoded), ":")
	if !ok || user == "" || pass == "" || strings.ContainsRune(user, 0) || strings.ContainsRune(pass, 0) {
		return "", "", true, ErrInvalidCredentials
	}

	return user, pass, true, nil
}
