package auth

import (
	"container/list"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
)

// Common errors returned by BasicAuthenticator.
var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrAuthThrottled      = errors.New("authentication throttled")
	ErrAuthBusy           = errors.New("authentication verifier busy")
)

// Default production bounds for BasicAuthenticator.
const (
	DefaultCacheMaxEntries     = 1024
	DefaultCacheTTL            = 5 * time.Minute
	DefaultIPMaxFailures       = 10
	DefaultUserMaxFailures     = 5
	DefaultFailureWindow       = 5 * time.Minute
	DefaultBaseCooldown        = 2 * time.Second
	DefaultMaxCooldown         = 60 * time.Second
	DefaultThrottleMaxKeys     = 10000
	DefaultMaxConcurrentHashes = 4
	DefaultMaxHashQueue        = 32
	DefaultHashWaitTimeout     = 5 * time.Second
)

// Valid PHC-formatted Argon2id hash with DefaultArgon2idParams used for constant-work
// dummy verification when a username or active PASSWORD credential does not exist.
const dummyArgon2idHash = "$argon2id$v=19$m=65536,t=3,p=4$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

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
	MaxConcurrentHashes int
	MaxHashQueue        int
	HashWaitTimeout     time.Duration

	// Optional test hooks.
	Now        func() time.Time
	VerifyFunc func(password, encodedHash string) (bool, error)
}

type inflightCall struct {
	done  chan struct{}
	match bool
	err   error
}

// BasicAuthenticator implements HTTP Basic authentication against PASSWORD credentials
// with per-IP and per-user failure throttling, bounded concurrent Argon2id hashing,
// and a bounded verification cache bound to exact credential and hash identity (ADR-0006 §3).
type BasicAuthenticator struct {
	store      BasicStore
	logger     *slog.Logger
	now        func() time.Time
	verifyFunc func(password, encodedHash string) (bool, error)

	hmacSecret [32]byte
	cache      *verificationCache
	cacheTTL   time.Duration

	ipThrottle   *boundedThrottleMap
	userThrottle *boundedThrottleMap

	hashSem         chan struct{}
	hashQueueSem    chan struct{}
	hashWaitTimeout time.Duration

	sfMu     sync.Mutex
	inflight map[[32]byte]*inflightCall
}

// NewBasicAuthenticator constructs a BasicAuthenticator backed by store.
func NewBasicAuthenticator(store BasicStore, opts BasicOptions) (*BasicAuthenticator, error) {
	if store == nil {
		return nil, errors.New("basic authenticator store is required")
	}

	opts = applyBasicOptionDefaults(opts)

	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, fmt.Errorf("generating cache hmac secret: %w", err)
	}

	var cache *verificationCache
	if !opts.DisableCache {
		cache = newVerificationCache(opts.CacheMaxEntries)
	}

	var ipThrottle, userThrottle *boundedThrottleMap
	if !opts.DisableThrottle {
		ipThrottle = newBoundedThrottleMap(secret[:], "throttle-ip", opts.IPMaxFailures, opts.FailureWindow, opts.BaseCooldown, opts.MaxCooldown, opts.ThrottleMaxKeys)
		userThrottle = newBoundedThrottleMap(secret[:], "throttle-user", opts.UserMaxFailures, opts.FailureWindow, opts.BaseCooldown, opts.MaxCooldown, opts.ThrottleMaxKeys)
	}

	return &BasicAuthenticator{
		store:           store,
		logger:          opts.Logger,
		now:             opts.Now,
		verifyFunc:      opts.VerifyFunc,
		hmacSecret:      secret,
		cache:           cache,
		cacheTTL:        opts.CacheTTL,
		ipThrottle:      ipThrottle,
		userThrottle:    userThrottle,
		hashSem:         make(chan struct{}, opts.MaxConcurrentHashes),
		hashQueueSem:    make(chan struct{}, opts.MaxConcurrentHashes+opts.MaxHashQueue),
		hashWaitTimeout: opts.HashWaitTimeout,
		inflight:        make(map[[32]byte]*inflightCall),
	}, nil
}

func applyBasicOptionDefaults(opts BasicOptions) BasicOptions {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.CacheMaxEntries <= 0 {
		opts.CacheMaxEntries = DefaultCacheMaxEntries
	}
	if opts.CacheTTL <= 0 {
		opts.CacheTTL = DefaultCacheTTL
	}
	if opts.IPMaxFailures <= 0 {
		opts.IPMaxFailures = DefaultIPMaxFailures
	}
	if opts.UserMaxFailures <= 0 {
		opts.UserMaxFailures = DefaultUserMaxFailures
	}
	if opts.FailureWindow <= 0 {
		opts.FailureWindow = DefaultFailureWindow
	}
	if opts.BaseCooldown <= 0 {
		opts.BaseCooldown = DefaultBaseCooldown
	}
	if opts.MaxCooldown <= 0 {
		opts.MaxCooldown = DefaultMaxCooldown
	}
	if opts.MaxCooldown < opts.BaseCooldown {
		opts.MaxCooldown = opts.BaseCooldown
	}
	if opts.ThrottleMaxKeys <= 0 {
		opts.ThrottleMaxKeys = DefaultThrottleMaxKeys
	}
	if opts.MaxConcurrentHashes <= 0 {
		opts.MaxConcurrentHashes = DefaultMaxConcurrentHashes
	}
	if opts.MaxHashQueue < 0 {
		opts.MaxHashQueue = 0
	} else if opts.MaxHashQueue == 0 {
		opts.MaxHashQueue = DefaultMaxHashQueue
	}
	if opts.HashWaitTimeout <= 0 {
		opts.HashWaitTimeout = DefaultHashWaitTimeout
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.VerifyFunc == nil {
		opts.VerifyFunc = VerifyPassword
	}
	return opts
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
		now := a.now()
		if a.isIPThrottled(clientIP, now) {
			return module.AuthResult{Handled: true}, ErrAuthThrottled
		}
		a.recordIPFailure(clientIP, now)
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
		a.recordFailure(clientIP, "", a.now())
		return nil, ErrInvalidCredentials
	}

	now := a.now()
	if a.isThrottled(clientIP, username, now) {
		a.logThrottled(ctx, clientIP, username)
		return nil, ErrAuthThrottled
	}

	user, err := a.store.GetUserByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if _, vErr := a.verifyWithBounds(ctx, clientIP, username, password, dummyArgon2idHash); vErr != nil {
				return nil, vErr
			}
			a.recordFailure(clientIP, username, a.now())
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	cred, err := a.store.GetActivePasswordCredentialByUserID(ctx, user.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if _, vErr := a.verifyWithBounds(ctx, clientIP, username, password, dummyArgon2idHash); vErr != nil {
				return nil, vErr
			}
			a.recordFailure(clientIP, username, a.now())
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	if !isUsablePasswordCredential(cred, now) {
		a.recordFailure(clientIP, username, now)
		return nil, ErrInvalidCredentials
	}

	cacheKey := a.computeCacheKey(user.ID, username, cred.ID, *cred.SecretHash, password)
	if a.cache == nil || !a.cache.get(cacheKey, now) {
		match, vErr := a.verifyWithBounds(ctx, clientIP, username, password, *cred.SecretHash)
		if vErr != nil {
			return nil, vErr
		}
		if !match {
			a.recordFailure(clientIP, username, a.now())
			return nil, ErrInvalidCredentials
		}
		if a.cache != nil {
			a.cache.put(cacheKey, a.now().Add(a.cacheTTL))
		}
	}

	a.resetUserThrottle(username)

	roles, err := a.store.ListUserRoles(ctx, user.ID)
	if err != nil {
		return nil, err
	}

	return domain.NewPrincipal(*user, roles, cred.Kind, cred.ID, cred.Scopes), nil
}

// ClearCache removes all cached password verification entries.
func (a *BasicAuthenticator) ClearCache() {
	if a.cache != nil {
		a.cache.clear()
	}
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

const maxThrottleLogAttrBytes = 128

func (a *BasicAuthenticator) logThrottled(ctx context.Context, clientIP, username string) {
	if a.logger != nil {
		a.logger.WarnContext(ctx, "basic authentication throttled",
			slog.String("client_ip", detachBoundedLogAttr(clientIP)),
			slog.String("username", detachBoundedLogAttr(username)),
		)
	}
}

func detachBoundedLogAttr(s string) string {
	if len(s) > maxThrottleLogAttrBytes {
		s = s[:maxThrottleLogAttrBytes]
	}
	return strings.Clone(s)
}

func (a *BasicAuthenticator) isIPThrottled(clientIP string, now time.Time) bool {
	return a.ipThrottle != nil && a.ipThrottle.isThrottled(clientIP, now)
}

func (a *BasicAuthenticator) isThrottled(clientIP, username string, now time.Time) bool {
	if a.ipThrottle != nil && a.ipThrottle.isThrottled(clientIP, now) {
		return true
	}
	if a.userThrottle != nil && a.userThrottle.isThrottled(username, now) {
		return true
	}
	return false
}

func (a *BasicAuthenticator) recordIPFailure(clientIP string, now time.Time) {
	if a.ipThrottle != nil {
		a.ipThrottle.recordFailure(clientIP, now)
	}
}

func (a *BasicAuthenticator) recordFailure(clientIP, username string, now time.Time) {
	if a.ipThrottle != nil {
		a.ipThrottle.recordFailure(clientIP, now)
	}
	if a.userThrottle != nil {
		a.userThrottle.recordFailure(username, now)
	}
}

func (a *BasicAuthenticator) resetUserThrottle(username string) {
	if a.userThrottle != nil {
		a.userThrottle.reset(username)
	}
}

func (a *BasicAuthenticator) verifyWithBounds(ctx context.Context, clientIP, username, password, encodedHash string) (bool, error) {
	sfKey := hmacLengthPrefixed(a.hmacSecret[:], "sf", encodedHash, password)

	a.sfMu.Lock()
	if call, exists := a.inflight[sfKey]; exists {
		a.sfMu.Unlock()
		select {
		case <-call.done:
			return call.match, call.err
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	call := &inflightCall{done: make(chan struct{})}
	a.inflight[sfKey] = call
	a.sfMu.Unlock()

	defer func() {
		a.sfMu.Lock()
		delete(a.inflight, sfKey)
		a.sfMu.Unlock()
		close(call.done)
	}()

	match, err := a.runBoundedVerify(ctx, clientIP, username, password, encodedHash)
	call.match = match
	call.err = err
	return match, err
}

func (a *BasicAuthenticator) runBoundedVerify(ctx context.Context, clientIP, username, password, encodedHash string) (bool, error) {
	select {
	case a.hashQueueSem <- struct{}{}:
		defer func() { <-a.hashQueueSem }()
	default:
		return false, ErrAuthBusy
	}

	timer := time.NewTimer(a.hashWaitTimeout)
	defer timer.Stop()

	select {
	case a.hashSem <- struct{}{}:
		defer func() { <-a.hashSem }()
	case <-timer.C:
		return false, ErrAuthBusy
	case <-ctx.Done():
		return false, ctx.Err()
	}

	if a.isThrottled(clientIP, username, a.now()) {
		return false, ErrAuthThrottled
	}

	return a.verifyFunc(password, encodedHash)
}

func (a *BasicAuthenticator) computeCacheKey(userID, username, credID, secretHash, password string) [32]byte {
	return hmacLengthPrefixed(a.hmacSecret[:], "cache", userID, username, credID, secretHash, password)
}

func hmacLengthPrefixed(secret []byte, parts ...string) [32]byte {
	mac := hmac.New(sha256.New, secret)
	var lenBuf [4]byte
	for _, p := range parts {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(p)))
		_, _ = mac.Write(lenBuf[:])
		_, _ = mac.Write([]byte(p))
	}
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
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

// extractClientIP extracts the peer IP directly from RemoteAddr without trusting
// arbitrary forwarded headers (X-Forwarded-For, X-Real-IP, Forwarded).
func extractClientIP(remoteAddr string) string {
	trimmed := strings.TrimSpace(remoteAddr)
	if trimmed == "" {
		return "unknown"
	}
	if ap, err := netip.ParseAddrPort(trimmed); err == nil {
		return ap.Addr().Unmap().String()
	}
	if host, _, err := net.SplitHostPort(trimmed); err == nil {
		if addr, err := netip.ParseAddr(host); err == nil {
			return addr.Unmap().String()
		}
		if host != "" {
			return host
		}
	}
	if addr, err := netip.ParseAddr(trimmed); err == nil {
		return addr.Unmap().String()
	}
	return trimmed
}

type cacheEntry struct {
	key       [32]byte
	expiresAt time.Time
	elem      *list.Element
}

type verificationCache struct {
	maxEntries int
	mu         sync.Mutex
	entries    map[[32]byte]*cacheEntry
	lru        *list.List
}

func newVerificationCache(maxEntries int) *verificationCache {
	return &verificationCache{
		maxEntries: maxEntries,
		entries:    make(map[[32]byte]*cacheEntry),
		lru:        list.New(),
	}
}

func (c *verificationCache) get(key [32]byte, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return false
	}
	if !now.Before(entry.expiresAt) {
		c.lru.Remove(entry.elem)
		delete(c.entries, key)
		return false
	}
	c.lru.MoveToFront(entry.elem)
	return true
}

func (c *verificationCache) put(key [32]byte, expiresAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[key]; ok {
		entry.expiresAt = expiresAt
		c.lru.MoveToFront(entry.elem)
		return
	}

	for len(c.entries) >= c.maxEntries {
		back := c.lru.Back()
		if back == nil {
			break
		}
		evicted := back.Value.(*cacheEntry)
		c.lru.Remove(back)
		delete(c.entries, evicted.key)
	}

	entry := &cacheEntry{
		key:       key,
		expiresAt: expiresAt,
	}
	entry.elem = c.lru.PushFront(entry)
	c.entries[key] = entry
}

func (c *verificationCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[[32]byte]*cacheEntry)
	c.lru.Init()
}

type throttleEntry struct {
	key            [32]byte
	failures       int
	trips          int
	lastFailure    time.Time
	throttledUntil time.Time
	elem           *list.Element
}

type boundedThrottleMap struct {
	hmacSecret   [32]byte
	tag          string
	maxFailures  int
	window       time.Duration
	baseCooldown time.Duration
	maxCooldown  time.Duration
	maxEntries   int

	mu      sync.Mutex
	entries map[[32]byte]*throttleEntry
	lru     *list.List
}

func newBoundedThrottleMap(hmacSecret []byte, tag string, maxFailures int, window, baseCooldown, maxCooldown time.Duration, maxEntries int) *boundedThrottleMap {
	var secret [32]byte
	copy(secret[:], hmacSecret)
	return &boundedThrottleMap{
		hmacSecret:   secret,
		tag:          tag,
		maxFailures:  maxFailures,
		window:       window,
		baseCooldown: baseCooldown,
		maxCooldown:  maxCooldown,
		maxEntries:   maxEntries,
		entries:      make(map[[32]byte]*throttleEntry),
		lru:          list.New(),
	}
}

func (m *boundedThrottleMap) keyFor(rawKey string) [32]byte {
	return hmacLengthPrefixed(m.hmacSecret[:], m.tag, rawKey)
}

func (m *boundedThrottleMap) isThrottled(rawKey string, now time.Time) bool {
	if rawKey == "" {
		return false
	}
	key := m.keyFor(rawKey)
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[key]
	if !ok {
		return false
	}
	if !now.Before(entry.throttledUntil) {
		if now.Sub(entry.lastFailure) > m.window {
			m.lru.Remove(entry.elem)
			delete(m.entries, key)
		}
		return false
	}
	m.lru.MoveToFront(entry.elem)
	return true
}

func (m *boundedThrottleMap) recordFailure(rawKey string, now time.Time) {
	if rawKey == "" {
		return
	}
	key := m.keyFor(rawKey)
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[key]
	if !ok {
		m.evictIfFull()
		entry = &throttleEntry{
			key:         key,
			failures:    1,
			lastFailure: now,
		}
		if entry.failures >= m.maxFailures {
			entry.trips = 1
			entry.throttledUntil = now.Add(m.baseCooldown)
		}
		entry.elem = m.lru.PushFront(entry)
		m.entries[key] = entry
		return
	}

	m.lru.MoveToFront(entry.elem)
	if now.Before(entry.throttledUntil) {
		entry.lastFailure = now
		return
	}

	if now.Sub(entry.lastFailure) > m.window {
		entry.failures = 0
		entry.trips = 0
		entry.throttledUntil = time.Time{}
	}

	entry.failures++
	entry.lastFailure = now
	if entry.failures >= m.maxFailures {
		entry.trips++
		entry.throttledUntil = now.Add(m.cooldownForTrips(entry.trips))
	}
}

func (m *boundedThrottleMap) reset(rawKey string) {
	if rawKey == "" {
		return
	}
	key := m.keyFor(rawKey)
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.entries[key]; ok {
		m.lru.Remove(entry.elem)
		delete(m.entries, key)
	}
}

func (m *boundedThrottleMap) cooldownForTrips(trips int) time.Duration {
	cooldown := m.baseCooldown
	for i := 1; i < trips && cooldown < m.maxCooldown; i++ {
		if cooldown > m.maxCooldown/2 {
			return m.maxCooldown
		}
		cooldown *= 2
	}
	if cooldown > m.maxCooldown {
		return m.maxCooldown
	}
	return cooldown
}

func (m *boundedThrottleMap) evictIfFull() {
	for len(m.entries) >= m.maxEntries {
		back := m.lru.Back()
		if back == nil {
			return
		}
		candidate := back.Value.(*throttleEntry)
		m.lru.Remove(back)
		delete(m.entries, candidate.key)
	}
}
