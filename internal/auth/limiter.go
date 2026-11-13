package auth

import (
	"container/list"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// Common errors returned by Tobiten authenticators.
var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrAuthThrottled      = errors.New("authentication throttled")
	ErrAuthBusy           = errors.New("authentication verifier busy")
)

// Default production bounds for bounded verification, caching, and throttling.
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

	maxThrottleLogAttrBytes = 128
)

// Valid PHC-formatted Argon2id hash with DefaultArgon2idParams used for constant-work
// dummy verification when a credential or user does not exist.
const dummyArgon2idHash = "$argon2id$v=19$m=65536,t=3,p=4$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

// HashLimiterOptions configures bounded concurrent Argon2id verification and singleflight deduplication.
type HashLimiterOptions struct {
	MaxConcurrentHashes int
	MaxHashQueue        int
	HashWaitTimeout     time.Duration
	VerifyFunc          func(secret, encodedHash string) (bool, error)
}

type inflightCall struct {
	done  chan struct{}
	match bool
	err   error
}

// HashLimiter bounds concurrent Argon2id verifications and deduplicates identical
// in-flight verifications across authenticators using fixed-width HMAC keys (ADR-0006 §3).
type HashLimiter struct {
	hmacSecret      [32]byte
	verifyFunc      func(secret, encodedHash string) (bool, error)
	hashSem         chan struct{}
	hashQueueSem    chan struct{}
	hashWaitTimeout time.Duration

	sfMu     sync.Mutex
	inflight map[[32]byte]*inflightCall
}

// NewHashLimiter constructs a bounded Argon2id verification limiter.
func NewHashLimiter(opts HashLimiterOptions) (*HashLimiter, error) {
	opts = applyLimiterOptionDefaults(opts)

	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, fmt.Errorf("generating limiter hmac secret: %w", err)
	}

	return &HashLimiter{
		hmacSecret:      secret,
		verifyFunc:      opts.VerifyFunc,
		hashSem:         make(chan struct{}, opts.MaxConcurrentHashes),
		hashQueueSem:    make(chan struct{}, opts.MaxConcurrentHashes+opts.MaxHashQueue),
		hashWaitTimeout: opts.HashWaitTimeout,
		inflight:        make(map[[32]byte]*inflightCall),
	}, nil
}

func applyLimiterOptionDefaults(opts HashLimiterOptions) HashLimiterOptions {
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
	if opts.VerifyFunc == nil {
		opts.VerifyFunc = VerifyPassword
	}
	return opts
}

// Verify runs secret verification against encodedHash under bounded concurrency and singleflight.
// If preVerify is non-nil, it is invoked after acquiring a hashing slot and before running the KDF.
func (l *HashLimiter) Verify(ctx context.Context, secret, encodedHash string, preVerify func() error) (bool, error) {
	sfKey := hmacLengthPrefixed(l.hmacSecret[:], "sf", encodedHash, secret)

	l.sfMu.Lock()
	if call, exists := l.inflight[sfKey]; exists {
		l.sfMu.Unlock()
		select {
		case <-call.done:
			return call.match, call.err
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	call := &inflightCall{done: make(chan struct{})}
	l.inflight[sfKey] = call
	l.sfMu.Unlock()

	defer func() {
		l.sfMu.Lock()
		delete(l.inflight, sfKey)
		l.sfMu.Unlock()
		close(call.done)
	}()

	match, err := l.runBoundedVerify(ctx, secret, encodedHash, preVerify)
	call.match = match
	call.err = err
	return match, err
}

func (l *HashLimiter) runBoundedVerify(ctx context.Context, secret, encodedHash string, preVerify func() error) (bool, error) {
	select {
	case l.hashQueueSem <- struct{}{}:
		defer func() { <-l.hashQueueSem }()
	default:
		return false, ErrAuthBusy
	}

	timer := time.NewTimer(l.hashWaitTimeout)
	defer timer.Stop()

	select {
	case l.hashSem <- struct{}{}:
		defer func() { <-l.hashSem }()
	case <-timer.C:
		return false, ErrAuthBusy
	case <-ctx.Done():
		return false, ctx.Err()
	}

	if preVerify != nil {
		if err := preVerify(); err != nil {
			return false, err
		}
	}

	return l.verifyFunc(secret, encodedHash)
}

type cacheAndThrottleDefaults struct {
	logger          *slog.Logger
	cacheMaxEntries int
	cacheTTL        time.Duration
	ipMaxFailures   int
	idMaxFailures   int
	failureWindow   time.Duration
	baseCooldown    time.Duration
	maxCooldown     time.Duration
	throttleMaxKeys int
	now             func() time.Time
}

func normalizeCacheAndThrottleDefaults(d cacheAndThrottleDefaults) cacheAndThrottleDefaults {
	if d.logger == nil {
		d.logger = slog.Default()
	}
	if d.cacheMaxEntries <= 0 {
		d.cacheMaxEntries = DefaultCacheMaxEntries
	}
	if d.cacheTTL <= 0 {
		d.cacheTTL = DefaultCacheTTL
	}
	if d.ipMaxFailures <= 0 {
		d.ipMaxFailures = DefaultIPMaxFailures
	}
	if d.idMaxFailures <= 0 {
		d.idMaxFailures = DefaultUserMaxFailures
	}
	if d.failureWindow <= 0 {
		d.failureWindow = DefaultFailureWindow
	}
	if d.baseCooldown <= 0 {
		d.baseCooldown = DefaultBaseCooldown
	}
	if d.maxCooldown <= 0 {
		d.maxCooldown = DefaultMaxCooldown
	}
	if d.maxCooldown < d.baseCooldown {
		d.maxCooldown = d.baseCooldown
	}
	if d.throttleMaxKeys <= 0 {
		d.throttleMaxKeys = DefaultThrottleMaxKeys
	}
	if d.now == nil {
		d.now = time.Now
	}
	return d
}

func resolveHashLimiter(shared *HashLimiter, opts HashLimiterOptions) (*HashLimiter, error) {
	if shared != nil && opts.VerifyFunc == nil &&
		opts.MaxConcurrentHashes <= 0 && opts.MaxHashQueue <= 0 && opts.HashWaitTimeout <= 0 {
		return shared, nil
	}
	return NewHashLimiter(opts)
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

func detachBoundedLogAttr(s string) string {
	if len(s) > maxThrottleLogAttrBytes {
		s = s[:maxThrottleLogAttrBytes]
	}
	return strings.Clone(s)
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

type authThrottlePair struct {
	ipThrottle *boundedThrottleMap
	idThrottle *boundedThrottleMap
}

func newAuthThrottlePair(secret []byte, ipTag, idTag string, d cacheAndThrottleDefaults) authThrottlePair {
	return authThrottlePair{
		ipThrottle: newBoundedThrottleMap(secret, ipTag, d.ipMaxFailures, d.failureWindow, d.baseCooldown, d.maxCooldown, d.throttleMaxKeys),
		idThrottle: newBoundedThrottleMap(secret, idTag, d.idMaxFailures, d.failureWindow, d.baseCooldown, d.maxCooldown, d.throttleMaxKeys),
	}
}

func (p authThrottlePair) isIPThrottled(clientIP string, now time.Time) bool {
	return p.ipThrottle != nil && p.ipThrottle.isThrottled(clientIP, now)
}

func (p authThrottlePair) isThrottled(clientIP, id string, now time.Time) bool {
	if p.ipThrottle != nil && p.ipThrottle.isThrottled(clientIP, now) {
		return true
	}
	if p.idThrottle != nil && p.idThrottle.isThrottled(id, now) {
		return true
	}
	return false
}

func (p authThrottlePair) recordIPFailure(clientIP string, now time.Time) {
	if p.ipThrottle != nil {
		p.ipThrottle.recordFailure(clientIP, now)
	}
}

func (p authThrottlePair) recordFailure(clientIP, id string, now time.Time) {
	if p.ipThrottle != nil {
		p.ipThrottle.recordFailure(clientIP, now)
	}
	if p.idThrottle != nil {
		p.idThrottle.recordFailure(id, now)
	}
}

type authRuntimeConfig struct {
	Logger              *slog.Logger
	DisableCache        bool
	CacheMaxEntries     int
	CacheTTL            time.Duration
	DisableThrottle     bool
	IPMaxFailures       int
	IDMaxFailures       int
	FailureWindow       time.Duration
	BaseCooldown        time.Duration
	MaxCooldown         time.Duration
	ThrottleMaxKeys     int
	IPThrottleTag       string
	IDThrottleTag       string
	Limiter             *HashLimiter
	MaxConcurrentHashes int
	MaxHashQueue        int
	HashWaitTimeout     time.Duration
	Now                 func() time.Time
	VerifyFunc          func(secret, encodedHash string) (bool, error)
}

type authRuntime struct {
	logger     *slog.Logger
	now        func() time.Time
	hmacSecret [32]byte
	cache      *verificationCache
	cacheTTL   time.Duration
	throttles  authThrottlePair
	limiter    *HashLimiter
}

func newAuthRuntime(cfg authRuntimeConfig) (*authRuntime, error) {
	defaults := normalizeCacheAndThrottleDefaults(cacheAndThrottleDefaults{
		logger:          cfg.Logger,
		cacheMaxEntries: cfg.CacheMaxEntries,
		cacheTTL:        cfg.CacheTTL,
		ipMaxFailures:   cfg.IPMaxFailures,
		idMaxFailures:   cfg.IDMaxFailures,
		failureWindow:   cfg.FailureWindow,
		baseCooldown:    cfg.BaseCooldown,
		maxCooldown:     cfg.MaxCooldown,
		throttleMaxKeys: cfg.ThrottleMaxKeys,
		now:             cfg.Now,
	})

	limiter, err := resolveHashLimiter(cfg.Limiter, HashLimiterOptions{
		MaxConcurrentHashes: cfg.MaxConcurrentHashes,
		MaxHashQueue:        cfg.MaxHashQueue,
		HashWaitTimeout:     cfg.HashWaitTimeout,
		VerifyFunc:          cfg.VerifyFunc,
	})
	if err != nil {
		return nil, err
	}

	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, fmt.Errorf("generating auth hmac secret: %w", err)
	}

	var cache *verificationCache
	if !cfg.DisableCache {
		cache = newVerificationCache(defaults.cacheMaxEntries)
	}

	var throttles authThrottlePair
	if !cfg.DisableThrottle {
		throttles = newAuthThrottlePair(secret[:], cfg.IPThrottleTag, cfg.IDThrottleTag, defaults)
	}

	return &authRuntime{
		logger:     defaults.logger,
		now:        defaults.now,
		hmacSecret: secret,
		cache:      cache,
		cacheTTL:   defaults.cacheTTL,
		throttles:  throttles,
		limiter:    limiter,
	}, nil
}

func (rt *authRuntime) verifyWithBounds(ctx context.Context, clientIP, id, secret, encodedHash string) (bool, error) {
	return rt.limiter.Verify(ctx, secret, encodedHash, func() error {
		if rt.throttles.isThrottled(clientIP, id, rt.now()) {
			return ErrAuthThrottled
		}
		return nil
	})
}

func (rt *authRuntime) clearCache() {
	if rt.cache != nil {
		rt.cache.clear()
	}
}

func (p authThrottlePair) resetID(id string) {
	if p.idThrottle != nil {
		p.idThrottle.reset(id)
	}
}
