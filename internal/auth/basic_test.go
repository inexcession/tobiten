package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

type stubBasicStore struct {
	mu           sync.RWMutex
	users        map[string]domain.User
	creds        map[string]*domain.Credential
	roles        map[string][]domain.Role
	roleCalls    atomic.Int32
	ignoreActive bool // when true, GetActivePasswordCredentialByUserID returns stored cred even if revoked/expired (to verify defense-in-depth checks)
	forcedErr    error
}

func newStubBasicStore() *stubBasicStore {
	return &stubBasicStore{
		users: make(map[string]domain.User),
		creds: make(map[string]*domain.Credential),
		roles: make(map[string][]domain.Role),
	}
}

func (s *stubBasicStore) setUser(u domain.User, cred *domain.Credential, roles []domain.Role) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[u.Username] = u
	if cred != nil {
		cp := *cred
		s.creds[u.ID] = &cp
	} else {
		delete(s.creds, u.ID)
	}
	rCopy := make([]domain.Role, len(roles))
	copy(rCopy, roles)
	s.roles[u.ID] = rCopy
}

func (s *stubBasicStore) GetUserByUsername(_ context.Context, username string) (*domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.forcedErr != nil {
		return nil, s.forcedErr
	}
	u, ok := s.users[username]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return &u, nil
}

func (s *stubBasicStore) GetActivePasswordCredentialByUserID(_ context.Context, userID string) (*domain.Credential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.forcedErr != nil {
		return nil, s.forcedErr
	}
	c, ok := s.creds[userID]
	if !ok || c == nil {
		return nil, storage.ErrNotFound
	}
	if !s.ignoreActive {
		if c.Kind != domain.CredentialKindPassword || c.IsRevoked() || c.IsExpired(time.Now()) {
			return nil, storage.ErrNotFound
		}
	}
	cp := *c
	return &cp, nil
}

func (s *stubBasicStore) ListUserRoles(_ context.Context, userID string) ([]domain.Role, error) {
	s.roleCalls.Add(1)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.forcedErr != nil {
		return nil, s.forcedErr
	}
	roles := s.roles[userID]
	out := make([]domain.Role, len(roles))
	copy(out, roles)
	return out, nil
}

func makeBasicRequest(username, password, remoteAddr string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	raw := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	req.Header.Set("Authorization", "Basic "+raw)
	return req
}

func fastVerify(password, encodedHash string) (bool, error) {
	return encodedHash == "fast:"+password, nil
}

func TestBasicAuthenticator_HTTPHeaderParsingAndAuth(t *testing.T) {
	store := newStubBasicStore()
	hash, err := HashPassword("p@ss:with:colons")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	scheme := HashSchemeArgon2id
	adminRole, _ := domain.BuiltInRoleByID(domain.RoleAdminID)
	store.setUser(
		domain.User{ID: "u-1", Username: "alice"},
		&domain.Credential{
			ID:         "c-1",
			UserID:     "u-1",
			Kind:       domain.CredentialKindPassword,
			SecretHash: &hash,
			HashScheme: &scheme,
		},
		[]domain.Role{adminRole},
	)

	auth, err := NewBasicAuthenticator(store, BasicOptions{})
	if err != nil {
		t.Fatalf("NewBasicAuthenticator: %v", err)
	}

	if auth.Name() != "basic" {
		t.Fatalf("Name() = %q, want basic", auth.Name())
	}

	t.Run("no authorization header passes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		res, err := auth.Authenticate(req)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if res.Handled || res.Principal != nil {
			t.Fatalf("expected AuthPass, got %+v", res)
		}
	})

	t.Run("non-basic scheme passes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		req.Header.Set("Authorization", "Bearer tobiten_pat_abc_xyz")
		res, err := auth.Authenticate(req)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if res.Handled {
			t.Fatalf("expected AuthPass for Bearer header, got %+v", res)
		}
	})

	t.Run("valid basic with colons in password succeeds with real Argon2id", func(t *testing.T) {
		req := makeBasicRequest("alice", "p@ss:with:colons", "10.0.0.1:12345")
		res, err := auth.Authenticate(req)
		if err != nil {
			t.Fatalf("Authenticate failed: %v", err)
		}
		if !res.Handled || res.Principal == nil {
			t.Fatalf("expected principal, got %+v", res)
		}
		if res.Principal.User.ID != "u-1" || res.Principal.User.Username != "alice" {
			t.Errorf("unexpected user: %+v", res.Principal.User)
		}
		if res.Principal.CredentialID != "c-1" || res.Principal.CredentialKind != domain.CredentialKindPassword {
			t.Errorf("unexpected credential metadata: %+v", res.Principal)
		}
		if !res.Principal.HasPermission(domain.PermissionUsersAdmin) {
			t.Errorf("expected Admin permissions, got %v", res.Principal.Permissions)
		}
		if got := store.roleCalls.Load(); got != 1 {
			t.Errorf("expected single ListUserRoles query, got %d", got)
		}
	})

	t.Run("lowercase basic scheme accepted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		req.RemoteAddr = "10.0.0.2:12345"
		req.Header.Set("Authorization", "basic "+base64.StdEncoding.EncodeToString([]byte("alice:p@ss:with:colons")))
		res, err := auth.Authenticate(req)
		if err != nil || !res.Handled || res.Principal == nil {
			t.Fatalf("expected success for lowercase basic scheme, got res=%+v err=%v", res, err)
		}
	})

	malformedCases := []struct {
		name    string
		headers []string
	}{
		{name: "empty payload", headers: []string{"Basic"}},
		{name: "invalid base64", headers: []string{"Basic !!!not-base64!!!"}},
		{name: "missing colon", headers: []string{"Basic " + base64.StdEncoding.EncodeToString([]byte("aliceonly"))}},
		{name: "empty username", headers: []string{"Basic " + base64.StdEncoding.EncodeToString([]byte(":secret"))}},
		{name: "empty password", headers: []string{"Basic " + base64.StdEncoding.EncodeToString([]byte("alice:"))}},
		{name: "null byte", headers: []string{"Basic " + base64.StdEncoding.EncodeToString([]byte("alice:sec\x00ret"))}},
		{
			name: "multiple basic headers",
			headers: []string{
				"Basic " + base64.StdEncoding.EncodeToString([]byte("alice:p@ss:with:colons")),
				"Basic " + base64.StdEncoding.EncodeToString([]byte("bob:other")),
			},
		},
	}
	for i, tc := range malformedCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
			req.RemoteAddr = fmt.Sprintf("10.0.1.%d:9000", i+1)
			for _, h := range tc.headers {
				req.Header.Add("Authorization", h)
			}
			res, err := auth.Authenticate(req)
			if err == nil || !res.Handled || res.Principal != nil {
				t.Fatalf("expected Handled error for %s, got res=%+v err=%v", tc.name, res, err)
			}
		})
	}
}

func TestBasicAuthenticator_CacheBindingAndLiveChecks(t *testing.T) {
	store := newStubBasicStore()
	store.ignoreActive = true // Ensure BasicAuthenticator itself checks IsRevoked/IsExpired even on hits.

	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	currentTime := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}

	var verifyCalls atomic.Int32
	scheme := HashSchemeArgon2id
	hash1 := "fast:pass-one"
	adminRole, _ := domain.BuiltInRoleByID(domain.RoleAdminID)
	readerRole, _ := domain.BuiltInRoleByID(domain.RoleReaderID)
	restrictedRole, _ := domain.BuiltInRoleByID(domain.RoleRestrictedReaderID)

	store.setUser(
		domain.User{ID: "u-1", Username: "alice"},
		&domain.Credential{
			ID:         "c-1",
			UserID:     "u-1",
			Kind:       domain.CredentialKindPassword,
			SecretHash: &hash1,
			HashScheme: &scheme,
		},
		[]domain.Role{adminRole},
	)

	auth, err := NewBasicAuthenticator(store, BasicOptions{
		CacheMaxEntries: 2,
		CacheTTL:        5 * time.Minute,
		Now:             currentTime,
		VerifyFunc: func(password, encodedHash string) (bool, error) {
			verifyCalls.Add(1)
			return fastVerify(password, encodedHash)
		},
	})
	if err != nil {
		t.Fatalf("NewBasicAuthenticator: %v", err)
	}

	// 1. Cold verify warms cache.
	p, err := auth.AuthenticateBasic(t.Context(), "alice", "pass-one", "192.0.2.10")
	if err != nil {
		t.Fatalf("initial auth failed: %v", err)
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("verifyCalls = %d, want 1", verifyCalls.Load())
	}
	if !p.HasPermission(domain.PermissionUsersAdmin) {
		t.Fatalf("expected Admin permission, got %v", p.Permissions)
	}

	// 2. Cache hit skips expensive KDF, still resolves live roles.
	p, err = auth.AuthenticateBasic(t.Context(), "alice", "pass-one", "192.0.2.10")
	if err != nil {
		t.Fatalf("cached auth failed: %v", err)
	}
	if p == nil || !p.HasPermission(domain.PermissionUsersAdmin) {
		t.Fatalf("expected Admin permission on cache hit, got %+v", p)
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("verifyCalls = %d, want 1 (cache hit)", verifyCalls.Load())
	}
	if store.roleCalls.Load() != 2 {
		t.Fatalf("roleCalls = %d, want 2 (live role resolution on hit)", store.roleCalls.Load())
	}

	// 3. Wrong password misses cache immediately.
	if _, err := auth.AuthenticateBasic(t.Context(), "alice", "wrong-pass", "192.0.2.11"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials for wrong password, got %v", err)
	}
	if verifyCalls.Load() != 2 {
		t.Fatalf("verifyCalls = %d, want 2", verifyCalls.Load())
	}

	// 4. Role change takes effect immediately even on cache hit.
	store.setUser(
		domain.User{ID: "u-1", Username: "alice"},
		&domain.Credential{
			ID:         "c-1",
			UserID:     "u-1",
			Kind:       domain.CredentialKindPassword,
			SecretHash: &hash1,
			HashScheme: &scheme,
		},
		[]domain.Role{restrictedRole},
	)
	p, err = auth.AuthenticateBasic(t.Context(), "alice", "pass-one", "192.0.2.10")
	if err != nil {
		t.Fatalf("auth after role downgrade failed: %v", err)
	}
	if verifyCalls.Load() != 2 {
		t.Fatalf("verifyCalls = %d, want 2 (still cache hit)", verifyCalls.Load())
	}
	if p.HasPermission(domain.PermissionUsersAdmin) {
		t.Errorf("stale Admin permission returned after role change")
	}
	if !p.RequiresAllowLabels() {
		t.Errorf("expected RequiresAllowLabels=true after Restricted reader change")
	}

	// 5. Warmed-cache password replacement (new hash & new credential ID) invalidates immediately.
	hash2 := "fast:pass-two"
	store.setUser(
		domain.User{ID: "u-1", Username: "alice"},
		&domain.Credential{
			ID:         "c-2",
			UserID:     "u-1",
			Kind:       domain.CredentialKindPassword,
			SecretHash: &hash2,
			HashScheme: &scheme,
		},
		[]domain.Role{readerRole},
	)
	if _, err := auth.AuthenticateBasic(t.Context(), "alice", "pass-one", "192.0.2.10"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("old password must fail immediately after replacement, got %v", err)
	}
	p, err = auth.AuthenticateBasic(t.Context(), "alice", "pass-two", "192.0.2.10")
	if err != nil || !p.HasPermission(domain.PermissionLibraryRead) {
		t.Fatalf("new password failed after replacement: p=%+v err=%v", p, err)
	}

	// 6. Warmed-cache in-place hash replacement (same credential ID, new SecretHash) invalidates immediately.
	hash3 := "fast:pass-three"
	store.setUser(
		domain.User{ID: "u-1", Username: "alice"},
		&domain.Credential{
			ID:         "c-2",
			UserID:     "u-1",
			Kind:       domain.CredentialKindPassword,
			SecretHash: &hash3,
			HashScheme: &scheme,
		},
		[]domain.Role{readerRole},
	)
	if _, err := auth.AuthenticateBasic(t.Context(), "alice", "pass-two", "192.0.2.10"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("old password must fail immediately on in-place hash update, got %v", err)
	}
	if _, err := auth.AuthenticateBasic(t.Context(), "alice", "pass-three", "192.0.2.10"); err != nil {
		t.Fatalf("new password failed after in-place hash update: %v", err)
	}

	// 7. Warmed-cache expiry check (Credential.IsExpired) rejects immediately without verifying.
	exp := currentTime().Add(30 * time.Second)
	store.setUser(
		domain.User{ID: "u-1", Username: "alice"},
		&domain.Credential{
			ID:         "c-2",
			UserID:     "u-1",
			Kind:       domain.CredentialKindPassword,
			SecretHash: &hash3,
			HashScheme: &scheme,
			ExpiresAt:  &exp,
		},
		[]domain.Role{readerRole},
	)
	if _, err := auth.AuthenticateBasic(t.Context(), "alice", "pass-three", "192.0.2.10"); err != nil {
		t.Fatalf("expected valid before expiry, got %v", err)
	}
	advance(31 * time.Second)
	beforeExpiryReject := verifyCalls.Load()
	if _, err := auth.AuthenticateBasic(t.Context(), "alice", "pass-three", "192.0.2.10"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials after expiry, got %v", err)
	}
	if verifyCalls.Load() != beforeExpiryReject {
		t.Errorf("expired credential should fail before KDF")
	}

	// 8. Warmed-cache revocation check (Credential.IsRevoked) rejects immediately.
	revokedAt := currentTime()
	store.setUser(
		domain.User{ID: "u-1", Username: "alice"},
		&domain.Credential{
			ID:         "c-2",
			UserID:     "u-1",
			Kind:       domain.CredentialKindPassword,
			SecretHash: &hash3,
			HashScheme: &scheme,
			RevokedAt:  &revokedAt,
		},
		[]domain.Role{readerRole},
	)
	if _, err := auth.AuthenticateBasic(t.Context(), "alice", "pass-three", "192.0.2.10"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials after revocation, got %v", err)
	}

	// 9. Cache TTL expiry and LRU eviction bound.
	hBob, hCarol := "fast:bob-pw", "fast:carol-pw"
	store.setUser(domain.User{ID: "u-1", Username: "alice"}, &domain.Credential{ID: "c-4", UserID: "u-1", Kind: domain.CredentialKindPassword, SecretHash: &hash3, HashScheme: &scheme}, []domain.Role{readerRole})
	store.setUser(domain.User{ID: "u-2", Username: "bob"}, &domain.Credential{ID: "c-5", UserID: "u-2", Kind: domain.CredentialKindPassword, SecretHash: &hBob, HashScheme: &scheme}, []domain.Role{readerRole})
	store.setUser(domain.User{ID: "u-3", Username: "carol"}, &domain.Credential{ID: "c-6", UserID: "u-3", Kind: domain.CredentialKindPassword, SecretHash: &hCarol, HashScheme: &scheme}, []domain.Role{readerRole})

	auth.ClearCache()
	verifyCalls.Store(0)

	_, _ = auth.AuthenticateBasic(t.Context(), "alice", "pass-three", "192.0.2.20")
	_, _ = auth.AuthenticateBasic(t.Context(), "bob", "bob-pw", "192.0.2.21")
	_, _ = auth.AuthenticateBasic(t.Context(), "carol", "carol-pw", "192.0.2.22") // evicts alice (CacheMaxEntries=2)
	if verifyCalls.Load() != 3 {
		t.Fatalf("verifyCalls = %d, want 3", verifyCalls.Load())
	}

	// Bob and Carol should still hit cache.
	_, _ = auth.AuthenticateBasic(t.Context(), "bob", "bob-pw", "192.0.2.21")
	_, _ = auth.AuthenticateBasic(t.Context(), "carol", "carol-pw", "192.0.2.22")
	if verifyCalls.Load() != 3 {
		t.Fatalf("expected cache hits for bob and carol, verifyCalls = %d", verifyCalls.Load())
	}

	// Alice was evicted, so she misses and re-verifies.
	_, _ = auth.AuthenticateBasic(t.Context(), "alice", "pass-three", "192.0.2.20")
	if verifyCalls.Load() != 4 {
		t.Fatalf("expected evicted alice to re-verify, verifyCalls = %d", verifyCalls.Load())
	}

	// Advancing past CacheTTL forces re-verification.
	advance(6 * time.Minute)
	_, _ = auth.AuthenticateBasic(t.Context(), "alice", "pass-three", "192.0.2.20")
	if verifyCalls.Load() != 5 {
		t.Fatalf("expected TTL expiry to re-verify, verifyCalls = %d", verifyCalls.Load())
	}
}

func TestBasicAuthenticator_ThrottlingAndNoLockout(t *testing.T) {
	store := newStubBasicStore()
	scheme := HashSchemeArgon2id
	hAlice := "fast:alice-secret"
	hBob := "fast:bob-secret"
	readerRole, _ := domain.BuiltInRoleByID(domain.RoleReaderID)
	store.setUser(domain.User{ID: "u-1", Username: "alice"}, &domain.Credential{ID: "c-1", UserID: "u-1", Kind: domain.CredentialKindPassword, SecretHash: &hAlice, HashScheme: &scheme}, []domain.Role{readerRole})
	store.setUser(domain.User{ID: "u-2", Username: "bob"}, &domain.Credential{ID: "c-2", UserID: "u-2", Kind: domain.CredentialKindPassword, SecretHash: &hBob, HashScheme: &scheme}, []domain.Role{readerRole})

	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	currentTime := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}

	var verifyCalls atomic.Int32
	auth, err := NewBasicAuthenticator(store, BasicOptions{
		UserMaxFailures: 3,
		IPMaxFailures:   5,
		FailureWindow:   1 * time.Minute,
		BaseCooldown:    5 * time.Second,
		MaxCooldown:     20 * time.Second,
		ThrottleMaxKeys: 100,
		Now:             currentTime,
		VerifyFunc: func(password, encodedHash string) (bool, error) {
			verifyCalls.Add(1)
			return fastVerify(password, encodedHash)
		},
	})
	if err != nil {
		t.Fatalf("NewBasicAuthenticator: %v", err)
	}

	t.Run("per-user throttle across rotating IPs without locking account", func(t *testing.T) {
		verifyCalls.Store(0)
		// 3 wrong attempts for alice from 3 different IPs.
		for i := range 3 {
			ip := fmt.Sprintf("198.51.100.%d", i+1)
			_, err := auth.AuthenticateBasic(t.Context(), "alice", "wrong", ip)
			if !errors.Is(err, ErrInvalidCredentials) {
				t.Fatalf("attempt %d err = %v, want ErrInvalidCredentials", i+1, err)
			}
		}
		if verifyCalls.Load() != 3 {
			t.Fatalf("verifyCalls = %d, want 3", verifyCalls.Load())
		}

		// 4th attempt from yet another IP is throttled BEFORE KDF (even with valid password!).
		_, err := auth.AuthenticateBasic(t.Context(), "alice", "alice-secret", "198.51.100.99")
		if !errors.Is(err, ErrAuthThrottled) {
			t.Fatalf("expected ErrAuthThrottled, got %v", err)
		}
		if verifyCalls.Load() != 3 {
			t.Fatalf("throttled request should not invoke KDF, verifyCalls = %d", verifyCalls.Load())
		}

		// Bob is unaffected on that same IP.
		if _, err := auth.AuthenticateBasic(t.Context(), "bob", "bob-secret", "198.51.100.99"); err != nil {
			t.Fatalf("bob should not be throttled, got %v", err)
		}

		// Advancing only part of cooldown (still throttled) and hammering must NOT permanently lock out alice.
		advance(2 * time.Second)
		for range 5 {
			_, err := auth.AuthenticateBasic(t.Context(), "alice", "wrong", "198.51.100.50")
			if !errors.Is(err, ErrAuthThrottled) {
				t.Fatalf("expected ErrAuthThrottled during cooldown, got %v", err)
			}
		}

		// Advancing past remaining cooldown (total +6s > 5s BaseCooldown) allows valid credentials to succeed (no lockout!).
		advance(4 * time.Second)
		p, err := auth.AuthenticateBasic(t.Context(), "alice", "alice-secret", "198.51.100.99")
		if err != nil || p == nil || p.User.Username != "alice" {
			t.Fatalf("alice should recover after cooldown with valid credentials (no lockout), p=%+v err=%v", p, err)
		}
	})

	t.Run("per-IP throttle ignores spoofed forwarded headers", func(t *testing.T) {
		verifyCalls.Store(0)
		// Send 5 failures for different nonexistent users from the same RemoteAddr while spoofing X-Forwarded-For / X-Real-IP.
		for i := range 5 {
			req := makeBasicRequest(fmt.Sprintf("unknown-%d", i), "wrong", "203.0.113.10:5555")
			req.Header.Set("X-Forwarded-For", fmt.Sprintf("1.1.1.%d", i+1))
			req.Header.Set("X-Real-IP", fmt.Sprintf("2.2.2.%d", i+1))
			req.Header.Set("Forwarded", fmt.Sprintf("for=3.3.3.%d", i+1))
			res, err := auth.Authenticate(req)
			if !res.Handled || !errors.Is(err, ErrInvalidCredentials) {
				t.Fatalf("attempt %d expected ErrInvalidCredentials, got res=%+v err=%v", i+1, res, err)
			}
		}
		if verifyCalls.Load() != 5 {
			t.Fatalf("verifyCalls = %d, want 5", verifyCalls.Load())
		}

		// 6th request from same RemoteAddr (even with valid bob creds and spoofed XFF) is throttled without KDF.
		req := makeBasicRequest("bob", "bob-secret", "203.0.113.10:5556")
		req.Header.Set("X-Forwarded-For", "8.8.8.8")
		res, err := auth.Authenticate(req)
		if !res.Handled || !errors.Is(err, ErrAuthThrottled) {
			t.Fatalf("expected ErrAuthThrottled from same RemoteAddr, got res=%+v err=%v", res, err)
		}
		if verifyCalls.Load() != 5 {
			t.Fatalf("throttled IP request must not invoke KDF, verifyCalls = %d", verifyCalls.Load())
		}

		// Different RemoteAddr for bob succeeds.
		reqOther := makeBasicRequest("bob", "bob-secret", "203.0.113.20:1111")
		res, err = auth.Authenticate(reqOther)
		if err != nil || !res.Handled || res.Principal == nil {
			t.Fatalf("expected bob from other IP to succeed, got res=%+v err=%v", res, err)
		}
	})

	t.Run("exponential backoff and bounded throttle maps", func(t *testing.T) {
		authBounded, err := NewBasicAuthenticator(store, BasicOptions{
			UserMaxFailures: 2,
			IPMaxFailures:   2,
			FailureWindow:   10 * time.Minute,
			BaseCooldown:    2 * time.Second,
			MaxCooldown:     6 * time.Second,
			ThrottleMaxKeys: 3,
			Now:             currentTime,
			VerifyFunc:      fastVerify,
		})
		if err != nil {
			t.Fatalf("NewBasicAuthenticator: %v", err)
		}

		// Trip exponential backoff on user "unknown-exp" across distinct IPs.
		_, _ = authBounded.AuthenticateBasic(t.Context(), "unknown-exp", "bad", "10.1.0.1")
		_, _ = authBounded.AuthenticateBasic(t.Context(), "unknown-exp", "bad", "10.1.0.2")
		if _, err := authBounded.AuthenticateBasic(t.Context(), "unknown-exp", "bad", "10.1.0.3"); !errors.Is(err, ErrAuthThrottled) {
			t.Fatalf("expected 1st throttle (2s), got %v", err)
		}
		advance(3 * time.Second) // Past 2s cooldown -> next failure trips 4s cooldown.
		if _, err := authBounded.AuthenticateBasic(t.Context(), "unknown-exp", "bad", "10.1.0.4"); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("expected attempt allowed after 2s cooldown, got %v", err)
		}
		advance(3 * time.Second) // Still inside 4s cooldown!
		if _, err := authBounded.AuthenticateBasic(t.Context(), "unknown-exp", "bad", "10.1.0.5"); !errors.Is(err, ErrAuthThrottled) {
			t.Fatalf("expected 2nd throttle (4s) active at +3s, got %v", err)
		}

		// Exceed ThrottleMaxKeys=3 and confirm map sizes stay bounded.
		for i := range 20 {
			_, _ = authBounded.AuthenticateBasic(t.Context(), fmt.Sprintf("flood-%d", i), "bad", fmt.Sprintf("10.2.0.%d", i+1))
		}
		if got := len(authBounded.ipThrottle.entries); got > 3 {
			t.Errorf("ipThrottle size = %d, want <= 3", got)
		}
		if got := len(authBounded.userThrottle.entries); got > 3 {
			t.Errorf("userThrottle size = %d, want <= 3", got)
		}
	})
}

func TestBasicAuthenticator_ConcurrencyBoundsAndSingleflight(t *testing.T) {
	store := newStubBasicStore()
	scheme := HashSchemeArgon2id
	hAlice := "fast:alice-secret"
	readerRole, _ := domain.BuiltInRoleByID(domain.RoleReaderID)
	store.setUser(domain.User{ID: "u-1", Username: "alice"}, &domain.Credential{ID: "c-1", UserID: "u-1", Kind: domain.CredentialKindPassword, SecretHash: &hAlice, HashScheme: &scheme}, []domain.Role{readerRole})

	t.Run("singleflight deduplicates identical concurrent cold-cache logins", func(t *testing.T) {
		var verifyCalls atomic.Int32
		entered := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once

		auth, err := NewBasicAuthenticator(store, BasicOptions{
			MaxConcurrentHashes: 4,
			VerifyFunc: func(password, encodedHash string) (bool, error) {
				verifyCalls.Add(1)
				once.Do(func() { close(entered) })
				<-release
				return fastVerify(password, encodedHash)
			},
		})
		if err != nil {
			t.Fatalf("NewBasicAuthenticator: %v", err)
		}

		const workers = 16
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for i := range workers {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				p, authErr := auth.AuthenticateBasic(t.Context(), "alice", "alice-secret", fmt.Sprintf("10.10.0.%d", idx+1))
				if authErr == nil && p == nil {
					authErr = errors.New("nil principal")
				}
				errs <- authErr
			}(i)
		}

		<-entered
		time.Sleep(25 * time.Millisecond)
		close(release)
		wg.Wait()
		close(errs)

		for e := range errs {
			if e != nil {
				t.Fatalf("concurrent singleflight login failed: %v", e)
			}
		}
		if got := verifyCalls.Load(); got != 1 {
			t.Fatalf("expected singleflight to invoke VerifyFunc once, got %d", got)
		}
	})

	t.Run("peak concurrent hashing is bounded and excess queue fails fast", func(t *testing.T) {
		var active, maxActive atomic.Int32
		gate := make(chan struct{})

		auth, err := NewBasicAuthenticator(store, BasicOptions{
			DisableCache:        true,
			DisableThrottle:     true,
			MaxConcurrentHashes: 2,
			MaxHashQueue:        2, // total queue capacity = 2 running + 2 waiting = 4
			HashWaitTimeout:     2 * time.Second,
			VerifyFunc: func(password, encodedHash string) (bool, error) {
				cur := active.Add(1)
				for {
					prev := maxActive.Load()
					if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
						break
					}
				}
				<-gate
				active.Add(-1)
				return fastVerify(password, encodedHash)
			},
		})
		if err != nil {
			t.Fatalf("NewBasicAuthenticator: %v", err)
		}

		const total = 10
		var wg sync.WaitGroup
		var busyCount, badCredCount atomic.Int32

		for i := range total {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				// Distinct passwords so singleflight does not merge them.
				_, authErr := auth.AuthenticateBasic(t.Context(), "alice", fmt.Sprintf("wrong-%d", idx), fmt.Sprintf("10.20.0.%d", idx+1))
				if errors.Is(authErr, ErrAuthBusy) {
					busyCount.Add(1)
				} else if errors.Is(authErr, ErrInvalidCredentials) {
					badCredCount.Add(1)
				} else {
					t.Errorf("unexpected error: %v", authErr)
				}
			}(i)
		}

		// Wait briefly for 2 active + 2 queued; excess 6 should fail immediately with ErrAuthBusy.
		deadline := time.Now().Add(2 * time.Second)
		for busyCount.Load() < 6 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		close(gate)
		wg.Wait()

		if peak := maxActive.Load(); peak > 2 {
			t.Fatalf("peak concurrent hashes = %d, want <= 2", peak)
		}
		if busyCount.Load() == 0 {
			t.Fatalf("expected excess requests to fail fast with ErrAuthBusy, got 0")
		}
		if busyCount.Load()+badCredCount.Load() != total {
			t.Fatalf("unexpected total outcomes: busy=%d bad=%d", busyCount.Load(), badCredCount.Load())
		}
	})
}

func TestBasicAuthenticator_BackendErrorsDoNotThrottle(t *testing.T) {
	store := newStubBasicStore()
	store.forcedErr = errors.New("postgres connection reset")

	auth, err := NewBasicAuthenticator(store, BasicOptions{
		UserMaxFailures: 2,
		IPMaxFailures:   2,
	})
	if err != nil {
		t.Fatalf("NewBasicAuthenticator: %v", err)
	}

	for range 5 {
		_, err := auth.AuthenticateBasic(t.Context(), "alice", "secret", "192.0.2.50")
		if err == nil || errors.Is(err, ErrAuthThrottled) {
			t.Fatalf("DB failure should surface error without throttling, got %v", err)
		}
	}
}

func TestBasicAuthenticator_BoundedDetachedThrottleKeys(t *testing.T) {
	// 1. Structural proof: throttle map keys and entries must be fixed-width [32]byte digests
	// with zero retained string/slice references to request credential buffers.
	mapKeyType := reflect.TypeOf((boundedThrottleMap{}).entries).Key()
	if mapKeyType.Kind() != reflect.Array || mapKeyType.Len() != 32 || mapKeyType.Elem().Kind() != reflect.Uint8 {
		t.Fatalf("throttle map key type = %v, want [32]uint8", mapKeyType)
	}
	entryType := reflect.TypeOf(throttleEntry{})
	for i := range entryType.NumField() {
		f := entryType.Field(i)
		if f.Type.Kind() == reflect.String || f.Type.Kind() == reflect.Slice {
			t.Fatalf("throttleEntry field %s has variable-length reference type %v; must stay fixed-width", f.Name, f.Type)
		}
	}

	// Verify log attribute helper detaches from large backing strings and bounds length.
	largeBuf := strings.Repeat("u", 64) + ":" + strings.Repeat("p", 128*1024)
	shortSub := largeBuf[:64]
	detachedShort := detachBoundedLogAttr(shortSub)
	if detachedShort != shortSub || unsafe.StringData(detachedShort) == unsafe.StringData(largeBuf) {
		t.Fatalf("detachBoundedLogAttr must copy short substrings off the large backing buffer")
	}
	detachedLong := detachBoundedLogAttr(largeBuf)
	if len(detachedLong) != maxThrottleLogAttrBytes || unsafe.StringData(detachedLong) == unsafe.StringData(largeBuf) {
		t.Fatalf("detachBoundedLogAttr long len = %d, want %d detached from backing buffer", len(detachedLong), maxThrottleLogAttrBytes)
	}

	store := newStubBasicStore()
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	currentTime := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}

	var verifyCalls atomic.Int32
	auth, err := NewBasicAuthenticator(store, BasicOptions{
		UserMaxFailures: 3,
		IPMaxFailures:   5,
		FailureWindow:   1 * time.Minute,
		BaseCooldown:    5 * time.Second,
		MaxCooldown:     20 * time.Second,
		ThrottleMaxKeys: 32,
		Now:             currentTime,
		VerifyFunc: func(password, encodedHash string) (bool, error) {
			verifyCalls.Add(1)
			return fastVerify(password, encodedHash)
		},
	})
	if err != nil {
		t.Fatalf("NewBasicAuthenticator: %v", err)
	}

	t.Run("large passwords with short usernames store fixed-width keys and stay bounded", func(t *testing.T) {
		largePassword := strings.Repeat("P", 128*1024)
		for i := range 64 {
			username := fmt.Sprintf("u-%03d", i)
			peer := fmt.Sprintf("[2001:db8::%x]:1234", i+1)
			res, authErr := auth.Authenticate(makeBasicRequest(username, largePassword, peer))
			if !res.Handled || !errors.Is(authErr, ErrInvalidCredentials) {
				t.Fatalf("attempt %d expected ErrInvalidCredentials, got res=%+v err=%v", i, res, authErr)
			}
		}

		if got := len(auth.userThrottle.entries); got != 32 {
			t.Fatalf("userThrottle entries = %d, want 32 (ThrottleMaxKeys bound)", got)
		}
		if got := len(auth.ipThrottle.entries); got != 32 {
			t.Fatalf("ipThrottle entries = %d, want 32 (ThrottleMaxKeys bound)", got)
		}
		// Latest entry must match the HMAC digest key of u-063.
		expectedKey := auth.userThrottle.keyFor("u-063")
		if _, ok := auth.userThrottle.entries[expectedKey]; !ok {
			t.Fatalf("expected fixed-width digest key for u-063 in userThrottle")
		}
	})

	t.Run("large usernames group across peers, throttle, recover, and evict", func(t *testing.T) {
		auth.userThrottle.entries = make(map[[32]byte]*throttleEntry)
		auth.userThrottle.lru.Init()
		auth.ipThrottle.entries = make(map[[32]byte]*throttleEntry)
		auth.ipThrottle.lru.Init()
		verifyCalls.Store(0)

		largeUser := strings.Repeat("x", 128*1024)
		scheme := HashSchemeArgon2id
		hash := "fast:right-secret"
		readerRole, _ := domain.BuiltInRoleByID(domain.RoleReaderID)
		store.setUser(
			domain.User{ID: "u-large", Username: largeUser},
			&domain.Credential{
				ID:         "c-large",
				UserID:     "u-large",
				Kind:       domain.CredentialKindPassword,
				SecretHash: &hash,
				HashScheme: &scheme,
			},
			[]domain.Role{readerRole},
		)

		// 3 failures for the same 128 KiB username across 3 rotating peers collapse into 1 digest entry.
		for i := range 3 {
			peer := fmt.Sprintf("[2001:db8:1::%x]:443", i+1)
			res, authErr := auth.Authenticate(makeBasicRequest(largeUser, "wrong-pass", peer))
			if !res.Handled || !errors.Is(authErr, ErrInvalidCredentials) {
				t.Fatalf("attempt %d expected ErrInvalidCredentials, got res=%+v err=%v", i+1, res, authErr)
			}
		}
		if got := len(auth.userThrottle.entries); got != 1 {
			t.Fatalf("userThrottle entries = %d, want 1 (same 128 KiB user grouped across peers)", got)
		}
		if _, ok := auth.userThrottle.entries[auth.userThrottle.keyFor(largeUser)]; !ok {
			t.Fatalf("expected fixed-width digest key for largeUser")
		}

		// 4th attempt from a fresh peer is throttled before invoking KDF.
		res, authErr := auth.Authenticate(makeBasicRequest(largeUser, "right-secret", "[2001:db8:1::99]:443"))
		if !res.Handled || !errors.Is(authErr, ErrAuthThrottled) {
			t.Fatalf("expected ErrAuthThrottled for largeUser, got res=%+v err=%v", res, authErr)
		}
		if verifyCalls.Load() != 3 {
			t.Fatalf("throttled attempt must not run KDF, verifyCalls = %d", verifyCalls.Load())
		}

		// Advancing past BaseCooldown restores access and resets the largeUser throttle entry.
		advance(6 * time.Second)
		res, authErr = auth.Authenticate(makeBasicRequest(largeUser, "right-secret", "[2001:db8:1::99]:443"))
		if authErr != nil || !res.Handled || res.Principal == nil {
			t.Fatalf("expected largeUser recovery after cooldown, got res=%+v err=%v", res, authErr)
		}
		if got := len(auth.userThrottle.entries); got != 0 {
			t.Fatalf("expected userThrottle entry cleared after valid login, got %d", got)
		}

		// Distinct 128 KiB usernames stay bounded by ThrottleMaxKeys=32.
		for i := range 40 {
			u := fmt.Sprintf("%s-%02d", strings.Repeat("y", 128*1024), i)
			peer := fmt.Sprintf("[2001:db8:2::%x]:443", i+1)
			_, _ = auth.Authenticate(makeBasicRequest(u, "bad", peer))
		}
		if got := len(auth.userThrottle.entries); got != 32 {
			t.Fatalf("userThrottle entries = %d, want 32 after large-username flood", got)
		}
	})
}
