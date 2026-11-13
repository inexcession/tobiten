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

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

type stubPATStore struct {
	mu           sync.RWMutex
	users        map[string]domain.User
	creds        map[string]*domain.Credential
	roles        map[string][]domain.Role
	roleCalls    atomic.Int32
	lastUsedHits atomic.Int32
	forcedErr    error
	lastUsedErr  error
}

func newStubPATStore() *stubPATStore {
	return &stubPATStore{
		users: make(map[string]domain.User),
		creds: make(map[string]*domain.Credential),
		roles: make(map[string][]domain.Role),
	}
}

func (s *stubPATStore) setUserAndRoles(u domain.User, roles []domain.Role) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[u.ID] = u
	rCopy := make([]domain.Role, len(roles))
	copy(rCopy, roles)
	s.roles[u.ID] = rCopy
}

func (s *stubPATStore) setCredential(cred *domain.Credential) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cred == nil {
		return
	}
	cp := *cred
	if cred.Scopes != nil {
		cp.Scopes = make([]domain.PATScope, len(cred.Scopes))
		copy(cp.Scopes, cred.Scopes)
	}
	s.creds[cred.ID] = &cp
}

func (s *stubPATStore) GetCredentialByID(_ context.Context, id string) (*domain.Credential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.forcedErr != nil {
		return nil, s.forcedErr
	}
	c, ok := s.creds[id]
	if !ok || c == nil {
		return nil, storage.ErrNotFound
	}
	cp := *c
	if c.Scopes != nil {
		cp.Scopes = make([]domain.PATScope, len(c.Scopes))
		copy(cp.Scopes, c.Scopes)
	}
	return &cp, nil
}

func (s *stubPATStore) GetUserByID(_ context.Context, id string) (*domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.forcedErr != nil {
		return nil, s.forcedErr
	}
	u, ok := s.users[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return &u, nil
}

func (s *stubPATStore) ListUserRoles(_ context.Context, userID string) ([]domain.Role, error) {
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

func (s *stubPATStore) UpdateCredentialLastUsed(_ context.Context, id string, lastUsedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forcedErr != nil {
		return s.forcedErr
	}
	if s.lastUsedErr != nil {
		return s.lastUsedErr
	}
	c, ok := s.creds[id]
	if !ok || c == nil {
		return storage.ErrNotFound
	}
	ts := lastUsedAt.UTC()
	c.LastUsedAt = &ts
	s.lastUsedHits.Add(1)
	return nil
}

func (s *stubPATStore) getLastUsed(id string) *time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.creds[id]
	if !ok || c == nil || c.LastUsedAt == nil {
		return nil
	}
	ts := *c.LastUsedAt
	return &ts
}

func makePATBearerRequest(token, remoteAddr string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}

func makePATAPIKeyRequest(token, remoteAddr string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	req.Header.Set("X-API-Key", token)
	return req
}

func TestPATAuthenticator_TokenFormatAndHelpers(t *testing.T) {
	credID := domain.MustNewID()
	tok, hash, err := NewPAT(credID)
	if err != nil {
		t.Fatalf("NewPAT: %v", err)
	}
	if !strings.HasPrefix(tok, PATPrefix+credID+"_") {
		t.Fatalf("unexpected token prefix: %q", tok)
	}

	parsedID, parsedSecret, ok := ParsePAT(tok)
	if !ok {
		t.Fatalf("ParsePAT failed for generated token")
	}
	if parsedID != credID {
		t.Fatalf("parsedID = %q, want %q", parsedID, credID)
	}
	match, err := VerifyPassword(parsedSecret, hash)
	if err != nil || !match {
		t.Fatalf("VerifyPassword(parsedSecret, hash) = (%v, %v), want (true, nil)", match, err)
	}

	// Secret containing underscores must round-trip via first-underscore split.
	tokWithUnderscore, err := FormatPAT("019318a0-0000-7000-8000-000000000099", "abc_def_ghi")
	if err != nil {
		t.Fatalf("FormatPAT: %v", err)
	}
	id2, sec2, ok := ParsePAT(tokWithUnderscore)
	if !ok || id2 != "019318a0-0000-7000-8000-000000000099" || sec2 != "abc_def_ghi" {
		t.Fatalf("ParsePAT with underscores = (%q, %q, %v)", id2, sec2, ok)
	}

	invalidTokens := []string{
		"",
		"not_a_pat",
		"tb_pat_id_secret",
		"tobiten_pat_",
		"tobiten_pat_onlyid",
		"tobiten_pat__secret",
		"tobiten_pat_id_",
		"tobiten_pat_id\x00_secret",
		"tobiten_pat_id_sec\x00ret",
		"tobiten_pat_id with space_secret",
		"tobiten_pat_id_sec ret",
		"tobiten_pat_" + strings.Repeat("a", maxPATIDBytes+1) + "_secret",
	}
	for _, bad := range invalidTokens {
		if _, _, ok := ParsePAT(bad); ok {
			t.Errorf("ParsePAT(%q) unexpectedly succeeded", bad)
		}
	}

	if _, err := FormatPAT("has_underscore", "secret"); err == nil {
		t.Errorf("FormatPAT should reject credential ID containing underscore")
	}
	if _, err := FormatPAT("", "secret"); err == nil {
		t.Errorf("FormatPAT should reject empty credential ID")
	}
	if _, err := FormatPAT("cred-1", ""); err == nil {
		t.Errorf("FormatPAT should reject empty secret")
	}
}

func TestPATAuthenticator_HeaderParsingAndAuth(t *testing.T) {
	store := newStubPATStore()
	adminRole, _ := domain.BuiltInRoleByID(domain.RoleAdminID)
	store.setUserAndRoles(domain.User{ID: "u-1", Username: "alice"}, []domain.Role{adminRole})

	credID := "019318a0-0000-7000-8000-000000000101"
	token, hash, err := NewPAT(credID)
	if err != nil {
		t.Fatalf("NewPAT: %v", err)
	}
	scheme := HashSchemeArgon2id
	store.setCredential(&domain.Credential{
		ID:         credID,
		UserID:     "u-1",
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeRead},
		SecretHash: &hash,
		HashScheme: &scheme,
	})

	patAuth, err := NewPATAuthenticator(store, PATOptions{})
	if err != nil {
		t.Fatalf("NewPATAuthenticator: %v", err)
	}
	if patAuth.Name() != "pat" {
		t.Fatalf("Name() = %q, want pat", patAuth.Name())
	}

	t.Run("no auth headers passes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		res, err := patAuth.Authenticate(req)
		if err != nil || res.Handled || res.Principal != nil {
			t.Fatalf("expected AuthPass, got res=%+v err=%v", res, err)
		}
	})

	t.Run("basic scheme passes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:pass")))
		res, err := patAuth.Authenticate(req)
		if err != nil || res.Handled || res.Principal != nil {
			t.Fatalf("expected AuthPass for Basic header, got res=%+v err=%v", res, err)
		}
	})

	t.Run("valid Bearer PAT resolves scope-capped principal with real Argon2id", func(t *testing.T) {
		req := makePATBearerRequest(token, "10.0.0.1:1234")
		res, err := patAuth.Authenticate(req)
		if err != nil || !res.Handled || res.Principal == nil {
			t.Fatalf("Authenticate(Bearer) failed: res=%+v err=%v", res, err)
		}
		if res.Principal.User.ID != "u-1" || res.Principal.CredentialID != credID || res.Principal.CredentialKind != domain.CredentialKindPAT {
			t.Fatalf("unexpected principal identity: %+v", res.Principal)
		}
		if !res.Principal.HasPermission(domain.PermissionLibraryRead) || res.Principal.HasPermission(domain.PermissionUsersAdmin) {
			t.Fatalf("expected read-only capped permissions, got %v", res.Principal.Permissions)
		}
		if got := store.roleCalls.Load(); got != 1 {
			t.Errorf("expected single ListUserRoles query, got %d", got)
		}
		if store.getLastUsed(credID) == nil {
			t.Errorf("expected last_used_at to be updated on valid Bearer PAT")
		}
	})

	t.Run("lowercase bearer scheme accepted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		req.RemoteAddr = "10.0.0.2:1234"
		req.Header.Set("Authorization", "bearer "+token)
		res, err := patAuth.Authenticate(req)
		if err != nil || !res.Handled || res.Principal == nil {
			t.Fatalf("expected lowercase bearer success, got res=%+v err=%v", res, err)
		}
	})

	t.Run("valid X-API-Key PAT resolves principal", func(t *testing.T) {
		req := makePATAPIKeyRequest(token, "10.0.0.3:1234")
		res, err := patAuth.Authenticate(req)
		if err != nil || !res.Handled || res.Principal == nil {
			t.Fatalf("Authenticate(X-API-Key) failed: res=%+v err=%v", res, err)
		}
		if res.Principal.CredentialID != credID {
			t.Fatalf("unexpected credential ID: %q", res.Principal.CredentialID)
		}
	})

	t.Run("repeated identical Bearer and X-API-Key accepted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		req.RemoteAddr = "10.0.0.4:1234"
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-API-Key", token)
		res, err := patAuth.Authenticate(req)
		if err != nil || !res.Handled || res.Principal == nil {
			t.Fatalf("expected repeated identical PAT success, got res=%+v err=%v", res, err)
		}
	})

	malformedCases := []struct {
		name   string
		header func(*http.Request)
	}{
		{name: "empty bearer", header: func(r *http.Request) { r.Header.Set("Authorization", "Bearer") }},
		{name: "blank bearer", header: func(r *http.Request) { r.Header.Set("Authorization", "Bearer   ") }},
		{name: "non-pat bearer", header: func(r *http.Request) { r.Header.Set("Authorization", "Bearer not-a-pat") }},
		{name: "empty x-api-key", header: func(r *http.Request) { r.Header.Set("X-API-Key", "   ") }},
		{name: "non-pat x-api-key", header: func(r *http.Request) { r.Header.Set("X-API-Key", "tb_pat_bad_format") }},
		{
			name: "conflicting bearer and x-api-key",
			header: func(r *http.Request) {
				r.Header.Set("Authorization", "Bearer "+token)
				r.Header.Set("X-API-Key", "tobiten_pat_other_secret")
			},
		},
		{
			name: "conflicting multiple bearer headers",
			header: func(r *http.Request) {
				r.Header.Add("Authorization", "Bearer "+token)
				r.Header.Add("Authorization", "Bearer tobiten_pat_other_secret")
			},
		},
		{
			name: "conflicting multiple x-api-key headers",
			header: func(r *http.Request) {
				r.Header.Add("X-API-Key", token)
				r.Header.Add("X-API-Key", "tobiten_pat_other_secret")
			},
		},
	}
	for i, tc := range malformedCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
			req.RemoteAddr = fmt.Sprintf("10.0.1.%d:9000", i+1)
			tc.header(req)
			res, err := patAuth.Authenticate(req)
			if err == nil || !res.Handled || res.Principal != nil {
				t.Fatalf("expected Handled error for %s, got res=%+v err=%v", tc.name, res, err)
			}
		})
	}
}

func TestPATAuthenticator_ExpiryRevocationCacheAndRawChecks(t *testing.T) {
	store := newStubPATStore()

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
	fastVerify := func(secret, encodedHash string) (bool, error) {
		verifyCalls.Add(1)
		return encodedHash == "fast:"+secret, nil
	}

	adminRole, _ := domain.BuiltInRoleByID(domain.RoleAdminID)
	readerRole, _ := domain.BuiltInRoleByID(domain.RoleReaderID)
	store.setUserAndRoles(domain.User{ID: "u-1", Username: "alice"}, []domain.Role{adminRole})

	scheme := HashSchemeArgon2id
	hash1 := "fast:secret-1"
	exp := now.Add(10 * time.Minute)
	store.setCredential(&domain.Credential{
		ID:         "pat-1",
		UserID:     "u-1",
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeProgressWrite},
		SecretHash: &hash1,
		HashScheme: &scheme,
		ExpiresAt:  &exp,
	})

	patAuth, err := NewPATAuthenticator(store, PATOptions{
		CacheTTL:        5 * time.Minute,
		DisableThrottle: true,
		Now:             currentTime,
		VerifyFunc:      fastVerify,
	})
	if err != nil {
		t.Fatalf("NewPATAuthenticator: %v", err)
	}

	// 1. Valid PAT resolves, warms cache, and advances last_used_at.
	p, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_secret-1", "10.0.0.1")
	if err != nil || p == nil {
		t.Fatalf("expected valid PAT success, got p=%+v err=%v", p, err)
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("verifyCalls = %d, want 1", verifyCalls.Load())
	}
	firstUsed := store.getLastUsed("pat-1")
	if firstUsed == nil || !firstUsed.Equal(now) {
		t.Fatalf("first last_used_at = %v, want %v", firstUsed, now)
	}

	// 2. Second use hits cache (no KDF) but still advances last_used_at.
	advance(30 * time.Second)
	p, err = patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_secret-1", "10.0.0.1")
	if err != nil || p == nil {
		t.Fatalf("expected cached PAT success, got err=%v", err)
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("expected cache hit to skip KDF, verifyCalls = %d", verifyCalls.Load())
	}
	secondUsed := store.getLastUsed("pat-1")
	if secondUsed == nil || !secondUsed.After(*firstUsed) {
		t.Fatalf("expected last_used_at to advance from %v, got %v", firstUsed, secondUsed)
	}

	// 3. Live role downgrade reflects immediately even on cache hit.
	store.setUserAndRoles(domain.User{ID: "u-1", Username: "alice"}, []domain.Role{readerRole})
	store.setCredential(&domain.Credential{
		ID:         "pat-1",
		UserID:     "u-1",
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeAdmin},
		SecretHash: &hash1,
		HashScheme: &scheme,
		ExpiresAt:  &exp,
		LastUsedAt: secondUsed,
	})
	p, err = patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_secret-1", "10.0.0.1")
	if err != nil {
		t.Fatalf("AuthenticatePAT after role change: %v", err)
	}
	if p.HasPermission(domain.PermissionUsersAdmin) || !p.HasPermission(domain.PermissionProgressWrite) {
		t.Fatalf("expected Reader-capped permissions despite admin scope, got %v", p.Permissions)
	}

	// 4. SecretHash replacement invalidates cache immediately.
	hash2 := "fast:secret-2"
	store.setCredential(&domain.Credential{
		ID:         "pat-1",
		UserID:     "u-1",
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeRead},
		SecretHash: &hash2,
		HashScheme: &scheme,
		ExpiresAt:  &exp,
		LastUsedAt: secondUsed,
	})
	if _, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_secret-1", "10.0.0.1"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected old secret rejected after hash replacement, got %v", err)
	}
	if _, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_secret-2", "10.0.0.1"); err != nil {
		t.Fatalf("expected new secret accepted, got %v", err)
	}
	usedBeforeExpiry := store.getLastUsed("pat-1")

	// 5. Expiry rejects immediately even on warmed cache without updating last_used_at or invoking KDF.
	callsBeforeExpiry := verifyCalls.Load()
	advance(11 * time.Minute)
	if _, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_secret-2", "10.0.0.1"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected expired PAT rejected, got %v", err)
	}
	if verifyCalls.Load() != callsBeforeExpiry {
		t.Fatalf("expired PAT must not invoke KDF")
	}
	if !store.getLastUsed("pat-1").Equal(*usedBeforeExpiry) {
		t.Fatalf("expired PAT must not advance last_used_at")
	}

	// 6. Revocation rejects immediately even on warmed cache without updating last_used_at or invoking KDF.
	futureExp := currentTime().Add(1 * time.Hour)
	store.setCredential(&domain.Credential{
		ID:         "pat-1",
		UserID:     "u-1",
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeRead},
		SecretHash: &hash2,
		HashScheme: &scheme,
		ExpiresAt:  &futureExp,
		LastUsedAt: usedBeforeExpiry,
	})
	if _, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_secret-2", "10.0.0.1"); err != nil {
		t.Fatalf("expected re-warmed PAT accepted, got %v", err)
	}
	usedBeforeRevoke := store.getLastUsed("pat-1")
	revokedAt := currentTime()
	store.setCredential(&domain.Credential{
		ID:         "pat-1",
		UserID:     "u-1",
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeRead},
		SecretHash: &hash2,
		HashScheme: &scheme,
		ExpiresAt:  &futureExp,
		RevokedAt:  &revokedAt,
		LastUsedAt: usedBeforeRevoke,
	})
	callsBeforeRevoke := verifyCalls.Load()
	if _, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_secret-2", "10.0.0.1"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected revoked PAT rejected, got %v", err)
	}
	if verifyCalls.Load() != callsBeforeRevoke {
		t.Fatalf("revoked PAT must not invoke KDF")
	}
	if !store.getLastUsed("pat-1").Equal(*usedBeforeRevoke) {
		t.Fatalf("revoked PAT must not advance last_used_at")
	}

	// 7. Raw credential invariant rejections (wrong kind, empty scopes, invalid scope, bad hash scheme, nil hash).
	badCreds := []struct {
		name string
		cred *domain.Credential
	}{
		{
			name: "password credential kind rejected",
			cred: &domain.Credential{ID: "bad-1", UserID: "u-1", Kind: domain.CredentialKindPassword, SecretHash: &hash2, HashScheme: &scheme},
		},
		{
			name: "empty scopes rejected",
			cred: &domain.Credential{ID: "bad-2", UserID: "u-1", Kind: domain.CredentialKindPAT, Scopes: nil, SecretHash: &hash2, HashScheme: &scheme},
		},
		{
			name: "invalid scope rejected",
			cred: &domain.Credential{ID: "bad-3", UserID: "u-1", Kind: domain.CredentialKindPAT, Scopes: []domain.PATScope{"unknown:scope"}, SecretHash: &hash2, HashScheme: &scheme},
		},
		{
			name: "unsupported hash scheme rejected",
			cred: func() *domain.Credential {
				sha := "sha256"
				return &domain.Credential{ID: "bad-4", UserID: "u-1", Kind: domain.CredentialKindPAT, Scopes: []domain.PATScope{domain.ScopeRead}, SecretHash: &hash2, HashScheme: &sha}
			}(),
		},
		{
			name: "nil secret hash rejected",
			cred: &domain.Credential{ID: "bad-5", UserID: "u-1", Kind: domain.CredentialKindPAT, Scopes: []domain.PATScope{domain.ScopeRead}, SecretHash: nil, HashScheme: &scheme},
		},
	}
	for _, tc := range badCreds {
		t.Run(tc.name, func(t *testing.T) {
			store.setCredential(tc.cred)
			callsBefore := verifyCalls.Load()
			_, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_"+tc.cred.ID+"_secret-2", "10.0.0.1")
			if !errors.Is(err, ErrInvalidCredentials) {
				t.Fatalf("%s: expected ErrInvalidCredentials, got %v", tc.name, err)
			}
			if verifyCalls.Load() != callsBefore {
				t.Fatalf("%s: must not invoke KDF", tc.name)
			}
			if store.getLastUsed(tc.cred.ID) != nil {
				t.Fatalf("%s: must not update last_used_at", tc.name)
			}
		})
	}

	// 8. Missing credential runs dummy bounded KDF and rejects without updating last_used_at.
	callsBeforeMissing := verifyCalls.Load()
	if _, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_missing-id_secret-2", "10.0.0.1"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("missing credential: got %v, want ErrInvalidCredentials", err)
	}
	if verifyCalls.Load() != callsBeforeMissing+1 {
		t.Fatalf("missing credential expected dummy KDF call")
	}

	// 9. Missing user rejects without updating last_used_at.
	store.setCredential(&domain.Credential{
		ID:         "pat-orphan",
		UserID:     "u-missing",
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeRead},
		SecretHash: &hash2,
		HashScheme: &scheme,
	})
	if _, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-orphan_secret-2", "10.0.0.1"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("missing user: got %v, want ErrInvalidCredentials", err)
	}
	if store.getLastUsed("pat-orphan") != nil {
		t.Fatalf("missing user must not update last_used_at")
	}
}

func TestPATAuthenticator_BoundedVerificationSharedLimiterAndThrottling(t *testing.T) {
	t.Run("shared HashLimiter bounds aggregate concurrent KDF across Basic and PAT", func(t *testing.T) {
		var active, maxActive atomic.Int32
		gate := make(chan struct{})

		limiter, err := NewHashLimiter(HashLimiterOptions{
			MaxConcurrentHashes: 2,
			MaxHashQueue:        2,
			HashWaitTimeout:     2 * time.Second,
			VerifyFunc: func(secret, encodedHash string) (bool, error) {
				cur := active.Add(1)
				for {
					prev := maxActive.Load()
					if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
						break
					}
				}
				<-gate
				active.Add(-1)
				return encodedHash == "fast:"+secret, nil
			},
		})
		if err != nil {
			t.Fatalf("NewHashLimiter: %v", err)
		}

		basicStore := newStubBasicStore()
		patStore := newStubPATStore()
		scheme := HashSchemeArgon2id
		h := "fast:good"
		readerRole, _ := domain.BuiltInRoleByID(domain.RoleReaderID)

		basicStore.setUser(domain.User{ID: "u-1", Username: "alice"}, &domain.Credential{
			ID: "c-basic", UserID: "u-1", Kind: domain.CredentialKindPassword, SecretHash: &h, HashScheme: &scheme,
		}, []domain.Role{readerRole})
		patStore.setUserAndRoles(domain.User{ID: "u-1", Username: "alice"}, []domain.Role{readerRole})
		patStore.setCredential(&domain.Credential{
			ID: "c-pat", UserID: "u-1", Kind: domain.CredentialKindPAT, Scopes: []domain.PATScope{domain.ScopeRead}, SecretHash: &h, HashScheme: &scheme,
		})

		basicAuth, err := NewBasicAuthenticator(basicStore, BasicOptions{
			DisableCache:    true,
			DisableThrottle: true,
			Limiter:         limiter,
		})
		if err != nil {
			t.Fatalf("NewBasicAuthenticator: %v", err)
		}
		patAuth, err := NewPATAuthenticator(patStore, PATOptions{
			DisableCache:    true,
			DisableThrottle: true,
			Limiter:         limiter,
		})
		if err != nil {
			t.Fatalf("NewPATAuthenticator: %v", err)
		}

		const total = 10
		var wg sync.WaitGroup
		var busyCount, badCount atomic.Int32
		for i := range total {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				var authErr error
				if idx%2 == 0 {
					_, authErr = basicAuth.AuthenticateBasic(t.Context(), "alice", fmt.Sprintf("wrong-%d", idx), fmt.Sprintf("10.1.0.%d", idx+1))
				} else {
					_, authErr = patAuth.AuthenticatePAT(t.Context(), fmt.Sprintf("tobiten_pat_c-pat_wrong-%d", idx), fmt.Sprintf("10.1.0.%d", idx+1))
				}
				switch {
				case errors.Is(authErr, ErrAuthBusy):
					busyCount.Add(1)
				case errors.Is(authErr, ErrInvalidCredentials):
					badCount.Add(1)
				default:
					t.Errorf("unexpected error: %v", authErr)
				}
			}(i)
		}

		deadline := time.Now().Add(2 * time.Second)
		for busyCount.Load() < 6 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		close(gate)
		wg.Wait()

		if peak := maxActive.Load(); peak > 2 {
			t.Fatalf("aggregate peak concurrent hashes = %d, want <= 2", peak)
		}
		if busyCount.Load() == 0 {
			t.Fatalf("expected excess requests to fail fast with ErrAuthBusy")
		}
		if busyCount.Load()+badCount.Load() != total {
			t.Fatalf("unexpected outcome counts: busy=%d bad=%d", busyCount.Load(), badCount.Load())
		}
	})

	t.Run("throttling, cooldown recovery, reset on valid use, and backend error non-throttling", func(t *testing.T) {
		store := newStubPATStore()
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

		readerRole, _ := domain.BuiltInRoleByID(domain.RoleReaderID)
		store.setUserAndRoles(domain.User{ID: "u-1", Username: "alice"}, []domain.Role{readerRole})
		scheme := HashSchemeArgon2id
		hash := "fast:good-secret"
		store.setCredential(&domain.Credential{
			ID:         "pat-1",
			UserID:     "u-1",
			Kind:       domain.CredentialKindPAT,
			Scopes:     []domain.PATScope{domain.ScopeRead},
			SecretHash: &hash,
			HashScheme: &scheme,
		})

		var verifyCalls atomic.Int32
		patAuth, err := NewPATAuthenticator(store, PATOptions{
			CredMaxFailures: 3,
			IPMaxFailures:   5,
			FailureWindow:   1 * time.Minute,
			BaseCooldown:    5 * time.Second,
			MaxCooldown:     20 * time.Second,
			Now:             currentTime,
			VerifyFunc: func(secret, encodedHash string) (bool, error) {
				verifyCalls.Add(1)
				return encodedHash == "fast:"+secret, nil
			},
		})
		if err != nil {
			t.Fatalf("NewPATAuthenticator: %v", err)
		}

		for i := range 3 {
			_, authErr := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_wrong", fmt.Sprintf("10.2.0.%d", i+1))
			if !errors.Is(authErr, ErrInvalidCredentials) {
				t.Fatalf("failure %d got %v, want ErrInvalidCredentials", i+1, authErr)
			}
		}

		// 4th request (even with valid secret from a different peer) is throttled without KDF or last_used_at update.
		callsBefore := verifyCalls.Load()
		if _, authErr := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_good-secret", "10.2.0.99"); !errors.Is(authErr, ErrAuthThrottled) {
			t.Fatalf("expected ErrAuthThrottled, got %v", authErr)
		}
		if verifyCalls.Load() != callsBefore {
			t.Fatalf("throttled request must not invoke KDF")
		}
		if store.getLastUsed("pat-1") != nil {
			t.Fatalf("throttled request must not update last_used_at")
		}

		// Advance past BaseCooldown -> valid token succeeds and resets cred throttle.
		advance(6 * time.Second)
		p, authErr := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_good-secret", "10.2.0.99")
		if authErr != nil || p == nil {
			t.Fatalf("expected recovery after cooldown, got p=%+v err=%v", p, authErr)
		}
		if got := len(patAuth.credThrottle.entries); got != 0 {
			t.Fatalf("expected credThrottle cleared after valid auth, got %d", got)
		}

		// Backend DB errors must never throttle.
		store.forcedErr = errors.New("db down")
		for range 6 {
			_, err := patAuth.AuthenticatePAT(t.Context(), "tobiten_pat_pat-1_good-secret", "10.3.0.1")
			if err == nil || errors.Is(err, ErrAuthThrottled) {
				t.Fatalf("backend error should propagate without throttling, got %v", err)
			}
		}
	})
}

func TestPATAuthenticator_BoundedDetachedRetainedState(t *testing.T) {
	// Structural fixed-width proof: cache and throttle maps store only [32]byte digests.
	for _, typ := range []reflect.Type{
		reflect.TypeOf((boundedThrottleMap{}).entries).Key(),
		reflect.TypeOf((verificationCache{}).entries).Key(),
	} {
		if typ.Kind() != reflect.Array || typ.Len() != 32 || typ.Elem().Kind() != reflect.Uint8 {
			t.Fatalf("retained map key type = %v, want [32]uint8", typ)
		}
	}
	for _, structTyp := range []reflect.Type{
		reflect.TypeOf(throttleEntry{}),
		reflect.TypeOf(cacheEntry{}),
	} {
		for i := range structTyp.NumField() {
			f := structTyp.Field(i)
			if f.Type.Kind() == reflect.String || f.Type.Kind() == reflect.Slice {
				t.Fatalf("%s.%s has variable-length reference type %v; must stay fixed-width", structTyp.Name(), f.Name, f.Type)
			}
		}
	}

	store := newStubPATStore()
	patAuth, err := NewPATAuthenticator(store, PATOptions{
		ThrottleMaxKeys: 16,
		CacheMaxEntries: 16,
		VerifyFunc: func(secret, encodedHash string) (bool, error) {
			return encodedHash == "fast:"+secret, nil
		},
	})
	if err != nil {
		t.Fatalf("NewPATAuthenticator: %v", err)
	}

	// Oversized 128 KiB secret across 40 distinct credential IDs must stay bounded to ThrottleMaxKeys=16.
	largeSecret := strings.Repeat("S", 128*1024)
	for i := range 40 {
		credID := fmt.Sprintf("pat-flood-%02d", i)
		peer := fmt.Sprintf("[2001:db8:3::%x]:443", i+1)
		res, authErr := patAuth.Authenticate(makePATBearerRequest(PATPrefix+credID+"_"+largeSecret, peer))
		if !res.Handled || !errors.Is(authErr, ErrInvalidCredentials) {
			t.Fatalf("attempt %d expected ErrInvalidCredentials, got res=%+v err=%v", i, res, authErr)
		}
	}
	if got := len(patAuth.credThrottle.entries); got != 16 {
		t.Fatalf("credThrottle entries = %d, want 16", got)
	}
	if got := len(patAuth.ipThrottle.entries); got != 16 {
		t.Fatalf("ipThrottle entries = %d, want 16", got)
	}

	// Verify detachBoundedLogAttr clones and bounds large token substrings.
	fullTok := PATPrefix + "pat-1_" + largeSecret
	subID := fullTok[len(PATPrefix) : len(PATPrefix)+5]
	detached := detachBoundedLogAttr(subID)
	if detached != subID || unsafe.StringData(detached) == unsafe.StringData(fullTok) {
		t.Fatalf("detachBoundedLogAttr must detach from large token backing buffer")
	}
}

func TestPATAuthenticator_WarmedCacheRoleDemotionIsolated(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	currentTime := func() time.Time { return now }

	var verifyCalls atomic.Int32
	fastVerify := func(secret, encodedHash string) (bool, error) {
		verifyCalls.Add(1)
		return encodedHash == "fast:"+secret, nil
	}

	curatorRole, ok := domain.BuiltInRoleByID(domain.RoleCuratorID)
	if !ok {
		t.Fatal("RoleCuratorID not found")
	}
	readerRole, ok := domain.BuiltInRoleByID(domain.RoleReaderID)
	if !ok {
		t.Fatal("RoleReaderID not found")
	}
	restrictedRole, ok := domain.BuiltInRoleByID(domain.RoleRestrictedReaderID)
	if !ok {
		t.Fatal("RoleRestrictedReaderID not found")
	}

	user := domain.User{ID: "u-demote", Username: "demote-target"}
	store := newStubPATStore()
	store.setUserAndRoles(user, []domain.Role{curatorRole})

	// Credential ID, presented token, SecretHash, and Scopes remain constant throughout.
	const (
		credID         = "pat-demote-1"
		presentedToken = "tobiten_pat_" + credID + "_secret-constant"
	)
	scheme := HashSchemeArgon2id
	hash := "fast:secret-constant"
	exp := now.Add(30 * time.Minute)
	store.setCredential(&domain.Credential{
		ID:         credID,
		UserID:     user.ID,
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeAdmin},
		SecretHash: &hash,
		HashScheme: &scheme,
		ExpiresAt:  &exp,
	})

	patAuth, err := NewPATAuthenticator(store, PATOptions{
		CacheTTL:        5 * time.Minute,
		DisableThrottle: true,
		Now:             currentTime,
		VerifyFunc:      fastVerify,
	})
	if err != nil {
		t.Fatalf("NewPATAuthenticator: %v", err)
	}

	// 1. Warm the PAT cache while user holds Curator role (admin PAT is capped by Curator perms).
	p1, err := patAuth.AuthenticatePAT(t.Context(), presentedToken, "10.0.0.10")
	if err != nil || p1 == nil {
		t.Fatalf("initial Curator AuthenticatePAT: p=%+v err=%v", p1, err)
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("verifyCalls after warm = %d, want 1", verifyCalls.Load())
	}
	if p1.HasPermission(domain.PermissionLibraryAdmin) {
		t.Fatal("Curator's admin-scoped PAT must still be capped below library:admin")
	}

	metadataMutations := 0
	progressMutations := 0
	if err := app.Authorize(p1, domain.PermissionMetadataEdit, func() error {
		metadataMutations++
		return nil
	}); err != nil {
		t.Fatalf("Curator metadata:edit = %v, want nil", err)
	}
	if err := app.Authorize(p1, domain.PermissionProgressWrite, func() error {
		progressMutations++
		return nil
	}); err != nil {
		t.Fatalf("Curator progress:write = %v, want nil", err)
	}
	if metadataMutations != 1 || progressMutations != 1 {
		t.Fatalf("mutation counts = (%d, %d), want (1, 1)", metadataMutations, progressMutations)
	}

	// 2. Advance clock within CacheTTL and demote ONLY role grants (Curator -> Reader).
	now = now.Add(1 * time.Minute)
	store.setUserAndRoles(user, []domain.Role{readerRole})

	p2, err := patAuth.AuthenticatePAT(t.Context(), presentedToken, "10.0.0.10")
	if err != nil || p2 == nil {
		t.Fatalf("Reader AuthenticatePAT: p=%+v err=%v", p2, err)
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("expected warm PAT cache hit after role demotion, verifyCalls = %d", verifyCalls.Load())
	}
	if p2.CredentialID != credID || p2.RequiresAllowLabels() {
		t.Fatalf("unexpected principal metadata after Reader demotion: %+v", p2)
	}
	if err := app.Authorize(p2, domain.PermissionMetadataEdit, func() error {
		metadataMutations++
		return nil
	}); !errors.Is(err, app.ErrForbidden) {
		t.Fatalf("Reader metadata:edit = %v, want ErrForbidden", err)
	}
	if metadataMutations != 1 {
		t.Fatalf("denied metadata callback ran after demotion: metadataMutations=%d, want 1", metadataMutations)
	}
	if err := app.Authorize(p2, domain.PermissionProgressWrite, func() error {
		progressMutations++
		return nil
	}); err != nil {
		t.Fatalf("Reader progress:write = %v, want nil", err)
	}

	// 3. Still within CacheTTL, demote ONLY role grants (Reader -> Restricted reader).
	now = now.Add(1 * time.Minute)
	store.setUserAndRoles(user, []domain.Role{restrictedRole})

	p3, err := patAuth.AuthenticatePAT(t.Context(), presentedToken, "10.0.0.10")
	if err != nil || p3 == nil {
		t.Fatalf("Restricted reader AuthenticatePAT: p=%+v err=%v", p3, err)
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("expected warm PAT cache hit on Restricted reader demotion, verifyCalls = %d", verifyCalls.Load())
	}
	if !p3.RequiresAllowLabels() {
		t.Fatal("Restricted reader principal must expose RequiresAllowLabels() == true (I-5)")
	}
	if err := app.Authorize(p3, domain.PermissionMetadataEdit, func() error {
		metadataMutations++
		return nil
	}); !errors.Is(err, app.ErrForbidden) || metadataMutations != 1 {
		t.Fatalf("Restricted reader metadata:edit err=%v mutations=%d, want ErrForbidden and 1", err, metadataMutations)
	}

	// 4. Still within CacheTTL, remove all role grants; read & progress write are also denied.
	now = now.Add(1 * time.Minute)
	store.setUserAndRoles(user, nil)

	p4, err := patAuth.AuthenticatePAT(t.Context(), presentedToken, "10.0.0.10")
	if err != nil || p4 == nil {
		t.Fatalf("Zero-role AuthenticatePAT: p=%+v err=%v", p4, err)
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("expected warm PAT cache hit on zero-role demotion, verifyCalls = %d", verifyCalls.Load())
	}
	if err := app.Authorize(p4, domain.PermissionProgressWrite, func() error {
		progressMutations++
		return nil
	}); !errors.Is(err, app.ErrForbidden) {
		t.Fatalf("Zero-role progress:write = %v, want ErrForbidden", err)
	}
	if err := app.Authorize(p4, domain.PermissionLibraryRead, func() error {
		progressMutations++
		return nil
	}); !errors.Is(err, app.ErrForbidden) {
		t.Fatalf("Zero-role library:read = %v, want ErrForbidden", err)
	}
	if progressMutations != 2 {
		t.Fatalf("progressMutations = %d, want 2 (no callback after zero-role demotion)", progressMutations)
	}
}
