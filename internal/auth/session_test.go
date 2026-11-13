package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

type memSessionStore struct {
	mu       sync.Mutex
	users    map[string]domain.User
	creds    map[string]domain.Credential
	roles    map[string][]domain.Role
	sessions map[string]domain.Session
}

func newMemSessionStore() *memSessionStore {
	return &memSessionStore{
		users:    make(map[string]domain.User),
		creds:    make(map[string]domain.Credential),
		roles:    make(map[string][]domain.Role),
		sessions: make(map[string]domain.Session),
	}
}

func (m *memSessionStore) addUserWithPassword(t *testing.T, password string, roles ...domain.Role) string {
	t.Helper()
	const (
		id       = "u1"
		username = "alice"
	)
	hash, err := HashPassword(password)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	now := time.Now().UTC()
	credID := "cred-" + id
	m.mu.Lock()
	defer m.mu.Unlock()
	m.users[id] = domain.User{ID: id, Username: username, CreatedAt: now, UpdatedAt: now}
	scheme := HashSchemeArgon2id
	m.creds[credID] = domain.Credential{
		ID:         credID,
		UserID:     id,
		Kind:       domain.CredentialKindPassword,
		SecretHash: &hash,
		HashScheme: &scheme,
		CreatedAt:  now,
	}
	m.roles[id] = append([]domain.Role(nil), roles...)
	return credID
}

func (m *memSessionStore) GetUserByUsername(_ context.Context, username string) (*domain.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, u := range m.users {
		if u.Username == username {
			copyU := u
			return &copyU, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (m *memSessionStore) GetActivePasswordCredentialByUserID(_ context.Context, userID string) (*domain.Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	for _, c := range m.creds {
		if c.UserID == userID && c.Kind == domain.CredentialKindPassword && !c.IsRevoked() && !c.IsExpired(now) {
			copyC := c
			return &copyC, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (m *memSessionStore) GetUserByID(_ context.Context, id string) (*domain.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.users[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	copyU := u
	return &copyU, nil
}

func (m *memSessionStore) GetCredentialByID(_ context.Context, id string) (*domain.Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.creds[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	copyC := c
	if c.SecretHash != nil {
		h := *c.SecretHash
		copyC.SecretHash = &h
	}
	if len(c.Scopes) > 0 {
		copyC.Scopes = append([]domain.PATScope(nil), c.Scopes...)
	}
	return &copyC, nil
}

func (m *memSessionStore) ListUserRoles(_ context.Context, userID string) ([]domain.Role, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]domain.Role(nil), m.roles[userID]...), nil
}

func (m *memSessionStore) CreateSession(_ context.Context, s *domain.Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := s.Validate(); err != nil {
		return err
	}
	cred, ok := m.creds[s.CredentialID]
	if !ok ||
		cred.UserID != s.UserID ||
		cred.IsRevoked() ||
		cred.IsExpired(s.CreatedAt) ||
		!domain.MatchesCredentialSecretDigest(cred.Kind, cred.SecretHash, s.CredentialHashDigest) {
		return storage.ErrCredentialChanged
	}
	for _, existing := range m.sessions {
		if existing.TokenHash == s.TokenHash {
			return storage.ErrAlreadyExists
		}
	}
	m.sessions[s.ID] = *s
	return nil
}

func (m *memSessionStore) rotateSecretHash(credID, newHash string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.creds[credID]
	if ok {
		c.SecretHash = &newHash
		m.creds[credID] = c
	}
	ts := at
	for id, s := range m.sessions {
		if s.CredentialID == credID && s.RevokedAt == nil {
			s.RevokedAt = &ts
			m.sessions[id] = s
		}
	}
}

func (m *memSessionStore) revokeCredential(credID string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.creds[credID]
	if ok {
		ts := at
		c.RevokedAt = &ts
		m.creds[credID] = c
	}
	ts := at
	for id, s := range m.sessions {
		if s.CredentialID == credID && s.RevokedAt == nil {
			s.RevokedAt = &ts
			m.sessions[id] = s
		}
	}
}

func (m *memSessionStore) activeSessionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, s := range m.sessions {
		if s.RevokedAt == nil {
			count++
		}
	}
	return count
}

func (m *memSessionStore) GetSessionByTokenHash(_ context.Context, tokenHash string) (*domain.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.sessions {
		if s.TokenHash == tokenHash {
			copyS := s
			return &copyS, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (m *memSessionStore) UpdateSessionLastUsed(_ context.Context, id string, lastUsedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return storage.ErrNotFound
	}
	ts := lastUsedAt
	s.LastUsedAt = &ts
	m.sessions[id] = s
	return nil
}

func (m *memSessionStore) RevokeSession(_ context.Context, id string, revokedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return storage.ErrNotFound
	}
	ts := revokedAt
	s.RevokedAt = &ts
	m.sessions[id] = s
	return nil
}

func (m *memSessionStore) RevokeSessionsByCredentialID(_ context.Context, credID string, revokedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts := revokedAt
	for id, s := range m.sessions {
		if s.CredentialID == credID && s.RevokedAt == nil {
			s.RevokedAt = &ts
			m.sessions[id] = s
		}
	}
	return nil
}

func (m *memSessionStore) DeleteSession(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[id]; !ok {
		return storage.ErrNotFound
	}
	delete(m.sessions, id)
	return nil
}

func (m *memSessionStore) DeleteSessionsByCredentialID(_ context.Context, credID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, s := range m.sessions {
		if s.CredentialID == credID {
			delete(m.sessions, id)
		}
	}
	return nil
}

func newTestSessionHarness(t *testing.T, nowFn func() time.Time) (*memSessionStore, *SessionService, *SessionAuthenticator) {
	t.Helper()
	store := newMemSessionStore()
	limiter, err := NewHashLimiter(HashLimiterOptions{MaxConcurrentHashes: 4, MaxHashQueue: 16})
	if err != nil {
		t.Fatalf("new limiter: %v", err)
	}
	basic, err := NewBasicAuthenticator(store, BasicOptions{
		Limiter:         limiter,
		IPMaxFailures:   3,
		UserMaxFailures: 3,
		FailureWindow:   time.Minute,
		BaseCooldown:    time.Minute,
		MaxCooldown:     time.Minute,
		Now:             nowFn,
	})
	if err != nil {
		t.Fatalf("new basic auth: %v", err)
	}
	svc, err := NewSessionService(basic, store, store, SessionOptions{
		TTL: time.Hour,
		Now: nowFn,
	})
	if err != nil {
		t.Fatalf("new session svc: %v", err)
	}
	auth, err := NewSessionAuthenticator(store, store, SessionOptions{
		Now:              nowFn,
		LastUsedInterval: -1,
	})
	if err != nil {
		t.Fatalf("new session auth: %v", err)
	}
	return store, svc, auth
}

func TestSessionLifecycleAndInvalidation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	store, svc, auth := newTestSessionHarness(t, func() time.Time { return now })

	adminRole := domain.Role{ID: "r-admin", Name: "ADMIN", Permissions: domain.AllPermissions()}
	readerRole := domain.Role{ID: "r-reader", Name: "READER", Permissions: []domain.Permission{domain.PermissionLibraryRead}}
	credID := store.addUserWithPassword(t, "correct-horse-battery", adminRole)

	res, err := svc.Login(context.Background(), app.LoginRequest{
		Username:   "alice",
		Password:   "correct-horse-battery",
		RemoteAddr: "192.0.2.10:12345",
	})
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	if res.SessionToken == "" || res.CSRFToken == "" {
		t.Fatalf("expected non-empty tokens, got session=%q csrf=%q", res.SessionToken, res.CSRFToken)
	}

	reqWithCookie := func(name, value string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
		if name != "" {
			r.AddCookie(&http.Cookie{Name: name, Value: value})
		}
		return r
	}

	// Valid cookie returns AuthSuccessCookie with Admin capabilities.
	authRes, err := auth.Authenticate(reqWithCookie(NativeSessionCookieName, res.SessionToken))
	if err != nil || !authRes.Handled || !authRes.RequiresCSRF || authRes.CSRFTokenHash == "" {
		t.Fatalf("unexpected auth result: %+v err=%v", authRes, err)
	}
	if !authRes.Principal.HasPermission(domain.PermissionUsersAdmin) {
		t.Fatalf("expected PermissionUsersAdmin on session principal, caps=%v", authRes.Principal.Permissions)
	}

	// Komga cookies must never authenticate via SessionAuthenticator.
	for _, komgaCookie := range []string{"KOMGA-SESSION", "komga-remember-me"} {
		passRes, err := auth.Authenticate(reqWithCookie(komgaCookie, res.SessionToken))
		if err != nil || passRes.Handled {
			t.Fatalf("expected Komga cookie %q to pass unhandled, got handled=%v err=%v", komgaCookie, passRes.Handled, err)
		}
	}

	// Duplicate or malformed native cookies must be rejected.
	dupReq := reqWithCookie(NativeSessionCookieName, res.SessionToken)
	dupReq.AddCookie(&http.Cookie{Name: NativeSessionCookieName, Value: res.SessionToken})
	if _, err := auth.Authenticate(dupReq); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials for duplicate cookie, got %v", err)
	}

	// Live role demotion takes effect immediately on the active session.
	store.mu.Lock()
	store.roles["u1"] = []domain.Role{readerRole}
	store.mu.Unlock()
	authRes, err = auth.Authenticate(reqWithCookie(NativeSessionCookieName, res.SessionToken))
	if err != nil {
		t.Fatalf("auth after role change failed: %v", err)
	}
	if authRes.Principal.HasPermission(domain.PermissionUsersAdmin) || !authRes.Principal.HasPermission(domain.PermissionLibraryRead) {
		t.Fatalf("expected live demotion to READER only, got caps=%v", authRes.Principal.Permissions)
	}

	// In-place password hash replacement permanently invalidates session in storage even if old hash is restored.
	newHash, err := HashPassword("rotated-password-999")
	if err != nil {
		t.Fatalf("hash replacement password: %v", err)
	}
	store.mu.Lock()
	origHash := *store.creds[credID].SecretHash
	store.mu.Unlock()
	store.rotateSecretHash(credID, newHash, now)
	if _, err := auth.Authenticate(reqWithCookie(NativeSessionCookieName, res.SessionToken)); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected invalidation on password hash replacement, got %v", err)
	}
	store.rotateSecretHash(credID, origHash, now)
	if _, err := auth.Authenticate(reqWithCookie(NativeSessionCookieName, res.SessionToken)); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected old session to stay permanently invalidated after restoring old hash, got %v", err)
	}

	// Mint a second session after rotation, test expiry on a third session, revocation, and deletion.
	res2, err := svc.Login(context.Background(), app.LoginRequest{
		Username:   "alice",
		Password:   "correct-horse-battery",
		RemoteAddr: "192.0.2.10:12346",
	})
	if err != nil {
		t.Fatalf("second login failed: %v", err)
	}
	store.revokeCredential(credID, now)
	if _, err := auth.Authenticate(reqWithCookie(NativeSessionCookieName, res2.SessionToken)); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected invalidation on credential revocation, got %v", err)
	}
	store.mu.Lock()
	c := store.creds[credID]
	c.RevokedAt = nil
	store.creds[credID] = c
	store.mu.Unlock()
	if _, err := auth.Authenticate(reqWithCookie(NativeSessionCookieName, res2.SessionToken)); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected revoked session to remain permanently invalid, got %v", err)
	}

	res3, err := svc.Login(context.Background(), app.LoginRequest{
		Username:   "alice",
		Password:   "correct-horse-battery",
		RemoteAddr: "192.0.2.10:12347",
	})
	if err != nil {
		t.Fatalf("third login failed: %v", err)
	}
	now = now.Add(2 * time.Hour)
	if _, err := auth.Authenticate(reqWithCookie(NativeSessionCookieName, res3.SessionToken)); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected invalidation after session TTL, got %v", err)
	}
	now = now.Add(-2 * time.Hour)

	res4, err := svc.Login(context.Background(), app.LoginRequest{
		Username:   "alice",
		Password:   "correct-horse-battery",
		RemoteAddr: "192.0.2.10:12348",
	})
	if err != nil {
		t.Fatalf("fourth login failed: %v", err)
	}
	// Deleting originating credential immediately invalidates session.
	store.mu.Lock()
	delete(store.creds, credID)
	store.mu.Unlock()
	if _, err := auth.Authenticate(reqWithCookie(NativeSessionCookieName, res4.SessionToken)); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected invalidation on credential deletion, got %v", err)
	}
}

func TestSessionRejectsNonNativeSessionKind(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	store, _, auth := newTestSessionHarness(t, func() time.Time { return now })
	readerRole := domain.Role{ID: "r-reader", Name: "READER", Permissions: []domain.Permission{domain.PermissionLibraryRead}}
	credID := store.addUserWithPassword(t, "secret", readerRole)

	store.mu.Lock()
	credHash := HashOpaqueToken(*store.creds[credID].SecretHash)
	store.mu.Unlock()

	rawToken := "komga-session-secret-token"
	err := store.CreateSession(context.Background(), &domain.Session{
		ID:                   "sess-komga-1",
		UserID:               "u1",
		CredentialID:         credID,
		Kind:                 domain.SessionKindKomga,
		TokenHash:            HashOpaqueToken(rawToken),
		CSRFTokenHash:        HashOpaqueToken("csrf"),
		CredentialHashDigest: credHash,
		CreatedAt:            now,
		ExpiresAt:            now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("create komga session: %v", err)
	}

	r := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	r.AddCookie(&http.Cookie{Name: NativeSessionCookieName, Value: rawToken})
	if _, err := auth.Authenticate(r); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected KOMGA session row to be rejected on native authenticator, got %v", err)
	}
}

func TestSessionLoginThrottleStablePeerIPAcrossPorts(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	store, svc, _ := newTestSessionHarness(t, func() time.Time { return now })
	readerRole := domain.Role{ID: "r-reader", Name: "READER", Permissions: []domain.Permission{domain.PermissionLibraryRead}}
	store.addUserWithPassword(t, "correct-password", readerRole)

	ports := []string{
		"198.51.100.25:40001",
		"198.51.100.25:40002",
		"[::ffff:198.51.100.25]:40003",
	}
	for i, remoteAddr := range ports {
		_, err := svc.Login(context.Background(), app.LoginRequest{
			Username:   "unknown-user-" + string(rune('a'+i)),
			Password:   "wrong",
			RemoteAddr: remoteAddr,
		})
		if !errors.Is(err, ErrInvalidCredentials) || !errors.Is(err, app.ErrUnauthorized) {
			t.Fatalf("attempt %d: expected ErrInvalidCredentials+ErrUnauthorized, got %v", i, err)
		}
	}

	// Fourth attempt from same peer IP with another port must hit peer-IP throttle even for a valid user.
	_, err := svc.Login(context.Background(), app.LoginRequest{
		Username:   "alice",
		Password:   "correct-password",
		RemoteAddr: "198.51.100.25:49999",
	})
	if !errors.Is(err, ErrAuthThrottled) || !errors.Is(err, app.ErrTooManyRequests) {
		t.Fatalf("expected ErrAuthThrottled+ErrTooManyRequests on 4th attempt across port changes, got %v", err)
	}
}

type barrierSessionRepo struct {
	storage.SessionRepository
	beforeCreate func()
}

func (b *barrierSessionRepo) CreateSession(ctx context.Context, s *domain.Session) error {
	if b.beforeCreate != nil {
		b.beforeCreate()
	}
	return b.SessionRepository.CreateSession(ctx, s)
}

func TestSessionLoginRace_CredentialRotationBarriers(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("rotation during password verification rejects login with zero active sessions", func(t *testing.T) {
		store := newMemSessionStore()
		adminRole := domain.Role{ID: "r-admin", Name: "Admin", Permissions: domain.AllPermissions()}
		credID := store.addUserWithPassword(t, "old-pass", adminRole)
		store.mu.Lock()
		oldHash := *store.creds[credID].SecretHash
		store.mu.Unlock()
		const newHash = "digest:new-pass"
		store.rotateSecretHash(credID, "digest:old-pass", now)
		// Ensure rotation helper doesn't leave stale sessions, update back cleanly:
		store.mu.Lock()
		c := store.creds[credID]
		c.SecretHash = &oldHash
		store.creds[credID] = c
		store.mu.Unlock()

		enteredVerify := make(chan struct{})
		proceedVerify := make(chan struct{})
		var once sync.Once

		limiter, err := NewHashLimiter(HashLimiterOptions{
			MaxConcurrentHashes: 2,
			MaxHashQueue:        8,
			VerifyFunc: func(secret, encodedHash string) (bool, error) {
				if encodedHash == oldHash && secret == "old-pass" {
					once.Do(func() {
						close(enteredVerify)
						<-proceedVerify
					})
					return true, nil
				}
				return encodedHash == "digest:"+secret, nil
			},
		})
		if err != nil {
			t.Fatalf("new limiter: %v", err)
		}
		basic, err := NewBasicAuthenticator(store, BasicOptions{
			Limiter:      limiter,
			DisableCache: true,
			Now:          func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("new basic: %v", err)
		}
		svc, err := NewSessionService(basic, store, store, SessionOptions{
			TTL: time.Hour,
			Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("new session svc: %v", err)
		}

		type loginOutcome struct {
			res *app.LoginResult
			err error
		}
		ch := make(chan loginOutcome, 1)
		go func() {
			r, lErr := svc.Login(context.Background(), app.LoginRequest{
				Username:   "alice",
				Password:   "old-pass",
				RemoteAddr: "198.51.100.10:1001",
			})
			ch <- loginOutcome{res: r, err: lErr}
		}()

		<-enteredVerify
		store.rotateSecretHash(credID, newHash, now)
		close(proceedVerify)

		out := <-ch
		if !errors.Is(out.err, app.ErrUnauthorized) || !errors.Is(out.err, ErrInvalidCredentials) || out.res != nil {
			t.Fatalf("expected raced login to fail with ErrUnauthorized/ErrInvalidCredentials and nil result, got res=%v err=%v", out.res, out.err)
		}
		if store.activeSessionCount() != 0 {
			t.Fatalf("expected 0 active sessions after raced login, got %d", store.activeSessionCount())
		}
		// Unchanged control with new password succeeds.
		ctrlRes, ctrlErr := svc.Login(context.Background(), app.LoginRequest{
			Username:   "alice",
			Password:   "new-pass",
			RemoteAddr: "198.51.100.10:1002",
		})
		if ctrlErr != nil || ctrlRes == nil {
			t.Fatalf("expected unchanged new-password login control to succeed, got res=%v err=%v", ctrlRes, ctrlErr)
		}
	})

	t.Run("rotation between credential check and CreateSession leaves zero active sessions", func(t *testing.T) {
		store := newMemSessionStore()
		adminRole := domain.Role{ID: "r-admin", Name: "Admin", Permissions: domain.AllPermissions()}
		credID := store.addUserWithPassword(t, "old-pass", adminRole)
		store.mu.Lock()
		oldHash := *store.creds[credID].SecretHash
		store.mu.Unlock()

		enteredCreate := make(chan struct{})
		proceedCreate := make(chan struct{})
		var once sync.Once
		barrierRepo := &barrierSessionRepo{
			SessionRepository: store,
			beforeCreate: func() {
				once.Do(func() {
					close(enteredCreate)
					<-proceedCreate
				})
			},
		}

		limiter, err := NewHashLimiter(HashLimiterOptions{
			MaxConcurrentHashes: 2,
			MaxHashQueue:        8,
			VerifyFunc: func(secret, encodedHash string) (bool, error) {
				return encodedHash == oldHash && secret == "old-pass", nil
			},
		})
		if err != nil {
			t.Fatalf("new limiter: %v", err)
		}
		basic, err := NewBasicAuthenticator(store, BasicOptions{
			Limiter:      limiter,
			DisableCache: true,
			Now:          func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("new basic: %v", err)
		}
		svc, err := NewSessionService(basic, store, barrierRepo, SessionOptions{
			TTL: time.Hour,
			Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("new session svc: %v", err)
		}

		ch := make(chan error, 1)
		go func() {
			_, lErr := svc.Login(context.Background(), app.LoginRequest{
				Username:   "alice",
				Password:   "old-pass",
				RemoteAddr: "198.51.100.11:1001",
			})
			ch <- lErr
		}()

		<-enteredCreate
		store.rotateSecretHash(credID, "digest:rotated", now)
		close(proceedCreate)

		lErr := <-ch
		if !errors.Is(lErr, app.ErrUnauthorized) || !errors.Is(lErr, ErrInvalidCredentials) {
			t.Fatalf("expected ErrUnauthorized/ErrInvalidCredentials when rotated before CreateSession, got %v", lErr)
		}
		if store.activeSessionCount() != 0 {
			t.Fatalf("expected 0 active sessions after CreateSession race, got %d", store.activeSessionCount())
		}
	})
}
