package auth

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
)

type stubXAuthTokenAuthenticator struct {
	tokens map[string]*domain.Principal
}

func (a *stubXAuthTokenAuthenticator) Name() string { return "x-auth-token-stub" }

func (a *stubXAuthTokenAuthenticator) Tier() module.CredentialTier {
	return module.CredentialTierExplicit
}

func (a *stubXAuthTokenAuthenticator) Authenticate(r *http.Request) (module.AuthResult, error) {
	vals := r.Header.Values("X-Auth-Token")
	if len(vals) == 0 {
		return module.AuthPass(), nil
	}
	first := strings.TrimSpace(vals[0])
	if first == "" {
		return module.AuthResult{Handled: true}, ErrInvalidCredentials
	}
	for _, v := range vals[1:] {
		if strings.TrimSpace(v) != first {
			return module.AuthResult{Handled: true}, ErrInvalidCredentials
		}
	}
	p, ok := a.tokens[first]
	if !ok || p == nil {
		return module.AuthResult{Handled: true}, ErrInvalidCredentials
	}
	return module.AuthSuccess(p), nil
}

type stubTrustedProxyAuthenticator struct {
	trustedPrefix netip.Prefix
	principals    map[string]*domain.Principal
}

func (a *stubTrustedProxyAuthenticator) Name() string { return "trusted-proxy-stub" }

func (a *stubTrustedProxyAuthenticator) Tier() module.CredentialTier {
	return module.CredentialTierTrustedProxy
}

func (a *stubTrustedProxyAuthenticator) Authenticate(r *http.Request) (module.AuthResult, error) {
	ipStr := extractClientIP(r.RemoteAddr)
	addr, _ := netip.ParseAddr(ipStr)
	if !addr.IsValid() || !a.trustedPrefix.Contains(addr) {
		return module.AuthPass(), nil
	}

	remoteUser := strings.TrimSpace(r.Header.Get("Remote-User"))
	fwdUser := strings.TrimSpace(r.Header.Get("X-Forwarded-User"))
	if remoteUser == "" && fwdUser == "" {
		return module.AuthPass(), nil
	}
	if remoteUser != "" && fwdUser != "" && remoteUser != fwdUser {
		return module.AuthResult{Handled: true}, ErrInvalidCredentials
	}
	user := remoteUser
	if user == "" {
		user = fwdUser
	}
	p, ok := a.principals[user]
	if !ok || p == nil {
		return module.AuthResult{Handled: true}, ErrInvalidCredentials
	}
	return module.AuthSuccess(p), nil
}

type stubCookieAuthenticator struct {
	sessions map[string]*domain.Principal
}

func (a *stubCookieAuthenticator) Name() string { return "cookie-stub" }

func (a *stubCookieAuthenticator) Tier() module.CredentialTier {
	return module.CredentialTierCookie
}

func (a *stubCookieAuthenticator) Authenticate(r *http.Request) (module.AuthResult, error) {
	c, err := r.Cookie("tobiten_session")
	if err != nil {
		if errors.Is(err, http.ErrNoCookie) {
			return module.AuthPass(), nil
		}
		return module.AuthResult{Handled: true}, err
	}
	p, ok := a.sessions[c.Value]
	if !ok || p == nil {
		return module.AuthResult{Handled: true}, ErrInvalidCredentials
	}
	return module.AuthSuccess(p), nil
}

type precedenceFixture struct {
	basicAuth  *BasicAuthenticator
	patAuth    *PATAuthenticator
	xAuthStub  *stubXAuthTokenAuthenticator
	proxyStub  *stubTrustedProxyAuthenticator
	cookieStub *stubCookieAuthenticator
	patStore   *stubPATStore
	kdfCalls   *atomic.Int32

	alice          domain.User
	bob            domain.User
	basicCredID    string
	patReadID      string
	patReadToken   string
	patProgID      string
	patProgToken   string
	xAuthCredID    string
	proxyCredID    string
	cookieCredID   string
	basicPass      string
	xAuthToken     string
	cookieToken    string
	trustedIP      string
	untrustedIP    string
	basicPrincipal *domain.Principal
	patReadPrinc   *domain.Principal
	patProgPrinc   *domain.Principal
	xAuthPrincipal *domain.Principal
	proxyPrincipal *domain.Principal
	cookiePrinc    *domain.Principal
}

func newPrecedenceFixture(t *testing.T) *precedenceFixture {
	t.Helper()

	adminRole, _ := domain.BuiltInRoleByID(domain.RoleAdminID)
	curatorRole, _ := domain.BuiltInRoleByID(domain.RoleCuratorID)

	alice := domain.User{ID: "usr-alice", Username: "alice"}
	bob := domain.User{ID: "usr-bob", Username: "bob"}

	var kdfCalls atomic.Int32
	verifyFn := func(secret, encodedHash string) (bool, error) {
		kdfCalls.Add(1)
		return encodedHash == "fast:"+secret, nil
	}
	limiter, err := NewHashLimiter(HashLimiterOptions{
		MaxConcurrentHashes: 4,
		MaxHashQueue:        16,
		VerifyFunc:          verifyFn,
	})
	if err != nil {
		t.Fatalf("NewHashLimiter: %v", err)
	}

	scheme := HashSchemeArgon2id
	basicPass := "alice-password-secret"
	basicHash := "fast:" + basicPass
	basicCredID := "cred-basic-alice"
	basicStore := newStubBasicStore()
	basicStore.setUser(alice, &domain.Credential{
		ID:         basicCredID,
		UserID:     alice.ID,
		Kind:       domain.CredentialKindPassword,
		SecretHash: &basicHash,
		HashScheme: &scheme,
	}, []domain.Role{adminRole})

	basicAuth, err := NewBasicAuthenticator(basicStore, BasicOptions{
		DisableThrottle: true,
		Limiter:         limiter,
	})
	if err != nil {
		t.Fatalf("NewBasicAuthenticator: %v", err)
	}

	patStore := newStubPATStore()
	patStore.setUserAndRoles(alice, []domain.Role{adminRole})

	patReadID := "pat-read-alice"
	patReadSecret := "read-secret-abc"
	patReadToken, err := FormatPAT(patReadID, patReadSecret)
	if err != nil {
		t.Fatalf("FormatPAT(read): %v", err)
	}
	patReadHash := "fast:" + patReadSecret
	patStore.setCredential(&domain.Credential{
		ID:         patReadID,
		UserID:     alice.ID,
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeRead},
		SecretHash: &patReadHash,
		HashScheme: &scheme,
	})

	patProgID := "pat-prog-alice"
	patProgSecret := "prog-secret-xyz"
	patProgToken, err := FormatPAT(patProgID, patProgSecret)
	if err != nil {
		t.Fatalf("FormatPAT(prog): %v", err)
	}
	patProgHash := "fast:" + patProgSecret
	patStore.setCredential(&domain.Credential{
		ID:         patProgID,
		UserID:     alice.ID,
		Kind:       domain.CredentialKindPAT,
		Scopes:     []domain.PATScope{domain.ScopeProgressWrite},
		SecretHash: &patProgHash,
		HashScheme: &scheme,
	})

	patAuth, err := NewPATAuthenticator(patStore, PATOptions{
		DisableThrottle: true,
		Limiter:         limiter,
	})
	if err != nil {
		t.Fatalf("NewPATAuthenticator: %v", err)
	}

	xAuthCredID := "cred-xauth-bob"
	xAuthPrincipal := domain.NewPrincipal(bob, []domain.Role{curatorRole}, domain.CredentialKindPassword, xAuthCredID, nil)
	xAuthStub := &stubXAuthTokenAuthenticator{
		tokens: map[string]*domain.Principal{
			"valid-xauth-bob": xAuthPrincipal,
		},
	}

	proxyCredID := "cred-proxy-bob"
	proxyPrincipal := domain.NewPrincipal(bob, []domain.Role{curatorRole}, domain.CredentialKindOIDCLink, proxyCredID, nil)
	proxyStub := &stubTrustedProxyAuthenticator{
		trustedPrefix: netip.MustParsePrefix("10.10.0.0/16"),
		principals: map[string]*domain.Principal{
			"bob": proxyPrincipal,
		},
	}

	cookieCredID := "cred-cookie-alice"
	cookiePrincipal := domain.NewPrincipal(alice, []domain.Role{adminRole}, domain.CredentialKindPassword, cookieCredID, nil)
	cookieStub := &stubCookieAuthenticator{
		sessions: map[string]*domain.Principal{
			"valid-cookie-alice": cookiePrincipal,
		},
	}

	return &precedenceFixture{
		basicAuth:      basicAuth,
		patAuth:        patAuth,
		xAuthStub:      xAuthStub,
		proxyStub:      proxyStub,
		cookieStub:     cookieStub,
		patStore:       patStore,
		kdfCalls:       &kdfCalls,
		alice:          alice,
		bob:            bob,
		basicCredID:    basicCredID,
		patReadID:      patReadID,
		patReadToken:   patReadToken,
		patProgID:      patProgID,
		patProgToken:   patProgToken,
		xAuthCredID:    xAuthCredID,
		proxyCredID:    proxyCredID,
		cookieCredID:   cookieCredID,
		basicPass:      basicPass,
		xAuthToken:     "valid-xauth-bob",
		cookieToken:    "valid-cookie-alice",
		trustedIP:      "10.10.1.5:4321",
		untrustedIP:    "198.51.100.9:4321",
		basicPrincipal: domain.NewPrincipal(alice, []domain.Role{adminRole}, domain.CredentialKindPassword, basicCredID, nil),
		patReadPrinc:   domain.NewPrincipal(alice, []domain.Role{adminRole}, domain.CredentialKindPAT, patReadID, []domain.PATScope{domain.ScopeRead}),
		patProgPrinc:   domain.NewPrincipal(alice, []domain.Role{adminRole}, domain.CredentialKindPAT, patProgID, []domain.PATScope{domain.ScopeProgressWrite}),
		xAuthPrincipal: xAuthPrincipal,
		proxyPrincipal: proxyPrincipal,
		cookiePrinc:    cookiePrincipal,
	}
}

func (f *precedenceFixture) protectedHandler(auths []module.Authenticator) http.Handler {
	target := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := domain.MustPrincipalFromContext(r.Context())
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p)
	})
	return middleware.Auth(middleware.AuthOptions{
		Authenticators: auths,
	})(target)
}

func TestCredentialPrecedence_SourcePairMatrix(t *testing.T) {
	f := newPrecedenceFixture(t)

	type sourceID string
	const (
		srcAPIKey     sourceID = "api_key"
		srcBearer     sourceID = "bearer"
		srcBasic      sourceID = "basic"
		srcXAuthToken sourceID = "x_auth_token"
		srcProxy      sourceID = "proxy"
		srcCookie     sourceID = "cookie"
	)

	type sourceSpec struct {
		id           sourceID
		tier         module.CredentialTier
		applyValid   func(r *http.Request)
		applyInvalid func(r *http.Request)
		wantCredID   string
		wantUserID   string
		wantPerms    []domain.Permission
	}

	sources := []sourceSpec{
		{
			id:   srcAPIKey,
			tier: module.CredentialTierExplicit,
			applyValid: func(r *http.Request) {
				r.Header.Add("X-API-Key", f.patReadToken)
			},
			applyInvalid: func(r *http.Request) {
				r.Header.Add("X-API-Key", "tobiten_pat_"+f.patReadID+"_wrong-secret")
			},
			wantCredID: f.patReadID,
			wantUserID: f.alice.ID,
			wantPerms:  []domain.Permission{domain.PermissionLibraryRead},
		},
		{
			id:   srcBearer,
			tier: module.CredentialTierExplicit,
			applyValid: func(r *http.Request) {
				r.Header.Add("Authorization", "Bearer "+f.patProgToken)
			},
			applyInvalid: func(r *http.Request) {
				r.Header.Add("Authorization", "Bearer tobiten_pat_"+f.patProgID+"_wrong-secret")
			},
			wantCredID: f.patProgID,
			wantUserID: f.alice.ID,
			wantPerms:  []domain.Permission{domain.PermissionLibraryRead, domain.PermissionProgressWrite},
		},
		{
			id:   srcBasic,
			tier: module.CredentialTierExplicit,
			applyValid: func(r *http.Request) {
				r.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:"+f.basicPass)))
			},
			applyInvalid: func(r *http.Request) {
				r.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:wrong-password")))
			},
			wantCredID: f.basicCredID,
			wantUserID: f.alice.ID,
			wantPerms:  f.basicPrincipal.Permissions,
		},
		{
			id:   srcXAuthToken,
			tier: module.CredentialTierExplicit,
			applyValid: func(r *http.Request) {
				r.Header.Add("X-Auth-Token", f.xAuthToken)
			},
			applyInvalid: func(r *http.Request) {
				r.Header.Add("X-Auth-Token", "bad-xauth-token")
			},
			wantCredID: f.xAuthCredID,
			wantUserID: f.bob.ID,
			wantPerms:  f.xAuthPrincipal.Permissions,
		},
		{
			id:   srcProxy,
			tier: module.CredentialTierTrustedProxy,
			applyValid: func(r *http.Request) {
				r.RemoteAddr = f.trustedIP
				r.Header.Set("Remote-User", "bob")
			},
			applyInvalid: func(r *http.Request) {
				r.RemoteAddr = f.trustedIP
				r.Header.Set("Remote-User", "unknown-proxy-user")
			},
			wantCredID: f.proxyCredID,
			wantUserID: f.bob.ID,
			wantPerms:  f.proxyPrincipal.Permissions,
		},
		{
			id:   srcCookie,
			tier: module.CredentialTierCookie,
			applyValid: func(r *http.Request) {
				r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: f.cookieToken})
			},
			applyInvalid: func(r *http.Request) {
				r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "expired-or-unknown-cookie"})
			},
			wantCredID: f.cookieCredID,
			wantUserID: f.alice.ID,
			wantPerms:  f.cookiePrinc.Permissions,
		},
	}

	orders := []struct {
		name  string
		auths []module.Authenticator
	}{
		{
			name:  "basic_pat_xauth_proxy_cookie",
			auths: []module.Authenticator{f.basicAuth, f.patAuth, f.xAuthStub, f.proxyStub, f.cookieStub},
		},
		{
			name:  "cookie_proxy_xauth_pat_basic",
			auths: []module.Authenticator{f.cookieStub, f.proxyStub, f.xAuthStub, f.patAuth, f.basicAuth},
		},
		{
			name:  "pat_basic_proxy_cookie_xauth",
			auths: []module.Authenticator{f.patAuth, f.basicAuth, f.proxyStub, f.cookieStub, f.xAuthStub},
		},
	}

	for _, ord := range orders {
		h := f.protectedHandler(ord.auths)

		for i := range len(sources) {
			for j := i + 1; j < len(sources); j++ {
				s1, s2 := sources[i], sources[j]
				pairName := string(s1.id) + "_vs_" + string(s2.id)

				t.Run(ord.name+"/"+pairName, func(t *testing.T) {
					type combo struct {
						name       string
						s1Valid    bool
						s2Valid    bool
						wantStatus int
						wantWinner *sourceSpec
					}

					var combos []combo
					if s1.tier == module.CredentialTierExplicit && s2.tier == module.CredentialTierExplicit {
						// Every pair of distinct explicit sources conflicts and returns 401.
						combos = []combo{
							{name: "valid+valid", s1Valid: true, s2Valid: true, wantStatus: http.StatusUnauthorized},
							{name: "valid+invalid", s1Valid: true, s2Valid: false, wantStatus: http.StatusUnauthorized},
							{name: "invalid+valid", s1Valid: false, s2Valid: true, wantStatus: http.StatusUnauthorized},
							{name: "invalid+invalid", s1Valid: false, s2Valid: false, wantStatus: http.StatusUnauthorized},
						}
					} else {
						// s1 is strictly higher tier than s2 in our ordered slice.
						winner := s1
						combos = []combo{
							{name: "valid_high+valid_low", s1Valid: true, s2Valid: true, wantStatus: http.StatusOK, wantWinner: &winner},
							{name: "valid_high+invalid_low", s1Valid: true, s2Valid: false, wantStatus: http.StatusOK, wantWinner: &winner},
							{name: "invalid_high+valid_low", s1Valid: false, s2Valid: true, wantStatus: http.StatusUnauthorized},
							{name: "invalid_high+invalid_low", s1Valid: false, s2Valid: false, wantStatus: http.StatusUnauthorized},
						}
					}

					for _, c := range combos {
						t.Run(c.name, func(t *testing.T) {
							req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
							req.RemoteAddr = "192.0.2.50:1234"
							if c.s1Valid {
								s1.applyValid(req)
							} else {
								s1.applyInvalid(req)
							}
							if c.s2Valid {
								s2.applyValid(req)
							} else {
								s2.applyInvalid(req)
							}

							rec := httptest.NewRecorder()
							h.ServeHTTP(rec, req)

							if rec.Code != c.wantStatus {
								t.Fatalf("status = %d, want %d (body: %s)", rec.Code, c.wantStatus, rec.Body.String())
							}
							if c.wantStatus == http.StatusUnauthorized {
								if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Basic ") {
									t.Errorf("missing WWW-Authenticate Basic challenge: %q", rec.Header().Get("WWW-Authenticate"))
								}
								return
							}

							var got domain.Principal
							if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
								t.Fatalf("decode principal: %v", err)
							}
							if got.CredentialID != c.wantWinner.wantCredID || got.User.ID != c.wantWinner.wantUserID {
								t.Fatalf("got principal=(user=%s, cred=%s), want (user=%s, cred=%s)",
									got.User.ID, got.CredentialID, c.wantWinner.wantUserID, c.wantWinner.wantCredID)
							}
							if !slices.Equal(got.Permissions, c.wantWinner.wantPerms) {
								t.Fatalf("got permissions=%v, want %v (no grant union)", got.Permissions, c.wantWinner.wantPerms)
							}
						})
					}
				})
			}
		}
	}
}

func TestCredentialPrecedence_RepeatedIdenticalVsDistinctSameUser(t *testing.T) {
	f := newPrecedenceFixture(t)
	h := f.protectedHandler([]module.Authenticator{f.cookieStub, f.proxyStub, f.xAuthStub, f.patAuth, f.basicAuth})

	basicAlice := base64.StdEncoding.EncodeToString([]byte("alice:" + f.basicPass))

	t.Run("repeated identical credentials accepted without widening scopes", func(t *testing.T) {
		cases := []struct {
			name       string
			setup      func(*http.Request)
			wantCredID string
			wantPerms  []domain.Permission
		}{
			{
				name: "repeated identical Basic",
				setup: func(r *http.Request) {
					r.Header.Add("Authorization", "Basic "+basicAlice)
					r.Header.Add("Authorization", "basic "+basicAlice)
				},
				wantCredID: f.basicCredID,
				wantPerms:  f.basicPrincipal.Permissions,
			},
			{
				name: "repeated identical Bearer",
				setup: func(r *http.Request) {
					r.Header.Add("Authorization", "Bearer "+f.patReadToken)
					r.Header.Add("Authorization", "bearer "+f.patReadToken)
				},
				wantCredID: f.patReadID,
				wantPerms:  []domain.Permission{domain.PermissionLibraryRead},
			},
			{
				name: "repeated identical X-API-Key",
				setup: func(r *http.Request) {
					r.Header.Add("X-API-Key", f.patReadToken)
					r.Header.Add("X-API-Key", f.patReadToken)
				},
				wantCredID: f.patReadID,
				wantPerms:  []domain.Permission{domain.PermissionLibraryRead},
			},
			{
				name: "identical Bearer and X-API-Key",
				setup: func(r *http.Request) {
					r.Header.Add("Authorization", "Bearer "+f.patReadToken)
					r.Header.Add("X-API-Key", f.patReadToken)
				},
				wantCredID: f.patReadID,
				wantPerms:  []domain.Permission{domain.PermissionLibraryRead},
			},
			{
				name: "repeated identical X-Auth-Token",
				setup: func(r *http.Request) {
					r.Header.Add("X-Auth-Token", f.xAuthToken)
					r.Header.Add("X-Auth-Token", f.xAuthToken)
				},
				wantCredID: f.xAuthCredID,
				wantPerms:  f.xAuthPrincipal.Permissions,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				req.RemoteAddr = f.trustedIP
				req.Header.Set("Remote-User", "bob")
				req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: f.cookieToken})
				tc.setup(req)

				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
				}
				var got domain.Principal
				if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
					t.Fatalf("decode principal: %v", err)
				}
				if got.CredentialID != tc.wantCredID || !slices.Equal(got.Permissions, tc.wantPerms) {
					t.Fatalf("got cred=%s perms=%v, want cred=%s perms=%v", got.CredentialID, got.Permissions, tc.wantCredID, tc.wantPerms)
				}
			})
		}
	})

	t.Run("distinct credentials for same user fail 401 with zero KDF or last_used_at updates", func(t *testing.T) {
		beforeReadCred, err := f.patStore.GetCredentialByID(t.Context(), f.patReadID)
		if err != nil {
			t.Fatalf("GetCredentialByID(read): %v", err)
		}
		beforeProgCred, err := f.patStore.GetCredentialByID(t.Context(), f.patProgID)
		if err != nil {
			t.Fatalf("GetCredentialByID(prog): %v", err)
		}

		cases := []struct {
			name  string
			setup func(*http.Request)
		}{
			{
				name: "two distinct Basic passwords same user",
				setup: func(r *http.Request) {
					r.Header.Add("Authorization", "Basic "+basicAlice)
					r.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:other-secret")))
				},
			},
			{
				name: "two distinct Bearer PATs same user Alice",
				setup: func(r *http.Request) {
					r.Header.Add("Authorization", "Bearer "+f.patReadToken)
					r.Header.Add("Authorization", "Bearer "+f.patProgToken)
				},
			},
			{
				name: "two distinct X-API-Key PATs same user Alice",
				setup: func(r *http.Request) {
					r.Header.Add("X-API-Key", f.patReadToken)
					r.Header.Add("X-API-Key", f.patProgToken)
				},
			},
			{
				name: "Bearer PAT1 plus X-API-Key PAT2 same user Alice",
				setup: func(r *http.Request) {
					r.Header.Add("Authorization", "Bearer "+f.patReadToken)
					r.Header.Add("X-API-Key", f.patProgToken)
				},
			},
			{
				name: "Basic Alice plus Bearer PAT Alice",
				setup: func(r *http.Request) {
					r.Header.Add("Authorization", "Basic "+basicAlice)
					r.Header.Add("Authorization", "Bearer "+f.patReadToken)
				},
			},
			{
				name: "Basic Alice plus X-API-Key PAT Alice",
				setup: func(r *http.Request) {
					r.Header.Add("Authorization", "Basic "+basicAlice)
					r.Header.Add("X-API-Key", f.patReadToken)
				},
			},
			{
				name: "two distinct X-Auth-Token values",
				setup: func(r *http.Request) {
					r.Header.Add("X-Auth-Token", f.xAuthToken)
					r.Header.Add("X-Auth-Token", "other-xauth-token")
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				kdfBefore := f.kdfCalls.Load()
				req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				req.RemoteAddr = f.trustedIP
				req.Header.Set("Remote-User", "bob")
				req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: f.cookieToken})
				tc.setup(req)

				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401", rec.Code)
				}
				if gotKDF := f.kdfCalls.Load() - kdfBefore; gotKDF != 0 {
					t.Fatalf("expected 0 KDF calls on upfront explicit conflict, got %d", gotKDF)
				}
			})
		}

		afterReadCred, _ := f.patStore.GetCredentialByID(t.Context(), f.patReadID)
		afterProgCred, _ := f.patStore.GetCredentialByID(t.Context(), f.patProgID)
		if !equalTimePtr(beforeReadCred.LastUsedAt, afterReadCred.LastUsedAt) ||
			!equalTimePtr(beforeProgCred.LastUsedAt, afterProgCred.LastUsedAt) {
			t.Fatalf("conflicting explicit credentials must not update PAT last_used_at")
		}
	})
}

func TestCredentialPrecedence_ProductionChainXAuthAndAllowlist(t *testing.T) {
	f := newPrecedenceFixture(t)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST /api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /api/v1/auth/setup", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("GET /api/v1/protected", func(w http.ResponseWriter, r *http.Request) {
		p := domain.MustPrincipalFromContext(r.Context())
		_ = json.NewEncoder(w).Encode(p)
	})

	// Production chain has Basic and PAT (no X-Auth-Token verifier yet), plus proxy/cookie stubs.
	h := middleware.Auth(middleware.AuthOptions{
		Authenticators: []module.Authenticator{f.basicAuth, f.patAuth, f.proxyStub, f.cookieStub},
	})(mux)

	t.Run("X-Auth-Token without matching verifier fails 401 on protected and allowlist routes", func(t *testing.T) {
		for _, target := range []struct {
			method string
			path   string
		}{
			{http.MethodGet, "/api/v1/protected"},
			{http.MethodGet, "/healthz"},
			{http.MethodPost, "/api/v1/auth/login"},
			{http.MethodPost, "/api/v1/auth/setup"},
		} {
			req := httptest.NewRequest(target.method, target.path, nil)
			req.RemoteAddr = f.trustedIP
			req.Header.Set("Remote-User", "bob")
			req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: f.cookieToken})
			req.Header.Set("X-Auth-Token", "some-session-token")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s %s: status = %d, want 401", target.method, target.path, rec.Code)
			}
		}
	})

	t.Run("untrusted proxy IP passes through to valid cookie", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		req.RemoteAddr = f.untrustedIP
		req.Header.Set("Remote-User", "bob")
		req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: f.cookieToken})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var got domain.Principal
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode principal: %v", err)
		}
		if got.CredentialID != f.cookieCredID || got.User.ID != f.alice.ID {
			t.Fatalf("expected cookie principal, got %+v", got)
		}
	})

	t.Run("public allowlist and 405 behavior preserved when unauthenticated", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /healthz = %d, want 200", rec.Code)
		}

		reqMethod := httptest.NewRequest(http.MethodPost, "/healthz", nil)
		recMethod := httptest.NewRecorder()
		h.ServeHTTP(recMethod, reqMethod)
		if recMethod.Code != http.StatusMethodNotAllowed {
			t.Fatalf("POST /healthz = %d, want 405", recMethod.Code)
		}

		reqLogin := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
		recLogin := httptest.NewRecorder()
		h.ServeHTTP(recLogin, reqLogin)
		if recLogin.Code != http.StatusOK {
			t.Fatalf("POST /api/v1/auth/login = %d, want 200", recLogin.Code)
		}

		reqSetup := httptest.NewRequest(http.MethodPost, "/api/v1/auth/setup", nil)
		recSetup := httptest.NewRecorder()
		h.ServeHTTP(recSetup, reqSetup)
		if recSetup.Code != http.StatusCreated {
			t.Fatalf("POST /api/v1/auth/setup = %d, want 201", recSetup.Code)
		}
	})
}

func equalTimePtr(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}

type legacyUntieredAuth struct {
	name  string
	calls *atomic.Int32
	fn    func(*http.Request) (module.AuthResult, error)
}

func (a *legacyUntieredAuth) Name() string { return a.name }

func (a *legacyUntieredAuth) Authenticate(r *http.Request) (module.AuthResult, error) {
	a.calls.Add(1)
	return a.fn(r)
}

type customTierAuth struct {
	*legacyUntieredAuth
	tier module.CredentialTier
}

func (a *customTierAuth) Tier() module.CredentialTier { return a.tier }

func TestCredentialPrecedence_RealProvidersUntieredAndUnknownFallback(t *testing.T) {
	f := newPrecedenceFixture(t)

	checkCookieFn := func(r *http.Request) (module.AuthResult, error) {
		c, err := r.Cookie("tobiten_session")
		if err != nil {
			if errors.Is(err, http.ErrNoCookie) {
				return module.AuthPass(), nil
			}
			return module.AuthResult{Handled: true}, err
		}
		if c.Value == f.cookieToken {
			return module.AuthSuccess(f.cookiePrinc), nil
		}
		return module.AuthResult{Handled: true}, ErrInvalidCredentials
	}
	checkProxyFn := func(r *http.Request) (module.AuthResult, error) {
		u := strings.TrimSpace(r.Header.Get("Remote-User"))
		if u == "" {
			return module.AuthPass(), nil
		}
		if u == "bob" {
			return module.AuthSuccess(f.proxyPrincipal), nil
		}
		return module.AuthResult{Handled: true}, ErrInvalidCredentials
	}

	type lowerSpec struct {
		name       string
		applyValid func(*http.Request)
		wantCredID string
		wantUserID string
		build      func(*atomic.Int32) module.Authenticator
	}

	specs := []lowerSpec{
		{
			name:       "no-tier-cookie",
			applyValid: func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: f.cookieToken}) },
			wantCredID: f.cookieCredID,
			wantUserID: f.alice.ID,
			build: func(c *atomic.Int32) module.Authenticator {
				return &legacyUntieredAuth{name: "no-tier-cookie", calls: c, fn: checkCookieFn}
			},
		},
		{
			name:       "unspecified-cookie",
			applyValid: func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: f.cookieToken}) },
			wantCredID: f.cookieCredID,
			wantUserID: f.alice.ID,
			build: func(c *atomic.Int32) module.Authenticator {
				return &customTierAuth{legacyUntieredAuth: &legacyUntieredAuth{name: "unspecified-cookie", calls: c, fn: checkCookieFn}, tier: module.CredentialTierUnspecified}
			},
		},
		{
			name:       "unknown-tier-cookie",
			applyValid: func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: f.cookieToken}) },
			wantCredID: f.cookieCredID,
			wantUserID: f.alice.ID,
			build: func(c *atomic.Int32) module.Authenticator {
				return &customTierAuth{legacyUntieredAuth: &legacyUntieredAuth{name: "unknown-tier-cookie", calls: c, fn: checkCookieFn}, tier: module.CredentialTier(99)}
			},
		},
		{
			name:       "no-tier-proxy",
			applyValid: func(r *http.Request) { r.Header.Set("Remote-User", "bob") },
			wantCredID: f.proxyCredID,
			wantUserID: f.bob.ID,
			build: func(c *atomic.Int32) module.Authenticator {
				return &legacyUntieredAuth{name: "no-tier-proxy", calls: c, fn: checkProxyFn}
			},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /api/v1/protected", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(domain.MustPrincipalFromContext(r.Context()))
	})

	for _, sp := range specs {
		t.Run(sp.name, func(t *testing.T) {
			var lowerCalls atomic.Int32
			lower := sp.build(&lowerCalls)

			chains := []struct {
				name  string
				auths []module.Authenticator
			}{
				{"real_basic_pat_then_lower", []module.Authenticator{f.basicAuth, f.patAuth, lower}},
				{"lower_then_real_pat_basic", []module.Authenticator{lower, f.patAuth, f.basicAuth}},
			}

			for _, ch := range chains {
				h := middleware.Auth(middleware.AuthOptions{Authenticators: ch.auths})(mux)
				for _, target := range []string{"/api/v1/protected", "/healthz"} {
					beforeKDF := f.kdfCalls.Load()
					lowerCalls.Store(0)
					req := httptest.NewRequest(http.MethodGet, target, nil)
					sp.applyValid(req)
					req.Header.Set("X-Auth-Token", "unverified-token")
					rec := httptest.NewRecorder()
					h.ServeHTTP(rec, req)
					if rec.Code != http.StatusUnauthorized {
						t.Fatalf("%s %s: status = %d, want 401", ch.name, target, rec.Code)
					}
					if lowerCalls.Load() != 0 {
						t.Fatalf("%s %s: lowerCalls = %d, want 0", ch.name, target, lowerCalls.Load())
					}
					if f.kdfCalls.Load() != beforeKDF {
						t.Fatalf("%s %s: unexpected KDF work (%d -> %d)", ch.name, target, beforeKDF, f.kdfCalls.Load())
					}
				}
			}

			// Missing Basic/PAT verifier regressions with real partial explicit chains.
			hBasicOnly := middleware.Auth(middleware.AuthOptions{Authenticators: []module.Authenticator{lower, f.basicAuth}})(mux)
			for _, mutate := range []func(*http.Request){
				func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+f.patReadToken) },
				func(r *http.Request) { r.Header.Set("Authorization", "Bearer") },
				func(r *http.Request) { r.Header.Set("X-API-Key", f.patReadToken) },
			} {
				lowerCalls.Store(0)
				req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				sp.applyValid(req)
				mutate(req)
				rec := httptest.NewRecorder()
				hBasicOnly.ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized || lowerCalls.Load() != 0 {
					t.Fatalf("basic-only missing PAT verifier: status=%d lowerCalls=%d", rec.Code, lowerCalls.Load())
				}
			}

			hPATOnly := middleware.Auth(middleware.AuthOptions{Authenticators: []module.Authenticator{lower, f.patAuth}})(mux)
			for _, mutate := range []func(*http.Request){
				func(r *http.Request) { r.SetBasicAuth("alice", f.basicPass) },
				func(r *http.Request) { r.Header.Set("Authorization", "Basic not_base64") },
			} {
				lowerCalls.Store(0)
				req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				sp.applyValid(req)
				mutate(req)
				rec := httptest.NewRecorder()
				hPATOnly.ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized || lowerCalls.Load() != 0 {
					t.Fatalf("pat-only missing Basic verifier: status=%d lowerCalls=%d", rec.Code, lowerCalls.Load())
				}
			}

			// Positive controls: valid explicit wins and lower-only works without explicit headers.
			hFull := middleware.Auth(middleware.AuthOptions{Authenticators: []module.Authenticator{lower, f.basicAuth, f.patAuth}})(mux)

			lowerCalls.Store(0)
			reqPAT := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
			sp.applyValid(reqPAT)
			reqPAT.Header.Set("Authorization", "Bearer "+f.patReadToken)
			recPAT := httptest.NewRecorder()
			hFull.ServeHTTP(recPAT, reqPAT)
			if recPAT.Code != http.StatusOK || lowerCalls.Load() != 0 {
				t.Fatalf("valid PAT positive control: status=%d lowerCalls=%d", recPAT.Code, lowerCalls.Load())
			}
			var gotPAT domain.Principal
			if err := json.Unmarshal(recPAT.Body.Bytes(), &gotPAT); err != nil {
				t.Fatalf("decode PAT principal: %v", err)
			}
			if gotPAT.CredentialID != f.patReadID {
				t.Fatalf("PAT control credential = %q, want %q", gotPAT.CredentialID, f.patReadID)
			}

			lowerCalls.Store(0)
			reqLow := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
			sp.applyValid(reqLow)
			recLow := httptest.NewRecorder()
			hFull.ServeHTTP(recLow, reqLow)
			if recLow.Code != http.StatusOK || lowerCalls.Load() != 1 {
				t.Fatalf("lower-only positive control: status=%d lowerCalls=%d", recLow.Code, lowerCalls.Load())
			}
			var gotLow domain.Principal
			if err := json.Unmarshal(recLow.Body.Bytes(), &gotLow); err != nil {
				t.Fatalf("decode lower principal: %v", err)
			}
			if gotLow.CredentialID != sp.wantCredID || gotLow.User.ID != sp.wantUserID {
				t.Fatalf("lower control = (%s,%s), want (%s,%s)", gotLow.User.ID, gotLow.CredentialID, sp.wantUserID, sp.wantCredID)
			}
		})
	}
}

var (
	_ module.TieredAuthenticator = (*BasicAuthenticator)(nil)
	_ module.TieredAuthenticator = (*PATAuthenticator)(nil)
)
