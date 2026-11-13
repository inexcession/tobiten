package middleware_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
)

type staticSubPathResolver struct {
	subPath        string
	subPathDecoded string
}

func (s staticSubPathResolver) SubPath() string        { return s.subPath }
func (s staticSubPathResolver) SubPathDecoded() string { return s.subPathDecoded }

type testAuthenticator struct {
	name      string
	token     string
	principal *domain.Principal
	failErr   error
}

func (t *testAuthenticator) Name() string { return t.name }

func (t *testAuthenticator) Authenticate(r *http.Request) (module.AuthResult, error) {
	if t.failErr != nil {
		return module.AuthResult{Handled: true}, t.failErr
	}
	if t.token == "" {
		return module.AuthPass(), nil
	}
	header := r.Header.Get("X-Test-Token")
	if header == "" {
		return module.AuthPass(), nil
	}
	if header != t.token {
		return module.AuthResult{Handled: true}, errors.New("bad token")
	}
	return module.AuthSuccess(t.principal), nil
}

func testTargetHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := domain.PrincipalFromContext(r.Context())
		if ok && p != nil {
			w.Header().Set("X-Principal", p.User.Username)
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("target reached"))
	})
}

func TestAuthMiddleware_ClosedAllowlist(t *testing.T) {
	auth := middleware.Auth(middleware.AuthOptions{})(testTargetHandler())

	t.Run("GET /healthz is allowlisted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
		if body := rec.Body.String(); body != "target reached" {
			t.Errorf("body = %q, want 'target reached'", body)
		}
	})

	t.Run("HEAD /healthz is allowlisted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/healthz", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
	})

	t.Run("POST /healthz returns 405 Method Not Allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
		if allow := rec.Header().Get("Allow"); !strings.Contains(allow, "GET") {
			t.Errorf("Allow header = %q, want it to contain GET", allow)
		}
	})

	t.Run("POST /api/v1/auth/login is allowlisted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader("{}"))
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
	})

	t.Run("GET /api/v1/auth/login returns 405 Method Not Allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/login", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
		if allow := rec.Header().Get("Allow"); allow != "POST" {
			t.Errorf("Allow header = %q, want 'POST'", allow)
		}
	})

	t.Run("POST /api/v1/auth/setup is allowlisted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/setup", strings.NewReader("{}"))
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
	})

	t.Run("GET /api/v1/auth/setup returns 405 Method Not Allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/setup", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
		if allow := rec.Header().Get("Allow"); allow != "POST" {
			t.Errorf("Allow header = %q, want 'POST'", allow)
		}
	})

	t.Run("HEAD /api/v1/auth/setup returns 405 Method Not Allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/api/v1/auth/setup", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
	})
}

func TestAuthMiddleware_ProtectedDescendants(t *testing.T) {
	auth := middleware.Auth(middleware.AuthOptions{})(testTargetHandler())

	descendants := []string{
		"/healthz/private",
		"/healthz/",
		"/healthz/deep/path",
		"/api/v1/auth/setup/child",
		"/api/v1/auth/setup/",
		"/api/v1/auth/login/token",
		"/api/v1/auth/login/",
		"/api/v1/users",
		"/api/v1/series",
		"/komga/api/v1/books",
		"/opds/v1.2/catalog",
	}

	for _, path := range descendants {
		t.Run("denies "+path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			auth.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("path %q status = %d, want 401", path, rec.Code)
			}
			if wwwAuth := rec.Header().Get("WWW-Authenticate"); !strings.HasPrefix(wwwAuth, "Basic") {
				t.Errorf("WWW-Authenticate = %q, want Basic scheme", wwwAuth)
			}
			body, _ := io.ReadAll(rec.Body)
			if !strings.Contains(string(body), "unauthorized") {
				t.Errorf("body = %q, want it to contain 'unauthorized'", string(body))
			}
		})
	}
}

func TestAuthMiddleware_SubPathResolver(t *testing.T) {
	resolver := staticSubPathResolver{
		subPath:        "/tobiten",
		subPathDecoded: "/tobiten",
	}
	auth := middleware.Auth(middleware.AuthOptions{
		SubPathResolver: resolver,
	})(testTargetHandler())

	t.Run("direct /healthz works", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
	})

	t.Run("subpath /tobiten/healthz works", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/tobiten/healthz", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
	})

	t.Run("subpath /tobiten/api/v1/auth/setup works with POST", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/tobiten/api/v1/auth/setup", strings.NewReader("{}"))
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
	})

	t.Run("subpath /tobiten/api/v1/auth/setup with GET returns 405", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/tobiten/api/v1/auth/setup", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
	})

	t.Run("subpath /tobiten/healthz/private is protected (401)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/tobiten/healthz/private", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("subpath /tobiten/api/v1/books is protected (401)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/tobiten/api/v1/books", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
	})
}

func TestAuthMiddleware_AuthenticatorChain(t *testing.T) {
	alicePrincipal := domain.NewPrincipal(
		domain.User{ID: "usr_alice", Username: "alice"},
		domain.BuiltInRoles()[:1],
		domain.CredentialKindPassword,
		"cred_1",
		nil,
	)

	passAuth := &testAuthenticator{name: "pass-auth"}
	aliceAuth := &testAuthenticator{name: "alice-auth", token: "alice-secret", principal: alicePrincipal}
	failingAuth := &testAuthenticator{name: "failing-auth", failErr: errors.New("explicit failure")}

	t.Run("succeeds when authenticator recognizes credentials and propagates Principal", func(t *testing.T) {
		auth := middleware.Auth(middleware.AuthOptions{
			Authenticators: []module.Authenticator{passAuth, aliceAuth},
		})(testTargetHandler())

		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		req.Header.Set("X-Test-Token", "alice-secret")
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
		if p := rec.Header().Get("X-Principal"); p != "alice" {
			t.Errorf("X-Principal = %q, want 'alice'", p)
		}
	})

	t.Run("returns 401 when token is invalid", func(t *testing.T) {
		auth := middleware.Auth(middleware.AuthOptions{
			Authenticators: []module.Authenticator{passAuth, aliceAuth},
		})(testTargetHandler())

		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		req.Header.Set("X-Test-Token", "wrong-secret")
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
		if wwwAuth := rec.Header().Get("WWW-Authenticate"); !strings.HasPrefix(wwwAuth, "Basic") {
			t.Errorf("WWW-Authenticate = %q, want Basic scheme", wwwAuth)
		}
	})

	t.Run("returns 401 when authenticator fails explicitly", func(t *testing.T) {
		auth := middleware.Auth(middleware.AuthOptions{
			Authenticators: []module.Authenticator{failingAuth},
		})(testTargetHandler())

		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("returns 401 when no authenticator handles request", func(t *testing.T) {
		auth := middleware.Auth(middleware.AuthOptions{
			Authenticators: []module.Authenticator{passAuth},
		})(testTargetHandler())

		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		rec := httptest.NewRecorder()
		auth.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
		body := rec.Body.String()
		if body == "" || !strings.Contains(body, "unauthorized") {
			t.Errorf("body = %q, want non-empty error json", body)
		}
	})
}

type tieredStubAuth struct {
	name string
	tier module.CredentialTier
	fn   func(r *http.Request) (module.AuthResult, error)
}

func (a *tieredStubAuth) Name() string                { return a.name }
func (a *tieredStubAuth) Tier() module.CredentialTier { return a.tier }
func (a *tieredStubAuth) Authenticate(r *http.Request) (module.AuthResult, error) {
	return a.fn(r)
}

func TestAuthMiddleware_PrecedenceAndClassification(t *testing.T) {
	adminRole, _ := domain.BuiltInRoleByID(domain.RoleAdminID)
	curatorRole, _ := domain.BuiltInRoleByID(domain.RoleCuratorID)
	readerRole, _ := domain.BuiltInRoleByID(domain.RoleReaderID)

	explicitPrincipal := domain.NewPrincipal(
		domain.User{ID: "usr-explicit", Username: "explicit-user"},
		[]domain.Role{readerRole},
		domain.CredentialKindPAT,
		"cred-explicit",
		[]domain.PATScope{domain.ScopeRead},
	)
	proxyPrincipal := domain.NewPrincipal(
		domain.User{ID: "usr-proxy", Username: "proxy-user"},
		[]domain.Role{curatorRole},
		domain.CredentialKindOIDCLink,
		"cred-proxy",
		nil,
	)
	cookiePrincipal := domain.NewPrincipal(
		domain.User{ID: "usr-cookie", Username: "cookie-user"},
		[]domain.Role{adminRole},
		domain.CredentialKindPassword,
		"cred-cookie",
		nil,
	)

	xAuthStub := &tieredStubAuth{
		name: "x-auth-token",
		tier: module.CredentialTierExplicit,
		fn: func(r *http.Request) (module.AuthResult, error) {
			tok := r.Header.Get("X-Auth-Token")
			if tok == "" {
				return module.AuthPass(), nil
			}
			if tok == "valid-xauth" {
				return module.AuthSuccess(explicitPrincipal), nil
			}
			return module.AuthResult{Handled: true}, errors.New("invalid x-auth-token")
		},
	}
	proxyStub := &tieredStubAuth{
		name: "trusted-proxy-stub",
		tier: module.CredentialTierTrustedProxy,
		fn: func(r *http.Request) (module.AuthResult, error) {
			u := r.Header.Get("Remote-User")
			if u == "" {
				return module.AuthPass(), nil
			}
			if u == "proxy-user" {
				return module.AuthSuccess(proxyPrincipal), nil
			}
			return module.AuthResult{Handled: true}, errors.New("invalid proxy user")
		},
	}
	cookieStub := &tieredStubAuth{
		name: "cookie-stub",
		tier: module.CredentialTierCookie,
		fn: func(r *http.Request) (module.AuthResult, error) {
			c, err := r.Cookie("tobiten_session")
			if err != nil {
				if errors.Is(err, http.ErrNoCookie) {
					return module.AuthPass(), nil
				}
				return module.AuthResult{Handled: true}, err
			}
			if c.Value == "valid-cookie" {
				return module.AuthSuccess(cookiePrincipal), nil
			}
			return module.AuthResult{Handled: true}, errors.New("invalid cookie")
		},
	}

	orders := []struct {
		name  string
		auths []module.Authenticator
	}{
		{name: "explicit-proxy-cookie", auths: []module.Authenticator{xAuthStub, proxyStub, cookieStub}},
		{name: "cookie-proxy-explicit", auths: []module.Authenticator{cookieStub, proxyStub, xAuthStub}},
	}

	for _, ord := range orders {
		t.Run(ord.name, func(t *testing.T) {
			h := middleware.Auth(middleware.AuthOptions{
				Authenticators: ord.auths,
			})(testTargetHandler())

			t.Run("explicit beats proxy and cookie", func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				req.Header.Set("X-Auth-Token", "valid-xauth")
				req.Header.Set("Remote-User", "proxy-user")
				req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "valid-cookie"})
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK || rec.Header().Get("X-Principal") != "explicit-user" {
					t.Fatalf("got status=%d principal=%q, want 200 explicit-user", rec.Code, rec.Header().Get("X-Principal"))
				}
			})

			t.Run("proxy beats cookie", func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				req.Header.Set("Remote-User", "proxy-user")
				req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "valid-cookie"})
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK || rec.Header().Get("X-Principal") != "proxy-user" {
					t.Fatalf("got status=%d principal=%q, want 200 proxy-user", rec.Code, rec.Header().Get("X-Principal"))
				}
			})

			t.Run("invalid explicit never falls back to proxy or cookie", func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				req.Header.Set("X-Auth-Token", "bad-xauth")
				req.Header.Set("Remote-User", "proxy-user")
				req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "valid-cookie"})
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401", rec.Code)
				}
			})

			t.Run("invalid handled proxy never falls back to cookie", func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				req.Header.Set("Remote-User", "unknown-user")
				req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "valid-cookie"})
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401", rec.Code)
				}
			})

			t.Run("malformed and unsupported explicit sources fail 401 without fallback", func(t *testing.T) {
				cases := []struct {
					name   string
					mutate func(*http.Request)
				}{
					{name: "empty authorization", mutate: func(r *http.Request) { r.Header.Set("Authorization", "   ") }},
					{name: "digest scheme", mutate: func(r *http.Request) { r.Header.Set("Authorization", `Digest username="alice"`) }},
					{name: "empty bearer", mutate: func(r *http.Request) { r.Header.Set("Authorization", "Bearer") }},
					{name: "empty basic", mutate: func(r *http.Request) { r.Header.Set("Authorization", "Basic") }},
					{name: "empty x-api-key", mutate: func(r *http.Request) { r.Header.Set("X-API-Key", "") }},
					{name: "empty x-auth-token", mutate: func(r *http.Request) { r.Header.Set("X-Auth-Token", "  ") }},
				}
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
						req.Header.Set("Remote-User", "proxy-user")
						req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "valid-cookie"})
						tc.mutate(req)
						rec := httptest.NewRecorder()
						h.ServeHTTP(rec, req)
						if rec.Code != http.StatusUnauthorized {
							t.Fatalf("%s: status = %d, want 401", tc.name, rec.Code)
						}
					})
				}
			})
		})
	}

	t.Run("X-Auth-Token without matching verifier fails 401 and never falls back", func(t *testing.T) {
		h := middleware.Auth(middleware.AuthOptions{
			Authenticators: []module.Authenticator{proxyStub, cookieStub},
		})(testTargetHandler())

		for _, target := range []string{"/api/v1/protected", "/healthz"} {
			req := httptest.NewRequest(http.MethodGet, target, nil)
			req.Header.Set("X-Auth-Token", "unverified-token")
			req.Header.Set("Remote-User", "proxy-user")
			req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "valid-cookie"})
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("target %s: status = %d, want 401", target, rec.Code)
			}
		}
	})

	t.Run("same-tier conflicting custom explicit authenticators fail 401 even for same user", func(t *testing.T) {
		alice1 := domain.NewPrincipal(
			domain.User{ID: "usr-alice", Username: "alice"},
			[]domain.Role{readerRole},
			domain.CredentialKindPAT,
			"cred-1",
			[]domain.PATScope{domain.ScopeRead},
		)
		alice2 := domain.NewPrincipal(
			domain.User{ID: "usr-alice", Username: "alice"},
			[]domain.Role{readerRole},
			domain.CredentialKindPAT,
			"cred-2",
			[]domain.PATScope{domain.ScopeRead},
		)
		a1 := &tieredStubAuth{
			name: "custom-1",
			tier: module.CredentialTierExplicit,
			fn:   func(r *http.Request) (module.AuthResult, error) { return module.AuthSuccess(alice1), nil },
		}
		a2 := &tieredStubAuth{
			name: "custom-2",
			tier: module.CredentialTierExplicit,
			fn:   func(r *http.Request) (module.AuthResult, error) { return module.AuthSuccess(alice2), nil },
		}
		h := middleware.Auth(middleware.AuthOptions{
			Authenticators: []module.Authenticator{a1, a2},
		})(testTargetHandler())

		req := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
	})
}

type untieredStubAuth struct {
	name string
	fn   func(r *http.Request) (module.AuthResult, error)
}

func (a *untieredStubAuth) Name() string { return a.name }
func (a *untieredStubAuth) Authenticate(r *http.Request) (module.AuthResult, error) {
	return a.fn(r)
}

type tierFallbackFixture struct {
	explicitPrincipal    *domain.Principal
	lowerCookiePrincipal *domain.Principal
	lowerProxyPrincipal  *domain.Principal
}

func newTierFallbackFixture() tierFallbackFixture {
	adminRole, _ := domain.BuiltInRoleByID(domain.RoleAdminID)
	readerRole, _ := domain.BuiltInRoleByID(domain.RoleReaderID)
	return tierFallbackFixture{
		explicitPrincipal: domain.NewPrincipal(
			domain.User{ID: "usr-explicit", Username: "explicit-user"},
			[]domain.Role{readerRole},
			domain.CredentialKindPAT,
			"cred-explicit",
			[]domain.PATScope{domain.ScopeRead},
		),
		lowerCookiePrincipal: domain.NewPrincipal(
			domain.User{ID: "usr-legacy-cookie", Username: "legacy-cookie-user"},
			[]domain.Role{adminRole},
			domain.CredentialKindPassword,
			"cred-legacy-cookie",
			nil,
		),
		lowerProxyPrincipal: domain.NewPrincipal(
			domain.User{ID: "usr-legacy-proxy", Username: "legacy-proxy-user"},
			[]domain.Role{adminRole},
			domain.CredentialKindOIDCLink,
			"cred-legacy-proxy",
			nil,
		),
	}
}

func (f tierFallbackFixture) makeCookieFn(calls *int) func(*http.Request) (module.AuthResult, error) {
	return func(r *http.Request) (module.AuthResult, error) {
		*calls++
		c, err := r.Cookie("tobiten_session")
		if err != nil {
			if errors.Is(err, http.ErrNoCookie) {
				return module.AuthPass(), nil
			}
			return module.AuthResult{Handled: true}, err
		}
		if c.Value == "valid-cookie" {
			return module.AuthSuccess(f.lowerCookiePrincipal), nil
		}
		return module.AuthResult{Handled: true}, errors.New("invalid cookie")
	}
}

func (f tierFallbackFixture) makeProxyFn(calls *int) func(*http.Request) (module.AuthResult, error) {
	return func(r *http.Request) (module.AuthResult, error) {
		*calls++
		u := r.Header.Get("Remote-User")
		if u == "" {
			return module.AuthPass(), nil
		}
		if u == "proxy-user" {
			return module.AuthSuccess(f.lowerProxyPrincipal), nil
		}
		return module.AuthResult{Handled: true}, errors.New("invalid proxy user")
	}
}

func (f tierFallbackFixture) makeBasicVerifier() module.Authenticator {
	return &tieredStubAuth{
		name: "basic-verifier",
		tier: module.CredentialTierExplicit,
		fn: func(r *http.Request) (module.AuthResult, error) {
			u, p, ok := r.BasicAuth()
			if !ok {
				hdr := strings.ToLower(strings.TrimSpace(r.Header.Get("Authorization")))
				if strings.HasPrefix(hdr, "basic") {
					return module.AuthResult{Handled: true}, errors.New("malformed basic")
				}
				return module.AuthPass(), nil
			}
			if u == "alice" && p == "secret" {
				return module.AuthSuccess(f.explicitPrincipal), nil
			}
			return module.AuthResult{Handled: true}, errors.New("bad basic")
		},
	}
}

func (f tierFallbackFixture) makeBearerVerifier() module.Authenticator {
	return &tieredStubAuth{
		name: "bearer-verifier",
		tier: module.CredentialTierExplicit,
		fn: func(r *http.Request) (module.AuthResult, error) {
			raw := r.Header.Get("Authorization")
			tok, ok := strings.CutPrefix(raw, "Bearer ")
			if !ok {
				if strings.EqualFold(strings.TrimSpace(raw), "bearer") {
					return module.AuthResult{Handled: true}, errors.New("empty bearer")
				}
				return module.AuthPass(), nil
			}
			if tok == "valid-pat" {
				return module.AuthSuccess(f.explicitPrincipal), nil
			}
			return module.AuthResult{Handled: true}, errors.New("bad bearer")
		},
	}
}

type lowerTierVariant struct {
	name         string
	wantUsername string
	applyValid   func(*http.Request)
	build        func(calls *int) module.Authenticator
}

func (f tierFallbackFixture) variants() []lowerTierVariant {
	return []lowerTierVariant{
		{
			name:         "no-tier-cookie",
			wantUsername: "legacy-cookie-user",
			applyValid:   func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "valid-cookie"}) },
			build: func(calls *int) module.Authenticator {
				return &untieredStubAuth{name: "no-tier-cookie", fn: f.makeCookieFn(calls)}
			},
		},
		{
			name:         "unspecified-cookie",
			wantUsername: "legacy-cookie-user",
			applyValid:   func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "valid-cookie"}) },
			build: func(calls *int) module.Authenticator {
				return &tieredStubAuth{name: "unspecified-cookie", tier: module.CredentialTierUnspecified, fn: f.makeCookieFn(calls)}
			},
		},
		{
			name:         "unknown-tier-cookie",
			wantUsername: "legacy-cookie-user",
			applyValid:   func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "valid-cookie"}) },
			build: func(calls *int) module.Authenticator {
				return &tieredStubAuth{name: "unknown-tier-cookie", tier: module.CredentialTier(99), fn: f.makeCookieFn(calls)}
			},
		},
		{
			name:         "no-tier-proxy",
			wantUsername: "legacy-proxy-user",
			applyValid:   func(r *http.Request) { r.Header.Set("Remote-User", "proxy-user") },
			build: func(calls *int) module.Authenticator {
				return &untieredStubAuth{name: "no-tier-proxy", fn: f.makeProxyFn(calls)}
			},
		},
		{
			name:         "unspecified-proxy",
			wantUsername: "legacy-proxy-user",
			applyValid:   func(r *http.Request) { r.Header.Set("Remote-User", "proxy-user") },
			build: func(calls *int) module.Authenticator {
				return &tieredStubAuth{name: "unspecified-proxy", tier: module.CredentialTierUnspecified, fn: f.makeProxyFn(calls)}
			},
		},
		{
			name:         "unknown-tier-proxy",
			wantUsername: "legacy-proxy-user",
			applyValid:   func(r *http.Request) { r.Header.Set("Remote-User", "proxy-user") },
			build: func(calls *int) module.Authenticator {
				return &tieredStubAuth{name: "unknown-tier-proxy", tier: module.CredentialTier(99), fn: f.makeProxyFn(calls)}
			},
		},
	}
}

func assertRejectsWithoutLowerCall(t *testing.T, auths []module.Authenticator, applyLower, mutate func(*http.Request), lowerCalls *int) {
	t.Helper()
	h := middleware.Auth(middleware.AuthOptions{Authenticators: auths})(testTargetHandler())
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/protected"},
		{http.MethodGet, "/healthz"},
		{http.MethodPost, "/api/v1/auth/login"},
		{http.MethodPost, "/api/v1/auth/setup"},
	}
	for _, rt := range routes {
		*lowerCalls = 0
		req := httptest.NewRequest(rt.method, rt.path, nil)
		applyLower(req)
		mutate(req)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s: status = %d, want 401", rt.method, rt.path, rec.Code)
		}
		if *lowerCalls != 0 {
			t.Fatalf("%s %s: lowerCalls = %d, want 0", rt.method, rt.path, *lowerCalls)
		}
	}
}

func TestAuthMiddleware_UntieredAndUnknownTierFallback(t *testing.T) {
	f := newTierFallbackFixture()
	for _, v := range f.variants() {
		t.Run(v.name, func(t *testing.T) {
			var lowerCalls int
			lowerAuth := v.build(&lowerCalls)

			t.Run("unverified X-Auth-Token returns 401 without calling lower provider", func(t *testing.T) {
				mutate := func(r *http.Request) { r.Header.Set("X-Auth-Token", "unverified-token") }
				assertRejectsWithoutLowerCall(t, []module.Authenticator{f.makeBasicVerifier(), f.makeBearerVerifier(), lowerAuth}, v.applyValid, mutate, &lowerCalls)
				assertRejectsWithoutLowerCall(t, []module.Authenticator{lowerAuth, f.makeBearerVerifier(), f.makeBasicVerifier()}, v.applyValid, mutate, &lowerCalls)
			})

			t.Run("missing-verifier and malformed Basic or Bearer return 401 without calling lower provider", func(t *testing.T) {
				cases := []struct {
					name   string
					auths  []module.Authenticator
					mutate func(*http.Request)
				}{
					{"missing Basic verifier with valid-syntax Basic", []module.Authenticator{f.makeBearerVerifier(), lowerAuth}, func(r *http.Request) { r.SetBasicAuth("alice", "secret") }},
					{"missing Basic verifier with malformed Basic base64", []module.Authenticator{lowerAuth, f.makeBearerVerifier()}, func(r *http.Request) { r.Header.Set("Authorization", "Basic not_base64") }},
					{"missing Basic verifier with empty Basic", []module.Authenticator{lowerAuth}, func(r *http.Request) { r.Header.Set("Authorization", "Basic") }},
					{"missing Bearer verifier with Bearer token", []module.Authenticator{f.makeBasicVerifier(), lowerAuth}, func(r *http.Request) { r.Header.Set("Authorization", "Bearer tobiten_pat_id_secret") }},
					{"missing Bearer verifier with malformed empty Bearer", []module.Authenticator{lowerAuth, f.makeBasicVerifier()}, func(r *http.Request) { r.Header.Set("Authorization", "Bearer") }},
					{"missing X-API-Key verifier", []module.Authenticator{lowerAuth, f.makeBasicVerifier()}, func(r *http.Request) { r.Header.Set("X-API-Key", "tobiten_pat_id_secret") }},
					{"unsupported Authorization scheme", []module.Authenticator{lowerAuth, f.makeBasicVerifier(), f.makeBearerVerifier()}, func(r *http.Request) { r.Header.Set("Authorization", `Digest username="alice"`) }},
				}
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						assertRejectsWithoutLowerCall(t, tc.auths, v.applyValid, tc.mutate, &lowerCalls)
					})
				}
			})

			t.Run("positive controls: matching explicit wins and lower-only still works", func(t *testing.T) {
				h := middleware.Auth(middleware.AuthOptions{
					Authenticators: []module.Authenticator{lowerAuth, f.makeBasicVerifier(), f.makeBearerVerifier()},
				})(testTargetHandler())

				lowerCalls = 0
				reqExp := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				v.applyValid(reqExp)
				reqExp.SetBasicAuth("alice", "secret")
				recExp := httptest.NewRecorder()
				h.ServeHTTP(recExp, reqExp)
				if recExp.Code != http.StatusOK || recExp.Header().Get("X-Principal") != "explicit-user" || lowerCalls != 0 {
					t.Fatalf("explicit control: status=%d principal=%q lowerCalls=%d", recExp.Code, recExp.Header().Get("X-Principal"), lowerCalls)
				}

				lowerCalls = 0
				reqLow := httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil)
				v.applyValid(reqLow)
				recLow := httptest.NewRecorder()
				h.ServeHTTP(recLow, reqLow)
				if recLow.Code != http.StatusOK || recLow.Header().Get("X-Principal") != v.wantUsername || lowerCalls != 1 {
					t.Fatalf("lower-only control: status=%d principal=%q lowerCalls=%d", recLow.Code, recLow.Header().Get("X-Principal"), lowerCalls)
				}
			})
		})
	}
}
