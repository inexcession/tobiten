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
