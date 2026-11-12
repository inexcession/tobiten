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
