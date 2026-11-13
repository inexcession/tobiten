package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
)

type stubOriginResolver struct {
	origin     string
	https      bool
	cookiePath string
}

func (s stubOriginResolver) Origin() string           { return s.origin }
func (s stubOriginResolver) IsHTTPS() bool            { return s.https }
func (s stubOriginResolver) NativeCookiePath() string { return s.cookiePath }
func (s stubOriginResolver) SubPath() string          { return "" }
func (s stubOriginResolver) SubPathDecoded() string   { return "" }

type stubSessionCookieAuth struct {
	token    string
	csrfHash string
	user     domain.User
}

func (a stubSessionCookieAuth) Name() string { return "native-session" }
func (a stubSessionCookieAuth) Tier() module.CredentialTier {
	return module.CredentialTierCookie
}
func (a stubSessionCookieAuth) Authenticate(r *http.Request) (module.AuthResult, error) {
	c, err := r.Cookie("tobiten_session")
	if errors.Is(err, http.ErrNoCookie) {
		return module.AuthPass(), nil
	}
	if err != nil {
		return module.AuthResult{}, err
	}
	if c.Value != a.token {
		return module.AuthResult{}, errors.New("invalid session")
	}
	role := domain.Role{ID: "r1", Name: "ADMIN", Permissions: domain.AllPermissions()}
	p := domain.NewPrincipal(a.user, []domain.Role{role}, domain.CredentialKindPassword, "cred-1", nil)
	return module.AuthSuccessCookie(p, a.csrfHash), nil
}

type stubBasicExplicitAuth struct {
	user domain.User
}

func (a stubBasicExplicitAuth) Name() string { return "basic" }
func (a stubBasicExplicitAuth) Tier() module.CredentialTier {
	return module.CredentialTierExplicit
}
func (a stubBasicExplicitAuth) Authenticate(r *http.Request) (module.AuthResult, error) {
	u, p, ok := r.BasicAuth()
	if !ok {
		return module.AuthPass(), nil
	}
	if u != "alice" || p != "secret" {
		return module.AuthResult{}, errors.New("invalid basic")
	}
	role := domain.Role{ID: "r1", Name: "ADMIN", Permissions: domain.AllPermissions()}
	pr := domain.NewPrincipal(a.user, []domain.Role{role}, domain.CredentialKindPassword, "cred-basic", nil)
	return module.AuthSuccess(pr), nil
}

func sha256Hex(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func TestAuthMiddleware_CSRFAndBrowserOriginEnforcement(t *testing.T) {
	t.Parallel()

	alice := domain.User{ID: "u-1", Username: "alice"}
	csrfToken := "csrf-secret-value"
	sessionAuth := stubSessionCookieAuth{
		token:    "sess-valid-1",
		csrfHash: sha256Hex(csrfToken),
		user:     alice,
	}
	basicAuth := stubBasicExplicitAuth{user: alice}
	resolver := stubOriginResolver{origin: "https://comics.example.com", https: true, cookiePath: "/"}

	var handlerCalled bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	h := Auth(AuthOptions{
		Authenticators:  []module.Authenticator{basicAuth, sessionAuth},
		OriginResolver:  resolver,
		SubPathResolver: resolver,
	})(next)

	runReq := func(r *http.Request) *httptest.ResponseRecorder {
		handlerCalled = false
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec
	}

	// 1. Safe GET with cookie does not require CSRF token.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/libraries", nil)
	req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "sess-valid-1"})
	if rec := runReq(req); rec.Code != http.StatusOK || !handlerCalled {
		t.Fatalf("GET with cookie: code=%d called=%v", rec.Code, handlerCalled)
	}

	// 2. Cookie mutation without X-CSRF-Token must be 403 BEFORE handler.
	req = httptest.NewRequest(http.MethodPost, "/api/v1/libraries", nil)
	req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "sess-valid-1"})
	if rec := runReq(req); rec.Code != http.StatusForbidden || handlerCalled {
		t.Fatalf("POST cookie without CSRF: code=%d called=%v", rec.Code, handlerCalled)
	}

	// 3. Cookie mutation with wrong X-CSRF-Token must be 403 BEFORE handler.
	req = httptest.NewRequest(http.MethodPost, "/api/v1/libraries", nil)
	req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "sess-valid-1"})
	req.Header.Set("X-CSRF-Token", "wrong-token")
	if rec := runReq(req); rec.Code != http.StatusForbidden || handlerCalled {
		t.Fatalf("POST cookie wrong CSRF: code=%d called=%v", rec.Code, handlerCalled)
	}

	// 4. Cookie mutation with matching X-CSRF-Token and omitted browser headers succeeds.
	req = httptest.NewRequest(http.MethodPost, "/api/v1/libraries", nil)
	req.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "sess-valid-1"})
	req.Header.Set("X-CSRF-Token", csrfToken)
	if rec := runReq(req); rec.Code != http.StatusOK || !handlerCalled {
		t.Fatalf("POST cookie valid CSRF: code=%d called=%v", rec.Code, handlerCalled)
	}

	// 5. Foreign / null / cross-site headers rejected with 403 BEFORE handler on BOTH cookie mutations and login.
	badOriginCases := []struct {
		name   string
		path   string
		cookie string
		csrf   string
		origin string
		ref    string
		fetch  string
	}{
		{"login null origin", "/api/v1/auth/login", "", "", "null", "", ""},
		{"login foreign origin", "/api/v1/auth/login", "", "", "https://evil.example", "", ""},
		{"login foreign referer", "/api/v1/auth/login", "", "", "", "https://evil.example/form", ""},
		{"login cross-site fetch", "/api/v1/auth/login", "", "", "", "", "cross-site"},
		{"login same-site fetch", "/api/v1/auth/login", "", "", "", "", "same-site"},
		{"cookie mutation null origin", "/api/v1/libraries", "sess-valid-1", csrfToken, "null", "", ""},
		{"cookie mutation foreign origin", "/api/v1/libraries", "sess-valid-1", csrfToken, "http://comics.example.com", "", ""},
		{"cookie mutation foreign referer", "/api/v1/libraries", "sess-valid-1", csrfToken, "", "https://evil.example/", ""},
		{"cookie mutation cross-site fetch", "/api/v1/libraries", "sess-valid-1", csrfToken, "https://comics.example.com", "", "cross-site"},
	}
	for _, tc := range badOriginCases {
		r := httptest.NewRequest(http.MethodPost, tc.path, nil)
		if tc.cookie != "" {
			r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: tc.cookie})
		}
		if tc.csrf != "" {
			r.Header.Set("X-CSRF-Token", tc.csrf)
		}
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		if tc.ref != "" {
			r.Header.Set("Referer", tc.ref)
		}
		if tc.fetch != "" {
			r.Header.Set("Sec-Fetch-Site", tc.fetch)
		}
		if rec := runReq(r); rec.Code != http.StatusForbidden || handlerCalled {
			t.Fatalf("%s: expected 403 without handler call, got code=%d called=%v", tc.name, rec.Code, handlerCalled)
		}
	}

	// 6. Same-origin login succeeds even if client carries an expired/invalid session cookie,
	// without requiring an unauthenticated CSRF token.
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "expired-stale-cookie"})
	r.Header.Set("Origin", "https://comics.example.com:443")
	r.Header.Set("Referer", "https://comics.example.com/login")
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	if rec := runReq(r); rec.Code != http.StatusOK || !handlerCalled {
		t.Fatalf("same-origin login with stale cookie: code=%d called=%v", rec.Code, handlerCalled)
	}

	// 7. Invalid cookie on protected route still 401s.
	r = httptest.NewRequest(http.MethodGet, "/api/v1/libraries", nil)
	r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "expired-stale-cookie"})
	if rec := runReq(r); rec.Code != http.StatusUnauthorized || handlerCalled {
		t.Fatalf("invalid cookie on protected route: code=%d called=%v", rec.Code, handlerCalled)
	}

	// 8. Precedence: invalid Basic + valid cookie must 401 (no cookie fallback);
	// valid Basic + valid cookie succeeds on mutation without CSRF token.
	r = httptest.NewRequest(http.MethodPost, "/api/v1/libraries", nil)
	r.SetBasicAuth("alice", "wrong-pass")
	r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "sess-valid-1"})
	r.Header.Set("X-CSRF-Token", csrfToken)
	if rec := runReq(r); rec.Code != http.StatusUnauthorized || handlerCalled {
		t.Fatalf("invalid basic + valid cookie: expected 401, got code=%d called=%v", rec.Code, handlerCalled)
	}

	r = httptest.NewRequest(http.MethodPost, "/api/v1/libraries", nil)
	r.SetBasicAuth("alice", "secret")
	r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "sess-valid-1"})
	if rec := runReq(r); rec.Code != http.StatusOK || !handlerCalled {
		t.Fatalf("valid basic + ambient cookie without CSRF: code=%d called=%v", rec.Code, handlerCalled)
	}
}

type stubPATExplicitAuth struct {
	user domain.User
}

func (a stubPATExplicitAuth) Name() string                { return "pat" }
func (a stubPATExplicitAuth) Tier() module.CredentialTier { return module.CredentialTierExplicit }
func (a stubPATExplicitAuth) Authenticate(r *http.Request) (module.AuthResult, error) {
	key := r.Header.Get("X-API-Key")
	if key == "" {
		return module.AuthPass(), nil
	}
	if key != "pat-valid-key" {
		return module.AuthResult{}, errors.New("invalid pat")
	}
	role := domain.Role{ID: "r1", Name: "ADMIN", Permissions: domain.AllPermissions()}
	pr := domain.NewPrincipal(a.user, []domain.Role{role}, domain.CredentialKindPAT, "cred-pat", nil)
	return module.AuthSuccess(pr), nil
}

type stubProxyAuth struct {
	user domain.User
}

func (a stubProxyAuth) Name() string                { return "trusted-proxy" }
func (a stubProxyAuth) Tier() module.CredentialTier { return module.CredentialTierTrustedProxy }
func (a stubProxyAuth) Authenticate(r *http.Request) (module.AuthResult, error) {
	u := r.Header.Get("X-Remote-User")
	if u == "" {
		return module.AuthPass(), nil
	}
	if u != a.user.Username {
		return module.AuthResult{}, errors.New("unknown proxy user")
	}
	role := domain.Role{ID: "r1", Name: "ADMIN", Permissions: domain.AllPermissions()}
	pr := domain.NewPrincipal(a.user, []domain.Role{role}, domain.CredentialKindPassword, "cred-proxy", nil)
	return module.AuthSuccess(pr), nil
}

func TestAuthMiddleware_BrowserOriginChecksForExplicitAndProxyAuth(t *testing.T) {
	t.Parallel()

	alice := domain.User{ID: "u-1", Username: "alice"}
	resolver := stubOriginResolver{origin: "https://comics.example.com", https: true, cookiePath: "/"}

	var handlerCalls int
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusOK)
	})

	h := Auth(AuthOptions{
		Authenticators: []module.Authenticator{
			stubBasicExplicitAuth{user: alice},
			stubPATExplicitAuth{user: alice},
			stubSessionCookieAuth{token: "sess-valid-1", csrfHash: sha256Hex("csrf-secret"), user: alice},
			stubProxyAuth{user: alice},
		},
		OriginResolver:  resolver,
		SubPathResolver: resolver,
	})(next)

	run := func(r *http.Request) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec
	}

	tiers := []struct {
		name   string
		method string
		attach func(r *http.Request)
	}{
		{
			name:   "basic",
			method: http.MethodPost,
			attach: func(r *http.Request) { r.SetBasicAuth("alice", "secret") },
		},
		{
			name:   "pat",
			method: http.MethodPut,
			attach: func(r *http.Request) { r.Header.Set("X-API-Key", "pat-valid-key") },
		},
		{
			name:   "trusted-proxy",
			method: http.MethodDelete,
			attach: func(r *http.Request) { r.Header.Set("X-Remote-User", "alice") },
		},
	}

	badHeaders := []struct {
		name   string
		origin string
		ref    string
		fetch  string
	}{
		{name: "foreign origin", origin: "https://evil.example"},
		{name: "scheme mismatch origin", origin: "http://comics.example.com"},
		{name: "null origin", origin: "null"},
		{name: "foreign referer", ref: "https://evil.example/csrf"},
		{name: "cross-site fetch", fetch: "cross-site"},
		{name: "same-site fetch", fetch: "same-site"},
	}

	for _, tier := range tiers {
		for _, hdr := range badHeaders {
			before := handlerCalls
			r := httptest.NewRequest(tier.method, "/api/v1/libraries/1", nil)
			tier.attach(r)
			if hdr.origin != "" {
				r.Header.Set("Origin", hdr.origin)
			}
			if hdr.ref != "" {
				r.Header.Set("Referer", hdr.ref)
			}
			if hdr.fetch != "" {
				r.Header.Set("Sec-Fetch-Site", hdr.fetch)
			}
			rec := run(r)
			if rec.Code != http.StatusForbidden || handlerCalls != before {
				t.Fatalf("%s + %s: expected 403 and zero handler calls, got code=%d calls=%d",
					tier.name, hdr.name, rec.Code, handlerCalls-before)
			}
		}

		// Matching same-origin browser request succeeds without X-CSRF-Token.
		before := handlerCalls
		r := httptest.NewRequest(tier.method, "/api/v1/libraries/1", nil)
		tier.attach(r)
		r.Header.Set("Origin", "https://comics.example.com")
		r.Header.Set("Referer", "https://comics.example.com/libraries")
		r.Header.Set("Sec-Fetch-Site", "same-origin")
		if rec := run(r); rec.Code != http.StatusOK || handlerCalls != before+1 {
			t.Fatalf("%s same-origin without CSRF: code=%d calls=%d", tier.name, rec.Code, handlerCalls-before)
		}

		// Header-omitting non-browser client succeeds without X-CSRF-Token.
		before = handlerCalls
		r = httptest.NewRequest(tier.method, "/api/v1/libraries/1", nil)
		tier.attach(r)
		if rec := run(r); rec.Code != http.StatusOK || handlerCalls != before+1 {
			t.Fatalf("%s non-browser omission: code=%d calls=%d", tier.name, rec.Code, handlerCalls-before)
		}
	}

	// Invalid explicit Basic or PAT must fail 401 with no cookie fallback, regardless of Origin headers.
	invalidExplicit := []struct {
		name   string
		origin string
		attach func(r *http.Request)
	}{
		{
			name:   "bad basic foreign origin",
			origin: "https://evil.example",
			attach: func(r *http.Request) {
				r.SetBasicAuth("alice", "wrong")
				r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "sess-valid-1"})
				r.Header.Set("X-CSRF-Token", "csrf-secret")
			},
		},
		{
			name:   "bad pat same origin",
			origin: "https://comics.example.com",
			attach: func(r *http.Request) {
				r.Header.Set("X-API-Key", "pat-wrong")
				r.AddCookie(&http.Cookie{Name: "tobiten_session", Value: "sess-valid-1"})
				r.Header.Set("X-CSRF-Token", "csrf-secret")
			},
		},
	}
	for _, tc := range invalidExplicit {
		before := handlerCalls
		r := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/1", nil)
		tc.attach(r)
		r.Header.Set("Origin", tc.origin)
		if rec := run(r); rec.Code != http.StatusUnauthorized || handlerCalls != before {
			t.Fatalf("%s: expected 401 without handler call, got code=%d calls=%d", tc.name, rec.Code, handlerCalls-before)
		}
	}
}
