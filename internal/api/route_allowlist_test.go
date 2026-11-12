package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/api/openapi"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
)

type tokenAuth struct {
	token     string
	principal *domain.Principal
}

func (a *tokenAuth) Name() string { return "token-auth" }

func (a *tokenAuth) Authenticate(r *http.Request) (module.AuthResult, error) {
	tok := r.Header.Get("X-Auth-Token")
	if tok == "" {
		return module.AuthPass(), nil
	}
	if tok != a.token {
		return module.AuthResult{Handled: true}, errors.New("invalid auth token")
	}
	return module.AuthSuccess(a.principal), nil
}

// TestM1AC5_RouteAllowlistWalker satisfies M1-AC5 and slice M1-T8.1 done_when:
// "The M1-AC5 route test walks every registered route and fails if any route outside
// the allowlist answers without credentials."
func TestM1AC5_RouteAllowlistWalker(t *testing.T) {
	reg := module.NewRegistry()
	_ = reg.Register(api.NewSystemModule())
	_ = reg.Register(openapi.NewModule(app.NewStubServices()))

	// Also register mock modules simulating future modules (Komga, OPDS, MCP)
	mockProtectedMod := module.NewSimpleModule("mock-protected")
	mockHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"secret_catalog_data"}`))
	})
	mockProtectedMod.Mounts = []module.RouteMount{
		{ModuleName: "mock-protected", Prefix: "/api/v1/library", Handler: mockHandler},
		{ModuleName: "mock-protected", Prefix: "/komga/api/v1", Handler: mockHandler},
		{ModuleName: "mock-protected", Prefix: "/opds/v1.2", Handler: mockHandler},
		{ModuleName: "mock-protected", Prefix: "/mcp", Handler: mockHandler},
	}
	if err := reg.Register(mockProtectedMod); err != nil {
		t.Fatalf("register mock module: %v", err)
	}

	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080",
	}

	srv, err := api.NewServer(api.ServerOptions{
		Config:   cfg,
		Registry: reg,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Run(ctx)
	}()

	baseURL := srv.URL()
	client := &http.Client{Timeout: 3 * time.Second}

	// Walk every registered route prefix from enabled modules
	routes := srv.Registry().Routes()
	if len(routes) == 0 {
		t.Fatal("expected registered routes, got none")
	}

	methods := []string{
		http.MethodGet,
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodHead,
	}

	for _, mount := range routes {
		prefix := "/" + strings.Trim(mount.Prefix, "/")
		for _, method := range methods {
			targetURL := baseURL + prefix

			req, err := http.NewRequest(method, targetURL, nil)
			if err != nil {
				t.Fatalf("NewRequest(%s, %s) failed: %v", method, targetURL, err)
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("client.Do failed for %s %s: %v", method, targetURL, err)
			}
			bodyBytes, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			// Check allowlist status
			isAllowlisted := false
			if prefix == "/healthz" && (method == http.MethodGet || method == http.MethodHead) {
				isAllowlisted = true
			} else if prefix == "/api/v1/auth/login" && method == http.MethodPost {
				isAllowlisted = true
			} else if prefix == "/api/v1/auth/setup" && method == http.MethodPost {
				isAllowlisted = true
			}

			if isAllowlisted {
				// Allowlisted route responds without credentials
				if resp.StatusCode == http.StatusUnauthorized {
					t.Errorf("allowlisted route %s %s was unexpectedly denied 401", method, prefix)
				}
				continue
			}

			// NOT allowlisted:
			// 1. Must NEVER respond with 2xx success (never answer without credentials)
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				t.Errorf("SECURITY VIOLATION: non-allowlisted route %s %s answered 2xx without credentials (status %d, body: %s)",
					method, prefix, resp.StatusCode, string(bodyBytes))
			}

			// 2. Must NEVER return an empty 200
			if resp.StatusCode == http.StatusOK {
				t.Errorf("route %s %s returned 200 OK without credentials", method, prefix)
			}

			// 3. For allowlist target paths with unallowed method, returns 405 Method Not Allowed
			if prefix == "/healthz" || prefix == "/api/v1/auth/login" || prefix == "/api/v1/auth/setup" {
				if resp.StatusCode != http.StatusMethodNotAllowed {
					t.Errorf("allowlist target with wrong method %s %s status = %d, want 405 Method Not Allowed",
						method, prefix, resp.StatusCode)
				}
				continue
			}

			// 4. All other non-allowlisted routes MUST return 401 Unauthorized with WWW-Authenticate: Basic
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("route %s %s status = %d, want 401 Unauthorized", method, prefix, resp.StatusCode)
			}

			wwwAuth := resp.Header.Get("WWW-Authenticate")
			if !strings.HasPrefix(wwwAuth, "Basic") {
				t.Errorf("route %s %s WWW-Authenticate header = %q, want Basic challenge", method, prefix, wwwAuth)
			}

			if method != http.MethodHead {
				if len(bodyBytes) == 0 {
					t.Errorf("route %s %s returned empty body on 401", method, prefix)
				}
				var errResp map[string]any
				if err := json.Unmarshal(bodyBytes, &errResp); err != nil {
					t.Errorf("route %s %s returned non-JSON body on 401: %s", method, prefix, string(bodyBytes))
				}
			}
		}
	}
}

func TestM1AC5_ExactMethodPathAllowlisting(t *testing.T) {
	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080",
	}
	srv, err := api.NewServer(api.ServerOptions{Config: cfg})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = srv.Run(ctx) }()

	baseURL := srv.URL()
	client := &http.Client{Timeout: 3 * time.Second}

	t.Run("exact /healthz allows GET and HEAD", func(t *testing.T) {
		for _, m := range []string{http.MethodGet, http.MethodHead} {
			req, _ := http.NewRequest(m, baseURL+"/healthz", nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("%s /healthz failed: %v", m, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("%s /healthz status = %d, want 200", m, resp.StatusCode)
			}
		}
	})

	t.Run("exact /healthz denies non-GET/HEAD methods with 405", func(t *testing.T) {
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			req, _ := http.NewRequest(m, baseURL+"/healthz", nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("%s /healthz failed: %v", m, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("%s /healthz status = %d, want 405", m, resp.StatusCode)
			}
			allow := resp.Header.Get("Allow")
			if !strings.Contains(allow, "GET") {
				t.Errorf("%s /healthz Allow header = %q, want it to contain GET", m, allow)
			}
		}
	})

	t.Run("descendants of /healthz are protected and return 401 without credentials", func(t *testing.T) {
		descendants := []string{
			"/healthz/private",
			"/healthz/",
			"/healthz/deep/path",
		}
		for _, path := range descendants {
			for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodHead} {
				req, _ := http.NewRequest(m, baseURL+path, nil)
				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("%s %s failed: %v", m, path, err)
				}
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusUnauthorized {
					t.Errorf("%s %s status = %d, want 401 Unauthorized", m, path, resp.StatusCode)
				}
				if auth := resp.Header.Get("WWW-Authenticate"); !strings.HasPrefix(auth, "Basic") {
					t.Errorf("%s %s WWW-Authenticate = %q, want Basic", m, path, auth)
				}
			}
		}
	})

	t.Run("descendants of auth endpoints are protected and return 401", func(t *testing.T) {
		descendants := []string{
			"/api/v1/auth/setup/child",
			"/api/v1/auth/setup/",
			"/api/v1/auth/login/secret",
			"/api/v1/auth/login/",
		}
		for _, path := range descendants {
			for _, m := range []string{http.MethodGet, http.MethodPost} {
				req, _ := http.NewRequest(m, baseURL+path, nil)
				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("%s %s failed: %v", m, path, err)
				}
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusUnauthorized {
					t.Errorf("%s %s status = %d, want 401 Unauthorized", m, path, resp.StatusCode)
				}
			}
		}
	})
}

func TestM1AC5_DirectAndConfiguredSubpathAliases(t *testing.T) {
	// BaseURL configured with sub-path /comics (ADR-0020 §6)
	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080/comics",
	}
	srv, err := api.NewServer(api.ServerOptions{Config: cfg})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = srv.Run(ctx) }()

	baseURL := srv.URL()
	client := &http.Client{Timeout: 3 * time.Second}

	t.Run("both direct and subpath /healthz return 200", func(t *testing.T) {
		for _, path := range []string{"/healthz", "/comics/healthz"} {
			resp, err := client.Get(baseURL + path)
			if err != nil {
				t.Fatalf("GET %s failed: %v", path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status = %d, want 200", path, resp.StatusCode)
			}
		}
	})

	t.Run("both direct and subpath /healthz/private return 401", func(t *testing.T) {
		for _, path := range []string{"/healthz/private", "/comics/healthz/private"} {
			resp, err := client.Get(baseURL + path)
			if err != nil {
				t.Fatalf("GET %s failed: %v", path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("GET %s status = %d, want 401", path, resp.StatusCode)
			}
		}
	})

	t.Run("both direct and subpath /api/v1/auth/setup with GET return 405", func(t *testing.T) {
		for _, path := range []string{"/api/v1/auth/setup", "/comics/api/v1/auth/setup"} {
			resp, err := client.Get(baseURL + path)
			if err != nil {
				t.Fatalf("GET %s failed: %v", path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("GET %s status = %d, want 405", path, resp.StatusCode)
			}
		}
	})

	t.Run("both direct and subpath /api/v1/auth/setup with POST reach handler (503 without services)", func(t *testing.T) {
		for _, path := range []string{"/api/v1/auth/setup", "/comics/api/v1/auth/setup"} {
			resp, err := client.Post(baseURL+path, "application/json", strings.NewReader("{}"))
			if err != nil {
				t.Fatalf("POST %s failed: %v", path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Errorf("POST %s status = %d, want 503 Service Unavailable", path, resp.StatusCode)
			}
		}
	})

	t.Run("both direct and subpath protected endpoints return 401", func(t *testing.T) {
		for _, path := range []string{"/api/v1/users", "/comics/api/v1/users"} {
			resp, err := client.Get(baseURL + path)
			if err != nil {
				t.Fatalf("GET %s failed: %v", path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("GET %s status = %d, want 401", path, resp.StatusCode)
			}
		}
	})
}

func TestM1AC5_CompositeRouting_FallbackCannotBypassAuth(t *testing.T) {
	// A root module that would return 200 OK on any request reaching it
	rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>SPA Root</html>"))
	})
	rootMod := module.NewSimpleModule("spa-root")
	rootMod.Mounts = []module.RouteMount{
		{ModuleName: "spa-root", Prefix: "/", Handler: rootHandler},
	}

	apiMux := http.NewServeMux()
	apiMux.HandleFunc("GET /api/v1/data", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	})
	apiMod := module.NewSimpleModule("api-mod")
	apiMod.Mounts = []module.RouteMount{
		{ModuleName: "api-mod", Prefix: "/api/v1", Handler: apiMux},
	}

	reg := module.NewRegistry()
	_ = reg.Register(rootMod)
	_ = reg.Register(apiMod)

	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080",
	}

	srv, err := api.NewServer(api.ServerOptions{
		Config:   cfg,
		Registry: reg,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = srv.Run(ctx) }()

	baseURL := srv.URL()
	client := &http.Client{Timeout: 3 * time.Second}

	// 1. Unauthenticated request to /api/v1/data MUST NOT fall back to SPA root (200 OK)
	resp, err := client.Get(baseURL + "/api/v1/data")
	if err != nil {
		t.Fatalf("GET /api/v1/data failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 Unauthorized (composite fallback bypassed auth! body: %s)",
			resp.StatusCode, string(body))
	}
	if strings.Contains(string(body), "SPA Root") {
		t.Errorf("composite fallback served public response without auth: %s", string(body))
	}

	// 2. Unauthenticated request to nonexistent /api/v1/missing MUST NOT fall back to SPA root (200 OK)
	respMissing, err := client.Get(baseURL + "/api/v1/missing")
	if err != nil {
		t.Fatalf("GET /api/v1/missing failed: %v", err)
	}
	bodyMissing, _ := io.ReadAll(respMissing.Body)
	_ = respMissing.Body.Close()

	if respMissing.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 Unauthorized (composite fallback bypassed auth! body: %s)",
			respMissing.StatusCode, string(bodyMissing))
	}
}

func TestM1AC5_InjectedAuthenticator_DenialAndVerifiedPrincipal(t *testing.T) {
	testUser := domain.User{ID: "usr_alice", Username: "alice"}
	testPrincipal := domain.NewPrincipal(
		testUser,
		domain.BuiltInRoles()[:1],
		domain.CredentialKindPassword,
		"cred_1",
		nil,
	)

	authenticator := &tokenAuth{
		token:     "secret-token-123",
		principal: testPrincipal,
	}

	reg := module.NewRegistry()
	_ = reg.Register(api.NewSystemModule())

	apiMod := module.NewSimpleModule("test-api")
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("GET /api/v1/me", func(w http.ResponseWriter, r *http.Request) {
		p, ok := domain.PrincipalFromContext(r.Context())
		if !ok || p == nil {
			http.Error(w, "missing principal", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":          p.User.ID,
			"username":    p.User.Username,
			"permissions": p.Permissions,
		})
	})
	apiMod.Mounts = []module.RouteMount{
		{ModuleName: "test-api", Prefix: "/api/v1/me", Handler: apiMux},
	}
	_ = reg.Register(apiMod)

	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080",
	}

	srv, err := api.NewServer(api.ServerOptions{
		Config:         cfg,
		Registry:       reg,
		Authenticators: []module.Authenticator{authenticator},
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = srv.Run(ctx) }()

	baseURL := srv.URL()
	client := &http.Client{Timeout: 3 * time.Second}

	t.Run("no credentials returns 401", func(t *testing.T) {
		resp, err := client.Get(baseURL + "/api/v1/me")
		if err != nil {
			t.Fatalf("GET /api/v1/me failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", resp.StatusCode)
		}
		if auth := resp.Header.Get("WWW-Authenticate"); !strings.HasPrefix(auth, "Basic") {
			t.Errorf("WWW-Authenticate = %q, want Basic", auth)
		}
	})

	t.Run("invalid token returns 401", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, baseURL+"/api/v1/me", nil)
		req.Header.Set("X-Auth-Token", "wrong-token")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET /api/v1/me failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", resp.StatusCode)
		}
	})

	t.Run("valid token propagates Principal to handler", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, baseURL+"/api/v1/me", nil)
		req.Header.Set("X-Auth-Token", "secret-token-123")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET /api/v1/me failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("status = %d, want 200 (body: %s)", resp.StatusCode, string(b))
		}

		var payload struct {
			ID          string              `json:"id"`
			Username    string              `json:"username"`
			Permissions []domain.Permission `json:"permissions"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			t.Fatalf("decode JSON failed: %v", err)
		}

		if payload.ID != "usr_alice" {
			t.Errorf("payload.ID = %q, want 'usr_alice'", payload.ID)
		}
		if payload.Username != "alice" {
			t.Errorf("payload.Username = %q, want 'alice'", payload.Username)
		}
		if len(payload.Permissions) == 0 {
			t.Errorf("expected permissions, got none")
		}
	})
}

func TestM1AC5_RoutingSecurityRegressions_ExemptionsBoundToPublicHandlers(t *testing.T) {
	testPrincipal := domain.NewPrincipal(
		domain.User{ID: "usr_sentinel", Username: "sentinel"},
		domain.BuiltInRoles()[:1],
		domain.CredentialKindPassword,
		"cred_sentinel",
		nil,
	)
	authenticator := &tokenAuth{
		token:     "sentinel-token",
		principal: testPrincipal,
	}

	t.Run("collision: Base /comics with protected mount /comics/healthz", func(t *testing.T) {
		reg := module.NewRegistry()
		_ = reg.Register(api.NewSystemModule())

		// Protected module mounted directly at /comics/healthz
		protectedMarker := "PROTECTED_SENTINEL_COMICS_HEALTHZ"
		protectedMod := module.NewSimpleModule("protected-comics")
		protectedMod.Mounts = []module.RouteMount{
			{
				ModuleName: "protected-comics",
				Prefix:     "/comics/healthz",
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(protectedMarker))
				}),
			},
		}
		_ = reg.Register(protectedMod)

		cfg := &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			BaseURL: "http://localhost:8080/comics",
		}
		srv, err := api.NewServer(api.ServerOptions{
			Config:         cfg,
			Registry:       reg,
			Authenticators: []module.Authenticator{authenticator},
		})
		if err != nil {
			t.Fatalf("NewServer failed: %v", err)
		}
		handler := srv.Handler()

		// 1. Unauthenticated GET /comics/healthz MUST return 401 and NEVER execute the protected marker
		reqUnauth := httptest.NewRequest(http.MethodGet, "/comics/healthz", nil)
		recUnauth := httptest.NewRecorder()
		handler.ServeHTTP(recUnauth, reqUnauth)

		if recUnauth.Code != http.StatusUnauthorized {
			t.Errorf("unauthenticated GET /comics/healthz status = %d, want 401 Unauthorized", recUnauth.Code)
		}
		if strings.Contains(recUnauth.Body.String(), protectedMarker) {
			t.Fatalf("SECURITY VIOLATION: unauthenticated GET /comics/healthz executed protected handler: %s", recUnauth.Body.String())
		}
		if auth := recUnauth.Header().Get("WWW-Authenticate"); !strings.HasPrefix(auth, "Basic") {
			t.Errorf("WWW-Authenticate = %q, want Basic", auth)
		}

		// 2. Authenticated GET /comics/healthz executes the direct protected handler (direct precedence preserved)
		reqAuth := httptest.NewRequest(http.MethodGet, "/comics/healthz", nil)
		reqAuth.Header.Set("X-Auth-Token", "sentinel-token")
		recAuth := httptest.NewRecorder()
		handler.ServeHTTP(recAuth, reqAuth)

		if recAuth.Code != http.StatusOK {
			t.Errorf("authenticated GET /comics/healthz status = %d, want 200", recAuth.Code)
		}
		if recAuth.Body.String() != protectedMarker {
			t.Errorf("authenticated GET /comics/healthz body = %q, want %q", recAuth.Body.String(), protectedMarker)
		}

		// 3. Positive control: genuine direct /healthz returns 200 ok
		reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		recHealth := httptest.NewRecorder()
		handler.ServeHTTP(recHealth, reqHealth)

		if recHealth.Code != http.StatusOK {
			t.Errorf("genuine GET /healthz status = %d, want 200", recHealth.Code)
		}
		if recHealth.Body.String() != "ok\n" {
			t.Errorf("genuine GET /healthz body = %q, want 'ok\\n'", recHealth.Body.String())
		}
	})

	t.Run("encoded base: Base /a%2Fb with protected root mount /", func(t *testing.T) {
		reg := module.NewRegistry()
		_ = reg.Register(api.NewSystemModule())

		protectedMarker := "PROTECTED_SENTINEL_ROOT"
		rootMod := module.NewSimpleModule("protected-root")
		rootMod.Mounts = []module.RouteMount{
			{
				ModuleName: "protected-root",
				Prefix:     "/",
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(protectedMarker))
				}),
			},
		}
		_ = reg.Register(rootMod)

		cfg := &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			BaseURL: "http://localhost:8080/a%2Fb",
		}
		srv, err := api.NewServer(api.ServerOptions{
			Config:         cfg,
			Registry:       reg,
			Authenticators: []module.Authenticator{authenticator},
		})
		if err != nil {
			t.Fatalf("NewServer failed: %v", err)
		}
		handler := srv.Handler()

		// 1. Unauthenticated decoded spelling GET /a/b/healthz MUST return 401 and NEVER execute protected root
		reqDecoded := httptest.NewRequest(http.MethodGet, "/a/b/healthz", nil)
		recDecoded := httptest.NewRecorder()
		handler.ServeHTTP(recDecoded, reqDecoded)

		if recDecoded.Code != http.StatusUnauthorized {
			t.Errorf("unauthenticated decoded GET /a/b/healthz status = %d, want 401", recDecoded.Code)
		}
		if strings.Contains(recDecoded.Body.String(), protectedMarker) {
			t.Fatalf("SECURITY VIOLATION: decoded /a/b/healthz executed protected handler: %s", recDecoded.Body.String())
		}

		// 2. Authenticated decoded spelling GET /a/b/healthz hits root handler with valid Principal
		reqDecodedAuth := httptest.NewRequest(http.MethodGet, "/a/b/healthz", nil)
		reqDecodedAuth.Header.Set("X-Auth-Token", "sentinel-token")
		recDecodedAuth := httptest.NewRecorder()
		handler.ServeHTTP(recDecodedAuth, reqDecodedAuth)

		if recDecodedAuth.Code != http.StatusOK {
			t.Errorf("authenticated decoded GET /a/b/healthz status = %d, want 200", recDecodedAuth.Code)
		}
		if recDecodedAuth.Body.String() != protectedMarker {
			t.Errorf("authenticated decoded body = %q, want %q", recDecodedAuth.Body.String(), protectedMarker)
		}

		// 3. Unauthenticated double-encoded spelling GET /a%252Fb/healthz MUST return 401 and NEVER execute protected root
		reqDouble := httptest.NewRequest(http.MethodGet, "/a%252Fb/healthz", nil)
		recDouble := httptest.NewRecorder()
		handler.ServeHTTP(recDouble, reqDouble)

		if recDouble.Code != http.StatusUnauthorized {
			t.Errorf("unauthenticated double-encoded GET /a%%252Fb/healthz status = %d, want 401", recDouble.Code)
		}
		if strings.Contains(recDouble.Body.String(), protectedMarker) {
			t.Fatalf("SECURITY VIOLATION: double-encoded executed protected handler: %s", recDouble.Body.String())
		}

		// 4. Authenticated double-encoded spelling GET /a%252Fb/healthz hits root handler with valid Principal
		reqDoubleAuth := httptest.NewRequest(http.MethodGet, "/a%252Fb/healthz", nil)
		reqDoubleAuth.Header.Set("X-Auth-Token", "sentinel-token")
		recDoubleAuth := httptest.NewRecorder()
		handler.ServeHTTP(recDoubleAuth, reqDoubleAuth)

		if recDoubleAuth.Code != http.StatusOK {
			t.Errorf("authenticated double-encoded status = %d, want 200", recDoubleAuth.Code)
		}
		if recDoubleAuth.Body.String() != protectedMarker {
			t.Errorf("authenticated double-encoded body = %q, want %q", recDoubleAuth.Body.String(), protectedMarker)
		}

		// 5. Positive controls: genuine escaped subpath alias /a%2Fb/healthz returns 200 ok
		reqEscaped := httptest.NewRequest(http.MethodGet, "/a%2Fb/healthz", nil)
		recEscaped := httptest.NewRecorder()
		handler.ServeHTTP(recEscaped, reqEscaped)

		if recEscaped.Code != http.StatusOK {
			t.Errorf("genuine escaped GET /a%%2Fb/healthz status = %d, want 200 (body: %s)", recEscaped.Code, recEscaped.Body.String())
		}
		if recEscaped.Body.String() != "ok\n" {
			t.Errorf("genuine escaped body = %q, want 'ok\\n'", recEscaped.Body.String())
		}

		// 6. Direct /healthz returns 200 ok
		reqDirect := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		recDirect := httptest.NewRecorder()
		handler.ServeHTTP(recDirect, reqDirect)

		if recDirect.Code != http.StatusOK {
			t.Errorf("genuine direct GET /healthz status = %d, want 200", recDirect.Code)
		}
		if recDirect.Body.String() != "ok\n" {
			t.Errorf("genuine direct body = %q, want 'ok\\n'", recDirect.Body.String())
		}
	})

	t.Run("absent or disabled public module: protected root mount /", func(t *testing.T) {
		// Case A: No OpenAPI module registered at all
		regNoOpenAPI := module.NewRegistry()
		_ = regNoOpenAPI.Register(api.NewSystemModule())

		protectedMarker := "PROTECTED_SENTINEL_ROOT"
		rootMod := module.NewSimpleModule("protected-root")
		rootMod.Mounts = []module.RouteMount{
			{
				ModuleName: "protected-root",
				Prefix:     "/",
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(protectedMarker))
				}),
			},
		}
		_ = regNoOpenAPI.Register(rootMod)

		cfg := &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			BaseURL: "http://localhost:8080",
		}
		srvNoOpenAPI, err := api.NewServer(api.ServerOptions{
			Config:         cfg,
			Registry:       regNoOpenAPI,
			Authenticators: []module.Authenticator{authenticator},
		})
		if err != nil {
			t.Fatalf("NewServer failed: %v", err)
		}
		handlerNoOpenAPI := srvNoOpenAPI.Handler()

		// 1. Unauthenticated POST /api/v1/auth/login with no OpenAPI module MUST NOT execute protected root
		for _, endpoint := range []string{"/api/v1/auth/login", "/api/v1/auth/setup"} {
			reqUnauth := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(`{}`))
			recUnauth := httptest.NewRecorder()
			handlerNoOpenAPI.ServeHTTP(recUnauth, reqUnauth)

			if recUnauth.Code != http.StatusUnauthorized {
				t.Errorf("unauthenticated POST %s (no OpenAPI) status = %d, want 401", endpoint, recUnauth.Code)
			}
			if strings.Contains(recUnauth.Body.String(), protectedMarker) {
				t.Fatalf("SECURITY VIOLATION: POST %s executed protected root handler: %s", endpoint, recUnauth.Body.String())
			}
		}

		// 2. Authenticated POST /api/v1/auth/login executes fallback root handler
		reqAuth := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{}`))
		reqAuth.Header.Set("X-Auth-Token", "sentinel-token")
		recAuth := httptest.NewRecorder()
		handlerNoOpenAPI.ServeHTTP(recAuth, reqAuth)

		if recAuth.Code != http.StatusOK {
			t.Errorf("authenticated POST /api/v1/auth/login (no OpenAPI) status = %d, want 200", recAuth.Code)
		}
		if recAuth.Body.String() != protectedMarker {
			t.Errorf("authenticated POST body = %q, want %q", recAuth.Body.String(), protectedMarker)
		}

		// Case B: OpenAPI module enabled: unauthenticated POST reaches login handler, NOT root
		regWithOpenAPI := module.NewRegistry()
		_ = regWithOpenAPI.Register(api.NewSystemModule())
		_ = regWithOpenAPI.Register(openapi.NewModule(app.NewStubServices()))
		_ = regWithOpenAPI.Register(rootMod)

		srvWithOpenAPI, err := api.NewServer(api.ServerOptions{
			Config:         cfg,
			Registry:       regWithOpenAPI,
			Authenticators: []module.Authenticator{authenticator},
		})
		if err != nil {
			t.Fatalf("NewServer failed: %v", err)
		}
		handlerWithOpenAPI := srvWithOpenAPI.Handler()

		reqLogin := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"password"}`))
		recLogin := httptest.NewRecorder()
		handlerWithOpenAPI.ServeHTTP(recLogin, reqLogin)

		// Reached OpenAPI login handler (returns 200 login response, NOT root protected marker)
		if strings.Contains(recLogin.Body.String(), protectedMarker) {
			t.Fatalf("SECURITY VIOLATION: POST /api/v1/auth/login reached root instead of OpenAPI: %s", recLogin.Body.String())
		}
		if recLogin.Code != http.StatusOK {
			t.Errorf("login status = %d, want 200 (body: %s)", recLogin.Code, recLogin.Body.String())
		}
	})
}
