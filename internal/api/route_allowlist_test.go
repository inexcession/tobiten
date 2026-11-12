package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
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
