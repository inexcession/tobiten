package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api/openapi"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
)

type passthroughAuth struct{}

func (passthroughAuth) Name() string { return "test-passthrough" }

func (passthroughAuth) Authenticate(r *http.Request) (module.AuthResult, error) {
	return module.AuthSuccess(&domain.Principal{
		User: domain.User{ID: "test-user-id", Username: "testuser"},
	}), nil
}

func TestNewServer_Validation(t *testing.T) {
	_, err := NewServer(ServerOptions{Config: nil})
	if err == nil {
		t.Errorf("expected error when Config is nil, got nil")
	}
}

func TestServer_Healthz(t *testing.T) {
	cfg := &config.Config{
		Host: "127.0.0.1",
		Port: 0,
	}
	srv, err := NewServer(ServerOptions{Config: cfg})
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

	t.Run("GET /healthz returns 200 with no version or details", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("reading body failed: %v", err)
		}
		body := string(bodyBytes)

		// Liveness only: body must be "ok\n"
		if body != "ok\n" {
			t.Errorf("body = %q, want %q", body, "ok\n")
		}

		// Must not contain version or details
		for _, forbidden := range []string{"0.1.0", "dev", "tobitend", "version", "db", "database", "postgres", "uptime", "memory"} {
			if strings.Contains(strings.ToLower(body), forbidden) {
				t.Errorf("body contains forbidden detail %q: %s", forbidden, body)
			}
		}

		// Header must not leak server version
		if s := resp.Header.Get("Server"); s != "" {
			t.Errorf("Server header must not be set, got %q", s)
		}
		if v := resp.Header.Get("X-Version"); v != "" {
			t.Errorf("X-Version header must not be set, got %q", v)
		}
	})

	t.Run("HEAD /healthz returns 200 with empty body", func(t *testing.T) {
		resp, err := http.Head(baseURL + "/healthz")
		if err != nil {
			t.Fatalf("HEAD /healthz failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if len(body) != 0 {
			t.Errorf("HEAD body length = %d, want 0", len(body))
		}
	})

	t.Run("POST /healthz returns 405 Method Not Allowed", func(t *testing.T) {
		resp, err := http.Post(baseURL+"/healthz", "text/plain", nil)
		if err != nil {
			t.Fatalf("POST /healthz failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405 Method Not Allowed", resp.StatusCode)
		}
	})
}

func TestServer_GracefulShutdown_InFlightRequestsComplete(t *testing.T) {
	cfg := &config.Config{
		Host: "127.0.0.1",
		Port: 0,
	}
	srv, err := NewServer(ServerOptions{
		Config:         cfg,
		Authenticators: []module.Authenticator{passthroughAuth{}},
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	requestStarted := make(chan struct{})
	allowRequestToFinish := make(chan struct{})

	// Register a slow test handler on the mux
	srv.Mux().HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-allowRequestToFinish
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("slow request completed"))
	})

	serverCtx, cancelServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Run(serverCtx)
	}()

	baseURL := srv.URL()

	// Start slow request in background
	type clientResult struct {
		statusCode int
		body       string
		err        error
	}
	clientCh := make(chan clientResult, 1)
	go func() {
		resp, err := http.Get(baseURL + "/slow")
		if err != nil {
			clientCh <- clientResult{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		clientCh <- clientResult{
			statusCode: resp.StatusCode,
			body:       string(b),
			err:        err,
		}
	}()

	// Wait until slow request is inside handler
	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for request to start")
	}

	// Request is in-flight! Now trigger server shutdown by cancelling server context
	cancelServer()

	// Small pause to ensure Shutdown has begun
	time.Sleep(50 * time.Millisecond)

	// Release slow request to finish
	close(allowRequestToFinish)

	// Check that the in-flight request completed successfully with 200 OK
	select {
	case res := <-clientCh:
		if res.err != nil {
			t.Fatalf("in-flight request failed: %v", res.err)
		}
		if res.statusCode != http.StatusOK {
			t.Errorf("in-flight request status = %d, want 200", res.statusCode)
		}
		if res.body != "slow request completed" {
			t.Errorf("in-flight request body = %q, want 'slow request completed'", res.body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for in-flight request to finish")
	}

	// Check that srv.Run returned cleanly with nil
	select {
	case err := <-serverDone:
		if err != nil {
			t.Errorf("srv.Run() returned error: %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server to shut down")
	}

	// Verify that new requests are rejected after shutdown
	_, err = http.Get(baseURL + "/healthz")
	if err == nil {
		t.Errorf("expected error connecting to shut down server, got nil")
	}
}

func TestServer_RequestLoggingIntegration(t *testing.T) {
	var logBuf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := &config.Config{
		Host: "127.0.0.1",
		Port: 0,
	}
	srv, err := NewServer(ServerOptions{
		Config: cfg,
		Logger: testLogger,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Run(ctx)
	}()

	client := &http.Client{}
	req, err := http.NewRequest(http.MethodGet, srv.URL()+"/healthz?token=supersecret_token&apiKey=secret123", nil)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	req.Header.Set("Authorization", "Bearer topsecret_pat")
	req.Header.Set("Cookie", "session=topsecret_cookie")
	req.Header.Set("X-API-Key", "topsecret_apikey")
	req.Header.Set("X-Auth-Token", "topsecret_authtoken")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do failed: %v", err)
	}
	_ = resp.Body.Close()

	output := logBuf.String()

	// Parse JSON lines to find the "http request" record
	var requestLog map[string]any
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for _, line := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err == nil {
			if m["msg"] == "http request" {
				requestLog = m
				break
			}
		}
	}

	if requestLog == nil {
		t.Fatalf("expected 'http request' log record in output:\n%s", output)
	}

	// Check path is /healthz without query string
	if requestLog["path"] != "/healthz" {
		t.Errorf("path = %v, want '/healthz'", requestLog["path"])
	}

	// Check none of the secrets or query parameters leaked
	for _, secret := range []string{
		"supersecret_token",
		"secret123",
		"topsecret_pat",
		"topsecret_cookie",
		"topsecret_apikey",
		"topsecret_authtoken",
		"token=",
		"apiKey=",
	} {
		if strings.Contains(output, secret) {
			t.Errorf("log output leaked %q:\n%s", secret, output)
		}
	}
}

func TestServer_CustomListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	defer ln.Close()

	cfg := &config.Config{
		Host: "127.0.0.1",
		Port: 8080, // should be ignored in favor of custom listener
	}
	srv, err := NewServer(ServerOptions{
		Config:   cfg,
		Listener: ln,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	if srv.Addr() != ln.Addr().String() {
		t.Errorf("srv.Addr() = %q, want %q", srv.Addr(), ln.Addr().String())
	}
}

func TestServer_AbsoluteURLBuilder_ForwardedHeadersIgnored(t *testing.T) {
	testCases := []struct {
		name         string
		configured   string
		expectedRoot string
	}{
		{
			name:         "root base http",
			configured:   "http://localhost:8080",
			expectedRoot: "http://localhost:8080",
		},
		{
			name:         "sub-path base https",
			configured:   "https://books.example.com/comics",
			expectedRoot: "https://books.example.com/comics",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				Host:    "127.0.0.1",
				Port:    0,
				BaseURL: tc.configured,
			}
			srv, err := NewServer(ServerOptions{
				Config:         cfg,
				Authenticators: []module.Authenticator{passthroughAuth{}},
			})
			if err != nil {
				t.Fatalf("NewServer failed: %v", err)
			}

			// Verify Server methods
			if srv.BaseURL() != tc.expectedRoot {
				t.Errorf("srv.BaseURL() = %q, want %q", srv.BaseURL(), tc.expectedRoot)
			}
			if srv.URLBuilder().BaseURL() != tc.expectedRoot {
				t.Errorf("srv.URLBuilder().BaseURL() = %q, want %q", srv.URLBuilder().BaseURL(), tc.expectedRoot)
			}

			// Verify PublicURL builds on BaseURL, never on listener address srv.URL()
			setupPublic := srv.PublicURL("/api/v1/auth/setup")
			expectedSetup := tc.expectedRoot + "/api/v1/auth/setup"
			if setupPublic != expectedSetup {
				t.Errorf("srv.PublicURL() = %q, want %q", setupPublic, expectedSetup)
			}
			if strings.HasPrefix(setupPublic, srv.URL()) {
				t.Errorf("srv.PublicURL() must not use local listener address srv.URL() (%q)", srv.URL())
			}

			// Register a test route that exercises link generation in request context
			type linkPayload struct {
				SetupURL  string `json:"setup_url"`
				OPDSURL   string `json:"opds_url"`
				SeriesURL string `json:"series_url"`
			}

			srv.Mux().HandleFunc("GET /test/links", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				payload := linkPayload{
					SetupURL:  srv.PublicURL("/api/v1/auth/setup"),
					OPDSURL:   URLFromRequest(r, "/komga/opds/v1.2/catalog"),
					SeriesURL: URLBuilderFromRequest(r).URL("/komga/api/v1/series?page=0&size=20"),
				}
				_ = json.NewEncoder(w).Encode(payload)
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			serverDone := make(chan error, 1)
			go func() {
				serverDone <- srv.Run(ctx)
			}()

			// Send HTTP request to live server listener with aggressive forwarded headers
			req, err := http.NewRequest(http.MethodGet, srv.URL()+"/test/links", nil)
			if err != nil {
				t.Fatalf("http.NewRequest failed: %v", err)
			}
			req.Host = "internal-docker-host:8080"
			req.Header.Set("X-Forwarded-Host", "attacker.evil.com")
			req.Header.Set("X-Forwarded-Proto", "https")
			req.Header.Set("X-Forwarded-For", "203.0.113.195, 70.41.3.18")
			req.Header.Set("X-Forwarded-Port", "9999")
			req.Header.Set("X-Forwarded-Prefix", "/spoofed-prefix")
			req.Header.Set("Forwarded", "for=192.0.2.60;proto=https;by=203.0.113.43;host=forwarded.example.org")

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("HTTP request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}

			var payload linkPayload
			if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
				t.Fatalf("decoding response failed: %v", err)
			}

			// Validate all links match configured BaseURL exactly
			wantSetup := tc.expectedRoot + "/api/v1/auth/setup"
			wantOPDS := tc.expectedRoot + "/komga/opds/v1.2/catalog"
			wantSeries := tc.expectedRoot + "/komga/api/v1/series?page=0&size=20"

			if payload.SetupURL != wantSetup {
				t.Errorf("SetupURL = %q, want %q (forwarded headers were consulted!)", payload.SetupURL, wantSetup)
			}
			if payload.OPDSURL != wantOPDS {
				t.Errorf("OPDSURL = %q, want %q (forwarded headers were consulted!)", payload.OPDSURL, wantOPDS)
			}
			if payload.SeriesURL != wantSeries {
				t.Errorf("SeriesURL = %q, want %q (forwarded headers were consulted!)", payload.SeriesURL, wantSeries)
			}

			// Verify hostile header values are completely absent from response
			for _, forbidden := range []string{
				"attacker.evil.com",
				"internal-docker-host",
				"spoofed-prefix",
				"forwarded.example.org",
				"9999",
			} {
				if strings.Contains(payload.SetupURL, forbidden) ||
					strings.Contains(payload.OPDSURL, forbidden) ||
					strings.Contains(payload.SeriesURL, forbidden) {
					t.Errorf("response leaked forwarded/proxy header value %q", forbidden)
				}
			}

			cancel()
			select {
			case err := <-serverDone:
				if err != nil {
					t.Fatalf("srv.Run failed: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("server failed to shutdown within timeout")
			}
		})
	}
}

func TestServer_ModuleRouteLifecycle(t *testing.T) {
	// done_when: A test module registers a route that answers when enabled
	// and is absent when disabled by config.

	newTestModule := func() *module.SimpleModule {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("hello from greeter module"))
		})
		mod := module.NewSimpleModule("greeter-module")
		mod.Mounts = []module.RouteMount{
			{Prefix: "/greeter", Handler: handler},
		}
		mod.Auths = []module.Authenticator{passthroughAuth{}}
		return mod
	}

	t.Run("module route answers 200 when enabled", func(t *testing.T) {
		reg := module.NewRegistry()
		if err := reg.Register(newTestModule()); err != nil {
			t.Fatalf("Register error: %v", err)
		}

		cfg := &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			BaseURL: "http://localhost:8080",
		}
		srv, err := NewServer(ServerOptions{
			Config:         cfg,
			Registry:       reg,
			Authenticators: []module.Authenticator{passthroughAuth{}},
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

		// Verify module route answers 200 OK
		resp, err := http.Get(baseURL + "/greeter/hello")
		if err != nil {
			t.Fatalf("GET /greeter/hello failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "hello from greeter module" {
			t.Errorf("body = %q, want %q", string(body), "hello from greeter module")
		}

		// Bare prefix /greeter also answers 200 OK
		respBare, err := http.Get(baseURL + "/greeter")
		if err != nil {
			t.Fatalf("GET /greeter failed: %v", err)
		}
		_ = respBare.Body.Close()
		if respBare.StatusCode != http.StatusOK {
			t.Errorf("bare prefix status = %d, want 200", respBare.StatusCode)
		}

		// Core healthz route still works
		respHealth, err := http.Get(baseURL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz failed: %v", err)
		}
		_ = respHealth.Body.Close()
		if respHealth.StatusCode != http.StatusOK {
			t.Errorf("healthz status = %d, want 200", respHealth.StatusCode)
		}
	})

	t.Run("module route is absent (404) when disabled by config", func(t *testing.T) {
		reg := module.NewRegistry()
		if err := reg.Register(newTestModule()); err != nil {
			t.Fatalf("Register error: %v", err)
		}

		cfg := &config.Config{
			Host:            "127.0.0.1",
			Port:            0,
			BaseURL:         "http://localhost:8080",
			DisabledModules: []string{"greeter-module"},
		}
		srv, err := NewServer(ServerOptions{
			Config:         cfg,
			Registry:       reg,
			Authenticators: []module.Authenticator{passthroughAuth{}},
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

		// Verify module route is absent (404 Not Found)
		resp, err := http.Get(baseURL + "/greeter/hello")
		if err != nil {
			t.Fatalf("GET /greeter/hello failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("status = %d, want 404 (route must be absent when disabled)", resp.StatusCode)
		}

		// Bare prefix /greeter also absent
		respBare, err := http.Get(baseURL + "/greeter")
		if err != nil {
			t.Fatalf("GET /greeter failed: %v", err)
		}
		_ = respBare.Body.Close()
		if respBare.StatusCode != http.StatusNotFound {
			t.Errorf("bare prefix status = %d, want 404", respBare.StatusCode)
		}

		// Core healthz route still answers 200 OK
		respHealth, err := http.Get(baseURL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz failed: %v", err)
		}
		_ = respHealth.Body.Close()
		if respHealth.StatusCode != http.StatusOK {
			t.Errorf("healthz status = %d, want 200", respHealth.StatusCode)
		}
	})

	t.Run("sub-path mount agreement and reverse proxy requests (ADR-0020 §6)", func(t *testing.T) {
		reg := module.NewRegistry()
		if err := reg.Register(newTestModule()); err != nil {
			t.Fatalf("Register error: %v", err)
		}

		cfg := &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			BaseURL: "http://localhost:8080/tobiten",
		}
		srv, err := NewServer(ServerOptions{
			Config:         cfg,
			Registry:       reg,
			Authenticators: []module.Authenticator{passthroughAuth{}},
		})
		if err != nil {
			t.Fatalf("NewServer failed: %v", err)
		}

		// Verify public URL generated with subpath
		wantPublicURL := "http://localhost:8080/tobiten/greeter/hello"
		if got := srv.PublicURL("/greeter/hello"); got != wantPublicURL {
			t.Errorf("PublicURL = %q, want %q", got, wantPublicURL)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		serverDone := make(chan error, 1)
		go func() {
			serverDone <- srv.Run(ctx)
		}()

		baseURL := srv.URL()

		// 1. Request via sub-path (as forwarded by plain reverse proxy)
		respSub, err := http.Get(baseURL + "/tobiten/greeter/hello")
		if err != nil {
			t.Fatalf("GET /tobiten/greeter/hello failed: %v", err)
		}
		defer respSub.Body.Close()

		if respSub.StatusCode != http.StatusOK {
			t.Errorf("sub-path request status = %d, want 200", respSub.StatusCode)
		}
		bodySub, _ := io.ReadAll(respSub.Body)
		if string(bodySub) != "hello from greeter module" {
			t.Errorf("sub-path body = %q, want %q", string(bodySub), "hello from greeter module")
		}

		// 2. Direct un-prefixed request also succeeds
		respDirect, err := http.Get(baseURL + "/greeter/hello")
		if err != nil {
			t.Fatalf("GET /greeter/hello failed: %v", err)
		}
		_ = respDirect.Body.Close()
		if respDirect.StatusCode != http.StatusOK {
			t.Errorf("direct request status = %d, want 200", respDirect.StatusCode)
		}

		// 3. Sub-path healthz request succeeds
		respSubHealth, err := http.Get(baseURL + "/tobiten/healthz")
		if err != nil {
			t.Fatalf("GET /tobiten/healthz failed: %v", err)
		}
		_ = respSubHealth.Body.Close()
		if respSubHealth.StatusCode != http.StatusOK {
			t.Errorf("sub-path healthz status = %d, want 200", respSubHealth.StatusCode)
		}
	})

	newRootModule := func() *module.SimpleModule {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("hello from root module at " + r.URL.Path))
		})
		mod := module.NewSimpleModule("root-module")
		mod.Mounts = []module.RouteMount{
			{Prefix: "/", Handler: handler},
		}
		return mod
	}

	t.Run("encoded %20 base URL regression with root and prefixed mounts", func(t *testing.T) {
		reg := module.NewRegistry()
		if err := reg.Register(newTestModule()); err != nil {
			t.Fatalf("Register(greeter) error: %v", err)
		}
		if err := reg.Register(newRootModule()); err != nil {
			t.Fatalf("Register(root) error: %v", err)
		}

		cfg := &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			BaseURL: "http://localhost:8080/my%20base",
		}
		srv, err := NewServer(ServerOptions{
			Config:         cfg,
			Registry:       reg,
			Authenticators: []module.Authenticator{passthroughAuth{}},
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

		// 1. Prefixed module route under %20 base
		for _, target := range []string{
			"/my%20base/greeter",
			"/my%20base/greeter/",
			"/my%20base/greeter/hello",
		} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status = %d, want 200", target, resp.StatusCode)
			}
		}

		// 2. Direct un-prefixed route
		respDirect, err := http.Get(baseURL + "/greeter/hello")
		if err != nil {
			t.Fatalf("GET /greeter/hello failed: %v", err)
		}
		_ = respDirect.Body.Close()
		if respDirect.StatusCode != http.StatusOK {
			t.Errorf("GET /greeter/hello status = %d, want 200", respDirect.StatusCode)
		}

		// 3. Root module route under %20 base (both bare and with sub-paths)
		for _, target := range []string{
			"/my%20base",
			"/my%20base/",
			"/my%20base/root-item",
		} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status = %d, want 200", target, resp.StatusCode)
			}
		}

		// 4. Healthz under %20 base and at root
		for _, target := range []string{"/my%20base/healthz", "/healthz"} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status = %d, want 200", target, resp.StatusCode)
			}
		}
	})

	t.Run("encoded %20 base URL with config-disabled modules", func(t *testing.T) {
		reg := module.NewRegistry()
		if err := reg.Register(newTestModule()); err != nil {
			t.Fatalf("Register(greeter) error: %v", err)
		}
		if err := reg.Register(newRootModule()); err != nil {
			t.Fatalf("Register(root) error: %v", err)
		}

		cfg := &config.Config{
			Host:            "127.0.0.1",
			Port:            0,
			BaseURL:         "http://localhost:8080/my%20base",
			DisabledModules: []string{"greeter-module", "root-module"},
		}
		srv, err := NewServer(ServerOptions{
			Config:         cfg,
			Registry:       reg,
			Authenticators: []module.Authenticator{passthroughAuth{}},
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

		// Disabled routes must return 404
		for _, target := range []string{
			"/my%20base/greeter",
			"/my%20base/greeter/hello",
			"/greeter/hello",
			"/my%20base/root-item",
		} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("GET %s status = %d, want 404 for disabled module", target, resp.StatusCode)
			}
		}

		// Core healthz must still answer 200 OK
		for _, target := range []string{"/my%20base/healthz", "/healthz"} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status = %d, want 200", target, resp.StatusCode)
			}
		}
	})

	t.Run("encoded %2F base URL regression with root and prefixed mounts", func(t *testing.T) {
		reg := module.NewRegistry()
		if err := reg.Register(newTestModule()); err != nil {
			t.Fatalf("Register(greeter) error: %v", err)
		}
		if err := reg.Register(newRootModule()); err != nil {
			t.Fatalf("Register(root) error: %v", err)
		}

		cfg := &config.Config{
			Host:    "127.0.0.1",
			Port:    0,
			BaseURL: "http://localhost:8080/a%2Fb",
		}
		srv, err := NewServer(ServerOptions{
			Config:         cfg,
			Registry:       reg,
			Authenticators: []module.Authenticator{passthroughAuth{}},
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

		// 1. Prefixed module route under %2F base
		for _, target := range []string{
			"/a%2Fb/greeter",
			"/a%2Fb/greeter/",
			"/a%2Fb/greeter/hello",
		} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status = %d, want 200", target, resp.StatusCode)
			}
		}

		// 2. Direct un-prefixed route
		respDirect, err := http.Get(baseURL + "/greeter/hello")
		if err != nil {
			t.Fatalf("GET /greeter/hello failed: %v", err)
		}
		_ = respDirect.Body.Close()
		if respDirect.StatusCode != http.StatusOK {
			t.Errorf("GET /greeter/hello status = %d, want 200", respDirect.StatusCode)
		}

		// 3. Root module route under %2F base
		for _, target := range []string{
			"/a%2Fb",
			"/a%2Fb/",
			"/a%2Fb/root-item",
		} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status = %d, want 200", target, resp.StatusCode)
			}
		}

		// 4. Healthz under %2F base and at root
		for _, target := range []string{"/a%2Fb/healthz", "/healthz"} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status = %d, want 200", target, resp.StatusCode)
			}
		}
	})

	t.Run("encoded %2F base URL with config-disabled modules", func(t *testing.T) {
		reg := module.NewRegistry()
		if err := reg.Register(newTestModule()); err != nil {
			t.Fatalf("Register(greeter) error: %v", err)
		}
		if err := reg.Register(newRootModule()); err != nil {
			t.Fatalf("Register(root) error: %v", err)
		}

		cfg := &config.Config{
			Host:            "127.0.0.1",
			Port:            0,
			BaseURL:         "http://localhost:8080/a%2Fb",
			DisabledModules: []string{"greeter-module", "root-module"},
		}
		srv, err := NewServer(ServerOptions{
			Config:         cfg,
			Registry:       reg,
			Authenticators: []module.Authenticator{passthroughAuth{}},
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

		// Disabled routes must return 404
		for _, target := range []string{
			"/a%2Fb/greeter",
			"/a%2Fb/greeter/hello",
			"/greeter/hello",
			"/a%2Fb/root-item",
		} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("GET %s status = %d, want 404 for disabled module", target, resp.StatusCode)
			}
		}

		// Core healthz must still answer 200 OK
		for _, target := range []string{"/a%2Fb/healthz", "/healthz"} {
			resp, err := http.Get(baseURL + target)
			if err != nil {
				t.Fatalf("GET %s failed: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status = %d, want 200", target, resp.StatusCode)
			}
		}
	})
}

func TestServer_DefaultWiringFailClosed(t *testing.T) {
	// Daemon and nil-service defaults must fail closed/unavailable until real services land
	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080",
	}
	srv, err := NewServer(ServerOptions{Config: cfg})
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

	t.Run("GET /healthz resolves through registry", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "ok\n" {
			t.Errorf("body = %q, want 'ok\\n'", string(body))
		}
	})

	t.Run("POST /healthz returns 405 Method Not Allowed", func(t *testing.T) {
		resp, err := http.Post(baseURL+"/healthz", "text/plain", nil)
		if err != nil {
			t.Fatalf("POST /healthz failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405 Method Not Allowed", resp.StatusCode)
		}
	})

	t.Run("anonymous GET /api/v1/auth/setup is denied (405)", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/api/v1/auth/setup")
		if err != nil {
			t.Fatalf("GET /api/v1/auth/setup failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405 Method Not Allowed (denied)", resp.StatusCode)
		}
	})

	t.Run("anonymous HEAD /api/v1/auth/setup is denied (405)", func(t *testing.T) {
		resp, err := http.Head(baseURL + "/api/v1/auth/setup")
		if err != nil {
			t.Fatalf("HEAD /api/v1/auth/setup failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405 Method Not Allowed (denied)", resp.StatusCode)
		}
	})

	t.Run("POST /api/v1/auth/setup fails closed (503 Service Unavailable)", func(t *testing.T) {
		setupPayload := `{"setup_token":"token-123","username":"admin","password":"secretpassword"}`
		resp, err := http.Post(baseURL+"/api/v1/auth/setup", "application/json", strings.NewReader(setupPayload))
		if err != nil {
			t.Fatalf("POST /api/v1/auth/setup failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusServiceUnavailable {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("status = %d, want 503 Service Unavailable (body: %s)", resp.StatusCode, string(body))
		}
	})

	t.Run("POST /api/v1/auth/login fails closed (503 Service Unavailable)", func(t *testing.T) {
		loginPayload := `{"username":"admin","password":"secretpassword"}`
		resp, err := http.Post(baseURL+"/api/v1/auth/login", "application/json", strings.NewReader(loginPayload))
		if err != nil {
			t.Fatalf("POST /api/v1/auth/login failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusServiceUnavailable {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("status = %d, want 503 Service Unavailable (body: %s)", resp.StatusCode, string(body))
		}
	})
}

func TestServer_ExplicitFakeServiceWiring(t *testing.T) {
	// Permissive auth/setup fakes are explicitly test-injected
	reg := module.NewRegistry()
	_ = reg.Register(NewSystemModule())
	_ = reg.Register(openapi.NewModule(app.NewStubServices()))

	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080",
	}
	srv, err := NewServer(ServerOptions{
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

	t.Run("GET /healthz returns 200 OK", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
	})

	t.Run("GET /api/v1/auth/setup denied (not allowlisted)", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/api/v1/auth/setup")
		if err != nil {
			t.Fatalf("GET /api/v1/auth/setup failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405 Method Not Allowed", resp.StatusCode)
		}
	})

	t.Run("HEAD /api/v1/auth/setup denied (not allowlisted)", func(t *testing.T) {
		resp, err := http.Head(baseURL + "/api/v1/auth/setup")
		if err != nil {
			t.Fatalf("HEAD /api/v1/auth/setup failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405 Method Not Allowed", resp.StatusCode)
		}
	})

	t.Run("POST /api/v1/auth/setup creates admin and seals", func(t *testing.T) {
		setupPayload := `{"setup_token":"token-123","username":"admin","password":"secretpassword"}`
		resp, err := http.Post(baseURL+"/api/v1/auth/setup", "application/json", strings.NewReader(setupPayload))
		if err != nil {
			t.Fatalf("POST /api/v1/auth/setup failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("status = %d, want 201 Created (body: %s)", resp.StatusCode, string(body))
		}

		// Subsequent setup attempt must fail with 403 Forbidden (sealed)
		resp2, err := http.Post(baseURL+"/api/v1/auth/setup", "application/json", strings.NewReader(setupPayload))
		if err != nil {
			t.Fatalf("POST /api/v1/auth/setup 2nd attempt failed: %v", err)
		}
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusForbidden {
			t.Errorf("second setup attempt status = %d, want 403 Forbidden", resp2.StatusCode)
		}
	})

	t.Run("POST /api/v1/auth/login succeeds with credentials", func(t *testing.T) {
		loginPayload := `{"username":"admin","password":"secretpassword"}`
		resp, err := http.Post(baseURL+"/api/v1/auth/login", "application/json", strings.NewReader(loginPayload))
		if err != nil {
			t.Fatalf("POST /api/v1/auth/login failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("status = %d, want 200 OK (body: %s)", resp.StatusCode, string(body))
		}
	})

	t.Run("POST /api/v1/auth/login fails on empty credentials", func(t *testing.T) {
		loginPayload := `{"username":"","password":""}`
		resp, err := http.Post(baseURL+"/api/v1/auth/login", "application/json", strings.NewReader(loginPayload))
		if err != nil {
			t.Fatalf("POST /api/v1/auth/login failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d, want 400 Bad Request", resp.StatusCode)
		}
	})
}

func TestServer_DisablingOpenAPIModule(t *testing.T) {
	cfg := &config.Config{
		Host:            "127.0.0.1",
		Port:            0,
		BaseURL:         "http://localhost:8080",
		DisabledModules: []string{"openapi"},
	}
	srv, err := NewServer(ServerOptions{Config: cfg})
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

	// /healthz (system module) still works
	respHealth, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	_ = respHealth.Body.Close()
	if respHealth.StatusCode != http.StatusOK {
		t.Errorf("healthz status = %d, want 200", respHealth.StatusCode)
	}

	// /api/v1/auth/login returns 404 (disabled module)
	respLogin, err := http.Post(baseURL+"/api/v1/auth/login", "application/json", strings.NewReader(`{"username":"admin","password":"pwd"}`))
	if err != nil {
		t.Fatalf("POST /api/v1/auth/login failed: %v", err)
	}
	_ = respLogin.Body.Close()
	if respLogin.StatusCode != http.StatusNotFound {
		t.Errorf("disabled openapi route status = %d, want 404", respLogin.StatusCode)
	}
}

func TestServer_DuplicatePatternComposition(t *testing.T) {
	// Tests the composition of root module (prefix: "/") and an API module (prefix: "/api")
	// when BaseURL has sub-path "/api". Both modules attempt to claim pattern "/api" and "/api/".
	// The server must compose without panicking, prioritizing direct handlers before subpath fallbacks.
	reg := module.NewRegistry()

	rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("root module: " + r.URL.Path))
	})
	rootMod := module.NewSimpleModule("root-module")
	rootMod.Mounts = []module.RouteMount{
		{ModuleName: "root-module", Prefix: "/", Handler: rootHandler},
	}
	if err := reg.Register(rootMod); err != nil {
		t.Fatalf("register rootMod failed: %v", err)
	}

	apiMux := http.NewServeMux()
	apiMux.HandleFunc("GET /api/specific", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("api module: specific endpoint"))
	})
	apiMod := module.NewSimpleModule("api-module")
	apiMod.Mounts = []module.RouteMount{
		{ModuleName: "api-module", Prefix: "/api", Handler: apiMux},
	}
	if err := reg.Register(apiMod); err != nil {
		t.Fatalf("register apiMod failed: %v", err)
	}

	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080/api",
	}

	// Must NOT panic on NewServer despite duplicate "/api" and "/api/" patterns
	srv, err := NewServer(ServerOptions{
		Config:         cfg,
		Registry:       reg,
		Authenticators: []module.Authenticator{passthroughAuth{}},
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

	// 1. Direct request to API module's specific endpoint
	respSpecific, err := http.Get(baseURL + "/api/specific")
	if err != nil {
		t.Fatalf("GET /api/specific failed: %v", err)
	}
	defer respSpecific.Body.Close()
	if respSpecific.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", respSpecific.StatusCode)
	}
	bodySpecific, _ := io.ReadAll(respSpecific.Body)
	if string(bodySpecific) != "api module: specific endpoint" {
		t.Errorf("body = %q, want 'api module: specific endpoint'", string(bodySpecific))
	}

	// 2. Request to non-API route under /api falls back to root module
	respFallback, err := http.Get(baseURL + "/api/app.js")
	if err != nil {
		t.Fatalf("GET /api/app.js failed: %v", err)
	}
	defer respFallback.Body.Close()
	if respFallback.StatusCode != http.StatusOK {
		t.Errorf("fallback status = %d, want 200", respFallback.StatusCode)
	}
	bodyFallback, _ := io.ReadAll(respFallback.Body)
	if !strings.Contains(string(bodyFallback), "root module") {
		t.Errorf("expected root module response, got %q", string(bodyFallback))
	}

	// 3. Direct request to root module works
	respRoot, err := http.Get(baseURL + "/something-else")
	if err != nil {
		t.Fatalf("GET /something-else failed: %v", err)
	}
	_ = respRoot.Body.Close()
	if respRoot.StatusCode != http.StatusOK {
		t.Errorf("root status = %d, want 200", respRoot.StatusCode)
	}
	if err := serverTestCleanupCheck(t, cancel, serverDone); err != nil {
		t.Fatal(err)
	}
}

func serverTestCleanupCheck(t *testing.T, cancel context.CancelFunc, serverDone <-chan error) error {
	t.Helper()
	cancel()
	select {
	case err := <-serverDone:
		return err
	case <-time.After(3 * time.Second):
		return context.DeadlineExceeded
	}
}

type streamingStubEventService struct {
	attached chan struct{}
	sendCh   chan app.StreamEvent
}

func (s *streamingStubEventService) Stream(
	ctx context.Context,
	p *domain.Principal,
	_ app.EventStreamFilter,
	onAttached func() error,
	emit func(app.StreamEvent) error,
) error {
	if p == nil {
		return app.ErrUnauthorized
	}
	if onAttached != nil {
		if err := onAttached(); err != nil {
			return err
		}
	}
	select {
	case s.attached <- struct{}{}:
	default:
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-s.sendCh:
			if err := emit(ev); err != nil {
				return err
			}
		}
	}
}

type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestServer_CompositeRouteCollision_StreamingSSEFlushAndLogging(t *testing.T) {
	// Exercise colliding modules sharing the "/api/v1" pattern in both orders:
	// 1) OpenAPI first, colliding module second (OpenAPI runs inside fallbackRecorder).
	// 2) Colliding module returning 404 first, OpenAPI second (fallbackRecorder after 404).
	for _, tc := range []struct {
		name         string
		openapiFirst bool
	}{
		{name: "openapi first under composite fallbackRecorder", openapiFirst: true},
		{name: "colliding 404 module first then openapi", openapiFirst: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf syncLogBuffer
			testLogger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			stubEvents := &streamingStubEventService{
				attached: make(chan struct{}, 1),
				sendCh:   make(chan app.StreamEvent, 4),
			}
			openapiMod := openapi.NewModule(&app.Services{
				Events: stubEvents,
			})

			colliderMux := http.NewServeMux()
			colliderMux.HandleFunc("GET /api/v1/collider", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("from-collider"))
			})
			colliderMod := module.NewSimpleModule("collider-api")
			colliderMod.Mounts = []module.RouteMount{
				{ModuleName: "collider-api", Prefix: "/api/v1", Handler: colliderMux},
			}

			reg := module.NewRegistry()
			_ = reg.Register(NewSystemModule())
			if tc.openapiFirst {
				_ = reg.Register(openapiMod)
				_ = reg.Register(colliderMod)
			} else {
				_ = reg.Register(colliderMod)
				_ = reg.Register(openapiMod)
			}

			cfg := &config.Config{
				Host:    "127.0.0.1",
				Port:    0,
				BaseURL: "http://localhost:8080",
			}
			srv, err := NewServer(ServerOptions{
				Config:         cfg,
				Logger:         testLogger,
				Registry:       reg,
				Authenticators: []module.Authenticator{passthroughAuth{}},
			})
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			serverDone := make(chan error, 1)
			go func() {
				serverDone <- srv.Run(ctx)
			}()
			defer func() {
				_ = serverTestCleanupCheck(t, cancel, serverDone)
			}()

			baseURL := srv.URL()
			client := &http.Client{Timeout: 5 * time.Second}

			// Verify fallback routing still reaches collider endpoint.
			respCol, err := client.Get(baseURL + "/api/v1/collider")
			if err != nil {
				t.Fatalf("GET /api/v1/collider: %v", err)
			}
			colBody, _ := io.ReadAll(respCol.Body)
			_ = respCol.Body.Close()
			if respCol.StatusCode != http.StatusOK || string(colBody) != "from-collider" {
				t.Fatalf("collider response = (%d, %q), want (200, from-collider)", respCol.StatusCode, string(colBody))
			}

			// Open SSE stream over real TCP connection without timeout on body reads.
			streamCtx, cancelStream := context.WithCancel(context.Background())
			defer cancelStream()
			req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, baseURL+"/api/v1/events", nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			streamClient := &http.Client{}
			resp, err := streamClient.Do(req)
			if err != nil {
				t.Fatalf("GET /api/v1/events: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				_ = resp.Body.Close()
				t.Fatalf("SSE status = %d, want 200", resp.StatusCode)
			}
			if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
				_ = resp.Body.Close()
				t.Fatalf("SSE Content-Type = %q, want text/event-stream", ct)
			}

			select {
			case <-stubEvents.attached:
			case <-time.After(2 * time.Second):
				_ = resp.Body.Close()
				t.Fatal("timed out waiting for SSE handler attach")
			}

			// Send an event while the stream remains open and verify the frame flushes immediately.
			stubEvents.sendCh <- app.StreamEvent{
				ID:   "7",
				Name: "job.started",
				Data: json.RawMessage(`{"sequence":7}`),
			}

			lineCh := make(chan string, 16)
			readErrCh := make(chan error, 1)
			go func() {
				buf := make([]byte, 4096)
				for {
					n, rErr := resp.Body.Read(buf)
					if n > 0 {
						lineCh <- string(buf[:n])
					}
					if rErr != nil {
						readErrCh <- rErr
						return
					}
				}
			}()

			var received strings.Builder
			deadline := time.After(2 * time.Second)
			for !strings.Contains(received.String(), "\n\n") {
				select {
				case chunk := <-lineCh:
					received.WriteString(chunk)
				case rErr := <-readErrCh:
					t.Fatalf("SSE stream ended early (%v) with partial data %q", rErr, received.String())
				case <-deadline:
					t.Fatalf("timed out waiting for flushed SSE frame while stream open; got %q", received.String())
				}
			}
			gotFrame := received.String()
			if !strings.Contains(gotFrame, "id: 7\n") || !strings.Contains(gotFrame, "event: job.started\n") || !strings.Contains(gotFrame, `data: {"sequence":7}`) {
				t.Fatalf("unexpected flushed SSE frame: %q", gotFrame)
			}

			// Close client stream and verify RequestLogger records final status 200 and bytes > 0.
			cancelStream()
			_ = resp.Body.Close()

			logDeadline := time.Now().Add(2 * time.Second)
			var foundEventsLog map[string]any
			for time.Now().Before(logDeadline) {
				for line := range strings.SplitSeq(strings.TrimSpace(logBuf.String()), "\n") {
					var m map[string]any
					if err := json.Unmarshal([]byte(line), &m); err == nil && m["msg"] == "http request" && m["path"] == "/api/v1/events" {
						foundEventsLog = m
						break
					}
				}
				if foundEventsLog != nil {
					break
				}
				time.Sleep(15 * time.Millisecond)
			}
			if foundEventsLog == nil {
				t.Fatalf("expected RequestLogger entry for /api/v1/events, got logs:\n%s", logBuf.String())
			}
			if st, ok := foundEventsLog["status"].(float64); !ok || int(st) != http.StatusOK {
				t.Errorf("logged status = %v, want 200", foundEventsLog["status"])
			}
			if b, ok := foundEventsLog["bytes"].(float64); !ok || int(b) < len(gotFrame) {
				t.Errorf("logged bytes = %v, want >= %d", foundEventsLog["bytes"], len(gotFrame))
			}
		})
	}
}

func TestServer_GracefulShutdown_EndsIdleSSEWhileOrdinaryInFlightCompletes(t *testing.T) {
	stubEvents := &streamingStubEventService{
		attached: make(chan struct{}, 1),
		sendCh:   make(chan app.StreamEvent, 1),
	}
	reg := module.NewRegistry()
	_ = reg.Register(NewSystemModule())
	_ = reg.Register(openapi.NewModule(&app.Services{Events: stubEvents}))

	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080",
	}
	srv, err := NewServer(ServerOptions{
		Config:         cfg,
		Registry:       reg,
		Authenticators: []module.Authenticator{passthroughAuth{}},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	slowStarted := make(chan struct{})
	allowSlowFinish := make(chan struct{})
	srv.Mux().HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) {
		close(slowStarted)
		select {
		case <-allowSlowFinish:
		case <-r.Context().Done():
			http.Error(w, "unexpected request cancellation", http.StatusRequestTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ordinary request finished"))
	})

	serverCtx, cancelServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Run(serverCtx)
	}()

	baseURL := srv.URL()

	// 1. Open an idle SSE connection and keep it open on the client side.
	sseResp, err := http.Get(baseURL + "/api/v1/events")
	if err != nil {
		t.Fatalf("GET /api/v1/events: %v", err)
	}
	defer sseResp.Body.Close()
	if sseResp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want 200", sseResp.StatusCode)
	}
	select {
	case <-stubEvents.attached:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE attach")
	}

	sseEOF := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(sseResp.Body)
		sseEOF <- readErr
	}()

	// 2. Start an ordinary in-flight request that must NOT be cancelled on shutdown.
	type slowResult struct {
		status int
		body   string
		err    error
	}
	slowCh := make(chan slowResult, 1)
	go func() {
		resp, reqErr := http.Get(baseURL + "/slow")
		if reqErr != nil {
			slowCh <- slowResult{err: reqErr}
			return
		}
		defer resp.Body.Close()
		b, readErr := io.ReadAll(resp.Body)
		slowCh <- slowResult{status: resp.StatusCode, body: string(b), err: readErr}
	}()

	select {
	case <-slowStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for /slow to start")
	}

	// 3. Initiate graceful shutdown while the idle SSE client stays connected and /slow is in-flight.
	cancelServer()

	// Idle SSE stream must terminate promptly without waiting for client disconnect.
	select {
	case readErr := <-sseEOF:
		if readErr != nil {
			t.Fatalf("idle SSE ReadAll error = %v, want clean EOF", readErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for idle SSE stream to close on shutdown")
	}

	// Ordinary in-flight /slow request must still be running (not aborted) until released.
	select {
	case res := <-slowCh:
		t.Fatalf("/slow completed prematurely during shutdown: %+v", res)
	case srvErr := <-serverDone:
		t.Fatalf("srv.Run exited before ordinary in-flight /slow completed: %v", srvErr)
	case <-time.After(50 * time.Millisecond):
	}

	// 4. Release /slow and verify both /slow and srv.Run complete cleanly well before DefaultShutdownTimeout.
	close(allowSlowFinish)
	select {
	case res := <-slowCh:
		if res.err != nil || res.status != http.StatusOK || res.body != "ordinary request finished" {
			t.Fatalf("/slow result = %+v, want 200 'ordinary request finished'", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for /slow response")
	}

	select {
	case srvErr := <-serverDone:
		if srvErr != nil {
			t.Fatalf("srv.Run returned error: %v, want nil", srvErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for srv.Run to return cleanly")
	}
}

type failingFlushWriter struct {
	*httptest.ResponseRecorder
	flushErr error
}

func (f *failingFlushWriter) FlushError() error {
	f.Flush()
	return f.flushErr
}

func TestFallbackRecorder_FlushErrorPropagation(t *testing.T) {
	wantErr := errors.New("underlying flush failure")
	underlying := &failingFlushWriter{
		ResponseRecorder: httptest.NewRecorder(),
		flushErr:         wantErr,
	}
	bufferedRec := &fallbackRecorder{underlying: underlying}

	// Buffered 404 responses stay un-passed-through so Flush() is a no-op before fallback.
	bufferedRec.WriteHeader(http.StatusNotFound)
	if err := http.NewResponseController(bufferedRec).Flush(); err != nil {
		t.Fatalf("buffered 404 Flush() error = %v, want nil", err)
	}
	if bufferedRec.statusCode != http.StatusNotFound || !bufferedRec.written || bufferedRec.passedThrough {
		t.Fatalf("status=%d written=%v passedThrough=%v, want 404/true/false", bufferedRec.statusCode, bufferedRec.written, bufferedRec.passedThrough)
	}

	// Once passed through (e.g. 200 streaming response), FlushError propagates underlying FlushError error.
	streamRec := &fallbackRecorder{underlying: underlying}
	if err := http.NewResponseController(streamRec).Flush(); !errors.Is(err, wantErr) {
		t.Fatalf("stream Flush() error = %v, want %v", err, wantErr)
	}
}

type blockingServerSSEWriter struct {
	http.ResponseWriter
	mu            sync.Mutex
	deadline      time.Time
	deadlineSetCh chan time.Time
	blockingCh    chan struct{}
	flushCount    int
}

func newBlockingServerSSEWriter(w http.ResponseWriter) *blockingServerSSEWriter {
	return &blockingServerSSEWriter{
		ResponseWriter: w,
		deadlineSetCh:  make(chan time.Time, 8),
		blockingCh:     make(chan struct{}, 1),
	}
}

func (w *blockingServerSSEWriter) SetWriteDeadline(d time.Time) error {
	w.mu.Lock()
	w.deadline = d
	w.mu.Unlock()
	select {
	case w.deadlineSetCh <- d:
	default:
	}
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(d)
}

func (w *blockingServerSSEWriter) FlushError() error {
	w.mu.Lock()
	w.flushCount++
	count := w.flushCount
	w.mu.Unlock()
	if count == 1 {
		// Header flush completes immediately so the SSE client sees 200 OK.
		return http.NewResponseController(w.ResponseWriter).Flush()
	}
	// Block subsequent frame flush deterministically until SetWriteDeadline expires.
	select {
	case w.blockingCh <- struct{}{}:
	default:
	}
	for {
		w.mu.Lock()
		d := w.deadline
		w.mu.Unlock()
		if !d.IsZero() {
			wait := time.Until(d)
			if wait <= 0 {
				return os.ErrDeadlineExceeded
			}
			select {
			case <-time.After(wait):
			case d = <-w.deadlineSetCh:
				if !d.IsZero() && !d.After(time.Now()) {
					return os.ErrDeadlineExceeded
				}
			}
			continue
		}
		d = <-w.deadlineSetCh
		if !d.IsZero() && !d.After(time.Now()) {
			return os.ErrDeadlineExceeded
		}
	}
}

func TestServer_GracefulShutdown_AbortsBlockedSSEWriteWhileOrdinaryInFlightCompletes(t *testing.T) {
	stubEvents := &streamingStubEventService{
		attached: make(chan struct{}, 1),
		sendCh:   make(chan app.StreamEvent, 1),
	}
	reg := module.NewRegistry()
	_ = reg.Register(NewSystemModule())
	_ = reg.Register(openapi.NewModule(&app.Services{Events: stubEvents}))

	cfg := &config.Config{
		Host:    "127.0.0.1",
		Port:    0,
		BaseURL: "http://localhost:8080",
	}
	srv, err := NewServer(ServerOptions{
		Config:         cfg,
		Registry:       reg,
		Authenticators: []module.Authenticator{passthroughAuth{}},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	sseBlocked := make(chan struct{})
	var sseBlockedOnce sync.Once
	baseHandler := srv.httpServer.Handler
	srv.httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/events" && r.Method == http.MethodGet {
			bw := newBlockingServerSSEWriter(w)
			go func() {
				<-bw.blockingCh
				sseBlockedOnce.Do(func() { close(sseBlocked) })
			}()
			baseHandler.ServeHTTP(bw, r)
			return
		}
		baseHandler.ServeHTTP(w, r)
	})

	slowStarted := make(chan struct{})
	allowSlowFinish := make(chan struct{})
	srv.Mux().HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) {
		close(slowStarted)
		select {
		case <-allowSlowFinish:
		case <-r.Context().Done():
			http.Error(w, "unexpected request cancellation", http.StatusRequestTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ordinary request finished"))
	})

	serverCtx, cancelServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Run(serverCtx)
	}()

	baseURL := srv.URL()

	// 1. Open SSE stream and emit an event whose flush blocks mid-write.
	sseResp, err := http.Get(baseURL + "/api/v1/events")
	if err != nil {
		t.Fatalf("GET /api/v1/events: %v", err)
	}
	defer sseResp.Body.Close()
	if sseResp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want 200", sseResp.StatusCode)
	}
	select {
	case <-stubEvents.attached:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE attach")
	}

	stubEvents.sendCh <- app.StreamEvent{
		ID:   "99",
		Name: "job.started",
		Data: json.RawMessage(`{"sequence":99}`),
	}
	select {
	case <-sseBlocked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE flush to enter blocked state")
	}

	sseEOF := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(sseResp.Body)
		sseEOF <- readErr
	}()

	// 2. Start an ordinary in-flight request that must survive shutdown cancellation until released.
	type slowResult struct {
		status int
		body   string
		err    error
	}
	slowCh := make(chan slowResult, 1)
	go func() {
		resp, reqErr := http.Get(baseURL + "/slow")
		if reqErr != nil {
			slowCh <- slowResult{err: reqErr}
			return
		}
		defer resp.Body.Close()
		b, readErr := io.ReadAll(resp.Body)
		slowCh <- slowResult{status: resp.StatusCode, body: string(b), err: readErr}
	}()
	select {
	case <-slowStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for /slow to start")
	}

	// 3. Cancel serverCtx; blocked SSE write must abort immediately while /slow keeps running.
	cancelServer()
	select {
	case readErr := <-sseEOF:
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			t.Fatalf("aborted SSE ReadAll error = %v, want nil or unexpected EOF", readErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for blocked SSE stream to finish during shutdown")
	}
	select {
	case res := <-slowCh:
		t.Fatalf("/slow finished prematurely during shutdown: %+v", res)
	case srvErr := <-serverDone:
		t.Fatalf("srv.Run exited before /slow finished: %v", srvErr)
	case <-time.After(50 * time.Millisecond):
	}

	// 4. Release /slow and verify clean completion.
	close(allowSlowFinish)
	select {
	case res := <-slowCh:
		if res.err != nil || res.status != http.StatusOK || res.body != "ordinary request finished" {
			t.Fatalf("/slow result = %+v, want 200 'ordinary request finished'", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for /slow response")
	}
	select {
	case srvErr := <-serverDone:
		if srvErr != nil {
			t.Fatalf("srv.Run returned error: %v, want nil", srvErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for srv.Run to finish cleanly")
	}
}
