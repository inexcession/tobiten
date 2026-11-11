package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/module"
)

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
	srv, err := NewServer(ServerOptions{Config: cfg})
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
			srv, err := NewServer(ServerOptions{Config: cfg})
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
			Config:   cfg,
			Registry: reg,
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
