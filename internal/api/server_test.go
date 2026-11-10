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
