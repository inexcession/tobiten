package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
)

func TestVersionString(t *testing.T) {
	got := versionString()
	want := "tobitend 0.1.0-dev"
	if got != want {
		t.Fatalf("versionString() = %q, want %q", got, want)
	}
}

func TestRunVersion(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "flag --version",
			args: []string{"--version"},
			want: "tobitend 0.1.0-dev\n",
		},
		{
			name: "flag -version",
			args: []string{"-version"},
			want: "tobitend 0.1.0-dev\n",
		},
		{
			name: "flag -v",
			args: []string{"-v"},
			want: "tobitend 0.1.0-dev\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("os.Pipe() error: %v", err)
			}

			origStdout := os.Stdout
			os.Stdout = w

			exitCode := run(tc.args)

			_ = w.Close()
			os.Stdout = origStdout

			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)
			_ = r.Close()

			if exitCode != 0 {
				t.Errorf("run(%v) exit code = %d, want 0", tc.args, exitCode)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("run(%v) output = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}

func TestRunHelp(t *testing.T) {
	for _, flag := range []string{"--help", "-help", "-h"} {
		t.Run(flag, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("os.Pipe() error: %v", err)
			}

			origStderr := os.Stderr
			os.Stderr = w

			exitCode := run([]string{flag})

			_ = w.Close()
			os.Stderr = origStderr

			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)
			_ = r.Close()

			if exitCode != 0 {
				t.Errorf("run(%s) exit code = %d, want 0", flag, exitCode)
			}
			out := buf.String()
			if !bytes.Contains(buf.Bytes(), []byte("Usage of tobitend:")) {
				t.Errorf("run(%s) stderr = %q, want usage output", flag, out)
			}
			// Verify canonical double-dash options are presented (ADR-0014 §2, I-8)
			for _, canonical := range []string{"--help", "--version", "--base-url", "--log-format", "--port", "--data-dir"} {
				if !bytes.Contains(buf.Bytes(), []byte(canonical)) {
					t.Errorf("run(%s) usage missing canonical option %q", flag, canonical)
				}
			}
			// -v alias must be omitted from canonical help
			for _, line := range strings.Split(out, "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "-v\t") || strings.HasPrefix(trimmed, "-v ") || trimmed == "-v" {
					t.Errorf("run(%s) usage contains omitted alias -v: %q", flag, line)
				}
			}
			// I-7: secrets and client API key must NOT have CLI flags
			for _, forbidden := range []string{"admin-pass", "secret-key", "database-url", "api-key"} {
				if bytes.Contains(buf.Bytes(), []byte(forbidden)) {
					t.Errorf("run(%s) usage contains forbidden flag %q", flag, forbidden)
				}
			}
		})
	}
}

func TestRunInvalidFlag(t *testing.T) {
	invalidFlags := []string{
		"--invalid-unknown-flag",
		"-invalid-unknown-flag",
		"--admin-pass=secret",
		"-admin-pass=secret",
		"--secret-key=secret",
		"-secret-key=secret",
		"--database-url=postgres://localhost/db",
		"-database-url=postgres://localhost/db",
		"--api-key=token",
		"-api-key=token",
	}
	for _, flg := range invalidFlags {
		t.Run(flg, func(t *testing.T) {
			exitCode := run([]string{flg})
			if exitCode != 2 {
				t.Errorf("run(%s) exit code = %d, want 2", flg, exitCode)
			}
		})
	}
}

func TestRunStorageModeRefusal(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		envKey     string
		envVal     string
		wantExit   int
		wantErrSub string
		wantNotSub string
	}{
		{
			name:       "local mode with database URL refuses startup",
			args:       []string{"--storage-mode=local"},
			envKey:     "TOBITEN_DATABASE_URL",
			envVal:     "postgres://user:super_secret_password@localhost:5432/db",
			wantExit:   1,
			wantErrSub: "storage mode 'local' requires an empty TOBITEN_DATABASE_URL",
			wantNotSub: "super_secret_password",
		},
		{
			name:       "postgres mode without database URL refuses startup",
			args:       []string{"--storage-mode=postgres"},
			envKey:     "TOBITEN_DATABASE_URL",
			envVal:     "",
			wantExit:   1,
			wantErrSub: "storage mode 'postgres' requires a non-empty TOBITEN_DATABASE_URL",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.envKey, tc.envVal)

			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("os.Pipe() error: %v", err)
			}

			origStderr := os.Stderr
			os.Stderr = w

			exitCode := run(tc.args)

			_ = w.Close()
			os.Stderr = origStderr

			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)
			_ = r.Close()

			if exitCode != tc.wantExit {
				t.Errorf("run(%v) exit code = %d, want %d", tc.args, exitCode, tc.wantExit)
			}
			if !bytes.Contains(buf.Bytes(), []byte(tc.wantErrSub)) {
				t.Errorf("run(%v) stderr = %q, want it to contain %q", tc.args, buf.String(), tc.wantErrSub)
			}
			if tc.wantNotSub != "" && bytes.Contains(buf.Bytes(), []byte(tc.wantNotSub)) {
				t.Errorf("run(%v) stderr = %q, must NOT contain secret %q", tc.args, buf.String(), tc.wantNotSub)
			}
		})
	}
}

func TestRunLoggingWiring(t *testing.T) {
	bypassStorageForHTTPTest(t)
	origLogger := slog.Default()
	defer slog.SetDefault(origLogger)

	origStartServer := startServer
	startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
		return nil
	}
	defer func() { startServer = origStartServer }()

	t.Run("flags wire format and level", func(t *testing.T) {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe() error: %v", err)
		}
		origStdout := os.Stdout
		os.Stdout = w

		exitCode := run([]string{"--log-format=table", "--log-level=debug"})

		_ = w.Close()
		os.Stdout = origStdout

		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		_ = r.Close()

		if exitCode != 0 {
			t.Fatalf("run() exit code = %d, want 0", exitCode)
		}

		logger := slog.Default()
		// Debug level should be enabled
		if !logger.Enabled(context.Background(), slog.LevelDebug) {
			t.Errorf("expected LevelDebug to be enabled with --log-level=debug")
		}
	})

	t.Run("env vars wire format and level", func(t *testing.T) {
		t.Setenv("TOBITEN_LOG_FORMAT", "text")
		t.Setenv("TOBITEN_LOG_LEVEL", "warn")

		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe() error: %v", err)
		}
		origStdout := os.Stdout
		os.Stdout = w

		exitCode := run([]string{})

		_ = w.Close()
		os.Stdout = origStdout

		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		_ = r.Close()

		if exitCode != 0 {
			t.Fatalf("run() exit code = %d, want 0", exitCode)
		}

		logger := slog.Default()
		// Info level should be disabled when Level=warn
		if logger.Enabled(context.Background(), slog.LevelInfo) {
			t.Errorf("expected LevelInfo to be disabled with TOBITEN_LOG_LEVEL=warn")
		}
		if !logger.Enabled(context.Background(), slog.LevelWarn) {
			t.Errorf("expected LevelWarn to be enabled with TOBITEN_LOG_LEVEL=warn")
		}
	})

	t.Run("invalid log format refuses execution", func(t *testing.T) {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe() error: %v", err)
		}
		origStderr := os.Stderr
		os.Stderr = w

		exitCode := run([]string{"--log-format=invalid_fmt"})

		_ = w.Close()
		os.Stderr = origStderr

		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		_ = r.Close()

		if exitCode != 1 {
			t.Errorf("run(--log-format=invalid_fmt) exit code = %d, want 1", exitCode)
		}
		if !bytes.Contains(buf.Bytes(), []byte("unsupported log format")) {
			t.Errorf("expected stderr to contain 'unsupported log format', got: %s", buf.String())
		}
	})

	t.Run("invalid log level refuses execution", func(t *testing.T) {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe() error: %v", err)
		}
		origStderr := os.Stderr
		os.Stderr = w

		exitCode := run([]string{"--log-level=invalid_lvl"})

		_ = w.Close()
		os.Stderr = origStderr

		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		_ = r.Close()

		if exitCode != 1 {
			t.Errorf("run(--log-level=invalid_lvl) exit code = %d, want 1", exitCode)
		}
		if !bytes.Contains(buf.Bytes(), []byte("unsupported log level")) {
			t.Errorf("expected stderr to contain 'unsupported log level', got: %s", buf.String())
		}
	})
}

func TestRunLoggingTableColorWiring(t *testing.T) {
	bypassStorageForHTTPTest(t)
	origLogger := slog.Default()
	defer slog.SetDefault(origLogger)

	origStartServer := startServer
	startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
		return nil
	}
	defer func() { startServer = origStartServer }()

	t.Run("NO_COLOR disables color in table format", func(t *testing.T) {
		t.Setenv("NO_COLOR", "1")
		exitCode := run([]string{"--log-format=table"})
		if exitCode != 0 {
			t.Fatalf("run exit code = %d", exitCode)
		}
	})

	t.Run("piped stdout disables color in table format", func(t *testing.T) {
		t.Setenv("NO_COLOR", "")
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe() error: %v", err)
		}
		origStdout := os.Stdout
		os.Stdout = w

		exitCode := run([]string{"--log-format=table"})

		_ = w.Close()
		os.Stdout = origStdout
		_ = r.Close()

		if exitCode != 0 {
			t.Fatalf("run exit code = %d", exitCode)
		}
	})
}

func getFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on port 0: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestRunServer_GracefulShutdown(t *testing.T) {
	bypassStorageForHTTPTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port := getFreePort(t)
	args := []string{fmt.Sprintf("--port=%d", port)}

	origStartServer := startServer
	defer func() { startServer = origStartServer }()

	started := make(chan struct{})
	var srvURL string

	startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
		srv, err := api.NewServer(api.ServerOptions{
			Config: cfg,
			Logger: logger,
		})
		if err != nil {
			return err
		}
		srvURL = srv.URL()
		close(started)
		return srv.Run(ctx)
	}

	done := make(chan int, 1)
	go func() {
		done <- runContext(ctx, args)
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server to start")
	}

	// Verify /healthz returns 200
	resp, err := http.Get(srvURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	// Trigger graceful shutdown
	cancel()

	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("runContext() exit code = %d, want 0", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func TestBuildRegistry(t *testing.T) {
	t.Run("nil config returns registry with compiled-in modules failing closed by default", func(t *testing.T) {
		reg := BuildRegistry(nil)
		if reg == nil {
			t.Fatal("BuildRegistry(nil) returned nil")
		}
		if reg.Module("system") == nil {
			t.Errorf("expected 'system' module to be registered")
		}
		if reg.Module("openapi") == nil {
			t.Errorf("expected 'openapi' module to be registered")
		}
		if !reg.IsEnabled("system") {
			t.Errorf("expected 'system' module to be enabled by default")
		}
		if !reg.IsEnabled("openapi") {
			t.Errorf("expected 'openapi' module to be enabled by default")
		}

		// Defaults fail closed/unavailable with 503 until real services land
		routes := reg.Routes()
		var openapiHandler http.Handler
		for _, m := range routes {
			if m.ModuleName == "openapi" && m.Prefix == "/api/v1" {
				openapiHandler = m.Handler
				break
			}
		}
		if openapiHandler == nil {
			t.Fatal("could not find openapi route mount")
		}

		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"pwd"}`))
		rec := httptest.NewRecorder()
		openapiHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("default openapi login status = %d, want 503 Service Unavailable", rec.Code)
		}
	})

	t.Run("explicit fake services allow auth operations", func(t *testing.T) {
		fakeServices := app.NewStubServices()
		reg := BuildRegistryWithServices(nil, fakeServices)
		if reg == nil {
			t.Fatal("BuildRegistryWithServices returned nil")
		}

		routes := reg.Routes()
		var openapiHandler http.Handler
		for _, m := range routes {
			if m.ModuleName == "openapi" && m.Prefix == "/api/v1" {
				openapiHandler = m.Handler
				break
			}
		}
		if openapiHandler == nil {
			t.Fatal("could not find openapi route mount")
		}

		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/setup", strings.NewReader(`{"setup_token":"tok","username":"admin","password":"pwd"}`))
		rec := httptest.NewRecorder()
		openapiHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Errorf("fake-service setup status = %d, want 201 Created", rec.Code)
		}
	})

	t.Run("applies disabled modules from config", func(t *testing.T) {
		cfg := &config.Config{
			DisabledModules: []string{"openapi"},
		}
		reg := BuildRegistry(cfg)
		if reg == nil {
			t.Fatal("BuildRegistry(cfg) returned nil")
		}
		if reg.IsEnabled("openapi") {
			t.Errorf("expected 'openapi' module to be disabled by config")
		}
		if !reg.IsEnabled("system") {
			t.Errorf("expected 'system' module to remain enabled")
		}
	})
}

// These tests isolate CLI logging/HTTP behavior; startup_test covers real storage.
func bypassStorageForHTTPTest(t *testing.T) {
	t.Helper()
	original := startDaemon
	t.Cleanup(func() { startDaemon = original })
	startDaemon = func(ctx context.Context, cfg *config.Config, serve func(context.Context, *config.Config, *slog.Logger) error) error {
		if err := setupLogging(cfg); err != nil {
			return err
		}
		return serve(ctx, cfg, slog.Default())
	}
}
