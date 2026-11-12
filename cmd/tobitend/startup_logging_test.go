package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/storage/embedded"
)

func captureLifecycleOutput(t *testing.T, run func() int) (int, string, string) {
	t.Helper()
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	originalOut, originalErr, originalLogger := os.Stdout, os.Stderr, slog.Default()
	defer func() { os.Stdout = originalOut; os.Stderr = originalErr; slog.SetDefault(originalLogger) }()
	os.Stdout, os.Stderr = outW, errW
	code := run()
	if err := outW.Close(); err != nil {
		t.Fatal(err)
	}
	if err := errW.Close(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := io.Copy(&stdout, outR); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(&stderr, errR); err != nil {
		t.Fatal(err)
	}
	if err := outR.Close(); err != nil {
		t.Fatal(err)
	}
	if err := errR.Close(); err != nil {
		t.Fatal(err)
	}
	return code, stdout.String(), stderr.String()
}

func assertJSONLifecycle(t *testing.T, output string, count int) {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != count {
		t.Fatalf("got %d log lines: %s", len(lines), output)
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("non-JSON lifecycle log: %s", line)
		}
		for _, key := range []string{"time", "level", "service", "msg"} {
			if record[key] == nil {
				t.Fatalf("missing %s: %s", key, line)
			}
		}
		if record["service"] != "tobitend" {
			t.Fatalf("wrong service: %s", line)
		}
	}
}

func TestLifecycleErrorsUseConfiguredStructuredLogging(t *testing.T) {
	original := startDaemon
	t.Cleanup(func() { startDaemon = original })
	for _, format := range []string{"json", "text", "table"} {
		t.Run(format, func(t *testing.T) {
			startDaemon = func(_ context.Context, cfg *config.Config, _ func(context.Context, *config.Config, *slog.Logger, *app.Services) error) error {
				if err := setupLogging(cfg); err != nil {
					return err
				}
				return errors.Join(errors.New("server failure"), errors.New("cleanup failure"))
			}
			code, out, stderr := captureLifecycleOutput(t, func() int { return runContext(t.Context(), []string{"--log-format=" + format}) })
			if code != 1 || stderr != "" {
				t.Fatalf("code=%d stderr=%q", code, stderr)
			}
			if format == "json" {
				assertJSONLifecycle(t, out, 1)
			}
			if !strings.Contains(out, "server failure") || !strings.Contains(out, "cleanup failure") || !strings.Contains(out, "tobitend") || !strings.Contains(out, "ERROR") {
				t.Fatalf("missing lifecycle fields: %q", out)
			}
		})
	}
}

func TestBootstrapLogsCarryServiceAndStructuredErrors(t *testing.T) {
	original := newLocalPostgres
	t.Cleanup(func() { newLocalPostgres = original })
	newLocalPostgres = func(opts embedded.Options) (localPostgres, error) {
		opts.Logger.Warn("bootstrap diagnostic")
		return nil, errors.Join(errors.New("bootstrap failed"), errors.New("second cause"))
	}
	t.Setenv("TOBITEN_DATABASE_URL", "")
	t.Setenv("TOBITEN_ADMIN_PASS", "do-not-log-bootstrap-password")
	code, out, stderr := captureLifecycleOutput(t, func() int {
		return runContext(t.Context(), []string{"--data-dir=" + t.TempDir(), "--pg-bin=" + fakePGBinaries(t), "--storage-mode=local"})
	})
	if code != 1 || stderr != "" {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	assertJSONLifecycle(t, out, 2)
	if strings.Contains(out, "do-not-log-bootstrap-password") {
		t.Fatal("bootstrap password leaked")
	}
}

func TestBootstrapConnectionErrorDoesNotExposeDatabaseSecret(t *testing.T) {
	t.Setenv("TOBITEN_DATABASE_URL", "postgres://operator:do-not-log-db-password@127.0.0.1:1/postgres?sslmode=disable")
	code, out, stderr := captureLifecycleOutput(t, func() int {
		return runContext(t.Context(), []string{"--storage-mode=postgres"})
	})
	if code != 1 || stderr != "" {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	assertJSONLifecycle(t, out, 1)
	if strings.Contains(out, "do-not-log-db-password") {
		t.Fatal("database password leaked")
	}
}
