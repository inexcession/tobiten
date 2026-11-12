package embedded

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setupTestSupervisorWithProbe(t *testing.T, probe func(ctx context.Context, binDir, dataDir string) (CompressionCapabilities, error)) (*Supervisor, string, *bytes.Buffer) {
	t.Helper()

	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}
	createDummyBinary(t, binDir, "pg_ctl", "18.6", true)
	createDummyBinary(t, binDir, "initdb", "18.6", true)
	createDummyBinary(t, binDir, "postgres", "18.6", true)

	dataDir := filepath.Join(tmpDir, "pgdata")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatalf("failed to create dataDir: %v", err)
	}
	// Fabricate existing cluster so ensureClusterInitialized does not run initdb
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("18\n"), 0600); err != nil {
		t.Fatalf("failed to write PG_VERSION: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "postgresql.conf"), []byte("# initial config\n"), 0600); err != nil {
		t.Fatalf("failed to write postgresql.conf: %v", err)
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s, err := New(Options{
		BinDir:           binDir,
		DataDir:          dataDir,
		Logger:           logger,
		isRoot:           func() bool { return false },
		compressionProbe: probe,
	})
	if err != nil {
		t.Fatalf("unexpected error creating supervisor: %v", err)
	}

	return s, dataDir, &logBuf
}

func TestSupervisor_CompressionSettings_FullConfig(t *testing.T) {
	mockProbe := func(_ context.Context, _, _ string) (CompressionCapabilities, error) {
		return CompressionCapabilities{
			HasZstdWAL:  true,
			HasLZ4Toast: true,
		}, nil
	}

	s, dataDir, logBuf := setupTestSupervisorWithProbe(t, mockProbe)

	// Pre-cancelled context executes up through configuration write (step 7) and returns at step 8
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = s.Start(ctx)

	// 1. Verify postgresql.conf contains full compression configuration
	confPath := filepath.Join(dataDir, "postgresql.conf")
	confBytes, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	confContent := string(confBytes)

	if !strings.Contains(confContent, "wal_compression = 'zstd'") {
		t.Fatalf("expected postgresql.conf to contain wal_compression = 'zstd', got:\n%s", confContent)
	}
	if !strings.Contains(confContent, "default_toast_compression = 'lz4'") {
		t.Fatalf("expected postgresql.conf to contain default_toast_compression = 'lz4', got:\n%s", confContent)
	}

	// 2. Verify no compression warnings were logged
	logOutput := logBuf.String()
	if strings.Contains(logOutput, "wal_compression") || strings.Contains(logOutput, "default_toast_compression") {
		t.Fatalf("expected no compression warnings in log for full config, got:\n%s", logOutput)
	}

	// 3. Verify capabilities and error diagnostic accessors
	caps := s.CompressionCapabilities()
	if !caps.HasZstdWAL || !caps.HasLZ4Toast {
		t.Fatalf("expected CompressionCapabilities() to report both supported, got: %+v", caps)
	}
	if s.CompressionError() != nil {
		t.Fatalf("expected CompressionError() == nil, got: %v", s.CompressionError())
	}
}

func TestSupervisor_CompressionSettings_WarnedFallbackConfig(t *testing.T) {
	mockProbe := func(_ context.Context, _, _ string) (CompressionCapabilities, error) {
		return CompressionCapabilities{
			HasZstdWAL:  false,
			HasLZ4Toast: false,
		}, nil
	}

	s, dataDir, logBuf := setupTestSupervisorWithProbe(t, mockProbe)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = s.Start(ctx)

	// 1. Verify postgresql.conf omits both compression settings
	confPath := filepath.Join(dataDir, "postgresql.conf")
	confBytes, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	confContent := string(confBytes)

	if strings.Contains(confContent, "wal_compression") {
		t.Fatalf("expected postgresql.conf to omit wal_compression, got:\n%s", confContent)
	}
	if strings.Contains(confContent, "default_toast_compression") {
		t.Fatalf("expected postgresql.conf to omit default_toast_compression, got:\n%s", confContent)
	}

	// 2. Verify warnings logged for both unsupported settings
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, `"level":"WARN"`) {
		t.Fatalf("expected WARN log entry for unsupported compression, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "zstd WAL compression") {
		t.Fatalf("expected warning for zstd WAL compression, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "lz4 TOAST compression") {
		t.Fatalf("expected warning for lz4 TOAST compression, got:\n%s", logOutput)
	}

	// 3. Verify capabilities and error diagnostic accessors
	caps := s.CompressionCapabilities()
	if caps.HasZstdWAL || caps.HasLZ4Toast {
		t.Fatalf("expected CompressionCapabilities() to report neither supported, got: %+v", caps)
	}
	if s.CompressionError() != nil {
		t.Fatalf("expected CompressionError() == nil, got: %v", s.CompressionError())
	}
}

func TestSupervisor_CompressionSettings_PartialZstdOnly(t *testing.T) {
	mockProbe := func(_ context.Context, _, _ string) (CompressionCapabilities, error) {
		return CompressionCapabilities{
			HasZstdWAL:  true,
			HasLZ4Toast: false,
		}, nil
	}

	s, dataDir, logBuf := setupTestSupervisorWithProbe(t, mockProbe)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = s.Start(ctx)

	confBytes, err := os.ReadFile(filepath.Join(dataDir, "postgresql.conf"))
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	confContent := string(confBytes)

	if !strings.Contains(confContent, "wal_compression = 'zstd'") {
		t.Fatalf("expected postgresql.conf to contain wal_compression = 'zstd', got:\n%s", confContent)
	}
	if strings.Contains(confContent, "default_toast_compression") {
		t.Fatalf("expected postgresql.conf to omit default_toast_compression, got:\n%s", confContent)
	}

	logOutput := logBuf.String()
	if strings.Contains(logOutput, "zstd WAL compression") {
		t.Fatalf("unexpected warning for supported zstd WAL compression, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "lz4 TOAST compression") {
		t.Fatalf("expected warning for unsupported lz4 TOAST compression, got:\n%s", logOutput)
	}

	caps := s.CompressionCapabilities()
	if !caps.HasZstdWAL || caps.HasLZ4Toast {
		t.Fatalf("expected HasZstdWAL=true and HasLZ4Toast=false, got: %+v", caps)
	}
	if s.CompressionError() != nil {
		t.Fatalf("expected CompressionError() == nil, got: %v", s.CompressionError())
	}
}

func TestSupervisor_CompressionSettings_PartialLZ4Only(t *testing.T) {
	mockProbe := func(_ context.Context, _, _ string) (CompressionCapabilities, error) {
		return CompressionCapabilities{
			HasZstdWAL:  false,
			HasLZ4Toast: true,
		}, nil
	}

	s, dataDir, logBuf := setupTestSupervisorWithProbe(t, mockProbe)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = s.Start(ctx)

	confBytes, err := os.ReadFile(filepath.Join(dataDir, "postgresql.conf"))
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	confContent := string(confBytes)

	if strings.Contains(confContent, "wal_compression") {
		t.Fatalf("expected postgresql.conf to omit wal_compression, got:\n%s", confContent)
	}
	if !strings.Contains(confContent, "default_toast_compression = 'lz4'") {
		t.Fatalf("expected postgresql.conf to contain default_toast_compression = 'lz4', got:\n%s", confContent)
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "zstd WAL compression") {
		t.Fatalf("expected warning for unsupported zstd WAL compression, got:\n%s", logOutput)
	}
	if strings.Contains(logOutput, "lz4 TOAST compression") {
		t.Fatalf("unexpected warning for supported lz4 TOAST compression, got:\n%s", logOutput)
	}

	caps := s.CompressionCapabilities()
	if caps.HasZstdWAL || !caps.HasLZ4Toast {
		t.Fatalf("expected HasZstdWAL=false and HasLZ4Toast=true, got: %+v", caps)
	}
	if s.CompressionError() != nil {
		t.Fatalf("expected CompressionError() == nil, got: %v", s.CompressionError())
	}
}

func TestSupervisor_CompressionSettings_StaleConfigDoesNotHideSupportedCodec(t *testing.T) {
	// Regression: Stale unsupported settings in existing cluster files (postgresql.conf / postgresql.auto.conf)
	// must not cause capability detection to fail and hide supported codecs.
	mockProbe := func(_ context.Context, _, _ string) (CompressionCapabilities, error) {
		return CompressionCapabilities{
			HasZstdWAL:  false,
			HasLZ4Toast: true,
		}, nil
	}

	s, dataDir, logBuf := setupTestSupervisorWithProbe(t, mockProbe)

	// Write stale unsupported settings in dataDir/postgresql.conf and postgresql.auto.conf
	staleConf := `# Stale configuration from earlier run or user edit
wal_compression = 'invalid_stale_algo'
default_toast_compression = 'stale_broken'
`
	if err := os.WriteFile(filepath.Join(dataDir, "postgresql.conf"), []byte(staleConf), 0600); err != nil {
		t.Fatalf("failed to write stale postgresql.conf: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "postgresql.auto.conf"), []byte("wal_compression = 'stale_auto'\n"), 0600); err != nil {
		t.Fatalf("failed to write stale postgresql.auto.conf: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = s.Start(ctx)

	// 1. Verify that the supported codec (lz4) was emitted despite stale config
	confBytes, err := os.ReadFile(filepath.Join(dataDir, "postgresql.conf"))
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	confContent := string(confBytes)

	if !strings.Contains(confContent, "default_toast_compression = 'lz4'") {
		t.Fatalf("expected postgresql.conf to contain default_toast_compression = 'lz4', got:\n%s", confContent)
	}
	if strings.Contains(confContent, "wal_compression = 'zstd'") {
		t.Fatalf("expected postgresql.conf to omit unsupported wal_compression=zstd, got:\n%s", confContent)
	}

	// 2. Verify capabilities and diagnostics are preserved
	caps := s.CompressionCapabilities()
	if !caps.HasLZ4Toast {
		t.Fatalf("expected HasLZ4Toast == true, got false; stale config hid supported codec!")
	}
	if caps.HasZstdWAL {
		t.Fatalf("expected HasZstdWAL == false, got true")
	}
	if s.CompressionError() != nil {
		t.Fatalf("expected CompressionError() == nil, got %v", s.CompressionError())
	}

	// 3. Verify only unsupported setting warned, no probe failure
	logOutput := logBuf.String()
	if strings.Contains(logOutput, "failed to probe postgresql compression capabilities") {
		t.Fatalf("unexpected probe failure warning in log:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "zstd WAL compression") {
		t.Fatalf("expected warning for unsupported zstd WAL compression, got:\n%s", logOutput)
	}
}

func TestSupervisor_CompressionSettings_ProbeErrorFallback(t *testing.T) {
	simulatedErr := errors.New("simulated probe failure")
	mockProbe := func(_ context.Context, _, _ string) (CompressionCapabilities, error) {
		return CompressionCapabilities{}, simulatedErr
	}

	s, dataDir, logBuf := setupTestSupervisorWithProbe(t, mockProbe)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = s.Start(ctx)

	confBytes, err := os.ReadFile(filepath.Join(dataDir, "postgresql.conf"))
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	confContent := string(confBytes)

	if strings.Contains(confContent, "wal_compression") {
		t.Fatalf("expected postgresql.conf to omit wal_compression, got:\n%s", confContent)
	}
	if strings.Contains(confContent, "default_toast_compression") {
		t.Fatalf("expected postgresql.conf to omit default_toast_compression, got:\n%s", confContent)
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "failed to probe postgresql compression capabilities") {
		t.Fatalf("expected warning about probe failure, got:\n%s", logOutput)
	}

	caps := s.CompressionCapabilities()
	if caps.HasZstdWAL || caps.HasLZ4Toast {
		t.Fatalf("expected empty capabilities on probe error, got: %+v", caps)
	}
	if !errors.Is(s.CompressionError(), simulatedErr) {
		t.Fatalf("expected CompressionError() to preserve simulatedErr, got: %v", s.CompressionError())
	}
}

func TestSupervisor_CompressionSettings_TimeoutDiagnostic(t *testing.T) {
	mockProbe := func(_ context.Context, _, _ string) (CompressionCapabilities, error) {
		return CompressionCapabilities{}, context.DeadlineExceeded
	}

	s, dataDir, logBuf := setupTestSupervisorWithProbe(t, mockProbe)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = s.Start(ctx)

	confBytes, err := os.ReadFile(filepath.Join(dataDir, "postgresql.conf"))
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	confContent := string(confBytes)

	if strings.Contains(confContent, "wal_compression") || strings.Contains(confContent, "default_toast_compression") {
		t.Fatalf("expected compression tuning to be omitted on timeout, got:\n%s", confContent)
	}

	if !errors.Is(s.CompressionError(), context.DeadlineExceeded) {
		t.Fatalf("expected CompressionError() to preserve DeadlineExceeded, got: %v", s.CompressionError())
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "failed to probe postgresql compression capabilities") {
		t.Fatalf("expected probe failure warning in log, got:\n%s", logOutput)
	}
}

func TestSupervisor_CheckCompressionMethod(t *testing.T) {
	mockProbe := func(_ context.Context, _, _ string) (CompressionCapabilities, error) {
		return CompressionCapabilities{
			HasZstdWAL:  true,
			HasLZ4Toast: true,
		}, nil
	}

	s, _, _ := setupTestSupervisorWithProbe(t, mockProbe)

	caps, err := s.CheckCompression(context.Background())
	if err != nil {
		t.Fatalf("unexpected error from CheckCompression: %v", err)
	}
	if !caps.HasZstdWAL || !caps.HasLZ4Toast {
		t.Fatalf("expected both capabilities true, got: %+v", caps)
	}
	if s.CompressionCapabilities() != caps {
		t.Fatalf("expected cached capabilities to match, got: %+v", s.CompressionCapabilities())
	}
	if s.CompressionError() != nil {
		t.Fatalf("expected CompressionError() == nil, got: %v", s.CompressionError())
	}
}

func createDummyPostgresWithCompression(t *testing.T, binDir string, supportZstd, supportLZ4 bool) {
	t.Helper()
	p := filepath.Join(binDir, "postgres")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "postgres (PostgreSQL) 18.6"
  exit 0
fi

has_zstd=%t
has_lz4=%t

for arg in "$@"; do
  if [ "$arg" = "wal_compression=zstd" ]; then
    if [ "$has_zstd" = "true" ]; then
      echo "zstd"
      exit 0
    else
      echo "FATAL:  invalid value for parameter \"wal_compression\": \"zstd\"" >&2
      exit 1
    fi
  fi
  if [ "$arg" = "default_toast_compression=lz4" ]; then
    if [ "$has_lz4" = "true" ]; then
      echo "lz4"
      exit 0
    else
      echo "FATAL:  invalid value for parameter \"default_toast_compression\": \"lz4\"" >&2
      exit 1
    fi
  fi
done

exit 0
`, supportZstd, supportLZ4)

	if err := os.WriteFile(p, []byte(script), 0755); err != nil {
		t.Fatalf("failed to create dummy postgres binary: %v", err)
	}
}

func TestProbeCompression_WithDummyScript(t *testing.T) {
	tests := []struct {
		name        string
		supportZstd bool
		supportLZ4  bool
	}{
		{"both_supported", true, true},
		{"neither_supported", false, false},
		{"zstd_only", true, false},
		{"lz4_only", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			binDir := filepath.Join(tmpDir, "bins")
			if err := os.MkdirAll(binDir, 0755); err != nil {
				t.Fatalf("failed to create binDir: %v", err)
			}
			createDummyPostgresWithCompression(t, binDir, tt.supportZstd, tt.supportLZ4)

			caps, err := ProbeCompression(context.Background(), binDir, "")
			if err != nil {
				t.Fatalf("unexpected error from ProbeCompression: %v", err)
			}
			if caps.HasZstdWAL != tt.supportZstd {
				t.Fatalf("expected HasZstdWAL == %t, got %t", tt.supportZstd, caps.HasZstdWAL)
			}
			if caps.HasLZ4Toast != tt.supportLZ4 {
				t.Fatalf("expected HasLZ4Toast == %t, got %t", tt.supportLZ4, caps.HasLZ4Toast)
			}
		})
	}
}

func TestProbeCompression_IsolatedFromDataDir(t *testing.T) {
	// Proves that even if dataDir contains broken configuration files,
	// ProbeCompression remains fully isolated and succeeds.
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}
	createDummyPostgresWithCompression(t, binDir, true, true)

	brokenDataDir := filepath.Join(tmpDir, "broken_cluster")
	if err := os.MkdirAll(brokenDataDir, 0700); err != nil {
		t.Fatalf("failed to create brokenDataDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(brokenDataDir, "PG_VERSION"), []byte("18\n"), 0600); err != nil {
		t.Fatalf("failed to write PG_VERSION: %v", err)
	}
	if err := os.WriteFile(filepath.Join(brokenDataDir, "postgresql.conf"), []byte("syntax_error_unsupported = true\n"), 0600); err != nil {
		t.Fatalf("failed to write postgresql.conf: %v", err)
	}
	if err := os.WriteFile(filepath.Join(brokenDataDir, "postgresql.auto.conf"), []byte("wal_compression = 'bad'\n"), 0600); err != nil {
		t.Fatalf("failed to write postgresql.auto.conf: %v", err)
	}

	caps, err := ProbeCompression(context.Background(), binDir, brokenDataDir)
	if err != nil {
		t.Fatalf("ProbeCompression failed on dataDir with broken config: %v", err)
	}
	if !caps.HasZstdWAL || !caps.HasLZ4Toast {
		t.Fatalf("expected both capabilities true, got: %+v", caps)
	}
}

func TestProbeCompression_Timeout(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}

	// Create dummy binary that sleeps for 10 seconds
	p := filepath.Join(binDir, "postgres")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "postgres (PostgreSQL) 18.6"
  exit 0
fi
sleep 10
exit 0
`
	if err := os.WriteFile(p, []byte(script), 0755); err != nil {
		t.Fatalf("failed to create sleeping postgres binary: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := ProbeCompression(ctx, binDir, "")
	if err == nil {
		t.Fatal("expected timeout error from ProbeCompression, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout/deadline exceeded error, got: %v", err)
	}
}

func TestProbeCompression_UnrelatedCommandFailure(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}

	// Create dummy binary that crashes/fails with exit code 2 and unrelated stderr
	p := filepath.Join(binDir, "postgres")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "postgres (PostgreSQL) 18.6"
  exit 0
fi
echo "FATAL: failed to initialize shared memory" >&2
exit 2
`
	if err := os.WriteFile(p, []byte(script), 0755); err != nil {
		t.Fatalf("failed to create failing postgres binary: %v", err)
	}

	_, err := ProbeCompression(context.Background(), binDir, "")
	if err == nil {
		t.Fatal("expected error from ProbeCompression on unrelated command failure, got nil")
	}
	if !strings.Contains(err.Error(), "failed to probe") {
		t.Fatalf("expected 'failed to probe' in error, got: %v", err)
	}
}

func TestProbeCompression_MissingBinary(t *testing.T) {
	_, err := ProbeCompression(context.Background(), "/nonexistent/bin/dir", "")
	if err == nil {
		t.Fatal("expected error for nonexistent binary directory, got nil")
	}
}

func TestProbeCompression_RealHostPostgres(t *testing.T) {
	binDir, err := FindBinDir("")
	if err != nil {
		t.Skipf("skipping real host postgres test: %v", err)
	}

	caps, err := ProbeCompression(context.Background(), binDir, "")
	if err != nil {
		t.Fatalf("unexpected error probing real host postgres: %v", err)
	}

	t.Logf("Real host PostgreSQL at %s compression capabilities: %+v", binDir, caps)
}
