package embedded

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
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
		BinDir:            binDir,
		DataDir:           dataDir,
		Logger:            logger,
		isRoot:            func() bool { return false },
		resolvePIDTimeout: 10 * time.Millisecond,
		compressionProbe:  probe,
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
	ctx := context.Background()
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

	ctx := context.Background()
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

	ctx := context.Background()
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

	ctx := context.Background()
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
	// This exercises ProbeCompression directly (compressionProbe: nil) so that the supervisor evaluates
	// capabilities through isolated execution and writes the effective configuration.
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}
	createDummyBinary(t, binDir, "pg_ctl", "18.6", true)
	createDummyBinary(t, binDir, "initdb", "18.6", true)
	// Partial binary support: zstd WAL unsupported, lz4 TOAST supported
	createDummyPostgresWithCompression(t, binDir, false, true)

	dataDir := filepath.Join(tmpDir, "pgdata")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatalf("failed to create dataDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("18\n"), 0600); err != nil {
		t.Fatalf("failed to write PG_VERSION: %v", err)
	}

	// 1. Write stale unsupported settings into existing cluster files (postgresql.conf and postgresql.auto.conf).
	// Stale settings inside the prior Tobiten block simulate an earlier run where codecs were configured,
	// while postgresql.auto.conf simulates runtime alter system or external edits.
	// User custom settings outside the Tobiten block simulate pre-existing user customizations that must be preserved.
	staleConf := `# Existing cluster configuration with user comments and custom settings
custom_work_mem = '64MB'

# --- BEGIN TOBITEN EMBEDDED CONFIGURATION ---
wal_compression = 'invalid_stale_algo'
default_toast_compression = 'stale_broken'
# --- END TOBITEN EMBEDDED CONFIGURATION ---
`
	if err := os.WriteFile(filepath.Join(dataDir, "postgresql.conf"), []byte(staleConf), 0600); err != nil {
		t.Fatalf("failed to write stale postgresql.conf: %v", err)
	}
	staleAutoContent := "wal_compression = 'stale_auto'\n"
	if err := os.WriteFile(filepath.Join(dataDir, "postgresql.auto.conf"), []byte(staleAutoContent), 0600); err != nil {
		t.Fatalf("failed to write stale postgresql.auto.conf: %v", err)
	}

	// 2. Prove contaminated input fails when evaluated directly without isolated probing.
	// The dummy fixture inspects effective -D and config inputs; evaluating against dataDir without --config-file=/dev/null
	// aborts with fatal configuration error on contaminated parameters.
	cmdDirect := exec.Command(filepath.Join(binDir, "postgres"), "-D", dataDir, "-c", "default_toast_compression=lz4", "-C", "default_toast_compression")
	directOut, directErr := cmdDirect.CombinedOutput()
	if directErr == nil {
		t.Fatalf("expected direct command with contaminated cluster input to fail, got success (output: %s)", string(directOut))
	}
	if !strings.Contains(string(directOut), "contains errors") {
		t.Fatalf("expected fatal configuration error from contaminated input, got:\n%s", string(directOut))
	}

	// 3. Supervisor startup with isolated probing (compressionProbe left unset to exercise ProbeCompression directly)
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s, err := New(Options{
		BinDir:            binDir,
		DataDir:           dataDir,
		Logger:            logger,
		isRoot:            func() bool { return false },
		resolvePIDTimeout: 10 * time.Millisecond,
		// compressionProbe left unset to exercise ProbeCompression directly!
	})
	if err != nil {
		t.Fatalf("unexpected error creating supervisor: %v", err)
	}

	ctx := context.Background()
	_ = s.Start(ctx)

	// 4. Prove isolated probing preserves partial support
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

	// Verify only unsupported setting warned, no probe failure
	logOutput := logBuf.String()
	if strings.Contains(logOutput, "failed to probe postgresql compression capabilities") {
		t.Fatalf("unexpected probe failure warning in log:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "zstd WAL compression") {
		t.Fatalf("expected warning for unsupported zstd WAL compression, got:\n%s", logOutput)
	}

	// 5. Prove cluster files and effective configuration are preserved
	confBytes, err := os.ReadFile(filepath.Join(dataDir, "postgresql.conf"))
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	confContent := string(confBytes)

	// User setting outside the Tobiten block was preserved
	if !strings.Contains(confContent, "custom_work_mem = '64MB'") {
		t.Fatalf("expected postgresql.conf to preserve pre-existing user settings outside Tobiten block, got:\n%s", confContent)
	}

	// Verify the Tobiten configuration block specifically
	startIdx := strings.Index(confContent, confBeginMarker)
	endIdx := strings.Index(confContent, confEndMarker)
	if startIdx == -1 || endIdx == -1 || endIdx < startIdx {
		t.Fatalf("expected tobiten config block in postgresql.conf, got:\n%s", confContent)
	}
	tobitenBlock := confContent[startIdx:endIdx]
	if !strings.Contains(tobitenBlock, "default_toast_compression = 'lz4'") {
		t.Fatalf("expected tobiten block to contain default_toast_compression = 'lz4', got:\n%s", tobitenBlock)
	}
	if strings.Contains(tobitenBlock, "wal_compression") {
		t.Fatalf("expected tobiten block to omit unsupported wal_compression, got:\n%s", tobitenBlock)
	}

	if !strings.Contains(confContent, "default_toast_compression = 'lz4'") {
		t.Fatalf("expected postgresql.conf to contain default_toast_compression = 'lz4', got:\n%s", confContent)
	}
	if strings.Contains(tobitenBlock, "wal_compression") {
		t.Fatalf("expected tobiten block to omit unsupported wal_compression, got:\n%s", tobitenBlock)
	}

	// Stale postgresql.auto.conf was preserved intact
	autoBytes, err := os.ReadFile(filepath.Join(dataDir, "postgresql.auto.conf"))
	if err != nil {
		t.Fatalf("failed to read postgresql.auto.conf: %v", err)
	}
	if string(autoBytes) != staleAutoContent {
		t.Fatalf("expected postgresql.auto.conf to be preserved, got %q", string(autoBytes))
	}
}

func TestSupervisor_CompressionSettings_ProbeErrorFallback(t *testing.T) {
	simulatedErr := errors.New("simulated probe failure")
	mockProbe := func(_ context.Context, _, _ string) (CompressionCapabilities, error) {
		return CompressionCapabilities{}, simulatedErr
	}

	s, dataDir, logBuf := setupTestSupervisorWithProbe(t, mockProbe)

	ctx := context.Background()
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

	ctx := context.Background()
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

data_dir=""
config_file=""

prev=""
for arg in "$@"; do
  if [ "$prev" = "-D" ]; then
    data_dir="$arg"
  elif [ "$prev" = "--config-file" ]; then
    config_file="$arg"
  fi
  case "$arg" in
    -D*)
      if [ "$arg" != "-D" ]; then
        data_dir="${arg#-D}"
      fi
      ;;
    --config-file=*)
      config_file="${arg#--config-file=}"
      ;;
  esac
  prev="$arg"
done

# Inspect effective configuration inputs if not bypassed by --config-file=/dev/null
if [ "$config_file" != "/dev/null" ]; then
  target_conf=""
  if [ -n "$config_file" ]; then
    target_conf="$config_file"
  elif [ -n "$data_dir" ]; then
    target_conf="$data_dir/postgresql.conf"
  fi

  if [ -n "$target_conf" ] && [ -f "$target_conf" ]; then
    if grep -q -E "invalid_stale_algo|stale_broken|syntax_error" "$target_conf" 2>/dev/null; then
      echo "FATAL: configuration file \"$target_conf\" contains errors" >&2
      exit 2
    fi
  fi

  if [ -n "$data_dir" ] && [ -f "$data_dir/postgresql.auto.conf" ]; then
    if grep -q -E "invalid_stale_algo|stale_broken|stale_auto|syntax_error|bad" "$data_dir/postgresql.auto.conf" 2>/dev/null; then
      echo "FATAL: configuration file \"$data_dir/postgresql.auto.conf\" contains errors" >&2
      exit 2
    fi
  fi
fi

for arg in "$@"; do
  case "$arg" in
    wal_compression=zstd|wal_compression)
      if [ "$has_zstd" = "true" ]; then
        echo "zstd"
        exit 0
      else
        echo "FATAL:  invalid value for parameter \"wal_compression\": \"zstd\"" >&2
        exit 1
      fi
      ;;
    default_toast_compression=lz4|default_toast_compression)
      if [ "$has_lz4" = "true" ]; then
        echo "lz4"
        exit 0
      else
        echo "FATAL:  invalid value for parameter \"default_toast_compression\": \"lz4\"" >&2
        exit 1
      fi
      ;;
  esac
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

	// Prove contaminated input fails when evaluated directly without isolated probing
	cmdDirect := exec.Command(filepath.Join(binDir, "postgres"), "-D", brokenDataDir, "-c", "wal_compression=zstd", "-C", "wal_compression")
	directOut, directErr := cmdDirect.CombinedOutput()
	if directErr == nil {
		t.Fatalf("expected direct command on broken dataDir to fail, got success (output: %s)", string(directOut))
	}
	if !strings.Contains(string(directOut), "contains errors") {
		t.Fatalf("expected fatal configuration error from broken dataDir, got:\n%s", string(directOut))
	}

	caps, err := ProbeCompression(context.Background(), binDir, brokenDataDir)
	if err != nil {
		t.Fatalf("ProbeCompression failed on dataDir with broken config: %v", err)
	}
	if !caps.HasZstdWAL || !caps.HasLZ4Toast {
		t.Fatalf("expected both capabilities true, got: %+v", caps)
	}
}

func TestProbeCompression_InternalDeadline_LiveParent(t *testing.T) {
	// Regression: verify that ProbeCompression enforces its internal timeout when given a live parent context,
	// and that child process termination and fixture cleanup are prompt and bounded (< 1s, never 10s).
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}

	// Use exec sleep so signals kill the process immediately without leaving child sh/sleep processes running
	p := filepath.Join(binDir, "postgres")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "postgres (PostgreSQL) 18.6"
  exit 0
fi
exec sleep 2
`
	if err := os.WriteFile(p, []byte(script), 0755); err != nil {
		t.Fatalf("failed to create sleeping postgres binary: %v", err)
	}

	start := time.Now()
	// Live parent context without a parent deadline: exercises ProbeCompression's internal deadline
	liveParentCtx := context.Background()
	_, err := ProbeCompressionWithTimeout(liveParentCtx, binDir, "", 100*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error from ProbeCompressionWithTimeout, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout error wrapping DeadlineExceeded, got: %v", err)
	}
	// Verify execution was bounded by the internal timeout (< 1 second, never the 10s trap)
	if elapsed > 1*time.Second {
		t.Fatalf("expected prompt bounded cancellation, but execution took %v", elapsed)
	}
}

func TestProbeCompression_PerSettingTimeout_ParentWithLaterDeadline(t *testing.T) {
	// Regression: verify that ProbeCompression enforces the per-setting probe timeout
	// even when the parent context has a much later deadline (e.g. 10s),
	// rather than inheriting the parent's late deadline.
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}

	p := filepath.Join(binDir, "postgres")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "postgres (PostgreSQL) 18.6"
  exit 0
fi
exec sleep 5
`
	if err := os.WriteFile(p, []byte(script), 0755); err != nil {
		t.Fatalf("failed to create sleeping postgres binary: %v", err)
	}

	start := time.Now()
	// Parent context has a 10s deadline, but probe has 100ms timeout
	parentCtx, parentCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer parentCancel()

	_, err := ProbeCompressionWithTimeout(parentCtx, binDir, "", 100*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error from ProbeCompressionWithTimeout, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout error wrapping DeadlineExceeded, got: %v", err)
	}
	// Verify execution was bounded by the per-setting timeout (< 1s, did not wait 5s or 10s)
	if elapsed > 1*time.Second {
		t.Fatalf("expected per-setting timeout enforcement (< 1s), but execution took %v", elapsed)
	}
	// Verify parent context remains active and uncancelled
	if parentCtx.Err() != nil {
		t.Fatalf("expected parent context to remain active, got: %v", parentCtx.Err())
	}
}

func TestProbeCompression_PreservesEarlierParentCancellationAndDeadlines(t *testing.T) {
	// Regression: verify that ProbeCompression preserves earlier parent context cancellation
	// and earlier parent deadlines, returning the parent context's error promptly.
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}

	p := filepath.Join(binDir, "postgres")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "postgres (PostgreSQL) 18.6"
  exit 0
fi
exec sleep 5
`
	if err := os.WriteFile(p, []byte(script), 0755); err != nil {
		t.Fatalf("failed to create sleeping postgres binary: %v", err)
	}

	t.Run("earlier_parent_deadline", func(t *testing.T) {
		start := time.Now()
		// Parent deadline 50ms, probe timeout 2s
		parentCtx, parentCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer parentCancel()

		_, err := ProbeCompressionWithTimeout(parentCtx, binDir, "", 2*time.Second)
		elapsed := time.Since(start)

		if err == nil {
			t.Fatal("expected error from parent deadline, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
		}
		if parentCtx.Err() != context.DeadlineExceeded {
			t.Fatalf("expected parentCtx.Err() == DeadlineExceeded, got: %v", parentCtx.Err())
		}
		if elapsed > 1*time.Second {
			t.Fatalf("expected prompt parent deadline cancellation (< 1s), took: %v", elapsed)
		}
	})

	t.Run("earlier_parent_cancellation", func(t *testing.T) {
		start := time.Now()
		// Parent cancel called at 30ms, probe timeout 2s
		parentCtx, parentCancel := context.WithCancel(context.Background())
		timer := time.AfterFunc(30*time.Millisecond, parentCancel)
		defer timer.Stop()

		_, err := ProbeCompressionWithTimeout(parentCtx, binDir, "", 2*time.Second)
		elapsed := time.Since(start)

		if err == nil {
			t.Fatal("expected error from parent cancellation, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
		if parentCtx.Err() != context.Canceled {
			t.Fatalf("expected parentCtx.Err() == Canceled, got: %v", parentCtx.Err())
		}
		if elapsed > 1*time.Second {
			t.Fatalf("expected prompt parent cancellation (< 1s), took: %v", elapsed)
		}
	})
}

func TestProbeSetting_StabilizedLocaleDoesNotHideCodec(t *testing.T) {
	// Regression: Translated PostgreSQL error messages (e.g. German, French, Japanese)
	// must not hide supported codecs or cause false probe errors.
	// stabilizedLocaleEnv ensures LC_ALL=C is passed to postgres -C.
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}

	// Dummy postgres script that checks LC_ALL:
	// If LC_ALL is "C", emits standard English rejection.
	// If LC_ALL is localized (de_DE), emits translated German rejection.
	p := filepath.Join(binDir, "postgres")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "postgres (PostgreSQL) 18.6"
  exit 0
fi

for arg in "$@"; do
  if [ "$arg" = "wal_compression=zstd" ]; then
    if [ "$LC_ALL" = "C" ]; then
      echo 'FATAL:  invalid value for parameter "wal_compression": "zstd"' >&2
    else
      echo 'FATAL:  ungueltiger Wert fuer Parameter "wal_compression": "zstd"' >&2
    fi
    exit 1
  fi
  if [ "$arg" = "default_toast_compression=lz4" ]; then
    echo "lz4"
    exit 0
  fi
done
exit 0
`
	if err := os.WriteFile(p, []byte(script), 0755); err != nil {
		t.Fatalf("failed to create dummy localized postgres binary: %v", err)
	}

	// Simulate host system configured with German locale
	t.Setenv("LC_ALL", "de_DE.UTF-8")
	t.Setenv("LANG", "de_DE.UTF-8")

	caps, err := ProbeCompression(context.Background(), binDir, "")
	if err != nil {
		t.Fatalf("ProbeCompression failed under localized host environment: %v", err)
	}
	if caps.HasZstdWAL {
		t.Fatalf("expected HasZstdWAL == false, got true")
	}
	if !caps.HasLZ4Toast {
		t.Fatalf("expected HasLZ4Toast == true, got false")
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
