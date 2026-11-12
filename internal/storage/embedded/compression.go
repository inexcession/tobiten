package embedded

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// DefaultProbeTimeout is the default bounded timeout for probing compression capabilities.
const DefaultProbeTimeout = 3 * time.Second

// CompressionCapabilities describes the compression algorithms supported by a PostgreSQL build (ADR-0001 §2).
type CompressionCapabilities struct {
	// HasZstdWAL indicates whether the PostgreSQL build supports wal_compression = 'zstd'.
	HasZstdWAL bool
	// HasLZ4Toast indicates whether the PostgreSQL build supports default_toast_compression = 'lz4'.
	HasLZ4Toast bool
}

// stabilizedLocaleEnv returns the process environment with LC_* and LANG variables overridden to C
// to ensure PostgreSQL parameter rejection messages are emitted in standard untranslated English.
func stabilizedLocaleEnv() []string {
	env := os.Environ()
	filtered := make([]string, 0, len(env)+3)
	for _, kv := range env {
		if strings.HasPrefix(kv, "LC_") || strings.HasPrefix(kv, "LANG=") {
			continue
		}
		filtered = append(filtered, kv)
	}
	return append(filtered, "LC_ALL=C", "LC_MESSAGES=C", "LANG=C")
}

func probeSetting(ctx context.Context, postgresBin, probeDir, setting, value string, timeout time.Duration) (bool, error) {
	if timeout <= 0 {
		timeout = DefaultProbeTimeout
	}
	cmdCtx, cancel := deriveBoundedContext(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, postgresBin,
		"-D", probeDir,
		"--config-file=/dev/null",
		"-c", fmt.Sprintf("%s=%s", setting, value),
		"-C", setting,
	)
	cmd.Env = stabilizedLocaleEnv()
	cmd.WaitDelay = 500 * time.Millisecond

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		if strings.TrimSpace(stdout.String()) == value {
			return true, nil
		}
		return false, fmt.Errorf("unexpected output probing %s: %q", setting, stdout.String())
	}

	// 1. Check for command context timeout or caller context cancellation
	if cmdCtx.Err() != nil {
		if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
			return false, fmt.Errorf("probe of %s timed out: %w", setting, cmdCtx.Err())
		}
		return false, cmdCtx.Err()
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	// 2. Check if this is a genuine parameter rejection by PostgreSQL (unsupported setting)
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		errStr := stderr.String()
		if strings.Contains(errStr, "invalid value for parameter") && strings.Contains(errStr, setting) {
			return false, nil
		}
	}

	// 3. Any other failure is an unrelated command failure (crash, syntax error, missing library)
	return false, fmt.Errorf("failed to probe %s: %w (stderr: %s)", setting, err, strings.TrimSpace(stderr.String()))
}

// ProbeCompression detects whether the PostgreSQL binary set supports zstd WAL compression
// and lz4 TOAST compression (ADR-0001 §2).
// Probes run against an isolated temporary directory with zero configuration, ensuring that
// stale or invalid settings in existing cluster files never contaminate capability detection.
func ProbeCompression(ctx context.Context, binDir, dataDir string) (CompressionCapabilities, error) {
	return ProbeCompressionWithTimeout(ctx, binDir, dataDir, DefaultProbeTimeout)
}

// ProbeCompressionWithTimeout detects compression capabilities with an explicit per-setting timeout.
func ProbeCompressionWithTimeout(ctx context.Context, binDir, _ string, timeout time.Duration) (CompressionCapabilities, error) {
	if binDir == "" {
		discovered, err := FindBinDir("")
		if err != nil {
			return CompressionCapabilities{}, fmt.Errorf("failed to locate postgresql binaries: %w", err)
		}
		binDir = discovered
	}

	postgresBin := filepath.Join(binDir, "postgres")
	if fi, err := os.Stat(postgresBin); err != nil {
		return CompressionCapabilities{}, fmt.Errorf("missing postgres binary in %s: %w", binDir, err)
	} else if fi.IsDir() || (fi.Mode().Perm()&0111) == 0 {
		return CompressionCapabilities{}, fmt.Errorf("postgres binary in %s is not executable", binDir)
	}

	// Always use a clean, isolated temporary directory for capability probing
	tmpDir, err := os.MkdirTemp("", "tobiten-pg-probe-*")
	if err != nil {
		return CompressionCapabilities{}, fmt.Errorf("failed to create temporary probe directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	pgVersionPath := filepath.Join(tmpDir, "PG_VERSION")
	if err := os.WriteFile(pgVersionPath, []byte(fmt.Sprintf("%d\n", PinnedMajor)), 0600); err != nil {
		return CompressionCapabilities{}, fmt.Errorf("failed to write probe PG_VERSION: %w", err)
	}
	confPath := filepath.Join(tmpDir, "postgresql.conf")
	if err := os.WriteFile(confPath, []byte(""), 0600); err != nil {
		return CompressionCapabilities{}, fmt.Errorf("failed to write probe postgresql.conf: %w", err)
	}

	hasZstd, err := probeSetting(ctx, postgresBin, tmpDir, "wal_compression", "zstd", timeout)
	if err != nil {
		return CompressionCapabilities{}, err
	}

	hasLZ4, err := probeSetting(ctx, postgresBin, tmpDir, "default_toast_compression", "lz4", timeout)
	if err != nil {
		return CompressionCapabilities{}, err
	}

	return CompressionCapabilities{
		HasZstdWAL:  hasZstd,
		HasLZ4Toast: hasLZ4,
	}, nil
}
