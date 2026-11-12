package embedded

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// CompressionCapabilities describes the compression algorithms supported by a PostgreSQL build (ADR-0001 §2).
type CompressionCapabilities struct {
	// HasZstdWAL indicates whether the PostgreSQL build supports wal_compression = 'zstd'.
	HasZstdWAL bool
	// HasLZ4Toast indicates whether the PostgreSQL build supports default_toast_compression = 'lz4'.
	HasLZ4Toast bool
}

func probeSetting(ctx context.Context, postgresBin, dataDir, setting, value string) (bool, error) {
	cmdCtx, cancel := deriveBoundedContext(ctx, 3*time.Second)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, postgresBin,
		"-D", dataDir,
		"-c", fmt.Sprintf("%s=%s", setting, value),
		"-C", setting,
	)

	out, err := cmd.Output()
	if err == nil {
		if strings.TrimSpace(string(out)) == value {
			return true, nil
		}
		return false, nil
	}

	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		// PostgreSQL evaluated the parameter and rejected it (unsupported method).
		return false, nil
	}

	return false, fmt.Errorf("failed to run postgres -C %s: %w", setting, err)
}

// ProbeCompression detects whether the PostgreSQL binary set supports zstd WAL compression
// and lz4 TOAST compression (ADR-0001 §2).
func ProbeCompression(ctx context.Context, binDir, dataDir string) (CompressionCapabilities, error) {
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

	targetDir := dataDir
	var cleanUpTemp bool
	if targetDir == "" || !clusterVersionFileExists(targetDir) {
		tmpDir, err := os.MkdirTemp("", "tobiten-pg-probe-*")
		if err != nil {
			return CompressionCapabilities{}, fmt.Errorf("failed to create temporary probe directory: %w", err)
		}
		targetDir = tmpDir
		cleanUpTemp = true
		defer func() {
			if cleanUpTemp {
				_ = os.RemoveAll(tmpDir)
			}
		}()

		// Write minimal configuration and version file required for postgres -C
		pgVersionPath := filepath.Join(targetDir, "PG_VERSION")
		if err := os.WriteFile(pgVersionPath, []byte(fmt.Sprintf("%d\n", PinnedMajor)), 0600); err != nil {
			return CompressionCapabilities{}, fmt.Errorf("failed to write probe PG_VERSION: %w", err)
		}
		confPath := filepath.Join(targetDir, "postgresql.conf")
		if err := os.WriteFile(confPath, []byte(""), 0600); err != nil {
			return CompressionCapabilities{}, fmt.Errorf("failed to write probe postgresql.conf: %w", err)
		}
	}

	hasZstd, err := probeSetting(ctx, postgresBin, targetDir, "wal_compression", "zstd")
	if err != nil {
		return CompressionCapabilities{}, err
	}

	hasLZ4, err := probeSetting(ctx, postgresBin, targetDir, "default_toast_compression", "lz4")
	if err != nil {
		return CompressionCapabilities{}, err
	}

	return CompressionCapabilities{
		HasZstdWAL:  hasZstd,
		HasLZ4Toast: hasLZ4,
	}, nil
}

func clusterVersionFileExists(dataDir string) bool {
	fi, err := os.Stat(filepath.Join(dataDir, "PG_VERSION"))
	return err == nil && !fi.IsDir()
}
