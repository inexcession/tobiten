// Package embedded provides supervision of a local PostgreSQL 18 instance for embedded storage mode (ADR-0001).
package embedded

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// PinnedMajor is the exact PostgreSQL major version required by Tobiten for embedded mode (ADR-0001 §4).
const PinnedMajor = 18

// RequiredBinaries are the PostgreSQL binaries required for embedded supervisor execution (ADR-0001).
var RequiredBinaries = []string{"pg_ctl", "initdb", "postgres"}

var versionRegex = regexp.MustCompile(`(?i)(?:postgres(?:ql)?|pg_ctl|initdb)\s+(?:\([^\)]+\)\s+)?(\d+)(?:\.(\d+))?`)

// ParsePgMajorVersion extracts the PostgreSQL major version integer from output such as
// "postgres (PostgreSQL) 18.6", "pg_ctl (PostgreSQL) 18.0", or "initdb (PostgreSQL) 18.1.2".
func ParsePgMajorVersion(versionStr string) (int, error) {
	matches := versionRegex.FindStringSubmatch(versionStr)
	if len(matches) < 2 {
		return 0, fmt.Errorf("unable to parse postgres version string: %q", versionStr)
	}
	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("invalid major version number in %q: %w", versionStr, err)
	}
	return major, nil
}

// ValidateBinDir verifies that dir contains the complete set of required PostgreSQL binaries
// (pg_ctl, initdb, postgres), that each binary is an executable regular file,
// and that each binary's major version satisfies the match predicate.
func ValidateBinDir(dir string, match func(major int) bool) error {
	if dir == "" {
		return errors.New("binary directory path is empty")
	}

	for _, name := range RequiredBinaries {
		binPath := filepath.Join(dir, name)
		fi, err := os.Stat(binPath)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("missing required postgresql binary %q in %s", name, dir)
			}
			return fmt.Errorf("cannot stat binary %q in %s: %w", name, dir, err)
		}
		if fi.IsDir() {
			return fmt.Errorf("binary %q in %s is a directory", name, dir)
		}
		if (fi.Mode().Perm() & 0111) == 0 {
			return fmt.Errorf("binary %q in %s is not executable", name, dir)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		cmd := exec.CommandContext(ctx, binPath, "--version")
		out, err := cmd.Output()
		cancel()
		if err != nil {
			return fmt.Errorf("failed to run %s --version: %w", name, err)
		}

		major, err := ParsePgMajorVersion(string(out))
		if err != nil {
			return fmt.Errorf("failed to parse version for %s: %w", name, err)
		}

		if match != nil && !match(major) {
			return fmt.Errorf("binary %q in %s has major version %d, which does not satisfy version requirements", name, dir, major)
		}
	}

	return nil
}

// IsValidBinDir reports whether ValidateBinDir returns nil.
func IsValidBinDir(dir string, match func(major int) bool) bool {
	return ValidateBinDir(dir, match) == nil
}

func candidateDirs(major int) []string {
	return []string{
		fmt.Sprintf("/opt/local/lib/postgresql%d/bin", major),
		fmt.Sprintf("/opt/homebrew/opt/postgresql@%d/bin", major),
		fmt.Sprintf("/usr/local/opt/postgresql@%d/bin", major),
		fmt.Sprintf("/usr/lib/postgresql/%d/bin", major),
	}
}

// FindBinDir discovers PostgreSQL binaries matching the product's pinned major (PinnedMajor = 18).
// Candidates checked in order:
// 1. explicitBinDir or TOBITEN_PG_BIN environment variable
// 2. MacPorts default: /opt/local/lib/postgresql18/bin
// 3. Homebrew Apple Silicon: /opt/homebrew/opt/postgresql@18/bin
// 4. Homebrew Intel: /usr/local/opt/postgresql@18/bin
// 5. Debian/Ubuntu: /usr/lib/postgresql/18/bin
// 6. PATH lookup for pg_ctl
func FindBinDir(explicitBinDir string) (string, error) {
	return FindBinDirMatching(explicitBinDir, func(major int) bool {
		return major == PinnedMajor
	})
}

// FindBinDirAtLeast discovers PostgreSQL binaries with major version >= minMajor.
// Used by test harnesses and tools that accept newer PostgreSQL installations.
func FindBinDirAtLeast(minMajor int, explicitBinDir string) (string, error) {
	return FindBinDirMatching(explicitBinDir, func(major int) bool {
		return major >= minMajor
	})
}

// FindBinDirMatching discovers PostgreSQL binaries using a custom version validation predicate.
func FindBinDirMatching(explicitBinDir string, match func(major int) bool) (string, error) {
	if explicitBinDir != "" {
		clean, err := filepath.Abs(explicitBinDir)
		if err != nil {
			return "", fmt.Errorf("failed to resolve absolute path for %q: %w", explicitBinDir, err)
		}
		if err := ValidateBinDir(clean, match); err != nil {
			return "", fmt.Errorf("explicit binary directory %q is invalid: %w", explicitBinDir, err)
		}
		return clean, nil
	}

	var candidates []string
	if envBin := os.Getenv("TOBITEN_PG_BIN"); envBin != "" {
		candidates = append(candidates, envBin)
	}

	candidates = append(candidates, candidateDirs(PinnedMajor)...)

	if pathBin, err := exec.LookPath("pg_ctl"); err == nil {
		candidates = append(candidates, filepath.Dir(pathBin))
	}

	seen := make(map[string]bool)
	for _, dir := range candidates {
		clean, err := filepath.Abs(dir)
		if err != nil {
			clean = filepath.Clean(dir)
		}
		if seen[clean] {
			continue
		}
		seen[clean] = true

		if err := ValidateBinDir(clean, match); err == nil {
			return clean, nil
		}
	}

	return "", fmt.Errorf("postgresql %d binaries not found: set TOBITEN_PG_BIN, install postgresql %d, or ensure pg_ctl is on PATH", PinnedMajor, PinnedMajor)
}
