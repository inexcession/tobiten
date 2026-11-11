// Package embedded provides supervision of a local PostgreSQL 18 instance for embedded storage mode (ADR-0001).
package embedded

import (
	"context"
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

// IsValidBinDir checks whether dir contains executable pg_ctl and initdb binaries,
// and whether pg_ctl --version satisfies the match predicate.
func IsValidBinDir(dir string, match func(major int) bool) bool {
	pgCtl := filepath.Join(dir, "pg_ctl")
	initdb := filepath.Join(dir, "initdb")

	if fi, err := os.Stat(pgCtl); err != nil || fi.IsDir() || (fi.Mode().Perm()&0111) == 0 {
		return false
	}
	if fi, err := os.Stat(initdb); err != nil || fi.IsDir() || (fi.Mode().Perm()&0111) == 0 {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, pgCtl, "--version")
	out, err := cmd.Output()
	if err != nil {
		return false
	}

	major, err := ParsePgMajorVersion(string(out))
	if err != nil {
		return false
	}

	if match != nil && !match(major) {
		return false
	}

	return true
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
	var candidates []string

	if explicitBinDir != "" {
		candidates = append(candidates, explicitBinDir)
	} else if envBin := os.Getenv("TOBITEN_PG_BIN"); envBin != "" {
		candidates = append(candidates, envBin)
	}

	candidates = append(candidates, candidateDirs(PinnedMajor)...)

	if pathBin, err := exec.LookPath("pg_ctl"); err == nil {
		candidates = append(candidates, filepath.Dir(pathBin))
	}

	seen := make(map[string]bool)
	for _, dir := range candidates {
		clean := filepath.Clean(dir)
		if seen[clean] {
			continue
		}
		seen[clean] = true

		if IsValidBinDir(clean, match) {
			return clean, nil
		}
	}

	return "", fmt.Errorf("postgresql %d binaries not found: set TOBITEN_PG_BIN, install postgresql %d, or ensure pg_ctl is on PATH", PinnedMajor, PinnedMajor)
}
