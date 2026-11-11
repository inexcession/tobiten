package embedded

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Sentinel errors for cluster version validation (ADR-0001 §4).
var (
	// ErrOlderPGVersion is returned when an existing database cluster was initialized with an older PostgreSQL major version.
	// It provides actionable guidance pointing to 'tobiten-cli db upgrade'.
	ErrOlderPGVersion = errors.New("postgresql cluster version is older than pinned major: run 'tobiten-cli db upgrade' to upgrade your database")

	// ErrIncompatiblePGVersion is returned when an existing database cluster version is newer than the pinned major.
	ErrIncompatiblePGVersion = errors.New("incompatible postgresql cluster version")

	// ErrInvalidPGVersion is returned when the PG_VERSION file is missing required version data or corrupted.
	ErrInvalidPGVersion = errors.New("invalid or corrupted PG_VERSION file")
)

// FilesystemInfo contains information about a filesystem mount.
type FilesystemInfo struct {
	Type      string
	IsNetwork bool
}

// ParseClusterVersion parses the PostgreSQL major version integer from the contents of a PG_VERSION file.
func ParseClusterVersion(content string) (int, error) {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return 0, fmt.Errorf("%w: file is empty", ErrInvalidPGVersion)
	}
	parts := strings.SplitN(trimmed, ".", 2)
	major, err := strconv.Atoi(parts[0])
	if err != nil || major <= 0 {
		return 0, fmt.Errorf("%w: invalid major version %q: %v", ErrInvalidPGVersion, trimmed, err)
	}
	return major, nil
}

// ReadClusterVersion reads and parses the PostgreSQL major version from PG_VERSION in dataDir.
// If PG_VERSION does not exist, it returns 0, os.ErrNotExist.
func ReadClusterVersion(dataDir string) (int, error) {
	versionPath := filepath.Join(dataDir, "PG_VERSION")
	data, err := os.ReadFile(versionPath)
	if err != nil {
		return 0, err
	}
	return ParseClusterVersion(string(data))
}

// ValidateClusterVersion validates that the PostgreSQL cluster in dataDir matches PinnedMajor (ADR-0001 §4).
// If PG_VERSION does not exist, it returns nil (fresh cluster directory).
// If PG_VERSION is older than PinnedMajor, it returns an error wrapping ErrOlderPGVersion pointing to 'tobiten-cli db upgrade'.
// If PG_VERSION is newer than PinnedMajor, it returns an error wrapping ErrIncompatiblePGVersion.
func ValidateClusterVersion(dataDir string) error {
	ver, err := ReadClusterVersion(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		if errors.Is(err, ErrInvalidPGVersion) {
			return err
		}
		return fmt.Errorf("failed to validate cluster version in %s: %w", dataDir, err)
	}

	if ver < PinnedMajor {
		return fmt.Errorf("%w: cluster has PostgreSQL version %d, but Tobiten requires PostgreSQL %d (run 'tobiten-cli db upgrade' to upgrade): %s",
			ErrOlderPGVersion, ver, PinnedMajor, dataDir)
	}

	if ver > PinnedMajor {
		return fmt.Errorf("%w: cluster has PostgreSQL version %d, which is newer than pinned major %d: %s",
			ErrIncompatiblePGVersion, ver, PinnedMajor, dataDir)
	}

	return nil
}

// findExistingAncestor traverses path upwards until an existing directory is found.
func findExistingAncestor(path string) string {
	cur := filepath.Clean(path)
	for {
		fi, err := os.Stat(cur)
		if err == nil {
			if fi.IsDir() {
				return cur
			}
			return filepath.Dir(cur)
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return cur
		}
		cur = parent
	}
}

// DetectFilesystem returns filesystem information for the specified path using statfs.
func DetectFilesystem(path string) (FilesystemInfo, error) {
	return statfsInfo(path)
}
