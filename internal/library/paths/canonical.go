package paths

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const maxSymlinkDepth = 64

// CanonicalPath resolves existing symlinks, including links to not-yet-created
// targets, without cleaning away filesystem-dependent symlink/.. traversal.
// Arbitrary non-UTF-8 POSIX path bytes are preserved.
func CanonicalPath(path string) (string, error) {
	if strings.IndexByte(path, 0) >= 0 {
		return "", ErrInvalidRawPath
	}
	return canonicalizePath(path)
}

// CanonicalPathBytes validates that raw is non-empty and NUL-free, then resolves it
// with CanonicalPath while preserving raw POSIX path bytes.
func CanonicalPathBytes(raw []byte) ([]byte, error) {
	if len(raw) == 0 || bytes.IndexByte(raw, 0) >= 0 {
		return nil, ErrInvalidRawPath
	}
	canon, err := canonicalizePath(string(raw))
	if err != nil {
		return nil, err
	}
	return []byte(canon), nil
}

func initialPathComponents(path string) ([]string, error) {
	if path == "" {
		path = "."
	}
	if filepath.IsAbs(path) {
		return splitPathComponents(path), nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get current working directory: %w", err)
	}
	return append(splitPathComponents(cwd), splitPathComponents(path)...), nil
}

func resolveParentDir(resolved string) (string, error) {
	if resolved == "/" {
		return "/", nil
	}
	// Refuse '..' traversal through a component that does not exist on disk,
	// preserving filesystem semantics where non-existent paths cannot resolve '..'.
	fi, err := os.Lstat(resolved)
	if err != nil {
		return "", fmt.Errorf("cannot resolve '..' through non-existent path %q: %w", resolved, err)
	}
	if !fi.IsDir() {
		return "", fmt.Errorf("cannot resolve '..' through non-directory %q", resolved)
	}
	return filepath.Dir(resolved), nil
}

func resolveSymlinkComponent(candidate, resolved string, remaining []string, symlinks *int) (string, []string, error) {
	*symlinks++
	if *symlinks > maxSymlinkDepth {
		return "", nil, fmt.Errorf("too many levels of symbolic links: %q", candidate)
	}
	target, err := os.Readlink(candidate)
	if err != nil {
		return "", nil, fmt.Errorf("failed to readlink %q: %w", candidate, err)
	}
	if filepath.IsAbs(target) {
		resolved = "/"
	}
	return resolved, append(splitPathComponents(target), remaining...), nil
}

// canonicalizePath returns the canonical absolute path with all symlinks resolved,
// preserving filesystem symlink and ".." semantics even if intermediate leaf
// components or target directories do not exist yet.
func canonicalizePath(path string) (string, error) {
	remaining, err := initialPathComponents(path)
	if err != nil {
		return "", err
	}

	resolved := "/"
	symlinks := 0

	for len(remaining) > 0 {
		name := remaining[0]
		remaining = remaining[1:]

		switch name {
		case "", ".":
			continue
		case "..":
			resolved, err = resolveParentDir(resolved)
			if err != nil {
				return "", err
			}
			continue
		}

		candidate := filepath.Join(resolved, name)
		fi, err := os.Lstat(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				// Target does not exist on disk.
				// Append candidate and continue. Subsequent '..' will be safely refused.
				resolved = candidate
				continue
			}
			return "", fmt.Errorf("failed to stat %q: %w", candidate, err)
		}

		if fi.Mode()&os.ModeSymlink != 0 {
			resolved, remaining, err = resolveSymlinkComponent(candidate, resolved, remaining, &symlinks)
			if err != nil {
				return "", err
			}
			continue
		}

		resolved = candidate
	}

	return filepath.Clean(resolved), nil
}

func splitPathComponents(p string) []string {
	p = filepath.ToSlash(p)
	parts := strings.Split(p, "/")
	var res []string
	for _, part := range parts {
		if part != "" && part != "." {
			res = append(res, part)
		}
	}
	return res
}
