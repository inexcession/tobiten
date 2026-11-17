package embedded

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Sentinel lock errors.
var (
	ErrDataDirLocked   = errors.New("embedded postgresql data directory is locked by another process")
	ErrSocketDirLocked = errors.New("embedded postgresql socket directory is locked by another process")
)

// CanonicalPath resolves existing symlinks, including links to not-yet-created
// targets, without cleaning away filesystem-dependent symlink/.. traversal.
func CanonicalPath(path string) (string, error) {
	return canonicalizePath(path)
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
	if *symlinks > 64 {
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

// DataDirLockPath returns the lock file path corresponding to a data directory.
// The lock file is placed in the parent directory alongside the canonical data directory,
// preserving initdb's requirement that the data directory itself be empty.
func DataDirLockPath(dataDir string) string {
	canonical, err := canonicalizePath(dataDir)
	if err != nil {
		canonical = filepath.Clean(dataDir)
	}
	return canonical + ".lock"
}

// SocketDirLockPath returns the lock file path for a socket directory.
func SocketDirLockPath(socketDir string) string {
	canonical, err := canonicalizePath(socketDir)
	if err != nil {
		canonical = filepath.Clean(socketDir)
	}
	return canonical + ".lock"
}

// fileLock represents an advisory cross-process file lock held via syscall.Flock.
type fileLock struct {
	path string
	file *os.File
}

// acquireFileLock attempts to acquire an exclusive, non-blocking advisory lock on path.
// If the lock is held by another process, it returns lockedErr (wrapping details of the holder).
func acquireFileLock(path string, lockedErr error, targetDesc string) (*fileLock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory for lock file %q: %w", path, err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file %q: %w", path, err)
	}

	flockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if flockErr != nil {
		_ = f.Close()
		if errors.Is(flockErr, syscall.EWOULDBLOCK) || errors.Is(flockErr, syscall.EAGAIN) {
			holderPID := ""
			if data, readErr := os.ReadFile(path); readErr == nil {
				holderPID = strings.TrimSpace(string(data))
			}
			if holderPID != "" {
				return nil, fmt.Errorf("%w: %s (held by PID %s in lock file %s)", lockedErr, targetDesc, holderPID, path)
			}
			return nil, fmt.Errorf("%w: %s (lock file %s)", lockedErr, targetDesc, path)
		}
		return nil, fmt.Errorf("failed to acquire lock on %q: %w", path, flockErr)
	}

	// Write current process PID into the lock file for diagnostics.
	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())
	_ = f.Sync()

	return &fileLock{path: path, file: f}, nil
}

// Unlock releases the advisory lock and closes the underlying file descriptor.
// It does not delete the lock file on disk, avoiding inode-divergence race conditions.
func (l *fileLock) Unlock() error {
	if l == nil || l.file == nil {
		return nil
	}
	f := l.file
	l.file = nil
	defer func() { _ = f.Close() }()
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// LockDataDir acquires exclusive ownership of the PostgreSQL data directory.
// It resolves symlinks to use the canonical cluster identity, and returns an unlock
// function or ErrDataDirLocked if another process holds it.
func LockDataDir(dataDir string) (func() error, error) {
	canonical, err := canonicalizePath(dataDir)
	if err != nil {
		canonical = filepath.Clean(dataDir)
	}
	lockPath := canonical + ".lock"
	lock, err := acquireFileLock(lockPath, ErrDataDirLocked, canonical)
	if err != nil {
		return nil, err
	}
	return lock.Unlock, nil
}
