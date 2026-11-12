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

// canonicalizePath returns the canonical absolute path with all symlinks resolved,
// even if the target file/directory or intermediate leaf components do not exist yet.
func canonicalizePath(path string) (string, error) {
	return canonicalizePathDepth(path, 0)
}

func canonicalizePathDepth(path string, depth int) (string, error) {
	if depth > 64 {
		return "", fmt.Errorf("too many levels of symbolic links: %q", path)
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path for %q: %w", path, err)
	}
	abs = filepath.Clean(abs)

	// If the path itself exists and can be fully evaluated, do so.
	if realPath, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(realPath), nil
	}

	// If abs exists as a symlink pointing to a non-existent target (broken or not-yet-created leaf):
	if fi, err := os.Lstat(abs); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(abs)
		if err == nil {
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(abs), target)
			}
			return canonicalizePathDepth(target, depth+1)
		}
	}

	// If the path does not exist, find the lowest existing ancestor,
	// resolve symlinks on it, and re-append the remaining components.
	curr := abs
	var trailing []string
	for {
		parent := filepath.Dir(curr)
		if parent == curr {
			break
		}
		trailing = append([]string{filepath.Base(curr)}, trailing...)
		curr = parent

		if fi, err := os.Lstat(curr); err == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				target, err := os.Readlink(curr)
				if err == nil {
					if !filepath.IsAbs(target) {
						target = filepath.Join(filepath.Dir(curr), target)
					}
					resolvedParent, err := canonicalizePathDepth(target, depth+1)
					if err == nil {
						curr = resolvedParent
						break
					}
				}
			}
			if resolved, err := filepath.EvalSymlinks(curr); err == nil {
				curr = resolved
				break
			}
		}
	}

	result := curr
	for _, seg := range trailing {
		result = filepath.Join(result, seg)
	}
	result = filepath.Clean(result)

	if result != abs {
		return canonicalizePathDepth(result, depth+1)
	}
	return result, nil
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
