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

// DataDirLockPath returns the lock file path corresponding to a data directory.
// The lock file is placed in the parent directory alongside the data directory,
// preserving initdb's requirement that the data directory itself be empty.
func DataDirLockPath(dataDir string) string {
	clean := filepath.Clean(dataDir)
	return clean + ".lock"
}

// SocketDirLockPath returns the lock file path for a socket directory.
func SocketDirLockPath(socketDir string) string {
	clean := filepath.Clean(socketDir)
	return clean + ".lock"
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
// It returns an unlock function or ErrDataDirLocked if another process holds it.
func LockDataDir(dataDir string) (func() error, error) {
	lockPath := DataDirLockPath(dataDir)
	lock, err := acquireFileLock(lockPath, ErrDataDirLocked, dataDir)
	if err != nil {
		return nil, err
	}
	return lock.Unlock, nil
}
