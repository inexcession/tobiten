package embedded

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func init() {
	if os.Getenv("TOBITEN_TEST_SUBPROCESS_SUPERVISE") == "1" {
		runSubprocessSupervisor()
	}
}

func runSubprocessSupervisor() {
	dataDir := os.Getenv("TOBITEN_TEST_SUBPROCESS_DATADIR")
	binDir := os.Getenv("TOBITEN_TEST_SUBPROCESS_BINDIR")
	s, err := New(Options{
		DataDir: dataDir,
		BinDir:  binDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "new error: %v\n", err)
		os.Exit(2)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = s.Start(startCtx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start error: %v\n", err)
		os.Exit(1)
	}

	_ = s.Stop(context.Background())
	os.Exit(0)
}

func writeFakeBinaries(t *testing.T, binDir string) {
	t.Helper()
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}

	writeExecutable := func(name, script string) {
		p := filepath.Join(binDir, name)
		if err := os.WriteFile(p, []byte(script), 0755); err != nil {
			t.Fatalf("failed to write %s: %v", name, err)
		}
	}

	writeExecutable("initdb", "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo \"initdb (PostgreSQL) 18.6\"; exit 0; fi\nexit 0\n")
	writeExecutable("postgres", "#!/bin/sh\nif [ \"$1\" = \"--version\" ] || [ \"$1\" = \"-V\" ]; then echo \"postgres (PostgreSQL) 18.6\"; exit 0; fi\nexit 0\n")

	pgCtlScript := `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "pg_ctl (PostgreSQL) 18.6"
  exit 0
fi

dataDir=""
mode=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -D) dataDir="$2"; shift 2 ;;
    start) mode="start"; shift ;;
    stop) mode="stop"; shift ;;
    *) shift ;;
  esac
done

if [ "$mode" = "start" ]; then
  sh -c '
    echo "$$" > "'"$dataDir"'/postmaster.pid"
    while true; do sleep 1; done
  ' >/dev/null 2>&1 </dev/null &
  child_pid=$!
  echo "$child_pid" > "$dataDir/.forked"
  exit 0
fi

if [ "$mode" = "stop" ]; then
  if [ -f "$dataDir/postmaster.pid" ]; then
    pid=$(head -n 1 "$dataDir/postmaster.pid")
    if [ -n "$pid" ]; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$dataDir/postmaster.pid"
  fi
  exit 0
fi
`
	writeExecutable("pg_ctl", pgCtlScript)
}

func TestLock_DataDirLockPath(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"/data/postgres", "/data/postgres.lock"},
		{"/data/postgres/", "/data/postgres.lock"},
		{"postgres", "postgres.lock"},
		{"./data/postgres", "data/postgres.lock"},
	}

	for _, c := range cases {
		got := DataDirLockPath(c.input)
		if got != c.expected {
			t.Errorf("DataDirLockPath(%q) = %q, expected %q", c.input, got, c.expected)
		}
	}
}

func TestLock_SocketDirLockPath(t *testing.T) {
	got := SocketDirLockPath("/tmp/tbt-1234")
	expected := "/tmp/tbt-1234.lock"
	if got != expected {
		t.Fatalf("SocketDirLockPath = %q, expected %q", got, expected)
	}
}

func TestLock_AcquireAndRelease(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "pgdata")
	lockPath := DataDirLockPath(dataDir)

	// 1. Acquire lock
	lock1, err := acquireFileLock(lockPath, ErrDataDirLocked, dataDir)
	if err != nil {
		t.Fatalf("failed to acquire initial lock: %v", err)
	}

	// Lock file should exist and contain our PID
	content, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("failed to read lock file: %v", err)
	}
	pidStr := strings.TrimSpace(string(content))
	if pidStr != strconv.Itoa(os.Getpid()) {
		t.Fatalf("expected PID %d in lock file, got %q", os.Getpid(), pidStr)
	}

	// 2. Second acquire while first is held must fail with ErrDataDirLocked
	_, err = acquireFileLock(lockPath, ErrDataDirLocked, dataDir)
	if err == nil {
		t.Fatal("expected second acquireFileLock to fail, got nil")
	}
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("expected ErrDataDirLocked, got: %v", err)
	}
	if !strings.Contains(err.Error(), strconv.Itoa(os.Getpid())) {
		t.Fatalf("expected error to name holding PID %d, got: %v", os.Getpid(), err)
	}
	if !strings.Contains(err.Error(), lockPath) {
		t.Fatalf("expected error to name lock path %s, got: %v", lockPath, err)
	}

	// 3. Unlock first lock
	if err := lock1.Unlock(); err != nil {
		t.Fatalf("failed to unlock: %v", err)
	}

	// 4. Second acquire should now succeed
	lock2, err := acquireFileLock(lockPath, ErrDataDirLocked, dataDir)
	if err != nil {
		t.Fatalf("failed to acquire lock after unlock: %v", err)
	}
	_ = lock2.Unlock()
}

func TestLock_StaleLockFileFromDeadProcess(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "pgdata")
	lockPath := DataDirLockPath(dataDir)

	// Write a dead PID into the lock file without holding any flock
	if err := os.WriteFile(lockPath, []byte("99999999\n"), 0600); err != nil {
		t.Fatalf("failed to write lock file: %v", err)
	}

	// acquireFileLock must succeed because no process holds flock on it
	lock, err := acquireFileLock(lockPath, ErrDataDirLocked, dataDir)
	if err != nil {
		t.Fatalf("expected acquireFileLock to succeed on unheld stale lock file: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	// Content must now be our PID
	content, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("failed to read lock file: %v", err)
	}
	if strings.TrimSpace(string(content)) != strconv.Itoa(os.Getpid()) {
		t.Fatalf("expected PID %d in lock file, got %q", os.Getpid(), strings.TrimSpace(string(content)))
	}
}

func TestLock_LockDataDirPublicAPI(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "pgdata")

	unlock, err := LockDataDir(dataDir)
	if err != nil {
		t.Fatalf("expected LockDataDir to succeed: %v", err)
	}

	// Second call must fail
	_, err = LockDataDir(dataDir)
	if err == nil {
		t.Fatal("expected second LockDataDir to fail, got nil")
	}
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("expected ErrDataDirLocked, got: %v", err)
	}

	// Unlock
	if err := unlock(); err != nil {
		t.Fatalf("unlock failed: %v", err)
	}

	// Now third call succeeds
	unlock2, err := LockDataDir(dataDir)
	if err != nil {
		t.Fatalf("expected third LockDataDir to succeed after unlock: %v", err)
	}
	_ = unlock2()
}

func TestSupervisor_SecondSupervisorRefused_SameProcess(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	dataDir := filepath.Join(tmpDir, "pgdata")
	writeFakeBinaries(t, binDir)

	s1, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create s1: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s1.Start(ctx); err != nil {
		t.Fatalf("s1.Start failed: %v", err)
	}
	defer func() { _ = s1.Stop(context.Background()) }()

	if !s1.DataDirLocked() {
		t.Fatal("expected s1 to hold DataDirLock")
	}

	// Create supervisor 2 for the same data directory
	s2, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create s2: %v", err)
	}

	err = s2.Start(ctx)
	if err == nil {
		_ = s2.Stop(context.Background())
		t.Fatal("expected s2.Start to fail with data directory lock error, got nil")
	}
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("expected ErrDataDirLocked, got: %v", err)
	}
	if !strings.Contains(err.Error(), s1.DataLockPath()) {
		t.Fatalf("expected error to name lock file %s, got: %v", s1.DataLockPath(), err)
	}
	if !strings.Contains(err.Error(), strconv.Itoa(os.Getpid())) {
		t.Fatalf("expected error to name holding PID %d, got: %v", os.Getpid(), err)
	}

	// Stop s1 cleanly
	if err := s1.Stop(context.Background()); err != nil {
		t.Fatalf("s1.Stop failed: %v", err)
	}
	if s1.DataDirLocked() {
		t.Fatal("expected s1 to have released DataDirLock after Stop")
	}

	// Now s2.Start must succeed
	if err := s2.Start(ctx); err != nil {
		t.Fatalf("expected s2.Start to succeed after s1 stopped: %v", err)
	}
	if !s2.DataDirLocked() {
		t.Fatal("expected s2 to hold DataDirLock")
	}
	if err := s2.Stop(context.Background()); err != nil {
		t.Fatalf("s2.Stop failed: %v", err)
	}
}

func TestSupervisor_CrossProcessExclusion(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	dataDir := filepath.Join(tmpDir, "pgdata")
	writeFakeBinaries(t, binDir)

	s1, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create s1: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s1.Start(ctx); err != nil {
		t.Fatalf("s1.Start failed: %v", err)
	}
	defer func() { _ = s1.Stop(context.Background()) }()

	// Spawn actual second OS subprocess attempting to supervise the same data directory
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+dataDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected second process to exit with non-zero exit code while s1 is running, output: %s", string(out))
	}

	outStr := string(out)
	if !strings.Contains(outStr, "embedded postgresql data directory is locked by another process") {
		t.Fatalf("expected clear lock error in subprocess output, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, strconv.Itoa(os.Getpid())) {
		t.Fatalf("expected subprocess output to cite holding PID %d, got:\n%s", os.Getpid(), outStr)
	}
	if !strings.Contains(outStr, s1.DataLockPath()) {
		t.Fatalf("expected subprocess output to name lock file %s, got:\n%s", s1.DataLockPath(), outStr)
	}

	// Stop s1
	if err := s1.Stop(context.Background()); err != nil {
		t.Fatalf("s1.Stop failed: %v", err)
	}

	// Now second subprocess should succeed
	cmd2 := exec.Command(os.Args[0])
	cmd2.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+dataDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("expected second process to succeed after s1 stopped, got: %v (output: %s)", err2, string(out2))
	}
}

func TestSupervisor_TempSocketDir_LockAndNamespaceSafety(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	writeFakeBinaries(t, binDir)

	// Create very long data path to force fallback to deterministic temporary socket dir
	longDataDir := filepath.Join(tmpDir, strings.Repeat("long_path_segment_", 6), "pgdata")
	if err := os.MkdirAll(longDataDir, 0700); err != nil {
		t.Fatalf("failed to create longDataDir: %v", err)
	}

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: longDataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create supervisor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start supervisor with temp socket: %v", err)
	}
	defer func() { _ = s.Stop(context.Background()) }()

	if !s.SocketDirLocked() {
		t.Fatal("expected SocketDirLocked() to be true for temporary socket dir")
	}
	sockLockPath := s.SocketLockPath()
	if sockLockPath == "" {
		t.Fatal("expected non-empty SocketLockPath()")
	}

	// Direct attempt to lock the same socket lock path must fail with ErrSocketDirLocked
	_, err = acquireFileLock(sockLockPath, ErrSocketDirLocked, s.SocketDir())
	if err == nil {
		t.Fatal("expected acquireFileLock on socket lock path to fail while held, got nil")
	}
	if !errors.Is(err, ErrSocketDirLocked) {
		t.Fatalf("expected ErrSocketDirLocked, got: %v", err)
	}

	sockDir := s.SocketDir()
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("failed to stop supervisor: %v", err)
	}

	if s.SocketDirLocked() {
		t.Fatal("expected SocketDirLocked() to be false after Stop")
	}
	// Newly created temp socket directory should have been removed
	if _, statErr := os.Stat(sockDir); !os.IsNotExist(statErr) {
		t.Fatalf("expected newly created temp socket dir %s to be removed after Stop, but it exists", sockDir)
	}
}

func TestSupervisor_TempSocketDir_PreExistingContentsPreserved(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	writeFakeBinaries(t, binDir)

	longDataDir := filepath.Join(tmpDir, strings.Repeat("nested_long_path_", 6), "pgdata")
	if err := os.MkdirAll(longDataDir, 0700); err != nil {
		t.Fatalf("failed to create longDataDir: %v", err)
	}

	// Compute deterministic temp socket dir
	absLongData, _ := filepath.Abs(longDataDir)
	h := sha256.Sum256([]byte(absLongData))
	deterministicSockDir := filepath.Join(os.TempDir(), fmt.Sprintf("tbt-%x", h[:4]))

	// Pre-create the directory with a foreign file
	if err := os.MkdirAll(deterministicSockDir, 0700); err != nil {
		t.Fatalf("failed to create deterministicSockDir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(deterministicSockDir)
		_ = os.Remove(SocketDirLockPath(deterministicSockDir))
	}()

	sentinelFile := filepath.Join(deterministicSockDir, "preserve_me.txt")
	if err := os.WriteFile(sentinelFile, []byte("pre-existing content\n"), 0644); err != nil {
		t.Fatalf("failed to write sentinel file: %v", err)
	}

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: longDataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create supervisor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start supervisor: %v", err)
	}

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("failed to stop supervisor: %v", err)
	}

	// Pre-existing directory and its sentinel file MUST NOT be removed!
	if _, statErr := os.Stat(deterministicSockDir); os.IsNotExist(statErr) {
		t.Fatalf("expected pre-existing socket directory %s to be preserved, but it was removed", deterministicSockDir)
	}
	data, readErr := os.ReadFile(sentinelFile)
	if readErr != nil {
		t.Fatalf("expected sentinel file %s to be preserved, got error: %v", sentinelFile, readErr)
	}
	if string(data) != "pre-existing content\n" {
		t.Fatalf("sentinel file content altered: %q", string(data))
	}
}

func TestSupervisor_FailedStopRetainsLock_ExcludesSecondProcess(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	dataDir := filepath.Join(tmpDir, "pgdata")
	writeFakeBinaries(t, binDir)

	confirmCount := 0
	s, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
		confirmExit: func(pid int, timeout time.Duration) error {
			confirmCount++
			if confirmCount == 1 {
				// Simulate failure on first Stop
				return errors.New("simulated exit confirmation failure")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("failed to create supervisor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("s.Start failed: %v", err)
	}
	defer func() { _ = s.Stop(context.Background()) }()

	// First Stop fails due to exit confirmation failure
	stopErr := s.Stop(context.Background())
	if stopErr == nil {
		t.Fatal("expected first Stop to fail, got nil")
	}

	// Ownership must be RETAINED!
	if !s.DataDirLocked() {
		t.Fatal("expected DataDirLock to be retained after failed Stop")
	}
	if !s.Running() {
		t.Fatal("expected Running() to be true (retryable state) after failed Stop")
	}

	// Second process attempting to supervise MUST be excluded
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+dataDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected second process to be excluded while ownership is retained after failed Stop, output: %s", string(out))
	}
	if !strings.Contains(string(out), "embedded postgresql data directory is locked by another process") {
		t.Fatalf("expected clear lock error in subprocess output, got:\n%s", string(out))
	}

	// Retry Stop: this time confirmExit succeeds
	retryErr := s.Stop(context.Background())
	if retryErr != nil {
		t.Fatalf("expected retry Stop to succeed, got: %v", retryErr)
	}

	if s.DataDirLocked() {
		t.Fatal("expected DataDirLock to be released after successful retry Stop")
	}

	// Second process can now succeed
	cmd2 := exec.Command(os.Args[0])
	cmd2.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+dataDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("expected second process to succeed after retry Stop succeeded: %v (output: %s)", err2, string(out2))
	}
}

func TestSupervisor_StartupErrorBeforeLaunchReleasesLock(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "pgdata")
	invalidBinDir := filepath.Join(tmpDir, "nonexistent-bin")

	s, err := New(Options{
		BinDir:  invalidBinDir,
		DataDir: dataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create supervisor: %v", err)
	}

	err = s.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start to fail due to invalid binDir, got nil")
	}

	// Lock MUST be released
	if s.DataDirLocked() {
		t.Fatal("expected DataDirLock to be released when Start fails before launch")
	}

	// Calling LockDataDir directly should succeed immediately
	unlock, lockErr := LockDataDir(dataDir)
	if lockErr != nil {
		t.Fatalf("expected LockDataDir to succeed after failed Start released lock: %v", lockErr)
	}
	_ = unlock()
}

func TestSupervisor_PendingLaunchRetainsLock_ExcludesSecondProcess(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	dataDir := filepath.Join(tmpDir, "pgdata")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}

	writeExecutable := func(name, script string) {
		p := filepath.Join(binDir, name)
		if err := os.WriteFile(p, []byte(script), 0755); err != nil {
			t.Fatalf("failed to write %s: %v", name, err)
		}
	}

	writeExecutable("initdb", "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo \"initdb (PostgreSQL) 18.6\"; exit 0; fi\nexit 0\n")
	writeExecutable("postgres", "#!/bin/sh\nif [ \"$1\" = \"--version\" ] || [ \"$1\" = \"-V\" ]; then echo \"postgres (PostgreSQL) 18.6\"; exit 0; fi\nexit 0\n")

	// pg_ctl script where child holds until .release is written
	pgCtlScript := `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "pg_ctl (PostgreSQL) 18.6"
  exit 0
fi

dataDir=""
mode=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -D) dataDir="$2"; shift 2 ;;
    start) mode="start"; shift ;;
    stop) mode="stop"; shift ;;
    *) shift ;;
  esac
done

if [ "$mode" = "start" ]; then
  if [ -f "$dataDir/.hang_pgctl" ]; then
    sh -c '
      echo "$$" > "'"$dataDir"'/postmaster.pid.tmp"
      while [ ! -f "'"$dataDir"'/.release" ]; do
        sleep 0.01
      done
      mv "'"$dataDir"'/postmaster.pid.tmp" "'"$dataDir"'/postmaster.pid"
      while true; do sleep 1; done
    ' >/dev/null 2>&1 </dev/null &
    child_pid=$!
    echo "$child_pid" > "$dataDir/.forked"
    while true; do sleep 1; done
  else
    sh -c '
      echo "$$" > "'"$dataDir"'/postmaster.pid"
      while true; do sleep 1; done
    ' >/dev/null 2>&1 </dev/null &
    child_pid=$!
    echo "$child_pid" > "$dataDir/.forked"
    exit 0
  fi
fi

if [ "$mode" = "stop" ]; then
  if [ ! -f "$dataDir/postmaster.pid" ]; then
    echo "pg_ctl: PID absent" >&2
    exit 1
  fi
  pid=$(head -n 1 "$dataDir/postmaster.pid")
  if [ -n "$pid" ]; then
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$dataDir/postmaster.pid"
  exit 0
fi
`
	writeExecutable("pg_ctl", pgCtlScript)

	startCtx, cancelStart := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelStart()

	s, err := New(Options{
		BinDir:            binDir,
		DataDir:           dataDir,
		NoSync:            true,
		isRoot:            func() bool { return false },
		resolvePIDTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create supervisor: %v", err)
	}

	forkedFile := filepath.Join(dataDir, ".forked")
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	releaseFile := filepath.Join(dataDir, ".release")
	hangFile := filepath.Join(dataDir, ".hang_pgctl")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatalf("failed to create dataDir: %v", err)
	}
	if err := os.WriteFile(hangFile, []byte("1\n"), 0644); err != nil {
		t.Fatalf("failed to write hang file: %v", err)
	}

	var recordedChildPID int
	t.Cleanup(func() {
		_ = os.WriteFile(releaseFile, []byte("release\n"), 0644)
		_ = s.Stop(context.Background())
		if recordedChildPID > 0 {
			_ = syscall.Kill(recordedChildPID, syscall.SIGKILL)
		}
	})

	go func() {
		for i := 0; i < 400; i++ {
			if data, err := os.ReadFile(forkedFile); err == nil && len(data) > 0 {
				pidStr := strings.TrimSpace(string(data))
				if p, parseErr := strconv.Atoi(pidStr); parseErr == nil && p > 0 {
					recordedChildPID = p
					cancelStart()
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	startErr := s.Start(startCtx)
	if startErr == nil {
		t.Fatal("expected Start to fail due to context cancellation, got nil")
	}

	// Verify pending launch and data lock are RETAINED
	if !s.PendingLaunch() {
		t.Fatal("expected PendingLaunch() to be true")
	}
	if !s.DataDirLocked() {
		t.Fatal("expected DataDirLocked() to be true while pending launch")
	}

	// Subprocess attempting to supervise must be excluded
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+dataDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected second process to be excluded during pending launch, output: %s", string(out))
	}
	if !strings.Contains(string(out), "embedded postgresql data directory is locked by another process") {
		t.Fatalf("expected clear lock error in subprocess output, got:\n%s", string(out))
	}

	// Release child and wait for PID file
	if err := os.WriteFile(releaseFile, []byte("release\n"), 0644); err != nil {
		t.Fatalf("failed to write release file: %v", err)
	}

	for i := 0; i < 200; i++ {
		if _, statErr := os.Stat(pidFile); statErr == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Retry Stop now that PID is published
	retryErr := s.Stop(context.Background())
	if retryErr != nil {
		t.Fatalf("expected retry Stop to succeed after PID publication, got: %v", retryErr)
	}

	if s.DataDirLocked() {
		t.Fatal("expected DataDirLocked() to be false after successful Stop")
	}

	_ = os.Remove(hangFile)

	// Now second process must succeed
	cmd2 := exec.Command(os.Args[0])
	cmd2.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+dataDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("expected second process to succeed after retry Stop succeeded: %v (output: %s)", err2, string(out2))
	}
}
