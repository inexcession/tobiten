package embedded

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSupervisor_RelativePathsResolvedToAbsolute(t *testing.T) {
	relData := "data/test-pg"
	relSocket := "data/test-socket"
	relLog := "data/test.log"
	relBin := "bin/pg"

	s, err := New(Options{
		DataDir:   relData,
		SocketDir: relSocket,
		LogFile:   relLog,
		BinDir:    relBin,
	})
	if err != nil {
		t.Fatalf("unexpected error creating supervisor: %v", err)
	}

	if !filepath.IsAbs(s.DataDir()) {
		t.Errorf("expected absolute DataDir, got %q", s.DataDir())
	}
	if !filepath.IsAbs(s.SocketDir()) {
		t.Errorf("expected absolute SocketDir, got %q", s.SocketDir())
	}
	if !filepath.IsAbs(s.BinDir()) {
		t.Errorf("expected absolute BinDir, got %q", s.BinDir())
	}
}

func TestSupervisor_Validation(t *testing.T) {
	// Missing DataDir
	_, err := New(Options{})
	if err == nil {
		t.Fatal("expected error when DataDir is missing, got nil")
	}

	// Default port
	s, err := New(Options{DataDir: "/tmp/data"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Port() != 5432 {
		t.Fatalf("expected default port 5432, got %d", s.Port())
	}

	// Custom port
	s2, err := New(Options{DataDir: "/tmp/data", Port: 5543})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s2.Port() != 5543 {
		t.Fatalf("expected custom port 5543, got %d", s2.Port())
	}
}

func TestSupervisor_RefuseRoot(t *testing.T) {
	tmpDir := t.TempDir()

	s, err := New(Options{
		DataDir: tmpDir,
		isRoot: func() bool {
			return true // Simulate running as root
		},
	})
	if err != nil {
		t.Fatalf("unexpected error creating supervisor: %v", err)
	}

	err = s.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start to fail when running as root, got nil")
	}
	if !errors.Is(err, ErrRootForbidden) {
		t.Fatalf("expected ErrRootForbidden, got %v", err)
	}
}

func TestDefaultIsRoot(t *testing.T) {
	expected := os.Geteuid() == 0
	if defaultIsRoot() != expected {
		t.Fatalf("expected defaultIsRoot() == %v, got %v", expected, defaultIsRoot())
	}
}

func TestSupervisor_ValidateBinariesBeforeDataMutation(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "pgdata")
	invalidBinDir := filepath.Join(tmpDir, "invalid-bins")
	if err := os.MkdirAll(invalidBinDir, 0755); err != nil {
		t.Fatalf("failed to create invalidBinDir: %v", err)
	}

	// Create an incomplete binary set (only pg_ctl, missing initdb and postgres)
	createDummyBinary(t, invalidBinDir, "pg_ctl", "18.6", true)

	s, err := New(Options{
		BinDir:  invalidBinDir,
		DataDir: dataDir,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = s.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start to fail on incomplete binary set, got nil")
	}

	// Verify NO data mutation occurred: dataDir must not have been created
	if _, statErr := os.Stat(dataDir); !os.IsNotExist(statErr) {
		t.Fatalf("expected dataDir to not exist after validation failure, but stat succeeded: %v", statErr)
	}
}

func TestSupervisor_SocketPathTooLong(t *testing.T) {
	tmpDir := t.TempDir()
	longSocketDir := tmpDir + "/" + strings.Repeat("toolongpathsegment", 10)

	s, err := New(Options{
		BinDir:    tmpDir,
		DataDir:   tmpDir,
		SocketDir: longSocketDir,
		isRoot:    func() bool { return false },
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = s.Start(context.Background())
	if err == nil {
		t.Fatal("expected error for socket path exceeding 104 bytes, got nil")
	}
	if !errors.Is(err, ErrSocketPathTooLong) {
		t.Fatalf("expected ErrSocketPathTooLong, got: %v", err)
	}
}

func TestSupervisor_ConnStringEscaping(t *testing.T) {
	tmpDir := t.TempDir()
	socketDirWithSpaces := filepath.Join(tmpDir, "socket with spaces")

	s, err := New(Options{
		DataDir:   tmpDir,
		SocketDir: socketDirWithSpaces,
		Port:      5432,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	connStr := s.ConnString()
	expectedHostPart := "host='" + socketDirWithSpaces + "'"
	if !strings.Contains(connStr, expectedHostPart) {
		t.Errorf("expected ConnString to contain %s, got: %s", expectedHostPart, connStr)
	}

	dbURL := s.DatabaseURL()
	if !strings.Contains(dbURL, "host=") {
		t.Errorf("expected DatabaseURL to contain host parameter, got: %s", dbURL)
	}
}

func TestSupervisor_StopNotRunning(t *testing.T) {
	s, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if s.Running() {
		t.Fatal("expected Running() to be false initially")
	}
	if s.PID() != 0 {
		t.Fatalf("expected PID() to be 0 initially, got %d", s.PID())
	}

	// Idempotent stop when not running
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("unexpected error on Stop when not running: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("unexpected error on Close: %v", err)
	}
}

func TestSupervisor_StartupPreCancelled(t *testing.T) {
	binDir, err := FindBinDir("")
	if err != nil {
		t.Skipf("skipping test: no PostgreSQL %d binaries found on host: %v", PinnedMajor, err)
	}

	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "pgdata")

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Start with already-cancelled context (fails before launch)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = s.Start(ctx)
	if err == nil {
		t.Fatal("expected Start to fail with cancelled context, got nil")
	}

	if s.Running() {
		t.Fatal("expected Running() to be false after cancelled start")
	}
	if s.PID() != 0 {
		t.Fatalf("expected PID() to be 0 after cancelled start, got %d", s.PID())
	}
}

func TestSupervisor_PostLaunchCancellation(t *testing.T) {
	binDir, err := FindBinDir("")
	if err != nil {
		t.Skipf("skipping test: no PostgreSQL %d binaries found on host: %v", PinnedMajor, err)
	}

	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "pgdata")

	var launchedPID int
	t.Cleanup(func() {
		if launchedPID > 0 {
			_ = syscall.Kill(launchedPID, syscall.SIGKILL)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
		postLaunchHook: func(sup *Supervisor, pid int) error {
			launchedPID = pid
			// Cancel context post-launch so Start must clean up the running postmaster
			cancel()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = s.Start(ctx)
	if err == nil {
		t.Fatal("expected Start to fail due to post-launch cancellation, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled in error, got: %v", err)
	}

	if launchedPID <= 0 {
		t.Fatal("expected postLaunchHook to capture a valid launched PID")
	}

	// Verify the launched postmaster was cleaned up and is confirmed dead
	if killErr := syscall.Kill(launchedPID, 0); !errors.Is(killErr, syscall.ESRCH) {
		t.Fatalf("expected launched postmaster process %d to be dead (ESRCH), got: %v", launchedPID, killErr)
	}

	if s.Running() {
		t.Fatal("expected Running() to be false after cancelled start")
	}
	if s.PID() != 0 {
		t.Fatalf("expected PID() to be 0 after cancelled start, got %d", s.PID())
	}
}

func TestSupervisor_CleanupFailure(t *testing.T) {
	binDir, err := FindBinDir("")
	if err != nil {
		t.Skipf("skipping test: no PostgreSQL %d binaries found on host: %v", PinnedMajor, err)
	}

	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "pgdata")

	var launchedPID int
	t.Cleanup(func() {
		if launchedPID > 0 {
			_ = syscall.Kill(launchedPID, syscall.SIGKILL)
		}
	})

	simulatedErr := errors.New("simulated exit failure: process hung")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
		postLaunchHook: func(sup *Supervisor, pid int) error {
			launchedPID = pid
			return errors.New("post-launch trigger failure")
		},
		confirmExit: func(pid int, timeout time.Duration) error {
			return simulatedErr
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = s.Start(ctx)
	if err == nil {
		t.Fatal("expected Start to fail, got nil")
	}

	// Verify cleanup failure error is returned and propagated
	if !strings.Contains(err.Error(), "simulated exit failure: process hung") {
		t.Fatalf("expected error to contain cleanup failure, got: %v", err)
	}

	// Verify owned PID is RETAINED
	if s.PID() != launchedPID {
		t.Fatalf("expected owned PID %d to be retained, got %d", launchedPID, s.PID())
	}

	// Verify retryable state is RETAINED
	if !s.Running() {
		t.Fatal("expected Running() to be true (retryable state) on cleanup failure")
	}

	// Verify socket directory is NOT cleared
	if s.SocketDir() == "" {
		t.Fatal("expected SocketDir to be retained on cleanup failure")
	}

	// Retry stop with restored exit confirmation to verify retryable state and avoid process leak
	s.confirmExit = confirmProcessExit
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()

	if stopErr := s.Stop(stopCtx); stopErr != nil {
		t.Fatalf("expected retry Stop() to succeed, got %v", stopErr)
	}

	if s.Running() {
		t.Fatal("expected Running() to be false after successful retry Stop")
	}
	if s.PID() != 0 {
		t.Fatalf("expected PID() to be 0 after successful retry Stop, got %d", s.PID())
	}
	if killErr := syscall.Kill(launchedPID, 0); !errors.Is(killErr, syscall.ESRCH) {
		t.Fatalf("expected postmaster process %d to be dead (ESRCH), got: %v", launchedPID, killErr)
	}
}

func TestSupervisor_CancelAfterForkBeforePIDCreation(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("unexpected error creating supervisor: %v", err)
	}

	forkedFile := filepath.Join(dataDir, ".forked")
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	releaseFile := filepath.Join(dataDir, ".release")

	var recordedChildPID int
	t.Cleanup(func() {
		if recordedChildPID > 0 {
			_ = syscall.Kill(recordedChildPID, syscall.SIGKILL)
		}
	})

	cancelTriggered := make(chan struct{})
	go func() {
		for i := 0; i < 400; i++ {
			if data, err := os.ReadFile(forkedFile); err == nil && len(data) > 0 {
				pidStr := strings.TrimSpace(string(data))
				if p, parseErr := strconv.Atoi(pidStr); parseErr == nil && p > 0 {
					recordedChildPID = p
					// Verify postmaster.pid does NOT exist yet when we cancel
					if _, statErr := os.Stat(pidFile); os.IsNotExist(statErr) {
						cancel()
						close(cancelTriggered)
						// Deterministically release child to publish postmaster.pid during Start resolution
						_ = os.WriteFile(releaseFile, []byte("release\n"), 0644)
						return
					}
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Errorf("timeout waiting for .forked file before postmaster.pid")
	}()

	startErr := s.Start(ctx)
	if startErr == nil {
		t.Fatal("expected Start to fail due to context cancellation, got nil")
	}

	select {
	case <-cancelTriggered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cancelTriggered")
	}

	if recordedChildPID <= 0 {
		t.Fatal("expected child PID to be recorded")
	}

	// Verify that the child process was resolved and cleaned up (not discarded as an orphan)
	if killErr := syscall.Kill(recordedChildPID, 0); !errors.Is(killErr, syscall.ESRCH) {
		t.Fatalf("expected child process %d to be dead (ESRCH), got: %v", recordedChildPID, killErr)
	}

	if s.Running() {
		t.Fatal("expected supervisor to not be Running() after cleanup")
	}
	if s.PID() != 0 {
		t.Fatalf("expected PID() to be 0 after cleanup, got %d", s.PID())
	}
}

func TestSupervisor_StopBeforePIDPublicationFailsAndRetainsState(t *testing.T) {
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

	// Fake pg_ctl failing when postmaster.pid is absent during stop
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
fi

if [ "$mode" = "stop" ]; then
  if [ ! -f "$dataDir/postmaster.pid" ]; then
    echo "pg_ctl: no server running (PID absent)" >&2
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

	// Short resolution timeout so Start fails resolution before .release is published
	s, err := New(Options{
		BinDir:            binDir,
		DataDir:           dataDir,
		NoSync:            true,
		isRoot:            func() bool { return false },
		resolvePIDTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error creating supervisor: %v", err)
	}

	forkedFile := filepath.Join(dataDir, ".forked")
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	releaseFile := filepath.Join(dataDir, ".release")

	var recordedChildPID int
	t.Cleanup(func() {
		if recordedChildPID > 0 {
			_ = syscall.Kill(recordedChildPID, syscall.SIGKILL)
		}
	})

	cancelTriggered := make(chan struct{})
	go func() {
		for i := 0; i < 400; i++ {
			if data, err := os.ReadFile(forkedFile); err == nil && len(data) > 0 {
				pidStr := strings.TrimSpace(string(data))
				if p, parseErr := strconv.Atoi(pidStr); parseErr == nil && p > 0 {
					recordedChildPID = p
					if _, statErr := os.Stat(pidFile); os.IsNotExist(statErr) {
						cancelStart()
						close(cancelTriggered)
						return
					}
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Errorf("timeout waiting for .forked file before postmaster.pid")
	}()

	startErr := s.Start(startCtx)
	if startErr == nil {
		t.Fatal("expected Start to fail due to context cancellation, got nil")
	}

	select {
	case <-cancelTriggered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cancelTriggered")
	}

	// Verify pending launch is retained
	if !s.PendingLaunch() {
		t.Fatal("expected PendingLaunch() to be true")
	}
	if !s.Running() {
		t.Fatal("expected Running() to be true (retryable state)")
	}

	// 1. Call Stop BEFORE PID publication, while PID is absent and fake pg_ctl stop fails
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStop()

	stopErr := s.Stop(stopCtx)
	if stopErr == nil {
		t.Fatal("expected Stop to fail when PID is absent and cleanup fails, got nil")
	}

	// Verify pending state, running state, and socket are RETAINED
	if !s.PendingLaunch() {
		t.Fatal("expected PendingLaunch() to be retained after failed Stop")
	}
	if !s.Running() {
		t.Fatal("expected Running() to be retained after failed Stop")
	}
	if s.SocketDir() == "" {
		t.Fatal("expected SocketDir to be retained after failed Stop")
	}
	if killErr := syscall.Kill(recordedChildPID, 0); killErr != nil {
		t.Fatalf("expected child process %d to still be alive before PID publication, got: %v", recordedChildPID, killErr)
	}

	// 2. Publish PID
	if err := os.WriteFile(releaseFile, []byte("release\n"), 0644); err != nil {
		t.Fatalf("failed to write release file: %v", err)
	}

	// Wait for child to publish postmaster.pid
	for i := 0; i < 200; i++ {
		if _, statErr := os.Stat(pidFile); statErr == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 3. Retry Stop now that PID is published
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRetry()

	retryErr := s.Stop(retryCtx)
	if retryErr != nil {
		t.Fatalf("expected retry Stop() to succeed after PID publication, got: %v", retryErr)
	}

	if s.PendingLaunch() {
		t.Fatal("expected PendingLaunch() to be false after successful Stop")
	}
	if s.Running() {
		t.Fatal("expected Running() to be false after successful Stop")
	}
	if s.PID() != 0 {
		t.Fatalf("expected PID() to be 0 after successful Stop, got %d", s.PID())
	}

	// 4. Verify child exit confirmed
	if killErr := syscall.Kill(recordedChildPID, 0); !errors.Is(killErr, syscall.ESRCH) {
		t.Fatalf("expected child process %d to be dead (ESRCH), got: %v", recordedChildPID, killErr)
	}
}
