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
	socketDir := os.Getenv("TOBITEN_TEST_SUBPROCESS_SOCKETDIR")
	portStr := os.Getenv("TOBITEN_TEST_SUBPROCESS_PORT")
	port := 5432
	if portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 {
			port = p
		}
	}
	s, err := New(Options{
		DataDir:   dataDir,
		BinDir:    binDir,
		SocketDir: socketDir,
		Port:      port,
		NoSync:    true,
		isRoot:    func() bool { return false },
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
}

func TestLock_DataDirLockPath_SymlinkAliases(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Existing directory symlink
	realDir := filepath.Join(tmpDir, "real_cluster")
	if err := os.MkdirAll(realDir, 0700); err != nil {
		t.Fatalf("failed to create realDir: %v", err)
	}
	aliasDir := filepath.Join(tmpDir, "alias_cluster")
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	lockReal := DataDirLockPath(realDir)
	lockAlias := DataDirLockPath(aliasDir)
	if lockReal != lockAlias {
		t.Fatalf("expected identical lock path for alias: real=%q, alias=%q", lockReal, lockAlias)
	}

	// 2. Symlinked ancestor of not-yet-created cluster
	ancestorReal := filepath.Join(tmpDir, "ancestor_real")
	if err := os.MkdirAll(ancestorReal, 0700); err != nil {
		t.Fatalf("failed to create ancestorReal: %v", err)
	}
	ancestorLink := filepath.Join(tmpDir, "ancestor_link")
	if err := os.Symlink(ancestorReal, ancestorLink); err != nil {
		t.Fatalf("failed to create ancestor symlink: %v", err)
	}

	newClusterViaReal := filepath.Join(ancestorReal, "nested", "pgdata")
	newClusterViaLink := filepath.Join(ancestorLink, "nested", "pgdata")

	lockViaReal := DataDirLockPath(newClusterViaReal)
	lockViaLink := DataDirLockPath(newClusterViaLink)
	if lockViaReal != lockViaLink {
		t.Fatalf("expected identical lock path for ancestor symlink: real=%q, link=%q", lockViaReal, lockViaLink)
	}

	// 3. Broken/not-yet-created leaf symlink
	nonExistentTarget := filepath.Join(tmpDir, "future_real_cluster")
	brokenLeaf := filepath.Join(tmpDir, "broken_leaf_symlink")
	if err := os.Symlink(nonExistentTarget, brokenLeaf); err != nil {
		t.Fatalf("failed to create leaf symlink: %v", err)
	}
	lockTarget := DataDirLockPath(nonExistentTarget)
	lockLeaf := DataDirLockPath(brokenLeaf)
	if lockTarget != lockLeaf {
		t.Fatalf("expected identical lock path for not-yet-created leaf target: target=%q, leaf=%q", lockTarget, lockLeaf)
	}

	// 4. Symlink with intermediate symlink followed by '..' and not-yet-created target:
	// alias -> parent/link/../future where parent/link -> other/subdir
	parent := filepath.Join(tmpDir, "parent")
	other := filepath.Join(tmpDir, "other")
	subdir := filepath.Join(other, "subdir")
	if err := os.MkdirAll(parent, 0755); err != nil {
		t.Fatalf("failed to create parent: %v", err)
	}
	if err := os.MkdirAll(subdir, 0755); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(subdir, link); err != nil {
		t.Fatalf("failed to create link symlink: %v", err)
	}
	alias := filepath.Join(tmpDir, "alias_symlink_dotdot")
	symlinkTarget := filepath.Join(parent, "link") + "/../future"
	if err := os.Symlink(symlinkTarget, alias); err != nil {
		t.Fatalf("failed to create alias symlink: %v", err)
	}

	// Before target exists: must resolve to other/future, NOT parent/future
	canonBefore, err := canonicalizePath(alias)
	if err != nil {
		t.Fatalf("canonicalizePath failed before creation: %v", err)
	}
	expectedTarget := filepath.Join(other, "future")
	expectedCanon, err := canonicalizePath(expectedTarget)
	if err != nil {
		t.Fatalf("canonicalizePath(expectedTarget) failed: %v", err)
	}
	if canonBefore != expectedCanon {
		t.Fatalf("expected canonical path before creation to be %q, got: %q", expectedCanon, canonBefore)
	}
	if lockAlias, lockTargetSym := DataDirLockPath(alias), DataDirLockPath(expectedTarget); lockAlias != lockTargetSym {
		t.Fatalf("expected identical lock path before creation: alias=%q, target=%q", lockAlias, lockTargetSym)
	}

	// Now create target directory and prove identity remains stable and matches EvalSymlinks
	if err := os.MkdirAll(expectedTarget, 0700); err != nil {
		t.Fatalf("failed to create target: %v", err)
	}
	canonAfter, err := canonicalizePath(alias)
	if err != nil {
		t.Fatalf("canonicalizePath failed after creation: %v", err)
	}
	if canonAfter != canonBefore {
		t.Fatalf("identity changed after target creation: before=%q, after=%q", canonBefore, canonAfter)
	}
	evalSymlinks, err := filepath.EvalSymlinks(alias)
	if err != nil {
		t.Fatalf("filepath.EvalSymlinks failed: %v", err)
	}
	if canonAfter != filepath.Clean(evalSymlinks) {
		t.Fatalf("expected canonAfter %q to match filepath.EvalSymlinks %q", canonAfter, evalSymlinks)
	}
}

func TestLock_SocketDirLockPath(t *testing.T) {
	got := SocketDirLockPath("/tmp/tbt-1234")
	if !strings.HasSuffix(got, ".lock") {
		t.Fatalf("SocketDirLockPath = %q, expected suffix .lock", got)
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
	realDir := filepath.Join(tmpDir, "real_cluster")
	if err := os.MkdirAll(realDir, 0700); err != nil {
		t.Fatalf("failed to create realDir: %v", err)
	}
	aliasDir := filepath.Join(tmpDir, "alias_cluster")
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	// Lock via alias
	unlock, err := LockDataDir(aliasDir)
	if err != nil {
		t.Fatalf("expected LockDataDir(alias) to succeed: %v", err)
	}

	// Second call via realDir MUST fail with ErrDataDirLocked
	_, err = LockDataDir(realDir)
	if err == nil {
		t.Fatal("expected second LockDataDir(real) to fail, got nil")
	}
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("expected ErrDataDirLocked, got: %v", err)
	}

	// Second call via aliasDir MUST also fail
	_, err = LockDataDir(aliasDir)
	if err == nil {
		t.Fatal("expected LockDataDir(alias) to fail while held, got nil")
	}
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("expected ErrDataDirLocked, got: %v", err)
	}

	// Unlock
	if err := unlock(); err != nil {
		t.Fatalf("unlock failed: %v", err)
	}

	// Now call via realDir succeeds
	unlock2, err := LockDataDir(realDir)
	if err != nil {
		t.Fatalf("expected LockDataDir(real) to succeed after unlock: %v", err)
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

func TestSupervisor_CrossProcessAliasExclusion_BeforePIDPublication_NoConfigMutation(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	realDir := filepath.Join(tmpDir, "real_pgdata")
	if err := os.MkdirAll(realDir, 0700); err != nil {
		t.Fatalf("failed to create realDir: %v", err)
	}
	aliasDir := filepath.Join(tmpDir, "alias_pgdata")
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	writeFakeBinaries(t, binDir)

	canonicalDir, err := canonicalizePath(realDir)
	if err != nil {
		t.Fatalf("canonicalizePath failed: %v", err)
	}
	hangFile := filepath.Join(canonicalDir, ".hang_pgctl")
	releaseFile := filepath.Join(canonicalDir, ".release")
	if err := os.WriteFile(hangFile, []byte("1\n"), 0644); err != nil {
		t.Fatalf("failed to write hang file: %v", err)
	}

	s1, err := New(Options{
		BinDir:            binDir,
		DataDir:           aliasDir,
		NoSync:            true,
		isRoot:            func() bool { return false },
		resolvePIDTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create s1: %v", err)
	}

	t.Cleanup(func() {
		_ = os.WriteFile(releaseFile, []byte("release\n"), 0644)
		_ = s1.Stop(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// s1.Start begins launch and acquires data lock, but hangs before PID publication
	startErr := s1.Start(ctx)
	if startErr == nil {
		t.Fatal("expected s1.Start to fail with resolution timeout before PID publication, got nil")
	}

	// Verify that postmaster.pid DOES NOT EXIST yet!
	pidFile := filepath.Join(canonicalDir, "postmaster.pid")
	if _, statErr := os.Stat(pidFile); !os.IsNotExist(statErr) {
		t.Fatalf("expected postmaster.pid to NOT exist before PID publication, stat: %v", statErr)
	}

	// Verify that s1 holds the data directory lock
	if !s1.DataDirLocked() {
		t.Fatal("expected s1 to hold data-directory lock before PID publication")
	}

	// Record postgresql.conf content created by s1
	confPath := filepath.Join(s1.DataDir(), "postgresql.conf")
	initialConf, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}

	// 1. Second subprocess using realDir (the alias target) MUST be excluded while PID is NOT published
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+realDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected second process to be excluded via alias, output: %s", string(out))
	}
	if !strings.Contains(string(out), "embedded postgresql data directory is locked by another process") {
		t.Fatalf("expected clear lock error in subprocess output, got:\n%s", string(out))
	}

	// Verify postmaster.pid STILL does not exist
	if _, statErr := os.Stat(pidFile); !os.IsNotExist(statErr) {
		t.Fatalf("expected postmaster.pid to STILL not exist after rejected attempt, stat: %v", statErr)
	}

	// 2. Assert that postgresql.conf was NOT mutated by the rejected subprocess
	afterConf, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("failed to re-read postgresql.conf: %v", err)
	}
	if string(initialConf) != string(afterConf) {
		t.Fatal("postgresql.conf was mutated by rejected subprocess attempt!")
	}

	// 3. Assert public LockDataDir API also excludes via alias
	_, err = LockDataDir(realDir)
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("expected LockDataDir(realDir) to fail with ErrDataDirLocked, got: %v", err)
	}
	_, err = LockDataDir(aliasDir)
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("expected LockDataDir(aliasDir) to fail with ErrDataDirLocked, got: %v", err)
	}

	// 4. Release s1 hang and stop s1
	if err := os.WriteFile(releaseFile, []byte("release\n"), 0644); err != nil {
		t.Fatalf("failed to write release file: %v", err)
	}
	if err := s1.Stop(context.Background()); err != nil {
		t.Fatalf("s1.Stop failed: %v", err)
	}
	_ = os.Remove(hangFile)
	_ = os.Remove(releaseFile)

	// 5. Now second process on realDir succeeds
	cmd2 := exec.Command(os.Args[0])
	cmd2.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+realDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("expected second process to succeed after s1 stopped: %v (output: %s)", err2, string(out2))
	}
}

func TestSupervisor_MissingTarget_SymlinkDotDot_StableIdentityAndExclusionWithoutPID(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	writeFakeBinaries(t, binDir)

	parent := filepath.Join(tmpDir, "parent")
	other := filepath.Join(tmpDir, "other")
	subdir := filepath.Join(other, "subdir")
	if err := os.MkdirAll(parent, 0755); err != nil {
		t.Fatalf("failed to create parent: %v", err)
	}
	if err := os.MkdirAll(subdir, 0755); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(subdir, link); err != nil {
		t.Fatalf("failed to create link symlink: %v", err)
	}

	// alias -> parent/link/../future where other/future does NOT exist yet!
	alias := filepath.Join(tmpDir, "alias")
	target := filepath.Join(parent, "link") + "/../future"
	if err := os.Symlink(target, alias); err != nil {
		t.Fatalf("failed to create alias symlink: %v", err)
	}

	expectedTarget := filepath.Join(other, "future")
	expectedCanon, err := canonicalizePath(expectedTarget)
	if err != nil {
		t.Fatalf("canonicalizePath(expectedTarget) failed: %v", err)
	}

	// 1. Prove intended canonical path while target is absent
	canonBefore, err := canonicalizePath(alias)
	if err != nil {
		t.Fatalf("canonicalizePath(alias) failed: %v", err)
	}
	if canonBefore != expectedCanon {
		t.Fatalf("expected canonical path before target creation %q, got: %q", expectedCanon, canonBefore)
	}
	lockAlias := DataDirLockPath(alias)
	lockTarget := DataDirLockPath(expectedTarget)
	if lockAlias != lockTarget {
		t.Fatalf("expected identical lock path for alias and intended target: alias=%q, target=%q", lockAlias, lockTarget)
	}

	// Prove target directory and postmaster.pid do not exist
	if _, err := os.Stat(canonBefore); !os.IsNotExist(err) {
		t.Fatalf("expected target directory %q to not exist yet", canonBefore)
	}
	pidPath := filepath.Join(canonBefore, "postmaster.pid")
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("expected postmaster.pid %q to not exist", pidPath)
	}

	// 2. Process 1 acquires maintenance lock via alias while target is absent (no PID file)
	unlock, err := LockDataDir(alias)
	if err != nil {
		t.Fatalf("LockDataDir(alias) failed: %v", err)
	}
	defer func() { _ = unlock() }()

	// 3. Second process attempts to supervise intended target and must be excluded without a PID file
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+expectedTarget,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected second process on missing-target to be excluded, output: %s", string(out))
	}
	if !strings.Contains(string(out), "embedded postgresql data directory is locked by another process") {
		t.Fatalf("expected clear lock error in subprocess output, got:\n%s", string(out))
	}

	// Assert no PID file was published and no config was created
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatal("postmaster.pid was created by rejected process!")
	}
	confPath := filepath.Join(canonBefore, "postgresql.conf")
	if _, err := os.Stat(confPath); !os.IsNotExist(err) {
		t.Fatal("postgresql.conf was created by rejected attempt!")
	}

	// 4. Release maintenance lock, create target directory, and prove stable identity before/after creation
	if err := unlock(); err != nil {
		t.Fatalf("failed to unlock: %v", err)
	}
	if err := os.MkdirAll(expectedTarget, 0700); err != nil {
		t.Fatalf("failed to create target directory: %v", err)
	}

	canonAfter, err := canonicalizePath(alias)
	if err != nil {
		t.Fatalf("canonicalizePath(alias) after creation failed: %v", err)
	}
	if canonAfter != canonBefore {
		t.Fatalf("identity changed after target creation: before=%q, after=%q", canonBefore, canonAfter)
	}
	evalAfter, err := filepath.EvalSymlinks(alias)
	if err != nil {
		t.Fatalf("filepath.EvalSymlinks failed: %v", err)
	}
	if canonAfter != filepath.Clean(evalAfter) {
		t.Fatalf("expected canonAfter %q to match filepath.EvalSymlinks %q", canonAfter, evalAfter)
	}

	// 5. Second process can now supervise intended target successfully
	cmd2 := exec.Command(os.Args[0])
	cmd2.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+expectedTarget,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
	)
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("expected second process to succeed after target creation and unlock, got: %v (output: %s)", err2, string(out2))
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
	canonicalLongData, err := canonicalizePath(longDataDir)
	if err != nil {
		t.Fatalf("canonicalizePath failed: %v", err)
	}
	h := sha256.Sum256([]byte(canonicalLongData))
	deterministicSockDir, err := canonicalizePath(filepath.Join(os.TempDir(), fmt.Sprintf("tbt-%x", h[:4])))
	if err != nil {
		t.Fatalf("canonicalizePath temp sock failed: %v", err)
	}

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

func TestSupervisor_SocketDir_InspectionErrorFailsSafelyAndPreserves(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	writeFakeBinaries(t, binDir)

	longDataDir := filepath.Join(tmpDir, strings.Repeat("unreadable_sock_", 6), "pgdata")
	if err := os.MkdirAll(longDataDir, 0700); err != nil {
		t.Fatalf("failed to create longDataDir: %v", err)
	}

	canonicalLongData, err := canonicalizePath(longDataDir)
	if err != nil {
		t.Fatalf("canonicalizePath failed: %v", err)
	}
	h := sha256.Sum256([]byte(canonicalLongData))
	deterministicSockDir, err := canonicalizePath(filepath.Join(os.TempDir(), fmt.Sprintf("tbt-%x", h[:4])))
	if err != nil {
		t.Fatalf("canonicalizePath temp sock failed: %v", err)
	}

	// Pre-create the directory with mode 0300 (write/exec only, no read permission -> ReadDir returns EACCES)
	if err := os.MkdirAll(deterministicSockDir, 0700); err != nil {
		t.Fatalf("failed to create deterministicSockDir: %v", err)
	}
	if err := os.Chmod(deterministicSockDir, 0300); err != nil {
		t.Fatalf("failed to chmod 0300: %v", err)
	}
	defer func() {
		_ = os.Chmod(deterministicSockDir, 0700)
		_ = os.RemoveAll(deterministicSockDir)
		_ = os.Remove(SocketDirLockPath(deterministicSockDir))
	}()

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

	// Start MUST fail safely on inspection error
	startErr := s.Start(ctx)
	if startErr == nil {
		_ = s.Stop(context.Background())
		t.Fatal("expected Start to fail safely on unreadable socket directory, got nil")
	}
	if !strings.Contains(startErr.Error(), "failed to inspect existing socket directory") {
		t.Fatalf("expected inspection error, got: %v", startErr)
	}

	// Pre-existing directory MUST NOT have been removed by startup failure cleanup!
	if _, statErr := os.Stat(deterministicSockDir); os.IsNotExist(statErr) {
		t.Fatalf("pre-existing unreadable socket directory %s was removed by cleanup!", deterministicSockDir)
	}
}

func TestSupervisor_SocketDir_PreExistingEmptyDirectoryPreservedOnStop(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	writeFakeBinaries(t, binDir)

	longDataDir := filepath.Join(tmpDir, strings.Repeat("empty_pre_exist_", 6), "pgdata")
	if err := os.MkdirAll(longDataDir, 0700); err != nil {
		t.Fatalf("failed to create longDataDir: %v", err)
	}

	canonicalLongData, err := canonicalizePath(longDataDir)
	if err != nil {
		t.Fatalf("canonicalizePath failed: %v", err)
	}
	h := sha256.Sum256([]byte(canonicalLongData))
	deterministicSockDir, err := canonicalizePath(filepath.Join(os.TempDir(), fmt.Sprintf("tbt-%x", h[:4])))
	if err != nil {
		t.Fatalf("canonicalizePath temp sock failed: %v", err)
	}

	// Pre-create empty directory
	if err := os.MkdirAll(deterministicSockDir, 0700); err != nil {
		t.Fatalf("failed to create deterministicSockDir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(deterministicSockDir)
		_ = os.Remove(SocketDirLockPath(deterministicSockDir))
	}()

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
		t.Fatalf("s.Start failed: %v", err)
	}

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("s.Stop failed: %v", err)
	}

	// Empty pre-existing directory was NOT created by this startup; it MUST NOT be removed!
	if _, statErr := os.Stat(deterministicSockDir); os.IsNotExist(statErr) {
		t.Fatalf("pre-existing empty socket directory was removed on Stop!")
	}
}

func TestSupervisor_ExplicitSocketDir_ParticipatesInNamespaceLocking(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	writeFakeBinaries(t, binDir)

	dataDirA := filepath.Join(tmpDir, strings.Repeat("data_a_long_path_", 6), "pgdata")
	dataDirB := filepath.Join(tmpDir, "data_b")
	if err := os.MkdirAll(dataDirA, 0700); err != nil {
		t.Fatalf("failed to create dataDirA: %v", err)
	}
	if err := os.MkdirAll(dataDirB, 0700); err != nil {
		t.Fatalf("failed to create dataDirB: %v", err)
	}

	// Supervisor A uses fallback socket directory
	sA, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDirA,
		Port:    5432,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create sA: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sA.Start(ctx); err != nil {
		t.Fatalf("sA.Start failed: %v", err)
	}
	defer func() { _ = sA.Stop(context.Background()) }()

	sockDirA := sA.SocketDir()
	if sockDirA == "" {
		t.Fatal("expected non-empty socketDir for sA")
	}

	// Supervisor B has different dataDir, different port (5433), but explicit SocketDir pointing at sA's socketDir
	sB, err := New(Options{
		BinDir:    binDir,
		DataDir:   dataDirB,
		SocketDir: sockDirA,
		Port:      5433,
		NoSync:    true,
		isRoot:    func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create sB: %v", err)
	}

	err = sB.Start(ctx)
	if err == nil {
		_ = sB.Stop(context.Background())
		t.Fatal("expected sB.Start to fail due to socket namespace collision, got nil")
	}
	if !errors.Is(err, ErrSocketDirLocked) {
		t.Fatalf("expected ErrSocketDirLocked, got: %v", err)
	}

	// Stop sA
	if err := sA.Stop(context.Background()); err != nil {
		t.Fatalf("sA.Stop failed: %v", err)
	}

	// Now sB can start with its explicit SocketDir
	if err := sB.Start(ctx); err != nil {
		t.Fatalf("expected sB.Start to succeed after sA stopped: %v", err)
	}
	if !sB.SocketDirLocked() {
		t.Fatal("expected sB to hold socket lock")
	}
	if err := sB.Stop(context.Background()); err != nil {
		t.Fatalf("sB.Stop failed: %v", err)
	}
}

func TestSupervisor_CrossProcess_ExplicitSocketDirContention(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	writeFakeBinaries(t, binDir)

	dataDirA := filepath.Join(tmpDir, strings.Repeat("long_cluster_a_", 6), "pgdata")
	dataDirB := filepath.Join(tmpDir, "cluster_b")
	if err := os.MkdirAll(dataDirA, 0700); err != nil {
		t.Fatalf("failed to create dataDirA: %v", err)
	}
	if err := os.MkdirAll(dataDirB, 0700); err != nil {
		t.Fatalf("failed to create dataDirB: %v", err)
	}

	sA, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDirA,
		Port:    5432,
		NoSync:  true,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create sA: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sA.Start(ctx); err != nil {
		t.Fatalf("sA.Start failed: %v", err)
	}
	defer func() { _ = sA.Stop(context.Background()) }()

	sockDirA := sA.SocketDir()

	// Spawn subprocess with different dataDir and port, but explicit SocketDir = sockDirA
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+dataDirB,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
		"TOBITEN_TEST_SUBPROCESS_SOCKETDIR="+sockDirA,
		"TOBITEN_TEST_SUBPROCESS_PORT=5433",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected second process to be excluded by socket lock, output: %s", string(out))
	}
	if !strings.Contains(string(out), "embedded postgresql socket directory is locked by another process") {
		t.Fatalf("expected ErrSocketDirLocked in output, got:\n%s", string(out))
	}

	// Stop sA
	if err := sA.Stop(context.Background()); err != nil {
		t.Fatalf("sA.Stop failed: %v", err)
	}

	// Subprocess B can now succeed
	cmd2 := exec.Command(os.Args[0])
	cmd2.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+dataDirB,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
		"TOBITEN_TEST_SUBPROCESS_SOCKETDIR="+sockDirA,
		"TOBITEN_TEST_SUBPROCESS_PORT=5433",
	)
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("expected subprocess B to succeed after sA stopped: %v (output: %s)", err2, string(out2))
	}
}

func TestSupervisor_FailedStopRetainsLock_ExcludesSecondProcess(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	// Long data path forces fallback temporary socket directory
	dataDir := filepath.Join(tmpDir, strings.Repeat("long_stop_retain_", 6), "pgdata")
	otherDataDir := filepath.Join(tmpDir, "other_data")
	if err := os.MkdirAll(otherDataDir, 0700); err != nil {
		t.Fatalf("failed to create otherDataDir: %v", err)
	}
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

	// BOTH DataDirLock AND SocketDirLock MUST be RETAINED!
	if !s.DataDirLocked() {
		t.Fatal("expected DataDirLock to be retained after failed Stop")
	}
	if !s.SocketDirLocked() {
		t.Fatal("expected SocketDirLock to be retained after failed Stop")
	}
	if !s.Running() {
		t.Fatal("expected Running() to be true (retryable state) after failed Stop")
	}

	// 1. Second process attempting to supervise dataDir MUST be excluded
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

	// 2. Third process attempting to use the retained socket directory with different dataDir and port MUST also be excluded
	cmdSocket := exec.Command(os.Args[0])
	cmdSocket.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+otherDataDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
		"TOBITEN_TEST_SUBPROCESS_SOCKETDIR="+s.SocketDir(),
		"TOBITEN_TEST_SUBPROCESS_PORT=5433",
	)
	outSocket, errSocket := cmdSocket.CombinedOutput()
	if errSocket == nil {
		t.Fatalf("expected process targeting retained socket dir to be excluded, output: %s", string(outSocket))
	}
	if !strings.Contains(string(outSocket), "embedded postgresql socket directory is locked by another process") {
		t.Fatalf("expected ErrSocketDirLocked in output, got:\n%s", string(outSocket))
	}

	// Retry Stop: this time confirmExit succeeds
	retryErr := s.Stop(context.Background())
	if retryErr != nil {
		t.Fatalf("expected retry Stop to succeed, got: %v", retryErr)
	}

	if s.DataDirLocked() {
		t.Fatal("expected DataDirLock to be released after successful retry Stop")
	}
	if s.SocketDirLocked() {
		t.Fatal("expected SocketDirLock to be released after successful retry Stop")
	}

	// Second process on dataDir can now succeed
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
	// Use long data path so fallback temporary socket is active
	dataDir := filepath.Join(tmpDir, strings.Repeat("long_pending_launch_", 6), "pgdata")
	otherDataDir := filepath.Join(tmpDir, "other_pending_data")
	if err := os.MkdirAll(otherDataDir, 0700); err != nil {
		t.Fatalf("failed to create otherDataDir: %v", err)
	}
	writeFakeBinaries(t, binDir)

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

	canonicalDataDir := s.DataDir()
	forkedFile := filepath.Join(canonicalDataDir, ".forked")
	pidFile := filepath.Join(canonicalDataDir, "postmaster.pid")
	releaseFile := filepath.Join(canonicalDataDir, ".release")
	hangFile := filepath.Join(canonicalDataDir, ".hang_pgctl")
	if err := os.MkdirAll(canonicalDataDir, 0700); err != nil {
		t.Fatalf("failed to create canonicalDataDir: %v", err)
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

	// Verify pending launch, data lock, and socket lock are RETAINED
	if !s.PendingLaunch() {
		t.Fatal("expected PendingLaunch() to be true")
	}
	if !s.DataDirLocked() {
		t.Fatal("expected DataDirLocked() to be true while pending launch")
	}
	if !s.SocketDirLocked() {
		t.Fatal("expected SocketDirLocked() to be true while pending launch")
	}

	// 1. Subprocess attempting to supervise dataDir must be excluded
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

	// 2. Subprocess attempting to use socketDir with different dataDir and port must be excluded
	cmdSocket := exec.Command(os.Args[0])
	cmdSocket.Env = append(os.Environ(),
		"TOBITEN_TEST_SUBPROCESS_SUPERVISE=1",
		"TOBITEN_TEST_SUBPROCESS_DATADIR="+otherDataDir,
		"TOBITEN_TEST_SUBPROCESS_BINDIR="+binDir,
		"TOBITEN_TEST_SUBPROCESS_SOCKETDIR="+s.SocketDir(),
		"TOBITEN_TEST_SUBPROCESS_PORT=5433",
	)
	outSocket, errSocket := cmdSocket.CombinedOutput()
	if errSocket == nil {
		t.Fatalf("expected process targeting retained socket dir to be excluded during pending launch, output: %s", string(outSocket))
	}
	if !strings.Contains(string(outSocket), "embedded postgresql socket directory is locked by another process") {
		t.Fatalf("expected ErrSocketDirLocked in output, got:\n%s", string(outSocket))
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
	if s.SocketDirLocked() {
		t.Fatal("expected SocketDirLocked() to be false after successful Stop")
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
