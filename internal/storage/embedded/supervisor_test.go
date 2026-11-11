package embedded

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestSupervisor_StartupCancellationCleanup(t *testing.T) {
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

	// Start with already-cancelled context
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
