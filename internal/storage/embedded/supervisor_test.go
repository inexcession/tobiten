package embedded

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

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
