package embedded

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSupervisor_Integration(t *testing.T) {
	binDir, err := FindBinDir("")
	if err != nil {
		t.Skipf("skipping integration test: no PostgreSQL %d binaries found on host: %v", PinnedMajor, err)
	}

	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "pgdata")

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		NoSync:  true, // Fast initdb for test
	})
	if err != nil {
		t.Fatalf("failed to create supervisor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// Start the supervised instance
	if err := s.Start(ctx); err != nil {
		t.Fatalf("supervisor Start failed: %v", err)
	}

	if !s.Running() {
		t.Fatal("expected supervisor to be Running()")
	}
	savedPID := s.PID()
	if savedPID <= 0 {
		t.Fatalf("expected positive PID, got %d", savedPID)
	}
	if s.SocketDir() == "" {
		t.Fatal("expected non-empty SocketDir")
	}

	// 1. Connect through pgx via DatabaseURL
	dbURL := s.DatabaseURL()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		_ = s.Stop(context.Background())
		t.Fatalf("failed to connect via DatabaseURL %q: %v", dbURL, err)
	}

	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		pool.Close()
		_ = s.Stop(context.Background())
		t.Fatalf("failed to execute query on embedded instance: %v", err)
	}
	if one != 1 {
		pool.Close()
		_ = s.Stop(context.Background())
		t.Fatalf("expected 1, got %d", one)
	}

	var currentDB, currentUser string
	if err := pool.QueryRow(ctx, "SELECT current_database(), current_user").Scan(&currentDB, &currentUser); err != nil {
		pool.Close()
		_ = s.Stop(context.Background())
		t.Fatalf("failed to query database and user: %v", err)
	}
	if currentDB != "postgres" || currentUser != "postgres" {
		pool.Close()
		_ = s.Stop(context.Background())
		t.Fatalf("expected postgres/postgres, got %s/%s", currentDB, currentUser)
	}
	pool.Close()

	// 2. Connect through pgx via ConnString
	connStr := s.ConnString()
	pool2, err := pgxpool.New(ctx, connStr)
	if err != nil {
		_ = s.Stop(context.Background())
		t.Fatalf("failed to connect via ConnString %q: %v", connStr, err)
	}
	var fortyTwo int
	if err := pool2.QueryRow(ctx, "SELECT 42").Scan(&fortyTwo); err != nil {
		pool2.Close()
		_ = s.Stop(context.Background())
		t.Fatalf("failed to query with ConnString: %v", err)
	}
	if fortyTwo != 42 {
		pool2.Close()
		_ = s.Stop(context.Background())
		t.Fatalf("expected 42, got %d", fortyTwo)
	}
	pool2.Close()

	// 3. Stop cleanly
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer stopCancel()

	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("supervisor Stop failed: %v", err)
	}

	if s.Running() {
		t.Fatal("expected supervisor to not be Running() after Stop")
	}
	if s.PID() != 0 {
		t.Fatalf("expected PID() to be 0 after Stop, got %d", s.PID())
	}

	// 4. External post-stop check: confirm owned process has truly exited
	killErr := syscall.Kill(savedPID, 0)
	if !errors.Is(killErr, syscall.ESRCH) {
		t.Fatalf("expected process %d to be dead (ESRCH), got: %v", savedPID, killErr)
	}

	// 5. External post-stop check: confirm connection attempts fail
	connCheckCtx, connCheckCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer connCheckCancel()
	deadPool, poolErr := pgxpool.New(connCheckCtx, dbURL)
	if poolErr == nil {
		pingErr := deadPool.Ping(connCheckCtx)
		deadPool.Close()
		if pingErr == nil {
			t.Fatal("expected connection to stopped PostgreSQL instance to fail, but ping succeeded")
		}
	}

	// 6. Confirm UNIX socket is not accepting connections
	sockPath := filepath.Join(s.SocketDir(), ".s.PGSQL.5432")
	conn, dialErr := net.DialTimeout("unix", sockPath, 500*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		t.Fatalf("expected unix socket dial to fail after stop, but connection succeeded")
	}

	// 7. Idempotent stop check
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("subsequent Stop call failed: %v", err)
	}
}

func TestSupervisor_InitializeBeforeNestedSocketDir_Integration(t *testing.T) {
	binDir, err := FindBinDir("")
	if err != nil {
		t.Skipf("skipping integration test: no PostgreSQL %d binaries found on host: %v", PinnedMajor, err)
	}

	tmpDir, err := os.MkdirTemp("/tmp", "tbt-nst-*")
	if err != nil {
		t.Fatalf("failed to create temp dir in /tmp: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()
	dataDir := filepath.Join(tmpDir, "pgdata")
	nestedSocketDir := filepath.Join(dataDir, "nested-sock")

	s, err := New(Options{
		BinDir:    binDir,
		DataDir:   dataDir,
		SocketDir: nestedSocketDir,
		NoSync:    true,
	})
	if err != nil {
		t.Fatalf("failed to create supervisor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start supervisor with nested socket dir: %v", err)
	}
	defer func() {
		_ = s.Stop(context.Background())
	}()

	pool, err := pgxpool.New(ctx, s.DatabaseURL())
	if err != nil {
		t.Fatalf("failed to connect to instance with nested socket dir: %v", err)
	}
	defer pool.Close()

	var result int
	if err := pool.QueryRow(ctx, "SELECT 100").Scan(&result); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if result != 100 {
		t.Fatalf("expected 100, got %d", result)
	}
}

func TestSupervisor_RefuseRoot_Integration(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(Options{
		DataDir: tmpDir,
		isRoot:  func() bool { return true },
	})
	if err != nil {
		t.Fatalf("failed to create supervisor: %v", err)
	}

	err = s.Start(context.Background())
	if !errors.Is(err, ErrRootForbidden) {
		t.Fatalf("expected ErrRootForbidden, got %v", err)
	}
}
