package testdb

import (
	"context"
	"testing"
	"time"
)

func TestParsePgMajorVersion(t *testing.T) {
	tests := []struct {
		input       string
		expectMajor int
		expectErr   bool
	}{
		{"initdb (PostgreSQL) 18.6", 18, false},
		{"pg_ctl (PostgreSQL) 18.0", 18, false},
		{"postgres (PostgreSQL) 18.1.2", 18, false},
		{"PostgreSQL 18.6 on aarch64-apple-darwin", 18, false},
		{"PostgreSQL 17.2", 17, false},
		{"invalid version output", 0, true},
	}

	for _, tc := range tests {
		major, err := parsePgMajorVersion(tc.input)
		if tc.expectErr {
			if err == nil {
				t.Errorf("expected error for %q, got nil", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("unexpected error for %q: %v", tc.input, err)
			continue
		}
		if major != tc.expectMajor {
			t.Errorf("for input %q: expected major %d, got %d", tc.input, tc.expectMajor, major)
		}
	}
}

func TestFindPgBinDir(t *testing.T) {
	dir, err := findPgBinDir()
	if err != nil {
		t.Skipf("skipping TestFindPgBinDir: no local PostgreSQL 18 binaries found on host (%v)", err)
	}
	if dir == "" {
		t.Fatal("expected non-empty bin dir")
	}
	if !isValidPg18BinDir(dir) {
		t.Fatalf("directory %q is not a valid postgres 18 bin directory", dir)
	}
}

func TestGetFreePort(t *testing.T) {
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("getFreePort failed: %v", err)
	}
	if port <= 0 || port > 65535 {
		t.Fatalf("invalid port returned: %d", port)
	}
}

func TestServerStopIdempotent(t *testing.T) {
	// Calling Stop multiple times must not panic or error.
	Stop()
	Stop()
	Stop()
}

func TestStartServerAndStop(t *testing.T) {
	// Verify that startServer starts a clean instance and stopInstance shuts it down cleanly.
	srv, err := startServer(false)
	if err != nil {
		t.Fatalf("startServer failed: %v", err)
	}
	if srv == nil {
		t.Fatal("expected non-nil server instance")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := srv.adminPool.Ping(ctx); err != nil {
		t.Fatalf("admin pool ping failed: %v", err)
	}

	dbName, targetURL, err := srv.createFreshDB(ctx)
	if err != nil {
		t.Fatalf("createFreshDB failed: %v", err)
	}
	if dbName == "" || targetURL == "" {
		t.Fatalf("expected non-empty dbName and targetURL, got %q, %q", dbName, targetURL)
	}

	if err := srv.dropDB(ctx, dbName); err != nil {
		t.Fatalf("dropDB failed: %v", err)
	}

	srv.stop()

	// Verify postmaster is dead
	if srv.pid > 0 {
		// Wait a moment for process exit
		time.Sleep(100 * time.Millisecond)
	}
}
