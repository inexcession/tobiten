package embedded

import (
	"os"
	"path/filepath"
	"testing"
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
		{"PostgreSQL 19.0beta1", 19, false},
		{"invalid version output", 0, true},
		{"", 0, true},
	}

	for _, tc := range tests {
		major, err := ParsePgMajorVersion(tc.input)
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

func TestFindBinDir(t *testing.T) {
	dir, err := FindBinDir("")
	if err != nil {
		t.Skipf("skipping TestFindBinDir: no PostgreSQL %d binaries found on host: %v", PinnedMajor, err)
	}
	if dir == "" {
		t.Fatal("expected non-empty binary directory")
	}

	if !IsValidBinDir(dir, func(major int) bool { return major == PinnedMajor }) {
		t.Fatalf("directory %q is not a valid postgresql %d bin directory", dir, PinnedMajor)
	}
}

func TestFindBinDirAtLeast(t *testing.T) {
	dir, err := FindBinDirAtLeast(18, "")
	if err != nil {
		t.Skipf("skipping TestFindBinDirAtLeast: no PostgreSQL 18+ binaries found on host: %v", err)
	}
	if dir == "" {
		t.Fatal("expected non-empty binary directory")
	}

	if !IsValidBinDir(dir, func(major int) bool { return major >= 18 }) {
		t.Fatalf("directory %q is not a valid postgresql 18+ bin directory", dir)
	}
}

func TestFindBinDir_NotFound(t *testing.T) {
	// Search in an explicit non-existent directory and without matching candidate paths
	tmpEmpty := t.TempDir()
	_, err := FindBinDirMatching(tmpEmpty, func(major int) bool {
		return major == 9999 // impossible major version
	})
	if err == nil {
		t.Fatal("expected error when no matching binaries exist, got nil")
	}
}

func TestIsValidBinDir_InvalidDir(t *testing.T) {
	tmpDir := t.TempDir()

	// Empty dir: no binaries
	if IsValidBinDir(tmpDir, nil) {
		t.Fatal("expected IsValidBinDir to return false for empty dir")
	}

	// Missing initdb
	pgCtl := filepath.Join(tmpDir, "pg_ctl")
	if err := os.WriteFile(pgCtl, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatalf("failed to write dummy pg_ctl: %v", err)
	}
	if IsValidBinDir(tmpDir, nil) {
		t.Fatal("expected IsValidBinDir to return false when initdb is missing")
	}

	// Non-executable pg_ctl
	initdb := filepath.Join(tmpDir, "initdb")
	if err := os.WriteFile(initdb, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatalf("failed to write dummy initdb: %v", err)
	}
	if err := os.Chmod(pgCtl, 0644); err != nil {
		t.Fatalf("failed to chmod pg_ctl: %v", err)
	}
	if IsValidBinDir(tmpDir, nil) {
		t.Fatal("expected IsValidBinDir to return false when pg_ctl is not executable")
	}
}
