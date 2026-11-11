package embedded

import (
	"fmt"
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
	tmpEmpty := t.TempDir()
	_, err := FindBinDirMatching(tmpEmpty, func(major int) bool {
		return major == 9999
	})
	if err == nil {
		t.Fatal("expected error when no matching binaries exist, got nil")
	}
}

func createDummyBinary(t *testing.T, dir, name string, version string, executable bool) {
	t.Helper()
	p := filepath.Join(dir, name)
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo \"%s (PostgreSQL) %s\"\nfi\nexit 0\n", name, version)
	mode := os.FileMode(0644)
	if executable {
		mode = 0755
	}
	if err := os.WriteFile(p, []byte(script), mode); err != nil {
		t.Fatalf("failed to create dummy binary %s: %v", name, err)
	}
}

func TestValidateBinDir_CompleteSet(t *testing.T) {
	tmpDir := t.TempDir()

	createDummyBinary(t, tmpDir, "pg_ctl", "18.6", true)
	createDummyBinary(t, tmpDir, "initdb", "18.6", true)
	createDummyBinary(t, tmpDir, "postgres", "18.6", true)

	if err := ValidateBinDir(tmpDir, func(m int) bool { return m == 18 }); err != nil {
		t.Fatalf("expected valid binary set to pass, got: %v", err)
	}
}

func TestValidateBinDir_MissingBinary(t *testing.T) {
	// Missing postgres
	tmp1 := t.TempDir()
	createDummyBinary(t, tmp1, "pg_ctl", "18.6", true)
	createDummyBinary(t, tmp1, "initdb", "18.6", true)
	if err := ValidateBinDir(tmp1, func(m int) bool { return m == 18 }); err == nil {
		t.Fatal("expected error for missing postgres binary, got nil")
	}

	// Missing initdb
	tmp2 := t.TempDir()
	createDummyBinary(t, tmp2, "pg_ctl", "18.6", true)
	createDummyBinary(t, tmp2, "postgres", "18.6", true)
	if err := ValidateBinDir(tmp2, func(m int) bool { return m == 18 }); err == nil {
		t.Fatal("expected error for missing initdb binary, got nil")
	}

	// Missing pg_ctl
	tmp3 := t.TempDir()
	createDummyBinary(t, tmp3, "initdb", "18.6", true)
	createDummyBinary(t, tmp3, "postgres", "18.6", true)
	if err := ValidateBinDir(tmp3, func(m int) bool { return m == 18 }); err == nil {
		t.Fatal("expected error for missing pg_ctl binary, got nil")
	}
}

func TestValidateBinDir_NonExecutable(t *testing.T) {
	tmpDir := t.TempDir()
	createDummyBinary(t, tmpDir, "pg_ctl", "18.6", false) // non-executable
	createDummyBinary(t, tmpDir, "initdb", "18.6", true)
	createDummyBinary(t, tmpDir, "postgres", "18.6", true)

	if err := ValidateBinDir(tmpDir, func(m int) bool { return m == 18 }); err == nil {
		t.Fatal("expected error for non-executable pg_ctl, got nil")
	}
}

func TestValidateBinDir_MismatchedVersion(t *testing.T) {
	tmpDir := t.TempDir()
	createDummyBinary(t, tmpDir, "pg_ctl", "17.2", true) // major 17
	createDummyBinary(t, tmpDir, "initdb", "17.2", true)
	createDummyBinary(t, tmpDir, "postgres", "17.2", true)

	if err := ValidateBinDir(tmpDir, func(m int) bool { return m == 18 }); err == nil {
		t.Fatal("expected error for major version 17 when 18 required, got nil")
	}
}
