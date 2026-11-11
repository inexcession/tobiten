package embedded

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseClusterVersion(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		wantMajor int
		wantErr   bool
		errTarget error
	}{
		{
			name:      "exact 18 newline",
			content:   "18\n",
			wantMajor: 18,
		},
		{
			name:      "exact 18 bare",
			content:   "18",
			wantMajor: 18,
		},
		{
			name:      "padded whitespace",
			content:   "  18 \t\n",
			wantMajor: 18,
		},
		{
			name:      "minor version notation",
			content:   "18.2\n",
			wantMajor: 18,
		},
		{
			name:      "older major 17",
			content:   "17\n",
			wantMajor: 17,
		},
		{
			name:      "older major 16",
			content:   "16\n",
			wantMajor: 16,
		},
		{
			name:      "newer major 19",
			content:   "19\n",
			wantMajor: 19,
		},
		{
			name:      "empty file",
			content:   "",
			wantErr:   true,
			errTarget: ErrInvalidPGVersion,
		},
		{
			name:      "whitespace only",
			content:   "   \n\t  ",
			wantErr:   true,
			errTarget: ErrInvalidPGVersion,
		},
		{
			name:      "non-numeric",
			content:   "invalid\n",
			wantErr:   true,
			errTarget: ErrInvalidPGVersion,
		},
		{
			name:      "negative number",
			content:   "-18\n",
			wantErr:   true,
			errTarget: ErrInvalidPGVersion,
		},
		{
			name:      "zero",
			content:   "0\n",
			wantErr:   true,
			errTarget: ErrInvalidPGVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseClusterVersion(tt.content)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errTarget != nil && !errors.Is(err, tt.errTarget) {
					t.Fatalf("expected error wrapping %v, got: %v", tt.errTarget, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantMajor {
				t.Fatalf("expected major %d, got %d", tt.wantMajor, got)
			}
		})
	}
}

func TestValidateClusterVersion_MissingIsNil(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "nonexistent")

	if err := ValidateClusterVersion(dataDir); err != nil {
		t.Fatalf("expected nil error for missing PG_VERSION, got: %v", err)
	}
}

func TestValidateClusterVersion_OlderRefusedWithActionableMessage(t *testing.T) {
	for _, oldVer := range []int{17, 16, 15, 14, 13} {
		t.Run(fmt.Sprintf("version_%d", oldVer), func(t *testing.T) {
			tmpDir := t.TempDir()
			pgVersionPath := filepath.Join(tmpDir, "PG_VERSION")
			if err := os.WriteFile(pgVersionPath, []byte(fmt.Sprintf("%d\n", oldVer)), 0600); err != nil {
				t.Fatalf("failed to write PG_VERSION: %v", err)
			}

			err := ValidateClusterVersion(tmpDir)
			if err == nil {
				t.Fatalf("expected refusal for older version %d, got nil", oldVer)
			}
			if !errors.Is(err, ErrOlderPGVersion) {
				t.Fatalf("expected error wrapping ErrOlderPGVersion, got: %v", err)
			}
			if !strings.Contains(err.Error(), "tobiten-cli db upgrade") {
				t.Fatalf("expected error to contain actionable 'tobiten-cli db upgrade' guidance, got: %v", err)
			}
		})
	}
}

func TestValidateClusterVersion_NewerRefused(t *testing.T) {
	tmpDir := t.TempDir()
	pgVersionPath := filepath.Join(tmpDir, "PG_VERSION")
	if err := os.WriteFile(pgVersionPath, []byte("19\n"), 0600); err != nil {
		t.Fatalf("failed to write PG_VERSION: %v", err)
	}

	err := ValidateClusterVersion(tmpDir)
	if err == nil {
		t.Fatal("expected refusal for newer version 19, got nil")
	}
	if !errors.Is(err, ErrIncompatiblePGVersion) {
		t.Fatalf("expected error wrapping ErrIncompatiblePGVersion, got: %v", err)
	}
}

func TestValidateClusterVersion_ExactPinnedAllowed(t *testing.T) {
	tmpDir := t.TempDir()
	pgVersionPath := filepath.Join(tmpDir, "PG_VERSION")
	if err := os.WriteFile(pgVersionPath, []byte(fmt.Sprintf("%d\n", PinnedMajor)), 0600); err != nil {
		t.Fatalf("failed to write PG_VERSION: %v", err)
	}

	if err := ValidateClusterVersion(tmpDir); err != nil {
		t.Fatalf("expected pinned version %d to be accepted, got: %v", PinnedMajor, err)
	}
}

func TestSupervisor_RefuseOlderPGVersionWithoutConfigMutation(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}
	createDummyBinary(t, binDir, "pg_ctl", "18.6", true)
	createDummyBinary(t, binDir, "initdb", "18.6", true)
	createDummyBinary(t, binDir, "postgres", "18.6", true)

	dataDir := filepath.Join(tmpDir, "pgdata")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatalf("failed to create dataDir: %v", err)
	}

	// Fabricate an older PostgreSQL 17 cluster
	pgVersionPath := filepath.Join(dataDir, "PG_VERSION")
	if err := os.WriteFile(pgVersionPath, []byte("17\n"), 0600); err != nil {
		t.Fatalf("failed to write PG_VERSION: %v", err)
	}

	confPath := filepath.Join(dataDir, "postgresql.conf")
	originalConfContent := "# Original unmutated configuration from PG 17\nshared_buffers = 128MB\n"
	if err := os.WriteFile(confPath, []byte(originalConfContent), 0600); err != nil {
		t.Fatalf("failed to write postgresql.conf: %v", err)
	}

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("unexpected error creating supervisor: %v", err)
	}

	startErr := s.Start(context.Background())
	if startErr == nil {
		t.Fatal("expected Start to refuse older PG_VERSION 17, got nil")
	}

	if !errors.Is(startErr, ErrOlderPGVersion) {
		t.Fatalf("expected error wrapping ErrOlderPGVersion, got: %v", startErr)
	}
	if !strings.Contains(startErr.Error(), "tobiten-cli db upgrade") {
		t.Fatalf("expected error to contain actionable 'tobiten-cli db upgrade' guidance, got: %v", startErr)
	}

	// Prove configuration was NOT mutated or rewritten
	confBytes, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	if string(confBytes) != originalConfContent {
		t.Fatalf("postgresql.conf was mutated despite refusal:\nExpected:\n%s\nGot:\n%s", originalConfContent, string(confBytes))
	}

	// Prove supervisor did not record running state
	if s.Running() {
		t.Fatal("supervisor reported Running() == true after refusal")
	}
	if s.PID() != 0 {
		t.Fatalf("expected PID == 0 after refusal, got %d", s.PID())
	}
}

func TestSupervisor_RefuseCorruptedPGVersionWithoutConfigMutation(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}
	createDummyBinary(t, binDir, "pg_ctl", "18.6", true)
	createDummyBinary(t, binDir, "initdb", "18.6", true)
	createDummyBinary(t, binDir, "postgres", "18.6", true)

	dataDir := filepath.Join(tmpDir, "pgdata")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatalf("failed to create dataDir: %v", err)
	}

	pgVersionPath := filepath.Join(dataDir, "PG_VERSION")
	if err := os.WriteFile(pgVersionPath, []byte("garbage\n"), 0600); err != nil {
		t.Fatalf("failed to write corrupted PG_VERSION: %v", err)
	}

	confPath := filepath.Join(dataDir, "postgresql.conf")
	originalConf := "# Unchanged\n"
	if err := os.WriteFile(confPath, []byte(originalConf), 0600); err != nil {
		t.Fatalf("failed to write postgresql.conf: %v", err)
	}

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		isRoot:  func() bool { return false },
	})
	if err != nil {
		t.Fatalf("unexpected error creating supervisor: %v", err)
	}

	startErr := s.Start(context.Background())
	if startErr == nil {
		t.Fatal("expected Start to refuse corrupted PG_VERSION, got nil")
	}
	if !errors.Is(startErr, ErrInvalidPGVersion) {
		t.Fatalf("expected ErrInvalidPGVersion, got: %v", startErr)
	}

	confBytes, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("failed to read postgresql.conf: %v", err)
	}
	if string(confBytes) != originalConf {
		t.Fatalf("postgresql.conf was mutated despite refusal")
	}
}

func TestSupervisor_NetworkFilesystemLoudWarning(t *testing.T) {
	networkFSTypes := []string{"nfs", "smbfs", "cifs", "smb"}

	for _, fsType := range networkFSTypes {
		t.Run("fs_"+fsType, func(t *testing.T) {
			tmpDir := t.TempDir()
			binDir := filepath.Join(tmpDir, "bins")
			if err := os.MkdirAll(binDir, 0755); err != nil {
				t.Fatalf("failed to create binDir: %v", err)
			}
			createDummyBinary(t, binDir, "pg_ctl", "18.6", true)
			createDummyBinary(t, binDir, "initdb", "18.6", true)
			createDummyBinary(t, binDir, "postgres", "18.6", true)

			dataDir := filepath.Join(tmpDir, "pgdata")

			var logBuf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			s, err := New(Options{
				BinDir:  binDir,
				DataDir: dataDir,
				Logger:  logger,
				isRoot:  func() bool { return false },
				fsProbe: func(path string) (string, bool, error) {
					return fsType, true, nil
				},
			})
			if err != nil {
				t.Fatalf("unexpected error creating supervisor: %v", err)
			}

			// Pre-cancelled context stops Start right after pre-checks (before running pg_ctl)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_ = s.Start(ctx)

			// 1. Verify diagnostic reporting
			if !s.IsNetworkFS() {
				t.Fatalf("expected IsNetworkFS() == true for %s", fsType)
			}
			if s.FilesystemType() != fsType {
				t.Fatalf("expected FilesystemType() == %q, got %q", fsType, s.FilesystemType())
			}

			// 2. Verify loud warning was logged with appropriate attributes
			logOutput := logBuf.String()
			if !strings.Contains(logOutput, `"level":"WARN"`) {
				t.Fatalf("expected WARN log entry for network filesystem %s, got logs:\n%s", fsType, logOutput)
			}
			if !strings.Contains(logOutput, "network filesystem") {
				t.Fatalf("expected log to mention 'network filesystem', got:\n%s", logOutput)
			}
			if !strings.Contains(logOutput, fsType) {
				t.Fatalf("expected log to mention fs_type %s, got:\n%s", fsType, logOutput)
			}
			if !strings.Contains(logOutput, dataDir) {
				t.Fatalf("expected log to mention data_dir %s, got:\n%s", dataDir, logOutput)
			}
		})
	}
}

func TestSupervisor_LocalFilesystemNoWarning(t *testing.T) {
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bins")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create binDir: %v", err)
	}
	createDummyBinary(t, binDir, "pg_ctl", "18.6", true)
	createDummyBinary(t, binDir, "initdb", "18.6", true)
	createDummyBinary(t, binDir, "postgres", "18.6", true)

	dataDir := filepath.Join(tmpDir, "pgdata")

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s, err := New(Options{
		BinDir:  binDir,
		DataDir: dataDir,
		Logger:  logger,
		isRoot:  func() bool { return false },
		fsProbe: func(path string) (string, bool, error) {
			return "apfs", false, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.Start(ctx)

	if s.IsNetworkFS() {
		t.Fatal("expected IsNetworkFS() == false for local filesystem")
	}
	if s.FilesystemType() != "apfs" {
		t.Fatalf("expected FilesystemType() == 'apfs', got %q", s.FilesystemType())
	}

	logOutput := logBuf.String()
	if strings.Contains(logOutput, `"level":"WARN"`) {
		t.Fatalf("did not expect WARN log for local filesystem, got:\n%s", logOutput)
	}
}

func TestSupervisor_CheckFilesystemMethod(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(Options{
		DataDir: tmpDir,
		fsProbe: func(path string) (string, bool, error) {
			return "nfs", true, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	info, err := s.CheckFilesystem()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !info.IsNetwork || info.Type != "nfs" {
		t.Fatalf("unexpected info: %+v", info)
	}
	if !s.IsNetworkFS() || s.FilesystemType() != "nfs" {
		t.Fatalf("expected IsNetworkFS()=true, FilesystemType()=nfs; got %v, %s", s.IsNetworkFS(), s.FilesystemType())
	}
}

func TestFindExistingAncestor(t *testing.T) {
	tmpDir := t.TempDir()
	deepNonExistent := filepath.Join(tmpDir, "sub1", "sub2", "sub3", "pgdata")

	ancestor := findExistingAncestor(deepNonExistent)
	if ancestor != tmpDir {
		t.Fatalf("expected ancestor %q, got %q", tmpDir, ancestor)
	}

	// Existing directory resolves to itself
	if findExistingAncestor(tmpDir) != tmpDir {
		t.Fatalf("expected existing dir %q to resolve to itself, got %q", tmpDir, findExistingAncestor(tmpDir))
	}
}

func TestDetectFilesystem_NativeHost(t *testing.T) {
	tmpDir := t.TempDir()
	info, err := DetectFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("unexpected error calling DetectFilesystem on host: %v", err)
	}
	if info.Type == "" {
		t.Fatal("expected non-empty filesystem type on host")
	}
	t.Logf("Host temporary directory filesystem: Type=%s, IsNetwork=%v", info.Type, info.IsNetwork)
}
