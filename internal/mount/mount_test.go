package mount

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestDetectFilesystem_NativeHost(t *testing.T) {
	dir := t.TempDir()
	info, err := DetectFilesystem(dir)
	if err != nil {
		t.Fatalf("DetectFilesystem(%q) error = %v", dir, err)
	}
	if info.Type == "" {
		t.Fatal("expected non-empty filesystem type")
	}

	infoBytes, err := DetectFilesystemBytes([]byte(dir))
	if err != nil {
		t.Fatalf("DetectFilesystemBytes(%q) error = %v", dir, err)
	}
	if infoBytes != info {
		t.Fatalf("DetectFilesystemBytes = %+v, want %+v", infoBytes, info)
	}
}

func TestDetectFilesystem_Errors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if _, err := DetectFilesystem(missing); err == nil {
		t.Fatalf("expected error for missing path %q", missing)
	}

	for _, bad := range [][]byte{nil, {}, []byte("/tmp/has\x00nul")} {
		if _, err := DetectFilesystemBytes(bad); !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("DetectFilesystemBytes(%q) err = %v, want ErrInvalidPath", bad, err)
		}
	}
}
