package paths

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalPath_SymlinkAndTraversalSemantics(t *testing.T) {
	base := t.TempDir()
	canonBase, err := CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath(base) failed: %v", err)
	}

	realDir := filepath.Join(canonBase, "real")
	subDir := filepath.Join(realDir, "sub")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(subDir): %v", err)
	}
	otherDir := filepath.Join(canonBase, "other")
	if err := os.MkdirAll(otherDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(otherDir): %v", err)
	}

	aliasDir := filepath.Join(canonBase, "alias")
	if err := os.Symlink(subDir, aliasDir); err != nil {
		t.Fatalf("Symlink(aliasDir): %v", err)
	}

	relLink := filepath.Join(otherDir, "rel_link")
	if err := os.Symlink("../real/sub", relLink); err != nil {
		t.Fatalf("Symlink(relLink): %v", err)
	}

	brokenTarget := filepath.Join(canonBase, "not_yet_created", "leaf")
	brokenLink := filepath.Join(canonBase, "broken_link")
	if err := os.Symlink(brokenTarget, brokenLink); err != nil {
		t.Fatalf("Symlink(brokenLink): %v", err)
	}

	t.Chdir(canonBase)

	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "root slash", in: "/", want: "/"},
		{name: "trailing slash", in: realDir + "/", want: realDir},
		{name: "symlink alias", in: aliasDir, want: subDir},
		{name: "symlink with dotdot", in: aliasDir + "/..", want: realDir},
		{name: "relative symlink", in: relLink, want: subDir},
		{name: "relative path", in: "./other/../real/sub", want: subDir},
		{name: "broken symlink", in: brokenLink, want: brokenTarget},
		{name: "nonexistent under symlink", in: aliasDir + "/future/child", want: filepath.Join(subDir, "future", "child")},
		{name: "empty defaults to cwd", in: "", want: canonBase},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CanonicalPath(tc.in)
			if err != nil {
				t.Fatalf("CanonicalPath(%q) error = %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("CanonicalPath(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCanonicalPath_RejectsInvalidTraversalAndSymlinkLoop(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	canonBase, err := CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath(base): %v", err)
	}

	// Refuse '..' through non-existent directory.
	if _, err := CanonicalPath(canonBase + "/missing/../real"); err == nil {
		t.Fatal("expected error for '..' through non-existent directory")
	}

	// Refuse '..' through a regular file.
	regFile := filepath.Join(canonBase, "file.txt")
	if err := os.WriteFile(regFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := CanonicalPath(regFile + "/../other"); err == nil {
		t.Fatal("expected error for '..' through regular file")
	}

	// Refuse NUL bytes.
	if _, err := CanonicalPath("/mnt/\x00/a"); !errors.Is(err, ErrInvalidRawPath) {
		t.Fatalf("CanonicalPath(NUL) err = %v, want ErrInvalidRawPath", err)
	}
	if _, err := CanonicalPathBytes(nil); !errors.Is(err, ErrInvalidRawPath) {
		t.Fatalf("CanonicalPathBytes(nil) err = %v, want ErrInvalidRawPath", err)
	}
	if _, err := CanonicalPathBytes([]byte("/mnt/\x00/a")); !errors.Is(err, ErrInvalidRawPath) {
		t.Fatalf("CanonicalPathBytes(NUL) err = %v, want ErrInvalidRawPath", err)
	}

	// 64-link limit: 64 symlinks succeed, 65 fail.
	target := filepath.Join(canonBase, "final")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("MkdirAll(final): %v", err)
	}
	cur := target
	for i := 1; i <= 65; i++ {
		next := filepath.Join(canonBase, fmt.Sprintf("link-%02d", i))
		if err := os.Symlink(cur, next); err != nil {
			t.Fatalf("Symlink(%d): %v", i, err)
		}
		cur = next
	}

	link64 := filepath.Join(canonBase, "link-64")
	got64, err := CanonicalPath(link64)
	if err != nil {
		t.Fatalf("CanonicalPath(link-64) unexpected error: %v", err)
	}
	if got64 != target {
		t.Fatalf("CanonicalPath(link-64) = %q, want %q", got64, target)
	}

	link65 := filepath.Join(canonBase, "link-65")
	_, err = CanonicalPath(link65)
	if err == nil || !strings.Contains(err.Error(), "too many levels of symbolic links") {
		t.Fatalf("CanonicalPath(link-65) err = %v, want symlink depth error", err)
	}
}

func TestCanonicalPathBytes_PreservesInvalidUTF8(t *testing.T) {
	t.Parallel()
	raw := []byte{'/', 'm', 'n', 't', '/', 'l', 'i', 'b', '-', 0xff, 0xfe, '/', 's', 'u', 'b', '/'}
	want := []byte{'/', 'm', 'n', 't', '/', 'l', 'i', 'b', '-', 0xff, 0xfe, '/', 's', 'u', 'b'}

	got, err := CanonicalPathBytes(raw)
	if err != nil {
		t.Fatalf("CanonicalPathBytes(%x) error = %v", raw, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("CanonicalPathBytes(%x) = %x, want %x", raw, got, want)
	}
}
