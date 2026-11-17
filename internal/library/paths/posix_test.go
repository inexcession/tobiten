package paths

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
)

func TestJoinRootRelative_ValidationAndPreservation(t *testing.T) {
	root := []byte("/library/root")

	validCases := []struct {
		name string
		root []byte
		rel  []byte
		want []byte
	}{
		{
			name: "nested relative",
			root: root,
			rel:  []byte("Series/v01.cbz"),
			want: []byte("/library/root/Series/v01.cbz"),
		},
		{
			name: "root with trailing slash",
			root: []byte("/library/root/"),
			rel:  []byte("Series/v01.cbz"),
			want: []byte("/library/root/Series/v01.cbz"),
		},
		{
			name: "filesystem root slash",
			root: []byte("/"),
			rel:  []byte("v01.cbz"),
			want: []byte("/v01.cbz"),
		},
		{
			name: "posix punctuation preserved",
			root: root,
			rel:  []byte(`dir/..\a:sample.cbz`),
			want: []byte(`/library/root/dir/..\a:sample.cbz`),
		},
		{
			name: "invalid utf8 bytes preserved",
			root: root,
			rel:  []byte{0xff, 0xfe, '/', 'b', 'o', 'o', 'k', '.', 'c', 'b', 'z'},
			want: append([]byte("/library/root/"), []byte{0xff, 0xfe, '/', 'b', 'o', 'o', 'k', '.', 'c', 'b', 'z'}...),
		},
	}

	for _, tc := range validCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := JoinRootRelative(tc.root, tc.rel)
			if err != nil {
				t.Fatalf("JoinRootRelative unexpected error: %v", err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("expected joined bytes %x (%q), got %x (%q)", tc.want, tc.want, got, got)
			}
		})
	}

	badRelatives := [][]byte{
		nil,
		{},
		[]byte("/abs/path.cbz"),
		[]byte("."),
		[]byte("./book.cbz"),
		[]byte(".."),
		[]byte("../escape.cbz"),
		[]byte("sub/.."),
		[]byte("sub/../../escape.cbz"),
		[]byte("sub//book.cbz"),
		[]byte("sub/"),
		[]byte("nul\x00byte.cbz"),
	}
	for _, rel := range badRelatives {
		if _, err := JoinRootRelative(root, rel); !errors.Is(err, domain.ErrInvalidAssetPath) {
			t.Errorf("JoinRootRelative(%q, %q) expected ErrInvalidAssetPath, got %v", root, rel, err)
		}
		if _, err := OpenRootRelative(root, rel); !errors.Is(err, domain.ErrInvalidAssetPath) {
			t.Errorf("OpenRootRelative(%q, %q) expected ErrInvalidAssetPath, got %v", root, rel, err)
		}
		if _, err := ReadRootRelative(root, rel); !errors.Is(err, domain.ErrInvalidAssetPath) {
			t.Errorf("ReadRootRelative(%q, %q) expected ErrInvalidAssetPath, got %v", root, rel, err)
		}
	}

	badRoots := [][]byte{
		nil,
		{},
		[]byte("relative/root"),
		[]byte("/root/\x00bad"),
	}
	for _, badRoot := range badRoots {
		if _, err := JoinRootRelative(badRoot, []byte("v01.cbz")); !errors.Is(err, domain.ErrInvalidRootPath) {
			t.Errorf("JoinRootRelative(%q, v01.cbz) expected ErrInvalidRootPath, got %v", badRoot, err)
		}
	}

	if _, err := OpenRaw(nil); !errors.Is(err, ErrInvalidRawPath) {
		t.Errorf("OpenRaw(nil) expected ErrInvalidRawPath, got %v", err)
	}
	if _, err := OpenRaw([]byte("/tmp/\x00bad")); !errors.Is(err, ErrInvalidRawPath) {
		t.Errorf("OpenRaw(nul) expected ErrInvalidRawPath, got %v", err)
	}
}

func TestPOSIXFileHelpers_NFCAndNFDSeparateDirectories(t *testing.T) {
	base := t.TempDir()
	// Separate root directories prevent APFS normalization equivalence from collapsing
	// NFC and NFD spellings into a single directory entry.
	nfcRoot := []byte(filepath.Join(base, "nfc-root"))
	nfdRoot := []byte(filepath.Join(base, "nfd-root"))

	nfcRel := []byte("Series/\u304c.cbz")       // が (NFC)
	nfdRel := []byte("Series/\u304b\u3099.cbz") // か + U+3099 (NFD)
	if bytes.Equal(nfcRel, nfdRel) {
		t.Fatal("test setup error: NFC and NFD relative bytes must differ")
	}

	nfcID, err := domain.NewRelativePathIdentity(nfcRel)
	if err != nil {
		t.Fatalf("NewRelativePathIdentity(nfc) failed: %v", err)
	}
	nfdID, err := domain.NewRelativePathIdentity(nfdRel)
	if err != nil {
		t.Fatalf("NewRelativePathIdentity(nfd) failed: %v", err)
	}
	if nfcID.Match != nfdID.Match {
		t.Fatalf("expected equal NFC match keys, got %q vs %q", nfcID.Match, nfdID.Match)
	}

	nfcPayload := []byte("nfc-cbz-bytes")
	nfdPayload := []byte("nfd-cbz-bytes")

	writeFixtureFile(t, nfcRoot, nfcRel, nfcPayload)
	writeFixtureFile(t, nfdRoot, nfdRel, nfdPayload)

	assertReadAndOpen(t, nfcRoot, nfcRel, nfcPayload)
	assertReadAndOpen(t, nfdRoot, nfdRel, nfdPayload)

	// Verify directory entries on disk retain the exact raw bytes we wrote in each root.
	assertDirHasExactEntry(t, append(append([]byte{}, nfcRoot...), []byte("/Series")...), []byte("\u304c.cbz"))
	assertDirHasExactEntry(t, append(append([]byte{}, nfdRoot...), []byte("/Series")...), []byte("\u304b\u3099.cbz"))
}

func TestPOSIXFileHelpers_InvalidUTF8OnDisk(t *testing.T) {
	root := []byte(t.TempDir())
	rel := []byte{0xff, 0xfe, '/', 'b', 'o', 'o', 'k', '.', 'c', 'b', 'z'}

	id, err := domain.NewRelativePathIdentity(rel)
	if err != nil {
		t.Fatalf("NewRelativePathIdentity failed: %v", err)
	}
	if !id.InvalidUTF8 || id.Display != `\xff\xfe/book.cbz` {
		t.Fatalf("unexpected identity for invalid UTF-8 path: %+v", id)
	}

	dirRaw, err := JoinRootRelative(root, []byte{0xff, 0xfe})
	if err != nil {
		t.Fatalf("JoinRootRelative(dir) failed: %v", err)
	}
	if err := os.Mkdir(string(dirRaw), 0o755); err != nil {
		if runtime.GOOS != "linux" && isInvalidFilenameEncodingErr(err) {
			t.Skipf("filesystem rejected invalid UTF-8 directory creation (%v); byte-level tests ran", err)
		}
		t.Fatalf("failed to create invalid UTF-8 directory on %s: %v", runtime.GOOS, err)
	}

	fileRaw, err := JoinRootRelative(root, rel)
	if err != nil {
		t.Fatalf("JoinRootRelative(file) failed: %v", err)
	}
	payload := []byte("invalid-utf8-cbz-bytes")
	if err := os.WriteFile(string(fileRaw), payload, 0o600); err != nil {
		if runtime.GOOS != "linux" && isInvalidFilenameEncodingErr(err) {
			t.Skipf("filesystem rejected invalid UTF-8 file creation (%v); byte-level tests ran", err)
		}
		t.Fatalf("failed to create invalid UTF-8 file on %s: %v", runtime.GOOS, err)
	}

	assertDirHasExactEntry(t, root, []byte{0xff, 0xfe})
	assertReadAndOpen(t, root, rel, payload)
}

func TestPOSIXFileHelpers_ValidPunctuationOnDisk(t *testing.T) {
	root := []byte(t.TempDir())
	rel := []byte(`dir/..\a:sample.cbz`)
	payload := []byte("punctuation-cbz-bytes")
	writeFixtureFile(t, root, rel, payload)
	assertReadAndOpen(t, root, rel, payload)
}

func isInvalidFilenameEncodingErr(err error) bool {
	return errors.Is(err, syscall.EILSEQ) || errors.Is(err, syscall.EINVAL)
}

func writeFixtureFile(t *testing.T, root, rel, content []byte) {
	t.Helper()
	joined, err := JoinRootRelative(root, rel)
	if err != nil {
		t.Fatalf("JoinRootRelative failed: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(string(joined)), 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(string(joined), content, 0o600); err != nil {
		t.Fatalf("WriteFile(%x) failed: %v", joined, err)
	}
}

func assertReadAndOpen(t *testing.T, root, rel, want []byte) {
	t.Helper()

	info, err := StatRootRelative(root, rel)
	if err != nil {
		t.Fatalf("StatRootRelative(%x) failed: %v", rel, err)
	}
	if info.Size() != int64(len(want)) {
		t.Fatalf("StatRootRelative size = %d, want %d", info.Size(), len(want))
	}

	linfo, err := LstatRootRelative(root, rel)
	if err != nil {
		t.Fatalf("LstatRootRelative(%x) failed: %v", rel, err)
	}
	if linfo.Size() != int64(len(want)) {
		t.Fatalf("LstatRootRelative size = %d, want %d", linfo.Size(), len(want))
	}

	gotRead, err := ReadRootRelative(root, rel)
	if err != nil {
		t.Fatalf("ReadRootRelative(%x) failed: %v", rel, err)
	}
	if !bytes.Equal(gotRead, want) {
		t.Fatalf("ReadRootRelative got %q, want %q", gotRead, want)
	}

	f, err := OpenRootRelative(root, rel)
	if err != nil {
		t.Fatalf("OpenRootRelative(%x) failed: %v", rel, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	buf := make([]byte, len(want))
	n, err := f.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != len(want) || !bytes.Equal(buf, want) {
		t.Fatalf("ReadAt got (%d, %q), want (%d, %q)", n, buf, len(want), want)
	}

	joined, err := JoinRootRelative(root, rel)
	if err != nil {
		t.Fatalf("JoinRootRelative failed: %v", err)
	}
	rawFile, err := OpenRaw(joined)
	if err != nil {
		t.Fatalf("OpenRaw(%x) failed: %v", joined, err)
	}
	_ = rawFile.Close()
}

func assertDirHasExactEntry(t *testing.T, dirBytes, wantName []byte) {
	t.Helper()
	entries, err := os.ReadDir(string(dirBytes))
	if err != nil {
		t.Fatalf("ReadDir(%x) failed: %v", dirBytes, err)
	}
	for _, entry := range entries {
		if bytes.Equal([]byte(entry.Name()), wantName) {
			return
		}
	}
	t.Fatalf("directory %x missing expected raw entry %x", dirBytes, wantName)
}
