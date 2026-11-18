package scanner_test

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/inexcession/tobiten/internal/container/cbz"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/zeebo/xxh3"
)

type spyReaderAt struct {
	data []byte
	mu   sync.Mutex
	reqs [][2]int64
}

func (s *spyReaderAt) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	s.reqs = append(s.reqs, [2]int64{off, int64(len(p))})
	s.mu.Unlock()
	return bytes.NewReader(s.data).ReadAt(p, off)
}

func (s *spyReaderAt) assertRanges(t *testing.T, allowed [][2]int64) {
	t.Helper()
	if len(s.reqs) == 0 {
		t.Fatal("expected at least one ReadAt call")
	}
	for _, r := range s.reqs {
		start, end := r[0], r[0]+r[1]
		ok := false
		for _, a := range allowed {
			if start >= a[0] && end <= a[1] {
				ok = true
				break
			}
		}
		if !ok {
			t.Errorf("ReadAt [%d, %d) outside allowed ranges %v", start, end, allowed)
		}
	}
}

type failingReaderAt struct {
	data    []byte
	failAt  int64
	failErr error
}

func (f *failingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off <= f.failAt && f.failAt < off+int64(len(p)) {
		return 0, f.failErr
	}
	return bytes.NewReader(f.data).ReadAt(p, off)
}

type zipEntry struct {
	name   string
	data   []byte
	method uint16
}

func buildDeterministicZip(t testing.TB, comment string, entries ...zipEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, e := range entries {
		fh := &zip.FileHeader{
			Name:   e.name,
			Method: e.method,
		}
		w, err := zw.CreateHeader(fh)
		if err != nil {
			t.Fatalf("CreateHeader(%q): %v", e.name, err)
		}
		if _, err := w.Write(e.data); err != nil {
			t.Fatalf("Write(%q): %v", e.name, err)
		}
	}
	if comment != "" {
		if err := zw.SetComment(comment); err != nil {
			t.Fatalf("SetComment: %v", err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

func deterministicPattern(n int, seed byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((i*31 + int(seed)) & 0xff)
	}
	return out
}

func TestPartialFingerprint_DeterministicAndHexFormat(t *testing.T) {
	hexPattern := regexp.MustCompile(`^[0-9a-f]{16}$`)
	data := buildDeterministicZip(t, "comment",
		zipEntry{name: "001.jpg", data: deterministicPattern(16*1024, 1), method: zip.Store},
		zipEntry{name: "002.jpg", data: deterministicPattern(8*1024, 2), method: zip.Deflate},
	)

	first, err := scanner.PartialFingerprint(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("PartialFingerprint: %v", err)
	}
	if !hexPattern.MatchString(first) {
		t.Fatalf("digest %q does not match 16 lowercase hex chars", first)
	}

	second, err := scanner.PartialFingerprint(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("PartialFingerprint second call: %v", err)
	}
	if first != second {
		t.Fatalf("non-deterministic digest: %q != %q", first, second)
	}
}

func TestPartialFingerprint_SensitiveRegionsAndEntryPayloadInvariance(t *testing.T) {
	// Store a 32 KB first entry so [0, 4096) falls inside entry 1, and [4096, dir.Offset)
	// has plenty of entry payload outside both the 4 KB head and the central directory.
	data := buildDeterministicZip(t, "",
		zipEntry{name: "001.jpg", data: deterministicPattern(32*1024, 7), method: zip.Store},
		zipEntry{name: "002.jpg", data: deterministicPattern(8*1024, 9), method: zip.Store},
	)
	dir, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("LocateDirectory: %v", err)
	}
	if dir.Offset <= scanner.PartialSegmentBytes {
		t.Fatalf("expected central directory offset > %d, got %d", scanner.PartialSegmentBytes, dir.Offset)
	}

	base, err := scanner.PartialFingerprint(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("PartialFingerprint(base): %v", err)
	}

	// Changing any byte in the first 4 KB must change the fingerprint.
	for off := 0; off < scanner.PartialSegmentBytes; off++ {
		mut := append([]byte(nil), data...)
		mut[off] ^= 0x01
		got, err := scanner.PartialFingerprint(bytes.NewReader(mut), int64(len(mut)))
		if err != nil {
			t.Fatalf("head byte %d: %v", off, err)
		}
		if got == base {
			t.Fatalf("head mutation at offset %d did not change fingerprint %s", off, base)
		}
	}

	// Changing any byte in the located ZIP central directory must change the fingerprint.
	for off := dir.Offset; off < dir.Offset+dir.Size; off++ {
		mut := append([]byte(nil), data...)
		mut[off] ^= 0x01
		got, err := scanner.PartialFingerprint(bytes.NewReader(mut), int64(len(mut)))
		if err != nil {
			t.Fatalf("central directory byte %d: %v", off, err)
		}
		if got == base {
			t.Fatalf("central directory mutation at offset %d did not change fingerprint %s", off, base)
		}
	}

	// Changing entry data strictly between the 4 KB head and central directory must NOT change it.
	for _, off := range []int64{scanner.PartialSegmentBytes, (scanner.PartialSegmentBytes + dir.Offset) / 2, dir.Offset - 1} {
		mut := append([]byte(nil), data...)
		mut[off] ^= 0xff
		got, err := scanner.PartialFingerprint(bytes.NewReader(mut), int64(len(mut)))
		if err != nil {
			t.Fatalf("entry payload byte %d: %v", off, err)
		}
		if got != base {
			t.Fatalf("entry payload mutation at offset %d changed fingerprint: got %s, want %s", off, got, base)
		}
	}
}

func TestPartialFingerprint_SizeSensitivity(t *testing.T) {
	// Identical all-zero head and tail bytes, differing only in size.
	a := make([]byte, 16*1024)
	b := make([]byte, 16*1024+1)

	fpA, err := scanner.PartialFingerprint(bytes.NewReader(a), int64(len(a)))
	if err != nil {
		t.Fatal(err)
	}
	fpB, err := scanner.PartialFingerprint(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	if fpA == fpB {
		t.Fatalf("size change produced identical fingerprint %s", fpA)
	}
}

func TestPartialFingerprint_TrailingFallbackForNonZIPAndMissingEOCD(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{
			name: "non-zip rar-like payload",
			data: append([]byte("Rar!\x1a\x07\x01\x00"), deterministicPattern(16*1024, 3)...),
		},
		{
			name: "zip magic without EOCD",
			data: append([]byte("PK\x03\x04"), deterministicPattern(16*1024, 5)...),
		},
		{
			name: "zip with corrupted EOCD signature",
			data: func() []byte {
				z := buildDeterministicZip(t, "",
					zipEntry{name: "001.jpg", data: deterministicPattern(16*1024, 11), method: zip.Store},
				)
				// Corrupt the EOCD signature PK\x05\x06 at len(z)-22.
				z[len(z)-22] = 'X'
				return z
			}(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			size := int64(len(tc.data))
			base, err := scanner.PartialFingerprint(bytes.NewReader(tc.data), size)
			if err != nil {
				t.Fatalf("PartialFingerprint: %v", err)
			}

			// Mutating middle bytes outside [0, 4096) and [size-4096, size) leaves digest unchanged.
			mid := append([]byte(nil), tc.data...)
			mid[scanner.PartialSegmentBytes] ^= 0xff
			mid[size-scanner.PartialSegmentBytes-1] ^= 0xff
			gotMid, err := scanner.PartialFingerprint(bytes.NewReader(mid), size)
			if err != nil {
				t.Fatal(err)
			}
			if gotMid != base {
				t.Fatalf("middle mutation changed fallback fingerprint: got %s, want %s", gotMid, base)
			}

			// Mutating first byte of head or first/last byte of trailing 4 KB changes digest.
			for _, off := range []int64{0, scanner.PartialSegmentBytes - 1, size - scanner.PartialSegmentBytes, size - 1} {
				mut := append([]byte(nil), tc.data...)
				mut[off] ^= 0x01
				got, err := scanner.PartialFingerprint(bytes.NewReader(mut), size)
				if err != nil {
					t.Fatal(err)
				}
				if got == base {
					t.Fatalf("mutation at offset %d did not change fallback fingerprint %s", off, base)
				}
			}
		})
	}
}

func TestPartialFingerprint_ShortFilesAndBoundaries(t *testing.T) {
	inputs := [][]byte{
		{},
		{0x00},
		[]byte("short-non-zip"),
		deterministicPattern(scanner.PartialSegmentBytes-1, 1),
		deterministicPattern(scanner.PartialSegmentBytes, 2),
		deterministicPattern(scanner.PartialSegmentBytes+1, 3),
		buildDeterministicZip(t, ""), // 22-byte empty ZIP
		buildDeterministicZip(t, "", zipEntry{name: "1.jpg", data: []byte("tiny"), method: zip.Store}),
		append([]byte("PK\x03\x04"), deterministicPattern(128, 4)...), // short corrupt ZIP
	}

	seen := make(map[string]int, len(inputs))
	for i, data := range inputs {
		got, err := scanner.PartialFingerprint(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("case %d (len=%d): %v", i, len(data), err)
		}
		if prev, dup := seen[got]; dup {
			t.Fatalf("case %d (len=%d) collided with case %d: %s", i, len(data), prev, got)
		}
		seen[got] = i
	}
}

func TestPartialFingerprint_TrailingArchiveComments(t *testing.T) {
	// Use a 5000-byte comment so the entire trailing 4 KB lies inside the comment,
	// forcing the fingerprinter to hash the located central directory instead.
	comment := strings.Repeat("C", 5000)
	data := buildDeterministicZip(t, comment,
		zipEntry{name: "001.jpg", data: deterministicPattern(16*1024, 13), method: zip.Store},
	)
	dir, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("LocateDirectory: %v", err)
	}
	if dir.Offset+dir.Size >= int64(len(data))-scanner.PartialSegmentBytes {
		t.Fatalf("expected central directory before trailing 4 KB; dir=%+v len=%d", dir, len(data))
	}

	base, err := scanner.PartialFingerprint(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// Mutating the central directory must change the fingerprint even though it is
	// outside both the first 4 KB and the trailing 4 KB.
	mutCD := append([]byte(nil), data...)
	mutCD[dir.Offset] ^= 0x01
	gotCD, err := scanner.PartialFingerprint(bytes.NewReader(mutCD), int64(len(mutCD)))
	if err != nil {
		t.Fatal(err)
	}
	if gotCD == base {
		t.Fatalf("central directory mutation with long comment did not change fingerprint %s", base)
	}

	// Also verify maximum 65535-byte ZIP comment works.
	maxCommentData := buildDeterministicZip(t, strings.Repeat("M", 65535),
		zipEntry{name: "001.jpg", data: deterministicPattern(8*1024, 17), method: zip.Store},
	)
	maxDir, err := cbz.LocateDirectory(bytes.NewReader(maxCommentData), int64(len(maxCommentData)))
	if err != nil {
		t.Fatalf("LocateDirectory(65535 comment): %v", err)
	}
	maxBase, err := scanner.PartialFingerprint(bytes.NewReader(maxCommentData), int64(len(maxCommentData)))
	if err != nil {
		t.Fatal(err)
	}
	mutMaxCD := append([]byte(nil), maxCommentData...)
	mutMaxCD[maxDir.Offset+maxDir.Size-1] ^= 0x01
	gotMaxCD, err := scanner.PartialFingerprint(bytes.NewReader(mutMaxCD), int64(len(mutMaxCD)))
	if err != nil {
		t.Fatal(err)
	}
	if gotMaxCD == maxBase {
		t.Fatalf("central directory mutation with 65535-byte comment did not change fingerprint %s", maxBase)
	}
}

func TestPartialFingerprint_ZIP64AndReadBounds(t *testing.T) {
	// More than 65535 entries triggers ZIP64 end records and a >64 KB central directory,
	// exercising both ZIP64 metadata location and chunked central-directory hashing.
	const entryCount = 70000
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w0, err := zw.CreateHeader(&zip.FileHeader{Name: "000000.jpg", Method: zip.Store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w0.Write(deterministicPattern(16*1024, 21)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < entryCount; i++ {
		if _, err := zw.CreateHeader(&zip.FileHeader{Name: fmt.Sprintf("%06d.jpg", i), Method: zip.Store}); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.SetComment("zip64-comment"); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	size := int64(len(data))

	dir, err := cbz.LocateDirectory(bytes.NewReader(data), size)
	if err != nil {
		t.Fatalf("LocateDirectory: %v", err)
	}
	if !dir.Zip64 {
		t.Fatalf("expected Zip64=true, got %+v", dir)
	}

	spy := &spyReaderAt{data: data}
	base, err := scanner.PartialFingerprint(spy, size)
	if err != nil {
		t.Fatalf("PartialFingerprint(ZIP64): %v", err)
	}

	const tailSearchWindow = 65 * 1024
	spy.assertRanges(t, [][2]int64{
		{0, scanner.PartialSegmentBytes},
		{max(0, size-tailSearchWindow), size},
		{dir.EndOffset, dir.EndOffset + 56},
		{dir.Offset, dir.Offset + dir.Size},
	})

	// Entry 0's payload past 4 KB is outside head and central directory.
	mutPayload := append([]byte(nil), data...)
	mutPayload[8*1024] ^= 0xff
	gotPayload, err := scanner.PartialFingerprint(bytes.NewReader(mutPayload), size)
	if err != nil {
		t.Fatal(err)
	}
	if gotPayload != base {
		t.Fatalf("ZIP64 entry payload mutation changed fingerprint: got %s, want %s", gotPayload, base)
	}

	// Mutating a byte in the ZIP64 central directory must change the fingerprint.
	mutCD := append([]byte(nil), data...)
	mutCD[dir.Offset+dir.Size/2] ^= 0x01
	gotCD, err := scanner.PartialFingerprint(bytes.NewReader(mutCD), size)
	if err != nil {
		t.Fatal(err)
	}
	if gotCD == base {
		t.Fatalf("ZIP64 central directory mutation did not change fingerprint %s", base)
	}
}

func TestPartialFingerprint_ConfinesReads(t *testing.T) {
	t.Run("large plain ZIP", func(t *testing.T) {
		data := buildDeterministicZip(t, "tail",
			zipEntry{name: "1.jpg", data: deterministicPattern(1<<20, 1), method: zip.Store},
			zipEntry{name: "2.jpg", data: deterministicPattern(1<<20, 2), method: zip.Store},
		)
		size := int64(len(data))
		dir, err := cbz.LocateDirectory(bytes.NewReader(data), size)
		if err != nil {
			t.Fatal(err)
		}

		spy := &spyReaderAt{data: data}
		if _, err := scanner.PartialFingerprint(spy, size); err != nil {
			t.Fatal(err)
		}
		spy.assertRanges(t, [][2]int64{
			{0, scanner.PartialSegmentBytes},
			{size - 65*1024, size},
			{dir.Offset, dir.Offset + dir.Size},
		})
	})

	t.Run("large non-ZIP", func(t *testing.T) {
		data := deterministicPattern(1<<20, 9)
		size := int64(len(data))
		spy := &spyReaderAt{data: data}
		if _, err := scanner.PartialFingerprint(spy, size); err != nil {
			t.Fatal(err)
		}
		spy.assertRanges(t, [][2]int64{
			{0, scanner.PartialSegmentBytes},
			{size - 65*1024, size},
		})
	})

	t.Run("large ZIP without EOCD", func(t *testing.T) {
		data := append([]byte("PK\x03\x04"), deterministicPattern(1<<20, 11)...)
		size := int64(len(data))
		spy := &spyReaderAt{data: data}
		if _, err := scanner.PartialFingerprint(spy, size); err != nil {
			t.Fatal(err)
		}
		spy.assertRanges(t, [][2]int64{
			{0, scanner.PartialSegmentBytes},
			{size - 65*1024, size},
		})
	})

	t.Run("short file <= 4 KB", func(t *testing.T) {
		data := buildDeterministicZip(t, "", zipEntry{name: "1.jpg", data: []byte("abc"), method: zip.Store})
		size := int64(len(data))
		spy := &spyReaderAt{data: data}
		if _, err := scanner.PartialFingerprint(spy, size); err != nil {
			t.Fatal(err)
		}
		spy.assertRanges(t, [][2]int64{{0, size}})
	})

	t.Run("central directory straddling 4 KB boundary", func(t *testing.T) {
		// Entry payload puts dir.Offset < 4096 while dir.Offset + dir.Size > 4096.
		data := buildDeterministicZip(t, "",
			zipEntry{name: "1.jpg", data: deterministicPattern(3950, 15), method: zip.Store},
			zipEntry{name: "2.jpg", data: []byte("tail"), method: zip.Store},
		)
		size := int64(len(data))
		dir, err := cbz.LocateDirectory(bytes.NewReader(data), size)
		if err != nil {
			t.Fatal(err)
		}
		if dir.Offset >= scanner.PartialSegmentBytes || dir.Offset+dir.Size <= scanner.PartialSegmentBytes {
			t.Fatalf("expected central directory to straddle 4096, got dir=%+v", dir)
		}
		spy := &spyReaderAt{data: data}
		if _, err := scanner.PartialFingerprint(spy, size); err != nil {
			t.Fatal(err)
		}
		spy.assertRanges(t, [][2]int64{
			{0, scanner.PartialSegmentBytes},
			{max(0, size-65*1024), size},
			{dir.Offset, dir.Offset + dir.Size},
		})
	})
}

type failOnZip64EndReaderAt struct {
	data         []byte
	z64EndOffset int64
	err          error
}

func (f *failOnZip64EndReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off == f.z64EndOffset && len(p) == 56 {
		return 0, f.err
	}
	return bytes.NewReader(f.data).ReadAt(p, off)
}

func TestPartialFingerprint_PrefixedReadableZIP(t *testing.T) {
	// Non-ZIP preamble (8 KB) + zip.Writer with SetOffset + 5000-byte trailing comment
	// so head[0:4096] has no ZIP magic and the central directory precedes the trailing 4 KB.
	preamble := bytes.Repeat([]byte("PREAMBLE"), 1024)
	var buf bytes.Buffer
	buf.Write(preamble)
	zw := zip.NewWriter(&buf)
	zw.SetOffset(int64(len(preamble)))
	w, err := zw.CreateHeader(&zip.FileHeader{Name: "001.jpg", Method: zip.Store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(deterministicPattern(16*1024, 29)); err != nil {
		t.Fatal(err)
	}
	if err := zw.SetComment(strings.Repeat("C", 5000)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	size := int64(len(data))

	r, err := cbz.NewReader(bytes.NewReader(data), size)
	if err != nil {
		t.Fatalf("cbz.NewReader failed on prefixed ZIP: %v", err)
	}
	dir := r.Directory()
	if dir.Offset+dir.Size >= size-scanner.PartialSegmentBytes {
		t.Fatalf("expected central directory before trailing 4 KB, got dir=%+v size=%d", dir, size)
	}

	spy := &spyReaderAt{data: data}
	base, err := scanner.PartialFingerprint(spy, size)
	if err != nil {
		t.Fatalf("PartialFingerprint: %v", err)
	}
	if want := referenceLayoutHex(t, data, true); base != want {
		t.Fatalf("prefixed ZIP did not use central directory: got %s, want %s", base, want)
	}
	spy.assertRanges(t, [][2]int64{
		{0, scanner.PartialSegmentBytes},
		{max(0, size-65*1024), size},
		{dir.Offset, dir.Offset + dir.Size},
	})

	// Mutating the central directory (which precedes the trailing 4 KB) must change the fingerprint.
	mutCD := append([]byte(nil), data...)
	mutCD[dir.Offset] ^= 0x01
	gotCD, err := scanner.PartialFingerprint(bytes.NewReader(mutCD), size)
	if err != nil {
		t.Fatal(err)
	}
	if gotCD == base {
		t.Fatalf("central directory mutation on prefixed ZIP did not change fingerprint %s", base)
	}

	// Mutating entry payload between preamble/local-header and central directory must NOT change it.
	payloadOff := int64(len(preamble)) + 1024
	mutPayload := append([]byte(nil), data...)
	mutPayload[payloadOff] ^= 0xff
	gotPayload, err := scanner.PartialFingerprint(bytes.NewReader(mutPayload), size)
	if err != nil {
		t.Fatal(err)
	}
	if gotPayload != base {
		t.Fatalf("prefixed ZIP payload mutation changed fingerprint: got %s, want %s", gotPayload, base)
	}

	// Ambiguous unadjusted prepended ZIP (without SetOffset) remains rejected and falls back cleanly.
	unadjusted := append(bytes.Repeat([]byte("#"), 64), buildDeterministicZip(t, strings.Repeat("C", 5000),
		zipEntry{name: "001.jpg", data: deterministicPattern(16*1024, 31), method: zip.Store},
	)...)
	unadjSize := int64(len(unadjusted))
	if _, err := cbz.LocateDirectory(bytes.NewReader(unadjusted), unadjSize); err == nil {
		t.Fatal("expected ambiguous unadjusted prepended ZIP to fail LocateDirectory")
	}
	unadjSpy := &spyReaderAt{data: unadjusted}
	gotUnadj, err := scanner.PartialFingerprint(unadjSpy, unadjSize)
	if err != nil {
		t.Fatal(err)
	}
	if wantFallback := referenceLayoutHex(t, unadjusted, false); gotUnadj != wantFallback {
		t.Fatalf("ambiguous prepended ZIP did not fall back: got %s, want %s", gotUnadj, wantFallback)
	}
	unadjSpy.assertRanges(t, [][2]int64{
		{0, scanner.PartialSegmentBytes},
		{max(0, unadjSize-65*1024), unadjSize},
	})
}

func TestPartialFingerprint_Errors(t *testing.T) {
	if _, err := scanner.PartialFingerprint(nil, 0); !errors.Is(err, scanner.ErrNilReader) {
		t.Fatalf("nil reader error = %v, want ErrNilReader", err)
	}
	if _, err := scanner.PartialFingerprint(bytes.NewReader(nil), -1); !errors.Is(err, scanner.ErrNegativeSize) {
		t.Fatalf("negative size error = %v, want ErrNegativeSize", err)
	}

	ioErr := errors.New("simulated disk I/O error")
	zipData := buildDeterministicZip(t, "",
		zipEntry{name: "1.jpg", data: deterministicPattern(16*1024, 3), method: zip.Store},
	)
	// Underlying ReadAt failures in the tail search must propagate rather than falling back.
	tailFail := &failingReaderAt{data: zipData, failAt: int64(len(zipData)) - 10, failErr: ioErr}
	if _, err := scanner.PartialFingerprint(tailFail, int64(len(zipData))); !errors.Is(err, ioErr) {
		t.Fatalf("tail I/O error = %v, want %v", err, ioErr)
	}

	// Underlying ReadAt failures at the ZIP64 end record must propagate rather than falling back.
	z64Data := buildMinimalDeterministicZIP64(t)
	z64Dir, err := cbz.LocateDirectory(bytes.NewReader(z64Data), int64(len(z64Data)))
	if err != nil {
		t.Fatal(err)
	}
	z64Fail := &failOnZip64EndReaderAt{
		data:         z64Data,
		z64EndOffset: z64Dir.EndOffset,
		err:          ioErr,
	}
	if _, err := scanner.PartialFingerprint(z64Fail, int64(len(z64Data))); !errors.Is(err, ioErr) {
		t.Fatalf("zip64 end-record I/O error = %v, want %v", err, ioErr)
	}

	// Out-of-bounds ZIP64 end-record offset must fall back to trailing 4 KB without out-of-bounds reads.
	badZ64 := append([]byte(nil), z64Data...)
	locOffset := len(badZ64) - 22 - len("z64") - 20
	binary.LittleEndian.PutUint64(badZ64[locOffset+8:locOffset+16], uint64(len(badZ64))+1024)
	badSpy := &spyReaderAt{data: badZ64}
	gotBadZ64, err := scanner.PartialFingerprint(badSpy, int64(len(badZ64)))
	if err != nil {
		t.Fatalf("out-of-bounds zip64 fallback failed: %v", err)
	}
	if want := referenceLayoutHex(t, badZ64, false); gotBadZ64 != want {
		t.Fatalf("out-of-bounds zip64 got %s, want fallback %s", gotBadZ64, want)
	}
	badSpy.assertRanges(t, [][2]int64{
		{0, scanner.PartialSegmentBytes},
		{max(0, int64(len(badZ64))-65*1024), int64(len(badZ64))},
	})

	// Short ReadAt returning EOF before size must fail.
	if _, err := scanner.PartialFingerprint(bytes.NewReader([]byte("short")), 100); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short reader error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func buildMinimalDeterministicZIP64(t testing.TB) []byte {
	t.Helper()
	// Build a deterministic ZIP64 fixture directly with one Store entry and
	// explicit ZIP64 end-of-central-directory + locator records (no wall-clock fields).
	payload := deterministicPattern(9000, 33)
	name := []byte("page1.jpg")

	var buf bytes.Buffer
	// Local file header (30 bytes) + name + payload.
	lhOffset := uint64(buf.Len())
	var lh [30]byte
	binary.LittleEndian.PutUint32(lh[0:4], 0x04034b50)
	binary.LittleEndian.PutUint16(lh[4:6], 45) // version needed (ZIP64)
	binary.LittleEndian.PutUint32(lh[18:22], uint32(len(payload)))
	binary.LittleEndian.PutUint32(lh[22:26], uint32(len(payload)))
	binary.LittleEndian.PutUint16(lh[26:28], uint16(len(name)))
	buf.Write(lh[:])
	buf.Write(name)
	buf.Write(payload)

	// Central directory header (46 bytes) + name.
	cdOffset := uint64(buf.Len())
	var cd [46]byte
	binary.LittleEndian.PutUint32(cd[0:4], 0x02014b50)
	binary.LittleEndian.PutUint16(cd[4:6], 45)
	binary.LittleEndian.PutUint16(cd[6:8], 45)
	binary.LittleEndian.PutUint32(cd[20:24], uint32(len(payload)))
	binary.LittleEndian.PutUint32(cd[24:28], uint32(len(payload)))
	binary.LittleEndian.PutUint16(cd[28:30], uint16(len(name)))
	binary.LittleEndian.PutUint32(cd[42:46], uint32(lhOffset))
	buf.Write(cd[:])
	buf.Write(name)
	cdSize := uint64(buf.Len()) - cdOffset

	// ZIP64 end of central directory record (56 bytes).
	z64EndOffset := uint64(buf.Len())
	var z64End [56]byte
	binary.LittleEndian.PutUint32(z64End[0:4], 0x06064b50)
	binary.LittleEndian.PutUint64(z64End[4:12], 44) // remaining bytes
	binary.LittleEndian.PutUint16(z64End[12:14], 45)
	binary.LittleEndian.PutUint16(z64End[14:16], 45)
	binary.LittleEndian.PutUint64(z64End[24:32], 1)
	binary.LittleEndian.PutUint64(z64End[32:40], 1)
	binary.LittleEndian.PutUint64(z64End[40:48], cdSize)
	binary.LittleEndian.PutUint64(z64End[48:56], cdOffset)
	buf.Write(z64End[:])

	// ZIP64 end of central directory locator (20 bytes).
	var z64Loc [20]byte
	binary.LittleEndian.PutUint32(z64Loc[0:4], 0x07064b50)
	binary.LittleEndian.PutUint64(z64Loc[8:16], z64EndOffset)
	binary.LittleEndian.PutUint32(z64Loc[16:20], 1)
	buf.Write(z64Loc[:])

	// Classic end of central directory record (22 bytes) with 0xffff / 0xffffffff sentinels.
	comment := []byte("z64")
	var eocd [22]byte
	binary.LittleEndian.PutUint32(eocd[0:4], 0x06054b50)
	binary.LittleEndian.PutUint16(eocd[8:10], 0xffff)
	binary.LittleEndian.PutUint16(eocd[10:12], 0xffff)
	binary.LittleEndian.PutUint32(eocd[12:16], 0xffffffff)
	binary.LittleEndian.PutUint32(eocd[16:20], 0xffffffff)
	binary.LittleEndian.PutUint16(eocd[20:22], uint16(len(comment)))
	buf.Write(eocd[:])
	buf.Write(comment)

	data := buf.Bytes()
	dir, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data)))
	if err != nil || !dir.Zip64 {
		t.Fatalf("minimal ZIP64 fixture invalid: dir=%+v err=%v", dir, err)
	}
	return data
}

func referenceLayoutHex(t *testing.T, data []byte, zipCD bool) string {
	t.Helper()
	var wire bytes.Buffer
	wire.WriteString("tobiten:xxh3_64_partial:v1\x00")
	var u64 [8]byte
	binary.LittleEndian.PutUint64(u64[:], uint64(len(data)))
	wire.Write(u64[:])
	headLen := min(len(data), scanner.PartialSegmentBytes)
	binary.LittleEndian.PutUint64(u64[:], uint64(headLen))
	wire.Write(u64[:])
	wire.Write(data[:headLen])

	if zipCD {
		dir, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("LocateDirectory: %v", err)
		}
		wire.WriteByte(0x01)
		binary.LittleEndian.PutUint64(u64[:], uint64(dir.Size))
		wire.Write(u64[:])
		wire.Write(data[dir.Offset : dir.Offset+dir.Size])
	} else {
		tailLen := min(len(data), scanner.PartialSegmentBytes)
		wire.WriteByte(0x02)
		binary.LittleEndian.PutUint64(u64[:], uint64(tailLen))
		wire.Write(u64[:])
		wire.Write(data[len(data)-tailLen:])
	}
	return fmt.Sprintf("%016x", xxh3.Hash(wire.Bytes()))
}

func TestPartialFingerprint_GoldenVectors(t *testing.T) {
	// These vectors pin the persisted XXH3_64_PARTIAL hex format and byte layout.
	// Changing any vector here breaks backwards compatibility with stored asset_digests.
	vectors := []struct {
		name  string
		data  []byte
		zipCD bool
		want  string
	}{
		{
			name:  "empty",
			data:  nil,
			zipCD: false,
			want:  "bd149fcc14061f0b",
		},
		{
			name:  "short non-zip",
			data:  []byte("tobiten-golden-short"),
			zipCD: false,
			want:  "ad1aa0e43f0dce6b",
		},
		{
			name:  "large non-zip",
			data:  deterministicPattern(12*1024, 42),
			zipCD: false,
			want:  "51684a9219d31149",
		},
		{
			name:  "short valid zip",
			data:  buildDeterministicZip(t, "", zipEntry{name: "1.jpg", data: []byte("img"), method: zip.Store}),
			zipCD: true,
			want:  "a61978edaf7e6397",
		},
		{
			name: "large valid zip with comment",
			data: buildDeterministicZip(t, "tobiten-comment",
				zipEntry{name: "001.jpg", data: deterministicPattern(10*1024, 7), method: zip.Store},
				zipEntry{name: "002.png", data: deterministicPattern(6*1024, 8), method: zip.Deflate},
			),
			zipCD: true,
			want:  "f803dd26ff8d7567",
		},
		{
			name:  "zip magic fallback without EOCD",
			data:  append([]byte("PK\x03\x04"), deterministicPattern(9000, 99)...),
			zipCD: false,
			want:  "beee22e868a719d3",
		},
		{
			name:  "zip64",
			data:  buildMinimalDeterministicZIP64(t),
			zipCD: true,
			want:  "f4210aa5ad4f6141",
		},
	}

	for _, tc := range vectors {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scanner.PartialFingerprint(bytes.NewReader(tc.data), int64(len(tc.data)))
			if err != nil {
				t.Fatalf("PartialFingerprint: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
			if ref := referenceLayoutHex(t, tc.data, tc.zipCD); ref != tc.want {
				t.Errorf("reference wire layout produced %q, want %q", ref, tc.want)
			}
		})
	}
}
