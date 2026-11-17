package cbz_test

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/container/cbz"
)

type file struct {
	name   string
	data   string
	method uint16 // zip.Store or zip.Deflate
}

func store(name, data string) file   { return file{name, data, zip.Store} }
func deflate(name, data string) file { return file{name, data, zip.Deflate} }

func buildZip(t testing.TB, comment string, files ...file) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range files {
		w, err := zw.CreateHeader(&zip.FileHeader{Name: f.name, Method: f.method})
		if err != nil {
			t.Fatalf("CreateHeader(%q): %v", f.name, err)
		}
		if _, err := io.WriteString(w, f.data); err != nil {
			t.Fatal(err)
		}
	}
	if comment != "" {
		if err := zw.SetComment(comment); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func open(t testing.TB, data []byte) *cbz.Reader {
	t.Helper()
	r, err := cbz.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return r
}

func readEntry(t testing.TB, r container.ContainerReader, e container.Entry) string {
	t.Helper()
	rc, err := r.Open(e.Index)
	if err != nil {
		t.Fatalf("Open(%q): %v", e.Name, err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", e.Name, err)
	}
	return string(b)
}

func pageNames(r container.ContainerReader) []string {
	var out []string
	for _, p := range r.Pages() {
		out = append(out, string(p.NameBytes))
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type spyReaderAt struct {
	data []byte
	mu   sync.Mutex
	reqs [][2]int64 // offset, length
}

func (s *spyReaderAt) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	s.reqs = append(s.reqs, [2]int64{off, int64(len(p))})
	s.mu.Unlock()
	return bytes.NewReader(s.data).ReadAt(p, off)
}

func TestReader_StreamsStoreAndDeflateWithoutTempFiles(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	pageA := strings.Repeat("A", 4096)
	pageB := strings.Repeat("page-two-", 2000)
	comic := `<?xml version="1.0"?><ComicInfo><Series>Test</Series></ComicInfo>`
	data := buildZip(t, "",
		store("001.jpg", pageA),
		deflate("002.png", pageB),
		deflate("ComicInfo.xml", comic),
	)

	reg := container.NewFormatRegistry()
	if err := reg.Register(cbz.Format()); err != nil {
		t.Fatal(err)
	}
	r, err := reg.Open(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("registry Open: %v", err)
	}
	defer func() { _ = r.Close() }()

	if r.Format() != cbz.FormatName {
		t.Errorf("Format() = %q", r.Format())
	}
	pages := r.Pages()
	if len(pages) != 2 {
		t.Fatalf("pages = %v", pageNames(r))
	}
	if got := readEntry(t, r, pages[0]); got != pageA {
		t.Errorf("store page differs (%d bytes)", len(got))
	}
	if got := readEntry(t, r, pages[1]); got != pageB {
		t.Errorf("deflate page differs (%d bytes)", len(got))
	}
	if pages[0].Size != int64(len(pageA)) || pages[1].Size != int64(len(pageB)) {
		t.Errorf("page sizes = %d,%d", pages[0].Size, pages[1].Size)
	}

	ci, ok := r.ComicInfo()
	if !ok {
		t.Fatal("ComicInfo.xml not found")
	}
	if got := readEntry(t, r, ci); got != comic {
		t.Errorf("ComicInfo.xml = %q", got)
	}

	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("temporary directory has %d entries after reading, want none", len(entries))
	}
}

func TestReader_ReadsOnlyThroughReaderAt(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	data := buildZip(t, "", store("001.jpg", big), store("002.jpg", "tiny"))
	spy := &spyReaderAt{data: data}
	r, err := cbz.NewReader(spy, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	spy.mu.Lock()
	spy.reqs = nil
	spy.mu.Unlock()

	// Streaming the small page must not touch the large page's bytes.
	if got := readEntry(t, r, r.Pages()[1]); got != "tiny" {
		t.Fatalf("page = %q", got)
	}
	var total int64
	for _, q := range spy.reqs {
		total += q[1]
	}
	if total >= 1<<16 {
		t.Errorf("reading a 4-byte page fetched %d bytes", total)
	}
}

func TestReader_PageOrderingAndFiltering(t *testing.T) {
	data := buildZip(t, "",
		store("10.jpg", "x"), store("2.jpg", "x"), store("1.jpg", "x"),
		store("__MACOSX/1.jpg", "x"), store("__MACOSX/._2.jpg", "x"),
		store("._1.jpg", "x"), store(".DS_Store", "x"), store(".hidden/9.jpg", "x"),
		store("dir/", ""), store("ch10/p1.png", "x"), store("ch2/p1.png", "x"),
		store("Cover.JPG", "x"), store("notes.txt", "x"), store("ComicInfo.xml", "<ComicInfo/>"),
	)
	r := open(t, data)
	want := []string{"1.jpg", "2.jpg", "10.jpg", "ch2/p1.png", "ch10/p1.png", "Cover.JPG"}
	if got := pageNames(r); !equalStrings(got, want) {
		t.Errorf("pages = %v, want %v", got, want)
	}
	if got := len(r.Entries()); got != 14 {
		t.Errorf("Entries() = %d, want all 14 (including filtered ones)", got)
	}
}

func TestReader_ComicInfoLookup(t *testing.T) {
	t.Run("case-insensitive root", func(t *testing.T) {
		r := open(t, buildZip(t, "", store("1.jpg", "x"), store("comicinfo.XML", "<ComicInfo/>")))
		if _, ok := r.ComicInfo(); !ok {
			t.Error("lower-case comicinfo.XML not found")
		}
	})
	t.Run("ignores nested and macOS debris", func(t *testing.T) {
		r := open(t, buildZip(t, "",
			store("1.jpg", "x"),
			store("__MACOSX/ComicInfo.xml", "junk"),
			store("sub/ComicInfo.xml", "nested"),
		))
		if _, ok := r.ComicInfo(); ok {
			t.Error("non-root ComicInfo.xml reported")
		}
	})
	t.Run("absent", func(t *testing.T) {
		r := open(t, buildZip(t, "", store("1.jpg", "x")))
		if _, ok := r.ComicInfo(); ok {
			t.Error("ComicInfo.xml reported on archive without one")
		}
	})
}

func TestReader_ImageCodecDiagnostics(t *testing.T) {
	r := open(t, buildZip(t, "", store("1.jpg", "x"), store("2.avif", "x"), store("3.JXL", "x")))
	d := r.Diagnostics()
	if len(d.ImageCodecs) != 2 || d.ImageCodecs[0] != "jxl" || d.ImageCodecs[1] != "avif" {
		t.Errorf("ImageCodecs = %v, want [jxl avif]", d.ImageCodecs)
	}
	if d.InvalidUTF8Path {
		t.Error("InvalidUTF8Path set for ASCII names")
	}

	plain := open(t, buildZip(t, "", store("1.jpg", "x"), store("2.webp", "x")))
	if got := plain.Diagnostics().ImageCodecs; len(got) != 0 {
		t.Errorf("ImageCodecs = %v, want none", got)
	}

	// Returned diagnostics must be a copy.
	d.ImageCodecs[0] = "mutated"
	if got := r.Diagnostics().ImageCodecs[0]; got != "jxl" {
		t.Errorf("Diagnostics exposes internal state: %q", got)
	}
}

func TestReader_NonUTF8EntryNames(t *testing.T) {
	data := buildZip(t, "",
		store("caf\xe9/02.jpg", "two"),
		store("caf\xe9/01.jpg", "one"),
		store("\xff\xfe.png", "bin"),
	)
	r := open(t, data)
	if !r.Diagnostics().InvalidUTF8Path {
		t.Error("InvalidUTF8Path not recorded")
	}
	entries := r.Entries()
	if len(entries) != 3 {
		t.Fatalf("Entries() = %d", len(entries))
	}
	if !bytes.Equal(entries[0].NameBytes, []byte("caf\xe9/02.jpg")) {
		t.Errorf("raw name altered: %q", entries[0].NameBytes)
	}
	if entries[0].Name != `caf\xe9/02.jpg` {
		t.Errorf("display name = %q, want escaped bytes", entries[0].Name)
	}
	want := []string{"caf\xe9/01.jpg", "caf\xe9/02.jpg", "\xff\xfe.png"}
	if got := pageNames(r); !equalStrings(got, want) {
		t.Errorf("pages = %q, want %q", got, want)
	}
	if got := readEntry(t, r, r.Pages()[0]); got != "one" {
		t.Errorf("page content = %q", got)
	}
}

func TestReader_InsecurePathsAreListedNotRejected(t *testing.T) {
	r := open(t, buildZip(t, "", store("../evil.jpg", "x"), store("/abs.jpg", "y"), store("1.jpg", "z")))
	if got := len(r.Entries()); got != 3 {
		t.Fatalf("Entries() = %d, want 3", got)
	}
	if got := readEntry(t, r, r.Entries()[0]); got != "x" {
		t.Errorf("content = %q", got)
	}
}

func TestReader_EmptyArchive(t *testing.T) {
	data := buildZip(t, "")
	r := open(t, data)
	if len(r.Entries()) != 0 || len(r.Pages()) != 0 {
		t.Error("empty archive has entries")
	}
	if !cbz.Sniff(data) {
		t.Error("empty archive not recognized by Sniff")
	}
}

func TestReader_OpenErrors(t *testing.T) {
	r := open(t, buildZip(t, "", store("d/", ""), store("1.jpg", "x")))
	for _, idx := range []int{-1, 2, 99, 0} { // out of range and a directory
		if _, err := r.Open(idx); !errors.Is(err, container.ErrEntryNotFound) {
			t.Errorf("Open(%d) error = %v, want ErrEntryNotFound", idx, err)
		}
	}
}

func TestReader_EncryptedAndUnsupportedEntries(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.CreateHeader(&zip.FileHeader{Name: "1.jpg", Method: zip.Store, Flags: 0x1})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("secret"))
	payload := []byte("data")
	raw, err := zw.CreateRaw(&zip.FileHeader{
		Name: "2.jpg", Method: 99,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = raw.Write(payload)
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	r := open(t, buf.Bytes())
	if _, err := r.Open(0); !errors.Is(err, container.ErrEncrypted) {
		t.Errorf("encrypted Open error = %v, want ErrEncrypted", err)
	}
	if _, err := r.Open(1); !errors.Is(err, container.ErrUnsupported) {
		t.Errorf("unknown method Open error = %v, want ErrUnsupported", err)
	}
}

func TestReader_CorruptArchivesFailCleanly(t *testing.T) {
	good := buildZip(t, "", store("1.jpg", strings.Repeat("a", 500)), deflate("2.jpg", strings.Repeat("b", 500)))

	flipped := append([]byte(nil), good...)
	// Make the central directory size in the end record larger than the file.
	// (A wrong offset alone is tolerated: like archive/zip, the offset is
	// derived from the end record's position to support prepended data.)
	for i := len(flipped) - 10; i < len(flipped)-6; i++ {
		flipped[i] = 0xee
	}
	badSig := append([]byte(nil), good...)
	// Destroy the first central-directory signature.
	if i := bytes.Index(badSig, []byte("PK\x01\x02")); i >= 0 {
		badSig[i+2] = 0xff
	}

	cases := map[string][]byte{
		"empty":                 {},
		"too small":             []byte("PK\x03\x04"),
		"magic only garbage":    append([]byte("PK\x03\x04"), bytes.Repeat([]byte{0xab}, 4096)...),
		"truncated end record":  good[:len(good)-10],
		"truncated half":        good[:len(good)/2],
		"bad directory size":    flipped,
		"bad directory entries": badSig,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			r, err := cbz.NewReader(bytes.NewReader(data), int64(len(data)))
			if err == nil {
				t.Fatalf("NewReader succeeded with %d entries", len(r.Entries()))
			}
			if !errors.Is(err, container.ErrCorrupt) {
				t.Errorf("error = %v, want ErrCorrupt", err)
			}
		})
	}

	if _, err := cbz.NewReader(bytes.NewReader(good), -1); !errors.Is(err, container.ErrCorrupt) {
		t.Errorf("negative size error = %v, want ErrCorrupt", err)
	}
}

func TestReader_CorruptEntryDataFailsOnRead(t *testing.T) {
	payload := strings.Repeat("z", 300)
	data := buildZip(t, "", store("1.jpg", payload))
	i := bytes.Index(data, []byte(payload))
	if i < 0 {
		t.Fatal("payload not found")
	}
	data[i+10] ^= 0xff
	r := open(t, data)
	rc, err := r.Open(0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := io.ReadAll(rc); err == nil {
		t.Error("corrupted Store entry read without a checksum error")
	}
}

// ---- central directory locator ------------------------------------------

func checkDirectory(t *testing.T, data []byte, d cbz.Directory, zip64 bool) {
	t.Helper()
	if d.Zip64 != zip64 {
		t.Errorf("Zip64 = %v, want %v", d.Zip64, zip64)
	}
	if !bytes.HasPrefix(data[d.Offset:], []byte("PK\x01\x02")) {
		t.Errorf("no central-directory record at offset %d", d.Offset)
	}
	if d.Offset+d.Size != d.EndOffset {
		t.Errorf("directory [%d,+%d) does not end at EndOffset %d", d.Offset, d.Size, d.EndOffset)
	}
	sig := "PK\x05\x06"
	if zip64 {
		sig = "PK\x06\x06"
	}
	if !bytes.HasPrefix(data[d.EndOffset:], []byte(sig)) {
		t.Errorf("end record at %d is not %q", d.EndOffset, sig)
	}
}

func TestLocateDirectory_Plain(t *testing.T) {
	data := buildZip(t, "", store("1.jpg", "aaaa"), deflate("2.jpg", strings.Repeat("b", 100)))
	d, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	checkDirectory(t, data, d, false)
	if d.EndOffset+22 != int64(len(data)) {
		t.Errorf("EndOffset = %d, want %d", d.EndOffset, len(data)-22)
	}
	if got := open(t, data).Directory(); got != d {
		t.Errorf("Reader.Directory() = %+v, want %+v", got, d)
	}
}

func TestLocateDirectory_Commented(t *testing.T) {
	for _, n := range []int{1, 100, 65535} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			data := buildZip(t, strings.Repeat("c", n), store("1.jpg", "a"), store("2.jpg", "b"))
			d, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			checkDirectory(t, data, d, false)
			if d.EndOffset+22+int64(n) != int64(len(data)) {
				t.Errorf("EndOffset %d does not leave a %d-byte comment", d.EndOffset, n)
			}
			// The archive must still open and stream.
			r := open(t, data)
			if len(r.Pages()) != 2 {
				t.Errorf("pages = %v", pageNames(r))
			}
		})
	}
}

func TestLocateDirectory_Zip64(t *testing.T) {
	// More than 65535 records forces archive/zip's writer to emit ZIP64 end
	// records, so the classic end record carries 0xffff markers.
	const count = 70000
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for i := 0; i < count; i++ {
		if _, err := zw.CreateHeader(&zip.FileHeader{Name: fmt.Sprintf("%06d.jpg", i), Method: zip.Store}); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.SetComment("zip64 and commented"); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()

	d, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	checkDirectory(t, data, d, true)

	r := open(t, data)
	if got := len(r.Pages()); got != count {
		t.Fatalf("pages = %d, want %d", got, count)
	}
	if r.Directory() != d {
		t.Errorf("Reader.Directory() = %+v, want %+v", r.Directory(), d)
	}
	pages := r.Pages()
	if string(pages[0].NameBytes) != "000000.jpg" || string(pages[count-1].NameBytes) != "069999.jpg" {
		t.Errorf("page order wrong: %q ... %q", pages[0].NameBytes, pages[count-1].NameBytes)
	}
}

func TestLocateDirectory_PrependedData(t *testing.T) {
	zipData := buildZip(t, "", store("1.jpg", "a"), store("2.jpg", "b"))
	plain, err := cbz.LocateDirectory(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		t.Fatal(err)
	}
	prefix := bytes.Repeat([]byte("#"), 37)
	data := append(append([]byte(nil), prefix...), zipData...)

	d, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if d.Offset != plain.Offset+37 || d.Size != plain.Size || d.EndOffset != plain.EndOffset+37 {
		t.Errorf("directory = %+v, want shifted by 37 from %+v", d, plain)
	}
	checkDirectory(t, data, d, false)
}

func TestLocateDirectory_Errors(t *testing.T) {
	for name, data := range map[string][]byte{
		"empty":    {},
		"short":    []byte("PK\x05\x06"),
		"no eocd":  bytes.Repeat([]byte{0x41}, 5000),
		"all zero": make([]byte, 100),
	} {
		if _, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data))); !errors.Is(err, container.ErrCorrupt) {
			t.Errorf("%s: error = %v, want ErrCorrupt", name, err)
		}
	}

	// Truncated comment: the end record claims more comment than the file holds.
	data := buildZip(t, "abcdef", store("1.jpg", "a"))
	data = data[:len(data)-3]
	if _, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data))); !errors.Is(err, container.ErrCorrupt) {
		t.Errorf("truncated comment error = %v, want ErrCorrupt", err)
	}
}

func TestLocateDirectory_ConfinesReadsToTailWindow(t *testing.T) {
	data := buildZip(t, "tail", store("1.jpg", strings.Repeat("p", 1<<20)), store("2.jpg", strings.Repeat("q", 1<<20)))
	spy := &spyReaderAt{data: data}
	if _, err := cbz.LocateDirectory(spy, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	const window = 65 * 1024
	if len(spy.reqs) == 0 {
		t.Fatal("no reads recorded")
	}
	for _, q := range spy.reqs {
		if q[1] > window || q[0] < int64(len(data))-window {
			t.Errorf("read [%d,+%d) escapes the %d-byte tail window", q[0], q[1], window)
		}
	}
}

// fakeDirectoryRecord builds a central-directory record named "1.jpg" with the given
// saturation, extra field, and (possibly lying) variable-length field sizes.
func fakeDirectoryRecord(compressedSize uint32, extra []byte, nameLen, extraLen, commentLen uint16) []byte {
	rec := make([]byte, 46)
	copy(rec, "PK\x01\x02")
	binary.LittleEndian.PutUint32(rec[20:], compressedSize)
	binary.LittleEndian.PutUint16(rec[28:], nameLen)
	binary.LittleEndian.PutUint16(rec[30:], extraLen)
	binary.LittleEndian.PutUint16(rec[32:], commentLen)
	rec = append(rec, "1.jpg"...)
	return append(rec, extra...)
}

// TestLocateDirectory_PrependedDataSelectionMatchesArchiveZip covers archives
// whose prepended data places directory-like bytes at the stored offset.
// archive/zip trusts that offset only when a complete directory header parses
// there; the locator must make the same choice or the fingerprinter would hash
// the wrong bytes while the reader opens the real directory.
func TestLocateDirectory_PrependedDataSelectionMatchesArchiveZip(t *testing.T) {
	zipData := buildZip(t, "", store("1.jpg", "page"))
	plain, err := cbz.LocateDirectory(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		t.Fatal(err)
	}
	stored := plain.Offset // the offset the end record stores
	saturatedExtra := func(size uint16, fill int) []byte {
		e := []byte{0x01, 0x00, byte(size), byte(size >> 8)}
		return append(e, make([]byte, fill)...)
	}

	cases := []struct {
		name        string
		record      []byte
		literalWins bool // archive/zip reads the directory at the stored offset
	}{
		{
			name:        "complete header at stored offset",
			record:      fakeDirectoryRecord(4, nil, 5, 0, 0),
			literalWins: true,
		},
		{
			name:        "signature only, field lengths overrun the file",
			record:      fakeDirectoryRecord(4, nil, 0xffff, 0xffff, 0xffff),
			literalWins: false,
		},
		{
			name:        "zip64 extra too short for saturated size",
			record:      fakeDirectoryRecord(0xffffffff, saturatedExtra(0, 0), 5, 4, 0),
			literalWins: false,
		},
		{
			name:        "zip64 extra complete for saturated size",
			record:      fakeDirectoryRecord(0xffffffff, saturatedExtra(8, 8), 5, 12, 0),
			literalWins: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prefix := make([]byte, int(stored)+len(tc.record)+64)
			copy(prefix[stored:], tc.record)
			data := append(append([]byte(nil), prefix...), zipData...)

			r, err := cbz.NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			if got := len(r.Entries()); got != 1 || r.Entries()[0].Name != "1.jpg" {
				t.Fatalf("entries = %+v", r.Entries())
			}

			located, err := cbz.LocateDirectory(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			if located != r.Directory() {
				t.Errorf("LocateDirectory = %+v, Reader.Directory() = %+v", located, r.Directory())
			}
			want := stored + int64(len(prefix))
			if tc.literalWins {
				want = stored
			}
			if located.Offset != want {
				t.Errorf("Offset = %d, want %d (literalWins=%v)", located.Offset, want, tc.literalWins)
			}
			if !bytes.HasPrefix(data[located.Offset:], []byte("PK\x01\x02")) {
				t.Errorf("no directory record at located offset %d", located.Offset)
			}
		})
	}
}

func TestLocateDirectory_ProbeReadsAreBounded(t *testing.T) {
	zipData := buildZip(t, "", store("1.jpg", strings.Repeat("p", 1<<20)))
	plain, err := cbz.LocateDirectory(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		t.Fatal(err)
	}
	stored := plain.Offset
	rec := fakeDirectoryRecord(4, nil, 0xffff, 0xffff, 0xffff)
	prefix := make([]byte, int(stored)+len(rec)+64)
	copy(prefix[stored:], rec)
	// The archive body is large, so an unbounded probe would be visible.
	data := append(append([]byte(nil), prefix...), zipData...)

	spy := &spyReaderAt{data: data}
	if _, err := cbz.LocateDirectory(spy, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	const window = 65 * 1024
	const maxProbe = 46 + 3*65535
	for _, q := range spy.reqs {
		inTail := q[0] >= int64(len(data))-window && q[1] <= window
		inProbe := q[0] >= stored && q[0] <= stored+46 && q[1] <= maxProbe
		if !inTail && !inProbe {
			t.Errorf("read [%d,+%d) is neither in the tail window nor the bounded directory probe at %d", q[0], q[1], stored)
		}
	}
}

func TestReader_ReturnsIndependentEntryNames(t *testing.T) {
	r := open(t, buildZip(t, "", store("ComicInfo.xml", "<ComicInfo/>"), store("1.jpg", "x"), store("2.jpg", "y")))

	entries := r.Entries()
	pages := r.Pages()
	ci, ok := r.ComicInfo()
	if !ok {
		t.Fatal("ComicInfo.xml not found")
	}
	for _, name := range [][]byte{entries[0].NameBytes, entries[1].NameBytes, pages[0].NameBytes, ci.NameBytes} {
		for i := range name {
			name[i] = 'Z'
		}
	}

	for label, got := range map[string][]byte{
		"Entries":   r.Entries()[1].NameBytes,
		"Pages":     r.Pages()[0].NameBytes,
		"ComicInfo": mustComicInfo(t, r).NameBytes,
	} {
		if strings.Contains(string(got), "Z") {
			t.Errorf("%s returned a name altered through another result: %q", label, got)
		}
	}
	if got := r.Entries()[1]; string(got.NameBytes) != "1.jpg" || got.Name != "1.jpg" {
		t.Errorf("entry = %q / %q, want raw and display names to agree", got.NameBytes, got.Name)
	}
	if got := mustComicInfo(t, r); string(got.NameBytes) != "ComicInfo.xml" {
		t.Errorf("ComicInfo name = %q", got.NameBytes)
	}
	// Streaming must still work after the callers scribbled on their copies.
	if got := readEntry(t, r, r.Pages()[1]); got != "y" {
		t.Errorf("page content = %q", got)
	}
}

func mustComicInfo(t testing.TB, r *cbz.Reader) container.Entry {
	t.Helper()
	e, ok := r.ComicInfo()
	if !ok {
		t.Fatal("ComicInfo.xml not found")
	}
	return e
}
