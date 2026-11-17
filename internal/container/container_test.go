package container_test

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/container/cbz"
)

// stubReader is a minimal ContainerReader returned by test formats.
type stubReader struct{ name string }

func (s stubReader) Format() string                   { return s.name }
func (stubReader) Entries() []container.Entry         { return nil }
func (stubReader) Pages() []container.Entry           { return nil }
func (stubReader) ComicInfo() (container.Entry, bool) { return container.Entry{}, false }
func (stubReader) Open(int) (io.ReadCloser, error)    { return nil, container.ErrEntryNotFound }
func (stubReader) Diagnostics() container.Diagnostics { return container.Diagnostics{} }
func (stubReader) Close() error                       { return nil }

func stubFormat(name string, priority int, prefix string) container.Format {
	return container.Format{
		Name:     name,
		Priority: priority,
		Sniff:    func(h []byte) bool { return bytes.HasPrefix(h, []byte(prefix)) },
		Open: func(io.ReaderAt, int64) (container.ContainerReader, error) {
			return stubReader{name: name}, nil
		},
	}
}

// spyReaderAt records every ReadAt request.
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

func TestFormatRegistry_RegisterValidation(t *testing.T) {
	reg := container.NewFormatRegistry()

	if err := reg.Register(container.Format{Name: "  "}); err == nil {
		t.Error("empty name accepted")
	}
	if err := reg.Register(container.Format{Name: "nofuncs"}); err == nil {
		t.Error("format without Sniff/Open accepted")
	}
	if err := reg.Register(stubFormat("Alpha", 0, "A")); err != nil {
		t.Fatalf("Register(Alpha): %v", err)
	}
	err := reg.Register(stubFormat(" alpha ", 5, "B"))
	if !errors.Is(err, container.ErrDuplicateFormat) {
		t.Errorf("duplicate name error = %v, want ErrDuplicateFormat", err)
	}
	if got := len(reg.Formats()); got != 1 {
		t.Errorf("Formats() = %d, want 1", got)
	}
}

func TestFormatRegistry_PriorityOrder(t *testing.T) {
	reg := container.NewFormatRegistry()
	// Both claim the same magic; the higher priority must win regardless of
	// registration order, and ties keep registration order.
	for _, f := range []container.Format{
		stubFormat("generic-zip", 0, "PK"),
		stubFormat("epub", 10, "PK"),
		stubFormat("generic-zip-2", 0, "PK"),
	} {
		if err := reg.Register(f); err != nil {
			t.Fatal(err)
		}
	}
	var names []string
	for _, f := range reg.Formats() {
		names = append(names, f.Name)
	}
	want := []string{"epub", "generic-zip", "generic-zip-2"}
	if len(names) != len(want) {
		t.Fatalf("order = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("order = %v, want %v", names, want)
		}
	}

	data := []byte("PK-anything")
	r, err := reg.Open(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if r.Format() != "epub" {
		t.Errorf("opened with %q, want epub", r.Format())
	}
}

func TestFormatRegistry_SniffCBZMagic(t *testing.T) {
	reg := container.NewFormatRegistry()
	if err := reg.Register(cbz.Format()); err != nil {
		t.Fatal(err)
	}

	for name, data := range map[string][]byte{
		"local file header": []byte("PK\x03\x04rest"),
		"empty archive":     []byte("PK\x05\x06" + string(make([]byte, 18))),
		"spanned marker":    []byte("PK\x07\x08rest"),
	} {
		f, err := reg.Sniff(bytes.NewReader(data), int64(len(data)))
		if err != nil || f.Name != cbz.FormatName {
			t.Errorf("%s: Sniff = (%q, %v), want cbz", name, f.Name, err)
		}
	}

	for name, data := range map[string][]byte{
		"empty":        {},
		"short prefix": []byte("PK\x03"),
		"png":          []byte("\x89PNG\r\n\x1a\nxxxx"),
		"rar":          []byte("Rar!\x1a\x07\x00xxxx"),
		"text":         []byte("hello world"),
	} {
		_, err := reg.Sniff(bytes.NewReader(data), int64(len(data)))
		if !errors.Is(err, container.ErrUnknownFormat) {
			t.Errorf("%s: Sniff error = %v, want ErrUnknownFormat", name, err)
		}
	}
}

func TestFormatRegistry_SniffReadsBoundedHeaderOnly(t *testing.T) {
	data := append([]byte("PK\x03\x04"), make([]byte, 1<<20)...)
	spy := &spyReaderAt{data: data}
	reg := container.NewFormatRegistry()
	if err := reg.Register(cbz.Format()); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Sniff(spy, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if len(spy.reqs) != 1 || spy.reqs[0] != [2]int64{0, container.SniffLen} {
		t.Errorf("Sniff reads = %v, want one read of [0,%d)", spy.reqs, container.SniffLen)
	}
}

func TestNaturalCompare(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"2.jpg", "10.jpg", -1},
		{"10.jpg", "2.jpg", 1},
		{"page9.png", "page10.png", -1},
		{"001.jpg", "2.jpg", -1},
		{"a.jpg", "B.jpg", -1},
		{"B.jpg", "a.jpg", 1},
		{"ch2/p1.jpg", "ch10/p1.jpg", -1},
		{"x", "x", 0},
		{"x01", "x1", -1}, // equal value: deterministic byte-order tie break
		{"x1", "x01", 1},
		{"a", "a1", -1},
		{"\xff1.jpg", "\xff2.jpg", -1}, // non-UTF-8 bytes are ordered, not rejected
	}
	for _, tc := range cases {
		if got := container.NaturalCompare([]byte(tc.a), []byte(tc.b)); got != tc.want {
			t.Errorf("NaturalCompare(%q,%q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestSelectPages(t *testing.T) {
	names := []string{
		"10.jpg", "2.JPG", "1.png", "__MACOSX/1.jpg", "._1.jpg", ".hidden/3.jpg",
		"sub/.5.jpg", "notes.txt", "ComicInfo.xml", "noext", "z.jxl", "y.avif",
	}
	entries := make([]container.Entry, 0, len(names)+1)
	for i, n := range names {
		entries = append(entries, container.Entry{Index: i, NameBytes: []byte(n), Name: n})
	}
	entries = append(entries, container.Entry{Index: len(names), NameBytes: []byte("dir.jpg/"), IsDir: true})

	pages := container.SelectPages(entries)
	var got []string
	for _, p := range pages {
		got = append(got, string(p.NameBytes))
	}
	want := []string{"1.png", "2.JPG", "10.jpg", "y.avif", "z.jxl"}
	if len(got) != len(want) {
		t.Fatalf("pages = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pages = %v, want %v", got, want)
		}
	}
	if len(entries) != len(names)+1 || entries[0].NameBytes[0] != '1' {
		t.Error("SelectPages modified its input")
	}

	codecs := container.DiagnoseCodecs(pages)
	if len(codecs) != 2 || codecs[0] != container.CodecJXL || codecs[1] != container.CodecAVIF {
		t.Errorf("DiagnoseCodecs = %v, want [jxl avif]", codecs)
	}
	if got := container.DiagnoseCodecs(pages[:3]); len(got) != 0 {
		t.Errorf("DiagnoseCodecs(no jxl/avif) = %v, want empty", got)
	}
}
