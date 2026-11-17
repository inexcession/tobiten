// Package cbz implements the pure-Go ZIP/CBZ container backend (ADR-0004,
// ADR-0009). It reads pages and ComicInfo.xml straight from an io.ReaderAt, so
// both Store and Deflate archives stream with zero temporary disk writes and
// the same code serves both build tiers.
package cbz

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/domain"
)

// FormatName is the registered name of this backend.
const FormatName = "cbz"

const zipFlagEncrypted = 0x1

// Format returns the registration for the container.FormatRegistry. It claims
// any ZIP by magic bytes at the default priority, so a more specific ZIP-based
// format such as EPUB can register ahead of it.
func Format() container.Format {
	return container.Format{
		Name:  FormatName,
		Sniff: Sniff,
		Open:  Open,
	}
}

// Sniff reports whether header starts with a ZIP signature: a local file
// header, an empty archive's end record, or a spanned-archive marker.
func Sniff(header []byte) bool {
	if len(header) < 4 {
		return false
	}
	return bytes.HasPrefix(header, []byte("PK\x03\x04")) ||
		bytes.HasPrefix(header, []byte("PK\x05\x06")) ||
		bytes.HasPrefix(header, []byte("PK\x07\x08"))
}

// Open implements container.Format.Open.
func Open(ra io.ReaderAt, size int64) (container.ContainerReader, error) {
	return NewReader(ra, size)
}

// Reader is a container.ContainerReader over a ZIP archive.
type Reader struct {
	zr          *zip.Reader
	dir         Directory
	entries     []container.Entry
	pages       []container.Entry
	comicInfo   int
	diagnostics container.Diagnostics
}

var _ container.ContainerReader = (*Reader)(nil)

// NewReader opens the ZIP archive in the size bytes of ra. Damaged archives
// fail with container.ErrCorrupt. Entry names that are not valid UTF-8 are
// listed, not rejected, and entry paths that archive/zip considers insecure
// (absolute or ".." paths) are listed too, because nothing is ever extracted.
func NewReader(ra io.ReaderAt, size int64) (*Reader, error) {
	if size < 0 {
		return nil, fmt.Errorf("%w: negative size", container.ErrCorrupt)
	}
	dir, err := LocateDirectory(ra, size)
	if err != nil {
		return nil, err
	}
	zr, err := zip.NewReader(ra, size)
	if err != nil && (!errors.Is(err, zip.ErrInsecurePath) || zr == nil) {
		return nil, fmt.Errorf("%w: %v", container.ErrCorrupt, err)
	}

	r := &Reader{zr: zr, dir: dir, comicInfo: -1}
	r.entries = make([]container.Entry, len(zr.File))
	for i, f := range zr.File {
		name := []byte(f.Name)
		display, invalid := domain.FormatPathDisplay(name)
		if invalid {
			r.diagnostics.InvalidUTF8Path = true
		}
		r.entries[i] = container.Entry{
			Index:     i,
			NameBytes: name,
			Name:      display,
			Size:      int64(f.UncompressedSize64),
			IsDir:     len(name) > 0 && name[len(name)-1] == '/',
		}
		if r.comicInfo < 0 && isComicInfo(r.entries[i]) {
			r.comicInfo = i
		}
	}
	r.pages = container.SelectPages(r.entries)
	r.diagnostics.ImageCodecs = container.DiagnoseCodecs(r.pages)
	return r, nil
}

func isComicInfo(e container.Entry) bool {
	if e.IsDir || container.IsHiddenPath(e.NameBytes) {
		return false
	}
	return bytes.EqualFold(e.NameBytes, []byte("ComicInfo.xml"))
}

// Format implements container.ContainerReader.
func (r *Reader) Format() string { return FormatName }

// Directory returns the located central directory (offset and size).
func (r *Reader) Directory() Directory { return r.dir }

// Entries implements container.ContainerReader.
func (r *Reader) Entries() []container.Entry {
	return append([]container.Entry(nil), r.entries...)
}

// Pages implements container.ContainerReader.
func (r *Reader) Pages() []container.Entry {
	return append([]container.Entry(nil), r.pages...)
}

// ComicInfo implements container.ContainerReader.
func (r *Reader) ComicInfo() (container.Entry, bool) {
	if r.comicInfo < 0 {
		return container.Entry{}, false
	}
	return r.entries[r.comicInfo], true
}

// Diagnostics implements container.ContainerReader.
func (r *Reader) Diagnostics() container.Diagnostics {
	d := r.diagnostics
	d.ImageCodecs = append([]string(nil), d.ImageCodecs...)
	return d
}

// Open implements container.ContainerReader. Compressed data is inflated on
// the fly from the underlying io.ReaderAt; nothing is written to disk.
func (r *Reader) Open(index int) (io.ReadCloser, error) {
	if index < 0 || index >= len(r.zr.File) || r.entries[index].IsDir {
		return nil, container.ErrEntryNotFound
	}
	f := r.zr.File[index]
	if f.Flags&zipFlagEncrypted != 0 {
		return nil, fmt.Errorf("%w: %s", container.ErrEncrypted, r.entries[index].Name)
	}
	rc, err := f.Open()
	if err != nil {
		if errors.Is(err, zip.ErrAlgorithm) {
			return nil, fmt.Errorf("%w: %v", container.ErrUnsupported, err)
		}
		return nil, fmt.Errorf("%w: %v", container.ErrCorrupt, err)
	}
	return rc, nil
}

// Close implements container.ContainerReader. The caller owns the io.ReaderAt.
func (r *Reader) Close() error { return nil }
