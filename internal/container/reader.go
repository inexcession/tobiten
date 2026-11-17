// Package container defines the archive abstraction behind Tobiten's page and
// metadata delivery (ADR-0004, ADR-0009, ADR-0018 S8). Concrete backends such
// as internal/container/cbz implement ContainerReader and register themselves
// by content sniffing in a FormatRegistry assembled by the composition root.
package container

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// SniffLen is the maximum number of leading bytes a Format.Sniff function
// receives. It is large enough for signatures that are not at offset zero,
// such as the PDF header, which may start anywhere in the first 1024 bytes.
const SniffLen = 1024

// Sentinel errors shared by every container backend.
var (
	// ErrUnknownFormat means no registered format recognized the content.
	ErrUnknownFormat = errors.New("container: unknown format")
	// ErrCorrupt means the container structure is damaged or truncated.
	ErrCorrupt = errors.New("container: corrupt archive")
	// ErrUnsupported means the container uses a feature the backend cannot read,
	// such as an unknown ZIP compression method.
	ErrUnsupported = errors.New("container: unsupported feature")
	// ErrEncrypted means an entry is encrypted and cannot be read.
	ErrEncrypted = errors.New("container: encrypted entry")
	// ErrEntryNotFound means an entry index is out of range or names a directory.
	ErrEntryNotFound = errors.New("container: entry not found")
	// ErrDuplicateFormat means a format name is already registered.
	ErrDuplicateFormat = errors.New("container: format already registered")
)

// Entry describes one member of a container.
type Entry struct {
	// Index identifies the entry for ContainerReader.Open. It is stable for the
	// lifetime of the reader and follows the container's own listing order.
	Index int
	// NameBytes is the raw entry path exactly as stored. It is the only
	// representation suitable for identity; it may not be valid UTF-8.
	NameBytes []byte
	// Name is valid UTF-8 display text derived from NameBytes. Invalid bytes
	// are escaped as \xHH (domain.FormatPathDisplay); it is not reversible.
	Name string
	// Size is the uncompressed size in bytes.
	Size int64
	// IsDir reports a directory marker entry.
	IsDir bool
}

// Diagnostics records non-fatal observations made while opening a container.
// They are informative, not alarming: the files remain valid (ADR-0004 §4).
type Diagnostics struct {
	// ImageCodecs lists page codecs that some clients and the pure-Go tier
	// cannot decode, in the fixed order "jxl", "avif". It is empty when none
	// of the pages use them.
	ImageCodecs []string
	// InvalidUTF8Path reports at least one entry name that is not valid UTF-8.
	InvalidUTF8Path bool
}

// ContainerReader reads pages and metadata from one archive without writing
// temporary files. Implementations are safe for concurrent use after Open.
//
//nolint:revive // name is fixed by ADR-0004 and ADR-0018 S8, which call the seam ContainerReader
type ContainerReader interface {
	// Format returns the registered name of the backend, for example "cbz".
	Format() string
	// Entries lists every entry in the container's own order.
	Entries() []Entry
	// Pages lists the readable image pages in natural reading order, excluding
	// directories, hidden entries, and macOS resource-fork debris.
	Pages() []Entry
	// ComicInfo returns the root-level ComicInfo.xml entry, if present.
	ComicInfo() (Entry, bool)
	// Open streams the uncompressed bytes of the entry with the given index.
	Open(index int) (io.ReadCloser, error)
	// Diagnostics returns the observations recorded at open time.
	Diagnostics() Diagnostics
	// Close releases backend resources. It never closes the underlying
	// io.ReaderAt, which the caller owns.
	Close() error
}

// Format describes a registered container backend (ADR-0018 S8).
type Format struct {
	// Name is the unique, case-insensitive backend name.
	Name string
	// Priority orders sniffing: higher values are tried first, so a more
	// specific format (EPUB over a plain ZIP) can claim shared magic bytes.
	// Ties keep registration order.
	Priority int
	// Sniff reports whether the leading bytes (at most SniffLen) belong to
	// this format. It must not retain header.
	Sniff func(header []byte) bool
	// Open returns a reader over size bytes of ra.
	Open func(ra io.ReaderAt, size int64) (ContainerReader, error)
}

// FormatRegistry resolves content to a container backend by sniffing.
type FormatRegistry struct {
	mu      sync.RWMutex
	formats []Format
}

// NewFormatRegistry constructs an empty registry.
func NewFormatRegistry() *FormatRegistry {
	return &FormatRegistry{}
}

// Register adds a format. It rejects empty names, missing functions, and
// duplicate names (compared case-insensitively).
func (r *FormatRegistry) Register(f Format) error {
	name := strings.ToLower(strings.TrimSpace(f.Name))
	if name == "" {
		return errors.New("container: format name cannot be empty")
	}
	if f.Sniff == nil || f.Open == nil {
		return fmt.Errorf("container: format %q requires Sniff and Open", f.Name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.formats {
		if strings.ToLower(strings.TrimSpace(existing.Name)) == name {
			return fmt.Errorf("%w: %q", ErrDuplicateFormat, f.Name)
		}
	}
	r.formats = append(r.formats, f)
	sort.SliceStable(r.formats, func(i, j int) bool {
		return r.formats[i].Priority > r.formats[j].Priority
	})
	return nil
}

// Formats returns the registered formats in sniffing order.
func (r *FormatRegistry) Formats() []Format {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Format, len(r.formats))
	copy(out, r.formats)
	return out
}

// Sniff returns the first format, in priority order, that recognizes the
// leading bytes of ra. It reads at most SniffLen bytes from offset zero.
func (r *FormatRegistry) Sniff(ra io.ReaderAt, size int64) (Format, error) {
	if size <= 0 {
		return Format{}, ErrUnknownFormat
	}
	n := int64(SniffLen)
	if size < n {
		n = size
	}
	header := make([]byte, n)
	if err := ReadFullAt(ra, header, 0); err != nil {
		return Format{}, fmt.Errorf("container: read header: %w", err)
	}
	for _, f := range r.Formats() {
		if f.Sniff(header) {
			return f, nil
		}
	}
	return Format{}, ErrUnknownFormat
}

// Open sniffs the content and opens it with the matching backend.
func (r *FormatRegistry) Open(ra io.ReaderAt, size int64) (ContainerReader, error) {
	f, err := r.Sniff(ra, size)
	if err != nil {
		return nil, err
	}
	return f.Open(ra, size)
}

// ReadFullAt fills buf from ra at off. Unlike io.ReadFull it accepts the
// io.EOF that io.ReaderAt may return together with a complete read, and
// reports io.ErrUnexpectedEOF for a short read.
func ReadFullAt(ra io.ReaderAt, buf []byte, off int64) error {
	n, err := ra.ReadAt(buf, off)
	if n == len(buf) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
