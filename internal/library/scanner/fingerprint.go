// Package scanner implements filesystem scanning primitives for library roots,
// including the XXH3-64 partial file fingerprinter (ADR-0003).
package scanner

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/container/cbz"
	"github.com/zeebo/xxh3"
)

const (
	// PartialSegmentBytes is the maximum size of the leading (head) and
	// fallback trailing (tail) segments hashed by PartialFingerprint (ADR-0003 §1).
	PartialSegmentBytes = 4 * 1024

	partialReadChunkBytes = 64 * 1024
	partialDomainPrefix   = "tobiten:xxh3_64_partial:v1\x00"

	tailModeZIPCentralDirectory byte = 0x01
	tailModeTrailingBytes       byte = 0x02
)

var (
	// ErrNilReader is returned when PartialFingerprint receives a nil io.ReaderAt.
	ErrNilReader = errors.New("scanner: reader cannot be nil")
	// ErrNegativeSize is returned when PartialFingerprint receives a negative file size.
	ErrNegativeSize = errors.New("scanner: size cannot be negative")
)

// PartialFingerprint computes the persisted XXH3_64_PARTIAL hex digest over
// size bytes of ra (ADR-0003 §1, ADR-0018 S2).
//
// Reads are confined to:
//   - the head [0, min(size, 4096)),
//   - the bounded ZIP end-of-central-directory tail search and any ZIP64
//     metadata and central-directory ranges located by cbz.LocateDirectory, and
//   - for non-ZIP inputs or ZIPs whose central directory cannot be located, the
//     trailing [max(0, size-4096), size) fallback range.
//
// Exact byte layout fed to XXH3-64 (compatibility contract; persisted in
// asset_digests with kind XXH3_64_PARTIAL):
//
//   - 27 bytes: ASCII domain tag "tobiten:xxh3_64_partial:v1\x00"
//   - 8 bytes: uint64(size), little-endian
//   - 8 bytes: uint64(len(head)), little-endian, where len(head) = min(size, 4096)
//   - N bytes: head = file[0 : len(head)]
//   - 1 byte: tail mode (0x01 = ZIP central directory, 0x02 = trailing <=4 KB)
//   - 8 bytes: uint64(tailLen), little-endian
//   - M bytes: tail bytes (ZIP central directory [dir.Offset, dir.Offset+dir.Size)
//     for mode 0x01, or file[size-min(size,4096) : size] for mode 0x02)
//
// The 64-bit XXH3 sum is encoded as 16 lowercase hexadecimal digits (big-endian).
func PartialFingerprint(ra io.ReaderAt, size int64) (string, error) {
	if ra == nil {
		return "", ErrNilReader
	}
	if size < 0 {
		return "", ErrNegativeSize
	}

	headLen := min(size, int64(PartialSegmentBytes))
	head := make([]byte, headLen)
	if headLen > 0 {
		if err := container.ReadFullAt(ra, head, 0); err != nil {
			return "", fmt.Errorf("scanner: read fingerprint head: %w", err)
		}
	}

	h := xxh3.New()
	writePartialHeader(h, size, head)

	dir, err := cbz.LocateDirectory(ra, size)
	if err == nil {
		if err := writeZIPCentralDirectory(h, ra, size, head, dir); err != nil {
			return "", err
		}
		return formatXXH364(h.Sum64()), nil
	}
	if !errors.Is(err, container.ErrCorrupt) {
		return "", fmt.Errorf("scanner: locate zip central directory: %w", err)
	}

	if err := writeTrailingFallback(h, ra, size, head); err != nil {
		return "", err
	}
	return formatXXH364(h.Sum64()), nil
}

func writePartialHeader(h *xxh3.Hasher, size int64, head []byte) {
	var meta [16]byte
	binary.LittleEndian.PutUint64(meta[0:8], uint64(size))
	binary.LittleEndian.PutUint64(meta[8:16], uint64(len(head)))
	_, _ = h.WriteString(partialDomainPrefix)
	_, _ = h.Write(meta[:])
	_, _ = h.Write(head)
}

func writeZIPCentralDirectory(h *xxh3.Hasher, ra io.ReaderAt, size int64, head []byte, dir cbz.Directory) error {
	if dir.Size < 0 || dir.Offset < 0 || dir.Offset+dir.Size > size {
		return fmt.Errorf("scanner: invalid zip central directory bounds [%d,+%d) for size %d", dir.Offset, dir.Size, size)
	}
	writeTailHeader(h, tailModeZIPCentralDirectory, uint64(dir.Size))
	if dir.Size == 0 {
		return nil
	}
	// Reuse head when the entire central directory lies within the already-read prefix.
	if dir.Offset+dir.Size <= int64(len(head)) {
		_, _ = h.Write(head[dir.Offset : dir.Offset+dir.Size])
		return nil
	}
	return hashReaderRange(h, ra, dir.Offset, dir.Size, "central directory")
}

func writeTrailingFallback(h *xxh3.Hasher, ra io.ReaderAt, size int64, head []byte) error {
	tailLen := min(size, int64(PartialSegmentBytes))
	writeTailHeader(h, tailModeTrailingBytes, uint64(tailLen))
	if tailLen == 0 {
		return nil
	}
	if size <= int64(len(head)) {
		_, _ = h.Write(head[:size])
		return nil
	}
	return hashReaderRange(h, ra, size-tailLen, tailLen, "tail")
}

func writeTailHeader(h *xxh3.Hasher, mode byte, length uint64) {
	var meta [9]byte
	meta[0] = mode
	binary.LittleEndian.PutUint64(meta[1:9], length)
	_, _ = h.Write(meta[:])
}

func hashReaderRange(h *xxh3.Hasher, ra io.ReaderAt, offset, length int64, label string) error {
	bufSize := min(length, int64(partialReadChunkBytes))
	buf := make([]byte, bufSize)
	remaining := length
	pos := offset
	for remaining > 0 {
		chunk := buf
		if remaining < int64(len(chunk)) {
			chunk = buf[:remaining]
		}
		if err := container.ReadFullAt(ra, chunk, pos); err != nil {
			return fmt.Errorf("scanner: read fingerprint %s: %w", label, err)
		}
		_, _ = h.Write(chunk)
		pos += int64(len(chunk))
		remaining -= int64(len(chunk))
	}
	return nil
}

func formatXXH364(sum uint64) string {
	return fmt.Sprintf("%016x", sum)
}
