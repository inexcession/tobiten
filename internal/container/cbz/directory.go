package cbz

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/inexcession/tobiten/internal/container"
)

const (
	eocdSignature       = 0x06054b50
	eocdLen             = 22
	zip64LocSignature   = 0x07064b50
	zip64LocLen         = 20
	zip64EndSignature   = 0x06064b50
	zip64EndLen         = 56
	cdHeaderSignature   = 0x02014b50
	maxEOCDSearchWindow = 65 * 1024 // same bounded window as archive/zip
)

// Directory locates the ZIP central directory inside a file. All offsets are
// absolute file offsets and already account for data prepended to the archive.
type Directory struct {
	// Offset is where the first central-directory record starts.
	Offset int64
	// Size is the byte length of the central directory records.
	Size int64
	// EndOffset is where the end-of-central-directory metadata begins: the
	// ZIP64 end record when present, otherwise the classic end record. The
	// bytes from EndOffset to the end of the file (end records and archive
	// comment) are the ZIP metadata that follows the central directory.
	EndOffset int64
	// Zip64 reports that the sizes came from the ZIP64 end-of-central-directory
	// record rather than the classic one.
	Zip64 bool
}

// LocateDirectory finds the central directory of the ZIP archive stored in the
// size bytes of ra. It supports plain, ZIP64, and commented archives and
// mirrors archive/zip's rules for choosing the end record, so a file that
// Open accepts yields the range its entries are read from.
//
// Reads are confined to a bounded tail window (at most 65*1024 bytes) plus the
// 20-byte ZIP64 locator and the 56-byte ZIP64 end record when present. The
// M1-T10.1 fingerprinter reuses this function to hash exactly those ranges.
func LocateDirectory(ra io.ReaderAt, size int64) (Directory, error) {
	if size < eocdLen {
		return Directory{}, fmt.Errorf("%w: file too small for a zip end record", container.ErrCorrupt)
	}

	eocdOffset, eocd, err := findEOCD(ra, size)
	if err != nil {
		return Directory{}, err
	}

	d := Directory{
		Size:      int64(binary.LittleEndian.Uint32(eocd[12:16])),
		Offset:    int64(binary.LittleEndian.Uint32(eocd[16:20])),
		EndOffset: eocdOffset,
	}
	records := uint64(binary.LittleEndian.Uint16(eocd[10:12]))
	rawSize := binary.LittleEndian.Uint32(eocd[12:16])
	rawOffset := binary.LittleEndian.Uint32(eocd[16:20])

	if records == 0xffff || rawSize == 0xffffffff || rawOffset == 0xffffffff {
		z64Offset, found, err := findZip64End(ra, eocdOffset)
		if err != nil {
			return Directory{}, err
		}
		if found {
			if d, err = readZip64End(ra, z64Offset); err != nil {
				return Directory{}, err
			}
		}
	}
	return normalizeDirectory(ra, size, d)
}

// findEOCD returns the absolute offset and bytes of the classic end record.
func findEOCD(ra io.ReaderAt, size int64) (int64, []byte, error) {
	window := int64(maxEOCDSearchWindow)
	if size < window {
		window = size
	}
	start := size - window
	buf := make([]byte, window)
	if err := container.ReadFullAt(ra, buf, start); err != nil {
		return 0, nil, fmt.Errorf("container: read zip tail: %w", err)
	}
	for i := len(buf) - eocdLen; i >= 0; i-- {
		if binary.LittleEndian.Uint32(buf[i:]) != eocdSignature {
			continue
		}
		commentLen := int(binary.LittleEndian.Uint16(buf[i+20:]))
		if i+eocdLen+commentLen > len(buf) {
			return 0, nil, fmt.Errorf("%w: truncated zip comment", container.ErrCorrupt)
		}
		return start + int64(i), buf[i : i+eocdLen], nil
	}
	return 0, nil, fmt.Errorf("%w: zip end record not found", container.ErrCorrupt)
}

// findZip64End returns the offset of the ZIP64 end record named by the locator
// that immediately precedes the classic end record. found is false when no
// locator is present.
func findZip64End(ra io.ReaderAt, eocdOffset int64) (offset int64, found bool, err error) {
	locStart := eocdOffset - zip64LocLen
	if locStart < 0 {
		return 0, false, nil
	}
	loc := make([]byte, zip64LocLen)
	if err := container.ReadFullAt(ra, loc, locStart); err != nil {
		return 0, false, fmt.Errorf("container: read zip64 locator: %w", err)
	}
	if binary.LittleEndian.Uint32(loc[0:4]) != zip64LocSignature {
		return 0, false, nil
	}
	if binary.LittleEndian.Uint32(loc[4:8]) != 0 || binary.LittleEndian.Uint32(loc[16:20]) != 1 {
		return 0, false, fmt.Errorf("%w: multi-disk zip64 archives are not supported", container.ErrUnsupported)
	}
	p := binary.LittleEndian.Uint64(loc[8:16])
	if p > uint64(locStart) {
		return 0, false, fmt.Errorf("%w: zip64 end record offset out of range", container.ErrCorrupt)
	}
	return int64(p), true, nil
}

func readZip64End(ra io.ReaderAt, offset int64) (Directory, error) {
	rec := make([]byte, zip64EndLen)
	if err := container.ReadFullAt(ra, rec, offset); err != nil {
		return Directory{}, fmt.Errorf("%w: read zip64 end record: %v", container.ErrCorrupt, err)
	}
	if binary.LittleEndian.Uint32(rec[0:4]) != zip64EndSignature {
		return Directory{}, fmt.Errorf("%w: bad zip64 end record signature", container.ErrCorrupt)
	}
	size := binary.LittleEndian.Uint64(rec[40:48])
	cdOffset := binary.LittleEndian.Uint64(rec[48:56])
	if size > 1<<63-1 || cdOffset > 1<<63-1 {
		return Directory{}, fmt.Errorf("%w: zip64 directory range overflows", container.ErrCorrupt)
	}
	return Directory{
		Size:      int64(size),
		Offset:    int64(cdOffset),
		EndOffset: offset,
		Zip64:     true,
	}, nil
}

// normalizeDirectory converts the stored (archive-relative) offset into an
// absolute one and validates the range against the file size.
func normalizeDirectory(ra io.ReaderAt, size int64, d Directory) (Directory, error) {
	base := d.EndOffset - d.Size - d.Offset
	if o := base + d.Offset; o < 0 || o > size {
		return Directory{}, fmt.Errorf("%w: central directory offset outside file", container.ErrCorrupt)
	}
	// Like archive/zip: a positive base is only trusted when no directory
	// record sits at the stored offset taken literally.
	if base > 0 && d.Offset+4 <= size {
		sig := make([]byte, 4)
		if err := container.ReadFullAt(ra, sig, d.Offset); err == nil &&
			binary.LittleEndian.Uint32(sig) == cdHeaderSignature {
			base = 0
		}
	}
	d.Offset += base
	if d.Offset < 0 || d.Size < 0 || d.Offset+d.Size > d.EndOffset {
		return Directory{}, fmt.Errorf("%w: central directory overlaps the end record", container.ErrCorrupt)
	}
	return d, nil
}
