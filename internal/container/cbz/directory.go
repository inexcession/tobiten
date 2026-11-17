package cbz

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

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
	zip64ExtraID        = 0x0001
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
// 20-byte ZIP64 locator and the 56-byte ZIP64 end record when present. One
// further bounded probe happens only when the end record disagrees with the
// stored directory offset (data prepended to the archive): like archive/zip,
// the stored offset is trusted over the derived one only if a complete
// central-directory header parses there, which reads at most 46+3*65535
// bytes starting at that stored offset. The M1-T10.1 fingerprinter reuses
// this function and hashes the located ranges.
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
	// Like archive/zip, a locator that names another disk or a multi-disk
	// archive is treated as absent rather than as an error.
	if binary.LittleEndian.Uint32(loc[4:8]) != 0 || binary.LittleEndian.Uint32(loc[16:20]) != 1 {
		return 0, false, nil
	}
	p := binary.LittleEndian.Uint64(loc[8:16])
	if p > math.MaxInt64 {
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
	if size > math.MaxInt64 || cdOffset > math.MaxInt64 {
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
// absolute one and validates the range against the file size. It reproduces
// archive/zip's choice of base offset so the located range is the one the
// reader parses.
func normalizeDirectory(ra io.ReaderAt, size int64, d Directory) (Directory, error) {
	base := d.EndOffset - d.Size - d.Offset
	if o := base + d.Offset; o < 0 || o >= size {
		return Directory{}, fmt.Errorf("%w: central directory offset outside file", container.ErrCorrupt)
	}
	// A positive base is only trusted when no complete directory header parses
	// at the stored offset taken literally (data prepended to the archive).
	if base > 0 && directoryHeaderParses(ra, d.Offset, size) {
		base = 0
	}
	d.Offset += base
	if d.Offset < 0 || d.Size < 0 || d.Offset+d.Size > d.EndOffset {
		return Directory{}, fmt.Errorf("%w: central directory overlaps the end record", container.ErrCorrupt)
	}
	return d, nil
}

// directoryHeaderParses reports whether archive/zip's directory-header parser
// would accept the bytes at off: the fixed 46-byte header with the central
// directory signature, its complete name, extra, and comment fields, and
// well-formed ZIP64 extended information for every saturated field.
func directoryHeaderParses(ra io.ReaderAt, off, size int64) bool {
	if off < 0 || off >= size {
		return false
	}
	r := io.NewSectionReader(ra, off, size-off)
	var hdr [46]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return false
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != cdHeaderSignature {
		return false
	}
	nameLen := int(binary.LittleEndian.Uint16(hdr[28:30]))
	extraLen := int(binary.LittleEndian.Uint16(hdr[30:32]))
	commentLen := int(binary.LittleEndian.Uint16(hdr[32:34]))
	variable := make([]byte, nameLen+extraLen+commentLen)
	if _, err := io.ReadFull(r, variable); err != nil {
		return false
	}
	return zip64ExtraWellFormed(hdr[:], variable[nameLen:nameLen+extraLen])
}

// zip64ExtraWellFormed mirrors the only parse failure archive/zip raises for
// extra data: a ZIP64 extended-information field that is too short for the
// header fields that are saturated at 0xffffffff.
func zip64ExtraWellFormed(hdr, extra []byte) bool {
	need := 0
	for _, field := range [][]byte{hdr[24:28], hdr[20:24], hdr[42:46]} { // uncompressed, compressed, header offset
		if binary.LittleEndian.Uint32(field) == math.MaxUint32 {
			need += 8
		}
	}
	for len(extra) >= 4 {
		tag := binary.LittleEndian.Uint16(extra[0:2])
		fieldSize := int(binary.LittleEndian.Uint16(extra[2:4]))
		extra = extra[4:]
		if len(extra) < fieldSize {
			break
		}
		// archive/zip consumes 8 bytes per saturated field, in order, and
		// fails as soon as fewer than 8 bytes remain; every ZIP64 field counts.
		if tag == zip64ExtraID && fieldSize < need {
			return false
		}
		extra = extra[fieldSize:]
	}
	return true
}
