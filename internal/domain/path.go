package domain

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const hexLowerDigits = "0123456789abcdef"

// ErrInvalidRootPath is returned when an absolute root or intake path is empty,
// relative, or contains a NUL byte (ADR-0003 §5).
var ErrInvalidRootPath = errors.New("root path must be non-empty, absolute, and NUL-free")

// PathIdentity holds the exact OS bytes of a path alongside its derived valid-UTF-8
// display text, invalid-UTF-8 diagnostic flag, and NFC match key (ADR-0003 §5).
// Display and Match are for presentation and comparison only; filesystem access
// must always use Bytes.
type PathIdentity struct {
	Bytes       []byte `json:"bytes"`
	Display     string `json:"display"`
	Match       string `json:"match"`
	InvalidUTF8 bool   `json:"invalid_utf8"`
}

// NewPathIdentity constructs a PathIdentity from exact OS path bytes, cloning raw so
// caller mutations do not alter the stored identity.
func NewPathIdentity(raw []byte) PathIdentity {
	display, invalidUTF8 := FormatPathDisplay(raw)
	return PathIdentity{
		Bytes:       slices.Clone(raw),
		Display:     display,
		Match:       PathMatchKey(display),
		InvalidUTF8: invalidUTF8,
	}
}

// NewRelativePathIdentity validates raw as a safe root-relative POSIX path and
// constructs its PathIdentity.
func NewRelativePathIdentity(raw []byte) (PathIdentity, error) {
	if err := ValidateRelativePathBytes(raw); err != nil {
		return PathIdentity{}, err
	}
	return NewPathIdentity(raw), nil
}

// NewRootPathIdentity validates raw as a non-empty, absolute, NUL-free POSIX path
// and constructs its PathIdentity.
func NewRootPathIdentity(raw []byte) (PathIdentity, error) {
	if err := ValidateRootPathBytes(raw); err != nil {
		return PathIdentity{}, err
	}
	return NewPathIdentity(raw), nil
}

// ValidateRelativePathBytes verifies that raw is a non-empty, NUL-free, relative POSIX
// path with no empty segments, "." self-references, or ".." traversal components.
// Legal Unix filename bytes such as colons, backslashes, and arbitrary non-UTF-8 bytes
// are preserved and accepted (ADR-0003 §5).
func ValidateRelativePathBytes(raw []byte) error {
	if len(raw) == 0 || bytes.IndexByte(raw, 0) >= 0 || raw[0] == '/' {
		return ErrInvalidAssetPath
	}

	start := 0
	for start <= len(raw) {
		sep := bytes.IndexByte(raw[start:], '/')
		var seg []byte
		var next int
		if sep < 0 {
			seg = raw[start:]
			next = len(raw) + 1
		} else {
			seg = raw[start : start+sep]
			next = start + sep + 1
		}

		if len(seg) == 0 || isDotSegment(seg) {
			return ErrInvalidAssetPath
		}
		start = next
	}

	return nil
}

func isDotSegment(seg []byte) bool {
	if len(seg) == 1 && seg[0] == '.' {
		return true
	}
	return len(seg) == 2 && seg[0] == '.' && seg[1] == '.'
}

// ValidateRootPathBytes verifies that raw is a non-empty, NUL-free, absolute POSIX path.
func ValidateRootPathBytes(raw []byte) error {
	if len(raw) == 0 || bytes.IndexByte(raw, 0) >= 0 || raw[0] != '/' {
		return ErrInvalidRootPath
	}
	return nil
}

// FormatPathDisplay converts raw OS path bytes into valid UTF-8 display text.
// Valid UTF-8 bytes pass through unchanged; each invalid byte is escaped as \xHH
// and invalidUTF8 is reported as true. Display text is non-reversible and must
// never be used for filesystem access (ADR-0003 §5).
func FormatPathDisplay(raw []byte) (display string, invalidUTF8 bool) {
	if utf8.Valid(raw) {
		return string(raw), false
	}

	var b strings.Builder
	b.Grow(len(raw))
	rest := raw
	for len(rest) > 0 {
		r, size := utf8.DecodeRune(rest)
		if r == utf8.RuneError && size == 1 {
			b.WriteString(`\x`)
			b.WriteByte(hexLowerDigits[rest[0]>>4])
			b.WriteByte(hexLowerDigits[rest[0]&0x0f])
			rest = rest[1:]
			invalidUTF8 = true
			continue
		}
		b.Write(rest[:size])
		rest = rest[size:]
	}
	return b.String(), invalidUTF8
}

// PathMatchKey derives the Unicode NFC match key from display text for search,
// deduplication, and cross-OS path comparison (ADR-0003 §5).
func PathMatchKey(display string) string {
	return norm.NFC.String(display)
}
