package container

import (
	"bytes"
	"sort"
)

// Image codec names recorded in Diagnostics.ImageCodecs.
const (
	CodecJXL  = "jxl"
	CodecAVIF = "avif"
)

// pageExtensions maps lower-case image file extensions to their codec name.
var pageExtensions = map[string]string{
	".jpg":  "jpeg",
	".jpeg": "jpeg",
	".jpe":  "jpeg",
	".png":  "png",
	".gif":  "gif",
	".webp": "webp",
	".bmp":  "bmp",
	".tif":  "tiff",
	".tiff": "tiff",
	".jxl":  CodecJXL,
	".avif": CodecAVIF,
}

// IsHiddenPath reports whether any '/'-separated component of the raw entry
// path is a dotfile or the macOS "__MACOSX" resource-fork directory.
func IsHiddenPath(name []byte) bool {
	for len(name) > 0 {
		var part []byte
		if i := bytes.IndexByte(name, '/'); i >= 0 {
			part, name = name[:i], name[i+1:]
		} else {
			part, name = name, nil
		}
		if len(part) == 0 {
			continue
		}
		if part[0] == '.' || string(part) == "__MACOSX" {
			return true
		}
	}
	return false
}

// PageCodec returns the image codec implied by the entry's extension, or ""
// when the name is not an image page. Matching is ASCII case-insensitive.
func PageCodec(name []byte) string {
	base := name
	if i := bytes.LastIndexByte(base, '/'); i >= 0 {
		base = base[i+1:]
	}
	dot := bytes.LastIndexByte(base, '.')
	if dot < 0 {
		return ""
	}
	return pageExtensions[string(asciiLower(base[dot:]))]
}

// IsPage reports whether e is a visible image page.
func IsPage(e Entry) bool {
	return !e.IsDir && !IsHiddenPath(e.NameBytes) && PageCodec(e.NameBytes) != ""
}

// SelectPages returns the visible image pages of entries in natural reading
// order. The input slice is not modified.
func SelectPages(entries []Entry) []Entry {
	pages := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if IsPage(e) {
			pages = append(pages, e)
		}
	}
	sort.SliceStable(pages, func(i, j int) bool {
		if c := NaturalCompare(pages[i].NameBytes, pages[j].NameBytes); c != 0 {
			return c < 0
		}
		return pages[i].Index < pages[j].Index
	})
	return pages
}

// DiagnoseCodecs returns the problem codecs ("jxl", "avif") used by pages, in
// that fixed order, or nil when neither is present.
func DiagnoseCodecs(pages []Entry) []string {
	var jxl, avif bool
	for _, p := range pages {
		switch PageCodec(p.NameBytes) {
		case CodecJXL:
			jxl = true
		case CodecAVIF:
			avif = true
		}
	}
	var out []string
	if jxl {
		out = append(out, CodecJXL)
	}
	if avif {
		out = append(out, CodecAVIF)
	}
	return out
}

// NaturalCompare orders raw names for reading: runs of ASCII digits compare by
// numeric value, other bytes compare case-insensitively (ASCII), and any
// remaining tie is broken by plain byte order so the result is deterministic.
// It returns -1, 0, or +1 and is 0 only for byte-identical names.
func NaturalCompare(a, b []byte) int {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if isDigit(a[i]) && isDigit(b[j]) {
			si, sj := i, j
			for i < len(a) && isDigit(a[i]) {
				i++
			}
			for j < len(b) && isDigit(b[j]) {
				j++
			}
			if c := compareDigitRuns(a[si:i], b[sj:j]); c != 0 {
				return c
			}
			continue
		}
		la, lb := lowerByte(a[i]), lowerByte(b[j])
		if la != lb {
			if la < lb {
				return -1
			}
			return 1
		}
		i++
		j++
	}
	switch ra, rb := len(a)-i, len(b)-j; {
	case ra < rb:
		return -1
	case ra > rb:
		return 1
	}
	return bytes.Compare(a, b)
}

func compareDigitRuns(a, b []byte) int {
	a = bytes.TrimLeft(a, "0")
	b = bytes.TrimLeft(b, "0")
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return bytes.Compare(a, b)
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func lowerByte(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

func asciiLower(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		out[i] = lowerByte(c)
	}
	return out
}
