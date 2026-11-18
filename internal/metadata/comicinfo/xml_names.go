package comicinfo

import (
	"bytes"
	"errors"
)

var errStartTag = errors.New("comicinfo: invalid start tag")

// validateStartTag checks attribute separation against the source, which
// encoding/xml's tokenizer does not enforce. Attribute values can contain >.
func validateStartTag(tag []byte) error {
	if len(tag) < 3 || tag[0] != '<' {
		return errStartTag
	}
	i := 1
	for i < len(tag) && !xmlSpace(tag[i]) && tag[i] != '/' && tag[i] != '>' {
		i++
	}
	for i < len(tag) {
		hadSpace := false
		for i < len(tag) && xmlSpace(tag[i]) {
			hadSpace = true
			i++
		}
		if i == len(tag) {
			return errStartTag
		}
		if tag[i] == '>' || tag[i] == '/' {
			return nil
		}
		if !hadSpace {
			return errStartTag
		}
		var err error
		i, err = skipAttribute(tag, i)
		if err != nil {
			return err
		}
	}
	return errStartTag
}

func skipAttribute(tag []byte, i int) (int, error) {
	start := i
	for i < len(tag) && !xmlSpace(tag[i]) && tag[i] != '=' && tag[i] != '/' && tag[i] != '>' {
		i++
	}
	if i == start {
		return 0, errStartTag
	}
	for i < len(tag) && xmlSpace(tag[i]) {
		i++
	}
	if i == len(tag) || tag[i] != '=' {
		return 0, errStartTag
	}
	i++
	for i < len(tag) && xmlSpace(tag[i]) {
		i++
	}
	if i == len(tag) || (tag[i] != '\'' && tag[i] != '"') {
		return 0, errStartTag
	}
	quote := tag[i]
	i++
	end := bytes.IndexByte(tag[i:], quote)
	if end < 0 {
		return 0, errStartTag
	}
	if err := validateCharRefs(tag[i : i+end]); err != nil {
		return 0, err
	}
	return i + end + 1, nil
}

func xmlSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\r' || b == '\n' }

// XML 1.0 fifth edition NameStartChar ranges, excluding colon for NCName.
var nameStartRanges = [][2]rune{
	{'A', 'Z'}, {'a', 'z'}, {'_', '_'}, {0xC0, 0xD6}, {0xD8, 0xF6}, {0xF8, 0x2FF},
	{0x370, 0x37D}, {0x37F, 0x1FFF}, {0x200C, 0x200D}, {0x2070, 0x218F},
	{0x2C00, 0x2FEF}, {0x3001, 0xD7FF}, {0xF900, 0xFDCF}, {0xFDF0, 0xFFFD}, {0x10000, 0xEFFFF},
}
var nameExtraRanges = [][2]rune{{'-', '-'}, {'.', '.'}, {'0', '9'}, {0xB7, 0xB7}, {0x300, 0x36F}, {0x203F, 0x2040}}

func inNameRanges(r rune, ranges [][2]rune) bool {
	for _, pair := range ranges {
		if r >= pair[0] && r <= pair[1] {
			return true
		}
	}
	return false
}
func validNCName(s string) bool {
	first := true
	for _, r := range s {
		if !inNameRanges(r, nameStartRanges) && (first || !inNameRanges(r, nameExtraRanges)) {
			return false
		}
		first = false
	}
	return !first
}
