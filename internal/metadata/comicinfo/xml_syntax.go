package comicinfo

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	xmlNamespace    = "http://www.w3.org/XML/1998/namespace"
	xmlnsNamespace  = "http://www.w3.org/xmlns/"
	maxElementDepth = 64
)

type namespaceBinding struct {
	prefix string
	prev   string
	had    bool
}

type namespaceTracker struct {
	scope    map[string]string
	declared map[string]bool
	bindings []namespaceBinding
	frames   [maxElementDepth]int
	depth    int
}

func newNamespaceTracker() *namespaceTracker {
	return &namespaceTracker{
		scope: map[string]string{"xml": xmlNamespace},
	}
}

// RawToken preserves prefixes and the source offsets lost by Token. Check XML
// characters, processing instructions, character references, namespace scope,
// and literal prolog/epilog whitespace before decoding names.
func validateDocumentSyntax(data []byte) error {
	if err := validateXMLChars(data); err != nil {
		return err
	}
	d := xml.NewDecoder(bytes.NewReader(data))
	ns := newNamespaceTracker()
	for {
		start := d.InputOffset()
		tok, err := d.RawToken()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("comicinfo: XML: %w", err)
		}
		switch t := tok.(type) {
		case xml.CharData:
			if err := validateCharData(data[start:d.InputOffset()], ns.depth); err != nil {
				return err
			}
		case xml.ProcInst:
			if err := validateProcInst(data[start:d.InputOffset()], t); err != nil {
				return err
			}
		case xml.StartElement:
			if ns.depth >= maxElementDepth {
				return errors.New("comicinfo: element nesting depth exceeded")
			}
			if err := validateStartTag(data[start:d.InputOffset()]); err != nil {
				return err
			}
			if err := ns.pushStart(t); err != nil {
				return err
			}
		case xml.EndElement:
			if err := ns.popEnd(t.Name); err != nil {
				return err
			}
		}
	}
}

func (ns *namespaceTracker) pushStart(t xml.StartElement) error {
	if ns.depth >= maxElementDepth {
		return errors.New("comicinfo: element nesting depth exceeded")
	}
	frameStart := len(ns.bindings)
	if err := ns.applyDeclarations(t.Attr); err != nil {
		return err
	}
	if err := checkPrefix(t.Name, ns.scope); err != nil {
		return err
	}
	for _, a := range t.Attr {
		if a.Name.Space == "xmlns" || (a.Name.Space == "" && a.Name.Local == "xmlns") {
			continue
		}
		if err := checkPrefix(a.Name, ns.scope); err != nil {
			return err
		}
	}
	ns.frames[ns.depth] = frameStart
	ns.depth++
	return nil
}

func (ns *namespaceTracker) popEnd(name xml.Name) error {
	if ns.depth == 0 {
		return errors.New("comicinfo: unexpected end")
	}
	if err := checkPrefix(name, ns.scope); err != nil {
		return err
	}
	ns.depth--
	frameStart := ns.frames[ns.depth]
	for i := len(ns.bindings) - 1; i >= frameStart; i-- {
		b := ns.bindings[i]
		if b.had {
			ns.scope[b.prefix] = b.prev
		} else {
			delete(ns.scope, b.prefix)
		}
	}
	ns.bindings = ns.bindings[:frameStart]
	return nil
}

func (ns *namespaceTracker) applyDeclarations(attrs []xml.Attr) error {
	if len(ns.declared) > 0 {
		clear(ns.declared)
	}
	for _, a := range attrs {
		prefix := ""
		switch {
		case a.Name.Space == "xmlns":
			prefix = a.Name.Local
			if !validNCName(prefix) {
				return errors.New("comicinfo: invalid namespace prefix")
			}
		case a.Name.Space == "" && a.Name.Local == "xmlns":
		default:
			continue
		}
		if ns.declared == nil {
			ns.declared = make(map[string]bool)
		}
		if ns.declared[prefix] {
			return errors.New("comicinfo: duplicate namespace declaration")
		}
		ns.declared[prefix] = true
		if prefix == "xmlns" || strings.Contains(prefix, ":") || a.Value == xmlnsNamespace || (prefix != "" && a.Value == "") || (prefix == "xml" && a.Value != xmlNamespace) || (prefix != "xml" && a.Value == xmlNamespace) {
			return errors.New("comicinfo: invalid namespace declaration")
		}
		prev, had := ns.scope[prefix]
		ns.bindings = append(ns.bindings, namespaceBinding{prefix: prefix, prev: prev, had: had})
		ns.scope[prefix] = a.Value
	}
	return nil
}

func checkPrefix(name xml.Name, scope map[string]string) error {
	if !validNCName(name.Local) || (name.Space != "" && !validNCName(name.Space)) || name.Space == "xmlns" {
		return errors.New("comicinfo: invalid qualified name")
	}
	if name.Space != "" && scope[name.Space] == "" {
		return fmt.Errorf("comicinfo: undeclared namespace prefix %q", name.Space)
	}
	return nil
}

func validateXMLChars(data []byte) error {
	for i := 0; i < len(data); {
		r, size := utf8.DecodeRune(data[i:])
		if (r == utf8.RuneError && size == 1) || !validXMLChar(r) {
			return fmt.Errorf("comicinfo: illegal XML character %U", r)
		}
		i += size
	}
	return nil
}

func validXMLChar(r rune) bool {
	return r == 0x09 || r == 0x0A || r == 0x0D ||
		(r >= 0x20 && r <= 0xD7FF) ||
		(r >= 0xE000 && r <= 0xFFFD) ||
		(r >= 0x10000 && r <= 0x10FFFF)
}

func validateCharData(raw []byte, depth int) error {
	if depth == 0 && len(bytes.Trim(raw, " \t\r\n")) != 0 {
		return errors.New("comicinfo: nonliteral whitespace outside root")
	}
	if bytes.HasPrefix(raw, []byte("<![CDATA[")) {
		return nil
	}
	return validateCharRefs(raw)
}

func validateProcInst(raw []byte, t xml.ProcInst) error {
	if !validNCName(t.Target) {
		return errors.New("comicinfo: invalid processing instruction target")
	}
	prefixLen := 2 + len(t.Target)
	if len(raw) < prefixLen+2 || !bytes.HasPrefix(raw, []byte("<?")) || !bytes.HasSuffix(raw, []byte("?>")) {
		return errors.New("comicinfo: invalid processing instruction")
	}
	rest := raw[prefixLen : len(raw)-2]
	if len(rest) > 0 && !xmlSpace(rest[0]) {
		return errors.New("comicinfo: processing instruction missing whitespace")
	}
	return nil
}

func validateCharRefs(b []byte) error {
	for i := 0; i < len(b); i++ {
		if b[i] != '&' || i+1 >= len(b) || b[i+1] != '#' {
			continue
		}
		j := i + 2
		base := 10
		if j < len(b) && b[j] == 'x' {
			base = 16
			j++
		}
		start := j
		for j < len(b) && b[j] != ';' {
			j++
		}
		if j == start || j >= len(b) {
			return errors.New("comicinfo: invalid character reference")
		}
		if err := validateCharRefDigits(b[start:j], base); err != nil {
			return err
		}
		i = j
	}
	return nil
}

func validateCharRefDigits(digits []byte, base int) error {
	var r rune
	for _, c := range digits {
		d, ok := charRefDigit(c, base)
		if !ok {
			return errors.New("comicinfo: invalid character reference")
		}
		r = r*rune(base) + d
		if r > 0x10FFFF {
			return errors.New("comicinfo: character reference out of range")
		}
	}
	if !validXMLChar(r) {
		return fmt.Errorf("comicinfo: illegal character reference %U", r)
	}
	return nil
}

func charRefDigit(c byte, base int) (rune, bool) {
	switch {
	case c >= '0' && c <= '9':
		return rune(c - '0'), true
	case base == 16 && c >= 'a' && c <= 'f':
		return rune(c-'a') + 10, true
	case base == 16 && c >= 'A' && c <= 'F':
		return rune(c-'A') + 10, true
	default:
		return 0, false
	}
}
