package comicinfo

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
)

const xmlNamespace = "http://www.w3.org/XML/1998/namespace"
const xmlnsNamespace = "http://www.w3.org/2000/xmlns/"

// RawToken preserves prefixes and the source offsets lost by Token. Check the
// namespace scope and literal prolog/epilog whitespace before decoding names.
func validateDocumentSyntax(data []byte) error {
	d := xml.NewDecoder(bytes.NewReader(data))
	scopes := []map[string]string{{"xml": xmlNamespace}}
	depth := 0
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
			if depth == 0 && len(bytes.Trim(data[start:d.InputOffset()], " \t\r\n")) != 0 {
				return errors.New("comicinfo: nonliteral whitespace outside root")
			}
		case xml.StartElement:
			if err := validateStartTag(data[start:d.InputOffset()]); err != nil {
				return err
			}
			scope, err := validateStart(t, scopes[len(scopes)-1])
			if err != nil {
				return err
			}
			scopes = append(scopes, scope)
			depth++
		case xml.EndElement:
			if depth == 0 {
				return errors.New("comicinfo: unexpected end")
			}
			if err := checkPrefix(t.Name, scopes[len(scopes)-1]); err != nil {
				return err
			}
			scopes = scopes[:len(scopes)-1]
			depth--
		}
	}
}

func validateStart(t xml.StartElement, parent map[string]string) (map[string]string, error) {
	scope, err := namespaceScope(parent, t.Attr)
	if err != nil {
		return nil, err
	}
	if err := checkPrefix(t.Name, scope); err != nil {
		return nil, err
	}
	for _, a := range t.Attr {
		if a.Name.Space == "xmlns" || (a.Name.Space == "" && a.Name.Local == "xmlns") {
			continue
		}
		if err := checkPrefix(a.Name, scope); err != nil {
			return nil, err
		}
	}
	return scope, nil
}

func namespaceScope(parent map[string]string, attrs []xml.Attr) (map[string]string, error) {
	scope := make(map[string]string, len(parent))
	for k, v := range parent {
		scope[k] = v
	}
	declared := make(map[string]bool)
	for _, a := range attrs {
		prefix := ""
		switch {
		case a.Name.Space == "xmlns":
			prefix = a.Name.Local
			if !validNCName(prefix) {
				return nil, errors.New("comicinfo: invalid namespace prefix")
			}
		case a.Name.Space == "" && a.Name.Local == "xmlns":
		default:
			continue
		}
		if declared[prefix] {
			return nil, errors.New("comicinfo: duplicate namespace declaration")
		}
		declared[prefix] = true
		if prefix == "xmlns" || strings.Contains(prefix, ":") || a.Value == xmlnsNamespace || (prefix != "" && a.Value == "") || (prefix == "xml" && a.Value != xmlNamespace) || (prefix != "xml" && a.Value == xmlNamespace) {
			return nil, errors.New("comicinfo: invalid namespace declaration")
		}
		scope[prefix] = a.Value
	}
	return scope, nil
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
