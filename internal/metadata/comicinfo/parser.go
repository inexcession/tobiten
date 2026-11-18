// Package comicinfo parses the consumed ComicInfo v2 fields (ADR-0007 I-11).
package comicinfo

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Mode int

const (
	Strict Mode = iota
	Lenient
)

type Metadata struct {
	Series, Title, Number, Summary, Publisher, Imprint, LanguageISO, AgeRating, Manga, GTIN, Web, CoverImage string
	Volume, Count, Year, Month, Day                                                                          *int32
	Writer, Penciller, Inker, Colorist, Letterer, CoverArtist, Editor, Translator                            []string
	Characters, Teams, Locations, Genre, Tags                                                                []string
	Pages                                                                                                    []Page
}
type Page struct {
	Image int32
	Type  []string
}
type Diagnostic struct{ Field, Reason string }

type element struct {
	name     xml.Name
	attrs    []xml.Attr
	text     string
	children []*element
}

// Parse reads one complete XML document. Fatal errors return no metadata or diagnostics.
func Parse(r io.Reader, mode Mode) (Metadata, []Diagnostic, error) {
	if mode != Strict && mode != Lenient {
		return Metadata{}, nil, errors.New("comicinfo: invalid mode")
	}
	root, err := readDocument(r)
	if err != nil {
		return Metadata{}, nil, err
	}
	p := parser{mode: mode, seen: make(map[string]bool), indices: make(map[int32]bool)}
	name, err := p.name(root.name, []string{"ComicInfo"})
	if err != nil || name != "ComicInfo" {
		return Metadata{}, nil, errors.New("comicinfo: invalid root")
	}
	for _, child := range root.children {
		p.field(child)
	}
	p.checkDate()
	if p.err != nil {
		return Metadata{}, nil, p.err
	}
	return p.data, p.diagnostics, nil
}

func readDocument(r io.Reader) (*element, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("comicinfo: read: %w", err)
	}
	data = bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
	if !utf8.Valid(data) {
		return nil, errors.New("comicinfo: invalid UTF-8")
	}
	d := xml.NewDecoder(bytes.NewReader(data))
	b := documentBuilder{}
	for {
		start := d.InputOffset()
		tok, e := d.Token()
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			return nil, fmt.Errorf("comicinfo: XML: %w", e)
		}
		if err := b.add(tok, start); err != nil {
			return nil, err
		}
	}
	if b.root == nil || len(b.stack) != 0 {
		return nil, errors.New("comicinfo: incomplete XML")
	}
	return b.root, nil
}

type documentBuilder struct {
	root  *element
	stack []*element
}

var xmlDeclaration = regexp.MustCompile(`^version[ \t\r\n]*=[ \t\r\n]*("1\.0"|'1\.0')([ \t\r\n]+encoding[ \t\r\n]*=[ \t\r\n]*("[Uu][Tt][Ff]-8"|'[Uu][Tt][Ff]-8'))?([ \t\r\n]+standalone[ \t\r\n]*=[ \t\r\n]*("(yes|no)"|'(yes|no)'))?[ \t\r\n]*$`)

func (b *documentBuilder) add(tok xml.Token, start int64) error {
	switch t := tok.(type) {
	case xml.Directive:
		return errors.New("comicinfo: XML directives are unsupported")
	case xml.ProcInst:
		if strings.EqualFold(t.Target, "xml") && (t.Target != "xml" || start != 0) {
			return errors.New("comicinfo: misplaced XML declaration")
		}
		if t.Target == "xml" && !xmlDeclaration.Match(bytes.TrimSpace(t.Inst)) {
			return errors.New("comicinfo: invalid XML declaration")
		}
	case xml.StartElement:
		attrs := make(map[xml.Name]bool, len(t.Attr))
		for _, attr := range t.Attr {
			if attrs[attr.Name] {
				return errors.New("comicinfo: duplicate XML attribute")
			}
			attrs[attr.Name] = true
		}
		n := &element{name: t.Name, attrs: t.Attr}
		if len(b.stack) == 0 {
			if b.root != nil {
				return errors.New("comicinfo: multiple roots")
			}
			b.root = n
		} else {
			parent := b.stack[len(b.stack)-1]
			parent.children = append(parent.children, n)
		}
		b.stack = append(b.stack, n)
	case xml.EndElement:
		if len(b.stack) == 0 {
			return errors.New("comicinfo: unexpected end")
		}
		b.stack = b.stack[:len(b.stack)-1]
	case xml.CharData:
		if len(b.stack) == 0 {
			if strings.Trim(string(t), " \t\r\n") != "" {
				return errors.New("comicinfo: content outside root")
			}
		} else {
			b.stack[len(b.stack)-1].text += string(t)
		}
	}
	return nil
}

var fields = []string{"Series", "Title", "Number", "Volume", "Count", "Summary", "Publisher", "Imprint", "LanguageISO", "AgeRating", "Manga", "Year", "Month", "Day", "GTIN", "Web", "Writer", "Penciller", "Inker", "Colorist", "Letterer", "CoverArtist", "Editor", "Translator", "Characters", "Teams", "Locations", "Genre", "Tags", "CoverImage", "Pages"}
var manga = []string{"Unknown", "No", "Yes", "YesAndRightToLeft"}
var ages = []string{"Unknown", "Adults Only 18+", "Early Childhood", "Everyone", "Everyone 10+", "G", "Kids to Adults", "M", "MA15+", "Mature 17+", "PG", "R18+", "Rating Pending", "Teen", "X18+"}
var pageTypes = []string{"FrontCover", "InnerCover", "Roundup", "Story", "Advertisement", "Editorial", "Letters", "Preview", "BackCover", "Other", "Deleted"}

type parser struct {
	mode        Mode
	data        Metadata
	diagnostics []Diagnostic
	err         error
	seen        map[string]bool
	occurrences map[string]bool
	indices     map[int32]bool
}

func (p *parser) problem(field, reason string) {
	if p.mode == Strict {
		if p.err == nil {
			p.err = fmt.Errorf("comicinfo: %s: %s", field, reason)
		}
	} else {
		p.diagnostics = append(p.diagnostics, Diagnostic{field, reason})
	}
}
func (p *parser) name(n xml.Name, options []string) (string, error) {
	if n.Space != "" {
		return "", nil
	}
	for _, v := range options {
		if n.Local == v {
			return v, nil
		}
		if asciiEqualFold(n.Local, v) {
			if p.mode == Strict {
				return "", fmt.Errorf("incorrect casing for %s", v)
			}
			return v, nil
		}
	}
	return "", nil
}
func (p *parser) field(n *element) {
	name, err := p.name(n.name, fields)
	if err != nil {
		p.problem(n.name.Local, err.Error())
		return
	}
	if name == "" {
		return
	}
	if p.occurrences == nil {
		p.occurrences = make(map[string]bool)
	}
	if p.seen[name] || (p.mode == Strict && p.occurrences[name]) {
		p.problem(name, "duplicate singleton")
		return
	}
	p.occurrences[name] = true
	if name == "Pages" {
		p.seen[name] = true
		p.pages(n)
		return
	}
	if len(n.children) > 0 {
		p.problem(name, "expected text")
		return
	}
	value := strings.TrimSpace(n.text)
	if value == "" {
		return
	}
	if !p.setField(name, value) {
		return
	}
	p.seen[name] = true
}
func split(value string) []string {
	var out []string
	for _, v := range strings.Split(value, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
func (p *parser) setField(name, value string) bool {
	switch name {
	case "Volume", "Count", "Year", "Month", "Day":
		return p.setInteger(name, value)
	case "Manga", "AgeRating":
		opts := manga
		if name == "AgeRating" {
			opts = ages
		}
		v, ok := p.enum(value, opts)
		if !ok {
			p.problem(name, "invalid enum value")
			return false
		}
		if name == "Manga" {
			p.data.Manga = v
		} else {
			p.data.AgeRating = v
		}
		return true
	}
	switch name {
	case "Series":
		p.data.Series = value
	case "Title":
		p.data.Title = value
	case "Number":
		p.data.Number = value
	case "Summary":
		p.data.Summary = value
	case "Publisher":
		p.data.Publisher = value
	case "Imprint":
		p.data.Imprint = value
	case "LanguageISO":
		p.data.LanguageISO = value
	case "GTIN":
		p.data.GTIN = value
	case "Web":
		p.data.Web = value
	case "CoverImage":
		p.data.CoverImage = value
	case "Writer":
		p.data.Writer = split(value)
	case "Penciller":
		p.data.Penciller = split(value)
	case "Inker":
		p.data.Inker = split(value)
	case "Colorist":
		p.data.Colorist = split(value)
	case "Letterer":
		p.data.Letterer = split(value)
	case "CoverArtist":
		p.data.CoverArtist = split(value)
	case "Editor":
		p.data.Editor = split(value)
	case "Translator":
		p.data.Translator = split(value)
	case "Characters":
		p.data.Characters = split(value)
	case "Teams":
		p.data.Teams = split(value)
	case "Locations":
		p.data.Locations = split(value)
	case "Genre":
		p.data.Genre = split(value)
	case "Tags":
		p.data.Tags = split(value)
	}
	return true
}
func (p *parser) setInteger(name, value string) bool {
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n < -1 {
		p.problem(name, "invalid integer")
		return false
	}
	if n == -1 {
		return true
	}
	minVal, maxVal := int64(0), int64(2147483647)
	switch name {
	case "Year":
		minVal, maxVal = 1, 9999
	case "Month":
		minVal, maxVal = 1, 12
	case "Day":
		minVal, maxVal = 1, 31
	}
	if n < minVal || n > maxVal {
		p.problem(name, "out of range")
		return false
	}
	v := int32(n)
	switch name {
	case "Volume":
		p.data.Volume = &v
	case "Count":
		p.data.Count = &v
	case "Year":
		p.data.Year = &v
	case "Month":
		p.data.Month = &v
	case "Day":
		p.data.Day = &v
	}
	return true
}

func (p *parser) enum(value string, options []string) (string, bool) {
	for _, v := range options {
		if value == v || (p.mode == Lenient && asciiEqualFold(value, v)) {
			return v, true
		}
	}
	return "", false
}
func asciiEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		x, y := a[i], b[i]
		if x >= 'A' && x <= 'Z' {
			x += 'a' - 'A'
		}
		if y >= 'A' && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
func (p *parser) checkDate() {
	d := p.data
	if d.Month == nil || d.Day == nil {
		return
	}
	year := 2000
	if d.Year != nil {
		year = int(*d.Year)
	}
	if time.Date(year, time.Month(*d.Month), int(*d.Day), 0, 0, 0, 0, time.UTC).Month() != time.Month(*d.Month) {
		p.problem("Day", "invalid calendar date")
		p.data.Day = nil
	}
}
func (p *parser) pages(n *element) {
	for _, child := range n.children {
		name, err := p.name(child.name, []string{"Page"})
		if err != nil {
			p.problem("Pages", err.Error())
			continue
		}
		if name == "" {
			continue
		}
		p.page(child)
	}
}
func (p *parser) page(n *element) {
	var image, kind string
	var hasImage, hasType bool
	for _, a := range n.attrs {
		name, err := p.name(a.Name, []string{"Image", "Type"})
		if err != nil {
			p.problem("Page", err.Error())
			continue
		}
		switch name {
		case "Image":
			if hasImage {
				p.problem("Page.Image", "duplicate attribute")
			} else {
				image = a.Value
				hasImage = true
			}
		case "Type":
			if hasType {
				p.problem("Page.Type", "duplicate attribute")
			} else {
				kind = a.Value
				hasType = true
			}
		}
	}
	i, err := strconv.ParseInt(strings.TrimSpace(image), 10, 32)
	if !hasImage || err != nil || i < 0 {
		p.problem("Page.Image", "missing or invalid index")
		return
	}
	index := int32(i)
	if p.indices[index] {
		p.problem(fmt.Sprintf("Page[%d].Image", index), "duplicate page index")
		return
	}
	page := Page{Image: index}
	if !hasType {
		page.Type = []string{"Story"}
	} else {
		for _, token := range strings.Fields(kind) {
			v, ok := p.enum(token, pageTypes)
			if !ok {
				p.problem(fmt.Sprintf("Page[%d].Type", index), "invalid flag: "+token)
				continue
			}
			page.Type = append(page.Type, v)
		}
	}
	p.indices[index] = true
	p.data.Pages = append(p.data.Pages, page)
}
