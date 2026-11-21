// Package resolver implements Tobiten's pure per-library series resolver chain
// (ADR-0019 §1, M1B-D5) without database or filesystem side effects.
package resolver

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/metadata/comicinfo"
)

// SeriesResolution describes the resolved primary series identity and display
// metadata when a resolver chain yields a series (ADR-0019 §1, M1B-D5).
type SeriesResolution struct {
	// Key is the library-scoped canonical series match key. Metadata and
	// valid-UTF-8 filename winners share the title/run namespace; invalid-UTF-8
	// filename titles use a raw-byte title namespace; folder winners use a
	// separate namespace keyed by the complete root-relative parent path.
	Key string

	// Source is the resolver in the chain that won the series.
	Source domain.ResolverName

	// Title is the preserved source display title for the series.
	Title string

	// TitleSort is the deterministic baseline sort key derived from Title.
	TitleSort string

	// NormalizedTitle is the NFC, Unicode case-folded, whitespace-collapsed
	// series title for valid-UTF-8 metadata and filename winners (empty for
	// folder winners and invalid-UTF-8 filename titles).
	NormalizedTitle string

	// MetadataRun is the nonnegative ComicInfo Volume when Source is
	// ResolverMetadata and a nonnegative Volume was present; nil denotes an
	// unspecified run.
	MetadataRun *int32

	// FolderPath is the complete root-relative parent directory identity when
	// Source is ResolverFolder (nil otherwise).
	FolderPath *domain.PathIdentity
}

// Result is the pure output of running a library's resolver chain on one
// inspected file's ComicInfo metadata and root-relative path (ADR-0019 §1).
type Result struct {
	// Standalone is true when no resolver in the chain yielded a series.
	Standalone bool

	// Series holds the resolved primary series when Standalone is false.
	Series *SeriesResolution

	// VolumeKind classifies the bibliographic unit (defaults to VolumeKindVolume).
	VolumeKind domain.VolumeKind

	// Number is the resolved item number (and initial primary membership
	// position when non-standalone), or nil when unnumbered.
	Number *float64

	// NumberSource identifies which resolver supplied Number (empty when
	// Number is nil).
	NumberSource domain.ResolverName
}

type candidate struct {
	series     *SeriesResolution
	volumeKind domain.VolumeKind
	number     *float64
}

// Resolve executes chain as a pure function over meta and relPath (ADR-0019 §1,
// M1B-D5). The first resolver in chain that yields a series wins; later
// resolvers may only fill item numbering left unset by the winning resolver and
// never replace the series or alter its key. For standalone results, numbering
// comes from the first usable candidate in chain order.
func Resolve(meta *comicinfo.Metadata, relPath domain.PathIdentity, chain []domain.ResolverName) (Result, error) {
	if err := domain.ValidateRelativePathBytes(relPath.Bytes); err != nil {
		return Result{}, err
	}
	if err := domain.ValidateResolverChain(chain); err != nil {
		return Result{}, err
	}

	candidates := make([]candidate, len(chain))
	winnerIdx := -1
	for i, name := range chain {
		candidates[i] = evaluateResolver(name, meta, relPath.Bytes)
		if winnerIdx < 0 && candidates[i].series != nil {
			winnerIdx = i
		}
	}

	out := Result{
		Standalone: winnerIdx < 0,
		VolumeKind: domain.VolumeKindVolume,
	}
	startIdx := 0
	if winnerIdx >= 0 {
		out.Series = candidates[winnerIdx].series
		startIdx = winnerIdx
	}

	for i := startIdx; i < len(candidates); i++ {
		if candidates[i].number != nil {
			num := *candidates[i].number
			out.Number = &num
			out.VolumeKind = candidates[i].volumeKind
			out.NumberSource = chain[i]
			break
		}
	}
	return out, nil
}

// NormalizeSeriesTitle normalizes title for series matching per M1B-D5: NFC,
// Unicode case folding, NFC normalization, and trimmed, collapsed whitespace
// while preserving punctuation and edition words. Returns "" if title contains
// only whitespace.
func NormalizeSeriesTitle(title string) string {
	trimmed := strings.TrimSpace(title)
	if trimmed == "" {
		return ""
	}
	folded := cases.Fold().String(norm.NFC.String(trimmed))
	normalized := norm.NFC.String(folded)
	return strings.Join(strings.Fields(normalized), " ")
}

// BaselineTitleSort computes the deterministic baseline series title_sort for
// M1B. Full multi-language sort keys arrive in M2-T17.
func BaselineTitleSort(title string) string {
	return NormalizeSeriesTitle(title)
}

func evaluateResolver(name domain.ResolverName, meta *comicinfo.Metadata, relBytes []byte) candidate {
	switch name {
	case domain.ResolverMetadata:
		return resolveMetadata(meta)
	case domain.ResolverFolder:
		return resolveFolder(relBytes)
	case domain.ResolverFilename:
		return resolveFilename(relBytes)
	default:
		return candidate{}
	}
}

func resolveMetadata(meta *comicinfo.Metadata) candidate {
	if meta == nil {
		return candidate{}
	}

	var cand candidate
	if title := strings.TrimSpace(meta.Series); title != "" {
		normTitle := NormalizeSeriesTitle(title)
		if normTitle != "" {
			var run *int32
			key := "title:run=none:" + normTitle
			if meta.Volume != nil && *meta.Volume >= 0 {
				v := *meta.Volume
				run = &v
				key = fmt.Sprintf("title:run=%d:%s", v, normTitle)
			}
			cand.series = &SeriesResolution{
				Key:             key,
				Source:          domain.ResolverMetadata,
				Title:           title,
				TitleSort:       BaselineTitleSort(title),
				NormalizedTitle: normTitle,
				MetadataRun:     run,
			}
		}
	}

	if num, ok := parseComicInfoNumber(meta.Number); ok {
		cand.number = &num
		cand.volumeKind = domain.VolumeKindVolume
	}
	return cand
}

func resolveFolder(relBytes []byte) candidate {
	lastSlash := bytes.LastIndexByte(relBytes, '/')
	if lastSlash < 0 {
		return candidate{}
	}

	parentBytes := relBytes[:lastSlash]
	leafBytes := parentBytes
	if prevSlash := bytes.LastIndexByte(parentBytes, '/'); prevSlash >= 0 {
		leafBytes = parentBytes[prevSlash+1:]
	}

	displayTitle, _ := domain.FormatPathDisplay(leafBytes)
	if strings.TrimSpace(displayTitle) == "" {
		return candidate{}
	}

	parentID := domain.NewPathIdentity(parentBytes)
	key := "folder:utf8:" + parentID.Match
	if parentID.InvalidUTF8 {
		key = "folder:raw:" + hex.EncodeToString(parentID.Bytes)
	}

	return candidate{
		series: &SeriesResolution{
			Key:        key,
			Source:     domain.ResolverFolder,
			Title:      displayTitle,
			TitleSort:  BaselineTitleSort(displayTitle),
			FolderPath: &parentID,
		},
	}
}

func resolveFilename(relBytes []byte) candidate {
	baseBytes := relBytes
	if lastSlash := bytes.LastIndexByte(relBytes, '/'); lastSlash >= 0 {
		baseBytes = relBytes[lastSlash+1:]
	}

	stemBytes := cleanFilenameStemBytes(baseBytes)
	if len(stemBytes) == 0 {
		return candidate{}
	}

	match, ok := findFilenameMarker(stemBytes)
	if !ok {
		return candidate{}
	}

	cand := candidate{
		volumeKind: match.kind,
		number:     &match.number,
	}

	prefixBytes := stemBytes[:match.start]
	if match.start > 0 && (stemBytes[match.start-1] == '-' || stemBytes[match.start-1] == '_') {
		prefixBytes = stemBytes[:match.start-1]
	}

	seriesBytes := cleanSeriesPrefixBytes(prefixBytes)
	if len(seriesBytes) == 0 {
		return cand
	}

	if !utf8.Valid(seriesBytes) {
		displayTitle, _ := domain.FormatPathDisplay(seriesBytes)
		if strings.TrimSpace(displayTitle) == "" {
			return cand
		}
		cand.series = &SeriesResolution{
			Key:       "title:raw:" + hex.EncodeToString(seriesBytes),
			Source:    domain.ResolverFilename,
			Title:     displayTitle,
			TitleSort: BaselineTitleSort(displayTitle),
		}
		return cand
	}

	seriesTitle := string(seriesBytes)
	normTitle := NormalizeSeriesTitle(seriesTitle)
	if normTitle == "" {
		return cand
	}

	cand.series = &SeriesResolution{
		Key:             "title:run=none:" + normTitle,
		Source:          domain.ResolverFilename,
		Title:           seriesTitle,
		TitleSort:       BaselineTitleSort(seriesTitle),
		NormalizedTitle: normTitle,
	}
	return cand
}

// parseComicInfoNumber parses a strict signed decimal string that fits in a
// finite float64 (M1B-D5). Scientific notation, hex floats, underscores,
// NaN/Inf, fractions, and non-numeric strings are rejected.
func parseComicInfoNumber(raw string) (float64, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || !isStrictSignedDecimal(s) {
		return 0, false
	}
	val, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
		return 0, false
	}
	if val == 0 {
		val = 0
	}
	return val, true
}

func isStrictSignedDecimal(s string) bool {
	if s[0] == '+' || s[0] == '-' {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	dotIdx := strings.IndexByte(s, '.')
	if dotIdx < 0 {
		return isASCIIDigits(s)
	}
	intPart := s[:dotIdx]
	fracPart := s[dotIdx+1:]
	if fracPart == "" || !isASCIIDigits(fracPart) {
		return false
	}
	return intPart == "" || isASCIIDigits(intPart)
}

func isASCIIDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func cleanFilenameStemBytes(name []byte) []byte {
	stem := stripFileExtensionBytes(name)
	stem = stripBracketedTagsBytes(stem)
	return collapseWhitespaceBytes(stem)
}

func stripFileExtensionBytes(name []byte) []byte {
	dot := bytes.LastIndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 {
		return name
	}
	if !isPlausibleExtension(name[dot+1:]) {
		return name
	}
	return name[:dot]
}

func isPlausibleExtension(ext []byte) bool {
	if !isASCIILetter(ext[0]) {
		return false
	}
	for i := 1; i < len(ext); i++ {
		c := ext[i]
		if !isASCIILetter(c) && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func isASCIILetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func stripBracketedTagsBytes(s []byte) []byte {
	var b bytes.Buffer
	b.Grow(len(s))
	depth := 0
	groupStart := -1
	for i := range len(s) {
		switch s[i] {
		case '[':
			if depth == 0 {
				groupStart = i
			}
			depth++
		case ']':
			if depth > 0 {
				depth--
				if depth == 0 {
					b.WriteByte(' ')
					groupStart = -1
				}
				continue
			}
			b.WriteByte(s[i])
		default:
			if depth == 0 {
				b.WriteByte(s[i])
			}
		}
	}
	if depth > 0 && groupStart >= 0 {
		b.Write(s[groupStart:])
	}
	return b.Bytes()
}

// collapseWhitespaceBytes trims leading/trailing Unicode whitespace and
// collapses whitespace runs to a single ASCII space while preserving raw
// invalid UTF-8 bytes verbatim.
func collapseWhitespaceBytes(raw []byte) []byte {
	out := make([]byte, 0, len(raw))
	pendingSpace := false
	rest := raw
	for len(rest) > 0 {
		r, size := utf8.DecodeRune(rest)
		if r == utf8.RuneError && size == 1 {
			if pendingSpace && len(out) > 0 {
				out = append(out, ' ')
			}
			pendingSpace = false
			out = append(out, rest[0])
			rest = rest[1:]
			continue
		}
		if unicode.IsSpace(r) {
			if len(out) > 0 {
				pendingSpace = true
			}
			rest = rest[size:]
			continue
		}
		if pendingSpace {
			out = append(out, ' ')
			pendingSpace = false
		}
		out = append(out, rest[:size]...)
		rest = rest[size:]
	}
	return out
}

type filenameMarkerMatch struct {
	start  int
	kind   domain.VolumeKind
	number float64
}

func findFilenameMarker(stem []byte) (filenameMarkerMatch, bool) {
	for i := range len(stem) {
		if i > 0 && !isMarkerLeftBoundary(stem[i-1]) {
			continue
		}
		kind, prefixLen, ok := matchMarkerPrefix(stem[i:])
		if !ok {
			continue
		}
		numLen, ok := scanUnsignedDecimal(stem[i+prefixLen:])
		if !ok {
			continue
		}
		end := i + prefixLen + numLen
		if !isMarkerRightBoundary(stem[end:]) {
			continue
		}
		val, err := strconv.ParseFloat(string(stem[i+prefixLen:end]), 64)
		if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
			continue
		}
		return filenameMarkerMatch{
			start:  i,
			kind:   kind,
			number: val,
		}, true
	}
	return filenameMarkerMatch{}, false
}

func isMarkerLeftBoundary(b byte) bool {
	return b == ' ' || b == '_' || b == '-'
}

func matchMarkerPrefix(rest []byte) (domain.VolumeKind, int, bool) {
	if len(rest) >= 2 && (rest[0] == 's' || rest[0] == 'S') && (rest[1] == 'p' || rest[1] == 'P') {
		return domain.VolumeKindSpecial, 2, true
	}
	if len(rest) >= 1 {
		switch rest[0] {
		case 'v', 'V':
			return domain.VolumeKindVolume, 1, true
		case 'c', 'C':
			return domain.VolumeKindChapter, 1, true
		case '#':
			return domain.VolumeKindIssue, 1, true
		}
	}
	return "", 0, false
}

func scanUnsignedDecimal(s []byte) (int, bool) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, false
	}
	if i < len(s) && s[i] == '.' {
		fracStart := i + 1
		j := fracStart
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j > fracStart {
			return j, true
		}
	}
	return i, true
}

func isMarkerRightBoundary(rest []byte) bool {
	if len(rest) == 0 {
		return true
	}
	r, size := utf8.DecodeRune(rest)
	if r == utf8.RuneError && size == 1 {
		return false
	}
	return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '.'
}

func cleanSeriesPrefixBytes(raw []byte) []byte {
	trimmed := collapseWhitespaceBytes(raw)
	for {
		switch {
		case bytes.HasSuffix(trimmed, []byte(" -")), bytes.HasSuffix(trimmed, []byte(" _")):
			trimmed = collapseWhitespaceBytes(trimmed[:len(trimmed)-2])
		case bytes.HasSuffix(trimmed, []byte("_")):
			trimmed = collapseWhitespaceBytes(trimmed[:len(trimmed)-1])
		default:
			return trimmed
		}
	}
}
