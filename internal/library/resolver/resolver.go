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
	// filename winners share the title/run namespace; folder winners use a
	// separate namespace keyed by the complete root-relative parent path.
	Key string

	// Source is the resolver in the chain that won the series.
	Source domain.ResolverName

	// Title is the preserved source display title for the series.
	Title string

	// TitleSort is the deterministic baseline sort key derived from Title.
	TitleSort string

	// NormalizedTitle is the NFC, Unicode case-folded, whitespace-collapsed
	// series title for metadata and filename winners (empty for folder winners).
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
// never replace the series or alter its key.
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
	if winnerIdx >= 0 {
		out.Series = candidates[winnerIdx].series
		if candidates[winnerIdx].number != nil {
			num := *candidates[winnerIdx].number
			out.Number = &num
			out.VolumeKind = candidates[winnerIdx].volumeKind
			out.NumberSource = chain[winnerIdx]
			return out, nil
		}
	}

	for i, cand := range candidates {
		if cand.number != nil {
			num := *cand.number
			out.Number = &num
			out.VolumeKind = cand.volumeKind
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

	displayName, _ := domain.FormatPathDisplay(baseBytes)
	stem := cleanFilenameStem(displayName)
	if stem == "" {
		return candidate{}
	}

	match, ok := findFilenameMarker(stem)
	if !ok {
		return candidate{}
	}

	cand := candidate{
		volumeKind: match.kind,
		number:     &match.number,
	}

	prefix := stem[:match.start]
	if match.start > 0 && (stem[match.start-1] == '-' || stem[match.start-1] == '_') {
		prefix = stem[:match.start-1]
	}

	seriesTitle := cleanSeriesPrefix(prefix)
	if seriesTitle == "" {
		return cand
	}

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

func cleanFilenameStem(name string) string {
	stem := stripFileExtension(name)
	stem = stripBracketedTags(stem)
	return strings.Join(strings.Fields(stem), " ")
}

func stripFileExtension(name string) string {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 {
		return name
	}
	if !isPlausibleExtension(name[dot+1:]) {
		return name
	}
	return name[:dot]
}

func isPlausibleExtension(ext string) bool {
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

func stripBracketedTags(s string) string {
	var b strings.Builder
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
		b.WriteString(s[groupStart:])
	}
	return b.String()
}

type filenameMarkerMatch struct {
	start  int
	kind   domain.VolumeKind
	number float64
}

func findFilenameMarker(stem string) (filenameMarkerMatch, bool) {
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
		val, err := strconv.ParseFloat(stem[i+prefixLen:end], 64)
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

func matchMarkerPrefix(rest string) (domain.VolumeKind, int, bool) {
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

func scanUnsignedDecimal(s string) (int, bool) {
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

func isMarkerRightBoundary(rest string) bool {
	if rest == "" {
		return true
	}
	r, _ := utf8.DecodeRuneInString(rest)
	return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '.'
}

func cleanSeriesPrefix(raw string) string {
	trimmed := strings.TrimSpace(raw)
	for {
		switch {
		case strings.HasSuffix(trimmed, " -"), strings.HasSuffix(trimmed, " _"):
			trimmed = strings.TrimSpace(trimmed[:len(trimmed)-2])
		case strings.HasSuffix(trimmed, "_"):
			trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, "_"))
		default:
			return trimmed
		}
	}
}
