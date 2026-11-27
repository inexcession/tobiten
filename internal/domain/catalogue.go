package domain

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// Common domain errors for catalogue types.
var (
	ErrEmptySeriesID             = errors.New("series ID cannot be empty")
	ErrEmptyLibraryID            = errors.New("library ID cannot be empty")
	ErrEmptySeriesTitle          = errors.New("series title cannot be empty")
	ErrEmptyResolverKey          = errors.New("series resolver key cannot be empty")
	ErrConflictingManualCover    = errors.New("manual series cover cannot set both cover_volume_id and cover_image_digest")
	ErrInvalidCoverPolicy        = errors.New("invalid cover policy")
	ErrEmptyVolumeID             = errors.New("volume ID cannot be empty")
	ErrInvalidVolumeKind         = errors.New("invalid volume kind")
	ErrInvalidChapterRange       = errors.New("chapter range must set start, end, and source together with start <= end")
	ErrInvalidRangeSource        = errors.New("invalid chapter range source")
	ErrInvalidMembershipKind     = errors.New("invalid series membership kind")
	ErrInvalidMembershipSource   = errors.New("invalid series membership source")
	ErrInvalidMembershipPosition = errors.New("series membership position must be finite")
	ErrMultiplePrimarySeries     = errors.New("volume cannot have more than one primary series membership")
	ErrMissingPrimarySeries      = errors.New("non-standalone volume must have exactly one primary series membership")
	ErrDuplicateSeriesMember     = errors.New("volume cannot have duplicate memberships in the same series")
	ErrEmptyAssetID              = errors.New("asset ID cannot be empty")
	ErrEmptyLibraryRootID        = errors.New("library root ID cannot be empty")
	ErrInvalidAssetPath          = errors.New("asset relative path must be non-empty, relative, and non-escaping")
	ErrInvalidMediaFormat        = errors.New("invalid media format")
	ErrRetiredWithoutMissing     = errors.New("retired asset must have missing_since set")
	ErrInvalidDigestKind         = errors.New("digest kind cannot be empty")
	ErrEmptyDigestValue          = errors.New("digest value cannot be empty")
	ErrInvalidDigestReason       = errors.New("invalid digest reason")
)

// FieldLocks tracks Plex-style manually locked metadata fields (ADR-0007, ADR-0008).
type FieldLocks map[string]bool

// IsLocked reports whether field is locked against automatic overwrites.
func (l FieldLocks) IsLocked(field string) bool {
	if len(l) == 0 {
		return false
	}
	return l[field]
}

// Lock marks field as manually locked, returning the initialized map.
func (l FieldLocks) Lock(field string) FieldLocks {
	if l == nil {
		l = make(FieldLocks)
	}
	l[field] = true
	return l
}

// Unlock removes a manual lock from field.
func (l FieldLocks) Unlock(field string) {
	delete(l, field)
}

// HasAnyLock reports whether any field is marked locked.
func (l FieldLocks) HasAnyLock() bool {
	for _, locked := range l {
		if locked {
			return true
		}
	}
	return false
}

// SeriesStatus represents the publication status of a series.
type SeriesStatus string

const (
	SeriesStatusOngoing   SeriesStatus = "ONGOING"
	SeriesStatusEnded     SeriesStatus = "ENDED"
	SeriesStatusHiatus    SeriesStatus = "HIATUS"
	SeriesStatusAbandoned SeriesStatus = "ABANDONED"
)

// CoverPolicy controls how a series selects its cover image (ADR-0010 §1).
type CoverPolicy string

const (
	CoverPolicyFirst       CoverPolicy = "FIRST"
	CoverPolicyLatest      CoverPolicy = "LATEST"
	CoverPolicyFirstUnread CoverPolicy = "FIRST_UNREAD"
	CoverPolicyManual      CoverPolicy = "MANUAL"
)

// ValidCoverPolicies returns all defined series cover policies.
func ValidCoverPolicies() []CoverPolicy {
	return []CoverPolicy{
		CoverPolicyFirst,
		CoverPolicyLatest,
		CoverPolicyFirstUnread,
		CoverPolicyManual,
	}
}

// IsValid reports whether p is a recognized CoverPolicy.
func (p CoverPolicy) IsValid() bool {
	return slices.Contains(ValidCoverPolicies(), p)
}

// Series represents a logical series or distinct edition of a franchise (ADR-0008 §2, ADR-0019).
type Series struct {
	ID               string         `json:"id"`
	LibraryID        string         `json:"library_id"`
	ResolverKey      *string        `json:"-"`
	Title            string         `json:"title"`
	TitleSort        string         `json:"title_sort"`
	Language         *string        `json:"language,omitempty"`
	OriginalTitle    *string        `json:"original_title,omitempty"`
	RomajiTitle      *string        `json:"romaji_title,omitempty"`
	Franchise        *string        `json:"franchise,omitempty"`
	EditionName      *string        `json:"edition_name,omitempty"`
	Publisher        *string        `json:"publisher,omitempty"`
	Imprint          *string        `json:"imprint,omitempty"`
	Status           SeriesStatus   `json:"status"`
	Summary          *string        `json:"summary,omitempty"`
	AgeRating        *string        `json:"age_rating,omitempty"`
	Genres           []string       `json:"genres"`
	Tags             []string       `json:"tags"`
	CoverPolicy      *CoverPolicy   `json:"cover_policy,omitempty"`
	CoverVolumeID    *string        `json:"cover_volume_id,omitempty"`
	CoverImageDigest *string        `json:"cover_image_digest,omitempty"`
	LockedFields     FieldLocks     `json:"locked_fields"`
	ExternalIDs      map[string]any `json:"external_ids"`
	TotalVolumes     *int           `json:"total_volumes,omitempty"`
	ReleaseYear      *int           `json:"release_year,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

// Validate checks Series invariants aligned with the database schema.
func (s *Series) Validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return ErrEmptySeriesID
	}
	if strings.TrimSpace(s.LibraryID) == "" {
		return ErrEmptyLibraryID
	}
	if strings.TrimSpace(s.Title) == "" {
		return ErrEmptySeriesTitle
	}
	if s.ResolverKey != nil && strings.TrimSpace(*s.ResolverKey) == "" {
		return ErrEmptyResolverKey
	}
	if s.CoverPolicy != nil && !s.CoverPolicy.IsValid() {
		return ErrInvalidCoverPolicy
	}
	if s.CoverVolumeID != nil && s.CoverImageDigest != nil {
		return ErrConflictingManualCover
	}
	return nil
}

// VolumeKind identifies the bibliographic unit kind of a Volume (ADR-0008 §3).
type VolumeKind string

const (
	VolumeKindVolume  VolumeKind = "VOLUME"
	VolumeKindChapter VolumeKind = "CHAPTER"
	VolumeKindIssue   VolumeKind = "ISSUE"
	VolumeKindAnnual  VolumeKind = "ANNUAL"
	VolumeKindSpecial VolumeKind = "SPECIAL"
	VolumeKindOmnibus VolumeKind = "OMNIBUS"
)

// ValidVolumeKinds returns all 1.0 volume kinds.
func ValidVolumeKinds() []VolumeKind {
	return []VolumeKind{
		VolumeKindVolume,
		VolumeKindChapter,
		VolumeKindIssue,
		VolumeKindAnnual,
		VolumeKindSpecial,
		VolumeKindOmnibus,
	}
}

// IsValid reports whether k is a recognized VolumeKind.
func (k VolumeKind) IsValid() bool {
	return slices.Contains(ValidVolumeKinds(), k)
}

// ChapterRangeSource records the provenance of a volume's chapter coverage (ADR-0008 §1).
type ChapterRangeSource string

const (
	ChapterRangeSourceManual  ChapterRangeSource = "MANUAL"
	ChapterRangeSourceArchive ChapterRangeSource = "ARCHIVE"
	ChapterRangeSourceScraper ChapterRangeSource = "SCRAPER"
)

// ValidChapterRangeSources returns all defined chapter-range provenance sources.
func ValidChapterRangeSources() []ChapterRangeSource {
	return []ChapterRangeSource{
		ChapterRangeSourceManual,
		ChapterRangeSourceArchive,
		ChapterRangeSourceScraper,
	}
}

// IsValid reports whether s is a recognized ChapterRangeSource.
func (s ChapterRangeSource) IsValid() bool {
	return slices.Contains(ValidChapterRangeSources(), s)
}

// ChapterRange represents a complete, known chapter coverage range for a Volume (ADR-0008 §1, M0 S-06).
type ChapterRange struct {
	Start  float64            `json:"start"`
	End    float64            `json:"end"`
	Source ChapterRangeSource `json:"source"`
}

// Validate checks ChapterRange invariants.
func (r ChapterRange) Validate() error {
	if !r.Source.IsValid() {
		return ErrInvalidRangeSource
	}
	if r.Start > r.End {
		return ErrInvalidChapterRange
	}
	return nil
}

// MembershipKind classifies a volume's relationship to a series (ADR-0008 §5).
type MembershipKind string

const (
	MembershipKindSeries    MembershipKind = "SERIES"
	MembershipKindSubSeries MembershipKind = "SUB_SERIES"
	MembershipKindUniverse  MembershipKind = "UNIVERSE"
	MembershipKindOther     MembershipKind = "OTHER"
)

// ValidMembershipKinds returns all 1.0 series membership kinds.
func ValidMembershipKinds() []MembershipKind {
	return []MembershipKind{
		MembershipKindSeries,
		MembershipKindSubSeries,
		MembershipKindUniverse,
		MembershipKindOther,
	}
}

// IsValid reports whether k is a recognized MembershipKind.
func (k MembershipKind) IsValid() bool {
	return slices.Contains(ValidMembershipKinds(), k)
}

// MembershipSource records how a series membership was established (ADR-0008 §5, M0 S-08).
type MembershipSource string

const (
	MembershipSourceResolver MembershipSource = "RESOLVER"
	MembershipSourceMetadata MembershipSource = "METADATA"
	MembershipSourceManual   MembershipSource = "MANUAL"
)

// ValidMembershipSources returns all defined membership provenance sources.
func ValidMembershipSources() []MembershipSource {
	return []MembershipSource{
		MembershipSourceResolver,
		MembershipSourceMetadata,
		MembershipSourceManual,
	}
}

// IsValid reports whether s is a recognized MembershipSource.
func (s MembershipSource) IsValid() bool {
	return slices.Contains(ValidMembershipSources(), s)
}

// SeriesMembership associates a Volume with a Series (ADR-0008 §5).
// A standalone volume has zero memberships; any non-standalone volume has exactly one primary
// membership (IsPrimary=true) and zero or more secondary memberships.
type SeriesMembership struct {
	VolumeID     string           `json:"volume_id"`
	SeriesID     string           `json:"series_id"`
	IsPrimary    bool             `json:"is_primary"`
	Kind         MembershipKind   `json:"kind"`
	Position     *float64         `json:"position,omitempty"`
	Source       MembershipSource `json:"source"`
	LockedFields FieldLocks       `json:"locked_fields"`
}

// Validate checks SeriesMembership invariants.
func (m *SeriesMembership) Validate() error {
	if strings.TrimSpace(m.VolumeID) == "" {
		return ErrEmptyVolumeID
	}
	if strings.TrimSpace(m.SeriesID) == "" {
		return ErrEmptySeriesID
	}
	if !m.Kind.IsValid() {
		return ErrInvalidMembershipKind
	}
	if !m.Source.IsValid() {
		return ErrInvalidMembershipSource
	}
	if m.Position != nil && (math.IsNaN(*m.Position) || math.IsInf(*m.Position, 0)) {
		return ErrInvalidMembershipPosition
	}
	return nil
}

// IsProtectedFromAutoSync reports whether this membership must survive automatic
// resolver or metadata sync (ADR-0008 §5, ADR-0019 §1).
func (m *SeriesMembership) IsProtectedFromAutoSync() bool {
	return m.Source == MembershipSourceManual || m.LockedFields.HasAnyLock()
}

// ValidateSeriesMemberships checks the membership set invariants for a single volume (ADR-0008 §5):
// zero memberships (standalone) is valid; non-empty memberships must all belong to volumeID,
// target distinct series, and contain exactly one primary membership.
func ValidateSeriesMemberships(volumeID string, memberships []SeriesMembership) error {
	if len(memberships) == 0 {
		return nil
	}
	seenSeries := make(map[string]struct{}, len(memberships))
	primaryCount := 0
	for i := range memberships {
		m := &memberships[i]
		if err := m.Validate(); err != nil {
			return err
		}
		if m.VolumeID != volumeID {
			return fmt.Errorf("membership volume_id %q does not match volume %q", m.VolumeID, volumeID)
		}
		if _, dup := seenSeries[m.SeriesID]; dup {
			return ErrDuplicateSeriesMember
		}
		seenSeries[m.SeriesID] = struct{}{}
		if m.IsPrimary {
			primaryCount++
		}
	}
	if primaryCount == 0 {
		return ErrMissingPrimarySeries
	}
	if primaryCount > 1 {
		return ErrMultiplePrimarySeries
	}
	return nil
}

// Volume represents a bibliographic unit with zero or more manifestations (ADR-0008 §3, ADR-0018 S5).
// Primary and secondary series relationships live exclusively in Memberships (ADR-0008 §5);
// there is no separate SeriesID field on the domain Volume.
type Volume struct {
	ID                 string              `json:"id"`
	LibraryID          string              `json:"library_id"`
	Kind               VolumeKind          `json:"kind"`
	Number             *float64            `json:"number,omitempty"`
	ChapterStart       *float64            `json:"chapter_start,omitempty"`
	ChapterEnd         *float64            `json:"chapter_end,omitempty"`
	ChapterRangeSource *ChapterRangeSource `json:"chapter_range_source,omitempty"`
	Title              *string             `json:"title,omitempty"`
	Summary            *string             `json:"summary,omitempty"`
	ReleaseDate        *time.Time          `json:"release_date,omitempty"`
	PageCount          int                 `json:"page_count"`
	PreferredAssetID   *string             `json:"preferred_asset_id,omitempty"`
	// LocatorAssetID holds the requesting user's reading-progress locator asset ID
	// populated by read queries for ADR-0018 S5 preference resolution; never serialized.
	LocatorAssetID string             `json:"-"`
	ExternalIDs    map[string]any     `json:"external_ids"`
	LockedFields   FieldLocks         `json:"locked_fields"`
	Memberships    []SeriesMembership `json:"memberships,omitempty"`
	Assets         []MediaAsset       `json:"assets,omitempty"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

// Validate checks Volume invariants aligned with the database schema and ADR-0008.
func (v *Volume) Validate() error {
	if strings.TrimSpace(v.ID) == "" {
		return ErrEmptyVolumeID
	}
	if strings.TrimSpace(v.LibraryID) == "" {
		return ErrEmptyLibraryID
	}
	if !v.Kind.IsValid() {
		return ErrInvalidVolumeKind
	}
	if v.PageCount < 0 {
		return errors.New("volume page_count cannot be negative")
	}
	if err := v.validateChapterRange(); err != nil {
		return err
	}
	if err := ValidateSeriesMemberships(v.ID, v.Memberships); err != nil {
		return err
	}
	for i := range v.Assets {
		a := &v.Assets[i]
		if err := a.Validate(); err != nil {
			return err
		}
		if a.VolumeID != v.ID {
			return fmt.Errorf("asset %q volume_id %q does not match volume %q", a.ID, a.VolumeID, v.ID)
		}
	}
	if v.PreferredAssetID != nil && strings.TrimSpace(*v.PreferredAssetID) == "" {
		return ErrEmptyAssetID
	}
	return nil
}

func (v *Volume) validateChapterRange() error {
	hasStart := v.ChapterStart != nil
	hasEnd := v.ChapterEnd != nil
	hasSrc := v.ChapterRangeSource != nil
	if hasStart != hasEnd || hasStart != hasSrc {
		return ErrInvalidChapterRange
	}
	if !hasStart {
		return nil
	}
	if !v.ChapterRangeSource.IsValid() {
		return ErrInvalidRangeSource
	}
	if *v.ChapterStart > *v.ChapterEnd {
		return ErrInvalidChapterRange
	}
	return nil
}

// HasKnownChapterRange reports whether the volume carries a complete chapter range.
func (v *Volume) HasKnownChapterRange() bool {
	return v.ChapterStart != nil && v.ChapterEnd != nil && v.ChapterRangeSource != nil &&
		v.ChapterRangeSource.IsValid() && *v.ChapterStart <= *v.ChapterEnd
}

// ChapterRange returns the volume's complete chapter range when known.
func (v *Volume) ChapterRange() (ChapterRange, bool) {
	if !v.HasKnownChapterRange() {
		return ChapterRange{}, false
	}
	return ChapterRange{
		Start:  *v.ChapterStart,
		End:    *v.ChapterEnd,
		Source: *v.ChapterRangeSource,
	}, true
}

// AvailableAssets returns all attached digital manifestations that are neither missing nor retired.
func (v *Volume) AvailableAssets() []MediaAsset {
	if len(v.Assets) == 0 {
		return nil
	}
	available := make([]MediaAsset, 0, len(v.Assets))
	for _, a := range v.Assets {
		if a.IsAvailable() {
			available = append(available, a)
		}
	}
	return available
}

// HasManifestations reports whether the volume has any manifestation records attached.
func (v *Volume) HasManifestations() bool {
	return len(v.Assets) > 0
}

// IsOwned reports whether the volume has at least one available manifestation (ADR-0018 S5).
func (v *Volume) IsOwned() bool {
	return len(v.AvailableAssets()) > 0
}

// IsReadable reports whether the volume has at least one available manifestation to read (ADR-0018 S5).
func (v *Volume) IsReadable() bool {
	return v.IsOwned()
}

// IsMissing reports whether the volume has no available manifestations (zero assets, or all
// manifestations missing/retired) and should be presented as missing (ADR-0008 §3, ADR-0018 S5).
func (v *Volume) IsMissing() bool {
	return !v.IsOwned()
}

// IsStandalone reports whether the volume has no series memberships (ADR-0008 §5).
func (v *Volume) IsStandalone() bool {
	return len(v.Memberships) == 0
}

// PrimaryMembership returns the volume's primary SeriesMembership, if present.
func (v *Volume) PrimaryMembership() (SeriesMembership, bool) {
	for _, m := range v.Memberships {
		if m.IsPrimary {
			return m, true
		}
	}
	return SeriesMembership{}, false
}

// PrimarySeriesID returns the series ID from the volume's primary SeriesMembership, if present.
func (v *Volume) PrimarySeriesID() (string, bool) {
	m, ok := v.PrimaryMembership()
	if !ok {
		return "", false
	}
	return m.SeriesID, true
}

// SecondaryMemberships returns all non-primary series memberships for the volume.
func (v *Volume) SecondaryMemberships() []SeriesMembership {
	var out []SeriesMembership
	for _, m := range v.Memberships {
		if !m.IsPrimary {
			out = append(out, m)
		}
	}
	return out
}

// SelectPreferredAsset resolves the stable preferred file for the volume per ADR-0018 S5:
//  1. explicit manual preferred_asset_id if available and eligible;
//  2. the asset recorded in the user's existing locator if available and eligible;
//  3. the oldest eligible asset ordered by (CreatedAt, ID).
//
// Callers must supply a non-nil isEligible predicate that enforces repository-derived
// access restrictions and trash exclusions (marked_for_deletion). A nil predicate fails
// closed and selects no asset.
func (v *Volume) SelectPreferredAsset(locatorAssetID string, isEligible func(MediaAsset) bool) (*MediaAsset, bool) {
	if isEligible == nil {
		return nil, false
	}
	eligible := func(a MediaAsset) bool {
		return a.IsAvailable() && isEligible(a)
	}

	if v.PreferredAssetID != nil && *v.PreferredAssetID != "" {
		for i := range v.Assets {
			if v.Assets[i].ID == *v.PreferredAssetID && eligible(v.Assets[i]) {
				chosen := v.Assets[i]
				return &chosen, true
			}
		}
	}

	if locatorAssetID != "" {
		for i := range v.Assets {
			if v.Assets[i].ID == locatorAssetID && eligible(v.Assets[i]) {
				chosen := v.Assets[i]
				return &chosen, true
			}
		}
	}

	var best *MediaAsset
	for i := range v.Assets {
		cand := v.Assets[i]
		if !eligible(cand) {
			continue
		}
		if best == nil || compareAssetOrder(cand, *best) < 0 {
			c := cand
			best = &c
		}
	}
	if best == nil {
		return nil, false
	}
	return best, true
}

func compareAssetOrder(a, b MediaAsset) int {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		if a.CreatedAt.Before(b.CreatedAt) {
			return -1
		}
		return 1
	}
	return cmp.Compare(a.ID, b.ID)
}

// MediaFormat identifies the container format of a MediaAsset.
type MediaFormat string

const (
	MediaFormatCBZ  MediaFormat = "CBZ"
	MediaFormatCBR  MediaFormat = "CBR"
	MediaFormatCB7  MediaFormat = "CB7"
	MediaFormatEPUB MediaFormat = "EPUB"
	MediaFormatPDF  MediaFormat = "PDF"
)

// ValidMediaFormats returns all 1.0 media container formats.
func ValidMediaFormats() []MediaFormat {
	return []MediaFormat{
		MediaFormatCBZ,
		MediaFormatCBR,
		MediaFormatCB7,
		MediaFormatEPUB,
		MediaFormatPDF,
	}
}

// IsValid reports whether f is a recognized MediaFormat.
func (f MediaFormat) IsValid() bool {
	return slices.Contains(ValidMediaFormats(), f)
}

// DigestKind identifies a typed digest algorithm stored in asset_digests (ADR-0018 S2).
type DigestKind string

const (
	DigestKindXXH364Partial DigestKind = "XXH3_64_PARTIAL"
	DigestKindXXH3128Full   DigestKind = "XXH3_128_FULL"
)

// ValidDigestKinds returns all 1.0 digest kinds.
func ValidDigestKinds() []DigestKind {
	return []DigestKind{
		DigestKindXXH364Partial,
		DigestKindXXH3128Full,
	}
}

// IsValid reports whether k is a recognized 1.0 DigestKind.
func (k DigestKind) IsValid() bool {
	return slices.Contains(ValidDigestKinds(), k)
}

// DigestReason records why an asset digest was recorded (ADR-0018 S2).
type DigestReason string

const (
	DigestReasonScan           DigestReason = "SCAN"
	DigestReasonWriteBack      DigestReason = "WRITE_BACK"
	DigestReasonRepack         DigestReason = "REPACK"
	DigestReasonExternalModify DigestReason = "EXTERNAL_MODIFY"
	DigestReasonDedupe         DigestReason = "DEDUPE"
)

// ValidDigestReasons returns all defined digest recording reasons.
func ValidDigestReasons() []DigestReason {
	return []DigestReason{
		DigestReasonScan,
		DigestReasonWriteBack,
		DigestReasonRepack,
		DigestReasonExternalModify,
		DigestReasonDedupe,
	}
}

// IsValid reports whether r is a recognized DigestReason.
func (r DigestReason) IsValid() bool {
	return slices.Contains(ValidDigestReasons(), r)
}

// AssetDigest represents a current or historical file digest for a MediaAsset (ADR-0018 S2).
type AssetDigest struct {
	AssetID    string       `json:"asset_id"`
	Kind       DigestKind   `json:"kind"`
	Value      string       `json:"value"`
	IsCurrent  bool         `json:"is_current"`
	Reason     DigestReason `json:"reason"`
	RecordedAt time.Time    `json:"recorded_at"`
}

// Validate checks AssetDigest invariants.
func (d *AssetDigest) Validate() error {
	if strings.TrimSpace(d.AssetID) == "" {
		return ErrEmptyAssetID
	}
	if strings.TrimSpace(string(d.Kind)) == "" {
		return ErrInvalidDigestKind
	}
	if strings.TrimSpace(d.Value) == "" {
		return ErrEmptyDigestValue
	}
	if !d.Reason.IsValid() {
		return ErrInvalidDigestReason
	}
	return nil
}

// AssetDiagnostics holds container and codec compatibility diagnostics for a MediaAsset.
type AssetDiagnostics struct {
	SolidArchive    bool     `json:"solid_archive,omitempty"`
	ImageCodecs     []string `json:"image_codecs,omitempty"`
	InvalidUTF8Path bool     `json:"invalid_utf8_path,omitempty"`
}

// MediaAsset represents a digital file manifestation of a Volume (ADR-0003, ADR-0018 S5).
type MediaAsset struct {
	ID              string           `json:"id"`
	VolumeID        string           `json:"volume_id"`
	LibraryRootID   string           `json:"library_root_id"`
	RelPathBytes    []byte           `json:"rel_path_bytes"`
	RelPathDisplay  string           `json:"rel_path_display"`
	RelPathMatch    string           `json:"rel_path_match"`
	FileSize        int64            `json:"file_size"`
	FileMtime       *time.Time       `json:"file_mtime,omitempty"`
	Format          MediaFormat      `json:"format"`
	PageCount       int              `json:"page_count"`
	CoverPageIndex  int              `json:"cover_page_index"`
	SuppressedPages []int            `json:"suppressed_pages"`
	Diagnostics     AssetDiagnostics `json:"diagnostics"`
	Digests         []AssetDigest    `json:"digests,omitempty"`
	MissingSince    *time.Time       `json:"missing_since,omitempty"`
	RetiredAt       *time.Time       `json:"retired_at,omitempty"`
	// Trashed marks manifestations excluded by marked_for_deletion when loaded in an
	// IncludeTrashed admin scope so readable page-count selection skips them; never serialized.
	Trashed   bool      `json:"-"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Validate checks MediaAsset invariants aligned with the database schema and ADR-0003.
func (a *MediaAsset) Validate() error {
	if strings.TrimSpace(a.ID) == "" {
		return ErrEmptyAssetID
	}
	if strings.TrimSpace(a.VolumeID) == "" {
		return ErrEmptyVolumeID
	}
	if strings.TrimSpace(a.LibraryRootID) == "" {
		return ErrEmptyLibraryRootID
	}
	if err := ValidateRelativePathBytes(a.RelPathBytes); err != nil {
		return err
	}
	if a.FileSize < 0 {
		return errors.New("asset file_size cannot be negative")
	}
	if !a.Format.IsValid() {
		return ErrInvalidMediaFormat
	}
	if a.PageCount < 0 {
		return errors.New("asset page_count cannot be negative")
	}
	if a.CoverPageIndex < 0 {
		return errors.New("asset cover_page_index cannot be negative")
	}
	if a.RetiredAt != nil && a.MissingSince == nil {
		return ErrRetiredWithoutMissing
	}
	return nil
}

// IsMissing reports whether the asset is currently marked missing on disk.
func (a *MediaAsset) IsMissing() bool {
	return a.MissingSince != nil
}

// IsRetired reports whether the asset's missing grace period has expired.
func (a *MediaAsset) IsRetired() bool {
	return a.RetiredAt != nil
}

// IsAvailable reports whether the asset is neither missing nor retired.
func (a *MediaAsset) IsAvailable() bool {
	return a.MissingSince == nil && a.RetiredAt == nil
}

// CurrentDigest returns the current digest of the requested kind, if present.
func (a *MediaAsset) CurrentDigest(kind DigestKind) (AssetDigest, bool) {
	for _, d := range a.Digests {
		if d.Kind == kind && d.IsCurrent {
			return d, true
		}
	}
	return AssetDigest{}, false
}
