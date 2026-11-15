package domain

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// Common domain errors for reading progress and locators.
var (
	ErrInvalidLocatorType       = errors.New("invalid locator type (must be page or text)")
	ErrMissingPagePosition      = errors.New("page locator requires non-negative page")
	ErrInvalidPageCount         = errors.New("page locator page_count cannot be negative")
	ErrMissingTextHref          = errors.New("text locator requires non-empty href")
	ErrMissingTextProgression   = errors.New("text locator requires progression between 0 and 1")
	ErrInvalidTotalProgression  = errors.New("total_progression must be between 0 and 1")
	ErrUnexpectedLocatorField   = errors.New("locator contains fields incompatible with its type")
	ErrInvalidLocatorDigest     = errors.New("locator asset_digest requires non-empty kind and value")
	ErrMissingLocatorAssetID    = errors.New("recorded locator requires non-empty asset_id")
	ErrMissingLocatorDigest     = errors.New("recorded locator requires asset_digest")
	ErrMissingRecordedPageCount = errors.New("recorded page locator requires positive page_count")
	ErrEmptyProgressSource      = errors.New("reading progress source cannot be empty")
)

// LocatorType identifies the Readium-style locator variant (ADR-0018 S4).
type LocatorType string

const (
	LocatorTypePage LocatorType = "page"
	LocatorTypeText LocatorType = "text"
)

// ValidLocatorTypes returns all defined locator types.
func ValidLocatorTypes() []LocatorType {
	return []LocatorType{
		LocatorTypePage,
		LocatorTypeText,
	}
}

// IsValid reports whether t is a recognized LocatorType.
func (t LocatorType) IsValid() bool {
	return slices.Contains(ValidLocatorTypes(), t)
}

// LocatorDigest captures the source asset's digest kind and hex value when a locator was written
// so content replacements on the same asset remain detectable (ADR-0018 S4).
type LocatorDigest struct {
	Kind  DigestKind `json:"kind"`
	Value string     `json:"value"`
}

// Validate checks LocatorDigest invariants.
func (d LocatorDigest) Validate() error {
	if strings.TrimSpace(string(d.Kind)) == "" || strings.TrimSpace(d.Value) == "" {
		return ErrInvalidLocatorDigest
	}
	return nil
}

// String formats the digest as "<kind>:<value>".
func (d LocatorDigest) String() string {
	return string(d.Kind) + ":" + d.Value
}

// ParseLocatorDigest parses a "<kind>:<value>" string into a LocatorDigest.
// Returns nil without error for an empty input string.
func ParseLocatorDigest(raw string) (*LocatorDigest, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	kind, value, ok := strings.Cut(raw, ":")
	if !ok || strings.TrimSpace(kind) == "" || strings.TrimSpace(value) == "" {
		return nil, ErrInvalidLocatorDigest
	}
	return &LocatorDigest{
		Kind:  DigestKind(kind),
		Value: value,
	}, nil
}

// Locator represents a typed reading position modeled on the Readium Locator (ADR-0018 S4).
// Pointer fields for numeric positions ensure zero-valued positions (page 0, progression 0.0)
// are preserved verbatim through JSON serialization and distinguishable from omitted fields.
type Locator struct {
	Type        LocatorType    `json:"type"`
	AssetID     string         `json:"asset_id,omitempty"`
	AssetDigest *LocatorDigest `json:"asset_digest,omitempty"`

	// Page locator fields (Type == LocatorTypePage)
	Page      *int `json:"page,omitempty"`
	PageCount *int `json:"page_count,omitempty"`

	// Text locator fields (Type == LocatorTypeText)
	Href        string   `json:"href,omitempty"`
	Progression *float64 `json:"progression,omitempty"`
	CFI         *string  `json:"cfi,omitempty"`
}

// NewPageLocator constructs a page-based Locator for image media (ADR-0018 S4).
func NewPageLocator(page, pageCount int, assetID string, digest *LocatorDigest) Locator {
	p := page
	pc := pageCount
	var d *LocatorDigest
	if digest != nil {
		copied := *digest
		d = &copied
	}
	return Locator{
		Type:        LocatorTypePage,
		AssetID:     assetID,
		AssetDigest: d,
		Page:        &p,
		PageCount:   &pc,
	}
}

// NewTextLocator constructs a text-based Locator for reflowable media (ADR-0018 S4).
func NewTextLocator(href string, progression float64, cfi *string, assetID string, digest *LocatorDigest) Locator {
	prog := progression
	var cfiCopy *string
	if cfi != nil {
		v := *cfi
		cfiCopy = &v
	}
	var d *LocatorDigest
	if digest != nil {
		copied := *digest
		d = &copied
	}
	return Locator{
		Type:        LocatorTypeText,
		AssetID:     assetID,
		AssetDigest: d,
		Href:        href,
		Progression: &prog,
		CFI:         cfiCopy,
	}
}

// Validate checks Locator invariants for its declared Type.
func (l *Locator) Validate() error {
	if l.AssetDigest != nil {
		if err := l.AssetDigest.Validate(); err != nil {
			return err
		}
	}

	switch l.Type {
	case LocatorTypePage:
		if l.Href != "" || l.Progression != nil || l.CFI != nil {
			return ErrUnexpectedLocatorField
		}
		if l.Page == nil || *l.Page < 0 {
			return ErrMissingPagePosition
		}
		if l.PageCount != nil && *l.PageCount < 0 {
			return ErrInvalidPageCount
		}
		return nil
	case LocatorTypeText:
		if l.Page != nil || l.PageCount != nil {
			return ErrUnexpectedLocatorField
		}
		if strings.TrimSpace(l.Href) == "" {
			return ErrMissingTextHref
		}
		if l.Progression == nil || math.IsNaN(*l.Progression) || *l.Progression < 0.0 || *l.Progression > 1.0 {
			return ErrMissingTextProgression
		}
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidLocatorType, l.Type)
	}
}

// ValidateRecorded checks invariants for a persisted locator in reading_progress (ADR-0018 S4):
// in addition to shape validation, recorded locators must carry the source asset_id and
// asset_digest, and recorded page locators must carry a positive source page_count.
func (l *Locator) ValidateRecorded() error {
	if err := l.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(l.AssetID) == "" {
		return ErrMissingLocatorAssetID
	}
	if l.AssetDigest == nil {
		return ErrMissingLocatorDigest
	}
	if l.Type == LocatorTypePage && (l.PageCount == nil || *l.PageCount <= 0) {
		return ErrMissingRecordedPageCount
	}
	return nil
}

// MatchesAsset reports whether the locator was recorded against the given asset and current digest.
func (l *Locator) MatchesAsset(assetID string, current *AssetDigest) bool {
	if assetID == "" || l.AssetID != assetID || l.AssetDigest == nil || current == nil {
		return false
	}
	return l.AssetDigest.Kind == current.Kind && l.AssetDigest.Value == current.Value
}

// ReadingProgress represents the recorded reading state for a (user, volume) pair (ADR-0018 S4).
type ReadingProgress struct {
	UserID           string    `json:"user_id"`
	VolumeID         string    `json:"volume_id"`
	Locator          Locator   `json:"locator"`
	TotalProgression float64   `json:"total_progression"`
	Device           *string   `json:"device,omitempty"`
	Source           string    `json:"source"`
	Completed        bool      `json:"completed"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// Validate checks ReadingProgress invariants aligned with the reading_progress table (ADR-0018 S4).
func (p *ReadingProgress) Validate() error {
	if strings.TrimSpace(p.UserID) == "" {
		return ErrEmptyUserID
	}
	if strings.TrimSpace(p.VolumeID) == "" {
		return ErrEmptyVolumeID
	}
	if strings.TrimSpace(p.Source) == "" {
		return ErrEmptyProgressSource
	}
	if math.IsNaN(p.TotalProgression) || p.TotalProgression < 0.0 || p.TotalProgression > 1.0 {
		return ErrInvalidTotalProgression
	}
	return p.Locator.ValidateRecorded()
}
