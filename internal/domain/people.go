package domain

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// Common domain errors for people, credits, and entities.
var (
	ErrEmptyPersonID         = errors.New("person ID cannot be empty")
	ErrEmptyPersonName       = errors.New("person name cannot be empty")
	ErrEmptyImportKey        = errors.New("import key cannot be empty")
	ErrEmptyCreditID         = errors.New("credit ID cannot be empty")
	ErrInvalidCreditRole     = errors.New("invalid credit role")
	ErrInvalidCreditTarget   = errors.New("credit must set exactly one of series_id or volume_id")
	ErrDuplicateCredit       = errors.New("duplicate credit for person and role")
	ErrEmptyEntityID         = errors.New("entity ID cannot be empty")
	ErrInvalidEntityKind     = errors.New("invalid entity kind")
	ErrEmptyEntityName       = errors.New("entity name cannot be empty")
	ErrDuplicateVolumeEntity = errors.New("duplicate entity link for volume")
)

// NormalizeImportName computes the M1B-D4 normalized import matching key for a
// person or entity canonical name: trim Unicode whitespace, normalize to NFC,
// apply Unicode case folding, and normalize the result to NFC (ADR-0008 §6).
// Returns "" if the trimmed name is empty.
func NormalizeImportName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	folded := cases.Fold().String(norm.NFC.String(trimmed))
	return norm.NFC.String(folded)
}

// CreditRole identifies a creator's role on a Series or Volume (ADR-0008 §6).
type CreditRole string

// Defined 1.0 credit roles matching 0004_catalogue.sql.
const (
	CreditRoleWriter      CreditRole = "WRITER"
	CreditRoleArtist      CreditRole = "ARTIST"
	CreditRolePenciller   CreditRole = "PENCILLER"
	CreditRoleInker       CreditRole = "INKER"
	CreditRoleColorist    CreditRole = "COLORIST"
	CreditRoleLetterer    CreditRole = "LETTERER"
	CreditRoleCoverArtist CreditRole = "COVER_ARTIST"
	CreditRoleEditor      CreditRole = "EDITOR"
	CreditRoleTranslator  CreditRole = "TRANSLATOR"
	CreditRoleAuthor      CreditRole = "AUTHOR"
)

// ValidCreditRoles returns all 10 defined credit roles.
func ValidCreditRoles() []CreditRole {
	return []CreditRole{
		CreditRoleWriter,
		CreditRoleArtist,
		CreditRolePenciller,
		CreditRoleInker,
		CreditRoleColorist,
		CreditRoleLetterer,
		CreditRoleCoverArtist,
		CreditRoleEditor,
		CreditRoleTranslator,
		CreditRoleAuthor,
	}
}

// IsValid reports whether r is a recognized CreditRole.
func (r CreditRole) IsValid() bool {
	return slices.Contains(ValidCreditRoles(), r)
}

// EntityKind identifies the type of a non-creator narrative entity (ADR-0008 §6).
type EntityKind string

// Defined 1.0 entity kinds matching 0004_catalogue.sql.
const (
	EntityKindCharacter EntityKind = "CHARACTER"
	EntityKindTeam      EntityKind = "TEAM"
	EntityKindLocation  EntityKind = "LOCATION"
)

// ValidEntityKinds returns all 3 defined entity kinds.
func ValidEntityKinds() []EntityKind {
	return []EntityKind{
		EntityKindCharacter,
		EntityKindTeam,
		EntityKindLocation,
	}
}

// IsValid reports whether k is a recognized EntityKind.
func (k EntityKind) IsValid() bool {
	return slices.Contains(ValidEntityKinds(), k)
}

// Person represents a creator or contributor (ADR-0008 §6, M1B-D4).
// Name holds the preserved display name; ImportKey holds the normalized matching
// key for name-only imports (nil for separately curated same-name identities).
type Person struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	ImportKey     *string        `json:"import_key,omitempty"`
	NameNative    *string        `json:"name_native,omitempty"`
	NameRomanized *string        `json:"name_romanized,omitempty"`
	Aliases       []string       `json:"aliases"`
	ExternalIDs   map[string]any `json:"external_ids"`
	LockedFields  FieldLocks     `json:"locked_fields"`
}

// Validate checks Person invariants aligned with the database schema.
func (p *Person) Validate() error {
	if strings.TrimSpace(p.ID) == "" {
		return ErrEmptyPersonID
	}
	if strings.TrimSpace(p.Name) == "" {
		return ErrEmptyPersonName
	}
	if p.ImportKey != nil && strings.TrimSpace(*p.ImportKey) == "" {
		return ErrEmptyImportKey
	}
	return nil
}

// Credit links a Person to either a Series or a Volume with a CreditRole (ADR-0008 §6).
type Credit struct {
	ID           string     `json:"id"`
	PersonID     string     `json:"person_id"`
	SeriesID     *string    `json:"series_id,omitempty"`
	VolumeID     *string    `json:"volume_id,omitempty"`
	Role         CreditRole `json:"role"`
	Position     int        `json:"position"`
	LockedFields FieldLocks `json:"locked_fields"`
}

// Validate checks Credit invariants aligned with 0004_catalogue.sql.
func (c *Credit) Validate() error {
	if strings.TrimSpace(c.ID) == "" {
		return ErrEmptyCreditID
	}
	if strings.TrimSpace(c.PersonID) == "" {
		return ErrEmptyPersonID
	}
	hasSeries := c.SeriesID != nil
	hasVolume := c.VolumeID != nil
	if hasSeries == hasVolume {
		return ErrInvalidCreditTarget
	}
	if hasSeries && strings.TrimSpace(*c.SeriesID) == "" {
		return ErrEmptySeriesID
	}
	if hasVolume && strings.TrimSpace(*c.VolumeID) == "" {
		return ErrEmptyVolumeID
	}
	if !c.Role.IsValid() {
		return ErrInvalidCreditRole
	}
	return nil
}

// ValidateSeriesCredits verifies that every credit targets seriesID with no duplicate
// (PersonID, Role) entries.
func ValidateSeriesCredits(seriesID string, credits []Credit) error {
	if strings.TrimSpace(seriesID) == "" {
		return ErrEmptySeriesID
	}
	return validateCreditsForTarget(credits, func(c *Credit) error {
		if c.VolumeID != nil || c.SeriesID == nil || *c.SeriesID != seriesID {
			return fmt.Errorf("credit %q target does not match series %q", c.ID, seriesID)
		}
		return nil
	})
}

// ValidateVolumeCredits verifies that every credit targets volumeID with no duplicate
// (PersonID, Role) entries.
func ValidateVolumeCredits(volumeID string, credits []Credit) error {
	if strings.TrimSpace(volumeID) == "" {
		return ErrEmptyVolumeID
	}
	return validateCreditsForTarget(credits, func(c *Credit) error {
		if c.SeriesID != nil || c.VolumeID == nil || *c.VolumeID != volumeID {
			return fmt.Errorf("credit %q target does not match volume %q", c.ID, volumeID)
		}
		return nil
	})
}

func validateCreditsForTarget(credits []Credit, checkTarget func(*Credit) error) error {
	type creditKey struct {
		personID string
		role     CreditRole
	}
	seen := make(map[creditKey]struct{}, len(credits))
	for i := range credits {
		c := &credits[i]
		if err := c.Validate(); err != nil {
			return err
		}
		if err := checkTarget(c); err != nil {
			return err
		}
		key := creditKey{personID: c.PersonID, role: c.Role}
		if _, dup := seen[key]; dup {
			return fmt.Errorf("%w: (%s, %s)", ErrDuplicateCredit, c.PersonID, c.Role)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// Entity represents a character, team, or location (ADR-0008 §6, M1B-D4).
// Name holds the preserved display name; ImportKey holds the normalized matching
// key scoped by Kind for name-only imports (nil for separately curated identities).
type Entity struct {
	ID           string         `json:"id"`
	Kind         EntityKind     `json:"kind"`
	Name         string         `json:"name"`
	ImportKey    *string        `json:"import_key,omitempty"`
	Aliases      []string       `json:"aliases"`
	ExternalIDs  map[string]any `json:"external_ids"`
	LockedFields FieldLocks     `json:"locked_fields"`
}

// Validate checks Entity invariants aligned with the database schema.
func (e *Entity) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return ErrEmptyEntityID
	}
	if !e.Kind.IsValid() {
		return ErrInvalidEntityKind
	}
	if strings.TrimSpace(e.Name) == "" {
		return ErrEmptyEntityName
	}
	if e.ImportKey != nil && strings.TrimSpace(*e.ImportKey) == "" {
		return ErrEmptyImportKey
	}
	return nil
}

// VolumeEntity links a Volume to an Entity (ADR-0008 §6).
type VolumeEntity struct {
	VolumeID     string     `json:"volume_id"`
	EntityID     string     `json:"entity_id"`
	LockedFields FieldLocks `json:"locked_fields"`
}

// Validate checks VolumeEntity invariants.
func (ve *VolumeEntity) Validate() error {
	if strings.TrimSpace(ve.VolumeID) == "" {
		return ErrEmptyVolumeID
	}
	if strings.TrimSpace(ve.EntityID) == "" {
		return ErrEmptyEntityID
	}
	return nil
}

// ValidateVolumeEntities verifies that all links belong to volumeID with distinct EntityIDs.
func ValidateVolumeEntities(volumeID string, links []VolumeEntity) error {
	if strings.TrimSpace(volumeID) == "" {
		return ErrEmptyVolumeID
	}
	seen := make(map[string]struct{}, len(links))
	for i := range links {
		ve := &links[i]
		if err := ve.Validate(); err != nil {
			return err
		}
		if ve.VolumeID != volumeID {
			return fmt.Errorf("volume_entity volume_id %q does not match volume %q", ve.VolumeID, volumeID)
		}
		if _, dup := seen[ve.EntityID]; dup {
			return fmt.Errorf("%w: %s", ErrDuplicateVolumeEntity, ve.EntityID)
		}
		seen[ve.EntityID] = struct{}{}
	}
	return nil
}
