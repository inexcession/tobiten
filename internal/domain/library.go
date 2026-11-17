package domain

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// Common domain errors for libraries and library roots.
var (
	ErrEmptyLibraryName             = errors.New("library name cannot be empty")
	ErrInvalidLibraryMode           = errors.New("invalid library mode")
	ErrVirtualLibraryWithIntake     = errors.New("virtual library must not have an intake path")
	ErrManagedLibraryRequiresIntake = errors.New("managed library requires an intake path")
	ErrInvalidPathIdentity          = errors.New("path identity display and match keys must match raw path bytes")
	ErrEmptyResolverChain           = errors.New("resolver chain cannot be empty")
	ErrInvalidResolverName          = errors.New("invalid resolver name")
	ErrDuplicateResolverName        = errors.New("duplicate resolver name in chain")
	ErrInvalidMetadataPrecedence    = errors.New("invalid metadata precedence")
	ErrInvalidLibraryRootState      = errors.New("invalid library root state")
)

// LibraryMode identifies the operational mode of a library (ADR-0002).
type LibraryMode string

const (
	LibraryModeVirtual LibraryMode = "VIRTUAL"
	LibraryModeManaged LibraryMode = "MANAGED"
)

// ValidLibraryModes returns all supported library operational modes.
func ValidLibraryModes() []LibraryMode {
	return []LibraryMode{
		LibraryModeVirtual,
		LibraryModeManaged,
	}
}

// IsValid reports whether m is a recognized LibraryMode.
func (m LibraryMode) IsValid() bool {
	return slices.Contains(ValidLibraryModes(), m)
}

// ParseLibraryMode parses a mode string (such as config TOBITEN_LIBRARY_MODE) into a LibraryMode.
func ParseLibraryMode(raw string) (LibraryMode, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case string(LibraryModeVirtual):
		return LibraryModeVirtual, nil
	case string(LibraryModeManaged):
		return LibraryModeManaged, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidLibraryMode, raw)
	}
}

// ResolverName identifies a series resolver stage in a library's resolver chain (ADR-0019 §1).
type ResolverName string

const (
	ResolverMetadata ResolverName = "metadata"
	ResolverFolder   ResolverName = "folder"
	ResolverFilename ResolverName = "filename"
)

// ValidResolverNames returns all supported series resolver names.
func ValidResolverNames() []ResolverName {
	return []ResolverName{
		ResolverMetadata,
		ResolverFolder,
		ResolverFilename,
	}
}

// IsValid reports whether r is a recognized ResolverName.
func (r ResolverName) IsValid() bool {
	return slices.Contains(ValidResolverNames(), r)
}

// DefaultResolverChain returns a fresh copy of the default per-library resolver chain
// (metadata -> folder -> filename, ADR-0019 §1).
func DefaultResolverChain() []ResolverName {
	return []ResolverName{
		ResolverMetadata,
		ResolverFolder,
		ResolverFilename,
	}
}

// ValidateResolverChain verifies that chain is non-empty and contains only unique, recognized resolvers.
func ValidateResolverChain(chain []ResolverName) error {
	if len(chain) == 0 {
		return ErrEmptyResolverChain
	}
	seen := make(map[ResolverName]struct{}, len(chain))
	for _, r := range chain {
		if !r.IsValid() {
			return fmt.Errorf("%w: %q", ErrInvalidResolverName, r)
		}
		if _, dup := seen[r]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateResolverName, r)
		}
		seen[r] = struct{}{}
	}
	return nil
}

// MetadataPrecedence controls whether embedded file metadata or online scrapers win for unlocked fields (ADR-0007).
type MetadataPrecedence string

const (
	MetadataPrecedenceEmbeddedOverOnline MetadataPrecedence = "EMBEDDED_OVER_ONLINE"
	MetadataPrecedenceOnlineOverEmbedded MetadataPrecedence = "ONLINE_OVER_EMBEDDED"
)

// ValidMetadataPrecedences returns all supported metadata precedence modes.
func ValidMetadataPrecedences() []MetadataPrecedence {
	return []MetadataPrecedence{
		MetadataPrecedenceEmbeddedOverOnline,
		MetadataPrecedenceOnlineOverEmbedded,
	}
}

// IsValid reports whether p is a recognized MetadataPrecedence.
func (p MetadataPrecedence) IsValid() bool {
	return slices.Contains(ValidMetadataPrecedences(), p)
}

// LibraryRootState tracks whether a root passed its latest availability pre-check (ADR-0019 §5).
type LibraryRootState string

const (
	LibraryRootStateOnline  LibraryRootState = "ONLINE"
	LibraryRootStateOffline LibraryRootState = "OFFLINE"
)

// ValidLibraryRootStates returns all supported library root availability states.
func ValidLibraryRootStates() []LibraryRootState {
	return []LibraryRootState{
		LibraryRootStateOnline,
		LibraryRootStateOffline,
	}
}

// IsValid reports whether s is a recognized LibraryRootState.
func (s LibraryRootState) IsValid() bool {
	return slices.Contains(ValidLibraryRootStates(), s)
}

// ValidateRootPathIdentity verifies that p is a valid absolute, NUL-free root or intake
// path whose Display and Match fields match the canonical derivation from Bytes (M1-T20.1).
func ValidateRootPathIdentity(p PathIdentity) error {
	if err := ValidateRootPathBytes(p.Bytes); err != nil {
		return err
	}
	expected := NewPathIdentity(p.Bytes)
	if p.Display != expected.Display || p.Match != expected.Match {
		return ErrInvalidPathIdentity
	}
	return nil
}

// ClonePathIdentity returns a deep copy of p with its InvalidUTF8 flag normalized from Bytes.
func ClonePathIdentity(p PathIdentity) PathIdentity {
	return PathIdentity{
		Bytes:       slices.Clone(p.Bytes),
		Display:     p.Display,
		Match:       p.Match,
		InvalidUTF8: !utf8.Valid(p.Bytes),
	}
}

// Library represents a configured virtual or managed media library (ADR-0002, ADR-0019).
type Library struct {
	ID                    string             `json:"id"`
	Name                  string             `json:"name"`
	Mode                  LibraryMode        `json:"mode"`
	IntakePath            *PathIdentity      `json:"intake_path,omitempty"`
	AutoMetadataWriteback bool               `json:"auto_metadata_writeback"`
	ResolverChain         []ResolverName     `json:"resolver_chain"`
	MetadataPrecedence    MetadataPrecedence `json:"metadata_precedence"`
	DefaultCoverPolicy    CoverPolicy        `json:"default_cover_policy"`
	CreatedAt             time.Time          `json:"created_at"`
}

// Validate checks Library invariants aligned with 0004_catalogue.sql, ADR-0002, and ADR-0019.
func (l *Library) Validate() error {
	if l == nil {
		return errors.New("library cannot be nil")
	}
	if strings.TrimSpace(l.ID) == "" {
		return ErrEmptyLibraryID
	}
	if strings.TrimSpace(l.Name) == "" {
		return ErrEmptyLibraryName
	}
	if err := l.validateModeAndIntake(); err != nil {
		return err
	}
	if err := ValidateResolverChain(l.ResolverChain); err != nil {
		return err
	}
	if !l.MetadataPrecedence.IsValid() {
		return ErrInvalidMetadataPrecedence
	}
	if !l.DefaultCoverPolicy.IsValid() {
		return ErrInvalidCoverPolicy
	}
	return nil
}

func (l *Library) validateModeAndIntake() error {
	switch l.Mode {
	case LibraryModeVirtual:
		if l.IntakePath != nil {
			return ErrVirtualLibraryWithIntake
		}
		return nil
	case LibraryModeManaged:
		if l.IntakePath == nil {
			return ErrManagedLibraryRequiresIntake
		}
		return ValidateRootPathIdentity(*l.IntakePath)
	default:
		return ErrInvalidLibraryMode
	}
}

// LibraryRoot represents one absolute root directory belonging to a Library (ADR-0003, ADR-0019).
type LibraryRoot struct {
	ID            string           `json:"id"`
	LibraryID     string           `json:"library_id"`
	Path          PathIdentity     `json:"path"`
	State         LibraryRootState `json:"state"`
	LastCheckedAt *time.Time       `json:"last_checked_at,omitempty"`
	LastError     *string          `json:"last_error,omitempty"`
}

// Validate checks LibraryRoot invariants aligned with 0004_catalogue.sql, ADR-0003, and ADR-0019.
func (r *LibraryRoot) Validate() error {
	if r == nil {
		return errors.New("library root cannot be nil")
	}
	if strings.TrimSpace(r.ID) == "" {
		return ErrEmptyLibraryRootID
	}
	if strings.TrimSpace(r.LibraryID) == "" {
		return ErrEmptyLibraryID
	}
	if err := ValidateRootPathIdentity(r.Path); err != nil {
		return err
	}
	if !r.State.IsValid() {
		return ErrInvalidLibraryRootState
	}
	return nil
}
