package storage

import (
	"context"

	"github.com/inexcession/tobiten/internal/domain"
)

// SeriesMembershipRepository defines system-level transactional operations for
// primary and secondary series memberships (ADR-0008 §5, ADR-0019 §1).
// It is kept separate from CatalogueRepository so principal-facing reads stay
// isolated under repository-layer label enforcement (ADR-0006 §5).
type SeriesMembershipRepository interface {
	// ListMembershipsByVolume returns all memberships for volumeID, ordered with
	// the primary membership first, then by series_id. An empty slice indicates
	// a standalone volume.
	ListMembershipsByVolume(ctx context.Context, volumeID string) ([]domain.SeriesMembership, error)

	// ListMembershipsBySeries returns all memberships belonging to seriesID,
	// ordered by primary status, position, and volume_id.
	ListMembershipsBySeries(ctx context.Context, seriesID string) ([]domain.SeriesMembership, error)

	// GetMembership returns the membership for (volumeID, seriesID), or ErrNotFound.
	GetMembership(ctx context.Context, volumeID, seriesID string) (*domain.SeriesMembership, error)

	// SetVolumeMemberships replaces the full membership set of volumeID inside a
	// deferred-constraint transaction. An empty slice detaches the volume to
	// standalone (zero memberships). Non-empty slices must contain exactly one
	// primary membership.
	SetVolumeMemberships(ctx context.Context, volumeID string, memberships []domain.SeriesMembership) error

	// DetachVolumeToStandalone removes all series memberships for volumeID inside
	// a deferred-constraint transaction, leaving the volume standalone.
	DetachVolumeToStandalone(ctx context.Context, volumeID string) error

	// PromotePrimary atomically promotes an existing membership (volumeID, seriesID)
	// to primary and demotes the prior primary within a single deferred-constraint
	// transaction. Returns ErrNotFound if the volume or membership does not exist.
	PromotePrimary(ctx context.Context, volumeID, seriesID string) error

	// SyncResolverMemberships reconciles automatically resolved memberships for
	// volumeID against existing rows while preserving MANUAL and locked
	// memberships and locked fields. Desired memberships must not use MANUAL source
	// or supply locked_fields.
	SyncResolverMemberships(ctx context.Context, volumeID string, desired []domain.SeriesMembership) error
}
