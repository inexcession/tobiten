package storage

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// CatalogueReadScope identifies the authenticated principal and label-policy inputs
// applied to every repository-layer catalogue read and count query (ADR-0006 §5).
type CatalogueReadScope struct {
	Principal      *domain.Principal
	GroupPolicies  []domain.GroupLabelPolicy
	IncludeTrashed bool
}

// SeriesReadFilter specifies optional filtering and pagination for series read/count queries.
type SeriesReadFilter struct {
	LibraryID string
	Search    string
	Limit     int
	Offset    int
}

// VolumeReadFilter specifies optional filtering and pagination for volume read/count queries.
type VolumeReadFilter struct {
	LibraryID string
	SeriesID  string
	Search    string
	Limit     int
	Offset    int
}

// ReadingProgressReadFilter specifies optional filtering and pagination for reading-progress queries.
type ReadingProgressReadFilter struct {
	Completed *bool
	Limit     int
	Offset    int
}

// AuthorizedAssetFile holds the authorized manifestation and root path projection needed
// to stream pages, render thumbnails, or download a media asset (ADR-0003, ADR-0006 §5).
// Path projections are returned exclusively for assets authorized by the label predicate.
type AuthorizedAssetFile struct {
	AssetID         string
	VolumeID        string
	LibraryID       string
	LibraryRootID   string
	RootPathBytes   []byte
	RootPathDisplay string
	RootState       string
	RelPathBytes    []byte
	RelPathDisplay  string
	FileSize        int64
	Format          domain.MediaFormat
	PageCount       int
	CoverPageIndex  int
	SuppressedPages []int
	MissingSince    *time.Time
	RetiredAt       *time.Time
}

// CatalogueRepository defines label-enforced read and count operations across series,
// volumes, media assets, authorized file projections, and per-user reading progress (ADR-0006 §5).
// Every method must apply the repository LabelPredicate and fail closed on missing principals
// or predicate errors:
//   - GetVolumeByID and ListVolumes populate each volume's authorized Memberships and Assets
//     and clear PreferredAssetID when the referenced asset is not authorized for scope.
//   - SelectPreferredAssetFileForVolume always excludes trashed items (marked_for_deletion),
//     even when scope.IncludeTrashed is true for an admin.
//   - Reading progress methods bind ownership strictly to the exact scope.Principal.User.ID
//     in addition to enforcing volume visibility.
type CatalogueRepository interface {
	GetSeriesByID(ctx context.Context, scope CatalogueReadScope, id string) (*domain.Series, error)
	ListSeries(ctx context.Context, scope CatalogueReadScope, filter SeriesReadFilter) ([]domain.Series, error)
	CountSeries(ctx context.Context, scope CatalogueReadScope, filter SeriesReadFilter) (int64, error)

	GetVolumeByID(ctx context.Context, scope CatalogueReadScope, id string) (*domain.Volume, error)
	ListVolumes(ctx context.Context, scope CatalogueReadScope, filter VolumeReadFilter) ([]domain.Volume, error)
	CountVolumes(ctx context.Context, scope CatalogueReadScope, filter VolumeReadFilter) (int64, error)

	GetAssetByID(ctx context.Context, scope CatalogueReadScope, id string) (*domain.MediaAsset, error)
	ListAssetsByVolume(ctx context.Context, scope CatalogueReadScope, volumeID string) ([]domain.MediaAsset, error)
	CountAssetsByVolume(ctx context.Context, scope CatalogueReadScope, volumeID string) (int64, error)

	GetAuthorizedAssetFile(ctx context.Context, scope CatalogueReadScope, assetID string) (*AuthorizedAssetFile, error)
	SelectPreferredAssetFileForVolume(ctx context.Context, scope CatalogueReadScope, volumeID string) (*AuthorizedAssetFile, error)

	GetReadingProgress(ctx context.Context, scope CatalogueReadScope, volumeID string) (*domain.ReadingProgress, error)
	ListReadingProgress(ctx context.Context, scope CatalogueReadScope, filter ReadingProgressReadFilter) ([]domain.ReadingProgress, error)
	CountReadingProgress(ctx context.Context, scope CatalogueReadScope, filter ReadingProgressReadFilter) (int64, error)
}
