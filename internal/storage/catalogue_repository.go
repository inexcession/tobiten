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

const (
	// DefaultCataloguePageLimit is the default page size when series/volume filter Limit is zero.
	DefaultCataloguePageLimit = 50
	// MaxCataloguePageLimit is the hard maximum page size enforced on paged catalogue reads (ADR-0020 §7).
	MaxCataloguePageLimit = 200
	// DefaultFolderBrowseLimit is the default page size when FolderBrowseFilter.Limit is zero.
	DefaultFolderBrowseLimit = 50
	// MaxFolderBrowseLimit is the hard maximum page size enforced on folder-browse reads (ADR-0020 §7).
	MaxFolderBrowseLimit = 200
)

// SeriesPageResult holds one server-paginated slice of visible series together
// with the exact total count computed in the same query snapshot (ADR-0020 §7).
type SeriesPageResult struct {
	Items      []domain.Series
	TotalCount int64
	Limit      int
	Offset     int
}

// VolumePageResult holds one server-paginated slice of visible volumes together
// with the exact total count computed in the same query snapshot (ADR-0020 §7).
type VolumePageResult struct {
	Items      []domain.Volume
	TotalCount int64
	Limit      int
	Offset     int
}

// FolderEntryKind distinguishes immediate child folders from immediate child files
// in a library folder-browse listing (ADR-0019 §4).
type FolderEntryKind string

const (
	FolderEntryKindFolder FolderEntryKind = "FOLDER"
	FolderEntryKindFile   FolderEntryKind = "FILE"
)

// FolderBrowseFilter specifies the target library, root-relative folder bytes, and
// pagination parameters for CatalogueRepository.BrowseFolder (ADR-0019 §4, ADR-0020 §7).
// FolderPathBytes may be nil or empty to browse immediate children of the library's roots;
// non-empty values must pass domain.ValidateRelativePathBytes.
type FolderBrowseFilter struct {
	LibraryID       string
	FolderPathBytes []byte
	Limit           int
	Offset          int
}

// FolderEntry represents one immediate child folder or available file in a library's
// folder-browse listing (ADR-0019 §4).
type FolderEntry struct {
	Kind          FolderEntryKind
	Name          domain.PathIdentity
	RelPath       domain.PathIdentity
	AssetID       string
	VolumeID      string
	LibraryRootID string
	Format        domain.MediaFormat
	FileSize      int64
	PageCount     int
}

// FolderBrowseResult holds one server-paginated slice of immediate child folders and files
// across all roots of a library, respecting label enforcement and asset availability.
type FolderBrowseResult struct {
	LibraryID  string
	FolderPath domain.PathIdentity
	Entries    []FolderEntry
	TotalCount int64
	Limit      int
	Offset     int
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
	DigestKind      domain.DigestKind
	Digest          string
	PageCount       int
	CoverPageIndex  int
	SuppressedPages []int
	MissingSince    *time.Time
	RetiredAt       *time.Time
}

// SeriesCoverCursor identifies a volume's position in a series' FIRST sort
// order so batched cover lookups resume without ordinal drift across snapshots.
type SeriesCoverCursor struct {
	Position *float64
	Number   *float64
	VolumeID string
}

// SeriesCoverCandidate pairs one visible, non-trashed series member volume with its
// FIRST sort cursor and preference-ranked, visible, available, non-trashed asset files
// (ADR-0006 §5, ADR-0010 §2).
type SeriesCoverCandidate struct {
	VolumeID string
	Cursor   SeriesCoverCursor
	Files    []AuthorizedAssetFile
}

// CatalogueRepository defines label-enforced read and count operations across series,
// volumes, media assets, authorized file projections, and per-user reading progress (ADR-0006 §5).
// Every method must apply the repository LabelPredicate and fail closed on missing principals
// or predicate errors:
//   - GetVolumeByID and ListVolumes populate each volume's authorized Memberships and available
//     Assets (excluding missing/retired manifestations) and clear PreferredAssetID when the
//     referenced asset is not authorized or not available for scope.
//   - GetAssetByID, ListAssetsByVolume, CountAssetsByVolume, GetAuthorizedAssetFile, and
//     SelectPreferredAssetFileForVolume operate on available manifestations only
//     (missing_since IS NULL AND retired_at IS NULL).
//   - SelectPreferredAssetFileForVolume always excludes trashed items (marked_for_deletion),
//     even when scope.IncludeTrashed is true for an admin.
//   - Reading progress methods bind ownership strictly to the exact scope.Principal.User.ID
//     in addition to enforcing volume visibility.
//   - BrowseFolder lists immediate child folders (merged across all roots of the library by
//     exact root-relative bytes) and available file manifestations (missing_since IS NULL AND
//     retired_at IS NULL) under FolderPathBytes, applying LabelPredicate and server-side
//     pagination capped at MaxFolderBrowseLimit.
type CatalogueRepository interface {
	GetSeriesByID(ctx context.Context, scope CatalogueReadScope, id string) (*domain.Series, error)
	ListSeries(ctx context.Context, scope CatalogueReadScope, filter SeriesReadFilter) ([]domain.Series, error)
	ListSeriesPaged(ctx context.Context, scope CatalogueReadScope, filter SeriesReadFilter) (*SeriesPageResult, error)
	CountSeries(ctx context.Context, scope CatalogueReadScope, filter SeriesReadFilter) (int64, error)

	GetVolumeByID(ctx context.Context, scope CatalogueReadScope, id string) (*domain.Volume, error)
	ListVolumes(ctx context.Context, scope CatalogueReadScope, filter VolumeReadFilter) ([]domain.Volume, error)
	ListVolumesPaged(ctx context.Context, scope CatalogueReadScope, filter VolumeReadFilter) (*VolumePageResult, error)
	CountVolumes(ctx context.Context, scope CatalogueReadScope, filter VolumeReadFilter) (int64, error)

	GetAssetByID(ctx context.Context, scope CatalogueReadScope, id string) (*domain.MediaAsset, error)
	ListAssetsByVolume(ctx context.Context, scope CatalogueReadScope, volumeID string) ([]domain.MediaAsset, error)
	CountAssetsByVolume(ctx context.Context, scope CatalogueReadScope, volumeID string) (int64, error)

	GetAuthorizedAssetFile(ctx context.Context, scope CatalogueReadScope, assetID string) (*AuthorizedAssetFile, error)
	SelectPreferredAssetFileForVolume(ctx context.Context, scope CatalogueReadScope, volumeID string) (*AuthorizedAssetFile, error)
	ListAuthorizedVolumeAssetFiles(ctx context.Context, scope CatalogueReadScope, volumeID string) ([]AuthorizedAssetFile, error)
	ListSeriesCoverCandidates(ctx context.Context, scope CatalogueReadScope, seriesID string, limit int, after *SeriesCoverCursor) ([]SeriesCoverCandidate, error)

	GetReadingProgress(ctx context.Context, scope CatalogueReadScope, volumeID string) (*domain.ReadingProgress, error)
	ListReadingProgress(ctx context.Context, scope CatalogueReadScope, filter ReadingProgressReadFilter) ([]domain.ReadingProgress, error)
	CountReadingProgress(ctx context.Context, scope CatalogueReadScope, filter ReadingProgressReadFilter) (int64, error)

	BrowseFolder(ctx context.Context, scope CatalogueReadScope, filter FolderBrowseFilter) (*FolderBrowseResult, error)
}
