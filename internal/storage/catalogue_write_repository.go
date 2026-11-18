package storage

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// CatalogueWriteRepository defines system-level persistence operations for series,
// volumes, media assets, and multi-digest history (ADR-0003, ADR-0008, ADR-0018 S2/S5).
// It is kept separate from CatalogueRepository so every principal-facing read stays
// isolated under repository-layer label enforcement (ADR-0006 §5).
type CatalogueWriteRepository interface {
	// Series
	CreateSeries(ctx context.Context, series *domain.Series) error
	GetSeriesByID(ctx context.Context, id string) (*domain.Series, error)
	UpdateSeries(ctx context.Context, series *domain.Series) error

	// Volumes
	CreateVolume(ctx context.Context, volume *domain.Volume) error
	GetVolumeByID(ctx context.Context, id string) (*domain.Volume, error)
	UpdateVolume(ctx context.Context, volume *domain.Volume) error

	// Media assets
	CreateAsset(ctx context.Context, asset *domain.MediaAsset) error
	GetAssetByID(ctx context.Context, id string) (*domain.MediaAsset, error)
	UpdateAsset(ctx context.Context, asset *domain.MediaAsset) error
	UpdateAssetLocation(ctx context.Context, assetID, libraryRootID string, relPath domain.PathIdentity, updatedAt time.Time) error
	ListAssetsByRoot(ctx context.Context, libraryRootID string) ([]domain.MediaAsset, error)

	// Asset digests
	RecordAssetDigest(ctx context.Context, digest *domain.AssetDigest) error
	FindAssetsByDigest(ctx context.Context, libraryID string, kind domain.DigestKind, value string) ([]domain.MediaAsset, error)
}
