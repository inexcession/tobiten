package app

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// Library represents a configured library container (ADR-0002 §1).
type Library struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Path      string    `json:"path"`
	Mode      string    `json:"mode"` // VIRTUAL or MANAGED
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Series represents a logical series containing volumes (ADR-0019).
type Series struct {
	ID        string    `json:"id"`
	LibraryID string    `json:"library_id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Volume represents a bibliographic unit with zero or more manifestations (ADR-0018 S5).
type Volume struct {
	ID         string    `json:"id"`
	LibraryID  string    `json:"library_id"`
	SeriesID   *string   `json:"series_id,omitempty"`
	Title      string    `json:"title"`
	NumberSort float64   `json:"number_sort"`
	AssetCount int       `json:"asset_count"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// SeriesFilter specifies criteria for listing series.
type SeriesFilter struct {
	LibraryID string `json:"library_id,omitempty"`
	Search    string `json:"search,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

// VolumeFilter specifies criteria for listing volumes.
type VolumeFilter struct {
	LibraryID string `json:"library_id,omitempty"`
	SeriesID  string `json:"series_id,omitempty"`
	Search    string `json:"search,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

// LibraryService defines use-cases for library catalogue, series, volumes, and scanning.
type LibraryService interface {
	ListLibraries(ctx context.Context, principal *domain.Principal) ([]Library, error)
	GetLibrary(ctx context.Context, principal *domain.Principal, id string) (*Library, error)
	ListSeries(ctx context.Context, principal *domain.Principal, filter SeriesFilter) ([]Series, error)
	GetSeries(ctx context.Context, principal *domain.Principal, id string) (*Series, error)
	ListVolumes(ctx context.Context, principal *domain.Principal, filter VolumeFilter) ([]Volume, error)
	GetVolume(ctx context.Context, principal *domain.Principal, id string) (*Volume, error)
	ScanLibrary(ctx context.Context, principal *domain.Principal, libraryID string) (string, error)
}
