package app

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// Locator represents a typed reading position modeled on Readium Locator (ADR-0018 S4).
type Locator struct {
	Type        string  `json:"type"` // "page" or "text"
	Page        int     `json:"page,omitempty"`
	PageCount   int     `json:"page_count,omitempty"`
	AssetID     string  `json:"asset_id,omitempty"`
	AssetDigest string  `json:"asset_digest,omitempty"`
	Href        string  `json:"href,omitempty"`
	Progression float64 `json:"progression,omitempty"`
	CFI         *string `json:"cfi,omitempty"`
}

// ReadingProgress represents the recorded reading state for a (user, volume) pair (ADR-0018 S4).
type ReadingProgress struct {
	UserID           string    `json:"user_id"`
	VolumeID         string    `json:"volume_id"`
	Completed        bool      `json:"completed"`
	TotalProgression float64   `json:"total_progression"`
	Locator          Locator   `json:"locator"`
	Device           string    `json:"device,omitempty"`
	Source           string    `json:"source,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// UpdateProgressRequest defines parameters for updating reading progress.
type UpdateProgressRequest struct {
	VolumeID         string  `json:"volume_id"`
	Completed        *bool   `json:"completed,omitempty"`
	TotalProgression float64 `json:"total_progression"`
	Locator          Locator `json:"locator"`
	Device           string  `json:"device,omitempty"`
	Source           string  `json:"source,omitempty"`
}

// ReadingService defines use-cases for locator-based reading progress.
type ReadingService interface {
	GetProgress(ctx context.Context, principal *domain.Principal, volumeID string) (*ReadingProgress, error)
	UpdateProgress(ctx context.Context, principal *domain.Principal, req UpdateProgressRequest) (*ReadingProgress, error)
	MarkAsRead(ctx context.Context, principal *domain.Principal, volumeID string) (*ReadingProgress, error)
	MarkAsUnread(ctx context.Context, principal *domain.Principal, volumeID string) error
}
