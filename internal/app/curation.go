package app

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// CoverRendition contains rendered cover image bytes and metadata (ADR-0010 §1).
type CoverRendition struct {
	TargetID    string `json:"target_id"`
	ContentType string `json:"content_type"`
	Data        []byte `json:"data,omitempty"`
	ETag        string `json:"etag"`
}

// HidePageRequest defines parameters for suppressing an ad/credit slide (ADR-0010 §2).
type HidePageRequest struct {
	AssetID    string `json:"asset_id"`
	PageIndex  int    `json:"page_index"`
	Suppressed bool   `json:"suppressed"`
}

// StageTrashRequest defines parameters to stage an item for two-stage deletion (ADR-0010 §3).
type StageTrashRequest struct {
	ItemType domain.ItemType `json:"item_type"` // SERIES, VOLUME, ASSET
	ItemID   string          `json:"item_id"`
}

// TrashItem represents an item currently staged with SERVER label 'marked_for_deletion' (ADR-0010 §3).
type TrashItem struct {
	ID       string          `json:"id"`
	ItemType domain.ItemType `json:"item_type"`
	Title    string          `json:"title"`
	TaggedAt time.Time       `json:"tagged_at"`
}

// PurgeTrashRequest defines administrator confirmation for permanent disk purge (ADR-0010 §3).
type PurgeTrashRequest struct {
	ItemIDs []string `json:"item_ids"`
}

// CurationService defines use-cases for covers, ad suppression, and two-stage deletion.
type CurationService interface {
	GetCover(ctx context.Context, principal *domain.Principal, targetID string) (*CoverRendition, error)
	HidePage(ctx context.Context, principal *domain.Principal, req HidePageRequest) error
	StageTrash(ctx context.Context, principal *domain.Principal, req StageTrashRequest) error
	ListTrash(ctx context.Context, principal *domain.Principal) ([]TrashItem, error)
	RestoreTrash(ctx context.Context, principal *domain.Principal, itemID string) error
	PurgeTrash(ctx context.Context, principal *domain.Principal, req PurgeTrashRequest) (string, error)
}
