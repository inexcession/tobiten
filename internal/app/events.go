package app

import (
	"context"
	"encoding/json"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// EventStreamFilter configures optional filtering for the authenticated SSE event stream (ADR-0013 §3).
type EventStreamFilter struct {
	JobID string
}

// PublicEvent is the public versioned envelope serialized in SSE data frames for
// committed domain_events rows, omitting internal BIGSERIAL row IDs (ADR-0013 §3, ADR-0018 S3).
type PublicEvent struct {
	Sequence      int64            `json:"sequence"`
	PublishedAt   time.Time        `json:"published_at"`
	Kind          domain.EventKind `json:"kind"`
	Version       int              `json:"version"`
	AggregateType string           `json:"aggregate_type"`
	AggregateID   string           `json:"aggregate_id"`
	Payload       json.RawMessage  `json:"payload"`
	CreatedAt     time.Time        `json:"created_at"`
}

// StreamEvent represents a single SSE frame emitted by EventService.Stream.
// ID is the committed published_sequence string for durable events and empty for
// ephemeral progress ticks (which must carry no SSE ID per ADR-0018 S3).
type StreamEvent struct {
	ID   string
	Name string
	Data json.RawMessage
}

// EventService streams permitted real-time and committed domain events to an authenticated principal.
type EventService interface {
	Stream(
		ctx context.Context,
		principal *domain.Principal,
		filter EventStreamFilter,
		onAttached func() error,
		emit func(StreamEvent) error,
	) error
}
