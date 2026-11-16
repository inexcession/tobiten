package storage

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// EventRepository defines persistence operations for the domain_events transactional outbox (ADR-0018 S3).
type EventRepository interface {
	// WithTx executes fn inside a transaction (or savepoint when already inside one).
	WithTx(ctx context.Context, fn func(repo EventRepository) error) error

	// InTx reports whether this repository handle is bound to an active caller transaction.
	InTx() bool

	// AppendEvent inserts an unpublished domain event inside the repository's active
	// transaction, leaving published_sequence and published_at NULL.
	// Pool-backed or non-transactional repositories reject appends with ErrTransactionRequired.
	// It populates *event with the persisted row (including generated ID and CreatedAt).
	AppendEvent(ctx context.Context, event *domain.DomainEvent) error

	// GetEventByID retrieves a domain event by its internal BIGSERIAL row ID.
	// Returns ErrNotFound if no matching row exists.
	GetEventByID(ctx context.Context, id int64) (*domain.DomainEvent, error)

	// PublishUnsequencedEvents selects all committed domain_events rows where
	// published_sequence IS NULL regardless of raw id, assigns contiguous monotonic
	// published_sequence values and published_at in ascending order, and updates the
	// publication sequence watermark inside its own serialized transaction.
	// Transaction-bound repositories (InTx() == true) reject this call with
	// ErrRootRepositoryRequired.
	PublishUnsequencedEvents(ctx context.Context, publishedAt time.Time) ([]domain.DomainEvent, error)

	// GetPublishedEventBySequence retrieves a published domain event by its published_sequence.
	// Returns ErrNotFound if no matching published row exists.
	GetPublishedEventBySequence(ctx context.Context, seq int64) (*domain.DomainEvent, error)

	// GetNextPublishedEventAfter returns the earliest committed published domain event
	// with published_sequence > afterSeq. Returns ErrNotFound if none exists.
	GetNextPublishedEventAfter(ctx context.Context, afterSeq int64) (*domain.DomainEvent, error)

	// ListPublishedEventsAfter returns up to limit committed published domain events with
	// published_sequence > afterSeq ordered by published_sequence ASC.
	ListPublishedEventsAfter(ctx context.Context, afterSeq int64, limit int) ([]domain.DomainEvent, error)

	// EnsureEventSubscriber inserts sub if absent (preserving existing position/state if present)
	// and returns the persisted row. Returns ErrAlreadyExists if an existing row has a different Kind.
	EnsureEventSubscriber(ctx context.Context, sub domain.EventSubscriber) (*domain.EventSubscriber, error)

	// GetEventSubscriber retrieves a durable subscriber cursor row by ID.
	// Returns ErrNotFound if absent.
	GetEventSubscriber(ctx context.Context, id string) (*domain.EventSubscriber, error)

	// ListEventSubscribers returns durable subscriber rows ordered by ID, optionally filtered by state.
	ListEventSubscribers(ctx context.Context, state *domain.SubscriberState) ([]domain.EventSubscriber, error)

	// AdvanceEventSubscriberCursor atomically locks the subscriber row, verifies state == ACTIVE
	// and that targetSeq is the next committed published_sequence after the current position,
	// and advances position to targetSeq. If position is already >= targetSeq, it returns the
	// existing row without regressing.
	AdvanceEventSubscriberCursor(ctx context.Context, id string, targetSeq int64, now time.Time) (*domain.EventSubscriber, error)

	// UpdateEventSubscriberState updates a durable subscriber's state (ACTIVE, PAUSED, STALLED).
	UpdateEventSubscriberState(ctx context.Context, id string, state domain.SubscriberState, stalledAt *time.Time, now time.Time) (*domain.EventSubscriber, error)
}
