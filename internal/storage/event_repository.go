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
}
