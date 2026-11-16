package storage

import (
	"context"

	"github.com/inexcession/tobiten/internal/domain"
)

// EventRepository defines persistence operations for the domain_events transactional outbox (ADR-0018 S3).
type EventRepository interface {
	// WithTx executes fn inside a transaction (or savepoint when already inside one).
	WithTx(ctx context.Context, fn func(repo EventRepository) error) error

	// AppendEvent inserts an unpublished domain event inside the repository's current
	// transaction or connection, leaving published_sequence and published_at NULL.
	// It populates *event with the persisted row (including generated ID and CreatedAt).
	AppendEvent(ctx context.Context, event *domain.DomainEvent) error

	// GetEventByID retrieves a domain event by its internal BIGSERIAL row ID.
	// Returns ErrNotFound if no matching row exists.
	GetEventByID(ctx context.Context, id int64) (*domain.DomainEvent, error)
}
