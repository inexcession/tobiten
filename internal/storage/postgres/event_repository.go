package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/jackc/pgx/v5"
)

var _ storage.EventRepository = (*EventRepository)(nil)

// EventRepository implements storage.EventRepository backed by PostgreSQL and sqlc.
type EventRepository struct {
	q     *Queries
	tx    TxBeginner
	scope *txScope
	inTx  bool
}

// NewEventRepository creates a new EventRepository using the provided database handle
// (such as a *pgxpool.Pool or an active pgx.Tx). AppendEvent requires an active pgx.Tx
// (either passed directly or entered via WithTx).
func NewEventRepository(db DBTX) *EventRepository {
	var beginner TxBeginner
	if b, ok := db.(TxBeginner); ok {
		beginner = b
	}
	var scope *txScope
	extTx, inTx := db.(pgx.Tx)
	if inTx {
		scope = newCallerTxScope(extTx)
	}
	return &EventRepository{
		q:     New(db),
		tx:    beginner,
		scope: scope,
		inTx:  inTx,
	}
}

// InTx reports whether this repository handle is bound to an active PostgreSQL transaction.
func (r *EventRepository) InTx() bool {
	return r != nil && r.inTx
}

// WithTx executes fn inside a transaction (or nested savepoint when already inside one).
// Unconfirmed savepoint cleanup may close the caller's connection to abort the transaction.
func (r *EventRepository) WithTx(ctx context.Context, fn func(repo storage.EventRepository) error) error {
	if r == nil {
		return errors.New("event repository is required")
	}
	if fn == nil {
		return errors.New("transaction callback cannot be nil")
	}
	return runTx(ctx, r.tx, r.scope, "event", func(child *txScope) error {
		return fn(&EventRepository{q: New(child.tx), tx: child.tx, scope: child, inTx: true})
	})
}

// AppendEvent inserts an unpublished domain event into domain_events within an active
// transaction, leaving published_sequence and published_at NULL. Pool-backed or
// non-transactional repositories return storage.ErrTransactionRequired.
func (r *EventRepository) AppendEvent(ctx context.Context, event *domain.DomainEvent) error {
	if !r.InTx() {
		return storage.ErrTransactionRequired
	}
	if event == nil {
		return errors.New("domain event cannot be nil")
	}

	candidate := *event
	if len(candidate.Payload) == 0 {
		candidate.Payload = json.RawMessage("{}")
	}
	if err := candidate.ValidateUnpublished(); err != nil {
		return err
	}

	createdAt := candidate.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	} else {
		createdAt = createdAt.UTC()
	}

	row, err := r.q.AppendDomainEvent(ctx, AppendDomainEventParams{
		Kind:          string(candidate.Kind),
		Version:       int32(candidate.Version),
		AggregateType: candidate.AggregateType,
		AggregateID:   candidate.AggregateID,
		Payload:       []byte(candidate.Payload),
		CreatedAt:     createdAt,
	})
	if err != nil {
		return fmt.Errorf("appending domain event %q: %w", candidate.Kind, err)
	}

	*event = DomainEventToDomain(row)
	return nil
}

// GetEventByID retrieves a domain event by its internal BIGSERIAL row ID.
func (r *EventRepository) GetEventByID(ctx context.Context, id int64) (*domain.DomainEvent, error) {
	if id <= 0 {
		return nil, storage.ErrNotFound
	}
	row, err := r.q.GetDomainEventByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	out := DomainEventToDomain(row)
	return &out, nil
}
