// Package events implements the transactional domain-event outbox (ADR-0018 S3).
package events

import (
	"context"
	"errors"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

// Common errors returned by the transactional outbox.
var (
	ErrNilOutboxAppender = errors.New("outbox appender cannot be nil")
	ErrNilDomainEvent    = errors.New("domain event cannot be nil")
)

// Appender persists an unpublished domain event inside the caller's active transaction.
// storage.EventRepository satisfies this interface.
type Appender interface {
	InTx() bool
	AppendEvent(ctx context.Context, event *domain.DomainEvent) error
}

// OutboxOption configures an Outbox instance.
type OutboxOption func(*Outbox)

// WithClock overrides the timestamp source used when an appended event has a zero CreatedAt.
func WithClock(now func() time.Time) OutboxOption {
	return func(o *Outbox) {
		if now != nil {
			o.now = now
		}
	}
}

// Outbox validates and appends unpublished domain events inside a caller's transaction
// without assigning published_sequence or published_at (ADR-0018 S3).
type Outbox struct {
	now func() time.Time
}

// NewOutbox constructs an Outbox with the provided options.
func NewOutbox(opts ...OutboxOption) *Outbox {
	o := &Outbox{
		now: time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return o
}

// Append validates an unpublished domain event and writes it through appender.
// Callers must pass an appender bound to an active database transaction (InTx() == true)
// so the event commits or rolls back atomically with the caller's state change.
func (o *Outbox) Append(ctx context.Context, appender Appender, event *domain.DomainEvent) error {
	if appender == nil {
		return ErrNilOutboxAppender
	}
	if !appender.InTx() {
		return storage.ErrTransactionRequired
	}
	if event == nil {
		return ErrNilDomainEvent
	}

	candidate := *event
	if candidate.CreatedAt.IsZero() {
		nowFn := time.Now
		if o != nil && o.now != nil {
			nowFn = o.now
		}
		candidate.CreatedAt = nowFn().UTC()
	} else {
		candidate.CreatedAt = candidate.CreatedAt.UTC()
	}
	if err := candidate.ValidateUnpublished(); err != nil {
		return err
	}

	if err := appender.AppendEvent(ctx, &candidate); err != nil {
		return err
	}
	*event = candidate
	return nil
}

// AppendPayload builds a versioned domain event and appends it through appender.
func (o *Outbox) AppendPayload(
	ctx context.Context,
	appender Appender,
	kind domain.EventKind,
	version int,
	aggregateType, aggregateID string,
	payload any,
) (*domain.DomainEvent, error) {
	nowFn := time.Now
	if o != nil && o.now != nil {
		nowFn = o.now
	}
	ev, err := domain.NewDomainEvent(kind, version, aggregateType, aggregateID, payload, nowFn().UTC())
	if err != nil {
		return nil, err
	}
	if err := o.Append(ctx, appender, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}

// AppendJobLifecycle builds a version 1 job lifecycle event and appends it through appender.
// Internal EVENT_DELIVERY jobs return domain.ErrInternalJobEventSuppressed.
func (o *Outbox) AppendJobLifecycle(
	ctx context.Context,
	appender Appender,
	kind domain.EventKind,
	job domain.Job,
) (*domain.DomainEvent, error) {
	nowFn := time.Now
	if o != nil && o.now != nil {
		nowFn = o.now
	}
	ev, err := domain.NewJobLifecycleEvent(kind, job, nowFn().UTC())
	if err != nil {
		return nil, err
	}
	if err := o.Append(ctx, appender, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}
