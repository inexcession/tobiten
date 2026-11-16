package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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

// PublishUnsequencedEvents selects all committed domain_events rows where
// published_sequence IS NULL regardless of raw id, assigns contiguous monotonic
// published_sequence values and published_at in ascending order, and updates the
// publication sequence watermark inside its own serialized transaction.
// Transaction-bound repositories (InTx() == true) reject this call with
// storage.ErrRootRepositoryRequired.
func (r *EventRepository) PublishUnsequencedEvents(ctx context.Context, publishedAt time.Time) ([]domain.DomainEvent, error) {
	if r == nil {
		return nil, errors.New("event repository is required")
	}
	if r.InTx() {
		return nil, storage.ErrRootRepositoryRequired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ts := publishedAt
	if ts.IsZero() {
		ts = time.Now().UTC()
	} else {
		ts = ts.UTC()
	}

	var published []domain.DomainEvent
	err := r.WithTx(ctx, func(txRepo storage.EventRepository) error {
		pgTxRepo, ok := txRepo.(*EventRepository)
		if !ok {
			return errors.New("unexpected event repository implementation inside transaction")
		}
		var err error
		published, err = pgTxRepo.publishUnsequencedInTx(ctx, ts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return published, nil
}

func (r *EventRepository) publishUnsequencedInTx(ctx context.Context, ts time.Time) ([]domain.DomainEvent, error) {
	lastSeq, err := r.q.LockDomainEventSequence(ctx)
	if err != nil {
		return nil, fmt.Errorf("locking domain_event_sequence: %w", err)
	}

	rows, err := r.q.PublishUnsequencedDomainEvents(ctx, PublishUnsequencedDomainEventsParams{
		BaseSequence: lastSeq,
		PublishedAt:  ts,
	})
	if err != nil {
		return nil, fmt.Errorf("publishing unsequenced domain events: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	published := make([]domain.DomainEvent, 0, len(rows))
	var maxSeq int64
	for _, row := range rows {
		ev := DomainEventToDomain(DomainEvent(row))
		if !ev.IsPublished() {
			return nil, fmt.Errorf("published domain event %d missing publication metadata", ev.ID)
		}
		if *ev.PublishedSequence > maxSeq {
			maxSeq = *ev.PublishedSequence
		}
		published = append(published, ev)
	}

	if err := r.q.UpdateDomainEventSequence(ctx, maxSeq); err != nil {
		return nil, fmt.Errorf("updating domain_event_sequence watermark to %d: %w", maxSeq, err)
	}

	return published, nil
}

func normalizeEventTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t.UTC()
}

// GetPublishedEventBySequence retrieves a published domain event by its published_sequence.
func (r *EventRepository) GetPublishedEventBySequence(ctx context.Context, seq int64) (*domain.DomainEvent, error) {
	if seq <= 0 {
		return nil, storage.ErrNotFound
	}
	row, err := r.q.GetPublishedDomainEventBySequence(ctx, pgtype.Int8{Int64: seq, Valid: true})
	if err != nil {
		return nil, mapPgError(err)
	}
	out := DomainEventToDomain(row)
	return &out, nil
}

// GetNextPublishedEventAfter returns the earliest committed published domain event
// with published_sequence > afterSeq.
func (r *EventRepository) GetNextPublishedEventAfter(ctx context.Context, afterSeq int64) (*domain.DomainEvent, error) {
	if afterSeq < 0 {
		afterSeq = 0
	}
	row, err := r.q.GetNextPublishedDomainEventAfter(ctx, pgtype.Int8{Int64: afterSeq, Valid: true})
	if err != nil {
		return nil, mapPgError(err)
	}
	out := DomainEventToDomain(row)
	return &out, nil
}

// ListPublishedEventsAfter returns up to limit committed published domain events with
// published_sequence > afterSeq ordered by published_sequence ASC.
func (r *EventRepository) ListPublishedEventsAfter(ctx context.Context, afterSeq int64, limit int) ([]domain.DomainEvent, error) {
	if afterSeq < 0 {
		return nil, domain.ErrInvalidSubscriberPosition
	}
	if limit < 1 || limit > math.MaxInt32 {
		return nil, fmt.Errorf("invalid event list limit=%d", limit)
	}
	rows, err := r.q.ListPublishedDomainEventsAfter(ctx, ListPublishedDomainEventsAfterParams{
		PublishedSequence: pgtype.Int8{Int64: afterSeq, Valid: true},
		Limit:             int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("listing published domain events: %w", err)
	}
	out := make([]domain.DomainEvent, len(rows))
	for i, row := range rows {
		out[i] = DomainEventToDomain(row)
	}
	return out, nil
}

// EnsureEventSubscriber inserts sub if absent (preserving existing position/state if present)
// and returns the persisted row.
func (r *EventRepository) EnsureEventSubscriber(ctx context.Context, sub domain.EventSubscriber) (*domain.EventSubscriber, error) {
	if r == nil {
		return nil, errors.New("event repository is required")
	}
	candidate := sub
	if candidate.State == "" {
		candidate.State = domain.SubscriberStateActive
	}
	if err := candidate.Validate(); err != nil {
		return nil, err
	}
	ts := normalizeEventTime(candidate.UpdatedAt)

	row, err := r.q.InsertEventSubscriber(ctx, InsertEventSubscriberParams{
		ID:        candidate.ID,
		Kind:      candidate.Kind,
		Position:  candidate.Position,
		State:     string(candidate.State),
		StalledAt: candidate.StalledAt,
		UpdatedAt: ts,
	})
	if err == nil {
		out := EventSubscriberToDomain(row)
		return &out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, mapPgError(err)
	}

	existing, err := r.q.GetEventSubscriberByID(ctx, candidate.ID)
	if err != nil {
		return nil, mapPgError(err)
	}
	if existing.Kind != candidate.Kind {
		return nil, fmt.Errorf("%w: event subscriber %q exists with kind %q, got %q",
			storage.ErrAlreadyExists, candidate.ID, existing.Kind, candidate.Kind)
	}
	out := EventSubscriberToDomain(existing)
	return &out, nil
}

// GetEventSubscriber retrieves a durable subscriber cursor row by ID.
func (r *EventRepository) GetEventSubscriber(ctx context.Context, id string) (*domain.EventSubscriber, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptySubscriberID
	}
	row, err := r.q.GetEventSubscriberByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	out := EventSubscriberToDomain(row)
	return &out, nil
}

// ListEventSubscribers returns durable subscriber rows ordered by ID, optionally filtered by state.
func (r *EventRepository) ListEventSubscribers(ctx context.Context, state *domain.SubscriberState) ([]domain.EventSubscriber, error) {
	var stateStr *string
	if state != nil {
		if !state.IsValid() {
			return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSubscriberState, *state)
		}
		s := string(*state)
		stateStr = &s
	}
	rows, err := r.q.ListEventSubscribers(ctx, stateStr)
	if err != nil {
		return nil, fmt.Errorf("listing event subscribers: %w", err)
	}
	out := make([]domain.EventSubscriber, len(rows))
	for i, row := range rows {
		out[i] = EventSubscriberToDomain(row)
	}
	return out, nil
}

// AdvanceEventSubscriberCursor atomically locks the subscriber row, verifies state == ACTIVE
// and that targetSeq is the next committed published_sequence after the current position,
// and advances position to targetSeq.
func (r *EventRepository) AdvanceEventSubscriberCursor(ctx context.Context, id string, targetSeq int64, now time.Time) (*domain.EventSubscriber, error) {
	if r == nil {
		return nil, errors.New("event repository is required")
	}
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptySubscriberID
	}
	if targetSeq <= 0 {
		return nil, domain.ErrInvalidPublishedSeq
	}
	ts := normalizeEventTime(now)

	var out domain.EventSubscriber
	run := func(txRepo *EventRepository) error {
		var err error
		out, err = txRepo.advanceSubscriberCursorInTx(ctx, id, targetSeq, ts)
		return err
	}
	if r.InTx() {
		if err := run(r); err != nil {
			return nil, err
		}
		return &out, nil
	}
	if err := r.WithTx(ctx, func(repo storage.EventRepository) error {
		pgTxRepo, ok := repo.(*EventRepository)
		if !ok {
			return errors.New("unexpected event repository implementation inside transaction")
		}
		return run(pgTxRepo)
	}); err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *EventRepository) advanceSubscriberCursorInTx(ctx context.Context, id string, targetSeq int64, ts time.Time) (domain.EventSubscriber, error) {
	row, err := r.q.GetEventSubscriberByIDForUpdate(ctx, id)
	if err != nil {
		return domain.EventSubscriber{}, mapPgError(err)
	}
	if row.State != string(domain.SubscriberStateActive) {
		return domain.EventSubscriber{}, fmt.Errorf("%w: subscriber %q is in state %s", domain.ErrSubscriberNotActive, id, row.State)
	}
	if row.Position >= targetSeq {
		return EventSubscriberToDomain(row), nil
	}
	nextRow, err := r.q.GetNextPublishedDomainEventAfter(ctx, pgtype.Int8{Int64: row.Position, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.EventSubscriber{}, fmt.Errorf("%w: no published event after position %d for subscriber %q",
				domain.ErrCursorSequenceMismatch, row.Position, id)
		}
		return domain.EventSubscriber{}, fmt.Errorf("checking next published event for subscriber %q: %w", id, err)
	}
	if !nextRow.PublishedSequence.Valid || nextRow.PublishedSequence.Int64 != targetSeq {
		return domain.EventSubscriber{}, fmt.Errorf("%w: subscriber %q at position %d must process sequence %d before %d",
			domain.ErrCursorSequenceMismatch, id, row.Position, nextRow.PublishedSequence.Int64, targetSeq)
	}

	updated, err := r.q.UpdateEventSubscriberCursor(ctx, UpdateEventSubscriberCursorParams{
		ID:        id,
		Position:  targetSeq,
		UpdatedAt: ts,
	})
	if err != nil {
		return domain.EventSubscriber{}, fmt.Errorf("updating cursor for subscriber %q: %w", id, err)
	}
	return EventSubscriberToDomain(updated), nil
}

// UpdateEventSubscriberState updates a durable subscriber's state (ACTIVE, PAUSED, STALLED).
func (r *EventRepository) UpdateEventSubscriberState(ctx context.Context, id string, state domain.SubscriberState, stalledAt *time.Time, now time.Time) (*domain.EventSubscriber, error) {
	if r == nil {
		return nil, errors.New("event repository is required")
	}
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptySubscriberID
	}
	if !state.IsValid() {
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSubscriberState, state)
	}
	var normalizedStalledAt *time.Time
	if stalledAt != nil {
		t := stalledAt.UTC()
		normalizedStalledAt = &t
	}
	if (state == domain.SubscriberStateStalled) != (normalizedStalledAt != nil) {
		return nil, domain.ErrInvalidStalledPair
	}
	ts := normalizeEventTime(now)

	row, err := r.q.UpdateEventSubscriberState(ctx, UpdateEventSubscriberStateParams{
		ID:        id,
		State:     string(state),
		StalledAt: normalizedStalledAt,
		UpdatedAt: ts,
	})
	if err != nil {
		return nil, mapPgError(err)
	}
	out := EventSubscriberToDomain(row)
	return &out, nil
}

// ScheduleNextEventDelivery atomically locks subscriberID's row FOR UPDATE, reads the
// next committed published event after its cursor, checks for a terminal failure since
// subscriber.updated_at, and enqueues at most one active EVENT_DELIVERY job.
func (r *EventRepository) ScheduleNextEventDelivery(ctx context.Context, subscriberID string, maxAttempts int, now time.Time) (*domain.Job, bool, error) {
	if r == nil {
		return nil, false, errors.New("event repository is required")
	}
	if strings.TrimSpace(subscriberID) == "" {
		return nil, false, domain.ErrEmptySubscriberID
	}
	if maxAttempts < 1 || maxAttempts > math.MaxInt32 {
		return nil, false, domain.ErrInvalidJobAttempts
	}
	ts := normalizeEventTime(now)

	var scheduled *domain.Job
	var created bool
	run := func(txRepo *EventRepository) error {
		var err error
		scheduled, created, err = txRepo.scheduleNextEventDeliveryInTx(ctx, subscriberID, maxAttempts, ts)
		return err
	}
	if r.InTx() {
		if err := run(r); err != nil {
			return nil, false, err
		}
		return scheduled, created, nil
	}
	if err := r.WithTx(ctx, func(repo storage.EventRepository) error {
		pgTxRepo, ok := repo.(*EventRepository)
		if !ok {
			return errors.New("unexpected event repository implementation inside transaction")
		}
		return run(pgTxRepo)
	}); err != nil {
		return nil, false, err
	}
	return scheduled, created, nil
}

func (r *EventRepository) scheduleNextEventDeliveryInTx(
	ctx context.Context,
	subscriberID string,
	maxAttempts int,
	ts time.Time,
) (*domain.Job, bool, error) {
	subRow, err := r.q.GetEventSubscriberByIDForUpdate(ctx, subscriberID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, mapPgError(err)
	}
	if subRow.State != string(domain.SubscriberStateActive) {
		return nil, false, nil
	}

	nextEvRow, err := r.q.GetNextPublishedDomainEventAfter(ctx, pgtype.Int8{Int64: subRow.Position, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("querying next published event after %d for subscriber %q: %w", subRow.Position, subscriberID, err)
	}
	nextEv := DomainEventToDomain(nextEvRow)
	seq, ok := nextEv.Cursor()
	if !ok {
		return nil, false, nil
	}

	failed, err := r.q.HasFailedEventDeliveryJobSince(ctx, HasFailedEventDeliveryJobSinceParams{
		SubscriberID:      subscriberID,
		PublishedSequence: seq,
		Since:             subRow.UpdatedAt.UTC(),
	})
	if err != nil {
		return nil, false, fmt.Errorf("checking failed delivery for subscriber %q sequence %d: %w", subscriberID, seq, err)
	}
	if failed {
		return nil, false, nil
	}

	dedupeKey, err := domain.EventDeliveryDedupeKey(subscriberID)
	if err != nil {
		return nil, false, err
	}
	existing, err := r.q.GetActiveJobByDedupeKey(ctx, &dedupeKey)
	if err == nil {
		out := JobToDomain(existing)
		return &out, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("checking active delivery job for subscriber %q: %w", subscriberID, err)
	}

	payloadBytes, err := json.Marshal(domain.EventDeliveryPayloadV1{
		SubscriberID:      subscriberID,
		PublishedSequence: seq,
		EventID:           nextEv.ID,
	})
	if err != nil {
		return nil, false, fmt.Errorf("marshaling delivery payload for subscriber %q sequence %d: %w", subscriberID, seq, err)
	}

	candidate, err := prepareJobForEnqueue(&domain.Job{
		Kind:        domain.JobKindEventDelivery,
		Payload:     payloadBytes,
		DedupeKey:   &dedupeKey,
		MaxAttempts: maxAttempts,
		RunAfter:    ts,
		CreatedAt:   ts,
		UpdatedAt:   ts,
	})
	if err != nil {
		return nil, false, err
	}

	row, err := r.q.EnqueueJob(ctx, EnqueueJobParams{
		ID:          candidate.ID,
		Kind:        string(candidate.Kind),
		Payload:     []byte(candidate.Payload),
		Checkpoint:  []byte(candidate.Checkpoint),
		MaxAttempts: int32(candidate.MaxAttempts),
		CreatedBy:   candidate.CreatedBy,
		DedupeKey:   candidate.DedupeKey,
		RunAfter:    candidate.RunAfter,
		CreatedAt:   candidate.CreatedAt,
		UpdatedAt:   candidate.UpdatedAt,
	})
	if err == nil {
		out := JobToDomain(row)
		return &out, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, mapJobWriteError(err)
	}

	// An active job with the canonical dedupe key was inserted outside this lock; inspect once
	// without looping so a concurrent terminal failure cannot be bypassed.
	activeRow, activeErr := r.q.GetActiveJobByDedupeKey(ctx, &dedupeKey)
	if activeErr == nil {
		out := JobToDomain(activeRow)
		return &out, false, nil
	}
	if errors.Is(activeErr, pgx.ErrNoRows) {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("looking up active delivery job for subscriber %q: %w", subscriberID, activeErr)
}
