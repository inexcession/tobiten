package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/logging"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
)

// DefaultDeliveryPollInterval controls how often an idle DeliveryCoordinator checks
// for pending published events when no Wake signal arrived.
const DefaultDeliveryPollInterval = 200 * time.Millisecond

// Sentinel errors returned by durable subscriber delivery coordination.
var (
	ErrNilDeliveryDependencies    = errors.New("event repository and job repository are required")
	ErrInvalidSubscriber          = errors.New("invalid event subscriber registration")
	ErrDuplicateSubscriber        = errors.New("duplicate event subscriber registration")
	ErrMissingSubscriberHandler   = errors.New("event subscriber has no delivery handler")
	ErrDeliveryAlreadyRunning     = errors.New("event delivery coordinator is already running")
	ErrInvalidDeliveryInterval    = errors.New("event delivery poll interval must be non-negative")
	ErrInvalidDeliveryMaxAttempts = errors.New("event delivery max attempts must be positive")
)

// Subscriber delivers a published domain event and reports whether it was
// acknowledged, skipped by an authorized filter, or failed.
type Subscriber interface {
	ID() string
	Kind() string
	Deliver(ctx context.Context, event domain.DomainEvent) (domain.EventDeliveryOutcome, error)
}

// SubscriberSpec adapts a subscriber ID, kind, and callback into a Subscriber.
type SubscriberSpec struct {
	SubscriberID   string
	SubscriberKind string
	DeliverFn      func(ctx context.Context, event domain.DomainEvent) (domain.EventDeliveryOutcome, error)
}

// ID returns the subscriber identifier persisted in event_subscribers.id.
func (s SubscriberSpec) ID() string { return s.SubscriberID }

// Kind returns the subscriber module kind persisted in event_subscribers.kind.
func (s SubscriberSpec) Kind() string { return s.SubscriberKind }

// Deliver invokes DeliverFn for a published domain event.
func (s SubscriberSpec) Deliver(ctx context.Context, event domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
	if s.DeliverFn == nil {
		return 0, ErrMissingSubscriberHandler
	}
	return s.DeliverFn(ctx, event)
}

// SubscriberRegistrar is an optional interface implemented by modules that register
// durable event subscribers directly on a DeliveryCoordinator.
type SubscriberRegistrar interface {
	RegisterSubscribers(d *DeliveryCoordinator) error
}

// DeliveryOptions configures a DeliveryCoordinator instance.
type DeliveryOptions struct {
	Logger       *slog.Logger
	Now          func() time.Time
	PollInterval time.Duration
	MaxAttempts  int
	OnFatal      func(error)
}

type registeredSubscriber struct {
	id      string
	kind    string
	handler Subscriber
}

// DeliveryCoordinator drains committed published domain events from each active
// durable subscriber's event_subscribers.position cursor in publication-sequence
// order and dispatches them as internal EVENT_DELIVERY durable jobs (ADR-0018 S3).
type DeliveryCoordinator struct {
	events       storage.EventRepository
	jobs         storage.JobRepository
	runner       *jobs.Runner
	logger       *slog.Logger
	now          func() time.Time
	pollInterval time.Duration
	maxAttempts  int
	onFatal      func(error)

	drainMu sync.Mutex

	subMu       sync.RWMutex
	subscribers map[string]registeredSubscriber

	loop wakeLoop
}

// NewDeliveryCoordinator constructs a DeliveryCoordinator over eventsRepo, jobsRepo, and runner.
func NewDeliveryCoordinator(
	eventsRepo storage.EventRepository,
	jobsRepo storage.JobRepository,
	runner *jobs.Runner,
	opts DeliveryOptions,
) *DeliveryCoordinator {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = domain.DefaultJobMaxAttempts
	}
	return &DeliveryCoordinator{
		events:       eventsRepo,
		jobs:         jobsRepo,
		runner:       runner,
		logger:       logger,
		now:          nowFn,
		pollInterval: opts.PollInterval,
		maxAttempts:  maxAttempts,
		onFatal:      opts.OnFatal,
		subscribers:  make(map[string]registeredSubscriber),
	}
}

// RegisterSubscriber registers a durable subscriber handler.
func (d *DeliveryCoordinator) RegisterSubscriber(sub Subscriber) error {
	if d == nil {
		return ErrNilDeliveryDependencies
	}
	if sub == nil {
		return fmt.Errorf("%w: nil subscriber", ErrInvalidSubscriber)
	}
	id := strings.TrimSpace(sub.ID())
	if id == "" || id != sub.ID() {
		return fmt.Errorf("%w: invalid subscriber ID %q", ErrInvalidSubscriber, sub.ID())
	}
	kind := strings.TrimSpace(sub.Kind())
	if kind == "" {
		return fmt.Errorf("%w: subscriber %q has empty kind", ErrInvalidSubscriber, id)
	}
	return d.registerEntry(registeredSubscriber{
		id:      id,
		kind:    kind,
		handler: sub,
	})
}

func (d *DeliveryCoordinator) registerEntry(entry registeredSubscriber) error {
	d.subMu.Lock()
	defer d.subMu.Unlock()

	existing, exists := d.subscribers[entry.id]
	if !exists {
		d.subscribers[entry.id] = entry
		return nil
	}
	if existing.kind != entry.kind {
		return fmt.Errorf("%w: subscriber %q kind %q conflicts with %q",
			ErrDuplicateSubscriber, entry.id, entry.kind, existing.kind)
	}
	if existing.handler != nil && entry.handler != nil {
		return fmt.Errorf("%w: subscriber %q already registered", ErrDuplicateSubscriber, entry.id)
	}
	if existing.handler == nil {
		existing.handler = entry.handler
	}
	d.subscribers[entry.id] = existing
	return nil
}

// RegisterModuleSubscribers registers all durable EventSubscribers declared by enabled modules
// in reg, and invokes RegisterSubscribers on modules implementing SubscriberRegistrar.
func (d *DeliveryCoordinator) RegisterModuleSubscribers(reg module.Registry) error {
	if d == nil {
		return ErrNilDeliveryDependencies
	}
	if reg == nil {
		return nil
	}

	seenInRegistry := make(map[string]struct{})
	for _, es := range reg.EventSubscribers() {
		if es == nil {
			return fmt.Errorf("%w: nil module event subscriber", ErrInvalidSubscriber)
		}
		id := strings.TrimSpace(es.Name())
		if id == "" || id != es.Name() {
			return fmt.Errorf("%w: invalid module subscriber name %q", ErrInvalidSubscriber, es.Name())
		}
		if _, dup := seenInRegistry[id]; dup {
			return fmt.Errorf("%w: module registry declares duplicate subscriber %q", ErrDuplicateSubscriber, id)
		}
		seenInRegistry[id] = struct{}{}

		kind := id
		if typed, ok := es.(module.TypedEventSubscriber); ok {
			if k := strings.TrimSpace(typed.Kind()); k != "" {
				kind = k
			}
		}

		var handler Subscriber
		if delivering, ok := es.(module.DeliveringEventSubscriber); ok {
			handler = SubscriberSpec{
				SubscriberID:   id,
				SubscriberKind: kind,
				DeliverFn:      delivering.DeliverEvent,
			}
		}
		if err := d.registerEntry(registeredSubscriber{
			id:      id,
			kind:    kind,
			handler: handler,
		}); err != nil {
			return err
		}
	}

	for _, m := range reg.EnabledModules() {
		if registrar, ok := m.(SubscriberRegistrar); ok {
			if err := registrar.RegisterSubscribers(d); err != nil {
				return fmt.Errorf("registering event subscribers for module %q: %w", m.Name(), err)
			}
		}
	}
	return nil
}

func (d *DeliveryCoordinator) snapshotSubscribers() []registeredSubscriber {
	d.subMu.RLock()
	defer d.subMu.RUnlock()

	ids := make([]string, 0, len(d.subscribers))
	for id := range d.subscribers {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	out := make([]registeredSubscriber, 0, len(ids))
	for _, id := range ids {
		out = append(out, d.subscribers[id])
	}
	return out
}

func (d *DeliveryCoordinator) lookupSubscriber(id string) (registeredSubscriber, bool) {
	d.subMu.RLock()
	defer d.subMu.RUnlock()
	entry, ok := d.subscribers[id]
	return entry, ok
}

// SyncSubscribers ensures every registered durable subscriber has a row in event_subscribers,
// preserving existing position and state for rows already persisted.
func (d *DeliveryCoordinator) SyncSubscribers(ctx context.Context) error {
	if d == nil || d.events == nil {
		return ErrNilDeliveryDependencies
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	now := d.now().UTC()
	for _, entry := range d.snapshotSubscribers() {
		if _, err := d.events.EnsureEventSubscriber(ctx, domain.EventSubscriber{
			ID:        entry.id,
			Kind:      entry.kind,
			Position:  0,
			State:     domain.SubscriberStateActive,
			UpdatedAt: now,
		}); err != nil {
			return fmt.Errorf("ensuring event subscriber %q: %w", entry.id, err)
		}
	}
	return nil
}

// KindConfig returns the jobs.KindConfig for internal EVENT_DELIVERY jobs.
func (d *DeliveryCoordinator) KindConfig() jobs.KindConfig {
	return jobs.KindConfig{
		Kind:               domain.JobKindEventDelivery,
		ResumePolicy:       domain.JobResumeRestart,
		Handler:            jobs.HandlerFunc(d.executeDeliveryJob),
		Reconciler:         jobs.ReconcileFunc(d.reconcileDeliveryJob),
		RequiresReconciler: true,
		OnSucceeded: func(_ context.Context, _ domain.Job) {
			d.Wake()
		},
	}
}

// RegisterJobs registers the internal EVENT_DELIVERY job kind on r.
func (d *DeliveryCoordinator) RegisterJobs(r *jobs.Runner) error {
	if r == nil {
		return errors.New("job runner cannot be nil")
	}
	return r.RegisterKind(d.KindConfig())
}

// DrainOnce inspects each active registered durable subscriber's cursor position and
// enqueues at most one EVENT_DELIVERY job for the next committed published event in
// publication-sequence order. It returns the number of newly enqueued jobs.
func (d *DeliveryCoordinator) DrainOnce(ctx context.Context) (int, error) {
	if d == nil || d.events == nil || d.jobs == nil {
		return 0, ErrNilDeliveryDependencies
	}
	if d.maxAttempts < 1 {
		return 0, ErrInvalidDeliveryMaxAttempts
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	d.drainMu.Lock()
	defer d.drainMu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	activeState := domain.SubscriberStateActive
	subs, err := d.events.ListEventSubscribers(ctx, &activeState)
	if err != nil {
		return 0, fmt.Errorf("listing active event subscribers: %w", err)
	}

	enqueued := 0
	now := d.now().UTC()
	for _, sub := range subs {
		if err := ctx.Err(); err != nil {
			return enqueued, err
		}
		if _, registered := d.lookupSubscriber(sub.ID); !registered {
			continue
		}

		nextEv, err := d.events.GetNextPublishedEventAfter(ctx, sub.Position)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return enqueued, fmt.Errorf("querying next published event after %d for subscriber %q: %w", sub.Position, sub.ID, err)
		}
		seq, ok := nextEv.Cursor()
		if !ok {
			continue
		}

		failed, err := d.jobs.HasFailedEventDeliverySince(ctx, sub.ID, seq, sub.UpdatedAt)
		if err != nil {
			return enqueued, fmt.Errorf("checking failed delivery for subscriber %q sequence %d: %w", sub.ID, seq, err)
		}
		if failed {
			continue
		}

		payloadBytes, err := json.Marshal(domain.EventDeliveryPayloadV1{
			SubscriberID:      sub.ID,
			PublishedSequence: seq,
			EventID:           nextEv.ID,
		})
		if err != nil {
			return enqueued, fmt.Errorf("marshaling delivery payload for subscriber %q sequence %d: %w", sub.ID, seq, err)
		}
		dedupeKey, err := domain.EventDeliveryDedupeKey(sub.ID)
		if err != nil {
			return enqueued, err
		}

		created, err := d.jobs.Enqueue(ctx, &domain.Job{
			Kind:        domain.JobKindEventDelivery,
			Payload:     payloadBytes,
			DedupeKey:   &dedupeKey,
			MaxAttempts: d.maxAttempts,
			RunAfter:    now,
			CreatedAt:   now,
		})
		if err != nil {
			return enqueued, fmt.Errorf("enqueueing delivery job for subscriber %q sequence %d: %w", sub.ID, seq, err)
		}
		if created {
			enqueued++
		}
	}

	if enqueued > 0 && d.runner != nil {
		d.runner.Notify(domain.JobKindEventDelivery)
	}
	return enqueued, nil
}

func (d *DeliveryCoordinator) executeDeliveryJob(ctx context.Context, exec *jobs.Execution) error {
	if exec == nil {
		return fmt.Errorf("%w: nil job execution", jobs.ErrPermanentFailure)
	}
	job := exec.Job()
	payload, err := domain.DecodeEventDeliveryPayload(job.Payload)
	if err != nil {
		return fmt.Errorf("%w: %v", jobs.ErrPermanentFailure, err)
	}

	entry, registered := d.lookupSubscriber(payload.SubscriberID)
	if !registered || entry.handler == nil {
		return fmt.Errorf("%w: %q", ErrMissingSubscriberHandler, payload.SubscriberID)
	}

	subRow, err := d.events.GetEventSubscriber(ctx, payload.SubscriberID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("%w: subscriber %q not found", jobs.ErrPermanentFailure, payload.SubscriberID)
		}
		return err
	}
	if subRow.State != domain.SubscriberStateActive {
		return fmt.Errorf("%w: subscriber %q is in state %s", jobs.ErrJobCancelled, payload.SubscriberID, subRow.State)
	}
	if subRow.Position >= payload.PublishedSequence {
		return nil
	}

	nextEv, err := d.events.GetNextPublishedEventAfter(ctx, subRow.Position)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("%w: published event sequence %d not found for subscriber %q",
				jobs.ErrPermanentFailure, payload.PublishedSequence, payload.SubscriberID)
		}
		return err
	}
	nextSeq, ok := nextEv.Cursor()
	if !ok || nextSeq != payload.PublishedSequence {
		return fmt.Errorf("%w: subscriber %q at position %d must deliver sequence %d before %d",
			domain.ErrCursorSequenceMismatch, payload.SubscriberID, subRow.Position, nextSeq, payload.PublishedSequence)
	}

	outcome, err := entry.handler.Deliver(ctx, *nextEv)
	if err != nil {
		return err
	}
	if !outcome.IsValid() {
		return fmt.Errorf("%w: %d", domain.ErrInvalidDeliveryOutcome, outcome)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if _, err := d.events.AdvanceEventSubscriberCursor(ctx, payload.SubscriberID, payload.PublishedSequence, d.now().UTC()); err != nil {
		if errors.Is(err, domain.ErrSubscriberNotActive) {
			return fmt.Errorf("%w: %v", jobs.ErrJobCancelled, err)
		}
		return err
	}
	return nil
}

func decodePayloadForReconcile(raw json.RawMessage) (domain.EventDeliveryPayloadV1, string) {
	payload, err := domain.DecodeEventDeliveryPayload(raw)
	if err != nil {
		return domain.EventDeliveryPayloadV1{}, err.Error()
	}
	return payload, ""
}

func (d *DeliveryCoordinator) lookupSubscriberForReconcile(ctx context.Context, id string) (*domain.EventSubscriber, error) {
	subRow, err := d.events.GetEventSubscriber(ctx, id)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return subRow, nil
}

func (d *DeliveryCoordinator) reconcileDeliveryJob(ctx context.Context, job domain.Job) (domain.JobReconcileOutcome, error) {
	payload, invalidReason := decodePayloadForReconcile(job.Payload)
	if invalidReason != "" {
		return domain.JobReconcileOutcome{
			Action: domain.JobReconcileFailed,
			Error:  invalidReason,
		}, nil
	}

	subRow, err := d.lookupSubscriberForReconcile(ctx, payload.SubscriberID)
	if err != nil {
		return domain.JobReconcileOutcome{}, err
	}
	if subRow == nil {
		return domain.JobReconcileOutcome{
			Action: domain.JobReconcileFailed,
			Error:  fmt.Sprintf("event subscriber %q not found", payload.SubscriberID),
		}, nil
	}
	if subRow.State != domain.SubscriberStateActive {
		return domain.JobReconcileOutcome{
			Action: domain.JobReconcileCancelled,
			Error:  fmt.Sprintf("subscriber %q is in state %s", payload.SubscriberID, subRow.State),
		}, nil
	}
	if subRow.Position >= payload.PublishedSequence {
		return domain.JobReconcileOutcome{
			Action: domain.JobReconcileSucceeded,
		}, nil
	}
	return domain.JobReconcileOutcome{
		Action: domain.JobReconcileRequeue,
	}, nil
}

// Start synchronizes registered subscriber cursor rows and launches the background
// cursor drain loop, which performs an initial startup drain and then wakes on
// Wake or the configured poll interval.
func (d *DeliveryCoordinator) Start(ctx context.Context) error {
	if d == nil || d.events == nil || d.jobs == nil {
		return ErrNilDeliveryDependencies
	}
	if ctx == nil {
		return errors.New("lifecycle context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.pollInterval < 0 {
		return ErrInvalidDeliveryInterval
	}
	if d.maxAttempts < 1 {
		return ErrInvalidDeliveryMaxAttempts
	}
	if err := d.SyncSubscribers(ctx); err != nil {
		return err
	}

	interval := d.pollInterval
	if interval == 0 {
		interval = DefaultDeliveryPollInterval
	}

	return d.loop.start(
		ctx,
		interval,
		ErrDeliveryAlreadyRunning,
		func(loopCtx context.Context) error {
			_, err := d.DrainOnce(loopCtx)
			return err
		},
		func(err error) {
			if d.logger != nil {
				d.logger.Error("event delivery coordinator failed; stopping", logging.Err(err))
			}
			if d.onFatal != nil {
				d.onFatal(err)
			}
		},
	)
}

// Wake schedules a prompt cursor drain pass on the running DeliveryCoordinator.
func (d *DeliveryCoordinator) Wake() {
	if d == nil {
		return
	}
	d.loop.wake()
}

// Stop cancels the running DeliveryCoordinator loop and waits for it to exit.
func (d *DeliveryCoordinator) Stop() {
	if d == nil {
		return
	}
	d.loop.stop()
}

// Wait blocks until the current background DeliveryCoordinator loop exits.
func (d *DeliveryCoordinator) Wait() {
	if d == nil {
		return
	}
	d.loop.wait()
}

// Err returns the fatal error that stopped the most recent DeliveryCoordinator run, if any.
func (d *DeliveryCoordinator) Err() error {
	if d == nil {
		return nil
	}
	return d.loop.err()
}
