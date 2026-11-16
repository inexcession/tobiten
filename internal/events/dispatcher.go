package events

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/logging"
)

// DefaultDispatcherPollInterval controls how often an idle background Dispatcher
// checks for newly committed unsequenced outbox rows when no Notify signal arrived.
const DefaultDispatcherPollInterval = 200 * time.Millisecond

// Common errors returned by the serialized domain-event dispatcher.
var (
	ErrNilEventPublisher               = errors.New("event publisher cannot be nil")
	ErrDispatcherRequiresRootPublisher = errors.New("dispatcher requires a non-transactional event publisher")
	ErrDispatcherAlreadyRunning        = errors.New("event dispatcher is already running")
	ErrInvalidDispatcherInterval       = errors.New("dispatcher poll interval must be non-negative")
)

// EventPublisher assigns monotonic publication sequences and timestamps to all
// committed unsequenced outbox rows in a single serialized transaction.
// storage.EventRepository satisfies this interface.
type EventPublisher interface {
	InTx() bool
	PublishUnsequencedEvents(ctx context.Context, publishedAt time.Time) ([]domain.DomainEvent, error)
}

// DispatcherOption configures a Dispatcher instance.
type DispatcherOption func(*Dispatcher)

// WithDispatcherClock overrides the timestamp source used for published_at.
func WithDispatcherClock(now func() time.Time) DispatcherOption {
	return func(d *Dispatcher) {
		if now != nil {
			d.now = now
		}
	}
}

// WithDispatcherLogger sets the logger used by background dispatcher loops.
func WithDispatcherLogger(logger *slog.Logger) DispatcherOption {
	return func(d *Dispatcher) {
		if logger != nil {
			d.logger = logger
		}
	}
}

// WithDispatcherPollInterval sets the idle poll interval for Start.
func WithDispatcherPollInterval(interval time.Duration) DispatcherOption {
	return func(d *Dispatcher) {
		d.pollInterval = interval
	}
}

// WithOnPublished registers a hook invoked in publication-sequence order after
// the sequencing transaction commits. If sequencing fails or rolls back, the
// hook is never invoked.
func WithOnPublished(fn func(ctx context.Context, events []domain.DomainEvent) error) DispatcherOption {
	return func(d *Dispatcher) {
		d.onPublished = fn
	}
}

// WithOnFatal registers a callback invoked when the background dispatcher loop
// stops due to a non-cancellation error.
func WithOnFatal(fn func(error)) DispatcherOption {
	return func(d *Dispatcher) {
		d.onFatal = fn
	}
}

type dispatcherGeneration struct {
	cancel context.CancelCauseFunc
	wake   chan struct{}
	wg     sync.WaitGroup
}

// Dispatcher is the single serialized transactional-outbox dispatcher (ADR-0018 S3).
// Each dispatch selects all committed domain_events rows with published_sequence IS NULL
// regardless of raw id, assigns contiguous published_sequence values and published_at
// inside a serialized PostgreSQL transaction, and commits before any post-commit delivery.
type Dispatcher struct {
	repo         EventPublisher
	now          func() time.Time
	logger       *slog.Logger
	pollInterval time.Duration
	onPublished  func(ctx context.Context, events []domain.DomainEvent) error
	onFatal      func(error)

	dispatchMu sync.Mutex

	stateMu sync.RWMutex
	runGen  *dispatcherGeneration
	runErr  error
}

// NewDispatcher constructs a Dispatcher backed by publisher.
func NewDispatcher(publisher EventPublisher, opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		repo:         publisher,
		now:          time.Now,
		logger:       slog.Default(),
		pollInterval: DefaultDispatcherPollInterval,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}
	return d
}

// DispatchOnce runs one serialized sequencing pass over all committed unsequenced
// domain_events rows, committing published_sequence and published_at before any
// post-commit notification or delivery hook executes.
func (d *Dispatcher) DispatchOnce(ctx context.Context) ([]domain.DomainEvent, error) {
	if d == nil || d.repo == nil {
		return nil, ErrNilEventPublisher
	}
	if d.repo.InTx() {
		return nil, ErrDispatcherRequiresRootPublisher
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	d.dispatchMu.Lock()
	defer d.dispatchMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	nowFn := time.Now
	if d.now != nil {
		nowFn = d.now
	}
	published, err := d.repo.PublishUnsequencedEvents(ctx, nowFn().UTC())
	if err != nil {
		return nil, err
	}
	if len(published) == 0 {
		return nil, nil
	}

	if d.onPublished != nil {
		if err := d.onPublished(ctx, slices.Clone(published)); err != nil {
			return published, err
		}
	}
	return published, nil
}

// Start launches the serialized background dispatcher goroutine.
// It performs an initial pass and then wakes on Notify or the configured poll interval.
func (d *Dispatcher) Start(ctx context.Context) error {
	if d == nil || d.repo == nil {
		return ErrNilEventPublisher
	}
	if d.repo.InTx() {
		return ErrDispatcherRequiresRootPublisher
	}
	if ctx == nil {
		return errors.New("lifecycle context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.pollInterval < 0 {
		return ErrInvalidDispatcherInterval
	}

	d.stateMu.Lock()
	defer d.stateMu.Unlock()

	if d.runGen != nil {
		return ErrDispatcherAlreadyRunning
	}

	interval := d.pollInterval
	if interval == 0 {
		interval = DefaultDispatcherPollInterval
	}

	loopCtx, loopCancel := context.WithCancelCause(ctx)
	gen := &dispatcherGeneration{
		cancel: loopCancel,
		wake:   make(chan struct{}, 1),
	}

	d.runErr = nil
	d.runGen = gen

	gen.wg.Add(1)
	go d.runLoop(loopCtx, gen, interval)

	return nil
}

// Notify wakes the running background dispatcher loop so newly committed events
// are sequenced promptly without waiting for the next poll interval.
func (d *Dispatcher) Notify() {
	if d == nil {
		return
	}
	d.stateMu.RLock()
	gen := d.runGen
	d.stateMu.RUnlock()

	if gen == nil {
		return
	}
	select {
	case gen.wake <- struct{}{}:
	default:
	}
}

// Stop cancels the running background dispatcher loop and waits for it to exit.
func (d *Dispatcher) Stop() {
	if d == nil {
		return
	}
	d.stateMu.RLock()
	gen := d.runGen
	d.stateMu.RUnlock()

	if gen == nil {
		return
	}
	gen.cancel(nil)
	d.waitGeneration(gen)
}

// Wait blocks until the current background dispatcher loop exits.
func (d *Dispatcher) Wait() {
	if d == nil {
		return
	}
	d.stateMu.RLock()
	gen := d.runGen
	d.stateMu.RUnlock()

	d.waitGeneration(gen)
}

// Err returns the fatal error that stopped the most recent background dispatcher run, if any.
func (d *Dispatcher) Err() error {
	if d == nil {
		return nil
	}
	d.stateMu.RLock()
	defer d.stateMu.RUnlock()
	return d.runErr
}

func (d *Dispatcher) waitGeneration(gen *dispatcherGeneration) {
	if gen == nil {
		return
	}
	gen.wg.Wait()

	d.stateMu.Lock()
	if d.runGen == gen {
		d.runGen = nil
	}
	d.stateMu.Unlock()
}

func (d *Dispatcher) runLoop(ctx context.Context, gen *dispatcherGeneration, interval time.Duration) {
	defer gen.wg.Done()
	defer func() {
		d.stateMu.Lock()
		if d.runGen == gen {
			d.runGen = nil
		}
		d.stateMu.Unlock()
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		if _, err := d.DispatchOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			d.failGeneration(gen, err)
			return
		}

		if !waitForNextDispatch(ctx, interval, gen.wake) {
			return
		}
	}
}

func (d *Dispatcher) failGeneration(gen *dispatcherGeneration, err error) {
	if d.logger != nil {
		d.logger.Error("event dispatcher failed; stopping", logging.Err(err))
	}

	d.stateMu.Lock()
	shouldNotify := false
	if d.runGen == gen && d.runErr == nil {
		d.runErr = err
		shouldNotify = d.onFatal != nil
	}
	onFatal := d.onFatal
	d.stateMu.Unlock()

	gen.cancel(err)
	if shouldNotify {
		onFatal(err)
	}
}

func waitForNextDispatch(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return ctx.Err() == nil
	case <-timer.C:
		return ctx.Err() == nil
	}
}
