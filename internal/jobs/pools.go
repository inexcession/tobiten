package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/logging"
	"github.com/inexcession/tobiten/internal/storage"
)

const (
	// DefaultWorkerPoolSize is the small per-kind worker pool default (ADR-0017 §6).
	DefaultWorkerPoolSize = 2

	// DefaultPollInterval controls how often idle workers check for newly runnable jobs
	// when no explicit Notify signal has woken them.
	DefaultPollInterval = 200 * time.Millisecond
)

type kindPool struct {
	kind domain.JobKind
	size int
	wake chan struct{}
}

type poolGeneration struct {
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup
	pools  map[domain.JobKind]*kindPool
}

// Kinds returns a sorted list of all registered job kinds.
func (r *Runner) Kinds() []domain.JobKind {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]domain.JobKind, 0, len(r.kinds))
	for k := range r.kinds {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// SetPoolSizes replaces the runner's per-kind pool size overrides before pools are started.
func (r *Runner) SetPoolSizes(sizes map[domain.JobKind]int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.activePools != nil {
		return ErrPoolsAlreadyRunning
	}
	for kind, size := range sizes {
		if size < 0 {
			return fmt.Errorf("%w: kind %q pool size %d must be non-negative", ErrInvalidPoolSize, kind, size)
		}
	}
	if len(sizes) == 0 {
		r.poolSizes = nil
		return nil
	}
	copied := make(map[domain.JobKind]int, len(sizes))
	for k, v := range sizes {
		copied[k] = v
	}
	r.poolSizes = copied
	return nil
}

// PoolSize returns the effective bounded worker pool size for a registered kind.
func (r *Runner) PoolSize(kind domain.JobKind) (int, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cfg, ok := r.kinds[kind]
	if !ok {
		return 0, false
	}
	size, err := r.resolvePoolSizeLocked(cfg)
	if err != nil {
		return 0, false
	}
	return size, true
}

// Err returns the fatal worker pool error that stopped the most recent pool run, if any.
func (r *Runner) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.poolErr
}

func (r *Runner) validatePoolOptionsLocked() error {
	if r.defaultPoolSize < 0 {
		return fmt.Errorf("%w: default pool size %d must be non-negative", ErrInvalidPoolSize, r.defaultPoolSize)
	}
	if r.pollInterval < 0 {
		return fmt.Errorf("%w: poll interval %s must be non-negative", ErrInvalidPoolSize, r.pollInterval)
	}
	for kind, size := range r.poolSizes {
		if size < 0 {
			return fmt.Errorf("%w: kind %q pool size %d must be non-negative", ErrInvalidPoolSize, kind, size)
		}
	}
	return nil
}

func (r *Runner) resolvePoolSizeLocked(cfg KindConfig) (int, error) {
	if r.defaultPoolSize < 0 {
		return 0, fmt.Errorf("%w: default pool size %d must be non-negative", ErrInvalidPoolSize, r.defaultPoolSize)
	}
	if override, ok := r.poolSizes[cfg.Kind]; ok {
		if override < 0 {
			return 0, fmt.Errorf("%w: kind %q pool size %d must be non-negative", ErrInvalidPoolSize, cfg.Kind, override)
		}
		if override > 0 {
			return override, nil
		}
	}
	if cfg.PoolSize < 0 {
		return 0, fmt.Errorf("%w: kind %q pool size %d must be non-negative", ErrInvalidPoolSize, cfg.Kind, cfg.PoolSize)
	}
	if cfg.PoolSize > 0 {
		return cfg.PoolSize, nil
	}
	if r.defaultPoolSize > 0 {
		return r.defaultPoolSize, nil
	}
	return DefaultWorkerPoolSize, nil
}

// Start launches independent bounded worker pools for all registered job kinds that have a Handler.
// Startup recovery must complete under daemon ownership before calling Start.
// Workers claim and execute jobs using ctx and stop claiming as soon as ctx is cancelled.
func (r *Runner) Start(ctx context.Context) error {
	if r.repo == nil {
		return errors.New("job repository is required")
	}
	if ctx == nil {
		return errors.New("lifecycle context is required")
	}
	if !r.recovered.Load() {
		return ErrRecoveryNotCompleted
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.activePools != nil {
		return ErrPoolsAlreadyRunning
	}
	if r.activeDirectClaims > 0 {
		return ErrDirectClaimInProgress
	}
	if err := r.validatePoolOptionsLocked(); err != nil {
		return err
	}

	type plannedPool struct {
		cfg  KindConfig
		pool *kindPool
	}
	planned := make([]plannedPool, 0, len(r.kinds))
	pools := make(map[domain.JobKind]*kindPool, len(r.kinds))

	for kind, cfg := range r.kinds {
		if cfg.Handler == nil {
			continue
		}
		size, err := r.resolvePoolSizeLocked(cfg)
		if err != nil {
			return err
		}
		p := &kindPool{
			kind: kind,
			size: size,
			wake: make(chan struct{}, 1),
		}
		pools[kind] = p
		planned = append(planned, plannedPool{cfg: cfg, pool: p})
	}

	interval := r.pollInterval
	if interval == 0 {
		interval = DefaultPollInterval
	}

	poolCtx, poolCancel := context.WithCancelCause(ctx)
	gen := &poolGeneration{
		cancel: poolCancel,
		pools:  pools,
	}

	r.poolErr = nil
	r.activePools = gen

	for _, item := range planned {
		for range item.pool.size {
			gen.wg.Add(1)
			go r.runPoolWorker(poolCtx, gen, item.cfg, item.pool, interval)
		}
		r.logger.Debug("started job worker pool",
			slog.String("job_kind", string(item.cfg.Kind)),
			slog.Int("pool_size", item.pool.size),
		)
	}

	return nil
}

// Notify wakes an idle worker in kind's pool, if running, so newly enqueued work is claimed promptly.
func (r *Runner) Notify(kind domain.JobKind) {
	r.mu.RLock()
	gen := r.activePools
	r.mu.RUnlock()

	if gen == nil {
		return
	}
	pool := gen.pools[kind]
	if pool == nil {
		return
	}
	select {
	case pool.wake <- struct{}{}:
	default:
	}
}

// NotifyAll wakes an idle worker in every running per-kind pool.
func (r *Runner) NotifyAll() {
	r.mu.RLock()
	gen := r.activePools
	r.mu.RUnlock()

	if gen == nil {
		return
	}
	for _, pool := range gen.pools {
		select {
		case pool.wake <- struct{}{}:
		default:
		}
	}
}

func (r *Runner) waitGeneration(gen *poolGeneration) {
	if gen == nil {
		return
	}
	gen.wg.Wait()

	r.mu.Lock()
	if r.activePools == gen {
		r.activePools = nil
	}
	r.mu.Unlock()
}

// Wait blocks until all worker pool goroutines in the current pool generation have exited.
func (r *Runner) Wait() {
	r.mu.RLock()
	gen := r.activePools
	r.mu.RUnlock()

	r.waitGeneration(gen)
}

// Stop cancels the current pool generation and waits for its active workers to finish.
func (r *Runner) Stop() {
	r.mu.RLock()
	gen := r.activePools
	r.mu.RUnlock()

	if gen == nil {
		return
	}
	gen.cancel(nil)
	r.waitGeneration(gen)
}

func (r *Runner) failPoolGeneration(gen *poolGeneration, err error) {
	r.logger.Error("job worker outcome persistence failed; stopping worker pools",
		logging.Err(err),
	)

	r.mu.Lock()
	shouldNotify := false
	if r.activePools == gen && r.poolErr == nil {
		r.poolErr = err
		shouldNotify = r.onFatal != nil
	}
	onFatal := r.onFatal
	r.mu.Unlock()

	gen.cancel(err)
	if shouldNotify {
		onFatal(err)
	}
}

func (r *Runner) runPoolWorker(ctx context.Context, gen *poolGeneration, cfg KindConfig, pool *kindPool, interval time.Duration) {
	defer gen.wg.Done()

	for {
		if ctx.Err() != nil {
			return
		}

		claimed, err := r.claimAndRunKind(ctx, cfg, pool)
		if ctx.Err() != nil {
			return
		}
		if claimed {
			if err != nil {
				r.failPoolGeneration(gen, err)
				return
			}
			continue
		}

		if err != nil {
			r.logger.Error("job worker claim failed",
				slog.String("job_kind", string(cfg.Kind)),
				logging.Err(err),
			)
		}
		if !waitForNextPoll(ctx, interval, pool.wake) {
			return
		}
	}
}

func (r *Runner) claimAndRunKind(ctx context.Context, cfg KindConfig, pool *kindPool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	job, err := r.repo.ClaimNext(ctx, cfg.Kind, r.now())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		}
		return false, err
	}

	if pool.size > 1 && ctx.Err() == nil {
		select {
		case pool.wake <- struct{}{}:
		default:
		}
	}

	return true, r.executeClaimedJob(ctx, cfg, job)
}

func waitForNextPoll(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
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
