package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

	if r.poolsRunning {
		return ErrPoolsAlreadyRunning
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

	poolCtx, poolCancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	r.pools = pools
	r.poolCancel = poolCancel
	r.poolWG = wg
	r.poolsRunning = true

	for _, item := range planned {
		for range item.pool.size {
			wg.Add(1)
			go r.runPoolWorker(poolCtx, wg, item.cfg, item.pool, interval)
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
	pool := r.pools[kind]
	r.mu.RUnlock()

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
	pools := make([]*kindPool, 0, len(r.pools))
	for _, pool := range r.pools {
		pools = append(pools, pool)
	}
	r.mu.RUnlock()

	for _, pool := range pools {
		select {
		case pool.wake <- struct{}{}:
		default:
		}
	}
}

// Wait blocks until all worker pool goroutines started by Start have exited.
func (r *Runner) Wait() {
	r.mu.RLock()
	wg := r.poolWG
	r.mu.RUnlock()

	if wg != nil {
		wg.Wait()
	}

	r.mu.Lock()
	if r.poolWG == wg {
		r.poolsRunning = false
		r.pools = nil
		r.poolCancel = nil
		r.poolWG = nil
	}
	r.mu.Unlock()
}

// Stop cancels all running worker pools and waits for active workers to finish.
func (r *Runner) Stop() {
	r.mu.RLock()
	cancel := r.poolCancel
	r.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
	r.Wait()
}

func (r *Runner) runPoolWorker(ctx context.Context, wg *sync.WaitGroup, cfg KindConfig, pool *kindPool, interval time.Duration) {
	defer wg.Done()

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
				r.logger.Error("job worker step failed",
					slog.String("job_kind", string(cfg.Kind)),
					logging.Err(err),
				)
				if !waitForNextPoll(ctx, interval, pool.wake) {
					return
				}
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
