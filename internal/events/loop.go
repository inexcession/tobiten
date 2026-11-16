package events

import (
	"context"
	"sync"
	"time"
)

type wakeGeneration struct {
	cancel context.CancelCauseFunc
	wake   chan struct{}
	wg     sync.WaitGroup
}

// wakeLoop manages the lifecycle of a single wakeable background polling goroutine.
type wakeLoop struct {
	mu     sync.RWMutex
	gen    *wakeGeneration
	runErr error
}

func (l *wakeLoop) start(
	ctx context.Context,
	interval time.Duration,
	alreadyRunningErr error,
	step func(context.Context) error,
	onFail func(error),
) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.gen != nil {
		return alreadyRunningErr
	}

	loopCtx, loopCancel := context.WithCancelCause(ctx)
	gen := &wakeGeneration{
		cancel: loopCancel,
		wake:   make(chan struct{}, 1),
	}

	l.runErr = nil
	l.gen = gen

	gen.wg.Add(1)
	go l.run(loopCtx, gen, interval, step, onFail)
	return nil
}

func (l *wakeLoop) wake() {
	if l == nil {
		return
	}
	l.mu.RLock()
	gen := l.gen
	l.mu.RUnlock()

	if gen == nil {
		return
	}
	select {
	case gen.wake <- struct{}{}:
	default:
	}
}

func (l *wakeLoop) stop() {
	if l == nil {
		return
	}
	l.mu.RLock()
	gen := l.gen
	l.mu.RUnlock()

	if gen == nil {
		return
	}
	gen.cancel(nil)
	l.waitGeneration(gen)
}

func (l *wakeLoop) wait() {
	if l == nil {
		return
	}
	l.mu.RLock()
	gen := l.gen
	l.mu.RUnlock()

	l.waitGeneration(gen)
}

func (l *wakeLoop) err() error {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.runErr
}

func (l *wakeLoop) waitGeneration(gen *wakeGeneration) {
	if gen == nil {
		return
	}
	gen.wg.Wait()

	l.mu.Lock()
	if l.gen == gen {
		l.gen = nil
	}
	l.mu.Unlock()
}

func (l *wakeLoop) run(
	ctx context.Context,
	gen *wakeGeneration,
	interval time.Duration,
	step func(context.Context) error,
	onFail func(error),
) {
	defer gen.wg.Done()
	defer func() {
		l.mu.Lock()
		if l.gen == gen {
			l.gen = nil
		}
		l.mu.Unlock()
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		if err := step(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			l.failGeneration(gen, err, onFail)
			return
		}

		if !waitForNextDispatch(ctx, interval, gen.wake) {
			return
		}
	}
}

func (l *wakeLoop) failGeneration(gen *wakeGeneration, err error, onFail func(error)) {
	l.mu.Lock()
	shouldNotify := false
	if l.gen == gen && l.runErr == nil {
		l.runErr = err
		shouldNotify = onFail != nil
	}
	l.mu.Unlock()

	gen.cancel(err)
	if shouldNotify {
		onFail(err)
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
