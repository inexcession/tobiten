package events

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

const (
	// DefaultProgressMinInterval is the default per-job rate limit between ephemeral progress ticks.
	DefaultProgressMinInterval = 250 * time.Millisecond

	// DefaultProgressSubscriberBuffer is the default channel buffer capacity per live progress listener.
	DefaultProgressSubscriberBuffer = 64
)

// Sentinel errors returned by the ephemeral progress-tick hub.
var (
	ErrNilProgressHub        = errors.New("progress hub cannot be nil")
	ErrInvalidProgressRate   = errors.New("progress minimum interval must be non-negative")
	ErrInvalidProgressBuffer = errors.New("progress subscriber buffer size must be non-negative")
	ErrPrincipalRequired     = errors.New("authenticated principal is required for progress subscription")
)

// ProgressHubOptions configures a ProgressHub instance.
type ProgressHubOptions struct {
	MinInterval time.Duration
	Now         func() time.Time
}

// ProgressSubscribeOptions configures a live ephemeral progress subscription.
type ProgressSubscribeOptions struct {
	Principal  *domain.Principal
	JobID      string
	BufferSize int
}

type progressSubscriber struct {
	id        uint64
	principal *domain.Principal
	jobID     string
	ch        chan domain.JobProgressTick
	closed    bool
}

// ProgressHub broadcasts rate-limited, ephemeral worker progress ticks to live
// authenticated listeners without persisting outbox rows or assigning SSE IDs
// (ADR-0013 §3, ADR-0017 §2, ADR-0018 S3).
type ProgressHub struct {
	minInterval time.Duration
	now         func() time.Time

	mu          sync.RWMutex
	nextID      uint64
	lastSent    map[string]time.Time
	subscribers map[uint64]*progressSubscriber
}

// NewProgressHub constructs an ephemeral in-memory ProgressHub.
func NewProgressHub(opts ProgressHubOptions) (*ProgressHub, error) {
	if opts.MinInterval < 0 {
		return nil, ErrInvalidProgressRate
	}
	minInterval := opts.MinInterval
	if minInterval == 0 {
		minInterval = DefaultProgressMinInterval
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &ProgressHub{
		minInterval: minInterval,
		now:         nowFn,
		lastSent:    make(map[string]time.Time),
		subscribers: make(map[uint64]*progressSubscriber),
	}, nil
}

// Publish emits tick to matching live listeners when allowed by the per-job rate limit.
// Internal EVENT_DELIVERY jobs and invalid ticks are suppressed. Publish never writes to
// storage and never blocks on slow subscribers.
func (h *ProgressHub) Publish(tick domain.JobProgressTick) bool {
	if h == nil || tick.Kind.IsInternal() {
		return false
	}
	if err := tick.Validate(); err != nil {
		return false
	}

	nowFn := time.Now
	if h.now != nil {
		nowFn = h.now
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	now := nowFn().UTC()
	if last, ok := h.lastSent[tick.JobID]; ok && !now.Before(last) && now.Sub(last) < h.minInterval {
		return false
	}
	h.lastSent[tick.JobID] = now

	cloned := tick.Clone()
	for _, sub := range h.subscribers {
		if sub.closed {
			continue
		}
		if sub.jobID != "" && sub.jobID != cloned.JobID {
			continue
		}
		if !cloned.VisibleTo(sub.principal) {
			continue
		}
		select {
		case sub.ch <- cloned:
		default:
		}
	}
	return true
}

// ClearJob releases per-job rate-limit state once a job finishes executing.
func (h *ProgressHub) ClearJob(jobID string) {
	if h == nil || strings.TrimSpace(jobID) == "" {
		return
	}
	h.mu.Lock()
	delete(h.lastSent, jobID)
	h.mu.Unlock()
}

// Subscribe registers a live listener filtered by opts.Principal's job visibility rules
// and optional opts.JobID. The returned channel closes when ctx is cancelled or the
// returned unsubscribe function is called.
func (h *ProgressHub) Subscribe(ctx context.Context, opts ProgressSubscribeOptions) (<-chan domain.JobProgressTick, func(), error) {
	if h == nil {
		return nil, nil, ErrNilProgressHub
	}
	if ctx == nil {
		return nil, nil, errors.New("subscription context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if opts.Principal == nil {
		return nil, nil, ErrPrincipalRequired
	}
	if opts.BufferSize < 0 {
		return nil, nil, ErrInvalidProgressBuffer
	}

	bufSize := opts.BufferSize
	if bufSize == 0 {
		bufSize = DefaultProgressSubscriberBuffer
	}

	h.mu.Lock()
	h.nextID++
	sub := &progressSubscriber{
		id:        h.nextID,
		principal: opts.Principal,
		jobID:     strings.TrimSpace(opts.JobID),
		ch:        make(chan domain.JobProgressTick, bufSize),
	}
	h.subscribers[sub.id] = sub
	h.mu.Unlock()

	var once sync.Once
	unsub := func() {
		once.Do(func() {
			h.mu.Lock()
			if !sub.closed {
				sub.closed = true
				delete(h.subscribers, sub.id)
				close(sub.ch)
			}
			h.mu.Unlock()
		})
	}
	stopAfter := context.AfterFunc(ctx, unsub)
	cancel := func() {
		stopAfter()
		unsub()
	}

	return sub.ch, cancel, nil
}

// SubscriberCount returns the number of currently active live progress listeners.
func (h *ProgressHub) SubscriberCount() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}
