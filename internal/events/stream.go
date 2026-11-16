package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

const (
	// DefaultStreamBatchSize is the maximum number of committed events fetched per drain query.
	DefaultStreamBatchSize = 100

	// DefaultStreamPollInterval is the fallback drain interval when no dispatcher wake signal arrives.
	DefaultStreamPollInterval = time.Second
)

// WakeSource provides non-blocking publication wake notifications for live SSE drains.
// *Dispatcher satisfies this interface.
type WakeSource interface {
	Subscribe() (<-chan struct{}, func())
}

// StreamOptions configures a StreamService instance.
type StreamOptions struct {
	ProgressHub  *ProgressHub
	WakeSource   WakeSource
	PollInterval time.Duration
	BatchSize    int
}

// StreamService implements app.EventService by draining committed domain_events in
// published_sequence order and forwarding live ephemeral worker progress ticks from
// ProgressHub, enforcing current job ownership and effective jobs:read_all on every item
// (ADR-0013 §3, ADR-0018 S3).
type StreamService struct {
	events       storage.EventRepository
	jobs         storage.JobRepository
	pollInterval time.Duration
	batchSize    int

	mu       sync.RWMutex
	progress *ProgressHub
	wake     WakeSource

	notifyMu   sync.Mutex
	nextNotify uint64
	notifySubs map[uint64]chan struct{}
}

var _ app.EventService = (*StreamService)(nil)

// NewStreamService constructs a StreamService backed by eventsRepo and jobsRepo.
func NewStreamService(eventsRepo storage.EventRepository, jobsRepo storage.JobRepository, opts StreamOptions) *StreamService {
	pollInterval := opts.PollInterval
	if pollInterval == 0 {
		pollInterval = DefaultStreamPollInterval
	} else if pollInterval < 0 {
		pollInterval = 0
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultStreamBatchSize
	}
	return &StreamService{
		events:       eventsRepo,
		jobs:         jobsRepo,
		pollInterval: pollInterval,
		batchSize:    batchSize,
		progress:     opts.ProgressHub,
		wake:         opts.WakeSource,
		notifySubs:   make(map[uint64]chan struct{}),
	}
}

// SetProgressHub attaches or replaces the ephemeral progress hub used by live streams.
func (s *StreamService) SetProgressHub(hub *ProgressHub) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.progress = hub
	s.mu.Unlock()
}

// SetWakeSource attaches or replaces the dispatcher wake source used by live streams.
func (s *StreamService) SetWakeSource(wake WakeSource) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.wake = wake
	s.mu.Unlock()
}

// Notify wakes all active live SSE drains to check for newly committed published events.
func (s *StreamService) Notify() {
	if s == nil {
		return
	}
	s.notifyMu.Lock()
	for _, ch := range s.notifySubs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	s.notifyMu.Unlock()
}

func (s *StreamService) subscribeManual() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	s.notifyMu.Lock()
	if s.notifySubs == nil {
		s.notifySubs = make(map[uint64]chan struct{})
	}
	s.nextNotify++
	id := s.nextNotify
	s.notifySubs[id] = ch
	s.notifyMu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			s.notifyMu.Lock()
			delete(s.notifySubs, id)
			s.notifyMu.Unlock()
		})
	}
}

// Stream attaches a live authenticated event stream for principal, starting from the current
// committed publication sequence (no M2 replay yet) with catch-up on attach.
func (s *StreamService) Stream(
	ctx context.Context,
	principal *domain.Principal,
	filter app.EventStreamFilter,
	onAttached func() error,
	emit func(app.StreamEvent) error,
) error {
	if principal == nil {
		return app.ErrUnauthorized
	}
	if ctx == nil {
		return errors.New("stream context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.events == nil || s.jobs == nil {
		return fmt.Errorf("%w: event stream service is unavailable", app.ErrServiceBusy)
	}
	if emit == nil {
		return errors.New("stream emit callback is required")
	}

	s.mu.RLock()
	progress := s.progress
	wake := s.wake
	s.mu.RUnlock()

	cursor, err := s.events.GetLatestPublishedSequence(ctx)
	if err != nil {
		return err
	}

	var wakeCh <-chan struct{}
	unsubWake := func() {}
	if wake != nil {
		wakeCh, unsubWake = wake.Subscribe()
	}
	defer unsubWake()

	manualCh, unsubManual := s.subscribeManual()
	defer unsubManual()

	var progressCh <-chan domain.JobProgressTick
	unsubProgress := func() {}
	if progress != nil {
		ch, cancelProgress, err := progress.Subscribe(ctx, ProgressSubscribeOptions{
			Principal: principal,
			JobID:     strings.TrimSpace(filter.JobID),
		})
		if err != nil {
			return err
		}
		progressCh = ch
		unsubProgress = cancelProgress
	}
	defer unsubProgress()

	if onAttached != nil {
		if err := onAttached(); err != nil {
			return err
		}
	}

	jobIDFilter := strings.TrimSpace(filter.JobID)
	if err := s.drainCommitted(ctx, principal, jobIDFilter, &cursor, emit); err != nil {
		if ctx.Err() == nil {
			return err
		}
		return nil
	}

	err = s.runLoop(ctx, principal, jobIDFilter, &cursor, wakeCh, manualCh, progressCh, emit)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *StreamService) runLoop(
	ctx context.Context,
	principal *domain.Principal,
	jobIDFilter string,
	cursor *int64,
	wakeCh <-chan struct{},
	manualCh <-chan struct{},
	progressCh <-chan domain.JobProgressTick,
	emit func(app.StreamEvent) error,
) error {
	var pollCh <-chan time.Time
	if s.pollInterval > 0 {
		ticker := time.NewTicker(s.pollInterval)
		defer ticker.Stop()
		pollCh = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-wakeCh:
			if err := s.drainCommitted(ctx, principal, jobIDFilter, cursor, emit); err != nil {
				return err
			}
		case <-manualCh:
			if err := s.drainCommitted(ctx, principal, jobIDFilter, cursor, emit); err != nil {
				return err
			}
		case <-pollCh:
			if err := s.drainCommitted(ctx, principal, jobIDFilter, cursor, emit); err != nil {
				return err
			}
		case tick, ok := <-progressCh:
			if !ok {
				progressCh = nil
				continue
			}
			if err := emitProgressTick(tick, emit); err != nil {
				return err
			}
		}
	}
}

func (s *StreamService) drainCommitted(
	ctx context.Context,
	principal *domain.Principal,
	jobIDFilter string,
	cursor *int64,
	emit func(app.StreamEvent) error,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := s.events.ListPublishedEventsAfter(ctx, *cursor, s.batchSize)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}

		for _, ev := range batch {
			seq, ok := ev.Cursor()
			if !ok {
				continue
			}
			if seq > *cursor {
				*cursor = seq
			}
			streamEv, visible, err := s.projectJobEvent(ctx, principal, jobIDFilter, ev, seq)
			if err != nil {
				return err
			}
			if !visible {
				continue
			}
			if err := emit(streamEv); err != nil {
				return err
			}
		}

		if len(batch) < s.batchSize {
			return nil
		}
	}
}

func isJobLifecycleEventKind(kind domain.EventKind) bool {
	switch kind {
	case domain.EventJobStarted, domain.EventJobFinished, domain.EventJobFailed, domain.EventJobCancelled:
		return true
	default:
		return false
	}
}

func (s *StreamService) lookupJob(ctx context.Context, jobID string) (*domain.Job, error) {
	job, err := s.jobs.GetJobByID(ctx, jobID)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}

func decodeJobEventPayload(ev domain.DomainEvent) (domain.JobEventPayloadV1, bool) {
	payload, err := domain.DecodeEventPayload[domain.JobEventPayloadV1](ev)
	if err != nil || strings.TrimSpace(payload.JobID) == "" || payload.Kind.IsInternal() {
		return domain.JobEventPayloadV1{}, false
	}
	return payload, true
}

func (s *StreamService) projectJobEvent(
	ctx context.Context,
	principal *domain.Principal,
	jobIDFilter string,
	ev domain.DomainEvent,
	seq int64,
) (app.StreamEvent, bool, error) {
	if !isJobLifecycleEventKind(ev.Kind) {
		return app.StreamEvent{}, false, nil
	}
	payload, ok := decodeJobEventPayload(ev)
	if !ok {
		return app.StreamEvent{}, false, nil
	}
	if jobIDFilter != "" && payload.JobID != jobIDFilter {
		return app.StreamEvent{}, false, nil
	}

	job, err := s.lookupJob(ctx, payload.JobID)
	if err != nil {
		return app.StreamEvent{}, false, err
	}
	if job == nil || job.Kind.IsInternal() || !job.VisibleTo(principal) {
		return app.StreamEvent{}, false, nil
	}

	var publishedAt time.Time
	if ev.PublishedAt != nil {
		publishedAt = ev.PublishedAt.UTC()
	}
	pub := app.PublicEvent{
		Sequence:      seq,
		PublishedAt:   publishedAt,
		Kind:          ev.Kind,
		Version:       ev.Version,
		AggregateType: ev.AggregateType,
		AggregateID:   ev.AggregateID,
		Payload:       ev.Payload,
		CreatedAt:     ev.CreatedAt.UTC(),
	}
	data, err := json.Marshal(pub)
	if err != nil {
		return app.StreamEvent{}, false, err
	}
	return app.StreamEvent{
		ID:   strconv.FormatInt(seq, 10),
		Name: string(ev.Kind),
		Data: data,
	}, true, nil
}

func emitProgressTick(tick domain.JobProgressTick, emit func(app.StreamEvent) error) error {
	if tick.Kind.IsInternal() {
		return nil
	}
	data, err := json.Marshal(tick)
	if err != nil {
		return err
	}
	return emit(app.StreamEvent{
		ID:   "", // Ephemeral progress ticks carry no SSE ID (ADR-0018 S3).
		Name: domain.ProgressTickEventName,
		Data: data,
	})
}
