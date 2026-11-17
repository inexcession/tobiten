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
	Users        storage.UserRepository
	Sessions     storage.SessionRepository
	ProgressHub  *ProgressHub
	WakeSource   WakeSource
	PollInterval time.Duration
	BatchSize    int
	Now          func() time.Time
}

// StreamService implements app.EventService by draining committed domain_events in
// published_sequence order and forwarding live ephemeral worker progress ticks from
// ProgressHub, enforcing live credential validity, effective role/PAT grants, and
// current job ownership on every lifecycle and progress frame (ADR-0006, ADR-0013 §3, ADR-0018 S3).
type StreamService struct {
	events       storage.EventRepository
	jobs         storage.JobRepository
	users        storage.UserRepository
	sessions     storage.SessionRepository
	pollInterval time.Duration
	batchSize    int
	now          func() time.Time

	mu       sync.RWMutex
	progress *ProgressHub
	wake     WakeSource

	notifyMu   sync.Mutex
	nextNotify uint64
	notifySubs map[uint64]chan struct{}
}

var _ app.EventService = (*StreamService)(nil)

// NewStreamService constructs a StreamService backed by eventsRepo, jobsRepo, and opts.Users.
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
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &StreamService{
		events:       eventsRepo,
		jobs:         jobsRepo,
		users:        opts.Users,
		sessions:     opts.Sessions,
		pollInterval: pollInterval,
		batchSize:    batchSize,
		now:          nowFn,
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
	if s == nil || s.events == nil || s.jobs == nil || s.users == nil {
		return fmt.Errorf("%w: event stream service is unavailable", app.ErrServiceBusy)
	}
	if emit == nil {
		return errors.New("stream emit callback is required")
	}

	livePrincipal, valid, err := s.resolveLivePrincipal(ctx, principal)
	if err != nil {
		return err
	}
	if !valid {
		return app.ErrUnauthorized
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
	updateProgress := func(*domain.Principal) {}
	if progress != nil {
		sub, subErr := progress.SubscribeControlled(ctx, ProgressSubscribeOptions{
			Principal: livePrincipal,
			JobID:     strings.TrimSpace(filter.JobID),
		})
		if subErr != nil {
			return subErr
		}
		progressCh = sub.Ticks
		unsubProgress = sub.Cancel
		if sub.UpdatePrincipal != nil {
			updateProgress = sub.UpdatePrincipal
		}
	}
	defer unsubProgress()

	if onAttached != nil {
		if err := onAttached(); err != nil {
			return err
		}
	}

	jobIDFilter := strings.TrimSpace(filter.JobID)
	keepOpen, err := s.drainCommitted(ctx, principal, jobIDFilter, &cursor, updateProgress, emit)
	if err != nil {
		if ctx.Err() == nil {
			return err
		}
		return nil
	}
	if !keepOpen {
		return nil
	}

	err = s.runLoop(ctx, principal, jobIDFilter, &cursor, wakeCh, manualCh, progressCh, updateProgress, emit)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *StreamService) runLoop(
	ctx context.Context,
	origin *domain.Principal,
	jobIDFilter string,
	cursor *int64,
	wakeCh <-chan struct{},
	manualCh <-chan struct{},
	progressCh <-chan domain.JobProgressTick,
	updateProgress func(*domain.Principal),
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
			keepOpen, err := s.drainCommitted(ctx, origin, jobIDFilter, cursor, updateProgress, emit)
			if err != nil || !keepOpen {
				return err
			}
		case <-manualCh:
			keepOpen, err := s.drainCommitted(ctx, origin, jobIDFilter, cursor, updateProgress, emit)
			if err != nil || !keepOpen {
				return err
			}
		case <-pollCh:
			keepOpen, err := s.drainCommitted(ctx, origin, jobIDFilter, cursor, updateProgress, emit)
			if err != nil || !keepOpen {
				return err
			}
		case tick, ok := <-progressCh:
			if !ok {
				progressCh = nil
				continue
			}
			keepOpen, err := s.handleProgressTick(ctx, origin, jobIDFilter, tick, updateProgress, emit)
			if err != nil || !keepOpen {
				return err
			}
		}
	}
}

func (s *StreamService) refreshLivePrincipal(
	ctx context.Context,
	origin *domain.Principal,
	updateProgress func(*domain.Principal),
) (*domain.Principal, bool, error) {
	live, valid, err := s.resolveLivePrincipal(ctx, origin)
	if err != nil || !valid {
		return nil, false, err
	}
	if updateProgress != nil {
		updateProgress(live)
	}
	return live, true, nil
}

func (s *StreamService) emitCommittedEvent(
	ctx context.Context,
	origin *domain.Principal,
	jobIDFilter string,
	cursor *int64,
	ev domain.DomainEvent,
	updateProgress func(*domain.Principal),
	emit func(app.StreamEvent) error,
) (bool, error) {
	seq, ok := ev.Cursor()
	if !ok {
		return true, nil
	}
	if seq > *cursor {
		*cursor = seq
	}
	streamEv, visible, keepOpen, err := s.projectJobEvent(ctx, origin, jobIDFilter, ev, seq, updateProgress)
	if err != nil {
		return false, err
	}
	if !keepOpen {
		return false, nil
	}
	if !visible {
		return true, nil
	}
	if err := emit(streamEv); err != nil {
		return false, err
	}
	return true, nil
}

func (s *StreamService) drainCommitted(
	ctx context.Context,
	origin *domain.Principal,
	jobIDFilter string,
	cursor *int64,
	updateProgress func(*domain.Principal),
	emit func(app.StreamEvent) error,
) (bool, error) {
	if _, valid, err := s.refreshLivePrincipal(ctx, origin, updateProgress); err != nil || !valid {
		return false, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		batch, err := s.events.ListPublishedEventsAfter(ctx, *cursor, s.batchSize)
		if err != nil {
			return false, err
		}
		if len(batch) == 0 {
			return true, nil
		}

		for _, ev := range batch {
			keepOpen, err := s.emitCommittedEvent(ctx, origin, jobIDFilter, cursor, ev, updateProgress, emit)
			if err != nil || !keepOpen {
				return false, err
			}
		}

		if len(batch) < s.batchSize {
			return true, nil
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

func (s *StreamService) lookupCredential(ctx context.Context, credID string) (*domain.Credential, error) {
	cred, err := s.users.GetCredentialByID(ctx, credID)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return cred, nil
}

func (s *StreamService) lookupSession(ctx context.Context, sessionID string) (*domain.Session, error) {
	sess, err := s.sessions.GetSessionByID(ctx, sessionID)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return sess, nil
}

func (s *StreamService) lookupUser(ctx context.Context, userID string) (*domain.User, error) {
	user, err := s.users.GetUserByID(ctx, userID)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return user, nil
}

func isUsableStreamCredential(cred *domain.Credential, origin *domain.Principal, now time.Time) bool {
	if cred == nil || origin == nil {
		return false
	}
	if cred.UserID != origin.User.ID || cred.Kind != origin.CredentialKind {
		return false
	}
	if cred.IsRevoked() || cred.IsExpired(now) {
		return false
	}
	if !domain.MatchesCredentialSecretDigest(cred.Kind, cred.SecretHash, origin.CredentialHashDigest) {
		return false
	}
	return cred.Validate() == nil
}

func (s *StreamService) resolveLivePrincipal(ctx context.Context, origin *domain.Principal) (*domain.Principal, bool, error) {
	if origin == nil || strings.TrimSpace(origin.User.ID) == "" || strings.TrimSpace(origin.CredentialID) == "" {
		return nil, false, nil
	}

	nowFn := time.Now
	if s.now != nil {
		nowFn = s.now
	}
	now := nowFn().UTC()

	cred, err := s.lookupCredential(ctx, origin.CredentialID)
	if err != nil {
		return nil, false, err
	}
	if !isUsableStreamCredential(cred, origin, now) {
		return nil, false, nil
	}

	if origin.SessionID != "" {
		if s.sessions == nil {
			return nil, false, nil
		}
		sess, err := s.lookupSession(ctx, origin.SessionID)
		if err != nil {
			return nil, false, err
		}
		if sess == nil || sess.UserID != origin.User.ID || sess.CredentialID != cred.ID ||
			sess.IsRevoked() || sess.IsExpired(now) ||
			!domain.MatchesCredentialSecretDigest(cred.Kind, cred.SecretHash, sess.CredentialHashDigest) {
			return nil, false, nil
		}
	}

	user, err := s.lookupUser(ctx, origin.User.ID)
	if err != nil {
		return nil, false, err
	}
	if user == nil {
		return nil, false, nil
	}

	roles, err := s.users.ListUserRoles(ctx, user.ID)
	if err != nil {
		return nil, false, err
	}

	live := domain.NewPrincipal(*user, roles, cred.Kind, cred.ID, cred.Scopes)
	live.SessionID = origin.SessionID
	live.CredentialHashDigest = origin.CredentialHashDigest
	return live, true, nil
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
	origin *domain.Principal,
	jobIDFilter string,
	ev domain.DomainEvent,
	seq int64,
	updateProgress func(*domain.Principal),
) (app.StreamEvent, bool, bool, error) {
	if !isJobLifecycleEventKind(ev.Kind) {
		return app.StreamEvent{}, false, true, nil
	}
	payload, ok := decodeJobEventPayload(ev)
	if !ok {
		return app.StreamEvent{}, false, true, nil
	}
	if jobIDFilter != "" && payload.JobID != jobIDFilter {
		return app.StreamEvent{}, false, true, nil
	}

	live, valid, err := s.refreshLivePrincipal(ctx, origin, updateProgress)
	if err != nil || !valid {
		return app.StreamEvent{}, false, false, err
	}

	job, err := s.lookupJob(ctx, payload.JobID)
	if err != nil {
		return app.StreamEvent{}, false, false, err
	}
	if job == nil || job.Kind.IsInternal() || !job.VisibleTo(live) {
		return app.StreamEvent{}, false, true, nil
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
		return app.StreamEvent{}, false, false, err
	}
	return app.StreamEvent{
		ID:   strconv.FormatInt(seq, 10),
		Name: string(ev.Kind),
		Data: data,
	}, true, true, nil
}

func (s *StreamService) handleProgressTick(
	ctx context.Context,
	origin *domain.Principal,
	jobIDFilter string,
	tick domain.JobProgressTick,
	updateProgress func(*domain.Principal),
	emit func(app.StreamEvent) error,
) (bool, error) {
	if tick.Kind.IsInternal() {
		return true, nil
	}
	if jobIDFilter != "" && tick.JobID != jobIDFilter {
		return true, nil
	}

	live, valid, err := s.refreshLivePrincipal(ctx, origin, updateProgress)
	if err != nil || !valid {
		return false, err
	}

	job, err := s.lookupJob(ctx, tick.JobID)
	if err != nil {
		return false, err
	}
	if job == nil || job.Kind.IsInternal() || !job.VisibleTo(live) {
		return true, nil
	}

	outTick := tick.Clone()
	if job.CreatedBy != nil {
		createdBy := *job.CreatedBy
		outTick.CreatedBy = &createdBy
	} else {
		outTick.CreatedBy = nil
	}

	if err := emitProgressTick(outTick, emit); err != nil {
		return false, err
	}
	return true, nil
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
