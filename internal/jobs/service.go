package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

// JobNotifier wakes bounded worker pools when a new job is enqueued.
type JobNotifier interface {
	Notify(kind domain.JobKind)
}

// TriggerRequest specifies parameters for enqueueing a durable background job (ADR-0017 §1–§2).
type TriggerRequest struct {
	Kind               domain.JobKind
	Payload            json.RawMessage
	DedupeKey          *string
	LockKey            string
	ConflictDedupeKeys []string
	MaxAttempts        int
	CreatedBy          *string
	RunAfter           time.Time
}

// TriggerJob validates req, enqueues a durable QUEUED job (returning the existing active
// job ID if an active dedupe_key collides), wakes notifier when newly created outside a
// caller transaction, and returns the resulting job ID (ADR-0017 §1–§2).
func TriggerJob(
	ctx context.Context,
	repo storage.JobRepository,
	notifier JobNotifier,
	req TriggerRequest,
	now time.Time,
) (string, error) {
	if repo == nil {
		return "", fmt.Errorf("%w: job repository is unavailable", app.ErrServiceBusy)
	}
	if strings.TrimSpace(string(req.Kind)) == "" {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyJobKind)
	}
	if req.DedupeKey != nil && strings.TrimSpace(*req.DedupeKey) == "" {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyDedupeKey)
	}
	for _, key := range req.ConflictDedupeKeys {
		if strings.TrimSpace(key) == "" {
			return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrEmptyDedupeKey)
		}
	}
	if req.CreatedBy != nil && strings.TrimSpace(*req.CreatedBy) == "" {
		return "", fmt.Errorf("%w: created_by cannot be empty when present", app.ErrInvalidInput)
	}
	if len(req.Payload) > 0 && !json.Valid(req.Payload) {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrInvalidJobJSON)
	}
	if req.MaxAttempts < 0 {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, domain.ErrInvalidJobAttempts)
	}

	ts := now.UTC()
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	job := domain.Job{
		Kind:        req.Kind,
		State:       domain.JobStateQueued,
		Payload:     req.Payload,
		MaxAttempts: req.MaxAttempts,
		CreatedBy:   req.CreatedBy,
		DedupeKey:   req.DedupeKey,
		RunAfter:    req.RunAfter,
		CreatedAt:   ts,
	}

	var (
		created bool
		err     error
	)
	if req.LockKey != "" || len(req.ConflictDedupeKeys) > 0 {
		created, err = repo.EnqueueExclusive(ctx, &job, req.LockKey, req.ConflictDedupeKeys)
	} else {
		created, err = repo.Enqueue(ctx, &job)
	}
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrJobConflict):
			return "", fmt.Errorf("%w: %w", app.ErrConflict, err)
		case errors.Is(err, domain.ErrEmptyJobKind),
			errors.Is(err, domain.ErrInvalidJobJSON),
			errors.Is(err, domain.ErrEmptyDedupeKey),
			errors.Is(err, domain.ErrInvalidJobAttempts),
			errors.Is(err, domain.ErrInvalidDeliveryPayload),
			errors.Is(err, domain.ErrInvalidEventDeliveryDedupeKey):
			return "", fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
		case errors.Is(err, storage.ErrNotFound):
			return "", fmt.Errorf("%w: %w", app.ErrNotFound, err)
		default:
			return "", err
		}
	}

	if created && notifier != nil && !repo.InTx() {
		notifier.Notify(job.Kind)
	}
	return job.ID, nil
}

// ServiceOptions configures the concrete application job service.
type ServiceOptions struct {
	Now          func() time.Time
	Notifier     JobNotifier
	OnTransition func(domain.Job)
}

// Service implements app.JobService over storage.JobRepository with object-level
// creator/jobs:read_all visibility and I-10 cancellation authority enforcement.
type Service struct {
	repo         storage.JobRepository
	now          func() time.Time
	mu           sync.RWMutex
	notifier     JobNotifier
	onTransition func(domain.Job)
}

var _ app.JobService = (*Service)(nil)

// NewService constructs a concrete job Service backed by repo.
func NewService(repo storage.JobRepository, opts ServiceOptions) *Service {
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Service{
		repo:         repo,
		now:          nowFn,
		notifier:     opts.Notifier,
		onTransition: opts.OnTransition,
	}
}

// SetNotifier attaches or updates the pool notifier used by Trigger.
func (s *Service) SetNotifier(notifier JobNotifier) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.notifier = notifier
	s.mu.Unlock()
}

// Notify forwards a worker-pool wakeup signal to the attached notifier, if any.
func (s *Service) Notify(kind domain.JobKind) {
	if s == nil {
		return
	}
	if notifier := s.currentNotifier(); notifier != nil {
		notifier.Notify(kind)
	}
}

// SetOnTransition attaches or updates the post-commit lifecycle transition callback.
func (s *Service) SetOnTransition(fn func(domain.Job)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.onTransition = fn
	s.mu.Unlock()
}

func (s *Service) currentNotifier() JobNotifier {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.notifier
}

func (s *Service) notifyTransition(job domain.Job) {
	if s == nil || job.Kind.IsInternal() {
		return
	}
	s.mu.RLock()
	fn := s.onTransition
	s.mu.RUnlock()
	if fn == nil {
		return
	}
	if _, ok := domain.JobLifecycleEventKind(job.State); ok {
		fn(job)
	}
}

// Trigger enqueues a background job using the shared durable trigger rules (ADR-0017 §1–§2).
func (s *Service) Trigger(ctx context.Context, req TriggerRequest) (string, error) {
	if s == nil || s.repo == nil {
		return "", fmt.Errorf("%w: job service is unavailable", app.ErrServiceBusy)
	}
	return TriggerJob(ctx, s.repo, s.currentNotifier(), req, s.now())
}

// ListJobs returns jobs visible to principal (creator-owned only, or all jobs when holding jobs:read_all).
func (s *Service) ListJobs(ctx context.Context, principal *domain.Principal) ([]app.JobInfo, error) {
	if principal == nil {
		return nil, app.ErrUnauthorized
	}
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("%w: job service is unavailable", app.ErrServiceBusy)
	}

	var filter storage.JobFilter
	if !principal.HasPermission(domain.PermissionJobsReadAll) {
		if strings.TrimSpace(principal.User.ID) == "" {
			return []app.JobInfo{}, nil
		}
		uid := principal.User.ID
		filter.CreatedBy = &uid
	}

	rows, err := s.repo.ListJobs(ctx, filter)
	if err != nil {
		return nil, err
	}

	out := make([]app.JobInfo, 0, len(rows))
	for _, row := range rows {
		if !row.VisibleTo(principal) {
			continue
		}
		out = append(out, app.JobInfoFromDomain(row))
	}
	return out, nil
}

// GetJob returns job details if visible to principal (creator or jobs:read_all).
// Both absent and invisible jobs return app.ErrNotFound to prevent object-ID leakage.
func (s *Service) GetJob(ctx context.Context, principal *domain.Principal, jobID string) (*app.JobInfo, error) {
	if principal == nil {
		return nil, app.ErrUnauthorized
	}
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("%w: job service is unavailable", app.ErrServiceBusy)
	}
	if strings.TrimSpace(jobID) == "" {
		return nil, app.ErrNotFound
	}

	job, err := s.repo.GetJobByID(ctx, jobID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, app.ErrNotFound
		}
		return nil, err
	}
	if !job.VisibleTo(principal) {
		return nil, app.ErrNotFound
	}

	info := app.JobInfoFromDomain(*job)
	return &info, nil
}

// CancelJob requests cancellation of jobID according to decision I-10:
//   - Absent or invisible job -> app.ErrNotFound
//   - Visible job without cancellation authority -> app.ErrForbidden
//   - Authorized caller targeting a terminal job -> app.ErrConflict
//   - Queued job transitions immediately to CANCELLED; running job keeps RUNNING with cancel_requested=true.
func (s *Service) CancelJob(ctx context.Context, principal *domain.Principal, jobID string) (*app.JobInfo, error) {
	if principal == nil {
		return nil, app.ErrUnauthorized
	}
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("%w: job service is unavailable", app.ErrServiceBusy)
	}
	if strings.TrimSpace(jobID) == "" {
		return nil, app.ErrNotFound
	}

	var updated *domain.Job
	err := s.repo.WithTx(ctx, func(txRepo storage.JobRepository) error {
		job, err := txRepo.GetJobByIDForUpdate(ctx, jobID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return app.ErrNotFound
			}
			return err
		}
		if !job.VisibleTo(principal) {
			return app.ErrNotFound
		}
		if !job.CanCancel(principal) {
			return app.ErrForbidden
		}

		res, err := txRepo.RequestCancel(ctx, jobID, s.now())
		if err != nil {
			switch {
			case errors.Is(err, storage.ErrNotFound):
				return app.ErrNotFound
			case errors.Is(err, domain.ErrInvalidJobTransition):
				return app.ErrConflict
			default:
				return err
			}
		}
		updated = res
		return nil
	})
	if err != nil {
		return nil, err
	}

	if updated.State == domain.JobStateCancelled {
		s.notifyTransition(*updated)
	}
	info := app.JobInfoFromDomain(*updated)
	return &info, nil
}
