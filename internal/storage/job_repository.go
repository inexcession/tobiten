package storage

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// JobFilter specifies optional filtering and pagination for listing jobs.
type JobFilter struct {
	Kind      *domain.JobKind
	State     *domain.JobState
	CreatedBy *string
	Limit     int
	Offset    int
}

// JobRepository defines persistence and state-machine operations for durable background jobs (ADR-0017).
type JobRepository interface {
	// WithTx executes fn inside a transaction (or savepoint when already inside one).
	WithTx(ctx context.Context, fn func(repo JobRepository) error) error

	// Enqueue inserts a new QUEUED job, or returns the existing active (QUEUED or RUNNING) job
	// when a non-nil dedupe_key collides with idx_jobs_active_dedupe.
	// It populates *job with the persisted row and reports created=true when newly inserted
	// or created=false when deduplicated to an active job.
	Enqueue(ctx context.Context, job *domain.Job) (created bool, err error)

	// GetJobByID retrieves a job by its ID. Returns ErrNotFound if no such job exists.
	GetJobByID(ctx context.Context, id string) (*domain.Job, error)

	// ListJobs returns jobs matching filter ordered newest-first.
	ListJobs(ctx context.Context, filter JobFilter) ([]domain.Job, error)

	// ClaimNext claims the next eligible QUEUED job of the given kind (run_after <= now,
	// cancel_requested = false, attempts < max_attempts) in a short transaction using
	// FOR UPDATE SKIP LOCKED, moving it to RUNNING and incrementing attempts.
	// Returns ErrNotFound if no runnable job of that kind is currently eligible.
	ClaimNext(ctx context.Context, kind domain.JobKind, now time.Time) (*domain.Job, error)

	// MarkSucceeded transitions a RUNNING job at the claimed attempt to SUCCEEDED,
	// clearing last_error and setting finished_at. Returns ErrNotFound if missing,
	// domain.ErrStaleJobAttempt if the job is at a different attempt, or
	// domain.ErrInvalidJobTransition if the job is not RUNNING.
	MarkSucceeded(ctx context.Context, id string, attempt int, now time.Time) (*domain.Job, error)

	// FailOrRetry records a failure for a RUNNING job at the claimed attempt.
	// If cancel_requested is true, the job transitions to CANCELLED instead of retrying.
	// Otherwise, if attempts < max_attempts it returns to QUEUED with run_after set to retryAt
	// (or now + domain.JobRetryBackoff(attempts) when retryAt is zero); if attempts >= max_attempts
	// it transitions to FAILED. Returns domain.ErrStaleJobAttempt / domain.ErrInvalidJobTransition
	// if the job is no longer RUNNING at attempt.
	FailOrRetry(ctx context.Context, id string, attempt int, errMsg string, retryAt time.Time, now time.Time) (*domain.Job, error)

	// RequestCancel requests cancellation of an active job.
	// A QUEUED job transitions immediately to CANCELLED (releasing any dedupe_key).
	// A RUNNING job sets cancel_requested=true so the worker cancels at its next safe point.
	// Terminal jobs return domain.ErrInvalidJobTransition.
	RequestCancel(ctx context.Context, id string, now time.Time) (*domain.Job, error)

	// MarkCancelled transitions a RUNNING job at the claimed attempt to CANCELLED,
	// setting cancel_requested=true and finished_at. Returns domain.ErrStaleJobAttempt /
	// domain.ErrInvalidJobTransition if the job is not RUNNING at attempt.
	MarkCancelled(ctx context.Context, id string, attempt int, errMsg *string, now time.Time) (*domain.Job, error)

	// IsCancelRequested reports whether cancel_requested is set on the job.
	// Returns ErrNotFound if no such job exists.
	IsCancelRequested(ctx context.Context, id string) (bool, error)
}
