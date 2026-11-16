package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ storage.JobRepository = (*JobRepository)(nil)

const maxDedupeEnqueueAttempts = 5

// JobRepository implements storage.JobRepository backed by PostgreSQL and sqlc.
type JobRepository struct {
	q  *Queries
	tx TxBeginner
}

// NewJobRepository creates a new JobRepository using the provided database handle.
func NewJobRepository(db DBTX) *JobRepository {
	var beginner TxBeginner
	if b, ok := db.(TxBeginner); ok {
		beginner = b
	}
	return &JobRepository{
		q:  New(db),
		tx: beginner,
	}
}

// WithTx executes fn inside a transaction (or nested savepoint when already inside one).
func (r *JobRepository) WithTx(ctx context.Context, fn func(repo storage.JobRepository) error) error {
	_, err := runInJobTx(ctx, r, func(tx pgx.Tx, q *Queries) (struct{}, error) {
		return struct{}{}, fn(&JobRepository{q: q, tx: tx})
	})
	return err
}

func runInJobTx[T any](ctx context.Context, r *JobRepository, fn func(tx pgx.Tx, q *Queries) (T, error)) (T, error) {
	var zero T
	if r.tx == nil {
		return zero, errors.New("db connection does not support transactions")
	}
	tx, err := r.tx.Begin(ctx)
	if err != nil {
		return zero, fmt.Errorf("beginning job transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	res, err := fn(tx, r.q.WithTx(tx))
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("committing job transaction: %w", err)
	}
	return res, nil
}

func normalizeJobTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t.UTC()
}

func normalizeJobJSON(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage("{}"), nil
	}
	if !json.Valid(raw) {
		return nil, domain.ErrInvalidJobJSON
	}
	return raw, nil
}

func prepareJobForEnqueue(job *domain.Job) (domain.Job, error) {
	if job == nil {
		return domain.Job{}, errors.New("job cannot be nil")
	}
	candidate := *job

	if candidate.ID == "" {
		id, err := domain.NewID()
		if err != nil {
			return domain.Job{}, fmt.Errorf("generating job ID: %w", err)
		}
		candidate.ID = id
	}
	if candidate.State == "" {
		candidate.State = domain.JobStateQueued
	} else if candidate.State != domain.JobStateQueued {
		return domain.Job{}, fmt.Errorf("%w: new job must start in QUEUED state, got %s", domain.ErrInvalidJobTransition, candidate.State)
	}
	if candidate.Attempts != 0 {
		return domain.Job{}, domain.ErrInvalidJobAttempts
	}
	if candidate.MaxAttempts == 0 {
		candidate.MaxAttempts = domain.DefaultJobMaxAttempts
	}

	payload, err := normalizeJobJSON(candidate.Payload)
	if err != nil {
		return domain.Job{}, err
	}
	checkpoint, err := normalizeJobJSON(candidate.Checkpoint)
	if err != nil {
		return domain.Job{}, err
	}
	candidate.Payload = payload
	candidate.Checkpoint = checkpoint

	if err := candidate.Validate(); err != nil {
		return domain.Job{}, err
	}

	createdAt := normalizeJobTime(candidate.CreatedAt)
	updatedAt := createdAt
	if !candidate.UpdatedAt.IsZero() {
		updatedAt = candidate.UpdatedAt.UTC()
	}
	runAfter := createdAt
	if !candidate.RunAfter.IsZero() {
		runAfter = candidate.RunAfter.UTC()
	}
	candidate.CreatedAt = createdAt
	candidate.UpdatedAt = updatedAt
	candidate.RunAfter = runAfter
	return candidate, nil
}

func mapJobWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23503" {
			return storage.ErrNotFound
		}
	}
	return mapPgError(err)
}

// Enqueue inserts a new QUEUED job or returns the existing active job if dedupe_key collides.
func (r *JobRepository) Enqueue(ctx context.Context, job *domain.Job) (bool, error) {
	candidate, err := prepareJobForEnqueue(job)
	if err != nil {
		return false, err
	}

	params := EnqueueJobParams{
		ID:          candidate.ID,
		Kind:        string(candidate.Kind),
		Payload:     []byte(candidate.Payload),
		Checkpoint:  []byte(candidate.Checkpoint),
		MaxAttempts: int32(candidate.MaxAttempts),
		CreatedBy:   candidate.CreatedBy,
		DedupeKey:   candidate.DedupeKey,
		RunAfter:    candidate.RunAfter,
		CreatedAt:   candidate.CreatedAt,
		UpdatedAt:   candidate.UpdatedAt,
	}

	for range maxDedupeEnqueueAttempts {
		row, err := r.q.EnqueueJob(ctx, params)
		if err == nil {
			*job = JobToDomain(row)
			return true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return false, mapJobWriteError(err)
		}

		// ON CONFLICT DO NOTHING fired on idx_jobs_active_dedupe; look up the active job.
		existing, err := r.q.GetActiveJobByDedupeKey(ctx, candidate.DedupeKey)
		if err == nil {
			*job = JobToDomain(existing)
			return false, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("looking up active job for dedupe_key: %w", err)
		}
		// Active job completed between ON CONFLICT and SELECT; retry insert.
	}

	return false, errors.New("exhausted enqueue retries resolving active dedupe conflict")
}

// GetJobByID retrieves a job by its ID.
func (r *JobRepository) GetJobByID(ctx context.Context, id string) (*domain.Job, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyJobID
	}
	row, err := r.q.GetJobByID(ctx, id)
	if err != nil {
		return nil, mapPgError(err)
	}
	out := JobToDomain(row)
	return &out, nil
}

// ListJobs returns jobs matching filter ordered newest-first.
func (r *JobRepository) ListJobs(ctx context.Context, filter storage.JobFilter) ([]domain.Job, error) {
	if filter.Limit < 0 || filter.Offset < 0 || filter.Limit > math.MaxInt32 || filter.Offset > math.MaxInt32 {
		return nil, fmt.Errorf("invalid pagination limit=%d offset=%d", filter.Limit, filter.Offset)
	}

	var kindStr *string
	if filter.Kind != nil {
		s := string(*filter.Kind)
		kindStr = &s
	}
	var stateStr *string
	if filter.State != nil {
		s := string(*filter.State)
		stateStr = &s
	}
	var limit pgtype.Int4
	if filter.Limit > 0 {
		limit = pgtype.Int4{Int32: int32(filter.Limit), Valid: true}
	}

	rows, err := r.q.ListJobs(ctx, ListJobsParams{
		Kind:      kindStr,
		State:     stateStr,
		CreatedBy: filter.CreatedBy,
		RowOffset: int32(filter.Offset),
		RowLimit:  limit,
	})
	if err != nil {
		return nil, fmt.Errorf("listing jobs: %w", err)
	}

	out := make([]domain.Job, len(rows))
	for i, row := range rows {
		out[i] = JobToDomain(row)
	}
	return out, nil
}

// ClaimNext claims the next runnable QUEUED job of kind in a short transaction using FOR UPDATE SKIP LOCKED.
func (r *JobRepository) ClaimNext(ctx context.Context, kind domain.JobKind, now time.Time) (*domain.Job, error) {
	if strings.TrimSpace(string(kind)) == "" {
		return nil, domain.ErrEmptyJobKind
	}
	ts := normalizeJobTime(now)

	claimed, err := runInJobTx(ctx, r, func(_ pgx.Tx, q *Queries) (domain.Job, error) {
		jobID, err := q.GetNextRunnableJobIDForUpdate(ctx, GetNextRunnableJobIDForUpdateParams{
			Kind:     string(kind),
			RunAfter: ts,
		})
		if err != nil {
			return domain.Job{}, mapPgError(err)
		}
		row, err := q.ClaimJobByID(ctx, ClaimJobByIDParams{
			ID:        jobID,
			UpdatedAt: ts,
		})
		if err != nil {
			return domain.Job{}, fmt.Errorf("claiming job %q: %w", jobID, err)
		}
		return JobToDomain(row), nil
	})
	if err != nil {
		return nil, err
	}
	return &claimed, nil
}

// MarkSucceeded transitions a RUNNING job to SUCCEEDED.
func (r *JobRepository) MarkSucceeded(ctx context.Context, id string, now time.Time) (*domain.Job, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyJobID
	}
	ts := normalizeJobTime(now)

	updated, err := runInJobTx(ctx, r, func(_ pgx.Tx, q *Queries) (domain.Job, error) {
		current, err := q.GetJobByIDForUpdate(ctx, id)
		if err != nil {
			return domain.Job{}, mapPgError(err)
		}
		if !domain.JobState(current.State).CanTransitionTo(domain.JobStateSucceeded) {
			return domain.Job{}, fmt.Errorf("%w: cannot mark %s job as SUCCEEDED", domain.ErrInvalidJobTransition, current.State)
		}
		row, err := q.MarkJobSucceeded(ctx, MarkJobSucceededParams{
			ID:        id,
			UpdatedAt: ts,
		})
		if err != nil {
			return domain.Job{}, fmt.Errorf("marking job %q succeeded: %w", id, err)
		}
		return JobToDomain(row), nil
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

// FailOrRetry records a failure for a RUNNING job, requeueing with backoff while attempts < max_attempts
// (unless cancellation was requested) or transitioning to FAILED once max_attempts is reached.
func (r *JobRepository) FailOrRetry(ctx context.Context, id string, errMsg string, retryAt time.Time, now time.Time) (*domain.Job, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyJobID
	}
	ts := normalizeJobTime(now)

	updated, err := runInJobTx(ctx, r, func(_ pgx.Tx, q *Queries) (domain.Job, error) {
		current, err := q.GetJobByIDForUpdate(ctx, id)
		if err != nil {
			return domain.Job{}, mapPgError(err)
		}
		if current.State != string(domain.JobStateRunning) {
			return domain.Job{}, fmt.Errorf("%w: cannot fail or retry job in state %s", domain.ErrInvalidJobTransition, current.State)
		}

		var errPtr *string
		if errMsg != "" {
			errPtr = &errMsg
		}

		if current.CancelRequested {
			row, err := q.MarkJobCancelled(ctx, MarkJobCancelledParams{
				ID:        id,
				UpdatedAt: ts,
				LastError: errPtr,
			})
			if err != nil {
				return domain.Job{}, fmt.Errorf("cancelling failing job %q: %w", id, err)
			}
			return JobToDomain(row), nil
		}

		if int(current.Attempts) < int(current.MaxAttempts) {
			nextRun := retryAt
			if nextRun.IsZero() {
				nextRun = ts.Add(domain.JobRetryBackoff(int(current.Attempts)))
			} else {
				nextRun = nextRun.UTC()
			}
			row, err := q.RequeueJobForRetry(ctx, RequeueJobForRetryParams{
				ID:        id,
				LastError: errPtr,
				RunAfter:  nextRun,
				UpdatedAt: ts,
			})
			if err != nil {
				return domain.Job{}, fmt.Errorf("requeueing job %q for retry: %w", id, err)
			}
			return JobToDomain(row), nil
		}

		row, err := q.MarkJobFailed(ctx, MarkJobFailedParams{
			ID:        id,
			LastError: errPtr,
			UpdatedAt: ts,
		})
		if err != nil {
			return domain.Job{}, fmt.Errorf("marking job %q failed: %w", id, err)
		}
		return JobToDomain(row), nil
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

// RequestCancel cancels a QUEUED job immediately or sets cancel_requested=true on a RUNNING job.
func (r *JobRepository) RequestCancel(ctx context.Context, id string, now time.Time) (*domain.Job, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyJobID
	}
	ts := normalizeJobTime(now)

	updated, err := runInJobTx(ctx, r, func(_ pgx.Tx, q *Queries) (domain.Job, error) {
		current, err := q.GetJobByIDForUpdate(ctx, id)
		if err != nil {
			return domain.Job{}, mapPgError(err)
		}

		switch domain.JobState(current.State) {
		case domain.JobStateQueued:
			row, err := q.MarkJobCancelled(ctx, MarkJobCancelledParams{
				ID:        id,
				UpdatedAt: ts,
			})
			if err != nil {
				return domain.Job{}, fmt.Errorf("cancelling queued job %q: %w", id, err)
			}
			return JobToDomain(row), nil
		case domain.JobStateRunning:
			row, err := q.SetJobCancelRequested(ctx, SetJobCancelRequestedParams{
				ID:        id,
				UpdatedAt: ts,
			})
			if err != nil {
				return domain.Job{}, fmt.Errorf("requesting cancel on running job %q: %w", id, err)
			}
			return JobToDomain(row), nil
		default:
			return domain.Job{}, fmt.Errorf("%w: cannot cancel job in terminal state %s", domain.ErrInvalidJobTransition, current.State)
		}
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

// MarkCancelled transitions a QUEUED or RUNNING job to CANCELLED.
func (r *JobRepository) MarkCancelled(ctx context.Context, id string, errMsg *string, now time.Time) (*domain.Job, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.ErrEmptyJobID
	}
	ts := normalizeJobTime(now)

	updated, err := runInJobTx(ctx, r, func(_ pgx.Tx, q *Queries) (domain.Job, error) {
		current, err := q.GetJobByIDForUpdate(ctx, id)
		if err != nil {
			return domain.Job{}, mapPgError(err)
		}
		if !domain.JobState(current.State).CanTransitionTo(domain.JobStateCancelled) {
			return domain.Job{}, fmt.Errorf("%w: cannot mark %s job as CANCELLED", domain.ErrInvalidJobTransition, current.State)
		}
		row, err := q.MarkJobCancelled(ctx, MarkJobCancelledParams{
			ID:        id,
			UpdatedAt: ts,
			LastError: errMsg,
		})
		if err != nil {
			return domain.Job{}, fmt.Errorf("marking job %q cancelled: %w", id, err)
		}
		return JobToDomain(row), nil
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

// IsCancelRequested reports whether cancel_requested is set on the job.
func (r *JobRepository) IsCancelRequested(ctx context.Context, id string) (bool, error) {
	if strings.TrimSpace(id) == "" {
		return false, domain.ErrEmptyJobID
	}
	requested, err := r.q.IsJobCancelRequested(ctx, id)
	if err != nil {
		return false, mapPgError(err)
	}
	return requested, nil
}
