-- name: EnqueueJob :one
INSERT INTO jobs (
    id,
    kind,
    state,
    payload,
    checkpoint,
    attempts,
    max_attempts,
    cancel_requested,
    last_error,
    created_by,
    dedupe_key,
    run_after,
    created_at,
    started_at,
    updated_at,
    finished_at
)
VALUES (
    $1, $2, 'QUEUED', $3, $4, 0, $5, FALSE, NULL, $6, $7, $8, $9, NULL, $10, NULL
)
ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL AND state IN ('QUEUED', 'RUNNING') DO NOTHING
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: GetActiveJobByDedupeKey :one
SELECT id, kind, state, payload, checkpoint, attempts, max_attempts,
       cancel_requested, last_error, created_by, dedupe_key,
       run_after, created_at, started_at, updated_at, finished_at
FROM jobs
WHERE dedupe_key = $1
  AND state IN ('QUEUED', 'RUNNING')
LIMIT 1;

-- name: GetJobByID :one
SELECT id, kind, state, payload, checkpoint, attempts, max_attempts,
       cancel_requested, last_error, created_by, dedupe_key,
       run_after, created_at, started_at, updated_at, finished_at
FROM jobs
WHERE id = $1;

-- name: GetJobByIDForUpdate :one
SELECT id, kind, state, payload, checkpoint, attempts, max_attempts,
       cancel_requested, last_error, created_by, dedupe_key,
       run_after, created_at, started_at, updated_at, finished_at
FROM jobs
WHERE id = $1
FOR UPDATE;

-- name: ListJobs :many
SELECT id, kind, state, payload, checkpoint, attempts, max_attempts,
       cancel_requested, last_error, created_by, dedupe_key,
       run_after, created_at, started_at, updated_at, finished_at
FROM jobs
WHERE (sqlc.narg(kind)::text IS NULL OR kind = sqlc.narg(kind)::text)
  AND (sqlc.narg(state)::text IS NULL OR state = sqlc.narg(state)::text)
  AND (sqlc.narg(created_by)::text IS NULL OR created_by = sqlc.narg(created_by)::text)
ORDER BY created_at DESC, id DESC
LIMIT sqlc.narg(row_limit)::int OFFSET sqlc.arg(row_offset)::int;

-- name: GetNextRunnableJobIDForUpdate :one
SELECT id
FROM jobs
WHERE kind = $1
  AND state = 'QUEUED'
  AND run_after <= $2
  AND cancel_requested = FALSE
  AND attempts < max_attempts
ORDER BY run_after ASC, created_at ASC, id ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;

-- name: ClaimJobByID :one
UPDATE jobs
SET state = 'RUNNING',
    attempts = attempts + 1,
    started_at = COALESCE(started_at, $2),
    updated_at = $2
WHERE id = $1
  AND state = 'QUEUED'
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: MarkJobSucceeded :one
UPDATE jobs
SET state = 'SUCCEEDED',
    last_error = NULL,
    updated_at = $2,
    finished_at = $2
WHERE id = $1
  AND state = 'RUNNING'
  AND attempts = $3
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: RequeueJobForRetry :one
UPDATE jobs
SET state = 'QUEUED',
    last_error = $2,
    run_after = $3,
    updated_at = $4,
    finished_at = NULL
WHERE id = $1
  AND state = 'RUNNING'
  AND attempts = $5
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: MarkJobFailed :one
UPDATE jobs
SET state = 'FAILED',
    last_error = $2,
    updated_at = $3,
    finished_at = $3
WHERE id = $1
  AND state = 'RUNNING'
  AND attempts = $4
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: MarkJobCancelled :one
UPDATE jobs
SET state = 'CANCELLED',
    cancel_requested = TRUE,
    last_error = COALESCE(sqlc.narg(last_error)::text, last_error),
    updated_at = $2,
    finished_at = $2
WHERE id = $1
  AND state = 'RUNNING'
  AND attempts = $3
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: CancelQueuedJob :one
UPDATE jobs
SET state = 'CANCELLED',
    cancel_requested = TRUE,
    updated_at = $2,
    finished_at = $2
WHERE id = $1
  AND state = 'QUEUED'
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: SetJobCancelRequested :one
UPDATE jobs
SET cancel_requested = TRUE,
    updated_at = $2
WHERE id = $1
  AND state = 'RUNNING'
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: IsJobCancelRequested :one
SELECT cancel_requested
FROM jobs
WHERE id = $1;

-- name: SaveJobCheckpoint :one
UPDATE jobs
SET checkpoint = $2,
    updated_at = $3
WHERE id = $1
  AND state = 'RUNNING'
  AND attempts = $4
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: ListRunningJobs :many
SELECT id, kind, state, payload, checkpoint, attempts, max_attempts,
       cancel_requested, last_error, created_by, dedupe_key,
       run_after, created_at, started_at, updated_at, finished_at
FROM jobs
WHERE state = 'RUNNING'
ORDER BY created_at ASC, id ASC;

-- name: RequeueInterruptedJob :one
UPDATE jobs
SET state = 'QUEUED',
    checkpoint = $2,
    last_error = COALESCE(sqlc.narg(last_error)::text, last_error),
    run_after = $3,
    updated_at = $4,
    finished_at = NULL
WHERE id = $1
  AND state = 'RUNNING'
  AND attempts = $5
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: RecoverJobCancelled :one
UPDATE jobs
SET state = 'CANCELLED',
    cancel_requested = TRUE,
    checkpoint = $2,
    last_error = COALESCE(sqlc.narg(last_error)::text, last_error),
    updated_at = $3,
    finished_at = $3
WHERE id = $1
  AND state = 'RUNNING'
  AND attempts = $4
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: RecoverJobFailed :one
UPDATE jobs
SET state = 'FAILED',
    checkpoint = $2,
    last_error = $3,
    updated_at = $4,
    finished_at = $4
WHERE id = $1
  AND state = 'RUNNING'
  AND attempts = $5
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: RecoverJobSucceeded :one
UPDATE jobs
SET state = 'SUCCEEDED',
    checkpoint = $2,
    last_error = NULL,
    updated_at = $3,
    finished_at = $3
WHERE id = $1
  AND state = 'RUNNING'
  AND attempts = $4
RETURNING id, kind, state, payload, checkpoint, attempts, max_attempts,
          cancel_requested, last_error, created_by, dedupe_key,
          run_after, created_at, started_at, updated_at, finished_at;

-- name: HasFailedEventDeliveryJobSince :one
SELECT EXISTS (
    SELECT 1
    FROM jobs
    WHERE kind = 'EVENT_DELIVERY'
      AND state = 'FAILED'
      AND dedupe_key = ('EVENT_DELIVERY:' || sqlc.arg(subscriber_id)::text)
      AND payload @> jsonb_build_object(
          'subscriber_id', sqlc.arg(subscriber_id)::text,
          'published_sequence', sqlc.arg(published_sequence)::bigint
      )
      AND updated_at >= sqlc.arg(since)::timestamptz
);
