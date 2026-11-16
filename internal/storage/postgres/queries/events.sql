-- name: AppendDomainEvent :one
INSERT INTO domain_events (
    published_sequence,
    published_at,
    kind,
    version,
    aggregate_type,
    aggregate_id,
    payload,
    created_at
)
VALUES (
    NULL, NULL, $1, $2, $3, $4, $5, $6
)
RETURNING id, published_sequence, published_at, kind, version, aggregate_type, aggregate_id, payload, created_at;

-- name: GetDomainEventByID :one
SELECT id, published_sequence, published_at, kind, version, aggregate_type, aggregate_id, payload, created_at
FROM domain_events
WHERE id = $1;

-- name: LockDomainEventSequence :one
SELECT last_sequence
FROM domain_event_sequence
WHERE id = TRUE
FOR UPDATE;

-- name: PublishUnsequencedDomainEvents :many
WITH base AS (
    SELECT GREATEST(
        sqlc.arg(base_sequence)::bigint,
        COALESCE((SELECT MAX(published_sequence) FROM domain_events WHERE published_sequence IS NOT NULL), 0)
    ) AS seq
),
numbered AS (
    SELECT
        e.id,
        (SELECT seq FROM base) + ROW_NUMBER() OVER (ORDER BY e.id ASC) AS next_seq
    FROM domain_events e
    WHERE e.published_sequence IS NULL
    ORDER BY e.id ASC
),
updated AS (
    UPDATE domain_events d
    SET
        published_sequence = n.next_seq,
        published_at = sqlc.arg(published_at)::timestamptz
    FROM numbered n
    WHERE d.id = n.id
      AND d.published_sequence IS NULL
    RETURNING
        d.id,
        d.published_sequence,
        d.published_at,
        d.kind,
        d.version,
        d.aggregate_type,
        d.aggregate_id,
        d.payload,
        d.created_at
)
SELECT
    id,
    published_sequence,
    published_at,
    kind,
    version,
    aggregate_type,
    aggregate_id,
    payload,
    created_at
FROM updated
ORDER BY published_sequence ASC;

-- name: UpdateDomainEventSequence :exec
UPDATE domain_event_sequence
SET last_sequence = $1
WHERE id = TRUE;
