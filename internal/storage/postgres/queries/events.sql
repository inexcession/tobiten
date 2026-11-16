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
