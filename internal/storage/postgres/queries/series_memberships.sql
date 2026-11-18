-- name: LockVolumeForMembershipUpdate :one
SELECT id
FROM volumes
WHERE id = $1
FOR UPDATE;

-- name: ListSeriesMembershipsByVolumeForWrite :many
SELECT volume_id, series_id, is_primary, kind, position, source, locked_fields
FROM series_memberships
WHERE volume_id = $1
ORDER BY is_primary DESC, series_id ASC;

-- name: ListSeriesMembershipsByVolumeForUpdate :many
SELECT volume_id, series_id, is_primary, kind, position, source, locked_fields
FROM series_memberships
WHERE volume_id = $1
ORDER BY is_primary DESC, series_id ASC
FOR UPDATE;

-- name: ListSeriesMembershipsBySeriesForWrite :many
SELECT volume_id, series_id, is_primary, kind, position, source, locked_fields
FROM series_memberships
WHERE series_id = $1
ORDER BY is_primary DESC, position ASC NULLS LAST, volume_id ASC;

-- name: GetSeriesMembershipForWrite :one
SELECT volume_id, series_id, is_primary, kind, position, source, locked_fields
FROM series_memberships
WHERE volume_id = $1
  AND series_id = $2;

-- name: DeleteSeriesMembershipsBySeriesIDs :exec
DELETE FROM series_memberships
WHERE volume_id = $1
  AND series_id = ANY(sqlc.arg(series_ids)::text[]);

-- name: DemotePrimarySeriesMembership :exec
UPDATE series_memberships
SET is_primary = FALSE
WHERE volume_id = $1
  AND is_primary = TRUE;

-- name: PromotePrimarySeriesMembership :one
UPDATE series_memberships
SET is_primary = TRUE
WHERE volume_id = $1
  AND series_id = $2
RETURNING volume_id, series_id, is_primary, kind, position, source, locked_fields;

-- name: UpdateSeriesMembership :one
UPDATE series_memberships
SET is_primary = $3,
    kind = $4,
    position = $5,
    source = $6,
    locked_fields = $7
WHERE volume_id = $1
  AND series_id = $2
RETURNING volume_id, series_id, is_primary, kind, position, source, locked_fields;

-- name: InsertSeriesMembership :one
INSERT INTO series_memberships (
    volume_id,
    series_id,
    is_primary,
    kind,
    position,
    source,
    locked_fields
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING volume_id, series_id, is_primary, kind, position, source, locked_fields;
