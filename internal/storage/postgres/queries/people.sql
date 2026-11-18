-- name: CreatePerson :one
INSERT INTO people (
    id,
    name,
    name_native,
    name_romanized,
    aliases,
    external_ids,
    locked_fields,
    import_key
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id, name, name_native, name_romanized, aliases, external_ids, locked_fields, import_key;

-- name: FindOrInsertPersonByImportKey :one
INSERT INTO people (
    id,
    name,
    name_native,
    name_romanized,
    aliases,
    external_ids,
    locked_fields,
    import_key
)
VALUES ($1, $2, NULL, NULL, '{}', '{}', '{}', $3)
ON CONFLICT (import_key) WHERE import_key IS NOT NULL DO NOTHING
RETURNING id, name, name_native, name_romanized, aliases, external_ids, locked_fields, import_key;

-- name: GetPersonByID :one
SELECT id, name, name_native, name_romanized, aliases, external_ids, locked_fields, import_key
FROM people
WHERE id = $1;

-- name: GetPersonByIDForUpdate :one
SELECT id, name, name_native, name_romanized, aliases, external_ids, locked_fields, import_key
FROM people
WHERE id = $1
FOR UPDATE;

-- name: GetPersonByImportKey :one
SELECT id, name, name_native, name_romanized, aliases, external_ids, locked_fields, import_key
FROM people
WHERE import_key = $1;

-- name: UpdatePerson :one
UPDATE people
SET name = $2,
    name_native = $3,
    name_romanized = $4,
    aliases = $5,
    external_ids = $6,
    locked_fields = $7,
    import_key = $8
WHERE id = $1
RETURNING id, name, name_native, name_romanized, aliases, external_ids, locked_fields, import_key;

-- name: DeletePerson :execrows
DELETE FROM people
WHERE id = $1;

-- name: LockSeriesForCreditUpdate :one
SELECT id
FROM series
WHERE id = $1
FOR UPDATE;

-- name: LockVolumeForCreditUpdate :one
SELECT id
FROM volumes
WHERE id = $1
FOR UPDATE;

-- name: CreateCredit :one
INSERT INTO credits (
    id,
    person_id,
    series_id,
    volume_id,
    role,
    position,
    locked_fields
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, person_id, series_id, volume_id, role, position, locked_fields;

-- name: GetCreditByID :one
SELECT id, person_id, series_id, volume_id, role, position, locked_fields
FROM credits
WHERE id = $1;

-- name: UpdateCredit :one
UPDATE credits
SET person_id = $2,
    series_id = $3,
    volume_id = $4,
    role = $5,
    position = $6,
    locked_fields = $7
WHERE id = $1
RETURNING id, person_id, series_id, volume_id, role, position, locked_fields;

-- name: DeleteCredit :execrows
DELETE FROM credits
WHERE id = $1;

-- name: DeleteCreditsByIDs :exec
DELETE FROM credits
WHERE id = ANY(sqlc.arg(ids)::text[]);

-- name: ListCreditsBySeries :many
SELECT id, person_id, series_id, volume_id, role, position, locked_fields
FROM credits
WHERE series_id = $1
ORDER BY position ASC, role ASC, id ASC;

-- name: ListCreditsBySeriesForUpdate :many
SELECT id, person_id, series_id, volume_id, role, position, locked_fields
FROM credits
WHERE series_id = $1
ORDER BY position ASC, role ASC, id ASC
FOR UPDATE;

-- name: ListCreditsByVolume :many
SELECT id, person_id, series_id, volume_id, role, position, locked_fields
FROM credits
WHERE volume_id = $1
ORDER BY position ASC, role ASC, id ASC;

-- name: ListCreditsByVolumeForUpdate :many
SELECT id, person_id, series_id, volume_id, role, position, locked_fields
FROM credits
WHERE volume_id = $1
ORDER BY position ASC, role ASC, id ASC
FOR UPDATE;

-- name: ListCreditsByPerson :many
SELECT id, person_id, series_id, volume_id, role, position, locked_fields
FROM credits
WHERE person_id = $1
ORDER BY position ASC, id ASC;

-- name: CreateEntity :one
INSERT INTO entities (
    id,
    kind,
    name,
    aliases,
    external_ids,
    locked_fields,
    import_key
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, kind, name, aliases, external_ids, locked_fields, import_key;

-- name: FindOrInsertEntityByImportKey :one
INSERT INTO entities (
    id,
    kind,
    name,
    aliases,
    external_ids,
    locked_fields,
    import_key
)
VALUES ($1, $2, $3, '{}', '{}', '{}', $4)
ON CONFLICT (kind, import_key) WHERE import_key IS NOT NULL DO NOTHING
RETURNING id, kind, name, aliases, external_ids, locked_fields, import_key;

-- name: GetEntityByID :one
SELECT id, kind, name, aliases, external_ids, locked_fields, import_key
FROM entities
WHERE id = $1;

-- name: GetEntityByIDForUpdate :one
SELECT id, kind, name, aliases, external_ids, locked_fields, import_key
FROM entities
WHERE id = $1
FOR UPDATE;

-- name: GetEntityByImportKey :one
SELECT id, kind, name, aliases, external_ids, locked_fields, import_key
FROM entities
WHERE kind = $1
  AND import_key = $2;

-- name: UpdateEntity :one
UPDATE entities
SET kind = $2,
    name = $3,
    aliases = $4,
    external_ids = $5,
    locked_fields = $6,
    import_key = $7
WHERE id = $1
RETURNING id, kind, name, aliases, external_ids, locked_fields, import_key;

-- name: DeleteEntity :execrows
DELETE FROM entities
WHERE id = $1;

-- name: ListVolumeEntities :many
SELECT volume_id, entity_id, locked_fields
FROM volume_entities
WHERE volume_id = $1
ORDER BY entity_id ASC;

-- name: ListVolumeEntitiesForUpdate :many
SELECT volume_id, entity_id, locked_fields
FROM volume_entities
WHERE volume_id = $1
ORDER BY entity_id ASC
FOR UPDATE;

-- name: ListVolumeEntitiesByEntity :many
SELECT volume_id, entity_id, locked_fields
FROM volume_entities
WHERE entity_id = $1
ORDER BY volume_id ASC;

-- name: InsertVolumeEntity :one
INSERT INTO volume_entities (
    volume_id,
    entity_id,
    locked_fields
)
VALUES ($1, $2, $3)
RETURNING volume_id, entity_id, locked_fields;

-- name: UpdateVolumeEntity :one
UPDATE volume_entities
SET locked_fields = $3
WHERE volume_id = $1
  AND entity_id = $2
RETURNING volume_id, entity_id, locked_fields;

-- name: DeleteVolumeEntitiesByEntityIDs :exec
DELETE FROM volume_entities
WHERE volume_id = $1
  AND entity_id = ANY(sqlc.arg(entity_ids)::text[]);
