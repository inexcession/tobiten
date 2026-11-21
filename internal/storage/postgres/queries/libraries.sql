-- name: CreateLibrary :one
INSERT INTO libraries (
    id,
    name,
    mode,
    intake_path_bytes,
    intake_path_display,
    intake_path_match,
    auto_metadata_writeback,
    resolver_chain,
    metadata_precedence,
    default_cover_policy,
    created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING id, name, mode, intake_path_bytes, intake_path_display, intake_path_match,
          auto_metadata_writeback, resolver_chain, metadata_precedence, default_cover_policy, created_at;

-- name: GetLibraryByID :one
SELECT id, name, mode, intake_path_bytes, intake_path_display, intake_path_match,
       auto_metadata_writeback, resolver_chain, metadata_precedence, default_cover_policy, created_at
FROM libraries
WHERE id = $1;

-- name: GetLibraryByName :one
SELECT id, name, mode, intake_path_bytes, intake_path_display, intake_path_match,
       auto_metadata_writeback, resolver_chain, metadata_precedence, default_cover_policy, created_at
FROM libraries
WHERE name = $1;

-- name: ListLibraries :many
SELECT id, name, mode, intake_path_bytes, intake_path_display, intake_path_match,
       auto_metadata_writeback, resolver_chain, metadata_precedence, default_cover_policy, created_at
FROM libraries
ORDER BY created_at ASC, id ASC;

-- name: UpdateLibrary :one
UPDATE libraries
SET name = $2,
    mode = $3,
    intake_path_bytes = $4,
    intake_path_display = $5,
    intake_path_match = $6,
    auto_metadata_writeback = $7,
    resolver_chain = $8,
    metadata_precedence = $9,
    default_cover_policy = $10
WHERE id = $1
RETURNING id, name, mode, intake_path_bytes, intake_path_display, intake_path_match,
          auto_metadata_writeback, resolver_chain, metadata_precedence, default_cover_policy, created_at;

-- name: LockLibrary :one
SELECT id
FROM libraries
WHERE id = $1
FOR UPDATE;

-- name: LibraryHasCatalogueRows :one
SELECT (
    EXISTS (SELECT 1 FROM series s WHERE s.library_id = $1)
    OR EXISTS (SELECT 1 FROM volumes v WHERE v.library_id = $1)
)::boolean AS has_rows;

-- name: DeleteLibrary :execrows
DELETE FROM libraries
WHERE id = $1;

-- name: CreateLibraryRoot :one
INSERT INTO library_roots (
    id,
    library_id,
    path_bytes,
    path_display,
    path_match,
    state,
    last_checked_at,
    last_error
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id, library_id, path_bytes, path_display, path_match, state, last_checked_at, last_error;

-- name: GetLibraryRootByID :one
SELECT id, library_id, path_bytes, path_display, path_match, state, last_checked_at, last_error
FROM library_roots
WHERE id = $1;

-- name: ListLibraryRootsByLibrary :many
SELECT id, library_id, path_bytes, path_display, path_match, state, last_checked_at, last_error
FROM library_roots
WHERE library_id = $1
ORDER BY path_match ASC, id ASC;

-- name: ListAllLibraryRoots :many
SELECT id, library_id, path_bytes, path_display, path_match, state, last_checked_at, last_error
FROM library_roots
ORDER BY library_id ASC, path_match ASC, id ASC;

-- name: UpdateLibraryRoot :one
UPDATE library_roots
SET library_id = $2,
    path_bytes = $3,
    path_display = $4,
    path_match = $5,
    state = $6,
    last_checked_at = $7,
    last_error = $8
WHERE id = $1
RETURNING id, library_id, path_bytes, path_display, path_match, state, last_checked_at, last_error;

-- name: UpdateLibraryRootStatus :execrows
UPDATE library_roots
SET state = $2,
    last_checked_at = $3,
    last_error = $4
WHERE id = $1;

-- name: DeleteLibraryRoot :execrows
DELETE FROM library_roots
WHERE id = $1;

-- name: GetTransactionIsolation :one
SELECT current_setting('transaction_isolation')::text AS isolation_level;

-- name: AcquireLibraryConfigLock :exec
SELECT pg_advisory_xact_lock($1::bigint);
