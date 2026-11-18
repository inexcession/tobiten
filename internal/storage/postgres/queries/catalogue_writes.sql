-- name: CreateSeries :one
INSERT INTO series (
    id,
    library_id,
    title,
    title_sort,
    language,
    original_title,
    romaji_title,
    franchise,
    edition_name,
    publisher,
    imprint,
    status,
    summary,
    age_rating,
    genres,
    tags,
    cover_policy,
    cover_volume_id,
    cover_image_digest,
    locked_fields,
    external_ids,
    total_volumes,
    release_year,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
    $17, $18, $19, $20, $21, $22, $23, $24, $25
)
RETURNING id, library_id, title, title_sort, language, original_title, romaji_title,
          franchise, edition_name, publisher, imprint, status, summary, age_rating,
          genres, tags, cover_policy, cover_volume_id, cover_image_digest,
          locked_fields, external_ids, total_volumes, release_year, created_at, updated_at;

-- name: GetSeriesByIDForWrite :one
SELECT id, library_id, title, title_sort, language, original_title, romaji_title,
       franchise, edition_name, publisher, imprint, status, summary, age_rating,
       genres, tags, cover_policy, cover_volume_id, cover_image_digest,
       locked_fields, external_ids, total_volumes, release_year, created_at, updated_at
FROM series
WHERE id = $1;

-- name: UpdateSeries :one
UPDATE series
SET library_id = $2,
    title = $3,
    title_sort = $4,
    language = $5,
    original_title = $6,
    romaji_title = $7,
    franchise = $8,
    edition_name = $9,
    publisher = $10,
    imprint = $11,
    status = $12,
    summary = $13,
    age_rating = $14,
    genres = $15,
    tags = $16,
    cover_policy = $17,
    cover_volume_id = $18,
    cover_image_digest = $19,
    locked_fields = $20,
    external_ids = $21,
    total_volumes = $22,
    release_year = $23,
    updated_at = $24
WHERE id = $1
RETURNING id, library_id, title, title_sort, language, original_title, romaji_title,
          franchise, edition_name, publisher, imprint, status, summary, age_rating,
          genres, tags, cover_policy, cover_volume_id, cover_image_digest,
          locked_fields, external_ids, total_volumes, release_year, created_at, updated_at;

-- name: CreateVolume :one
INSERT INTO volumes (
    id,
    library_id,
    kind,
    number,
    chapter_start,
    chapter_end,
    chapter_range_source,
    title,
    summary,
    release_date,
    page_count,
    preferred_asset_id,
    external_ids,
    locked_fields,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
)
RETURNING id, library_id, kind, number, chapter_start, chapter_end, chapter_range_source,
          title, summary, release_date, page_count, preferred_asset_id, external_ids,
          locked_fields, created_at, updated_at;

-- name: GetVolumeByIDForWrite :one
SELECT id, library_id, kind, number, chapter_start, chapter_end, chapter_range_source,
       title, summary, release_date, page_count, preferred_asset_id, external_ids,
       locked_fields, created_at, updated_at
FROM volumes
WHERE id = $1;

-- name: UpdateVolume :one
UPDATE volumes
SET library_id = $2,
    kind = $3,
    number = $4,
    chapter_start = $5,
    chapter_end = $6,
    chapter_range_source = $7,
    title = $8,
    summary = $9,
    release_date = $10,
    page_count = $11,
    preferred_asset_id = $12,
    external_ids = $13,
    locked_fields = $14,
    updated_at = $15
WHERE id = $1
RETURNING id, library_id, kind, number, chapter_start, chapter_end, chapter_range_source,
          title, summary, release_date, page_count, preferred_asset_id, external_ids,
          locked_fields, created_at, updated_at;

-- name: CreateMediaAsset :one
INSERT INTO media_assets (
    id,
    volume_id,
    library_root_id,
    rel_path_bytes,
    rel_path_display,
    rel_path_match,
    file_size,
    file_mtime,
    format,
    page_count,
    cover_page_index,
    suppressed_pages,
    diagnostics,
    missing_since,
    retired_at,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
)
RETURNING id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match,
          file_size, file_mtime, format, page_count, cover_page_index, suppressed_pages,
          diagnostics, missing_since, retired_at, created_at, updated_at;

-- name: GetMediaAssetByIDForWrite :one
SELECT id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match,
       file_size, file_mtime, format, page_count, cover_page_index, suppressed_pages,
       diagnostics, missing_since, retired_at, created_at, updated_at
FROM media_assets
WHERE id = $1;

-- name: LockMediaAssetForUpdate :one
SELECT id
FROM media_assets
WHERE id = $1
FOR UPDATE;

-- name: UpdateMediaAsset :one
UPDATE media_assets
SET volume_id = $2,
    library_root_id = $3,
    rel_path_bytes = $4,
    rel_path_display = $5,
    rel_path_match = $6,
    file_size = $7,
    file_mtime = $8,
    format = $9,
    page_count = $10,
    cover_page_index = $11,
    suppressed_pages = $12,
    diagnostics = $13,
    missing_since = $14,
    retired_at = $15,
    updated_at = $16
WHERE id = $1
RETURNING id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match,
          file_size, file_mtime, format, page_count, cover_page_index, suppressed_pages,
          diagnostics, missing_since, retired_at, created_at, updated_at;

-- name: UpdateMediaAssetLocation :one
UPDATE media_assets
SET library_root_id = $2,
    rel_path_bytes = $3,
    rel_path_display = $4,
    rel_path_match = $5,
    diagnostics = CASE
        WHEN sqlc.arg(invalid_utf8_path)::boolean THEN jsonb_set(diagnostics, '{invalid_utf8_path}', 'true'::jsonb, true)
        ELSE diagnostics - 'invalid_utf8_path'
    END,
    updated_at = $6
WHERE id = $1
RETURNING id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match,
          file_size, file_mtime, format, page_count, cover_page_index, suppressed_pages,
          diagnostics, missing_since, retired_at, created_at, updated_at;

-- name: ListMediaAssetsByRoot :many
SELECT id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match,
       file_size, file_mtime, format, page_count, cover_page_index, suppressed_pages,
       diagnostics, missing_since, retired_at, created_at, updated_at
FROM media_assets
WHERE library_root_id = $1
ORDER BY rel_path_bytes ASC, id ASC;

-- name: InsertAssetDigest :one
INSERT INTO asset_digests (
    asset_id,
    kind,
    value,
    is_current,
    reason,
    recorded_at
)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING asset_id, kind, value, is_current, reason, recorded_at;

-- name: DemoteCurrentAssetDigests :exec
UPDATE asset_digests
SET is_current = FALSE
WHERE asset_id = $1
  AND kind = $2
  AND is_current = TRUE;

-- name: UpsertCurrentAssetDigest :one
INSERT INTO asset_digests (
    asset_id,
    kind,
    value,
    is_current,
    reason,
    recorded_at
)
VALUES ($1, $2, $3, TRUE, $4, $5)
ON CONFLICT (asset_id, kind, value) DO UPDATE
SET is_current = TRUE,
    reason = EXCLUDED.reason,
    recorded_at = EXCLUDED.recorded_at
RETURNING asset_id, kind, value, is_current, reason, recorded_at;

-- name: ListCurrentAssetDigestsByAssetIDs :many
SELECT asset_id, kind, value, is_current, reason, recorded_at
FROM asset_digests
WHERE asset_id = ANY($1::text[])
  AND is_current = TRUE
ORDER BY asset_id ASC, kind ASC;

-- name: ListAssetDigestsByAssetIDs :many
SELECT asset_id, kind, value, is_current, reason, recorded_at
FROM asset_digests
WHERE asset_id = ANY($1::text[])
ORDER BY asset_id ASC, kind ASC, is_current DESC, recorded_at DESC, value ASC;

-- name: FindMediaAssetsByDigestInLibrary :many
SELECT a.id, a.volume_id, a.library_root_id, a.rel_path_bytes, a.rel_path_display, a.rel_path_match,
       a.file_size, a.file_mtime, a.format, a.page_count, a.cover_page_index, a.suppressed_pages,
       a.diagnostics, a.missing_since, a.retired_at, a.created_at, a.updated_at
FROM media_assets a
JOIN library_roots r ON r.id = a.library_root_id
WHERE r.library_id = $1
  AND EXISTS (
      SELECT 1
      FROM asset_digests d
      WHERE d.asset_id = a.id
        AND d.kind = $2
        AND d.value = $3
  )
ORDER BY a.created_at ASC, a.id ASC;
