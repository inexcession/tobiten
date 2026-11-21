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
    updated_at,
    resolver_key
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
    $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
)
RETURNING id, library_id, title, title_sort, language, original_title, romaji_title,
          franchise, edition_name, publisher, imprint, status, summary, age_rating,
          genres, tags, cover_policy, cover_volume_id, cover_image_digest,
          locked_fields, external_ids, total_volumes, release_year, created_at, updated_at,
          resolver_key, resolver_key_hash;

-- name: AcquireSeriesResolverKeyLock :exec
SELECT pg_advisory_xact_lock(
    1414746699,
    hashtext(
        sqlc.arg(library_id)::text || ':' ||
        encode(sha256(convert_to(sqlc.arg(resolver_key)::text, 'UTF8')), 'hex')
    )
);

-- name: GetSeriesByIDForWrite :one
SELECT id, library_id, title, title_sort, language, original_title, romaji_title,
       franchise, edition_name, publisher, imprint, status, summary, age_rating,
       genres, tags, cover_policy, cover_volume_id, cover_image_digest,
       locked_fields, external_ids, total_volumes, release_year, created_at, updated_at,
       resolver_key, resolver_key_hash
FROM series
WHERE id = $1;

-- name: GetSeriesByIDForWriteForUpdate :one
SELECT id, library_id, title, title_sort, language, original_title, romaji_title,
       franchise, edition_name, publisher, imprint, status, summary, age_rating,
       genres, tags, cover_policy, cover_volume_id, cover_image_digest,
       locked_fields, external_ids, total_volumes, release_year, created_at, updated_at,
       resolver_key, resolver_key_hash
FROM series
WHERE id = $1
FOR UPDATE;

-- name: GetSeriesByResolverKey :one
SELECT id, library_id, title, title_sort, language, original_title, romaji_title,
       franchise, edition_name, publisher, imprint, status, summary, age_rating,
       genres, tags, cover_policy, cover_volume_id, cover_image_digest,
       locked_fields, external_ids, total_volumes, release_year, created_at, updated_at,
       resolver_key, resolver_key_hash
FROM series
WHERE library_id = $1
  AND resolver_key_hash = sha256(convert_to(sqlc.arg(resolver_key)::text, 'UTF8'))
  AND resolver_key = sqlc.arg(resolver_key)::text;

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
    updated_at = $24,
    resolver_key = $25
WHERE id = $1
RETURNING id, library_id, title, title_sort, language, original_title, romaji_title,
          franchise, edition_name, publisher, imprint, status, summary, age_rating,
          genres, tags, cover_policy, cover_volume_id, cover_image_digest,
          locked_fields, external_ids, total_volumes, release_year, created_at, updated_at,
          resolver_key, resolver_key_hash;

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

-- name: GetVolumeByIDForWriteForUpdate :one
SELECT id, library_id, kind, number, chapter_start, chapter_end, chapter_range_source,
       title, summary, release_date, page_count, preferred_asset_id, external_ids,
       locked_fields, created_at, updated_at
FROM volumes
WHERE id = $1
FOR UPDATE;

-- name: FindVolumesByPrimarySeriesKindNumberForUpdate :many
SELECT v.id
FROM volumes v
JOIN series_memberships sm ON sm.volume_id = v.id
WHERE sm.series_id = $1
  AND sm.is_primary = TRUE
  AND v.library_id = $2
  AND v.kind = $3
  AND v.number = $4
ORDER BY v.created_at ASC, v.id ASC
FOR UPDATE OF v;

-- name: GetVerifiedVolumesByPrimarySeriesKindNumberForUpdate :many
SELECT v.id, v.library_id, v.kind, v.number, v.chapter_start, v.chapter_end,
       v.chapter_range_source, v.title, v.summary, v.release_date, v.page_count,
       v.preferred_asset_id, v.external_ids, v.locked_fields, v.created_at, v.updated_at
FROM volumes v
JOIN series_memberships sm ON sm.volume_id = v.id
WHERE v.id = ANY(sqlc.arg(volume_ids)::text[])
  AND sm.series_id = $1
  AND sm.is_primary = TRUE
  AND v.library_id = $2
  AND v.kind = $3
  AND v.number = $4
ORDER BY v.created_at ASC, v.id ASC
FOR UPDATE OF v, sm;

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

-- name: GetMediaAssetWithDigestsByIDForWrite :many
SELECT sqlc.embed(a),
       d.kind AS digest_kind,
       d.value AS digest_value,
       d.is_current AS digest_is_current,
       d.reason AS digest_reason,
       d.recorded_at AS digest_recorded_at
FROM media_assets a
LEFT JOIN asset_digests d ON d.asset_id = a.id
WHERE a.id = $1
ORDER BY d.kind ASC, d.is_current DESC, d.recorded_at DESC, d.value ASC;

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

-- name: ListMediaAssetsWithCurrentDigestsByRoot :many
SELECT sqlc.embed(a),
       d.kind AS digest_kind,
       d.value AS digest_value,
       d.is_current AS digest_is_current,
       d.reason AS digest_reason,
       d.recorded_at AS digest_recorded_at
FROM media_assets a
LEFT JOIN asset_digests d ON d.asset_id = a.id AND d.is_current = TRUE
WHERE a.library_root_id = $1
ORDER BY a.rel_path_bytes ASC, a.id ASC, d.kind ASC;

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

-- name: FindMediaAssetsWithDigestsByDigestInLibrary :many
SELECT sqlc.embed(a),
       d.kind AS digest_kind,
       d.value AS digest_value,
       d.is_current AS digest_is_current,
       d.reason AS digest_reason,
       d.recorded_at AS digest_recorded_at
FROM media_assets a
JOIN library_roots r ON r.id = a.library_root_id
LEFT JOIN asset_digests d ON d.asset_id = a.id
WHERE r.library_id = $1
  AND EXISTS (
      SELECT 1
      FROM asset_digests match_d
      WHERE match_d.asset_id = a.id
        AND match_d.kind = $2
        AND match_d.value = $3
  )
ORDER BY a.created_at ASC, a.id ASC, d.kind ASC, d.is_current DESC, d.recorded_at DESC, d.value ASC;
