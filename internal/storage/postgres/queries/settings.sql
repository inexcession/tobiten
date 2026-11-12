-- name: GetSystemSetting :one
SELECT key, value, is_secret, updated_at, updated_by
FROM system_settings
WHERE key = $1;

-- name: ListSystemSettings :many
SELECT key, value, is_secret, updated_at, updated_by
FROM system_settings
ORDER BY key;

-- name: UpsertSystemSetting :one
INSERT INTO system_settings (key, value, is_secret, updated_at, updated_by)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value,
    is_secret = EXCLUDED.is_secret,
    updated_at = EXCLUDED.updated_at,
    updated_by = EXCLUDED.updated_by
RETURNING key, value, is_secret, updated_at, updated_by;

-- name: DeleteSystemSetting :exec
DELETE FROM system_settings
WHERE key = $1;
