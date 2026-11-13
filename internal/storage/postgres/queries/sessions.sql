-- name: GetCredentialForSessionLock :one
SELECT id, user_id, kind, secret_hash, expires_at, revoked_at
FROM credentials
WHERE id = $1
FOR SHARE;

-- name: CreateSession :one
INSERT INTO sessions (
    id,
    user_id,
    credential_id,
    kind,
    token_hash,
    csrf_token_hash,
    credential_hash_digest,
    created_at,
    last_used_at,
    expires_at,
    revoked_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING id, user_id, credential_id, kind, token_hash, csrf_token_hash, credential_hash_digest, created_at, last_used_at, expires_at, revoked_at;

-- name: GetSessionByTokenHash :one
SELECT id, user_id, credential_id, kind, token_hash, csrf_token_hash, credential_hash_digest, created_at, last_used_at, expires_at, revoked_at
FROM sessions
WHERE token_hash = $1
LIMIT 1;

-- name: UpdateSessionLastUsed :execrows
UPDATE sessions
SET last_used_at = $2
WHERE id = $1;

-- name: RevokeSession :execrows
UPDATE sessions
SET revoked_at = $2
WHERE id = $1 AND revoked_at IS NULL;

-- name: RevokeSessionsByCredentialID :execrows
UPDATE sessions
SET revoked_at = $2
WHERE credential_id = $1 AND revoked_at IS NULL;

-- name: DeleteSession :execrows
DELETE FROM sessions
WHERE id = $1;

-- name: DeleteSessionsByCredentialID :execrows
DELETE FROM sessions
WHERE credential_id = $1;

-- name: DeleteExpiredSessions :execrows
DELETE FROM sessions
WHERE expires_at <= $1;
