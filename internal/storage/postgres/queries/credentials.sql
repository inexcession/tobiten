-- name: CreateCredential :exec
INSERT INTO credentials (
    id, user_id, kind, scopes, secret_hash, hash_scheme, external_subject, label, created_at, last_used_at, expires_at, revoked_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
);

-- name: GetCredentialByID :one
SELECT id, user_id, kind, scopes, secret_hash, hash_scheme, external_subject, label, created_at, last_used_at, expires_at, revoked_at
FROM credentials
WHERE id = $1;

-- name: GetActivePasswordCredentialByUserID :one
SELECT id, user_id, kind, scopes, secret_hash, hash_scheme, external_subject, label, created_at, last_used_at, expires_at, revoked_at
FROM credentials
WHERE user_id = $1 AND kind = 'PASSWORD' AND revoked_at IS NULL;

-- name: GetActiveOIDCCredentialBySubject :one
SELECT id, user_id, kind, scopes, secret_hash, hash_scheme, external_subject, label, created_at, last_used_at, expires_at, revoked_at
FROM credentials
WHERE external_subject = $1 AND kind = 'OIDC_LINK' AND revoked_at IS NULL;

-- name: ListCredentialsByUserID :many
SELECT id, user_id, kind, scopes, secret_hash, hash_scheme, external_subject, label, created_at, last_used_at, expires_at, revoked_at
FROM credentials
WHERE user_id = $1
ORDER BY created_at ASC;

-- name: RevokeCredential :execrows
UPDATE credentials
SET revoked_at = $2
WHERE id = $1;

-- name: DeleteCredential :execrows
DELETE FROM credentials
WHERE id = $1;

-- name: UpdateCredentialLastUsed :execrows
UPDATE credentials
SET last_used_at = $2
WHERE id = $1;
