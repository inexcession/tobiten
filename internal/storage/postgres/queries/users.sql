-- name: CreateUser :exec
INSERT INTO users (id, username, created_at, updated_at)
VALUES ($1, $2, $3, $4);

-- name: GetUserByID :one
SELECT id, username, created_at, updated_at
FROM users
WHERE id = $1;

-- name: GetUserByUsername :one
SELECT id, username, created_at, updated_at
FROM users
WHERE username = $1;

-- name: UpdateUser :execrows
UPDATE users
SET username = $2, updated_at = $3
WHERE id = $1;

-- name: DeleteUser :execrows
DELETE FROM users
WHERE id = $1;

-- name: CountUsers :one
SELECT COUNT(*) FROM users;

-- name: ListUsers :many
SELECT id, username, created_at, updated_at
FROM users
ORDER BY created_at ASC;
