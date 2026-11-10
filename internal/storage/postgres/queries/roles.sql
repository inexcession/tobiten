-- name: ListRoles :many
SELECT id, name, built_in, requires_allow_labels
FROM roles
ORDER BY id ASC;

-- name: GetRoleByID :one
SELECT id, name, built_in, requires_allow_labels
FROM roles
WHERE id = $1;

-- name: GetRoleByName :one
SELECT id, name, built_in, requires_allow_labels
FROM roles
WHERE name = $1;

-- name: ListPermissionsForRole :many
SELECT permission
FROM role_permissions
WHERE role_id = $1
ORDER BY permission ASC;

-- name: ListAllRolePermissions :many
SELECT role_id, permission
FROM role_permissions
ORDER BY role_id, permission ASC;

-- name: AssignUserRole :exec
INSERT INTO user_roles (user_id, role_id)
VALUES ($1, $2)
ON CONFLICT (user_id, role_id) DO NOTHING;

-- name: RemoveUserRole :execrows
DELETE FROM user_roles
WHERE user_id = $1 AND role_id = $2;

-- name: ListUserRoles :many
SELECT
    r.id,
    r.name,
    r.built_in,
    r.requires_allow_labels,
    COALESCE(
        ARRAY_AGG(rp.permission) FILTER (WHERE rp.permission IS NOT NULL),
        '{}'::text[]
    )::text[] AS permissions
FROM user_roles ur
JOIN roles r ON r.id = ur.role_id
LEFT JOIN role_permissions rp ON rp.role_id = r.id
WHERE ur.user_id = $1
GROUP BY r.id, r.name, r.built_in, r.requires_allow_labels
ORDER BY r.name ASC;
