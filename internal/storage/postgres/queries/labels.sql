-- name: CreateLabel :exec
INSERT INTO labels (id, scope, owner_id, name, built_in)
VALUES ($1, $2, $3, $4, $5);

-- name: UpsertBuiltInServerLabel :exec
INSERT INTO labels (id, scope, owner_id, name, built_in)
VALUES ($1, 'SERVER', NULL, $2, TRUE)
ON CONFLICT (name) WHERE scope = 'SERVER' DO UPDATE
SET built_in = TRUE;

-- name: EnsureUserBuiltInLabel :exec
INSERT INTO labels (id, scope, owner_id, name, built_in)
VALUES ($1, 'USER', $2, $3, TRUE)
ON CONFLICT (owner_id, name) WHERE scope = 'USER' DO UPDATE
SET built_in = TRUE;

-- name: GetLabelByID :one
SELECT id, scope, owner_id, name, built_in
FROM labels
WHERE id = $1;

-- name: GetServerLabelByName :one
SELECT id, scope, owner_id, name, built_in
FROM labels
WHERE scope = 'SERVER' AND name = $1;

-- name: GetUserLabelByName :one
SELECT id, scope, owner_id, name, built_in
FROM labels
WHERE scope = 'USER' AND owner_id = $1 AND name = $2;

-- name: ListServerLabels :many
SELECT id, scope, owner_id, name, built_in
FROM labels
WHERE scope = 'SERVER'
ORDER BY name, id;

-- name: ListUserLabels :many
SELECT id, scope, owner_id, name, built_in
FROM labels
WHERE scope = 'USER' AND owner_id = $1
ORDER BY name, id;

-- name: RenameLabel :one
UPDATE labels
SET name = $2
WHERE id = $1
RETURNING id, scope, owner_id, name, built_in;

-- name: DeleteLabel :execrows
DELETE FROM labels
WHERE id = $1;

-- name: AssignItemLabel :exec
INSERT INTO item_labels (label_id, item_type, item_id)
VALUES ($1, $2, $3)
ON CONFLICT (label_id, item_type, item_id) DO NOTHING;

-- name: RemoveItemLabel :execrows
DELETE FROM item_labels
WHERE label_id = $1 AND item_type = $2 AND item_id = $3;

-- name: ListItemLabels :many
SELECT l.id, l.scope, l.owner_id, l.name, l.built_in
FROM item_labels il
JOIN labels l ON l.id = il.label_id
WHERE il.item_type = $1 AND il.item_id = $2
ORDER BY l.scope, l.name, l.id;

-- name: ListItemIDsByLabel :many
SELECT item_id
FROM item_labels
WHERE label_id = $1 AND item_type = $2
ORDER BY item_id;

-- name: RemoveAllItemLabelsForItem :exec
DELETE FROM item_labels
WHERE item_type = $1 AND item_id = $2;

-- name: SetUserLabelPolicy :exec
INSERT INTO user_label_policies (user_id, label_id, effect)
VALUES ($1, $2, $3)
ON CONFLICT (user_id, label_id) DO UPDATE
SET effect = EXCLUDED.effect;

-- name: RemoveUserLabelPolicy :execrows
DELETE FROM user_label_policies
WHERE user_id = $1 AND label_id = $2;

-- name: ListUserLabelPolicies :many
SELECT user_id, label_id, effect
FROM user_label_policies
WHERE user_id = $1
ORDER BY label_id;

-- name: ListLabelPoliciesByLabel :many
SELECT user_id, label_id, effect
FROM user_label_policies
WHERE label_id = $1
ORDER BY user_id;
