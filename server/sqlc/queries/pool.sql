-- name: GetPool :one
SELECT *
FROM pool
WHERE org_id = $1
  AND id = $2
  AND deleted_at IS NULL;

-- name: ListPools :many
SELECT *
FROM pool
WHERE org_id = $1
  AND deleted_at IS NULL
ORDER BY created_at DESC;

-- name: CreatePool :one
INSERT INTO pool (org_id, pool_name, url, username, password_enc, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $6)
RETURNING id;

-- name: UpdatePool :exec
UPDATE pool
SET pool_name     = $1,
    url           = $2,
    username      = $3,
    password_enc = $4,
    updated_at    = $5
WHERE org_id = $6
  AND id = $7;

-- name: SoftDeletePool :exec
UPDATE pool
SET deleted_at = CURRENT_TIMESTAMP
WHERE org_id = $1
  AND id = $2;

-- name: DeletePool :exec
DELETE
FROM pool
WHERE id = $1;

-- name: GetTotalPools :one
SELECT COUNT(*)
FROM pool
WHERE org_id = $1
  AND deleted_at IS NULL;

-- name: GetOrCreateSV2TranslatorRoute :one
INSERT INTO sv2_translator_route (
    org_id,
    upstream_url,
    username,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (org_id, upstream_url, username)
DO UPDATE SET updated_at = sv2_translator_route.updated_at
RETURNING *;

-- name: GetSV2TranslatorRouteByPort :one
SELECT *
FROM sv2_translator_route
WHERE org_id = $1
  AND listen_port = $2;
