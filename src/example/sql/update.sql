-- name: UpdateUserEmail :one
-- Update a user's email address and return the updated row.
UPDATE users
SET email = $2
WHERE id = $1
RETURNING id, name, email;

-- name: PublishPost :exec
-- Mark a post as published.
UPDATE posts
SET published = true
WHERE id = $1;

-- name: UpdateUser :one
-- Update a user's profile fields.
UPDATE users
SET name = $2, email = $3, bio = $4, is_active = $5
WHERE id = $1
RETURNING id, name, email, bio, is_active, created_at;

-- name: DeactivateUser :execrows
-- Deactivate a user account. Returns affected row count.
UPDATE users
SET is_active = false
WHERE id = $1;
