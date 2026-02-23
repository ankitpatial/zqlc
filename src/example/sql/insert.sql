-- name: CreateUser :one
-- Insert a new user and return the created row.
INSERT INTO users (name, email, bio)
VALUES ($1, $2, $3)
RETURNING id, name, email, bio, is_active, created_at;

-- name: CreatePost :one
-- Create a new post for a user.
INSERT INTO posts (user_id, title, body)
VALUES ($1, $2, $3)
RETURNING id, user_id, title, body, published, created_at;

-- name: AddComment :one
-- Add a comment to a post.
INSERT INTO comments (post_id, user_id, body)
VALUES ($1, $2, $3)
RETURNING id, post_id, user_id, body, created_at;

-- name: TagPost :exec
-- Associate a tag with a post.
INSERT INTO post_tags (post_id, tag_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;
