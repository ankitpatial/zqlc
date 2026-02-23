-- name: FindUserById :one
-- Find a single user by their primary key.
SELECT id, name, email, bio, is_active, created_at
FROM users
WHERE id = $1;

-- name: ListUsers :many
-- List all users ordered by creation date (newest first).
SELECT id, name, email, bio, is_active, created_at
FROM users
ORDER BY created_at DESC;

-- name: GetPostWithAuthor :one
-- Get a post along with its author name.
SELECT p.id, p.title, p.body, p.published, p.created_at,
       u.name AS author_name
FROM posts p
JOIN users u ON u.id = p.user_id
WHERE p.id = $1;

-- name: ListPostComments :many
-- List comments on a post with commenter names.
SELECT c.id, c.body, c.created_at,
       u.name AS commenter_name
FROM comments c
JOIN users u ON u.id = c.user_id
WHERE c.post_id = $1
ORDER BY c.created_at;

-- name: CountPostsByUser :many
-- Count posts per user using GROUP BY.
SELECT u.id AS user_id, u.name,
       COUNT(p.id)::int AS post_count
FROM users u
LEFT JOIN posts p ON p.user_id = u.id
GROUP BY u.id, u.name
ORDER BY post_count DESC;

-- name: ListPostsByTag :many
-- List posts that have a given tag (many-to-many through post_tags).
SELECT p.id, p.title, p.created_at,
       u.name AS author_name
FROM posts p
JOIN post_tags pt ON pt.post_id = p.id
JOIN tags t ON t.id = pt.tag_id
JOIN users u ON u.id = p.user_id
WHERE t.name = $1
ORDER BY p.created_at DESC;
