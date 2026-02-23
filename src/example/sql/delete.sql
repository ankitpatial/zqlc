-- name: DeletePost :exec
-- Delete a post by ID.
DELETE FROM posts
WHERE id = $1;

-- name: RemovePostTag :exec
-- Remove a tag from a post.
DELETE FROM post_tags
WHERE post_id = $1 AND tag_id = $2;
