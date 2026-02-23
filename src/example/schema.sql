-- Example schema for zqlc demonstration.
-- Run this against your PostgreSQL database before running zqlc.
--
--   psql -f src/example/schema.sql

CREATE TABLE IF NOT EXISTS users (
    id         SERIAL PRIMARY KEY,
    name       TEXT        NOT NULL,
    email      TEXT        NOT NULL UNIQUE,
    bio        TEXT,
    is_active  BOOLEAN     NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS posts (
    id         SERIAL PRIMARY KEY,
    user_id    INT         NOT NULL REFERENCES users(id),
    title      TEXT        NOT NULL,
    body       TEXT        NOT NULL,
    published  BOOLEAN     NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS comments (
    id         SERIAL PRIMARY KEY,
    post_id    INT         NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    user_id    INT         NOT NULL REFERENCES users(id),
    body       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tags (
    id   SERIAL PRIMARY KEY,
    name TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS post_tags (
    post_id INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    tag_id  INT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (post_id, tag_id)
);

-- Seed data (optional)
INSERT INTO users (name, email, bio) VALUES
    ('Alice',   'alice@example.com',   'Zig enthusiast'),
    ('Bob',     'bob@example.com',     NULL),
    ('Charlie', 'charlie@example.com', 'Writes about databases')
ON CONFLICT (email) DO NOTHING;
