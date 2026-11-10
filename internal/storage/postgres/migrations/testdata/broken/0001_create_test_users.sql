CREATE TABLE test_users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
