-- Modified content changes the checksum
CREATE TABLE test_users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    phone TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
