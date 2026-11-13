-- 0002_sessions.sql: Native and adapter session storage bound to originating credentials.
-- Forward-only additive migration (ADR-0018 §2, ADR-0006).

CREATE TABLE IF NOT EXISTS sessions (
    id                      TEXT        PRIMARY KEY,
    user_id                 TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    credential_id           TEXT        NOT NULL REFERENCES credentials(id) ON DELETE CASCADE,
    kind                    TEXT        NOT NULL CHECK (kind IN ('NATIVE', 'KOMGA', 'KOMGA_REMEMBER_ME')),
    token_hash              TEXT        NOT NULL UNIQUE,
    csrf_token_hash         TEXT        NOT NULL,
    credential_hash_digest  TEXT        NOT NULL DEFAULT '',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at            TIMESTAMPTZ,
    expires_at              TIMESTAMPTZ NOT NULL,
    revoked_at              TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sessions_credential_id ON sessions (credential_id);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions (expires_at);
