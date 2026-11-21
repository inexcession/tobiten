-- 0009_series_resolver_key.sql: Add nullable library-scoped resolver_key on series
-- for M1B-D5 series matching while preserving curated and pre-upgrade series.
-- Forward-only additive migration (ADR-0001 §6, ADR-0018 §2, ADR-0019 §1).

ALTER TABLE series
    ADD COLUMN resolver_key TEXT,
    ADD CONSTRAINT series_resolver_key_nonempty CHECK (resolver_key IS NULL OR resolver_key <> '');

CREATE UNIQUE INDEX idx_series_library_resolver_key
    ON series (library_id, resolver_key)
    WHERE resolver_key IS NOT NULL;

CREATE INDEX idx_series_memberships_series_primary
    ON series_memberships (series_id)
    WHERE is_primary = TRUE;
