-- 0010_series_resolver_key_hash.sql: Replace the unbounded resolver_key B-tree
-- index with a fixed-width SHA-256 digest unique index so arbitrarily long
-- resolver keys fit within PostgreSQL's B-tree index tuple limit while
-- keeping uniqueness DB-enforced across all transaction isolation levels.

DROP INDEX idx_series_library_resolver_key;

ALTER TABLE series
    ADD COLUMN resolver_key_hash BYTEA;

UPDATE series
SET resolver_key_hash = sha256(convert_to(resolver_key, 'UTF8'))
WHERE resolver_key IS NOT NULL;

ALTER TABLE series
    ADD CONSTRAINT series_resolver_key_hash_pair
        CHECK ((resolver_key IS NULL) = (resolver_key_hash IS NULL)),
    ADD CONSTRAINT series_resolver_key_hash_len
        CHECK (resolver_key_hash IS NULL OR octet_length(resolver_key_hash) = 32);

CREATE UNIQUE INDEX idx_series_library_resolver_key_hash
    ON series (library_id, resolver_key_hash)
    WHERE resolver_key_hash IS NOT NULL;
