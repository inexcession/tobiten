-- 0009_series_resolver_key.sql: Add nullable library-scoped resolver_key and bounded
-- SHA-256 index key on series for M1B-D5 matching while preserving curated and
-- pre-upgrade series.
-- Forward-only additive migration (ADR-0001 §6, ADR-0018 §2, ADR-0019 §1).

ALTER TABLE series
    ADD COLUMN resolver_key TEXT,
    ADD COLUMN resolver_key_hash BYTEA,
    ADD CONSTRAINT series_resolver_key_nonempty CHECK (resolver_key IS NULL OR resolver_key <> ''),
    ADD CONSTRAINT series_resolver_key_hash_pair CHECK ((resolver_key IS NULL) = (resolver_key_hash IS NULL));

-- Bounded B-tree index over (library_id, 32-byte SHA-256) avoids PostgreSQL's
-- 2704-byte B-tree entry limit on long, deep, or hex-escaped resolver keys while
-- exact uniqueness and exact-key lookup remain governed by (library_id, resolver_key).
CREATE INDEX idx_series_library_resolver_key_hash
    ON series (library_id, resolver_key_hash)
    WHERE resolver_key_hash IS NOT NULL;

CREATE OR REPLACE FUNCTION trg_series_resolver_key_guard()
RETURNS TRIGGER AS $$
DECLARE
    v_hash    BYTEA;
    v_lock_hi INT := 1414746699; -- 'TSRK' namespace for series resolver keys
    v_lock_lo INT;
BEGIN
    IF NEW.resolver_key IS NULL THEN
        NEW.resolver_key_hash := NULL;
        RETURN NEW;
    END IF;

    v_hash := sha256(convert_to(NEW.resolver_key, 'UTF8'));
    NEW.resolver_key_hash := v_hash;

    IF TG_OP = 'INSERT'
       OR OLD.library_id IS DISTINCT FROM NEW.library_id
       OR OLD.resolver_key IS DISTINCT FROM NEW.resolver_key THEN
        v_lock_lo := hashtext(NEW.library_id || ':' || encode(v_hash, 'hex'));
        PERFORM pg_advisory_xact_lock(v_lock_hi, v_lock_lo);

        IF EXISTS (
            SELECT 1
            FROM series s
            WHERE s.library_id = NEW.library_id
              AND s.resolver_key_hash = v_hash
              AND s.resolver_key = NEW.resolver_key
              AND s.id <> NEW.id
        ) THEN
            RAISE EXCEPTION 'duplicate series resolver_key in library %', NEW.library_id
                USING ERRCODE = '23505',
                      CONSTRAINT = 'idx_series_library_resolver_key';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_series_resolver_key_guard
    BEFORE INSERT OR UPDATE OF library_id, resolver_key ON series
    FOR EACH ROW
    EXECUTE FUNCTION trg_series_resolver_key_guard();

CREATE INDEX idx_series_memberships_series_primary
    ON series_memberships (series_id)
    WHERE is_primary = TRUE;
