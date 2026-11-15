-- 0004_catalogue.sql: M1B Library Catalogue tables and cross-table integrity triggers.
-- Forward-only additive migration (ADR-0001 §6, ADR-0018 §2).
-- Appended in M1A (M1-T4.4) so M1-T9.2 can verify repository label predicates against real catalogue tables.

-- 2c. Libraries & roots (ADR-0019). A library has one resolver chain and one or more roots.
--     Overlapping roots/intakes within or across libraries and canonical aliases are rejected.
CREATE TABLE libraries (
    id                      TEXT PRIMARY KEY,
    name                    TEXT UNIQUE NOT NULL,
    mode                    TEXT NOT NULL DEFAULT 'VIRTUAL' CHECK (mode IN ('VIRTUAL', 'MANAGED')),
    intake_path_bytes       BYTEA,                         -- configured managed-library dropzone, no fixed /intake path
    intake_path_display     TEXT,
    intake_path_match       TEXT,
    auto_metadata_writeback BOOLEAN NOT NULL DEFAULT FALSE, -- managed scraper write-back opt-in
    resolver_chain          TEXT[] NOT NULL DEFAULT '{metadata,folder,filename}',
    metadata_precedence     TEXT NOT NULL DEFAULT 'EMBEDDED_OVER_ONLINE', -- or 'ONLINE_OVER_EMBEDDED' (ADR-0007); locked edits always win
    default_cover_policy    TEXT NOT NULL DEFAULT 'FIRST',              -- 'FIRST', 'LATEST', 'FIRST_UNREAD', 'MANUAL' (ADR-0010)
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((mode = 'MANAGED') = (intake_path_bytes IS NOT NULL)),
    CHECK ((intake_path_bytes IS NULL) = (intake_path_display IS NULL)),
    CHECK ((intake_path_bytes IS NULL) = (intake_path_match IS NULL))
);

CREATE TABLE library_roots (
    id              TEXT PRIMARY KEY,
    library_id      TEXT NOT NULL REFERENCES libraries(id) ON DELETE CASCADE,
    path_bytes      BYTEA UNIQUE NOT NULL,           -- raw OS bytes (ADR-0003)
    path_display    TEXT NOT NULL,
    path_match      TEXT NOT NULL,                   -- NFC match key; never used as filesystem identity
    state           TEXT NOT NULL DEFAULT 'ONLINE',  -- 'ONLINE', 'OFFLINE' (missing / unreadable / empty without .tobiten-root marker)
    last_checked_at TIMESTAMPTZ,
    last_error      TEXT
);

-- 3. Series & Multi-Edition Franchise Grouping (ADR-0007, ADR-0008)
CREATE TABLE series (
    id                 TEXT PRIMARY KEY,
    library_id         TEXT NOT NULL REFERENCES libraries(id) ON DELETE CASCADE,
    title              TEXT NOT NULL,
    title_sort         TEXT NOT NULL,                               -- generated (natural numeric, per-language article stripping,
                                                                    -- Japanese reading: kana › romaji › original) unless locked
    language           TEXT,                                        -- drives article stripping for title_sort
    original_title     TEXT,
    romaji_title       TEXT,
    franchise          TEXT,                                        -- Links Tankōbon, Kanzenban, Omnibus editions
    edition_name       TEXT,                                        -- e.g. 'Tankōbon', 'Kanzenban', 'Omnibus'
    publisher          TEXT,
    imprint            TEXT,
    status             TEXT NOT NULL DEFAULT 'ONGOING',
    summary            TEXT,
    age_rating         TEXT,
    genres             TEXT[] NOT NULL DEFAULT '{}',
    tags               TEXT[] NOT NULL DEFAULT '{}',
    cover_policy       TEXT,                                        -- NULL = library default; 'FIRST' / 'LATEST' / 'FIRST_UNREAD' / 'MANUAL'
    cover_volume_id    TEXT,                                        -- MANUAL: chosen volume; FK to volumes added after volumes (ON DELETE SET NULL)
    cover_image_digest TEXT,                                        -- MANUAL: uploaded artwork, IMAGE_XXH3_128_FULL key in the covers dir (ADR-0015)
    locked_fields      JSONB NOT NULL DEFAULT '{}',                 -- Plex-style field locks: {"title": true, "summary": true}
    external_ids       JSONB NOT NULL DEFAULT '{}',                 -- {"anilist": 30013, "comicvine": 4050, "isbn": "..."}
    total_volumes      INT,
    release_year       INT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (NOT (cover_volume_id IS NOT NULL AND cover_image_digest IS NOT NULL))  -- a manual cover is one or the other
);

CREATE INDEX idx_series_title_trgm ON series USING GIN (title gin_trgm_ops);
CREATE INDEX idx_series_romaji_trgm ON series USING GIN (romaji_title gin_trgm_ops);
CREATE INDEX idx_series_external_ids ON series USING GIN (external_ids jsonb_path_ops);

-- 4. Volumes / Chapters / Issues — bibliographic units (ADR-0008, ADR-0018 S5).
--    A volume may have zero manifestations (media_assets); owned/readable/missing are derived.
--    All primary/secondary memberships live in series_memberships. No membership = standalone.
CREATE TABLE volumes (
    id                   TEXT PRIMARY KEY,
    library_id           TEXT NOT NULL REFERENCES libraries(id) ON DELETE CASCADE,
    kind                 TEXT NOT NULL DEFAULT 'VOLUME',  -- 'VOLUME', 'CHAPTER', 'ISSUE', 'ANNUAL', 'SPECIAL', 'OMNIBUS'
    number               DOUBLE PRECISION,                -- intrinsic volume/chapter number, nullable for unnumbered books;
                                                          -- primary membership.position owns series-specific numbering
    chapter_start        DOUBLE PRECISION,                -- Smart Stream: chapter coverage start (e.g. 291), NULL if unknown
    chapter_end          DOUBLE PRECISION,                -- Smart Stream: chapter coverage end (e.g. 300), NULL if unknown
    chapter_range_source TEXT,                            -- 'MANUAL' (locked), 'ARCHIVE', 'SCRAPER'
    title                TEXT,
    summary              TEXT,
    release_date         DATE,
    page_count           INT NOT NULL DEFAULT 0,           -- descriptive only; adapter count comes from served asset
    preferred_asset_id   TEXT,                             -- explicit manual preference, FK added after media_assets
    external_ids         JSONB NOT NULL DEFAULT '{}',      -- ADR-0007 §4: per-volume ids, e.g. {"isbn13": "9784088...", "comicvine_issue": 123}
    locked_fields        JSONB NOT NULL DEFAULT '{}',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((chapter_start IS NULL) = (chapter_end IS NULL)),            -- a range is known in full or not at all
    CHECK ((chapter_start IS NULL) = (chapter_range_source IS NULL)),
    CHECK (chapter_start IS NULL OR chapter_start <= chapter_end)
);

CREATE INDEX idx_volumes_external_ids ON volumes USING GIN (external_ids jsonb_path_ops);

ALTER TABLE series ADD CONSTRAINT fk_series_cover_volume
    FOREIGN KEY (cover_volume_id) REFERENCES volumes(id) ON DELETE SET NULL
    DEFERRABLE INITIALLY DEFERRED;

-- 4b. All series memberships (ADR-0008; Round 3): e.g. Discworld #8 (primary) + City Watch #2.
--     Every membership has its own position; at most one is primary. A series member must have
--     exactly one primary (enforced by idx_memberships_one_primary + deferred constraint trigger);
--     zero rows means standalone. Single-series clients see the primary.
CREATE TABLE series_memberships (
    volume_id     TEXT NOT NULL REFERENCES volumes(id) ON DELETE CASCADE,
    series_id     TEXT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    is_primary    BOOLEAN NOT NULL DEFAULT FALSE,
    kind          TEXT NOT NULL DEFAULT 'SERIES',       -- 'SERIES', 'SUB_SERIES', 'UNIVERSE', 'OTHER'
    position      DOUBLE PRECISION,                     -- numbering/position within this series
    source        TEXT NOT NULL DEFAULT 'RESOLVER',     -- 'RESOLVER' (chain result), 'METADATA' (EPUB/OPF collection), 'MANUAL'
    locked_fields JSONB NOT NULL DEFAULT '{}',          -- manual move/merge/split corrections survive rescans (ADR-0019 §1)
    PRIMARY KEY (volume_id, series_id)
);

CREATE UNIQUE INDEX idx_memberships_one_primary ON series_memberships (volume_id) WHERE is_primary;

-- 4c. People, credits & entities (ADR-0008); all field-lockable via locked_fields (ADR-0007).
CREATE TABLE people (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,                   -- canonical display name
    name_native    TEXT,                            -- native script, e.g. 尾田栄一郎
    name_romanized TEXT,
    aliases        TEXT[] NOT NULL DEFAULT '{}',
    external_ids   JSONB NOT NULL DEFAULT '{}',
    locked_fields  JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE credits (
    id            TEXT PRIMARY KEY,
    person_id     TEXT NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    series_id     TEXT REFERENCES series(id) ON DELETE CASCADE,
    volume_id     TEXT REFERENCES volumes(id) ON DELETE CASCADE,
    role          TEXT NOT NULL,                        -- 'WRITER', 'ARTIST', 'PENCILLER', 'INKER', 'COLORIST', 'LETTERER',
                                                        -- 'COVER_ARTIST', 'EDITOR', 'TRANSLATOR', 'AUTHOR'
    position      INT NOT NULL DEFAULT 0,
    locked_fields JSONB NOT NULL DEFAULT '{}',          -- lock credit role/membership against automatic replacement
    CHECK ((series_id IS NULL) <> (volume_id IS NULL))
);

CREATE UNIQUE INDEX idx_credits_series ON credits (series_id, person_id, role) WHERE series_id IS NOT NULL;
CREATE UNIQUE INDEX idx_credits_volume ON credits (volume_id, person_id, role) WHERE volume_id IS NOT NULL;

CREATE TABLE entities (
    id           TEXT PRIMARY KEY,
    kind         TEXT NOT NULL,                      -- 'CHARACTER', 'TEAM', 'LOCATION'
    name         TEXT NOT NULL,
    aliases      TEXT[] NOT NULL DEFAULT '{}',
    external_ids JSONB NOT NULL DEFAULT '{}',
    locked_fields JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE volume_entities (
    volume_id     TEXT NOT NULL REFERENCES volumes(id) ON DELETE CASCADE,
    entity_id     TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    locked_fields JSONB NOT NULL DEFAULT '{}',       -- preserve manually curated entity links
    PRIMARY KEY (volume_id, entity_id)
);

-- 5. Digital Media Assets — the 1.0 manifestation type (ADR-0002, ADR-0010, ADR-0018 S5)
CREATE TABLE media_assets (
    id               TEXT PRIMARY KEY,
    volume_id        TEXT NOT NULL REFERENCES volumes(id) ON DELETE CASCADE,
    library_root_id  TEXT NOT NULL REFERENCES library_roots(id),
    rel_path_bytes   BYTEA NOT NULL,                 -- exact root-relative OS bytes; reject absolute paths and escapes
    rel_path_display TEXT NOT NULL,                  -- root-relative UTF-8 display (escaped if invalid)
    rel_path_match   TEXT NOT NULL,                  -- root-relative NFC match key, never filesystem identity
    file_size        BIGINT NOT NULL,
    file_mtime       TIMESTAMPTZ,                    -- ADR-0003 §1: last seen mtime; size+mtime unchanged = skip re-fingerprinting
    format           TEXT NOT NULL,                  -- 'CBZ', 'CBR', 'CB7', 'EPUB', 'PDF'
    page_count       INT NOT NULL DEFAULT 0,
    cover_page_index INT NOT NULL DEFAULT 0,
    suppressed_pages INT[] NOT NULL DEFAULT '{}',    -- Ad/spam pages marked Type="Deleted"
    diagnostics      JSONB NOT NULL DEFAULT '{}',    -- e.g. {"solid_archive": true, "image_codecs": ["jxl", "avif"], "invalid_utf8_path": true}
    missing_since    TIMESTAMPTZ,                    -- soft delete: file not found while its root is ONLINE
    retired_at       TIMESTAMPTZ,                    -- keep digests; clear on matching reappearance even after grace
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (library_root_id, rel_path_bytes),
    CHECK (retired_at IS NULL OR missing_since IS NOT NULL)  -- only a missing asset can be retired
);

CREATE INDEX idx_media_assets_volume ON media_assets (volume_id);

-- library_root_id has no ON DELETE CASCADE on purpose: a root with assets is removed only through
-- an explicit administrator operation that first relocates or purges them (ADR-0019 §7).
ALTER TABLE volumes ADD CONSTRAINT fk_volume_preferred_asset
    FOREIGN KEY (preferred_asset_id) REFERENCES media_assets(id) ON DELETE SET NULL
    DEFERRABLE INITIALLY DEFERRED;

-- 6. Asset Digests (ADR-0003, ADR-0018 S2): file identity as typed digests.
CREATE TABLE asset_digests (
    asset_id    TEXT NOT NULL REFERENCES media_assets(id) ON DELETE CASCADE,
    kind        TEXT NOT NULL,                       -- 'XXH3_64_PARTIAL' (scan fingerprint), 'XXH3_128_FULL' (dedupe confirmation)
    value       TEXT NOT NULL,                       -- hex
    is_current  BOOLEAN NOT NULL DEFAULT TRUE,
    reason      TEXT NOT NULL DEFAULT 'SCAN',        -- 'SCAN', 'WRITE_BACK', 'REPACK', 'EXTERNAL_MODIFY', 'DEDUPE'
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_id, kind, value)
);

CREATE UNIQUE INDEX idx_asset_digests_one_current ON asset_digests (asset_id, kind) WHERE is_current;
CREATE INDEX idx_asset_digests_current_lookup ON asset_digests (kind, value) WHERE is_current;
CREATE INDEX idx_asset_digests_history_lookup ON asset_digests (kind, value) WHERE NOT is_current;

-- 7. Reading Progress (ADR-0018 S4): Readium-style locators; adapters translate their client's format.
CREATE TABLE reading_progress (
    user_id           TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    volume_id         TEXT NOT NULL REFERENCES volumes(id) ON DELETE CASCADE,
    locator           JSONB NOT NULL,
    total_progression REAL NOT NULL DEFAULT 0,       -- 0.0–1.0 across the whole volume
    device            TEXT,
    source            TEXT NOT NULL,                 -- adapter that wrote it: 'KOMGA', 'REST', 'WEB', 'MCP', ...
    completed         BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, volume_id)
);

-- =============================================================================
-- Deferred Cross-Table Constraint Triggers (ARCHITECTURE §4.5, ADR-0008, ADR-0010, ADR-0018, ADR-0019)
-- Every trigger re-reads current committed/transaction row state at execution time (commit or
-- SET CONSTRAINTS IMMEDIATE) and acquires FOR SHARE / FOR NO KEY UPDATE row locks so concurrent
-- transactions cannot commit inconsistent cross-table state.
-- =============================================================================

-- Helper: verify that if volume v_volume_id has any series_memberships, it has exactly one primary.
CREATE OR REPLACE FUNCTION check_volume_primary_membership(v_volume_id TEXT)
RETURNS VOID AS $$
DECLARE
    v_total   BIGINT;
    v_primary BIGINT;
BEGIN
    PERFORM 1 FROM volumes WHERE id = v_volume_id FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        -- Volume was deleted (e.g., ON DELETE CASCADE removed all its memberships).
        RETURN;
    END IF;

    SELECT COUNT(*), COUNT(*) FILTER (WHERE is_primary)
    INTO v_total, v_primary
    FROM series_memberships
    WHERE volume_id = v_volume_id;

    IF v_total > 0 AND v_primary <> 1 THEN
        RAISE EXCEPTION 'volume % with series memberships must have exactly one primary membership (found % primary of % total)',
            v_volume_id, v_primary, v_total;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 1. series_memberships integrity:
--    - one-primary-membership invariant (for any non-standalone volume)
--    - same-library invariant (volumes.library_id = series.library_id)
--    - protect series.cover_volume_id when a membership is removed or moved
CREATE OR REPLACE FUNCTION trg_check_series_memberships_integrity()
RETURNS TRIGGER AS $$
DECLARE
    v_vol_lib   TEXT;
    v_ser_lib   TEXT;
    v_cover_vol TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM check_volume_primary_membership(NEW.volume_id);

        IF EXISTS (SELECT 1 FROM series_memberships WHERE volume_id = NEW.volume_id AND series_id = NEW.series_id) THEN
            SELECT library_id INTO v_vol_lib FROM volumes WHERE id = NEW.volume_id FOR SHARE;
            SELECT library_id INTO v_ser_lib FROM series WHERE id = NEW.series_id FOR SHARE;
            IF v_vol_lib IS DISTINCT FROM v_ser_lib THEN
                RAISE EXCEPTION 'series_memberships volume % (library %) and series % (library %) must belong to the same library',
                    NEW.volume_id, v_vol_lib, NEW.series_id, v_ser_lib;
            END IF;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.volume_id IS DISTINCT FROM NEW.volume_id THEN
            IF OLD.volume_id < NEW.volume_id THEN
                PERFORM check_volume_primary_membership(OLD.volume_id);
                PERFORM check_volume_primary_membership(NEW.volume_id);
            ELSE
                PERFORM check_volume_primary_membership(NEW.volume_id);
                PERFORM check_volume_primary_membership(OLD.volume_id);
            END IF;
        ELSE
            PERFORM check_volume_primary_membership(NEW.volume_id);
        END IF;

        IF EXISTS (SELECT 1 FROM series_memberships WHERE volume_id = NEW.volume_id AND series_id = NEW.series_id) THEN
            SELECT library_id INTO v_vol_lib FROM volumes WHERE id = NEW.volume_id FOR SHARE;
            SELECT library_id INTO v_ser_lib FROM series WHERE id = NEW.series_id FOR SHARE;
            IF v_vol_lib IS DISTINCT FROM v_ser_lib THEN
                RAISE EXCEPTION 'series_memberships volume % (library %) and series % (library %) must belong to the same library',
                    NEW.volume_id, v_vol_lib, NEW.series_id, v_ser_lib;
            END IF;
        END IF;

        IF OLD.series_id IS DISTINCT FROM NEW.series_id OR OLD.volume_id IS DISTINCT FROM NEW.volume_id THEN
            SELECT cover_volume_id INTO v_cover_vol FROM series WHERE id = OLD.series_id FOR SHARE;
            IF FOUND AND v_cover_vol = OLD.volume_id THEN
                IF NOT EXISTS (SELECT 1 FROM series_memberships WHERE series_id = OLD.series_id AND volume_id = OLD.volume_id) THEN
                    RAISE EXCEPTION 'cannot move membership (%, %) while series % references volume as cover_volume_id',
                        OLD.volume_id, OLD.series_id, OLD.series_id;
                END IF;
            END IF;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM check_volume_primary_membership(OLD.volume_id);

        SELECT cover_volume_id INTO v_cover_vol FROM series WHERE id = OLD.series_id FOR SHARE;
        IF FOUND AND v_cover_vol = OLD.volume_id THEN
            IF NOT EXISTS (SELECT 1 FROM series_memberships WHERE series_id = OLD.series_id AND volume_id = OLD.volume_id) THEN
                RAISE EXCEPTION 'cannot delete membership (%, %) while series % references volume as cover_volume_id',
                    OLD.volume_id, OLD.series_id, OLD.series_id;
            END IF;
        END IF;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_series_memberships_integrity
    AFTER INSERT OR UPDATE OR DELETE ON series_memberships
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION trg_check_series_memberships_integrity();

-- 2. series integrity:
--    - manual cover_volume_id must be a member of that series
--    - changing series.library_id must keep all member volumes in the same library
CREATE OR REPLACE FUNCTION trg_check_series_integrity()
RETURNS TRIGGER AS $$
DECLARE
    v_ser_lib   TEXT;
    v_cover_vol TEXT;
BEGIN
    SELECT library_id, cover_volume_id INTO v_ser_lib, v_cover_vol
    FROM series WHERE id = NEW.id FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    IF v_cover_vol IS NOT NULL THEN
        PERFORM 1
        FROM series_memberships
        WHERE series_id = NEW.id
          AND volume_id = v_cover_vol
        FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'series % cover_volume_id % must be a member of that series', NEW.id, v_cover_vol;
        END IF;
    END IF;

    PERFORM 1
    FROM series_memberships m
    JOIN volumes v ON v.id = m.volume_id
    WHERE m.series_id = NEW.id
    FOR SHARE OF m, v;

    IF EXISTS (
        SELECT 1
        FROM series_memberships m
        JOIN volumes v ON v.id = m.volume_id
        WHERE m.series_id = NEW.id
          AND v.library_id IS DISTINCT FROM v_ser_lib
    ) THEN
        RAISE EXCEPTION 'series % library % conflicts with member volume library', NEW.id, v_ser_lib;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_series_integrity
    AFTER INSERT OR UPDATE OF library_id, cover_volume_id ON series
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION trg_check_series_integrity();

-- 3. volumes integrity:
--    - preferred_asset_id must belong to this volume
--    - changing volumes.library_id must keep same-library memberships and same-library asset roots
CREATE OR REPLACE FUNCTION trg_check_volumes_integrity()
RETURNS TRIGGER AS $$
DECLARE
    v_vol_lib    TEXT;
    v_pref_asset TEXT;
    v_asset_vol  TEXT;
BEGIN
    SELECT library_id, preferred_asset_id INTO v_vol_lib, v_pref_asset
    FROM volumes WHERE id = NEW.id FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    IF v_pref_asset IS NOT NULL THEN
        SELECT volume_id INTO v_asset_vol
        FROM media_assets
        WHERE id = v_pref_asset
        FOR SHARE;
        IF NOT FOUND OR v_asset_vol IS DISTINCT FROM NEW.id THEN
            RAISE EXCEPTION 'volume % preferred_asset_id % must belong to that volume (belongs to %)',
                NEW.id, v_pref_asset, v_asset_vol;
        END IF;
    END IF;

    PERFORM 1
    FROM series_memberships m
    JOIN series s ON s.id = m.series_id
    WHERE m.volume_id = NEW.id
    FOR SHARE OF m, s;

    IF EXISTS (
        SELECT 1
        FROM series_memberships m
        JOIN series s ON s.id = m.series_id
        WHERE m.volume_id = NEW.id
          AND s.library_id IS DISTINCT FROM v_vol_lib
    ) THEN
        RAISE EXCEPTION 'volume % library % conflicts with series membership library', NEW.id, v_vol_lib;
    END IF;

    PERFORM 1
    FROM media_assets a
    JOIN library_roots r ON r.id = a.library_root_id
    WHERE a.volume_id = NEW.id
    FOR SHARE OF a, r;

    IF EXISTS (
        SELECT 1
        FROM media_assets a
        JOIN library_roots r ON r.id = a.library_root_id
        WHERE a.volume_id = NEW.id
          AND r.library_id IS DISTINCT FROM v_vol_lib
    ) THEN
        RAISE EXCEPTION 'volume % library % conflicts with media_asset library_root library', NEW.id, v_vol_lib;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_volumes_integrity
    AFTER INSERT OR UPDATE OF library_id, preferred_asset_id ON volumes
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION trg_check_volumes_integrity();

-- 4. media_assets integrity:
--    - asset's volume and library_root must belong to the same library
--    - reassigning media_assets.volume_id cannot orphan a volume's preferred_asset_id
CREATE OR REPLACE FUNCTION trg_check_media_assets_integrity()
RETURNS TRIGGER AS $$
DECLARE
    v_vol_id     TEXT;
    v_root_id    TEXT;
    v_vol_lib    TEXT;
    v_root_lib   TEXT;
    v_pref_asset TEXT;
BEGIN
    SELECT volume_id, library_root_id INTO v_vol_id, v_root_id
    FROM media_assets WHERE id = NEW.id FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    SELECT library_id INTO v_vol_lib FROM volumes WHERE id = v_vol_id FOR SHARE;
    SELECT library_id INTO v_root_lib FROM library_roots WHERE id = v_root_id FOR SHARE;
    IF v_vol_lib IS DISTINCT FROM v_root_lib THEN
        RAISE EXCEPTION 'media_asset % volume % (library %) and library_root % (library %) must belong to the same library',
            NEW.id, v_vol_id, v_vol_lib, v_root_id, v_root_lib;
    END IF;

    IF TG_OP = 'UPDATE' AND OLD.volume_id IS DISTINCT FROM v_vol_id THEN
        SELECT preferred_asset_id INTO v_pref_asset
        FROM volumes WHERE id = OLD.volume_id FOR SHARE;
        IF FOUND AND v_pref_asset = OLD.id THEN
            RAISE EXCEPTION 'cannot reassign asset % from volume % while it is preferred_asset_id',
                OLD.id, OLD.volume_id;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_media_assets_integrity
    AFTER INSERT OR UPDATE OF volume_id, library_root_id ON media_assets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION trg_check_media_assets_integrity();

-- 5. library_roots integrity:
--    - changing library_roots.library_id must keep all its assets aligned with their volumes' library
CREATE OR REPLACE FUNCTION trg_check_library_roots_integrity()
RETURNS TRIGGER AS $$
DECLARE
    v_root_lib TEXT;
BEGIN
    SELECT library_id INTO v_root_lib FROM library_roots WHERE id = NEW.id FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    PERFORM 1
    FROM media_assets a
    JOIN volumes v ON v.id = a.volume_id
    WHERE a.library_root_id = NEW.id
    FOR SHARE OF a, v;

    IF EXISTS (
        SELECT 1
        FROM media_assets a
        JOIN volumes v ON v.id = a.volume_id
        WHERE a.library_root_id = NEW.id
          AND v.library_id IS DISTINCT FROM v_root_lib
    ) THEN
        RAISE EXCEPTION 'library_root % library % conflicts with media_asset volume library', NEW.id, v_root_lib;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_library_roots_integrity
    AFTER UPDATE OF library_id ON library_roots
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION trg_check_library_roots_integrity();
