-- 0008_people_entities_import_keys.sql: Add nullable normalized import keys on people and
-- entities for M1B-D4 cross-library name-only import matching while preserving curated
-- same-name identities and pre-upgrade IDs/references (ADR-0008 §6).
-- Forward-only additive migration (ADR-0001 §6, ADR-0018 §2).

ALTER TABLE people
    ADD COLUMN import_key TEXT,
    ADD CONSTRAINT people_import_key_nonempty CHECK (import_key IS NULL OR import_key <> '');

CREATE UNIQUE INDEX idx_people_import_key
    ON people (import_key)
    WHERE import_key IS NOT NULL;

ALTER TABLE entities
    ADD COLUMN import_key TEXT,
    ADD CONSTRAINT entities_import_key_nonempty CHECK (import_key IS NULL OR import_key <> '');

CREATE UNIQUE INDEX idx_entities_import_key
    ON entities (kind, import_key)
    WHERE import_key IS NOT NULL;

CREATE INDEX idx_credits_person ON credits (person_id);
CREATE INDEX idx_volume_entities_entity ON volume_entities (entity_id);
