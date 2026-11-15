-- 0005_labels.sql: Hardens SERVER-only policy trigger against Repeatable Read snapshot races,
-- protects the canonical trash label and its item marks across generic mutations/reseeding,
-- and seeds built-in SERVER (marked_for_deletion) and per-user USER (want-to-read, favorite, dnf) labels.
-- Forward-only additive migration (ADR-0006 §5, M1-T9.1).

-- Replace the lock-only FOR SHARE check with a no-op tuple update on a non-scope column.
-- Updating built_in creates a new tuple version of the referenced labels row without firing
-- AFTER UPDATE OF scope ON labels, forcing any concurrent REPEATABLE READ transaction that
-- attempts to change that label's scope from SERVER to USER to fail with serialization error
-- 40001 instead of reading a stale snapshot that misses the newly committed policy row.
CREATE OR REPLACE FUNCTION trg_check_user_label_policy_scope()
RETURNS TRIGGER AS $$
DECLARE
    v_scope TEXT;
BEGIN
    UPDATE labels
    SET built_in = built_in
    WHERE id = NEW.label_id
    RETURNING scope INTO v_scope;

    IF v_scope IS DISTINCT FROM 'SERVER' THEN
        RAISE EXCEPTION 'user_label_policies only permit SERVER labels, got % for label_id %', v_scope, NEW.label_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Ensure the canonical SERVER trash label row (id = 019318a0-0000-7000-8000-000000000030,
-- name = marked_for_deletion) exists cleanly, preserving any pre-existing marks and policies.
DO $$
DECLARE
    v_canonical_id CONSTANT TEXT := '019318a0-0000-7000-8000-000000000030';
    v_existing_id TEXT;
BEGIN
    SELECT id INTO v_existing_id
    FROM labels
    WHERE scope = 'SERVER' AND name = 'marked_for_deletion';

    IF v_existing_id IS NOT NULL AND v_existing_id <> v_canonical_id THEN
        UPDATE labels
        SET name = '__legacy_marked_for_deletion__' || v_existing_id
        WHERE id = v_existing_id;
    END IF;

    INSERT INTO labels (id, scope, owner_id, name, built_in)
    VALUES (v_canonical_id, 'SERVER', NULL, 'marked_for_deletion', TRUE)
    ON CONFLICT (id) DO UPDATE
    SET scope = 'SERVER',
        owner_id = NULL,
        name = 'marked_for_deletion',
        built_in = TRUE;

    IF v_existing_id IS NOT NULL AND v_existing_id <> v_canonical_id THEN
        INSERT INTO item_labels (label_id, item_type, item_id)
        SELECT v_canonical_id, item_type, item_id
        FROM item_labels
        WHERE label_id = v_existing_id
        ON CONFLICT (label_id, item_type, item_id) DO NOTHING;

        INSERT INTO user_label_policies (user_id, label_id, effect)
        SELECT user_id, v_canonical_id, effect
        FROM user_label_policies
        WHERE label_id = v_existing_id
        ON CONFLICT (user_id, label_id) DO NOTHING;

        DELETE FROM user_label_policies WHERE label_id = v_existing_id;
        DELETE FROM labels WHERE id = v_existing_id;
    END IF;
END $$;

-- Protect the canonical trash label and its staged item_labels marks against generic
-- renames, scope/owner/built_in changes, and deletions (which would cascade-delete marks).
-- No-op updates (such as trg_check_user_label_policy_scope's SET built_in = built_in or
-- idempotent reseeding) leave every column identical and remain permitted.
CREATE OR REPLACE FUNCTION trg_protect_canonical_trash_label()
RETURNS TRIGGER AS $$
DECLARE
    v_is_canonical BOOLEAN;
BEGIN
    v_is_canonical := OLD.id = '019318a0-0000-7000-8000-000000000030'
        OR (OLD.scope = 'SERVER' AND OLD.name = 'marked_for_deletion');

    IF NOT v_is_canonical THEN
        RETURN COALESCE(NEW, OLD);
    END IF;

    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'cannot delete protected built-in trash label %', OLD.id;
    END IF;

    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.scope IS DISTINCT FROM 'SERVER'
       OR NEW.owner_id IS NOT NULL
       OR NEW.name IS DISTINCT FROM 'marked_for_deletion'
       OR NEW.built_in IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'cannot modify protected built-in trash label %', OLD.id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_labels_protect_canonical_trash
    BEFORE UPDATE OR DELETE ON labels
    FOR EACH ROW
    EXECUTE FUNCTION trg_protect_canonical_trash_label();

-- Backfill personal built-in USER labels for all existing accounts while preserving any
-- pre-existing personal label IDs and item_labels assignments.
INSERT INTO labels (id, scope, owner_id, name, built_in)
SELECT uuidv7()::text, 'USER', u.id, v.name, TRUE
FROM users u
CROSS JOIN (VALUES ('want-to-read'), ('favorite'), ('dnf')) AS v(name)
ON CONFLICT (owner_id, name) WHERE scope = 'USER' DO UPDATE
SET built_in = TRUE;
