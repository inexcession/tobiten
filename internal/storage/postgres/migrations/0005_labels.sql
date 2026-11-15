-- 0005_labels.sql: Hardens SERVER-only policy trigger against Repeatable Read snapshot races
-- and seeds the built-in SERVER label (marked_for_deletion).
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

-- Seed the built-in SERVER label used to mark items for deletion (ADR-0006 §5).
-- Existing upgraded databases seeded from 0001.sql already contain this row, so conflict
-- targets the partial unique index idx_labels_server cleanly while ensuring built_in = TRUE.
INSERT INTO labels (id, scope, owner_id, name, built_in)
VALUES ('019318a0-0000-7000-8000-000000000030', 'SERVER', NULL, 'marked_for_deletion', TRUE)
ON CONFLICT (name) WHERE scope = 'SERVER' DO UPDATE
SET built_in = TRUE;
