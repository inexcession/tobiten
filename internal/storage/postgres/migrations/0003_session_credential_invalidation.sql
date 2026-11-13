-- 0003_session_credential_invalidation.sql: Persistently revoke derived sessions
-- when an originating credential is revoked or its secret/identity changes (ADR-0006 §3).

CREATE OR REPLACE FUNCTION revoke_sessions_on_credential_change()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.revoked_at IS NOT NULL AND OLD.revoked_at IS NULL)
       OR (NEW.secret_hash IS DISTINCT FROM OLD.secret_hash)
       OR (NEW.user_id IS DISTINCT FROM OLD.user_id)
       OR (NEW.kind IS DISTINCT FROM OLD.kind) THEN
        UPDATE sessions
        SET revoked_at = COALESCE(revoked_at, COALESCE(NEW.revoked_at, NOW()))
        WHERE credential_id = NEW.id
          AND revoked_at IS NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_credentials_revoke_sessions ON credentials;
CREATE TRIGGER trg_credentials_revoke_sessions
    AFTER UPDATE ON credentials
    FOR EACH ROW
    EXECUTE FUNCTION revoke_sessions_on_credential_change();
