-- 0006_domain_event_sequence.sql: Singleton monotonic publication-sequence watermark
-- for the serialized transactional outbox dispatcher (ADR-0018 S3, M1-T17.2).
-- Forward-only additive migration (ADR-0018 §2).

CREATE TABLE IF NOT EXISTS domain_event_sequence (
    id            BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0)
);

INSERT INTO domain_event_sequence (id, last_sequence)
SELECT TRUE, COALESCE(MAX(published_sequence), 0)
FROM domain_events
ON CONFLICT (id) DO NOTHING;
