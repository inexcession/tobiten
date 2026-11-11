//go:build generator

package migrations_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

const sampleSeedDataSQL = `
-- 1. system_settings
INSERT INTO system_settings (key, value, is_secret) VALUES
    ('oidc.default_role_id', '"019318a0-0000-7000-8000-000000000003"'::jsonb, false),
    ('events.retention_days', '7'::jsonb, false),
    ('backup.passphrase', '"Y2hhbmdlbWUtc2VjcmV0"'::jsonb, true);

-- 2. users
INSERT INTO users (id, username, created_at, updated_at) VALUES
    ('019318a0-0000-7000-8000-000000000010', 'admin', '2026-11-10 12:00:00+00', '2026-11-10 12:00:00+00'),
    ('019318a0-0000-7000-8000-000000000011', 'alice', '2026-11-10 12:01:00+00', '2026-11-10 12:01:00+00'),
    ('019318a0-0000-7000-8000-000000000012', 'bob',   '2026-11-10 12:02:00+00', '2026-11-10 12:02:00+00');

UPDATE system_settings SET updated_by = '019318a0-0000-7000-8000-000000000010' WHERE key = 'events.retention_days';

-- 3. credentials
INSERT INTO credentials (id, user_id, kind, scopes, secret_hash, hash_scheme, external_subject, label, created_at) VALUES
    ('019318a0-0000-7000-8000-000000000020', '019318a0-0000-7000-8000-000000000010', 'PASSWORD', '{}', '$argon2id$v=19$m=65536,t=3,p=4$adminhash', 'argon2id', NULL, 'Admin password', '2026-11-10 12:00:00+00'),
    ('019318a0-0000-7000-8000-000000000021', '019318a0-0000-7000-8000-000000000010', 'PAT', '{read,progress:write,metadata:write,library:admin,admin}', '$argon2id$v=19$m=65536,t=3,p=4$patadminhash', 'argon2id', NULL, 'Admin PAT', '2026-11-10 12:05:00+00'),
    ('019318a0-0000-7000-8000-000000000022', '019318a0-0000-7000-8000-000000000011', 'PAT', '{read,progress:write}', '$argon2id$v=19$m=65536,t=3,p=4$patalicehash', 'argon2id', NULL, 'Mihon on Pixel', '2026-11-10 12:10:00+00'),
    ('019318a0-0000-7000-8000-000000000023', '019318a0-0000-7000-8000-000000000012', 'OIDC_LINK', '{}', NULL, NULL, 'https://auth.example.com|bob-subject-id', 'Authelia SSO', '2026-11-10 12:15:00+00');

-- 4. roles & permissions (built-in roles 1..4 already exist from 0001_init.sql)
INSERT INTO roles (id, name, built_in, requires_allow_labels) VALUES
    ('019318a0-0000-7000-8000-000000000005', 'Junior Curator', false, false);
INSERT INTO role_permissions (role_id, permission) VALUES
    ('019318a0-0000-7000-8000-000000000005', 'library:read'),
    ('019318a0-0000-7000-8000-000000000005', 'metadata:edit');

-- 5. user_roles
INSERT INTO user_roles (user_id, role_id) VALUES
    ('019318a0-0000-7000-8000-000000000010', '019318a0-0000-7000-8000-000000000001'),
    ('019318a0-0000-7000-8000-000000000011', '019318a0-0000-7000-8000-000000000003'),
    ('019318a0-0000-7000-8000-000000000012', '019318a0-0000-7000-8000-000000000004');

-- 6. labels (SERVER and USER)
INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES
    ('019318a0-0000-7000-8000-000000000030', 'SERVER', NULL, 'marked_for_deletion', true),
    ('019318a0-0000-7000-8000-000000000031', 'SERVER', NULL, 'kids-safe', false),
    ('019318a0-0000-7000-8000-000000000032', 'SERVER', NULL, 'mature-content', false),
    ('019318a0-0000-7000-8000-000000000033', 'USER', '019318a0-0000-7000-8000-000000000011', 'favorite', true),
    ('019318a0-0000-7000-8000-000000000034', 'USER', '019318a0-0000-7000-8000-000000000011', 'want-to-read', true);

-- 7. item_labels
INSERT INTO item_labels (label_id, item_type, item_id) VALUES
    ('019318a0-0000-7000-8000-000000000031', 'SERIES', '019318a0-0000-7000-8000-000000000090'),
    ('019318a0-0000-7000-8000-000000000032', 'SERIES', '019318a0-0000-7000-8000-000000000091'),
    ('019318a0-0000-7000-8000-000000000033', 'VOLUME', '019318a0-0000-7000-8000-000000000092');

-- 8. user_label_policies
INSERT INTO user_label_policies (user_id, label_id, effect) VALUES
    ('019318a0-0000-7000-8000-000000000012', '019318a0-0000-7000-8000-000000000031', 'ALLOW'),
    ('019318a0-0000-7000-8000-000000000011', '019318a0-0000-7000-8000-000000000032', 'RESTRICT');

-- 9. domain_events
INSERT INTO domain_events (id, published_sequence, published_at, kind, version, aggregate_type, aggregate_id, payload, created_at) VALUES
    (1, 1, '2026-11-10 12:00:00+00', 'user.created', 1, 'user', '019318a0-0000-7000-8000-000000000010', '{"username":"admin"}'::jsonb, '2026-11-10 12:00:00+00'),
    (2, NULL, NULL, 'job.started', 1, 'job', '019318a0-0000-7000-8000-000000000040', '{"kind":"SCAN"}'::jsonb, '2026-11-10 12:30:00+00');
SELECT setval('domain_events_id_seq', 2, true);

-- 10. event_subscribers
INSERT INTO event_subscribers (id, kind, position, state, stalled_at, updated_at) VALUES
    ('notify:shoutrrr', 'shoutrrr', 1, 'ACTIVE', NULL, '2026-11-10 12:00:00+00'),
    ('webhook:discord', 'webhook', 0, 'PAUSED', NULL, '2026-11-10 12:05:00+00');

-- 11. jobs
INSERT INTO jobs (id, kind, state, payload, checkpoint, attempts, max_attempts, cancel_requested, last_error, created_by, dedupe_key, run_after, created_at, started_at, updated_at, finished_at) VALUES
    ('019318a0-0000-7000-8000-000000000040', 'SCAN', 'QUEUED', '{"library_id":"019318a0-0000-7000-8000-000000000080"}'::jsonb, '{}'::jsonb, 0, 3, false, NULL, '019318a0-0000-7000-8000-000000000010', 'SCAN:019318a0-0000-7000-8000-000000000080', '2026-11-10 12:30:00+00', '2026-11-10 12:30:00+00', NULL, '2026-11-10 12:30:00+00', NULL),
    ('019318a0-0000-7000-8000-000000000041', 'PURGE', 'SUCCEEDED', '{"items_purged":3}'::jsonb, '{"stage":"completed"}'::jsonb, 1, 3, false, NULL, '019318a0-0000-7000-8000-000000000010', NULL, '2026-11-10 12:35:00+00', '2026-11-10 12:35:00+00', '2026-11-10 12:35:01+00', '2026-11-10 12:35:05+00', '2026-11-10 12:35:05+00');
`

func findPgDump() (string, error) {
	candidates := []string{
		"/opt/local/lib/postgresql18/bin/pg_dump",
		"/opt/homebrew/opt/postgresql@18/bin/pg_dump",
		"/usr/local/opt/postgresql@18/bin/pg_dump",
		"/usr/lib/postgresql/18/bin/pg_dump",
	}
	if envBin := os.Getenv("TOBITEN_PG_BIN"); envBin != "" {
		candidates = append([]string{filepath.Join(envBin, "pg_dump")}, candidates...)
	}
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return c, nil
		}
	}
	return exec.LookPath("pg_dump")
}

// TestGenerateSeed0001 is a maintenance generator invoked with:
//
//	go test -v -tags=generator -run TestGenerateSeed0001 ./internal/storage/postgres/migrations/...
//
// It boots a temporary PostgreSQL instance, applies 0001_init.sql, inserts standard
// sample rows, dumps via pg_dump, and writes testdata/seeds/0001.sql.
func TestGenerateSeed0001(t *testing.T) {
	db := testdb.New(t)
	ctx := context.Background()

	// 1. Apply core migrations (0001_init.sql)
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("applying core migrations: %v", err)
	}

	// 2. Insert sample seed rows
	if _, err := db.Exec(ctx, sampleSeedDataSQL); err != nil {
		t.Fatalf("inserting sample seed rows: %v", err)
	}

	// 3. Find pg_dump
	pgDumpPath, err := findPgDump()
	if err != nil {
		t.Fatalf("pg_dump not found: %v", err)
	}

	// 4. Run pg_dump
	cmd := exec.Command(pgDumpPath,
		"--no-owner",
		"--no-privileges",
		"--inserts",
		db.URL,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("pg_dump failed: %v\nOutput: %s", err, string(out))
	}

	seedsDir := filepath.Join("testdata", "seeds")
	if err := os.MkdirAll(seedsDir, 0755); err != nil {
		t.Fatalf("creating seeds dir: %v", err)
	}

	target := filepath.Join(seedsDir, "0001.sql")
	if err := os.WriteFile(target, out, 0644); err != nil {
		t.Fatalf("writing seed file %s: %v", target, err)
	}
	t.Logf("Successfully generated seed fixture: %s (%d bytes)", target, len(out))
}
