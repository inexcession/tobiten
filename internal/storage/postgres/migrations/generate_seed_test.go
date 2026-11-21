//go:build generator

package migrations_test

import (
	"bytes"
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

	// 1. Apply only 0001_init.sql so the historical 0001 seed stays pinned to version 1.
	coreMigrations, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("collecting core migrations: %v", err)
	}
	var mig0001 []migrations.Migration
	for _, m := range coreMigrations {
		if m.Version == 1 {
			mig0001 = append(mig0001, m)
			break
		}
	}
	if len(mig0001) != 1 || mig0001[0].Name != "0001_init.sql" {
		t.Fatalf("expected 0001_init.sql in CoreFS, got %+v", mig0001)
	}
	if err := migrations.Run(ctx, db, mig0001); err != nil {
		t.Fatalf("applying 0001_init.sql: %v", err)
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

const sampleCatalogue0007DataSQL = `
-- 12. libraries & library_roots
INSERT INTO libraries (id, name, mode, resolver_chain, metadata_precedence, default_cover_policy, created_at) VALUES
    ('019318a0-0000-7000-8000-000000000080', 'Manga', 'VIRTUAL', '{metadata,folder,filename}', 'EMBEDDED_OVER_ONLINE', 'FIRST', '2026-11-10 12:00:00+00'),
    ('019318a0-0000-7000-8000-000000000081', 'Comics', 'VIRTUAL', '{metadata,folder,filename}', 'EMBEDDED_OVER_ONLINE', 'FIRST', '2026-11-10 12:01:00+00');

INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match, state) VALUES
    ('019318a0-0000-7000-8000-000000000085', '019318a0-0000-7000-8000-000000000080', '\x2f7372762f6d616e6761', '/srv/manga', '/srv/manga', 'ONLINE'),
    ('019318a0-0000-7000-8000-000000000086', '019318a0-0000-7000-8000-000000000081', '\x2f7372762f636f6d696373', '/srv/comics', '/srv/comics', 'ONLINE');

-- 13. series & volumes & series_memberships
INSERT INTO series (id, library_id, title, title_sort, status, created_at, updated_at) VALUES
    ('019318a0-0000-7000-8000-000000000090', '019318a0-0000-7000-8000-000000000080', 'One Piece', 'One Piece', 'ONGOING', '2026-11-10 12:05:00+00', '2026-11-10 12:05:00+00'),
    ('019318a0-0000-7000-8000-000000000091', '019318a0-0000-7000-8000-000000000081', 'Batman', 'Batman', 'ONGOING', '2026-11-10 12:06:00+00', '2026-11-10 12:06:00+00');

INSERT INTO volumes (id, library_id, kind, number, title, page_count, created_at, updated_at) VALUES
    ('019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000080', 'VOLUME', 1, 'Romance Dawn', 200, '2026-11-10 12:05:00+00', '2026-11-10 12:05:00+00'),
    ('019318a0-0000-7000-8000-000000000093', '019318a0-0000-7000-8000-000000000081', 'ISSUE', 1, 'Year One', 32, '2026-11-10 12:06:00+00', '2026-11-10 12:06:00+00');

INSERT INTO series_memberships (volume_id, series_id, is_primary, kind, position, source, locked_fields) VALUES
    ('019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000090', true, 'SERIES', 1, 'RESOLVER', '{}'::jsonb),
    ('019318a0-0000-7000-8000-000000000093', '019318a0-0000-7000-8000-000000000091', true, 'SERIES', 1, 'RESOLVER', '{}'::jsonb);

-- 14. media_assets & asset_digests
INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format, page_count, created_at, updated_at) VALUES
    ('019318a0-0000-7000-8000-000000000098', '019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000085', '\x4f6e65205069656365207630312e63627a', 'One Piece v01.cbz', 'One Piece v01.cbz', 1048576, 'CBZ', 200, '2026-11-10 12:05:00+00', '2026-11-10 12:05:00+00');

INSERT INTO asset_digests (asset_id, kind, value, is_current, reason, recorded_at) VALUES
    ('019318a0-0000-7000-8000-000000000098', 'XXH3_64_PARTIAL', '0123456789abcdef', true, 'SCAN', '2026-11-10 12:05:00+00');

-- 15. people, credits, entities, volume_entities (including same-name pre-0008 rows and field locks)
INSERT INTO people (id, name, name_native, name_romanized, aliases, external_ids, locked_fields) VALUES
    ('019318a0-0000-7000-8000-000000000101', 'Eiichiro Oda', '尾田栄一郎', 'Oda Eiichiro', '{Oda}', '{"anilist": 1881}'::jsonb, '{"name": true, "name_native": true}'::jsonb),
    ('019318a0-0000-7000-8000-000000000102', 'John Smith', NULL, NULL, '{"J. Smith"}', '{"comicvine": 100}'::jsonb, '{}'::jsonb),
    ('019318a0-0000-7000-8000-000000000103', 'John Smith', NULL, NULL, '{}', '{"comicvine": 200}'::jsonb, '{"external_ids": true}'::jsonb);

INSERT INTO credits (id, person_id, series_id, volume_id, role, position, locked_fields) VALUES
    ('019318a0-0000-7000-8000-000000000201', '019318a0-0000-7000-8000-000000000101', '019318a0-0000-7000-8000-000000000090', NULL, 'AUTHOR', 0, '{"role": true}'::jsonb),
    ('019318a0-0000-7000-8000-000000000202', '019318a0-0000-7000-8000-000000000101', NULL, '019318a0-0000-7000-8000-000000000092', 'WRITER', 0, '{}'::jsonb),
    ('019318a0-0000-7000-8000-000000000203', '019318a0-0000-7000-8000-000000000102', NULL, '019318a0-0000-7000-8000-000000000093', 'PENCILLER', 0, '{}'::jsonb),
    ('019318a0-0000-7000-8000-000000000204', '019318a0-0000-7000-8000-000000000103', NULL, '019318a0-0000-7000-8000-000000000093', 'INKER', 1, '{}'::jsonb);

INSERT INTO entities (id, kind, name, aliases, external_ids, locked_fields) VALUES
    ('019318a0-0000-7000-8000-000000000301', 'CHARACTER', 'Monkey D. Luffy', '{Luffy}', '{"anilist": 40}'::jsonb, '{"name": true}'::jsonb),
    ('019318a0-0000-7000-8000-000000000302', 'TEAM', 'Straw Hat Pirates', '{}', '{}'::jsonb, '{}'::jsonb),
    ('019318a0-0000-7000-8000-000000000303', 'LOCATION', 'Gotham', '{"Gotham City"}', '{}'::jsonb, '{}'::jsonb),
    ('019318a0-0000-7000-8000-000000000304', 'CHARACTER', 'Gotham', '{}', '{}'::jsonb, '{}'::jsonb);

INSERT INTO volume_entities (volume_id, entity_id, locked_fields) VALUES
    ('019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000301', '{"entity": true}'::jsonb),
    ('019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000302', '{}'::jsonb),
    ('019318a0-0000-7000-8000-000000000093', '019318a0-0000-7000-8000-000000000303', '{}'::jsonb),
    ('019318a0-0000-7000-8000-000000000093', '019318a0-0000-7000-8000-000000000304', '{}'::jsonb);
`

// TestGenerateSeed0007 is a maintenance generator invoked with:
//
//	go test -v -tags=generator -run TestGenerateSeed0007 ./internal/storage/postgres/migrations/...
//
// It boots a temporary PostgreSQL instance, applies 0001_init.sql + M1A seed data,
// upgrades through 0007, inserts populated catalogue rows (including people, credits,
// entities, volume_entities), dumps via pg_dump, and writes testdata/seeds/0007.sql.
func TestGenerateSeed0007(t *testing.T) {
	db := testdb.New(t)
	ctx := context.Background()

	coreMigrations, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("collecting core migrations: %v", err)
	}
	var mig0001 []migrations.Migration
	var migsUpTo7 []migrations.Migration
	for _, m := range coreMigrations {
		if m.Version == 1 {
			mig0001 = append(mig0001, m)
		}
		if m.Version <= 7 {
			migsUpTo7 = append(migsUpTo7, m)
		}
	}
	if len(mig0001) != 1 || len(migsUpTo7) != 7 {
		t.Fatalf("expected 1 migration at v1 and 7 up to v7, got %d and %d", len(mig0001), len(migsUpTo7))
	}
	if err := migrations.Run(ctx, db, mig0001); err != nil {
		t.Fatalf("applying 0001_init.sql: %v", err)
	}
	if _, err := db.Exec(ctx, sampleSeedDataSQL); err != nil {
		t.Fatalf("inserting sample M1A seed rows: %v", err)
	}
	if err := migrations.Run(ctx, db, migsUpTo7); err != nil {
		t.Fatalf("applying migrations 0002..0007: %v", err)
	}
	if _, err := db.Exec(ctx, sampleCatalogue0007DataSQL); err != nil {
		t.Fatalf("inserting sample catalogue 0007 seed rows: %v", err)
	}

	pgDumpPath, err := findPgDump()
	if err != nil {
		t.Fatalf("pg_dump not found: %v", err)
	}

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

	out = append(bytes.TrimRight(out, "\r\n"), '\n')
	target := filepath.Join(seedsDir, "0007.sql")
	if err := os.WriteFile(target, out, 0644); err != nil {
		t.Fatalf("writing seed file %s: %v", target, err)
	}
	t.Logf("Successfully generated seed fixture: %s (%d bytes)", target, len(out))
}

const sampleCatalogue0009DataSQL = `
-- 16. 0009-seeded keyed series and its references (volumes, memberships, assets, credits, item_labels)
INSERT INTO series (id, library_id, title, title_sort, status, created_at, updated_at, resolver_key) VALUES
    ('019318a0-0000-7000-8000-000000000094', '019318a0-0000-7000-8000-000000000080', 'Dorohedoro', 'Dorohedoro', 'COMPLETED', '2026-11-11 10:00:00+00', '2026-11-11 10:00:00+00', 'title:run=2018:dorohedoro');

INSERT INTO volumes (id, library_id, kind, number, title, page_count, created_at, updated_at) VALUES
    ('019318a0-0000-7000-8000-000000000095', '019318a0-0000-7000-8000-000000000080', 'VOLUME', 1, 'Dorohedoro v01', 192, '2026-11-11 10:00:00+00', '2026-11-11 10:00:00+00');

INSERT INTO series_memberships (volume_id, series_id, is_primary, kind, position, source, locked_fields) VALUES
    ('019318a0-0000-7000-8000-000000000095', '019318a0-0000-7000-8000-000000000094', true, 'SERIES', 1, 'RESOLVER', '{}'::jsonb);

INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format, page_count, created_at, updated_at) VALUES
    ('019318a0-0000-7000-8000-000000000099', '019318a0-0000-7000-8000-000000000095', '019318a0-0000-7000-8000-000000000085', '\x446f726f6865646f726f207630312e63627a', 'Dorohedoro v01.cbz', 'Dorohedoro v01.cbz', 2097152, 'CBZ', 192, '2026-11-11 10:00:00+00', '2026-11-11 10:00:00+00');

INSERT INTO asset_digests (asset_id, kind, value, is_current, reason, recorded_at) VALUES
    ('019318a0-0000-7000-8000-000000000099', 'XXH3_64_PARTIAL', 'fedcba9876543210', true, 'SCAN', '2026-11-11 10:00:00+00');

INSERT INTO credits (id, person_id, series_id, volume_id, role, position, locked_fields) VALUES
    ('019318a0-0000-7000-8000-000000000205', '019318a0-0000-7000-8000-000000000101', '019318a0-0000-7000-8000-000000000094', NULL, 'ARTIST', 0, '{}'::jsonb);

INSERT INTO item_labels (label_id, item_type, item_id) VALUES
    ('019318a0-0000-7000-8000-000000000033', 'SERIES', '019318a0-0000-7000-8000-000000000094');
`

// TestGenerateSeed0009 is a maintenance generator invoked with:
//
//	go test -v -tags=generator -run TestGenerateSeed0009 ./internal/storage/postgres/migrations/...
//
// It boots a temporary PostgreSQL instance, applies 0001_init.sql + M1A seed data,
// upgrades through 0009, inserts populated 0007 + 0009 catalogue rows (including a
// keyed series and its referencing rows), dumps via pg_dump, and writes
// testdata/seeds/0009.sql.
func TestGenerateSeed0009(t *testing.T) {
	db := testdb.New(t)
	ctx := context.Background()

	coreMigrations, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("collecting core migrations: %v", err)
	}
	var mig0001 []migrations.Migration
	var migsUpTo9 []migrations.Migration
	for _, m := range coreMigrations {
		if m.Version == 1 {
			mig0001 = append(mig0001, m)
		}
		if m.Version <= 9 {
			migsUpTo9 = append(migsUpTo9, m)
		}
	}
	if len(mig0001) != 1 || len(migsUpTo9) != 9 {
		t.Fatalf("expected 1 migration at v1 and 9 up to v9, got %d and %d", len(mig0001), len(migsUpTo9))
	}
	if err := migrations.Run(ctx, db, mig0001); err != nil {
		t.Fatalf("applying 0001_init.sql: %v", err)
	}
	if _, err := db.Exec(ctx, sampleSeedDataSQL); err != nil {
		t.Fatalf("inserting sample M1A seed rows: %v", err)
	}
	if err := migrations.Run(ctx, db, migsUpTo9); err != nil {
		t.Fatalf("applying migrations 0002..0009: %v", err)
	}
	if _, err := db.Exec(ctx, sampleCatalogue0007DataSQL); err != nil {
		t.Fatalf("inserting sample catalogue 0007 seed rows: %v", err)
	}
	if _, err := db.Exec(ctx, sampleCatalogue0009DataSQL); err != nil {
		t.Fatalf("inserting sample catalogue 0009 seed rows: %v", err)
	}

	pgDumpPath, err := findPgDump()
	if err != nil {
		t.Fatalf("pg_dump not found: %v", err)
	}

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

	out = append(bytes.TrimRight(out, "\r\n"), '\n')
	target := filepath.Join(seedsDir, "0009.sql")
	if err := os.WriteFile(target, out, 0644); err != nil {
		t.Fatalf("writing seed file %s: %v", target, err)
	}
	t.Logf("Successfully generated seed fixture: %s (%d bytes)", target, len(out))
}
