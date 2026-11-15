package migrations_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func applyCatalogueMigrations(t *testing.T) migrations.DB {
	t.Helper()
	return apply0001Init(t)
}

func seedBasicLibrary(ctx context.Context, t *testing.T, db migrations.DB, libID, name, rootID string, rootPath string) {
	t.Helper()
	if _, err := db.Exec(ctx, `INSERT INTO libraries (id, name) VALUES ($1, $2)`, libID, name); err != nil {
		t.Fatalf("inserting library %s: %v", libID, err)
	}
	if rootID != "" {
		if _, err := db.Exec(ctx, `
			INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match)
			VALUES ($1, $2, $3, $4, $4)`,
			rootID, libID, []byte(rootPath), rootPath,
		); err != nil {
			t.Fatalf("inserting library_root %s: %v", rootID, err)
		}
	}
}

func TestMigration0004_AppliesAndTablesExist(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	applied, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("querying applied migrations: %v", err)
	}
	if len(applied) < 4 || applied[3].Version != 4 || applied[3].Name != "0004_catalogue.sql" {
		t.Fatalf("expected 0004_catalogue.sql as fourth migration, got %+v", applied)
	}

	expectedTables := []string{
		"libraries",
		"library_roots",
		"series",
		"volumes",
		"series_memberships",
		"people",
		"credits",
		"entities",
		"volume_entities",
		"media_assets",
		"asset_digests",
		"reading_progress",
	}
	for _, table := range expectedTables {
		var exists bool
		err := db.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
			table,
		).Scan(&exists)
		if err != nil {
			t.Fatalf("checking table existence for %q: %v", table, err)
		}
		if !exists {
			t.Errorf("expected table %q to exist in database", table)
		}
	}

	// Re-running migrations must be an idempotent no-op.
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("re-running CoreFS failed: %v", err)
	}
}

func TestMigration0004_LibrariesAndRootsChecks(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// VIRTUAL library with NULL intake succeeds.
	if _, err := db.Exec(ctx, `INSERT INTO libraries (id, name, mode) VALUES ('lib-v', 'Virtual', 'VIRTUAL')`); err != nil {
		t.Fatalf("virtual library insert failed: %v", err)
	}

	// VIRTUAL library with intake path fails CHECK.
	if _, err := db.Exec(ctx, `
		INSERT INTO libraries (id, name, mode, intake_path_bytes, intake_path_display, intake_path_match)
		VALUES ('lib-v-bad', 'BadVirtual', 'VIRTUAL', $1, '/intake', '/intake')`, []byte("/intake")); !isCheckViolation(err) {
		t.Fatalf("expected check violation for VIRTUAL library with intake, got %v", err)
	}

	// MANAGED library without intake fails CHECK.
	if _, err := db.Exec(ctx, `INSERT INTO libraries (id, name, mode) VALUES ('lib-m-bad', 'BadManaged', 'MANAGED')`); !isCheckViolation(err) {
		t.Fatalf("expected check violation for MANAGED library without intake, got %v", err)
	}

	// MANAGED library with partial intake fields fails CHECK.
	if _, err := db.Exec(ctx, `
		INSERT INTO libraries (id, name, mode, intake_path_bytes)
		VALUES ('lib-m-partial', 'PartialManaged', 'MANAGED', $1)`, []byte("/intake")); !isCheckViolation(err) {
		t.Fatalf("expected check violation for partial intake fields, got %v", err)
	}

	// MANAGED library with all intake fields succeeds.
	if _, err := db.Exec(ctx, `
		INSERT INTO libraries (id, name, mode, intake_path_bytes, intake_path_display, intake_path_match)
		VALUES ('lib-m', 'Managed', 'MANAGED', $1, '/intake', '/intake')`, []byte("/intake")); err != nil {
		t.Fatalf("valid MANAGED library failed: %v", err)
	}

	// Duplicate path_bytes across library_roots fails UNIQUE.
	if _, err := db.Exec(ctx, `
		INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match)
		VALUES ('root-1', 'lib-v', $1, '/comics', '/comics')`, []byte("/comics")); err != nil {
		t.Fatalf("root-1 insert failed: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match)
		VALUES ('root-2', 'lib-m', $1, '/comics', '/comics')`, []byte("/comics")); !isUniqueViolation(err) {
		t.Fatalf("expected unique violation for duplicate root path_bytes, got %v", err)
	}

	// Root owning assets cannot be deleted directly (NO ACTION / RESTRICT).
	if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('vol-1', 'lib-v')`); err != nil {
		t.Fatalf("insert vol-1: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
		VALUES ('asset-1', 'vol-1', 'root-1', $1, 'book.cbz', 'book.cbz', 100, 'CBZ')`, []byte("book.cbz")); err != nil {
		t.Fatalf("insert asset-1: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM library_roots WHERE id = 'root-1'`); !isForeignKeyViolation(err) {
		t.Fatalf("expected foreign key violation deleting root with assets, got %v", err)
	}

	// Empty root can be deleted cleanly.
	if _, err := db.Exec(ctx, `
		INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match)
		VALUES ('root-empty', 'lib-v', $1, '/empty', '/empty')`, []byte("/empty")); err != nil {
		t.Fatalf("insert root-empty: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM library_roots WHERE id = 'root-empty'`); err != nil {
		t.Fatalf("deleting empty root failed: %v", err)
	}
}

func TestMigration0004_OnePrimaryMembership(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "lib-1", "Manga", "root-1", "/manga")
	if _, err := db.Exec(ctx, `
		INSERT INTO series (id, library_id, title, title_sort) VALUES
			('s1', 'lib-1', 'Main Series', 'Main Series'),
			('s2', 'lib-1', 'Sub Series', 'Sub Series'),
			('s3', 'lib-1', 'Universe', 'Universe')`); err != nil {
		t.Fatalf("insert series: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO volumes (id, library_id) VALUES ('v-standalone', 'lib-1'), ('v1', 'lib-1'), ('v2', 'lib-1')`); err != nil {
		t.Fatalf("insert volumes: %v", err)
	}

	// 1. Standalone volume (0 memberships) is valid; single secondary without primary fails at commit.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO series_memberships (volume_id, series_id, is_primary, kind)
		VALUES ('v-standalone', 's2', FALSE, 'SUB_SERIES')`); err != nil {
		t.Fatalf("insert secondary inside tx: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected commit with secondary-only membership to fail, but it succeeded")
	}

	// 2. Valid deferred edit: insert secondary first, then insert primary in same transaction.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO series_memberships (volume_id, series_id, is_primary, kind)
		VALUES ('v1', 's2', FALSE, 'SUB_SERIES')`); err != nil {
		t.Fatalf("insert secondary first: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO series_memberships (volume_id, series_id, is_primary, kind)
		VALUES ('v1', 's1', TRUE, 'SERIES')`); err != nil {
		t.Fatalf("insert primary second: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected deferred secondary-then-primary commit to succeed, got: %v", err)
	}

	// 3. Second primary for the same volume is rejected by idx_memberships_one_primary.
	if _, err := db.Exec(ctx, `
		INSERT INTO series_memberships (volume_id, series_id, is_primary, kind)
		VALUES ('v1', 's3', TRUE, 'UNIVERSE')`); !isUniqueViolation(err) {
		t.Fatalf("expected unique violation for second primary membership, got: %v", err)
	}

	// 4. Invalid update on series_memberships: demoting primary without promoting another fails at commit.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE series_memberships SET is_primary = FALSE WHERE volume_id = 'v1' AND series_id = 's1'`); err != nil {
		t.Fatalf("demote primary in tx: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected commit demoting sole primary while secondary exists to fail")
	}

	// 5. Valid deferred edit: demote s1 and promote s2 in the same transaction.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE series_memberships SET is_primary = FALSE WHERE volume_id = 'v1' AND series_id = 's1'`); err != nil {
		t.Fatalf("demote s1: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE series_memberships SET is_primary = TRUE WHERE volume_id = 'v1' AND series_id = 's2'`); err != nil {
		t.Fatalf("promote s2: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected deferred primary swap to succeed, got: %v", err)
	}

	// 6. Invalid delete from series_memberships side: deleting primary s2 while secondary s1 remains fails at commit.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'v1' AND series_id = 's2'`); err != nil {
		t.Fatalf("delete primary membership: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected commit deleting primary while secondary remains to fail")
	}

	// 7. Invalid delete from series side: deleting primary series s2 cascades delete of (v1, s2),
	// leaving secondary (v1, s1) without a primary -> must fail at commit.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM series WHERE id = 's2'`); err != nil {
		t.Fatalf("delete primary series s2: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected commit deleting primary series while volume retains secondary membership to fail")
	}

	// 8. Valid delete from series side when it is the volume's ONLY membership (volume becomes standalone).
	if _, err := db.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('v2', 's3', TRUE)`); err != nil {
		t.Fatalf("insert (v2, s3): %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM series WHERE id = 's3'`); err != nil {
		t.Fatalf("expected deleting sole series s3 for v2 to succeed, got: %v", err)
	}

	// 9. Valid deferred edit: deleting all memberships makes v1 standalone; deleting volume cascades cleanly.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'v1' AND series_id = 's2'`); err != nil {
		t.Fatalf("delete s2: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'v1' AND series_id = 's1'`); err != nil {
		t.Fatalf("delete s1: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected removing all memberships to make volume standalone succeed, got: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('v1', 's1', TRUE), ('v1', 's2', FALSE)`); err != nil {
		t.Fatalf("re-insert memberships: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM volumes WHERE id = 'v1'`); err != nil {
		t.Fatalf("deleting volume with primary+secondary memberships failed: %v", err)
	}
}

func TestMigration0004_SameLibraryMembership(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "lib-a", "Lib A", "root-a", "/a")
	seedBasicLibrary(ctx, t, db, "lib-b", "Lib B", "root-b", "/b")

	if _, err := db.Exec(ctx, `
		INSERT INTO series (id, library_id, title, title_sort) VALUES
			('sa', 'lib-a', 'Series A', 'Series A'),
			('sb', 'lib-b', 'Series B', 'Series B')`); err != nil {
		t.Fatalf("insert series: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO volumes (id, library_id) VALUES ('va', 'lib-a'), ('vb', 'lib-b')`); err != nil {
		t.Fatalf("insert volumes: %v", err)
	}

	// Cross-library membership insert must fail at commit.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('va', 'sb', TRUE)`); err != nil {
		t.Fatalf("insert cross-lib membership: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected cross-library membership insert to fail on commit")
	}

	// Same-library membership succeeds.
	if _, err := db.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('va', 'sa', TRUE)`); err != nil {
		t.Fatalf("insert same-library membership: %v", err)
	}

	// Invalid update from volumes side: moving va to lib-b while linked to sa (lib-a) fails.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE volumes SET library_id = 'lib-b' WHERE id = 'va'`); err != nil {
		t.Fatalf("update volume library: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected moving volume to lib-b while member of lib-a series to fail")
	}

	// Invalid update from series side: moving sa to lib-b while va is in lib-a fails.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE series SET library_id = 'lib-b' WHERE id = 'sa'`); err != nil {
		t.Fatalf("update series library: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected moving series to lib-b while containing lib-a volume to fail")
	}

	// Valid deferred edit: move both va and sa to lib-b in the same transaction.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE volumes SET library_id = 'lib-b' WHERE id = 'va'`); err != nil {
		t.Fatalf("update va: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE series SET library_id = 'lib-b' WHERE id = 'sa'`); err != nil {
		t.Fatalf("update sa: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected deferred move of both volume and series to succeed, got: %v", err)
	}

	// Valid deferred edit: move va back to lib-a AND reassign its membership to another lib-a series.
	if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ('sa2', 'lib-a', 'Series A2', 'Series A2')`); err != nil {
		t.Fatalf("insert sa2: %v", err)
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE volumes SET library_id = 'lib-a' WHERE id = 'va'`); err != nil {
		t.Fatalf("update va back to lib-a: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE series_memberships SET series_id = 'sa2' WHERE volume_id = 'va' AND series_id = 'sa'`); err != nil {
		t.Fatalf("reassign membership to sa2: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected deferred volume+membership move to succeed, got: %v", err)
	}
}

func TestMigration0004_SeriesManualCoverLinks(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "lib-1", "Manga", "root-1", "/manga")
	if _, err := db.Exec(ctx, `
		INSERT INTO series (id, library_id, title, title_sort) VALUES
			('s1', 'lib-1', 'S1', 'S1'),
			('s2', 'lib-1', 'S2', 'S2')`); err != nil {
		t.Fatalf("insert series: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO volumes (id, library_id) VALUES ('v1', 'lib-1'), ('v2', 'lib-1'), ('v-other', 'lib-1')`); err != nil {
		t.Fatalf("insert volumes: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO series_memberships (volume_id, series_id, is_primary)
		VALUES ('v1', 's1', TRUE), ('v-other', 's2', TRUE)`); err != nil {
		t.Fatalf("insert memberships: %v", err)
	}

	// Setting both cover_volume_id and cover_image_digest fails CHECK.
	if _, err := db.Exec(ctx, `
		UPDATE series SET cover_volume_id = 'v1', cover_image_digest = 'deadbeef' WHERE id = 's1'`); !isCheckViolation(err) {
		t.Fatalf("expected check violation when setting both cover_volume_id and cover_image_digest, got: %v", err)
	}

	// Setting cover_volume_id to a non-member volume fails at commit.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE series SET cover_volume_id = 'v-other' WHERE id = 's1'`); err != nil {
		t.Fatalf("update cover_volume_id: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected setting cover_volume_id to non-member volume to fail")
	}

	// Valid deferred edit: insert new series with cover_volume_id before adding membership in same tx.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO series (id, library_id, title, title_sort, cover_policy, cover_volume_id)
		VALUES ('s-new', 'lib-1', 'S New', 'S New', 'MANUAL', 'v2')`); err != nil {
		t.Fatalf("insert s-new with cover_volume_id: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO series_memberships (volume_id, series_id, is_primary)
		VALUES ('v2', 's-new', TRUE)`); err != nil {
		t.Fatalf("insert membership for s-new: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected deferred cover_volume_id + membership insert to succeed, got: %v", err)
	}

	// Invalid delete from series_memberships side: removing membership while s-new.cover_volume_id = v2 fails.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'v2' AND series_id = 's-new'`); err != nil {
		t.Fatalf("delete membership: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected deleting cover volume's membership to fail at commit")
	}

	// Invalid update from series_memberships side: moving membership to s2 while s-new.cover_volume_id = v2 fails.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE series_memberships SET series_id = 's2' WHERE volume_id = 'v2' AND series_id = 's-new'`); err != nil {
		t.Fatalf("move membership: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected moving cover volume's membership to another series to fail at commit")
	}

	// Valid deferred edit: clear cover_volume_id and delete membership in same tx.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'v2' AND series_id = 's-new'`); err != nil {
		t.Fatalf("delete membership: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE series SET cover_volume_id = NULL WHERE id = 's-new'`); err != nil {
		t.Fatalf("clear cover_volume_id: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected clearing cover_volume_id and deleting membership in same tx to succeed, got: %v", err)
	}

	// Deleting the referenced cover volume cascades ON DELETE SET NULL cleanly.
	if _, err := db.Exec(ctx, `UPDATE series SET cover_policy = 'MANUAL', cover_volume_id = 'v1' WHERE id = 's1'`); err != nil {
		t.Fatalf("set s1 cover_volume_id=v1: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM volumes WHERE id = 'v1'`); err != nil {
		t.Fatalf("deleting cover volume v1 failed: %v", err)
	}
	var coverVol *string
	if err := db.QueryRow(ctx, `SELECT cover_volume_id FROM series WHERE id = 's1'`).Scan(&coverVol); err != nil {
		t.Fatalf("querying s1 cover_volume_id: %v", err)
	}
	if coverVol != nil {
		t.Fatalf("expected cover_volume_id to be NULL after volume delete, got %q", *coverVol)
	}
}

func TestMigration0004_PreferredAssetOwnership(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "lib-1", "Manga", "root-1", "/manga")
	if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('v1', 'lib-1'), ('v2', 'lib-1')`); err != nil {
		t.Fatalf("insert volumes: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
		VALUES
			('a1', 'v1', 'root-1', $1, 'v1.cbz', 'v1.cbz', 100, 'CBZ'),
			('a2', 'v2', 'root-1', $2, 'v2.cbz', 'v2.cbz', 200, 'CBZ')`,
		[]byte("v1.cbz"), []byte("v2.cbz")); err != nil {
		t.Fatalf("insert assets: %v", err)
	}

	// Setting preferred_asset_id to an asset belonging to another volume fails at commit.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE volumes SET preferred_asset_id = 'a2' WHERE id = 'v1'`); err != nil {
		t.Fatalf("update preferred_asset_id: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected setting preferred_asset_id to another volume's asset to fail")
	}

	// Valid deferred edit: insert new volume with preferred_asset_id before inserting the asset.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO volumes (id, library_id, preferred_asset_id) VALUES ('v3', 'lib-1', 'a3')`); err != nil {
		t.Fatalf("insert v3 with deferred preferred_asset_id: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
		VALUES ('a3', 'v3', 'root-1', $1, 'v3.cbz', 'v3.cbz', 300, 'CBZ')`, []byte("v3.cbz")); err != nil {
		t.Fatalf("insert a3: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected deferred volume+preferred asset insert to succeed, got: %v", err)
	}

	// Invalid update from media_assets side: reassigning a3.volume_id away from v3 fails at commit.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE media_assets SET volume_id = 'v2' WHERE id = 'a3'`); err != nil {
		t.Fatalf("update asset volume_id: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected reassigning preferred asset to another volume to fail")
	}

	// Valid deferred edit: reassign a3.volume_id AND clear v3.preferred_asset_id in same tx.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE media_assets SET volume_id = 'v2' WHERE id = 'a3'`); err != nil {
		t.Fatalf("update a3 volume_id: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE volumes SET preferred_asset_id = NULL WHERE id = 'v3'`); err != nil {
		t.Fatalf("clear v3 preferred_asset_id: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected deferred asset move + clear preferred_asset_id to succeed, got: %v", err)
	}

	// Deleting preferred asset cascades ON DELETE SET NULL cleanly.
	if _, err := db.Exec(ctx, `UPDATE volumes SET preferred_asset_id = 'a1' WHERE id = 'v1'`); err != nil {
		t.Fatalf("set v1 preferred_asset_id=a1: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM media_assets WHERE id = 'a1'`); err != nil {
		t.Fatalf("deleting preferred asset a1 failed: %v", err)
	}
	var pref *string
	if err := db.QueryRow(ctx, `SELECT preferred_asset_id FROM volumes WHERE id = 'v1'`).Scan(&pref); err != nil {
		t.Fatalf("querying v1 preferred_asset_id: %v", err)
	}
	if pref != nil {
		t.Fatalf("expected preferred_asset_id to be NULL after asset delete, got %q", *pref)
	}
}

func TestMigration0004_AssetRootOwnership(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "lib-a", "Lib A", "root-a", "/a")
	seedBasicLibrary(ctx, t, db, "lib-b", "Lib B", "root-b", "/b")

	if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('va', 'lib-a'), ('vb', 'lib-b')`); err != nil {
		t.Fatalf("insert volumes: %v", err)
	}

	// Insert asset linking lib-a volume with lib-b root must fail at commit.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
		VALUES ('a-bad', 'va', 'root-b', $1, 'bad.cbz', 'bad.cbz', 100, 'CBZ')`, []byte("bad.cbz")); err != nil {
		t.Fatalf("insert bad asset: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected cross-library asset/root insert to fail at commit")
	}

	// Valid asset in lib-a.
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
		VALUES ('aa', 'va', 'root-a', $1, 'ok.cbz', 'ok.cbz', 100, 'CBZ')`, []byte("ok.cbz")); err != nil {
		t.Fatalf("insert valid asset: %v", err)
	}

	// Invalid update from media_assets side: moving root to root-b while va is in lib-a fails.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE media_assets SET library_root_id = 'root-b' WHERE id = 'aa'`); err != nil {
		t.Fatalf("update asset root: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected updating asset root to another library to fail")
	}

	// Invalid update from volumes side: moving va to lib-b while aa is on root-a fails.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE volumes SET library_id = 'lib-b' WHERE id = 'va'`); err != nil {
		t.Fatalf("update volume library: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected updating volume library while asset sits on root-a to fail")
	}

	// Invalid update from library_roots side: moving root-a to lib-b while va is in lib-a fails.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE library_roots SET library_id = 'lib-b' WHERE id = 'root-a'`); err != nil {
		t.Fatalf("update root library: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected updating root library while asset volume is in lib-a to fail")
	}

	// Valid deferred edit: move both va and root-a to lib-b in the same transaction.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE volumes SET library_id = 'lib-b' WHERE id = 'va'`); err != nil {
		t.Fatalf("update va: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE library_roots SET library_id = 'lib-b' WHERE id = 'root-a'`); err != nil {
		t.Fatalf("update root-a: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("expected deferred move of both volume and root to lib-b to succeed, got: %v", err)
	}
}

func TestMigration0004_ChapterRangePairing(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "lib-1", "Manga", "", "")

	// Valid cases: all NULL, start < end, start = end.
	validRows := []struct {
		id     string
		start  any
		end    any
		source any
	}{
		{"v-none", nil, nil, nil},
		{"v-range", 291.0, 300.0, "MANUAL"},
		{"v-single", 10.5, 10.5, "ARCHIVE"},
	}
	for _, tc := range validRows {
		if _, err := db.Exec(ctx, `
			INSERT INTO volumes (id, library_id, chapter_start, chapter_end, chapter_range_source)
			VALUES ($1, 'lib-1', $2, $3, $4)`, tc.id, tc.start, tc.end, tc.source); err != nil {
			t.Fatalf("valid volume %s rejected: %v", tc.id, err)
		}
	}

	// Invalid cases: partial fields or start > end.
	invalidRows := []struct {
		id     string
		start  any
		end    any
		source any
	}{
		{"bad-no-end", 1.0, nil, "MANUAL"},
		{"bad-no-start", nil, 10.0, "MANUAL"},
		{"bad-no-source", 1.0, 10.0, nil},
		{"bad-only-source", nil, nil, "SCRAPER"},
		{"bad-order", 15.0, 10.0, "MANUAL"},
	}
	for _, tc := range invalidRows {
		_, err := db.Exec(ctx, `
			INSERT INTO volumes (id, library_id, chapter_start, chapter_end, chapter_range_source)
			VALUES ($1, 'lib-1', $2, $3, $4)`, tc.id, tc.start, tc.end, tc.source)
		if !isCheckViolation(err) {
			t.Fatalf("invalid chapter range %s: expected check violation, got %v", tc.id, err)
		}
	}
}

func TestMigration0004_RetiredOnlyIfMissing(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "lib-1", "Manga", "root-1", "/manga")
	if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('v1', 'lib-1')`); err != nil {
		t.Fatalf("insert volume: %v", err)
	}

	// Available (NULL, NULL) succeeds.
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format, missing_since, retired_at)
		VALUES ('a-ok', 'v1', 'root-1', $1, 'ok.cbz', 'ok.cbz', 100, 'CBZ', NULL, NULL)`, []byte("ok.cbz")); err != nil {
		t.Fatalf("available asset rejected: %v", err)
	}

	// Missing within grace (NOT NULL, NULL) succeeds.
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format, missing_since, retired_at)
		VALUES ('a-miss', 'v1', 'root-1', $1, 'miss.cbz', 'miss.cbz', 100, 'CBZ', now(), NULL)`, []byte("miss.cbz")); err != nil {
		t.Fatalf("missing asset rejected: %v", err)
	}

	// Retired after grace (NOT NULL, NOT NULL) succeeds.
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format, missing_since, retired_at)
		VALUES ('a-ret', 'v1', 'root-1', $1, 'ret.cbz', 'ret.cbz', 100, 'CBZ', now() - interval '8 days', now())`, []byte("ret.cbz")); err != nil {
		t.Fatalf("retired asset rejected: %v", err)
	}

	// Retired without missing (NULL, NOT NULL) fails CHECK.
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format, missing_since, retired_at)
		VALUES ('a-bad', 'v1', 'root-1', $1, 'bad.cbz', 'bad.cbz', 100, 'CBZ', NULL, now())`, []byte("bad.cbz")); !isCheckViolation(err) {
		t.Fatalf("expected check violation for retired_at without missing_since, got: %v", err)
	}

	// Clearing missing_since without clearing retired_at fails CHECK.
	if _, err := db.Exec(ctx, `UPDATE media_assets SET missing_since = NULL WHERE id = 'a-ret'`); !isCheckViolation(err) {
		t.Fatalf("expected check violation when clearing missing_since while retired_at is set, got: %v", err)
	}

	// Clearing both restores the asset cleanly.
	if _, err := db.Exec(ctx, `UPDATE media_assets SET missing_since = NULL, retired_at = NULL WHERE id = 'a-ret'`); err != nil {
		t.Fatalf("clearing both missing_since and retired_at failed: %v", err)
	}
}

func TestMigration0004_CreditsAndDigestsConstraints(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "lib-1", "Manga", "root-1", "/manga")
	if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ('s1', 'lib-1', 'S1', 'S1')`); err != nil {
		t.Fatalf("insert series: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('v1', 'lib-1')`); err != nil {
		t.Fatalf("insert volume: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO people (id, name) VALUES ('p1', 'Oda')`); err != nil {
		t.Fatalf("insert person: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
		VALUES ('a1', 'v1', 'root-1', $1, 'a1.cbz', 'a1.cbz', 100, 'CBZ')`, []byte("a1.cbz")); err != nil {
		t.Fatalf("insert asset: %v", err)
	}

	// credits: both NULL or both non-NULL fail CHECK.
	if _, err := db.Exec(ctx, `INSERT INTO credits (id, person_id, role) VALUES ('c-none', 'p1', 'WRITER')`); !isCheckViolation(err) {
		t.Fatalf("expected check violation when both series_id and volume_id are NULL, got: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO credits (id, person_id, series_id, volume_id, role) VALUES ('c-both', 'p1', 's1', 'v1', 'WRITER')`); !isCheckViolation(err) {
		t.Fatalf("expected check violation when both series_id and volume_id are set, got: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO credits (id, person_id, series_id, role) VALUES ('c1', 'p1', 's1', 'WRITER')`); err != nil {
		t.Fatalf("valid series credit failed: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO credits (id, person_id, series_id, role) VALUES ('c2', 'p1', 's1', 'WRITER')`); !isUniqueViolation(err) {
		t.Fatalf("expected unique violation on duplicate series credit, got: %v", err)
	}

	// asset_digests: multiple non-current allowed, at most one current per (asset_id, kind).
	if _, err := db.Exec(ctx, `
		INSERT INTO asset_digests (asset_id, kind, value, is_current, reason) VALUES
			('a1', 'XXH3_64_PARTIAL', 'old1', FALSE, 'SCAN'),
			('a1', 'XXH3_64_PARTIAL', 'old2', FALSE, 'WRITE_BACK'),
			('a1', 'XXH3_64_PARTIAL', 'cur1', TRUE, 'WRITE_BACK')`); err != nil {
		t.Fatalf("valid asset_digests history + current failed: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO asset_digests (asset_id, kind, value, is_current, reason)
		VALUES ('a1', 'XXH3_64_PARTIAL', 'cur2', TRUE, 'SCAN')`); !isUniqueViolation(err) {
		t.Fatalf("expected unique violation for second current digest, got: %v", err)
	}
}

func TestMigration0004_ConcurrentCrossTableRaces(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "lib-a", "Lib A", "root-a", "/a")
	seedBasicLibrary(ctx, t, db, "lib-b", "Lib B", "root-b", "/b")

	// Race 1: Concurrent INSERT into series_memberships (va in lib-a, sa in lib-a) vs UPDATE volumes SET library_id = 'lib-b'.
	t.Run("membership_vs_volume_library", func(t *testing.T) {
		if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ('r1-s', 'lib-a', 'R1', 'R1')`); err != nil {
			t.Fatalf("insert series: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('r1-v', 'lib-a')`); err != nil {
			t.Fatalf("insert volume: %v", err)
		}

		tx1, tx2 := beginPair(ctx, t, db)
		defer rollbackQuiet(ctx, tx1)
		defer rollbackQuiet(ctx, tx2)

		if _, err := tx1.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('r1-v', 'r1-s', TRUE)`); err != nil {
			t.Fatalf("tx1 insert membership: %v", err)
		}
		if _, err := tx2.Exec(ctx, `UPDATE volumes SET library_id = 'lib-b' WHERE id = 'r1-v'`); err != nil {
			t.Fatalf("tx2 update volume library: %v", err)
		}

		err1, err2 := commitPairConcurrently(ctx, tx1, tx2)
		if err1 == nil && err2 == nil {
			t.Fatal("expected at least one concurrent transaction to fail, but both committed")
		}

		var mismatchCount int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM series_memberships m
			JOIN volumes v ON v.id = m.volume_id
			JOIN series s ON s.id = m.series_id
			WHERE m.volume_id = 'r1-v' AND v.library_id <> s.library_id`).Scan(&mismatchCount); err != nil {
			t.Fatalf("querying mismatch: %v", err)
		}
		if mismatchCount != 0 {
			t.Fatalf("found %d cross-library memberships after concurrent race", mismatchCount)
		}
	})

	// Race 2: Concurrent INSERT of secondary membership vs DELETE of sole primary membership.
	t.Run("one_primary_membership_race", func(t *testing.T) {
		if _, err := db.Exec(ctx, `
			INSERT INTO series (id, library_id, title, title_sort) VALUES
				('r2-s1', 'lib-a', 'R2 S1', 'R2 S1'),
				('r2-s2', 'lib-a', 'R2 S2', 'R2 S2')`); err != nil {
			t.Fatalf("insert series: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('r2-v', 'lib-a')`); err != nil {
			t.Fatalf("insert volume: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('r2-v', 'r2-s1', TRUE)`); err != nil {
			t.Fatalf("insert initial primary: %v", err)
		}

		tx1, tx2 := beginPair(ctx, t, db)
		defer rollbackQuiet(ctx, tx1)
		defer rollbackQuiet(ctx, tx2)

		if _, err := tx1.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary, kind) VALUES ('r2-v', 'r2-s2', FALSE, 'SUB_SERIES')`); err != nil {
			t.Fatalf("tx1 insert secondary: %v", err)
		}
		if _, err := tx2.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'r2-v' AND series_id = 'r2-s1'`); err != nil {
			t.Fatalf("tx2 delete primary: %v", err)
		}

		err1, err2 := commitPairConcurrently(ctx, tx1, tx2)
		if err1 == nil && err2 == nil {
			t.Fatal("expected at least one concurrent transaction to fail, but both committed")
		}

		var total, primary int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*), COUNT(*) FILTER (WHERE is_primary)
			FROM series_memberships
			WHERE volume_id = 'r2-v'`).Scan(&total, &primary); err != nil {
			t.Fatalf("querying memberships: %v", err)
		}
		if total > 0 && primary != 1 {
			t.Fatalf("invalid primary invariant after concurrent race: total=%d primary=%d", total, primary)
		}
	})

	// Race 3: Concurrent UPDATE series.cover_volume_id vs DELETE series_memberships.
	t.Run("series_cover_link_race", func(t *testing.T) {
		if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ('r3-s', 'lib-a', 'R3', 'R3')`); err != nil {
			t.Fatalf("insert series: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('r3-v', 'lib-a')`); err != nil {
			t.Fatalf("insert volume: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('r3-v', 'r3-s', TRUE)`); err != nil {
			t.Fatalf("insert membership: %v", err)
		}

		tx1, tx2 := beginPair(ctx, t, db)
		defer rollbackQuiet(ctx, tx1)
		defer rollbackQuiet(ctx, tx2)

		if _, err := tx1.Exec(ctx, `UPDATE series SET cover_policy = 'MANUAL', cover_volume_id = 'r3-v' WHERE id = 'r3-s'`); err != nil {
			t.Fatalf("tx1 set cover_volume_id: %v", err)
		}
		if _, err := tx2.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'r3-v' AND series_id = 'r3-s'`); err != nil {
			t.Fatalf("tx2 delete membership: %v", err)
		}

		err1, err2 := commitPairConcurrently(ctx, tx1, tx2)
		if err1 == nil && err2 == nil {
			t.Fatal("expected at least one concurrent transaction to fail, but both committed")
		}

		var orphaned int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM series s
			WHERE s.id = 'r3-s'
			  AND s.cover_volume_id IS NOT NULL
			  AND NOT EXISTS (
			      SELECT 1 FROM series_memberships m
			      WHERE m.series_id = s.id AND m.volume_id = s.cover_volume_id
			  )`).Scan(&orphaned); err != nil {
			t.Fatalf("querying orphaned cover_volume_id: %v", err)
		}
		if orphaned != 0 {
			t.Fatalf("found orphaned cover_volume_id after concurrent race")
		}
	})

	// Race 4: Concurrent UPDATE volumes.preferred_asset_id vs UPDATE media_assets.volume_id.
	t.Run("preferred_asset_ownership_race", func(t *testing.T) {
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('r4-v1', 'lib-a'), ('r4-v2', 'lib-a')`); err != nil {
			t.Fatalf("insert volumes: %v", err)
		}
		if _, err := db.Exec(ctx, `
			INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
			VALUES ('r4-a1', 'r4-v1', 'root-a', $1, 'r4.cbz', 'r4.cbz', 100, 'CBZ')`, []byte("r4.cbz")); err != nil {
			t.Fatalf("insert asset: %v", err)
		}

		tx1, tx2 := beginPair(ctx, t, db)
		defer rollbackQuiet(ctx, tx1)
		defer rollbackQuiet(ctx, tx2)

		if _, err := tx1.Exec(ctx, `UPDATE volumes SET preferred_asset_id = 'r4-a1' WHERE id = 'r4-v1'`); err != nil {
			t.Fatalf("tx1 set preferred_asset_id: %v", err)
		}
		if _, err := tx2.Exec(ctx, `UPDATE media_assets SET volume_id = 'r4-v2' WHERE id = 'r4-a1'`); err != nil {
			t.Fatalf("tx2 move asset: %v", err)
		}

		err1, err2 := commitPairConcurrently(ctx, tx1, tx2)
		if err1 == nil && err2 == nil {
			t.Fatal("expected at least one concurrent transaction to fail, but both committed")
		}

		var mismatched int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM volumes v
			JOIN media_assets a ON a.id = v.preferred_asset_id
			WHERE v.id = 'r4-v1' AND a.volume_id <> v.id`).Scan(&mismatched); err != nil {
			t.Fatalf("querying preferred_asset mismatch: %v", err)
		}
		if mismatched != 0 {
			t.Fatalf("found mismatched preferred_asset_id after concurrent race")
		}
	})

	// Race 5: Concurrent INSERT into media_assets vs UPDATE library_roots.library_id.
	t.Run("asset_vs_root_library_race", func(t *testing.T) {
		if _, err := db.Exec(ctx, `
			INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match)
			VALUES ('r5-root', 'lib-a', $1, '/r5', '/r5')`, []byte("/r5")); err != nil {
			t.Fatalf("insert root: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('r5-v', 'lib-a')`); err != nil {
			t.Fatalf("insert volume: %v", err)
		}

		tx1, tx2 := beginPair(ctx, t, db)
		defer rollbackQuiet(ctx, tx1)
		defer rollbackQuiet(ctx, tx2)

		if _, err := tx1.Exec(ctx, `
			INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
			VALUES ('r5-a', 'r5-v', 'r5-root', $1, 'r5.cbz', 'r5.cbz', 100, 'CBZ')`, []byte("r5.cbz")); err != nil {
			t.Fatalf("tx1 insert asset: %v", err)
		}
		if _, err := tx2.Exec(ctx, `UPDATE library_roots SET library_id = 'lib-b' WHERE id = 'r5-root'`); err != nil {
			t.Fatalf("tx2 update root library: %v", err)
		}

		err1, err2 := commitPairConcurrently(ctx, tx1, tx2)
		if err1 == nil && err2 == nil {
			t.Fatal("expected at least one concurrent transaction to fail, but both committed")
		}

		var mismatched int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM media_assets a
			JOIN volumes v ON v.id = a.volume_id
			JOIN library_roots r ON r.id = a.library_root_id
			WHERE a.id = 'r5-a' AND v.library_id <> r.library_id`).Scan(&mismatched); err != nil {
			t.Fatalf("querying asset/root mismatch: %v", err)
		}
		if mismatched != 0 {
			t.Fatalf("found mismatched asset/root library after concurrent race")
		}
	})

	// Race 6: Immediate trigger check followed by concurrent update before commit.
	// Verifies FOR SHARE locks held by SET CONSTRAINTS ALL IMMEDIATE block conflicting updates
	// until commit, so the second transaction sees the committed row and aborts.
	t.Run("immediate_check_blocks_concurrent_library_move", func(t *testing.T) {
		if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ('r6-s', 'lib-a', 'R6', 'R6')`); err != nil {
			t.Fatalf("insert series: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('r6-v', 'lib-a')`); err != nil {
			t.Fatalf("insert volume: %v", err)
		}

		tx1, tx2 := beginPair(ctx, t, db)
		defer rollbackQuiet(ctx, tx1)
		defer rollbackQuiet(ctx, tx2)

		if _, err := tx1.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('r6-v', 'r6-s', TRUE)`); err != nil {
			t.Fatalf("tx1 insert membership: %v", err)
		}
		if _, err := tx1.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); err != nil {
			t.Fatalf("tx1 set constraints immediate: %v", err)
		}

		updateDone := make(chan error, 1)
		go func() {
			_, err := tx2.Exec(ctx, `UPDATE series SET library_id = 'lib-b' WHERE id = 'r6-s'`)
			if err != nil {
				updateDone <- err
				return
			}
			updateDone <- tx2.Commit(ctx)
		}()

		// Give tx2 a moment to block on tx1's FOR SHARE lock on r6-s.
		select {
		case err := <-updateDone:
			t.Fatalf("tx2 should have blocked on tx1's FOR SHARE lock, finished early with: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		if err := tx1.Commit(ctx); err != nil {
			t.Fatalf("tx1 commit failed: %v", err)
		}
		if err := <-updateDone; err == nil {
			t.Fatal("expected tx2 to fail after tx1 committed membership in lib-a")
		}
	})
}

func TestMigration0004_RepeatableReadIsolationRaces(t *testing.T) {
	t.Parallel()
	db := applyCatalogueMigrations(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedBasicLibrary(ctx, t, db, "iso-lib-a", "Iso Lib A", "iso-root-a", "/iso-a")
	seedBasicLibrary(ctx, t, db, "iso-lib-b", "Iso Lib B", "iso-root-b", "/iso-b")

	for _, iso := range []struct {
		name  string
		level pgx.TxIsoLevel
	}{
		{name: "read_committed", level: pgx.ReadCommitted},
		{name: "repeatable_read", level: pgx.RepeatableRead},
	} {
		t.Run("primary_membership_simultaneous/"+iso.name, func(t *testing.T) {
			s1 := "pm-sim-s1-" + iso.name
			s2 := "pm-sim-s2-" + iso.name
			v := "pm-sim-v-" + iso.name

			if _, err := db.Exec(ctx, `
				INSERT INTO series (id, library_id, title, title_sort) VALUES
					($1, 'iso-lib-a', 'S1', 'S1'),
					($2, 'iso-lib-a', 'S2', 'S2')`, s1, s2); err != nil {
				t.Fatalf("insert series: %v", err)
			}
			if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ($1, 'iso-lib-a')`, v); err != nil {
				t.Fatalf("insert volume: %v", err)
			}
			if _, err := db.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ($1, $2, TRUE)`, v, s1); err != nil {
				t.Fatalf("insert initial primary: %v", err)
			}

			tx1, tx2 := beginPairWithIso(ctx, t, db, iso.level)
			defer rollbackQuiet(ctx, tx1)
			defer rollbackQuiet(ctx, tx2)

			if _, err := tx1.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary, kind) VALUES ($1, $2, FALSE, 'SUB_SERIES')`, v, s2); err != nil {
				t.Fatalf("tx1 insert secondary: %v", err)
			}
			if _, err := tx2.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = $1 AND series_id = $2`, v, s1); err != nil {
				t.Fatalf("tx2 delete primary: %v", err)
			}

			err1, err2 := commitPairConcurrently(ctx, tx1, tx2)
			if err1 == nil && err2 == nil {
				t.Fatal("expected at least one concurrent transaction to fail, but both committed")
			}

			var total, primary int
			if err := db.QueryRow(ctx, `
				SELECT COUNT(*), COUNT(*) FILTER (WHERE is_primary)
				FROM series_memberships
				WHERE volume_id = $1`, v).Scan(&total, &primary); err != nil {
				t.Fatalf("querying memberships: %v", err)
			}
			if (total > 0 && primary != 1) || total > 2 {
				t.Fatalf("invalid final primary invariant (%s): total=%d primary=%d", iso.name, total, primary)
			}
			if err1 == nil && err2 != nil && (total != 2 || primary != 1) {
				t.Fatalf("expected {s1 primary, s2 secondary} when tx1 wins (%s), got total=%d primary=%d", iso.name, total, primary)
			}
			if err2 == nil && err1 != nil && (total != 0 || primary != 0) {
				t.Fatalf("expected empty memberships when tx2 wins (%s), got total=%d primary=%d", iso.name, total, primary)
			}
		})

		t.Run("same_library_volume_side_simultaneous/"+iso.name, func(t *testing.T) {
			s := "slv-sim-s-" + iso.name
			v := "slv-sim-v-" + iso.name

			if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ($1, 'iso-lib-a', 'SLV', 'SLV')`, s); err != nil {
				t.Fatalf("insert series: %v", err)
			}
			if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ($1, 'iso-lib-a')`, v); err != nil {
				t.Fatalf("insert volume: %v", err)
			}

			tx1, tx2 := beginPairWithIso(ctx, t, db, iso.level)
			defer rollbackQuiet(ctx, tx1)
			defer rollbackQuiet(ctx, tx2)

			if _, err := tx1.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ($1, $2, TRUE)`, v, s); err != nil {
				t.Fatalf("tx1 insert membership: %v", err)
			}
			if _, err := tx2.Exec(ctx, `UPDATE volumes SET library_id = 'iso-lib-b' WHERE id = $1`, v); err != nil {
				t.Fatalf("tx2 update volume library: %v", err)
			}

			err1, err2 := commitPairConcurrently(ctx, tx1, tx2)
			if err1 == nil && err2 == nil {
				t.Fatal("expected at least one concurrent transaction to fail, but both committed")
			}

			assertSameLibraryMembershipState(ctx, t, db, v, s)
		})

		t.Run("same_library_series_side_simultaneous/"+iso.name, func(t *testing.T) {
			s := "sls-sim-s-" + iso.name
			v := "sls-sim-v-" + iso.name

			if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ($1, 'iso-lib-a', 'SLS', 'SLS')`, s); err != nil {
				t.Fatalf("insert series: %v", err)
			}
			if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ($1, 'iso-lib-a')`, v); err != nil {
				t.Fatalf("insert volume: %v", err)
			}

			tx1, tx2 := beginPairWithIso(ctx, t, db, iso.level)
			defer rollbackQuiet(ctx, tx1)
			defer rollbackQuiet(ctx, tx2)

			if _, err := tx1.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ($1, $2, TRUE)`, v, s); err != nil {
				t.Fatalf("tx1 insert membership: %v", err)
			}
			if _, err := tx2.Exec(ctx, `UPDATE series SET library_id = 'iso-lib-b' WHERE id = $1`, s); err != nil {
				t.Fatalf("tx2 update series library: %v", err)
			}

			err1, err2 := commitPairConcurrently(ctx, tx1, tx2)
			if err1 == nil && err2 == nil {
				t.Fatal("expected at least one concurrent transaction to fail, but both committed")
			}

			assertSameLibraryMembershipState(ctx, t, db, v, s)
		})
	}

	// Staggered REPEATABLE READ snapshot-first tests:
	// tx1 pins a snapshot before tx2 commits a new child/link, then tx1 attempts
	// the conflicting parent/primary mutation. Without parent tuple-version bumps,
	// tx1's stale snapshot would miss tx2's committed child row.
	t.Run("primary_membership_snapshot_delete_after_secondary_commit", func(t *testing.T) {
		if _, err := db.Exec(ctx, `
			INSERT INTO series (id, library_id, title, title_sort) VALUES
				('pm-snap1-s1', 'iso-lib-a', 'PM1 S1', 'PM1 S1'),
				('pm-snap1-s2', 'iso-lib-a', 'PM1 S2', 'PM1 S2')`); err != nil {
			t.Fatalf("insert series: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('pm-snap1-v', 'iso-lib-a')`); err != nil {
			t.Fatalf("insert volume: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('pm-snap1-v', 'pm-snap1-s1', TRUE)`); err != nil {
			t.Fatalf("insert primary: %v", err)
		}

		tx1 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx1)
		pinRepeatableReadSnapshot(ctx, t, tx1)

		tx2 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx2)
		if _, err := tx2.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary, kind) VALUES ('pm-snap1-v', 'pm-snap1-s2', FALSE, 'SUB_SERIES')`); err != nil {
			t.Fatalf("tx2 insert secondary: %v", err)
		}
		if err := tx2.Commit(ctx); err != nil {
			t.Fatalf("tx2 commit failed: %v", err)
		}

		if _, err := tx1.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'pm-snap1-v' AND series_id = 'pm-snap1-s1'`); err != nil {
			t.Fatalf("tx1 delete primary: %v", err)
		}
		if err := tx1.Commit(ctx); err == nil {
			t.Fatal("expected tx1 (RepeatableRead stale snapshot) deleting primary after secondary committed to fail")
		}

		var total, primary int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*), COUNT(*) FILTER (WHERE is_primary)
			FROM series_memberships WHERE volume_id = 'pm-snap1-v'`).Scan(&total, &primary); err != nil {
			t.Fatalf("query memberships: %v", err)
		}
		if total != 2 || primary != 1 {
			t.Fatalf("expected 2 memberships with 1 primary preserved, got total=%d primary=%d", total, primary)
		}
	})

	t.Run("primary_membership_snapshot_insert_secondary_after_delete_commit", func(t *testing.T) {
		if _, err := db.Exec(ctx, `
			INSERT INTO series (id, library_id, title, title_sort) VALUES
				('pm-snap2-s1', 'iso-lib-a', 'PM2 S1', 'PM2 S1'),
				('pm-snap2-s2', 'iso-lib-a', 'PM2 S2', 'PM2 S2')`); err != nil {
			t.Fatalf("insert series: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('pm-snap2-v', 'iso-lib-a')`); err != nil {
			t.Fatalf("insert volume: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('pm-snap2-v', 'pm-snap2-s1', TRUE)`); err != nil {
			t.Fatalf("insert primary: %v", err)
		}

		tx1 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx1)
		pinRepeatableReadSnapshot(ctx, t, tx1)

		tx2 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx2)
		if _, err := tx2.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'pm-snap2-v' AND series_id = 'pm-snap2-s1'`); err != nil {
			t.Fatalf("tx2 delete primary: %v", err)
		}
		if err := tx2.Commit(ctx); err != nil {
			t.Fatalf("tx2 commit failed: %v", err)
		}

		if _, err := tx1.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary, kind) VALUES ('pm-snap2-v', 'pm-snap2-s2', FALSE, 'SUB_SERIES')`); err != nil {
			t.Fatalf("tx1 insert secondary: %v", err)
		}
		if err := tx1.Commit(ctx); err == nil {
			t.Fatal("expected tx1 (RepeatableRead stale snapshot) inserting secondary without primary after tx2 deleted primary to fail")
		}

		var total, primary int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*), COUNT(*) FILTER (WHERE is_primary)
			FROM series_memberships WHERE volume_id = 'pm-snap2-v'`).Scan(&total, &primary); err != nil {
			t.Fatalf("query memberships: %v", err)
		}
		if total != 0 || primary != 0 {
			t.Fatalf("expected 0 memberships after tx2 delete and tx1 failure, got total=%d primary=%d", total, primary)
		}
	})

	t.Run("same_library_volume_snapshot_move_after_membership_commit", func(t *testing.T) {
		if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ('sl-vsnap-s', 'iso-lib-a', 'SLV', 'SLV')`); err != nil {
			t.Fatalf("insert series: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('sl-vsnap-v', 'iso-lib-a')`); err != nil {
			t.Fatalf("insert volume: %v", err)
		}

		tx1 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx1)
		pinRepeatableReadSnapshot(ctx, t, tx1)

		tx2 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx2)
		if _, err := tx2.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('sl-vsnap-v', 'sl-vsnap-s', TRUE)`); err != nil {
			t.Fatalf("tx2 insert membership: %v", err)
		}
		if err := tx2.Commit(ctx); err != nil {
			t.Fatalf("tx2 commit failed: %v", err)
		}

		if _, err := tx1.Exec(ctx, `UPDATE volumes SET library_id = 'iso-lib-b' WHERE id = 'sl-vsnap-v'`); err == nil {
			if cerr := tx1.Commit(ctx); cerr == nil {
				t.Fatal("expected tx1 moving volume library after tx2 committed membership to fail")
			}
		}

		assertSameLibraryMembershipState(ctx, t, db, "sl-vsnap-v", "sl-vsnap-s")
		var count int
		var volLib string
		if err := db.QueryRow(ctx, `SELECT (SELECT COUNT(*) FROM series_memberships WHERE volume_id = 'sl-vsnap-v'), library_id FROM volumes WHERE id = 'sl-vsnap-v'`).Scan(&count, &volLib); err != nil {
			t.Fatalf("query final volume state: %v", err)
		}
		if count != 1 || volLib != "iso-lib-a" {
			t.Fatalf("expected committed membership preserved in iso-lib-a, got count=%d volLib=%s", count, volLib)
		}
	})

	t.Run("same_library_series_snapshot_move_after_membership_commit", func(t *testing.T) {
		if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ('sl-ssnap-s', 'iso-lib-a', 'SLS', 'SLS')`); err != nil {
			t.Fatalf("insert series: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ('sl-ssnap-v', 'iso-lib-a')`); err != nil {
			t.Fatalf("insert volume: %v", err)
		}

		tx1 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx1)
		pinRepeatableReadSnapshot(ctx, t, tx1)

		tx2 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx2)
		if _, err := tx2.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('sl-ssnap-v', 'sl-ssnap-s', TRUE)`); err != nil {
			t.Fatalf("tx2 insert membership: %v", err)
		}
		if err := tx2.Commit(ctx); err != nil {
			t.Fatalf("tx2 commit failed: %v", err)
		}

		if _, err := tx1.Exec(ctx, `UPDATE series SET library_id = 'iso-lib-b' WHERE id = 'sl-ssnap-s'`); err == nil {
			if cerr := tx1.Commit(ctx); cerr == nil {
				t.Fatal("expected tx1 moving series library after tx2 committed membership to fail")
			}
		}

		assertSameLibraryMembershipState(ctx, t, db, "sl-ssnap-v", "sl-ssnap-s")
		var count int
		var serLib string
		if err := db.QueryRow(ctx, `SELECT (SELECT COUNT(*) FROM series_memberships WHERE series_id = 'sl-ssnap-s'), library_id FROM series WHERE id = 'sl-ssnap-s'`).Scan(&count, &serLib); err != nil {
			t.Fatalf("query final series state: %v", err)
		}
		if count != 1 || serLib != "iso-lib-a" {
			t.Fatalf("expected committed membership preserved in iso-lib-a, got count=%d serLib=%s", count, serLib)
		}
	})

	t.Run("asset_ownership_snapshot_move_volume_or_root_after_asset_commit", func(t *testing.T) {
		if _, err := db.Exec(ctx, `
			INSERT INTO volumes (id, library_id) VALUES
				('ao-snap-v1', 'iso-lib-a'),
				('ao-snap-v2', 'iso-lib-a')`); err != nil {
			t.Fatalf("insert volumes: %v", err)
		}
		if _, err := db.Exec(ctx, `
			INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match)
			VALUES ('ao-snap-root', 'iso-lib-a', $1, '/ao', '/ao')`, []byte("/ao")); err != nil {
			t.Fatalf("insert root: %v", err)
		}

		// Volume library move after asset commit on stale snapshot.
		tx1 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx1)
		pinRepeatableReadSnapshot(ctx, t, tx1)

		tx2 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx2)
		if _, err := tx2.Exec(ctx, `
			INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, format, file_size)
			VALUES ('ao-snap-a1', 'ao-snap-v1', 'iso-root-a', $1, 'a', 'a', 'CBZ', 10)`, []byte("a")); err != nil {
			t.Fatalf("tx2 insert asset 1: %v", err)
		}
		if err := tx2.Commit(ctx); err != nil {
			t.Fatalf("tx2 commit failed: %v", err)
		}

		if _, err := tx1.Exec(ctx, `UPDATE volumes SET library_id = 'iso-lib-b' WHERE id = 'ao-snap-v1'`); err == nil {
			if cerr := tx1.Commit(ctx); cerr == nil {
				t.Fatal("expected tx1 moving volume library after asset committed to fail")
			}
		}

		// Root library move after asset commit on stale snapshot.
		tx3 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx3)
		pinRepeatableReadSnapshot(ctx, t, tx3)

		tx4 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx4)
		if _, err := tx4.Exec(ctx, `
			INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, format, file_size)
			VALUES ('ao-snap-a2', 'ao-snap-v2', 'ao-snap-root', $1, 'b', 'b', 'CBZ', 10)`, []byte("b")); err != nil {
			t.Fatalf("tx4 insert asset 2: %v", err)
		}
		if err := tx4.Commit(ctx); err != nil {
			t.Fatalf("tx4 commit failed: %v", err)
		}

		if _, err := tx3.Exec(ctx, `UPDATE library_roots SET library_id = 'iso-lib-b' WHERE id = 'ao-snap-root'`); err == nil {
			if cerr := tx3.Commit(ctx); cerr == nil {
				t.Fatal("expected tx3 moving root library after asset committed to fail")
			}
		}

		var mismatchCount, assetCount int
		var v1Lib, rootLib string
		if err := db.QueryRow(ctx, `
			SELECT
				(SELECT COUNT(*)
				 FROM media_assets a
				 JOIN volumes v ON v.id = a.volume_id
				 JOIN library_roots r ON r.id = a.library_root_id
				 WHERE a.id IN ('ao-snap-a1', 'ao-snap-a2') AND v.library_id <> r.library_id),
				(SELECT COUNT(*) FROM media_assets WHERE id IN ('ao-snap-a1', 'ao-snap-a2')),
				(SELECT library_id FROM volumes WHERE id = 'ao-snap-v1'),
				(SELECT library_id FROM library_roots WHERE id = 'ao-snap-root')`).Scan(&mismatchCount, &assetCount, &v1Lib, &rootLib); err != nil {
			t.Fatalf("query asset mismatch: %v", err)
		}
		if mismatchCount != 0 || assetCount != 2 || v1Lib != "iso-lib-a" || rootLib != "iso-lib-a" {
			t.Fatalf("unexpected asset/root ownership state: mismatch=%d assets=%d v1Lib=%s rootLib=%s", mismatchCount, assetCount, v1Lib, rootLib)
		}
	})

	t.Run("series_cover_and_preferred_asset_snapshot_races", func(t *testing.T) {
		if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ('cp-snap-s', 'iso-lib-a', 'CP', 'CP')`); err != nil {
			t.Fatalf("insert series: %v", err)
		}
		if _, err := db.Exec(ctx, `
			INSERT INTO volumes (id, library_id) VALUES
				('cp-snap-v1', 'iso-lib-a'),
				('cp-snap-v2', 'iso-lib-a')`); err != nil {
			t.Fatalf("insert volumes: %v", err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO series_memberships (volume_id, series_id, is_primary) VALUES ('cp-snap-v1', 'cp-snap-s', TRUE)`); err != nil {
			t.Fatalf("insert membership: %v", err)
		}
		if _, err := db.Exec(ctx, `
			INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, format, file_size)
			VALUES ('cp-snap-a1', 'cp-snap-v1', 'iso-root-a', $1, 'c', 'c', 'CBZ', 10)`, []byte("c")); err != nil {
			t.Fatalf("insert asset: %v", err)
		}

		// Delete membership on stale snapshot after cover_volume_id was set.
		tx1 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx1)
		pinRepeatableReadSnapshot(ctx, t, tx1)

		tx2 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx2)
		if _, err := tx2.Exec(ctx, `UPDATE series SET cover_policy = 'MANUAL', cover_volume_id = 'cp-snap-v1' WHERE id = 'cp-snap-s'`); err != nil {
			t.Fatalf("tx2 set cover: %v", err)
		}
		if err := tx2.Commit(ctx); err != nil {
			t.Fatalf("tx2 commit failed: %v", err)
		}

		if _, err := tx1.Exec(ctx, `DELETE FROM series_memberships WHERE volume_id = 'cp-snap-v1' AND series_id = 'cp-snap-s'`); err != nil {
			t.Fatalf("tx1 delete membership: %v", err)
		}
		if err := tx1.Commit(ctx); err == nil {
			t.Fatal("expected tx1 deleting cover membership on stale snapshot to fail")
		}

		// Reparent asset on stale snapshot after preferred_asset_id was set.
		tx3 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx3)
		pinRepeatableReadSnapshot(ctx, t, tx3)

		tx4 := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, tx4)
		if _, err := tx4.Exec(ctx, `UPDATE volumes SET preferred_asset_id = 'cp-snap-a1' WHERE id = 'cp-snap-v1'`); err != nil {
			t.Fatalf("tx4 set preferred asset: %v", err)
		}
		if err := tx4.Commit(ctx); err != nil {
			t.Fatalf("tx4 commit failed: %v", err)
		}

		if _, err := tx3.Exec(ctx, `UPDATE media_assets SET volume_id = 'cp-snap-v2' WHERE id = 'cp-snap-a1'`); err != nil {
			t.Fatalf("tx3 reparent asset: %v", err)
		}
		if err := tx3.Commit(ctx); err == nil {
			t.Fatal("expected tx3 reparenting preferred asset on stale snapshot to fail")
		}

		var coverBroken, prefBroken int
		if err := db.QueryRow(ctx, `
			SELECT
				(SELECT COUNT(*) FROM series s WHERE s.id = 'cp-snap-s' AND s.cover_volume_id IS NOT NULL
					AND NOT EXISTS (SELECT 1 FROM series_memberships m WHERE m.series_id = s.id AND m.volume_id = s.cover_volume_id)),
				(SELECT COUNT(*) FROM volumes v JOIN media_assets a ON a.id = v.preferred_asset_id
					WHERE v.id = 'cp-snap-v1' AND a.volume_id <> v.id)`).Scan(&coverBroken, &prefBroken); err != nil {
			t.Fatalf("query final cover/preferred state: %v", err)
		}
		if coverBroken != 0 || prefBroken != 0 {
			t.Fatalf("broken final cover/preferred state: coverBroken=%d prefBroken=%d", coverBroken, prefBroken)
		}
	})
}

func assertSameLibraryMembershipState(ctx context.Context, t *testing.T, db migrations.DB, volumeID, seriesID string) {
	t.Helper()
	var mismatchCount, membershipCount int
	var volLib, serLib string
	if err := db.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM series_memberships m
			 JOIN volumes v ON v.id = m.volume_id
			 JOIN series s ON s.id = m.series_id
			 WHERE m.volume_id = $1 AND v.library_id <> s.library_id),
			(SELECT COUNT(*) FROM series_memberships WHERE volume_id = $1 AND series_id = $2),
			(SELECT library_id FROM volumes WHERE id = $1),
			(SELECT library_id FROM series WHERE id = $2)`, volumeID, seriesID).Scan(&mismatchCount, &membershipCount, &volLib, &serLib); err != nil {
		t.Fatalf("querying same-library final state: %v", err)
	}
	if mismatchCount != 0 {
		t.Fatalf("found %d cross-library memberships", mismatchCount)
	}
	if membershipCount == 1 && volLib != serLib {
		t.Fatalf("membership exists while volume library (%s) != series library (%s)", volLib, serLib)
	}
	if membershipCount > 1 {
		t.Fatalf("unexpected membership count %d", membershipCount)
	}
}

func beginTxWithIso(ctx context.Context, t *testing.T, db migrations.DB, iso pgx.TxIsoLevel) pgx.Tx {
	t.Helper()
	tdb, ok := db.(*testdb.TestDB)
	if !ok {
		t.Fatalf("expected *testdb.TestDB, got %T", db)
	}
	tx, err := tdb.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: iso})
	if err != nil {
		t.Fatalf("begin tx (%s): %v", iso, err)
	}
	return tx
}

func beginPairWithIso(ctx context.Context, t *testing.T, db migrations.DB, iso pgx.TxIsoLevel) (pgx.Tx, pgx.Tx) {
	t.Helper()
	tx1 := beginTxWithIso(ctx, t, db, iso)
	tdb := db.(*testdb.TestDB)
	tx2, err := tdb.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: iso})
	if err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("begin tx2 (%s): %v", iso, err)
	}
	return tx1, tx2
}

func pinRepeatableReadSnapshot(ctx context.Context, t *testing.T, tx pgx.Tx) {
	t.Helper()
	var n int
	if err := tx.QueryRow(ctx, `SELECT 1`).Scan(&n); err != nil {
		t.Fatalf("pinning repeatable read snapshot: %v", err)
	}
}

func beginPair(ctx context.Context, t *testing.T, db migrations.DB) (pgx.Tx, pgx.Tx) {
	t.Helper()
	tx1, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	tx2, err := db.Begin(ctx)
	if err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("begin tx2: %v", err)
	}
	return tx1, tx2
}

func rollbackQuiet(ctx context.Context, tx pgx.Tx) {
	if tx != nil {
		_ = tx.Rollback(ctx)
	}
}

func commitPairConcurrently(ctx context.Context, tx1, tx2 pgx.Tx) (error, error) {
	var err1, err2 error
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start
		err1 = tx1.Commit(ctx)
	}()
	go func() {
		defer wg.Done()
		<-start
		err2 = tx2.Commit(ctx)
	}()

	close(start)
	wg.Wait()
	return err1, err2
}
