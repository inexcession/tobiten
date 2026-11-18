package migrations_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestUpgradeSeeds_MigrateToHead(t *testing.T) {
	seeds, err := migrations.ListSeeds(migrations.SeedsFS, "testdata/seeds")
	if err != nil {
		t.Fatalf("failed to list seeds: %v", err)
	}
	if len(seeds) == 0 {
		t.Fatal("expected at least one seed fixture in testdata/seeds, found none")
	}

	for _, seedPath := range seeds {
		seedName := filepath.Base(seedPath)
		seedVersion, err := strconv.Atoi(strings.TrimSuffix(seedName, ".sql"))
		if err != nil {
			t.Fatalf("parsing seed version from %q: %v", seedName, err)
		}
		t.Run(seedName, func(t *testing.T) {
			db := testdb.New(t)
			ctx := context.Background()

			// 1. Restore the seed fixture into a fresh database.
			if err := migrations.RestoreSeedFS(ctx, db, migrations.SeedsFS, seedPath); err != nil {
				t.Fatalf("failed to restore seed %q: %v", seedPath, err)
			}

			// 2. Verify all seeded tables at the seed's starting version.
			verifySeededData(ctx, t, db, seedVersion)
			if seedVersion >= 7 {
				verifySeededCatalogue0007Data(ctx, t, db, false)
			}

			// 3. Migrate the seeded database up to HEAD schema.
			if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
				t.Fatalf("migrating seeded database %q to head failed: %v", seedName, err)
			}

			// 4. Verify all core migrations are marked applied and checksums match.
			applied, err := migrations.Applied(ctx, db)
			if err != nil {
				t.Fatalf("querying applied migrations after upgrade: %v", err)
			}
			coreMigrations, err := migrations.Collect(migrations.CoreFS)
			if err != nil {
				t.Fatalf("collecting core migrations: %v", err)
			}
			if len(applied) != len(coreMigrations) {
				t.Fatalf("expected %d applied migrations at head, got %d", len(coreMigrations), len(applied))
			}
			for i, core := range coreMigrations {
				app := applied[i]
				if app.Version != core.Version {
					t.Errorf("migration %d: expected version %d, got %d", i, core.Version, app.Version)
				}
				if app.Checksum != core.Checksum {
					t.Errorf("migration %d (%s): checksum mismatch: recorded %s, expected %s",
						core.Version, core.Name, app.Checksum, core.Checksum)
				}
			}

			// 5. Verify seeded data survived the migration to head intact.
			verifySeededData(ctx, t, db, len(coreMigrations))
			if seedVersion >= 7 {
				verifySeededCatalogue0007Data(ctx, t, db, true)
			}

			// 6. Verify catalogue tables from 0004_catalogue.sql exist after upgrading from 0001.
			for _, table := range []string{
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
			} {
				var exists bool
				if err := db.QueryRow(ctx,
					"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
					table,
				).Scan(&exists); err != nil {
					t.Fatalf("checking table %q after upgrade: %v", table, err)
				}
				if !exists {
					t.Errorf("expected table %q to exist after migrating %s to head", table, seedName)
				}
			}

			// 7. Verify 0005_labels.sql built-in SERVER label seed and personal built-in backfill.
			var labelScope string
			var builtIn bool
			if err := db.QueryRow(ctx,
				`SELECT scope, built_in FROM labels WHERE id = $1 AND name = $2`,
				domain.LabelMarkedForDeletionID, domain.LabelMarkedForDeletion,
			).Scan(&labelScope, &builtIn); err != nil {
				t.Fatalf("verify marked_for_deletion seed after upgrade: %v", err)
			}
			if labelScope != string(domain.LabelScopeServer) || !builtIn {
				t.Fatalf("unexpected marked_for_deletion state after upgrade: scope=%q built_in=%v", labelScope, builtIn)
			}

			// Verify all seeded users (empty admin/bob and partial alice) have all 3 personal built-ins.
			seededUsers := []string{
				"019318a0-0000-7000-8000-000000000010", // admin
				"019318a0-0000-7000-8000-000000000011", // alice
				"019318a0-0000-7000-8000-000000000012", // bob
			}
			for _, uid := range seededUsers {
				for _, name := range domain.BuiltInUserLabelNames() {
					var (
						id         string
						lblBuiltIn bool
					)
					if err := db.QueryRow(ctx,
						`SELECT id, built_in FROM labels WHERE scope = 'USER' AND owner_id = $1 AND name = $2`,
						uid, name,
					).Scan(&id, &lblBuiltIn); err != nil {
						t.Fatalf("missing backfilled personal label %q for user %s: %v", name, uid, err)
					}
					if id == "" || !lblBuiltIn {
						t.Fatalf("unexpected backfilled personal label %q for user %s: id=%q built_in=%v", name, uid, id, lblBuiltIn)
					}
				}
			}

			// Verify alice's existing personal label IDs and item_labels assignment survived upgrade.
			aliceExisting := []struct {
				id   string
				name string
			}{
				{id: "019318a0-0000-7000-8000-000000000033", name: domain.LabelFavorite},
				{id: "019318a0-0000-7000-8000-000000000034", name: domain.LabelWantToRead},
			}
			for _, want := range aliceExisting {
				var gotName string
				if err := db.QueryRow(ctx,
					`SELECT name FROM labels WHERE id = $1 AND scope = 'USER' AND owner_id = '019318a0-0000-7000-8000-000000000011'`,
					want.id,
				).Scan(&gotName); err != nil || gotName != want.name {
					t.Fatalf("alice pre-existing label %s (%s) was not preserved: name=%q err=%v", want.id, want.name, gotName, err)
				}
			}
			var assignedCount int
			if err := db.QueryRow(ctx,
				`SELECT COUNT(*) FROM item_labels WHERE label_id = '019318a0-0000-7000-8000-000000000033' AND item_type = 'VOLUME' AND item_id = '019318a0-0000-7000-8000-000000000092'`,
			).Scan(&assignedCount); err != nil || assignedCount != 1 {
				t.Fatalf("alice pre-existing item_labels assignment did not survive upgrade: count=%d err=%v", assignedCount, err)
			}

			// Verify 0006_domain_event_sequence.sql initialized last_sequence from seeded domain_events.
			var lastEventSeq int64
			if err := db.QueryRow(ctx,
				`SELECT last_sequence FROM domain_event_sequence WHERE id = TRUE`,
			).Scan(&lastEventSeq); err != nil {
				t.Fatalf("verify domain_event_sequence after upgrade: %v", err)
			}
			if lastEventSeq != 1 {
				t.Fatalf("expected domain_event_sequence.last_sequence = 1 after upgrade, got %d", lastEventSeq)
			}

			// Verify 0007_job_cancel_permissions.sql granted jobs:cancel_own to all built-in roles
			// and jobs:cancel_all to Admin only (decision I-10).
			for _, roleID := range []string{
				domain.RoleAdminID,
				domain.RoleCuratorID,
				domain.RoleReaderID,
				domain.RoleRestrictedReaderID,
			} {
				var hasCancelOwn, hasCancelAll bool
				if err := db.QueryRow(ctx,
					`SELECT
						EXISTS(SELECT 1 FROM role_permissions WHERE role_id = $1 AND permission = 'jobs:cancel_own'),
						EXISTS(SELECT 1 FROM role_permissions WHERE role_id = $1 AND permission = 'jobs:cancel_all')`,
					roleID,
				).Scan(&hasCancelOwn, &hasCancelAll); err != nil {
					t.Fatalf("verify job cancel permissions for role %s after upgrade: %v", roleID, err)
				}
				if !hasCancelOwn {
					t.Errorf("role %s missing jobs:cancel_own after upgrade", roleID)
				}
				wantCancelAll := roleID == domain.RoleAdminID
				if hasCancelAll != wantCancelAll {
					t.Errorf("role %s jobs:cancel_all = %v, want %v after upgrade", roleID, hasCancelAll, wantCancelAll)
				}
			}
		})
	}
}

func TestUpgradeSeeds_FailsOnBrokenMigration(t *testing.T) {
	db := testdb.New(t)
	ctx := context.Background()

	// 1. Restore the 0001 seed fixture.
	if err := migrations.RestoreSeedFS(ctx, db, migrations.SeedsFS, "testdata/seeds/0001.sql"); err != nil {
		t.Fatalf("failed to restore 0001 seed: %v", err)
	}

	// Verify sample rows exist
	var initialUsers int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&initialUsers); err != nil {
		t.Fatalf("failed to count users: %v", err)
	}
	if initialUsers != 3 {
		t.Fatalf("expected 3 initial users, got %d", initialUsers)
	}

	// 2. Prepare a migration list that appends a breaking migration above HEAD.
	// Attempting to add a NOT NULL column with no DEFAULT to 'users' MUST fail with
	// PostgreSQL SQLSTATE 23502 (not_null_violation) rather than version ordering rejection.
	coreMigrations, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("failed to collect core migrations: %v", err)
	}
	nextVersion := coreMigrations[len(coreMigrations)-1].Version + 1
	brokenName := fmt.Sprintf("%04d_broken_not_null.sql", nextVersion)

	breakingMigration := migrations.Migration{
		Version:  nextVersion,
		Name:     brokenName,
		SQL:      "ALTER TABLE users ADD COLUMN email TEXT NOT NULL;",
		Checksum: "sha256:breakingnotnull",
	}

	migrationPlan := make([]migrations.Migration, len(coreMigrations), len(coreMigrations)+1)
	copy(migrationPlan, coreMigrations)
	migrationPlan = append(migrationPlan, breakingMigration)

	// 3. Attempt to run the broken migration on the seeded database.
	err = migrations.Run(ctx, db, migrationPlan)
	if err == nil {
		t.Fatal("expected migration to fail on seeded data, but it succeeded")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, brokenName) {
		t.Errorf("expected error to mention broken migration name %q, got: %s", brokenName, errMsg)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23502" {
		t.Fatalf("expected PostgreSQL not_null_violation (23502), got: %v", err)
	}

	// 4. Verify transaction rollback:
	// - nextVersion must NOT be recorded in schema_migrations
	applied, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("failed to query applied migrations: %v", err)
	}
	if len(applied) != len(coreMigrations) {
		t.Errorf("expected %d applied migrations after rollback, got %d", len(coreMigrations), len(applied))
	}
	for _, app := range applied {
		if app.Version == nextVersion {
			t.Errorf("broken migration version %d was recorded in schema_migrations despite failure", nextVersion)
		}
	}

	// - The column 'email' must NOT exist in users
	var columnExists bool
	colCheckSQL := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'users' AND column_name = 'email'
		)`
	if err := db.QueryRow(ctx, colCheckSQL).Scan(&columnExists); err != nil {
		t.Fatalf("checking column existence: %v", err)
	}
	if columnExists {
		t.Errorf("column 'email' was added to users despite migration rollback")
	}

	// - Seeded data remains unharmed
	var postUsers int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&postUsers); err != nil {
		t.Fatalf("counting users after failure: %v", err)
	}
	if postUsers != initialUsers {
		t.Errorf("expected user count %d preserved after rollback, got %d", initialUsers, postUsers)
	}
}

func TestUpgradeSeeds_FailsOnIncompatibleConstraint(t *testing.T) {
	db := testdb.New(t)
	ctx := context.Background()

	// 1. Restore the 0001 seed fixture.
	if err := migrations.RestoreSeedFS(ctx, db, migrations.SeedsFS, "testdata/seeds/0001.sql"); err != nil {
		t.Fatalf("failed to restore 0001 seed: %v", err)
	}

	// 2. Prepare a migration above HEAD with an incompatible CHECK constraint.
	// system_settings has keys like 'events.retention_days' (length 21).
	// Adding CHECK (length(key) < 5) must fail with SQLSTATE 23514 (check_violation).
	coreMigrations, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("collecting core migrations: %v", err)
	}
	nextVersion := coreMigrations[len(coreMigrations)-1].Version + 1
	brokenName := fmt.Sprintf("%04d_broken_check_constraint.sql", nextVersion)

	breakingMigration := migrations.Migration{
		Version:  nextVersion,
		Name:     brokenName,
		SQL:      "ALTER TABLE system_settings ADD CONSTRAINT check_short_keys CHECK (length(key) < 5);",
		Checksum: "sha256:breakingcheck",
	}

	err = migrations.Run(ctx, db, append(coreMigrations, breakingMigration))
	if err == nil {
		t.Fatal("expected check constraint migration to fail on seeded rows, but succeeded")
	}
	if !strings.Contains(err.Error(), brokenName) {
		t.Errorf("expected error to mention broken migration name %q, got: %s", brokenName, err.Error())
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("expected PostgreSQL check_violation (23514), got: %v", err)
	}

	// Verify rollback
	applied, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("failed to query applied migrations: %v", err)
	}
	if len(applied) != len(coreMigrations) {
		t.Errorf("expected %d applied migrations after rollback, got %d", len(coreMigrations), len(applied))
	}
	for _, app := range applied {
		if app.Version == nextVersion {
			t.Errorf("broken migration version %d was recorded in schema_migrations despite failure", nextVersion)
		}
	}

	var constraintExists bool
	conCheckSQL := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.table_constraints
			WHERE constraint_name = 'check_short_keys'
		)`
	if err := db.QueryRow(ctx, conCheckSQL).Scan(&constraintExists); err != nil {
		t.Fatalf("checking constraint existence: %v", err)
	}
	if constraintExists {
		t.Errorf("constraint 'check_short_keys' exists despite migration failure")
	}
}

func TestUpgradeSeeds_ChecksumMatchesHead(t *testing.T) {
	// Verify that every seed fixture records exact checksums matching CoreFS.
	coreMigrations, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("collecting core migrations: %v", err)
	}
	if len(coreMigrations) == 0 {
		t.Fatal("expected core migrations to have at least version 1")
	}
	seeds, err := migrations.ListSeeds(migrations.SeedsFS, "testdata/seeds")
	if err != nil {
		t.Fatalf("listing seeds: %v", err)
	}
	for _, seedPath := range seeds {
		seedName := filepath.Base(seedPath)
		seedVersion, err := strconv.Atoi(strings.TrimSuffix(seedName, ".sql"))
		if err != nil {
			t.Fatalf("parsing seed version from %q: %v", seedName, err)
		}
		seedBytes, err := fs.ReadFile(migrations.SeedsFS, seedPath)
		if err != nil {
			t.Fatalf("reading seed %s: %v", seedName, err)
		}
		seedContent := string(seedBytes)
		for _, mig := range coreMigrations {
			if mig.Version > int64(seedVersion) {
				continue
			}
			expectedSnippet := fmt.Sprintf("(%d, '%s', '%s',", mig.Version, mig.Name, mig.Checksum)
			if !strings.Contains(seedContent, expectedSnippet) {
				t.Errorf("seed fixture %s missing matching migration %d (%s) checksum %s",
					seedName, mig.Version, mig.Name, mig.Checksum)
			}
		}
	}
}

func TestUpgradeSeeds_PsqlRestore(t *testing.T) {
	// If psql is available, verify that native psql restores the seed fixture cleanly.
	psqlPath := findPsql()
	if psqlPath == "" {
		t.Skip("psql binary not found on PATH or standard locations, skipping psql restore test")
	}

	db := testdb.New(t)
	ctx := context.Background()

	// Locate the on-disk file
	seedDiskPath := filepath.Join("testdata", "seeds", "0001.sql")
	if _, err := os.Stat(seedDiskPath); err != nil {
		t.Fatalf("seed file not found at %s: %v", seedDiskPath, err)
	}

	cmd := exec.Command(psqlPath, "-v", "ON_ERROR_STOP=1", db.URL, "-f", seedDiskPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("psql restore failed: %v\nOutput: %s", err, string(out))
	}

	// Verify sample data
	var userCount int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&userCount); err != nil {
		t.Fatalf("counting users after psql restore: %v", err)
	}
	if userCount != 3 {
		t.Errorf("expected 3 users after psql restore, got %d", userCount)
	}

	// Verify migrate to head works on psql-restored database
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("migrating psql-restored db to head failed: %v", err)
	}
}

func verifySeededData(ctx context.Context, t testing.TB, db migrations.DB, expectedMigrations int) {
	t.Helper()

	checkCount := func(table string, expected int) {
		t.Helper()
		var count int
		query := fmt.Sprintf("SELECT count(*) FROM %s", table)
		if err := db.QueryRow(ctx, query).Scan(&count); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		if count != expected {
			t.Errorf("table %s: expected %d rows, got %d", table, expected, count)
		}
	}

	checkCount("users", 3)
	checkCount("credentials", 4)
	checkCount("user_roles", 3)
	if expectedMigrations == 1 {
		checkCount("labels", 5)
	} else {
		// 0005_labels.sql retains the 5 seeded labels (preserving IDs/assignments) and
		// completes the 3 personal built-in USER labels for all 3 existing users (12 total).
		checkCount("labels", 12)
	}
	checkCount("item_labels", 3)
	checkCount("user_label_policies", 2)
	checkCount("domain_events", 2)
	checkCount("event_subscribers", 2)
	checkCount("jobs", 2)
	checkCount("system_settings", 3)
	checkCount("schema_migrations", expectedMigrations)

	// Roles: 4 built-in roles + 1 custom Junior Curator = 5
	var roleCount int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM roles").Scan(&roleCount); err != nil {
		t.Fatalf("counting roles: %v", err)
	}
	if roleCount < 5 {
		t.Errorf("expected at least 5 roles, got %d", roleCount)
	}
}

func verifySeededCatalogue0007Data(ctx context.Context, t testing.TB, db migrations.DB, postUpgrade bool) {
	t.Helper()

	checkCount := func(table string, expected int) {
		t.Helper()
		var count int
		query := fmt.Sprintf("SELECT count(*) FROM %s", table)
		if err := db.QueryRow(ctx, query).Scan(&count); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		if count != expected {
			t.Errorf("table %s: expected %d rows, got %d", table, expected, count)
		}
	}

	checkCount("libraries", 2)
	checkCount("library_roots", 2)
	checkCount("series", 2)
	checkCount("volumes", 2)
	checkCount("series_memberships", 2)
	checkCount("media_assets", 1)
	checkCount("asset_digests", 1)
	checkCount("people", 3)
	checkCount("credits", 4)
	checkCount("entities", 4)
	checkCount("volume_entities", 4)

	// Verify exact pre-existing people IDs, names, and duplicate-name preservation.
	type wantPerson struct {
		id         string
		name       string
		nameNative *string
	}
	odaNative := "尾田栄一郎"
	for _, want := range []wantPerson{
		{id: "019318a0-0000-7000-8000-000000000101", name: "Eiichiro Oda", nameNative: &odaNative},
		{id: "019318a0-0000-7000-8000-000000000102", name: "John Smith"},
		{id: "019318a0-0000-7000-8000-000000000103", name: "John Smith"},
	} {
		var (
			gotName   string
			gotNative *string
		)
		if err := db.QueryRow(ctx, `SELECT name, name_native FROM people WHERE id = $1`, want.id).Scan(&gotName, &gotNative); err != nil {
			t.Fatalf("missing seeded person %s: %v", want.id, err)
		}
		if gotName != want.name {
			t.Errorf("person %s name = %q, want %q", want.id, gotName, want.name)
		}
		if (gotNative == nil) != (want.nameNative == nil) || (gotNative != nil && *gotNative != *want.nameNative) {
			t.Errorf("person %s name_native = %v, want %v", want.id, gotNative, want.nameNative)
		}
	}

	// Verify exact pre-existing credit IDs and references survived.
	for _, want := range []struct {
		id       string
		personID string
		seriesID *string
		volumeID *string
		role     string
	}{
		{id: "019318a0-0000-7000-8000-000000000201", personID: "019318a0-0000-7000-8000-000000000101", seriesID: strPtr("019318a0-0000-7000-8000-000000000090"), role: "AUTHOR"},
		{id: "019318a0-0000-7000-8000-000000000202", personID: "019318a0-0000-7000-8000-000000000101", volumeID: strPtr("019318a0-0000-7000-8000-000000000092"), role: "WRITER"},
		{id: "019318a0-0000-7000-8000-000000000203", personID: "019318a0-0000-7000-8000-000000000102", volumeID: strPtr("019318a0-0000-7000-8000-000000000093"), role: "PENCILLER"},
		{id: "019318a0-0000-7000-8000-000000000204", personID: "019318a0-0000-7000-8000-000000000103", volumeID: strPtr("019318a0-0000-7000-8000-000000000093"), role: "INKER"},
	} {
		var (
			gotPerson string
			gotSeries *string
			gotVolume *string
			gotRole   string
		)
		if err := db.QueryRow(ctx,
			`SELECT person_id, series_id, volume_id, role FROM credits WHERE id = $1`,
			want.id,
		).Scan(&gotPerson, &gotSeries, &gotVolume, &gotRole); err != nil {
			t.Fatalf("missing seeded credit %s: %v", want.id, err)
		}
		if gotPerson != want.personID || gotRole != want.role ||
			!optStrEq(gotSeries, want.seriesID) || !optStrEq(gotVolume, want.volumeID) {
			t.Errorf("credit %s = (%s, %v, %v, %s), want (%s, %v, %v, %s)",
				want.id, gotPerson, gotSeries, gotVolume, gotRole,
				want.personID, want.seriesID, want.volumeID, want.role)
		}
	}

	// Verify exact pre-existing entity IDs and volume_entities links survived.
	for _, want := range []struct {
		id   string
		kind string
		name string
	}{
		{id: "019318a0-0000-7000-8000-000000000301", kind: "CHARACTER", name: "Monkey D. Luffy"},
		{id: "019318a0-0000-7000-8000-000000000302", kind: "TEAM", name: "Straw Hat Pirates"},
		{id: "019318a0-0000-7000-8000-000000000303", kind: "LOCATION", name: "Gotham"},
		{id: "019318a0-0000-7000-8000-000000000304", kind: "CHARACTER", name: "Gotham"},
	} {
		var gotKind, gotName string
		if err := db.QueryRow(ctx, `SELECT kind, name FROM entities WHERE id = $1`, want.id).Scan(&gotKind, &gotName); err != nil {
			t.Fatalf("missing seeded entity %s: %v", want.id, err)
		}
		if gotKind != want.kind || gotName != want.name {
			t.Errorf("entity %s = (%s, %q), want (%s, %q)", want.id, gotKind, gotName, want.kind, want.name)
		}
	}
	for _, want := range []struct {
		volID string
		entID string
	}{
		{volID: "019318a0-0000-7000-8000-000000000092", entID: "019318a0-0000-7000-8000-000000000301"},
		{volID: "019318a0-0000-7000-8000-000000000092", entID: "019318a0-0000-7000-8000-000000000302"},
		{volID: "019318a0-0000-7000-8000-000000000093", entID: "019318a0-0000-7000-8000-000000000303"},
		{volID: "019318a0-0000-7000-8000-000000000093", entID: "019318a0-0000-7000-8000-000000000304"},
	} {
		var exists bool
		if err := db.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM volume_entities WHERE volume_id = $1 AND entity_id = $2)`,
			want.volID, want.entID,
		).Scan(&exists); err != nil || !exists {
			t.Fatalf("missing volume_entities (%s, %s): exists=%v err=%v", want.volID, want.entID, exists, err)
		}
	}

	if !postUpgrade {
		return
	}

	// Post-upgrade (0008): pre-existing rows have import_key NULL and coexist with new keyed imports,
	// while duplicate keyed imports are rejected by idx_people_import_key and idx_entities_import_key.
	var nonNullPeopleKeys, nonNullEntityKeys int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM people WHERE import_key IS NOT NULL`).Scan(&nonNullPeopleKeys); err != nil {
		t.Fatalf("checking people import_key: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT count(*) FROM entities WHERE import_key IS NOT NULL`).Scan(&nonNullEntityKeys); err != nil {
		t.Fatalf("checking entities import_key: %v", err)
	}
	if nonNullPeopleKeys != 0 || nonNullEntityKeys != 0 {
		t.Errorf("expected pre-existing rows to retain NULL import_key, got people=%d entities=%d", nonNullPeopleKeys, nonNullEntityKeys)
	}

	if _, err := db.Exec(ctx, `INSERT INTO people (id, name, import_key) VALUES ('post-p1', 'John Smith', 'john smith')`); err != nil {
		t.Fatalf("inserting keyed person alongside pre-upgrade curated John Smith rows failed: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO people (id, name, import_key) VALUES ('post-p2', 'JOHN SMITH', 'john smith')`); !isUniqueViolation(err) {
		t.Fatalf("expected unique violation on duplicate people.import_key after upgrade, got: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO entities (id, kind, name, import_key) VALUES ('post-e1', 'LOCATION', 'Gotham', 'gotham')`); err != nil {
		t.Fatalf("inserting keyed LOCATION Gotham alongside pre-upgrade row failed: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO entities (id, kind, name, import_key) VALUES ('post-e2', 'CHARACTER', 'Gotham', 'gotham')`); err != nil {
		t.Fatalf("inserting keyed CHARACTER Gotham alongside LOCATION Gotham failed: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO entities (id, kind, name, import_key) VALUES ('post-e3', 'LOCATION', 'GOTHAM', 'gotham')`); !isUniqueViolation(err) {
		t.Fatalf("expected unique violation on duplicate entities(kind, import_key) after upgrade, got: %v", err)
	}
}

func strPtr(s string) *string { return &s }

func optStrEq(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func findPsql() string {
	candidates := []string{
		"/opt/local/lib/postgresql18/bin/psql",
		"/opt/homebrew/opt/postgresql@18/bin/psql",
		"/usr/local/opt/postgresql@18/bin/psql",
		"/usr/lib/postgresql/18/bin/psql",
	}
	if envBin := os.Getenv("TOBITEN_PG_BIN"); envBin != "" {
		candidates = append([]string{filepath.Join(envBin, "psql")}, candidates...)
	}
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return c
		}
	}
	if p, err := exec.LookPath("psql"); err == nil {
		return p
	}
	return ""
}
