package migrations_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
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
		t.Run(seedName, func(t *testing.T) {
			db := testdb.New(t)
			ctx := context.Background()

			// 1. Restore the seed fixture into a fresh database.
			if err := migrations.RestoreSeedFS(ctx, db, migrations.SeedsFS, seedPath); err != nil {
				t.Fatalf("failed to restore seed %q: %v", seedPath, err)
			}

			// 2. Verify all seeded M1A tables contain sample rows.
			verifySeededData(ctx, t, db, 1)

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

	// 2. Prepare a migration list that includes a breaking migration.
	// Migration 0002 attempts to add a NOT NULL column with no DEFAULT to 'users'.
	// Because 'users' already has 3 rows, this MUST fail with a NOT NULL constraint violation (SQLSTATE 23502).
	coreMigrations, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("failed to collect core migrations: %v", err)
	}

	breakingMigration := migrations.Migration{
		Version:  2,
		Name:     "0002_broken_not_null.sql",
		SQL:      "ALTER TABLE users ADD COLUMN email TEXT NOT NULL;",
		Checksum: "sha256:breaking0002notnull",
	}

	migrationPlan := append(coreMigrations, breakingMigration)

	// 3. Attempt to run the broken migration on the seeded database.
	err = migrations.Run(ctx, db, migrationPlan)
	if err == nil {
		t.Fatal("expected migration to fail on seeded data, but it succeeded")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "0002_broken_not_null.sql") {
		t.Errorf("expected error to mention broken migration name, got: %s", errMsg)
	}

	// 4. Verify transaction rollback:
	// - Version 2 must NOT be recorded in schema_migrations
	applied, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("failed to query applied migrations: %v", err)
	}
	for _, app := range applied {
		if app.Version == 2 {
			t.Errorf("broken migration version 2 was recorded in schema_migrations despite failure")
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

	// 2. Prepare a migration with an incompatible CHECK constraint.
	// system_settings has keys like 'events.retention_days' (length 21).
	// Adding CHECK (length(key) < 5) must fail because of existing rows.
	coreMigrations, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("collecting core migrations: %v", err)
	}

	breakingMigration := migrations.Migration{
		Version:  2,
		Name:     "0002_broken_check_constraint.sql",
		SQL:      "ALTER TABLE system_settings ADD CONSTRAINT check_short_keys CHECK (length(key) < 5);",
		Checksum: "sha256:breaking0002check",
	}

	err = migrations.Run(ctx, db, append(coreMigrations, breakingMigration))
	if err == nil {
		t.Fatal("expected check constraint migration to fail on seeded rows, but succeeded")
	}

	// Verify rollback
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

func TestUpgradeSeeds_ChecksumMatchesHead0001(t *testing.T) {
	// Verify that the seed fixture 0001.sql records the exact same checksum
	// as CoreFS migration 0001_init.sql.
	coreMigrations, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("collecting core migrations: %v", err)
	}
	if len(coreMigrations) == 0 {
		t.Fatal("expected core migrations to have at least version 1")
	}
	mig0001 := coreMigrations[0]

	seedBytes, err := fs.ReadFile(migrations.SeedsFS, "testdata/seeds/0001.sql")
	if err != nil {
		t.Fatalf("reading seed 0001.sql: %v", err)
	}

	// The seed fixture has:
	// INSERT INTO public.schema_migrations VALUES (1, '0001_init.sql', '<checksum>', ...)
	expectedSnippet := fmt.Sprintf("(1, '0001_init.sql', '%s',", mig0001.Checksum)
	if !strings.Contains(string(seedBytes), expectedSnippet) {
		t.Errorf("seed fixture 0001.sql does not match current 0001_init.sql checksum %s", mig0001.Checksum)
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
	checkCount("labels", 5)
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
