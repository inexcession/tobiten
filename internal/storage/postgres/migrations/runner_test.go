package migrations_test

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

//go:embed testdata/*
var testdataFS embed.FS

func subFS(t *testing.T, dir string) fs.FS {
	t.Helper()
	sub, err := fs.Sub(testdataFS, dir)
	if err != nil {
		t.Fatalf("failed to create sub filesystem for %s: %v", dir, err)
	}
	return sub
}

func TestCollect(t *testing.T) {
	t.Parallel()

	t.Run("valid single source", func(t *testing.T) {
		migs, err := migrations.Collect(subFS(t, "testdata/valid"))
		if err != nil {
			t.Fatalf("unexpected error collecting migrations: %v", err)
		}
		if len(migs) != 2 {
			t.Fatalf("expected 2 migrations, got %d", len(migs))
		}
		if migs[0].Version != 1 || migs[0].Name != "0001_create_test_users.sql" {
			t.Errorf("unexpected migration 0: %+v", migs[0])
		}
		if migs[1].Version != 2 || migs[1].Name != "0002_create_test_profiles.sql" {
			t.Errorf("unexpected migration 1: %+v", migs[1])
		}
		if migs[0].Checksum == "" || migs[1].Checksum == "" {
			t.Errorf("expected non-empty checksums, got %q, %q", migs[0].Checksum, migs[1].Checksum)
		}
	})

	t.Run("multiple sources merged and ordered", func(t *testing.T) {
		migs, err := migrations.Collect(
			subFS(t, "testdata/module"),
			subFS(t, "testdata/valid"),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(migs) != 3 {
			t.Fatalf("expected 3 migrations, got %d", len(migs))
		}
		if migs[0].Version != 1 || migs[1].Version != 2 || migs[2].Version != 3 {
			t.Fatalf("expected versions [1, 2, 3], got [%d, %d, %d]",
				migs[0].Version, migs[1].Version, migs[2].Version)
		}
	})

	t.Run("invalid filename rejected", func(t *testing.T) {
		_, err := migrations.Collect(subFS(t, "testdata/invalid_name"))
		if err == nil {
			t.Fatal("expected error on invalid filename, got nil")
		}
		if !errors.Is(err, migrations.ErrInvalidMigration) {
			t.Fatalf("expected ErrInvalidMigration, got %v", err)
		}
	})

	t.Run("duplicate versions rejected", func(t *testing.T) {
		// Passing the same source twice generates duplicate versions.
		_, err := migrations.Collect(
			subFS(t, "testdata/valid"),
			subFS(t, "testdata/valid"),
		)
		if err == nil {
			t.Fatal("expected error on duplicate versions, got nil")
		}
		if !errors.Is(err, migrations.ErrOutOfOrderMigration) {
			t.Fatalf("expected ErrOutOfOrderMigration, got %v", err)
		}
	})
}

func TestRun_EmptyDatabase(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := migrations.RunFS(ctx, db, subFS(t, "testdata/valid"))
	if err != nil {
		t.Fatalf("failed to run migrations on empty database: %v", err)
	}

	applied, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("failed to query applied migrations: %v", err)
	}
	if len(applied) != 2 {
		t.Fatalf("expected 2 applied migrations, got %d", len(applied))
	}
	if applied[0].Version != 1 || applied[0].Name != "0001_create_test_users.sql" {
		t.Errorf("unexpected applied[0]: %+v", applied[0])
	}
	if applied[1].Version != 2 || applied[1].Name != "0002_create_test_profiles.sql" {
		t.Errorf("unexpected applied[1]: %+v", applied[1])
	}

	// Verify created tables can be used.
	_, err = db.Exec(ctx, `INSERT INTO test_users (id, email) VALUES ('u1', 'test@example.com')`)
	if err != nil {
		t.Fatalf("failed to insert into created table test_users: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO test_profiles (user_id, bio) VALUES ('u1', 'Hello world')`)
	if err != nil {
		t.Fatalf("failed to insert into created table test_profiles: %v", err)
	}
}

func TestRun_ReRunIsNoOp(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	validFS := subFS(t, "testdata/valid")

	// First run applies migrations.
	if err := migrations.RunFS(ctx, db, validFS); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}

	appliedBefore, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("failed to read applied: %v", err)
	}

	// Second run must be a no-op.
	if err := migrations.RunFS(ctx, db, validFS); err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	appliedAfter, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("failed to read applied after second run: %v", err)
	}

	if len(appliedBefore) != len(appliedAfter) {
		t.Fatalf("applied count changed: before %d, after %d", len(appliedBefore), len(appliedAfter))
	}
	for i := range appliedBefore {
		if appliedBefore[i].Version != appliedAfter[i].Version ||
			appliedBefore[i].Checksum != appliedAfter[i].Checksum ||
			appliedBefore[i].AppliedAt != appliedAfter[i].AppliedAt {
			t.Errorf("migration row %d mutated during no-op rerun", i)
		}
	}
}

func TestRun_ModifiedFileRefused(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Apply original migrations.
	if err := migrations.RunFS(ctx, db, subFS(t, "testdata/valid")); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}

	// Attempt to run with modified 0001 file.
	err := migrations.RunFS(ctx, db, subFS(t, "testdata/modified"))
	if err == nil {
		t.Fatal("expected error on modified migration file, got nil")
	}
	if !errors.Is(err, migrations.ErrModifiedMigration) {
		t.Fatalf("expected ErrModifiedMigration, got %v", err)
	}
}

func TestRun_OutOfOrderRefused_NonAscending(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	outOfOrder := []migrations.Migration{
		{Version: 2, Name: "0002_second.sql", SQL: "SELECT 1;", Checksum: "sha256:2"},
		{Version: 1, Name: "0001_first.sql", SQL: "SELECT 1;", Checksum: "sha256:1"},
	}

	err := migrations.Run(ctx, db, outOfOrder)
	if err == nil {
		t.Fatal("expected error on non-ascending migrations, got nil")
	}
	if !errors.Is(err, migrations.ErrOutOfOrderMigration) {
		t.Fatalf("expected ErrOutOfOrderMigration, got %v", err)
	}
}

func TestRun_OutOfOrderRefused_DuplicateVersion(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	duplicates := []migrations.Migration{
		{Version: 1, Name: "0001_first.sql", SQL: "SELECT 1;", Checksum: "sha256:1a"},
		{Version: 1, Name: "0001_dup.sql", SQL: "SELECT 1;", Checksum: "sha256:1b"},
	}

	err := migrations.Run(ctx, db, duplicates)
	if err == nil {
		t.Fatal("expected error on duplicate versions, got nil")
	}
	if !errors.Is(err, migrations.ErrOutOfOrderMigration) {
		t.Fatalf("expected ErrOutOfOrderMigration, got %v", err)
	}
}

func TestRun_OutOfOrderRefused_EarlierVersionAfterLaterApplied(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Manually apply version 2 to database.
	onlySecond := []migrations.Migration{
		{Version: 2, Name: "0002_create_test_profiles.sql", SQL: "CREATE TABLE test_profiles (id TEXT PRIMARY KEY);", Checksum: "sha256:fake"},
	}
	if err := migrations.Run(ctx, db, onlySecond); err != nil {
		t.Fatalf("failed to apply version 2: %v", err)
	}

	// Now try to run with [1, 2]. Version 1 is unapplied but its version < version 2.
	both := []migrations.Migration{
		{Version: 1, Name: "0001_create_test_users.sql", SQL: "CREATE TABLE test_users (id TEXT PRIMARY KEY);", Checksum: "sha256:fake1"},
		{Version: 2, Name: "0002_create_test_profiles.sql", SQL: "CREATE TABLE test_profiles (id TEXT PRIMARY KEY);", Checksum: "sha256:fake"},
	}
	err := migrations.Run(ctx, db, both)
	if err == nil {
		t.Fatal("expected error on out-of-order migration, got nil")
	}
	if !errors.Is(err, migrations.ErrOutOfOrderMigration) {
		t.Fatalf("expected ErrOutOfOrderMigration, got %v", err)
	}
}

func TestRun_OutOfOrderRefused_GapInDatabase(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Apply 1 and 3.
	initial := []migrations.Migration{
		{Version: 1, Name: "0001_a.sql", SQL: "CREATE TABLE t1 (id INT);", Checksum: "sha256:1"},
		{Version: 3, Name: "0003_c.sql", SQL: "CREATE TABLE t3 (id INT);", Checksum: "sha256:3"},
	}
	if err := migrations.Run(ctx, db, initial); err != nil {
		t.Fatalf("failed to apply initial: %v", err)
	}

	// Now present [1, 2, 3].
	withGap := []migrations.Migration{
		{Version: 1, Name: "0001_a.sql", SQL: "CREATE TABLE t1 (id INT);", Checksum: "sha256:1"},
		{Version: 2, Name: "0002_b.sql", SQL: "CREATE TABLE t2 (id INT);", Checksum: "sha256:2"},
		{Version: 3, Name: "0003_c.sql", SQL: "CREATE TABLE t3 (id INT);", Checksum: "sha256:3"},
	}
	err := migrations.Run(ctx, db, withGap)
	if err == nil {
		t.Fatal("expected error on out-of-order gap migration, got nil")
	}
	if !errors.Is(err, migrations.ErrOutOfOrderMigration) {
		t.Fatalf("expected ErrOutOfOrderMigration, got %v", err)
	}
}

func TestRun_MissingMigrationRefused(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	initial := []migrations.Migration{
		{Version: 1, Name: "0001_a.sql", SQL: "CREATE TABLE t1 (id INT);", Checksum: "sha256:1"},
		{Version: 2, Name: "0002_b.sql", SQL: "CREATE TABLE t2 (id INT);", Checksum: "sha256:2"},
	}
	if err := migrations.Run(ctx, db, initial); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}

	// Present only version 1 when DB has version 1 and 2.
	fewer := []migrations.Migration{
		{Version: 1, Name: "0001_a.sql", SQL: "CREATE TABLE t1 (id INT);", Checksum: "sha256:1"},
	}
	err := migrations.Run(ctx, db, fewer)
	if err == nil {
		t.Fatal("expected error when applied migrations exceed provided list, got nil")
	}
	if !errors.Is(err, migrations.ErrMissingMigration) {
		t.Fatalf("expected ErrMissingMigration, got %v", err)
	}
}

func TestRun_TransactionRollback(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := migrations.RunFS(ctx, db, subFS(t, "testdata/broken"))
	if err == nil {
		t.Fatal("expected error on broken migration, got nil")
	}

	applied, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("failed to query applied migrations: %v", err)
	}

	// 0001 must be applied, but 0002 must not be applied.
	if len(applied) != 1 {
		t.Fatalf("expected exactly 1 applied migration, got %d", len(applied))
	}
	if applied[0].Version != 1 {
		t.Errorf("expected version 1 applied, got %d", applied[0].Version)
	}

	// Table test_users should exist from migration 1.
	var count int
	err = db.QueryRow(ctx, "SELECT count(*) FROM test_users").Scan(&count)
	if err != nil {
		t.Fatalf("table test_users should exist: %v", err)
	}
}

func TestRun_MultipleFSSources(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := migrations.RunFS(ctx, db,
		subFS(t, "testdata/valid"),
		subFS(t, "testdata/module"),
	)
	if err != nil {
		t.Fatalf("failed to run migrations across multiple sources: %v", err)
	}

	applied, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("failed to query applied: %v", err)
	}
	if len(applied) != 3 {
		t.Fatalf("expected 3 applied migrations, got %d", len(applied))
	}

	// Verify all three tables exist.
	tables := []string{"test_users", "test_profiles", "module_data"}
	for _, tbl := range tables {
		var exists bool
		query := "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)"
		if err := db.QueryRow(ctx, query, tbl).Scan(&exists); err != nil {
			t.Fatalf("failed to check table %s existence: %v", tbl, err)
		}
		if !exists {
			t.Errorf("expected table %s to exist", tbl)
		}
	}
}
