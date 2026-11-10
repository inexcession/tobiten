package testdb_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/testdb"
)

func TestMain(m *testing.M) {
	os.Exit(testdb.Run(m))
}

// TestWriteAndReadRow satisfies the M1-T28.2 acceptance criterion:
// "One test writes and reads a row through pgx against the harness, locally and in CI."
func TestWriteAndReadRow(t *testing.T) {
	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Create a table.
	_, err := db.Exec(ctx, `
		CREATE TABLE test_harness_records (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			page_count INT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		);
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// 2. Insert a row using pgx parameter binding.
	const (
		expectedID    = "vol_01j7bcx8y9z0123456789abcde"
		expectedTitle = "One Piece, Vol. 1"
		expectedPages = 208
	)
	expectedTime := time.Date(2026, time.November, 10, 12, 0, 0, 0, time.UTC)

	_, err = db.Exec(ctx, `
		INSERT INTO test_harness_records (id, title, page_count, created_at)
		VALUES ($1, $2, $3, $4);
	`, expectedID, expectedTitle, expectedPages, expectedTime)
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	// 3. Read the row back through pgx.
	var (
		actualID    string
		actualTitle string
		actualPages int
		actualTime  time.Time
	)
	err = db.QueryRow(ctx, `
		SELECT id, title, page_count, created_at
		FROM test_harness_records
		WHERE id = $1;
	`, expectedID).Scan(&actualID, &actualTitle, &actualPages, &actualTime)
	if err != nil {
		t.Fatalf("failed to query row: %v", err)
	}

	// 4. Validate fields.
	if actualID != expectedID {
		t.Errorf("expected id %q, got %q", expectedID, actualID)
	}
	if actualTitle != expectedTitle {
		t.Errorf("expected title %q, got %q", expectedTitle, actualTitle)
	}
	if actualPages != expectedPages {
		t.Errorf("expected page_count %d, got %d", expectedPages, actualPages)
	}
	if !actualTime.Equal(expectedTime) {
		t.Errorf("expected created_at %v, got %v", expectedTime, actualTime)
	}
}

func TestPostgresVersion(t *testing.T) {
	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		serverVersionNum int
		versionString    string
	)
	err := db.QueryRow(ctx, "SELECT current_setting('server_version_num')::int, version()").
		Scan(&serverVersionNum, &versionString)
	if err != nil {
		t.Fatalf("failed to query postgres version: %v", err)
	}

	// PostgreSQL 18+ is required by ADR-0001 and M1-T28.2.
	if serverVersionNum < 180000 {
		t.Fatalf("PostgreSQL 18+ required (server_version_num >= 180000), but server reported %d (%s)", serverVersionNum, versionString)
	}
	t.Logf("Verified PostgreSQL version: %s (server_version_num: %d)", versionString, serverVersionNum)
}

func TestFreshDatabaseIsolation(t *testing.T) {
	db1 := testdb.New(t)
	db2 := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if db1.Name == db2.Name {
		t.Fatalf("expected distinct database names, but got %q for both", db1.Name)
	}

	// Create table in db1 only.
	_, err := db1.Exec(ctx, "CREATE TABLE db1_only_table (id TEXT PRIMARY KEY);")
	if err != nil {
		t.Fatalf("failed to create table in db1: %v", err)
	}

	// Verify table does not exist in db2.
	var exists bool
	err = db2.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_name = 'db1_only_table'
		);
	`).Scan(&exists)
	if err != nil {
		t.Fatalf("failed to inspect db2 tables: %v", err)
	}
	if exists {
		t.Fatal("expected db1_only_table to NOT exist in isolated db2, but it was found")
	}
}

func TestNewPackageDB(t *testing.T) {
	pkgDB, cleanup, err := testdb.NewPackageDB()
	if err != nil {
		t.Fatalf("NewPackageDB failed: %v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var one int
	if err := pkgDB.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("failed to query package DB: %v", err)
	}
	if one != 1 {
		t.Fatalf("expected 1, got %d", one)
	}
}
