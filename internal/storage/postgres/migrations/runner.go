package migrations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	pathPkg "path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// MigrationLockID is a 64-bit advisory lock key used to serialize
// migration operations within PostgreSQL ("tobiten_" in ASCII hex).
const MigrationLockID int64 = 0x746f626974656e5f

// CoreFS embeds all core SQL migrations packaged in this directory.
//
//go:embed *.sql
var CoreFS embed.FS

// Sentinel errors returned by the migration runner.
var (
	ErrModifiedMigration   = errors.New("migration modified")
	ErrOutOfOrderMigration = errors.New("migration out of order")
	ErrInvalidMigration    = errors.New("invalid migration")
	ErrMissingMigration    = errors.New("missing migration")
)

// Migration represents a parsed plain-SQL migration script.
type Migration struct {
	Version  int64
	Name     string
	SQL      string
	Checksum string
}

// AppliedMigration represents a record in the schema_migrations table.
type AppliedMigration struct {
	Version   int64
	Name      string
	Checksum  string
	AppliedAt time.Time
}

// DB represents the minimal PostgreSQL database interface required by the runner.
// *pgxpool.Pool, *pgx.Conn, and *testdb.TestDB satisfy this interface.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var migrationFilenamePattern = regexp.MustCompile(`^([0-9]+)(?:_([a-zA-Z0-9_\-]+))?\.sql$`)

func parseMigrationFilename(filename string) (int64, error) {
	matches := migrationFilenamePattern.FindStringSubmatch(filename)
	if len(matches) == 0 {
		return 0, fmt.Errorf("filename %q does not match pattern <version>_<description>.sql", filename)
	}

	version, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid version number in %q: %w", filename, err)
	}
	if version <= 0 {
		return 0, fmt.Errorf("migration version in %q must be positive, got %d", filename, version)
	}

	return version, nil
}

// Collect discovers and parses migration files from one or more fs.FS sources.
// It normalizes line endings to LF, calculates SHA-256 checksums, sorts the migrations
// in ascending version order, and rejects duplicate versions or malformed filenames.
func Collect(sources ...fs.FS) ([]Migration, error) {
	var migrations []Migration

	for _, source := range sources {
		if source == nil {
			continue
		}

		err := fs.WalkDir(source, ".", func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}

			filename := pathPkg.Base(p)
			if !strings.HasSuffix(filename, ".sql") {
				return nil
			}

			version, err := parseMigrationFilename(filename)
			if err != nil {
				return fmt.Errorf("%w: %w (at %s)", ErrInvalidMigration, err, p)
			}

			content, err := fs.ReadFile(source, p)
			if err != nil {
				return fmt.Errorf("reading migration %s: %w", p, err)
			}

			normalized := bytes.ReplaceAll(content, []byte("\r\n"), []byte("\n"))
			sum := sha256.Sum256(normalized)
			checksum := fmt.Sprintf("sha256:%x", sum)

			migrations = append(migrations, Migration{
				Version:  version,
				Name:     filename,
				SQL:      string(normalized),
				Checksum: checksum,
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	for i := 1; i < len(migrations); i++ {
		if migrations[i].Version == migrations[i-1].Version {
			return nil, fmt.Errorf("%w: duplicate migration version %d (%s and %s)",
				ErrOutOfOrderMigration, migrations[i].Version, migrations[i-1].Name, migrations[i].Name)
		}
	}

	return migrations, nil
}

// Run applies an ordered list of migrations to the database.
// It ensures the schema_migrations table exists, verifies that all already-applied
// migrations match the incoming sequence (version, name, checksum), refuses modified
// or out-of-order migrations, and executes each new migration file in its own transaction.
// If all migrations have already been applied, Run is a no-op.
func Run(ctx context.Context, db DB, migrations []Migration) error {
	// 1. Verify that the input migrations list has strictly ascending versions.
	for i := 1; i < len(migrations); i++ {
		if migrations[i].Version <= migrations[i-1].Version {
			return fmt.Errorf("%w: migrations must have strictly ascending versions: got version %d (%s) after %d (%s)",
				ErrOutOfOrderMigration, migrations[i].Version, migrations[i].Name,
				migrations[i-1].Version, migrations[i-1].Name)
		}
	}

	// 2. Ensure schema_migrations table exists.
	if err := ensureSchemaMigrationsTable(ctx, db); err != nil {
		return err
	}

	// 3. Query all applied migrations.
	applied, err := Applied(ctx, db)
	if err != nil {
		return fmt.Errorf("querying applied migrations: %w", err)
	}

	nApplied := len(applied)
	if nApplied > len(migrations) {
		return fmt.Errorf("%w: database has %d applied migrations, but only %d migrations were provided",
			ErrMissingMigration, nApplied, len(migrations))
	}

	// 4. Verify applied migrations match the beginning of the planned sequence.
	for i := 0; i < nApplied; i++ {
		app := applied[i]
		mig := migrations[i]

		if mig.Version != app.Version {
			return fmt.Errorf("%w: migration version mismatch at position %d: database recorded %d (%s), provided %d (%s)",
				ErrOutOfOrderMigration, i, app.Version, app.Name, mig.Version, mig.Name)
		}

		if mig.Checksum != app.Checksum {
			return fmt.Errorf("%w: migration %d (%s) checksum mismatch: database recorded %s, current file is %s",
				ErrModifiedMigration, mig.Version, mig.Name, app.Checksum, mig.Checksum)
		}

		if mig.Name != app.Name {
			return fmt.Errorf("%w: migration %d name mismatch: database recorded %q, current file is %q",
				ErrModifiedMigration, mig.Version, app.Name, mig.Name)
		}
	}

	// If all migrations are already applied and verified, re-running is a no-op.
	if nApplied == len(migrations) {
		return nil
	}

	// 5. Apply pending migrations forward, each in its own transaction.
	for i := nApplied; i < len(migrations); i++ {
		if err := applyMigration(ctx, db, migrations[i]); err != nil {
			return err
		}
	}

	return nil
}

// RunFS collects migrations from the given sources and applies them to the database.
func RunFS(ctx context.Context, db DB, sources ...fs.FS) error {
	migs, err := Collect(sources...)
	if err != nil {
		return err
	}
	return Run(ctx, db, migs)
}

func ensureSchemaMigrationsTable(ctx context.Context, db DB) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction for schema_migrations init: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", MigrationLockID); err != nil {
		return fmt.Errorf("acquiring advisory lock for schema_migrations init: %w", err)
	}

	const createTableSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     BIGINT PRIMARY KEY,
    name        TEXT NOT NULL,
    checksum    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);`

	if _, err := tx.Exec(ctx, createTableSQL); err != nil {
		return fmt.Errorf("creating schema_migrations table: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing schema_migrations table creation: %w", err)
	}

	return nil
}

func applyMigration(ctx context.Context, db DB, mig Migration) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction for migration %s: %w", mig.Name, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", MigrationLockID); err != nil {
		return fmt.Errorf("acquiring advisory lock for migration %s: %w", mig.Name, err)
	}

	var exists bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)", mig.Version).Scan(&exists)
	if err != nil {
		return fmt.Errorf("checking migration existence for %s: %w", mig.Name, err)
	}
	if exists {
		return nil
	}

	if _, err := tx.Exec(ctx, mig.SQL); err != nil {
		return fmt.Errorf("executing migration %s (version %d): %w", mig.Name, mig.Version, err)
	}

	const insertRecordSQL = `
INSERT INTO schema_migrations (version, name, checksum, applied_at)
VALUES ($1, $2, $3, now());`

	if _, err := tx.Exec(ctx, insertRecordSQL, mig.Version, mig.Name, mig.Checksum); err != nil {
		return fmt.Errorf("recording migration %s (version %d): %w", mig.Name, mig.Version, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing migration %s (version %d): %w", mig.Name, mig.Version, err)
	}

	return nil
}

// Applied returns all applied migrations ordered by version ascending.
func Applied(ctx context.Context, db DB) ([]AppliedMigration, error) {
	rows, err := db.Query(ctx, "SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var applied []AppliedMigration
	for rows.Next() {
		var a AppliedMigration
		if err := rows.Scan(&a.Version, &a.Name, &a.Checksum, &a.AppliedAt); err != nil {
			return nil, fmt.Errorf("scanning schema_migrations row: %w", err)
		}
		applied = append(applied, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading schema_migrations rows: %w", err)
	}

	return applied, nil
}
