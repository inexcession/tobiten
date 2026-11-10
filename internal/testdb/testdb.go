package testdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestDB represents an isolated, fresh PostgreSQL 18 test database.
type TestDB struct {
	// Pool is the connection pool to the isolated test database.
	Pool *pgxpool.Pool
	// URL is the connection string URL for the isolated test database.
	URL string
	// Name is the unique name of the isolated test database.
	Name string
}

// Exec executes a SQL statement using the database connection pool.
func (db *TestDB) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return db.Pool.Exec(ctx, sql, arguments...)
}

// Begin starts a transaction using the database connection pool.
func (db *TestDB) Begin(ctx context.Context) (pgx.Tx, error) {
	return db.Pool.Begin(ctx)
}

// Query executes a query that returns rows using the database connection pool.
func (db *TestDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return db.Pool.Query(ctx, sql, args...)
}

// QueryRow executes a query that is expected to return at most one row.
func (db *TestDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return db.Pool.QueryRow(ctx, sql, args...)
}

// Close closes the test database connection pool.
func (db *TestDB) Close() {
	if db.Pool != nil {
		db.Pool.Close()
	}
}

// New provisions a fresh, isolated PostgreSQL 18 database for the calling test.
// It automatically handles startup (using system binaries discovered via TOBITEN_PG_BIN
// or MacPorts, or an existing TOBITEN_TEST_DATABASE_URL).
// When the test finishes, the database is dropped and resources are cleaned up.
// If not running under Run(m), the PostgreSQL server is stopped after the test completes.
func New(t testing.TB) *TestDB {
	t.Helper()

	srv, err := getOrStartServer(false)
	if err != nil {
		t.Fatalf("testdb: failed to initialize PostgreSQL server: %v", err)
	}

	globalMu.Lock()
	srv.activeTests++
	managed := srv.managedByM
	globalMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dbName, targetURL, err := srv.createFreshDB(ctx)
	if err != nil {
		t.Fatalf("testdb: failed to create fresh test database: %v", err)
	}

	poolCfg, err := pgxpool.ParseConfig(targetURL)
	if err != nil {
		_ = srv.dropDB(context.Background(), dbName)
		t.Fatalf("testdb: failed to parse test pool config: %v", err)
	}
	poolCfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		_ = srv.dropDB(context.Background(), dbName)
		t.Fatalf("testdb: failed to connect to fresh test database %q: %v", dbName, err)
	}

	testDB := &TestDB{
		Pool: pool,
		URL:  targetURL,
		Name: dbName,
	}

	t.Cleanup(func() {
		testDB.Close()

		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		_ = srv.dropDB(dropCtx, dbName)

		globalMu.Lock()
		srv.activeTests--
		shouldStop := srv.activeTests == 0 && !managed
		globalMu.Unlock()

		if shouldStop {
			Stop()
		}
	})

	return testDB
}

// NewPackageDB provisions a fresh database scoped to a package test suite (e.g. for TestMain).
// The caller is responsible for invoking the returned cleanup function when the package tests finish.
func NewPackageDB() (*TestDB, func(), error) {
	srv, err := getOrStartServer(true)
	if err != nil {
		return nil, nil, fmt.Errorf("testdb: failed to initialize PostgreSQL server: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dbName, targetURL, err := srv.createFreshDB(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("testdb: failed to create fresh test database: %w", err)
	}

	poolCfg, err := pgxpool.ParseConfig(targetURL)
	if err != nil {
		_ = srv.dropDB(context.Background(), dbName)
		return nil, nil, fmt.Errorf("testdb: failed to parse test pool config: %w", err)
	}
	poolCfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		_ = srv.dropDB(context.Background(), dbName)
		return nil, nil, fmt.Errorf("testdb: failed to connect to fresh test database %q: %w", dbName, err)
	}

	testDB := &TestDB{
		Pool: pool,
		URL:  targetURL,
		Name: dbName,
	}

	cleanup := func() {
		testDB.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		_ = srv.dropDB(dropCtx, dbName)
	}

	return testDB, cleanup, nil
}

// Run manages the lifecycle of the PostgreSQL 18 server for a test package via TestMain.
// It starts the server when needed, runs the test suite m.Run(), and guarantees that
// the PostgreSQL server is stopped and all temporary resources are cleaned up before returning.
func Run(m *testing.M) int {
	_, _ = getOrStartServer(true)
	defer Stop()
	return m.Run()
}

// Stop shuts down any running managed PostgreSQL test server, removes temporary directories,
// and resets the global server state. It is safe to call multiple times.
func Stop() {
	globalMu.Lock()
	srv := currentServer
	currentServer = nil
	globalMu.Unlock()

	if srv != nil {
		srv.stop()
	}
}
