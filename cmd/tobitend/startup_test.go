package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/auth"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage/embedded"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func TestMain(m *testing.M) { os.Exit(testdb.Run(m)) }

func hostedConfig(t *testing.T, databaseURL string) *config.Config {
	t.Helper()
	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres", "TOBITEN_DATABASE_URL=" + databaseURL,
		"TOBITEN_ADMIN_USER=alice", "TOBITEN_ADMIN_PASS=initial-password",
	}))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func migrateStartupDB(t *testing.T, db *testdb.TestDB) {
	t.Helper()
	if err := migrations.RunFS(t.Context(), db, migrations.CoreFS); err != nil {
		t.Fatal(err)
	}
}

func putSetting(t *testing.T, db *testdb.TestDB, key, value string) {
	t.Helper()
	if _, err := db.Exec(t.Context(), "INSERT INTO system_settings(key,value) VALUES ($1,to_jsonb($2::text)) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value); err != nil {
		t.Fatal(err)
	}
}

func TestDaemonEnvironmentAdminProvisioning(t *testing.T) {
	db := testdb.New(t)
	originalServer, originalLogger := startServer, slog.Default()
	t.Cleanup(func() { startServer = originalServer; slog.SetDefault(originalLogger) })
	t.Setenv("TOBITEN_STORAGE_MODE", "postgres")
	t.Setenv("TOBITEN_DATABASE_URL", db.URL)
	t.Setenv("TOBITEN_ADMIN_USER", "alice")
	t.Setenv("TOBITEN_ADMIN_PASS", "initial-password")
	var firstID, firstHash string
	calls := 0
	startServer = func(ctx context.Context, cfg *config.Config, _ *slog.Logger) error {
		calls++
		if cfg.AdminPass != "" {
			t.Error("bootstrap password retained in serving config")
		}
		repo := postgres.NewUserRepository(db.Pool)
		users, err := repo.ListUsers(ctx)
		if err != nil || len(users) != 1 {
			t.Fatalf("users=%v err=%v", users, err)
		}
		user := users[0]
		if user.Username != "alice" {
			t.Fatalf("username=%q", user.Username)
		}
		roles, err := repo.ListUserRoles(ctx, user.ID)
		if err != nil || len(roles) != 1 || roles[0].ID != domain.RoleAdminID {
			t.Fatalf("roles=%v err=%v", roles, err)
		}
		credentials, err := repo.ListCredentialsByUserID(ctx, user.ID)
		if err != nil || len(credentials) != 1 {
			t.Fatalf("credentials count=%d err=%v", len(credentials), err)
		}
		cred := credentials[0]
		if cred.Kind != domain.CredentialKindPassword || cred.SecretHash == nil || cred.HashScheme == nil || *cred.HashScheme != auth.HashSchemeArgon2id {
			t.Fatal("invalid password credential")
		}
		if ok, err := auth.VerifyPassword("initial-password", *cred.SecretHash); err != nil || !ok {
			t.Fatal(err)
		}
		if firstID == "" {
			firstID, firstHash = user.ID, *cred.SecretHash
		} else if user.ID != firstID || *cred.SecretHash != firstHash {
			t.Error("restart replaced account or password")
		}
		return nil
	}
	if code := runContext(t.Context(), nil); code != 0 {
		t.Fatalf("first start=%d", code)
	}
	t.Setenv("TOBITEN_ADMIN_USER", "replacement")
	t.Setenv("TOBITEN_ADMIN_PASS", "replacement-password")
	if code := runContext(t.Context(), nil); code != 0 {
		t.Fatalf("second start=%d", code)
	}
	if calls != 2 {
		t.Fatalf("server calls=%d", calls)
	}
}

func TestStartupSettingsBeforeProvisioningAndServing(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)
	putSetting(t, db, "server.base_url", "https://books.example.test/sub")
	putSetting(t, db, "log.level", "debug")
	putSetting(t, db, "log.format", "text")
	// Secret rows are not applied, including a corrupted value that would fail config validation.
	if _, err := db.Exec(t.Context(), "INSERT INTO system_settings(key,value,is_secret) VALUES ('server.port','\"secret-ciphertext\"',true)"); err != nil {
		t.Fatal(err)
	}
	original := slog.Default()
	t.Cleanup(func() { slog.SetDefault(original) })
	err := runDaemon(t.Context(), hostedConfig(t, db.URL), func(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
		if cfg.BaseURL != "https://books.example.test/sub" || !logger.Enabled(ctx, slog.LevelDebug) || cfg.LogFormat != "text" {
			t.Error("stored settings not resolved before serving/logging")
		}
		if cfg.SourceOf("TOBITEN_BASE_URL") != config.SourceSystemSettings || cfg.IsEnvLocked("TOBITEN_BASE_URL") {
			t.Error("incorrect persisted provenance")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStartupIdentityConflictNoAdminAndCleanup(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)
	putSetting(t, db, "storage.data_dir", t.TempDir())
	called := false
	err := runDaemon(t.Context(), hostedConfig(t, db.URL), func(context.Context, *config.Config, *slog.Logger) error { called = true; return nil })
	if err == nil || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("expected actionable conflict: %v", err)
	}
	if called {
		t.Fatal("served on conflict")
	}
	count, err := postgres.NewUserRepository(db.Pool).CountUsers(t.Context())
	if err != nil || count != 0 {
		t.Fatalf("users=%d err=%v", count, err)
	}
	// Failed startup must release its lifetime session lock and application connections.
	if _, err := db.Exec(t.Context(), "DELETE FROM system_settings"); err != nil {
		t.Fatal(err)
	}
	d, err := openDaemonStorage(t.Context(), hostedConfig(t, db.URL))
	if err != nil {
		t.Fatalf("restart after conflict: %v", err)
	}
	if err := d.close(); err != nil {
		t.Fatal(err)
	}
}

func TestDaemonOwnershipConflictAndLoss(t *testing.T) {
	db := testdb.New(t)
	cfg := hostedConfig(t, db.URL)
	d, err := openDaemonStorage(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.close(); err != nil {
			t.Error(err)
		}
	}()
	if other, err := openDaemonStorage(t.Context(), hostedConfig(t, db.URL)); err == nil {
		_ = other.close()
		t.Fatal("second daemon accepted")
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	done := make(chan struct{})
	go func() { defer close(done); monitorOwnership(ctx, d.owner, cancel) }()
	if _, err := db.Exec(t.Context(), "SELECT pg_terminate_backend($1)", d.owner.PgConn().PID()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		if !strings.Contains(context.Cause(ctx).Error(), "ownership connection lost") {
			t.Fatal(context.Cause(ctx))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ownership loss not detected")
	}
	<-done
}

func TestRunDaemonOwnershipLossStopsServing(t *testing.T) {
	db := testdb.New(t)
	original := slog.Default()
	t.Cleanup(func() { slog.SetDefault(original) })
	err := runDaemon(t.Context(), hostedConfig(t, db.URL), func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
		// Identify the dedicated owner by its granted advisory lock, not the application pool.
		if _, err := db.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND granted AND classid=$1::oid AND objid=$2::oid", daemonLockID>>32, daemonLockID&0xffffffff); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("serving was not cancelled")
		}
	})
	if err == nil || !strings.Contains(err.Error(), "ownership connection lost") {
		t.Fatalf("ownership failure=%v", err)
	}
}

func TestLocalDaemonProvisioningAndRestart(t *testing.T) {
	if _, err := embedded.FindBinDir(""); err != nil {
		t.Skipf("local-supervisor coverage requires host PostgreSQL binaries: %v", err)
	}
	dataDir := t.TempDir()
	original := slog.Default()
	t.Cleanup(func() { slog.SetDefault(original) })
	var firstID string
	for range 2 {
		cfg, err := config.Load([]string{"--data-dir=" + dataDir}, config.WithoutEnvFile(), config.WithEnviron([]string{"TOBITEN_ADMIN_USER=local-user", "TOBITEN_ADMIN_PASS=local-password"}))
		if err != nil {
			t.Fatal(err)
		}
		err = runDaemon(t.Context(), cfg, func(ctx context.Context, cfg *config.Config, _ *slog.Logger) error {
			// postmaster.pid records the actual socket directory, including long-path fallback.
			pid, err := os.ReadFile(filepath.Join(dataDir, "postgres", "postmaster.pid"))
			if err != nil {
				t.Fatal(err)
			}
			socketDir := strings.Split(string(pid), "\n")[4]
			conn, err := pgx.Connect(ctx, "host="+embedded.EscapeConnStringValue(socketDir)+" user=postgres dbname=postgres sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			var id string
			if err := conn.QueryRow(ctx, "SELECT id FROM users WHERE username='local-user'").Scan(&id); err != nil {
				t.Fatal(err)
			}
			if firstID == "" {
				firstID = id
			} else if firstID != id {
				t.Error("local restart changed user")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dataDir, "postgres", "postmaster.pid")); !os.IsNotExist(err) {
		t.Fatalf("local postmaster survived shutdown: %v", err)
	}
}

func TestLocalStartupConflictCleansUp(t *testing.T) {
	if _, err := embedded.FindBinDir(""); err != nil {
		t.Skipf("local-supervisor coverage requires host PostgreSQL binaries: %v", err)
	}
	t.Chdir(t.TempDir())
	load := func(args ...string) *config.Config {
		t.Helper()
		cfg, err := config.Load(args, config.WithoutEnvFile(), config.WithEnviron([]string{"TOBITEN_ADMIN_USER=alice", "TOBITEN_ADMIN_PASS=password"}))
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	d, err := openDaemonStorage(t.Context(), load())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.pool.Exec(t.Context(), "INSERT INTO system_settings(key,value) VALUES ('storage.mode','\"postgres\"')"); err != nil {
		_ = d.close()
		t.Fatal(err)
	}
	if err := d.close(); err != nil {
		t.Fatal(err)
	}
	err = runDaemon(t.Context(), load(), func(context.Context, *config.Config, *slog.Logger) error {
		t.Fatal("served on identity conflict")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("conflict=%v", err)
	}
	if _, err := os.Stat("data/postgres/postmaster.pid"); !os.IsNotExist(err) {
		t.Fatalf("failed startup left local PostgreSQL running: %v", err)
	}
	// A pinned bootstrap mode overrides the conflicting row and can reclaim both
	// supervisor ownership and database ownership after the failed startup.
	d, err = openDaemonStorage(t.Context(), load("--storage-mode=local"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.close(); err != nil {
			t.Error(err)
		}
	}()
	count, err := postgres.NewUserRepository(d.pool).CountUsers(t.Context())
	if err != nil || count != 0 {
		t.Fatalf("conflicting startup wrote users: count=%d err=%v", count, err)
	}
}
