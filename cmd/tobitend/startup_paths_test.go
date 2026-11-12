package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/storage/embedded"
	"github.com/jackc/pgx/v5"
)

func TestStorageRootAliasesProvisionActualPGDATA(t *testing.T) {
	if _, err := embedded.FindBinDir(""); err != nil {
		t.Skipf("host PostgreSQL binaries required: %v", err)
	}
	for _, kind := range []string{"missing-relative", "missing-absolute", "symlink-dotdot"} {
		t.Run(kind, func(t *testing.T) {
			base := t.TempDir()
			t.Chdir(base)
			// Resolve the temporary-directory ancestor too (/var -> /private/var on macOS).
			base, err := embedded.CanonicalPath(base)
			if err != nil {
				t.Fatal(err)
			}
			root, target := "./data", filepath.Join(base, "future")
			switch kind {
			case "missing-relative":
				err = os.Symlink("future", "data")
			case "missing-absolute":
				err = os.Symlink(target, "data")
			case "symlink-dotdot":
				if err = os.MkdirAll(filepath.Join(base, "other", "nested"), 0700); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink("other/nested", "link")
				root, target = base+"/link/../data", filepath.Join(base, "other", "data")
			}
			if err != nil {
				t.Fatal(err)
			}
			frozenBefore, err := embedded.CanonicalPath(root)
			if err != nil || frozenBefore != target {
				t.Fatalf("root=%s want=%s err=%v", frozenBefore, target, err)
			}
			originalFactory, originalLogger := newLocalPostgres, slog.Default()
			t.Cleanup(func() { newLocalPostgres = originalFactory; slog.SetDefault(originalLogger) })
			var supervisor *embedded.Supervisor
			newLocalPostgres = func(opts embedded.Options) (localPostgres, error) {
				s, err := embedded.New(opts)
				supervisor = s
				return s, err
			}
			var firstID string
			for start := range 2 {
				env := []string{"TOBITEN_ADMIN_USER=alice", "TOBITEN_ADMIN_PASS=password"}
				if start == 1 {
					env = append(env, "TOBITEN_COVERS_DIR=independent-covers", "TOBITEN_CACHE_DIR=independent-cache")
				}
				if kind == "symlink-dotdot" {
					env = append(env, "TOBITEN_DATA_DIR="+root)
				}
				cfg, err := config.Load(nil, config.WithEnviron(env), config.WithoutEnvFile())
				if err != nil {
					t.Fatal(err)
				}
				err = runDaemon(t.Context(), cfg, func(ctx context.Context, cfg *config.Config, _ *slog.Logger, _ *app.Services) error {
					conn, err := pgx.Connect(ctx, supervisor.DatabaseURL())
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close(ctx)
					var pgdata, id string
					if err := conn.QueryRow(ctx, "SELECT current_setting('data_directory'),id FROM users WHERE username='alice'").Scan(&pgdata, &id); err != nil {
						t.Fatal(err)
					}
					if pgdata != filepath.Join(target, "postgres") || cfg.DataDir != target || cfg.PostgresDir != pgdata || cfg.BackupsDir != filepath.Join(target, "backups") || cfg.SecretKeyPath != filepath.Join(target, "secret.key") {
						t.Fatalf("opened wrong storage: PGDATA=%s cfg=%+v", pgdata, cfg.EffectiveConfig())
					}
					if start == 0 {
						if cfg.CoversDir != filepath.Join(target, "covers") || cfg.CacheDir != filepath.Join(target, "cache") {
							t.Fatal("default directories did not follow resolved root")
						}
					} else if cfg.CoversDir != "independent-covers" || cfg.CacheDir != "independent-cache" {
						t.Fatal("overwrote independent directories")
					}
					if kind != "symlink-dotdot" && (cfg.IsEnvLocked("TOBITEN_DATA_DIR") || cfg.SourceOf("TOBITEN_DATA_DIR") != config.SourceDefault && cfg.SourceOf("TOBITEN_DATA_DIR") != config.SourceSystemSettings) {
						t.Fatal("canonicalization changed provenance")
					}
					if firstID == "" {
						firstID = id
					} else if firstID != id {
						t.Fatal("restart changed initial user")
					}
					_, err = conn.Exec(ctx, "INSERT INTO system_settings(key,value) VALUES ('storage.data_dir',to_jsonb($1::text)) ON CONFLICT(key) DO UPDATE SET value=excluded.value", target)
					return err
				})
				if err != nil {
					t.Fatal(err)
				}
				if supervisor.Running() || supervisor.DataDirLocked() || supervisor.SocketDirLocked() {
					t.Fatal("local ownership retained after successful cleanup")
				}
			}
			if _, err := os.Stat(filepath.Join(target, "postgres", "postmaster.pid")); !os.IsNotExist(err) {
				t.Fatalf("postmaster remains: %v", err)
			}
			if kind == "symlink-dotdot" {
				if _, err := os.Stat(filepath.Join(base, "data")); !os.IsNotExist(err) {
					t.Fatalf("lexically cleaned sibling root was created: %v", err)
				}
			}
		})
	}
}
