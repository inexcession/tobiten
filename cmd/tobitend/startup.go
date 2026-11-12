package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/inexcession/tobiten/internal/bootstrap"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/storage/embedded"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// daemonLockID is separate from the migration lock ("tobitend" in ASCII hex).
const daemonLockID int64 = 0x746f626974656e64

type daemonStorage struct {
	owner *pgx.Conn
	pool  *pgxpool.Pool
	local *embedded.Supervisor
}

func (d *daemonStorage) close() error {
	if d.pool != nil {
		d.pool.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var err error
	if d.owner != nil {
		err = d.owner.Close(ctx)
	}
	if d.local != nil && d.local.Running() {
		err = errors.Join(err, d.local.Stop(ctx))
	}
	return err
}

func openDaemonStorage(ctx context.Context, cfg *config.Config) (_ *daemonStorage, err error) {
	frozen, err := identifyStorage(cfg)
	if err != nil {
		return nil, err
	}
	d := &daemonStorage{}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.close())
		}
	}()
	connectionString := cfg.DatabaseURL
	if cfg.StorageMode == "local" {
		// The supervisor emits bootstrap diagnostics before persisted logging is available.
		d.local, err = embedded.New(embedded.Options{DataDir: cfg.PostgresDir, BinDir: cfg.PgBin,
			Logger: slog.New(slog.NewJSONHandler(os.Stdout, nil))})
		if err != nil {
			return nil, err
		}
		if err = d.local.Start(ctx); err != nil {
			return nil, err
		}
		connectionString = d.local.DatabaseURL()
	}
	d.owner, err = pgx.Connect(ctx, connectionString)
	if err != nil {
		return nil, errors.New("could not connect to PostgreSQL; check TOBITEN_DATABASE_URL or local PostgreSQL configuration")
	}
	var version int
	if err = d.owner.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		return nil, err
	}
	if version < 180000 {
		return nil, errors.New("PostgreSQL 18 or later is required")
	}
	var owned bool
	if err = d.owner.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", daemonLockID).Scan(&owned); err != nil {
		return nil, err
	}
	if !owned {
		return nil, errors.New("another tobitend already owns this database; stop it before starting another daemon")
	}
	if err = migrations.RunFS(ctx, d.owner, migrations.CoreFS); err != nil {
		return nil, err
	}
	d.pool, err = postgres.OpenPool(ctx, connectionString)
	if err != nil {
		return nil, errors.New("could not open PostgreSQL application pool")
	}
	settings, err := postgres.NewSettingsRepository(d.pool).GetSettingsMap(ctx)
	if err != nil {
		return nil, err
	}
	if err = applyStartupSettings(cfg, settings, frozen); err != nil {
		return nil, err
	}
	return d, nil
}

// runDaemon owns storage until HTTP serving has stopped. The dedicated lock
// connection is monitored independently; loss cancels application operations.
func runDaemon(ctx context.Context, cfg *config.Config, serve func(context.Context, *config.Config, *slog.Logger) error) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	d, err := openDaemonStorage(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.close()) }()
	stopped := make(chan struct{})
	defer func() { cancel(nil); <-stopped }()
	go func() {
		defer close(stopped)
		monitorOwnership(ctx, d.owner, cancel)
	}()
	if err := setupLogging(cfg); err != nil {
		return err
	}
	if err := bootstrap.ProvisionAdmin(ctx, postgres.NewUserRepository(d.pool), cfg.AdminUser, cfg.AdminPass); err != nil {
		return err
	}
	cfg.AdminPass = ""
	slog.Info("starting server daemon", slog.String("version", Version))
	err = serve(ctx, cfg, slog.Default())
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return errors.Join(err, cause)
	}
	return err
}

func monitorOwnership(ctx context.Context, owner *pgx.Conn, cancel context.CancelCauseFunc) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkCtx, stop := context.WithTimeout(ctx, 2*time.Second)
			err := owner.Ping(checkCtx)
			stop()
			if err != nil {
				if ctx.Err() == nil {
					cancel(fmt.Errorf("database ownership connection lost: %w", err))
				}
				return
			}
		}
	}
}
