package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/auth"
	"github.com/inexcession/tobiten/internal/bootstrap"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/logging"
	"github.com/inexcession/tobiten/internal/storage/embedded"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// daemonLockID is separate from the migration lock ("tobitend" in ASCII hex).
const daemonLockID int64 = 0x746f626974656e64

type localPostgres interface {
	Start(context.Context) error
	Stop(context.Context) error
	Running() bool
	DatabaseURL() string
}

var newLocalPostgres = func(opts embedded.Options) (localPostgres, error) {
	return embedded.New(opts)
}

type daemonStorage struct {
	owner *pgx.Conn
	pool  *pgxpool.Pool
	local localPostgres
}

func (d *daemonStorage) close() error {
	if d.pool != nil {
		d.pool.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Keep the DB session until the supervised process has confirmed its exit.
	// A failed Stop retains the supervisor's data/socket locks and this owner.
	if d.local != nil && d.local.Running() {
		if err := d.local.Stop(ctx); err != nil {
			return err
		}
	}
	if d.owner != nil {
		return d.owner.Close(ctx)
	}
	return nil
}

// finishCleanup deliberately outlives cancellation: exiting while a child is
// unaccounted for would discard its ownership locks and permit an unsafe successor.
func (d *daemonStorage) finishCleanup(logger *slog.Logger) {
	failed := false
	for {
		err := d.close()
		if err == nil {
			if failed {
				logger.Info("PostgreSQL cleanup completed")
			}
			return
		}
		if !failed {
			logger.Error("PostgreSQL cleanup failed; retaining ownership and retrying", logging.Err(err))
		}
		failed = true
		time.Sleep(time.Second)
	}
}

// openDaemonStorage returns a non-nil cleanup handle on failure if a child still
// needs cleanup. Callers must retain it and retry close before relinquishing ownership.
func openDaemonStorage(ctx context.Context, cfg *config.Config) (*daemonStorage, error) {
	d := &daemonStorage{}
	if err := d.open(ctx, cfg); err != nil {
		if cleanupErr := d.close(); cleanupErr != nil {
			return d, errors.Join(err, cleanupErr)
		}
		return nil, err
	}
	return d, nil
}

func (d *daemonStorage) open(ctx context.Context, cfg *config.Config) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	frozen, err := identifyStorage(cfg)
	if err != nil {
		return err
	}
	cfg.SetResolvedDataDir(frozen.dataDir)
	connectionString := cfg.DatabaseURL
	if cfg.StorageMode == "local" {
		// The supervisor emits bootstrap diagnostics before persisted logging is available.
		d.local, err = newLocalPostgres(embedded.Options{DataDir: cfg.PostgresDir, BinDir: cfg.PgBin,
			Logger: bootstrapLogger()})
		if err != nil {
			return err
		}
		if err = d.local.Start(ctx); err != nil {
			return err
		}
		connectionString = d.local.DatabaseURL()
	}
	d.owner, err = pgx.Connect(ctx, connectionString)
	if err != nil {
		return errors.New("could not connect to PostgreSQL; check TOBITEN_DATABASE_URL or local PostgreSQL configuration")
	}
	var version int
	if err = d.owner.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		return err
	}
	if version < 180000 {
		return errors.New("PostgreSQL 18 or later is required")
	}
	var owned bool
	if err = d.owner.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", daemonLockID).Scan(&owned); err != nil {
		return err
	}
	if !owned {
		return errors.New("another tobitend already owns this database; stop it before starting another daemon")
	}
	if err = migrations.RunFS(ctx, d.owner, migrations.CoreFS); err != nil {
		return err
	}
	d.pool, err = postgres.OpenPool(ctx, connectionString)
	if err != nil {
		return errors.New("could not open PostgreSQL application pool")
	}
	settings, err := postgres.NewSettingsRepository(d.pool).GetSettingsMap(ctx)
	if err != nil {
		return err
	}
	return applyStartupSettings(cfg, settings, frozen)
}

// runDaemon owns storage until HTTP serving has stopped. The dedicated lock
// connection is monitored independently; loss cancels application operations.
func runDaemon(ctx context.Context, cfg *config.Config, serve func(context.Context, *config.Config, *slog.Logger, *app.Services) error) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	d, err := openDaemonStorage(ctx, cfg)
	if d != nil {
		defer func() { d.finishCleanup(slog.Default()) }()
	}
	if err != nil {
		return err
	}
	stopped := make(chan struct{})
	defer func() { cancel(nil); <-stopped }()
	go func() {
		defer close(stopped)
		monitorOwnership(ctx, d.owner, cancel)
	}()
	if err := setupLogging(cfg); err != nil {
		return err
	}
	userRepo := postgres.NewUserRepository(d.pool)
	if err := bootstrap.ProvisionAdmin(ctx, userRepo, cfg.AdminUser, cfg.AdminPass); err != nil {
		return err
	}
	cfg.AdminPass = ""
	ub, err := api.NewURLBuilder(cfg.BaseURL)
	if err != nil {
		return err
	}
	setup, err := bootstrap.NewSetup(ctx, userRepo, slog.Default(), ub.URL("/api/v1/auth/setup"))
	if err != nil {
		return err
	}
	defer setup.Close()
	basicAuth, err := auth.NewBasicAuthenticator(userRepo, auth.BasicOptions{
		Logger: slog.Default(),
	})
	if err != nil {
		return err
	}
	services := &app.Services{
		Setup:     setup,
		BasicAuth: basicAuth,
	}
	slog.Info("starting server daemon", slog.String("version", Version))
	err = serve(ctx, cfg, slog.Default(), services)
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
