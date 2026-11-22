package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/auth"
	"github.com/inexcession/tobiten/internal/bootstrap"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/logging"
	"github.com/inexcession/tobiten/internal/module"
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

var buildDaemonRegistry = BuildRegistryWithDeps

var configureDaemonJobRunner = func(*jobs.Runner, module.Registry, *config.Config, *app.Services) error {
	return nil
}

var newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
	return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{})
}

var inspectDaemonWorkers = func(*daemonWorkers) {}

var attachDaemonScanTrigger = func(*scanner.ScanTrigger) {}

type parsedPoolSetting struct {
	rawKey string
	suffix string
	size   int
}

type parsedPoolConfig struct {
	defaultPoolSize int
	byNorm          map[string][]parsedPoolSetting
}

func parsePoolSettings(cfg *config.Config) (parsedPoolConfig, error) {
	var out parsedPoolConfig
	if cfg == nil {
		return out, nil
	}

	sys := cfg.SystemSettings()
	keys := make([]string, 0, len(sys))
	for key := range sys {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	var defaultKey string
	var defaultSeen bool
	out.byNorm = make(map[string][]parsedPoolSetting)

	for _, key := range keys {
		rawVal := sys[key]
		trimmedKey := strings.TrimSpace(key)
		trimmedVal := strings.TrimSpace(rawVal)
		if trimmedVal == "" {
			continue
		}
		if strings.EqualFold(trimmedKey, "jobs.default_pool_size") {
			n, err := strconv.Atoi(trimmedVal)
			if err != nil || n < 1 {
				return out, fmt.Errorf("%w: system_settings %q = %q (must be a positive integer)", jobs.ErrInvalidPoolSize, key, rawVal)
			}
			if defaultSeen {
				return out, fmt.Errorf("%w: conflicting default pool size settings %q (%d) and %q (%d)",
					jobs.ErrInvalidPoolSize, defaultKey, out.defaultPoolSize, key, n)
			}
			defaultSeen = true
			defaultKey = key
			out.defaultPoolSize = n
			continue
		}

		const poolPrefix = "jobs.pool."
		if len(trimmedKey) >= len(poolPrefix) && strings.EqualFold(trimmedKey[:len(poolPrefix)], poolPrefix) {
			suffix := strings.TrimSpace(trimmedKey[len(poolPrefix):])
			if suffix == "" {
				return out, fmt.Errorf("%w: system_settings %q has empty job kind", jobs.ErrInvalidPoolSize, key)
			}
			n, err := strconv.Atoi(trimmedVal)
			if err != nil || n < 1 {
				return out, fmt.Errorf("%w: system_settings %q = %q (must be a positive integer)", jobs.ErrInvalidPoolSize, key, rawVal)
			}
			entry := parsedPoolSetting{rawKey: key, suffix: suffix, size: n}
			norm := strings.ToLower(suffix)
			out.byNorm[norm] = append(out.byNorm[norm], entry)
		}
	}

	return out, nil
}

func resolvePoolSizesForKinds(parsed parsedPoolConfig, registeredKinds []domain.JobKind) (map[domain.JobKind]int, error) {
	exactRegistered := make(map[domain.JobKind]struct{}, len(registeredKinds))
	normRegistered := make(map[string][]domain.JobKind, len(registeredKinds))
	for _, kind := range registeredKinds {
		exactRegistered[kind] = struct{}{}
		norm := strings.ToLower(string(kind))
		normRegistered[norm] = append(normRegistered[norm], kind)
	}

	norms := make([]string, 0, len(parsed.byNorm))
	for norm := range parsed.byNorm {
		norms = append(norms, norm)
	}
	slices.Sort(norms)

	poolSizes := make(map[domain.JobKind]int)
	for _, norm := range norms {
		entries := parsed.byNorm[norm]
		if len(entries) > 1 {
			// Multiple setting keys normalize to the same name. Only allow if each setting
			// resolves cleanly to a distinct exact registered kind; otherwise reject the alias conflict.
			seenTargets := make(map[domain.JobKind]string, len(entries))
			for _, e := range entries {
				target := domain.JobKind(e.suffix)
				if _, isExact := exactRegistered[target]; !isExact {
					return nil, fmt.Errorf("%w: conflicting pool size settings %q (%d) and %q (%d)",
						jobs.ErrInvalidPoolSize, entries[0].rawKey, entries[0].size, e.rawKey, e.size)
				}
				if prevKey, dup := seenTargets[target]; dup {
					return nil, fmt.Errorf("%w: conflicting pool size settings %q and %q for job kind %q",
						jobs.ErrInvalidPoolSize, prevKey, e.rawKey, target)
				}
				seenTargets[target] = e.rawKey
				poolSizes[target] = e.size
			}
			continue
		}

		e := entries[0]
		exact := domain.JobKind(e.suffix)
		if _, ok := exactRegistered[exact]; ok {
			poolSizes[exact] = e.size
			continue
		}

		matches := normRegistered[norm]
		switch len(matches) {
		case 1:
			poolSizes[matches[0]] = e.size
		case 0:
			poolSizes[exact] = e.size
		default:
			return nil, fmt.Errorf("%w: setting %q is ambiguous across registered job kinds %v",
				jobs.ErrInvalidPoolSize, e.rawKey, matches)
		}
	}

	if len(poolSizes) == 0 {
		return nil, nil
	}
	return poolSizes, nil
}

type daemonWorkers struct {
	runner      *jobs.Runner
	delivery    *events.DeliveryCoordinator
	dispatcher  *events.Dispatcher
	progressHub *events.ProgressHub
}

func (w *daemonWorkers) stopAndWait() {
	if w == nil {
		return
	}
	if w.dispatcher != nil {
		w.dispatcher.Stop()
	}
	if w.delivery != nil {
		w.delivery.Stop()
	}
	if w.runner != nil {
		w.runner.Stop()
	}
}

func buildDaemonJobComponents(
	cancel context.CancelCauseFunc,
	pool *pgxpool.Pool,
	defaultPoolSize int,
) (*jobs.Runner, *events.DeliveryCoordinator, *events.Dispatcher, *events.ProgressHub, error) {
	onFatal := func(fatalErr error) {
		if cancel != nil {
			cancel(fatalErr)
		}
	}

	eventRepo := postgres.NewEventRepository(pool)
	jobRepo := postgres.NewJobRepository(pool)
	progressHub, err := events.NewProgressHub(events.ProgressHubOptions{})
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("initializing job progress hub: %w", err)
	}
	var dispatcher *events.Dispatcher
	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{
		Logger:          slog.Default(),
		DefaultPoolSize: defaultPoolSize,
		OnFatal:         onFatal,
		ProgressHub:     progressHub,
		OnTransition: func(job domain.Job) {
			if dispatcher != nil && !job.Kind.IsInternal() {
				dispatcher.Notify()
			}
		},
	})
	delivery := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner, events.DeliveryOptions{
		Logger:  slog.Default(),
		OnFatal: onFatal,
	})
	dispatcher = events.NewDispatcher(eventRepo,
		events.WithDispatcherLogger(slog.Default()),
		events.WithOnPublished(func(_ context.Context, _ []domain.DomainEvent) error {
			delivery.Wake()
			return nil
		}),
		events.WithOnFatal(onFatal),
	)
	return runner, delivery, dispatcher, progressHub, nil
}

func applyDaemonPoolSizes(parsed parsedPoolConfig, runner *jobs.Runner) error {
	sizes, err := resolvePoolSizesForKinds(parsed, runner.Kinds())
	if err != nil {
		return err
	}
	return runner.SetPoolSizes(sizes)
}

func startDaemonJobs(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	pool *pgxpool.Pool,
	cfg *config.Config,
	services *app.Services,
) (*daemonWorkers, error) {
	parsed, err := parsePoolSettings(cfg)
	if err != nil {
		return nil, err
	}
	var scannerWorkers int
	if cfg != nil {
		scannerWorkers = cfg.ScannerWorkers
	}
	if scannerWorkers < 0 {
		return nil, scanner.ErrInvalidWorkerCount
	}
	sc, err := newDaemonScanner(pool, cfg)
	if err != nil {
		return nil, fmt.Errorf("initializing library scanner: %w", err)
	}

	reg := buildDaemonRegistry(cfg, services, RegistryDeps{
		Scanner:        sc,
		ScannerWorkers: scannerWorkers,
	})
	runner, delivery, dispatcher, progressHub, err := buildDaemonJobComponents(cancel, pool, parsed.defaultPoolSize)
	if err != nil {
		return nil, err
	}

	if err := runner.RegisterModuleKinds(reg); err != nil {
		return nil, fmt.Errorf("registering module job kinds: %w", err)
	}
	if err := runner.RegisterModuleHandlers(reg); err != nil {
		return nil, fmt.Errorf("registering module job handlers: %w", err)
	}
	if err := delivery.RegisterModuleSubscribers(reg); err != nil {
		return nil, fmt.Errorf("registering module event subscribers: %w", err)
	}
	if err := delivery.RegisterJobs(runner); err != nil {
		return nil, fmt.Errorf("registering internal EVENT_DELIVERY job kind: %w", err)
	}

	if err := applyDaemonPoolSizes(parsed, runner); err != nil {
		return nil, err
	}
	if err := configureDaemonJobRunner(runner, reg, cfg, services); err != nil {
		return nil, fmt.Errorf("configuring daemon job runner: %w", err)
	}
	if err := applyDaemonPoolSizes(parsed, runner); err != nil {
		return nil, err
	}

	if err := delivery.SyncSubscribers(ctx); err != nil {
		return nil, fmt.Errorf("syncing durable event subscribers: %w", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		return nil, fmt.Errorf("recovering interrupted jobs: %w", err)
	}
	if err := runner.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting job worker pools: %w", err)
	}
	if err := delivery.Start(ctx); err != nil {
		runner.Stop()
		return nil, fmt.Errorf("starting event delivery coordinator: %w", err)
	}
	if err := dispatcher.Start(ctx); err != nil {
		delivery.Stop()
		runner.Stop()
		return nil, fmt.Errorf("starting domain event dispatcher: %w", err)
	}
	workers := &daemonWorkers{
		runner:      runner,
		delivery:    delivery,
		dispatcher:  dispatcher,
		progressHub: progressHub,
	}
	inspectDaemonWorkers(workers)
	return workers, nil
}

func buildDaemonServices(
	pool *pgxpool.Pool,
	setup *bootstrap.Setup,
	userRepo *postgres.UserRepository,
) (*app.Services, *jobs.Service, *events.StreamService, error) {
	hashLimiter, err := auth.NewHashLimiter(auth.HashLimiterOptions{})
	if err != nil {
		return nil, nil, nil, err
	}
	basicAuth, err := auth.NewBasicAuthenticator(userRepo, auth.BasicOptions{
		Logger:  slog.Default(),
		Limiter: hashLimiter,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	patAuth, err := auth.NewPATAuthenticator(userRepo, auth.PATOptions{
		Logger:  slog.Default(),
		Limiter: hashLimiter,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	sessionRepo := postgres.NewSessionRepository(pool)
	sessionService, err := auth.NewSessionService(basicAuth, userRepo, sessionRepo, auth.SessionOptions{})
	if err != nil {
		return nil, nil, nil, err
	}
	sessionAuth, err := auth.NewSessionAuthenticator(userRepo, sessionRepo, auth.SessionOptions{})
	if err != nil {
		return nil, nil, nil, err
	}
	eventRepo := postgres.NewEventRepository(pool)
	jobRepo := postgres.NewJobRepository(pool)
	jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})
	eventService := events.NewStreamService(eventRepo, jobRepo, events.StreamOptions{
		Users:    userRepo,
		Sessions: sessionRepo,
	})
	services := app.NewAuthorizingServices(&app.Services{
		Setup:       setup,
		BasicAuth:   basicAuth,
		PATAuth:     patAuth,
		Session:     sessionService,
		SessionAuth: sessionAuth,
		Jobs:        jobService,
		Events:      eventService,
	})
	return services, jobService, eventService, nil
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
	services, jobService, eventService, err := buildDaemonServices(d.pool, setup, userRepo)
	if err != nil {
		return err
	}
	workers, err := startDaemonJobs(ctx, cancel, d.pool, cfg, services)
	if err != nil {
		return err
	}
	eventService.SetProgressHub(workers.progressHub)
	eventService.SetWakeSource(workers.dispatcher)
	jobService.SetNotifier(workers.runner)
	jobService.SetOnTransition(func(job domain.Job) {
		if workers != nil && workers.dispatcher != nil && !job.Kind.IsInternal() {
			workers.dispatcher.Notify()
		}
	})
	attachDaemonScanTrigger(scanner.NewScanTrigger(jobService))
	defer func() {
		cancel(nil)
		workers.stopAndWait()
	}()
	slog.Info("starting server daemon", slog.String("version", Version))
	err = serve(ctx, cfg, slog.Default(), services)
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return errors.Join(err, cause)
	}
	return err
}

const checkDaemonOwnershipSQL = `SELECT EXISTS (
	SELECT 1
	FROM pg_locks
	WHERE pid = pg_backend_pid()
	  AND locktype = 'advisory'
	  AND granted
	  AND classid = $1::oid
	  AND objid = $2::oid
	  AND objsubid = 1
)`

func monitorOwnership(ctx context.Context, owner *pgx.Conn, cancel context.CancelCauseFunc) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	classID := int64(uint32(daemonLockID >> 32))
	objID := int64(uint32(daemonLockID & 0xffffffff))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkCtx, stop := context.WithTimeout(ctx, 2*time.Second)
			var stillOwned bool
			err := owner.QueryRow(checkCtx, checkDaemonOwnershipSQL, classID, objID).Scan(&stillOwned)
			stop()
			if err != nil {
				if ctx.Err() == nil {
					cancel(fmt.Errorf("database ownership connection lost: %w", err))
				}
				return
			}
			if !stillOwned {
				if ctx.Err() == nil {
					cancel(errors.New("database ownership advisory lock lost"))
				}
				return
			}
		}
	}
}
