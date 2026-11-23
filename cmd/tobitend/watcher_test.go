package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/roots"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/mount"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRunDaemon_WatchersStartAfterRecoveryAndStopBeforeStorageCleanup(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)

	originalRegistryBuilder := buildDaemonRegistry
	originalNewWatcher := newDaemonWatcher
	originalStartWatchers := startDaemonWatchers
	originalStopWatchers := stopDaemonWatchers
	originalLogger := slog.Default()
	t.Cleanup(func() {
		buildDaemonRegistry = originalRegistryBuilder
		newDaemonWatcher = originalNewWatcher
		startDaemonWatchers = originalStartWatchers
		stopDaemonWatchers = originalStopWatchers
		slog.SetDefault(originalLogger)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	rootDir1 := filepath.Join(t.TempDir(), "slow-nas-1")
	rootDir2 := filepath.Join(t.TempDir(), "slow-nas-2")
	writeTestFile(t, filepath.Join(rootDir1, "Vol01.cbz"), buildTestCBZ(t, "Slow", "One", "1"))
	writeTestFile(t, filepath.Join(rootDir2, "Vol02.cbz"), buildTestCBZ(t, "Slow", "Two", "2"))
	seedTestLibrary(ctx, t, db.Pool, "Slow NAS Library 1", rootDir1)
	seedTestLibrary(ctx, t, db.Pool, "Slow NAS Library 2", rootDir2)

	// Seed an interrupted RUNNING SCAN job so runner.Recover must reconcile it before watchers start.
	jobRepo := postgres.NewJobRepository(db.Pool)
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	seeded := &domain.Job{
		Kind:        domain.JobKindScan,
		Payload:     []byte(`{"library_id":"lib-lifecycle"}`),
		MaxAttempts: 3,
		CreatedAt:   t0,
		RunAfter:    t0,
	}
	if _, err := jobRepo.Enqueue(ctx, seeded); err != nil {
		t.Fatalf("Enqueue seeded job: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindScan, t0.Add(time.Second)); err != nil {
		t.Fatalf("ClaimNext seeded job: %v", err)
	}

	var mu sync.Mutex
	var order []string
	record := func(step string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, step)
	}

	var dbPool *pgxpool.Pool

	buildDaemonRegistry = func(cfg *config.Config, services *app.Services, _ RegistryDeps) module.Registry {
		reg := BuildRegistryWithServices(cfg, services)
		_ = reg.Register(&module.SimpleModule{
			ModuleName: "test-lifecycle-probe",
			Jobs: []module.JobKind{
				module.JobKindSpec{
					Kind:             domain.JobKindScan,
					Policy:           domain.JobResumeRestart,
					RequireReconcile: true,
					ReconcileHook: domain.JobReconcileFunc(func(hookCtx context.Context, job domain.Job) (domain.JobReconcileOutcome, error) {
						record("recover")
						return domain.JobReconcileOutcome{Action: domain.JobReconcileFailed, Error: "test-reconciled"}, nil
					}),
				},
			},
		})
		return reg
	}

	probeEntered := make(chan struct{})
	releaseProbe := make(chan struct{})
	var releaseProbeOnce sync.Once
	releaseProbeFn := func() {
		releaseProbeOnce.Do(func() { close(releaseProbe) })
	}
	t.Cleanup(releaseProbeFn)

	var probeOnce sync.Once
	var probeCalls atomic.Int32
	var watcherRunCtx atomic.Pointer[context.Context]

	newDaemonWatcher = func(pool *pgxpool.Pool, cfg *config.Config, ds *daemonServices) (*scanner.Watcher, error) {
		repo := postgres.NewLibraryRepository(pool)
		return scanner.NewWatcher(scanner.WatcherOptions{
			Roots: contextCapturingRootLister{
				inner: repo,
				onList: func(listCtx context.Context) {
					watcherRunCtx.CompareAndSwap(nil, &listCtx)
				},
			},
			Trigger:      ds.jobs,
			Jobs:         postgres.NewJobRepository(pool),
			PollInterval: 50 * time.Millisecond,
			MountDetector: scanner.MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
				probeCalls.Add(1)
				probeOnce.Do(func() {
					close(probeEntered)
					select {
					case <-releaseProbe:
					case <-ctx.Done():
					}
				})
				return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
			}),
		})
	}

	startDaemonWatchers = func(wCtx context.Context, pool *pgxpool.Pool, cfg *config.Config, ds *daemonServices) (*daemonWatchers, error) {
		mu.Lock()
		recovered := slices.Contains(order, "recover")
		mu.Unlock()
		if !recovered {
			return nil, errors.New("startDaemonWatchers ran before job recovery completed")
		}
		dbPool = pool
		w, err := originalStartWatchers(wCtx, pool, cfg, ds)
		if err != nil {
			return nil, err
		}
		record("watcher_start")
		return w, nil
	}

	stopDaemonWatchers = func(w *daemonWatchers) {
		if dbPool == nil {
			t.Error("expected live pool before watcher stop")
		} else if err := dbPool.Ping(context.Background()); err != nil {
			t.Errorf("storage pool already closed before watcher stop: %v", err)
		}
		if pids := queryDaemonLockPIDs(t, db); len(pids) != 1 {
			t.Errorf("expected daemon advisory lock held during watcher stop, got pids=%v", pids)
		}
		// Start watcher shutdown while the first mount probe is still blocked, verify Stop
		// cancels the watcher context immediately and waits to join the in-flight probe,
		// then release the first probe so any second root is skipped.
		stopDone := make(chan struct{})
		go func() {
			originalStopWatchers(w)
			close(stopDone)
		}()
		if runCtxPtr := watcherRunCtx.Load(); runCtxPtr == nil {
			t.Error("expected watcher run context captured before mount probe")
		} else {
			select {
			case <-(*runCtxPtr).Done():
			case <-time.After(2 * time.Second):
				t.Error("watcher Stop did not cancel watcher context while first mount probe was blocked")
			}
		}
		select {
		case <-stopDone:
			t.Error("watcher Stop returned before blocked first mount probe finished")
		default:
		}
		releaseProbeFn()
		select {
		case <-stopDone:
		case <-time.After(3 * time.Second):
			t.Error("watcher Stop timed out during shutdown")
		}
		if dbPool != nil {
			if err := dbPool.Ping(context.Background()); err != nil {
				t.Errorf("storage pool closed before watcher join finished: %v", err)
			}
		}
		if pids := queryDaemonLockPIDs(t, db); len(pids) != 1 {
			t.Errorf("expected daemon advisory lock still held right after watcher stop, got pids=%v", pids)
		}
		record("watcher_stop")
	}

	err := runDaemon(ctx, hostedConfig(t, db.URL), func(_ context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		record("serve")
		// HTTP serving must begin before the held initial mount probe returns.
		select {
		case <-probeEntered:
		case <-time.After(2 * time.Second):
			return errors.New("background watcher did not reach initial mount probe")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
	}

	if gotProbes := probeCalls.Load(); gotProbes != 1 {
		t.Fatalf("mount probe calls = %d, want 1 (second root must not be probed after shutdown begins)", gotProbes)
	}

	if pids := queryDaemonLockPIDs(t, db); len(pids) != 0 {
		t.Fatalf("daemon advisory lock remained held after shutdown: pids=%v", pids)
	}

	wantOrder := []string{"recover", "watcher_start", "serve", "watcher_stop"}
	mu.Lock()
	gotOrder := slices.Clone(order)
	mu.Unlock()
	if !slices.Equal(gotOrder, wantOrder) {
		t.Fatalf("lifecycle order = %v, want %v", gotOrder, wantOrder)
	}
}

type contextCapturingRootLister struct {
	inner  scanner.RootLister
	onList func(context.Context)
}

func (c contextCapturingRootLister) ListAllLibraryRoots(ctx context.Context) ([]domain.LibraryRoot, error) {
	if c.onList != nil {
		c.onList(ctx)
	}
	return c.inner.ListAllLibraryRoots(ctx)
}

func TestRunDaemon_PollingWatcherTriggersDurableAutomaticScan(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)

	originalNewWatcher := newDaemonWatcher
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonWatcher = originalNewWatcher
		slog.SetDefault(originalLogger)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "nas-manga")
	writeTestFile(t, filepath.Join(rootDir, "Horizon/Vol01.cbz"), buildTestCBZ(t, "Horizon", "One", "1"))
	lib, _ := seedTestLibrary(ctx, t, db.Pool, "NAS Polling Library", rootDir)

	newDaemonWatcher = func(pool *pgxpool.Pool, _ *config.Config, ds *daemonServices) (*scanner.Watcher, error) {
		return scanner.NewWatcher(scanner.WatcherOptions{
			Roots:   postgres.NewLibraryRepository(pool),
			Trigger: ds.jobs,
			MountDetector: scanner.MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
				return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
			}),
			PollInterval:   15 * time.Millisecond,
			DebounceWindow: 45 * time.Millisecond,
			Logger:         slog.Default(),
		})
	}

	jobRepo := postgres.NewJobRepository(db.Pool)

	err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		kindScan := domain.JobKindScan
		// Unchanged baseline must not enqueue any SCAN across multiple ticks.
		time.Sleep(75 * time.Millisecond)
		existing, err := jobRepo.ListJobs(serveCtx, storage.JobFilter{Kind: &kindScan})
		if err != nil {
			return err
		}
		if len(existing) != 0 {
			return fmt.Errorf("expected 0 baseline SCAN jobs, got %d", len(existing))
		}

		// Create multiple files within one debounce window -> single automatic SCAN.
		writeTestFile(t, filepath.Join(rootDir, "Horizon/Vol02.cbz"), buildTestCBZ(t, "Horizon", "Two", "2"))
		writeTestFile(t, filepath.Join(rootDir, "Horizon/Vol03.cbz"), buildTestCBZ(t, "Horizon", "Three", "3"))

		var scanJobs []domain.Job
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			scanJobs, err = jobRepo.ListJobs(serveCtx, storage.JobFilter{Kind: &kindScan})
			if err != nil {
				return err
			}
			if len(scanJobs) > 0 && scanJobs[0].State == domain.JobStateSucceeded {
				break
			}
			time.Sleep(15 * time.Millisecond)
		}
		if len(scanJobs) != 1 {
			return fmt.Errorf("got %d SCAN jobs after burst, want 1", len(scanJobs))
		}
		job := scanJobs[0]
		if job.State != domain.JobStateSucceeded {
			return fmt.Errorf("SCAN job state = %s, want SUCCEEDED", job.State)
		}
		if job.CreatedBy != nil {
			return fmt.Errorf("SCAN job CreatedBy = %v, want nil", *job.CreatedBy)
		}
		wantDedupe := "SCAN:" + lib.ID
		if job.DedupeKey == nil || *job.DedupeKey != wantDedupe {
			return fmt.Errorf("SCAN job DedupeKey = %v, want %q", job.DedupeKey, wantDedupe)
		}

		// Verify newly added files were indexed into the catalogue.
		var volumeCount int
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM volumes WHERE library_id = $1`, lib.ID).Scan(&volumeCount); err != nil {
			return err
		}
		if volumeCount != 3 {
			return fmt.Errorf("indexed volumes = %d, want 3", volumeCount)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
	}
}

func TestRunDaemon_PollingWatcherRetriesAfterRelocationAndRunningScan(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)

	originalNewWatcher := newDaemonWatcher
	originalNewRelocator := newDaemonRelocator
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonWatcher = originalNewWatcher
		newDaemonRelocator = originalNewRelocator
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "nas-retry")
	relocTarget := filepath.Join(t.TempDir(), "nas-retry-reloc")
	writeTestFile(t, filepath.Join(rootDir, "Horizon/Vol01.cbz"), buildTestCBZ(t, "Horizon", "One", "1"))
	writeTestFile(t, filepath.Join(relocTarget, "Horizon/Vol01.cbz"), buildTestCBZ(t, "Horizon", "One", "1"))
	lib, root := seedTestLibrary(ctx, t, db.Pool, "Retry Polling Library", rootDir)

	relocStarted := make(chan struct{})
	releaseReloc := make(chan struct{})
	var releaseRelocOnce sync.Once
	releaseRelocFn := func() {
		releaseRelocOnce.Do(func() { close(releaseReloc) })
	}
	t.Cleanup(releaseRelocFn)

	var relocOnce sync.Once
	newDaemonRelocator = func(pool *pgxpool.Pool, _ *config.Config) (*roots.Relocator, error) {
		return roots.NewRelocator(postgres.NewTransactor(pool), roots.RelocatorOptions{
			OnBeforeVerify: func(hookCtx context.Context, _ domain.Job) error {
				relocOnce.Do(func() {
					close(relocStarted)
					select {
					case <-releaseReloc:
					case <-hookCtx.Done():
					case <-ctx.Done():
					}
				})
				return nil
			},
		})
	}

	scanHoldEntered := make(chan struct{})
	releaseHeldScan := make(chan struct{})
	var releaseHeldScanOnce sync.Once
	releaseHeldScanFn := func() {
		releaseHeldScanOnce.Do(func() { close(releaseHeldScan) })
	}
	t.Cleanup(releaseHeldScanFn)

	var holdScanOnce sync.Once
	var holdFirstRead atomic.Bool
	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
			ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
				if holdFirstRead.Load() {
					holdScanOnce.Do(func() {
						close(scanHoldEntered)
						select {
						case <-releaseHeldScan:
						case <-ctx.Done():
						}
					})
				}
				return os.ReadDir(string(rawDir))
			},
		})
	}

	var (
		trigMu        sync.Mutex
		triggerTrials int
	)
	getTriggerTrials := func() int {
		trigMu.Lock()
		defer trigMu.Unlock()
		return triggerTrials
	}

	newDaemonWatcher = func(pool *pgxpool.Pool, _ *config.Config, ds *daemonServices) (*scanner.Watcher, error) {
		return scanner.NewWatcher(scanner.WatcherOptions{
			Roots:          postgres.NewLibraryRepository(pool),
			Trigger:        ds.jobs,
			Jobs:           postgres.NewJobRepository(pool),
			PollInterval:   15 * time.Millisecond,
			DebounceWindow: 30 * time.Millisecond,
			RetryInterval:  30 * time.Millisecond,
			MountDetector: scanner.MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
				return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
			}),
			OnTrigger: func(libraryID, _ string, _ error) {
				if libraryID == lib.ID {
					trigMu.Lock()
					triggerTrials++
					trigMu.Unlock()
				}
			},
		})
	}

	dsPtr, _ := captureDaemonAssembly(t)
	jobRepo := postgres.NewJobRepository(db.Pool)

	err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		defer releaseRelocFn()
		defer releaseHeldScanFn()

		ds := *dsPtr
		kindScan := domain.JobKindScan

		// Let the background watcher establish its initial baseline.
		time.Sleep(50 * time.Millisecond)

		// Phase 1: Active RELOCATE_ROOT rejects SCAN; watcher retains pending change and retries once relocation ends.
		relocJobID, err := roots.TriggerRelocateRoot(serveCtx, ds.jobs, root.LibraryID, root.ID, []byte(relocTarget), roots.TriggerRelocateOptions{})
		if err != nil {
			return fmt.Errorf("TriggerRelocateRoot: %w", err)
		}
		select {
		case <-relocStarted:
		case <-time.After(3 * time.Second):
			return errors.New("relocation job did not enter OnBeforeVerify")
		}

		writeTestFile(t, filepath.Join(rootDir, "Horizon/Vol02.cbz"), buildTestCBZ(t, "Horizon", "Two", "2"))
		writeTestFile(t, filepath.Join(relocTarget, "Horizon/Vol02.cbz"), buildTestCBZ(t, "Horizon", "Two", "2"))

		deadline := time.Now().Add(3 * time.Second)
		for getTriggerTrials() < 2 && time.Now().Before(deadline) {
			time.Sleep(15 * time.Millisecond)
		}
		if getTriggerTrials() < 2 {
			return fmt.Errorf("expected at least 2 deferred trigger retries during relocation, got %d", getTriggerTrials())
		}
		scansDuringReloc, err := jobRepo.ListJobs(serveCtx, storage.JobFilter{Kind: &kindScan})
		if err != nil {
			return err
		}
		if len(scansDuringReloc) != 0 {
			return fmt.Errorf("expected 0 SCAN jobs during active relocation, got %d", len(scansDuringReloc))
		}

		releaseRelocFn()
		waitDaemonJobState(serveCtx, t, jobRepo, relocJobID, domain.JobStateSucceeded)

		var scanJobs []domain.Job
		deadline = time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			scanJobs, err = jobRepo.ListJobs(serveCtx, storage.JobFilter{Kind: &kindScan})
			if err != nil {
				return err
			}
			if len(scanJobs) == 1 && scanJobs[0].State == domain.JobStateSucceeded {
				break
			}
			time.Sleep(15 * time.Millisecond)
		}
		if len(scanJobs) != 1 || scanJobs[0].State != domain.JobStateSucceeded {
			return fmt.Errorf("after relocation: scanJobs = %+v, want 1 SUCCEEDED", scanJobs)
		}
		if scanJobs[0].CreatedBy != nil {
			return fmt.Errorf("after relocation: CreatedBy = %v, want nil", *scanJobs[0].CreatedBy)
		}

		var volumeCount int
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM volumes WHERE library_id = $1`, lib.ID).Scan(&volumeCount); err != nil {
			return err
		}
		if volumeCount != 2 {
			return fmt.Errorf("volumes after post-relocation scan = %d, want 2", volumeCount)
		}

		// Phase 2: Change observed while an older SCAN is already RUNNING retries and triggers one follow-up SCAN.
		holdFirstRead.Store(true)
		manualScanID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return fmt.Errorf("manual TriggerScan: %w", err)
		}
		select {
		case <-scanHoldEntered:
		case <-time.After(3 * time.Second):
			return errors.New("held running scan did not enter ReadDir")
		}

		trialsBefore := getTriggerTrials()
		writeTestFile(t, filepath.Join(relocTarget, "Horizon/Vol03.cbz"), buildTestCBZ(t, "Horizon", "Three", "3"))

		deadline = time.Now().Add(3 * time.Second)
		for getTriggerTrials() < trialsBefore+2 && time.Now().Before(deadline) {
			time.Sleep(15 * time.Millisecond)
		}
		if getTriggerTrials() < trialsBefore+2 {
			return fmt.Errorf("expected at least 2 deferred trigger retries while SCAN running, got %d", getTriggerTrials()-trialsBefore)
		}

		holdFirstRead.Store(false)
		releaseHeldScanFn()
		waitDaemonJobState(serveCtx, t, jobRepo, manualScanID, domain.JobStateSucceeded)

		deadline = time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			scanJobs, err = jobRepo.ListJobs(serveCtx, storage.JobFilter{Kind: &kindScan})
			if err != nil {
				return err
			}
			if len(scanJobs) >= 3 && scanJobs[0].State == domain.JobStateSucceeded && scanJobs[1].State == domain.JobStateSucceeded && scanJobs[2].State == domain.JobStateSucceeded {
				break
			}
			time.Sleep(15 * time.Millisecond)
		}
		if len(scanJobs) != 3 {
			return fmt.Errorf("after running scan finished: got %d SCAN jobs, want 3", len(scanJobs))
		}
		// ListJobs orders newest-first, so scanJobs[0] is the automatic follow-up.
		followUp := scanJobs[0]
		if followUp.ID == manualScanID || followUp.State != domain.JobStateSucceeded || followUp.CreatedBy != nil {
			return fmt.Errorf("unexpected follow-up SCAN job: %+v (manual=%s)", followUp, manualScanID)
		}

		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM volumes WHERE library_id = $1`, lib.ID).Scan(&volumeCount); err != nil {
			return err
		}
		if volumeCount != 3 {
			return fmt.Errorf("volumes after follow-up scan = %d, want 3", volumeCount)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
	}
}

func TestRunDaemon_ConfiguredPollIntervalReachesProductionWatcher(t *testing.T) {
	t.Run("valid system_settings override reaches production watcher", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			raw  string
			want time.Duration
		}{
			{name: "duration string", raw: "25ms", want: 25 * time.Millisecond},
			{name: "integer seconds", raw: "30", want: 30 * time.Second},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db := testdb.New(t)
				migrateStartupDB(t, db)
				putSetting(t, db, scanner.PollIntervalSettingKey, tc.raw)

				originalStartWatchers := startDaemonWatchers
				originalLogger := slog.Default()
				t.Cleanup(func() {
					startDaemonWatchers = originalStartWatchers
					slog.SetDefault(originalLogger)
				})

				var observedInterval time.Duration
				startDaemonWatchers = func(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, ds *daemonServices) (*daemonWatchers, error) {
					w, err := originalStartWatchers(ctx, pool, cfg, ds)
					if err == nil && w != nil && w.mount != nil {
						observedInterval = w.mount.PollInterval()
					}
					return w, err
				}

				err := runDaemon(t.Context(), hostedConfig(t, db.URL), func(context.Context, *config.Config, *slog.Logger, *app.Services) error {
					return nil
				})
				if err != nil {
					t.Fatalf("runDaemon err = %v", err)
				}
				if observedInterval != tc.want {
					t.Fatalf("production watcher PollInterval = %v, want %v", observedInterval, tc.want)
				}
			})
		}
	})

	t.Run("invalid and overflowing system_settings poll intervals fail startup", func(t *testing.T) {
		for _, raw := range []string{"0s", "-5s", "0", "-1", "18446744074", "99999999999999999999", "not-a-duration"} {
			t.Run("raw_"+raw, func(t *testing.T) {
				db := testdb.New(t)
				migrateStartupDB(t, db)
				putSetting(t, db, scanner.PollIntervalSettingKey, raw)

				originalLogger := slog.Default()
				t.Cleanup(func() {
					slog.SetDefault(originalLogger)
				})

				err := runDaemon(t.Context(), hostedConfig(t, db.URL), func(context.Context, *config.Config, *slog.Logger, *app.Services) error {
					return nil
				})
				if !errors.Is(err, scanner.ErrInvalidPollInterval) {
					t.Fatalf("runDaemon(%q) err = %v, want ErrInvalidPollInterval", raw, err)
				}
			})
		}
	})
}
