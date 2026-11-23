package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
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
	originalStartWatchers := startDaemonWatchers
	originalStopWatchers := stopDaemonWatchers
	originalLogger := slog.Default()
	t.Cleanup(func() {
		buildDaemonRegistry = originalRegistryBuilder
		startDaemonWatchers = originalStartWatchers
		stopDaemonWatchers = originalStopWatchers
		slog.SetDefault(originalLogger)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

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
		originalStopWatchers(w)
		record("watcher_stop")
	}

	err := runDaemon(ctx, hostedConfig(t, db.URL), func(_ context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		record("serve")
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
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
