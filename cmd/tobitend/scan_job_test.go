package main

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/bootstrap"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func buildTestCBZ(t *testing.T, series, title string, number string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	ci := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<ComicInfo>
  <Series>%s</Series>
  <Title>%s</Title>
  <Number>%s</Number>
</ComicInfo>`, series, title, number)
	wCI, err := zw.Create("ComicInfo.xml")
	if err != nil {
		t.Fatalf("zip create ComicInfo.xml: %v", err)
	}
	if _, err := wCI.Write([]byte(ci)); err != nil {
		t.Fatalf("zip write ComicInfo.xml: %v", err)
	}
	wPage, err := zw.Create("001.jpg")
	if err != nil {
		t.Fatalf("zip create 001.jpg: %v", err)
	}
	if _, err := wPage.Write([]byte("page-bytes-" + series + "-" + number)); err != nil {
		t.Fatalf("zip write 001.jpg: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func writeTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func seedTestLibrary(ctx context.Context, t *testing.T, pool *pgxpool.Pool, name string, rootDir string) (domain.Library, domain.LibraryRoot) {
	t.Helper()
	tx := postgres.NewTransactor(pool)
	var lib domain.Library
	var root domain.LibraryRoot
	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		lib = domain.Library{
			Name: name,
			Mode: domain.LibraryModeVirtual,
		}
		if err := st.Libraries().CreateLibrary(ctx, &lib); err != nil {
			return err
		}
		rp, err := domain.NewRootPathIdentity([]byte(rootDir))
		if err != nil {
			return err
		}
		root = domain.LibraryRoot{
			LibraryID: lib.ID,
			Path:      rp,
			State:     domain.LibraryRootStateOnline,
		}
		return st.Libraries().CreateLibraryRoot(ctx, &root)
	}); err != nil {
		t.Fatalf("seedTestLibrary(%s): %v", name, err)
	}
	return lib, root
}

func waitDaemonJobState(ctx context.Context, t *testing.T, repo storage.JobRepository, id string, want domain.JobState) *domain.Job {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got, err := repo.GetJobByID(ctx, id)
		if err == nil && got.State == want {
			return got
		}
		time.Sleep(15 * time.Millisecond)
	}
	got, err := repo.GetJobByID(ctx, id)
	t.Fatalf("job %s = (%+v, %v), want state %s", id, got, err, want)
	return nil
}

func captureDaemonAssembly(t *testing.T) (**daemonServices, **daemonWorkers) {
	t.Helper()
	var services *daemonServices
	var workers *daemonWorkers

	originalBuild := buildDaemonServices
	originalStart := startDaemonJobs
	buildDaemonServices = func(pool *pgxpool.Pool, setup *bootstrap.Setup, userRepo *postgres.UserRepository) (*daemonServices, error) {
		ds, err := originalBuild(pool, setup, userRepo)
		if err == nil {
			services = ds
		}
		return ds, err
	}
	startDaemonJobs = func(ctx context.Context, cancel context.CancelCauseFunc, pool *pgxpool.Pool, cfg *config.Config, appServices *app.Services) (*daemonWorkers, error) {
		w, err := originalStart(ctx, cancel, pool, cfg, appServices)
		if err == nil {
			workers = w
		}
		return w, err
	}
	t.Cleanup(func() {
		buildDaemonServices = originalBuild
		startDaemonJobs = originalStart
	})
	return &services, &workers
}

func TestRunDaemonScanJob_ProductionWiringCatalogueMissingAndProgress(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)

	originalLogger := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(originalLogger)
	})

	dsPtr, workersPtr := captureDaemonAssembly(t)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "manga")
	file1 := filepath.Join(rootDir, "Horizon/Vol01.cbz")
	file2 := filepath.Join(rootDir, "Horizon/Vol02.cbz")
	writeTestFile(t, file1, buildTestCBZ(t, "Horizon", "First", "1"))
	writeTestFile(t, file2, buildTestCBZ(t, "Horizon", "Second", "2"))

	lib, root := seedTestLibrary(ctx, t, db.Pool, "Prod Scan Library", rootDir)
	jobRepo := postgres.NewJobRepository(db.Pool)

	err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		workers := *workersPtr
		ds := *dsPtr
		if workers == nil || workers.runner == nil || workers.progressHub == nil {
			return errors.New("daemonWorkers not initialized")
		}
		if ds == nil || ds.triggerScan == nil {
			return errors.New("daemonServices.triggerScan not wired in runDaemon")
		}
		if sz, active := workers.runner.ActivePoolSize(domain.JobKindScan); !active || sz != jobs.DefaultWorkerPoolSize {
			return fmt.Errorf("SCAN ActivePoolSize = (%d, %v), want (%d, true)", sz, active, jobs.DefaultWorkerPoolSize)
		}

		adminPrincipal := &domain.Principal{
			User:        domain.User{ID: "admin-observer", Username: "alice"},
			Permissions: []domain.Permission{domain.PermissionJobsReadAll},
		}
		tickCh, unsub, err := workers.progressHub.Subscribe(serveCtx, events.ProgressSubscribeOptions{
			Principal:  adminPrincipal,
			BufferSize: 16,
		})
		if err != nil {
			return err
		}
		defer unsub()

		jobID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return fmt.Errorf("TriggerScan #1: %w", err)
		}
		waitDaemonJobState(serveCtx, t, jobRepo, jobID, domain.JobStateSucceeded)

		// At least one ephemeral progress tick must have arrived with scanned_count >= 1 and current_path.
		var sawTick bool
		deadline := time.Now().Add(2 * time.Second)
		for !sawTick && time.Now().Before(deadline) {
			select {
			case tick := <-tickCh:
				if tick.JobID == jobID && tick.Kind == domain.JobKindScan && tick.ScannedCount >= 1 && tick.CurrentPath != "" {
					sawTick = true
				}
			case <-time.After(50 * time.Millisecond):
			}
		}
		if !sawTick {
			return errors.New("did not observe ephemeral job.progress tick for production SCAN job")
		}

		// Verify populated catalogue tables.
		var seriesCount, volumeCount, assetCount, digestCount int
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM series WHERE library_id = $1`, lib.ID).Scan(&seriesCount); err != nil {
			return err
		}
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM volumes WHERE library_id = $1`, lib.ID).Scan(&volumeCount); err != nil {
			return err
		}
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM media_assets WHERE library_root_id = $1 AND missing_since IS NULL`, root.ID).Scan(&assetCount); err != nil {
			return err
		}
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM asset_digests ad JOIN media_assets ma ON ma.id = ad.asset_id WHERE ma.library_root_id = $1 AND ad.is_current`, root.ID).Scan(&digestCount); err != nil {
			return err
		}
		if seriesCount != 1 || volumeCount != 2 || assetCount != 2 || digestCount != 2 {
			return fmt.Errorf("catalogue counts = (series=%d, volumes=%d, assets=%d, digests=%d), want (1, 2, 2, 2)",
				seriesCount, volumeCount, assetCount, digestCount)
		}

		// Delete one file and run a second SCAN job -> missing-file handling sets missing_since and emits asset.missing.
		if err := os.Remove(file2); err != nil {
			return err
		}
		jobID2, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return fmt.Errorf("TriggerScan #2: %w", err)
		}
		waitDaemonJobState(serveCtx, t, jobRepo, jobID2, domain.JobStateSucceeded)

		var missingAssets, missingEvents int
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM media_assets WHERE library_root_id = $1 AND missing_since IS NOT NULL`, root.ID).Scan(&missingAssets); err != nil {
			return err
		}
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM domain_events WHERE kind = 'asset.missing'`).Scan(&missingEvents); err != nil {
			return err
		}
		if missingAssets != 1 || missingEvents != 1 {
			return fmt.Errorf("missing reconciliation = (assets=%d, events=%d), want (1, 1)", missingAssets, missingEvents)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
	}
}

func TestRunDaemonScanJob_DedupeAndPoolIsolationM1BD2(t *testing.T) {
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	t.Run("jobs.pool.scan=1 serializes SCAN jobs while TOBITEN_SCANNER_WORKERS=3 inspects concurrently", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)
		putSetting(t, db, "jobs.pool.scan", "1")

		dsPtr, workersPtr := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootA := filepath.Join(t.TempDir(), "lib-a")
		rootB := filepath.Join(t.TempDir(), "lib-b")
		for i := 1; i <= 3; i++ {
			writeTestFile(t, filepath.Join(rootA, fmt.Sprintf("Alpha/v%02d.cbz", i)), buildTestCBZ(t, "Alpha", fmt.Sprintf("A%d", i), fmt.Sprintf("%d", i)))
		}
		writeTestFile(t, filepath.Join(rootB, "Beta/v01.cbz"), buildTestCBZ(t, "Beta", "B1", "1"))

		libA, _ := seedTestLibrary(ctx, t, db.Pool, "Lib A", rootA)
		libB, _ := seedTestLibrary(ctx, t, db.Pool, "Lib B", rootB)

		var activeInspects atomic.Int32
		var maxActiveInspects atomic.Int32
		libAEntered := make(chan struct{}, 4)
		releaseLibA := make(chan struct{})

		newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
			return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
				Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
					if strings.HasPrefix(string(rawPath), rootA) {
						cur := activeInspects.Add(1)
						defer activeInspects.Add(-1)
						for {
							prev := maxActiveInspects.Load()
							if cur <= prev || maxActiveInspects.CompareAndSwap(prev, cur) {
								break
							}
						}
						libAEntered <- struct{}{}
						select {
						case <-releaseLibA:
						case <-openCtx.Done():
							return nil, openCtx.Err()
						}
					}
					return os.Open(string(rawPath))
				},
			})
		}

		cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
			"TOBITEN_STORAGE_MODE=postgres",
			"TOBITEN_DATABASE_URL=" + db.URL,
			"TOBITEN_ADMIN_USER=alice",
			"TOBITEN_ADMIN_PASS=initial-password",
			"TOBITEN_SCANNER_WORKERS=3",
		}))
		if err != nil {
			t.Fatalf("config.Load: %v", err)
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		err = runDaemon(ctx, cfg, func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			workers := *workersPtr
			ds := *dsPtr
			if sz, active := workers.runner.ActivePoolSize(domain.JobKindScan); !active || sz != 1 {
				return fmt.Errorf("SCAN ActivePoolSize = (%d, %v), want (1, true)", sz, active)
			}

			jobA1, err := ds.triggerScan(serveCtx, libA.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			// Wait until libA's 3 inspection workers all enter concurrently inside the single SCAN job.
			for range 3 {
				select {
				case <-libAEntered:
				case <-time.After(10 * time.Second):
					return errors.New("timed out waiting for 3 concurrent inspection workers inside single SCAN job")
				}
			}

			// Duplicate trigger for libA must return the exact same active job_id.
			jobA2, err := ds.triggerScan(serveCtx, libA.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			if jobA2 != jobA1 {
				return fmt.Errorf("duplicate TriggerScan(libA) = %q, want %q", jobA2, jobA1)
			}

			// Trigger for libB gets a distinct job_id and must remain QUEUED while SCAN pool (size=1) runs libA.
			jobB, err := ds.triggerScan(serveCtx, libB.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			if jobB == jobA1 {
				return fmt.Errorf("TriggerScan(libB) reused jobA1 %q", jobA1)
			}
			time.Sleep(150 * time.Millisecond)
			gotB, err := jobRepo.GetJobByID(serveCtx, jobB)
			if err != nil {
				return err
			}
			if gotB.State != domain.JobStateQueued || gotB.Attempts != 0 {
				return fmt.Errorf("libB job while SCAN pool=1 saturated = (%+v), want QUEUED attempts=0", gotB)
			}

			close(releaseLibA)
			waitDaemonJobState(serveCtx, t, jobRepo, jobA1, domain.JobStateSucceeded)
			waitDaemonJobState(serveCtx, t, jobRepo, jobB, domain.JobStateSucceeded)

			if got := maxActiveInspects.Load(); got != 3 {
				return fmt.Errorf("maxActiveInspects = %d, want 3 from TOBITEN_SCANNER_WORKERS", got)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})

	t.Run("jobs.pool.scan=2 runs 2 SCAN jobs concurrently while TOBITEN_SCANNER_WORKERS=1 serializes inspections per scan", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)
		putSetting(t, db, "jobs.pool.scan", "2")

		dsPtr, workersPtr := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		root1 := filepath.Join(t.TempDir(), "lib-1")
		root2 := filepath.Join(t.TempDir(), "lib-2")
		root3 := filepath.Join(t.TempDir(), "lib-3")
		for idx, r := range []string{root1, root2, root3} {
			writeTestFile(t, filepath.Join(r, "Series/v01.cbz"), buildTestCBZ(t, "S", fmt.Sprintf("V1-%d", idx), "1"))
			writeTestFile(t, filepath.Join(r, "Series/v02.cbz"), buildTestCBZ(t, "S", fmt.Sprintf("V2-%d", idx), "2"))
		}

		lib1, _ := seedTestLibrary(ctx, t, db.Pool, "Concurrent Lib 1", root1)
		lib2, _ := seedTestLibrary(ctx, t, db.Pool, "Concurrent Lib 2", root2)
		lib3, _ := seedTestLibrary(ctx, t, db.Pool, "Concurrent Lib 3", root3)

		var mu sync.Mutex
		activePerRoot := make(map[string]int)
		maxPerRoot := make(map[string]int)
		enteredRoots := make(chan string, 10)
		releaseRoots := make(chan struct{})

		newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
			return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
				Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
					p := string(rawPath)
					var rootKey string
					for _, candidate := range []string{root1, root2, root3} {
						if strings.HasPrefix(p, candidate) {
							rootKey = candidate
							break
						}
					}
					if rootKey != "" {
						mu.Lock()
						activePerRoot[rootKey]++
						if activePerRoot[rootKey] > maxPerRoot[rootKey] {
							maxPerRoot[rootKey] = activePerRoot[rootKey]
						}
						mu.Unlock()
						defer func() {
							mu.Lock()
							activePerRoot[rootKey]--
							mu.Unlock()
						}()
						enteredRoots <- rootKey
						select {
						case <-releaseRoots:
						case <-openCtx.Done():
							return nil, openCtx.Err()
						}
					}
					return os.Open(p)
				},
			})
		}

		cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
			"TOBITEN_STORAGE_MODE=postgres",
			"TOBITEN_DATABASE_URL=" + db.URL,
			"TOBITEN_ADMIN_USER=alice",
			"TOBITEN_ADMIN_PASS=initial-password",
			"TOBITEN_SCANNER_WORKERS=1",
		}))
		if err != nil {
			t.Fatalf("config.Load: %v", err)
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		err = runDaemon(ctx, cfg, func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			workers := *workersPtr
			ds := *dsPtr
			if sz, active := workers.runner.ActivePoolSize(domain.JobKindScan); !active || sz != 2 {
				return fmt.Errorf("SCAN ActivePoolSize = (%d, %v), want (2, true)", sz, active)
			}

			j1, err := ds.triggerScan(serveCtx, lib1.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			j2, err := ds.triggerScan(serveCtx, lib2.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			j3, err := ds.triggerScan(serveCtx, lib3.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}

			// Wait for 2 distinct roots to enter concurrently.
			seenActiveRoots := make(map[string]bool)
			deadline := time.Now().Add(10 * time.Second)
			for len(seenActiveRoots) < 2 && time.Now().Before(deadline) {
				select {
				case rk := <-enteredRoots:
					seenActiveRoots[rk] = true
				case <-time.After(100 * time.Millisecond):
				}
			}
			if len(seenActiveRoots) != 2 {
				return fmt.Errorf("expected 2 concurrent SCAN jobs, saw roots %v", seenActiveRoots)
			}

			// Give time to prove neither job opens a 2nd file concurrently (cap=1) and 3rd job stays QUEUED.
			time.Sleep(150 * time.Millisecond)
			var runningCount, queuedCount int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM jobs WHERE kind = 'SCAN' AND state = 'RUNNING'`).Scan(&runningCount); err != nil {
				return err
			}
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM jobs WHERE kind = 'SCAN' AND state = 'QUEUED'`).Scan(&queuedCount); err != nil {
				return err
			}
			if runningCount != 2 || queuedCount != 1 {
				return fmt.Errorf("while pool=2 saturated: running=%d queued=%d, want 2 and 1", runningCount, queuedCount)
			}

			close(releaseRoots)
			for _, id := range []string{j1, j2, j3} {
				waitDaemonJobState(serveCtx, t, jobRepo, id, domain.JobStateSucceeded)
			}

			mu.Lock()
			defer mu.Unlock()
			for _, rk := range []string{root1, root2, root3} {
				if maxPerRoot[rk] != 1 {
					return fmt.Errorf("root %s max concurrent inspections = %d, want 1", rk, maxPerRoot[rk])
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})
}

type failNTimesTransactor struct {
	inner     storage.Transactor
	failUntil atomic.Int32
	calls     atomic.Int32
	err       error
}

func (f *failNTimesTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	n := f.calls.Add(1)
	if n <= f.failUntil.Load() {
		return f.err
	}
	return f.inner.WithTx(ctx, fn)
}

func (f *failNTimesTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return f.inner.WithReadCommittedTx(ctx, fn)
}

func TestRunDaemonScanJob_RetryBackoffMaxAttemptsAndUnknownLibrary(t *testing.T) {
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	t.Run("unknown library fails permanently without retry and releases dedupe key", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)

		dsPtr, _ := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		jobRepo := postgres.NewJobRepository(db.Pool)

		err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			ds := *dsPtr
			jobID, err := ds.triggerScan(serveCtx, "nonexistent-lib", scanner.TriggerScanOptions{MaxAttempts: 3})
			if err != nil {
				return err
			}
			got := waitDaemonJobState(serveCtx, t, jobRepo, jobID, domain.JobStateFailed)
			if got.Attempts != 1 {
				return fmt.Errorf("unknown library job attempts = %d, want 1 (no retry)", got.Attempts)
			}
			var failedEvents int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.failed'`, jobID).Scan(&failedEvents); err != nil {
				return err
			}
			if failedEvents != 1 {
				return fmt.Errorf("job.failed count = %d, want 1", failedEvents)
			}
			retriggeredID, err := ds.triggerScan(serveCtx, "nonexistent-lib", scanner.TriggerScanOptions{MaxAttempts: 3})
			if err != nil {
				return err
			}
			if retriggeredID == jobID {
				return fmt.Errorf("dedupe key not released after permanent failure: reused %q", jobID)
			}
			waitDaemonJobState(serveCtx, t, jobRepo, retriggeredID, domain.JobStateFailed)
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})

	t.Run("handler error requeues with backoff and no job.failed then succeeds on retry", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)

		dsPtr, _ := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "retry-root")
		writeTestFile(t, filepath.Join(rootDir, "RetrySaga/v01.cbz"), buildTestCBZ(t, "RetrySaga", "One", "1"))
		lib, root := seedTestLibrary(ctx, t, db.Pool, "Retry Lib", rootDir)

		var failingTx *failNTimesTransactor
		newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
			failingTx = &failNTimesTransactor{
				inner: postgres.NewTransactor(pool),
				err:   errors.New("transient database blip"),
			}
			failingTx.failUntil.Store(1)
			return scanner.NewScanner(failingTx, scanner.Options{})
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			ds := *dsPtr
			jobID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{MaxAttempts: 2})
			if err != nil {
				return err
			}

			// Wait for attempt 1 to fail and requeue with backoff.
			deadline := time.Now().Add(5 * time.Second)
			var requeued *domain.Job
			for time.Now().Before(deadline) {
				got, err := jobRepo.GetJobByID(serveCtx, jobID)
				if err == nil && got.State == domain.JobStateQueued && got.Attempts == 1 {
					requeued = got
					break
				}
				time.Sleep(15 * time.Millisecond)
			}
			if requeued == nil {
				got, _ := jobRepo.GetJobByID(serveCtx, jobID)
				return fmt.Errorf("expected requeue after attempt 1 failure, got %+v", got)
			}
			if !requeued.RunAfter.After(requeued.UpdatedAt) {
				return fmt.Errorf("expected backoff RunAfter > UpdatedAt, got run_after=%v updated_at=%v", requeued.RunAfter, requeued.UpdatedAt)
			}
			var failedDuringRetry int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.failed'`, jobID).Scan(&failedDuringRetry); err != nil {
				return err
			}
			if failedDuringRetry != 0 {
				return fmt.Errorf("job.failed appended on retryable attempt 1: count=%d", failedDuringRetry)
			}

			// Attempt 2 runs after 5s backoff and succeeds.
			gotSucc := waitDaemonJobState(serveCtx, t, jobRepo, jobID, domain.JobStateSucceeded)
			if gotSucc.Attempts != 2 {
				return fmt.Errorf("succeeded Attempts = %d, want 2", gotSucc.Attempts)
			}
			var assetCount int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&assetCount); err != nil {
				return err
			}
			if assetCount != 1 {
				return fmt.Errorf("assetCount after retry = %d, want 1", assetCount)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})

	t.Run("exhausting max_attempts ends FAILED with one job.failed event and releases dedupe key", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)

		dsPtr, _ := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "exhaust-root")
		writeTestFile(t, filepath.Join(rootDir, "Exhaust/v01.cbz"), buildTestCBZ(t, "Exhaust", "One", "1"))
		lib, _ := seedTestLibrary(ctx, t, db.Pool, "Exhaust Lib", rootDir)

		newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
			failingTx := &failNTimesTransactor{
				inner: postgres.NewTransactor(pool),
				err:   errors.New("persistent scan failure"),
			}
			failingTx.failUntil.Store(100)
			return scanner.NewScanner(failingTx, scanner.Options{})
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			ds := *dsPtr
			jobID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{MaxAttempts: 2})
			if err != nil {
				return err
			}

			gotFailed := waitDaemonJobState(serveCtx, t, jobRepo, jobID, domain.JobStateFailed)
			if gotFailed.Attempts != 2 || gotFailed.FinishedAt == nil {
				return fmt.Errorf("exhausted job = %+v, want FAILED attempts=2 finished_at!=nil", gotFailed)
			}
			var failedEvents int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.failed'`, jobID).Scan(&failedEvents); err != nil {
				return err
			}
			if failedEvents != 1 {
				return fmt.Errorf("job.failed count = %d, want 1", failedEvents)
			}

			// Dedupe key must be released so a fresh trigger gets a new job ID.
			newJobID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{MaxAttempts: 2})
			if err != nil {
				return err
			}
			if newJobID == jobID {
				return fmt.Errorf("dedupe key not released after exhaustion: reused %q", jobID)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})

	t.Run("incomplete ONLINE walk retries with backoff then succeeds on recovery", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)

		dsPtr, _ := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "incomplete-root")
		blockedDir := filepath.Join(rootDir, "Blocked")
		writeTestFile(t, filepath.Join(blockedDir, "v01.cbz"), buildTestCBZ(t, "BlockedSaga", "One", "1"))
		writeTestFile(t, filepath.Join(rootDir, "Good/v02.cbz"), buildTestCBZ(t, "GoodSaga", "Two", "2"))
		lib, root := seedTestLibrary(ctx, t, db.Pool, "Incomplete Daemon Lib", rootDir)

		var blockSubdir atomic.Bool
		blockSubdir.Store(true)
		newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
			return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
				ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
					if blockSubdir.Load() && string(rawDir) == blockedDir {
						return nil, os.ErrPermission
					}
					return os.ReadDir(string(rawDir))
				},
			})
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			ds := *dsPtr
			jobID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{MaxAttempts: 2})
			if err != nil {
				return err
			}

			deadline := time.Now().Add(5 * time.Second)
			var requeued *domain.Job
			for time.Now().Before(deadline) {
				got, err := jobRepo.GetJobByID(serveCtx, jobID)
				if err == nil && got.State == domain.JobStateQueued && got.Attempts == 1 {
					requeued = got
					break
				}
				time.Sleep(15 * time.Millisecond)
			}
			if requeued == nil {
				got, _ := jobRepo.GetJobByID(serveCtx, jobID)
				return fmt.Errorf("expected incomplete ONLINE walk requeue after attempt 1, got %+v", got)
			}
			var assetCountBefore int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&assetCountBefore); err != nil {
				return err
			}
			if assetCountBefore != 0 {
				return fmt.Errorf("assets after incomplete walk = %d, want 0", assetCountBefore)
			}

			blockSubdir.Store(false)
			gotSucc := waitDaemonJobState(serveCtx, t, jobRepo, jobID, domain.JobStateSucceeded)
			if gotSucc.Attempts != 2 {
				return fmt.Errorf("recovered walk Attempts = %d, want 2", gotSucc.Attempts)
			}
			var assetCountAfter int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&assetCountAfter); err != nil {
				return err
			}
			if assetCountAfter != 2 {
				return fmt.Errorf("assets after recovered walk = %d, want 2", assetCountAfter)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})

	t.Run("offline library stops cleanly without retrying", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)

		dsPtr, _ := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()

		missingRoot := filepath.Join(t.TempDir(), "unmounted-drive")
		lib, _ := seedTestLibrary(ctx, t, db.Pool, "Offline Daemon Lib", missingRoot)

		jobRepo := postgres.NewJobRepository(db.Pool)
		err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			ds := *dsPtr
			jobID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{MaxAttempts: 3})
			if err != nil {
				return err
			}
			got := waitDaemonJobState(serveCtx, t, jobRepo, jobID, domain.JobStateSucceeded)
			if got.Attempts != 1 {
				return fmt.Errorf("offline library Attempts = %d, want 1", got.Attempts)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})
}
