package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/api/middleware"
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
	buildDaemonServices = func(pool *pgxpool.Pool, cfg *config.Config, setup *bootstrap.Setup, userRepo *postgres.UserRepository) (*daemonServices, error) {
		ds, err := originalBuild(pool, cfg, setup, userRepo)
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

		var blockRoot atomic.Bool
		blockRoot.Store(true)
		var rootReads atomic.Int32
		newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
			return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
				ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
					if blockRoot.Load() && string(rawDir) == rootDir && rootReads.Add(1) >= 2 {
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

			blockRoot.Store(false)
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

type daemonAssetCommitHookTransactor struct {
	inner            storage.Transactor
	createdAssets    atomic.Int32
	onAfterCommit    func(ctx context.Context, createCount int32)
	blockNextReadTx  atomic.Bool
	onBlockedNextTx  func()
	unblockNextReadT chan struct{}
}

func (h *daemonAssetCommitHookTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return h.inner.WithTx(ctx, fn)
}

func (h *daemonAssetCommitHookTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	if h.blockNextReadTx.Load() {
		if h.onBlockedNextTx != nil {
			h.onBlockedNextTx()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.unblockNextReadT:
		}
	}
	var created bool
	err := h.inner.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		return fn(&daemonAssetCommitHookTx{Tx: tx, created: &created})
	})
	if err == nil && created {
		n := h.createdAssets.Add(1)
		if h.onAfterCommit != nil {
			h.onAfterCommit(ctx, n)
		}
	}
	return err
}

type daemonAssetCommitHookTx struct {
	storage.Tx
	created *bool
}

func (h *daemonAssetCommitHookTx) CatalogueWrites() storage.CatalogueWriteRepository {
	return &daemonAssetCommitHookRepo{
		CatalogueWriteRepository: h.Tx.CatalogueWrites(),
		created:                  h.created,
	}
}

type daemonAssetCommitHookRepo struct {
	storage.CatalogueWriteRepository
	created *bool
}

func (r *daemonAssetCommitHookRepo) CreateAsset(ctx context.Context, asset *domain.MediaAsset) error {
	if err := r.CatalogueWriteRepository.CreateAsset(ctx, asset); err != nil {
		return err
	}
	*r.created = true
	return nil
}

func TestRunDaemonScanJob_CancelAtFileBoundary(t *testing.T) {
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	t.Run("cancel mid-walk stops at next file boundary without missing marks or partial move/modify", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)
		dsPtr, _ := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "cancel-walk-root")
		existingPath := filepath.Join(rootDir, "Existing/v01.cbz")
		modifyPath := filepath.Join(rootDir, "Modify/m01.cbz")
		oldMovePath := filepath.Join(rootDir, "Move/old.cbz")
		renamedMovePath := filepath.Join(rootDir, "Move/renamed.cbz")
		gonePath := filepath.Join(rootDir, "Vanish/gone.cbz")
		blockNewPath := filepath.Join(rootDir, "Zebra/01_block.cbz")
		pipelineNewPath := filepath.Join(rootDir, "Zebra/02_pipeline.cbz")
		unopenedNewPath := filepath.Join(rootDir, "Zebra/03_unopened.cbz")

		writeTestFile(t, existingPath, buildTestCBZ(t, "Existing", "One", "1"))
		writeTestFile(t, modifyPath, buildTestCBZ(t, "Modify", "One", "1"))
		writeTestFile(t, oldMovePath, buildTestCBZ(t, "Move", "One", "1"))
		writeTestFile(t, gonePath, buildTestCBZ(t, "Vanish", "One", "1"))

		lib, root := seedTestLibrary(ctx, t, db.Pool, "Daemon Cancel Walk Lib", rootDir)

		var blockActive atomic.Bool
		enteredBlock := make(chan struct{}, 1)
		releaseBlock := make(chan struct{})
		var openMu sync.Mutex
		openedOnCancel := make(map[string]int)

		newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
			return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
				Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
					p := string(rawPath)
					if blockActive.Load() {
						openMu.Lock()
						openedOnCancel[p]++
						openMu.Unlock()
						if p == blockNewPath {
							select {
							case enteredBlock <- struct{}{}:
							default:
							}
							select {
							case <-releaseBlock:
							case <-openCtx.Done():
								return nil, openCtx.Err()
							}
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
		err = runDaemon(ctx, cfg, func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, services *app.Services) error {
			ds := *dsPtr
			baseID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			waitDaemonJobState(serveCtx, t, jobRepo, baseID, domain.JobStateSucceeded)

			var origModDigest string
			if err := db.QueryRow(serveCtx, `
				SELECT ad.value FROM asset_digests ad
				JOIN media_assets ma ON ma.id = ad.asset_id
				WHERE ma.library_root_id = $1 AND ma.rel_path_display = 'Modify/m01.cbz' AND ad.is_current
			`, root.ID).Scan(&origModDigest); err != nil {
				return err
			}

			if err := os.Rename(oldMovePath, renamedMovePath); err != nil {
				return err
			}
			writeTestFile(t, modifyPath, buildTestCBZ(t, "Modify", "One Updated", "1"))
			if err := os.Remove(gonePath); err != nil {
				return err
			}
			writeTestFile(t, blockNewPath, buildTestCBZ(t, "Zebra", "Z1", "1"))
			writeTestFile(t, pipelineNewPath, buildTestCBZ(t, "Zebra", "Z2", "2"))
			writeTestFile(t, unopenedNewPath, buildTestCBZ(t, "Zebra", "Z3", "3"))

			blockActive.Store(true)
			cancelJobID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}

			select {
			case <-enteredBlock:
			case <-time.After(10 * time.Second):
				return errors.New("timed out waiting for Open(Zebra/01_block.cbz)")
			}

			adminPrincipal := &domain.Principal{
				User:        domain.User{ID: "admin-canceller", Username: "alice"},
				Permissions: []domain.Permission{domain.PermissionJobsReadAll, domain.PermissionJobsCancelAll},
			}
			cancelSnap, err := services.Jobs.CancelJob(serveCtx, adminPrincipal, cancelJobID)
			if err != nil {
				return fmt.Errorf("CancelJob: %w", err)
			}
			if cancelSnap.State != string(domain.JobStateRunning) || !cancelSnap.CancelRequested {
				return fmt.Errorf("CancelJob snapshot = %+v, want RUNNING with cancel_requested=true", cancelSnap)
			}
			close(releaseBlock)

			got := waitDaemonJobState(serveCtx, t, jobRepo, cancelJobID, domain.JobStateCancelled)
			if got.FinishedAt == nil || got.Attempts != 1 {
				return fmt.Errorf("cancelled job = %+v, want attempts=1 finished_at!=nil", got)
			}

			openMu.Lock()
			pipelineCalls := openedOnCancel[pipelineNewPath]
			unopenedCalls := openedOnCancel[unopenedNewPath]
			existingCalls := openedOnCancel[existingPath]
			openMu.Unlock()
			if pipelineCalls != 0 || unopenedCalls != 0 || existingCalls != 0 {
				return fmt.Errorf("unexpected opens on cancelled walk (workers=1): pipeline=%d unopened=%d existing=%d",
					pipelineCalls, unopenedCalls, existingCalls)
			}

			var assetCount, missingCount, movedOrModifiedCount, cancelEvents int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE missing_since IS NOT NULL) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&assetCount, &missingCount); err != nil {
				return err
			}
			if assetCount != 4 || missingCount != 0 {
				return fmt.Errorf("assets after cancel = (total=%d, missing=%d), want (4, 0)", assetCount, missingCount)
			}
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM domain_events WHERE kind IN ('asset.moved', 'asset.modified', 'asset.missing')`).Scan(&movedOrModifiedCount); err != nil {
				return err
			}
			if movedOrModifiedCount != 0 {
				return fmt.Errorf("partial-walk reconcile events = %d, want 0", movedOrModifiedCount)
			}
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.cancelled'`, cancelJobID).Scan(&cancelEvents); err != nil {
				return err
			}
			if cancelEvents != 1 {
				return fmt.Errorf("job.cancelled count = %d, want 1", cancelEvents)
			}

			var curModDigest string
			if err := db.QueryRow(serveCtx, `
				SELECT ad.value FROM asset_digests ad
				JOIN media_assets ma ON ma.id = ad.asset_id
				WHERE ma.library_root_id = $1 AND ma.rel_path_display = 'Modify/m01.cbz' AND ad.is_current
			`, root.ID).Scan(&curModDigest); err != nil {
				return err
			}
			if curModDigest != origModDigest {
				return fmt.Errorf("Modify/m01.cbz digest changed from %q to %q", origModDigest, curModDigest)
			}
			var oldMovePresent bool
			if err := db.QueryRow(serveCtx, `SELECT EXISTS (SELECT 1 FROM media_assets WHERE library_root_id = $1 AND rel_path_display = 'Move/old.cbz')`, root.ID).Scan(&oldMovePresent); err != nil {
				return err
			}
			if !oldMovePresent {
				return errors.New("Move/old.cbz was unexpectedly moved during cancelled partial walk")
			}

			// Dedupe key released -> clean follow-up scan succeeds.
			blockActive.Store(false)
			followID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			if followID == cancelJobID {
				return fmt.Errorf("post-cancel trigger reused job ID %q", cancelJobID)
			}
			waitDaemonJobState(serveCtx, t, jobRepo, followID, domain.JobStateSucceeded)
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})

	t.Run("cancel during write phase keeps committed files intact and skips missing marks", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)
		dsPtr, _ := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "cancel-write-root")
		writeTestFile(t, filepath.Join(rootDir, "Existing/v01.cbz"), buildTestCBZ(t, "Existing", "One", "1"))
		gonePath := filepath.Join(rootDir, "Vanish/gone.cbz")
		writeTestFile(t, gonePath, buildTestCBZ(t, "Vanish", "One", "1"))

		lib, root := seedTestLibrary(ctx, t, db.Pool, "Daemon Cancel Write Lib", rootDir)

		var targetJobID atomic.Value
		var cancelAtCreate atomic.Int32
		var servicesRef atomic.Pointer[app.Services]
		adminPrincipal := &domain.Principal{
			User:        domain.User{ID: "admin-canceller", Username: "alice"},
			Permissions: []domain.Permission{domain.PermissionJobsReadAll, domain.PermissionJobsCancelAll},
		}

		newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
			hookTx := &daemonAssetCommitHookTransactor{
				inner: postgres.NewTransactor(pool),
				onAfterCommit: func(txCtx context.Context, createCount int32) {
					if createCount == cancelAtCreate.Load() {
						svc := servicesRef.Load()
						jid, _ := targetJobID.Load().(string)
						if svc != nil && jid != "" {
							if _, cErr := svc.Jobs.CancelJob(txCtx, adminPrincipal, jid); cErr != nil {
								t.Errorf("CancelJob after commit %d: %v", createCount, cErr)
							}
						}
					}
				},
			}
			return scanner.NewScanner(hookTx, scanner.Options{})
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, services *app.Services) error {
			servicesRef.Store(services)
			ds := *dsPtr

			baseID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			waitDaemonJobState(serveCtx, t, jobRepo, baseID, domain.JobStateSucceeded)

			if err := os.Remove(gonePath); err != nil {
				return err
			}
			writeTestFile(t, filepath.Join(rootDir, "New/a.cbz"), buildTestCBZ(t, "New", "A", "1"))
			writeTestFile(t, filepath.Join(rootDir, "New/b.cbz"), buildTestCBZ(t, "New", "B", "2"))
			writeTestFile(t, filepath.Join(rootDir, "New/c.cbz"), buildTestCBZ(t, "New", "C", "3"))

			cancelAtCreate.Store(3) // 2 baseline + New/a.cbz
			cancelID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			targetJobID.Store(cancelID)

			waitDaemonJobState(serveCtx, t, jobRepo, cancelID, domain.JobStateCancelled)

			var total, missing int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE missing_since IS NOT NULL) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&total, &missing); err != nil {
				return err
			}
			if total != 3 || missing != 0 {
				return fmt.Errorf("assets after write-phase cancel = (total=%d, missing=%d), want (3, 0)", total, missing)
			}
			var hasA bool
			if err := db.QueryRow(serveCtx, `SELECT EXISTS (SELECT 1 FROM media_assets WHERE library_root_id = $1 AND rel_path_display = 'New/a.cbz')`, root.ID).Scan(&hasA); err != nil {
				return err
			}
			if !hasA {
				return errors.New("expected committed New/a.cbz to remain intact")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})

	t.Run("cancel accepted during final write with no other work ends CANCELLED with committed asset and released dedupe key", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)
		dsPtr, _ := captureDaemonAssembly(t)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "cancel-final-write-root")
		soloPath := filepath.Join(rootDir, "Solo/v01.cbz")
		writeTestFile(t, soloPath, buildTestCBZ(t, "Solo", "One", "1"))
		lib, root := seedTestLibrary(ctx, t, db.Pool, "Daemon Cancel Final Write Lib", rootDir)

		var targetJobID atomic.Value
		var cancelIssued atomic.Bool
		var servicesRef atomic.Pointer[app.Services]
		adminPrincipal := &domain.Principal{
			User:        domain.User{ID: "admin-canceller", Username: "alice"},
			Permissions: []domain.Permission{domain.PermissionJobsReadAll, domain.PermissionJobsCancelAll},
		}

		newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
			hookTx := &daemonAssetCommitHookTransactor{
				inner: postgres.NewTransactor(pool),
				onAfterCommit: func(txCtx context.Context, createCount int32) {
					if createCount == 1 && cancelIssued.CompareAndSwap(false, true) {
						svc := servicesRef.Load()
						var jid string
						for jid == "" && txCtx.Err() == nil {
							jid, _ = targetJobID.Load().(string)
							if jid == "" {
								time.Sleep(time.Millisecond)
							}
						}
						if svc != nil && jid != "" {
							snap, cErr := svc.Jobs.CancelJob(txCtx, adminPrincipal, jid)
							if cErr != nil {
								t.Errorf("CancelJob during final write: %v", cErr)
							} else if snap.State != string(domain.JobStateRunning) || !snap.CancelRequested {
								t.Errorf("CancelJob snapshot = %+v, want RUNNING with cancel_requested=true", snap)
							}
						}
					}
				},
			}
			return scanner.NewScanner(hookTx, scanner.Options{})
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, services *app.Services) error {
			servicesRef.Store(services)
			ds := *dsPtr

			cancelID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			targetJobID.Store(cancelID)

			got := waitDaemonJobState(serveCtx, t, jobRepo, cancelID, domain.JobStateCancelled)
			if got.FinishedAt == nil || got.Attempts != 1 {
				return fmt.Errorf("cancelled final-write job = %+v, want attempts=1 finished_at!=nil", got)
			}

			var total, missing, cancelEvents int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE missing_since IS NOT NULL) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&total, &missing); err != nil {
				return err
			}
			if total != 1 || missing != 0 {
				return fmt.Errorf("assets after final-write cancel = (total=%d, missing=%d), want (1, 0)", total, missing)
			}
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.cancelled'`, cancelID).Scan(&cancelEvents); err != nil {
				return err
			}
			if cancelEvents != 1 {
				return fmt.Errorf("job.cancelled count = %d, want 1", cancelEvents)
			}

			followID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
			if err != nil {
				return err
			}
			if followID == cancelID {
				return fmt.Errorf("dedupe key not released after final-write cancel: reused %q", cancelID)
			}
			waitDaemonJobState(serveCtx, t, jobRepo, followID, domain.JobStateSucceeded)
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
	})
}

func TestRunDaemonScanJob_StopMidScanAndRestartRecovery(t *testing.T) {
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	db := testdb.New(t)
	migrateStartupDB(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "stop-restart-root")
	existingPath := filepath.Join(rootDir, "Existing/v01.cbz")
	newAPath := filepath.Join(rootDir, "New/a.cbz")
	newBPath := filepath.Join(rootDir, "New/b.cbz")
	newCPath := filepath.Join(rootDir, "New/c.cbz")
	writeTestFile(t, existingPath, buildTestCBZ(t, "Existing", "One", "1"))

	lib, root := seedTestLibrary(ctx, t, db.Pool, "Daemon Stop Restart Lib", rootDir)
	jobRepo := postgres.NewJobRepository(db.Pool)

	// Daemon 1: baseline scan commits Existing/v01.cbz, then second scan commits New/a.cbz
	// and blocks before New/b.cbz so stopping the daemon leaves the SCAN job RUNNING.
	dsPtr1, _ := captureDaemonAssembly(t)
	var blockOnSecondCreate atomic.Int32
	blockedBeforeNewB := make(chan struct{}, 1)
	unblockCh := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-unblockCh:
		default:
			close(unblockCh)
		}
	})

	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		var hookTx *daemonAssetCommitHookTransactor
		hookTx = &daemonAssetCommitHookTransactor{
			inner:            postgres.NewTransactor(pool),
			unblockNextReadT: unblockCh,
			onAfterCommit: func(_ context.Context, createCount int32) {
				if createCount == blockOnSecondCreate.Load() {
					hookTx.blockNextReadTx.Store(true)
				}
			},
			onBlockedNextTx: func() {
				select {
				case blockedBeforeNewB <- struct{}{}:
				default:
				}
			},
		}
		return scanner.NewScanner(hookTx, scanner.Options{})
	}

	var interruptedJobID string
	err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		ds := *dsPtr1
		baseID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return err
		}
		waitDaemonJobState(serveCtx, t, jobRepo, baseID, domain.JobStateSucceeded)

		writeTestFile(t, newAPath, buildTestCBZ(t, "New", "A", "1"))
		writeTestFile(t, newBPath, buildTestCBZ(t, "New", "B", "2"))
		writeTestFile(t, newCPath, buildTestCBZ(t, "New", "C", "3"))

		blockOnSecondCreate.Store(2) // 1 baseline + New/a.cbz
		interruptedJobID, err = ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return err
		}

		select {
		case <-blockedBeforeNewB:
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for scan #2 to commit New/a.cbz and reach New/b.cbz")
		}
		// Returning from serve cancels runDaemon's worker context and stops workers mid-scan.
		return nil
	})
	if err != nil {
		t.Fatalf("daemon 1 runDaemon err = %v", err)
	}

	midJob, err := jobRepo.GetJobByID(ctx, interruptedJobID)
	if err != nil {
		t.Fatalf("GetJobByID mid-stop: %v", err)
	}
	if midJob.State != domain.JobStateRunning || midJob.Attempts != 1 {
		t.Fatalf("job after daemon stop = (state=%s, attempts=%d), want (RUNNING, 1)", midJob.State, midJob.Attempts)
	}

	// Daemon 2: startup recovery requeues the RUNNING job and completes without re-opening committed unchanged files.
	var openMu sync.Mutex
	openedOnRestart := make(map[string]int)
	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				p := string(rawPath)
				openMu.Lock()
				openedOnRestart[p]++
				openMu.Unlock()
				return os.Open(p)
			},
		})
	}

	err = runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		got := waitDaemonJobState(serveCtx, t, jobRepo, interruptedJobID, domain.JobStateSucceeded)
		if got.Attempts != 2 {
			return fmt.Errorf("recovered job Attempts = %d, want 2", got.Attempts)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("daemon 2 runDaemon err = %v", err)
	}

	openMu.Lock()
	existingOpens := openedOnRestart[existingPath]
	newAOpens := openedOnRestart[newAPath]
	newBOpens := openedOnRestart[newBPath]
	newCOpens := openedOnRestart[newCPath]
	openMu.Unlock()
	if existingOpens != 0 || newAOpens != 0 || newBOpens != 1 || newCOpens != 1 {
		returnErr := fmt.Sprintf("restart opens = (existing=%d, newA=%d, newB=%d, newC=%d), want (0, 0, 1, 1)",
			existingOpens, newAOpens, newBOpens, newCOpens)
		t.Fatal(returnErr)
	}

	var totalAssets, missingAssets, missingEvents int
	if err := db.QueryRow(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE missing_since IS NOT NULL) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&totalAssets, &missingAssets); err != nil {
		t.Fatalf("count media_assets: %v", err)
	}
	if totalAssets != 4 || missingAssets != 0 {
		t.Fatalf("final media_assets = (total=%d, missing=%d), want (4, 0)", totalAssets, missingAssets)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE kind = 'asset.missing'`).Scan(&missingEvents); err != nil {
		t.Fatalf("count asset.missing: %v", err)
	}
	if missingEvents != 0 {
		t.Fatalf("asset.missing count = %d, want 0", missingEvents)
	}
}

func TestRunDaemonScanJob_StopOnFinalWriteRemainsRecoverable(t *testing.T) {
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	db := testdb.New(t)
	migrateStartupDB(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "stop-final-write-root")
	soloPath := filepath.Join(rootDir, "Solo/v01.cbz")
	writeTestFile(t, soloPath, buildTestCBZ(t, "Solo", "One", "1"))
	lib, root := seedTestLibrary(ctx, t, db.Pool, "Daemon Stop Final Write Lib", rootDir)
	jobRepo := postgres.NewJobRepository(db.Pool)

	dsPtr1, _ := captureDaemonAssembly(t)
	finalCommitted := make(chan struct{}, 1)
	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		hookTx := &daemonAssetCommitHookTransactor{
			inner: postgres.NewTransactor(pool),
			onAfterCommit: func(txCtx context.Context, createCount int32) {
				if createCount == 1 {
					select {
					case finalCommitted <- struct{}{}:
					default:
					}
					<-txCtx.Done()
				}
			},
		}
		return scanner.NewScanner(hookTx, scanner.Options{})
	}

	var interruptedID string
	if err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		ds := *dsPtr1
		var err error
		interruptedID, err = ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return err
		}
		select {
		case <-finalCommitted:
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for final write commit")
		}
		return nil
	}); err != nil {
		t.Fatalf("daemon 1 runDaemon err = %v", err)
	}

	midJob, err := jobRepo.GetJobByID(ctx, interruptedID)
	if err != nil {
		t.Fatalf("GetJobByID mid-stop: %v", err)
	}
	if midJob.State != domain.JobStateRunning || midJob.Attempts != 1 {
		t.Fatalf("job after final-write daemon stop = (state=%s, attempts=%d), want (RUNNING, 1)", midJob.State, midJob.Attempts)
	}

	var openMu sync.Mutex
	openedOnRecover := make(map[string]int)
	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				p := string(rawPath)
				openMu.Lock()
				openedOnRecover[p]++
				openMu.Unlock()
				return os.Open(p)
			},
		})
	}

	if err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		got := waitDaemonJobState(serveCtx, t, jobRepo, interruptedID, domain.JobStateSucceeded)
		if got.Attempts != 2 {
			return fmt.Errorf("recovered job Attempts = %d, want 2", got.Attempts)
		}
		return nil
	}); err != nil {
		t.Fatalf("daemon 2 runDaemon err = %v", err)
	}

	openMu.Lock()
	soloOpens := openedOnRecover[soloPath]
	openMu.Unlock()
	if soloOpens != 0 {
		t.Fatalf("Solo/v01.cbz re-opened %d times on recovery, want 0", soloOpens)
	}
	var total, missing int
	if err := db.QueryRow(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE missing_since IS NOT NULL) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&total, &missing); err != nil {
		t.Fatalf("count media_assets: %v", err)
	}
	if total != 1 || missing != 0 {
		t.Fatalf("final media_assets = (total=%d, missing=%d), want (1, 0)", total, missing)
	}
}

func TestScanKillRecoveryHelperProcess(t *testing.T) {
	if os.Getenv("TOBITEN_SCAN_KILL_HELPER") != "crash" {
		return
	}
	dbURL := os.Getenv("TOBITEN_SCAN_KILL_DB_URL")
	libID := os.Getenv("TOBITEN_SCAN_KILL_LIB_ID")
	if dbURL == "" || libID == "" {
		t.Fatal("missing TOBITEN_SCAN_KILL_DB_URL or TOBITEN_SCAN_KILL_LIB_ID")
	}

	originalNewScanner := newDaemonScanner
	t.Cleanup(func() {
		newDaemonScanner = originalNewScanner
	})

	readyOnce := sync.Once{}
	unblock := make(chan struct{})
	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		var hookTx *daemonAssetCommitHookTransactor
		hookTx = &daemonAssetCommitHookTransactor{
			inner:            postgres.NewTransactor(pool),
			unblockNextReadT: unblock,
			onAfterCommit: func(_ context.Context, createCount int32) {
				if createCount == 1 {
					hookTx.blockNextReadTx.Store(true)
				}
			},
			onBlockedNextTx: func() {
				readyOnce.Do(func() {
					fmt.Println("SCAN_KILL_HELPER_READY")
				})
			},
		}
		return scanner.NewScanner(hookTx, scanner.Options{})
	}

	dsPtr, _ := captureDaemonAssembly(t)
	ctx := context.Background()
	err := runDaemon(ctx, hostedConfig(t, dbURL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		ds := *dsPtr
		if _, err := ds.triggerScan(serveCtx, libID, scanner.TriggerScanOptions{}); err != nil {
			return err
		}
		<-serveCtx.Done()
		return serveCtx.Err()
	})
	if err != nil {
		t.Fatalf("helper runDaemon: %v", err)
	}
}

func TestRunDaemonScanJob_ProcessKilledMidScanRecovers(t *testing.T) {
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	db := testdb.New(t)
	migrateStartupDB(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "kill-recover-root")
	existingPath := filepath.Join(rootDir, "Existing/v01.cbz")
	newAPath := filepath.Join(rootDir, "New/a.cbz")
	newBPath := filepath.Join(rootDir, "New/b.cbz")
	newCPath := filepath.Join(rootDir, "New/c.cbz")
	writeTestFile(t, existingPath, buildTestCBZ(t, "Existing", "One", "1"))

	lib, root := seedTestLibrary(ctx, t, db.Pool, "Daemon Kill Recover Lib", rootDir)
	jobRepo := postgres.NewJobRepository(db.Pool)

	// Daemon 1: baseline scan commits Existing/v01.cbz, then exits before launching crash subprocess.
	dsPtr1, _ := captureDaemonAssembly(t)
	if err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		ds := *dsPtr1
		baseID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return err
		}
		waitDaemonJobState(serveCtx, t, jobRepo, baseID, domain.JobStateSucceeded)
		return nil
	}); err != nil {
		t.Fatalf("baseline runDaemon err = %v", err)
	}

	writeTestFile(t, newAPath, buildTestCBZ(t, "New", "A", "1"))
	writeTestFile(t, newBPath, buildTestCBZ(t, "New", "B", "2"))
	writeTestFile(t, newCPath, buildTestCBZ(t, "New", "C", "3"))

	exePath, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	crashCmd := exec.Command(exePath, "-test.run=^TestScanKillRecoveryHelperProcess$")
	crashCmd.Env = append(os.Environ(),
		"TOBITEN_TEST_DATABASE_URL="+db.URL,
		"TOBITEN_SCAN_KILL_HELPER=crash",
		"TOBITEN_SCAN_KILL_DB_URL="+db.URL,
		"TOBITEN_SCAN_KILL_LIB_ID="+lib.ID,
	)
	stdout, err := crashCmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	var stderrBuf strings.Builder
	var stderrMu sync.Mutex
	crashCmd.Stderr = &lockedWriter{mu: &stderrMu, sb: &stderrBuf}
	if err := crashCmd.Start(); err != nil {
		t.Fatalf("start crash helper: %v", err)
	}
	t.Cleanup(func() {
		if crashCmd.Process != nil {
			_ = crashCmd.Process.Kill()
			_ = crashCmd.Wait()
		}
	})

	readyCh := make(chan bool, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "SCAN_KILL_HELPER_READY" {
				readyCh <- true
				return
			}
		}
		readyCh <- false
	}()

	select {
	case ok := <-readyCh:
		if !ok {
			_ = crashCmd.Wait()
			stderrMu.Lock()
			msg := stderrBuf.String()
			stderrMu.Unlock()
			t.Fatalf("crash helper exited before ready; stderr:\n%s", msg)
		}
	case <-time.After(15 * time.Second):
		stderrMu.Lock()
		msg := stderrBuf.String()
		stderrMu.Unlock()
		t.Fatalf("timed out waiting for crash helper; stderr:\n%s", msg)
	}

	running, err := jobRepo.ListRunningJobs(ctx)
	if err != nil || len(running) != 1 {
		t.Fatalf("running jobs before SIGKILL = (%d, %v), want 1", len(running), err)
	}
	killedJobID := running[0].ID

	if err := crashCmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL crash helper: %v", err)
	}
	if waitErr := crashCmd.Wait(); waitErr == nil {
		t.Fatal("expected non-nil exit error after SIGKILL")
	}

	// Wait briefly for PostgreSQL backend to tear down the killed helper's advisory lock.
	lockDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(lockDeadline) {
		var locks int
		if qErr := db.QueryRow(ctx, `SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory'`).Scan(&locks); qErr == nil && locks == 0 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Recovery daemon: tracks opened files and verifies committed files are not re-opened.
	var openMu sync.Mutex
	openedOnRecover := make(map[string]int)
	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				p := string(rawPath)
				openMu.Lock()
				openedOnRecover[p]++
				openMu.Unlock()
				return os.Open(p)
			},
		})
	}

	if err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		got := waitDaemonJobState(serveCtx, t, jobRepo, killedJobID, domain.JobStateSucceeded)
		if got.Attempts != 2 {
			return fmt.Errorf("recovered killed job Attempts = %d, want 2", got.Attempts)
		}
		return nil
	}); err != nil {
		t.Fatalf("recovery runDaemon err = %v", err)
	}

	openMu.Lock()
	existingOpens := openedOnRecover[existingPath]
	newAOpens := openedOnRecover[newAPath]
	newBOpens := openedOnRecover[newBPath]
	newCOpens := openedOnRecover[newCPath]
	openMu.Unlock()
	if existingOpens != 0 || newAOpens != 0 || newBOpens != 1 || newCOpens != 1 {
		t.Fatalf("post-SIGKILL opens = (existing=%d, newA=%d, newB=%d, newC=%d), want (0, 0, 1, 1)",
			existingOpens, newAOpens, newBOpens, newCOpens)
	}

	var totalAssets, missingAssets, missingEvents int
	if err := db.QueryRow(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE missing_since IS NOT NULL) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&totalAssets, &missingAssets); err != nil {
		t.Fatalf("count media_assets: %v", err)
	}
	if totalAssets != 4 || missingAssets != 0 {
		t.Fatalf("final media_assets = (total=%d, missing=%d), want (4, 0)", totalAssets, missingAssets)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE kind = 'asset.missing'`).Scan(&missingEvents); err != nil {
		t.Fatalf("count asset.missing: %v", err)
	}
	if missingEvents != 0 {
		t.Fatalf("asset.missing count = %d, want 0", missingEvents)
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	sb *strings.Builder
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sb.Write(p)
}

func TestRunDaemonScanJob_InterruptionsExhaustMaxAttemptsAndFreshTriggerCompletes(t *testing.T) {
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	db := testdb.New(t)
	migrateStartupDB(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "exhaust-restart-root")
	existingPath := filepath.Join(rootDir, "Existing/v01.cbz")
	newAPath := filepath.Join(rootDir, "New/a.cbz")
	newBPath := filepath.Join(rootDir, "New/b.cbz")
	writeTestFile(t, existingPath, buildTestCBZ(t, "Existing", "One", "1"))

	lib, root := seedTestLibrary(ctx, t, db.Pool, "Daemon Exhaust Interruption Lib", rootDir)
	jobRepo := postgres.NewJobRepository(db.Pool)

	// Baseline scan commits Existing/v01.cbz.
	dsPtrBase, _ := captureDaemonAssembly(t)
	if err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		ds := *dsPtrBase
		baseID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return err
		}
		waitDaemonJobState(serveCtx, t, jobRepo, baseID, domain.JobStateSucceeded)
		return nil
	}); err != nil {
		t.Fatalf("baseline runDaemon err = %v", err)
	}

	writeTestFile(t, newAPath, buildTestCBZ(t, "New", "A", "1"))
	writeTestFile(t, newBPath, buildTestCBZ(t, "New", "B", "2"))

	// Trigger and interrupt 3 attempts (DefaultScanMaxAttempts = 3):
	// Attempt 1 commits New/a.cbz then blocks before New/b.cbz; attempts 2 & 3 interrupt on Open(New/b.cbz).
	var exhaustedJobID string
	for attempt := 1; attempt <= scanner.DefaultScanMaxAttempts; attempt++ {
		interrupted := make(chan struct{}, 1)
		unblock := make(chan struct{})
		if attempt == 1 {
			newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
				var hookTx *daemonAssetCommitHookTransactor
				hookTx = &daemonAssetCommitHookTransactor{
					inner:            postgres.NewTransactor(pool),
					unblockNextReadT: unblock,
					onAfterCommit: func(_ context.Context, createCount int32) {
						if createCount == 1 {
							hookTx.blockNextReadTx.Store(true)
						}
					},
					onBlockedNextTx: func() {
						select {
						case interrupted <- struct{}{}:
						default:
						}
					},
				}
				return scanner.NewScanner(hookTx, scanner.Options{})
			}
		} else {
			newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
				return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
					Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
						if string(rawPath) == newBPath {
							select {
							case interrupted <- struct{}{}:
							default:
							}
							<-openCtx.Done()
							return nil, openCtx.Err()
						}
						return os.Open(string(rawPath))
					},
				})
			}
		}

		dsPtr, _ := captureDaemonAssembly(t)
		err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			if attempt == 1 {
				ds := *dsPtr
				var tErr error
				exhaustedJobID, tErr = ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
				if tErr != nil {
					return tErr
				}
			}
			select {
			case <-interrupted:
			case <-time.After(10 * time.Second):
				return fmt.Errorf("attempt %d timed out waiting for interruption point", attempt)
			}
			return nil
		})
		close(unblock)
		if err != nil {
			t.Fatalf("attempt %d runDaemon err = %v", attempt, err)
		}

		got, err := jobRepo.GetJobByID(ctx, exhaustedJobID)
		if err != nil {
			t.Fatalf("GetJobByID after attempt %d: %v", attempt, err)
		}
		if got.State != domain.JobStateRunning || got.Attempts != attempt {
			t.Fatalf("after attempt %d: job = (state=%s, attempts=%d), want (RUNNING, %d)", attempt, got.State, got.Attempts, attempt)
		}
	}

	// Daemon 4: startup recovery transitions exhausted job to FAILED, preserves catalogue consistency,
	// releases dedupe key, and a fresh trigger completes the scan without re-opening committed files.
	var openMu sync.Mutex
	openedOnFresh := make(map[string]int)
	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				p := string(rawPath)
				openMu.Lock()
				openedOnFresh[p]++
				openMu.Unlock()
				return os.Open(p)
			},
		})
	}

	dsPtrFinal, _ := captureDaemonAssembly(t)
	if err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		ds := *dsPtrFinal
		failedJob, err := jobRepo.GetJobByID(serveCtx, exhaustedJobID)
		if err != nil {
			return err
		}
		if failedJob.State != domain.JobStateFailed || failedJob.Attempts != scanner.DefaultScanMaxAttempts || failedJob.FinishedAt == nil {
			return fmt.Errorf("recovered exhausted job = %+v, want FAILED attempts=%d finished_at!=nil", failedJob, scanner.DefaultScanMaxAttempts)
		}
		var failedEvents int
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.failed'`, exhaustedJobID).Scan(&failedEvents); err != nil {
			return err
		}
		if failedEvents != 1 {
			return fmt.Errorf("job.failed count = %d, want 1", failedEvents)
		}

		// Verify consistent pre-fresh catalogue (Existing + New/a.cbz intact, no missing marks).
		var preTotal, preMissing int
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE missing_since IS NOT NULL) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&preTotal, &preMissing); err != nil {
			return err
		}
		if preTotal != 2 || preMissing != 0 {
			return fmt.Errorf("pre-fresh catalogue = (total=%d, missing=%d), want (2, 0)", preTotal, preMissing)
		}

		freshJobID, err := ds.triggerScan(serveCtx, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return err
		}
		if freshJobID == exhaustedJobID {
			return fmt.Errorf("dedupe key not released after recovery failure: reused %q", exhaustedJobID)
		}
		gotSucc := waitDaemonJobState(serveCtx, t, jobRepo, freshJobID, domain.JobStateSucceeded)
		if gotSucc.Attempts != 1 {
			return fmt.Errorf("fresh job Attempts = %d, want 1", gotSucc.Attempts)
		}
		return nil
	}); err != nil {
		t.Fatalf("final runDaemon err = %v", err)
	}

	openMu.Lock()
	existingOpens := openedOnFresh[existingPath]
	newAOpens := openedOnFresh[newAPath]
	newBOpens := openedOnFresh[newBPath]
	openMu.Unlock()
	if existingOpens != 0 || newAOpens != 0 || newBOpens != 1 {
		t.Fatalf("fresh scan opens = (existing=%d, newA=%d, newB=%d), want (0, 0, 1)", existingOpens, newAOpens, newBOpens)
	}

	var totalAssets, missingAssets, missingEvents int
	if err := db.QueryRow(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE missing_since IS NOT NULL) FROM media_assets WHERE library_root_id = $1`, root.ID).Scan(&totalAssets, &missingAssets); err != nil {
		t.Fatalf("count final media_assets: %v", err)
	}
	if totalAssets != 3 || missingAssets != 0 {
		t.Fatalf("final media_assets = (total=%d, missing=%d), want (3, 0)", totalAssets, missingAssets)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE kind = 'asset.missing'`).Scan(&missingEvents); err != nil {
		t.Fatalf("count asset.missing: %v", err)
	}
	if missingEvents != 0 {
		t.Fatalf("asset.missing count = %d, want 0", missingEvents)
	}
}

func TestDaemonScanTriggerJobsAndSSEEndToEnd(t *testing.T) {
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	db := testdb.New(t)
	migrateStartupDB(t, db)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Seed 3 libraries on disk:
	// - lib1: 2 CBZs, used for active-scan dedupe + creator/jobs:read_all SSE + finish + post-finish re-trigger
	// - lib2: 3 CBZs, used for HTTP cancel -> CANCELLED (gates on 02.cbz so 01.cbz emits progress first, 03.cbz stays unopened) + automatic scan
	// - lib3: 1 CBZ, triggered by otherLibAdmin at the end to prove its SSE stream was live with 0 prior leaks
	baseDir := t.TempDir()
	root1 := filepath.Join(baseDir, "lib1")
	root2 := filepath.Join(baseDir, "lib2")
	root3 := filepath.Join(baseDir, "lib3")

	lib1File1 := filepath.Join(root1, "Horizon/01.cbz")
	lib1File2 := filepath.Join(root1, "Horizon/02.cbz")
	lib2File1 := filepath.Join(root2, "CancelSaga/01.cbz")
	lib2File2 := filepath.Join(root2, "CancelSaga/02.cbz")
	lib2File3 := filepath.Join(root2, "CancelSaga/03.cbz")
	lib3File1 := filepath.Join(root3, "Other/01.cbz")

	writeTestFile(t, lib1File1, buildTestCBZ(t, "Horizon", "First", "1"))
	writeTestFile(t, lib1File2, buildTestCBZ(t, "Horizon", "Second", "2"))
	writeTestFile(t, lib2File1, buildTestCBZ(t, "CancelSaga", "One", "1"))
	writeTestFile(t, lib2File2, buildTestCBZ(t, "CancelSaga", "Two", "2"))
	writeTestFile(t, lib2File3, buildTestCBZ(t, "CancelSaga", "Three", "3"))
	writeTestFile(t, lib3File1, buildTestCBZ(t, "Other", "One", "1"))

	lib1, _ := seedTestLibrary(ctx, t, db.Pool, "E2E Scan Lib 1", root1)
	lib2, _ := seedTestLibrary(ctx, t, db.Pool, "E2E Scan Lib 2", root2)
	lib3, _ := seedTestLibrary(ctx, t, db.Pool, "E2E Scan Lib 3", root3)

	// Gate hooks so we deterministically hold lib1's first scan RUNNING during dedupe/snapshot checks
	// and hold lib2's first scan RUNNING on 02.cbz during cancel checks without racing fast disk I/O.
	var gateLib1Once atomic.Bool
	gateLib1Once.Store(true)
	lib1Entered := make(chan struct{}, 1)
	releaseLib1 := make(chan struct{})

	var gateLib2Once atomic.Bool
	gateLib2Once.Store(true)
	lib2Entered := make(chan struct{}, 1)
	releaseLib2 := make(chan struct{})
	var lib2File3Opened atomic.Bool

	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
			Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				p := string(rawPath)
				if p == lib1File1 && gateLib1Once.CompareAndSwap(true, false) {
					select {
					case lib1Entered <- struct{}{}:
					default:
					}
					select {
					case <-releaseLib1:
					case <-openCtx.Done():
						return nil, openCtx.Err()
					}
				}
				if p == lib2File2 && gateLib2Once.CompareAndSwap(true, false) {
					select {
					case lib2Entered <- struct{}{}:
					default:
					}
					select {
					case <-releaseLib2:
					case <-openCtx.Done():
						return nil, openCtx.Err()
					}
				}
				if p == lib2File3 {
					lib2File3Opened.Store(true)
				}
				return os.Open(p)
			},
		})
	}

	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_ADMIN_USER=sysadmin",
		"TOBITEN_ADMIN_PASS=admin-secret-123",
		"TOBITEN_BASE_URL=http://localhost:8080/comics",
		"TOBITEN_LOG_FORMAT=json",
		"TOBITEN_SCANNER_WORKERS=1",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	dsPtr, _ := captureDaemonAssembly(t)
	userRepo := postgres.NewUserRepository(db.Pool)

	err = runDaemon(ctx, cfg, func(serveCtx context.Context, cfg *config.Config, logger *slog.Logger, services *app.Services) error {
		ds := *dsPtr
		reg := BuildRegistryWithServices(cfg, services)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		srv, err := api.NewServer(api.ServerOptions{
			Config:   cfg,
			Logger:   logger,
			Listener: ln,
			Registry: reg,
		})
		if err != nil {
			_ = ln.Close()
			return err
		}
		srvCtx, cancelSrv := context.WithCancel(serveCtx)
		srvDone := make(chan error, 1)
		go func() {
			srvDone <- srv.Run(srvCtx)
		}()
		defer func() {
			cancelSrv()
			if sErr := <-srvDone; sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
				t.Errorf("srv.Run: %v", sErr)
			}
		}()

		baseURL := srv.URL()
		httpClient := &http.Client{Timeout: 5 * time.Second}

		// Custom roles:
		// - roleLibAdminNoJobsAll: library:read + library:admin + jobs:cancel_own (NO jobs:read_all!)
		// - roleJobsReadAllOnly: library:read + jobs:read_all (NO library:admin, NO jobs:cancel_all!)
		const (
			roleLibAdminNoJobsAllID = "019318a0-0000-7000-8000-000000000201"
			roleJobsReadAllOnlyID   = "019318a0-0000-7000-8000-000000000202"
		)
		if _, err := db.Exec(serveCtx, `
			INSERT INTO roles (id, name, built_in, requires_allow_labels) VALUES
				($1, 'Library Admin No ReadAll', FALSE, FALSE),
				($2, 'Jobs ReadAll Only', FALSE, FALSE)
		`, roleLibAdminNoJobsAllID, roleJobsReadAllOnlyID); err != nil {
			return err
		}
		if _, err := db.Exec(serveCtx, `
			INSERT INTO role_permissions (role_id, permission) VALUES
				($1, 'library:read'), ($1, 'library:admin'), ($1, 'jobs:cancel_own'),
				($2, 'library:read'), ($2, 'jobs:read_all')
		`, roleLibAdminNoJobsAllID, roleJobsReadAllOnlyID); err != nil {
			return err
		}

		sysadmin, err := userRepo.GetUserByUsername(serveCtx, "sysadmin")
		if err != nil {
			return err
		}
		creator := createTestUserWithPassword(serveCtx, t, userRepo, "scan-creator", "creator-pass-123", roleLibAdminNoJobsAllID)
		otherLibAdmin := createTestUserWithPassword(serveCtx, t, userRepo, "scan-other", "other-pass-123", roleLibAdminNoJobsAllID)
		auditor := createTestUserWithPassword(serveCtx, t, userRepo, "scan-auditor", "auditor-pass-123", roleJobsReadAllOnlyID)
		reader := createTestUserWithPassword(serveCtx, t, userRepo, "scan-reader", "reader-pass-123", domain.RoleReaderID)
		curator := createTestUserWithPassword(serveCtx, t, userRepo, "scan-curator", "curator-pass-123", domain.RoleCuratorID)

		sysReadPAT := createTestPAT(serveCtx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeRead})
		sysLibAdminPAT := createTestPAT(serveCtx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeLibraryAdmin})
		sysAdminPAT := createTestPAT(serveCtx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeAdmin})

		doReq := func(method, path string, mutate func(*http.Request)) (int, http.Header, string) {
			req, reqErr := http.NewRequestWithContext(serveCtx, method, baseURL+path, nil)
			if reqErr != nil {
				t.Fatalf("NewRequest(%s %s): %v", method, path, reqErr)
			}
			if mutate != nil {
				mutate(req)
			}
			resp, doErr := httpClient.Do(req)
			if doErr != nil {
				t.Fatalf("%s %s: %v", method, path, doErr)
			}
			defer resp.Body.Close()
			raw, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				t.Fatalf("ReadAll(%s %s): %v", method, path, readErr)
			}
			return resp.StatusCode, resp.Header, string(raw)
		}

		// Cookie session login for creator to test CSRF enforcement and valid cookie-triggered scan.
		loginReq, err := http.NewRequestWithContext(serveCtx, http.MethodPost, baseURL+"/api/v1/auth/login",
			strings.NewReader(`{"username":"scan-creator","password":"creator-pass-123"}`))
		if err != nil {
			return err
		}
		loginReq.Header.Set("Content-Type", "application/json")
		loginResp, err := httpClient.Do(loginReq)
		if err != nil {
			return err
		}
		_ = loginResp.Body.Close()
		if loginResp.StatusCode != http.StatusOK {
			return fmt.Errorf("creator login = %d, want 200", loginResp.StatusCode)
		}
		var creatorCookie *http.Cookie
		for _, c := range loginResp.Cookies() {
			if c.Name == app.NativeSessionCookieName {
				creatorCookie = c
			}
		}
		creatorCSRF := loginResp.Header.Get("X-CSRF-Token")
		if creatorCookie == nil || creatorCSRF == "" {
			return errors.New("missing session cookie or CSRF token")
		}

		// 1. Unauthenticated POST /scan returns 401 (direct + subpath).
		for _, p := range []string{
			"/api/v1/libraries/" + lib1.ID + "/scan",
			"/comics/api/v1/libraries/" + lib1.ID + "/scan",
		} {
			code, hdr, _ := doReq(http.MethodPost, p, nil)
			if code != http.StatusUnauthorized || !strings.HasPrefix(hdr.Get("WWW-Authenticate"), "Basic") {
				return fmt.Errorf("unauthenticated POST %s = (%d, WWW-Authenticate=%q), want (401, Basic)", p, code, hdr.Get("WWW-Authenticate"))
			}
		}

		// 2. Missing library:admin returns 403 and writes no job rows; cookie without CSRF returns 403.
		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"reader basic", func(r *http.Request) { r.SetBasicAuth(reader.Username, "reader-pass-123") }},
			{"curator basic", func(r *http.Request) { r.SetBasicAuth(curator.Username, "curator-pass-123") }},
			{"auditor jobs:read_all basic", func(r *http.Request) { r.SetBasicAuth(auditor.Username, "auditor-pass-123") }},
			{"sysadmin read PAT", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+sysReadPAT) }},
			{"cookie without CSRF", func(r *http.Request) { r.AddCookie(creatorCookie) }},
		} {
			code, _, body := doReq(http.MethodPost, "/api/v1/libraries/"+lib1.ID+"/scan", tc.mutate)
			if code != http.StatusForbidden {
				return fmt.Errorf("%s POST /scan = %d (%s), want 403", tc.name, code, body)
			}
		}

		// 3. Unknown library returns 404 and writes no job row.
		code404, _, _ := doReq(http.MethodPost, "/api/v1/libraries/unknown-library-id/scan", func(r *http.Request) {
			r.SetBasicAuth(creator.Username, "creator-pass-123")
		})
		if code404 != http.StatusNotFound {
			return fmt.Errorf("unknown library POST /scan = %d, want 404", code404)
		}
		var preJobs int
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM jobs`).Scan(&preJobs); err != nil {
			return err
		}
		if preJobs != 0 {
			return fmt.Errorf("expected 0 jobs after denied/404 requests, got %d", preJobs)
		}

		// 4. Open live SSE streams before triggering scans:
		//    - creatorStream (creator: no jobs:read_all)
		//    - auditorStream (auditor: jobs:read_all)
		//    - otherStream (otherLibAdmin: no jobs:read_all)
		creatorStream := openSSEStream(t, baseURL+"/api/v1/events", creator.Username, "creator-pass-123", "")
		auditorStream := openSSEStream(t, baseURL+"/comics/api/v1/events", auditor.Username, "auditor-pass-123", "")
		otherStream := openSSEStream(t, baseURL+"/api/v1/events", otherLibAdmin.Username, "other-pass-123", "")
		defer func() {
			creatorStream.close()
			auditorStream.close()
			otherStream.close()
		}()
		creatorTracker := newStreamScanSSETracker(t, "creator", creatorStream)
		auditorTracker := newStreamScanSSETracker(t, "auditor", auditorStream)
		otherTracker := newStreamScanSSETracker(t, "otherLibAdmin", otherStream)

		// 5. Creator triggers lib1 scan -> 202 with job_id; repeated requests while RUNNING return the same job_id.
		trigCode, trigHdr, trigBody := doReq(http.MethodPost, "/comics/api/v1/libraries/"+lib1.ID+"/scan", func(r *http.Request) {
			r.SetBasicAuth(creator.Username, "creator-pass-123")
		})
		if trigCode != http.StatusAccepted {
			return fmt.Errorf("creator POST /scan = %d (%s), want 202", trigCode, trigBody)
		}
		if trigHdr.Get("Cache-Control") != "no-store" {
			return fmt.Errorf("POST /scan Cache-Control = %q, want no-store", trigHdr.Get("Cache-Control"))
		}
		var trigResp middleware.JobAcceptedResponse
		mustUnmarshal(t, trigBody, &trigResp)
		if trigResp.JobID == "" {
			return errors.New("expected non-empty job_id from POST /scan")
		}
		jobID1 := trigResp.JobID

		// Wait until worker is actively inside lib1File1.
		select {
		case <-lib1Entered:
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for lib1 scan to enter RUNNING")
		}

		// Repeated triggers while active return the exact same job_id (for both creator and another library:admin principal).
		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"creator repeat", func(r *http.Request) { r.SetBasicAuth(creator.Username, "creator-pass-123") }},
			{"sysadmin library:admin PAT repeat", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+sysLibAdminPAT) }},
		} {
			dupCode, _, dupBody := doReq(http.MethodPost, "/api/v1/libraries/"+lib1.ID+"/scan", tc.mutate)
			if dupCode != http.StatusAccepted {
				return fmt.Errorf("%s POST /scan = %d (%s), want 202", tc.name, dupCode, dupBody)
			}
			var dupResp middleware.JobAcceptedResponse
			mustUnmarshal(t, dupBody, &dupResp)
			if dupResp.JobID != jobID1 {
				return fmt.Errorf("%s returned job_id=%q, want %q", tc.name, dupResp.JobID, jobID1)
			}
		}

		// GET /api/v1/jobs/{job_id}: creator and jobs:read_all see RUNNING snapshot; another user gets 404.
		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"creator", func(r *http.Request) { r.SetBasicAuth(creator.Username, "creator-pass-123") }},
			{"auditor", func(r *http.Request) { r.SetBasicAuth(auditor.Username, "auditor-pass-123") }},
		} {
			c, _, b := doReq(http.MethodGet, "/api/v1/jobs/"+jobID1, tc.mutate)
			if c != http.StatusOK {
				return fmt.Errorf("%s GET /jobs/%s = %d (%s), want 200", tc.name, jobID1, c, b)
			}
			var info app.JobInfo
			mustUnmarshal(t, b, &info)
			if info.ID != jobID1 || info.Kind != string(domain.JobKindScan) || info.State != string(domain.JobStateRunning) ||
				info.CreatedBy == nil || *info.CreatedBy != creator.ID {
				return fmt.Errorf("%s GET /jobs/%s snapshot = %+v", tc.name, jobID1, info)
			}
		}
		if c, _, _ := doReq(http.MethodGet, "/api/v1/jobs/"+jobID1, func(r *http.Request) {
			r.SetBasicAuth(otherLibAdmin.Username, "other-pass-123")
		}); c != http.StatusNotFound {
			return fmt.Errorf("non-creator GET /jobs/%s = %d, want 404", jobID1, c)
		}

		// Release lib1 scan to complete both files and verify SSE lifecycle + ephemeral job.progress on creator & auditor.
		close(releaseLib1)

		if err := creatorTracker.collectFinishedWithProgress(jobID1); err != nil {
			return err
		}
		if err := auditorTracker.collectFinishedWithProgress(jobID1); err != nil {
			return err
		}

		// GET /api/v1/jobs/{job_id} reflects persisted SUCCEEDED snapshot.
		getSuccCode, _, getSuccBody := doReq(http.MethodGet, "/api/v1/jobs/"+jobID1, func(r *http.Request) {
			r.SetBasicAuth(creator.Username, "creator-pass-123")
		})
		if getSuccCode != http.StatusOK {
			return fmt.Errorf("GET finished job = %d (%s), want 200", getSuccCode, getSuccBody)
		}
		var succInfo app.JobInfo
		mustUnmarshal(t, getSuccBody, &succInfo)
		if succInfo.State != string(domain.JobStateSucceeded) || succInfo.FinishedAt == nil {
			return fmt.Errorf("finished job snapshot = %+v, want SUCCEEDED with finished_at", succInfo)
		}

		// 6. Trigger lib2 scan via cookie + CSRF, wait for 01.cbz progress + 02.cbz gate, then cancel via POST /api/v1/jobs/{job_id}/cancel -> CANCELLED.
		codeLib2, _, bodyLib2 := doReq(http.MethodPost, "/api/v1/libraries/"+lib2.ID+"/scan", func(r *http.Request) {
			r.AddCookie(creatorCookie)
			r.Header.Set("X-CSRF-Token", creatorCSRF)
		})
		if codeLib2 != http.StatusAccepted {
			return fmt.Errorf("cookie POST /scan(lib2) = %d (%s), want 202", codeLib2, bodyLib2)
		}
		var respLib2 middleware.JobAcceptedResponse
		mustUnmarshal(t, bodyLib2, &respLib2)
		jobID2 := respLib2.JobID

		select {
		case <-lib2Entered:
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for lib2 scan to enter 02.cbz")
		}

		if err := creatorTracker.waitStartedAndProgress(jobID2); err != nil {
			return err
		}
		if err := auditorTracker.waitStartedAndProgress(jobID2); err != nil {
			return err
		}

		// Non-creator cannot cancel (404); auditor without jobs:cancel_all gets 403; creator cancels (200).
		if c, _, _ := doReq(http.MethodPost, "/api/v1/jobs/"+jobID2+"/cancel", func(r *http.Request) {
			r.SetBasicAuth(otherLibAdmin.Username, "other-pass-123")
		}); c != http.StatusNotFound {
			return fmt.Errorf("non-creator cancel = %d, want 404", c)
		}
		if c, _, _ := doReq(http.MethodPost, "/api/v1/jobs/"+jobID2+"/cancel", func(r *http.Request) {
			r.SetBasicAuth(auditor.Username, "auditor-pass-123")
		}); c != http.StatusForbidden {
			return fmt.Errorf("auditor (no cancel_all) cancel = %d, want 403", c)
		}
		cancelCode, _, cancelBody := doReq(http.MethodPost, "/comics/api/v1/jobs/"+jobID2+"/cancel", func(r *http.Request) {
			r.AddCookie(creatorCookie)
			r.Header.Set("X-CSRF-Token", creatorCSRF)
		})
		if cancelCode != http.StatusOK {
			return fmt.Errorf("creator cancel = %d (%s), want 200", cancelCode, cancelBody)
		}
		var cancelSnap app.JobInfo
		mustUnmarshal(t, cancelBody, &cancelSnap)
		if !cancelSnap.CancelRequested {
			return fmt.Errorf("cancel snapshot = %+v, want cancel_requested=true", cancelSnap)
		}

		close(releaseLib2)

		if err := creatorTracker.waitTerminal(jobID2, string(domain.EventJobCancelled)); err != nil {
			return err
		}
		if err := auditorTracker.waitTerminal(jobID2, string(domain.EventJobCancelled)); err != nil {
			return err
		}
		if lib2File3Opened.Load() {
			return errors.New("cancelled scan unexpectedly opened third archive CancelSaga/03.cbz")
		}
		getCanCode, _, getCanBody := doReq(http.MethodGet, "/api/v1/jobs/"+jobID2, func(r *http.Request) {
			r.SetBasicAuth(creator.Username, "creator-pass-123")
		})
		if getCanCode != http.StatusOK {
			return fmt.Errorf("GET cancelled job = %d (%s), want 200", getCanCode, getCanBody)
		}
		var canInfo app.JobInfo
		mustUnmarshal(t, getCanBody, &canInfo)
		if canInfo.State != string(domain.JobStateCancelled) || !canInfo.CancelRequested {
			return fmt.Errorf("cancelled job snapshot = %+v, want CANCELLED and cancel_requested=true", canInfo)
		}

		// 7. Automatic scan (created_by NULL) is visible only with jobs:read_all.
		autoJobID, err := ds.triggerScan(serveCtx, lib2.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return fmt.Errorf("trigger automatic scan: %w", err)
		}
		if err := auditorTracker.collectFinishedWithProgress(autoJobID); err != nil {
			return err
		}

		// GET /api/v1/jobs/{autoJobID}: auditor and admin PAT get 200 with created_by=null; creator/other/reader get 404.
		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"auditor", func(r *http.Request) { r.SetBasicAuth(auditor.Username, "auditor-pass-123") }},
			{"sysadmin admin PAT", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+sysAdminPAT) }},
		} {
			c, _, b := doReq(http.MethodGet, "/api/v1/jobs/"+autoJobID, tc.mutate)
			if c != http.StatusOK {
				return fmt.Errorf("%s GET automatic job = %d (%s), want 200", tc.name, c, b)
			}
			var autoInfo app.JobInfo
			mustUnmarshal(t, b, &autoInfo)
			if autoInfo.CreatedBy != nil {
				return fmt.Errorf("automatic job CreatedBy = %v, want nil", *autoInfo.CreatedBy)
			}
		}
		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"creator(library:admin without jobs:read_all)", func(r *http.Request) { r.SetBasicAuth(creator.Username, "creator-pass-123") }},
			{"otherLibAdmin", func(r *http.Request) { r.SetBasicAuth(otherLibAdmin.Username, "other-pass-123") }},
			{"reader", func(r *http.Request) { r.SetBasicAuth(reader.Username, "reader-pass-123") }},
			{"sysadmin library:admin PAT", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+sysLibAdminPAT) }},
		} {
			c, _, _ := doReq(http.MethodGet, "/api/v1/jobs/"+autoJobID, tc.mutate)
			if c != http.StatusNotFound {
				return fmt.Errorf("%s GET automatic job = %d, want 404", tc.name, c)
			}
		}

		// 8. Prove creator's SSE stream never received autoJobID and otherStream never received ANY of
		//    jobID1, jobID2, or autoJobID by triggering one job each for creator and otherLibAdmin and
		//    asserting their next lifecycle frames belong exclusively to those new jobs (while tolerating
		//    only delayed progress ticks from earlier authorized jobs on the same stream).
		reTrigCode, _, reTrigBody := doReq(http.MethodPost, "/api/v1/libraries/"+lib1.ID+"/scan", func(r *http.Request) {
			r.SetBasicAuth(creator.Username, "creator-pass-123")
		})
		if reTrigCode != http.StatusAccepted {
			return fmt.Errorf("post-finish re-trigger = %d (%s), want 202", reTrigCode, reTrigBody)
		}
		var reTrigResp middleware.JobAcceptedResponse
		mustUnmarshal(t, reTrigBody, &reTrigResp)
		if reTrigResp.JobID == jobID1 {
			return fmt.Errorf("re-trigger after SUCCEEDED reused %q", jobID1)
		}
		if err := creatorTracker.collectFinishedWithProgress(reTrigResp.JobID); err != nil {
			return err
		}

		otherCode, _, otherBody := doReq(http.MethodPost, "/api/v1/libraries/"+lib3.ID+"/scan", func(r *http.Request) {
			r.SetBasicAuth(otherLibAdmin.Username, "other-pass-123")
		})
		if otherCode != http.StatusAccepted {
			return fmt.Errorf("otherLibAdmin POST /scan(lib3) = %d (%s), want 202", otherCode, otherBody)
		}
		var otherResp middleware.JobAcceptedResponse
		mustUnmarshal(t, otherBody, &otherResp)
		return otherTracker.collectFinishedWithProgress(otherResp.JobID)
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
	}
}

type scanSSETracker struct {
	who             string
	nextFrame       func() (sseFrame, error)
	priorAuthorized map[string]bool
	lastSeq         int64
}

func newStreamScanSSETracker(t *testing.T, who string, stream *sseClientStream) *scanSSETracker {
	t.Helper()
	return &scanSSETracker{
		who:             who,
		priorAuthorized: make(map[string]bool),
		nextFrame: func() (sseFrame, error) {
			return stream.nextFrame(t), nil
		},
	}
}

func newSliceScanSSETracker(frames []sseFrame) *scanSSETracker {
	idx := 0
	return &scanSSETracker{
		who:             "test",
		priorAuthorized: make(map[string]bool),
		nextFrame: func() (sseFrame, error) {
			if idx >= len(frames) {
				return sseFrame{}, errors.New("unexpected end of SSE frames")
			}
			f := frames[idx]
			idx++
			return f, nil
		},
	}
}

func (tr *scanSSETracker) handleProgressFrame(f sseFrame, wantJobID string) (bool, error) {
	if f.ID != "" {
		return false, fmt.Errorf("%s progress frame carried SSE ID %q, want empty", tr.who, f.ID)
	}
	var tick domain.JobProgressTick
	if err := json.Unmarshal([]byte(f.Data), &tick); err != nil {
		return false, fmt.Errorf("%s invalid progress JSON %q: %w", tr.who, f.Data, err)
	}
	if tick.ScannedCount < 1 || tick.CurrentPath == "" {
		return false, fmt.Errorf("%s incomplete progress tick: %+v", tr.who, tick)
	}
	if tick.JobID == wantJobID {
		return true, nil
	}
	if tr.priorAuthorized[tick.JobID] {
		return false, nil
	}
	return false, fmt.Errorf("%s received unauthorized progress tick for job %q (want %q)", tr.who, tick.JobID, wantJobID)
}

func (tr *scanSSETracker) parseLifecycleFrame(f sseFrame, wantJobID string) error {
	var pub app.PublicEvent
	if err := json.Unmarshal([]byte(f.Data), &pub); err != nil {
		return fmt.Errorf("%s invalid event JSON %q: %w", tr.who, f.Data, err)
	}
	if pub.AggregateID != wantJobID {
		return fmt.Errorf("%s received lifecycle event for job %q (%s), want %q", tr.who, pub.AggregateID, f.Event, wantJobID)
	}
	seq, err := strconv.ParseInt(f.ID, 10, 64)
	if err != nil || seq != pub.Sequence || seq <= 0 {
		return fmt.Errorf("%s invalid SSE id=%q vs seq=%d", tr.who, f.ID, pub.Sequence)
	}
	if tr.lastSeq > 0 && seq <= tr.lastSeq {
		return fmt.Errorf("%s non-increasing SSE sequence %d after %d", tr.who, seq, tr.lastSeq)
	}
	tr.lastSeq = seq
	return nil
}

func (tr *scanSSETracker) collectFinishedWithProgress(wantJobID string) error {
	var lifecycle []sseFrame
	var sawProgress bool
	for len(lifecycle) < 2 || !sawProgress {
		f, err := tr.nextFrame()
		if err != nil {
			return err
		}
		if f.Event == domain.ProgressTickEventName {
			matched, pErr := tr.handleProgressFrame(f, wantJobID)
			if pErr != nil {
				return pErr
			}
			if matched {
				sawProgress = true
			}
			continue
		}
		if lErr := tr.parseLifecycleFrame(f, wantJobID); lErr != nil {
			return lErr
		}
		lifecycle = append(lifecycle, f)
		if len(lifecycle) > 2 {
			return fmt.Errorf("%s received extra lifecycle event %s for %q", tr.who, f.Event, wantJobID)
		}
	}
	if lifecycle[0].Event != string(domain.EventJobStarted) || lifecycle[1].Event != string(domain.EventJobFinished) {
		return fmt.Errorf("%s events for %s = [%s %s], want [job.started job.finished]", tr.who, wantJobID, lifecycle[0].Event, lifecycle[1].Event)
	}
	tr.priorAuthorized[wantJobID] = true
	return nil
}

func (tr *scanSSETracker) waitStartedAndProgress(wantJobID string) error {
	var sawStarted, sawProgress bool
	for !sawStarted || !sawProgress {
		f, err := tr.nextFrame()
		if err != nil {
			return err
		}
		if f.Event == domain.ProgressTickEventName {
			matched, pErr := tr.handleProgressFrame(f, wantJobID)
			if pErr != nil {
				return pErr
			}
			if matched {
				sawProgress = true
			}
			continue
		}
		if lErr := tr.parseLifecycleFrame(f, wantJobID); lErr != nil {
			return lErr
		}
		if sawStarted || f.Event != string(domain.EventJobStarted) {
			return fmt.Errorf("%s unexpected pre-cancel event (%s, %q), want single job.started", tr.who, f.Event, wantJobID)
		}
		sawStarted = true
	}
	return nil
}

func (tr *scanSSETracker) waitTerminal(wantJobID, wantTerminal string) error {
	for {
		f, err := tr.nextFrame()
		if err != nil {
			return err
		}
		if f.Event == domain.ProgressTickEventName {
			if _, pErr := tr.handleProgressFrame(f, wantJobID); pErr != nil {
				return pErr
			}
			continue
		}
		if lErr := tr.parseLifecycleFrame(f, wantJobID); lErr != nil {
			return lErr
		}
		if f.Event != wantTerminal {
			return fmt.Errorf("%s expected %s for %q, got %s", tr.who, wantTerminal, wantJobID, f.Event)
		}
		tr.priorAuthorized[wantJobID] = true
		return nil
	}
}

func TestScanSSECollector_ToleratesDelayedAuthorizedProgressAndRejectsHiddenOrOutOfOrder(t *testing.T) {
	t.Parallel()

	lifecycleFrame := func(seq int64, kind domain.EventKind, jobID string) sseFrame {
		return sseFrame{
			ID:    strconv.FormatInt(seq, 10),
			Event: string(kind),
			Data:  fmt.Sprintf(`{"sequence":%d,"kind":%q,"aggregate_id":%q}`, seq, kind, jobID),
		}
	}
	progressFrame := func(id, jobID string, count int64, path string) sseFrame {
		return sseFrame{
			ID:    id,
			Event: domain.ProgressTickEventName,
			Data:  fmt.Sprintf(`{"job_id":%q,"kind":"SCAN","scanned_count":%d,"current_path":%q}`, jobID, count, path),
		}
	}

	t.Run("tolerates delayed ticks from earlier authorized job across subsequent phases", func(t *testing.T) {
		frames := []sseFrame{
			// job-1 finishes after first progress tick, leaving a second buffered tick behind job.finished.
			lifecycleFrame(1, domain.EventJobStarted, "job-1"),
			progressFrame("", "job-1", 1, "Series/01.cbz"),
			lifecycleFrame(2, domain.EventJobFinished, "job-1"),
			// Delayed buffered tick from job-1 arrives before job-2 starts, then another between job-2 start and progress.
			progressFrame("", "job-1", 2, "Series/02.cbz"),
			lifecycleFrame(3, domain.EventJobStarted, "job-2"),
			progressFrame("", "job-1", 2, "Series/02.cbz"),
			progressFrame("", "job-2", 1, "Next/01.cbz"),
			lifecycleFrame(4, domain.EventJobFinished, "job-2"),
		}
		tr := newSliceScanSSETracker(frames)
		if err := tr.collectFinishedWithProgress("job-1"); err != nil {
			t.Fatalf("phase 1 failed: %v", err)
		}
		if err := tr.collectFinishedWithProgress("job-2"); err != nil {
			t.Fatalf("phase 2 failed on delayed authorized progress: %v", err)
		}
	})

	t.Run("rejects progress tick for hidden or unauthorized job", func(t *testing.T) {
		frames := []sseFrame{
			lifecycleFrame(1, domain.EventJobStarted, "job-1"),
			progressFrame("", "job-1", 1, "Series/01.cbz"),
			lifecycleFrame(2, domain.EventJobFinished, "job-1"),
			// Unauthorized automatic job tick arrives before job-2.
			progressFrame("", "job-auto", 1, "Hidden/01.cbz"),
			lifecycleFrame(3, domain.EventJobStarted, "job-2"),
			progressFrame("", "job-2", 1, "Next/01.cbz"),
			lifecycleFrame(4, domain.EventJobFinished, "job-2"),
		}
		tr := newSliceScanSSETracker(frames)
		if err := tr.collectFinishedWithProgress("job-1"); err != nil {
			t.Fatalf("phase 1 failed: %v", err)
		}
		if err := tr.collectFinishedWithProgress("job-2"); err == nil || !strings.Contains(err.Error(), "unauthorized progress tick") {
			t.Fatalf("expected unauthorized progress error for job-auto, got %v", err)
		}
	})

	t.Run("rejects progress frame with SSE ID, unexpected lifecycle job, and out-of-order lifecycle", func(t *testing.T) {
		badProgressID := newSliceScanSSETracker([]sseFrame{
			lifecycleFrame(1, domain.EventJobStarted, "job-1"),
			progressFrame("99", "job-1", 1, "Series/01.cbz"),
			lifecycleFrame(2, domain.EventJobFinished, "job-1"),
		})
		if err := badProgressID.collectFinishedWithProgress("job-1"); err == nil || !strings.Contains(err.Error(), "carried SSE ID") {
			t.Fatalf("expected SSE ID rejection on progress frame, got %v", err)
		}

		hiddenLifecycle := newSliceScanSSETracker([]sseFrame{
			lifecycleFrame(1, domain.EventJobStarted, "job-1"),
			progressFrame("", "job-1", 1, "Series/01.cbz"),
			lifecycleFrame(2, domain.EventJobFinished, "job-1"),
			lifecycleFrame(3, domain.EventJobStarted, "job-auto"),
		})
		if err := hiddenLifecycle.collectFinishedWithProgress("job-1"); err != nil {
			t.Fatalf("phase 1 failed: %v", err)
		}
		if err := hiddenLifecycle.collectFinishedWithProgress("job-2"); err == nil || !strings.Contains(err.Error(), "lifecycle event for job \"job-auto\"") {
			t.Fatalf("expected unexpected lifecycle job error, got %v", err)
		}

		outOfOrder := newSliceScanSSETracker([]sseFrame{
			lifecycleFrame(1, domain.EventJobFinished, "job-1"),
			progressFrame("", "job-1", 1, "Series/01.cbz"),
			lifecycleFrame(2, domain.EventJobStarted, "job-1"),
		})
		if err := outOfOrder.collectFinishedWithProgress("job-1"); err == nil || !strings.Contains(err.Error(), "want [job.started job.finished]") {
			t.Fatalf("expected out-of-order lifecycle error, got %v", err)
		}

		nonIncreasingSeq := newSliceScanSSETracker([]sseFrame{
			lifecycleFrame(5, domain.EventJobStarted, "job-1"),
			progressFrame("", "job-1", 1, "Series/01.cbz"),
			lifecycleFrame(5, domain.EventJobFinished, "job-1"),
		})
		if err := nonIncreasingSeq.collectFinishedWithProgress("job-1"); err == nil || !strings.Contains(err.Error(), "non-increasing SSE sequence") {
			t.Fatalf("expected non-increasing SSE sequence error, got %v", err)
		}
	})
}
