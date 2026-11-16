package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

type testJobRegistrarModule struct {
	*module.SimpleModule
	register func(r *jobs.Runner) error
}

func (m *testJobRegistrarModule) RegisterJobs(r *jobs.Runner) error {
	return m.register(r)
}

func TestRunDaemonBoundedWorkerPoolsNoStarvationAndJoinBeforeCleanup(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)
	putSetting(t, db, "jobs.pool.repack", "1")

	originalRegistryBuilder := buildDaemonRegistry
	originalConfigHook := configureDaemonJobRunner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		buildDaemonRegistry = originalRegistryBuilder
		configureDaemonJobRunner = originalConfigHook
		slog.SetDefault(originalLogger)
	})

	const scanCap = 2
	const totalScan = scanCap + 1 // N+1 queued/recovered jobs of SCAN

	var activeScan atomic.Int32
	var maxActiveScan atomic.Int32
	var scanCompleted atomic.Int32
	var resumedCursor atomic.Value
	scanEntered := make(chan struct{}, totalScan+2)
	releaseScan := make(chan struct{})

	var holdMode atomic.Bool
	holdEntered := make(chan struct{}, scanCap)
	serveReturning := make(chan struct{})
	allowHoldExit := make(chan struct{})

	repackDone := make(chan string, 2)

	var seenScanPoolSize, seenRepackPoolSize int
	var runnerHasRecoveredBeforeServe atomic.Bool

	buildDaemonRegistry = func(cfg *config.Config, services *app.Services) module.Registry {
		reg := BuildRegistryWithServices(cfg, services)
		_ = reg.Register(&testJobRegistrarModule{
			SimpleModule: &module.SimpleModule{
				ModuleName: "test-bounded-pools",
				Jobs: []module.JobKind{
					module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint, Workers: 5},
					module.JobKindSpec{Kind: domain.JobKindRepack, Policy: domain.JobResumeRestart, Workers: 4},
				},
			},
			register: func(r *jobs.Runner) error {
				if err := r.RegisterKind(jobs.KindConfig{
					Kind:         domain.JobKindScan,
					ResumePolicy: domain.JobResumeCheckpoint,
					Handler: jobs.HandlerFunc(func(runCtx context.Context, exec *jobs.Execution) error {
						if holdMode.Load() {
							holdEntered <- struct{}{}
							<-runCtx.Done()
							<-allowHoldExit
							return runCtx.Err()
						}

						var cp struct {
							Cursor string `json:"cursor"`
						}
						if err := exec.DecodeCheckpoint(&cp); err != nil {
							return err
						}
						if cp.Cursor != "" {
							resumedCursor.Store(cp.Cursor)
						}

						cur := activeScan.Add(1)
						defer activeScan.Add(-1)
						for {
							prev := maxActiveScan.Load()
							if cur <= prev || maxActiveScan.CompareAndSwap(prev, cur) {
								break
							}
						}

						scanEntered <- struct{}{}
						select {
						case <-releaseScan:
						case <-runCtx.Done():
							return runCtx.Err()
						}
						scanCompleted.Add(1)
						return nil
					}),
				}); err != nil {
					return err
				}
				return r.RegisterKind(jobs.KindConfig{
					Kind:         domain.JobKindRepack,
					ResumePolicy: domain.JobResumeRestart,
					Handler: jobs.HandlerFunc(func(_ context.Context, exec *jobs.Execution) error {
						repackDone <- exec.Job().ID
						return nil
					}),
				})
			},
		})
		return reg
	}

	configureDaemonJobRunner = func(r *jobs.Runner, _ module.Registry, _ *config.Config, _ *app.Services) error {
		seenScanPoolSize, _ = r.PoolSize(domain.JobKindScan)
		seenRepackPoolSize, _ = r.PoolSize(domain.JobKindRepack)
		if r.HasRecovered() {
			runnerHasRecoveredBeforeServe.Store(true)
		}
		return nil
	}

	ctx := t.Context()
	repo := postgres.NewJobRepository(db.Pool)
	t0 := time.Now().UTC().Add(-time.Minute)

	// Seed 1 interrupted RUNNING SCAN job (recovered before pools start) + 2 QUEUED SCAN jobs (= N+1 = 3)
	// and 2 QUEUED REPACK jobs.
	jScanInterrupted := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: t0, RunAfter: t0}
	if _, err := repo.Enqueue(ctx, jScanInterrupted); err != nil {
		t.Fatalf("Enqueue interrupted scan: %v", err)
	}
	claimedInterrupted, err := repo.ClaimNext(ctx, domain.JobKindScan, t0)
	if err != nil {
		t.Fatalf("ClaimNext interrupted scan: %v", err)
	}
	if _, err := repo.SaveCheckpoint(ctx, jScanInterrupted.ID, claimedInterrupted.Attempts, json.RawMessage(`{"cursor":"vol-04"}`), t0); err != nil {
		t.Fatalf("SaveCheckpoint interrupted scan: %v", err)
	}

	jScan2 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: t0.Add(time.Second), RunAfter: t0}
	jScan3 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: t0.Add(2 * time.Second), RunAfter: t0}
	jRepack1 := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: t0, RunAfter: t0}
	jRepack2 := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: t0.Add(time.Second), RunAfter: t0}
	for _, j := range []*domain.Job{jScan2, jScan3, jRepack1, jRepack2} {
		if _, err := repo.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue %s: %v", j.Kind, err)
		}
	}

	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_ADMIN_USER=alice",
		"TOBITEN_ADMIN_PASS=initial-password",
		"TOBITEN_SCANNER_WORKERS=2",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	var jHold1, jHold2, jUnclaimed *domain.Job
	daemonDone := make(chan error, 1)

	go func() {
		daemonDone <- runDaemon(ctx, cfg, func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			if runnerHasRecoveredBeforeServe.Load() {
				return errors.New("runner reported HasRecovered before Recover ran")
			}

			// 1. Wait for SCAN pool (size N=2) to saturate with 2 concurrent handlers.
			for range scanCap {
				select {
				case <-scanEntered:
				case <-time.After(10 * time.Second):
					return fmt.Errorf("timed out waiting for %d concurrent SCAN workers (active=%d)", scanCap, activeScan.Load())
				}
			}

			// Confirm at most N=2 run concurrently while the 3rd remains QUEUED.
			time.Sleep(250 * time.Millisecond)
			if got := maxActiveScan.Load(); got != scanCap {
				return fmt.Errorf("maxActiveScan = %d, want %d", got, scanCap)
			}
			var runningScanCount, queuedScanCount int
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM jobs WHERE kind = 'SCAN' AND state = 'RUNNING'`).Scan(&runningScanCount); err != nil {
				return err
			}
			if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM jobs WHERE kind = 'SCAN' AND state = 'QUEUED'`).Scan(&queuedScanCount); err != nil {
				return err
			}
			if runningScanCount != scanCap || queuedScanCount != 1 {
				return fmt.Errorf("while saturated: SCAN running=%d queued=%d, want running=%d queued=1", runningScanCount, queuedScanCount, scanCap)
			}

			// 2. While SCAN pool is saturated, REPACK pool progresses and finishes both jobs.
			for range 2 {
				select {
				case <-repackDone:
				case <-time.After(10 * time.Second):
					return errors.New("REPACK pool starved while SCAN pool saturated")
				}
			}

			// 3. Release SCAN barrier; verify the 3rd SCAN job runs and all 3 succeed.
			close(releaseScan)
			select {
			case <-scanEntered:
			case <-time.After(10 * time.Second):
				return errors.New("timed out waiting for (N+1)th SCAN job")
			}
			deadline := time.Now().Add(10 * time.Second)
			for scanCompleted.Load() < totalScan && time.Now().Before(deadline) {
				time.Sleep(15 * time.Millisecond)
			}
			if scanCompleted.Load() != totalScan {
				return fmt.Errorf("scanCompleted = %d, want %d", scanCompleted.Load(), totalScan)
			}

			// 4. Switch handler to hold mode, saturate all SCAN workers with jHold1 + jHold2,
			// enqueue jUnclaimed while saturated, then return from serve so runDaemon cancels
			// the lifecycle context and joins workers before storage cleanup.
			holdMode.Store(true)
			now := time.Now().UTC()
			jHold1 = &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now, RunAfter: now}
			jHold2 = &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now.Add(time.Millisecond), RunAfter: now}
			jUnclaimed = &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now.Add(time.Second), RunAfter: now}
			if _, err := repo.Enqueue(serveCtx, jHold1); err != nil {
				return err
			}
			if _, err := repo.Enqueue(serveCtx, jHold2); err != nil {
				return err
			}

			for range scanCap {
				select {
				case <-holdEntered:
				case <-time.After(10 * time.Second):
					return errors.New("timed out waiting for hold jobs to saturate SCAN pool")
				}
			}

			// All SCAN workers are blocked inside their handlers, so jUnclaimed cannot be claimed
			// before serve returns and runDaemon cancels the lifecycle context.
			if _, err := repo.Enqueue(serveCtx, jUnclaimed); err != nil {
				return err
			}
			close(serveReturning)
			return nil
		})
	}()

	// Wait until serve has finished step 4 and is returning into runDaemon's deferred join.
	select {
	case <-serveReturning:
	case err := <-daemonDone:
		t.Fatalf("runDaemon exited unexpectedly early: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for serveReturning")
	}

	// Give runDaemon time to enter its deferred jobRunner.Wait().
	// While allowHoldExit is still open, runDaemon MUST NOT exit and MUST still hold daemonLockID.
	select {
	case err := <-daemonDone:
		t.Fatalf("runDaemon returned before active worker exited (err=%v)", err)
	case <-time.After(150 * time.Millisecond):
	}
	if pids := queryDaemonLockPIDs(t, db); len(pids) != 1 {
		t.Fatalf("daemonLockID released before workers joined: pids=%v", pids)
	}

	// Allow the active handler to exit; runDaemon must now finish joining workers, clean up storage, and exit nil.
	close(allowHoldExit)
	select {
	case err := <-daemonDone:
		if err != nil {
			t.Fatalf("runDaemon returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for runDaemon to finish after worker exit")
	}

	if pids := queryDaemonLockPIDs(t, db); len(pids) != 0 {
		t.Fatalf("daemonLockID still held after runDaemon exit: pids=%v", pids)
	}

	if seenScanPoolSize != 2 || seenRepackPoolSize != 1 {
		t.Fatalf("pool sizes = (scan=%d, repack=%d), want (2, 1)", seenScanPoolSize, seenRepackPoolSize)
	}
	if got, _ := resumedCursor.Load().(string); got != "vol-04" {
		t.Fatalf("interrupted SCAN resumed cursor = %q, want vol-04", got)
	}
	if maxActiveScan.Load() != scanCap {
		t.Fatalf("maxActiveScan = %d, want %d", maxActiveScan.Load(), scanCap)
	}

	for _, id := range []string{jScanInterrupted.ID, jScan2.ID, jScan3.ID, jRepack1.ID, jRepack2.ID} {
		got, err := repo.GetJobByID(ctx, id)
		if err != nil || got.State != domain.JobStateSucceeded {
			t.Fatalf("job %s = (%+v, %v), want SUCCEEDED", id, got, err)
		}
	}

	for _, id := range []string{jHold1.ID, jHold2.ID} {
		gotHold, err := repo.GetJobByID(ctx, id)
		if err != nil || gotHold.State != domain.JobStateRunning || gotHold.Attempts != 1 {
			t.Fatalf("hold job %s = (%+v, %v), want RUNNING attempts=1", id, gotHold, err)
		}
	}
	gotUnclaimed, err := repo.GetJobByID(ctx, jUnclaimed.ID)
	if err != nil || gotUnclaimed.State != domain.JobStateQueued || gotUnclaimed.Attempts != 0 {
		t.Fatalf("jUnclaimed = (%+v, %v), want QUEUED attempts=0 (claims must stop on cancellation)", gotUnclaimed, err)
	}
}

func TestRunDaemonPoolConfigValidation(t *testing.T) {
	originalLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(originalLogger) })

	t.Run("invalid jobs.pool setting fails startup before serving", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)
		putSetting(t, db, "jobs.pool.scan", "0")

		served := false
		err := runDaemon(t.Context(), hostedConfig(t, db.URL), func(context.Context, *config.Config, *slog.Logger, *app.Services) error {
			served = true
			return nil
		})
		if !errors.Is(err, jobs.ErrInvalidPoolSize) {
			t.Fatalf("runDaemon err = %v, want ErrInvalidPoolSize", err)
		}
		if served {
			t.Fatal("serve must not run on invalid jobs.pool setting")
		}
	})

	t.Run("jobs.default_pool_size configures unregistered default", func(t *testing.T) {
		db := testdb.New(t)
		migrateStartupDB(t, db)
		putSetting(t, db, "jobs.default_pool_size", "3")

		originalRegistryBuilder := buildDaemonRegistry
		originalConfigHook := configureDaemonJobRunner
		t.Cleanup(func() {
			buildDaemonRegistry = originalRegistryBuilder
			configureDaemonJobRunner = originalConfigHook
		})

		buildDaemonRegistry = func(cfg *config.Config, services *app.Services) module.Registry {
			reg := BuildRegistryWithServices(cfg, services)
			_ = reg.Register(&module.SimpleModule{
				ModuleName: "default-pool-check",
				Jobs: []module.JobKind{
					module.JobKindSpec{Kind: domain.JobKindScrape, Policy: domain.JobResumeRestart},
				},
			})
			return reg
		}

		var gotSize int
		configureDaemonJobRunner = func(r *jobs.Runner, _ module.Registry, _ *config.Config, _ *app.Services) error {
			gotSize, _ = r.PoolSize(domain.JobKindScrape)
			return nil
		}

		err := runDaemon(t.Context(), hostedConfig(t, db.URL), func(context.Context, *config.Config, *slog.Logger, *app.Services) error {
			return nil
		})
		if err != nil {
			t.Fatalf("runDaemon err = %v", err)
		}
		if gotSize != 3 {
			t.Fatalf("SCRAPE pool size = %d, want 3 from jobs.default_pool_size", gotSize)
		}
	})
}
