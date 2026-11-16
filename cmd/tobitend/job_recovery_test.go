package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"syscall"
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

func registerTestJobKindsModule(reg module.Registry) {
	_ = reg.Register(&module.SimpleModule{
		ModuleName: "test-job-recovery",
		Jobs: []module.JobKind{
			module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint},
			module.JobKindSpec{Kind: domain.JobKindRepack, Policy: domain.JobResumeRestart},
		},
	})
}

func TestRunDaemonRecoversInterruptedJobsUnderOwnershipBeforeServing(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)

	originalRegistryBuilder := buildDaemonRegistry
	originalLogger := slog.Default()
	t.Cleanup(func() {
		buildDaemonRegistry = originalRegistryBuilder
		slog.SetDefault(originalLogger)
	})

	buildDaemonRegistry = func(cfg *config.Config, services *app.Services) module.Registry {
		reg := BuildRegistryWithServices(cfg, services)
		registerTestJobKindsModule(reg)
		return reg
	}

	ctx := t.Context()
	repo := postgres.NewJobRepository(db.Pool)
	t0 := time.Date(2026, 11, 16, 14, 0, 0, 0, time.UTC)

	// Seed four interrupted RUNNING jobs before daemon startup:
	// 1) SCAN (RESUME_FROM_CHECKPOINT) with a saved checkpoint
	// 2) REPACK (RESTART) with a saved checkpoint that must be discarded
	// 3) SCAN with cancel_requested=true and a dedupe_key
	// 4) SCAN with attempts=max_attempts and a dedupe_key
	cpScan := json.RawMessage(`{"last_path":"manga/vol03.cbz","progress_pct":65}`)
	cpRepack := json.RawMessage(`{"stage":"compressing"}`)

	jScan := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: t0, RunAfter: t0}
	jRepack := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: t0.Add(time.Second), RunAfter: t0}
	dedupeCancel := "SCAN:daemon-cancel"
	jCancel := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, DedupeKey: &dedupeCancel, CreatedAt: t0.Add(2 * time.Second), RunAfter: t0}
	dedupeExhausted := "SCAN:daemon-exhausted"
	jExhausted := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 1, DedupeKey: &dedupeExhausted, CreatedAt: t0.Add(3 * time.Second), RunAfter: t0}

	for _, j := range []*domain.Job{jScan, jRepack, jCancel, jExhausted} {
		if _, err := repo.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue(%s): %v", j.Kind, err)
		}
	}

	cScan, err := repo.ClaimNext(ctx, domain.JobKindScan, t0)
	if err != nil {
		t.Fatalf("ClaimNext scan: %v", err)
	}
	if _, err := repo.SaveCheckpoint(ctx, jScan.ID, cScan.Attempts, cpScan, t0); err != nil {
		t.Fatalf("SaveCheckpoint scan: %v", err)
	}

	cRepack, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0)
	if err != nil {
		t.Fatalf("ClaimNext repack: %v", err)
	}
	if _, err := repo.SaveCheckpoint(ctx, jRepack.ID, cRepack.Attempts, cpRepack, t0); err != nil {
		t.Fatalf("SaveCheckpoint repack: %v", err)
	}

	if _, err := repo.ClaimNext(ctx, domain.JobKindScan, t0); err != nil {
		t.Fatalf("ClaimNext cancel: %v", err)
	}
	if _, err := repo.RequestCancel(ctx, jCancel.ID, t0); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	if _, err := repo.ClaimNext(ctx, domain.JobKindScan, t0); err != nil {
		t.Fatalf("ClaimNext exhausted: %v", err)
	}

	serveCalled := false
	err = runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		serveCalled = true

		// Ownership lock must be held during recovery and serving.
		pids := queryDaemonLockPIDs(t, db)
		if len(pids) != 1 {
			t.Fatalf("expected 1 daemon lock session, got %v", pids)
		}

		// No RUNNING rows may survive recovery before serving/claims begin.
		var runningCount int
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM jobs WHERE state = 'RUNNING'`).Scan(&runningCount); err != nil {
			return err
		}
		if runningCount != 0 {
			return fmt.Errorf("runningCount = %d, want 0", runningCount)
		}

		gotScan, err := repo.GetJobByID(serveCtx, jScan.ID)
		if err != nil {
			return err
		}
		if gotScan.State != domain.JobStateQueued || gotScan.Attempts != 1 {
			return fmt.Errorf("gotScan = %+v, want QUEUED attempts=1", gotScan)
		}
		var scanMap map[string]any
		if err := json.Unmarshal(gotScan.Checkpoint, &scanMap); err != nil || scanMap["last_path"] != "manga/vol03.cbz" {
			return fmt.Errorf("scan checkpoint lost: %s (err=%v)", gotScan.Checkpoint, err)
		}

		gotRepack, err := repo.GetJobByID(serveCtx, jRepack.ID)
		if err != nil {
			return err
		}
		if gotRepack.State != domain.JobStateQueued || gotRepack.Attempts != 1 || string(gotRepack.Checkpoint) != "{}" {
			return fmt.Errorf("gotRepack = %+v, want QUEUED attempts=1 checkpoint={}", gotRepack)
		}

		gotCancel, err := repo.GetJobByID(serveCtx, jCancel.ID)
		if err != nil || gotCancel.State != domain.JobStateCancelled {
			return fmt.Errorf("gotCancel = (%+v, %v), want CANCELLED", gotCancel, err)
		}
		gotExhausted, err := repo.GetJobByID(serveCtx, jExhausted.ID)
		if err != nil || gotExhausted.State != domain.JobStateFailed {
			return fmt.Errorf("gotExhausted = (%+v, %v), want FAILED", gotExhausted, err)
		}

		// Dedupe keys of resolved cancelled and exhausted rows must be free.
		for _, key := range []string{dedupeCancel, dedupeExhausted} {
			probe := &domain.Job{Kind: domain.JobKindScan, DedupeKey: &key, CreatedAt: t0.Add(time.Minute)}
			if created, err := repo.Enqueue(serveCtx, probe); err != nil || !created {
				return fmt.Errorf("dedupe key %q still blocked: created=%v err=%v", key, created, err)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon failed: %v", err)
	}
	if !serveCalled {
		t.Fatal("expected serve callback to run after recovery")
	}
}

// TestJobRecoveryHelperProcess is a subprocess entrypoint used by
// TestDaemonProcessInterruptionCheckpointResumeAndRestart to simulate SIGKILL mid-run
// and subsequent restart recovery under real daemon ownership.
func TestJobRecoveryHelperProcess(t *testing.T) {
	mode := os.Getenv("TOBITEN_JOB_HELPER_MODE")
	if mode == "" {
		return
	}
	dbURL := os.Getenv("TOBITEN_JOB_HELPER_DB_URL")
	if dbURL == "" {
		t.Fatal("missing TOBITEN_JOB_HELPER_DB_URL")
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + dbURL,
		"TOBITEN_ADMIN_USER=alice",
		"TOBITEN_ADMIN_PASS=initial-password",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	d, err := openDaemonStorage(ctx, cfg)
	if d != nil {
		defer func() { d.finishCleanup(slog.Default()) }()
	}
	if err != nil {
		t.Fatalf("openDaemonStorage: %v", err)
	}

	stopped := make(chan struct{})
	defer func() {
		cancel(nil)
		<-stopped
	}()
	go func() {
		defer close(stopped)
		monitorOwnership(ctx, d.owner, cancel)
	}()

	reg := BuildRegistryWithServices(cfg, nil)
	registerTestJobKindsModule(reg)

	repo := postgres.NewJobRepository(d.pool)
	runner := jobs.NewRunner(repo, jobs.RunnerOptions{Logger: slog.Default()})
	if err := runner.RegisterModuleKinds(reg); err != nil {
		t.Fatalf("RegisterModuleKinds: %v", err)
	}

	switch mode {
	case "crash":
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("crash helper Recover: %v", err)
		}

		// Claim both jobs, persist checkpoints, then block until parent sends SIGKILL.
		scanReady := make(chan struct{})
		repackReady := make(chan struct{})
		if err := runner.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			Handler: jobs.HandlerFunc(func(runCtx context.Context, exec *jobs.Execution) error {
				if err := exec.SaveCheckpointValue(runCtx, map[string]any{
					"cursor":       "page-12",
					"progress_pct": 60,
				}); err != nil {
					return err
				}
				close(scanReady)
				<-runCtx.Done()
				return runCtx.Err()
			}),
		}); err != nil {
			t.Fatalf("RegisterKind SCAN: %v", err)
		}
		if err := runner.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindRepack,
			ResumePolicy: domain.JobResumeRestart,
			Handler: jobs.HandlerFunc(func(runCtx context.Context, exec *jobs.Execution) error {
				if err := exec.SaveCheckpointValue(runCtx, map[string]any{
					"stage": "should-be-discarded",
				}); err != nil {
					return err
				}
				close(repackReady)
				<-runCtx.Done()
				return runCtx.Err()
			}),
		}); err != nil {
			t.Fatalf("RegisterKind REPACK: %v", err)
		}

		go func() { _, _ = runner.ClaimAndRunNext(ctx, domain.JobKindScan) }()
		go func() { _, _ = runner.ClaimAndRunNext(ctx, domain.JobKindRepack) }()

		select {
		case <-scanReady:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for scan checkpoint")
		}
		select {
		case <-repackReady:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for repack checkpoint")
		}

		fmt.Println("HELPER_READY")
		<-ctx.Done()

	case "recover":
		var resumedCursor string
		var resumedAttempts int
		var restartedCheckpoint string
		var restartedAttempts int

		if err := runner.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			Handler: jobs.HandlerFunc(func(_ context.Context, exec *jobs.Execution) error {
				resumedAttempts = exec.Job().Attempts
				var cp struct {
					Cursor string `json:"cursor"`
				}
				if err := exec.DecodeCheckpoint(&cp); err != nil {
					return err
				}
				resumedCursor = cp.Cursor
				return nil
			}),
		}); err != nil {
			t.Fatalf("RegisterKind SCAN: %v", err)
		}
		if err := runner.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindRepack,
			ResumePolicy: domain.JobResumeRestart,
			Handler: jobs.HandlerFunc(func(_ context.Context, exec *jobs.Execution) error {
				restartedAttempts = exec.Job().Attempts
				restartedCheckpoint = string(exec.Checkpoint())
				return nil
			}),
		}); err != nil {
			t.Fatalf("RegisterKind REPACK: %v", err)
		}

		report, err := runner.Recover(ctx)
		if err != nil {
			t.Fatalf("recover helper Recover: %v", err)
		}
		if report.TotalRunning != 2 || report.RequeuedCheckpoint != 1 || report.RequeuedRestart != 1 {
			t.Fatalf("unexpected recovery report: %+v", report)
		}

		// Verify 0 RUNNING rows survive recovery BEFORE any normal claim.
		var runningCount int
		if err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE state = 'RUNNING'`).Scan(&runningCount); err != nil {
			t.Fatalf("count running jobs: %v", err)
		}
		if runningCount != 0 {
			t.Fatalf("runningCount after recovery = %d, want 0", runningCount)
		}

		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext(SCAN) = (%v, %v)", claimed, err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindRepack); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext(REPACK) = (%v, %v)", claimed, err)
		}

		if resumedCursor != "page-12" || resumedAttempts != 2 {
			t.Fatalf("resumed SCAN = (cursor=%q, attempts=%d), want (page-12, 2)", resumedCursor, resumedAttempts)
		}
		if restartedCheckpoint != "{}" || restartedAttempts != 2 {
			t.Fatalf("restarted REPACK = (checkpoint=%q, attempts=%d), want ({}, 2)", restartedCheckpoint, restartedAttempts)
		}
		fmt.Println("HELPER_RECOVERED")

	default:
		t.Fatalf("unknown TOBITEN_JOB_HELPER_MODE=%q", mode)
	}
}

func TestDaemonProcessInterruptionCheckpointResumeAndRestart(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)

	repo := postgres.NewJobRepository(db.Pool)
	t0 := time.Now().UTC()

	scanJob := &domain.Job{
		Kind:        domain.JobKindScan,
		MaxAttempts: 3,
		CreatedAt:   t0,
		RunAfter:    t0,
	}
	repackJob := &domain.Job{
		Kind:        domain.JobKindRepack,
		MaxAttempts: 3,
		CreatedAt:   t0,
		RunAfter:    t0,
	}
	if _, err := repo.Enqueue(t.Context(), scanJob); err != nil {
		t.Fatalf("Enqueue scanJob: %v", err)
	}
	if _, err := repo.Enqueue(t.Context(), repackJob); err != nil {
		t.Fatalf("Enqueue repackJob: %v", err)
	}

	exePath, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	// 1. Start crash subprocess: acquires daemonLockID, claims both jobs, saves checkpoints, blocks.
	crashCmd := exec.Command(exePath, "-test.run=^TestJobRecoveryHelperProcess$")
	crashCmd.Env = append(os.Environ(),
		"TOBITEN_TEST_DATABASE_URL="+db.URL,
		"TOBITEN_JOB_HELPER_MODE=crash",
		"TOBITEN_JOB_HELPER_DB_URL="+db.URL,
	)
	stdout, err := crashCmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	var crashStderr syncBuffer
	crashCmd.Stderr = &crashStderr
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
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.TrimSpace(scanner.Text()) == "HELPER_READY" {
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
			t.Fatalf("crash helper exited before HELPER_READY; stderr:\n%s", crashStderr.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for crash helper; stderr:\n%s", crashStderr.String())
	}

	// Confirm both jobs are RUNNING at attempt 1 before killing the process.
	running, err := repo.ListRunningJobs(t.Context())
	if err != nil || len(running) != 2 {
		t.Fatalf("running jobs before SIGKILL = (%d, %v), want 2", len(running), err)
	}

	// 2. Hard-kill the process mid-run (SIGKILL).
	if err := crashCmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL crash helper: %v", err)
	}
	waitErr := crashCmd.Wait()
	if waitErr == nil {
		t.Fatal("expected non-nil error after SIGKILL")
	}

	// 3. Start recovery subprocess: acquires daemonLockID, recovers RUNNING rows, resumes SCAN & restarts REPACK.
	recoverCmd := exec.Command(exePath, "-test.run=^TestJobRecoveryHelperProcess$")
	recoverCmd.Env = append(os.Environ(),
		"TOBITEN_TEST_DATABASE_URL="+db.URL,
		"TOBITEN_JOB_HELPER_MODE=recover",
		"TOBITEN_JOB_HELPER_DB_URL="+db.URL,
	)
	out, err := recoverCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("recover helper failed: %v\noutput:\n%s", err, out)
	}
	if !strings.Contains(string(out), "HELPER_RECOVERED") {
		t.Fatalf("recover helper output missing HELPER_RECOVERED:\n%s", out)
	}

	// 4. Verify final PostgreSQL state: 0 RUNNING, both SUCCEEDED with monotonic attempt=2.
	finalRunning, err := repo.ListRunningJobs(t.Context())
	if err != nil || len(finalRunning) != 0 {
		t.Fatalf("final running jobs = (%d, %v), want 0", len(finalRunning), err)
	}
	finalScan, err := repo.GetJobByID(t.Context(), scanJob.ID)
	if err != nil || finalScan.State != domain.JobStateSucceeded || finalScan.Attempts != 2 {
		t.Fatalf("finalScan = (%+v, %v), want SUCCEEDED attempts=2", finalScan, err)
	}
	finalRepack, err := repo.GetJobByID(t.Context(), repackJob.ID)
	if err != nil || finalRepack.State != domain.JobStateSucceeded || finalRepack.Attempts != 2 || string(finalRepack.Checkpoint) != "{}" {
		t.Fatalf("finalRepack = (%+v, %v), want SUCCEEDED attempts=2 checkpoint={}", finalRepack, err)
	}
}
