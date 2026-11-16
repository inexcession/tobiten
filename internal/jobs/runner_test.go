package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestMain(m *testing.M) {
	os.Exit(testdb.Run(m))
}

func setupRunnerTestDB(t *testing.T) (*testdb.TestDB, *postgres.JobRepository) {
	t.Helper()
	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("migrations.RunFS: %v", err)
	}
	return db, postgres.NewJobRepository(db.Pool)
}

func TestKindRegistryAndModuleIntegration(t *testing.T) {
	t.Parallel()
	runner := jobs.NewRunner(nil, jobs.RunnerOptions{})

	if err := runner.RegisterKind(jobs.KindConfig{Kind: "", ResumePolicy: domain.JobResumeRestart}); !errors.Is(err, jobs.ErrInvalidJobKind) {
		t.Fatalf("empty kind err = %v, want ErrInvalidJobKind", err)
	}
	if err := runner.RegisterKind(jobs.KindConfig{Kind: domain.JobKindScan, ResumePolicy: domain.JobResumePolicy("BOGUS")}); !errors.Is(err, jobs.ErrInvalidJobKind) {
		t.Fatalf("invalid policy err = %v, want ErrInvalidJobKind", err)
	}

	reg := module.NewRegistry()
	modScan := &module.SimpleModule{
		ModuleName: "scanner",
		Jobs: []module.JobKind{
			module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint},
		},
	}
	modRepack := &module.SimpleModule{
		ModuleName: "repack",
		Jobs: []module.JobKind{
			module.NamedJobKind(domain.JobKindRepack),
		},
	}
	if err := reg.Register(modScan); err != nil {
		t.Fatalf("Register scanner: %v", err)
	}
	if err := reg.Register(modRepack); err != nil {
		t.Fatalf("Register repack: %v", err)
	}

	if err := runner.RegisterModuleKinds(reg); err != nil {
		t.Fatalf("RegisterModuleKinds: %v", err)
	}

	if p, ok := runner.KindPolicy(domain.JobKindScan); !ok || p != domain.JobResumeCheckpoint {
		t.Fatalf("SCAN policy = (%q, %v), want (%q, true)", p, ok, domain.JobResumeCheckpoint)
	}
	if p, ok := runner.KindPolicy(domain.JobKindRepack); !ok || p != domain.JobResumeRestart {
		t.Fatalf("REPACK policy = (%q, %v), want (%q, true)", p, ok, domain.JobResumeRestart)
	}

	// Attaching a handler to an already module-registered kind with the matching policy succeeds.
	if err := runner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeCheckpoint,
		Handler:      jobs.HandlerFunc(func(context.Context, *jobs.Execution) error { return nil }),
	}); err != nil {
		t.Fatalf("attaching handler to module kind failed: %v", err)
	}

	// Conflicting policy is rejected.
	if err := runner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeRestart,
	}); !errors.Is(err, jobs.ErrDuplicateJobKind) {
		t.Fatalf("conflicting policy err = %v, want ErrDuplicateJobKind", err)
	}

	// Duplicate kind across two modules in a registry is rejected.
	dupReg := module.NewRegistry()
	_ = dupReg.Register(&module.SimpleModule{
		ModuleName: "m1",
		Jobs:       []module.JobKind{module.JobKindSpec{Kind: domain.JobKindScrape, Policy: domain.JobResumeCheckpoint}},
	})
	_ = dupReg.Register(&module.SimpleModule{
		ModuleName: "m2",
		Jobs:       []module.JobKind{module.JobKindSpec{Kind: domain.JobKindScrape, Policy: domain.JobResumeCheckpoint}},
	})
	dupRunner := jobs.NewRunner(nil, jobs.RunnerOptions{})
	if err := dupRunner.RegisterModuleKinds(dupReg); !errors.Is(err, jobs.ErrDuplicateJobKind) {
		t.Fatalf("duplicate module kind err = %v, want ErrDuplicateJobKind", err)
	}
}

func TestRunner_ClaimBlockedBeforeRecovery(t *testing.T) {
	t.Parallel()
	_, repo := setupRunnerTestDB(t)

	runner := jobs.NewRunner(repo, jobs.RunnerOptions{})
	if err := runner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeCheckpoint,
		Handler:      jobs.HandlerFunc(func(context.Context, *jobs.Execution) error { return nil }),
	}); err != nil {
		t.Fatalf("RegisterKind: %v", err)
	}

	if runner.HasRecovered() {
		t.Fatal("expected HasRecovered() == false before Recover")
	}
	if _, err := runner.ClaimAndRunNext(t.Context(), domain.JobKindScan); !errors.Is(err, jobs.ErrRecoveryNotCompleted) {
		t.Fatalf("ClaimAndRunNext before Recover = %v, want ErrRecoveryNotCompleted", err)
	}

	report, err := runner.Recover(t.Context())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if report.TotalRunning != 0 || !runner.HasRecovered() {
		t.Fatalf("unexpected recovery state: report=%+v hasRecovered=%v", report, runner.HasRecovered())
	}

	claimed, err := runner.ClaimAndRunNext(t.Context(), domain.JobKindScan)
	if err != nil || claimed {
		t.Fatalf("ClaimAndRunNext on empty queue = (%v, %v), want (false, nil)", claimed, err)
	}
}

func TestRunner_RecoveryCheckpointResumeRestartCancelAndExhaustion(t *testing.T) {
	t.Parallel()
	db, repo := setupRunnerTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	// Step 1: Simulate a pre-crash runner that claims jobs, saves checkpoints, and gets interrupted.
	preCrashRunner := jobs.NewRunner(repo, jobs.RunnerOptions{Now: clock})
	reg := module.NewRegistry()
	if err := reg.Register(&module.SimpleModule{
		ModuleName: "core-jobs",
		Jobs: []module.JobKind{
			module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint},
			module.JobKindSpec{Kind: domain.JobKindRepack, Policy: domain.JobResumeRestart},
		},
	}); err != nil {
		t.Fatalf("reg.Register: %v", err)
	}
	if err := preCrashRunner.RegisterModuleKinds(reg); err != nil {
		t.Fatalf("RegisterModuleKinds: %v", err)
	}

	type scanCheckpoint struct {
		LastPath    string  `json:"last_path"`
		Processed   int     `json:"processed"`
		ProgressPct float64 `json:"progress_pct"`
	}

	var scanCancel context.CancelFunc
	if err := preCrashRunner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeCheckpoint,
		Handler: jobs.HandlerFunc(func(runCtx context.Context, exec *jobs.Execution) error {
			if err := exec.SaveCheckpointValue(runCtx, scanCheckpoint{
				LastPath:    "manga/ch02.cbz",
				Processed:   2,
				ProgressPct: 50,
			}); err != nil {
				return err
			}
			scanCancel()
			return runCtx.Err()
		}),
	}); err != nil {
		t.Fatalf("RegisterKind SCAN: %v", err)
	}

	var repackCancel context.CancelFunc
	if err := preCrashRunner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindRepack,
		ResumePolicy: domain.JobResumeRestart,
		Handler: jobs.HandlerFunc(func(runCtx context.Context, exec *jobs.Execution) error {
			if err := exec.SaveCheckpointValue(runCtx, map[string]any{"stage": "halfway"}); err != nil {
				return err
			}
			repackCancel()
			return runCtx.Err()
		}),
	}); err != nil {
		t.Fatalf("RegisterKind REPACK: %v", err)
	}

	var intakeCancel context.CancelFunc
	if err := preCrashRunner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindIntake,
		ResumePolicy: domain.JobResumeCheckpoint,
		Handler: jobs.HandlerFunc(func(runCtx context.Context, exec *jobs.Execution) error {
			if err := exec.SaveCheckpointValue(runCtx, map[string]any{"step": "copied"}); err != nil {
				return err
			}
			intakeCancel()
			return runCtx.Err()
		}),
	}); err != nil {
		t.Fatalf("RegisterKind INTAKE: %v", err)
	}

	if _, err := preCrashRunner.Recover(ctx); err != nil {
		t.Fatalf("preCrashRunner.Recover: %v", err)
	}

	scanJob := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now, RunAfter: now}
	repackJob := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: now.Add(time.Second), RunAfter: now}
	intakeJob := &domain.Job{Kind: domain.JobKindIntake, MaxAttempts: 1, CreatedAt: now.Add(2 * time.Second), RunAfter: now}
	if _, err := repo.Enqueue(ctx, scanJob); err != nil {
		t.Fatalf("Enqueue scanJob: %v", err)
	}
	if _, err := repo.Enqueue(ctx, repackJob); err != nil {
		t.Fatalf("Enqueue repackJob: %v", err)
	}
	if _, err := repo.Enqueue(ctx, intakeJob); err != nil {
		t.Fatalf("Enqueue intakeJob: %v", err)
	}

	var scanRunCtx, repackRunCtx, intakeRunCtx context.Context
	scanRunCtx, scanCancel = context.WithCancel(ctx)
	defer scanCancel()
	repackRunCtx, repackCancel = context.WithCancel(ctx)
	defer repackCancel()
	intakeRunCtx, intakeCancel = context.WithCancel(ctx)
	defer intakeCancel()

	if claimed, err := preCrashRunner.ClaimAndRunNext(scanRunCtx, domain.JobKindScan); !claimed || !errors.Is(err, context.Canceled) {
		t.Fatalf("ClaimAndRunNext(SCAN) = (%v, %v), want (true, context.Canceled)", claimed, err)
	}
	if claimed, err := preCrashRunner.ClaimAndRunNext(repackRunCtx, domain.JobKindRepack); !claimed || !errors.Is(err, context.Canceled) {
		t.Fatalf("ClaimAndRunNext(REPACK) = (%v, %v), want (true, context.Canceled)", claimed, err)
	}
	if claimed, err := preCrashRunner.ClaimAndRunNext(intakeRunCtx, domain.JobKindIntake); !claimed || !errors.Is(err, context.Canceled) {
		t.Fatalf("ClaimAndRunNext(INTAKE) = (%v, %v), want (true, context.Canceled)", claimed, err)
	}

	// Also create:
	// - an interrupted RUNNING job with cancel_requested=true and a dedupe_key
	// - an interrupted RUNNING job with attempts=max_attempts and a dedupe_key
	// - an interrupted RUNNING job with an unregistered kind and a dedupe_key
	dedupeCancel := "SCAN:cancelled-mid-run"
	jCancel := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, DedupeKey: &dedupeCancel, CreatedAt: now.Add(3 * time.Second), RunAfter: now}
	if _, err := repo.Enqueue(ctx, jCancel); err != nil {
		t.Fatalf("Enqueue jCancel: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindScan, now); err != nil {
		t.Fatalf("ClaimNext jCancel: %v", err)
	}
	if _, err := repo.RequestCancel(ctx, jCancel.ID, now); err != nil {
		t.Fatalf("RequestCancel jCancel: %v", err)
	}

	dedupeExhaust := "SCAN:exhausted-mid-run"
	jExhaust := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 1, DedupeKey: &dedupeExhaust, CreatedAt: now.Add(4 * time.Second), RunAfter: now}
	if _, err := repo.Enqueue(ctx, jExhaust); err != nil {
		t.Fatalf("Enqueue jExhaust: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindScan, now); err != nil {
		t.Fatalf("ClaimNext jExhaust: %v", err)
	}

	dedupeUnreg := "CUSTOM:unregistered"
	jUnreg := &domain.Job{Kind: domain.JobKind("CUSTOM_UNREGISTERED"), MaxAttempts: 3, DedupeKey: &dedupeUnreg, CreatedAt: now.Add(5 * time.Second), RunAfter: now}
	if _, err := repo.Enqueue(ctx, jUnreg); err != nil {
		t.Fatalf("Enqueue jUnreg: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKind("CUSTOM_UNREGISTERED"), now); err != nil {
		t.Fatalf("ClaimNext jUnreg: %v", err)
	}

	// Step 2: Restarted runner recovers all RUNNING rows BEFORE normal claims.
	restarted := jobs.NewRunner(repo, jobs.RunnerOptions{Now: clock})
	if err := restarted.RegisterModuleKinds(reg); err != nil {
		t.Fatalf("restarted.RegisterModuleKinds: %v", err)
	}

	var scanResumedFrom scanCheckpoint
	var scanAttemptsSeen int
	if err := restarted.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeCheckpoint,
		Handler: jobs.HandlerFunc(func(_ context.Context, exec *jobs.Execution) error {
			scanAttemptsSeen = exec.Job().Attempts
			return exec.DecodeCheckpoint(&scanResumedFrom)
		}),
	}); err != nil {
		t.Fatalf("restarted RegisterKind SCAN: %v", err)
	}

	var repackCheckpointSeen string
	var repackAttemptsSeen int
	if err := restarted.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindRepack,
		ResumePolicy: domain.JobResumeRestart,
		Handler: jobs.HandlerFunc(func(_ context.Context, exec *jobs.Execution) error {
			repackAttemptsSeen = exec.Job().Attempts
			repackCheckpointSeen = string(exec.Checkpoint())
			return nil
		}),
	}); err != nil {
		t.Fatalf("restarted RegisterKind REPACK: %v", err)
	}

	var intakeReconcilerCalled atomic.Bool
	if err := restarted.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindIntake,
		ResumePolicy: domain.JobResumeCheckpoint,
		Reconciler: jobs.ReconcileFunc(func(_ context.Context, job domain.Job) (jobs.ReconcileOutcome, error) {
			intakeReconcilerCalled.Store(true)
			return jobs.ReconcileOutcome{Action: jobs.ReconcileSucceeded}, nil
		}),
	}); err != nil {
		t.Fatalf("restarted RegisterKind INTAKE: %v", err)
	}

	report, err := restarted.Recover(ctx)
	if err != nil {
		t.Fatalf("restarted.Recover: %v", err)
	}
	if report.TotalRunning != 6 ||
		report.RequeuedCheckpoint != 1 ||
		report.RequeuedRestart != 1 ||
		report.Succeeded != 1 ||
		report.Cancelled != 1 ||
		report.Failed != 2 ||
		report.UnregisteredFailed != 1 {
		t.Fatalf("unexpected RecoveryReport: %+v", report)
	}
	if !intakeReconcilerCalled.Load() {
		t.Fatal("expected custom reconciler for INTAKE to run")
	}

	// No RUNNING rows may survive recovery.
	var runningCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE state = 'RUNNING'`).Scan(&runningCount); err != nil {
		t.Fatalf("count running jobs: %v", err)
	}
	if runningCount != 0 {
		t.Fatalf("running job count after recovery = %d, want 0", runningCount)
	}

	// Attempt numbers must remain monotonic (still 1 after requeue, before second claim).
	gotScanAfterRecover, err := repo.GetJobByID(ctx, scanJob.ID)
	if err != nil {
		t.Fatalf("GetJobByID scanJob: %v", err)
	}
	if gotScanAfterRecover.State != domain.JobStateQueued || gotScanAfterRecover.Attempts != 1 {
		t.Fatalf("scanJob after recover = %+v, want QUEUED attempts=1", gotScanAfterRecover)
	}

	// Cancelled, exhausted, and unregistered rows must NOT be QUEUED and must release their dedupe keys.
	for _, tc := range []struct {
		id     string
		dedupe string
		state  domain.JobState
	}{
		{id: jCancel.ID, dedupe: dedupeCancel, state: domain.JobStateCancelled},
		{id: jExhaust.ID, dedupe: dedupeExhaust, state: domain.JobStateFailed},
		{id: jUnreg.ID, dedupe: dedupeUnreg, state: domain.JobStateFailed},
	} {
		got, err := repo.GetJobByID(ctx, tc.id)
		if err != nil {
			t.Fatalf("GetJobByID(%s): %v", tc.id, err)
		}
		if got.State != tc.state {
			t.Fatalf("job %s state = %s, want %s", tc.id, got.State, tc.state)
		}
		probe := &domain.Job{Kind: domain.JobKindScan, DedupeKey: &tc.dedupe, CreatedAt: now}
		if created, err := repo.Enqueue(ctx, probe); err != nil || !created {
			t.Fatalf("dedupe key %q not released (created=%v, err=%v)", tc.dedupe, created, err)
		}
		// Clean up probe so ClaimAndRunNext below claims only scanJob.
		if _, err := repo.RequestCancel(ctx, probe.ID, now); err != nil {
			t.Fatalf("cancel probe %s: %v", probe.ID, err)
		}
	}

	// Step 3: Normal claims execute resumed SCAN (from checkpoint) and restarted REPACK (from empty).
	claimedScan, err := restarted.ClaimAndRunNext(ctx, domain.JobKindScan)
	if err != nil || !claimedScan {
		t.Fatalf("ClaimAndRunNext(SCAN) = (%v, %v), want (true, nil)", claimedScan, err)
	}
	if scanAttemptsSeen != 2 {
		t.Fatalf("resumed SCAN attempts = %d, want 2 (monotonic)", scanAttemptsSeen)
	}
	if scanResumedFrom.LastPath != "manga/ch02.cbz" || scanResumedFrom.Processed != 2 {
		t.Fatalf("resumed SCAN checkpoint = %+v, want LastPath=manga/ch02.cbz Processed=2", scanResumedFrom)
	}

	claimedRepack, err := restarted.ClaimAndRunNext(ctx, domain.JobKindRepack)
	if err != nil || !claimedRepack {
		t.Fatalf("ClaimAndRunNext(REPACK) = (%v, %v), want (true, nil)", claimedRepack, err)
	}
	if repackAttemptsSeen != 2 {
		t.Fatalf("restarted REPACK attempts = %d, want 2 (monotonic)", repackAttemptsSeen)
	}
	if repackCheckpointSeen != "{}" {
		t.Fatalf("restarted REPACK checkpoint = %q, want {}", repackCheckpointSeen)
	}

	finalScan, err := repo.GetJobByID(ctx, scanJob.ID)
	if err != nil || finalScan.State != domain.JobStateSucceeded || finalScan.Attempts != 2 {
		t.Fatalf("final scanJob = (%+v, %v), want SUCCEEDED attempts=2", finalScan, err)
	}
	finalRepack, err := repo.GetJobByID(ctx, repackJob.ID)
	if err != nil || finalRepack.State != domain.JobStateSucceeded || finalRepack.Attempts != 2 {
		t.Fatalf("final repackJob = (%+v, %v), want SUCCEEDED attempts=2", finalRepack, err)
	}
}

func TestRunner_ReconcilerFailureAbortsRecoveryAndHandlerOutcomePaths(t *testing.T) {
	t.Parallel()
	_, repo := setupRunnerTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	now := time.Date(2026, 11, 16, 13, 0, 0, 0, time.UTC)

	// Reconciler error leaves HasRecovered() == false.
	failingJob := &domain.Job{Kind: domain.JobKindWriteBack, MaxAttempts: 3, CreatedAt: now}
	if _, err := repo.Enqueue(ctx, failingJob); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindWriteBack, now); err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}

	sentinel := errors.New("io error inspecting temp file")
	r := jobs.NewRunner(repo, jobs.RunnerOptions{Now: func() time.Time { return now }})
	if err := r.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindWriteBack,
		ResumePolicy: domain.JobResumeCheckpoint,
		Handler:      jobs.HandlerFunc(func(context.Context, *jobs.Execution) error { return nil }),
		Reconciler: jobs.ReconcileFunc(func(context.Context, domain.Job) (jobs.ReconcileOutcome, error) {
			return jobs.ReconcileOutcome{}, sentinel
		}),
	}); err != nil {
		t.Fatalf("RegisterKind: %v", err)
	}

	if _, err := r.Recover(ctx); !errors.Is(err, sentinel) {
		t.Fatalf("Recover err = %v, want %v", err, sentinel)
	}
	if r.HasRecovered() {
		t.Fatal("HasRecovered() must remain false when Recover fails")
	}
	if _, err := r.ClaimAndRunNext(ctx, domain.JobKindWriteBack); !errors.Is(err, jobs.ErrRecoveryNotCompleted) {
		t.Fatalf("ClaimAndRunNext after failed Recover = %v, want ErrRecoveryNotCompleted", err)
	}

	// ReconcileOutcome ReconcileCancelled and ReconcileFailed paths + ErrJobCancelled / ErrPermanentFailure in handler.
	r2 := jobs.NewRunner(repo, jobs.RunnerOptions{Now: func() time.Time { return now }})
	if err := r2.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindWriteBack,
		ResumePolicy: domain.JobResumeCheckpoint,
		Reconciler: jobs.ReconcileFunc(func(_ context.Context, j domain.Job) (jobs.ReconcileOutcome, error) {
			return jobs.ReconcileOutcome{
				Action:     jobs.ReconcileCancelled,
				Checkpoint: json.RawMessage(`{"reconciled":true}`),
				Error:      "aborted during recovery",
			}, nil
		}),
	}); err != nil {
		t.Fatalf("RegisterKind r2 writeback: %v", err)
	}
	if err := r2.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScrape,
		ResumePolicy: domain.JobResumeRestart,
		Handler: jobs.HandlerFunc(func(runCtx context.Context, exec *jobs.Execution) error {
			cancelled, err := exec.CheckCancelled(runCtx)
			if err != nil {
				return err
			}
			if cancelled {
				return jobs.ErrJobCancelled
			}
			return jobs.ErrPermanentFailure
		}),
	}); err != nil {
		t.Fatalf("RegisterKind r2 scrape: %v", err)
	}

	rep, err := r2.Recover(ctx)
	if err != nil {
		t.Fatalf("r2.Recover: %v", err)
	}
	if rep.Cancelled != 1 {
		t.Fatalf("r2 report = %+v, want Cancelled=1", rep)
	}
	gotWB, err := repo.GetJobByID(ctx, failingJob.ID)
	if err != nil || gotWB.State != domain.JobStateCancelled {
		t.Fatalf("gotWB = (%+v, %v), want CANCELLED", gotWB, err)
	}

	permFailJob := &domain.Job{Kind: domain.JobKindScrape, MaxAttempts: 5, CreatedAt: now}
	if _, err := repo.Enqueue(ctx, permFailJob); err != nil {
		t.Fatalf("Enqueue permFailJob: %v", err)
	}
	if claimed, err := r2.ClaimAndRunNext(ctx, domain.JobKindScrape); !claimed || err != nil {
		t.Fatalf("ClaimAndRunNext permFailJob = (%v, %v), want (true, nil)", claimed, err)
	}
	gotPerm, err := repo.GetJobByID(ctx, permFailJob.ID)
	if err != nil || gotPerm.State != domain.JobStateFailed || gotPerm.Attempts != 1 {
		t.Fatalf("gotPerm = (%+v, %v), want FAILED attempts=1", gotPerm, err)
	}

	// Verify SaveCheckpoint refuses writes when ctx is cancelled.
	cancelCheckJob := &domain.Job{Kind: domain.JobKindScrape, MaxAttempts: 2, CreatedAt: now}
	if _, err := repo.Enqueue(ctx, cancelCheckJob); err != nil {
		t.Fatalf("Enqueue cancelCheckJob: %v", err)
	}
	r3 := jobs.NewRunner(repo, jobs.RunnerOptions{Now: func() time.Time { return now }})
	var saveErr error
	if err := r3.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScrape,
		ResumePolicy: domain.JobResumeRestart,
		Handler: jobs.HandlerFunc(func(runCtx context.Context, exec *jobs.Execution) error {
			cCtx, stop := context.WithCancel(runCtx)
			stop()
			saveErr = exec.SaveCheckpointValue(cCtx, map[string]int{"x": 1})
			return nil
		}),
	}); err != nil {
		t.Fatalf("RegisterKind r3: %v", err)
	}
	if _, err := r3.Recover(ctx); err != nil {
		t.Fatalf("r3.Recover: %v", err)
	}
	if claimed, err := r3.ClaimAndRunNext(ctx, domain.JobKindScrape); !claimed || err != nil {
		t.Fatalf("ClaimAndRunNext r3 = (%v, %v)", claimed, err)
	}
	if !errors.Is(saveErr, context.Canceled) {
		t.Fatalf("SaveCheckpoint with cancelled ctx = %v, want context.Canceled", saveErr)
	}
}
