package jobs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
)

func waitForJobSucceeded(ctx context.Context, t *testing.T, repo storage.JobRepository, id string) *domain.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := repo.GetJobByID(ctx, id)
		if err == nil && got.State == domain.JobStateSucceeded {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := repo.GetJobByID(ctx, id)
	t.Fatalf("job %s = (%+v, %v), want state %s", id, got, err, domain.JobStateSucceeded)
	return nil
}

type registrarModule struct {
	*module.SimpleModule
	onRegister func(r *jobs.Runner) error
}

func (m *registrarModule) RegisterJobs(r *jobs.Runner) error {
	return m.onRegister(r)
}

func TestRunner_PoolSizeResolutionAndStartValidation(t *testing.T) {
	t.Parallel()
	_, repo := setupRunnerTestDB(t)

	t.Run("requires recovery and valid lifecycle context", func(t *testing.T) {
		r := jobs.NewRunner(repo, jobs.RunnerOptions{})
		if err := r.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			Handler:      jobs.HandlerFunc(func(context.Context, *jobs.Execution) error { return nil }),
		}); err != nil {
			t.Fatalf("RegisterKind: %v", err)
		}

		if err := r.Start(t.Context()); !errors.Is(err, jobs.ErrRecoveryNotCompleted) {
			t.Fatalf("Start before Recover = %v, want ErrRecoveryNotCompleted", err)
		}

		if _, err := r.Recover(t.Context()); err != nil {
			t.Fatalf("Recover: %v", err)
		}

		//nolint:staticcheck // testing explicit nil context guard
		if err := r.Start(nil); err == nil {
			t.Fatal("Start(nil) expected error, got nil")
		}

		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := r.Start(cancelledCtx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Start(cancelledCtx) = %v, want context.Canceled", err)
		}
	})

	t.Run("rejects negative pool sizes and conflicting overrides", func(t *testing.T) {
		r := jobs.NewRunner(repo, jobs.RunnerOptions{})
		if err := r.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			PoolSize:     -1,
		}); !errors.Is(err, jobs.ErrInvalidPoolSize) {
			t.Fatalf("negative KindConfig.PoolSize err = %v, want ErrInvalidPoolSize", err)
		}

		if err := r.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			PoolSize:     3,
		}); err != nil {
			t.Fatalf("RegisterKind SCAN: %v", err)
		}
		if err := r.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			PoolSize:     4,
		}); !errors.Is(err, jobs.ErrDuplicateJobKind) {
			t.Fatalf("conflicting PoolSize err = %v, want ErrDuplicateJobKind", err)
		}

		rBadOpt := jobs.NewRunner(repo, jobs.RunnerOptions{DefaultPoolSize: -2})
		if _, err := rBadOpt.Recover(t.Context()); err != nil {
			t.Fatalf("Recover rBadOpt: %v", err)
		}
		if err := rBadOpt.Start(t.Context()); !errors.Is(err, jobs.ErrInvalidPoolSize) {
			t.Fatalf("Start with negative DefaultPoolSize = %v, want ErrInvalidPoolSize", err)
		}

		rBadMap := jobs.NewRunner(repo, jobs.RunnerOptions{
			PoolSizes: map[domain.JobKind]int{domain.JobKindRepack: -1},
		})
		if _, err := rBadMap.Recover(t.Context()); err != nil {
			t.Fatalf("Recover rBadMap: %v", err)
		}
		if err := rBadMap.Start(t.Context()); !errors.Is(err, jobs.ErrInvalidPoolSize) {
			t.Fatalf("Start with negative PoolSizes entry = %v, want ErrInvalidPoolSize", err)
		}
	})

	t.Run("pool size precedence and double-start protection", func(t *testing.T) {
		reg := module.NewRegistry()
		if err := reg.Register(&module.SimpleModule{
			ModuleName: "jobs-mod",
			Jobs: []module.JobKind{
				module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint, Workers: 4},
				module.JobKindSpec{Kind: domain.JobKindRepack, Policy: domain.JobResumeRestart, Workers: 5},
				module.JobKindSpec{Kind: domain.JobKindScrape, Policy: domain.JobResumeRestart},
			},
		}); err != nil {
			t.Fatalf("reg.Register: %v", err)
		}

		r := jobs.NewRunner(repo, jobs.RunnerOptions{
			DefaultPoolSize: 3,
			PoolSizes: map[domain.JobKind]int{
				domain.JobKindScan: 1, // Overrides module Workers=4
			},
			PollInterval: 20 * time.Millisecond,
		})
		if err := r.RegisterModuleKinds(reg); err != nil {
			t.Fatalf("RegisterModuleKinds: %v", err)
		}

		if sz, ok := r.PoolSize(domain.JobKindScan); !ok || sz != 1 {
			t.Fatalf("SCAN PoolSize = (%d, %v), want (1, true)", sz, ok)
		}
		if sz, ok := r.PoolSize(domain.JobKindRepack); !ok || sz != 5 {
			t.Fatalf("REPACK PoolSize = (%d, %v), want (5, true)", sz, ok)
		}
		if sz, ok := r.PoolSize(domain.JobKindScrape); !ok || sz != 3 {
			t.Fatalf("SCRAPE PoolSize = (%d, %v), want (3, true)", sz, ok)
		}

		// Default runner uses DefaultWorkerPoolSize (2) when unset.
		rDef := jobs.NewRunner(repo, jobs.RunnerOptions{})
		_ = rDef.RegisterKind(jobs.KindConfig{Kind: domain.JobKindPurge, ResumePolicy: domain.JobResumeRestart})
		if sz, ok := rDef.PoolSize(domain.JobKindPurge); !ok || sz != jobs.DefaultWorkerPoolSize {
			t.Fatalf("default PoolSize = (%d, %v), want (%d, true)", sz, ok, jobs.DefaultWorkerPoolSize)
		}

		// Attach one handler so Start spawns a live pool, then verify ErrPoolsAlreadyRunning.
		if err := r.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			Handler:      jobs.HandlerFunc(func(context.Context, *jobs.Execution) error { return nil }),
		}); err != nil {
			t.Fatalf("RegisterKind SCAN handler: %v", err)
		}
		if _, err := r.Recover(t.Context()); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if err := r.Start(t.Context()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer r.Stop()

		if err := r.Start(t.Context()); !errors.Is(err, jobs.ErrPoolsAlreadyRunning) {
			t.Fatalf("second Start = %v, want ErrPoolsAlreadyRunning", err)
		}
		if err := r.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindIntake,
			ResumePolicy: domain.JobResumeCheckpoint,
		}); !errors.Is(err, jobs.ErrPoolsAlreadyRunning) {
			t.Fatalf("RegisterKind while running = %v, want ErrPoolsAlreadyRunning", err)
		}
	})
}

func TestRunner_BoundedPoolCapAndNoCrossKindStarvation(t *testing.T) {
	t.Parallel()
	_, repo := setupRunnerTestDB(t)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	const scanPoolSize = 3
	const totalScanJobs = scanPoolSize + 1 // N+1 queued jobs of one kind

	var activeScan atomic.Int32
	var maxActiveScan atomic.Int32
	var completedScan atomic.Int32
	scanEntered := make(chan struct{}, totalScanJobs)
	releaseScan := make(chan struct{})

	var completedRepack atomic.Int32
	repackDone := make(chan string, 3)

	runner := jobs.NewRunner(repo, jobs.RunnerOptions{
		PollInterval: 20 * time.Millisecond,
		PoolSizes: map[domain.JobKind]int{
			domain.JobKindScan: scanPoolSize,
		},
	})
	if err := runner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeCheckpoint,
		Handler: jobs.HandlerFunc(func(runCtx context.Context, _ *jobs.Execution) error {
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
			completedScan.Add(1)
			return nil
		}),
	}); err != nil {
		t.Fatalf("RegisterKind SCAN: %v", err)
	}
	if err := runner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindRepack,
		ResumePolicy: domain.JobResumeRestart,
		PoolSize:     1,
		Handler: jobs.HandlerFunc(func(_ context.Context, exec *jobs.Execution) error {
			completedRepack.Add(1)
			repackDone <- exec.Job().ID
			return nil
		}),
	}); err != nil {
		t.Fatalf("RegisterKind REPACK: %v", err)
	}

	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	now := time.Now().UTC()
	scanIDs := make([]string, totalScanJobs)
	for i := range totalScanJobs {
		j := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now.Add(time.Duration(i) * time.Millisecond), RunAfter: now}
		if _, err := repo.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue scan %d: %v", i, err)
		}
		scanIDs[i] = j.ID
	}

	repackJobs := make([]*domain.Job, 2)
	for i := range repackJobs {
		j := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: now.Add(time.Duration(i) * time.Millisecond), RunAfter: now}
		if _, err := repo.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue repack %d: %v", i, err)
		}
		repackJobs[i] = j
	}

	if err := runner.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer runner.Stop()

	// Wait until all N workers in the SCAN pool are simultaneously saturated.
	for range scanPoolSize {
		select {
		case <-scanEntered:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %d concurrent SCAN workers (active=%d)", scanPoolSize, activeScan.Load())
		}
	}

	// Give the runner enough poll cycles to prove the (N+1)th SCAN job does NOT start while saturated.
	time.Sleep(80 * time.Millisecond)
	if got := maxActiveScan.Load(); got != scanPoolSize {
		t.Fatalf("maxActiveScan = %d, want %d", got, scanPoolSize)
	}
	if got := activeScan.Load(); got != scanPoolSize {
		t.Fatalf("activeScan = %d, want %d", got, scanPoolSize)
	}
	select {
	case <-scanEntered:
		t.Fatal("more than N SCAN jobs entered handler concurrently")
	default:
	}

	// While SCAN is saturated, REPACK jobs must run and finish without starvation.
	for range 2 {
		select {
		case <-repackDone:
		case <-time.After(5 * time.Second):
			t.Fatal("REPACK pool starved while SCAN pool was saturated")
		}
	}

	// Enqueue an extra REPACK job while SCAN is still saturated and wake it via Notify.
	extraRepack := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: now.Add(time.Second), RunAfter: now}
	if _, err := repo.Enqueue(ctx, extraRepack); err != nil {
		t.Fatalf("Enqueue extraRepack: %v", err)
	}
	runner.Notify(domain.JobKindRepack)
	select {
	case id := <-repackDone:
		if id != extraRepack.ID {
			t.Fatalf("repackDone id = %q, want %q", id, extraRepack.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("extra REPACK job starved while SCAN pool was saturated")
	}
	if completedRepack.Load() != 3 {
		t.Fatalf("completedRepack = %d, want 3", completedRepack.Load())
	}

	// Wait for persisted SUCCEEDED state in PostgreSQL (not just in-handler counters).
	for _, j := range append(repackJobs, extraRepack) {
		waitForJobSucceeded(ctx, t, repo, j.ID)
	}

	// Unblock SCAN workers and verify the (N+1)th job executes and all N+1 persist SUCCEEDED.
	close(releaseScan)
	select {
	case <-scanEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for (N+1)th SCAN job to start after release")
	}

	for _, id := range scanIDs {
		waitForJobSucceeded(ctx, t, repo, id)
	}
	if completedScan.Load() != totalScanJobs {
		t.Fatalf("completedScan = %d, want %d", completedScan.Load(), totalScanJobs)
	}
	if maxActiveScan.Load() != scanPoolSize {
		t.Fatalf("maxActiveScan = %d, want %d", maxActiveScan.Load(), scanPoolSize)
	}

	runner.Stop()
	// Verify Start succeeds again cleanly after Stop.
	if err := runner.Start(ctx); err != nil {
		t.Fatalf("restart after Stop failed: %v", err)
	}
	runner.Stop()
}

func TestRunner_PoolCancellationStopsClaimsAndJoinsWorkers(t *testing.T) {
	t.Parallel()
	_, repo := setupRunnerTestDB(t)

	poolCtx, cancelPool := context.WithCancel(t.Context())
	defer cancelPool()

	inHandler := make(chan struct{})
	allowReturn := make(chan struct{})

	runner := jobs.NewRunner(repo, jobs.RunnerOptions{
		PollInterval: 15 * time.Millisecond,
	})
	if err := runner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeCheckpoint,
		PoolSize:     1,
		Handler: jobs.HandlerFunc(func(runCtx context.Context, _ *jobs.Execution) error {
			close(inHandler)
			<-runCtx.Done()
			<-allowReturn
			return runCtx.Err()
		}),
	}); err != nil {
		t.Fatalf("RegisterKind: %v", err)
	}
	if _, err := runner.Recover(t.Context()); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	now := time.Now().UTC()
	j1 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now, RunAfter: now}
	j2 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now.Add(time.Millisecond), RunAfter: now}
	if _, err := repo.Enqueue(t.Context(), j1); err != nil {
		t.Fatalf("Enqueue j1: %v", err)
	}
	if _, err := repo.Enqueue(t.Context(), j2); err != nil {
		t.Fatalf("Enqueue j2: %v", err)
	}

	if err := runner.Start(poolCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-inHandler:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for j1 to enter handler")
	}

	// Cancel lifecycle context; Wait must block until active handler finishes.
	cancelPool()
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		runner.Wait()
	}()

	select {
	case <-waitDone:
		t.Fatal("runner.Wait returned before active handler exited")
	case <-time.After(80 * time.Millisecond):
	}

	close(allowReturn)
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("runner.Wait did not return after active handler exited")
	}

	// j1 must remain RUNNING (for startup recovery), and j2 must remain QUEUED (claims stopped).
	got1, err := repo.GetJobByID(t.Context(), j1.ID)
	if err != nil || got1.State != domain.JobStateRunning || got1.Attempts != 1 {
		t.Fatalf("j1 = (%+v, %v), want RUNNING attempts=1", got1, err)
	}
	got2, err := repo.GetJobByID(t.Context(), j2.ID)
	if err != nil || got2.State != domain.JobStateQueued || got2.Attempts != 0 {
		t.Fatalf("j2 = (%+v, %v), want QUEUED attempts=0 (claims must stop on cancellation)", got2, err)
	}
}

func TestRunner_RegisterModuleHandlersRespectsEnablement(t *testing.T) {
	t.Parallel()
	_, repo := setupRunnerTestDB(t)

	reg := module.NewRegistry()
	var enabledCalled, disabledCalled atomic.Bool

	enabledMod := &registrarModule{
		SimpleModule: &module.SimpleModule{
			ModuleName: "enabled-mod",
			Jobs: []module.JobKind{
				module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint},
			},
		},
		onRegister: func(r *jobs.Runner) error {
			enabledCalled.Store(true)
			return r.RegisterKind(jobs.KindConfig{
				Kind:         domain.JobKindScan,
				ResumePolicy: domain.JobResumeCheckpoint,
				Handler:      jobs.HandlerFunc(func(context.Context, *jobs.Execution) error { return nil }),
			})
		},
	}
	disabledMod := &registrarModule{
		SimpleModule: &module.SimpleModule{
			ModuleName: "disabled-mod",
			Jobs: []module.JobKind{
				module.JobKindSpec{Kind: domain.JobKindRepack, Policy: domain.JobResumeRestart},
			},
		},
		onRegister: func(_ *jobs.Runner) error {
			disabledCalled.Store(true)
			return nil
		},
	}
	if err := reg.Register(enabledMod); err != nil {
		t.Fatalf("Register enabledMod: %v", err)
	}
	if err := reg.Register(disabledMod); err != nil {
		t.Fatalf("Register disabledMod: %v", err)
	}
	reg.SetEnabled("disabled-mod", false)

	runner := jobs.NewRunner(repo, jobs.RunnerOptions{})
	if err := runner.RegisterModuleKinds(reg); err != nil {
		t.Fatalf("RegisterModuleKinds: %v", err)
	}
	if err := runner.RegisterModuleHandlers(reg); err != nil {
		t.Fatalf("RegisterModuleHandlers: %v", err)
	}
	if !enabledCalled.Load() {
		t.Fatal("expected RegisterJobs to be called on enabled module")
	}
	if disabledCalled.Load() {
		t.Fatal("RegisterJobs must not be called on disabled module")
	}
}

func TestRunner_DirectClaimAndManagedPoolsMutuallyExclusive(t *testing.T) {
	t.Parallel()
	_, repo := setupRunnerTestDB(t)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	var active atomic.Int32
	var maxActive atomic.Int32
	entered := make(chan struct{}, 4)
	release := make(chan struct{})

	runner := jobs.NewRunner(repo, jobs.RunnerOptions{
		PollInterval: 15 * time.Millisecond,
	})
	if err := runner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeCheckpoint,
		PoolSize:     1,
		Handler: jobs.HandlerFunc(func(runCtx context.Context, _ *jobs.Execution) error {
			cur := active.Add(1)
			defer active.Add(-1)
			for {
				prev := maxActive.Load()
				if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
					break
				}
			}
			entered <- struct{}{}
			select {
			case <-release:
				return nil
			case <-runCtx.Done():
				return runCtx.Err()
			}
		}),
	}); err != nil {
		t.Fatalf("RegisterKind: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	now := time.Now().UTC()
	j1 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now, RunAfter: now}
	j2 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now.Add(time.Millisecond), RunAfter: now}
	j3 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now.Add(2 * time.Millisecond), RunAfter: now}
	for _, j := range []*domain.Job{j1, j2, j3} {
		if _, err := repo.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// 1. Overlapping transition: ClaimAndRunNext is in flight -> Start rejects with ErrDirectClaimInProgress.
	directDone := make(chan error, 1)
	go func() {
		_, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan)
		directDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for direct claim handler")
	}
	if err := runner.Start(ctx); !errors.Is(err, jobs.ErrDirectClaimInProgress) {
		t.Fatalf("Start during active direct claim = %v, want ErrDirectClaimInProgress", err)
	}

	// Release direct claim; wait for it to finish, then Start succeeds for pool size N=1.
	release <- struct{}{}
	if err := <-directDone; err != nil {
		t.Fatalf("ClaimAndRunNext failed: %v", err)
	}
	waitForJobSucceeded(ctx, t, repo, j1.ID)

	if err := runner.Start(ctx); err != nil {
		t.Fatalf("Start after direct claim finished: %v", err)
	}
	defer runner.Stop()

	// 2. Pool saturated at N=1 (running j2, j3 queued): ClaimAndRunNext must fail cleanly without exceeding N.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pool worker to claim j2")
	}

	claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan)
	if claimed || !errors.Is(err, jobs.ErrPoolsAlreadyRunning) {
		t.Fatalf("ClaimAndRunNext while pools running = (%v, %v), want (false, ErrPoolsAlreadyRunning)", claimed, err)
	}

	close(release)
	waitForJobSucceeded(ctx, t, repo, j2.ID)
	waitForJobSucceeded(ctx, t, repo, j3.ID)
	if maxActive.Load() != 1 {
		t.Fatalf("maxActive = %d, want 1", maxActive.Load())
	}
}

type flakyOutcomeRepo struct {
	storage.JobRepository
	mu              sync.Mutex
	failTransient   int
	ambiguousCommit bool
	failPermanent   bool
	markCalls       int
}

func (f *flakyOutcomeRepo) MarkSucceeded(ctx context.Context, id string, attempt int, now time.Time) (*domain.Job, error) {
	f.mu.Lock()
	f.markCalls++
	if f.failPermanent {
		f.mu.Unlock()
		return nil, errors.New("persistent outcome write failure")
	}
	if f.failTransient > 0 {
		f.failTransient--
		f.mu.Unlock()
		return nil, errors.New("transient outcome write failure")
	}
	ambiguous := f.ambiguousCommit
	if ambiguous {
		f.ambiguousCommit = false
	}
	f.mu.Unlock()

	res, err := f.JobRepository.MarkSucceeded(ctx, id, attempt, now)
	if err != nil {
		return nil, err
	}
	if ambiguous {
		// Simulate dropped response after PostgreSQL already committed SUCCEEDED.
		return nil, errors.New("connection reset after commit")
	}
	return res, nil
}

func TestRunner_OutcomePersistenceTransientAmbiguousAndFatal(t *testing.T) {
	t.Parallel()
	_, baseRepo := setupRunnerTestDB(t)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	t.Run("transient write failure retries and runs handler once", func(t *testing.T) {
		repo := &flakyOutcomeRepo{JobRepository: baseRepo, failTransient: 2}
		var runs atomic.Int32
		runner := jobs.NewRunner(repo, jobs.RunnerOptions{PollInterval: 15 * time.Millisecond})
		if err := runner.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			PoolSize:     1,
			Handler: jobs.HandlerFunc(func(_ context.Context, _ *jobs.Execution) error {
				runs.Add(1)
				return nil
			}),
		}); err != nil {
			t.Fatalf("RegisterKind: %v", err)
		}
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		now := time.Now().UTC()
		j1 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now, RunAfter: now}
		j2 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now.Add(time.Millisecond), RunAfter: now}
		for _, j := range []*domain.Job{j1, j2} {
			if _, err := baseRepo.Enqueue(ctx, j); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}

		if err := runner.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer runner.Stop()

		waitForJobSucceeded(ctx, t, baseRepo, j1.ID)
		waitForJobSucceeded(ctx, t, baseRepo, j2.ID)
		if runs.Load() != 2 {
			t.Fatalf("handler runs = %d, want 2 (once per job)", runs.Load())
		}
		if runner.Err() != nil {
			t.Fatalf("unexpected runner.Err() = %v", runner.Err())
		}
	})

	t.Run("ambiguous committed write resolves via readback and runs handler once", func(t *testing.T) {
		repo := &flakyOutcomeRepo{JobRepository: baseRepo, ambiguousCommit: true}
		var runs atomic.Int32
		runner := jobs.NewRunner(repo, jobs.RunnerOptions{PollInterval: 15 * time.Millisecond})
		if err := runner.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindRepack,
			ResumePolicy: domain.JobResumeRestart,
			PoolSize:     1,
			Handler: jobs.HandlerFunc(func(_ context.Context, _ *jobs.Execution) error {
				runs.Add(1)
				return nil
			}),
		}); err != nil {
			t.Fatalf("RegisterKind: %v", err)
		}
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		now := time.Now().UTC()
		j1 := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: now, RunAfter: now}
		j2 := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: now.Add(time.Millisecond), RunAfter: now}
		for _, j := range []*domain.Job{j1, j2} {
			if _, err := baseRepo.Enqueue(ctx, j); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}

		if err := runner.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer runner.Stop()

		waitForJobSucceeded(ctx, t, baseRepo, j1.ID)
		waitForJobSucceeded(ctx, t, baseRepo, j2.ID)
		if runs.Load() != 2 {
			t.Fatalf("handler runs = %d, want 2 (once per job)", runs.Load())
		}
		if runner.Err() != nil {
			t.Fatalf("unexpected runner.Err() = %v", runner.Err())
		}
	})

	t.Run("unrecoverable outcome write stops pools and notifies OnFatal without claiming next job", func(t *testing.T) {
		repo := &flakyOutcomeRepo{JobRepository: baseRepo, failPermanent: true}
		var runs atomic.Int32
		var fatalCalls atomic.Int32
		fatalCh := make(chan error, 2)

		runner := jobs.NewRunner(repo, jobs.RunnerOptions{
			PollInterval: 15 * time.Millisecond,
			OnFatal: func(err error) {
				fatalCalls.Add(1)
				fatalCh <- err
			},
		})
		if err := runner.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScrape,
			ResumePolicy: domain.JobResumeRestart,
			PoolSize:     1,
			Handler: jobs.HandlerFunc(func(_ context.Context, _ *jobs.Execution) error {
				runs.Add(1)
				return nil
			}),
		}); err != nil {
			t.Fatalf("RegisterKind: %v", err)
		}
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}

		now := time.Now().UTC()
		j1 := &domain.Job{Kind: domain.JobKindScrape, MaxAttempts: 3, CreatedAt: now, RunAfter: now}
		j2 := &domain.Job{Kind: domain.JobKindScrape, MaxAttempts: 3, CreatedAt: now.Add(time.Millisecond), RunAfter: now}
		if _, err := baseRepo.Enqueue(ctx, j1); err != nil {
			t.Fatalf("Enqueue j1: %v", err)
		}
		if _, err := baseRepo.Enqueue(ctx, j2); err != nil {
			t.Fatalf("Enqueue j2: %v", err)
		}

		if err := runner.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		select {
		case fatalErr := <-fatalCh:
			if !errors.Is(fatalErr, jobs.ErrOutcomePersistenceFailed) {
				t.Fatalf("fatalErr = %v, want ErrOutcomePersistenceFailed", fatalErr)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for OnFatal callback")
		}

		runner.Wait()
		if !errors.Is(runner.Err(), jobs.ErrOutcomePersistenceFailed) {
			t.Fatalf("runner.Err() = %v, want ErrOutcomePersistenceFailed", runner.Err())
		}
		if fatalCalls.Load() != 1 {
			t.Fatalf("fatalCalls = %d, want 1", fatalCalls.Load())
		}
		if runs.Load() != 1 {
			t.Fatalf("handler runs = %d, want 1", runs.Load())
		}

		got1, err := baseRepo.GetJobByID(ctx, j1.ID)
		if err != nil || got1.State != domain.JobStateRunning || got1.Attempts != 1 {
			t.Fatalf("j1 = (%+v, %v), want RUNNING attempts=1 for recovery", got1, err)
		}
		got2, err := baseRepo.GetJobByID(ctx, j2.ID)
		if err != nil || got2.State != domain.JobStateQueued || got2.Attempts != 0 {
			t.Fatalf("j2 = (%+v, %v), want QUEUED attempts=0 (must stop claiming on fatal error)", got2, err)
		}
	})
}

func TestRunner_StopCancelsAndJoinsSameGeneration(t *testing.T) {
	t.Parallel()
	_, repo := setupRunnerTestDB(t)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	var genPhase atomic.Int32
	gen1Entered := make(chan struct{})
	gen1Release := make(chan struct{})
	gen2Entered := make(chan struct{})
	gen2Release := make(chan struct{})

	runner := jobs.NewRunner(repo, jobs.RunnerOptions{PollInterval: 15 * time.Millisecond})
	if err := runner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeCheckpoint,
		PoolSize:     1,
		Handler: jobs.HandlerFunc(func(runCtx context.Context, _ *jobs.Execution) error {
			if genPhase.Load() == 1 {
				close(gen1Entered)
				<-runCtx.Done()
				<-gen1Release
				return runCtx.Err()
			}
			close(gen2Entered)
			select {
			case <-gen2Release:
				return runCtx.Err()
			case <-runCtx.Done():
				return runCtx.Err()
			}
		}),
	}); err != nil {
		t.Fatalf("RegisterKind: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	now := time.Now().UTC()
	j1 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now, RunAfter: now}
	if _, err := repo.Enqueue(ctx, j1); err != nil {
		t.Fatalf("Enqueue j1: %v", err)
	}
	j2 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now.Add(time.Second), RunAfter: now}

	// Start Gen 1 and block inside handler.
	genPhase.Store(1)
	if err := runner.Start(ctx); err != nil {
		t.Fatalf("Start gen1: %v", err)
	}
	select {
	case <-gen1Entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for gen1 handler")
	}

	// Overlap Wait and Stop on Gen 1, then immediately start Gen 2 once Wait returns
	// while Gen 1's Stop may still be unwinding.
	waitDone := make(chan struct{})
	stopDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		runner.Wait()
	}()
	time.Sleep(20 * time.Millisecond)
	go func() {
		defer close(stopDone)
		runner.Stop()
	}()
	time.Sleep(20 * time.Millisecond)
	close(gen1Release)

	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait on gen1 timed out")
	}

	genPhase.Store(2)
	if _, err := repo.Enqueue(ctx, j2); err != nil {
		t.Fatalf("Enqueue j2: %v", err)
	}
	if err := runner.Start(ctx); err != nil {
		t.Fatalf("Start gen2: %v", err)
	}
	select {
	case <-gen2Entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for gen2 handler")
	}

	// Gen 1's Stop must complete without blocking on Gen 2.
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop on gen1 blocked on gen2")
	}

	// Gen 2 must remain uncancelled and complete j2 cleanly after release.
	close(gen2Release)
	waitForJobSucceeded(ctx, t, repo, j2.ID)
	runner.Stop()
	for range 10 {
		if err := runner.Start(ctx); err != nil {
			t.Fatalf("cycle Start: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); runner.Wait() }()
		go func() { defer wg.Done(); runner.Stop() }()
		wg.Wait()
	}
}

type flakyClaimRepo struct {
	storage.JobRepository
	claimCalls atomic.Int32
	dropAck    atomic.Bool
}

func (f *flakyClaimRepo) ClaimNext(ctx context.Context, kind domain.JobKind, now time.Time) (*domain.Job, error) {
	f.claimCalls.Add(1)
	job, err := f.JobRepository.ClaimNext(ctx, kind, now)
	if err != nil {
		return nil, err
	}
	if f.dropAck.Swap(false) {
		// Simulate a committed ClaimNext transaction whose acknowledgement was lost.
		return nil, errors.New("connection reset after claim commit")
	}
	return job, nil
}

func TestRunner_AmbiguousClaimCommitEntersFailStopBeforeClaimingMore(t *testing.T) {
	t.Parallel()
	_, baseRepo := setupRunnerTestDB(t)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// 1. Verify known no-claim (ErrNotFound on empty queue) keeps pools healthy without fail-stop,
	// and a subsequent job completes normally.
	t.Run("empty queue ErrNotFound continues polling without fail-stop", func(t *testing.T) {
		var fatalCalls atomic.Int32
		runner := jobs.NewRunner(baseRepo, jobs.RunnerOptions{
			PollInterval: 15 * time.Millisecond,
			OnFatal:      func(error) { fatalCalls.Add(1) },
		})
		if err := runner.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindRepack,
			ResumePolicy: domain.JobResumeRestart,
			PoolSize:     1,
			Handler:      jobs.HandlerFunc(func(context.Context, *jobs.Execution) error { return nil }),
		}); err != nil {
			t.Fatalf("RegisterKind: %v", err)
		}
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if err := runner.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer runner.Stop()

		time.Sleep(50 * time.Millisecond)
		if fatalCalls.Load() != 0 || runner.Err() != nil {
			t.Fatalf("unexpected fatal on empty queue: calls=%d err=%v", fatalCalls.Load(), runner.Err())
		}

		now := time.Now().UTC()
		j := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: now, RunAfter: now}
		if _, err := baseRepo.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		waitForJobSucceeded(ctx, t, baseRepo, j.ID)
	})

	// 2. Simulate a committed ClaimNext with a lost acknowledgement: worker must enter fail-stop
	// immediately without claiming more jobs, leaving the committed RUNNING job for startup recovery.
	t.Run("committed claim with lost ack enters fail-stop and recovers cleanly on restart", func(t *testing.T) {
		repo := &flakyClaimRepo{JobRepository: baseRepo}
		repo.dropAck.Store(true)

		var handlerRuns atomic.Int32
		var fatalCalls atomic.Int32
		fatalCh := make(chan error, 2)

		runner := jobs.NewRunner(repo, jobs.RunnerOptions{
			PollInterval: 15 * time.Millisecond,
			OnFatal: func(err error) {
				fatalCalls.Add(1)
				fatalCh <- err
			},
		})
		if err := runner.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			PoolSize:     1,
			Handler: jobs.HandlerFunc(func(context.Context, *jobs.Execution) error {
				handlerRuns.Add(1)
				return nil
			}),
		}); err != nil {
			t.Fatalf("RegisterKind: %v", err)
		}
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}

		now := time.Now().UTC()
		dedupe := "SCAN:ambiguous-claim-key"
		j1 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, DedupeKey: &dedupe, CreatedAt: now, RunAfter: now}
		j2 := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: now.Add(time.Millisecond), RunAfter: now}
		if _, err := baseRepo.Enqueue(ctx, j1); err != nil {
			t.Fatalf("Enqueue j1: %v", err)
		}
		if _, err := baseRepo.Enqueue(ctx, j2); err != nil {
			t.Fatalf("Enqueue j2: %v", err)
		}

		if err := runner.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		select {
		case fatalErr := <-fatalCh:
			if !errors.Is(fatalErr, jobs.ErrClaimFailed) {
				t.Fatalf("fatalErr = %v, want ErrClaimFailed", fatalErr)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for OnFatal on ambiguous claim")
		}

		runner.Wait()
		if !errors.Is(runner.Err(), jobs.ErrClaimFailed) {
			t.Fatalf("runner.Err() = %v, want ErrClaimFailed", runner.Err())
		}
		if fatalCalls.Load() != 1 {
			t.Fatalf("fatalCalls = %d, want 1", fatalCalls.Load())
		}
		if repo.claimCalls.Load() != 1 {
			t.Fatalf("claimCalls = %d, want 1 (must fail-stop before claiming more)", repo.claimCalls.Load())
		}
		if handlerRuns.Load() != 0 {
			t.Fatalf("handlerRuns = %d, want 0", handlerRuns.Load())
		}

		got1, err := baseRepo.GetJobByID(ctx, j1.ID)
		if err != nil || got1.State != domain.JobStateRunning || got1.Attempts != 1 {
			t.Fatalf("j1 = (%+v, %v), want RUNNING attempts=1", got1, err)
		}
		got2, err := baseRepo.GetJobByID(ctx, j2.ID)
		if err != nil || got2.State != domain.JobStateQueued || got2.Attempts != 0 {
			t.Fatalf("j2 = (%+v, %v), want QUEUED attempts=0", got2, err)
		}

		// Simulate daemon restart: startup recovery reconciles the stranded RUNNING row and runs both jobs.
		restarted := jobs.NewRunner(baseRepo, jobs.RunnerOptions{PollInterval: 15 * time.Millisecond})
		if err := restarted.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			PoolSize:     1,
			Handler:      jobs.HandlerFunc(func(context.Context, *jobs.Execution) error { return nil }),
		}); err != nil {
			t.Fatalf("restarted RegisterKind: %v", err)
		}
		report, err := restarted.Recover(ctx)
		if err != nil {
			t.Fatalf("restarted Recover: %v", err)
		}
		if report.TotalRunning != 1 || report.RequeuedCheckpoint != 1 {
			t.Fatalf("unexpected recovery report: %+v", report)
		}
		if err := restarted.Start(ctx); err != nil {
			t.Fatalf("restarted Start: %v", err)
		}
		defer restarted.Stop()

		waitForJobSucceeded(ctx, t, baseRepo, j1.ID)
		waitForJobSucceeded(ctx, t, baseRepo, j2.ID)
		if created, err := baseRepo.Enqueue(ctx, &domain.Job{Kind: domain.JobKindScan, DedupeKey: &dedupe, CreatedAt: now}); err != nil || !created {
			t.Fatalf("dedupe key %q not released after recovery completion: created=%v err=%v", dedupe, created, err)
		}
	})
}
