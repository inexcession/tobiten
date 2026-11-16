package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func setupJobTestDB(t *testing.T) (*testdb.TestDB, *postgres.JobRepository, *postgres.UserRepository) {
	t.Helper()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("setupJobTestDB: applying migrations: %v", err)
	}

	return db, postgres.NewJobRepository(db.Pool), postgres.NewUserRepository(db.Pool)
}

func TestJobRepository_EnqueueGetListAndNilDedupe(t *testing.T) {
	t.Parallel()
	_, repo, userRepo := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	creator := &domain.User{
		ID:       domain.MustNewID(),
		Username: "job-creator",
	}
	if err := userRepo.CreateUser(ctx, creator); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	j1 := &domain.Job{
		Kind:      domain.JobKindScan,
		Payload:   json.RawMessage(`{"library_id":"lib-1"}`),
		CreatedBy: &creator.ID,
	}
	created, err := repo.Enqueue(ctx, j1)
	if err != nil {
		t.Fatalf("Enqueue j1: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for first job")
	}
	if j1.ID == "" || j1.State != domain.JobStateQueued || j1.MaxAttempts != domain.DefaultJobMaxAttempts {
		t.Fatalf("unexpected enqueued job defaults: %+v", j1)
	}

	// Nil dedupe_key allows multiple concurrent jobs of the same kind.
	j2 := &domain.Job{
		Kind:    domain.JobKindScan,
		Payload: json.RawMessage(`{"library_id":"lib-2"}`),
	}
	created2, err := repo.Enqueue(ctx, j2)
	if err != nil {
		t.Fatalf("Enqueue j2: %v", err)
	}
	if !created2 || j2.ID == j1.ID {
		t.Fatalf("expected second nil-dedupe job with distinct ID, created=%v id1=%s id2=%s", created2, j1.ID, j2.ID)
	}

	j3 := &domain.Job{
		Kind: domain.JobKindIntake,
	}
	if _, err := repo.Enqueue(ctx, j3); err != nil {
		t.Fatalf("Enqueue j3: %v", err)
	}

	got, err := repo.GetJobByID(ctx, j1.ID)
	if err != nil {
		t.Fatalf("GetJobByID: %v", err)
	}
	if got.ID != j1.ID || got.CreatedBy == nil || *got.CreatedBy != creator.ID || string(got.Payload) != `{"library_id": "lib-1"}` {
		// Postgres JSONB formats keys with a space after ':'.
		var payloadMap map[string]string
		if jsonErr := json.Unmarshal(got.Payload, &payloadMap); jsonErr != nil || payloadMap["library_id"] != "lib-1" {
			t.Fatalf("unexpected GetJobByID result: %+v (payload=%s)", got, got.Payload)
		}
	}

	if _, err := repo.GetJobByID(ctx, domain.MustNewID()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unknown job, got %v", err)
	}

	scanKind := domain.JobKindScan
	scanJobs, err := repo.ListJobs(ctx, storage.JobFilter{Kind: &scanKind})
	if err != nil {
		t.Fatalf("ListJobs kind=SCAN: %v", err)
	}
	if len(scanJobs) != 2 {
		t.Fatalf("expected 2 SCAN jobs, got %d", len(scanJobs))
	}

	userJobs, err := repo.ListJobs(ctx, storage.JobFilter{CreatedBy: &creator.ID, Limit: 1})
	if err != nil {
		t.Fatalf("ListJobs created_by: %v", err)
	}
	if len(userJobs) != 1 || userJobs[0].ID != j1.ID {
		t.Fatalf("expected [j1], got %+v", userJobs)
	}

	queuedState := domain.JobStateQueued
	pagedQueued, err := repo.ListJobs(ctx, storage.JobFilter{State: &queuedState, Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("ListJobs state+offset: %v", err)
	}
	if len(pagedQueued) != 2 {
		t.Fatalf("expected 2 paginated QUEUED jobs, got %d", len(pagedQueued))
	}

	if _, err := repo.ClaimNext(ctx, "", time.Now()); !errors.Is(err, domain.ErrEmptyJobKind) {
		t.Fatalf("expected ErrEmptyJobKind for empty kind claim, got %v", err)
	}
	if _, err := repo.IsCancelRequested(ctx, domain.MustNewID()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for IsCancelRequested on unknown job, got %v", err)
	}

	// Error cases.
	if _, err := repo.Enqueue(ctx, &domain.Job{Kind: domain.JobKindScan, Payload: json.RawMessage(`{bad`)}); !errors.Is(err, domain.ErrInvalidJobJSON) {
		t.Fatalf("expected ErrInvalidJobJSON, got %v", err)
	}
	if _, err := repo.Enqueue(ctx, &domain.Job{Kind: domain.JobKindScan, State: domain.JobStateRunning}); !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("expected ErrInvalidJobTransition for non-QUEUED enqueue, got %v", err)
	}
	missingUser := domain.MustNewID()
	if _, err := repo.Enqueue(ctx, &domain.Job{Kind: domain.JobKindScan, CreatedBy: &missingUser}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing created_by user, got %v", err)
	}
	dupID := &domain.Job{ID: j1.ID, Kind: domain.JobKindScan}
	if _, err := repo.Enqueue(ctx, dupID); !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists for duplicate explicit ID, got %v", err)
	}
	blankDedupe := "   "
	if _, err := repo.Enqueue(ctx, &domain.Job{Kind: domain.JobKindScan, DedupeKey: &blankDedupe}); !errors.Is(err, domain.ErrEmptyDedupeKey) {
		t.Fatalf("expected ErrEmptyDedupeKey for blank dedupe_key, got %v", err)
	}
}

func TestJobRepository_DedupeAndTerminalReuse(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dedupe := "SCAN:lib-42"
	t0 := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

	first := &domain.Job{
		Kind:      domain.JobKindScan,
		DedupeKey: &dedupe,
		CreatedAt: t0,
	}
	created, err := repo.Enqueue(ctx, first)
	if err != nil || !created {
		t.Fatalf("first Enqueue = (%v, %v), want (true, nil)", created, err)
	}

	// Duplicate trigger while QUEUED returns existing job ID.
	second := &domain.Job{
		Kind:      domain.JobKindScan,
		DedupeKey: &dedupe,
		CreatedAt: t0.Add(time.Second),
	}
	created, err = repo.Enqueue(ctx, second)
	if err != nil {
		t.Fatalf("second Enqueue while QUEUED: %v", err)
	}
	if created || second.ID != first.ID {
		t.Fatalf("duplicate QUEUED enqueue = (created=%v, id=%s), want (false, %s)", created, second.ID, first.ID)
	}

	// Claim to RUNNING; duplicate trigger while RUNNING still returns existing job ID.
	claimed, err := repo.ClaimNext(ctx, domain.JobKindScan, t0.Add(2*time.Second))
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if claimed.ID != first.ID || claimed.State != domain.JobStateRunning {
		t.Fatalf("unexpected claimed job: %+v", claimed)
	}

	third := &domain.Job{
		Kind:      domain.JobKindScan,
		DedupeKey: &dedupe,
		CreatedAt: t0.Add(3 * time.Second),
	}
	created, err = repo.Enqueue(ctx, third)
	if err != nil {
		t.Fatalf("third Enqueue while RUNNING: %v", err)
	}
	if created || third.ID != first.ID {
		t.Fatalf("duplicate RUNNING enqueue = (created=%v, id=%s), want (false, %s)", created, third.ID, first.ID)
	}

	// Transition to terminal SUCCEEDED; dedupe_key is released for reuse.
	if _, err := repo.MarkSucceeded(ctx, first.ID, t0.Add(4*time.Second)); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}

	fourth := &domain.Job{
		Kind:        domain.JobKindScan,
		DedupeKey:   &dedupe,
		MaxAttempts: 1,
		CreatedAt:   t0.Add(5 * time.Second),
	}
	created, err = repo.Enqueue(ctx, fourth)
	if err != nil {
		t.Fatalf("fourth Enqueue after SUCCEEDED: %v", err)
	}
	if !created || fourth.ID == first.ID {
		t.Fatalf("expected new job after SUCCEEDED, got (created=%v, id=%s vs %s)", created, fourth.ID, first.ID)
	}

	// Terminal FAILED also releases dedupe_key.
	if _, err := repo.ClaimNext(ctx, domain.JobKindScan, t0.Add(6*time.Second)); err != nil {
		t.Fatalf("ClaimNext fourth: %v", err)
	}
	failed, err := repo.FailOrRetry(ctx, fourth.ID, "boom", time.Time{}, t0.Add(7*time.Second))
	if err != nil {
		t.Fatalf("FailOrRetry fourth: %v", err)
	}
	if failed.State != domain.JobStateFailed {
		t.Fatalf("expected FAILED, got %s", failed.State)
	}

	fifth := &domain.Job{
		Kind:      domain.JobKindScan,
		DedupeKey: &dedupe,
		CreatedAt: t0.Add(8 * time.Second),
	}
	created, err = repo.Enqueue(ctx, fifth)
	if err != nil || !created || fifth.ID == fourth.ID {
		t.Fatalf("expected new job after FAILED, got (created=%v, err=%v, id=%s)", created, err, fifth.ID)
	}

	// Terminal CANCELLED also releases dedupe_key.
	if _, err := repo.RequestCancel(ctx, fifth.ID, t0.Add(9*time.Second)); err != nil {
		t.Fatalf("RequestCancel fifth: %v", err)
	}
	sixth := &domain.Job{
		Kind:      domain.JobKindScan,
		DedupeKey: &dedupe,
		CreatedAt: t0.Add(10 * time.Second),
	}
	created, err = repo.Enqueue(ctx, sixth)
	if err != nil || !created || sixth.ID == fifth.ID {
		t.Fatalf("expected new job after CANCELLED, got (created=%v, err=%v, id=%s)", created, err, sixth.ID)
	}
}

func TestJobRepository_ConcurrentDuplicateEnqueue(t *testing.T) {
	t.Parallel()
	db, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const workers = 12
	dedupe := "SCAN:concurrent-lib"

	type result struct {
		id      string
		created bool
		err     error
	}

	start := make(chan struct{})
	results := make(chan result, workers)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			job := &domain.Job{
				Kind:      domain.JobKindScan,
				DedupeKey: &dedupe,
			}
			created, err := repo.Enqueue(ctx, job)
			results <- result{id: job.ID, created: created, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var createdCount int
	var canonicalID string
	for res := range results {
		if res.err != nil {
			t.Fatalf("concurrent Enqueue error: %v", res.err)
		}
		if res.id == "" {
			t.Fatal("expected non-empty job ID from concurrent Enqueue")
		}
		if canonicalID == "" {
			canonicalID = res.id
		} else if res.id != canonicalID {
			t.Fatalf("concurrent Enqueue returned mismatched IDs: %s vs %s", res.id, canonicalID)
		}
		if res.created {
			createdCount++
		}
	}

	if createdCount != 1 {
		t.Fatalf("expected exactly 1 Enqueue call to report created=true, got %d", createdCount)
	}

	var rowCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE dedupe_key = $1`, dedupe).Scan(&rowCount); err != nil {
		t.Fatalf("counting jobs: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected 1 persisted job row, got %d", rowCount)
	}
}

func TestJobRepository_ConcurrentPerKindClaimNoDoubleClaim(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

	const scanCount = 24
	const intakeCount = 6

	expectedScanIDs := make(map[string]bool, scanCount)
	for i := range scanCount {
		j := &domain.Job{
			Kind:      domain.JobKindScan,
			Payload:   json.RawMessage(fmt.Sprintf(`{"seq":%d}`, i)),
			CreatedAt: t0.Add(time.Duration(i) * time.Millisecond),
		}
		if _, err := repo.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue scan %d: %v", i, err)
		}
		expectedScanIDs[j.ID] = true
	}

	expectedIntakeIDs := make(map[string]bool, intakeCount)
	for i := range intakeCount {
		j := &domain.Job{
			Kind:      domain.JobKindIntake,
			Payload:   json.RawMessage(fmt.Sprintf(`{"seq":%d}`, i)),
			CreatedAt: t0.Add(time.Duration(i) * time.Millisecond),
		}
		if _, err := repo.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue intake %d: %v", i, err)
		}
		expectedIntakeIDs[j.ID] = true
	}

	claimAll := func(kind domain.JobKind, workers int) map[string]int {
		start := make(chan struct{})
		var mu sync.Mutex
		claimed := make(map[string]int)
		var wg sync.WaitGroup
		errCh := make(chan error, workers)

		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for {
					job, err := repo.ClaimNext(ctx, kind, t0.Add(time.Second))
					if errors.Is(err, storage.ErrNotFound) {
						return
					}
					if err != nil {
						errCh <- err
						return
					}
					if job.Kind != kind {
						errCh <- fmt.Errorf("claimed wrong kind %s for worker %s", job.Kind, kind)
						return
					}
					if job.State != domain.JobStateRunning || job.Attempts != 1 {
						errCh <- fmt.Errorf("unexpected claimed state/attempts: %+v", job)
						return
					}
					mu.Lock()
					claimed[job.ID]++
					mu.Unlock()
				}
			}()
		}

		close(start)
		wg.Wait()
		close(errCh)

		for err := range errCh {
			t.Fatalf("worker error for %s: %v", kind, err)
		}
		return claimed
	}

	var scanClaimed, intakeClaimed map[string]int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		scanClaimed = claimAll(domain.JobKindScan, 8)
	}()
	go func() {
		defer wg.Done()
		intakeClaimed = claimAll(domain.JobKindIntake, 3)
	}()
	wg.Wait()

	if len(scanClaimed) != scanCount {
		t.Fatalf("claimed %d SCAN jobs, want %d", len(scanClaimed), scanCount)
	}
	for id := range expectedScanIDs {
		if scanClaimed[id] != 1 {
			t.Fatalf("SCAN job %s claimed %d times, want 1", id, scanClaimed[id])
		}
	}

	if len(intakeClaimed) != intakeCount {
		t.Fatalf("claimed %d INTAKE jobs, want %d", len(intakeClaimed), intakeCount)
	}
	for id := range expectedIntakeIDs {
		if intakeClaimed[id] != 1 {
			t.Fatalf("INTAKE job %s claimed %d times, want 1", id, intakeClaimed[id])
		}
	}
}

func TestJobRepository_AttemptsBackoffAndMaxAttempts(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

	job := &domain.Job{
		Kind:        domain.JobKindRepack,
		MaxAttempts: 3,
		CreatedAt:   t0,
		RunAfter:    t0.Add(10 * time.Second), // future run_after should delay initial claim
	}
	if _, err := repo.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Not eligible before initial run_after.
	if _, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0.Add(9*time.Second)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound before initial run_after, got %v", err)
	}

	// Attempt 1 claim at t0 + 10s.
	tClaim1 := t0.Add(10 * time.Second)
	claimed, err := repo.ClaimNext(ctx, domain.JobKindRepack, tClaim1)
	if err != nil {
		t.Fatalf("ClaimNext attempt 1: %v", err)
	}
	if claimed.Attempts != 1 || claimed.State != domain.JobStateRunning || claimed.StartedAt == nil {
		t.Fatalf("unexpected attempt 1 state: %+v", claimed)
	}
	initialStartedAt := *claimed.StartedAt

	// Fail attempt 1 with explicit retryAt (+30s).
	tFail1 := tClaim1.Add(time.Second)
	explicitRetryAt := tFail1.Add(30 * time.Second)
	retried1, err := repo.FailOrRetry(ctx, job.ID, "io timeout", explicitRetryAt, tFail1)
	if err != nil {
		t.Fatalf("FailOrRetry attempt 1: %v", err)
	}
	if retried1.State != domain.JobStateQueued || retried1.Attempts != 1 || retried1.FinishedAt != nil {
		t.Fatalf("unexpected retried1 state: %+v", retried1)
	}
	if retried1.LastError == nil || *retried1.LastError != "io timeout" {
		t.Fatalf("expected last_error='io timeout', got %v", retried1.LastError)
	}
	if !retried1.RunAfter.Equal(explicitRetryAt) {
		t.Fatalf("run_after = %v, want %v", retried1.RunAfter, explicitRetryAt)
	}

	// Not eligible before explicitRetryAt.
	if _, err := repo.ClaimNext(ctx, domain.JobKindRepack, explicitRetryAt.Add(-time.Millisecond)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound before backoff expires, got %v", err)
	}

	// Attempt 2 claim at explicitRetryAt.
	claimed2, err := repo.ClaimNext(ctx, domain.JobKindRepack, explicitRetryAt)
	if err != nil {
		t.Fatalf("ClaimNext attempt 2: %v", err)
	}
	if claimed2.Attempts != 2 || claimed2.State != domain.JobStateRunning || claimed2.StartedAt == nil || !claimed2.StartedAt.Equal(initialStartedAt) {
		t.Fatalf("unexpected attempt 2 state: %+v", claimed2)
	}

	// Fail attempt 2 with zero retryAt -> uses default exponential backoff for attempt 2 (10s).
	tFail2 := explicitRetryAt.Add(time.Second)
	expectedDefaultRetryAt := tFail2.Add(domain.JobRetryBackoff(2))
	retried2, err := repo.FailOrRetry(ctx, job.ID, "disk busy", time.Time{}, tFail2)
	if err != nil {
		t.Fatalf("FailOrRetry attempt 2: %v", err)
	}
	if retried2.State != domain.JobStateQueued || !retried2.RunAfter.Equal(expectedDefaultRetryAt) {
		t.Fatalf("unexpected retried2 state/run_after: %+v, want %v", retried2, expectedDefaultRetryAt)
	}

	// Not eligible before default backoff expires.
	if _, err := repo.ClaimNext(ctx, domain.JobKindRepack, expectedDefaultRetryAt.Add(-time.Second)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound before default backoff expires, got %v", err)
	}

	// Attempt 3 claim at expectedDefaultRetryAt.
	claimed3, err := repo.ClaimNext(ctx, domain.JobKindRepack, expectedDefaultRetryAt)
	if err != nil {
		t.Fatalf("ClaimNext attempt 3: %v", err)
	}
	if claimed3.Attempts != 3 || claimed3.State != domain.JobStateRunning {
		t.Fatalf("unexpected attempt 3 state: %+v", claimed3)
	}

	// Fail attempt 3 -> max_attempts reached -> moves to FAILED.
	tFail3 := expectedDefaultRetryAt.Add(time.Second)
	failed, err := repo.FailOrRetry(ctx, job.ID, "corrupt archive", time.Time{}, tFail3)
	if err != nil {
		t.Fatalf("FailOrRetry attempt 3: %v", err)
	}
	if failed.State != domain.JobStateFailed || failed.Attempts != 3 || failed.FinishedAt == nil || !failed.FinishedAt.Equal(tFail3) {
		t.Fatalf("expected FAILED at max_attempts, got %+v", failed)
	}
	if failed.LastError == nil || *failed.LastError != "corrupt archive" {
		t.Fatalf("expected last_error='corrupt archive', got %v", failed.LastError)
	}

	// No longer claimable.
	if _, err := repo.ClaimNext(ctx, domain.JobKindRepack, tFail3.Add(time.Hour)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after FAILED, got %v", err)
	}

	// Verify MarkSucceeded after a prior retry clears last_error.
	retrySuccessJob := &domain.Job{
		Kind:        domain.JobKindRepack,
		MaxAttempts: 2,
		CreatedAt:   t0,
	}
	if _, err := repo.Enqueue(ctx, retrySuccessJob); err != nil {
		t.Fatalf("Enqueue retrySuccessJob: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0); err != nil {
		t.Fatalf("ClaimNext 1: %v", err)
	}
	if _, err := repo.FailOrRetry(ctx, retrySuccessJob.ID, "transient", t0.Add(time.Second), t0); err != nil {
		t.Fatalf("FailOrRetry: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0.Add(time.Second)); err != nil {
		t.Fatalf("ClaimNext 2: %v", err)
	}
	succeeded, err := repo.MarkSucceeded(ctx, retrySuccessJob.ID, t0.Add(2*time.Second))
	if err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}
	if succeeded.State != domain.JobStateSucceeded || succeeded.LastError != nil || succeeded.FinishedAt == nil {
		t.Fatalf("expected SUCCEEDED with cleared last_error, got %+v", succeeded)
	}
}

func TestJobRepository_CancelRequestedAndStateMachine(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	dedupeQueued := "PURGE:item-1"

	// 1. RequestCancel on QUEUED job transitions immediately to CANCELLED and releases dedupe_key.
	queuedJob := &domain.Job{
		Kind:      domain.JobKindPurge,
		DedupeKey: &dedupeQueued,
		CreatedAt: t0,
	}
	if _, err := repo.Enqueue(ctx, queuedJob); err != nil {
		t.Fatalf("Enqueue queuedJob: %v", err)
	}
	cancelledQueued, err := repo.RequestCancel(ctx, queuedJob.ID, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("RequestCancel queuedJob: %v", err)
	}
	if cancelledQueued.State != domain.JobStateCancelled || !cancelledQueued.CancelRequested || cancelledQueued.FinishedAt == nil {
		t.Fatalf("expected immediate CANCELLED for queued job, got %+v", cancelledQueued)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindPurge, t0.Add(2*time.Second)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected cancelled queued job not to be claimable, got %v", err)
	}

	// 2. RequestCancel on RUNNING job sets cancel_requested=true while staying RUNNING until worker observes it.
	runningJob := &domain.Job{
		Kind:        domain.JobKindPurge,
		MaxAttempts: 3,
		CreatedAt:   t0.Add(3 * time.Second),
	}
	if _, err := repo.Enqueue(ctx, runningJob); err != nil {
		t.Fatalf("Enqueue runningJob: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindPurge, t0.Add(4*time.Second)); err != nil {
		t.Fatalf("ClaimNext runningJob: %v", err)
	}

	requested, err := repo.RequestCancel(ctx, runningJob.ID, t0.Add(5*time.Second))
	if err != nil {
		t.Fatalf("RequestCancel runningJob: %v", err)
	}
	if requested.State != domain.JobStateRunning || !requested.CancelRequested || requested.FinishedAt != nil {
		t.Fatalf("expected RUNNING with cancel_requested=true, got %+v", requested)
	}

	isCancelled, err := repo.IsCancelRequested(ctx, runningJob.ID)
	if err != nil || !isCancelled {
		t.Fatalf("IsCancelRequested = (%v, %v), want (true, nil)", isCancelled, err)
	}

	// Idempotent RequestCancel on RUNNING job succeeds.
	requestedAgain, err := repo.RequestCancel(ctx, runningJob.ID, t0.Add(6*time.Second))
	if err != nil || !requestedAgain.CancelRequested || requestedAgain.State != domain.JobStateRunning {
		t.Fatalf("idempotent RequestCancel = (%+v, %v)", requestedAgain, err)
	}

	// Worker observes cancellation and marks cancelled.
	cancelNote := "cancelled at safe point"
	markedCancelled, err := repo.MarkCancelled(ctx, runningJob.ID, &cancelNote, t0.Add(7*time.Second))
	if err != nil {
		t.Fatalf("MarkCancelled: %v", err)
	}
	if markedCancelled.State != domain.JobStateCancelled || markedCancelled.FinishedAt == nil {
		t.Fatalf("expected CANCELLED, got %+v", markedCancelled)
	}

	// 3. FailOrRetry on RUNNING job with cancel_requested=true must NOT retry even when attempts < max_attempts.
	cancelOnFailJob := &domain.Job{
		Kind:        domain.JobKindPurge,
		MaxAttempts: 3,
		CreatedAt:   t0.Add(8 * time.Second),
	}
	if _, err := repo.Enqueue(ctx, cancelOnFailJob); err != nil {
		t.Fatalf("Enqueue cancelOnFailJob: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindPurge, t0.Add(9*time.Second)); err != nil {
		t.Fatalf("ClaimNext cancelOnFailJob: %v", err)
	}
	if _, err := repo.RequestCancel(ctx, cancelOnFailJob.ID, t0.Add(10*time.Second)); err != nil {
		t.Fatalf("RequestCancel cancelOnFailJob: %v", err)
	}
	afterFail, err := repo.FailOrRetry(ctx, cancelOnFailJob.ID, "context canceled", time.Time{}, t0.Add(11*time.Second))
	if err != nil {
		t.Fatalf("FailOrRetry on cancel_requested job: %v", err)
	}
	if afterFail.State != domain.JobStateCancelled || afterFail.FinishedAt == nil {
		t.Fatalf("expected CANCELLED instead of retry, got %+v", afterFail)
	}

	// 4. Invalid transitions return domain.ErrInvalidJobTransition; MarkCancelled works on QUEUED too.
	queuedDirectCancel := &domain.Job{Kind: domain.JobKindPurge, CreatedAt: t0.Add(11 * time.Second)}
	if _, err := repo.Enqueue(ctx, queuedDirectCancel); err != nil {
		t.Fatalf("Enqueue queuedDirectCancel: %v", err)
	}
	if res, err := repo.MarkCancelled(ctx, queuedDirectCancel.ID, nil, t0.Add(11*time.Second)); err != nil || res.State != domain.JobStateCancelled {
		t.Fatalf("MarkCancelled on QUEUED = (%+v, %v), want CANCELLED", res, err)
	}

	queuedOnly := &domain.Job{Kind: domain.JobKindPurge, CreatedAt: t0.Add(12 * time.Second)}
	if _, err := repo.Enqueue(ctx, queuedOnly); err != nil {
		t.Fatalf("Enqueue queuedOnly: %v", err)
	}
	if _, err := repo.MarkSucceeded(ctx, queuedOnly.ID, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("MarkSucceeded on QUEUED = %v, want ErrInvalidJobTransition", err)
	}
	if _, err := repo.FailOrRetry(ctx, queuedOnly.ID, "err", time.Time{}, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("FailOrRetry on QUEUED = %v, want ErrInvalidJobTransition", err)
	}

	for _, terminalID := range []string{cancelledQueued.ID, markedCancelled.ID} {
		if _, err := repo.MarkSucceeded(ctx, terminalID, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
			t.Fatalf("MarkSucceeded on terminal %s = %v, want ErrInvalidJobTransition", terminalID, err)
		}
		if _, err := repo.FailOrRetry(ctx, terminalID, "err", time.Time{}, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
			t.Fatalf("FailOrRetry on terminal %s = %v, want ErrInvalidJobTransition", terminalID, err)
		}
		if _, err := repo.RequestCancel(ctx, terminalID, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
			t.Fatalf("RequestCancel on terminal %s = %v, want ErrInvalidJobTransition", terminalID, err)
		}
		if _, err := repo.MarkCancelled(ctx, terminalID, nil, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
			t.Fatalf("MarkCancelled on terminal %s = %v, want ErrInvalidJobTransition", terminalID, err)
		}
	}

	// 5. WithTx rollback does not persist enqueued job.
	rolledBackID := domain.MustNewID()
	sentinelErr := errors.New("rollback tx")
	err = repo.WithTx(ctx, func(txRepo storage.JobRepository) error {
		if _, err := txRepo.Enqueue(ctx, &domain.Job{ID: rolledBackID, Kind: domain.JobKindScan}); err != nil {
			return err
		}
		return sentinelErr
	})
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("WithTx err = %v, want %v", err, sentinelErr)
	}
	if _, err := repo.GetJobByID(ctx, rolledBackID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected rolled-back job not found, got %v", err)
	}
}
