package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func pgxTxRepeatableRead() pgx.TxOptions {
	return pgx.TxOptions{IsoLevel: pgx.RepeatableRead}
}

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
	for _, badMax := range []int{-1, math.MaxInt32 + 1, (1 << 32) + 1, math.MaxInt} {
		overflowDedupe := fmt.Sprintf("SCAN:overflow-%d", badMax)
		badJob := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: badMax, DedupeKey: &overflowDedupe}
		if _, err := repo.Enqueue(ctx, badJob); !errors.Is(err, domain.ErrInvalidJobAttempts) {
			t.Fatalf("Enqueue(MaxAttempts=%d) = %v, want ErrInvalidJobAttempts", badMax, err)
		}
		// Invalid request must not persist or occupy the dedupe key.
		validAtBoundary := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: math.MaxInt32, DedupeKey: &overflowDedupe}
		if createdBoundary, err := repo.Enqueue(ctx, validAtBoundary); err != nil || !createdBoundary {
			t.Fatalf("Enqueue(MaxAttempts=MaxInt32, dedupe=%s) = (%v, %v), want (true, nil)", overflowDedupe, createdBoundary, err)
		}
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
	if _, err := repo.MarkSucceeded(ctx, first.ID, claimed.Attempts, t0.Add(4*time.Second)); err != nil {
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
	claimedFourth, err := repo.ClaimNext(ctx, domain.JobKindScan, t0.Add(6*time.Second))
	if err != nil {
		t.Fatalf("ClaimNext fourth: %v", err)
	}
	failed, err := repo.FailOrRetry(ctx, fourth.ID, claimedFourth.Attempts, "boom", time.Time{}, t0.Add(7*time.Second))
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
	retried1, err := repo.FailOrRetry(ctx, job.ID, claimed.Attempts, "io timeout", explicitRetryAt, tFail1)
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
	retried2, err := repo.FailOrRetry(ctx, job.ID, claimed2.Attempts, "disk busy", time.Time{}, tFail2)
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
	failed, err := repo.FailOrRetry(ctx, job.ID, claimed3.Attempts, "corrupt archive", time.Time{}, tFail3)
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
	rs1, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0)
	if err != nil {
		t.Fatalf("ClaimNext 1: %v", err)
	}
	if _, err := repo.FailOrRetry(ctx, retrySuccessJob.ID, rs1.Attempts, "transient", t0.Add(time.Second), t0); err != nil {
		t.Fatalf("FailOrRetry: %v", err)
	}
	rs2, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("ClaimNext 2: %v", err)
	}
	succeeded, err := repo.MarkSucceeded(ctx, retrySuccessJob.ID, rs2.Attempts, t0.Add(2*time.Second))
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
	claimedRunning, err := repo.ClaimNext(ctx, domain.JobKindPurge, t0.Add(4*time.Second))
	if err != nil {
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
	markedCancelled, err := repo.MarkCancelled(ctx, runningJob.ID, claimedRunning.Attempts, &cancelNote, t0.Add(7*time.Second))
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
	claimedCancelOnFail, err := repo.ClaimNext(ctx, domain.JobKindPurge, t0.Add(9*time.Second))
	if err != nil {
		t.Fatalf("ClaimNext cancelOnFailJob: %v", err)
	}
	if _, err := repo.RequestCancel(ctx, cancelOnFailJob.ID, t0.Add(10*time.Second)); err != nil {
		t.Fatalf("RequestCancel cancelOnFailJob: %v", err)
	}
	afterFail, err := repo.FailOrRetry(ctx, cancelOnFailJob.ID, claimedCancelOnFail.Attempts, "context canceled", time.Time{}, t0.Add(11*time.Second))
	if err != nil {
		t.Fatalf("FailOrRetry on cancel_requested job: %v", err)
	}
	if afterFail.State != domain.JobStateCancelled || afterFail.FinishedAt == nil {
		t.Fatalf("expected CANCELLED instead of retry, got %+v", afterFail)
	}

	// 4. Invalid transitions return domain.ErrInvalidJobTransition.
	queuedOnly := &domain.Job{Kind: domain.JobKindPurge, CreatedAt: t0.Add(12 * time.Second)}
	if _, err := repo.Enqueue(ctx, queuedOnly); err != nil {
		t.Fatalf("Enqueue queuedOnly: %v", err)
	}
	if _, err := repo.MarkSucceeded(ctx, queuedOnly.ID, 1, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("MarkSucceeded on QUEUED = %v, want ErrInvalidJobTransition", err)
	}
	if _, err := repo.FailOrRetry(ctx, queuedOnly.ID, 1, "err", time.Time{}, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("FailOrRetry on QUEUED = %v, want ErrInvalidJobTransition", err)
	}
	if _, err := repo.MarkCancelled(ctx, queuedOnly.ID, 1, nil, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("MarkCancelled on QUEUED = %v, want ErrInvalidJobTransition", err)
	}

	for _, terminalID := range []string{cancelledQueued.ID, markedCancelled.ID} {
		if _, err := repo.MarkSucceeded(ctx, terminalID, 1, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
			t.Fatalf("MarkSucceeded on terminal %s = %v, want ErrInvalidJobTransition", terminalID, err)
		}
		if _, err := repo.FailOrRetry(ctx, terminalID, 1, "err", time.Time{}, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
			t.Fatalf("FailOrRetry on terminal %s = %v, want ErrInvalidJobTransition", terminalID, err)
		}
		if _, err := repo.RequestCancel(ctx, terminalID, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
			t.Fatalf("RequestCancel on terminal %s = %v, want ErrInvalidJobTransition", terminalID, err)
		}
		if _, err := repo.MarkCancelled(ctx, terminalID, 1, nil, t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
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

func TestJobRepository_StaleWorkerOutcomePreservesNewerClaim(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 11, 0, 0, 0, time.UTC)
	dedupe := "SCRAPE:stale-outcome"
	job := &domain.Job{
		Kind:        domain.JobKindScrape,
		MaxAttempts: 3,
		DedupeKey:   &dedupe,
		CreatedAt:   t0,
	}
	if _, err := repo.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Worker A claims attempt 1.
	claim1, err := repo.ClaimNext(ctx, domain.JobKindScrape, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("ClaimNext attempt 1: %v", err)
	}
	if claim1.Attempts != 1 {
		t.Fatalf("expected attempt 1, got %d", claim1.Attempts)
	}

	// Invalid attempt numbers on RUNNING job are rejected and preserve claim 1.
	for _, badAttempt := range []int{0, -1, math.MaxInt32 + 1} {
		if _, err := repo.MarkSucceeded(ctx, job.ID, badAttempt, t0.Add(2*time.Second)); !errors.Is(err, domain.ErrInvalidJobAttempts) {
			t.Fatalf("MarkSucceeded(attempt=%d) = %v, want ErrInvalidJobAttempts", badAttempt, err)
		}
	}
	if _, err := repo.MarkSucceeded(ctx, job.ID, 2, t0.Add(2*time.Second)); !errors.Is(err, domain.ErrStaleJobAttempt) {
		t.Fatalf("MarkSucceeded(ahead attempt=2) = %v, want ErrStaleJobAttempt", err)
	}

	// Attempt 1 fails and is retried; Worker B claims attempt 2.
	tRetry := t0.Add(5 * time.Second)
	if _, err := repo.FailOrRetry(ctx, job.ID, claim1.Attempts, "attempt-1-err", tRetry, t0.Add(3*time.Second)); err != nil {
		t.Fatalf("FailOrRetry attempt 1: %v", err)
	}

	// Stale Worker A reporting again while QUEUED is rejected.
	if _, err := repo.MarkSucceeded(ctx, job.ID, claim1.Attempts, t0.Add(4*time.Second)); !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("MarkSucceeded on QUEUED job = %v, want ErrInvalidJobTransition", err)
	}

	claim2, err := repo.ClaimNext(ctx, domain.JobKindScrape, tRetry)
	if err != nil {
		t.Fatalf("ClaimNext attempt 2: %v", err)
	}
	if claim2.Attempts != 2 || claim2.State != domain.JobStateRunning {
		t.Fatalf("expected RUNNING attempt 2, got %+v", claim2)
	}

	assertClaim2Preserved := func(step string) {
		t.Helper()
		cur, err := repo.GetJobByID(ctx, job.ID)
		if err != nil {
			t.Fatalf("%s: GetJobByID: %v", step, err)
		}
		if cur.State != domain.JobStateRunning ||
			cur.Attempts != 2 ||
			cur.CancelRequested ||
			cur.FinishedAt != nil ||
			cur.DedupeKey == nil || *cur.DedupeKey != dedupe ||
			cur.LastError == nil || *cur.LastError != "attempt-1-err" ||
			!cur.UpdatedAt.Equal(tRetry) {
			t.Fatalf("%s: newer claim mutated: %+v", step, cur)
		}
		probe := &domain.Job{Kind: domain.JobKindScrape, DedupeKey: &dedupe, CreatedAt: tRetry}
		if probeCreated, err := repo.Enqueue(ctx, probe); err != nil || probeCreated || probe.ID != job.ID {
			t.Fatalf("%s: active dedupe_key check = (created=%v, id=%s, err=%v), want (false, %s, nil)", step, probeCreated, probe.ID, err, job.ID)
		}
	}

	// Stale Worker A (holding attempt 1) tries all three outcome methods; all fail and preserve attempt 2.
	tLate := tRetry.Add(time.Second)
	if _, err := repo.MarkSucceeded(ctx, job.ID, claim1.Attempts, tLate); !errors.Is(err, domain.ErrStaleJobAttempt) || !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("stale MarkSucceeded = %v, want ErrStaleJobAttempt & ErrInvalidJobTransition", err)
	}
	assertClaim2Preserved("after stale MarkSucceeded")

	if _, err := repo.FailOrRetry(ctx, job.ID, claim1.Attempts, "stale-err", tLate.Add(time.Minute), tLate); !errors.Is(err, domain.ErrStaleJobAttempt) || !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("stale FailOrRetry = %v, want ErrStaleJobAttempt & ErrInvalidJobTransition", err)
	}
	assertClaim2Preserved("after stale FailOrRetry")

	staleCancelNote := "stale cancel"
	if _, err := repo.MarkCancelled(ctx, job.ID, claim1.Attempts, &staleCancelNote, tLate); !errors.Is(err, domain.ErrStaleJobAttempt) || !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("stale MarkCancelled = %v, want ErrStaleJobAttempt & ErrInvalidJobTransition", err)
	}
	assertClaim2Preserved("after stale MarkCancelled")

	// Worker B (holding current attempt 2) completes the job cleanly.
	tDone := tRetry.Add(2 * time.Second)
	done, err := repo.MarkSucceeded(ctx, job.ID, claim2.Attempts, tDone)
	if err != nil {
		t.Fatalf("current worker MarkSucceeded: %v", err)
	}
	if done.State != domain.JobStateSucceeded || done.Attempts != 2 || done.LastError != nil || done.FinishedAt == nil || !done.FinishedAt.Equal(tDone) {
		t.Fatalf("unexpected SUCCEEDED job: %+v", done)
	}
}

func TestJobRepository_CheckpointsAndRecoveryPrimitives(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

	// 1. Enqueue and claim a resumable job.
	dedupeResume := "SCAN:recover-resume"
	jResume := &domain.Job{
		Kind:        domain.JobKindScan,
		MaxAttempts: 3,
		DedupeKey:   &dedupeResume,
		CreatedAt:   t0,
	}
	if _, err := repo.Enqueue(ctx, jResume); err != nil {
		t.Fatalf("Enqueue jResume: %v", err)
	}
	// SaveCheckpoint on QUEUED job fails.
	if _, err := repo.SaveCheckpoint(ctx, jResume.ID, 1, json.RawMessage(`{"step":1}`), t0); !errors.Is(err, domain.ErrInvalidJobTransition) {
		t.Fatalf("SaveCheckpoint on QUEUED = %v, want ErrInvalidJobTransition", err)
	}

	claim1, err := repo.ClaimNext(ctx, domain.JobKindScan, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("ClaimNext jResume: %v", err)
	}
	// Invalid JSON checkpoint rejected.
	if _, err := repo.SaveCheckpoint(ctx, jResume.ID, claim1.Attempts, json.RawMessage(`{bad`), t0); !errors.Is(err, domain.ErrInvalidJobJSON) {
		t.Fatalf("SaveCheckpoint invalid JSON = %v, want ErrInvalidJobJSON", err)
	}
	// Stale attempt checkpoint rejected.
	if _, err := repo.SaveCheckpoint(ctx, jResume.ID, claim1.Attempts+1, json.RawMessage(`{"step":99}`), t0); !errors.Is(err, domain.ErrStaleJobAttempt) {
		t.Fatalf("SaveCheckpoint stale attempt = %v, want ErrStaleJobAttempt", err)
	}

	cpSaved := json.RawMessage(`{"cursor":"series/002","progress_pct":55}`)
	updated, err := repo.SaveCheckpoint(ctx, jResume.ID, claim1.Attempts, cpSaved, t0.Add(2*time.Second))
	if err != nil {
		t.Fatalf("SaveCheckpoint valid: %v", err)
	}
	var cpMap map[string]any
	if err := json.Unmarshal(updated.Checkpoint, &cpMap); err != nil || cpMap["cursor"] != "series/002" {
		t.Fatalf("unexpected saved checkpoint: %s (err=%v)", updated.Checkpoint, err)
	}

	// 2. Cancelled RUNNING job + exhausted RUNNING job + MarkFailed job.
	dedupeCancel := "SCAN:recover-cancel"
	jCancel := &domain.Job{
		Kind:        domain.JobKindScan,
		MaxAttempts: 3,
		DedupeKey:   &dedupeCancel,
		CreatedAt:   t0.Add(3 * time.Second),
	}
	if _, err := repo.Enqueue(ctx, jCancel); err != nil {
		t.Fatalf("Enqueue jCancel: %v", err)
	}
	claimCancel, err := repo.ClaimNext(ctx, domain.JobKindScan, t0.Add(4*time.Second))
	if err != nil {
		t.Fatalf("ClaimNext jCancel: %v", err)
	}
	if _, err := repo.RequestCancel(ctx, jCancel.ID, t0.Add(5*time.Second)); err != nil {
		t.Fatalf("RequestCancel jCancel: %v", err)
	}

	dedupeExhausted := "SCAN:recover-exhausted"
	jExhausted := &domain.Job{
		Kind:        domain.JobKindScan,
		MaxAttempts: 1,
		DedupeKey:   &dedupeExhausted,
		CreatedAt:   t0.Add(6 * time.Second),
	}
	if _, err := repo.Enqueue(ctx, jExhausted); err != nil {
		t.Fatalf("Enqueue jExhausted: %v", err)
	}
	claimExhausted, err := repo.ClaimNext(ctx, domain.JobKindScan, t0.Add(7*time.Second))
	if err != nil {
		t.Fatalf("ClaimNext jExhausted: %v", err)
	}

	// ListRunningJobs returns all three in deterministic order.
	running, err := repo.ListRunningJobs(ctx)
	if err != nil {
		t.Fatalf("ListRunningJobs: %v", err)
	}
	if len(running) != 3 || running[0].ID != jResume.ID || running[1].ID != jCancel.ID || running[2].ID != jExhausted.ID {
		t.Fatalf("ListRunningJobs = %+v, want [%s, %s, %s]", running, jResume.ID, jCancel.ID, jExhausted.ID)
	}

	// Stale RequeueInterrupted rejected.
	if _, err := repo.RequeueInterrupted(ctx, jResume.ID, claim1.Attempts+1, cpSaved, nil, t0.Add(8*time.Second), t0.Add(8*time.Second)); !errors.Is(err, domain.ErrStaleJobAttempt) {
		t.Fatalf("stale RequeueInterrupted = %v, want ErrStaleJobAttempt", err)
	}

	// RequeueInterrupted preserves monotonic attempts and checkpoint for resumable job.
	requeued, err := repo.RequeueInterrupted(ctx, jResume.ID, claim1.Attempts, cpSaved, nil, t0.Add(8*time.Second), t0.Add(8*time.Second))
	if err != nil {
		t.Fatalf("RequeueInterrupted jResume: %v", err)
	}
	if requeued.State != domain.JobStateQueued || requeued.Attempts != 1 || requeued.FinishedAt != nil {
		t.Fatalf("unexpected requeued jResume: %+v", requeued)
	}

	// RequeueInterrupted resolves cancel_requested=true to CANCELLED and releases dedupe_key.
	cancelled, err := repo.RequeueInterrupted(ctx, jCancel.ID, claimCancel.Attempts, json.RawMessage(`{"partial":true}`), nil, t0.Add(8*time.Second), t0.Add(8*time.Second))
	if err != nil {
		t.Fatalf("RequeueInterrupted jCancel: %v", err)
	}
	if cancelled.State != domain.JobStateCancelled || !cancelled.CancelRequested || cancelled.FinishedAt == nil {
		t.Fatalf("expected CANCELLED for jCancel, got %+v", cancelled)
	}
	if created, err := repo.Enqueue(ctx, &domain.Job{Kind: domain.JobKindScan, DedupeKey: &dedupeCancel, CreatedAt: t0.Add(9 * time.Second)}); err != nil || !created {
		t.Fatalf("expected dedupe key %s released after recovery cancel, got created=%v err=%v", dedupeCancel, created, err)
	}

	// RequeueInterrupted resolves attempts>=max_attempts to FAILED and releases dedupe_key.
	failed, err := repo.RequeueInterrupted(ctx, jExhausted.ID, claimExhausted.Attempts, nil, nil, t0.Add(8*time.Second), t0.Add(8*time.Second))
	if err != nil {
		t.Fatalf("RequeueInterrupted jExhausted: %v", err)
	}
	if failed.State != domain.JobStateFailed || failed.FinishedAt == nil || failed.LastError == nil {
		t.Fatalf("expected FAILED for jExhausted, got %+v", failed)
	}
	if created, err := repo.Enqueue(ctx, &domain.Job{Kind: domain.JobKindScan, DedupeKey: &dedupeExhausted, CreatedAt: t0.Add(10 * time.Second)}); err != nil || !created {
		t.Fatalf("expected dedupe key %s released after recovery fail, got created=%v err=%v", dedupeExhausted, created, err)
	}

	// MarkFailed transitions directly to FAILED even when attempts < max_attempts.
	jDirectFail := &domain.Job{Kind: domain.JobKindIntake, MaxAttempts: 3, CreatedAt: t0.Add(11 * time.Second)}
	if _, err := repo.Enqueue(ctx, jDirectFail); err != nil {
		t.Fatalf("Enqueue jDirectFail: %v", err)
	}
	claimDF, err := repo.ClaimNext(ctx, domain.JobKindIntake, t0.Add(12*time.Second))
	if err != nil {
		t.Fatalf("ClaimNext jDirectFail: %v", err)
	}
	if _, err := repo.MarkFailed(ctx, jDirectFail.ID, claimDF.Attempts+1, "stale", t0.Add(13*time.Second)); !errors.Is(err, domain.ErrStaleJobAttempt) {
		t.Fatalf("stale MarkFailed = %v, want ErrStaleJobAttempt", err)
	}
	df, err := repo.MarkFailed(ctx, jDirectFail.ID, claimDF.Attempts, "unregistered kind", t0.Add(13*time.Second))
	if err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if df.State != domain.JobStateFailed || df.Attempts != 1 || df.LastError == nil || *df.LastError != "unregistered kind" {
		t.Fatalf("unexpected MarkFailed result: %+v", df)
	}
}

func TestJobRepository_AtomicTerminalRecoveryAndRollback(t *testing.T) {
	t.Parallel()
	db, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 13, 0, 0, 0, time.UTC)

	// 1. RecoverSucceeded commits checkpoint and SUCCEEDED atomically; failed transition rolls back checkpoint.
	jSucc := &domain.Job{Kind: domain.JobKindScan, MaxAttempts: 3, CreatedAt: t0, RunAfter: t0}
	if _, err := repo.Enqueue(ctx, jSucc); err != nil {
		t.Fatalf("Enqueue jSucc: %v", err)
	}
	cSucc, err := repo.ClaimNext(ctx, domain.JobKindScan, t0)
	if err != nil {
		t.Fatalf("ClaimNext jSucc: %v", err)
	}
	origCP := json.RawMessage(`{"step":1,"note":"original"}`)
	if _, err := repo.SaveCheckpoint(ctx, jSucc.ID, cSucc.Attempts, origCP, t0); err != nil {
		t.Fatalf("SaveCheckpoint jSucc: %v", err)
	}

	// Stale attempt failure leaves original checkpoint and RUNNING state intact.
	if _, err := repo.RecoverSucceeded(ctx, jSucc.ID, cSucc.Attempts+1, json.RawMessage(`{"step":999}`), t0); !errors.Is(err, domain.ErrStaleJobAttempt) {
		t.Fatalf("stale RecoverSucceeded = %v, want ErrStaleJobAttempt", err)
	}
	afterStale, err := repo.GetJobByID(ctx, jSucc.ID)
	if err != nil {
		t.Fatalf("GetJobByID after stale: %v", err)
	}
	var cpCheck map[string]any
	if err := json.Unmarshal(afterStale.Checkpoint, &cpCheck); err != nil || cpCheck["note"] != "original" || afterStale.State != domain.JobStateRunning {
		t.Fatalf("checkpoint/state mutated on failed transition: state=%s cp=%s", afterStale.State, afterStale.Checkpoint)
	}

	// Simulate an aborted transaction on a terminal transition (e.g. DB trigger / constraint error).
	if _, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION test_abort_job_terminal_update() RETURNS trigger AS $$
		BEGIN
			IF NEW.state = 'SUCCEEDED' AND NEW.id = '%s' THEN
				RAISE EXCEPTION 'simulated terminal commit failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER trg_test_abort_job_terminal_update
		BEFORE UPDATE ON jobs
		FOR EACH ROW EXECUTE FUNCTION test_abort_job_terminal_update();
	`, jSucc.ID)); err != nil {
		t.Fatalf("create test abort trigger: %v", err)
	}

	if _, err := repo.RecoverSucceeded(ctx, jSucc.ID, cSucc.Attempts, json.RawMessage(`{"step":2,"note":"reconciled"}`), t0.Add(time.Second)); err == nil {
		t.Fatal("expected RecoverSucceeded to fail under simulated DB trigger error")
	}
	afterTriggerFail, err := repo.GetJobByID(ctx, jSucc.ID)
	if err != nil {
		t.Fatalf("GetJobByID after trigger failure: %v", err)
	}
	if err := json.Unmarshal(afterTriggerFail.Checkpoint, &cpCheck); err != nil || cpCheck["note"] != "original" || afterTriggerFail.State != domain.JobStateRunning {
		t.Fatalf("failed terminal transition leaked checkpoint: state=%s cp=%s", afterTriggerFail.State, afterTriggerFail.Checkpoint)
	}

	if _, err := db.Exec(ctx, `DROP TRIGGER trg_test_abort_job_terminal_update ON jobs; DROP FUNCTION test_abort_job_terminal_update();`); err != nil {
		t.Fatalf("cleanup test trigger: %v", err)
	}

	// Now RecoverSucceeded succeeds atomically with the reconciled checkpoint.
	doneSucc, err := repo.RecoverSucceeded(ctx, jSucc.ID, cSucc.Attempts, json.RawMessage(`{"step":2,"note":"reconciled"}`), t0.Add(2*time.Second))
	if err != nil {
		t.Fatalf("RecoverSucceeded: %v", err)
	}
	if err := json.Unmarshal(doneSucc.Checkpoint, &cpCheck); err != nil || cpCheck["note"] != "reconciled" || doneSucc.State != domain.JobStateSucceeded {
		t.Fatalf("doneSucc = (%+v, cp=%s), want SUCCEEDED with reconciled checkpoint", doneSucc, doneSucc.Checkpoint)
	}

	// 2. RecoverCancelled preserves existing checkpoint when override is nil, and commits override when provided.
	jCanc := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: t0, RunAfter: t0}
	if _, err := repo.Enqueue(ctx, jCanc); err != nil {
		t.Fatalf("Enqueue jCanc: %v", err)
	}
	cCanc, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0)
	if err != nil {
		t.Fatalf("ClaimNext jCanc: %v", err)
	}
	if _, err := repo.SaveCheckpoint(ctx, jCanc.ID, cCanc.Attempts, json.RawMessage(`{"kept":true}`), t0); err != nil {
		t.Fatalf("SaveCheckpoint jCanc: %v", err)
	}
	reason := "cancelled in recovery"
	doneCanc, err := repo.RecoverCancelled(ctx, jCanc.ID, cCanc.Attempts, nil, &reason, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("RecoverCancelled: %v", err)
	}
	if err := json.Unmarshal(doneCanc.Checkpoint, &cpCheck); err != nil || cpCheck["kept"] != true || doneCanc.State != domain.JobStateCancelled {
		t.Fatalf("doneCanc = (%+v, cp=%s), want CANCELLED preserving checkpoint", doneCanc, doneCanc.Checkpoint)
	}

	// 3. RecoverFailed commits override checkpoint and FAILED atomically.
	jFail := &domain.Job{Kind: domain.JobKindRepack, MaxAttempts: 3, CreatedAt: t0, RunAfter: t0}
	if _, err := repo.Enqueue(ctx, jFail); err != nil {
		t.Fatalf("Enqueue jFail: %v", err)
	}
	cFail, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0)
	if err != nil {
		t.Fatalf("ClaimNext jFail: %v", err)
	}
	doneFail, err := repo.RecoverFailed(ctx, jFail.ID, cFail.Attempts, json.RawMessage(`{"evidence":"corrupt-tail"}`), "corrupt", t0.Add(time.Second))
	if err != nil {
		t.Fatalf("RecoverFailed: %v", err)
	}
	if err := json.Unmarshal(doneFail.Checkpoint, &cpCheck); err != nil || cpCheck["evidence"] != "corrupt-tail" || doneFail.State != domain.JobStateFailed {
		t.Fatalf("doneFail = (%+v, cp=%s), want FAILED with evidence checkpoint", doneFail, doneFail.Checkpoint)
	}
}

func TestJobRepository_HasFailedEventDeliverySince(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 15, 0, 0, 0, time.UTC)
	payloadBytes, err := json.Marshal(domain.EventDeliveryPayloadV1{
		SubscriberID:      "sub:one",
		PublishedSequence: 3,
		EventID:           10,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	dedupeKey := "EVENT_DELIVERY:sub:one"
	j := &domain.Job{
		Kind:        domain.JobKindEventDelivery,
		Payload:     payloadBytes,
		DedupeKey:   &dedupeKey,
		MaxAttempts: 1,
		CreatedAt:   t0,
		RunAfter:    t0,
	}
	if _, err := repo.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindEventDelivery, t0); err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}

	// While RUNNING, HasFailedEventDeliverySince must be false.
	has, err := repo.HasFailedEventDeliverySince(ctx, "sub:one", 3, t0)
	if err != nil || has {
		t.Fatalf("while RUNNING: has=%v err=%v, want false", has, err)
	}

	failTime := t0.Add(5 * time.Second)
	if _, err := repo.FailOrRetry(ctx, j.ID, 1, "downstream error", time.Time{}, failTime); err != nil {
		t.Fatalf("FailOrRetry: %v", err)
	}

	// Once FAILED at failTime, queries since <= failTime match; queries since > failTime do not.
	if has, err := repo.HasFailedEventDeliverySince(ctx, "sub:one", 3, t0); err != nil || !has {
		t.Fatalf("at t0: has=%v err=%v, want true", has, err)
	}
	if has, err := repo.HasFailedEventDeliverySince(ctx, "sub:one", 3, failTime); err != nil || !has {
		t.Fatalf("at failTime: has=%v err=%v, want true", has, err)
	}
	if has, err := repo.HasFailedEventDeliverySince(ctx, "sub:one", 3, failTime.Add(time.Second)); err != nil || has {
		t.Fatalf("after failTime: has=%v err=%v, want false", has, err)
	}
	if has, err := repo.HasFailedEventDeliverySince(ctx, "sub:one", 4, t0); err != nil || has {
		t.Fatalf("other seq: has=%v err=%v, want false", has, err)
	}
	if has, err := repo.HasFailedEventDeliverySince(ctx, "sub:two", 3, t0); err != nil || has {
		t.Fatalf("other subscriber: has=%v err=%v, want false", has, err)
	}
}

func TestJobRepository_EventDeliveryEnforcesCanonicalDedupeKey(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 15, 30, 0, 0, time.UTC)
	payloadBytes, err := json.Marshal(domain.EventDeliveryPayloadV1{
		SubscriberID:      "sub:canon",
		PublishedSequence: 1,
		EventID:           1,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	// 1. Missing dedupe key rejected.
	if _, err := repo.Enqueue(ctx, &domain.Job{
		Kind:      domain.JobKindEventDelivery,
		Payload:   payloadBytes,
		CreatedAt: t0,
		RunAfter:  t0,
	}); !errors.Is(err, domain.ErrInvalidEventDeliveryDedupeKey) {
		t.Fatalf("missing dedupe_key err = %v, want ErrInvalidEventDeliveryDedupeKey", err)
	}

	// 2. Mismatched dedupe key rejected.
	wrongKey := "EVENT_DELIVERY:sub:other"
	if _, err := repo.Enqueue(ctx, &domain.Job{
		Kind:      domain.JobKindEventDelivery,
		Payload:   payloadBytes,
		DedupeKey: &wrongKey,
		CreatedAt: t0,
		RunAfter:  t0,
	}); !errors.Is(err, domain.ErrInvalidEventDeliveryDedupeKey) {
		t.Fatalf("mismatched dedupe_key err = %v, want ErrInvalidEventDeliveryDedupeKey", err)
	}

	// 3. Invalid payload rejected.
	canonKey, _ := domain.EventDeliveryDedupeKey("sub:canon")
	if _, err := repo.Enqueue(ctx, &domain.Job{
		Kind:      domain.JobKindEventDelivery,
		Payload:   json.RawMessage(`{"subscriber_id":"sub:canon"}`),
		DedupeKey: &canonKey,
		CreatedAt: t0,
		RunAfter:  t0,
	}); !errors.Is(err, domain.ErrInvalidDeliveryPayload) {
		t.Fatalf("invalid payload err = %v, want ErrInvalidDeliveryPayload", err)
	}

	// 4. Canonical dedupe key succeeds.
	created, err := repo.Enqueue(ctx, &domain.Job{
		Kind:      domain.JobKindEventDelivery,
		Payload:   payloadBytes,
		DedupeKey: &canonKey,
		CreatedAt: t0,
		RunAfter:  t0,
	})
	if err != nil || !created {
		t.Fatalf("canonical Enqueue = (%v, %v), want (true, nil)", created, err)
	}
}

func listJobLifecycleEventsForJob(ctx context.Context, t *testing.T, db *testdb.TestDB, jobID string) []domain.DomainEvent {
	t.Helper()
	rows, err := db.Query(ctx, `
		SELECT id, published_sequence, published_at, kind, version, aggregate_type, aggregate_id, payload, created_at
		FROM domain_events
		WHERE aggregate_type = 'job' AND aggregate_id = $1
		ORDER BY id ASC
	`, jobID)
	if err != nil {
		t.Fatalf("querying domain_events for job %q: %v", jobID, err)
	}
	defer rows.Close()

	var out []domain.DomainEvent
	for rows.Next() {
		var (
			ev           domain.DomainEvent
			publishedSeq *int64
			publishedAt  *time.Time
			payload      []byte
		)
		if err := rows.Scan(
			&ev.ID,
			&publishedSeq,
			&publishedAt,
			&ev.Kind,
			&ev.Version,
			&ev.AggregateType,
			&ev.AggregateID,
			&payload,
			&ev.CreatedAt,
		); err != nil {
			t.Fatalf("scanning domain_event for job %q: %v", jobID, err)
		}
		ev.PublishedSequence = publishedSeq
		ev.PublishedAt = publishedAt
		ev.Payload = json.RawMessage(payload)
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating domain_events for job %q: %v", jobID, err)
	}
	return out
}

func assertJobLifecycleEvent(
	t *testing.T,
	ev domain.DomainEvent,
	wantKind domain.EventKind,
	wantJobID string,
	wantJobKind domain.JobKind,
	wantState domain.JobState,
	wantAttempts int,
	wantErr *string,
) {
	t.Helper()
	if ev.PublishedSequence != nil || ev.PublishedAt != nil {
		t.Fatalf("outbox event %d must stay unpublished, got seq=%v at=%v", ev.ID, ev.PublishedSequence, ev.PublishedAt)
	}
	if ev.Kind != wantKind || ev.Version != domain.EventVersion1 || ev.AggregateType != domain.AggregateTypeJob || ev.AggregateID != wantJobID {
		t.Fatalf("event envelope = %+v, want kind=%s job=%s", ev, wantKind, wantJobID)
	}
	payload, err := domain.DecodeEventPayload[domain.JobEventPayloadV1](ev)
	if err != nil {
		t.Fatalf("DecodeEventPayload(%s): %v", ev.Kind, err)
	}
	if payload.JobID != wantJobID || payload.Kind != wantJobKind || payload.State != wantState || payload.Attempts != wantAttempts {
		t.Fatalf("payload = %+v, want job=%s kind=%s state=%s attempts=%d", payload, wantJobID, wantJobKind, wantState, wantAttempts)
	}
	if (wantErr == nil) != (payload.LastError == nil) || (wantErr != nil && *wantErr != *payload.LastError) {
		t.Fatalf("payload.LastError = %v, want %v", payload.LastError, wantErr)
	}
}

func TestJobRepository_LifecycleOutboxEventsCommittedRolledBackAndRetried(t *testing.T) {
	t.Parallel()
	db, repo, userRepo := setupJobTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 18, 30, 0, 0, time.UTC)
	creator := &domain.User{
		ID:       domain.MustNewID(),
		Username: "lifecycle-creator",
	}
	if err := userRepo.CreateUser(ctx, creator); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// 1. Committed retry + success path: Enqueue (0 events) -> ClaimNext #1 (job.started) ->
	// FailOrRetry requeue (0 new events) -> ClaimNext #2 (job.started) -> MarkSucceeded (job.finished).
	scanJob := &domain.Job{
		Kind:        domain.JobKindScan,
		MaxAttempts: 2,
		CreatedBy:   &creator.ID,
		CreatedAt:   t0,
		RunAfter:    t0,
	}
	if _, err := repo.Enqueue(ctx, scanJob); err != nil {
		t.Fatalf("Enqueue scanJob: %v", err)
	}
	if evs := listJobLifecycleEventsForJob(ctx, t, db, scanJob.ID); len(evs) != 0 {
		t.Fatalf("expected 0 events on QUEUED enqueue, got %d", len(evs))
	}

	c1, err := repo.ClaimNext(ctx, domain.JobKindScan, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("ClaimNext #1: %v", err)
	}
	evs := listJobLifecycleEventsForJob(ctx, t, db, scanJob.ID)
	if len(evs) != 1 {
		t.Fatalf("after ClaimNext #1 got %d events, want 1", len(evs))
	}
	assertJobLifecycleEvent(t, evs[0], domain.EventJobStarted, scanJob.ID, domain.JobKindScan, domain.JobStateRunning, 1, nil)

	// Retry requeue (RUNNING -> QUEUED) writes no lifecycle event.
	retryTime := t0.Add(2 * time.Second)
	if _, err := repo.FailOrRetry(ctx, scanJob.ID, c1.Attempts, "transient io err", retryTime, retryTime); err != nil {
		t.Fatalf("FailOrRetry retry: %v", err)
	}
	if evs := listJobLifecycleEventsForJob(ctx, t, db, scanJob.ID); len(evs) != 1 {
		t.Fatalf("expected 0 new events on retry requeue, got %d total", len(evs))
	}

	c2, err := repo.ClaimNext(ctx, domain.JobKindScan, t0.Add(3*time.Second))
	if err != nil {
		t.Fatalf("ClaimNext #2: %v", err)
	}
	if _, err := repo.MarkSucceeded(ctx, scanJob.ID, c2.Attempts, t0.Add(4*time.Second)); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}
	evs = listJobLifecycleEventsForJob(ctx, t, db, scanJob.ID)
	if len(evs) != 3 {
		t.Fatalf("after retry + succeed got %d events, want 3", len(evs))
	}
	retryErr := "transient io err"
	assertJobLifecycleEvent(t, evs[1], domain.EventJobStarted, scanJob.ID, domain.JobKindScan, domain.JobStateRunning, 2, &retryErr)
	assertJobLifecycleEvent(t, evs[2], domain.EventJobFinished, scanJob.ID, domain.JobKindScan, domain.JobStateSucceeded, 2, nil)

	// 2. Exhausted FailOrRetry emits job.failed.
	failJob := &domain.Job{
		Kind:        domain.JobKindIntake,
		MaxAttempts: 1,
		CreatedAt:   t0,
		RunAfter:    t0,
	}
	if _, err := repo.Enqueue(ctx, failJob); err != nil {
		t.Fatalf("Enqueue failJob: %v", err)
	}
	cf, err := repo.ClaimNext(ctx, domain.JobKindIntake, t0.Add(5*time.Second))
	if err != nil {
		t.Fatalf("ClaimNext failJob: %v", err)
	}
	if _, err := repo.FailOrRetry(ctx, failJob.ID, cf.Attempts, "boom", time.Time{}, t0.Add(6*time.Second)); err != nil {
		t.Fatalf("FailOrRetry terminal: %v", err)
	}
	evs = listJobLifecycleEventsForJob(ctx, t, db, failJob.ID)
	if len(evs) != 2 {
		t.Fatalf("failJob events = %d, want 2", len(evs))
	}
	boom := "boom"
	assertJobLifecycleEvent(t, evs[0], domain.EventJobStarted, failJob.ID, domain.JobKindIntake, domain.JobStateRunning, 1, nil)
	assertJobLifecycleEvent(t, evs[1], domain.EventJobFailed, failJob.ID, domain.JobKindIntake, domain.JobStateFailed, 1, &boom)

	// 3. Cancellation paths:
	// 3a. RequestCancel on QUEUED emits job.cancelled directly.
	queuedCancelJob := &domain.Job{Kind: domain.JobKindRepack, CreatedAt: t0, RunAfter: t0}
	if _, err := repo.Enqueue(ctx, queuedCancelJob); err != nil {
		t.Fatalf("Enqueue queuedCancelJob: %v", err)
	}
	if _, err := repo.RequestCancel(ctx, queuedCancelJob.ID, t0.Add(time.Second)); err != nil {
		t.Fatalf("RequestCancel queued: %v", err)
	}
	evs = listJobLifecycleEventsForJob(ctx, t, db, queuedCancelJob.ID)
	if len(evs) != 1 {
		t.Fatalf("queuedCancelJob events = %d, want 1", len(evs))
	}
	assertJobLifecycleEvent(t, evs[0], domain.EventJobCancelled, queuedCancelJob.ID, domain.JobKindRepack, domain.JobStateCancelled, 0, nil)

	// 3b. RequestCancel on RUNNING emits nothing while RUNNING; MarkCancelled emits job.cancelled.
	runCancelJob := &domain.Job{Kind: domain.JobKindRepack, CreatedAt: t0, RunAfter: t0}
	if _, err := repo.Enqueue(ctx, runCancelJob); err != nil {
		t.Fatalf("Enqueue runCancelJob: %v", err)
	}
	cr, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("ClaimNext runCancelJob: %v", err)
	}
	if _, err := repo.RequestCancel(ctx, runCancelJob.ID, t0.Add(2*time.Second)); err != nil {
		t.Fatalf("RequestCancel running: %v", err)
	}
	if evs := listJobLifecycleEventsForJob(ctx, t, db, runCancelJob.ID); len(evs) != 1 {
		t.Fatalf("RequestCancel on RUNNING should not emit extra event, got %d", len(evs))
	}
	cancelMsg := "cancelled by user"
	if _, err := repo.MarkCancelled(ctx, runCancelJob.ID, cr.Attempts, &cancelMsg, t0.Add(3*time.Second)); err != nil {
		t.Fatalf("MarkCancelled: %v", err)
	}
	evs = listJobLifecycleEventsForJob(ctx, t, db, runCancelJob.ID)
	if len(evs) != 2 {
		t.Fatalf("runCancelJob events = %d, want 2", len(evs))
	}
	assertJobLifecycleEvent(t, evs[1], domain.EventJobCancelled, runCancelJob.ID, domain.JobKindRepack, domain.JobStateCancelled, 1, &cancelMsg)

	// 4. Recovery transitions (RecoverSucceeded, RecoverCancelled, RecoverFailed, RequeueInterrupted).
	recJobs := make([]*domain.Job, 4)
	for i := range recJobs {
		j := &domain.Job{Kind: domain.JobKindWriteBack, MaxAttempts: 2, CreatedAt: t0, RunAfter: t0}
		if _, err := repo.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue recJob[%d]: %v", i, err)
		}
		claimed, err := repo.ClaimNext(ctx, domain.JobKindWriteBack, t0.Add(time.Second))
		if err != nil {
			t.Fatalf("ClaimNext recJob[%d]: %v", i, err)
		}
		recJobs[i] = claimed
	}
	if _, err := repo.RecoverSucceeded(ctx, recJobs[0].ID, 1, nil, t0.Add(2*time.Second)); err != nil {
		t.Fatalf("RecoverSucceeded: %v", err)
	}
	if _, err := repo.RecoverCancelled(ctx, recJobs[1].ID, 1, nil, nil, t0.Add(2*time.Second)); err != nil {
		t.Fatalf("RecoverCancelled: %v", err)
	}
	if _, err := repo.RecoverFailed(ctx, recJobs[2].ID, 1, nil, "fatal recovery", t0.Add(2*time.Second)); err != nil {
		t.Fatalf("RecoverFailed: %v", err)
	}
	if _, err := repo.RequeueInterrupted(ctx, recJobs[3].ID, 1, nil, nil, t0.Add(2*time.Second), t0.Add(2*time.Second)); err != nil {
		t.Fatalf("RequeueInterrupted: %v", err)
	}
	assertJobLifecycleEvent(t, listJobLifecycleEventsForJob(ctx, t, db, recJobs[0].ID)[1], domain.EventJobFinished, recJobs[0].ID, domain.JobKindWriteBack, domain.JobStateSucceeded, 1, nil)
	assertJobLifecycleEvent(t, listJobLifecycleEventsForJob(ctx, t, db, recJobs[1].ID)[1], domain.EventJobCancelled, recJobs[1].ID, domain.JobKindWriteBack, domain.JobStateCancelled, 1, nil)
	fatalRec := "fatal recovery"
	assertJobLifecycleEvent(t, listJobLifecycleEventsForJob(ctx, t, db, recJobs[2].ID)[1], domain.EventJobFailed, recJobs[2].ID, domain.JobKindWriteBack, domain.JobStateFailed, 1, &fatalRec)
	if evs := listJobLifecycleEventsForJob(ctx, t, db, recJobs[3].ID); len(evs) != 1 {
		t.Fatalf("RequeueInterrupted to QUEUED emitted unexpected event: count=%d", len(evs))
	}

	// 5. Rolled-back caller transaction reverts both job transition and lifecycle event.
	rbJob := &domain.Job{Kind: domain.JobKindPurge, CreatedAt: t0, RunAfter: t0}
	if _, err := repo.Enqueue(ctx, rbJob); err != nil {
		t.Fatalf("Enqueue rbJob: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindPurge, t0.Add(time.Second)); err != nil {
		t.Fatalf("ClaimNext rbJob: %v", err)
	}
	rollbackErr := errors.New("abort caller tx")
	if err := repo.WithTx(ctx, func(txRepo storage.JobRepository) error {
		if _, err := txRepo.MarkSucceeded(ctx, rbJob.ID, 1, t0.Add(2*time.Second)); err != nil {
			return err
		}
		return rollbackErr
	}); !errors.Is(err, rollbackErr) {
		t.Fatalf("WithTx err = %v, want %v", err, rollbackErr)
	}
	gotRB, err := repo.GetJobByID(ctx, rbJob.ID)
	if err != nil || gotRB.State != domain.JobStateRunning {
		t.Fatalf("rolled-back job state = (%+v, %v), want RUNNING", gotRB, err)
	}
	if evs := listJobLifecycleEventsForJob(ctx, t, db, rbJob.ID); len(evs) != 1 || evs[0].Kind != domain.EventJobStarted {
		t.Fatalf("rolled-back transition left unexpected events: %+v", evs)
	}

	// 6. Failure appending to domain_events inside MarkSucceeded rolls back the job state update too!
	if _, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION fail_job_finished_outbox_%s() RETURNS TRIGGER AS $$
		BEGIN
			IF NEW.aggregate_id = '%s' AND NEW.kind = 'job.finished' THEN
				RAISE EXCEPTION 'simulated outbox write failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER trg_fail_job_finished_outbox
		BEFORE INSERT ON domain_events
		FOR EACH ROW EXECUTE FUNCTION fail_job_finished_outbox_%s();
	`, "test", rbJob.ID, "test")); err != nil {
		t.Fatalf("creating outbox fault trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DROP TRIGGER IF EXISTS trg_fail_job_finished_outbox ON domain_events`)
		_, _ = db.Exec(context.Background(), `DROP FUNCTION IF EXISTS fail_job_finished_outbox_test()`)
	})

	if _, err := repo.MarkSucceeded(ctx, rbJob.ID, 1, t0.Add(3*time.Second)); err == nil {
		t.Fatal("expected MarkSucceeded to fail when domain_events insert fails")
	}
	gotAfterFault, err := repo.GetJobByID(ctx, rbJob.ID)
	if err != nil || gotAfterFault.State != domain.JobStateRunning {
		t.Fatalf("job must remain RUNNING when outbox insert fails in tx, got (%+v, %v)", gotAfterFault, err)
	}
	if evs := listJobLifecycleEventsForJob(ctx, t, db, rbJob.ID); len(evs) != 1 {
		t.Fatalf("expected still 1 event after failed MarkSucceeded, got %d", len(evs))
	}

	// Clear fault and retry MarkSucceeded: transitions cleanly and writes exactly 1 job.finished event.
	if _, err := db.Exec(ctx, `DROP TRIGGER trg_fail_job_finished_outbox ON domain_events`); err != nil {
		t.Fatalf("dropping trigger: %v", err)
	}
	if _, err := repo.MarkSucceeded(ctx, rbJob.ID, 1, t0.Add(4*time.Second)); err != nil {
		t.Fatalf("MarkSucceeded retry after fault cleared: %v", err)
	}
	evs = listJobLifecycleEventsForJob(ctx, t, db, rbJob.ID)
	if len(evs) != 2 {
		t.Fatalf("after retry got %d events, want 2", len(evs))
	}
	assertJobLifecycleEvent(t, evs[1], domain.EventJobFinished, rbJob.ID, domain.JobKindPurge, domain.JobStateSucceeded, 1, nil)

	// 7. Internal EVENT_DELIVERY jobs suppress lifecycle events across every transition.
	delivPayload, _ := json.Marshal(domain.EventDeliveryPayloadV1{
		SubscriberID:      "sub:suppress",
		PublishedSequence: 1,
		EventID:           1,
	})
	delivKey, _ := domain.EventDeliveryDedupeKey("sub:suppress")
	delivJob := &domain.Job{
		Kind:        domain.JobKindEventDelivery,
		Payload:     delivPayload,
		DedupeKey:   &delivKey,
		MaxAttempts: 2,
		CreatedAt:   t0,
		RunAfter:    t0,
	}
	if _, err := repo.Enqueue(ctx, delivJob); err != nil {
		t.Fatalf("Enqueue delivJob: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindEventDelivery, t0.Add(time.Second)); err != nil {
		t.Fatalf("ClaimNext delivJob: %v", err)
	}
	if _, err := repo.MarkSucceeded(ctx, delivJob.ID, 1, t0.Add(2*time.Second)); err != nil {
		t.Fatalf("MarkSucceeded delivJob: %v", err)
	}
	if evs := listJobLifecycleEventsForJob(ctx, t, db, delivJob.ID); len(evs) != 0 {
		t.Fatalf("expected 0 lifecycle events for EVENT_DELIVERY job, got %d", len(evs))
	}
}

func TestJobRepository_EnqueueExclusiveAndIsolation(t *testing.T) {
	t.Parallel()
	db, repo, _ := setupJobTestDB(t)
	transactor := postgres.NewTransactor(db.Pool)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	scanKey := "SCAN:lib-excl"
	relocKey := "RELOCATE_ROOT:lib-excl"
	lockKey := "library:lib-excl"

	// 1. Persisted SCAN blocks RELOCATE_ROOT across QUEUED, RUNNING, and retry backoff.
	scanJob := &domain.Job{
		Kind:        domain.JobKindScan,
		DedupeKey:   &scanKey,
		MaxAttempts: 2,
		CreatedAt:   t0,
		RunAfter:    t0,
	}
	created, err := repo.Enqueue(ctx, scanJob)
	if err != nil || !created {
		t.Fatalf("Enqueue scanJob = (%v, %v)", created, err)
	}

	if _, err := repo.EnqueueExclusive(ctx, &domain.Job{
		Kind:      domain.JobKindRelocateRoot,
		DedupeKey: &relocKey,
	}, lockKey, []string{scanKey, relocKey}); !errors.Is(err, domain.ErrJobConflict) {
		t.Fatalf("EnqueueExclusive(RELOCATE_ROOT while SCAN QUEUED) = %v, want ErrJobConflict", err)
	}

	// Claim SCAN -> RUNNING; RELOCATE_ROOT still blocked, repeat SCAN deduplicates.
	claimedScan, err := repo.ClaimNext(ctx, domain.JobKindScan, t0)
	if err != nil {
		t.Fatalf("ClaimNext SCAN: %v", err)
	}
	if _, err := repo.EnqueueExclusive(ctx, &domain.Job{
		Kind:      domain.JobKindRelocateRoot,
		DedupeKey: &relocKey,
	}, lockKey, []string{scanKey, relocKey}); !errors.Is(err, domain.ErrJobConflict) {
		t.Fatalf("EnqueueExclusive(RELOCATE_ROOT while SCAN RUNNING) = %v, want ErrJobConflict", err)
	}
	repeatScan := &domain.Job{Kind: domain.JobKindScan, DedupeKey: &scanKey}
	if created, err := repo.EnqueueExclusive(ctx, repeatScan, lockKey, []string{relocKey}); err != nil || created || repeatScan.ID != scanJob.ID {
		t.Fatalf("repeat SCAN = (%v, %v, %q), want (false, nil, %q)", created, err, repeatScan.ID, scanJob.ID)
	}

	// Put SCAN into retry backoff (QUEUED with future RunAfter); RELOCATE_ROOT still blocked.
	if _, err := repo.FailOrRetry(ctx, claimedScan.ID, claimedScan.Attempts, "transient", t0.Add(time.Minute), t0.Add(time.Second)); err != nil {
		t.Fatalf("FailOrRetry SCAN: %v", err)
	}
	if _, err := repo.EnqueueExclusive(ctx, &domain.Job{
		Kind:      domain.JobKindRelocateRoot,
		DedupeKey: &relocKey,
	}, lockKey, []string{scanKey, relocKey}); !errors.Is(err, domain.ErrJobConflict) {
		t.Fatalf("EnqueueExclusive(RELOCATE_ROOT while SCAN in backoff) = %v, want ErrJobConflict", err)
	}

	// Finish SCAN -> reservation released; RELOCATE_ROOT succeeds.
	claimedScan2, err := repo.ClaimNext(ctx, domain.JobKindScan, t0.Add(time.Minute))
	if err != nil {
		t.Fatalf("ClaimNext SCAN retry: %v", err)
	}
	if _, err := repo.MarkSucceeded(ctx, claimedScan2.ID, claimedScan2.Attempts, t0.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkSucceeded SCAN: %v", err)
	}

	relocJob := &domain.Job{
		Kind:      domain.JobKindRelocateRoot,
		DedupeKey: &relocKey,
		CreatedAt: t0.Add(3 * time.Minute),
		RunAfter:  t0.Add(3 * time.Minute),
	}
	if created, err := repo.EnqueueExclusive(ctx, relocJob, lockKey, []string{scanKey, relocKey}); err != nil || !created {
		t.Fatalf("EnqueueExclusive(RELOCATE_ROOT after SCAN terminal) = (%v, %v)", created, err)
	}

	// Active RELOCATE_ROOT blocks both SCAN and a second RELOCATE_ROOT.
	if _, err := repo.EnqueueExclusive(ctx, &domain.Job{
		Kind:      domain.JobKindScan,
		DedupeKey: &scanKey,
	}, lockKey, []string{relocKey}); !errors.Is(err, domain.ErrJobConflict) {
		t.Fatalf("EnqueueExclusive(SCAN while RELOCATE_ROOT active) = %v, want ErrJobConflict", err)
	}
	if _, err := repo.EnqueueExclusive(ctx, &domain.Job{
		Kind:      domain.JobKindRelocateRoot,
		DedupeKey: &relocKey,
	}, lockKey, []string{scanKey, relocKey}); !errors.Is(err, domain.ErrJobConflict) {
		t.Fatalf("EnqueueExclusive(second RELOCATE_ROOT) = %v, want ErrJobConflict", err)
	}

	// 2. Stricter isolation caller transaction fails closed with ErrTransactionIsolation.
	if err := transactor.WithRepeatableReadTx(ctx, func(_ storage.ReadTx) error {
		return nil
	}); err != nil {
		t.Fatalf("WithRepeatableReadTx smoke check: %v", err)
	}
	txRR, err := db.Pool.BeginTx(ctx, pgxTxRepeatableRead())
	if err != nil {
		t.Fatalf("BeginTx REPEATABLE READ: %v", err)
	}
	defer func() { _ = txRR.Rollback(ctx) }()
	rrRepo := postgres.NewJobRepository(txRR)
	otherKey := "RELOCATE_ROOT:lib-rr"
	if _, err := rrRepo.EnqueueExclusive(ctx, &domain.Job{
		Kind:      domain.JobKindRelocateRoot,
		DedupeKey: &otherKey,
	}, "library:lib-rr", []string{"SCAN:lib-rr", otherKey}); !errors.Is(err, storage.ErrTransactionIsolation) {
		t.Fatalf("EnqueueExclusive inside REPEATABLE READ = %v, want ErrTransactionIsolation", err)
	}
	_ = txRR.Rollback(ctx)

	// 3. Concurrent barrier race on an independent library: exactly 1 succeeds, 1 gets ErrJobConflict.
	raceScanKey := "SCAN:lib-race"
	raceRelocKey := "RELOCATE_ROOT:lib-race"
	raceLockKey := "library:lib-race"
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			if idx == 0 {
				_, errs[idx] = repo.EnqueueExclusive(ctx, &domain.Job{
					Kind:      domain.JobKindScan,
					DedupeKey: &raceScanKey,
				}, raceLockKey, []string{raceRelocKey})
			} else {
				_, errs[idx] = repo.EnqueueExclusive(ctx, &domain.Job{
					Kind:      domain.JobKindRelocateRoot,
					DedupeKey: &raceRelocKey,
				}, raceLockKey, []string{raceScanKey, raceRelocKey})
			}
		}(i)
	}
	close(start)
	wg.Wait()

	var okCount, conflictCount int
	for _, e := range errs {
		switch {
		case e == nil:
			okCount++
		case errors.Is(e, domain.ErrJobConflict):
			conflictCount++
		default:
			t.Fatalf("unexpected concurrent EnqueueExclusive error: %v", e)
		}
	}
	if okCount != 1 || conflictCount != 1 {
		t.Fatalf("concurrent race outcomes = (ok=%d, conflict=%d), errs=%v", okCount, conflictCount, errs)
	}
}
