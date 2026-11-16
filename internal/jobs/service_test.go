package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

type recordingNotifier struct {
	mu    sync.Mutex
	kinds []domain.JobKind
}

func (n *recordingNotifier) Notify(kind domain.JobKind) {
	n.mu.Lock()
	n.kinds = append(n.kinds, kind)
	n.mu.Unlock()
}

func (n *recordingNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.kinds)
}

func createServiceTestUser(ctx context.Context, t *testing.T, db *testdb.TestDB, username string, roleID string) domain.User {
	t.Helper()
	uid, err := domain.NewID()
	if err != nil {
		t.Fatalf("NewID: %v", err)
	}
	userRepo := postgres.NewUserRepository(db.Pool)
	u := &domain.User{ID: uid, Username: username}
	if err := userRepo.CreateUser(ctx, u); err != nil {
		t.Fatalf("CreateUser(%s): %v", username, err)
	}
	if roleID != "" {
		if err := userRepo.AssignUserRole(ctx, u.ID, roleID); err != nil {
			t.Fatalf("AssignUserRole(%s): %v", username, err)
		}
	}
	return *u
}

func mustBuiltInRole(t *testing.T, id string) domain.Role {
	t.Helper()
	r, ok := domain.BuiltInRoleByID(id)
	if !ok {
		t.Fatalf("built-in role %q not found", id)
	}
	return r
}

func TestService_TriggerDeduplicationAndValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, repo := setupRunnerTestDB(t)

	notifier := &recordingNotifier{}
	t0 := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	svc := jobs.NewService(repo, jobs.ServiceOptions{
		Now:      func() time.Time { return t0 },
		Notifier: notifier,
	})

	creator := createServiceTestUser(ctx, t, db, "alice-trigger", domain.RoleReaderID)
	creatorPrincipal := domain.NewPrincipal(creator, []domain.Role{mustBuiltInRole(t, domain.RoleReaderID)}, domain.CredentialKindPassword, "c-alice", nil)

	// Validation guards.
	if _, err := svc.Trigger(ctx, jobs.TriggerRequest{}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("empty kind err = %v, want ErrInvalidInput", err)
	}
	emptyStr := "  "
	if _, err := svc.Trigger(ctx, jobs.TriggerRequest{Kind: domain.JobKindScan, DedupeKey: &emptyStr}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("empty dedupe_key err = %v, want ErrInvalidInput", err)
	}
	if _, err := svc.Trigger(ctx, jobs.TriggerRequest{Kind: domain.JobKindScan, CreatedBy: &emptyStr}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("empty created_by err = %v, want ErrInvalidInput", err)
	}
	if _, err := svc.Trigger(ctx, jobs.TriggerRequest{Kind: domain.JobKindScan, Payload: json.RawMessage(`{broken`)}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("invalid json payload err = %v, want ErrInvalidInput", err)
	}
	if _, err := svc.Trigger(ctx, jobs.TriggerRequest{Kind: domain.JobKindScan, MaxAttempts: -1}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("negative max_attempts err = %v, want ErrInvalidInput", err)
	}
	if _, err := jobs.TriggerJob(ctx, nil, nil, jobs.TriggerRequest{Kind: domain.JobKindScan}, t0); !errors.Is(err, app.ErrServiceBusy) {
		t.Fatalf("nil repo err = %v, want ErrServiceBusy", err)
	}

	dedupe := "SCAN:lib-42"
	id1, err := svc.Trigger(ctx, jobs.TriggerRequest{
		Kind:      domain.JobKindScan,
		Payload:   json.RawMessage(`{"library_id":"lib-42"}`),
		DedupeKey: &dedupe,
		CreatedBy: &creator.ID,
	})
	if err != nil {
		t.Fatalf("first Trigger: %v", err)
	}
	if id1 == "" {
		t.Fatal("expected non-empty job ID")
	}
	if notifier.count() != 1 {
		t.Fatalf("notifier count = %d, want 1", notifier.count())
	}

	// Repeated trigger with same active dedupe_key returns the same job ID without waking again.
	id2, err := svc.Trigger(ctx, jobs.TriggerRequest{
		Kind:      domain.JobKindScan,
		Payload:   json.RawMessage(`{"library_id":"lib-42"}`),
		DedupeKey: &dedupe,
		CreatedBy: &creator.ID,
	})
	if err != nil {
		t.Fatalf("duplicate Trigger: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("duplicate Trigger returned %q, want %q", id2, id1)
	}
	if notifier.count() != 1 {
		t.Fatalf("duplicate trigger unexpectedly woke notifier (%d times)", notifier.count())
	}

	// Cancelling releases the dedupe_key so a subsequent trigger creates a fresh job.
	if _, err := svc.CancelJob(ctx, creatorPrincipal, id1); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	id3, err := svc.Trigger(ctx, jobs.TriggerRequest{
		Kind:      domain.JobKindScan,
		Payload:   json.RawMessage(`{"library_id":"lib-42"}`),
		DedupeKey: &dedupe,
		CreatedBy: &creator.ID,
	})
	if err != nil {
		t.Fatalf("post-cancel Trigger: %v", err)
	}
	if id3 == id1 {
		t.Fatalf("post-cancel Trigger reused old job ID %q", id3)
	}
	if notifier.count() != 2 {
		t.Fatalf("notifier count = %d, want 2", notifier.count())
	}
}

func TestService_VisibilityAndCancellationMatrix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, repo := setupRunnerTestDB(t)

	var transitionMu sync.Mutex
	var transitions []domain.Job
	t0 := time.Date(2026, 11, 16, 11, 0, 0, 0, time.UTC)
	svc := jobs.NewService(repo, jobs.ServiceOptions{
		Now: func() time.Time { return t0 },
		OnTransition: func(job domain.Job) {
			transitionMu.Lock()
			transitions = append(transitions, job)
			transitionMu.Unlock()
		},
	})

	alice := createServiceTestUser(ctx, t, db, "alice-jobs", domain.RoleReaderID)
	bob := createServiceTestUser(ctx, t, db, "bob-jobs", domain.RoleReaderID)
	ephemeralCreator := createServiceTestUser(ctx, t, db, "temp-creator", domain.RoleReaderID)
	adminUser := createServiceTestUser(ctx, t, db, "admin-jobs", domain.RoleAdminID)

	readerRole := mustBuiltInRole(t, domain.RoleReaderID)
	adminRole := mustBuiltInRole(t, domain.RoleAdminID)
	readAllOnlyRole := domain.Role{ID: "role-read-all", Name: "Job Auditor", Permissions: []domain.Permission{domain.PermissionJobsReadAll}}
	cancelAllOnlyRole := domain.Role{ID: "role-cancel-all", Name: "Blind Canceller", Permissions: []domain.Permission{domain.PermissionJobsCancelAll}}

	alicePass := domain.NewPrincipal(alice, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-alice", nil)
	aliceReadPAT := domain.NewPrincipal(alice, []domain.Role{readerRole}, domain.CredentialKindPAT, "c-alice-pat-read", []domain.PATScope{domain.ScopeRead})
	aliceAdminPAT := domain.NewPrincipal(alice, []domain.Role{readerRole}, domain.CredentialKindPAT, "c-alice-pat-admin", []domain.PATScope{domain.ScopeAdmin})
	bobPass := domain.NewPrincipal(bob, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-bob", nil)
	adminPass := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPassword, "c-admin", nil)
	adminLibPAT := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPAT, "c-admin-lib", []domain.PATScope{domain.ScopeLibraryAdmin})
	adminAdminPAT := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPAT, "c-admin-pat", []domain.PATScope{domain.ScopeAdmin})
	readAllOnly := domain.NewPrincipal(bob, []domain.Role{readAllOnlyRole}, domain.CredentialKindPassword, "c-bob-readall", nil)
	cancelAllOnly := domain.NewPrincipal(bob, []domain.Role{cancelAllOnlyRole}, domain.CredentialKindPassword, "c-bob-cancelall", nil)

	aliceQueuedID, err := svc.Trigger(ctx, jobs.TriggerRequest{Kind: domain.JobKindScan, CreatedBy: &alice.ID})
	if err != nil {
		t.Fatalf("Trigger aliceQueued: %v", err)
	}
	aliceRunningID, err := svc.Trigger(ctx, jobs.TriggerRequest{Kind: domain.JobKindRepack, CreatedBy: &alice.ID})
	if err != nil {
		t.Fatalf("Trigger aliceRunning: %v", err)
	}
	if _, err := repo.ClaimNext(ctx, domain.JobKindRepack, t0); err != nil {
		t.Fatalf("ClaimNext aliceRunning: %v", err)
	}
	aliceSucceededID, err := svc.Trigger(ctx, jobs.TriggerRequest{Kind: domain.JobKindScrape, CreatedBy: &alice.ID})
	if err != nil {
		t.Fatalf("Trigger aliceSucceeded: %v", err)
	}
	claimedSucc, err := repo.ClaimNext(ctx, domain.JobKindScrape, t0)
	if err != nil {
		t.Fatalf("ClaimNext aliceSucceeded: %v", err)
	}
	if _, err := repo.MarkSucceeded(ctx, aliceSucceededID, claimedSucc.Attempts, t0); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}

	autoQueuedID, err := svc.Trigger(ctx, jobs.TriggerRequest{Kind: domain.JobKindScan, CreatedBy: nil})
	if err != nil {
		t.Fatalf("Trigger autoQueued: %v", err)
	}
	deletedCreatorQueuedID, err := svc.Trigger(ctx, jobs.TriggerRequest{Kind: domain.JobKindWriteBack, CreatedBy: &ephemeralCreator.ID})
	if err != nil {
		t.Fatalf("Trigger deletedCreatorQueued: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM users WHERE id = $1`, ephemeralCreator.ID); err != nil {
		t.Fatalf("delete ephemeral creator: %v", err)
	}

	// 1. Unauthenticated callers fail closed with ErrUnauthorized.
	if _, err := svc.ListJobs(ctx, nil); !errors.Is(err, app.ErrUnauthorized) {
		t.Fatalf("ListJobs(nil) = %v, want ErrUnauthorized", err)
	}
	if _, err := svc.GetJob(ctx, nil, aliceQueuedID); !errors.Is(err, app.ErrUnauthorized) {
		t.Fatalf("GetJob(nil) = %v, want ErrUnauthorized", err)
	}
	if _, err := svc.CancelJob(ctx, nil, aliceQueuedID); !errors.Is(err, app.ErrUnauthorized) {
		t.Fatalf("CancelJob(nil) = %v, want ErrUnauthorized", err)
	}

	// 2. ListJobs visibility: creator sees own jobs only; jobs:read_all sees all (including nil/deleted creator).
	aliceList, err := svc.ListJobs(ctx, alicePass)
	if err != nil {
		t.Fatalf("ListJobs(alice): %v", err)
	}
	if len(aliceList) != 3 {
		t.Fatalf("alice ListJobs count = %d, want 3", len(aliceList))
	}
	bobList, err := svc.ListJobs(ctx, bobPass)
	if err != nil {
		t.Fatalf("ListJobs(bob): %v", err)
	}
	if len(bobList) != 0 {
		t.Fatalf("bob ListJobs count = %d, want 0", len(bobList))
	}
	adminList, err := svc.ListJobs(ctx, adminPass)
	if err != nil {
		t.Fatalf("ListJobs(admin): %v", err)
	}
	if len(adminList) != 5 {
		t.Fatalf("admin ListJobs count = %d, want 5", len(adminList))
	}

	// 3. GetJob visibility: absent and inaccessible IDs return identical ErrNotFound.
	for _, hiddenID := range []string{"", "nonexistent-job-id", aliceQueuedID, autoQueuedID, deletedCreatorQueuedID} {
		if _, err := svc.GetJob(ctx, bobPass, hiddenID); !errors.Is(err, app.ErrNotFound) {
			t.Fatalf("bob GetJob(%q) = %v, want ErrNotFound", hiddenID, err)
		}
		if _, err := svc.GetJob(ctx, adminLibPAT, hiddenID); !errors.Is(err, app.ErrNotFound) {
			t.Fatalf("admin library:admin PAT GetJob(%q) = %v, want ErrNotFound", hiddenID, err)
		}
		if _, err := svc.GetJob(ctx, cancelAllOnly, hiddenID); !errors.Is(err, app.ErrNotFound) {
			t.Fatalf("cancelAllOnly GetJob(%q) = %v, want ErrNotFound", hiddenID, err)
		}
	}

	gotCreator, err := svc.GetJob(ctx, alicePass, aliceQueuedID)
	if err != nil || gotCreator.ID != aliceQueuedID {
		t.Fatalf("alice GetJob(own) = (%+v, %v), want %q", gotCreator, err, aliceQueuedID)
	}
	gotAuto, err := svc.GetJob(ctx, readAllOnly, autoQueuedID)
	if err != nil || gotAuto.CreatedBy != nil {
		t.Fatalf("readAllOnly GetJob(auto) = (%+v, %v), want nil CreatedBy", gotAuto, err)
	}
	gotDeletedCreator, err := svc.GetJob(ctx, adminAdminPAT, deletedCreatorQueuedID)
	if err != nil || gotDeletedCreator.CreatedBy != nil {
		t.Fatalf("admin GetJob(deletedCreator) = (%+v, %v), want nil CreatedBy", gotDeletedCreator, err)
	}

	// 4. CancelJob visibility + I-10 authorization + terminal conflict rules.
	// 4a. Invisible jobs return ErrNotFound (even if terminal or holding jobs:cancel_all without jobs:read_all).
	for _, hiddenID := range []string{"", "nonexistent-job-id", aliceQueuedID, aliceSucceededID, autoQueuedID, deletedCreatorQueuedID} {
		if _, err := svc.CancelJob(ctx, bobPass, hiddenID); !errors.Is(err, app.ErrNotFound) {
			t.Fatalf("bob CancelJob(%q) = %v, want ErrNotFound", hiddenID, err)
		}
		if _, err := svc.CancelJob(ctx, cancelAllOnly, hiddenID); !errors.Is(err, app.ErrNotFound) {
			t.Fatalf("cancelAllOnly CancelJob(%q) = %v, want ErrNotFound", hiddenID, err)
		}
	}

	// 4b. Visible jobs without cancellation authority return ErrForbidden (even on terminal jobs).
	for _, id := range []string{aliceQueuedID, aliceRunningID, aliceSucceededID} {
		if _, err := svc.CancelJob(ctx, aliceReadPAT, id); !errors.Is(err, app.ErrForbidden) {
			t.Fatalf("alice read-PAT CancelJob(%q) = %v, want ErrForbidden", id, err)
		}
	}
	for _, id := range []string{aliceQueuedID, aliceRunningID, aliceSucceededID, autoQueuedID, deletedCreatorQueuedID} {
		if _, err := svc.CancelJob(ctx, readAllOnly, id); !errors.Is(err, app.ErrForbidden) {
			t.Fatalf("readAllOnly CancelJob(%q) = %v, want ErrForbidden", id, err)
		}
	}

	// 4c. Creator with jobs:cancel_own (admin PAT capped to Reader) cancels own QUEUED job -> CANCELLED immediately.
	cancelledQueued, err := svc.CancelJob(ctx, aliceAdminPAT, aliceQueuedID)
	if err != nil {
		t.Fatalf("alice admin-PAT CancelJob(queued): %v", err)
	}
	if cancelledQueued.State != string(domain.JobStateCancelled) || !cancelledQueued.CancelRequested || cancelledQueued.FinishedAt == nil {
		t.Fatalf("unexpected queued cancel snapshot: %+v", cancelledQueued)
	}

	// 4d. Creator cancels own RUNNING job -> remains RUNNING with cancel_requested=true.
	cancelledRunning, err := svc.CancelJob(ctx, alicePass, aliceRunningID)
	if err != nil {
		t.Fatalf("alice CancelJob(running): %v", err)
	}
	if cancelledRunning.State != string(domain.JobStateRunning) || !cancelledRunning.CancelRequested || cancelledRunning.FinishedAt != nil {
		t.Fatalf("unexpected running cancel snapshot: %+v", cancelledRunning)
	}

	// 4e. Authorized caller cancelling terminal job -> ErrConflict.
	if _, err := svc.CancelJob(ctx, alicePass, aliceSucceededID); !errors.Is(err, app.ErrConflict) {
		t.Fatalf("CancelJob(succeeded) = %v, want ErrConflict", err)
	}
	if _, err := svc.CancelJob(ctx, adminPass, aliceQueuedID); !errors.Is(err, app.ErrConflict) {
		t.Fatalf("CancelJob(already cancelled) = %v, want ErrConflict", err)
	}

	// 4f. Admin with jobs:read_all + jobs:cancel_all cancels automatic and creator-deleted jobs.
	cancelledAuto, err := svc.CancelJob(ctx, adminAdminPAT, autoQueuedID)
	if err != nil {
		t.Fatalf("admin CancelJob(auto): %v", err)
	}
	if cancelledAuto.State != string(domain.JobStateCancelled) || !cancelledAuto.CancelRequested {
		t.Fatalf("unexpected auto cancel snapshot: %+v", cancelledAuto)
	}
	cancelledDeleted, err := svc.CancelJob(ctx, adminPass, deletedCreatorQueuedID)
	if err != nil {
		t.Fatalf("admin CancelJob(deletedCreator): %v", err)
	}
	if cancelledDeleted.State != string(domain.JobStateCancelled) || !cancelledDeleted.CancelRequested {
		t.Fatalf("unexpected deleted-creator cancel snapshot: %+v", cancelledDeleted)
	}

	transitionMu.Lock()
	gotTransitions := len(transitions)
	transitionMu.Unlock()
	if gotTransitions != 3 {
		t.Fatalf("OnTransition fired %d times for 3 immediate QUEUED cancellations, want 3", gotTransitions)
	}
}
