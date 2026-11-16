package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func TestJobValidationAndHelpers(t *testing.T) {
	now := time.Now().UTC()
	creatorID := "user-creator"
	otherID := "user-other"
	dedupe := "SCAN:lib-1"

	job := Job{
		ID:          "job-1",
		Kind:        JobKindScan,
		State:       JobStateQueued,
		Payload:     json.RawMessage(`{"library_id":"lib-1"}`),
		Checkpoint:  json.RawMessage(`{}`),
		Attempts:    0,
		MaxAttempts: 3,
		CreatedBy:   &creatorID,
		DedupeKey:   &dedupe,
		RunAfter:    now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := job.Validate(); err != nil {
		t.Fatalf("Job.Validate() unexpected error: %v", err)
	}
	if !job.IsActive() || job.IsTerminal() {
		t.Error("expected QUEUED job to be active and non-terminal")
	}
	if !job.CanRetry() {
		t.Error("expected CanRetry() == true when Attempts < MaxAttempts")
	}
	if job.Kind.IsInternal() {
		t.Error("expected SCAN job not to be internal")
	}
	if !JobKindEventDelivery.IsInternal() {
		t.Error("expected EVENT_DELIVERY to be internal")
	}

	// Visibility rules (creator or jobs:read_all; automatic jobs admin-only).
	creator := NewPrincipal(User{ID: creatorID, Username: "alice"}, []Role{mustBuiltInRole(t, RoleReaderID)}, CredentialKindPassword, "c1", nil)
	other := NewPrincipal(User{ID: otherID, Username: "bob"}, []Role{mustBuiltInRole(t, RoleReaderID)}, CredentialKindPassword, "c2", nil)
	admin := NewPrincipal(User{ID: otherID, Username: "admin"}, []Role{mustBuiltInRole(t, RoleAdminID)}, CredentialKindPassword, "c3", nil)

	if job.VisibleTo(nil) {
		t.Error("nil principal must not see any job")
	}
	if !job.VisibleTo(creator) {
		t.Error("creator must see own job")
	}
	if job.VisibleTo(other) {
		t.Error("non-creator without jobs:read_all must not see job")
	}
	if !job.VisibleTo(admin) {
		t.Error("admin with jobs:read_all must see job")
	}

	autoJob := job
	autoJob.CreatedBy = nil
	if autoJob.VisibleTo(creator) {
		t.Error("automatic job with nil CreatedBy must not be visible without jobs:read_all")
	}
	if !autoJob.VisibleTo(admin) {
		t.Error("automatic job must be visible to jobs:read_all")
	}

	// Cancellation authority rules (decision I-10).
	if job.CanCancel(nil) {
		t.Error("nil principal must not be able to cancel any job")
	}
	if !job.CanCancel(creator) {
		t.Error("creator with built-in Reader role (jobs:cancel_own) must be able to cancel own job")
	}
	creatorReadPAT := NewPrincipal(User{ID: creatorID, Username: "alice"}, []Role{mustBuiltInRole(t, RoleReaderID)}, CredentialKindPAT, "pat-read", []PATScope{ScopeRead})
	if job.CanCancel(creatorReadPAT) {
		t.Error("creator with read PAT lacks jobs:cancel_own and must not cancel own job")
	}
	creatorAdminPAT := NewPrincipal(User{ID: creatorID, Username: "alice"}, []Role{mustBuiltInRole(t, RoleReaderID)}, CredentialKindPAT, "pat-admin", []PATScope{ScopeAdmin})
	if !job.CanCancel(creatorAdminPAT) {
		t.Error("creator with admin PAT capped to Reader role keeps jobs:cancel_own and can cancel own job")
	}
	if job.CanCancel(other) {
		t.Error("other user without jobs:read_all and jobs:cancel_all must not cancel another user's job")
	}
	readAllOnly := NewPrincipal(User{ID: otherID, Username: "auditor"}, []Role{{ID: "r-readall", Name: "Auditor", Permissions: []Permission{PermissionJobsReadAll}}}, CredentialKindPassword, "c4", nil)
	if job.CanCancel(readAllOnly) {
		t.Error("principal with jobs:read_all only (without jobs:cancel_all) must not cancel another user's job")
	}
	cancelAllOnly := NewPrincipal(User{ID: otherID, Username: "canceller"}, []Role{{ID: "r-cancelall", Name: "Canceller", Permissions: []Permission{PermissionJobsCancelAll}}}, CredentialKindPassword, "c5", nil)
	if job.CanCancel(cancelAllOnly) {
		t.Error("principal with jobs:cancel_all without jobs:read_all must not cancel another user's job")
	}
	if !job.CanCancel(admin) {
		t.Error("admin with jobs:read_all and jobs:cancel_all must be able to cancel another user's job")
	}
	if autoJob.CanCancel(creator) {
		t.Error("non-admin must not cancel automatic job")
	}
	if autoJob.CanCancel(readAllOnly) {
		t.Error("jobs:read_all without jobs:cancel_all must not cancel automatic job")
	}
	if !autoJob.CanCancel(admin) {
		t.Error("admin with jobs:read_all and jobs:cancel_all must be able to cancel automatic job")
	}

	badState := job
	badState.State = JobState("BOGUS")
	if !errors.Is(badState.Validate(), ErrInvalidJobState) {
		t.Fatalf("expected ErrInvalidJobState, got %v", badState.Validate())
	}

	// State transitions.
	if !JobStateQueued.CanTransitionTo(JobStateRunning) || !JobStateQueued.CanTransitionTo(JobStateCancelled) {
		t.Error("QUEUED must allow transition to RUNNING and CANCELLED")
	}
	if JobStateQueued.CanTransitionTo(JobStateSucceeded) || JobStateQueued.CanTransitionTo(JobStateFailed) {
		t.Error("QUEUED must not transition directly to SUCCEEDED or FAILED")
	}
	for _, next := range []JobState{JobStateQueued, JobStateSucceeded, JobStateFailed, JobStateCancelled} {
		if !JobStateRunning.CanTransitionTo(next) {
			t.Errorf("RUNNING must allow transition to %s", next)
		}
	}
	for _, term := range []JobState{JobStateSucceeded, JobStateFailed, JobStateCancelled} {
		for _, next := range ValidJobStates() {
			if term.CanTransitionTo(next) {
				t.Errorf("terminal state %s must not transition to %s", term, next)
			}
		}
	}

	// 32-bit PostgreSQL INT bounds.
	overflowMax := job
	overflowMax.MaxAttempts = math.MaxInt32 + 1
	if !errors.Is(overflowMax.Validate(), ErrInvalidJobAttempts) {
		t.Fatalf("expected ErrInvalidJobAttempts for max_attempts > MaxInt32, got %v", overflowMax.Validate())
	}
	overflowAttempts := job
	overflowAttempts.Attempts = math.MaxInt32 + 1
	if !errors.Is(overflowAttempts.Validate(), ErrInvalidJobAttempts) {
		t.Fatalf("expected ErrInvalidJobAttempts for attempts > MaxInt32, got %v", overflowAttempts.Validate())
	}

	// Exponential retry backoff with cap.
	if got := JobRetryBackoff(1); got != 5*time.Second {
		t.Errorf("JobRetryBackoff(1) = %v, want 5s", got)
	}
	if got := JobRetryBackoff(2); got != 10*time.Second {
		t.Errorf("JobRetryBackoff(2) = %v, want 10s", got)
	}
	if got := JobRetryBackoff(100); got != 5*time.Minute {
		t.Errorf("JobRetryBackoff(100) = %v, want 5m cap", got)
	}

	if !JobResumeRestart.IsValid() || !JobResumeCheckpoint.IsValid() {
		t.Error("expected both JobResumeRestart and JobResumeCheckpoint to be valid")
	}
	if JobResumePolicy("").IsValid() || JobResumePolicy("UNKNOWN").IsValid() {
		t.Error("expected empty or unknown JobResumePolicy to be invalid")
	}
}

func TestDomainEventAndSubscriber(t *testing.T) {
	now := time.Now().UTC()
	seq := int64(42)

	unpub := DomainEvent{
		ID:            99,
		Kind:          EventJobStarted,
		Version:       1,
		AggregateType: "job",
		AggregateID:   "job-1",
		Payload:       json.RawMessage(`{"job_id":"job-1"}`),
		CreatedAt:     now,
	}
	if err := unpub.Validate(); err != nil {
		t.Fatalf("unpublished event Validate() error: %v", err)
	}
	if unpub.IsPublished() {
		t.Error("expected unpublished event IsPublished() == false")
	}
	if _, ok := unpub.Cursor(); ok {
		t.Error("unpublished event must not return raw ID as Cursor")
	}

	pub := unpub
	pub.PublishedSequence = &seq
	pub.PublishedAt = &now
	if err := pub.Validate(); err != nil {
		t.Fatalf("published event Validate() error: %v", err)
	}
	if !pub.IsPublished() {
		t.Error("expected published event IsPublished() == true")
	}
	cursor, ok := pub.Cursor()
	if !ok || cursor != 42 {
		t.Fatalf("Cursor() = (%d, %v), want (42, true)", cursor, ok)
	}

	halfPublished := unpub
	halfPublished.PublishedSequence = &seq
	if !errors.Is(halfPublished.Validate(), ErrInvalidPublicationPair) {
		t.Fatalf("expected ErrInvalidPublicationPair, got %v", halfPublished.Validate())
	}

	sub := EventSubscriber{
		ID:        "notify:shoutrrr",
		Kind:      "shoutrrr",
		Position:  42,
		State:     SubscriberStateActive,
		UpdatedAt: now,
	}
	if err := sub.Validate(); err != nil {
		t.Fatalf("EventSubscriber.Validate() error: %v", err)
	}

	badStalled := sub
	badStalled.State = SubscriberStateStalled
	if !errors.Is(badStalled.Validate(), ErrInvalidStalledPair) {
		t.Fatalf("expected ErrInvalidStalledPair, got %v", badStalled.Validate())
	}

	negPos := sub
	negPos.Position = -1
	if !errors.Is(negPos.Validate(), ErrInvalidSubscriberPosition) {
		t.Fatalf("expected ErrInvalidSubscriberPosition, got %v", negPos.Validate())
	}
}

func mustBuiltInRole(t *testing.T, id string) Role {
	t.Helper()
	r, ok := BuiltInRoleByID(id)
	if !ok {
		t.Fatalf("missing built-in role %q", id)
	}
	return r
}
