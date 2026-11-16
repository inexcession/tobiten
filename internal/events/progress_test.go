package events_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
)

func testPrincipalWithPerms(userID string, perms ...domain.Permission) *domain.Principal {
	return &domain.Principal{
		User: domain.User{
			ID:       userID,
			Username: userID,
		},
		Permissions: perms,
	}
}

func TestProgressHub_ValidationRateLimitVisibilityAndCleanup(t *testing.T) {
	if _, err := events.NewProgressHub(events.ProgressHubOptions{MinInterval: -time.Millisecond}); !errors.Is(err, events.ErrInvalidProgressRate) {
		t.Fatalf("negative MinInterval err = %v, want ErrInvalidProgressRate", err)
	}

	var nilHub *events.ProgressHub
	if _, _, err := nilHub.Subscribe(context.Background(), events.ProgressSubscribeOptions{}); !errors.Is(err, events.ErrNilProgressHub) {
		t.Fatalf("nil hub Subscribe err = %v, want ErrNilProgressHub", err)
	}
	if nilHub.Publish(domain.JobProgressTick{JobID: "j1", Kind: domain.JobKindScan}) {
		t.Fatal("nil hub Publish must return false")
	}

	t0 := time.Date(2026, 11, 16, 18, 0, 0, 0, time.UTC)
	now := t0
	var clockMu sync.Mutex
	hub, err := events.NewProgressHub(events.ProgressHubOptions{
		MinInterval: 100 * time.Millisecond,
		Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return now
		},
	})
	if err != nil {
		t.Fatalf("NewProgressHub: %v", err)
	}

	creatorID := "user-creator"
	otherID := "user-other"
	creator := testPrincipalWithPerms(creatorID, domain.PermissionLibraryRead)
	other := testPrincipalWithPerms(otherID, domain.PermissionLibraryRead)
	admin := testPrincipalWithPerms("user-admin", domain.PermissionLibraryRead, domain.PermissionJobsReadAll)

	if _, _, err := hub.Subscribe(context.Background(), events.ProgressSubscribeOptions{}); !errors.Is(err, events.ErrPrincipalRequired) {
		t.Fatalf("missing principal err = %v, want ErrPrincipalRequired", err)
	}
	if _, _, err := hub.Subscribe(context.Background(), events.ProgressSubscribeOptions{
		Principal:  creator,
		BufferSize: -1,
	}); !errors.Is(err, events.ErrInvalidProgressBuffer) {
		t.Fatalf("negative buffer err = %v, want ErrInvalidProgressBuffer", err)
	}

	ctxCreator, cancelCreator := context.WithCancel(context.Background())
	defer cancelCreator()
	chCreator, unsubCreator, err := hub.Subscribe(ctxCreator, events.ProgressSubscribeOptions{
		Principal: creator,
	})
	if err != nil {
		t.Fatalf("Subscribe creator: %v", err)
	}
	defer unsubCreator()

	chOther, unsubOther, err := hub.Subscribe(context.Background(), events.ProgressSubscribeOptions{
		Principal: other,
	})
	if err != nil {
		t.Fatalf("Subscribe other: %v", err)
	}
	defer unsubOther()

	chAdmin, unsubAdmin, err := hub.Subscribe(context.Background(), events.ProgressSubscribeOptions{
		Principal: admin,
	})
	if err != nil {
		t.Fatalf("Subscribe admin: %v", err)
	}
	defer unsubAdmin()

	chJobOnly, unsubJobOnly, err := hub.Subscribe(context.Background(), events.ProgressSubscribeOptions{
		Principal: admin,
		JobID:     "job-other",
	})
	if err != nil {
		t.Fatalf("Subscribe filtered job: %v", err)
	}
	defer unsubJobOnly()

	if hub.SubscriberCount() != 4 {
		t.Fatalf("SubscriberCount = %d, want 4", hub.SubscriberCount())
	}

	// Internal EVENT_DELIVERY ticks are suppressed.
	if hub.Publish(domain.JobProgressTick{
		JobID:        "deliv-1",
		Kind:         domain.JobKindEventDelivery,
		ScannedCount: 1,
		DurationMS:   5,
	}) {
		t.Fatal("expected EVENT_DELIVERY progress tick to be suppressed")
	}

	// First tick at t0 delivers to creator and admin, not other or job-other filter.
	if !hub.Publish(domain.JobProgressTick{
		JobID:        "job-1",
		Kind:         domain.JobKindScan,
		CreatedBy:    &creatorID,
		ScannedCount: 1,
		CurrentPath:  "/lib/a.cbz",
		DurationMS:   10,
	}) {
		t.Fatal("expected initial tick at t0 to publish")
	}

	// Rapid tick 50ms later is rate-limited.
	clockMu.Lock()
	now = t0.Add(50 * time.Millisecond)
	clockMu.Unlock()
	if hub.Publish(domain.JobProgressTick{
		JobID:        "job-1",
		Kind:         domain.JobKindScan,
		CreatedBy:    &creatorID,
		ScannedCount: 2,
		CurrentPath:  "/lib/b.cbz",
		DurationMS:   60,
	}) {
		t.Fatal("expected tick within MinInterval to be rate-limited")
	}

	// Tick at t0+100ms passes rate limit.
	clockMu.Lock()
	now = t0.Add(100 * time.Millisecond)
	clockMu.Unlock()
	if !hub.Publish(domain.JobProgressTick{
		JobID:        "job-1",
		Kind:         domain.JobKindScan,
		CreatedBy:    &creatorID,
		ScannedCount: 3,
		CurrentPath:  "/lib/c.cbz",
		DurationMS:   110,
	}) {
		t.Fatal("expected tick at MinInterval boundary to publish")
	}

	// Automatic job (nil CreatedBy) is visible to admin only.
	if !hub.Publish(domain.JobProgressTick{
		JobID:        "job-auto",
		Kind:         domain.JobKindScan,
		CreatedBy:    nil,
		ScannedCount: 7,
		CurrentPath:  "/lib/auto.cbz",
		DurationMS:   20,
	}) {
		t.Fatal("expected automatic job tick to publish")
	}

	// Creator receives the two allowed job-1 ticks only.
	for i, wantCount := range []int64{1, 3} {
		select {
		case got := <-chCreator:
			if got.JobID != "job-1" || got.ScannedCount != wantCount {
				t.Fatalf("creator tick %d = %+v, want job-1 count=%d", i, got, wantCount)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for creator tick %d", i)
		}
	}
	select {
	case unexpected := <-chCreator:
		t.Fatalf("creator received unexpected tick: %+v", unexpected)
	default:
	}

	// Non-creator receives nothing.
	select {
	case unexpected := <-chOther:
		t.Fatalf("non-creator received unexpected tick: %+v", unexpected)
	default:
	}

	// Admin receives all three published ticks; job-other listener receives nothing.
	for i, wantID := range []string{"job-1", "job-1", "job-auto"} {
		select {
		case got := <-chAdmin:
			if got.JobID != wantID {
				t.Fatalf("admin tick %d = %+v, want JobID=%s", i, got, wantID)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for admin tick %d", i)
		}
	}
	select {
	case unexpected := <-chJobOnly:
		t.Fatalf("job-filtered listener received unexpected tick: %+v", unexpected)
	default:
	}

	// Context cancellation unsubscribes and closes channel cleanly.
	cancelCreator()
	select {
	case _, open := <-chCreator:
		if open {
			t.Fatal("expected creator channel to close after context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for creator channel close")
	}

	// ClearJob removes rate-limit state so a subsequent tick for same ID publishes immediately.
	hub.ClearJob("job-1")
	if !hub.Publish(domain.JobProgressTick{
		JobID:        "job-1",
		Kind:         domain.JobKindScan,
		CreatedBy:    &creatorID,
		ScannedCount: 4,
		DurationMS:   120,
	}) {
		t.Fatal("expected tick after ClearJob to publish immediately")
	}
	<-chAdmin

	// Slow subscriber with buffer 1 never blocks concurrent Publish calls.
	chSlow, unsubSlow, err := hub.Subscribe(context.Background(), events.ProgressSubscribeOptions{
		Principal:  admin,
		BufferSize: 1,
	})
	if err != nil {
		t.Fatalf("Subscribe slow: %v", err)
	}
	defer unsubSlow()

	for i := range 5 {
		hub.ClearJob("job-burst")
		hub.Publish(domain.JobProgressTick{
			JobID:        "job-burst",
			Kind:         domain.JobKindScan,
			ScannedCount: int64(i + 1),
			DurationMS:   int64(i),
		})
	}
	if len(chSlow) != 1 {
		t.Fatalf("slow buffer len = %d, want 1", len(chSlow))
	}
}
