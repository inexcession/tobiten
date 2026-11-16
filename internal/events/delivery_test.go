package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

func appendPublishedCatalogEvents(
	ctx context.Context,
	t *testing.T,
	tx *postgres.Transactor,
	eventRepo *postgres.EventRepository,
	now time.Time,
	aggregateIDs ...string,
) []domain.DomainEvent {
	t.Helper()
	outbox := events.NewOutbox(events.WithClock(func() time.Time { return now }))
	if err := tx.WithTx(ctx, func(stx storage.Tx) error {
		for _, id := range aggregateIDs {
			if _, err := outbox.AppendPayload(ctx, stx.Events(),
				domain.EventAssetAdded, domain.EventVersion1, domain.AggregateTypeAsset, id,
				domain.AssetEventPayloadV1{
					AssetID:       id,
					VolumeID:      "vol-" + id,
					LibraryID:     "lib-1",
					LibraryRootID: "root-1",
				}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("append events %v: %v", aggregateIDs, err)
	}
	published, err := eventRepo.PublishUnsequencedEvents(ctx, now)
	if err != nil {
		t.Fatalf("PublishUnsequencedEvents: %v", err)
	}
	return published
}

func waitSubscriberPosition(ctx context.Context, t *testing.T, repo storage.EventRepository, id string, wantPos int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			t.Fatalf("context cancelled waiting for subscriber %q position %d: %v", id, wantPos, ctx.Err())
		}
		sub, err := repo.GetEventSubscriber(ctx, id)
		if err == nil && sub.Position == wantPos {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	sub, err := repo.GetEventSubscriber(ctx, id)
	t.Fatalf("subscriber %q = (%+v, %v), want position %d", id, sub, err, wantPos)
}

func countJobLifecycleEvents(ctx context.Context, t *testing.T, db *testdb.TestDB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE kind LIKE 'job.%'`).Scan(&count); err != nil {
		t.Fatalf("counting job.* domain_events: %v", err)
	}
	return count
}

func TestDeliveryCoordinator_UnitValidationAndModuleRegistration(t *testing.T) {
	ctx := context.Background()

	var nilCoord *events.DeliveryCoordinator
	if _, err := nilCoord.DrainOnce(ctx); !errors.Is(err, events.ErrNilDeliveryDependencies) {
		t.Fatalf("nil DrainOnce err = %v, want ErrNilDeliveryDependencies", err)
	}
	if err := nilCoord.Start(ctx); !errors.Is(err, events.ErrNilDeliveryDependencies) {
		t.Fatalf("nil Start err = %v, want ErrNilDeliveryDependencies", err)
	}

	d := events.NewDeliveryCoordinator(nil, nil, nil, events.DeliveryOptions{})
	if err := d.RegisterSubscriber(nil); !errors.Is(err, events.ErrInvalidSubscriber) {
		t.Fatalf("nil subscriber err = %v, want ErrInvalidSubscriber", err)
	}
	if err := d.RegisterSubscriber(events.SubscriberSpec{SubscriberID: " ", SubscriberKind: "k"}); !errors.Is(err, events.ErrInvalidSubscriber) {
		t.Fatalf("empty ID err = %v, want ErrInvalidSubscriber", err)
	}
	if err := d.RegisterSubscriber(events.SubscriberSpec{SubscriberID: "sub-1", SubscriberKind: " "}); !errors.Is(err, events.ErrInvalidSubscriber) {
		t.Fatalf("empty kind err = %v, want ErrInvalidSubscriber", err)
	}
	if err := d.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub-1",
		SubscriberKind: "webhook",
		DeliverFn: func(context.Context, domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			return domain.DeliveryAcknowledged, nil
		},
	}); err != nil {
		t.Fatalf("RegisterSubscriber: %v", err)
	}
	if err := d.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub-1",
		SubscriberKind: "webhook",
		DeliverFn: func(context.Context, domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			return domain.DeliveryAcknowledged, nil
		},
	}); !errors.Is(err, events.ErrDuplicateSubscriber) {
		t.Fatalf("duplicate RegisterSubscriber err = %v, want ErrDuplicateSubscriber", err)
	}

	reg := module.NewRegistry()
	_ = reg.Register(&module.SimpleModule{
		ModuleName: "mod-subs",
		Subscribers: []module.EventSubscriber{
			module.EventSubscriberSpec{
				ID:      "notify:shoutrrr",
				SubKind: "shoutrrr",
				DeliverFunc: func(context.Context, domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
					return domain.DeliveryAcknowledged, nil
				},
			},
			module.NamedEventSubscriber("named-only"),
		},
	})
	if err := d.RegisterModuleSubscribers(reg); err != nil {
		t.Fatalf("RegisterModuleSubscribers: %v", err)
	}
	// Attaching a handler later for a module-declared NamedEventSubscriber succeeds when kind matches.
	if err := d.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "named-only",
		SubscriberKind: "named-only",
		DeliverFn: func(context.Context, domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			return domain.DeliverySkipped, nil
		},
	}); err != nil {
		t.Fatalf("attaching handler to NamedEventSubscriber: %v", err)
	}
}

func TestDelivery_FailingEventBlocksLaterEventsUntilRetrySucceedsAndSuppressesLifecycle(t *testing.T) {
	t.Parallel()
	db, tx, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var mu sync.Mutex
	currentTime := time.Date(2026, 11, 16, 17, 0, 0, 0, time.UTC)
	nowFn := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return currentTime
	}
	advanceTime := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		currentTime = currentTime.Add(d)
	}

	// Publish 3 events in publication-sequence order.
	appendPublishedCatalogEvents(ctx, t, tx, eventRepo, nowFn(), "asset-1", "asset-2", "asset-3")

	var deliveredIDs []string
	var seq1Attempts atomic.Int32

	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{
		Now: nowFn,
	})
	delivery := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner, events.DeliveryOptions{
		Now:         nowFn,
		MaxAttempts: 3,
	})
	if err := delivery.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:blocking",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			if *ev.PublishedSequence == 1 {
				attempt := seq1Attempts.Add(1)
				if attempt < 3 {
					return 0, fmt.Errorf("downstream 503 on attempt %d", attempt)
				}
			}
			mu.Lock()
			deliveredIDs = append(deliveredIDs, ev.AggregateID)
			mu.Unlock()
			return domain.DeliveryAcknowledged, nil
		},
	}); err != nil {
		t.Fatalf("RegisterSubscriber: %v", err)
	}
	if err := delivery.RegisterJobs(runner); err != nil {
		t.Fatalf("RegisterJobs: %v", err)
	}
	if err := delivery.SyncSubscribers(ctx); err != nil {
		t.Fatalf("SyncSubscribers: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Drain #1 enqueues seq=1 only.
	n, err := delivery.DrainOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("DrainOnce #1 = (%d, %v), want (1, nil)", n, err)
	}

	// Attempt 1 fails -> cursor stays at 0, job is QUEUED for backoff retry, and seq=2 is NOT enqueued.
	claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery)
	if err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext attempt 1 = (%v, %v), want (true, nil)", claimed, err)
	}
	subAfter1, err := eventRepo.GetEventSubscriber(ctx, "sub:blocking")
	if err != nil || subAfter1.Position != 0 {
		t.Fatalf("after attempt 1 failure: sub = (%+v, %v), want position 0", subAfter1, err)
	}
	// Repeating DrainOnce while seq=1 is waiting for retry must deduplicate and enqueue nothing new.
	if nAgain, err := delivery.DrainOnce(ctx); err != nil || nAgain != 0 {
		t.Fatalf("DrainOnce while retry queued = (%d, %v), want (0, nil)", nAgain, err)
	}
	activeKind := domain.JobKindEventDelivery
	queuedJobs, err := jobRepo.ListJobs(ctx, storage.JobFilter{Kind: &activeKind})
	if err != nil || len(queuedJobs) != 1 || queuedJobs[0].State != domain.JobStateQueued || queuedJobs[0].Attempts != 1 {
		t.Fatalf("expected 1 QUEUED retry job at attempts=1, got %+v (err=%v)", queuedJobs, err)
	}
	firstJob := queuedJobs[0]

	// Advance past 1st backoff (5s): Attempt 2 fails -> cursor STILL at 0, seq=2 blocked.
	advanceTime(6 * time.Second)
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext attempt 2 = (%v, %v), want (true, nil)", claimed, err)
	}
	subAfter2, err := eventRepo.GetEventSubscriber(ctx, "sub:blocking")
	if err != nil || subAfter2.Position != 0 {
		t.Fatalf("after attempt 2 failure: sub = (%+v, %v), want position 0", subAfter2, err)
	}
	if nAgain, err := delivery.DrainOnce(ctx); err != nil || nAgain != 0 {
		t.Fatalf("DrainOnce after attempt 2 = (%d, %v), want (0, nil)", nAgain, err)
	}

	// Advance past 2nd backoff (10s): Attempt 3 succeeds -> cursor advances to 1.
	advanceTime(11 * time.Second)
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext attempt 3 = (%v, %v), want (true, nil)", claimed, err)
	}
	subAfter3, err := eventRepo.GetEventSubscriber(ctx, "sub:blocking")
	if err != nil || subAfter3.Position != 1 {
		t.Fatalf("after attempt 3 success: sub = (%+v, %v), want position 1", subAfter3, err)
	}
	firstSucceededJob, err := jobRepo.GetJobByID(ctx, firstJob.ID)
	if err != nil || firstSucceededJob.State != domain.JobStateSucceeded {
		t.Fatalf("first job = (%+v, %v), want SUCCEEDED", firstSucceededJob, err)
	}

	// Now DrainOnce + execution delivers seq=2, then seq=3 in order.
	for _, wantSeq := range []int64{2, 3} {
		n, err := delivery.DrainOnce(ctx)
		if err != nil || n != 1 {
			t.Fatalf("DrainOnce seq %d = (%d, %v), want (1, nil)", wantSeq, n, err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext seq %d = (%v, %v), want (true, nil)", wantSeq, claimed, err)
		}
		sub, err := eventRepo.GetEventSubscriber(ctx, "sub:blocking")
		if err != nil || sub.Position != wantSeq {
			t.Fatalf("sub after seq %d = (%+v, %v), want position %d", wantSeq, sub, err, wantSeq)
		}
	}

	wantOrder := []string{"asset-1", "asset-2", "asset-3"}
	if !slices.Equal(deliveredIDs, wantOrder) {
		t.Fatalf("delivered order = %v, want %v", deliveredIDs, wantOrder)
	}

	// Verify zero job.* events were produced in domain_events during EVENT_DELIVERY execution,
	// and verify lifecycle emission against the actual persisted EVENT_DELIVERY job rows is suppressed.
	if jobEvCount := countJobLifecycleEvents(ctx, t, db); jobEvCount != 0 {
		t.Fatalf("expected 0 job.* domain_events from EVENT_DELIVERY execution, got %d", jobEvCount)
	}
	outbox := events.NewOutbox(events.WithClock(nowFn))
	err = eventRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		runningSnapshot := *firstSucceededJob
		runningSnapshot.State = domain.JobStateRunning
		if _, err := outbox.AppendJobLifecycle(ctx, txRepo, domain.EventJobStarted, runningSnapshot); !errors.Is(err, domain.ErrInternalJobEventSuppressed) {
			return fmt.Errorf("expected ErrInternalJobEventSuppressed for RUNNING EVENT_DELIVERY, got %v", err)
		}
		if _, err := outbox.AppendJobLifecycle(ctx, txRepo, domain.EventJobFinished, *firstSucceededJob); !errors.Is(err, domain.ErrInternalJobEventSuppressed) {
			return fmt.Errorf("expected ErrInternalJobEventSuppressed for SUCCEEDED EVENT_DELIVERY, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("lifecycle suppression verification failed: %v", err)
	}
	if jobEvCount := countJobLifecycleEvents(ctx, t, db); jobEvCount != 0 {
		t.Fatalf("expected 0 job.* domain_events after lifecycle suppression check, got %d", jobEvCount)
	}
}

func TestDelivery_PermanentFailureBlocksWithoutSpinUntilReset(t *testing.T) {
	t.Parallel()
	db, tx, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 17, 10, 0, 0, time.UTC)
	now := t0
	nowFn := func() time.Time { return now }

	appendPublishedCatalogEvents(ctx, t, tx, eventRepo, t0, "perm-1", "perm-2")

	var failSeq1 atomic.Bool
	failSeq1.Store(true)
	var delivered []string

	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{Now: nowFn})
	delivery := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner, events.DeliveryOptions{
		Now:         nowFn,
		MaxAttempts: 1,
	})
	_ = delivery.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:perm",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			if *ev.PublishedSequence == 1 && failSeq1.Load() {
				return 0, errors.New("400 permanent error")
			}
			delivered = append(delivered, ev.AggregateID)
			return domain.DeliveryAcknowledged, nil
		},
	})
	_ = delivery.RegisterJobs(runner)
	if err := delivery.SyncSubscribers(ctx); err != nil {
		t.Fatalf("SyncSubscribers: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if n, err := delivery.DrainOnce(ctx); err != nil || n != 1 {
		t.Fatalf("DrainOnce = (%d, %v), want (1, nil)", n, err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext = (%v, %v)", claimed, err)
	}

	// Job is FAILED, cursor stays at 0, and subsequent DrainOnce calls must NOT spin or skip to seq=2.
	sub, err := eventRepo.GetEventSubscriber(ctx, "sub:perm")
	if err != nil || sub.Position != 0 {
		t.Fatalf("sub = (%+v, %v), want position 0", sub, err)
	}
	for range 3 {
		if n, err := delivery.DrainOnce(ctx); err != nil || n != 0 {
			t.Fatalf("DrainOnce after terminal failure = (%d, %v), want (0, nil) without spin", n, err)
		}
	}
	if len(delivered) != 0 {
		t.Fatalf("expected 0 deliveries while seq=1 failed, got %v", delivered)
	}

	// Verify lifecycle suppression for FAILED EVENT_DELIVERY job snapshot as well.
	failedState := domain.JobStateFailed
	failedJobs, err := jobRepo.ListJobs(ctx, storage.JobFilter{State: &failedState})
	if err != nil || len(failedJobs) != 1 {
		t.Fatalf("failedJobs = (%+v, %v), want 1 FAILED job", failedJobs, err)
	}
	outbox := events.NewOutbox(events.WithClock(nowFn))
	if err := eventRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		_, err := outbox.AppendJobLifecycle(ctx, txRepo, domain.EventJobFailed, failedJobs[0])
		return err
	}); !errors.Is(err, domain.ErrInternalJobEventSuppressed) {
		t.Fatalf("AppendJobLifecycle FAILED EVENT_DELIVERY err = %v, want ErrInternalJobEventSuppressed", err)
	}
	if countJobLifecycleEvents(ctx, t, db) != 0 {
		t.Fatalf("expected 0 job.* events after FAILED EVENT_DELIVERY")
	}

	// Operator fixes downstream issue and reactivates/resets subscriber at a later timestamp.
	failSeq1.Store(false)
	now = t0.Add(time.Minute)
	if _, err := eventRepo.UpdateEventSubscriberState(ctx, "sub:perm", domain.SubscriberStateActive, nil, now); err != nil {
		t.Fatalf("reactivate subscriber: %v", err)
	}
	for _, wantSeq := range []int64{1, 2} {
		if n, err := delivery.DrainOnce(ctx); err != nil || n != 1 {
			t.Fatalf("DrainOnce post-reset seq %d = (%d, %v)", wantSeq, n, err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext post-reset seq %d = (%v, %v)", wantSeq, claimed, err)
		}
	}
	if !slices.Equal(delivered, []string{"perm-1", "perm-2"}) {
		t.Fatalf("delivered = %v, want [perm-1 perm-2]", delivered)
	}
}

func TestDelivery_AuthorizedFilterSkipAdvancesAndUnauthorizedErrorDoesNot(t *testing.T) {
	t.Parallel()
	db, tx, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 17, 20, 0, 0, time.UTC)
	now := t0
	nowFn := func() time.Time { return now }

	appendPublishedCatalogEvents(ctx, t, tx, eventRepo, t0, "skip-1", "ack-2", "auth-err-3")

	var delivered []string
	var authCheckFail atomic.Bool
	authCheckFail.Store(true)

	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{Now: nowFn})
	delivery := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner, events.DeliveryOptions{
		Now:         nowFn,
		MaxAttempts: 3,
	})
	_ = delivery.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:filtered",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			switch *ev.PublishedSequence {
			case 1:
				// Authorized filter-skip: advances cursor without calling external delivery.
				return domain.DeliverySkipped, nil
			case 2:
				delivered = append(delivered, ev.AggregateID)
				return domain.DeliveryAcknowledged, nil
			case 3:
				if authCheckFail.Load() {
					return 0, errors.New("authorization policy lookup failed")
				}
				return domain.DeliverySkipped, nil
			default:
				return 0, fmt.Errorf("unexpected sequence %d", *ev.PublishedSequence)
			}
		},
	})
	_ = delivery.RegisterJobs(runner)
	if err := delivery.SyncSubscribers(ctx); err != nil {
		t.Fatalf("SyncSubscribers: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// seq=1 skipped -> position advances to 1.
	if n, err := delivery.DrainOnce(ctx); err != nil || n != 1 {
		t.Fatalf("DrainOnce seq 1 = (%d, %v)", n, err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext seq 1 = (%v, %v)", claimed, err)
	}
	sub1, err := eventRepo.GetEventSubscriber(ctx, "sub:filtered")
	if err != nil || sub1.Position != 1 {
		t.Fatalf("after authorized filter-skip: sub = (%+v, %v), want position 1", sub1, err)
	}

	// seq=2 acknowledged -> position advances to 2.
	if n, err := delivery.DrainOnce(ctx); err != nil || n != 1 {
		t.Fatalf("DrainOnce seq 2 = (%d, %v)", n, err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext seq 2 = (%v, %v)", claimed, err)
	}
	sub2, err := eventRepo.GetEventSubscriber(ctx, "sub:filtered")
	if err != nil || sub2.Position != 2 {
		t.Fatalf("after ack: sub = (%+v, %v), want position 2", sub2, err)
	}

	// seq=3 fails auth lookup -> position MUST stay at 2 (fail-closed).
	if n, err := delivery.DrainOnce(ctx); err != nil || n != 1 {
		t.Fatalf("DrainOnce seq 3 = (%d, %v)", n, err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext seq 3 fail = (%v, %v)", claimed, err)
	}
	sub3Fail, err := eventRepo.GetEventSubscriber(ctx, "sub:filtered")
	if err != nil || sub3Fail.Position != 2 {
		t.Fatalf("after failed auth check: sub = (%+v, %v), want position 2", sub3Fail, err)
	}

	// Resolve auth check and retry -> position advances to 3.
	authCheckFail.Store(false)
	now = t0.Add(10 * time.Second)
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext seq 3 retry = (%v, %v)", claimed, err)
	}
	sub3Done, err := eventRepo.GetEventSubscriber(ctx, "sub:filtered")
	if err != nil || sub3Done.Position != 3 {
		t.Fatalf("after retry skip: sub = (%+v, %v), want position 3", sub3Done, err)
	}
	if !slices.Equal(delivered, []string{"ack-2"}) {
		t.Fatalf("delivered = %v, want [ack-2]", delivered)
	}
	if countJobLifecycleEvents(ctx, t, db) != 0 {
		t.Fatalf("expected 0 job.* events")
	}
}

func TestDelivery_OverlappingHooksStartupDrainAndOutOfOrderGuard(t *testing.T) {
	t.Parallel()
	db, tx, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 17, 30, 0, 0, time.UTC)
	nowFn := func() time.Time { return t0 }

	// 1. Seed pre-existing committed published events BEFORE startup.
	appendPublishedCatalogEvents(ctx, t, tx, eventRepo, t0, "boot-1", "boot-2")

	var mu sync.Mutex
	var delivered []int64

	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{
		Now:          nowFn,
		PollInterval: 10 * time.Millisecond,
	})
	delivery := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner, events.DeliveryOptions{
		Now:          nowFn,
		PollInterval: 10 * time.Millisecond,
	})
	if err := delivery.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:ordered",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			mu.Lock()
			delivered = append(delivered, *ev.PublishedSequence)
			mu.Unlock()
			return domain.DeliveryAcknowledged, nil
		},
	}); err != nil {
		t.Fatalf("RegisterSubscriber: %v", err)
	}
	if err := delivery.RegisterJobs(runner); err != nil {
		t.Fatalf("RegisterJobs: %v", err)
	}

	// Verify out-of-order rogue job (seq=2 queued while position=0) is rejected and cannot skip seq=1.
	if err := delivery.SyncSubscribers(ctx); err != nil {
		t.Fatalf("SyncSubscribers: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	roguePayload, _ := json.Marshal(domain.EventDeliveryPayloadV1{
		SubscriberID:      "sub:ordered",
		PublishedSequence: 2,
	})
	rogueJob := &domain.Job{
		Kind:        domain.JobKindEventDelivery,
		Payload:     roguePayload,
		MaxAttempts: 1,
		CreatedAt:   t0,
		RunAfter:    t0,
	}
	if _, err := jobRepo.Enqueue(ctx, rogueJob); err != nil {
		t.Fatalf("Enqueue rogue job: %v", err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext rogue job = (%v, %v)", claimed, err)
	}
	rogueAfter, err := jobRepo.GetJobByID(ctx, rogueJob.ID)
	if err != nil || rogueAfter.State != domain.JobStateFailed {
		t.Fatalf("rogue job = (%+v, %v), want FAILED", rogueAfter, err)
	}
	subBeforeStart, err := eventRepo.GetEventSubscriber(ctx, "sub:ordered")
	if err != nil || subBeforeStart.Position != 0 {
		t.Fatalf("position after rogue job = %d, want 0", subBeforeStart.Position)
	}

	// Start worker pools & delivery coordinator (drains boot-1 and boot-2 on startup).
	if err := runner.SetPoolSizes(map[domain.JobKind]int{domain.JobKindEventDelivery: 4}); err != nil {
		t.Fatalf("SetPoolSizes: %v", err)
	}
	if err := runner.Start(ctx); err != nil {
		t.Fatalf("runner.Start: %v", err)
	}
	defer runner.Stop()

	if err := delivery.Start(ctx); err != nil {
		t.Fatalf("delivery.Start: %v", err)
	}
	defer delivery.Stop()

	waitSubscriberPosition(ctx, t, eventRepo, "sub:ordered", 2)

	// 2. Now launch two independent Dispatcher instances whose post-commit hooks overlap
	//    while producers commit additional events concurrently.
	hookEntered := make(chan struct{}, 4)
	releaseHooks := make(chan struct{})

	d1 := events.NewDispatcher(eventRepo, events.WithOnPublished(func(_ context.Context, _ []domain.DomainEvent) error {
		hookEntered <- struct{}{}
		select {
		case <-releaseHooks:
		case <-ctx.Done():
			return ctx.Err()
		}
		delivery.Wake()
		return nil
	}))
	d2 := events.NewDispatcher(eventRepo, events.WithOnPublished(func(_ context.Context, _ []domain.DomainEvent) error {
		delivery.Wake()
		return nil
	}))

	// Append seq 3..4, dispatch via d1 and hold its hook open.
	outbox := events.NewOutbox()
	if err := tx.WithTx(ctx, func(stx storage.Tx) error {
		for _, id := range []string{"live-3", "live-4"} {
			if _, err := outbox.AppendPayload(ctx, stx.Events(), domain.EventAssetAdded, 1, domain.AggregateTypeAsset, id, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("append live-3..4: %v", err)
	}

	d1Err := make(chan error, 1)
	go func() {
		_, err := d1.DispatchOnce(ctx)
		d1Err <- err
	}()

	select {
	case <-hookEntered:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for d1 hook: %v", ctx.Err())
	}

	// While d1's hook is still blocked, append seq 5..6 and dispatch via d2 (so d2's hook fires first!).
	if err := tx.WithTx(ctx, func(stx storage.Tx) error {
		for _, id := range []string{"live-5", "live-6"} {
			if _, err := outbox.AppendPayload(ctx, stx.Events(), domain.EventAssetAdded, 1, domain.AggregateTypeAsset, id, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("append live-5..6: %v", err)
	}
	if _, err := d2.DispatchOnce(ctx); err != nil {
		t.Fatalf("d2.DispatchOnce: %v", err)
	}

	// Even before d1's hook returns, d2's Wake drains 3, 4, 5, 6 in strict sequence order!
	waitSubscriberPosition(ctx, t, eventRepo, "sub:ordered", 6)

	close(releaseHooks)
	if err := <-d1Err; err != nil {
		t.Fatalf("d1.DispatchOnce err: %v", err)
	}

	mu.Lock()
	gotDelivered := slices.Clone(delivered)
	mu.Unlock()

	wantSeqs := []int64{1, 2, 3, 4, 5, 6}
	if !slices.Equal(gotDelivered, wantSeqs) {
		t.Fatalf("delivered sequences = %v, want %v", gotDelivered, wantSeqs)
	}
	if countJobLifecycleEvents(ctx, t, db) != 0 {
		t.Fatalf("expected 0 job.* events")
	}
}

func TestDelivery_PausedSubscriberAndCrashRecoveryReconciliation(t *testing.T) {
	t.Parallel()
	db, tx, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 16, 17, 45, 0, 0, time.UTC)
	nowFn := func() time.Time { return t0 }
	appendPublishedCatalogEvents(ctx, t, tx, eventRepo, t0, "rec-1", "rec-2")

	var delivered []int64
	runner1 := jobs.NewRunner(jobRepo, jobs.RunnerOptions{Now: nowFn})
	delivery1 := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner1, events.DeliveryOptions{Now: nowFn})
	_ = delivery1.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:recover",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			delivered = append(delivered, *ev.PublishedSequence)
			return domain.DeliveryAcknowledged, nil
		},
	})
	_ = delivery1.RegisterJobs(runner1)
	if err := delivery1.SyncSubscribers(ctx); err != nil {
		t.Fatalf("SyncSubscribers: %v", err)
	}

	// Simulate crash AFTER AdvanceEventSubscriberCursor(1) committed but BEFORE MarkSucceeded committed.
	if n, err := delivery1.DrainOnce(ctx); err != nil || n != 1 {
		t.Fatalf("DrainOnce = (%d, %v)", n, err)
	}
	claimed1, err := jobRepo.ClaimNext(ctx, domain.JobKindEventDelivery, t0)
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if _, err := eventRepo.AdvanceEventSubscriberCursor(ctx, "sub:recover", 1, t0); err != nil {
		t.Fatalf("AdvanceEventSubscriberCursor(1): %v", err)
	}

	// Also test a PAUSED subscriber whose queued job cancels without delivering or advancing.
	if _, err := eventRepo.EnsureEventSubscriber(ctx, domain.EventSubscriber{
		ID:        "sub:paused",
		Kind:      "webhook",
		State:     domain.SubscriberStateActive,
		UpdatedAt: t0,
	}); err != nil {
		t.Fatalf("EnsureEventSubscriber sub:paused: %v", err)
	}
	pausedPayload, _ := json.Marshal(domain.EventDeliveryPayloadV1{
		SubscriberID:      "sub:paused",
		PublishedSequence: 1,
	})
	pausedDedupe, _ := domain.EventDeliveryDedupeKey("sub:paused")
	pausedJob := &domain.Job{
		Kind:      domain.JobKindEventDelivery,
		Payload:   pausedPayload,
		DedupeKey: &pausedDedupe,
		CreatedAt: t0,
		RunAfter:  t0,
	}
	if _, err := jobRepo.Enqueue(ctx, pausedJob); err != nil {
		t.Fatalf("Enqueue pausedJob: %v", err)
	}
	if _, err := eventRepo.UpdateEventSubscriberState(ctx, "sub:paused", domain.SubscriberStatePaused, nil, t0); err != nil {
		t.Fatalf("pause sub:paused: %v", err)
	}

	// New runner recovers under startup reconciliation:
	// claimed1 (position already 1) reconciles directly to SUCCEEDED without re-delivering seq=1!
	runner2 := jobs.NewRunner(jobRepo, jobs.RunnerOptions{Now: nowFn})
	delivery2 := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner2, events.DeliveryOptions{Now: nowFn})
	_ = delivery2.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:recover",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			delivered = append(delivered, *ev.PublishedSequence)
			return domain.DeliveryAcknowledged, nil
		},
	})
	_ = delivery2.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:paused",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			t.Fatalf("paused subscriber must not receive delivery for seq %d", *ev.PublishedSequence)
			return domain.DeliveryAcknowledged, nil
		},
	})
	_ = delivery2.RegisterJobs(runner2)

	report, err := runner2.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if report.Succeeded != 1 {
		t.Fatalf("recovery report.Succeeded = %d, want 1", report.Succeeded)
	}
	gotClaimed1, err := jobRepo.GetJobByID(ctx, claimed1.ID)
	if err != nil || gotClaimed1.State != domain.JobStateSucceeded {
		t.Fatalf("reconciled job = (%+v, %v), want SUCCEEDED", gotClaimed1, err)
	}

	// Claiming pausedJob runs executeDeliveryJob which sees PAUSED and cancels it without advancing cursor.
	if claimed, err := runner2.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext pausedJob = (%v, %v)", claimed, err)
	}
	gotPausedJob, err := jobRepo.GetJobByID(ctx, pausedJob.ID)
	if err != nil || gotPausedJob.State != domain.JobStateCancelled {
		t.Fatalf("pausedJob = (%+v, %v), want CANCELLED", gotPausedJob, err)
	}
	gotPausedSub, err := eventRepo.GetEventSubscriber(ctx, "sub:paused")
	if err != nil || gotPausedSub.Position != 0 {
		t.Fatalf("sub:paused = (%+v, %v), want position 0", gotPausedSub, err)
	}

	// DrainOnce skips PAUSED sub:paused and enqueues seq=2 for ACTIVE sub:recover.
	if n, err := delivery2.DrainOnce(ctx); err != nil || n != 1 {
		t.Fatalf("DrainOnce after recovery = (%d, %v), want (1, nil)", n, err)
	}
	if claimed, err := runner2.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext seq 2 = (%v, %v)", claimed, err)
	}
	if !slices.Equal(delivered, []int64{2}) {
		t.Fatalf("delivered = %v, want [2] (seq=1 must not be re-delivered after reconciled advance)", delivered)
	}

	// Verify lifecycle suppression on CANCELLED EVENT_DELIVERY job snapshot as well.
	outbox := events.NewOutbox()
	if err := eventRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		_, err := outbox.AppendJobLifecycle(ctx, txRepo, domain.EventJobCancelled, *gotPausedJob)
		return err
	}); !errors.Is(err, domain.ErrInternalJobEventSuppressed) {
		t.Fatalf("AppendJobLifecycle CANCELLED EVENT_DELIVERY err = %v, want ErrInternalJobEventSuppressed", err)
	}
	if countJobLifecycleEvents(ctx, t, db) != 0 {
		t.Fatalf("expected 0 job.* events")
	}
}
