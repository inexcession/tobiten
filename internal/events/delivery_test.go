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

	var mu sync.Mutex
	currentTime := time.Date(2026, 11, 16, 17, 30, 0, 0, time.UTC)
	nowFn := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return currentTime
	}

	// 1. Seed pre-existing committed published events BEFORE startup.
	appendPublishedCatalogEvents(ctx, t, tx, eventRepo, nowFn(), "boot-1", "boot-2")

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
	rogueDedupe, _ := domain.EventDeliveryDedupeKey("sub:ordered")
	rogueJob := &domain.Job{
		Kind:        domain.JobKindEventDelivery,
		Payload:     roguePayload,
		DedupeKey:   &rogueDedupe,
		MaxAttempts: 1,
		CreatedAt:   nowFn(),
		RunAfter:    nowFn(),
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

	mu.Lock()
	currentTime = currentTime.Add(time.Second)
	mu.Unlock()

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

type pauseBeforeScheduleEventRepo struct {
	storage.EventRepository
	enteringSchedule chan<- struct{}
	releaseSchedule  <-chan struct{}
	once             sync.Once
}

func (p *pauseBeforeScheduleEventRepo) ScheduleNextEventDelivery(
	ctx context.Context,
	subscriberID string,
	maxAttempts int,
	now time.Time,
) (*domain.Job, bool, error) {
	p.once.Do(func() {
		close(p.enteringSchedule)
		select {
		case <-p.releaseSchedule:
		case <-ctx.Done():
		}
	})
	return p.EventRepository.ScheduleNextEventDelivery(ctx, subscriberID, maxAttempts, now)
}

func TestDelivery_ConcurrentDrainCannotBypassTerminalRetryLimit(t *testing.T) {
	t.Parallel()
	db, tx, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var mu sync.Mutex
	currentTime := time.Date(2026, 11, 16, 18, 0, 0, 0, time.UTC)
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

	appendPublishedCatalogEvents(ctx, t, tx, eventRepo, nowFn(), "lim-1", "lim-2")

	var failSeq1 atomic.Bool
	failSeq1.Store(true)
	var delivered []int64

	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{Now: nowFn})
	baseDelivery := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner, events.DeliveryOptions{
		Now:         nowFn,
		MaxAttempts: 2,
	})
	subSpec := events.SubscriberSpec{
		SubscriberID:   "sub:retry-limit",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			if *ev.PublishedSequence == 1 && failSeq1.Load() {
				return 0, errors.New("downstream permanent 500")
			}
			mu.Lock()
			delivered = append(delivered, *ev.PublishedSequence)
			mu.Unlock()
			return domain.DeliveryAcknowledged, nil
		},
	}
	if err := baseDelivery.RegisterSubscriber(subSpec); err != nil {
		t.Fatalf("RegisterSubscriber: %v", err)
	}
	if err := baseDelivery.RegisterJobs(runner); err != nil {
		t.Fatalf("RegisterJobs: %v", err)
	}
	if err := baseDelivery.SyncSubscribers(ctx); err != nil {
		t.Fatalf("SyncSubscribers: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Attempt 1 fails -> QUEUED with backoff.
	if n, err := baseDelivery.DrainOnce(ctx); err != nil || n != 1 {
		t.Fatalf("initial DrainOnce = (%d, %v), want (1, nil)", n, err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext attempt 1 = (%v, %v)", claimed, err)
	}

	// Claim attempt 2 (the final attempt) directly so it is RUNNING while a concurrent drain decides to schedule.
	advanceTime(6 * time.Second)
	finalRunning, err := jobRepo.ClaimNext(ctx, domain.JobKindEventDelivery, nowFn())
	if err != nil || finalRunning.Attempts != 2 {
		t.Fatalf("ClaimNext final attempt = (%+v, %v), want attempts=2", finalRunning, err)
	}

	// Start a concurrent DrainOnce that lists active subscribers and pauses right before ScheduleNextEventDelivery.
	entering := make(chan struct{})
	release := make(chan struct{})
	pausedRepo := &pauseBeforeScheduleEventRepo{
		EventRepository:  eventRepo,
		enteringSchedule: entering,
		releaseSchedule:  release,
	}
	racyDelivery := events.NewDeliveryCoordinator(pausedRepo, jobRepo, runner, events.DeliveryOptions{
		Now:         nowFn,
		MaxAttempts: 2,
	})
	_ = racyDelivery.RegisterSubscriber(subSpec)

	type drainResult struct {
		n   int
		err error
	}
	drainCh := make(chan drainResult, 1)
	go func() {
		n, err := racyDelivery.DrainOnce(ctx)
		drainCh <- drainResult{n: n, err: err}
	}()

	select {
	case <-entering:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for DrainOnce to enter paused schedule: %v", ctx.Err())
	}

	// While the enqueue decision is paused, commit the final failure -> FAILED.
	if _, err := jobRepo.FailOrRetry(ctx, finalRunning.ID, finalRunning.Attempts, "final failure", time.Time{}, nowFn()); err != nil {
		t.Fatalf("FailOrRetry final attempt: %v", err)
	}

	// Release the paused DrainOnce and also race a direct JobRepository.Enqueue attempt.
	close(release)
	select {
	case res := <-drainCh:
		if res.err != nil || res.n != 0 {
			t.Fatalf("paused DrainOnce = (%d, %v), want (0, nil)", res.n, res.err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for paused DrainOnce: %v", ctx.Err())
	}

	canonPayload, _ := json.Marshal(domain.EventDeliveryPayloadV1{
		SubscriberID:      "sub:retry-limit",
		PublishedSequence: 1,
	})
	canonKey, _ := domain.EventDeliveryDedupeKey("sub:retry-limit")
	if created, err := jobRepo.Enqueue(ctx, &domain.Job{
		Kind:        domain.JobKindEventDelivery,
		Payload:     canonPayload,
		DedupeKey:   &canonKey,
		MaxAttempts: 2,
		CreatedAt:   nowFn(),
		RunAfter:    nowFn(),
	}); created || !errors.Is(err, domain.ErrEventDeliveryBlocked) {
		t.Fatalf("direct Enqueue after terminal failure = (%v, %v), want (false, ErrEventDeliveryBlocked)", created, err)
	}

	// Verify total EVENT_DELIVERY job count stays 1 (FAILED at attempts=2) and cursor stays 0.
	kindFilter := domain.JobKindEventDelivery
	allJobs, err := jobRepo.ListJobs(ctx, storage.JobFilter{Kind: &kindFilter})
	if err != nil || len(allJobs) != 1 || allJobs[0].State != domain.JobStateFailed || allJobs[0].Attempts != 2 {
		t.Fatalf("jobs = (%+v, %v), want exactly 1 FAILED job at attempts=2", allJobs, err)
	}
	subAfterFail, err := eventRepo.GetEventSubscriber(ctx, "sub:retry-limit")
	if err != nil || subAfterFail.Position != 0 {
		t.Fatalf("subscriber after terminal failure = (%+v, %v), want position 0", subAfterFail, err)
	}

	// After subscriber reset/reactivation at a later timestamp, seq=1 and seq=2 deliver cleanly.
	failSeq1.Store(false)
	advanceTime(time.Second)
	if _, err := eventRepo.UpdateEventSubscriberState(ctx, "sub:retry-limit", domain.SubscriberStateActive, nil, nowFn()); err != nil {
		t.Fatalf("reactivate subscriber: %v", err)
	}
	for _, wantSeq := range []int64{1, 2} {
		if n, err := baseDelivery.DrainOnce(ctx); err != nil || n != 1 {
			t.Fatalf("post-reset DrainOnce seq %d = (%d, %v), want (1, nil)", wantSeq, n, err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
			t.Fatalf("post-reset ClaimAndRunNext seq %d = (%v, %v)", wantSeq, claimed, err)
		}
	}

	mu.Lock()
	gotDelivered := slices.Clone(delivered)
	mu.Unlock()
	if !slices.Equal(gotDelivered, []int64{1, 2}) {
		t.Fatalf("delivered = %v, want [1 2]", gotDelivered)
	}
	if countJobLifecycleEvents(ctx, t, db) != 0 {
		t.Fatalf("expected 0 job.* events")
	}
}

func TestDelivery_EnforcesCanonicalDedupeKeyWithMultipleWorkers(t *testing.T) {
	t.Parallel()
	db, tx, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t0 := time.Now().UTC()
	appendPublishedCatalogEvents(ctx, t, tx, eventRepo, t0, "mw-1", "mw-2")

	var mu sync.Mutex
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var deliveredSeqs []int64

	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{
		PollInterval: 10 * time.Millisecond,
	})
	delivery := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner, events.DeliveryOptions{
		PollInterval: 10 * time.Millisecond,
	})
	if err := delivery.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:multi",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			cur := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				prev := maxInFlight.Load()
				if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(25 * time.Millisecond)
			mu.Lock()
			deliveredSeqs = append(deliveredSeqs, *ev.PublishedSequence)
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

	// Insert rows directly into PostgreSQL bypassing JobRepository.Enqueue to simulate
	// executable jobs with missing (NULL) and mismatched dedupe_key values.
	payloadSeq1, _ := json.Marshal(domain.EventDeliveryPayloadV1{
		SubscriberID:      "sub:multi",
		PublishedSequence: 1,
	})
	missingID, err := domain.NewID()
	if err != nil {
		t.Fatalf("NewID missingID: %v", err)
	}
	mismatchedID, err := domain.NewID()
	if err != nil {
		t.Fatalf("NewID mismatchedID: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO jobs (id, kind, state, payload, checkpoint, attempts, max_attempts, dedupe_key, run_after, created_at, updated_at)
		VALUES
			($1, 'EVENT_DELIVERY', 'QUEUED', $2, '{}', 0, 3, NULL, $4, $4, $4),
			($3, 'EVENT_DELIVERY', 'QUEUED', $2, '{}', 0, 3, 'EVENT_DELIVERY:wrong-sub', $4, $4, $4)
	`, missingID, payloadSeq1, mismatchedID, t0); err != nil {
		t.Fatalf("insert invalid dedupe jobs: %v", err)
	}

	if err := runner.SetPoolSizes(map[domain.JobKind]int{domain.JobKindEventDelivery: 4}); err != nil {
		t.Fatalf("SetPoolSizes: %v", err)
	}
	if err := runner.Start(ctx); err != nil {
		t.Fatalf("runner.Start: %v", err)
	}
	defer runner.Stop()

	// Wait until both non-canonical jobs fail without invoking Deliver or advancing the cursor.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		jMissing, err1 := jobRepo.GetJobByID(ctx, missingID)
		jMismatch, err2 := jobRepo.GetJobByID(ctx, mismatchedID)
		if err1 == nil && err2 == nil &&
			jMissing.State == domain.JobStateFailed &&
			jMismatch.State == domain.JobStateFailed {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	for _, badID := range []string{missingID, mismatchedID} {
		gotBad, err := jobRepo.GetJobByID(ctx, badID)
		if err != nil || gotBad.State != domain.JobStateFailed {
			t.Fatalf("non-canonical job %s = (%+v, %v), want FAILED", badID, gotBad, err)
		}
	}
	mu.Lock()
	deliveredBeforeDrain := slices.Clone(deliveredSeqs)
	mu.Unlock()
	if len(deliveredBeforeDrain) != 0 {
		t.Fatalf("non-canonical jobs invoked callback: %v", deliveredBeforeDrain)
	}

	// Now start DeliveryCoordinator; canonical EVENT_DELIVERY:sub:multi jobs deliver seq 1 and 2 sequentially.
	if err := delivery.Start(ctx); err != nil {
		t.Fatalf("delivery.Start: %v", err)
	}
	defer delivery.Stop()

	waitSubscriberPosition(ctx, t, eventRepo, "sub:multi", 2)

	if maxInFlight.Load() != 1 {
		t.Fatalf("max concurrent callbacks for same subscriber = %d, want 1", maxInFlight.Load())
	}
	mu.Lock()
	gotDelivered := slices.Clone(deliveredSeqs)
	mu.Unlock()
	if !slices.Equal(gotDelivered, []int64{1, 2}) {
		t.Fatalf("delivered sequences = %v, want [1 2]", gotDelivered)
	}
	if countJobLifecycleEvents(ctx, t, db) != 0 {
		t.Fatalf("expected 0 job.* events")
	}
}

func TestDeliveryCoordinator_UnsetModuleCallbackRejectedAndNamedPreserved(t *testing.T) {
	t.Parallel()
	_, tx, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 0. Typed nil *module.EventSubscriberSpec and *events.SubscriberSpec held in interfaces
	// must be rejected cleanly without panicking on value-receiver method calls.
	var nilModulePtr *module.EventSubscriberSpec
	nilModReg := module.NewRegistry()
	_ = nilModReg.Register(&module.SimpleModule{
		ModuleName:  "typed-nil-mod",
		Subscribers: []module.EventSubscriber{nilModulePtr},
	})
	nilCheckCoord := events.NewDeliveryCoordinator(eventRepo, jobRepo, nil, events.DeliveryOptions{})
	if err := nilCheckCoord.RegisterModuleSubscribers(nilModReg); !errors.Is(err, events.ErrInvalidSubscriber) {
		t.Fatalf("typed nil *module.EventSubscriberSpec err = %v, want ErrInvalidSubscriber", err)
	}
	var nilEventSubPtr *events.SubscriberSpec
	if err := nilCheckCoord.RegisterSubscriber(nilEventSubPtr); !errors.Is(err, events.ErrInvalidSubscriber) {
		t.Fatalf("typed nil *events.SubscriberSpec err = %v, want ErrInvalidSubscriber", err)
	}
	if err := eventRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		txCoord := events.NewDeliveryCoordinator(txRepo, jobRepo, nil, events.DeliveryOptions{})
		if _, err := txCoord.DrainOnce(ctx); !errors.Is(err, storage.ErrRootRepositoryRequired) {
			return fmt.Errorf("tx-bound DrainOnce = %v, want ErrRootRepositoryRequired", err)
		}
		if err := txCoord.Start(ctx); !errors.Is(err, storage.ErrRootRepositoryRequired) {
			return fmt.Errorf("tx-bound Start = %v, want ErrRootRepositoryRequired", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("tx-bound DeliveryCoordinator guard: %v", err)
	}

	// 1. Value and pointer EventSubscriberSpec with nil DeliverFunc are rejected at registration.
	for _, tc := range []struct {
		name string
		sub  module.EventSubscriber
	}{
		{
			name: "value spec with nil callback",
			sub: module.EventSubscriberSpec{
				ID:      "sub:nil-val",
				SubKind: "webhook",
			},
		},
		{
			name: "pointer spec with nil callback",
			sub: &module.EventSubscriberSpec{
				ID:      "sub:nil-ptr",
				SubKind: "webhook",
			},
		},
	} {
		reg := module.NewRegistry()
		_ = reg.Register(&module.SimpleModule{
			ModuleName:  "bad-sub-mod",
			Subscribers: []module.EventSubscriber{tc.sub},
		})
		coord := events.NewDeliveryCoordinator(eventRepo, jobRepo, nil, events.DeliveryOptions{})
		if err := coord.RegisterModuleSubscribers(reg); !errors.Is(err, events.ErrMissingSubscriberHandler) {
			t.Fatalf("%s: RegisterModuleSubscribers err = %v, want ErrMissingSubscriberHandler", tc.name, err)
		}
	}

	// Direct RegisterSubscriber with nil DeliverFn on SubscriberSpec is also rejected.
	directCoord := events.NewDeliveryCoordinator(eventRepo, jobRepo, nil, events.DeliveryOptions{})
	if err := directCoord.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:direct-nil",
		SubscriberKind: "webhook",
	}); !errors.Is(err, events.ErrMissingSubscriberHandler) {
		t.Fatalf("direct nil DeliverFn err = %v, want ErrMissingSubscriberHandler", err)
	}

	// 2. NamedEventSubscriber descriptor preserves handler attachment and executes cleanly.
	t0 := time.Date(2026, 11, 16, 18, 10, 0, 0, time.UTC)
	nowFn := func() time.Time { return t0 }
	appendPublishedCatalogEvents(ctx, t, tx, eventRepo, t0, "named-1")

	reg := module.NewRegistry()
	_ = reg.Register(&module.SimpleModule{
		ModuleName:  "named-mod",
		Subscribers: []module.EventSubscriber{module.NamedEventSubscriber("sub:named")},
	})

	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{Now: nowFn})
	coord := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner, events.DeliveryOptions{Now: nowFn})
	if err := coord.RegisterModuleSubscribers(reg); err != nil {
		t.Fatalf("RegisterModuleSubscribers(named): %v", err)
	}
	var seen []string
	if err := coord.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:named",
		SubscriberKind: "sub:named",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			seen = append(seen, ev.AggregateID)
			return domain.DeliveryAcknowledged, nil
		},
	}); err != nil {
		t.Fatalf("attach handler to NamedEventSubscriber: %v", err)
	}
	if err := coord.RegisterJobs(runner); err != nil {
		t.Fatalf("RegisterJobs: %v", err)
	}
	if err := coord.SyncSubscribers(ctx); err != nil {
		t.Fatalf("SyncSubscribers: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n, err := coord.DrainOnce(ctx); err != nil || n != 1 {
		t.Fatalf("DrainOnce = (%d, %v), want (1, nil)", n, err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext = (%v, %v)", claimed, err)
	}
	if !slices.Equal(seen, []string{"named-1"}) {
		t.Fatalf("seen = %v, want [named-1]", seen)
	}
}

func TestDelivery_RegularJobPositiveControlAndProgressHubNoRows(t *testing.T) {
	t.Parallel()
	db, _, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var clockMu sync.Mutex
	now := time.Date(2026, 11, 16, 19, 15, 0, 0, time.UTC)
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}

	userRepo := postgres.NewUserRepository(db.Pool)
	creator := &domain.User{ID: domain.MustNewID(), Username: "pos-ctrl-creator"}
	if err := userRepo.CreateUser(ctx, creator); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	creatorPrincipal := testPrincipalWithPerms(creator.ID, domain.PermissionLibraryRead)
	otherPrincipal := testPrincipalWithPerms("other-user", domain.PermissionLibraryRead)
	adminPrincipal := testPrincipalWithPerms("admin-user", domain.PermissionLibraryRead, domain.PermissionJobsReadAll)

	hub, err := events.NewProgressHub(events.ProgressHubOptions{
		MinInterval: 100 * time.Millisecond,
		Now:         nowFn,
	})
	if err != nil {
		t.Fatalf("NewProgressHub: %v", err)
	}

	chCreator, unsubCreator, err := hub.Subscribe(ctx, events.ProgressSubscribeOptions{Principal: creatorPrincipal})
	if err != nil {
		t.Fatalf("Subscribe creator: %v", err)
	}
	defer unsubCreator()

	chOther, unsubOther, err := hub.Subscribe(ctx, events.ProgressSubscribeOptions{Principal: otherPrincipal})
	if err != nil {
		t.Fatalf("Subscribe other: %v", err)
	}
	defer unsubOther()

	chAdmin, unsubAdmin, err := hub.Subscribe(ctx, events.ProgressSubscribeOptions{Principal: adminPrincipal})
	if err != nil {
		t.Fatalf("Subscribe admin: %v", err)
	}
	defer unsubAdmin()

	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{
		Now:         nowFn,
		ProgressHub: hub,
	})
	coord := events.NewDeliveryCoordinator(eventRepo, jobRepo, runner, events.DeliveryOptions{Now: nowFn})
	dispatcher := events.NewDispatcher(eventRepo, events.WithDispatcherClock(nowFn))

	var delivered []domain.DomainEvent
	if err := coord.RegisterSubscriber(events.SubscriberSpec{
		SubscriberID:   "sub:pos-control",
		SubscriberKind: "webhook",
		DeliverFn: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
			delivered = append(delivered, ev)
			return domain.DeliveryAcknowledged, nil
		},
	}); err != nil {
		t.Fatalf("RegisterSubscriber: %v", err)
	}
	if err := coord.RegisterJobs(runner); err != nil {
		t.Fatalf("RegisterJobs EVENT_DELIVERY: %v", err)
	}
	if err := runner.RegisterKind(jobs.KindConfig{
		Kind:         domain.JobKindScan,
		ResumePolicy: domain.JobResumeCheckpoint,
		Handler: jobs.HandlerFunc(func(_ context.Context, exec *jobs.Execution) error {
			// Tick 1 at t0: delivered.
			if !exec.ReportProgress(1, "/manga/v01.cbz") {
				return errors.New("expected tick 1 to publish")
			}
			// Tick 2 at +40ms: rate-limited.
			advance(40 * time.Millisecond)
			if exec.ReportProgress(2, "/manga/v02.cbz") {
				return errors.New("expected tick 2 to be rate-limited")
			}
			// Tick 3 at +120ms: delivered.
			advance(80 * time.Millisecond)
			if !exec.ReportProgress(3, "/manga/v03.cbz") {
				return errors.New("expected tick 3 to publish")
			}
			return nil
		}),
	}); err != nil {
		t.Fatalf("RegisterKind SCAN: %v", err)
	}

	if err := coord.SyncSubscribers(ctx); err != nil {
		t.Fatalf("SyncSubscribers: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Enqueue and execute a regular SCAN job (positive control).
	scanJob := &domain.Job{
		Kind:      domain.JobKindScan,
		CreatedBy: &creator.ID,
		CreatedAt: nowFn(),
		RunAfter:  nowFn(),
	}
	if _, err := jobRepo.Enqueue(ctx, scanJob); err != nil {
		t.Fatalf("Enqueue scanJob: %v", err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext SCAN = (%v, %v)", claimed, err)
	}

	// Verify live progress ticks reached creator & admin with no IDs and left no non-lifecycle rows.
	for _, wantCount := range []int64{1, 3} {
		tickC := <-chCreator
		tickA := <-chAdmin
		if tickC.JobID != scanJob.ID || tickC.ScannedCount != wantCount || tickA.ScannedCount != wantCount {
			t.Fatalf("unexpected progress ticks: creator=%+v admin=%+v want count=%d", tickC, tickA, wantCount)
		}
	}
	select {
	case unexpected := <-chOther:
		t.Fatalf("non-creator received unexpected progress tick: %+v", unexpected)
	default:
	}

	// Sequence the outbox events from the regular SCAN job and deliver them via EVENT_DELIVERY.
	published, err := dispatcher.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("DispatchOnce: %v", err)
	}
	if len(published) != 2 {
		t.Fatalf("published %d events, want 2 (job.started + job.finished)", len(published))
	}
	for range 2 {
		if n, err := coord.DrainOnce(ctx); err != nil || n != 1 {
			t.Fatalf("DrainOnce = (%d, %v), want (1, nil)", n, err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindEventDelivery); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext EVENT_DELIVERY = (%v, %v)", claimed, err)
		}
	}
	waitSubscriberPosition(ctx, t, eventRepo, "sub:pos-control", 2)

	if len(delivered) != 2 || delivered[0].Kind != domain.EventJobStarted || delivered[1].Kind != domain.EventJobFinished {
		t.Fatalf("delivered = %+v, want [job.started job.finished]", delivered)
	}

	// Total domain_events must remain 2: zero rows for progress ticks, zero rows for EVENT_DELIVERY!
	var totalCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events`).Scan(&totalCount); err != nil {
		t.Fatalf("counting total domain_events: %v", err)
	}
	if totalCount != 2 {
		t.Fatalf("total domain_events = %d, want 2 (only SCAN job.started + job.finished)", totalCount)
	}
}
