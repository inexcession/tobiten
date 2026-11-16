package main

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

func waitEventSubscriberPosition(ctx context.Context, t *testing.T, repo storage.EventRepository, id string, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			t.Fatalf("context cancelled waiting for subscriber %q position %d: %v", id, want, ctx.Err())
		}
		sub, err := repo.GetEventSubscriber(ctx, id)
		if err == nil && sub.Position == want {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	sub, err := repo.GetEventSubscriber(ctx, id)
	t.Fatalf("subscriber %q = (%+v, %v), want position %d", id, sub, err, want)
}

func TestRunDaemonEventDeliveryPipelineAndLifecycleSuppression(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)

	originalRegistryBuilder := buildDaemonRegistry
	originalLogger := slog.Default()
	t.Cleanup(func() {
		buildDaemonRegistry = originalRegistryBuilder
		slog.SetDefault(originalLogger)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	eventRepo := postgres.NewEventRepository(db.Pool)
	outbox := events.NewOutbox()
	appendBatch := func(ids ...string) {
		if err := eventRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
			for _, id := range ids {
				if _, err := outbox.AppendPayload(ctx, txRepo, domain.EventAssetAdded, 1, domain.AggregateTypeAsset, id, nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("append outbox events %v: %v", ids, err)
		}
	}

	// Seed two committed unpublished events before daemon startup.
	appendBatch("pre-1", "pre-2")

	var mu sync.Mutex
	var seenSeqs []int64
	var seenIDs []string

	buildDaemonRegistry = func(cfg *config.Config, services *app.Services) module.Registry {
		reg := BuildRegistryWithServices(cfg, services)
		_ = reg.Register(&module.SimpleModule{
			ModuleName: "test-event-delivery",
			Subscribers: []module.EventSubscriber{
				module.EventSubscriberSpec{
					ID:      "sub:daemon-e2e",
					SubKind: "webhook",
					DeliverFunc: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
						mu.Lock()
						seenSeqs = append(seenSeqs, *ev.PublishedSequence)
						seenIDs = append(seenIDs, ev.AggregateID)
						mu.Unlock()
						if ev.AggregateID == "live-skip-4" {
							return domain.DeliverySkipped, nil
						}
						return domain.DeliveryAcknowledged, nil
					},
				},
			},
		})
		return reg
	}

	err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		// Wait for startup sequencing + startup drain of pre-1, pre-2.
		waitEventSubscriberPosition(serveCtx, t, eventRepo, "sub:daemon-e2e", 2)

		// Append live events (acknowledged + authorized filter-skip) while daemon is running.
		appendBatch("live-ack-3", "live-skip-4")
		waitEventSubscriberPosition(serveCtx, t, eventRepo, "sub:daemon-e2e", 4)
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
	}

	mu.Lock()
	gotSeqs := slices.Clone(seenSeqs)
	gotIDs := slices.Clone(seenIDs)
	mu.Unlock()

	if !slices.Equal(gotSeqs, []int64{1, 2, 3, 4}) {
		t.Fatalf("seen published sequences = %v, want [1 2 3 4]", gotSeqs)
	}
	if !slices.Equal(gotIDs, []string{"pre-1", "pre-2", "live-ack-3", "live-skip-4"}) {
		t.Fatalf("seen aggregate IDs = %v, want [pre-1 pre-2 live-ack-3 live-skip-4]", gotIDs)
	}

	var jobEventCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE kind LIKE 'job.%'`).Scan(&jobEventCount); err != nil {
		t.Fatalf("counting job.* domain_events: %v", err)
	}
	if jobEventCount != 0 {
		t.Fatalf("expected 0 job.* events from EVENT_DELIVERY execution, got %d", jobEventCount)
	}
}

func TestRunDaemonRegularJobPositiveControlAndEventDeliverySuppression(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)

	originalRegistryBuilder := buildDaemonRegistry
	originalConfigHook := configureDaemonJobRunner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		buildDaemonRegistry = originalRegistryBuilder
		configureDaemonJobRunner = originalConfigHook
		slog.SetDefault(originalLogger)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	eventRepo := postgres.NewEventRepository(db.Pool)
	jobRepo := postgres.NewJobRepository(db.Pool)

	var mu sync.Mutex
	var delivered []domain.EventKind

	buildDaemonRegistry = func(cfg *config.Config, services *app.Services) module.Registry {
		reg := BuildRegistryWithServices(cfg, services)
		_ = reg.Register(&module.SimpleModule{
			ModuleName: "test-regular-job-pos-control",
			Jobs: []module.JobKind{
				module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint},
			},
			Subscribers: []module.EventSubscriber{
				module.EventSubscriberSpec{
					ID:      "sub:job-lifecycle",
					SubKind: "webhook",
					DeliverFunc: func(_ context.Context, ev domain.DomainEvent) (domain.EventDeliveryOutcome, error) {
						mu.Lock()
						delivered = append(delivered, ev.Kind)
						mu.Unlock()
						return domain.DeliveryAcknowledged, nil
					},
				},
			},
		})
		return reg
	}

	configureDaemonJobRunner = func(r *jobs.Runner, _ module.Registry, _ *config.Config, _ *app.Services) error {
		return r.RegisterKind(jobs.KindConfig{
			Kind:         domain.JobKindScan,
			ResumePolicy: domain.JobResumeCheckpoint,
			Handler: jobs.HandlerFunc(func(execCtx context.Context, exec *jobs.Execution) error {
				exec.ReportProgress(execCtx, 1, "/library/vol1.cbz")
				exec.ReportProgress(execCtx, 2, "/library/vol2.cbz")
				return nil
			}),
		})
	}

	var scanJobID string
	err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		scanJob := &domain.Job{Kind: domain.JobKindScan}
		if _, err := jobRepo.Enqueue(serveCtx, scanJob); err != nil {
			t.Fatalf("Enqueue scanJob: %v", err)
		}
		scanJobID = scanJob.ID

		// Regular job emits job.started (seq=1) + job.finished (seq=2); EVENT_DELIVERY delivers both without emitting any job.* events.
		waitEventSubscriberPosition(serveCtx, t, eventRepo, "sub:job-lifecycle", 2)
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
	}

	mu.Lock()
	gotKinds := slices.Clone(delivered)
	mu.Unlock()
	if !slices.Equal(gotKinds, []domain.EventKind{domain.EventJobStarted, domain.EventJobFinished}) {
		t.Fatalf("delivered kinds = %v, want [job.started job.finished]", gotKinds)
	}

	var totalEvents, nonScanEvents int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events`).Scan(&totalEvents); err != nil {
		t.Fatalf("counting total domain_events: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id <> $1`, scanJobID).Scan(&nonScanEvents); err != nil {
		t.Fatalf("counting non-scan domain_events: %v", err)
	}
	if totalEvents != 2 || nonScanEvents != 0 {
		t.Fatalf("totalEvents=%d nonScanEvents=%d, want total=2 nonScan=0", totalEvents, nonScanEvents)
	}
}
