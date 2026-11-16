package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func setupStreamTestDB(t *testing.T) (*testdb.TestDB, *postgres.EventRepository, *postgres.JobRepository, *postgres.UserRepository) {
	t.Helper()
	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("applying core migrations: %v", err)
	}
	return db, postgres.NewEventRepository(db.Pool), postgres.NewJobRepository(db.Pool), postgres.NewUserRepository(db.Pool)
}

func TestStreamService_ValidationGuards(t *testing.T) {
	t.Parallel()
	var nilSvc *events.StreamService
	p := testPrincipalWithPerms("u-1", domain.PermissionLibraryRead)

	if err := nilSvc.Stream(context.Background(), nil, app.EventStreamFilter{}, nil, nil); !errors.Is(err, app.ErrUnauthorized) {
		t.Fatalf("nil principal err = %v, want ErrUnauthorized", err)
	}
	if err := nilSvc.Stream(context.Background(), p, app.EventStreamFilter{}, nil, func(app.StreamEvent) error { return nil }); !errors.Is(err, app.ErrServiceBusy) {
		t.Fatalf("nil service err = %v, want ErrServiceBusy", err)
	}
}

func TestStreamService_OrderedDrainVisibilityCatchupAndProgress(t *testing.T) {
	t.Parallel()
	db, eventRepo, jobRepo, userRepo := setupStreamTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Create real users so jobs.created_by FK constraints and ON DELETE SET NULL work cleanly.
	alice := &domain.User{ID: domain.MustNewID(), Username: "stream-alice"}
	bob := &domain.User{ID: domain.MustNewID(), Username: "stream-bob"}
	tempCreator := &domain.User{ID: domain.MustNewID(), Username: "stream-temp"}
	for _, u := range []*domain.User{alice, bob, tempCreator} {
		if err := userRepo.CreateUser(ctx, u); err != nil {
			t.Fatalf("CreateUser(%s): %v", u.Username, err)
		}
	}

	alicePrincipal := testPrincipalWithPerms(alice.ID, domain.PermissionLibraryRead)
	bobPrincipal := testPrincipalWithPerms(bob.ID, domain.PermissionLibraryRead)
	readAllPrincipal := testPrincipalWithPerms("u-admin", domain.PermissionLibraryRead, domain.PermissionJobsReadAll)

	progressHub, err := events.NewProgressHub(events.ProgressHubOptions{MinInterval: time.Nanosecond})
	if err != nil {
		t.Fatalf("NewProgressHub: %v", err)
	}
	dispatcher := events.NewDispatcher(eventRepo, events.WithDispatcherPollInterval(10*time.Second))
	if err := dispatcher.Start(ctx); err != nil {
		t.Fatalf("dispatcher.Start: %v", err)
	}
	defer dispatcher.Stop()

	// Use a very long fallback PollInterval so the test proves dispatcher wake signals drive the drain.
	svc := events.NewStreamService(eventRepo, jobRepo, events.StreamOptions{
		ProgressHub:  progressHub,
		WakeSource:   dispatcher,
		PollInterval: 30 * time.Second,
	})

	// Seed a historical job + lifecycle event BEFORE any stream attaches (no M2 replay yet).
	preJob := &domain.Job{Kind: domain.JobKindScan, CreatedBy: &alice.ID}
	if _, err := jobRepo.Enqueue(ctx, preJob); err != nil {
		t.Fatalf("Enqueue preJob: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindScan, time.Now().UTC()); err != nil {
		t.Fatalf("ClaimNext preJob: %v", err)
	}
	if _, err := dispatcher.DispatchOnce(ctx); err != nil {
		t.Fatalf("DispatchOnce preJob: %v", err)
	}

	startStream := func(p *domain.Principal, jobFilter string) (<-chan app.StreamEvent, context.CancelFunc) {
		streamCtx, cancelStream := context.WithCancel(ctx)
		ch := make(chan app.StreamEvent, 32)
		attached := make(chan struct{})
		go func() {
			_ = svc.Stream(streamCtx, p, app.EventStreamFilter{JobID: jobFilter}, func() error {
				close(attached)
				return nil
			}, func(ev app.StreamEvent) error {
				ch <- ev
				return nil
			})
		}()
		select {
		case <-attached:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for stream attach")
		}
		return ch, cancelStream
	}

	aliceCh, cancelAlice := startStream(alicePrincipal, "")
	defer cancelAlice()
	bobCh, cancelBob := startStream(bobPrincipal, "")
	defer cancelBob()
	adminCh, cancelAdmin := startStream(readAllPrincipal, "")
	defer cancelAdmin()

	// Append a non-job domain event (must be skipped by M1A job stream while advancing cursor)
	// and an internal EVENT_DELIVERY job (must emit no lifecycle events).
	if err := eventRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		_, err := events.NewOutbox().AppendPayload(ctx, txRepo, domain.EventAssetAdded, domain.EventVersion1, domain.AggregateTypeAsset, "asset-1", domain.AssetEventPayloadV1{
			AssetID:       "asset-1",
			VolumeID:      "vol-1",
			LibraryID:     "lib-1",
			LibraryRootID: "root-1",
		})
		return err
	}); err != nil {
		t.Fatalf("append asset.added: %v", err)
	}
	delivKey := "EVENT_DELIVERY:sub-test"
	delivJob := &domain.Job{
		Kind:      domain.JobKindEventDelivery,
		Payload:   json.RawMessage(`{"subscriber_id":"sub-test","published_sequence":1}`),
		DedupeKey: &delivKey,
	}
	if _, err := jobRepo.Enqueue(ctx, delivJob); err != nil {
		t.Fatalf("Enqueue delivJob: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindEventDelivery, time.Now().UTC()); err != nil {
		t.Fatalf("ClaimNext delivJob: %v", err)
	}

	// Trigger Alice's job: ClaimNext -> ProgressHub tick -> MarkSucceeded, waking dispatcher on each transition.
	aliceJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: &alice.ID}
	if _, err := jobRepo.Enqueue(ctx, aliceJob); err != nil {
		t.Fatalf("Enqueue aliceJob: %v", err)
	}
	claimedAlice, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, time.Now().UTC())
	if err != nil {
		t.Fatalf("ClaimNext aliceJob: %v", err)
	}
	dispatcher.Notify()

	// Emit ephemeral progress tick (carries no SSE ID).
	if !progressHub.Publish(domain.JobProgressTick{
		JobID:        aliceJob.ID,
		Kind:         domain.JobKindRepack,
		CreatedBy:    &alice.ID,
		ScannedCount: 5,
		CurrentPath:  "/comics/ch1.cbz",
		DurationMS:   42,
	}) {
		t.Fatal("expected ProgressHub.Publish to succeed")
	}

	if _, err := jobRepo.MarkSucceeded(ctx, claimedAlice.ID, claimedAlice.Attempts, time.Now().UTC()); err != nil {
		t.Fatalf("MarkSucceeded aliceJob: %v", err)
	}
	dispatcher.Notify()

	// Also run an automatic (creatorless) job and a temp-creator job whose creator is deleted before dispatch!
	autoJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: nil}
	if _, err := jobRepo.Enqueue(ctx, autoJob); err != nil {
		t.Fatalf("Enqueue autoJob: %v", err)
	}
	claimedAuto, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, time.Now().UTC())
	if err != nil {
		t.Fatalf("ClaimNext autoJob: %v", err)
	}
	if _, err := jobRepo.MarkSucceeded(ctx, claimedAuto.ID, claimedAuto.Attempts, time.Now().UTC()); err != nil {
		t.Fatalf("MarkSucceeded autoJob: %v", err)
	}

	deletedCreatorJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: &tempCreator.ID}
	if _, err := jobRepo.Enqueue(ctx, deletedCreatorJob); err != nil {
		t.Fatalf("Enqueue deletedCreatorJob: %v", err)
	}
	claimedDel, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, time.Now().UTC())
	if err != nil {
		t.Fatalf("ClaimNext deletedCreatorJob: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM users WHERE id = $1`, tempCreator.ID); err != nil {
		t.Fatalf("delete tempCreator: %v", err)
	}
	dispatcher.Notify()

	// Verify Alice receives her job.started, job.progress (no ID!), and job.finished in order, and NO preJob or autoJob or deletedCreatorJob!
	var aliceLifecycle []app.StreamEvent
	var aliceProgress []app.StreamEvent
	deadline := time.After(5 * time.Second)
	for len(aliceLifecycle) < 2 || len(aliceProgress) < 1 {
		select {
		case ev := <-aliceCh:
			if ev.Name == domain.ProgressTickEventName {
				if ev.ID != "" {
					t.Fatalf("progress tick carried SSE ID %q, want empty", ev.ID)
				}
				aliceProgress = append(aliceProgress, ev)
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal(ev.Data, &pub); err != nil {
				t.Fatalf("unmarshal PublicEvent: %v", err)
			}
			if pub.AggregateID == preJob.ID {
				t.Fatalf("Alice received pre-attach event for %s (replay should not run in M1A)", preJob.ID)
			}
			if pub.AggregateID != aliceJob.ID {
				t.Fatalf("Alice received event for non-owned job %s (%s)", pub.AggregateID, ev.Name)
			}
			aliceLifecycle = append(aliceLifecycle, ev)
		case <-deadline:
			t.Fatalf("timed out waiting for Alice events (lifecycle=%d progress=%d)", len(aliceLifecycle), len(aliceProgress))
		}
	}
	if aliceLifecycle[0].Name != string(domain.EventJobStarted) || aliceLifecycle[1].Name != string(domain.EventJobFinished) {
		t.Fatalf("Alice lifecycle order = [%s %s], want [job.started job.finished]", aliceLifecycle[0].Name, aliceLifecycle[1].Name)
	}
	if aliceLifecycle[0].ID == "" || aliceLifecycle[1].ID == "" || aliceLifecycle[0].ID >= aliceLifecycle[1].ID {
		t.Fatalf("Alice lifecycle IDs = [%q %q], want ascending sequence IDs", aliceLifecycle[0].ID, aliceLifecycle[1].ID)
	}

	// Now trigger a job owned by Bob; Bob must receive ONLY Bob's job events (nothing from Alice/auto/deleted).
	bobJob := &domain.Job{Kind: domain.JobKindRepack, CreatedBy: &bob.ID}
	if _, err := jobRepo.Enqueue(ctx, bobJob); err != nil {
		t.Fatalf("Enqueue bobJob: %v", err)
	}
	if _, err := jobRepo.ClaimNext(ctx, domain.JobKindRepack, time.Now().UTC()); err != nil {
		t.Fatalf("ClaimNext bobJob: %v", err)
	}
	dispatcher.Notify()

	select {
	case ev := <-bobCh:
		var pub app.PublicEvent
		if err := json.Unmarshal(ev.Data, &pub); err != nil {
			t.Fatalf("unmarshal Bob event: %v", err)
		}
		if pub.AggregateID != bobJob.ID {
			t.Fatalf("non-creator Bob received event for %s (%s), want only %s", pub.AggregateID, ev.Name, bobJob.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Bob's own job.started event")
	}

	// Admin with jobs:read_all receives Alice's, autoJob's, deletedCreatorJob's, and Bob's lifecycle events.
	seenByAdmin := make(map[string][]string)
	adminDeadline := time.After(5 * time.Second)
	for len(seenByAdmin[aliceJob.ID]) < 2 || len(seenByAdmin[autoJob.ID]) < 2 || len(seenByAdmin[claimedDel.ID]) < 1 || len(seenByAdmin[bobJob.ID]) < 1 {
		select {
		case ev := <-adminCh:
			if ev.Name == domain.ProgressTickEventName {
				continue
			}
			var pub app.PublicEvent
			if err := json.Unmarshal(ev.Data, &pub); err != nil {
				t.Fatalf("unmarshal admin event: %v", err)
			}
			seenByAdmin[pub.AggregateID] = append(seenByAdmin[pub.AggregateID], ev.Name)
		case <-adminDeadline:
			t.Fatalf("timed out waiting for admin events, saw: %+v", seenByAdmin)
		}
	}
	if len(seenByAdmin[delivJob.ID]) != 0 {
		t.Fatalf("admin saw internal EVENT_DELIVERY lifecycle events: %v", seenByAdmin[delivJob.ID])
	}
}
