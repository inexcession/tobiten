package events_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

type spyPublisher struct {
	inTx   bool
	called int
	events []domain.DomainEvent
	err    error
}

func (s *spyPublisher) InTx() bool {
	return s.inTx
}

func (s *spyPublisher) PublishUnsequencedEvents(_ context.Context, _ time.Time) ([]domain.DomainEvent, error) {
	s.called++
	if s.err != nil {
		return nil, s.err
	}
	return s.events, nil
}

func TestDispatcher_UnitValidationAndGuards(t *testing.T) {
	ctx := context.Background()

	var nilDispatcher *events.Dispatcher
	if _, err := nilDispatcher.DispatchOnce(ctx); !errors.Is(err, events.ErrNilEventPublisher) {
		t.Fatalf("nil Dispatcher.DispatchOnce err = %v, want ErrNilEventPublisher", err)
	}
	if err := nilDispatcher.Start(ctx); !errors.Is(err, events.ErrNilEventPublisher) {
		t.Fatalf("nil Dispatcher.Start err = %v, want ErrNilEventPublisher", err)
	}

	txPub := &spyPublisher{inTx: true}
	dTx := events.NewDispatcher(txPub)
	if _, err := dTx.DispatchOnce(ctx); !errors.Is(err, events.ErrDispatcherRequiresRootPublisher) {
		t.Fatalf("tx publisher DispatchOnce err = %v, want ErrDispatcherRequiresRootPublisher", err)
	}
	if err := dTx.Start(ctx); !errors.Is(err, events.ErrDispatcherRequiresRootPublisher) {
		t.Fatalf("tx publisher Start err = %v, want ErrDispatcherRequiresRootPublisher", err)
	}
	if txPub.called != 0 {
		t.Fatalf("expected tx publisher not to be called, got %d", txPub.called)
	}

	var hookCalls int
	emptyPub := &spyPublisher{inTx: false}
	dEmpty := events.NewDispatcher(emptyPub, events.WithOnPublished(func(_ context.Context, _ []domain.DomainEvent) error {
		hookCalls++
		return nil
	}))
	got, err := dEmpty.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("empty DispatchOnce err = %v", err)
	}
	if len(got) != 0 || hookCalls != 0 {
		t.Fatalf("expected no events and zero onPublished calls on empty poll, got len=%d hookCalls=%d", len(got), hookCalls)
	}

	dNeg := events.NewDispatcher(emptyPub, events.WithDispatcherPollInterval(-time.Millisecond))
	if err := dNeg.Start(ctx); !errors.Is(err, events.ErrInvalidDispatcherInterval) {
		t.Fatalf("negative poll interval Start err = %v, want ErrInvalidDispatcherInterval", err)
	}
}

func TestDispatcher_LowerRawIDCommittedLaterGetsLaterSequence(t *testing.T) {
	t.Parallel()
	db, _, eventRepo, _, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Separate PostgreSQL connections simulate concurrent producers committing out of ID order.
	connLow, err := pgx.Connect(ctx, db.URL)
	if err != nil {
		t.Fatalf("connect connLow: %v", err)
	}
	defer func() { _ = connLow.Close(context.Background()) }()

	connHigh, err := pgx.Connect(ctx, db.URL)
	if err != nil {
		t.Fatalf("connect connHigh: %v", err)
	}
	defer func() { _ = connHigh.Close(context.Background()) }()

	outbox := events.NewOutbox()

	// Connection 1 inserts the lower raw ID and holds its transaction open.
	txLow, err := connLow.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin txLow: %v", err)
	}
	defer func() { _ = txLow.Rollback(context.Background()) }()

	txLowRepo := postgres.NewEventRepository(txLow)
	evLow, err := outbox.AppendPayload(ctx, txLowRepo,
		domain.EventJobStarted, domain.EventVersion1, domain.AggregateTypeJob, "job-lower-raw-id",
		domain.JobEventPayloadV1{
			JobID:    "job-lower-raw-id",
			Kind:     domain.JobKindScan,
			State:    domain.JobStateRunning,
			Attempts: 1,
		})
	if err != nil {
		t.Fatalf("append evLow: %v", err)
	}

	// Reject transaction-bound publication through both the repository and Dispatcher,
	// and verify from inside the producer transaction that evLow stays unpublished.
	if _, err := txLowRepo.PublishUnsequencedEvents(ctx, time.Now().UTC()); !errors.Is(err, storage.ErrRootRepositoryRequired) {
		t.Fatalf("expected ErrRootRepositoryRequired on txLowRepo.PublishUnsequencedEvents, got %v", err)
	}
	if _, err := events.NewDispatcher(txLowRepo).DispatchOnce(ctx); !errors.Is(err, events.ErrDispatcherRequiresRootPublisher) {
		t.Fatalf("expected ErrDispatcherRequiresRootPublisher on txLowRepo dispatcher, got %v", err)
	}
	inTxLow, err := txLowRepo.GetEventByID(ctx, evLow.ID)
	if err != nil {
		t.Fatalf("GetEventByID inside txLow after rejected publish: %v", err)
	}
	if inTxLow.IsPublished() {
		t.Fatalf("expected evLow inside txLow to stay unpublished after rejection, got %+v", inTxLow)
	}

	// Connection 2 inserts a higher raw ID and commits first.
	txHigh, err := connHigh.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin txHigh: %v", err)
	}
	defer func() { _ = txHigh.Rollback(context.Background()) }()

	evHigh, err := outbox.AppendPayload(ctx, postgres.NewEventRepository(txHigh),
		domain.EventJobFinished, domain.EventVersion1, domain.AggregateTypeJob, "job-higher-raw-id",
		domain.JobEventPayloadV1{
			JobID:    "job-higher-raw-id",
			Kind:     domain.JobKindScan,
			State:    domain.JobStateSucceeded,
			Attempts: 1,
		})
	if err != nil {
		t.Fatalf("append evHigh: %v", err)
	}
	if err := txHigh.Commit(ctx); err != nil {
		t.Fatalf("Commit txHigh: %v", err)
	}

	if evLow.ID >= evHigh.ID {
		t.Fatalf("expected evLow.ID (%d) < evHigh.ID (%d)", evLow.ID, evHigh.ID)
	}

	clockNow := time.Date(2026, 11, 16, 16, 0, 0, 0, time.UTC)
	dispatcher := events.NewDispatcher(eventRepo, events.WithDispatcherClock(func() time.Time {
		return clockNow
	}))

	// Dispatch #1 publishes the committed higher raw ID at sequence 1; lower raw ID is invisible.
	batch1, err := dispatcher.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("DispatchOnce #1: %v", err)
	}
	if len(batch1) != 1 || batch1[0].ID != evHigh.ID {
		t.Fatalf("expected only evHigh (%d) in batch1, got %+v", evHigh.ID, batch1)
	}
	if batch1[0].PublishedSequence == nil || *batch1[0].PublishedSequence != 1 {
		t.Fatalf("expected evHigh published_sequence = 1, got %v", batch1[0].PublishedSequence)
	}
	if batch1[0].PublishedAt == nil || !batch1[0].PublishedAt.Equal(clockNow) {
		t.Fatalf("expected evHigh published_at = %v, got %v", clockNow, batch1[0].PublishedAt)
	}
	if _, err := eventRepo.GetEventByID(ctx, evLow.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected uncommitted evLow (%d) to remain invisible, got err=%v", evLow.ID, err)
	}

	// Now Connection 1 commits the lower raw ID.
	if err := txLow.Commit(ctx); err != nil {
		t.Fatalf("Commit txLow: %v", err)
	}

	// Before Dispatch #2, evLow is visible as unpublished.
	beforeDispatch2, err := eventRepo.GetEventByID(ctx, evLow.ID)
	if err != nil {
		t.Fatalf("GetEventByID(%d) after commit: %v", evLow.ID, err)
	}
	if beforeDispatch2.IsPublished() {
		t.Fatalf("expected evLow unpublished before DispatchOnce #2, got %+v", beforeDispatch2)
	}

	// Dispatch #2 scans all rows with published_sequence IS NULL regardless of raw ID,
	// assigning sequence 2 (later than evHigh's sequence 1) to the lower raw ID.
	clockNow = clockNow.Add(10 * time.Second)
	batch2, err := dispatcher.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("DispatchOnce #2: %v", err)
	}
	if len(batch2) != 1 || batch2[0].ID != evLow.ID {
		t.Fatalf("expected evLow (%d) in batch2, got %+v", evLow.ID, batch2)
	}
	if batch2[0].PublishedSequence == nil || *batch2[0].PublishedSequence != 2 {
		t.Fatalf("expected evLow published_sequence = 2, got %v", batch2[0].PublishedSequence)
	}
	if batch2[0].PublishedAt == nil || !batch2[0].PublishedAt.Equal(clockNow) {
		t.Fatalf("expected evLow published_at = %v, got %v", clockNow, batch2[0].PublishedAt)
	}
}

type faultCommitDB struct {
	*testdb.TestDB
	failCommitsRemaining atomic.Int32
	commitErr            error
	onBegin              func(pgx.Tx) pgx.Tx
}

func (f *faultCommitDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := f.TestDB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if f.onBegin != nil {
		tx = f.onBegin(tx)
	}
	return &faultCommitTx{Tx: tx, db: f}, nil
}

type faultCommitTx struct {
	pgx.Tx
	db *faultCommitDB
}

func (t *faultCommitTx) Commit(ctx context.Context) error {
	if t.db.failCommitsRemaining.Add(-1) >= 0 {
		return t.db.commitErr
	}
	return t.Tx.Commit(ctx)
}

func TestDispatcher_CommitsBeforeAnyDeliveryAndRollbackRetryHasNoGap(t *testing.T) {
	t.Parallel()
	db, transactor, eventRepo, _, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	outbox := events.NewOutbox()

	// Publish one baseline event -> sequence 1.
	var ev1, ev2, ev3 domain.DomainEvent
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		ev, err := outbox.AppendPayload(ctx, tx.Events(), domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "asset-1", nil)
		if err != nil {
			return err
		}
		ev1 = *ev
		return nil
	}); err != nil {
		t.Fatalf("append ev1: %v", err)
	}

	observerConn, err := pgx.Connect(ctx, db.URL)
	if err != nil {
		t.Fatalf("connect observerConn: %v", err)
	}
	defer func() { _ = observerConn.Close(context.Background()) }()

	var deliveredBatches [][]domain.DomainEvent
	assertCommittedObserver := func(obsCtx context.Context, batch []domain.DomainEvent) error {
		deliveredBatches = append(deliveredBatches, batch)
		for _, ev := range batch {
			var seq sql.NullInt64
			var pubAt sql.NullTime
			if err := observerConn.QueryRow(obsCtx,
				`SELECT published_sequence, published_at FROM domain_events WHERE id = $1`,
				ev.ID,
			).Scan(&seq, &pubAt); err != nil {
				return err
			}
			if !seq.Valid || !pubAt.Valid || seq.Int64 != *ev.PublishedSequence {
				t.Fatalf("separate connection did not see committed publication metadata for event %d before delivery hook: seq=%v pubAt=%v want=%d",
					ev.ID, seq, pubAt, *ev.PublishedSequence)
			}
		}
		return nil
	}

	faultDB := &faultCommitDB{
		TestDB:    db,
		commitErr: errors.New("injected commit failure"),
	}
	faultRepo := postgres.NewEventRepository(faultDB)
	fixedTime := time.Date(2026, 11, 16, 16, 10, 0, 0, time.UTC)
	dispatcher := events.NewDispatcher(
		faultRepo,
		events.WithDispatcherClock(func() time.Time { return fixedTime }),
		events.WithOnPublished(assertCommittedObserver),
	)

	firstPub, err := dispatcher.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("baseline DispatchOnce: %v", err)
	}
	if len(firstPub) != 1 || firstPub[0].ID != ev1.ID || *firstPub[0].PublishedSequence != 1 {
		t.Fatalf("unexpected baseline publish: %+v", firstPub)
	}

	// Append two more events.
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		e2, err := outbox.AppendPayload(ctx, tx.Events(), domain.EventAssetModified, 1, domain.AggregateTypeAsset, "asset-2", nil)
		if err != nil {
			return err
		}
		ev2 = *e2
		e3, err := outbox.AppendPayload(ctx, tx.Events(), domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "asset-3", nil)
		if err != nil {
			return err
		}
		ev3 = *e3
		return nil
	}); err != nil {
		t.Fatalf("append ev2/ev3: %v", err)
	}

	// Force the next sequencing commit to fail and roll back.
	faultDB.failCommitsRemaining.Store(1)
	if _, err := dispatcher.DispatchOnce(ctx); !errors.Is(err, faultDB.commitErr) {
		t.Fatalf("expected injected commit error, got %v", err)
	}

	// Delivery hook must NOT have been called for the rolled-back attempt.
	if len(deliveredBatches) != 1 {
		t.Fatalf("expected delivery hook not to run on rolled-back dispatch (want 1 batch, got %d)", len(deliveredBatches))
	}

	// Rows must remain unpublished in PostgreSQL.
	for _, id := range []int64{ev2.ID, ev3.ID} {
		got, err := eventRepo.GetEventByID(ctx, id)
		if err != nil {
			t.Fatalf("GetEventByID(%d): %v", id, err)
		}
		if got.PublishedSequence != nil || got.PublishedAt != nil {
			t.Fatalf("expected event %d unpublished after commit rollback, got seq=%v at=%v", id, got.PublishedSequence, got.PublishedAt)
		}
	}

	// Retry must assign 2 and 3 (no gap consumed!) and commit before delivery hook runs.
	fixedTime = fixedTime.Add(30 * time.Second)
	retried, err := dispatcher.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("retry DispatchOnce: %v", err)
	}
	if len(retried) != 2 {
		t.Fatalf("expected 2 retried events, got %d", len(retried))
	}
	if retried[0].ID != ev2.ID || *retried[0].PublishedSequence != 2 || !retried[0].PublishedAt.Equal(fixedTime) {
		t.Fatalf("unexpected retried[0]: %+v", retried[0])
	}
	if retried[1].ID != ev3.ID || *retried[1].PublishedSequence != 3 || !retried[1].PublishedAt.Equal(fixedTime) {
		t.Fatalf("unexpected retried[1]: %+v", retried[1])
	}
	if len(deliveredBatches) != 2 {
		t.Fatalf("expected delivery hook to run after committed retry, got %d batches", len(deliveredBatches))
	}
}

type blockingCommitBeginner struct {
	*testdb.TestDB
	mu           sync.Mutex
	calls        int
	firstEntered chan struct{}
	enterOnce    sync.Once
	releaseFirst chan struct{}
}

func (b *blockingCommitBeginner) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := b.TestDB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.calls++
	callNum := b.calls
	b.mu.Unlock()

	return &blockingCommitTx{
		Tx:           tx,
		callNum:      callNum,
		firstEntered: b.firstEntered,
		enterOnce:    &b.enterOnce,
		releaseFirst: b.releaseFirst,
	}, nil
}

type blockingCommitTx struct {
	pgx.Tx
	callNum      int
	firstEntered chan struct{}
	enterOnce    *sync.Once
	releaseFirst chan struct{}
}

func (t *blockingCommitTx) Commit(ctx context.Context) error {
	if t.callNum == 1 {
		t.enterOnce.Do(func() { close(t.firstEntered) })
		select {
		case <-t.releaseFirst:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return t.Tx.Commit(ctx)
}

type lockAttemptDB struct {
	*testdb.TestDB
	lockAttempted chan struct{}
	attemptOnce   sync.Once
}

func (d *lockAttemptDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := d.TestDB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &lockAttemptTx{Tx: tx, db: d}, nil
}

type lockAttemptTx struct {
	pgx.Tx
	db *lockAttemptDB
}

func (t *lockAttemptTx) Begin(ctx context.Context) (pgx.Tx, error) {
	sp, err := t.Tx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &lockAttemptTx{Tx: sp, db: t.db}, nil
}

func (t *lockAttemptTx) QueryRow(ctx context.Context, sqlStr string, args ...any) pgx.Row {
	if strings.Contains(sqlStr, "domain_event_sequence") && strings.Contains(sqlStr, "FOR UPDATE") {
		t.db.attemptOnce.Do(func() { close(t.db.lockAttempted) })
	}
	return t.Tx.QueryRow(ctx, sqlStr, args...)
}

func waitForChanBounded(ctx context.Context, t *testing.T, ch <-chan struct{}, desc string) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", desc, ctx.Err())
	}
}

func waitForWaitGroupBounded(ctx context.Context, t *testing.T, wg *sync.WaitGroup, desc string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	waitForChanBounded(ctx, t, done, desc)
}

func waitForSequenceLockWait(ctx context.Context, t *testing.T, db *testdb.TestDB) {
	t.Helper()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := db.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%domain_event_sequence%'
			)
		`).Scan(&waiting)
		if err != nil {
			t.Fatalf("polling pg_stat_activity for lock wait: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for second publisher to block on domain_event_sequence lock: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestDispatcher_OverlappingCallsDeterministicAndConcurrent(t *testing.T) {
	t.Parallel()
	db, transactor, _, _, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	outbox := events.NewOutbox()

	// Part 1: Deterministic overlap across two independent Dispatcher instances hitting PostgreSQL.
	// Dispatcher 1 enters its sequencing transaction (holding the DB row lock) and pauses before Commit;
	// while Dispatcher 2 is concurrently waiting on the DB row lock, a producer commits another event.
	// When Dispatcher 1 commits, Dispatcher 2 must wake up on a fresh snapshot, publish the new event
	// at the next contiguous sequence, and never duplicate or skip sequences.
	var evA, evB domain.DomainEvent
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		e, err := outbox.AppendPayload(ctx, tx.Events(), domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "overlap-A", nil)
		if err != nil {
			return err
		}
		evA = *e
		return nil
	}); err != nil {
		t.Fatalf("append evA: %v", err)
	}

	blockingDB := &blockingCommitBeginner{
		TestDB:       db,
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	lockWaitDB := &lockAttemptDB{
		TestDB:        db,
		lockAttempted: make(chan struct{}),
	}
	d1 := events.NewDispatcher(postgres.NewEventRepository(blockingDB))
	d2 := events.NewDispatcher(postgres.NewEventRepository(lockWaitDB))

	var (
		wg   sync.WaitGroup
		res1 []domain.DomainEvent
		res2 []domain.DomainEvent
		err1 error
		err2 error
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		res1, err1 = d1.DispatchOnce(ctx)
	}()

	waitForChanBounded(ctx, t, blockingDB.firstEntered, "first dispatcher to reach commit")

	// While d1 holds the PostgreSQL lock, launch d2 and synchronize on d2 reaching and
	// blocking on LockDomainEventSequence in PostgreSQL before committing evB and releasing d1.
	wg.Add(1)
	go func() {
		defer wg.Done()
		res2, err2 = d2.DispatchOnce(ctx)
	}()

	waitForChanBounded(ctx, t, lockWaitDB.lockAttempted, "second dispatcher to reach lock query")
	waitForSequenceLockWait(ctx, t, db)

	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		e, err := outbox.AppendPayload(ctx, tx.Events(), domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "overlap-B", nil)
		if err != nil {
			return err
		}
		evB = *e
		return nil
	}); err != nil {
		t.Fatalf("append evB: %v", err)
	}
	close(blockingDB.releaseFirst)
	waitForWaitGroupBounded(ctx, t, &wg, "overlapping dispatchers to finish")

	if err1 != nil || err2 != nil {
		t.Fatalf("overlapping dispatch errors: err1=%v err2=%v", err1, err2)
	}
	if len(res1) != 1 || res1[0].ID != evA.ID || *res1[0].PublishedSequence != 1 {
		t.Fatalf("expected d1 to publish evA (%d) at seq 1, got %+v", evA.ID, res1)
	}
	if len(res2) != 1 || res2[0].ID != evB.ID || *res2[0].PublishedSequence != 2 {
		t.Fatalf("expected d2 to publish evB (%d) at seq 2, got %+v", evB.ID, res2)
	}

	// Part 2: High-concurrency overlapping background Dispatcher + manual DispatchOnce callers + producers.
	var (
		deliveredMu sync.Mutex
		delivered   []domain.DomainEvent
	)
	recordDelivery := func(_ context.Context, batch []domain.DomainEvent) error {
		deliveredMu.Lock()
		delivered = append(delivered, batch...)
		deliveredMu.Unlock()
		return nil
	}

	bgDispatcher := events.NewDispatcher(
		postgres.NewEventRepository(db.Pool),
		events.WithDispatcherPollInterval(5*time.Millisecond),
		events.WithOnPublished(recordDelivery),
	)
	callerDispatcher := events.NewDispatcher(
		postgres.NewEventRepository(db.Pool),
		events.WithOnPublished(recordDelivery),
	)

	if err := bgDispatcher.Start(ctx); err != nil {
		t.Fatalf("bgDispatcher.Start: %v", err)
	}
	defer bgDispatcher.Stop()

	const (
		prodWorkers = 5
		perProducer = 12
	)
	const stressEvents = prodWorkers * perProducer

	var stressWG sync.WaitGroup
	stressErrs := make(chan error, prodWorkers+4)
	for p := range prodWorkers {
		stressWG.Add(1)
		go func(pid int) {
			defer stressWG.Done()
			for i := range perProducer {
				if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
					_, err := outbox.AppendPayload(ctx, tx.Events(), domain.EventProgressUpdated, 1,
						domain.AggregateTypeVolume, "vol-stress", domain.ProgressUpdatedPayloadV1{
							UserID:           "usr-1",
							VolumeID:         "vol-stress",
							Locator:          domain.NewPageLocator(pid*100+i, 200, "asset-1", nil),
							TotalProgression: 0.5,
							Completed:        false,
							Source:           "komga",
						})
					return err
				}); err != nil {
					stressErrs <- err
					return
				}
				bgDispatcher.Notify()
			}
		}(p)
	}

	for range 3 {
		stressWG.Add(1)
		go func() {
			defer stressWG.Done()
			for range 15 {
				if _, err := callerDispatcher.DispatchOnce(ctx); err != nil {
					stressErrs <- err
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	waitForWaitGroupBounded(ctx, t, &stressWG, "concurrent stress producers and dispatchers")
	close(stressErrs)
	for e := range stressErrs {
		t.Fatalf("stress error: %v", e)
	}

	// Final drain after producers finished.
	if _, err := callerDispatcher.DispatchOnce(ctx); err != nil {
		t.Fatalf("final DispatchOnce: %v", err)
	}
	bgDispatcher.Stop()
	if err := bgDispatcher.Err(); err != nil {
		t.Fatalf("bgDispatcher.Err(): %v", err)
	}

	// Verify all 2 + stressEvents rows in PostgreSQL have contiguous sequences 1..(2+stressEvents)
	// and non-NULL published_at, with zero duplicates and zero gaps.
	totalExpected := int64(2 + stressEvents)
	rows, err := db.Query(ctx,
		`SELECT id, published_sequence, published_at FROM domain_events ORDER BY published_sequence ASC NULLS LAST, id ASC`,
	)
	if err != nil {
		t.Fatalf("querying domain_events: %v", err)
	}
	defer rows.Close()

	var (
		count   int64
		prevSeq int64
	)
	for rows.Next() {
		var (
			id    int64
			seq   sql.NullInt64
			pubAt sql.NullTime
		)
		if err := rows.Scan(&id, &seq, &pubAt); err != nil {
			t.Fatalf("scan domain_events: %v", err)
		}
		if !seq.Valid || !pubAt.Valid {
			t.Fatalf("event %d remained unpublished: seq=%v pubAt=%v", id, seq, pubAt)
		}
		count++
		if seq.Int64 != prevSeq+1 {
			t.Fatalf("expected contiguous sequence %d for event %d, got %d", prevSeq+1, id, seq.Int64)
		}
		prevSeq = seq.Int64
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != totalExpected {
		t.Fatalf("expected %d total events, got %d", totalExpected, count)
	}

	// Verify delivered slices across both dispatchers cover all stressEvents without duplicates.
	deliveredMu.Lock()
	defer deliveredMu.Unlock()
	if len(delivered) != stressEvents {
		t.Fatalf("expected %d delivered stress events, got %d", stressEvents, len(delivered))
	}
	seenDelivered := make(map[int64]bool, stressEvents)
	for _, ev := range delivered {
		if seenDelivered[*ev.PublishedSequence] {
			t.Fatalf("duplicate delivered sequence %d", *ev.PublishedSequence)
		}
		seenDelivered[*ev.PublishedSequence] = true
	}
}

func TestDispatcher_BackgroundLoopLifecycleAndOnFatal(t *testing.T) {
	t.Parallel()

	fatalErr := errors.New("publisher failed")
	pub := &spyPublisher{err: fatalErr}
	fatalCh := make(chan error, 1)

	d := events.NewDispatcher(
		pub,
		events.WithDispatcherPollInterval(5*time.Millisecond),
		events.WithOnFatal(func(err error) {
			fatalCh <- err
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	d.Wait()

	select {
	case got := <-fatalCh:
		if !errors.Is(got, fatalErr) {
			t.Fatalf("onFatal got %v, want %v", got, fatalErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onFatal")
	}
	if !errors.Is(d.Err(), fatalErr) {
		t.Fatalf("d.Err() = %v, want %v", d.Err(), fatalErr)
	}

	// Healthy background loop can Start, reject duplicate Start, Notify, and Stop cleanly.
	healthyPub := &spyPublisher{}
	d2 := events.NewDispatcher(healthyPub, events.WithDispatcherPollInterval(time.Hour))
	if err := d2.Start(ctx); err != nil {
		t.Fatalf("d2.Start: %v", err)
	}
	if err := d2.Start(ctx); !errors.Is(err, events.ErrDispatcherAlreadyRunning) {
		t.Fatalf("expected ErrDispatcherAlreadyRunning, got %v", err)
	}
	d2.Notify()
	d2.Stop()
	if d2.Err() != nil {
		t.Fatalf("expected nil d2.Err() after Stop, got %v", d2.Err())
	}
}
