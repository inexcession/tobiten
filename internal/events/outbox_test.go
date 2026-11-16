package events_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

type spyAppender struct {
	called int
	last   *domain.DomainEvent
}

func (s *spyAppender) AppendEvent(_ context.Context, event *domain.DomainEvent) error {
	s.called++
	copied := *event
	copied.ID = int64(s.called)
	s.last = &copied
	*event = copied
	return nil
}

func setupOutboxTestDB(t *testing.T) (*testdb.TestDB, *postgres.Transactor, *postgres.EventRepository, *postgres.JobRepository, *postgres.SettingsRepository) {
	t.Helper()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("setupOutboxTestDB: applying migrations: %v", err)
	}

	return db,
		postgres.NewTransactor(db.Pool),
		postgres.NewEventRepository(db.Pool),
		postgres.NewJobRepository(db.Pool),
		postgres.NewSettingsRepository(db.Pool)
}

func TestOutbox_UnitValidationAndSuppression(t *testing.T) {
	fixedNow := time.Date(2026, 11, 16, 14, 0, 0, 0, time.UTC)
	outbox := events.NewOutbox(events.WithClock(func() time.Time { return fixedNow }))
	ctx := context.Background()

	if err := outbox.Append(ctx, nil, &domain.DomainEvent{}); !errors.Is(err, events.ErrNilOutboxAppender) {
		t.Fatalf("expected ErrNilOutboxAppender, got %v", err)
	}
	spy := &spyAppender{}
	if err := outbox.Append(ctx, spy, nil); !errors.Is(err, events.ErrNilDomainEvent) {
		t.Fatalf("expected ErrNilDomainEvent, got %v", err)
	}

	// Internal EVENT_DELIVERY jobs must not append lifecycle events.
	if _, err := outbox.AppendJobLifecycle(ctx, spy, domain.EventJobFinished, domain.Job{
		ID:          "deliv-1",
		Kind:        domain.JobKindEventDelivery,
		State:       domain.JobStateSucceeded,
		Attempts:    1,
		MaxAttempts: 3,
	}); !errors.Is(err, domain.ErrInternalJobEventSuppressed) {
		t.Fatalf("expected ErrInternalJobEventSuppressed, got %v", err)
	}

	// Pre-published event must be rejected without calling Appender.
	seq := int64(10)
	if err := outbox.Append(ctx, spy, &domain.DomainEvent{
		PublishedSequence: &seq,
		PublishedAt:       &fixedNow,
		Kind:              domain.EventJobStarted,
		Version:           1,
		AggregateType:     domain.AggregateTypeJob,
		AggregateID:       "job-1",
	}); !errors.Is(err, domain.ErrUnexpectedPublicationMetadata) {
		t.Fatalf("expected ErrUnexpectedPublicationMetadata, got %v", err)
	}
	if spy.called != 0 {
		t.Fatalf("expected spy Appender not to be called on invalid inputs, got %d calls", spy.called)
	}

	// Valid append fills CreatedAt from Outbox clock and leaves publication unset.
	ev, err := outbox.AppendPayload(ctx, spy, domain.EventVolumeCompleted, domain.EventVersion1,
		domain.AggregateTypeVolume, "vol-1", domain.VolumeEventPayloadV1{
			VolumeID:  "vol-1",
			LibraryID: "lib-1",
		})
	if err != nil {
		t.Fatalf("AppendPayload: %v", err)
	}
	if spy.called != 1 || ev.ID != 1 || !ev.CreatedAt.Equal(fixedNow) || ev.IsPublished() {
		t.Fatalf("unexpected appended event: %+v (calls=%d)", ev, spy.called)
	}
}

func TestOutbox_CommitAndRollbackWithCallerWrite(t *testing.T) {
	t.Parallel()
	db, transactor, eventRepo, jobRepo, settingsRepo := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	outbox := events.NewOutbox()

	// 1. Rolled-back transaction: neither caller writes (setting + job) nor outbox event appear.
	rollbackErr := errors.New("caller write failure")
	var rolledBackJobID string
	var rolledBackEventID int64

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		if err := tx.Settings().SetSetting(ctx, &domain.SystemSetting{
			Key:   "test.outbox.rollback",
			Value: []byte(`"should-not-persist"`),
		}); err != nil {
			return err
		}
		job := &domain.Job{
			Kind: domain.JobKindScan,
		}
		created, err := tx.Jobs().Enqueue(ctx, job)
		if err != nil || !created {
			return err
		}
		rolledBackJobID = job.ID

		ev, err := outbox.AppendPayload(ctx, tx.Events(), domain.EventAssetAdded, domain.EventVersion1,
			domain.AggregateTypeAsset, "asset-rb-1", domain.AssetEventPayloadV1{
				AssetID:       "asset-rb-1",
				VolumeID:      "vol-rb-1",
				LibraryID:     "lib-rb-1",
				LibraryRootID: "root-rb-1",
				Format:        domain.MediaFormatCBZ,
				PathDisplay:   "Series/v01.cbz",
			})
		if err != nil {
			return err
		}
		rolledBackEventID = ev.ID
		return rollbackErr
	})
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("WithTx rollback err = %v, want %v", err, rollbackErr)
	}

	if _, err := settingsRepo.GetSetting(ctx, "test.outbox.rollback"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected rolled-back setting to be absent, got %v", err)
	}
	if _, err := jobRepo.GetJobByID(ctx, rolledBackJobID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected rolled-back job %q to be absent, got %v", rolledBackJobID, err)
	}
	if _, err := eventRepo.GetEventByID(ctx, rolledBackEventID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected rolled-back event %d to be absent, got %v", rolledBackEventID, err)
	}

	// 2. Committed transaction: both caller writes and outbox event commit together,
	//    with published_sequence NULL and published_at NULL.
	var committedJobID string
	var committedEventID int64

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		if err := tx.Settings().SetSetting(ctx, &domain.SystemSetting{
			Key:   "test.outbox.commit",
			Value: []byte(`"persisted"`),
		}); err != nil {
			return err
		}
		job := &domain.Job{
			Kind: domain.JobKindScan,
		}
		created, err := tx.Jobs().Enqueue(ctx, job)
		if err != nil || !created {
			return err
		}
		committedJobID = job.ID

		ev, err := outbox.AppendPayload(ctx, tx.Events(), domain.EventAssetAdded, domain.EventVersion1,
			domain.AggregateTypeAsset, "asset-commit-1", domain.AssetEventPayloadV1{
				AssetID:       "asset-commit-1",
				VolumeID:      "vol-commit-1",
				LibraryID:     "lib-commit-1",
				LibraryRootID: "root-commit-1",
				Format:        domain.MediaFormatCBZ,
				PathDisplay:   "Series/v01.cbz",
			})
		if err != nil {
			return err
		}
		committedEventID = ev.ID
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx commit err = %v", err)
	}

	setting, err := settingsRepo.GetSetting(ctx, "test.outbox.commit")
	if err != nil || setting.ValueString() != "persisted" {
		t.Fatalf("expected committed setting, got %+v, err=%v", setting, err)
	}
	if _, err := jobRepo.GetJobByID(ctx, committedJobID); err != nil {
		t.Fatalf("expected committed job %q, got %v", committedJobID, err)
	}

	gotEvent, err := eventRepo.GetEventByID(ctx, committedEventID)
	if err != nil {
		t.Fatalf("expected committed event %d, got %v", committedEventID, err)
	}
	if gotEvent.PublishedSequence != nil || gotEvent.PublishedAt != nil {
		t.Fatalf("expected published_sequence and published_at nil, got seq=%v at=%v",
			gotEvent.PublishedSequence, gotEvent.PublishedAt)
	}

	// Verify directly in SQL that published_sequence and published_at are NULL.
	var pubSeq sql.NullInt64
	var pubAt sql.NullTime
	var kind string
	var version int
	if err := db.QueryRow(ctx,
		`SELECT published_sequence, published_at, kind, version FROM domain_events WHERE id = $1`,
		committedEventID,
	).Scan(&pubSeq, &pubAt, &kind, &version); err != nil {
		t.Fatalf("querying raw domain_events row: %v", err)
	}
	if pubSeq.Valid || pubAt.Valid {
		t.Fatalf("expected SQL published_sequence and published_at NULL, got seq=%v at=%v", pubSeq, pubAt)
	}
	if kind != string(domain.EventAssetAdded) || version != 1 {
		t.Fatalf("unexpected SQL row kind=%q version=%d", kind, version)
	}
}

func TestOutbox_DirectPgxTxCommitAndRollback(t *testing.T) {
	t.Parallel()
	db, _, eventRepo, jobRepo, _ := setupOutboxTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	outbox := events.NewOutbox()

	// Caller holds a raw pgx.Tx and wraps it via postgres.NewEventRepository(tx).
	tx1, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	txJob1 := &domain.Job{Kind: domain.JobKindIntake}
	if _, err := postgres.NewJobRepository(tx1).Enqueue(ctx, txJob1); err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("Enqueue tx1: %v", err)
	}
	txEvent1, err := outbox.AppendPayload(ctx, postgres.NewEventRepository(tx1),
		domain.EventProgressUpdated, domain.EventVersion1,
		domain.AggregateTypeVolume, "vol-pgx-1", domain.ProgressUpdatedPayloadV1{
			UserID:           "usr-1",
			VolumeID:         "vol-pgx-1",
			Locator:          domain.NewPageLocator(0, 12, "asset-1", &domain.LocatorDigest{Kind: domain.DigestKindXXH364Partial, Value: "abcd"}),
			TotalProgression: 0,
			Completed:        false,
			Source:           "komga",
		})
	if err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("AppendPayload tx1: %v", err)
	}
	if err := tx1.Rollback(ctx); err != nil {
		t.Fatalf("Rollback tx1: %v", err)
	}
	if _, err := jobRepo.GetJobByID(ctx, txJob1.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected rolled-back job %q absent, got %v", txJob1.ID, err)
	}
	if _, err := eventRepo.GetEventByID(ctx, txEvent1.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected rolled-back event %d absent, got %v", txEvent1.ID, err)
	}

	// Second caller pgx.Tx commits both.
	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	defer func() { _ = tx2.Rollback(ctx) }()

	txJob2 := &domain.Job{Kind: domain.JobKindIntake}
	if _, err := postgres.NewJobRepository(tx2).Enqueue(ctx, txJob2); err != nil {
		t.Fatalf("Enqueue tx2: %v", err)
	}
	txEvent2, err := outbox.AppendPayload(ctx, postgres.NewEventRepository(tx2),
		domain.EventProgressUpdated, domain.EventVersion1,
		domain.AggregateTypeVolume, "vol-pgx-2", domain.ProgressUpdatedPayloadV1{
			UserID:           "usr-1",
			VolumeID:         "vol-pgx-2",
			Locator:          domain.NewPageLocator(3, 12, "asset-1", &domain.LocatorDigest{Kind: domain.DigestKindXXH364Partial, Value: "abcd"}),
			TotalProgression: 0.25,
			Completed:        false,
			Source:           "komga",
		})
	if err != nil {
		t.Fatalf("AppendPayload tx2: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	if _, err := jobRepo.GetJobByID(ctx, txJob2.ID); err != nil {
		t.Fatalf("expected committed job %q, got %v", txJob2.ID, err)
	}
	got, err := eventRepo.GetEventByID(ctx, txEvent2.ID)
	if err != nil {
		t.Fatalf("expected committed event %d, got %v", txEvent2.ID, err)
	}
	if got.PublishedSequence != nil || got.PublishedAt != nil {
		t.Fatalf("expected unpublished event, got seq=%v at=%v", got.PublishedSequence, got.PublishedAt)
	}
	decoded, err := domain.DecodeEventPayload[domain.ProgressUpdatedPayloadV1](*got)
	if err != nil || decoded.Locator.Page == nil || *decoded.Locator.Page != 3 {
		t.Fatalf("unexpected decoded progress payload: %+v, err=%v", decoded, err)
	}
}
