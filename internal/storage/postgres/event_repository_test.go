package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func setupEventTestDB(t *testing.T) (*testdb.TestDB, *postgres.EventRepository) {
	t.Helper()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("setupEventTestDB: applying migrations: %v", err)
	}

	return db, postgres.NewEventRepository(db.Pool)
}

func TestEventRepository_RejectsPoolBackedAppend(t *testing.T) {
	t.Parallel()
	db, poolRepo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if poolRepo.InTx() {
		t.Fatal("expected pool-backed EventRepository.InTx() == false")
	}

	ev, err := domain.NewDomainEvent(
		domain.EventJobStarted,
		domain.EventVersion1,
		domain.AggregateTypeJob,
		"job-pool-rejected",
		domain.JobEventPayloadV1{
			JobID:    "job-pool-rejected",
			Kind:     domain.JobKindScan,
			State:    domain.JobStateRunning,
			Attempts: 1,
		},
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("NewDomainEvent: %v", err)
	}

	if err := poolRepo.AppendEvent(ctx, &ev); !errors.Is(err, storage.ErrTransactionRequired) {
		t.Fatalf("expected ErrTransactionRequired for pool-backed AppendEvent, got %v", err)
	}

	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1`, "job-pool-rejected").Scan(&count); err != nil {
		t.Fatalf("counting domain_events: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 domain_events rows after rejected pool append, got %d", count)
	}
}

func TestEventRepository_AppendVisibilityRollbackAndCommit(t *testing.T) {
	t.Parallel()
	db, repo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Rollback: an event appended inside a rolled-back transaction never appears.
	sentinelErr := errors.New("abort transaction")
	var rolledBackID int64
	err := repo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		if !txRepo.InTx() {
			t.Fatal("expected tx-scoped EventRepository.InTx() == true")
		}
		ev, err := domain.NewDomainEvent(
			domain.EventJobStarted,
			domain.EventVersion1,
			domain.AggregateTypeJob,
			"job-rollback-1",
			domain.JobEventPayloadV1{
				JobID:    "job-rollback-1",
				Kind:     domain.JobKindScan,
				State:    domain.JobStateRunning,
				Attempts: 1,
			},
			time.Now().UTC(),
		)
		if err != nil {
			return err
		}
		if err := txRepo.AppendEvent(ctx, &ev); err != nil {
			return err
		}
		if ev.ID <= 0 {
			t.Fatalf("expected positive generated event ID inside tx, got %d", ev.ID)
		}
		rolledBackID = ev.ID

		// Uncommitted event must not be visible from outside the transaction.
		if _, err := repo.GetEventByID(ctx, rolledBackID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for uncommitted event from pool, got %v", err)
		}
		return sentinelErr
	})
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("WithTx rollback err = %v, want %v", err, sentinelErr)
	}
	if _, err := repo.GetEventByID(ctx, rolledBackID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("rolled-back event %d must never appear, got err=%v", rolledBackID, err)
	}

	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1`, "job-rollback-1").Scan(&count); err != nil {
		t.Fatalf("counting rolled-back domain_events: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 domain_events rows after rollback, got %d", count)
	}

	// 2. Commit: committed event appears with published_sequence NULL and published_at NULL.
	var committed domain.DomainEvent
	err = repo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		ev, err := domain.NewDomainEvent(
			domain.EventSeriesMetadataChanged,
			domain.EventVersion1,
			domain.AggregateTypeSeries,
			"ser-commit-1",
			domain.SeriesEventPayloadV1{
				SeriesID:  "ser-commit-1",
				LibraryID: "lib-1",
			},
			time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC),
		)
		if err != nil {
			return err
		}
		if err := txRepo.AppendEvent(ctx, &ev); err != nil {
			return err
		}
		committed = ev
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx commit err = %v", err)
	}

	got, err := repo.GetEventByID(ctx, committed.ID)
	if err != nil {
		t.Fatalf("GetEventByID(%d): %v", committed.ID, err)
	}
	if got.PublishedSequence != nil || got.PublishedAt != nil || got.IsPublished() {
		t.Fatalf("expected unpublished event with nil published_sequence/published_at, got seq=%v at=%v",
			got.PublishedSequence, got.PublishedAt)
	}
	if got.Kind != domain.EventSeriesMetadataChanged || got.Version != 1 || got.AggregateType != domain.AggregateTypeSeries || got.AggregateID != "ser-commit-1" {
		t.Fatalf("unexpected persisted event metadata: %+v", got)
	}
	decoded, err := domain.DecodeEventPayload[domain.SeriesEventPayloadV1](*got)
	if err != nil || decoded.SeriesID != "ser-commit-1" || decoded.LibraryID != "lib-1" {
		t.Fatalf("unexpected decoded payload: %+v, err=%v", decoded, err)
	}
}

func TestEventRepository_NestedWithTxSavepoints(t *testing.T) {
	t.Parallel()
	_, repo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var outerID, innerRolledBackID, outerRolledBackInnerID int64
	innerErr := errors.New("inner savepoint rollback")
	outerErr := errors.New("outer rollback")

	// Inner savepoint rolls back while outer commits.
	err := repo.WithTx(ctx, func(outer storage.EventRepository) error {
		evOuter, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "asset-outer", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := outer.AppendEvent(ctx, &evOuter); err != nil {
			return err
		}
		outerID = evOuter.ID

		if err := outer.WithTx(ctx, func(inner storage.EventRepository) error {
			evInner, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "asset-inner-rb", nil, time.Now().UTC())
			if err != nil {
				return err
			}
			if err := inner.AppendEvent(ctx, &evInner); err != nil {
				return err
			}
			innerRolledBackID = evInner.ID
			return innerErr
		}); !errors.Is(err, innerErr) {
			t.Fatalf("expected innerErr, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer commit err: %v", err)
	}
	if _, err := repo.GetEventByID(ctx, outerID); err != nil {
		t.Fatalf("expected outer event %d to be committed, got %v", outerID, err)
	}
	if _, err := repo.GetEventByID(ctx, innerRolledBackID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected inner rolled-back event %d to be absent, got %v", innerRolledBackID, err)
	}

	// Inner savepoint commits, then outer rolls back -> inner must not persist.
	err = repo.WithTx(ctx, func(outer storage.EventRepository) error {
		if err := outer.WithTx(ctx, func(inner storage.EventRepository) error {
			evInner, err := domain.NewDomainEvent(domain.EventVolumeRemoved, 1, domain.AggregateTypeVolume, "vol-inner-commit", nil, time.Now().UTC())
			if err != nil {
				return err
			}
			if err := inner.AppendEvent(ctx, &evInner); err != nil {
				return err
			}
			outerRolledBackInnerID = evInner.ID
			return nil
		}); err != nil {
			return err
		}
		return outerErr
	})
	if !errors.Is(err, outerErr) {
		t.Fatalf("expected outerErr, got %v", err)
	}
	if _, err := repo.GetEventByID(ctx, outerRolledBackInnerID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected event %d in rolled-back outer tx to be absent, got %v", outerRolledBackInnerID, err)
	}
}

func TestEventRepository_CancelledInnerSavepointCannotSurviveOuterCommit(t *testing.T) {
	t.Parallel()
	db, repo := setupEventTestDB(t)

	outerCtx, outerCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer outerCancel()

	var outerID, cancelledErrID, cancelledCommitID int64

	err := repo.WithTx(outerCtx, func(outer storage.EventRepository) error {
		evOuter, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "asset-outer-survives", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := outer.AppendEvent(outerCtx, &evOuter); err != nil {
			return err
		}
		outerID = evOuter.ID

		// Case 1: inner context is cancelled after writing, and callback returns ctx.Err().
		innerCtx1, innerCancel1 := context.WithCancel(outerCtx)
		err1 := outer.WithTx(innerCtx1, func(inner storage.EventRepository) error {
			ev, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "asset-inner-cancel-err", nil, time.Now().UTC())
			if err != nil {
				return err
			}
			if err := inner.AppendEvent(innerCtx1, &ev); err != nil {
				return err
			}
			cancelledErrID = ev.ID
			innerCancel1()
			return innerCtx1.Err()
		})
		if !errors.Is(err1, context.Canceled) {
			t.Fatalf("expected context.Canceled from inner savepoint 1, got %v", err1)
		}

		// Case 2: inner context is cancelled after writing, and callback returns nil so Commit fails.
		innerCtx2, innerCancel2 := context.WithCancel(outerCtx)
		err2 := outer.WithTx(innerCtx2, func(inner storage.EventRepository) error {
			ev, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "asset-inner-cancel-commit", nil, time.Now().UTC())
			if err != nil {
				return err
			}
			if err := inner.AppendEvent(innerCtx2, &ev); err != nil {
				return err
			}
			cancelledCommitID = ev.ID
			innerCancel2()
			return nil
		})
		if !errors.Is(err2, context.Canceled) {
			t.Fatalf("expected context.Canceled from inner savepoint 2 commit, got %v", err2)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("outer commit failed: %v", err)
	}

	if _, err := repo.GetEventByID(outerCtx, outerID); err != nil {
		t.Fatalf("expected outer event %d to commit, got %v", outerID, err)
	}
	if _, err := repo.GetEventByID(outerCtx, cancelledErrID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cancelled inner event %d survived outer commit! err=%v", cancelledErrID, err)
	}
	if _, err := repo.GetEventByID(outerCtx, cancelledCommitID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cancelled-on-commit inner event %d survived outer commit! err=%v", cancelledCommitID, err)
	}

	var leakedCount int
	if err := db.QueryRow(outerCtx,
		`SELECT COUNT(*) FROM domain_events WHERE aggregate_id IN ('asset-inner-cancel-err', 'asset-inner-cancel-commit')`,
	).Scan(&leakedCount); err != nil {
		t.Fatalf("counting leaked events: %v", err)
	}
	if leakedCount != 0 {
		t.Fatalf("expected 0 leaked inner events after outer commit, got %d", leakedCount)
	}
}

type cancelDuringSavepointCommitBeginner struct {
	pgx.Tx
	cancel context.CancelFunc
}

func (b *cancelDuringSavepointCommitBeginner) Begin(ctx context.Context) (pgx.Tx, error) {
	sp, err := b.Tx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &cancelDuringSavepointCommitTx{Tx: sp, cancel: b.cancel}, nil
}

type cancelDuringSavepointCommitTx struct {
	pgx.Tx
	cancel context.CancelFunc
}

func (t *cancelDuringSavepointCommitTx) Commit(ctx context.Context) error {
	t.cancel()
	return t.Tx.Commit(ctx)
}

func TestEventRepository_CancelledReleaseSavepointAbortsParentBeforeCommit(t *testing.T) {
	t.Parallel()
	db, poolRepo := setupEventTestDB(t)

	outerCtx, outerCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer outerCancel()

	outerTx, err := db.Begin(outerCtx)
	if err != nil {
		t.Fatalf("Begin outerTx: %v", err)
	}
	defer func() { _ = outerTx.Rollback(context.Background()) }()

	innerCtx, innerCancel := context.WithCancel(outerCtx)
	defer innerCancel()

	wrappedOuter := &cancelDuringSavepointCommitBeginner{Tx: outerTx, cancel: innerCancel}
	innerErr := postgres.NewTransactor(wrappedOuter).WithTx(innerCtx, func(innerTx storage.Tx) error {
		ev, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "asset-release-sp-cancel", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		return innerTx.Events().AppendEvent(innerCtx, &ev)
	})
	if !errors.Is(innerErr, context.Canceled) {
		t.Fatalf("expected context.Canceled from failed RELEASE SAVEPOINT, got %v", innerErr)
	}

	// Even if the caller ignores innerErr and tries to commit outerTx, savepoint cleanup
	// aborted outerTx so the inner event can never be committed.
	if err := outerTx.Commit(outerCtx); err == nil {
		t.Fatal("expected outerTx.Commit to fail after unconfirmed savepoint cleanup, but it succeeded")
	}

	var count int
	if err := db.QueryRow(outerCtx,
		`SELECT COUNT(*) FROM domain_events WHERE aggregate_id = 'asset-release-sp-cancel'`,
	).Scan(&count); err != nil {
		t.Fatalf("counting events: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after aborted parent commit, got %d", count)
	}
	if _, err := poolRepo.GetEventByID(outerCtx, 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestEventRepository_Guards(t *testing.T) {
	t.Parallel()
	_, repo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := repo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		if err := txRepo.AppendEvent(ctx, nil); err == nil {
			t.Fatal("expected error for nil event")
		}

		seq := int64(5)
		now := time.Now().UTC()
		if err := txRepo.AppendEvent(ctx, &domain.DomainEvent{
			PublishedSequence: &seq,
			PublishedAt:       &now,
			Kind:              domain.EventJobStarted,
			Version:           1,
			AggregateType:     domain.AggregateTypeJob,
			AggregateID:       "job-1",
		}); !errors.Is(err, domain.ErrUnexpectedPublicationMetadata) {
			t.Fatalf("expected ErrUnexpectedPublicationMetadata, got %v", err)
		}

		if err := txRepo.AppendEvent(ctx, &domain.DomainEvent{
			ID:            42,
			Kind:          domain.EventJobStarted,
			Version:       1,
			AggregateType: domain.AggregateTypeJob,
			AggregateID:   "job-1",
		}); !errors.Is(err, domain.ErrUnexpectedEventID) {
			t.Fatalf("expected ErrUnexpectedEventID, got %v", err)
		}

		if err := txRepo.AppendEvent(ctx, &domain.DomainEvent{
			Kind:          domain.EventJobStarted,
			Version:       math.MaxInt32 + 1,
			AggregateType: domain.AggregateTypeJob,
			AggregateID:   "job-1",
		}); !errors.Is(err, domain.ErrInvalidEventVersion) {
			t.Fatalf("expected ErrInvalidEventVersion, got %v", err)
		}

		if err := txRepo.AppendEvent(ctx, &domain.DomainEvent{
			Kind:          domain.EventJobStarted,
			Version:       1,
			AggregateType: domain.AggregateTypeJob,
			AggregateID:   "job-1",
			Payload:       json.RawMessage(`{broken`),
		}); !errors.Is(err, domain.ErrInvalidEventPayload) {
			t.Fatalf("expected ErrInvalidEventPayload, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx guards err: %v", err)
	}
}
