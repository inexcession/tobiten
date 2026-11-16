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

func TestEventRepository_AppendVisibilityRollbackAndCommit(t *testing.T) {
	t.Parallel()
	db, repo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Rollback: an event appended inside a rolled-back transaction never appears.
	sentinelErr := errors.New("abort transaction")
	var rolledBackID int64
	err := repo.WithTx(ctx, func(txRepo storage.EventRepository) error {
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

func TestEventRepository_Guards(t *testing.T) {
	t.Parallel()
	_, repo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := repo.AppendEvent(ctx, nil); err == nil {
		t.Fatal("expected error for nil event")
	}

	seq := int64(5)
	now := time.Now().UTC()
	if err := repo.AppendEvent(ctx, &domain.DomainEvent{
		PublishedSequence: &seq,
		PublishedAt:       &now,
		Kind:              domain.EventJobStarted,
		Version:           1,
		AggregateType:     domain.AggregateTypeJob,
		AggregateID:       "job-1",
	}); !errors.Is(err, domain.ErrUnexpectedPublicationMetadata) {
		t.Fatalf("expected ErrUnexpectedPublicationMetadata, got %v", err)
	}

	if err := repo.AppendEvent(ctx, &domain.DomainEvent{
		ID:            42,
		Kind:          domain.EventJobStarted,
		Version:       1,
		AggregateType: domain.AggregateTypeJob,
		AggregateID:   "job-1",
	}); !errors.Is(err, domain.ErrUnexpectedEventID) {
		t.Fatalf("expected ErrUnexpectedEventID, got %v", err)
	}

	if err := repo.AppendEvent(ctx, &domain.DomainEvent{
		Kind:          domain.EventJobStarted,
		Version:       math.MaxInt32 + 1,
		AggregateType: domain.AggregateTypeJob,
		AggregateID:   "job-1",
	}); !errors.Is(err, domain.ErrInvalidEventVersion) {
		t.Fatalf("expected ErrInvalidEventVersion, got %v", err)
	}

	if err := repo.AppendEvent(ctx, &domain.DomainEvent{
		Kind:          domain.EventJobStarted,
		Version:       1,
		AggregateType: domain.AggregateTypeJob,
		AggregateID:   "job-1",
		Payload:       json.RawMessage(`{broken`),
	}); !errors.Is(err, domain.ErrInvalidEventPayload) {
		t.Fatalf("expected ErrInvalidEventPayload, got %v", err)
	}
}
