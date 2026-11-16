package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

type twoLevelRollbackControl struct {
	errByLevel    map[int]error
	rollbackCalls map[int]int
	commitCalls   map[int]int
}

type twoLevelFaultBeginner struct {
	postgres.TxBeginner
	ctrl *twoLevelRollbackControl
}

func (b *twoLevelFaultBeginner) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := b.TxBeginner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &twoLevelFaultTx{Tx: tx, level: 0, ctrl: b.ctrl}, nil
}

type twoLevelFaultTx struct {
	pgx.Tx
	level int
	ctrl  *twoLevelRollbackControl
}

func (t *twoLevelFaultTx) Begin(ctx context.Context) (pgx.Tx, error) {
	sp, err := t.Tx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &twoLevelFaultTx{Tx: sp, level: t.level + 1, ctrl: t.ctrl}, nil
}

func (t *twoLevelFaultTx) Commit(ctx context.Context) error {
	t.ctrl.commitCalls[t.level]++
	return t.Tx.Commit(ctx)
}

func (t *twoLevelFaultTx) Rollback(ctx context.Context) error {
	t.ctrl.rollbackCalls[t.level]++
	if err := t.ctrl.errByLevel[t.level]; err != nil {
		// Fail before sending ROLLBACK / ROLLBACK TO SAVEPOINT SQL to PostgreSQL.
		return err
	}
	return t.Tx.Rollback(ctx)
}

func TestEventRepository_TwoLevelSavepointRollbackFailurePreventsRootCommit(t *testing.T) {
	t.Parallel()
	db, poolRepo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Both inner (level 2) and parent (level 1) savepoint rollbacks fail before SQL is sent;
	//    even when parent and root catch errors and attempt to commit, root commit is prevented
	//    and none of root, parent, or inner events survive.
	errInnerRB := errors.New("forced inner savepoint rollback failure")
	errParentRB := errors.New("forced parent savepoint rollback failure")
	ctrlBothFail := &twoLevelRollbackControl{
		errByLevel:    map[int]error{1: errParentRB, 2: errInnerRB},
		rollbackCalls: make(map[int]int),
		commitCalls:   make(map[int]int),
	}

	var observedParentErr error
	rootErr := postgres.NewTransactor(&twoLevelFaultBeginner{TxBeginner: db.Pool, ctrl: ctrlBothFail}).WithTx(ctx, func(root storage.Tx) error {
		rootEv, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "twolevel-root-aborted", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := root.Events().AppendEvent(ctx, &rootEv); err != nil {
			return err
		}

		observedParentErr = root.WithTx(ctx, func(parent storage.Tx) error {
			parentEv, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "twolevel-parent-aborted", nil, time.Now().UTC())
			if err != nil {
				return err
			}
			if err := parent.Events().AppendEvent(ctx, &parentEv); err != nil {
				return err
			}

			innerErr := parent.WithTx(ctx, func(inner storage.Tx) error {
				innerEv, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "twolevel-inner-aborted", nil, time.Now().UTC())
				if err != nil {
					return err
				}
				if err := inner.Events().AppendEvent(ctx, &innerEv); err != nil {
					return err
				}
				return errors.New("inner callback failure")
			})
			if innerErr == nil {
				t.Fatal("expected inner savepoint error")
			}
			// Parent swallows innerErr and attempts to commit.
			return nil
		})
		if observedParentErr == nil {
			t.Fatal("expected parent savepoint to fail when inner rollback failed")
		}
		// Root swallows parent error and attempts to commit.
		return nil
	})
	if rootErr == nil {
		t.Fatal("expected root WithTx to fail instead of committing after unconfirmed two-level savepoint rollback")
	}
	if ctrlBothFail.commitCalls[0] != 0 || ctrlBothFail.commitCalls[1] != 0 || ctrlBothFail.commitCalls[2] != 0 {
		t.Fatalf("expected zero commits at all levels, got %+v", ctrlBothFail.commitCalls)
	}
	if ctrlBothFail.rollbackCalls[2] == 0 || ctrlBothFail.rollbackCalls[1] == 0 || ctrlBothFail.rollbackCalls[0] == 0 {
		t.Fatalf("expected rollback escalation across all levels, got %+v", ctrlBothFail.rollbackCalls)
	}

	var leakedCount int
	if err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM domain_events WHERE aggregate_id IN ('twolevel-root-aborted', 'twolevel-parent-aborted', 'twolevel-inner-aborted')`,
	).Scan(&leakedCount); err != nil {
		t.Fatalf("counting leaked events: %v", err)
	}
	if leakedCount != 0 {
		t.Fatalf("expected 0 committed events after root abort, got %d", leakedCount)
	}

	// 2. Inner (level 2) rollback fails before SQL, parent (level 1) savepoint rollback succeeds:
	//    parent cannot commit, but root preserves outer commit cleanly without parent/inner writes.
	ctrlParentRecovers := &twoLevelRollbackControl{
		errByLevel:    map[int]error{2: errInnerRB},
		rollbackCalls: make(map[int]int),
		commitCalls:   make(map[int]int),
	}
	var rootSurvivesID int64
	err := postgres.NewTransactor(&twoLevelFaultBeginner{TxBeginner: db.Pool, ctrl: ctrlParentRecovers}).WithTx(ctx, func(root storage.Tx) error {
		rootEv, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "twolevel-root-parent-ok", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := root.Events().AppendEvent(ctx, &rootEv); err != nil {
			return err
		}
		rootSurvivesID = rootEv.ID

		parentErr := root.WithTx(ctx, func(parent storage.Tx) error {
			parentEv, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "twolevel-parent-rolledback", nil, time.Now().UTC())
			if err != nil {
				return err
			}
			if err := parent.Events().AppendEvent(ctx, &parentEv); err != nil {
				return err
			}
			_ = parent.WithTx(ctx, func(inner storage.Tx) error {
				innerEv, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "twolevel-inner-rolledback", nil, time.Now().UTC())
				if err != nil {
					return err
				}
				if err := inner.Events().AppendEvent(ctx, &innerEv); err != nil {
					return err
				}
				return errors.New("inner failure")
			})
			return nil
		})
		if parentErr == nil {
			t.Fatal("expected parent to fail when inner rollback failed")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected root commit to succeed after parent savepoint rolled back cleanly, got %v", err)
	}
	if ctrlParentRecovers.commitCalls[0] != 1 {
		t.Fatalf("expected root commit count 1, got %+v", ctrlParentRecovers.commitCalls)
	}
	if _, err := poolRepo.GetEventByID(ctx, rootSurvivesID); err != nil {
		t.Fatalf("expected root event %d to commit, got %v", rootSurvivesID, err)
	}
	var rolledBackChildren int
	if err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM domain_events WHERE aggregate_id IN ('twolevel-parent-rolledback', 'twolevel-inner-rolledback')`,
	).Scan(&rolledBackChildren); err != nil {
		t.Fatalf("counting rolled-back child events: %v", err)
	}
	if rolledBackChildren != 0 {
		t.Fatalf("expected 0 child events after parent savepoint rollback, got %d", rolledBackChildren)
	}

	// 3. Successful savepoint rollback at level 2 preserves both parent and root commits.
	ctrlSuccess := &twoLevelRollbackControl{
		errByLevel:    map[int]error{},
		rollbackCalls: make(map[int]int),
		commitCalls:   make(map[int]int),
	}
	var committedRootID, committedParentID, rolledBackInnerID int64
	err = postgres.NewTransactor(&twoLevelFaultBeginner{TxBeginner: db.Pool, ctrl: ctrlSuccess}).WithTx(ctx, func(root storage.Tx) error {
		rootEv, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "twolevel-root-ok", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := root.Events().AppendEvent(ctx, &rootEv); err != nil {
			return err
		}
		committedRootID = rootEv.ID

		return root.WithTx(ctx, func(parent storage.Tx) error {
			parentEv, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "twolevel-parent-ok", nil, time.Now().UTC())
			if err != nil {
				return err
			}
			if err := parent.Events().AppendEvent(ctx, &parentEv); err != nil {
				return err
			}
			committedParentID = parentEv.ID

			innerErr := parent.WithTx(ctx, func(inner storage.Tx) error {
				innerEv, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "twolevel-inner-rb-ok", nil, time.Now().UTC())
				if err != nil {
					return err
				}
				if err := inner.Events().AppendEvent(ctx, &innerEv); err != nil {
					return err
				}
				rolledBackInnerID = innerEv.ID
				return errors.New("expected inner rollback")
			})
			if innerErr == nil {
				t.Fatal("expected inner error")
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("expected outer commit to succeed after successful savepoint rollback, got %v", err)
	}
	if ctrlSuccess.commitCalls[0] != 1 || ctrlSuccess.commitCalls[1] != 1 {
		t.Fatalf("expected root and parent commits to succeed, got %+v", ctrlSuccess.commitCalls)
	}
	if _, err := poolRepo.GetEventByID(ctx, committedRootID); err != nil {
		t.Fatalf("expected root event %d to commit, got %v", committedRootID, err)
	}
	if _, err := poolRepo.GetEventByID(ctx, committedParentID); err != nil {
		t.Fatalf("expected parent event %d to commit, got %v", committedParentID, err)
	}
	if _, err := poolRepo.GetEventByID(ctx, rolledBackInnerID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected rolled-back inner event %d to be absent, got %v", rolledBackInnerID, err)
	}
}

type sqlTracer struct {
	mu  sync.Mutex
	sql []string
}

func (t *sqlTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	t.mu.Lock()
	t.sql = append(t.sql, data.SQL)
	t.mu.Unlock()
	return ctx
}

func (t *sqlTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *sqlTracer) statements() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.sql)
}

func connectTracedConn(ctx context.Context, t *testing.T, dbURL string) (*pgx.Conn, *sqlTracer) {
	t.Helper()
	tracer := &sqlTracer{}
	cfg, err := pgx.ParseConfig(dbURL)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	cfg.Tracer = tracer
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("ConnectConfig: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close(context.Background())
	})
	return conn, tracer
}

func TestEventRepository_CallerSuppliedPgxSavepointRollbackFailurePreventsRawRootCommit(t *testing.T) {
	t.Parallel()
	db, poolRepo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	errInnerRB := errors.New("forced inner savepoint rollback failure")
	errCallerSPRB := errors.New("forced caller savepoint rollback failure")

	runFailureCase := func(t *testing.T, name string, invoke func(sp pgx.Tx) error) {
		t.Helper()
		conn, tracer := connectTracedConn(ctx, t, db.URL)

		// Caller owns a raw pgx root tx and a caller-supplied pgx savepoint (level 1).
		rawRoot, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("%s: rawRoot Begin: %v", name, err)
		}
		defer func() { _ = rawRoot.Rollback(context.Background()) }()

		rawSP1, err := rawRoot.Begin(ctx)
		if err != nil {
			t.Fatalf("%s: rawSP1 Begin: %v", name, err)
		}

		// Wrap rawSP1 at level 1 (caller-supplied savepoint) and level 2 (inner savepoint)
		// so both Rollback calls fail before sending any SQL.
		ctrl := &twoLevelRollbackControl{
			errByLevel:    map[int]error{1: errCallerSPRB, 2: errInnerRB},
			rollbackCalls: make(map[int]int),
			commitCalls:   make(map[int]int),
		}
		wrappedSP1 := &twoLevelFaultTx{Tx: rawSP1, level: 1, ctrl: ctrl}

		txErr := invoke(wrappedSP1)
		if txErr == nil {
			t.Fatalf("%s: expected error from failed inner+caller-savepoint rollback, got nil", name)
		}
		if ctrl.rollbackCalls[2] == 0 || ctrl.rollbackCalls[1] == 0 {
			t.Fatalf("%s: expected both level-2 and level-1 Rollback attempts, got %+v", name, ctrl.rollbackCalls)
		}
		if ctrl.commitCalls[2] != 0 {
			t.Fatalf("%s: expected zero inner commits, got %+v", name, ctrl.commitCalls)
		}

		// Verify neither rollback sent ROLLBACK / ROLLBACK TO SAVEPOINT SQL to PostgreSQL.
		for _, stmt := range tracer.statements() {
			lower := strings.ToLower(strings.TrimSpace(stmt))
			if strings.HasPrefix(lower, "rollback") {
				t.Fatalf("%s: unexpected rollback SQL sent on wire: %q (all=%v)", name, stmt, tracer.statements())
			}
		}

		// Even if the caller ignores txErr and attempts to commit the raw pgx root, it must fail.
		if commitErr := rawRoot.Commit(ctx); commitErr == nil {
			t.Fatalf("%s: expected rawRoot.Commit to fail after unconfirmed savepoint rollback, but it succeeded", name)
		}

		var count int
		if err := db.QueryRow(ctx,
			`SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1`, name,
		).Scan(&count); err != nil {
			t.Fatalf("%s: counting domain_events: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s: expected 0 committed events after raw root commit failure, got %d", name, count)
		}
	}

	runFailureCase(t, "caller-sp-new-transactor", func(sp pgx.Tx) error {
		return postgres.NewTransactor(sp).WithTx(ctx, func(inner storage.Tx) error {
			ev, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "caller-sp-new-transactor", nil, time.Now().UTC())
			if err != nil {
				return err
			}
			if err := inner.Events().AppendEvent(ctx, &ev); err != nil {
				return err
			}
			return errors.New("inner transactor failure")
		})
	})

	runFailureCase(t, "caller-sp-new-event-repo", func(sp pgx.Tx) error {
		return postgres.NewEventRepository(sp).WithTx(ctx, func(inner storage.EventRepository) error {
			ev, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "caller-sp-new-event-repo", nil, time.Now().UTC())
			if err != nil {
				return err
			}
			if err := inner.AppendEvent(ctx, &ev); err != nil {
				return err
			}
			return errors.New("inner event-repo failure")
		})
	})

	// Preserve raw pgx direct append and outer commit after confirmed savepoint rollback
	// across both NewTransactor(sp1) and NewEventRepository(sp1).
	conn, tracer := connectTracedConn(ctx, t, db.URL)
	rawRoot, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("preservation rawRoot Begin: %v", err)
	}
	defer func() { _ = rawRoot.Rollback(context.Background()) }()

	directRootEv, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "raw-pgx-direct-root", nil, time.Now().UTC())
	if err != nil {
		t.Fatalf("NewDomainEvent directRootEv: %v", err)
	}
	if err := postgres.NewEventRepository(rawRoot).AppendEvent(ctx, &directRootEv); err != nil {
		t.Fatalf("rawRoot direct AppendEvent: %v", err)
	}

	rawSP1, err := rawRoot.Begin(ctx)
	if err != nil {
		t.Fatalf("preservation rawSP1 Begin: %v", err)
	}
	directSP1Ev, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "raw-pgx-direct-sp1", nil, time.Now().UTC())
	if err != nil {
		t.Fatalf("NewDomainEvent directSP1Ev: %v", err)
	}
	if err := postgres.NewEventRepository(rawSP1).AppendEvent(ctx, &directSP1Ev); err != nil {
		t.Fatalf("rawSP1 direct AppendEvent: %v", err)
	}

	var rolledBackTransactorEvID, rolledBackRepoEvID int64
	if err := postgres.NewTransactor(rawSP1).WithTx(ctx, func(inner storage.Tx) error {
		ev, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "raw-sp-confirmed-rb-transactor", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := inner.Events().AppendEvent(ctx, &ev); err != nil {
			return err
		}
		rolledBackTransactorEvID = ev.ID
		return errors.New("confirm inner rollback via transactor")
	}); err == nil {
		t.Fatal("expected inner transactor error")
	}

	if err := postgres.NewEventRepository(rawSP1).WithTx(ctx, func(inner storage.EventRepository) error {
		ev, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "raw-sp-confirmed-rb-repo", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := inner.AppendEvent(ctx, &ev); err != nil {
			return err
		}
		rolledBackRepoEvID = ev.ID
		return errors.New("confirm inner rollback via event repo")
	}); err == nil {
		t.Fatal("expected inner event repo error")
	}

	if err := rawSP1.Commit(ctx); err != nil {
		t.Fatalf("rawSP1.Commit after confirmed inner rollbacks: %v", err)
	}
	if err := rawRoot.Commit(ctx); err != nil {
		t.Fatalf("rawRoot.Commit after confirmed inner rollbacks: %v", err)
	}

	var sawRollbackToSavepoint bool
	for _, stmt := range tracer.statements() {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(stmt)), "rollback to savepoint") {
			sawRollbackToSavepoint = true
			break
		}
	}
	if !sawRollbackToSavepoint {
		t.Fatalf("expected confirmed rollback to savepoint SQL on wire, got %v", tracer.statements())
	}

	for _, id := range []int64{directRootEv.ID, directSP1Ev.ID} {
		got, err := poolRepo.GetEventByID(ctx, id)
		if err != nil {
			t.Fatalf("expected direct raw-pgx event %d to commit, got %v", id, err)
		}
		if got.PublishedSequence != nil || got.PublishedAt != nil {
			t.Fatalf("expected direct raw-pgx event %d unpublished, got seq=%v at=%v", id, got.PublishedSequence, got.PublishedAt)
		}
	}
	for _, id := range []int64{rolledBackTransactorEvID, rolledBackRepoEvID} {
		if _, err := poolRepo.GetEventByID(ctx, id); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected rolled-back inner event %d absent after rawRoot commit, got %v", id, err)
		}
	}
}

func TestEventRepository_CallerOwnedSavepointRollback(t *testing.T) {
	t.Parallel()
	db, poolRepo := setupEventTestDB(t)

	entryPoints := []struct {
		name   string
		withTx func(context.Context, pgx.Tx, func(storage.EventRepository) error) error
	}{
		{
			name: "NewTransactor",
			withTx: func(ctx context.Context, tx pgx.Tx, fn func(storage.EventRepository) error) error {
				return postgres.NewTransactor(tx).WithTx(ctx, func(inner storage.Tx) error {
					return fn(inner.Events())
				})
			},
		},
		{
			name: "NewEventRepository",
			withTx: func(ctx context.Context, tx pgx.Tx, fn func(storage.EventRepository) error) error {
				return postgres.NewEventRepository(tx).WithTx(ctx, fn)
			},
		},
	}
	errCallback := errors.New("inner callback failed")
	errInnerRollback := errors.New("inner rollback failed before SQL")
	errParentRollback := errors.New("parent rollback failed before SQL")
	cases := []struct {
		name           string
		rollbackErrors map[int]error
		wantRootCommit bool
	}{
		{"both_rollbacks_fail", map[int]error{1: errParentRollback, 2: errInnerRollback}, false},
		{"parent_rollback_succeeds", map[int]error{2: errInnerRollback}, true},
		{"inner_rollback_succeeds", nil, true},
	}
	for _, entry := range entryPoints {
		t.Run(entry.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()

					root, err := db.Begin(ctx)
					if err != nil {
						t.Fatalf("beginning caller transaction: %v", err)
					}
					defer func() { _ = root.Rollback(context.Background()) }()

					rootEvent, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "caller-root", nil, time.Time{})
					if err != nil {
						t.Fatal(err)
					}
					if err := postgres.NewEventRepository(root).AppendEvent(ctx, &rootEvent); err != nil {
						t.Fatalf("appending directly to caller transaction: %v", err)
					}

					savepoint, err := root.Begin(ctx)
					if err != nil {
						t.Fatalf("beginning caller savepoint: %v", err)
					}
					ctrl := &twoLevelRollbackControl{
						errByLevel:    tc.rollbackErrors,
						rollbackCalls: make(map[int]int),
					}
					wrapped := &twoLevelFaultTx{Tx: savepoint, level: 1, ctrl: ctrl}
					innerEvent, err := domain.NewDomainEvent(domain.EventAssetRemoved, 1, domain.AggregateTypeAsset, "caller-inner", nil, time.Time{})
					if err != nil {
						t.Fatal(err)
					}
					err = entry.withTx(ctx, wrapped, func(repo storage.EventRepository) error {
						if err := repo.AppendEvent(ctx, &innerEvent); err != nil {
							return err
						}
						return errCallback
					})
					if !errors.Is(err, errCallback) || innerEvent.ID <= 0 {
						t.Fatalf("expected inner callback error after appending an event, got event ID %d and error %v", innerEvent.ID, err)
					}
					for _, rollbackErr := range tc.rollbackErrors {
						if !errors.Is(err, rollbackErr) {
							t.Fatalf("expected rollback error %v, got %v", rollbackErr, err)
						}
					}
					wantParentRollbacks := 1
					if tc.rollbackErrors[2] == nil {
						wantParentRollbacks = 0
						if err := savepoint.Commit(ctx); err != nil {
							t.Fatalf("releasing caller savepoint after confirmed inner rollback: %v", err)
						}
					}
					if ctrl.rollbackCalls[2] != 1 || ctrl.rollbackCalls[1] != wantParentRollbacks {
						t.Fatalf("expected one inner and %d parent rollback attempts, got %v", wantParentRollbacks, ctrl.rollbackCalls)
					}

					// Use the caller's raw pgx handle to commit regardless of the callback error.
					if err := root.Commit(ctx); (err == nil) != tc.wantRootCommit {
						t.Fatalf("caller root commit should succeed = %t, got %v", tc.wantRootCommit, err)
					}
					if _, err := poolRepo.GetEventByID(ctx, innerEvent.ID); !errors.Is(err, storage.ErrNotFound) {
						t.Fatalf("expected inner event to be absent, got %v", err)
					}
					_, err = poolRepo.GetEventByID(ctx, rootEvent.ID)
					if tc.wantRootCommit && err != nil {
						t.Fatalf("expected caller root event to commit, got %v", err)
					}
					if !tc.wantRootCommit && !errors.Is(err, storage.ErrNotFound) {
						t.Fatalf("expected caller root event to be absent, got %v", err)
					}
				})
			}
		})
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

func TestEventRepository_PublishUnsequenced_OutOfOrderCommitAcrossConnections(t *testing.T) {
	t.Parallel()
	db, poolRepo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Open two completely separate PostgreSQL connections to model concurrent producers.
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

	// Connection 1 starts a transaction and inserts an event that receives the lower raw ID,
	// leaving it uncommitted.
	txLow, err := connLow.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin txLow: %v", err)
	}
	defer func() { _ = txLow.Rollback(context.Background()) }()

	evLow, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "asset-low-raw", nil, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	txLowRepo := postgres.NewEventRepository(txLow)
	if err := txLowRepo.AppendEvent(ctx, &evLow); err != nil {
		t.Fatalf("AppendEvent evLow: %v", err)
	}

	// Reject transaction-bound publication inside the producer transaction and verify from
	// inside txLow that its lower raw ID remains unpublished.
	if _, err := txLowRepo.PublishUnsequencedEvents(ctx, time.Now().UTC()); !errors.Is(err, storage.ErrRootRepositoryRequired) {
		t.Fatalf("expected ErrRootRepositoryRequired on txLowRepo.PublishUnsequencedEvents, got %v", err)
	}
	if err := poolRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		_, err := txRepo.PublishUnsequencedEvents(ctx, time.Now().UTC())
		return err
	}); !errors.Is(err, storage.ErrRootRepositoryRequired) {
		t.Fatalf("expected ErrRootRepositoryRequired on WithTx-bound PublishUnsequencedEvents, got %v", err)
	}

	inTxLow, err := txLowRepo.GetEventByID(ctx, evLow.ID)
	if err != nil {
		t.Fatalf("GetEventByID inside txLow after rejected publish: %v", err)
	}
	if inTxLow.PublishedSequence != nil || inTxLow.PublishedAt != nil || inTxLow.IsPublished() {
		t.Fatalf("expected evLow inside producer txLow to stay unpublished after rejection, got seq=%v at=%v",
			inTxLow.PublishedSequence, inTxLow.PublishedAt)
	}
	var rawSeq sql.NullInt64
	var rawPubAt sql.NullTime
	if err := txLow.QueryRow(ctx,
		`SELECT published_sequence, published_at FROM domain_events WHERE id = $1`,
		evLow.ID,
	).Scan(&rawSeq, &rawPubAt); err != nil {
		t.Fatalf("querying evLow inside txLow: %v", err)
	}
	if rawSeq.Valid || rawPubAt.Valid {
		t.Fatalf("expected SQL published_sequence and published_at NULL inside txLow after rejection, got seq=%v at=%v", rawSeq, rawPubAt)
	}

	// Connection 2 starts a transaction, inserts an event that receives a higher raw ID,
	// and commits first.
	txHigh, err := connHigh.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin txHigh: %v", err)
	}
	defer func() { _ = txHigh.Rollback(context.Background()) }()

	evHigh, err := domain.NewDomainEvent(domain.EventAssetAdded, 1, domain.AggregateTypeAsset, "asset-high-raw", nil, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewEventRepository(txHigh).AppendEvent(ctx, &evHigh); err != nil {
		t.Fatalf("AppendEvent evHigh: %v", err)
	}
	if err := txHigh.Commit(ctx); err != nil {
		t.Fatalf("Commit txHigh: %v", err)
	}

	if evLow.ID >= evHigh.ID {
		t.Fatalf("expected evLow.ID (%d) < evHigh.ID (%d)", evLow.ID, evHigh.ID)
	}

	// Root publisher sequences only the committed higher raw ID; lower raw ID stays invisible
	// to root and unpublished inside txLow.
	t1 := time.Date(2026, 11, 16, 15, 0, 0, 0, time.UTC)
	batch1, err := poolRepo.PublishUnsequencedEvents(ctx, t1)
	if err != nil {
		t.Fatalf("PublishUnsequencedEvents batch1: %v", err)
	}
	if len(batch1) != 1 || batch1[0].ID != evHigh.ID {
		t.Fatalf("expected only evHigh (%d) in batch1, got %+v", evHigh.ID, batch1)
	}
	if batch1[0].PublishedSequence == nil || *batch1[0].PublishedSequence != 1 {
		t.Fatalf("expected evHigh published_sequence = 1, got %v", batch1[0].PublishedSequence)
	}
	if batch1[0].PublishedAt == nil || !batch1[0].PublishedAt.Equal(t1) {
		t.Fatalf("expected evHigh published_at = %v, got %v", t1, batch1[0].PublishedAt)
	}
	if _, err := poolRepo.GetEventByID(ctx, evLow.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected uncommitted evLow (%d) to remain invisible, got err=%v", evLow.ID, err)
	}
	stillUnpublishedInTx, err := txLowRepo.GetEventByID(ctx, evLow.ID)
	if err != nil {
		t.Fatalf("GetEventByID inside txLow after batch1: %v", err)
	}
	if stillUnpublishedInTx.IsPublished() {
		t.Fatalf("expected evLow inside txLow to remain unpublished after batch1, got %+v", stillUnpublishedInTx)
	}

	// Now Connection 1 commits the lower raw ID.
	if err := txLow.Commit(ctx); err != nil {
		t.Fatalf("Commit txLow: %v", err)
	}

	// Second root publish pass scans all published_sequence IS NULL rows regardless of raw ID
	// and assigns the later sequence (2) to the lower raw ID.
	t2 := t1.Add(5 * time.Second)
	batch2, err := poolRepo.PublishUnsequencedEvents(ctx, t2)
	if err != nil {
		t.Fatalf("PublishUnsequencedEvents batch2: %v", err)
	}
	if len(batch2) != 1 || batch2[0].ID != evLow.ID {
		t.Fatalf("expected evLow (%d) in batch2, got %+v", evLow.ID, batch2)
	}
	if batch2[0].PublishedSequence == nil || *batch2[0].PublishedSequence != 2 {
		t.Fatalf("expected evLow published_sequence = 2, got %v", batch2[0].PublishedSequence)
	}
	if batch2[0].PublishedAt == nil || !batch2[0].PublishedAt.Equal(t2) {
		t.Fatalf("expected evLow published_at = %v, got %v", t2, batch2[0].PublishedAt)
	}

	// Idempotent empty pass.
	batch3, err := poolRepo.PublishUnsequencedEvents(ctx, t2.Add(time.Second))
	if err != nil {
		t.Fatalf("PublishUnsequencedEvents batch3: %v", err)
	}
	if len(batch3) != 0 {
		t.Fatalf("expected 0 events in batch3, got %d", len(batch3))
	}
}

type repoFaultCommitDB struct {
	*testdb.TestDB
	failCommitsRemaining atomic.Int32
	commitErr            error
}

func (f *repoFaultCommitDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := f.TestDB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &repoFaultCommitTx{Tx: tx, db: f}, nil
}

type repoFaultCommitTx struct {
	pgx.Tx
	db *repoFaultCommitDB
}

func (t *repoFaultCommitTx) Commit(ctx context.Context) error {
	if t.db.failCommitsRemaining.Add(-1) >= 0 {
		return t.db.commitErr
	}
	return t.Tx.Commit(ctx)
}

func TestEventRepository_PublishUnsequenced_RollbackLeavesUnpublishedAndConsumesNoGap(t *testing.T) {
	t.Parallel()
	db, poolRepo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Seed 1 committed event and publish it -> published_sequence = 1.
	var ev1, ev2, ev3 domain.DomainEvent
	err := poolRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		var err error
		ev1, err = domain.NewDomainEvent(domain.EventJobStarted, 1, domain.AggregateTypeJob, "job-seq-1", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		return txRepo.AppendEvent(ctx, &ev1)
	})
	if err != nil {
		t.Fatalf("append ev1: %v", err)
	}

	t1 := time.Date(2026, 11, 16, 15, 10, 0, 0, time.UTC)
	firstBatch, err := poolRepo.PublishUnsequencedEvents(ctx, t1)
	if err != nil || len(firstBatch) != 1 || *firstBatch[0].PublishedSequence != 1 {
		t.Fatalf("initial publish failed: batch=%+v err=%v", firstBatch, err)
	}

	// Append two more unsequenced events.
	err = poolRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
		var err error
		ev2, err = domain.NewDomainEvent(domain.EventJobFinished, 1, domain.AggregateTypeJob, "job-seq-2", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := txRepo.AppendEvent(ctx, &ev2); err != nil {
			return err
		}
		ev3, err = domain.NewDomainEvent(domain.EventJobFailed, 1, domain.AggregateTypeJob, "job-seq-3", nil, time.Now().UTC())
		if err != nil {
			return err
		}
		return txRepo.AppendEvent(ctx, &ev3)
	})
	if err != nil {
		t.Fatalf("append ev2/ev3: %v", err)
	}

	// Exercise rollback and gapless retry through the non-transactional root entry point.
	faultDB := &repoFaultCommitDB{
		TestDB:    db,
		commitErr: errors.New("injected root publish commit failure"),
	}
	rootFaultRepo := postgres.NewEventRepository(faultDB)
	if rootFaultRepo.InTx() {
		t.Fatal("expected rootFaultRepo.InTx() == false")
	}

	faultDB.failCommitsRemaining.Store(1)
	tRollback := t1.Add(time.Minute)
	if _, err := rootFaultRepo.PublishUnsequencedEvents(ctx, tRollback); !errors.Is(err, faultDB.commitErr) {
		t.Fatalf("expected injected commit error, got %v", err)
	}

	// Both rows must remain unpublished and watermark must still be 1.
	for _, id := range []int64{ev2.ID, ev3.ID} {
		got, err := poolRepo.GetEventByID(ctx, id)
		if err != nil {
			t.Fatalf("GetEventByID(%d): %v", id, err)
		}
		if got.PublishedSequence != nil || got.PublishedAt != nil {
			t.Fatalf("expected event %d unpublished after rollback, got seq=%v at=%v", id, got.PublishedSequence, got.PublishedAt)
		}
	}
	var watermark int64
	if err := db.QueryRow(ctx, `SELECT last_sequence FROM domain_event_sequence WHERE id = TRUE`).Scan(&watermark); err != nil {
		t.Fatalf("querying domain_event_sequence: %v", err)
	}
	if watermark != 1 {
		t.Fatalf("expected watermark 1 after rollback, got %d", watermark)
	}

	// Retry through the same root entry point must assign 2 and 3 with no gap.
	tRetry := t1.Add(2 * time.Minute)
	retried, err := rootFaultRepo.PublishUnsequencedEvents(ctx, tRetry)
	if err != nil {
		t.Fatalf("retry PublishUnsequencedEvents: %v", err)
	}
	if len(retried) != 2 {
		t.Fatalf("expected 2 retried events, got %d", len(retried))
	}
	if retried[0].ID != ev2.ID || *retried[0].PublishedSequence != 2 || !retried[0].PublishedAt.Equal(tRetry) {
		t.Fatalf("unexpected retried[0]: %+v", retried[0])
	}
	if retried[1].ID != ev3.ID || *retried[1].PublishedSequence != 3 || !retried[1].PublishedAt.Equal(tRetry) {
		t.Fatalf("unexpected retried[1]: %+v", retried[1])
	}
}

func TestEventRepository_PublishUnsequenced_ConcurrentSerializesWithoutGapsOrDuplicates(t *testing.T) {
	t.Parallel()
	db, poolRepo := setupEventTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		producerCount    = 6
		eventsPerProd    = 15
		publisherWorkers = 4
	)
	const totalEvents = producerCount * eventsPerProd

	var wg sync.WaitGroup
	errCh := make(chan error, producerCount+publisherWorkers)
	producersDone := make(chan struct{})

	var prodWG sync.WaitGroup
	for p := range producerCount {
		prodWG.Add(1)
		go func(pid int) {
			defer prodWG.Done()
			for i := range eventsPerProd {
				err := poolRepo.WithTx(ctx, func(txRepo storage.EventRepository) error {
					ev, err := domain.NewDomainEvent(
						domain.EventAssetAdded,
						1,
						domain.AggregateTypeAsset,
						"concurrent-asset",
						domain.AssetEventPayloadV1{
							AssetID:       "a",
							VolumeID:      "v",
							LibraryID:     "l",
							LibraryRootID: "r",
							PathDisplay:   string(rune('a'+pid)) + "/" + string(rune('a'+i)),
						},
						time.Now().UTC(),
					)
					if err != nil {
						return err
					}
					return txRepo.AppendEvent(ctx, &ev)
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}(p)
	}

	go func() {
		prodWG.Wait()
		close(producersDone)
	}()

	var (
		mu         sync.Mutex
		allBatches []domain.DomainEvent
	)
	for range publisherWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each worker uses its own EventRepository over db.Pool to trigger real SQL lock contention.
			workerRepo := postgres.NewEventRepository(db.Pool)
			for {
				select {
				case <-producersDone:
					batch, err := workerRepo.PublishUnsequencedEvents(ctx, time.Now().UTC())
					if err != nil {
						errCh <- err
						return
					}
					if len(batch) > 0 {
						mu.Lock()
						allBatches = append(allBatches, batch...)
						mu.Unlock()
					}
					return
				default:
					batch, err := workerRepo.PublishUnsequencedEvents(ctx, time.Now().UTC())
					if err != nil {
						errCh <- err
						return
					}
					if len(batch) > 0 {
						mu.Lock()
						allBatches = append(allBatches, batch...)
						mu.Unlock()
					}
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	wgDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgDone)
	}()
	select {
	case <-wgDone:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for concurrent workers: %v", ctx.Err())
	}
	// One final drain in case a producer committed right as workers saw producersDone.
	finalBatch, err := poolRepo.PublishUnsequencedEvents(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("final drain: %v", err)
	}
	allBatches = append(allBatches, finalBatch...)

	close(errCh)
	for e := range errCh {
		t.Fatalf("concurrent error: %v", e)
	}

	if len(allBatches) != totalEvents {
		t.Fatalf("expected %d total published events across workers, got %d", totalEvents, len(allBatches))
	}

	seenSeq := make(map[int64]bool, totalEvents)
	seenID := make(map[int64]bool, totalEvents)
	for _, ev := range allBatches {
		if !ev.IsPublished() {
			t.Fatalf("event %d returned without publication metadata", ev.ID)
		}
		if seenID[ev.ID] {
			t.Fatalf("duplicate event ID %d returned by concurrent publishers", ev.ID)
		}
		seenID[ev.ID] = true
		seq := *ev.PublishedSequence
		if seenSeq[seq] {
			t.Fatalf("duplicate published_sequence %d", seq)
		}
		seenSeq[seq] = true
	}
	for expected := int64(1); expected <= totalEvents; expected++ {
		if !seenSeq[expected] {
			t.Fatalf("missing published_sequence %d (gap detected)", expected)
		}
	}
}
