package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
)

func TestPeopleCreditsEntities_AllRolesAndKindsRoundTripViaTx(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		series  domain.Series
		vol     domain.Volume
		persons []domain.Person
		ents    []domain.Entity
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ := seedTestLibraryAndRoot(ctx, t, tx, "Manga", []byte("/srv/manga"))
		series = seedMembershipSeries(ctx, t, tx, lib.ID, "Series One")
		vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 1)

		roles := domain.ValidCreditRoles()
		for i, role := range roles {
			p := domain.Person{
				Name:        "Creator " + string(role),
				Aliases:     []string{"Alias " + string(role)},
				ExternalIDs: map[string]any{"idx": i},
			}
			if err := tx.People().CreatePerson(ctx, &p); err != nil {
				return err
			}
			persons = append(persons, p)

			sc := domain.Credit{
				PersonID: p.ID,
				SeriesID: &series.ID,
				Role:     role,
				Position: i,
			}
			if err := tx.Credits().CreateCredit(ctx, &sc); err != nil {
				return err
			}

			vc := domain.Credit{
				PersonID: p.ID,
				VolumeID: &vol.ID,
				Role:     role,
				Position: i,
			}
			if err := tx.Credits().CreateCredit(ctx, &vc); err != nil {
				return err
			}
		}

		for i, kind := range domain.ValidEntityKinds() {
			e := domain.Entity{
				Kind:        kind,
				Name:        "Entity " + string(kind),
				Aliases:     []string{"Alt " + string(kind)},
				ExternalIDs: map[string]any{"idx": i},
			}
			if err := tx.Entities().CreateEntity(ctx, &e); err != nil {
				return err
			}
			ents = append(ents, e)
		}

		links := make([]domain.VolumeEntity, len(ents))
		for i, e := range ents {
			links[i] = domain.VolumeEntity{VolumeID: vol.ID, EntityID: e.ID}
		}
		return tx.Entities().SetVolumeEntities(ctx, vol.ID, links)
	})
	if err != nil {
		t.Fatalf("WithTx round-trip setup: %v", err)
	}

	creditRepo := postgres.NewCreditRepository(db.Pool)
	seriesCredits, err := creditRepo.ListCreditsBySeries(ctx, series.ID)
	if err != nil {
		t.Fatalf("ListCreditsBySeries: %v", err)
	}
	if len(seriesCredits) != 10 {
		t.Fatalf("len(seriesCredits) = %d, want 10", len(seriesCredits))
	}
	for i, c := range seriesCredits {
		if c.Role != domain.ValidCreditRoles()[i] || c.Position != i || c.PersonID != persons[i].ID {
			t.Fatalf("seriesCredits[%d] = %+v, unexpected", i, c)
		}
	}

	volCredits, err := creditRepo.ListCreditsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListCreditsByVolume: %v", err)
	}
	if len(volCredits) != 10 {
		t.Fatalf("len(volCredits) = %d, want 10", len(volCredits))
	}

	byPerson, err := creditRepo.ListCreditsByPerson(ctx, persons[0].ID)
	if err != nil {
		t.Fatalf("ListCreditsByPerson: %v", err)
	}
	if len(byPerson) != 2 {
		t.Fatalf("len(ListCreditsByPerson) = %d, want 2 (1 series + 1 volume)", len(byPerson))
	}

	entRepo := postgres.NewEntityRepository(db.Pool)
	volEnts, err := entRepo.ListVolumeEntities(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListVolumeEntities: %v", err)
	}
	if len(volEnts) != 3 {
		t.Fatalf("len(volEnts) = %d, want 3", len(volEnts))
	}
	for _, e := range ents {
		got, err := entRepo.GetEntityByID(ctx, e.ID)
		if err != nil {
			t.Fatalf("GetEntityByID(%s): %v", e.ID, err)
		}
		if got.Kind != e.Kind || got.Name != e.Name || !slices.Equal(got.Aliases, e.Aliases) {
			t.Fatalf("GetEntityByID(%s) = %+v, want %+v", e.ID, got, e)
		}
	}
}

func TestCreditRepository_TargetValidationAndUniqueIndexes(t *testing.T) {
	t.Parallel()
	_, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ := seedTestLibraryAndRoot(ctx, t, tx, "Comics", []byte("/srv/comics"))
		s1 := seedMembershipSeries(ctx, t, tx, lib.ID, "S1")
		s2 := seedMembershipSeries(ctx, t, tx, lib.ID, "S2")
		v1 := seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindIssue, 1)
		v2 := seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindIssue, 2)

		p := domain.Person{Name: "Alan Moore"}
		if err := tx.People().CreatePerson(ctx, &p); err != nil {
			return err
		}

		// Reject neither series_id nor volume_id.
		if err := tx.Credits().CreateCredit(ctx, &domain.Credit{
			PersonID: p.ID,
			Role:     domain.CreditRoleWriter,
		}); !errors.Is(err, domain.ErrInvalidCreditTarget) {
			t.Fatalf("CreateCredit(neither) = %v, want ErrInvalidCreditTarget", err)
		}

		// Reject both series_id and volume_id.
		if err := tx.Credits().CreateCredit(ctx, &domain.Credit{
			PersonID: p.ID,
			SeriesID: &s1.ID,
			VolumeID: &v1.ID,
			Role:     domain.CreditRoleWriter,
		}); !errors.Is(err, domain.ErrInvalidCreditTarget) {
			t.Fatalf("CreateCredit(both) = %v, want ErrInvalidCreditTarget", err)
		}

		// Series unique index: (series_id, person_id, role).
		if err := tx.Credits().CreateCredit(ctx, &domain.Credit{
			PersonID: p.ID,
			SeriesID: &s1.ID,
			Role:     domain.CreditRoleWriter,
		}); err != nil {
			return err
		}
		if err := tx.WithTx(ctx, func(sub storage.Tx) error {
			return sub.Credits().CreateCredit(ctx, &domain.Credit{
				PersonID: p.ID,
				SeriesID: &s1.ID,
				Role:     domain.CreditRoleWriter,
			})
		}); !errors.Is(err, storage.ErrAlreadyExists) {
			t.Fatalf("duplicate series credit = %v, want storage.ErrAlreadyExists", err)
		}
		// Same person, different role on same series succeeds; same role on s2 succeeds.
		if err := tx.Credits().CreateCredit(ctx, &domain.Credit{
			PersonID: p.ID,
			SeriesID: &s1.ID,
			Role:     domain.CreditRoleAuthor,
		}); err != nil {
			return err
		}
		if err := tx.Credits().CreateCredit(ctx, &domain.Credit{
			PersonID: p.ID,
			SeriesID: &s2.ID,
			Role:     domain.CreditRoleWriter,
		}); err != nil {
			return err
		}

		// Volume unique index: (volume_id, person_id, role).
		if err := tx.Credits().CreateCredit(ctx, &domain.Credit{
			PersonID: p.ID,
			VolumeID: &v1.ID,
			Role:     domain.CreditRoleWriter,
		}); err != nil {
			return err
		}
		if err := tx.WithTx(ctx, func(sub storage.Tx) error {
			return sub.Credits().CreateCredit(ctx, &domain.Credit{
				PersonID: p.ID,
				VolumeID: &v1.ID,
				Role:     domain.CreditRoleWriter,
			})
		}); !errors.Is(err, storage.ErrAlreadyExists) {
			t.Fatalf("duplicate volume credit = %v, want storage.ErrAlreadyExists", err)
		}
		return tx.Credits().CreateCredit(ctx, &domain.Credit{
			PersonID: p.ID,
			VolumeID: &v2.ID,
			Role:     domain.CreditRoleWriter,
		})
	})
	if err != nil {
		t.Fatalf("WithTx credit index checks: %v", err)
	}
}

func TestPeopleAndEntities_NormalizedReuseAndCoexistence(t *testing.T) {
	t.Parallel()
	_, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		people := tx.People()
		entities := tx.Entities()

		// Name-only person variants share one ID and preserve original display name.
		p1, err := people.FindOrCreatePersonByName(ctx, "\u00c9lodie Dupont")
		if err != nil {
			return err
		}
		for _, variant := range []string{
			"  e\u0301lodie dupont\u3000",
			"\u00c9LODIE DUPONT",
			"\u00a0e\u0301LoDiE dUpOnT\t",
		} {
			got, err := people.FindOrCreatePersonByName(ctx, variant)
			if err != nil {
				return err
			}
			if got.ID != p1.ID {
				t.Fatalf("variant %q got ID %s, want %s", variant, got.ID, p1.ID)
			}
			if got.Name != "\u00c9lodie Dupont" {
				t.Fatalf("variant %q overwrote display name to %q", variant, got.Name)
			}
		}

		// Separately curated same-name people (ImportKey == nil) coexist with each other
		// and with a name-only import sharing that name.
		curated1 := domain.Person{Name: "John Smith"}
		curated2 := domain.Person{Name: "John Smith"}
		if err := people.CreatePerson(ctx, &curated1); err != nil {
			return err
		}
		if err := people.CreatePerson(ctx, &curated2); err != nil {
			return err
		}
		importedJohn, err := people.FindOrCreatePersonByName(ctx, "John Smith")
		if err != nil {
			return err
		}
		if curated1.ID == curated2.ID || curated1.ID == importedJohn.ID || curated2.ID == importedJohn.ID {
			t.Fatalf("curated and imported same-name people collapsed: %s, %s, %s", curated1.ID, curated2.ID, importedJohn.ID)
		}
		if curated1.ImportKey != nil || curated2.ImportKey != nil || importedJohn.ImportKey == nil {
			t.Fatalf("unexpected import_key states: %v, %v, %v", curated1.ImportKey, curated2.ImportKey, importedJohn.ImportKey)
		}

		// Entity normalized variants reuse same-kind entity while preserving display name.
		eLoc1, err := entities.FindOrCreateEntityByName(ctx, domain.EntityKindLocation, "Weißstadt")
		if err != nil {
			return err
		}
		eLoc2, err := entities.FindOrCreateEntityByName(ctx, domain.EntityKindLocation, "  WEISSSTADT\u00a0")
		if err != nil {
			return err
		}
		if eLoc2.ID != eLoc1.ID || eLoc2.Name != "Weißstadt" {
			t.Fatalf("entity variant got %+v, want ID=%s Name=%q", eLoc2, eLoc1.ID, "Weißstadt")
		}

		// Different entity kinds with the same normalized name coexist.
		eChar, err := entities.FindOrCreateEntityByName(ctx, domain.EntityKindCharacter, "Gotham")
		if err != nil {
			return err
		}
		eTeam, err := entities.FindOrCreateEntityByName(ctx, domain.EntityKindTeam, "GOTHAM")
		if err != nil {
			return err
		}
		eLoc, err := entities.FindOrCreateEntityByName(ctx, domain.EntityKindLocation, "gotham")
		if err != nil {
			return err
		}
		if eChar.ID == eTeam.ID || eChar.ID == eLoc.ID || eTeam.ID == eLoc.ID {
			t.Fatalf("different entity kinds collapsed: %s, %s, %s", eChar.ID, eTeam.ID, eLoc.ID)
		}

		// Separately curated same-kind same-name entities coexist with keyed imports.
		curEnt1 := domain.Entity{Kind: domain.EntityKindCharacter, Name: "Robin"}
		curEnt2 := domain.Entity{Kind: domain.EntityKindCharacter, Name: "Robin"}
		if err := entities.CreateEntity(ctx, &curEnt1); err != nil {
			return err
		}
		if err := entities.CreateEntity(ctx, &curEnt2); err != nil {
			return err
		}
		impEnt, err := entities.FindOrCreateEntityByName(ctx, domain.EntityKindCharacter, "Robin")
		if err != nil {
			return err
		}
		if curEnt1.ID == curEnt2.ID || curEnt1.ID == impEnt.ID || curEnt2.ID == impEnt.ID {
			t.Fatalf("curated and imported same-name entities collapsed: %s, %s, %s", curEnt1.ID, curEnt2.ID, impEnt.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx normalized reuse and coexistence: %v", err)
	}
}

func runConcurrentTxPair(
	ctx context.Context,
	transactor storage.Transactor,
	fn func(idx int, tx storage.Tx) (string, error),
) ([2]string, error) {
	var (
		wg        sync.WaitGroup
		ready     sync.WaitGroup
		startCh   = make(chan struct{})
		startOnce sync.Once
		setupErr  atomic.Bool
		ids       [2]string
		errs      [2]error
	)
	release := func() {
		startOnce.Do(func() {
			close(startCh)
		})
	}
	defer release()

	ready.Add(2)
	wg.Add(2)
	for i := range 2 {
		go func(idx int) {
			defer wg.Done()
			var (
				readyOnce sync.Once
				entered   bool
			)
			markReady := func() {
				readyOnce.Do(ready.Done)
			}
			defer func() {
				if !entered {
					setupErr.Store(true)
				}
				markReady()
			}()

			errs[idx] = transactor.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
				entered = true
				markReady()
				select {
				case <-startCh:
				case <-ctx.Done():
					return ctx.Err()
				}
				if setupErr.Load() {
					return errors.New("aborted due to peer transaction setup failure")
				}
				id, err := fn(idx, tx)
				if err != nil {
					return err
				}
				ids[idx] = id
				return nil
			})
		}(i)
	}

	readyDone := make(chan struct{})
	go func() {
		ready.Wait()
		close(readyDone)
	}()

	select {
	case <-readyDone:
	case <-ctx.Done():
		release()
		wg.Wait()
		var joined []error
		for i, err := range errs {
			if err != nil {
				joined = append(joined, fmt.Errorf("worker[%d]: %w", i, err))
			}
		}
		if len(joined) > 0 {
			return [2]string{}, fmt.Errorf("waiting for concurrent transactions: %w (%w)", ctx.Err(), errors.Join(joined...))
		}
		return [2]string{}, fmt.Errorf("waiting for concurrent transactions: %w", ctx.Err())
	}

	release()
	wg.Wait()

	var joined []error
	for i, err := range errs {
		if err != nil {
			joined = append(joined, fmt.Errorf("worker[%d]: %w", i, err))
		}
	}
	if len(joined) > 0 {
		return [2]string{}, errors.Join(joined...)
	}
	return ids, nil
}

type stubTransactor struct {
	withReadCommitted func(ctx context.Context, fn func(tx storage.Tx) error) error
}

func (s stubTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return s.withReadCommitted(ctx, fn)
}

func (s stubTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return s.withReadCommitted(ctx, fn)
}

func TestConcurrentTxPair_ReleasesBarrierOnSetupFailure(t *testing.T) {
	t.Parallel()

	t.Run("releases peer immediately when transaction setup fails", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		beginErr := errors.New("begin tx failed")
		var calls atomic.Int32
		peerDone := make(chan struct{})
		txr := stubTransactor{
			withReadCommitted: func(ctx context.Context, fn func(tx storage.Tx) error) error {
				if calls.Add(1) == 1 {
					defer close(peerDone)
					return fn(nil)
				}
				return beginErr
			},
		}

		start := time.Now()
		_, err := runConcurrentTxPair(ctx, txr, func(idx int, _ storage.Tx) (string, error) {
			t.Errorf("worker[%d] callback should not run after peer setup failure", idx)
			return "", nil
		})
		if !errors.Is(err, beginErr) {
			t.Fatalf("runConcurrentTxPair err = %v, want %v", err, beginErr)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("runConcurrentTxPair took %v, expected immediate release on setup failure", elapsed)
		}
		select {
		case <-peerDone:
		case <-time.After(time.Second):
			t.Fatal("peer worker remained blocked after setup failure")
		}
	})

	t.Run("releases workers on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var entered atomic.Int32
		txr := stubTransactor{
			withReadCommitted: func(ctx context.Context, fn func(tx storage.Tx) error) error {
				if entered.Add(1) == 1 {
					return fn(nil)
				}
				<-ctx.Done()
				return ctx.Err()
			},
		}
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		_, err := runConcurrentTxPair(ctx, txr, func(int, storage.Tx) (string, error) {
			return "", nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runConcurrentTxPair err = %v, want context.Canceled", err)
		}
	})
}

func TestPeopleAndEntities_ConcurrentFindOrCreate(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t.Run("person_concurrent_find_or_create", func(t *testing.T) {
		variants := [2]string{"Stan Lee", "  STAN LEE\u3000"}
		ids, err := runConcurrentTxPair(ctx, transactor, func(idx int, tx storage.Tx) (string, error) {
			p, err := tx.People().FindOrCreatePersonByName(ctx, variants[idx])
			if err != nil {
				return "", err
			}
			return p.ID, nil
		})
		if err != nil {
			t.Fatalf("concurrent FindOrCreatePersonByName failed: %v", err)
		}
		if ids[0] == "" || ids[0] != ids[1] {
			t.Fatalf("concurrent FindOrCreatePersonByName returned different IDs: %q vs %q", ids[0], ids[1])
		}

		var count int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM people WHERE import_key = $1`, domain.NormalizeImportName("Stan Lee")).Scan(&count); err != nil {
			t.Fatalf("counting people: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected 1 person row for concurrent import, got %d", count)
		}
	})

	t.Run("entity_concurrent_find_or_create", func(t *testing.T) {
		variants := [2]string{"X-Men", "  x-men "}
		ids, err := runConcurrentTxPair(ctx, transactor, func(idx int, tx storage.Tx) (string, error) {
			e, err := tx.Entities().FindOrCreateEntityByName(ctx, domain.EntityKindTeam, variants[idx])
			if err != nil {
				return "", err
			}
			return e.ID, nil
		})
		if err != nil {
			t.Fatalf("concurrent FindOrCreateEntityByName failed: %v", err)
		}
		if ids[0] == "" || ids[0] != ids[1] {
			t.Fatalf("concurrent FindOrCreateEntityByName returned different IDs: %q vs %q", ids[0], ids[1])
		}

		var count int
		if err := db.QueryRow(ctx,
			`SELECT count(*) FROM entities WHERE kind = 'TEAM' AND import_key = $1`,
			domain.NormalizeImportName("X-Men"),
		).Scan(&count); err != nil {
			t.Fatalf("counting entities: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected 1 entity row for concurrent import, got %d", count)
		}
	})

	t.Run("repeatable_read_conflict_fails_closed", func(t *testing.T) {
		txA, err := db.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
		if err != nil {
			t.Fatalf("BeginTx A: %v", err)
		}
		defer func() { _ = txA.Rollback(ctx) }()

		txB, err := db.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
		if err != nil {
			t.Fatalf("BeginTx B: %v", err)
		}
		defer func() { _ = txB.Rollback(ctx) }()

		// Establish snapshots on both transactions before either inserts.
		if _, err := txA.Exec(ctx, "SELECT 1"); err != nil {
			t.Fatalf("snapshot A: %v", err)
		}
		if _, err := txB.Exec(ctx, "SELECT 1"); err != nil {
			t.Fatalf("snapshot B: %v", err)
		}

		if _, err := postgres.NewPersonRepository(txA).FindOrCreatePersonByName(ctx, "Repeatable Race"); err != nil {
			t.Fatalf("txA FindOrCreatePersonByName: %v", err)
		}
		if err := txA.Commit(ctx); err != nil {
			t.Fatalf("txA Commit: %v", err)
		}

		_, errB := postgres.NewPersonRepository(txB).FindOrCreatePersonByName(ctx, "repeatable race")
		var pgErr *pgconn.PgError
		isSerializationErr := errors.As(errB, &pgErr) && pgErr.Code == "40001"
		if !errors.Is(errB, storage.ErrTransactionIsolation) && !isSerializationErr {
			t.Fatalf("txB under REPEATABLE READ = %v, want ErrTransactionIsolation or SQLSTATE 40001", errB)
		}
	})
}

func TestSyncHelpers_FieldLockPreservation(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		series           domain.Series
		vol              domain.Volume
		personLocked     domain.Person
		personWild       domain.Person
		personUnlocked   domain.Person
		entityLocked     domain.Entity
		entityUnlocked   domain.Entity
		lockedSeriesCred domain.Credit
		lockedVolCred    domain.Credit
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ := seedTestLibraryAndRoot(ctx, t, tx, "Library", []byte("/srv/lib"))
		series = seedMembershipSeries(ctx, t, tx, lib.ID, "Series Lock")
		vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 1)

		native := "尾田栄一郎"
		romanized := "Oda Eiichiro"
		key := domain.NormalizeImportName("Eiichiro Oda")
		personLocked = domain.Person{
			Name:          "Eiichiro Oda",
			ImportKey:     &key,
			NameNative:    &native,
			NameRomanized: &romanized,
			Aliases:       []string{"Oda"},
			ExternalIDs:   map[string]any{"anilist": 1881},
			LockedFields:  domain.FieldLocks{"name": true, "name_native": true, "aliases": true},
		}
		if err := tx.People().CreatePerson(ctx, &personLocked); err != nil {
			return err
		}

		wildNative := "浦沢直樹"
		personWild = domain.Person{
			Name:         "Naoki Urasawa",
			NameNative:   &wildNative,
			ExternalIDs:  map[string]any{"anilist": 1882},
			LockedFields: domain.FieldLocks{"*": true},
		}
		if err := tx.People().CreatePerson(ctx, &personWild); err != nil {
			return err
		}

		personUnlocked = domain.Person{Name: "Unlocked Author"}
		if err := tx.People().CreatePerson(ctx, &personUnlocked); err != nil {
			return err
		}

		entKey := domain.NormalizeImportName("Monkey D. Luffy")
		entityLocked = domain.Entity{
			Kind:         domain.EntityKindCharacter,
			Name:         "Monkey D. Luffy",
			ImportKey:    &entKey,
			Aliases:      []string{"Straw Hat"},
			ExternalIDs:  map[string]any{"anilist": 40},
			LockedFields: domain.FieldLocks{"name": true, "external_ids": true},
		}
		if err := tx.Entities().CreateEntity(ctx, &entityLocked); err != nil {
			return err
		}

		entityUnlocked = domain.Entity{
			Kind: domain.EntityKindTeam,
			Name: "Straw Hat Pirates",
		}
		if err := tx.Entities().CreateEntity(ctx, &entityUnlocked); err != nil {
			return err
		}

		// Seed initial credits: one locked, one unlocked on both series and volume.
		lockedSeriesCred = domain.Credit{
			PersonID:     personLocked.ID,
			SeriesID:     &series.ID,
			Role:         domain.CreditRoleAuthor,
			Position:     0,
			LockedFields: domain.FieldLocks{"role": true},
		}
		unlockedSeriesCred := domain.Credit{
			PersonID: personUnlocked.ID,
			SeriesID: &series.ID,
			Role:     domain.CreditRoleEditor,
			Position: 1,
		}
		if err := tx.Credits().SetSeriesCredits(ctx, series.ID, []domain.Credit{lockedSeriesCred, unlockedSeriesCred}); err != nil {
			return err
		}

		lockedVolCred = domain.Credit{
			PersonID:     personLocked.ID,
			VolumeID:     &vol.ID,
			Role:         domain.CreditRoleWriter,
			Position:     0,
			LockedFields: domain.FieldLocks{"credit": true},
		}
		unlockedVolCred := domain.Credit{
			PersonID: personUnlocked.ID,
			VolumeID: &vol.ID,
			Role:     domain.CreditRoleTranslator,
			Position: 1,
		}
		if err := tx.Credits().SetVolumeCredits(ctx, vol.ID, []domain.Credit{lockedVolCred, unlockedVolCred}); err != nil {
			return err
		}

		// Seed initial volume_entities: one locked, one unlocked.
		return tx.Entities().SetVolumeEntities(ctx, vol.ID, []domain.VolumeEntity{
			{VolumeID: vol.ID, EntityID: entityLocked.ID, LockedFields: domain.FieldLocks{"entity": true}},
			{VolumeID: vol.ID, EntityID: entityUnlocked.ID},
		})
	})
	if err != nil {
		t.Fatalf("WithTx seed lock fixtures: %v", err)
	}

	// SyncPerson & SyncEntity reject incoming locked_fields.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		badPerson := personLocked
		badPerson.LockedFields = domain.FieldLocks{"name": true}
		if err := tx.People().SyncPerson(ctx, &badPerson); !errors.Is(err, storage.ErrAutomaticSyncLockedFields) {
			t.Fatalf("SyncPerson with locked_fields = %v, want ErrAutomaticSyncLockedFields", err)
		}

		badEnt := entityLocked
		badEnt.LockedFields = domain.FieldLocks{"name": true}
		if err := tx.Entities().SyncEntity(ctx, &badEnt); !errors.Is(err, storage.ErrAutomaticSyncLockedFields) {
			t.Fatalf("SyncEntity with locked_fields = %v, want ErrAutomaticSyncLockedFields", err)
		}

		badCredit := []domain.Credit{{
			PersonID:     personUnlocked.ID,
			VolumeID:     &vol.ID,
			Role:         domain.CreditRoleWriter,
			LockedFields: domain.FieldLocks{"role": true},
		}}
		if err := tx.Credits().SyncVolumeCredits(ctx, vol.ID, badCredit); !errors.Is(err, storage.ErrAutomaticSyncLockedFields) {
			t.Fatalf("SyncVolumeCredits with locked_fields = %v, want ErrAutomaticSyncLockedFields", err)
		}

		badLink := []domain.VolumeEntity{{
			VolumeID:     vol.ID,
			EntityID:     entityUnlocked.ID,
			LockedFields: domain.FieldLocks{"entity": true},
		}}
		if err := tx.Entities().SyncVolumeEntities(ctx, vol.ID, badLink); !errors.Is(err, storage.ErrAutomaticSyncLockedFields) {
			t.Fatalf("SyncVolumeEntities with locked_fields = %v, want ErrAutomaticSyncLockedFields", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx rejection checks: %v", err)
	}

	// Perform syncs: locked person/entity fields and locked credit/link rows stay untouched,
	// while unlocked fields/credits/links are updated/replaced.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		newNative := "Changed Native"
		newRoman := "Eiichiro Oda Updated"
		syncedPerson := domain.Person{
			ID:            personLocked.ID,
			Name:          "Changed Name",
			NameNative:    &newNative,
			NameRomanized: &newRoman,
			Aliases:       []string{"Changed Alias"},
			ExternalIDs:   map[string]any{"anilist": 9999, "mu": 42},
		}
		if err := tx.People().SyncPerson(ctx, &syncedPerson); err != nil {
			return err
		}
		// Locked fields + import_key + locked_fields remain intact; unlocked fields updated.
		if syncedPerson.Name != "Eiichiro Oda" ||
			syncedPerson.NameNative == nil || *syncedPerson.NameNative != "尾田栄一郎" ||
			!slices.Equal(syncedPerson.Aliases, []string{"Oda"}) ||
			syncedPerson.ImportKey == nil || *syncedPerson.ImportKey != *personLocked.ImportKey ||
			!maps.Equal(syncedPerson.LockedFields, personLocked.LockedFields) {
			t.Fatalf("SyncPerson did not preserve locked fields: %+v", syncedPerson)
		}
		if syncedPerson.NameRomanized == nil || *syncedPerson.NameRomanized != newRoman ||
			syncedPerson.ExternalIDs["mu"] != json.Number("42") {
			t.Fatalf("SyncPerson did not update unlocked fields: %+v", syncedPerson)
		}

		// Wildcard "*" lock leaves every field untouched.
		syncedWild := domain.Person{
			ID:          personWild.ID,
			Name:        "Should Not Change",
			ExternalIDs: map[string]any{"anilist": 1},
		}
		if err := tx.People().SyncPerson(ctx, &syncedWild); err != nil {
			return err
		}
		if syncedWild.Name != "Naoki Urasawa" || syncedWild.NameNative == nil || *syncedWild.NameNative != "浦沢直樹" {
			t.Fatalf("SyncPerson wildcard lock mutated person: %+v", syncedWild)
		}

		// SyncEntity preserves locked name/external_ids while updating unlocked aliases.
		syncedEnt := domain.Entity{
			ID:          entityLocked.ID,
			Kind:        domain.EntityKindCharacter,
			Name:        "Luffy Renamed",
			Aliases:     []string{"Straw Hat Luffy", "Fifth Emperor"},
			ExternalIDs: map[string]any{"anilist": 999},
		}
		if err := tx.Entities().SyncEntity(ctx, &syncedEnt); err != nil {
			return err
		}
		if syncedEnt.Name != "Monkey D. Luffy" ||
			syncedEnt.ExternalIDs["anilist"] != json.Number("40") ||
			syncedEnt.ImportKey == nil || *syncedEnt.ImportKey != *entityLocked.ImportKey ||
			!maps.Equal(syncedEnt.LockedFields, entityLocked.LockedFields) {
			t.Fatalf("SyncEntity did not preserve locked fields: %+v", syncedEnt)
		}
		if !slices.Equal(syncedEnt.Aliases, []string{"Straw Hat Luffy", "Fifth Emperor"}) {
			t.Fatalf("SyncEntity did not update unlocked aliases: %+v", syncedEnt)
		}

		// SyncSeriesCredits: omit lockedSeriesCred, remove unlockedSeriesCred, add new unlocked credit.
		if err := tx.Credits().SyncSeriesCredits(ctx, series.ID, []domain.Credit{
			{PersonID: personWild.ID, SeriesID: &series.ID, Role: domain.CreditRoleWriter, Position: 3},
		}); err != nil {
			return err
		}

		// SyncVolumeCredits: pass lockedVolCred with a DIFFERENT position (99) + replace unlocked translator with colorist.
		if err := tx.Credits().SyncVolumeCredits(ctx, vol.ID, []domain.Credit{
			{PersonID: personLocked.ID, VolumeID: &vol.ID, Role: domain.CreditRoleWriter, Position: 99},
			{PersonID: personWild.ID, VolumeID: &vol.ID, Role: domain.CreditRoleColorist, Position: 2},
		}); err != nil {
			return err
		}

		// SyncVolumeEntities: omit locked entityLocked, remove entityUnlocked, add a new entity.
		newEnt, err := tx.Entities().FindOrCreateEntityByName(ctx, domain.EntityKindLocation, "East Blue")
		if err != nil {
			return err
		}
		return tx.Entities().SyncVolumeEntities(ctx, vol.ID, []domain.VolumeEntity{
			{VolumeID: vol.ID, EntityID: newEnt.ID},
		})
	})
	if err != nil {
		t.Fatalf("WithTx syncs: %v", err)
	}

	creditRepo := postgres.NewCreditRepository(db.Pool)
	gotSeriesCreds, err := creditRepo.ListCreditsBySeries(ctx, series.ID)
	if err != nil {
		t.Fatalf("ListCreditsBySeries after sync: %v", err)
	}
	if len(gotSeriesCreds) != 2 {
		t.Fatalf("len(seriesCredits) = %d, want 2 (1 locked + 1 new)", len(gotSeriesCreds))
	}
	if gotSeriesCreds[0].PersonID != personLocked.ID || gotSeriesCreds[0].Role != domain.CreditRoleAuthor ||
		!maps.Equal(gotSeriesCreds[0].LockedFields, domain.FieldLocks{"role": true}) {
		t.Fatalf("locked series credit altered: %+v", gotSeriesCreds[0])
	}
	if gotSeriesCreds[1].PersonID != personWild.ID || gotSeriesCreds[1].Role != domain.CreditRoleWriter {
		t.Fatalf("new unlocked series credit missing: %+v", gotSeriesCreds[1])
	}

	gotVolCreds, err := creditRepo.ListCreditsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListCreditsByVolume after sync: %v", err)
	}
	if len(gotVolCreds) != 2 {
		t.Fatalf("len(volCredits) = %d, want 2 (1 locked + 1 new)", len(gotVolCreds))
	}
	if gotVolCreds[0].PersonID != personLocked.ID || gotVolCreds[0].Position != 0 ||
		!maps.Equal(gotVolCreds[0].LockedFields, domain.FieldLocks{"credit": true}) {
		t.Fatalf("locked volume credit altered (expected position 0 untouched): %+v", gotVolCreds[0])
	}
	if gotVolCreds[1].PersonID != personWild.ID || gotVolCreds[1].Role != domain.CreditRoleColorist {
		t.Fatalf("new unlocked volume credit missing: %+v", gotVolCreds[1])
	}

	entRepo := postgres.NewEntityRepository(db.Pool)
	gotLinks, err := entRepo.ListVolumeEntities(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListVolumeEntities after sync: %v", err)
	}
	if len(gotLinks) != 2 {
		t.Fatalf("len(volEntities) = %d, want 2 (1 locked + 1 new)", len(gotLinks))
	}
	foundLocked := false
	foundUnlocked := false
	for _, l := range gotLinks {
		if l.EntityID == entityLocked.ID {
			foundLocked = true
			if !maps.Equal(l.LockedFields, domain.FieldLocks{"entity": true}) {
				t.Fatalf("locked volume_entity lost lock: %+v", l)
			}
		}
		if l.EntityID == entityUnlocked.ID {
			foundUnlocked = true
		}
	}
	if !foundLocked {
		t.Fatal("locked volume_entity link was removed by SyncVolumeEntities")
	}
	if foundUnlocked {
		t.Fatal("unlocked volume_entity link should have been replaced by SyncVolumeEntities")
	}
}

func TestPeopleCreditsEntities_TxRollbackAtomicity(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sentinel := errors.New("abort transaction")
	var createdPersonID string
	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		p, err := tx.People().FindOrCreatePersonByName(ctx, "Rollback Person")
		if err != nil {
			return err
		}
		createdPersonID = p.ID
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTx error = %v, want sentinel", err)
	}

	_, err = postgres.NewPersonRepository(db.Pool).GetPersonByID(ctx, createdPersonID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected rolled-back person to be absent (ErrNotFound), got: %v", err)
	}
}

func TestPeopleCreditsEntities_IdempotentSyncAndCRUD(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		series domain.Series
		vol    domain.Volume
		person domain.Person
		entity domain.Entity
		credit domain.Credit
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ := seedTestLibraryAndRoot(ctx, t, tx, "Books", []byte("/srv/books"))
		series = seedMembershipSeries(ctx, t, tx, lib.ID, "Series Idem")
		vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 1)

		p, err := tx.People().FindOrCreatePersonByName(ctx, "Kentaro Miura")
		if err != nil {
			return err
		}
		person = *p
		native := "三浦建太郎"
		person.NameNative = &native
		person.Aliases = []string{"Miura"}
		person.ExternalIDs = map[string]any{"anilist": 1868}
		if err := tx.People().UpdatePerson(ctx, &person); err != nil {
			return err
		}

		e, err := tx.Entities().FindOrCreateEntityByName(ctx, domain.EntityKindCharacter, "Guts")
		if err != nil {
			return err
		}
		entity = *e
		entity.Aliases = []string{"Black Swordsman"}
		if err := tx.Entities().UpdateEntity(ctx, &entity); err != nil {
			return err
		}

		if err := tx.Credits().SyncVolumeCredits(ctx, vol.ID, []domain.Credit{
			{PersonID: person.ID, VolumeID: &vol.ID, Role: domain.CreditRoleAuthor, Position: 0},
		}); err != nil {
			return err
		}
		if err := tx.Credits().SyncSeriesCredits(ctx, series.ID, []domain.Credit{
			{PersonID: person.ID, SeriesID: &series.ID, Role: domain.CreditRoleWriter, Position: 0},
		}); err != nil {
			return err
		}
		if err := tx.Entities().SyncVolumeEntities(ctx, vol.ID, []domain.VolumeEntity{
			{VolumeID: vol.ID, EntityID: entity.ID},
		}); err != nil {
			return err
		}

		gotVolCreds, err := tx.Credits().ListCreditsByVolume(ctx, vol.ID)
		if err != nil {
			return err
		}
		credit = gotVolCreds[0]

		// Lookup by import_key succeeds.
		byKeyP, err := tx.People().GetPersonByImportKey(ctx, *person.ImportKey)
		if err != nil || byKeyP.ID != person.ID {
			t.Fatalf("GetPersonByImportKey = %+v, err=%v", byKeyP, err)
		}
		byKeyE, err := tx.Entities().GetEntityByImportKey(ctx, domain.EntityKindCharacter, *entity.ImportKey)
		if err != nil || byKeyE.ID != entity.ID {
			t.Fatalf("GetEntityByImportKey = %+v, err=%v", byKeyE, err)
		}
		byEntLinks, err := tx.Entities().ListVolumeEntitiesByEntity(ctx, entity.ID)
		if err != nil || len(byEntLinks) != 1 || byEntLinks[0].VolumeID != vol.ID {
			t.Fatalf("ListVolumeEntitiesByEntity = %+v, err=%v", byEntLinks, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx setup: %v", err)
	}

	readXmins := func() (pXmin, eXmin, vcXmin, scXmin, veXmin string) {
		t.Helper()
		if err := db.QueryRow(ctx, `SELECT xmin::text FROM people WHERE id = $1`, person.ID).Scan(&pXmin); err != nil {
			t.Fatalf("read people xmin: %v", err)
		}
		if err := db.QueryRow(ctx, `SELECT xmin::text FROM entities WHERE id = $1`, entity.ID).Scan(&eXmin); err != nil {
			t.Fatalf("read entities xmin: %v", err)
		}
		if err := db.QueryRow(ctx, `SELECT xmin::text FROM credits WHERE id = $1`, credit.ID).Scan(&vcXmin); err != nil {
			t.Fatalf("read volume credit xmin: %v", err)
		}
		if err := db.QueryRow(ctx, `SELECT xmin::text FROM credits WHERE series_id = $1`, series.ID).Scan(&scXmin); err != nil {
			t.Fatalf("read series credit xmin: %v", err)
		}
		if err := db.QueryRow(ctx, `SELECT xmin::text FROM volume_entities WHERE volume_id = $1 AND entity_id = $2`, vol.ID, entity.ID).Scan(&veXmin); err != nil {
			t.Fatalf("read volume_entities xmin: %v", err)
		}
		return
	}

	p1, e1, vc1, sc1, ve1 := readXmins()

	// Re-running all sync helpers with identical inputs must execute zero writes (xmin unchanged).
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		samePerson := domain.Person{
			ID:          person.ID,
			Name:        person.Name,
			NameNative:  person.NameNative,
			Aliases:     slices.Clone(person.Aliases),
			ExternalIDs: map[string]any{"anilist": 1868},
		}
		if err := tx.People().SyncPerson(ctx, &samePerson); err != nil {
			return err
		}
		sameEntity := domain.Entity{
			ID:          entity.ID,
			Kind:        entity.Kind,
			Name:        entity.Name,
			Aliases:     slices.Clone(entity.Aliases),
			ExternalIDs: map[string]any{},
		}
		if err := tx.Entities().SyncEntity(ctx, &sameEntity); err != nil {
			return err
		}
		if err := tx.Credits().SyncVolumeCredits(ctx, vol.ID, []domain.Credit{
			{PersonID: person.ID, VolumeID: &vol.ID, Role: domain.CreditRoleAuthor, Position: 0},
		}); err != nil {
			return err
		}
		if err := tx.Credits().SyncSeriesCredits(ctx, series.ID, []domain.Credit{
			{PersonID: person.ID, SeriesID: &series.ID, Role: domain.CreditRoleWriter, Position: 0},
		}); err != nil {
			return err
		}
		return tx.Entities().SyncVolumeEntities(ctx, vol.ID, []domain.VolumeEntity{
			{VolumeID: vol.ID, EntityID: entity.ID},
		})
	})
	if err != nil {
		t.Fatalf("WithTx identical syncs: %v", err)
	}

	p2, e2, vc2, sc2, ve2 := readXmins()
	if p1 != p2 || e1 != e2 || vc1 != vc2 || sc1 != sc2 || ve1 != ve2 {
		t.Fatalf("identical sync rewrote rows: before=(%s,%s,%s,%s,%s) after=(%s,%s,%s,%s,%s)",
			p1, e1, vc1, sc1, ve1, p2, e2, vc2, sc2, ve2)
	}

	// UpdateCredit, DeleteCredit, DeleteEntity, DeletePerson & ErrNotFound checks.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		credit.Position = 4
		if err := tx.Credits().UpdateCredit(ctx, &credit); err != nil {
			return err
		}
		gotC, err := tx.Credits().GetCreditByID(ctx, credit.ID)
		if err != nil || gotC.Position != 4 {
			t.Fatalf("GetCreditByID after update = %+v, err=%v", gotC, err)
		}
		if err := tx.Credits().DeleteCredit(ctx, credit.ID); err != nil {
			return err
		}
		if err := tx.Credits().DeleteCredit(ctx, credit.ID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("second DeleteCredit = %v, want ErrNotFound", err)
		}
		if err := tx.Entities().DeleteEntity(ctx, entity.ID); err != nil {
			return err
		}
		if err := tx.Entities().DeleteEntity(ctx, entity.ID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("second DeleteEntity = %v, want ErrNotFound", err)
		}
		if err := tx.People().DeletePerson(ctx, person.ID); err != nil {
			return err
		}
		if err := tx.People().DeletePerson(ctx, person.ID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("second DeletePerson = %v, want ErrNotFound", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx CRUD delete checks: %v", err)
	}
}

func TestSyncHelpers_LockedCreditIDAndKeyedEntityKindPreservation(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		series           domain.Series
		vol              domain.Volume
		personLocked     domain.Person
		personUnlocked   domain.Person
		keyedEntity      domain.Entity
		unkeyedEntity    domain.Entity
		lockedSeriesID   string
		unlockedSeriesID string
		lockedVolID      string
		unlockedVolID    string
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ := seedTestLibraryAndRoot(ctx, t, tx, "Sync Protection", []byte("/srv/sync-protect"))
		series = seedMembershipSeries(ctx, t, tx, lib.ID, "Protected Series")
		vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 1)

		personLocked = domain.Person{Name: "Locked Creator"}
		if err := tx.People().CreatePerson(ctx, &personLocked); err != nil {
			return err
		}
		personUnlocked = domain.Person{Name: "Unlocked Creator"}
		if err := tx.People().CreatePerson(ctx, &personUnlocked); err != nil {
			return err
		}

		if err := tx.Credits().SetSeriesCredits(ctx, series.ID, []domain.Credit{
			{
				PersonID:     personLocked.ID,
				SeriesID:     &series.ID,
				Role:         domain.CreditRoleAuthor,
				Position:     0,
				LockedFields: domain.FieldLocks{"role": true},
			},
			{
				PersonID: personUnlocked.ID,
				SeriesID: &series.ID,
				Role:     domain.CreditRoleWriter,
				Position: 1,
			},
		}); err != nil {
			return err
		}
		sc, err := tx.Credits().ListCreditsBySeries(ctx, series.ID)
		if err != nil {
			return err
		}
		for _, c := range sc {
			if c.Role == domain.CreditRoleAuthor {
				lockedSeriesID = c.ID
			} else {
				unlockedSeriesID = c.ID
			}
		}

		if err := tx.Credits().SetVolumeCredits(ctx, vol.ID, []domain.Credit{
			{
				PersonID:     personLocked.ID,
				VolumeID:     &vol.ID,
				Role:         domain.CreditRoleWriter,
				Position:     0,
				LockedFields: domain.FieldLocks{"credit": true},
			},
			{
				PersonID: personUnlocked.ID,
				VolumeID: &vol.ID,
				Role:     domain.CreditRoleTranslator,
				Position: 1,
			},
		}); err != nil {
			return err
		}
		vc, err := tx.Credits().ListCreditsByVolume(ctx, vol.ID)
		if err != nil {
			return err
		}
		for _, c := range vc {
			if c.Role == domain.CreditRoleWriter {
				lockedVolID = c.ID
			} else {
				unlockedVolID = c.ID
			}
		}

		ke, err := tx.Entities().FindOrCreateEntityByName(ctx, domain.EntityKindCharacter, "Monkey D. Luffy")
		if err != nil {
			return err
		}
		keyedEntity = *ke

		unkeyedEntity = domain.Entity{
			Kind: domain.EntityKindCharacter,
			Name: "Curated Character",
		}
		if err := tx.Entities().CreateEntity(ctx, &unkeyedEntity); err != nil {
			return err
		}

		return tx.Entities().SetVolumeEntities(ctx, vol.ID, []domain.VolumeEntity{
			{VolumeID: vol.ID, EntityID: keyedEntity.ID},
		})
	})
	if err != nil {
		t.Fatalf("WithTx setup: %v", err)
	}

	// Automatic sync carrying a locked credit's ID under a different role must not
	// overwrite the locked row or its locks on either series or volume credits.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		if err := tx.Credits().SyncSeriesCredits(ctx, series.ID, []domain.Credit{
			{ID: lockedSeriesID, PersonID: personLocked.ID, SeriesID: &series.ID, Role: domain.CreditRolePenciller, Position: 2},
			{ID: lockedSeriesID, PersonID: personUnlocked.ID, SeriesID: &series.ID, Role: domain.CreditRoleWriter, Position: 1},
		}); err != nil {
			return err
		}

		if err := tx.Credits().SyncVolumeCredits(ctx, vol.ID, []domain.Credit{
			{ID: lockedVolID, PersonID: personLocked.ID, VolumeID: &vol.ID, Role: domain.CreditRoleInker, Position: 2},
			{ID: lockedVolID, PersonID: personUnlocked.ID, VolumeID: &vol.ID, Role: domain.CreditRoleTranslator, Position: 1},
		}); err != nil {
			return err
		}

		// SetSeriesCredits and SetVolumeCredits reject duplicate explicit credit IDs.
		if err := tx.Credits().SetSeriesCredits(ctx, series.ID, []domain.Credit{
			{ID: lockedSeriesID, PersonID: personLocked.ID, SeriesID: &series.ID, Role: domain.CreditRoleAuthor, Position: 0},
			{ID: lockedSeriesID, PersonID: personUnlocked.ID, SeriesID: &series.ID, Role: domain.CreditRoleWriter, Position: 1},
		}); err == nil {
			t.Fatal("SetSeriesCredits accepted duplicate explicit credit IDs")
		}
		if err := tx.Credits().SetVolumeCredits(ctx, vol.ID, []domain.Credit{
			{ID: lockedVolID, PersonID: personLocked.ID, VolumeID: &vol.ID, Role: domain.CreditRoleWriter, Position: 0},
			{ID: lockedVolID, PersonID: personUnlocked.ID, VolumeID: &vol.ID, Role: domain.CreditRoleTranslator, Position: 1},
		}); err == nil {
			t.Fatal("SetVolumeCredits accepted duplicate explicit credit IDs")
		}

		// SyncEntity on a keyed entity preserves Kind even when "kind" is not in LockedFields
		// and a conflicting Kind is proposed.
		syncedKeyed := domain.Entity{
			ID:      keyedEntity.ID,
			Kind:    domain.EntityKindTeam,
			Name:    keyedEntity.Name,
			Aliases: []string{"Straw Hat"},
		}
		if err := tx.Entities().SyncEntity(ctx, &syncedKeyed); err != nil {
			return err
		}
		if syncedKeyed.Kind != domain.EntityKindCharacter {
			t.Fatalf("SyncEntity changed keyed entity kind to %q, want %q", syncedKeyed.Kind, domain.EntityKindCharacter)
		}
		if !slices.Equal(syncedKeyed.Aliases, []string{"Straw Hat"}) {
			t.Fatalf("SyncEntity did not update unlocked aliases: %+v", syncedKeyed)
		}

		// Unkeyed entity without "kind" locked still allows SyncEntity to update Kind.
		syncedUnkeyed := domain.Entity{
			ID:   unkeyedEntity.ID,
			Kind: domain.EntityKindTeam,
			Name: unkeyedEntity.Name,
		}
		if err := tx.Entities().SyncEntity(ctx, &syncedUnkeyed); err != nil {
			return err
		}
		if syncedUnkeyed.Kind != domain.EntityKindTeam {
			t.Fatalf("SyncEntity on unkeyed entity did not update kind: got %q, want %q", syncedUnkeyed.Kind, domain.EntityKindTeam)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx sync checks: %v", err)
	}

	creditRepo := postgres.NewCreditRepository(db.Pool)
	seriesCredits, err := creditRepo.ListCreditsBySeries(ctx, series.ID)
	if err != nil {
		t.Fatalf("ListCreditsBySeries: %v", err)
	}
	if len(seriesCredits) != 3 {
		t.Fatalf("len(seriesCredits) = %d, want 3", len(seriesCredits))
	}
	if seriesCredits[0].ID != lockedSeriesID || seriesCredits[0].Role != domain.CreditRoleAuthor ||
		seriesCredits[0].Position != 0 || !maps.Equal(seriesCredits[0].LockedFields, domain.FieldLocks{"role": true}) {
		t.Fatalf("locked series credit altered: %+v", seriesCredits[0])
	}
	if seriesCredits[1].ID != unlockedSeriesID || seriesCredits[1].Role != domain.CreditRoleWriter {
		t.Fatalf("unlocked series credit lost persisted ID: %+v (want %s)", seriesCredits[1], unlockedSeriesID)
	}
	if seriesCredits[2].Role != domain.CreditRolePenciller || seriesCredits[2].ID == lockedSeriesID || seriesCredits[2].ID == unlockedSeriesID {
		t.Fatalf("new series credit invalid or collided: %+v", seriesCredits[2])
	}

	volCredits, err := creditRepo.ListCreditsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListCreditsByVolume: %v", err)
	}
	if len(volCredits) != 3 {
		t.Fatalf("len(volCredits) = %d, want 3", len(volCredits))
	}
	if volCredits[0].ID != lockedVolID || volCredits[0].Role != domain.CreditRoleWriter ||
		volCredits[0].Position != 0 || !maps.Equal(volCredits[0].LockedFields, domain.FieldLocks{"credit": true}) {
		t.Fatalf("locked volume credit altered: %+v", volCredits[0])
	}
	if volCredits[1].ID != unlockedVolID || volCredits[1].Role != domain.CreditRoleTranslator {
		t.Fatalf("unlocked volume credit lost persisted ID: %+v (want %s)", volCredits[1], unlockedVolID)
	}
	if volCredits[2].Role != domain.CreditRoleInker || volCredits[2].ID == lockedVolID || volCredits[2].ID == unlockedVolID {
		t.Fatalf("new volume credit invalid or collided: %+v", volCredits[2])
	}

	// Verify the original kind + import_key + ID still resolve and no duplicate was created.
	entRepo := postgres.NewEntityRepository(db.Pool)
	byKey, err := entRepo.GetEntityByImportKey(ctx, domain.EntityKindCharacter, *keyedEntity.ImportKey)
	if err != nil {
		t.Fatalf("GetEntityByImportKey after conflicting SyncEntity: %v", err)
	}
	if byKey.ID != keyedEntity.ID || byKey.Kind != domain.EntityKindCharacter {
		t.Fatalf("GetEntityByImportKey = %+v, want ID=%s Kind=CHARACTER", byKey, keyedEntity.ID)
	}
	reimported, err := entRepo.FindOrCreateEntityByName(ctx, domain.EntityKindCharacter, "  monkey d. luffy ")
	if err != nil {
		t.Fatalf("FindOrCreateEntityByName after conflicting SyncEntity: %v", err)
	}
	if reimported.ID != keyedEntity.ID {
		t.Fatalf("FindOrCreateEntityByName created second ID %s, want %s", reimported.ID, keyedEntity.ID)
	}
	if _, err := entRepo.GetEntityByImportKey(ctx, domain.EntityKindTeam, *keyedEntity.ImportKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetEntityByImportKey(TEAM) = %v, want ErrNotFound", err)
	}
	links, err := entRepo.ListVolumeEntities(ctx, vol.ID)
	if err != nil || len(links) != 1 || links[0].EntityID != keyedEntity.ID {
		t.Fatalf("ListVolumeEntities = %+v, err=%v", links, err)
	}
}
