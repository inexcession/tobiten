package postgres_test

import (
	"context"
	"errors"
	"maps"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
)

func seedTestPrincipalScope(ctx context.Context, t *testing.T, tx storage.Tx, username string) storage.CatalogueReadScope {
	t.Helper()
	now := time.Now().UTC()
	u := domain.User{
		ID:        domain.MustNewID(),
		Username:  username,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := tx.Users().CreateUser(ctx, &u); err != nil {
		t.Fatalf("CreateUser(%q): %v", username, err)
	}
	role, err := tx.Users().GetRoleByName(ctx, domain.RoleReaderName)
	if err != nil {
		t.Fatalf("GetRoleByName(%q): %v", domain.RoleReaderName, err)
	}
	if err := tx.Users().AssignUserRole(ctx, u.ID, role.ID); err != nil {
		t.Fatalf("AssignUserRole: %v", err)
	}
	return storage.CatalogueReadScope{
		Principal: domain.NewPrincipal(u, []domain.Role{*role}, domain.CredentialKindPassword, "cred-"+u.ID, nil),
	}
}

func seedMembershipSeries(
	ctx context.Context,
	t *testing.T,
	tx storage.Tx,
	libraryID string,
	title string,
) domain.Series {
	t.Helper()
	s := domain.Series{
		LibraryID: libraryID,
		Title:     title,
		TitleSort: title,
		Status:    domain.SeriesStatusOngoing,
	}
	if err := tx.CatalogueWrites().CreateSeries(ctx, &s); err != nil {
		t.Fatalf("CreateSeries(%q): %v", title, err)
	}
	return s
}

func seedMembershipVolume(
	ctx context.Context,
	t *testing.T,
	tx storage.Tx,
	libraryID string,
	kind domain.VolumeKind,
	num float64,
) domain.Volume {
	t.Helper()
	n := num
	v := domain.Volume{
		LibraryID: libraryID,
		Kind:      kind,
		Number:    &n,
	}
	if err := tx.CatalogueWrites().CreateVolume(ctx, &v); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	return v
}

func membershipFloatPtr(v float64) *float64 {
	return &v
}

func assertMembershipsMatch(t *testing.T, got, want []domain.SeriesMembership) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(memberships) = %d, want %d\ngot:  %+v\nwant: %+v", len(got), len(want), got, want)
	}
	for i := range want {
		g := got[i]
		w := want[i]
		if g.VolumeID != w.VolumeID ||
			g.SeriesID != w.SeriesID ||
			g.IsPrimary != w.IsPrimary ||
			g.Kind != w.Kind ||
			g.Source != w.Source {
			t.Fatalf("membership[%d] = %+v, want %+v", i, g, w)
		}
		if (g.Position == nil) != (w.Position == nil) || (g.Position != nil && *g.Position != *w.Position) {
			t.Fatalf("membership[%d].Position = %v, want %v", i, g.Position, w.Position)
		}
		if !maps.Equal(g.LockedFields, w.LockedFields) {
			t.Fatalf("membership[%d].LockedFields = %v, want %v", i, g.LockedFields, w.LockedFields)
		}
	}
}

func TestSeriesMembershipRepository_StandaloneZeroMembershipAndDetachment(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		scope storage.CatalogueReadScope
		vol   domain.Volume
		s1    domain.Series
		s2    domain.Series
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		scope = seedTestPrincipalScope(ctx, t, tx, "standalone-reader")
		lib, _ := seedTestLibraryAndRoot(ctx, t, tx, "Books", []byte("/srv/books"))
		s1 = seedMembershipSeries(ctx, t, tx, lib.ID, "Main")
		s2 = seedMembershipSeries(ctx, t, tx, lib.ID, "Sub")
		vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 1)

		// Standalone volume starts with zero memberships.
		got, err := tx.SeriesMemberships().ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			return err
		}
		if len(got) != 0 {
			t.Fatalf("expected 0 memberships for standalone volume, got %+v", got)
		}

		// Explicit empty SetVolumeMemberships on standalone is idempotent.
		return tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, nil)
	})
	if err != nil {
		t.Fatalf("WithTx standalone setup: %v", err)
	}

	catRepo := postgres.NewCatalogueRepository(db.Pool)
	loadedVol, err := catRepo.GetVolumeByID(ctx, scope, vol.ID)
	if err != nil {
		t.Fatalf("CatalogueRepository.GetVolumeByID(standalone): %v", err)
	}
	if !loadedVol.IsStandalone() {
		t.Fatalf("expected standalone volume from CatalogueRepository, got %+v", loadedVol.Memberships)
	}

	// Attach primary + secondary, then detach back to standalone.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		sm := tx.SeriesMemberships()
		return sm.SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
			{
				VolumeID:  vol.ID,
				SeriesID:  s1.ID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Position:  membershipFloatPtr(1),
				Source:    domain.MembershipSourceResolver,
			},
			{
				VolumeID:  vol.ID,
				SeriesID:  s2.ID,
				IsPrimary: false,
				Kind:      domain.MembershipKindSubSeries,
				Position:  membershipFloatPtr(2),
				Source:    domain.MembershipSourceMetadata,
			},
		})
	})
	if err != nil {
		t.Fatalf("WithTx attach: %v", err)
	}

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		sm := tx.SeriesMemberships()
		if err := sm.DetachVolumeToStandalone(ctx, vol.ID); err != nil {
			return err
		}
		// Idempotent second detach.
		return sm.DetachVolumeToStandalone(ctx, vol.ID)
	})
	if err != nil {
		t.Fatalf("WithTx detach: %v", err)
	}

	detached, err := postgres.NewSeriesMembershipRepository(db.Pool).ListMembershipsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume after detach: %v", err)
	}
	if len(detached) != 0 {
		t.Fatalf("expected 0 memberships after detach, got %+v", detached)
	}

	loadedAfter, err := catRepo.GetVolumeByID(ctx, scope, vol.ID)
	if err != nil {
		t.Fatalf("GetVolumeByID after detach: %v", err)
	}
	if !loadedAfter.IsStandalone() {
		t.Fatalf("expected standalone volume after detach, got %+v", loadedAfter.Memberships)
	}

	// Missing volume returns ErrNotFound on detach.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.SeriesMemberships().DetachVolumeToStandalone(ctx, domain.MustNewID())
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("DetachVolumeToStandalone(missing) = %v, want storage.ErrNotFound", err)
	}
}

func TestSeriesMembershipRepository_OnePrimaryAndMultiSecondaryRoundTrip(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		scope   storage.CatalogueReadScope
		vol     domain.Volume
		sMain   domain.Series
		sSub    domain.Series
		sUniv   domain.Series
		sOther  domain.Series
		sNewPri domain.Series
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		scope = seedTestPrincipalScope(ctx, t, tx, "multi-reader")
		lib, _ := seedTestLibraryAndRoot(ctx, t, tx, "Manga", []byte("/srv/manga"))
		sMain = seedMembershipSeries(ctx, t, tx, lib.ID, "Discworld")
		sSub = seedMembershipSeries(ctx, t, tx, lib.ID, "City Watch")
		sUniv = seedMembershipSeries(ctx, t, tx, lib.ID, "Ankh-Morpork")
		sOther = seedMembershipSeries(ctx, t, tx, lib.ID, "Reading Club")
		sNewPri = seedMembershipSeries(ctx, t, tx, lib.ID, "Witches")
		vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 8)

		desired := []domain.SeriesMembership{
			{
				VolumeID:  vol.ID,
				SeriesID:  sSub.ID,
				IsPrimary: false,
				Kind:      domain.MembershipKindSubSeries,
				Position:  membershipFloatPtr(2),
				Source:    domain.MembershipSourceMetadata,
			},
			{
				VolumeID:  vol.ID,
				SeriesID:  sMain.ID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Position:  membershipFloatPtr(8),
				Source:    domain.MembershipSourceResolver,
			},
			{
				VolumeID:     vol.ID,
				SeriesID:     sUniv.ID,
				IsPrimary:    false,
				Kind:         domain.MembershipKindUniverse,
				Position:     membershipFloatPtr(14.5),
				Source:       domain.MembershipSourceManual,
				LockedFields: domain.FieldLocks{"position": true},
			},
			{
				VolumeID:  vol.ID,
				SeriesID:  sOther.ID,
				IsPrimary: false,
				Kind:      domain.MembershipKindOther,
				Position:  nil,
				Source:    domain.MembershipSourceResolver,
			},
		}
		return tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, desired)
	})
	if err != nil {
		t.Fatalf("WithTx multi-membership create: %v", err)
	}

	smRepo := postgres.NewSeriesMembershipRepository(db.Pool)
	got, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume: %v", err)
	}

	// Ordered primary first, then secondaries by series_id ascending.
	if len(got) != 4 || !got[0].IsPrimary || got[0].SeriesID != sMain.ID {
		t.Fatalf("expected primary %s first among 4 memberships, got %+v", sMain.ID, got)
	}

	gotUniv, err := smRepo.GetMembership(ctx, vol.ID, sUniv.ID)
	if err != nil {
		t.Fatalf("GetMembership(universe): %v", err)
	}
	if gotUniv.Kind != domain.MembershipKindUniverse ||
		gotUniv.Position == nil || *gotUniv.Position != 14.5 ||
		gotUniv.Source != domain.MembershipSourceManual ||
		!gotUniv.LockedFields.IsLocked("position") {
		t.Fatalf("unexpected universe membership round-trip: %+v", gotUniv)
	}

	bySeries, err := smRepo.ListMembershipsBySeries(ctx, sSub.ID)
	if err != nil {
		t.Fatalf("ListMembershipsBySeries: %v", err)
	}
	if len(bySeries) != 1 || bySeries[0].VolumeID != vol.ID || bySeries[0].IsPrimary {
		t.Fatalf("unexpected ListMembershipsBySeries result: %+v", bySeries)
	}

	// Verify CatalogueRepository returns all memberships with primary identified.
	catVol, err := postgres.NewCatalogueRepository(db.Pool).GetVolumeByID(ctx, scope, vol.ID)
	if err != nil {
		t.Fatalf("CatalogueRepository.GetVolumeByID: %v", err)
	}
	if len(catVol.Memberships) != 4 {
		t.Fatalf("len(catVol.Memberships) = %d, want 4", len(catVol.Memberships))
	}
	if priID, ok := catVol.PrimarySeriesID(); !ok || priID != sMain.ID {
		t.Fatalf("PrimarySeriesID = (%q, %v), want %q", priID, ok, sMain.ID)
	}

	// Invalid sets must be rejected before persistence.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		sm := tx.SeriesMemberships()
		invalidCases := [][]domain.SeriesMembership{
			{
				{VolumeID: vol.ID, SeriesID: sMain.ID, IsPrimary: false, Kind: domain.MembershipKindSeries, Source: domain.MembershipSourceResolver},
			},
			{
				{VolumeID: vol.ID, SeriesID: sMain.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Source: domain.MembershipSourceResolver},
				{VolumeID: vol.ID, SeriesID: sSub.ID, IsPrimary: true, Kind: domain.MembershipKindSubSeries, Source: domain.MembershipSourceMetadata},
			},
			{
				{VolumeID: vol.ID, SeriesID: sMain.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Source: domain.MembershipSourceResolver},
				{VolumeID: vol.ID, SeriesID: sMain.ID, IsPrimary: false, Kind: domain.MembershipKindSubSeries, Source: domain.MembershipSourceMetadata},
			},
			{
				{VolumeID: vol.ID, SeriesID: sMain.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(math.NaN()), Source: domain.MembershipSourceResolver},
			},
		}
		for i, cand := range invalidCases {
			if err := sm.SetVolumeMemberships(ctx, vol.ID, cand); err == nil {
				t.Fatalf("expected invalid SetVolumeMemberships case %d to fail", i)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx invalid cases: %v", err)
	}

	// Replace set: swap primary to sNewPri, demote sMain to secondary, keep sUniv, drop sSub & sOther.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
			{
				VolumeID:  vol.ID,
				SeriesID:  sNewPri.ID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Position:  membershipFloatPtr(3),
				Source:    domain.MembershipSourceManual,
			},
			{
				VolumeID:  vol.ID,
				SeriesID:  sMain.ID,
				IsPrimary: false,
				Kind:      domain.MembershipKindSubSeries,
				Position:  membershipFloatPtr(8),
				Source:    domain.MembershipSourceResolver,
			},
			{
				VolumeID:     vol.ID,
				SeriesID:     sUniv.ID,
				IsPrimary:    false,
				Kind:         domain.MembershipKindUniverse,
				Position:     membershipFloatPtr(14.5),
				Source:       domain.MembershipSourceManual,
				LockedFields: domain.FieldLocks{"position": true},
			},
		})
	})
	if err != nil {
		t.Fatalf("WithTx replace set: %v", err)
	}

	replaced, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume after replace: %v", err)
	}
	if len(replaced) != 3 || !replaced[0].IsPrimary || replaced[0].SeriesID != sNewPri.ID {
		t.Fatalf("unexpected replaced memberships: %+v", replaced)
	}
	if _, err := smRepo.GetMembership(ctx, vol.ID, sSub.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected dropped membership %s to return ErrNotFound, got %v", sSub.ID, err)
	}
}

func TestSeriesMembershipRepository_AtomicPrimaryPromotion(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		vol domain.Volume
		s1  domain.Series
		s2  domain.Series
		s3  domain.Series
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ := seedTestLibraryAndRoot(ctx, t, tx, "Comics", []byte("/srv/comics"))
		s1 = seedMembershipSeries(ctx, t, tx, lib.ID, "Series One")
		s2 = seedMembershipSeries(ctx, t, tx, lib.ID, "Series Two")
		s3 = seedMembershipSeries(ctx, t, tx, lib.ID, "Series Three")
		vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindIssue, 1)

		return tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
			{
				VolumeID:     vol.ID,
				SeriesID:     s1.ID,
				IsPrimary:    true,
				Kind:         domain.MembershipKindSeries,
				Position:     membershipFloatPtr(1),
				Source:       domain.MembershipSourceResolver,
				LockedFields: domain.FieldLocks{"position": true},
			},
			{
				VolumeID:     vol.ID,
				SeriesID:     s2.ID,
				IsPrimary:    false,
				Kind:         domain.MembershipKindSubSeries,
				Position:     membershipFloatPtr(42),
				Source:       domain.MembershipSourceMetadata,
				LockedFields: domain.FieldLocks{"kind": true},
			},
			{
				VolumeID:  vol.ID,
				SeriesID:  s3.ID,
				IsPrimary: false,
				Kind:      domain.MembershipKindUniverse,
				Position:  membershipFloatPtr(7),
				Source:    domain.MembershipSourceManual,
			},
		})
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Promote s2 to primary inside storage.Tx.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		sm := tx.SeriesMemberships()
		if err := sm.PromotePrimary(ctx, vol.ID, s2.ID); err != nil {
			return err
		}
		// Idempotent re-promotion of the current primary.
		return sm.PromotePrimary(ctx, vol.ID, s2.ID)
	})
	if err != nil {
		t.Fatalf("PromotePrimary(s2): %v", err)
	}

	smRepo := postgres.NewSeriesMembershipRepository(db.Pool)
	got, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume: %v", err)
	}
	if len(got) != 3 || !got[0].IsPrimary || got[0].SeriesID != s2.ID {
		t.Fatalf("expected s2 promoted to primary, got %+v", got)
	}
	if got[0].Kind != domain.MembershipKindSubSeries || *got[0].Position != 42 ||
		got[0].Source != domain.MembershipSourceMetadata || !got[0].LockedFields.IsLocked("kind") {
		t.Fatalf("expected promoted membership fields preserved, got %+v", got[0])
	}

	gotS1, err := smRepo.GetMembership(ctx, vol.ID, s1.ID)
	if err != nil {
		t.Fatalf("GetMembership(s1): %v", err)
	}
	if gotS1.IsPrimary || !gotS1.LockedFields.IsLocked("position") || *gotS1.Position != 1 {
		t.Fatalf("expected demoted s1 to retain fields, got %+v", gotS1)
	}

	// Promoting a non-member series or missing volume returns ErrNotFound.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.SeriesMemberships().PromotePrimary(ctx, vol.ID, domain.MustNewID())
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("PromotePrimary(non-member) = %v, want storage.ErrNotFound", err)
	}
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.SeriesMemberships().PromotePrimary(ctx, domain.MustNewID(), s2.ID)
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("PromotePrimary(missing volume) = %v, want storage.ErrNotFound", err)
	}

	// Concurrent promotions serialize on the volume row and preserve the 1-primary invariant.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, targetSeries := range []string{s1.ID, s3.ID} {
		wg.Go(func() {
			errs <- transactor.WithTx(ctx, func(tx storage.Tx) error {
				return tx.SeriesMemberships().PromotePrimary(ctx, vol.ID, targetSeries)
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent PromotePrimary failed: %v", err)
		}
	}

	afterConcurrent, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume after concurrent promote: %v", err)
	}
	if err := domain.ValidateSeriesMemberships(vol.ID, afterConcurrent); err != nil {
		t.Fatalf("invalid memberships after concurrent promote: %v (%+v)", err, afterConcurrent)
	}
}

func TestSeriesMembershipRepository_CrossLibraryAndCoverVolumeRejections(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		libA, libB domain.Library
		sA1, sA2   domain.Series
		sB         domain.Series
		volA       domain.Volume
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		libA, _ = seedTestLibraryAndRoot(ctx, t, tx, "Library A", []byte("/srv/a"))
		libB, _ = seedTestLibraryAndRoot(ctx, t, tx, "Library B", []byte("/srv/b"))
		sA1 = seedMembershipSeries(ctx, t, tx, libA.ID, "Series A1")
		sA2 = seedMembershipSeries(ctx, t, tx, libA.ID, "Series A2")
		sB = seedMembershipSeries(ctx, t, tx, libB.ID, "Series B")
		volA = seedMembershipVolume(ctx, t, tx, libA.ID, domain.VolumeKindVolume, 1)

		return tx.SeriesMemberships().SetVolumeMemberships(ctx, volA.ID, []domain.SeriesMembership{
			{
				VolumeID:  volA.ID,
				SeriesID:  sA1.ID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Position:  membershipFloatPtr(1),
				Source:    domain.MembershipSourceResolver,
			},
			{
				VolumeID:  volA.ID,
				SeriesID:  sA2.ID,
				IsPrimary: false,
				Kind:      domain.MembershipKindSubSeries,
				Position:  membershipFloatPtr(1),
				Source:    domain.MembershipSourceMetadata,
			},
		})
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// 1. Cross-library SetVolumeMemberships + Outbox event must reject at commit and roll back completely.
	crossErr := transactor.WithTx(ctx, func(tx storage.Tx) error {
		if err := tx.SeriesMemberships().SetVolumeMemberships(ctx, volA.ID, []domain.SeriesMembership{
			{
				VolumeID:  volA.ID,
				SeriesID:  sA1.ID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Source:    domain.MembershipSourceResolver,
			},
			{
				VolumeID:  volA.ID,
				SeriesID:  sB.ID,
				IsPrimary: false,
				Kind:      domain.MembershipKindSubSeries,
				Source:    domain.MembershipSourceMetadata,
			},
		}); err != nil {
			return err
		}
		ev, err := domain.NewDomainEvent(
			domain.EventAssetAdded,
			domain.EventVersion1,
			domain.AggregateTypeVolume,
			volA.ID,
			map[string]string{"note": "should rollback"},
			time.Now().UTC(),
		)
		if err != nil {
			return err
		}
		return tx.Events().AppendEvent(ctx, &ev)
	})
	if crossErr == nil {
		t.Fatal("expected cross-library SetVolumeMemberships to fail at commit")
	}
	var pgErr *pgconn.PgError
	if !errors.As(crossErr, &pgErr) {
		t.Fatalf("expected PostgreSQL trigger error, got %v", crossErr)
	}

	var eventCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1`, volA.ID).Scan(&eventCount); err != nil {
		t.Fatalf("querying rolled-back event: %v", err)
	}
	if eventCount != 0 {
		t.Fatalf("expected rolled-back outbox event count 0, got %d", eventCount)
	}

	// Cross-library SyncResolverMemberships is also rejected at commit.
	crossSyncErr := transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.SeriesMemberships().SyncResolverMemberships(ctx, volA.ID, []domain.SeriesMembership{
			{
				VolumeID:  volA.ID,
				SeriesID:  sB.ID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Source:    domain.MembershipSourceResolver,
			},
		})
	})
	if crossSyncErr == nil {
		t.Fatal("expected cross-library SyncResolverMemberships to fail at commit")
	}

	// Nonexistent series_id maps FK violation to storage.ErrNotFound.
	missingSeriesErr := transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.SeriesMemberships().SetVolumeMemberships(ctx, volA.ID, []domain.SeriesMembership{
			{
				VolumeID:  volA.ID,
				SeriesID:  domain.MustNewID(),
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Source:    domain.MembershipSourceResolver,
			},
		})
	})
	if !errors.Is(missingSeriesErr, storage.ErrNotFound) {
		t.Fatalf("SetVolumeMemberships(missing series) = %v, want storage.ErrNotFound", missingSeriesErr)
	}

	// 2. Cover-volume rejections: point sA1.cover_volume_id at volA.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		s, err := tx.CatalogueWrites().GetSeriesByID(ctx, sA1.ID)
		if err != nil {
			return err
		}
		manual := domain.CoverPolicyManual
		s.CoverPolicy = &manual
		s.CoverVolumeID = &volA.ID
		return tx.CatalogueWrites().UpdateSeries(ctx, s)
	})
	if err != nil {
		t.Fatalf("setting cover_volume_id: %v", err)
	}

	// Detaching volA while sA1 references it as cover_volume_id must reject at commit.
	detachCoverErr := transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.SeriesMemberships().DetachVolumeToStandalone(ctx, volA.ID)
	})
	if detachCoverErr == nil {
		t.Fatal("expected DetachVolumeToStandalone to reject while series cover_volume_id references volume")
	}

	// Dropping sA1 via SetVolumeMemberships or SyncResolverMemberships must also reject at commit.
	dropCoverErr := transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.SeriesMemberships().SetVolumeMemberships(ctx, volA.ID, []domain.SeriesMembership{
			{
				VolumeID:  volA.ID,
				SeriesID:  sA2.ID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Source:    domain.MembershipSourceResolver,
			},
		})
	})
	if dropCoverErr == nil {
		t.Fatal("expected dropping cover-volume series membership to reject at commit")
	}

	// Promoting sA2 while keeping sA1 as secondary succeeds because sA1 still has volA as a member.
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.SeriesMemberships().PromotePrimary(ctx, volA.ID, sA2.ID)
	}); err != nil {
		t.Fatalf("PromotePrimary(sA2) while sA1 uses cover_volume_id failed: %v", err)
	}

	// Clearing cover_volume_id and detaching in the SAME deferred transaction succeeds.
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		if err := tx.SeriesMemberships().DetachVolumeToStandalone(ctx, volA.ID); err != nil {
			return err
		}
		s, err := tx.CatalogueWrites().GetSeriesByID(ctx, sA1.ID)
		if err != nil {
			return err
		}
		s.CoverPolicy = nil
		s.CoverVolumeID = nil
		return tx.CatalogueWrites().UpdateSeries(ctx, s)
	}); err != nil {
		t.Fatalf("clearing cover_volume_id and detaching in one tx failed: %v", err)
	}

	afterClear, err := postgres.NewSeriesMembershipRepository(db.Pool).ListMembershipsByVolume(ctx, volA.ID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume after clear+detach: %v", err)
	}
	if len(afterClear) != 0 {
		t.Fatalf("expected 0 memberships after clear+detach, got %+v", afterClear)
	}
}

func TestSeriesMembershipRepository_SyncResolverMembershipsPreservesManualAndLocked(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var (
		lib domain.Library
		s1  domain.Series
		s2  domain.Series
		s3  domain.Series
		s4  domain.Series
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ = seedTestLibraryAndRoot(ctx, t, tx, "SyncLib", []byte("/srv/synclib"))
		s1 = seedMembershipSeries(ctx, t, tx, lib.ID, "Series 1")
		s2 = seedMembershipSeries(ctx, t, tx, lib.ID, "Series 2")
		s3 = seedMembershipSeries(ctx, t, tx, lib.ID, "Series 3")
		s4 = seedMembershipSeries(ctx, t, tx, lib.ID, "Series 4")
		return nil
	})
	if err != nil {
		t.Fatalf("seed series failed: %v", err)
	}

	smRepo := postgres.NewSeriesMembershipRepository(db.Pool)

	t.Run("unprotected memberships replace and detach cleanly", func(t *testing.T) {
		var vol domain.Volume
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 1)
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{VolumeID: vol.ID, SeriesID: s1.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(1), Source: domain.MembershipSourceResolver},
				{VolumeID: vol.ID, SeriesID: s2.ID, IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: membershipFloatPtr(2), Source: domain.MembershipSourceMetadata},
			})
		}); err != nil {
			t.Fatalf("initial sync: %v", err)
		}

		// Replace with new primary s3 and secondary s4.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{VolumeID: vol.ID, SeriesID: s3.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(5), Source: domain.MembershipSourceResolver},
				{VolumeID: vol.ID, SeriesID: s4.ID, IsPrimary: false, Kind: domain.MembershipKindUniverse, Position: membershipFloatPtr(9), Source: domain.MembershipSourceMetadata},
			})
		}); err != nil {
			t.Fatalf("replace sync: %v", err)
		}

		got, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume: %v", err)
		}
		assertMembershipsMatch(t, got, []domain.SeriesMembership{
			{VolumeID: vol.ID, SeriesID: s3.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(5), Source: domain.MembershipSourceResolver},
			{VolumeID: vol.ID, SeriesID: s4.ID, IsPrimary: false, Kind: domain.MembershipKindUniverse, Position: membershipFloatPtr(9), Source: domain.MembershipSourceMetadata},
		})

		// Standalone sync detaches unprotected memberships.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, nil)
		}); err != nil {
			t.Fatalf("standalone sync: %v", err)
		}
		gotEmpty, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume(standalone): %v", err)
		}
		if len(gotEmpty) != 0 {
			t.Fatalf("expected 0 memberships after standalone sync, got %+v", gotEmpty)
		}
	})

	t.Run("MANUAL primary survives divergent primary and standalone sync while unlocked secondaries sync", func(t *testing.T) {
		var vol domain.Volume
		manualPrimary := domain.SeriesMembership{
			VolumeID:  "",
			SeriesID:  s1.ID,
			IsPrimary: true,
			Kind:      domain.MembershipKindSeries,
			Position:  membershipFloatPtr(10),
			Source:    domain.MembershipSourceManual,
		}
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 10)
			manualPrimary.VolumeID = vol.ID
			return tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
				manualPrimary,
				{VolumeID: vol.ID, SeriesID: s2.ID, IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: membershipFloatPtr(1), Source: domain.MembershipSourceMetadata},
			})
		}); err != nil {
			t.Fatalf("seed manual primary: %v", err)
		}

		// Automatic sync proposes s3 as primary (must be ignored) and s4 as secondary (must apply, dropping unlocked s2).
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{VolumeID: vol.ID, SeriesID: s3.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(99), Source: domain.MembershipSourceResolver},
				{VolumeID: vol.ID, SeriesID: s4.ID, IsPrimary: false, Kind: domain.MembershipKindUniverse, Position: membershipFloatPtr(4), Source: domain.MembershipSourceMetadata},
			})
		}); err != nil {
			t.Fatalf("sync with divergent primary: %v", err)
		}

		got, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume: %v", err)
		}
		assertMembershipsMatch(t, got, []domain.SeriesMembership{
			manualPrimary,
			{VolumeID: vol.ID, SeriesID: s4.ID, IsPrimary: false, Kind: domain.MembershipKindUniverse, Position: membershipFloatPtr(4), Source: domain.MembershipSourceMetadata},
		})

		// Standalone automatic sync still keeps MANUAL primary intact and drops unlocked s4.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, nil)
		}); err != nil {
			t.Fatalf("sync standalone over manual primary: %v", err)
		}
		gotAfterStandalone, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume(after standalone): %v", err)
		}
		assertMembershipsMatch(t, gotAfterStandalone, []domain.SeriesMembership{manualPrimary})
	})

	t.Run("MANUAL secondary survives primary replacement, standalone sync, and primary collision", func(t *testing.T) {
		var vol domain.Volume
		var manualSec domain.SeriesMembership
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 2)
			manualSec = domain.SeriesMembership{
				VolumeID:  vol.ID,
				SeriesID:  s2.ID,
				IsPrimary: false,
				Kind:      domain.MembershipKindSubSeries,
				Position:  membershipFloatPtr(77),
				Source:    domain.MembershipSourceManual,
			}
			return tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{VolumeID: vol.ID, SeriesID: s1.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(2), Source: domain.MembershipSourceResolver},
				manualSec,
			})
		}); err != nil {
			t.Fatalf("seed manual secondary: %v", err)
		}

		// Replace unprotected primary s1 with s3 -> s2 MANUAL secondary survives.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{VolumeID: vol.ID, SeriesID: s3.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(3), Source: domain.MembershipSourceResolver},
			})
		}); err != nil {
			t.Fatalf("sync replacing primary: %v", err)
		}
		got, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume: %v", err)
		}
		assertMembershipsMatch(t, got, []domain.SeriesMembership{
			{VolumeID: vol.ID, SeriesID: s3.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(3), Source: domain.MembershipSourceResolver},
			manualSec,
		})

		// Proposing MANUAL secondary s2 as primary must not overwrite MANUAL secondary; s3 stays primary.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{VolumeID: vol.ID, SeriesID: s2.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(1), Source: domain.MembershipSourceResolver},
			})
		}); err != nil {
			t.Fatalf("sync colliding with manual secondary: %v", err)
		}
		gotCollision, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume(collision): %v", err)
		}
		assertMembershipsMatch(t, gotCollision, []domain.SeriesMembership{
			{VolumeID: vol.ID, SeriesID: s3.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(3), Source: domain.MembershipSourceResolver},
			manualSec,
		})

		// Standalone automatic sync retains primary s3 so surviving MANUAL secondary s2 remains valid.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, nil)
		}); err != nil {
			t.Fatalf("sync standalone with manual secondary: %v", err)
		}
		gotStandalone, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume(standalone): %v", err)
		}
		assertMembershipsMatch(t, gotStandalone, []domain.SeriesMembership{
			{VolumeID: vol.ID, SeriesID: s3.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(3), Source: domain.MembershipSourceResolver},
			manualSec,
		})
	})

	t.Run("locked_fields on non-MANUAL rows protect locked fields and allow position-locked primary replacement", func(t *testing.T) {
		var vol domain.Volume
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 5)
			return tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:     vol.ID,
					SeriesID:     s1.ID,
					IsPrimary:    true,
					Kind:         domain.MembershipKindSeries,
					Position:     membershipFloatPtr(5),
					Source:       domain.MembershipSourceResolver,
					LockedFields: domain.FieldLocks{"position": true},
				},
				{
					VolumeID:     vol.ID,
					SeriesID:     s2.ID,
					IsPrimary:    false,
					Kind:         domain.MembershipKindSubSeries,
					Position:     membershipFloatPtr(12),
					Source:       domain.MembershipSourceMetadata,
					LockedFields: domain.FieldLocks{"is_primary": true},
				},
			})
		}); err != nil {
			t.Fatalf("seed locked memberships: %v", err)
		}

		// 1. Same series s1 in desired: locked position (5) survives while unlocked fields update; s2 survives absent from desired.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:  vol.ID,
					SeriesID:  s1.ID,
					IsPrimary: true,
					Kind:      domain.MembershipKindOther,
					Position:  membershipFloatPtr(999),
					Source:    domain.MembershipSourceMetadata,
				},
			})
		}); err != nil {
			t.Fatalf("sync same series with locked position: %v", err)
		}
		got, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume: %v", err)
		}
		assertMembershipsMatch(t, got, []domain.SeriesMembership{
			{
				VolumeID:     vol.ID,
				SeriesID:     s1.ID,
				IsPrimary:    true,
				Kind:         domain.MembershipKindOther,
				Position:     membershipFloatPtr(5),
				Source:       domain.MembershipSourceMetadata,
				LockedFields: domain.FieldLocks{"position": true},
			},
			{
				VolumeID:     vol.ID,
				SeriesID:     s2.ID,
				IsPrimary:    false,
				Kind:         domain.MembershipKindSubSeries,
				Position:     membershipFloatPtr(12),
				Source:       domain.MembershipSourceMetadata,
				LockedFields: domain.FieldLocks{"is_primary": true},
			},
		})

		// 2. Standalone sync when no eligible replacement primary exists preserves position-locked s1 as primary.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, nil)
		}); err != nil {
			t.Fatalf("sync standalone over position-locked primary: %v", err)
		}
		gotStandalone, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume(standalone): %v", err)
		}
		assertMembershipsMatch(t, gotStandalone, got)

		// 3. Replacement primary s3 WITHOUT s1 in desired: s3 becomes primary and s1 is retained as secondary with locked position.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:  vol.ID,
					SeriesID:  s3.ID,
					IsPrimary: true,
					Kind:      domain.MembershipKindSeries,
					Position:  membershipFloatPtr(1),
					Source:    domain.MembershipSourceResolver,
				},
			})
		}); err != nil {
			t.Fatalf("sync replacing position-locked primary without s1 in desired: %v", err)
		}
		gotReplacedWithoutS1, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume(replaced without s1): %v", err)
		}
		assertMembershipsMatch(t, gotReplacedWithoutS1, []domain.SeriesMembership{
			{
				VolumeID:  vol.ID,
				SeriesID:  s3.ID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Position:  membershipFloatPtr(1),
				Source:    domain.MembershipSourceResolver,
			},
			{
				VolumeID:     vol.ID,
				SeriesID:     s1.ID,
				IsPrimary:    false,
				Kind:         domain.MembershipKindOther,
				Position:     membershipFloatPtr(5),
				Source:       domain.MembershipSourceMetadata,
				LockedFields: domain.FieldLocks{"position": true},
			},
			{
				VolumeID:     vol.ID,
				SeriesID:     s2.ID,
				IsPrimary:    false,
				Kind:         domain.MembershipKindSubSeries,
				Position:     membershipFloatPtr(12),
				Source:       domain.MembershipSourceMetadata,
				LockedFields: domain.FieldLocks{"is_primary": true},
			},
		})

		// 4. Reset primary to position-locked s1, then replace primary with s3 WITH s1 in desired as secondary.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().PromotePrimary(ctx, vol.ID, s1.ID)
		}); err != nil {
			t.Fatalf("PromotePrimary(s1): %v", err)
		}
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:  vol.ID,
					SeriesID:  s3.ID,
					IsPrimary: true,
					Kind:      domain.MembershipKindSeries,
					Position:  membershipFloatPtr(7),
					Source:    domain.MembershipSourceResolver,
				},
				{
					VolumeID:  vol.ID,
					SeriesID:  s1.ID,
					IsPrimary: false,
					Kind:      domain.MembershipKindSeries,
					Position:  membershipFloatPtr(777),
					Source:    domain.MembershipSourceResolver,
				},
			})
		}); err != nil {
			t.Fatalf("sync replacing position-locked primary with s1 in desired: %v", err)
		}
		gotReplacedWithS1, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume(replaced with s1): %v", err)
		}
		assertMembershipsMatch(t, gotReplacedWithS1, []domain.SeriesMembership{
			{
				VolumeID:  vol.ID,
				SeriesID:  s3.ID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Position:  membershipFloatPtr(7),
				Source:    domain.MembershipSourceResolver,
			},
			{
				VolumeID:     vol.ID,
				SeriesID:     s1.ID,
				IsPrimary:    false,
				Kind:         domain.MembershipKindSeries,
				Position:     membershipFloatPtr(5),
				Source:       domain.MembershipSourceResolver,
				LockedFields: domain.FieldLocks{"position": true},
			},
			{
				VolumeID:     vol.ID,
				SeriesID:     s2.ID,
				IsPrimary:    false,
				Kind:         domain.MembershipKindSubSeries,
				Position:     membershipFloatPtr(12),
				Source:       domain.MembershipSourceMetadata,
				LockedFields: domain.FieldLocks{"is_primary": true},
			},
		})

		// 5. Primary with is_primary lock cannot be replaced by divergent automatic sync.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().PromotePrimary(ctx, vol.ID, s2.ID)
		}); err != nil {
			t.Fatalf("PromotePrimary(s2): %v", err)
		}
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:  vol.ID,
					SeriesID:  s4.ID,
					IsPrimary: true,
					Kind:      domain.MembershipKindSeries,
					Position:  membershipFloatPtr(1),
					Source:    domain.MembershipSourceResolver,
				},
			})
		}); err != nil {
			t.Fatalf("sync over is_primary-locked primary s2: %v", err)
		}
		gotProtectedPrimary, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume(protected primary): %v", err)
		}
		if !gotProtectedPrimary[0].IsPrimary || gotProtectedPrimary[0].SeriesID != s2.ID {
			t.Fatalf("expected is_primary-locked s2 to remain primary, got %+v", gotProtectedPrimary)
		}
		if _, err := smRepo.GetMembership(ctx, vol.ID, s4.ID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected overridden primary s4 not to be added, got %v", err)
		}

		// 6. Reject MANUAL source and non-empty LockedFields in automatic sync input.
		err = transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:  vol.ID,
					SeriesID:  s1.ID,
					IsPrimary: true,
					Kind:      domain.MembershipKindSeries,
					Source:    domain.MembershipSourceManual,
				},
			})
		})
		if !errors.Is(err, domain.ErrInvalidMembershipSource) {
			t.Fatalf("SyncResolverMemberships(MANUAL input) = %v, want ErrInvalidMembershipSource", err)
		}

		err = transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:     vol.ID,
					SeriesID:     s4.ID,
					IsPrimary:    true,
					Kind:         domain.MembershipKindSeries,
					Source:       domain.MembershipSourceResolver,
					LockedFields: domain.FieldLocks{"is_primary": true},
				},
			})
		})
		if !errors.Is(err, storage.ErrAutomaticSyncLockedFields) {
			t.Fatalf("SyncResolverMemberships(LockedFields input) = %v, want ErrAutomaticSyncLockedFields", err)
		}
	})

	t.Run("automatic sync cannot inject locks onto new or unlocked rows before standalone detach", func(t *testing.T) {
		var vol domain.Volume
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 6)
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:  vol.ID,
					SeriesID:  s1.ID,
					IsPrimary: true,
					Kind:      domain.MembershipKindSeries,
					Position:  membershipFloatPtr(6),
					Source:    domain.MembershipSourceResolver,
				},
			})
		}); err != nil {
			t.Fatalf("initial sync: %v", err)
		}

		// Attempt to sneak a lock onto the existing unlocked row or a new row must fail and leave DB clean.
		for _, cand := range []domain.SeriesMembership{
			{
				VolumeID:     vol.ID,
				SeriesID:     s1.ID,
				IsPrimary:    true,
				Kind:         domain.MembershipKindSeries,
				Position:     membershipFloatPtr(6),
				Source:       domain.MembershipSourceResolver,
				LockedFields: domain.FieldLocks{"is_primary": true},
			},
			{
				VolumeID:     vol.ID,
				SeriesID:     s2.ID,
				IsPrimary:    true,
				Kind:         domain.MembershipKindSeries,
				Position:     membershipFloatPtr(6),
				Source:       domain.MembershipSourceResolver,
				LockedFields: domain.FieldLocks{"position": false},
			},
		} {
			err := transactor.WithTx(ctx, func(tx storage.Tx) error {
				return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{cand})
			})
			if !errors.Is(err, storage.ErrAutomaticSyncLockedFields) {
				t.Fatalf("SyncResolverMemberships(%+v) = %v, want ErrAutomaticSyncLockedFields", cand.LockedFields, err)
			}
		}

		// Subsequent standalone sync detaches cleanly because no lock was created.
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, nil)
		}); err != nil {
			t.Fatalf("standalone sync: %v", err)
		}
		got, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("expected 0 memberships after standalone detach, got %+v", got)
		}
	})

	t.Run("secondary with position-only lock can be promoted by automatic sync while preserving locked position", func(t *testing.T) {
		var vol domain.Volume
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			vol = seedMembershipVolume(ctx, t, tx, lib.ID, domain.VolumeKindVolume, 4)
			return tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:  vol.ID,
					SeriesID:  s1.ID,
					IsPrimary: true,
					Kind:      domain.MembershipKindSeries,
					Position:  membershipFloatPtr(4),
					Source:    domain.MembershipSourceResolver,
				},
				{
					VolumeID:     vol.ID,
					SeriesID:     s2.ID,
					IsPrimary:    false,
					Kind:         domain.MembershipKindSubSeries,
					Position:     membershipFloatPtr(88),
					Source:       domain.MembershipSourceMetadata,
					LockedFields: domain.FieldLocks{"position": true},
				},
			})
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}

		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.SeriesMemberships().SyncResolverMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{
					VolumeID:  vol.ID,
					SeriesID:  s2.ID,
					IsPrimary: true,
					Kind:      domain.MembershipKindSeries,
					Position:  membershipFloatPtr(999),
					Source:    domain.MembershipSourceResolver,
				},
			})
		}); err != nil {
			t.Fatalf("sync promoting locked-position secondary: %v", err)
		}

		got, err := smRepo.ListMembershipsByVolume(ctx, vol.ID)
		if err != nil {
			t.Fatalf("ListMembershipsByVolume: %v", err)
		}
		assertMembershipsMatch(t, got, []domain.SeriesMembership{
			{
				VolumeID:     vol.ID,
				SeriesID:     s2.ID,
				IsPrimary:    true,
				Kind:         domain.MembershipKindSeries,
				Position:     membershipFloatPtr(88),
				Source:       domain.MembershipSourceResolver,
				LockedFields: domain.FieldLocks{"position": true},
			},
		})
	})
}

func TestSeriesMembershipRepository_TransactionAtomicityAcrossRepositories(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib domain.Library
		s1  domain.Series
		s2  domain.Series
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ = seedTestLibraryAndRoot(ctx, t, tx, "TxLib", []byte("/srv/txlib"))
		s1 = seedMembershipSeries(ctx, t, tx, lib.ID, "Series A")
		s2 = seedMembershipSeries(ctx, t, tx, lib.ID, "Series B")
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	// 1. Committed transaction persists volume, memberships, and outbox event together.
	committedVolID := domain.MustNewID()
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		vol := domain.Volume{
			ID:        committedVolID,
			LibraryID: lib.ID,
			Kind:      domain.VolumeKindVolume,
		}
		if err := tx.CatalogueWrites().CreateVolume(ctx, &vol); err != nil {
			return err
		}
		if err := tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
			{VolumeID: vol.ID, SeriesID: s1.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(1), Source: domain.MembershipSourceResolver},
			{VolumeID: vol.ID, SeriesID: s2.ID, IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: membershipFloatPtr(2), Source: domain.MembershipSourceMetadata},
		}); err != nil {
			return err
		}
		ev, err := domain.NewDomainEvent(
			domain.EventAssetAdded,
			domain.EventVersion1,
			domain.AggregateTypeVolume,
			vol.ID,
			map[string]string{"status": "committed"},
			time.Now().UTC(),
		)
		if err != nil {
			return err
		}
		return tx.Events().AppendEvent(ctx, &ev)
	}); err != nil {
		t.Fatalf("committed multi-repo tx failed: %v", err)
	}

	smRepo := postgres.NewSeriesMembershipRepository(db.Pool)
	got, err := smRepo.ListMembershipsByVolume(ctx, committedVolID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume(committed): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 committed memberships, got %+v", got)
	}

	// 2. Rolled-back transaction leaves no volume, membership, or outbox event.
	rolledBackVolID := domain.MustNewID()
	sentinelErr := errors.New("abort transaction")
	rbErr := transactor.WithTx(ctx, func(tx storage.Tx) error {
		vol := domain.Volume{
			ID:        rolledBackVolID,
			LibraryID: lib.ID,
			Kind:      domain.VolumeKindVolume,
		}
		if err := tx.CatalogueWrites().CreateVolume(ctx, &vol); err != nil {
			return err
		}
		if err := tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
			{VolumeID: vol.ID, SeriesID: s1.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: membershipFloatPtr(1), Source: domain.MembershipSourceResolver},
		}); err != nil {
			return err
		}
		ev, err := domain.NewDomainEvent(
			domain.EventAssetAdded,
			domain.EventVersion1,
			domain.AggregateTypeVolume,
			vol.ID,
			map[string]string{"status": "rolled back"},
			time.Now().UTC(),
		)
		if err != nil {
			return err
		}
		if err := tx.Events().AppendEvent(ctx, &ev); err != nil {
			return err
		}
		return sentinelErr
	})
	if !errors.Is(rbErr, sentinelErr) {
		t.Fatalf("rolled-back tx err = %v, want sentinelErr", rbErr)
	}

	if _, err := postgres.NewCatalogueWriteRepository(db.Pool).GetVolumeByID(ctx, rolledBackVolID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetVolumeByID(rolledBack) = %v, want storage.ErrNotFound", err)
	}
	rbMemberships, err := smRepo.ListMembershipsByVolume(ctx, rolledBackVolID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume(rolledBack): %v", err)
	}
	if len(rbMemberships) != 0 {
		t.Fatalf("expected 0 memberships after rollback, got %+v", rbMemberships)
	}
	var rbEventCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1`, rolledBackVolID).Scan(&rbEventCount); err != nil {
		t.Fatalf("count rolled-back events: %v", err)
	}
	if rbEventCount != 0 {
		t.Fatalf("expected 0 events after rollback, got %d", rbEventCount)
	}
}
