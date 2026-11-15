package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
)

type catalogueEnforcementCtx struct {
	ctx       context.Context
	cat       *seededCatalogue
	repo      *postgres.CatalogueRepository
	admin     *domain.Principal
	adminPAT  *domain.Principal
	reader    *domain.Principal
	timmy     *domain.Principal
	multiRole *domain.Principal
}

type catalogueReadEnforcementSpec struct {
	runEnforcement func(t *testing.T, ec *catalogueEnforcementCtx)
	callWithRepo   func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error
}

// catalogueReadEnforcementSpecs maps every exported read/count method on CatalogueRepository
// to both its PostgreSQL visibility-enforcement test and its predicate-error propagation call.
// TestCatalogueRepository_EveryReadMethodAppliesPredicate fails if any read method lacks an entry here.
var catalogueReadEnforcementSpecs = map[string]catalogueReadEnforcementSpec{
	"GetSeriesByID": {
		runEnforcement: enforceGetSeriesByID,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.GetSeriesByID(ctx, scope, "s-any")
			return err
		},
	},
	"ListSeries": {
		runEnforcement: enforceListSeries,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.ListSeries(ctx, scope, storage.SeriesReadFilter{})
			return err
		},
	},
	"CountSeries": {
		runEnforcement: enforceCountSeries,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.CountSeries(ctx, scope, storage.SeriesReadFilter{})
			return err
		},
	},
	"GetVolumeByID": {
		runEnforcement: enforceGetVolumeByID,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.GetVolumeByID(ctx, scope, "v-any")
			return err
		},
	},
	"ListVolumes": {
		runEnforcement: enforceListVolumes,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.ListVolumes(ctx, scope, storage.VolumeReadFilter{})
			return err
		},
	},
	"CountVolumes": {
		runEnforcement: enforceCountVolumes,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.CountVolumes(ctx, scope, storage.VolumeReadFilter{})
			return err
		},
	},
	"GetAssetByID": {
		runEnforcement: enforceGetAssetByID,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.GetAssetByID(ctx, scope, "a-any")
			return err
		},
	},
	"ListAssetsByVolume": {
		runEnforcement: enforceListAssetsByVolume,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.ListAssetsByVolume(ctx, scope, "v-any")
			return err
		},
	},
	"CountAssetsByVolume": {
		runEnforcement: enforceCountAssetsByVolume,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.CountAssetsByVolume(ctx, scope, "v-any")
			return err
		},
	},
	"GetAuthorizedAssetFile": {
		runEnforcement: enforceGetAuthorizedAssetFile,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.GetAuthorizedAssetFile(ctx, scope, "a-any")
			return err
		},
	},
	"SelectPreferredAssetFileForVolume": {
		runEnforcement: enforceSelectPreferredAssetFileForVolume,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.SelectPreferredAssetFileForVolume(ctx, scope, "v-any")
			return err
		},
	},
	"GetReadingProgress": {
		runEnforcement: enforceGetReadingProgress,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.GetReadingProgress(ctx, scope, "v-any")
			return err
		},
	},
	"ListReadingProgress": {
		runEnforcement: enforceListReadingProgress,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.ListReadingProgress(ctx, scope, storage.ReadingProgressReadFilter{})
			return err
		},
	},
	"CountReadingProgress": {
		runEnforcement: enforceCountReadingProgress,
		callWithRepo: func(ctx context.Context, repo *postgres.CatalogueRepository, scope storage.CatalogueReadScope) error {
			_, err := repo.CountReadingProgress(ctx, scope, storage.ReadingProgressReadFilter{})
			return err
		},
	},
}

func TestCatalogueRepository_EveryReadMethodHasEnforcementTest(t *testing.T) {
	ctx := context.Background()
	cat := setupSeededCatalogue(t)
	repo := postgres.NewCatalogueRepository(cat.tdb.Pool)

	ec := &catalogueEnforcementCtx{
		ctx:       ctx,
		cat:       cat,
		repo:      repo,
		admin:     domain.NewPrincipal(*cat.adminUser, []domain.Role{cat.adminRole}, domain.CredentialKindPassword, "c-admin", nil),
		adminPAT:  domain.NewPrincipal(*cat.adminUser, []domain.Role{cat.adminRole}, domain.CredentialKindPAT, "c-admin-pat", []domain.PATScope{domain.ScopeRead}),
		reader:    domain.NewPrincipal(*cat.readerUser, []domain.Role{cat.readerRole}, domain.CredentialKindPassword, "c-reader", nil),
		timmy:     domain.NewPrincipal(*cat.restrictedUser, []domain.Role{cat.restrictedRole}, domain.CredentialKindPassword, "c-timmy", nil),
		multiRole: domain.NewPrincipal(*cat.multiRoleUser, []domain.Role{cat.readerRole, cat.restrictedRole}, domain.CredentialKindPassword, "c-multi", nil),
	}

	// Seed additional reading progress for adminUser on vInKids and vInTrashedSeries
	// to prove per-user scoping on ReadingProgress methods.
	for _, item := range []struct {
		volID     string
		completed bool
	}{
		{cat.vInKids, true},
		{cat.vInTrashedSeries, false},
	} {
		if _, err := cat.tdb.Exec(ctx, `
			INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, source, completed)
			VALUES ($1, $2, '{"type":"page","page":5}', 0.8, 'KOMGA', $3)`,
			cat.adminUser.ID, item.volID, item.completed,
		); err != nil {
			t.Fatalf("seed admin reading_progress for %s: %v", item.volID, err)
		}
	}

	// Persist policies:
	// - reader: RESTRICT nsfw (blacklist mode)
	// - timmy (Restricted reader): ALLOW kids, RESTRICT nsfw
	// - multiRole: zero policies (must fail closed)
	mustSetUserPolicies(ctx, t, cat, cat.readerUser.ID, []domain.UserLabelPolicy{
		{UserID: cat.readerUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
	})
	mustSetUserPolicies(ctx, t, cat, cat.restrictedUser.ID, []domain.UserLabelPolicy{
		{UserID: cat.restrictedUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
		{UserID: cat.restrictedUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
	})
	mustSetUserPolicies(ctx, t, cat, cat.multiRoleUser.ID, nil)

	names := make([]string, 0, len(catalogueReadEnforcementSpecs))
	for name := range catalogueReadEnforcementSpecs {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		spec := catalogueReadEnforcementSpecs[name]
		t.Run(name, func(t *testing.T) {
			spec.runEnforcement(t, ec)
		})
	}
}

func TestCatalogueRepository_PredicateErrorsFailClosedAcrossAllMethods(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("injected predicate query failure")

	failingDB := &predicateFailingDBTX{err: boom}
	failingRepo := postgres.NewCatalogueRepository(failingDB)
	var nilRepo *postgres.CatalogueRepository
	uninitRepo := postgres.NewCatalogueRepository(nil)

	readerRole, ok := domain.BuiltInRoleByName(domain.RoleReaderName)
	if !ok {
		t.Fatal("Reader role missing")
	}
	principal := domain.NewPrincipal(domain.User{ID: "u-err", Username: "erruser"}, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-1", nil)
	scope := storage.CatalogueReadScope{Principal: principal}

	for name, spec := range catalogueReadEnforcementSpecs {
		t.Run(name, func(t *testing.T) {
			failingDB.queriesCalled = 0
			err := spec.callWithRepo(ctx, failingRepo, scope)
			if !errors.Is(err, boom) {
				t.Fatalf("%s: expected injected predicate error %v, got %v", name, boom, err)
			}
			if failingDB.queriesCalled != 1 {
				t.Fatalf("%s: expected only predicate load query to run before failing, got %d queries", name, failingDB.queriesCalled)
			}

			if err := spec.callWithRepo(ctx, nilRepo, storage.CatalogueReadScope{}); err == nil {
				t.Fatalf("%s: nil repository must return error even with nil principal", name)
			}
			if err := spec.callWithRepo(ctx, uninitRepo, storage.CatalogueReadScope{}); err == nil {
				t.Fatalf("%s: uninitialized repository must return error even with nil principal", name)
			}
		})
	}
}

type predicateFailingDBTX struct {
	err           error
	queriesCalled int
}

func (f *predicateFailingDBTX) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	f.queriesCalled++
	return pgconn.CommandTag{}, f.err
}

func (f *predicateFailingDBTX) Query(context.Context, string, ...any) (pgx.Rows, error) {
	f.queriesCalled++
	return nil, f.err
}

func (f *predicateFailingDBTX) QueryRow(context.Context, string, ...any) pgx.Row {
	f.queriesCalled++
	return errRow{err: f.err}
}

type errRow struct {
	err error
}

func (r errRow) Scan(...any) error {
	return r.err
}

func mustSetUserPolicies(ctx context.Context, t *testing.T, cat *seededCatalogue, userID string, policies []domain.UserLabelPolicy) {
	t.Helper()
	if _, err := cat.tdb.Exec(ctx, `DELETE FROM user_label_policies WHERE user_id = $1`, userID); err != nil {
		t.Fatalf("clearing policies for %s: %v", userID, err)
	}
	for _, p := range policies {
		if err := cat.labelRepo.SetUserLabelPolicy(ctx, &p); err != nil {
			t.Fatalf("SetUserLabelPolicy(%s, %s): %v", userID, p.LabelID, err)
		}
	}
}

func assertNotFound(t *testing.T, label string, err error) {
	t.Helper()
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("%s: expected storage.ErrNotFound (404), got %v", label, err)
	}
}

func enforceGetSeriesByID(t *testing.T, ec *catalogueEnforcementCtx) {
	// Permitted series returns full entity
	got, err := ec.repo.GetSeriesByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.sKids)
	if err != nil || got.ID != ec.cat.sKids || got.Title != "Kids Series" {
		t.Fatalf("timmy GetSeriesByID(sKids) = (%+v, %v)", got, err)
	}

	// Restricted/unallowed/user-labeled/trashed series return ErrNotFound
	for _, tc := range []struct {
		name  string
		scope storage.CatalogueReadScope
		id    string
	}{
		{"nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, ec.cat.sKids},
		{"empty user principal", storage.CatalogueReadScope{Principal: &domain.Principal{}}, ec.cat.sKids},
		{"multiRole zero policies", storage.CatalogueReadScope{Principal: ec.multiRole, IncludeTrashed: true}, ec.cat.sKids},
		{"reader RESTRICT nsfw on sNSFW", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.sNSFW},
		{"reader RESTRICT nsfw on sKidsNSFW", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.sKidsNSFW},
		{"timmy ALLOW kids on sClean", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.sClean},
		{"timmy ALLOW kids on sUserKidsOnly (USER label ignored)", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.sUserKidsOnly},
		{"timmy group RESTRICT kids cancels ALLOW", storage.CatalogueReadScope{
			Principal:     ec.timmy,
			GroupPolicies: []domain.GroupLabelPolicy{{GroupClaim: "g", LabelID: ec.cat.lblKids, Effect: domain.PolicyEffectRestrict}},
		}, ec.cat.sKids},
		{"admin normal hides sTrashed", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: false}, ec.cat.sTrashed},
		{"admin read PAT IncludeTrashed=true hides sTrashed", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, ec.cat.sTrashed},
	} {
		_, err := ec.repo.GetSeriesByID(ec.ctx, tc.scope, tc.id)
		assertNotFound(t, tc.name, err)
	}

	// Admin with PermissionTrashPurge and IncludeTrashed=true sees sTrashed
	trashed, err := ec.repo.GetSeriesByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.sTrashed)
	if err != nil || trashed.ID != ec.cat.sTrashed {
		t.Fatalf("admin trash GetSeriesByID(sTrashed) = (%+v, %v)", trashed, err)
	}
}

func enforceListSeries(t *testing.T, ec *catalogueEnforcementCtx) {
	assertSeriesIDs := func(name string, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter, want []string) {
		t.Helper()
		list, err := ec.repo.ListSeries(ec.ctx, scope, filter)
		if err != nil {
			t.Fatalf("%s: ListSeries err: %v", name, err)
		}
		got := make([]string, len(list))
		for i, s := range list {
			got[i] = s.ID
		}
		slices.Sort(got)
		wantSorted := slices.Clone(want)
		slices.Sort(wantSorted)
		if !slices.Equal(got, wantSorted) {
			t.Fatalf("%s: ListSeries IDs = %v, want %v", name, got, wantSorted)
		}
	}

	assertSeriesIDs("nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, storage.SeriesReadFilter{}, nil)
	assertSeriesIDs("multiRole fail closed", storage.CatalogueReadScope{Principal: ec.multiRole, IncludeTrashed: true}, storage.SeriesReadFilter{}, nil)
	assertSeriesIDs("timmy ALLOW kids + RESTRICT nsfw", storage.CatalogueReadScope{Principal: ec.timmy}, storage.SeriesReadFilter{}, []string{ec.cat.sKids})
	assertSeriesIDs("timmy + group ALLOW all-ages", storage.CatalogueReadScope{
		Principal:     ec.timmy,
		GroupPolicies: []domain.GroupLabelPolicy{{GroupClaim: "family", LabelID: ec.cat.lblAllAges, Effect: domain.PolicyEffectAllow}},
	}, storage.SeriesReadFilter{}, []string{ec.cat.sKids, ec.cat.sAllAges})
	assertSeriesIDs("reader RESTRICT nsfw", storage.CatalogueReadScope{Principal: ec.reader}, storage.SeriesReadFilter{}, []string{
		ec.cat.sClean, ec.cat.sKids, ec.cat.sAllAges, ec.cat.sUserKidsOnly,
	})
	assertSeriesIDs("reader Search=NSFW returns empty", storage.CatalogueReadScope{Principal: ec.reader}, storage.SeriesReadFilter{Search: "NSFW"}, nil)
	assertSeriesIDs("reader Search=Series excludes restricted/trashed", storage.CatalogueReadScope{Principal: ec.reader}, storage.SeriesReadFilter{Search: "Series"}, []string{
		ec.cat.sClean, ec.cat.sKids, ec.cat.sAllAges, ec.cat.sUserKidsOnly,
	})
	assertSeriesIDs("admin IncludeTrashed=true includes sTrashed", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, storage.SeriesReadFilter{Search: "Trashed"}, []string{ec.cat.sTrashed})
	assertSeriesIDs("adminPAT IncludeTrashed=true excludes sTrashed", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, storage.SeriesReadFilter{Search: "Trashed"}, nil)
	// Literal % search wildcard escaping check
	assertSeriesIDs("Search literal percent matches nothing", storage.CatalogueReadScope{Principal: ec.admin}, storage.SeriesReadFilter{Search: "%"}, nil)

	// Pagination check (Limit + Offset)
	paged, err := ec.repo.ListSeries(ec.ctx, storage.CatalogueReadScope{Principal: ec.reader}, storage.SeriesReadFilter{Limit: 2, Offset: 1})
	if err != nil || len(paged) != 2 {
		t.Fatalf("reader ListSeries(Limit=2, Offset=1) = (%d, %v), want 2", len(paged), err)
	}

	// Negative pagination fails closed
	if _, err := ec.repo.ListSeries(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin}, storage.SeriesReadFilter{Limit: -1}); err == nil {
		t.Fatal("expected error for negative Limit")
	}
	if _, err := ec.repo.ListSeries(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin}, storage.SeriesReadFilter{Offset: -1}); err == nil {
		t.Fatal("expected error for negative Offset")
	}
}

func enforceCountSeries(t *testing.T, ec *catalogueEnforcementCtx) {
	assertCount := func(name string, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter, want int64) {
		t.Helper()
		cnt, err := ec.repo.CountSeries(ec.ctx, scope, filter)
		if err != nil {
			t.Fatalf("%s: CountSeries err: %v", name, err)
		}
		if cnt != want {
			t.Fatalf("%s: CountSeries = %d, want %d", name, cnt, want)
		}
	}

	assertCount("nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, storage.SeriesReadFilter{}, 0)
	assertCount("multiRole zero policies", storage.CatalogueReadScope{Principal: ec.multiRole}, storage.SeriesReadFilter{}, 0)
	assertCount("timmy ALLOW kids + RESTRICT nsfw", storage.CatalogueReadScope{Principal: ec.timmy}, storage.SeriesReadFilter{}, 1)
	assertCount("timmy Search=NSFW", storage.CatalogueReadScope{Principal: ec.timmy}, storage.SeriesReadFilter{Search: "NSFW"}, 0)
	assertCount("reader RESTRICT nsfw", storage.CatalogueReadScope{Principal: ec.reader}, storage.SeriesReadFilter{}, 4)
	assertCount("admin normal", storage.CatalogueReadScope{Principal: ec.admin}, storage.SeriesReadFilter{}, int64(len(ec.cat.allSeriesIDs)-1))
	assertCount("adminPAT IncludeTrashed=true", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, storage.SeriesReadFilter{}, int64(len(ec.cat.allSeriesIDs)-1))
	assertCount("admin IncludeTrashed=true", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, storage.SeriesReadFilter{}, int64(len(ec.cat.allSeriesIDs)))
}

func enforceGetVolumeByID(t *testing.T, ec *catalogueEnforcementCtx) {
	got, err := ec.repo.GetVolumeByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vInKids)
	if err != nil || got.ID != ec.cat.vInKids {
		t.Fatalf("timmy GetVolumeByID(vInKids) = (%+v, %v)", got, err)
	}
	if got.IsStandalone() || len(got.Memberships) != 1 || got.Memberships[0].SeriesID != ec.cat.sKids {
		t.Fatalf("timmy vInKids Memberships = %+v, want [%s]", got.Memberships, ec.cat.sKids)
	}
	if !got.IsOwned() || len(got.Assets) != 1 || got.Assets[0].ID != ec.cat.aKidsClean {
		t.Fatalf("timmy vInKids Assets = %+v, want [%s]", got.Assets, ec.cat.aKidsClean)
	}

	// Cross-series membership filtering: vInKidsAndClean belongs to sKids + sClean, so timmy sees ONLY sKids.
	vCross, err := ec.repo.GetVolumeByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vInKidsAndClean)
	if err != nil {
		t.Fatalf("timmy GetVolumeByID(vInKidsAndClean) err: %v", err)
	}
	if len(vCross.Memberships) != 1 || vCross.Memberships[0].SeriesID != ec.cat.sKids {
		t.Fatalf("timmy GetVolumeByID(vInKidsAndClean) leaked hidden series membership: %+v", vCross.Memberships)
	}
	// Reader sees both authorized memberships on vInKidsAndClean.
	vCrossReader, err := ec.repo.GetVolumeByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKidsAndClean)
	if err != nil || len(vCrossReader.Memberships) != 2 {
		t.Fatalf("reader GetVolumeByID(vInKidsAndClean) Memberships = %+v, err=%v (want 2)", vCrossReader, err)
	}

	// Standalone and zero-asset checks populate empty non-nil associations truthfully.
	vStandalone, err := ec.repo.GetVolumeByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vStandaloneKids)
	if err != nil || !vStandalone.IsStandalone() || len(vStandalone.Memberships) != 0 {
		t.Fatalf("timmy standalone GetVolumeByID = (%+v, %v)", vStandalone, err)
	}
	vZero, err := ec.repo.GetVolumeByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vZeroAssetsInKids)
	if err != nil || vZero.IsOwned() || !vZero.IsMissing() || len(vZero.Assets) != 0 {
		t.Fatalf("timmy zero-assets GetVolumeByID = (%+v, %v)", vZero, err)
	}

	// PreferredAssetID redaction: set preferred_asset_id = aKidsNSFW on vInKids.
	if _, err := ec.cat.tdb.Exec(ec.ctx, `UPDATE volumes SET preferred_asset_id = $1 WHERE id = $2`, ec.cat.aKidsNSFW, ec.cat.vInKids); err != nil {
		t.Fatalf("set preferred_asset_id=aKidsNSFW: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ec.cat.tdb.Exec(context.Background(), `UPDATE volumes SET preferred_asset_id = NULL WHERE id = $1`, ec.cat.vInKids)
	})
	vReader, err := ec.repo.GetVolumeByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKids)
	if err != nil {
		t.Fatalf("reader GetVolumeByID(vInKids) err: %v", err)
	}
	if vReader.PreferredAssetID != nil {
		t.Fatalf("reader GetVolumeByID(vInKids) leaked restricted PreferredAssetID %q", *vReader.PreferredAssetID)
	}
	if len(vReader.Assets) != 1 || vReader.Assets[0].ID != ec.cat.aKidsClean {
		t.Fatalf("reader GetVolumeByID(vInKids) Assets = %+v, want [%s]", vReader.Assets, ec.cat.aKidsClean)
	}
	vAdmin, err := ec.repo.GetVolumeByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.vInKids)
	if err != nil || vAdmin.PreferredAssetID == nil || *vAdmin.PreferredAssetID != ec.cat.aKidsNSFW || len(vAdmin.Assets) != 2 {
		t.Fatalf("admin GetVolumeByID(vInKids) = (%+v, %v)", vAdmin, err)
	}
	_, _ = ec.cat.tdb.Exec(ec.ctx, `UPDATE volumes SET preferred_asset_id = NULL WHERE id = $1`, ec.cat.vInKids)

	for _, tc := range []struct {
		name  string
		scope storage.CatalogueReadScope
		id    string
	}{
		{"nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, ec.cat.vInKids},
		{"multiRole fail closed", storage.CatalogueReadScope{Principal: ec.multiRole, IncludeTrashed: true}, ec.cat.vInKids},
		{"reader RESTRICT nsfw on vInKidsVolRestricted", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKidsVolRestricted},
		{"reader RESTRICT nsfw on alternate path vAltPathRestrictedPrimary", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vAltPathRestrictedPrimary},
		{"reader RESTRICT nsfw on alternate path vAltPathRestrictedSec", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vAltPathRestrictedSec},
		{"reader RESTRICT nsfw on vStandaloneNSFW", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vStandaloneNSFW},
		{"timmy ALLOW kids on vAllowedVolInClean (top-down I-4)", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vAllowedVolInClean},
		{"timmy ALLOW kids on vStandaloneUnlabeled (USER label ignored)", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vStandaloneUnlabeled},
		{"admin normal hides vTrashedInKids", storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.vTrashedInKids},
		{"adminPAT IncludeTrashed=true hides vInTrashedSeries", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, ec.cat.vInTrashedSeries},
	} {
		_, err := ec.repo.GetVolumeByID(ec.ctx, tc.scope, tc.id)
		assertNotFound(t, tc.name, err)
	}

	// Admin trash view sees trashed volumes and trashed asset associations.
	trashed, err := ec.repo.GetVolumeByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.vTrashedInKids)
	if err != nil || trashed.ID != ec.cat.vTrashedInKids {
		t.Fatalf("admin trash GetVolumeByID(vTrashedInKids) = (%+v, %v)", trashed, err)
	}
}

func enforceListVolumes(t *testing.T, ec *catalogueEnforcementCtx) {
	assertVolIDs := func(name string, scope storage.CatalogueReadScope, filter storage.VolumeReadFilter, want []string) []domain.Volume {
		t.Helper()
		list, err := ec.repo.ListVolumes(ec.ctx, scope, filter)
		if err != nil {
			t.Fatalf("%s: ListVolumes err: %v", name, err)
		}
		got := make([]string, len(list))
		for i, v := range list {
			got[i] = v.ID
		}
		slices.Sort(got)
		wantSorted := slices.Clone(want)
		slices.Sort(wantSorted)
		if !slices.Equal(got, wantSorted) {
			t.Fatalf("%s: ListVolumes IDs = %v, want %v", name, got, wantSorted)
		}
		return list
	}

	assertVolIDs("nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, storage.VolumeReadFilter{}, nil)
	assertVolIDs("multiRole fail closed", storage.CatalogueReadScope{Principal: ec.multiRole}, storage.VolumeReadFilter{}, nil)
	timmyVols := assertVolIDs("timmy ALLOW kids + RESTRICT nsfw", storage.CatalogueReadScope{Principal: ec.timmy}, storage.VolumeReadFilter{}, []string{
		ec.cat.vInKids, ec.cat.vInKidsAndClean, ec.cat.vZeroAssetsInKids, ec.cat.vStandaloneKids,
	})
	for _, v := range timmyVols {
		if v.ID == ec.cat.vInKidsAndClean && (len(v.Memberships) != 1 || v.Memberships[0].SeriesID != ec.cat.sKids) {
			t.Fatalf("timmy ListVolumes leaked unauthorized membership on vInKidsAndClean: %+v", v.Memberships)
		}
		if v.ID == ec.cat.vInKids && (len(v.Assets) != 1 || v.Assets[0].ID != ec.cat.aKidsClean) {
			t.Fatalf("timmy ListVolumes leaked restricted/trashed asset on vInKids: %+v", v.Assets)
		}
	}

	// Verify ListVolumes redacts unauthorized PreferredAssetID on listed volumes.
	if _, err := ec.cat.tdb.Exec(ec.ctx, `UPDATE volumes SET preferred_asset_id = $1 WHERE id = $2`, ec.cat.aKidsTrashed, ec.cat.vInKids); err != nil {
		t.Fatalf("set preferred_asset_id=aKidsTrashed: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ec.cat.tdb.Exec(context.Background(), `UPDATE volumes SET preferred_asset_id = NULL WHERE id = $1`, ec.cat.vInKids)
	})
	adminNormalVols := assertVolIDs("admin normal SeriesID=sKids excludes trashed preferred_asset_id", storage.CatalogueReadScope{Principal: ec.admin}, storage.VolumeReadFilter{SeriesID: ec.cat.sKids}, []string{
		ec.cat.vInKids, ec.cat.vInKidsVolRestricted, ec.cat.vAltPathRestrictedPrimary, ec.cat.vAltPathRestrictedSec, ec.cat.vInKidsAndClean, ec.cat.vZeroAssetsInKids,
	})
	for _, v := range adminNormalVols {
		if v.ID == ec.cat.vInKids {
			if v.PreferredAssetID != nil {
				t.Fatalf("admin normal ListVolumes leaked trashed PreferredAssetID %q", *v.PreferredAssetID)
			}
			if len(v.Assets) != 2 {
				t.Fatalf("admin normal ListVolumes(vInKids) Assets = %+v, want 2 non-trashed assets", v.Assets)
			}
		}
	}
	_, _ = ec.cat.tdb.Exec(ec.ctx, `UPDATE volumes SET preferred_asset_id = NULL WHERE id = $1`, ec.cat.vInKids)

	// Cross-series visibility check: vInKidsAndClean is in sKids + sClean, but sClean is hidden from timmy!
	assertVolIDs("timmy SeriesID=sClean hides vInKidsAndClean", storage.CatalogueReadScope{Principal: ec.timmy}, storage.VolumeReadFilter{SeriesID: ec.cat.sClean}, nil)
	assertVolIDs("timmy SeriesID=sKids shows vInKidsAndClean", storage.CatalogueReadScope{Principal: ec.timmy}, storage.VolumeReadFilter{SeriesID: ec.cat.sKids}, []string{
		ec.cat.vInKids, ec.cat.vInKidsAndClean, ec.cat.vZeroAssetsInKids,
	})
	assertVolIDs("reader SeriesID=sKids excludes restricted & trashed", storage.CatalogueReadScope{Principal: ec.reader}, storage.VolumeReadFilter{SeriesID: ec.cat.sKids}, []string{
		ec.cat.vInKids, ec.cat.vInKidsAndClean, ec.cat.vZeroAssetsInKids,
	})
	assertVolIDs("reader Search=Trashed returns empty", storage.CatalogueReadScope{Principal: ec.reader}, storage.VolumeReadFilter{Search: "Trashed"}, nil)
	assertVolIDs("admin IncludeTrashed=true Search=Trashed returns trashed volumes", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, storage.VolumeReadFilter{Search: "Trashed"}, []string{
		ec.cat.vInTrashedSeries, ec.cat.vTrashedInKids, ec.cat.vStandaloneTrashed,
	})

	paged, err := ec.repo.ListVolumes(ec.ctx, storage.CatalogueReadScope{Principal: ec.timmy}, storage.VolumeReadFilter{Limit: 2, Offset: 1})
	if err != nil || len(paged) != 2 {
		t.Fatalf("timmy ListVolumes(Limit=2, Offset=1) = (%d, %v), want 2", len(paged), err)
	}
}

func enforceCountVolumes(t *testing.T, ec *catalogueEnforcementCtx) {
	assertCount := func(name string, scope storage.CatalogueReadScope, filter storage.VolumeReadFilter, want int64) {
		t.Helper()
		cnt, err := ec.repo.CountVolumes(ec.ctx, scope, filter)
		if err != nil {
			t.Fatalf("%s: CountVolumes err: %v", name, err)
		}
		if cnt != want {
			t.Fatalf("%s: CountVolumes = %d, want %d", name, cnt, want)
		}
	}

	assertCount("nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, storage.VolumeReadFilter{}, 0)
	assertCount("multiRole zero policies", storage.CatalogueReadScope{Principal: ec.multiRole}, storage.VolumeReadFilter{}, 0)
	assertCount("timmy total", storage.CatalogueReadScope{Principal: ec.timmy}, storage.VolumeReadFilter{}, 4)
	assertCount("timmy SeriesID=sClean (hidden series)", storage.CatalogueReadScope{Principal: ec.timmy}, storage.VolumeReadFilter{SeriesID: ec.cat.sClean}, 0)
	assertCount("timmy SeriesID=sNSFW (restricted series)", storage.CatalogueReadScope{Principal: ec.timmy}, storage.VolumeReadFilter{SeriesID: ec.cat.sNSFW}, 0)
	assertCount("reader SeriesID=sNSFW", storage.CatalogueReadScope{Principal: ec.reader}, storage.VolumeReadFilter{SeriesID: ec.cat.sNSFW}, 0)
	assertCount("adminPAT IncludeTrashed=true excludes 3 trashed volumes", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, storage.VolumeReadFilter{}, int64(len(ec.cat.allVolumeIDs)-3))
	assertCount("admin IncludeTrashed=true sees all volumes", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, storage.VolumeReadFilter{}, int64(len(ec.cat.allVolumeIDs)))
}

func enforceGetAssetByID(t *testing.T, ec *catalogueEnforcementCtx) {
	got, err := ec.repo.GetAssetByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.aKidsClean)
	if err != nil || got.ID != ec.cat.aKidsClean {
		t.Fatalf("timmy GetAssetByID(aKidsClean) = (%+v, %v)", got, err)
	}

	for _, tc := range []struct {
		name  string
		scope storage.CatalogueReadScope
		id    string
	}{
		{"nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, ec.cat.aKidsClean},
		{"multiRole fail closed", storage.CatalogueReadScope{Principal: ec.multiRole}, ec.cat.aKidsClean},
		{"reader RESTRICT nsfw on aKidsNSFW", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.aKidsNSFW},
		{"reader RESTRICT nsfw on aCleanInRestrictedVol", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.aCleanInRestrictedVol},
		{"reader RESTRICT nsfw on aInAltRestricted", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.aInAltRestricted},
		{"timmy ALLOW kids on aAllowedInHiddenVol", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.aAllowedInHiddenVol},
		{"timmy ALLOW kids on aAllowedInUnallowedStandal", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.aAllowedInUnallowedStandal},
		{"admin normal hides aKidsTrashed", storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.aKidsTrashed},
		{"adminPAT IncludeTrashed=true hides aInTrashedVol", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, ec.cat.aInTrashedVol},
	} {
		_, err := ec.repo.GetAssetByID(ec.ctx, tc.scope, tc.id)
		assertNotFound(t, tc.name, err)
	}

	trashed, err := ec.repo.GetAssetByID(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.aKidsTrashed)
	if err != nil || trashed.ID != ec.cat.aKidsTrashed {
		t.Fatalf("admin trash GetAssetByID(aKidsTrashed) = (%+v, %v)", trashed, err)
	}
}

func enforceListAssetsByVolume(t *testing.T, ec *catalogueEnforcementCtx) {
	assertAssetIDs := func(name string, scope storage.CatalogueReadScope, volID string, want []string) {
		t.Helper()
		list, err := ec.repo.ListAssetsByVolume(ec.ctx, scope, volID)
		if err != nil {
			t.Fatalf("%s: ListAssetsByVolume err: %v", name, err)
		}
		got := make([]string, len(list))
		for i, a := range list {
			got[i] = a.ID
		}
		slices.Sort(got)
		wantSorted := slices.Clone(want)
		slices.Sort(wantSorted)
		if !slices.Equal(got, wantSorted) {
			t.Fatalf("%s: ListAssetsByVolume(%s) = %v, want %v", name, volID, got, wantSorted)
		}
	}

	assertAssetIDs("nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, ec.cat.vInKids, nil)
	assertAssetIDs("multiRole zero policies", storage.CatalogueReadScope{Principal: ec.multiRole}, ec.cat.vInKids, nil)
	assertAssetIDs("reader RESTRICT nsfw on vInKids returns only clean variant", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKids, []string{ec.cat.aKidsClean})
	assertAssetIDs("timmy ALLOW kids + RESTRICT nsfw on vInKids returns only clean variant", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vInKids, []string{ec.cat.aKidsClean})
	assertAssetIDs("timmy on vAllowedVolInClean returns empty", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vAllowedVolInClean, nil)
	assertAssetIDs("reader on vAltPathRestrictedSec returns empty", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vAltPathRestrictedSec, nil)
	assertAssetIDs("admin normal on vInKids hides trashed variant", storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.vInKids, []string{ec.cat.aKidsClean, ec.cat.aKidsNSFW})
	assertAssetIDs("adminPAT IncludeTrashed=true on vInKids hides trashed variant", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, ec.cat.vInKids, []string{ec.cat.aKidsClean, ec.cat.aKidsNSFW})
	assertAssetIDs("admin IncludeTrashed=true on vInKids shows all 3 variants", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.vInKids, []string{ec.cat.aKidsClean, ec.cat.aKidsNSFW, ec.cat.aKidsTrashed})
}

func enforceCountAssetsByVolume(t *testing.T, ec *catalogueEnforcementCtx) {
	assertCount := func(name string, scope storage.CatalogueReadScope, volID string, want int64) {
		t.Helper()
		cnt, err := ec.repo.CountAssetsByVolume(ec.ctx, scope, volID)
		if err != nil {
			t.Fatalf("%s: CountAssetsByVolume err: %v", name, err)
		}
		if cnt != want {
			t.Fatalf("%s: CountAssetsByVolume(%s) = %d, want %d", name, volID, cnt, want)
		}
	}

	assertCount("nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, ec.cat.vInKids, 0)
	assertCount("multiRole zero policies", storage.CatalogueReadScope{Principal: ec.multiRole}, ec.cat.vInKids, 0)
	assertCount("reader RESTRICT nsfw on vInKids", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKids, 1)
	assertCount("reader RESTRICT nsfw on vInKidsVolRestricted", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKidsVolRestricted, 0)
	assertCount("timmy on vAllowedVolInClean", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vAllowedVolInClean, 0)
	assertCount("admin normal on vInKids", storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.vInKids, 2)
	assertCount("adminPAT IncludeTrashed=true on vInKids", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, ec.cat.vInKids, 2)
	assertCount("admin IncludeTrashed=true on vInKids", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.vInKids, 3)
}

func enforceGetAuthorizedAssetFile(t *testing.T, ec *catalogueEnforcementCtx) {
	file, err := ec.repo.GetAuthorizedAssetFile(ec.ctx, storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.aKidsClean)
	if err != nil {
		t.Fatalf("timmy GetAuthorizedAssetFile(aKidsClean) err: %v", err)
	}
	if file.AssetID != ec.cat.aKidsClean ||
		file.VolumeID != ec.cat.vInKids ||
		file.LibraryID != ec.cat.libraryID ||
		file.LibraryRootID != ec.cat.rootID ||
		!bytes.Equal(file.RootPathBytes, []byte("/manga")) ||
		!bytes.Equal(file.RelPathBytes, []byte("kids-clean.cbz")) ||
		file.Format != domain.MediaFormatCBZ {
		t.Fatalf("unexpected AuthorizedAssetFile: %+v", file)
	}

	for _, tc := range []struct {
		name  string
		scope storage.CatalogueReadScope
		id    string
	}{
		{"nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, ec.cat.aKidsClean},
		{"multiRole fail closed", storage.CatalogueReadScope{Principal: ec.multiRole}, ec.cat.aKidsClean},
		{"reader RESTRICT nsfw on aKidsNSFW", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.aKidsNSFW},
		{"reader RESTRICT nsfw on aCleanInRestrictedVol", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.aCleanInRestrictedVol},
		{"reader RESTRICT nsfw on aInAltRestricted", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.aInAltRestricted},
		{"timmy ALLOW kids on aAllowedInHiddenVol", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.aAllowedInHiddenVol},
		{"admin normal hides trashed asset file", storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.aKidsTrashed},
		{"adminPAT IncludeTrashed=true hides trashed asset file", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, ec.cat.aKidsTrashed},
	} {
		_, err := ec.repo.GetAuthorizedAssetFile(ec.ctx, tc.scope, tc.id)
		assertNotFound(t, tc.name, err)
	}

	trashedFile, err := ec.repo.GetAuthorizedAssetFile(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.aKidsTrashed)
	if err != nil || trashedFile.AssetID != ec.cat.aKidsTrashed || !bytes.Equal(trashedFile.RelPathBytes, []byte("kids-trashed.cbz")) {
		t.Fatalf("admin trash GetAuthorizedAssetFile(aKidsTrashed) = (%+v, %v)", trashedFile, err)
	}
}

func enforceSelectPreferredAssetFileForVolume(t *testing.T, ec *catalogueEnforcementCtx) {
	// Set vInKids.preferred_asset_id = aKidsNSFW and point reader's progress locator to aKidsNSFW.
	// Restricted readers must NOT receive aKidsNSFW and must fall back cleanly to aKidsClean.
	if _, err := ec.cat.tdb.Exec(ec.ctx, `UPDATE volumes SET preferred_asset_id = $1 WHERE id = $2`, ec.cat.aKidsNSFW, ec.cat.vInKids); err != nil {
		t.Fatalf("set preferred_asset_id: %v", err)
	}
	if _, err := ec.cat.tdb.Exec(ec.ctx, `
		UPDATE reading_progress
		SET locator = jsonb_build_object('type', 'page', 'page', 2, 'page_count', 20, 'asset_id', $1::text)
		WHERE user_id = $2 AND volume_id = $3`,
		ec.cat.aKidsNSFW, ec.cat.readerUser.ID, ec.cat.vInKids,
	); err != nil {
		t.Fatalf("set reader locator asset_id: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ec.cat.tdb.Exec(context.Background(), `UPDATE volumes SET preferred_asset_id = NULL WHERE id = $1`, ec.cat.vInKids)
	})

	// Admin (unrestricted) gets preferred_asset_id aKidsNSFW.
	adminChosen, err := ec.repo.SelectPreferredAssetFileForVolume(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.vInKids)
	if err != nil || adminChosen.AssetID != ec.cat.aKidsNSFW {
		t.Fatalf("admin SelectPreferredAssetFileForVolume(vInKids) = (%+v, %v), want %s", adminChosen, err, ec.cat.aKidsNSFW)
	}

	// Reader (RESTRICT nsfw) falls back to aKidsClean even with preferred_asset_id and locator pointing at aKidsNSFW.
	readerChosen, err := ec.repo.SelectPreferredAssetFileForVolume(ec.ctx, storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKids)
	if err != nil || readerChosen.AssetID != ec.cat.aKidsClean {
		t.Fatalf("reader SelectPreferredAssetFileForVolume(vInKids) = (%+v, %v), want %s", readerChosen, err, ec.cat.aKidsClean)
	}

	// Admin with IncludeTrashed=true must still exclude trashed preferred_asset_id and trashed locator targets.
	if _, err := ec.cat.tdb.Exec(ec.ctx, `UPDATE volumes SET preferred_asset_id = $1 WHERE id = $2`, ec.cat.aKidsTrashed, ec.cat.vInKids); err != nil {
		t.Fatalf("set preferred_asset_id=aKidsTrashed: %v", err)
	}
	if _, err := ec.cat.tdb.Exec(ec.ctx, `
		UPDATE reading_progress
		SET locator = jsonb_build_object('type', 'page', 'page', 5, 'page_count', 20, 'asset_id', $1::text)
		WHERE user_id = $2 AND volume_id = $3`,
		ec.cat.aKidsTrashed, ec.cat.adminUser.ID, ec.cat.vInKids,
	); err != nil {
		t.Fatalf("set admin locator asset_id: %v", err)
	}
	adminTrashPref, err := ec.repo.SelectPreferredAssetFileForVolume(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.vInKids)
	if err != nil || adminTrashPref.AssetID != ec.cat.aKidsClean {
		t.Fatalf("admin IncludeTrashed=true SelectPreferredAssetFileForVolume(vInKids) = (%+v, %v), want non-trashed fallback %s", adminTrashPref, err, ec.cat.aKidsClean)
	}
	_, _ = ec.cat.tdb.Exec(ec.ctx, `UPDATE volumes SET preferred_asset_id = NULL WHERE id = $1`, ec.cat.vInKids)

	// Exact principal ID binding for locator lookup: padded user ID must not resolve real reader's locator.
	const cleanAltAssetID = "a-kids-clean-locator-check"
	if _, err := ec.cat.tdb.Exec(ec.ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format, page_count, created_at)
		VALUES ($1, $2, $3, $4, 'kids-clean-2.cbz', 'kids-clean-2.cbz', 1024, 'CBZ', 20, NOW() + INTERVAL '1 hour')`,
		cleanAltAssetID, ec.cat.vInKids, ec.cat.rootID, []byte("kids-clean-2.cbz"),
	); err != nil {
		t.Fatalf("insert secondary clean asset: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ec.cat.tdb.Exec(context.Background(), `DELETE FROM media_assets WHERE id = $1`, cleanAltAssetID)
	})
	if _, err := ec.cat.tdb.Exec(ec.ctx, `
		UPDATE reading_progress
		SET locator = jsonb_build_object('type', 'page', 'page', 3, 'page_count', 20, 'asset_id', $1::text)
		WHERE user_id = $2 AND volume_id = $3`,
		cleanAltAssetID, ec.cat.readerUser.ID, ec.cat.vInKids,
	); err != nil {
		t.Fatalf("point reader locator to %s: %v", cleanAltAssetID, err)
	}
	readerByLocator, err := ec.repo.SelectPreferredAssetFileForVolume(ec.ctx, storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKids)
	if err != nil || readerByLocator.AssetID != cleanAltAssetID {
		t.Fatalf("reader locator selection = (%+v, %v), want %s", readerByLocator, err, cleanAltAssetID)
	}
	paddedReader := domain.NewPrincipal(domain.User{ID: " " + ec.cat.readerUser.ID + " ", Username: ec.cat.readerUser.Username}, ec.reader.Roles, domain.CredentialKindPassword, "c-pad", nil)
	paddedChosen, err := ec.repo.SelectPreferredAssetFileForVolume(ec.ctx, storage.CatalogueReadScope{Principal: paddedReader}, ec.cat.vInKids)
	if err != nil || paddedChosen.AssetID != ec.cat.aKidsClean {
		t.Fatalf("padded reader ID must not resolve reader locator; got (%+v, %v), want oldest %s", paddedChosen, err, ec.cat.aKidsClean)
	}
	if _, err := ec.cat.tdb.Exec(ec.ctx, `DELETE FROM media_assets WHERE id = $1`, cleanAltAssetID); err != nil {
		t.Fatalf("cleanup secondary clean asset: %v", err)
	}

	// Restricted/hidden/trashed/zero-asset volumes return ErrNotFound.
	for _, tc := range []struct {
		name  string
		scope storage.CatalogueReadScope
		volID string
	}{
		{"nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, ec.cat.vInKids},
		{"multiRole zero policies", storage.CatalogueReadScope{Principal: ec.multiRole}, ec.cat.vInKids},
		{"reader on restricted vInKidsVolRestricted", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKidsVolRestricted},
		{"reader on alt-restricted vAltPathRestrictedSec", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vAltPathRestrictedSec},
		{"timmy on vAllowedVolInClean", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vAllowedVolInClean},
		{"timmy on vZeroAssetsInKids", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vZeroAssetsInKids},
		{"admin normal on vTrashedInKids", storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.vTrashedInKids},
		{"admin IncludeTrashed=true excludes trashed volume vTrashedInKids", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.vTrashedInKids},
		{"admin IncludeTrashed=true excludes volume in trashed series", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.vInTrashedSeries},
		{"adminPAT IncludeTrashed=true on vTrashedInKids", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, ec.cat.vTrashedInKids},
	} {
		_, err := ec.repo.SelectPreferredAssetFileForVolume(ec.ctx, tc.scope, tc.volID)
		assertNotFound(t, tc.name, err)
	}

	// Missing asset is skipped by SelectPreferredAsset; when the only permitted asset is missing, ErrNotFound is returned.
	now := time.Now().UTC()
	if _, err := ec.cat.tdb.Exec(ec.ctx, `UPDATE media_assets SET missing_since = $1 WHERE id = $2`, now, ec.cat.aKidsClean); err != nil {
		t.Fatalf("mark aKidsClean missing: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ec.cat.tdb.Exec(context.Background(), `UPDATE media_assets SET missing_since = NULL WHERE id = $1`, ec.cat.aKidsClean)
	})
	_, err = ec.repo.SelectPreferredAssetFileForVolume(ec.ctx, storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vInKids)
	assertNotFound(t, "timmy when only permitted variant is missing", err)
}

func enforceGetReadingProgress(t *testing.T, ec *catalogueEnforcementCtx) {
	// Reader sees own progress on visible volume vInKids (page 2 or 1, NOT admin's page 5).
	rpReader, err := ec.repo.GetReadingProgress(ec.ctx, storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKids)
	if err != nil || rpReader.UserID != ec.cat.readerUser.ID || rpReader.VolumeID != ec.cat.vInKids || rpReader.Source != "REST" {
		t.Fatalf("reader GetReadingProgress(vInKids) = (%+v, %v)", rpReader, err)
	}

	// Admin sees own progress on vInKids (KOMGA, completed=true).
	rpAdmin, err := ec.repo.GetReadingProgress(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.vInKids)
	if err != nil || rpAdmin.UserID != ec.cat.adminUser.ID || rpAdmin.Source != "KOMGA" || !rpAdmin.Completed {
		t.Fatalf("admin GetReadingProgress(vInKids) = (%+v, %v)", rpAdmin, err)
	}

	paddedReader := domain.NewPrincipal(domain.User{ID: " " + ec.cat.readerUser.ID, Username: ec.cat.readerUser.Username}, ec.reader.Roles, domain.CredentialKindPassword, "c-pad-r", nil)
	paddedAdmin := domain.NewPrincipal(domain.User{ID: ec.cat.adminUser.ID + " ", Username: ec.cat.adminUser.Username}, ec.admin.Roles, domain.CredentialKindPassword, "c-pad-a", nil)

	// Restricted/other-user/padded-ID/trashed progress returns ErrNotFound.
	for _, tc := range []struct {
		name  string
		scope storage.CatalogueReadScope
		volID string
	}{
		{"nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, ec.cat.vInKids},
		{"whitespace-only user ID", storage.CatalogueReadScope{Principal: domain.NewPrincipal(domain.User{ID: "   "}, ec.admin.Roles, domain.CredentialKindPassword, "c-ws", nil), IncludeTrashed: true}, ec.cat.vInKids},
		{"padded reader ID must not match reader progress", storage.CatalogueReadScope{Principal: paddedReader}, ec.cat.vInKids},
		{"padded admin ID must not match admin progress", storage.CatalogueReadScope{Principal: paddedAdmin, IncludeTrashed: true}, ec.cat.vInKids},
		{"multiRole zero policies", storage.CatalogueReadScope{Principal: ec.multiRole}, ec.cat.vInKids},
		{"timmy cannot see reader or admin progress on vInKids", storage.CatalogueReadScope{Principal: ec.timmy}, ec.cat.vInKids},
		{"admin cannot see reader progress on vStandaloneKids", storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.vStandaloneKids},
		{"reader RESTRICT nsfw hides own progress on vInKidsVolRestricted", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vInKidsVolRestricted},
		{"reader RESTRICT nsfw hides own progress on vAltPathRestrictedSec", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vAltPathRestrictedSec},
		{"reader hides own progress on vTrashedInKids", storage.CatalogueReadScope{Principal: ec.reader}, ec.cat.vTrashedInKids},
		{"admin normal hides own progress on vInTrashedSeries", storage.CatalogueReadScope{Principal: ec.admin}, ec.cat.vInTrashedSeries},
		{"adminPAT IncludeTrashed=true hides own progress on vInTrashedSeries", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, ec.cat.vInTrashedSeries},
	} {
		_, err := ec.repo.GetReadingProgress(ec.ctx, tc.scope, tc.volID)
		assertNotFound(t, tc.name, err)
	}

	// Admin trash view sees own progress on vInTrashedSeries (not reader's vTrashedInKids!).
	rpAdminTrash, err := ec.repo.GetReadingProgress(ec.ctx, storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, ec.cat.vInTrashedSeries)
	if err != nil || rpAdminTrash.UserID != ec.cat.adminUser.ID {
		t.Fatalf("admin trash GetReadingProgress(vInTrashedSeries) = (%+v, %v)", rpAdminTrash, err)
	}
}

func enforceListReadingProgress(t *testing.T, ec *catalogueEnforcementCtx) {
	assertProgressVols := func(name string, scope storage.CatalogueReadScope, filter storage.ReadingProgressReadFilter, wantUserID string, wantVols []string) {
		t.Helper()
		list, err := ec.repo.ListReadingProgress(ec.ctx, scope, filter)
		if err != nil {
			t.Fatalf("%s: ListReadingProgress err: %v", name, err)
		}
		got := make([]string, len(list))
		for i, rp := range list {
			if wantUserID != "" && rp.UserID != wantUserID {
				t.Fatalf("%s: leaked foreign user_id %q (want %q)", name, rp.UserID, wantUserID)
			}
			got[i] = rp.VolumeID
		}
		slices.Sort(got)
		wantSorted := slices.Clone(wantVols)
		slices.Sort(wantSorted)
		if !slices.Equal(got, wantSorted) {
			t.Fatalf("%s: ListReadingProgress vols = %v, want %v", name, got, wantSorted)
		}
	}

	paddedAdmin := domain.NewPrincipal(domain.User{ID: " " + ec.cat.adminUser.ID + " ", Username: ec.cat.adminUser.Username}, ec.admin.Roles, domain.CredentialKindPassword, "c-pad-a", nil)
	assertProgressVols("nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, storage.ReadingProgressReadFilter{}, "", nil)
	assertProgressVols("padded admin ID sees nothing", storage.CatalogueReadScope{Principal: paddedAdmin, IncludeTrashed: true}, storage.ReadingProgressReadFilter{}, "", nil)
	assertProgressVols("timmy has no progress", storage.CatalogueReadScope{Principal: ec.timmy}, storage.ReadingProgressReadFilter{}, ec.cat.restrictedUser.ID, nil)
	assertProgressVols("reader RESTRICT nsfw", storage.CatalogueReadScope{Principal: ec.reader}, storage.ReadingProgressReadFilter{}, ec.cat.readerUser.ID, []string{
		ec.cat.vInKids, ec.cat.vAllowedVolInClean, ec.cat.vStandaloneKids,
	})
	assertProgressVols("admin normal sees only own non-trashed progress", storage.CatalogueReadScope{Principal: ec.admin}, storage.ReadingProgressReadFilter{}, ec.cat.adminUser.ID, []string{ec.cat.vInKids})
	assertProgressVols("adminPAT IncludeTrashed=true hides trashed", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, storage.ReadingProgressReadFilter{}, ec.cat.adminUser.ID, []string{ec.cat.vInKids})
	assertProgressVols("admin IncludeTrashed=true sees both own progress rows", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, storage.ReadingProgressReadFilter{}, ec.cat.adminUser.ID, []string{ec.cat.vInKids, ec.cat.vInTrashedSeries})

	completedTrue := true
	assertProgressVols("admin IncludeTrashed=true Completed=true", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, storage.ReadingProgressReadFilter{Completed: &completedTrue}, ec.cat.adminUser.ID, []string{ec.cat.vInKids})
}

func enforceCountReadingProgress(t *testing.T, ec *catalogueEnforcementCtx) {
	assertCount := func(name string, scope storage.CatalogueReadScope, filter storage.ReadingProgressReadFilter, want int64) {
		t.Helper()
		cnt, err := ec.repo.CountReadingProgress(ec.ctx, scope, filter)
		if err != nil {
			t.Fatalf("%s: CountReadingProgress err: %v", name, err)
		}
		if cnt != want {
			t.Fatalf("%s: CountReadingProgress = %d, want %d", name, cnt, want)
		}
	}

	paddedReader := domain.NewPrincipal(domain.User{ID: ec.cat.readerUser.ID + " ", Username: ec.cat.readerUser.Username}, ec.reader.Roles, domain.CredentialKindPassword, "c-pad-r", nil)
	assertCount("nil principal", storage.CatalogueReadScope{IncludeTrashed: true}, storage.ReadingProgressReadFilter{}, 0)
	assertCount("padded reader ID counts 0", storage.CatalogueReadScope{Principal: paddedReader}, storage.ReadingProgressReadFilter{}, 0)
	assertCount("multiRole zero policies", storage.CatalogueReadScope{Principal: ec.multiRole}, storage.ReadingProgressReadFilter{}, 0)
	assertCount("timmy has 0 progress", storage.CatalogueReadScope{Principal: ec.timmy}, storage.ReadingProgressReadFilter{}, 0)
	assertCount("reader RESTRICT nsfw", storage.CatalogueReadScope{Principal: ec.reader}, storage.ReadingProgressReadFilter{}, 3)
	assertCount("admin normal", storage.CatalogueReadScope{Principal: ec.admin}, storage.ReadingProgressReadFilter{}, 1)
	assertCount("adminPAT IncludeTrashed=true", storage.CatalogueReadScope{Principal: ec.adminPAT, IncludeTrashed: true}, storage.ReadingProgressReadFilter{}, 1)
	assertCount("admin IncludeTrashed=true", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, storage.ReadingProgressReadFilter{}, 2)

	completedFalse := false
	assertCount("admin IncludeTrashed=true Completed=false", storage.CatalogueReadScope{Principal: ec.admin, IncludeTrashed: true}, storage.ReadingProgressReadFilter{Completed: &completedFalse}, 1)
}
