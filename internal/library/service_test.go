package library_test

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

type libraryFixture struct {
	tdb        *testdb.TestDB
	svc        app.LibraryService
	admin      *domain.Principal
	reader     *domain.Principal
	timmy      *domain.Principal
	noRead     *domain.Principal
	lib1ID     string
	lib2ID     string
	sCleanID   string
	sKidsID    string
	sNSFWID    string
	sTrashedID string
	vCleanID   string
	vNSFWID    string
	vStandID   string
	vMissingID string
	rawDirHex  string
}

func setupLibraryFixture(t *testing.T) *libraryFixture {
	t.Helper()
	tdb := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, tdb, migrations.CoreFS); err != nil {
		t.Fatalf("migrations.RunFS: %v", err)
	}

	transactor := postgres.NewTransactor(tdb.Pool)
	rawSvc, err := library.NewService(transactor)
	if err != nil {
		t.Fatalf("library.NewService: %v", err)
	}

	f := &libraryFixture{
		tdb:        tdb,
		svc:        app.NewAuthorizingLibraryService(rawSvc),
		lib1ID:     "lib-main",
		lib2ID:     "lib-alt",
		sCleanID:   "ser-clean",
		sKidsID:    "ser-kids",
		sNSFWID:    "ser-nsfw",
		sTrashedID: "ser-trashed",
		vCleanID:   "vol-clean",
		vNSFWID:    "vol-nsfw",
		vStandID:   "vol-standalone",
		vMissingID: "vol-missing",
	}

	seedUsersAndPolicies(ctx, t, transactor, f)
	seedLibrariesAndCatalogue(ctx, t, transactor, f)
	return f
}

func seedUsersAndPolicies(ctx context.Context, t *testing.T, txer storage.Transactor, f *libraryFixture) {
	t.Helper()
	var adminRole, readerRole, restrictedRole domain.Role
	for _, r := range domain.BuiltInRoles() {
		switch r.ID {
		case domain.RoleAdminID:
			adminRole = r
		case domain.RoleReaderID:
			readerRole = r
		case domain.RoleRestrictedReaderID:
			restrictedRole = r
		}
	}
	jobsOnlyRole := domain.Role{
		ID:          "role-jobs-only",
		Name:        "Jobs Only",
		Permissions: []domain.Permission{domain.PermissionJobsReadAll},
	}

	if err := txer.WithTx(ctx, func(tx storage.Tx) error {
		users := tx.Users()
		for _, spec := range []struct {
			id   string
			name string
			role string
		}{
			{"u-admin", "admin", domain.RoleAdminID},
			{"u-reader", "reader", domain.RoleReaderID},
			{"u-timmy", "timmy", domain.RoleRestrictedReaderID},
		} {
			u := domain.User{ID: spec.id, Username: spec.name}
			if err := users.CreateUser(ctx, &u); err != nil {
				return err
			}
			if err := users.AssignUserRole(ctx, spec.id, spec.role); err != nil {
				return err
			}
		}

		labels := tx.Labels()
		if err := labels.EnsureBuiltInServerLabels(ctx); err != nil {
			return err
		}
		for _, l := range []domain.Label{
			{ID: "lbl-kids", Name: "Kids", Scope: domain.LabelScopeServer},
			{ID: "lbl-nsfw", Name: "NSFW", Scope: domain.LabelScopeServer},
			{ID: "lbl-personal", Name: "Personal", Scope: domain.LabelScopeUser, OwnerID: ptrStr("u-reader")},
		} {
			lbl := l
			if err := labels.CreateLabel(ctx, &lbl); err != nil {
				return err
			}
		}
		for _, pol := range []domain.UserLabelPolicy{
			{UserID: "u-reader", LabelID: "lbl-nsfw", Effect: domain.PolicyEffectRestrict},
			{UserID: "u-timmy", LabelID: "lbl-kids", Effect: domain.PolicyEffectAllow},
			{UserID: "u-timmy", LabelID: "lbl-nsfw", Effect: domain.PolicyEffectRestrict},
		} {
			p := pol
			if err := labels.SetUserLabelPolicy(ctx, &p); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seedUsersAndPolicies: %v", err)
	}

	f.admin = domain.NewPrincipal(domain.User{ID: "u-admin", Username: "admin"}, []domain.Role{adminRole}, domain.CredentialKindPassword, "c-admin", nil)
	f.reader = domain.NewPrincipal(domain.User{ID: "u-reader", Username: "reader"}, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-reader", nil)
	f.timmy = domain.NewPrincipal(domain.User{ID: "u-timmy", Username: "timmy"}, []domain.Role{restrictedRole}, domain.CredentialKindPassword, "c-timmy", nil)
	f.noRead = domain.NewPrincipal(domain.User{ID: "u-noread", Username: "noread"}, []domain.Role{jobsOnlyRole}, domain.CredentialKindPassword, "c-noread", nil)
}

func seedLibrariesAndCatalogue(ctx context.Context, t *testing.T, txer storage.Transactor, f *libraryFixture) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	intake := domain.NewPathIdentity([]byte("/srv/secret/intake"))
	rootErr := "stat root: lstat /mnt/secret/offline: no such file or directory"
	rawFolderBytes := []byte("Raw-\xff-Dir")
	f.rawDirHex = hex.EncodeToString(rawFolderBytes)
	rawFileBytes := append(append([]byte{}, rawFolderBytes...), []byte("/ep1.cbz")...)

	if err := txer.WithTx(ctx, func(tx storage.Tx) error {
		libs := tx.Libraries()
		lib1 := domain.Library{
			ID:                 f.lib1ID,
			Name:               "Main Manga",
			Mode:               domain.LibraryModeManaged,
			IntakePath:         &intake,
			ResolverChain:      domain.DefaultResolverChain(),
			MetadataPrecedence: domain.MetadataPrecedenceEmbeddedOverOnline,
			DefaultCoverPolicy: domain.CoverPolicyFirst,
			CreatedAt:          now,
		}
		lib2 := domain.Library{
			ID:                 f.lib2ID,
			Name:               "Alt Comics",
			Mode:               domain.LibraryModeVirtual,
			ResolverChain:      domain.DefaultResolverChain(),
			MetadataPrecedence: domain.MetadataPrecedenceEmbeddedOverOnline,
			DefaultCoverPolicy: domain.CoverPolicyFirst,
			CreatedAt:          now.Add(time.Second),
		}
		if err := libs.CreateLibrary(ctx, &lib1); err != nil {
			return err
		}
		if err := libs.CreateLibrary(ctx, &lib2); err != nil {
			return err
		}
		for _, r := range []domain.LibraryRoot{
			{ID: "root-1a", LibraryID: f.lib1ID, Path: domain.NewPathIdentity([]byte("/mnt/secret/vol1")), State: domain.LibraryRootStateOnline},
			{ID: "root-1b", LibraryID: f.lib1ID, Path: domain.NewPathIdentity([]byte("/mnt/secret/offline")), State: domain.LibraryRootStateOffline, LastError: &rootErr},
			{ID: "root-2a", LibraryID: f.lib2ID, Path: domain.NewPathIdentity([]byte("/mnt/secret/alt")), State: domain.LibraryRootStateOnline},
		} {
			root := r
			if err := libs.CreateLibraryRoot(ctx, &root); err != nil {
				return err
			}
		}

		cw := tx.CatalogueWrites()
		for _, s := range []domain.Series{
			{ID: f.sCleanID, LibraryID: f.lib1ID, Title: "Clean Series", TitleSort: "clean series", Status: domain.SeriesStatusOngoing, Genres: []string{"Action"}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: f.sKidsID, LibraryID: f.lib1ID, Title: "Kids Series", TitleSort: "kids series", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: f.sNSFWID, LibraryID: f.lib1ID, Title: "NSFW Series", TitleSort: "nsfw series", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: f.sTrashedID, LibraryID: f.lib1ID, Title: "Trashed Series", TitleSort: "trashed series", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
		} {
			ser := s
			if err := cw.CreateSeries(ctx, &ser); err != nil {
				return err
			}
		}

		vCleanTitle := "Clean Vol 1"
		vNSFWTitle := "NSFW Vol 1"
		vStandTitle := "Standalone Anthology"
		vMissingTitle := "Missing Kids Vol"
		for _, v := range []domain.Volume{
			{ID: f.vCleanID, LibraryID: f.lib1ID, Kind: domain.VolumeKindVolume, Title: &vCleanTitle, CreatedAt: now, UpdatedAt: now},
			{ID: f.vNSFWID, LibraryID: f.lib1ID, Kind: domain.VolumeKindVolume, Title: &vNSFWTitle, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now},
			{ID: f.vStandID, LibraryID: f.lib1ID, Kind: domain.VolumeKindVolume, Title: &vStandTitle, CreatedAt: now.Add(2 * time.Millisecond), UpdatedAt: now},
			{ID: f.vMissingID, LibraryID: f.lib1ID, Kind: domain.VolumeKindVolume, Title: &vMissingTitle, CreatedAt: now.Add(3 * time.Millisecond), UpdatedAt: now},
		} {
			vol := v
			if err := cw.CreateVolume(ctx, &vol); err != nil {
				return err
			}
		}

		sm := tx.SeriesMemberships()
		pos := 1.0
		for _, m := range []domain.SeriesMembership{
			{VolumeID: f.vCleanID, SeriesID: f.sCleanID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceResolver},
			{VolumeID: f.vNSFWID, SeriesID: f.sNSFWID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceResolver},
			{VolumeID: f.vMissingID, SeriesID: f.sKidsID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceResolver},
		} {
			mem := m
			if err := sm.SetVolumeMemberships(ctx, mem.VolumeID, []domain.SeriesMembership{mem}); err != nil {
				return err
			}
		}

		for _, a := range []struct {
			id    string
			volID string
			rel   []byte
			size  int64
			pages int
		}{
			{"asset-clean", f.vCleanID, []byte("Clean Series/vol1.cbz"), 100, 10},
			{"asset-raw", f.vCleanID, rawFileBytes, 120, 12},
			{"asset-stand", f.vStandID, []byte("standalone.cbz"), 90, 8},
			{"asset-nsfw", f.vNSFWID, []byte("RestrictedFolder/secret.cbz"), 80, 6},
		} {
			ident := domain.NewPathIdentity(a.rel)
			asset := domain.MediaAsset{
				ID:             a.id,
				VolumeID:       a.volID,
				LibraryRootID:  "root-1a",
				RelPathBytes:   ident.Bytes,
				RelPathDisplay: ident.Display,
				RelPathMatch:   ident.Match,
				FileSize:       a.size,
				Format:         domain.MediaFormatCBZ,
				PageCount:      a.pages,
			}
			if err := cw.CreateAsset(ctx, &asset); err != nil {
				return err
			}
		}

		labels := tx.Labels()
		for _, a := range []domain.ItemLabel{
			{LabelID: "lbl-kids", ItemType: domain.ItemTypeSeries, ItemID: f.sKidsID},
			{LabelID: "lbl-nsfw", ItemType: domain.ItemTypeSeries, ItemID: f.sNSFWID},
			{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeSeries, ItemID: f.sTrashedID},
			{LabelID: "lbl-kids", ItemType: domain.ItemTypeVolume, ItemID: f.vStandID},
			{LabelID: "lbl-personal", ItemType: domain.ItemTypeSeries, ItemID: f.sCleanID},
		} {
			il := a
			if err := labels.AssignItemLabel(ctx, &il); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seedLibrariesAndCatalogue: %v", err)
	}
}

func TestLibraryService_LibrariesAndAdminRedaction(t *testing.T) {
	f := setupLibraryFixture(t)
	ctx := context.Background()

	// Missing permission -> Forbidden
	if _, err := f.svc.ListLibraries(ctx, f.noRead, app.LibraryFilter{}); !errors.Is(err, app.ErrForbidden) {
		t.Fatalf("noRead ListLibraries = %v, want ErrForbidden", err)
	}

	// Admin sees path_display, last_error, intake_path_display and OFFLINE aggregate state on lib-main.
	adminPage, err := f.svc.ListLibraries(ctx, f.admin, app.LibraryFilter{Limit: 1, Offset: 0})
	if err != nil {
		t.Fatalf("admin ListLibraries: %v", err)
	}
	if adminPage.TotalCount != 2 || adminPage.Limit != 1 || len(adminPage.Items) != 1 {
		t.Fatalf("unexpected admin ListLibraries page: %+v", adminPage)
	}
	mainLib := adminPage.Items[0]
	if mainLib.State != "OFFLINE" || mainLib.IntakePathDisplay == nil || *mainLib.IntakePathDisplay != "/srv/secret/intake" {
		t.Fatalf("unexpected admin mainLib: %+v", mainLib)
	}
	if len(mainLib.Roots) != 2 {
		t.Fatalf("expected 2 admin mainLib roots, got %+v", mainLib.Roots)
	}
	rootsByID := map[string]app.LibraryRoot{}
	for _, r := range mainLib.Roots {
		rootsByID[r.ID] = r
	}
	if r1a := rootsByID["root-1a"]; r1a.PathDisplay == nil || *r1a.PathDisplay != "/mnt/secret/vol1" || r1a.LastError != nil || r1a.State != "ONLINE" {
		t.Fatalf("unexpected admin root-1a: %+v", r1a)
	}
	if r1b := rootsByID["root-1b"]; r1b.PathDisplay == nil || *r1b.PathDisplay != "/mnt/secret/offline" || r1b.LastError == nil || r1b.State != "OFFLINE" {
		t.Fatalf("unexpected admin root-1b: %+v", r1b)
	}

	// Reader sees roots and ONLINE/OFFLINE state, but all host path strings and last_error are redacted.
	readerLib, err := f.svc.GetLibrary(ctx, f.reader, f.lib1ID)
	if err != nil {
		t.Fatalf("reader GetLibrary: %v", err)
	}
	if readerLib.State != "OFFLINE" || readerLib.IntakePathDisplay != nil {
		t.Fatalf("expected reader intake redacted, got %+v", readerLib)
	}
	for _, r := range readerLib.Roots {
		if r.PathDisplay != nil || r.LastError != nil {
			t.Fatalf("expected reader root path_display & last_error nil, got %+v", r)
		}
	}

	altLib, err := f.svc.GetLibrary(ctx, f.reader, f.lib2ID)
	if err != nil {
		t.Fatalf("reader GetLibrary(lib2): %v", err)
	}
	if altLib.State != "ONLINE" || len(altLib.Roots) != 1 || altLib.Roots[0].State != "ONLINE" {
		t.Fatalf("expected lib2 ONLINE, got %+v", altLib)
	}

	if _, err := f.svc.GetLibrary(ctx, f.reader, "missing-lib"); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("GetLibrary(missing) = %v, want ErrNotFound", err)
	}
	if _, err := f.svc.ScanLibrary(ctx, f.admin, f.lib1ID); !errors.Is(err, app.ErrNotImplemented) {
		t.Fatalf("ScanLibrary = %v, want ErrNotImplemented", err)
	}
}

func TestLibraryService_SeriesVolumesAndFolderBrowse(t *testing.T) {
	f := setupLibraryFixture(t)
	ctx := context.Background()

	// Reader sees sClean and sKids (not sNSFW or sTrashed; USER-scoped lbl-personal on sClean is ignored!).
	serPage, err := f.svc.ListSeries(ctx, f.reader, app.SeriesFilter{LibraryID: f.lib1ID})
	if err != nil {
		t.Fatalf("reader ListSeries: %v", err)
	}
	if serPage.TotalCount != 2 || len(serPage.Items) != 2 {
		t.Fatalf("reader ListSeries = %+v, want 2 items", serPage)
	}
	if _, err := f.svc.GetSeries(ctx, f.reader, f.sNSFWID); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("reader GetSeries(NSFW) = %v, want ErrNotFound", err)
	}
	if _, err := f.svc.GetSeries(ctx, f.admin, f.sTrashedID); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("admin GetSeries(trashed) = %v, want ErrNotFound", err)
	}

	// Restricted reader (timmy: ALLOW kids) sees only sKids in series, and vMissing + vStand in volumes.
	timmySeries, err := f.svc.ListSeries(ctx, f.timmy, app.SeriesFilter{})
	if err != nil || timmySeries.TotalCount != 1 || timmySeries.Items[0].ID != f.sKidsID {
		t.Fatalf("timmy ListSeries = (%+v, %v), want [sKids]", timmySeries, err)
	}
	timmyVols, err := f.svc.ListVolumes(ctx, f.timmy, app.VolumeFilter{})
	if err != nil || timmyVols.TotalCount != 2 {
		t.Fatalf("timmy ListVolumes = (%+v, %v), want 2", timmyVols, err)
	}
	for _, v := range timmyVols.Items {
		if v.ID == f.vStandID && v.SeriesID != nil {
			t.Fatalf("expected standalone volume nil SeriesID, got %v", *v.SeriesID)
		}
		if v.ID == f.vMissingID && (v.AssetCount != 0 || !v.IsMissing) {
			t.Fatalf("expected zero-asset missing volume preserved, got %+v", v)
		}
	}

	// Folder browse hides RestrictedFolder from reader and preserves raw non-UTF-8 folder bytes.
	topFolder, err := f.svc.BrowseFolder(ctx, f.reader, app.FolderBrowseFilter{LibraryID: f.lib1ID})
	if err != nil {
		t.Fatalf("BrowseFolder(root): %v", err)
	}
	for _, e := range topFolder.Entries {
		if e.NameDisplay == "RestrictedFolder" {
			t.Fatalf("BrowseFolder leaked restricted subfolder: %+v", e)
		}
	}

	rawBytes, _ := hex.DecodeString(f.rawDirHex)
	subFolder, err := f.svc.BrowseFolder(ctx, f.reader, app.FolderBrowseFilter{
		LibraryID:       f.lib1ID,
		FolderPathBytes: rawBytes,
	})
	if err != nil {
		t.Fatalf("BrowseFolder(raw non-UTF-8): %v", err)
	}
	if !subFolder.InvalidUTF8 || subFolder.FolderPathHex != f.rawDirHex || len(subFolder.Entries) != 1 {
		t.Fatalf("unexpected raw non-UTF-8 subfolder result: %+v", subFolder)
	}

	if _, err := f.svc.BrowseFolder(ctx, f.reader, app.FolderBrowseFilter{LibraryID: "missing-lib"}); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("BrowseFolder(missing lib) = %v, want ErrNotFound", err)
	}
	if _, err := f.svc.BrowseFolder(ctx, f.reader, app.FolderBrowseFilter{LibraryID: f.lib1ID, FolderPathBytes: []byte("../etc")}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("BrowseFolder(traversal) = %v, want ErrInvalidInput", err)
	}
}

func ptrStr(s string) *string { return &s }
