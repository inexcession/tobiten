package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/bootstrap"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestDaemonLibraryAndCatalogueReadAPI(t *testing.T) {
	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("migrations.RunFS: %v", err)
	}

	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_BASE_URL=http://localhost:8080/comics",
		"TOBITEN_LOG_FORMAT=json",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	userRepo := postgres.NewUserRepository(db.Pool)
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	setup, err := bootstrap.NewSetup(ctx, userRepo, discardLogger, "http://localhost:8080/comics/api/v1/auth/setup")
	if err != nil {
		t.Fatalf("bootstrap.NewSetup: %v", err)
	}
	defer setup.Close()

	daemonSvcs, err := newDaemonServices(db.Pool, setup, userRepo)
	if err != nil {
		t.Fatalf("newDaemonServices: %v", err)
	}
	if daemonSvcs.app == nil || daemonSvcs.app.Library == nil {
		t.Fatal("expected newDaemonServices to wire LibraryService")
	}

	reg := BuildRegistryWithServices(cfg, daemonSvcs.app)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv, err := api.NewServer(api.ServerOptions{
		Config:   cfg,
		Logger:   discardLogger,
		Listener: ln,
		Registry: reg,
	})
	if err != nil {
		_ = ln.Close()
		t.Fatalf("api.NewServer: %v", err)
	}
	defer func() { _ = srv.Close() }()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	admin := createTestUserWithPassword(ctx, t, userRepo, "libadmin", "admin-pass-123", domain.RoleAdminID)
	reader := createTestUserWithPassword(ctx, t, userRepo, "libreader", "reader-pass-123", domain.RoleReaderID)
	restricted := createTestUserWithPassword(ctx, t, userRepo, "kidreader", "kid-pass-123", domain.RoleRestrictedReaderID)
	_ = createTestUserWithPassword(ctx, t, userRepo, "blankkid", "blank-pass-123", domain.RoleRestrictedReaderID)
	noReadUser := createTestUserWithPassword(ctx, t, userRepo, "jobsonly", "jobs-pass-123", "")

	// Seed a custom role lacking domain.PermissionLibraryRead for 403 verification.
	if _, err := db.Exec(ctx, `INSERT INTO roles (id, name, built_in, requires_allow_labels) VALUES ($1, $2, false, false)`,
		"role-jobs-only", "Jobs Only"); err != nil {
		t.Fatalf("insert role-jobs-only: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO role_permissions (role_id, permission) VALUES ($1, $2)`,
		"role-jobs-only", string(domain.PermissionJobsReadAll)); err != nil {
		t.Fatalf("insert role_permissions: %v", err)
	}
	if err := userRepo.AssignUserRole(ctx, noReadUser.ID, "role-jobs-only"); err != nil {
		t.Fatalf("assign custom role: %v", err)
	}

	adminReadPAT := createTestPAT(ctx, t, userRepo, admin.ID, []domain.PATScope{domain.ScopeRead})
	adminFullPAT := createTestPAT(ctx, t, userRepo, admin.ID, []domain.PATScope{domain.ScopeRead, domain.ScopeLibraryAdmin})

	rawDirHex := seedDaemonCatalogueFixture(ctx, t, db.Pool, reader.ID, restricted.ID)
	client := &http.Client{Timeout: 5 * time.Second}

	t.Run("M1-AC5 closed allowlist and ScanLibrary remains unimplemented", func(t *testing.T) {
		for _, p := range []string{
			"/api/v1/libraries",
			"/comics/api/v1/libraries/lib-main",
			"/api/v1/libraries/lib-main/folders",
			"/api/v1/series",
			"/api/v1/series/ser-clean",
			"/api/v1/volumes",
			"/api/v1/volumes/vol-stand",
		} {
			resp, err := client.Get(ts.URL + p)
			if err != nil {
				t.Fatalf("GET %s: %v", p, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unauthenticated GET %s = %d, want 401", p, resp.StatusCode)
			}
		}

		adminPrincipal := domain.NewPrincipal(admin, []domain.Role{{ID: domain.RoleAdminID, Permissions: []domain.Permission{domain.PermissionLibraryAdmin}}}, domain.CredentialKindPassword, "c1", nil)
		if _, err := daemonSvcs.app.Library.ScanLibrary(ctx, adminPrincipal, "lib-main"); !errors.Is(err, app.ErrNotImplemented) {
			t.Fatalf("ScanLibrary = %v, want ErrNotImplemented", err)
		}
	})

	t.Run("missing library:read returns 403 Forbidden across all routes even on invalid query params", func(t *testing.T) {
		for _, p := range []string{
			"/api/v1/libraries",
			"/api/v1/libraries?offset=bad",
			"/api/v1/libraries/lib-main",
			"/api/v1/libraries/lib-main/folders",
			"/api/v1/libraries/lib-main/folders?path_hex=zzzz",
			"/api/v1/series",
			"/api/v1/series?library_id=%20",
			"/api/v1/series/ser-clean",
			"/api/v1/volumes",
			"/api/v1/volumes?limit=-1",
			"/api/v1/volumes/vol-stand",
		} {
			code, _ := doBasicGet(t, client, ts.URL+p, "jobsonly", "jobs-pass-123")
			if code != http.StatusForbidden {
				t.Fatalf("missing library:read GET %s = %d, want 403", p, code)
			}
		}
	})

	t.Run("libraries, roots, ONLINE/OFFLINE state, and library:admin redaction", func(t *testing.T) {
		code, readerBody := doBasicGet(t, client, ts.URL+"/comics/api/v1/libraries?limit=1&offset=0", "libreader", "reader-pass-123")
		if code != http.StatusOK {
			t.Fatalf("reader GET libraries = %d (%s), want 200", code, readerBody)
		}
		var page app.LibraryPage
		mustUnmarshal(t, readerBody, &page)
		if page.TotalCount != 2 || page.Limit != 1 || len(page.Items) != 1 {
			t.Fatalf("unexpected libraries page: %+v", page)
		}
		if page.Items[0].State != "OFFLINE" || page.Items[0].IntakePathDisplay != nil {
			t.Fatalf("expected OFFLINE + redacted intake on lib-main for reader, got %+v", page.Items[0])
		}
		for _, r := range page.Items[0].Roots {
			if r.PathDisplay != nil || r.LastError != nil {
				t.Fatalf("reader leaked root path/last_error: %+v", r)
			}
		}

		// ScopeRead PAT owned by admin must ALSO redact path_display & last_error (ADR-0006 I-2).
		patCode, patBody := doBearerGet(t, client, ts.URL+"/api/v1/libraries/lib-main", adminReadPAT)
		if patCode != http.StatusOK || !strings.Contains(patBody, `"path_display":null`) || !strings.Contains(patBody, `"last_error":null`) {
			t.Fatalf("read PAT should redact root paths & last_error: %d %s", patCode, patBody)
		}

		// Admin PAT with ScopeLibraryAdmin sees display paths & last_error.
		adminCode, adminBody := doBearerGet(t, client, ts.URL+"/api/v1/libraries/lib-main", adminFullPAT)
		if adminCode != http.StatusOK {
			t.Fatalf("admin GET /libraries/lib-main = %d (%s)", adminCode, adminBody)
		}
		var adminLib app.Library
		mustUnmarshal(t, adminBody, &adminLib)
		if adminLib.IntakePathDisplay == nil || *adminLib.IntakePathDisplay != "/srv/intake/main" {
			t.Fatalf("expected admin intake display, got %+v", adminLib)
		}
		if len(adminLib.Roots) != 2 {
			t.Fatalf("expected 2 roots, got %d", len(adminLib.Roots))
		}

		// Online library reports ONLINE.
		codeAlt, bodyAlt := doBasicGet(t, client, ts.URL+"/api/v1/libraries/lib-alt", "libreader", "reader-pass-123")
		if codeAlt != http.StatusOK {
			t.Fatalf("reader GET /libraries/lib-alt = %d", codeAlt)
		}
		var altLib app.Library
		mustUnmarshal(t, bodyAlt, &altLib)
		if altLib.State != "ONLINE" || len(altLib.Roots) != 1 || altLib.Roots[0].State != "ONLINE" {
			t.Fatalf("expected ONLINE lib-alt, got %+v", altLib)
		}
	})

	t.Run("series, volumes, label enforcement, standalone series_id null, and pagination", func(t *testing.T) {
		// Reader sees clean + kids series (total_count=2), not nsfw or trashed.
		code, body := doBasicGet(t, client, ts.URL+"/comics/api/v1/series?library_id=lib-main&limit=999", "libreader", "reader-pass-123")
		if code != http.StatusOK {
			t.Fatalf("GET /series = %d (%s)", code, body)
		}
		var sp app.SeriesPage
		mustUnmarshal(t, body, &sp)
		if sp.TotalCount != 2 || sp.Limit != app.MaxLibraryPageLimit || len(sp.Items) != 2 {
			t.Fatalf("unexpected series page: %+v", sp)
		}

		// Restricted, trashed, or unallowed items return 404 by ID (same as missing).
		for _, id := range []string{"ser-nsfw", "ser-trashed", "ser-does-not-exist"} {
			c, _ := doBasicGet(t, client, ts.URL+"/api/v1/series/"+id, "libreader", "reader-pass-123")
			if c != http.StatusNotFound {
				t.Fatalf("reader GET /series/%s = %d, want 404", id, c)
			}
		}

		// Kid reader with ALLOW kids sees only ser-kids; blankkid with 0 ALLOW fails closed (0 items, 404 by ID).
		kidCode, kidBody := doBasicGet(t, client, ts.URL+"/api/v1/series", "kidreader", "kid-pass-123")
		if kidCode != http.StatusOK {
			t.Fatalf("kid GET /series = %d", kidCode)
		}
		var kidPage app.SeriesPage
		mustUnmarshal(t, kidBody, &kidPage)
		if kidPage.TotalCount != 1 || kidPage.Items[0].ID != "ser-kids" {
			t.Fatalf("kid series = %+v, want [ser-kids]", kidPage)
		}
		if c, _ := doBasicGet(t, client, ts.URL+"/api/v1/series/ser-clean", "kidreader", "kid-pass-123"); c != http.StatusNotFound {
			t.Fatalf("kid GET /series/ser-clean = %d, want 404", c)
		}

		blankCode, blankBody := doBasicGet(t, client, ts.URL+"/api/v1/series", "blankkid", "blank-pass-123")
		if blankCode != http.StatusOK {
			t.Fatalf("blankkid GET /series = %d", blankCode)
		}
		var blankPage app.SeriesPage
		mustUnmarshal(t, blankBody, &blankPage)
		if blankPage.TotalCount != 0 || len(blankPage.Items) != 0 {
			t.Fatalf("expected fail-closed empty series for blankkid, got %+v", blankPage)
		}

		// Standalone volume lists with explicit `"series_id":null` and `"series_memberships":[]` on both list and detail routes.
		vCode, vBody := doBasicGet(t, client, ts.URL+"/api/v1/volumes?search=Standalone", "libreader", "reader-pass-123")
		if vCode != http.StatusOK || !strings.Contains(vBody, `"series_id":null`) || !strings.Contains(vBody, `"series_memberships":[]`) {
			t.Fatalf("expected explicit null series_id and [] series_memberships in GET /volumes: %d %s", vCode, vBody)
		}
		vIDCode, vIDBody := doBasicGet(t, client, ts.URL+"/api/v1/volumes/vol-stand", "libreader", "reader-pass-123")
		if vIDCode != http.StatusOK || !strings.Contains(vIDBody, `"series_id":null`) || !strings.Contains(vIDBody, `"series_memberships":[]`) {
			t.Fatalf("expected explicit null series_id and [] series_memberships in GET /volumes/vol-stand: %d %s", vIDCode, vIDBody)
		}
		if c, _ := doBasicGet(t, client, ts.URL+"/api/v1/volumes/vol-nsfw", "libreader", "reader-pass-123"); c != http.StatusNotFound {
			t.Fatalf("reader GET /volumes/vol-nsfw = %d, want 404", c)
		}

		// Page count is derived from the permitted available file (not volumes.page_count or restricted preferred asset)
		// and returns 0 when no readable file exists on both GET /volumes and GET /volumes/{id}.
		listAllCode, listAllBody := doBasicGet(t, client, ts.URL+"/api/v1/volumes?library_id=lib-main", "libreader", "reader-pass-123")
		if listAllCode != http.StatusOK {
			t.Fatalf("reader GET /volumes = %d (%s)", listAllCode, listAllBody)
		}
		var readerVolPage app.VolumePage
		mustUnmarshal(t, listAllBody, &readerVolPage)
		volByID := make(map[string]app.Volume, len(readerVolPage.Items))
		for _, v := range readerVolPage.Items {
			volByID[v.ID] = v
		}
		if vc := volByID["vol-clean"]; vc.PageCount != 24 || vc.PreferredAssetID != nil {
			t.Fatalf("reader list vol-clean = %+v, want PageCount=24 and redacted PreferredAssetID", vc)
		}
		if vnf := volByID["vol-nofile"]; vnf.PageCount != 0 || vnf.AssetCount != 0 || !vnf.IsMissing {
			t.Fatalf("reader list vol-nofile = %+v, want PageCount=0, AssetCount=0, IsMissing=true", vnf)
		}
		for _, check := range []struct {
			id       string
			wantPage int
		}{
			{"vol-clean", 24},
			{"vol-nofile", 0},
		} {
			dc, db := doBasicGet(t, client, ts.URL+"/api/v1/volumes/"+check.id, "libreader", "reader-pass-123")
			if dc != http.StatusOK {
				t.Fatalf("reader GET /volumes/%s = %d", check.id, dc)
			}
			var dv app.Volume
			mustUnmarshal(t, db, &dv)
			if dv.PageCount != check.wantPage {
				t.Fatalf("reader detail GET /volumes/%s PageCount = %d, want %d", check.id, dv.PageCount, check.wantPage)
			}
		}

		// Admin who can access the restricted preferred asset sees its preferred page_count (144).
		admVC, admVB := doBasicGet(t, client, ts.URL+"/api/v1/volumes/vol-clean", "libadmin", "admin-pass-123")
		if admVC != http.StatusOK {
			t.Fatalf("admin GET /volumes/vol-clean = %d", admVC)
		}
		var admVol app.Volume
		mustUnmarshal(t, admVB, &admVol)
		if admVol.PageCount != 144 {
			t.Fatalf("admin detail GET /volumes/vol-clean PageCount = %d, want 144", admVol.PageCount)
		}

		// Two visible memberships (primary + secondary) for reader; hidden primary + allowed secondary for kidreader.
		vm := volByID["vol-multi"]
		if vm.SeriesID == nil || *vm.SeriesID != "ser-clean" || len(vm.SeriesMemberships) != 2 ||
			vm.SeriesMemberships[0].SeriesID != "ser-clean" || !vm.SeriesMemberships[0].IsPrimary ||
			vm.SeriesMemberships[0].Position == nil || *vm.SeriesMemberships[0].Position != 1.0 ||
			vm.SeriesMemberships[1].SeriesID != "ser-kids" || vm.SeriesMemberships[1].IsPrimary ||
			vm.SeriesMemberships[1].Position == nil || *vm.SeriesMemberships[1].Position != 7.0 {
			t.Fatalf("reader vol-multi memberships mismatch: %+v", vm)
		}

		kidVolC, kidVolB := doBasicGet(t, client, ts.URL+"/api/v1/volumes/vol-multi", "kidreader", "kid-pass-123")
		if kidVolC != http.StatusOK {
			t.Fatalf("kidreader GET /volumes/vol-multi = %d (%s)", kidVolC, kidVolB)
		}
		if !strings.Contains(kidVolB, `"series_id":null`) {
			t.Fatalf("expected explicit null series_id when primary is hidden: %s", kidVolB)
		}
		var kidVol app.Volume
		mustUnmarshal(t, kidVolB, &kidVol)
		if kidVol.SeriesID != nil || len(kidVol.SeriesMemberships) != 1 ||
			kidVol.SeriesMemberships[0].SeriesID != "ser-kids" || kidVol.SeriesMemberships[0].IsPrimary ||
			kidVol.SeriesMemberships[0].Position == nil || *kidVol.SeriesMemberships[0].Position != 7.0 {
			t.Fatalf("kidreader vol-multi memberships mismatch: %+v", kidVol)
		}
	})

	t.Run("folders browse merges roots, enforces labels, and supports raw non-UTF-8 paths", func(t *testing.T) {
		topCode, topBody := doBasicGet(t, client, ts.URL+"/comics/api/v1/libraries/lib-main/folders", "libreader", "reader-pass-123")
		if topCode != http.StatusOK {
			t.Fatalf("reader GET folders = %d (%s)", topCode, topBody)
		}
		var topRes app.FolderBrowseResult
		mustUnmarshal(t, topBody, &topRes)
		for _, e := range topRes.Entries {
			if e.NameDisplay == "RestrictedDir" {
				t.Fatalf("folder browse leaked restricted directory: %+v", e)
			}
		}

		subCode, subBody := doBasicGet(t, client, ts.URL+"/api/v1/libraries/lib-main/folders?path_hex="+rawDirHex, "libreader", "reader-pass-123")
		if subCode != http.StatusOK {
			t.Fatalf("raw path_hex folder browse = %d (%s)", subCode, subBody)
		}
		var subRes app.FolderBrowseResult
		mustUnmarshal(t, subBody, &subRes)
		if !subRes.InvalidUTF8 || len(subRes.Entries) != 1 || subRes.Entries[0].NameDisplay != "ep1.cbz" {
			t.Fatalf("unexpected raw subfolder entries: %+v", subRes)
		}

		// Percent-escaped raw bytes ?path=Raw-%FF-Dir also work end-to-end.
		pctCode, _ := doBasicGet(t, client, ts.URL+"/api/v1/libraries/lib-main/folders?path=Raw-%FF-Dir", "libreader", "reader-pass-123")
		if pctCode != http.StatusOK {
			t.Fatalf("percent-escaped non-UTF-8 folder browse = %d, want 200", pctCode)
		}

		if c, _ := doBasicGet(t, client, ts.URL+"/api/v1/libraries/missing-lib/folders", "libreader", "reader-pass-123"); c != http.StatusNotFound {
			t.Fatalf("missing library folder browse = %d, want 404", c)
		}
		if c, _ := doBasicGet(t, client, ts.URL+"/api/v1/libraries/lib-main/folders?path=../escape", "libreader", "reader-pass-123"); c != http.StatusBadRequest {
			t.Fatalf("traversal folder browse = %d, want 400", c)
		}
	})
}

func seedDaemonCatalogueFixture(ctx context.Context, t *testing.T, db postgres.DBTX, readerUserID, kidUserID string) string {
	t.Helper()
	pool := postgres.NewTransactor(db.(postgres.TxBeginner))
	now := time.Now().UTC().Truncate(time.Microsecond)
	intake := domain.NewPathIdentity([]byte("/srv/intake/main"))
	rootErr := "stat root: /mnt/lib-main/offline missing"
	rawDir := []byte("Raw-\xff-Dir")
	rawFile := append(append([]byte{}, rawDir...), []byte("/ep1.cbz")...)

	if err := pool.WithTx(ctx, func(tx storage.Tx) error {
		libs := tx.Libraries()
		for _, lib := range []domain.Library{
			{ID: "lib-main", Name: "Main", Mode: domain.LibraryModeManaged, IntakePath: &intake, ResolverChain: domain.DefaultResolverChain(), MetadataPrecedence: domain.MetadataPrecedenceEmbeddedOverOnline, DefaultCoverPolicy: domain.CoverPolicyFirst, CreatedAt: now},
			{ID: "lib-alt", Name: "Alt", Mode: domain.LibraryModeVirtual, ResolverChain: domain.DefaultResolverChain(), MetadataPrecedence: domain.MetadataPrecedenceEmbeddedOverOnline, DefaultCoverPolicy: domain.CoverPolicyFirst, CreatedAt: now.Add(time.Second)},
		} {
			l := lib
			if err := libs.CreateLibrary(ctx, &l); err != nil {
				return err
			}
		}
		for _, r := range []domain.LibraryRoot{
			{ID: "r-main-1", LibraryID: "lib-main", Path: domain.NewPathIdentity([]byte("/mnt/lib-main/r1")), State: domain.LibraryRootStateOnline},
			{ID: "r-main-2", LibraryID: "lib-main", Path: domain.NewPathIdentity([]byte("/mnt/lib-main/r2")), State: domain.LibraryRootStateOffline, LastError: &rootErr},
			{ID: "r-alt-1", LibraryID: "lib-alt", Path: domain.NewPathIdentity([]byte("/mnt/lib-alt/r1")), State: domain.LibraryRootStateOnline},
		} {
			root := r
			if err := libs.CreateLibraryRoot(ctx, &root); err != nil {
				return err
			}
		}

		cw := tx.CatalogueWrites()
		for _, s := range []domain.Series{
			{ID: "ser-clean", LibraryID: "lib-main", Title: "Clean Series", TitleSort: "clean series", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-kids", LibraryID: "lib-main", Title: "Kids Series", TitleSort: "kids series", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-nsfw", LibraryID: "lib-main", Title: "NSFW Series", TitleSort: "nsfw series", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-trashed", LibraryID: "lib-main", Title: "Trashed Series", TitleSort: "trashed series", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
		} {
			ser := s
			if err := cw.CreateSeries(ctx, &ser); err != nil {
				return err
			}
		}

		standTitle := "Standalone Anthology"
		nsfwTitle := "NSFW Vol 1"
		multiTitle := "Crossover Vol 1"
		noFileTitle := "No Readable File Vol"
		for _, v := range []domain.Volume{
			{ID: "vol-clean", LibraryID: "lib-main", Kind: domain.VolumeKindVolume, PageCount: 144, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-stand", LibraryID: "lib-main", Kind: domain.VolumeKindVolume, Title: &standTitle, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-nsfw", LibraryID: "lib-main", Kind: domain.VolumeKindVolume, Title: &nsfwTitle, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-multi", LibraryID: "lib-main", Kind: domain.VolumeKindVolume, Title: &multiTitle, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-nofile", LibraryID: "lib-main", Kind: domain.VolumeKindVolume, Title: &noFileTitle, PageCount: 144, CreatedAt: now, UpdatedAt: now},
		} {
			vol := v
			if err := cw.CreateVolume(ctx, &vol); err != nil {
				return err
			}
		}

		sm := tx.SeriesMemberships()
		pos := 1.0
		secPos := 7.0
		for _, entry := range []struct {
			volID string
			list  []domain.SeriesMembership
		}{
			{"vol-clean", []domain.SeriesMembership{{VolumeID: "vol-clean", SeriesID: "ser-clean", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceResolver}}},
			{"vol-nsfw", []domain.SeriesMembership{{VolumeID: "vol-nsfw", SeriesID: "ser-nsfw", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceResolver}}},
			{"vol-multi", []domain.SeriesMembership{
				{VolumeID: "vol-multi", SeriesID: "ser-clean", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceResolver},
				{VolumeID: "vol-multi", SeriesID: "ser-kids", IsPrimary: false, Kind: domain.MembershipKindSubSeries, Position: &secPos, Source: domain.MembershipSourceMetadata},
			}},
			{"vol-nofile", []domain.SeriesMembership{{VolumeID: "vol-nofile", SeriesID: "ser-clean", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceResolver}}},
		} {
			if err := sm.SetVolumeMemberships(ctx, entry.volID, entry.list); err != nil {
				return err
			}
		}

		for _, a := range []struct {
			id        string
			vol       string
			root      string
			rel       []byte
			pages     int
			createdAt time.Time
		}{
			{"a-clean", "vol-clean", "r-main-1", []byte("Clean/v1.cbz"), 24, now},
			{"a-raw", "vol-clean", "r-main-2", rawFile, 32, now.Add(time.Second)},
			{"a-clean-nsfw", "vol-clean", "r-main-1", []byte("RestrictedDir/v1-uncensored.cbz"), 144, now.Add(2 * time.Second)},
			{"a-stand", "vol-stand", "r-main-1", []byte("Standalone.cbz"), 10, now},
			{"a-nsfw", "vol-nsfw", "r-main-1", []byte("RestrictedDir/nsfw.cbz"), 50, now},
			{"a-multi", "vol-multi", "r-main-1", []byte("Clean/multi.cbz"), 18, now},
			{"a-nofile-nsfw", "vol-nofile", "r-main-1", []byte("RestrictedDir/nofile.cbz"), 144, now},
		} {
			ident := domain.NewPathIdentity(a.rel)
			asset := domain.MediaAsset{
				ID:             a.id,
				VolumeID:       a.vol,
				LibraryRootID:  a.root,
				RelPathBytes:   ident.Bytes,
				RelPathDisplay: ident.Display,
				RelPathMatch:   ident.Match,
				FileSize:       100,
				Format:         domain.MediaFormatCBZ,
				PageCount:      a.pages,
				CreatedAt:      a.createdAt,
				UpdatedAt:      a.createdAt,
			}
			if err := cw.CreateAsset(ctx, &asset); err != nil {
				return err
			}
		}
		prefID := "a-clean-nsfw"
		if err := cw.UpdateVolume(ctx, &domain.Volume{
			ID:               "vol-clean",
			LibraryID:        "lib-main",
			Kind:             domain.VolumeKindVolume,
			PageCount:        144,
			PreferredAssetID: &prefID,
			CreatedAt:        now,
			UpdatedAt:        now,
		}); err != nil {
			return err
		}

		labels := tx.Labels()
		if err := labels.EnsureBuiltInServerLabels(ctx); err != nil {
			return err
		}
		for _, l := range []domain.Label{
			{ID: "lbl-kids", Name: "Kids", Scope: domain.LabelScopeServer},
			{ID: "lbl-nsfw", Name: "NSFW", Scope: domain.LabelScopeServer},
		} {
			lbl := l
			if err := labels.CreateLabel(ctx, &lbl); err != nil {
				return err
			}
		}
		if err := labels.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{UserID: readerUserID, LabelID: "lbl-nsfw", Effect: domain.PolicyEffectRestrict}); err != nil {
			return err
		}
		if err := labels.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{UserID: kidUserID, LabelID: "lbl-kids", Effect: domain.PolicyEffectAllow}); err != nil {
			return err
		}
		for _, il := range []domain.ItemLabel{
			{LabelID: "lbl-kids", ItemType: domain.ItemTypeSeries, ItemID: "ser-kids"},
			{LabelID: "lbl-nsfw", ItemType: domain.ItemTypeSeries, ItemID: "ser-nsfw"},
			{LabelID: "lbl-nsfw", ItemType: domain.ItemTypeAsset, ItemID: "a-clean-nsfw"},
			{LabelID: "lbl-nsfw", ItemType: domain.ItemTypeAsset, ItemID: "a-nofile-nsfw"},
			{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeSeries, ItemID: "ser-trashed"},
		} {
			itemLabel := il
			if err := labels.AssignItemLabel(ctx, &itemLabel); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seedDaemonCatalogueFixture: %v", err)
	}
	return hex.EncodeToString(rawDir)
}

func doBasicGet(t *testing.T, client *http.Client, url, user, pass string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest(%s): %v", url, err)
	}
	req.SetBasicAuth(user, pass)
	return execReq(t, client, req)
}

func doBearerGet(t *testing.T, client *http.Client, url, token string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest(%s): %v", url, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return execReq(t, client, req)
}

func execReq(t *testing.T, client *http.Client, req *http.Request) (int, string) {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return resp.StatusCode, string(raw)
}

func mustUnmarshal(t *testing.T, raw string, dst any) {
	t.Helper()
	if err := json.Unmarshal([]byte(raw), dst); err != nil {
		t.Fatalf("Unmarshal JSON %q: %v", raw, err)
	}
}
