package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/bootstrap"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/scanner"
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

	daemonSvcs, err := newDaemonServices(db.Pool, cfg, setup, userRepo)
	if err != nil {
		t.Fatalf("newDaemonServices: %v", err)
	}
	if daemonSvcs.app == nil || daemonSvcs.app.Library == nil || daemonSvcs.app.LibraryConfig == nil {
		t.Fatal("expected newDaemonServices to wire LibraryService and LibraryConfigService")
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

	t.Run("M1-AC5 closed allowlist and unknown library scan returns 404", func(t *testing.T) {
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

		for _, p := range []string{
			"/api/v1/libraries/lib-main/scan",
			"/comics/api/v1/libraries/lib-main/scan",
		} {
			resp, err := client.Post(ts.URL+p, "application/json", nil)
			if err != nil {
				t.Fatalf("POST %s: %v", p, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unauthenticated POST %s = %d, want 401", p, resp.StatusCode)
			}
		}

		adminPrincipal := domain.NewPrincipal(admin, []domain.Role{{ID: domain.RoleAdminID, Permissions: []domain.Permission{domain.PermissionLibraryAdmin}}}, domain.CredentialKindPassword, "c1", nil)
		if _, err := daemonSvcs.app.Library.ScanLibrary(ctx, adminPrincipal, "missing-lib"); !errors.Is(err, app.ErrNotFound) {
			t.Fatalf("ScanLibrary(missing-lib) = %v, want ErrNotFound", err)
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

func startLibraryTestServer(t *testing.T, db *testdb.TestDB, env []string) (*httptest.Server, *postgres.UserRepository) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("migrations.RunFS: %v", err)
	}

	baseEnv := []string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_BASE_URL=http://localhost:8080/comics",
		"TOBITEN_LOG_FORMAT=json",
	}
	baseEnv = append(baseEnv, env...)
	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron(baseEnv))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	userRepo := postgres.NewUserRepository(db.Pool)
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	setup, err := bootstrap.NewSetup(ctx, userRepo, discardLogger, "http://localhost:8080/comics/api/v1/auth/setup")
	if err != nil {
		t.Fatalf("bootstrap.NewSetup: %v", err)
	}
	t.Cleanup(func() { setup.Close() })

	daemonSvcs, err := newDaemonServices(db.Pool, cfg, setup, userRepo)
	if err != nil {
		t.Fatalf("newDaemonServices: %v", err)
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
	t.Cleanup(func() { _ = srv.Close() })

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, userRepo
}

func countLibraryAndRootRows(ctx context.Context, t *testing.T, db *testdb.TestDB) (int, int) {
	t.Helper()
	var libs, roots int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM libraries`).Scan(&libs); err != nil {
		t.Fatalf("count libraries: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM library_roots`).Scan(&roots); err != nil {
		t.Fatalf("count library_roots: %v", err)
	}
	return libs, roots
}

func TestDaemonLibraryProvisioningRoutes(t *testing.T) {
	db := testdb.New(t)
	ts, userRepo := startLibraryTestServer(t, db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	admin := createTestUserWithPassword(ctx, t, userRepo, "provadmin", "admin-pass-123", domain.RoleAdminID)
	_ = createTestUserWithPassword(ctx, t, userRepo, "provreader", "reader-pass-123", domain.RoleReaderID)
	_ = createTestUserWithPassword(ctx, t, userRepo, "provcurator", "curator-pass-123", domain.RoleCuratorID)

	readPAT := createTestPAT(ctx, t, userRepo, admin.ID, []domain.PATScope{domain.ScopeRead})
	metaPAT := createTestPAT(ctx, t, userRepo, admin.ID, []domain.PATScope{domain.ScopeMetadataWrite})
	libAdminPAT := createTestPAT(ctx, t, userRepo, admin.ID, []domain.PATScope{domain.ScopeLibraryAdmin})

	baseDir := t.TempDir()
	canonBase, err := paths.CanonicalPath(baseDir)
	if err != nil {
		t.Fatalf("CanonicalPath(baseDir): %v", err)
	}
	root1 := filepath.Join(canonBase, "root1")
	root1Sub := filepath.Join(root1, "nested")
	root2 := filepath.Join(canonBase, "root2")
	root3 := filepath.Join(canonBase, "root3")
	parentDir := filepath.Join(canonBase, "parent")
	parentChild := filepath.Join(parentDir, "child")
	intakeDir := filepath.Join(canonBase, "intake")
	for _, d := range []string{root1Sub, root2, root3, parentChild, intakeDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}
	aliasRoot1 := filepath.Join(canonBase, "alias-root1")
	if err := os.Symlink(root1, aliasRoot1); err != nil {
		t.Fatalf("Symlink(alias-root1): %v", err)
	}
	aliasParent := filepath.Join(canonBase, "alias-parent")
	if err := os.Symlink(parentDir, aliasParent); err != nil {
		t.Fatalf("Symlink(alias-parent): %v", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}

	doJSONPost := func(url, body string, mutate func(*http.Request)) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
		if err != nil {
			t.Fatalf("NewRequest(%s): %v", url, err)
		}
		req.Header.Set("Content-Type", "application/json")
		if mutate != nil {
			mutate(req)
		}
		return execReq(t, client, req)
	}

	t.Run("unauthenticated requests return 401 on direct and subpath routes", func(t *testing.T) {
		for _, p := range []string{
			"/api/v1/libraries",
			"/comics/api/v1/libraries",
			"/api/v1/libraries/lib-1/roots",
			"/comics/api/v1/libraries/lib-1/roots",
		} {
			code, _ := doJSONPost(ts.URL+p, `{"name":"X","root_path":"`+root1+`"}`, nil)
			if code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated POST %s = %d, want 401", p, code)
			}
		}
	})

	t.Run("missing library:admin returns 403 and writes no row", func(t *testing.T) {
		beforeLibs, beforeRoots := countLibraryAndRootRows(ctx, t, db)

		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"reader basic", func(r *http.Request) { r.SetBasicAuth("provreader", "reader-pass-123") }},
			{"curator basic", func(r *http.Request) { r.SetBasicAuth("provcurator", "curator-pass-123") }},
			{"read PAT", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+readPAT) }},
			{"metadata PAT", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+metaPAT) }},
		} {
			for _, p := range []string{"/api/v1/libraries", "/api/v1/libraries/lib-1/roots"} {
				code, _ := doJSONPost(ts.URL+p, `{malformed`, tc.mutate)
				if code != http.StatusForbidden {
					t.Fatalf("%s POST %s = %d, want 403", tc.name, p, code)
				}
			}
		}

		afterLibs, afterRoots := countLibraryAndRootRows(ctx, t, db)
		if afterLibs != beforeLibs || afterRoots != beforeRoots {
			t.Fatalf("unauthorized requests mutated DB: before=(%d,%d) after=(%d,%d)", beforeLibs, beforeRoots, afterLibs, afterRoots)
		}
	})

	t.Run("cookie session enforces CSRF and creates library and root when valid", func(t *testing.T) {
		loginReq, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/auth/login", strings.NewReader(`{"username":"provadmin","password":"admin-pass-123"}`))
		if err != nil {
			t.Fatalf("NewRequest login: %v", err)
		}
		loginResp, err := client.Do(loginReq)
		if err != nil {
			t.Fatalf("login failed: %v", err)
		}
		loginRaw, _ := io.ReadAll(loginResp.Body)
		_ = loginResp.Body.Close()
		if loginResp.StatusCode != http.StatusOK {
			t.Fatalf("login status = %d (%s)", loginResp.StatusCode, string(loginRaw))
		}
		var sessCookie *http.Cookie
		for _, c := range loginResp.Cookies() {
			if c.Name == app.NativeSessionCookieName {
				sessCookie = c
			}
		}
		csrfToken := loginResp.Header.Get("X-CSRF-Token")
		if sessCookie == nil || csrfToken == "" {
			t.Fatalf("expected session cookie and X-CSRF-Token, got cookie=%v csrf=%q", sessCookie, csrfToken)
		}

		beforeLibs, beforeRoots := countLibraryAndRootRows(ctx, t, db)

		// Cookie without CSRF or with wrong CSRF or foreign origin -> 403 and no DB write.
		for _, tc := range []struct {
			name   string
			csrf   string
			origin string
			fetch  string
		}{
			{"missing csrf", "", "", ""},
			{"wrong csrf", "wrong-csrf-token", "", ""},
			{"foreign origin", csrfToken, "https://evil.example", ""},
			{"cross-site fetch", csrfToken, "http://localhost:8080", "cross-site"},
		} {
			code, _ := doJSONPost(ts.URL+"/api/v1/libraries", `{"name":"CSRF Blocked","root_path":"`+root1+`"}`, func(r *http.Request) {
				r.AddCookie(&http.Cookie{Name: app.NativeSessionCookieName, Value: sessCookie.Value})
				if tc.csrf != "" {
					r.Header.Set("X-CSRF-Token", tc.csrf)
				}
				if tc.origin != "" {
					r.Header.Set("Origin", tc.origin)
				}
				if tc.fetch != "" {
					r.Header.Set("Sec-Fetch-Site", tc.fetch)
				}
			})
			if code != http.StatusForbidden {
				t.Fatalf("create library (%s) = %d, want 403", tc.name, code)
			}
		}

		afterDeniedLibs, afterDeniedRoots := countLibraryAndRootRows(ctx, t, db)
		if afterDeniedLibs != beforeLibs || afterDeniedRoots != beforeRoots {
			t.Fatalf("CSRF-denied requests mutated DB: before=(%d,%d) after=(%d,%d)", beforeLibs, beforeRoots, afterDeniedLibs, afterDeniedRoots)
		}

		// Valid cookie + valid CSRF creates VIRTUAL library (default mode) and its first root in one tx.
		createBody := `{"name":"Cookie Manga","root_path":"` + root1 + `"}`
		code, respBody := doJSONPost(ts.URL+"/comics/api/v1/libraries", createBody, func(r *http.Request) {
			r.AddCookie(&http.Cookie{Name: app.NativeSessionCookieName, Value: sessCookie.Value})
			r.Header.Set("X-CSRF-Token", csrfToken)
			r.Header.Set("Origin", "http://localhost:8080")
			r.Header.Set("Sec-Fetch-Site", "same-origin")
		})
		if code != http.StatusCreated {
			t.Fatalf("valid cookie POST /libraries = %d (%s), want 201", code, respBody)
		}
		var created app.Library
		mustUnmarshal(t, respBody, &created)
		if created.Name != "Cookie Manga" || created.Mode != "VIRTUAL" || len(created.Roots) != 1 ||
			created.Roots[0].PathDisplay == nil || *created.Roots[0].PathDisplay != root1 {
			t.Fatalf("unexpected created library: %+v", created)
		}

		// Cookie without CSRF on POST /libraries/{id}/roots -> 403 and no row.
		addNoCSRFCode, _ := doJSONPost(ts.URL+"/api/v1/libraries/"+created.ID+"/roots", `{"path":"`+root2+`"}`, func(r *http.Request) {
			r.AddCookie(&http.Cookie{Name: app.NativeSessionCookieName, Value: sessCookie.Value})
		})
		if addNoCSRFCode != http.StatusForbidden {
			t.Fatalf("cookie POST /roots without CSRF = %d, want 403", addNoCSRFCode)
		}

		// Valid cookie + CSRF adds a second root.
		addCode, addBody := doJSONPost(ts.URL+"/comics/api/v1/libraries/"+created.ID+"/roots", `{"path":"`+root2+`"}`, func(r *http.Request) {
			r.AddCookie(&http.Cookie{Name: app.NativeSessionCookieName, Value: sessCookie.Value})
			r.Header.Set("X-CSRF-Token", csrfToken)
		})
		if addCode != http.StatusCreated {
			t.Fatalf("valid cookie POST /roots = %d (%s), want 201", addCode, addBody)
		}
		var addedRoot app.LibraryRoot
		mustUnmarshal(t, addBody, &addedRoot)
		if addedRoot.LibraryID != created.ID || addedRoot.PathDisplay == nil || *addedRoot.PathDisplay != root2 {
			t.Fatalf("unexpected added root: %+v", addedRoot)
		}

		// GET /api/v1/libraries/{id} reflects both persisted roots.
		getCode, getBody := doBearerGet(t, client, ts.URL+"/api/v1/libraries/"+created.ID, libAdminPAT)
		if getCode != http.StatusOK {
			t.Fatalf("GET /libraries/%s = %d (%s)", created.ID, getCode, getBody)
		}
		var fetched app.Library
		mustUnmarshal(t, getBody, &fetched)
		if len(fetched.Roots) != 2 {
			t.Fatalf("expected 2 persisted roots, got %+v", fetched.Roots)
		}
	})

	t.Run("overlap rejections (equal, nested, containing, symlink-aliased) return 409 and write no row", func(t *testing.T) {
		// Create a library anchored at parentChild (/parent/child) using library:admin PAT.
		code, body := doJSONPost(ts.URL+"/api/v1/libraries", `{"name":"PAT Managed","mode":"MANAGED","intake_path":"`+intakeDir+`","root_path":"`+parentChild+`"}`, func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+libAdminPAT)
		})
		if code != http.StatusCreated {
			t.Fatalf("PAT POST /libraries = %d (%s), want 201", code, body)
		}
		var patLib app.Library
		mustUnmarshal(t, body, &patLib)

		beforeLibs, beforeRoots := countLibraryAndRootRows(ctx, t, db)

		overlapPaths := []struct {
			name string
			path string
		}{
			{"equal root", root1},
			{"nested inside existing root", root1Sub},
			{"containing existing root", parentDir},
			{"equal via symlink", aliasRoot1},
			{"nested via symlink", filepath.Join(aliasRoot1, "nested")},
			{"containing via symlink", aliasParent},
			{"overlaps existing managed intake", filepath.Join(intakeDir, "sub")},
		}

		for _, tc := range overlapPaths {
			c1, b1 := doJSONPost(ts.URL+"/api/v1/libraries", `{"name":"Overlap-`+tc.name+`","root_path":"`+tc.path+`"}`, func(r *http.Request) {
				r.Header.Set("Authorization", "Bearer "+libAdminPAT)
			})
			if c1 != http.StatusConflict {
				t.Fatalf("CreateLibrary(%s) = %d (%s), want 409", tc.name, c1, b1)
			}

			c2, b2 := doJSONPost(ts.URL+"/api/v1/libraries/"+patLib.ID+"/roots", `{"path":"`+tc.path+`"}`, func(r *http.Request) {
				r.Header.Set("Authorization", "Bearer "+libAdminPAT)
			})
			if c2 != http.StatusConflict {
				t.Fatalf("AddRoot(%s) = %d (%s), want 409", tc.name, c2, b2)
			}
		}

		// Duplicate library name also returns 409 and writes no root row.
		dupCode, dupBody := doJSONPost(ts.URL+"/api/v1/libraries", `{"name":"PAT Managed","root_path":"`+root3+`"}`, func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+libAdminPAT)
		})
		if dupCode != http.StatusConflict {
			t.Fatalf("duplicate library name = %d (%s), want 409", dupCode, dupBody)
		}

		afterLibs, afterRoots := countLibraryAndRootRows(ctx, t, db)
		if afterLibs != beforeLibs || afterRoots != beforeRoots {
			t.Fatalf("409 rejections mutated DB: before=(%d,%d) after=(%d,%d)", beforeLibs, beforeRoots, afterLibs, afterRoots)
		}

		// Disjoint root succeeds via PAT, and missing library returns 404.
		okCode, _ := doJSONPost(ts.URL+"/api/v1/libraries/"+patLib.ID+"/roots", `{"path":"`+root3+`"}`, func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+libAdminPAT)
		})
		if okCode != http.StatusCreated {
			t.Fatalf("PAT AddRoot(disjoint) = %d, want 201", okCode)
		}

		notFoundCode, _ := doJSONPost(ts.URL+"/api/v1/libraries/does-not-exist/roots", `{"path":"`+filepath.Join(canonBase, "root4")+`"}`, func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+libAdminPAT)
		})
		if notFoundCode != http.StatusNotFound {
			t.Fatalf("AddRoot(missing lib) = %d, want 404", notFoundCode)
		}
	})
}

func TestDaemonLibraryProvisioningDefaultModeFromEnv(t *testing.T) {
	db := testdb.New(t)
	ts, userRepo := startLibraryTestServer(t, db, []string{"TOBITEN_LIBRARY_MODE=managed"})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = createTestUserWithPassword(ctx, t, userRepo, "modeadmin", "admin-pass-123", domain.RoleAdminID)

	baseDir := t.TempDir()
	canonBase, err := paths.CanonicalPath(baseDir)
	if err != nil {
		t.Fatalf("CanonicalPath(baseDir): %v", err)
	}
	intake := filepath.Join(canonBase, "intake")
	rootManaged := filepath.Join(canonBase, "managed-root")
	rootVirtual := filepath.Join(canonBase, "virtual-root")
	rootNoIntake := filepath.Join(canonBase, "no-intake-root")
	for _, d := range []string{intake, rootManaged, rootVirtual, rootNoIntake} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}

	client := &http.Client{Timeout: 5 * time.Second}
	postAsAdmin := func(body string) (int, string) {
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/libraries", strings.NewReader(body))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth("modeadmin", "admin-pass-123")
		return execReq(t, client, req)
	}

	// 1. Mode omitted with TOBITEN_LIBRARY_MODE=managed defaults to MANAGED (requires intake_path).
	badCode, badBody := postAsAdmin(`{"name":"Missing Intake","root_path":"` + rootNoIntake + `"}`)
	if badCode != http.StatusBadRequest {
		t.Fatalf("omitted mode under TOBITEN_LIBRARY_MODE=managed without intake = %d (%s), want 400", badCode, badBody)
	}

	// 2. Mode omitted with intake_path creates MANAGED library.
	okCode, okBody := postAsAdmin(`{"name":"Default Managed","root_path":"` + rootManaged + `","intake_path":"` + intake + `"}`)
	if okCode != http.StatusCreated {
		t.Fatalf("omitted mode with intake = %d (%s), want 201", okCode, okBody)
	}
	var managedLib app.Library
	mustUnmarshal(t, okBody, &managedLib)
	if managedLib.Mode != "MANAGED" || managedLib.IntakePathDisplay == nil || *managedLib.IntakePathDisplay != intake {
		t.Fatalf("expected MANAGED library from TOBITEN_LIBRARY_MODE=managed, got %+v", managedLib)
	}

	// 3. Explicit VIRTUAL mode overrides TOBITEN_LIBRARY_MODE=managed.
	vCode, vBody := postAsAdmin(`{"name":"Explicit Virtual","mode":"VIRTUAL","root_path":"` + rootVirtual + `"}`)
	if vCode != http.StatusCreated {
		t.Fatalf("explicit VIRTUAL = %d (%s), want 201", vCode, vBody)
	}
	var virtualLib app.Library
	mustUnmarshal(t, vBody, &virtualLib)
	if virtualLib.Mode != "VIRTUAL" || virtualLib.IntakePathDisplay != nil {
		t.Fatalf("expected explicit VIRTUAL library, got %+v", virtualLib)
	}
}

func TestDaemonLibraryRootRemovalGuardRoute(t *testing.T) {
	db := testdb.New(t)
	ts, userRepo := startLibraryTestServer(t, db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	admin := createTestUserWithPassword(ctx, t, userRepo, "rmadmin", "admin-pass-123", domain.RoleAdminID)
	reader := createTestUserWithPassword(ctx, t, userRepo, "rmreader", "reader-pass-123", domain.RoleReaderID)
	restricted := createTestUserWithPassword(ctx, t, userRepo, "rmkid", "kid-pass-123", domain.RoleRestrictedReaderID)
	_ = createTestUserWithPassword(ctx, t, userRepo, "rmcurator", "curator-pass-123", domain.RoleCuratorID)

	readPAT := createTestPAT(ctx, t, userRepo, admin.ID, []domain.PATScope{domain.ScopeRead})
	libAdminPAT := createTestPAT(ctx, t, userRepo, admin.ID, []domain.PATScope{domain.ScopeLibraryAdmin})

	seedDaemonCatalogueFixture(ctx, t, db.Pool, reader.ID, restricted.ID)

	// Seed a reading_progress row on vol-clean to prove 409 preserves all catalogue and user state.
	if _, err := db.Exec(ctx, `
		INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, source, completed)
		VALUES ($1, 'vol-clean', '{"type":"page","page":4}'::jsonb, 0.25, 'KOMGA', false)
	`, reader.ID); err != nil {
		t.Fatalf("seed reading_progress: %v", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	doReq := func(method, path, body string, mutate func(*http.Request)) (int, string) {
		t.Helper()
		var bodyReader io.Reader
		if body != "" {
			bodyReader = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, bodyReader)
		if err != nil {
			t.Fatalf("NewRequest(%s %s): %v", method, path, err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if mutate != nil {
			mutate(req)
		}
		return execReq(t, client, req)
	}

	// Add an empty root to lib-main via the M1B-D1 provisioning route.
	baseDir := t.TempDir()
	canonBase, err := paths.CanonicalPath(baseDir)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	emptyDir := filepath.Join(canonBase, "empty-root")
	if err := os.MkdirAll(emptyDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	addCode, addBody := doReq(http.MethodPost, "/api/v1/libraries/lib-main/roots", `{"path":"`+emptyDir+`"}`, func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+libAdminPAT)
	})
	if addCode != http.StatusCreated {
		t.Fatalf("POST /roots = %d (%s), want 201", addCode, addBody)
	}
	var emptyRoot app.LibraryRoot
	mustUnmarshal(t, addBody, &emptyRoot)

	// Log in admin for cookie CSRF verification.
	loginCode, _, sessCookie, csrfToken := func() (int, string, *http.Cookie, string) {
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/auth/login", strings.NewReader(`{"username":"rmadmin","password":"admin-pass-123"}`))
		if err != nil {
			t.Fatalf("NewRequest login: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("login failed: %v", err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var ck *http.Cookie
		for _, c := range resp.Cookies() {
			if c.Name == app.NativeSessionCookieName {
				ck = c
			}
		}
		return resp.StatusCode, string(raw), ck, resp.Header.Get("X-CSRF-Token")
	}()
	if loginCode != http.StatusOK || sessCookie == nil || csrfToken == "" {
		t.Fatalf("admin login failed: code=%d cookie=%v csrf=%q", loginCode, sessCookie, csrfToken)
	}

	// 1. Unauthenticated DELETE returns 401 on direct and subpath routes.
	for _, p := range []string{
		"/api/v1/libraries/lib-main/roots/" + emptyRoot.ID,
		"/comics/api/v1/libraries/lib-main/roots/" + emptyRoot.ID,
	} {
		code, _ := doReq(http.MethodDelete, p, "", nil)
		if code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated DELETE %s = %d, want 401", p, code)
		}
	}

	// 2. Missing library:admin or missing CSRF returns 403 and preserves all roots.
	_, beforeRoots := countLibraryAndRootRows(ctx, t, db)
	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
	}{
		{"reader basic", func(r *http.Request) { r.SetBasicAuth("rmreader", "reader-pass-123") }},
		{"curator basic", func(r *http.Request) { r.SetBasicAuth("rmcurator", "curator-pass-123") }},
		{"read PAT", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+readPAT) }},
		{"cookie without CSRF", func(r *http.Request) { r.AddCookie(sessCookie) }},
	} {
		code, body := doReq(http.MethodDelete, "/api/v1/libraries/lib-main/roots/"+emptyRoot.ID, "", tc.mutate)
		if code != http.StatusForbidden {
			t.Fatalf("%s DELETE emptyRoot = %d (%s), want 403", tc.name, code, body)
		}
	}
	_, afterDeniedRoots := countLibraryAndRootRows(ctx, t, db)
	if afterDeniedRoots != beforeRoots {
		t.Fatalf("denied DELETE mutated roots: before=%d after=%d", beforeRoots, afterDeniedRoots)
	}

	// 3. Unknown root or mismatched library/root returns 404.
	for _, p := range []string{
		"/api/v1/libraries/lib-main/roots/does-not-exist",
		"/api/v1/libraries/lib-alt/roots/r-main-1",
	} {
		code, _ := doReq(http.MethodDelete, p, "", func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+libAdminPAT)
		})
		if code != http.StatusNotFound {
			t.Fatalf("DELETE %s = %d, want 404", p, code)
		}
	}

	// 4. Removing a root while any asset belongs to it (available, missing, or retired) returns 409
	//    and preserves the root, assets, volumes, series, and reading_progress.
	for _, tc := range []struct {
		name   string
		rootID string
		prep   string
	}{
		{
			name:   "available assets",
			rootID: "r-main-1",
		},
		{
			name:   "missing asset",
			rootID: "r-main-2",
			prep:   `UPDATE media_assets SET missing_since = NOW(), retired_at = NULL WHERE id = 'a-raw'`,
		},
		{
			name:   "retired asset",
			rootID: "r-main-2",
			prep:   `UPDATE media_assets SET missing_since = NOW() - INTERVAL '10 days', retired_at = NOW() WHERE id = 'a-raw'`,
		},
	} {
		if tc.prep != "" {
			if _, err := db.Exec(ctx, tc.prep); err != nil {
				t.Fatalf("prep %s: %v", tc.name, err)
			}
		}
		code, body := doReq(http.MethodDelete, "/api/v1/libraries/lib-main/roots/"+tc.rootID, "", func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+libAdminPAT)
		})
		if code != http.StatusConflict {
			t.Fatalf("DELETE %s (%s) = %d (%s), want 409", tc.rootID, tc.name, code, body)
		}
	}

	var assetCount, volCount, serCount, progCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM media_assets WHERE library_root_id IN ('r-main-1', 'r-main-2')`).Scan(&assetCount); err != nil || assetCount != 7 {
		t.Fatalf("media_assets preserved = (%d, %v), want 7", assetCount, err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM volumes WHERE library_id = 'lib-main'`).Scan(&volCount); err != nil || volCount != 5 {
		t.Fatalf("volumes preserved = (%d, %v), want 5", volCount, err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM series WHERE library_id = 'lib-main'`).Scan(&serCount); err != nil || serCount != 4 {
		t.Fatalf("series preserved = (%d, %v), want 4", serCount, err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM reading_progress WHERE user_id = $1 AND volume_id = 'vol-clean'`, reader.ID).Scan(&progCount); err != nil || progCount != 1 {
		t.Fatalf("reading_progress preserved = (%d, %v), want 1", progCount, err)
	}

	// 5. Removing an empty root via valid cookie + CSRF on subpath route succeeds with 204.
	delCode, delBody := doReq(http.MethodDelete, "/comics/api/v1/libraries/lib-main/roots/"+emptyRoot.ID, "", func(r *http.Request) {
		r.AddCookie(sessCookie)
		r.Header.Set("X-CSRF-Token", csrfToken)
	})
	if delCode != http.StatusNoContent || delBody != "" {
		t.Fatalf("DELETE emptyRoot = (%d, %q), want (204, empty)", delCode, delBody)
	}
	if _, err := postgres.NewLibraryRepository(db.Pool).GetLibraryRootByID(ctx, emptyRoot.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected emptyRoot deleted from DB, got %v", err)
	}
	_, finalRoots := countLibraryAndRootRows(ctx, t, db)
	if finalRoots != beforeRoots-1 {
		t.Fatalf("final roots = %d, want %d", finalRoots, beforeRoots-1)
	}

	// 6. Removing the sole empty root of a library returns 409 Conflict and preserves the root.
	soleDir := filepath.Join(canonBase, "sole-empty-root")
	if err := os.MkdirAll(soleDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(soleDir): %v", err)
	}
	createSoleCode, createSoleBody := doReq(http.MethodPost, "/api/v1/libraries", `{"name":"Sole Root Lib","root_path":"`+soleDir+`"}`, func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+libAdminPAT)
	})
	if createSoleCode != http.StatusCreated {
		t.Fatalf("POST /libraries (Sole Root Lib) = %d (%s), want 201", createSoleCode, createSoleBody)
	}
	var soleLib app.Library
	mustUnmarshal(t, createSoleBody, &soleLib)
	if len(soleLib.Roots) != 1 {
		t.Fatalf("expected 1 root on Sole Root Lib, got %+v", soleLib.Roots)
	}
	soleDelCode, soleDelBody := doReq(http.MethodDelete, "/comics/api/v1/libraries/"+soleLib.ID+"/roots/"+soleLib.Roots[0].ID, "", func(r *http.Request) {
		r.AddCookie(sessCookie)
		r.Header.Set("X-CSRF-Token", csrfToken)
	})
	if soleDelCode != http.StatusConflict {
		t.Fatalf("DELETE sole empty root = %d (%s), want 409", soleDelCode, soleDelBody)
	}
	if _, err := postgres.NewLibraryRepository(db.Pool).GetLibraryRootByID(ctx, soleLib.Roots[0].ID); err != nil {
		t.Fatalf("sole root must remain persisted after 409: %v", err)
	}
}

func TestDaemonThumbnailRoutes(t *testing.T) {
	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("migrations.RunFS: %v", err)
	}

	coversDir := filepath.Join(t.TempDir(), "covers")
	rootDir := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir rootDir: %v", err)
	}

	makeCBZ := func(filename string, c color.RGBA) {
		var pngBuf bytes.Buffer
		img := image.NewRGBA(image.Rect(0, 0, 80, 120))
		for y := range 120 {
			for x := range 80 {
				img.SetRGBA(x, y, c)
			}
		}
		if err := png.Encode(&pngBuf, img); err != nil {
			t.Fatalf("png.Encode: %v", err)
		}
		var zipBuf bytes.Buffer
		zw := zip.NewWriter(&zipBuf)
		w, err := zw.Create("001.png")
		if err != nil {
			t.Fatalf("zip.Create: %v", err)
		}
		if _, err := w.Write(pngBuf.Bytes()); err != nil {
			t.Fatalf("zip.Write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("zip.Close: %v", err)
		}
		if err := os.WriteFile(filepath.Join(rootDir, filename), zipBuf.Bytes(), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", filename, err)
		}
	}

	makeCBZ("alpha-1-nsfw.cbz", color.RGBA{R: 30, G: 40, B: 220, A: 255})  // blue
	makeCBZ("alpha-1-clean.cbz", color.RGBA{R: 220, G: 30, B: 30, A: 255}) // red
	makeCBZ("alpha-2-clean.cbz", color.RGBA{R: 30, G: 210, B: 30, A: 255}) // green
	makeCBZ("kids-1.cbz", color.RGBA{R: 220, G: 210, B: 30, A: 255})       // yellow
	makeCBZ("trashed.cbz", color.RGBA{R: 180, G: 40, B: 180, A: 255})

	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_BASE_URL=http://localhost:8080/comics",
		"TOBITEN_COVERS_DIR=" + coversDir,
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

	daemonSvcs, err := newDaemonServices(db.Pool, cfg, setup, userRepo)
	if err != nil {
		t.Fatalf("newDaemonServices: %v", err)
	}
	if daemonSvcs.app == nil || daemonSvcs.app.Curation == nil {
		t.Fatal("expected newDaemonServices to wire CurationService through NewAuthorizingServices")
	}
	if err := daemonSvcs.startBackground(ctx); err != nil {
		t.Fatalf("startBackground: %v", err)
	}
	defer daemonSvcs.stopBackground()

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

	admin := createTestUserWithPassword(ctx, t, userRepo, "thumbadmin", "admin-pass-123", domain.RoleAdminID)
	reader := createTestUserWithPassword(ctx, t, userRepo, "thumbreader", "reader-pass-123", domain.RoleReaderID)
	kid := createTestUserWithPassword(ctx, t, userRepo, "thumbkid", "kid-pass-123", domain.RoleRestrictedReaderID)
	_ = createTestUserWithPassword(ctx, t, userRepo, "thumbblank", "blank-pass-123", domain.RoleRestrictedReaderID)
	noReadUser := createTestUserWithPassword(ctx, t, userRepo, "thumbnoread", "noread-pass-123", "")
	if _, err := db.Exec(ctx, `INSERT INTO roles (id, name, built_in, requires_allow_labels) VALUES ('role-thumb-noread', 'NoRead', false, false)`); err != nil {
		t.Fatalf("insert role-thumb-noread: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO role_permissions (role_id, permission) VALUES ('role-thumb-noread', $1)`, string(domain.PermissionJobsReadAll)); err != nil {
		t.Fatalf("insert role_permissions: %v", err)
	}
	if err := userRepo.AssignUserRole(ctx, noReadUser.ID, "role-thumb-noread"); err != nil {
		t.Fatalf("AssignUserRole: %v", err)
	}

	adminReadPAT := createTestPAT(ctx, t, userRepo, admin.ID, []domain.PATScope{domain.ScopeRead})

	tx := postgres.NewTransactor(db.Pool)
	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		if err := st.Libraries().CreateLibrary(ctx, &domain.Library{
			ID:            "lib-thumb",
			Name:          "Thumb Lib",
			Mode:          domain.LibraryModeVirtual,
			ResolverChain: domain.DefaultResolverChain(),
		}); err != nil {
			return err
		}
		return st.Libraries().CreateLibraryRoot(ctx, &domain.LibraryRoot{
			ID:        "root-thumb",
			LibraryID: "lib-thumb",
			Path:      domain.NewPathIdentity([]byte(rootDir)),
			State:     domain.LibraryRootStateOnline,
		})
	}); err != nil {
		t.Fatalf("create library/root: %v", err)
	}

	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(ctx, "lib-thumb", scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	byName := make(map[string]domain.MediaAsset)
	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		list, listErr := st.CatalogueWrites().ListAssetsByRoot(ctx, "root-thumb")
		if listErr != nil {
			return listErr
		}
		for _, a := range list {
			byName[a.RelPathDisplay] = a
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		cw := st.CatalogueWrites()
		for _, s := range []domain.Series{
			{ID: "ser-thumb-alpha", LibraryID: "lib-thumb", Title: "Alpha", TitleSort: "alpha", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-thumb-kids", LibraryID: "lib-thumb", Title: "Kids", TitleSort: "kids", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-thumb-trashed", LibraryID: "lib-thumb", Title: "Trashed", TitleSort: "trashed", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
		} {
			ser := s
			if err := cw.CreateSeries(ctx, &ser); err != nil {
				return err
			}
		}

		prefNSFW := byName["alpha-1-nsfw.cbz"].ID
		for _, v := range []domain.Volume{
			{ID: "vol-thumb-multi", LibraryID: "lib-thumb", Kind: domain.VolumeKindVolume, PreferredAssetID: &prefNSFW, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-thumb-restr", LibraryID: "lib-thumb", Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-thumb-clean2", LibraryID: "lib-thumb", Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-thumb-kids", LibraryID: "lib-thumb", Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-thumb-trashed", LibraryID: "lib-thumb", Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
		} {
			vol := v
			if err := cw.CreateVolume(ctx, &vol); err != nil {
				return err
			}
		}

		sm := st.SeriesMemberships()
		p1, p2 := 1.0, 2.0
		for _, m := range []struct {
			volID string
			list  []domain.SeriesMembership
		}{
			{"vol-thumb-restr", []domain.SeriesMembership{{VolumeID: "vol-thumb-restr", SeriesID: "ser-thumb-alpha", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual}}},
			{"vol-thumb-clean2", []domain.SeriesMembership{{VolumeID: "vol-thumb-clean2", SeriesID: "ser-thumb-alpha", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p2, Source: domain.MembershipSourceManual}}},
			{"vol-thumb-kids", []domain.SeriesMembership{{VolumeID: "vol-thumb-kids", SeriesID: "ser-thumb-kids", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual}}},
			{"vol-thumb-trashed", []domain.SeriesMembership{{VolumeID: "vol-thumb-trashed", SeriesID: "ser-thumb-trashed", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual}}},
		} {
			if err := sm.SetVolumeMemberships(ctx, m.volID, m.list); err != nil {
				return err
			}
		}

		moveAsset := func(name, volID string) error {
			a, getErr := cw.GetAssetByIDForUpdate(ctx, byName[name].ID)
			if getErr != nil {
				return getErr
			}
			a.VolumeID = volID
			return cw.UpdateAsset(ctx, a)
		}
		if err := moveAsset("alpha-1-nsfw.cbz", "vol-thumb-multi"); err != nil {
			return err
		}
		if err := moveAsset("alpha-1-clean.cbz", "vol-thumb-multi"); err != nil {
			return err
		}
		if err := moveAsset("alpha-2-clean.cbz", "vol-thumb-clean2"); err != nil {
			return err
		}
		if err := moveAsset("kids-1.cbz", "vol-thumb-kids"); err != nil {
			return err
		}
		if err := moveAsset("trashed.cbz", "vol-thumb-trashed"); err != nil {
			return err
		}

		// Re-point vol-thumb-restr to a copy of alpha-1-nsfw.cbz so its sole asset is restricted.
		nsfwAsset := byName["alpha-1-nsfw.cbz"]
		nsfwDigest, _ := nsfwAsset.CurrentDigest(domain.DigestKindXXH364Partial)
		origBytes, readErr := os.ReadFile(filepath.Join(rootDir, "alpha-1-nsfw.cbz"))
		if readErr != nil {
			return readErr
		}
		copyName := "alpha-1-restr-only.cbz"
		if writeErr := os.WriteFile(filepath.Join(rootDir, copyName), origBytes, 0o644); writeErr != nil {
			return writeErr
		}
		restrOnly := domain.MediaAsset{
			ID:             "a-thumb-restr-only",
			VolumeID:       "vol-thumb-restr",
			LibraryRootID:  "root-thumb",
			RelPathBytes:   []byte(copyName),
			RelPathDisplay: copyName,
			RelPathMatch:   copyName,
			FileSize:       int64(len(origBytes)),
			Format:         domain.MediaFormatCBZ,
			PageCount:      1,
			CoverPageIndex: 0,
			Digests: []domain.AssetDigest{{
				AssetID:   "a-thumb-restr-only",
				Kind:      domain.DigestKindXXH364Partial,
				Value:     nsfwDigest.Value,
				IsCurrent: true,
				Reason:    domain.DigestReasonScan,
			}},
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := cw.CreateAsset(ctx, &restrOnly); err != nil {
			return err
		}

		labels := st.Labels()
		if err := labels.EnsureBuiltInServerLabels(ctx); err != nil {
			return err
		}
		for _, l := range []domain.Label{
			{ID: "lbl-thumb-nsfw", Name: "NSFW", Scope: domain.LabelScopeServer},
			{ID: "lbl-thumb-kids", Name: "Kids", Scope: domain.LabelScopeServer},
		} {
			lbl := l
			if err := labels.CreateLabel(ctx, &lbl); err != nil {
				return err
			}
		}
		if err := labels.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{UserID: reader.ID, LabelID: "lbl-thumb-nsfw", Effect: domain.PolicyEffectRestrict}); err != nil {
			return err
		}
		if err := labels.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{UserID: kid.ID, LabelID: "lbl-thumb-kids", Effect: domain.PolicyEffectAllow}); err != nil {
			return err
		}
		for _, il := range []domain.ItemLabel{
			{LabelID: "lbl-thumb-nsfw", ItemType: domain.ItemTypeAsset, ItemID: byName["alpha-1-nsfw.cbz"].ID},
			{LabelID: "lbl-thumb-nsfw", ItemType: domain.ItemTypeAsset, ItemID: "a-thumb-restr-only"},
			{LabelID: "lbl-thumb-kids", ItemType: domain.ItemTypeSeries, ItemID: "ser-thumb-kids"},
			{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeVolume, ItemID: "vol-thumb-trashed"},
			{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeSeries, ItemID: "ser-thumb-trashed"},
		} {
			item := il
			if err := labels.AssignItemLabel(ctx, &item); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed thumbnail HTTP fixture: %v", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	doThumbReq := func(method, path, user, pass, bearer, ifNoneMatch string) (*http.Response, []byte) {
		t.Helper()
		req, reqErr := http.NewRequest(method, ts.URL+path, nil)
		if reqErr != nil {
			t.Fatalf("NewRequest(%s %s): %v", method, path, reqErr)
		}
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		if ifNoneMatch != "" {
			req.Header.Set("If-None-Match", ifNoneMatch)
		}
		resp, doErr := client.Do(req)
		if doErr != nil {
			t.Fatalf("%s %s: %v", method, path, doErr)
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			t.Fatalf("read body %s %s: %v", method, path, readErr)
		}
		return resp, body
	}

	assertJPEGColor := func(t *testing.T, data []byte, wantR, wantG, wantB uint8) {
		t.Helper()
		decoded, decErr := jpeg.Decode(bytes.NewReader(data))
		if decErr != nil {
			t.Fatalf("jpeg.Decode: %v", decErr)
		}
		b := decoded.Bounds()
		r32, g32, b32, _ := decoded.At(b.Min.X+b.Dx()/2, b.Min.Y+b.Dy()/2).RGBA()
		gotR, gotG, gotB := uint8(r32>>8), uint8(g32>>8), uint8(b32>>8)
		diff := func(a, b uint8) int {
			if a > b {
				return int(a - b)
			}
			return int(b - a)
		}
		if diff(gotR, wantR) > 35 || diff(gotG, wantG) > 35 || diff(gotB, wantB) > 35 {
			t.Fatalf("decoded JPEG RGB=(%d,%d,%d), want ~(%d,%d,%d)", gotR, gotG, gotB, wantR, wantG, wantB)
		}
	}

	// 1. AuthN/AuthZ before serving or evaluating If-None-Match.
	for _, path := range []string{
		"/api/v1/volumes/vol-thumb-multi/thumbnail",
		"/comics/api/v1/series/ser-thumb-alpha/thumbnail",
	} {
		resp401, _ := doThumbReq(http.MethodGet, path, "", "", "", "*")
		if resp401.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthenticated %s = %d, want 401", path, resp401.StatusCode)
		}
		resp403, _ := doThumbReq(http.MethodGet, path, "thumbnoread", "noread-pass-123", "", "*")
		if resp403.StatusCode != http.StatusForbidden {
			t.Fatalf("no library:read %s = %d, want 403", path, resp403.StatusCode)
		}
	}

	// 2. Volume thumbnail: cross-principal selection, strong ETag, Cache-Control: private, no-cache, HEAD, and 304 revalidation.
	admVolResp, admVolBody := doThumbReq(http.MethodGet, "/api/v1/volumes/vol-thumb-multi/thumbnail", "", "", adminReadPAT, "")
	if admVolResp.StatusCode != http.StatusOK {
		t.Fatalf("admin GET vol-thumb-multi = %d (%s)", admVolResp.StatusCode, string(admVolBody))
	}
	if admVolResp.Header.Get("Content-Type") != "image/jpeg" || admVolResp.Header.Get("Cache-Control") != "private, no-cache" {
		t.Fatalf("admin vol headers: CT=%q CC=%q", admVolResp.Header.Get("Content-Type"), admVolResp.Header.Get("Cache-Control"))
	}
	admVolETag := admVolResp.Header.Get("ETag")
	if !strings.HasPrefix(admVolETag, `"`) || !strings.HasSuffix(admVolETag, `"`) || strings.HasPrefix(admVolETag, "W/") {
		t.Fatalf("expected strong quoted ETag, got %q", admVolETag)
	}
	assertJPEGColor(t, admVolBody, 30, 40, 220) // NSFW blue

	rdrVolResp, rdrVolBody := doThumbReq(http.MethodGet, "/comics/api/v1/volumes/vol-thumb-multi/thumbnail", "thumbreader", "reader-pass-123", "", admVolETag)
	if rdrVolResp.StatusCode != http.StatusOK {
		t.Fatalf("reader GET vol-thumb-multi with admin ETag = %d, want 200", rdrVolResp.StatusCode)
	}
	rdrVolETag := rdrVolResp.Header.Get("ETag")
	if rdrVolETag == "" || rdrVolETag == admVolETag || strings.HasPrefix(rdrVolETag, "W/") {
		t.Fatalf("expected distinct strong reader ETag, got %q vs admin %q", rdrVolETag, admVolETag)
	}
	assertJPEGColor(t, rdrVolBody, 220, 30, 30) // Clean red

	// Matching If-None-Match for reader -> 304 with private, no-cache and strong ETag.
	rdr304Resp, rdr304Body := doThumbReq(http.MethodGet, "/api/v1/volumes/vol-thumb-multi/thumbnail", "thumbreader", "reader-pass-123", "", rdrVolETag)
	if rdr304Resp.StatusCode != http.StatusNotModified || len(rdr304Body) != 0 {
		t.Fatalf("reader 304 check = (%d, len=%d), want (304, 0)", rdr304Resp.StatusCode, len(rdr304Body))
	}
	if rdr304Resp.Header.Get("Cache-Control") != "private, no-cache" || rdr304Resp.Header.Get("ETag") != rdrVolETag {
		t.Fatalf("reader 304 headers: CC=%q ETag=%q", rdr304Resp.Header.Get("Cache-Control"), rdr304Resp.Header.Get("ETag"))
	}

	// HEAD request -> 200 with JPEG content-type, ETag, and empty body.
	headResp, headBody := doThumbReq(http.MethodHead, "/api/v1/volumes/vol-thumb-multi/thumbnail", "thumbreader", "reader-pass-123", "", "")
	if headResp.StatusCode != http.StatusOK || len(headBody) != 0 || headResp.Header.Get("ETag") != rdrVolETag {
		t.Fatalf("HEAD vol-thumb-multi = (%d, len=%d, ETag=%q)", headResp.StatusCode, len(headBody), headResp.Header.Get("ETag"))
	}

	// 3. Series thumbnail FIRST selection & fallback: Admin gets blue vol 1; Reader falls back to green vol 2.
	admSerResp, admSerBody := doThumbReq(http.MethodGet, "/api/v1/series/ser-thumb-alpha/thumbnail", "thumbadmin", "admin-pass-123", "", "")
	if admSerResp.StatusCode != http.StatusOK {
		t.Fatalf("admin GET ser-thumb-alpha = %d", admSerResp.StatusCode)
	}
	assertJPEGColor(t, admSerBody, 30, 40, 220)
	admSerETag := admSerResp.Header.Get("ETag")

	rdrSerResp, rdrSerBody := doThumbReq(http.MethodGet, "/api/v1/series/ser-thumb-alpha/thumbnail", "thumbreader", "reader-pass-123", "", admSerETag)
	if rdrSerResp.StatusCode != http.StatusOK {
		t.Fatalf("reader GET ser-thumb-alpha with admin ETag = %d, want 200", rdrSerResp.StatusCode)
	}
	assertJPEGColor(t, rdrSerBody, 30, 210, 30)
	rdrSerETag := rdrSerResp.Header.Get("ETag")
	if rdrSerETag == "" || rdrSerETag == admSerETag {
		t.Fatalf("expected distinct series ETags: reader=%q admin=%q", rdrSerETag, admSerETag)
	}
	rdrSer304, _ := doThumbReq(http.MethodGet, "/comics/api/v1/series/ser-thumb-alpha/thumbnail", "thumbreader", "reader-pass-123", "", rdrSerETag)
	if rdrSer304.StatusCode != http.StatusNotModified {
		t.Fatalf("reader series 304 = %d, want 304", rdrSer304.StatusCode)
	}

	// Restricted reader ALLOW kids sees ser-thumb-kids (yellow), gets 404 on unallowed series/volumes even with valid ETag.
	kidResp, kidBody := doThumbReq(http.MethodGet, "/api/v1/series/ser-thumb-kids/thumbnail", "thumbkid", "kid-pass-123", "", "")
	if kidResp.StatusCode != http.StatusOK {
		t.Fatalf("kid GET ser-thumb-kids = %d", kidResp.StatusCode)
	}
	assertJPEGColor(t, kidBody, 220, 210, 30)

	// 4. 404 for restricted, trashed, unknown, and wrong-kind targets even when If-None-Match is set.
	for _, tc := range []struct {
		name        string
		path        string
		user        string
		pass        string
		ifNoneMatch string
	}{
		{"unknown volume", "/api/v1/volumes/vol-missing/thumbnail", "thumbadmin", "admin-pass-123", "*"},
		{"unknown series", "/api/v1/series/ser-missing/thumbnail", "thumbadmin", "admin-pass-123", "*"},
		{"series id on volume route", "/api/v1/volumes/ser-thumb-alpha/thumbnail", "thumbadmin", "admin-pass-123", "*"},
		{"volume id on series route", "/api/v1/series/vol-thumb-multi/thumbnail", "thumbadmin", "admin-pass-123", "*"},
		{"admin trashed volume", "/api/v1/volumes/vol-thumb-trashed/thumbnail", "thumbadmin", "admin-pass-123", "*"},
		{"admin trashed series", "/api/v1/series/ser-thumb-trashed/thumbnail", "thumbadmin", "admin-pass-123", "*"},
		{"reader only-restricted volume", "/api/v1/volumes/vol-thumb-restr/thumbnail", "thumbreader", "reader-pass-123", admSerETag},
		{"kid unallowed series with admin ETag", "/api/v1/series/ser-thumb-alpha/thumbnail", "thumbkid", "kid-pass-123", admSerETag},
		{"kid unallowed volume with reader ETag", "/api/v1/volumes/vol-thumb-multi/thumbnail", "thumbkid", "kid-pass-123", rdrVolETag},
		{"blank kid zero ALLOW", "/api/v1/series/ser-thumb-kids/thumbnail", "thumbblank", "blank-pass-123", "*"},
	} {
		resp, _ := doThumbReq(http.MethodGet, tc.path, tc.user, tc.pass, "", tc.ifNoneMatch)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: GET %s = %d, want 404", tc.name, tc.path, resp.StatusCode)
		}
		if resp.Header.Get("ETag") != "" {
			t.Fatalf("%s: 404 leaked ETag %q", tc.name, resp.Header.Get("ETag"))
		}
	}

	// 5. After trashing vol-thumb-clean2, revalidating with the previously valid ETag must return 404 (not 304).
	if _, err := db.Exec(ctx, `INSERT INTO item_labels (label_id, item_type, item_id) VALUES ($1, 'VOLUME', 'vol-thumb-clean2')`, domain.LabelMarkedForDeletionID); err != nil {
		t.Fatalf("trash vol-thumb-clean2: %v", err)
	}
	afterTrashResp, _ := doThumbReq(http.MethodGet, "/api/v1/series/ser-thumb-alpha/thumbnail", "thumbreader", "reader-pass-123", "", rdrSerETag)
	if afterTrashResp.StatusCode != http.StatusNotFound {
		t.Fatalf("after trashing sole visible volume, series revalidation = %d, want 404", afterTrashResp.StatusCode)
	}
}
