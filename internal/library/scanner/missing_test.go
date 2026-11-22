package scanner_test

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

type fakeScannerClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(initial time.Time) *fakeScannerClock {
	return &fakeScannerClock{now: initial.UTC()}
}

func (c *fakeScannerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeScannerClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

func (c *fakeScannerClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.UTC()
}

func listOutboxEventRows(ctx context.Context, t *testing.T, db *testdb.TestDB) []domain.DomainEvent {
	t.Helper()
	rows, err := db.Query(ctx, `
		SELECT id, kind, version, aggregate_type, aggregate_id, payload, created_at
		FROM domain_events
		ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("query domain_events: %v", err)
	}
	defer rows.Close()
	var out []domain.DomainEvent
	for rows.Next() {
		var ev domain.DomainEvent
		if err := rows.Scan(&ev.ID, &ev.Kind, &ev.Version, &ev.AggregateType, &ev.AggregateID, &ev.Payload, &ev.CreatedAt); err != nil {
			t.Fatalf("scan domain_event: %v", err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate domain_events: %v", err)
	}
	return out
}

func setSystemSetting(ctx context.Context, t *testing.T, transactor *postgres.Transactor, key, jsonLiteral string) {
	t.Helper()
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.Settings().SetSetting(ctx, &domain.SystemSetting{
			Key:   key,
			Value: []byte(jsonLiteral),
		})
	}); err != nil {
		t.Fatalf("SetSetting(%s=%s): %v", key, jsonLiteral, err)
	}
}

func assertAvailableReadsExcludeAsset(
	ctx context.Context,
	t *testing.T,
	catRepo *postgres.CatalogueRepository,
	scope storage.CatalogueReadScope,
	assetID, volumeID, libraryID string,
) {
	t.Helper()
	if _, err := catRepo.GetAssetByID(ctx, scope, assetID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetAssetByID(%s) = %v, want ErrNotFound", assetID, err)
	}
	if _, err := catRepo.GetAuthorizedAssetFile(ctx, scope, assetID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetAuthorizedAssetFile(%s) = %v, want ErrNotFound", assetID, err)
	}
	if _, err := catRepo.SelectPreferredAssetFileForVolume(ctx, scope, volumeID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("SelectPreferredAssetFileForVolume(%s) = %v, want ErrNotFound", volumeID, err)
	}
	listAssets, err := catRepo.ListAssetsByVolume(ctx, scope, volumeID)
	if err != nil || len(listAssets) != 0 {
		t.Fatalf("ListAssetsByVolume(%s) = (%+v, %v), want empty", volumeID, listAssets, err)
	}
	cnt, err := catRepo.CountAssetsByVolume(ctx, scope, volumeID)
	if err != nil || cnt != 0 {
		t.Fatalf("CountAssetsByVolume(%s) = (%d, %v), want 0", volumeID, cnt, err)
	}
	vol, err := catRepo.GetVolumeByID(ctx, scope, volumeID)
	if err != nil {
		t.Fatalf("GetVolumeByID(%s): %v", volumeID, err)
	}
	if !vol.IsMissing() || vol.IsOwned() || len(vol.Assets) != 0 || vol.PreferredAssetID != nil {
		t.Fatalf("expected retained missing volume with zero available assets, got %+v", vol)
	}
	vols, err := catRepo.ListVolumes(ctx, scope, storage.VolumeReadFilter{LibraryID: libraryID})
	if err != nil {
		t.Fatalf("ListVolumes(%s): %v", libraryID, err)
	}
	var foundVol bool
	for _, v := range vols {
		if v.ID == volumeID {
			foundVol = true
			if !v.IsMissing() || len(v.Assets) != 0 || v.PreferredAssetID != nil {
				t.Fatalf("ListVolumes volume unexpected state: %+v", v)
			}
		}
	}
	if !foundVol {
		t.Fatalf("volume %s absent from ListVolumes", volumeID)
	}
}

func assertAvailableReadsIncludeAsset(
	ctx context.Context,
	t *testing.T,
	catRepo *postgres.CatalogueRepository,
	scope storage.CatalogueReadScope,
	assetID, volumeID string,
) {
	t.Helper()
	gotAsset, err := catRepo.GetAssetByID(ctx, scope, assetID)
	if err != nil || gotAsset.ID != assetID || !gotAsset.IsAvailable() {
		t.Fatalf("GetAssetByID(%s) = (%+v, %v)", assetID, gotAsset, err)
	}
	gotFile, err := catRepo.GetAuthorizedAssetFile(ctx, scope, assetID)
	if err != nil || gotFile.AssetID != assetID {
		t.Fatalf("GetAuthorizedAssetFile(%s) = (%+v, %v)", assetID, gotFile, err)
	}
	pref, err := catRepo.SelectPreferredAssetFileForVolume(ctx, scope, volumeID)
	if err != nil || pref.AssetID != assetID {
		t.Fatalf("SelectPreferredAssetFileForVolume(%s) = (%+v, %v)", volumeID, pref, err)
	}
	vol, err := catRepo.GetVolumeByID(ctx, scope, volumeID)
	if err != nil || vol.IsMissing() || !vol.IsOwned() || len(vol.Assets) != 1 || vol.Assets[0].ID != assetID {
		t.Fatalf("GetVolumeByID(%s) = (%+v, %v)", volumeID, vol, err)
	}
}

func TestMissing_SoftDeleteRetainsCatalogueAndExcludesAvailableReads(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	filePath := filepath.Join(rootDir, "Chronicle/v01.cbz")
	cbzBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Missing Saga</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("page-1"), method: zip.Store},
	)
	writeDiskFile(t, filePath, cbzBytes)

	var lib domain.Library
	var root domain.LibraryRoot
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "SoftDelete Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	t0 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(t0)
	sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}

	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 1 {
		t.Fatalf("initial scan: res=%+v err=%v", res, err)
	}
	assets := listAssetsForRoot(ctx, t, transactor, root.ID)
	if len(assets) != 1 {
		t.Fatalf("expected 1 asset, got %d", len(assets))
	}
	orig := assets[0]

	userID := seedUserAndReadingProgress(ctx, t, db, transactor, "missing-reader", orig, 1)
	if _, err := db.Exec(ctx, `UPDATE volumes SET preferred_asset_id = $1 WHERE id = $2`, orig.ID, orig.VolumeID); err != nil {
		t.Fatalf("set preferred_asset_id: %v", err)
	}
	// Seed a SERVER label on the asset and a USER label on the volume to verify label retention.
	var serverLblID string
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lbl := domain.Label{ID: domain.MustNewID(), Scope: domain.LabelScopeServer, Name: "custom-tag"}
		if err := tx.Labels().CreateLabel(ctx, &lbl); err != nil {
			return err
		}
		serverLblID = lbl.ID
		if err := tx.Labels().AssignItemLabel(ctx, &domain.ItemLabel{
			LabelID:  lbl.ID,
			ItemType: domain.ItemTypeAsset,
			ItemID:   orig.ID,
		}); err != nil {
			return err
		}
		fav, err := tx.Labels().GetUserLabelByName(ctx, userID, domain.LabelFavorite)
		if err != nil {
			return err
		}
		return tx.Labels().AssignItemLabel(ctx, &domain.ItemLabel{
			LabelID:  fav.ID,
			ItemType: domain.ItemTypeVolume,
			ItemID:   orig.VolumeID,
		})
	}); err != nil {
		t.Fatalf("seed labels: %v", err)
	}

	// Keep a sibling file so the root stays non-empty and ONLINE.
	keepBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Missing Saga</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("page-keep"), method: zip.Store},
	)
	writeDiskFile(t, filepath.Join(rootDir, "Chronicle/v02.cbz"), keepBytes)
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 1 {
		t.Fatalf("second scan: res=%+v err=%v", res, err)
	}

	// Unlink v01.cbz and scan at tMissing.
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	tMissing := clock.Advance(2 * time.Hour)
	resMiss, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("missing scan: %v", err)
	}
	if !resMiss.Complete || !resMiss.ApplyRemovals || resMiss.MissingCount != 1 || resMiss.RetiredCount != 0 {
		t.Fatalf("unexpected missing scan result: %+v", resMiss)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	gotMissing, err := cw.GetAssetByID(ctx, orig.ID)
	if err != nil {
		t.Fatalf("CatalogueWrites.GetAssetByID: %v", err)
	}
	if !gotMissing.IsMissing() || gotMissing.IsRetired() || !gotMissing.MissingSince.Equal(tMissing) {
		t.Fatalf("unexpected MissingSince/RetiredAt: %+v (want MissingSince=%v)", gotMissing, tMissing)
	}
	if cur, ok := gotMissing.CurrentDigest(domain.DigestKindXXH364Partial); !ok || cur.Value == "" {
		t.Fatalf("expected digests retained on missing asset, got %+v", gotMissing.Digests)
	}

	// Available-asset reads exclude orig while retaining volume, series, labels, and reading_progress.
	catRepo := postgres.NewCatalogueRepository(db.Pool)
	adminScope := storage.CatalogueReadScope{Principal: adminPrincipalForReconcileTest(), IncludeTrashed: true}
	assertAvailableReadsExcludeAsset(ctx, t, catRepo, adminScope, orig.ID, orig.VolumeID, lib.ID)
	assertReadingProgressPreserved(ctx, t, db, userID, orig.VolumeID, orig.ID, 1)

	var labelCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM item_labels WHERE (item_type = 'ASSET' AND item_id = $1 AND label_id = $2) OR (item_type = 'VOLUME' AND item_id = $3)`,
		orig.ID, serverLblID, orig.VolumeID).Scan(&labelCount); err != nil || labelCount != 2 {
		t.Fatalf("expected 2 retained item_labels, got %d (err=%v)", labelCount, err)
	}

	// Verify asset.missing outbox event payload.
	events := listOutboxEventRows(ctx, t, db)
	var missingEvents []domain.DomainEvent
	for _, ev := range events {
		if ev.Kind == domain.EventAssetMissing && ev.AggregateID == orig.ID {
			missingEvents = append(missingEvents, ev)
		}
	}
	if len(missingEvents) != 1 || missingEvents[0].AggregateType != domain.AggregateTypeAsset {
		t.Fatalf("expected 1 asset.missing event for %s, got %+v", orig.ID, missingEvents)
	}
	payload, err := domain.DecodeEventPayload[domain.AssetEventPayloadV1](missingEvents[0])
	if err != nil || payload.AssetID != orig.ID || payload.VolumeID != orig.VolumeID || payload.LibraryID != lib.ID || payload.PathDisplay != "Chronicle/v01.cbz" {
		t.Fatalf("unexpected asset.missing payload: %+v (err=%v)", payload, err)
	}

	// Rescan within grace: MissingSince must stay tMissing and no duplicate asset.missing event.
	clock.Advance(24 * time.Hour)
	resStillMissing, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil || !resStillMissing.Complete || resStillMissing.MissingCount != 0 || resStillMissing.RetiredCount != 0 {
		t.Fatalf("unexpected within-grace rescan: res=%+v err=%v", resStillMissing, err)
	}
	afterWait, err := cw.GetAssetByID(ctx, orig.ID)
	if err != nil || afterWait.MissingSince == nil || !afterWait.MissingSince.Equal(tMissing) {
		t.Fatalf("MissingSince altered on within-grace rescan: %+v (err=%v)", afterWait, err)
	}
	events2 := listOutboxEventRows(ctx, t, db)
	var missingCount2 int
	for _, ev := range events2 {
		if ev.Kind == domain.EventAssetMissing && ev.AggregateID == orig.ID {
			missingCount2++
		}
	}
	if missingCount2 != 1 {
		t.Fatalf("expected still 1 asset.missing event within grace, got %d", missingCount2)
	}
}

func TestMissing_OfflineOrIncompleteWalkMarksNothingMissing(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	root1Dir := filepath.Join(base, "root-1")
	root2Dir := filepath.Join(base, "root-2")
	file1 := filepath.Join(root1Dir, "book1.cbz")
	file2 := filepath.Join(root2Dir, "sub/book2.cbz")

	writeDiskFile(t, file1, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Safe</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
	))
	writeDiskFile(t, file2, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Safe</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("p2"), method: zip.Store},
	))

	var lib domain.Library
	var r1, r2 domain.LibraryRoot
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Safety Lib", nil, []byte(root1Dir), []byte(root2Dir))
		r1, r2 = roots[0], roots[1]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	t0 := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(t0)
	sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 2 {
		t.Fatalf("initial scan: res=%+v err=%v", res, err)
	}
	a1 := listAssetsForRoot(ctx, t, transactor, r1.ID)[0]
	a2 := listAssetsForRoot(ctx, t, transactor, r2.ID)[0]

	// 1. Delete file1 on root1, but make root2 empty (OFFLINE) -> pre-check stops; a1 stays available.
	if err := os.Remove(file1); err != nil {
		t.Fatalf("remove file1: %v", err)
	}
	if err := os.Remove(file2); err != nil {
		t.Fatalf("remove file2: %v", err)
	}
	resOff, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("offline scan returned err: %v", err)
	}
	if !resOff.Offline || resOff.Complete || resOff.ApplyRemovals {
		t.Fatalf("expected OFFLINE scan without removals, got %+v", resOff)
	}
	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	if got1, _ := cw.GetAssetByID(ctx, a1.ID); got1.MissingSince != nil {
		t.Fatalf("expected a1 not marked missing when library OFFLINE, got %+v", got1)
	}

	// 2. Restore file2 in an unreadable subdirectory -> root2 passes pre-check via marker,
	//    but walk reports incomplete on unreadable subtree -> incomplete walk marks nothing missing.
	writeDiskFile(t, filepath.Join(root2Dir, scanner.RootMarkerFilename), []byte{})
	writeDiskFile(t, file2, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Safe</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("p2"), method: zip.Store},
	))
	if a2.FileMtime != nil {
		if err := os.Chtimes(file2, *a2.FileMtime, *a2.FileMtime); err != nil {
			t.Fatalf("Chtimes(file2): %v", err)
		}
	}
	subDir := filepath.Join(root2Dir, "sub")

	// Also add a qualifying file in root1 so root1 stays ONLINE even though file1 is missing.
	otherFile := filepath.Join(root1Dir, "other.cbz")
	writeDiskFile(t, otherFile, buildDeterministicZip(t, "",
		zipEntry{name: "001.jpg", data: []byte("other"), method: zip.Store},
	))

	resInc, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
			if string(rawDir) == subDir {
				return nil, os.ErrPermission
			}
			return os.ReadDir(string(rawDir))
		},
	})
	if err != nil {
		t.Fatalf("unexpected error on incomplete subtree walk: %v", err)
	}
	if resInc.Complete || resInc.ApplyRemovals {
		t.Fatalf("expected incomplete walk with ApplyRemovals=false, got %+v", resInc)
	}
	if got1, _ := cw.GetAssetByID(ctx, a1.ID); got1.MissingSince != nil {
		t.Fatalf("expected a1 not marked missing on incomplete walk, got %+v", got1)
	}
	// Verify other.cbz was NOT indexed either because the walk was incomplete.
	if len(listAssetsForRoot(ctx, t, transactor, r1.ID)) != 1 {
		t.Fatalf("expected no new asset indexed on incomplete walk")
	}

	// 3. Stale root snapshot before executeMarkMissing -> ErrStalePreCheck rolls back cleanly.
	if err := os.Remove(otherFile); err != nil {
		t.Fatalf("remove otherFile: %v", err)
	}
	writeDiskFile(t, filepath.Join(root1Dir, scanner.RootMarkerFilename), []byte{})
	var staleOnce sync.Once
	hookTx := &reconcileHookTransactor{
		inner: transactor,
		beforeSecondReadCommitted: func() error {
			var hookErr error
			staleOnce.Do(func() {
				hookErr = transactor.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
					return tx.Libraries().UpdateLibraryRootStatus(ctx, r2, domain.LibraryRootStateOffline, clock.Now().Add(time.Second), nil)
				})
			})
			return hookErr
		},
	}
	scHook, err := scanner.NewScanner(hookTx, scanner.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("NewScanner(hook): %v", err)
	}
	resStale, err := scHook.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if !errors.Is(err, scanner.ErrStalePreCheck) {
		t.Fatalf("expected ErrStalePreCheck, got res=%+v err=%v", resStale, err)
	}
	if resStale.Complete || resStale.ApplyRemovals {
		t.Fatalf("expected Complete=false ApplyRemovals=false on ErrStalePreCheck, got %+v", resStale)
	}
	if got1, _ := cw.GetAssetByID(ctx, a1.ID); got1.MissingSince != nil {
		t.Fatalf("expected a1 MissingSince rolled back on ErrStalePreCheck, got %+v", got1)
	}
	for _, ev := range listOutboxEventRows(ctx, t, db) {
		if ev.Kind == domain.EventAssetMissing {
			t.Fatalf("unexpected asset.missing event after aborted/incomplete scans: %+v", ev)
		}
	}
}

func TestMissing_RevivalWithinGraceCurrentAndHistoricalBothPaths(t *testing.T) {
	t.Parallel()

	type revivalCase struct {
		name       string
		historical bool
		otherPath  bool
		crossRoot  bool
	}
	cases := []revivalCase{
		{name: "same path current digest within grace", historical: false, otherPath: false},
		{name: "another path across roots current digest within grace", historical: false, otherPath: true, crossRoot: true},
		{name: "same path historical digest within grace", historical: true, otherPath: false},
		{name: "another path historical digest within grace", historical: true, otherPath: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, transactor := setupIndexerTestDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			base := t.TempDir()
			root1Dir := filepath.Join(base, "r1")
			root2Dir := filepath.Join(base, "r2")
			writeDiskFile(t, filepath.Join(root1Dir, scanner.RootMarkerFilename), []byte{})
			writeDiskFile(t, filepath.Join(root2Dir, scanner.RootMarkerFilename), []byte{})

			origRel := "Series/v01.cbz"
			origPath := filepath.Join(root1Dir, origRel)
			v1Bytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Revive Saga</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("v1-p1"), method: zip.Store},
			)
			v2Bytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Revive Saga</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("v2-p1"), method: zip.Store},
				zipEntry{name: "002.jpg", data: []byte("v2-p2"), method: zip.Store},
			)
			writeDiskFile(t, origPath, v1Bytes)

			var lib domain.Library
			var r1, r2 domain.LibraryRoot
			if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
				var roots []domain.LibraryRoot
				lib, roots = seedLibraryAndRoots(ctx, t, tx, "Revive Lib", nil, []byte(root1Dir), []byte(root2Dir))
				r1, r2 = roots[0], roots[1]
				return nil
			}); err != nil {
				t.Fatalf("seed failed: %v", err)
			}

			t0 := time.Date(2026, 5, 10, 9, 0, 0, 0, time.UTC)
			clock := newFakeClock(t0)
			sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
			if err != nil {
				t.Fatalf("NewScanner: %v", err)
			}

			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 1 {
				t.Fatalf("initial scan: res=%+v err=%v", res, err)
			}
			orig := listAssetsForRoot(ctx, t, transactor, r1.ID)[0]

			if tc.historical {
				clock.Advance(time.Minute)
				writeDiskFile(t, origPath, v2Bytes)
				if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.ModifiedCount != 1 {
					t.Fatalf("modify scan: res=%+v err=%v", res, err)
				}
				cw := postgres.NewCatalogueWriteRepository(db.Pool)
				origPtr, err := cw.GetAssetByID(ctx, orig.ID)
				if err != nil {
					t.Fatalf("GetAssetByID after modify: %v", err)
				}
				orig = *origPtr
			}

			userID := seedUserAndReadingProgress(ctx, t, db, transactor, "revive-reader", orig, 1)

			// Remove file -> mark missing.
			clock.Advance(time.Hour)
			if err := os.Remove(origPath); err != nil {
				t.Fatalf("remove: %v", err)
			}
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.MissingCount != 1 {
				t.Fatalf("missing scan: res=%+v err=%v", res, err)
			}

			// Reappear within grace (3 days < 7 days).
			clock.Advance(3 * 24 * time.Hour)
			reappearBytes := v1Bytes
			targetRoot := r1.ID
			targetPath := origPath
			targetRel := origRel
			if tc.otherPath {
				targetRel = "Moved/v01-restored.cbz"
				if tc.crossRoot {
					targetRoot = r2.ID
					targetPath = filepath.Join(root2Dir, targetRel)
				} else {
					targetPath = filepath.Join(root1Dir, targetRel)
				}
			}
			writeDiskFile(t, targetPath, reappearBytes)

			resRev, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
			if err != nil {
				t.Fatalf("revival scan: %v", err)
			}
			if !resRev.Complete || resRev.RestoredCount != 1 || resRev.IndexedCount != 0 {
				t.Fatalf("unexpected revival scan result: %+v", resRev)
			}
			if tc.otherPath && resRev.MovedCount != 1 {
				t.Fatalf("expected MovedCount=1 on other-path revival, got %+v", resRev)
			}
			if !tc.otherPath && resRev.MovedCount != 0 {
				t.Fatalf("expected MovedCount=0 on same-path revival, got %+v", resRev)
			}

			cw := postgres.NewCatalogueWriteRepository(db.Pool)
			restored, err := cw.GetAssetByID(ctx, orig.ID)
			if err != nil {
				t.Fatalf("GetAssetByID(restored): %v", err)
			}
			if !restored.IsAvailable() || restored.MissingSince != nil || restored.RetiredAt != nil {
				t.Fatalf("expected cleared MissingSince/RetiredAt, got %+v", restored)
			}
			if restored.LibraryRootID != targetRoot || restored.RelPathDisplay != targetRel {
				t.Fatalf("unexpected location after revival: root=%s path=%s", restored.LibraryRootID, restored.RelPathDisplay)
			}
			if tc.historical {
				if restored.PageCount != 1 {
					t.Fatalf("expected PageCount=1 from restored v1 bytes, got %d", restored.PageCount)
				}
				if len(restored.Digests) != 2 {
					t.Fatalf("expected 2 digests retained (v1 current + v2 history), got %+v", restored.Digests)
				}
				vol, err := cw.GetVolumeByID(ctx, orig.VolumeID)
				if err != nil || vol.PageCount != 1 {
					t.Fatalf("expected unlocked volume PageCount=1 synced, got (%+v, %v)", vol, err)
				}
			}

			catRepo := postgres.NewCatalogueRepository(db.Pool)
			adminScope := storage.CatalogueReadScope{Principal: adminPrincipalForReconcileTest(), IncludeTrashed: true}
			assertAvailableReadsIncludeAsset(ctx, t, catRepo, adminScope, orig.ID, orig.VolumeID)
			assertReadingProgressPreserved(ctx, t, db, userID, orig.VolumeID, orig.ID, 1)

			var kinds []domain.EventKind
			for _, ev := range listOutboxEventRows(ctx, t, db) {
				if ev.AggregateID == orig.ID {
					kinds = append(kinds, ev.Kind)
				}
			}
			if !slices.Contains(kinds, domain.EventAssetRestored) {
				t.Fatalf("expected asset.restored event, got %v", kinds)
			}
			if tc.otherPath && !slices.Contains(kinds, domain.EventAssetMoved) {
				t.Fatalf("expected asset.moved event on other-path revival, got %v", kinds)
			}
			if !tc.otherPath && slices.Contains(kinds, domain.EventAssetMoved) {
				t.Fatalf("did not expect asset.moved event on same-path revival, got %v", kinds)
			}

			assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 1)
		})
	}
}

func TestMissing_ConfigurableGracePeriodAndRetirement(t *testing.T) {
	t.Parallel()

	t.Run("default 7-day grace period retires after 7 days and keeps volume", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		writeDiskFile(t, filepath.Join(rootDir, scanner.RootMarkerFilename), []byte{})
		filePath := filepath.Join(rootDir, "Vol1.cbz")
		writeDiskFile(t, filePath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Retire Saga</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
		))

		var lib domain.Library
		var root domain.LibraryRoot
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Default Grace Lib", nil, []byte(rootDir))
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		t0 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
		clock := newFakeClock(t0)
		sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 1 {
			t.Fatalf("initial scan: res=%+v err=%v", res, err)
		}
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		// Remove file at tMissing. Note: no user reading_progress seeded — volume still survives grace!
		tMissing := clock.Advance(time.Hour)
		if err := os.Remove(filePath); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.MissingCount != 1 {
			t.Fatalf("missing scan: res=%+v err=%v", res, err)
		}

		// Just before 7 days: NOT retired.
		clock.Set(tMissing.Add(7*24*time.Hour - time.Second))
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RetiredCount != 0 {
			t.Fatalf("before-grace scan: res=%+v err=%v", res, err)
		}
		cw := postgres.NewCatalogueWriteRepository(db.Pool)
		if got, _ := cw.GetAssetByID(ctx, orig.ID); got.RetiredAt != nil {
			t.Fatalf("expected RetiredAt nil before 7 days, got %+v", got)
		}

		// At exact 7 days: next scan sets retired_at!
		tRetired := clock.SetAndGet(tMissing.Add(7 * 24 * time.Hour))
		resRet, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil || !resRet.Complete || resRet.RetiredCount != 1 || resRet.MissingCount != 0 {
			t.Fatalf("retire scan: res=%+v err=%v", resRet, err)
		}
		gotRet, err := cw.GetAssetByID(ctx, orig.ID)
		if err != nil {
			t.Fatalf("GetAssetByID(retired): %v", err)
		}
		if !gotRet.IsMissing() || !gotRet.IsRetired() || !gotRet.MissingSince.Equal(tMissing) || !gotRet.RetiredAt.Equal(tRetired) {
			t.Fatalf("unexpected retired state: %+v (want MissingSince=%v RetiredAt=%v)", gotRet, tMissing, tRetired)
		}
		if len(gotRet.Digests) == 0 {
			t.Fatal("expected digests retained after retirement")
		}

		catRepo := postgres.NewCatalogueRepository(db.Pool)
		adminScope := storage.CatalogueReadScope{Principal: adminPrincipalForReconcileTest(), IncludeTrashed: true}
		assertAvailableReadsExcludeAsset(ctx, t, catRepo, adminScope, orig.ID, orig.VolumeID, lib.ID)

		// Retirement must NOT emit another asset.missing event.
		var missingEventCount int
		for _, ev := range listOutboxEventRows(ctx, t, db) {
			if ev.Kind == domain.EventAssetMissing && ev.AggregateID == orig.ID {
				missingEventCount++
			}
		}
		if missingEventCount != 1 {
			t.Fatalf("expected exactly 1 asset.missing event after retirement, got %d", missingEventCount)
		}

		// Subsequent scan while retired is a no-op.
		clock.Advance(24 * time.Hour)
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RetiredCount != 0 || res.MissingCount != 0 {
			t.Fatalf("repeat retired scan: res=%+v err=%v", res, err)
		}
	})

	t.Run("system_settings scanner.missing_grace_days and service option override", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		writeDiskFile(t, filepath.Join(rootDir, scanner.RootMarkerFilename), []byte{})
		filePath := filepath.Join(rootDir, "Vol1.cbz")
		writeDiskFile(t, filePath, buildDeterministicZip(t, "",
			zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
		))

		var lib domain.Library
		var root domain.LibraryRoot
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Configured Grace Lib", nil, []byte(rootDir))
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		setSystemSetting(ctx, t, transactor, scanner.MissingGraceDaysSettingKey, `3`)
		t0 := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
		clock := newFakeClock(t0)
		sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}

		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 1 {
			t.Fatalf("initial scan: res=%+v err=%v", res, err)
		}
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		tMissing := clock.Advance(time.Hour)
		if err := os.Remove(filePath); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.MissingCount != 1 {
			t.Fatalf("missing scan: res=%+v err=%v", res, err)
		}

		// At +2d23h (< 3 days), still in grace.
		clock.Set(tMissing.Add(3*24*time.Hour - time.Hour))
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RetiredCount != 0 {
			t.Fatalf("before 3-day grace scan: res=%+v err=%v", res, err)
		}

		// At +3d, retires!
		clock.Set(tMissing.Add(3 * 24 * time.Hour))
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RetiredCount != 1 {
			t.Fatalf("at 3-day grace scan: res=%+v err=%v", res, err)
		}

		// Restore and test dynamic update to "0" days + invalid setting rejection + Options override.
		writeDiskFile(t, filePath, buildDeterministicZip(t, "",
			zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
		))
		clock.Advance(time.Hour)
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RestoredCount != 1 {
			t.Fatalf("restore scan: res=%+v err=%v", res, err)
		}

		setSystemSetting(ctx, t, transactor, scanner.MissingGraceDaysSettingKey, `"0"`)
		if err := os.Remove(filePath); err != nil {
			t.Fatalf("remove 2: %v", err)
		}
		clock.Advance(time.Hour)
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.MissingCount != 1 || res.RetiredCount != 0 {
			t.Fatalf("zero-grace first scan must set missing only: res=%+v err=%v", res, err)
		}
		clock.Advance(time.Second)
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RetiredCount != 1 {
			t.Fatalf("zero-grace next scan must retire: res=%+v err=%v", res, err)
		}

		// Invalid setting returns ErrInvalidMissingGracePeriod.
		setSystemSetting(ctx, t, transactor, scanner.MissingGraceDaysSettingKey, `-5`)
		if _, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); !errors.Is(err, scanner.ErrInvalidMissingGracePeriod) {
			t.Fatalf("expected ErrInvalidMissingGracePeriod for -5, got %v", err)
		}

		// Negative service option rejected at construction; valid service option overrides invalid DB setting.
		neg := -time.Hour
		if _, err := scanner.NewScanner(transactor, scanner.Options{MissingGracePeriod: &neg}); !errors.Is(err, scanner.ErrInvalidMissingGracePeriod) {
			t.Fatalf("expected ErrInvalidMissingGracePeriod for negative option, got %v", err)
		}
		override := 2 * time.Hour
		scOverride, err := scanner.NewScanner(transactor, scanner.Options{
			Now:                clock.Now,
			MissingGracePeriod: &override,
		})
		if err != nil {
			t.Fatalf("NewScanner(override): %v", err)
		}
		// Revive asset first using scOverride (ignores invalid DB setting).
		writeDiskFile(t, filePath, buildDeterministicZip(t, "",
			zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
		))
		clock.Advance(time.Minute)
		if res, err := scOverride.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RestoredCount != 1 {
			t.Fatalf("override restore scan: res=%+v err=%v", res, err)
		}
		_ = db
		_ = orig
	})
}

func (c *fakeScannerClock) SetAndGet(t time.Time) time.Time {
	c.Set(t)
	return c.Now()
}

func TestMissing_RevivalAfterGraceClearsMissingAndRetired(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		historical bool
		otherPath  bool
	}{
		{name: "same path current digest after grace", historical: false, otherPath: false},
		{name: "another path current digest after grace", historical: false, otherPath: true},
		{name: "same path historical digest after grace", historical: true, otherPath: false},
		{name: "another path historical digest after grace", historical: true, otherPath: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, transactor := setupIndexerTestDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			rootDir := filepath.Join(t.TempDir(), "root")
			writeDiskFile(t, filepath.Join(rootDir, scanner.RootMarkerFilename), []byte{})
			origRel := "Old/vol1.cbz"
			origPath := filepath.Join(rootDir, origRel)

			v1Bytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>PostGrace</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("v1"), method: zip.Store},
			)
			v2Bytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>PostGrace</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("v2"), method: zip.Store},
				zipEntry{name: "002.jpg", data: []byte("v2-2"), method: zip.Store},
			)
			writeDiskFile(t, origPath, v1Bytes)

			var lib domain.Library
			var root domain.LibraryRoot
			if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
				var roots []domain.LibraryRoot
				lib, roots = seedLibraryAndRoots(ctx, t, tx, "PostGrace Lib", nil, []byte(rootDir))
				root = roots[0]
				return nil
			}); err != nil {
				t.Fatalf("seed failed: %v", err)
			}

			t0 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
			clock := newFakeClock(t0)
			sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
			if err != nil {
				t.Fatalf("NewScanner: %v", err)
			}
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 1 {
				t.Fatalf("initial scan: res=%+v err=%v", res, err)
			}
			orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

			if tc.historical {
				clock.Advance(time.Minute)
				writeDiskFile(t, origPath, v2Bytes)
				if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.ModifiedCount != 1 {
					t.Fatalf("modify scan: res=%+v err=%v", res, err)
				}
				cw := postgres.NewCatalogueWriteRepository(db.Pool)
				origPtr, _ := cw.GetAssetByID(ctx, orig.ID)
				orig = *origPtr
			}

			userID := seedUserAndReadingProgress(ctx, t, db, transactor, "postgrace-reader", orig, 1)

			// Missing -> advance 8 days -> retire.
			clock.Advance(time.Hour)
			if err := os.Remove(origPath); err != nil {
				t.Fatalf("remove: %v", err)
			}
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.MissingCount != 1 {
				t.Fatalf("missing scan: res=%+v err=%v", res, err)
			}
			clock.Advance(8 * 24 * time.Hour)
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RetiredCount != 1 {
				t.Fatalf("retire scan: res=%+v err=%v", res, err)
			}

			// Reappear after retirement.
			clock.Advance(24 * time.Hour)
			targetRel := origRel
			targetPath := origPath
			if tc.otherPath {
				targetRel = "Revived/vol1.cbz"
				targetPath = filepath.Join(rootDir, targetRel)
			}
			writeDiskFile(t, targetPath, v1Bytes)

			resRev, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
			if err != nil || !resRev.Complete || resRev.RestoredCount != 1 || resRev.IndexedCount != 0 {
				t.Fatalf("post-grace revival scan: res=%+v err=%v", resRev, err)
			}

			cw := postgres.NewCatalogueWriteRepository(db.Pool)
			got, err := cw.GetAssetByID(ctx, orig.ID)
			if err != nil {
				t.Fatalf("GetAssetByID: %v", err)
			}
			if !got.IsAvailable() || got.MissingSince != nil || got.RetiredAt != nil || got.VolumeID != orig.VolumeID || got.RelPathDisplay != targetRel {
				t.Fatalf("expected fully revived asset at %s, got %+v", targetRel, got)
			}
			assertReadingProgressPreserved(ctx, t, db, userID, orig.VolumeID, orig.ID, 1)

			catRepo := postgres.NewCatalogueRepository(db.Pool)
			adminScope := storage.CatalogueReadScope{Principal: adminPrincipalForReconcileTest(), IncludeTrashed: true}
			assertAvailableReadsIncludeAsset(ctx, t, catRepo, adminScope, orig.ID, orig.VolumeID)
			assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 1)
		})
	}
}

func TestMissing_DifferentFileAtMissingOrRetiredPathRecordsModification(t *testing.T) {
	t.Parallel()

	for _, afterGrace := range []bool{false, true} {
		name := "within grace"
		if afterGrace {
			name = "after grace (retired)"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, transactor := setupIndexerTestDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			rootDir := filepath.Join(t.TempDir(), "root")
			writeDiskFile(t, filepath.Join(rootDir, scanner.RootMarkerFilename), []byte{})
			relPath := "Series/vol1.cbz"
			filePath := filepath.Join(rootDir, relPath)

			v1Bytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Replace Saga</Series><Number>1</Number><Title>Old Title</Title><Summary>Old Summary</Summary></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("v1"), method: zip.Store},
			)
			v2Bytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Replace Saga</Series><Number>1</Number><Title>New Title</Title><Summary>New Summary</Summary></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("v2-1"), method: zip.Store},
				zipEntry{name: "002.jpg", data: []byte("v2-2"), method: zip.Store},
			)
			writeDiskFile(t, filePath, v1Bytes)

			var lib domain.Library
			var root domain.LibraryRoot
			if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
				var roots []domain.LibraryRoot
				lib, roots = seedLibraryAndRoots(ctx, t, tx, "ReplaceAtMissing Lib", nil, []byte(rootDir))
				root = roots[0]
				return nil
			}); err != nil {
				t.Fatalf("seed failed: %v", err)
			}

			t0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			clock := newFakeClock(t0)
			sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
			if err != nil {
				t.Fatalf("NewScanner: %v", err)
			}
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 1 {
				t.Fatalf("initial scan: res=%+v err=%v", res, err)
			}
			orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]
			origDigest, _ := orig.CurrentDigest(domain.DigestKindXXH364Partial)
			userID := seedUserAndReadingProgress(ctx, t, db, transactor, "replace-reader", orig, 1)

			// Lock volume title to verify external modification preserves locked fields.
			lockedJSON, err := json.Marshal(domain.FieldLocks{"title": true})
			if err != nil {
				t.Fatalf("marshal locks: %v", err)
			}
			if _, err := db.Exec(ctx, `UPDATE volumes SET title = 'Curator Locked Title', locked_fields = $1 WHERE id = $2`, lockedJSON, orig.VolumeID); err != nil {
				t.Fatalf("lock volume title: %v", err)
			}

			// Remove file -> missing (and retire if afterGrace).
			clock.Advance(time.Hour)
			if err := os.Remove(filePath); err != nil {
				t.Fatalf("remove: %v", err)
			}
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.MissingCount != 1 {
				t.Fatalf("missing scan: res=%+v err=%v", res, err)
			}
			if afterGrace {
				clock.Advance(8 * 24 * time.Hour)
				if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RetiredCount != 1 {
					t.Fatalf("retire scan: res=%+v err=%v", res, err)
				}
			}

			// Write a completely different file at the missing/retired asset's path.
			clock.Advance(time.Hour)
			writeDiskFile(t, filePath, v2Bytes)
			resMod, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
			if err != nil || !resMod.Complete || resMod.ModifiedCount != 1 || resMod.RestoredCount != 1 || resMod.IndexedCount != 0 {
				t.Fatalf("modify-at-missing scan: res=%+v err=%v", resMod, err)
			}

			cw := postgres.NewCatalogueWriteRepository(db.Pool)
			got, err := cw.GetAssetByID(ctx, orig.ID)
			if err != nil {
				t.Fatalf("GetAssetByID: %v", err)
			}
			if !got.IsAvailable() || got.MissingSince != nil || got.RetiredAt != nil || got.PageCount != 2 {
				t.Fatalf("expected available modified asset with 2 pages, got %+v", got)
			}
			newDigest, ok := got.CurrentDigest(domain.DigestKindXXH364Partial)
			if !ok || newDigest.Value == origDigest.Value || newDigest.Reason != domain.DigestReasonExternalModify || len(got.Digests) != 2 {
				t.Fatalf("unexpected digest history after modification: %+v", got.Digests)
			}

			vol, err := cw.GetVolumeByID(ctx, orig.VolumeID)
			if err != nil {
				t.Fatalf("GetVolumeByID: %v", err)
			}
			if vol.Title == nil || *vol.Title != "Curator Locked Title" || vol.Summary == nil || *vol.Summary != "New Summary" {
				t.Fatalf("unexpected volume metadata after modification: %+v", vol)
			}
			assertReadingProgressPreserved(ctx, t, db, userID, orig.VolumeID, orig.ID, 1)

			var kinds []domain.EventKind
			for _, ev := range listOutboxEventRows(ctx, t, db) {
				if ev.AggregateID == orig.ID {
					kinds = append(kinds, ev.Kind)
				}
			}
			if !slices.Contains(kinds, domain.EventAssetModified) || !slices.Contains(kinds, domain.EventAssetRestored) {
				t.Fatalf("expected asset.modified and asset.restored events, got %v", kinds)
			}

			catRepo := postgres.NewCatalogueRepository(db.Pool)
			adminScope := storage.CatalogueReadScope{Principal: adminPrincipalForReconcileTest(), IncludeTrashed: true}
			assertAvailableReadsIncludeAsset(ctx, t, catRepo, adminScope, orig.ID, orig.VolumeID)
			assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 1)
		})
	}
}

func TestMissing_SwapActiveOntoRevivedMissingPath(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	writeDiskFile(t, filepath.Join(rootDir, scanner.RootMarkerFilename), []byte{})
	pathA := filepath.Join(rootDir, "a.cbz")
	pathM := filepath.Join(rootDir, "m.cbz")
	bytesA := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Swap</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("bytes-a"), method: zip.Store},
	)
	bytesM := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Swap</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("bytes-m"), method: zip.Store},
	)
	writeDiskFile(t, pathA, bytesA)
	writeDiskFile(t, pathM, bytesM)

	var lib domain.Library
	var root domain.LibraryRoot
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "MissingSwap Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(t0)
	sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 2 {
		t.Fatalf("initial scan: res=%+v err=%v", res, err)
	}
	byPath := make(map[string]domain.MediaAsset)
	for _, a := range listAssetsForRoot(ctx, t, transactor, root.ID) {
		byPath[a.RelPathDisplay] = a
	}
	origA := byPath["a.cbz"]
	origM := byPath["m.cbz"]

	// Remove m.cbz -> M goes missing and retires.
	clock.Advance(time.Hour)
	if err := os.Remove(pathM); err != nil {
		t.Fatalf("remove m.cbz: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.MissingCount != 1 {
		t.Fatalf("missing scan: res=%+v err=%v", res, err)
	}
	clock.Advance(8 * 24 * time.Hour)
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.RetiredCount != 1 {
		t.Fatalf("retire scan: res=%+v err=%v", res, err)
	}

	// Swap: active A moves onto m.cbz while retired M revives at a.cbz.
	clock.Advance(time.Hour)
	if err := os.Remove(pathA); err != nil {
		t.Fatalf("remove a.cbz: %v", err)
	}
	writeDiskFile(t, pathM, bytesA)
	writeDiskFile(t, pathA, bytesM)

	resSwap, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil || !resSwap.Complete || resSwap.MovedCount != 2 || resSwap.RestoredCount != 1 || resSwap.IndexedCount != 0 {
		t.Fatalf("swap scan: res=%+v err=%v", resSwap, err)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	gotA, _ := cw.GetAssetByID(ctx, origA.ID)
	gotM, _ := cw.GetAssetByID(ctx, origM.ID)
	if gotA.RelPathDisplay != "m.cbz" || !gotA.IsAvailable() {
		t.Fatalf("expected origA moved to m.cbz and available, got %+v", gotA)
	}
	if gotM.RelPathDisplay != "a.cbz" || !gotM.IsAvailable() {
		t.Fatalf("expected origM revived at a.cbz and available, got %+v", gotM)
	}
}
