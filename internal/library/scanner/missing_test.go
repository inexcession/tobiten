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
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
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

func findAssetByRelPath(t *testing.T, assets []domain.MediaAsset, relPath string) domain.MediaAsset {
	t.Helper()
	for _, a := range assets {
		if a.RelPathDisplay == relPath {
			return a
		}
	}
	t.Fatalf("asset %q not found in %+v", relPath, assets)
	return domain.MediaAsset{}
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
	events := loadOutboxEventsDetailed(ctx, t, db)
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
	events2 := loadOutboxEventsDetailed(ctx, t, db)
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
	keep1 := filepath.Join(root1Dir, "keep1.cbz")
	file2 := filepath.Join(root2Dir, "sub/book2.cbz")

	writeDiskFile(t, file1, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Safe</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
	))
	writeDiskFile(t, keep1, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Safe</Series><Number>3</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("keep-1"), method: zip.Store},
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
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 3 {
		t.Fatalf("initial scan: res=%+v err=%v", res, err)
	}
	a1 := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, r1.ID), "book1.cbz")
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

	// 2. Restore file2, add other.cbz in root1, but fail root2 ReadDir after pre-check
	//    -> incomplete root walk marks nothing missing and defers all additions.
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

	// Also add a qualifying file in root1 so root1 stays ONLINE even though file1 is missing.
	otherFile := filepath.Join(root1Dir, "other.cbz")
	writeDiskFile(t, otherFile, buildDeterministicZip(t, "",
		zipEntry{name: "001.jpg", data: []byte("other"), method: zip.Store},
	))

	var root2Reads atomic.Int32
	resInc, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
			if string(rawDir) == root2Dir && root2Reads.Add(1) >= 2 {
				return nil, os.ErrPermission
			}
			return os.ReadDir(string(rawDir))
		},
	})
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected root ReadDir error on incomplete root walk, got res=%+v err=%v", resInc, err)
	}
	if resInc.Complete || resInc.ApplyRemovals {
		t.Fatalf("expected incomplete walk with ApplyRemovals=false, got %+v", resInc)
	}
	if got1, _ := cw.GetAssetByID(ctx, a1.ID); got1.MissingSince != nil {
		t.Fatalf("expected a1 not marked missing on incomplete walk, got %+v", got1)
	}
	// Verify other.cbz was NOT indexed either because the root walk failed.
	if len(listAssetsForRoot(ctx, t, transactor, r1.ID)) != 2 {
		t.Fatalf("expected no new asset indexed on incomplete walk")
	}

	// 3. Stale root snapshot before executeMarkMissing -> ErrStalePreCheck rolls back cleanly.
	if err := os.Remove(otherFile); err != nil {
		t.Fatalf("remove otherFile: %v", err)
	}
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
	for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
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
			writeDiskFile(t, filepath.Join(root2Dir, scanner.RootMarkerFilename), []byte{})

			origRel := "Series/v01.cbz"
			origPath := filepath.Join(root1Dir, origRel)
			keepPath := filepath.Join(root1Dir, "Series/keep.cbz")
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
			writeDiskFile(t, keepPath, buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Revive Saga</Series><Number>99</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("keep-sibling"), method: zip.Store},
			))

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

			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 2 {
				t.Fatalf("initial scan: res=%+v err=%v", res, err)
			}
			orig := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, r1.ID), origRel)

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
			for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
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

			assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 2)
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
		filePath := filepath.Join(rootDir, "Vol1.cbz")
		writeDiskFile(t, filePath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Retire Saga</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
		))
		writeDiskFile(t, filepath.Join(rootDir, "Keep.cbz"), buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Retire Saga</Series><Number>99</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("keep"), method: zip.Store},
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
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 2 {
			t.Fatalf("initial scan: res=%+v err=%v", res, err)
		}
		orig := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "Vol1.cbz")

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
		for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
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
		filePath := filepath.Join(rootDir, "Vol1.cbz")
		writeDiskFile(t, filePath, buildDeterministicZip(t, "",
			zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
		))
		writeDiskFile(t, filepath.Join(rootDir, "Keep.cbz"), buildDeterministicZip(t, "",
			zipEntry{name: "001.jpg", data: []byte("keep-grace"), method: zip.Store},
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

		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 2 {
			t.Fatalf("initial scan: res=%+v err=%v", res, err)
		}
		orig := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "Vol1.cbz")

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
			origRel := "Old/vol1.cbz"
			origPath := filepath.Join(rootDir, origRel)
			keepPath := filepath.Join(rootDir, "Old/keep.cbz")

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
			writeDiskFile(t, keepPath, buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>PostGrace</Series><Number>99</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("keep-postgrace"), method: zip.Store},
			))

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
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 2 {
				t.Fatalf("initial scan: res=%+v err=%v", res, err)
			}
			orig := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), origRel)

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
			assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 2)
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
			relPath := "Series/vol1.cbz"
			filePath := filepath.Join(rootDir, relPath)
			keepPath := filepath.Join(rootDir, "Series/keep.cbz")

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
			writeDiskFile(t, keepPath, buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Replace Saga</Series><Number>99</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("keep-replace"), method: zip.Store},
			))

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
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.IndexedCount != 2 {
				t.Fatalf("initial scan: res=%+v err=%v", res, err)
			}
			orig := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), relPath)
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
			for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
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
			assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 2)
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

func TestMissing_SkippedOriginalPathDoesNotReviveOntoCopyThenRestoresAtOwnPath(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	origPath := filepath.Join(rootDir, "a.cbz")
	keepPath := filepath.Join(rootDir, "keep.cbz")
	copyPath := filepath.Join(rootDir, "copy.cbz")
	bookBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Permission</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("same-bytes-skip-revival"), method: zip.Store},
	)
	writeDiskFile(t, origPath, bookBytes)
	writeDiskFile(t, keepPath, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Permission</Series><Number>99</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("keep-skip"), method: zip.Store},
	))

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Skipped Revival Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	t0 := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(t0)
	sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 2 {
		t.Fatalf("initial scan: res=%+v err=%v", res, err)
	}
	orig := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "a.cbz")
	userID := seedUserAndReadingProgress(ctx, t, db, transactor, "skip-reader", orig, 4)

	// Mark a.cbz missing cleanly.
	clock.Advance(time.Hour)
	if err := os.Remove(origPath); err != nil {
		t.Fatalf("remove orig: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || res.MissingCount != 1 {
		t.Fatalf("missing scan: res=%+v err=%v", res, err)
	}

	// Both a.cbz (unreadable/skipped) and matching copy.cbz (readable) appear on disk.
	clock.Advance(2 * time.Hour)
	writeDiskFile(t, origPath, bookBytes)
	writeDiskFile(t, copyPath, bookBytes)
	resSkip, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			if string(rawPath) == origPath {
				return nil, os.ErrPermission
			}
			return os.Open(string(rawPath))
		},
	})
	if err != nil || !resSkip.Complete || resSkip.IndexedCount != 1 || resSkip.MovedCount != 0 || resSkip.RestoredCount != 0 {
		t.Fatalf("expected copy.cbz indexed as new and 0 moves/restores while a.cbz skipped, got res=%+v err=%v", resSkip, err)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	gotOrigStillMissing, err := cw.GetAssetByID(ctx, orig.ID)
	if err != nil || gotOrigStillMissing.MissingSince == nil || gotOrigStillMissing.RelPathDisplay != "a.cbz" || gotOrigStillMissing.VolumeID != orig.VolumeID {
		t.Fatalf("expected orig asset to remain at a.cbz in missing state on same volume, got %+v (err=%v)", gotOrigStillMissing, err)
	}
	assertReadingProgressPreserved(ctx, t, db, userID, orig.VolumeID, orig.ID, 4)

	assetsAfterCopy := listAssetsForRoot(ctx, t, transactor, root.ID)
	if len(assetsAfterCopy) != 3 {
		t.Fatalf("expected 3 assets (missing orig + keep.cbz + new copy.cbz), got %+v", assetsAfterCopy)
	}
	copyAsset := findAssetByRelPath(t, assetsAfterCopy, "copy.cbz")
	if copyAsset.ID == orig.ID || copyAsset.RelPathDisplay != "copy.cbz" || !copyAsset.IsAvailable() || copyAsset.VolumeID != orig.VolumeID {
		t.Fatalf("expected copy.cbz indexed as separate asset on volume %s, got %+v (orig=%+v)", orig.VolumeID, copyAsset, orig)
	}

	// Next readable scan restores orig at a.cbz on its original ID, volume, and progress.
	clock.Advance(time.Hour)
	resRestore, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil || !resRestore.Complete || resRestore.RestoredCount != 1 || resRestore.MovedCount != 0 || resRestore.IndexedCount != 0 {
		t.Fatalf("restore scan: res=%+v err=%v", resRestore, err)
	}
	gotRestored, err := cw.GetAssetByID(ctx, orig.ID)
	if err != nil || !gotRestored.IsAvailable() || gotRestored.RelPathDisplay != "a.cbz" || gotRestored.VolumeID != orig.VolumeID {
		t.Fatalf("expected orig restored cleanly at a.cbz, got %+v (err=%v)", gotRestored, err)
	}
	assertReadingProgressPreserved(ctx, t, db, userID, orig.VolumeID, orig.ID, 4)

	var origEvents []domain.EventKind
	for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
		if ev.AggregateID == orig.ID {
			origEvents = append(origEvents, ev.Kind)
		}
	}
	if !slices.Contains(origEvents, domain.EventAssetRestored) || slices.Contains(origEvents, domain.EventAssetMoved) {
		t.Fatalf("expected asset.restored without asset.moved for orig, got %v", origEvents)
	}
}

func TestMissing_StaleAbsenceRejectedAfterNewerOnlinePreCheck(t *testing.T) {
	t.Parallel()

	for _, advancingClock := range []bool{true, false} {
		name := "same_clock_timestamp"
		if advancingClock {
			name = "advancing_clock_timestamp"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, transactor := setupIndexerTestDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			rootDir := filepath.Join(t.TempDir(), "root")
			bookPath := filepath.Join(rootDir, "a.cbz")
			keepPath := filepath.Join(rootDir, "keep.cbz")
			bookBytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Race</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("race-bytes"), method: zip.Store},
			)
			writeDiskFile(t, bookPath, bookBytes)
			writeDiskFile(t, keepPath, buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Race</Series><Number>99</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("keep-race"), method: zip.Store},
			))

			var lib domain.Library
			var root domain.LibraryRoot
			if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
				var roots []domain.LibraryRoot
				lib, roots = seedLibraryAndRoots(ctx, t, tx, "Stale Absence Race Lib", nil, []byte(rootDir))
				root = roots[0]
				return nil
			}); err != nil {
				t.Fatalf("seed failed: %v", err)
			}

			t0 := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
			clock := newFakeClock(t0)
			scB, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
			if err != nil {
				t.Fatalf("NewScanner(scB): %v", err)
			}
			if res, err := scB.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 2 {
				t.Fatalf("initial scan: res=%+v err=%v", res, err)
			}
			orig := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "a.cbz")

			// Delete a.cbz so Scan A walks and sees it absent, then pause right before executeMarkMissing.
			if err := os.Remove(bookPath); err != nil {
				t.Fatalf("remove a.cbz: %v", err)
			}
			if advancingClock {
				clock.Advance(time.Minute)
			}

			scanAPaused := make(chan struct{})
			releaseScanA := make(chan struct{})
			var pauseOnce sync.Once
			hookTx := &reconcileHookTransactor{
				inner: transactor,
				beforeSecondReadCommitted: func() error {
					pauseOnce.Do(func() {
						close(scanAPaused)
						select {
						case <-releaseScanA:
						case <-ctx.Done():
						}
					})
					return ctx.Err()
				},
			}
			scA, err := scanner.NewScanner(hookTx, scanner.Options{Now: clock.Now})
			if err != nil {
				t.Fatalf("NewScanner(scA): %v", err)
			}

			var (
				resA scanner.ScanResult
				errA error
			)
			doneA := make(chan struct{})
			go func() {
				defer close(doneA)
				resA, errA = scA.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
			}()

			select {
			case <-scanAPaused:
			case <-doneA:
				t.Fatalf("Scan A exited before pausing at executeMarkMissing: res=%+v err=%v", resA, errA)
			case <-ctx.Done():
				t.Fatalf("timed out waiting for Scan A pause: %v", ctx.Err())
			}

			// File returns with its original size and mtime; Scan B completes ONLINE seeing it unchanged.
			writeDiskFile(t, bookPath, bookBytes)
			if orig.FileMtime != nil {
				if err := os.Chtimes(bookPath, *orig.FileMtime, *orig.FileMtime); err != nil {
					t.Fatalf("Chtimes(a.cbz): %v", err)
				}
			}
			if advancingClock {
				clock.Advance(time.Minute)
			}
			resB, errB := scB.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
			if errB != nil || !resB.Complete || resB.UnchangedCount != 2 || resB.MissingCount != 0 || resB.IndexedCount != 0 {
				t.Fatalf("expected Scan B to complete cleanly seeing a.cbz unchanged, got res=%+v err=%v", resB, errB)
			}

			// Release Scan A's missing write; it must reject as stale and preserve availability.
			close(releaseScanA)
			<-doneA
			if !errors.Is(errA, scanner.ErrStalePreCheck) {
				t.Fatalf("expected stale Scan A to fail with ErrStalePreCheck, got res=%+v err=%v", resA, errA)
			}
			if resA.Complete || resA.ApplyRemovals {
				t.Fatalf("expected Scan A Complete=false ApplyRemovals=false, got %+v", resA)
			}

			cw := postgres.NewCatalogueWriteRepository(db.Pool)
			got, err := cw.GetAssetByID(ctx, orig.ID)
			if err != nil || !got.IsAvailable() || got.MissingSince != nil || got.RetiredAt != nil {
				t.Fatalf("expected a.cbz to remain available after stale Scan A rejection, got %+v (err=%v)", got, err)
			}
			catRepo := postgres.NewCatalogueRepository(db.Pool)
			adminScope := storage.CatalogueReadScope{Principal: adminPrincipalForReconcileTest(), IncludeTrashed: true}
			assertAvailableReadsIncludeAsset(ctx, t, catRepo, adminScope, orig.ID, orig.VolumeID)

			for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
				if ev.Kind == domain.EventAssetMissing {
					t.Fatalf("expected no asset.missing outbox event from superseded Scan A, got %+v", ev)
				}
			}
		})
	}
}

func TestMissing_EmptyRootAfterPreCheckReturnsStalePreCheckAndMarksNothingMissing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name               string
		emptyStartingAtHit int32
		deleteOneOnDisk    bool
	}{
		{
			name:               "empty immediately after pre-check (walk and re-check)",
			emptyStartingAtHit: 2,
		},
		{
			name:               "empty right before executeMarkMissing",
			emptyStartingAtHit: 3,
			deleteOneOnDisk:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, transactor := setupIndexerTestDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			rootDir := filepath.Join(t.TempDir(), "root")
			book1Path := filepath.Join(rootDir, "book1.cbz")
			book2Path := filepath.Join(rootDir, "book2.cbz")
			writeDiskFile(t, book1Path, buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>EmptyAfterPreCheck</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("b1"), method: zip.Store},
			))
			writeDiskFile(t, book2Path, buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>EmptyAfterPreCheck</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("b2"), method: zip.Store},
			))

			var (
				lib  domain.Library
				root domain.LibraryRoot
			)
			if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
				var roots []domain.LibraryRoot
				lib, roots = seedLibraryAndRoots(ctx, t, tx, "EmptyAfterPreCheck Lib", nil, []byte(rootDir))
				root = roots[0]
				return nil
			}); err != nil {
				t.Fatalf("seed failed: %v", err)
			}

			sc, err := scanner.NewScanner(transactor, scanner.Options{})
			if err != nil {
				t.Fatalf("NewScanner: %v", err)
			}
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 2 {
				t.Fatalf("initial scan = (%+v, %v)", res, err)
			}

			if tc.deleteOneOnDisk {
				if err := os.Remove(book1Path); err != nil {
					t.Fatalf("Remove(book1.cbz): %v", err)
				}
			}

			var rootReads atomic.Int32
			res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
				ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
					if string(rawDir) == rootDir && rootReads.Add(1) >= tc.emptyStartingAtHit {
						return nil, nil
					}
					return os.ReadDir(string(rawDir))
				},
			})
			if !errors.Is(err, scanner.ErrStalePreCheck) {
				t.Fatalf("expected ErrStalePreCheck, got res=%+v err=%v", res, err)
			}
			if res.Complete || res.ApplyRemovals || res.MissingCount != 0 {
				t.Fatalf("expected Complete=false ApplyRemovals=false MissingCount=0, got %+v", res)
			}

			for _, asset := range listAssetsForRoot(ctx, t, transactor, root.ID) {
				if !asset.IsAvailable() || asset.MissingSince != nil {
					t.Fatalf("expected asset %s to remain available, got %+v", asset.RelPathDisplay, asset)
				}
			}
			for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
				if ev.Kind == domain.EventAssetMissing {
					t.Fatalf("expected no asset.missing event, got %+v", ev)
				}
			}
		})
	}
}

func TestMissing_BrokenDirectorySymlinkWithKnownAssetsMarksNothingMissing(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	rootDir := filepath.Join(base, "root")
	extDir := filepath.Join(base, "external-series")
	keepPath := filepath.Join(rootDir, "keep.cbz")
	targetBookPath := filepath.Join(extDir, "vol01.cbz")

	writeDiskFile(t, keepPath, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Keep</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("keep"), method: zip.Store},
	))
	writeDiskFile(t, targetBookPath, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Linked</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("linked"), method: zip.Store},
	))
	mustSymlink(t, extDir, filepath.Join(rootDir, "series_link"))

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "BrokenDirLink Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 2 {
		t.Fatalf("initial scan = (%+v, %v)", res, err)
	}

	if err := os.RemoveAll(extDir); err != nil {
		t.Fatalf("RemoveAll(extDir): %v", err)
	}

	res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("ScanLibrary after broken symlink: %v", err)
	}
	if res.Complete || !res.ApplyRemovals || res.MissingCount != 0 {
		t.Fatalf("expected Complete=false ApplyRemovals=true MissingCount=0, got %+v", res)
	}
	if len(res.IncompletePrefixes) != 1 ||
		res.IncompletePrefixes[0].RelPath.Display != "series_link" ||
		res.IncompletePrefixes[0].Reason != scanner.SkipReasonBrokenSymlink {
		t.Fatalf("IncompletePrefixes = %+v, want [series_link broken_symlink]", res.IncompletePrefixes)
	}

	for _, asset := range listAssetsForRoot(ctx, t, transactor, root.ID) {
		if !asset.IsAvailable() || asset.MissingSince != nil {
			t.Fatalf("expected asset %s to remain available, got %+v", asset.RelPathDisplay, asset)
		}
	}
	for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
		if ev.Kind == domain.EventAssetMissing {
			t.Fatalf("expected no asset.missing event, got %+v", ev)
		}
	}
}

func TestMissing_RootWithZeroConfirmedKnownAssetsMarksIncompleteAndNothingMissing(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	writeDiskFile(t, filepath.Join(rootDir, "readme.txt"), []byte("keeps root online"))
	bookPath := filepath.Join(rootDir, "nested/vol01.cbz")
	writeDiskFile(t, bookPath, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>NestedMount</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("nested-book"), method: zip.Store},
	))

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "ZeroConfirmed Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 1 {
		t.Fatalf("initial scan = (%+v, %v)", res, err)
	}

	if err := os.Remove(bookPath); err != nil {
		t.Fatalf("Remove(bookPath): %v", err)
	}

	res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("ScanLibrary with zero confirmed known assets: %v", err)
	}
	if res.Complete || res.ApplyRemovals || res.MissingCount != 0 || res.Roots[0].Complete {
		t.Fatalf("expected incomplete scan with ApplyRemovals=false MissingCount=0, got %+v", res)
	}

	assets := listAssetsForRoot(ctx, t, transactor, root.ID)
	if len(assets) != 1 || !assets[0].IsAvailable() || assets[0].MissingSince != nil {
		t.Fatalf("expected asset to remain available, got %+v", assets)
	}
	for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
		if ev.Kind == domain.EventAssetMissing {
			t.Fatalf("expected no asset.missing event, got %+v", ev)
		}
	}
}

func TestMissing_MarkerPresentWithZeroConfirmedKnownAssetsMarksIncompleteAndNothingMissing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		brokenMarker bool
	}{
		{name: "valid marker remains while indexed books are unseen"},
		{name: "broken symlink marker with unrelated file while indexed books are unseen", brokenMarker: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, transactor := setupIndexerTestDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			base := t.TempDir()
			rootDir := filepath.Join(base, "root")
			if tc.brokenMarker {
				writeDiskFile(t, filepath.Join(rootDir, "readme.txt"), []byte("keeps root online"))
				mustSymlink(t, filepath.Join(base, "missing-marker-target"), filepath.Join(rootDir, scanner.RootMarkerFilename))
			} else {
				writeDiskFile(t, filepath.Join(rootDir, scanner.RootMarkerFilename), []byte{})
			}

			book1Path := filepath.Join(rootDir, "nested/vol01.cbz")
			book2Path := filepath.Join(rootDir, "nested/vol02.cbz")
			writeDiskFile(t, book1Path, buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>MarkerGuard</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("m1"), method: zip.Store},
			))
			writeDiskFile(t, book2Path, buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>MarkerGuard</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("m2"), method: zip.Store},
			))

			var (
				lib  domain.Library
				root domain.LibraryRoot
			)
			if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
				var roots []domain.LibraryRoot
				lib, roots = seedLibraryAndRoots(ctx, t, tx, "MarkerZeroConfirm Lib", nil, []byte(rootDir))
				root = roots[0]
				return nil
			}); err != nil {
				t.Fatalf("seed failed: %v", err)
			}

			sc, err := scanner.NewScanner(transactor, scanner.Options{})
			if err != nil {
				t.Fatalf("NewScanner: %v", err)
			}
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 2 {
				t.Fatalf("initial scan = (%+v, %v)", res, err)
			}

			if err := os.RemoveAll(filepath.Join(rootDir, "nested")); err != nil {
				t.Fatalf("RemoveAll(nested): %v", err)
			}

			res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
			if err != nil {
				t.Fatalf("ScanLibrary: %v", err)
			}
			if res.Complete || res.ApplyRemovals || res.MissingCount != 0 || res.Roots[0].Complete {
				t.Fatalf("expected incomplete scan with ApplyRemovals=false MissingCount=0, got %+v", res)
			}

			assets := listAssetsForRoot(ctx, t, transactor, root.ID)
			if len(assets) != 2 {
				t.Fatalf("expected 2 assets retained, got %d", len(assets))
			}
			for _, asset := range assets {
				if !asset.IsAvailable() || asset.MissingSince != nil {
					t.Fatalf("expected asset %s to remain available, got %+v", asset.RelPathDisplay, asset)
				}
			}
			for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
				if ev.Kind == domain.EventAssetMissing {
					t.Fatalf("expected no asset.missing event, got %+v", ev)
				}
			}
		})
	}
}

func TestMissing_CrossRootMatchDoesNotAuthorizeUnobservedSourceRootRemovals(t *testing.T) {
	t.Parallel()

	t.Run("cross-root match leaves unobserved source root incomplete and emits no missing or move events", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		base := t.TempDir()
		root1Dir := filepath.Join(base, "r1")
		root2Dir := filepath.Join(base, "r2")
		writeDiskFile(t, filepath.Join(root1Dir, scanner.RootMarkerFilename), []byte{})

		bytesA := buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>CrossRoot</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("cross-root-a"), method: zip.Store},
		)
		bytesB := buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>CrossRoot</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("cross-root-b"), method: zip.Store},
		)
		bytesKeep2 := buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>CrossRoot</Series><Number>3</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("cross-root-keep2"), method: zip.Store},
		)

		pathA := filepath.Join(root1Dir, "a.cbz")
		pathB := filepath.Join(root1Dir, "b.cbz")
		writeDiskFile(t, pathA, bytesA)
		writeDiskFile(t, pathB, bytesB)
		writeDiskFile(t, filepath.Join(root2Dir, "keep2.cbz"), bytesKeep2)

		var (
			lib    domain.Library
			r1, r2 domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "CrossRootUnobserved Lib", nil, []byte(root1Dir), []byte(root2Dir))
			r1, r2 = roots[0], roots[1]
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 3 {
			t.Fatalf("initial scan = (%+v, %v)", res, err)
		}
		origA := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, r1.ID), "a.cbz")
		origB := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, r1.ID), "b.cbz")

		// Source root loses visibility of both known assets while keeping its marker,
		// and a matching copy of a.cbz appears in root2.
		if err := os.Remove(pathA); err != nil {
			t.Fatalf("Remove(a.cbz): %v", err)
		}
		if err := os.Remove(pathB); err != nil {
			t.Fatalf("Remove(b.cbz): %v", err)
		}
		writeDiskFile(t, filepath.Join(root2Dir, "a_copy.cbz"), bytesA)

		res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("ScanLibrary: %v", err)
		}
		if res.Complete || res.ApplyRemovals || res.MissingCount != 0 || res.MovedCount != 0 || res.Roots[0].Complete {
			t.Fatalf("expected incomplete source root with no moves or removals, got %+v", res)
		}

		cw := postgres.NewCatalogueWriteRepository(db.Pool)
		gotA, err := cw.GetAssetByID(ctx, origA.ID)
		if err != nil || gotA.LibraryRootID != r1.ID || gotA.RelPathDisplay != "a.cbz" || !gotA.IsAvailable() || gotA.MissingSince != nil {
			t.Fatalf("expected a.cbz protected at r1, got (%+v, %v)", gotA, err)
		}
		gotB, err := cw.GetAssetByID(ctx, origB.ID)
		if err != nil || gotB.LibraryRootID != r1.ID || gotB.RelPathDisplay != "b.cbz" || !gotB.IsAvailable() || gotB.MissingSince != nil {
			t.Fatalf("expected b.cbz protected at r1, got (%+v, %v)", gotB, err)
		}
		if r2Assets := listAssetsForRoot(ctx, t, transactor, r2.ID); len(r2Assets) != 1 || r2Assets[0].RelPathDisplay != "keep2.cbz" {
			t.Fatalf("expected no assets committed in r2 during incomplete pass, got %+v", r2Assets)
		}
		for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
			if ev.Kind == domain.EventAssetMissing || ev.Kind == domain.EventAssetMoved {
				t.Fatalf("expected no asset.missing or asset.moved event, got %+v", ev)
			}
		}
	})

	t.Run("same-root rename with sibling deletion authorizes removal", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		bytesA := buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>SameRootRename</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("same-root-a"), method: zip.Store},
		)
		bytesB := buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>SameRootRename</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("same-root-b"), method: zip.Store},
		)

		pathA := filepath.Join(rootDir, "a.cbz")
		pathB := filepath.Join(rootDir, "b.cbz")
		writeDiskFile(t, pathA, bytesA)
		writeDiskFile(t, pathB, bytesB)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "SameRootRenameDelete Lib", nil, []byte(rootDir))
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 2 {
			t.Fatalf("initial scan = (%+v, %v)", res, err)
		}
		origA := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "a.cbz")
		origB := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "b.cbz")

		mustRenameDiskFile(t, pathA, filepath.Join(rootDir, "a_renamed.cbz"))
		if err := os.Remove(pathB); err != nil {
			t.Fatalf("Remove(b.cbz): %v", err)
		}

		res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("ScanLibrary: %v", err)
		}
		if !res.Complete || !res.ApplyRemovals || res.MovedCount != 1 || res.MissingCount != 1 {
			t.Fatalf("expected Complete=true ApplyRemovals=true MovedCount=1 MissingCount=1, got %+v", res)
		}

		cw := postgres.NewCatalogueWriteRepository(db.Pool)
		gotA, err := cw.GetAssetByID(ctx, origA.ID)
		if err != nil || gotA.RelPathDisplay != "a_renamed.cbz" || !gotA.IsAvailable() {
			t.Fatalf("expected a.cbz moved to a_renamed.cbz, got (%+v, %v)", gotA, err)
		}
		gotB, err := cw.GetAssetByID(ctx, origB.ID)
		if err != nil || !gotB.IsMissing() {
			t.Fatalf("expected b.cbz marked missing, got (%+v, %v)", gotB, err)
		}
	})
}

func TestMissing_SkippedBrokenFileLinkDoesNotAuthorizeUnseenSiblingRemoval(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	rootDir := filepath.Join(base, "root")
	extDir := filepath.Join(base, "ext")
	writeDiskFile(t, filepath.Join(rootDir, "readme.txt"), []byte("keeps root online"))

	targetPath := filepath.Join(extDir, "target.cbz")
	linkPath := filepath.Join(rootDir, "dangling.cbz")
	siblingPath := filepath.Join(rootDir, "sibling.cbz")
	writeDiskFile(t, targetPath, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>DanglingFile</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("linked-file"), method: zip.Store},
	))
	mustSymlink(t, targetPath, linkPath)
	writeDiskFile(t, siblingPath, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>DanglingFile</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("sibling-file"), method: zip.Store},
	))

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "SkippedBrokenFileLink Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 2 {
		t.Fatalf("initial scan = (%+v, %v)", res, err)
	}
	origLink := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "dangling.cbz")
	origSibling := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "sibling.cbz")

	// Break the file symlink (skipped without failing walk) and hide/remove the sibling.
	if err := os.Remove(targetPath); err != nil {
		t.Fatalf("Remove(target.cbz): %v", err)
	}
	if err := os.Remove(siblingPath); err != nil {
		t.Fatalf("Remove(sibling.cbz): %v", err)
	}

	res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}
	if res.Complete || res.ApplyRemovals || res.MissingCount != 0 || res.Roots[0].Complete {
		t.Fatalf("expected incomplete scan with ApplyRemovals=false MissingCount=0, got %+v", res)
	}
	if !slices.ContainsFunc(res.Roots[0].Skipped, func(s scanner.SkippedEntry) bool {
		return s.RelPath.Display == "dangling.cbz" && s.Reason == scanner.SkipReasonBrokenSymlink
	}) {
		t.Fatalf("expected dangling.cbz recorded as broken_symlink skip, got %+v", res.Roots[0].Skipped)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	for _, orig := range []domain.MediaAsset{origLink, origSibling} {
		got, err := cw.GetAssetByID(ctx, orig.ID)
		if err != nil || !got.IsAvailable() || got.MissingSince != nil {
			t.Fatalf("expected asset %s to remain available, got (%+v, %v)", orig.RelPathDisplay, got, err)
		}
	}
	for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
		if ev.Kind == domain.EventAssetMissing {
			t.Fatalf("expected no asset.missing event, got %+v", ev)
		}
	}
}

func TestMissing_SameRootRestoreAuthorizesSiblingRemoval(t *testing.T) {
	t.Parallel()

	for _, historical := range []bool{false, true} {
		name := "current digest reappearance"
		if historical {
			name = "historical digest reappearance"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, transactor := setupIndexerTestDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			rootDir := filepath.Join(t.TempDir(), "root")
			v1Bytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>RestoreAuthorizes</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("restore-v1"), method: zip.Store},
			)
			v2Bytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>RestoreAuthorizes</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("restore-v2"), method: zip.Store},
				zipEntry{name: "002.jpg", data: []byte("restore-v2-p2"), method: zip.Store},
			)
			siblingBytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>RestoreAuthorizes</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("restore-sibling"), method: zip.Store},
			)

			pathA := filepath.Join(rootDir, "a.cbz")
			pathB := filepath.Join(rootDir, "b.cbz")
			writeDiskFile(t, pathA, v1Bytes)
			writeDiskFile(t, pathB, siblingBytes)

			var (
				lib  domain.Library
				root domain.LibraryRoot
			)
			if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
				var roots []domain.LibraryRoot
				lib, roots = seedLibraryAndRoots(ctx, t, tx, "RestoreAuthorizes Lib", nil, []byte(rootDir))
				root = roots[0]
				return nil
			}); err != nil {
				t.Fatalf("seed failed: %v", err)
			}

			t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
			clock := newFakeClock(t0)
			sc, err := scanner.NewScanner(transactor, scanner.Options{Now: clock.Now})
			if err != nil {
				t.Fatalf("NewScanner: %v", err)
			}
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 2 {
				t.Fatalf("initial scan = (%+v, %v)", res, err)
			}
			origA := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "a.cbz")
			origB := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "b.cbz")

			if historical {
				clock.Advance(time.Minute)
				writeDiskFile(t, pathA, v2Bytes)
				if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.ModifiedCount != 1 {
					t.Fatalf("modify scan = (%+v, %v)", res, err)
				}
			}

			// Mark a.cbz missing while b.cbz keeps the root confirmed.
			clock.Advance(time.Hour)
			if err := os.Remove(pathA); err != nil {
				t.Fatalf("Remove(a.cbz): %v", err)
			}
			if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.MissingCount != 1 {
				t.Fatalf("mark a missing scan = (%+v, %v)", res, err)
			}

			// Now a.cbz reappears at its own path (only observed known asset in root) while b.cbz is deleted.
			clock.Advance(time.Hour)
			writeDiskFile(t, pathA, v1Bytes)
			if err := os.Remove(pathB); err != nil {
				t.Fatalf("Remove(b.cbz): %v", err)
			}

			res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
			if err != nil {
				t.Fatalf("restore+delete scan: %v", err)
			}
			if !res.Complete || !res.ApplyRemovals || res.RestoredCount != 1 || res.MissingCount != 1 {
				t.Fatalf("expected Complete=true ApplyRemovals=true RestoredCount=1 MissingCount=1, got %+v", res)
			}

			cw := postgres.NewCatalogueWriteRepository(db.Pool)
			gotA, err := cw.GetAssetByID(ctx, origA.ID)
			if err != nil || !gotA.IsAvailable() || gotA.MissingSince != nil || gotA.RetiredAt != nil {
				t.Fatalf("expected a.cbz restored and available, got (%+v, %v)", gotA, err)
			}
			gotB, err := cw.GetAssetByID(ctx, origB.ID)
			if err != nil || !gotB.IsMissing() || gotB.IsAvailable() {
				t.Fatalf("expected b.cbz marked missing, got (%+v, %v)", gotB, err)
			}

			events := loadOutboxEventsDetailed(ctx, t, db)
			var restoredA, missingB int
			for _, ev := range events {
				if ev.Kind == domain.EventAssetRestored && ev.AggregateID == origA.ID {
					restoredA++
				}
				if ev.Kind == domain.EventAssetMissing && ev.AggregateID == origB.ID {
					missingB++
				}
			}
			if restoredA != 1 || missingB != 1 {
				t.Fatalf("expected 1 asset.restored for a.cbz and 1 asset.missing for b.cbz, got restored=%d missing=%d (events=%+v)",
					restoredA, missingB, events)
			}
		})
	}
}

func TestMissing_BrokenDirectorySymlinkScopesSubtreeAndReconcilesSiblings(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	rootDir := filepath.Join(base, "root")
	extDir := filepath.Join(base, "external-series")

	keepBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Keep</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("keep-book"), method: zip.Store},
	)
	linkedBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Linked</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("linked-book"), method: zip.Store},
	)
	deletedBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Keep</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("deleted-outside"), method: zip.Store},
	)
	newBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Keep</Series><Number>3</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("new-outside"), method: zip.Store},
	)

	writeDiskFile(t, filepath.Join(rootDir, "keep.cbz"), keepBytes)
	deletedPath := filepath.Join(rootDir, "deleted.cbz")
	writeDiskFile(t, deletedPath, deletedBytes)
	writeDiskFile(t, filepath.Join(extDir, "vol01.cbz"), linkedBytes)
	mustSymlink(t, extDir, filepath.Join(rootDir, "series_link"))

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "ScopedBrokenDirLink Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seedLibraryAndRoots: %v", err)
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 3 {
		t.Fatalf("initial scan = (%+v, %v)", res, err)
	}
	origLinked := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "series_link/vol01.cbz")
	origDeleted := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "deleted.cbz")

	// Break the directory symlink target, delete deleted.cbz, add new.cbz, and add a copy of linkedBytes.
	if err := os.RemoveAll(extDir); err != nil {
		t.Fatalf("RemoveAll(extDir): %v", err)
	}
	if err := os.Remove(deletedPath); err != nil {
		t.Fatalf("Remove(deleted.cbz): %v", err)
	}
	writeDiskFile(t, filepath.Join(rootDir, "new.cbz"), newBytes)
	writeDiskFile(t, filepath.Join(rootDir, "copy_of_linked.cbz"), linkedBytes)

	res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}
	if res.Complete || !res.ApplyRemovals || res.IndexedCount != 2 || res.MovedCount != 0 || res.MissingCount != 1 {
		t.Fatalf("unexpected scoped broken-link scan result: %+v", res)
	}
	if len(res.IncompletePrefixes) != 1 || res.IncompletePrefixes[0].RelPath.Display != "series_link" {
		t.Fatalf("IncompletePrefixes = %+v, want [series_link]", res.IncompletePrefixes)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	gotLinked, err := cw.GetAssetByID(ctx, origLinked.ID)
	if err != nil || !gotLinked.IsAvailable() || gotLinked.MissingSince != nil {
		t.Fatalf("expected series_link/vol01.cbz active, got (%+v, %v)", gotLinked, err)
	}
	gotDeleted, err := cw.GetAssetByID(ctx, origDeleted.ID)
	if err != nil || !gotDeleted.IsMissing() {
		t.Fatalf("expected deleted.cbz marked missing, got (%+v, %v)", gotDeleted, err)
	}

	copyAsset := findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "copy_of_linked.cbz")
	if copyAsset.ID == origLinked.ID {
		t.Fatalf("expected copy_of_linked.cbz indexed as distinct asset from %s", origLinked.ID)
	}
	_ = findAssetByRelPath(t, listAssetsForRoot(ctx, t, transactor, root.ID), "new.cbz")

	for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
		if ev.Kind == domain.EventAssetMoved {
			t.Fatalf("unexpected asset.moved event: %+v", ev)
		}
		if ev.Kind == domain.EventAssetMissing && ev.AggregateID == origLinked.ID {
			t.Fatalf("unexpected asset.missing event for protected linked asset: %+v", ev)
		}
	}
}
