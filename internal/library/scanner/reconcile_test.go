package scanner_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/roots"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

func seedUserAndReadingProgress(
	ctx context.Context,
	t *testing.T,
	db *testdb.TestDB,
	transactor *postgres.Transactor,
	username string,
	asset domain.MediaAsset,
	page int,
) string {
	t.Helper()
	var userID string
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		u := domain.User{ID: domain.MustNewID(), Username: username}
		if err := tx.Users().CreateUser(ctx, &u); err != nil {
			return err
		}
		userID = u.ID
		return nil
	}); err != nil {
		t.Fatalf("CreateUser(%q): %v", username, err)
	}

	cur, ok := asset.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok {
		t.Fatalf("asset %s missing current partial digest", asset.ID)
	}
	pageCount := max(asset.PageCount, 1)
	locBytes, err := json.Marshal(domain.Locator{
		Type:      domain.LocatorTypePage,
		Page:      &page,
		AssetID:   asset.ID,
		PageCount: &pageCount,
		AssetDigest: &domain.LocatorDigest{
			Kind:  cur.Kind,
			Value: cur.Value,
		},
	})
	if err != nil {
		t.Fatalf("marshal locator: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, source, completed)
		VALUES ($1, $2, $3, 0.5, 'KOMGA', FALSE)`,
		userID, asset.VolumeID, locBytes,
	); err != nil {
		t.Fatalf("insert reading_progress: %v", err)
	}
	return userID
}

func assertReadingProgressPreserved(
	ctx context.Context,
	t *testing.T,
	db *testdb.TestDB,
	userID, volumeID, assetID string,
	wantPage int,
) {
	t.Helper()
	var locRaw []byte
	var prog float32
	if err := db.QueryRow(ctx, `
		SELECT locator, total_progression
		FROM reading_progress
		WHERE user_id = $1 AND volume_id = $2`,
		userID, volumeID,
	).Scan(&locRaw, &prog); err != nil {
		t.Fatalf("query reading_progress (%s, %s): %v", userID, volumeID, err)
	}
	var loc domain.Locator
	if err := json.Unmarshal(locRaw, &loc); err != nil {
		t.Fatalf("unmarshal locator: %v", err)
	}
	if loc.AssetID != assetID || loc.Page == nil || *loc.Page != wantPage || prog != 0.5 {
		t.Fatalf("reading_progress altered: loc=%+v prog=%v", loc, prog)
	}
}

func assertUnchangedRescanZeroOpens(ctx context.Context, t *testing.T, sc *scanner.Scanner, libID string, wantUnchanged int) {
	t.Helper()
	var opens atomic.Int32
	res, err := sc.ScanLibrary(ctx, libID, scanner.ScanOptions{
		Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			opens.Add(1)
			return os.Open(string(rawPath))
		},
	})
	if err != nil {
		t.Fatalf("unchanged rescan failed: %v", err)
	}
	if !res.Complete || res.UnchangedCount != wantUnchanged || res.MovedCount != 0 || res.ModifiedCount != 0 || res.IndexedCount != 0 || opens.Load() != 0 {
		t.Fatalf("unexpected rescan after reconciliation: res=%+v opens=%d", res, opens.Load())
	}
}

func mustRenameDiskFile(t *testing.T, oldPath, newPath string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(newPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(newPath), err)
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatalf("Rename(%s -> %s): %v", oldPath, newPath, err)
	}
}

func TestReconcile_RenameWithinRoot_BothSortOrders(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		oldPath string
		newPath string
	}{
		{
			name:    "new path sorts before old",
			oldPath: "Series/zzz_v01.cbz",
			newPath: "Series/aaa_v01.cbz",
		},
		{
			name:    "new path sorts after old",
			oldPath: "Series/aaa_v01.cbz",
			newPath: "Series/zzz_v01.cbz",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, transactor := setupIndexerTestDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			rootDir := filepath.Join(t.TempDir(), "root")
			cbzBytes := buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Move Test</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("page-1"), method: zip.Store},
			)
			writeDiskFile(t, filepath.Join(rootDir, tc.oldPath), cbzBytes)

			var (
				lib  domain.Library
				root domain.LibraryRoot
			)
			if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
				var roots []domain.LibraryRoot
				lib, roots = seedLibraryAndRoots(ctx, t, tx, "Rename Lib", nil, []byte(rootDir))
				root = roots[0]
				return nil
			}); err != nil {
				t.Fatalf("seed failed: %v", err)
			}

			sc, err := scanner.NewScanner(transactor, scanner.Options{})
			if err != nil {
				t.Fatalf("NewScanner: %v", err)
			}
			res1, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
			if err != nil || !res1.Complete || res1.IndexedCount != 1 {
				t.Fatalf("initial scan = (%+v, %v)", res1, err)
			}

			initialAssets := listAssetsForRoot(ctx, t, transactor, root.ID)
			if len(initialAssets) != 1 {
				t.Fatalf("expected 1 asset, got %d", len(initialAssets))
			}
			orig := initialAssets[0]
			userID := seedUserAndReadingProgress(ctx, t, db, transactor, "reader", orig, 7)

			mustRenameDiskFile(t, filepath.Join(rootDir, tc.oldPath), filepath.Join(rootDir, tc.newPath))

			res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
			if err != nil {
				t.Fatalf("rename scan failed: %v", err)
			}
			if !res2.Complete || res2.MovedCount != 1 || res2.IndexedCount != 0 || res2.ModifiedCount != 0 {
				t.Fatalf("unexpected rename scan result: %+v", res2)
			}

			afterAssets := listAssetsForRoot(ctx, t, transactor, root.ID)
			if len(afterAssets) != 1 {
				t.Fatalf("expected 1 asset after move, got %d", len(afterAssets))
			}
			moved := afterAssets[0]
			if moved.ID != orig.ID || moved.VolumeID != orig.VolumeID || moved.LibraryRootID != root.ID {
				t.Fatalf("asset identity changed: got %+v, want ID=%s Vol=%s", moved, orig.ID, orig.VolumeID)
			}
			if !bytes.Equal(moved.RelPathBytes, []byte(tc.newPath)) || moved.RelPathDisplay != tc.newPath {
				t.Fatalf("unexpected moved path: bytes=%q display=%q", moved.RelPathBytes, moved.RelPathDisplay)
			}
			assertReadingProgressPreserved(ctx, t, db, userID, orig.VolumeID, orig.ID, 7)

			events := loadOutboxEventsDetailed(ctx, t, db)
			if len(events) != 2 || events[1].Kind != domain.EventAssetMoved || events[1].AggregateID != orig.ID {
				t.Fatalf("expected [asset.added, asset.moved], got %+v", events)
			}
			payload, err := domain.DecodeEventPayload[domain.AssetEventPayloadV1](events[1])
			if err != nil {
				t.Fatalf("DecodeEventPayload(asset.moved): %v", err)
			}
			if payload.AssetID != orig.ID || payload.VolumeID != orig.VolumeID || payload.LibraryID != lib.ID ||
				payload.LibraryRootID != root.ID || payload.PreviousPathDisplay != tc.oldPath || payload.PathDisplay != tc.newPath {
				t.Fatalf("unexpected asset.moved payload: %+v", payload)
			}

			assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 1)
		})
	}
}

func TestReconcile_MoveAcrossRootsAndDirectoryRename(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	rootA := filepath.Join(base, "root-a")
	rootB := filepath.Join(base, "root-b")

	cbzCrossA := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("cross-a"), method: zip.Store})
	cbzCrossB := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("cross-b"), method: zip.Store})
	cbzDir1 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("dir-1"), method: zip.Store})
	cbzDir2 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("dir-2"), method: zip.Store})
	cbzDir3 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("dir-3"), method: zip.Store})

	writeDiskFile(t, filepath.Join(rootA, "Inbox/cross_a.cbz"), cbzCrossA)
	writeDiskFile(t, filepath.Join(rootB, "Inbox/cross_b.cbz"), cbzCrossB)
	writeDiskFile(t, filepath.Join(rootA, "OldDir/v1.cbz"), cbzDir1)
	writeDiskFile(t, filepath.Join(rootA, "OldDir/v2.cbz"), cbzDir2)
	writeDiskFile(t, filepath.Join(rootA, "OldDir/v3.cbz"), cbzDir3)

	var (
		lib   domain.Library
		roots []domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Cross-Root Lib", nil, []byte(rootA), []byte(rootB))
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	res1, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil || !res1.Complete || res1.IndexedCount != 5 {
		t.Fatalf("initial scan = (%+v, %v)", res1, err)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	beforeAssets, err := cw.ListAssetsByLibrary(ctx, lib.ID)
	if err != nil || len(beforeAssets) != 5 {
		t.Fatalf("ListAssetsByLibrary = (%d, %v)", len(beforeAssets), err)
	}
	byRel := make(map[string]domain.MediaAsset, 5)
	for _, a := range beforeAssets {
		byRel[a.RelPathDisplay] = a
	}
	userID := seedUserAndReadingProgress(ctx, t, db, transactor, "multi-reader", byRel["Inbox/cross_a.cbz"], 3)

	// Move across roots in both directions and rename OldDir -> NewDir.
	mustRenameDiskFile(t, filepath.Join(rootA, "Inbox/cross_a.cbz"), filepath.Join(rootB, "Archive/cross_a.cbz"))
	mustRenameDiskFile(t, filepath.Join(rootB, "Inbox/cross_b.cbz"), filepath.Join(rootA, "Archive/cross_b.cbz"))
	mustRenameDiskFile(t, filepath.Join(rootA, "OldDir"), filepath.Join(rootA, "NewDir"))

	res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("reconcile scan failed: %v", err)
	}
	if !res2.Complete || res2.MovedCount != 5 || res2.IndexedCount != 0 || res2.ModifiedCount != 0 {
		t.Fatalf("unexpected move scan result: %+v", res2)
	}

	afterAssets, err := cw.ListAssetsByLibrary(ctx, lib.ID)
	if err != nil || len(afterAssets) != 5 {
		t.Fatalf("after ListAssetsByLibrary = (%d, %v)", len(afterAssets), err)
	}
	byID := make(map[string]domain.MediaAsset, 5)
	for _, a := range afterAssets {
		byID[a.ID] = a
	}

	expectedMoves := []struct {
		oldRel  string
		newRoot string
		newRel  string
	}{
		{"Inbox/cross_a.cbz", roots[1].ID, "Archive/cross_a.cbz"},
		{"Inbox/cross_b.cbz", roots[0].ID, "Archive/cross_b.cbz"},
		{"OldDir/v1.cbz", roots[0].ID, "NewDir/v1.cbz"},
		{"OldDir/v2.cbz", roots[0].ID, "NewDir/v2.cbz"},
		{"OldDir/v3.cbz", roots[0].ID, "NewDir/v3.cbz"},
	}
	for _, exp := range expectedMoves {
		orig := byRel[exp.oldRel]
		got := byID[orig.ID]
		if got.VolumeID != orig.VolumeID || got.LibraryRootID != exp.newRoot || got.RelPathDisplay != exp.newRel {
			t.Fatalf("asset %s (%s) = root=%s path=%q, want root=%s path=%q",
				orig.ID, exp.oldRel, got.LibraryRootID, got.RelPathDisplay, exp.newRoot, exp.newRel)
		}
	}
	assertReadingProgressPreserved(ctx, t, db, userID, byRel["Inbox/cross_a.cbz"].VolumeID, byRel["Inbox/cross_a.cbz"].ID, 3)

	events := loadOutboxEventsDetailed(ctx, t, db)
	movedEvents := 0
	for _, ev := range events {
		if ev.Kind == domain.EventAssetMoved {
			movedEvents++
		}
	}
	if movedEvents != 5 {
		t.Fatalf("expected 5 asset.moved events, got %d", movedEvents)
	}

	assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 5)
}

func TestReconcile_MoveViaHistoricalDigest(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	cbzV1 := buildDeterministicZip(t, "", zipEntry{name: "001.jxl", data: []byte("version-1-jxl"), method: zip.Store})
	cbzV2 := buildDeterministicZip(t, "",
		zipEntry{name: "001.avif", data: []byte("version-2-modified-page-1"), method: zip.Store},
		zipEntry{name: "002.png", data: []byte("version-2-modified-page-2"), method: zip.Store},
	)

	oldFile := filepath.Join(rootDir, "v01.cbz")
	writeDiskFile(t, oldFile, cbzV1)

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Historical Move Lib", nil, []byte(rootDir))
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
		t.Fatalf("scan 1 = (%+v, %v)", res, err)
	}

	orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]
	v1Digest, ok := orig.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok {
		t.Fatalf("missing initial partial digest on orig asset %+v", orig)
	}
	if orig.PageCount != 1 || !slices.Equal(orig.Diagnostics.ImageCodecs, []string{"jxl"}) {
		t.Fatalf("unexpected initial layout/codecs: page_count=%d codecs=%v", orig.PageCount, orig.Diagnostics.ImageCodecs)
	}
	userID := seedUserAndReadingProgress(ctx, t, db, transactor, "hist-reader", orig, 4)

	// External modify v01.cbz -> 2-page cbzV2 so cbzV1 digest becomes historical (is_current=false).
	writeDiskFile(t, oldFile, cbzV2)
	future := time.Now().Add(2 * time.Second).UTC()
	if err := os.Chtimes(oldFile, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.ModifiedCount != 1 {
		t.Fatalf("scan 2 (modify) = (%+v, %v)", res, err)
	}

	cwRepo := postgres.NewCatalogueWriteRepository(db.Pool)
	afterMod, err := cwRepo.GetAssetByID(ctx, orig.ID)
	if err != nil {
		t.Fatalf("GetAssetByID(afterMod): %v", err)
	}
	v2Digest, ok := afterMod.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok || v2Digest.Value == v1Digest.Value {
		t.Fatalf("expected rotated V2 current digest, got (%+v, %v)", v2Digest, ok)
	}
	if afterMod.PageCount != 2 || !slices.Equal(afterMod.Diagnostics.ImageCodecs, []string{"avif"}) {
		t.Fatalf("unexpected V2 layout/codecs: page_count=%d codecs=%v", afterMod.PageCount, afterMod.Diagnostics.ImageCodecs)
	}

	// Remove v01.cbz and place original 1-page cbzV1 bytes at a new path -> must match historical
	// digest, move, promote V1 back to current digest while keeping V2 in history, and update layout.
	if err := os.Remove(oldFile); err != nil {
		t.Fatalf("remove oldFile: %v", err)
	}
	movedFile := filepath.Join(rootDir, "Archive/v01_original.cbz")
	writeDiskFile(t, movedFile, cbzV1)

	res3, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("scan 3 (historical move): %v", err)
	}
	if !res3.Complete || !res3.ApplyRemovals || res3.MovedCount != 1 || res3.IndexedCount != 0 || res3.ModifiedCount != 0 {
		t.Fatalf("unexpected scan 3 result: %+v", res3)
	}

	moved, err := cwRepo.GetAssetByID(ctx, orig.ID)
	if err != nil {
		t.Fatalf("GetAssetByID(moved): %v", err)
	}
	if moved.RelPathDisplay != "Archive/v01_original.cbz" || moved.VolumeID != orig.VolumeID {
		t.Fatalf("expected moved path keeping volume %s, got %+v", orig.VolumeID, moved)
	}
	if moved.PageCount != 1 || !slices.Equal(moved.Diagnostics.ImageCodecs, []string{"jxl"}) || moved.FileSize != int64(len(cbzV1)) {
		t.Fatalf("expected 1-page JXL file facts after historical move, got page_count=%d codecs=%v size=%d",
			moved.PageCount, moved.Diagnostics.ImageCodecs, moved.FileSize)
	}
	curAfterMove, ok := moved.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok || curAfterMove.Value != v1Digest.Value || !curAfterMove.IsCurrent || curAfterMove.Reason != domain.DigestReasonScan {
		t.Fatalf("expected current digest promoted back to V1 (%s), got (%+v, %v)", v1Digest.Value, curAfterMove, ok)
	}
	if len(moved.Digests) != 2 {
		t.Fatalf("expected 2 retained digests, got %+v", moved.Digests)
	}
	foundHistoricalV2 := false
	for _, d := range moved.Digests {
		if d.Kind == v2Digest.Kind && d.Value == v2Digest.Value {
			if d.IsCurrent || d.Reason != domain.DigestReasonExternalModify {
				t.Fatalf("expected historical V2 digest demoted with EXTERNAL_MODIFY reason, got %+v", d)
			}
			foundHistoricalV2 = true
		}
	}
	if !foundHistoricalV2 {
		t.Fatalf("expected V2 digest %s retained in asset_digests history, got %+v", v2Digest.Value, moved.Digests)
	}

	var volPageCount int
	if err := db.QueryRow(ctx, `SELECT page_count FROM volumes WHERE id = $1`, orig.VolumeID).Scan(&volPageCount); err != nil {
		t.Fatalf("query volume page_count: %v", err)
	}
	if volPageCount != 1 {
		t.Fatalf("expected volume page_count synced back to 1, got %d", volPageCount)
	}

	assertReadingProgressPreserved(ctx, t, db, userID, orig.VolumeID, orig.ID, 4)
	assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 1)
}

func TestReconcile_PathExchangeAndRenameChain(t *testing.T) {
	t.Parallel()

	t.Run("same-root path exchange", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		bytesA := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("short-A"), method: zip.Store})
		bytesB := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("longer-content-for-B"), method: zip.Store})
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
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Swap Same Root", nil, []byte(rootDir))
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

		before := listAssetsForRoot(ctx, t, transactor, root.ID)
		assetA, assetB := before[0], before[1]
		if assetA.RelPathDisplay != "a.cbz" {
			assetA, assetB = assetB, assetA
		}
		userA := seedUserAndReadingProgress(ctx, t, db, transactor, "swap-user-a", assetA, 2)
		userB := seedUserAndReadingProgress(ctx, t, db, transactor, "swap-user-b", assetB, 9)

		// Exchange a.cbz <-> b.cbz.
		tmpPath := filepath.Join(t.TempDir(), "swap.tmp")
		mustRenameDiskFile(t, pathA, tmpPath)
		mustRenameDiskFile(t, pathB, pathA)
		mustRenameDiskFile(t, tmpPath, pathB)

		res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("swap scan failed: %v", err)
		}
		if !res2.Complete || res2.MovedCount != 2 || res2.ModifiedCount != 0 || res2.IndexedCount != 0 {
			t.Fatalf("unexpected swap result: %+v", res2)
		}

		cw := postgres.NewCatalogueWriteRepository(db.Pool)
		gotA, err := cw.GetAssetByID(ctx, assetA.ID)
		if err != nil {
			t.Fatalf("GetAssetByID(A): %v", err)
		}
		gotB, err := cw.GetAssetByID(ctx, assetB.ID)
		if err != nil {
			t.Fatalf("GetAssetByID(B): %v", err)
		}
		if gotA.RelPathDisplay != "b.cbz" || gotB.RelPathDisplay != "a.cbz" {
			t.Fatalf("expected swapped paths A->b.cbz B->a.cbz, got A=%q B=%q", gotA.RelPathDisplay, gotB.RelPathDisplay)
		}
		assertReadingProgressPreserved(ctx, t, db, userA, assetA.VolumeID, assetA.ID, 2)
		assertReadingProgressPreserved(ctx, t, db, userB, assetB.VolumeID, assetB.ID, 9)

		movedByAsset := make(map[string]domain.AssetEventPayloadV1)
		for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
			if ev.Kind == domain.EventAssetMoved {
				p, err := domain.DecodeEventPayload[domain.AssetEventPayloadV1](ev)
				if err != nil {
					t.Fatalf("DecodeEventPayload: %v", err)
				}
				movedByAsset[ev.AggregateID] = p
			}
		}
		if len(movedByAsset) != 2 ||
			movedByAsset[assetA.ID].PreviousPathDisplay != "a.cbz" || movedByAsset[assetA.ID].PathDisplay != "b.cbz" ||
			movedByAsset[assetB.ID].PreviousPathDisplay != "b.cbz" || movedByAsset[assetB.ID].PathDisplay != "a.cbz" {
			t.Fatalf("unexpected swap asset.moved payloads: %+v", movedByAsset)
		}

		assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 2)
	})

	t.Run("cross-root exchange and rename chain", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		base := t.TempDir()
		rootA := filepath.Join(base, "rA")
		rootB := filepath.Join(base, "rB")

		// Two files across roots for swap, and two files in rootA for chain (c1->c2->c3).
		bytesX := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("swap-X-1"), method: zip.Store})
		bytesY := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("swap-Y-longer-22"), method: zip.Store})
		bytesC1 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("chain-1"), method: zip.Store})
		bytesC2 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("chain-2-longer-content"), method: zip.Store})

		writeDiskFile(t, filepath.Join(rootA, "x.cbz"), bytesX)
		writeDiskFile(t, filepath.Join(rootB, "y.cbz"), bytesY)
		writeDiskFile(t, filepath.Join(rootA, "c1.cbz"), bytesC1)
		writeDiskFile(t, filepath.Join(rootA, "c2.cbz"), bytesC2)

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Chain Swap Lib", nil, []byte(rootA), []byte(rootB))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 4 {
			t.Fatalf("initial scan = (%+v, %v)", res, err)
		}

		cw := postgres.NewCatalogueWriteRepository(db.Pool)
		allBefore, err := cw.ListAssetsByLibrary(ctx, lib.ID)
		if err != nil || len(allBefore) != 4 {
			t.Fatalf("ListAssetsByLibrary = (%d, %v)", len(allBefore), err)
		}
		byName := make(map[string]domain.MediaAsset, 4)
		for _, a := range allBefore {
			byName[a.RelPathDisplay] = a
		}

		// Cross-root swap x.cbz <-> y.cbz, plus rename chain c2.cbz -> c3.cbz, c1.cbz -> c2.cbz.
		tmp := filepath.Join(t.TempDir(), "tmp.cbz")
		mustRenameDiskFile(t, filepath.Join(rootA, "x.cbz"), tmp)
		mustRenameDiskFile(t, filepath.Join(rootB, "y.cbz"), filepath.Join(rootA, "x.cbz"))
		mustRenameDiskFile(t, tmp, filepath.Join(rootB, "y.cbz"))

		mustRenameDiskFile(t, filepath.Join(rootA, "c2.cbz"), filepath.Join(rootA, "c3.cbz"))
		mustRenameDiskFile(t, filepath.Join(rootA, "c1.cbz"), filepath.Join(rootA, "c2.cbz"))

		res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("chain+swap scan failed: %v", err)
		}
		if !res2.Complete || res2.MovedCount != 4 || res2.ModifiedCount != 0 || res2.IndexedCount != 0 {
			t.Fatalf("unexpected chain+swap result: %+v", res2)
		}

		gotX, _ := cw.GetAssetByID(ctx, byName["x.cbz"].ID)
		gotY, _ := cw.GetAssetByID(ctx, byName["y.cbz"].ID)
		gotC1, _ := cw.GetAssetByID(ctx, byName["c1.cbz"].ID)
		gotC2, _ := cw.GetAssetByID(ctx, byName["c2.cbz"].ID)
		if gotX.LibraryRootID != roots[1].ID || gotX.RelPathDisplay != "y.cbz" {
			t.Fatalf("expected X -> rootB/y.cbz, got root=%s path=%q", gotX.LibraryRootID, gotX.RelPathDisplay)
		}
		if gotY.LibraryRootID != roots[0].ID || gotY.RelPathDisplay != "x.cbz" {
			t.Fatalf("expected Y -> rootA/x.cbz, got root=%s path=%q", gotY.LibraryRootID, gotY.RelPathDisplay)
		}
		if gotC1.RelPathDisplay != "c2.cbz" || gotC2.RelPathDisplay != "c3.cbz" {
			t.Fatalf("expected chain c1->c2, c2->c3; got c1=%q c2=%q", gotC1.RelPathDisplay, gotC2.RelPathDisplay)
		}
		assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 4)
	})
}

func TestReconcile_ExternalModifyPreservesLockedFieldsAndTouchRefreshesMtime(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	cbzOrig := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<?xml version="1.0" encoding="utf-8"?>
<ComicInfo>
  <Series>Chronicle</Series>
  <Number>1</Number>
  <Title>Original Title</Title>
  <Summary>Original Summary</Summary>
  <Publisher>Original Publisher</Publisher>
  <Imprint>Original Imprint</Imprint>
  <Writer>Alice Writer</Writer>
  <Inker>Temp Inker</Inker>
  <Characters>Hero Alpha</Characters>
  <Locations>Old City</Locations>
</ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("page-1"), method: zip.Store},
	)
	cbzTouch := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("touch-only"), method: zip.Store})

	modFile := filepath.Join(rootDir, "chronicle_01.cbz")
	touchFile := filepath.Join(rootDir, "touch_only.cbz")
	writeDiskFile(t, modFile, cbzOrig)
	writeDiskFile(t, touchFile, cbzTouch)

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Modify Lib", nil, []byte(rootDir))
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

	assets := listAssetsForRoot(ctx, t, transactor, root.ID)
	var modAsset, touchAsset domain.MediaAsset
	for _, a := range assets {
		if a.RelPathDisplay == "chronicle_01.cbz" {
			modAsset = a
		} else {
			touchAsset = a
		}
	}
	userID := seedUserAndReadingProgress(ctx, t, db, transactor, "mod-reader", modAsset, 1)

	// Lock volume.title, series.publisher, primary membership (MANUAL), Alice Writer credit, and Hero Alpha entity link.
	var primarySeriesID string
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		vol, err := cw.GetVolumeByIDForUpdate(ctx, modAsset.VolumeID)
		if err != nil {
			return err
		}
		vol.LockedFields = vol.LockedFields.Lock("title")
		if err := cw.UpdateVolume(ctx, vol); err != nil {
			return err
		}

		mems, err := tx.SeriesMemberships().ListMembershipsByVolume(ctx, vol.ID)
		if err != nil || len(mems) != 1 {
			return errors.New("expected 1 membership")
		}
		primarySeriesID = mems[0].SeriesID
		if err := tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
			{
				VolumeID:  vol.ID,
				SeriesID:  primarySeriesID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Position:  mems[0].Position,
				Source:    domain.MembershipSourceManual,
			},
		}); err != nil {
			return err
		}

		ser, err := cw.GetSeriesByIDForUpdate(ctx, primarySeriesID)
		if err != nil {
			return err
		}
		ser.LockedFields = ser.LockedFields.Lock("publisher")
		if err := cw.UpdateSeriesMetadata(ctx, ser); err != nil {
			return err
		}

		// Lock Alice Writer credit and Hero Alpha volume_entity via raw SQL on this test DB.
		return nil
	}); err != nil {
		t.Fatalf("lock setup tx failed: %v", err)
	}
	if _, err := db.Exec(ctx, `
		UPDATE credits SET locked_fields = '{"*": true}'::jsonb
		WHERE volume_id = $1 AND role = 'WRITER'`, modAsset.VolumeID); err != nil {
		t.Fatalf("lock credit: %v", err)
	}
	if _, err := db.Exec(ctx, `
		UPDATE volume_entities SET locked_fields = '{"*": true}'::jsonb
		WHERE volume_id = $1
		  AND entity_id IN (SELECT id FROM entities WHERE kind = 'CHARACTER' AND name = 'Hero Alpha')`,
		modAsset.VolumeID); err != nil {
		t.Fatalf("lock volume_entity: %v", err)
	}

	// Overwrite modFile with same Series ("Chronicle") + updated metadata and 2 pages,
	// and touch touchFile's mtime without changing contents.
	cbzUpdated := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<?xml version="1.0" encoding="utf-8"?>
<ComicInfo>
  <Series>Chronicle</Series>
  <Number>2</Number>
  <Title>Overwritten Title</Title>
  <Summary>Updated Summary</Summary>
  <Publisher>New Publisher</Publisher>
  <Imprint>New Imprint</Imprint>
  <Writer>Bob Writer</Writer>
  <Characters>Hero Beta</Characters>
</ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("page-1-updated"), method: zip.Store},
		zipEntry{name: "002.jpg", data: []byte("page-2-new"), method: zip.Store},
	)
	writeDiskFile(t, modFile, cbzUpdated)
	future := time.Now().Add(4 * time.Second).UTC()
	if err := os.Chtimes(modFile, future, future); err != nil {
		t.Fatalf("chtimes modFile: %v", err)
	}
	if err := os.Chtimes(touchFile, future, future); err != nil {
		t.Fatalf("chtimes touchFile: %v", err)
	}

	res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("modify scan failed: %v", err)
	}
	if !res2.Complete || res2.ModifiedCount != 1 || res2.MovedCount != 0 || res2.IndexedCount != 0 || res2.ChangedCount != 2 {
		t.Fatalf("unexpected modify scan result: %+v", res2)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	gotVol, _ := cw.GetVolumeByID(ctx, modAsset.VolumeID)
	if gotVol.Title == nil || *gotVol.Title != "Original Title" {
		t.Fatalf("locked volume.title was overwritten: %v", gotVol.Title)
	}
	if gotVol.Summary == nil || *gotVol.Summary != "Updated Summary" || gotVol.PageCount != 2 {
		t.Fatalf("unlocked volume fields did not update: %+v", gotVol)
	}
	gotSer, _ := cw.GetSeriesByID(ctx, primarySeriesID)
	if gotSer.Publisher == nil || *gotSer.Publisher != "Original Publisher" {
		t.Fatalf("locked series.publisher was overwritten: %v", gotSer.Publisher)
	}
	if gotSer.Imprint == nil || *gotSer.Imprint != "New Imprint" {
		t.Fatalf("unlocked series.imprint did not update: %v", gotSer.Imprint)
	}

	// Verify Alice Writer stayed (locked) and Bob Writer was added, while Temp Inker was removed.
	credits, err := postgres.NewCreditRepository(db.Pool).ListCreditsByVolume(ctx, modAsset.VolumeID)
	if err != nil {
		t.Fatalf("ListCreditsByVolume: %v", err)
	}
	var writerCount, inkerCount int
	for _, c := range credits {
		switch c.Role {
		case domain.CreditRoleWriter:
			writerCount++
		case domain.CreditRoleInker:
			inkerCount++
		}
	}
	if writerCount != 2 || inkerCount != 0 {
		t.Fatalf("expected 2 writers (locked Alice + Bob) and 0 inkers, got credits=%+v", credits)
	}

	// Verify Hero Alpha stayed (locked), Hero Beta added, Old City removed.
	links, err := postgres.NewEntityRepository(db.Pool).ListVolumeEntities(ctx, modAsset.VolumeID)
	if err != nil {
		t.Fatalf("ListVolumeEntities: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("expected 2 entity links (locked Hero Alpha + Hero Beta), got %+v", links)
	}

	// Verify touchFile did NOT rotate digest or emit asset.modified, and updated mtime so next scan opens 0 files.
	touchAfter, _ := cw.GetAssetByID(ctx, touchAsset.ID)
	if len(touchAfter.Digests) != 1 {
		t.Fatalf("touch-only file rotated digests: %+v", touchAfter.Digests)
	}
	assertReadingProgressPreserved(ctx, t, db, userID, modAsset.VolumeID, modAsset.ID, 1)

	// Now overwrite modFile again with a DIFFERENT <Series> ("Orphan Candidate") under MANUAL membership:
	// must NOT change primary membership and must NOT create an orphan series row.
	cbzDiffSeries := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Orphan Candidate</Series><Number>99</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("page-1-v3"), method: zip.Store},
		zipEntry{name: "002.jpg", data: []byte("page-2-v3"), method: zip.Store},
	)
	writeDiskFile(t, modFile, cbzDiffSeries)
	future2 := future.Add(4 * time.Second)
	if err := os.Chtimes(modFile, future2, future2); err != nil {
		t.Fatalf("chtimes modFile v3: %v", err)
	}

	res3, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil || !res3.Complete || res3.ModifiedCount != 1 {
		t.Fatalf("scan 3 (diff series under MANUAL) = (%+v, %v)", res3, err)
	}
	var seriesCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM series WHERE library_id = $1`, lib.ID).Scan(&seriesCount); err != nil {
		t.Fatalf("count series: %v", err)
	}
	if seriesCount != 1 {
		t.Fatalf("expected 1 series (no orphan created under MANUAL membership), got %d", seriesCount)
	}

	assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 2)
}

func TestReconcile_CopyWhileOriginalRemains_AndCrossLibraryReappearance(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	rootA := filepath.Join(base, "lib-a")
	rootB := filepath.Join(base, "lib-b")
	cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("shared-bytes"), method: zip.Store})
	writeDiskFile(t, filepath.Join(rootA, "orig.cbz"), cbzBytes)
	writeDiskFile(t, filepath.Join(rootB, "initial.cbz"), buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("b-only"), method: zip.Store}))

	var (
		libA, libB     domain.Library
		rootAR, rootBR domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var rA, rB []domain.LibraryRoot
		libA, rA = seedLibraryAndRoots(ctx, t, tx, "Copy Lib A", nil, []byte(rootA))
		libB, rB = seedLibraryAndRoots(ctx, t, tx, "Copy Lib B", nil, []byte(rootB))
		rootAR, rootBR = rA[0], rB[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, libA.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 1 {
		t.Fatalf("initial scan A = (%+v, %v)", res, err)
	}
	if res, err := sc.ScanLibrary(ctx, libB.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 1 {
		t.Fatalf("initial scan B = (%+v, %v)", res, err)
	}
	origA := listAssetsForRoot(ctx, t, transactor, rootAR.ID)[0]

	// Add copies sorting both before ("aaa_copy.cbz") and after ("zzz_copy.cbz") orig.cbz in libA,
	// AND add the same bytes into libB ("from_a.cbz").
	writeDiskFile(t, filepath.Join(rootA, "aaa_copy.cbz"), cbzBytes)
	writeDiskFile(t, filepath.Join(rootA, "zzz_copy.cbz"), cbzBytes)
	writeDiskFile(t, filepath.Join(rootB, "from_a.cbz"), cbzBytes)

	resA2, err := sc.ScanLibrary(ctx, libA.ID, scanner.ScanOptions{})
	if err != nil || !resA2.Complete || resA2.IndexedCount != 2 || resA2.MovedCount != 0 || resA2.UnchangedCount != 1 {
		t.Fatalf("libA copy scan = (%+v, %v)", resA2, err)
	}
	resB2, err := sc.ScanLibrary(ctx, libB.ID, scanner.ScanOptions{})
	if err != nil || !resB2.Complete || resB2.IndexedCount != 1 || resB2.MovedCount != 0 {
		t.Fatalf("libB copy scan = (%+v, %v)", resB2, err)
	}

	assetsA := listAssetsForRoot(ctx, t, transactor, rootAR.ID)
	assetsB := listAssetsForRoot(ctx, t, transactor, rootBR.ID)
	if len(assetsA) != 3 || len(assetsB) != 2 {
		t.Fatalf("unexpected asset counts A=%d B=%d (want 3, 2)", len(assetsA), len(assetsB))
	}
	var keptOrig bool
	for _, a := range assetsA {
		if a.ID == origA.ID && a.RelPathDisplay == "orig.cbz" {
			keptOrig = true
		}
	}
	if !keptOrig {
		t.Fatalf("original asset %s moved despite still existing on disk: %+v", origA.ID, assetsA)
	}

	for _, ev := range loadOutboxEventsDetailed(ctx, t, db) {
		if ev.Kind == domain.EventAssetMoved {
			t.Fatalf("unexpected asset.moved event when copies were added: %+v", ev)
		}
	}
}

func TestReconcile_IncompleteWalkReconcilesNothing(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	bKeep := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("keep"), method: zip.Store})
	bRename := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("rename"), method: zip.Store})
	bMod := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("mod-v1"), method: zip.Store})
	bSwap1 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("swap-1"), method: zip.Store})
	bSwap2 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("swap-2-longer"), method: zip.Store})

	writeDiskFile(t, filepath.Join(rootDir, "locked_dir/keep.cbz"), bKeep)
	writeDiskFile(t, filepath.Join(rootDir, "rename_me.cbz"), bRename)
	writeDiskFile(t, filepath.Join(rootDir, "modify_me.cbz"), bMod)
	writeDiskFile(t, filepath.Join(rootDir, "swap1.cbz"), bSwap1)
	writeDiskFile(t, filepath.Join(rootDir, "swap2.cbz"), bSwap2)

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Incomplete Walk Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 5 {
		t.Fatalf("initial scan = (%+v, %v)", res, err)
	}

	beforeEvents := len(loadOutboxEventsDetailed(ctx, t, db))
	beforeAssets := listAssetsForRoot(ctx, t, transactor, root.ID)

	// Mutate disk: rename, modify, and swap.
	mustRenameDiskFile(t, filepath.Join(rootDir, "rename_me.cbz"), filepath.Join(rootDir, "renamed.cbz"))
	writeDiskFile(t, filepath.Join(rootDir, "modify_me.cbz"), buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("mod-v2-changed"), method: zip.Store}))
	future := time.Now().Add(2 * time.Second).UTC()
	if err := os.Chtimes(filepath.Join(rootDir, "modify_me.cbz"), future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	tmp := filepath.Join(t.TempDir(), "s.tmp")
	mustRenameDiskFile(t, filepath.Join(rootDir, "swap1.cbz"), tmp)
	mustRenameDiskFile(t, filepath.Join(rootDir, "swap2.cbz"), filepath.Join(rootDir, "swap1.cbz"))
	mustRenameDiskFile(t, tmp, filepath.Join(rootDir, "swap2.cbz"))

	// Scan with unreadable locked_dir -> incomplete walk must reconcile nothing!
	resInc, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
			if filepath.Base(string(rawDir)) == "locked_dir" {
				return nil, os.ErrPermission
			}
			return os.ReadDir(string(rawDir))
		},
	})
	if err != nil {
		t.Fatalf("incomplete scan unexpected error: %v", err)
	}
	if resInc.Complete || resInc.ApplyRemovals || resInc.MovedCount != 0 || resInc.ModifiedCount != 0 || resInc.IndexedCount != 0 {
		t.Fatalf("incomplete walk reconciled unexpectedly: %+v", resInc)
	}
	if len(loadOutboxEventsDetailed(ctx, t, db)) != beforeEvents {
		t.Fatalf("expected no outbox events during incomplete walk")
	}
	duringAssets := listAssetsForRoot(ctx, t, transactor, root.ID)
	if len(duringAssets) != len(beforeAssets) {
		t.Fatalf("asset count changed during incomplete walk")
	}
	for i := range beforeAssets {
		if !bytes.Equal(beforeAssets[i].RelPathBytes, duringAssets[i].RelPathBytes) {
			t.Fatalf("asset path changed during incomplete walk: %q -> %q", beforeAssets[i].RelPathBytes, duringAssets[i].RelPathBytes)
		}
	}

	// Now run a complete scan -> everything reconciles cleanly.
	resFull, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("complete scan failed: %v", err)
	}
	if !resFull.Complete || resFull.MovedCount != 3 || resFull.ModifiedCount != 1 || resFull.IndexedCount != 0 {
		t.Fatalf("expected 3 moves + 1 modify on complete rescan, got %+v", resFull)
	}
	assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 5)
}

func TestReconcile_OfflineLibraryStopsBeforeWalkAndNeverDuplicatesOfflineAsset(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	root1 := filepath.Join(base, "r1")
	root2 := filepath.Join(base, "r2")
	book1Bytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("book-one"), method: zip.Store})
	book2Bytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("book-two"), method: zip.Store})
	writeDiskFile(t, filepath.Join(root1, "book1.cbz"), book1Bytes)
	writeDiskFile(t, filepath.Join(root2, "book2.cbz"), book2Bytes)

	var lib domain.Library
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ = seedLibraryAndRoots(ctx, t, tx, "Offline D10 Lib", nil, []byte(root1), []byte(root2))
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

	// Make root1 disappear (OFFLINE) AND place book1's exact bytes into root2.
	if err := os.RemoveAll(root1); err != nil {
		t.Fatalf("RemoveAll(root1): %v", err)
	}
	writeDiskFile(t, filepath.Join(root2, "reappeared_from_r1.cbz"), book1Bytes)

	var opens atomic.Int32
	resOffline, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			opens.Add(1)
			return os.Open(string(rawPath))
		},
	})
	if err != nil {
		t.Fatalf("offline scan returned error: %v", err)
	}
	if !resOffline.Offline || resOffline.Complete || resOffline.ApplyRemovals ||
		resOffline.IndexedCount != 0 || resOffline.MovedCount != 0 || opens.Load() != 0 || len(resOffline.Roots) != 0 {
		t.Fatalf("expected M1B-D10 early offline stop with 0 opens/writes, got res=%+v opens=%d", resOffline, opens.Load())
	}

	allAssets, err := postgres.NewCatalogueWriteRepository(db.Pool).ListAssetsByLibrary(ctx, lib.ID)
	if err != nil {
		t.Fatalf("ListAssetsByLibrary: %v", err)
	}
	if len(allAssets) != 2 {
		t.Fatalf("expected still 2 assets (never create duplicate for offline root asset), got %d", len(allAssets))
	}
	events := loadOutboxEventsDetailed(ctx, t, db)
	if events[len(events)-1].Kind != domain.EventLibraryOffline {
		t.Fatalf("expected trailing library.offline event, got %+v", events)
	}
}

func TestReconcile_InvalidUTF8MoveDiagnostic(t *testing.T) {
	t.Parallel()

	t.Run("synthetic across all platforms", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("utf8-move"), method: zip.Store})
		fakeRoot := []byte("/synthetic/utf8-move-root")
		now := time.Unix(1700000000, 0).UTC()

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "UTF8 Move Synth", nil, fakeRoot)
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		// Controlled synthetic directory state: stage 0 = valid, stage 1 = invalid1, stage 2 = invalid2, stage 3 = valid2.
		currentDirName := "valid"
		currentFileName := "book.cbz"
		currentMtime := now

		scanHooks := scanner.ScanOptions{
			ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
				if string(rawDir) == string(fakeRoot) {
					return []os.DirEntry{syntheticDirEntry{name: currentDirName, mode: fs.ModeDir, mtime: currentMtime}}, nil
				}
				subDir := string(append(append([]byte(nil), fakeRoot...), append([]byte{'/'}, []byte(currentDirName)...)...))
				if string(rawDir) == subDir {
					return []os.DirEntry{syntheticDirEntry{name: currentFileName, mode: 0, size: int64(len(cbzBytes)), mtime: currentMtime}}, nil
				}
				return nil, os.ErrNotExist
			},
			Stat: func(rawPath []byte) (os.FileInfo, error) {
				if string(rawPath) == string(fakeRoot) {
					return syntheticFileInfo{name: "utf8-move-root", mode: fs.ModeDir | 0o755, mtime: currentMtime, id: 1}, nil
				}
				subDir := string(append(append([]byte(nil), fakeRoot...), append([]byte{'/'}, []byte(currentDirName)...)...))
				if string(rawPath) == subDir {
					return syntheticFileInfo{name: currentDirName, mode: fs.ModeDir | 0o755, mtime: currentMtime, id: 2}, nil
				}
				return syntheticFileInfo{name: currentFileName, mode: 0o644, size: int64(len(cbzBytes)), mtime: currentMtime, id: 3}, nil
			},
			Open: func(_ context.Context, _ []byte) (scanner.ReadAtCloser, error) {
				return &bytesReadAtCloser{Reader: bytes.NewReader(cbzBytes)}, nil
			},
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}

		// 1. Initial valid UTF-8 index.
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanHooks); err != nil || !res.Complete || res.IndexedCount != 1 {
			t.Fatalf("stage 0 scan = (%+v, %v)", res, err)
		}
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		// 2. Move valid -> invalid UTF-8: sets flag and emits BOTH asset.moved and asset.invalid_utf8_path.
		currentDirName = string([]byte{0xff, 0xfe})
		currentFileName = string([]byte{'b', 'o', 'o', 'k', 0x80, '.', 'c', 'b', 'z'})
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanHooks); err != nil || !res.Complete || res.MovedCount != 1 {
			t.Fatalf("stage 1 scan = (%+v, %v)", res, err)
		}
		got1 := listAssetsForRoot(ctx, t, transactor, root.ID)[0]
		if got1.ID != orig.ID || !got1.Diagnostics.InvalidUTF8Path || got1.RelPathDisplay != `\xff\xfe/book\x80.cbz` {
			t.Fatalf("unexpected stage 1 asset: %+v", got1)
		}
		events1 := loadOutboxEventsDetailed(ctx, t, db)
		if len(events1) != 3 || events1[1].Kind != domain.EventAssetMoved || events1[2].Kind != domain.EventAssetInvalidUTF8Path {
			t.Fatalf("stage 1 expected [added, moved, invalid_utf8_path], got %+v", events1)
		}
		invPayload, err := domain.DecodeEventPayload[domain.AssetEventPayloadV1](events1[2])
		if err != nil || invPayload.PathDisplay != `\xff\xfe/book\x80.cbz` || invPayload.PreviousPathDisplay != "" {
			t.Fatalf("unexpected invalid_utf8_path payload: %+v err=%v", invPayload, err)
		}

		// 3. Move invalid -> different invalid UTF-8: emits asset.moved ONLY (no duplicate invalid_utf8_path).
		currentDirName = string([]byte{0xfe})
		currentFileName = string([]byte{'o', 't', 'h', 'e', 'r', 0x81, '.', 'c', 'b', 'z'})
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanHooks); err != nil || !res.Complete || res.MovedCount != 1 {
			t.Fatalf("stage 2 scan = (%+v, %v)", res, err)
		}
		events2 := loadOutboxEventsDetailed(ctx, t, db)
		if len(events2) != 4 || events2[3].Kind != domain.EventAssetMoved {
			t.Fatalf("stage 2 expected single asset.moved without duplicate invalid_utf8_path, got %+v", events2)
		}

		// 4. Move invalid -> valid UTF-8: clears invalid_utf8_path diagnostic and emits asset.moved ONLY.
		currentDirName = "clean"
		currentFileName = "book.cbz"
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanHooks); err != nil || !res.Complete || res.MovedCount != 1 {
			t.Fatalf("stage 3 scan = (%+v, %v)", res, err)
		}
		got3 := listAssetsForRoot(ctx, t, transactor, root.ID)[0]
		if got3.ID != orig.ID || got3.Diagnostics.InvalidUTF8Path || got3.RelPathDisplay != "clean/book.cbz" {
			t.Fatalf("stage 3 expected cleared InvalidUTF8Path, got %+v", got3)
		}
		events3 := loadOutboxEventsDetailed(ctx, t, db)
		if len(events3) != 5 || events3[4].Kind != domain.EventAssetMoved {
			t.Fatalf("stage 3 expected single asset.moved, got %+v", events3)
		}
	})

	t.Run("on disk when filesystem permits", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		root := []byte(t.TempDir())
		cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("disk-utf8"), method: zip.Store})
		validPath := filepath.Join(string(root), "valid.cbz")
		writeDiskFile(t, validPath, cbzBytes)

		invalidRel := []byte{'b', 'a', 'd', 0x80, '.', 'c', 'b', 'z'}
		invalidAbs, err := paths.JoinRootRelative(root, invalidRel)
		if err != nil {
			t.Fatal(err)
		}

		var lib domain.Library
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, _ = seedLibraryAndRoots(ctx, t, tx, "UTF8 Move Disk", nil, root)
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 1 {
			t.Fatalf("initial disk scan = (%+v, %v)", res, err)
		}

		if err := os.Rename(validPath, string(invalidAbs)); err != nil {
			if runtime.GOOS != "linux" && (errors.Is(err, syscall.EILSEQ) || errors.Is(err, syscall.EINVAL)) {
				t.Skipf("filesystem rejected invalid UTF-8 rename (%v)", err)
			}
			t.Fatalf("Rename to invalid UTF-8: %v", err)
		}

		res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil || !res2.Complete || res2.MovedCount != 1 {
			t.Fatalf("disk invalid UTF-8 move scan = (%+v, %v)", res2, err)
		}
		events := loadOutboxEventsDetailed(ctx, t, db)
		if len(events) != 3 || events[1].Kind != domain.EventAssetMoved || events[2].Kind != domain.EventAssetInvalidUTF8Path {
			t.Fatalf("disk move expected [added, moved, invalid_utf8_path], got %+v", events)
		}
	})
}

func TestReconcile_MovePlusVacatedNewFileAndOverwriteOntoUnmovedPath(t *testing.T) {
	t.Parallel()

	t.Run("move plus new file at vacated path", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		bytesD1 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("d1-short"), method: zip.Store})
		bytesDNew := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("dnew-different-length"), method: zip.Store})

		pathA := filepath.Join(rootDir, "a.cbz")
		pathB := filepath.Join(rootDir, "b.cbz")
		writeDiskFile(t, pathA, bytesD1)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Vacated Path Lib", nil, []byte(rootDir))
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
		origA := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		mustRenameDiskFile(t, pathA, pathB)
		writeDiskFile(t, pathA, bytesDNew)

		res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("scan 2 failed: %v", err)
		}
		if !res2.Complete || res2.MovedCount != 1 || res2.IndexedCount != 1 || res2.ModifiedCount != 0 {
			t.Fatalf("expected 1 move + 1 index, got %+v", res2)
		}

		gotA, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, origA.ID)
		if err != nil || gotA.RelPathDisplay != "b.cbz" {
			t.Fatalf("expected origA at b.cbz, got (%+v, %v)", gotA, err)
		}
		assets := listAssetsForRoot(ctx, t, transactor, root.ID)
		if len(assets) != 2 {
			t.Fatalf("expected 2 assets, got %d", len(assets))
		}
		assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 2)
	})

	t.Run("overwrite onto unmoved path becomes EXTERNAL_MODIFY", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		bytesD1 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("d1-short"), method: zip.Store})
		bytesD2 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("d2-longer-payload"), method: zip.Store})

		pathA := filepath.Join(rootDir, "a.cbz")
		pathB := filepath.Join(rootDir, "b.cbz")
		writeDiskFile(t, pathA, bytesD1)
		writeDiskFile(t, pathB, bytesD2)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Overwrite Lib", nil, []byte(rootDir))
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

		var origA domain.MediaAsset
		for _, a := range listAssetsForRoot(ctx, t, transactor, root.ID) {
			if a.RelPathDisplay == "a.cbz" {
				origA = a
			}
		}

		// Overwrite a.cbz with b.cbz (D2), leaving b.cbz gone and a.cbz holding D2.
		mustRenameDiskFile(t, pathB, pathA)

		res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("overwrite scan failed: %v", err)
		}
		if !res2.Complete || res2.ModifiedCount != 1 || res2.MovedCount != 0 || res2.IndexedCount != 0 {
			t.Fatalf("expected 1 modify + 0 moves + 0 indexes, got %+v", res2)
		}

		gotA, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, origA.ID)
		if err != nil {
			t.Fatalf("GetAssetByID(A): %v", err)
		}
		if gotA.RelPathDisplay != "a.cbz" || len(gotA.Digests) != 2 {
			t.Fatalf("expected A at a.cbz with rotated digest, got %+v", gotA)
		}
		cur, ok := gotA.CurrentDigest(domain.DigestKindXXH364Partial)
		if !ok || cur.Reason != domain.DigestReasonExternalModify {
			t.Fatalf("expected EXTERNAL_MODIFY current digest on A, got (%+v, %v)", cur, ok)
		}
		events := loadOutboxEventsDetailed(ctx, t, db)
		if !slices.ContainsFunc(events, func(e domain.DomainEvent) bool {
			return e.Kind == domain.EventAssetModified && e.AggregateID == origA.ID
		}) {
			t.Fatalf("expected asset.modified event for A (%s), got %+v", origA.ID, events)
		}
	})
}

func TestReconcile_IncompleteWalkDefersNewDigestAdditionsAcrossRoots(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	root1 := filepath.Join(base, "r1")
	root2 := filepath.Join(base, "r2")

	cbzNewInRoot1 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("genuinely-new-digest-root1"), method: zip.Store})
	cbzVisibleInRoot2 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("visible-root2"), method: zip.Store})
	cbzLockedInRoot2 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("locked-root2"), method: zip.Store})

	writeDiskFile(t, filepath.Join(root1, "brand_new.cbz"), cbzNewInRoot1)
	writeDiskFile(t, filepath.Join(root2, "visible.cbz"), cbzVisibleInRoot2)
	writeDiskFile(t, filepath.Join(root2, "locked_dir/hidden.cbz"), cbzLockedInRoot2)

	var (
		lib   domain.Library
		roots []domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Defer New Across Roots Lib", nil, []byte(root1), []byte(root2))
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}

	res1, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
			if filepath.Base(string(rawDir)) == "locked_dir" {
				return nil, os.ErrPermission
			}
			return os.ReadDir(string(rawDir))
		},
	})
	if err != nil {
		t.Fatalf("ScanLibrary incomplete walk: %v", err)
	}
	if res1.Complete || res1.ApplyRemovals || res1.IndexedCount != 0 || res1.SkippedCount != 1 {
		t.Fatalf("expected Complete=false ApplyRemovals=false IndexedCount=0 SkippedCount=1, got %+v", res1)
	}
	if len(listAssetsForRoot(ctx, t, transactor, roots[0].ID)) != 0 ||
		len(listAssetsForRoot(ctx, t, transactor, roots[1].ID)) != 0 {
		t.Fatal("expected zero media_assets committed before all roots walk cleanly")
	}
	if events := loadOutboxEventsDetailed(ctx, t, db); len(events) != 0 {
		t.Fatalf("expected zero outbox events during incomplete walk, got %+v", events)
	}

	// Once root2 becomes readable, rescan completes the walk and indexes all 3 files.
	res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("ScanLibrary complete rescan: %v", err)
	}
	if !res2.Complete || !res2.ApplyRemovals || res2.IndexedCount != 3 || res2.SkippedCount != 0 {
		t.Fatalf("expected Complete=true ApplyRemovals=true IndexedCount=3, got %+v", res2)
	}
	assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 3)
}

type reconcileHookTransactor struct {
	inner             storage.Transactor
	readCommittedCall atomic.Int32
	// beforeSecondReadCommitted fires right before the first post-walk
	// WithReadCommittedTx call (after PreCheckLibrary's call #1).
	beforeSecondReadCommitted func() error
	// onLockVolume calls hook right before GetVolumeByIDForUpdate on the
	// reconciliation transaction.
	onLockVolume func() error
	// wrapUpdateAsset runs hook right after cw.UpdateAsset completes.
	onAfterUpdateAsset func() error
}

func (r *reconcileHookTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return r.inner.WithTx(ctx, fn)
}

func (r *reconcileHookTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	callNum := r.readCommittedCall.Add(1)
	if callNum == 2 && r.beforeSecondReadCommitted != nil {
		if err := r.beforeSecondReadCommitted(); err != nil {
			return err
		}
	}
	return r.inner.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		if callNum == 2 && (r.onLockVolume != nil || r.onAfterUpdateAsset != nil) {
			return fn(&reconcileHookTx{Tx: tx, parent: r})
		}
		return fn(tx)
	})
}

type reconcileHookTx struct {
	storage.Tx
	parent *reconcileHookTransactor
}

func (r *reconcileHookTx) CatalogueWrites() storage.CatalogueWriteRepository {
	return &reconcileHookCatalogueRepo{
		CatalogueWriteRepository: r.Tx.CatalogueWrites(),
		parent:                   r.parent,
	}
}

type reconcileHookCatalogueRepo struct {
	storage.CatalogueWriteRepository
	parent *reconcileHookTransactor
}

func (r *reconcileHookCatalogueRepo) GetVolumeByIDForUpdate(ctx context.Context, id string) (*domain.Volume, error) {
	if r.parent.onLockVolume != nil {
		if err := r.parent.onLockVolume(); err != nil {
			return nil, err
		}
	}
	return r.CatalogueWriteRepository.GetVolumeByIDForUpdate(ctx, id)
}

func (r *reconcileHookCatalogueRepo) UpdateAsset(ctx context.Context, asset *domain.MediaAsset) error {
	if err := r.CatalogueWriteRepository.UpdateAsset(ctx, asset); err != nil {
		return err
	}
	if r.parent.onAfterUpdateAsset != nil {
		return r.parent.onAfterUpdateAsset()
	}
	return nil
}

func TestReconcile_RootSnapshotInvalidatedBeforeWrite(t *testing.T) {
	t.Parallel()

	t.Run("root relocated before move write returns ErrStalePreCheck and blocks stale commit", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		base := t.TempDir()
		oldRootDir := filepath.Join(base, "old-root")
		relocatedDir := filepath.Join(base, "new-root")
		cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("relocate-test"), method: zip.Store})
		writeDiskFile(t, filepath.Join(oldRootDir, "book.cbz"), cbzBytes)
		writeDiskFile(t, filepath.Join(relocatedDir, "other.cbz"), cbzBytes)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Root Relocate Reconcile Lib", nil, []byte(oldRootDir))
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
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		mustRenameDiskFile(t, filepath.Join(oldRootDir, "book.cbz"), filepath.Join(oldRootDir, "book_renamed.cbz"))

		hookTx := &reconcileHookTransactor{
			inner: transactor,
			beforeSecondReadCommitted: func() error {
				return transactor.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
					updated := root
					updated.Path = mustRootPathIdentity(t, []byte(relocatedDir))
					return tx.Libraries().UpdateLibraryRoot(ctx, &updated)
				})
			},
		}
		scHook, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(hook): %v", err)
		}

		res2, err := scHook.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if !errors.Is(err, scanner.ErrStalePreCheck) {
			t.Fatalf("expected ErrStalePreCheck on root relocation before move write, got err=%v res=%+v", err, res2)
		}
		if res2.Complete || res2.ApplyRemovals || res2.MovedCount != 0 {
			t.Fatalf("expected Complete=false ApplyRemovals=false MovedCount=0, got %+v", res2)
		}
		got, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, orig.ID)
		if err != nil || got.RelPathDisplay != "book.cbz" {
			t.Fatalf("expected stale move rolled back leaving book.cbz intact, got (%+v, %v)", got, err)
		}
		events := loadOutboxEventsDetailed(ctx, t, db)
		if len(events) != 1 || events[0].Kind != domain.EventAssetAdded {
			t.Fatalf("expected only initial asset.added event, got %+v", events)
		}
	})

	t.Run("newer OFFLINE verdict before move write returns ErrStalePreCheck and clears ApplyRemovals", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("offline-verdict-test"), method: zip.Store})
		writeDiskFile(t, filepath.Join(rootDir, "book.cbz"), cbzBytes)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Offline Verdict Reconcile Lib", nil, []byte(rootDir))
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
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		mustRenameDiskFile(t, filepath.Join(rootDir, "book.cbz"), filepath.Join(rootDir, "book_renamed.cbz"))

		hookTx := &reconcileHookTransactor{
			inner: transactor,
			beforeSecondReadCommitted: func() error {
				return transactor.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
					msg := "unmounted concurrently"
					return tx.Libraries().UpdateLibraryRootStatus(ctx, root, domain.LibraryRootStateOffline, time.Now().UTC(), &msg)
				})
			},
		}
		scHook, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(hook): %v", err)
		}

		res2, err := scHook.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if !errors.Is(err, scanner.ErrStalePreCheck) {
			t.Fatalf("expected ErrStalePreCheck on newer OFFLINE verdict, got err=%v res=%+v", err, res2)
		}
		if res2.Complete || res2.ApplyRemovals || res2.MovedCount != 0 {
			t.Fatalf("expected Complete=false ApplyRemovals=false MovedCount=0, got %+v", res2)
		}
		got, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, orig.ID)
		if err != nil || got.RelPathDisplay != "book.cbz" {
			t.Fatalf("expected stale move rolled back leaving book.cbz intact, got (%+v, %v)", got, err)
		}
	})

	t.Run("newer OFFLINE verdict on unchanged scan clears ApplyRemovals", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("empty-diff-offline"), method: zip.Store})
		writeDiskFile(t, filepath.Join(rootDir, "book.cbz"), cbzBytes)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Empty Diff Offline Lib", nil, []byte(rootDir))
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

		hookTx := &reconcileHookTransactor{
			inner: transactor,
			beforeSecondReadCommitted: func() error {
				return transactor.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
					msg := "unmounted during unchanged walk"
					return tx.Libraries().UpdateLibraryRootStatus(ctx, root, domain.LibraryRootStateOffline, time.Now().UTC(), &msg)
				})
			},
		}
		scHook, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(hook): %v", err)
		}

		res2, err := scHook.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if !errors.Is(err, scanner.ErrStalePreCheck) {
			t.Fatalf("expected ErrStalePreCheck on unchanged scan with newer OFFLINE verdict, got err=%v res=%+v", err, res2)
		}
		if res2.Complete || res2.ApplyRemovals {
			t.Fatalf("expected Complete=false ApplyRemovals=false, got %+v", res2)
		}
	})

	t.Run("reconciliation write blocks concurrent ConfigService.AddRoot until commit then subsequent write rejects ErrStalePreCheck", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath: %v", err)
		}
		root1Dir := filepath.Join(canonBase, "root-1")
		root2Dir := filepath.Join(canonBase, "root-2")
		cbzMove := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("sync-addroot-move"), method: zip.Store})
		cbzNew := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("sync-addroot-new"), method: zip.Store})
		cbzRoot2 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("sync-addroot-root2"), method: zip.Store})
		writeDiskFile(t, filepath.Join(root1Dir, "a.cbz"), cbzMove)
		writeDiskFile(t, filepath.Join(root2Dir, "r2.cbz"), cbzRoot2)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var seeded []domain.LibraryRoot
			lib, seeded = seedLibraryAndRoots(ctx, t, tx, "Sync AddRoot Blocks Lib", nil, []byte(root1Dir))
			root = seeded[0]
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
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		mustRenameDiskFile(t, filepath.Join(root1Dir, "a.cbz"), filepath.Join(root1Dir, "b.cbz"))
		writeDiskFile(t, filepath.Join(root1Dir, "c.cbz"), cbzNew)

		moveInTx := make(chan struct{})
		releaseMove := make(chan struct{})
		var releaseMoveOnce sync.Once
		closeMove := func() { releaseMoveOnce.Do(func() { close(releaseMove) }) }

		scanDone := make(chan struct{})
		addDone := make(chan struct{})
		addStarted := false
		defer func() {
			cancel()
			closeMove()
			<-scanDone
			if addStarted {
				<-addDone
			}
		}()

		hookTx := &reconcileHookTransactor{
			inner: transactor,
			onAfterUpdateAsset: func() error {
				close(moveInTx)
				select {
				case <-releaseMove:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}
		scHook, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(hook): %v", err)
		}

		var (
			scanRes scanner.ScanResult
			scanErr error
		)
		go func() {
			defer close(scanDone)
			scanRes, scanErr = scHook.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		}()

		select {
		case <-moveInTx:
		case <-scanDone:
			t.Fatalf("scan exited before holding root guard: res=%+v err=%v", scanRes, scanErr)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for move write tx: %v", ctx.Err())
		}

		cfgSvc, err := roots.NewConfigService(transactor, roots.ConfigServiceOptions{})
		if err != nil {
			t.Fatalf("NewConfigService: %v", err)
		}
		admin := adminPrincipalForReconcileTest()

		addStarted = true
		var (
			addedRoot *domain.LibraryRoot
			addErr    error
		)
		go func() {
			defer close(addDone)
			addedRoot, addErr = cfgSvc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
				LibraryID: lib.ID,
				Path:      []byte(root2Dir),
			})
		}()

		if err := waitForScannerLockWaiter(ctx, db, addDone); err != nil {
			t.Fatalf("expected ConfigService.AddRoot to block until move tx commits: %v (addErr=%v)", err, addErr)
		}

		closeMove()
		<-addDone
		if addErr != nil || addedRoot == nil {
			t.Fatalf("AddRoot failed after move commit: root=%+v err=%v", addedRoot, addErr)
		}
		<-scanDone
		if !errors.Is(scanErr, scanner.ErrStalePreCheck) {
			t.Fatalf("expected ErrStalePreCheck on subsequent write after AddRoot commit, got err=%v res=%+v", scanErr, scanRes)
		}
		if scanRes.Complete || scanRes.ApplyRemovals {
			t.Fatalf("expected Complete=false ApplyRemovals=false after root insertion, got %+v", scanRes)
		}
		gotAssets := listAssetsForRoot(ctx, t, transactor, root.ID)
		if len(gotAssets) != 1 || gotAssets[0].ID != orig.ID || gotAssets[0].RelPathDisplay != "b.cbz" {
			t.Fatalf("expected move #1 committed and c.cbz deferred/unindexed after AddRoot, got %+v", gotAssets)
		}
	})

	t.Run("concurrent ConfigService.AddRoot blocks reconciliation write until commit then rejects ErrStalePreCheck", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath: %v", err)
		}
		root1Dir := filepath.Join(canonBase, "root-1")
		root2Dir := filepath.Join(canonBase, "root-2")
		cbzMove := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("sync-addroot-first"), method: zip.Store})
		cbzRoot2 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("sync-addroot-second"), method: zip.Store})
		writeDiskFile(t, filepath.Join(root1Dir, "a.cbz"), cbzMove)
		writeDiskFile(t, filepath.Join(root2Dir, "r2.cbz"), cbzRoot2)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var seeded []domain.LibraryRoot
			lib, seeded = seedLibraryAndRoots(ctx, t, tx, "Sync AddRoot Before Write Lib", nil, []byte(root1Dir))
			root = seeded[0]
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
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		mustRenameDiskFile(t, filepath.Join(root1Dir, "a.cbz"), filepath.Join(root1Dir, "b.cbz"))

		addPreCommit := make(chan struct{})
		releaseAdd := make(chan struct{})
		var releaseAddOnce sync.Once
		closeAdd := func() { releaseAddOnce.Do(func() { close(releaseAdd) }) }

		addDone := make(chan struct{})
		addStarted := false
		scanDone := make(chan struct{})
		defer func() {
			cancel()
			closeAdd()
			<-scanDone
			if addStarted {
				<-addDone
			}
		}()

		holdAddTx := &commitHoldTransactor{
			inner:     transactor,
			preCommit: addPreCommit,
			release:   releaseAdd,
		}
		cfgSvc, err := roots.NewConfigService(holdAddTx, roots.ConfigServiceOptions{})
		if err != nil {
			t.Fatalf("NewConfigService(hold): %v", err)
		}
		admin := adminPrincipalForReconcileTest()
		var addErr error

		hookTx := &reconcileHookTransactor{
			inner: transactor,
			beforeSecondReadCommitted: func() error {
				addStarted = true
				go func() {
					defer close(addDone)
					_, addErr = cfgSvc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
						LibraryID: lib.ID,
						Path:      []byte(root2Dir),
					})
				}()
				select {
				case <-addPreCommit:
					return nil
				case <-addDone:
					return fmt.Errorf("AddRoot exited before pre-commit hold: %w", addErr)
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}
		scHook, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(hook): %v", err)
		}

		var (
			scanRes scanner.ScanResult
			scanErr error
		)
		go func() {
			defer close(scanDone)
			scanRes, scanErr = scHook.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		}()

		if err := waitForScannerLockWaiter(ctx, db, scanDone); err != nil {
			t.Fatalf("expected reconciliation write to block on AddRoot lock: %v (scanErr=%v)", err, scanErr)
		}

		closeAdd()
		<-addDone
		if addErr != nil {
			t.Fatalf("AddRoot failed: %v", addErr)
		}
		<-scanDone
		if !errors.Is(scanErr, scanner.ErrStalePreCheck) {
			t.Fatalf("expected ErrStalePreCheck after AddRoot committed, got err=%v res=%+v", scanErr, scanRes)
		}
		if scanRes.Complete || scanRes.ApplyRemovals || scanRes.MovedCount != 0 {
			t.Fatalf("expected Complete=false ApplyRemovals=false MovedCount=0, got %+v", scanRes)
		}
		got, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, orig.ID)
		if err != nil || got.RelPathDisplay != "a.cbz" {
			t.Fatalf("expected move rolled back leaving a.cbz intact, got (%+v, %v)", got, err)
		}
	})
}

func waitForScannerLockWaiter(ctx context.Context, db *testdb.TestDB, earlyExit <-chan struct{}) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-earlyExit:
			return errors.New("raced transaction exited before blocking on lock")
		case <-waitCtx.Done():
			return fmt.Errorf("waiting for lock contention: %w", waitCtx.Err())
		default:
		}

		var waiting bool
		if err := db.QueryRow(waitCtx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
			)`).Scan(&waiting); err != nil {
			return fmt.Errorf("polling lock waiter: %w", err)
		}
		if waiting {
			return nil
		}

		select {
		case <-earlyExit:
			return errors.New("raced transaction exited before blocking on lock")
		case <-waitCtx.Done():
			return fmt.Errorf("waiting for lock contention: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func TestReconcile_OverlappingMoveAndModifySerializesWithoutDeadlockAndSurfacesStaleState(t *testing.T) {
	t.Parallel()

	t.Run("modify holds volume then locks asset while move waits on volume and surfaces ErrStaleAssetState", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("overlap-base"), method: zip.Store})
		writeDiskFile(t, filepath.Join(rootDir, "a.cbz"), cbzBytes)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Overlap Move/Mod 1", nil, []byte(rootDir))
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
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		mustRenameDiskFile(t, filepath.Join(rootDir, "a.cbz"), filepath.Join(rootDir, "b.cbz"))

		modReady := make(chan struct{})
		releaseMod := make(chan struct{})
		var releaseModOnce sync.Once
		closeMod := func() {
			releaseModOnce.Do(func() { close(releaseMod) })
		}

		var modErr error
		modDone := make(chan struct{})
		modStarted := false

		scanDone := make(chan struct{})
		defer func() {
			cancel()
			closeMod()
			<-scanDone
			if modStarted {
				<-modDone
			}
		}()

		hookTx := &reconcileHookTransactor{
			inner: transactor,
			onLockVolume: func() error {
				modStarted = true
				go func() {
					defer close(modDone)
					modErr = transactor.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
						cw := tx.CatalogueWrites()
						if _, err := cw.GetVolumeByIDForUpdate(ctx, orig.VolumeID); err != nil {
							return err
						}
						close(modReady)
						select {
						case <-releaseMod:
						case <-ctx.Done():
							return ctx.Err()
						}
						// Lock asset AFTER scan's GetVolumeByIDForUpdate is already waiting on volume!
						// This used to deadlock (40P01) when move locked media_assets before volumes.
						asset, err := cw.GetAssetByIDForUpdate(ctx, orig.ID)
						if err != nil {
							return err
						}
						now := time.Now().UTC().Truncate(time.Microsecond)
						d := domain.AssetDigest{
							AssetID:    asset.ID,
							Kind:       domain.DigestKindXXH364Partial,
							Value:      "concurrent-external-modify-digest",
							IsCurrent:  true,
							Reason:     domain.DigestReasonExternalModify,
							RecordedAt: now,
						}
						if err := cw.RecordAssetDigest(ctx, &d); err != nil {
							return err
						}
						updated := *asset
						updated.FileSize = asset.FileSize + 10
						updated.UpdatedAt = now
						return cw.UpdateAssetContent(ctx, &updated)
					})
				}()
				select {
				case <-modReady:
					return nil
				case <-modDone:
					return fmt.Errorf("concurrent modifier exited early: %w", modErr)
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}
		scHook, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(hook): %v", err)
		}

		var (
			scanRes scanner.ScanResult
			scanErr error
		)
		go func() {
			defer close(scanDone)
			scanRes, scanErr = scHook.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		}()

		select {
		case <-modReady:
		case <-scanDone:
			t.Fatalf("scan exited before concurrent modifier acquired volume lock: res=%+v err=%v", scanRes, scanErr)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for modifier lock: %v", ctx.Err())
		}

		if err := waitForScannerLockWaiter(ctx, db, scanDone); err != nil {
			t.Fatalf("expected move transaction to wait on volume lock: %v", err)
		}

		closeMod()
		<-modDone
		if modErr != nil {
			t.Fatalf("concurrent modifier failed: %v", modErr)
		}

		<-scanDone
		if !errors.Is(scanErr, scanner.ErrStaleAssetState) {
			t.Fatalf("expected ErrStaleAssetState from stale move write, got err=%v res=%+v", scanErr, scanRes)
		}
		if scanRes.Complete || scanRes.ApplyRemovals || scanRes.MovedCount != 0 {
			t.Fatalf("expected Complete=false ApplyRemovals=false MovedCount=0, got %+v", scanRes)
		}

		got, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, orig.ID)
		if err != nil {
			t.Fatalf("GetAssetByID: %v", err)
		}
		cur, ok := got.CurrentDigest(domain.DigestKindXXH364Partial)
		if !ok || cur.Value != "concurrent-external-modify-digest" || got.RelPathDisplay != "a.cbz" {
			t.Fatalf("expected concurrent modify preserved at a.cbz, got asset=%+v cur=(%+v, %v)", got, cur, ok)
		}
	})

	t.Run("move holds volume then asset while concurrent modifier waits on volume and commits cleanly", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("overlap-order-2"), method: zip.Store})
		writeDiskFile(t, filepath.Join(rootDir, "a.cbz"), cbzBytes)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Overlap Move/Mod 2", nil, []byte(rootDir))
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
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		mustRenameDiskFile(t, filepath.Join(rootDir, "a.cbz"), filepath.Join(rootDir, "b.cbz"))

		moveUpdated := make(chan struct{})
		releaseMove := make(chan struct{})
		var releaseMoveOnce sync.Once
		closeMove := func() {
			releaseMoveOnce.Do(func() { close(releaseMove) })
		}

		modDone := make(chan struct{})
		scanDone := make(chan struct{})
		var (
			modErr  error
			scanRes scanner.ScanResult
			scanErr error
		)
		defer func() {
			cancel()
			closeMove()
			<-scanDone
			<-modDone
		}()

		hookTx := &reconcileHookTransactor{
			inner: transactor,
			onAfterUpdateAsset: func() error {
				close(moveUpdated)
				select {
				case <-releaseMove:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}
		scHook, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(hook): %v", err)
		}

		go func() {
			defer close(scanDone)
			scanRes, scanErr = scHook.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		}()

		select {
		case <-moveUpdated:
		case <-scanDone:
			close(modDone)
			t.Fatalf("scan exited before holding locks: res=%+v err=%v", scanRes, scanErr)
		case <-ctx.Done():
			close(modDone)
			t.Fatalf("timed out waiting for move UpdateAsset: %v", ctx.Err())
		}

		go func() {
			defer close(modDone)
			modErr = transactor.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
				cw := tx.CatalogueWrites()
				vol, err := cw.GetVolumeByIDForUpdate(ctx, orig.VolumeID)
				if err != nil {
					return err
				}
				asset, err := cw.GetAssetByIDForUpdate(ctx, orig.ID)
				if err != nil {
					return err
				}
				if asset.VolumeID != vol.ID || asset.RelPathDisplay != "b.cbz" {
					return fmt.Errorf("unexpected post-move asset state: %+v", asset)
				}
				return nil
			})
		}()

		if err := waitForScannerLockWaiter(ctx, db, modDone); err != nil {
			t.Fatalf("expected modifier to wait cleanly on volume lock: %v", err)
		}

		closeMove()
		<-scanDone
		if scanErr != nil || !scanRes.Complete || !scanRes.ApplyRemovals || scanRes.MovedCount != 1 {
			t.Fatalf("move scan failed: res=%+v err=%v", scanRes, scanErr)
		}
		<-modDone
		if modErr != nil {
			t.Fatalf("concurrent modifier failed after move commit: %v", modErr)
		}
	})
}

func mustRootPathIdentity(t *testing.T, raw []byte) domain.PathIdentity {
	t.Helper()
	p, err := domain.NewRootPathIdentity(raw)
	if err != nil {
		t.Fatalf("NewRootPathIdentity(%q): %v", raw, err)
	}
	return p
}

func adminPrincipalForReconcileTest() *domain.Principal {
	var adminRole domain.Role
	for _, r := range domain.BuiltInRoles() {
		if r.ID == domain.RoleAdminID {
			adminRole = r
			break
		}
	}
	return domain.NewPrincipal(
		domain.User{ID: "user-admin-reconcile", Username: "admin"},
		[]domain.Role{adminRole},
		domain.CredentialKindPassword,
		"cred-admin-reconcile",
		nil,
	)
}

func TestReconcile_RestoredHistoricalVersionCopiedPreservesOriginalAsset(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	cbzV1 := buildDeterministicZip(t, "", zipEntry{name: "001.jxl", data: []byte("restored-v1-jxl"), method: zip.Store})
	cbzV2 := buildDeterministicZip(t, "",
		zipEntry{name: "001.avif", data: []byte("modified-v2-page-1"), method: zip.Store},
		zipEntry{name: "002.png", data: []byte("modified-v2-page-2"), method: zip.Store},
	)

	pathA := filepath.Join(rootDir, "a.cbz")
	pathB := filepath.Join(rootDir, "b.cbz")
	writeDiskFile(t, pathA, cbzV1)

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Restored Historical Copy Lib", nil, []byte(rootDir))
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
		t.Fatalf("scan 1 = (%+v, %v)", res, err)
	}
	orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]
	v1Digest, ok := orig.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok {
		t.Fatalf("missing initial V1 digest on %+v", orig)
	}
	userID := seedUserAndReadingProgress(ctx, t, db, transactor, "restore-copy-reader", orig, 3)

	// Scan 2: overwrite a.cbz with V2 so V1 becomes historical.
	writeDiskFile(t, pathA, cbzV2)
	t2 := time.Now().Add(2 * time.Second).UTC()
	if err := os.Chtimes(pathA, t2, t2); err != nil {
		t.Fatalf("chtimes V2: %v", err)
	}
	if res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res2.Complete || res2.ModifiedCount != 1 {
		t.Fatalf("scan 2 = (%+v, %v)", res2, err)
	}
	afterV2, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, orig.ID)
	if err != nil {
		t.Fatalf("GetAssetByID(afterV2): %v", err)
	}
	v2Digest, ok := afterV2.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok || v2Digest.Value == v1Digest.Value {
		t.Fatalf("expected rotated V2 digest, got (%+v, %v)", v2Digest, ok)
	}

	// Overwrite a.cbz in place with V1 AND copy V1 to b.cbz.
	writeDiskFile(t, pathA, cbzV1)
	t3 := t2.Add(2 * time.Second)
	if err := os.Chtimes(pathA, t3, t3); err != nil {
		t.Fatalf("chtimes V1: %v", err)
	}
	writeDiskFile(t, pathB, cbzV1)

	res3, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("scan 3: %v", err)
	}
	if !res3.Complete || !res3.ApplyRemovals || res3.ModifiedCount != 1 || res3.IndexedCount != 1 || res3.MovedCount != 0 {
		t.Fatalf("expected 1 modify + 1 index + 0 moves, got %+v", res3)
	}

	assets := listAssetsForRoot(ctx, t, transactor, root.ID)
	if len(assets) != 2 {
		t.Fatalf("expected 2 assets, got %d", len(assets))
	}
	var gotA, gotB domain.MediaAsset
	for _, a := range assets {
		switch a.RelPathDisplay {
		case "a.cbz":
			gotA = a
		case "b.cbz":
			gotB = a
		default:
			t.Fatalf("unexpected asset path %q", a.RelPathDisplay)
		}
	}
	if gotA.ID != orig.ID || gotA.VolumeID != orig.VolumeID {
		t.Fatalf("expected a.cbz to preserve orig ID/volume (%s, %s), got (%s, %s)", orig.ID, orig.VolumeID, gotA.ID, gotA.VolumeID)
	}
	if gotB.ID == orig.ID {
		t.Fatalf("expected b.cbz to receive a new asset ID, got orig ID %s", orig.ID)
	}

	fullA, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, orig.ID)
	if err != nil {
		t.Fatalf("GetAssetByID(orig): %v", err)
	}
	if fullA.PageCount != 1 || !slices.Equal(fullA.Diagnostics.ImageCodecs, []string{"jxl"}) || fullA.FileSize != int64(len(cbzV1)) {
		t.Fatalf("expected restored 1-page JXL facts on a.cbz, got page_count=%d codecs=%v size=%d",
			fullA.PageCount, fullA.Diagnostics.ImageCodecs, fullA.FileSize)
	}
	curA, ok := fullA.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok || curA.Value != v1Digest.Value || curA.Reason != domain.DigestReasonExternalModify {
		t.Fatalf("expected V1 EXTERNAL_MODIFY current digest on a.cbz, got (%+v, %v)", curA, ok)
	}
	if len(fullA.Digests) != 2 {
		t.Fatalf("expected 2 digests retained on a.cbz, got %+v", fullA.Digests)
	}

	assertReadingProgressPreserved(ctx, t, db, userID, orig.VolumeID, orig.ID, 3)

	events := loadOutboxEventsDetailed(ctx, t, db)
	var sawModifiedA, sawAddedB, sawMoved bool
	for _, ev := range events {
		switch ev.Kind {
		case domain.EventAssetModified:
			if ev.AggregateID == orig.ID {
				var p domain.AssetEventPayloadV1
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					t.Fatalf("unmarshal asset.modified: %v", err)
				}
				if p.PathDisplay == "a.cbz" {
					sawModifiedA = true
				}
			}
		case domain.EventAssetAdded:
			if ev.AggregateID == gotB.ID {
				var p domain.AssetEventPayloadV1
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					t.Fatalf("unmarshal asset.added: %v", err)
				}
				if p.PathDisplay == "b.cbz" {
					sawAddedB = true
				}
			}
		case domain.EventAssetMoved:
			sawMoved = true
		}
	}
	if !sawModifiedA || !sawAddedB || sawMoved {
		t.Fatalf("unexpected events: sawModifiedA=%v sawAddedB=%v sawMoved=%v events=%+v", sawModifiedA, sawAddedB, sawMoved, events)
	}

	assertUnchangedRescanZeroOpens(ctx, t, sc, lib.ID, 2)
}

func TestReconcile_ConcurrentTargetedIndexFileAndMoveNoDeadlock(t *testing.T) {
	t.Parallel()

	t.Run("targeted IndexFile holds volume before commit while move waits on library lock", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		cbzBase := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("target-idx-move-1"), method: zip.Store})
		writeDiskFile(t, filepath.Join(rootDir, "a.cbz"), cbzBase)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var seeded []domain.LibraryRoot
			lib, seeded = seedLibraryAndRoots(ctx, t, tx, "Target Index Then Move Lib", nil, []byte(rootDir))
			root = seeded[0]
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
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		mustRenameDiskFile(t, filepath.Join(rootDir, "a.cbz"), filepath.Join(rootDir, "b.cbz"))

		idxPreCommit := make(chan struct{})
		releaseIdx := make(chan struct{})
		var releaseIdxOnce sync.Once
		closeIdx := func() { releaseIdxOnce.Do(func() { close(releaseIdx) }) }

		idxDone := make(chan struct{})
		scanDone := make(chan struct{})
		var (
			idxRes  *scanner.IndexFileResult
			idxErr  error
			scanRes scanner.ScanResult
			scanErr error
		)
		defer func() {
			cancel()
			closeIdx()
			<-idxDone
			<-scanDone
		}()

		holdIdxTx := &commitHoldTransactor{
			inner:     transactor,
			preCommit: idxPreCommit,
			release:   releaseIdx,
		}
		idx, err := scanner.NewIndexer(holdIdxTx, scanner.IndexerOptions{})
		if err != nil {
			t.Fatalf("NewIndexer: %v", err)
		}
		go func() {
			defer close(idxDone)
			idxRes, idxErr = idx.IndexFile(ctx, scanner.IndexFileInput{
				LibraryID:      lib.ID,
				LibraryRootID:  root.ID,
				TargetVolumeID: orig.VolumeID,
				Inspected: scanner.InspectedFile{
					RelPath:    mustRelIdentity(t, []byte("variant.cbz")),
					Size:       512,
					Format:     domain.MediaFormatCBZ,
					DigestKind: domain.DigestKindXXH364Partial,
					Digest:     "fedcba9876543210",
					PageCount:  12,
				},
			})
		}()

		select {
		case <-idxPreCommit:
		case <-idxDone:
			close(scanDone)
			t.Fatalf("targeted IndexFile exited before pre-commit barrier: res=%+v err=%v", idxRes, idxErr)
		case <-ctx.Done():
			close(scanDone)
			t.Fatalf("timed out waiting for IndexFile pre-commit barrier: %v", ctx.Err())
		}

		go func() {
			defer close(scanDone)
			scanRes, scanErr = sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		}()

		if err := waitForScannerLockWaiter(ctx, db, scanDone); err != nil {
			t.Fatalf("expected move scan to wait cleanly on library lock: %v (scanErr=%v)", err, scanErr)
		}

		closeIdx()
		<-idxDone
		if idxErr != nil || idxRes == nil || idxRes.Volume.ID != orig.VolumeID {
			t.Fatalf("targeted IndexFile failed: res=%+v err=%v", idxRes, idxErr)
		}
		<-scanDone
		if scanErr != nil || !scanRes.Complete || !scanRes.ApplyRemovals || scanRes.MovedCount != 1 {
			t.Fatalf("move scan failed: res=%+v err=%v", scanRes, scanErr)
		}

		gotAssets := listAssetsForRoot(ctx, t, transactor, root.ID)
		if len(gotAssets) != 2 {
			t.Fatalf("expected 2 assets on root after concurrent targeted index + move, got %d", len(gotAssets))
		}
		for _, a := range gotAssets {
			if a.VolumeID != orig.VolumeID {
				t.Fatalf("expected asset %s on volume %s, got %s", a.RelPathDisplay, orig.VolumeID, a.VolumeID)
			}
			if a.ID == orig.ID && a.RelPathDisplay != "b.cbz" {
				t.Fatalf("expected orig asset moved to b.cbz, got %s", a.RelPathDisplay)
			}
		}
	})

	t.Run("ScanLibrary move holds library and volume while targeted IndexFile waits on library lock", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		cbzBase := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("target-idx-move-2"), method: zip.Store})
		writeDiskFile(t, filepath.Join(rootDir, "a.cbz"), cbzBase)

		var (
			lib  domain.Library
			root domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var seeded []domain.LibraryRoot
			lib, seeded = seedLibraryAndRoots(ctx, t, tx, "Move Then Target Index Lib", nil, []byte(rootDir))
			root = seeded[0]
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
		orig := listAssetsForRoot(ctx, t, transactor, root.ID)[0]

		mustRenameDiskFile(t, filepath.Join(rootDir, "a.cbz"), filepath.Join(rootDir, "b.cbz"))

		moveUpdated := make(chan struct{})
		releaseMove := make(chan struct{})
		var releaseMoveOnce sync.Once
		closeMove := func() { releaseMoveOnce.Do(func() { close(releaseMove) }) }

		scanDone := make(chan struct{})
		idxDone := make(chan struct{})
		idxStarted := false
		var (
			scanRes scanner.ScanResult
			scanErr error
			idxRes  *scanner.IndexFileResult
			idxErr  error
		)
		defer func() {
			cancel()
			closeMove()
			<-scanDone
			if idxStarted {
				<-idxDone
			}
		}()

		hookTx := &reconcileHookTransactor{
			inner: transactor,
			onAfterUpdateAsset: func() error {
				close(moveUpdated)
				select {
				case <-releaseMove:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}
		scHook, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(hook): %v", err)
		}
		go func() {
			defer close(scanDone)
			scanRes, scanErr = scHook.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		}()

		select {
		case <-moveUpdated:
		case <-scanDone:
			t.Fatalf("scan exited before holding locks: res=%+v err=%v", scanRes, scanErr)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for move UpdateAsset: %v", ctx.Err())
		}

		idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
		if err != nil {
			t.Fatalf("NewIndexer: %v", err)
		}
		idxStarted = true
		go func() {
			defer close(idxDone)
			idxRes, idxErr = idx.IndexFile(ctx, scanner.IndexFileInput{
				LibraryID:      lib.ID,
				LibraryRootID:  root.ID,
				TargetVolumeID: orig.VolumeID,
				Inspected: scanner.InspectedFile{
					RelPath:    mustRelIdentity(t, []byte("variant.cbz")),
					Size:       512,
					Format:     domain.MediaFormatCBZ,
					DigestKind: domain.DigestKindXXH364Partial,
					Digest:     "0123456789abcdef",
					PageCount:  12,
				},
			})
		}()

		if err := waitForScannerLockWaiter(ctx, db, idxDone); err != nil {
			t.Fatalf("expected targeted IndexFile to wait cleanly on library lock: %v (idxErr=%v)", err, idxErr)
		}

		closeMove()
		<-scanDone
		if scanErr != nil || !scanRes.Complete || !scanRes.ApplyRemovals || scanRes.MovedCount != 1 {
			t.Fatalf("move scan failed: res=%+v err=%v", scanRes, scanErr)
		}
		<-idxDone
		if idxErr != nil || idxRes == nil || idxRes.Volume.ID != orig.VolumeID {
			t.Fatalf("targeted IndexFile failed after move commit: res=%+v err=%v", idxRes, idxErr)
		}

		gotAssets := listAssetsForRoot(ctx, t, transactor, root.ID)
		if len(gotAssets) != 2 {
			t.Fatalf("expected 2 assets on root after concurrent move + targeted index, got %d", len(gotAssets))
		}
		for _, a := range gotAssets {
			if a.VolumeID != orig.VolumeID {
				t.Fatalf("expected asset %s on volume %s, got %s", a.RelPathDisplay, orig.VolumeID, a.VolumeID)
			}
			if a.ID == orig.ID && a.RelPathDisplay != "b.cbz" {
				t.Fatalf("expected orig asset moved to b.cbz, got %s", a.RelPathDisplay)
			}
		}
	})
}
